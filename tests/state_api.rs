use std::fs;

use anyhow::Result;
use flocal::model::{
    Entry, ObjectHash, PeerConfig, PeerId, Record, RelativePath, ShareId, Version,
};
use flocal::reconcile::Conflict;
use flocal::state::{InstallTempPhase, State};
use tempfile::tempdir;

fn record(path: &[u8], peer: &str, sequence: u64, entry: Entry) -> Result<Record> {
    Ok(Record {
        path: RelativePath::from_bytes(path.to_vec())?,
        version: Version {
            peer: PeerId(peer.into()),
            sequence,
            timestamp_ns: sequence as i64,
            seen: Vec::new(),
            entry,
        },
    })
}

#[test]
fn state_metadata_and_install_intent_lifecycle() -> Result<()> {
    let temp = tempdir()?;
    let root = temp.path().join("root");
    let nested = root.join("nested");
    fs::create_dir_all(&nested)?;
    let mut state = State::open(temp.path().join("state"))?;
    let share = state.init_share(&root)?;
    assert_eq!(state.find_share(&nested)?.0, share);
    let nested_share = state.init_share(&nested)?;
    let deeper = nested.join("deeper");
    fs::create_dir_all(&deeper)?;
    assert_eq!(state.find_share(&deeper)?.0, nested_share);
    assert_eq!(state.root_for(&share)?, root.canonicalize()?);
    assert!(state.lock_share(&share).is_ok());
    assert!(state.lock_global_sync().is_ok());
    assert_eq!(state.next_sequence(&share)?, 1);
    assert_eq!(state.next_sequence(&share)?, 2);

    let path = RelativePath::from_bytes(b"file".to_vec())?;
    let records = vec![record(b"file", "peer", 1, Entry::Tombstone)?];
    let (intent, created) = state.set_install_intent(&share, &records)?;
    assert!(created);
    assert!(matches!(intent.temps[0].phase, InstallTempPhase::Pending));
    assert!(!state.is_owned_temp(&share, &path)?);
    state.mark_install_temp_creating(&share, &path)?;
    assert!(matches!(
        state.install_intent(&share)?.unwrap().temps[0].phase,
        InstallTempPhase::Creating
    ));
    let old_token = state.install_intent(&share)?.unwrap().temps[0]
        .token
        .clone();
    let new_token = state.rotate_unowned_install_temp(&share, &path)?;
    assert_ne!(old_token, new_token);
    state.mark_install_temp_owned(&share, &path)?;
    assert!(!state.is_owned_temp(&share, &path)?);
    assert!(state.is_owned_temp(&share, &RelativePath::from_bytes(new_token.into_bytes())?)?);
    assert!(state.rotate_unowned_install_temp(&share, &path).is_err());
    assert_eq!(state.install_intents()?.len(), 1);
    state.clear_install_intent(&share)?;
    assert!(state.install_intent(&share)?.is_none());

    state.replace_records(&share, &records)?;
    assert_eq!(state.records(&share)?, records);
    state.set_initial_complete(&share)?;
    assert!(state.initial_complete(&share)?);

    let peer = PeerConfig {
        peer_id: PeerId("remote".into()),
        host: "host".into(),
        remote_path: b"/remote".to_vec(),
        executable: "/usr/bin/flocal".into(),
    };
    state.set_peer(&share, &peer)?;
    assert_eq!(state.peer(&share)?, Some(peer));
    Ok(())
}

#[test]
fn registered_share_conflict_and_object_lifecycle() -> Result<()> {
    let temp = tempdir()?;
    let root = temp.path().join("root");
    fs::create_dir_all(&root)?;
    let mut state = State::open(temp.path().join("state"))?;
    let share = ShareId("share-test".into());
    let bound = PeerId("bound".into());
    state.register_share_bound(&share, &root, &bound)?;
    assert_eq!(state.bound_peer(&share)?, Some(bound));
    state.register_share(&share, &root)?;

    let bytes = b"content";
    let hash = ObjectHash::from_blake3(blake3::hash(bytes));
    state.import_object(&hash, bytes)?;
    assert_eq!(state.read_object(&hash)?, bytes);
    assert_eq!(state.open_verified_object(&hash)?.metadata()?.len(), 7);
    let file = fs::File::open(state.object_path(&hash))?;
    assert_eq!(state.hash_object(file)?.0, hash);

    let winner = record(
        b"file",
        "winner",
        2,
        Entry::File {
            hash: hash.clone(),
            size: 7,
            executable: false,
        },
    )?;
    let loser = record(b"file", "loser", 1, Entry::Tombstone)?;
    let conflict = Conflict {
        path: winner.path.clone(),
        winner,
        loser,
    };
    state.add_conflicts(&share, std::slice::from_ref(&conflict))?;
    let stored = state.conflicts(&share)?;
    assert_eq!(stored.len(), 1);
    assert_eq!(state.conflict(&share, &stored[0].id)?.id, stored[0].id);
    state.prune_unreferenced_objects()?;
    assert!(state.object_path(&hash).exists());

    let other = ObjectHash::from_blake3(blake3::hash(b"other"));
    let mut sink = state.begin_object(other.clone(), 5)?;
    assert!(sink.write_chunk(b"too long").is_err());
    drop(sink);
    let mut sink = state.begin_object(other.clone(), 5)?;
    sink.write_chunk(b"other")?;
    sink.finish()?;
    assert_eq!(state.read_object(&other)?, b"other");
    state.prune_unreferenced_objects()?;
    assert!(!state.object_path(&other).exists());
    Ok(())
}

#[test]
fn state_rejects_conflicting_bindings_and_invalid_objects() -> Result<()> {
    let temp = tempdir()?;
    let first = temp.path().join("first");
    let second = temp.path().join("second");
    fs::create_dir_all(&first)?;
    fs::create_dir_all(&second)?;
    let mut state = State::open(temp.path().join("state"))?;
    let initialized = state.init_share(&first)?;
    assert!(state.init_share(&first).is_err());
    assert!(state.register_share(&initialized, &second).is_err());

    let unbound = ShareId("share-unbound".into());
    state.register_share(&unbound, &second)?;
    state.register_share_bound(&unbound, &second, &PeerId("peer-one".into()))?;
    assert!(
        state
            .register_share_bound(&unbound, &second, &PeerId("peer-two".into()))
            .is_err()
    );
    assert!(
        state
            .register_share_bound(&unbound, &first, &PeerId("peer-one".into()))
            .is_err()
    );

    let hash = ObjectHash::from_blake3(blake3::hash(b"valid"));
    assert!(state.import_object(&hash, b"invalid").is_err());
    state.import_object(&hash, b"valid")?;
    state.import_object(&hash, b"valid")?;
    let input = temp.path().join("input");
    fs::write(&input, b"valid")?;
    assert_eq!(state.store_object(fs::File::open(&input)?)?.0, hash);

    let incomplete = ObjectHash::from_blake3(blake3::hash(b"five"));
    let mut sink = state.begin_object(incomplete, 5)?;
    sink.write_chunk(b"four")?;
    assert!(sink.finish().is_err());
    let wrong = ObjectHash::from_blake3(blake3::hash(b"other"));
    let mut sink = state.begin_object(wrong, 4)?;
    sink.write_chunk(b"four")?;
    assert!(sink.finish().is_err());
    Ok(())
}
