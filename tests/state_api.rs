use std::fs;
use std::path::Path;

use anyhow::Result;
use flocal::model::{
    Entry, ObjectHash, PeerConfig, PeerId, Record, RelativePath, ShareId, Version,
};
use flocal::reconcile::Conflict;
use flocal::state::{InstallTempPhase, State};
use tempfile::tempdir;

#[cfg(unix)]
fn path_bytes(path: &Path) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    path.as_os_str().as_bytes().to_vec()
}

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
    assert!(state.init_share(&nested).is_err());
    let deeper = nested.join("deeper");
    fs::create_dir_all(&deeper)?;
    assert_eq!(state.find_share(&deeper)?.0, share);
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
    assert!(!state.is_owned_temp(&share, Path::new("file"))?);
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
    assert!(!state.is_owned_temp(&share, Path::new("file"))?);
    assert!(state.is_owned_temp(&share, Path::new(&new_token))?);
    assert!(state.rotate_unowned_install_temp(&share, &path).is_err());
    assert_eq!(state.install_intents()?.len(), 1);
    state.clear_install_intent(&share)?;
    assert!(state.install_intent(&share)?.is_none());

    state.replace_records(&share, &records)?;
    assert_eq!(state.records(&share)?, records);
    state.set_initial_complete(&share)?;
    assert!(state.initial_complete(&share)?);
    state.set_watch_enabled(&share, true)?;
    state.set_blocked(&share, "root identity changed")?;
    let managed = state.managed_share(&share)?;
    assert!(managed.watch_enabled);
    assert_eq!(
        managed.blocked_diagnostic.as_deref(),
        Some("root identity changed")
    );
    state.clear_blocked(&share)?;
    let generation = state.watch_intent_generation(&share)?;
    state.set_initial_complete_and_watch_enabled(&share, generation)?;
    assert!(state.managed_share(&share)?.initial_complete);

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

#[test]
fn bound_peer_of_an_unregistered_share_is_none() -> Result<()> {
    let temp = tempdir()?;
    let state = State::open(temp.path().join("state"))?;
    assert!(
        state
            .bound_peer(&ShareId("never-registered".into()))?
            .is_none()
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn state_refuses_a_symlinked_state_directory() -> Result<()> {
    let temp = tempdir()?;
    let target = temp.path().join("target");
    let link = temp.path().join("state");
    fs::create_dir(&target)?;
    std::os::unix::fs::symlink(&target, &link)?;
    assert!(State::open(&link).is_err());
    Ok(())
}

#[test]
fn concurrent_overlapping_registrations_admit_at_most_one_root() -> Result<()> {
    let temp = tempdir()?;
    let root = temp.path().join("root");
    let nested = root.join("nested");
    let state_dir = temp.path().join("state");
    fs::create_dir_all(&nested)?;
    State::open(&state_dir)?;
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let first_barrier = barrier.clone();
    let first_state = state_dir.clone();
    let first_root = root.clone();
    let first = std::thread::spawn(move || {
        let state = State::open(first_state)?;
        first_barrier.wait();
        state.init_share(&first_root)
    });
    let second_barrier = barrier.clone();
    let second_state = state_dir.clone();
    let second_nested = nested.clone();
    let second = std::thread::spawn(move || {
        let state = State::open(second_state)?;
        second_barrier.wait();
        state.init_share(&second_nested)
    });
    let outcomes = [
        first.join().expect("first registration thread"),
        second.join().expect("second registration thread"),
    ];
    let successes = outcomes.iter().filter(|outcome| outcome.is_ok()).count();
    assert_eq!(successes, 1);
    Ok(())
}

#[test]
fn root_identity_is_persisted_and_replacements_are_not_adopted() -> Result<()> {
    let temp = tempdir()?;
    let root = temp.path().join("root");
    let original = temp.path().join("original-root");
    fs::create_dir(&root)?;
    let state_dir = temp.path().join("state");
    let state = State::open(&state_dir)?;
    let share = state.init_share(&root)?;
    let expected = state.expected_root_identity(&share)?;
    assert_eq!(state.validate_root_identity(&share)?, expected);
    drop(state);

    fs::rename(&root, &original)?;
    fs::create_dir(&root)?;
    let state = State::open(&state_dir)?;
    let error = state
        .validate_root_identity(&share)
        .expect_err("replacement root must not be adopted");
    assert!(error.to_string().contains("root identity changed"));
    assert_eq!(state.expected_root_identity(&share)?, expected);
    drop(state);

    fs::remove_dir(&root)?;
    let state = State::open(&state_dir)?;
    let missing = state
        .validate_root_identity(&share)
        .expect_err("missing root must be terminal");
    assert!(
        missing
            .downcast_ref::<flocal::state::RootIdentityChanged>()
            .is_some()
    );
    assert!(missing.to_string().contains("is unavailable"));
    drop(state);

    std::os::unix::fs::symlink(&original, &root)?;
    let state = State::open(&state_dir)?;
    let symlink = state
        .validate_root_identity(&share)
        .expect_err("symlinked root must be terminal");
    assert!(
        symlink
            .downcast_ref::<flocal::state::RootIdentityChanged>()
            .is_some()
    );
    drop(state);
    fs::remove_file(&root)?;

    fs::rename(&original, &root)?;
    let state = State::open(&state_dir)?;
    assert_eq!(state.validate_root_identity(&share)?, expected);
    let query_error = state
        .expected_root_identity(&ShareId("missing-share".into()))
        .expect_err("missing rows remain ordinary state/query errors");
    assert!(
        query_error
            .downcast_ref::<flocal::state::RootIdentityChanged>()
            .is_none()
    );
    drop(state);

    let connection = rusqlite::Connection::open(state_dir.join("state.sqlite3"))?;
    connection.execute(
        "UPDATE shares SET root_device='invalid' WHERE share_id=?1",
        [&share.0],
    )?;
    drop(connection);
    let state = State::open(&state_dir)?;
    let corrupt = state
        .expected_root_identity(&share)
        .expect_err("malformed persisted identity must be terminal");
    assert!(
        corrupt
            .downcast_ref::<flocal::state::RootIdentityChanged>()
            .is_some()
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn initialization_refuses_a_symlinked_root() -> Result<()> {
    use std::os::unix::fs::symlink;

    let temporary = tempdir()?;
    let target = temporary.path().join("target");
    let root = temporary.path().join("root");
    std::fs::create_dir(&target)?;
    symlink(&target, &root)?;
    let state = State::open(temporary.path().join("state"))?;
    let error = state
        .init_share(&root)
        .expect_err("a symlink must not become a managed root");
    assert!(error.to_string().contains("symbolic link"));
    Ok(())
}

#[test]
fn legacy_root_identity_backfill_is_transactional() -> Result<()> {
    let temp = tempdir()?;
    let valid = temp.path().join("valid");
    let missing = temp.path().join("missing");
    fs::create_dir(&valid)?;
    let state_dir = temp.path().join("state");
    fs::create_dir(&state_dir)?;
    let database = state_dir.join("state.sqlite3");
    {
        let connection = rusqlite::Connection::open(&database)?;
        connection.execute_batch(
            "CREATE TABLE shares (
                share_id TEXT PRIMARY KEY,
                root BLOB NOT NULL UNIQUE,
                sequence INTEGER NOT NULL DEFAULT 0,
                initial_complete INTEGER NOT NULL DEFAULT 0,
                peer_json TEXT,
                bound_peer TEXT
            );",
        )?;
        connection.execute(
            "INSERT INTO shares(share_id, root) VALUES(?1, ?2)",
            rusqlite::params!["valid", path_bytes(&valid.canonicalize()?)],
        )?;
        connection.execute(
            "INSERT INTO shares(share_id, root) VALUES(?1, ?2)",
            rusqlite::params!["missing", path_bytes(&missing)],
        )?;
    }

    let error = match State::open(&state_dir) {
        Ok(_) => panic!("one invalid root must abort all backfill"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("cannot bind legacy share"));
    let connection = rusqlite::Connection::open(&database)?;
    let columns: Vec<String> = connection
        .prepare("PRAGMA table_info(shares)")?
        .query_map([], |row| row.get(1))?
        .collect::<Result<_, _>>()?;
    assert!(!columns.iter().any(|column| column == "root_device"));
    drop(connection);

    fs::create_dir(&missing)?;
    let state = State::open(&state_dir)?;
    assert!(
        state
            .expected_root_identity(&ShareId("valid".into()))
            .is_ok()
    );
    assert!(
        state
            .expected_root_identity(&ShareId("missing".into()))
            .is_ok()
    );
    Ok(())
}
