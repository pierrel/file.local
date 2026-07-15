use std::fs;

use anyhow::Result;
use flocal::model::{Entry, ObjectHash, PeerId, Record, RelativePath, Version};
use flocal::reconcile::{Conflict, Plan};
use flocal::state::State;
use flocal::sync::{self, Message};
use tempfile::tempdir;

fn record(path: &[u8], entry: Entry) -> Result<Record> {
    Ok(Record {
        path: RelativePath::from_bytes(path.to_vec())?,
        version: Version {
            peer: PeerId("peer".into()),
            sequence: 1,
            timestamp_ns: 1,
            seen: Vec::new(),
            entry,
        },
    })
}

#[test]
fn snapshot_and_plan_protocol_round_trip() -> Result<()> {
    let records = vec![
        record(b"dir", Entry::Directory)?,
        record(
            b"link",
            Entry::Symlink {
                target: b"dir".to_vec(),
            },
        )?,
        record(b"gone", Entry::Tombstone)?,
    ];
    let mut wire = Vec::new();
    sync::write_snapshot(&mut wire, &records)?;
    assert_eq!(sync::read_snapshot(&mut wire.as_slice())?, records);

    let conflict = Conflict {
        path: records[2].path.clone(),
        winner: records[2].clone(),
        loser: records[0].clone(),
    };
    let plan = Plan {
        records: records.clone(),
        conflicts: vec![conflict],
    };
    let mut wire = Vec::new();
    sync::write_plan(&mut wire, &plan)?;
    let mut input = wire.as_slice();
    let mut observed_records = Vec::new();
    let mut observed_conflicts = Vec::new();
    loop {
        match sync::read_message(&mut input)? {
            Message::ApplyChunk { records, conflicts } => {
                observed_records.extend(records);
                observed_conflicts.extend(conflicts);
            }
            Message::ApplyEnd => break,
            other => panic!("unexpected message: {other:?}"),
        }
    }
    assert_eq!(observed_records, plan.records);
    assert_eq!(observed_conflicts, plan.conflicts);
    assert!(sync::read_snapshot(&mut [].as_slice()).is_err());
    let mut wrong_snapshot = Vec::new();
    sync::write_message(&mut wrong_snapshot, &Message::Done)?;
    assert!(sync::read_snapshot(&mut wrong_snapshot.as_slice()).is_err());
    assert!(
        sync::write_message(
            &mut Vec::new(),
            &Message::ObjectChunk {
                data: vec![0; sync::MAX_FRAME],
            },
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn object_transfer_round_trip_and_failures() -> Result<()> {
    let temp = tempdir()?;
    let source = State::open(temp.path().join("source"))?;
    let target = State::open(temp.path().join("target"))?;
    let bytes = vec![7u8; 300_000];
    let input_path = temp.path().join("input");
    fs::write(&input_path, &bytes)?;
    let (hash, size) = source.store_object(fs::File::open(&input_path)?)?;
    assert_eq!(size, bytes.len() as u64);
    let mut wire = Vec::new();
    sync::send_object(&source, &hash, &mut wire)?;
    let mut input = wire.as_slice();
    match sync::read_message(&mut input)? {
        Message::ObjectStart {
            hash: sent,
            size: sent_size,
        } => {
            assert_eq!(sent, hash);
            sync::receive_object(&target, sent, sent_size, &mut input)?;
        }
        other => panic!("unexpected message: {other:?}"),
    }
    assert_eq!(target.read_object(&hash)?, bytes);

    let wrong = ObjectHash::from_blake3(blake3::hash(b"wrong"));
    let mut invalid = Vec::new();
    sync::write_message(
        &mut invalid,
        &Message::ObjectChunk {
            data: b"data".to_vec(),
        },
    )?;
    sync::write_message(&mut invalid, &Message::ObjectEnd)?;
    assert!(sync::receive_object(&target, wrong, 4, &mut invalid.as_slice()).is_err());
    let unexpected = ObjectHash::from_blake3(blake3::hash(b"none"));
    let mut invalid = Vec::new();
    sync::write_message(&mut invalid, &Message::Done)?;
    assert!(sync::receive_object(&target, unexpected, 4, &mut invalid.as_slice()).is_err());
    Ok(())
}

#[test]
fn required_hashes_respect_ignore_and_cached_integrity() -> Result<()> {
    let temp = tempdir()?;
    let root = temp.path().join("root");
    fs::create_dir_all(&root)?;
    fs::write(root.join(".gitignore"), "ignored\n")?;
    let state = State::open(temp.path().join("state"))?;
    let share = state.init_share(&root)?;
    let hash = ObjectHash::from_blake3(blake3::hash(b"data"));
    let included = record(
        b"included",
        Entry::File {
            hash: hash.clone(),
            size: 4,
            executable: false,
        },
    )?;
    let ignored = Record {
        path: RelativePath::from_bytes(b"ignored".to_vec())?,
        ..included.clone()
    };
    assert_eq!(
        sync::required_hashes(&state, &[included.clone(), included.clone()]),
        vec![hash.clone()]
    );
    assert_eq!(
        sync::required_hashes_for_share(&state, &share, &[included.clone(), ignored])?,
        vec![hash.clone()]
    );
    state.import_object(&hash, b"data")?;
    assert!(sync::required_hashes(&state, std::slice::from_ref(&included)).is_empty());
    assert!(sync::has_verified_object(&state, &hash));
    let mut conflicting = included.clone();
    if let Entry::File { size, .. } = &mut conflicting.version.entry {
        *size = 5;
    }
    assert!(
        sync::required_hashes_for_share(&state, &share, &[included.clone(), conflicting]).is_err()
    );
    Ok(())
}

#[test]
fn apply_plan_rejects_duplicate_paths_before_installing() -> Result<()> {
    let temp = tempdir()?;
    let root = temp.path().join("root");
    fs::create_dir_all(&root)?;
    let mut state = State::open(temp.path().join("state"))?;
    let share = state.init_share(&root)?;
    let duplicate = record(b"duplicate", Entry::Directory)?;

    let error = sync::apply_plan(&mut state, &share, &[duplicate.clone(), duplicate])
        .expect_err("duplicate paths must be rejected");
    assert_eq!(error.to_string(), "apply plan contains duplicate paths");
    assert!(state.install_intent(&share)?.is_none());
    Ok(())
}

#[test]
fn model_formatting_and_reconcile_cover_nonconflicting_branches() -> Result<()> {
    assert!(RelativePath::from_bytes(Vec::new()).is_err());
    assert!(RelativePath::from_bytes(b"nul\0path".to_vec()).is_err());
    let path = RelativePath::from_bytes(b"non-utf8-\xff".to_vec())?;
    assert!(format!("{path:?}").contains("RelativePath"));
    let hash = ObjectHash::from_blake3(blake3::hash(b"value"));
    assert_eq!(hash.to_string(), hash.as_str());

    let mut local = record(b"same", Entry::Directory)?;
    local.version.peer = PeerId("local".into());
    local.version.timestamp_ns = 2;
    let mut remote = local.clone();
    remote.version.peer = PeerId("remote".into());
    remote.version.timestamp_ns = 1;
    let equal_entry =
        flocal::reconcile::reconcile(std::slice::from_ref(&local), std::slice::from_ref(&remote));
    assert_eq!(equal_entry.records[0].version.peer, local.version.peer);

    local.version.seen.push(remote.version.id());
    let causal =
        flocal::reconcile::reconcile(std::slice::from_ref(&local), std::slice::from_ref(&remote));
    assert_eq!(causal.records[0].version.peer, local.version.peer);
    let union =
        flocal::reconcile::reconcile(&[local], &[remote, record(b"remote", Entry::Directory)?]);
    assert_eq!(union.records.len(), 2);
    Ok(())
}
