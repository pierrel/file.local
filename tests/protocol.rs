use std::fs;

use anyhow::Result;
use flocal::model::{Entry, ObjectHash, PeerId, Record, RelativePath, Version};
use flocal::reconcile::{Conflict, Plan};
use flocal::state::State;
use flocal::sync::{self, InitialMessage, Message, V2Envelope, V2RoundFrame, V2SessionFrame};
use tempfile::tempdir;

fn record(path: &[u8], entry: Entry) -> Result<Record> {
    Ok(Record {
        path: RelativePath::from_bytes(path.to_vec())?,
        version: Version {
            peer: PeerId("peer".into()),
            sequence: 1,
            id_authenticator: None,
            timestamp_ns: 1,
            seen: Vec::new(),
            merge_base: None,
            version_authenticator: None,
            base_authenticator: None,
            entry,
        },
    })
}

#[test]
fn explicit_v3_wire_format_and_initial_dispatch_are_versioned() -> Result<()> {
    let message = Message::Sync {
        protocol: sync::SYNC_PROTOCOL_VERSION,
        share: flocal::model::ShareId("share".into()),
        peer: PeerId("peer".into()),
        dry_run: false,
    };
    let mut wire = Vec::new();
    sync::write_message(&mut wire, &message)?;
    assert_eq!(
        &wire[4..],
        br#"{"type":"sync","protocol":3,"share":"share","peer":"peer","dry_run":false}"#
    );
    assert!(matches!(
        sync::read_message(&mut wire.as_slice())?,
        Message::Sync { protocol: 3, .. }
    ));

    let initial = InitialMessage::Sync {
        protocol: sync::SYNC_PROTOCOL_VERSION,
        share: flocal::model::ShareId("share".into()),
        peer: PeerId("peer".into()),
        dry_run: false,
    };
    let mut initial_wire = Vec::new();
    sync::write_initial_message(&mut initial_wire, &initial)?;
    assert_eq!(
        sync::read_initial_message(&mut initial_wire.as_slice())?,
        initial
    );

    let mut non_handshake = Vec::new();
    sync::write_message(&mut non_handshake, &Message::Done)?;
    assert!(sync::read_initial_message(&mut non_handshake.as_slice()).is_err());
    Ok(())
}

#[test]
fn v2_session_and_round_frames_are_closed_and_round_tagged() -> Result<()> {
    let share = flocal::model::ShareId("share".into());
    let peer = PeerId("peer".into());
    let hash = ObjectHash::from_blake3(blake3::hash(b"object"));
    let sample = record(b"file", Entry::Tombstone)?;
    let conflict = Conflict::whole_file(
        sample.clone(),
        record(b"loser", Entry::Directory)?,
        flocal::merge::FallbackReason::IncompatibleKind,
    );
    let session_frames = vec![
        V2SessionFrame::WatchOpen {
            protocol: sync::WATCH_PROTOCOL_VERSION,
            share,
            peer: peer.clone(),
        },
        V2SessionFrame::WatchAccepted {
            protocol: sync::WATCH_PROTOCOL_VERSION,
            peer,
        },
        V2SessionFrame::Ready { generation: 3 },
        V2SessionFrame::UnsettledChunk {
            paths: vec![sample.path.clone()],
        },
        V2SessionFrame::Changed { generation: 4 },
        V2SessionFrame::Ping { nonce: 5 },
        V2SessionFrame::Pong { nonce: 5 },
    ];
    for frame in session_frames {
        let envelope = V2Envelope::Session { frame };
        let mut wire = Vec::new();
        sync::write_v2_envelope(&mut wire, &envelope)?;
        assert_eq!(sync::read_v2_envelope(&mut wire.as_slice())?, envelope);
        assert!(sync::read_message(&mut wire.as_slice()).is_err());
    }

    let round_frames = vec![
        V2RoundFrame::SyncStart {
            connector_generation: 7,
            responder_generation: 8,
        },
        V2RoundFrame::SyncAccepted,
        V2RoundFrame::SnapshotChunk {
            records: vec![sample.clone()],
        },
        V2RoundFrame::SnapshotEnd,
        V2RoundFrame::Need {
            hashes: vec![hash.clone()],
        },
        V2RoundFrame::ObjectStart { hash, size: 6 },
        V2RoundFrame::ObjectChunk {
            data: b"object".to_vec(),
        },
        V2RoundFrame::ObjectEnd,
        V2RoundFrame::ApplyChunk {
            records: vec![sample.clone()],
            conflicts: vec![conflict],
            merges: Vec::new(),
        },
        V2RoundFrame::ApplyEnd,
        V2RoundFrame::Applied,
        V2RoundFrame::RoundInvalidated {
            path: sample.path.clone(),
        },
        V2RoundFrame::Done,
        V2RoundFrame::SyncFinished,
        V2RoundFrame::SyncFailed {
            retryable: true,
            message: "temporary".into(),
        },
    ];
    for frame in round_frames {
        let envelope = V2Envelope::Round { round: 11, frame };
        let mut wire = Vec::new();
        sync::write_v2_envelope(&mut wire, &envelope)?;
        assert_eq!(sync::read_v2_envelope(&mut wire.as_slice())?, envelope);
        let json = std::str::from_utf8(&wire[4..])?;
        assert!(json.contains("\"scope\":\"round\"") && json.contains("\"round\":11"));
    }
    Ok(())
}

#[test]
fn versioned_decoders_reject_cross_version_untagged_unknown_and_oversized_frames() -> Result<()> {
    let initial = InitialMessage::WatchOpen {
        protocol: sync::WATCH_PROTOCOL_VERSION,
        share: flocal::model::ShareId("share".into()),
        peer: PeerId("peer".into()),
    };
    let mut wire = Vec::new();
    sync::write_initial_message(&mut wire, &initial)?;
    assert!(sync::read_message(&mut wire.as_slice()).is_err());
    assert!(sync::read_v2_envelope(&mut wire.as_slice()).is_err());

    fn framed(json: &[u8]) -> Vec<u8> {
        let mut wire = (json.len() as u32).to_be_bytes().to_vec();
        wire.extend_from_slice(json);
        wire
    }
    let untagged = framed(br#"{"type":"done"}"#);
    assert!(sync::read_v2_envelope(&mut untagged.as_slice()).is_err());
    let unknown = framed(br#"{"scope":"session","frame":{"type":"surprise"}}"#);
    assert!(sync::read_v2_envelope(&mut unknown.as_slice()).is_err());
    let extra = framed(br#"{"scope":"session","frame":{"type":"ping","nonce":1,"extra":2}}"#);
    assert!(sync::read_v2_envelope(&mut extra.as_slice()).is_err());

    let oversized = ((sync::MAX_FRAME as u32) + 1).to_be_bytes().to_vec();
    assert!(sync::read_initial_message(&mut oversized.as_slice()).is_err());
    assert!(sync::read_v2_envelope(&mut oversized.as_slice()).is_err());
    assert!(
        sync::write_v2_envelope(
            &mut Vec::new(),
            &V2Envelope::Round {
                round: 1,
                frame: V2RoundFrame::ObjectChunk {
                    data: vec![0; sync::MAX_FRAME],
                },
            },
        )
        .is_err()
    );
    Ok(())
}

#[test]
#[cfg(unix)]
fn persistent_frames_have_absolute_slow_read_and_blocked_write_deadlines() -> Result<()> {
    use std::io::Write as _;
    use std::os::unix::net::UnixStream;
    use std::time::{Duration, Instant};

    let (mut slow_writer, slow_reader) = UnixStream::pair()?;
    let drip = std::thread::spawn(move || {
        slow_writer.write_all(&[0]).unwrap();
        std::thread::sleep(Duration::from_millis(100));
        let _ = slow_writer.write_all(&[0, 0, 0]);
    });
    let error =
        sync::read_v2_envelope_until(&slow_reader, Instant::now() + Duration::from_millis(20))
            .expect_err("a slow-dripped prefix must time out absolutely");
    assert!(
        format!("{error:#}").contains("deadline exceeded"),
        "{error:#}"
    );
    drip.join().expect("slow writer joins");

    let (blocked_writer, _blocked_reader) = UnixStream::pair()?;
    let large = V2Envelope::Round {
        round: 1,
        frame: V2RoundFrame::ObjectChunk {
            data: vec![0; sync::MAX_FRAME / 4],
        },
    };
    let error = sync::write_v2_envelope_until(
        &blocked_writer,
        &large,
        Instant::now() + Duration::from_millis(20),
    )
    .expect_err("a peer that stops reading must not block a frame forever");
    assert!(
        format!("{error:#}").contains("deadline exceeded"),
        "{error:#}"
    );

    let (writer, reader) = UnixStream::pair()?;
    let envelope = V2Envelope::Session {
        frame: V2SessionFrame::Ping { nonce: 9 },
    };
    sync::write_v2_envelope_until(&writer, &envelope, Instant::now() + Duration::from_secs(1))?;
    assert_eq!(
        sync::read_v2_envelope_until(&reader, Instant::now() + Duration::from_secs(1))?,
        envelope
    );
    Ok(())
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

    let conflict = Conflict::whole_file(
        records[2].clone(),
        records[0].clone(),
        flocal::merge::FallbackReason::AbsentBase,
    );
    let plan = Plan {
        records: records.clone(),
        conflicts: vec![conflict],
        merges: Vec::new(),
    };
    let mut wire = Vec::new();
    sync::write_plan(&mut wire, &plan)?;
    let mut input = wire.as_slice();
    let mut observed_records = Vec::new();
    let mut observed_conflicts = Vec::new();
    loop {
        match sync::read_message(&mut input)? {
            Message::ApplyChunk {
                records, conflicts, ..
            } => {
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
