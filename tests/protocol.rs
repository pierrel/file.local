use std::fs;
use std::io::Write;
use std::process::{Command, Stdio};

use anyhow::Result;
use flocal::model::{
    Entry, ObjectHash, PeerId, Record, RelationshipId, RelativePath, ShareId, Version,
};
use flocal::reconcile::{Conflict, Plan};
use flocal::state::State;
use flocal::sync::{
    self, InitialMessage, Message, RegisterRelationshipResponse, RelationshipRequest,
    RemoveRelationshipResponse, V2Envelope, V2RoundFrame, V2SessionFrame,
};
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
fn explicit_v5_wire_format_and_initial_dispatch_are_versioned() -> Result<()> {
    let message = Message::Sync {
        protocol: sync::SYNC_PROTOCOL_VERSION,
        share: flocal::model::ShareId("share".into()),
        peer: PeerId("peer".into()),
        relationship: flocal::model::RelationshipId::parse("relationship".into())?,
        dry_run: false,
    };
    let mut wire = Vec::new();
    sync::write_message(&mut wire, &message)?;
    assert_eq!(
        &wire[4..],
        br#"{"type":"sync","protocol":5,"share":"share","peer":"peer","relationship":"relationship","dry_run":false}"#
    );
    assert!(matches!(
        sync::read_message(&mut wire.as_slice())?,
        Message::Sync { protocol: 5, .. }
    ));

    let initial = InitialMessage::Sync {
        protocol: sync::SYNC_PROTOCOL_VERSION,
        share: flocal::model::ShareId("share".into()),
        peer: PeerId("peer".into()),
        relationship: flocal::model::RelationshipId::parse("relationship".into())?,
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
fn relationship_wire_formats_are_exact_and_echo_every_identity() -> Result<()> {
    let register = RelationshipRequest::RegisterRelationship {
        registration_protocol: sync::RELATIONSHIP_REGISTRATION_PROTOCOL_VERSION,
        share: ShareId("share".into()),
        peer: PeerId("connector".into()),
        root: b"/root".to_vec(),
        relationship: RelationshipId("relationship".into()),
    };
    let mut wire = Vec::new();
    sync::write_relationship_request(&mut wire, &register)?;
    assert_eq!(
        &wire[4..],
        br#"{"type":"register_relationship","registration_protocol":1,"share":"share","peer":"connector","root":[47,114,111,111,116],"relationship":"relationship"}"#
    );
    assert_eq!(
        sync::read_relationship_request(&mut wire.as_slice())?,
        register
    );

    let remove = RelationshipRequest::RemoveRelationship {
        removal_protocol: sync::RELATIONSHIP_REMOVAL_PROTOCOL_VERSION,
        share: ShareId("share".into()),
        peer: PeerId("connector".into()),
        expected_peer: PeerId("responder".into()),
        relationship: RelationshipId("relationship".into()),
    };
    let mut wire = Vec::new();
    sync::write_relationship_request(&mut wire, &remove)?;
    assert_eq!(
        &wire[4..],
        br#"{"type":"remove_relationship","removal_protocol":1,"share":"share","peer":"connector","expected_peer":"responder","relationship":"relationship"}"#
    );
    assert_eq!(
        sync::read_relationship_request(&mut wire.as_slice())?,
        remove
    );

    let registered = RegisterRelationshipResponse::Registered {
        registration_protocol: sync::RELATIONSHIP_REGISTRATION_PROTOCOL_VERSION,
        share: ShareId("share".into()),
        peer: PeerId("responder".into()),
        relationship: RelationshipId("relationship".into()),
        prior_share: None,
    };
    let mut wire = Vec::new();
    sync::write_register_relationship_response(&mut wire, &registered)?;
    assert_eq!(
        &wire[4..],
        br#"{"type":"registered","registration_protocol":1,"share":"share","peer":"responder","relationship":"relationship"}"#
    );
    assert_eq!(
        sync::read_register_relationship_response(&mut wire.as_slice())?,
        registered
    );

    let remapped = RegisterRelationshipResponse::Registered {
        registration_protocol: sync::RELATIONSHIP_REGISTRATION_PROTOCOL_VERSION,
        share: ShareId("share".into()),
        peer: PeerId("responder".into()),
        relationship: RelationshipId("relationship".into()),
        prior_share: Some(ShareId("prior".into())),
    };
    let mut wire = Vec::new();
    sync::write_register_relationship_response(&mut wire, &remapped)?;
    assert_eq!(
        &wire[4..],
        br#"{"type":"registered","registration_protocol":1,"share":"share","peer":"responder","relationship":"relationship","prior_share":"prior"}"#
    );
    assert_eq!(
        sync::read_register_relationship_response(&mut wire.as_slice())?,
        remapped
    );

    let absent = RemoveRelationshipResponse::Absent {
        removal_protocol: sync::RELATIONSHIP_REMOVAL_PROTOCOL_VERSION,
        share: ShareId("share".into()),
        peer: PeerId("responder".into()),
        relationship: RelationshipId("relationship".into()),
    };
    let mut wire = Vec::new();
    sync::write_remove_relationship_response(&mut wire, &absent)?;
    assert_eq!(
        &wire[4..],
        br#"{"type":"absent","removal_protocol":1,"share":"share","peer":"responder","relationship":"relationship"}"#
    );
    assert_eq!(
        sync::read_remove_relationship_response(&mut wire.as_slice())?,
        absent
    );

    let register_error = RegisterRelationshipResponse::Error {
        message: "registration failed".into(),
    };
    let mut wire = Vec::new();
    sync::write_register_relationship_response(&mut wire, &register_error)?;
    assert_eq!(
        &wire[4..],
        br#"{"type":"error","message":"registration failed"}"#
    );
    assert_eq!(
        sync::read_register_relationship_response(&mut wire.as_slice())?,
        register_error
    );

    let remove_error = RemoveRelationshipResponse::Error {
        message: "removal failed".into(),
    };
    let mut wire = Vec::new();
    sync::write_remove_relationship_response(&mut wire, &remove_error)?;
    assert_eq!(
        &wire[4..],
        br#"{"type":"error","message":"removal failed"}"#
    );
    assert_eq!(
        sync::read_remove_relationship_response(&mut wire.as_slice())?,
        remove_error
    );
    Ok(())
}

#[test]
fn malformed_relationship_frame_does_not_open_or_migrate_state() -> Result<()> {
    let temp = tempdir()?;
    let state_dir = temp.path().join("state");
    let mut child = Command::new(env!("CARGO_BIN_EXE_flocal"))
        .args(["protocol", "relationship"])
        .env("FLOCAL_STATE_DIR", &state_dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?;
    let bytes = b"{}";
    let mut input = child.stdin.take().expect("relationship stdin");
    input.write_all(&(bytes.len() as u32).to_be_bytes())?;
    input.write_all(bytes)?;
    drop(input);

    let output = child.wait_with_output()?;
    assert!(!output.status.success());
    assert!(!state_dir.exists());
    Ok(())
}

#[test]
fn relationship_wire_semantic_limits_are_exact() -> Result<()> {
    let max_id = "a".repeat(sync::MAX_RELATIONSHIP_ID_BYTES);
    let too_long_id = "a".repeat(sync::MAX_RELATIONSHIP_ID_BYTES + 1);
    let max_root = {
        let mut root = vec![b'a'; sync::MAX_RELATIONSHIP_ROOT_BYTES];
        root[0] = b'/';
        root
    };
    let max_request = RelationshipRequest::RegisterRelationship {
        registration_protocol: sync::RELATIONSHIP_REGISTRATION_PROTOCOL_VERSION,
        share: ShareId(max_id.clone()),
        peer: PeerId(max_id.clone()),
        root: max_root.clone(),
        relationship: RelationshipId(max_id.clone()),
    };
    let mut max_wire = Vec::new();
    sync::write_relationship_request(&mut max_wire, &max_request)?;
    assert_eq!(
        sync::read_relationship_request(&mut max_wire.as_slice())?,
        max_request
    );

    let mut too_long_root = max_root;
    too_long_root.push(b'a');
    let too_long_root_request = RelationshipRequest::RegisterRelationship {
        registration_protocol: sync::RELATIONSHIP_REGISTRATION_PROTOCOL_VERSION,
        share: ShareId("share".into()),
        peer: PeerId("peer".into()),
        root: too_long_root,
        relationship: RelationshipId("relationship".into()),
    };
    assert!(sync::write_relationship_request(&mut Vec::new(), &too_long_root_request).is_err());
    let body = serde_json::to_vec(&too_long_root_request)?;
    let mut raw = (body.len() as u32).to_be_bytes().to_vec();
    raw.extend(body);
    assert!(sync::read_relationship_request(&mut raw.as_slice()).is_err());
    for invalid_root in [Vec::new(), b"relative".to_vec(), b"/nul\0root".to_vec()] {
        assert!(
            sync::write_relationship_request(
                &mut Vec::new(),
                &RelationshipRequest::RegisterRelationship {
                    registration_protocol: sync::RELATIONSHIP_REGISTRATION_PROTOCOL_VERSION,
                    share: ShareId("share".into()),
                    peer: PeerId("peer".into()),
                    root: invalid_root,
                    relationship: RelationshipId("relationship".into()),
                },
            )
            .is_err()
        );
    }

    let invalid_id_requests = [
        RelationshipRequest::RegisterRelationship {
            registration_protocol: sync::RELATIONSHIP_REGISTRATION_PROTOCOL_VERSION,
            share: ShareId(too_long_id.clone()),
            peer: PeerId("peer".into()),
            root: b"/root".to_vec(),
            relationship: RelationshipId("relationship".into()),
        },
        RelationshipRequest::RegisterRelationship {
            registration_protocol: sync::RELATIONSHIP_REGISTRATION_PROTOCOL_VERSION,
            share: ShareId("share".into()),
            peer: PeerId(too_long_id.clone()),
            root: b"/root".to_vec(),
            relationship: RelationshipId("relationship".into()),
        },
        RelationshipRequest::RegisterRelationship {
            registration_protocol: sync::RELATIONSHIP_REGISTRATION_PROTOCOL_VERSION,
            share: ShareId("share".into()),
            peer: PeerId("peer".into()),
            root: b"/root".to_vec(),
            relationship: RelationshipId(too_long_id.clone()),
        },
        RelationshipRequest::RemoveRelationship {
            removal_protocol: sync::RELATIONSHIP_REMOVAL_PROTOCOL_VERSION,
            share: ShareId("share".into()),
            peer: PeerId("peer".into()),
            expected_peer: PeerId(too_long_id.clone()),
            relationship: RelationshipId("relationship".into()),
        },
    ];
    for request in invalid_id_requests {
        let body = serde_json::to_vec(&request)?;
        let mut raw = (body.len() as u32).to_be_bytes().to_vec();
        raw.extend(body);
        assert!(sync::read_relationship_request(&mut raw.as_slice()).is_err());
        assert!(sync::write_relationship_request(&mut Vec::new(), &request).is_err());
    }
    let unsafe_id = RelationshipRequest::RemoveRelationship {
        removal_protocol: sync::RELATIONSHIP_REMOVAL_PROTOCOL_VERSION,
        share: ShareId("bad;share".into()),
        peer: PeerId("peer".into()),
        expected_peer: PeerId("responder".into()),
        relationship: RelationshipId("relationship".into()),
    };
    assert!(sync::write_relationship_request(&mut Vec::new(), &unsafe_id).is_err());

    let max_error = "é".repeat(sync::MAX_RELATIONSHIP_ERROR_BYTES / 2);
    let max_register_error = RegisterRelationshipResponse::Error {
        message: max_error.clone(),
    };
    let mut max_register_error_wire = Vec::new();
    sync::write_register_relationship_response(&mut max_register_error_wire, &max_register_error)?;
    assert_eq!(
        sync::read_register_relationship_response(&mut max_register_error_wire.as_slice())?,
        max_register_error
    );
    let max_remove_error = RemoveRelationshipResponse::Error { message: max_error };
    let mut max_remove_error_wire = Vec::new();
    sync::write_remove_relationship_response(&mut max_remove_error_wire, &max_remove_error)?;
    assert_eq!(
        sync::read_remove_relationship_response(&mut max_remove_error_wire.as_slice())?,
        max_remove_error
    );
    let too_long_error = "x".repeat(sync::MAX_RELATIONSHIP_ERROR_BYTES + 1);
    assert!(
        sync::write_register_relationship_response(
            &mut Vec::new(),
            &RegisterRelationshipResponse::Error {
                message: too_long_error.clone(),
            },
        )
        .is_err()
    );
    assert!(
        sync::write_remove_relationship_response(
            &mut Vec::new(),
            &RemoveRelationshipResponse::Error {
                message: too_long_error,
            },
        )
        .is_err()
    );
    let raw_error = RegisterRelationshipResponse::Error {
        message: "x".repeat(sync::MAX_RELATIONSHIP_ERROR_BYTES + 1),
    };
    let body = serde_json::to_vec(&raw_error)?;
    let mut raw = (body.len() as u32).to_be_bytes().to_vec();
    raw.extend(body);
    assert!(sync::read_register_relationship_response(&mut raw.as_slice()).is_err());

    for message in ["first\nsecond", "first\rsecond"] {
        let response = RegisterRelationshipResponse::Error {
            message: message.into(),
        };
        assert!(sync::write_register_relationship_response(&mut Vec::new(), &response).is_err());
        let body = serde_json::to_vec(&response)?;
        let mut raw = (body.len() as u32).to_be_bytes().to_vec();
        raw.extend(body);
        assert!(sync::read_register_relationship_response(&mut raw.as_slice()).is_err());
    }

    let invalid_prior = RegisterRelationshipResponse::Registered {
        registration_protocol: sync::RELATIONSHIP_REGISTRATION_PROTOCOL_VERSION,
        share: ShareId("share".into()),
        peer: PeerId("peer".into()),
        relationship: RelationshipId("relationship".into()),
        prior_share: Some(ShareId(too_long_id)),
    };
    assert!(sync::write_register_relationship_response(&mut Vec::new(), &invalid_prior).is_err());
    Ok(())
}

#[test]
fn relationship_decoders_reject_unknown_malformed_oversized_and_cross_protocol_frames() -> Result<()>
{
    fn framed(json: &[u8]) -> Vec<u8> {
        let mut wire = (json.len() as u32).to_be_bytes().to_vec();
        wire.extend_from_slice(json);
        wire
    }

    let unknown = framed(br#"{"type":"surprise"}"#);
    assert!(sync::read_relationship_request(&mut unknown.as_slice()).is_err());
    let extra = framed(
        br#"{"type":"remove_relationship","removal_protocol":1,"share":"share","peer":"peer","expected_peer":"responder","relationship":"relationship","extra":true}"#,
    );
    assert!(sync::read_relationship_request(&mut extra.as_slice()).is_err());
    let malformed = framed(
        br#"{"type":"remove_relationship","removal_protocol":1,"share":"share","peer":"peer","relationship":"relationship"}"#,
    );
    assert!(sync::read_relationship_request(&mut malformed.as_slice()).is_err());
    let unknown_response = framed(br#"{"type":"surprise"}"#);
    assert!(sync::read_register_relationship_response(&mut unknown_response.as_slice()).is_err());
    assert!(sync::read_remove_relationship_response(&mut unknown_response.as_slice()).is_err());
    let extra_error = framed(br#"{"type":"error","message":"failed","extra":true}"#);
    assert!(sync::read_register_relationship_response(&mut extra_error.as_slice()).is_err());
    assert!(sync::read_remove_relationship_response(&mut extra_error.as_slice()).is_err());
    let oversized = ((sync::MAX_FRAME as u32) + 1).to_be_bytes().to_vec();
    assert!(sync::read_relationship_request(&mut oversized.as_slice()).is_err());
    assert!(sync::read_register_relationship_response(&mut oversized.as_slice()).is_err());
    assert!(sync::read_remove_relationship_response(&mut oversized.as_slice()).is_err());

    let request = RelationshipRequest::RemoveRelationship {
        removal_protocol: sync::RELATIONSHIP_REMOVAL_PROTOCOL_VERSION,
        share: ShareId("share".into()),
        peer: PeerId("peer".into()),
        expected_peer: PeerId("responder".into()),
        relationship: RelationshipId("relationship".into()),
    };
    let mut relationship_wire = Vec::new();
    sync::write_relationship_request(&mut relationship_wire, &request)?;
    assert!(sync::read_message(&mut relationship_wire.as_slice()).is_err());
    assert!(sync::read_initial_message(&mut relationship_wire.as_slice()).is_err());
    assert!(sync::read_v2_envelope(&mut relationship_wire.as_slice()).is_err());

    let mut existing_wire = Vec::new();
    sync::write_message(&mut existing_wire, &Message::Done)?;
    assert!(sync::read_relationship_request(&mut existing_wire.as_slice()).is_err());

    let registered = RegisterRelationshipResponse::Registered {
        registration_protocol: sync::RELATIONSHIP_REGISTRATION_PROTOCOL_VERSION,
        share: ShareId("share".into()),
        peer: PeerId("peer".into()),
        relationship: RelationshipId("relationship".into()),
        prior_share: None,
    };
    let mut registered_wire = Vec::new();
    sync::write_register_relationship_response(&mut registered_wire, &registered)?;
    assert!(sync::read_remove_relationship_response(&mut registered_wire.as_slice()).is_err());
    let absent = RemoveRelationshipResponse::Absent {
        removal_protocol: sync::RELATIONSHIP_REMOVAL_PROTOCOL_VERSION,
        share: ShareId("share".into()),
        peer: PeerId("peer".into()),
        relationship: RelationshipId("relationship".into()),
    };
    let mut absent_wire = Vec::new();
    sync::write_remove_relationship_response(&mut absent_wire, &absent)?;
    assert!(sync::read_register_relationship_response(&mut absent_wire.as_slice()).is_err());
    Ok(())
}

#[test]
#[cfg(unix)]
fn relationship_deadline_codecs_round_trip_each_closed_frame_family() -> Result<()> {
    use std::os::unix::net::UnixStream;
    use std::time::{Duration, Instant};

    let request = RelationshipRequest::RemoveRelationship {
        removal_protocol: sync::RELATIONSHIP_REMOVAL_PROTOCOL_VERSION,
        share: ShareId("share".into()),
        peer: PeerId("peer".into()),
        expected_peer: PeerId("responder".into()),
        relationship: RelationshipId("relationship".into()),
    };
    let registered = RegisterRelationshipResponse::Registered {
        registration_protocol: sync::RELATIONSHIP_REGISTRATION_PROTOCOL_VERSION,
        share: ShareId("share".into()),
        peer: PeerId("responder".into()),
        relationship: RelationshipId("relationship".into()),
        prior_share: None,
    };
    let absent = RemoveRelationshipResponse::Absent {
        removal_protocol: sync::RELATIONSHIP_REMOVAL_PROTOCOL_VERSION,
        share: ShareId("share".into()),
        peer: PeerId("responder".into()),
        relationship: RelationshipId("relationship".into()),
    };
    let deadline = || Instant::now() + Duration::from_secs(1);
    let (writer, reader) = UnixStream::pair()?;
    sync::write_relationship_request_until(&writer, &request, deadline())?;
    sync::write_register_relationship_response_until(&writer, &registered, deadline())?;
    sync::write_remove_relationship_response_until(&writer, &absent, deadline())?;
    assert_eq!(
        sync::read_relationship_request_until(&reader, deadline())?,
        request
    );
    assert_eq!(
        sync::read_register_relationship_response_until(&reader, deadline())?,
        registered
    );
    assert_eq!(
        sync::read_remove_relationship_response_until(&reader, deadline())?,
        absent
    );
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

    let reservation = sync::Reservation {
        id: sync::SchedulingId::generate(),
        network_order: sync::NetworkOrder::new(1)?,
        nonce: sync::SchedulingNonce::generate(),
    };
    let round_frames = vec![
        V2RoundFrame::SyncStart(sync::SyncStart {
            id: reservation.id.clone(),
            network_order: reservation.network_order,
            nonce: reservation.nonce.clone(),
            connector_generation: 7,
            responder_generation: 8,
        }),
        V2RoundFrame::SyncAccepted(reservation),
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
        relationship: flocal::model::RelationshipId::parse("relationship".into())?,
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
