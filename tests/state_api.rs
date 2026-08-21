use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use flocal::model::{
    Entry, ObjectHash, PeerConfig, PeerId, Record, RelationshipId, RelativePath, ShareId, Version,
};
use flocal::reconcile::Conflict;
#[cfg(feature = "e2e-test-hooks")]
use flocal::state::RecoveryLimitKind;
use flocal::state::{
    DEFAULT_RECOVERY_BUDGET_BYTES, InstallTempPhase, RecoveryLimitExceeded, State,
};
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
            id_authenticator: None,
            timestamp_ns: sequence as i64,
            seen: Vec::new(),
            merge_base: None,
            version_authenticator: None,
            base_authenticator: None,
            entry,
        },
    })
}

#[test]
fn version_authentication_rejects_forged_local_history() -> Result<()> {
    let temp = tempdir()?;
    let root = temp.path().join("root");
    fs::create_dir(&root)?;
    let state = State::open(temp.path().join("state"))?;
    let share = state.init_share(&root)?;
    let local = state.peer_id()?;
    let mut record = record(b"file", &local.0, 1, Entry::Directory)?;
    state.authenticate_record(&share, &mut record)?;
    state.validate_remote_records(&share, &[], std::slice::from_ref(&record))?;

    let mut replayed = record.clone();
    replayed.path = RelativePath::from_bytes(b"replayed".to_vec())?;
    assert!(
        state
            .validate_remote_records(&share, &[record.clone()], &[replayed])
            .is_err()
    );

    let mut forged_base_tag = record.clone();
    forged_base_tag.version.base_authenticator = Some("forged".into());
    assert!(
        state
            .validate_remote_records(&share, &[record.clone()], &[forged_base_tag])
            .is_err()
    );

    let mut forged = record.clone();
    forged.version.entry = Entry::Tombstone;
    assert!(
        state
            .validate_remote_records(&share, &[record], &[forged])
            .is_err()
    );
    Ok(())
}

#[test]
fn object_provenance_is_share_scoped_and_round_cleanup_is_serialized() -> Result<()> {
    let temp = tempdir()?;
    let first_root = temp.path().join("first");
    let second_root = temp.path().join("second");
    let state_path = temp.path().join("state");
    fs::create_dir(&first_root)?;
    fs::create_dir(&second_root)?;
    let bytes = b"private to first share";
    let hash = ObjectHash::from_blake3(blake3::hash(bytes));
    let second_share;
    {
        let mut state = State::open(&state_path)?;
        let first_share = state.init_share(&first_root)?;
        second_share = state.init_share(&second_root)?;
        state.import_object(&hash, bytes)?;
        let first = record(
            b"file",
            "first",
            1,
            Entry::File {
                hash: hash.clone(),
                size: bytes.len() as u64,
                executable: false,
            },
        )?;
        state.replace_records(&first_share, std::slice::from_ref(&first))?;
        assert_eq!(
            flocal::sync::required_hashes_for_share(&state, &second_share, &[first])?,
            vec![hash.clone()]
        );
        state.mark_object_receiving(&second_share, &hash)?;
    }
    let state = State::open(&state_path)?;
    assert!(
        state.object_path(&hash).exists(),
        "first share still owns the object"
    );

    let orphan = ObjectHash::from_blake3(blake3::hash(b"orphan"));
    state.mark_object_receiving(&second_share, &orphan)?;
    let mut sink = state.begin_object(orphan.clone(), 6)?;
    sink.write_chunk(b"orphan")?;
    sink.finish()?;
    drop(state);
    let mut state = State::open(&state_path)?;
    assert!(
        state.object_path(&orphan).exists(),
        "opening state must not erase another process's live transfer root"
    );
    let mut request = state.enqueue_sync(None, flocal::state::SyncOperation::Maintenance, None)?;
    let permit = request
        .try_activate()?
        .context("maintenance request did not activate")?;
    state.clear_pending_objects(&second_share)?;
    state.prune_unreferenced_objects()?;
    permit.finish()?;
    assert!(!state.object_path(&orphan).exists());
    Ok(())
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
    let mut request = state.enqueue_sync(None, flocal::state::SyncOperation::Maintenance, None)?;
    request
        .try_activate()?
        .context("maintenance request did not activate")?
        .finish()?;
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
        peer_id: Some(PeerId("remote".into())),
        relationship: None,
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
    state.register_relationship(
        &share,
        &root,
        &bound,
        &RelationshipId::parse("relationship-state-api".into())?,
    )?;
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
    let conflict = Conflict::whole_file(winner, loser, flocal::merge::FallbackReason::AbsentBase);
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
fn recovery_usage_prune_and_budget_are_durable_and_reference_safe() -> Result<()> {
    let temp = tempdir()?;
    let root = temp.path().join("root");
    let state_dir = temp.path().join("state");
    fs::create_dir_all(&root)?;
    let mut state = State::open(&state_dir)?;
    let share = state.init_share(&root)?;
    let bytes = b"recover me";
    let hash = ObjectHash::from_blake3(blake3::hash(bytes));
    state.import_object(&hash, bytes)?;
    let first = Conflict::whole_file(
        record(
            b"first",
            "one",
            2,
            Entry::File {
                hash: hash.clone(),
                size: bytes.len() as u64,
                executable: false,
            },
        )?,
        record(b"first", "two", 1, Entry::Tombstone)?,
        flocal::merge::FallbackReason::AbsentBase,
    );
    state.add_conflicts(&share, std::slice::from_ref(&first))?;
    let usage = state.recovery_usage(&share)?;
    assert_eq!(usage.conflicts, 1);
    assert_eq!(usage.object_bytes, bytes.len() as u64);
    assert!(usage.metadata_bytes > 256);
    assert_eq!(usage.used_bytes, usage.object_bytes + usage.metadata_bytes);
    assert_eq!(usage.reclaimable_bytes, bytes.len() as u64);

    let all_preview = state.recovery_prune_plan(&share, &[])?;
    let second = Conflict::whole_file(
        record(
            b"second",
            "one",
            4,
            Entry::File {
                hash: hash.clone(),
                size: bytes.len() as u64,
                executable: false,
            },
        )?,
        record(b"second", "two", 3, Entry::Tombstone)?,
        flocal::merge::FallbackReason::AbsentBase,
    );
    state.add_conflicts(&share, std::slice::from_ref(&second))?;
    #[cfg(feature = "e2e-test-hooks")]
    {
        fs::write(state_dir.join(".e2e-recovery-temp-fail-after"), b"1")?;
        assert!(state.recovery_prune_plan(&share, &[]).is_err());
        fs::remove_file(state_dir.join(".e2e-recovery-temp-fail-after"))?;
        assert_eq!(state.conflicts(&share)?.len(), 2);
    }
    assert!(
        state
            .prune_recovery(&share, &[], &all_preview.selection_token)
            .is_err()
    );

    let first_id = flocal::reconcile::conflict_id(&first);
    let selected = state.recovery_prune_plan(&share, std::slice::from_ref(&first_id))?;
    assert_eq!(selected.reclaimable_bytes, 0);
    state.prune_recovery(
        &share,
        std::slice::from_ref(&first_id),
        &selected.selection_token,
    )?;
    assert!(state.object_path(&hash).exists());
    assert_eq!(state.recovery_usage(&share)?.conflicts, 1);

    let remaining = state.recovery_prune_plan(&share, &[])?;
    assert_eq!(remaining.reclaimable_bytes, bytes.len() as u64);
    state.prune_recovery(&share, &[], &remaining.selection_token)?;
    assert!(!state.object_path(&hash).exists());
    assert_eq!(state.recovery_usage(&share)?.conflicts, 0);

    let raised = DEFAULT_RECOVERY_BUDGET_BYTES + 1024;
    assert_eq!(
        state.raise_recovery_budget(&share, raised)?,
        DEFAULT_RECOVERY_BUDGET_BYTES
    );
    assert!(state.raise_recovery_budget(&share, raised).is_err());
    drop(state);
    assert_eq!(State::open(&state_dir)?.recovery_budget(&share)?, raised);
    Ok(())
}

#[test]
fn recovery_reclaimability_respects_every_durable_object_root() -> Result<()> {
    let temp = tempdir()?;
    let first_root = temp.path().join("first");
    let second_root = temp.path().join("second");
    fs::create_dir_all(&first_root)?;
    fs::create_dir_all(&second_root)?;
    let mut state = State::open(temp.path().join("state"))?;
    let first_share = state.init_share(&first_root)?;
    let second_share = state.init_share(&second_root)?;
    let bytes = b"shared recovery object";
    let hash = ObjectHash::from_blake3(blake3::hash(bytes));
    state.import_object(&hash, bytes)?;
    let file = |path: &[u8], peer: &str, sequence: u64| {
        record(
            path,
            peer,
            sequence,
            Entry::File {
                hash: hash.clone(),
                size: bytes.len() as u64,
                executable: false,
            },
        )
    };
    let conflict = Conflict::whole_file(
        file(b"conflict", "winner", 2)?,
        record(b"conflict", "loser", 1, Entry::Tombstone)?,
        flocal::merge::FallbackReason::AbsentBase,
    );
    state.add_conflicts(&first_share, std::slice::from_ref(&conflict))?;
    let id = flocal::reconcile::conflict_id(&conflict);
    let reclaimable = |state: &mut State| -> Result<(u64, u64)> {
        Ok((
            state.recovery_usage(&first_share)?.reclaimable_bytes,
            state
                .recovery_prune_plan(&first_share, std::slice::from_ref(&id))?
                .reclaimable_bytes,
        ))
    };

    let current = file(b"current", "current", 3)?;
    state.replace_records(&second_share, std::slice::from_ref(&current))?;
    assert_eq!(reclaimable(&mut state)?, (0, 0));

    let mut with_base = record(b"with-base", "current", 4, Entry::Tombstone)?;
    with_base.version.merge_base = current.version.as_base();
    state.replace_records(&second_share, std::slice::from_ref(&with_base))?;
    assert_eq!(reclaimable(&mut state)?, (0, 0));

    state.replace_records(&second_share, std::slice::from_ref(&current))?;
    state.acknowledge_shared_heads(&second_share, std::slice::from_ref(&current))?;
    state.replace_records(&second_share, &[])?;
    assert_eq!(reclaimable(&mut state)?, (0, 0));
    state.acknowledge_shared_heads(&second_share, &[])?;

    state.set_plan_install_intent(
        &second_share,
        std::slice::from_ref(&current),
        std::slice::from_ref(&conflict),
    )?;
    assert_eq!(reclaimable(&mut state)?, (0, 0));
    state.clear_install_intent(&second_share)?;

    state.mark_object_receiving(&second_share, &hash)?;
    assert_eq!(reclaimable(&mut state)?, (0, 0));
    state.clear_pending_objects(&second_share)?;
    assert_eq!(
        reclaimable(&mut state)?,
        (bytes.len() as u64, bytes.len() as u64)
    );
    Ok(())
}

#[test]
fn recovery_projection_rejects_oversized_conflicts_before_insertion() -> Result<()> {
    let temp = tempdir()?;
    let root = temp.path().join("root");
    fs::create_dir_all(&root)?;
    let mut state = State::open(temp.path().join("state"))?;
    let share = state.init_share(&root)?;
    let hash = ObjectHash::from_blake3(blake3::hash(b"declared"));
    let conflict = Conflict::whole_file(
        record(
            b"large",
            "one",
            2,
            Entry::File {
                hash,
                size: DEFAULT_RECOVERY_BUDGET_BYTES,
                executable: false,
            },
        )?,
        record(b"large", "two", 1, Entry::Tombstone)?,
        flocal::merge::FallbackReason::AbsentBase,
    );
    let error = state
        .add_conflicts(&share, &[conflict])
        .expect_err("metadata must put the projected charge over budget");
    assert!(error.downcast_ref::<RecoveryLimitExceeded>().is_some());
    assert!(state.conflicts(&share)?.is_empty());
    Ok(())
}

#[test]
#[cfg(feature = "e2e-test-hooks")]
fn cumulative_count_and_metadata_limits_survive_restart() -> Result<()> {
    let temp = tempdir()?;
    let root = temp.path().join("root");
    let state_dir = temp.path().join("state");
    fs::create_dir(&root)?;
    let mut state = State::open(&state_dir)?;
    let share = state.init_share(&root)?;
    let make_conflict = |path: &[u8], sequence: u64| -> Result<Conflict> {
        Ok(Conflict::whole_file(
            record(path, "winner", sequence, Entry::Directory)?,
            record(path, "loser", sequence - 1, Entry::Tombstone)?,
            flocal::merge::FallbackReason::AbsentBase,
        ))
    };
    let first = make_conflict(b"first", 2)?;
    let second = make_conflict(b"second", 4)?;
    fs::write(state_dir.join(".e2e-recovery-conflict-limit"), b"1")?;
    state.add_conflicts(&share, std::slice::from_ref(&first))?;
    drop(state);

    let mut state = State::open(&state_dir)?;
    let count_error = state
        .add_conflicts(&share, std::slice::from_ref(&second))
        .expect_err("the cumulative conflict cap must survive restart");
    assert_eq!(
        count_error
            .downcast_ref::<RecoveryLimitExceeded>()
            .unwrap()
            .kind,
        RecoveryLimitKind::ConflictCount
    );

    fs::remove_file(state_dir.join(".e2e-recovery-conflict-limit"))?;
    let current_metadata = state.recovery_usage(&share)?.metadata_bytes;
    fs::write(
        state_dir.join(".e2e-recovery-metadata-limit"),
        (current_metadata + 1).to_string(),
    )?;
    let metadata_error = state
        .add_conflicts(&share, &[second])
        .expect_err("the cumulative metadata cap must reject the next row");
    assert_eq!(
        metadata_error
            .downcast_ref::<RecoveryLimitExceeded>()
            .unwrap()
            .kind,
        RecoveryLimitKind::MetadataBytes
    );
    assert_eq!(state.conflicts(&share)?.len(), 1);
    Ok(())
}

#[test]
fn object_sink_and_collector_remove_unpublished_temporaries() -> Result<()> {
    let temp = tempdir()?;
    let state_dir = temp.path().join("state");
    let state = State::open(&state_dir)?;
    let expected = ObjectHash::from_blake3(blake3::hash(b"expected"));
    assert!(state.import_object(&expected, b"different").is_err());
    state.import_object(&expected, b"expected")?;
    #[cfg(feature = "e2e-test-hooks")]
    {
        fs::write(state_dir.join(".e2e-object-enospc"), b"1")?;
        state.import_object(&expected, b"expected")?;
        let source = state_dir.join("source");
        fs::write(&source, b"expected")?;
        assert_eq!(state.store_object(fs::File::open(source)?)?.0, expected);
        fs::remove_file(state_dir.join(".e2e-object-enospc"))?;
    }
    for entry in fs::read_dir(state_dir.join("objects"))? {
        assert!(!entry?.file_name().to_string_lossy().starts_with(".tmp-"));
    }

    let held_hash = ObjectHash::from_blake3(blake3::hash(b"held"));
    let held = state.begin_object(held_hash, 4)?;
    let open_path = state_dir.clone();
    let (opened, result) = std::sync::mpsc::channel();
    let opener = std::thread::spawn(move || {
        opened.send(State::open(open_path).map(|_| ())).ok();
    });
    let opened = result
        .recv_timeout(std::time::Duration::from_secs(1))
        .context("State::open blocked behind a live object transfer")?;
    opened?;
    drop(held);
    opener.join().expect("state opener joins");

    drop(state);
    fs::write(state_dir.join("objects/.tmp-crash"), b"partial")?;
    let state = State::open(&state_dir)?;
    assert!(state_dir.join("objects/.tmp-crash").exists());
    state.prune_unreferenced_objects()?;
    assert!(!state_dir.join("objects/.tmp-crash").exists());
    Ok(())
}

#[test]
#[cfg(feature = "e2e-test-hooks")]
fn invalidation_recovery_obeys_recovery_admission() -> Result<()> {
    let temp = tempdir()?;
    let root = temp.path().join("root");
    let state_dir = temp.path().join("state");
    fs::create_dir(&root)?;
    let mut state = State::open(&state_dir)?;
    let share = state.init_share(&root)?;
    let (intent, _) = state.set_install_intent(&share, &[])?;
    let hash = ObjectHash::from_blake3(blake3::hash(b"large"));
    let conflict = Conflict::whole_file(
        record(
            b"race",
            "winner",
            2,
            Entry::File {
                hash,
                size: DEFAULT_RECOVERY_BUDGET_BYTES,
                executable: false,
            },
        )?,
        record(b"race", "loser", 1, Entry::Tombstone)?,
        flocal::merge::FallbackReason::AbsentBase,
    );
    fs::write(state_dir.join(".e2e-recovery-budget-bytes"), b"1")?;
    let unsettled = RelativePath::from_bytes(b"race".to_vec())?;
    let error = state
        .retire_invalidated_install(&share, &intent, &[], &[conflict], &unsettled)
        .expect_err("invalidation recovery must obey the same admission gate");
    assert!(error.downcast_ref::<RecoveryLimitExceeded>().is_some());
    assert!(state.install_intent(&share)?.is_some());
    assert!(state.conflicts(&share)?.is_empty());
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
    let relationship = RelationshipId::parse("relationship-one".into())?;
    state.register_relationship(&unbound, &second, &PeerId("peer-one".into()), &relationship)?;
    assert!(
        state
            .register_relationship(&unbound, &second, &PeerId("peer-two".into()), &relationship,)
            .is_err()
    );
    assert!(
        state
            .register_relationship(&unbound, &first, &PeerId("peer-one".into()), &relationship,)
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

#[cfg(unix)]
#[test]
fn private_managed_state_marker_selects_an_absolute_state_directory() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    const HOME: &str = "FLOCAL_TEST_MANAGED_STATE_MARKER_HOME";
    if let Some(home) = std::env::var_os(HOME) {
        let home = std::path::PathBuf::from(home);
        let state_dir = home.parent().expect("temporary parent").join("state");
        assert_eq!(State::managed_state_dir()?, Some(state_dir));
        return Ok(());
    }

    let temp = tempdir()?;
    let home = temp.path().join("home");
    let state_dir = temp.path().join("state");
    let marker = home.join(".config/file.local/managed-state");
    fs::create_dir_all(marker.parent().expect("marker parent"))?;
    fs::write(&marker, format!("{}\n", state_dir.display()))?;
    fs::set_permissions(&marker, fs::Permissions::from_mode(0o600))?;
    let output = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "private_managed_state_marker_selects_an_absolute_state_directory",
        ])
        .env("HOME", &home)
        .env(HOME, &home)
        .output()?;
    assert!(output.status.success(), "{:?}", output);
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
fn managed_share_state_changes_are_durable_and_generation_guarded() -> Result<()> {
    let temporary = tempdir()?;
    let root = temporary.path().join("root");
    fs::create_dir(&root)?;
    let mut state = State::open(temporary.path().join("state"))?;
    let share = state.init_share(&root)?;
    let initial = state.watch_intent_generation(&share)?;
    state.set_watch_enabled_if_generation(&share, true, initial)?;
    assert!(state.managed_share(&share)?.watch_enabled);
    assert!(
        state
            .set_watch_enabled_if_generation(&share, false, initial)
            .is_err()
    );
    let current = state.watch_intent_generation(&share)?;
    state.set_initial_complete_and_watch_enabled(&share, current)?;
    let managed = state.managed_share(&share)?;
    assert!(managed.initial_complete);
    assert!(managed.watch_enabled);
    state.set_blocked(&share, &"x".repeat(5000))?;
    assert_eq!(
        state
            .managed_share(&share)?
            .blocked_diagnostic
            .as_deref()
            .unwrap()
            .len(),
        4096
    );
    state.clear_blocked(&share)?;
    assert!(state.managed_share(&share)?.blocked_diagnostic.is_none());
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
