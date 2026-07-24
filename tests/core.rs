use std::fs;

use anyhow::Result;
use flocal::model::Entry;
use flocal::scan::scan;
use flocal::state::State;
use flocal::sync::{ShareRoot, apply_plan, plan, refresh, refresh_with_root};
use tempfile::tempdir;

#[test]
fn scan_apply_and_delete_round_trip() -> Result<()> {
    let temp = tempdir()?;
    let source = temp.path().join("source");
    let target = temp.path().join("target");
    fs::create_dir_all(&source)?;
    fs::create_dir_all(&target)?;
    fs::write(source.join("hello.txt"), "hello")?;

    let mut source_state = State::open(temp.path().join("source-state"))?;
    let source_share = source_state.init_share(&source)?;
    let first = scan(&source_state, &source_share, &source, &[])?;
    source_state.replace_records(&source_share, &first)?;

    let mut target_state = State::open(temp.path().join("target-state"))?;
    target_state.register_share(&source_share, &target)?;
    for record in &first {
        if let Entry::File { hash, .. } = &record.version.entry {
            target_state.import_object(hash, &source_state.read_object(hash)?)?;
        }
    }
    apply_plan(&mut target_state, &source_share, &first)?;
    assert_eq!(fs::read_to_string(target.join("hello.txt"))?, "hello");

    fs::remove_file(source.join("hello.txt"))?;
    let second = scan(&source_state, &source_share, &source, &first)?;
    assert!(
        second
            .iter()
            .any(|r| matches!(r.version.entry, Entry::Tombstone))
    );
    apply_plan(&mut target_state, &source_share, &second)?;
    assert!(!target.join("hello.txt").exists());
    Ok(())
}

#[test]
fn held_share_root_rejects_path_replacement_without_tombstones() -> Result<()> {
    let temp = tempdir()?;
    let root = temp.path().join("root");
    let held_tree = temp.path().join("held-tree");
    fs::create_dir(&root)?;
    fs::write(root.join("kept.txt"), "kept")?;
    let mut state = State::open(temp.path().join("state"))?;
    let share = state.init_share(&root)?;
    refresh(&mut state, &share)?;
    let capability = ShareRoot::open(&state, &share)?;

    fs::rename(&root, &held_tree)?;
    fs::create_dir(&root)?;
    let before = state.records(&share)?;
    let error = refresh_with_root(&mut state, &share, &capability)
        .expect_err("replacement path must invalidate the held session");
    assert!(error.to_string().contains("root identity changed"));
    assert_eq!(state.records(&share)?, before);
    assert!(
        state
            .records(&share)?
            .iter()
            .all(|record| !matches!(record.version.entry, Entry::Tombstone))
    );
    assert!(fs::read_dir(&root)?.next().is_none());
    assert_eq!(fs::read_to_string(held_tree.join("kept.txt"))?, "kept");
    Ok(())
}

#[test]
fn ignored_existing_file_is_not_tombstoned() -> Result<()> {
    let temp = tempdir()?;
    let root = temp.path().join("root");
    fs::create_dir_all(&root)?;
    fs::write(root.join("local.env"), "secret")?;
    let mut state = State::open(temp.path().join("state"))?;
    let share = state.init_share(&root)?;
    let first = refresh(&mut state, &share)?;
    assert!(first.iter().any(|r| r.path.display() == "local.env"));
    fs::write(root.join(".gitignore"), "local.env\n")?;
    let advertised = refresh(&mut state, &share)?;
    assert!(!advertised.iter().any(|r| r.path.display() == "local.env"));
    let persisted = state.records(&share)?;
    let record = persisted
        .iter()
        .find(|r| r.path.display() == "local.env")
        .unwrap();
    assert!(!matches!(record.version.entry, Entry::Tombstone));
    Ok(())
}

#[test]
fn ignored_unsynchronized_collision_blocks_apply() -> Result<()> {
    let temp = tempdir()?;
    let root = temp.path().join("root");
    fs::create_dir_all(&root)?;
    fs::write(root.join(".gitignore"), "local.env\n")?;
    fs::write(root.join("local.env"), "local secret")?;
    let mut state = State::open(temp.path().join("state"))?;
    let share = state.init_share(&root)?;
    let remote = flocal::model::Record {
        path: flocal::model::RelativePath::from_bytes(b"local.env".to_vec())?,
        version: flocal::model::Version {
            peer: flocal::model::PeerId("remote".into()),
            sequence: 1,
            timestamp_ns: 1,
            seen: Vec::new(),
            entry: Entry::Tombstone,
        },
    };

    let error = apply_plan(&mut state, &share, &[remote]).expect_err("collision must block");
    assert!(error.to_string().contains("ignored local content"));
    assert_eq!(fs::read_to_string(root.join("local.env"))?, "local secret");
    assert!(state.install_intent(&share)?.is_none());
    Ok(())
}

#[test]
fn directory_only_ignore_protects_against_tombstones() -> Result<()> {
    let temp = tempdir()?;
    let root = temp.path().join("root");
    fs::create_dir_all(root.join("only-dir"))?;
    let mut state = State::open(temp.path().join("state"))?;
    let share = state.init_share(&root)?;
    let first = refresh(&mut state, &share)?;
    let mut tombstone = first
        .iter()
        .find(|record| record.path.display() == "only-dir")
        .unwrap()
        .clone();
    tombstone.version.entry = Entry::Tombstone;
    tombstone.version.sequence += 1;
    fs::write(root.join(".gitignore"), "only-dir/\n")?;

    apply_plan(&mut state, &share, &[tombstone])?;
    assert!(root.join("only-dir").is_dir());
    assert!(state.records(&share)?.iter().any(|record| {
        record.path.display() == "only-dir" && matches!(record.version.entry, Entry::Directory)
    }));
    Ok(())
}

#[test]
fn synchronizes_symlinks_and_executable_mode_changes() -> Result<()> {
    use std::os::unix::fs::{PermissionsExt, symlink};

    let temp = tempdir()?;
    let source = temp.path().join("source");
    let target = temp.path().join("target");
    fs::create_dir_all(&source)?;
    fs::create_dir_all(&target)?;
    fs::write(source.join("script"), "first")?;
    fs::set_permissions(source.join("script"), fs::Permissions::from_mode(0o755))?;
    symlink("script", source.join("link"))?;
    let mut source_state = State::open(temp.path().join("source-state"))?;
    let share = source_state.init_share(&source)?;
    let mut target_state = State::open(temp.path().join("target-state"))?;
    target_state.register_share(&share, &target)?;
    let first = scan(&source_state, &share, &source, &[])?;
    copy_objects(&source_state, &target_state, &first)?;
    apply_plan(&mut target_state, &share, &first)?;
    assert_eq!(
        fs::read_link(target.join("link"))?,
        std::path::Path::new("script")
    );
    assert_ne!(
        fs::metadata(target.join("script"))?.permissions().mode() & 0o111,
        0
    );

    source_state.replace_records(&share, &first)?;
    fs::write(source.join("script"), "second")?;
    fs::set_permissions(source.join("script"), fs::Permissions::from_mode(0o644))?;
    fs::remove_file(source.join("link"))?;
    let second = scan(&source_state, &share, &source, &first)?;
    copy_objects(&source_state, &target_state, &second)?;
    apply_plan(&mut target_state, &share, &second)?;
    assert_eq!(
        fs::metadata(target.join("script"))?.permissions().mode() & 0o111,
        0
    );
    assert!(!target.join("link").exists());
    Ok(())
}

#[test]
fn preview_reports_deletion_without_advancing_persisted_state() -> Result<()> {
    let temp = tempdir()?;
    let root = temp.path().join("root");
    fs::create_dir_all(&root)?;
    fs::write(root.join("file"), "value")?;
    let _socket = std::os::unix::net::UnixListener::bind(root.join("socket"))?;
    let mut state = State::open(temp.path().join("state"))?;
    let share = state.init_share(&root)?;
    let first = refresh(&mut state, &share)?;
    fs::remove_file(root.join("file"))?;
    let previewed = flocal::scan::preview(&state, &share, &root, &first)?;
    assert!(
        previewed
            .iter()
            .any(|record| matches!(record.version.entry, Entry::Tombstone))
    );
    assert_eq!(state.records(&share)?, first);
    Ok(())
}

#[test]
fn applies_nested_deletions_and_file_directory_transitions() -> Result<()> {
    let temp = tempdir()?;
    let source = temp.path().join("source");
    let target = temp.path().join("target");
    fs::create_dir_all(source.join("node"))?;
    fs::create_dir_all(&target)?;
    fs::write(source.join("node/child"), "child")?;
    let mut source_state = State::open(temp.path().join("source-state"))?;
    let share = source_state.init_share(&source)?;
    let mut target_state = State::open(temp.path().join("target-state"))?;
    target_state.register_share(&share, &target)?;

    let first = scan(&source_state, &share, &source, &[])?;
    copy_objects(&source_state, &target_state, &first)?;
    apply_plan(&mut target_state, &share, &first)?;
    source_state.replace_records(&share, &first)?;

    fs::remove_dir_all(source.join("node"))?;
    fs::write(source.join("node"), "now a file")?;
    let second = scan(&source_state, &share, &source, &first)?;
    copy_objects(&source_state, &target_state, &second)?;
    apply_plan(&mut target_state, &share, &second)?;
    assert_eq!(fs::read_to_string(target.join("node"))?, "now a file");
    source_state.replace_records(&share, &second)?;

    fs::remove_file(source.join("node"))?;
    fs::create_dir(source.join("node"))?;
    fs::write(source.join("node/child"), "again")?;
    let third = scan(&source_state, &share, &source, &second)?;
    copy_objects(&source_state, &target_state, &third)?;
    apply_plan(&mut target_state, &share, &third)?;
    assert_eq!(fs::read_to_string(target.join("node/child"))?, "again");
    Ok(())
}

#[test]
fn offline_deletion_survives_scan_cycles_until_peer_returns() -> Result<()> {
    let temp = tempdir()?;
    let a_root = temp.path().join("a");
    let b_root = temp.path().join("b");
    fs::create_dir_all(a_root.join("dir"))?;
    fs::create_dir_all(&b_root)?;
    fs::write(a_root.join("dir/child"), "content")?;

    let mut a = State::open(temp.path().join("a-state"))?;
    let share = a.init_share(&a_root)?;
    let mut b = State::open(temp.path().join("b-state"))?;
    b.register_share(&share, &b_root)?;

    // Initial sync with both peers reachable, in run_sync's order.
    let initial = plan(&refresh(&mut a, &share)?, &refresh(&mut b, &share)?);
    copy_objects(&a, &b, &initial.records)?;
    apply_plan(&mut b, &share, &initial.records)?;
    apply_plan(&mut a, &share, &initial.records)?;
    assert_eq!(fs::read_to_string(b_root.join("dir/child"))?, "content");

    // A deletes the directory; the peer stays unreachable across several
    // watch cycles, each of which persists a refresh before failing to
    // connect.
    fs::remove_dir_all(a_root.join("dir"))?;
    for _ in 0..3 {
        let advertised = refresh(&mut a, &share)?;
        assert!(
            advertised
                .iter()
                .any(|record| matches!(record.version.entry, Entry::Tombstone)),
            "the tombstone must stay advertised while the peer is unreachable"
        );
    }

    // The peer returns: the deletion must propagate, not resurrect.
    let reunion = plan(&refresh(&mut a, &share)?, &refresh(&mut b, &share)?);
    apply_plan(&mut a, &share, &reunion.records)?;
    apply_plan(&mut b, &share, &reunion.records)?;
    assert!(!a_root.join("dir").exists());
    assert!(!b_root.join("dir").exists());

    // A steady-state round replays the retained tombstones through
    // apply_plan; it must not recreate any part of the deleted tree on
    // either peer.
    let steady = plan(&refresh(&mut a, &share)?, &refresh(&mut b, &share)?);
    apply_plan(&mut a, &share, &steady.records)?;
    apply_plan(&mut b, &share, &steady.records)?;
    assert!(!a_root.join("dir").exists());
    assert!(!b_root.join("dir").exists());

    // A recreated path supersedes the retained tombstones on both peers.
    fs::create_dir_all(a_root.join("dir"))?;
    fs::write(a_root.join("dir/child"), "again")?;
    let recreated = plan(&refresh(&mut a, &share)?, &refresh(&mut b, &share)?);
    copy_objects(&a, &b, &recreated.records)?;
    apply_plan(&mut b, &share, &recreated.records)?;
    apply_plan(&mut a, &share, &recreated.records)?;
    assert_eq!(fs::read_to_string(b_root.join("dir/child"))?, "again");
    Ok(())
}

#[test]
fn foreign_tombstone_for_unknown_path_is_not_persisted_or_applied() -> Result<()> {
    let temp = tempdir()?;
    let root = temp.path().join("root");
    fs::create_dir_all(&root)?;
    let mut state = State::open(temp.path().join("state"))?;
    let share = state.init_share(&root)?;
    let foreign = flocal::model::Record {
        path: flocal::model::RelativePath::from_bytes(b"ghost/child".to_vec())?,
        version: flocal::model::Version {
            peer: flocal::model::PeerId("peer-foreign".into()),
            sequence: 1,
            timestamp_ns: 1,
            seen: Vec::new(),
            entry: Entry::Tombstone,
        },
    };

    apply_plan(&mut state, &share, &[foreign])?;
    assert!(state.records(&share)?.is_empty());
    assert!(!root.join("ghost").exists());
    Ok(())
}

fn copy_objects(source: &State, target: &State, records: &[flocal::model::Record]) -> Result<()> {
    for record in records {
        if let Entry::File { hash, .. } = &record.version.entry {
            target.import_object(hash, &source.read_object(hash)?)?;
        }
    }
    Ok(())
}

#[test]
fn nonempty_directory_replacement_rolls_back() -> Result<()> {
    let temp = tempdir()?;
    let source = temp.path().join("source");
    let target = temp.path().join("target");
    fs::create_dir_all(source.join("node"))?;
    fs::create_dir_all(&target)?;
    fs::write(source.join("node/child"), "keep")?;
    let mut source_state = State::open(temp.path().join("source-state"))?;
    let share = source_state.init_share(&source)?;
    let mut target_state = State::open(temp.path().join("target-state"))?;
    target_state.register_share(&share, &target)?;
    let first = scan(&source_state, &share, &source, &[])?;
    copy_objects(&source_state, &target_state, &first)?;
    apply_plan(&mut target_state, &share, &first)?;
    source_state.replace_records(&share, &first)?;

    fs::write(target.join("node/.gitignore"), "child\n")?;
    fs::remove_dir_all(source.join("node"))?;
    fs::write(source.join("node"), "replacement")?;
    let second = scan(&source_state, &share, &source, &first)?;
    copy_objects(&source_state, &target_state, &second)?;
    assert!(apply_plan(&mut target_state, &share, &second).is_err());
    assert!(target.join("node").is_dir());
    assert_eq!(fs::read_to_string(target.join("node/child"))?, "keep");
    assert!(fs::read_dir(&target)?.all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".flocal-tmp-")
    }));
    Ok(())
}

#[test]
fn recovers_tombstone_after_exchange_crash() -> Result<()> {
    let temp = tempdir()?;
    let root = temp.path().join("root");
    fs::create_dir_all(&root)?;
    fs::write(root.join("gone"), "old")?;
    let mut state = State::open(temp.path().join("state"))?;
    let share = state.init_share(&root)?;
    let first = scan(&state, &share, &root, &[])?;
    state.replace_records(&share, &first)?;
    fs::remove_file(root.join("gone"))?;
    let deletion = scan(&state, &share, &root, &first)?;
    let (intent, fresh) = state.set_install_intent(&share, &deletion)?;
    assert!(fresh);
    let token = &intent.temps[0].token;
    fs::write(root.join("gone"), token)?;
    fs::write(root.join(token), "old")?;
    state.mark_install_temp_owned(&share, &deletion[0].path)?;

    apply_plan(&mut state, &share, &deletion)?;
    assert!(!root.join("gone").exists());
    assert!(!root.join(token).exists());
    assert!(state.install_intent(&share)?.is_none());
    Ok(())
}

#[test]
fn unowned_temp_collision_rotates_without_touching_user_file() -> Result<()> {
    let temp = tempdir()?;
    let source = temp.path().join("source");
    let target = temp.path().join("target");
    fs::create_dir_all(&source)?;
    fs::create_dir_all(&target)?;
    fs::write(source.join("file"), "content")?;
    let source_state = State::open(temp.path().join("source-state"))?;
    let share = source_state.init_share(&source)?;
    let records = scan(&source_state, &share, &source, &[])?;
    let mut target_state = State::open(temp.path().join("target-state"))?;
    target_state.register_share(&share, &target)?;
    copy_objects(&source_state, &target_state, &records)?;
    let (intent, fresh) = target_state.set_install_intent(&share, &records)?;
    assert!(fresh);
    let colliding_token = intent.temps[0].token.clone();
    fs::write(target.join(&colliding_token), "user-owned")?;

    apply_plan(&mut target_state, &share, &records)?;
    assert_eq!(fs::read_to_string(target.join("file"))?, "content");
    assert_eq!(
        fs::read_to_string(target.join(colliding_token))?,
        "user-owned"
    );
    Ok(())
}

#[test]
fn recovers_file_created_during_creating_phase() -> Result<()> {
    let temp = tempdir()?;
    let source = temp.path().join("source");
    let target = temp.path().join("target");
    fs::create_dir_all(&source)?;
    fs::create_dir_all(&target)?;
    fs::write(source.join("file"), "content")?;
    let source_state = State::open(temp.path().join("source-state"))?;
    let share = source_state.init_share(&source)?;
    let records = scan(&source_state, &share, &source, &[])?;
    let mut target_state = State::open(temp.path().join("target-state"))?;
    target_state.register_share(&share, &target)?;
    copy_objects(&source_state, &target_state, &records)?;
    let (intent, _) = target_state.set_install_intent(&share, &records)?;
    let install_temp = &intent.temps[0];
    target_state.mark_install_temp_creating(&share, &install_temp.path)?;
    fs::write(target.join(&install_temp.token), "content")?;

    apply_plan(&mut target_state, &share, &records)?;
    assert_eq!(fs::read_to_string(target.join("file"))?, "content");
    assert!(!target.join(&install_temp.token).exists());
    Ok(())
}

#[test]
fn mismatched_creating_temp_remains_internal_and_retryable() -> Result<()> {
    let temp = tempdir()?;
    let source = temp.path().join("source");
    let target = temp.path().join("target");
    fs::create_dir_all(&source)?;
    fs::create_dir_all(&target)?;
    fs::write(source.join("file"), "content")?;
    let source_state = State::open(temp.path().join("source-state"))?;
    let share = source_state.init_share(&source)?;
    let records = scan(&source_state, &share, &source, &[])?;
    let mut target_state = State::open(temp.path().join("target-state"))?;
    target_state.register_share(&share, &target)?;
    copy_objects(&source_state, &target_state, &records)?;
    let (intent, _) = target_state.set_install_intent(&share, &records)?;
    let install_temp = &intent.temps[0];
    target_state.mark_install_temp_creating(&share, &install_temp.path)?;
    fs::write(target.join(&install_temp.token), "partial")?;

    assert!(apply_plan(&mut target_state, &share, &records).is_err());
    let scanned = scan(&target_state, &share, &target, &[])?;
    assert!(
        scanned
            .iter()
            .all(|record| { record.path.as_bytes() != install_temp.token.as_bytes() })
    );
    apply_plan(&mut target_state, &share, &records)?;
    assert_eq!(fs::read_to_string(target.join("file"))?, "content");
    assert_eq!(
        fs::read_to_string(target.join(&install_temp.token))?,
        "partial"
    );
    let scanned = scan(&target_state, &share, &target, &[])?;
    assert!(
        scanned
            .iter()
            .all(|record| record.path.as_bytes() != install_temp.token.as_bytes())
    );
    Ok(())
}
