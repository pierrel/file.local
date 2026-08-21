use std::fs;
use std::path::Path;
use std::process::{Command, Output};

use anyhow::{Context, Result};
use flocal::model::{Entry, ObjectHash, PeerId, Record, RelativePath, Version};
use flocal::reconcile::Conflict;
use flocal::state::State;
use tempfile::tempdir;

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

fn conflict(path: &[u8], bytes: &[u8], sequence: u64) -> Result<(Conflict, ObjectHash)> {
    let hash = ObjectHash::from_blake3(blake3::hash(bytes));
    Ok((
        Conflict::whole_file(
            record(
                path,
                "winner",
                sequence,
                Entry::File {
                    hash: hash.clone(),
                    size: bytes.len() as u64,
                    executable: false,
                },
            )?,
            record(path, "loser", sequence - 1, Entry::Tombstone)?,
            flocal::merge::FallbackReason::AbsentBase,
        ),
        hash,
    ))
}

fn flocal(state: &Path, arguments: &[&str]) -> Result<Output> {
    Command::new(env!("CARGO_BIN_EXE_flocal"))
        .args(arguments)
        .env("FLOCAL_STATE_DIR", state)
        .output()
        .context("running flocal")
}

fn flocal_ok(state: &Path, arguments: &[&str]) -> Result<Output> {
    let output = flocal(state, arguments)?;
    anyhow::ensure!(
        output.status.success(),
        "flocal {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(output)
}

#[test]
fn recovery_cli_reports_budgets_and_applies_token_bound_pruning() -> Result<()> {
    let temp = tempdir()?;
    let root = temp.path().join("root");
    let state_dir = temp.path().join("state");
    fs::create_dir(&root)?;
    let mut state = State::open(&state_dir)?;
    let share = state.init_share(&root)?;
    let (first, first_hash) = conflict(b"first", b"first bytes", 2)?;
    let (second, second_hash) = conflict(b"second", b"second bytes", 4)?;
    state.import_object(&first_hash, b"first bytes")?;
    state.import_object(&second_hash, b"second bytes")?;
    state.add_conflicts(&share, &[first.clone(), second.clone()])?;
    drop(state);

    let root_text = root.to_str().context("UTF-8 test root")?;
    let status = flocal_ok(&state_dir, &["status", root_text])?;
    assert!(String::from_utf8_lossy(&status.stdout).contains("Recovery: 2 conflicts"));
    let status = flocal_ok(&state_dir, &["status", root_text, "--json"])?;
    let status: serde_json::Value = serde_json::from_slice(&status.stdout)?;
    assert_eq!(status["schema"], 6);
    assert_eq!(status["recovery"]["conflicts"], 2);

    let first_id = flocal::reconcile::conflict_id(&first);
    let preview = flocal_ok(
        &state_dir,
        &["conflicts", "prune", root_text, &first_id, "--json"],
    )?;
    let preview: serde_json::Value = serde_json::from_slice(&preview.stdout)?;
    assert_eq!(preview["applied"], false);
    let token = preview["selection_token"]
        .as_str()
        .context("selection token")?;
    let applied = flocal_ok(
        &state_dir,
        &[
            "conflicts",
            "prune",
            root_text,
            &first_id,
            "--selection",
            token,
            "--yes",
            "--json",
        ],
    )?;
    let applied: serde_json::Value = serde_json::from_slice(&applied.stdout)?;
    assert_eq!(applied["applied"], true);
    assert_eq!(applied["collection_pending"], false);

    let preview = flocal_ok(&state_dir, &["conflicts", "prune", root_text])?;
    let preview_text = String::from_utf8_lossy(&preview.stdout);
    assert!(preview_text.contains("Share allowance released"));
    let token = preview_text
        .split("Selection token: ")
        .nth(1)
        .and_then(|tail| tail.split_whitespace().next())
        .context("human selection token")?;
    let applied = flocal_ok(
        &state_dir,
        &[
            "conflicts",
            "prune",
            root_text,
            "--selection",
            token,
            "--yes",
        ],
    )?;
    assert!(String::from_utf8_lossy(&applied.stdout).contains("Pruned 1 recovery conflicts"));

    let budget = flocal_ok(&state_dir, &["conflicts", "budget", root_text, "11GiB"])?;
    assert!(String::from_utf8_lossy(&budget.stdout).contains("Raised local recovery budget"));
    let budget = flocal_ok(
        &state_dir,
        &[
            "conflicts",
            "budget",
            "--share",
            &share.0,
            "12GiB",
            "--json",
        ],
    )?;
    let budget: serde_json::Value = serde_json::from_slice(&budget.stdout)?;
    assert_eq!(budget["target"], "local");
    assert_eq!(budget["budget_bytes"], 12u64 * 1024 * 1024 * 1024);

    let state = State::open(&state_dir)?;
    state.set_blocked(&share, "recovery storage budget exceeded")?;
    drop(state);
    let budget = flocal_ok(
        &state_dir,
        &["conflicts", "budget", "--share", &share.0, "13GiB"],
    )?;
    assert!(String::from_utf8_lossy(&budget.stderr).contains("this installation is the responder"));
    Ok(())
}

#[test]
#[cfg(feature = "e2e-test-hooks")]
fn recovery_cli_failure_paths_leave_truthful_durable_state() -> Result<()> {
    let temp = tempdir()?;
    let root = temp.path().join("root");
    let state_dir = temp.path().join("state");
    fs::create_dir(&root)?;
    let mut state = State::open(&state_dir)?;
    let share = state.init_share(&root)?;
    let (first, hash) = conflict(b"first", b"bytes", 2)?;
    state.import_object(&hash, b"bytes")?;
    state.add_conflicts(&share, std::slice::from_ref(&first))?;
    drop(state);
    let root_text = root.to_str().context("UTF-8 test root")?;
    let id = flocal::reconcile::conflict_id(&first);

    for arguments in [
        vec!["conflicts", "prune", root_text, &id, "--yes"],
        vec!["conflicts", "prune", root_text, &id, "--selection", "bad"],
        vec!["conflicts", "prune", root_text, &id, &id],
        vec!["conflicts", "budget", root_text, "0"],
        vec!["conflicts", "budget", root_text, "1.5GiB"],
        vec!["conflicts", "budget", root_text, "18446744073709551615GiB"],
        vec!["conflicts", "budget", "--share", "bad/share", "11GiB"],
        vec![
            "conflicts",
            "budget",
            root_text,
            "11GiB",
            "--share",
            &share.0,
        ],
    ] {
        assert!(!flocal(&state_dir, &arguments)?.status.success());
    }

    fs::write(state_dir.join(".e2e-recovery-budget-bytes"), b"1")?;
    let status = flocal_ok(&state_dir, &["status", root_text])?;
    assert!(String::from_utf8_lossy(&status.stdout).contains("recovery storage is at its limit"));
    fs::remove_file(state_dir.join(".e2e-recovery-budget-bytes"))?;

    fs::write(state_dir.join(".e2e-recovery-temp-fail"), b"1")?;
    assert!(!flocal(&state_dir, &["status", root_text])?.status.success());
    assert!(
        !flocal(&state_dir, &["conflicts", "prune", root_text])?
            .status
            .success()
    );
    let preview = flocal_ok(
        &state_dir,
        &["conflicts", "prune", root_text, &id, "--json"],
    )?;
    let preview: serde_json::Value = serde_json::from_slice(&preview.stdout)?;
    let token = preview["selection_token"].as_str().context("token")?;
    fs::remove_file(state_dir.join(".e2e-recovery-temp-fail"))?;
    fs::write(state_dir.join(".e2e-collector-fail"), b"1")?;
    let applied = flocal(
        &state_dir,
        &[
            "conflicts",
            "prune",
            root_text,
            &id,
            "--selection",
            token,
            "--yes",
            "--json",
        ],
    )?;
    assert!(!applied.status.success());
    let applied_json: serde_json::Value = serde_json::from_slice(&applied.stdout)?;
    assert_eq!(applied_json["applied"], true);
    assert_eq!(applied_json["collection_pending"], true);
    fs::remove_file(state_dir.join(".e2e-collector-fail"))?;
    let state = State::open(&state_dir)?;
    assert!(state.conflicts(&share)?.is_empty());
    assert!(state.object_path(&hash).exists());
    state.prune_unreferenced_objects()?;
    assert!(!state.object_path(&hash).exists());
    Ok(())
}

#[test]
#[cfg(feature = "e2e-test-hooks")]
fn oversized_full_preview_uses_paged_ids_and_selected_pruning() -> Result<()> {
    let temp = tempdir()?;
    let root = temp.path().join("root");
    let state_dir = temp.path().join("state");
    fs::create_dir(&root)?;
    let mut state = State::open(&state_dir)?;
    let share = state.init_share(&root)?;
    let mut expected_ids = Vec::new();
    for (path, bytes, sequence) in [
        (b"first".as_slice(), b"one".as_slice(), 2),
        (b"second".as_slice(), b"two".as_slice(), 4),
        (b"third".as_slice(), b"three".as_slice(), 6),
    ] {
        let (conflict, hash) = conflict(path, bytes, sequence)?;
        state.import_object(&hash, bytes)?;
        expected_ids.push(flocal::reconcile::conflict_id(&conflict));
        state.add_conflicts(&share, &[conflict])?;
    }
    expected_ids.sort();
    drop(state);
    fs::write(state_dir.join(".e2e-recovery-preview-summary-limit"), b"1")?;

    let root_text = root.to_str().context("UTF-8 test root")?;
    let full = flocal(&state_dir, &["conflicts", "prune", root_text])?;
    assert!(!full.status.success());
    assert!(String::from_utf8_lossy(&full.stderr).contains("all-conflict preview is too large"));

    let first_page = flocal_ok(
        &state_dir,
        &[
            "conflicts",
            "list",
            root_text,
            "--ids",
            "--limit",
            "1",
            "--json",
        ],
    )?;
    let first_page: serde_json::Value = serde_json::from_slice(&first_page.stdout)?;
    assert_eq!(first_page["conflicts"][0]["id"], expected_ids[0]);
    assert_eq!(first_page["next_after"], expected_ids[0]);
    let second_page = flocal_ok(
        &state_dir,
        &[
            "conflicts",
            "list",
            root_text,
            "--ids",
            "--limit",
            "1",
            "--after",
            &expected_ids[0],
            "--json",
        ],
    )?;
    let second_page: serde_json::Value = serde_json::from_slice(&second_page.stdout)?;
    assert_eq!(second_page["conflicts"][0]["id"], expected_ids[1]);

    let preview = flocal_ok(
        &state_dir,
        &["conflicts", "prune", root_text, &expected_ids[0], "--json"],
    )?;
    let preview: serde_json::Value = serde_json::from_slice(&preview.stdout)?;
    let token = preview["selection_token"].as_str().context("token")?;
    flocal_ok(
        &state_dir,
        &[
            "conflicts",
            "prune",
            root_text,
            &expected_ids[0],
            "--selection",
            token,
            "--yes",
        ],
    )?;
    assert_eq!(State::open(&state_dir)?.conflicts(&share)?.len(), 2);
    Ok(())
}

#[test]
#[cfg(unix)]
fn restore_and_prune_wait_for_the_same_object_lock() -> Result<()> {
    let temp = tempdir()?;
    let root = temp.path().join("root");
    let state_dir = temp.path().join("state");
    fs::create_dir(&root)?;
    let mut state = State::open(&state_dir)?;
    let share = state.init_share(&root)?;
    let (conflict, hash) = conflict(b"locked", b"recoverable", 2)?;
    state.import_object(&hash, b"recoverable")?;
    state.add_conflicts(&share, std::slice::from_ref(&conflict))?;
    let id = flocal::reconcile::conflict_id(&conflict);
    let root_text = root.to_str().context("UTF-8 test root")?;

    let object_lock = state.lock_objects()?;
    let destination = temp.path().join("restored");
    let mut restore = Command::new(env!("CARGO_BIN_EXE_flocal"))
        .args([
            "restore",
            root_text,
            &id,
            "--version",
            "winner",
            "--to",
            destination.to_str().context("UTF-8 destination")?,
        ])
        .env("FLOCAL_STATE_DIR", &state_dir)
        .spawn()?;
    std::thread::sleep(std::time::Duration::from_millis(100));
    assert!(
        restore.try_wait()?.is_none(),
        "restore bypassed the object lock"
    );
    drop(object_lock);
    assert!(restore.wait()?.success());
    assert_eq!(fs::read(&destination)?, b"recoverable");

    let preview = state.recovery_prune_plan(&share, std::slice::from_ref(&id))?;
    let object_lock = state.lock_objects()?;
    let mut prune = Command::new(env!("CARGO_BIN_EXE_flocal"))
        .args([
            "conflicts",
            "prune",
            root_text,
            &id,
            "--selection",
            &preview.selection_token,
            "--yes",
        ])
        .env("FLOCAL_STATE_DIR", &state_dir)
        .spawn()?;
    std::thread::sleep(std::time::Duration::from_millis(100));
    assert!(
        prune.try_wait()?.is_none(),
        "prune bypassed the object lock"
    );
    drop(object_lock);
    assert!(prune.wait()?.success());
    assert!(state.conflicts(&share)?.is_empty());
    Ok(())
}
