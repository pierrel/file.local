//! Catalog #9 (concurrent-edit conflict with a recoverable loser), #19
//! (concurrent deletions and delete-versus-edit convergence), and clean
//! three-way merge after concurrent offline edits.

use anyhow::Result;

use crate::harness as e2e;

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn concurrent_nonoverlapping_edits_preserve_both_changes() -> Result<()> {
    let (a, b) = e2e::pair()?;
    a.write("shared.txt", "first: base\nsecond: base\n")?;
    a.sync()?;

    b.offline()?;
    a.write("shared.txt", "first: edited on a\nsecond: base\n")?;
    b.write("shared.txt", "first: base\nsecond: edited on b\n")?;
    b.online()?;
    a.sync()?;

    e2e::assert_trees_equal(&a, &b)?;
    let visible = a.read("shared.txt")?;
    anyhow::ensure!(
        visible == "first: edited on a\nsecond: edited on b\n",
        "non-overlapping edits were not both preserved: {visible:?}"
    );
    a.conflicts()?.expect_none()?;
    Ok(())
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn concurrent_edit_keeps_one_winner_and_a_recoverable_loser() -> Result<()> {
    let (a, b) = e2e::pair()?;
    a.write("notes.txt", "base")?;
    a.sync()?;

    b.offline()?;
    a.write("notes.txt", "edit from a")?;
    b.write("notes.txt", "edit from b")?;
    b.online()?;
    a.sync()?;

    // Both peers show the same deterministic winner; which side wins depends
    // on discovery-scan ordering, so only recoverability is asserted.
    e2e::assert_trees_equal(&a, &b)?;
    let conflicts = a.conflicts()?;
    let conflict = conflicts.expect_one("notes.txt")?;
    let winner = a.read("notes.txt")?;
    let loser = a.restore_loser(&conflict.id)?;
    let mut both = [winner, loser];
    both.sort();
    anyhow::ensure!(
        both == ["edit from a", "edit from b"],
        "winner and loser must preserve both inputs, got {both:?}"
    );
    Ok(())
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn overlapping_merge_keeps_clean_regions_and_every_recovery_input() -> Result<()> {
    let (a, b) = e2e::pair()?;
    let base = "top: base\nmiddle: base\nbottom: base\n";
    a.write("overlap.txt", base)?;
    a.sync()?;

    b.offline()?;
    a.write("overlap.txt", "top: a\nmiddle: a\nbottom: base\n")?;
    b.write("overlap.txt", "top: base\nmiddle: b\nbottom: b\n")?;
    b.online()?;
    a.sync()?;

    e2e::assert_trees_equal(&a, &b)?;
    let visible = a.read("overlap.txt")?;
    anyhow::ensure!(visible.starts_with("top: a\n"), "clean A region was lost");
    anyhow::ensure!(visible.ends_with("bottom: b\n"), "clean B region was lost");
    anyhow::ensure!(
        visible.contains("middle: a\n") || visible.contains("middle: b\n"),
        "overlap did not choose either complete input"
    );
    let conflicts = a.conflicts()?;
    let conflict = conflicts.expect_one("overlap.txt")?;
    anyhow::ensure!(
        a.restore_base(&conflict.id)? == base,
        "base was not retained"
    );
    anyhow::ensure!(
        a.restore_merged(&conflict.id)? == visible,
        "merged recovery bytes differ from the visible file"
    );
    Ok(())
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn concurrent_deletions_and_delete_versus_edit_converge() -> Result<()> {
    let (a, b) = e2e::pair()?;
    a.write("both-delete.txt", "doomed")?;
    a.write("delete-vs-edit.txt", "contested")?;
    a.sync()?;

    b.offline()?;
    a.remove("both-delete.txt")?;
    b.remove("both-delete.txt")?;
    a.remove("delete-vs-edit.txt")?;
    b.write("delete-vs-edit.txt", "edited while offline")?;
    b.online()?;
    a.sync()?;

    // Both sides agree; the delete-versus-edit winner follows the documented
    // discovery-scan ordering and is deliberately not asserted.
    a.assert_absent("both-delete.txt")?;
    b.assert_absent("both-delete.txt")?;
    e2e::assert_trees_equal(&a, &b)?;

    a.sync()?; // a second round must change nothing further
    e2e::assert_trees_equal(&a, &b)
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn recovery_usage_is_visible_and_pruning_is_explicitly_local() -> Result<()> {
    let (a, b) = e2e::pair()?;
    a.write("recovery.txt", "base")?;
    a.sync()?;

    b.offline()?;
    a.write("recovery.txt", "from a")?;
    b.write("recovery.txt", "from b")?;
    b.online()?;
    a.sync()?;

    let conflict_id = a.conflicts()?.expect_one("recovery.txt")?.id.clone();
    let before = a.status()?.recovery;
    anyhow::ensure!(before.conflicts == 1 && before.used_bytes > 0);
    anyhow::ensure!(before.budget_bytes > before.used_bytes && !before.over_budget);
    anyhow::ensure!(before.reclaimable_bytes <= before.used_bytes);
    a.prune_conflict(&conflict_id)?;
    anyhow::ensure!(a.status()?.recovery.conflicts == 0);
    anyhow::ensure!(
        b.status()?.recovery.conflicts == 1,
        "pruning one installation unexpectedly pruned its peer"
    );
    e2e::assert_trees_equal(&a, &b)?;
    b.prune_conflict(&conflict_id)?;
    anyhow::ensure!(b.status()?.recovery.conflicts == 0);
    let local_budget = a.status()?.recovery.budget_bytes;
    a.raise_peer_recovery_budget("11GiB")?;
    anyhow::ensure!(a.status()?.recovery.budget_bytes == local_budget);
    anyhow::ensure!(b.status()?.recovery.budget_bytes == 11 * 1024 * 1024 * 1024);
    Ok(())
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn object_store_enospc_leaves_both_visible_trees_and_recovery_untouched() -> Result<()> {
    let (a, b) = e2e::pair()?;
    a.write("full.txt", "base")?;
    a.sync()?;

    b.offline()?;
    a.write("full.txt", "from a")?;
    b.write("full.txt", "from b")?;
    b.online()?;
    a.object_enospc(true)?;
    a.sync_expect_err("No space left on device")?;
    a.assert_file("full.txt", "from a")?;
    b.assert_file("full.txt", "from b")?;
    a.conflicts()?.expect_none()?;
    b.conflicts()?.expect_none()?;
    a.assert_no_object_temporaries()?;

    a.object_enospc(false)?;
    a.sync()?;
    e2e::assert_trees_equal(&a, &b)?;
    anyhow::ensure!(a.status()?.recovery.conflicts == 1);
    Ok(())
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn both_endpoints_reject_over_budget_recovery_before_visible_mutation() -> Result<()> {
    let (a, b) = e2e::pair()?;
    a.write("bounded.txt", "base")?;
    a.sync()?;

    b.offline()?;
    a.write("bounded.txt", "from a")?;
    b.write("bounded.txt", "from b")?;
    b.online()?;

    a.recovery_budget_limit(Some(1))?;
    a.sync_dry_run()?;
    a.sync_expect_err("recovery storage budget exceeded")?;
    a.assert_file("bounded.txt", "from a")?;
    b.assert_file("bounded.txt", "from b")?;
    a.conflicts()?.expect_none()?;
    b.conflicts()?.expect_none()?;
    a.recovery_budget_limit(None)?;

    b.recovery_budget_limit(Some(1))?;
    a.sync_expect_err("remote rejected recovery plan: recovery storage budget exceeded")?;
    a.assert_file("bounded.txt", "from a")?;
    b.assert_file("bounded.txt", "from b")?;
    a.conflicts()?.expect_none()?;
    b.conflicts()?.expect_none()?;
    b.recovery_budget_limit(None)?;

    a.sync()?;
    e2e::assert_trees_equal(&a, &b)?;
    anyhow::ensure!(a.status()?.recovery.conflicts == 1);
    anyhow::ensure!(b.status()?.recovery.conflicts == 1);
    a.recovery_budget_limit(Some(1))?;
    a.sync_dry_run()?;
    a.recovery_budget_limit(None)?;
    Ok(())
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn managed_watch_blocks_once_and_resumes_after_a_budget_raise() -> Result<()> {
    let (a, b) = e2e::managed_pair()?;
    a.write("managed-budget.txt", "base")?;
    b.wait_for_file("managed-budget.txt", "base")?;
    a.recovery_budget_limit(Some(1))?;

    b.offline()?;
    a.write("managed-budget.txt", "from a")?;
    b.write("managed-budget.txt", "from b")?;
    b.online()?;
    a.wait_for_sync_diagnostic("recovery storage budget exceeded")?;
    a.assert_file("managed-budget.txt", "from a")?;
    b.assert_file("managed-budget.txt", "from b")?;
    a.conflicts()?.expect_none()?;
    b.conflicts()?.expect_none()?;

    a.recovery_budget_limit(None)?;
    a.raise_recovery_budget("11GiB")?;
    a.wait_for_status(|status| status.recovery.conflicts == 1)?;
    b.wait_for_status(|status| status.recovery.conflicts == 1)?;
    e2e::assert_trees_equal(&a, &b)?;

    a.write("managed-peer-budget.txt", "base")?;
    b.wait_for_file("managed-peer-budget.txt", "base")?;
    b.recovery_budget_limit(Some(1))?;
    b.offline()?;
    a.write("managed-peer-budget.txt", "from a")?;
    b.write("managed-peer-budget.txt", "from b")?;
    b.online()?;
    a.wait_for_sync_diagnostic("recovery storage budget exceeded")?;
    a.assert_file("managed-peer-budget.txt", "from a")?;
    b.assert_file("managed-peer-budget.txt", "from b")?;
    b.recovery_budget_limit(None)?;
    a.raise_peer_recovery_budget("11GiB")?;
    a.wait_for_status(|status| status.recovery.conflicts == 2)?;
    b.wait_for_status(|status| status.recovery.conflicts == 2)?;
    e2e::assert_trees_equal(&a, &b)?;
    a.sync_stop()?;
    Ok(())
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn managed_watch_resumes_after_count_and_metadata_pruning() -> Result<()> {
    let (a, b) = e2e::managed_pair()?;
    a.write("first-limit.txt", "base")?;
    b.wait_for_file("first-limit.txt", "base")?;
    b.offline()?;
    a.write("first-limit.txt", "from a")?;
    b.write("first-limit.txt", "from b")?;
    b.online()?;
    a.wait_for_status(|status| status.recovery.conflicts == 1)?;
    let first = a.conflicts()?.expect_one("first-limit.txt")?.id.clone();

    a.recovery_conflict_limit(Some(1))?;
    a.write("count-limit.txt", "base")?;
    b.wait_for_file("count-limit.txt", "base")?;
    b.offline()?;
    a.write("count-limit.txt", "from a")?;
    b.write("count-limit.txt", "from b")?;
    b.online()?;
    a.wait_for_sync_diagnostic("recovery conflict count exceeded")?;
    a.prune_conflict(&first)?;
    a.wait_for_status(|status| status.recovery.conflicts == 1)?;
    e2e::assert_trees_equal(&a, &b)?;

    let second = a.conflicts()?.expect_one("count-limit.txt")?.id.clone();
    let metadata = a.status()?.recovery.metadata_bytes;
    a.recovery_conflict_limit(None)?;
    a.recovery_metadata_limit(Some(metadata + metadata / 2))?;
    a.write("metadata-limit.txt", "base")?;
    b.wait_for_file("metadata-limit.txt", "base")?;
    b.offline()?;
    a.write("metadata-limit.txt", "from a")?;
    b.write("metadata-limit.txt", "from b")?;
    b.online()?;
    a.wait_for_sync_diagnostic("recovery metadata limit exceeded")?;
    a.prune_conflict(&second)?;
    a.wait_for_status(|status| status.recovery.conflicts == 1)?;
    e2e::assert_trees_equal(&a, &b)?;
    a.recovery_metadata_limit(None)?;
    a.sync_stop()?;
    Ok(())
}
