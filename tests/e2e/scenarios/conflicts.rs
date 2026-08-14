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
