//! Catalog #9 (concurrent-edit conflict with a recoverable loser), #19
//! (concurrent deletions and delete-versus-edit convergence), and the
//! known-broken three-way merge story from the 2026-08-13 reconciliation.

use anyhow::Result;

use crate::harness as e2e;

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn concurrent_nonoverlapping_edits_preserve_both_changes() -> Result<()> {
    e2e::known_failure(|| {
        let (a, b) = e2e::pair()?;
        a.write("shared.txt", "first: base\nsecond: base\n")?;
        a.sync()?;

        b.offline()?;
        a.write("shared.txt", "first: edited on a\nsecond: base\n")?;
        b.write("shared.txt", "first: base\nsecond: edited on b\n")?;
        b.online()?;
        a.sync()?;

        e2e::assert_trees_equal(&a, &b)?;
        anyhow::ensure!(
            a.read("shared.txt")? == "first: edited on a\nsecond: edited on b\n",
            "non-overlapping edits were not both preserved"
        );
        a.conflicts()?.expect_none()?;
        Ok(())
    })
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
