//! Compact generated trees exercise persistence changes through both sync modes.
//! Row-mutation bounds are unit-tested; these scenarios pin user-visible safety.

use anyhow::Result;

use crate::harness as e2e;

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn generated_tree_preserves_shared_bases_and_conflicts_across_noop_rounds() -> Result<()> {
    let (a, b) = e2e::containers()?;
    a.write_mixed_tree(12, 16)?;
    a.write("shared.txt", "first: base\nsecond: base\n")?;
    a.write("contested.txt", "base")?;
    b.write("peer-only.txt", "initial union")?;
    a.init()?;
    let (a, b) = a.peer_add(b)?;
    a.sync()?;

    b.assert_file("generated/group-11/file-15.txt", "group=11 file=15\n")?;
    b.assert_dir("generated/group-11/empty")?;
    b.assert_symlink("generated/group-11/current", "file-0.txt")?;
    b.assert_exec("generated/group-11/file-0.txt")?;
    a.assert_file("peer-only.txt", "initial union")?;
    a.sync()?;
    a.sync()?;
    a.conflicts()?.expect_none()?;
    e2e::assert_trees_equal(&a, &b)?;

    b.offline()?;
    a.write("shared.txt", "first: edited on a\nsecond: base\n")?;
    b.write("shared.txt", "first: base\nsecond: edited on b\n")?;
    a.write("contested.txt", "edit from a")?;
    b.write("contested.txt", "edit from b")?;
    a.remove("generated/group-0/file-2.txt")?;
    b.remove("generated/group-1/file-2.txt")?;
    b.online()?;
    a.sync()?;

    a.assert_file("shared.txt", "first: edited on a\nsecond: edited on b\n")?;
    b.assert_absent("generated/group-0/file-2.txt")?;
    a.assert_absent("generated/group-1/file-2.txt")?;
    e2e::assert_trees_equal(&a, &b)?;
    let conflict_id = a.conflicts()?.expect_one("contested.txt")?.id.clone();
    let mut inputs = [a.read("contested.txt")?, a.restore_loser(&conflict_id)?];
    inputs.sort();
    anyhow::ensure!(inputs == ["edit from a", "edit from b"]);
    anyhow::ensure!(b.conflicts()?.expect_one("contested.txt")?.id == conflict_id);

    a.write("generated/group-0/file-2.txt", "recreated after deletion")?;
    a.sync()?;
    a.sync()?;
    b.assert_file("generated/group-0/file-2.txt", "recreated after deletion")?;
    a.assert_file("generated/group-11/file-15.txt", "group=11 file=15\n")?;
    anyhow::ensure!(a.conflicts()?.expect_one("contested.txt")?.id == conflict_id);
    e2e::assert_trees_equal(&a, &b)
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn generated_tree_watch_catches_up_after_daemon_restart() -> Result<()> {
    let (a, b) = e2e::managed_containers()?;
    a.write_mixed_tree(8, 16)?;
    a.init()?;
    let (a, b) = a.peer_add(b)?;
    a.sync()?;
    a.sync_start()?;
    a.wait_for_managed_connection("watching")?;

    a.sync_stop()?;
    a.write("generated/group-0/file-1.txt", "edited on a")?;
    b.write("generated/group-1/file-1.txt", "edited on b")?;
    a.remove("generated/group-2/file-1.txt")?;
    a.sync_start()?;
    a.restart_daemon()?;

    b.wait_for_file("generated/group-0/file-1.txt", "edited on a")?;
    a.wait_for_file("generated/group-1/file-1.txt", "edited on b")?;
    b.wait_absent("generated/group-2/file-1.txt")?;
    b.write("generated/group-2/file-1.txt", "recreated on b")?;
    a.wait_for_file("generated/group-2/file-1.txt", "recreated on b")?;
    b.assert_file("generated/group-7/file-15.txt", "group=7 file=15\n")?;
    b.assert_symlink("generated/group-7/current", "file-0.txt")?;
    b.assert_exec("generated/group-7/file-0.txt")?;
    a.sync_stop()?;
    a.conflicts()?.expect_none()?;
    e2e::assert_trees_equal(&a, &b)
}
