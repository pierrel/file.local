//! Catalog #6 (offline deletion retention), #7 (nested deletion), #8
//! (recreation over a retained tombstone), and #11 (status and tombstone
//! counts). #6, #7, #8, and #11 pinned the tombstone early-drop bug family
//! as known failures until this branch's fix promoted them.

use anyhow::Result;

use crate::harness as e2e;

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn offline_deletion_propagates_and_nothing_resurrects() -> Result<()> {
    let (a, b) = e2e::pair()?;
    a.write("dir/child", "v1")?;
    a.sync()?;
    b.assert_file("dir/child", "v1")?;

    b.offline()?;
    a.remove_dir_all("dir")?;
    for _ in 0..3 {
        a.sync_expect_offline()?; // a watch cycle: refresh, then fail to reach b
    }
    a.assert_status(|s| s.tombstones == Some(2))?; // exactly dir and dir/child

    b.online()?;
    a.sync()?;
    a.assert_absent("dir")?;
    b.assert_absent("dir")?;

    a.sync()?; // steady-state round: retained tombstones must not reapply
    a.assert_absent("dir")?;
    b.assert_absent("dir")?;
    e2e::assert_trees_equal(&a, &b)
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn nested_directory_deletion_propagates() -> Result<()> {
    let (a, b) = e2e::pair()?;
    a.write("tree/branch/leaf", "green")?;
    a.write("tree/root", "brown")?;
    a.sync()?;
    b.assert_file("tree/branch/leaf", "green")?;

    a.remove_dir_all("tree")?;
    a.sync()?;
    b.assert_absent("tree")?;
    e2e::assert_trees_equal(&a, &b)
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn recreation_supersedes_a_retained_tombstone() -> Result<()> {
    let (a, b) = e2e::pair()?;
    a.write("dir/child", "first")?;
    a.sync()?;

    b.offline()?;
    a.remove_dir_all("dir")?;
    for _ in 0..2 {
        a.sync_expect_offline()?;
    }
    b.online()?;
    a.sync()?;
    b.assert_absent("dir")?;

    a.write("dir/child", "second")?;
    a.sync()?;
    b.assert_file("dir/child", "second")?;
    e2e::assert_trees_equal(&a, &b)
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn status_reports_the_deletion_lifecycle() -> Result<()> {
    let (a, b) = e2e::pair()?;
    a.write("keep.txt", "stays")?;
    a.write("gone.txt", "leaves")?;
    a.sync()?;
    a.assert_status(|s| s.entries == 2 && s.tombstones == Some(0))?;

    a.remove("gone.txt")?;
    a.sync()?;
    a.assert_status(|s| s.entries == 1 && s.tombstones == Some(1))?;
    b.assert_absent("gone.txt")?;

    a.write("gone.txt", "back")?;
    a.sync()?;
    a.assert_status(|s| s.entries == 2 && s.tombstones == Some(0))?;
    Ok(())
}
