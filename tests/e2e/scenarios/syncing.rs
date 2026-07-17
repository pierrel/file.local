//! Catalog #3 (initial union), #4 (entry kinds and reserved names), #5
//! (offline edits and rename catch-up), and #20 (exec round-trip and binary
//! content).

use anyhow::Result;

use crate::harness as e2e;
use crate::harness::known_failure;

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn initial_union_merges_both_nonempty_trees() -> Result<()> {
    let (a, b) = e2e::containers()?;
    a.write("only-a.txt", "from a")?;
    b.write("only-b.txt", "from b")?;
    a.write("both.txt", "a version")?;
    b.write("both.txt", "b version")?;
    a.init()?;
    let (a, b) = a.peer_add(b)?;
    a.sync()?;

    a.assert_file("only-b.txt", "from b")?;
    b.assert_file("only-a.txt", "from a")?;
    e2e::assert_trees_equal(&a, &b)?;
    a.conflicts()?.expect_one("both.txt")?;
    Ok(())
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn files_directories_symlinks_and_exec_bits_sync_both_ways() -> Result<()> {
    let (a, b) = e2e::pair()?;
    a.write("src/lib.txt", "from a")?;
    a.write("run.sh", "#!/bin/sh\n")?;
    a.set_exec("run.sh")?;
    a.symlink("current", "src/lib.txt")?;
    a.mkdir("empty")?;
    a.write(".flocal-tmp-reserved", "never syncs")?;
    b.write("reply.txt", "from b")?;

    a.sync()?;

    b.assert_file("src/lib.txt", "from a")?;
    b.assert_exec("run.sh")?;
    b.assert_symlink("current", "src/lib.txt")?;
    b.assert_dir("empty")?;
    b.assert_absent(".flocal-tmp-reserved")?;
    a.assert_file("reply.txt", "from b")?;
    e2e::assert_trees_equal(&a, &b)
}

// Discovered by this harness: on main, the rename's delete half is lost
// after a single unreachable-peer cycle (the tombstone early-drop PR #2
// fixes), so the old name survives on the reconnecting peer. Verified to
// pass with PR #2 merged; promoted when it lands.
#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn offline_edits_and_renames_catch_up_after_reconnection() -> Result<()> {
    known_failure(|| {
        let (a, b) = e2e::pair()?;
        a.write("notes.txt", "v1")?;
        a.write("old-name.txt", "movable")?;
        a.sync()?;

        b.offline()?;
        a.write("notes.txt", "v2 from a")?;
        a.rename("old-name.txt", "new-name.txt")?;
        b.write("reply.txt", "written while offline")?;
        a.sync_expect_offline()?;

        b.online()?;
        a.sync()?;

        b.assert_file("notes.txt", "v2 from a")?;
        b.assert_file("new-name.txt", "movable")?;
        b.assert_absent("old-name.txt")?;
        a.assert_file("reply.txt", "written while offline")?;
        e2e::assert_trees_equal(&a, &b)
    })
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn exec_bit_round_trips_and_binary_content_survives() -> Result<()> {
    let (a, b) = e2e::pair()?;
    a.write("tool.sh", "#!/bin/sh\n")?;
    a.set_exec("tool.sh")?;
    a.write_bytes("blob.bin", &[0x00, 0x9f, 0x92, 0x96, 0xff, 0x00, 0x0a])?;
    a.sync()?;
    b.assert_exec("tool.sh")?;
    e2e::assert_trees_equal(&a, &b)?;

    a.unset_exec("tool.sh")?;
    a.sync()?;
    b.assert_not_exec("tool.sh")?;
    e2e::assert_trees_equal(&a, &b)
}
