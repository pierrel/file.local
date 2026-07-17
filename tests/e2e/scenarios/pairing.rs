//! Catalog #1 (install and pairing), #2 (preview/confirm gate), and #10
//! (peer identity mismatch).

use anyhow::Result;

use crate::harness as e2e;
use crate::harness::known_failure;

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn pairing_over_the_alias_discovers_and_registers() -> Result<()> {
    let (a, b) = e2e::containers()?;
    a.init()?;
    let (a, b) = a.peer_add(b)?;
    a.write("hello.txt", "hello")?;
    a.sync()?;
    b.assert_file("hello.txt", "hello")?;
    e2e::assert_trees_equal(&a, &b)
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn preview_and_declined_confirmation_change_nothing() -> Result<()> {
    let (a, b) = e2e::containers()?;
    a.init()?;
    let (a, b) = a.peer_add(b)?;
    a.write("pending.txt", "not yet")?;

    a.sync_dry_run()?;
    b.assert_absent("pending.txt")?;

    a.sync_decline()?;
    b.assert_absent("pending.txt")?;

    a.sync()?;
    b.assert_file("pending.txt", "not yet")?;
    Ok(())
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn wiped_responder_is_rejected_without_touching_files() -> Result<()> {
    known_failure(|| {
        let (a, b) = e2e::pair()?;
        a.write("kept.txt", "safe")?;
        a.sync()?;

        b.reset_state()?;
        a.sync_expect_err("peer identity mismatch")?;
        a.assert_file("kept.txt", "safe")?;
        b.assert_file("kept.txt", "safe")?;
        Ok(())
    })
}
