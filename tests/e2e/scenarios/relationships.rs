//! Clean relationship-removal boundaries over real SSH and daemon control.

use anyhow::Result;

use crate::harness as e2e;

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn reachable_removal_retains_recovery_and_allows_a_fresh_pairing() -> Result<()> {
    let (a, b) = e2e::managed_pair()?;
    a.write("retained.txt", "base")?;
    b.wait_for_file("retained.txt", "base")?;

    b.offline()?;
    a.write("retained.txt", "from a")?;
    b.write("retained.txt", "from b")?;
    b.online()?;
    a.wait_for_status(|status| status.recovery.conflicts == 1)?;
    b.wait_for_status(|status| status.recovery.conflicts == 1)?;
    e2e::assert_trees_equal(&a, &b)?;

    let conflict_id = a.conflicts()?.expect_one("retained.txt")?.id.clone();
    let visible = a.read("retained.txt")?;
    let loser = a.restore_loser(&conflict_id)?;

    a.sync_remove()?;
    a.assert_sync_list_empty()?;
    b.assert_sync_list_empty()?;
    let a_status = a.status()?;
    let b_status = b.status()?;
    anyhow::ensure!(
        a_status.relationship_state == "unpaired"
            && a_status.bound_peer.is_none()
            && !a_status.removal_pending
            && a_status.removal_error.is_none()
    );
    anyhow::ensure!(
        b_status.relationship_state == "unpaired"
            && b_status.bound_peer.is_none()
            && !b_status.removal_pending
            && b_status.removal_error.is_none()
    );
    a.assert_file("retained.txt", &visible)?;
    b.assert_file("retained.txt", &visible)?;
    anyhow::ensure!(a.conflicts()?.expect_one("retained.txt")?.id == conflict_id);
    anyhow::ensure!(a.restore_loser(&conflict_id)? == loser);
    anyhow::ensure!(b.status()?.recovery.conflicts == 1);

    a.write("only-a.txt", "a")?;
    b.write("only-b.txt", "b")?;
    a.sync_add_to(&b)?;
    a.assert_file("only-b.txt", "b")?;
    b.assert_file("only-a.txt", "a")?;
    e2e::assert_trees_equal(&a, &b)?;
    anyhow::ensure!(a.conflicts()?.expect_one("retained.txt")?.id == conflict_id);
    a.sync_stop()?;
    Ok(())
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn offline_removal_stays_pending_until_the_user_retries() -> Result<()> {
    let (a, b) = e2e::managed_pair()?;
    a.write("kept.txt", "safe")?;
    b.wait_for_file("kept.txt", "safe")?;

    b.offline()?;
    a.sync_remove_expect_err("relationship removal is pending and disabled")?;
    a.assert_sync_removing()?;
    let pending = a.status()?;
    anyhow::ensure!(
        pending.relationship_state == "removing"
            && pending.removal_pending
            && pending.removal_error.is_some()
    );
    a.assert_file("kept.txt", "safe")?;
    b.assert_file("kept.txt", "safe")?;

    b.online()?;
    a.assert_sync_removing()?;
    a.sync_remove()?;
    a.assert_sync_list_empty()?;
    b.assert_sync_list_empty()?;
    a.assert_file("kept.txt", "safe")?;
    b.assert_file("kept.txt", "safe")?;
    Ok(())
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn local_only_removal_is_explicitly_asymmetric() -> Result<()> {
    let (a, b) = e2e::managed_pair()?;
    a.write("kept.txt", "safe")?;
    b.wait_for_file("kept.txt", "safe")?;

    a.sync_remove_local_only()?;
    a.assert_sync_list_empty()?;
    b.assert_sync_role("responder")?;
    let a_status = a.status()?;
    let b_status = b.status()?;
    anyhow::ensure!(a_status.relationship_state == "unpaired" && a_status.bound_peer.is_none());
    anyhow::ensure!(b_status.relationship_state == "responder" && b_status.bound_peer.is_some());
    a.assert_file("kept.txt", "safe")?;
    b.assert_file("kept.txt", "safe")?;

    a.write("local-after-removal.txt", "local")?;
    b.assert_absent("local-after-removal.txt")?;
    b.sync_remove_local_only()?;
    b.assert_sync_list_empty()?;
    anyhow::ensure!(b.status()?.relationship_state == "unpaired");
    Ok(())
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn active_foreground_watch_keeps_removal_pending_without_detaching_the_peer() -> Result<()> {
    let (a, b) = e2e::managed_pair()?;
    a.sync_stop()?;
    let watch = a.watch_start()?;
    watch.wait_for_log("Peer connected")?;

    a.sync_remove_expect_err("another sync/watch operation already owns this share")?;
    a.assert_sync_removing()?;
    b.assert_sync_role("responder")?;

    watch.stop()?;
    a.sync_remove()?;
    a.assert_sync_list_empty()?;
    b.assert_sync_list_empty()?;
    Ok(())
}
