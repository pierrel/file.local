//! Base-revision to candidate installation and migration scenarios.

use anyhow::Result;

use crate::harness as e2e;

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn idle_managed_pair_survives_a_real_candidate_install() -> Result<()> {
    let (a, b) = e2e::upgrade_managed_pair()?;
    let before_a = a.status()?;
    let before_b = b.status()?;
    a.write("before-upgrade.txt", "created by the base connector")?;
    b.wait_for_file("before-upgrade.txt", "created by the base connector")?;

    a.install_candidate()?;
    a.wait_for_managed_connection("reconnecting")?;
    a.write("mixed-version.txt", "held until the responder upgrade")?;
    b.assert_absent("mixed-version.txt")?;
    b.install_candidate()?;

    b.wait_for_file("mixed-version.txt", "held until the responder upgrade")?;
    a.write("after-upgrade-a.txt", "candidate connector resumed")?;
    b.write("after-upgrade-b.txt", "candidate responder resumed")?;
    b.wait_for_file("after-upgrade-a.txt", "candidate connector resumed")?;
    a.wait_for_file("after-upgrade-b.txt", "candidate responder resumed")?;
    b.install_candidate()?;
    a.install_candidate()?;
    a.write("after-compatible-reinstall.txt", "responder drained itself")?;
    b.wait_for_file("after-compatible-reinstall.txt", "responder drained itself")?;
    a.wait_for_managed_connection("watching")?;
    a.assert_clean_upgrade_status(&before_a)?;
    b.assert_clean_upgrade_status(&before_b)?;
    a.assert_sync_role("connector")?;
    b.assert_sync_role("responder")?;
    e2e::assert_trees_equal(&a, &b)
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn in_progress_managed_sync_survives_a_real_candidate_install() -> Result<()> {
    let (a, b) = e2e::upgrade_managed_pair()?;
    let before_a = a.status()?;
    let before_b = b.status()?;
    a.arm_apply_stops(1)?;
    b.write("during-upgrade.txt", "interrupted base apply")?;
    let stopped = a.wait_for_stopped_apply_process()?;
    anyhow::ensure!(
        a.status()?.pending_install,
        "base daemon stopped before apply without recording install intent"
    );
    a.kill_daemon_on_service_stop()?;

    a.install_candidate()?;
    drop(stopped);
    b.install_candidate()?;

    a.wait_for_file("during-upgrade.txt", "interrupted base apply")?;
    anyhow::ensure!(
        !a.status()?.pending_install,
        "candidate did not clear the interrupted install intent"
    );
    a.write("after-upgrade.txt", "candidate session resumed")?;
    b.wait_for_file("after-upgrade.txt", "candidate session resumed")?;
    a.wait_for_managed_connection("watching")?;
    a.assert_clean_upgrade_status(&before_a)?;
    b.assert_clean_upgrade_status(&before_b)?;
    a.assert_sync_role("connector")?;
    b.assert_sync_role("responder")?;
    e2e::assert_trees_equal(&a, &b)
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn foreground_watch_recovers_an_old_interrupted_install_after_upgrade() -> Result<()> {
    let (a, b) = e2e::upgrade_managed_pair()?;
    a.sync_stop()?;
    let watch = a.watch_start_with_apply_stop()?;
    b.write("interrupted-watch.txt", "written while the old watch is stopped")?;
    watch.wait_stopped()?;
    anyhow::ensure!(
        a.status()?.pending_install,
        "old foreground watch stopped before apply without recording install intent"
    );
    watch.kill()?;

    a.install_candidate()?;
    b.install_candidate()?;

    let recovered_watch = a.watch_start()?;
    recovered_watch.wait_for_log("Peer connected")?;
    a.wait_for_recovered_install()?;
    b.wait_for_file("interrupted-watch.txt", "written while the old watch is stopped")?;
    anyhow::ensure!(
        !a.status()?.pending_install && a.status()?.unsettled.is_empty(),
        "foreground recovery left pending or unsettled state"
    );
    e2e::assert_trees_equal(&a, &b)
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn foreground_watch_causes_a_safe_upgrade_refusal() -> Result<()> {
    let (a, b) = e2e::upgrade_managed_pair()?;
    a.sync_stop()?;
    let watch = a.watch_start()?;
    watch.wait_for_log("Peer connected")?;

    a.install_candidate_expect_err("stop any foreground `flocal watch`")?;
    watch.stop()?;

    a.install_candidate()?;
    b.install_candidate()?;
    a.sync_start()?;
    a.sync_stop()?;
    let candidate_watch = a.watch_start()?;
    candidate_watch.wait_for_log("Peer connected")?;
    a.install_candidate_expect_err("stop any foreground `flocal watch`")?;
    candidate_watch.stop()?;
    a.install_candidate()?;
    a.sync_start()?;
    a.write(
        "after-refusal.txt",
        "old service and state were recoverable",
    )?;
    b.wait_for_file(
        "after-refusal.txt",
        "old service and state were recoverable",
    )?;
    e2e::assert_trees_equal(&a, &b)
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn failed_candidate_migration_is_retryable_without_state_repair() -> Result<()> {
    let (a, b) = e2e::upgrade_managed_pair()?;
    a.arm_migration_failure()?;
    a.install_candidate_expect_err("injected state migration failure")?;

    a.clear_migration_failure()?;
    a.install_candidate()?;
    b.install_candidate()?;
    b.write(
        "after-migration-retry.txt",
        "retry completed the interrupted upgrade",
    )?;
    a.wait_for_file(
        "after-migration-retry.txt",
        "retry completed the interrupted upgrade",
    )?;
    e2e::assert_trees_equal(&a, &b)
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn responder_first_legacy_upgrade_refuses_then_connector_first_succeeds() -> Result<()> {
    let (a, b) = e2e::upgrade_managed_pair()?;
    a.write("before-responder-first.txt", "base session is established")?;
    b.wait_for_file("before-responder-first.txt", "base session is established")?;
    b.install_candidate_expect_err("upgrade the connector first")?;

    a.install_candidate()?;
    b.install_candidate()?;
    a.write(
        "connector-first.txt",
        "safe retry preserved the relationship",
    )?;
    b.wait_for_file(
        "connector-first.txt",
        "safe retry preserved the relationship",
    )?;
    e2e::assert_trees_equal(&a, &b)
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn fresh_install_uses_the_same_owned_installer_path() -> Result<()> {
    let (a, b) = e2e::fresh_installed_pair()?;
    b.write("fresh-install.txt", "managed candidate installation")?;
    a.wait_for_file("fresh-install.txt", "managed candidate installation")?;
    e2e::assert_trees_equal(&a, &b)
}
