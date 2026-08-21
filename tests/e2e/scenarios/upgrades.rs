//! Base-revision to candidate installation and migration scenarios.

use anyhow::Result;

use crate::harness as e2e;

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn idle_managed_pair_survives_a_real_candidate_install() -> Result<()> {
    let (a, b) = e2e::upgrade_managed_pair()?;
    a.write("before-upgrade.txt", "created by the base connector")?;
    b.wait_for_file("before-upgrade.txt", "created by the base connector")?;

    a.install_candidate()?;
    b.install_candidate()?;

    a.write("after-upgrade-a.txt", "candidate connector resumed")?;
    b.write("after-upgrade-b.txt", "candidate responder resumed")?;
    b.wait_for_file("after-upgrade-a.txt", "candidate connector resumed")?;
    a.wait_for_file("after-upgrade-b.txt", "candidate responder resumed")?;
    b.install_candidate()?;
    a.write("after-compatible-reinstall.txt", "responder drained itself")?;
    b.wait_for_file("after-compatible-reinstall.txt", "responder drained itself")?;
    e2e::assert_trees_equal(&a, &b)
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn in_progress_managed_sync_survives_a_real_candidate_install() -> Result<()> {
    let (a, b) = e2e::upgrade_managed_pair()?;
    a.arm_apply_stops(1)?;
    b.write("during-upgrade.txt", "interrupted base apply")?;
    let stopped = a.wait_for_stopped_apply_process()?;

    a.install_candidate()?;
    drop(stopped);
    b.install_candidate()?;

    a.wait_for_file("during-upgrade.txt", "interrupted base apply")?;
    a.write("after-upgrade.txt", "candidate session resumed")?;
    b.wait_for_file("after-upgrade.txt", "candidate session resumed")?;
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
