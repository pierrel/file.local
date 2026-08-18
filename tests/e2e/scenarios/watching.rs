//! Catalog #13 (watch notices an editor-style atomic-rename save), #14
//! (watch catches up unattended after the peer goes offline and returns),
//! plus committed-action reporting and process-suspension recovery.
//! The scenarios exercise the persistent two-sided watcher through real SSH;
//! their `wait_*` primitives are assertions with a deadline.

use anyhow::Result;

use crate::harness as e2e;

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn daemon_managed_sync_starts_stops_and_restores_watch_ownership() -> Result<()> {
    let (a, b) = e2e::managed_pair()?;
    a.write("managed.txt", "first")?;
    b.wait_for_file("managed.txt", "first")?;

    a.sync_stop()?;
    a.write("stopped.txt", "must stay local while stopped")?;
    std::thread::sleep(std::time::Duration::from_secs(2));
    b.assert_absent("stopped.txt")?;

    a.sync_start()?;
    b.wait_for_file("stopped.txt", "must stay local while stopped")?;
    e2e::assert_trees_equal(&a, &b)
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn daemon_restart_restores_enabled_watches() -> Result<()> {
    let (a, b) = e2e::managed_pair()?;
    a.write("before-restart.txt", "first")?;
    b.wait_for_file("before-restart.txt", "first")?;
    a.restart_daemon()?;
    a.write("after-restart.txt", "restored watch")?;
    b.wait_for_file("after-restart.txt", "restored watch")?;
    e2e::assert_trees_equal(&a, &b)
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn persistent_watch_reuses_one_ssh_session_while_idle() -> Result<()> {
    let (a, b) = e2e::pair_with(e2e::Config {
        watch_max_session_bytes: None,
    })?;
    let sessions_before_watch = b.ssh_session_count()?;
    let watch = a.watch_start()?;

    b.write("remote-round.txt", "one persistent session")?;
    a.wait_for_file_promptly("remote-round.txt", "one persistent session")?;
    std::thread::sleep(std::time::Duration::from_secs(3));

    let watch_sessions = b.ssh_session_count()? - sessions_before_watch;
    anyhow::ensure!(
        watch_sessions == 1,
        "watch opened {watch_sessions} SSH sessions; expected one persistent session"
    );
    watch.stop()
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn watch_notices_an_atomic_rename_save() -> Result<()> {
    let (a, b) = e2e::pair_with(e2e::Config {
        watch_max_session_bytes: None,
    })?;
    a.write("notes.txt", "v1")?;
    a.sync()?;

    let watch = a.watch_start()?;
    a.write(".notes.txt.tmp", "v2")?; // editor safe-save: temp file...
    a.rename(".notes.txt.tmp", "notes.txt")?; // ...renamed over the original
    // The assertion: this fails with the dump if v2 never arrives by the
    // deadline, returning early — in which case the watcher is torn down by
    // `Watch`'s Drop backstop, not the `stop()` below. On success, `stop()`
    // terminates it and reports any teardown failure.
    b.wait_for_file("notes.txt", "v2")?;
    watch.stop()
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn watch_catches_up_unattended_after_the_peer_returns() -> Result<()> {
    let (a, b) = e2e::pair_with(e2e::Config {
        watch_max_session_bytes: None,
    })?;
    a.write("kept.txt", "stays")?;
    a.write("dropped.txt", "goes")?;
    a.sync()?;
    b.assert_file("dropped.txt", "goes")?;

    let watch = a.watch_start()?;
    b.offline()?;
    a.write("made-offline.txt", "while b was away")?;
    a.remove("dropped.txt")?;
    b.online()?;
    // Unattended catch-up: nothing drives a sync here — the running
    // watcher's own cycles must notice the reconnection and converge.
    b.wait_for_file("made-offline.txt", "while b was away")?;
    b.wait_absent("dropped.txt")?;
    watch.stop()?;
    e2e::assert_trees_equal(&a, &b)
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn watch_reports_only_applied_actions() -> Result<()> {
    let (a, _b) = e2e::pair_with(e2e::Config {
        watch_max_session_bytes: Some(1),
    })?;
    let watch = a.watch_start()?;
    a.write("from-awake.txt", "written before sleep")?;
    watch.wait_for_error("persistent round exceeds its cumulative transfer limit")?;
    // A watch action is a completion statement. The session byte limit
    // stopped this upload, so claiming it here is the reported bug.
    watch.assert_log_absent("UPLOAD from-awake.txt")?;
    watch.stop()
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn watch_does_not_remain_stuck_on_a_stale_install_intent() -> Result<()> {
    let (a, b) = e2e::pair_with(e2e::Config {
        watch_max_session_bytes: None,
    })?;
    a.write("race.txt", "base")?;
    a.sync()?;

    let watch = a.watch_start_with_apply_stop()?;
    let sessions = b.ssh_session_count()?;
    b.write("race.txt", "remote change")?;
    watch.wait_stopped()?;
    a.write("race.txt", "local raced change")?;
    watch.resume()?;

    watch.wait_for_log("UNSETTLED race.txt changed while synchronizing; recalculating now")?;
    a.assert_file("race.txt", "local raced change")?;
    b.wait_for_file("race.txt", "local raced change")?;
    watch.wait_for_log("SETTLED race.txt no longer blocks synchronization")?;
    anyhow::ensure!(
        !a.status()?.pending_install,
        "stale install intent remained pending"
    );
    anyhow::ensure!(
        a.status()?.unsettled.is_empty(),
        "settled path remained in durable status"
    );
    let conflicts = b.conflicts()?;
    let conflict = conflicts.expect_one("race.txt")?;
    anyhow::ensure!(
        b.restore_loser(&conflict.id)? == "remote change",
        "the responder did not retain its remote input as the conflict loser"
    );

    watch.assert_log_absent("a different install is already pending for this share")?;
    watch.assert_log_absent("Peer connection lost")?;
    anyhow::ensure!(
        b.ssh_session_count()? == sessions,
        "stale apply recovery opened a new SSH session"
    );
    watch.stop()
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn changing_path_does_not_block_a_stable_sibling() -> Result<()> {
    let (a, b) = e2e::pair_with(e2e::Config {
        watch_max_session_bytes: None,
    })?;
    a.write("changing.txt", "base")?;
    a.write("stable.txt", "base")?;
    a.sync()?;

    let watch = a.watch_start_with_apply_stops(2)?;
    let sessions = b.ssh_session_count()?;
    b.write("changing.txt", "remote")?;
    b.write("stable.txt", "stable remote")?;
    watch.wait_stopped()?;
    a.write("changing.txt", "local one")?;
    watch.resume_to_next_apply_stop()?;
    a.write("changing.txt", "local two")?;
    watch.resume()?;

    watch.wait_for_log("UNSETTLED changing.txt is still changing; stable paths will continue")?;
    a.wait_for_file("stable.txt", "stable remote")?;
    watch.wait_for_log("stable.txt")?;
    watch.wait_for_log("SETTLED changing.txt no longer blocks synchronization")?;
    b.wait_for_file("changing.txt", "local two")?;
    anyhow::ensure!(
        b.ssh_session_count()? == sessions,
        "unsettled-path recovery opened a new SSH session"
    );
    watch.assert_log_absent("Peer connection lost")?;
    watch.stop()
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn watch_recovers_when_peer_install_recovery_outlasts_a_frame_deadline() -> Result<()> {
    let (a, b) = e2e::pair_with(e2e::Config {
        watch_max_session_bytes: None,
    })?;
    a.write("recovery.txt", "base")?;
    a.sync()?;
    let watch = a.watch_start()?;
    watch.wait_for_log("Peer connected")?;

    b.arm_apply_stops(2)?;
    a.write("recovery.txt", "connector edit")?;
    let responder = b.wait_for_stopped_protocol_server()?;
    b.write("recovery.txt", "responder edit")?;
    let responder = b.resume_protocol_to_next_apply_stop(responder)?;
    watch.wait_for_log("UNSETTLED recovery.txt changed while synchronizing")?;

    anyhow::ensure!(
        a.status()?.unsettled == vec![b"recovery.txt".to_vec()],
        "connector did not persist the unsettled path"
    );
    anyhow::ensure!(
        b.status()?.pending_install,
        "responder did not retain the interrupted install"
    );
    b.arm_install_recovery_delay()?;
    let sessions_before_kill = b.ssh_session_count()?;
    b.kill_stopped_protocol_server(responder)?;

    watch.wait_for_log_within(
        "SETTLED recovery.txt no longer blocks synchronization",
        std::time::Duration::from_secs(90),
    )?;
    let connector_status = a.status()?;
    let responder_status = b.status()?;
    anyhow::ensure!(
        !connector_status.pending_install && connector_status.unsettled.is_empty(),
        "connector remained unsettled after recovery: {connector_status:?}"
    );
    anyhow::ensure!(
        !responder_status.pending_install && responder_status.unsettled.is_empty(),
        "responder retained recovery state: {responder_status:?}"
    );
    anyhow::ensure!(
        b.ssh_session_count()? == sessions_before_kill + 1,
        "the replacement SSH session did not survive install recovery"
    );
    a.assert_file("recovery.txt", "responder edit")?;
    b.assert_file("recovery.txt", "responder edit")?;
    e2e::assert_trees_equal(&a, &b)?;
    watch.stop()
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn watch_rescans_both_directions_after_process_suspension() -> Result<()> {
    let (a, b) = e2e::pair_with(e2e::Config {
        watch_max_session_bytes: None,
    })?;
    let watch = a.watch_start()?;
    watch.suspend()?;
    a.write("from-awake.txt", "written while watch was suspended")?;
    b.write("from-sleep.txt", "written while watch was suspended")?;
    std::thread::sleep(std::time::Duration::from_secs(2));
    watch.resume()?;

    b.wait_for_file("from-awake.txt", "written while watch was suspended")?;
    a.wait_for_file("from-sleep.txt", "written while watch was suspended")?;
    watch.wait_for_log("UPLOAD from-awake.txt")?;
    watch.wait_for_log("DOWNLOAD from-sleep.txt")?;
    e2e::assert_trees_equal(&a, &b)?;
    watch.stop()
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn watch_recovers_after_network_loss_during_process_suspension() -> Result<()> {
    let (a, b) = e2e::pair_with(e2e::Config {
        watch_max_session_bytes: None,
    })?;
    let watch = a.watch_start()?;
    watch.wait_for_log("Peer connected")?;
    let sessions_before_sleep = b.ssh_session_count()?;

    // Model a laptop sleeping while its network disappears: stop the watch
    // process, disconnect the peer's real Docker network, edit both mounted
    // trees, then restore process and network without an explicit sync.
    watch.suspend()?;
    b.offline()?;
    a.write("local-during-sleep.txt", "local offline edit")?;
    b.write("remote-during-sleep.txt", "remote offline edit")?;
    std::thread::sleep(std::time::Duration::from_secs(2));
    watch.resume()?;
    watch.wait_for_error_within(
        "synchronization failed; retrying in background",
        std::time::Duration::from_secs(60),
    )?;
    b.online()?;

    b.wait_for_file("local-during-sleep.txt", "local offline edit")?;
    a.wait_for_file("remote-during-sleep.txt", "remote offline edit")?;
    let sessions_after_recovery = b.ssh_session_count()?;
    anyhow::ensure!(
        sessions_after_recovery > sessions_before_sleep,
        "watch did not establish a new SSH session after network recovery"
    );
    e2e::assert_trees_equal(&a, &b)?;
    watch.stop()
}
