//! Installation scheduling when one user configures multiple shares.

use anyhow::{Context, Result};

use crate::harness as e2e;

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn second_share_add_waits_for_an_active_round_without_manual_retry() -> Result<()> {
    let (a, b) = e2e::managed_pair()?;
    a.sync_stop()?;
    a.init_second_share()?;
    a.write_second("setup.txt", "one setup command")?;

    let watch = a.watch_start_with_apply_stop()?;
    a.write("owner.txt", "first share holds the installation")?;
    watch.wait_stopped()?;
    a.arm_scheduling_wait_observation()?;

    let (add, release) = std::thread::scope(|scope| {
        let release = scope.spawn(|| {
            let observed = a.wait_for_scheduling_wait();
            let not_enabled = a.assert_second_sync_not_enabled();
            let absent = b.assert_second_absent("setup.txt");
            let resumed = watch.resume();
            observed?;
            not_enabled?;
            absent?;
            resumed
        });
        let add = a.sync_add_second_observed(&b);
        let release = release
            .join()
            .map_err(|_| anyhow::anyhow!("contention observer thread panicked"))?;
        Ok::<_, anyhow::Error>((add, release))
    })?;
    release?;
    let plan = add?;
    anyhow::ensure!(plan.contains("UPLOAD setup.txt"));
    a.assert_sync_add_queue_feedback(&plan)?;

    b.wait_for_file("owner.txt", "first share holds the installation")?;
    b.wait_for_second_file("setup.txt", "one setup command")?;
    watch.stop()?;
    a.assert_second_sync_enabled()?;
    a.sync_stop_second()
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn confirmed_second_share_start_queues_behind_an_active_round() -> Result<()> {
    let (a, b) = e2e::managed_pair()?;
    a.sync_stop()?;
    a.init_second_share()?;
    a.write_second("bootstrap.txt", "confirmed before contention")?;
    a.sync_add_second_to(&b)?;
    a.sync_stop_second()?;
    a.write_second("queued.txt", "started once while another share is active")?;

    let watch = a.watch_start_with_apply_stop()?;
    a.write("owner.txt", "first share still finishes")?;
    watch.wait_stopped()?;

    let start = a.sync_start_second_observed();
    let queued = a.assert_second_sync_queued();
    let absent = b.assert_second_absent("queued.txt");
    let preflight = (|| {
        let output = start?;
        queued?;
        absent?;
        a.assert_second_sync_start_queue_feedback(&output)
    })();
    let resumed = watch.resume();
    if let Err(error) = preflight {
        let stopped = watch.stop();
        resumed?;
        stopped?;
        return Err(error);
    }
    resumed?;

    b.wait_for_file("owner.txt", "first share still finishes")?;
    b.wait_for_second_file("queued.txt", "started once while another share is active")?;
    watch.stop()?;
    a.sync_stop_second()
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn queued_managed_stop_cancels_before_release() -> Result<()> {
    let (a, b) = e2e::managed_pair()?;
    a.sync_stop()?;
    a.init_second_share()?;
    a.write_second("bootstrap.txt", "confirmed before cancellation")?;
    a.sync_add_second_to(&b)?;
    a.sync_stop_second()?;
    a.write_second("canceled.txt", "must never reach the peer")?;

    let watch = a.watch_start_with_apply_stop()?;
    a.write("owner.txt", "active first share")?;
    watch.wait_stopped()?;
    a.sync_start_second_observed()?;
    a.assert_second_sync_queued()?;
    b.assert_second_absent("canceled.txt")?;

    a.sync_stop_second()?;
    a.assert_second_sync_stopped()?;
    watch.resume()?;
    b.wait_for_file("owner.txt", "active first share")?;
    watch.stop()?;

    // A later permit acquisition proves the canceled request had a chance to
    // run after the active owner released, rather than relying on a sleep.
    a.sync()?;
    b.assert_second_absent("canceled.txt")
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn persistent_watch_requeues_behind_an_older_managed_start() -> Result<()> {
    let (a, b) = e2e::managed_pair()?;
    a.sync_stop()?;
    a.init_second_share()?;
    a.write_second("bootstrap.txt", "confirmed before fairness check")?;
    a.sync_add_second_to(&b)?;
    a.sync_stop_second()?;
    a.write_second("fairness.txt", "older queued share runs first")?;

    let watch = a.watch_start_with_apply_stops(2)?;
    a.write("round-one.txt", "first watch round")?;
    watch.wait_stopped()?;
    a.sync_start_second_observed()?;
    a.wait_for_second_sync_queued_behind("/home/peer/share")?;

    // Round two becomes pending while round one still owns the installation.
    // The persistent watch must rejoin behind the already-queued second share.
    a.write("round-two.txt", "second watch round")?;

    watch.resume_for_next_apply_stop()?;
    let older = a.wait_for_stopped_apply_process()?;
    a.wait_for_sync_queued_behind("/home/peer/second-share")?;
    b.assert_absent("round-two.txt")?;

    older.resume()?;
    b.wait_for_second_file("fairness.txt", "older queued share runs first")?;
    a.wait_for_second_sync_idle()?;
    b.wait_for_file("round-one.txt", "first watch round")?;
    b.wait_for_file("round-two.txt", "second watch round")?;
    watch.stop()?;
    a.sync_stop_second()
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn daemon_crash_reconstructs_a_queued_managed_start() -> Result<()> {
    let (a, b) = e2e::managed_pair()?;
    a.sync_stop()?;
    a.init_second_share()?;
    a.write_second("bootstrap.txt", "confirmed before daemon crash")?;
    a.sync_add_second_to(&b)?;
    a.sync_stop_second()?;
    a.write_second("reconstructed.txt", "restored from durable intent")?;

    let watch = a.watch_start_with_apply_stop()?;
    a.write("owner.txt", "permit survives daemon crash")?;
    watch.wait_stopped()?;
    a.sync_start_second_observed()?;
    a.assert_second_sync_queued()?;
    b.assert_second_absent("reconstructed.txt")?;

    a.crash_and_restart_daemon()?;
    a.assert_second_sync_queued()?;
    b.assert_second_absent("reconstructed.txt")?;

    watch.resume()?;
    b.wait_for_file("owner.txt", "permit survives daemon crash")?;
    b.wait_for_second_file("reconstructed.txt", "restored from durable intent")?;
    watch.stop()?;
    a.sync_stop_second()
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn simultaneous_opposite_direction_starts_serialize_and_converge() -> Result<()> {
    let (a, b) = e2e::managed_pair()?;
    a.sync_stop()?;

    b.init_second_share()?;
    b.write_second("bootstrap.txt", "opposite connector confirmed")?;
    b.sync_add_second_to(&a)?;
    b.sync_stop_second()?;

    a.write("a-to-b.txt", "first relationship")?;
    b.write_second("b-to-a.txt", "opposite relationship")?;
    let a_id = b
        .second_status()?
        .bound_peer
        .context("opposite relationship did not expose peer A's ID")?;
    let b_id = a
        .status()?
        .bound_peer
        .context("primary relationship did not expose peer B's ID")?;
    let held = if a_id > b_id {
        b.hold_installation()?
    } else {
        a.hold_installation()?
    };
    a.arm_reservation_stop()?;
    b.arm_reservation_stop()?;

    let (first, opposite) = std::thread::scope(|scope| {
        let (first, opposite, first_worker, opposite_worker) = if a_id > b_id {
            let first = scope.spawn(|| a.sync_start_observed());
            let first_worker = a.wait_for_stopped_reservation_worker()?;
            let opposite = scope.spawn(|| b.sync_start_second_observed());
            let opposite_worker = match b.wait_for_stopped_reservation_worker() {
                Ok(worker) => worker,
                Err(error) => {
                    let _ = a.resume_reservation_worker(first_worker);
                    return Err(error);
                }
            };
            (first, opposite, first_worker, opposite_worker)
        } else {
            let opposite = scope.spawn(|| b.sync_start_second_observed());
            let opposite_worker = b.wait_for_stopped_reservation_worker()?;
            let first = scope.spawn(|| a.sync_start_observed());
            let first_worker = match a.wait_for_stopped_reservation_worker() {
                Ok(worker) => worker,
                Err(error) => {
                    let _ = b.resume_reservation_worker(opposite_worker);
                    return Err(error);
                }
            };
            (first, opposite, first_worker, opposite_worker)
        };
        let observed = (|| {
            a.assert_sync_durably_queued()?;
            b.assert_second_sync_durably_queued()
        })();
        let first_resumed = a.resume_reservation_worker(first_worker);
        let opposite_resumed = b.resume_reservation_worker(opposite_worker);
        observed?;
        first_resumed?;
        opposite_resumed?;
        let first = first
            .join()
            .map_err(|_| anyhow::anyhow!("first start thread panicked"))?;
        let opposite = opposite
            .join()
            .map_err(|_| anyhow::anyhow!("opposite start thread panicked"))?;
        Ok::<_, anyhow::Error>((first, opposite))
    })?;
    first?;
    opposite?;

    a.wait_for_opposite_scheduling_contention(&b)?;
    std::thread::sleep(flocal::sync::reservation_lease() + std::time::Duration::from_secs(1));
    a.wait_for_opposite_scheduling_contention(&b)?;
    held.resume()?;

    let convergence = (|| {
        b.wait_for_file("a-to-b.txt", "first relationship")?;
        a.wait_for_second_file("b-to-a.txt", "opposite relationship")
    })();
    let first_stopped = a.sync_stop();
    let opposite_stopped = b.sync_stop_second();
    convergence?;
    first_stopped?;
    opposite_stopped
}
