use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use flocal::state::State;
use tempfile::tempdir;

struct DaemonGuard {
    child: Option<std::process::Child>,
}

fn spawn_daemon(command: &mut Command) -> Result<DaemonGuard> {
    let child = command.spawn()?;
    Ok(DaemonGuard { child: Some(child) })
}

impl DaemonGuard {
    fn stop(mut self) -> Result<()> {
        self.stop_inner()
    }

    fn stop_inner(&mut self) -> Result<()> {
        let Some(mut child) = self.child.take() else {
            return Ok(());
        };
        let result = stop_daemon(&mut child);
        if result.is_err() {
            let _ = child.kill();
            let _ = child.wait();
        }
        result
    }
}

impl std::ops::Deref for DaemonGuard {
    type Target = std::process::Child;

    fn deref(&self) -> &Self::Target {
        self.child.as_ref().expect("daemon guard is disarmed")
    }
}

impl std::ops::DerefMut for DaemonGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.child.as_mut().expect("daemon guard is disarmed")
    }
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        let _ = self.stop_inner();
    }
}

fn stop_daemon(daemon: &mut std::process::Child) -> Result<()> {
    if let Some(status) = daemon.try_wait()? {
        anyhow::ensure!(status.success(), "daemon exited with {status}");
        return Ok(());
    }
    let status = Command::new("kill")
        .args(["-TERM", &daemon.id().to_string()])
        .status()?;
    anyhow::ensure!(status.success(), "kill failed with {status}");
    let deadline = Instant::now() + Duration::from_secs(12);
    loop {
        if let Some(status) = daemon.try_wait()? {
            anyhow::ensure!(status.success(), "daemon exited with {status}");
            return Ok(());
        }
        if Instant::now() >= deadline {
            let _ = daemon.kill();
            let _ = daemon.wait();
            anyhow::bail!("daemon did not stop within 12 seconds");
        }
        thread::sleep(Duration::from_millis(25));
    }
}

fn wait_until(mut condition: impl FnMut() -> Result<bool>, description: &str) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if condition()? {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(25));
    }
    anyhow::bail!("timed out waiting for {description}")
}

#[test]
fn daemon_serves_the_managed_sync_list_over_its_private_socket() -> Result<()> {
    let temporary = tempdir()?;
    let state = temporary.path().join("state");
    let binary = env!("CARGO_BIN_EXE_flocal");
    let daemon = spawn_daemon(
        Command::new(binary)
            .args(["daemon", "run"])
            .env("FLOCAL_STATE_DIR", &state)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    )?;
    let socket = state.join("run/daemon.sock");
    let deadline = Instant::now() + Duration::from_secs(5);
    while !socket.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    if !socket.exists() {
        anyhow::bail!("daemon did not create its control socket");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&socket)?.permissions().mode() & 0o777,
            0o600
        );
    }
    let output = Command::new(binary)
        .args(["sync", "list", "--json"])
        .env("FLOCAL_STATE_DIR", &state)
        .output()
        .context("running sync list")?;
    daemon.stop()?;
    assert!(output.status.success(), "{:?}", output);
    let listing: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(listing["schema"], 3);
    assert_eq!(listing["syncs"], serde_json::json!([]));
    Ok(())
}

#[test]
fn status_list_reads_stored_shares_without_starting_a_daemon() -> Result<()> {
    let temporary = tempdir()?;
    let state_dir = temporary.path().join("state");
    let root = temporary.path().join("root");
    std::fs::create_dir(&root)?;
    let mut state = State::open(&state_dir)?;
    let share = state.init_share(&root)?;
    state.set_peer(
        &share,
        &flocal::model::PeerConfig {
            peer_id: Some(flocal::model::PeerId("peer-stored-status".into())),
            relationship: None,
            host: "127.0.0.1".into(),
            remote_path: b"/remote".to_vec(),
            executable: "/bin/false".into(),
        },
    )?;
    state.set_watch_enabled(&share, true)?;
    drop(state);

    let output = Command::new(env!("CARGO_BIN_EXE_flocal"))
        .args(["status", "--list", "--json"])
        .env("FLOCAL_STATE_DIR", &state_dir)
        .output()?;
    assert!(output.status.success(), "{:?}", output);
    assert!(!state_dir.join("run/daemon.sock").exists());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["schema"], 1);
    assert_eq!(report["source"], "stored");
    assert_eq!(report["daemon"]["state"], "unavailable");
    assert_eq!(report["shares"].as_array().map(Vec::len), Some(1));
    assert_eq!(report["shares"][0]["share"], share.0);
    assert_eq!(report["shares"][0]["enabled"], true);
    assert_eq!(report["shares"][0]["connection_state"], "unknown");
    Ok(())
}

#[test]
fn status_list_reports_no_shares_without_creating_missing_state() -> Result<()> {
    let temporary = tempdir()?;
    let state_dir = temporary.path().join("missing-state");
    let output = Command::new(env!("CARGO_BIN_EXE_flocal"))
        .args(["status", "--list", "--json"])
        .env("FLOCAL_STATE_DIR", &state_dir)
        .output()?;
    assert!(output.status.success(), "{:?}", output);
    assert!(!state_dir.exists());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["source"], "stored");
    assert_eq!(report["shares"], serde_json::json!([]));
    Ok(())
}

#[test]
fn status_list_rejects_a_relative_state_directory() -> Result<()> {
    let output = Command::new(env!("CARGO_BIN_EXE_flocal"))
        .args(["status", "--list", "--json"])
        .env("FLOCAL_STATE_DIR", "relative-state")
        .output()?;
    assert!(!output.status.success(), "{output:?}");
    Ok(())
}

#[cfg(unix)]
#[test]
fn status_list_refuses_a_symlinked_daemon_run_directory() -> Result<()> {
    use std::os::unix::fs::symlink;

    let temporary = tempdir()?;
    let state_dir = temporary.path().join("state");
    State::open(&state_dir)?;
    let target = temporary.path().join("attacker-run");
    std::fs::create_dir(&target)?;
    symlink(&target, state_dir.join("run"))?;

    let output = Command::new(env!("CARGO_BIN_EXE_flocal"))
        .args(["status", "--list", "--json"])
        .env("FLOCAL_STATE_DIR", &state_dir)
        .output()?;
    assert!(!output.status.success(), "{output:?}");
    Ok(())
}

#[cfg(unix)]
fn run_status_list_with_fake_daemon(
    handler: impl FnOnce(std::os::unix::net::UnixStream) -> Result<()> + Send + 'static,
) -> Result<std::process::Output> {
    use std::io::Read;
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::UnixListener;

    let temporary = tempdir()?;
    let state_dir = temporary.path().join("state");
    drop(State::open(&state_dir)?);
    let run = state_dir.join("run");
    std::fs::create_dir(&run)?;
    std::fs::set_permissions(&run, std::fs::Permissions::from_mode(0o700))?;
    let socket = run.join("daemon.sock");
    let listener = UnixListener::bind(&socket)?;
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
    let server = thread::spawn(move || -> Result<()> {
        let (mut stream, _) = listener.accept()?;
        let mut request = Vec::new();
        let mut buffer = [0u8; 4096];
        loop {
            let count = stream.read(&mut buffer)?;
            anyhow::ensure!(count > 0, "status client closed before its request");
            request.extend_from_slice(&buffer[..count]);
            if request.contains(&b'\n') {
                break;
            }
        }
        handler(stream)
    });

    let output = Command::new(env!("CARGO_BIN_EXE_flocal"))
        .args(["status", "--list", "--json"])
        .env("FLOCAL_STATE_DIR", &state_dir)
        .output()?;
    server.join().expect("fake daemon server panicked")?;
    Ok(output)
}

#[cfg(unix)]
#[test]
fn status_list_rejects_a_malformed_live_daemon_reply() -> Result<()> {
    use std::io::Write;

    let output = run_status_list_with_fake_daemon(|mut stream| {
        stream.write_all(b"{not-json}\n")?;
        Ok(())
    })?;
    assert!(!output.status.success(), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("daemon sent an invalid response"),
        "{output:?}"
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn status_list_rejects_truncated_and_oversized_live_daemon_replies() -> Result<()> {
    use std::io::Write;

    for reply in [b"{\"partial\"".to_vec(), vec![b'x'; 64 * 1024 + 1]] {
        let output = run_status_list_with_fake_daemon(move |mut stream| {
            stream.write_all(&reply)?;
            Ok(())
        })?;
        assert!(!output.status.success(), "{output:?}");
        assert_eq!(output.stdout, b"");
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn status_list_falls_back_when_the_live_daemon_disconnects_or_times_out() -> Result<()> {
    for delay in [Duration::ZERO, Duration::from_millis(300)] {
        let output = run_status_list_with_fake_daemon(move |_stream| {
            if !delay.is_zero() {
                thread::sleep(delay);
            }
            Ok(())
        })?;
        assert!(output.status.success(), "{output:?}");
        let report: serde_json::Value = serde_json::from_slice(&output.stdout)?;
        assert_eq!(report["source"], "stored");
        assert_eq!(report["daemon"]["state"], "unavailable");
    }
    Ok(())
}

#[test]
fn status_list_reads_live_daemon_state() -> Result<()> {
    let temporary = tempdir()?;
    let state_dir = temporary.path().join("state");
    let root = temporary.path().join("root");
    std::fs::create_dir(&root)?;
    let mut state = State::open(&state_dir)?;
    let share = state.init_share(&root)?;
    state.set_peer(
        &share,
        &flocal::model::PeerConfig {
            peer_id: Some(flocal::model::PeerId("peer-status-list".into())),
            relationship: None,
            host: "127.0.0.1".into(),
            remote_path: b"/remote".to_vec(),
            executable: "/bin/false".into(),
        },
    )?;
    state.set_initial_complete(&share)?;
    state.set_watch_enabled(&share, true)?;
    drop(state);

    let binary = env!("CARGO_BIN_EXE_flocal");
    let daemon = spawn_daemon(
        Command::new(binary)
            .args(["daemon", "run"])
            .env("FLOCAL_STATE_DIR", &state_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    )?;
    wait_until(
        || Ok(state_dir.join("run/daemon.sock").exists()),
        "daemon control socket",
    )?;
    let output = Command::new(binary)
        .args(["status", "--list", "--json"])
        .env("FLOCAL_STATE_DIR", &state_dir)
        .output()?;
    daemon.stop()?;
    assert!(output.status.success(), "{:?}", output);
    let report: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["source"], "live");
    assert_eq!(report["daemon"]["state"], "live");
    assert_eq!(report["shares"][0]["share"], share.0);
    assert_eq!(report["shares"][0]["enabled"], true);
    Ok(())
}

#[test]
fn running_daemon_recovers_a_managed_install_created_after_startup() -> Result<()> {
    let temporary = tempdir()?;
    let state_dir = temporary.path().join("state");
    let startup_root = temporary.path().join("startup-root");
    let late_root = temporary.path().join("late-root");
    std::fs::create_dir(&startup_root)?;
    std::fs::create_dir(&late_root)?;
    let mut state = State::open(&state_dir)?;
    let startup_share = state.init_share(&startup_root)?;
    state.set_install_intent(&startup_share, &[])?;
    let late_share = state.init_share(&late_root)?;
    state.set_peer(
        &late_share,
        &flocal::model::PeerConfig {
            peer_id: Some(flocal::model::PeerId("peer-late".into())),
            relationship: None,
            host: "127.0.0.1".into(),
            remote_path: b"/remote".to_vec(),
            executable: "/bin/false".into(),
        },
    )?;
    let recovery_baseline = state.scheduling_snapshot()?.completion_sequence;
    drop(state);

    let binary = env!("CARGO_BIN_EXE_flocal");
    let daemon = spawn_daemon(
        Command::new(binary)
            .args(["daemon", "run"])
            .env("FLOCAL_STATE_DIR", &state_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    )?;
    let socket = state_dir.join("run/daemon.sock");
    wait_until(|| Ok(socket.exists()), "daemon control socket")?;
    wait_until(
        || {
            Ok(State::open(&state_dir)?
                .install_intent(&startup_share)?
                .is_none())
        },
        "startup install recovery",
    )?;
    wait_until(
        || {
            Ok(State::open(&state_dir)?
                .scheduling_snapshot()?
                .completion_sequence
                > recovery_baseline)
        },
        "startup recovery permit completion",
    )?;

    State::open(&state_dir)?.set_managed_plan_install_intent(&late_share, &[], &[], 0)?;
    wait_until(
        || {
            Ok(State::open(&state_dir)?
                .install_intent(&late_share)?
                .is_none())
        },
        "post-startup managed install recovery",
    )?;

    let state = State::open(&state_dir)?;
    let managed = state.managed_share(&late_share)?;
    assert!(managed.initial_complete);
    assert!(managed.watch_enabled);
    daemon.stop()
}

#[test]
fn daemon_stop_disables_a_share_durably() -> Result<()> {
    let temporary = tempdir()?;
    let state_dir = temporary.path().join("state");
    let root = temporary.path().join("root");
    std::fs::create_dir(&root)?;
    let state = State::open(&state_dir)?;
    let share = state.init_share(&root)?;
    state.set_initial_complete(&share)?;
    state.set_watch_enabled(&share, true)?;
    drop(state);

    let binary = env!("CARGO_BIN_EXE_flocal");
    let daemon = spawn_daemon(
        Command::new(binary)
            .args(["daemon", "run"])
            .env("FLOCAL_STATE_DIR", &state_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    )?;
    let socket = state_dir.join("run/daemon.sock");
    let deadline = Instant::now() + Duration::from_secs(5);
    while !socket.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    let output = Command::new(binary)
        .args(["sync", "stop", root.to_str().context("test root is utf-8")?])
        .env("FLOCAL_STATE_DIR", &state_dir)
        .output()?;
    daemon.stop()?;
    assert!(output.status.success(), "{:?}", output);
    assert!(
        !State::open(&state_dir)?
            .managed_share(&share)?
            .watch_enabled
    );
    Ok(())
}

#[cfg(not(target_os = "macos"))]
#[test]
fn daemon_cli_follows_paginated_sync_lists() -> Result<()> {
    #[cfg(target_os = "macos")]
    let temporary = tempfile::Builder::new()
        .prefix("f")
        .tempdir_in("/private/tmp")?;
    #[cfg(not(target_os = "macos"))]
    let temporary = tempfile::Builder::new().prefix("f").tempdir_in("/tmp")?;
    let state_dir = temporary.path().join("state");
    let mut state = State::open(&state_dir)?;
    for index in 0..20 {
        let root = temporary.path().join(format!("root-{index}"));
        std::fs::create_dir(&root)?;
        let share = state.init_share(&root)?;
        state.set_peer(
            &share,
            &flocal::model::PeerConfig {
                peer_id: Some(flocal::model::PeerId(format!("peer-{index}"))),
                relationship: None,
                host: "test-peer".into(),
                remote_path: b"/remote".to_vec(),
                executable: "/bin/false".into(),
            },
        )?;
        state.set_blocked(&share, &"x".repeat(4096))?;
    }
    drop(state);

    let binary = env!("CARGO_BIN_EXE_flocal");
    let daemon = spawn_daemon(
        Command::new(binary)
            .args(["daemon", "run"])
            .env("FLOCAL_STATE_DIR", &state_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    )?;
    let socket = state_dir.join("run/daemon.sock");
    let deadline = Instant::now() + Duration::from_secs(5);
    while !socket.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    let output = Command::new(binary)
        .args(["sync", "list", "--json"])
        .env("FLOCAL_STATE_DIR", &state_dir)
        .output()?;
    daemon.stop()?;
    assert!(output.status.success(), "{:?}", output);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout)?["syncs"]
            .as_array()
            .map(Vec::len),
        Some(20)
    );
    Ok(())
}

#[test]
fn daemon_control_cli_reports_missing_or_responder_only_shares() -> Result<()> {
    let temporary = tempdir()?;
    let state_dir = temporary.path().join("state");
    let root = temporary.path().join("root");
    std::fs::create_dir(&root)?;
    let state = State::open(&state_dir)?;
    let share = state.init_share(&root)?;
    drop(state);

    let binary = env!("CARGO_BIN_EXE_flocal");
    let daemon = spawn_daemon(
        Command::new(binary)
            .args(["daemon", "run"])
            .env("FLOCAL_STATE_DIR", &state_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    )?;
    let socket = state_dir.join("run/daemon.sock");
    let deadline = Instant::now() + Duration::from_secs(5);
    while !socket.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }

    let missing = Command::new(binary)
        .args(["sync", "start", "--share", "missing"])
        .env("FLOCAL_STATE_DIR", &state_dir)
        .output()?;
    assert!(!missing.status.success());
    let responder = Command::new(binary)
        .args([
            "sync",
            "start",
            root.to_str().context("test root is utf-8")?,
        ])
        .env("FLOCAL_STATE_DIR", &state_dir)
        .output()?;
    let conflicting_selector = Command::new(binary)
        .args([
            "sync",
            "start",
            root.to_str().context("test root is utf-8")?,
            "--share",
            &share.0,
        ])
        .env("FLOCAL_STATE_DIR", &state_dir)
        .output()?;
    let not_a_directory = temporary.path().join("not-a-directory");
    std::fs::write(&not_a_directory, "file")?;
    let invalid_root = Command::new(binary)
        .args([
            "sync",
            "add",
            not_a_directory.to_str().context("test path is utf-8")?,
            "--host",
            "test-peer",
            "--remote-path",
            "/remote",
            "--yes",
        ])
        .env("FLOCAL_STATE_DIR", &state_dir)
        .output()?;
    daemon.stop()?;
    assert!(!responder.status.success());
    assert!(!conflicting_selector.status.success());
    assert!(!invalid_root.status.success());
    Ok(())
}

#[cfg(unix)]
#[test]
fn daemon_guard_drop_forces_and_reaps_an_unresponsive_remote() -> Result<()> {
    let temporary = tempdir()?;
    let state_dir = temporary.path().join("state");
    let root = temporary.path().join("root");
    let bin_dir = temporary.path().join("bin");
    let started = temporary.path().join("remote-started");
    std::fs::create_dir(&root)?;
    std::fs::create_dir(&bin_dir)?;
    let fake_ssh = bin_dir.join("ssh");
    std::fs::write(
        &fake_ssh,
        r#"#!/bin/sh
printf '%s\n' "$$" > "$FLOCAL_TEST_REMOTE_STARTED"
exec sleep 600
"#,
    )?;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&fake_ssh, std::fs::Permissions::from_mode(0o755))?;

    let binary = env!("CARGO_BIN_EXE_flocal");
    let mut state = State::open(&state_dir)?;
    let share = state.init_share(&root)?;
    state.set_peer(
        &share,
        &flocal::model::PeerConfig {
            peer_id: Some(flocal::model::PeerId("peer-test".into())),
            relationship: Some(flocal::model::RelationshipId::generate()),
            host: "test-peer".into(),
            remote_path: b"/remote".to_vec(),
            executable: binary.into(),
        },
    )?;
    state.set_initial_complete(&share)?;
    state.set_watch_enabled(&share, true)?;
    drop(state);

    let path = format!(
        "{}:{}",
        bin_dir.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let mut daemon = spawn_daemon(
        Command::new(binary)
            .args(["daemon", "run"])
            .env("FLOCAL_STATE_DIR", &state_dir)
            .env("FLOCAL_TEST_REMOTE_STARTED", &started)
            .env("PATH", path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    )?;
    let mut remote_pid = None;
    let remote_started = wait_until(
        || {
            remote_pid = std::fs::read_to_string(&started)
                .ok()
                .and_then(|pid| pid.trim().parse::<u32>().ok())
                .filter(|pid| *pid > 0);
            Ok(remote_pid.is_some())
        },
        "unresponsive remote",
    );
    if remote_started.is_err() {
        let status = daemon.try_wait()?;
        let listing = Command::new(binary)
            .args(["sync", "list", "--json"])
            .env("FLOCAL_STATE_DIR", &state_dir)
            .output()?;
        anyhow::bail!(
            "unresponsive remote did not start (socket: {}, daemon status: {status:?}, syncs: {})",
            state_dir.join("run/daemon.sock").exists(),
            String::from_utf8_lossy(&listing.stdout),
        );
    }
    let remote_pid = remote_pid.context("unresponsive remote did not report its PID")?;

    let started = Instant::now();
    drop(daemon);
    assert!(started.elapsed() >= Duration::from_secs(10));
    wait_until(
        || {
            Ok(!Command::new("kill")
                .args(["-0", &remote_pid.to_string()])
                .stderr(Stdio::null())
                .status()?
                .success())
        },
        "unresponsive remote cleanup",
    )
}

#[test]
fn sync_add_pairs_then_starts_and_stops_a_managed_watch() -> Result<()> {
    let temporary = tempdir()?;
    let local_root = temporary.path().join("local");
    let remote_root = temporary.path().join("remote");
    let local_state = temporary.path().join("local-state");
    let remote_state = temporary.path().join("remote-state");
    let bin_dir = temporary.path().join("bin");
    std::fs::create_dir_all(&local_root)?;
    std::fs::create_dir_all(&remote_root)?;
    std::fs::create_dir(&bin_dir)?;
    let fake_ssh = bin_dir.join("ssh");
    let fake_ssh_script = r#"#!/bin/sh
for arg do last=$arg; done
case "$last" in
*"command -v flocal"*)
  printf '%s\n' "$FLOCAL_BIN"
  exit 0
  ;;
*"protocol relationship"*)
  exec env FLOCAL_STATE_DIR="$FAKE_REMOTE_STATE" "$FLOCAL_BIN" protocol relationship
  ;;
esac
exec env LLVM_PROFILE_FILE=/dev/null FLOCAL_STATE_DIR="$FAKE_REMOTE_STATE" "$FLOCAL_BIN" protocol serve
"#;
    std::fs::write(&fake_ssh, fake_ssh_script)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&fake_ssh, std::fs::Permissions::from_mode(0o755))?;
    }

    let binary = env!("CARGO_BIN_EXE_flocal");
    let path = format!(
        "{}:{}",
        bin_dir.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let daemon = spawn_daemon(
        Command::new(binary)
            .args(["daemon", "run"])
            .env("FLOCAL_STATE_DIR", &local_state)
            .env("FAKE_REMOTE_STATE", &remote_state)
            .env("FLOCAL_BIN", binary)
            .env("PATH", &path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    )?;
    let socket = local_state.join("run/daemon.sock");
    let deadline = Instant::now() + Duration::from_secs(5);
    while !socket.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    if !socket.exists() {
        anyhow::bail!("daemon did not create its control socket");
    }

    let invoke = |arguments: &[&str]| {
        Command::new(binary)
            .args(arguments)
            .env("FLOCAL_STATE_DIR", &local_state)
            .env("FAKE_REMOTE_STATE", &remote_state)
            .env("FLOCAL_BIN", binary)
            .env("PATH", &path)
            .output()
    };
    let add = invoke(&[
        "sync",
        "add",
        local_root.to_str().context("test root is utf-8")?,
        "--host",
        "test-peer",
        "--remote-path",
        remote_root.to_str().context("test root is utf-8")?,
        "--yes",
    ])?;
    if !add.status.success() {
        anyhow::bail!("sync add failed: {}", String::from_utf8_lossy(&add.stderr));
    }
    let add_stdout = String::from_utf8_lossy(&add.stdout);
    assert!(add_stdout.contains("Connected "));
    assert!(!add_stdout.contains("flocal: scanning local files"));
    let phases = String::from_utf8_lossy(&add.stderr);
    assert!(phases.contains("flocal: scanning local files"), "{phases}");
    assert!(
        phases.contains("flocal: waiting for the remote file scan"),
        "{phases}"
    );
    assert!(phases.contains("flocal: comparing file lists"), "{phases}");

    let stop_before_decline = invoke(&[
        "sync",
        "stop",
        local_root.to_str().context("test root is utf-8")?,
    ])?;
    assert!(
        stop_before_decline.status.success(),
        "{:?}",
        stop_before_decline
    );

    let declined_root = temporary.path().join("declined-local");
    let declined_remote = temporary.path().join("declined-remote");
    std::fs::create_dir(&declined_root)?;
    std::fs::create_dir(&declined_remote)?;
    let declined = Command::new(binary)
        .args([
            "sync",
            "add",
            declined_root.to_str().context("test root is utf-8")?,
            "--host",
            "test-peer",
            "--remote-path",
            declined_remote.to_str().context("test root is utf-8")?,
        ])
        .env("FLOCAL_STATE_DIR", &local_state)
        .env("FAKE_REMOTE_STATE", &remote_state)
        .env("FLOCAL_BIN", binary)
        .env("PATH", &path)
        .stdin(Stdio::null())
        .output()?;
    assert!(declined.status.success(), "{:?}", declined);
    assert!(
        !String::from_utf8_lossy(&declined.stdout).contains("Connected "),
        "{:?}",
        declined
    );
    assert!(
        !String::from_utf8_lossy(&declined.stdout).contains("flocal: scanning local files"),
        "{:?}",
        declined
    );
    let remove_declined = invoke(&[
        "sync",
        "remove",
        declined_root.to_str().context("test root is utf-8")?,
        "--local-only",
        "--yes",
    ])?;
    assert!(remove_declined.status.success(), "{:?}", remove_declined);
    let repeat = invoke(&[
        "sync",
        "add",
        local_root.to_str().context("test root is utf-8")?,
        "--host",
        "test-peer",
        "--remote-path",
        remote_root.to_str().context("test root is utf-8")?,
        "--yes",
    ])?;
    assert!(repeat.status.success(), "{:?}", repeat);
    let nested = local_root.join("nested");
    std::fs::create_dir(&nested)?;
    let nested_add = invoke(&[
        "sync",
        "add",
        nested.to_str().context("test root is utf-8")?,
        "--host",
        "test-peer",
        "--remote-path",
        remote_root.to_str().context("test root is utf-8")?,
        "--yes",
    ])?;
    assert!(!nested_add.status.success());
    assert!(String::from_utf8_lossy(&nested_add.stderr).contains("inside an existing share"));
    let conflicting = invoke(&[
        "sync",
        "add",
        local_root.to_str().context("test root is utf-8")?,
        "--host",
        "other-peer",
        "--remote-path",
        remote_root.to_str().context("test root is utf-8")?,
        "--yes",
    ])?;
    assert!(!conflicting.status.success());
    let listing = invoke(&["sync", "list", "--json"])?;
    assert!(listing.status.success(), "{:?}", listing);
    let syncs = serde_json::from_slice::<serde_json::Value>(&listing.stdout)?;
    assert_eq!(syncs["syncs"].as_array().map(Vec::len), Some(1));
    assert_eq!(syncs["syncs"][0]["enabled"], true);
    assert_eq!(syncs["syncs"][0]["initial_complete"], true);
    assert_eq!(syncs["syncs"][0]["role"], "connector");
    let share = syncs["syncs"][0]["share"]
        .as_str()
        .context("managed share has an id")?
        .to_owned();
    let listing = invoke(&["sync", "list"])?;
    assert!(listing.status.success(), "{:?}", listing);
    assert!(String::from_utf8_lossy(&listing.stdout).contains("enabled"));
    let start = invoke(&[
        "sync",
        "start",
        local_root.to_str().context("test root is utf-8")?,
    ])?;
    assert!(start.status.success(), "{:?}", start);

    daemon.stop()?;
    let renamed_root = temporary.path().join("renamed-local");
    std::fs::rename(&local_root, &renamed_root)?;
    let daemon = spawn_daemon(
        Command::new(binary)
            .args(["daemon", "run"])
            .env("FLOCAL_STATE_DIR", &local_state)
            .env("FAKE_REMOTE_STATE", &remote_state)
            .env("FLOCAL_BIN", binary)
            .env("PATH", &path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    )?;
    let deadline = Instant::now() + Duration::from_secs(5);
    let blocked = loop {
        let listing = invoke(&["sync", "list", "--json"])?;
        if listing.status.success() {
            let syncs = serde_json::from_slice::<serde_json::Value>(&listing.stdout)?;
            if syncs["syncs"][0]["state"] == "blocked" {
                break syncs;
            }
        }
        if Instant::now() >= deadline {
            anyhow::bail!("renamed managed root did not become blocked");
        }
        std::thread::sleep(Duration::from_millis(25));
    };
    assert!(
        blocked["syncs"][0]["diagnostic"]
            .as_str()
            .is_some_and(|diagnostic| !diagnostic.is_empty())
    );

    let restart = invoke(&["sync", "start", "--share", &share])?;
    assert!(!restart.status.success());
    let stop = invoke(&["sync", "stop", "--share", &share])?;
    if !stop.status.success() {
        anyhow::bail!(
            "sync stop failed: {}",
            String::from_utf8_lossy(&stop.stderr)
        );
    }
    std::fs::rename(&renamed_root, &local_root)?;
    let restart = invoke(&["sync", "start", "--share", &share])?;
    assert!(restart.status.success(), "{:?}", restart);
    std::fs::write(local_root.join("after-root-restore"), "restored")?;
    let deadline = Instant::now() + Duration::from_secs(5);
    while std::fs::read_to_string(remote_root.join("after-root-restore"))
        .ok()
        .as_deref()
        != Some("restored")
    {
        if Instant::now() >= deadline {
            anyhow::bail!("restored root did not resume synchronization");
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    std::fs::write(&fake_ssh, "#!/bin/sh\nexit 1\n")?;
    let offline_remove = invoke(&[
        "sync",
        "remove",
        local_root.to_str().context("test root is utf-8")?,
        "--yes",
    ])?;
    assert!(!offline_remove.status.success());
    let pending_status = invoke(&[
        "status",
        local_root.to_str().context("test root is utf-8")?,
        "--json",
    ])?;
    assert!(pending_status.status.success(), "{:?}", pending_status);
    let pending_status: serde_json::Value = serde_json::from_slice(&pending_status.stdout)?;
    assert_eq!(pending_status["relationship_state"], "removing");
    assert_eq!(pending_status["removal_pending"], true);
    std::fs::write(&fake_ssh, fake_ssh_script)?;
    let removed = invoke(&[
        "sync",
        "remove",
        local_root.to_str().context("test root is utf-8")?,
        "--yes",
    ])?;
    assert!(removed.status.success(), "{:?}", removed);
    let listing = invoke(&["sync", "list", "--json"])?;
    assert!(listing.status.success(), "{:?}", listing);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&listing.stdout)?["syncs"],
        serde_json::json!([])
    );
    for (state_dir, root) in [(&local_state, &local_root), (&remote_state, &remote_root)] {
        let status = Command::new(binary)
            .args([
                "status",
                root.to_str().context("test root is utf-8")?,
                "--json",
            ])
            .env("FLOCAL_STATE_DIR", state_dir)
            .output()?;
        assert!(status.status.success(), "{:?}", status);
        let status: serde_json::Value = serde_json::from_slice(&status.stdout)?;
        assert_eq!(status["schema"], 6);
        assert_eq!(status["relationship_state"], "unpaired");
        assert_eq!(status["removal_pending"], false);
    }
    daemon.stop()
}
