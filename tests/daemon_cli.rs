use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use flocal::state::State;
use tempfile::tempdir;

fn stop_daemon(daemon: &mut std::process::Child) -> Result<()> {
    let status = Command::new("kill")
        .args(["-TERM", &daemon.id().to_string()])
        .status()?;
    assert!(status.success(), "kill failed with {status}");
    let deadline = Instant::now() + Duration::from_secs(12);
    loop {
        if let Some(status) = daemon.try_wait()? {
            assert!(status.success(), "daemon exited with {status}");
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

#[test]
fn daemon_serves_the_managed_sync_list_over_its_private_socket() -> Result<()> {
    let temporary = tempdir()?;
    let state = temporary.path().join("state");
    let binary = env!("CARGO_BIN_EXE_flocal");
    let mut daemon = Command::new(binary)
        .args(["daemon", "run"])
        .env("FLOCAL_STATE_DIR", &state)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let socket = state.join("run/daemon.sock");
    let deadline = Instant::now() + Duration::from_secs(5);
    while !socket.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    if !socket.exists() {
        let _ = daemon.kill();
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
    stop_daemon(&mut daemon)?;
    assert!(output.status.success(), "{:?}", output);
    let listing: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(listing["schema"], 2);
    assert_eq!(listing["syncs"], serde_json::json!([]));
    Ok(())
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
    let mut daemon = Command::new(binary)
        .args(["daemon", "run"])
        .env("FLOCAL_STATE_DIR", &state_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let socket = state_dir.join("run/daemon.sock");
    let deadline = Instant::now() + Duration::from_secs(5);
    while !socket.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    let output = Command::new(binary)
        .args(["sync", "stop", root.to_str().context("test root is utf-8")?])
        .env("FLOCAL_STATE_DIR", &state_dir)
        .output()?;
    stop_daemon(&mut daemon)?;
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
    let mut daemon = Command::new(binary)
        .args(["daemon", "run"])
        .env("FLOCAL_STATE_DIR", &state_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let socket = state_dir.join("run/daemon.sock");
    let deadline = Instant::now() + Duration::from_secs(5);
    while !socket.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    let output = Command::new(binary)
        .args(["sync", "list", "--json"])
        .env("FLOCAL_STATE_DIR", &state_dir)
        .output()?;
    stop_daemon(&mut daemon)?;
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
    let mut daemon = Command::new(binary)
        .args(["daemon", "run"])
        .env("FLOCAL_STATE_DIR", &state_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
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
    stop_daemon(&mut daemon)?;
    assert!(!responder.status.success());
    assert!(!conflicting_selector.status.success());
    assert!(!invalid_root.status.success());
    Ok(())
}

#[cfg(unix)]
#[test]
fn daemon_sigterm_forces_and_reaps_an_unresponsive_remote() -> Result<()> {
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
: > "$FLOCAL_TEST_REMOTE_STARTED"
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
            relationship: None,
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
    let mut daemon = Command::new(binary)
        .args(["daemon", "run"])
        .env("FLOCAL_STATE_DIR", &state_dir)
        .env("FLOCAL_TEST_REMOTE_STARTED", &started)
        .env("PATH", path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let deadline = Instant::now() + Duration::from_secs(5);
    while !started.exists() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(25));
    }
    if !started.exists() {
        let status = daemon.try_wait()?;
        let listing = Command::new(binary)
            .args(["sync", "list", "--json"])
            .env("FLOCAL_STATE_DIR", &state_dir)
            .output()?;
        let _ = daemon.kill();
        let _ = daemon.wait();
        anyhow::bail!(
            "unresponsive remote did not start (socket: {}, daemon status: {status:?}, syncs: {})",
            state_dir.join("run/daemon.sock").exists(),
            String::from_utf8_lossy(&listing.stdout),
        );
    }

    let started = Instant::now();
    stop_daemon(&mut daemon)?;
    assert!(started.elapsed() >= Duration::from_secs(10));
    Ok(())
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
    let mut daemon = Command::new(binary)
        .args(["daemon", "run"])
        .env("FLOCAL_STATE_DIR", &local_state)
        .env("FAKE_REMOTE_STATE", &remote_state)
        .env("FLOCAL_BIN", binary)
        .env("PATH", &path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let socket = local_state.join("run/daemon.sock");
    let deadline = Instant::now() + Duration::from_secs(5);
    while !socket.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    if !socket.exists() {
        let _ = daemon.kill();
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
        let _ = daemon.kill();
        let _ = daemon.wait();
        anyhow::bail!("sync add failed: {}", String::from_utf8_lossy(&add.stderr));
    }
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

    stop_daemon(&mut daemon)?;
    let renamed_root = temporary.path().join("renamed-local");
    std::fs::rename(&local_root, &renamed_root)?;
    let mut daemon = Command::new(binary)
        .args(["daemon", "run"])
        .env("FLOCAL_STATE_DIR", &local_state)
        .env("FAKE_REMOTE_STATE", &remote_state)
        .env("FLOCAL_BIN", binary)
        .env("PATH", &path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
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
            let _ = daemon.kill();
            let _ = daemon.wait();
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
        let _ = daemon.kill();
        let _ = daemon.wait();
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
            let _ = daemon.kill();
            let _ = daemon.wait();
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
        assert_eq!(status["schema"], 5);
        assert_eq!(status["relationship_state"], "unpaired");
        assert_eq!(status["removal_pending"], false);
    }
    stop_daemon(&mut daemon)
}
