use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use flocal::state::State;
use tempfile::tempdir;

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
    let _ = daemon.kill();
    let _ = daemon.wait();
    assert!(output.status.success(), "{:?}", output);
    let listing: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(listing["schema"], 1);
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
    let _ = daemon.kill();
    let _ = daemon.wait();
    assert!(output.status.success(), "{:?}", output);
    assert!(
        !State::open(&state_dir)?
            .managed_share(&share)?
            .watch_enabled
    );
    Ok(())
}
