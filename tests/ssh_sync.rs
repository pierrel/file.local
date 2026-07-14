#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;

use anyhow::{Context, Result};
use flocal::state::State;
use tempfile::tempdir;

#[test]
fn bidirectional_sync_over_ssh_process_boundary() -> Result<()> {
    let temp = tempdir()?;
    let local_root = temp.path().join("local");
    let remote_root = temp.path().join("remote");
    let local_state = temp.path().join("local-state");
    let remote_state = temp.path().join("remote-state");
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&bin_dir)?;
    let fake_ssh = bin_dir.join("ssh");
    fs::write(
        &fake_ssh,
        r#"#!/bin/sh
for arg do last=$arg; done
if [ "$last" = "command -v flocal" ]; then
  printf '%s\n' "$FLOCAL_BIN"
  exit 0
fi
exec env FLOCAL_STATE_DIR="$FAKE_REMOTE_STATE" "$FLOCAL_BIN" protocol serve
"#,
    )?;
    fs::set_permissions(&fake_ssh, fs::Permissions::from_mode(0o755))?;

    let binary = env!("CARGO_BIN_EXE_flocal");
    let path = format!(
        "{}:{}",
        bin_dir.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let run = |arguments: &[&str]| -> Result<()> {
        let status = Command::new(binary)
            .args(arguments)
            .env("FLOCAL_STATE_DIR", &local_state)
            .env("FAKE_REMOTE_STATE", &remote_state)
            .env("FLOCAL_BIN", binary)
            .env("PATH", &path)
            .status()?;
        if !status.success() {
            anyhow::bail!("flocal {:?} failed", arguments);
        }
        Ok(())
    };

    run(&["init", local_root.to_str().context("utf8 local root")?])?;
    run(&[
        "peer",
        "add",
        local_root.to_str().unwrap(),
        "--host",
        "test-peer",
        "--remote-path",
        remote_root.to_str().unwrap(),
    ])?;
    fs::write(local_root.join("from-local.txt"), "local")?;
    run(&["sync", local_root.to_str().unwrap(), "--dry-run", "--yes"])?;
    let local_database = State::open(&local_state)?;
    let (share, _) = local_database.find_share(&local_root)?;
    assert!(local_database.records(&share)?.is_empty());
    let remote_database = State::open(&remote_state)?;
    assert!(remote_database.records(&share)?.is_empty());
    assert_eq!(fs::read_dir(local_state.join("objects"))?.count(), 0);
    assert_eq!(fs::read_dir(remote_state.join("objects"))?.count(), 0);
    drop(local_database);
    drop(remote_database);
    run(&["sync", local_root.to_str().unwrap(), "--yes"])?;
    assert_eq!(
        fs::read_to_string(remote_root.join("from-local.txt"))?,
        "local"
    );

    fs::write(remote_root.join("from-remote.txt"), "remote")?;
    run(&["sync", local_root.to_str().unwrap(), "--yes"])?;
    assert_eq!(
        fs::read_to_string(local_root.join("from-remote.txt"))?,
        "remote"
    );
    Ok(())
}
