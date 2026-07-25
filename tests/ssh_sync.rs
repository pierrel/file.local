#![cfg(unix)]

use std::fs;
use std::io::BufRead;
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

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
case "$last" in
"command -v flocal"*)
  printf '%s\n' "$FLOCAL_BIN"
  exit 0
  ;;
esac
exec env FLOCAL_STATE_DIR="$FAKE_REMOTE_STATE" FLOCAL_MAX_SESSION_BYTES="$FAKE_REMOTE_MAX_SESSION_BYTES" "$FLOCAL_BIN" protocol serve
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
            .env("FAKE_REMOTE_MAX_SESSION_BYTES", "10737418240")
            .env("PATH", &path)
            .status()?;
        if !status.success() {
            anyhow::bail!("flocal {:?} failed", arguments);
        }
        Ok(())
    };
    let run_fails = |arguments: &[&str]| -> Result<()> {
        let status = Command::new(binary)
            .args(arguments)
            .env("FLOCAL_STATE_DIR", &local_state)
            .env("FAKE_REMOTE_STATE", &remote_state)
            .env("FLOCAL_BIN", binary)
            .env("FAKE_REMOTE_MAX_SESSION_BYTES", "10737418240")
            .env("PATH", &path)
            .status()?;
        anyhow::ensure!(
            !status.success(),
            "flocal {:?} unexpectedly succeeded",
            arguments
        );
        Ok(())
    };
    let run_limited_fails = |arguments: &[&str], local_limit: &str| -> Result<()> {
        let status = Command::new(binary)
            .args(arguments)
            .env("FLOCAL_STATE_DIR", &local_state)
            .env("FLOCAL_MAX_SESSION_BYTES", local_limit)
            .env("FAKE_REMOTE_STATE", &remote_state)
            .env("FAKE_REMOTE_MAX_SESSION_BYTES", "10737418240")
            .env("FLOCAL_BIN", binary)
            .env("PATH", &path)
            .status()?;
        anyhow::ensure!(!status.success(), "limited sync unexpectedly succeeded");
        Ok(())
    };
    let run_with_limit = |arguments: &[&str], local_limit: &str| -> Result<()> {
        let status = Command::new(binary)
            .args(arguments)
            .env("FLOCAL_STATE_DIR", &local_state)
            .env("FLOCAL_MAX_SESSION_BYTES", local_limit)
            .env("FAKE_REMOTE_STATE", &remote_state)
            .env("FAKE_REMOTE_MAX_SESSION_BYTES", "10737418240")
            .env("FLOCAL_BIN", binary)
            .env("PATH", &path)
            .status()?;
        anyhow::ensure!(status.success(), "limited sync unexpectedly failed");
        Ok(())
    };

    run(&["init", local_root.to_str().context("utf8 local root")?])?;
    let unpaired = temp.path().join("unpaired");
    run(&["init", unpaired.to_str().unwrap()])?;
    run(&["peer", "list", unpaired.to_str().unwrap()])?;
    run_fails(&["watch", unpaired.to_str().unwrap()])?;
    run_fails(&[
        "peer",
        "add",
        unpaired.to_str().unwrap(),
        "--host",
        "-invalid",
        "--remote-path",
        remote_root.to_str().unwrap(),
    ])?;
    run_fails(&[
        "peer",
        "add",
        unpaired.to_str().unwrap(),
        "--host",
        "test-peer",
        "--remote-path",
        "relative",
    ])?;
    run(&[
        "peer",
        "add",
        local_root.to_str().unwrap(),
        "--host",
        "test-peer",
        "--remote-path",
        remote_root.to_str().unwrap(),
    ])?;
    run(&["peer", "list", local_root.to_str().unwrap()])?;
    run(&["peer", "list", local_root.to_str().unwrap(), "--json"])?;
    run(&["status", local_root.to_str().unwrap()])?;
    run(&["status", local_root.to_str().unwrap(), "--json"])?;
    run(&["conflicts", "list", local_root.to_str().unwrap()])?;
    run(&["conflicts", "list", local_root.to_str().unwrap(), "--json"])?;
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

    fs::write(local_root.join("from-local.txt"), "local edit")?;
    fs::write(remote_root.join("from-local.txt"), "remote edit")?;
    run(&["sync", local_root.to_str().unwrap(), "--yes", "--json"])?;
    let local_database = State::open(&local_state)?;
    let conflicts = local_database.conflicts(&share)?;
    let conflict = conflicts.first().context("expected a conflict")?;
    run(&["conflicts", "list", local_root.to_str().unwrap()])?;
    run(&["conflicts", "list", local_root.to_str().unwrap(), "--json"])?;
    run(&[
        "conflicts",
        "show",
        local_root.to_str().unwrap(),
        &conflict.id,
    ])?;
    run(&[
        "conflicts",
        "show",
        local_root.to_str().unwrap(),
        &conflict.id,
        "--json",
    ])?;
    let restored = temp.path().join("restored.txt");
    run(&[
        "restore",
        local_root.to_str().unwrap(),
        &conflict.id,
        "--version",
        "loser",
        "--to",
        restored.to_str().unwrap(),
    ])?;
    assert!(restored.exists());
    run_fails(&[
        "restore",
        local_root.to_str().unwrap(),
        &conflict.id,
        "--version",
        "winner",
        "--to",
        restored.to_str().unwrap(),
    ])?;
    let directory_destination = temp.path().join("restore-directory");
    fs::create_dir_all(&directory_destination)?;
    run_fails(&[
        "restore",
        local_root.to_str().unwrap(),
        &conflict.id,
        "--version",
        "winner",
        "--to",
        directory_destination.to_str().unwrap(),
        "--force",
    ])?;
    let symlink_destination = temp.path().join("restore-symlink");
    std::os::unix::fs::symlink(&restored, &symlink_destination)?;
    run_fails(&[
        "restore",
        local_root.to_str().unwrap(),
        &conflict.id,
        "--version",
        "winner",
        "--to",
        symlink_destination.to_str().unwrap(),
        "--force",
    ])?;
    assert!(symlink_destination.is_symlink());
    run(&[
        "restore",
        local_root.to_str().unwrap(),
        &conflict.id,
        "--version",
        "winner",
        "--to",
        restored.to_str().unwrap(),
        "--force",
    ])?;

    fs::write(remote_root.join("boundary-remote-a"), "123")?;
    fs::write(remote_root.join("boundary-remote-b"), "456")?;
    run_with_limit(&["sync", local_root.to_str().unwrap(), "--yes"], "6")?;
    fs::write(remote_root.join("cumulative-remote-a"), "789")?;
    fs::write(remote_root.join("cumulative-remote-b"), "012")?;
    run_limited_fails(&["sync", local_root.to_str().unwrap(), "--yes"], "5")?;
    assert!(!local_root.join("cumulative-remote-a").exists());
    assert!(!local_root.join("cumulative-remote-b").exists());

    fs::remove_file(remote_root.join("cumulative-remote-a"))?;
    fs::remove_file(remote_root.join("cumulative-remote-b"))?;
    run(&["sync", local_root.to_str().unwrap(), "--yes"])?;
    fs::write(local_root.join("boundary-local-a"), "abc")?;
    fs::write(local_root.join("boundary-local-b"), "def")?;
    run_with_limit(&["sync", local_root.to_str().unwrap(), "--yes"], "6")?;
    fs::write(local_root.join("cumulative-local-a"), "ghi")?;
    fs::write(local_root.join("cumulative-local-b"), "jkl")?;
    run_limited_fails(&["sync", local_root.to_str().unwrap(), "--yes"], "5")?;
    assert!(!remote_root.join("cumulative-local-a").exists());
    assert!(!remote_root.join("cumulative-local-b").exists());

    // Drive the persistent connector and responder through the fake SSH
    // process boundary, then edit in both directions without another command.
    let mut watcher = Command::new(binary)
        .args(["watch", local_root.to_str().unwrap()])
        .env("FLOCAL_STATE_DIR", &local_state)
        .env("FAKE_REMOTE_STATE", &remote_state)
        .env("FLOCAL_BIN", binary)
        .env("FAKE_REMOTE_MAX_SESSION_BYTES", "10737418240")
        .env("PATH", &path)
        .stdout(Stdio::piped())
        .spawn()?;
    let stdout = watcher.stdout.take().context("watch stdout")?;
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
    let ready_reader = std::thread::spawn(move || -> Result<()> {
        let mut stdout = std::io::BufReader::new(stdout);
        let mut first_line = String::new();
        stdout.read_line(&mut first_line)?;
        let mut connected_line = String::new();
        loop {
            connected_line.clear();
            anyhow::ensure!(
                stdout.read_line(&mut connected_line)? != 0,
                "watch exited before ready"
            );
            if connected_line.ends_with(" Peer connected\n") {
                ready_tx
                    .send(first_line)
                    .map_err(|_| anyhow::anyhow!("watch readiness receiver disappeared"))?;
                std::io::copy(&mut stdout, &mut std::io::sink())?;
                return Ok(());
            }
        }
    });
    let first_line = match ready_rx.recv_timeout(std::time::Duration::from_secs(10)) {
        Ok(line) => line,
        Err(error) => {
            watcher.kill()?;
            watcher.wait()?;
            let reader = ready_reader
                .join()
                .unwrap_or_else(|_| Err(anyhow::anyhow!("watch readiness reader panicked")));
            anyhow::bail!("watch did not become ready within 10 seconds: {error}; {reader:?}");
        }
    };

    fs::write(remote_root.join("persistent-remote"), "from responder")?;
    wait_for_contents(&local_root.join("persistent-remote"), b"from responder")?;
    fs::write(local_root.join("persistent-local"), "from connector")?;
    wait_for_contents(&remote_root.join("persistent-local"), b"from connector")?;
    // End watch through its real terminal root-identity path rather than
    // SIGKILL, so subprocess coverage and teardown both flush deterministically.
    let displaced_root = temp.path().join("displaced-remote-root");
    fs::rename(&remote_root, &displaced_root)?;
    fs::create_dir(&remote_root)?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let status = loop {
        if let Some(status) = watcher.try_wait()? {
            break status;
        }
        if std::time::Instant::now() >= deadline {
            watcher.kill()?;
            watcher.wait()?;
            anyhow::bail!("watch did not stop after its registered root identity changed");
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    };
    assert!(!status.success(), "root identity change is terminal");
    ready_reader
        .join()
        .map_err(|_| anyhow::anyhow!("watch readiness reader panicked"))??;
    let (timestamp, rest) = first_line
        .trim_end_matches('\n')
        .split_once(' ')
        .context("expected a timestamped watch startup line")?;
    assert_eq!(timestamp.len(), 20, "{timestamp:?}");
    assert!(
        timestamp.char_indices().all(|(i, c)| match i {
            4 | 7 => c == '-',
            10 => c == 'T',
            13 | 16 => c == ':',
            19 => c == 'Z',
            _ => c.is_ascii_digit(),
        }),
        "{timestamp:?} is not a YYYY-MM-DDTHH:MM:SSZ timestamp"
    );
    // flocal reports the canonicalized share root; on macOS that resolves
    // /var's symlink to /private/var, which the raw tempdir path does not.
    assert_eq!(
        rest,
        format!("Watching {}", local_root.canonicalize()?.display())
    );
    Ok(())
}

fn wait_for_contents(path: &std::path::Path, expected: &[u8]) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if fs::read(path).is_ok_and(|contents| contents == expected) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!("{} did not converge before the deadline", path.display());
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}
