#![cfg(target_os = "macos")]

use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Result, ensure};
use flocal::{
    state::State,
    sync::{self, ShareRoot},
};

struct MountedImage {
    image: PathBuf,
    mount: PathBuf,
}

impl MountedImage {
    fn new(parent: &Path, name: &str) -> Result<Self> {
        let image = Self {
            image: parent.join(format!("{name}.dmg")),
            mount: parent.join(name),
        };
        fs::create_dir(&image.mount)?;
        image.command(
            &[
                "create",
                "-size",
                "64m",
                "-fs",
                "APFS",
                "-volname",
                "flocal-test",
            ],
            &image.image,
        )?;
        image.attach()?;
        Ok(image)
    }

    fn command(&self, args: &[&str], path: &Path) -> Result<()> {
        let output = Command::new("hdiutil").args(args).arg(path).output()?;
        ensure!(
            output.status.success(),
            "hdiutil {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
    }

    fn attach(&self) -> Result<()> {
        let output = Command::new("hdiutil")
            .args(["attach", "-nobrowse", "-mountpoint"])
            .arg(&self.mount)
            .arg(&self.image)
            .output()?;
        ensure!(
            output.status.success(),
            "hdiutil attach: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
    }
    fn detach(&self) -> Result<()> {
        self.command(&["detach", "-force"], &self.mount)
    }
}

impl Drop for MountedImage {
    fn drop(&mut self) {
        let _ = Command::new("hdiutil")
            .args(["detach", "-force"])
            .arg(&self.mount)
            .output();
    }
}

#[test]
fn apfs_volume_reattachment_preserves_enrolled_directory_identity() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let image = MountedImage::new(temp.path(), "original")?;
    let root = image.mount.join("share");
    fs::create_dir(&root)?;
    fs::write(root.join("marker"), "original contents")?;
    let state_dir = temp.path().join("state");
    let mut state = State::open(&state_dir)?;
    let share = state.init_share(&root)?;
    let records = sync::refresh(&mut state, &share)?;
    let (intent, _) = state.set_install_intent(&share, &records)?;
    state.mark_install_temp_creating(&share, &intent.temps[0].path)?;
    fs::write(root.join(&intent.temps[0].token), "original contents")?;
    let old_capability = ShareRoot::open(&state, &share)?;
    let before = fs::metadata(&root)?;
    drop(state);
    image.detach()?;
    let _spacer = MountedImage::new(temp.path(), "spacer")?;
    image.attach()?;
    let after = fs::metadata(&root)?;
    ensure!(
        before.dev() != after.dev(),
        "test did not renumber the volume"
    );
    ensure!(before.ino() == after.ino(), "test replaced the directory");
    let mut state = State::open(&state_dir)?;
    ensure!(
        old_capability.validate(&state, &share).is_err(),
        "a stale capability must fail before any separate device refresh"
    );
    ensure!(state.validate_root_identity(&share)?.device == after.dev());
    ShareRoot::open(&state, &share)?.validate(&state, &share)?;
    ensure!(state.install_intent(&share)?.is_some());
    sync::apply_plan(&mut state, &share, &records)?;
    ensure!(state.install_intent(&share)?.is_none());
    ensure!(state.records(&share)? == records);
    ensure!(fs::read_to_string(root.join("marker"))? == "original contents");
    fs::write(root.join("later-edit"), "captured after remount")?;
    ensure!(sync::refresh(&mut state, &share)?.len() == 2);
    fs::rename(&root, image.mount.join("old-share"))?;
    fs::create_dir(&root)?;
    ensure!(state.validate_root_identity(&share).is_err());
    Ok(())
}
