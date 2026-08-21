use std::os::fd::AsFd;

use anyhow::Result;

pub fn sync_file(file: &impl AsFd) -> Result<()> {
    #[cfg(target_os = "macos")]
    rustix::fs::fcntl_fullfsync(file)?;
    #[cfg(not(target_os = "macos"))]
    rustix::fs::fsync(file)?;
    Ok(())
}
