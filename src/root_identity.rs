//! Descriptor-bound filesystem identity. Numeric device IDs remain runtime guards.

use std::fs::File;

use anyhow::{Result, bail};

pub(crate) fn filesystem_identity(file: &File) -> Result<Option<String>> {
    platform_identity(file)
}

fn identifier(prefix: &str, bytes: &[u8]) -> Option<String> {
    if bytes.iter().all(|byte| *byte == 0) {
        return None;
    }
    Some(format!(
        "{prefix}:{}",
        bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    ))
}

#[cfg(target_os = "linux")]
fn platform_identity(file: &File) -> Result<Option<String>> {
    use std::os::fd::AsRawFd;

    let fs = rustix::fs::fstatfs(file)?;
    // Layered filesystems can keep a UUID while replacing their lower view.
    // Only these native backends have the required volume/inode semantics.
    if ![
        libc::EXT4_SUPER_MAGIC as u64,
        libc::XFS_SUPER_MAGIC as u64,
        libc::BTRFS_SUPER_MAGIC as u64,
    ]
    .contains(&(fs.f_type as u64))
    {
        return Ok(None);
    }
    // Linux 6.9 added the descriptor-based FS_IOC_GETFSUUID query.
    let mut value = [0_u8; 17];
    const GET_FS_UUID: rustix::ioctl::Opcode = rustix::ioctl::opcode::read::<[u8; 17]>(0x15, 0);
    // SAFETY: the documented ioctl writes struct fsuuid2 (u8 length, 16 bytes)
    // into this initialized 17-byte buffer; the borrowed File keeps fd alive.
    let result = unsafe {
        libc::ioctl(
            file.as_raw_fd(),
            GET_FS_UUID as libc::c_ulong,
            value.as_mut_ptr(),
        )
    };
    if result < 0 {
        let error = std::io::Error::last_os_error();
        return match error.raw_os_error() {
            Some(libc::ENOTTY | libc::EOPNOTSUPP | libc::ENOSYS) => Ok(None),
            _ => Err(error.into()),
        };
    }
    let length = usize::from(value[0]);
    if length == 0 || length > 16 {
        bail!("filesystem returned an invalid UUID length");
    }
    let Some(uuid) = identifier("linux-uuid-v1", &value[1..=length]) else {
        return Ok(None);
    };
    // Btrfs inode numbers are local to a subvolume; its fsid includes that ID.
    let subvolume = if fs.f_type as u64 == libc::BTRFS_SUPER_MAGIC as u64 {
        format!(":{:016x}", rustix::fs::fstatvfs(file)?.f_fsid)
    } else {
        String::new()
    };
    Ok(Some(format!("{uuid}:{:x}{subvolume}", fs.f_type)))
}

#[cfg(target_os = "macos")]
fn platform_identity(file: &File) -> Result<Option<String>> {
    use std::os::fd::AsRawFd;

    let mut attributes = libc::attrlist {
        bitmapcount: libc::ATTR_BIT_MAP_COUNT,
        reserved: 0,
        commonattr: libc::ATTR_CMN_RETURNED_ATTRS,
        volattr: libc::ATTR_VOL_INFO | libc::ATTR_VOL_UUID,
        dirattr: 0,
        fileattr: 0,
        forkattr: 0,
    };
    // getattrlist packs a u32 length, five returned u32 masks, then UUID bytes.
    let mut buffer = [0_u8; 40];
    // SAFETY: attrlist is initialized; the output buffer is writable for its
    // exact advertised length, and the borrowed File keeps the descriptor alive.
    let result = unsafe {
        libc::fgetattrlist(
            file.as_raw_fd(),
            std::ptr::from_mut(&mut attributes).cast(),
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            0,
        )
    };
    if result < 0 {
        let error = std::io::Error::last_os_error();
        return match error.raw_os_error() {
            Some(libc::ENOTSUP) => Ok(None),
            _ => Err(error.into()),
        };
    }
    decode_volume_uuid(&buffer)
}

#[cfg(target_os = "macos")]
fn decode_volume_uuid(buffer: &[u8; 40]) -> Result<Option<String>> {
    let length = u32::from_ne_bytes(buffer[..4].try_into()?) as usize;
    if !(24..=buffer.len()).contains(&length) {
        bail!("filesystem returned an invalid volume attribute length");
    }
    let common = u32::from_ne_bytes(buffer[4..8].try_into()?);
    let volume = u32::from_ne_bytes(buffer[8..12].try_into()?);
    if common & libc::ATTR_CMN_RETURNED_ATTRS == 0 {
        bail!("filesystem omitted returned volume attribute masks");
    }
    if volume & libc::ATTR_VOL_UUID == 0 {
        return Ok(None);
    }
    if length != buffer.len() {
        bail!("filesystem returned a truncated volume UUID");
    }
    Ok(identifier("macos-volume-v1", &buffer[24..40]))
}
