use std::io::{Read, Write};
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::model::{Entry, ObjectHash, PeerId, Record, RelativePath, ShareId};
use crate::reconcile::{Plan, reconcile};
use crate::scan::{IgnoreMatcher, preview, scan};
use crate::state::{InstallTempPhase, State};

pub const MAX_FRAME: usize = 2 * 1024 * 1024;
pub const PROTOCOL_VERSION: u32 = 1;
pub const MAX_RECORDS_PER_SESSION: usize = 1_000_000;
pub const MAX_METADATA_BYTES_PER_SESSION: usize = 256 * 1024 * 1024;
pub fn max_transfer_bytes_per_session() -> u64 {
    std::env::var("FLOCAL_MAX_SESSION_BYTES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(10 * 1024 * 1024 * 1024)
}
const CHUNK: usize = 256 * 1024;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    Register {
        protocol: u32,
        share: ShareId,
        peer: PeerId,
        root: Vec<u8>,
    },
    Sync {
        protocol: u32,
        share: ShareId,
        peer: PeerId,
        dry_run: bool,
    },
    Accepted {
        protocol: u32,
        peer: PeerId,
    },
    SnapshotChunk {
        records: Vec<Record>,
    },
    SnapshotEnd,
    Need {
        hashes: Vec<ObjectHash>,
    },
    ObjectStart {
        hash: ObjectHash,
        size: u64,
    },
    ObjectChunk {
        data: Vec<u8>,
    },
    ObjectEnd,
    ApplyChunk {
        records: Vec<Record>,
        conflicts: Vec<crate::reconcile::Conflict>,
    },
    ApplyEnd,
    Applied,
    Cancel,
    Done,
    Error {
        message: String,
    },
}

pub fn write_message(writer: &mut impl Write, message: &Message) -> Result<()> {
    let bytes = serde_json::to_vec(message)?;
    if bytes.len() > MAX_FRAME {
        bail!("protocol frame too large");
    }
    writer.write_all(&(bytes.len() as u32).to_be_bytes())?;
    writer.write_all(&bytes)?;
    writer.flush()?;
    Ok(())
}

pub fn read_message(reader: &mut impl Read) -> Result<Message> {
    let mut length = [0u8; 4];
    reader.read_exact(&mut length)?;
    let length = u32::from_be_bytes(length) as usize;
    if length > MAX_FRAME {
        bail!("protocol frame too large");
    }
    let mut bytes = vec![0u8; length];
    reader.read_exact(&mut bytes)?;
    Ok(serde_json::from_slice(&bytes)?)
}

pub fn write_snapshot(writer: &mut impl Write, records: &[Record]) -> Result<()> {
    let envelope = serde_json::to_vec(&Message::SnapshotChunk {
        records: Vec::new(),
    })?
    .len();
    for chunk in bounded_chunks(records, envelope, "snapshot record")? {
        write_message(
            writer,
            &Message::SnapshotChunk {
                records: chunk.to_vec(),
            },
        )?;
    }
    write_message(writer, &Message::SnapshotEnd)
}

pub fn read_snapshot(reader: &mut impl Read) -> Result<Vec<Record>> {
    let mut records = Vec::new();
    let mut metadata_bytes = 0usize;
    loop {
        match read_message(reader)? {
            Message::SnapshotChunk { records: chunk } => {
                metadata_bytes = metadata_bytes.saturating_add(serde_json::to_vec(&chunk)?.len());
                if metadata_bytes > MAX_METADATA_BYTES_PER_SESSION {
                    bail!("snapshot exceeds session metadata limit");
                }
                records.extend(chunk);
            }
            Message::SnapshotEnd => return Ok(records),
            other => bail!("expected snapshot, got {other:?}"),
        }
        if records.len() > MAX_RECORDS_PER_SESSION {
            bail!("snapshot exceeds session record limit");
        }
    }
}

pub fn write_plan(writer: &mut impl Write, plan: &Plan) -> Result<()> {
    write_record_chunks(writer, &plan.records)?;
    write_conflict_chunks(writer, &plan.conflicts)?;
    write_message(writer, &Message::ApplyEnd)
}

fn write_record_chunks(writer: &mut impl Write, records: &[Record]) -> Result<()> {
    let envelope = serde_json::to_vec(&Message::ApplyChunk {
        records: Vec::new(),
        conflicts: Vec::new(),
    })?
    .len();
    for chunk in bounded_chunks(records, envelope, "plan record")? {
        write_message(
            writer,
            &Message::ApplyChunk {
                records: chunk.to_vec(),
                conflicts: Vec::new(),
            },
        )?;
    }
    Ok(())
}

fn write_conflict_chunks(
    writer: &mut impl Write,
    conflicts: &[crate::reconcile::Conflict],
) -> Result<()> {
    let envelope = serde_json::to_vec(&Message::ApplyChunk {
        records: Vec::new(),
        conflicts: Vec::new(),
    })?
    .len();
    for chunk in bounded_chunks(conflicts, envelope, "plan conflict")? {
        write_message(
            writer,
            &Message::ApplyChunk {
                records: Vec::new(),
                conflicts: chunk.to_vec(),
            },
        )?;
    }
    Ok(())
}

fn bounded_chunks<'a, T: serde::Serialize>(
    items: &'a [T],
    empty_envelope_size: usize,
    item_name: &str,
) -> Result<Vec<&'a [T]>> {
    let mut chunks = Vec::new();
    let mut start = 0;
    while start < items.len() {
        let mut end = start;
        let mut frame_size = empty_envelope_size;
        while end < items.len() {
            let separator = usize::from(end > start);
            let next_size = frame_size
                .saturating_add(separator)
                .saturating_add(serde_json::to_vec(&items[end])?.len());
            if next_size > MAX_FRAME {
                break;
            }
            frame_size = next_size;
            end += 1;
        }
        if end == start {
            bail!("single {item_name} exceeds protocol limit");
        }
        chunks.push(&items[start..end]);
        start = end;
    }
    Ok(chunks)
}

pub fn refresh(state: &mut State, share: &ShareId) -> Result<Vec<Record>> {
    let root = state.root_for(share)?;
    let previous = state.records(share)?;
    let records = scan(state, share, &root, &previous)?;
    state.replace_records(share, &records)?;
    advertised_records(&root, &records)
}

pub fn preview_refresh(state: &State, share: &ShareId) -> Result<Vec<Record>> {
    let root = state.root_for(share)?;
    advertised_records(
        &root,
        &preview(state, share, &root, &state.records(share)?)?,
    )
}

fn advertised_records(root: &Path, records: &[Record]) -> Result<Vec<Record>> {
    let matcher = IgnoreMatcher::new(root)?;
    Ok(records
        .iter()
        .filter(|record| !matcher.is_ignored(&record.path))
        .cloned()
        .collect())
}

pub fn apply_plan(state: &mut State, share: &ShareId, records: &[Record]) -> Result<()> {
    validate_unique_paths(records)?;
    validate_declared_sizes(records)?;
    let root = state.root_for(share)?;
    let prior = state.records(share)?;
    let prior: std::collections::HashMap<_, _> = prior
        .iter()
        .map(|record| (record.path.as_bytes().to_vec(), record))
        .collect();
    let (intent, _) = state.set_install_intent(share, records)?;
    let install_temps: std::collections::HashMap<&[u8], _> = intent
        .temps
        .iter()
        .map(|temp| (temp.path.as_bytes(), temp))
        .collect();
    let mut accepted = Vec::with_capacity(records.len());
    let matcher = IgnoreMatcher::new(&root)?;
    let mut ignore_cache = std::collections::HashMap::new();
    for record in records {
        if ignored_cached(&matcher, &record.path, &mut ignore_cache) {
            if let Some(old) = prior.get(record.path.as_bytes()) {
                accepted.push((*old).clone());
            }
        } else {
            accepted.push(record.clone());
        }
    }
    let mut accepted_paths: std::collections::HashSet<_> = accepted
        .iter()
        .map(|record| record.path.as_bytes().to_vec())
        .collect();
    for old in prior.values() {
        if accepted_paths.insert(old.path.as_bytes().to_vec())
            && ignored_cached(&matcher, &old.path, &mut ignore_cache)
        {
            accepted.push((*old).clone());
        }
    }
    let mut ordered = Vec::new();
    for record in &accepted {
        if !ignored_cached(&matcher, &record.path, &mut ignore_cache) {
            ordered.push(record.clone());
        }
    }
    ordered.sort_by_key(|record| {
        let depth = record.path.to_path_buf().components().count();
        match record.version.entry {
            Entry::Tombstone => (0, usize::MAX - depth),
            Entry::Directory => (1, depth),
            Entry::File { .. } | Entry::Symlink { .. } => (2, depth),
        }
    });
    for record in &ordered {
        let expected = prior
            .get(record.path.as_bytes())
            .map(|old| &old.version.entry);
        let install_temp = install_temps
            .get(record.path.as_bytes())
            .copied()
            .context("install intent is missing a temporary token")?;
        let mut token = install_temp.token.clone();
        let mut phase = install_temp.phase;
        if phase == InstallTempPhase::Pending {
            while install_temp_exists(&root, record, &token)? {
                token = state.rotate_unowned_install_temp(share, &record.path)?;
            }
            state.mark_install_temp_creating(share, &record.path)?;
            phase = InstallTempPhase::Creating;
        }
        if let Err(error) = apply_record(state, share, &root, record, expected, &token, phase) {
            let recovered = scan(state, share, &root, &state.records(share)?)?;
            state.replace_records(share, &recovered)?;
            return Err(error.context("apply stopped; state was recovered from disk"));
        }
    }
    state.replace_records(share, &accepted)?;
    state.clear_install_intent(share)?;
    Ok(())
}

fn install_temp_exists(root: &Path, record: &Record, token: &str) -> Result<bool> {
    let root = cap_std::fs::Dir::open_ambient_dir(root, cap_std::ambient_authority())?;
    let path = record.path.to_path_buf();
    let parent = path.parent().unwrap_or_else(|| Path::new(""));
    let directory = if parent.as_os_str().is_empty() {
        root.try_clone()?
    } else {
        match root.open_dir(parent) {
            Ok(directory) => directory,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
                ) =>
            {
                return Ok(false);
            }
            Err(error) => return Err(error.into()),
        }
    };
    Ok(directory.symlink_metadata(token).is_ok())
}

fn apply_record(
    state: &State,
    share: &ShareId,
    root: &Path,
    record: &Record,
    expected: Option<&Entry>,
    temp_name: &str,
    temp_phase: InstallTempPhase,
) -> Result<()> {
    use cap_std::ambient_authority;
    use cap_std::fs::{Dir, OpenOptions, Permissions, PermissionsExt};

    let root_dir = Dir::open_ambient_dir(root, ambient_authority())?;
    let relative = record.path.to_path_buf();
    let parent = relative.parent().unwrap_or_else(|| Path::new(""));
    root_dir.create_dir_all(parent)?;
    let parent_dir = if parent.as_os_str().is_empty() {
        root_dir.try_clone()?
    } else {
        root_dir.open_dir(parent)?
    };
    let name = Path::new(relative.file_name().context("entry has no basename")?);
    let temp = Path::new(temp_name);
    if temp_phase == InstallTempPhase::Owned
        && matches!(record.version.entry, Entry::Tombstone)
        && recover_tombstone(&parent_dir, temp, name, expected, temp_name)?
    {
        return Ok(());
    }
    if disk_matches_cap(&root_dir, &relative, &record.version.entry)? {
        recover_displaced(&parent_dir, temp, name, expected)?;
        return Ok(());
    }
    let temp_exists =
        temp_phase != InstallTempPhase::Pending && parent_dir.symlink_metadata(temp).is_ok();
    if temp_phase == InstallTempPhase::Creating && temp_exists {
        let staged_matches = if matches!(record.version.entry, Entry::Tombstone) {
            file_matches_token(&parent_dir, temp, temp_name)?
        } else {
            disk_matches_cap(&parent_dir, temp, &record.version.entry)?
        };
        if !staged_matches {
            state.rotate_unowned_install_temp(share, &record.path)?;
            bail!("creating install temporary does not match its intended entry");
        }
        state.mark_install_temp_owned(share, &record.path)?;
    }
    if temp_exists
        && !matches!(record.version.entry, Entry::Tombstone)
        && !disk_matches_cap(&parent_dir, temp, &record.version.entry)?
    {
        bail!("owned install temporary does not match its intended entry");
    }
    match &record.version.entry {
        Entry::Directory => {
            if !temp_exists {
                parent_dir.create_dir(temp)?;
                state.mark_install_temp_owned(share, &record.path)?;
            }
            atomic_install(&parent_dir, temp, name, expected)?;
        }
        Entry::File {
            hash,
            size,
            executable,
        } => {
            if !temp_exists {
                let mut source = state.open_verified_object(hash)?;
                if source.metadata()?.len() != *size {
                    bail!("stored verified object size differs from the validated record");
                }
                let mut options = OpenOptions::new();
                options.create_new(true).write(true);
                let mut output = parent_dir.open_with(temp, &options)?;
                std::io::copy(&mut source, &mut output)?;
                output.sync_all()?;
                output.set_permissions(Permissions::from_mode(if *executable {
                    0o700
                } else {
                    0o600
                }))?;
                state.mark_install_temp_owned(share, &record.path)?;
            }
            let old_mode = atomic_install(&parent_dir, temp, name, expected)?;
            if let Some(mut mode) = old_mode {
                if *executable {
                    mode |= 0o111;
                } else {
                    mode &= !0o111;
                }
                parent_dir.set_permissions(name, Permissions::from_mode(mode))?;
            }
        }
        Entry::Symlink {
            target: link_target,
        } => {
            if !temp_exists {
                parent_dir.symlink_contents(bytes_path(link_target), temp)?;
                state.mark_install_temp_owned(share, &record.path)?;
            }
            atomic_install(&parent_dir, temp, name, expected)?;
        }
        Entry::Tombstone => {
            if matches!(expected, None | Some(Entry::Tombstone)) {
                return Ok(());
            }
            if !temp_exists {
                let mut options = OpenOptions::new();
                options.create_new(true).write(true);
                let mut marker = parent_dir.open_with(temp, &options)?;
                marker.write_all(temp_name.as_bytes())?;
                marker.sync_all()?;
                state.mark_install_temp_owned(share, &record.path)?;
            }
            exchange(&parent_dir, temp, name)?;
            if !disk_matches_cap(&parent_dir, temp, expected.expect("checked above"))? {
                exchange(&parent_dir, temp, name)?;
                bail!("path changed while applying: {}", record.path.display());
            }
            if let Err(error) = remove_entry(&parent_dir, temp) {
                exchange(&parent_dir, temp, name)?;
                parent_dir.remove_file(temp)?;
                return Err(
                    error.context("deletion rolled back because displaced entry was not empty")
                );
            }
            parent_dir.remove_file(name)?;
        }
    }
    Ok(())
}

fn recover_tombstone(
    parent: &cap_std::fs::Dir,
    temp: &Path,
    target: &Path,
    expected: Option<&Entry>,
    token: &str,
) -> Result<bool> {
    if !file_matches_token(parent, target, token)? {
        return Ok(false);
    }
    match parent.symlink_metadata(temp) {
        Ok(_) => {
            let displaced_matches = expected
                .map(|entry| disk_matches_cap(parent, temp, entry))
                .transpose()?
                .unwrap_or(false);
            if !displaced_matches {
                exchange(parent, temp, target)?;
                remove_entry(parent, temp)?;
                bail!("unverified deletion was rolled back");
            }
            remove_entry(parent, temp)?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    parent.remove_file(target)?;
    Ok(true)
}

fn file_matches_token(parent: &cap_std::fs::Dir, path: &Path, token: &str) -> Result<bool> {
    use std::os::fd::AsFd;
    let fd = match rustix::fs::openat(
        parent.as_fd(),
        path,
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    ) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(_) => return Ok(false),
    };
    let mut file = std::fs::File::from(fd);
    if file.metadata()?.len() != token.len() as u64 {
        return Ok(false);
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(bytes == token.as_bytes())
}

fn recover_displaced(
    parent: &cap_std::fs::Dir,
    temp: &Path,
    target: &Path,
    expected: Option<&Entry>,
) -> Result<()> {
    let exists = match parent.symlink_metadata(temp) {
        Ok(_) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(error.into()),
    };
    if !exists {
        return Ok(());
    }
    let expected_matches = match expected {
        Some(entry) => disk_matches_cap(parent, temp, entry)?,
        None => false,
    };
    if expected_matches && remove_entry(parent, temp).is_ok() {
        return Ok(());
    }
    exchange(parent, temp, target)?;
    remove_entry(parent, temp)?;
    bail!("interrupted install contained an unverified displaced entry; original restored")
}

#[cfg(unix)]
fn bytes_path(bytes: &[u8]) -> &Path {
    use std::os::unix::ffi::OsStrExt;
    Path::new(std::ffi::OsStr::from_bytes(bytes))
}

fn atomic_install(
    root: &cap_std::fs::Dir,
    temp: &Path,
    target: &Path,
    expected: Option<&Entry>,
) -> Result<Option<u32>> {
    use cap_std::fs::PermissionsExt;
    if matches!(expected, None | Some(Entry::Tombstone)) {
        rename_noreplace(root, temp, target)?;
        return Ok(None);
    }
    exchange(root, temp, target)?;
    let expected = expected.expect("checked above");
    if !disk_matches_cap(root, temp, expected)? {
        exchange(root, temp, target)?;
        remove_entry(root, temp)?;
        bail!("path changed while applying");
    }
    let old_mode = root.symlink_metadata(temp)?.permissions().mode();
    if let Err(error) = remove_entry(root, temp) {
        exchange(root, temp, target)?;
        remove_entry(root, temp)?;
        return Err(error.context("displaced entry could not be removed; replacement rolled back"));
    }
    Ok(Some(old_mode))
}

fn disk_matches_cap(root: &cap_std::fs::Dir, path: &Path, expected: &Entry) -> Result<bool> {
    use cap_std::fs::PermissionsExt;
    let metadata = match root.symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(matches!(expected, Entry::Tombstone));
        }
        Err(error) => return Err(error.into()),
    };
    Ok(match expected {
        Entry::Tombstone => false,
        Entry::Directory => metadata.is_dir(),
        Entry::Symlink { target } if metadata.file_type().is_symlink() => {
            path_bytes(&root.read_link_contents(path)?) == *target
        }
        Entry::File {
            hash, executable, ..
        } if metadata.is_file() => {
            let mut file = root.open(path)?;
            let mut hasher = blake3::Hasher::new();
            let mut buffer = vec![0; CHUNK];
            loop {
                let read = file.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                hasher.update(&buffer[..read]);
            }
            hasher.finalize().to_hex().as_str() == hash.as_str()
                && (metadata.permissions().mode() & 0o111 != 0) == *executable
        }
        _ => false,
    })
}

fn remove_entry(root: &cap_std::fs::Dir, path: &Path) -> Result<()> {
    if root.symlink_metadata(path)?.is_dir() {
        root.remove_dir(path)?;
    } else {
        root.remove_file(path)?;
    }
    Ok(())
}

fn exchange(root: &cap_std::fs::Dir, from: &Path, to: &Path) -> Result<()> {
    renameat_with(root, from, to, rustix::fs::RenameFlags::EXCHANGE)
}

fn rename_noreplace(root: &cap_std::fs::Dir, from: &Path, to: &Path) -> Result<()> {
    renameat_with(root, from, to, rustix::fs::RenameFlags::NOREPLACE)
}

fn renameat_with(
    root: &cap_std::fs::Dir,
    from: &Path,
    to: &Path,
    flags: rustix::fs::RenameFlags,
) -> Result<()> {
    use std::os::fd::AsFd;
    rustix::fs::renameat_with(root.as_fd(), from, root.as_fd(), to, flags)?;
    Ok(())
}

#[cfg(unix)]
fn path_bytes(path: &Path) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    path.as_os_str().as_bytes().to_vec()
}

pub fn plan(local: &[Record], remote: &[Record]) -> Plan {
    reconcile(local, remote)
}

pub fn send_object(state: &State, hash: &ObjectHash, writer: &mut impl Write) -> Result<()> {
    let mut file = state.open_verified_object(hash)?;
    let size = file.metadata()?.len();
    write_message(
        writer,
        &Message::ObjectStart {
            hash: hash.clone(),
            size,
        },
    )?;
    let mut buffer = vec![0; CHUNK];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        write_message(
            writer,
            &Message::ObjectChunk {
                data: buffer[..read].to_vec(),
            },
        )?;
    }
    write_message(writer, &Message::ObjectEnd)
}

pub fn receive_object(
    state: &State,
    hash: ObjectHash,
    size: u64,
    reader: &mut impl Read,
) -> Result<()> {
    let mut sink = state.begin_object(hash, size)?;
    loop {
        match read_message(reader)? {
            Message::ObjectChunk { data } => {
                sink.write_chunk(&data)?;
            }
            Message::ObjectEnd => break,
            other => bail!("unexpected object message: {other:?}"),
        }
    }
    sink.finish()
}

pub fn required_hashes(state: &State, records: &[Record]) -> Vec<ObjectHash> {
    let mut seen = std::collections::HashSet::new();
    records
        .iter()
        .filter_map(|r| match &r.version.entry {
            Entry::File { hash, .. }
                if seen.insert(hash.clone()) && !has_verified_object(state, hash) =>
            {
                Some(hash.clone())
            }
            _ => None,
        })
        .collect()
}

pub fn required_hashes_for_share(
    state: &State,
    share: &ShareId,
    records: &[Record],
) -> Result<Vec<ObjectHash>> {
    validate_declared_sizes(records)?;
    let root = state.root_for(share)?;
    let matcher = IgnoreMatcher::new(&root)?;
    let mut hashes = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for record in records {
        if matcher.is_ignored(&record.path) {
            continue;
        }
        if let Entry::File { hash, size, .. } = &record.version.entry
            && seen.insert(hash.clone())
        {
            match state.open_verified_object(hash) {
                Ok(file) if file.metadata()?.len() != *size => {
                    bail!("stored verified object size differs from the validated record")
                }
                Ok(_) => {}
                Err(_) => hashes.push(hash.clone()),
            }
        }
    }
    hashes.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    hashes.dedup();
    Ok(hashes)
}

fn validate_declared_sizes(records: &[Record]) -> Result<()> {
    let mut sizes = std::collections::HashMap::new();
    for record in records {
        if let Entry::File { hash, size, .. } = &record.version.entry {
            match sizes.entry(hash.clone()) {
                std::collections::hash_map::Entry::Occupied(prior) => {
                    if *prior.get() != *size {
                        bail!("the same object hash has conflicting declared sizes");
                    }
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(*size);
                }
            }
        }
    }
    Ok(())
}

fn validate_unique_paths(records: &[Record]) -> Result<()> {
    let mut paths = std::collections::HashSet::new();
    for record in records {
        if !paths.insert(record.path.as_bytes()) {
            bail!("apply plan contains duplicate paths");
        }
    }
    Ok(())
}

fn ignored_cached(
    matcher: &IgnoreMatcher,
    path: &RelativePath,
    cache: &mut std::collections::HashMap<Vec<u8>, bool>,
) -> bool {
    if let Some(ignored) = cache.get(path.as_bytes()) {
        return *ignored;
    }
    let ignored = matcher.is_ignored(path);
    cache.insert(path.as_bytes().to_vec(), ignored);
    ignored
}

pub fn has_verified_object(state: &State, hash: &ObjectHash) -> bool {
    state.open_verified_object(hash).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{PeerId, RelativePath, Version};
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn rejects_oversized_frame_before_allocation() {
        let bytes = ((MAX_FRAME as u32) + 1).to_be_bytes().to_vec();
        assert!(read_message(&mut bytes.as_slice()).is_err());
    }

    #[test]
    fn plan_records_share_protocol_frames() -> Result<()> {
        let records = (0..100)
            .map(|sequence| {
                Ok(Record {
                    path: RelativePath::from_bytes(format!("file-{sequence}").into_bytes())?,
                    version: Version {
                        peer: PeerId("peer-test".into()),
                        sequence,
                        timestamp_ns: sequence as i64,
                        seen: Vec::new(),
                        entry: Entry::Tombstone,
                    },
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut wire = Vec::new();
        write_plan(
            &mut wire,
            &Plan {
                records,
                conflicts: Vec::new(),
            },
        )?;
        let mut input = wire.as_slice();
        let mut chunks = 0;
        loop {
            match read_message(&mut input)? {
                Message::ApplyChunk { records, .. } => {
                    chunks += 1;
                    assert!(records.len() > 1);
                }
                Message::ApplyEnd => break,
                other => panic!("unexpected message: {other:?}"),
            }
        }
        assert_eq!(chunks, 1);
        Ok(())
    }

    #[test]
    fn corrupt_cached_object_is_requested_and_replaced() -> Result<()> {
        let temp = tempdir()?;
        let state = State::open(temp.path().join("state"))?;
        let bytes = b"expected";
        let hash = ObjectHash::from_blake3(blake3::hash(bytes));
        state.import_object(&hash, bytes)?;
        fs::write(state.object_path(&hash), b"corrupt")?;
        let record = Record {
            path: RelativePath::from_bytes(b"file".to_vec())?,
            version: Version {
                peer: PeerId("peer-test".into()),
                sequence: 1,
                timestamp_ns: 1,
                seen: Vec::new(),
                entry: Entry::File {
                    hash: hash.clone(),
                    size: bytes.len() as u64,
                    executable: false,
                },
            },
        };
        assert_eq!(required_hashes(&state, &[record]), vec![hash.clone()]);
        let mut sink = state.begin_object(hash.clone(), bytes.len() as u64)?;
        sink.write_chunk(bytes)?;
        sink.finish()?;
        assert_eq!(state.read_object(&hash)?, bytes);
        Ok(())
    }

    #[test]
    fn apply_refuses_symlink_parent() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path().join("root");
        fs::create_dir_all(&root)?;
        #[cfg(unix)]
        std::os::unix::fs::symlink(temp.path(), root.join("escape"))?;
        let path = RelativePath::from_bytes(b"escape/file".to_vec())?;
        let directory = cap_std::fs::Dir::open_ambient_dir(&root, cap_std::ambient_authority())?;
        assert!(directory.create_dir_all(path.to_path_buf()).is_err());
        Ok(())
    }
}
