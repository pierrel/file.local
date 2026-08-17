use std::io::{Read, Write};
use std::os::fd::AsFd;
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::model::{Entry, ObjectHash, PeerId, Record, RelativePath, ShareId};
use crate::reconcile::{MergeCandidate, Plan, reconcile};
use crate::scan::{IgnoreMatcher, preview_cap_with_ignores, scan_cap, scan_cap_with_ignores};
pub use crate::state::RootIdentityChanged;
use crate::state::{InstallTempPhase, RootIdentity, State};

pub const MAX_FRAME: usize = 2 * 1024 * 1024;
pub const SYNC_PROTOCOL_VERSION: u32 = 3;
pub const WATCH_PROTOCOL_VERSION: u32 = 5;
/// Compatibility name for the existing one-shot synchronization protocol.
pub const PROTOCOL_VERSION: u32 = SYNC_PROTOCOL_VERSION;
pub const MAX_RECORDS_PER_SESSION: usize = 1_000_000;
pub const MAX_METADATA_BYTES_PER_SESSION: usize = 256 * 1024 * 1024;
pub const MAX_MERGE_CANDIDATES_PER_ROUND: usize = 64;
pub const MAX_MERGE_WORK_PER_ROUND: usize = 32_000_000;
pub const MAX_MERGE_HUNKS_PER_ROUND: usize = 4_096;
pub const MAX_MERGED_BYTES_PER_ROUND: usize = 2 * 1024 * 1024;
pub fn max_transfer_bytes_per_session() -> u64 {
    std::env::var("FLOCAL_MAX_SESSION_BYTES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(10 * 1024 * 1024 * 1024)
}

#[derive(Debug)]
pub struct RoundBudget {
    deadline: Instant,
    metadata_bytes: usize,
    transfer_bytes: u64,
}

impl RoundBudget {
    pub fn new(deadline: Instant) -> Self {
        Self {
            deadline,
            metadata_bytes: 0,
            transfer_bytes: 0,
        }
    }

    pub fn check(&self) -> Result<()> {
        if Instant::now() >= self.deadline {
            bail!("persistent synchronization round exceeded its time limit");
        }
        Ok(())
    }

    pub fn frame_deadline(&self) -> Result<Instant> {
        self.check()?;
        Ok(self.deadline.min(Instant::now() + default_frame_deadline()))
    }

    pub fn phase_deadline(&self) -> Result<Instant> {
        self.check()?;
        Ok(self.deadline)
    }

    pub fn add_metadata(&mut self, bytes: usize) -> Result<()> {
        self.metadata_bytes = self.metadata_bytes.saturating_add(bytes);
        if self.metadata_bytes > MAX_METADATA_BYTES_PER_SESSION {
            bail!("persistent round exceeds its cumulative metadata limit");
        }
        Ok(())
    }

    pub fn add_transfer(&mut self, bytes: u64) -> Result<()> {
        self.transfer_bytes = self.transfer_bytes.saturating_add(bytes);
        if self.transfer_bytes > max_transfer_bytes_per_session() {
            bail!("persistent round exceeds its cumulative transfer limit");
        }
        Ok(())
    }
}
const CHUNK: usize = 256 * 1024;

#[derive(Debug)]
pub struct ApplyInvalidated {
    pub path: RelativePath,
}

impl std::fmt::Display for ApplyInvalidated {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "path changed while applying: {}",
            self.path.display()
        )
    }
}

impl std::error::Error for ApplyInvalidated {}

#[derive(Debug)]
struct InstallPreconditionChanged;

impl std::fmt::Display for InstallPreconditionChanged {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("install precondition changed")
    }
}

impl std::error::Error for InstallPreconditionChanged {}

pub struct ShareRoot {
    path: std::path::PathBuf,
    directory: cap_std::fs::Dir,
    identity: RootIdentity,
}

impl ShareRoot {
    pub fn open(state: &State, share: &ShareId) -> Result<Self> {
        use rustix::fs::{Mode, OFlags};
        state.validate_root_identity(share)?;
        let path = state.root_for(share)?;
        let fd = rustix::fs::open(
            &path,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        let file = std::fs::File::from(fd);
        let metadata = file.metadata()?;
        #[cfg(unix)]
        let identity = {
            use std::os::unix::fs::MetadataExt;
            RootIdentity {
                device: metadata.dev(),
                inode: metadata.ino(),
            }
        };
        if identity != state.expected_root_identity(share)? {
            return Err(RootIdentityChanged::new(
                "configured root identity changed while opening its directory capability",
            )
            .into());
        }
        let directory = cap_std::fs::Dir::from_std_file(file);
        state.validate_root_identity(share)?;
        Ok(Self {
            path,
            directory,
            identity,
        })
    }

    pub fn validate(&self, state: &State, share: &ShareId) -> Result<()> {
        if self.identity != state.expected_root_identity(share)? {
            return Err(RootIdentityChanged::new(
                "held root capability: configured root identity changed",
            )
            .into());
        }
        state.validate_root_identity(share)?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum V1Message {
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
        merges: Vec<crate::reconcile::MergeCandidate>,
    },
    ApplyEnd,
    Applied,
    HeadChunk {
        records: Vec<Record>,
    },
    CommitAck,
    Cancel,
    Done,
    Error {
        message: String,
    },
}

/// Compatibility name retained for the existing explicit-sync implementation.
pub type Message = V1Message;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum InitialMessage {
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
    WatchOpen {
        protocol: u32,
        share: ShareId,
        peer: PeerId,
    },
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "scope", rename_all = "snake_case", deny_unknown_fields)]
pub enum V2Envelope {
    Session { frame: V2SessionFrame },
    Round { round: u64, frame: V2RoundFrame },
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum V2SessionFrame {
    WatchOpen {
        protocol: u32,
        share: ShareId,
        peer: PeerId,
    },
    WatchAccepted {
        protocol: u32,
        peer: PeerId,
    },
    Ready {
        generation: u64,
    },
    UnsettledChunk {
        paths: Vec<RelativePath>,
    },
    Changed {
        generation: u64,
    },
    Ping {
        nonce: u64,
    },
    Pong {
        nonce: u64,
    },
    Error {
        retryable: bool,
        message: String,
    },
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum V2RoundFrame {
    SyncStart {
        connector_generation: u64,
        responder_generation: u64,
    },
    SyncAccepted,
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
        merges: Vec<crate::reconcile::MergeCandidate>,
    },
    ApplyEnd,
    Applied,
    HeadChunk {
        records: Vec<Record>,
    },
    RoundInvalidated {
        path: RelativePath,
    },
    Done,
    SyncFinished,
    SyncFailed {
        retryable: bool,
        message: String,
    },
}

fn write_frame(writer: &mut impl Write, frame: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec(frame)?;
    if bytes.len() > MAX_FRAME {
        bail!("protocol frame too large");
    }
    writer.write_all(&(bytes.len() as u32).to_be_bytes())?;
    writer.write_all(&bytes)?;
    writer.flush()?;
    Ok(())
}

fn read_frame<T: DeserializeOwned>(reader: &mut impl Read) -> Result<T> {
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

pub fn read_v2_envelope_until(reader: &impl AsFd, deadline: Instant) -> Result<V2Envelope> {
    let mut length = [0u8; 4];
    read_exact_until(reader, &mut length, deadline)?;
    read_v2_envelope_body_until(reader, length, deadline)
}

pub fn read_v2_envelope_in_phase(
    reader: &impl AsFd,
    phase_deadline: Instant,
) -> Result<V2Envelope> {
    read_v2_envelope_in_phase_with_frame_timeout(reader, phase_deadline, default_frame_deadline())
}

fn read_v2_envelope_in_phase_with_frame_timeout(
    reader: &impl AsFd,
    phase_deadline: Instant,
    frame_timeout: Duration,
) -> Result<V2Envelope> {
    let mut length = [0u8; 4];
    read_exact_until(reader, &mut length[..1], phase_deadline)
        .context("waiting for persistent protocol phase result")?;
    let frame_deadline = phase_deadline.min(Instant::now() + frame_timeout);
    read_exact_until(reader, &mut length[1..], frame_deadline)?;
    read_v2_envelope_body_until(reader, length, frame_deadline)
}

fn read_v2_envelope_body_until(
    reader: &impl AsFd,
    length: [u8; 4],
    deadline: Instant,
) -> Result<V2Envelope> {
    let length = u32::from_be_bytes(length) as usize;
    if length > MAX_FRAME {
        bail!("protocol frame too large");
    }
    let mut bytes = vec![0u8; length];
    read_exact_until(reader, &mut bytes, deadline)?;
    Ok(serde_json::from_slice(&bytes)?)
}

pub fn read_initial_message_until(reader: &impl AsFd, deadline: Instant) -> Result<InitialMessage> {
    let mut length = [0u8; 4];
    read_exact_until(reader, &mut length, deadline)?;
    let length = u32::from_be_bytes(length) as usize;
    if length > MAX_FRAME {
        bail!("protocol frame too large");
    }
    let mut bytes = vec![0u8; length];
    read_exact_until(reader, &mut bytes, deadline)?;
    Ok(serde_json::from_slice(&bytes)?)
}

pub fn write_v2_envelope_until(
    writer: &impl AsFd,
    envelope: &V2Envelope,
    deadline: Instant,
) -> Result<()> {
    let bytes = serde_json::to_vec(envelope)?;
    if bytes.len() > MAX_FRAME {
        bail!("protocol frame too large");
    }
    write_all_until(writer, &(bytes.len() as u32).to_be_bytes(), deadline)?;
    write_all_until(writer, &bytes, deadline)
}

pub fn write_initial_message_until(
    writer: &impl AsFd,
    message: &InitialMessage,
    deadline: Instant,
) -> Result<()> {
    let bytes = serde_json::to_vec(message)?;
    if bytes.len() > MAX_FRAME {
        bail!("protocol frame too large");
    }
    write_all_until(writer, &(bytes.len() as u32).to_be_bytes(), deadline)?;
    write_all_until(writer, &bytes, deadline)
}

fn read_exact_until(reader: &impl AsFd, mut buffer: &mut [u8], deadline: Instant) -> Result<()> {
    while !buffer.is_empty() {
        wait_fd(reader, rustix::event::PollFlags::IN, deadline)?;
        match rustix::io::read(reader, &mut *buffer) {
            Ok(0) => bail!("persistent peer closed the protocol stream"),
            Ok(count) => buffer = &mut buffer[count..],
            Err(rustix::io::Errno::AGAIN | rustix::io::Errno::INTR) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn write_all_until(writer: &impl AsFd, mut bytes: &[u8], deadline: Instant) -> Result<()> {
    let original = rustix::fs::fcntl_getfl(writer)?;
    rustix::fs::fcntl_setfl(writer, original | rustix::fs::OFlags::NONBLOCK)?;
    let outcome = (|| {
        while !bytes.is_empty() {
            wait_fd(writer, rustix::event::PollFlags::OUT, deadline)?;
            match rustix::io::write(writer, bytes) {
                Ok(0) => bail!("persistent peer stopped consuming protocol output"),
                Ok(count) => bytes = &bytes[count..],
                Err(rustix::io::Errno::AGAIN | rustix::io::Errno::INTR) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    })();
    let restore = rustix::fs::fcntl_setfl(writer, original);
    outcome.and(restore.map_err(Into::into))
}

fn wait_fd(fd: &impl AsFd, flags: rustix::event::PollFlags, deadline: Instant) -> Result<()> {
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .context("persistent protocol frame deadline exceeded")?;
    let mut descriptors = [rustix::event::PollFd::new(fd, flags)];
    let timeout = rustix::event::Timespec {
        tv_sec: remaining.as_secs().min(i64::MAX as u64) as i64,
        tv_nsec: remaining.subsec_nanos() as i64,
    };
    if rustix::event::poll(&mut descriptors, Some(&timeout))? == 0 {
        bail!("persistent protocol frame deadline exceeded");
    }
    Ok(())
}

pub fn default_frame_deadline() -> Duration {
    Duration::from_secs(30)
}

pub fn default_phase_deadline() -> Duration {
    Duration::from_secs(5 * 60)
}

pub fn input_ready_until(reader: &impl AsFd, deadline: Instant) -> Result<bool> {
    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
        return Ok(false);
    };
    let mut descriptors = [rustix::event::PollFd::new(
        reader,
        rustix::event::PollFlags::IN,
    )];
    let timeout = rustix::event::Timespec {
        tv_sec: remaining.as_secs().min(i64::MAX as u64) as i64,
        tv_nsec: remaining.subsec_nanos() as i64,
    };
    Ok(rustix::event::poll(&mut descriptors, Some(&timeout))? != 0)
}

pub fn write_message(writer: &mut impl Write, message: &Message) -> Result<()> {
    write_v1_message(writer, message)
}

pub fn read_message(reader: &mut impl Read) -> Result<Message> {
    read_v1_message(reader)
}

pub fn write_v1_message(writer: &mut impl Write, message: &V1Message) -> Result<()> {
    write_frame(writer, message)
}

pub fn read_v1_message(reader: &mut impl Read) -> Result<V1Message> {
    read_frame(reader)
}

pub fn write_initial_message(writer: &mut impl Write, message: &InitialMessage) -> Result<()> {
    write_frame(writer, message)
}

pub fn read_initial_message(reader: &mut impl Read) -> Result<InitialMessage> {
    read_frame(reader)
}

pub fn write_v2_envelope(writer: &mut impl Write, envelope: &V2Envelope) -> Result<()> {
    write_frame(writer, envelope)
}

pub fn read_v2_envelope(reader: &mut impl Read) -> Result<V2Envelope> {
    read_frame(reader)
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
    write_merge_chunks(writer, &plan.merges)?;
    write_message(writer, &Message::ApplyEnd)
}

pub fn write_heads(writer: &mut impl Write, records: &[Record]) -> Result<()> {
    let envelope = serde_json::to_vec(&Message::HeadChunk {
        records: Vec::new(),
    })?
    .len();
    for chunk in bounded_chunks(records, envelope, "acknowledged head")? {
        write_message(
            writer,
            &Message::HeadChunk {
                records: chunk.to_vec(),
            },
        )?;
    }
    Ok(())
}

pub fn regular_file_heads(records: &[Record]) -> Vec<Record> {
    let mut heads: Vec<_> = records
        .iter()
        .filter(|record| matches!(record.version.entry, Entry::File { .. }))
        .cloned()
        .collect();
    heads.sort_by(|left, right| left.path.as_bytes().cmp(right.path.as_bytes()));
    heads
}

pub fn intersect_heads(local: &[Record], remote: &[Record]) -> Result<Vec<Record>> {
    validate_unique_paths(local)?;
    validate_unique_paths(remote)?;
    let remote: std::collections::HashMap<_, _> = remote
        .iter()
        .map(|record| (record.path.as_bytes(), record))
        .collect();
    Ok(regular_file_heads(local)
        .into_iter()
        .filter(|record| remote.get(record.path.as_bytes()) == Some(&record))
        .collect())
}

pub fn validate_ack_heads(current: &[Record], proposed: &[Record]) -> Result<()> {
    let canonical = regular_file_heads(proposed);
    if canonical != proposed || proposed.windows(2).any(|pair| pair[0].path == pair[1].path) {
        bail!("acknowledged heads are not sorted unique regular-file records");
    }
    let current: std::collections::HashMap<_, _> = current
        .iter()
        .map(|record| (record.path.as_bytes(), record))
        .collect();
    if proposed
        .iter()
        .any(|record| current.get(record.path.as_bytes()) != Some(&record))
    {
        bail!("acknowledged head does not match durable current state");
    }
    Ok(())
}

fn write_record_chunks(writer: &mut impl Write, records: &[Record]) -> Result<()> {
    let envelope = serde_json::to_vec(&Message::ApplyChunk {
        records: Vec::new(),
        conflicts: Vec::new(),
        merges: Vec::new(),
    })?
    .len();
    for chunk in bounded_chunks(records, envelope, "plan record")? {
        write_message(
            writer,
            &Message::ApplyChunk {
                records: chunk.to_vec(),
                conflicts: Vec::new(),
                merges: Vec::new(),
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
        merges: Vec::new(),
    })?
    .len();
    for chunk in bounded_chunks(conflicts, envelope, "plan conflict")? {
        write_message(
            writer,
            &Message::ApplyChunk {
                records: Vec::new(),
                conflicts: chunk.to_vec(),
                merges: Vec::new(),
            },
        )?;
    }
    Ok(())
}

fn write_merge_chunks(
    writer: &mut impl Write,
    merges: &[crate::reconcile::MergeCandidate],
) -> Result<()> {
    let envelope = serde_json::to_vec(&Message::ApplyChunk {
        records: Vec::new(),
        conflicts: Vec::new(),
        merges: Vec::new(),
    })?
    .len();
    for chunk in bounded_chunks(merges, envelope, "merge candidate")? {
        write_message(
            writer,
            &Message::ApplyChunk {
                records: Vec::new(),
                conflicts: Vec::new(),
                merges: chunk.to_vec(),
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
    let root = ShareRoot::open(state, share)?;
    refresh_with_root(state, share, &root)
}

pub fn refresh_with_root(
    state: &mut State,
    share: &ShareId,
    root: &ShareRoot,
) -> Result<Vec<Record>> {
    root.validate(state, share)?;
    let previous = state.records(share)?;
    let (records, matcher) =
        scan_cap_with_ignores(state, share, &root.path, &root.directory, &previous)?;
    root.validate(state, share)?;
    state.replace_records(share, &records)?;
    Ok(advertised_records(&matcher, &records))
}

pub fn preview_refresh(state: &State, share: &ShareId) -> Result<Vec<Record>> {
    let root = ShareRoot::open(state, share)?;
    root.validate(state, share)?;
    let (records, matcher) = preview_cap_with_ignores(
        state,
        share,
        &root.path,
        &root.directory,
        &state.records(share)?,
    )?;
    root.validate(state, share)?;
    Ok(advertised_records(&matcher, &records))
}

fn advertised_records(matcher: &IgnoreMatcher, records: &[Record]) -> Vec<Record> {
    records
        .iter()
        .filter(|record| !matcher.is_record_ignored(record))
        .cloned()
        .collect()
}

pub fn apply_plan(state: &mut State, share: &ShareId, records: &[Record]) -> Result<()> {
    let root = ShareRoot::open(state, share)?;
    apply_plan_with_root(state, share, &root, records)
}

pub fn apply_complete_plan(state: &mut State, share: &ShareId, plan: &Plan) -> Result<()> {
    let root = ShareRoot::open(state, share)?;
    apply_complete_plan_with_root_skipping(
        state,
        share,
        &root,
        plan,
        &std::collections::HashSet::new(),
    )
}

pub fn apply_plan_with_root(
    state: &mut State,
    share: &ShareId,
    root: &ShareRoot,
    records: &[Record],
) -> Result<()> {
    apply_plan_with_root_skipping(
        state,
        share,
        root,
        records,
        &std::collections::HashSet::new(),
    )
}

pub fn apply_plan_with_root_skipping(
    state: &mut State,
    share: &ShareId,
    root: &ShareRoot,
    records: &[Record],
    retained_paths: &std::collections::HashSet<Vec<u8>>,
) -> Result<()> {
    apply_complete_plan_with_root_skipping(
        state,
        share,
        root,
        &Plan {
            records: records.to_vec(),
            conflicts: Vec::new(),
            merges: Vec::new(),
        },
        retained_paths,
    )
}

pub fn apply_complete_plan_with_root_skipping(
    state: &mut State,
    share: &ShareId,
    root: &ShareRoot,
    plan: &Plan,
    retained_paths: &std::collections::HashSet<Vec<u8>>,
) -> Result<()> {
    let records = &plan.records;
    validate_unique_paths(records)?;
    validate_declared_sizes(records)?;
    root.validate(state, share)?;
    state.ensure_recovery_limits(share, &plan.conflicts)?;
    let prior = state.records(share)?;
    let prior: std::collections::HashMap<_, _> = prior
        .iter()
        .map(|record| (record.path.as_bytes().to_vec(), record))
        .collect();
    let matcher = IgnoreMatcher::from_cap(&root.path, &root.directory)?;
    let root_dir = &root.directory;
    for record in records {
        if matcher.is_record_ignored(record) && !prior.contains_key(record.path.as_bytes()) {
            match root_dir.symlink_metadata(record.path.to_path_buf()) {
                Ok(_) => bail!("incoming path collides with unsynchronized ignored local content"),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
    let (intent, _) = state.set_plan_install_intent(share, records, &plan.conflicts)?;
    #[cfg(feature = "e2e-test-hooks")]
    e2e_stop_before_apply(state)?;
    let install_temps: std::collections::HashMap<&[u8], _> = intent
        .temps
        .iter()
        .map(|temp| (temp.path.as_bytes(), temp))
        .collect();
    let mut accepted = Vec::with_capacity(records.len());
    let mut ignore_cache = std::collections::HashMap::new();
    for record in records {
        let prior_record = prior.get(record.path.as_bytes());
        // A tombstone for a path this peer never recorded has no local
        // resurrection to prevent; persisting it would let a peer permanently
        // pollute local state with fabricated deletions.
        if prior_record.is_none() && matches!(record.version.entry, Entry::Tombstone) {
            continue;
        }
        if ignored_cached(&matcher, record, &mut ignore_cache) {
            if let Some(old) = prior_record {
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
            && ignored_cached(&matcher, old, &mut ignore_cache)
        {
            accepted.push((*old).clone());
        }
    }
    let mut ordered = Vec::new();
    for record in &accepted {
        if !ignored_cached(&matcher, record, &mut ignore_cache)
            && !retained_paths.contains(record.path.as_bytes())
        {
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
    let mut completed: Vec<Record> = Vec::new();
    for record in &ordered {
        let expected = prior
            .get(record.path.as_bytes())
            .map(|old| &old.version.entry);
        // A tombstone with nothing recorded to delete must not touch the
        // filesystem: creating its parent directories would resurrect a
        // deleted directory tree on every synchronization.
        if matches!(record.version.entry, Entry::Tombstone)
            && matches!(expected, None | Some(Entry::Tombstone))
        {
            continue;
        }
        let install_temp = install_temps
            .get(record.path.as_bytes())
            .copied()
            .context("install intent is missing a temporary token")?;
        let mut token = install_temp.token.clone();
        let mut phase = install_temp.phase;
        if phase == InstallTempPhase::Pending {
            while install_temp_exists(root_dir, record, &token)? {
                token = state.rotate_unowned_install_temp(share, &record.path)?;
            }
            state.mark_install_temp_creating(share, &record.path)?;
            phase = InstallTempPhase::Creating;
        }
        if let Err(error) = apply_record(state, share, root_dir, record, expected, &token, phase) {
            if let Some(invalidated) = error.downcast_ref::<ApplyInvalidated>() {
                let parent = record
                    .path
                    .to_path_buf()
                    .parent()
                    .unwrap_or_else(|| Path::new(""))
                    .to_path_buf();
                sync_directory_chain(root_dir, &parent)?;
                let mut baseline: std::collections::HashMap<_, _> = state
                    .records(share)?
                    .into_iter()
                    .map(|record| (record.path.as_bytes().to_vec(), record))
                    .collect();
                for applied in &completed {
                    baseline.insert(applied.path.as_bytes().to_vec(), applied.clone());
                }
                let mut baseline: Vec<_> = baseline.into_values().collect();
                baseline.sort_by(|left, right| left.path.as_bytes().cmp(right.path.as_bytes()));
                let recovered = scan_cap(state, share, &root.path, root_dir, &baseline)?;
                root.validate(state, share)?;
                let completed_paths: std::collections::HashSet<_> = completed
                    .iter()
                    .map(|record| record.path.as_bytes())
                    .collect();
                let recovered_by_path: std::collections::HashMap<_, _> = recovered
                    .iter()
                    .map(|record| (record.path.as_bytes(), record))
                    .collect();
                let mut conflicts: Vec<_> = intent
                    .conflicts
                    .iter()
                    .filter(|conflict| {
                        completed_paths.contains(conflict.path.as_bytes())
                            || recovered_by_path.get(conflict.path.as_bytes())
                                == Some(&conflict.winner())
                    })
                    .cloned()
                    .collect();
                conflicts.extend(reconcile(&recovered, records).conflicts);
                let current_intent = state
                    .install_intent(share)?
                    .context("install intent disappeared before invalidation recovery")?;
                if current_intent.records != intent.records {
                    bail!("install intent changed before invalidation recovery");
                }
                state.retire_invalidated_install(
                    share,
                    &current_intent,
                    &recovered,
                    &conflicts,
                    &invalidated.path,
                )?;
                return Err(ApplyInvalidated {
                    path: invalidated.path.clone(),
                }
                .into());
            }
            let recovered = scan_cap(state, share, &root.path, root_dir, &state.records(share)?)?;
            root.validate(state, share)?;
            state.replace_records(share, &recovered)?;
            return Err(error.context("apply stopped; state was recovered from disk"));
        }
        sync_applied_record(root_dir, record)
            .with_context(|| format!("making applied path durable: {}", record.path.display()))?;
        completed.push(record.clone());
    }
    root.validate(state, share)?;
    state.finish_install(share, &intent, &accepted)?;
    Ok(())
}

#[cfg(feature = "e2e-test-hooks")]
fn e2e_stop_before_apply(state: &State) -> Result<()> {
    if e2e_claim_apply_stop(state)? {
        e2e_publish_apply_stop_pid(state)?;
        signal_hook::low_level::raise(signal_hook::consts::SIGSTOP)?;
    }
    Ok(())
}

#[cfg(feature = "e2e-test-hooks")]
fn e2e_publish_apply_stop_pid(state: &State) -> Result<()> {
    use std::io::Write as _;

    let path = state.dir.join(".e2e-apply-stop.pid");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .context("publishing E2E stopped apply pid")?;
    write!(file, "{}", std::process::id())?;
    file.sync_all()?;
    Ok(())
}

#[cfg(feature = "e2e-test-hooks")]
fn e2e_claim_apply_stop(state: &State) -> Result<bool> {
    let marker = state.dir.join(".e2e-stop-before-apply");
    let claimed = state.dir.join(".e2e-stop-before-apply-claimed");
    match std::fs::rename(&marker, &claimed) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error).context("claiming E2E pre-apply stop marker"),
    }
    let metadata = std::fs::symlink_metadata(&claimed)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        bail!("E2E pre-apply stop marker is not a regular file");
    }
    let repeats = std::fs::read(&claimed)?;
    match repeats.as_slice() {
        b"" | b"1" => {}
        b"2" => std::fs::write(&marker, b"1")?,
        _ => bail!("E2E pre-apply stop marker has an invalid repeat count"),
    }
    std::fs::remove_file(&claimed)?;
    Ok(true)
}

fn install_temp_exists(root: &cap_std::fs::Dir, record: &Record, token: &str) -> Result<bool> {
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
    root_dir: &cap_std::fs::Dir,
    record: &Record,
    expected: Option<&Entry>,
    temp_name: &str,
    temp_phase: InstallTempPhase,
) -> Result<()> {
    use cap_std::fs::{OpenOptions, Permissions, PermissionsExt};
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
    if disk_matches_cap(root_dir, &relative, &record.version.entry)? {
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
        if temp_phase == InstallTempPhase::Owned
            && expected
                .map(|expected| disk_matches_cap(&parent_dir, temp, expected))
                .transpose()?
                .unwrap_or(false)
        {
            remove_entry(&parent_dir, temp)?;
            sync_directory_chain(&parent_dir, Path::new(""))?;
            return Err(ApplyInvalidated {
                path: record.path.clone(),
            }
            .into());
        }
        bail!("owned install temporary does not match its intended entry");
    }
    match &record.version.entry {
        Entry::Directory => {
            if !temp_exists {
                parent_dir.create_dir(temp)?;
                state.mark_install_temp_owned(share, &record.path)?;
            }
            atomic_install(&parent_dir, temp, name, expected, None)
                .map_err(|error| map_precondition(error, &record.path))?;
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
            atomic_install(&parent_dir, temp, name, expected, Some(*executable))
                .map_err(|error| map_precondition(error, &record.path))?;
        }
        Entry::Symlink {
            target: link_target,
        } => {
            if !temp_exists {
                parent_dir.symlink_contents(bytes_path(link_target), temp)?;
                state.mark_install_temp_owned(share, &record.path)?;
            }
            atomic_install(&parent_dir, temp, name, expected, None)
                .map_err(|error| map_precondition(error, &record.path))?;
        }
        Entry::Tombstone => {
            if !temp_exists {
                let mut options = OpenOptions::new();
                options.create_new(true).write(true);
                let mut marker = parent_dir.open_with(temp, &options)?;
                marker.write_all(temp_name.as_bytes())?;
                marker.sync_all()?;
                state.mark_install_temp_owned(share, &record.path)?;
            }
            exchange(&parent_dir, temp, name)?;
            let expected =
                expected.expect("tombstones with nothing to delete are skipped before apply");
            if !disk_matches_cap(&parent_dir, temp, expected)? {
                exchange(&parent_dir, temp, name)?;
                parent_dir.remove_file(temp)?;
                sync_directory_chain(&parent_dir, Path::new(""))?;
                return Err(ApplyInvalidated {
                    path: record.path.clone(),
                }
                .into());
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
    executable: Option<bool>,
) -> Result<()> {
    use cap_std::fs::PermissionsExt;
    if matches!(expected, None | Some(Entry::Tombstone)) {
        match rename_noreplace(root, temp, target) {
            Ok(()) => {}
            Err(error)
                if error
                    .downcast_ref::<rustix::io::Errno>()
                    .is_some_and(|error| *error == rustix::io::Errno::EXIST)
                    || error
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|error| error.kind() == std::io::ErrorKind::AlreadyExists) =>
            {
                remove_entry(root, temp)?;
                sync_directory_chain(root, Path::new(""))?;
                return Err(InstallPreconditionChanged.into());
            }
            Err(error) => return Err(error),
        }
        return Ok(());
    }
    if let Some(executable) = executable {
        let mut mode = root.symlink_metadata(target)?.permissions().mode();
        if executable {
            mode |= 0o111;
        } else {
            mode &= !0o111;
        }
        root.set_permissions(temp, cap_std::fs::Permissions::from_mode(mode))?;
        sync_regular_file(root.as_fd(), temp)?;
    }
    exchange(root, temp, target)?;
    let expected = expected.expect("checked above");
    if !disk_matches_cap(root, temp, expected)? {
        exchange(root, temp, target)?;
        remove_entry(root, temp)?;
        sync_directory_chain(root, Path::new(""))?;
        return Err(InstallPreconditionChanged.into());
    }
    if let Err(error) = remove_entry(root, temp) {
        exchange(root, temp, target)?;
        remove_entry(root, temp)?;
        return Err(error.context("displaced entry could not be removed; replacement rolled back"));
    }
    Ok(())
}

fn map_precondition(error: anyhow::Error, path: &RelativePath) -> anyhow::Error {
    if error.downcast_ref::<InstallPreconditionChanged>().is_some() {
        ApplyInvalidated { path: path.clone() }.into()
    } else {
        error
    }
}

fn sync_applied_record(root: &cap_std::fs::Dir, record: &Record) -> Result<()> {
    let path = record.path.to_path_buf();
    let parent = path.parent().unwrap_or_else(|| Path::new(""));
    let (directories, parent_fd) = open_directory_chain(root, parent)?;
    if matches!(record.version.entry, Entry::File { .. }) {
        sync_regular_file(
            parent_fd.as_fd(),
            Path::new(path.file_name().context("entry has no basename")?),
        )?;
    }
    let mut directories = directories;
    if matches!(record.version.entry, Entry::Directory) {
        directories.push(rustix::fs::openat(
            parent_fd.as_fd(),
            Path::new(path.file_name().context("entry has no basename")?),
            directory_open_flags(),
            rustix::fs::Mode::empty(),
        )?);
    }
    sync_open_directories(&directories)
}

fn sync_regular_file(directory: impl AsFd, path: &Path) -> Result<()> {
    let fd = rustix::fs::openat(
        directory,
        path,
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )?;
    #[cfg(target_os = "macos")]
    rustix::fs::fcntl_fullfsync(&fd)?;
    #[cfg(not(target_os = "macos"))]
    rustix::fs::fsync(&fd)?;
    Ok(())
}

fn sync_directory_chain(root: &cap_std::fs::Dir, relative: &Path) -> Result<()> {
    let (directories, _) = open_directory_chain(root, relative)?;
    sync_open_directories(&directories)
}

fn directory_open_flags() -> rustix::fs::OFlags {
    rustix::fs::OFlags::RDONLY
        | rustix::fs::OFlags::DIRECTORY
        | rustix::fs::OFlags::NOFOLLOW
        | rustix::fs::OFlags::CLOEXEC
}

fn open_directory_chain(
    root: &cap_std::fs::Dir,
    relative: &Path,
) -> Result<(Vec<rustix::fd::OwnedFd>, rustix::fd::OwnedFd)> {
    let flags = directory_open_flags();
    let mut directories = vec![rustix::fs::openat(
        root.as_fd(),
        Path::new("."),
        flags,
        rustix::fs::Mode::empty(),
    )?];
    for component in relative.components() {
        let directory = rustix::fs::openat(
            directories
                .last()
                .context("directory chain is empty")?
                .as_fd(),
            Path::new(component.as_os_str()),
            flags,
            rustix::fs::Mode::empty(),
        )?;
        directories.push(directory);
    }
    let leaf = directories
        .last()
        .context("directory chain is empty")?
        .try_clone()?;
    Ok((directories, leaf))
}

fn sync_open_directories(directories: &[rustix::fd::OwnedFd]) -> Result<()> {
    for directory in directories.iter().rev() {
        rustix::fs::fsync(directory)?;
    }
    Ok(())
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

pub fn materialize_merges(state: &State, share: &ShareId, plan: &mut Plan) -> Result<()> {
    let outcomes = compute_merge_outcomes(state, &plan.merges)?;
    for (candidate, outcome) in plan.merges.clone().into_iter().zip(outcomes) {
        match outcome {
            Ok(merged) => {
                let mut result = candidate.winner.clone();
                result.version.peer = state.peer_id()?;
                result.version.sequence = state.next_sequence(share)?;
                result.version.id_authenticator = None;
                result.version.timestamp_ns = candidate
                    .winner
                    .version
                    .timestamp_ns
                    .max(candidate.loser.version.timestamp_ns);
                result.version.seen.clear();
                remember_version(&mut result.version.seen, &candidate.winner.version);
                remember_version(&mut result.version.seen, &candidate.loser.version);
                result.version.merge_base = Some(candidate.base.clone());
                result.version.version_authenticator = None;
                result.version.base_authenticator = None;
                result.version.entry = Entry::File {
                    hash: merged.hash.clone(),
                    size: merged.bytes.len() as u64,
                    executable: merged.executable,
                };
                state.authenticate_record(share, &mut result)?;
                let mut sink =
                    state.begin_object(merged.hash.clone(), merged.bytes.len() as u64)?;
                sink.write_chunk(&merged.bytes)?;
                sink.finish()?;
                state.mark_object_generated(share, &merged.hash)?;
                replace_candidate_result(plan, &candidate, result, merged.hunks)?;
            }
            Err(reason) => replace_candidate_fallback(plan, &candidate, reason),
        }
    }
    sort_conflicts(plan);
    Ok(())
}

/// Computes the exact dry-run result without reserving a Version ID or
/// publishing a generated object.
pub fn preview_merges(state: &State, plan: &mut Plan) -> Result<()> {
    let outcomes = compute_merge_outcomes(state, &plan.merges)?;
    for (candidate, outcome) in plan.merges.clone().into_iter().zip(outcomes) {
        match outcome {
            Ok(merged) => {
                let mut result = candidate.winner.clone();
                result.version.entry = Entry::File {
                    hash: merged.hash,
                    size: merged.bytes.len() as u64,
                    executable: merged.executable,
                };
                replace_candidate_result(plan, &candidate, result, merged.hunks)?;
            }
            Err(reason) => replace_candidate_fallback(plan, &candidate, reason),
        }
    }
    sort_conflicts(plan);
    Ok(())
}

pub fn validate_materialized_plan_shape(
    proposed: &Plan,
    expected: &Plan,
    connector: &PeerId,
) -> Result<()> {
    if proposed.merges != expected.merges {
        bail!("merge candidates differ from deterministic reconciliation");
    }
    let candidate_paths: std::collections::HashSet<_> = expected
        .merges
        .iter()
        .map(|candidate| candidate.path.as_bytes())
        .collect();
    let proposed_by_path: std::collections::HashMap<_, _> = proposed
        .records
        .iter()
        .map(|record| (record.path.as_bytes(), record))
        .collect();
    let greatest_connector_sequence = expected
        .records
        .iter()
        .chain(
            expected
                .merges
                .iter()
                .flat_map(|candidate| [&candidate.winner, &candidate.loser]),
        )
        .filter(|record| record.version.peer == *connector)
        .map(|record| record.version.sequence)
        .max()
        .unwrap_or(0);
    let mut merge_sequences = std::collections::HashSet::new();
    let mut merged_bytes = 0u64;
    for record in &expected.records {
        let proposed_record = proposed_by_path
            .get(record.path.as_bytes())
            .context("materialized plan is missing a reconciled path")?;
        if !candidate_paths.contains(record.path.as_bytes()) {
            if *proposed_record != record {
                bail!("materialized plan changed a non-merge record");
            }
            continue;
        }
        let candidate = expected
            .merges
            .iter()
            .find(|candidate| candidate.path == record.path)
            .expect("candidate path came from this plan");
        if proposed.conflicts.iter().any(|conflict| {
            conflict.path == record.path
                && matches!(
                    conflict.resolution,
                    crate::reconcile::ConflictResolution::WholeFile { .. }
                )
        }) {
            if **proposed_record != candidate.winner {
                bail!("fallback changed the deterministic whole-file winner");
            }
            continue;
        }
        let Entry::File {
            hash: _,
            size: proposed_size,
            executable: proposed_executable,
        } = &proposed_record.version.entry
        else {
            bail!("materialized merge result is not a file")
        };
        let Entry::File {
            executable: winner_executable,
            ..
        } = &candidate.winner.version.entry
        else {
            bail!("merge winner is not a file")
        };
        if proposed_executable != winner_executable {
            bail!("materialized merge changed executable metadata");
        }
        let mut permitted = candidate.winner.clone();
        permitted.version.peer = connector.clone();
        permitted.version.sequence = proposed_record.version.sequence;
        permitted.version.id_authenticator = proposed_record.version.id_authenticator.clone();
        permitted.version.timestamp_ns = candidate
            .winner
            .version
            .timestamp_ns
            .max(candidate.loser.version.timestamp_ns);
        permitted.version.seen.clear();
        remember_version(&mut permitted.version.seen, &candidate.winner.version);
        remember_version(&mut permitted.version.seen, &candidate.loser.version);
        permitted.version.merge_base = Some(candidate.base.clone());
        permitted.version.version_authenticator =
            proposed_record.version.version_authenticator.clone();
        permitted.version.base_authenticator = proposed_record.version.base_authenticator.clone();
        permitted.version.entry = proposed_record.version.entry.clone();
        if **proposed_record != permitted {
            bail!("materialized merge changed version metadata");
        }
        merged_bytes = merged_bytes.saturating_add(*proposed_size);
        if proposed_record.version.sequence <= greatest_connector_sequence
            || !merge_sequences.insert(proposed_record.version.sequence)
            || proposed_record.version.id_authenticator.is_none()
            || proposed_record.version.version_authenticator.is_none()
            || proposed_record.version.base_authenticator.is_none()
            || *proposed_size > crate::merge::MAX_OUTPUT_BYTES as u64
            || merged_bytes > MAX_MERGED_BYTES_PER_ROUND as u64
        {
            bail!("materialized merge lacks a fresh authenticated connector identity");
        }
    }
    if proposed.records.len() != expected.records.len() {
        bail!("materialized plan has an unexpected record count");
    }
    let expected_ordinary: Vec<_> = expected
        .conflicts
        .iter()
        .filter(|conflict| !candidate_paths.contains(conflict.path.as_bytes()))
        .collect();
    let proposed_ordinary: Vec<_> = proposed
        .conflicts
        .iter()
        .filter(|conflict| !candidate_paths.contains(conflict.path.as_bytes()))
        .collect();
    if proposed_ordinary != expected_ordinary {
        bail!("materialized plan changed an unrelated conflict");
    }
    for candidate in &expected.merges {
        let outcomes: Vec<_> = proposed
            .conflicts
            .iter()
            .filter(|conflict| conflict.path == candidate.path)
            .collect();
        if outcomes.len() > 1 {
            bail!("materialized plan has duplicate candidate outcomes");
        }
        if let Some(outcome) = outcomes.first()
            && (outcome.base.as_ref() != Some(&candidate.base)
                || !outcome.inputs.contains(&candidate.winner)
                || !outcome.inputs.contains(&candidate.loser))
        {
            bail!("materialized conflict does not match its merge candidate");
        }
    }
    Ok(())
}

pub fn verify_materialized_plan(state: &State, proposed: &Plan, metadata: &Plan) -> Result<()> {
    let outcomes = compute_merge_outcomes(state, &metadata.merges)?;
    for (candidate, outcome) in metadata.merges.iter().zip(outcomes) {
        let proposed_record = proposed
            .records
            .iter()
            .find(|record| record.path == candidate.path)
            .context("materialized plan is missing a merge result")?;
        let proposed_conflicts: Vec<_> = proposed
            .conflicts
            .iter()
            .filter(|conflict| conflict.path == candidate.path)
            .collect();
        match outcome {
            Ok(merged) => {
                let expected_entry = Entry::File {
                    hash: merged.hash,
                    size: merged.bytes.len() as u64,
                    executable: merged.executable,
                };
                if proposed_record.version.entry != expected_entry {
                    bail!("peer merge bytes differ from deterministic reconciliation");
                }
                if merged.hunks.is_empty() {
                    if !proposed_conflicts.is_empty() {
                        bail!("clean merge unexpectedly carries a recovery conflict");
                    }
                } else {
                    let expected_conflict = crate::reconcile::Conflict::merged(
                        candidate.base.clone(),
                        candidate.winner.clone(),
                        candidate.loser.clone(),
                        proposed_record.clone(),
                        merged.hunks,
                    );
                    if proposed_conflicts != vec![&expected_conflict] {
                        bail!("peer overlap recovery differs from deterministic reconciliation");
                    }
                }
            }
            Err(reason) => {
                let mut expected_conflict = crate::reconcile::Conflict::whole_file(
                    candidate.winner.clone(),
                    candidate.loser.clone(),
                    reason,
                );
                expected_conflict.base = Some(candidate.base.clone());
                if proposed_record != &candidate.winner
                    || proposed_conflicts != vec![&expected_conflict]
                {
                    bail!("peer merge fallback differs from deterministic reconciliation");
                }
            }
        }
    }
    Ok(())
}

struct ComputedMerge {
    bytes: Vec<u8>,
    hash: ObjectHash,
    executable: bool,
    hunks: Vec<crate::merge::ConflictHunk>,
}

struct MergeInput {
    base: Vec<u8>,
    winner: Vec<u8>,
    loser: Vec<u8>,
    executable: bool,
}

fn compute_merge_outcomes(
    state: &State,
    candidates: &[MergeCandidate],
) -> Result<Vec<std::result::Result<ComputedMerge, crate::merge::FallbackReason>>> {
    if candidates
        .windows(2)
        .any(|pair| pair[0].path.as_bytes() >= pair[1].path.as_bytes())
    {
        bail!("merge candidates are not in canonical path order");
    }
    let mut work = 0usize;
    let mut hunks = 0usize;
    let mut output = 0usize;
    let mut exhausted = false;
    let mut outcomes = Vec::with_capacity(candidates.len());
    for (index, candidate) in candidates.iter().enumerate() {
        if exhausted || index >= MAX_MERGE_CANDIDATES_PER_ROUND {
            exhausted = true;
            outcomes.push(Err(crate::merge::FallbackReason::RoundMergeBudget));
            continue;
        }
        if candidate_file_sizes(candidate)?
            .into_iter()
            .any(|size| size > crate::merge::MAX_INPUT_BYTES as u64)
        {
            outcomes.push(Err(crate::merge::FallbackReason::InputBytes));
            continue;
        }
        let input = read_candidate(state, candidate)?;
        let candidate_work =
            match crate::merge::comparison_work(&input.base, &input.winner, &input.loser) {
                Ok(work) if work <= crate::merge::MAX_WORK => work,
                Ok(_) => {
                    outcomes.push(Err(crate::merge::FallbackReason::ComparisonWork));
                    continue;
                }
                Err(reason) => {
                    outcomes.push(Err(reason));
                    continue;
                }
            };
        if work
            .checked_add(candidate_work)
            .is_none_or(|total| total > MAX_MERGE_WORK_PER_ROUND)
        {
            exhausted = true;
            outcomes.push(Err(crate::merge::FallbackReason::RoundMergeBudget));
            continue;
        }
        work += candidate_work;
        match crate::merge::merge(&input.base, &input.winner, &input.loser, true) {
            Ok(merged) => {
                if hunks
                    .checked_add(merged.hunks.len())
                    .is_none_or(|total| total > MAX_MERGE_HUNKS_PER_ROUND)
                    || output
                        .checked_add(merged.bytes.len())
                        .is_none_or(|total| total > MAX_MERGED_BYTES_PER_ROUND)
                {
                    exhausted = true;
                    outcomes.push(Err(crate::merge::FallbackReason::RoundMergeBudget));
                    continue;
                }
                hunks += merged.hunks.len();
                output += merged.bytes.len();
                let hash = ObjectHash::from_blake3(blake3::hash(&merged.bytes));
                outcomes.push(Ok(ComputedMerge {
                    bytes: merged.bytes,
                    hash,
                    executable: input.executable,
                    hunks: merged.hunks,
                }));
            }
            Err(reason) => outcomes.push(Err(reason)),
        }
    }
    Ok(outcomes)
}

fn candidate_file_sizes(candidate: &MergeCandidate) -> Result<[u64; 3]> {
    let Entry::File { size: base, .. } = candidate.base.entry else {
        bail!("merge base is not a file")
    };
    let Entry::File { size: winner, .. } = candidate.winner.version.entry else {
        bail!("merge winner is not a file")
    };
    let Entry::File { size: loser, .. } = candidate.loser.version.entry else {
        bail!("merge loser is not a file")
    };
    Ok([base, winner, loser])
}

fn read_candidate(state: &State, candidate: &MergeCandidate) -> Result<MergeInput> {
    let Entry::File {
        hash: base_hash, ..
    } = &candidate.base.entry
    else {
        bail!("merge base is not a file")
    };
    let Entry::File {
        hash: winner_hash,
        executable,
        ..
    } = &candidate.winner.version.entry
    else {
        bail!("merge winner is not a file")
    };
    let Entry::File {
        hash: loser_hash, ..
    } = &candidate.loser.version.entry
    else {
        bail!("merge loser is not a file")
    };
    Ok(MergeInput {
        base: state.read_object(base_hash)?,
        winner: state.read_object(winner_hash)?,
        loser: state.read_object(loser_hash)?,
        executable: *executable,
    })
}

fn replace_candidate_result(
    plan: &mut Plan,
    candidate: &MergeCandidate,
    result: Record,
    hunks: Vec<crate::merge::ConflictHunk>,
) -> Result<()> {
    let record = plan
        .records
        .iter_mut()
        .find(|record| record.path == candidate.path)
        .context("merge candidate is missing its result record")?;
    *record = result;
    plan.conflicts
        .retain(|conflict| conflict.path != candidate.path);
    if !hunks.is_empty() {
        plan.conflicts.push(crate::reconcile::Conflict::merged(
            candidate.base.clone(),
            candidate.winner.clone(),
            candidate.loser.clone(),
            record.clone(),
            hunks,
        ));
    }
    Ok(())
}

fn replace_candidate_fallback(
    plan: &mut Plan,
    candidate: &MergeCandidate,
    reason: crate::merge::FallbackReason,
) {
    let mut conflict = crate::reconcile::Conflict::whole_file(
        candidate.winner.clone(),
        candidate.loser.clone(),
        reason,
    );
    conflict.base = Some(candidate.base.clone());
    plan.conflicts
        .retain(|conflict| conflict.path != candidate.path);
    plan.conflicts.push(conflict);
}

fn sort_conflicts(plan: &mut Plan) {
    plan.conflicts
        .sort_by(|left, right| left.path.as_bytes().cmp(right.path.as_bytes()));
}

fn remember_version(seen: &mut Vec<crate::model::VersionId>, version: &crate::model::Version) {
    for item in version
        .seen
        .iter()
        .cloned()
        .chain(std::iter::once(version.id()))
    {
        if let Some(existing) = seen.iter_mut().find(|known| known.peer == item.peer) {
            if item.sequence > existing.sequence {
                *existing = item;
            }
        } else {
            seen.push(item);
        }
    }
    seen.sort_by(|left, right| left.peer.0.cmp(&right.peer.0));
}

pub fn plan_records_with_inputs(plan: &Plan) -> Vec<Record> {
    let mut records = plan.records.clone();
    for conflict in &plan.conflicts {
        records.extend(conflict.inputs.clone());
        if let Some(base) = &conflict.base {
            let mut record = conflict.inputs[0].clone();
            record.version.entry = base.entry.clone();
            records.push(record);
        }
        if let Some(merged) = &conflict.merged {
            records.push(merged.clone());
        }
    }
    for candidate in &plan.merges {
        records.push(candidate.winner.clone());
        records.push(candidate.loser.clone());
        let mut base = candidate.winner.clone();
        base.version.entry = candidate.base.entry.clone();
        records.push(base);
    }
    records
}

pub fn authorized_hashes(records: &[Record]) -> std::collections::HashSet<ObjectHash> {
    let mut hashes = std::collections::HashSet::new();
    for record in records {
        if let Entry::File { hash, .. } = &record.version.entry {
            hashes.insert(hash.clone());
        }
        if let Some(base) = &record.version.merge_base
            && let Entry::File { hash, .. } = &base.entry
        {
            hashes.insert(hash.clone());
        }
    }
    hashes
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

pub fn receive_object_for_share(
    state: &State,
    share: &ShareId,
    hash: ObjectHash,
    size: u64,
    reader: &mut impl Read,
) -> Result<()> {
    state.mark_object_receiving(share, &hash)?;
    receive_object(state, hash.clone(), size, reader)?;
    state.mark_object_verified(share, &hash)
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
    let candidates = records
        .iter()
        .filter(|record| !matcher.is_record_ignored(record))
        .filter_map(|record| match &record.version.entry {
            Entry::File { hash, .. } => Some(hash.clone()),
            _ => None,
        })
        .collect();
    let authorized = state.share_authorized_objects(share, &candidates)?;
    let mut hashes = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for record in records {
        if matcher.is_record_ignored(record) {
            continue;
        }
        if let Entry::File { hash, size, .. } = &record.version.entry
            && seen.insert(hash.clone())
        {
            match state.open_verified_object(hash) {
                Ok(file) if file.metadata()?.len() != *size => {
                    bail!("stored verified object size differs from the validated record")
                }
                Ok(_) if authorized.contains(hash) => {}
                Ok(_) => hashes.push(hash.clone()),
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
    record: &Record,
    cache: &mut std::collections::HashMap<(Vec<u8>, u8), bool>,
) -> bool {
    let kind = match record.version.entry {
        Entry::Directory => 1,
        Entry::Tombstone => 2,
        _ => 0,
    };
    let key = (record.path.as_bytes().to_vec(), kind);
    if let Some(ignored) = cache.get(&key) {
        return *ignored;
    }
    let ignored = matcher.is_record_ignored(record);
    cache.insert(key, ignored);
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

    fn merge_record(
        peer: &str,
        sequence: u64,
        bytes: &[u8],
        base: Option<crate::model::BaseVersion>,
    ) -> Record {
        let mut seen = Vec::new();
        if let Some(base) = &base {
            seen.push(base.id.clone());
        }
        Record {
            path: RelativePath::from_bytes(b"shared.txt".to_vec()).unwrap(),
            version: Version {
                peer: PeerId(peer.into()),
                sequence,
                id_authenticator: None,
                timestamp_ns: sequence as i64,
                seen,
                merge_base: base,
                version_authenticator: None,
                base_authenticator: None,
                entry: Entry::File {
                    hash: ObjectHash::from_blake3(blake3::hash(bytes)),
                    size: bytes.len() as u64,
                    executable: false,
                },
            },
        }
    }

    #[test]
    fn materializes_and_verifies_clean_overlap_and_fallback_outcomes() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path().join("root");
        fs::create_dir(&root)?;
        let state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        let base_bytes = b"top\nmiddle\nbottom\n";
        let base_record = merge_record("base", 1, base_bytes, None);
        let base = base_record.version.as_base().unwrap();
        let a_bytes = b"TOP\nmiddle\nbottom\n";
        let b_bytes = b"top\nmiddle\nBOTTOM\n";
        for bytes in [
            base_bytes.as_slice(),
            a_bytes.as_slice(),
            b_bytes.as_slice(),
        ] {
            let hash = ObjectHash::from_blake3(blake3::hash(bytes));
            state.import_object(&hash, bytes)?;
        }
        let a = merge_record("a", 2, a_bytes, Some(base.clone()));
        let b = merge_record("b", 2, b_bytes, Some(base.clone()));
        assert!(validate_ack_heads(std::slice::from_ref(&a), &[a.clone(), a.clone()]).is_err());
        let metadata = plan(std::slice::from_ref(&a), std::slice::from_ref(&b));
        assert_eq!(metadata.merges.len(), 1);
        let mut preview = metadata.clone();
        preview_merges(&state, &mut preview)?;
        assert!(preview.conflicts.is_empty());
        let mut clean = metadata.clone();
        materialize_merges(&state, &share, &mut clean)?;
        assert!(clean.conflicts.is_empty());
        validate_materialized_plan_shape(&clean, &metadata, &state.peer_id()?)?;
        verify_materialized_plan(&state, &clean, &metadata)?;
        let Entry::File { hash, .. } = &clean.records[0].version.entry else {
            unreachable!()
        };
        assert_eq!(state.read_object(hash)?, b"TOP\nmiddle\nBOTTOM\n");

        let connector = state.peer_id()?;
        let mut invalid = clean.clone();
        invalid.merges.clear();
        assert!(validate_materialized_plan_shape(&invalid, &metadata, &connector).is_err());
        let mut invalid = clean.clone();
        invalid.records.clear();
        assert!(validate_materialized_plan_shape(&invalid, &metadata, &connector).is_err());
        let mut invalid = clean.clone();
        invalid.records[0].version.entry = Entry::Directory;
        assert!(validate_materialized_plan_shape(&invalid, &metadata, &connector).is_err());
        let mut invalid = clean.clone();
        if let Entry::File { executable, .. } = &mut invalid.records[0].version.entry {
            *executable = true;
        }
        assert!(validate_materialized_plan_shape(&invalid, &metadata, &connector).is_err());
        let mut invalid = clean.clone();
        invalid.records[0].version.peer = PeerId("forged".into());
        assert!(validate_materialized_plan_shape(&invalid, &metadata, &connector).is_err());
        let mut invalid = clean.clone();
        invalid.records.push(invalid.records[0].clone());
        assert!(validate_materialized_plan_shape(&invalid, &metadata, &connector).is_err());
        let mut invalid = clean.clone();
        if let Entry::File { hash, .. } = &mut invalid.records[0].version.entry {
            *hash = ObjectHash::from_blake3(blake3::hash(b"forged"));
        }
        assert!(verify_materialized_plan(&state, &invalid, &metadata).is_err());

        let overlap_bytes = b"other\nmiddle\nBOTTOM\n";
        let overlap_hash = ObjectHash::from_blake3(blake3::hash(overlap_bytes));
        state.import_object(&overlap_hash, overlap_bytes)?;
        let overlap = merge_record("b", 3, overlap_bytes, Some(base));
        let metadata = plan(&[a], &[overlap]);
        let mut materialized = metadata.clone();
        materialize_merges(&state, &share, &mut materialized)?;
        assert!(matches!(
            materialized.conflicts[0].resolution,
            crate::reconcile::ConflictResolution::MergedWithOverlaps
        ));
        validate_materialized_plan_shape(&materialized, &metadata, &state.peer_id()?)?;
        verify_materialized_plan(&state, &materialized, &metadata)?;
        let mut invalid = materialized.clone();
        invalid.conflicts[0].hunks.clear();
        assert!(verify_materialized_plan(&state, &invalid, &metadata).is_err());
        let mut invalid = materialized.clone();
        invalid.conflicts.push(invalid.conflicts[0].clone());
        assert!(validate_materialized_plan_shape(&invalid, &metadata, &state.peer_id()?).is_err());

        let binary = b"\0binary";
        let binary_hash = ObjectHash::from_blake3(blake3::hash(binary));
        state.import_object(&binary_hash, binary)?;
        let binary = merge_record("b", 4, binary, materialized.conflicts[0].base.clone());
        let metadata = plan(&[materialized.conflicts[0].inputs[0].clone()], &[binary]);
        if !metadata.merges.is_empty() {
            let mut preview = metadata.clone();
            preview_merges(&state, &mut preview)?;
            let mut fallback = metadata.clone();
            materialize_merges(&state, &share, &mut fallback)?;
            assert!(matches!(
                fallback.conflicts[0].resolution,
                crate::reconcile::ConflictResolution::WholeFile {
                    reason: crate::merge::FallbackReason::ContainsNul,
                    ..
                }
            ));
            validate_materialized_plan_shape(&fallback, &metadata, &state.peer_id()?)?;
            verify_materialized_plan(&state, &fallback, &metadata)?;
            let mut invalid = fallback.clone();
            invalid.records[0] = invalid.conflicts[0].loser().clone();
            assert!(verify_materialized_plan(&state, &invalid, &metadata).is_err());
        }
        Ok(())
    }

    #[test]
    fn cumulative_merge_budget_falls_back_from_the_first_exhausted_candidate() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path().join("root");
        fs::create_dir(&root)?;
        let state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        let base_bytes = b"left\nright\n";
        let a_bytes = b"LEFT\nright\n";
        let b_bytes = b"left\nRIGHT\n";
        for bytes in [
            base_bytes.as_slice(),
            a_bytes.as_slice(),
            b_bytes.as_slice(),
        ] {
            let hash = ObjectHash::from_blake3(blake3::hash(bytes));
            state.import_object(&hash, bytes)?;
        }
        let base = merge_record("base", 1, base_bytes, None)
            .version
            .as_base()
            .unwrap();
        let a = merge_record("a", 2, a_bytes, Some(base.clone()));
        let b = merge_record("b", 2, b_bytes, Some(base));
        let candidate = plan(&[a], &[b]).merges.remove(0);
        let mut many = Plan {
            records: Vec::new(),
            conflicts: Vec::new(),
            merges: Vec::new(),
        };
        for index in 0..=MAX_MERGE_CANDIDATES_PER_ROUND {
            let path = RelativePath::from_bytes(format!("{index:03}.txt").into_bytes())?;
            let mut next = candidate.clone();
            next.path = path.clone();
            next.winner.path = path.clone();
            next.loser.path = path.clone();
            many.records.push(next.winner.clone());
            many.merges.push(next);
        }
        materialize_merges(&state, &share, &mut many)?;
        assert_eq!(many.conflicts.len(), 1);
        assert!(matches!(
            many.conflicts[0].resolution,
            crate::reconcile::ConflictResolution::WholeFile {
                reason: crate::merge::FallbackReason::RoundMergeBudget,
                ..
            }
        ));
        assert_eq!(many.conflicts[0].path.as_bytes(), b"064.txt");
        Ok(())
    }

    #[test]
    fn install_precondition_errors_become_path_specific_invalidations() -> Result<()> {
        let path = RelativePath::from_bytes(b"changing/path".to_vec())?;
        assert_eq!(
            InstallPreconditionChanged.to_string(),
            "install precondition changed"
        );
        let error = map_precondition(InstallPreconditionChanged.into(), &path);
        let invalidated = error
            .downcast_ref::<ApplyInvalidated>()
            .expect("install preconditions have a typed public result");
        assert_eq!(invalidated.path, path);
        assert_eq!(
            invalidated.to_string(),
            "path changed while applying: changing/path"
        );

        let ordinary = map_precondition(anyhow::anyhow!("ordinary failure"), &path);
        assert_eq!(ordinary.to_string(), "ordinary failure");
        Ok(())
    }

    #[test]
    fn rejects_oversized_frame_before_allocation() {
        let bytes = ((MAX_FRAME as u32) + 1).to_be_bytes().to_vec();
        assert!(read_message(&mut bytes.as_slice()).is_err());
    }

    #[cfg(feature = "e2e-test-hooks")]
    #[test]
    fn e2e_apply_stop_marker_is_regular_and_one_shot() -> Result<()> {
        let temp = tempdir()?;
        let state = State::open(temp.path().join("state"))?;
        assert!(!e2e_claim_apply_stop(&state)?);

        let marker = state.dir.join(".e2e-stop-before-apply");
        fs::write(&marker, b"")?;
        assert!(e2e_claim_apply_stop(&state)?);
        assert!(!e2e_claim_apply_stop(&state)?);

        fs::write(&marker, b"2")?;
        assert!(e2e_claim_apply_stop(&state)?);
        assert!(e2e_claim_apply_stop(&state)?);
        assert!(!e2e_claim_apply_stop(&state)?);

        fs::write(&marker, b"3")?;
        assert!(e2e_claim_apply_stop(&state).is_err());
        fs::remove_file(state.dir.join(".e2e-stop-before-apply-claimed"))?;

        fs::create_dir(&marker)?;
        assert!(e2e_claim_apply_stop(&state).is_err());
        Ok(())
    }

    #[cfg(feature = "e2e-test-hooks")]
    #[test]
    fn e2e_apply_stop_pid_is_created_without_following_an_existing_entry() -> Result<()> {
        let temp = tempdir()?;
        let state = State::open(temp.path().join("state"))?;
        let pidfile = state.dir.join(".e2e-apply-stop.pid");
        e2e_publish_apply_stop_pid(&state)?;
        assert_eq!(
            fs::read_to_string(&pidfile)?,
            std::process::id().to_string()
        );
        assert!(e2e_publish_apply_stop_pid(&state).is_err());
        fs::remove_file(&pidfile)?;
        std::os::unix::fs::symlink(temp.path().join("target"), &pidfile)?;
        assert!(e2e_publish_apply_stop_pid(&state).is_err());
        Ok(())
    }

    #[test]
    fn no_replace_race_removes_the_owned_staged_entry() -> Result<()> {
        let temp = tempdir()?;
        fs::create_dir(temp.path().join("staged"))?;
        fs::write(temp.path().join("target"), b"concurrent")?;
        let root = cap_std::fs::Dir::open_ambient_dir(temp.path(), cap_std::ambient_authority())?;

        let error = atomic_install(&root, Path::new("staged"), Path::new("target"), None, None)
            .expect_err("the concurrent target must invalidate the no-replace install");

        assert!(error.downcast_ref::<InstallPreconditionChanged>().is_some());
        assert!(!temp.path().join("staged").exists());
        assert_eq!(fs::read(temp.path().join("target"))?, b"concurrent");
        Ok(())
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
                        id_authenticator: None,
                        timestamp_ns: sequence as i64,
                        seen: Vec::new(),
                        merge_base: None,
                        version_authenticator: None,
                        base_authenticator: None,
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
                merges: Vec::new(),
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
    fn round_budget_is_absolute_and_cumulative() {
        let expired = RoundBudget::new(Instant::now() - Duration::from_millis(1));
        assert!(expired.check().is_err());
        assert!(expired.frame_deadline().is_err());
        assert!(expired.phase_deadline().is_err());

        let mut metadata = RoundBudget::new(Instant::now() + Duration::from_secs(1));
        metadata
            .add_metadata(MAX_METADATA_BYTES_PER_SESSION)
            .unwrap();
        assert!(metadata.add_metadata(1).is_err());

        let mut transfer = RoundBudget::new(Instant::now() + Duration::from_secs(1));
        transfer
            .add_transfer(max_transfer_bytes_per_session())
            .unwrap();
        assert!(transfer.add_transfer(1).is_err());
    }

    #[test]
    fn phase_read_starts_its_frame_timeout_at_the_first_byte() -> Result<()> {
        use std::os::unix::net::UnixStream;

        let (reader, writer) = UnixStream::pair()?;
        let sender = std::thread::spawn(move || -> Result<()> {
            std::thread::sleep(Duration::from_millis(50));
            write_v2_envelope_until(
                &writer,
                &V2Envelope::Session {
                    frame: V2SessionFrame::Ping { nonce: 7 },
                },
                Instant::now() + Duration::from_secs(1),
            )
        });
        assert!(matches!(
            read_v2_envelope_in_phase_with_frame_timeout(
                &reader,
                Instant::now() + Duration::from_secs(1),
                Duration::from_millis(20),
            )?,
            V2Envelope::Session {
                frame: V2SessionFrame::Ping { nonce: 7 }
            }
        ));
        sender.join().expect("sender did not panic")
    }

    #[test]
    fn phase_read_does_not_renew_incomplete_frame_deadlines() -> Result<()> {
        use std::io::Write as _;
        use std::os::unix::net::UnixStream;

        let (mut prefix_writer, prefix_reader) = UnixStream::pair()?;
        prefix_writer.write_all(&[0])?;
        let prefix_error = read_v2_envelope_in_phase_with_frame_timeout(
            &prefix_reader,
            Instant::now() + Duration::from_secs(1),
            Duration::from_millis(20),
        )
        .expect_err("an incomplete prefix must time out");
        assert!(prefix_error.to_string().contains("deadline exceeded"));

        let (mut body_writer, body_reader) = UnixStream::pair()?;
        body_writer.write_all(&1u32.to_be_bytes())?;
        let body_error = read_v2_envelope_in_phase_with_frame_timeout(
            &body_reader,
            Instant::now() + Duration::from_secs(1),
            Duration::from_millis(20),
        )
        .expect_err("an incomplete body must time out");
        assert!(body_error.to_string().contains("deadline exceeded"));
        Ok(())
    }

    #[test]
    fn phase_read_does_not_renew_a_slow_drip_body() -> Result<()> {
        use std::io::Write as _;
        use std::os::unix::net::UnixStream;

        let envelope = V2Envelope::Session {
            frame: V2SessionFrame::Ping { nonce: 9 },
        };
        let body = serde_json::to_vec(&envelope)?;
        let (mut writer, reader) = UnixStream::pair()?;
        let sender = std::thread::spawn(move || {
            if writer
                .write_all(&(body.len() as u32).to_be_bytes())
                .is_err()
            {
                return;
            }
            for byte in body {
                if writer.write_all(&[byte]).is_err() {
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        });
        let error = read_v2_envelope_in_phase_with_frame_timeout(
            &reader,
            Instant::now() + Duration::from_secs(1),
            Duration::from_millis(40),
        )
        .expect_err("body bytes must not renew the frame deadline");
        assert!(error.to_string().contains("deadline exceeded"));
        drop(reader);
        sender.join().expect("sender did not panic");
        Ok(())
    }

    #[test]
    fn phase_read_enforces_its_absolute_cap_and_frame_size() -> Result<()> {
        use std::io::Write as _;
        use std::os::unix::net::UnixStream;

        let (_silent_writer, silent_reader) = UnixStream::pair()?;
        let phase_error = read_v2_envelope_in_phase_with_frame_timeout(
            &silent_reader,
            Instant::now() + Duration::from_millis(20),
            Duration::from_secs(1),
        )
        .expect_err("a silent phase must time out");
        assert!(
            format!("{phase_error:#}").contains("waiting for persistent protocol phase result")
        );

        let (mut writer, reader) = UnixStream::pair()?;
        writer.write_all(&((MAX_FRAME + 1) as u32).to_be_bytes())?;
        let size_error = read_v2_envelope_in_phase_with_frame_timeout(
            &reader,
            Instant::now() + Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .expect_err("an oversized frame must fail before allocation");
        assert!(size_error.to_string().contains("protocol frame too large"));
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
                id_authenticator: None,
                timestamp_ns: 1,
                seen: Vec::new(),
                merge_base: None,
                version_authenticator: None,
                base_authenticator: None,
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
