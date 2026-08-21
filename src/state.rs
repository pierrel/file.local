use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use anyhow::{Context, Result, bail};
use directories::ProjectDirs;
use fs2::FileExt;
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};

use crate::model::{
    Entry, ObjectHash, PeerConfig, PeerId, Record, RelationshipId, RelativePath, ShareId, Version,
    VersionId,
};
use crate::reconcile::{Conflict, ConflictResolution};

pub const DEFAULT_RECOVERY_BUDGET_BYTES: u64 = 10 * 1024 * 1024 * 1024;
pub const RECOVERY_ROW_OVERHEAD_BYTES: u64 = 256;
const MAX_ALL_PRUNE_SUMMARIES: u64 = 10_000;
const MAX_ALL_PRUNE_SUMMARY_BYTES: u64 = 8 * 1024 * 1024;
const MAX_SYNC_QUEUE_ROWS: i64 = 1024;
const MAX_SCHEDULER_DIRECTORY_ENTRIES: usize = 2048;
const STALE_QUEUE_CLEANUP_LIMIT: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncOperation {
    Recovery,
    Initial,
    Sync,
    Watch,
    Registration,
    Removal,
    Maintenance,
}

impl SyncOperation {
    fn as_str(self) -> &'static str {
        match self {
            Self::Recovery => "recovery",
            Self::Initial => "initial",
            Self::Sync => "sync",
            Self::Watch => "watch",
            Self::Registration => "registration",
            Self::Removal => "removal",
            Self::Maintenance => "maintenance",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "recovery" => Ok(Self::Recovery),
            "initial" => Ok(Self::Initial),
            "sync" => Ok(Self::Sync),
            "watch" => Ok(Self::Watch),
            "registration" => Ok(Self::Registration),
            "removal" => Ok(Self::Removal),
            "maintenance" => Ok(Self::Maintenance),
            _ => bail!("stored synchronization operation is invalid"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PairedQueueState {
    PendingAuthority,
    Parked,
    Prepared,
    Eligible,
}

impl PairedQueueState {
    fn as_str(self) -> &'static str {
        match self {
            Self::PendingAuthority => "pending_authority",
            Self::Parked => "parked",
            Self::Prepared => "prepared",
            Self::Eligible => "eligible",
        }
    }

    fn parse(value: Option<&str>) -> Result<Option<Self>> {
        value
            .map(|value| match value {
                "pending_authority" => Ok(Self::PendingAuthority),
                "parked" => Ok(Self::Parked),
                "prepared" => Ok(Self::Prepared),
                "eligible" => Ok(Self::Eligible),
                _ => bail!("stored paired scheduling state is invalid"),
            })
            .transpose()
    }

    fn eligible(self) -> bool {
        matches!(self, Self::Eligible)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct QueuePosition {
    pub ticket: i64,
    pub position: usize,
    pub active: Option<ScheduledRequestSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct ScheduledRequestSnapshot {
    pub ticket: i64,
    pub token: String,
    pub share: Option<ShareId>,
    pub relationship: Option<RelationshipId>,
    pub operation: SyncOperation,
    pub generation: Option<i64>,
    pub network_authority: Option<PeerId>,
    pub network_order: Option<i64>,
    pub paired_state: Option<PairedQueueState>,
    pub proxy_acknowledged: bool,
    pub active: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct SchedulingSnapshot {
    pub completion_sequence: i64,
    pub active: Option<ScheduledRequestSnapshot>,
    pub queued: Vec<ScheduledRequestSnapshot>,
}

impl SchedulingSnapshot {
    pub fn queue_position(&self, target: &ScheduledRequestSnapshot) -> Option<usize> {
        self.queued
            .iter()
            .position(|request| request.ticket == target.ticket)
            .map(|index| index + 1)
    }

    /// Returns the requests that may compete for the installation slot, in
    /// local FIFO order. Paired requests first retain their authority-local
    /// network order; the head from each authority then competes by ticket
    /// with ordinary local requests.
    pub fn eligible_candidates(&self) -> Vec<&ScheduledRequestSnapshot> {
        eligible_candidates(&self.queued)
    }

    /// Returns the eligible requests that must run before `target` under the
    /// same ordering used by activation.
    pub fn eligible_predecessors(
        &self,
        target: &ScheduledRequestSnapshot,
    ) -> Vec<&ScheduledRequestSnapshot> {
        eligible_predecessors(&self.queued, target)
    }
}

fn eligible_candidates(queued: &[ScheduledRequestSnapshot]) -> Vec<&ScheduledRequestSnapshot> {
    let mut authority_heads = std::collections::HashMap::new();
    for request in queued.iter().filter(|request| {
        request.paired_state == Some(PairedQueueState::Eligible)
            && request.network_authority.is_some()
    }) {
        let authority = request
            .network_authority
            .as_ref()
            .expect("filtered paired request has an authority");
        authority_heads
            .entry(authority)
            .and_modify(|head: &mut &ScheduledRequestSnapshot| {
                if (request.network_order, request.ticket) < (head.network_order, head.ticket) {
                    *head = request;
                }
            })
            .or_insert(request);
    }
    queued
        .iter()
        .filter(|request| {
            if request.paired_state.is_none() {
                return true;
            }
            if request.paired_state != Some(PairedQueueState::Eligible) {
                return false;
            }
            let Some(authority) = request.network_authority.as_ref() else {
                return false;
            };
            authority_heads
                .get(authority)
                .is_some_and(|head| head.ticket == request.ticket)
        })
        .collect()
}

fn eligible_predecessors<'a>(
    queued: &'a [ScheduledRequestSnapshot],
    target: &ScheduledRequestSnapshot,
) -> Vec<&'a ScheduledRequestSnapshot> {
    eligible_candidates(queued)
        .into_iter()
        .filter(|candidate| {
            let same_authority = target.network_authority.is_some()
                && candidate.network_authority == target.network_authority;
            (same_authority && candidate.network_order < target.network_order)
                || candidate.ticket < target.ticket
        })
        .collect()
}

#[derive(Debug)]
pub struct QueueCancelled;

impl std::fmt::Display for QueueCancelled {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("synchronization request was canceled")
    }
}

impl std::error::Error for QueueCancelled {}

#[derive(Debug)]
pub struct QueueRejoin;

impl std::fmt::Display for QueueRejoin {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("synchronization request must rejoin the queue")
    }
}

impl std::error::Error for QueueRejoin {}

pub struct QueueRequest {
    state_dir: PathBuf,
    ticket: i64,
    token: String,
    share: Option<ShareId>,
    generation: Option<i64>,
    owner: Option<File>,
}

pub struct InstallationPermit {
    request: Option<QueueRequest>,
    global: Option<File>,
}

type ActivationRow = (i64, Option<String>, Option<i64>, Option<String>);

#[derive(Clone, Debug, serde::Serialize)]
pub struct RecoveryUsage {
    pub conflicts: u64,
    pub conflict_limit: u64,
    pub conflicts_remaining: u64,
    pub object_bytes: u64,
    pub metadata_bytes: u64,
    pub metadata_limit_bytes: u64,
    pub metadata_remaining_bytes: u64,
    pub used_bytes: u64,
    pub budget_bytes: u64,
    pub remaining_bytes: u64,
    pub reclaimable_bytes: u64,
    pub over_budget: bool,
    pub over_conflict_limit: bool,
    pub over_metadata_limit: bool,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct RecoveryPruneConflict {
    pub id: String,
    pub path: RelativePath,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct RecoveryPrunePlan {
    pub conflicts: Vec<RecoveryPruneConflict>,
    pub selection_token: String,
    pub released_bytes: u64,
    pub reclaimable_bytes: u64,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct RecoveryPruneOutcome {
    pub plan: RecoveryPrunePlan,
    pub collection_pending: bool,
}

#[derive(Clone, serde::Serialize)]
struct RawConflictRow {
    id: String,
    path: Vec<u8>,
    winner: String,
    loser: String,
    created_ns: String,
    document: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryLimitKind {
    BudgetBytes,
    ConflictCount,
    MetadataBytes,
}

#[derive(Debug)]
pub struct RecoveryLimitExceeded {
    pub kind: RecoveryLimitKind,
    pub current: u64,
    pub projected: u64,
    pub limit: u64,
}

impl std::fmt::Display for RecoveryLimitExceeded {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (name, remediation) = match self.kind {
            RecoveryLimitKind::BudgetBytes => (
                "recovery storage budget",
                "prune conflicts or raise the recovery budget",
            ),
            RecoveryLimitKind::ConflictCount => {
                ("recovery conflict count", "prune recovery conflicts")
            }
            RecoveryLimitKind::MetadataBytes => {
                ("recovery metadata limit", "prune recovery conflicts")
            }
        };
        write!(
            formatter,
            "{name} exceeded: current {}, projected {}, limit {}; {remediation}",
            self.current, self.projected, self.limit
        )
    }
}

impl std::error::Error for RecoveryLimitExceeded {}

#[derive(Debug)]
pub struct RootIdentityChanged(String);

impl RootIdentityChanged {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl std::fmt::Display for RootIdentityChanged {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for RootIdentityChanged {}

#[derive(Clone, serde::Serialize)]
pub struct StoredConflict {
    pub id: String,
    pub path: RelativePath,
    pub winner: Record,
    pub loser: Record,
    pub resolution: ConflictResolution,
    pub base: Option<crate::model::BaseVersion>,
    pub inputs: [Record; 2],
    pub merged: Option<Record>,
    pub hunks: Vec<crate::merge::ConflictHunk>,
    pub created_ns: i64,
}

#[derive(Clone, serde::Serialize)]
pub struct ManagedShare {
    pub id: ShareId,
    pub root: PathBuf,
    pub binding: EndpointBinding,
    pub initial_complete: bool,
    pub watch_enabled: bool,
    pub blocked_diagnostic: Option<String>,
    pub removing_relationship: Option<RelationshipId>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum EndpointBinding {
    Connector(PeerConfig),
    Responder {
        peer: PeerId,
        relationship: Option<RelationshipId>,
    },
    Unpaired,
}

impl EndpointBinding {
    pub fn relationship(&self) -> Option<&RelationshipId> {
        match self {
            Self::Connector(peer) => peer.relationship.as_ref(),
            Self::Responder { relationship, .. } => relationship.as_ref(),
            Self::Unpaired => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PreparedRemoval {
    pub share: ShareId,
    pub relationship: RelationshipId,
    pub binding: EndpointBinding,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegistrationOutcome {
    pub prior_share: Option<ShareId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IncomingRemoval {
    Absent,
    Prepared(PreparedRemoval),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Detached {
    pub cleanup_warning: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemovalFailureState {
    Pending,
    Finalized,
    Changed,
}

pub struct State {
    pub dir: PathBuf,
    conn: Connection,
    _installation_barrier: File,
}

pub struct UpgradeBarrier(File);

pub struct LegacyUpgradeLocks {
    _files: Vec<File>,
}

pub enum UpgradeLockAttempt {
    Acquired(LegacyUpgradeLocks),
    Busy(PathBuf),
}

#[derive(Debug)]
pub struct UpgradePending;

impl std::fmt::Display for UpgradePending {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "upgrade is in progress; rerun `make install`")
    }
}

impl std::error::Error for UpgradePending {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RootIdentity {
    pub device: u64,
    pub inode: u64,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct InstallIntent {
    pub records: Vec<Record>,
    #[serde(default)]
    pub conflicts: Vec<Conflict>,
    pub temps: Vec<InstallTemp>,
    #[serde(default)]
    pub managed_generation: Option<i64>,
}

#[derive(Clone)]
pub struct InstallRecoveryIntent {
    pub share: ShareId,
    pub intent: InstallIntent,
    pub fingerprint: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstallIntentFailure {
    pub fingerprint: String,
    pub diagnostic: String,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct InstallTemp {
    pub path: RelativePath,
    pub token: String,
    pub phase: InstallTempPhase,
}

#[derive(Clone, Copy, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum InstallTempPhase {
    Pending,
    Creating,
    Owned,
}

pub struct ObjectSink {
    _budget_lock: File,
    file: Option<File>,
    temp_path: Option<PathBuf>,
    final_path: PathBuf,
    expected_hash: ObjectHash,
    expected_size: u64,
    written: u64,
    hasher: blake3::Hasher,
}

impl ObjectSink {
    pub fn already_present(&self) -> bool {
        self.file.is_none()
    }

    pub fn write_chunk(&mut self, bytes: &[u8]) -> Result<()> {
        if self.written.saturating_add(bytes.len() as u64) > self.expected_size {
            bail!("object exceeds declared size");
        }
        #[cfg(feature = "e2e-test-hooks")]
        if self
            .temp_path
            .as_deref()
            .and_then(Path::parent)
            .and_then(Path::parent)
            .is_some_and(|state| state.join(".e2e-object-enospc").exists())
        {
            return Err(std::io::Error::from_raw_os_error(28).into());
        }
        if let Some(file) = &mut self.file {
            file.write_all(bytes)?;
        }
        self.hasher.update(bytes);
        self.written += bytes.len() as u64;
        Ok(())
    }

    pub fn finish(self) -> Result<()> {
        if self.written != self.expected_size {
            bail!("object size mismatch");
        }
        let actual = ObjectHash::from_blake3(self.hasher.finalize());
        if actual != self.expected_hash {
            bail!("received object hash mismatch");
        }
        let Some(file) = &self.file else {
            return Ok(());
        };
        file.sync_all()?;
        let temp_path = self.temp_path.as_ref().expect("writer has a temporary");
        match fs::symlink_metadata(&self.final_path) {
            Ok(_) if object_path_matches(&self.final_path, &self.expected_hash)? => {
                fs::remove_file(temp_path)?;
            }
            Ok(metadata) => {
                if metadata.is_dir() {
                    fs::remove_dir(&self.final_path)?;
                } else {
                    fs::remove_file(&self.final_path)?;
                }
                fs::rename(temp_path, &self.final_path)?;
                sync_dir(self.final_path.parent().expect("object parent"))?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::rename(temp_path, &self.final_path)?;
                sync_dir(self.final_path.parent().expect("object parent"))?;
            }
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }
}

impl Drop for ObjectSink {
    fn drop(&mut self) {
        if let Some(temp_path) = &self.temp_path {
            let _ = fs::remove_file(temp_path);
        }
    }
}

impl QueueRequest {
    pub fn ticket(&self) -> i64 {
        self.ticket
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    pub fn cancel(mut self) -> Result<()> {
        let mut state = State::open(&self.state_dir)?;
        state.cancel_sync_request(&self.token)?;
        self.owner.take();
        remove_scheduler_owner(&self.state_dir, &self.token)?;
        Ok(())
    }

    /// Leaves the durable request queued while releasing this process's ownership.
    pub fn release_for_reclaim(mut self) {
        self.owner.take();
    }

    pub fn try_activate(&mut self) -> Result<Option<InstallationPermit>> {
        self.try_activate_after(&mut |_| Ok(()), &mut || false)
    }

    fn try_activate_after<P, C>(
        &mut self,
        prepare: &mut P,
        canceled: &mut C,
    ) -> Result<Option<InstallationPermit>>
    where
        P: FnMut(&mut State) -> Result<()>,
        C: FnMut() -> bool,
    {
        if self.owner.is_none() {
            bail!("synchronization request is no longer owned");
        }
        let mut state = State::open(&self.state_dir)?;
        let row: Option<ActivationRow> = state
            .conn
            .query_row(
                "SELECT active,share_id,generation,paired_state
                 FROM sync_queue WHERE ticket=?1 AND token=?2",
                params![self.ticket, self.token],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        let Some((active, stored_share, stored_generation, paired_state)) = row else {
            return Err(QueueCancelled.into());
        };
        if active != 0 {
            bail!("synchronization request is already active");
        }
        if stored_share.as_deref() != self.share.as_ref().map(|share| share.0.as_str())
            || stored_generation != self.generation
        {
            return Err(QueueCancelled.into());
        }
        if let (Some(share), Some(generation)) = (&self.share, self.generation) {
            let current: Option<i64> = state
                .conn
                .query_row(
                    "SELECT intent_generation FROM shares WHERE share_id=?1",
                    [&share.0],
                    |row| row.get(0),
                )
                .optional()?;
            if current != Some(generation) {
                state.cancel_sync_request(&self.token)?;
                return Err(QueueCancelled.into());
            }
        }
        if !PairedQueueState::parse(paired_state.as_deref())?.is_none_or(PairedQueueState::eligible)
        {
            return Ok(None);
        }

        let Some(global) = state.try_lock_global_sync_final()? else {
            return Ok(None);
        };
        let head = eligible_head_ticket(&state.conn)?;
        let no_active = state.conn.query_row(
            "SELECT NOT EXISTS(SELECT 1 FROM sync_queue WHERE active=1)",
            [],
            |row| row.get::<_, i64>(0),
        )? != 0;
        if head != Some(self.ticket) || !no_active {
            return Ok(None);
        }
        if let Err(error) = prepare(&mut state) {
            if error.downcast_ref::<QueueRejoin>().is_none() {
                return Err(error);
            }
            let transaction = state
                .conn
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let next = transaction.query_row(
                "SELECT COALESCE(MAX(ticket),0)+1 FROM sync_queue",
                [],
                |row| row.get::<_, i64>(0),
            )?;
            if next <= 0 {
                bail!("synchronization queue ticket exhausted");
            }
            let changed = transaction.execute(
                "UPDATE sync_queue SET ticket=?3
                 WHERE ticket=?1 AND token=?2 AND active=0",
                params![self.ticket, self.token, next],
            )?;
            if changed != 1 {
                return Err(QueueCancelled.into());
            }
            transaction.commit()?;
            self.ticket = next;
            drop(global);
            return Ok(None);
        }
        if canceled() {
            state.cancel_sync_request(&self.token)?;
            return Err(QueueCancelled.into());
        }
        let transaction = state
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        if canceled() {
            drop(transaction);
            state.cancel_sync_request(&self.token)?;
            return Err(QueueCancelled.into());
        }
        let head = eligible_head_ticket(&transaction)?;
        let no_active = transaction.query_row(
            "SELECT NOT EXISTS(SELECT 1 FROM sync_queue WHERE active=1)",
            [],
            |row| row.get::<_, i64>(0),
        )? != 0;
        let generation_current = match (&self.share, self.generation) {
            (Some(share), Some(generation)) => {
                transaction
                    .query_row(
                        "SELECT intent_generation=?2 FROM shares WHERE share_id=?1",
                        params![share.0, generation],
                        |row| row.get::<_, i64>(0),
                    )
                    .optional()?
                    == Some(1)
            }
            _ => true,
        };
        let changed = if head == Some(self.ticket) && no_active && generation_current {
            transaction.execute(
                "UPDATE sync_queue SET active=1
                 WHERE ticket=?1 AND token=?2 AND active=0
                       AND (paired_state IS NULL OR paired_state='eligible')",
                params![self.ticket, self.token],
            )?
        } else {
            0
        };
        transaction.commit()?;
        if changed == 0 {
            drop(global);
            if !generation_current {
                state.cancel_sync_request(&self.token)?;
                return Err(QueueCancelled.into());
            }
            return Ok(None);
        }

        let request = QueueRequest {
            state_dir: self.state_dir.clone(),
            ticket: self.ticket,
            token: self.token.clone(),
            share: self.share.clone(),
            generation: self.generation,
            owner: self.owner.take(),
        };
        Ok(Some(InstallationPermit {
            request: Some(request),
            global: Some(global),
        }))
    }

    pub fn wait<C, R>(self, mut canceled: C, mut report_position: R) -> Result<InstallationPermit>
    where
        C: FnMut() -> bool,
        R: FnMut(QueuePosition) -> Result<()>,
    {
        self.wait_with_prepare(&mut canceled, &mut report_position, |_| Ok(()))
    }

    pub fn wait_with_prepare<C, R, P>(
        mut self,
        mut canceled: C,
        mut report_position: R,
        mut prepare: P,
    ) -> Result<InstallationPermit>
    where
        C: FnMut() -> bool,
        R: FnMut(QueuePosition) -> Result<()>,
        P: FnMut(&mut State) -> Result<()>,
    {
        let mut delay = Duration::from_millis(10);
        let mut previous = None;
        let mut last_report = std::time::Instant::now() - Duration::from_secs(10);
        loop {
            if canceled() {
                let mut state = State::open(&self.state_dir)?;
                state.cancel_sync_request(&self.token)?;
                return Err(QueueCancelled.into());
            }
            if let Some(permit) = self.try_activate_after(&mut prepare, &mut canceled)? {
                return Ok(permit);
            }
            let mut state = State::open(&self.state_dir)?;
            let snapshot = state.scheduling_snapshot()?;
            let position = snapshot
                .queued
                .iter()
                .find(|request| request.ticket == self.ticket)
                .and_then(|request| snapshot.queue_position(request))
                .unwrap_or(0);
            let report = QueuePosition {
                ticket: self.ticket,
                position,
                active: snapshot.active,
            };
            if previous.as_ref() != Some(&report)
                || last_report.elapsed() >= Duration::from_secs(10)
            {
                report_position(report.clone())?;
                previous = Some(report);
                last_report = std::time::Instant::now();
            }
            std::thread::sleep(delay);
            delay = std::cmp::min(delay.saturating_mul(2), Duration::from_millis(250));
        }
    }
}

impl Drop for QueueRequest {
    fn drop(&mut self) {
        if self.owner.is_none() {
            return;
        }
        if let Ok(mut state) = State::open(&self.state_dir) {
            let _ = state.cancel_sync_request(&self.token);
        }
        self.owner.take();
        let _ = remove_scheduler_owner(&self.state_dir, &self.token);
    }
}

impl InstallationPermit {
    pub fn finish(mut self) -> Result<()> {
        self.finish_inner(true)
    }

    fn finish_inner(&mut self, completed: bool) -> Result<()> {
        let Some(request) = self.request.as_ref() else {
            return Ok(());
        };
        let mut state = State::open(&request.state_dir)?;
        let transaction = state
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let changed = transaction.execute(
            "DELETE FROM sync_queue WHERE ticket=?1 AND token=?2 AND active=1",
            params![request.ticket, request.token],
        )?;
        if changed == 0 {
            bail!("active synchronization request is missing");
        }
        if completed {
            transaction.execute(
                "UPDATE installation
                 SET scheduler_completion_sequence=scheduler_completion_sequence+1
                 WHERE singleton=1",
                [],
            )?;
        }
        transaction.commit()?;
        let mut request = self.request.take().expect("permit request checked above");
        request.owner.take();
        fs::remove_file(request.state_dir.join("scheduler").join(&request.token))?;
        self.global.take();
        Ok(())
    }
}

impl Drop for InstallationPermit {
    fn drop(&mut self) {
        let _ = self.finish_inner(false);
    }
}

struct RawScheduledRequest {
    ticket: i64,
    token: String,
    share: Option<String>,
    relationship: Option<String>,
    operation: String,
    generation: Option<i64>,
    network_authority: Option<String>,
    network_order: Option<i64>,
    paired_state: Option<String>,
    proxy_acknowledged: i64,
    active: i64,
}

impl RawScheduledRequest {
    fn into_snapshot(self) -> Result<ScheduledRequestSnapshot> {
        Ok(ScheduledRequestSnapshot {
            ticket: self.ticket,
            token: self.token,
            share: self.share.map(ShareId),
            relationship: self.relationship.map(RelationshipId::parse).transpose()?,
            operation: SyncOperation::parse(&self.operation)?,
            generation: self.generation,
            network_authority: self.network_authority.map(PeerId),
            network_order: self.network_order,
            paired_state: PairedQueueState::parse(self.paired_state.as_deref())?,
            proxy_acknowledged: self.proxy_acknowledged != 0,
            active: self.active != 0,
        })
    }
}

fn raw_scheduled_request_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<RawScheduledRequest> {
    Ok(RawScheduledRequest {
        ticket: row.get(0)?,
        token: row.get(1)?,
        share: row.get(2)?,
        relationship: row.get(3)?,
        operation: row.get(4)?,
        generation: row.get(5)?,
        network_authority: row.get(6)?,
        network_order: row.get(7)?,
        paired_state: row.get(8)?,
        proxy_acknowledged: row.get(9)?,
        active: row.get(10)?,
    })
}

fn scheduled_requests(connection: &Connection) -> Result<Vec<ScheduledRequestSnapshot>> {
    let mut statement = connection.prepare(
        "SELECT ticket,token,share_id,relationship_id,operation,generation,
                network_authority,network_order,paired_state,proxy_acknowledged,active
         FROM sync_queue ORDER BY ticket",
    )?;
    statement
        .query_map([], raw_scheduled_request_from_row)?
        .map(|row| row?.into_snapshot())
        .collect()
}

fn validate_pair_value(name: &str, value: &str) -> Result<()> {
    if value.len() > 256
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        bail!("{name} must be at most 256 safe ASCII characters");
    }
    Ok(())
}

fn honor_relationship_yield(
    transaction: &rusqlite::Transaction<'_>,
    relationship: &RelationshipId,
    current_time_ns: i64,
) -> Result<()> {
    let row: Option<(i64, String)> = transaction
        .query_row(
            "SELECT after_completion_sequence,retry_not_before_ns
             FROM relationship_yields WHERE relationship_id=?1",
            [&relationship.0],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((after_sequence, retry_not_before)) = row else {
        return Ok(());
    };
    let completion_sequence: i64 = transaction.query_row(
        "SELECT COALESCE((SELECT scheduler_completion_sequence
                          FROM installation WHERE singleton=1),0)",
        [],
        |row| row.get(0),
    )?;
    let retry_not_before = retry_not_before
        .parse::<i64>()
        .context("stored relationship retry time is invalid")?;
    if completion_sequence <= after_sequence && current_time_ns < retry_not_before {
        bail!("relationship synchronization is temporarily yielded");
    }
    transaction.execute(
        "DELETE FROM relationship_yields WHERE relationship_id=?1",
        [&relationship.0],
    )?;
    Ok(())
}

fn exact_paired_ticket(
    transaction: &rusqlite::Transaction<'_>,
    token: &str,
    relationship: &RelationshipId,
    network_authority: &PeerId,
    network_order: i64,
    pair_nonce: &str,
    paired_state: &str,
) -> Result<i64> {
    transaction
        .query_row(
            "SELECT ticket FROM sync_queue
             WHERE token=?1 AND relationship_id=?2 AND network_authority=?3
                   AND network_order=?4 AND pair_nonce=?5 AND paired_state=?6 AND active=0",
            params![
                token,
                relationship.0,
                network_authority.0,
                network_order,
                pair_nonce,
                paired_state
            ],
            |row| row.get(0),
        )
        .optional()?
        .ok_or_else(|| QueueCancelled.into())
}

fn eligible_head_ticket(connection: &Connection) -> Result<Option<i64>> {
    let rows = scheduled_requests(connection)?;
    Ok(eligible_candidates(&rows)
        .first()
        .map(|request| request.ticket))
}

fn eligible_head_token(connection: &Connection) -> Result<Option<String>> {
    let rows = scheduled_requests(connection)?;
    Ok(eligible_candidates(&rows)
        .first()
        .map(|request| request.token.clone()))
}

fn has_active_or_eligible_predecessor(
    transaction: &rusqlite::Transaction<'_>,
    ticket: i64,
    network_authority: &PeerId,
    network_order: i64,
) -> Result<bool> {
    let rows = scheduled_requests(transaction)?;
    let target = ScheduledRequestSnapshot {
        ticket,
        token: String::new(),
        share: None,
        relationship: None,
        operation: SyncOperation::Recovery,
        generation: None,
        network_authority: Some(network_authority.clone()),
        network_order: Some(network_order),
        paired_state: Some(PairedQueueState::Parked),
        proxy_acknowledged: false,
        active: false,
    };
    Ok(rows.iter().any(|request| request.active)
        || !eligible_predecessors(&rows, &target).is_empty())
}

fn paired_predecessor_fingerprint(
    transaction: &rusqlite::Transaction<'_>,
    ticket: i64,
    network_authority: &PeerId,
    network_order: i64,
) -> Result<String> {
    let requests = scheduled_requests(transaction)?;
    let target = ScheduledRequestSnapshot {
        ticket,
        token: String::new(),
        share: None,
        relationship: None,
        operation: SyncOperation::Recovery,
        generation: None,
        network_authority: Some(network_authority.clone()),
        network_order: Some(network_order),
        paired_state: Some(PairedQueueState::Parked),
        proxy_acknowledged: false,
        active: false,
    };
    let mut rows = requests
        .iter()
        .filter(|request| request.active)
        .chain(eligible_predecessors(&requests, &target))
        .map(|request| {
            (
                request.ticket,
                request.token.clone(),
                request.paired_state.map(PairedQueueState::as_str),
                request
                    .network_authority
                    .as_ref()
                    .map(|peer| peer.0.clone()),
                request.network_order,
                request.active,
            )
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| row.0);
    rows.dedup_by_key(|row| row.0);
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"flocal-paired-predecessors-v1\0");
    hasher.update(&serde_json::to_vec(&rows)?);
    Ok(hasher.finalize().to_hex().to_string())
}

const INSTALLATION_BARRIER_FILE: &str = "installation.barrier";
const INSTALLER_LOCK_FILE: &str = "installer.lock";
const UPGRADE_PENDING_FILE: &str = "upgrade.pending";

fn open_installation_barrier(dir: &Path) -> Result<File> {
    let path = dir.join(INSTALLATION_BARRIER_FILE);
    open_private_regular_file(&path, false).context("opening the private installation barrier")
}

fn ensure_upgrade_not_pending(dir: &Path) -> Result<()> {
    if upgrade_pending(dir)? {
        return Err(UpgradePending.into());
    }
    Ok(())
}

fn upgrade_pending(dir: &Path) -> Result<bool> {
    let path = dir.join(UPGRADE_PENDING_FILE);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    #[cfg(unix)]
    let private =
        metadata.uid() == rustix::process::geteuid().as_raw() && metadata.mode() & 0o077 == 0;
    #[cfg(not(unix))]
    let private = true;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() != 0 || !private {
        bail!("upgrade marker is not a private empty regular file");
    }
    Ok(true)
}

#[derive(Eq, PartialEq)]
struct LegacyUpgradeInventory {
    shares: Vec<(String, PathBuf)>,
    active_root: Option<PathBuf>,
}

impl LegacyUpgradeInventory {
    fn contention_path(&self, state_dir: &Path) -> PathBuf {
        self.active_root
            .clone()
            .or_else(|| (self.shares.len() == 1).then(|| self.shares[0].1.clone()))
            .unwrap_or_else(|| state_dir.to_path_buf())
    }
}

fn legacy_upgrade_inventory(dir: &Path) -> Result<LegacyUpgradeInventory> {
    let database = dir.join("state.sqlite3");
    match fs::symlink_metadata(&database) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(LegacyUpgradeInventory {
                shares: Vec::new(),
                active_root: None,
            });
        }
        Err(error) => return Err(error.into()),
        Ok(metadata) if !metadata.is_file() || metadata.file_type().is_symlink() => {
            bail!("state database is not a regular file")
        }
        Ok(_) => {}
    }
    let connection = Connection::open_with_flags(
        database,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let has_shares = connection
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='shares'",
            [],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    if !has_shares {
        return Ok(LegacyUpgradeInventory {
            shares: Vec::new(),
            active_root: None,
        });
    }
    let mut statement = connection.prepare("SELECT share_id,root FROM shares ORDER BY share_id")?;
    let shares = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, bytes_path(row.get(1)?)))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let active_root = connection
        .query_row(
            "SELECT shares.root FROM sync_queue
             JOIN shares ON shares.share_id=sync_queue.share_id
             WHERE sync_queue.active=1 LIMIT 1",
            [],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()?
        .map(bytes_path);
    Ok(LegacyUpgradeInventory {
        shares,
        active_root,
    })
}

fn try_upgrade_lock(path: &Path, locks: &mut Vec<File>) -> Result<bool> {
    let file = open_private_regular_file(path, false)
        .with_context(|| format!("opening private upgrade lock {}", path.display()))?;
    match FileExt::try_lock_exclusive(&file) {
        Ok(()) => {
            locks.push(file);
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(false),
        Err(error) => Err(error.into()),
    }
}

impl State {
    pub fn open_default() -> Result<Self> {
        Self::open(Self::default_dir()?)
    }

    pub fn default_dir() -> Result<PathBuf> {
        if let Some(path) = std::env::var_os("FLOCAL_STATE_DIR") {
            return Ok(path.into());
        }
        if let Some(path) = Self::managed_state_dir()? {
            return Ok(path);
        }
        let dirs = ProjectDirs::from("local", "file.local", "file.local")
            .context("could not determine user state directory")?;
        #[cfg(target_os = "linux")]
        let path = dirs
            .state_dir()
            .context("could not determine user state directory")?;
        #[cfg(not(target_os = "linux"))]
        let path = dirs.data_local_dir();
        Ok(path.to_path_buf())
    }

    pub fn managed_state_dir() -> Result<Option<PathBuf>> {
        let home = match std::env::var_os("HOME") {
            Some(home) => PathBuf::from(home),
            None => return Ok(None),
        };
        let marker = home.join(".config/file.local/managed-state");
        let metadata = match fs::symlink_metadata(&marker) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            bail!("managed daemon state marker is not a regular file");
        }
        #[cfg(unix)]
        {
            let uid = rustix::process::geteuid().as_raw();
            if metadata.uid() != uid || metadata.mode() & 0o077 != 0 {
                bail!("managed daemon state marker is not private");
            }
        }
        let path = PathBuf::from(fs::read_to_string(&marker)?.trim_end());
        if !path.is_absolute() {
            bail!("managed daemon state marker is not an absolute path");
        }
        Ok(Some(path))
    }

    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        ensure_private_directory(&dir)?;
        let barrier = open_installation_barrier(&dir)?;
        ensure_upgrade_not_pending(&dir)?;
        FileExt::lock_shared(&barrier)?;
        if let Err(error) = ensure_upgrade_not_pending(&dir) {
            let _ = FileExt::unlock(&barrier);
            return Err(error);
        }
        Self::open_with_barrier(dir, barrier)
    }

    pub fn try_acquire_upgrade_barrier(dir: impl AsRef<Path>) -> Result<Option<UpgradeBarrier>> {
        let dir = dir.as_ref();
        ensure_private_directory(dir)?;
        let barrier = open_installation_barrier(dir)?;
        match FileExt::try_lock_exclusive(&barrier) {
            Ok(()) => Ok(Some(UpgradeBarrier(barrier))),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub fn try_acquire_legacy_upgrade_locks(dir: impl AsRef<Path>) -> Result<UpgradeLockAttempt> {
        let dir = dir.as_ref();
        let shares = legacy_upgrade_inventory(dir)?;
        let mut locks = Vec::with_capacity(5 + shares.shares.len() * 2);
        for name in [
            "daemon.lock",
            "registration.lock",
            "objects.lock",
            "sync.lock",
            "scheduler.publish.lock",
        ] {
            if !try_upgrade_lock(&dir.join(name), &mut locks)? {
                return Ok(UpgradeLockAttempt::Busy(shares.contention_path(dir)));
            }
        }
        for (share, root) in &shares.shares {
            let name = blake3::hash(share.as_bytes()).to_hex().to_string();
            for directory in ["locks", "sessions"] {
                let parent = dir.join(directory);
                ensure_private_directory(&parent)?;
                if !try_upgrade_lock(&parent.join(&name), &mut locks)? {
                    return Ok(UpgradeLockAttempt::Busy(root.clone()));
                }
            }
        }
        if legacy_upgrade_inventory(dir)? != shares {
            return Ok(UpgradeLockAttempt::Busy(shares.contention_path(dir)));
        }
        Ok(UpgradeLockAttempt::Acquired(LegacyUpgradeLocks {
            _files: locks,
        }))
    }

    pub fn open_for_upgrade(dir: impl AsRef<Path>, barrier: &UpgradeBarrier) -> Result<Self> {
        Self::open_with_barrier(dir.as_ref().to_path_buf(), barrier.0.try_clone()?)
    }

    fn open_with_barrier(dir: PathBuf, barrier: File) -> Result<Self> {
        ensure_private_directory(&dir)?;
        ensure_private_directory(&dir.join("objects"))?;
        ensure_private_directory(&dir.join("scheduler"))?;
        set_private_dir(&dir)?;
        set_private_dir(&dir.join("objects"))?;
        set_private_dir(&dir.join("scheduler"))?;
        let database = dir.join("state.sqlite3");
        if let Ok(metadata) = fs::symlink_metadata(&database)
            && (!metadata.file_type().is_file() || metadata.file_type().is_symlink())
        {
            bail!("state database is not a regular file");
        }
        let mut conn = Connection::open(&database)?;
        set_private_file(&database)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        let transaction = conn.transaction()?;
        transaction.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS installation (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                peer_id TEXT NOT NULL,
                auth_key BLOB,
                scheduler_completion_sequence INTEGER NOT NULL DEFAULT 0,
                scheduler_cleanup_cursor INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS shares (
                share_id TEXT PRIMARY KEY,
                root BLOB NOT NULL UNIQUE,
                sequence INTEGER NOT NULL DEFAULT 0,
                initial_complete INTEGER NOT NULL DEFAULT 0,
                peer_json TEXT,
                bound_peer TEXT,
                bound_relationship TEXT,
                removing_relationship TEXT,
                root_device TEXT,
                root_inode TEXT,
                watch_enabled INTEGER NOT NULL DEFAULT 0,
                blocked_diagnostic TEXT,
                intent_generation INTEGER NOT NULL DEFAULT 0,
                recovery_budget_bytes INTEGER NOT NULL DEFAULT 10737418240
            );
            CREATE TABLE IF NOT EXISTS records (
                share_id TEXT NOT NULL,
                path BLOB NOT NULL,
                version_json TEXT NOT NULL,
                PRIMARY KEY (share_id, path)
            );
            CREATE TABLE IF NOT EXISTS conflicts (
                id TEXT PRIMARY KEY,
                share_id TEXT NOT NULL,
                path BLOB NOT NULL,
                winner_json TEXT NOT NULL,
                loser_json TEXT NOT NULL,
                created_ns TEXT NOT NULL,
                conflict_json TEXT
            );
            CREATE TABLE IF NOT EXISTS install_intents (
                share_id TEXT PRIMARY KEY,
                records_json TEXT NOT NULL,
                failure_fingerprint TEXT CHECK (
                    failure_fingerprint IS NULL OR
                    (LENGTH(failure_fingerprint)=64 AND failure_fingerprint NOT GLOB '*[^0-9a-f]*')
                ),
                failure_diagnostic TEXT CHECK (
                    failure_diagnostic IS NULL OR LENGTH(CAST(failure_diagnostic AS BLOB))<=4096
                ),
                CHECK (
                    (failure_fingerprint IS NULL AND failure_diagnostic IS NULL) OR
                    (failure_fingerprint IS NOT NULL AND failure_diagnostic IS NOT NULL)
                )
            );
            CREATE TABLE IF NOT EXISTS unsettled_paths (
                share_id TEXT NOT NULL,
                path BLOB NOT NULL,
                PRIMARY KEY (share_id, path)
            );
            CREATE TABLE IF NOT EXISTS shared_heads (
                share_id TEXT NOT NULL,
                path BLOB NOT NULL,
                base_json TEXT NOT NULL,
                PRIMARY KEY (share_id, path)
            );
            CREATE TABLE IF NOT EXISTS pending_objects (
                share_id TEXT NOT NULL,
                hash TEXT NOT NULL,
                provenance TEXT NOT NULL CHECK (provenance IN ('receiving','verified_from_bound_peer','generated_local')),
                PRIMARY KEY (share_id, hash)
            );
            CREATE TABLE IF NOT EXISTS sync_queue (
                ticket INTEGER PRIMARY KEY,
                token TEXT NOT NULL UNIQUE,
                share_id TEXT,
                relationship_id TEXT,
                operation TEXT NOT NULL CHECK (
                    operation IN ('recovery','initial','sync','watch','registration','removal','maintenance')
                ),
                generation INTEGER,
                dedupe_key TEXT UNIQUE,
                network_authority TEXT,
                network_order INTEGER,
                paired_state TEXT CHECK (
                    paired_state IS NULL OR paired_state IN ('pending_authority','parked','prepared','eligible')
                ),
                pair_nonce TEXT,
                predecessor_fingerprint TEXT,
                proxy_acknowledged INTEGER NOT NULL DEFAULT 0 CHECK (proxy_acknowledged IN (0,1)),
                active INTEGER NOT NULL DEFAULT 0 CHECK (active IN (0,1))
            );
            CREATE UNIQUE INDEX IF NOT EXISTS sync_queue_managed_generation
            ON sync_queue(share_id,generation)
            WHERE operation='watch' AND generation IS NOT NULL;
            CREATE UNIQUE INDEX IF NOT EXISTS sync_queue_one_active
            ON sync_queue(active) WHERE active=1;
            CREATE TABLE IF NOT EXISTS relationship_yields (
                relationship_id TEXT PRIMARY KEY,
                after_completion_sequence INTEGER,
                retry_not_before_ns TEXT
            );
            ",
        )?;
        let columns: Vec<String> = {
            let mut stmt = transaction.prepare("PRAGMA table_info(shares)")?;
            stmt.query_map([], |row| row.get(1))?
                .collect::<Result<_, _>>()?
        };
        let conflict_columns: Vec<String> = {
            let mut stmt = transaction.prepare("PRAGMA table_info(conflicts)")?;
            stmt.query_map([], |row| row.get(1))?
                .collect::<Result<_, _>>()?
        };
        let installation_columns: Vec<String> = {
            let mut stmt = transaction.prepare("PRAGMA table_info(installation)")?;
            stmt.query_map([], |row| row.get(1))?
                .collect::<Result<_, _>>()?
        };
        let sync_queue_columns: Vec<String> = {
            let mut stmt = transaction.prepare("PRAGMA table_info(sync_queue)")?;
            stmt.query_map([], |row| row.get(1))?
                .collect::<Result<_, _>>()?
        };
        let install_intent_columns: Vec<String> = {
            let mut stmt = transaction.prepare("PRAGMA table_info(install_intents)")?;
            stmt.query_map([], |row| row.get(1))?
                .collect::<Result<_, _>>()?
        };
        let network_order_index_is_current = transaction
            .query_row(
                "SELECT sql FROM sqlite_master
                 WHERE type='index' AND name='sync_queue_network_order'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .is_some_and(|sql| {
                sql.contains("WHERE network_authority IS NOT NULL AND network_order IS NOT NULL")
            });
        if !conflict_columns.iter().any(|name| name == "conflict_json") {
            transaction.execute("ALTER TABLE conflicts ADD COLUMN conflict_json TEXT", [])?;
        }
        if !installation_columns.iter().any(|name| name == "auth_key") {
            transaction.execute("ALTER TABLE installation ADD COLUMN auth_key BLOB", [])?;
        }
        if !installation_columns
            .iter()
            .any(|name| name == "scheduler_completion_sequence")
        {
            transaction.execute(
                "ALTER TABLE installation ADD COLUMN scheduler_completion_sequence INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        if !installation_columns
            .iter()
            .any(|name| name == "scheduler_cleanup_cursor")
        {
            transaction.execute(
                "ALTER TABLE installation ADD COLUMN scheduler_cleanup_cursor INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        if !sync_queue_columns
            .iter()
            .any(|name| name == "proxy_acknowledged")
        {
            transaction.execute(
                "ALTER TABLE sync_queue ADD COLUMN proxy_acknowledged INTEGER NOT NULL DEFAULT 0 CHECK (proxy_acknowledged IN (0,1))",
                [],
            )?;
        }
        if !sync_queue_columns
            .iter()
            .any(|name| name == "network_authority")
        {
            transaction.execute(
                "ALTER TABLE sync_queue ADD COLUMN network_authority TEXT",
                [],
            )?;
        }
        if !network_order_index_is_current {
            transaction.execute("DROP INDEX IF EXISTS sync_queue_network_order", [])?;
            transaction.execute_batch(
                "CREATE UNIQUE INDEX sync_queue_network_order
                 ON sync_queue(network_authority,network_order)
                 WHERE network_authority IS NOT NULL AND network_order IS NOT NULL;",
            )?;
        }
        if !install_intent_columns
            .iter()
            .any(|name| name == "failure_fingerprint")
        {
            transaction.execute(
                "ALTER TABLE install_intents ADD COLUMN failure_fingerprint TEXT",
                [],
            )?;
        }
        if !install_intent_columns
            .iter()
            .any(|name| name == "failure_diagnostic")
        {
            transaction.execute(
                "ALTER TABLE install_intents ADD COLUMN failure_diagnostic TEXT",
                [],
            )?;
        }
        transaction.execute_batch(
            "CREATE TRIGGER IF NOT EXISTS install_intents_failure_insert_guard
             BEFORE INSERT ON install_intents
             WHEN
                 (NEW.failure_fingerprint IS NULL AND NEW.failure_diagnostic IS NOT NULL) OR
                 (NEW.failure_fingerprint IS NOT NULL AND NEW.failure_diagnostic IS NULL) OR
                 (NEW.failure_fingerprint IS NOT NULL AND (
                     LENGTH(NEW.failure_fingerprint)<>64 OR
                     NEW.failure_fingerprint GLOB '*[^0-9a-f]*'
                 )) OR
                 (NEW.failure_diagnostic IS NOT NULL AND
                  LENGTH(CAST(NEW.failure_diagnostic AS BLOB))>4096)
             BEGIN
                 SELECT RAISE(ABORT, 'invalid install recovery classification');
             END;
             CREATE TRIGGER IF NOT EXISTS install_intents_failure_update_guard
             BEFORE UPDATE OF failure_fingerprint,failure_diagnostic ON install_intents
             WHEN
                 (NEW.failure_fingerprint IS NULL AND NEW.failure_diagnostic IS NOT NULL) OR
                 (NEW.failure_fingerprint IS NOT NULL AND NEW.failure_diagnostic IS NULL) OR
                 (NEW.failure_fingerprint IS NOT NULL AND (
                     LENGTH(NEW.failure_fingerprint)<>64 OR
                     NEW.failure_fingerprint GLOB '*[^0-9a-f]*'
                 )) OR
                 (NEW.failure_diagnostic IS NOT NULL AND
                  LENGTH(CAST(NEW.failure_diagnostic AS BLOB))>4096)
             BEGIN
                 SELECT RAISE(ABORT, 'invalid install recovery classification');
             END;",
        )?;
        if !columns.iter().any(|name| name == "bound_peer") {
            transaction.execute("ALTER TABLE shares ADD COLUMN bound_peer TEXT", [])?;
        }
        if !columns.iter().any(|name| name == "bound_relationship") {
            transaction.execute("ALTER TABLE shares ADD COLUMN bound_relationship TEXT", [])?;
        }
        if !columns.iter().any(|name| name == "removing_relationship") {
            transaction.execute(
                "ALTER TABLE shares ADD COLUMN removing_relationship TEXT",
                [],
            )?;
        }
        if !columns.iter().any(|name| name == "root_device") {
            transaction.execute("ALTER TABLE shares ADD COLUMN root_device TEXT", [])?;
        }
        if !columns.iter().any(|name| name == "root_inode") {
            transaction.execute("ALTER TABLE shares ADD COLUMN root_inode TEXT", [])?;
        }
        if !columns.iter().any(|name| name == "watch_enabled") {
            transaction.execute(
                "ALTER TABLE shares ADD COLUMN watch_enabled INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        if !columns.iter().any(|name| name == "blocked_diagnostic") {
            transaction.execute("ALTER TABLE shares ADD COLUMN blocked_diagnostic TEXT", [])?;
        }
        if !columns.iter().any(|name| name == "intent_generation") {
            transaction.execute(
                "ALTER TABLE shares ADD COLUMN intent_generation INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        if !columns.iter().any(|name| name == "recovery_budget_bytes") {
            transaction.execute(
                "ALTER TABLE shares ADD COLUMN recovery_budget_bytes INTEGER NOT NULL DEFAULT 10737418240",
                [],
            )?;
        }
        #[cfg(feature = "e2e-test-hooks")]
        if dir.join(".e2e-fail-state-migration").exists() {
            bail!("injected state migration failure");
        }
        let legacy: Vec<(String, Vec<u8>)> = {
            let mut statement = transaction.prepare(
                "SELECT share_id, root FROM shares
                 WHERE root_device IS NULL OR root_inode IS NULL",
            )?;
            statement
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect::<Result<_, _>>()?
        };
        let mut backfill = Vec::with_capacity(legacy.len());
        for (share, root) in legacy {
            let root = bytes_path(root);
            let identity = root_identity(&root).with_context(|| {
                format!(
                    "cannot bind legacy share {share} to {}; restore and verify the original root before retrying the upgrade",
                    root.display()
                )
            })?;
            backfill.push((share, root, identity));
        }
        for (share, root, identity) in backfill {
            let verified = root_identity(&root).with_context(|| {
                format!(
                    "legacy share {share} root changed while upgrading; restore and verify {} before retrying",
                    root.display()
                )
            })?;
            if verified != identity {
                bail!(
                    "legacy share {share} root changed while upgrading; refusing to bind a stale filesystem identity"
                );
            }
            transaction.execute(
                "UPDATE shares SET root_device=?2, root_inode=?3 WHERE share_id=?1",
                params![
                    share,
                    identity.device.to_string(),
                    identity.inode.to_string()
                ],
            )?;
        }
        transaction.commit()?;
        Ok(Self {
            dir,
            conn,
            _installation_barrier: barrier,
        })
    }

    pub fn upgrade_pending(&self) -> Result<bool> {
        upgrade_pending(&self.dir)
    }

    pub fn acquire_installer_lock(dir: impl AsRef<Path>) -> Result<File> {
        let path = dir.as_ref().join(INSTALLER_LOCK_FILE);
        let file = open_private_regular_file(&path, false)
            .context("opening the private installer lock")?;
        file.try_lock_exclusive()
            .context("another flocal installation is already in progress")?;
        Ok(file)
    }

    pub fn create_upgrade_pending(dir: impl AsRef<Path>) -> Result<()> {
        let dir = dir.as_ref();
        ensure_private_directory(dir)?;
        let path = dir.join(UPGRADE_PENDING_FILE);
        let file = match open_private_regular_file(&path, true) {
            Ok(file) => file,
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.kind() == std::io::ErrorKind::AlreadyExists) =>
            {
                upgrade_pending(dir)?;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        file.sync_all()?;
        sync_dir(dir)
    }

    pub fn remove_upgrade_pending(dir: impl AsRef<Path>) -> Result<()> {
        let dir = dir.as_ref();
        let path = dir.join(UPGRADE_PENDING_FILE);
        if upgrade_pending(dir)? {
            fs::remove_file(path)?;
            sync_dir(dir)?;
        }
        Ok(())
    }

    pub fn ensure_private_state_child(&self, name: &str) -> Result<PathBuf> {
        let path = self.dir.join(name);
        ensure_private_directory(&path)?;
        Ok(path)
    }

    pub fn peer_id(&self) -> Result<PeerId> {
        if let Some(value) = self
            .conn
            .query_row(
                "SELECT peer_id FROM installation WHERE singleton=1",
                [],
                |r| r.get(0),
            )
            .optional()?
        {
            return Ok(PeerId(value));
        }
        let id = PeerId::generate();
        self.conn.execute(
            "INSERT INTO installation(singleton, peer_id) VALUES(1, ?1)",
            [&id.0],
        )?;
        Ok(id)
    }

    fn authentication_key(&self) -> Result<[u8; 32]> {
        if let Some(bytes) = self
            .conn
            .query_row(
                "SELECT auth_key FROM installation WHERE singleton=1",
                [],
                |row| row.get::<_, Option<Vec<u8>>>(0),
            )
            .optional()?
            .flatten()
        {
            return bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("installation authentication key is invalid"));
        }
        let key: [u8; 32] = rand::random();
        self.conn.execute(
            "UPDATE installation SET auth_key=?1 WHERE singleton=1",
            [key.as_slice()],
        )?;
        Ok(key)
    }

    pub fn authenticate_record(&self, share: &ShareId, record: &mut Record) -> Result<()> {
        let key = self.authentication_key()?;
        let version = &mut record.version;
        version.id_authenticator = Some(authenticate(
            &key,
            &serde_json::to_vec(&(
                "flocal-version-id-v2",
                &share.0,
                &version.peer.0,
                version.sequence,
            ))?,
        ));
        version.base_authenticator = Some(authenticate(
            &key,
            &serde_json::to_vec(&(
                "flocal-base-version-v2",
                &share.0,
                version.id(),
                &version.entry,
            ))?,
        ));
        version.version_authenticator = Some(version_tag(&key, share, &record.path, version)?);
        Ok(())
    }

    pub fn validate_remote_records<'a>(
        &self,
        share: &ShareId,
        local_records: &'a [Record],
        remote_records: &'a [Record],
    ) -> Result<()> {
        let local = self.peer_id()?;
        let key = self.authentication_key()?;
        let mut canonical = std::collections::HashMap::new();
        let mut remote_paths = std::collections::HashSet::new();
        for record in local_records {
            insert_canonical_record(&mut canonical, record)?;
        }
        for record in remote_records {
            if !remote_paths.insert(record.path.as_bytes()) {
                bail!("peer snapshot contains duplicate paths");
            }
            let identity = (&record.version.peer, record.version.sequence);
            let untagged_legacy = record.version.id_authenticator.is_none()
                && record.version.version_authenticator.is_none()
                && record.version.base_authenticator.is_none();
            if untagged_legacy
                && canonical
                    .get(&identity)
                    .is_some_and(|local_record| *local_record == record)
            {
                continue;
            }
            validate_owned_id(&key, share, &local, &record.version.id())?;
            for seen in &record.version.seen {
                validate_owned_id(&key, share, &local, seen)?;
            }
            if let Some(base) = &record.version.merge_base {
                validate_owned_id(&key, share, &local, &base.id)?;
                if base.id.peer == local {
                    let expected = authenticate(
                        &key,
                        &serde_json::to_vec(&(
                            "flocal-base-version-v2",
                            &share.0,
                            &base.id,
                            &base.entry,
                        ))?,
                    );
                    if base.authenticator.as_deref() != Some(&expected) {
                        bail!("peer supplied an invalid locally owned merge base");
                    }
                }
            }
            if record.version.peer == local {
                let expected = version_tag(&key, share, &record.path, &record.version)?;
                if record.version.version_authenticator.as_deref() != Some(&expected) {
                    bail!("peer supplied invalid metadata for a locally owned version");
                }
            }
            insert_canonical_record(&mut canonical, record)?;
        }
        Ok(())
    }

    pub fn install_intent(&self, share: &ShareId) -> Result<Option<InstallIntent>> {
        let json: Option<String> = self
            .conn
            .query_row(
                "SELECT records_json FROM install_intents WHERE share_id=?1",
                [&share.0],
                |row| row.get(0),
            )
            .optional()?;
        json.map(|value| Ok(serde_json::from_str(&value)?))
            .transpose()
    }

    pub fn install_intent_fingerprint(intent: &InstallIntent) -> Result<String> {
        Ok(blake3::hash(&serde_json::to_vec(intent)?)
            .to_hex()
            .to_string())
    }

    pub fn install_recovery_intent(
        &self,
        share: &ShareId,
    ) -> Result<Option<InstallRecoveryIntent>> {
        let row: Option<(String, Option<String>, Option<String>)> = self
            .conn
            .query_row(
                "SELECT records_json,failure_fingerprint,failure_diagnostic
                 FROM install_intents WHERE share_id=?1",
                [&share.0],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((json, failure_fingerprint, failure_diagnostic)) = row else {
            return Ok(None);
        };
        let intent: InstallIntent = serde_json::from_str(&json)?;
        decode_install_intent_failure(&intent, failure_fingerprint, failure_diagnostic)?;
        Ok(Some(InstallRecoveryIntent {
            share: share.clone(),
            fingerprint: Self::install_intent_fingerprint(&intent)?,
            intent,
        }))
    }

    pub fn install_intent_failure(&self, share: &ShareId) -> Result<Option<InstallIntentFailure>> {
        let row: Option<(String, Option<String>, Option<String>)> = self
            .conn
            .query_row(
                "SELECT records_json,failure_fingerprint,failure_diagnostic
                 FROM install_intents WHERE share_id=?1",
                [&share.0],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((json, fingerprint, diagnostic)) = row else {
            return Ok(None);
        };
        let intent: InstallIntent = serde_json::from_str(&json)?;
        decode_install_intent_failure(&intent, fingerprint, diagnostic)
    }

    pub fn unclassified_install_intents(&self) -> Result<Vec<InstallRecoveryIntent>> {
        let mut statement = self.conn.prepare(
            "SELECT share_id,records_json,failure_fingerprint,failure_diagnostic
             FROM install_intents ORDER BY share_id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })?;
        let mut intents = Vec::new();
        for row in rows {
            let (share, json, failure_fingerprint, failure_diagnostic) = row?;
            let intent: InstallIntent = serde_json::from_str(&json)?;
            if decode_install_intent_failure(&intent, failure_fingerprint, failure_diagnostic)?
                .is_none()
            {
                intents.push(InstallRecoveryIntent {
                    share: ShareId(share),
                    fingerprint: Self::install_intent_fingerprint(&intent)?,
                    intent,
                });
            }
        }
        Ok(intents)
    }

    pub fn classify_install_intent_failure(
        &mut self,
        share: &ShareId,
        expected_fingerprint: &str,
        diagnostic: &str,
    ) -> Result<bool> {
        validate_install_intent_fingerprint(expected_fingerprint)?;
        let diagnostic = bounded_diagnostic(diagnostic);
        let transaction = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let row: Option<(String, Option<String>, Option<String>)> = transaction
            .query_row(
                "SELECT records_json,failure_fingerprint,failure_diagnostic
                 FROM install_intents WHERE share_id=?1",
                [&share.0],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((json, prior_fingerprint, prior_diagnostic)) = row else {
            transaction.commit()?;
            return Ok(false);
        };
        let intent: InstallIntent = serde_json::from_str(&json)?;
        decode_install_intent_failure(&intent, prior_fingerprint, prior_diagnostic)?;
        let actual_fingerprint = Self::install_intent_fingerprint(&intent)?;
        if actual_fingerprint != expected_fingerprint {
            transaction.commit()?;
            return Ok(false);
        }
        let changed = transaction.execute(
            "UPDATE install_intents
             SET failure_fingerprint=?2,failure_diagnostic=?3
             WHERE share_id=?1 AND records_json=?4",
            params![share.0, actual_fingerprint, diagnostic, json],
        )?;
        if changed != 1 {
            bail!("install intent changed while classifying recovery failure");
        }
        let share_changed = transaction.execute(
            "UPDATE shares SET blocked_diagnostic=?2 WHERE share_id=?1",
            params![share.0, diagnostic],
        )?;
        if share_changed != 1 {
            bail!("share disappeared while classifying install recovery failure");
        }
        transaction.execute(
            "DELETE FROM sync_queue WHERE share_id=?1 AND active=0",
            [&share.0],
        )?;
        transaction.commit()?;
        Ok(true)
    }

    pub fn begin_install_intent_retry(
        &mut self,
        share: &ShareId,
    ) -> Result<Option<InstallRecoveryIntent>> {
        let transaction = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let row: Option<(String, Option<String>, Option<String>)> = transaction
            .query_row(
                "SELECT records_json,failure_fingerprint,failure_diagnostic
                 FROM install_intents WHERE share_id=?1",
                [&share.0],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((json, failure_fingerprint, failure_diagnostic)) = row else {
            transaction.commit()?;
            return Ok(None);
        };
        let intent: InstallIntent = serde_json::from_str(&json)?;
        let failure =
            decode_install_intent_failure(&intent, failure_fingerprint, failure_diagnostic)?;
        let fingerprint = Self::install_intent_fingerprint(&intent)?;
        if let Some(failure) = failure {
            let changed = transaction.execute(
                "UPDATE install_intents
                 SET failure_fingerprint=NULL,failure_diagnostic=NULL
                 WHERE share_id=?1 AND records_json=?2 AND failure_fingerprint=?3",
                params![share.0, json, failure.fingerprint],
            )?;
            if changed != 1 {
                bail!("install intent changed while beginning recovery retry");
            }
            transaction.execute(
                "UPDATE shares SET blocked_diagnostic=NULL
                 WHERE share_id=?1 AND blocked_diagnostic=?2",
                params![share.0, failure.diagnostic],
            )?;
        }
        transaction.commit()?;
        Ok(Some(InstallRecoveryIntent {
            share: share.clone(),
            intent,
            fingerprint,
        }))
    }

    pub fn set_install_intent(
        &self,
        share: &ShareId,
        records: &[Record],
    ) -> Result<(InstallIntent, bool)> {
        self.set_plan_install_intent(share, records, &[])
    }

    pub fn set_plan_install_intent(
        &self,
        share: &ShareId,
        records: &[Record],
        conflicts: &[Conflict],
    ) -> Result<(InstallIntent, bool)> {
        self.set_plan_install_intent_inner(share, records, conflicts, None)
    }

    pub fn set_managed_plan_install_intent(
        &self,
        share: &ShareId,
        records: &[Record],
        conflicts: &[Conflict],
        expected_generation: i64,
    ) -> Result<(InstallIntent, bool)> {
        self.set_plan_install_intent_inner(share, records, conflicts, Some(expected_generation))
    }

    fn set_plan_install_intent_inner(
        &self,
        share: &ShareId,
        records: &[Record],
        conflicts: &[Conflict],
        managed_generation: Option<i64>,
    ) -> Result<(InstallIntent, bool)> {
        let transaction = rusqlite::Transaction::new_unchecked(
            &self.conn,
            rusqlite::TransactionBehavior::Immediate,
        )?;
        let existing: Option<String> = transaction
            .query_row(
                "SELECT records_json FROM install_intents WHERE share_id=?1",
                [&share.0],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(json) = existing {
            let intent: InstallIntent = serde_json::from_str(&json)?;
            if intent.records != records
                || intent.conflicts != conflicts
                || managed_generation
                    .is_some_and(|generation| intent.managed_generation != Some(generation))
            {
                bail!("a different install is already pending for this share");
            }
            transaction.commit()?;
            return Ok((intent, false));
        }
        let intent = InstallIntent {
            records: records.to_vec(),
            conflicts: conflicts.to_vec(),
            temps: records
                .iter()
                .map(|record| InstallTemp {
                    path: record.path.clone(),
                    token: format!(".flocal-tmp-{}", ShareId::generate().0),
                    phase: InstallTempPhase::Pending,
                })
                .collect(),
            managed_generation,
        };
        transaction.execute(
            "INSERT INTO install_intents(
                share_id,records_json,failure_fingerprint,failure_diagnostic
             ) VALUES(?1,?2,NULL,NULL)",
            params![share.0, serde_json::to_string(&intent)?],
        )?;
        transaction.execute("DELETE FROM pending_objects WHERE share_id=?1", [&share.0])?;
        transaction.commit()?;
        Ok((intent, true))
    }

    pub fn mark_object_receiving(&self, share: &ShareId, hash: &ObjectHash) -> Result<()> {
        self.conn.execute(
            "INSERT INTO pending_objects(share_id,hash,provenance) VALUES(?1,?2,'receiving')
             ON CONFLICT(share_id,hash) DO UPDATE SET provenance='receiving'",
            params![share.0, hash.as_str()],
        )?;
        Ok(())
    }

    pub fn mark_object_verified(&self, share: &ShareId, hash: &ObjectHash) -> Result<()> {
        let changed = self.conn.execute(
            "UPDATE pending_objects SET provenance='verified_from_bound_peer'
             WHERE share_id=?1 AND hash=?2 AND provenance='receiving'",
            params![share.0, hash.as_str()],
        )?;
        if changed != 1 {
            bail!("received object is missing its pending ownership record");
        }
        Ok(())
    }

    pub fn mark_object_generated(&self, share: &ShareId, hash: &ObjectHash) -> Result<()> {
        self.conn.execute(
            "INSERT INTO pending_objects(share_id,hash,provenance) VALUES(?1,?2,'generated_local')
             ON CONFLICT(share_id,hash) DO UPDATE SET provenance='generated_local'",
            params![share.0, hash.as_str()],
        )?;
        Ok(())
    }

    pub fn clear_pending_objects(&self, share: &ShareId) -> Result<()> {
        self.conn
            .execute("DELETE FROM pending_objects WHERE share_id=?1", [&share.0])?;
        Ok(())
    }

    pub fn share_authorized_objects(
        &self,
        share: &ShareId,
        candidates: &HashSet<ObjectHash>,
    ) -> Result<HashSet<ObjectHash>> {
        let mut authorized = HashSet::new();
        for record in self.records(share)? {
            remember_candidate_entry_hash(&mut authorized, candidates, &record.version.entry);
            if let Some(base) = record.version.merge_base {
                remember_candidate_entry_hash(&mut authorized, candidates, &base.entry);
            }
        }
        for base in self.shared_heads(share)?.into_values() {
            remember_candidate_entry_hash(&mut authorized, candidates, &base.entry);
        }
        let mut statement = self.conn.prepare(
            "SELECT winner_json,loser_json,conflict_json FROM conflicts WHERE share_id=?1",
        )?;
        let rows = statement.query_map([&share.0], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })?;
        for row in rows {
            let (winner, loser, document) = row?;
            let conflict = decode_conflict(&winner, &loser, document.as_deref())?;
            for entry in conflict_entries(&conflict) {
                remember_candidate_entry_hash(&mut authorized, candidates, entry);
            }
        }
        if let Some(intent) = self.install_intent(share)? {
            for record in intent.records {
                remember_candidate_entry_hash(&mut authorized, candidates, &record.version.entry);
            }
            for conflict in intent.conflicts {
                for entry in conflict_entries(&conflict) {
                    remember_candidate_entry_hash(&mut authorized, candidates, entry);
                }
            }
        }
        let mut statement = self.conn.prepare(
            "SELECT hash FROM pending_objects
             WHERE share_id=?1 AND provenance!='receiving'",
        )?;
        for row in statement.query_map([&share.0], |row| row.get::<_, String>(0))? {
            let hash = ObjectHash::parse(row?)?;
            if candidates.contains(&hash) {
                authorized.insert(hash);
            }
        }
        Ok(authorized)
    }

    pub fn finish_install(
        &mut self,
        share: &ShareId,
        expected: &InstallIntent,
        records: &[Record],
    ) -> Result<()> {
        self.finish_install_inner(share, expected, records, None)?;
        Ok(())
    }

    pub fn finish_install_and_enable_managed(
        &mut self,
        share: &ShareId,
        expected: &InstallIntent,
        records: &[Record],
        expected_generation: i64,
    ) -> Result<QueueRequest> {
        self.finish_install_inner(share, expected, records, Some(expected_generation))?
            .context("managed initial completion did not publish its queue request")
    }

    fn finish_install_inner(
        &mut self,
        share: &ShareId,
        expected: &InstallIntent,
        records: &[Record],
        managed_generation: Option<i64>,
    ) -> Result<Option<QueueRequest>> {
        let _publication = managed_generation
            .map(|_| self.lock_scheduler_publication())
            .transpose()?;
        let mut owner = None;
        let managed = if let Some(expected_generation) = managed_generation {
            self.cleanup_stale_sync_queue()?;
            self.cleanup_scheduler_orphans()?;
            self.ensure_scheduler_capacity()?;
            self.peer_id()?;
            let generation = expected_generation
                .checked_add(1)
                .context("watch intent generation overflow")?;
            let token = format!("request-{}", ShareId::generate().0);
            owner = Some(create_scheduler_owner(&self.dir, &token)?);
            Some((expected_generation, generation, token))
        } else {
            None
        };

        let result = (|| -> Result<Option<(i64, i64, String)>> {
            let current = self
                .install_intent(share)?
                .context("install intent disappeared before commit")?;
            if current.records != expected.records
                || current.conflicts != expected.conflicts
                || current.managed_generation != expected.managed_generation
                || current.temps.len() != expected.temps.len()
                || current
                    .temps
                    .iter()
                    .zip(&expected.temps)
                    .any(|(current, expected)| current.path != expected.path)
            {
                bail!("install intent changed before commit");
            }
            self.ensure_recovery_limits(share, &current.conflicts)?;
            let tx = self
                .conn
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            tx.execute("DELETE FROM records WHERE share_id=?1", [&share.0])?;
            for record in records {
                tx.execute(
                    "INSERT INTO records(share_id,path,version_json) VALUES(?1,?2,?3)",
                    params![
                        share.0,
                        record.path.as_bytes(),
                        serde_json::to_string(&record.version)?
                    ],
                )?;
            }
            for conflict in &current.conflicts {
                insert_conflict(&tx, share, conflict)?;
            }
            let published = if let Some((expected_generation, generation, token)) = &managed {
                let count: i64 =
                    tx.query_row("SELECT COUNT(*) FROM sync_queue", [], |row| row.get(0))?;
                if count >= MAX_SYNC_QUEUE_ROWS {
                    bail!("synchronization queue is full");
                }
                let changed = tx.execute(
                    "UPDATE shares SET initial_complete=1,watch_enabled=1,
                         intent_generation=?3,blocked_diagnostic=NULL
                     WHERE share_id=?1 AND intent_generation=?2
                           AND initial_complete=0 AND removing_relationship IS NULL",
                    params![share.0, expected_generation, generation],
                )?;
                if changed == 0 {
                    bail!(
                        "sync was stopped, removed, or reconfigured while its initial plan was applying"
                    );
                }
                tx.execute(
                    "INSERT INTO sync_queue(token,share_id,operation,generation)
                     VALUES(?1,?2,'watch',?3)",
                    params![token, share.0, generation],
                )?;
                Some((tx.last_insert_rowid(), *generation, token.clone()))
            } else {
                None
            };
            clear_install_failure_diagnostic(&tx, share)?;
            tx.execute("DELETE FROM install_intents WHERE share_id=?1", [&share.0])?;
            tx.commit()?;
            Ok(published)
        })();

        match result {
            Ok(Some((ticket, generation, token))) => Ok(Some(QueueRequest {
                state_dir: self.dir.clone(),
                ticket,
                token,
                share: Some(share.clone()),
                generation: Some(generation),
                owner,
            })),
            Ok(None) => Ok(None),
            Err(error) => {
                drop(owner.take());
                if let Some((_, _, token)) = managed {
                    let _ = remove_scheduler_owner(&self.dir, &token);
                }
                Err(error)
            }
        }
    }

    pub fn clear_install_intent(&mut self, share: &ShareId) -> Result<()> {
        let transaction = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        clear_install_failure_diagnostic(&transaction, share)?;
        transaction.execute("DELETE FROM install_intents WHERE share_id=?1", [&share.0])?;
        transaction.commit()?;
        Ok(())
    }

    pub fn unsettled_paths(&self, share: &ShareId) -> Result<Vec<RelativePath>> {
        let mut statement = self
            .conn
            .prepare("SELECT path FROM unsettled_paths WHERE share_id=?1 ORDER BY path")?;
        statement
            .query_map([&share.0], |row| row.get::<_, Vec<u8>>(0))?
            .map(|row| RelativePath::from_bytes(row?))
            .collect()
    }

    pub fn remember_unsettled_path(&mut self, share: &ShareId, path: &RelativePath) -> Result<()> {
        self.remember_unsettled_paths(share, std::slice::from_ref(path))
    }

    pub fn remember_unsettled_paths(
        &mut self,
        share: &ShareId,
        paths: &[RelativePath],
    ) -> Result<()> {
        let tx = self.conn.transaction()?;
        for path in paths {
            tx.execute(
                "INSERT OR IGNORE INTO unsettled_paths(share_id,path) VALUES(?1,?2)",
                params![share.0, path.as_bytes()],
            )?;
        }
        ensure_unsettled_limit(&tx, share)?;
        tx.commit()?;
        Ok(())
    }

    pub fn clear_unsettled_paths(&self, share: &ShareId) -> Result<Vec<RelativePath>> {
        let paths = self.unsettled_paths(share)?;
        self.conn
            .execute("DELETE FROM unsettled_paths WHERE share_id=?1", [&share.0])?;
        Ok(paths)
    }

    pub fn retire_invalidated_install(
        &mut self,
        share: &ShareId,
        expected: &InstallIntent,
        records: &[Record],
        conflicts: &[Conflict],
        unsettled: &RelativePath,
    ) -> Result<()> {
        let current = self
            .install_intent(share)?
            .context("install intent disappeared before invalidation recovery")?;
        if current.records != expected.records
            || current.temps.len() != expected.temps.len()
            || current
                .temps
                .iter()
                .zip(&expected.temps)
                .any(|(current, expected)| {
                    current.path != expected.path || current.token != expected.token
                })
        {
            bail!("install intent changed before invalidation recovery");
        }
        self.ensure_recovery_limits(share, conflicts)?;
        let tx = self.conn.transaction()?;
        tx.execute("DELETE FROM records WHERE share_id=?1", [&share.0])?;
        for record in records {
            tx.execute(
                "INSERT INTO records(share_id,path,version_json) VALUES(?1,?2,?3)",
                params![
                    share.0,
                    record.path.as_bytes(),
                    serde_json::to_string(&record.version)?
                ],
            )?;
        }
        for conflict in conflicts {
            insert_conflict(&tx, share, conflict)?;
        }
        tx.execute(
            "INSERT OR IGNORE INTO unsettled_paths(share_id,path) VALUES(?1,?2)",
            params![share.0, unsettled.as_bytes()],
        )?;
        ensure_unsettled_limit(&tx, share)?;
        clear_install_failure_diagnostic(&tx, share)?;
        tx.execute("DELETE FROM install_intents WHERE share_id=?1", [&share.0])?;
        tx.commit()?;
        Ok(())
    }

    pub fn install_intents(&self) -> Result<Vec<(ShareId, InstallIntent)>> {
        let mut statement = self
            .conn
            .prepare("SELECT share_id, records_json FROM install_intents ORDER BY share_id")?;
        statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .map(|row| {
                let (share, records) = row?;
                Ok((ShareId(share), serde_json::from_str(&records)?))
            })
            .collect()
    }

    pub fn is_owned_temp(&self, share: &ShareId, path: &Path) -> Result<bool> {
        let Some(intent) = self.install_intent(share)? else {
            return Ok(false);
        };
        Ok(intent
            .temps
            .iter()
            .filter(|temp| temp.phase != InstallTempPhase::Pending)
            .any(|temp| {
                let mut owned = temp.path.to_path_buf();
                owned.set_file_name(&temp.token);
                owned == path
            }))
    }

    pub fn mark_install_temp_owned(&self, share: &ShareId, path: &RelativePath) -> Result<()> {
        self.set_install_temp_phase(share, path, InstallTempPhase::Owned)
    }

    pub fn mark_install_temp_creating(&self, share: &ShareId, path: &RelativePath) -> Result<()> {
        self.set_install_temp_phase(share, path, InstallTempPhase::Creating)
    }

    fn set_install_temp_phase(
        &self,
        share: &ShareId,
        path: &RelativePath,
        phase: InstallTempPhase,
    ) -> Result<()> {
        let mut intent = self
            .install_intent(share)?
            .context("install intent is missing")?;
        let expected_json = serde_json::to_string(&intent)?;
        let temp = intent
            .temps
            .iter_mut()
            .find(|temp| &temp.path == path)
            .context("install temporary is missing")?;
        temp.phase = phase;
        self.replace_install_intent_json(share, &expected_json, &serde_json::to_string(&intent)?)
    }

    pub fn rotate_unowned_install_temp(
        &self,
        share: &ShareId,
        path: &RelativePath,
    ) -> Result<String> {
        let mut intent = self
            .install_intent(share)?
            .context("install intent is missing")?;
        let expected_json = serde_json::to_string(&intent)?;
        let temp = intent
            .temps
            .iter_mut()
            .find(|temp| &temp.path == path)
            .context("install temporary is missing")?;
        if temp.phase == InstallTempPhase::Owned {
            bail!("cannot rotate an owned install temporary");
        }
        temp.token = format!(".flocal-tmp-{}", ShareId::generate().0);
        temp.phase = InstallTempPhase::Pending;
        let token = temp.token.clone();
        self.replace_install_intent_json(share, &expected_json, &serde_json::to_string(&intent)?)?;
        Ok(token)
    }

    fn replace_install_intent_json(
        &self,
        share: &ShareId,
        expected_json: &str,
        replacement_json: &str,
    ) -> Result<()> {
        let transaction = rusqlite::Transaction::new_unchecked(
            &self.conn,
            rusqlite::TransactionBehavior::Immediate,
        )?;
        let classification: Option<(Option<String>, Option<String>)> = transaction
            .query_row(
                "SELECT failure_fingerprint,failure_diagnostic
                 FROM install_intents WHERE share_id=?1 AND records_json=?2",
                params![share.0, expected_json],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((failure_fingerprint, failure_diagnostic)) = classification else {
            bail!("install intent changed while updating its recovery state");
        };
        let expected: InstallIntent = serde_json::from_str(expected_json)?;
        let failure =
            decode_install_intent_failure(&expected, failure_fingerprint, failure_diagnostic)?;
        let changed = transaction.execute(
            "UPDATE install_intents
             SET records_json=?3,failure_fingerprint=NULL,failure_diagnostic=NULL
             WHERE share_id=?1 AND records_json=?2",
            params![share.0, expected_json, replacement_json],
        )?;
        if changed != 1 {
            bail!("install intent changed while updating its recovery state");
        }
        if let Some(failure) = failure {
            transaction.execute(
                "UPDATE shares SET blocked_diagnostic=NULL
                 WHERE share_id=?1 AND blocked_diagnostic=?2",
                params![share.0, failure.diagnostic],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn lock_share(&self, id: &ShareId) -> Result<File> {
        let locks = self.ensure_private_state_child("locks")?;
        let name = blake3::hash(id.0.as_bytes()).to_hex().to_string();
        let path = locks.join(name);
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        set_private_file(&path)?;
        file.try_lock_exclusive()
            .context("another sync/watch operation already owns this share")?;
        Ok(file)
    }

    pub fn lock_share_session(&self, id: &ShareId) -> Result<File> {
        let sessions = self.ensure_private_state_child("sessions")?;
        let name = blake3::hash(id.0.as_bytes()).to_hex().to_string();
        let path = sessions.join(name);
        if let Ok(metadata) = fs::symlink_metadata(&path)
            && (!metadata.is_file() || metadata.file_type().is_symlink())
        {
            bail!("share session lock is not a regular file");
        }
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        set_private_file(&path)?;
        file.try_lock_exclusive()
            .context("another sync/watch session already owns this share")?;
        Ok(file)
    }

    fn try_lock_global_sync_final(&self) -> Result<Option<File>> {
        let path = self.dir.join("sync.lock");
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        set_private_file(&path)?;
        match file.try_lock_exclusive() {
            Ok(()) => Ok(Some(file)),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                #[cfg(feature = "e2e-test-hooks")]
                self.observe_e2e_global_contention()?;
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }

    #[cfg(all(test, feature = "e2e-test-hooks"))]
    fn lock_global_sync(&self) -> Result<File> {
        self.try_lock_global_sync_final()?
            .context("another synchronization operation already owns this installation")
    }

    fn lock_scheduler_publication(&self) -> Result<File> {
        let path = self.dir.join("scheduler.publish.lock");
        if let Ok(metadata) = fs::symlink_metadata(&path)
            && (!metadata.is_file() || metadata.file_type().is_symlink())
        {
            bail!("scheduler publication lock is not a regular file");
        }
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        set_private_file(&path)?;
        file.lock_exclusive()?;
        Ok(file)
    }

    pub fn enqueue_sync(
        &mut self,
        share: Option<&ShareId>,
        operation: SyncOperation,
        generation: Option<i64>,
    ) -> Result<QueueRequest> {
        self.enqueue_sync_inner(
            share,
            None,
            operation,
            generation,
            None,
            None,
            None,
            MAX_SYNC_QUEUE_ROWS,
        )
    }

    fn scheduled_mutation<T>(
        &mut self,
        share: Option<&ShareId>,
        operation: SyncOperation,
        mutate: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        let permit = self
            .enqueue_sync(share, operation, None)?
            .wait_with_prepare(|| false, |_| Ok(()), crate::sync::recover_installs_locked)?;
        let result = mutate(self)?;
        permit.finish()?;
        Ok(result)
    }

    pub fn enqueue_pending_authority(
        &mut self,
        share: &ShareId,
        relationship: &RelationshipId,
        operation: SyncOperation,
        generation: Option<i64>,
    ) -> Result<QueueRequest> {
        self.enqueue_sync_inner(
            Some(share),
            Some(relationship),
            operation,
            generation,
            Some(&format!("relationship:{}", relationship.0)),
            Some(PairedQueueState::PendingAuthority),
            None,
            MAX_SYNC_QUEUE_ROWS,
        )
    }

    pub fn convert_managed_to_pending_authority(
        &mut self,
        token: &str,
        share: &ShareId,
        relationship: &RelationshipId,
        operation: SyncOperation,
        generation: Option<i64>,
    ) -> Result<bool> {
        validate_scheduler_token(token)?;
        relationship.validate()?;
        let transaction = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let dedupe_key = format!("relationship:{}", relationship.0);
        if transaction.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM sync_queue WHERE dedupe_key=?1 AND token<>?2
             )",
            params![dedupe_key, token],
            |row| row.get::<_, i64>(0),
        )? != 0
        {
            bail!("a synchronization request is already pending for this relationship");
        }
        honor_relationship_yield(&transaction, relationship, now_ns())?;
        let changed = transaction.execute(
            "UPDATE sync_queue
             SET relationship_id=?3,dedupe_key=?4,paired_state='pending_authority',
                 network_authority=NULL,network_order=NULL,pair_nonce=NULL,predecessor_fingerprint=NULL,
                 proxy_acknowledged=0
             WHERE token=?1 AND share_id=?2 AND operation=?5 AND generation IS ?6
                   AND paired_state IS NULL AND active=0",
            params![
                token,
                share.0,
                relationship.0,
                dedupe_key,
                operation.as_str(),
                generation,
            ],
        )?;
        transaction.commit()?;
        Ok(changed != 0)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn convert_managed_to_authoritative_parked(
        &mut self,
        token: &str,
        share: &ShareId,
        relationship: &RelationshipId,
        operation: SyncOperation,
        generation: Option<i64>,
        network_authority: &PeerId,
        pair_nonce: &str,
        predecessor_fingerprint: &str,
    ) -> Result<Option<i64>> {
        validate_scheduler_token(token)?;
        relationship.validate()?;
        validate_pair_value("pair nonce", pair_nonce)?;
        validate_pair_value("predecessor fingerprint", predecessor_fingerprint)?;
        validate_pair_value("network authority", &network_authority.0)?;
        let transaction = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let dedupe_key = format!("relationship:{}", relationship.0);
        if transaction.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM sync_queue WHERE dedupe_key=?1 AND token<>?2
             )",
            params![dedupe_key, token],
            |row| row.get::<_, i64>(0),
        )? != 0
        {
            bail!("a synchronization request is already pending for this relationship");
        }
        honor_relationship_yield(&transaction, relationship, now_ns())?;
        let network_order = transaction.query_row(
            "SELECT COALESCE(MAX(network_order),0)+1 FROM sync_queue
             WHERE network_authority=?1",
            [&network_authority.0],
            |row| row.get::<_, i64>(0),
        )?;
        if network_order <= 0 {
            bail!("network synchronization order exhausted");
        }
        let changed = transaction.execute(
            "UPDATE sync_queue
             SET relationship_id=?3,dedupe_key=?4,paired_state='parked',
                 network_authority=?5,network_order=?6,pair_nonce=?7,predecessor_fingerprint=?8,
                 proxy_acknowledged=0
             WHERE token=?1 AND share_id=?2 AND operation=?9 AND generation IS ?10
                   AND paired_state IS NULL AND active=0",
            params![
                token,
                share.0,
                relationship.0,
                dedupe_key,
                network_authority.0,
                network_order,
                pair_nonce,
                predecessor_fingerprint,
                operation.as_str(),
                generation,
            ],
        )?;
        transaction.commit()?;
        Ok((changed != 0).then_some(network_order))
    }

    #[allow(clippy::too_many_arguments)]
    fn enqueue_sync_inner(
        &mut self,
        share: Option<&ShareId>,
        relationship: Option<&RelationshipId>,
        operation: SyncOperation,
        generation: Option<i64>,
        dedupe_key: Option<&str>,
        paired_state: Option<PairedQueueState>,
        network_order: Option<i64>,
        max_rows: i64,
    ) -> Result<QueueRequest> {
        let _publication = self.lock_scheduler_publication()?;
        if operation == SyncOperation::Watch
            && paired_state.is_none()
            && let (Some(share), Some(generation)) = (share, generation)
            && let Some(request) = self.reclaim_managed_queue_request(share, generation)?
        {
            return Ok(request);
        }
        self.cleanup_stale_sync_queue()?;
        self.cleanup_scheduler_orphans()?;
        self.ensure_scheduler_capacity()?;
        self.peer_id()?;

        let token = format!("request-{}", ShareId::generate().0);
        let mut owner = None;
        let transaction = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let result = (|| -> Result<i64> {
            let count: i64 =
                transaction.query_row("SELECT COUNT(*) FROM sync_queue", [], |row| row.get(0))?;
            if count >= max_rows {
                bail!("synchronization queue is full");
            }
            if let Some(dedupe_key) = dedupe_key
                && transaction.query_row(
                    "SELECT EXISTS(SELECT 1 FROM sync_queue WHERE dedupe_key=?1)",
                    [dedupe_key],
                    |row| row.get::<_, i64>(0),
                )? != 0
            {
                bail!("a synchronization request is already pending for this relationship");
            }
            if let Some(relationship) = relationship {
                honor_relationship_yield(&transaction, relationship, now_ns())?;
            }
            owner = Some(create_scheduler_owner(&self.dir, &token)?);
            transaction.execute(
                "INSERT INTO sync_queue(
                    token,share_id,relationship_id,operation,generation,dedupe_key,
                    network_order,paired_state
                 ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                params![
                    token,
                    share.map(|share| &share.0),
                    relationship.map(|relationship| &relationship.0),
                    operation.as_str(),
                    generation,
                    dedupe_key,
                    network_order,
                    paired_state.map(PairedQueueState::as_str),
                ],
            )?;
            Ok(transaction.last_insert_rowid())
        })();
        let ticket = match result {
            Ok(ticket) => {
                if let Err(error) = transaction.commit() {
                    drop(owner.take());
                    let _ = remove_scheduler_owner(&self.dir, &token);
                    return Err(error.into());
                }
                ticket
            }
            Err(error) => {
                drop(transaction);
                drop(owner.take());
                let _ = fs::remove_file(self.dir.join("scheduler").join(&token));
                return Err(error);
            }
        };

        #[cfg(feature = "e2e-test-hooks")]
        if self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sync_queue WHERE active=1)",
            [],
            |row| row.get::<_, i64>(0),
        )? != 0
        {
            self.observe_e2e_global_contention()?;
        }

        Ok(QueueRequest {
            state_dir: self.dir.clone(),
            ticket,
            token,
            share: share.cloned(),
            generation,
            owner,
        })
    }

    fn reclaim_managed_queue_request(
        &mut self,
        share: &ShareId,
        generation: i64,
    ) -> Result<Option<QueueRequest>> {
        let existing: Option<(i64, String)> = self
            .conn
            .query_row(
                "SELECT ticket,token FROM sync_queue
                 WHERE share_id=?1 AND operation='watch' AND generation=?2",
                params![share.0, generation],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((ticket, token)) = existing else {
            return Ok(None);
        };
        validate_scheduler_token(&token)?;
        let owner = match open_scheduler_owner(&self.dir, &token) {
            Ok(owner) => owner,
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
            {
                create_scheduler_owner(&self.dir, &token)?
            }
            Err(error) => return Err(error),
        };
        owner.lock_shared()?;
        let transaction = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let changed = transaction.execute(
            "UPDATE sync_queue
                 SET active=0,relationship_id=NULL,dedupe_key=NULL,network_authority=NULL,network_order=NULL,
                 paired_state=NULL,pair_nonce=NULL,predecessor_fingerprint=NULL,
                 proxy_acknowledged=0
             WHERE ticket=?1 AND token=?2 AND share_id=?3
                   AND operation='watch' AND generation=?4",
            params![ticket, token, share.0, generation],
        )?;
        transaction.commit()?;
        if changed == 0 {
            drop(owner);
            remove_scheduler_owner(&self.dir, &token)?;
            return Ok(None);
        }
        Ok(Some(QueueRequest {
            state_dir: self.dir.clone(),
            ticket,
            token,
            share: Some(share.clone()),
            generation: Some(generation),
            owner: Some(owner),
        }))
    }

    pub fn cancel_sync_request(&mut self, token: &str) -> Result<bool> {
        validate_scheduler_token(token)?;
        let transaction = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let changed = transaction.execute(
            "DELETE FROM sync_queue WHERE token=?1 AND active=0",
            [token],
        )?;
        transaction.commit()?;
        Ok(changed != 0)
    }

    pub fn scheduling_snapshot(&mut self) -> Result<SchedulingSnapshot> {
        self.cleanup_stale_sync_queue()?;
        let completion_sequence = self.conn.query_row(
            "SELECT COALESCE((SELECT scheduler_completion_sequence
                              FROM installation WHERE singleton=1),0)",
            [],
            |row| row.get(0),
        )?;
        let rows = scheduled_requests(&self.conn)?;
        let mut active = None;
        let mut queued = Vec::new();
        for request in rows {
            if request.active {
                active = Some(request);
            } else {
                queued.push(request);
            }
        }
        Ok(SchedulingSnapshot {
            completion_sequence,
            active,
            queued,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn enqueue_authoritative_sync(
        &mut self,
        share: &ShareId,
        relationship: &RelationshipId,
        operation: SyncOperation,
        generation: Option<i64>,
        network_authority: &PeerId,
        pair_nonce: &str,
        predecessor_fingerprint: &str,
    ) -> Result<(QueueRequest, i64)> {
        self.enqueue_parked_sync(
            share,
            relationship,
            operation,
            generation,
            network_authority,
            None,
            pair_nonce,
            predecessor_fingerprint,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn enqueue_parked_proxy(
        &mut self,
        share: &ShareId,
        relationship: &RelationshipId,
        operation: SyncOperation,
        generation: Option<i64>,
        network_authority: &PeerId,
        network_order: i64,
        pair_nonce: &str,
        predecessor_fingerprint: &str,
    ) -> Result<QueueRequest> {
        self.enqueue_parked_sync(
            share,
            relationship,
            operation,
            generation,
            network_authority,
            Some(network_order),
            pair_nonce,
            predecessor_fingerprint,
            true,
        )
        .map(|(request, _)| request)
    }

    #[allow(clippy::too_many_arguments)]
    fn enqueue_parked_sync(
        &mut self,
        share: &ShareId,
        relationship: &RelationshipId,
        operation: SyncOperation,
        generation: Option<i64>,
        network_authority: &PeerId,
        network_order: Option<i64>,
        pair_nonce: &str,
        predecessor_fingerprint: &str,
        proxy_acknowledged: bool,
    ) -> Result<(QueueRequest, i64)> {
        relationship.validate()?;
        validate_pair_value("pair nonce", pair_nonce)?;
        validate_pair_value("predecessor fingerprint", predecessor_fingerprint)?;
        validate_pair_value("network authority", &network_authority.0)?;
        if network_order.is_some_and(|order| order <= 0) {
            bail!("network order must be positive");
        }
        let _publication = self.lock_scheduler_publication()?;
        self.cleanup_stale_sync_queue()?;
        self.cleanup_scheduler_orphans()?;
        self.ensure_scheduler_capacity()?;
        self.peer_id()?;
        let token = format!("request-{}", ShareId::generate().0);
        let mut owner = None;
        let transaction = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let result = (|| -> Result<(i64, i64)> {
            let count: i64 =
                transaction.query_row("SELECT COUNT(*) FROM sync_queue", [], |row| row.get(0))?;
            if count >= MAX_SYNC_QUEUE_ROWS {
                bail!("synchronization queue is full");
            }
            let dedupe_key = format!("relationship:{}", relationship.0);
            if transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM sync_queue WHERE dedupe_key=?1)",
                [&dedupe_key],
                |row| row.get::<_, i64>(0),
            )? != 0
            {
                bail!("a synchronization request is already pending for this relationship");
            }
            honor_relationship_yield(&transaction, relationship, now_ns())?;
            let network_order = match network_order {
                Some(order) => order,
                None => transaction
                    .query_row(
                        "SELECT COALESCE(MAX(network_order),0)+1 FROM sync_queue
                         WHERE network_authority=?1",
                        [&network_authority.0],
                        |row| row.get::<_, i64>(0),
                    )
                    .context("allocating network synchronization order")?,
            };
            if network_order <= 0 {
                bail!("network synchronization order exhausted");
            }
            owner = Some(create_scheduler_owner(&self.dir, &token)?);
            transaction.execute(
                "INSERT INTO sync_queue(
                    token,share_id,relationship_id,operation,generation,dedupe_key,
                    network_authority,network_order,paired_state,pair_nonce,predecessor_fingerprint,
                    proxy_acknowledged
                 ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,'parked',?9,?10,?11)",
                params![
                    token,
                    share.0,
                    relationship.0,
                    operation.as_str(),
                    generation,
                    dedupe_key,
                    network_authority.0,
                    network_order,
                    pair_nonce,
                    predecessor_fingerprint,
                    i64::from(proxy_acknowledged),
                ],
            )?;
            Ok((transaction.last_insert_rowid(), network_order))
        })();
        let (ticket, network_order) = match result {
            Ok(result) => {
                if let Err(error) = transaction.commit() {
                    drop(owner.take());
                    let _ = remove_scheduler_owner(&self.dir, &token);
                    return Err(error.into());
                }
                result
            }
            Err(error) => {
                drop(transaction);
                drop(owner.take());
                let _ = remove_scheduler_owner(&self.dir, &token);
                return Err(error);
            }
        };
        Ok((
            QueueRequest {
                state_dir: self.dir.clone(),
                ticket,
                token,
                share: Some(share.clone()),
                generation,
                owner,
            },
            network_order,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn convert_pending_authority_to_parked(
        &mut self,
        token: &str,
        share: &ShareId,
        relationship: &RelationshipId,
        operation: SyncOperation,
        generation: Option<i64>,
        network_authority: &PeerId,
        network_order: i64,
        pair_nonce: &str,
        predecessor_fingerprint: &str,
    ) -> Result<bool> {
        validate_scheduler_token(token)?;
        relationship.validate()?;
        validate_pair_value("pair nonce", pair_nonce)?;
        validate_pair_value("predecessor fingerprint", predecessor_fingerprint)?;
        validate_pair_value("network authority", &network_authority.0)?;
        if network_order <= 0 {
            bail!("network order must be positive");
        }
        let transaction = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        honor_relationship_yield(&transaction, relationship, now_ns())?;
        let changed = transaction.execute(
            "UPDATE sync_queue
             SET paired_state='parked',network_authority=?6,network_order=?7,pair_nonce=?8,
                 predecessor_fingerprint=?9,proxy_acknowledged=1
             WHERE token=?1 AND share_id=?2 AND relationship_id=?3
                   AND operation=?4 AND generation IS ?5
                   AND paired_state='pending_authority' AND active=0",
            params![
                token,
                share.0,
                relationship.0,
                operation.as_str(),
                generation,
                network_authority.0,
                network_order,
                pair_nonce,
                predecessor_fingerprint,
            ],
        )?;
        transaction.commit()?;
        Ok(changed != 0)
    }

    pub fn acknowledge_proxy_issue(
        &self,
        token: &str,
        relationship: &RelationshipId,
        network_authority: &PeerId,
        network_order: i64,
    ) -> Result<bool> {
        validate_scheduler_token(token)?;
        relationship.validate()?;
        Ok(self.conn.execute(
            "UPDATE sync_queue SET proxy_acknowledged=1
             WHERE token=?1 AND relationship_id=?2 AND network_authority=?3 AND network_order=?4
                   AND paired_state IN ('parked','prepared','eligible') AND active=0",
            params![token, relationship.0, network_authority.0, network_order],
        )? != 0)
    }

    pub fn next_unacknowledged_proxy(&mut self) -> Result<Option<ScheduledRequestSnapshot>> {
        let snapshot = self.scheduling_snapshot()?;
        Ok(snapshot
            .queued
            .into_iter()
            .filter(|request| {
                request.network_order.is_some()
                    && request.paired_state.is_some()
                    && !request.proxy_acknowledged
            })
            .min_by_key(|request| {
                (
                    request
                        .network_authority
                        .as_ref()
                        .map(|authority| authority.0.clone()),
                    request.network_order,
                    request.ticket,
                )
            }))
    }

    pub fn prepare_paired_sync(
        &mut self,
        token: &str,
        relationship: &RelationshipId,
        network_authority: &PeerId,
        network_order: i64,
        pair_nonce: &str,
    ) -> Result<Option<String>> {
        validate_scheduler_token(token)?;
        relationship.validate()?;
        validate_pair_value("pair nonce", pair_nonce)?;
        let transaction = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let ticket = exact_paired_ticket(
            &transaction,
            token,
            relationship,
            network_authority,
            network_order,
            pair_nonce,
            "parked",
        )?;
        if has_active_or_eligible_predecessor(
            &transaction,
            ticket,
            network_authority,
            network_order,
        )? {
            transaction.commit()?;
            return Ok(None);
        }
        let predecessor =
            paired_predecessor_fingerprint(&transaction, ticket, network_authority, network_order)?;
        let changed = transaction.execute(
            "UPDATE sync_queue SET paired_state='prepared',predecessor_fingerprint=?6
             WHERE token=?1 AND relationship_id=?2 AND network_authority=?3 AND network_order=?4
                   AND pair_nonce=?5 AND paired_state='parked' AND active=0",
            params![
                token,
                relationship.0,
                network_authority.0,
                network_order,
                pair_nonce,
                predecessor
            ],
        )?;
        if changed != 1 {
            bail!("paired synchronization changed while preparing");
        }
        transaction.commit()?;
        Ok(Some(predecessor))
    }

    pub fn commit_paired_sync(
        &mut self,
        token: &str,
        relationship: &RelationshipId,
        network_authority: &PeerId,
        network_order: i64,
        pair_nonce: &str,
        predecessor_fingerprint: &str,
    ) -> Result<bool> {
        validate_scheduler_token(token)?;
        relationship.validate()?;
        validate_pair_value("pair nonce", pair_nonce)?;
        validate_pair_value("predecessor fingerprint", predecessor_fingerprint)?;
        let transaction = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let ticket = exact_paired_ticket(
            &transaction,
            token,
            relationship,
            network_authority,
            network_order,
            pair_nonce,
            "prepared",
        )?;
        let current =
            paired_predecessor_fingerprint(&transaction, ticket, network_authority, network_order)?;
        let recovery_pending = transaction.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM install_intents WHERE failure_fingerprint IS NULL
             )",
            [],
            |row| row.get::<_, i64>(0),
        )? != 0;
        if current != predecessor_fingerprint
            || recovery_pending
            || has_active_or_eligible_predecessor(
                &transaction,
                ticket,
                network_authority,
                network_order,
            )?
        {
            transaction.execute(
                "UPDATE sync_queue SET paired_state='parked'
                 WHERE token=?1 AND relationship_id=?2 AND network_authority=?3 AND network_order=?4
                       AND pair_nonce=?5 AND paired_state='prepared' AND active=0",
                params![
                    token,
                    relationship.0,
                    network_authority.0,
                    network_order,
                    pair_nonce
                ],
            )?;
            transaction.commit()?;
            return Ok(false);
        }
        let changed = transaction.execute(
            "UPDATE sync_queue SET paired_state='eligible'
             WHERE token=?1 AND relationship_id=?2 AND network_authority=?3 AND network_order=?4
                   AND pair_nonce=?5 AND predecessor_fingerprint=?6
                   AND paired_state='prepared' AND active=0",
            params![
                token,
                relationship.0,
                network_authority.0,
                network_order,
                pair_nonce,
                predecessor_fingerprint,
            ],
        )?;
        if changed != 1 {
            bail!("paired synchronization changed while committing");
        }
        transaction.commit()?;
        Ok(true)
    }

    pub fn park_paired_sync(
        &self,
        token: &str,
        relationship: &RelationshipId,
        network_authority: &PeerId,
        network_order: i64,
        pair_nonce: &str,
    ) -> Result<bool> {
        validate_scheduler_token(token)?;
        relationship.validate()?;
        Ok(self.conn.execute(
            "UPDATE sync_queue SET paired_state='parked'
             WHERE token=?1 AND relationship_id=?2 AND network_authority=?3 AND network_order=?4
                   AND pair_nonce=?5 AND paired_state IN ('prepared','eligible')
                   AND active=0",
            params![
                token,
                relationship.0,
                network_authority.0,
                network_order,
                pair_nonce
            ],
        )? != 0)
    }

    pub fn paired_queue_position(&mut self, token: &str) -> Result<QueuePosition> {
        validate_scheduler_token(token)?;
        let snapshot = self.scheduling_snapshot()?;
        let request = snapshot
            .queued
            .iter()
            .find(|request| request.token == token)
            .context("paired synchronization request is missing")?;
        Ok(QueuePosition {
            ticket: request.ticket,
            position: snapshot.queue_position(request).unwrap_or(0),
            active: snapshot.active,
        })
    }

    pub fn record_relationship_yield(
        &mut self,
        relationship: &RelationshipId,
        retry_not_before_ns: i64,
    ) -> Result<()> {
        self.peer_id()?;
        let transaction = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let sequence: i64 = transaction.query_row(
            "SELECT scheduler_completion_sequence FROM installation WHERE singleton=1",
            [],
            |row| row.get(0),
        )?;
        transaction.execute(
            "INSERT INTO relationship_yields(
                relationship_id,after_completion_sequence,retry_not_before_ns
             ) VALUES(?1,?2,?3)
             ON CONFLICT(relationship_id) DO UPDATE SET
                after_completion_sequence=excluded.after_completion_sequence,
                retry_not_before_ns=excluded.retry_not_before_ns",
            params![relationship.0, sequence, retry_not_before_ns.to_string()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn consume_relationship_yield_if_released(
        &mut self,
        relationship: &RelationshipId,
        current_time_ns: i64,
    ) -> Result<bool> {
        let transaction = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let row: Option<(i64, String)> = transaction
            .query_row(
                "SELECT after_completion_sequence,retry_not_before_ns
                 FROM relationship_yields WHERE relationship_id=?1",
                [&relationship.0],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((after_sequence, retry_not_before)) = row else {
            transaction.commit()?;
            return Ok(true);
        };
        let completion_sequence: i64 = transaction.query_row(
            "SELECT COALESCE((SELECT scheduler_completion_sequence
                              FROM installation WHERE singleton=1),0)",
            [],
            |row| row.get(0),
        )?;
        let retry_not_before = retry_not_before
            .parse::<i64>()
            .context("stored relationship retry time is invalid")?;
        let released = completion_sequence > after_sequence || current_time_ns >= retry_not_before;
        if released {
            transaction.execute(
                "DELETE FROM relationship_yields WHERE relationship_id=?1",
                [&relationship.0],
            )?;
        }
        transaction.commit()?;
        Ok(released)
    }

    pub fn enable_and_enqueue_managed_sync(
        &mut self,
        share: &ShareId,
        expected_generation: i64,
    ) -> Result<QueueRequest> {
        let _publication = self.lock_scheduler_publication()?;
        self.cleanup_stale_sync_queue()?;
        self.cleanup_scheduler_orphans()?;
        self.ensure_scheduler_capacity()?;
        self.peer_id()?;
        let token = format!("request-{}", ShareId::generate().0);
        let mut owner = None;
        let transaction = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let result = (|| -> Result<(i64, i64)> {
            let count: i64 =
                transaction.query_row("SELECT COUNT(*) FROM sync_queue", [], |row| row.get(0))?;
            if count >= MAX_SYNC_QUEUE_ROWS {
                bail!("synchronization queue is full");
            }
            let changed = transaction.execute(
                "UPDATE shares SET watch_enabled=1,intent_generation=intent_generation+1
                 WHERE share_id=?1 AND intent_generation=?2
                       AND initial_complete=1 AND removing_relationship IS NULL",
                params![share.0, expected_generation],
            )?;
            if changed == 0 {
                bail!("sync was stopped, removed, or reconfigured while it was being enabled");
            }
            let generation = expected_generation
                .checked_add(1)
                .context("watch intent generation overflow")?;
            owner = Some(create_scheduler_owner(&self.dir, &token)?);
            transaction.execute(
                "INSERT INTO sync_queue(token,share_id,operation,generation)
                 VALUES(?1,?2,'watch',?3)",
                params![token, share.0, generation],
            )?;
            Ok((transaction.last_insert_rowid(), generation))
        })();
        let (ticket, generation) = match result {
            Ok(result) => {
                if let Err(error) = transaction.commit() {
                    drop(owner.take());
                    let _ = remove_scheduler_owner(&self.dir, &token);
                    return Err(error.into());
                }
                result
            }
            Err(error) => {
                drop(transaction);
                drop(owner.take());
                let _ = fs::remove_file(self.dir.join("scheduler").join(&token));
                return Err(error);
            }
        };
        Ok(QueueRequest {
            state_dir: self.dir.clone(),
            ticket,
            token,
            share: Some(share.clone()),
            generation: Some(generation),
            owner,
        })
    }

    pub fn stop_and_cancel_managed_sync(&mut self, share: &ShareId) -> Result<i64> {
        let transaction = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let generation: i64 = transaction
            .query_row(
                "SELECT intent_generation FROM shares
                 WHERE share_id=?1 AND removing_relationship IS NULL",
                [&share.0],
                |row| row.get(0),
            )
            .optional()?
            .context("share not found or relationship removal is pending")?;
        let next_generation = generation
            .checked_add(1)
            .context("watch intent generation overflow")?;
        transaction.execute(
            "UPDATE shares SET watch_enabled=0,intent_generation=?2 WHERE share_id=?1",
            params![share.0, next_generation],
        )?;
        transaction.execute(
            "DELETE FROM sync_queue
             WHERE share_id=?1 AND operation='watch' AND active=0 AND generation<=?2",
            params![share.0, generation],
        )?;
        transaction.commit()?;
        Ok(next_generation)
    }

    fn ensure_scheduler_capacity(&self) -> Result<()> {
        let count = fs::read_dir(self.dir.join("scheduler"))?
            .take(MAX_SCHEDULER_DIRECTORY_ENTRIES + 1)
            .count();
        if count >= MAX_SCHEDULER_DIRECTORY_ENTRIES {
            bail!("scheduler ownership directory is full");
        }
        Ok(())
    }

    fn cleanup_stale_sync_queue(&mut self) -> Result<()> {
        self.peer_id()?;
        let mut tokens = Vec::with_capacity(STALE_QUEUE_CLEANUP_LIMIT + 2);
        let mut seen = HashSet::new();
        let active: Option<String> = self
            .conn
            .query_row("SELECT token FROM sync_queue WHERE active=1", [], |row| {
                row.get(0)
            })
            .optional()?;
        if let Some(token) = active
            && seen.insert(token.clone())
        {
            tokens.push(token);
        }
        let eligible_head = eligible_head_token(&self.conn)?;
        if let Some(token) = eligible_head
            && seen.insert(token.clone())
        {
            tokens.push(token);
        }
        let candidates = {
            let transaction = self
                .conn
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let cursor: i64 = transaction.query_row(
                "SELECT scheduler_cleanup_cursor FROM installation WHERE singleton=1",
                [],
                |row| row.get(0),
            )?;
            let rows = {
                let mut statement = transaction.prepare(
                    "SELECT ticket,token FROM sync_queue
                     ORDER BY CASE WHEN ticket>?1 THEN 0 ELSE 1 END,ticket
                     LIMIT ?2",
                )?;
                statement
                    .query_map(params![cursor, STALE_QUEUE_CLEANUP_LIMIT as i64], |row| {
                        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                    })?
                    .collect::<std::result::Result<Vec<_>, _>>()?
            };
            let next_cursor = rows.last().map(|(ticket, _)| *ticket).unwrap_or(0);
            transaction.execute(
                "UPDATE installation SET scheduler_cleanup_cursor=?1 WHERE singleton=1",
                [next_cursor],
            )?;
            transaction.commit()?;
            rows
        };
        for (_, token) in candidates {
            if seen.insert(token.clone()) {
                tokens.push(token);
            }
        }
        for token in tokens {
            validate_scheduler_token(&token)?;
            let owner = match open_scheduler_owner(&self.dir, &token) {
                Ok(owner) => owner,
                Err(error)
                    if error
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
                {
                    let transaction = self
                        .conn
                        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                    transaction.execute("DELETE FROM sync_queue WHERE token=?1", [&token])?;
                    transaction.commit()?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            match owner.try_lock_exclusive() {
                Ok(()) => {
                    let transaction = self
                        .conn
                        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                    let changed =
                        transaction.execute("DELETE FROM sync_queue WHERE token=?1", [&token])?;
                    transaction.commit()?;
                    if changed != 0 {
                        fs::remove_file(self.dir.join("scheduler").join(&token))?;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    fn cleanup_scheduler_orphans(&mut self) -> Result<()> {
        let entries = fs::read_dir(self.dir.join("scheduler"))?
            .take(MAX_SCHEDULER_DIRECTORY_ENTRIES)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        for entry in entries {
            let token = entry
                .file_name()
                .into_string()
                .map_err(|_| anyhow::anyhow!("scheduler contains a non-UTF-8 entry"))?;
            validate_scheduler_token(&token)?;
            let owner = open_scheduler_owner(&self.dir, &token)?;
            match owner.try_lock_exclusive() {
                Ok(()) => {
                    let exists = self.conn.query_row(
                        "SELECT EXISTS(SELECT 1 FROM sync_queue WHERE token=?1)",
                        [&token],
                        |row| row.get::<_, i64>(0),
                    )? != 0;
                    if !exists {
                        fs::remove_file(entry.path())?;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    #[cfg(feature = "e2e-test-hooks")]
    fn observe_e2e_global_contention(&self) -> Result<()> {
        let marker = self.dir.join(".e2e-observe-global-contention");
        let observed = self.dir.join(".e2e-global-contention-observed");
        match fs::rename(&marker, &observed) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error).context("publishing E2E global contention"),
        }
        let metadata = fs::symlink_metadata(&observed)?;
        if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() != 0 {
            bail!("E2E global contention marker is not an empty regular file");
        }
        Ok(())
    }

    pub fn lock_objects(&self) -> Result<File> {
        let path = self.dir.join("objects.lock");
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        set_private_file(&path)?;
        file.lock_exclusive()?;
        Ok(file)
    }

    fn lock_registration(&self) -> Result<File> {
        let path = self.dir.join("registration.lock");
        if let Ok(metadata) = fs::symlink_metadata(&path)
            && (!metadata.file_type().is_file() || metadata.file_type().is_symlink())
        {
            bail!("registration lock is not a regular file");
        }
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        set_private_file(&path)?;
        file.try_lock_exclusive()
            .context("another share registration is in progress")?;
        Ok(file)
    }

    pub fn init_share(&self, root: &Path) -> Result<ShareId> {
        let _registration_lock = self.lock_registration()?;
        let metadata = fs::symlink_metadata(root)
            .with_context(|| format!("cannot inspect {}", root.display()))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            bail!("sync root must be an existing directory, not a symbolic link");
        }
        let root = root
            .canonicalize()
            .with_context(|| format!("cannot open {}", root.display()))?;
        let root_bytes = path_bytes(&root);
        let identity = root_identity(&root)?;
        if self
            .conn
            .query_row("SELECT 1 FROM shares WHERE root=?1", [&root_bytes], |_| {
                Ok(())
            })
            .optional()?
            .is_some()
        {
            bail!("directory is already registered");
        }
        self.reject_overlapping_root(&root)?;
        let id = ShareId::generate();
        self.conn.execute(
            "INSERT INTO shares(share_id, root, root_device, root_inode) VALUES(?1, ?2, ?3, ?4)",
            params![
                id.0,
                root_bytes,
                identity.device.to_string(),
                identity.inode.to_string()
            ],
        )?;
        Ok(id)
    }

    pub fn register_share(&self, id: &ShareId, root: &Path) -> Result<()> {
        let _registration_lock = self.lock_registration()?;
        fs::create_dir_all(root)?;
        let root = root.canonicalize()?;
        let root_bytes = path_bytes(&root);
        let identity = root_identity(&root)?;
        let existing: Option<(Vec<u8>, String, String)> = self
            .conn
            .query_row(
                "SELECT root, root_device, root_inode FROM shares WHERE share_id=?1",
                [&id.0],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        if let Some((path, device, inode)) = existing {
            if path != root_bytes {
                bail!("share ID is already registered to a different directory");
            }
            validate_identity_values(&root, identity, &device, &inode)?;
            return Ok(());
        }
        self.reject_overlapping_root(&root)?;
        self.conn.execute(
            "INSERT INTO shares(share_id, root, root_device, root_inode) VALUES(?1, ?2, ?3, ?4)",
            params![
                id.0,
                root_bytes,
                identity.device.to_string(),
                identity.inode.to_string()
            ],
        )?;
        Ok(())
    }

    pub fn register_relationship(
        &mut self,
        id: &ShareId,
        root: &Path,
        peer: &PeerId,
        relationship: &RelationshipId,
    ) -> Result<RegistrationOutcome> {
        self.scheduled_mutation(None, SyncOperation::Registration, |state| {
            state.register_relationship_locked(id, root, peer, relationship)
        })
    }

    pub fn register_relationship_locked(
        &mut self,
        id: &ShareId,
        root: &Path,
        peer: &PeerId,
        relationship: &RelationshipId,
    ) -> Result<RegistrationOutcome> {
        relationship.validate()?;
        let requested_root_bytes = path_bytes(root);
        if requested_root_bytes.contains(&0)
            || requested_root_bytes.len() > crate::sync::MAX_RELATIONSHIP_ROOT_BYTES
        {
            bail!("relationship root exceeds its safe wire bound or contains NUL");
        }
        if !root.is_absolute() {
            bail!("relationship root must be absolute");
        }
        let _registration_lock = self.lock_registration()?;
        let resolved = resolve_registration_path(root)?;
        let retained_before_create: Option<String> = self
            .conn
            .query_row(
                "SELECT share_id FROM shares WHERE root=?1",
                [path_bytes(&resolved.canonical)],
                |row| row.get(0),
            )
            .optional()?;
        let root_file = if resolved.missing.is_empty() {
            let metadata = fs::symlink_metadata(root)?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                bail!("relationship root must be a directory, not a symbolic link");
            }
            resolved.ancestor
        } else {
            match fs::symlink_metadata(root) {
                Ok(_) => bail!("relationship root changed while preparing registration"),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            if retained_before_create.is_some() {
                bail!("retained relationship root is missing and will not be recreated")
            }
            create_registration_tail(resolved.ancestor, &resolved.missing)?
        };
        let requested_path = root.to_path_buf();
        let identity = file_root_identity(&root_file, &resolved.canonical)?;
        if root_identity(root)? != identity {
            bail!("relationship root identity changed while preparing registration");
        }
        let root = canonical_registration_root(root, identity)?;
        if root != resolved.canonical {
            bail!("relationship root path changed while preparing registration");
        }
        let root_bytes = path_bytes(&root);
        let exact_root_share: Option<ShareId> = self
            .conn
            .query_row(
                "SELECT share_id FROM shares WHERE root=?1",
                [&root_bytes],
                |row| Ok(ShareId(row.get(0)?)),
            )
            .optional()?;
        let requested_root: Option<Vec<u8>> = self
            .conn
            .query_row(
                "SELECT root FROM shares WHERE share_id=?1",
                [&id.0],
                |row| row.get(0),
            )
            .optional()?;
        if requested_root
            .as_ref()
            .is_some_and(|stored| stored != &root_bytes)
        {
            bail!("share ID is already registered to a different directory");
        }
        self.reject_overlapping_root_except(&root, exact_root_share.as_ref())?;
        ensure_relationship_available(&self.conn, id, relationship)?;

        let transaction = self.conn.transaction()?;
        let outcome = match exact_root_share {
            Some(existing_id) if existing_id == *id => {
                let (binding, marker) = binding_and_marker(&transaction, id)?
                    .context("share disappeared during registration")?;
                if marker.is_some() {
                    bail!("relationship removal is pending");
                }
                validate_stored_root_identity(&transaction, id, &root, identity)?;
                match binding {
                    EndpointBinding::Responder {
                        peer: bound,
                        relationship: Some(bound_relationship),
                    } if bound == *peer && bound_relationship == *relationship => {
                        RegistrationOutcome { prior_share: None }
                    }
                    EndpointBinding::Unpaired => {
                        ensure_unpaired_registration_state(&transaction, id)?;
                        transaction.execute(
                            "UPDATE shares SET bound_peer=?2, bound_relationship=?3
                             WHERE share_id=?1",
                            params![id.0, peer.0, relationship.0],
                        )?;
                        RegistrationOutcome { prior_share: None }
                    }
                    _ => bail!("share is already bound to a different relationship"),
                }
            }
            Some(prior_share) => {
                let (binding, marker) = binding_and_marker(&transaction, &prior_share)?
                    .context("retained root disappeared during registration")?;
                if marker.is_some() || !matches!(binding, EndpointBinding::Unpaired) {
                    bail!("matching root is paired or pending removal");
                }
                ensure_unpaired_registration_state(&transaction, &prior_share)?;
                validate_stored_root_identity(&transaction, &prior_share, &root, identity)?;
                transaction.execute(
                    "UPDATE shares SET share_id=?2, bound_peer=?3, bound_relationship=?4
                     WHERE share_id=?1",
                    params![prior_share.0, id.0, peer.0, relationship.0],
                )?;
                transaction.execute(
                    "UPDATE conflicts SET share_id=?2 WHERE share_id=?1",
                    params![prior_share.0, id.0],
                )?;
                RegistrationOutcome {
                    prior_share: Some(prior_share),
                }
            }
            None => {
                transaction.execute(
                    "INSERT INTO shares(
                         share_id,root,bound_peer,bound_relationship,root_device,root_inode
                     ) VALUES(?1,?2,?3,?4,?5,?6)",
                    params![
                        id.0,
                        root_bytes,
                        peer.0,
                        relationship.0,
                        identity.device.to_string(),
                        identity.inode.to_string()
                    ],
                )?;
                RegistrationOutcome { prior_share: None }
            }
        };
        if root_identity(&requested_path)? != identity || root_identity(&root)? != identity {
            bail!("relationship root identity changed during registration");
        }
        transaction.commit()?;
        Ok(outcome)
    }

    pub fn acknowledge_legacy_registration(
        &mut self,
        id: &ShareId,
        root: &Path,
        peer: &PeerId,
    ) -> Result<()> {
        self.scheduled_mutation(Some(id), SyncOperation::Registration, |state| {
            state.acknowledge_legacy_registration_locked(id, root, peer)
        })
    }

    fn acknowledge_legacy_registration_locked(
        &mut self,
        id: &ShareId,
        root: &Path,
        peer: &PeerId,
    ) -> Result<()> {
        let _registration_lock = self.lock_registration()?;
        let metadata = fs::symlink_metadata(root)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            bail!("legacy relationship root must be an existing directory");
        }
        let root = root.canonicalize()?;
        let root_bytes = path_bytes(&root);
        let identity = root_identity(&root)?;
        let (binding, marker) = binding_and_marker(&self.conn, id)?
            .context("legacy registration cannot create a relationship")?;
        if marker.is_some() {
            bail!("relationship removal is pending");
        }
        let stored: Vec<u8> = self.conn.query_row(
            "SELECT root FROM shares WHERE share_id=?1",
            [&id.0],
            |row| row.get(0),
        )?;
        if stored != root_bytes {
            bail!("share ID is registered to a different directory");
        }
        validate_stored_root_identity(&self.conn, id, &root, identity)?;
        match binding {
            EndpointBinding::Responder {
                peer: bound,
                relationship: None,
            } if bound == *peer => Ok(()),
            _ => bail!("legacy registration is not an exact existing legacy binding"),
        }
    }

    pub fn find_share(&self, path: &Path) -> Result<(ShareId, PathBuf)> {
        let path = path.canonicalize()?;
        let mut stmt = self.conn.prepare("SELECT share_id, root FROM shares")?;
        let shares = stmt.query_map([], |row| {
            Ok((ShareId(row.get(0)?), bytes_path(row.get::<_, Vec<u8>>(1)?)))
        })?;
        let mut best = None;
        for share in shares {
            let (id, root) = share?;
            if path.starts_with(&root)
                && best.as_ref().is_none_or(|(_, prior): &(ShareId, PathBuf)| {
                    root.components().count() > prior.components().count()
                })
            {
                best = Some((id, root));
            }
        }
        best.context("path is not inside an initialized share")
    }

    pub fn find_share_by_exact_root(&self, path: &Path) -> Result<(ShareId, PathBuf)> {
        let requested = std::path::absolute(path)?;
        let identity = root_identity(&requested)?;
        let root = canonical_registration_root(&requested, identity)?;
        let share = self.find_share_by_exact_root_identity(&root, identity)?;
        if root_identity(&requested)? != identity {
            bail!("relationship root identity changed while selecting it for removal");
        }
        Ok(share)
    }

    fn find_share_by_exact_root_identity(
        &self,
        root: &Path,
        identity: RootIdentity,
    ) -> Result<(ShareId, PathBuf)> {
        let row: Option<(String, String, String)> = self
            .conn
            .query_row(
                "SELECT share_id,root_device,root_inode FROM shares WHERE root=?1",
                [path_bytes(root)],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let (share, device, inode) = row.context(
            "removal PATH must name the configured sync root; use --share when it is unavailable",
        )?;
        validate_identity_values(root, identity, &device, &inode)?;
        Ok((ShareId(share), root.to_path_buf()))
    }

    pub fn root_for(&self, id: &ShareId) -> Result<PathBuf> {
        let bytes: Vec<u8> =
            self.conn
                .query_row("SELECT root FROM shares WHERE share_id=?1", [&id.0], |r| {
                    r.get(0)
                })?;
        Ok(bytes_path(bytes))
    }

    pub fn expected_root_identity(&self, id: &ShareId) -> Result<RootIdentity> {
        let (device, inode): (String, String) = self.conn.query_row(
            "SELECT root_device, root_inode FROM shares WHERE share_id=?1",
            [&id.0],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        Ok(RootIdentity {
            device: device.parse().map_err(|error| {
                RootIdentityChanged::new(format!("stored root device is invalid: {error}"))
            })?,
            inode: inode.parse().map_err(|error| {
                RootIdentityChanged::new(format!("stored root inode is invalid: {error}"))
            })?,
        })
    }

    pub fn validate_root_identity(&self, id: &ShareId) -> Result<RootIdentity> {
        let root = self.root_for(id)?;
        let expected = self.expected_root_identity(id)?;
        let actual = root_identity(&root).map_err(|error| {
            RootIdentityChanged::new(format!(
                "configured root {} is unavailable: {error}; restore the original directory before retrying",
                root.display()
            ))
        })?;
        if actual != expected {
            return Err(RootIdentityChanged::new(format!(
                "configured root identity changed at {}; restore the original directory or deliberately remove and reinitialize the share",
                root.display()
            ))
            .into());
        }
        Ok(actual)
    }

    pub fn next_sequence(&self, id: &ShareId) -> Result<u64> {
        self.conn.execute(
            "UPDATE shares SET sequence=sequence+1 WHERE share_id=?1",
            [&id.0],
        )?;
        let value: i64 = self.conn.query_row(
            "SELECT sequence FROM shares WHERE share_id=?1",
            [&id.0],
            |r| r.get(0),
        )?;
        Ok(value as u64)
    }

    pub fn records(&self, id: &ShareId) -> Result<Vec<Record>> {
        let mut stmt = self
            .conn
            .prepare("SELECT path, version_json FROM records WHERE share_id=?1 ORDER BY path")?;
        let rows = stmt.query_map([&id.0], |r| {
            let path: Vec<u8> = r.get(0)?;
            let json: String = r.get(1)?;
            Ok((path, json))
        })?;
        rows.map(|row| {
            let (path, json) = row?;
            Ok(Record {
                path: RelativePath::from_bytes(path)?,
                version: serde_json::from_str(&json)?,
            })
        })
        .collect()
    }

    pub fn shared_heads(
        &self,
        share: &ShareId,
    ) -> Result<std::collections::HashMap<Vec<u8>, crate::model::BaseVersion>> {
        let mut statement = self
            .conn
            .prepare("SELECT path, base_json FROM shared_heads WHERE share_id=?1 ORDER BY path")?;
        statement
            .query_map([&share.0], |row| {
                Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, String>(1)?))
            })?
            .map(|row| {
                let (path, base) = row?;
                Ok((path, serde_json::from_str(&base)?))
            })
            .collect()
    }

    /// Marks only plan records that exactly match this endpoint's durable
    /// current record. Calling this after the peer's commit acknowledgement
    /// makes one-sided failures conservative without inventing a shared base.
    pub fn acknowledge_shared_heads(&mut self, share: &ShareId, plan: &[Record]) -> Result<()> {
        let current: std::collections::HashMap<_, _> = self
            .records(share)?
            .into_iter()
            .map(|record| (record.path.as_bytes().to_vec(), record))
            .collect();
        let tx = self.conn.transaction()?;
        tx.execute("DELETE FROM shared_heads WHERE share_id=?1", [&share.0])?;
        for record in plan {
            if current.get(record.path.as_bytes()) != Some(record) {
                continue;
            }
            let Some(base) = record.version.as_base() else {
                continue;
            };
            tx.execute(
                "INSERT INTO shared_heads(share_id,path,base_json) VALUES(?1,?2,?3)",
                params![
                    share.0,
                    record.path.as_bytes(),
                    serde_json::to_string(&base)?
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn replace_records(&mut self, id: &ShareId, records: &[Record]) -> Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute("DELETE FROM records WHERE share_id=?1", [&id.0])?;
        for record in records {
            tx.execute(
                "INSERT INTO records(share_id,path,version_json) VALUES(?1,?2,?3)",
                params![
                    id.0,
                    record.path.as_bytes(),
                    serde_json::to_string(&record.version)?
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn endpoint_binding(&self, id: &ShareId) -> Result<EndpointBinding> {
        binding_and_marker(&self.conn, id)?
            .map(|(binding, _)| binding)
            .context("share not found")
    }

    pub fn removing_relationship(&self, id: &ShareId) -> Result<Option<RelationshipId>> {
        binding_and_marker(&self.conn, id)?
            .map(|(_, marker)| marker)
            .context("share not found")
    }

    pub fn ensure_not_removing(&self, id: &ShareId) -> Result<()> {
        if self.removing_relationship(id)?.is_some() {
            bail!(
                "relationship removal is pending; rerun `flocal sync remove --share {}`",
                id.0
            );
        }
        Ok(())
    }

    pub fn prepare_connector_registration(
        &mut self,
        id: &ShareId,
        expected: &EndpointBinding,
        host: &str,
        remote_path: &[u8],
        executable: &str,
    ) -> Result<PeerConfig> {
        self.scheduled_mutation(Some(id), SyncOperation::Registration, |state| {
            state.prepare_connector_registration_locked(id, expected, host, remote_path, executable)
        })
    }

    pub fn prepare_connector_registration_locked(
        &mut self,
        id: &ShareId,
        expected: &EndpointBinding,
        host: &str,
        remote_path: &[u8],
        executable: &str,
    ) -> Result<PeerConfig> {
        let _registration_lock = self.lock_registration()?;
        self.validate_root_identity(id)?;
        let transaction = self.conn.transaction()?;
        let (binding, marker) = binding_and_marker(&transaction, id)?.context("share not found")?;
        if marker.is_some() {
            bail!("relationship removal is pending");
        }
        if let EndpointBinding::Connector(existing) = &binding {
            if existing.relationship.is_some()
                && existing.host == host
                && existing.remote_path == remote_path
            {
                return Ok(existing.clone());
            }
            bail!("share already has a connector configuration");
        }
        if &binding != expected || !matches!(binding, EndpointBinding::Unpaired) {
            bail!("share relationship changed since pairing preview");
        }
        let config = PeerConfig {
            peer_id: None,
            host: host.to_owned(),
            remote_path: remote_path.to_vec(),
            executable: executable.to_owned(),
            relationship: Some(RelationshipId::generate()),
        };
        config.validate()?;
        transaction.execute(
            "UPDATE shares SET peer_json=?2, watch_enabled=0,
             intent_generation=intent_generation+1 WHERE share_id=?1",
            params![id.0, serde_json::to_string(&config)?],
        )?;
        transaction.commit()?;
        Ok(config)
    }

    pub fn complete_connector_registration(
        &mut self,
        id: &ShareId,
        prepared: &PeerConfig,
        peer: &PeerId,
    ) -> Result<PeerConfig> {
        prepared.validate()?;
        if prepared.relationship.is_none() {
            bail!("connector registration completion requires a relationship identity");
        }
        self.scheduled_mutation(Some(id), SyncOperation::Registration, |state| {
            state.complete_connector_registration_locked(id, prepared, peer)
        })
    }

    pub fn complete_connector_registration_locked(
        &mut self,
        id: &ShareId,
        prepared: &PeerConfig,
        peer: &PeerId,
    ) -> Result<PeerConfig> {
        prepared.validate()?;
        if prepared.relationship.is_none() {
            bail!("connector registration completion requires a relationship identity");
        }
        let _registration_lock = self.lock_registration()?;
        let transaction = self.conn.transaction()?;
        let (binding, marker) = binding_and_marker(&transaction, id)?.context("share not found")?;
        if marker.is_some() {
            bail!("relationship removal is pending");
        }
        let completed = if let Some(expected_peer) = &prepared.peer_id {
            if expected_peer != peer {
                bail!("connector registration response changed peer identity");
            }
            prepared.clone()
        } else {
            PeerConfig {
                peer_id: Some(peer.clone()),
                ..prepared.clone()
            }
        };
        match binding {
            EndpointBinding::Connector(current)
                if current == *prepared && prepared.peer_id.is_none() =>
            {
                let changed = transaction.execute(
                    "UPDATE shares SET peer_json=?2 WHERE share_id=?1 AND peer_json=?3",
                    params![
                        id.0,
                        serde_json::to_string(&completed)?,
                        serde_json::to_string(prepared)?
                    ],
                )?;
                if changed != 1 {
                    bail!("connector registration changed during completion");
                }
                transaction.commit()?;
                Ok(completed)
            }
            EndpointBinding::Connector(current) if current == completed => Ok(completed),
            _ => bail!("connector registration changed before completion"),
        }
    }

    pub fn prepare_removal(
        &mut self,
        id: &ShareId,
        expected: &EndpointBinding,
    ) -> Result<PreparedRemoval> {
        let transaction = self.conn.transaction()?;
        let prepared = prepare_removal_transaction(&transaction, id, expected, None)?;
        transaction.commit()?;
        Ok(prepared)
    }

    pub fn prepare_incoming_removal(
        &mut self,
        id: &ShareId,
        peer: &PeerId,
        relationship: &RelationshipId,
    ) -> Result<IncomingRemoval> {
        self.scheduled_mutation(None, SyncOperation::Removal, |state| {
            state.prepare_incoming_removal_locked(id, peer, relationship)
        })
    }

    pub fn prepare_incoming_removal_locked(
        &mut self,
        id: &ShareId,
        peer: &PeerId,
        relationship: &RelationshipId,
    ) -> Result<IncomingRemoval> {
        relationship.validate()?;
        let Some((binding, marker)) = binding_and_marker(&self.conn, id)? else {
            return Ok(IncomingRemoval::Absent);
        };
        if marker.as_ref().is_some_and(|marker| marker != relationship) {
            return Ok(IncomingRemoval::Absent);
        }
        match &binding {
            EndpointBinding::Responder {
                peer: bound,
                relationship: Some(bound_relationship),
            } if bound_relationship != relationship => return Ok(IncomingRemoval::Absent),
            EndpointBinding::Responder {
                peer: bound,
                relationship: Some(_),
            } if bound != peer => bail!("relationship binding does not match requester"),
            EndpointBinding::Responder {
                peer: bound,
                relationship: None,
            } if bound != peer => bail!("legacy relationship binding does not match requester"),
            EndpointBinding::Responder { .. } => {}
            EndpointBinding::Connector(config)
                if config.relationship.as_ref() == Some(relationship) =>
            {
                bail!("relationship identity is bound in the wrong role")
            }
            EndpointBinding::Connector(_) | EndpointBinding::Unpaired => {
                return Ok(IncomingRemoval::Absent);
            }
        }
        let transaction = self.conn.transaction()?;
        let prepared = prepare_removal_transaction(&transaction, id, &binding, Some(relationship))?;
        transaction.commit()?;
        Ok(IncomingRemoval::Prepared(prepared))
    }

    pub fn set_removal_diagnostic(
        &self,
        id: &ShareId,
        relationship: &RelationshipId,
        diagnostic: &str,
    ) -> Result<()> {
        relationship.validate()?;
        let diagnostic = bounded_diagnostic(diagnostic);
        let changed = self.conn.execute(
            "UPDATE shares SET blocked_diagnostic=?3
             WHERE share_id=?1 AND removing_relationship=?2",
            params![id.0, relationship.0, diagnostic],
        )?;
        if changed != 1 {
            bail!("relationship removal changed before storing its diagnostic");
        }
        Ok(())
    }

    pub fn record_removal_failure(
        &mut self,
        prepared: &PreparedRemoval,
        diagnostic: &str,
    ) -> Result<RemovalFailureState> {
        self.scheduled_mutation(Some(&prepared.share), SyncOperation::Removal, |state| {
            state.record_removal_failure_locked(prepared, diagnostic)
        })
    }

    pub fn record_removal_failure_locked(
        &self,
        prepared: &PreparedRemoval,
        diagnostic: &str,
    ) -> Result<RemovalFailureState> {
        prepared.relationship.validate()?;
        let (binding, marker) =
            binding_and_marker(&self.conn, &prepared.share)?.context("share not found")?;
        if matches!((&binding, &marker), (EndpointBinding::Unpaired, None)) {
            return Ok(RemovalFailureState::Finalized);
        }
        if binding != prepared.binding || marker.as_ref() != Some(&prepared.relationship) {
            return Ok(RemovalFailureState::Changed);
        }
        let changed = self.conn.execute(
            "UPDATE shares SET blocked_diagnostic=?3
             WHERE share_id=?1 AND removing_relationship=?2",
            params![
                prepared.share.0,
                prepared.relationship.0,
                bounded_diagnostic(diagnostic)
            ],
        )?;
        if changed != 1 {
            bail!("relationship changed before recording its removal failure");
        }
        Ok(RemovalFailureState::Pending)
    }

    pub fn finalize_connector_removal(&mut self, prepared: &PreparedRemoval) -> Result<Detached> {
        self.scheduled_mutation(Some(&prepared.share), SyncOperation::Removal, |state| {
            state.finalize_connector_removal_locked(prepared)
        })
    }

    pub fn finalize_connector_removal_locked(
        &mut self,
        prepared: &PreparedRemoval,
    ) -> Result<Detached> {
        match &prepared.binding {
            EndpointBinding::Connector(config) if config.peer_id.is_some() => {}
            EndpointBinding::Connector(_) => {
                bail!("incomplete registration requires local-only removal")
            }
            _ => bail!("connector removal requires a connector binding"),
        }
        self.finalize_removal_locked(prepared, RemovalRole::Connector)
    }

    pub fn finalize_local_removal(&mut self, prepared: &PreparedRemoval) -> Result<Detached> {
        self.scheduled_mutation(Some(&prepared.share), SyncOperation::Removal, |state| {
            state.finalize_local_removal_locked(prepared)
        })
    }

    pub fn finalize_local_removal_locked(
        &mut self,
        prepared: &PreparedRemoval,
    ) -> Result<Detached> {
        if matches!(prepared.binding, EndpointBinding::Unpaired) {
            bail!("no relationship is configured");
        }
        self.finalize_removal_locked(prepared, RemovalRole::Either)
    }

    pub fn detach_incoming_relationship(&mut self, prepared: &PreparedRemoval) -> Result<Detached> {
        self.scheduled_mutation(Some(&prepared.share), SyncOperation::Removal, |state| {
            state.detach_incoming_relationship_locked(prepared)
        })
    }

    pub fn detach_incoming_relationship_locked(
        &mut self,
        prepared: &PreparedRemoval,
    ) -> Result<Detached> {
        if !matches!(prepared.binding, EndpointBinding::Responder { .. }) {
            bail!("incoming removal requires a responder binding");
        }
        self.finalize_removal_locked(prepared, RemovalRole::Responder)
    }

    fn finalize_removal_locked(
        &mut self,
        prepared: &PreparedRemoval,
        role: RemovalRole,
    ) -> Result<Detached> {
        prepared.relationship.validate()?;
        let transaction = self.conn.transaction()?;
        let (binding, marker) =
            binding_and_marker(&transaction, &prepared.share)?.context("share not found")?;
        if !matches!((&binding, &marker), (EndpointBinding::Unpaired, None)) {
            if binding != prepared.binding
                || marker.as_ref() != Some(&prepared.relationship)
                || !role.accepts(&binding)
            {
                bail!("relationship changed before removal finalization");
            }
            let installs: i64 = transaction.query_row(
                "SELECT COUNT(*) FROM install_intents WHERE share_id=?1",
                [&prepared.share.0],
                |row| row.get(0),
            )?;
            if installs != 0 {
                bail!("interrupted install must be recovered before relationship removal");
            }
            match binding {
                EndpointBinding::Connector(_) => {
                    transaction.execute(
                        "UPDATE shares SET peer_json=NULL WHERE share_id=?1",
                        [&prepared.share.0],
                    )?;
                }
                EndpointBinding::Responder { .. } => {
                    transaction.execute(
                        "UPDATE shares SET bound_peer=NULL, bound_relationship=NULL WHERE share_id=?1",
                        [&prepared.share.0],
                    )?;
                }
                EndpointBinding::Unpaired => unreachable!("validated above"),
            }
            transaction.execute(
                "UPDATE shares SET removing_relationship=NULL, blocked_diagnostic=NULL,
                 initial_complete=0, watch_enabled=0,
                 intent_generation=intent_generation+1 WHERE share_id=?1",
                [&prepared.share.0],
            )?;
            for table in [
                "records",
                "shared_heads",
                "unsettled_paths",
                "pending_objects",
            ] {
                transaction.execute(
                    &format!("DELETE FROM {table} WHERE share_id=?1"),
                    [&prepared.share.0],
                )?;
            }
        }
        transaction.commit()?;
        let cleanup_warning = self
            .prune_unreferenced_objects()
            .err()
            .map(|error| bounded_diagnostic(&format!("{error:#}")));
        Ok(Detached { cleanup_warning })
    }

    pub fn set_peer(&mut self, id: &ShareId, peer: &PeerConfig) -> Result<()> {
        peer.validate()?;
        let transaction = self.conn.transaction()?;
        let (binding, marker) = binding_and_marker(&transaction, id)?.context("share not found")?;
        if marker.is_some() {
            bail!("relationship removal is pending");
        }
        match binding {
            EndpointBinding::Unpaired => {}
            EndpointBinding::Connector(existing) if existing == *peer => return Ok(()),
            EndpointBinding::Connector(_) => bail!("share already has a connector configuration"),
            EndpointBinding::Responder { .. } => {
                bail!("responder share cannot also have a connector configuration")
            }
        }
        let changed = transaction.execute(
            "UPDATE shares SET peer_json=?2
             WHERE share_id=?1 AND removing_relationship IS NULL",
            params![id.0, serde_json::to_string(peer)?],
        )?;
        if changed != 1 {
            bail!("share not found or relationship removal is pending");
        }
        transaction.commit()?;
        Ok(())
    }

    #[cfg(feature = "e2e-test-hooks")]
    pub fn e2e_make_relationship_legacy(&mut self, id: &ShareId) -> Result<()> {
        let transaction = self.conn.transaction()?;
        let (binding, marker) = binding_and_marker(&transaction, id)?.context("share not found")?;
        if marker.is_some() {
            bail!("relationship removal is pending");
        }
        match binding {
            EndpointBinding::Connector(mut peer)
                if peer.peer_id.is_some() && peer.relationship.is_some() =>
            {
                peer.relationship = None;
                let changed = transaction.execute(
                    "UPDATE shares SET peer_json=?2 WHERE share_id=?1",
                    params![id.0, serde_json::to_string(&peer)?],
                )?;
                anyhow::ensure!(changed == 1, "share disappeared while making it legacy");
            }
            EndpointBinding::Responder {
                relationship: Some(_),
                ..
            } => {
                let changed = transaction.execute(
                    "UPDATE shares SET bound_relationship=NULL WHERE share_id=?1",
                    [&id.0],
                )?;
                anyhow::ensure!(changed == 1, "share disappeared while making it legacy");
            }
            _ => bail!("relationship is not a completed current relationship"),
        }
        transaction.commit()?;
        Ok(())
    }

    #[cfg(feature = "e2e-test-hooks")]
    pub fn e2e_assert_relationship_legacy(&self, id: &ShareId) -> Result<()> {
        match self.endpoint_binding(id)? {
            EndpointBinding::Connector(PeerConfig {
                peer_id: Some(_),
                relationship: None,
                ..
            })
            | EndpointBinding::Responder {
                relationship: None, ..
            } => Ok(()),
            _ => bail!("relationship is not a completed legacy relationship"),
        }
    }

    pub fn bound_peer(&self, id: &ShareId) -> Result<Option<PeerId>> {
        match binding_and_marker(&self.conn, id)? {
            Some((EndpointBinding::Responder { peer, .. }, _)) => Ok(Some(peer)),
            Some((EndpointBinding::Connector(_) | EndpointBinding::Unpaired, _)) | None => Ok(None),
        }
    }

    pub fn peer(&self, id: &ShareId) -> Result<Option<PeerConfig>> {
        match binding_and_marker(&self.conn, id)?.context("share not found")? {
            (EndpointBinding::Connector(peer), _) => Ok(Some(peer)),
            (EndpointBinding::Responder { .. } | EndpointBinding::Unpaired, _) => Ok(None),
        }
    }

    pub fn set_initial_complete(&self, id: &ShareId) -> Result<()> {
        let changed = self.conn.execute(
            "UPDATE shares SET initial_complete=1
             WHERE share_id=?1 AND removing_relationship IS NULL",
            [&id.0],
        )?;
        if changed != 1 {
            bail!("share not found or relationship removal is pending");
        }
        Ok(())
    }

    pub fn add_conflicts(&mut self, share: &ShareId, conflicts: &[Conflict]) -> Result<()> {
        self.scheduled_mutation(Some(share), SyncOperation::Maintenance, |state| {
            state.add_conflicts_locked(share, conflicts)
        })
    }

    pub fn add_conflicts_locked(&mut self, share: &ShareId, conflicts: &[Conflict]) -> Result<()> {
        self.ensure_recovery_limits(share, conflicts)?;
        let transaction = self.conn.transaction()?;
        for conflict in conflicts {
            insert_conflict(&transaction, share, conflict)?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn recovery_budget(&self, share: &ShareId) -> Result<u64> {
        #[cfg(feature = "e2e-test-hooks")]
        {
            let marker = self.dir.join(".e2e-recovery-budget-bytes");
            if marker.exists() {
                let value = fs::read_to_string(marker)?;
                return value
                    .trim()
                    .parse::<u64>()
                    .context("invalid E2E recovery budget marker");
            }
        }
        let value: i64 = self.conn.query_row(
            "SELECT recovery_budget_bytes FROM shares WHERE share_id=?1",
            [&share.0],
            |row| row.get(0),
        )?;
        u64::try_from(value).context("recovery budget is negative")
    }

    fn recovery_conflict_limit(&self) -> Result<u64> {
        #[cfg(feature = "e2e-test-hooks")]
        if let Some(limit) = self.recovery_test_limit(".e2e-recovery-conflict-limit")? {
            return Ok(limit);
        }
        Ok(crate::sync::MAX_RECORDS_PER_SESSION as u64)
    }

    fn recovery_metadata_limit(&self) -> Result<u64> {
        #[cfg(feature = "e2e-test-hooks")]
        if let Some(limit) = self.recovery_test_limit(".e2e-recovery-metadata-limit")? {
            return Ok(limit);
        }
        Ok(crate::sync::MAX_METADATA_BYTES_PER_SESSION as u64)
    }

    #[cfg(feature = "e2e-test-hooks")]
    fn recovery_test_limit(&self, name: &str) -> Result<Option<u64>> {
        let marker = self.dir.join(name);
        if !marker.exists() {
            return Ok(None);
        }
        Ok(Some(
            fs::read_to_string(marker)?
                .trim()
                .parse::<u64>()
                .context("invalid E2E recovery limit marker")?,
        ))
    }

    pub fn raise_recovery_budget(&mut self, share: &ShareId, budget: u64) -> Result<u64> {
        let budget = i64::try_from(budget).context("recovery budget exceeds SQLite limit")?;
        let transaction = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let previous: i64 = transaction.query_row(
            "SELECT recovery_budget_bytes FROM shares WHERE share_id=?1",
            [&share.0],
            |row| row.get(0),
        )?;
        if budget <= previous {
            bail!("new recovery budget must be greater than the current {previous} bytes");
        }
        let changed = transaction.execute(
            "UPDATE shares SET recovery_budget_bytes=?2 WHERE share_id=?1",
            params![share.0, budget],
        )?;
        if changed != 1 {
            bail!("share not found");
        }
        transaction.commit()?;
        u64::try_from(previous).context("recovery budget is negative")
    }

    pub fn recovery_usage(&self, share: &ShareId) -> Result<RecoveryUsage> {
        let _object_lock = self.lock_objects()?;
        let transaction = self.conn.unchecked_transaction()?;
        self.prepare_recovery_objects(share, &[])?;
        let result = (|| {
            let (conflicts, metadata_bytes) = recovery_row_totals(&self.conn, share)?;
            let object_bytes = temp_object_bytes(&self.conn, "recovery_objects")?;
            let budget_bytes = self.recovery_budget(share)?;
            let reclaimable_bytes = self.recovery_reclaimable_all(share)?;
            let used_bytes = object_bytes
                .checked_add(metadata_bytes)
                .context("recovery usage exceeds supported byte range")?;
            let conflict_limit = self.recovery_conflict_limit()?;
            let metadata_limit_bytes = self.recovery_metadata_limit()?;
            Ok(RecoveryUsage {
                conflicts,
                conflict_limit,
                conflicts_remaining: conflict_limit.saturating_sub(conflicts),
                object_bytes,
                metadata_bytes,
                metadata_limit_bytes,
                metadata_remaining_bytes: metadata_limit_bytes.saturating_sub(metadata_bytes),
                used_bytes,
                budget_bytes,
                remaining_bytes: budget_bytes.saturating_sub(used_bytes),
                reclaimable_bytes,
                over_budget: used_bytes > budget_bytes,
                over_conflict_limit: conflicts > conflict_limit,
                over_metadata_limit: metadata_bytes > metadata_limit_bytes,
            })
        })();
        let cleanup = self.conn.execute_batch(
            "DROP TABLE IF EXISTS temp.recovery_objects; DROP TABLE IF EXISTS temp.recovery_refs;",
        );
        let result = result.and_then(|usage| {
            cleanup?;
            Ok(usage)
        });
        match result {
            Ok(usage) => {
                transaction.commit()?;
                Ok(usage)
            }
            Err(error) => Err(error),
        }
    }

    pub fn ensure_recovery_limits(&self, share: &ShareId, planned: &[Conflict]) -> Result<()> {
        let _object_lock = self.lock_objects()?;
        self.prepare_recovery_objects(share, planned)?;
        let result = (|| {
            let (current_count, current_metadata) = recovery_row_totals(&self.conn, share)?;
            let mut projected_count = current_count;
            let mut projected_metadata = current_metadata;
            let mut planned_documents = HashMap::new();
            for conflict in planned {
                let id = crate::reconcile::conflict_id(conflict);
                let document = serde_json::to_string(conflict)?;
                if !remember_planned_document(&mut planned_documents, &id, &document)? {
                    continue;
                }
                match self.stored_conflict_document(&id)? {
                    Some((stored_share, stored))
                        if stored_share == *share && stored == document => {}
                    Some(_) => bail!("conflict ID collision for {id}"),
                    None => {
                        projected_count = projected_count
                            .checked_add(1)
                            .context("recovery conflict count overflow")?;
                        projected_metadata = projected_metadata
                            .checked_add(conflict_metadata_bytes(&id, conflict, &document)?)
                            .context("recovery metadata byte overflow")?;
                    }
                }
            }
            enforce_recovery_limit(
                RecoveryLimitKind::ConflictCount,
                current_count,
                projected_count,
                self.recovery_conflict_limit()?,
            )?;
            enforce_recovery_limit(
                RecoveryLimitKind::MetadataBytes,
                current_metadata,
                projected_metadata,
                self.recovery_metadata_limit()?,
            )?;
            let object_bytes = temp_object_bytes(&self.conn, "recovery_objects")?;
            let current = self.recovery_usage_charge(share)?;
            let projected = object_bytes
                .checked_add(projected_metadata)
                .context("recovery usage exceeds supported byte range")?;
            enforce_recovery_limit(
                RecoveryLimitKind::BudgetBytes,
                current,
                projected,
                self.recovery_budget(share)?,
            )
        })();
        let cleanup = self
            .conn
            .execute_batch("DROP TABLE IF EXISTS temp.recovery_objects;");
        result.and_then(|()| {
            cleanup?;
            Ok(())
        })
    }

    fn recovery_usage_charge(&self, share: &ShareId) -> Result<u64> {
        self.conn.execute_batch(
            "DROP TABLE IF EXISTS temp.current_recovery_objects;
             CREATE TEMP TABLE current_recovery_objects(
                 hash TEXT PRIMARY KEY,
                 size INTEGER NOT NULL
             ) WITHOUT ROWID;",
        )?;
        let result = (|| {
            self.stream_conflict_objects(share, "current_recovery_objects")?;
            let (_, metadata) = recovery_row_totals(&self.conn, share)?;
            temp_object_bytes(&self.conn, "current_recovery_objects")?
                .checked_add(metadata)
                .context("recovery usage exceeds supported byte range")
        })();
        let cleanup = self
            .conn
            .execute_batch("DROP TABLE IF EXISTS temp.current_recovery_objects;");
        result.and_then(|value| {
            cleanup?;
            Ok(value)
        })
    }

    fn stored_conflict_document(&self, id: &str) -> Result<Option<(ShareId, String)>> {
        self.conn
            .query_row(
                "SELECT share_id,conflict_json FROM conflicts WHERE id=?1",
                [id],
                |row| Ok((ShareId(row.get(0)?), row.get::<_, Option<String>>(1)?)),
            )
            .optional()?
            .map(|(share, document)| {
                document
                    .context("legacy conflict has no canonical document")
                    .map(|document| (share, document))
            })
            .transpose()
    }

    fn prepare_recovery_objects(&self, share: &ShareId, planned: &[Conflict]) -> Result<()> {
        #[cfg(feature = "e2e-test-hooks")]
        if self.dir.join(".e2e-recovery-temp-fail").exists() {
            bail!("injected recovery temporary-table failure");
        }
        self.conn.execute_batch(
            "DROP TABLE IF EXISTS temp.recovery_objects;
             CREATE TEMP TABLE recovery_objects(
                 hash TEXT PRIMARY KEY,
                 size INTEGER NOT NULL
             ) WITHOUT ROWID;",
        )?;
        self.stream_conflict_objects(share, "recovery_objects")?;
        for conflict in planned {
            insert_conflict_objects(&self.conn, "recovery_objects", conflict)?;
        }
        Ok(())
    }

    fn stream_conflict_objects(&self, share: &ShareId, table: &str) -> Result<()> {
        let mut statement = self.conn.prepare(
            "SELECT winner_json,loser_json,conflict_json FROM conflicts WHERE share_id=?1",
        )?;
        let rows = statement.query_map([&share.0], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })?;
        for row in rows {
            let (winner, loser, document) = row?;
            let conflict = decode_conflict(&winner, &loser, document.as_deref())?;
            insert_conflict_objects(&self.conn, table, &conflict)?;
        }
        Ok(())
    }

    fn recovery_reclaimable_all(&self, share: &ShareId) -> Result<u64> {
        self.conn.execute_batch(
            "DROP TABLE IF EXISTS temp.recovery_refs;
             CREATE TEMP TABLE recovery_refs(hash TEXT PRIMARY KEY) WITHOUT ROWID;",
        )?;
        self.stream_global_object_refs(Some(share), None, "recovery_refs")?;
        let mut statement = self.conn.prepare(
            "SELECT objects.hash FROM recovery_objects AS objects
             LEFT JOIN recovery_refs AS refs ON refs.hash=objects.hash
             WHERE refs.hash IS NULL",
        )?;
        let mut total = 0u64;
        for row in statement.query_map([], |row| row.get::<_, String>(0))? {
            let hash = ObjectHash::parse(row?)?;
            let path = self.object_path(&hash);
            let metadata = match fs::symlink_metadata(&path) {
                Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                    metadata
                }
                Ok(_) => bail!("stored object is not a regular file"),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            total = total
                .checked_add(metadata.len())
                .context("reclaimable object size overflow")?;
        }
        Ok(total)
    }

    fn stream_global_object_refs(
        &self,
        excluded_conflict_share: Option<&ShareId>,
        excluded_conflict_ids: Option<(&ShareId, &HashSet<String>)>,
        table: &str,
    ) -> Result<()> {
        self.visit_global_object_refs(excluded_conflict_share, excluded_conflict_ids, |hash| {
            insert_temp_hash(&self.conn, table, hash)
        })
    }

    fn visit_global_object_refs(
        &self,
        excluded_conflict_share: Option<&ShareId>,
        excluded_conflict_ids: Option<(&ShareId, &HashSet<String>)>,
        mut visit: impl FnMut(&ObjectHash) -> Result<()>,
    ) -> Result<()> {
        {
            let mut statement = self.conn.prepare("SELECT version_json FROM records")?;
            for row in statement.query_map([], |row| row.get::<_, String>(0))? {
                let version: Version = serde_json::from_str(&row?)?;
                visit_entry_object(&version.entry, &mut visit)?;
                if let Some(base) = version.merge_base {
                    visit_entry_object(&base.entry, &mut visit)?;
                }
            }
        }
        {
            let mut statement = self.conn.prepare("SELECT base_json FROM shared_heads")?;
            for row in statement.query_map([], |row| row.get::<_, String>(0))? {
                let base: crate::model::BaseVersion = serde_json::from_str(&row?)?;
                visit_entry_object(&base.entry, &mut visit)?;
            }
        }
        {
            let mut statement = self.conn.prepare(
                "SELECT id,share_id,winner_json,loser_json,conflict_json FROM conflicts",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    ShareId(row.get(1)?),
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            })?;
            for row in rows {
                let (id, share, winner, loser, document) = row?;
                if excluded_conflict_share == Some(&share) {
                    continue;
                }
                if excluded_conflict_ids.is_some_and(|(excluded_share, ids)| {
                    excluded_share == &share && ids.contains(&id)
                }) {
                    continue;
                }
                let conflict = decode_conflict(&winner, &loser, document.as_deref())?;
                for entry in conflict_entries(&conflict) {
                    visit_entry_object(entry, &mut visit)?;
                }
            }
        }
        for (_, intent) in self.install_intents()? {
            for record in intent.records {
                visit_entry_object(&record.version.entry, &mut visit)?;
            }
            for conflict in intent.conflicts {
                for entry in conflict_entries(&conflict) {
                    visit_entry_object(entry, &mut visit)?;
                }
            }
        }
        let mut statement = self.conn.prepare("SELECT hash FROM pending_objects")?;
        for row in statement.query_map([], |row| row.get::<_, String>(0))? {
            let hash = ObjectHash::parse(row?)?;
            visit(&hash)?;
        }
        Ok(())
    }

    pub fn recovery_prune_plan(
        &mut self,
        share: &ShareId,
        conflict_ids: &[String],
    ) -> Result<RecoveryPrunePlan> {
        self.scheduled_mutation(Some(share), SyncOperation::Maintenance, |state| {
            state.recovery_prune_plan_locked_with_objects(share, conflict_ids)
        })
    }

    pub fn recovery_prune_plan_locked_with_objects(
        &self,
        share: &ShareId,
        conflict_ids: &[String],
    ) -> Result<RecoveryPrunePlan> {
        let _object_lock = self.lock_objects()?;
        self.recovery_prune_plan_locked(share, conflict_ids)
    }

    pub fn prune_recovery(
        &mut self,
        share: &ShareId,
        conflict_ids: &[String],
        expected_token: &str,
    ) -> Result<RecoveryPruneOutcome> {
        self.scheduled_mutation(Some(share), SyncOperation::Maintenance, |state| {
            state.prune_recovery_locked(share, conflict_ids, expected_token)
        })
    }

    pub fn prune_recovery_locked(
        &mut self,
        share: &ShareId,
        conflict_ids: &[String],
        expected_token: &str,
    ) -> Result<RecoveryPruneOutcome> {
        let _object_lock = self.lock_objects()?;
        let plan = self.recovery_prune_plan_locked(share, conflict_ids)?;
        if plan.selection_token != expected_token {
            bail!(
                "recovery conflicts changed since preview; rerun the prune preview and apply with the new selection token"
            );
        }
        let transaction = self.conn.transaction()?;
        for conflict in &plan.conflicts {
            let changed = transaction.execute(
                "DELETE FROM conflicts WHERE share_id=?1 AND id=?2",
                params![share.0, conflict.id],
            )?;
            if changed != 1 {
                bail!("recovery conflicts changed while pruning");
            }
        }
        transaction.commit()?;
        let collection_pending = self.prune_unreferenced_objects_locked().is_err();
        Ok(RecoveryPruneOutcome {
            plan,
            collection_pending,
        })
    }

    fn recovery_prune_plan_locked(
        &self,
        share: &ShareId,
        conflict_ids: &[String],
    ) -> Result<RecoveryPrunePlan> {
        if !conflict_ids.is_empty() {
            let selected = self.raw_conflicts_for_prune(share, conflict_ids)?;
            return self.recovery_prune_plan_selected(share, selected);
        }
        self.recovery_prune_plan_all(share)
    }

    fn recovery_prune_plan_all(&self, share: &ShareId) -> Result<RecoveryPrunePlan> {
        #[cfg(feature = "e2e-test-hooks")]
        if self.dir.join(".e2e-recovery-temp-fail").exists() {
            bail!("injected recovery temporary-table failure");
        }
        #[cfg(feature = "e2e-test-hooks")]
        let fail_after = self.recovery_test_limit(".e2e-recovery-temp-fail-after")?;
        let (summaries, summary_bytes): (i64, i64) = self.conn.query_row(
            "SELECT COUNT(*),COALESCE(SUM(6*LENGTH(CAST(id AS BLOB))+16*LENGTH(path)+128),0)
             FROM conflicts WHERE share_id=?1",
            [&share.0],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        #[cfg(feature = "e2e-test-hooks")]
        let maximum_summaries = self
            .recovery_test_limit(".e2e-recovery-preview-summary-limit")?
            .unwrap_or(MAX_ALL_PRUNE_SUMMARIES);
        #[cfg(not(feature = "e2e-test-hooks"))]
        let maximum_summaries = MAX_ALL_PRUNE_SUMMARIES;
        enforce_all_prune_summary_bounds(
            u64::try_from(summaries)?,
            u64::try_from(summary_bytes)?,
            maximum_summaries,
            MAX_ALL_PRUNE_SUMMARY_BYTES,
        )?;
        self.conn.execute_batch(
            "DROP TABLE IF EXISTS temp.prune_selected_objects;
             DROP TABLE IF EXISTS temp.prune_refs;
             CREATE TEMP TABLE prune_selected_objects(
                 hash TEXT PRIMARY KEY,
                 size INTEGER NOT NULL
             ) WITHOUT ROWID;
             CREATE TEMP TABLE prune_refs(hash TEXT PRIMARY KEY) WITHOUT ROWID;",
        )?;
        let result = (|| {
            let mut released_metadata = 0u64;
            let mut selection_hasher = prune_selection_hasher(share, true);
            let mut conflicts = Vec::new();
            let mut statement = self.conn.prepare(
                "SELECT id,path,winner_json,loser_json,created_ns,conflict_json
                 FROM conflicts WHERE share_id=?1 ORDER BY id",
            )?;
            let rows = statement.query_map([&share.0], raw_conflict_from_row)?;
            for row in rows {
                #[cfg(feature = "e2e-test-hooks")]
                if fail_after == Some(conflicts.len() as u64) {
                    bail!("injected recovery temporary-table extension failure");
                }
                let row = row?;
                released_metadata = released_metadata
                    .checked_add(raw_conflict_metadata_bytes(&row)?)
                    .context("released recovery metadata overflow")?;
                let conflict = decode_conflict(&row.winner, &row.loser, row.document.as_deref())?;
                insert_conflict_objects(&self.conn, "prune_selected_objects", &conflict)?;
                hash_token_field(&mut selection_hasher, &serde_json::to_vec(&row)?);
                conflicts.push(RecoveryPruneConflict {
                    id: row.id,
                    path: RelativePath::from_bytes(row.path)?,
                });
            }
            let released_objects = temp_object_bytes(&self.conn, "prune_selected_objects")?;
            self.stream_global_object_refs(Some(share), None, "prune_refs")?;
            let mut reclaimable = 0u64;
            let mut statement = self.conn.prepare(
                "SELECT selected.hash FROM prune_selected_objects AS selected
                 LEFT JOIN prune_refs AS refs ON refs.hash=selected.hash
                 WHERE refs.hash IS NULL",
            )?;
            for row in statement.query_map([], |row| row.get::<_, String>(0))? {
                let hash = ObjectHash::parse(row?)?;
                match fs::symlink_metadata(self.object_path(&hash)) {
                    Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                        reclaimable = reclaimable
                            .checked_add(metadata.len())
                            .context("reclaimable object size overflow")?;
                    }
                    Ok(_) => bail!("stored object is not a regular file"),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
            Ok(RecoveryPrunePlan {
                conflicts,
                selection_token: selection_hasher.finalize().to_hex().to_string(),
                released_bytes: released_objects
                    .checked_add(released_metadata)
                    .context("released recovery size overflow")?,
                reclaimable_bytes: reclaimable,
            })
        })();
        let cleanup = self.conn.execute_batch(
            "DROP TABLE IF EXISTS temp.prune_selected_objects;
             DROP TABLE IF EXISTS temp.prune_refs;",
        );
        result.and_then(|plan| {
            cleanup?;
            Ok(plan)
        })
    }

    fn recovery_prune_plan_selected(
        &self,
        share: &ShareId,
        selected: Vec<RawConflictRow>,
    ) -> Result<RecoveryPrunePlan> {
        let selected_ids: HashSet<String> = selected.iter().map(|row| row.id.clone()).collect();
        let mut selected_objects = HashMap::new();
        let mut released_metadata = 0u64;
        for row in &selected {
            released_metadata = released_metadata
                .checked_add(raw_conflict_metadata_bytes(row)?)
                .context("released recovery metadata overflow")?;
            let conflict = decode_conflict(&row.winner, &row.loser, row.document.as_deref())?;
            remember_conflict_object_sizes(&mut selected_objects, &conflict)?;
        }

        let mut released_objects = selected_objects.clone();
        let mut statement = self.conn.prepare(
            "SELECT id,winner_json,loser_json,conflict_json FROM conflicts WHERE share_id=?1",
        )?;
        let rows = statement.query_map([&share.0], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })?;
        for row in rows {
            let (id, winner, loser, document) = row?;
            if selected_ids.contains(&id) {
                continue;
            }
            let conflict = decode_conflict(&winner, &loser, document.as_deref())?;
            forget_conflict_objects(&mut released_objects, &conflict);
        }
        let released_object_bytes = checked_object_size_sum(&released_objects)?;

        let mut reclaimable = selected_objects;
        self.eliminate_global_object_refs(&mut reclaimable, share, &selected_ids)?;
        let mut reclaimable_bytes = 0u64;
        for hash in reclaimable.keys() {
            match fs::symlink_metadata(self.object_path(hash)) {
                Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                    reclaimable_bytes = reclaimable_bytes
                        .checked_add(metadata.len())
                        .context("reclaimable object size overflow")?;
                }
                Ok(_) => bail!("stored object is not a regular file"),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(RecoveryPrunePlan {
            conflicts: selected
                .iter()
                .map(|row| {
                    Ok(RecoveryPruneConflict {
                        id: row.id.clone(),
                        path: RelativePath::from_bytes(row.path.clone())?,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            selection_token: prune_selection_token(share, false, &selected)?,
            released_bytes: released_object_bytes
                .checked_add(released_metadata)
                .context("released recovery size overflow")?,
            reclaimable_bytes,
        })
    }

    fn eliminate_global_object_refs(
        &self,
        candidates: &mut HashMap<ObjectHash, u64>,
        selected_share: &ShareId,
        selected_ids: &HashSet<String>,
    ) -> Result<()> {
        self.visit_global_object_refs(None, Some((selected_share, selected_ids)), |hash| {
            candidates.remove(hash);
            Ok(())
        })
    }

    fn raw_conflicts_for_prune(
        &self,
        share: &ShareId,
        conflict_ids: &[String],
    ) -> Result<Vec<RawConflictRow>> {
        debug_assert!(!conflict_ids.is_empty());
        let mut ids = conflict_ids.to_vec();
        ids.sort();
        if ids.windows(2).any(|pair| pair[0] == pair[1]) {
            bail!("duplicate conflict IDs are not allowed");
        }
        ids.into_iter()
            .map(|id| {
                self.conn
                    .query_row(
                        "SELECT id,path,winner_json,loser_json,created_ns,conflict_json
                         FROM conflicts WHERE share_id=?1 AND id=?2",
                        params![share.0, id],
                        raw_conflict_from_row,
                    )
                    .optional()?
                    .context(format!("conflict {id} not found in this share"))
            })
            .collect()
    }

    pub fn conflicts(&self, share: &ShareId) -> Result<Vec<StoredConflict>> {
        let mut stmt = self.conn.prepare(
            "SELECT id,path,winner_json,loser_json,created_ns,conflict_json FROM conflicts
             WHERE share_id=?1 ORDER BY CAST(created_ns AS INTEGER) DESC",
        )?;
        let rows = stmt.query_map([&share.0], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Vec<u8>>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, Option<String>>(5)?,
            ))
        })?;
        rows.map(|row| stored_conflict_from_parts(row?)).collect()
    }

    pub fn conflict_ids_page(
        &self,
        share: &ShareId,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RecoveryPruneConflict>> {
        if limit == 0 || limit > 1000 {
            bail!("conflict ID page limit must be between 1 and 1000");
        }
        let mut statement = self.conn.prepare(
            "SELECT id,path FROM conflicts
             WHERE share_id=?1 AND (?2 IS NULL OR id>?2)
             ORDER BY id LIMIT ?3",
        )?;
        statement
            .query_map(params![share.0, after, i64::try_from(limit + 1)?], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
            })?
            .map(|row| {
                let (id, path) = row?;
                Ok(RecoveryPruneConflict {
                    id,
                    path: RelativePath::from_bytes(path)?,
                })
            })
            .collect()
    }

    pub fn conflict(&self, share: &ShareId, id: &str) -> Result<StoredConflict> {
        let row = self
            .conn
            .query_row(
                "SELECT id,path,winner_json,loser_json,created_ns,conflict_json
                 FROM conflicts WHERE share_id=?1 AND id=?2",
                params![share.0, id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                    ))
                },
            )
            .optional()?
            .context("conflict not found")?;
        stored_conflict_from_parts(row)
    }

    pub fn initial_complete(&self, id: &ShareId) -> Result<bool> {
        Ok(self.conn.query_row(
            "SELECT initial_complete FROM shares WHERE share_id=?1",
            [&id.0],
            |r| r.get::<_, i64>(0),
        )? != 0)
    }

    pub fn managed_shares(&self) -> Result<Vec<ManagedShare>> {
        let mut statement = self.conn.prepare(
            "SELECT share_id, root, peer_json, initial_complete, watch_enabled, blocked_diagnostic,
                    bound_peer, bound_relationship, removing_relationship
             FROM shares ORDER BY share_id",
        )?;
        statement
            .query_map([], managed_share_from_row)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn managed_share(&self, id: &ShareId) -> Result<ManagedShare> {
        self.conn
            .query_row(
                "SELECT share_id, root, peer_json, initial_complete, watch_enabled, blocked_diagnostic,
                        bound_peer, bound_relationship, removing_relationship
                 FROM shares WHERE share_id=?1",
                [&id.0],
                managed_share_from_row,
            )
            .optional()?
            .context("share not found")
    }

    pub fn set_watch_enabled(&self, id: &ShareId, enabled: bool) -> Result<()> {
        let changed = self.conn.execute(
            "UPDATE shares SET watch_enabled=?2, intent_generation=intent_generation+1
             WHERE share_id=?1 AND removing_relationship IS NULL",
            params![id.0, i64::from(enabled)],
        )?;
        if changed == 0 {
            bail!("share not found or relationship removal is pending");
        }
        Ok(())
    }

    pub fn set_watch_enabled_if_generation(
        &self,
        id: &ShareId,
        enabled: bool,
        expected_generation: i64,
    ) -> Result<()> {
        let changed = self.conn.execute(
            "UPDATE shares SET watch_enabled=?2, intent_generation=intent_generation+1
             WHERE share_id=?1 AND intent_generation=?3 AND removing_relationship IS NULL",
            params![id.0, i64::from(enabled), expected_generation],
        )?;
        if changed == 0 {
            bail!("sync was stopped, removed, or reconfigured while its initial plan was running");
        }
        Ok(())
    }

    pub fn watch_intent_generation(&self, id: &ShareId) -> Result<i64> {
        self.conn
            .query_row(
                "SELECT intent_generation FROM shares WHERE share_id=?1",
                [&id.0],
                |row| row.get(0),
            )
            .context("share not found")
    }

    pub fn set_initial_complete_and_watch_enabled(
        &mut self,
        id: &ShareId,
        expected_generation: i64,
    ) -> Result<()> {
        let transaction = self.conn.transaction()?;
        let changed = transaction.execute(
            "UPDATE shares SET initial_complete=1, watch_enabled=1, blocked_diagnostic=NULL,
             intent_generation=intent_generation+1
             WHERE share_id=?1 AND intent_generation=?2 AND removing_relationship IS NULL",
            params![id.0, expected_generation],
        )?;
        if changed == 0 {
            bail!("sync was stopped, removed, or reconfigured while its initial plan was running");
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn set_blocked(&self, id: &ShareId, diagnostic: &str) -> Result<()> {
        let diagnostic = diagnostic.chars().take(4096).collect::<String>();
        let changed = self.conn.execute(
            "UPDATE shares SET blocked_diagnostic=?2
             WHERE share_id=?1 AND removing_relationship IS NULL",
            params![id.0, diagnostic],
        )?;
        if changed == 0 {
            bail!("share not found or relationship removal is pending");
        }
        Ok(())
    }

    pub fn clear_blocked(&self, id: &ShareId) -> Result<()> {
        let changed = self.conn.execute(
            "UPDATE shares SET blocked_diagnostic=NULL
             WHERE share_id=?1 AND removing_relationship IS NULL",
            [&id.0],
        )?;
        if changed == 0 {
            bail!("share not found or relationship removal is pending");
        }
        Ok(())
    }

    fn reject_overlapping_root(&self, candidate: &Path) -> Result<()> {
        self.reject_overlapping_root_except(candidate, None)
    }

    fn reject_overlapping_root_except(
        &self,
        candidate: &Path,
        except: Option<&ShareId>,
    ) -> Result<()> {
        let mut statement = self.conn.prepare("SELECT share_id, root FROM shares")?;
        let rows = statement.query_map([], |row| {
            Ok((ShareId(row.get(0)?), bytes_path(row.get::<_, Vec<u8>>(1)?)))
        })?;
        for row in rows {
            let (id, root) = row?;
            if except == Some(&id) {
                continue;
            }
            if candidate.starts_with(&root) || root.starts_with(candidate) {
                bail!(
                    "directory overlaps registered share {} at {}",
                    id.0,
                    root.display()
                );
            }
        }
        Ok(())
    }

    pub fn store_object(&self, mut input: File) -> Result<(ObjectHash, u64)> {
        let metadata_before = input.metadata()?;
        let (probe_hash, probe_size) = self.hash_object(input.try_clone()?)?;
        input.rewind()?;
        let mut sink = self.begin_object(probe_hash.clone(), probe_size)?;
        if sink.already_present() {
            return Ok((probe_hash, probe_size));
        }
        let mut buffer = vec![0u8; 1024 * 1024];
        loop {
            let read = input.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            sink.write_chunk(&buffer[..read])?;
        }
        let metadata_after = input.metadata()?;
        if !same_file_snapshot(&metadata_before, &metadata_after)? {
            bail!("file changed while it was being captured");
        }
        sink.finish()?;
        Ok((probe_hash, probe_size))
    }

    pub fn hash_object(&self, mut input: File) -> Result<(ObjectHash, u64)> {
        let before = input.metadata()?;
        let mut hasher = blake3::Hasher::new();
        let mut size = 0u64;
        let mut buffer = vec![0u8; 1024 * 1024];
        loop {
            let read = input.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
            size += read as u64;
        }
        if !same_file_snapshot(&before, &input.metadata()?)? {
            bail!("file changed while it was being hashed");
        }
        Ok((ObjectHash::from_blake3(hasher.finalize()), size))
    }

    fn available_object_bytes(&self) -> Result<u64> {
        let budget = std::env::var("FLOCAL_MAX_STATE_BYTES")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(10 * 1024 * 1024 * 1024);
        let used = fs::read_dir(self.dir.join("objects"))?.try_fold(0u64, |total, entry| {
            let entry = entry?;
            let metadata = fs::symlink_metadata(entry.path())?;
            Ok::<_, std::io::Error>(
                if metadata.is_file() && !metadata.file_type().is_symlink() {
                    total.saturating_add(metadata.len())
                } else {
                    total
                },
            )
        })?;
        Ok(budget.saturating_sub(used))
    }

    pub fn object_path(&self, hash: &ObjectHash) -> PathBuf {
        self.dir.join("objects").join(hash.as_str())
    }

    pub fn import_object(&self, expected: &ObjectHash, bytes: &[u8]) -> Result<()> {
        let mut sink = self.begin_object(expected.clone(), bytes.len() as u64)?;
        sink.write_chunk(bytes)?;
        sink.finish()
    }

    pub fn begin_object(
        &self,
        expected_hash: ObjectHash,
        expected_size: u64,
    ) -> Result<ObjectSink> {
        let budget_lock = self.lock_objects()?;
        let final_path = self.object_path(&expected_hash);
        let reclaimable = match fs::symlink_metadata(&final_path) {
            Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                if object_path_matches(&final_path, &expected_hash)? {
                    return Ok(ObjectSink {
                        _budget_lock: budget_lock,
                        file: None,
                        temp_path: None,
                        final_path,
                        expected_hash,
                        expected_size,
                        written: 0,
                        hasher: blake3::Hasher::new(),
                    });
                }
                metadata.len()
            }
            Ok(_) | Err(_) => 0,
        };
        if expected_size > self.available_object_bytes()?.saturating_add(reclaimable) {
            bail!("object exceeds remaining state storage budget");
        }
        if reclaimable > 0 {
            fs::remove_file(&final_path)?;
            sync_dir(final_path.parent().expect("object parent"))?;
        }
        let temp_path = self
            .dir
            .join("objects")
            .join(format!(".tmp-{}", ShareId::generate().0));
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)?;
        set_private_file(&temp_path)?;
        Ok(ObjectSink {
            _budget_lock: budget_lock,
            file: Some(file),
            temp_path: Some(temp_path),
            final_path,
            expected_hash,
            expected_size,
            written: 0,
            hasher: blake3::Hasher::new(),
        })
    }

    pub fn read_object(&self, hash: &ObjectHash) -> Result<Vec<u8>> {
        let bytes = fs::read(self.object_path(hash))?;
        if ObjectHash::from_blake3(blake3::hash(&bytes)) != *hash {
            bail!("stored object hash mismatch");
        }
        Ok(bytes)
    }

    pub fn open_verified_object(&self, hash: &ObjectHash) -> Result<File> {
        let path = self.object_path(hash);
        let metadata = fs::symlink_metadata(&path)?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            bail!("stored object is not a regular file");
        }
        let mut file = File::open(&path)?;
        let mut hasher = blake3::Hasher::new();
        let mut buffer = vec![0u8; 1024 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        if ObjectHash::from_blake3(hasher.finalize()) != *hash {
            bail!("stored object hash mismatch");
        }
        file.rewind()?;
        Ok(file)
    }

    pub fn prune_unreferenced_objects(&self) -> Result<()> {
        let _lock = self.lock_objects()?;
        self.prune_unreferenced_objects_locked()
    }

    fn prune_unreferenced_objects_locked(&self) -> Result<()> {
        #[cfg(feature = "e2e-test-hooks")]
        if self.dir.join(".e2e-collector-fail").exists() {
            bail!("injected object collection failure");
        }
        self.conn.execute_batch(
            "DROP TABLE IF EXISTS temp.collector_refs;
             CREATE TEMP TABLE collector_refs(hash TEXT PRIMARY KEY) WITHOUT ROWID;",
        )?;
        let result = (|| {
            self.stream_global_object_refs(None, None, "collector_refs")?;
            let mut removed = false;
            for entry in fs::read_dir(self.dir.join("objects"))? {
                let entry = entry?;
                let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                    continue;
                };
                if name.starts_with(".tmp-") {
                    let metadata = fs::symlink_metadata(entry.path())?;
                    if metadata.is_file() && !metadata.file_type().is_symlink() {
                        fs::remove_file(entry.path())?;
                        removed = true;
                    }
                    continue;
                }
                let Ok(hash) = ObjectHash::parse(name) else {
                    continue;
                };
                let referenced = self
                    .conn
                    .query_row(
                        "SELECT 1 FROM collector_refs WHERE hash=?1",
                        [hash.as_str()],
                        |_| Ok(()),
                    )
                    .optional()?
                    .is_some();
                if !referenced {
                    let metadata = fs::symlink_metadata(entry.path())?;
                    if metadata.is_file() && !metadata.file_type().is_symlink() {
                        fs::remove_file(entry.path())?;
                        removed = true;
                    }
                }
            }
            if removed {
                sync_dir(&self.dir.join("objects"))?;
            }
            Ok(())
        })();
        let cleanup = self
            .conn
            .execute_batch("DROP TABLE IF EXISTS temp.collector_refs;");
        result.and_then(|()| cleanup.map_err(Into::into))
    }
}

#[derive(Clone, Copy)]
enum RemovalRole {
    Connector,
    Responder,
    Either,
}

impl RemovalRole {
    fn accepts(self, binding: &EndpointBinding) -> bool {
        match self {
            Self::Connector => matches!(binding, EndpointBinding::Connector(_)),
            Self::Responder => matches!(binding, EndpointBinding::Responder { .. }),
            Self::Either => !matches!(binding, EndpointBinding::Unpaired),
        }
    }
}

fn bounded_diagnostic(diagnostic: &str) -> String {
    let mut bytes = 0usize;
    diagnostic
        .chars()
        .take_while(|character| {
            let next = bytes.saturating_add(character.len_utf8());
            if next > 4096 {
                false
            } else {
                bytes = next;
                true
            }
        })
        .collect()
}

fn validate_install_intent_fingerprint(fingerprint: &str) -> Result<()> {
    if fingerprint.len() != 64
        || !fingerprint
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("install intent fingerprint must be 64 lowercase hexadecimal characters");
    }
    Ok(())
}

fn decode_install_intent_failure(
    intent: &InstallIntent,
    fingerprint: Option<String>,
    diagnostic: Option<String>,
) -> Result<Option<InstallIntentFailure>> {
    match (fingerprint, diagnostic) {
        (None, None) => Ok(None),
        (Some(fingerprint), Some(diagnostic)) => {
            validate_install_intent_fingerprint(&fingerprint)?;
            if diagnostic.len() > 4096 {
                bail!("stored install recovery diagnostic exceeds its limit");
            }
            if State::install_intent_fingerprint(intent)? != fingerprint {
                bail!("stored install recovery classification does not match its intent");
            }
            Ok(Some(InstallIntentFailure {
                fingerprint,
                diagnostic,
            }))
        }
        _ => bail!("stored install recovery classification is incomplete"),
    }
}

fn clear_install_failure_diagnostic(
    transaction: &rusqlite::Transaction<'_>,
    share: &ShareId,
) -> Result<()> {
    let diagnostic: Option<Option<String>> = transaction
        .query_row(
            "SELECT failure_diagnostic FROM install_intents WHERE share_id=?1",
            [&share.0],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(Some(diagnostic)) = diagnostic {
        transaction.execute(
            "UPDATE shares SET blocked_diagnostic=NULL
             WHERE share_id=?1 AND blocked_diagnostic=?2",
            params![share.0, diagnostic],
        )?;
    }
    Ok(())
}

struct BindingColumns {
    peer_json: Option<String>,
    bound_peer: Option<String>,
    bound_relationship: Option<String>,
    marker: Option<String>,
}

fn binding_and_marker(
    connection: &Connection,
    share: &ShareId,
) -> Result<Option<(EndpointBinding, Option<RelationshipId>)>> {
    let row: Option<BindingColumns> = connection
        .query_row(
            "SELECT peer_json,bound_peer,bound_relationship,removing_relationship
             FROM shares WHERE share_id=?1",
            [&share.0],
            |row| {
                Ok(BindingColumns {
                    peer_json: row.get(0)?,
                    bound_peer: row.get(1)?,
                    bound_relationship: row.get(2)?,
                    marker: row.get(3)?,
                })
            },
        )
        .optional()?;
    row.map(decode_binding_columns).transpose()
}

fn decode_binding_columns(
    columns: BindingColumns,
) -> Result<(EndpointBinding, Option<RelationshipId>)> {
    let peer = columns
        .peer_json
        .map(|json| serde_json::from_str::<PeerConfig>(&json))
        .transpose()
        .context("stored connector configuration is invalid")?;
    let bound_relationship = columns
        .bound_relationship
        .map(RelationshipId::parse)
        .transpose()
        .context("stored responder relationship is invalid")?;
    let marker = columns
        .marker
        .map(RelationshipId::parse)
        .transpose()
        .context("stored removal relationship is invalid")?;
    let binding = match (peer, columns.bound_peer, bound_relationship) {
        (Some(_), Some(_), _) | (Some(_), None, Some(_)) => {
            bail!("share has simultaneous connector and responder bindings")
        }
        (Some(peer), None, None) => EndpointBinding::Connector(peer),
        (None, Some(peer), relationship) => EndpointBinding::Responder {
            peer: PeerId(peer),
            relationship,
        },
        (None, None, Some(_)) => bail!("responder relationship has no bound peer"),
        (None, None, None) => EndpointBinding::Unpaired,
    };
    Ok((binding, marker))
}

fn prepare_removal_transaction(
    transaction: &rusqlite::Transaction<'_>,
    share: &ShareId,
    expected: &EndpointBinding,
    legacy_relationship: Option<&RelationshipId>,
) -> Result<PreparedRemoval> {
    let (binding, marker) = binding_and_marker(transaction, share)?.context("share not found")?;
    if &binding != expected {
        bail!("share relationship changed since removal preview");
    }
    if matches!(binding, EndpointBinding::Unpaired) {
        bail!("no relationship is configured");
    }
    let relationship = if let Some(marker) = marker {
        if binding
            .relationship()
            .is_some_and(|relationship| relationship != &marker)
        {
            bail!("removal marker does not match the current relationship");
        }
        if legacy_relationship.is_some_and(|requested| requested != &marker) {
            bail!("removal marker does not match the requested relationship");
        }
        marker
    } else {
        let relationship = match (binding.relationship(), legacy_relationship) {
            (Some(stored), Some(requested)) if stored != requested => {
                bail!("requested relationship does not match the current binding")
            }
            (Some(stored), _) => stored.clone(),
            (None, Some(requested)) => requested.clone(),
            (None, None) => RelationshipId::generate(),
        };
        let changed = transaction.execute(
            "UPDATE shares SET removing_relationship=?2, watch_enabled=0,
             blocked_diagnostic=NULL, intent_generation=intent_generation+1
             WHERE share_id=?1 AND removing_relationship IS NULL",
            params![share.0, relationship.0],
        )?;
        if changed != 1 {
            bail!("relationship changed while preparing removal");
        }
        relationship
    };
    Ok(PreparedRemoval {
        share: share.clone(),
        relationship,
        binding,
    })
}

fn ensure_relationship_available(
    connection: &Connection,
    requested_share: &ShareId,
    relationship: &RelationshipId,
) -> Result<()> {
    let responder_collision: Option<String> = connection
        .query_row(
            "SELECT share_id FROM shares
             WHERE (bound_relationship=?1 OR removing_relationship=?1)
               AND share_id<>?2 LIMIT 1",
            params![relationship.0, requested_share.0],
            |row| row.get(0),
        )
        .optional()?;
    if responder_collision.is_some() {
        bail!("relationship identity is already bound to another share");
    }
    let mut statement = connection.prepare(
        "SELECT share_id,peer_json FROM shares WHERE peer_json IS NOT NULL AND share_id<>?1",
    )?;
    let rows = statement.query_map([&requested_share.0], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (_, json) = row?;
        let config: PeerConfig =
            serde_json::from_str(&json).context("stored connector configuration is invalid")?;
        if config.relationship.as_ref() == Some(relationship) {
            bail!("relationship identity is already bound to another share");
        }
    }
    Ok(())
}

fn ensure_unpaired_registration_state(connection: &Connection, share: &ShareId) -> Result<()> {
    let (initial_complete, watch_enabled): (i64, i64) = connection.query_row(
        "SELECT initial_complete,watch_enabled FROM shares WHERE share_id=?1",
        [&share.0],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    if initial_complete != 0 || watch_enabled != 0 {
        bail!("unpaired retained root has active synchronization state");
    }
    for table in [
        "records",
        "install_intents",
        "shared_heads",
        "unsettled_paths",
        "pending_objects",
    ] {
        let count: i64 = connection.query_row(
            &format!("SELECT COUNT(*) FROM {table} WHERE share_id=?1"),
            [&share.0],
            |row| row.get(0),
        )?;
        if count != 0 {
            bail!("unpaired retained root still has {table} state");
        }
    }
    Ok(())
}

fn validate_stored_root_identity(
    connection: &Connection,
    share: &ShareId,
    root: &Path,
    actual: RootIdentity,
) -> Result<()> {
    let (device, inode): (String, String) = connection.query_row(
        "SELECT root_device,root_inode FROM shares WHERE share_id=?1",
        [&share.0],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    validate_identity_values(root, actual, &device, &inode)
}

fn managed_share_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ManagedShare> {
    let (binding, removing_relationship) = decode_binding_columns(BindingColumns {
        peer_json: row.get(2)?,
        bound_peer: row.get(6)?,
        bound_relationship: row.get(7)?,
        marker: row.get(8)?,
    })
    .map_err(|error| invalid_binding_columns(2, error))?;
    Ok(ManagedShare {
        id: ShareId(row.get(0)?),
        root: bytes_path(row.get::<_, Vec<u8>>(1)?),
        binding,
        initial_complete: row.get::<_, i64>(3)? != 0,
        watch_enabled: row.get::<_, i64>(4)? != 0,
        blocked_diagnostic: row.get(5)?,
        removing_relationship,
    })
}

fn invalid_binding_columns(column: usize, error: anyhow::Error) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        column,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            error.to_string(),
        )),
    )
}

fn validate_scheduler_token(token: &str) -> Result<()> {
    if token.is_empty()
        || token.len() > 128
        || !token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        bail!("scheduler ownership token is invalid");
    }
    Ok(())
}

fn create_scheduler_owner(state_dir: &Path, token: &str) -> Result<File> {
    validate_scheduler_token(token)?;
    let scheduler = state_dir.join("scheduler");
    let path = scheduler.join(token);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    validate_scheduler_owner(&file)?;
    file.lock_shared()?;
    file.sync_all()?;
    sync_dir(&scheduler)?;
    Ok(file)
}

fn remove_scheduler_owner(state_dir: &Path, token: &str) -> Result<()> {
    validate_scheduler_token(token)?;
    match fs::remove_file(state_dir.join("scheduler").join(token)) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(unix)]
fn open_scheduler_owner(state_dir: &Path, token: &str) -> Result<File> {
    use rustix::fs::{Mode, OFlags};

    validate_scheduler_token(token)?;
    let path = state_dir.join("scheduler").join(token);
    let descriptor = rustix::fs::open(
        &path,
        OFlags::RDWR | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|error| std::io::Error::from_raw_os_error(error.raw_os_error()))?;
    let file = File::from(descriptor);
    validate_scheduler_owner(&file)?;
    Ok(file)
}

#[cfg(not(unix))]
fn open_scheduler_owner(state_dir: &Path, token: &str) -> Result<File> {
    validate_scheduler_token(token)?;
    let path = state_dir.join("scheduler").join(token);
    let metadata = fs::symlink_metadata(&path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        bail!("scheduler ownership entry is not a regular file");
    }
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    validate_scheduler_owner(&file)?;
    Ok(file)
}

fn validate_scheduler_owner(file: &File) -> Result<()> {
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        bail!("scheduler ownership entry is not a regular file");
    }
    #[cfg(unix)]
    {
        let uid = rustix::process::geteuid().as_raw();
        if metadata.uid() != uid || metadata.mode() & 0o077 != 0 {
            bail!("scheduler ownership entry is not private");
        }
    }
    Ok(())
}

#[cfg(unix)]
pub fn ensure_private_directory(path: &Path) -> Result<()> {
    use rustix::fs::{Mode, OFlags, mkdirat, openat};

    let path = private_directory_walk_path(path)?;
    let mut current = if path.is_absolute() {
        File::open("/")?
    } else {
        File::open(".")?
    };
    let mut current_path = if path.is_absolute() {
        PathBuf::from("/")
    } else {
        PathBuf::from(".")
    };
    validate_private_directory(&current, &current_path)?;

    for component in path.components() {
        let name = match component {
            Component::RootDir | Component::CurDir => continue,
            Component::Normal(name) => name,
            Component::ParentDir => std::ffi::OsStr::new(".."),
            Component::Prefix(_) => unreachable!("Unix paths have no prefix"),
        };
        let next_path = current_path.join(name);
        let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW;
        let next = match openat(&current, name, flags, Mode::empty()) {
            Ok(next) => next,
            Err(rustix::io::Errno::NOENT) if !matches!(component, Component::ParentDir) => {
                match mkdirat(&current, name, Mode::RUSR | Mode::WUSR | Mode::XUSR) {
                    Ok(()) | Err(rustix::io::Errno::EXIST) => {}
                    Err(error) => return Err(error.into()),
                }
                openat(&current, name, flags, Mode::empty())?
            }
            Err(error) => return Err(error.into()),
        };
        current = File::from(next);
        validate_private_directory(&current, &next_path)?;
        current_path = next_path;
    }
    Ok(())
}

#[cfg(not(unix))]
pub fn ensure_private_directory(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    Ok(())
}

#[cfg(unix)]
fn validate_private_directory(directory: &File, path: &Path) -> Result<()> {
    use rustix::fs::{FileType, Mode};

    let uid = rustix::process::geteuid().as_raw();
    let metadata = rustix::fs::fstat(directory)?;
    if !FileType::from_raw_mode(metadata.st_mode).is_dir() {
        bail!(
            "private directory contains an unsafe path component {}",
            path.display()
        );
    }
    let owner = metadata.st_uid;
    let mode = Mode::from_raw_mode(metadata.st_mode).as_raw_mode();
    let root_owned_sticky = owner == 0 && mode & 0o1000 != 0;
    if owner != uid && owner != 0 {
        bail!(
            "private directory component {} has an unexpected owner",
            path.display()
        );
    }
    if mode & 0o022 != 0 && !root_owned_sticky {
        bail!(
            "private directory component {} is writable by another user",
            path.display()
        );
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn private_directory_walk_path(path: &Path) -> Result<PathBuf> {
    for alias in [Path::new("/var"), Path::new("/tmp")] {
        let Ok(remainder) = path.strip_prefix(alias) else {
            continue;
        };
        let metadata = fs::symlink_metadata(alias)?;
        if metadata.file_type().is_symlink() {
            if metadata.uid() != 0 {
                bail!(
                    "private directory system alias {} is not root-owned",
                    alias.display()
                );
            }
            return Ok(alias.canonicalize()?.join(remainder));
        }
    }
    Ok(path.to_path_buf())
}

#[cfg(not(target_os = "macos"))]
fn private_directory_walk_path(path: &Path) -> Result<PathBuf> {
    Ok(path.to_path_buf())
}

fn remember_candidate_entry_hash(
    hashes: &mut HashSet<ObjectHash>,
    candidates: &HashSet<ObjectHash>,
    entry: &Entry,
) {
    if let Entry::File { hash, .. } = entry
        && candidates.contains(hash)
    {
        hashes.insert(hash.clone());
    }
}

fn authenticate(key: &[u8; 32], bytes: &[u8]) -> String {
    blake3::keyed_hash(key, bytes).to_hex().to_string()
}

fn version_tag(
    key: &[u8; 32],
    share: &ShareId,
    path: &RelativePath,
    version: &Version,
) -> Result<String> {
    Ok(authenticate(
        key,
        &serde_json::to_vec(&(
            "flocal-complete-record-v3",
            &share.0,
            path,
            &version.peer.0,
            version.sequence,
            &version.id_authenticator,
            version.timestamp_ns,
            &version.seen,
            &version.merge_base,
            &version.base_authenticator,
            &version.entry,
        ))?,
    ))
}

fn insert_canonical_record<'a>(
    canonical: &mut std::collections::HashMap<(&'a PeerId, u64), &'a Record>,
    record: &'a Record,
) -> Result<()> {
    let identity = (&record.version.peer, record.version.sequence);
    if let Some(previous) = canonical.insert(identity, record)
        && previous != record
    {
        bail!("the same version identity has contradictory records");
    }
    Ok(())
}

fn validate_owned_id(
    key: &[u8; 32],
    share: &ShareId,
    local: &PeerId,
    id: &VersionId,
) -> Result<()> {
    if &id.peer != local {
        return Ok(());
    }
    let expected = authenticate(
        key,
        &serde_json::to_vec(&("flocal-version-id-v2", &share.0, &id.peer.0, id.sequence))?,
    );
    if id.authenticator.as_deref() != Some(&expected) {
        bail!("peer supplied an invalid locally owned version identity");
    }
    Ok(())
}

fn object_path_matches(path: &Path, expected: &ObjectHash) -> Result<bool> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Ok(false);
    }
    let mut file = File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(ObjectHash::from_blake3(hasher.finalize()) == *expected)
}

fn same_file_snapshot(before: &fs::Metadata, after: &fs::Metadata) -> Result<bool> {
    if before.len() != after.len() || before.modified()? != after.modified()? {
        return Ok(false);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        return Ok(before.dev() == after.dev()
            && before.ino() == after.ino()
            && before.ctime() == after.ctime()
            && before.ctime_nsec() == after.ctime_nsec());
    }
    #[allow(unreachable_code)]
    Ok(true)
}

#[cfg(unix)]
fn root_identity(path: &Path) -> Result<RootIdentity> {
    use rustix::fs::{Mode, OFlags};

    let descriptor = rustix::fs::open(
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .with_context(|| {
        format!(
            "cannot open root directory {} without following links",
            path.display()
        )
    })?;
    let file = File::from(descriptor);
    file_root_identity(&file, path)
}

#[cfg(unix)]
fn file_root_identity(file: &File, path: &Path) -> Result<RootIdentity> {
    let metadata = file.metadata()?;
    if !metadata.is_dir() {
        bail!("configured root {} is not a directory", path.display());
    }
    Ok(RootIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

fn canonical_registration_root(root: &Path, identity: RootIdentity) -> Result<PathBuf> {
    let canonical = root.canonicalize()?;
    if root_identity(&canonical)? != identity {
        bail!("relationship root identity changed while resolving its path");
    }
    Ok(canonical)
}

struct ResolvedRegistrationPath {
    canonical: PathBuf,
    ancestor: File,
    missing: Vec<std::ffi::OsString>,
}

fn resolve_registration_path(path: &Path) -> Result<ResolvedRegistrationPath> {
    use rustix::fs::{Mode, OFlags};

    let mut existing = path.to_path_buf();
    let mut missing = Vec::new();
    loop {
        match fs::symlink_metadata(&existing) {
            Ok(_) => {
                let opened = File::from(rustix::fs::open(
                    &existing,
                    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
                    Mode::empty(),
                )?);
                let opened_identity = file_root_identity(&opened, &existing)?;
                let ancestor_path = existing.canonicalize()?;
                let ancestor = File::from(rustix::fs::open(
                    &ancestor_path,
                    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                    Mode::empty(),
                )?);
                if file_root_identity(&ancestor, &ancestor_path)? != opened_identity {
                    bail!("relationship root changed while resolving its existing ancestor");
                }
                missing.reverse();
                let mut canonical = ancestor_path;
                for component in &missing {
                    canonical.push(component);
                }
                return Ok(ResolvedRegistrationPath {
                    canonical,
                    ancestor,
                    missing,
                });
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let component = existing
            .file_name()
            .context("relationship root has no resolvable existing ancestor")?
            .to_owned();
        missing.push(component);
        if !existing.pop() {
            bail!("relationship root has no resolvable existing ancestor");
        }
    }
}

fn create_registration_tail(mut current: File, missing: &[std::ffi::OsString]) -> Result<File> {
    use rustix::fs::{Mode, OFlags, mkdirat, openat};

    let mode = Mode::RUSR
        | Mode::WUSR
        | Mode::XUSR
        | Mode::RGRP
        | Mode::WGRP
        | Mode::XGRP
        | Mode::ROTH
        | Mode::WOTH
        | Mode::XOTH;
    for component in missing {
        mkdirat(&current, component, mode).map_err(|error| {
            if error == rustix::io::Errno::EXIST {
                anyhow::anyhow!("relationship root changed while creating it")
            } else {
                error.into()
            }
        })?;
        current = File::from(openat(
            &current,
            component,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )?);
    }
    Ok(current)
}

fn validate_identity_values(
    root: &Path,
    actual: RootIdentity,
    device: &str,
    inode: &str,
) -> Result<()> {
    let expected = RootIdentity {
        device: device.parse().context("stored root device is invalid")?,
        inode: inode.parse().context("stored root inode is invalid")?,
    };
    if actual != expected {
        bail!(
            "configured root identity changed at {}; restore the original directory or deliberately remove and reinitialize the share",
            root.display()
        );
    }
    Ok(())
}

#[cfg(unix)]
fn path_bytes(path: &Path) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    path.as_os_str().as_bytes().to_vec()
}

#[cfg(unix)]
fn bytes_path(bytes: Vec<u8>) -> PathBuf {
    use std::os::unix::ffi::OsStringExt;
    std::ffi::OsString::from_vec(bytes).into()
}

#[cfg(unix)]
fn set_private_dir(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(unix)]
fn set_private_file(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(unix)]
fn open_private_regular_file(path: &Path, create_new: bool) -> Result<File> {
    use rustix::fs::{Mode, OFlags};
    use std::os::unix::fs::MetadataExt;

    let mut flags = OFlags::RDWR | OFlags::CLOEXEC | OFlags::NOFOLLOW;
    if create_new {
        flags |= OFlags::CREATE | OFlags::EXCL;
    } else {
        flags |= OFlags::CREATE;
    }
    let descriptor = rustix::fs::open(path, flags, Mode::RUSR | Mode::WUSR)
        .map_err(|error| std::io::Error::from_raw_os_error(error.raw_os_error()))?;
    let file = File::from(descriptor);
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.mode() & 0o077 != 0
    {
        bail!("{} is not an owned private regular file", path.display());
    }
    Ok(file)
}

#[cfg(not(unix))]
fn open_private_regular_file(path: &Path, create_new: bool) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    if create_new {
        options.create_new(true);
    } else {
        options.create(true);
    }
    Ok(options.open(path)?)
}

fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn recovery_row_totals(connection: &Connection, share: &ShareId) -> Result<(u64, u64)> {
    let mut statement = connection.prepare(
        "SELECT LENGTH(CAST(id AS BLOB)),LENGTH(path),
                LENGTH(CAST(winner_json AS BLOB)),LENGTH(CAST(loser_json AS BLOB)),
                LENGTH(CAST(created_ns AS BLOB)),
                COALESCE(LENGTH(CAST(conflict_json AS BLOB)),0)
         FROM conflicts WHERE share_id=?1",
    )?;
    let rows = statement.query_map([&share.0], |row| {
        Ok([
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, i64>(5)?,
        ])
    })?;
    let mut count = 0u64;
    let mut metadata = 0u64;
    for row in rows {
        count = count
            .checked_add(1)
            .context("recovery conflict count overflow")?;
        metadata = row?.into_iter().try_fold(
            metadata
                .checked_add(RECOVERY_ROW_OVERHEAD_BYTES)
                .context("recovery metadata byte overflow")?,
            |total, bytes| {
                total
                    .checked_add(u64::try_from(bytes).context("negative recovery field size")?)
                    .context("recovery metadata byte overflow")
            },
        )?;
    }
    Ok((count, metadata))
}

fn raw_conflict_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawConflictRow> {
    Ok(RawConflictRow {
        id: row.get(0)?,
        path: row.get(1)?,
        winner: row.get(2)?,
        loser: row.get(3)?,
        created_ns: row.get(4)?,
        document: row.get(5)?,
    })
}

fn stored_conflict_from_parts(
    (id, path, winner, loser, created, document): (
        String,
        Vec<u8>,
        String,
        String,
        String,
        Option<String>,
    ),
) -> Result<StoredConflict> {
    let legacy_winner: Record = serde_json::from_str(&winner)?;
    let legacy_loser: Record = serde_json::from_str(&loser)?;
    let conflict: Conflict = document
        .map(|json| serde_json::from_str(&json))
        .transpose()?
        .unwrap_or_else(|| {
            Conflict::whole_file(
                legacy_winner,
                legacy_loser,
                crate::merge::FallbackReason::Legacy,
            )
        });
    Ok(StoredConflict {
        id,
        path: RelativePath::from_bytes(path)?,
        winner: conflict.winner().clone(),
        loser: conflict.loser().clone(),
        resolution: conflict.resolution,
        base: conflict.base,
        inputs: conflict.inputs,
        merged: conflict.merged,
        hunks: conflict.hunks,
        created_ns: created.parse()?,
    })
}

fn raw_conflict_metadata_bytes(row: &RawConflictRow) -> Result<u64> {
    [
        row.id.len(),
        row.path.len(),
        row.winner.len(),
        row.loser.len(),
        row.created_ns.len(),
        row.document.as_ref().map_or(0, String::len),
        RECOVERY_ROW_OVERHEAD_BYTES as usize,
    ]
    .into_iter()
    .try_fold(0u64, |total, bytes| {
        total
            .checked_add(bytes as u64)
            .context("recovery metadata byte overflow")
    })
}

fn prune_selection_token(
    share: &ShareId,
    all: bool,
    conflicts: &[RawConflictRow],
) -> Result<String> {
    let mut hasher = prune_selection_hasher(share, all);
    for conflict in conflicts {
        hash_token_field(&mut hasher, &serde_json::to_vec(conflict)?);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn prune_selection_hasher(share: &ShareId, all: bool) -> blake3::Hasher {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"flocal-prune-selection-v1\0");
    hash_token_field(&mut hasher, share.0.as_bytes());
    hash_token_field(&mut hasher, if all { b"all" } else { b"selected" });
    hasher
}

fn hash_token_field(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn conflict_metadata_bytes(id: &str, conflict: &Conflict, document: &str) -> Result<u64> {
    let winner = serde_json::to_string(conflict.winner())?;
    let loser = serde_json::to_string(conflict.loser())?;
    [
        id.len(),
        conflict.path.as_bytes().len(),
        winner.len(),
        loser.len(),
        now_ns().to_string().len(),
        document.len(),
        RECOVERY_ROW_OVERHEAD_BYTES as usize,
    ]
    .into_iter()
    .try_fold(0u64, |total, bytes| {
        total
            .checked_add(bytes as u64)
            .context("recovery metadata byte overflow")
    })
}

fn decode_conflict(winner: &str, loser: &str, document: Option<&str>) -> Result<Conflict> {
    if let Some(document) = document {
        return Ok(serde_json::from_str(document)?);
    }
    Ok(Conflict::whole_file(
        serde_json::from_str(winner)?,
        serde_json::from_str(loser)?,
        crate::merge::FallbackReason::Legacy,
    ))
}

fn conflict_entries(conflict: &Conflict) -> impl Iterator<Item = &Entry> {
    conflict
        .inputs
        .iter()
        .map(|record| &record.version.entry)
        .chain(conflict.base.iter().map(|base| &base.entry))
        .chain(conflict.merged.iter().map(|record| &record.version.entry))
}

fn visit_entry_object(
    entry: &Entry,
    visit: &mut impl FnMut(&ObjectHash) -> Result<()>,
) -> Result<()> {
    if let Entry::File { hash, .. } = entry {
        visit(hash)?;
    }
    Ok(())
}

fn remember_conflict_object_sizes(
    objects: &mut HashMap<ObjectHash, u64>,
    conflict: &Conflict,
) -> Result<()> {
    for entry in conflict_entries(conflict) {
        if let Entry::File { hash, size, .. } = entry
            && let Some(previous) = objects.insert(hash.clone(), *size)
            && previous != *size
        {
            bail!("object {} has contradictory declared sizes", hash.as_str());
        }
    }
    Ok(())
}

fn forget_conflict_objects(objects: &mut HashMap<ObjectHash, u64>, conflict: &Conflict) {
    for entry in conflict_entries(conflict) {
        forget_entry_object(objects, entry);
    }
}

fn forget_entry_object(objects: &mut HashMap<ObjectHash, u64>, entry: &Entry) {
    if let Entry::File { hash, .. } = entry {
        objects.remove(hash);
    }
}

fn checked_object_size_sum(objects: &HashMap<ObjectHash, u64>) -> Result<u64> {
    objects.values().try_fold(0u64, |total, size| {
        total
            .checked_add(*size)
            .context("recovery object size overflow")
    })
}

fn insert_conflict_objects(
    connection: &Connection,
    table: &str,
    conflict: &Conflict,
) -> Result<()> {
    for entry in conflict_entries(conflict) {
        if let Entry::File { hash, size, .. } = entry {
            let size = i64::try_from(*size).context("object size exceeds SQLite limit")?;
            connection.execute(
                &format!("INSERT OR IGNORE INTO {table}(hash,size) VALUES(?1,?2)"),
                params![hash.as_str(), size],
            )?;
            let stored: i64 = connection.query_row(
                &format!("SELECT size FROM {table} WHERE hash=?1"),
                [hash.as_str()],
                |row| row.get(0),
            )?;
            if stored != size {
                bail!("object {} has contradictory declared sizes", hash.as_str());
            }
        }
    }
    Ok(())
}

fn insert_temp_hash(connection: &Connection, table: &str, hash: &ObjectHash) -> Result<()> {
    connection.execute(
        &format!("INSERT OR IGNORE INTO {table}(hash) VALUES(?1)"),
        [hash.as_str()],
    )?;
    Ok(())
}

fn enforce_all_prune_summary_bounds(
    conflicts: u64,
    summary_bytes: u64,
    maximum_conflicts: u64,
    maximum_summary_bytes: u64,
) -> Result<()> {
    if conflicts > maximum_conflicts || summary_bytes > maximum_summary_bytes {
        bail!(
            "all-conflict preview is too large; run `flocal conflicts list PATH --ids` and prune selected conflict IDs"
        );
    }
    Ok(())
}

fn temp_object_bytes(connection: &Connection, table: &str) -> Result<u64> {
    let mut statement = connection.prepare(&format!("SELECT size FROM {table}"))?;
    statement
        .query_map([], |row| row.get::<_, i64>(0))?
        .try_fold(0u64, |total, size| {
            total
                .checked_add(u64::try_from(size?).context("negative recovery object size")?)
                .context("recovery object size overflow")
        })
}

fn enforce_recovery_limit(
    kind: RecoveryLimitKind,
    current: u64,
    projected: u64,
    limit: u64,
) -> Result<()> {
    if projected > limit {
        return Err(RecoveryLimitExceeded {
            kind,
            current,
            projected,
            limit,
        }
        .into());
    }
    Ok(())
}

fn remember_planned_document(
    documents: &mut HashMap<String, String>,
    id: &str,
    document: &str,
) -> Result<bool> {
    match documents.get(id) {
        Some(previous) if previous == document => Ok(false),
        Some(_) => bail!("conflict ID collision for {id}"),
        None => {
            documents.insert(id.to_owned(), document.to_owned());
            Ok(true)
        }
    }
}

fn insert_conflict(connection: &Connection, share: &ShareId, conflict: &Conflict) -> Result<()> {
    let id = crate::reconcile::conflict_id(conflict);
    let document = serde_json::to_string(conflict)?;
    let existing: Option<(String, Option<String>)> = connection
        .query_row(
            "SELECT share_id,conflict_json FROM conflicts WHERE id=?1",
            [&id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    match existing {
        Some((stored_share, Some(stored))) if stored_share == share.0 && stored == document => {
            return Ok(());
        }
        Some(_) => bail!("conflict ID collision for {id}"),
        None => {}
    }
    connection.execute(
        "INSERT INTO conflicts(id,share_id,path,winner_json,loser_json,created_ns,conflict_json)
         VALUES(?1,?2,?3,?4,?5,?6,?7)",
        params![
            id,
            share.0,
            conflict.path.as_bytes(),
            serde_json::to_string(conflict.winner())?,
            serde_json::to_string(conflict.loser())?,
            now_ns().to_string(),
            document,
        ],
    )?;
    Ok(())
}

fn ensure_unsettled_limit(connection: &Connection, share: &ShareId) -> Result<()> {
    let (count, bytes): (i64, i64) = connection.query_row(
        "SELECT COUNT(*), COALESCE(SUM(LENGTH(path)),0) FROM unsettled_paths WHERE share_id=?1",
        [&share.0],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    if count > crate::sync::MAX_RECORDS_PER_SESSION as i64 {
        bail!("persistent watch readiness has too many unsettled paths");
    }
    // RelativePath JSON is an array of decimal bytes. Four encoded bytes per
    // raw byte plus 64 bytes of per-path frame/delimiter overhead is a
    // conservative upper bound used so every durable union is guaranteed to
    // fit the wire's metadata budget, even when each path needs its own frame.
    if bytes
        .saturating_mul(4)
        .saturating_add(count.saturating_mul(64))
        > crate::sync::MAX_METADATA_BYTES_PER_SESSION as i64
    {
        bail!("persistent watch readiness exceeds its cumulative metadata limit");
    }
    Ok(())
}

pub fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

pub fn file_record(
    path: RelativePath,
    peer: PeerId,
    sequence: u64,
    timestamp_ns: i64,
    seen: Vec<VersionId>,
    entry: Entry,
) -> Record {
    Record {
        path,
        version: Version {
            peer,
            sequence,
            id_authenticator: None,
            timestamp_ns,
            seen,
            merge_base: None,
            version_authenticator: None,
            base_authenticator: None,
            entry,
        },
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::process::Command;

    const PERMISSIVE_UMASK_STATE_DIR: &str = "FLOCAL_TEST_PERMISSIVE_UMASK_STATE_DIR";

    fn relationship(value: &str) -> RelationshipId {
        RelationshipId::parse(value.to_owned()).unwrap()
    }

    fn completed_connector(peer: &str, relationship: &str) -> PeerConfig {
        PeerConfig {
            peer_id: Some(PeerId(peer.into())),
            host: "peer-host".into(),
            remote_path: b"/remote/root".to_vec(),
            executable: "/usr/bin/flocal".into(),
            relationship: Some(self::relationship(relationship)),
        }
    }

    fn recovery_conflict() -> Result<Conflict> {
        Ok(Conflict::whole_file(
            file_record(
                RelativePath::from_bytes(b"conflicted".to_vec())?,
                PeerId("winner".into()),
                2,
                2,
                Vec::new(),
                Entry::Directory,
            ),
            file_record(
                RelativePath::from_bytes(b"conflicted".to_vec())?,
                PeerId("loser".into()),
                1,
                1,
                Vec::new(),
                Entry::Tombstone,
            ),
            crate::merge::FallbackReason::AbsentBase,
        ))
    }

    #[test]
    fn upgrade_marker_and_barrier_exclude_normal_state_users() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let state_dir = temp.path().join("state");
        let state = State::open(&state_dir)?;
        assert!(State::try_acquire_upgrade_barrier(&state_dir)?.is_none());
        drop(state);

        State::create_upgrade_pending(&state_dir)?;
        State::create_upgrade_pending(&state_dir)?;
        let error = match State::open(&state_dir) {
            Ok(_) => bail!("pending upgrade accepted a new state user"),
            Err(error) => error,
        };
        assert!(error.downcast_ref::<UpgradePending>().is_some());
        assert_eq!(
            error.to_string(),
            "upgrade is in progress; rerun `make install`"
        );
        State::remove_upgrade_pending(&state_dir)?;
        State::remove_upgrade_pending(&state_dir)?;

        let barrier = State::try_acquire_upgrade_barrier(&state_dir)?
            .context("barrier remained owned after State was dropped")?;
        let upgraded = State::open_for_upgrade(&state_dir, &barrier)?;
        drop(upgraded);
        drop(barrier);
        State::open(&state_dir)?;

        fs::write(state_dir.join(UPGRADE_PENDING_FILE), b"not empty")?;
        assert!(upgrade_pending(&state_dir).is_err());
        fs::remove_file(state_dir.join(UPGRADE_PENDING_FILE))?;
        fs::remove_file(state_dir.join(INSTALLATION_BARRIER_FILE))?;
        fs::create_dir(state_dir.join(INSTALLATION_BARRIER_FILE))?;
        assert!(State::open(&state_dir).is_err());
        Ok(())
    }

    #[test]
    fn legacy_upgrade_lock_reports_the_busy_share_root() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let absent = temp.path().join("absent-state");
        fs::create_dir(&absent)?;
        fs::set_permissions(&absent, fs::Permissions::from_mode(0o700))?;
        assert!(legacy_upgrade_inventory(&absent)?.shares.is_empty());
        Connection::open(absent.join("state.sqlite3"))?;
        assert!(legacy_upgrade_inventory(&absent)?.shares.is_empty());

        let root = temp.path().join("root");
        fs::create_dir(&root)?;
        let state_dir = temp.path().join("state");
        let state = State::open(&state_dir)?;
        let share = state.init_share(&root)?;
        let session = state.lock_share_session(&share)?;
        match State::try_acquire_legacy_upgrade_locks(&state_dir)? {
            UpgradeLockAttempt::Busy(path) => assert_eq!(path, root.canonicalize()?),
            UpgradeLockAttempt::Acquired(_) => bail!("upgrade ignored the active share session"),
        }
        drop(session);
        assert!(matches!(
            State::try_acquire_legacy_upgrade_locks(&state_dir)?,
            UpgradeLockAttempt::Acquired(_)
        ));
        let inventory = LegacyUpgradeInventory {
            shares: vec![(share.0, root.clone())],
            active_root: Some(root.clone()),
        };
        assert_eq!(inventory.contention_path(&state_dir), root);

        let invalid_locks = temp.path().join("invalid-locks");
        fs::create_dir(&invalid_locks)?;
        fs::set_permissions(&invalid_locks, fs::Permissions::from_mode(0o700))?;
        std::os::unix::fs::symlink("missing", invalid_locks.join("daemon.lock"))?;
        assert!(State::try_acquire_legacy_upgrade_locks(invalid_locks).is_err());
        Ok(())
    }

    #[cfg(feature = "e2e-test-hooks")]
    #[test]
    fn injected_upgrade_migration_failure_leaves_current_schema_openable() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let state_dir = temp.path().join("state");
        drop(State::open(&state_dir)?);
        fs::write(state_dir.join(".e2e-fail-state-migration"), b"")?;
        let error = match State::open(&state_dir) {
            Ok(_) => bail!("injected migration failure was ignored"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("injected state migration failure")
        );
        fs::remove_file(state_dir.join(".e2e-fail-state-migration"))?;
        State::open(&state_dir)?;
        Ok(())
    }

    #[test]
    fn failed_migration_rolls_back_schema_changes() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let state_dir = temp.path().join("state");
        fs::create_dir(&state_dir)?;
        fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o700))?;
        let database = state_dir.join("state.sqlite3");
        let connection = Connection::open(&database)?;
        connection.execute_batch(
            "CREATE TABLE shares (
                share_id TEXT PRIMARY KEY,
                root BLOB NOT NULL UNIQUE,
                sequence INTEGER NOT NULL DEFAULT 0,
                initial_complete INTEGER NOT NULL DEFAULT 0,
                peer_json TEXT
            );",
        )?;
        connection.execute(
            "INSERT INTO shares(share_id,root) VALUES('share-old',?1)",
            [path_bytes(&temp.path().join("missing-root"))],
        )?;
        drop(connection);

        assert!(State::open(&state_dir).is_err());
        let connection = Connection::open(&database)?;
        let columns: Vec<String> = connection
            .prepare("PRAGMA table_info(shares)")?
            .query_map([], |row| row.get(1))?
            .collect::<std::result::Result<_, _>>()?;
        assert!(!columns.contains(&"root_device".to_owned()));
        let sync_queue_exists = connection
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='sync_queue'",
                [],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        assert!(!sync_queue_exists);
        Ok(())
    }

    #[test]
    fn relationship_columns_migrate_and_legacy_peer_json_decodes() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let root = temp.path().join("root");
        fs::create_dir(&root)?;
        let identity = root_identity(&root)?;
        let state_dir = temp.path().join("state");
        fs::create_dir(&state_dir)?;
        fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o700))?;
        let connection = Connection::open(state_dir.join("state.sqlite3"))?;
        connection.execute_batch(
            "CREATE TABLE shares (
                share_id TEXT PRIMARY KEY,
                root BLOB NOT NULL UNIQUE,
                sequence INTEGER NOT NULL DEFAULT 0,
                initial_complete INTEGER NOT NULL DEFAULT 0,
                peer_json TEXT,
                bound_peer TEXT,
                root_device TEXT,
                root_inode TEXT,
                watch_enabled INTEGER NOT NULL DEFAULT 0,
                blocked_diagnostic TEXT,
                intent_generation INTEGER NOT NULL DEFAULT 0,
                recovery_budget_bytes INTEGER NOT NULL DEFAULT 10737418240
            );",
        )?;
        let legacy =
            r#"{"peer_id":"peer-old","host":"host","remote_path":[47],"executable":"/flocal"}"#;
        connection.execute(
            "INSERT INTO shares(share_id,root,peer_json,root_device,root_inode)
             VALUES('share-old',?1,?2,?3,?4)",
            params![
                path_bytes(&root),
                legacy,
                identity.device.to_string(),
                identity.inode.to_string()
            ],
        )?;
        drop(connection);

        let state = State::open(&state_dir)?;
        assert_eq!(
            state.endpoint_binding(&ShareId("share-old".into()))?,
            EndpointBinding::Connector(PeerConfig {
                peer_id: Some(PeerId("peer-old".into())),
                host: "host".into(),
                remote_path: b"/".to_vec(),
                executable: "/flocal".into(),
                relationship: None,
            })
        );
        let columns: Vec<String> = state
            .conn
            .prepare("PRAGMA table_info(shares)")?
            .query_map([], |row| row.get(1))?
            .collect::<std::result::Result<_, _>>()?;
        assert!(columns.contains(&"bound_relationship".to_owned()));
        assert!(columns.contains(&"removing_relationship".to_owned()));
        Ok(())
    }

    #[test]
    fn install_recovery_classification_is_exact_bounded_and_share_scoped() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let state_dir = temp.path().join("state");
        let root = temp.path().join("root");
        let other_root = temp.path().join("other-root");
        fs::create_dir(&root)?;
        fs::create_dir(&other_root)?;
        let mut state = State::open(&state_dir)?;
        let share = state.init_share(&root)?;
        let other_share = state.init_share(&other_root)?;
        let path = RelativePath::from_bytes(b"pending".to_vec())?;
        let record = file_record(
            path.clone(),
            PeerId("owner".into()),
            1,
            1,
            Vec::new(),
            Entry::Directory,
        );
        let (intent, created) = state.set_install_intent(&share, &[record])?;
        assert!(created);

        let recovery = state
            .install_recovery_intent(&share)?
            .context("install recovery intent is missing")?;
        assert_eq!(
            recovery.fingerprint,
            State::install_intent_fingerprint(&intent)?
        );
        assert_eq!(state.unclassified_install_intents()?.len(), 1);
        let wrong_fingerprint = "0".repeat(64);
        assert_ne!(recovery.fingerprint, wrong_fingerprint);
        assert!(!state.classify_install_intent_failure(
            &share,
            &wrong_fingerprint,
            "wrong intent",
        )?);

        let mut affected = state.enqueue_sync(Some(&share), SyncOperation::Recovery, None)?;
        let mut unrelated =
            state.enqueue_sync(Some(&other_share), SyncOperation::Maintenance, None)?;
        assert!(state.classify_install_intent_failure(
            &share,
            &recovery.fingerprint,
            &"é".repeat(3000),
        )?);
        let failure = state
            .install_intent_failure(&share)?
            .context("install recovery failure was not classified")?;
        assert_eq!(failure.fingerprint, recovery.fingerprint);
        assert_eq!(failure.diagnostic.len(), 4096);
        assert_eq!(
            state.managed_share(&share)?.blocked_diagnostic,
            Some(failure.diagnostic)
        );
        assert!(state.unclassified_install_intents()?.is_empty());

        let error = match affected.try_activate() {
            Err(error) => error,
            Ok(_) => bail!("classified share request was not canceled"),
        };
        assert!(error.downcast_ref::<QueueCancelled>().is_some());
        unrelated
            .try_activate()?
            .context("unrelated share did not remain eligible")?
            .finish()?;
        Ok(())
    }

    #[test]
    fn install_recovery_retry_and_intent_change_clear_only_their_classification() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let root = temp.path().join("root");
        fs::create_dir(&root)?;
        let mut state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        let path = RelativePath::from_bytes(b"pending".to_vec())?;
        let record = file_record(
            path.clone(),
            PeerId("owner".into()),
            1,
            1,
            Vec::new(),
            Entry::Directory,
        );
        state.set_install_intent(&share, &[record])?;
        let original = state
            .install_recovery_intent(&share)?
            .context("install recovery intent is missing")?;
        assert!(state.classify_install_intent_failure(
            &share,
            &original.fingerprint,
            "first failure",
        )?);

        let retry = state
            .begin_install_intent_retry(&share)?
            .context("classified intent disappeared before retry")?;
        assert_eq!(retry.fingerprint, original.fingerprint);
        assert!(state.install_intent_failure(&share)?.is_none());
        assert!(state.managed_share(&share)?.blocked_diagnostic.is_none());
        assert_eq!(state.unclassified_install_intents()?.len(), 1);

        assert!(state.classify_install_intent_failure(
            &share,
            &retry.fingerprint,
            "second failure",
        )?);
        state.set_blocked(&share, "newer unrelated diagnostic")?;
        state.mark_install_temp_creating(&share, &path)?;
        assert!(state.install_intent_failure(&share)?.is_none());
        assert_eq!(
            state.managed_share(&share)?.blocked_diagnostic.as_deref(),
            Some("newer unrelated diagnostic")
        );
        let changed = state
            .install_recovery_intent(&share)?
            .context("changed install recovery intent is missing")?;
        assert_ne!(changed.fingerprint, original.fingerprint);
        assert!(!state.classify_install_intent_failure(
            &share,
            &original.fingerprint,
            "stale failure",
        )?);
        Ok(())
    }

    #[test]
    fn install_recovery_classification_preserves_removal_identity() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let root = temp.path().join("root");
        fs::create_dir(&root)?;
        let mut state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        let config = completed_connector("peer", "relationship-recovery-removal");
        state.set_peer(&share, &config)?;
        let record = file_record(
            RelativePath::from_bytes(b"pending".to_vec())?,
            PeerId("owner".into()),
            1,
            1,
            Vec::new(),
            Entry::Directory,
        );
        state.set_install_intent(&share, &[record])?;
        let prepared = state.prepare_removal(&share, &EndpointBinding::Connector(config))?;
        let recovery = state
            .install_recovery_intent(&share)?
            .context("install recovery intent is missing")?;

        assert!(state.classify_install_intent_failure(
            &share,
            &recovery.fingerprint,
            "removal recovery failure",
        )?);
        let managed = state.managed_share(&share)?;
        assert_eq!(managed.removing_relationship, Some(prepared.relationship));
        assert_eq!(
            managed.blocked_diagnostic.as_deref(),
            Some("removal recovery failure")
        );
        state.begin_install_intent_retry(&share)?;
        assert!(state.managed_share(&share)?.blocked_diagnostic.is_none());
        Ok(())
    }

    #[test]
    fn legacy_install_intent_failure_columns_migrate() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let state_dir = temp.path().join("state");
        fs::create_dir(&state_dir)?;
        fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o700))?;
        let connection = Connection::open(state_dir.join("state.sqlite3"))?;
        connection.execute_batch(
            "CREATE TABLE install_intents (
                share_id TEXT PRIMARY KEY,
                records_json TEXT NOT NULL
            );",
        )?;
        drop(connection);

        let state = State::open(&state_dir)?;
        let columns: Vec<String> = state
            .conn
            .prepare("PRAGMA table_info(install_intents)")?
            .query_map([], |row| row.get(1))?
            .collect::<std::result::Result<_, _>>()?;
        assert!(columns.contains(&"failure_fingerprint".to_owned()));
        assert!(columns.contains(&"failure_diagnostic".to_owned()));
        let empty_intent = serde_json::to_string(&InstallIntent {
            records: Vec::new(),
            conflicts: Vec::new(),
            temps: Vec::new(),
            managed_generation: None,
        })?;
        assert!(
            state
                .conn
                .execute(
                    "INSERT INTO install_intents(
                share_id,records_json,failure_fingerprint,failure_diagnostic
             ) VALUES('invalid',?1,?2,NULL)",
                    params![empty_intent, "0".repeat(64)],
                )
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn connector_registration_is_durable_retryable_and_cas_completed() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let root = temp.path().join("root");
        fs::create_dir(&root)?;
        let state_dir = temp.path().join("state");
        let mut state = State::open(&state_dir)?;
        let share = state.init_share(&root)?;
        let prepared = state.prepare_connector_registration(
            &share,
            &EndpointBinding::Unpaired,
            "host",
            b"/remote",
            "/first/flocal",
        )?;
        assert!(prepared.peer_id.is_none());
        let durable_relationship = prepared.relationship.clone().unwrap();
        drop(state);

        let mut state = State::open(&state_dir)?;
        assert_eq!(
            state.endpoint_binding(&share)?,
            EndpointBinding::Connector(prepared.clone())
        );
        let retry = state.prepare_connector_registration(
            &share,
            &EndpointBinding::Connector(prepared.clone()),
            "host",
            b"/remote",
            "/newly-discovered/flocal",
        )?;
        assert_eq!(retry, prepared);
        assert_eq!(retry.relationship, Some(durable_relationship));
        assert!(
            state
                .prepare_connector_registration(
                    &share,
                    &EndpointBinding::Connector(prepared.clone()),
                    "host",
                    b"/different",
                    "/first/flocal",
                )
                .is_err()
        );

        let mut stale = prepared.clone();
        stale.host = "other".into();
        assert!(
            state
                .complete_connector_registration(&share, &stale, &PeerId("responder".into()))
                .is_err()
        );
        let completed = state.complete_connector_registration(
            &share,
            &prepared,
            &PeerId("responder".into()),
        )?;
        assert_eq!(completed.peer_id, Some(PeerId("responder".into())));
        assert_eq!(
            state.prepare_connector_registration(
                &share,
                &EndpointBinding::Connector(completed.clone()),
                "host",
                b"/remote",
                "/another/flocal",
            )?,
            completed
        );
        assert_eq!(
            state.complete_connector_registration(
                &share,
                &completed,
                &PeerId("responder".into())
            )?,
            completed
        );
        assert_eq!(
            state.complete_connector_registration(
                &share,
                &prepared,
                &PeerId("responder".into())
            )?,
            completed
        );
        assert!(
            state
                .complete_connector_registration(&share, &prepared, &PeerId("other".into()))
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn binding_decode_rejects_corrupt_durable_roles() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let root = temp.path().join("root");
        fs::create_dir(&root)?;
        let mut state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;

        state.conn.execute(
            "UPDATE shares SET peer_json='not json' WHERE share_id=?1",
            [&share.0],
        )?;
        assert!(state.endpoint_binding(&share).is_err());
        assert!(state.managed_share(&share).is_err());

        state.conn.execute(
            "UPDATE shares SET peer_json=NULL,bound_peer='connector',bound_relationship='bad;id'
             WHERE share_id=?1",
            [&share.0],
        )?;
        assert!(state.endpoint_binding(&share).is_err());
        assert!(state.managed_share(&share).is_err());

        state.conn.execute(
            "UPDATE shares SET bound_peer=NULL,bound_relationship='relationship-orphaned'
             WHERE share_id=?1",
            [&share.0],
        )?;
        assert!(state.endpoint_binding(&share).is_err());
        assert!(state.managed_share(&share).is_err());

        state.conn.execute(
            "UPDATE shares SET bound_relationship=NULL,removing_relationship='bad;id'
             WHERE share_id=?1",
            [&share.0],
        )?;
        assert!(state.endpoint_binding(&share).is_err());
        assert!(state.managed_share(&share).is_err());

        state.conn.execute(
            "UPDATE shares SET removing_relationship=NULL WHERE share_id=?1",
            [&share.0],
        )?;
        let config = completed_connector("responder", "relationship-corrupt");
        state.set_peer(&share, &config)?;
        state.conn.execute(
            "UPDATE shares SET bound_peer='connector',removing_relationship=NULL WHERE share_id=?1",
            [&share.0],
        )?;
        assert!(state.endpoint_binding(&share).is_err());
        assert!(state.peer(&share).is_err());
        assert!(state.managed_share(&share).is_err());
        Ok(())
    }

    #[test]
    fn incomplete_connector_can_only_be_abandoned_locally() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let root = temp.path().join("root");
        fs::create_dir(&root)?;
        let mut state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        let config = state.prepare_connector_registration(
            &share,
            &EndpointBinding::Unpaired,
            "host",
            b"/remote",
            "/flocal",
        )?;
        let prepared = state.prepare_removal(&share, &EndpointBinding::Connector(config))?;
        assert!(state.finalize_connector_removal(&prepared).is_err());
        state.finalize_local_removal(&prepared)?;
        assert_eq!(state.endpoint_binding(&share)?, EndpointBinding::Unpaired);
        Ok(())
    }

    #[test]
    fn connector_registration_rejects_a_replaced_local_root_before_persisting() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let root = temp.path().join("root");
        fs::create_dir(&root)?;
        let mut state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        fs::rename(&root, temp.path().join("original-root"))?;
        fs::create_dir(&root)?;
        assert!(
            state
                .prepare_connector_registration(
                    &share,
                    &EndpointBinding::Unpaired,
                    "host",
                    b"/remote",
                    "/flocal",
                )
                .is_err()
        );
        assert_eq!(state.endpoint_binding(&share)?, EndpointBinding::Unpaired);
        Ok(())
    }

    #[test]
    fn finalization_clears_fresh_sync_state_and_preserves_recovery_metadata() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let root = temp.path().join("root");
        fs::create_dir(&root)?;
        let mut state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        let config = completed_connector("responder", "relationship-remove");
        state.set_peer(&share, &config)?;
        state.add_conflicts(&share, &[recovery_conflict()?])?;
        let record = file_record(
            RelativePath::from_bytes(b"old".to_vec())?,
            PeerId("owner".into()),
            1,
            1,
            Vec::new(),
            Entry::Directory,
        );
        state.replace_records(&share, std::slice::from_ref(&record))?;
        state.conn.execute(
            "INSERT INTO shared_heads(share_id,path,base_json) VALUES(?1,?2,'{}')",
            params![share.0, b"old".as_slice()],
        )?;
        state.conn.execute(
            "INSERT INTO unsettled_paths(share_id,path) VALUES(?1,?2)",
            params![share.0, b"old".as_slice()],
        )?;
        state.conn.execute(
            "INSERT INTO pending_objects(share_id,hash,provenance)
             VALUES(?1,?2,'generated_local')",
            params![share.0, "a".repeat(64)],
        )?;
        state.conn.execute(
            "UPDATE shares SET sequence=41,initial_complete=1,watch_enabled=1,
             recovery_budget_bytes=123456 WHERE share_id=?1",
            [&share.0],
        )?;
        let identity = state.expected_root_identity(&share)?;
        let generation = state.watch_intent_generation(&share)?;
        let prepared = state.prepare_removal(&share, &EndpointBinding::Connector(config))?;
        assert!(state.ensure_not_removing(&share).is_err());
        state.set_removal_diagnostic(&share, &prepared.relationship, &"é".repeat(5000))?;
        let diagnostic = state.managed_share(&share)?.blocked_diagnostic.unwrap();
        assert_eq!(diagnostic.len(), 4096);
        assert_eq!(diagnostic.chars().count(), 2048);
        state.finalize_connector_removal(&prepared)?;

        assert_eq!(state.endpoint_binding(&share)?, EndpointBinding::Unpaired);
        let row: (i64, i64, i64, i64, Option<String>, Option<String>) = state.conn.query_row(
            "SELECT sequence,initial_complete,watch_enabled,recovery_budget_bytes,
                    removing_relationship,blocked_diagnostic FROM shares WHERE share_id=?1",
            [&share.0],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )?;
        assert_eq!(row, (41, 0, 0, 123456, None, None));
        assert_eq!(state.expected_root_identity(&share)?, identity);
        assert!(state.watch_intent_generation(&share)? >= generation + 2);
        for table in [
            "records",
            "shared_heads",
            "unsettled_paths",
            "pending_objects",
        ] {
            let count: i64 = state.conn.query_row(
                &format!("SELECT COUNT(*) FROM {table} WHERE share_id=?1"),
                [&share.0],
                |row| row.get(0),
            )?;
            assert_eq!(count, 0, "{table}");
        }
        assert_eq!(state.conflicts(&share)?.len(), 1);
        Ok(())
    }

    #[test]
    fn locked_finalization_refuses_a_durable_install_journal() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let root = temp.path().join("root");
        fs::create_dir(&root)?;
        let mut state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        let config = completed_connector("responder", "relationship-install");
        state.set_peer(&share, &config)?;
        let prepared =
            state.prepare_removal(&share, &EndpointBinding::Connector(config.clone()))?;
        state.conn.execute(
            "INSERT INTO install_intents(share_id,records_json) VALUES(?1,?2)",
            params![
                share.0,
                serde_json::to_string(&InstallIntent {
                    records: Vec::new(),
                    conflicts: Vec::new(),
                    temps: Vec::new(),
                    managed_generation: None,
                })?
            ],
        )?;
        assert!(state.finalize_connector_removal_locked(&prepared).is_err());
        assert_eq!(
            state.endpoint_binding(&share)?,
            EndpointBinding::Connector(config)
        );
        assert_eq!(
            state.removing_relationship(&share)?,
            Some(prepared.relationship)
        );
        Ok(())
    }

    #[test]
    fn removal_preparation_and_finalization_are_exact_snapshot_cas_operations() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let root = temp.path().join("root");
        fs::create_dir(&root)?;
        let mut state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        let config = completed_connector("responder", "relationship-cas");
        state.set_peer(&share, &config)?;
        let mut stale = config.clone();
        stale.host = "stale-host".into();
        assert!(
            state
                .prepare_removal(&share, &EndpointBinding::Connector(stale))
                .is_err()
        );
        assert_eq!(state.removing_relationship(&share)?, None);

        let prepared = state.prepare_removal(&share, &EndpointBinding::Connector(config))?;
        assert!(state.detach_incoming_relationship(&prepared).is_err());
        assert_eq!(state.prepare_removal(&share, &prepared.binding)?, prepared);
        let mut wrong_marker = prepared.clone();
        wrong_marker.relationship = relationship("relationship-wrong-marker");
        assert!(state.finalize_local_removal(&wrong_marker).is_err());
        assert_eq!(
            state.removing_relationship(&share)?,
            Some(prepared.relationship)
        );
        Ok(())
    }

    #[test]
    fn completed_removal_finalization_is_idempotent() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let root = temp.path().join("root");
        fs::create_dir(&root)?;
        let state_dir = temp.path().join("state");
        let mut first = State::open(&state_dir)?;
        let share = first.init_share(&root)?;
        let config = completed_connector("responder", "relationship-concurrent-remove");
        first.set_peer(&share, &config)?;
        let prepared = first.prepare_removal(&share, &EndpointBinding::Connector(config))?;

        let mut concurrent = State::open(&state_dir)?;
        concurrent.finalize_local_removal(&prepared)?;
        first.finalize_connector_removal(&prepared)?;
        assert_eq!(first.endpoint_binding(&share)?, EndpointBinding::Unpaired);
        assert_eq!(first.removing_relationship(&share)?, None);
        Ok(())
    }

    #[test]
    fn incoming_removal_is_absent_idempotent_and_does_not_create_share_locks() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let mut state = State::open(temp.path().join("state"))?;
        let missing = ShareId("share-missing".into());
        assert!(!state.dir.join("locks").exists());
        assert_eq!(
            state.prepare_incoming_removal(
                &missing,
                &PeerId("connector".into()),
                &relationship("relationship-missing")
            )?,
            IncomingRemoval::Absent
        );
        assert!(!state.dir.join("locks").exists());

        let root = temp.path().join("remote-root");
        let share = ShareId("share-incoming".into());
        let connector = PeerId("connector".into());
        let relationship = relationship("relationship-incoming");
        assert_eq!(
            state.register_relationship(&share, &root, &connector, &relationship)?,
            RegistrationOutcome { prior_share: None }
        );
        assert_eq!(
            state.prepare_incoming_removal(
                &share,
                &connector,
                &self::relationship("relationship-other")
            )?,
            IncomingRemoval::Absent
        );
        assert!(
            state
                .prepare_incoming_removal(&share, &PeerId("wrong".into()), &relationship)
                .is_err()
        );
        let IncomingRemoval::Prepared(prepared) =
            state.prepare_incoming_removal(&share, &connector, &relationship)?
        else {
            panic!("matching incoming removal must prepare");
        };
        assert!(state.finalize_connector_removal(&prepared).is_err());
        assert_eq!(
            state.prepare_incoming_removal(&share, &connector, &relationship)?,
            IncomingRemoval::Prepared(prepared.clone())
        );
        state.detach_incoming_relationship(&prepared)?;
        assert_eq!(
            state.prepare_incoming_removal(&share, &connector, &relationship)?,
            IncomingRemoval::Absent
        );
        Ok(())
    }

    #[cfg(feature = "e2e-test-hooks")]
    #[test]
    fn e2e_legacy_fixture_changes_only_completed_relationship_ids() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let mut state = State::open(temp.path().join("state"))?;

        let connector_root = temp.path().join("connector-root");
        std::fs::create_dir(&connector_root)?;
        let connector_share = state.init_share(&connector_root)?;
        state.set_peer(
            &connector_share,
            &completed_connector("responder", "relationship-connector"),
        )?;
        assert!(
            state
                .e2e_assert_relationship_legacy(&connector_share)
                .is_err()
        );
        state.e2e_make_relationship_legacy(&connector_share)?;
        state.e2e_assert_relationship_legacy(&connector_share)?;
        assert_eq!(state.peer(&connector_share)?.unwrap().relationship, None);
        assert!(
            state
                .e2e_make_relationship_legacy(&connector_share)
                .is_err()
        );

        let responder_root = temp.path().join("responder-root");
        std::fs::create_dir(&responder_root)?;
        let responder_share = ShareId::generate();
        state.register_relationship(
            &responder_share,
            &responder_root,
            &PeerId("connector".into()),
            &relationship("relationship-responder"),
        )?;
        state.e2e_make_relationship_legacy(&responder_share)?;
        state.e2e_assert_relationship_legacy(&responder_share)?;
        assert!(matches!(
            state.endpoint_binding(&responder_share)?,
            EndpointBinding::Responder {
                relationship: None,
                ..
            }
        ));
        assert!(
            state
                .e2e_make_relationship_legacy(&ShareId("share-missing".into()))
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn relationship_registration_is_exact_and_remaps_only_retained_state() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let root = temp.path().join("retained-root");
        fs::create_dir(&root)?;
        let mut state = State::open(temp.path().join("state"))?;
        let prior = state.init_share(&root)?;
        state.add_conflicts(&prior, &[recovery_conflict()?])?;
        state.conn.execute(
            "UPDATE shares SET sequence=17,recovery_budget_bytes=98765 WHERE share_id=?1",
            [&prior.0],
        )?;
        let identity = state.expected_root_identity(&prior)?;
        let incoming = ShareId("share-remapped".into());
        let peer = PeerId("connector".into());
        let relationship = relationship("relationship-remapped");
        assert_eq!(
            state.register_relationship(&incoming, &root, &peer, &relationship)?,
            RegistrationOutcome {
                prior_share: Some(prior.clone())
            }
        );
        assert!(state.endpoint_binding(&prior).is_err());
        assert_eq!(
            state.endpoint_binding(&incoming)?,
            EndpointBinding::Responder {
                peer: peer.clone(),
                relationship: Some(relationship.clone())
            }
        );
        assert_eq!(state.conflicts(&incoming)?.len(), 1);
        assert_eq!(state.expected_root_identity(&incoming)?, identity);
        let retained: (i64, i64) = state.conn.query_row(
            "SELECT sequence,recovery_budget_bytes FROM shares WHERE share_id=?1",
            [&incoming.0],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(retained, (17, 98765));
        assert_eq!(
            state.register_relationship(&incoming, &root, &peer, &relationship)?,
            RegistrationOutcome { prior_share: None }
        );
        assert!(
            state
                .register_relationship(&incoming, &root, &PeerId("different".into()), &relationship)
                .is_err()
        );

        let dirty_root = temp.path().join("dirty-root");
        fs::create_dir(&dirty_root)?;
        let dirty = state.init_share(&dirty_root)?;
        state.conn.execute(
            "INSERT INTO records(share_id,path,version_json) VALUES(?1,?2,'{}')",
            params![dirty.0, b"old".as_slice()],
        )?;
        assert!(
            state
                .register_relationship(
                    &ShareId("share-dirty-remap".into()),
                    &dirty_root,
                    &peer,
                    &self::relationship("relationship-dirty")
                )
                .is_err()
        );
        assert_eq!(state.endpoint_binding(&dirty)?, EndpointBinding::Unpaired);

        let replaced_root = temp.path().join("replaced-root");
        fs::create_dir(&replaced_root)?;
        let replaced_share = state.init_share(&replaced_root)?;
        let original = temp.path().join("original-root-moved");
        fs::rename(&replaced_root, &original)?;
        fs::create_dir(&replaced_root)?;
        assert!(
            state
                .register_relationship(
                    &ShareId("share-replacement".into()),
                    &replaced_root,
                    &peer,
                    &self::relationship("relationship-replacement")
                )
                .is_err()
        );
        assert_eq!(
            state.endpoint_binding(&replaced_share)?,
            EndpointBinding::Unpaired
        );

        let missing_parent = temp.path().join("missing-parent");
        let missing_alias = temp.path().join("missing-alias");
        fs::create_dir(&missing_parent)?;
        std::os::unix::fs::symlink(&missing_parent, &missing_alias)?;
        let missing_root = missing_alias.join("missing-root");
        fs::create_dir(&missing_root)?;
        let missing_share = state.init_share(&missing_root)?;
        fs::remove_dir(&missing_root)?;
        assert!(
            state
                .register_relationship(
                    &ShareId("share-missing-root".into()),
                    &missing_root,
                    &peer,
                    &self::relationship("relationship-missing-root")
                )
                .is_err()
        );
        assert!(!missing_root.exists());
        assert_eq!(
            state.endpoint_binding(&missing_share)?,
            EndpointBinding::Unpaired
        );
        Ok(())
    }

    #[test]
    fn opened_root_identity_cannot_be_retargeted_to_another_share() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let first_root = temp.path().join("first");
        let second_root = temp.path().join("second");
        fs::create_dir(&first_root)?;
        fs::create_dir(&second_root)?;
        let state = State::open(temp.path().join("state"))?;
        let first_share = state.init_share(&first_root)?;
        let second_share = state.init_share(&second_root)?;
        let first_stored_root = state.root_for(&first_share)?;
        let opened_identity = root_identity(&first_root)?;

        fs::rename(&first_root, temp.path().join("first-moved"))?;
        std::os::unix::fs::symlink(&second_root, &first_root)?;

        assert!(state.find_share_by_exact_root(&first_root).is_err());
        assert_eq!(
            state
                .find_share_by_exact_root_identity(&first_stored_root, opened_identity)?
                .0,
            first_share
        );
        assert_ne!(first_share, second_share);
        assert!(canonical_registration_root(&first_root, opened_identity).is_err());
        Ok(())
    }

    #[test]
    fn exact_root_selection_accepts_equivalent_canonical_paths() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let real_parent = temp.path().join("real");
        let root = real_parent.join("root");
        let nested = root.join("nested");
        fs::create_dir_all(&nested)?;
        let alias_parent = temp.path().join("alias");
        std::os::unix::fs::symlink(&real_parent, &alias_parent)?;
        let state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&alias_parent.join("root"))?;

        assert_eq!(state.find_share_by_exact_root(&root)?.0, share);
        assert_eq!(
            state
                .find_share_by_exact_root(&alias_parent.join("root"))?
                .0,
            share
        );
        assert_eq!(state.find_share_by_exact_root(&nested.join(".."))?.0, share);
        Ok(())
    }

    #[test]
    fn registration_creation_stays_with_its_opened_ancestor() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let first = temp.path().join("first");
        let second = temp.path().join("second");
        let alias = temp.path().join("alias");
        fs::create_dir(&first)?;
        fs::create_dir(&second)?;
        std::os::unix::fs::symlink(&first, &alias)?;

        let requested = alias.join("root");
        let resolved = resolve_registration_path(&requested)?;
        let expected = resolved.canonical.clone();
        fs::remove_file(&alias)?;
        std::os::unix::fs::symlink(&second, &alias)?;
        fs::create_dir(second.join("root"))?;

        let created = create_registration_tail(resolved.ancestor, &resolved.missing)?;
        let created_identity = file_root_identity(&created, &expected)?;
        assert!(first.join("root").is_dir());
        assert_ne!(root_identity(&requested)?, created_identity);
        assert!(canonical_registration_root(&requested, created_identity).is_err());

        let injection_target = temp.path().join("injection-target");
        fs::create_dir(&injection_target)?;
        let injected = first.join("injected").join("root");
        let resolved = resolve_registration_path(&injected)?;
        std::os::unix::fs::symlink(&injection_target, first.join("injected"))?;
        assert!(create_registration_tail(resolved.ancestor, &resolved.missing).is_err());
        assert!(!injection_target.join("root").exists());
        Ok(())
    }

    #[test]
    fn removal_collection_cannot_race_another_shares_uncommitted_object() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let first_root = temp.path().join("first");
        let second_root = temp.path().join("second");
        fs::create_dir(&first_root)?;
        fs::create_dir(&second_root)?;
        let state_dir = temp.path().join("state");
        let mut state = State::open(&state_dir)?;
        let first_share = state.init_share(&first_root)?;
        let second_share = state.init_share(&second_root)?;
        let connector = completed_connector("responder", "relationship-collector");
        state.set_peer(&first_share, &connector)?;
        let removal =
            state.prepare_removal(&first_share, &EndpointBinding::Connector(connector))?;

        let source = temp.path().join("captured");
        fs::write(&source, b"captured before its record commits")?;
        let (hash, size) = state.store_object(File::open(&source)?)?;
        let object_path = state.object_path(&hash);
        let mut owner = state.enqueue_sync(None, SyncOperation::Maintenance, None)?;
        let held_sync = owner
            .try_activate()?
            .context("maintenance owner did not activate")?;
        let (queued_tx, queued_rx) = std::sync::mpsc::sync_channel(1);
        let removal_copy = removal.clone();
        let state_dir_copy = state_dir.clone();
        let remover = std::thread::spawn(move || -> Result<()> {
            let mut remover = State::open(state_dir_copy)?;
            let request =
                remover.enqueue_sync(Some(&removal_copy.share), SyncOperation::Removal, None)?;
            queued_tx.send(()).expect("queue observer remains alive");
            let permit = request.wait(|| false, |_| Ok(()))?;
            remover.finalize_connector_removal_locked(&removal_copy)?;
            permit.finish()
        });
        queued_rx.recv()?;
        assert!(object_path.exists());

        state.replace_records(
            &second_share,
            &[file_record(
                RelativePath::from_bytes(b"captured".to_vec())?,
                PeerId("owner".into()),
                1,
                1,
                Vec::new(),
                Entry::File {
                    hash,
                    size,
                    executable: false,
                },
            )],
        )?;
        held_sync.finish()?;
        remover.join().expect("removal thread panicked")?;
        assert!(object_path.exists());
        Ok(())
    }

    #[cfg(feature = "e2e-test-hooks")]
    #[test]
    fn global_contention_hook_is_one_shot_and_rejects_a_symlink() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let state_dir = temp.path().join("state");
        let state = State::open(&state_dir)?;
        let _held = state.lock_global_sync()?;
        let marker = state_dir.join(".e2e-observe-global-contention");
        let observed = state_dir.join(".e2e-global-contention-observed");

        fs::write(&marker, [])?;
        assert!(State::open(&state_dir)?.lock_global_sync().is_err());
        assert!(!marker.exists());
        assert!(observed.is_file());

        fs::remove_file(&observed)?;
        std::os::unix::fs::symlink("sync.lock", &marker)?;
        let error = State::open(&state_dir)?
            .lock_global_sync()
            .expect_err("a symlinked E2E marker must be rejected");
        assert!(format!("{error:#}").contains("not an empty regular file"));
        Ok(())
    }

    #[test]
    fn legacy_registration_only_acknowledges_an_existing_null_incarnation() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let root = temp.path().join("legacy-root");
        fs::create_dir(&root)?;
        let mut state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        state.conn.execute(
            "UPDATE shares SET bound_peer='legacy-peer' WHERE share_id=?1",
            [&share.0],
        )?;
        state.acknowledge_legacy_registration(&share, &root, &PeerId("legacy-peer".into()))?;
        assert!(
            state
                .acknowledge_legacy_registration(&share, &root, &PeerId("wrong".into()))
                .is_err()
        );
        assert!(
            state
                .acknowledge_legacy_registration(
                    &ShareId("missing".into()),
                    &root,
                    &PeerId("legacy-peer".into())
                )
                .is_err()
        );
        let removal_relationship = relationship("relationship-legacy-removal");
        assert!(
            state
                .prepare_incoming_removal(&share, &PeerId("wrong".into()), &removal_relationship)
                .is_err()
        );
        let IncomingRemoval::Prepared(prepared) = state.prepare_incoming_removal(
            &share,
            &PeerId("legacy-peer".into()),
            &removal_relationship,
        )?
        else {
            panic!("legacy relationship must prepare for its matching peer");
        };
        assert_eq!(prepared.relationship, removal_relationship);
        state.detach_incoming_relationship(&prepared)?;
        Ok(())
    }

    #[test]
    fn planned_conflict_ids_are_exactly_idempotent() -> Result<()> {
        let mut documents = HashMap::new();
        assert!(remember_planned_document(&mut documents, "short", "first")?);
        assert!(!remember_planned_document(
            &mut documents,
            "short",
            "first"
        )?);
        assert!(remember_planned_document(&mut documents, "short", "second").is_err());
        Ok(())
    }

    #[test]
    fn every_recovery_limit_kind_is_typed() {
        for kind in [
            RecoveryLimitKind::BudgetBytes,
            RecoveryLimitKind::ConflictCount,
            RecoveryLimitKind::MetadataBytes,
        ] {
            let error = enforce_recovery_limit(kind, 1, 2, 1).unwrap_err();
            assert_eq!(
                error.downcast_ref::<RecoveryLimitExceeded>().unwrap().kind,
                kind
            );
        }
    }

    #[test]
    fn full_prune_summary_bounds_are_exact() {
        assert!(
            enforce_all_prune_summary_bounds(
                MAX_ALL_PRUNE_SUMMARIES,
                MAX_ALL_PRUNE_SUMMARY_BYTES,
                MAX_ALL_PRUNE_SUMMARIES,
                MAX_ALL_PRUNE_SUMMARY_BYTES,
            )
            .is_ok()
        );
        assert!(
            enforce_all_prune_summary_bounds(
                MAX_ALL_PRUNE_SUMMARIES + 1,
                0,
                MAX_ALL_PRUNE_SUMMARIES,
                MAX_ALL_PRUNE_SUMMARY_BYTES,
            )
            .is_err()
        );
        assert!(
            enforce_all_prune_summary_bounds(
                0,
                MAX_ALL_PRUNE_SUMMARY_BYTES + 1,
                MAX_ALL_PRUNE_SUMMARIES,
                MAX_ALL_PRUNE_SUMMARY_BYTES,
            )
            .is_err()
        );

        let id = "\"\n\\".repeat(128);
        let path = RelativePath::from_bytes(vec![0xff; 4096]).unwrap();
        let summary = RecoveryPruneConflict {
            id: id.clone(),
            path,
        };
        let actual = serde_json::to_string_pretty(&summary).unwrap().len() as u64;
        let conservative = 6 * id.len() as u64 + 16 * 4096 + 128;
        assert!(actual <= conservative);
    }

    #[test]
    fn recovery_metadata_counts_utf8_bytes() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let root = temp.path().join("root");
        fs::create_dir(&root)?;
        let mut state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        let conflict = Conflict::whole_file(
            file_record(
                RelativePath::from_bytes(b"unicode".to_vec())?,
                PeerId("peer-snow-雪".into()),
                2,
                2,
                Vec::new(),
                Entry::Directory,
            ),
            file_record(
                RelativePath::from_bytes(b"unicode".to_vec())?,
                PeerId("peer-cafe-é".into()),
                1,
                1,
                Vec::new(),
                Entry::Tombstone,
            ),
            crate::merge::FallbackReason::AbsentBase,
        );
        state.add_conflicts(&share, std::slice::from_ref(&conflict))?;
        let id = crate::reconcile::conflict_id(&conflict);
        let rows = state.raw_conflicts_for_prune(&share, &[id])?;
        let expected = raw_conflict_metadata_bytes(&rows[0])?;
        assert_eq!(recovery_row_totals(&state.conn, &share)?, (1, expected));
        Ok(())
    }

    #[test]
    fn legacy_conflict_rows_remain_inspectable_and_prunable() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let root = temp.path().join("root");
        fs::create_dir(&root)?;
        let mut state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        let winner = file_record(
            RelativePath::from_bytes(b"legacy-conflict".to_vec())?,
            PeerId("winner".into()),
            2,
            2,
            Vec::new(),
            Entry::Directory,
        );
        let loser = file_record(
            RelativePath::from_bytes(b"legacy-conflict".to_vec())?,
            PeerId("loser".into()),
            1,
            1,
            Vec::new(),
            Entry::Tombstone,
        );
        state.conn.execute(
            "INSERT INTO conflicts(id,share_id,path,winner_json,loser_json,created_ns,conflict_json)
             VALUES('legacy-id',?1,?2,?3,?4,'1',NULL)",
            params![
                share.0,
                b"legacy-conflict".as_slice(),
                serde_json::to_string(&winner)?,
                serde_json::to_string(&loser)?
            ],
        )?;
        let stored = state.conflict(&share, "legacy-id")?;
        assert_eq!(stored.winner, winner);
        assert_eq!(stored.loser, loser);
        #[cfg(feature = "e2e-test-hooks")]
        {
            fs::write(state.dir.join(".e2e-recovery-conflict-limit"), b"0")?;
            fs::write(state.dir.join(".e2e-recovery-metadata-limit"), b"0")?;
            let usage = state.recovery_usage(&share)?;
            assert!(usage.over_conflict_limit);
            assert!(usage.over_metadata_limit);
        }
        let plan = state.recovery_prune_plan(&share, &[])?;
        assert_eq!(plan.conflicts.len(), 1);
        state.prune_recovery(&share, &[], &plan.selection_token)?;
        assert!(state.conflicts(&share)?.is_empty());
        Ok(())
    }

    #[test]
    fn canonical_snapshot_rejects_reused_owner_sequence() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let root = temp.path().join("root");
        fs::create_dir(&root)?;
        let state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        let original = file_record(
            RelativePath::from_bytes(b"original".to_vec())?,
            PeerId("foreign".into()),
            7,
            now_ns(),
            Vec::new(),
            Entry::Directory,
        );
        state.validate_remote_records(
            &share,
            std::slice::from_ref(&original),
            std::slice::from_ref(&original),
        )?;

        let mut contradictory = original.clone();
        contradictory.version.id_authenticator = Some("different".into());
        assert!(
            state
                .validate_remote_records(&share, std::slice::from_ref(&original), &[contradictory],)
                .is_err()
        );
        let mut replayed = original.clone();
        replayed.path = RelativePath::from_bytes(b"replayed".to_vec())?;
        assert!(
            state
                .validate_remote_records(&share, &[], &[original, replayed])
                .is_err()
        );

        Ok(())
    }

    #[test]
    fn remote_snapshot_rejects_duplicate_paths() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let root = temp.path().join("root");
        fs::create_dir(&root)?;
        let state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        let first = file_record(
            RelativePath::from_bytes(b"duplicate".to_vec())?,
            PeerId("foreign".into()),
            8,
            now_ns(),
            Vec::new(),
            Entry::Directory,
        );
        let mut second = first.clone();
        second.version.sequence = 9;
        let error = state
            .validate_remote_records(&share, &[], &[first, second])
            .expect_err("a peer snapshot must contain each path once");
        assert_eq!(error.to_string(), "peer snapshot contains duplicate paths");
        Ok(())
    }

    #[test]
    fn exact_untagged_legacy_record_is_valid_for_read_only_preview() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let root = temp.path().join("root");
        fs::create_dir(&root)?;
        let state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        let legacy = file_record(
            RelativePath::from_bytes(b"legacy".to_vec())?,
            state.peer_id()?,
            1,
            now_ns(),
            Vec::new(),
            Entry::Directory,
        );

        state.validate_remote_records(
            &share,
            std::slice::from_ref(&legacy),
            std::slice::from_ref(&legacy),
        )?;
        let mut changed = legacy.clone();
        changed.path = RelativePath::from_bytes(b"changed".to_vec())?;
        assert!(
            state
                .validate_remote_records(&share, std::slice::from_ref(&legacy), &[changed])
                .is_err()
        );
        let mut forged = legacy;
        forged.version.id_authenticator = Some("forged".into());
        assert!(
            state
                .validate_remote_records(
                    &share,
                    std::slice::from_ref(&forged),
                    std::slice::from_ref(&forged),
                )
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn scheduler_activates_eligible_requests_in_fifo_order_and_only_one_at_a_time() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let state_dir = temporary.path().join("state");
        let mut state = State::open(&state_dir)?;
        let mut first = state.enqueue_sync(None, SyncOperation::Sync, None)?;
        let mut second = state.enqueue_sync(None, SyncOperation::Maintenance, None)?;
        let canceled = state.enqueue_sync(None, SyncOperation::Maintenance, None)?;
        let canceled_token = canceled.token().to_owned();
        canceled.cancel()?;
        assert!(
            !state
                .scheduling_snapshot()?
                .queued
                .iter()
                .any(|request| request.token == canceled_token)
        );
        assert!(first.ticket() < second.ticket());

        let first_permit = first
            .try_activate()?
            .context("first request did not activate")?;
        assert!(second.try_activate()?.is_none());
        let snapshot = State::open(&state_dir)?.scheduling_snapshot()?;
        assert_eq!(
            snapshot.active.as_ref().map(|row| row.ticket),
            Some(first.ticket)
        );
        first_permit.finish()?;

        let second_permit = second
            .try_activate()?
            .context("second request did not activate after the first completed")?;
        second_permit.finish()?;
        let snapshot = State::open(&state_dir)?.scheduling_snapshot()?;
        assert!(snapshot.active.is_none());
        assert!(snapshot.queued.is_empty());
        assert_eq!(snapshot.completion_sequence, 2);
        Ok(())
    }

    #[test]
    fn scheduler_rejoins_at_the_tail_when_activation_recovery_is_busy() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let state_dir = temporary.path().join("state");
        let mut state = State::open(&state_dir)?;
        let mut recovering = state.enqueue_sync(None, SyncOperation::Recovery, None)?;
        let mut successor = state.enqueue_sync(None, SyncOperation::Sync, None)?;
        let original_ticket = recovering.ticket();

        let mut canceled = || false;
        let mut recovery_busy = |_: &mut State| -> Result<()> { Err(QueueRejoin.into()) };
        assert!(
            recovering
                .try_activate_after(&mut recovery_busy, &mut canceled)?
                .is_none()
        );
        assert!(recovering.ticket() > successor.ticket());
        assert!(recovering.ticket() > original_ticket);

        successor
            .try_activate()?
            .context("successor did not advance after recovery rejoined")?
            .finish()?;
        recovering
            .try_activate()?
            .context("rejoined recovery did not later activate")?
            .finish()?;
        Ok(())
    }

    #[test]
    fn scheduler_activation_and_managed_stop_are_transactionally_ordered() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let state_dir = temporary.path().join("state");
        let root = temporary.path().join("root");
        let share = ShareId::generate();
        let mut state = State::open(&state_dir)?;
        state.register_share(&share, &root)?;
        state.set_initial_complete(&share)?;
        let request = state.enable_and_enqueue_managed_sync(&share, 0)?;
        assert_eq!(state.watch_intent_generation(&share)?, 1);
        let token = request.token().to_owned();
        assert_eq!(state.stop_and_cancel_managed_sync(&share)?, 2);

        let mut request = request;
        let error = match request.try_activate() {
            Ok(_) => bail!("stopped request activated"),
            Err(error) => error,
        };
        assert!(error.downcast_ref::<QueueCancelled>().is_some());
        assert!(
            !State::open(&state_dir)?
                .scheduling_snapshot()?
                .queued
                .iter()
                .any(|queued| queued.token == token)
        );
        Ok(())
    }

    #[test]
    fn initial_completion_enable_and_enqueue_are_one_generation_checked_commit() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let root = temporary.path().join("root");
        let share = ShareId::generate();
        let mut state = State::open(temporary.path().join("state"))?;
        state.register_share(&share, &root)?;
        let (intent, _) = state.set_plan_install_intent(&share, &[], &[])?;

        assert!(
            state
                .finish_install_and_enable_managed(&share, &intent, &[], 1)
                .is_err()
        );
        let managed = state.managed_share(&share)?;
        assert!(!managed.initial_complete);
        assert!(!managed.watch_enabled);
        assert!(state.scheduling_snapshot()?.queued.is_empty());

        assert!(state.install_intent(&share)?.is_some());
        let request = state.finish_install_and_enable_managed(&share, &intent, &[], 0)?;
        let managed = state.managed_share(&share)?;
        assert!(managed.initial_complete);
        assert!(managed.watch_enabled);
        assert_eq!(state.watch_intent_generation(&share)?, 1);
        assert_eq!(
            state.scheduling_snapshot()?.queued[0].token,
            request.token()
        );
        drop(request);
        Ok(())
    }

    #[test]
    fn scheduler_activation_cancellation_race_has_no_canceled_activation() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let state_dir = temporary.path().join("state");
        let root = temporary.path().join("root");
        let share = ShareId::generate();
        let mut state = State::open(&state_dir)?;
        state.register_share(&share, &root)?;
        state.set_initial_complete(&share)?;
        let mut request = state.enable_and_enqueue_managed_sync(&share, 0)?;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let activation_barrier = barrier.clone();
        let activation = std::thread::spawn(move || {
            activation_barrier.wait();
            request.try_activate()
        });

        barrier.wait();
        assert_eq!(state.stop_and_cancel_managed_sync(&share)?, 2);
        match activation.join().expect("activation thread panicked") {
            Ok(Some(permit)) => {
                assert!(state.scheduling_snapshot()?.active.is_some());
                permit.finish()?;
            }
            Err(error) if error.downcast_ref::<QueueCancelled>().is_some() => {
                assert!(state.scheduling_snapshot()?.active.is_none());
            }
            Ok(None) => bail!("activation race ended without activation or cancellation"),
            Err(error) => return Err(error),
        }
        assert!(!state.managed_share(&share)?.watch_enabled);
        Ok(())
    }

    #[test]
    fn scheduler_rechecks_cancellation_after_opening_activation_transaction() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let mut state = State::open(temporary.path().join("state"))?;
        let mut request = state.enqueue_sync(None, SyncOperation::Sync, None)?;
        let mut cancellation_checks = 0;
        let error = match request.try_activate_after(&mut |_| Ok(()), &mut || {
            cancellation_checks += 1;
            cancellation_checks == 2
        }) {
            Err(error) => error,
            Ok(_) => bail!("second cancellation check did not prevent activation"),
        };
        assert!(error.downcast_ref::<QueueCancelled>().is_some());
        assert!(state.scheduling_snapshot()?.active.is_none());
        Ok(())
    }

    #[test]
    fn scheduler_cleans_stale_rows_and_orphaned_owner_files() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let state_dir = temporary.path().join("state");
        let mut state = State::open(&state_dir)?;
        state.peer_id()?;

        let stale_token = "request-stale";
        let stale_owner = create_scheduler_owner(&state_dir, stale_token)?;
        assert_eq!(stale_owner.metadata()?.permissions().mode() & 0o777, 0o600);
        assert_eq!(
            fs::metadata(state_dir.join("scheduler"))?
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        state.conn.execute(
            "INSERT INTO sync_queue(token,operation,active) VALUES(?1,'sync',1)",
            [stale_token],
        )?;
        drop(stale_owner);
        let orphan_token = "request-orphan";
        let orphan_owner = create_scheduler_owner(&state_dir, orphan_token)?;
        drop(orphan_owner);
        state.conn.execute(
            "INSERT INTO sync_queue(token,operation,active) VALUES('request-missing','sync',0)",
            [],
        )?;

        let snapshot = state.scheduling_snapshot()?;
        state.cleanup_scheduler_orphans()?;
        assert!(snapshot.queued.is_empty());
        assert!(!state_dir.join("scheduler").join(stale_token).exists());
        assert!(!state_dir.join("scheduler").join(orphan_token).exists());

        let outside = temporary.path().join("outside");
        File::create(&outside)?;
        std::os::unix::fs::symlink(&outside, state_dir.join("scheduler/request-unsafe"))?;
        assert!(state.cleanup_scheduler_orphans().is_err());
        Ok(())
    }

    #[test]
    fn scheduler_cleanup_always_reclaims_a_stale_eligible_head_after_parked_rows() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let state_dir = temporary.path().join("state");
        let mut state = State::open(&state_dir)?;
        let share = ShareId::generate();
        let mut parked = Vec::new();
        for _ in 0..STALE_QUEUE_CLEANUP_LIMIT {
            parked.push(state.enqueue_pending_authority(
                &share,
                &RelationshipId::generate(),
                SyncOperation::Sync,
                None,
            )?);
        }
        let mut stale_head = state.enqueue_sync(None, SyncOperation::Sync, None)?;
        let stale_token = stale_head.token().to_owned();
        drop(stale_head.owner.take());
        drop(stale_head);

        let mut survivor = state.enqueue_sync(None, SyncOperation::Maintenance, None)?;
        assert!(
            !state
                .scheduling_snapshot()?
                .queued
                .iter()
                .any(|request| request.token == stale_token)
        );
        survivor
            .try_activate()?
            .context("request behind stale eligible head did not activate")?
            .finish()?;
        drop(parked);
        Ok(())
    }

    #[test]
    fn scheduler_cleanup_cursor_reaches_stale_rows_after_live_prefix() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let mut state = State::open(temporary.path().join("state"))?;
        let mut live = Vec::new();
        for _ in 0..STALE_QUEUE_CLEANUP_LIMIT {
            live.push(state.enqueue_sync(None, SyncOperation::Sync, None)?);
        }
        let stale = state.enqueue_sync(None, SyncOperation::Maintenance, None)?;
        let stale_token = stale.token().to_owned();
        stale.release_for_reclaim();
        state.conn.execute(
            "UPDATE installation SET scheduler_cleanup_cursor=0 WHERE singleton=1",
            [],
        )?;

        state.cleanup_stale_sync_queue()?;
        assert_ne!(
            state.conn.query_row(
                "SELECT COUNT(*) FROM sync_queue WHERE token=?1",
                [&stale_token],
                |row| row.get::<_, i64>(0),
            )?,
            0
        );
        state.cleanup_stale_sync_queue()?;
        assert_eq!(
            state.conn.query_row(
                "SELECT COUNT(*) FROM sync_queue WHERE token=?1",
                [&stale_token],
                |row| row.get::<_, i64>(0),
            )?,
            0
        );
        drop(live);
        Ok(())
    }

    #[test]
    fn scheduler_reclaims_exact_stale_managed_generation_without_losing_its_ticket() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let state_dir = temporary.path().join("state");
        let root = temporary.path().join("root");
        let share = ShareId::generate();
        let mut state = State::open(&state_dir)?;
        state.register_share(&share, &root)?;
        state.set_initial_complete(&share)?;
        let blocker_share = ShareId::generate();
        let mut parked = Vec::new();
        for _ in 0..STALE_QUEUE_CLEANUP_LIMIT {
            parked.push(state.enqueue_pending_authority(
                &blocker_share,
                &RelationshipId::generate(),
                SyncOperation::Sync,
                None,
            )?);
        }
        let mut original = state.enable_and_enqueue_managed_sync(&share, 0)?;
        let original_ticket = original.ticket();
        let original_token = original.token().to_owned();
        let relationship = RelationshipId::generate();
        let authority = state.peer_id()?;
        assert!(
            state
                .convert_managed_to_authoritative_parked(
                    original.token(),
                    &share,
                    &relationship,
                    SyncOperation::Watch,
                    Some(1),
                    &authority,
                    "nonce-before-crash",
                    "predecessor-before-crash",
                )?
                .is_some()
        );
        drop(original.owner.take());
        drop(original);

        let restored = state.enqueue_sync(Some(&share), SyncOperation::Watch, Some(1))?;
        assert_eq!(restored.ticket(), original_ticket);
        assert_eq!(restored.token(), original_token);
        let restored_snapshot = state
            .scheduling_snapshot()?
            .queued
            .into_iter()
            .find(|request| request.token == original_token)
            .context("restored request is visible")?;
        assert!(restored_snapshot.relationship.is_none());
        assert!(restored_snapshot.network_authority.is_none());
        assert!(restored_snapshot.network_order.is_none());
        assert!(restored_snapshot.paired_state.is_none());
        drop(restored);

        state.stop_and_cancel_managed_sync(&share)?;
        let mut missing_owner = state.enable_and_enqueue_managed_sync(&share, 2)?;
        let missing_token = missing_owner.token().to_owned();
        fs::remove_file(state_dir.join("scheduler").join(&missing_token))?;
        drop(missing_owner.owner.take());
        drop(missing_owner);
        let reclaimed = state.enqueue_sync(Some(&share), SyncOperation::Watch, Some(3))?;
        assert_eq!(reclaimed.token(), missing_token);
        assert!(state_dir.join("scheduler").join(&missing_token).is_file());
        drop(reclaimed);
        drop(parked);
        Ok(())
    }

    #[test]
    fn share_session_and_round_mutation_locks_are_distinct() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let state = State::open(temporary.path().join("state"))?;
        let share = ShareId::generate();
        let _session = state.lock_share_session(&share)?;
        let _round = state.lock_share(&share)?;
        assert!(state.lock_share_session(&share).is_err());
        Ok(())
    }

    #[test]
    fn scheduler_enforces_queue_cap_and_relationship_dedupe() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let state_dir = temporary.path().join("state");
        let mut state = State::open(&state_dir)?;
        let first =
            state.enqueue_sync_inner(None, None, SyncOperation::Sync, None, None, None, None, 1)?;
        assert!(
            state
                .enqueue_sync_inner(None, None, SyncOperation::Sync, None, None, None, None, 1,)
                .is_err()
        );
        drop(first);

        let share = ShareId::generate();
        let relationship = RelationshipId::generate();
        let pending =
            state.enqueue_pending_authority(&share, &relationship, SyncOperation::Sync, None)?;
        assert!(
            state
                .enqueue_pending_authority(&share, &relationship, SyncOperation::Sync, None,)
                .is_err()
        );
        drop(pending);
        Ok(())
    }

    #[test]
    fn paired_scheduler_orders_are_namespaced_by_authority() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let state_dir = temporary.path().join("state");
        let mut state = State::open(&state_dir)?;
        let share = ShareId::generate();
        let relationship_one = RelationshipId::generate();
        let relationship_two = RelationshipId::generate();
        let authority_one = PeerId("peer-authority-one".into());
        let authority_two = PeerId("peer-authority-two".into());
        let first = state.enqueue_pending_authority(
            &share,
            &relationship_one,
            SyncOperation::Sync,
            None,
        )?;
        let second = state.enqueue_pending_authority(
            &share,
            &relationship_two,
            SyncOperation::Sync,
            None,
        )?;
        assert!(state.convert_pending_authority_to_parked(
            first.token(),
            &share,
            &relationship_one,
            SyncOperation::Sync,
            None,
            &authority_one,
            41,
            "nonce-one",
            "before-one",
        )?);
        assert!(state.convert_pending_authority_to_parked(
            second.token(),
            &share,
            &relationship_two,
            SyncOperation::Sync,
            None,
            &authority_two,
            41,
            "nonce-two",
            "before-two",
        )?);
        let predecessor = state
            .prepare_paired_sync(
                first.token(),
                &relationship_one,
                &authority_one,
                41,
                "nonce-one",
            )?
            .context("first pair did not prepare")?;
        assert!(state.commit_paired_sync(
            first.token(),
            &relationship_one,
            &authority_one,
            41,
            "nonce-one",
            &predecessor,
        )?);
        let snapshot = state.scheduling_snapshot()?;
        let first_row = snapshot
            .queued
            .iter()
            .find(|row| row.token == first.token())
            .unwrap();
        let second_row = snapshot
            .queued
            .iter()
            .find(|row| row.token == second.token())
            .unwrap();
        assert_eq!(first_row.paired_state, Some(PairedQueueState::Eligible));
        assert_eq!(first_row.network_order, Some(41));
        assert_eq!(second_row.paired_state, Some(PairedQueueState::Parked));
        assert_eq!(second_row.network_authority, Some(authority_two));
        assert_eq!(second_row.network_order, Some(41));

        let hostile_authority = PeerId("peer-hostile-authority".into());
        let hostile_relationship = RelationshipId::generate();
        let hostile = state.enqueue_parked_proxy(
            &share,
            &hostile_relationship,
            SyncOperation::Sync,
            None,
            &hostile_authority,
            i64::MAX,
            "nonce-hostile",
            "before-hostile",
        )?;
        let local_authority = PeerId("peer-local-authority".into());
        let local_relationship = RelationshipId::generate();
        let (local, order) = state.enqueue_authoritative_sync(
            &share,
            &local_relationship,
            SyncOperation::Sync,
            None,
            &local_authority,
            "nonce-local",
            "before-local",
        )?;
        assert_eq!(order, 1);
        drop(hostile);
        drop(local);
        Ok(())
    }

    #[test]
    fn authoritative_order_ack_and_yield_are_durable_and_atomic() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let state_dir = temporary.path().join("state");
        let mut state = State::open(&state_dir)?;
        let share = ShareId::generate();
        let yielded = RelationshipId::generate();
        let authority = state.peer_id()?;
        state.record_relationship_yield(&yielded, now_ns().saturating_add(1_000_000_000))?;
        assert!(
            state
                .enqueue_authoritative_sync(
                    &share,
                    &yielded,
                    SyncOperation::Sync,
                    None,
                    &authority,
                    "nonce-yielded",
                    "",
                )
                .is_err()
        );

        let mut completed = state.enqueue_sync(None, SyncOperation::Maintenance, None)?;
        completed
            .try_activate()?
            .context("maintenance request did not activate")?
            .finish()?;
        let (first, first_order) = state.enqueue_authoritative_sync(
            &share,
            &yielded,
            SyncOperation::Sync,
            None,
            &authority,
            "nonce-first",
            "",
        )?;
        let second_relationship = RelationshipId::generate();
        let (second, second_order) = state.enqueue_authoritative_sync(
            &share,
            &second_relationship,
            SyncOperation::Sync,
            None,
            &authority,
            "nonce-second",
            "",
        )?;
        assert_eq!(first_order, 1);
        assert_eq!(second_order, 2);
        assert!(state.acknowledge_proxy_issue(first.token(), &yielded, &authority, first_order,)?);
        let first_snapshot = state
            .scheduling_snapshot()?
            .queued
            .into_iter()
            .find(|request| request.token == first.token())
            .unwrap();
        assert!(first_snapshot.proxy_acknowledged);
        drop(first);
        drop(second);
        Ok(())
    }

    #[test]
    fn paired_prepare_and_commit_recheck_earlier_network_predecessors() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let state_dir = temporary.path().join("state");
        let mut state = State::open(&state_dir)?;
        let share = ShareId::generate();
        let older_relationship = RelationshipId::generate();
        let newer_relationship = RelationshipId::generate();
        let authority = state.peer_id()?;
        let (older, older_order) = state.enqueue_authoritative_sync(
            &share,
            &older_relationship,
            SyncOperation::Sync,
            None,
            &authority,
            "nonce-older",
            "",
        )?;
        let (newer, newer_order) = state.enqueue_authoritative_sync(
            &share,
            &newer_relationship,
            SyncOperation::Sync,
            None,
            &authority,
            "nonce-newer",
            "",
        )?;
        let newer_predecessor = state
            .prepare_paired_sync(
                newer.token(),
                &newer_relationship,
                &authority,
                newer_order,
                "nonce-newer",
            )?
            .context("newer parked pair did not prepare")?;
        state
            .prepare_paired_sync(
                older.token(),
                &older_relationship,
                &authority,
                older_order,
                "nonce-older",
            )?
            .context("older parked pair did not prepare")?;
        assert_eq!(state.paired_queue_position(older.token())?.position, 1);
        assert!(state.park_paired_sync(
            older.token(),
            &older_relationship,
            &authority,
            older_order,
            "nonce-older",
        )?);
        let older_predecessor = state
            .prepare_paired_sync(
                older.token(),
                &older_relationship,
                &authority,
                older_order,
                "nonce-older",
            )?
            .context("re-parked older pair did not prepare again")?;
        assert!(state.commit_paired_sync(
            older.token(),
            &older_relationship,
            &authority,
            older_order,
            "nonce-older",
            &older_predecessor,
        )?);
        assert!(!state.commit_paired_sync(
            newer.token(),
            &newer_relationship,
            &authority,
            newer_order,
            "nonce-newer",
            &newer_predecessor,
        )?);
        let newer_snapshot = state
            .scheduling_snapshot()?
            .queued
            .into_iter()
            .find(|request| request.token == newer.token())
            .unwrap();
        assert_eq!(newer_snapshot.paired_state, Some(PairedQueueState::Parked));
        newer.cancel()?;
        let error = state
            .prepare_paired_sync(
                "request-missing",
                &newer_relationship,
                &authority,
                newer_order,
                "nonce-newer",
            )
            .expect_err("removed paired request must be cancellation");
        assert!(error.downcast_ref::<QueueCancelled>().is_some());
        Ok(())
    }

    #[test]
    fn relationship_yield_releases_after_completion_or_retry_time() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let state_dir = temporary.path().join("state");
        let mut state = State::open(&state_dir)?;
        let relationship = RelationshipId::generate();
        state.record_relationship_yield(&relationship, 100)?;
        assert!(!state.consume_relationship_yield_if_released(&relationship, 99)?);

        let mut abandoned = state.enqueue_sync(None, SyncOperation::Maintenance, None)?;
        drop(
            abandoned
                .try_activate()?
                .context("abandoned maintenance request did not activate")?,
        );
        assert!(!state.consume_relationship_yield_if_released(&relationship, 99)?);

        let mut request = state.enqueue_sync(None, SyncOperation::Maintenance, None)?;
        request
            .try_activate()?
            .context("maintenance request did not activate")?
            .finish()?;
        assert!(state.consume_relationship_yield_if_released(&relationship, 99)?);

        state.record_relationship_yield(&relationship, 200)?;
        assert!(!state.consume_relationship_yield_if_released(&relationship, 199)?);
        assert!(state.consume_relationship_yield_if_released(&relationship, 200)?);
        Ok(())
    }

    #[test]
    fn reopening_current_state_does_not_rebuild_scheduler_indexes() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let state_dir = temporary.path().join("state");
        let state = State::open(&state_dir)?;
        let before: i64 = state
            .conn
            .query_row("PRAGMA schema_version", [], |row| row.get(0))?;
        drop(state);

        let reopened = State::open(&state_dir)?;
        let after: i64 = reopened
            .conn
            .query_row("PRAGMA schema_version", [], |row| row.get(0))?;
        assert_eq!(after, before);
        Ok(())
    }

    #[test]
    fn eligible_candidates_preserve_network_order_per_authority_and_fifo_between_them() {
        fn request(
            ticket: i64,
            authority: Option<&str>,
            network_order: Option<i64>,
        ) -> ScheduledRequestSnapshot {
            ScheduledRequestSnapshot {
                ticket,
                token: format!("request-{ticket}"),
                share: None,
                relationship: None,
                operation: SyncOperation::Sync,
                generation: None,
                network_authority: authority.map(|value| PeerId(value.into())),
                network_order,
                paired_state: authority.map(|_| PairedQueueState::Eligible),
                proxy_acknowledged: true,
                active: false,
            }
        }

        let queued = vec![
            request(1, Some("peer-z"), Some(2)),
            request(10, Some("peer-a"), Some(1)),
            request(20, None, None),
            request(30, Some("peer-z"), Some(1)),
            request(40, Some("peer-0"), Some(1)),
        ];
        let snapshot = SchedulingSnapshot {
            completion_sequence: 0,
            active: None,
            queued,
        };
        assert_eq!(
            snapshot
                .eligible_candidates()
                .iter()
                .map(|request| request.ticket)
                .collect::<Vec<_>>(),
            vec![10, 20, 30, 40]
        );
        assert_eq!(
            snapshot
                .eligible_predecessors(&snapshot.queued[0])
                .iter()
                .map(|request| request.ticket)
                .collect::<Vec<_>>(),
            vec![30]
        );
    }

    #[test]
    fn private_directory_creation_ignores_a_permissive_umask() -> Result<()> {
        if let Some(path) = std::env::var_os(PERMISSIVE_UMASK_STATE_DIR) {
            let state = PathBuf::from(path);
            let old_umask = rustix::process::umask(rustix::fs::Mode::empty());
            let result = (|| {
                for directory in [
                    state.clone(),
                    state.join("objects"),
                    state.join("locks"),
                    state.join("run"),
                ] {
                    ensure_private_directory(&directory)?;
                    assert_eq!(fs::metadata(directory)?.permissions().mode() & 0o077, 0);
                }
                let opened = State::open(&state)?;
                drop(opened);
                State::create_upgrade_pending(&state)?;
                for file in [
                    state.join(INSTALLATION_BARRIER_FILE),
                    state.join(UPGRADE_PENDING_FILE),
                ] {
                    assert_eq!(fs::metadata(file)?.permissions().mode() & 0o077, 0);
                }
                Ok(())
            })();
            rustix::process::umask(old_umask);
            return result;
        }

        let temporary = tempfile::tempdir()?;
        let output = Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "state::tests::private_directory_creation_ignores_a_permissive_umask",
            ])
            .env(PERMISSIVE_UMASK_STATE_DIR, temporary.path().join("state"))
            .output()?;
        assert!(output.status.success(), "{:?}", output);
        Ok(())
    }
}
