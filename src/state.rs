use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::{Component, Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use anyhow::{Context, Result, bail};
use directories::ProjectDirs;
use fs2::FileExt;
use rusqlite::{Connection, OptionalExtension, params};

use crate::model::{
    Entry, ObjectHash, PeerConfig, PeerId, Record, RelativePath, ShareId, Version, VersionId,
};
use crate::reconcile::{Conflict, ConflictResolution};

pub const DEFAULT_RECOVERY_BUDGET_BYTES: u64 = 10 * 1024 * 1024 * 1024;
pub const RECOVERY_ROW_OVERHEAD_BYTES: u64 = 256;
const MAX_ALL_PRUNE_SUMMARIES: u64 = 10_000;
const MAX_ALL_PRUNE_SUMMARY_BYTES: u64 = 8 * 1024 * 1024;

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
    pub peer: Option<PeerConfig>,
    pub initial_complete: bool,
    pub watch_enabled: bool,
    pub blocked_diagnostic: Option<String>,
}

pub struct State {
    pub dir: PathBuf,
    conn: Connection,
}

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

impl State {
    pub fn open_default() -> Result<Self> {
        if let Some(path) = std::env::var_os("FLOCAL_STATE_DIR") {
            return Self::open(path);
        }
        if let Some(path) = Self::managed_state_dir()? {
            return Self::open(path);
        }
        let dirs = ProjectDirs::from("local", "file.local", "file.local")
            .context("could not determine user state directory")?;
        #[cfg(target_os = "linux")]
        let path = dirs
            .state_dir()
            .context("could not determine user state directory")?;
        #[cfg(not(target_os = "linux"))]
        let path = dirs.data_local_dir();
        Self::open(path)
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
        ensure_private_directory(&dir.join("objects"))?;
        set_private_dir(&dir)?;
        set_private_dir(&dir.join("objects"))?;
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
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS installation (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                peer_id TEXT NOT NULL,
                auth_key BLOB
            );
            CREATE TABLE IF NOT EXISTS shares (
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
                records_json TEXT NOT NULL
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
            ",
        )?;
        let columns: Vec<String> = {
            let mut stmt = conn.prepare("PRAGMA table_info(shares)")?;
            stmt.query_map([], |row| row.get(1))?
                .collect::<Result<_, _>>()?
        };
        let conflict_columns: Vec<String> = {
            let mut stmt = conn.prepare("PRAGMA table_info(conflicts)")?;
            stmt.query_map([], |row| row.get(1))?
                .collect::<Result<_, _>>()?
        };
        let installation_columns: Vec<String> = {
            let mut stmt = conn.prepare("PRAGMA table_info(installation)")?;
            stmt.query_map([], |row| row.get(1))?
                .collect::<Result<_, _>>()?
        };
        let transaction = conn.transaction()?;
        if !conflict_columns.iter().any(|name| name == "conflict_json") {
            transaction.execute("ALTER TABLE conflicts ADD COLUMN conflict_json TEXT", [])?;
        }
        if !installation_columns.iter().any(|name| name == "auth_key") {
            transaction.execute("ALTER TABLE installation ADD COLUMN auth_key BLOB", [])?;
        }
        if !columns.iter().any(|name| name == "bound_peer") {
            transaction.execute("ALTER TABLE shares ADD COLUMN bound_peer TEXT", [])?;
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
        Ok(Self { dir, conn })
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
        if let Some(intent) = self.install_intent(share)? {
            if intent.records != records || intent.conflicts != conflicts {
                bail!("a different install is already pending for this share");
            }
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
        };
        self.conn.execute(
            "INSERT INTO install_intents(share_id, records_json) VALUES(?1, ?2)
             ON CONFLICT(share_id) DO UPDATE SET records_json=excluded.records_json",
            params![share.0, serde_json::to_string(&intent)?],
        )?;
        self.conn
            .execute("DELETE FROM pending_objects WHERE share_id=?1", [&share.0])?;
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
        let current = self
            .install_intent(share)?
            .context("install intent disappeared before commit")?;
        if current.records != expected.records
            || current.conflicts != expected.conflicts
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
        for conflict in &current.conflicts {
            insert_conflict(&tx, share, conflict)?;
        }
        tx.execute("DELETE FROM install_intents WHERE share_id=?1", [&share.0])?;
        tx.commit()?;
        Ok(())
    }

    pub fn clear_install_intent(&self, share: &ShareId) -> Result<()> {
        self.conn
            .execute("DELETE FROM install_intents WHERE share_id=?1", [&share.0])?;
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
        let temp = intent
            .temps
            .iter_mut()
            .find(|temp| &temp.path == path)
            .context("install temporary is missing")?;
        temp.phase = phase;
        self.conn.execute(
            "UPDATE install_intents SET records_json=?2 WHERE share_id=?1",
            params![share.0, serde_json::to_string(&intent)?],
        )?;
        Ok(())
    }

    pub fn rotate_unowned_install_temp(
        &self,
        share: &ShareId,
        path: &RelativePath,
    ) -> Result<String> {
        let mut intent = self
            .install_intent(share)?
            .context("install intent is missing")?;
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
        self.conn.execute(
            "UPDATE install_intents SET records_json=?2 WHERE share_id=?1",
            params![share.0, serde_json::to_string(&intent)?],
        )?;
        Ok(token)
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

    pub fn lock_global_sync(&self) -> Result<File> {
        let path = self.dir.join("sync.lock");
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        set_private_file(&path)?;
        file.try_lock_exclusive()
            .context("another synchronization operation already owns this installation")?;
        Ok(file)
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

    pub fn register_share_bound(&mut self, id: &ShareId, root: &Path, peer: &PeerId) -> Result<()> {
        let _registration_lock = self.lock_registration()?;
        fs::create_dir_all(root)?;
        let root = root.canonicalize()?;
        let root_bytes = path_bytes(&root);
        let identity = root_identity(&root)?;
        self.reject_overlapping_root_except(&root, Some(id))?;
        let transaction = self.conn.transaction()?;
        let existing: Option<(Vec<u8>, Option<String>, String, String)> = transaction
            .query_row(
                "SELECT root, bound_peer, root_device, root_inode FROM shares WHERE share_id=?1",
                [&id.0],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        match existing {
            Some((path, _, _, _)) if path != root_bytes => {
                bail!("share ID is already registered to a different directory")
            }
            Some((_, Some(bound), _, _)) if bound != peer.0 => {
                bail!("share is bound to a different peer")
            }
            Some((_, _, device, inode)) => {
                validate_identity_values(&root, identity, &device, &inode)?;
                transaction.execute(
                    "UPDATE shares SET bound_peer=?2 WHERE share_id=?1",
                    params![id.0, peer.0],
                )?;
            }
            None => {
                transaction.execute(
                    "INSERT INTO shares(share_id, root, bound_peer, root_device, root_inode)
                     VALUES(?1, ?2, ?3, ?4, ?5)",
                    params![
                        id.0,
                        root_bytes,
                        peer.0,
                        identity.device.to_string(),
                        identity.inode.to_string()
                    ],
                )?;
            }
        }
        transaction.commit()?;
        Ok(())
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

    pub fn set_peer(&self, id: &ShareId, peer: &PeerConfig) -> Result<()> {
        self.conn.execute(
            "UPDATE shares SET peer_json=?2 WHERE share_id=?1",
            params![id.0, serde_json::to_string(peer)?],
        )?;
        Ok(())
    }

    pub fn bound_peer(&self, id: &ShareId) -> Result<Option<PeerId>> {
        // A share this installation never registered has no binding; it must
        // not surface as an internal database error, because the responder
        // turns a binding mismatch into its graceful rejection.
        let value: Option<Option<String>> = self
            .conn
            .query_row(
                "SELECT bound_peer FROM shares WHERE share_id=?1",
                [&id.0],
                |r| r.get(0),
            )
            .optional()?;
        Ok(value.flatten().map(PeerId))
    }

    pub fn peer(&self, id: &ShareId) -> Result<Option<PeerConfig>> {
        let json: Option<String> = self.conn.query_row(
            "SELECT peer_json FROM shares WHERE share_id=?1",
            [&id.0],
            |r| r.get(0),
        )?;
        json.map(|v| serde_json::from_str(&v).map_err(Into::into))
            .transpose()
    }

    pub fn set_initial_complete(&self, id: &ShareId) -> Result<()> {
        self.conn.execute(
            "UPDATE shares SET initial_complete=1 WHERE share_id=?1",
            [&id.0],
        )?;
        Ok(())
    }

    pub fn add_conflicts(&mut self, share: &ShareId, conflicts: &[Conflict]) -> Result<()> {
        let _global_lock = self.lock_global_sync()?;
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
        &self,
        share: &ShareId,
        conflict_ids: &[String],
    ) -> Result<RecoveryPrunePlan> {
        let _global_lock = self.lock_global_sync()?;
        let _object_lock = self.lock_objects()?;
        self.recovery_prune_plan_locked(share, conflict_ids)
    }

    pub fn prune_recovery(
        &mut self,
        share: &ShareId,
        conflict_ids: &[String],
        expected_token: &str,
    ) -> Result<RecoveryPruneOutcome> {
        let _global_lock = self.lock_global_sync()?;
        let _object_lock = self.lock_objects()?;
        let plan = self.recovery_prune_plan_locked(share, conflict_ids)?;
        if plan.selection_token != expected_token {
            bail!("recovery conflicts changed since preview; preview pruning again");
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
            "SELECT share_id, root, peer_json, initial_complete, watch_enabled, blocked_diagnostic
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
                "SELECT share_id, root, peer_json, initial_complete, watch_enabled, blocked_diagnostic
                 FROM shares WHERE share_id=?1",
                [&id.0],
                managed_share_from_row,
            )
            .optional()?
            .context("share not found")
    }

    pub fn set_watch_enabled(&self, id: &ShareId, enabled: bool) -> Result<()> {
        let changed = self.conn.execute(
            "UPDATE shares SET watch_enabled=?2, intent_generation=intent_generation+1 WHERE share_id=?1",
            params![id.0, i64::from(enabled)],
        )?;
        if changed == 0 {
            bail!("share not found");
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
             WHERE share_id=?1 AND intent_generation=?3",
            params![id.0, i64::from(enabled), expected_generation],
        )?;
        if changed == 0 {
            bail!("sync was stopped or reconfigured while its initial plan was running");
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
             intent_generation=intent_generation+1 WHERE share_id=?1 AND intent_generation=?2",
            params![id.0, expected_generation],
        )?;
        if changed == 0 {
            bail!("sync was stopped or reconfigured while its initial plan was running");
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn set_blocked(&self, id: &ShareId, diagnostic: &str) -> Result<()> {
        let diagnostic = diagnostic.chars().take(4096).collect::<String>();
        let changed = self.conn.execute(
            "UPDATE shares SET blocked_diagnostic=?2 WHERE share_id=?1",
            params![id.0, diagnostic],
        )?;
        if changed == 0 {
            bail!("share not found");
        }
        Ok(())
    }

    pub fn clear_blocked(&self, id: &ShareId) -> Result<()> {
        let changed = self.conn.execute(
            "UPDATE shares SET blocked_diagnostic=NULL WHERE share_id=?1",
            [&id.0],
        )?;
        if changed == 0 {
            bail!("share not found");
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

fn managed_share_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ManagedShare> {
    let peer: Option<String> = row.get(2)?;
    Ok(ManagedShare {
        id: ShareId(row.get(0)?),
        root: bytes_path(row.get::<_, Vec<u8>>(1)?),
        peer: peer
            .map(|json| serde_json::from_str(&json))
            .transpose()
            .map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    2,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?,
        initial_complete: row.get::<_, i64>(3)? != 0,
        watch_enabled: row.get::<_, i64>(4)? != 0,
        blocked_diagnostic: row.get(5)?,
    })
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
    let metadata = file.metadata()?;
    if !metadata.is_dir() {
        bail!("configured root {} is not a directory", path.display());
    }
    Ok(RootIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
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
