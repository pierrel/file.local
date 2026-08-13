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
use crate::reconcile::Conflict;

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
    file: File,
    temp_path: PathBuf,
    final_path: PathBuf,
    expected_hash: ObjectHash,
    expected_size: u64,
    written: u64,
    hasher: blake3::Hasher,
}

impl ObjectSink {
    pub fn write_chunk(&mut self, bytes: &[u8]) -> Result<()> {
        if self.written.saturating_add(bytes.len() as u64) > self.expected_size {
            bail!("object exceeds declared size");
        }
        self.file.write_all(bytes)?;
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
        self.file.sync_all()?;
        match fs::symlink_metadata(&self.final_path) {
            Ok(_) if object_path_matches(&self.final_path, &self.expected_hash)? => {
                fs::remove_file(&self.temp_path)?;
            }
            Ok(metadata) => {
                if metadata.is_dir() {
                    fs::remove_dir(&self.final_path)?;
                } else {
                    fs::remove_file(&self.final_path)?;
                }
                fs::rename(&self.temp_path, &self.final_path)?;
                sync_dir(self.final_path.parent().expect("object parent"))?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::rename(&self.temp_path, &self.final_path)?;
                sync_dir(self.final_path.parent().expect("object parent"))?;
            }
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }
}

impl Drop for ObjectSink {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.temp_path);
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
                peer_id TEXT NOT NULL
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
                intent_generation INTEGER NOT NULL DEFAULT 0
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
                created_ns TEXT NOT NULL
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
            ",
        )?;
        let columns: Vec<String> = {
            let mut stmt = conn.prepare("PRAGMA table_info(shares)")?;
            stmt.query_map([], |row| row.get(1))?
                .collect::<Result<_, _>>()?
        };
        let transaction = conn.transaction()?;
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
        if let Some(intent) = self.install_intent(share)? {
            if intent.records != records {
                bail!("a different install is already pending for this share");
            }
            return Ok((intent, false));
        }
        let intent = InstallIntent {
            records: records.to_vec(),
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
        Ok((intent, true))
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

    fn lock_objects(&self) -> Result<File> {
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

    pub fn add_conflicts(&self, share: &ShareId, conflicts: &[Conflict]) -> Result<()> {
        for conflict in conflicts {
            insert_conflict(&self.conn, share, conflict)?;
        }
        Ok(())
    }

    pub fn conflicts(&self, share: &ShareId) -> Result<Vec<StoredConflict>> {
        let mut stmt = self.conn.prepare(
            "SELECT id,path,winner_json,loser_json,created_ns FROM conflicts
             WHERE share_id=?1 ORDER BY CAST(created_ns AS INTEGER) DESC",
        )?;
        let rows = stmt.query_map([&share.0], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Vec<u8>>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
            ))
        })?;
        rows.map(|row| {
            let (id, path, winner, loser, created) = row?;
            Ok(StoredConflict {
                id,
                path: RelativePath::from_bytes(path)?,
                winner: serde_json::from_str(&winner)?,
                loser: serde_json::from_str(&loser)?,
                created_ns: created.parse()?,
            })
        })
        .collect()
    }

    pub fn conflict(&self, share: &ShareId, id: &str) -> Result<StoredConflict> {
        self.conflicts(share)?
            .into_iter()
            .find(|conflict| conflict.id == id)
            .context("conflict not found")
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
        let _budget_lock = self.lock_objects()?;
        let metadata_before = input.metadata()?;
        let (probe_hash, probe_size) = self.hash_object(input.try_clone()?)?;
        input.rewind()?;
        let probe_path = self.object_path(&probe_hash);
        if probe_path.exists() && object_path_matches(&probe_path, &probe_hash)? {
            return Ok((probe_hash, probe_size));
        }
        let available = self.available_object_bytes()?;
        if metadata_before.len() > available {
            bail!("captured object exceeds remaining state storage budget");
        }
        let temp_name = format!(".tmp-{}", ShareId::generate().0);
        let temp_path = self.dir.join("objects").join(temp_name);
        let mut temp = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)?;
        set_private_file(&temp_path)?;
        let mut hasher = blake3::Hasher::new();
        let mut size = 0u64;
        let mut buffer = vec![0u8; 1024 * 1024];
        loop {
            let read = input.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
            temp.write_all(&buffer[..read])?;
            size += read as u64;
            if size > available {
                fs::remove_file(temp_path)?;
                bail!("growing object exceeds remaining state storage budget");
            }
        }
        temp.sync_all()?;
        let metadata_after = input.metadata()?;
        if !same_file_snapshot(&metadata_before, &metadata_after)? {
            fs::remove_file(temp_path)?;
            bail!("file changed while it was being captured");
        }
        let hash = ObjectHash::from_blake3(hasher.finalize());
        let final_path = self.object_path(&hash);
        if final_path.exists() && object_path_matches(&final_path, &hash)? {
            fs::remove_file(temp_path)?;
        } else {
            fs::rename(temp_path, &final_path)?;
            sync_dir(final_path.parent().expect("object parent"))?;
        }
        Ok((hash, size))
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
        let _budget_lock = self.lock_objects()?;
        let actual = ObjectHash::from_blake3(blake3::hash(bytes));
        if &actual != expected {
            bail!("received object hash mismatch");
        }
        let final_path = self.object_path(expected);
        if final_path.exists() && object_path_matches(&final_path, expected)? {
            return Ok(());
        }
        if bytes.len() as u64 > self.available_object_bytes()? {
            bail!("object exceeds remaining state storage budget");
        }
        let temp_path = self
            .dir
            .join("objects")
            .join(format!(".tmp-{}", ShareId::generate().0));
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)?;
        set_private_file(&temp_path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        match fs::rename(&temp_path, &final_path) {
            Ok(()) => sync_dir(final_path.parent().expect("object parent"))?,
            Err(_error) if final_path.exists() => {
                fs::remove_file(temp_path)?;
            }
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    pub fn begin_object(
        &self,
        expected_hash: ObjectHash,
        expected_size: u64,
    ) -> Result<ObjectSink> {
        let budget_lock = self.lock_objects()?;
        let final_path = self.object_path(&expected_hash);
        let reclaimable = match fs::symlink_metadata(&final_path) {
            Ok(metadata)
                if metadata.is_file()
                    && !metadata.file_type().is_symlink()
                    && !object_path_matches(&final_path, &expected_hash)? =>
            {
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
            file,
            temp_path,
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
        let mut referenced = std::collections::HashSet::new();
        {
            let mut statement = self.conn.prepare("SELECT version_json FROM records")?;
            for row in statement.query_map([], |row| row.get::<_, String>(0))? {
                remember_entry_hash(
                    &mut referenced,
                    &serde_json::from_str::<Version>(&row?)?.entry,
                );
            }
        }
        {
            let mut statement = self
                .conn
                .prepare("SELECT winner_json, loser_json FROM conflicts")?;
            for row in statement.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })? {
                let (winner, loser) = row?;
                remember_entry_hash(
                    &mut referenced,
                    &serde_json::from_str::<Record>(&winner)?.version.entry,
                );
                remember_entry_hash(
                    &mut referenced,
                    &serde_json::from_str::<Record>(&loser)?.version.entry,
                );
            }
        }
        for (_, intent) in self.install_intents()? {
            for record in intent.records {
                remember_entry_hash(&mut referenced, &record.version.entry);
            }
        }
        for entry in fs::read_dir(self.dir.join("objects"))? {
            let entry = entry?;
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if name.starts_with(".tmp-") {
                let metadata = fs::symlink_metadata(entry.path())?;
                if metadata.is_file() && !metadata.file_type().is_symlink() {
                    fs::remove_file(entry.path())?;
                }
                continue;
            }
            let Ok(hash) = ObjectHash::parse(name) else {
                continue;
            };
            if !referenced.contains(&hash) {
                let metadata = fs::symlink_metadata(entry.path())?;
                if metadata.is_file() && !metadata.file_type().is_symlink() {
                    fs::remove_file(entry.path())?;
                }
            }
        }
        Ok(())
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

fn remember_entry_hash(hashes: &mut std::collections::HashSet<ObjectHash>, entry: &Entry) {
    if let Entry::File { hash, .. } = entry {
        hashes.insert(hash.clone());
    }
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

fn insert_conflict(connection: &Connection, share: &ShareId, conflict: &Conflict) -> Result<()> {
    let digest = blake3::hash(&serde_json::to_vec(&(
        conflict.path.as_bytes(),
        &conflict.winner.version,
        &conflict.loser.version,
    ))?);
    let id = format!("c-{}", &digest.to_hex()[..12]);
    connection.execute(
        "INSERT OR IGNORE INTO conflicts(id,share_id,path,winner_json,loser_json,created_ns)
         VALUES(?1,?2,?3,?4,?5,?6)",
        params![
            id,
            share.0,
            conflict.path.as_bytes(),
            serde_json::to_string(&conflict.winner)?,
            serde_json::to_string(&conflict.loser)?,
            now_ns().to_string(),
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
            timestamp_ns,
            seen,
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
