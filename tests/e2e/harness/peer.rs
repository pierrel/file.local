use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, bail};
use base64::Engine as _;

use super::docker::{
    RunContext, SECOND_SHARE, SHARE, base_image_tag, image_tag, prepare_upgrade_base, unique_token,
};

const POLL: Duration = Duration::from_millis(250);
const DEADLINE: Duration = Duration::from_secs(30);
const PROMPT_DEADLINE: Duration = Duration::from_secs(5);
const SETUP_COMMAND_DEADLINE: &str = "30s";
const START_COMMAND_DEADLINE: &str = "5s";
const TARGET_COMMAND_KILL_AFTER: &str = "1s";
/// Where a started watcher records its pid inside the container. One watch
/// per scenario container, so a fixed path suffices — and keeps the start
/// command a constant string with nothing interpolated into it.
const WATCH_PIDFILE: &str = "/tmp/flocal-watch.pid";
const WATCH_LOG: &str = "/home/peer/.flocal-watch.log";
const APPLY_STOP_MARKER: &str = "/home/peer/.local/state/file.local/.e2e-stop-before-apply";
const APPLY_STOP_PIDFILE: &str = "/home/peer/.local/state/file.local/.e2e-apply-stop.pid";
const RESERVATION_STOP_MARKER: &str =
    "/home/peer/.local/state/file.local/.e2e-stop-before-reservation";
const RESERVATION_STOP_PIDFILE: &str =
    "/home/peer/.local/state/file.local/.e2e-reservation-stop.pid";
const INSTALLATION_HOLD_PIDFILE: &str = "/tmp/flocal-e2e-installation-hold.pid";
const RECOVERY_DELAY_MARKER: &str =
    "/home/peer/.local/state/file.local/.e2e-delay-install-recovery";
const RECOVERY_DELAY_CLAIMED: &str =
    "/home/peer/.local/state/file.local/.e2e-delay-install-recovery-claimed";
const OBJECT_ENOSPC_MARKER: &str = "/home/peer/.local/state/file.local/.e2e-object-enospc";
const RECOVERY_BUDGET_MARKER: &str =
    "/home/peer/.local/state/file.local/.e2e-recovery-budget-bytes";
const RECOVERY_CONFLICT_LIMIT_MARKER: &str =
    "/home/peer/.local/state/file.local/.e2e-recovery-conflict-limit";
const RECOVERY_METADATA_LIMIT_MARKER: &str =
    "/home/peer/.local/state/file.local/.e2e-recovery-metadata-limit";
const SCHEDULING_WAIT_MARKER: &str =
    "/home/peer/.local/state/file.local/.e2e-observe-global-contention";
const SCHEDULING_WAIT_OBSERVED: &str =
    "/home/peer/.local/state/file.local/.e2e-global-contention-observed";
const DAEMON_PIDFILE: &str = "/home/peer/.flocal-daemon.pid";
const MIGRATION_FAILURE_MARKER: &str =
    "/home/peer/.local/state/file.local/.e2e-fail-state-migration";

fn is_flocal_executable(executable: &[u8]) -> bool {
    executable.ends_with(b"/flocal-real") || executable.ends_with(b"/.local/bin/flocal")
}
/// The product's status JSON schema this harness is written against. Schema 6
/// adds installation scheduling to schema 5's durable relationship lifecycle.
const STATUS_SCHEMA: u64 = 6;
/// The product's sync-list JSON schema this harness is written against.
const SYNC_LIST_SCHEMA: u64 = 3;
/// Recovery records use the versioned three-way merge shape.
const CONFLICTS_SCHEMA: u64 = 2;

/// One running container. Owns its removal; `--rm` plus the in-container
/// lifetime timeout are the backstops when this Drop never runs.
struct Container {
    context: Arc<RunContext>,
    name: String,
}

impl Drop for Container {
    fn drop(&mut self) {
        // The first container dropped during an unwind dumps while both
        // containers are still alive; RunContext's own Drop runs after
        // every container is gone.
        if std::thread::panicking() {
            self.context.dump_once();
        }
        if self.context.keep() {
            eprintln!("FLOCAL_E2E_KEEP=1: keeping container {}", self.name);
            return;
        }
        match std::process::Command::new("docker")
            .args(["rm", "-f", &self.name])
            .output()
        {
            Ok(output) if output.status.success() => {}
            Ok(output) => eprintln!(
                "e2e cleanup: removing container {} failed: {}",
                self.name,
                String::from_utf8_lossy(&output.stderr)
            ),
            Err(error) => {
                eprintln!(
                    "e2e cleanup: removing container {} failed: {error}",
                    self.name
                )
            }
        }
    }
}

/// A started, unpaired container. `peer_add` consumes both boxes and returns
/// the typed `(Connector, Peer)` pair.
pub struct PeerBox {
    peer: Peer,
}

/// Either peer's shared vocabulary: file operations, connectivity, waits,
/// and assertions. Every primitive takes `&self`.
pub struct Peer {
    context: Arc<RunContext>,
    container: Container,
    alias: String,
}

/// The initiating peer. Dereferences to `Peer` for the shared vocabulary;
/// only a `Connector` can `sync` or `watch`.
pub struct Connector {
    peer: Peer,
    watch_max_session_bytes: Option<u64>,
}

impl std::ops::Deref for Connector {
    type Target = Peer;
    fn deref(&self) -> &Peer {
        &self.peer
    }
}

/// The parsed `status --json`, pinned to `STATUS_SCHEMA`. Fields grow with
/// the scenarios that read them.
#[derive(Debug, serde::Deserialize)]
pub struct Status {
    pub schema: u64,
    pub share: String,
    pub bound_peer: Option<String>,
    pub relationship_state: String,
    pub removal_pending: bool,
    pub removal_error: Option<String>,
    pub entries: u64,
    pub pending_install: bool,
    pub unsettled: Vec<Vec<u8>>,
    #[serde(default)]
    pub tombstones: Option<u64>,
    pub recovery: RecoveryStatus,
    pub scheduling: SchedulingStatus,
}

#[derive(Debug, serde::Deserialize)]
pub struct SchedulingStatus {
    pub state: String,
    pub waiting_on: Option<String>,
    pub waiting_root: Option<DaemonPath>,
    pub operation: Option<String>,
    pub queue_position: Option<usize>,
    pub active_share: Option<String>,
    pub active_root: Option<DaemonPath>,
    pub active_operation: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct DaemonPath {
    encoding: String,
    data: String,
}

impl DaemonPath {
    fn decode(&self) -> Result<Vec<u8>> {
        anyhow::ensure!(self.encoding == "base64", "unexpected path encoding");
        Ok(base64::engine::general_purpose::STANDARD.decode(&self.data)?)
    }
}

impl SchedulingStatus {
    fn validate_queued(&self) -> Result<()> {
        anyhow::ensure!(self.state == "queued");
        anyhow::ensure!(self.operation.is_some());
        anyhow::ensure!(self.queue_position.is_some());
        match self.waiting_on.as_deref() {
            Some("local") => {
                if let Some(root) = &self.waiting_root {
                    root.decode()?;
                }
            }
            Some("peer") => anyhow::ensure!(self.waiting_root.is_none()),
            blocker => anyhow::bail!("queued synchronization has invalid blocker {blocker:?}"),
        }
        match (
            &self.active_share,
            &self.active_root,
            &self.active_operation,
        ) {
            (Some(_), Some(root), Some(_)) => {
                root.decode()?;
            }
            (None, None, None) => {}
            active => anyhow::bail!("partial active synchronization identity: {active:#?}"),
        }
        Ok(())
    }
}

#[derive(Debug, serde::Deserialize)]
struct SyncListing {
    schema: u64,
    syncs: Vec<SyncEntry>,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct SyncEntry {
    share: String,
    enabled: bool,
    state: String,
    connection_state: String,
    scheduling: String,
    waiting_on: Option<String>,
    operation: Option<String>,
    queue_position: Option<usize>,
    waiting_root: Option<DaemonPath>,
    active_share: Option<String>,
    active_root: Option<DaemonPath>,
    active_operation: Option<String>,
    role: String,
    registration_pending: bool,
    removal_pending: bool,
}

#[derive(Debug, serde::Deserialize)]
pub struct RecoveryStatus {
    pub conflicts: u64,
    pub used_bytes: u64,
    pub metadata_bytes: u64,
    pub budget_bytes: u64,
    pub reclaimable_bytes: u64,
    pub over_budget: bool,
}

#[derive(Debug, serde::Deserialize)]
pub struct ConflictEntry {
    pub id: String,
    pub path: String,
}

pub struct Conflicts(Vec<ConflictEntry>);

impl Conflicts {
    pub fn expect_none(&self) -> Result<()> {
        if self.0.is_empty() {
            Ok(())
        } else {
            bail!("expected no conflicts, found {}", self.0.len())
        }
    }

    pub fn expect_one(&self, path: &str) -> Result<&ConflictEntry> {
        let matching: Vec<_> = self.0.iter().filter(|c| c.path == path).collect();
        match matching.as_slice() {
            [one] => Ok(one),
            other => bail!(
                "expected exactly one conflict for {path}, found {}",
                other.len()
            ),
        }
    }
}

/// One tree entry as compared by `assert_trees_equal` and shown by the
/// failure dump: kind, the executable-bit boolean the product syncs, the
/// content hash for regular files, and the target text for symlinks.
#[derive(Debug, PartialEq, Eq)]
pub struct TreeEntry {
    pub kind: char,
    pub exec: bool,
    pub hash: Option<String>,
    pub target: Option<String>,
}

pub type Tree = BTreeMap<String, TreeEntry>;

/// A probe reports Ok(()) when its check holds, or a description of the
/// actual state when it does not.
type Probe = Box<dyn Fn(&Peer) -> Result<std::result::Result<(), String>>>;

/// A named check. Each check exists exactly once: `assert_x` is `wait_x`
/// with a zero deadline, so the immediate and eventual forms cannot drift.
struct Condition {
    describe: String,
    probe: Probe,
}

pub fn containers() -> Result<(PeerBox, PeerBox)> {
    // Everything up to the first scenario primitive is harness
    // infrastructure: its failures must not satisfy a known_failure pin.
    infra(setup_containers())
}

pub fn upgrade_containers() -> Result<(PeerBox, PeerBox)> {
    infra(setup_upgrade_containers())
}

fn setup_containers() -> Result<(PeerBox, PeerBox)> {
    let context = RunContext::new()?;
    let a = start_peer(&context, "peer-a", 0)?;
    let b = start_peer(&context, "peer-b", 1)?;
    for peer in [&a, &b] {
        peer.wait_sshd_ready()?;
        peer.install_ssh_material()?;
        peer.exec_ok(&["mkdir", "-p", SHARE])?;
    }
    Ok((PeerBox { peer: a }, PeerBox { peer: b }))
}

fn setup_upgrade_containers() -> Result<(PeerBox, PeerBox)> {
    let base = prepare_upgrade_base()?;
    let context = RunContext::new()?;
    context.record(format!("upgrade base commit: {base}"));
    let base_binary = context.temp.path().join("flocal-upgrade-base");
    let exporter = format!("flocal-e2e-{}-base-export", context.run_id);
    context.docker_ok(&[
        "create",
        "--label",
        super::docker::LABEL,
        "--name",
        &exporter,
        base_image_tag(),
    ])?;
    let source = format!("{exporter}:/usr/local/libexec/flocal-real");
    let destination = base_binary
        .to_str()
        .context("upgrade base binary path is not UTF-8")?;
    let copy = context.docker_ok(&["cp", &source, destination]);
    let remove = context.docker_ok(&["rm", "-f", &exporter]);
    copy?;
    remove?;

    let a = start_peer_with_installed(&context, "peer-a", 0, &base_binary)?;
    let b = start_peer_with_installed(&context, "peer-b", 1, &base_binary)?;
    for peer in [&a, &b] {
        peer.wait_sshd_ready()?;
        peer.install_ssh_material()?;
        peer.exec_ok(&["mkdir", "-p", SHARE])?;
    }
    Ok((PeerBox { peer: a }, PeerBox { peer: b }))
}

fn infra<T>(outcome: Result<T>) -> Result<T> {
    outcome.map_err(|error| {
        if error.is::<super::docker::InfraError>() {
            error
        } else {
            anyhow::Error::new(super::docker::InfraError(format!("{error:#}")))
        }
    })
}

/// The standard opening: two containers, `init` on A, `peer add` toward B,
/// one confirmed initial sync. Built from the public primitives so the
/// opening logic exists exactly once.
pub fn pair() -> Result<(Connector, Peer)> {
    let (a, b) = containers()?;
    a.init()?;
    let (connector, responder) = a.peer_add(b)?;
    connector.sync()?;
    Ok((connector, responder))
}

/// The daemon-managed opening: start both per-user daemons explicitly in the
/// Docker test containers, then use the public one-command setup flow. Docker
/// has no systemd user manager, so this exercises the same installed daemon
/// sockets that real user services own without pretending the containers have
/// login-service integration.
pub fn managed_pair() -> Result<(Connector, Peer)> {
    let (a, b) = containers()?;
    a.start_daemon()?;
    b.start_daemon()?;
    let output = a.peer.flocal_ok(&[
        "sync",
        "add",
        SHARE,
        "--host",
        &b.peer.alias,
        "--remote-path",
        SHARE,
        "--yes",
    ])?;
    drop(output);
    Ok((
        Connector {
            peer: a.peer,
            watch_max_session_bytes: None,
        },
        b.peer,
    ))
}

/// The knobs `pair_with` accepts beyond the standard opening.
pub struct Config {
    pub watch_max_session_bytes: Option<u64>,
}

/// `pair()` with knobs applied to the returned handles.
pub fn pair_with(config: Config) -> Result<(Connector, Peer)> {
    let (mut connector, responder) = pair()?;
    connector.watch_max_session_bytes = config.watch_max_session_bytes;
    Ok((connector, responder))
}

/// Strict expected failure: passes while the wrapped scenario fails, and
/// fails loudly the moment the scenario starts passing, so the fixing pull
/// request must promote the scenario (delete this wrapper) in the same
/// change. Failure dumps are suppressed inside: the failure is expected.
pub fn known_failure(body: impl FnOnce() -> Result<()>) -> Result<()> {
    struct Unsuppress;
    impl Drop for Unsuppress {
        fn drop(&mut self) {
            super::docker::set_suppress_dumps(false);
        }
    }
    super::docker::set_suppress_dumps(true);
    let _guard = Unsuppress;
    match body() {
        Err(error) if error.is::<super::docker::InfraError>() => Err(error),
        Err(error) => {
            eprintln!("known failure (expected): {error:#}");
            Ok(())
        }
        Ok(()) => bail!("scenario now passes — promote it to a plain test in this PR"),
    }
}

pub fn assert_trees_equal(a: &Peer, b: &Peer) -> Result<()> {
    let tree_a = a.tree()?;
    let tree_b = b.tree()?;
    if tree_a != tree_b {
        return Err(a.fail(format!(
            "trees differ:\n{}: {tree_a:#?}\n{}: {tree_b:#?}",
            a.alias, b.alias
        )));
    }
    Ok(())
}

fn start_peer(context: &Arc<RunContext>, alias: &str, volume: usize) -> Result<Peer> {
    start_peer_inner(context, alias, volume, None)
}

fn start_peer_with_installed(
    context: &Arc<RunContext>,
    alias: &str,
    volume: usize,
    executable: &std::path::Path,
) -> Result<Peer> {
    let peer = start_peer_inner(context, alias, volume, Some("/home/peer/.local/bin/flocal"))?;
    let staged = format!("{}:/tmp/flocal-upgrade-base", peer.container.name);
    let source = executable
        .to_str()
        .context("upgrade base binary path is not UTF-8")?;
    context.docker_ok(&["cp", source, &staged])?;
    context.docker_ok(&[
        "exec",
        "-u",
        "root",
        &peer.container.name,
        "sh",
        "-c",
        "install -d -m 700 -o peer -g peer /home/peer/.local /home/peer/.local/bin && install -m 755 -o peer -g peer /tmp/flocal-upgrade-base /home/peer/.local/bin/flocal && rm -f /tmp/flocal-upgrade-base",
    ])?;
    Ok(peer)
}

fn start_peer_inner(
    context: &Arc<RunContext>,
    alias: &str,
    volume: usize,
    installed: Option<&str>,
) -> Result<Peer> {
    let name = format!("flocal-e2e-{}-{alias}", context.run_id);
    let volume_arg = format!("{}:/home/peer", context.volumes[volume]);
    let mut arguments = vec![
        "run",
        "--rm",
        "-d",
        "--label",
        super::docker::LABEL,
        "--name",
        &name,
        "--network",
        &context.network,
        "--network-alias",
        alias,
        "-v",
        &volume_arg,
        "--memory",
        "512m",
        "--pids-limit",
        "256",
        "-e",
        "FLOCAL_E2E_CONTAINER_LIFETIME_SECONDS",
    ];
    let installed_environment = installed.map(|path| format!("FLOCAL_E2E_EXECUTABLE={path}"));
    if let Some(installed) = &installed_environment {
        arguments.extend_from_slice(&["-e", installed]);
    }
    arguments.push(image_tag());
    context.docker_ok(&arguments)?;
    context
        .containers
        .lock()
        .expect("containers lock")
        .push(name.clone());
    Ok(Peer {
        context: Arc::clone(context),
        container: Container {
            context: Arc::clone(context),
            name,
        },
        alias: alias.to_owned(),
    })
}

pub fn upgrade_managed_pair() -> Result<(Connector, Peer)> {
    let (a, b) = upgrade_containers()?;
    a.start_daemon()?;
    b.start_daemon()?;
    let output = a.peer.flocal_ok(&[
        "sync",
        "add",
        SHARE,
        "--host",
        &b.peer.alias,
        "--remote-path",
        SHARE,
        "--yes",
    ])?;
    drop(output);
    Ok((
        Connector {
            peer: a.peer,
            watch_max_session_bytes: None,
        },
        b.peer,
    ))
}

pub fn fresh_installed_pair() -> Result<(Connector, Peer)> {
    let (a, b) = containers()?;
    a.install_candidate()?;
    b.install_candidate()?;
    let output = a.peer.flocal_ok(&[
        "sync",
        "add",
        SHARE,
        "--host",
        &b.peer.alias,
        "--remote-path",
        SHARE,
        "--yes",
    ])?;
    drop(output);
    Ok((
        Connector {
            peer: a.peer,
            watch_max_session_bytes: None,
        },
        b.peer,
    ))
}

impl PeerBox {
    pub fn init(&self) -> Result<()> {
        self.peer
            .flocal_ok(&["init", SHARE])
            .map(|_| ())
            .context("flocal init")
    }

    /// Pairs self (the initiator) with the other peer, consuming both boxes
    /// into the typed roles.
    pub fn peer_add(self, other: PeerBox) -> Result<(Connector, Peer)> {
        let output = self.peer.flocal_ok(&[
            "peer",
            "add",
            SHARE,
            "--host",
            &other.peer.alias,
            "--remote-path",
            SHARE,
        ])?;
        drop(output);
        Ok((
            Connector {
                peer: self.peer,
                watch_max_session_bytes: None,
            },
            other.peer,
        ))
    }
}

impl std::ops::Deref for PeerBox {
    type Target = Peer;
    fn deref(&self) -> &Peer {
        &self.peer
    }
}

impl Connector {
    /// `flocal sync --yes`. A conflicting sync exits zero and records the
    /// conflict; a nonzero exit fails the scenario.
    pub fn sync(&self) -> Result<()> {
        self.flocal_ok(&["sync", SHARE, "--yes"]).map(|_| ())
    }

    pub fn sync_start(&self) -> Result<()> {
        self.flocal_ok(&["sync", "start", SHARE]).map(|_| ())
    }

    pub fn sync_start_observed(&self) -> Result<String> {
        self.peer.sync_start_observed_at(SHARE)
    }

    pub fn sync_stop(&self) -> Result<()> {
        self.flocal_ok(&["sync", "stop", SHARE]).map(|_| ())
    }

    pub fn restart_daemon(&self) -> Result<()> {
        self.peer.restart_daemon()
    }

    pub fn crash_and_restart_daemon(&self) -> Result<()> {
        self.peer.crash_and_restart_daemon()
    }

    pub fn wait_for_sync_diagnostic(&self, needle: &str) -> Result<()> {
        self.poll_until(
            &format!("managed sync did not report {needle:?}"),
            DEADLINE,
            |peer| {
                let output = peer.flocal_raw(&["sync", "list", "--json"])?;
                Ok((output.status.success()
                    && String::from_utf8_lossy(&output.stdout).contains(needle))
                .then_some(()))
            },
        )
    }

    pub fn wait_for_managed_watch_error(&self, needle: &str) -> Result<()> {
        self.wait_for_text("/home/peer/.flocal-daemon.log", needle)
    }

    pub fn raise_recovery_budget(&self, size: &str) -> Result<()> {
        self.flocal_ok(&["conflicts", "budget", SHARE, size])?;
        Ok(())
    }

    pub fn raise_peer_recovery_budget(&self, size: &str) -> Result<()> {
        self.flocal_ok(&["conflicts", "budget", SHARE, size, "--peer"])?;
        Ok(())
    }

    /// `flocal sync --dry-run`; asserts the connector's tree is unchanged by
    /// the preview.
    pub fn sync_dry_run(&self) -> Result<()> {
        let before = self.tree()?;
        self.flocal_ok(&["sync", SHARE, "--dry-run"])?;
        if self.tree()? != before {
            return Err(self.fail("--dry-run changed the connector's tree".into()));
        }
        Ok(())
    }

    /// Runs `flocal sync` without `--yes`, answers "n" to the initial
    /// confirmation, and asserts the prompt was actually shown and the
    /// connector's tree is unchanged. Only meaningful before the initial
    /// confirmed sync; the prompt assertion makes later misuse fail loudly.
    pub fn sync_decline(&self) -> Result<()> {
        let before = self.tree()?;
        let output = self.exec_with_stdin(&["flocal", "sync", SHARE], b"n\n")?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        if !output.status.success() {
            return Err(self.fail(format!("declined sync exited nonzero: {stdout}")));
        }
        if !stdout.contains("Apply this initial plan?") {
            return Err(self.fail(format!(
                "no initial confirmation prompt was shown; sync_decline is only \
                 meaningful before the initial sync. stdout: {stdout}"
            )));
        }
        if self.tree()? != before {
            return Err(self.fail("declined sync changed the connector's tree".into()));
        }
        Ok(())
    }

    /// Asserts the sync attempt fails (the peer is unreachable) and leaves
    /// the connector's tree unchanged. No error text is asserted: the
    /// product reports this path with generic connection errors.
    pub fn sync_expect_offline(&self) -> Result<()> {
        let before = self.tree()?;
        let output = self.flocal_raw(&["sync", SHARE, "--yes"])?;
        if output.status.success() {
            return Err(self.fail("sync succeeded although the peer is offline".into()));
        }
        if self.tree()? != before {
            return Err(self.fail("offline sync attempt changed the connector's tree".into()));
        }
        Ok(())
    }

    /// Asserts the sync attempt fails with the given substring on stderr.
    pub fn sync_expect_err(&self, needle: &str) -> Result<()> {
        let output = self.flocal_raw(&["sync", SHARE, "--yes"])?;
        let stderr = String::from_utf8_lossy(&output.stderr);
        if output.status.success() {
            return Err(self.fail(format!("sync succeeded; expected an error: {needle}")));
        }
        if !stderr.contains(needle) {
            return Err(self.fail(format!("expected {needle:?} in stderr, got: {stderr}")));
        }
        Ok(())
    }

    /// Starts a foreground `flocal watch` inside the container (detached
    /// from the harness process) and returns the guard holding it. The
    /// start command records the shell's pid before `exec`ing `flocal`, so
    /// the recorded pid *is* the watcher's — both the wrapper and the
    /// product are reached by `exec`. Waits until the watcher is running:
    /// a watch that dies on startup fails here, with the dump (its stderr
    /// is in the captured flocal log).
    pub fn watch_start(&self) -> Result<Watch<'_>> {
        let start = format!(
            ": >{WATCH_LOG} && : >/home/peer/.flocal-stderr.log && \
             echo \"$$\" >{WATCH_PIDFILE} && exec flocal watch {SHARE} >{WATCH_LOG}"
        );
        let max_bytes = self
            .watch_max_session_bytes
            .map(|bytes| format!("FLOCAL_MAX_SESSION_BYTES={bytes}"));
        let mut args = vec!["exec", "-d", "-u", "peer"];
        if let Some(max_bytes) = &max_bytes {
            args.extend_from_slice(&["-e", max_bytes]);
        }
        args.extend_from_slice(&[&self.container.name, "sh", "-c", &start]);
        self.context.docker_ok(&args)?;
        // The recorded pid appears once `echo` has written it. `echo >file`
        // truncates before it writes, so a `cat` landing in that window
        // reads an empty file successfully: an empty or unparseable read
        // means "not recorded yet", not a failure — keep polling.
        let pid = self.poll_until("flocal watch never recorded its pid", DEADLINE, |peer| {
            let output = peer.exec_raw(&["cat", "--", WATCH_PIDFILE])?;
            if !output.status.success() {
                return Ok(None);
            }
            Ok(String::from_utf8_lossy(&output.stdout)
                .trim()
                .parse::<u32>()
                .ok())
        })?;
        if !self.is_watcher(pid)? {
            return Err(self.fail(format!(
                "{}: flocal watch exited during startup; see its captured stderr",
                self.alias
            )));
        }
        Ok(Watch {
            peer: &self.peer,
            pid,
            stopped: false,
        })
    }

    pub fn watch_start_with_apply_stop(&self) -> Result<Watch<'_>> {
        self.watch_start_with_apply_stops(1)
    }

    pub fn watch_start_with_apply_stops(&self, count: u8) -> Result<Watch<'_>> {
        let watch = self.watch_start()?;
        watch.wait_for_log("Peer connected")?;
        self.arm_apply_stops(count)?;
        Ok(watch)
    }
}

/// A running `flocal watch`, held as a guard. `stop(self)` consumes the
/// guard, so double-stop is unrepresentable; `Drop` is the never-panicking
/// best-effort backstop. Even SIGKILL is lock-safe by the product's own
/// construction: `flock` releases on process exit, SQLite WAL recovers, and
/// the next command attempts durable install-intent recovery automatically.
pub struct Watch<'a> {
    peer: &'a Peer,
    pid: u32,
    stopped: bool,
}

pub struct StoppedApply<'a> {
    peer: &'a Peer,
    pid: u32,
    resumed: bool,
}

pub struct StoppedInstallation<'a> {
    peer: &'a Peer,
    pid: u32,
    resumed: bool,
}

impl StoppedInstallation<'_> {
    pub fn resume(mut self) -> Result<()> {
        let output = self.peer.signal("CONT", self.pid)?;
        if !output.status.success() {
            return Err(self.peer.fail(format!(
                "resuming installation holder (pid {}) failed: {}",
                self.pid,
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        self.resumed = true;
        Ok(())
    }
}

impl Drop for StoppedInstallation<'_> {
    fn drop(&mut self) {
        if self.resumed || !matches!(self.peer.is_stopped_flocal(self.pid), Ok(true)) {
            return;
        }
        let _ = self.peer.signal("CONT", self.pid);
    }
}

impl StoppedApply<'_> {
    pub fn resume(mut self) -> Result<()> {
        self.peer.resume_stopped_apply_process(self.pid)?;
        self.resumed = true;
        Ok(())
    }
}

impl Drop for StoppedApply<'_> {
    fn drop(&mut self) {
        if self.resumed || !matches!(self.peer.is_stopped_flocal(self.pid), Ok(true)) {
            return;
        }
        let _ = self.peer.remove_apply_stop_pidfile();
        let _ = self.peer.signal("CONT", self.pid);
    }
}

impl Watch<'_> {
    pub fn wait_stopped(&self) -> Result<()> {
        let pid = self.pid;
        self.peer.poll_until(
            &format!("flocal watch (pid {pid}) did not stop at the apply boundary"),
            DEADLINE,
            move |peer| {
                if !peer.is_watcher(pid)? {
                    return Err(peer.fail(format!(
                        "{}: flocal watch (pid {pid}) exited before the apply boundary",
                        peer.alias
                    )));
                }
                let output = peer.exec_raw(&["cat", "--", &format!("/proc/{pid}/status")])?;
                if !output.status.success() {
                    return Ok(None);
                }
                Ok(String::from_utf8_lossy(&output.stdout)
                    .lines()
                    .any(|line| line.starts_with("State:\tT"))
                    .then_some(()))
            },
        )
    }

    pub fn suspend(&self) -> Result<()> {
        let output = self.peer.signal("STOP", self.pid)?;
        if !output.status.success() {
            return Err(self.peer.fail(format!(
                "{}: suspending flocal watch (pid {}) failed: {}",
                self.peer.alias,
                self.pid,
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        Ok(())
    }

    pub fn resume(&self) -> Result<()> {
        if !self.peer.is_watcher(self.pid)? {
            return Err(self.peer.fail(format!(
                "{}: flocal watch (pid {}) exited before resume",
                self.peer.alias, self.pid
            )));
        }
        let output = self.peer.signal("CONT", self.pid)?;
        if !output.status.success() {
            return Err(self.peer.fail(format!(
                "{}: resuming flocal watch (pid {}) failed: {}",
                self.peer.alias,
                self.pid,
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        Ok(())
    }

    pub fn resume_to_next_apply_stop(&self) -> Result<()> {
        self.peer.remove_apply_stop_pidfile()?;
        self.resume()?;
        self.peer.poll_until(
            "second E2E apply-stop marker was not consumed",
            DEADLINE,
            |peer| {
                let output = peer.exec_raw(&["test", "!", "-e", APPLY_STOP_MARKER])?;
                Ok(output.status.success().then_some(()))
            },
        )?;
        self.wait_stopped()
    }

    pub fn resume_for_next_apply_stop(&self) -> Result<()> {
        self.peer.remove_apply_stop_pidfile()?;
        self.resume()
    }

    pub fn wait_for_error(&self, needle: &str) -> Result<()> {
        self.peer
            .wait_for_text("/home/peer/.flocal-stderr.log", needle)
    }

    pub fn wait_for_error_within(&self, needle: &str, deadline: Duration) -> Result<()> {
        self.peer
            .wait_for_text_within("/home/peer/.flocal-stderr.log", needle, deadline)
    }

    pub fn assert_log_absent(&self, needle: &str) -> Result<()> {
        let output = self.peer.exec_ok(&["cat", "--", WATCH_LOG])?;
        let log = String::from_utf8_lossy(&output.stdout);
        if log.contains(needle) {
            return Err(self.peer.fail(format!(
                "{}: watch log prematurely contained {needle:?}: {log}",
                self.peer.alias
            )));
        }
        Ok(())
    }

    pub fn wait_for_log(&self, needle: &str) -> Result<()> {
        self.peer.wait_for_text(WATCH_LOG, needle)
    }

    pub fn wait_for_log_or_error(&self, success: &str, failure: &str) -> Result<()> {
        self.peer.poll_until(
            &format!("watch output did not contain {success:?} or {failure:?}"),
            DEADLINE,
            |peer| {
                let stdout = peer.exec_raw(&["cat", "--", WATCH_LOG])?;
                let stderr = peer.exec_raw(&["cat", "--", "/home/peer/.flocal-stderr.log"])?;
                if !stdout.status.success() || !stderr.status.success() {
                    return Ok(None);
                }
                let errors = String::from_utf8_lossy(&stderr.stdout);
                if errors.contains(failure) {
                    bail!("watch reported {failure:?}: {errors}");
                }
                Ok(String::from_utf8_lossy(&stdout.stdout)
                    .contains(success)
                    .then_some(()))
            },
        )
    }

    pub fn wait_for_log_within(&self, needle: &str, deadline: Duration) -> Result<()> {
        self.peer.wait_for_text_within(WATCH_LOG, needle, deadline)
    }

    /// Terminates the watcher inside the container — killing the `docker
    /// exec` client would not, and a surviving watcher would keep holding
    /// its share-session lock and any active round permit — and waits for it
    /// to exit. Fails, with
    /// the dump, if the watcher had already died: an unnoticed mid-scenario
    /// watcher death would silence everything the scenario claims to test.
    pub fn stop(mut self) -> Result<()> {
        let pid = self.pid;
        if !self.peer.is_watcher(pid)? {
            // Gone already (or the pid was recycled by another process):
            // don't signal it, but fail loudly — a watcher that died
            // mid-scenario would silence everything the scenario tests.
            self.stopped = true;
            return Err(self.peer.fail(format!(
                "{}: flocal watch (pid {pid}) was no longer running at stop; \
                 see its captured stderr",
                self.peer.alias
            )));
        }
        let term = self.peer.signal("TERM", pid)?;
        if !term.status.success() && self.peer.is_watcher(pid)? {
            // The signal genuinely failed to deliver to a still-live
            // watcher (a nonzero `kill` on an already-exited pid is instead
            // the normal race — the poll below confirms it fast). Fail now
            // with `kill`'s own stderr, not a misleading deadline timeout.
            return Err(self.peer.fail(format!(
                "{}: sending SIGTERM to flocal watch (pid {pid}) failed: {}",
                self.peer.alias,
                String::from_utf8_lossy(&term.stderr).trim()
            )));
        }
        // Leave the SIGKILL backstop armed until exit is confirmed: if the
        // wait below times out, Drop still force-kills the survivor. The
        // identity check means a pid recycled after exit reads as gone.
        self.peer.poll_until(
            &format!("flocal watch (pid {pid}) did not exit after SIGTERM"),
            DEADLINE,
            move |peer| Ok((!peer.is_watcher(pid)?).then_some(())),
        )?;
        self.stopped = true;
        Ok(())
    }
}

impl Drop for Watch<'_> {
    fn drop(&mut self) {
        // Best-effort backstop: only signal while the pid is still our
        // watcher, never a process that recycled it.
        if !self.stopped && matches!(self.peer.is_watcher(self.pid), Ok(true)) {
            let _ = self.peer.signal("KILL", self.pid);
        }
    }
}

impl Peer {
    pub fn install_candidate(&self) -> Result<()> {
        self.exec_ok(&[
            "/usr/local/libexec/flocal-real",
            "daemon",
            "install",
            "/home/peer/.local/bin/flocal",
        ])?;
        Ok(())
    }

    pub fn install_candidate_expect_err(&self, needle: &str) -> Result<()> {
        let output = self.exec_raw(&[
            "/usr/local/libexec/flocal-real",
            "daemon",
            "install",
            "/home/peer/.local/bin/flocal",
        ])?;
        let message = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        if output.status.success() {
            return Err(self.fail(format!(
                "candidate install succeeded; expected an error containing {needle:?}"
            )));
        }
        if !message.contains(needle) {
            return Err(self.fail(format!(
                "candidate install error did not contain {needle:?}: {message}"
            )));
        }
        Ok(())
    }

    pub fn arm_migration_failure(&self) -> Result<()> {
        self.exec_ok(&["touch", "--", MIGRATION_FAILURE_MARKER])?;
        Ok(())
    }

    pub fn clear_migration_failure(&self) -> Result<()> {
        self.exec_ok(&["rm", "-f", "--", MIGRATION_FAILURE_MARKER])?;
        Ok(())
    }
    pub fn make_relationship_legacy(&self) -> Result<()> {
        self.flocal_ok(&["protocol", "e2e-make-relationship-legacy", SHARE])?;
        Ok(())
    }

    pub fn assert_relationship_legacy(&self) -> Result<()> {
        self.flocal_ok(&["protocol", "e2e-assert-relationship-legacy", SHARE])?;
        Ok(())
    }

    pub fn init_second_share(&self) -> Result<()> {
        self.exec_ok(&["mkdir", "-p", "--", SECOND_SHARE])?;
        self.flocal_ok(&["init", SECOND_SHARE]).map(|_| ())
    }

    pub fn sync_add_second_to(&self, other: &Peer) -> Result<()> {
        self.flocal_ok(&[
            "sync",
            "add",
            SECOND_SHARE,
            "--host",
            &other.alias,
            "--remote-path",
            SECOND_SHARE,
            "--yes",
        ])?;
        Ok(())
    }

    pub fn sync_add_second_observed(&self, other: &Peer) -> Result<String> {
        let arguments = [
            "sync",
            "add",
            SECOND_SHARE,
            "--host",
            &other.alias,
            "--remote-path",
            SECOND_SHARE,
            "--yes",
        ];
        let output = self.bounded_flocal_raw(&arguments, SETUP_COMMAND_DEADLINE)?;
        reject_target_timeout(&output, &arguments, SETUP_COMMAND_DEADLINE)?;
        if !output.status.success() {
            bail!(
                "{}: flocal {} failed: {}",
                self.alias,
                arguments.join(" "),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    pub fn sync_start_second_observed(&self) -> Result<String> {
        self.sync_start_observed_at(SECOND_SHARE)
    }

    pub fn sync_stop_second(&self) -> Result<()> {
        self.flocal_ok(&["sync", "stop", SECOND_SHARE])?;
        Ok(())
    }

    pub fn write_second(&self, path: &str, content: &str) -> Result<()> {
        let full = self.share_path_at(SECOND_SHARE, path)?;
        if let Some((parent, _)) = full.rsplit_once('/') {
            self.exec_ok(&["mkdir", "-p", "--", parent])?;
        }
        self.exec_with_stdin_ok(&["tee", "--", &full], content.as_bytes())?;
        Ok(())
    }

    pub fn assert_second_absent(&self, path: &str) -> Result<()> {
        self.check(
            self.absent_condition_at(SECOND_SHARE, path)?,
            Duration::ZERO,
        )
    }

    pub fn wait_for_second_file(&self, path: &str, content: &str) -> Result<()> {
        self.check(
            self.file_condition_at(SECOND_SHARE, path, content)?,
            DEADLINE,
        )
    }

    pub fn assert_second_sync_enabled(&self) -> Result<()> {
        self.assert_second_sync(true, None)
    }

    pub fn assert_second_sync_not_enabled(&self) -> Result<()> {
        let share = self.second_status()?.share;
        let listing = self.sync_list()?;
        match listing.syncs.iter().find(|sync| sync.share == share) {
            None => Ok(()),
            Some(sync) if !sync.enabled => Ok(()),
            Some(sync) => Err(self.fail(format!(
                "expected second share not to be enabled, got {sync:#?}"
            ))),
        }
    }

    pub fn assert_second_sync_queued(&self) -> Result<()> {
        self.assert_second_sync(true, Some("queued"))
    }

    pub fn assert_second_sync_durably_queued(&self) -> Result<()> {
        self.second_status()?.scheduling.validate_queued()
    }

    pub fn wait_for_second_sync_queued_behind(&self, root: &str) -> Result<()> {
        let share = self.second_status()?.share;
        let root = root.as_bytes().to_vec();
        self.poll_until(
            "second share did not report its local scheduling wait",
            DEADLINE,
            move |peer| {
                let listing = peer.sync_list()?;
                let Some(sync) = listing.syncs.iter().find(|sync| sync.share == share) else {
                    return Ok(None);
                };
                if sync.scheduling != "queued"
                    || sync.waiting_on.as_deref() != Some("local")
                    || sync.queue_position.is_none()
                    || sync
                        .waiting_root
                        .as_ref()
                        .map(DaemonPath::decode)
                        .transpose()?
                        .as_deref()
                        != Some(root.as_slice())
                {
                    return Ok(None);
                }
                Ok(Some(()))
            },
        )
    }

    pub fn wait_for_second_sync_idle(&self) -> Result<()> {
        let share = self.second_status()?.share;
        self.poll_until(
            "second share did not release its synchronization slot",
            DEADLINE,
            move |peer| {
                let output = peer.flocal_raw(&["sync", "list", "--json"])?;
                if !output.status.success() {
                    return Ok(None);
                }
                let Ok(listing) = serde_json::from_slice::<SyncListing>(&output.stdout) else {
                    return Ok(None);
                };
                Ok(listing
                    .syncs
                    .iter()
                    .find(|sync| sync.share == share)
                    .is_some_and(|sync| sync.scheduling == "idle")
                    .then_some(()))
            },
        )
    }

    pub fn assert_second_sync_stopped(&self) -> Result<()> {
        let share = self.second_status()?.share;
        let listing = self.sync_list()?;
        let Some(sync) = listing.syncs.iter().find(|sync| sync.share == share) else {
            return Err(self.fail("second share is absent from sync list".into()));
        };
        if sync.enabled || sync.scheduling != "idle" {
            return Err(self.fail(format!(
                "expected second share disabled and idle, got {sync:#?}"
            )));
        }
        Ok(())
    }

    pub fn assert_second_sync_start_queue_feedback(&self, output: &str) -> Result<()> {
        let share = self.second_status()?.share;
        let listing = self.sync_list()?;
        let Some(sync) = listing.syncs.iter().find(|sync| sync.share == share) else {
            return Err(self.fail("second share is absent from sync list".into()));
        };
        let Some(active_share) = sync.active_share.as_deref() else {
            return Err(self.fail(format!(
                "queued second share omitted its active share: {sync:#?}"
            )));
        };
        if sync.scheduling != "queued"
            || sync.waiting_on.as_deref() != Some("local")
            || sync.operation.is_none()
            || sync.queue_position.is_none()
            || sync
                .waiting_root
                .as_ref()
                .map(DaemonPath::decode)
                .transpose()?
                .as_deref()
                != Some(SHARE.as_bytes())
        {
            return Err(self.fail(format!(
                "expected second share to report local queue contention, got {sync:#?}"
            )));
        }
        anyhow::ensure!(sync.connection_state != "queued");
        anyhow::ensure!(sync.active_operation.is_some());
        anyhow::ensure!(
            sync.active_root
                .as_ref()
                .map(DaemonPath::decode)
                .transpose()?
                .as_deref()
                == Some(SHARE.as_bytes())
        );
        if !output.contains(&format!("{SHARE} sync")) {
            return Err(self.fail(format!(
                "queued sync start did not name the active root {SHARE:?}: {output}"
            )));
        }
        if output.contains(active_share) {
            return Err(self.fail(format!(
                "queued sync start exposed internal share ID {active_share:?}: {output}"
            )));
        }
        if output.contains("request-") {
            return Err(self.fail(format!(
                "queued sync start exposed an internal request ID: {output}"
            )));
        }
        if !output.contains("queue position") {
            return Err(self.fail(format!(
                "queued sync start omitted its queue position: {output}"
            )));
        }
        Ok(())
    }

    pub fn assert_sync_add_queue_feedback(&self, output: &str) -> Result<()> {
        let wait = output
            .lines()
            .find(|line| line.contains(&format!("{SHARE} sync")));
        let Some(wait) = wait else {
            return Err(self.fail(format!(
                "queued sync add did not name the active root {SHARE:?}: {output}"
            )));
        };
        if wait.contains("share-") || wait.contains("request-") {
            return Err(self.fail(format!(
                "queued sync add exposed an internal scheduler ID: {wait}"
            )));
        }
        if !wait.contains("queue position") {
            return Err(self.fail(format!(
                "queued sync add omitted its queue position: {output}"
            )));
        }
        Ok(())
    }

    pub fn wait_for_opposite_scheduling_contention(&self, other: &Peer) -> Result<()> {
        self.poll_until(
            "opposite-direction starts never exposed a scheduling wait",
            DEADLINE,
            |_| {
                let local = self.status()?;
                let remote = other.second_status()?;
                let queued = [&local.scheduling, &remote.scheduling]
                    .into_iter()
                    .find(|scheduling| scheduling.state == "queued");
                let Some(queued) = queued else {
                    return Ok(None);
                };
                queued.validate_queued()?;
                Ok(Some(()))
            },
        )
    }

    pub fn wait_for_sync_queued_behind(&self, root: &str) -> Result<()> {
        let root = root.as_bytes().to_vec();
        self.poll_until(
            "persistent watch did not report its local scheduling wait",
            DEADLINE,
            move |peer| {
                let scheduling = peer.status()?.scheduling;
                if scheduling.state != "queued" {
                    return Ok(None);
                }
                scheduling.validate_queued()?;
                if scheduling.waiting_on.as_deref() != Some("local")
                    || scheduling
                        .waiting_root
                        .as_ref()
                        .map(DaemonPath::decode)
                        .transpose()?
                        .as_deref()
                        != Some(root.as_slice())
                {
                    return Ok(None);
                }
                Ok(Some(()))
            },
        )
    }

    pub fn assert_sync_durably_queued(&self) -> Result<()> {
        self.status()?.scheduling.validate_queued()
    }

    pub fn arm_scheduling_wait_observation(&self) -> Result<()> {
        self.exec_ok(&[
            "rm",
            "-f",
            "--",
            SCHEDULING_WAIT_MARKER,
            SCHEDULING_WAIT_OBSERVED,
        ])?;
        self.exec_ok(&["touch", "--", SCHEDULING_WAIT_MARKER])?;
        Ok(())
    }

    pub fn wait_for_scheduling_wait(&self) -> Result<()> {
        self.poll_until(
            "no synchronization command joined the installation queue",
            DEADLINE,
            |peer| {
                let output = peer.exec_raw(&["test", "-f", SCHEDULING_WAIT_OBSERVED])?;
                Ok(output.status.success().then_some(()))
            },
        )
    }

    pub fn sync_remove(&self) -> Result<()> {
        self.flocal_ok(&["sync", "remove", SHARE, "--yes"])
            .map(|_| ())
    }

    pub fn sync_remove_local_only(&self) -> Result<()> {
        let output = self.flocal_ok(&["sync", "remove", SHARE, "--local-only", "--yes"])?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        if !stdout.contains("peer may remain registered") {
            return Err(self.fail(format!(
                "local-only removal omitted its asymmetric-state warning: {stdout}"
            )));
        }
        Ok(())
    }

    pub fn sync_remove_expect_err(&self, needle: &str) -> Result<()> {
        let output = self.flocal_raw(&["sync", "remove", SHARE, "--yes"])?;
        let stderr = String::from_utf8_lossy(&output.stderr);
        if output.status.success() {
            return Err(self.fail(format!(
                "relationship removal succeeded; expected an error containing {needle:?}"
            )));
        }
        if !stderr.contains(needle) {
            return Err(self.fail(format!(
                "expected {needle:?} in removal stderr, got: {stderr}"
            )));
        }
        Ok(())
    }

    pub fn sync_add_to(&self, other: &Peer) -> Result<()> {
        self.flocal_ok(&[
            "sync",
            "add",
            SHARE,
            "--host",
            &other.alias,
            "--remote-path",
            SHARE,
            "--yes",
        ])?;
        Ok(())
    }

    pub fn assert_sync_list_empty(&self) -> Result<()> {
        let listing = self.sync_list()?;
        if !listing.syncs.is_empty() {
            return Err(self.fail(format!(
                "expected no configured relationships, found {}",
                listing.syncs.len()
            )));
        }
        Ok(())
    }

    pub fn assert_sync_removing(&self) -> Result<()> {
        let listing = self.sync_list()?;
        match listing.syncs.as_slice() {
            [sync]
                if sync.role == "connector"
                    && !sync.enabled
                    && sync.state == "removing"
                    && !sync.registration_pending
                    && sync.removal_pending =>
            {
                Ok(())
            }
            syncs => Err(self.fail(format!(
                "expected one disabled connector removal, got {syncs:#?}"
            ))),
        }
    }

    pub fn assert_sync_role(&self, role: &str) -> Result<()> {
        let listing = self.sync_list()?;
        match listing.syncs.as_slice() {
            [sync] if sync.role == role && !sync.registration_pending && !sync.removal_pending => {
                Ok(())
            }
            syncs => Err(self.fail(format!("expected one {role} relationship, got {syncs:#?}"))),
        }
    }

    fn sync_list(&self) -> Result<SyncListing> {
        let output = self.flocal_ok(&["sync", "list", "--json"])?;
        let listing: SyncListing =
            serde_json::from_slice(&output.stdout).context("parsing sync list --json")?;
        if listing.schema != SYNC_LIST_SCHEMA {
            return Err(self.fail(format!(
                "sync list schema {} does not match the pinned {SYNC_LIST_SCHEMA}",
                listing.schema,
            )));
        }
        Ok(listing)
    }

    fn assert_second_sync(&self, enabled: bool, state: Option<&str>) -> Result<()> {
        let share = self.second_status()?.share;
        let listing = self.sync_list()?;
        let Some(sync) = listing.syncs.iter().find(|sync| sync.share == share) else {
            return Err(self.fail("second share is absent from sync list".into()));
        };
        if sync.enabled != enabled || state.is_some_and(|expected| sync.state != expected) {
            return Err(self.fail(format!(
                "expected second share enabled={enabled} state={state:?}, got {sync:#?}"
            )));
        }
        Ok(())
    }

    pub fn arm_apply_stops(&self, count: u8) -> Result<()> {
        anyhow::ensure!(
            matches!(count, 1 | 2),
            "apply stop count must be one or two"
        );
        self.remove_apply_stop_pidfile()?;
        let count = count.to_string();
        self.exec_ok(&[
            "sh",
            "-c",
            "printf %s \"$1\" >\"$2\"",
            "sh",
            &count,
            APPLY_STOP_MARKER,
        ])?;
        Ok(())
    }

    pub fn arm_reservation_stop(&self) -> Result<()> {
        self.exec_ok(&["rm", "-f", "--", RESERVATION_STOP_PIDFILE])?;
        self.exec_ok(&["touch", "--", RESERVATION_STOP_MARKER])?;
        Ok(())
    }

    pub fn hold_installation(&self) -> Result<StoppedInstallation<'_>> {
        self.exec_ok(&["rm", "-f", "--", INSTALLATION_HOLD_PIDFILE])?;
        let start = format!(
            "echo \"$$\" >{INSTALLATION_HOLD_PIDFILE} && exec flocal protocol e2e-hold-installation {SHARE}"
        );
        self.context.docker_ok(&[
            "exec",
            "-d",
            "-u",
            "peer",
            &self.container.name,
            "sh",
            "-c",
            &start,
        ])?;
        let pid = self.poll_until("installation holder never started", DEADLINE, |peer| {
            let output = peer.exec_raw(&["cat", "--", INSTALLATION_HOLD_PIDFILE])?;
            if !output.status.success() {
                return Ok(None);
            }
            let Some(pid) = String::from_utf8_lossy(&output.stdout)
                .trim()
                .parse::<u32>()
                .ok()
            else {
                return Ok(None);
            };
            Ok(peer.is_stopped_flocal(pid)?.then_some(pid))
        })?;
        Ok(StoppedInstallation {
            peer: self,
            pid,
            resumed: false,
        })
    }

    pub fn wait_for_stopped_reservation_worker(&self) -> Result<u32> {
        self.poll_until(
            "managed worker did not stop after its durable enqueue",
            DEADLINE,
            |peer| {
                let output = peer.exec_raw(&["cat", "--", RESERVATION_STOP_PIDFILE])?;
                if !output.status.success() {
                    return Ok(None);
                }
                let Some(pid) = String::from_utf8_lossy(&output.stdout)
                    .trim()
                    .parse::<u32>()
                    .ok()
                else {
                    return Ok(None);
                };
                Ok(peer.is_stopped_flocal(pid)?.then_some(pid))
            },
        )
    }

    pub fn resume_reservation_worker(&self, pid: u32) -> Result<()> {
        anyhow::ensure!(
            self.is_stopped_flocal(pid)?,
            "{}: pid {pid} is not a stopped flocal worker",
            self.alias
        );
        self.exec_ok(&["rm", "-f", "--", RESERVATION_STOP_PIDFILE])?;
        let output = self.signal("CONT", pid)?;
        if !output.status.success() {
            return Err(self.fail(format!(
                "{}: resuming reservation worker (pid {pid}) failed: {}",
                self.alias,
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        Ok(())
    }

    pub fn arm_install_recovery_delay(&self) -> Result<()> {
        self.exec_ok(&[
            "rm",
            "-f",
            "--",
            RECOVERY_DELAY_MARKER,
            RECOVERY_DELAY_CLAIMED,
        ])?;
        self.exec_ok(&["touch", "--", RECOVERY_DELAY_MARKER])?;
        Ok(())
    }

    pub fn wait_for_stopped_protocol_server(&self) -> Result<u32> {
        self.poll_until(
            "flocal protocol serve did not stop at the apply boundary",
            DEADLINE,
            |peer| {
                let output = peer.exec_raw(&["cat", "--", APPLY_STOP_PIDFILE])?;
                if !output.status.success() {
                    return Ok(None);
                }
                let Some(pid) = String::from_utf8_lossy(&output.stdout)
                    .trim()
                    .parse::<u32>()
                    .ok()
                else {
                    return Ok(None);
                };
                Ok(peer.is_stopped_protocol_server(pid)?.then_some(pid))
            },
        )
    }

    pub fn wait_for_stopped_apply_process(&self) -> Result<StoppedApply<'_>> {
        let pid = self.poll_until(
            "flocal process did not stop at the apply boundary",
            DEADLINE,
            |peer| {
                let output = peer.exec_raw(&["cat", "--", APPLY_STOP_PIDFILE])?;
                if !output.status.success() {
                    return Ok(None);
                }
                let Some(pid) = String::from_utf8_lossy(&output.stdout)
                    .trim()
                    .parse::<u32>()
                    .ok()
                else {
                    return Ok(None);
                };
                Ok(peer.is_stopped_flocal(pid)?.then_some(pid))
            },
        )?;
        Ok(StoppedApply {
            peer: self,
            pid,
            resumed: false,
        })
    }

    fn resume_stopped_apply_process(&self, pid: u32) -> Result<()> {
        anyhow::ensure!(
            self.is_stopped_flocal(pid)?,
            "{}: pid {pid} is not a stopped flocal process",
            self.alias
        );
        self.remove_apply_stop_pidfile()?;
        anyhow::ensure!(
            self.is_stopped_flocal(pid)?,
            "{}: stopped flocal process changed before resume",
            self.alias
        );
        let output = self.signal("CONT", pid)?;
        if !output.status.success() {
            return Err(self.fail(format!(
                "{}: resuming stopped flocal process (pid {pid}) failed: {}",
                self.alias,
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        Ok(())
    }

    pub fn resume_protocol_to_next_apply_stop(&self, pid: u32) -> Result<u32> {
        self.require_stopped_protocol_server(pid)?;
        self.remove_apply_stop_pidfile()?;
        self.require_stopped_protocol_server(pid)?;
        let output = self.signal("CONT", pid)?;
        if !output.status.success() {
            return Err(self.fail(format!(
                "{}: resuming flocal protocol serve (pid {pid}) failed: {}",
                self.alias,
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        let next = self.wait_for_stopped_protocol_server()?;
        anyhow::ensure!(
            next == pid,
            "responder protocol process changed across apply stops: {pid} -> {next}"
        );
        Ok(next)
    }

    pub fn kill_stopped_protocol_server(&self, pid: u32) -> Result<()> {
        self.require_stopped_protocol_server(pid)?;
        let output = self.signal("KILL", pid)?;
        if !output.status.success() {
            return Err(self.fail(format!(
                "{}: killing flocal protocol serve (pid {pid}) failed: {}",
                self.alias,
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        self.poll_until(
            &format!("flocal protocol serve (pid {pid}) survived SIGKILL"),
            DEADLINE,
            move |peer| Ok((!peer.is_protocol_server(pid)?).then_some(())),
        )
    }

    fn start_daemon(&self) -> Result<()> {
        self.context.docker_ok(&[
            "exec",
            "-d",
            "-u",
            "peer",
            &self.container.name,
            "sh",
            "-c",
            "echo \"$$\" >/home/peer/.flocal-daemon.pid && exec flocal daemon run >/home/peer/.flocal-daemon.log 2>&1",
        ])?;
        self.poll_until("daemon did not become responsive", DEADLINE, |peer| {
            let output = peer.flocal_raw(&["sync", "list", "--json"])?;
            Ok(output.status.success().then_some(()))
        })
    }

    pub fn conflicts(&self) -> Result<Conflicts> {
        let output = self.flocal_ok(&["conflicts", "list", SHARE, "--json"])?;
        #[derive(serde::Deserialize)]
        struct Listing {
            schema: u64,
            conflicts: Vec<RawConflict>,
        }
        #[derive(serde::Deserialize)]
        struct RawConflict {
            id: String,
            path: Vec<u8>,
        }
        let listing: Listing =
            serde_json::from_slice(&output.stdout).context("parsing conflicts list --json")?;
        if listing.schema != CONFLICTS_SCHEMA {
            return Err(self.fail(format!(
                "conflicts schema {} does not match the pinned {CONFLICTS_SCHEMA}",
                listing.schema
            )));
        }
        Ok(Conflicts(
            listing
                .conflicts
                .into_iter()
                .map(|conflict| ConflictEntry {
                    id: conflict.id,
                    path: String::from_utf8_lossy(&conflict.path).into_owned(),
                })
                .collect(),
        ))
    }

    pub fn prune_conflict(&self, id: &str) -> Result<()> {
        validate_component(id)?;
        #[derive(serde::Deserialize)]
        struct Preview {
            schema: u64,
            applied: bool,
            selection_token: String,
        }
        let preview = self.flocal_ok(&["conflicts", "prune", SHARE, id, "--json"])?;
        let preview: Preview =
            serde_json::from_slice(&preview.stdout).context("parsing conflict prune preview")?;
        if preview.schema != 1 || preview.applied {
            bail!("invalid conflict prune preview response");
        }
        self.flocal_ok(&[
            "conflicts",
            "prune",
            SHARE,
            id,
            "--selection",
            &preview.selection_token,
            "--yes",
            "--json",
        ])?;
        Ok(())
    }

    pub fn object_enospc(&self, enabled: bool) -> Result<()> {
        if enabled {
            self.exec_ok(&["touch", OBJECT_ENOSPC_MARKER])?;
        } else {
            self.exec_ok(&["rm", "-f", OBJECT_ENOSPC_MARKER])?;
        }
        Ok(())
    }

    pub fn assert_no_object_temporaries(&self) -> Result<()> {
        let output = self.exec_ok(&[
            "find",
            "/home/peer/.local/state/file.local/objects",
            "-maxdepth",
            "1",
            "-name",
            ".tmp-*",
            "-print",
        ])?;
        if !output.stdout.is_empty() {
            bail!(
                "partial object temporaries remained: {}",
                String::from_utf8_lossy(&output.stdout)
            );
        }
        Ok(())
    }

    pub fn recovery_budget_limit(&self, bytes: Option<u64>) -> Result<()> {
        match bytes {
            Some(bytes) => {
                self.exec_with_stdin_ok(
                    &["tee", RECOVERY_BUDGET_MARKER],
                    bytes.to_string().as_bytes(),
                )?;
            }
            None => {
                self.exec_ok(&["rm", "-f", RECOVERY_BUDGET_MARKER])?;
            }
        }
        Ok(())
    }

    pub fn recovery_conflict_limit(&self, limit: Option<u64>) -> Result<()> {
        self.recovery_limit_marker(RECOVERY_CONFLICT_LIMIT_MARKER, limit)
    }

    pub fn recovery_metadata_limit(&self, bytes: Option<u64>) -> Result<()> {
        self.recovery_limit_marker(RECOVERY_METADATA_LIMIT_MARKER, bytes)
    }

    fn recovery_limit_marker(&self, marker: &str, limit: Option<u64>) -> Result<()> {
        match limit {
            Some(limit) => {
                self.exec_with_stdin_ok(&["tee", marker], limit.to_string().as_bytes())?;
            }
            None => {
                self.exec_ok(&["rm", "-f", marker])?;
            }
        }
        Ok(())
    }

    /// Restores a conflict's losing input to a scratch path outside the
    /// share and returns its content.
    pub fn restore_loser(&self, id: &str) -> Result<String> {
        self.restore_selection(id, &["--version", "loser"])
    }

    pub fn restore_base(&self, id: &str) -> Result<String> {
        self.restore_selection(id, &["--base"])
    }

    pub fn restore_merged(&self, id: &str) -> Result<String> {
        self.restore_selection(id, &["--merged"])
    }

    fn restore_selection(&self, id: &str, selection: &[&str]) -> Result<String> {
        validate_component(id)?;
        let destination = format!("/home/peer/.e2e-restore-{}", unique_token());
        let mut arguments = vec!["restore", SHARE, id];
        arguments.extend_from_slice(selection);
        arguments.extend_from_slice(&["--to", &destination]);
        self.flocal_ok(&arguments)?;
        let output = self.exec_ok(&["cat", "--", &destination])?;
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    // ----- file operations (inside the container, as the peer user) -----

    pub fn write(&self, path: &str, content: &str) -> Result<()> {
        self.write_bytes(path, content.as_bytes())
    }

    pub fn write_bytes(&self, path: &str, content: &[u8]) -> Result<()> {
        let full = self.share_path(path)?;
        if let Some((parent, _)) = full.rsplit_once('/') {
            self.exec_ok(&["mkdir", "-p", "--", parent])?;
        }
        self.exec_with_stdin_ok(&["tee", "--", &full], content)
            .map(|_| ())
    }

    pub fn read(&self, path: &str) -> Result<String> {
        let full = self.share_path(path)?;
        let output = self.exec_ok(&["cat", "--", &full])?;
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    fn wait_for_text(&self, path: &str, needle: &str) -> Result<()> {
        self.wait_for_text_within(path, needle, DEADLINE)
    }

    fn wait_for_text_within(&self, path: &str, needle: &str, deadline: Duration) -> Result<()> {
        self.poll_until(
            &format!("{path} never contained {needle:?}"),
            deadline,
            |peer| {
                let output = peer.exec_raw(&["cat", "--", path])?;
                Ok((output.status.success()
                    && String::from_utf8_lossy(&output.stdout).contains(needle))
                .then_some(()))
            },
        )
    }

    pub fn mkdir(&self, path: &str) -> Result<()> {
        let full = self.share_path(path)?;
        self.exec_ok(&["mkdir", "-p", "--", &full]).map(|_| ())
    }

    pub fn rename(&self, from: &str, to: &str) -> Result<()> {
        let from = self.share_path(from)?;
        let to = self.share_path(to)?;
        self.exec_ok(&["mv", "-T", "--", &from, &to]).map(|_| ())
    }

    pub fn remove(&self, path: &str) -> Result<()> {
        let full = self.share_path(path)?;
        self.exec_ok(&["rm", "--", &full]).map(|_| ())
    }

    pub fn remove_dir_all(&self, path: &str) -> Result<()> {
        let full = self.share_path(path)?;
        self.exec_ok(&["rm", "-rf", "--", &full]).map(|_| ())
    }

    pub fn symlink(&self, path: &str, target: &str) -> Result<()> {
        if target.is_empty() || target.starts_with('-') || target.contains(['\0', '\t', '\n']) {
            bail!("invalid symlink target: {target:?}");
        }
        let full = self.share_path(path)?;
        self.exec_ok(&["ln", "-s", "--", target, &full]).map(|_| ())
    }

    pub fn set_exec(&self, path: &str) -> Result<()> {
        let full = self.share_path(path)?;
        self.exec_ok(&["chmod", "a+x", "--", &full]).map(|_| ())
    }

    pub fn unset_exec(&self, path: &str) -> Result<()> {
        let full = self.share_path(path)?;
        self.exec_ok(&["chmod", "a-x", "--", &full]).map(|_| ())
    }

    // ----- connectivity -----

    pub fn offline(&self) -> Result<()> {
        self.context
            .docker_ok(&[
                "network",
                "disconnect",
                &self.context.network,
                &self.container.name,
            ])
            .map(|_| ())
    }

    pub fn online(&self) -> Result<()> {
        self.context
            .docker_ok(&[
                "network",
                "connect",
                "--alias",
                &self.alias,
                &self.context.network,
                &self.container.name,
            ])
            .map(|_| ())
    }

    // ----- lifecycle -----

    /// Deletes this peer's flocal state directory, so it no longer holds the
    /// share registration or peer binding.
    pub fn reset_state(&self) -> Result<()> {
        self.exec_ok(&["rm", "-rf", "/home/peer/.local/state/file.local"])
            .map(|_| ())
    }

    pub fn status(&self) -> Result<Status> {
        self.status_at(SHARE)
    }

    pub fn second_status(&self) -> Result<Status> {
        self.status_at(SECOND_SHARE)
    }

    fn status_at(&self, root: &str) -> Result<Status> {
        let output = self.flocal_ok(&["status", root, "--json"])?;
        self.parse_status(&output.stdout)
    }

    fn parse_status(&self, output: &[u8]) -> Result<Status> {
        let status: Status = serde_json::from_slice(output).context("parsing status --json")?;
        if status.schema != STATUS_SCHEMA {
            return Err(self.fail(format!(
                "status schema {} does not match the pinned {STATUS_SCHEMA}",
                status.schema
            )));
        }
        Ok(status)
    }

    // ----- assertions and waits: one Condition per check -----

    pub fn assert_file(&self, path: &str, content: &str) -> Result<()> {
        self.check(self.file_condition(path, content)?, Duration::ZERO)
    }

    /// `assert_file` with the default deadline: polls the same Condition
    /// until it holds, or dumps and fails when the deadline expires.
    pub fn wait_for_file(&self, path: &str, content: &str) -> Result<()> {
        self.check(self.file_condition(path, content)?, DEADLINE)
    }

    pub fn wait_for_file_promptly(&self, path: &str, content: &str) -> Result<()> {
        self.check(self.file_condition(path, content)?, PROMPT_DEADLINE)
    }

    /// Counts authenticated SSH sessions accepted by this peer's real sshd.
    /// Scenarios take a baseline so pairing and explicit sync are excluded.
    pub fn ssh_session_count(&self) -> Result<usize> {
        let output = self.context.docker_ok(&["logs", &self.container.name])?;
        Ok([output.stdout.as_slice(), output.stderr.as_slice()]
            .concat()
            .split(|byte| *byte == b'\n')
            .filter(|line| String::from_utf8_lossy(line).contains("Accepted publickey for peer"))
            .count())
    }

    pub fn assert_absent(&self, path: &str) -> Result<()> {
        self.check(self.absent_condition(path)?, Duration::ZERO)
    }

    /// `assert_absent` with the default deadline.
    pub fn wait_absent(&self, path: &str) -> Result<()> {
        self.check(self.absent_condition(path)?, DEADLINE)
    }

    pub fn assert_status(&self, predicate: impl Fn(&Status) -> bool + 'static) -> Result<()> {
        self.check(Self::status_condition(predicate), Duration::ZERO)
    }

    pub fn wait_for_status(&self, predicate: impl Fn(&Status) -> bool + 'static) -> Result<()> {
        self.check(Self::status_condition(predicate), DEADLINE)
    }

    pub fn assert_dir(&self, path: &str) -> Result<()> {
        let full = self.share_path(path)?;
        let describe = format!("{path} is a directory");
        self.check(
            Condition {
                describe,
                probe: Box::new(move |peer| {
                    let output = peer.exec_raw(&["test", "!", "-L", &full, "-a", "-d", &full])?;
                    Ok(if output.status.success() {
                        Ok(())
                    } else {
                        Err("not a directory, a symlink, or missing".into())
                    })
                }),
            },
            Duration::ZERO,
        )
    }

    pub fn assert_exec(&self, path: &str) -> Result<()> {
        self.assert_exec_is(path, true)
    }

    pub fn assert_not_exec(&self, path: &str) -> Result<()> {
        self.assert_exec_is(path, false)
    }

    fn assert_exec_is(&self, path: &str, expected: bool) -> Result<()> {
        let full = self.share_path(path)?;
        let describe = format!("{path} executable bit is {expected}");
        self.check(
            Condition {
                describe,
                probe: Box::new(move |peer| {
                    let regular = peer
                        .exec_raw(&["test", "!", "-L", &full, "-a", "-f", &full])?
                        .status
                        .success();
                    if !regular {
                        return Ok(Err("not a regular file".into()));
                    }
                    let actual = peer.exec_raw(&["test", "-x", &full])?.status.success();
                    Ok(if actual == expected {
                        Ok(())
                    } else {
                        Err(format!("executable bit is {actual}"))
                    })
                }),
            },
            Duration::ZERO,
        )
    }

    pub fn assert_symlink(&self, path: &str, target: &str) -> Result<()> {
        let full = self.share_path(path)?;
        let expected = target.to_owned();
        let describe = format!("{path} is a symlink to {target}");
        self.check(
            Condition {
                describe,
                probe: Box::new(move |peer| {
                    let output = peer.exec_raw(&["readlink", "--", &full])?;
                    if !output.status.success() {
                        return Ok(Err("not a symlink or missing".into()));
                    }
                    let actual = String::from_utf8_lossy(&output.stdout)
                        .trim_end()
                        .to_owned();
                    Ok(if actual == expected {
                        Ok(())
                    } else {
                        Err(format!("links to {actual}"))
                    })
                }),
            },
            Duration::ZERO,
        )
    }

    fn file_condition(&self, path: &str, content: &str) -> Result<Condition> {
        self.file_condition_at(SHARE, path, content)
    }

    fn file_condition_at(&self, root: &str, path: &str, content: &str) -> Result<Condition> {
        let full = self.share_path_at(root, path)?;
        let expected = content.to_owned();
        Ok(Condition {
            describe: format!("{path} contains {content:?}"),
            probe: Box::new(move |peer| {
                let regular = peer
                    .exec_raw(&["test", "!", "-L", &full, "-a", "-f", &full])?
                    .status
                    .success();
                if !regular {
                    return Ok(Err("missing, a symlink, or not a regular file".into()));
                }
                let output = peer.exec_raw(&["cat", "--", &full])?;
                if !output.status.success() {
                    return Ok(Err("unreadable".into()));
                }
                let actual = String::from_utf8_lossy(&output.stdout).into_owned();
                Ok(if actual == expected {
                    Ok(())
                } else {
                    Err(format!("contains {actual:?}"))
                })
            }),
        })
    }

    fn restart_daemon(&self) -> Result<()> {
        let output = self.exec_ok(&["cat", "--", DAEMON_PIDFILE])?;
        let pid = String::from_utf8_lossy(&output.stdout)
            .trim()
            .parse::<u32>()
            .context("parsing daemon pid")?;
        self.exec_ok(&["kill", "-TERM", &pid.to_string()])?;
        self.poll_until("daemon did not stop", DEADLINE, move |peer| {
            let output = peer.exec_raw(&["kill", "-0", &pid.to_string()])?;
            Ok((!output.status.success()).then_some(()))
        })?;
        self.start_daemon()
    }

    fn crash_and_restart_daemon(&self) -> Result<()> {
        let output = self.exec_ok(&["cat", "--", DAEMON_PIDFILE])?;
        let pid = String::from_utf8_lossy(&output.stdout)
            .trim()
            .parse::<u32>()
            .context("parsing daemon pid")?;
        let killed = self.signal("KILL", pid)?;
        if !killed.status.success() {
            return Err(self.fail(format!(
                "{}: crashing daemon (pid {pid}) failed: {}",
                self.alias,
                String::from_utf8_lossy(&killed.stderr).trim()
            )));
        }
        self.poll_until("crashed daemon did not exit", DEADLINE, move |peer| {
            let output = peer.exec_raw(&["kill", "-0", &pid.to_string()])?;
            Ok((!output.status.success()).then_some(()))
        })?;
        self.start_daemon()
    }

    fn absent_condition(&self, path: &str) -> Result<Condition> {
        self.absent_condition_at(SHARE, path)
    }

    fn absent_condition_at(&self, root: &str, path: &str) -> Result<Condition> {
        let full = self.share_path_at(root, path)?;
        Ok(Condition {
            describe: format!("{path} is absent"),
            probe: Box::new(move |peer| {
                let exists = peer.exec_raw(&["test", "-e", &full])?.status.success()
                    || peer.exec_raw(&["test", "-L", &full])?.status.success();
                Ok(if exists {
                    Err("still present".into())
                } else {
                    Ok(())
                })
            }),
        })
    }

    fn status_condition(predicate: impl Fn(&Status) -> bool + 'static) -> Condition {
        Condition {
            describe: "status predicate".into(),
            probe: Box::new(move |peer| {
                let status = peer.status()?;
                Ok(if predicate(&status) {
                    Ok(())
                } else {
                    Err(format!("{status:?}"))
                })
            }),
        }
    }

    fn check(&self, condition: Condition, deadline: Duration) -> Result<()> {
        let started = Instant::now();
        loop {
            let last = (condition.probe)(self)?;
            match last {
                Ok(()) => return Ok(()),
                Err(actual) => {
                    if started.elapsed() >= deadline {
                        return Err(self.fail(format!(
                            "{}: expected {}; {}",
                            self.alias, condition.describe, actual
                        )));
                    }
                    std::thread::sleep(POLL);
                }
            }
        }
    }

    // ----- internals -----

    /// The internal tree walk feeding `assert_trees_equal` and the failure
    /// dump. Mirrors the product scanner's exclusions (`.git` and
    /// `.flocal-tmp-*` components), records the executable bit as the
    /// boolean the product syncs, hashes regular files, records symlink
    /// target text without following, and lists directories first-class.
    pub(super) fn tree(&self) -> Result<Tree> {
        let listing = self.exec_ok(&[
            "find",
            SHARE,
            "-mindepth",
            "1",
            "(",
            "-name",
            ".git",
            "-prune",
            ")",
            "-o",
            "(",
            "-name",
            ".flocal-tmp-*",
            "-prune",
            ")",
            "-o",
            "-printf",
            "%y\\t%m\\t%l\\t%p\\n",
        ])?;
        let mut tree = Tree::new();
        for line in String::from_utf8_lossy(&listing.stdout).lines() {
            let mut fields = line.splitn(4, '\t');
            let (Some(kind), Some(mode), Some(target), Some(path)) =
                (fields.next(), fields.next(), fields.next(), fields.next())
            else {
                bail!("unparseable tree line: {line}");
            };
            let kind = kind.chars().next().context("empty kind")?;
            let mode = u32::from_str_radix(mode, 8).context("tree mode")?;
            let Some(path) = path
                .strip_prefix(SHARE)
                .and_then(|rest| rest.strip_prefix('/'))
                .map(str::to_owned)
            else {
                bail!("tree path outside the share: {path}");
            };
            tree.insert(
                path,
                TreeEntry {
                    kind,
                    exec: kind == 'f' && mode & 0o111 != 0,
                    hash: None,
                    target: (kind == 'l').then(|| target.to_owned()),
                },
            );
        }
        let hashes = self.exec_ok(&[
            "find",
            SHARE,
            "-mindepth",
            "1",
            "(",
            "-name",
            ".git",
            "-prune",
            ")",
            "-o",
            "(",
            "-name",
            ".flocal-tmp-*",
            "-prune",
            ")",
            "-o",
            "-type",
            "f",
            "-exec",
            "sha256sum",
            "--",
            "{}",
            "+",
        ])?;
        for line in String::from_utf8_lossy(&hashes.stdout).lines() {
            let Some((hash, path)) = line.split_once("  ") else {
                bail!("unparseable hash line: {line}");
            };
            let Some(path) = path
                .strip_prefix(SHARE)
                .and_then(|rest| rest.strip_prefix('/'))
            else {
                bail!("hash path outside the share: {path}");
            };
            if let Some(entry) = tree.get_mut(path) {
                entry.hash = Some(hash.to_owned());
            }
        }
        Ok(tree)
    }

    fn fail(&self, message: String) -> anyhow::Error {
        self.context.dump_once();
        anyhow::anyhow!(message)
    }

    /// Sends a signal to a process inside the container. `kill` is a shell
    /// builtin here — the slim image ships no standalone binary — and the
    /// pid travels as a positional argument, never spliced into the script.
    fn signal(&self, signal: &str, pid: u32) -> Result<std::process::Output> {
        let script = format!("kill -{signal} \"$1\"");
        self.exec_raw(&["sh", "-c", &script, "kill", &pid.to_string()])
    }

    /// Reports whether `pid` is (still) the flocal watcher, by reading its
    /// `/proc/<pid>/cmdline`. The recorded pid can outlive the watcher and
    /// be recycled (the container caps pids), so `stop`/`Drop` gate every
    /// real signal on this identity check rather than a bare `kill -0`,
    /// which cannot tell a reused pid apart from the watcher. The watcher's
    /// argv holds both tokens for its whole life — `<…/flocal> watch
    /// <share>`, stable across the `sh -c`→wrapper→real `exec` chain (same
    /// pid throughout). The processes that could recycle its pid carry at
    /// most one: the `sh -c 'kill …'` signaler and the `cat /proc/…` probe
    /// hold neither, and other `flocal` subcommands (`status`, `sync`, …)
    /// hold "flocal" but not "watch". The one both-token process that reads
    /// the pidfile, `cat /tmp/flocal-watch.pid`, runs only during
    /// `watch_start`'s poll — before any watcher pid is known — never
    /// against a live watcher pid. `cmdline` is NUL-separated; match on
    /// substrings.
    fn is_watcher(&self, pid: u32) -> Result<bool> {
        let output = self.exec_raw(&["cat", "--", &format!("/proc/{pid}/cmdline")])?;
        if !output.status.success() {
            return Ok(false); // no such process
        }
        let cmdline = String::from_utf8_lossy(&output.stdout);
        Ok(cmdline.contains("flocal") && cmdline.contains("watch"))
    }

    fn remove_apply_stop_pidfile(&self) -> Result<()> {
        self.exec_ok(&["rm", "-f", "--", APPLY_STOP_PIDFILE])?;
        Ok(())
    }

    fn is_protocol_server(&self, pid: u32) -> Result<bool> {
        let output = self.exec_raw(&["cat", "--", &format!("/proc/{pid}/cmdline")])?;
        if !output.status.success() {
            return Ok(false);
        }
        let arguments: Vec<_> = output
            .stdout
            .split(|byte| *byte == 0)
            .filter(|argument| !argument.is_empty())
            .collect();
        Ok(matches!(arguments.as_slice(), [executable, protocol, serve]
            if is_flocal_executable(executable)
                && *protocol == b"protocol"
                && *serve == b"serve"))
    }

    fn is_stopped_protocol_server(&self, pid: u32) -> Result<bool> {
        if !self.is_protocol_server(pid)? {
            return Ok(false);
        }
        let output = self.exec_raw(&["cat", "--", &format!("/proc/{pid}/status")])?;
        if !output.status.success() {
            return Ok(false);
        }
        Ok(String::from_utf8_lossy(&output.stdout)
            .lines()
            .any(|line| line.starts_with("State:\tT")))
    }

    fn is_stopped_flocal(&self, pid: u32) -> Result<bool> {
        let command = self.exec_raw(&["cat", "--", &format!("/proc/{pid}/cmdline")])?;
        if !command.status.success()
            || !command
                .stdout
                .split(|byte| *byte == 0)
                .next()
                .is_some_and(is_flocal_executable)
        {
            return Ok(false);
        }
        let status = self.exec_raw(&["cat", "--", &format!("/proc/{pid}/status")])?;
        Ok(status.status.success()
            && String::from_utf8_lossy(&status.stdout)
                .lines()
                .any(|line| line.starts_with("State:\tT")))
    }

    fn require_stopped_protocol_server(&self, pid: u32) -> Result<()> {
        anyhow::ensure!(
            self.is_stopped_protocol_server(pid)?,
            "{}: pid {pid} is not a stopped flocal protocol serve process",
            self.alias
        );
        Ok(())
    }

    /// Polls `probe` every `POLL` until it yields `Some(value)`, or dumps and
    /// fails with `describe` (alias-prefixed, like every other failure) when
    /// `deadline` expires. The value-returning twin of `check`, which
    /// collapses its probe to `()`: here `Ok(None)` means "not yet — keep
    /// polling" (an absent pidfile, a transient empty read, a still-live pid).
    fn poll_until<T>(
        &self,
        describe: &str,
        deadline: Duration,
        mut probe: impl FnMut(&Peer) -> Result<Option<T>>,
    ) -> Result<T> {
        let started = Instant::now();
        loop {
            if let Some(value) = probe(self)? {
                return Ok(value);
            }
            if started.elapsed() >= deadline {
                return Err(self.fail(format!("{}: {describe}", self.alias)));
            }
            std::thread::sleep(POLL);
        }
    }

    fn flocal_ok(&self, args: &[&str]) -> Result<std::process::Output> {
        let output = self.flocal_raw(args)?;
        if !output.status.success() {
            return Err(self.fail(format!(
                "{}: flocal {} failed: {}",
                self.alias,
                args.join(" "),
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        Ok(output)
    }

    fn flocal_raw(&self, args: &[&str]) -> Result<std::process::Output> {
        let mut full = vec!["flocal"];
        full.extend_from_slice(args);
        self.exec_raw(&full)
    }

    /// Runs a target command behind the container's hard deadline. This is
    /// used by scheduling bug pins where blocking instead of queueing is
    /// itself a failure and must not wedge the harness.
    fn bounded_flocal_raw(&self, args: &[&str], deadline: &str) -> Result<std::process::Output> {
        let mut full = vec![
            "/usr/bin/timeout",
            "--kill-after",
            TARGET_COMMAND_KILL_AFTER,
            deadline,
            "flocal",
        ];
        full.extend_from_slice(args);
        self.exec_raw(&full)
    }

    fn sync_start_observed_at(&self, root: &str) -> Result<String> {
        let arguments = ["sync", "start", root];
        let output = self.bounded_flocal_raw(&arguments, START_COMMAND_DEADLINE)?;
        reject_target_timeout(&output, &arguments, START_COMMAND_DEADLINE)?;
        if !output.status.success() {
            bail!(
                "{}: flocal {} failed: {}",
                self.alias,
                arguments.join(" "),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    fn exec_ok(&self, command: &[&str]) -> Result<std::process::Output> {
        let output = self.exec_raw(command)?;
        if !output.status.success() {
            return Err(self.fail(format!(
                "{}: {} failed: {}",
                self.alias,
                command.join(" "),
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        Ok(output)
    }

    fn exec_raw(&self, command: &[&str]) -> Result<std::process::Output> {
        let mut args = vec!["exec", "-u", "peer", &self.container.name];
        args.extend_from_slice(command);
        self.context.docker_raw(&args)
    }

    fn exec_with_stdin(&self, command: &[&str], stdin: &[u8]) -> Result<std::process::Output> {
        let mut args = vec!["exec", "-i", "-u", "peer", &self.container.name];
        args.extend_from_slice(command);
        self.context.docker_with_stdin(&args, Some(stdin))
    }

    fn exec_with_stdin_ok(&self, command: &[&str], stdin: &[u8]) -> Result<std::process::Output> {
        let output = self.exec_with_stdin(command, stdin)?;
        if !output.status.success() {
            return Err(self.fail(format!(
                "{}: {} failed: {}",
                self.alias,
                command.join(" "),
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        Ok(output)
    }

    fn wait_sshd_ready(&self) -> Result<()> {
        let started = Instant::now();
        loop {
            let probe = self.context.docker_raw(&[
                "exec",
                &self.container.name,
                "bash",
                "-c",
                "exec 3<>/dev/tcp/127.0.0.1/22",
            ])?;
            if probe.status.success() {
                return Ok(());
            }
            if started.elapsed() >= DEADLINE {
                self.context.dump_once();
                bail!("{}: sshd did not become ready", self.alias);
            }
            std::thread::sleep(POLL);
        }
    }

    fn install_ssh_material(&self) -> Result<()> {
        let public = std::fs::read(self.context.temp.path().join("id_ed25519.pub"))?;
        let private = self.context.temp.path().join("id_ed25519");
        self.exec_ok(&["mkdir", "-p", "/home/peer/.ssh"])?;
        self.exec_with_stdin_ok(&["tee", "/home/peer/.ssh/authorized_keys"], &public)?;
        let target = format!("{}:/home/peer/.ssh/id_ed25519", self.container.name);
        self.context.docker_ok(&[
            "cp",
            private.to_str().context("temp path is not UTF-8")?,
            &target,
        ])?;
        let config = b"Host peer-a peer-b\n    User peer\n    IdentityFile ~/.ssh/id_ed25519\n    StrictHostKeyChecking accept-new\n";
        self.exec_with_stdin_ok(&["tee", "/home/peer/.ssh/config"], config)?;
        self.context.docker_ok(&[
            "exec",
            &self.container.name,
            "chown",
            "-R",
            "peer:peer",
            "/home/peer/.ssh",
        ])?;
        self.exec_ok(&["chmod", "700", "/home/peer/.ssh"])?;
        self.exec_ok(&[
            "chmod",
            "600",
            "/home/peer/.ssh/id_ed25519",
            "/home/peer/.ssh/authorized_keys",
            "/home/peer/.ssh/config",
        ])?;
        Ok(())
    }

    /// Validates a scenario-supplied share-relative path and returns its
    /// absolute form inside the container.
    fn share_path(&self, path: &str) -> Result<String> {
        self.share_path_at(SHARE, path)
    }

    fn share_path_at(&self, root: &str, path: &str) -> Result<String> {
        if root != SHARE && root != SECOND_SHARE {
            bail!("invalid scenario root: {root:?}");
        }
        if path.is_empty()
            || path.starts_with('/')
            || path.starts_with('-')
            || path.contains(['\0', '\t', '\n'])
            || path
                .split('/')
                .any(|component| component.is_empty() || component == "." || component == "..")
        {
            bail!("invalid scenario path: {path:?}");
        }
        Ok(format!("{root}/{path}"))
    }
}

fn validate_component(value: &str) -> Result<()> {
    if value.is_empty()
        || !value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        bail!("invalid identifier: {value:?}");
    }
    Ok(())
}

fn reject_target_timeout(
    output: &std::process::Output,
    arguments: &[&str],
    deadline: &str,
) -> Result<()> {
    if matches!(output.status.code(), Some(124 | 137)) {
        bail!(
            "flocal {} exceeded the E2E target-command deadline of {deadline}",
            arguments.join(" ")
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::os::unix::process::ExitStatusExt as _;

    use super::*;

    #[test]
    fn target_timeout_cannot_be_pinned_by_its_stderr() {
        let output = std::process::Output {
            status: std::process::ExitStatus::from_raw(124 << 8),
            stdout: Vec::new(),
            stderr: b"another synchronization operation already owns this installation".to_vec(),
        };
        let error = reject_target_timeout(&output, &["sync", "start"], "5s")
            .expect_err("timeout must be rejected before stderr classification");
        let error = format!("{error:#}");
        assert!(error.contains("exceeded the E2E target-command deadline"));
        assert!(!error.contains("another synchronization operation"));
    }
}
