use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, bail};

use super::docker::{RunContext, SHARE, image_tag, unique_token};

const POLL: Duration = Duration::from_millis(250);
const DEADLINE: Duration = Duration::from_secs(30);
const PROMPT_DEADLINE: Duration = Duration::from_secs(5);
/// Where a started watcher records its pid inside the container. One watch
/// per scenario container, so a fixed path suffices — and keeps the start
/// command a constant string with nothing interpolated into it.
const WATCH_PIDFILE: &str = "/tmp/flocal-watch.pid";
const WATCH_LOG: &str = "/home/peer/.flocal-watch.log";
const APPLY_STOP_MARKER: &str = "/home/peer/.local/state/file.local/.e2e-stop-before-apply";
const DAEMON_PIDFILE: &str = "/home/peer/.flocal-daemon.pid";
/// The product's status JSON schema this harness is written against. Schema 3
/// adds durable unsettled paths to schema 2's tombstone fields.
const STATUS_SCHEMA: u64 = 3;
/// The conflicts listing has its own schema, which stays 1 when the status
/// schema bumps to 3.
const CONFLICTS_SCHEMA: u64 = 1;

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
    pub entries: u64,
    pub pending_install: bool,
    pub unsettled: Vec<Vec<u8>>,
    #[serde(default)]
    pub tombstones: Option<u64>,
}

#[derive(Debug, serde::Deserialize)]
pub struct ConflictEntry {
    pub id: String,
    pub path: String,
}

pub struct Conflicts(Vec<ConflictEntry>);

impl Conflicts {
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

/// The daemon-managed opening: start the connector daemon explicitly in the
/// Docker test container, then use the public one-command setup flow. Docker
/// has no systemd user manager, so this exercises the same installed daemon
/// socket that a real user service owns without pretending the container has
/// login-service integration.
pub fn managed_pair() -> Result<(Connector, Peer)> {
    let (a, b) = containers()?;
    a.start_daemon()?;
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
    let outcome = body();
    match outcome {
        // A harness-infrastructure failure is not the expected scenario
        // failure; re-raise it so it cannot masquerade as a satisfied pin.
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
    let name = format!("flocal-e2e-{}-{alias}", context.run_id);
    let volume_arg = format!("{}:/home/peer", context.volumes[volume]);
    context.docker_ok(&[
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
        image_tag(),
    ])?;
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

    pub fn sync_stop(&self) -> Result<()> {
        self.flocal_ok(&["sync", "stop", SHARE]).map(|_| ())
    }

    pub fn restart_daemon(&self) -> Result<()> {
        self.peer.restart_daemon()
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
            ": >{WATCH_LOG} && echo \"$$\" >{WATCH_PIDFILE} && exec flocal watch {SHARE} >{WATCH_LOG}"
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
        anyhow::ensure!(
            matches!(count, 1 | 2),
            "apply stop count must be one or two"
        );
        let watch = self.watch_start()?;
        watch.wait_for_log("Peer connected")?;
        let count = count.to_string();
        self.exec_ok(&[
            "sh",
            "-c",
            "printf %s \"$1\" >\"$2\"",
            "sh",
            &count,
            APPLY_STOP_MARKER,
        ])?;
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

    pub fn wait_for_error(&self, needle: &str) -> Result<()> {
        self.peer
            .wait_for_text("/home/peer/.flocal-stderr.log", needle)
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

    /// Terminates the watcher inside the container — killing the `docker
    /// exec` client would not, and a surviving watcher would keep holding
    /// the share and global locks — and waits for it to exit. Fails, with
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
        self.poll_until(
            "daemon did not create its control socket",
            DEADLINE,
            |peer| {
                let output = peer.exec_raw(&[
                    "test",
                    "-S",
                    "/home/peer/.local/state/file.local/run/daemon.sock",
                ])?;
                Ok(output.status.success().then_some(()))
            },
        )
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

    /// Restores a conflict's losing input to a scratch path outside the
    /// share and returns its content.
    pub fn restore_loser(&self, id: &str) -> Result<String> {
        validate_component(id)?;
        let destination = format!("/home/peer/.e2e-restore-{}", unique_token());
        self.flocal_ok(&[
            "restore",
            SHARE,
            id,
            "--version",
            "loser",
            "--to",
            &destination,
        ])?;
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
        self.poll_until(
            &format!("{path} never contained {needle:?}"),
            DEADLINE,
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
        let output = self.flocal_ok(&["status", SHARE, "--json"])?;
        let status: Status =
            serde_json::from_slice(&output.stdout).context("parsing status --json")?;
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
        let full = self.share_path(path)?;
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

    fn absent_condition(&self, path: &str) -> Result<Condition> {
        let full = self.share_path(path)?;
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
    /// argv holds both tokens for its whole life — `<…/flocal-real> watch
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
        Ok(format!("{SHARE}/{path}"))
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
