use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use anyhow::{Context as _, Result, bail};

pub const SHARE: &str = "/home/peer/share";
const LABEL: &str = "flocal-e2e=1";
const IMAGE_TAG: &str = "flocal-e2e";

/// Everything one scenario run owns besides the two containers: the network,
/// the per-peer volumes, the throwaway keys, the transcript, and the
/// dumped-once flag. Shared by `Arc` between the peer handles; the last
/// clone's `Drop` removes the network and volumes.
pub struct RunContext {
    pub run_id: String,
    pub network: String,
    pub volumes: Vec<String>,
    pub temp: tempfile::TempDir,
    transcript: Mutex<Vec<String>>,
    dumped: AtomicBool,
    pub containers: Mutex<Vec<String>>,
    keep: bool,
}

impl RunContext {
    pub fn new() -> Result<std::sync::Arc<Self>> {
        ensure_ready()?;
        let run_id = unique_token();
        let network = format!("flocal-e2e-{run_id}-net");
        let volumes = vec![
            format!("flocal-e2e-{run_id}-vol-a"),
            format!("flocal-e2e-{run_id}-vol-b"),
        ];
        let temp = tempfile::Builder::new()
            .prefix("flocal-e2e-")
            .tempdir()
            .context("creating the run temporary directory")?;
        set_private_dir(temp.path())?;
        let context = std::sync::Arc::new(Self {
            run_id,
            network,
            volumes,
            temp,
            transcript: Mutex::new(Vec::new()),
            dumped: AtomicBool::new(false),
            containers: Mutex::new(Vec::new()),
            keep: std::env::var_os("FLOCAL_E2E_KEEP").is_some_and(|v| v == "1"),
        });
        context.docker_ok(&["network", "create", "--label", LABEL, &context.network])?;
        for volume in &context.volumes {
            context.docker_ok(&["volume", "create", "--label", LABEL, volume])?;
        }
        let mut keygen = Command::new("ssh-keygen");
        keygen
            .args(["-q", "-t", "ed25519", "-N", "", "-f"])
            .arg(context.temp.path().join("id_ed25519"));
        let output = keygen
            .output()
            .context("running ssh-keygen; the harness requires OpenSSH client tools")?;
        if !output.status.success() {
            bail!(
                "ssh-keygen failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(context)
    }

    /// Runs a docker command, records it in the transcript, and returns its
    /// output whether or not it succeeded.
    pub fn docker_raw(&self, args: &[&str]) -> Result<Output> {
        self.docker_with_stdin(args, None)
    }

    /// Runs a docker command that must succeed.
    pub fn docker_ok(&self, args: &[&str]) -> Result<Output> {
        let output = self.docker_raw(args)?;
        if !output.status.success() {
            bail!(
                "docker {} failed: {}",
                args.join(" "),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(output)
    }

    pub fn docker_with_stdin(&self, args: &[&str], stdin: Option<&[u8]>) -> Result<Output> {
        use std::io::Write as _;
        let started = Instant::now();
        let mut command = Command::new("docker");
        command.args(args);
        command.stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        });
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = command.spawn().context("spawning docker")?;
        if let Some(bytes) = stdin {
            child
                .stdin
                .take()
                .context("docker stdin unavailable")?
                .write_all(bytes)?;
        }
        let output = child.wait_with_output()?;
        self.record(format!(
            "docker {} -> {} in {}ms",
            args.join(" "),
            output.status,
            started.elapsed().as_millis()
        ));
        Ok(output)
    }

    pub fn record(&self, line: String) {
        self.transcript.lock().expect("transcript lock").push(line);
    }

    pub fn transcript_tail(&self, count: usize) -> Vec<String> {
        let transcript = self.transcript.lock().expect("transcript lock");
        let start = transcript.len().saturating_sub(count);
        transcript[start..].to_vec()
    }

    /// Prints the full failure diagnostics exactly once per run.
    pub fn dump_once(&self) {
        if suppressing_dumps() || self.dumped.swap(true, Ordering::SeqCst) {
            return;
        }
        crate::harness::dump::print_dump(self);
    }

    pub fn keep(&self) -> bool {
        self.keep
    }
}

impl Drop for RunContext {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.dump_once();
        }
        if self.keep {
            eprintln!(
                "FLOCAL_E2E_KEEP=1: keeping network {} and volumes {:?}",
                self.network, self.volumes
            );
            return;
        }
        let mut args = vec!["network", "rm", "-f", self.network.as_str()];
        log_cleanup_failure("network", Command::new("docker").args(&args).output());
        args = vec!["volume", "rm", "-f"];
        for volume in &self.volumes {
            args.push(volume);
        }
        log_cleanup_failure("volumes", Command::new("docker").args(&args).output());
    }
}

fn log_cleanup_failure(what: &str, result: std::io::Result<Output>) {
    match result {
        Ok(output) if output.status.success() => {}
        Ok(output) => eprintln!(
            "e2e cleanup: removing {what} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ),
        Err(error) => eprintln!("e2e cleanup: removing {what} failed: {error}"),
    }
}

/// A failure of the harness itself (daemon, sweep, or image build) rather
/// than of the scenario under test. `known_failure` re-raises these instead
/// of accepting them as the expected scenario failure.
#[derive(Debug)]
pub struct InfraError(pub String);

impl std::fmt::Display for InfraError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "harness infrastructure failure: {}", self.0)
    }
}

impl std::error::Error for InfraError {}

/// Serial tests each get fresh containers, but the daemon check, stale-run
/// sweep, and image build happen once per process.
fn ensure_ready() -> Result<()> {
    static READY: OnceLock<std::result::Result<(), String>> = OnceLock::new();
    READY
        .get_or_init(|| prepare().map_err(|error| format!("{error:#}")))
        .clone()
        .map_err(|message| anyhow::Error::new(InfraError(message)))
}

fn prepare() -> Result<()> {
    let version = Command::new("docker").args(["version"]).output();
    match version {
        Ok(output) if output.status.success() => {}
        _ => bail!("docker daemon unreachable; the e2e suite requires docker"),
    }
    sweep()?;
    build_image()
}

/// Removes anything a previous aborted run left behind. rm-only as an
/// invariant: the sweep never execs into or reads from swept objects, and it
/// assumes one `make e2e` run per host at a time.
fn sweep() -> Result<()> {
    let filter = format!("label={LABEL}");
    for (list, remove) in [
        (
            vec!["ps", "-aq", "--filter", filter.as_str()],
            vec!["rm", "-f"],
        ),
        (
            vec!["network", "ls", "-q", "--filter", filter.as_str()],
            vec!["network", "rm", "-f"],
        ),
        (
            vec!["volume", "ls", "-q", "--filter", filter.as_str()],
            vec!["volume", "rm", "-f"],
        ),
    ] {
        let output = Command::new("docker").args(&list).output()?;
        let ids: Vec<String> = String::from_utf8_lossy(&output.stdout)
            .split_whitespace()
            .map(str::to_owned)
            .collect();
        if ids.is_empty() {
            continue;
        }
        let mut args: Vec<&str> = remove.clone();
        args.extend(ids.iter().map(String::as_str));
        let _ = Command::new("docker").args(&args).output()?;
    }
    Ok(())
}

fn build_image() -> Result<()> {
    let root = env!("CARGO_MANIFEST_DIR");
    let output = Command::new("docker")
        .args(["build", "-f", "tests/e2e/Dockerfile", "-t", IMAGE_TAG, "."])
        .current_dir(root)
        .output()
        .context("running docker build")?;
    if !output.status.success() {
        bail!(
            "docker build failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

pub fn image_tag() -> &'static str {
    IMAGE_TAG
}

pub fn unique_token() -> String {
    use std::sync::atomic::AtomicU64;
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let count = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    format!("{}-{count}-{nanos}", std::process::id())
}

thread_local! {
    static SUPPRESS_DUMPS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub fn suppressing_dumps() -> bool {
    SUPPRESS_DUMPS.with(std::cell::Cell::get)
}

pub fn set_suppress_dumps(value: bool) {
    SUPPRESS_DUMPS.with(|cell| cell.set(value));
}

#[cfg(unix)]
fn set_private_dir(path: &std::path::Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    Ok(())
}
