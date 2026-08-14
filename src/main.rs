use std::io::{self, BufReader, BufWriter, Read, Write};
use std::os::fd::AsFd;
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use base64::Engine;
use clap::{Args, Parser, Subcommand, ValueEnum};
use flocal::model::{Entry, PeerConfig, ShareId};
use flocal::state::State;
use flocal::sync::{self, InitialMessage, Message, V2Envelope, V2RoundFrame, V2SessionFrame};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};

#[derive(Debug)]
struct RemoteWatchError {
    retryable: bool,
    message: String,
}

impl std::fmt::Display for RemoteWatchError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "remote watch rejected the session: {}",
            escaped(&self.message)
        )
    }
}

impl std::error::Error for RemoteWatchError {}

#[derive(Debug)]
struct WatchProtocolError(String);

impl std::fmt::Display for WatchProtocolError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for WatchProtocolError {}

fn watch_protocol_error(message: impl Into<String>) -> anyhow::Error {
    WatchProtocolError(message.into()).into()
}

macro_rules! watch_protocol_bail {
    ($($argument:tt)*) => {
        return Err(watch_protocol_error(format!($($argument)*)))
    };
}

#[derive(Parser)]
#[command(
    name = "flocal",
    version,
    about = "Local-first directory synchronization"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Init {
        path: PathBuf,
    },
    Peer {
        #[command(subcommand)]
        command: PeerCommand,
    },
    Sync(SyncArgs),
    Status {
        path: PathBuf,
        #[arg(long)]
        json: bool,
    },
    Conflicts {
        #[command(subcommand)]
        command: ConflictCommand,
    },
    Restore {
        path: PathBuf,
        conflict_id: String,
        #[arg(long, value_enum)]
        version: Option<RestoreVersion>,
        #[arg(long, conflicts_with_all = ["version", "base", "merged"])]
        input: Option<String>,
        #[arg(long, conflicts_with_all = ["version", "input", "merged"])]
        base: bool,
        #[arg(long, conflicts_with_all = ["version", "input", "base"])]
        merged: bool,
        #[arg(long = "to")]
        destination: PathBuf,
        #[arg(long)]
        force: bool,
    },
    Watch {
        path: PathBuf,
    },
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
    Protocol {
        #[command(subcommand)]
        command: ProtocolCommand,
    },
}

#[derive(Args)]
struct SyncArgs {
    #[command(subcommand)]
    command: Option<SyncCommand>,
    path: Option<PathBuf>,
    #[arg(long)]
    dry_run: bool,
    #[arg(long)]
    yes: bool,
    #[arg(long)]
    json: bool,
}

#[derive(Subcommand)]
enum SyncCommand {
    Add {
        path: PathBuf,
        #[arg(long)]
        host: String,
        #[arg(long)]
        remote_path: PathBuf,
        #[arg(long)]
        yes: bool,
    },
    List {
        #[arg(long)]
        json: bool,
    },
    Start {
        path: Option<PathBuf>,
        #[arg(long)]
        share: Option<String>,
        #[arg(long)]
        yes: bool,
    },
    Stop {
        path: Option<PathBuf>,
        #[arg(long)]
        share: Option<String>,
    },
}

#[derive(Subcommand)]
enum DaemonCommand {
    Run,
    PreflightService { executable: PathBuf },
    InstallService,
}

#[derive(Clone, Copy, ValueEnum)]
enum RestoreVersion {
    Winner,
    Loser,
}

#[derive(Subcommand)]
enum PeerCommand {
    Add {
        path: PathBuf,
        #[arg(long)]
        host: String,
        #[arg(long)]
        remote_path: PathBuf,
    },
    List {
        path: PathBuf,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum ProtocolCommand {
    Serve,
}

#[derive(Subcommand)]
enum ConflictCommand {
    List {
        path: PathBuf,
        #[arg(long)]
        json: bool,
    },
    Show {
        path: PathBuf,
        conflict_id: String,
        #[arg(long)]
        json: bool,
    },
}

fn main() {
    if let Err(error) = run() {
        eprintln!("flocal: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    if let Commands::Sync(arguments) = &cli.command {
        validate_sync_arguments(arguments)?;
    }
    let mut state = State::open_default()?;
    match cli.command {
        Commands::Init { path } => {
            std::fs::create_dir_all(&path)?;
            let id = state.init_share(&path)?;
            println!(
                "Initialized share {} at {}",
                id.0,
                path.canonicalize()?.display()
            );
        }
        Commands::Peer { command } => match command {
            PeerCommand::Add {
                path,
                host,
                remote_path,
            } => add_peer(&state, &path, &host, &remote_path)?,
            PeerCommand::List { path, json } => list_peer(&state, &path, json)?,
        },
        Commands::Sync(SyncArgs {
            command,
            path,
            dry_run,
            yes,
            json,
        }) => match command {
            Some(command) => sync_command(&mut state, command)?,
            None => {
                let path = path.context("sync requires a subcommand or PATH")?;
                if !dry_run {
                    recover_installs(&mut state)?;
                }
                run_sync(&mut state, &path, dry_run, yes, json, PlanReport::Full)?;
            }
        },
        Commands::Status { path, json } => status(&state, &path, json)?,
        Commands::Conflicts { command } => conflicts(&state, command)?,
        Commands::Restore {
            path,
            conflict_id,
            version,
            input,
            base,
            merged,
            destination,
            force,
        } => {
            let selector = RestoreSelector::new(version, input, base, merged)?;
            restore(&state, &path, &conflict_id, selector, &destination, force)?
        }
        Commands::Watch { path } => {
            recover_installs(&mut state)?;
            watch(&mut state, &path)?
        }
        Commands::Daemon {
            command: DaemonCommand::Run,
        } => daemon_run(state)?,
        Commands::Daemon {
            command: DaemonCommand::PreflightService { executable },
        } => preflight_daemon_service(&state, &executable)?,
        Commands::Daemon {
            command: DaemonCommand::InstallService,
        } => install_daemon_service(&state)?,
        Commands::Protocol {
            command: ProtocolCommand::Serve,
        } => {
            recover_installs(&mut state)?;
            serve(&mut state)?
        }
    }
    Ok(())
}

fn validate_sync_arguments(arguments: &SyncArgs) -> Result<()> {
    let managed_name = arguments
        .path
        .as_deref()
        .and_then(Path::to_str)
        .is_some_and(|name| matches!(name, "add" | "list" | "start" | "stop"));
    if (arguments.command.is_some() || managed_name)
        && (arguments.dry_run || arguments.yes || arguments.json)
    {
        bail!("legacy sync options cannot be combined with a managed sync subcommand");
    }
    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
enum DaemonRequest {
    List {
        cursor: Option<String>,
    },
    Start {
        share: String,
        generation: Option<i64>,
    },
    Stop {
        share: String,
    },
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
enum DaemonResponse {
    Ok,
    List {
        syncs: Vec<DaemonSync>,
        next: Option<String>,
    },
    Error {
        message: String,
    },
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct DaemonSync {
    share: String,
    root: DaemonPath,
    host: Option<String>,
    remote_path: Option<DaemonPath>,
    enabled: bool,
    initial_complete: bool,
    state: String,
    diagnostic: Option<String>,
    unsettled: usize,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct DaemonPath {
    encoding: String,
    data: String,
}

fn daemon_path(bytes: &[u8]) -> DaemonPath {
    DaemonPath {
        encoding: "base64".into(),
        data: base64::engine::general_purpose::STANDARD.encode(bytes),
    }
}

fn daemon_path_bytes(path: &DaemonPath) -> Result<Vec<u8>> {
    if path.encoding != "base64" {
        bail!("daemon returned an unsupported path encoding")
    }
    base64::engine::general_purpose::STANDARD
        .decode(&path.data)
        .context("daemon returned an invalid path encoding")
}

struct DaemonWorker {
    id: u64,
    stop: Arc<AtomicBool>,
    state: Arc<std::sync::atomic::AtomicU8>,
    stopping: Arc<AtomicBool>,
    finished: Arc<AtomicBool>,
    child: Arc<Mutex<Option<Child>>>,
}

enum WorkerEvent {
    Exited {
        share: ShareId,
        worker_id: u64,
        error: Option<String>,
    },
}

const WORKER_STARTING: u8 = 0;
const WORKER_RECONNECTING: u8 = 1;
const WORKER_WATCHING: u8 = 2;
static NEXT_WORKER_ID: AtomicU64 = AtomicU64::new(1);

fn sync_command(state: &mut State, command: SyncCommand) -> Result<()> {
    match command {
        SyncCommand::Add {
            path,
            host,
            remote_path,
            yes,
        } => {
            validate_sync_add_path(&path)?;
            let share = match state.find_share(&path) {
                Ok((share, _)) => {
                    let root = state.root_for(&share)?;
                    if root != path.canonicalize()? {
                        bail!("directory is inside an existing share; use the share root instead")
                    }
                    if let Some(peer) = state.peer(&share)? {
                        if peer.host != host || peer.remote_path != path_bytes(&remote_path) {
                            bail!(
                                "directory is already paired to a different remote; use `flocal sync start PATH`"
                            );
                        }
                    } else {
                        add_peer(state, &path, &host, &remote_path)?;
                    }
                    share
                }
                Err(_) => {
                    let share = state.init_share(&path)?;
                    add_peer(state, &path, &host, &remote_path)?;
                    share
                }
            };
            ensure_daemon(state)?;
            let generation = if state.initial_complete(&share)? {
                None
            } else {
                match complete_initial_and_enable(state, &share, yes)? {
                    Some(generation) => Some(generation),
                    None => return Ok(()),
                }
            };
            daemon_request(
                state,
                DaemonRequest::Start {
                    share: share.0,
                    generation,
                },
            )?;
        }
        SyncCommand::List { json } => {
            ensure_daemon(state)?;
            let mut cursor = None;
            let mut syncs = Vec::new();
            loop {
                let response = daemon_request(
                    state,
                    DaemonRequest::List {
                        cursor: cursor.clone(),
                    },
                )?;
                let DaemonResponse::List { syncs: page, next } = response else {
                    bail!("daemon returned an invalid response")
                };
                syncs.extend(page);
                match next {
                    Some(next) if cursor.as_deref() != Some(&next) => cursor = Some(next),
                    Some(_) => bail!("daemon returned an invalid list continuation"),
                    None => break,
                }
            }
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(
                        &serde_json::json!({"schema": 1, "syncs": syncs})
                    )?
                );
            } else {
                for sync in syncs {
                    let root = bytes_path(&daemon_path_bytes(&sync.root)?);
                    let peer = match (sync.host, sync.remote_path) {
                        (Some(host), Some(path)) => {
                            format!(
                                "{host}:{}",
                                bytes_path(&daemon_path_bytes(&path)?).display()
                            )
                        }
                        _ => "responder only".into(),
                    };
                    let diagnostic = sync
                        .diagnostic
                        .map(|value| format!("; {value}"))
                        .unwrap_or_default();
                    let desired = if sync.enabled { "enabled" } else { "disabled" };
                    let unsettled = if sync.unsettled == 0 {
                        String::new()
                    } else {
                        format!(
                            "; {} unsettled paths (see `flocal status {}`)",
                            sync.unsettled,
                            root.display()
                        )
                    };
                    println!(
                        "{}  {}  {}  {}{}{}",
                        root.display(),
                        peer,
                        desired,
                        sync.state,
                        diagnostic,
                        unsettled,
                    );
                }
            }
        }
        SyncCommand::Start { path, share, yes } => {
            ensure_daemon(state)?;
            let share = select_share(state, path.as_deref(), share.as_deref())?;
            let managed = state.managed_share(&share)?;
            let generation = if !managed.initial_complete {
                match complete_initial_and_enable(state, &share, yes)? {
                    Some(generation) => Some(generation),
                    None => return Ok(()),
                }
            } else {
                None
            };
            daemon_request(
                state,
                DaemonRequest::Start {
                    share: share.0,
                    generation,
                },
            )?;
        }
        SyncCommand::Stop { path, share } => {
            ensure_daemon(state)?;
            let share = select_share(state, path.as_deref(), share.as_deref())?;
            daemon_request(state, DaemonRequest::Stop { share: share.0 })?;
        }
    }
    Ok(())
}

fn validate_sync_add_path(path: &Path) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("cannot inspect sync root {}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!("sync root must be an existing directory, not a symbolic link");
    }
    Ok(())
}

fn complete_initial_and_enable(
    state: &mut State,
    share: &ShareId,
    yes: bool,
) -> Result<Option<i64>> {
    let generation = state.watch_intent_generation(share)?;
    let root = state.root_for(share)?;
    recover_installs(state)?;
    run_sync(state, &root, false, yes, false, PlanReport::Full)?;
    if state.initial_complete(share)? {
        state.set_initial_complete_and_watch_enabled(share, generation)?;
        return Ok(Some(state.watch_intent_generation(share)?));
    }
    Ok(None)
}

fn select_share(state: &State, path: Option<&Path>, share: Option<&str>) -> Result<ShareId> {
    match (path, share) {
        (Some(_), Some(_)) | (None, None) => bail!("provide exactly one of PATH or --share"),
        (Some(path), None) => Ok(state.find_share(path)?.0),
        (None, Some(share)) => state
            .managed_share(&ShareId(share.into()))
            .map(|share| share.id),
    }
}

fn daemon_socket(state: &State) -> PathBuf {
    state.dir.join("run").join("daemon.sock")
}

fn ensure_daemon(state: &State) -> Result<()> {
    let socket = daemon_socket(state);
    if daemon_request_inner(&socket, &DaemonRequest::List { cursor: None }).is_ok() {
        return Ok(());
    }
    if socket.exists() {
        for _ in 0..20 {
            std::thread::sleep(Duration::from_millis(100));
            if daemon_request_inner(&socket, &DaemonRequest::List { cursor: None }).is_ok() {
                return Ok(());
            }
        }
    }
    if std::env::var_os("FLOCAL_STATE_DIR").is_some()
        && State::managed_state_dir()?.as_deref() != Some(state.dir.as_path())
    {
        bail!(
            "FLOCAL_STATE_DIR selects an unmanaged state directory; run `flocal daemon run` there or rerun `make install` with that setting"
        );
    }
    eprintln!("flocal: daemon is not running; asking the service manager to start it");
    let status = {
        #[cfg(target_os = "linux")]
        {
            Command::new("systemctl")
                .args(["--user", "start", "flocal-daemon.service"])
                .status()
                .context("cannot ask systemd to start flocal-daemon.service")?
        }
        #[cfg(target_os = "macos")]
        {
            Command::new("launchctl")
                .arg("kickstart")
                .arg("-k")
                .arg(launchd_target()?)
                .status()
                .context("cannot ask launchd to start local.file-local.flocal-daemon")?
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            bail!("managed sync is supported on Linux and macOS only")
        }
    };
    if !status.success() {
        bail!("daemon is unavailable; run `make install`");
    }
    eprintln!("flocal: waiting for the daemon control socket");
    for _ in 0..20 {
        if daemon_request_inner(&socket, &DaemonRequest::List { cursor: None }).is_ok() {
            eprintln!("flocal: daemon is ready");
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    bail!("daemon did not start; run `make install` and inspect its user-service logs")
}

fn daemon_request(state: &State, request: DaemonRequest) -> Result<DaemonResponse> {
    match daemon_request_inner(&daemon_socket(state), &request)? {
        DaemonResponse::Error { message } => bail!("{message}"),
        response => Ok(response),
    }
}

fn daemon_request_inner(socket: &Path, request: &DaemonRequest) -> Result<DaemonResponse> {
    let mut stream = UnixStream::connect(socket)
        .with_context(|| format!("cannot connect to {}", socket.display()))?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.set_write_timeout(Some(Duration::from_secs(10)))?;
    serde_json::to_writer(&mut stream, request)?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    serde_json::from_slice(&read_daemon_message(&mut stream)?)
        .context("daemon sent an invalid response")
}

fn read_daemon_message(stream: &mut impl Read) -> Result<Vec<u8>> {
    let mut message = Vec::new();
    let mut buffer = [0u8; 4096];
    loop {
        let count = stream.read(&mut buffer)?;
        if count == 0 {
            bail!("daemon control connection closed before a complete message")
        }
        let end = buffer[..count]
            .iter()
            .position(|byte| *byte == b'\n')
            .map(|index| index + 1)
            .unwrap_or(count);
        if message.len().saturating_add(end) > MAX_DAEMON_MESSAGE_BYTES {
            bail!(
                "daemon control message exceeds {} bytes",
                MAX_DAEMON_MESSAGE_BYTES
            )
        }
        message.extend_from_slice(&buffer[..end]);
        if end < count || message.last() == Some(&b'\n') {
            return Ok(message);
        }
    }
}

#[cfg(target_os = "linux")]
fn install_daemon_service(state: &State) -> Result<()> {
    if Command::new("id").arg("-u").output()?.stdout == b"0\n" {
        bail!("make install is per-user; do not run it with sudo");
    }
    let executable = std::env::current_exe()?.canonicalize()?;
    validate_trusted_executable(&executable)?;
    install_systemd_service(state, &executable)
}

#[cfg(target_os = "linux")]
fn preflight_daemon_service(state: &State, executable: &Path) -> Result<()> {
    if Command::new("id").arg("-u").output()?.stdout == b"0\n" {
        bail!("make install is per-user; do not run it with sudo");
    }
    let status = Command::new("systemctl")
        .args(["--user", "show-environment"])
        .status()
        .context("cannot query the systemd user manager; log in graphically and retry")?;
    if !status.success() {
        bail!("systemd user services are unavailable; log in graphically and retry")
    }
    if !state.dir.is_absolute() {
        bail!("daemon state path must be absolute")
    }
    ensure_private_service_directory(
        executable
            .parent()
            .context("installed executable has no parent directory")?,
    )?;
    systemd_quote(&state.dir)?;
    Ok(())
}

#[cfg(target_os = "macos")]
fn preflight_daemon_service(_: &State, executable: &Path) -> Result<()> {
    if Command::new("id").arg("-u").output()?.stdout == b"0\n" {
        bail!("make install is per-user; do not run it with sudo");
    }
    ensure_private_service_directory(
        executable
            .parent()
            .context("installed executable has no parent directory")?,
    )?;
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn preflight_daemon_service(_: &State, _: &Path) -> Result<()> {
    bail!("managed sync is supported on Linux and macOS only")
}

#[cfg(target_os = "macos")]
fn install_daemon_service(state: &State) -> Result<()> {
    if Command::new("id").arg("-u").output()?.stdout == b"0\n" {
        bail!("make install is per-user; do not run it with sudo");
    }
    let executable = std::env::current_exe()?.canonicalize()?;
    validate_trusted_executable(&executable)?;
    install_launch_agent(state, &executable)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn install_daemon_service(_: &State) -> Result<()> {
    bail!("managed sync is supported on Linux and macOS only")
}

#[cfg(target_os = "linux")]
fn install_systemd_service(state: &State, executable: &Path) -> Result<()> {
    let environment = Command::new("systemctl")
        .args(["--user", "show-environment"])
        .output()
        .context("cannot query the systemd user manager; log in graphically and retry")?;
    if !environment.status.success() {
        bail!("systemd user services are unavailable; log in graphically and retry")
    }
    let environment = String::from_utf8(environment.stdout)?;
    let variables: std::collections::HashMap<_, _> = environment
        .lines()
        .filter_map(|line| line.split_once('='))
        .collect();
    let config = variables
        .get("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| {
            variables
                .get("HOME")
                .map(|home| PathBuf::from(home).join(".config"))
        })
        .context("could not determine the user service configuration directory")?;
    if !config.is_absolute() || !state.dir.is_absolute() {
        bail!("daemon service paths must be absolute")
    }
    let unit = config.join("systemd/user/flocal-daemon.service");
    let content = systemd_unit_content(executable, &state.dir)?;
    install_managed_state_marker(state)?;
    install_text_file(&unit, &content)?;
    run_manager("systemctl", &["--user", "daemon-reload"])?;
    run_manager(
        "systemctl",
        &["--user", "enable", "--now", "flocal-daemon.service"],
    )?;
    println!("Installed and started flocal-daemon.service");
    Ok(())
}

#[cfg(target_os = "linux")]
fn systemd_quote(path: &Path) -> Result<String> {
    let path = path
        .to_str()
        .context("systemd service paths must be valid UTF-8")?;
    validate_service_path_characters(path)?;
    if path.contains('%') {
        bail!("systemd service paths cannot contain control characters or %")
    }
    Ok(format!(
        "\"{}\"",
        path.replace('\\', "\\\\").replace('"', "\\\"")
    ))
}

#[cfg(target_os = "linux")]
fn systemd_unit_content(executable: &Path, state_dir: &Path) -> Result<String> {
    Ok(format!(
        "[Unit]\nDescription=file.local managed sync\n\n[Service]\nExecStart={} daemon run\nEnvironment=FLOCAL_STATE_DIR={}\nRestart=on-failure\nRestartSec=2\n\n[Install]\nWantedBy=default.target\n",
        systemd_quote(executable)?,
        systemd_quote(state_dir)?,
    ))
}

#[cfg(target_os = "macos")]
fn install_launch_agent(state: &State, executable: &Path) -> Result<()> {
    let target = launchd_target()?;
    let domain = target
        .trim_end_matches("/local.file-local.flocal-daemon")
        .to_owned();
    let available = Command::new("launchctl")
        .args(["print", &domain])
        .status()?
        .success();
    if !available {
        let checkout = std::env::current_dir()?.canonicalize()?;
        println!(
            "Installed binary only. In a macOS graphical session, run `cd {} && make install`.",
            shell_quote(&checkout)?
        );
        return Ok(());
    }
    let home = std::env::var_os("HOME").context("could not determine home directory")?;
    let plist =
        PathBuf::from(home).join("Library/LaunchAgents/local.file-local.flocal-daemon.plist");
    let executable = executable
        .to_str()
        .context("launchd executable path must be valid UTF-8")?;
    let state_dir = state
        .dir
        .to_str()
        .context("launchd state path must be valid UTF-8")?;
    let plist = plist
        .to_str()
        .context("launchd service path must be valid UTF-8")?
        .to_owned();
    let content = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict><key>Label</key><string>local.file-local.flocal-daemon</string><key>ProgramArguments</key><array><string>{}</string><string>daemon</string><string>run</string></array><key>EnvironmentVariables</key><dict><key>FLOCAL_STATE_DIR</key><string>{}</string></dict><key>RunAtLoad</key><true/><key>KeepAlive</key><true/></dict></plist>\n",
        xml_escape(executable)?,
        xml_escape(state_dir)?,
    );
    install_managed_state_marker(state)?;
    install_text_file(Path::new(&plist), &content)?;
    run_manager("launchctl", &["bootstrap", &domain, &plist])?;
    println!("Installed and started local.file-local.flocal-daemon");
    Ok(())
}

#[cfg(target_os = "macos")]
fn launchd_target() -> Result<String> {
    let uid = Command::new("id").arg("-u").output()?;
    let uid = String::from_utf8(uid.stdout)?.trim().to_owned();
    Ok(format!("gui/{uid}/local.file-local.flocal-daemon"))
}

fn validate_service_path_characters(value: &str) -> Result<()> {
    if value.chars().any(|character| {
        let code = character as u32;
        character.is_control()
            || (0xfdd0..=0xfdef).contains(&code)
            || code & 0xffff == 0xfffe
            || code & 0xffff == 0xffff
    }) {
        bail!("service paths cannot contain control characters or XML noncharacters")
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn xml_escape(value: &str) -> Result<String> {
    validate_service_path_characters(value)?;
    Ok(value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;"))
}

#[cfg(target_os = "macos")]
fn shell_quote(path: &Path) -> Result<String> {
    let path = path.to_str().context("checkout path must be valid UTF-8")?;
    Ok(format!("'{}'", path.replace('\'', "'\\''")))
}

fn install_text_file(path: &Path, content: &str) -> Result<()> {
    let parent = path
        .parent()
        .context("service definition has no parent directory")?;
    ensure_private_service_directory(parent)?;
    validate_private_file(path)?;
    let temporary = parent.join(format!(
        ".{}-{}",
        path.file_name().unwrap().to_string_lossy(),
        ShareId::generate().0
    ));
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    set_owner_only_file(&temporary)?;
    file.write_all(content.as_bytes())?;
    file.sync_all()?;
    std::fs::rename(temporary, path)?;
    Ok(())
}

fn install_managed_state_marker(state: &State) -> Result<()> {
    let home = std::env::var_os("HOME").context("could not determine home directory")?;
    let marker = PathBuf::from(home).join(".config/file.local/managed-state");
    let state_dir = state
        .dir
        .to_str()
        .context("managed daemon state path must be valid UTF-8")?;
    install_text_file(&marker, &format!("{state_dir}\n"))
}

#[cfg(unix)]
fn validate_trusted_executable(path: &Path) -> Result<()> {
    use std::os::unix::fs::MetadataExt;

    let parent = path
        .parent()
        .context("installed executable has no parent directory")?;
    validate_service_dir_chain(parent)?;
    let metadata = std::fs::symlink_metadata(path)?;
    let uid = rustix::process::geteuid().as_raw();
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || (metadata.uid() != uid && metadata.uid() != 0)
        || metadata.mode() & 0o022 != 0
        || metadata.mode() & 0o111 == 0
    {
        bail!("installed executable is not a trusted executable file");
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_trusted_executable(_: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn ensure_private_service_directory(path: &Path) -> Result<()> {
    flocal::state::ensure_private_directory(path)
}

#[cfg(not(unix))]
fn ensure_private_service_directory(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path)?;
    Ok(())
}

#[cfg(unix)]
fn validate_service_dir_chain(path: &Path) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    let uid = rustix::process::geteuid().as_raw();
    for component in path.ancestors() {
        let metadata = std::fs::symlink_metadata(component)?;
        #[cfg(target_os = "macos")]
        if metadata.file_type().is_symlink()
            && metadata.uid() == 0
            && matches!(component, path if path == Path::new("/var") || path == Path::new("/tmp"))
        {
            continue;
        }
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            bail!(
                "service directory contains an unsafe path component {}",
                component.display()
            );
        }
        let owner = metadata.uid();
        let mode = metadata.mode();
        if owner != uid && owner != 0 {
            bail!(
                "service directory component {} has an unexpected owner",
                component.display()
            );
        }
        if mode & 0o022 != 0 && !(owner == 0 && mode & 0o1000 != 0) {
            bail!(
                "service directory component {} is writable by another user",
                component.display()
            );
        }
    }
    Ok(())
}

fn run_manager(program: &str, arguments: &[&str]) -> Result<()> {
    let status = Command::new(program).args(arguments).status()?;
    if !status.success() {
        bail!("{program} {} failed", arguments.join(" "));
    }
    Ok(())
}

fn daemon_run(mut state: State) -> Result<()> {
    recover_daemon_installs(&mut state)?;
    state.ensure_private_state_child("run")?;
    validate_private_file(&state.dir.join("daemon.lock"))?;
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(state.dir.join("daemon.lock"))?;
    set_owner_only_file(&state.dir.join("daemon.lock"))?;
    fs2::FileExt::try_lock_exclusive(&lock).context("another flocal daemon is already running")?;
    let socket = daemon_socket(&state);
    if let Ok(metadata) = std::fs::symlink_metadata(&socket) {
        use std::os::unix::fs::FileTypeExt;
        if !metadata.file_type().is_socket() {
            bail!(
                "refusing to replace unexpected daemon socket path {}",
                socket.display()
            );
        }
        std::fs::remove_file(&socket)
            .with_context(|| format!("cannot remove stale socket {}", socket.display()))?;
    }
    let listener = UnixListener::bind(&socket)?;
    set_owner_only_socket(&socket)?;
    listener.set_nonblocking(true)?;
    let workers = Arc::new(Mutex::new(
        std::collections::HashMap::<ShareId, DaemonWorker>::new(),
    ));
    let (events_tx, events_rx) = std::sync::mpsc::channel();
    let clients = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let shutdown = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGTERM, shutdown.clone())?;
    restore_watches(&state, &workers, &events_tx)?;
    let mut shutdown_started = None;
    loop {
        while let Ok(event) = events_rx.try_recv() {
            apply_worker_event(&state, &workers, event)?;
        }
        if shutdown.load(Ordering::Relaxed) {
            let started = *shutdown_started.get_or_insert_with(std::time::Instant::now);
            if stop_daemon_workers(&workers)? {
                return Ok(());
            }
            if started.elapsed() >= Duration::from_secs(10) {
                force_stop_daemon_workers(&workers)?;
            }
            if started.elapsed() >= Duration::from_secs(11) {
                bail!("daemon shutdown timed out waiting for workers")
            }
            std::thread::sleep(Duration::from_millis(25));
            continue;
        }
        match listener.accept() {
            Ok((stream, _)) => {
                if clients.fetch_add(1, Ordering::Relaxed) >= MAX_DAEMON_CLIENTS {
                    clients.fetch_sub(1, Ordering::Relaxed);
                    continue;
                }
                let state_dir = state.dir.clone();
                let workers = workers.clone();
                let events = events_tx.clone();
                let clients = clients.clone();
                std::thread::spawn(move || {
                    if let Ok(mut state) = State::open(state_dir) {
                        let _ = handle_daemon_request(&mut state, &workers, &events, stream);
                    }
                    clients.fetch_sub(1, Ordering::Relaxed);
                });
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(25))
            }
            Err(error) => return Err(error.into()),
        }
    }
}

#[cfg(unix)]
fn validate_private_file(path: &Path) -> Result<()> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    use std::os::unix::fs::MetadataExt;
    if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
        bail!("{} is not a private regular file", path.display());
    }
    if metadata.uid() != rustix::process::geteuid().as_raw() || metadata.mode() & 0o077 != 0 {
        bail!("{} is not owned and private to this user", path.display());
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_private_file(_: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_owner_only_file(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    validate_private_file(path)
}

#[cfg(unix)]
fn set_owner_only_socket(path: &Path) -> Result<()> {
    use std::os::unix::fs::{FileTypeExt, PermissionsExt};
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_socket() {
        bail!("{} is not a socket", path.display());
    }
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_owner_only_socket(_: &Path) -> Result<()> {
    Ok(())
}

#[cfg(not(unix))]
fn set_owner_only_file(_: &Path) -> Result<()> {
    Ok(())
}

fn restore_watches(
    state: &State,
    workers: &Arc<Mutex<std::collections::HashMap<ShareId, DaemonWorker>>>,
    events: &std::sync::mpsc::Sender<WorkerEvent>,
) -> Result<()> {
    for share in state.managed_shares()? {
        if share.peer.is_some()
            && share.initial_complete
            && share.watch_enabled
            && share.blocked_diagnostic.is_none()
        {
            start_worker(state, workers, events, share.id)?;
        }
    }
    Ok(())
}

fn stop_daemon_workers(
    workers: &Arc<Mutex<std::collections::HashMap<ShareId, DaemonWorker>>>,
) -> Result<bool> {
    let workers = workers
        .lock()
        .map_err(|_| anyhow::anyhow!("daemon worker state is poisoned"))?;
    for worker in workers.values() {
        worker.stopping.store(true, Ordering::Relaxed);
        worker.stop.store(true, Ordering::Relaxed);
    }
    Ok(workers
        .values()
        .all(|worker| worker.finished.load(Ordering::Relaxed)))
}

fn force_stop_daemon_workers(
    workers: &Arc<Mutex<std::collections::HashMap<ShareId, DaemonWorker>>>,
) -> Result<()> {
    let workers = workers
        .lock()
        .map_err(|_| anyhow::anyhow!("daemon worker state is poisoned"))?;
    for worker in workers.values() {
        let mut child = worker
            .child
            .lock()
            .map_err(|_| anyhow::anyhow!("daemon worker child state is poisoned"))?;
        if let Some(mut child) = child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
    Ok(())
}

fn handle_daemon_request(
    state: &mut State,
    workers: &Arc<Mutex<std::collections::HashMap<ShareId, DaemonWorker>>>,
    events: &std::sync::mpsc::Sender<WorkerEvent>,
    mut stream: UnixStream,
) -> Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.set_write_timeout(Some(Duration::from_secs(10)))?;
    let response = match read_daemon_message(&mut stream)
        .and_then(|message| serde_json::from_slice::<DaemonRequest>(&message).map_err(Into::into))
    {
        Ok(DaemonRequest::List { cursor }) => match daemon_sync_page(state, workers, cursor) {
            Ok(response) => response,
            Err(error) => DaemonResponse::Error {
                message: format!("{error:#}"),
            },
        },
        Ok(DaemonRequest::Start { share, generation }) => {
            let share = ShareId(share);
            match start_managed_share(state, workers, events, share, generation) {
                Ok(()) => DaemonResponse::Ok,
                Err(error) => DaemonResponse::Error {
                    message: format!("{error:#}"),
                },
            }
        }
        Ok(DaemonRequest::Stop { share }) => {
            let share = ShareId(share);
            match stop_managed_share(state, workers, share) {
                Ok(()) => DaemonResponse::Ok,
                Err(error) => DaemonResponse::Error {
                    message: format!("{error:#}"),
                },
            }
        }
        Err(error) => DaemonResponse::Error {
            message: format!("invalid daemon request: {error}"),
        },
    };
    let mut bytes = serde_json::to_vec(&response)?;
    bytes.push(b'\n');
    if bytes.len() > MAX_DAEMON_MESSAGE_BYTES {
        bail!("daemon response exceeds {} bytes", MAX_DAEMON_MESSAGE_BYTES);
    }
    stream.write_all(&bytes)?;
    stream.flush()?;
    Ok(())
}

fn daemon_syncs(
    state: &State,
    workers: &Arc<Mutex<std::collections::HashMap<ShareId, DaemonWorker>>>,
) -> Result<Vec<DaemonSync>> {
    let workers = workers
        .lock()
        .map_err(|_| anyhow::anyhow!("daemon worker state is poisoned"))?;
    state
        .managed_shares()?
        .into_iter()
        .map(|share| {
            let state_name = if share.blocked_diagnostic.is_some() {
                "blocked"
            } else if let Some(worker) = workers.get(&share.id) {
                if worker.stopping.load(Ordering::Relaxed) {
                    "stopping"
                } else {
                    match worker.state.load(Ordering::Relaxed) {
                        WORKER_STARTING => "starting",
                        WORKER_RECONNECTING => "reconnecting",
                        WORKER_WATCHING => "watching",
                        _ => "starting",
                    }
                }
            } else {
                "stopped"
            };
            Ok(DaemonSync {
                share: share.id.0.clone(),
                root: daemon_path(&path_bytes(&share.root)),
                host: share.peer.as_ref().map(|peer| peer.host.clone()),
                remote_path: share
                    .peer
                    .as_ref()
                    .map(|peer| daemon_path(&peer.remote_path)),
                enabled: share.watch_enabled,
                initial_complete: share.initial_complete,
                state: state_name.into(),
                diagnostic: share.blocked_diagnostic,
                unsettled: state.unsettled_paths(&share.id)?.len(),
            })
        })
        .collect()
}

fn daemon_sync_page(
    state: &State,
    workers: &Arc<Mutex<std::collections::HashMap<ShareId, DaemonWorker>>>,
    cursor: Option<String>,
) -> Result<DaemonResponse> {
    let syncs = daemon_syncs(state, workers)?;
    let total = syncs.len();
    let start = cursor
        .as_deref()
        .map(|cursor| {
            syncs
                .iter()
                .position(|sync| sync.share == cursor)
                .map(|index| index + 1)
                .context("daemon list continuation is out of range")
        })
        .transpose()?
        .unwrap_or(0);
    let mut page = Vec::new();
    for sync in syncs.into_iter().skip(start) {
        let next = sync.share.clone();
        page.push(sync);
        let response = DaemonResponse::List {
            syncs: page.clone(),
            next: Some(next),
        };
        if serde_json::to_vec(&response)?.len() + 1 > MAX_DAEMON_MESSAGE_BYTES {
            page.pop();
            if page.is_empty() {
                bail!("one managed sync exceeds the daemon list page limit");
            }
            break;
        }
    }
    let next = (start + page.len() < total).then(|| page.last().unwrap().share.clone());
    Ok(DaemonResponse::List { syncs: page, next })
}

fn start_managed_share(
    state: &mut State,
    workers: &Arc<Mutex<std::collections::HashMap<ShareId, DaemonWorker>>>,
    events: &std::sync::mpsc::Sender<WorkerEvent>,
    share: ShareId,
    generation: Option<i64>,
) -> Result<()> {
    let managed = state.managed_share(&share)?;
    if managed.peer.is_none() {
        bail!("this machine is responder-only for the selected share");
    }
    if !managed.initial_complete {
        bail!("initial synchronization is incomplete; rerun `flocal sync start PATH` to review it");
    }
    if let Some(worker) = workers
        .lock()
        .map_err(|_| anyhow::anyhow!("daemon worker state is poisoned"))?
        .get(&share)
    {
        if worker.stopping.load(Ordering::Relaxed) {
            bail!("sync is still stopping; retry once it disappears from `flocal sync list`");
        }
        return Ok(());
    }
    state.validate_root_identity(&share)?;
    recover_daemon_share_install(state, &share)?;
    state.clear_blocked(&share)?;
    if let Some(generation) = generation {
        state.set_watch_enabled_if_generation(&share, true, generation)?;
    } else {
        state.set_watch_enabled(&share, true)?;
    }
    start_worker(state, workers, events, share)
}

fn stop_managed_share(
    state: &State,
    workers: &Arc<Mutex<std::collections::HashMap<ShareId, DaemonWorker>>>,
    share: ShareId,
) -> Result<()> {
    state.managed_share(&share)?;
    state.set_watch_enabled(&share, false)?;
    state.clear_blocked(&share)?;
    let finished = {
        let workers = workers
            .lock()
            .map_err(|_| anyhow::anyhow!("daemon worker state is poisoned"))?;
        workers.get(&share).map(|worker| {
            worker.stopping.store(true, Ordering::Relaxed);
            worker.stop.store(true, Ordering::Relaxed);
            worker.finished.clone()
        })
    };
    if let Some(finished) = finished {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !finished.load(Ordering::Relaxed) && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(25));
        }
        if !finished.load(Ordering::Relaxed) {
            bail!("sync stop is still waiting for the active round to finish; it remains disabled");
        }
    }
    Ok(())
}

fn start_worker(
    state: &State,
    workers: &Arc<Mutex<std::collections::HashMap<ShareId, DaemonWorker>>>,
    events: &std::sync::mpsc::Sender<WorkerEvent>,
    share: ShareId,
) -> Result<()> {
    let stop = Arc::new(AtomicBool::new(false));
    let live = Arc::new(std::sync::atomic::AtomicU8::new(WORKER_STARTING));
    let stopping = Arc::new(AtomicBool::new(false));
    let finished = Arc::new(AtomicBool::new(false));
    let child = Arc::new(Mutex::new(None));
    let worker_id = NEXT_WORKER_ID.fetch_add(1, Ordering::Relaxed);
    {
        let mut workers = workers
            .lock()
            .map_err(|_| anyhow::anyhow!("daemon worker state is poisoned"))?;
        if workers.contains_key(&share) {
            return Ok(());
        }
        workers.insert(
            share.clone(),
            DaemonWorker {
                id: worker_id,
                stop: stop.clone(),
                state: live.clone(),
                stopping,
                finished: finished.clone(),
                child: child.clone(),
            },
        );
    }
    let state_dir = state.dir.clone();
    let events = events.clone();
    std::thread::spawn(move || {
        let result = run_daemon_worker(&state_dir, &share, &stop, &live, child);
        let error = result.err().map(|error| format!("{error:#}"));
        finished.store(true, Ordering::Relaxed);
        let _ = events.send(WorkerEvent::Exited {
            share,
            worker_id,
            error,
        });
    });
    Ok(())
}

fn run_daemon_worker(
    state_dir: &Path,
    share: &ShareId,
    stop: &AtomicBool,
    live: &std::sync::atomic::AtomicU8,
    child: Arc<Mutex<Option<Child>>>,
) -> Result<()> {
    let mut state = State::open(state_dir)?;
    if !state.managed_share(share)?.watch_enabled {
        return Ok(());
    }
    state.validate_root_identity(share)?;
    let root = state.root_for(share)?;
    let _share_lock = state.lock_share(share)?;
    live.store(WORKER_RECONNECTING, Ordering::Relaxed);
    persistent_watch_loop_control(
        &mut state,
        share,
        &root,
        &mut io::stderr(),
        &mut io::stderr(),
        Some(stop),
        Some(live),
        child,
    )
}

fn apply_worker_event(
    state: &State,
    workers: &Arc<Mutex<std::collections::HashMap<ShareId, DaemonWorker>>>,
    event: WorkerEvent,
) -> Result<()> {
    match event {
        WorkerEvent::Exited {
            share,
            worker_id,
            error,
        } => {
            let removed = {
                let mut workers = workers
                    .lock()
                    .map_err(|_| anyhow::anyhow!("daemon worker state is poisoned"))?;
                (workers.get(&share).map(|worker| worker.id) == Some(worker_id))
                    .then(|| workers.remove(&share))
                    .flatten()
                    .is_some()
            };
            if removed
                && let Some(error) = error
                && state.managed_share(&share)?.watch_enabled
            {
                state.set_blocked(&share, &error)?;
            }
        }
    }
    Ok(())
}

fn recover_installs(state: &mut State) -> Result<()> {
    let _global_lock = state.lock_global_sync()?;
    for (share, intent) in state.install_intents()? {
        let _lock = state.lock_share(&share)?;
        recover_install_plan(state, &share, &intent)?;
    }
    Ok(())
}

fn recover_daemon_installs(state: &mut State) -> Result<()> {
    let _global_lock = state.lock_global_sync()?;
    for (share, intent) in state.install_intents()? {
        let recovery = recover_daemon_share_install_locked(state, &share, &intent);
        if let Err(error) = recovery {
            state.set_blocked(&share, &format!("{error:#}"))?;
        }
    }
    Ok(())
}

fn recover_daemon_share_install(state: &mut State, share: &ShareId) -> Result<()> {
    let _global_lock = state.lock_global_sync()?;
    let Some(intent) = state.install_intent(share)? else {
        return Ok(());
    };
    recover_daemon_share_install_locked(state, share, &intent)
}

fn recover_daemon_share_install_locked(
    state: &mut State,
    share: &ShareId,
    intent: &flocal::state::InstallIntent,
) -> Result<()> {
    state.validate_root_identity(share)?;
    let _lock = state.lock_share(share)?;
    recover_install_plan(state, share, intent)
}

fn recover_install_plan(
    state: &mut State,
    share: &ShareId,
    intent: &flocal::state::InstallIntent,
) -> Result<()> {
    let plan = flocal::reconcile::Plan {
        records: intent.records.clone(),
        conflicts: intent.conflicts.clone(),
        merges: Vec::new(),
    };
    match sync::apply_complete_plan(state, share, &plan) {
        Ok(()) => Ok(()),
        Err(error) if error.downcast_ref::<sync::ApplyInvalidated>().is_some() => Ok(()),
        Err(error) => {
            Err(error).with_context(|| format!("recovering interrupted install for {}", share.0))
        }
    }
}

fn add_peer(state: &State, path: &Path, host: &str, remote_path: &Path) -> Result<()> {
    validate_host(host)?;
    if !remote_path.is_absolute() {
        bail!("--remote-path must be absolute");
    }
    let (share, _) = state.find_share(path)?;
    let executable = discover_executable(host)?;
    let mut remote = Remote::spawn(host, &executable)?;
    sync::write_message(
        &mut remote.input,
        &Message::Register {
            protocol: sync::PROTOCOL_VERSION,
            share: share.clone(),
            peer: state.peer_id()?,
            root: path_bytes(remote_path),
        },
    )?;
    let peer_id = match sync::read_message(&mut remote.output)? {
        Message::Accepted { protocol, peer } if protocol == sync::PROTOCOL_VERSION => peer,
        Message::Accepted { .. } => bail!("remote protocol version is incompatible"),
        Message::Error { message } => bail!("remote rejected pairing: {}", escaped(&message)),
        other => bail!("unexpected pairing response: {other:?}"),
    };
    remote.finish()?;
    state.set_peer(
        &share,
        &PeerConfig {
            peer_id,
            host: host.into(),
            remote_path: path_bytes(remote_path),
            executable,
        },
    )?;
    println!(
        "Connected {} to {}:{}",
        share.0,
        host,
        remote_path.display()
    );
    Ok(())
}

fn list_peer(state: &State, path: &Path, json: bool) -> Result<()> {
    let (share, _) = state.find_share(path)?;
    let peer = state.peer(&share)?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({"schema": 1, "peer": peer}))?
        );
    } else if let Some(peer) = peer {
        println!("{}:{}", peer.host, bytes_path(&peer.remote_path).display());
    } else {
        println!("No peer configured");
    }
    Ok(())
}

/// Distinguishes the explicit `flocal sync` invocation — whose plan is the
/// full, unabridged action list — from `watch`'s repeating background sync,
/// whose plan is timestamped per line and silent about paths needing no
/// action, so a live watch log stays readable.
#[derive(Clone, Copy, PartialEq, Eq)]
enum PlanReport {
    Full,
    Preview,
    Watch,
}

#[derive(Default)]
#[allow(dead_code)]
struct SyncCompletion {
    watch_report: Option<Vec<u8>>,
    post_commit_error: Option<anyhow::Error>,
}

fn run_sync(
    state: &mut State,
    path: &Path,
    dry_run: bool,
    yes: bool,
    json: bool,
    report: PlanReport,
) -> Result<SyncCompletion> {
    let _global_lock = state.lock_global_sync()?;
    let (share, _) = state.find_share(path)?;
    let _share_lock = state.lock_share(&share)?;
    state.clear_pending_objects(&share)?;
    state.prune_unreferenced_objects()?;
    let peer = state
        .peer(&share)?
        .context("no peer configured; run `flocal peer add`")?;
    let local = if dry_run {
        sync::preview_refresh(state, &share)?
    } else {
        sync::refresh(state, &share)?
    };
    let mut remote = Remote::spawn(&peer.host, &peer.executable)?;
    sync::write_message(
        &mut remote.input,
        &Message::Sync {
            protocol: sync::PROTOCOL_VERSION,
            share: share.clone(),
            peer: state.peer_id()?,
            dry_run,
        },
    )?;
    match sync::read_message(&mut remote.output)? {
        Message::Accepted {
            protocol,
            peer: actual,
        } if protocol == sync::PROTOCOL_VERSION && actual == peer.peer_id => {}
        Message::Accepted { .. } => bail!("remote protocol or peer identity changed"),
        Message::Error { message } => bail!("remote rejected sync: {}", escaped(&message)),
        other => bail!("unexpected handshake response: {other:?}"),
    }
    let remote_records = sync::read_snapshot(&mut remote.output)?;
    state.validate_remote_records(&share, &local, &remote_records)?;
    let mut plan = sync::plan(&local, &remote_records);

    let root = state.root_for(&share)?;
    let matcher = flocal::scan::IgnoreMatcher::new(&root)?;
    let mut required_records = sync::plan_records_with_inputs(&plan);
    required_records.retain(|record| !matcher.is_record_ignored(record));
    let remote_authorized = sync::authorized_hashes(&remote_records);
    let mut needs = sync::required_hashes_for_share(state, &share, &required_records)?;
    needs.retain(|hash| remote_authorized.contains(hash));
    sync::write_message(
        &mut remote.input,
        &Message::Need {
            hashes: needs.clone(),
        },
    )?;
    let mut expected_sizes = std::collections::HashMap::new();
    for record in &required_records {
        if let Entry::File { hash, size, .. } = &record.version.entry {
            expected_sizes.insert(hash.clone(), *size);
        }
    }
    let transfer_limit = sync::max_transfer_bytes_per_session();
    let mut received_bytes = 0u64;
    for expected in needs {
        let response = match sync::read_message(&mut remote.output) {
            Ok(response) => response,
            Err(error) => return Err(remote.finish_after_error(error)),
        };
        match response {
            Message::ObjectStart { hash, size } if hash == expected => {
                if expected_sizes.get(&hash) != Some(&size) {
                    bail!("peer object size differs from the validated plan");
                }
                received_bytes = received_bytes.saturating_add(size);
                if received_bytes > transfer_limit {
                    bail!("inbound object transfer exceeds session byte limit");
                }
                if let Err(error) =
                    sync::receive_object_for_share(state, &share, hash, size, &mut remote.output)
                {
                    return Err(remote.finish_after_error(error));
                }
            }
            other => bail!("expected object {expected}, got {other:?}"),
        }
    }
    let completion = match sync::read_message(&mut remote.output) {
        Ok(response) => response,
        Err(error) => return Err(remote.finish_after_error(error)),
    };
    match completion {
        Message::Done => {}
        other => bail!("expected object completion, got {other:?}"),
    }

    let needs_confirmation = !state.initial_complete(&share)? && !yes;
    if dry_run || needs_confirmation {
        let mut preview = plan.clone();
        sync::preview_merges(state, &mut preview)?;
        if report == PlanReport::Full {
            print_plan(&local, &remote_records, &preview, json, PlanReport::Preview)?;
        }
    }
    if dry_run {
        sync::write_message(&mut remote.input, &Message::Cancel)?;
        state.clear_pending_objects(&share)?;
        state.prune_unreferenced_objects()?;
        remote.finish()?;
        return Ok(SyncCompletion::default());
    }
    if needs_confirmation && !confirm("Apply this initial plan?")? {
        sync::write_message(&mut remote.input, &Message::Cancel)?;
        state.clear_pending_objects(&share)?;
        state.prune_unreferenced_objects()?;
        remote.finish()?;
        return Ok(SyncCompletion::default());
    }
    sync::materialize_merges(state, &share, &mut plan)?;
    if report == PlanReport::Full && !needs_confirmation {
        print_plan(&local, &remote_records, &plan, json, report)?;
    }

    sync::write_snapshot(&mut remote.input, &local)?;
    sync::write_plan(&mut remote.input, &plan)?;
    let response = match sync::read_message(&mut remote.output) {
        Ok(response) => response,
        Err(error) => return Err(remote.finish_after_error(error)),
    };
    let remote_needs = match response {
        Message::Need { hashes } => hashes,
        other => bail!("expected remote object request, got {other:?}"),
    };
    let unique: std::collections::HashSet<_> = remote_needs.iter().collect();
    if unique.len() != remote_needs.len() {
        bail!("peer object request contains duplicate hashes");
    }
    let mut allowed_outbound = sync::authorized_hashes(&local);
    allowed_outbound.extend(sync::authorized_hashes(&plan.records));
    let mut outbound_bytes = 0u64;
    for hash in remote_needs {
        if !allowed_outbound.contains(&hash) {
            bail!("peer requested an object outside this share");
        }
        outbound_bytes =
            outbound_bytes.saturating_add(state.open_verified_object(&hash)?.metadata()?.len());
        if outbound_bytes > transfer_limit {
            bail!("outbound object transfer exceeds session byte limit");
        }
        sync::send_object(state, &hash, &mut remote.input)?;
    }
    sync::write_message(&mut remote.input, &Message::Done)?;
    let mut remote_heads = Vec::new();
    loop {
        let response = match sync::read_message(&mut remote.output) {
            Ok(response) => response,
            Err(error) => return Err(remote.finish_after_error(error)),
        };
        match response {
            Message::HeadChunk { records } => remote_heads.extend(records),
            Message::Applied => break,
            Message::Error { message } => bail!("remote apply failed: {}", escaped(&message)),
            other => bail!("expected apply acknowledgement, got {other:?}"),
        }
        if remote_heads.len() > sync::MAX_RECORDS_PER_SESSION {
            bail!("peer acknowledged-head manifest exceeds record limit");
        }
    }
    sync::apply_complete_plan(state, &share, &plan)?;
    state.set_initial_complete(&share)?;
    let current = state.records(&share)?;
    sync::validate_ack_heads(&remote_heads, &remote_heads)?;
    let shared_heads = sync::intersect_heads(&current, &remote_heads)?;
    state.acknowledge_shared_heads(&share, &shared_heads)?;
    state.prune_unreferenced_objects()?;
    let committed_report = if report == PlanReport::Watch {
        let mut output = Vec::new();
        write_plan_report(&mut output, &local, &remote_records, &plan, json, report)?;
        Some(output)
    } else {
        None
    };
    let finalization = sync::write_heads(&mut remote.input, &shared_heads)
        .and_then(|()| sync::write_message(&mut remote.input, &Message::CommitAck))
        .and_then(|()| remote.finish());
    if report == PlanReport::Watch {
        Ok(SyncCompletion {
            watch_report: committed_report,
            post_commit_error: finalization.err(),
        })
    } else {
        finalization?;
        Ok(SyncCompletion::default())
    }
}

fn serve(state: &mut State) -> Result<()> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let initial = sync::read_initial_message_until(
        &stdin,
        std::time::Instant::now() + sync::default_frame_deadline(),
    )?;
    match initial {
        InitialMessage::WatchOpen {
            protocol,
            share,
            peer,
        } => serve_watch_open(state, protocol, share, peer, &stdin, &stdout),
        initial => {
            let mut input = BufReader::new(TimedReader::new(stdin));
            let mut output = BufWriter::new(stdout.lock());
            serve_initial(state, initial, &mut input, &mut output)
        }
    }
}

#[cfg(test)]
fn serve_io(
    state: &mut State,
    mut input: &mut (impl Read + Send),
    output: &mut impl Write,
) -> Result<()> {
    let initial = sync::read_initial_message(&mut input)?;
    serve_initial(state, initial, input, output)
}

fn serve_initial(
    state: &mut State,
    initial: InitialMessage,
    mut input: &mut impl Read,
    mut output: &mut impl Write,
) -> Result<()> {
    match initial {
        InitialMessage::Register {
            protocol,
            share,
            peer,
            root,
        } => {
            if protocol != sync::PROTOCOL_VERSION {
                sync::write_message(
                    &mut output,
                    &Message::Error {
                        message: format!("unsupported protocol version {protocol}"),
                    },
                )?;
                return Ok(());
            }
            let _global_lock = match state.lock_global_sync() {
                Ok(lock) => lock,
                Err(error) => {
                    sync::write_message(
                        &mut output,
                        &Message::Error {
                            message: format!("{error:#}"),
                        },
                    )?;
                    return Ok(());
                }
            };
            let _share_lock = match state.lock_share(&share) {
                Ok(lock) => lock,
                Err(error) => {
                    sync::write_message(
                        &mut output,
                        &Message::Error {
                            message: format!("{error:#}"),
                        },
                    )?;
                    return Ok(());
                }
            };
            if let Err(error) = state.register_share_bound(&share, &bytes_path(&root), &peer) {
                sync::write_message(
                    &mut output,
                    &Message::Error {
                        message: format!("{error:#}"),
                    },
                )?;
                return Ok(());
            }
            sync::write_message(
                &mut output,
                &Message::Accepted {
                    protocol: sync::PROTOCOL_VERSION,
                    peer: state.peer_id()?,
                },
            )?;
        }
        InitialMessage::Sync {
            protocol,
            share,
            peer,
            dry_run,
        } => {
            if protocol != sync::PROTOCOL_VERSION {
                sync::write_message(
                    &mut output,
                    &Message::Error {
                        message: format!("unsupported protocol version {protocol}"),
                    },
                )?;
                return Ok(());
            }
            let _global_lock = match state.lock_global_sync() {
                Ok(lock) => lock,
                Err(error) => {
                    sync::write_message(
                        &mut output,
                        &Message::Error {
                            message: format!("{error:#}"),
                        },
                    )?;
                    return Ok(());
                }
            };
            let _share_lock = match state.lock_share(&share) {
                Ok(lock) => lock,
                Err(error) => {
                    sync::write_message(
                        &mut output,
                        &Message::Error {
                            message: format!("{error:#}"),
                        },
                    )?;
                    return Ok(());
                }
            };
            if state.bound_peer(&share)?.as_ref() != Some(&peer) {
                sync::write_message(
                    &mut output,
                    &Message::Error {
                        message: "peer identity mismatch".into(),
                    },
                )?;
                return Ok(());
            }
            sync::write_message(
                &mut output,
                &Message::Accepted {
                    protocol: sync::PROTOCOL_VERSION,
                    peer: state.peer_id()?,
                },
            )?;
            let records = if dry_run {
                sync::preview_refresh(state, &share)?
            } else {
                sync::refresh(state, &share)?
            };
            sync::write_snapshot(&mut output, &records)?;
            serve_sync(state, &share, &peer, &records, &mut input, &mut output)?;
        }
        InitialMessage::WatchOpen { .. } => {
            bail!("persistent watch requires a descriptor-backed protocol transport")
        }
    }
    Ok(())
}

fn serve_watch_open(
    state: &mut State,
    protocol: u32,
    share: ShareId,
    peer: flocal::model::PeerId,
    input: &impl AsFd,
    output: &impl AsFd,
) -> Result<()> {
    if protocol != sync::WATCH_PROTOCOL_VERSION {
        return write_v2_session(
            output,
            V2SessionFrame::Error {
                retryable: true,
                message: format!("unsupported persistent watch protocol version {protocol}"),
            },
        );
    }
    let _share_lock = match state.lock_share(&share) {
        Ok(lock) => lock,
        Err(error) => {
            return write_v2_session(
                output,
                V2SessionFrame::Error {
                    retryable: true,
                    message: format!("{error:#}"),
                },
            );
        }
    };
    if state.bound_peer(&share)?.as_ref() != Some(&peer) {
        return write_v2_session(
            output,
            V2SessionFrame::Error {
                retryable: false,
                message: "peer identity mismatch".into(),
            },
        );
    }
    let share_root = match sync::ShareRoot::open(state, &share) {
        Ok(root) => root,
        Err(error) => {
            return write_v2_session(
                output,
                V2SessionFrame::Error {
                    retryable: root_validation_retryable(&error),
                    message: format!("{error:#}"),
                },
            );
        }
    };
    let root = state.root_for(&share)?;
    let watch_state = flocal::watch::WatchState::default();
    let (watch_tx, watch_rx) = std::sync::mpsc::sync_channel(1);
    let callback_state = watch_state.clone();
    let mut watcher: RecommendedWatcher =
        match notify::recommended_watcher(move |event: notify::Result<notify::Event>| match event {
            Ok(event) if event.need_rescan() => {
                callback_state.lost("filesystem watcher reported an event gap", &watch_tx, ())
            }
            Ok(_) => callback_state.changed(&watch_tx, ()),
            Err(error) => callback_state.lost(error, &watch_tx, ()),
        }) {
            Ok(watcher) => watcher,
            Err(error) => {
                return write_v2_session(
                    output,
                    V2SessionFrame::Error {
                        retryable: true,
                        message: format!("cannot create filesystem watcher: {error}"),
                    },
                );
            }
        };
    if let Err(error) = watcher.watch(&root, RecursiveMode::Recursive) {
        return write_v2_session(
            output,
            V2SessionFrame::Error {
                retryable: true,
                message: format!("cannot watch share root: {error}"),
            },
        );
    }
    write_v2_session(
        output,
        V2SessionFrame::WatchAccepted {
            protocol: sync::WATCH_PROTOCOL_VERSION,
            peer: state.peer_id()?,
        },
    )?;
    write_watch_ready(output, 0, &state.unsettled_paths(&share)?)?;
    if let Err(error) = read_watch_ready(state, &share, input) {
        write_v2_session(
            output,
            V2SessionFrame::Error {
                retryable: false,
                message: format!("{error:#}"),
            },
        )?;
        return Ok(());
    }

    let config = flocal::watch::WatchConfig::default();
    let mut debounce = flocal::watch::Debounce::default();
    let mut advertised_generation = 0u64;
    let mut round = 0u64;
    let mut invalidation_cycle = InvalidationCycle::default();
    loop {
        if watch_rx.try_recv().is_ok() {
            debounce.notify(std::time::Instant::now());
        }
        let generation = match watch_state.snapshot() {
            flocal::watch::WatchSnapshot::Healthy { generation } => generation,
            flocal::watch::WatchSnapshot::Lost(error) => {
                write_v2_session(
                    output,
                    V2SessionFrame::Error {
                        retryable: true,
                        message: format!("filesystem watcher stopped: {error}"),
                    },
                )?;
                return Ok(());
            }
        };
        if generation > advertised_generation
            && debounce.take_due(std::time::Instant::now(), &config)
        {
            write_v2_session(output, V2SessionFrame::Changed { generation })?;
            advertised_generation = generation;
        }
        if !sync::input_ready_until(input, std::time::Instant::now() + Duration::from_millis(50))? {
            continue;
        }
        let envelope = sync::read_v2_envelope_until(
            input,
            std::time::Instant::now() + sync::default_frame_deadline(),
        )?;
        match envelope {
            V2Envelope::Session {
                frame: V2SessionFrame::Ping { nonce },
            } => write_v2_session(output, V2SessionFrame::Pong { nonce })?,
            V2Envelope::Round {
                round: incoming,
                frame:
                    V2RoundFrame::SyncStart {
                        connector_generation: _,
                        responder_generation: _,
                    },
            } if incoming == round + 1 => {
                round = incoming;
                let served = serve_v2_round(
                    state,
                    &share,
                    &peer,
                    &share_root,
                    round,
                    input,
                    output,
                    &invalidation_cycle.deferred,
                );
                if let Err(error) = served {
                    let retryable = error.downcast_ref::<WatchProtocolError>().is_none()
                        && error.downcast_ref::<sync::RootIdentityChanged>().is_none();
                    write_v2_session(
                        output,
                        V2SessionFrame::Error {
                            retryable,
                            message: format!("{error:#}"),
                        },
                    )?;
                    return Ok(());
                }
                match served.expect("checked above") {
                    ServedRound::Completed => {
                        invalidation_cycle.reset();
                    }
                    ServedRound::Invalidated(path) => {
                        invalidation_cycle.observe(path);
                    }
                }
            }
            other => {
                write_v2_session(
                    output,
                    V2SessionFrame::Error {
                        retryable: false,
                        message: format!("unexpected persistent watch frame: {other:?}"),
                    },
                )?;
                return Ok(());
            }
        }
    }
}

fn write_v2_session(output: &impl AsFd, frame: V2SessionFrame) -> Result<()> {
    sync::write_v2_envelope_until(
        output,
        &V2Envelope::Session { frame },
        std::time::Instant::now() + sync::default_frame_deadline(),
    )
}

fn v2_session_frame_fits(frame: V2SessionFrame) -> Result<bool> {
    Ok(serde_json::to_vec(&V2Envelope::Session { frame })?.len() <= sync::MAX_FRAME)
}

fn write_watch_ready(
    output: &impl AsFd,
    generation: u64,
    unsettled: &[flocal::model::RelativePath],
) -> Result<()> {
    let deadline = std::time::Instant::now() + sync::default_frame_deadline();
    let mut budget = sync::RoundBudget::new(deadline);
    let mut start = 0;
    while start < unsettled.len() {
        let mut end = start + 1;
        while end < unsettled.len()
            && end - start < 256
            && v2_session_frame_fits(V2SessionFrame::UnsettledChunk {
                paths: unsettled[start..=end].to_vec(),
            })?
        {
            end += 1;
        }
        let frame = V2SessionFrame::UnsettledChunk {
            paths: unsettled[start..end].to_vec(),
        };
        budget.add_metadata(serde_json::to_vec(&frame)?.len())?;
        sync::write_v2_envelope_until(
            output,
            &V2Envelope::Session { frame },
            budget.frame_deadline()?,
        )?;
        start = end;
    }
    sync::write_v2_envelope_until(
        output,
        &V2Envelope::Session {
            frame: V2SessionFrame::Ready { generation },
        },
        budget.frame_deadline()?,
    )
}

fn read_watch_ready(state: &mut State, share: &ShareId, input: &impl AsFd) -> Result<()> {
    let mut unsettled = Vec::new();
    let mut budget =
        sync::RoundBudget::new(std::time::Instant::now() + sync::default_frame_deadline());
    loop {
        match sync::read_v2_envelope_until(input, budget.frame_deadline()?)? {
            V2Envelope::Session {
                frame: frame @ V2SessionFrame::UnsettledChunk { .. },
            } => {
                budget
                    .add_metadata(serde_json::to_vec(&frame)?.len())
                    .map_err(|error| watch_protocol_error(format!("{error:#}")))?;
                let V2SessionFrame::UnsettledChunk { paths } = frame else {
                    unreachable!("matched unsettled chunk")
                };
                let count = unsettled
                    .len()
                    .checked_add(paths.len())
                    .context("persistent watch readiness path count overflow")?;
                anyhow::ensure!(
                    count <= sync::MAX_RECORDS_PER_SESSION,
                    "persistent watch readiness has too many unsettled paths"
                );
                unsettled.extend(paths);
            }
            V2Envelope::Session {
                frame: V2SessionFrame::Ready { .. },
            } => {
                unsettled.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
                unsettled.dedup();
                state.remember_unsettled_paths(share, &unsettled)?;
                return Ok(());
            }
            V2Envelope::Session {
                frame: V2SessionFrame::Error { retryable, message },
            } => return Err(RemoteWatchError { retryable, message }.into()),
            other => {
                return Err(watch_protocol_error(format!(
                    "unexpected persistent readiness frame: {other:?}"
                )));
            }
        }
    }
}

fn write_v2_round(
    output: &impl AsFd,
    round: u64,
    frame: V2RoundFrame,
    budget: &sync::RoundBudget,
) -> Result<()> {
    sync::write_v2_envelope_until(
        output,
        &V2Envelope::Round { round, frame },
        budget.frame_deadline()?,
    )
}

fn recv_v2_round(
    input: &impl AsFd,
    expected_round: u64,
    budget: &sync::RoundBudget,
) -> Result<V2RoundFrame> {
    match sync::read_v2_envelope_until(input, budget.frame_deadline()?)? {
        V2Envelope::Round { round, frame } if round == expected_round => Ok(frame),
        other => Err(watch_protocol_error(format!(
            "unexpected persistent round frame: {other:?}"
        ))),
    }
}

fn write_v2_snapshot(
    output: &impl AsFd,
    round: u64,
    records: &[flocal::model::Record],
    budget: &sync::RoundBudget,
) -> Result<()> {
    let mut start = 0;
    while start < records.len() {
        let mut end = start + 1;
        while end < records.len()
            && end - start < 256
            && v2_frame_fits(
                round,
                V2RoundFrame::SnapshotChunk {
                    records: records[start..=end].to_vec(),
                },
            )?
        {
            end += 1;
        }
        write_v2_round(
            output,
            round,
            V2RoundFrame::SnapshotChunk {
                records: records[start..end].to_vec(),
            },
            budget,
        )?;
        start = end;
    }
    write_v2_round(output, round, V2RoundFrame::SnapshotEnd, budget)
}

fn v2_frame_fits(round: u64, frame: V2RoundFrame) -> Result<bool> {
    Ok(serde_json::to_vec(&V2Envelope::Round { round, frame })?.len() <= sync::MAX_FRAME)
}

fn read_v2_snapshot(
    input: &impl AsFd,
    round: u64,
    budget: &mut sync::RoundBudget,
) -> Result<Vec<flocal::model::Record>> {
    let mut records = Vec::new();
    loop {
        match recv_v2_round(input, round, budget)? {
            V2RoundFrame::SnapshotChunk { records: chunk } => {
                budget
                    .add_metadata(serde_json::to_vec(&chunk)?.len())
                    .map_err(|error| watch_protocol_error(format!("{error:#}")))?;
                records.extend(chunk);
                if records.len() > sync::MAX_RECORDS_PER_SESSION {
                    watch_protocol_bail!("snapshot exceeds session record limit");
                }
            }
            V2RoundFrame::SnapshotEnd => return Ok(records),
            other => watch_protocol_bail!("expected persistent snapshot, got {other:?}"),
        }
    }
}

fn write_v2_plan(
    output: &impl AsFd,
    round: u64,
    plan: &flocal::reconcile::Plan,
    budget: &sync::RoundBudget,
) -> Result<()> {
    let mut start = 0;
    while start < plan.records.len() {
        let mut end = start + 1;
        while end < plan.records.len()
            && end - start < 256
            && v2_frame_fits(
                round,
                V2RoundFrame::ApplyChunk {
                    records: plan.records[start..=end].to_vec(),
                    conflicts: Vec::new(),
                    merges: Vec::new(),
                },
            )?
        {
            end += 1;
        }
        write_v2_round(
            output,
            round,
            V2RoundFrame::ApplyChunk {
                records: plan.records[start..end].to_vec(),
                conflicts: Vec::new(),
                merges: Vec::new(),
            },
            budget,
        )?;
        start = end;
    }
    let mut start = 0;
    while start < plan.conflicts.len() {
        let mut end = start + 1;
        while end < plan.conflicts.len()
            && end - start < 128
            && v2_frame_fits(
                round,
                V2RoundFrame::ApplyChunk {
                    records: Vec::new(),
                    conflicts: plan.conflicts[start..=end].to_vec(),
                    merges: Vec::new(),
                },
            )?
        {
            end += 1;
        }
        write_v2_round(
            output,
            round,
            V2RoundFrame::ApplyChunk {
                records: Vec::new(),
                conflicts: plan.conflicts[start..end].to_vec(),
                merges: Vec::new(),
            },
            budget,
        )?;
        start = end;
    }
    let mut start = 0;
    while start < plan.merges.len() {
        let mut end = start + 1;
        while end < plan.merges.len()
            && end - start < 128
            && v2_frame_fits(
                round,
                V2RoundFrame::ApplyChunk {
                    records: Vec::new(),
                    conflicts: Vec::new(),
                    merges: plan.merges[start..=end].to_vec(),
                },
            )?
        {
            end += 1;
        }
        write_v2_round(
            output,
            round,
            V2RoundFrame::ApplyChunk {
                records: Vec::new(),
                conflicts: Vec::new(),
                merges: plan.merges[start..end].to_vec(),
            },
            budget,
        )?;
        start = end;
    }
    write_v2_round(output, round, V2RoundFrame::ApplyEnd, budget)
}

fn read_v2_plan(
    input: &impl AsFd,
    round: u64,
    budget: &mut sync::RoundBudget,
) -> Result<flocal::reconcile::Plan> {
    let mut plan = flocal::reconcile::Plan {
        records: Vec::new(),
        conflicts: Vec::new(),
        merges: Vec::new(),
    };
    loop {
        match recv_v2_round(input, round, budget)? {
            V2RoundFrame::ApplyChunk {
                records,
                conflicts,
                merges,
            } => {
                budget
                    .add_metadata(serde_json::to_vec(&records)?.len())
                    .map_err(|error| watch_protocol_error(format!("{error:#}")))?;
                budget
                    .add_metadata(serde_json::to_vec(&conflicts)?.len())
                    .map_err(|error| watch_protocol_error(format!("{error:#}")))?;
                budget
                    .add_metadata(serde_json::to_vec(&merges)?.len())
                    .map_err(|error| watch_protocol_error(format!("{error:#}")))?;
                plan.records.extend(records);
                plan.conflicts.extend(conflicts);
                plan.merges.extend(merges);
                if plan.records.len() > sync::MAX_RECORDS_PER_SESSION
                    || plan.conflicts.len() > sync::MAX_RECORDS_PER_SESSION
                    || plan.merges.len() > sync::MAX_RECORDS_PER_SESSION
                {
                    watch_protocol_bail!("apply plan exceeds session record limit");
                }
            }
            V2RoundFrame::ApplyEnd => return Ok(plan),
            other => watch_protocol_bail!("expected persistent apply plan, got {other:?}"),
        }
    }
}

fn write_v2_heads(
    output: &impl AsFd,
    round: u64,
    records: &[flocal::model::Record],
    budget: &sync::RoundBudget,
) -> Result<()> {
    let records = sync::regular_file_heads(records);
    let mut start = 0;
    while start < records.len() {
        let mut end = start + 1;
        while end < records.len()
            && end - start < 256
            && v2_frame_fits(
                round,
                V2RoundFrame::HeadChunk {
                    records: records[start..=end].to_vec(),
                },
            )?
        {
            end += 1;
        }
        write_v2_round(
            output,
            round,
            V2RoundFrame::HeadChunk {
                records: records[start..end].to_vec(),
            },
            budget,
        )?;
        start = end;
    }
    Ok(())
}

fn send_v2_object(
    state: &State,
    hash: &flocal::model::ObjectHash,
    round: u64,
    output: &impl AsFd,
    budget: &mut sync::RoundBudget,
) -> Result<()> {
    let mut file = state.open_verified_object(hash)?;
    let size = file.metadata()?.len();
    budget.add_transfer(size)?;
    write_v2_round(
        output,
        round,
        V2RoundFrame::ObjectStart {
            hash: hash.clone(),
            size,
        },
        budget,
    )?;
    let mut buffer = vec![0; 256 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        write_v2_round(
            output,
            round,
            V2RoundFrame::ObjectChunk {
                data: buffer[..count].to_vec(),
            },
            budget,
        )?;
    }
    write_v2_round(output, round, V2RoundFrame::ObjectEnd, budget)?;
    Ok(())
}

fn receive_v2_object(
    state: &State,
    share: &ShareId,
    hash: flocal::model::ObjectHash,
    size: u64,
    round: u64,
    input: &impl AsFd,
    budget: &sync::RoundBudget,
) -> Result<()> {
    state.mark_object_receiving(share, &hash)?;
    let mut sink = state.begin_object(hash.clone(), size)?;
    loop {
        match recv_v2_round(input, round, budget)? {
            V2RoundFrame::ObjectChunk { data } => sink.write_chunk(&data)?,
            V2RoundFrame::ObjectEnd => {
                sink.finish()?;
                return state.mark_object_verified(share, &hash);
            }
            other => watch_protocol_bail!("unexpected persistent object frame: {other:?}"),
        }
    }
}

enum ServedRound {
    Completed,
    Invalidated(flocal::model::RelativePath),
}

#[allow(clippy::too_many_arguments)]
fn serve_v2_round(
    state: &mut State,
    share: &ShareId,
    connector: &flocal::model::PeerId,
    root: &sync::ShareRoot,
    round: u64,
    input: &impl AsFd,
    output: &impl AsFd,
    deferred: &std::collections::HashSet<Vec<u8>>,
) -> Result<ServedRound> {
    let _global_lock = state.lock_global_sync()?;
    state.clear_pending_objects(share)?;
    state.prune_unreferenced_objects()?;
    let mut budget =
        sync::RoundBudget::new(std::time::Instant::now() + Duration::from_secs(5 * 60));
    budget.check()?;
    let advertised = sync::refresh_with_root(state, share, root)?;
    budget.check()?;
    write_v2_round(output, round, V2RoundFrame::SyncAccepted, &budget)?;
    write_v2_snapshot(output, round, &advertised, &budget)?;

    let requested = match recv_v2_round(input, round, &budget)? {
        V2RoundFrame::Need { hashes } => hashes,
        other => watch_protocol_bail!("expected persistent object request, got {other:?}"),
    };
    let unique: std::collections::HashSet<_> = requested.iter().collect();
    if unique.len() != requested.len() {
        watch_protocol_bail!("object request contains duplicate hashes");
    }
    let allowed = sync::authorized_hashes(&advertised);
    for hash in requested {
        if !allowed.contains(&hash) {
            watch_protocol_bail!("peer requested an object outside this share");
        }
        send_v2_object(state, &hash, round, output, &mut budget)?;
    }
    write_v2_round(output, round, V2RoundFrame::Done, &budget)?;

    let peer_records = read_v2_snapshot(input, round, &mut budget)?;
    state
        .validate_remote_records(share, &advertised, &peer_records)
        .map_err(|error| watch_protocol_error(format!("{error:#}")))?;
    let plan = read_v2_plan(input, round, &mut budget)?;
    let expected = sync::plan(&advertised, &peer_records);
    sync::validate_materialized_plan_shape(&plan, &expected, connector)
        .map_err(|error| watch_protocol_error(format!("{error:#}")))?;
    let applied_plan = effective_plan(&advertised, &peer_records, &plan, deferred);
    let connector_plan = effective_plan(&peer_records, &advertised, &plan, deferred);
    let mut required = sync::plan_records_with_inputs(&expected);
    required.extend(plan.records.clone());
    let needs = sync::required_hashes_for_share(state, share, &required)?;
    let mut expected_sizes = std::collections::HashMap::new();
    for record in &required {
        if let Entry::File { hash, size, .. } = &record.version.entry {
            expected_sizes.insert(hash.clone(), *size);
        }
    }
    write_v2_round(
        output,
        round,
        V2RoundFrame::Need {
            hashes: needs.clone(),
        },
        &budget,
    )?;
    for expected_hash in needs {
        match recv_v2_round(input, round, &budget)? {
            V2RoundFrame::ObjectStart { hash, size } if hash == expected_hash => {
                if expected_sizes.get(&hash) != Some(&size) {
                    watch_protocol_bail!("peer object size differs from the validated plan");
                }
                budget
                    .add_transfer(size)
                    .map_err(|error| watch_protocol_error(format!("{error:#}")))?;
                receive_v2_object(state, share, hash, size, round, input, &budget)?;
            }
            other => {
                watch_protocol_bail!("expected persistent object {expected_hash}, got {other:?}")
            }
        }
    }
    match recv_v2_round(input, round, &budget)? {
        V2RoundFrame::Done => {}
        other => watch_protocol_bail!("expected persistent object completion, got {other:?}"),
    }
    sync::verify_materialized_plan(state, &plan, &expected)
        .map_err(|error| watch_protocol_error(format!("{error:#}")))?;
    budget.check()?;
    if let Err(error) = sync::apply_complete_plan_with_root_skipping(
        state,
        share,
        root,
        &applied_plan.plan,
        &applied_plan.retained_paths,
    ) {
        if let Some(invalidated) = error.downcast_ref::<sync::ApplyInvalidated>() {
            write_v2_round(
                output,
                round,
                V2RoundFrame::RoundInvalidated {
                    path: invalidated.path.clone(),
                },
                &budget,
            )?;
            return Ok(ServedRound::Invalidated(invalidated.path.clone()));
        }
        return Err(error);
    }
    budget.check()?;
    state.set_initial_complete(share)?;
    budget.check()?;
    state.prune_unreferenced_objects()?;
    budget.check()?;
    let current = state.records(share)?;
    write_v2_heads(output, round, &current, &budget)?;
    write_v2_round(output, round, V2RoundFrame::Applied, &budget)?;
    let mut acknowledged = Vec::new();
    loop {
        match recv_v2_round(input, round, &budget)? {
            V2RoundFrame::HeadChunk { records } => {
                budget
                    .add_metadata(serde_json::to_vec(&records)?.len())
                    .map_err(|error| watch_protocol_error(format!("{error:#}")))?;
                acknowledged.extend(records);
                if acknowledged.len() > sync::MAX_RECORDS_PER_SESSION {
                    watch_protocol_bail!("acknowledged-head manifest exceeds record limit");
                }
            }
            V2RoundFrame::SyncFinished => {
                sync::validate_ack_heads(&current, &acknowledged)
                    .map_err(|error| watch_protocol_error(format!("{error:#}")))?;
                state.acknowledge_shared_heads(share, &acknowledged)?;
                if deferred.is_empty() {
                    state.clear_unsettled_paths(share)?;
                }
                return Ok(ServedRound::Completed);
            }
            V2RoundFrame::RoundInvalidated { path }
                if accepts_invalidation(&connector_plan, &path) =>
            {
                state.remember_unsettled_path(share, &path)?;
                return Ok(ServedRound::Invalidated(path));
            }
            other => watch_protocol_bail!("expected persistent round completion, got {other:?}"),
        }
    }
}

fn serve_sync(
    state: &mut State,
    share: &ShareId,
    connector: &flocal::model::PeerId,
    advertised: &[flocal::model::Record],
    input: &mut impl io::Read,
    output: &mut impl Write,
) -> Result<()> {
    state.clear_pending_objects(share)?;
    state.prune_unreferenced_objects()?;
    let mut pending = flocal::reconcile::Plan {
        records: Vec::new(),
        conflicts: Vec::new(),
        merges: Vec::new(),
    };
    let mut plan_ready = false;
    let mut received_bytes = 0u64;
    let mut peer_records = Vec::new();
    let mut peer_snapshot_done = false;
    let mut metadata_bytes = 0usize;
    let mut allowed_outbound = sync::authorized_hashes(advertised);
    for conflict in state.conflicts(share)? {
        allowed_outbound.extend(
            [record_hash(&conflict.winner), record_hash(&conflict.loser)]
                .into_iter()
                .flatten(),
        );
    }
    let mut outbound_bytes = 0u64;
    let mut need_served = false;
    let mut requested_inbound = std::collections::HashMap::new();
    let mut applied_records = None;
    let mut acknowledged_heads = Vec::new();
    loop {
        match sync::read_message(input)? {
            Message::Need { hashes } => {
                if need_served || peer_snapshot_done || plan_ready {
                    bail!("object request received out of order");
                }
                let unique: std::collections::HashSet<_> = hashes.iter().collect();
                if unique.len() != hashes.len() {
                    bail!("object request contains duplicate hashes");
                }
                for hash in hashes {
                    if !allowed_outbound.contains(&hash) {
                        bail!("peer requested an object outside this share");
                    }
                    outbound_bytes = outbound_bytes
                        .saturating_add(state.open_verified_object(&hash)?.metadata()?.len());
                    if outbound_bytes > sync::max_transfer_bytes_per_session() {
                        bail!("outbound object transfer exceeds session byte limit");
                    }
                    sync::send_object(state, &hash, output)?;
                }
                need_served = true;
                sync::write_message(output, &Message::Done)?;
            }
            Message::ObjectStart { hash, size } => {
                if !plan_ready {
                    bail!("object received before a validated apply plan");
                }
                let Some(expected_size) = requested_inbound.remove(&hash) else {
                    bail!("unsolicited or duplicate object received");
                };
                if size != expected_size {
                    bail!("object size differs from the validated plan");
                }
                received_bytes = received_bytes.saturating_add(size);
                if received_bytes > sync::max_transfer_bytes_per_session() {
                    bail!("object transfer exceeds session byte limit");
                }
                sync::receive_object_for_share(state, share, hash, size, input)?
            }
            Message::SnapshotChunk { records } => {
                if peer_snapshot_done || plan_ready {
                    bail!("snapshot chunk received out of order");
                }
                if peer_records.len().saturating_add(records.len()) > sync::MAX_RECORDS_PER_SESSION
                {
                    bail!("peer snapshot exceeds session record limit");
                }
                metadata_bytes = metadata_bytes.saturating_add(serde_json::to_vec(&records)?.len());
                if metadata_bytes > sync::MAX_METADATA_BYTES_PER_SESSION {
                    bail!("peer snapshot exceeds session metadata limit");
                }
                peer_records.extend(records);
            }
            Message::SnapshotEnd if !peer_snapshot_done && !plan_ready => {
                state.validate_remote_records(share, advertised, &peer_records)?;
                peer_snapshot_done = true;
            }
            Message::ApplyChunk {
                records,
                conflicts,
                merges,
            } => {
                if !peer_snapshot_done || plan_ready {
                    bail!("apply chunk received out of order");
                }
                metadata_bytes = metadata_bytes.saturating_add(
                    serde_json::to_vec(&(
                        records.as_slice(),
                        conflicts.as_slice(),
                        merges.as_slice(),
                    ))?
                    .len(),
                );
                if metadata_bytes > sync::MAX_METADATA_BYTES_PER_SESSION {
                    bail!("apply plan exceeds session metadata limit");
                }
                if pending.records.len().saturating_add(records.len())
                    > sync::MAX_RECORDS_PER_SESSION
                    || pending.conflicts.len().saturating_add(conflicts.len())
                        > sync::MAX_RECORDS_PER_SESSION
                    || pending.merges.len().saturating_add(merges.len())
                        > sync::MAX_RECORDS_PER_SESSION
                {
                    bail!("apply plan exceeds session record limit");
                }
                pending.records.extend(records);
                pending.conflicts.extend(conflicts);
                pending.merges.extend(merges);
            }
            Message::ApplyEnd => {
                if !peer_snapshot_done || plan_ready {
                    bail!("apply end received out of order");
                }
                let expected = sync::plan(advertised, &peer_records);
                sync::validate_materialized_plan_shape(&pending, &expected, connector)?;
                plan_ready = true;
                let mut required_records = sync::plan_records_with_inputs(&expected);
                required_records.extend(pending.records.clone());
                let hashes = sync::required_hashes_for_share(state, share, &required_records)?;
                requested_inbound.clear();
                for hash in &hashes {
                    let size = required_records
                        .iter()
                        .find_map(|record| match &record.version.entry {
                            Entry::File {
                                hash: record_hash,
                                size,
                                ..
                            } if record_hash == hash => Some(*size),
                            _ => None,
                        })
                        .context("requested hash is missing from the validated plan")?;
                    requested_inbound.insert(hash.clone(), size);
                }
                sync::write_message(output, &Message::Need { hashes })?;
            }
            Message::Done if plan_ready => {
                if !requested_inbound.is_empty() {
                    bail!("peer ended object transfer before satisfying requested hashes")
                }
                let expected = sync::plan(advertised, &peer_records);
                sync::verify_materialized_plan(state, &pending, &expected)?;
                match sync::apply_complete_plan(state, share, &pending) {
                    Ok(()) => {
                        state.prune_unreferenced_objects()?;
                        let heads = sync::regular_file_heads(&state.records(share)?);
                        sync::write_heads(output, &heads)?;
                        sync::write_message(output, &Message::Applied)?;
                        applied_records = Some(state.records(share)?);
                        pending.records.clear();
                        pending.conflicts.clear();
                        pending.merges.clear();
                        plan_ready = false;
                    }
                    Err(error) => sync::write_message(
                        output,
                        &Message::Error {
                            message: format!("{error:#}"),
                        },
                    )?,
                }
            }
            Message::HeadChunk { records } if applied_records.is_some() => {
                acknowledged_heads.extend(records);
                if acknowledged_heads.len() > sync::MAX_RECORDS_PER_SESSION {
                    bail!("acknowledged-head manifest exceeds record limit");
                }
            }
            Message::CommitAck if applied_records.is_some() => {
                let current = applied_records.take().expect("checked above");
                sync::validate_ack_heads(&current, &acknowledged_heads)?;
                state.acknowledge_shared_heads(share, &acknowledged_heads)?;
                break;
            }
            Message::Cancel if !plan_ready => break,
            other => bail!("unexpected sync message: {other:?}"),
        }
    }
    Ok(())
}

fn status(state: &State, path: &Path, json: bool) -> Result<()> {
    let (share, root) = state.find_share(path)?;
    let peer = state.peer(&share)?;
    let records = state.records(&share)?;
    let entries = records
        .iter()
        .filter(|record| !matches!(record.version.entry, Entry::Tombstone))
        .count();
    let tombstones = records.len() - entries;
    let pending_install = state
        .install_intents()?
        .iter()
        .any(|(pending, _)| pending == &share);
    let unsettled = state.unsettled_paths(&share)?;
    if json {
        println!(
            "{}",
            serde_json::json!({"schema":3,"share":share.0,"root":root,"peer":peer,"entries":entries,"tombstones":tombstones,"initial_complete":state.initial_complete(&share)?,"view":"last_persisted_scan","pending_install":pending_install,"unsettled":unsettled})
        );
    } else {
        println!("Share: {}", share.0);
        println!("Root:  {}", root.display());
        println!(
            "Peer:  {}",
            peer.as_ref()
                .map(|p| p.host.as_str())
                .unwrap_or("not configured")
        );
        println!("Entries: {entries}");
        println!("Tombstones: {tombstones}");
        println!(
            "View:  last persisted scan (run `flocal sync {} --dry-run` to preview changes)",
            root.display()
        );
        if pending_install {
            println!("Warning: an interrupted install will be recovered by the next sync/watch");
        }
        if !unsettled.is_empty() {
            println!("Unsettled paths:");
            for path in unsettled {
                println!("  {}", path.display());
            }
        }
    }
    Ok(())
}

fn conflicts(state: &State, command: ConflictCommand) -> Result<()> {
    match command {
        ConflictCommand::List { path, json } => {
            let (share, _) = state.find_share(&path)?;
            let conflicts = state.conflicts(&share)?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(
                        &serde_json::json!({"schema": 2, "conflicts": conflicts})
                    )?
                );
            } else {
                for conflict in conflicts {
                    println!(
                        "{}  {}  {:?} winner={} loser={}",
                        conflict.id,
                        conflict.path.display(),
                        conflict.resolution,
                        conflict.winner.version.peer.0,
                        conflict.loser.version.peer.0
                    );
                }
            }
        }
        ConflictCommand::Show {
            path,
            conflict_id,
            json,
        } => {
            let (share, _) = state.find_share(&path)?;
            let conflict = state.conflict(&share, &conflict_id)?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(
                        &serde_json::json!({"schema": 2, "conflict": conflict})
                    )?
                );
            } else {
                println!("Conflict {}: {}", conflict.id, conflict.path.display());
                println!("Winner: {}", conflict.winner.version.peer.0);
                println!("Loser:  {}", conflict.loser.version.peer.0);
                println!("Resolution: {:?}", conflict.resolution);
                if let Some(base) = &conflict.base {
                    println!("Base: {}:{}", base.id.peer.0, base.id.sequence);
                }
                if let Some(merged) = &conflict.merged {
                    println!(
                        "Merged: {}:{}",
                        merged.version.peer.0, merged.version.sequence
                    );
                }
            }
        }
    }
    Ok(())
}

enum RestoreSelector {
    Version(RestoreVersion),
    Input(String),
    Base,
    Merged,
}

impl RestoreSelector {
    fn new(
        version: Option<RestoreVersion>,
        input: Option<String>,
        base: bool,
        merged: bool,
    ) -> Result<Self> {
        match (version, input, base, merged) {
            (Some(version), None, false, false) => Ok(Self::Version(version)),
            (None, Some(peer), false, false) => Ok(Self::Input(peer)),
            (None, None, true, false) => Ok(Self::Base),
            (None, None, false, true) => Ok(Self::Merged),
            _ => bail!("select exactly one of --version, --input, --base, or --merged"),
        }
    }
}

fn restore(
    state: &State,
    path: &Path,
    id: &str,
    selector: RestoreSelector,
    destination: &Path,
    force: bool,
) -> Result<()> {
    let (share, _) = state.find_share(path)?;
    let conflict = state.conflict(&share, id)?;
    let base_record;
    let record = match selector {
        RestoreSelector::Version(version) => {
            if matches!(version, RestoreVersion::Winner) {
                &conflict.winner
            } else {
                &conflict.loser
            }
        }
        RestoreSelector::Input(peer) => conflict
            .inputs
            .iter()
            .find(|record| record.version.peer.0 == peer)
            .context("conflict has no input owned by that peer")?,
        RestoreSelector::Base => {
            let base = conflict
                .base
                .as_ref()
                .context("conflict has no merge base")?;
            base_record = flocal::model::Record {
                path: conflict.path.clone(),
                version: flocal::model::Version {
                    peer: base.id.peer.clone(),
                    sequence: base.id.sequence,
                    id_authenticator: base.id.authenticator.clone(),
                    timestamp_ns: 0,
                    seen: Vec::new(),
                    merge_base: None,
                    version_authenticator: None,
                    base_authenticator: base.authenticator.clone(),
                    entry: base.entry.clone(),
                },
            };
            &base_record
        }
        RestoreSelector::Merged => conflict
            .merged
            .as_ref()
            .context("conflict has no merged result")?,
    };
    let hash = record_hash(record).context("selected conflict input is not a regular file")?;
    match std::fs::symlink_metadata(destination) {
        Ok(_) if !force => bail!("destination exists; use --force to replace it"),
        Ok(metadata) if !metadata.file_type().is_file() => {
            bail!("--force replaces regular files only")
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let parent = destination.parent().context("destination has no parent")?;
    std::fs::create_dir_all(parent)?;
    let temp = parent.join(format!(".flocal-restore-{}", ShareId::generate().0));
    let mut source = state.open_verified_object(&hash)?;
    let mut output = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp)?;
    std::io::copy(&mut source, &mut output)?;
    std::fs::File::open(&temp)?.sync_all()?;
    std::fs::rename(temp, destination)?;
    std::fs::File::open(parent)?.sync_all()?;
    println!("Restored {} to {}", id, destination.display());
    Ok(())
}

fn watch(state: &mut State, path: &Path) -> Result<()> {
    let (share, root) = state.find_share(path)?;
    if !state.initial_complete(&share)? {
        bail!("initial synchronization is incomplete; run `flocal sync PATH` and confirm it first");
    }
    let _share_lock = state.lock_share(&share)?;
    watch_log(&mut io::stdout(), &format!("Watching {}", root.display()))?;
    persistent_watch_loop(state, &share, &root, &mut io::stdout(), &mut io::stderr())
}

#[allow(clippy::too_many_arguments)]
fn persistent_watch_loop(
    state: &mut State,
    share: &ShareId,
    root_path: &Path,
    out: &mut impl Write,
    err: &mut impl Write,
) -> Result<()> {
    persistent_watch_loop_control(
        state,
        share,
        root_path,
        out,
        err,
        None,
        None,
        Arc::new(Mutex::new(None)),
    )
}

#[allow(clippy::too_many_arguments)]
fn persistent_watch_loop_control(
    state: &mut State,
    share: &ShareId,
    root_path: &Path,
    out: &mut impl Write,
    err: &mut impl Write,
    stop: Option<&AtomicBool>,
    worker_state: Option<&std::sync::atomic::AtomicU8>,
    child: Arc<Mutex<Option<Child>>>,
) -> Result<()> {
    let peer = state
        .peer(share)?
        .context("no peer configured; run `flocal peer add`")?;
    let config = flocal::watch::WatchConfig::default();
    let retry_policy = flocal::watch::RetryPolicy::default();
    let mut backoff = flocal::watch::RetryBackoff::default();
    let mut failures = WatchFailures::default();
    let mut connected = false;
    let mut connecting_logged = false;
    loop {
        if let Some(worker_state) = worker_state {
            worker_state.store(WORKER_RECONNECTING, Ordering::Relaxed);
        }
        if stop.is_some_and(|stop| stop.load(Ordering::Relaxed)) {
            return Ok(());
        }
        if !connecting_logged {
            watch_log(out, "Connecting to peer")?;
            connecting_logged = true;
        }
        let root = match sync::ShareRoot::open(state, share) {
            Ok(root) => root,
            Err(error) => {
                let delay =
                    root_open_retry_delay(error, &mut failures, &mut backoff, &retry_policy, err)?;
                sleep_until_stopped(delay, stop);
                continue;
            }
        };
        let (watch_state, events_rx, _watcher) = match create_local_watcher(root_path) {
            Ok(watcher) => watcher,
            Err(error) => {
                if let Some(event) = failures.failed(
                    &error,
                    std::time::Instant::now(),
                    WATCH_FAILURE_REPORT_INTERVAL,
                ) {
                    write_watch_event(err, event)?;
                }
                let delay = backoff.failed(&retry_policy, rand::random_range(-2_000..=2_000));
                sleep_until_stopped(delay, stop);
                continue;
            }
        };
        let outcome = persistent_watch_session(
            state,
            share,
            &root,
            &watch_state,
            &peer,
            &events_rx,
            out,
            err,
            &config,
            &mut backoff,
            &mut failures,
            &mut connected,
            stop,
            worker_state,
            child.clone(),
        );
        match outcome {
            Ok(()) if stop.is_some_and(|stop| stop.load(Ordering::Relaxed)) => return Ok(()),
            Ok(()) => bail!("persistent watch session ended unexpectedly"),
            Err(error) => {
                if std::mem::take(&mut connected) {
                    watch_log(err, "Peer connection lost; retrying")?;
                }
                let error = match watch_state.snapshot() {
                    flocal::watch::WatchSnapshot::Lost(lost) => {
                        anyhow::anyhow!("filesystem watcher stopped; recreating: {lost}")
                    }
                    flocal::watch::WatchSnapshot::Healthy { .. } => error,
                };
                if is_terminal_watch_error(&error) {
                    return Err(error);
                }
                if let Some(event) = failures.failed(
                    &error,
                    std::time::Instant::now(),
                    WATCH_FAILURE_REPORT_INTERVAL,
                ) {
                    write_watch_event(err, event)?;
                }
                let delay = backoff.failed(&retry_policy, rand::random_range(-2_000..=2_000));
                sleep_until_stopped(delay, stop);
            }
        }
    }
}

fn sleep_until_stopped(delay: Duration, stop: Option<&AtomicBool>) {
    let deadline = std::time::Instant::now() + delay;
    while std::time::Instant::now() < deadline {
        if stop.is_some_and(|stop| stop.load(Ordering::Relaxed)) {
            return;
        }
        std::thread::sleep(std::cmp::min(
            Duration::from_millis(100),
            deadline.saturating_duration_since(std::time::Instant::now()),
        ));
    }
}

fn root_open_retry_delay(
    error: anyhow::Error,
    failures: &mut WatchFailures,
    backoff: &mut flocal::watch::RetryBackoff,
    retry_policy: &flocal::watch::RetryPolicy,
    err: &mut impl Write,
) -> Result<Duration> {
    if !root_validation_retryable(&error) {
        return Err(error);
    }
    if let Some(event) = failures.failed(
        &error,
        std::time::Instant::now(),
        WATCH_FAILURE_REPORT_INTERVAL,
    ) {
        write_watch_event(err, event)?;
    }
    Ok(backoff.failed(retry_policy, rand::random_range(-2_000..=2_000)))
}

fn create_local_watcher(
    root: &Path,
) -> Result<(
    flocal::watch::WatchState,
    std::sync::mpsc::Receiver<PersistentEvent>,
    RecommendedWatcher,
)> {
    let watch_state = flocal::watch::WatchState::default();
    let (events_tx, events_rx) = std::sync::mpsc::sync_channel(1);
    let callback_state = watch_state.clone();
    let mut watcher =
        notify::recommended_watcher(move |event: notify::Result<notify::Event>| match event {
            Ok(event) if event.need_rescan() => callback_state.lost(
                "filesystem watcher reported an event gap",
                &events_tx,
                PersistentEvent::Wake,
            ),
            Ok(_) => callback_state.changed(&events_tx, PersistentEvent::Wake),
            Err(error) => callback_state.lost(error, &events_tx, PersistentEvent::Wake),
        })
        .context("cannot create filesystem watcher")?;
    watcher
        .watch(root, RecursiveMode::Recursive)
        .context("cannot watch share root")?;
    Ok((watch_state, events_rx, watcher))
}

fn is_terminal_watch_error(error: &anyhow::Error) -> bool {
    if let Some(remote) = error.downcast_ref::<RemoteWatchError>() {
        return !remote.retryable;
    }
    if error.downcast_ref::<WatchProtocolError>().is_some() {
        return true;
    }
    if error.downcast_ref::<sync::RootIdentityChanged>().is_some() {
        return true;
    }
    let message = format!("{error:#}");
    message.contains("root identity changed")
}

fn root_validation_retryable(error: &anyhow::Error) -> bool {
    error.downcast_ref::<sync::RootIdentityChanged>().is_none()
}

#[allow(clippy::too_many_arguments)]
fn persistent_watch_session(
    state: &mut State,
    share: &ShareId,
    root: &sync::ShareRoot,
    watch_state: &flocal::watch::WatchState,
    peer: &flocal::model::PeerConfig,
    events_rx: &std::sync::mpsc::Receiver<PersistentEvent>,
    out: &mut impl Write,
    err: &mut impl Write,
    config: &flocal::watch::WatchConfig,
    backoff: &mut flocal::watch::RetryBackoff,
    failures: &mut WatchFailures,
    connected: &mut bool,
    stop: Option<&AtomicBool>,
    worker_state: Option<&std::sync::atomic::AtomicU8>,
    child: Arc<Mutex<Option<Child>>>,
) -> Result<()> {
    while events_rx.try_recv().is_ok() {}
    if stop.is_some_and(|stop| stop.load(Ordering::Relaxed)) {
        return Ok(());
    }
    let remote = PersistentRemote::spawn(&peer.host, &peer.executable, child)?;
    if stop.is_some_and(|stop| stop.load(Ordering::Relaxed)) {
        return Ok(());
    }
    persistent_watch_session_io(
        state,
        share,
        root,
        watch_state,
        peer,
        events_rx,
        out,
        err,
        config,
        backoff,
        failures,
        connected,
        stop,
        worker_state,
        &remote.output,
        &remote.input,
    )
}

#[allow(clippy::too_many_arguments)]
fn persistent_watch_session_io(
    state: &mut State,
    share: &ShareId,
    root: &sync::ShareRoot,
    watch_state: &flocal::watch::WatchState,
    peer: &flocal::model::PeerConfig,
    events_rx: &std::sync::mpsc::Receiver<PersistentEvent>,
    out: &mut impl Write,
    err: &mut impl Write,
    config: &flocal::watch::WatchConfig,
    backoff: &mut flocal::watch::RetryBackoff,
    failures: &mut WatchFailures,
    connected: &mut bool,
    stop: Option<&AtomicBool>,
    worker_state: Option<&std::sync::atomic::AtomicU8>,
    remote_input: &impl AsFd,
    remote_output: &impl AsFd,
) -> Result<()> {
    sync::write_initial_message_until(
        remote_output,
        &InitialMessage::WatchOpen {
            protocol: sync::WATCH_PROTOCOL_VERSION,
            share: share.clone(),
            peer: state.peer_id()?,
        },
        std::time::Instant::now() + sync::default_frame_deadline(),
    )?;
    let accepted = sync::read_v2_envelope_until(
        remote_input,
        std::time::Instant::now() + sync::default_frame_deadline(),
    )?;
    match accepted {
        V2Envelope::Session {
            frame:
                V2SessionFrame::WatchAccepted {
                    protocol,
                    peer: actual,
                },
        } if protocol == sync::WATCH_PROTOCOL_VERSION && actual == peer.peer_id => {}
        V2Envelope::Session {
            frame: V2SessionFrame::Error { retryable, message },
        } => {
            let retryable =
                retryable || message.starts_with("unsupported persistent watch protocol version ");
            return Err(RemoteWatchError { retryable, message }.into());
        }
        other => {
            return Err(watch_protocol_error(format!(
                "remote does not support persistent watch protocol version {}: {other:?}; upgrade flocal on both peers",
                sync::WATCH_PROTOCOL_VERSION
            )));
        }
    }
    read_watch_ready(state, share, remote_input)?;
    write_watch_ready(remote_output, 0, &state.unsettled_paths(share)?)?;

    let mut remote_generation = 0u64;
    let mut round = 1u64;
    let mut completed_local = 0u64;
    let startup_local_generation = match watch_state.snapshot() {
        flocal::watch::WatchSnapshot::Healthy { generation } => generation,
        flocal::watch::WatchSnapshot::Lost(error) => bail!("filesystem watcher stopped: {error}"),
    };
    let report = connector_round_until_completed(
        state,
        share,
        root,
        &mut round,
        startup_local_generation,
        0,
        remote_input,
        remote_output,
        &mut remote_generation,
        out,
    )?;
    out.write_all(&report)?;
    watch_state
        .complete(startup_local_generation, &mut completed_local)
        .map_err(|error| anyhow::anyhow!("filesystem watcher stopped: {error}"))?;
    backoff.startup_round_succeeded();
    let recovery = failures.succeeded();
    if recovery.is_none() {
        watch_log(out, "Peer connected")?;
    }
    *connected = true;
    if let Some(worker_state) = worker_state {
        worker_state.store(WORKER_WATCHING, Ordering::Relaxed);
    }
    if let Some(event) = recovery {
        write_watch_event(err, event)?;
    }
    let now = std::time::Instant::now();
    let mut audit = flocal::watch::AuditSchedule::new(now);
    let mut heartbeat = flocal::watch::Heartbeat::new(now);
    let mut debounce = flocal::watch::Debounce::default();
    let mut scheduled_local_generation = completed_local;
    let mut scheduled_remote_generation = remote_generation;
    loop {
        if stop.is_some_and(|stop| stop.load(Ordering::Relaxed)) {
            return Ok(());
        }
        let now = std::time::Instant::now();
        let local_generation = match watch_state.snapshot() {
            flocal::watch::WatchSnapshot::Healthy { generation } => generation,
            flocal::watch::WatchSnapshot::Lost(error) => {
                bail!("filesystem watcher stopped: {error}")
            }
        };
        if local_generation > scheduled_local_generation {
            debounce.notify(now);
            scheduled_local_generation = local_generation;
        }
        if remote_generation > scheduled_remote_generation {
            debounce.notify(now);
            scheduled_remote_generation = remote_generation;
        }
        let round_due = debounce.take_due(now, config) || audit.is_due(now, config);
        if round_due {
            round = round.checked_add(1).context("watch round exhausted")?;
            let frozen_local = local_generation;
            let frozen_remote = remote_generation;
            let report = connector_round_until_completed(
                state,
                share,
                root,
                &mut round,
                frozen_local,
                frozen_remote,
                remote_input,
                remote_output,
                &mut remote_generation,
                out,
            )?;
            out.write_all(&report)?;
            watch_state
                .complete(frozen_local, &mut completed_local)
                .map_err(|error| anyhow::anyhow!("filesystem watcher stopped: {error}"))?;
            audit.completed_full_round(std::time::Instant::now());
            heartbeat.activity(std::time::Instant::now());
            continue;
        }
        if let Some(action) = heartbeat.due(now, config) {
            match action {
                flocal::watch::HeartbeatAction::Ping { nonce } => {
                    write_v2_session(remote_output, V2SessionFrame::Ping { nonce })?;
                }
                flocal::watch::HeartbeatAction::TimedOut => bail!("peer heartbeat timed out"),
            }
        }
        let deadlines = [
            debounce.deadline(config),
            audit.deadline(config),
            heartbeat.deadline(config),
        ];
        let deadline = deadlines
            .into_iter()
            .flatten()
            .min()
            .unwrap_or(now + config.heartbeat);
        let timeout = deadline.saturating_duration_since(std::time::Instant::now());
        let poll_deadline = std::cmp::min(
            std::time::Instant::now() + Duration::from_millis(50),
            std::time::Instant::now() + timeout,
        );
        if sync::input_ready_until(remote_input, poll_deadline)? {
            match sync::read_v2_envelope_until(
                remote_input,
                std::time::Instant::now() + sync::default_frame_deadline(),
            )? {
                V2Envelope::Session {
                    frame: V2SessionFrame::Changed { generation },
                } => remote_generation = remote_generation.max(generation),
                V2Envelope::Session {
                    frame: V2SessionFrame::Pong { nonce },
                } if heartbeat.pong(nonce, std::time::Instant::now()) => {}
                V2Envelope::Session {
                    frame: V2SessionFrame::Error { retryable, message },
                } => return Err(RemoteWatchError { retryable, message }.into()),
                other => {
                    return Err(watch_protocol_error(format!(
                        "unexpected persistent idle frame: {other:?}"
                    )));
                }
            }
        }
        while events_rx.try_recv().is_ok() {}
    }
}

enum ConnectorRound {
    Completed(Vec<u8>),
    Invalidated(flocal::model::RelativePath),
}

#[derive(Default)]
struct InvalidationCycle {
    retried: bool,
    first_path: Option<flocal::model::RelativePath>,
    deferred: std::collections::HashSet<Vec<u8>>,
}

enum InvalidationAction {
    Recalculate,
    Deferred { same_path: bool },
}

impl InvalidationCycle {
    fn observe(&mut self, path: flocal::model::RelativePath) -> InvalidationAction {
        if self.retried {
            self.deferred.insert(path.as_bytes().to_vec());
            InvalidationAction::Deferred {
                same_path: self.first_path.as_ref() == Some(&path),
            }
        } else {
            self.retried = true;
            self.first_path = Some(path);
            InvalidationAction::Recalculate
        }
    }

    fn reset(&mut self) {
        *self = Self::default();
    }
}

fn accepts_invalidation(plan: &EffectivePlan, path: &flocal::model::RelativePath) -> bool {
    plan.plan.records.iter().any(|record| record.path == *path)
        && !plan.retained_paths.contains(path.as_bytes())
}

fn report_invalidation(
    cycle: &mut InvalidationCycle,
    path: flocal::model::RelativePath,
    output: &mut impl Write,
) -> Result<()> {
    let display = path.display();
    let message = match cycle.observe(path) {
        InvalidationAction::Deferred { same_path: true } => {
            format!("UNSETTLED {display} is still changing; stable paths will continue")
        }
        InvalidationAction::Deferred { same_path: false } => {
            format!("UNSETTLED {display} changed during recalculation; stable paths will continue")
        }
        InvalidationAction::Recalculate => {
            format!("UNSETTLED {display} changed while synchronizing; recalculating now")
        }
    };
    watch_transition(output, &message)
}

fn report_settled(
    paths: impl IntoIterator<Item = flocal::model::RelativePath>,
    output: &mut impl Write,
) -> Result<()> {
    for path in paths {
        watch_transition(
            output,
            &format!(
                "SETTLED {} no longer blocks synchronization",
                path.display()
            ),
        )?;
    }
    Ok(())
}

#[derive(Clone)]
struct EffectivePlan {
    plan: flocal::reconcile::Plan,
    retained_paths: std::collections::HashSet<Vec<u8>>,
}

fn effective_plan(
    side: &[flocal::model::Record],
    other: &[flocal::model::Record],
    full: &flocal::reconcile::Plan,
    deferred: &std::collections::HashSet<Vec<u8>>,
) -> EffectivePlan {
    if deferred.is_empty() {
        return EffectivePlan {
            plan: full.clone(),
            retained_paths: std::collections::HashSet::new(),
        };
    }
    let side_by_path: std::collections::HashMap<_, _> = side
        .iter()
        .map(|record| (record.path.as_bytes(), record))
        .collect();
    let other_by_path: std::collections::HashMap<_, _> = other
        .iter()
        .map(|record| (record.path.as_bytes(), record))
        .collect();
    let target_by_path: std::collections::HashMap<_, _> = full
        .records
        .iter()
        .map(|record| (record.path.as_bytes(), record))
        .collect();
    let mut roots = std::collections::HashSet::new();
    for seed in deferred {
        let seed_path = bytes_path(seed);
        let mut root = seed_path.as_path();
        let mut ancestor = seed_path.parent();
        while let Some(path) = ancestor.filter(|path| !path.as_os_str().is_empty()) {
            let bytes = path_bytes(path);
            let is_directory = |record: Option<&&flocal::model::Record>| {
                record.is_some_and(|record| matches!(record.version.entry, Entry::Directory))
            };
            if !(is_directory(side_by_path.get(bytes.as_slice()))
                && is_directory(other_by_path.get(bytes.as_slice()))
                && is_directory(target_by_path.get(bytes.as_slice())))
            {
                root = path;
            }
            ancestor = path.parent();
        }
        roots.insert(root.to_path_buf());
    }
    let is_deferred = |path: &flocal::model::RelativePath| {
        let path = path.to_path_buf();
        path.ancestors().any(|ancestor| roots.contains(ancestor))
    };
    let mut records: Vec<_> = full
        .records
        .iter()
        .filter(|record| !is_deferred(&record.path))
        .cloned()
        .collect();
    let retained_paths: std::collections::HashSet<_> = side
        .iter()
        .filter(|record| is_deferred(&record.path))
        .map(|record| record.path.as_bytes().to_vec())
        .collect();
    records.extend(
        side.iter()
            .filter(|record| is_deferred(&record.path))
            .cloned(),
    );
    records.sort_by(|left, right| left.path.as_bytes().cmp(right.path.as_bytes()));
    let conflicts = full
        .conflicts
        .iter()
        .filter(|conflict| !is_deferred(&conflict.path))
        .cloned()
        .collect();
    EffectivePlan {
        plan: flocal::reconcile::Plan {
            records,
            conflicts,
            merges: full
                .merges
                .iter()
                .filter(|candidate| !is_deferred(&candidate.path))
                .cloned()
                .collect(),
        },
        retained_paths,
    }
}

#[allow(clippy::too_many_arguments)]
fn connector_round_until_completed(
    state: &mut State,
    share: &ShareId,
    root: &sync::ShareRoot,
    round: &mut u64,
    connector_generation: u64,
    responder_generation: u64,
    input: &impl AsFd,
    output: &impl AsFd,
    pending_remote_generation: &mut u64,
    watch_output: &mut impl Write,
) -> Result<Vec<u8>> {
    let mut invalidation_cycle = InvalidationCycle::default();
    loop {
        match connector_v2_round(
            state,
            share,
            root,
            *round,
            connector_generation,
            responder_generation,
            input,
            output,
            pending_remote_generation,
            &invalidation_cycle.deferred,
        )? {
            ConnectorRound::Completed(report) => return Ok(report),
            ConnectorRound::Invalidated(path) => {
                report_invalidation(&mut invalidation_cycle, path, watch_output)?;
                *round = round.checked_add(1).context("watch round exhausted")?;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn connector_v2_round(
    state: &mut State,
    share: &ShareId,
    root: &sync::ShareRoot,
    round: u64,
    connector_generation: u64,
    responder_generation: u64,
    input: &impl AsFd,
    output: &impl AsFd,
    pending_remote_generation: &mut u64,
    deferred: &std::collections::HashSet<Vec<u8>>,
) -> Result<ConnectorRound> {
    let _global_lock = state.lock_global_sync()?;
    state.clear_pending_objects(share)?;
    state.prune_unreferenced_objects()?;
    let mut budget =
        sync::RoundBudget::new(std::time::Instant::now() + Duration::from_secs(5 * 60));
    write_v2_round(
        output,
        round,
        V2RoundFrame::SyncStart {
            connector_generation,
            responder_generation,
        },
        &budget,
    )?;
    budget.check()?;
    let local = sync::refresh_with_root(state, share, root)?;
    budget.check()?;
    match recv_connector_round(input, round, &budget, pending_remote_generation, true)? {
        V2RoundFrame::SyncAccepted => {}
        other => watch_protocol_bail!("expected persistent sync acceptance, got {other:?}"),
    }
    let remote_records =
        read_connector_snapshot(input, round, &mut budget, pending_remote_generation)?;
    state
        .validate_remote_records(share, &local, &remote_records)
        .map_err(|error| watch_protocol_error(format!("{error:#}")))?;
    let mut plan = sync::plan(&local, &remote_records);
    let required = sync::plan_records_with_inputs(&plan);
    let needs = sync::required_hashes_for_share(state, share, &required)?;
    let mut expected_sizes = std::collections::HashMap::new();
    for record in &required {
        if let Entry::File { hash, size, .. } = &record.version.entry {
            expected_sizes.insert(hash.clone(), *size);
        }
    }
    write_v2_round(
        output,
        round,
        V2RoundFrame::Need {
            hashes: needs.clone(),
        },
        &budget,
    )?;
    for expected in needs {
        match recv_connector_round(input, round, &budget, pending_remote_generation, false)? {
            V2RoundFrame::ObjectStart { hash, size } if hash == expected => {
                if expected_sizes.get(&hash) != Some(&size) {
                    watch_protocol_bail!("peer object size differs from the validated plan");
                }
                budget
                    .add_transfer(size)
                    .map_err(|error| watch_protocol_error(format!("{error:#}")))?;
                receive_connector_object(
                    state,
                    share,
                    hash,
                    size,
                    round,
                    input,
                    &budget,
                    pending_remote_generation,
                )?;
            }
            other => watch_protocol_bail!("expected persistent object {expected}, got {other:?}"),
        }
    }
    match recv_connector_round(input, round, &budget, pending_remote_generation, false)? {
        V2RoundFrame::Done => {}
        other => watch_protocol_bail!("expected persistent object completion, got {other:?}"),
    }

    sync::materialize_merges(state, share, &mut plan)?;
    let applied_plan = effective_plan(&local, &remote_records, &plan, deferred);
    let responder_plan = effective_plan(&remote_records, &local, &plan, deferred);

    write_v2_snapshot(output, round, &local, &budget)?;
    write_v2_plan(output, round, &plan, &budget)?;
    let remote_needs =
        match recv_connector_round(input, round, &budget, pending_remote_generation, false)? {
            V2RoundFrame::Need { hashes } => hashes,
            other => {
                watch_protocol_bail!("expected remote persistent object request, got {other:?}")
            }
        };
    let unique: std::collections::HashSet<_> = remote_needs.iter().collect();
    if unique.len() != remote_needs.len() {
        watch_protocol_bail!("peer object request contains duplicate hashes");
    }
    let mut allowed = sync::authorized_hashes(&local);
    allowed.extend(sync::authorized_hashes(&plan.records));
    for hash in remote_needs {
        if !allowed.contains(&hash) {
            watch_protocol_bail!("peer requested an object outside this share");
        }
        send_v2_object(state, &hash, round, output, &mut budget)?;
    }
    write_v2_round(output, round, V2RoundFrame::Done, &budget)?;
    let mut remote_heads = Vec::new();
    loop {
        match recv_connector_round(input, round, &budget, pending_remote_generation, false)? {
            V2RoundFrame::HeadChunk { records } => {
                budget
                    .add_metadata(serde_json::to_vec(&records)?.len())
                    .map_err(|error| watch_protocol_error(format!("{error:#}")))?;
                remote_heads.extend(records);
                if remote_heads.len() > sync::MAX_RECORDS_PER_SESSION {
                    watch_protocol_bail!("peer acknowledged-head manifest exceeds record limit");
                }
            }
            V2RoundFrame::Applied => break,
            V2RoundFrame::RoundInvalidated { path }
                if accepts_invalidation(&responder_plan, &path) =>
            {
                state.remember_unsettled_path(share, &path)?;
                return Ok(ConnectorRound::Invalidated(path));
            }
            other => watch_protocol_bail!("expected persistent apply response, got {other:?}"),
        }
    }
    budget.check()?;
    if let Err(error) = sync::apply_complete_plan_with_root_skipping(
        state,
        share,
        root,
        &applied_plan.plan,
        &applied_plan.retained_paths,
    ) {
        if let Some(invalidated) = error.downcast_ref::<sync::ApplyInvalidated>() {
            write_v2_round(
                output,
                round,
                V2RoundFrame::RoundInvalidated {
                    path: invalidated.path.clone(),
                },
                &budget,
            )?;
            return Ok(ConnectorRound::Invalidated(invalidated.path.clone()));
        }
        return Err(error);
    }
    budget.check()?;
    state.set_initial_complete(share)?;
    budget.check()?;
    sync::validate_ack_heads(&remote_heads, &remote_heads)
        .map_err(|error| watch_protocol_error(format!("{error:#}")))?;
    let current = state.records(share)?;
    let shared_heads = sync::intersect_heads(&current, &remote_heads)
        .map_err(|error| watch_protocol_error(format!("{error:#}")))?;
    state.acknowledge_shared_heads(share, &shared_heads)?;
    budget.check()?;
    state.prune_unreferenced_objects()?;
    budget.check()?;
    let settled = if deferred.is_empty() {
        state.clear_unsettled_paths(share)?
    } else {
        Vec::new()
    };
    let responder_by_path: std::collections::HashMap<_, _> = responder_plan
        .plan
        .records
        .iter()
        .map(|record| (record.path.as_bytes(), record))
        .collect();
    let mut reported_plan = applied_plan.plan.clone();
    reported_plan.records.retain(|record| {
        responder_by_path
            .get(record.path.as_bytes())
            .is_some_and(|remote| {
                remote.version.id() == record.version.id()
                    && remote.version.entry == record.version.entry
            })
    });
    let mut report = Vec::new();
    write_plan_report(
        &mut report,
        &local,
        &remote_records,
        &reported_plan,
        false,
        PlanReport::Watch,
    )?;
    report_settled(settled, &mut report)?;
    write_v2_heads(output, round, &shared_heads, &budget)?;
    write_v2_round(output, round, V2RoundFrame::SyncFinished, &budget)?;
    Ok(ConnectorRound::Completed(report))
}

fn recv_connector_round(
    input: &impl AsFd,
    expected_round: u64,
    budget: &sync::RoundBudget,
    pending_remote_generation: &mut u64,
    allow_prior_session_frames: bool,
) -> Result<V2RoundFrame> {
    loop {
        match sync::read_v2_envelope_until(input, budget.frame_deadline()?)? {
            V2Envelope::Round {
                round,
                frame: V2RoundFrame::SyncFailed { retryable, message },
            } if round == expected_round => {
                return Err(RemoteWatchError { retryable, message }.into());
            }
            V2Envelope::Round { round, frame } if round == expected_round => return Ok(frame),
            V2Envelope::Session {
                frame: V2SessionFrame::Error { retryable, message },
            } => return Err(RemoteWatchError { retryable, message }.into()),
            V2Envelope::Session {
                frame: V2SessionFrame::Changed { generation },
            } if allow_prior_session_frames => {
                *pending_remote_generation = (*pending_remote_generation).max(generation);
            }
            V2Envelope::Session {
                frame: V2SessionFrame::Pong { .. },
            } if allow_prior_session_frames => {}
            other => {
                return Err(watch_protocol_error(format!(
                    "unexpected persistent round frame: {other:?}"
                )));
            }
        }
    }
}

fn read_connector_snapshot(
    input: &impl AsFd,
    round: u64,
    budget: &mut sync::RoundBudget,
    pending_remote_generation: &mut u64,
) -> Result<Vec<flocal::model::Record>> {
    let mut records = Vec::new();
    loop {
        match recv_connector_round(input, round, budget, pending_remote_generation, false)? {
            V2RoundFrame::SnapshotChunk { records: chunk } => {
                budget
                    .add_metadata(serde_json::to_vec(&chunk)?.len())
                    .map_err(|error| watch_protocol_error(format!("{error:#}")))?;
                records.extend(chunk);
                if records.len() > sync::MAX_RECORDS_PER_SESSION {
                    watch_protocol_bail!("snapshot exceeds session record limit");
                }
            }
            V2RoundFrame::SnapshotEnd => return Ok(records),
            other => watch_protocol_bail!("expected persistent snapshot, got {other:?}"),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn receive_connector_object(
    state: &State,
    share: &ShareId,
    hash: flocal::model::ObjectHash,
    size: u64,
    round: u64,
    input: &impl AsFd,
    budget: &sync::RoundBudget,
    pending_remote_generation: &mut u64,
) -> Result<()> {
    state.mark_object_receiving(share, &hash)?;
    let mut sink = state.begin_object(hash.clone(), size)?;
    loop {
        match recv_connector_round(input, round, budget, pending_remote_generation, false)? {
            V2RoundFrame::ObjectChunk { data } => sink.write_chunk(&data)?,
            V2RoundFrame::ObjectEnd => {
                sink.finish()?;
                return state.mark_object_verified(share, &hash);
            }
            other => watch_protocol_bail!("unexpected persistent object frame: {other:?}"),
        }
    }
}

#[cfg(feature = "e2e-test-hooks")]
const WATCH_FAILURE_REPORT_INTERVAL: Duration = Duration::from_millis(100);
#[cfg(not(feature = "e2e-test-hooks"))]
const WATCH_FAILURE_REPORT_INTERVAL: Duration = Duration::from_secs(5 * 60);
const MAX_DAEMON_MESSAGE_BYTES: usize = 64 * 1024;
const MAX_DAEMON_CLIENTS: usize = 16;

#[derive(Default)]
struct WatchFailures {
    count: u64,
    last_reported: Option<std::time::Instant>,
}

#[allow(dead_code)]
enum WatchEvent {
    First { error: String },
    Periodic { count: u64, error: String },
    Recovered { count: u64 },
}

#[allow(dead_code)]
impl WatchFailures {
    fn failed(
        &mut self,
        error: &anyhow::Error,
        now: std::time::Instant,
        interval: Duration,
    ) -> Option<WatchEvent> {
        self.count = self.count.saturating_add(1);
        if self.count == 1 {
            self.last_reported = Some(now);
            return Some(WatchEvent::First {
                error: watch_error(error),
            });
        }
        if self
            .last_reported
            .is_some_and(|last| now.duration_since(last) < interval)
        {
            return None;
        }
        self.last_reported = Some(now);
        Some(WatchEvent::Periodic {
            count: self.count,
            error: watch_error(error),
        })
    }

    fn succeeded(&mut self) -> Option<WatchEvent> {
        let count = std::mem::take(&mut self.count);
        self.last_reported = None;
        (count != 0).then_some(WatchEvent::Recovered { count })
    }
}

fn watch_error(error: &anyhow::Error) -> String {
    escaped(&format!("{error:#}"))
}

fn write_watch_event(destination: &mut impl Write, event: WatchEvent) -> Result<()> {
    let message = match event {
        WatchEvent::First { error } => {
            format!("synchronization failed; retrying in background: {error}")
        }
        WatchEvent::Periodic { count, error } => {
            format!(
                "synchronization still failing after {count} failed attempts; retrying: {error}"
            )
        }
        WatchEvent::Recovered { count } => {
            format!("synchronization resumed after {count} failed attempts")
        }
    };
    watch_log(destination, &message)
}

/// Writes one of watch's own status lines with a UTC timestamp prefix,
/// matching the per-line timestamps `write_plan_report` gives its
/// `PlanReport::Watch` output — every line a long-running `watch` prints is
/// timestamped, not just the synchronized-paths lines.
fn watch_log(destination: &mut impl Write, message: &str) -> Result<()> {
    writeln!(destination, "{} {message}", utc_timestamp())?;
    Ok(())
}

fn watch_transition(destination: &mut impl Write, message: &str) -> Result<()> {
    let bounded: String = message.chars().take(4096).collect();
    watch_log(destination, &bounded)
}

/// The padded action label for a path that matches on both peers. Named
/// here and reused below rather than repeated as a literal, so the KEEP
/// filter can never drift from the match arms that produce it.
const KEEP: &str = "KEEP  ";

fn print_plan(
    local: &[flocal::model::Record],
    remote: &[flocal::model::Record],
    plan: &flocal::reconcile::Plan,
    json: bool,
    report: PlanReport,
) -> Result<()> {
    write_plan_report(&mut io::stdout(), local, remote, plan, json, report)
}

fn write_plan_report(
    output: &mut impl Write,
    local: &[flocal::model::Record],
    remote: &[flocal::model::Record],
    plan: &flocal::reconcile::Plan,
    json: bool,
    report: PlanReport,
) -> Result<()> {
    if json {
        let recovery_ids: Vec<_> = plan
            .conflicts
            .iter()
            .map(|conflict| {
                serde_json::json!({
                    "path": conflict.path,
                    "recovery_id": if report == PlanReport::Preview {
                        None
                    } else {
                        Some(flocal::reconcile::conflict_id(conflict))
                    }
                })
            })
            .collect();
        writeln!(
            output,
            "{}",
            serde_json::json!({"schema": 2, "plan": plan, "recovery": recovery_ids})
        )?;
        return Ok(());
    }
    let local_by_path: std::collections::HashMap<_, _> =
        local.iter().map(|r| (r.path.as_bytes(), r)).collect();
    let remote_by_path: std::collections::HashMap<_, _> =
        remote.iter().map(|r| (r.path.as_bytes(), r)).collect();
    let conflicts_by_path: std::collections::HashMap<_, _> = plan
        .conflicts
        .iter()
        .map(|conflict| (conflict.path.as_bytes(), conflict))
        .collect();
    // `watch`'s repeating background sync timestamps every printed line and
    // omits KEEP: on an idle share almost every path matches on both peers,
    // and a KEEP line per path per rescan cycle would drown out the
    // changes a live log exists to show. `flocal sync`'s plan is unabridged.
    let prefix = match report {
        PlanReport::Full | PlanReport::Preview => String::new(),
        PlanReport::Watch => format!("{} ", utc_timestamp()),
    };
    for record in &plan.records {
        let local_record = local_by_path.get(record.path.as_bytes());
        let remote_record = remote_by_path.get(record.path.as_bytes());
        if let Some(conflict) = conflicts_by_path.get(record.path.as_bytes()) {
            let recovery = if report == PlanReport::Preview {
                "pending".to_owned()
            } else {
                flocal::reconcile::conflict_id(conflict)
            };
            match &conflict.resolution {
                flocal::reconcile::ConflictResolution::MergedWithOverlaps => writeln!(
                    output,
                    "{prefix}MERGE  {} (overlap; recovery {recovery})",
                    record.path.display()
                )?,
                flocal::reconcile::ConflictResolution::WholeFile { reason, .. } => writeln!(
                    output,
                    "{prefix}CONFLICT {} ({reason:?}; recovery {recovery})",
                    record.path.display()
                )?,
                flocal::reconcile::ConflictResolution::Destructive { .. } => writeln!(
                    output,
                    "{prefix}CONFLICT {} (destructive; recovery {recovery})",
                    record.path.display()
                )?,
            }
            continue;
        }
        let action = match (&record.version.entry, local_record, remote_record) {
            (_, Some(local), Some(remote))
                if local.version.id() == remote.version.id()
                    && local.version.entry == remote.version.entry =>
            {
                KEEP
            }
            (Entry::Tombstone, _, _) => "DELETE",
            (_, Some(_), Some(_)) => "MERGE ",
            (_, Some(_), None) => "UPLOAD",
            (_, None, Some(_)) => "DOWNLOAD",
            _ => KEEP,
        };
        if report == PlanReport::Watch && action == KEEP {
            continue;
        }
        writeln!(output, "{prefix}{action} {}", record.path.display())?;
    }
    Ok(())
}

fn confirm(prompt: &str) -> Result<bool> {
    print!("{prompt} [y/N] ");
    io::stdout().flush()?;
    let mut answer = String::new();
    io::stdin().read_line(&mut answer)?;
    Ok(matches!(
        answer.trim().to_ascii_lowercase().as_str(),
        "y" | "yes"
    ))
}

fn escaped(value: &str) -> String {
    value.escape_default().take(4096).collect()
}

/// A `YYYY-MM-DDTHH:MM:SSZ` timestamp for watch's log lines. Hand-rolled
/// over `std::time` rather than a calendar dependency: watch only ever
/// needs second-precision UTC (every container in this project's own
/// end-to-end harness already runs UTC), so there is no timezone database
/// to get right, only the well-known civil-time conversion below.
fn utc_timestamp() -> String {
    format_utc_timestamp(std::time::SystemTime::now())
}

fn format_utc_timestamp(instant: std::time::SystemTime) -> String {
    let elapsed = instant
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let total_seconds = elapsed.as_secs();
    let days_since_epoch = (total_seconds / 86_400) as i64;
    let seconds_of_day = total_seconds % 86_400;
    let (year, month, day) = civil_from_days(days_since_epoch);
    let (hour, minute, second) = (
        seconds_of_day / 3600,
        (seconds_of_day / 60) % 60,
        seconds_of_day % 60,
    );
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

/// Days since the Unix epoch to a proleptic-Gregorian (year, month, day).
/// Howard Hinnant's constant-time civil-time algorithm (public domain);
/// see <https://howardhinnant.github.io/date_algorithms.html>. Verified
/// against independently computed reference instants in
/// `utc_timestamp_matches_known_instants` below.
fn civil_from_days(days_since_epoch: i64) -> (i64, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = (z - era * 146_097) as u64;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era as i64 + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * month_index + 2) / 5 + 1) as u32;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    } as u32;
    let year = if month <= 2 { year + 1 } else { year };
    (year, month, day)
}

fn record_hash(record: &flocal::model::Record) -> Option<flocal::model::ObjectHash> {
    match &record.version.entry {
        Entry::File { hash, .. } => Some(hash.clone()),
        _ => None,
    }
}

struct Remote {
    child: Child,
    input: BufWriter<ChildStdin>,
    output: BufReader<TimedReader<ChildStdout>>,
    stderr: std::thread::JoinHandle<Vec<u8>>,
}

enum PersistentEvent {
    Wake,
}

struct PersistentRemote {
    child: Arc<Mutex<Option<Child>>>,
    input: ChildStdin,
    output: ChildStdout,
    stderr: Option<std::thread::JoinHandle<Vec<u8>>>,
}

impl PersistentRemote {
    fn spawn(host: &str, executable: &str, child_slot: Arc<Mutex<Option<Child>>>) -> Result<Self> {
        validate_host(host)?;
        validate_executable(executable)?;
        let command = format!("{executable} protocol serve");
        let mut child = Command::new("ssh")
            .args([
                "-T",
                "-o",
                "ConnectTimeout=10",
                "-o",
                "ServerAliveInterval=15",
                "-o",
                "ServerAliveCountMax=2",
                host,
                &command,
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let input = child.stdin.take().context("ssh stdin unavailable")?;
        let output = child.stdout.take().context("ssh stdout unavailable")?;
        let mut child_stderr = child.stderr.take().context("ssh stderr unavailable")?;
        let stderr = std::thread::spawn(move || {
            let mut kept = Vec::new();
            let mut buffer = [0u8; 8192];
            loop {
                match child_stderr.read(&mut buffer) {
                    Ok(0) | Err(_) => break,
                    Ok(count) if kept.len() < 64 * 1024 => {
                        let remaining = 64 * 1024 - kept.len();
                        kept.extend_from_slice(&buffer[..count.min(remaining)]);
                    }
                    Ok(_) => {}
                }
            }
            kept
        });
        *child_slot
            .lock()
            .map_err(|_| anyhow::anyhow!("persistent remote child state is poisoned"))? =
            Some(child);
        Ok(Self {
            child: child_slot,
            input,
            output,
            stderr: Some(stderr),
        })
    }
}

impl Drop for PersistentRemote {
    fn drop(&mut self) {
        if let Ok(mut child) = self.child.lock()
            && let Some(mut child) = child.take()
        {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(stderr) = self.stderr.take() {
            let _ = stderr.join();
        }
    }
}

impl Remote {
    fn spawn(host: &str, executable: &str) -> Result<Self> {
        validate_host(host)?;
        validate_executable(executable)?;
        let command = format!("{executable} protocol serve");
        let mut child = Command::new("ssh")
            .args([
                "-T",
                "-o",
                "ConnectTimeout=10",
                "-o",
                "ServerAliveInterval=15",
                "-o",
                "ServerAliveCountMax=2",
                host,
                &command,
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let input = BufWriter::new(child.stdin.take().context("ssh stdin unavailable")?);
        let output = BufReader::new(TimedReader::new(
            child.stdout.take().context("ssh stdout unavailable")?,
        ));
        let mut child_stderr = child.stderr.take().context("ssh stderr unavailable")?;
        let stderr = std::thread::spawn(move || {
            let mut kept = Vec::new();
            let mut buffer = [0u8; 8192];
            loop {
                match child_stderr.read(&mut buffer) {
                    Ok(0) | Err(_) => break,
                    Ok(count) if kept.len() < 64 * 1024 => {
                        let remaining = 64 * 1024 - kept.len();
                        kept.extend_from_slice(&buffer[..count.min(remaining)]);
                    }
                    Ok(_) => {}
                }
            }
            kept
        });
        Ok(Self {
            child,
            input,
            output,
            stderr,
        })
    }
    fn finish(mut self) -> Result<()> {
        drop(self.input);
        let status = self.child.wait()?;
        let stderr = self.stderr.join().unwrap_or_default();
        if !status.success() {
            bail!(
                "ssh exited with {status}: {}",
                escaped(&String::from_utf8_lossy(&stderr))
            );
        }
        Ok(())
    }

    fn finish_after_error(mut self, error: anyhow::Error) -> anyhow::Error {
        drop(self.input);
        let status = self.child.wait();
        let stderr = self.stderr.join().unwrap_or_default();
        anyhow::anyhow!(
            "{error:#}; remote exited with {:?}: {}",
            status.ok(),
            escaped(&String::from_utf8_lossy(&stderr))
        )
    }
}

struct TimedReader<R> {
    inner: R,
    started: std::time::Instant,
}

impl<R> TimedReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            started: std::time::Instant::now(),
        }
    }
}

impl<R: Read + AsFd> Read for TimedReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        use rustix::event::{PollFd, PollFlags, Timespec, poll};
        let total = Duration::from_secs(
            std::env::var("FLOCAL_MAX_SESSION_SECONDS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(60 * 60),
        );
        let Some(remaining) = total.checked_sub(self.started.elapsed()) else {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "peer protocol session exceeded its configured duration",
            ));
        };
        let wait = remaining.min(Duration::from_secs(30));
        let mut descriptors = [PollFd::new(&self.inner, PollFlags::IN)];
        let timeout = Timespec {
            tv_sec: wait.as_secs() as i64,
            tv_nsec: wait.subsec_nanos() as i64,
        };
        if poll(&mut descriptors, Some(&timeout))? == 0 {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "peer protocol read timed out",
            ));
        }
        self.inner.read(buffer)
    }
}

fn discover_executable(host: &str) -> Result<String> {
    validate_host(host)?;
    const DISCOVER_COMMAND: &str = r#"command -v flocal || { test -x "$HOME/.local/bin/flocal" && printf '%s\n' "$HOME/.local/bin/flocal"; }"#;
    let output = Command::new("ssh")
        .args([
            "-T",
            "-o",
            "ConnectTimeout=10",
            "-o",
            "ServerAliveInterval=15",
            "-o",
            "ServerAliveCountMax=2",
            host,
            DISCOVER_COMMAND,
        ])
        .output()?;
    if !output.status.success() {
        bail!("flocal is not available on remote PATH");
    }
    let executable = String::from_utf8(output.stdout)?.trim().to_owned();
    validate_executable(&executable)?;
    Ok(executable)
}

fn validate_host(host: &str) -> Result<()> {
    if host.is_empty() || host.starts_with('-') || host.contains(['\n', '\r', '\0']) {
        bail!("invalid SSH host");
    }
    Ok(())
}

fn validate_executable(path: &str) -> Result<()> {
    if !path.starts_with('/')
        || !path
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "/._+-".contains(c))
    {
        bail!("remote flocal path contains unsafe characters");
    }
    Ok(())
}

#[cfg(unix)]
fn path_bytes(path: &Path) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    path.as_os_str().as_bytes().to_vec()
}

#[cfg(unix)]
fn bytes_path(bytes: &[u8]) -> PathBuf {
    use std::os::unix::ffi::OsStrExt;
    std::ffi::OsStr::from_bytes(bytes).into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn test_record(path: &[u8], peer: &str, entry: Entry) -> flocal::model::Record {
        flocal::model::Record {
            path: flocal::model::RelativePath::from_bytes(path.to_vec()).unwrap(),
            version: flocal::model::Version {
                peer: flocal::model::PeerId(peer.into()),
                sequence: 1,
                id_authenticator: None,
                timestamp_ns: 1,
                seen: Vec::new(),
                merge_base: None,
                version_authenticator: None,
                base_authenticator: None,
                entry,
            },
        }
    }

    #[test]
    fn deferred_plan_keeps_each_sides_path_and_commits_stable_siblings() {
        let local_dir = test_record(b"dir", "local", Entry::Directory);
        let remote_dir = test_record(b"dir", "remote", Entry::Directory);
        let local_noisy = test_record(b"dir/noisy", "local", Entry::Directory);
        let remote_noisy = test_record(b"dir/noisy", "remote", Entry::Directory);
        let remote_stable = test_record(b"dir/stable", "remote", Entry::Directory);
        let full = flocal::reconcile::Plan {
            records: vec![
                remote_dir.clone(),
                remote_noisy.clone(),
                remote_stable.clone(),
            ],
            conflicts: Vec::new(),
            merges: Vec::new(),
        };
        let deferred = std::collections::HashSet::from([b"dir/noisy".to_vec()]);

        let local = effective_plan(
            &[local_dir.clone(), local_noisy.clone()],
            &[
                remote_dir.clone(),
                remote_noisy.clone(),
                remote_stable.clone(),
            ],
            &full,
            &deferred,
        );
        assert!(local.plan.records.contains(&local_noisy));
        assert!(local.plan.records.contains(&remote_stable));
        assert!(!local.plan.records.contains(&remote_noisy));
        assert!(local.retained_paths.contains(local_noisy.path.as_bytes()));

        let remote = effective_plan(
            &[remote_dir, remote_noisy.clone(), remote_stable],
            &[local_dir, local_noisy.clone()],
            &full,
            &deferred,
        );
        assert!(remote.plan.records.contains(&remote_noisy));
        assert!(!remote.plan.records.contains(&local_noisy));
        assert!(remote.retained_paths.contains(remote_noisy.path.as_bytes()));
    }

    #[test]
    fn deferred_plan_widens_across_an_ancestor_type_transition() {
        let local_parent = test_record(b"node", "local", Entry::Directory);
        let local_child = test_record(b"node/child", "local", Entry::Directory);
        let target_parent = test_record(
            b"node",
            "remote",
            Entry::Symlink {
                target: b"elsewhere".to_vec(),
            },
        );
        let full = flocal::reconcile::Plan {
            records: vec![target_parent.clone()],
            conflicts: Vec::new(),
            merges: Vec::new(),
        };
        let deferred = std::collections::HashSet::from([b"node/child".to_vec()]);
        let effective = effective_plan(
            &[local_parent.clone(), local_child.clone()],
            std::slice::from_ref(&target_parent),
            &full,
            &deferred,
        );

        assert_eq!(effective.plan.records, vec![local_parent, local_child]);
    }

    #[test]
    fn invalidation_cycle_retries_once_then_defers_and_resets() -> Result<()> {
        let first = flocal::model::RelativePath::from_bytes(b"first".to_vec())?;
        let second = flocal::model::RelativePath::from_bytes(b"second".to_vec())?;
        let mut cycle = InvalidationCycle::default();

        assert!(matches!(
            cycle.observe(first.clone()),
            InvalidationAction::Recalculate
        ));
        assert!(matches!(
            cycle.observe(first.clone()),
            InvalidationAction::Deferred { same_path: true }
        ));
        assert!(matches!(
            cycle.observe(second.clone()),
            InvalidationAction::Deferred { same_path: false }
        ));
        assert!(cycle.deferred.contains(first.as_bytes()));
        assert!(cycle.deferred.contains(second.as_bytes()));

        cycle.reset();
        assert!(matches!(
            cycle.observe(second),
            InvalidationAction::Recalculate
        ));
        assert!(cycle.deferred.is_empty());
        Ok(())
    }

    #[test]
    fn invalidation_reporting_and_validation_cover_every_transition() -> Result<()> {
        let first = flocal::model::RelativePath::from_bytes(b"first".to_vec())?;
        let second = flocal::model::RelativePath::from_bytes(b"second".to_vec())?;
        let record = test_record(b"first", "peer", Entry::Directory);
        let active = EffectivePlan {
            plan: flocal::reconcile::Plan {
                records: vec![record.clone()],
                conflicts: Vec::new(),
                merges: Vec::new(),
            },
            retained_paths: std::collections::HashSet::new(),
        };
        assert!(accepts_invalidation(&active, &first));
        assert!(!accepts_invalidation(&active, &second));
        let retained = EffectivePlan {
            retained_paths: std::collections::HashSet::from([b"first".to_vec()]),
            ..active
        };
        assert!(!accepts_invalidation(&retained, &first));

        let mut cycle = InvalidationCycle::default();
        let mut output = Vec::new();
        report_invalidation(&mut cycle, first.clone(), &mut output)?;
        report_invalidation(&mut cycle, first.clone(), &mut output)?;
        report_invalidation(&mut cycle, second.clone(), &mut output)?;
        report_settled([first, second], &mut output)?;
        let output = String::from_utf8(output)?;
        for expected in [
            "changed while synchronizing; recalculating now",
            "is still changing; stable paths will continue",
            "changed during recalculation; stable paths will continue",
            "SETTLED first no longer blocks synchronization",
            "SETTLED second no longer blocks synchronization",
        ] {
            assert!(output.contains(expected), "{output:?}");
        }
        Ok(())
    }

    #[test]
    fn readiness_errors_do_not_commit_partial_unsettled_paths() -> Result<()> {
        use std::os::unix::net::UnixStream;

        let temp = tempdir()?;
        let root = temp.path().join("root");
        std::fs::create_dir(&root)?;
        let mut state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        let partial = flocal::model::RelativePath::from_bytes(b"partial".to_vec())?;

        let (writer, reader) = UnixStream::pair()?;
        drop(reader);
        let error = write_watch_ready(&writer, 0, std::slice::from_ref(&partial))
            .expect_err("readiness writes must surface a disconnected peer");
        assert!(error.to_string().contains("Broken pipe"));

        let (writer, reader) = UnixStream::pair()?;
        write_v2_session(
            &writer,
            V2SessionFrame::UnsettledChunk {
                paths: vec![partial.clone()],
            },
        )?;
        write_v2_session(&writer, V2SessionFrame::Ping { nonce: 1 })?;
        let error = read_watch_ready(&mut state, &share, &reader)
            .expect_err("a non-readiness frame must fail the handshake");
        assert!(error.downcast_ref::<WatchProtocolError>().is_some());
        assert!(!state.unsettled_paths(&share)?.contains(&partial));

        let (writer, reader) = UnixStream::pair()?;
        write_v2_session(
            &writer,
            V2SessionFrame::Error {
                retryable: false,
                message: "peer rejected readiness".into(),
            },
        )?;
        let error = read_watch_ready(&mut state, &share, &reader)
            .expect_err("a remote error must fail the handshake");
        let remote = error
            .downcast_ref::<RemoteWatchError>()
            .expect("the remote error remains typed");
        assert!(!remote.retryable);
        assert_eq!(remote.message, "peer rejected readiness");
        Ok(())
    }

    #[test]
    fn transition_log_is_timestamped_single_line_and_bounded() -> Result<()> {
        let mut output = Vec::new();
        watch_transition(&mut output, &format!("UNSETTLED {}", "x".repeat(5000)))?;
        let output = String::from_utf8(output)?;
        assert_eq!(output.lines().count(), 1);
        assert_eq!(output.trim_end().chars().count(), 4096 + 21);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    const SYSTEMD_INSTALL_TEST_ROOT: &str = "FLOCAL_TEST_SYSTEMD_INSTALL_ROOT";

    #[cfg(unix)]
    const PERMISSIVE_UMASK_SERVICE_ROOT: &str = "FLOCAL_TEST_PERMISSIVE_UMASK_SERVICE_ROOT";

    #[cfg(unix)]
    #[test]
    fn private_service_directory_creation_ignores_a_permissive_umask() -> Result<()> {
        if let Some(root) = std::env::var_os(PERMISSIVE_UMASK_SERVICE_ROOT) {
            let root = PathBuf::from(root);
            let directory = root.join("config/file.local/systemd/user");
            let old_umask = rustix::process::umask(rustix::fs::Mode::empty());
            let result = (|| {
                ensure_private_service_directory(&directory)?;
                for relative in [
                    "config",
                    "config/file.local",
                    "config/file.local/systemd",
                    "config/file.local/systemd/user",
                ] {
                    use std::os::unix::fs::PermissionsExt;
                    assert_eq!(
                        std::fs::metadata(root.join(relative))?.permissions().mode() & 0o077,
                        0
                    );
                }
                Ok(())
            })();
            rustix::process::umask(old_umask);
            return result;
        }

        let temporary = tempdir()?;
        let output = Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "tests::private_service_directory_creation_ignores_a_permissive_umask",
            ])
            .env(PERMISSIVE_UMASK_SERVICE_ROOT, temporary.path())
            .output()?;
        assert!(output.status.success(), "{:?}", output);
        Ok(())
    }

    fn serve_messages(messages: &[Message]) -> Result<(Result<()>, Vec<u8>)> {
        let temp = tempdir()?;
        let root = temp.path().join("root");
        std::fs::create_dir_all(&root)?;
        let mut state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        let mut input = Vec::new();
        for message in messages {
            sync::write_message(&mut input, message)?;
        }
        let mut output = Vec::new();
        let result = serve_sync(
            &mut state,
            &share,
            &flocal::model::PeerId("connector".into()),
            &[],
            &mut input.as_slice(),
            &mut output,
        );
        Ok((result, output))
    }

    fn initial_message(message: Message) -> Result<(Result<()>, Vec<u8>)> {
        let temp = tempdir()?;
        let mut state = State::open(temp.path().join("state"))?;
        let mut input = Vec::new();
        sync::write_message(&mut input, &message)?;
        let mut output = Vec::new();
        let result = serve_io(&mut state, &mut input.as_slice(), &mut output);
        Ok((result, output))
    }

    #[test]
    fn serve_sync_accepts_empty_exchange_and_cancel() -> Result<()> {
        let (result, output) = serve_messages(&[
            Message::Need { hashes: Vec::new() },
            Message::SnapshotEnd,
            Message::ApplyEnd,
            Message::Done,
            Message::CommitAck,
        ])?;
        result?;
        let mut messages = output.as_slice();
        assert!(matches!(sync::read_message(&mut messages)?, Message::Done));
        assert!(
            matches!(sync::read_message(&mut messages)?, Message::Need { hashes } if hashes.is_empty())
        );
        assert!(matches!(
            sync::read_message(&mut messages)?,
            Message::Applied
        ));
        serve_messages(&[Message::Cancel])?.0?;
        Ok(())
    }

    #[test]
    fn serve_sync_rejects_out_of_order_and_untrusted_messages() -> Result<()> {
        let hash = flocal::model::ObjectHash::from_blake3(blake3::hash(b"x"));
        let cases = vec![
            vec![Message::Need {
                hashes: vec![hash.clone(), hash.clone()],
            }],
            vec![Message::Need {
                hashes: vec![hash.clone()],
            }],
            vec![Message::ObjectStart {
                hash: hash.clone(),
                size: 1,
            }],
            vec![Message::SnapshotEnd, Message::SnapshotEnd],
            vec![Message::ApplyChunk {
                records: Vec::new(),
                conflicts: Vec::new(),
                merges: Vec::new(),
            }],
            vec![Message::ApplyEnd],
            vec![Message::Accepted {
                protocol: sync::PROTOCOL_VERSION,
                peer: flocal::model::PeerId("unexpected".into()),
            }],
        ];
        for messages in cases {
            assert!(serve_messages(&messages)?.0.is_err());
        }
        Ok(())
    }

    #[test]
    fn formatting_and_validation_helpers_cover_all_entry_kinds() -> Result<()> {
        assert!(validate_host("host").is_ok());
        assert!(validate_host("").is_err());
        assert!(validate_host("-option").is_err());
        assert!(validate_host("bad\nhost").is_err());
        assert!(validate_executable("/usr/local/bin/flocal").is_ok());
        assert!(validate_executable("flocal").is_err());
        assert!(validate_executable("/bad path").is_err());
        assert_eq!(escaped("a\nb"), "a\\nb");

        let record = |name: &[u8], entry: Entry| flocal::model::Record {
            path: flocal::model::RelativePath::from_bytes(name.to_vec()).unwrap(),
            version: flocal::model::Version {
                peer: flocal::model::PeerId("peer".into()),
                sequence: 1,
                id_authenticator: None,
                timestamp_ns: 1,
                seen: Vec::new(),
                merge_base: None,
                version_authenticator: None,
                base_authenticator: None,
                entry,
            },
        };
        let directory = record(b"directory", Entry::Directory);
        let tombstone = record(b"deleted", Entry::Tombstone);
        let symlink = record(
            b"link",
            Entry::Symlink {
                target: b"target".to_vec(),
            },
        );
        assert!(record_hash(&directory).is_none());
        print_plan(
            &[directory.clone(), symlink.clone()],
            std::slice::from_ref(&directory),
            &flocal::reconcile::Plan {
                records: vec![directory.clone(), tombstone, symlink],
                conflicts: Vec::new(),
                merges: Vec::new(),
            },
            false,
            PlanReport::Full,
        )?;
        print_plan(
            &[],
            &[],
            &flocal::reconcile::Plan {
                records: Vec::new(),
                conflicts: Vec::new(),
                merges: Vec::new(),
            },
            true,
            PlanReport::Full,
        )?;
        let mut merge_local = record(b"merge", Entry::Directory);
        merge_local.version.sequence = 2;
        let merge_remote = record(b"merge", Entry::Directory);
        let download = record(b"download", Entry::Directory);
        let neither = record(b"neither", Entry::Directory);
        print_plan(
            std::slice::from_ref(&merge_local),
            &[merge_remote.clone(), download.clone()],
            &flocal::reconcile::Plan {
                records: vec![merge_local.clone(), download, neither],
                conflicts: vec![flocal::reconcile::Conflict::whole_file(
                    merge_local.clone(),
                    merge_remote.clone(),
                    flocal::merge::FallbackReason::AbsentBase,
                )],
                merges: Vec::new(),
            },
            false,
            PlanReport::Full,
        )?;
        let conflict = flocal::reconcile::Conflict::whole_file(
            merge_local.clone(),
            merge_remote,
            flocal::merge::FallbackReason::AbsentBase,
        );
        let preview_plan = flocal::reconcile::Plan {
            records: vec![merge_local],
            conflicts: vec![conflict],
            merges: Vec::new(),
        };
        let mut preview = Vec::new();
        write_plan_report(
            &mut preview,
            &[],
            &[],
            &preview_plan,
            false,
            PlanReport::Preview,
        )?;
        let preview = String::from_utf8(preview)?;
        assert!(preview.contains("recovery pending"), "{preview}");
        let mut preview_json = Vec::new();
        write_plan_report(
            &mut preview_json,
            &[],
            &[],
            &preview_plan,
            true,
            PlanReport::Preview,
        )?;
        let preview_json: serde_json::Value = serde_json::from_slice(&preview_json)?;
        assert!(preview_json["recovery"][0]["recovery_id"].is_null());
        Ok(())
    }

    #[test]
    fn sync_rejects_mixed_legacy_and_managed_syntax() {
        assert!(Cli::try_parse_from(["flocal", "sync", "/tmp/root", "--dry-run"]).is_ok());
        assert!(Cli::try_parse_from(["flocal", "sync", "list"]).is_ok());
        for arguments in [
            ["flocal", "sync", "--dry-run", "list"],
            ["flocal", "sync", "--json", "start"],
        ] {
            let cli = Cli::try_parse_from(arguments).expect("the ambiguous form parses first");
            let Commands::Sync(arguments) = cli.command else {
                unreachable!()
            };
            assert!(validate_sync_arguments(&arguments).is_err());
        }
        let cli = Cli::try_parse_from([
            "flocal",
            "sync",
            "--dry-run",
            "add",
            "/tmp/root",
            "--host",
            "host",
            "--remote-path",
            "/tmp/remote",
        ])
        .expect("the mixed add form parses first");
        let Commands::Sync(arguments) = cli.command else {
            unreachable!()
        };
        assert!(validate_sync_arguments(&arguments).is_err());
    }

    #[test]
    fn utc_timestamp_matches_known_instants() {
        let format = |epoch_seconds: u64| {
            format_utc_timestamp(
                std::time::UNIX_EPOCH + std::time::Duration::from_secs(epoch_seconds),
            )
        };
        // Reference epoch seconds independently computed with
        // `date -u -d '<instant>' +%s`, not derived from the code under test.
        assert_eq!(format(0), "1970-01-01T00:00:00Z");
        assert_eq!(format(946_684_800), "2000-01-01T00:00:00Z");
        assert_eq!(format(2_147_483_647), "2038-01-19T03:14:07Z");
        assert_eq!(format(1_709_210_096), "2024-02-29T12:34:56Z"); // leap day
        assert_eq!(format(1_784_715_330), "2026-07-22T10:15:30Z");
    }

    #[test]
    fn watch_failures_rate_limit_changing_errors_and_report_recovery() {
        let start = std::time::Instant::now();
        let interval = Duration::from_secs(300);
        let mut failures = WatchFailures::default();

        assert!(matches!(
            failures.failed(&anyhow::anyhow!("first\nline"), start, interval),
            Some(WatchEvent::First { error }) if error == "first\\nline"
        ));
        assert!(
            failures
                .failed(
                    &anyhow::anyhow!("different peer-controlled text"),
                    start + Duration::from_secs(299),
                    interval,
                )
                .is_none()
        );
        assert!(matches!(
            failures.failed(
                &anyhow::anyhow!("latest"),
                start + interval,
                interval,
            ),
            Some(WatchEvent::Periodic { count: 3, error }) if error == "latest"
        ));
        assert!(matches!(
            failures.succeeded(),
            Some(WatchEvent::Recovered { count: 3 })
        ));
        assert!(failures.succeeded().is_none());
    }

    #[test]
    fn watch_errors_are_single_line_and_bounded() {
        let error = anyhow::anyhow!("{}\n{}", "é".repeat(5000), "x".repeat(5000));
        let rendered = watch_error(&error);
        assert_eq!(rendered.chars().count(), 4096);
        assert!(!rendered.contains('\n') && !rendered.contains('\r'));
        assert!(
            rendered.is_ascii(),
            "non-ASCII is escaped before truncation"
        );
    }

    #[test]
    fn watch_retry_policy_is_typed_and_transport_failures_retry() {
        let transport = anyhow::anyhow!(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "protocol version text from a broken transport",
        ));
        assert!(!is_terminal_watch_error(&transport));
        assert!(root_validation_retryable(&transport));
        let protocol_error = watch_protocol_error("unexpected round frame");
        assert!(
            protocol_error.to_string() == "unexpected round frame"
                && is_terminal_watch_error(&protocol_error)
                && is_terminal_watch_error(
                    &sync::RootIdentityChanged::new("root identity changed").into()
                )
                && !root_validation_retryable(
                    &sync::RootIdentityChanged::new("root identity changed").into()
                )
        );
        assert!(is_terminal_watch_error(
            &RemoteWatchError {
                retryable: false,
                message: "identity mismatch".into(),
            }
            .into()
        ));
        assert!(!is_terminal_watch_error(
            &RemoteWatchError {
                retryable: true,
                message: "lock busy".into(),
            }
            .into()
        ));

        let mut failures = WatchFailures::default();
        let mut backoff = flocal::watch::RetryBackoff::default();
        let mut output = Vec::new();
        assert!(
            root_open_retry_delay(
                anyhow::anyhow!("database busy"),
                &mut failures,
                &mut backoff,
                &flocal::watch::RetryPolicy::default(),
                &mut output,
            )
            .is_ok()
                && String::from_utf8_lossy(&output).contains("retrying in background")
        );
        assert!(
            root_open_retry_delay(
                sync::RootIdentityChanged::new("root identity changed").into(),
                &mut failures,
                &mut backoff,
                &flocal::watch::RetryPolicy::default(),
                &mut output,
            )
            .is_err()
        );
    }

    #[test]
    fn persistent_responder_rejects_setup_failures_with_typed_frames() -> Result<()> {
        use std::os::unix::net::UnixStream;

        let temp = tempdir()?;
        let root = temp.path().join("watch-root");
        std::fs::create_dir(&root)?;
        let mut state = State::open(temp.path().join("watch-state"))?;
        let share = state.init_share(&root)?;
        let bound_peer = flocal::model::PeerId("bound-peer".into());
        state.register_share_bound(&share, &root, &bound_peer)?;
        state.set_peer(
            &share,
            &flocal::model::PeerConfig {
                host: "host".into(),
                executable: "/flocal".into(),
                remote_path: path_bytes(&root),
                peer_id: bound_peer.clone(),
            },
        )?;

        let reject = |state: &mut State, protocol, peer| -> Result<V2SessionFrame> {
            let (server_input, _client_output) = UnixStream::pair()?;
            let (server_output, client_input) = UnixStream::pair()?;
            serve_watch_open(
                state,
                protocol,
                share.clone(),
                peer,
                &server_input,
                &server_output,
            )?;
            match sync::read_v2_envelope_until(
                &client_input,
                std::time::Instant::now() + sync::default_frame_deadline(),
            )? {
                V2Envelope::Session { frame } => Ok(frame),
                other => anyhow::bail!("expected session rejection, got {other:?}"),
            }
        };

        assert!(matches!(
            reject(
                &mut state,
                sync::WATCH_PROTOCOL_VERSION + 1,
                bound_peer.clone()
            )?,
            V2SessionFrame::Error {
                retryable: true,
                ..
            }
        ));
        assert!(matches!(
            reject(
                &mut state,
                sync::WATCH_PROTOCOL_VERSION,
                flocal::model::PeerId("wrong-peer".into())
            )?,
            V2SessionFrame::Error {
                retryable: false,
                ..
            }
        ));

        let mut competing = State::open(temp.path().join("watch-state"))?;
        let share_lock = state.lock_share(&share)?;
        assert!(matches!(
            reject(
                &mut competing,
                sync::WATCH_PROTOCOL_VERSION,
                bound_peer.clone()
            )?,
            V2SessionFrame::Error {
                retryable: true,
                ..
            }
        ));
        drop(share_lock);

        let displaced = temp.path().join("watch-root-old");
        std::fs::rename(&root, displaced)?;
        std::fs::create_dir(&root)?;
        let replaced_root = reject(&mut state, sync::WATCH_PROTOCOL_VERSION, bound_peer)?;
        assert!(
            matches!(
                replaced_root,
                V2SessionFrame::Error {
                    retryable: false,
                    ..
                }
            ),
            "{replaced_root:?}"
        );
        Ok(())
    }

    #[test]
    fn connector_round_dispatches_session_and_terminal_frames() -> Result<()> {
        use std::os::unix::net::UnixStream;

        let receive = |envelope: V2Envelope,
                       allow_session|
         -> Result<(Result<V2RoundFrame>, u64)> {
            let (writer, reader) = UnixStream::pair()?;
            sync::write_v2_envelope_until(
                &writer,
                &envelope,
                std::time::Instant::now() + sync::default_frame_deadline(),
            )?;
            drop(writer);
            let budget = sync::RoundBudget::new(std::time::Instant::now() + Duration::from_secs(1));
            let mut generation = 0;
            let result = recv_connector_round(&reader, 7, &budget, &mut generation, allow_session);
            Ok((result, generation))
        };

        let (result, generation) = receive(
            V2Envelope::Session {
                frame: V2SessionFrame::Changed { generation: 9 },
            },
            true,
        )?;
        assert!(result.is_err(), "EOF follows the consumed Changed frame");
        assert_eq!(generation, 9);
        assert!(
            receive(
                V2Envelope::Session {
                    frame: V2SessionFrame::Pong { nonce: 1 },
                },
                true,
            )?
            .0
            .is_err()
        );
        for envelope in [
            V2Envelope::Round {
                round: 7,
                frame: V2RoundFrame::SyncFailed {
                    retryable: false,
                    message: "bad round".into(),
                },
            },
            V2Envelope::Session {
                frame: V2SessionFrame::Error {
                    retryable: false,
                    message: "bad session".into(),
                },
            },
            V2Envelope::Round {
                round: 8,
                frame: V2RoundFrame::Done,
            },
        ] {
            assert!(receive(envelope, false)?.0.is_err());
        }
        assert!(matches!(
            receive(
                V2Envelope::Round {
                    round: 7,
                    frame: V2RoundFrame::Done,
                },
                false,
            )?
            .0?,
            V2RoundFrame::Done
        ));
        Ok(())
    }

    #[test]
    fn persistent_plan_writer_chunks_conflicts() -> Result<()> {
        use std::os::unix::net::UnixStream;

        let version = flocal::model::Version {
            peer: flocal::model::PeerId("peer".into()),
            sequence: 1,
            id_authenticator: None,
            timestamp_ns: 1,
            seen: Vec::new(),
            merge_base: None,
            version_authenticator: None,
            base_authenticator: None,
            entry: Entry::Directory,
        };
        let winner = flocal::model::Record {
            path: flocal::model::RelativePath::from_bytes(b"conflict".to_vec())?,
            version,
        };
        let mut loser = winner.clone();
        loser.version.peer = flocal::model::PeerId("other".into());
        let plan = flocal::reconcile::Plan {
            records: Vec::new(),
            conflicts: vec![flocal::reconcile::Conflict::whole_file(
                winner,
                loser,
                flocal::merge::FallbackReason::AbsentBase,
            )],
            merges: Vec::new(),
        };
        let (writer, reader) = UnixStream::pair()?;
        let budget = sync::RoundBudget::new(std::time::Instant::now() + Duration::from_secs(1));
        write_v2_plan(&writer, 3, &plan, &budget)?;
        assert!(matches!(
            sync::read_v2_envelope_until(&reader, budget.frame_deadline()?)?,
            V2Envelope::Round {
                round: 3,
                frame: V2RoundFrame::ApplyChunk { conflicts, .. }
            } if conflicts.len() == 1
        ));
        assert!(matches!(
            sync::read_v2_envelope_until(&reader, budget.frame_deadline()?)?,
            V2Envelope::Round {
                round: 3,
                frame: V2RoundFrame::ApplyEnd
            }
        ));
        Ok(())
    }

    #[test]
    fn persistent_responder_handles_idle_frames_and_rejects_wrong_phase() -> Result<()> {
        use std::os::unix::net::UnixStream;

        let temp = tempdir()?;
        let root = temp.path().join("idle-root");
        std::fs::create_dir(&root)?;
        let mut state = State::open(temp.path().join("idle-state"))?;
        let share = state.init_share(&root)?;
        let peer = flocal::model::PeerId("idle-peer".into());
        state.register_share_bound(&share, &root, &peer)?;
        state.set_peer(
            &share,
            &flocal::model::PeerConfig {
                host: "host".into(),
                executable: "/flocal".into(),
                remote_path: path_bytes(&root),
                peer_id: peer.clone(),
            },
        )?;
        let (client_output, server_input) = UnixStream::pair()?;
        let (server_output, client_input) = UnixStream::pair()?;
        let responder = std::thread::spawn(move || {
            serve_watch_open(
                &mut state,
                sync::WATCH_PROTOCOL_VERSION,
                share,
                peer,
                &server_input,
                &server_output,
            )
        });
        let read = || {
            sync::read_v2_envelope_until(
                &client_input,
                std::time::Instant::now() + sync::default_frame_deadline(),
            )
        };
        assert!(matches!(
            read()?,
            V2Envelope::Session {
                frame: V2SessionFrame::WatchAccepted { .. }
            }
        ));
        write_v2_session(&client_output, V2SessionFrame::Ready { generation: 0 })?;
        assert!(matches!(
            read()?,
            V2Envelope::Session {
                frame: V2SessionFrame::Ready { .. }
            }
        ));
        write_v2_session(&client_output, V2SessionFrame::Ping { nonce: 42 })?;
        assert!(matches!(
            read()?,
            V2Envelope::Session {
                frame: V2SessionFrame::Pong { nonce: 42 }
            }
        ));
        let budget = sync::RoundBudget::new(std::time::Instant::now() + Duration::from_secs(1));
        write_v2_round(&client_output, 1, V2RoundFrame::Done, &budget)?;
        assert!(matches!(
            read()?,
            V2Envelope::Session {
                frame: V2SessionFrame::Error {
                    retryable: false,
                    ..
                }
            }
        ));
        responder.join().expect("responder joins")?;
        Ok(())
    }

    #[test]
    fn periodic_and_recovery_watch_events_have_operator_context() -> Result<()> {
        let mut output = Vec::new();
        write_watch_event(
            &mut output,
            WatchEvent::Periodic {
                count: 7,
                error: "still unreachable".into(),
            },
        )
        .expect("periodic event is writable");
        write_watch_event(&mut output, WatchEvent::Recovered { count: 7 })?;
        let output = String::from_utf8(output)?;
        let lines: Vec<_> = output.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].ends_with(
            "synchronization still failing after 7 failed attempts; retrying: still unreachable"
        ));
        assert!(lines[1].ends_with("synchronization resumed after 7 failed attempts"));
        Ok(())
    }

    #[test]
    fn persistent_v2_round_converges_both_filesystem_trees_in_process() -> Result<()> {
        use std::os::unix::net::UnixStream;

        let temp = tempdir()?;
        let local_root = temp.path().join("local");
        let remote_root = temp.path().join("remote");
        std::fs::create_dir_all(&local_root)?;
        std::fs::create_dir_all(&remote_root)?;
        std::fs::write(local_root.join("from-local"), b"local")?;
        std::fs::write(remote_root.join("from-remote"), b"remote")?;
        let mut local_state = State::open(temp.path().join("local-state"))?;
        let mut remote_state = State::open(temp.path().join("remote-state"))?;
        let local_share = local_state.init_share(&local_root)?;
        let remote_share = remote_state.init_share(&remote_root)?;
        let local_cap = sync::ShareRoot::open(&local_state, &local_share)?;
        let remote_cap = sync::ShareRoot::open(&remote_state, &remote_share)?;
        let connector_peer = local_state.peer_id()?;

        let (connector_stream, responder_reader) = UnixStream::pair()?;
        let (responder_stream, connector_reader) = UnixStream::pair()?;
        let responder = std::thread::spawn(move || -> Result<()> {
            let first = sync::read_v2_envelope_until(
                &responder_reader,
                std::time::Instant::now() + sync::default_frame_deadline(),
            )?;
            assert!(matches!(
                first,
                V2Envelope::Round {
                    round: 1,
                    frame: V2RoundFrame::SyncStart { .. }
                }
            ));
            serve_v2_round(
                &mut remote_state,
                &remote_share,
                &connector_peer,
                &remote_cap,
                1,
                &responder_reader,
                &responder_stream,
                &Default::default(),
            )?;
            Ok(())
        });
        let mut pending_remote = 0;
        let report = connector_v2_round(
            &mut local_state,
            &local_share,
            &local_cap,
            1,
            0,
            0,
            &connector_reader,
            &connector_stream,
            &mut pending_remote,
            &Default::default(),
        )?;
        responder.join().expect("responder joins")?;

        assert_eq!(std::fs::read(local_root.join("from-remote"))?, b"remote");
        assert_eq!(std::fs::read(remote_root.join("from-local"))?, b"local");
        let ConnectorRound::Completed(report) = report else {
            bail!("test round unexpectedly invalidated");
        };
        let report = String::from_utf8(report)?;
        assert!(report.contains("UPLOAD from-local"), "{report}");
        assert!(report.contains("DOWNLOAD from-remote"), "{report}");
        Ok(())
    }

    #[test]
    fn persistent_connector_rejects_duplicate_snapshot_paths_without_retrying() -> Result<()> {
        use std::os::unix::net::UnixStream;

        let temp = tempdir()?;
        let root = temp.path().join("local");
        std::fs::create_dir(&root)?;
        let mut state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        let root = sync::ShareRoot::open(&state, &share)?;
        let mut first = test_record(b"duplicate", "foreign", Entry::Directory);
        let mut second = first.clone();
        first.version.sequence = 8;
        second.version.sequence = 9;

        let (connector_stream, responder_reader) = UnixStream::pair()?;
        let (responder_stream, connector_reader) = UnixStream::pair()?;
        let responder = std::thread::spawn(move || -> Result<()> {
            let budget = sync::RoundBudget::new(std::time::Instant::now() + Duration::from_secs(1));
            assert!(matches!(
                sync::read_v2_envelope_until(&responder_reader, budget.frame_deadline()?)?,
                V2Envelope::Round {
                    round: 1,
                    frame: V2RoundFrame::SyncStart { .. }
                }
            ));
            write_v2_round(&responder_stream, 1, V2RoundFrame::SyncAccepted, &budget)?;
            write_v2_snapshot(&responder_stream, 1, &[first, second], &budget)
        });

        let mut pending_remote_generation = 0;
        let result = connector_v2_round(
            &mut state,
            &share,
            &root,
            1,
            0,
            0,
            &connector_reader,
            &connector_stream,
            &mut pending_remote_generation,
            &Default::default(),
        );
        let error = match result {
            Ok(_) => bail!("duplicate peer paths must terminate the persistent session"),
            Err(error) => error,
        };
        responder.join().expect("responder joins")?;
        assert_eq!(error.to_string(), "peer snapshot contains duplicate paths");
        assert!(is_terminal_watch_error(&error));
        Ok(())
    }

    #[test]
    fn persistent_session_handshake_startup_and_disconnect_run_in_process() -> Result<()> {
        use std::os::unix::net::UnixStream;

        let temp = tempdir()?;
        let local_root = temp.path().join("session-local");
        let remote_root = temp.path().join("session-remote");
        std::fs::create_dir_all(&local_root)?;
        std::fs::create_dir_all(&remote_root)?;
        std::fs::write(local_root.join("local"), b"local")?;
        std::fs::write(remote_root.join("remote"), b"remote")?;
        let mut local_state = State::open(temp.path().join("session-local-state"))?;
        let mut remote_state = State::open(temp.path().join("session-remote-state"))?;
        let local_share = local_state.init_share(&local_root)?;
        let remote_share = remote_state.init_share(&remote_root)?;
        let local_cap = sync::ShareRoot::open(&local_state, &local_share)?;
        let remote_cap = sync::ShareRoot::open(&remote_state, &remote_share)?;
        let remote_peer = remote_state.peer_id()?;
        let connector_peer = local_state.peer_id()?;
        let expected_remote_peer = remote_peer.clone();
        let local_seed = flocal::model::RelativePath::from_bytes(b"local-seed".to_vec())?;
        let remote_seed = flocal::model::RelativePath::from_bytes(b"remote-seed".to_vec())?;
        local_state.remember_unsettled_path(&local_share, &local_seed)?;
        let expected_local_seed = local_seed.clone();
        let expected_remote_seed = remote_seed.clone();

        let (connector_output, responder_input) = UnixStream::pair()?;
        let (responder_output, connector_input) = UnixStream::pair()?;
        let responder = std::thread::spawn(move || -> Result<()> {
            assert!(matches!(
                sync::read_initial_message_until(
                    &responder_input,
                    std::time::Instant::now() + sync::default_frame_deadline(),
                )?,
                InitialMessage::WatchOpen { .. }
            ));
            write_v2_session(
                &responder_output,
                V2SessionFrame::WatchAccepted {
                    protocol: sync::WATCH_PROTOCOL_VERSION,
                    peer: remote_peer,
                },
            )?;
            write_watch_ready(&responder_output, 0, std::slice::from_ref(&remote_seed))?;
            read_watch_ready(&mut remote_state, &remote_share, &responder_input)?;
            let unsettled = remote_state.unsettled_paths(&remote_share)?;
            assert!(unsettled.contains(&expected_local_seed));
            assert!(unsettled.contains(&expected_remote_seed));
            let start = sync::read_v2_envelope_until(
                &responder_input,
                std::time::Instant::now() + sync::default_frame_deadline(),
            )?;
            assert!(matches!(
                start,
                V2Envelope::Round {
                    round: 1,
                    frame: V2RoundFrame::SyncStart { .. }
                }
            ));
            serve_v2_round(
                &mut remote_state,
                &remote_share,
                &connector_peer,
                &remote_cap,
                1,
                &responder_input,
                &responder_output,
                &Default::default(),
            )?;
            Ok(())
        });

        let peer = flocal::model::PeerConfig {
            host: "in-process".into(),
            executable: "/flocal".into(),
            remote_path: path_bytes(&remote_root),
            peer_id: expected_remote_peer,
        };
        let watch_state = flocal::watch::WatchState::default();
        let (_events_tx, events_rx) = std::sync::mpsc::sync_channel(1);
        let mut output = Vec::new();
        let mut errors = Vec::new();
        let mut backoff = flocal::watch::RetryBackoff::default();
        let mut failures = WatchFailures::default();
        let mut connected = false;
        let result = persistent_watch_session_io(
            &mut local_state,
            &local_share,
            &local_cap,
            &watch_state,
            &peer,
            &events_rx,
            &mut output,
            &mut errors,
            &flocal::watch::WatchConfig::default(),
            &mut backoff,
            &mut failures,
            &mut connected,
            None,
            None,
            &connector_input,
            &connector_output,
        );
        assert!(result.is_err(), "responder EOF ends this one session");
        assert!(connected, "startup round reached connected state");
        responder.join().expect("responder joins")?;
        assert_eq!(std::fs::read(local_root.join("remote"))?, b"remote");
        assert_eq!(std::fs::read(remote_root.join("local"))?, b"local");
        assert!(local_state.unsettled_paths(&local_share)?.is_empty());
        assert!(String::from_utf8(output)?.contains("Peer connected"));
        Ok(())
    }

    #[test]
    fn watch_report_omits_keep_and_timestamps_every_line_full_report_does_not() -> Result<()> {
        let record = |name: &[u8], entry: Entry| flocal::model::Record {
            path: flocal::model::RelativePath::from_bytes(name.to_vec()).unwrap(),
            version: flocal::model::Version {
                peer: flocal::model::PeerId("peer".into()),
                sequence: 1,
                id_authenticator: None,
                timestamp_ns: 1,
                seen: Vec::new(),
                merge_base: None,
                version_authenticator: None,
                base_authenticator: None,
                entry,
            },
        };
        // kept matches on both peers (KEEP); uploaded exists locally only.
        let kept = record(b"kept", Entry::Directory);
        let uploaded = record(b"uploaded", Entry::Directory);
        let plan = flocal::reconcile::Plan {
            records: vec![kept.clone(), uploaded.clone()],
            conflicts: Vec::new(),
            merges: Vec::new(),
        };
        let local = [kept.clone(), uploaded.clone()];
        let remote = [kept.clone()];

        let mut full = Vec::new();
        write_plan_report(&mut full, &local, &remote, &plan, false, PlanReport::Full)?;
        let full = String::from_utf8(full)?;
        assert!(full.contains("KEEP   kept"), "{full:?}");
        assert!(full.contains("UPLOAD uploaded"), "{full:?}");
        // The explicit `flocal sync` report is unchanged: no timestamp.
        assert!(!full.lines().next().unwrap().starts_with(char::is_numeric));

        // format_utc_timestamp is independently verified above; here it only
        // needs to bracket the call so the timestamp actually printed can be
        // checked against known-correct bounds. The fixed-width
        // YYYY-MM-DDTHH:MM:SSZ format sorts lexicographically in
        // chronological order, so a plain string range check is exact
        // regardless of how many seconds elapse between the two bounds —
        // unlike an equality check against just the two endpoints, which
        // would spuriously fail if the printed timestamp legitimately lands
        // on a third, in-between second under a slow or loaded scheduler.
        let before = utc_timestamp();
        let mut watch = Vec::new();
        write_plan_report(&mut watch, &local, &remote, &plan, false, PlanReport::Watch)?;
        let after = utc_timestamp();
        let watch = String::from_utf8(watch)?;
        assert!(!watch.contains("KEEP"), "{watch:?}");
        let lines: Vec<&str> = watch.lines().collect();
        assert_eq!(lines.len(), 1, "{watch:?}");
        for (line, suffix) in lines.iter().zip(["UPLOAD uploaded"]) {
            let (timestamp, rest) = line.split_once(' ').expect("timestamped line");
            assert_eq!(rest, suffix, "{watch:?}");
            assert!(
                (before.as_str()..=after.as_str()).contains(&timestamp),
                "{timestamp} not in [{before}, {after}]"
            );
        }
        Ok(())
    }

    #[test]
    fn initial_protocol_registration_and_rejections() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path().join("root");
        let share = ShareId("share-protocol".into());
        let peer = flocal::model::PeerId("peer-protocol".into());
        let (result, output) = initial_message(Message::Register {
            protocol: sync::PROTOCOL_VERSION,
            share: share.clone(),
            peer: peer.clone(),
            root: path_bytes(&root),
        })?;
        result?;
        assert!(matches!(
            sync::read_message(&mut output.as_slice())?,
            Message::Accepted { .. }
        ));

        for message in [
            Message::Register {
                protocol: sync::PROTOCOL_VERSION + 1,
                share: share.clone(),
                peer: peer.clone(),
                root: path_bytes(&root),
            },
            Message::Sync {
                protocol: sync::PROTOCOL_VERSION + 1,
                share: share.clone(),
                peer: peer.clone(),
                dry_run: true,
            },
        ] {
            let (result, output) = initial_message(message)?;
            result?;
            assert!(matches!(
                sync::read_message(&mut output.as_slice())?,
                Message::Error { .. }
            ));
        }
        let (result, output) = initial_message(Message::Sync {
            protocol: sync::PROTOCOL_VERSION,
            share,
            peer,
            dry_run: true,
        })?;
        result?;
        assert!(matches!(
            sync::read_message(&mut output.as_slice())?,
            Message::Error { message } if message == "peer identity mismatch"
        ));
        assert!(initial_message(Message::Done)?.0.is_err());
        Ok(())
    }

    #[test]
    fn initial_protocol_runs_bound_dry_sync() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path().join("root");
        std::fs::create_dir_all(&root)?;
        let mut state = State::open(temp.path().join("state"))?;
        let share = ShareId("share-bound".into());
        let peer = flocal::model::PeerId("peer-bound".into());
        state.register_share_bound(&share, &root, &peer)?;
        let mut input = Vec::new();
        sync::write_message(
            &mut input,
            &Message::Sync {
                protocol: sync::PROTOCOL_VERSION,
                share,
                peer,
                dry_run: true,
            },
        )?;
        sync::write_message(&mut input, &Message::Cancel)?;
        let mut output = Vec::new();
        serve_io(&mut state, &mut input.as_slice(), &mut output)?;
        let mut messages = output.as_slice();
        assert!(matches!(
            sync::read_message(&mut messages)?,
            Message::Accepted { .. }
        ));
        assert!(matches!(
            sync::read_message(&mut messages)?,
            Message::SnapshotEnd
        ));
        Ok(())
    }

    #[test]
    fn registration_reports_lock_and_binding_failures() -> Result<()> {
        fn register(
            state: &mut State,
            share: &ShareId,
            peer: &flocal::model::PeerId,
            root: &Path,
        ) -> Result<Message> {
            let mut input = Vec::new();
            sync::write_message(
                &mut input,
                &Message::Register {
                    protocol: sync::PROTOCOL_VERSION,
                    share: share.clone(),
                    peer: peer.clone(),
                    root: path_bytes(root),
                },
            )?;
            let mut output = Vec::new();
            serve_io(state, &mut input.as_slice(), &mut output)?;
            sync::read_message(&mut output.as_slice())
        }

        let temp = tempdir()?;
        let first = temp.path().join("first");
        let second = temp.path().join("second");
        let mut state = State::open(temp.path().join("state"))?;
        let share = ShareId("share-register".into());
        let peer = flocal::model::PeerId("peer-register".into());
        assert!(matches!(
            register(&mut state, &share, &peer, &first)?,
            Message::Accepted { .. }
        ));
        assert!(matches!(
            register(&mut state, &share, &peer, &second)?,
            Message::Error { .. }
        ));
        assert!(matches!(
            register(
                &mut state,
                &share,
                &flocal::model::PeerId("different".into()),
                &first
            )?,
            Message::Error { .. }
        ));

        let global = state.lock_global_sync()?;
        assert!(matches!(
            register(&mut state, &ShareId("global-lock".into()), &peer, &first)?,
            Message::Error { .. }
        ));
        drop(global);
        let share_lock = state.lock_share(&ShareId("share-lock".into()))?;
        assert!(matches!(
            register(&mut state, &ShareId("share-lock".into()), &peer, &first)?,
            Message::Error { .. }
        ));
        drop(share_lock);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn systemd_unit_uses_absolute_escaped_paths_and_the_managed_entrypoint() {
        let unit = systemd_unit_content(
            Path::new("/opt/with space/flocal"),
            Path::new("/var/lib/flocal state"),
        )
        .unwrap();
        assert!(unit.contains("ExecStart=\"/opt/with space/flocal\" daemon run"));
        assert!(unit.contains("Environment=FLOCAL_STATE_DIR=\"/var/lib/flocal state\""));
        assert!(unit.contains("WantedBy=default.target"));
        assert!(
            systemd_unit_content(Path::new("/tmp/evil%name"), Path::new("/tmp/state")).is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn daemon_refuses_to_replace_an_unexpected_socket_file() -> Result<()> {
        let temp = tempdir()?;
        let state = State::open(temp.path().join("state"))?;
        std::fs::create_dir_all(state.dir.join("run"))?;
        std::fs::write(daemon_socket(&state), "not a socket")?;
        let error = daemon_run(state).expect_err("ordinary file must not be removed as a socket");
        assert!(error.to_string().contains("unexpected daemon socket path"));
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn daemon_refuses_a_symlinked_run_directory() -> Result<()> {
        let temp = tempdir()?;
        let state = State::open(temp.path().join("state"))?;
        let target = temp.path().join("target");
        std::fs::create_dir(&target)?;
        std::os::unix::fs::symlink(&target, state.dir.join("run"))?;
        daemon_run(state).expect_err("symlinked run directory must be refused");
        assert!(!target.join("daemon.sock").exists());
        Ok(())
    }

    #[test]
    fn sync_add_rejects_invalid_paths_before_daemon_activation() -> Result<()> {
        let temp = tempdir()?;
        let mut state = State::open(temp.path().join("state"))?;
        let missing = temp.path().join("missing");
        let error = sync_command(
            &mut state,
            SyncCommand::Add {
                path: missing,
                host: "peer".into(),
                remote_path: PathBuf::from("/remote"),
                yes: true,
            },
        )
        .expect_err("missing sync root must fail before daemon activation");
        assert!(error.to_string().contains("cannot inspect sync root"));
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn sync_add_rejects_a_symlink_before_daemon_activation() -> Result<()> {
        let temp = tempdir()?;
        let mut state = State::open(temp.path().join("state"))?;
        let target = temp.path().join("target");
        let link = temp.path().join("link");
        std::fs::create_dir(&target)?;
        std::os::unix::fs::symlink(target, &link)?;
        let error = sync_command(
            &mut state,
            SyncCommand::Add {
                path: link,
                host: "peer".into(),
                remote_path: PathBuf::from("/remote"),
                yes: true,
            },
        )
        .expect_err("symlinked sync root must fail before daemon activation");
        assert!(error.to_string().contains("not a symbolic link"));
        Ok(())
    }

    #[test]
    fn daemon_control_reports_invalid_list_continuations() -> Result<()> {
        let temp = tempdir()?;
        let mut state = State::open(temp.path().join("state"))?;
        let (server, mut client) = UnixStream::pair()?;
        let workers = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let (events, _event_rx) = std::sync::mpsc::channel();
        let worker_copy = workers.clone();
        let thread = std::thread::spawn(move || {
            handle_daemon_request(&mut state, &worker_copy, &events, server)
        });
        serde_json::to_writer(
            &mut client,
            &DaemonRequest::List {
                cursor: Some("missing".into()),
            },
        )?;
        client.write_all(b"\n")?;
        let response: DaemonResponse = serde_json::from_slice(&read_daemon_message(&mut client)?)?;
        assert!(
            matches!(response, DaemonResponse::Error { message } if message.contains("continuation"))
        );
        thread
            .join()
            .expect("daemon request thread must not panic")?;
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn daemon_control_lists_durable_syncs() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path().join("root");
        std::fs::create_dir(&root)?;
        let mut state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        state.set_watch_enabled(&share, true)?;
        state.set_blocked(&share, "repair required")?;
        let (server, mut client) = UnixStream::pair()?;
        let workers = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let (events, _event_rx) = std::sync::mpsc::channel();
        let worker_copy = workers.clone();
        let thread = std::thread::spawn(move || {
            handle_daemon_request(&mut state, &worker_copy, &events, server)
        });
        serde_json::to_writer(&mut client, &DaemonRequest::List { cursor: None })?;
        client.write_all(b"\n")?;
        let response: DaemonResponse = serde_json::from_slice(&read_daemon_message(&mut client)?)?;
        match response {
            DaemonResponse::List { syncs, next } => {
                assert_eq!(syncs.len(), 1);
                assert_eq!(next, None);
                assert_eq!(syncs[0].share, share.0);
                assert_eq!(
                    daemon_path_bytes(&syncs[0].root)?,
                    path_bytes(&root.canonicalize()?)
                );
                assert_eq!(syncs[0].state, "blocked");
                assert_eq!(syncs[0].diagnostic.as_deref(), Some("repair required"));
                assert!(syncs[0].enabled);
            }
            _ => panic!("unexpected daemon response"),
        }
        thread.join().expect("daemon request thread panicked")?;
        Ok(())
    }

    #[test]
    fn daemon_list_pages_large_valid_state() -> Result<()> {
        let temp = tempdir()?;
        let state = State::open(temp.path().join("state"))?;
        for index in 0..20 {
            let root = temp.path().join(format!("root-{index}"));
            std::fs::create_dir(&root)?;
            let share = state.init_share(&root)?;
            state.set_blocked(&share, &"x".repeat(4096))?;
        }
        let workers = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let mut cursor = None;
        let mut count = 0;
        loop {
            let DaemonResponse::List { syncs, next } = daemon_sync_page(&state, &workers, cursor)?
            else {
                panic!("expected list response")
            };
            assert!(!syncs.is_empty());
            count += syncs.len();
            match next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        assert_eq!(count, 20);
        assert!(daemon_sync_page(&state, &workers, Some("missing".into())).is_err());
        Ok(())
    }

    #[test]
    fn repaired_share_recovers_its_pending_install_before_restarting() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path().join("root");
        std::fs::create_dir(&root)?;
        let mut state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        state.set_install_intent(&share, &[])?;
        recover_daemon_share_install(&mut state, &share)?;
        assert!(state.install_intent(&share)?.is_none());
        Ok(())
    }

    #[test]
    fn managed_controls_validate_state_and_stop_a_worker() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path().join("root");
        std::fs::create_dir(&root)?;
        let mut state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        let workers = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let (events, _events_rx) = std::sync::mpsc::channel();

        assert!(
            start_managed_share(&mut state, &workers, &events, share.clone(), None)
                .expect_err("responder-only share must not start")
                .to_string()
                .contains("responder-only")
        );

        state.set_peer(
            &share,
            &PeerConfig {
                peer_id: flocal::model::PeerId("peer-test".into()),
                host: "example.invalid".into(),
                remote_path: path_bytes(Path::new("/remote")),
                executable: "/bin/false".into(),
            },
        )?;
        assert!(
            start_managed_share(&mut state, &workers, &events, share.clone(), None)
                .expect_err("initial sync must be complete")
                .to_string()
                .contains("initial synchronization")
        );

        state.set_initial_complete(&share)?;
        state.set_watch_enabled(&share, true)?;
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = Arc::new(AtomicBool::new(false));
        let finished = Arc::new(AtomicBool::new(true));
        workers.lock().unwrap().insert(
            share.clone(),
            DaemonWorker {
                id: 7,
                stop: stop.clone(),
                state: Arc::new(std::sync::atomic::AtomicU8::new(WORKER_WATCHING)),
                stopping: stopping.clone(),
                finished,
                child: Arc::new(Mutex::new(None)),
            },
        );
        stop_managed_share(&state, &workers, share.clone())?;
        assert!(!state.managed_share(&share)?.watch_enabled);
        assert!(stop.load(Ordering::Relaxed));
        assert!(stopping.load(Ordering::Relaxed));
        Ok(())
    }

    #[test]
    fn managed_worker_stops_during_an_unavailable_peer_retry() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path().join("root");
        std::fs::create_dir(&root)?;
        let mut state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        state.set_peer(
            &share,
            &PeerConfig {
                peer_id: flocal::model::PeerId("peer-test".into()),
                host: "127.0.0.1".into(),
                remote_path: path_bytes(Path::new("/remote")),
                executable: "/bin/false".into(),
            },
        )?;
        state.set_initial_complete(&share)?;
        let workers = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let (events, event_rx) = std::sync::mpsc::channel();
        start_managed_share(&mut state, &workers, &events, share.clone(), None)?;
        assert!(workers.lock().unwrap().contains_key(&share));
        stop_managed_share(&state, &workers, share.clone())?;
        let event = event_rx.recv_timeout(Duration::from_secs(3))?;
        apply_worker_event(&state, &workers, event)?;
        assert!(!state.managed_share(&share)?.watch_enabled);
        Ok(())
    }

    #[test]
    fn service_path_validation_rejects_invalid_characters() -> Result<()> {
        validate_service_path_characters("path<&>")?;
        let error = validate_service_path_characters("path\u{1}").unwrap_err();
        assert_eq!(
            error.to_string(),
            "service paths cannot contain control characters or XML noncharacters"
        );
        assert!(validate_service_path_characters("path\u{fdd0}").is_err());
        Ok(())
    }

    #[test]
    fn disabled_worker_exits_without_opening_a_remote_session() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path().join("root");
        std::fs::create_dir(&root)?;
        let state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        let workers = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let (events, event_rx) = std::sync::mpsc::channel();
        start_worker(&state, &workers, &events, share.clone())?;
        let event = event_rx.recv_timeout(Duration::from_secs(2))?;
        apply_worker_event(&state, &workers, event)?;
        assert!(workers.lock().unwrap().is_empty());
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn daemon_requests_return_structured_control_errors_and_live_states() -> Result<()> {
        fn request(
            state: &mut State,
            workers: &Arc<Mutex<std::collections::HashMap<ShareId, DaemonWorker>>>,
            events: &std::sync::mpsc::Sender<WorkerEvent>,
            request: DaemonRequest,
        ) -> Result<DaemonResponse> {
            let (server, mut client) = UnixStream::pair()?;
            let workers = workers.clone();
            let events = events.clone();
            std::thread::scope(|scope| {
                scope.spawn(|| handle_daemon_request(state, &workers, &events, server));
                serde_json::to_writer(&mut client, &request)?;
                client.write_all(b"\n")?;
                serde_json::from_slice(&read_daemon_message(&mut client)?).map_err(Into::into)
            })
        }

        let temp = tempdir()?;
        let root = temp.path().join("root");
        std::fs::create_dir(&root)?;
        let mut state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        state.set_initial_complete(&share)?;
        state.set_watch_enabled(&share, true)?;
        let workers = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let (events, _events_rx) = std::sync::mpsc::channel();

        assert!(matches!(
            request(
                &mut state,
                &workers,
                &events,
                DaemonRequest::Start {
                    share: "missing".into(),
                    generation: None
                }
            )?,
            DaemonResponse::Error { .. }
        ));
        assert!(matches!(
            request(
                &mut state,
                &workers,
                &events,
                DaemonRequest::Stop {
                    share: "missing".into()
                }
            )?,
            DaemonResponse::Error { .. }
        ));

        let stopping = Arc::new(AtomicBool::new(false));
        workers.lock().unwrap().insert(
            share.clone(),
            DaemonWorker {
                id: 11,
                stop: Arc::new(AtomicBool::new(false)),
                state: Arc::new(std::sync::atomic::AtomicU8::new(WORKER_RECONNECTING)),
                stopping: stopping.clone(),
                finished: Arc::new(AtomicBool::new(true)),
                child: Arc::new(Mutex::new(None)),
            },
        );
        let DaemonResponse::List { syncs, .. } = request(
            &mut state,
            &workers,
            &events,
            DaemonRequest::List { cursor: None },
        )?
        else {
            panic!("expected list")
        };
        assert_eq!(syncs[0].state, "reconnecting");
        assert!(matches!(
            request(
                &mut state,
                &workers,
                &events,
                DaemonRequest::Stop {
                    share: share.0.clone()
                }
            )?,
            DaemonResponse::Ok
        ));
        assert!(stopping.load(Ordering::Relaxed));
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn systemd_install_uses_manager_config_and_commits_the_state_marker() -> Result<()> {
        if let Some(root) = std::env::var_os(SYSTEMD_INSTALL_TEST_ROOT) {
            return assert_systemd_install(Path::new(&root));
        }

        let temp = tempdir()?;
        let bin = temp.path().join("bin");
        let manager_home = temp.path().join("manager-home");
        let client_home = temp.path().join("client-home");
        std::fs::create_dir(&bin)?;
        std::fs::create_dir(&manager_home)?;
        std::fs::create_dir(&client_home)?;
        let systemctl = bin.join("systemctl");
        std::fs::write(
            &systemctl,
            "#!/bin/sh\nif [ \"$2\" = show-environment ]; then printf 'HOME=%s\\n' \"$FLOCAL_TEST_MANAGER_HOME\"; fi\nexit 0\n",
        )?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&systemctl, std::fs::Permissions::from_mode(0o700))?;
        }
        let output = Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "tests::systemd_install_uses_manager_config_and_commits_the_state_marker",
            ])
            .env(SYSTEMD_INSTALL_TEST_ROOT, temp.path())
            .env(
                "PATH",
                format!(
                    "{}:{}",
                    bin.display(),
                    std::env::var_os("PATH")
                        .as_deref()
                        .unwrap_or_default()
                        .to_string_lossy()
                ),
            )
            .env("HOME", &client_home)
            .env("FLOCAL_TEST_MANAGER_HOME", &manager_home)
            .output()?;
        assert!(output.status.success(), "{:?}", output);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn assert_systemd_install(root: &Path) -> Result<()> {
        let manager_home = root.join("manager-home");
        let client_home = root.join("client-home");
        let state = State::open(root.join("state"))?;
        let executable = std::env::current_exe()?.canonicalize()?;
        preflight_daemon_service(&state, &executable)?;
        use std::os::unix::ffi::OsStringExt;
        let invalid_state =
            State::open(root.join(std::ffi::OsString::from_vec(b"state-\xff".to_vec())))?;
        assert!(install_systemd_service(&invalid_state, &executable).is_err());
        assert!(
            !manager_home
                .join(".config/systemd/user/flocal-daemon.service")
                .exists()
        );
        assert!(systemd_quote(Path::new("/tmp/invalid%path")).is_err());
        assert!(run_manager("/bin/false", &[]).is_err());
        assert!(ensure_daemon(&state).is_err());
        install_daemon_service(&state)?;
        assert!(
            manager_home
                .join(".config/systemd/user/flocal-daemon.service")
                .is_file()
        );
        assert_eq!(
            std::fs::read_to_string(client_home.join(".config/file.local/managed-state"))?,
            format!("{}\n", state.dir.display())
        );
        Ok(())
    }

    #[test]
    fn daemon_control_rejects_oversized_messages() {
        let input = vec![b'x'; MAX_DAEMON_MESSAGE_BYTES + 1];
        let error = read_daemon_message(&mut input.as_slice())
            .expect_err("control framing must bound allocation");
        assert!(error.to_string().contains("exceeds"));
    }

    #[cfg(unix)]
    #[test]
    fn service_definition_replacement_refuses_a_symlinked_parent() -> Result<()> {
        use std::os::unix::fs::{MetadataExt, symlink};

        let temp = tempdir()?;
        let target = temp.path().join("target");
        let link = temp.path().join("link");
        std::fs::create_dir(&target)?;
        symlink(&target, &link)?;
        assert!(install_text_file(&link.join("service"), "value").is_err());

        let definition = target.join("service");
        install_text_file(&definition, "value")?;
        assert_eq!(std::fs::read_to_string(&definition)?, "value");
        assert_eq!(std::fs::metadata(&definition)?.mode() & 0o777, 0o600);
        Ok(())
    }
}
