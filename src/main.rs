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
        version: RestoreVersion,
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
            destination,
            force,
        } => restore(&state, &path, &conflict_id, version, &destination, force)?,
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
            ensure_daemon(state)?;
            let share = match state.find_share(&path) {
                Ok((share, _)) => {
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
                    println!(
                        "{}  {}  {}  {}{}",
                        root.display(),
                        peer,
                        desired,
                        sync.state,
                        diagnostic
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
        bail!("launchd service paths cannot contain control characters or XML noncharacters")
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
            let state_name = if let Some(diagnostic) = &share.blocked_diagnostic {
                let _ = diagnostic;
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
                share: share.id.0,
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
        let mut candidate = page.clone();
        let next = sync.share.clone();
        candidate.push(sync);
        let response = DaemonResponse::List {
            syncs: candidate.clone(),
            next: Some(next),
        };
        if serde_json::to_vec(&response)?.len() + 1 > MAX_DAEMON_MESSAGE_BYTES {
            if page.is_empty() {
                bail!("one managed sync exceeds the daemon list page limit");
            }
            break;
        }
        page = candidate;
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
        sync::apply_plan(state, &share, &intent.records)
            .with_context(|| format!("recovering interrupted install for {}", share.0))?;
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
    sync::apply_plan(state, share, &intent.records)
        .with_context(|| format!("recovering interrupted install for {}", share.0))
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
    let plan = sync::plan(&local, &remote_records);
    if report == PlanReport::Full {
        print_plan(&local, &remote_records, &plan, json, report)?;
    }
    if dry_run {
        sync::write_message(&mut remote.input, &Message::Cancel)?;
        remote.finish()?;
        return Ok(SyncCompletion::default());
    }
    if !state.initial_complete(&share)? && !yes && !confirm("Apply this initial plan?")? {
        sync::write_message(&mut remote.input, &Message::Cancel)?;
        remote.finish()?;
        return Ok(SyncCompletion::default());
    }

    let root = state.root_for(&share)?;
    let matcher = flocal::scan::IgnoreMatcher::new(&root)?;
    let mut required_records = plan.records.clone();
    for conflict in &plan.conflicts {
        if matcher.is_record_ignored(&conflict.winner) {
            continue;
        }
        required_records.push(conflict.winner.clone());
        required_records.push(conflict.loser.clone());
    }
    let needs = sync::required_hashes_for_share(state, &share, &required_records)?;
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
        match sync::read_message(&mut remote.output)? {
            Message::ObjectStart { hash, size } if hash == expected => {
                if expected_sizes.get(&hash) != Some(&size) {
                    bail!("peer object size differs from the validated plan");
                }
                received_bytes = received_bytes.saturating_add(size);
                if received_bytes > transfer_limit {
                    bail!("inbound object transfer exceeds session byte limit");
                }
                sync::receive_object(state, hash, size, &mut remote.output)?;
            }
            other => bail!("expected object {expected}, got {other:?}"),
        }
    }
    match sync::read_message(&mut remote.output)? {
        Message::Done => {}
        other => bail!("expected object completion, got {other:?}"),
    }

    sync::write_snapshot(&mut remote.input, &local)?;
    sync::write_plan(&mut remote.input, &plan)?;
    let remote_needs = match sync::read_message(&mut remote.output)? {
        Message::Need { hashes } => hashes,
        other => bail!("expected remote object request, got {other:?}"),
    };
    let unique: std::collections::HashSet<_> = remote_needs.iter().collect();
    if unique.len() != remote_needs.len() {
        bail!("peer object request contains duplicate hashes");
    }
    let mut allowed_outbound: std::collections::HashSet<_> =
        local.iter().filter_map(record_hash).collect();
    for conflict in &plan.conflicts {
        allowed_outbound.extend(
            [record_hash(&conflict.winner), record_hash(&conflict.loser)]
                .into_iter()
                .flatten(),
        );
    }
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
    match sync::read_message(&mut remote.output)? {
        Message::Applied => {}
        Message::Error { message } => bail!("remote apply failed: {}", escaped(&message)),
        other => bail!("expected apply response, got {other:?}"),
    }
    sync::apply_plan(state, &share, &plan.records)?;
    state.add_conflicts(&share, &plan.conflicts)?;
    state.set_initial_complete(&share)?;
    state.prune_unreferenced_objects()?;
    let committed_report = if report == PlanReport::Watch {
        let mut output = Vec::new();
        write_plan_report(&mut output, &local, &remote_records, &plan, json, report)?;
        Some(output)
    } else {
        None
    };
    let finalization =
        sync::write_message(&mut remote.input, &Message::Done).and_then(|()| remote.finish());
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
            serve_sync(state, &share, &records, &mut input, &mut output)?;
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
                retryable: false,
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
    write_v2_session(output, V2SessionFrame::Ready { generation: 0 })?;

    let config = flocal::watch::WatchConfig::default();
    let mut debounce = flocal::watch::Debounce::default();
    let mut advertised_generation = 0u64;
    let mut round = 0u64;
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
            V2Envelope::Session {
                frame: V2SessionFrame::Ready { .. },
            } => {}
            V2Envelope::Round {
                round: incoming,
                frame:
                    V2RoundFrame::SyncStart {
                        connector_generation: _,
                        responder_generation: _,
                    },
            } if incoming == round + 1 => {
                round = incoming;
                if let Err(error) = serve_v2_round(state, &share, &share_root, round, input, output)
                {
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
    };
    loop {
        match recv_v2_round(input, round, budget)? {
            V2RoundFrame::ApplyChunk { records, conflicts } => {
                budget
                    .add_metadata(serde_json::to_vec(&records)?.len())
                    .map_err(|error| watch_protocol_error(format!("{error:#}")))?;
                budget
                    .add_metadata(serde_json::to_vec(&conflicts)?.len())
                    .map_err(|error| watch_protocol_error(format!("{error:#}")))?;
                plan.records.extend(records);
                plan.conflicts.extend(conflicts);
                if plan.records.len() > sync::MAX_RECORDS_PER_SESSION {
                    watch_protocol_bail!("apply plan exceeds session record limit");
                }
            }
            V2RoundFrame::ApplyEnd => return Ok(plan),
            other => watch_protocol_bail!("expected persistent apply plan, got {other:?}"),
        }
    }
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
    hash: flocal::model::ObjectHash,
    size: u64,
    round: u64,
    input: &impl AsFd,
    budget: &sync::RoundBudget,
) -> Result<()> {
    let mut sink = state.begin_object(hash, size)?;
    loop {
        match recv_v2_round(input, round, budget)? {
            V2RoundFrame::ObjectChunk { data } => sink.write_chunk(&data)?,
            V2RoundFrame::ObjectEnd => return sink.finish(),
            other => watch_protocol_bail!("unexpected persistent object frame: {other:?}"),
        }
    }
}

fn serve_v2_round(
    state: &mut State,
    share: &ShareId,
    root: &sync::ShareRoot,
    round: u64,
    input: &impl AsFd,
    output: &impl AsFd,
) -> Result<()> {
    let _global_lock = state.lock_global_sync()?;
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
    let allowed: std::collections::HashSet<_> = advertised.iter().filter_map(record_hash).collect();
    for hash in requested {
        if !allowed.contains(&hash) {
            watch_protocol_bail!("peer requested an object outside this share");
        }
        send_v2_object(state, &hash, round, output, &mut budget)?;
    }
    write_v2_round(output, round, V2RoundFrame::Done, &budget)?;

    let peer_records = read_v2_snapshot(input, round, &mut budget)?;
    let plan = read_v2_plan(input, round, &mut budget)?;
    let expected = sync::plan(&advertised, &peer_records);
    if plan.records != expected.records || plan.conflicts != expected.conflicts {
        watch_protocol_bail!("peer apply plan differs from local reconciliation");
    }
    let mut required = plan.records.clone();
    for conflict in &plan.conflicts {
        required.push(conflict.winner.clone());
        required.push(conflict.loser.clone());
    }
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
                receive_v2_object(state, hash, size, round, input, &budget)?;
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
    budget.check()?;
    sync::apply_plan_with_root(state, share, root, &plan.records)?;
    budget.check()?;
    state.add_conflicts(share, &plan.conflicts)?;
    budget.check()?;
    state.set_initial_complete(share)?;
    budget.check()?;
    state.prune_unreferenced_objects()?;
    budget.check()?;
    write_v2_round(output, round, V2RoundFrame::Applied, &budget)?;
    match recv_v2_round(input, round, &budget)? {
        V2RoundFrame::SyncFinished => Ok(()),
        other => watch_protocol_bail!("expected persistent round completion, got {other:?}"),
    }
}

fn serve_sync(
    state: &mut State,
    share: &ShareId,
    advertised: &[flocal::model::Record],
    input: &mut impl io::Read,
    output: &mut impl Write,
) -> Result<()> {
    let mut pending = flocal::reconcile::Plan {
        records: Vec::new(),
        conflicts: Vec::new(),
    };
    let mut plan_ready = false;
    let mut received_bytes = 0u64;
    let mut peer_records = Vec::new();
    let mut peer_snapshot_done = false;
    let mut metadata_bytes = 0usize;
    let mut allowed_outbound: std::collections::HashSet<_> =
        advertised.iter().filter_map(record_hash).collect();
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
                sync::receive_object(state, hash, size, input)?
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
                peer_snapshot_done = true;
            }
            Message::ApplyChunk { records, conflicts } => {
                if !peer_snapshot_done || plan_ready {
                    bail!("apply chunk received out of order");
                }
                metadata_bytes = metadata_bytes.saturating_add(
                    serde_json::to_vec(&(records.as_slice(), conflicts.as_slice()))?.len(),
                );
                if metadata_bytes > sync::MAX_METADATA_BYTES_PER_SESSION {
                    bail!("apply plan exceeds session metadata limit");
                }
                if pending.records.len().saturating_add(records.len())
                    > sync::MAX_RECORDS_PER_SESSION
                    || pending.conflicts.len().saturating_add(conflicts.len())
                        > sync::MAX_RECORDS_PER_SESSION
                {
                    bail!("apply plan exceeds session record limit");
                }
                pending.records.extend(records);
                pending.conflicts.extend(conflicts);
            }
            Message::ApplyEnd => {
                if !peer_snapshot_done || plan_ready {
                    bail!("apply end received out of order");
                }
                let expected = sync::plan(advertised, &peer_records);
                if pending != expected {
                    bail!("peer apply plan does not match deterministic reconciliation");
                }
                plan_ready = true;
                let mut required_records = pending.records.clone();
                for conflict in &pending.conflicts {
                    required_records.push(conflict.winner.clone());
                    required_records.push(conflict.loser.clone());
                }
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
                match sync::apply_plan(state, share, &pending.records) {
                    Ok(()) => {
                        state.add_conflicts(share, &pending.conflicts)?;
                        state.prune_unreferenced_objects()?;
                        sync::write_message(output, &Message::Applied)?;
                        pending.records.clear();
                        pending.conflicts.clear();
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
            Message::Done => break,
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
    if json {
        println!(
            "{}",
            serde_json::json!({"schema":2,"share":share.0,"root":root,"peer":peer,"entries":entries,"tombstones":tombstones,"initial_complete":state.initial_complete(&share)?,"view":"last_persisted_scan","pending_install":pending_install})
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
                        &serde_json::json!({"schema": 1, "conflicts": conflicts})
                    )?
                );
            } else {
                for conflict in conflicts {
                    println!(
                        "{}  {}  winner={} loser={}",
                        conflict.id,
                        conflict.path.display(),
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
                        &serde_json::json!({"schema": 1, "conflict": conflict})
                    )?
                );
            } else {
                println!("Conflict {}: {}", conflict.id, conflict.path.display());
                println!("Winner: {}", conflict.winner.version.peer.0);
                println!("Loser:  {}", conflict.loser.version.peer.0);
            }
        }
    }
    Ok(())
}

fn restore(
    state: &State,
    path: &Path,
    id: &str,
    version: RestoreVersion,
    destination: &Path,
    force: bool,
) -> Result<()> {
    let (share, _) = state.find_share(path)?;
    let conflict = state.conflict(&share, id)?;
    let record = if matches!(version, RestoreVersion::Winner) {
        &conflict.winner
    } else {
        &conflict.loser
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
        } => return Err(RemoteWatchError { retryable, message }.into()),
        other => {
            return Err(watch_protocol_error(format!(
                "remote does not support persistent watch protocol version 2: {other:?}; upgrade flocal on both peers"
            )));
        }
    }
    match sync::read_v2_envelope_until(
        remote_input,
        std::time::Instant::now() + sync::default_frame_deadline(),
    )? {
        V2Envelope::Session {
            frame: V2SessionFrame::Ready { .. },
        } => {}
        V2Envelope::Session {
            frame: V2SessionFrame::Error { retryable, message },
        } => return Err(RemoteWatchError { retryable, message }.into()),
        other => {
            return Err(watch_protocol_error(format!(
                "unexpected persistent readiness frame: {other:?}"
            )));
        }
    }
    write_v2_session(remote_output, V2SessionFrame::Ready { generation: 0 })?;

    let mut remote_generation = 0u64;
    let mut round = 1u64;
    let mut completed_local = 0u64;
    let startup_local_generation = match watch_state.snapshot() {
        flocal::watch::WatchSnapshot::Healthy { generation } => generation,
        flocal::watch::WatchSnapshot::Lost(error) => bail!("filesystem watcher stopped: {error}"),
    };
    let report = connector_v2_round(
        state,
        share,
        root,
        round,
        startup_local_generation,
        0,
        remote_input,
        remote_output,
        &mut remote_generation,
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
            let report = connector_v2_round(
                state,
                share,
                root,
                round,
                frozen_local,
                frozen_remote,
                remote_input,
                remote_output,
                &mut remote_generation,
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
) -> Result<Vec<u8>> {
    let _global_lock = state.lock_global_sync()?;
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
    let plan = sync::plan(&local, &remote_records);
    let mut required = plan.records.clone();
    for conflict in &plan.conflicts {
        required.push(conflict.winner.clone());
        required.push(conflict.loser.clone());
    }
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
    let mut allowed: std::collections::HashSet<_> = local.iter().filter_map(record_hash).collect();
    for conflict in &plan.conflicts {
        allowed.extend(
            [record_hash(&conflict.winner), record_hash(&conflict.loser)]
                .into_iter()
                .flatten(),
        );
    }
    for hash in remote_needs {
        if !allowed.contains(&hash) {
            watch_protocol_bail!("peer requested an object outside this share");
        }
        send_v2_object(state, &hash, round, output, &mut budget)?;
    }
    write_v2_round(output, round, V2RoundFrame::Done, &budget)?;
    match recv_connector_round(input, round, &budget, pending_remote_generation, false)? {
        V2RoundFrame::Applied => {}
        other => watch_protocol_bail!("expected persistent apply response, got {other:?}"),
    }
    budget.check()?;
    sync::apply_plan_with_root(state, share, root, &plan.records)?;
    budget.check()?;
    state.add_conflicts(share, &plan.conflicts)?;
    budget.check()?;
    state.set_initial_complete(share)?;
    budget.check()?;
    state.prune_unreferenced_objects()?;
    budget.check()?;
    let mut report = Vec::new();
    write_plan_report(
        &mut report,
        &local,
        &remote_records,
        &plan,
        false,
        PlanReport::Watch,
    )?;
    write_v2_round(output, round, V2RoundFrame::SyncFinished, &budget)?;
    Ok(report)
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

fn receive_connector_object(
    state: &State,
    hash: flocal::model::ObjectHash,
    size: u64,
    round: u64,
    input: &impl AsFd,
    budget: &sync::RoundBudget,
    pending_remote_generation: &mut u64,
) -> Result<()> {
    let mut sink = state.begin_object(hash, size)?;
    loop {
        match recv_connector_round(input, round, budget, pending_remote_generation, false)? {
            V2RoundFrame::ObjectChunk { data } => sink.write_chunk(&data)?,
            V2RoundFrame::ObjectEnd => return sink.finish(),
            other => watch_protocol_bail!("unexpected persistent object frame: {other:?}"),
        }
    }
}

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
        writeln!(output, "{}", serde_json::json!({"schema": 1, "plan": plan}))?;
        return Ok(());
    }
    let local_by_path: std::collections::HashMap<_, _> =
        local.iter().map(|r| (r.path.as_bytes(), r)).collect();
    let remote_by_path: std::collections::HashMap<_, _> =
        remote.iter().map(|r| (r.path.as_bytes(), r)).collect();
    // `watch`'s repeating background sync timestamps every printed line and
    // omits KEEP: on an idle share almost every path matches on both peers,
    // and a KEEP line per path per rescan cycle would drown out the
    // changes a live log exists to show. `flocal sync`'s plan is unabridged.
    let prefix = match report {
        PlanReport::Full => String::new(),
        PlanReport::Watch => format!("{} ", utc_timestamp()),
    };
    for record in &plan.records {
        let local_record = local_by_path.get(record.path.as_bytes());
        let remote_record = remote_by_path.get(record.path.as_bytes());
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
    for conflict in &plan.conflicts {
        writeln!(output, "{prefix}CONFLICT {}", conflict.path.display())?;
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
        let result = serve_sync(&mut state, &share, &[], &mut input.as_slice(), &mut output);
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
            Message::Done,
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
                timestamp_ns: 1,
                seen: Vec::new(),
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
                conflicts: vec![flocal::reconcile::Conflict {
                    path: merge_local.path.clone(),
                    winner: merge_local.clone(),
                    loser: merge_remote,
                }],
            },
            false,
            PlanReport::Full,
        )?;
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
                retryable: false,
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
        assert!(matches!(
            reject(&mut state, sync::WATCH_PROTOCOL_VERSION, bound_peer)?,
            V2SessionFrame::Error {
                retryable: false,
                ..
            }
        ));
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
            timestamp_ns: 1,
            seen: Vec::new(),
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
            conflicts: vec![flocal::reconcile::Conflict {
                path: winner.path.clone(),
                winner,
                loser,
            }],
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
        write_v2_session(&client_output, V2SessionFrame::Ready { generation: 0 })?;
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
                &remote_cap,
                1,
                &responder_reader,
                &responder_stream,
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
        )?;
        responder.join().expect("responder joins")?;

        assert_eq!(std::fs::read(local_root.join("from-remote"))?, b"remote");
        assert_eq!(std::fs::read(remote_root.join("from-local"))?, b"local");
        let report = String::from_utf8(report)?;
        assert!(report.contains("UPLOAD from-local"), "{report}");
        assert!(report.contains("DOWNLOAD from-remote"), "{report}");
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
        let expected_remote_peer = remote_peer.clone();

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
            write_v2_session(&responder_output, V2SessionFrame::Ready { generation: 0 })?;
            assert!(matches!(
                sync::read_v2_envelope_until(
                    &responder_input,
                    std::time::Instant::now() + sync::default_frame_deadline(),
                )?,
                V2Envelope::Session {
                    frame: V2SessionFrame::Ready { .. }
                }
            ));
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
                &remote_cap,
                1,
                &responder_input,
                &responder_output,
            )
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
                timestamp_ns: 1,
                seen: Vec::new(),
                entry,
            },
        };
        // kept matches on both peers (KEEP); uploaded exists locally only.
        let kept = record(b"kept", Entry::Directory);
        let uploaded = record(b"uploaded", Entry::Directory);
        let plan = flocal::reconcile::Plan {
            records: vec![kept.clone(), uploaded.clone()],
            conflicts: vec![flocal::reconcile::Conflict {
                path: kept.path.clone(),
                winner: kept.clone(),
                loser: uploaded.clone(),
            }],
        };
        let local = [kept.clone(), uploaded.clone()];
        let remote = [kept.clone()];

        let mut full = Vec::new();
        write_plan_report(&mut full, &local, &remote, &plan, false, PlanReport::Full)?;
        let full = String::from_utf8(full)?;
        assert!(full.contains("KEEP   kept"), "{full:?}");
        assert!(full.contains("UPLOAD uploaded"), "{full:?}");
        assert!(full.contains("CONFLICT kept"), "{full:?}");
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
        assert_eq!(lines.len(), 2, "{watch:?}");
        for (line, suffix) in lines.iter().zip(["UPLOAD uploaded", "CONFLICT kept"]) {
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
                assert_eq!(syncs[0].state, "stopped");
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
        assert!(validate_service_path_characters("path\u{1}").is_err());
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
