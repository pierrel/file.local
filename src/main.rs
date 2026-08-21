use std::fs::File;
use std::io::{self, Read, Write};
use std::os::fd::AsFd;
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use base64::Engine;
use clap::{Args, Parser, Subcommand, ValueEnum};
use flocal::model::{Entry, PeerConfig, PeerId, RelationshipId, ShareId};
use flocal::state::{
    EndpointBinding, IncomingRemoval, InstallationPermit, PairedQueueState, PreparedRemoval,
    QueuePosition, QueueRequest, RegistrationOutcome, RemovalFailureState, SchedulingSnapshot,
    State, SyncOperation, UpgradeLockAttempt, UpgradePending,
};
use flocal::sync::{
    self, InitialMessage, Message, RegisterRelationshipResponse, RelationshipRequest,
    RemoveRelationshipResponse, V2Envelope, V2RoundFrame, V2SessionFrame,
};
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
    Remove {
        path: Option<PathBuf>,
        #[arg(long)]
        share: Option<String>,
        #[arg(long)]
        local_only: bool,
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Subcommand)]
enum DaemonCommand {
    Run,
    #[command(hide = true)]
    Install {
        executable: PathBuf,
    },
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
    Relationship,
    #[cfg(feature = "e2e-test-hooks")]
    #[command(hide = true)]
    E2eHoldInstallation {
        path: PathBuf,
    },
    #[cfg(feature = "e2e-test-hooks")]
    #[command(hide = true)]
    E2eMakeRelationshipLegacy {
        path: PathBuf,
    },
    #[cfg(feature = "e2e-test-hooks")]
    #[command(hide = true)]
    E2eAssertRelationshipLegacy {
        path: PathBuf,
    },
}

#[derive(Subcommand)]
enum ConflictCommand {
    List {
        path: PathBuf,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        ids: bool,
        #[arg(long, requires = "ids")]
        limit: Option<usize>,
        #[arg(long, requires = "ids")]
        after: Option<String>,
    },
    Show {
        path: PathBuf,
        conflict_id: String,
        #[arg(long)]
        json: bool,
    },
    Prune {
        path: PathBuf,
        conflict_ids: Vec<String>,
        #[arg(long)]
        selection: Option<String>,
        #[arg(long)]
        yes: bool,
        #[arg(long)]
        json: bool,
    },
    Budget {
        path_or_size: String,
        size: Option<String>,
        #[arg(long)]
        share: Option<String>,
        #[arg(long)]
        peer: bool,
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
    if matches!(
        &cli.command,
        Commands::Protocol {
            command: ProtocolCommand::Relationship
        }
    ) {
        return serve_relationship();
    }
    if let Commands::Daemon {
        command: DaemonCommand::Install { executable },
    } = &cli.command
    {
        return install_daemon(executable);
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
            } => add_peer(&mut state, &path, &host, &remote_path)?,
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
                let completion = run_sync(
                    &mut state,
                    &path,
                    dry_run,
                    yes,
                    json,
                    PlanReport::Full,
                    None,
                )?;
                if completion.initial_applied {
                    let (share, _) = state.find_share(&path)?;
                    state.set_initial_complete(&share)?;
                }
            }
        },
        Commands::Status { path, json } => status(&mut state, &path, json)?,
        Commands::Conflicts { command } => conflicts(&mut state, command)?,
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
        Commands::Watch { path } => watch(&mut state, &path)?,
        Commands::Daemon {
            command: DaemonCommand::Run,
        } => daemon_run(state)?,
        Commands::Daemon {
            command: DaemonCommand::Install { .. },
        } => unreachable!("daemon install returns before state is opened"),
        Commands::Protocol {
            command: ProtocolCommand::Serve,
        } => serve(&mut state)?,
        Commands::Protocol {
            command: ProtocolCommand::Relationship,
        } => unreachable!("relationship protocol returns before state is opened"),
        #[cfg(feature = "e2e-test-hooks")]
        Commands::Protocol {
            command: ProtocolCommand::E2eHoldInstallation { path },
        } => e2e_hold_installation(&mut state, &path)?,
        #[cfg(feature = "e2e-test-hooks")]
        Commands::Protocol {
            command: ProtocolCommand::E2eMakeRelationshipLegacy { path },
        } => {
            let (share, _) = state.find_share(&path)?;
            state.e2e_make_relationship_legacy(&share)?;
        }
        #[cfg(feature = "e2e-test-hooks")]
        Commands::Protocol {
            command: ProtocolCommand::E2eAssertRelationshipLegacy { path },
        } => {
            let (share, _) = state.find_share(&path)?;
            state.e2e_assert_relationship_legacy(&share)?;
        }
    }
    Ok(())
}

fn validate_sync_arguments(arguments: &SyncArgs) -> Result<()> {
    let managed_name = arguments
        .path
        .as_deref()
        .and_then(Path::to_str)
        .is_some_and(|name| matches!(name, "add" | "list" | "start" | "stop" | "remove"));
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
    },
    Stop {
        share: String,
    },
    PrepareRemove {
        share: String,
        expected_binding: EndpointBinding,
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
    Prepared {
        removal: PreparedRemoval,
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
    connection_state: String,
    scheduling: String,
    waiting_on: Option<String>,
    operation: Option<SyncOperation>,
    queue_position: Option<usize>,
    waiting_root: Option<DaemonPath>,
    active_share: Option<String>,
    active_root: Option<DaemonPath>,
    active_operation: Option<SyncOperation>,
    diagnostic: Option<String>,
    unsettled: usize,
    role: String,
    registration_pending: bool,
    removal_pending: bool,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct DaemonPath {
    encoding: String,
    data: String,
}

enum SchedulingBlocker {
    Local(Option<PathBuf>),
    Peer,
}

struct ShareSchedulingView {
    state: &'static str,
    blocker: Option<SchedulingBlocker>,
    operation: Option<SyncOperation>,
    queue_position: Option<usize>,
    active_share: Option<ShareId>,
    active_root: Option<PathBuf>,
    active_operation: Option<SyncOperation>,
}

fn share_scheduling_view(
    state: &State,
    snapshot: &SchedulingSnapshot,
    share: &ShareId,
) -> Result<ShareSchedulingView> {
    let queued = snapshot
        .queued
        .iter()
        .enumerate()
        .find(|(_, request)| request.share.as_ref() == Some(share));
    let active_for_share = snapshot
        .active
        .as_ref()
        .filter(|request| request.share.as_ref() == Some(share));
    let active_share = snapshot
        .active
        .as_ref()
        .and_then(|request| request.share.clone());
    let active_root = active_share
        .as_ref()
        .map(|active| state.root_for(active))
        .transpose()?;
    let blocker = queued
        .map(|(_, request)| -> Result<SchedulingBlocker> {
            Ok(if snapshot.active.is_some() {
                SchedulingBlocker::Local(active_root.clone())
            } else if request
                .paired_state
                .is_some_and(|paired| paired != PairedQueueState::Eligible)
            {
                SchedulingBlocker::Peer
            } else {
                let predecessor_root = snapshot
                    .eligible_predecessors(request)
                    .first()
                    .and_then(|candidate| candidate.share.as_ref())
                    .map(|candidate| state.root_for(candidate))
                    .transpose()?;
                SchedulingBlocker::Local(predecessor_root)
            })
        })
        .transpose()?;
    Ok(ShareSchedulingView {
        state: if queued.is_some() {
            "queued"
        } else if active_for_share.is_some() {
            "active"
        } else {
            "idle"
        },
        blocker,
        operation: queued
            .map(|(_, request)| request.operation)
            .or_else(|| active_for_share.map(|request| request.operation)),
        queue_position: queued.and_then(|(_, request)| snapshot.queue_position(request)),
        active_share,
        active_root,
        active_operation: snapshot.active.as_ref().map(|active| active.operation),
    })
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
                        if peer.peer_id.is_none() {
                            add_peer(state, &path, &host, &remote_path)?;
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
            if !state.initial_complete(&share)? {
                if !complete_initial_and_enable(state, &share, yes)? {
                    return Ok(());
                }
            } else {
                daemon_request(
                    state,
                    DaemonRequest::Start {
                        share: share.0.clone(),
                    },
                )?;
            }
            report_managed_queue(state, &share)?;
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
                        &serde_json::json!({"schema": 3, "syncs": syncs})
                    )?
                );
            } else {
                for sync in syncs {
                    let root = bytes_path(&daemon_path_bytes(&sync.root)?);
                    let peer = match (sync.host, sync.remote_path) {
                        (Some(host), Some(path)) => {
                            format!(
                                "{host}:{}",
                                bytes_path(&daemon_path_bytes(&path)?).to_string_lossy()
                            )
                        }
                        _ => "responder".into(),
                    };
                    let diagnostic = sync
                        .diagnostic
                        .map(|value| format!("; {}", escaped(&value)))
                        .unwrap_or_default();
                    let desired = if sync.enabled { "enabled" } else { "disabled" };
                    let unsettled = if sync.unsettled == 0 {
                        String::new()
                    } else {
                        format!(
                            "; {} unsettled paths (see `flocal status {}`)",
                            sync.unsettled,
                            escaped(&root.to_string_lossy())
                        )
                    };
                    let guidance = if sync.removal_pending {
                        format!("; rerun `flocal sync remove --share {}`", sync.share)
                    } else if sync.registration_pending {
                        "; rerun `flocal sync add` or abandon with `flocal sync remove --local-only`"
                            .into()
                    } else {
                        String::new()
                    };
                    let scheduling = if sync.scheduling == "queued" {
                        match (sync.waiting_on.as_deref(), sync.waiting_root.as_ref()) {
                            (Some("local"), Some(waiting_root)) => format!(
                                "; waiting for {} sync to finish (queue position {})",
                                escaped(
                                    &bytes_path(&daemon_path_bytes(waiting_root)?)
                                        .to_string_lossy()
                                ),
                                sync.queue_position.unwrap_or(1)
                            ),
                            (Some("peer"), _) => format!(
                                "; waiting for the peer to finish another synchronization (queue position {})",
                                sync.queue_position.unwrap_or(1)
                            ),
                            _ => format!(
                                "; waiting for the installation synchronization slot (queue position {})",
                                sync.queue_position.unwrap_or(1)
                            ),
                        }
                    } else {
                        String::new()
                    };
                    println!(
                        "{}  {}  {}  {}{}{}{}{}",
                        escaped(&root.to_string_lossy()),
                        escaped(&peer),
                        desired,
                        sync.connection_state,
                        diagnostic,
                        unsettled,
                        guidance,
                        scheduling,
                    );
                }
            }
        }
        SyncCommand::Start { path, share, yes } => {
            ensure_daemon(state)?;
            let share = select_share(state, path.as_deref(), share.as_deref())?;
            let managed = state.managed_share(&share)?;
            if !managed.initial_complete {
                if !complete_initial_and_enable(state, &share, yes)? {
                    return Ok(());
                }
            } else {
                daemon_request(
                    state,
                    DaemonRequest::Start {
                        share: share.0.clone(),
                    },
                )?;
            }
            report_managed_queue(state, &share)?;
        }
        SyncCommand::Stop { path, share } => {
            ensure_daemon(state)?;
            let share = select_share(state, path.as_deref(), share.as_deref())?;
            daemon_request(state, DaemonRequest::Stop { share: share.0 })?;
        }
        SyncCommand::Remove {
            path,
            share,
            local_only,
            yes,
        } => remove_sync_relationship(state, path.as_deref(), share.as_deref(), local_only, yes)?,
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

fn complete_initial_and_enable(state: &mut State, share: &ShareId, yes: bool) -> Result<bool> {
    let generation = state.watch_intent_generation(share)?;
    let root = state.root_for(share)?;
    let completion = run_sync(
        state,
        &root,
        false,
        yes,
        false,
        PlanReport::Full,
        Some(generation),
    )?;
    Ok(completion.initial_applied)
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

fn report_managed_queue(state: &mut State, share: &ShareId) -> Result<()> {
    let snapshot = state.scheduling_snapshot()?;
    let view = share_scheduling_view(state, &snapshot, share)?;
    let Some(blocker) = view.blocker else {
        return Ok(());
    };
    let position = view.queue_position.unwrap_or(1);
    match blocker {
        SchedulingBlocker::Local(Some(root)) => println!(
            "Waiting for {} sync to finish (queue position {})",
            escaped(&root.to_string_lossy()),
            position
        ),
        SchedulingBlocker::Local(None) => println!(
            "Waiting for the installation synchronization slot (queue position {position})"
        ),
        SchedulingBlocker::Peer => println!(
            "Waiting for the peer to finish another synchronization (queue position {position})"
        ),
    }
    Ok(())
}

fn select_share_for_removal(
    state: &State,
    path: Option<&Path>,
    share: Option<&str>,
) -> Result<ShareId> {
    match (path, share) {
        (Some(_), Some(_)) | (None, None) => bail!("provide exactly one of PATH or --share"),
        (None, Some(share)) => {
            validate_share_id(share)?;
            state
                .managed_share(&ShareId(share.into()))
                .map(|share| share.id)
        }
        (Some(path), None) => state.find_share_by_exact_root(path).map(|(share, _)| share),
    }
}

fn remove_sync_relationship(
    state: &mut State,
    path: Option<&Path>,
    share: Option<&str>,
    local_only: bool,
    yes: bool,
) -> Result<()> {
    let share = select_share_for_removal(state, path, share)?;
    let root = state.root_for(&share)?;
    let binding = state.endpoint_binding(&share)?;
    if matches!(&binding, EndpointBinding::Unpaired) {
        println!("No relationship is configured; nothing to remove.");
        return Ok(());
    }
    let pending = state.removing_relationship(&share)?.is_some();
    let pending_install = state.install_intent(&share)?.is_some();

    println!("Remove relationship for:");
    println!("  Root:  {}", escaped(&root.to_string_lossy()));
    println!("  Share: {}", escaped(&share.0));
    match &binding {
        EndpointBinding::Connector(peer) => {
            println!("  Role:  connector");
            println!(
                "  Peer:  {}:{}",
                escaped(&peer.host),
                escaped(&bytes_path(&peer.remote_path).to_string_lossy())
            );
            if !local_only && peer.peer_id.is_none() {
                bail!(
                    "connector registration is incomplete; retry `flocal sync add` or use `flocal sync remove --local-only` to abandon it"
                );
            }
        }
        EndpointBinding::Responder { peer, .. } => {
            println!("  Role:  responder");
            println!("  Peer:  {}", escaped(&peer.0));
            if !local_only {
                bail!(
                    "this endpoint is the responder; run this command on the machine that originally ran `flocal sync add` (peer ID {}), or use `--local-only` here only if that connector is permanently unavailable",
                    escaped(&peer.0)
                );
            }
        }
        EndpointBinding::Unpaired => unreachable!(),
    }
    if local_only {
        println!("  Scope: local endpoint only");
        if pending {
            println!(
                "Warning: a prior two-sided removal may already have committed remotely even though its reply was lost."
            );
        } else if matches!(
            &binding,
            EndpointBinding::Connector(PeerConfig { peer_id: None, .. })
        ) {
            println!(
                "Warning: registration may already have committed remotely even though no reply was recorded."
            );
            println!(
                "Retry `flocal sync add` and then use normal removal if the responder is reachable."
            );
        }
        println!(
            "Warning: the peer may remain registered and must later run `flocal sync remove --local-only` before reusing that root."
        );
    } else {
        println!("  Scope: both endpoints");
    }
    println!(
        "If an earlier install was interrupted, its already-committed changes must finish before detach and may update visible entries."
    );
    if !yes && !confirm("Remove this relationship?")? {
        println!("Relationship unchanged.");
        return Ok(());
    }

    ensure_daemon(state)?;
    let response = daemon_request(
        state,
        DaemonRequest::PrepareRemove {
            share: share.0.clone(),
            expected_binding: binding.clone(),
        },
    )?;
    let DaemonResponse::Prepared { removal } = response else {
        bail!("daemon returned an invalid removal response")
    };

    if pending_install || state.install_intent(&share)?.is_some() {
        eprintln!("flocal: finishing an interrupted install before removal");
    }
    if let Err(error) = recover_daemon_share_install(state, &share) {
        return report_install_recovery_failure(state, &removal, &error);
    }

    let detached = if local_only {
        state.finalize_local_removal(&removal)?
    } else {
        let remote = remove_remote_relationship(state, &removal);
        if let Err(error) = remote {
            return report_remote_removal_failure(state, &removal, &error);
        }
        state.finalize_connector_removal(&removal)?
    };
    if let Some(warning) = detached.cleanup_warning {
        eprintln!(
            "flocal: relationship removed; object cleanup remains pending: {}",
            escaped(&warning)
        );
    }
    if local_only {
        println!(
            "Removed the local relationship. The peer may still require `flocal sync remove --local-only`."
        );
    } else {
        println!("Removed the relationship from both endpoints.");
    }
    Ok(())
}

fn report_remote_removal_failure(
    state: &mut State,
    removal: &PreparedRemoval,
    error: &anyhow::Error,
) -> Result<()> {
    let EndpointBinding::Connector(peer) = &removal.binding else {
        unreachable!("remote removal requires a connector binding")
    };
    match state.record_removal_failure(removal, &format!("{error:#}")) {
        Ok(RemovalFailureState::Pending) => bail!(
            "relationship removal is pending and disabled as of this peer failure; rerun `flocal sync remove --share {}` to retry: {error:#}",
            removal.share.0
        ),
        Ok(RemovalFailureState::Finalized) => bail!(
            "the local relationship was removed concurrently before the peer failure was classified, but removal from peer {} could not be confirmed; on that peer, run `flocal sync remove --share {} --local-only` before reusing its root: {error:#}",
            escaped(&peer.host),
            removal.share.0
        ),
        Ok(RemovalFailureState::Changed) => bail!(
            "removal from peer {} failed and the local relationship changed before that failure could be recorded; remote state is unconfirmed, so run `flocal sync remove --share {} --local-only` on that peer before reusing its root; remote error: {error:#}",
            escaped(&peer.host),
            removal.share.0
        ),
        Err(local_error) => bail!(
            "removal from peer {} failed, and the local removal failure could not be classified or recorded; inspect `flocal sync list` to determine whether this relationship is still pending. Remote state is unconfirmed, so run `flocal sync remove --share {} --local-only` on that peer before reusing its root; remote error: {error:#}; local state error: {local_error:#}",
            escaped(&peer.host),
            removal.share.0
        ),
    }
}

fn report_install_recovery_failure(
    state: &mut State,
    removal: &PreparedRemoval,
    error: &anyhow::Error,
) -> Result<()> {
    match state.record_removal_failure(removal, &format!("{error:#}")) {
        Ok(RemovalFailureState::Pending) => bail!(
            "relationship removal is pending and disabled as of this install recovery failure; retry after correcting install recovery: {error:#}"
        ),
        Ok(RemovalFailureState::Finalized) => bail!(
            "the local relationship was removed concurrently before this install recovery failure was classified; no local removal retry remained at that point: {error:#}"
        ),
        Ok(RemovalFailureState::Changed) => bail!(
            "install recovery failed and the local relationship changed before that failure could be recorded; inspect `flocal sync list` before retrying: {error:#}"
        ),
        Err(local_error) => bail!(
            "install recovery failed, and the local removal failure could not be classified or recorded; inspect `flocal sync list` to determine whether this relationship is still pending; recovery error: {error:#}; local state error: {local_error:#}"
        ),
    }
}

fn remove_remote_relationship(state: &State, prepared: &PreparedRemoval) -> Result<()> {
    let EndpointBinding::Connector(peer) = &prepared.binding else {
        bail!("two-sided removal requires a connector binding")
    };
    let expected_peer = peer.completed_peer_id()?.clone();
    let remote = RelationshipRemote::spawn(
        &peer.host,
        &peer.executable,
        sync::default_phase_deadline() + sync::default_frame_deadline(),
    )?;
    let request = RelationshipRequest::RemoveRelationship {
        removal_protocol: sync::RELATIONSHIP_REMOVAL_PROTOCOL_VERSION,
        share: prepared.share.clone(),
        peer: state.peer_id()?,
        expected_peer: expected_peer.clone(),
        relationship: prepared.relationship.clone(),
    };
    let exchange = (|| -> Result<RemoveRelationshipResponse> {
        sync::write_relationship_request_until(
            remote.input.as_ref().context("ssh stdin unavailable")?,
            &request,
            remote.deadline,
        )?;
        sync::read_remove_relationship_response_until(
            remote.output.as_ref().context("ssh stdout unavailable")?,
            remote.deadline,
        )
    })();
    let response = match exchange {
        Ok(response) => response,
        Err(error) => return Err(remote.finish_after_error(error)),
    };
    match response {
        RemoveRelationshipResponse::Absent {
            removal_protocol,
            share,
            peer,
            relationship,
        } if removal_protocol == sync::RELATIONSHIP_REMOVAL_PROTOCOL_VERSION
            && share == prepared.share
            && peer == expected_peer
            && relationship == prepared.relationship => {}
        RemoveRelationshipResponse::Error { message } => {
            let finish = remote.finish();
            finish?;
            bail!(
                "remote rejected relationship removal: {}",
                escaped(&message)
            )
        }
        _ => {
            return Err(remote.finish_after_error(anyhow::anyhow!(
                "remote returned a mismatched relationship-removal response"
            )));
        }
    }
    remote.finish()
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
    let read_timeout = if matches!(request, DaemonRequest::PrepareRemove { .. }) {
        Duration::from_secs(20)
    } else {
        Duration::from_secs(10)
    };
    stream.set_read_timeout(Some(read_timeout))?;
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

struct StagedExecutable {
    directory: cap_std::fs::Dir,
    sync_directory: File,
    temporary: PathBuf,
    destination: PathBuf,
    published: bool,
}

impl StagedExecutable {
    fn prepare(candidate: &Path, destination: &Path) -> Result<Self> {
        use cap_std::fs::{OpenOptions as CapOpenOptions, Permissions, PermissionsExt};

        if !destination.is_absolute() {
            bail!("installed executable path must be absolute")
        }
        let candidate = candidate.canonicalize()?;
        validate_trusted_executable(&candidate)?;
        let mut source = File::open(&candidate)?;
        validate_trusted_executable_file(&source)?;
        let parent = destination
            .parent()
            .context("installed executable has no parent directory")?;
        ensure_private_service_directory(parent)?;
        validate_service_dir_chain(parent)?;
        let sync_directory = File::open(parent)?;
        let directory = cap_std::fs::Dir::from_std_file(sync_directory.try_clone()?);
        let destination = PathBuf::from(
            destination
                .file_name()
                .context("installed executable has no file name")?,
        );
        validate_install_destination(&directory, &destination)?;
        let temporary = PathBuf::from(format!(".flocal-install-{}", ShareId::generate().0));
        let options = CapOpenOptions::new()
            .create_new(true)
            .write(true)
            .to_owned();
        let mut output = directory.open_with(&temporary, &options)?;
        io::copy(&mut source, &mut output).context("staging the candidate executable")?;
        directory.set_permissions(&temporary, Permissions::from_mode(0o755))?;
        flocal::durability::sync_file(&output)
            .context("syncing the staged candidate executable")?;
        Ok(Self {
            directory,
            sync_directory,
            temporary,
            destination,
            published: false,
        })
    }

    fn publish(&mut self) -> Result<()> {
        validate_install_destination(&self.directory, &self.destination)?;
        self.directory
            .rename(&self.temporary, &self.directory, &self.destination)
            .context("publishing the candidate executable")?;
        self.published = true;
        self.sync_directory
            .sync_all()
            .context("syncing the executable directory")?;
        Ok(())
    }
}

impl Drop for StagedExecutable {
    fn drop(&mut self) {
        if !self.published {
            let _ = self.directory.remove_file(&self.temporary);
        }
    }
}

fn validate_install_destination(directory: &cap_std::fs::Dir, path: &Path) -> Result<()> {
    let metadata = match directory.symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    #[cfg(unix)]
    {
        use cap_std::fs::MetadataExt;
        if !metadata.is_file()
            || metadata.file_type().is_symlink()
            || (metadata.uid() != rustix::process::geteuid().as_raw() && metadata.uid() != 0)
            || metadata.mode() & 0o022 != 0
        {
            bail!("installed executable destination is not a trusted regular file")
        }
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn install_daemon(destination: &Path) -> Result<()> {
    if rustix::process::geteuid().is_root() {
        bail!("make install is per-user; do not run it with sudo");
    }
    let state_dir = State::default_dir()?;
    if !state_dir.is_absolute() {
        bail!("daemon state path must be absolute")
    }
    flocal::state::ensure_private_directory(&state_dir)?;
    let _installer = State::acquire_installer_lock(&state_dir)?;
    validate_managed_state_selection(&state_dir, State::managed_state_dir()?)?;
    let inherited_pending = State::upgrade_pending_at(&state_dir)?;

    let candidate = std::env::current_exe()?;
    let mut staged = StagedExecutable::prepare(&candidate, destination)?;
    let service = prepare_daemon_service(&state_dir, destination)?;
    if service_managed(&service) {
        preflight_managed_state_marker()?;
        println!("Preflight complete; stopping managed synchronization");
    }
    if let Err(error) =
        stop_daemon_service(&service).context("stopping the managed daemon for upgrade")
    {
        return fail_before_publication(&service, &state_dir, inherited_pending, error);
    }
    let created_pending =
        match State::create_upgrade_pending(&state_dir).context("creating the upgrade marker") {
            Ok(created) => created,
            Err(error) => {
                return fail_before_publication(&service, &state_dir, inherited_pending, error);
            }
        };
    let preserve_pending = !created_pending;

    let quiesced = quiesce_for_upgrade(&state_dir);
    let (legacy_locks, barrier) = match quiesced {
        Ok(locks) => locks,
        Err(error) => {
            return fail_before_publication(&service, &state_dir, preserve_pending, error);
        }
    };

    if let Err(error) = staged.publish() {
        if !staged.published {
            drop(barrier);
            drop(legacy_locks);
            return fail_before_publication(&service, &state_dir, preserve_pending, error);
        }
        return Err(error.context(
            "the candidate executable was published; rerun `make install` to finish the upgrade",
        ));
    }
    println!("Candidate executable installed");
    if service_managed(&service) {
        let state = State::open_for_upgrade(&state_dir, &barrier)
            .context("migrating local state with the candidate")
            .context(
                "the candidate executable was published; rerun `make install` to finish the upgrade",
            )?;
        publish_daemon_service(&service, &state.dir)
            .context("publishing the managed service definition")
            .context(
                "the candidate executable was published; rerun `make install` to finish the upgrade",
            )?;
        drop(state);
        println!("State migration and managed service update complete");
    }
    complete_upgrade_marker(&state_dir, service_managed(&service), preserve_pending).context(
        "the candidate executable was published; rerun `make install` to finish the upgrade",
    )?;
    drop(barrier);
    drop(legacy_locks);
    if service_managed(&service) {
        println!("Starting enabled managed synchronization");
        start_and_verify_daemon(
            &service,
            &state_dir,
            std::time::Instant::now() + Duration::from_secs(5),
        )?;
        println!(
            "Installation complete; enabled syncs are restarting automatically. Upgrade the other endpoint next. Reconnecting and local-only edits are expected until then; no re-pair or state reset is needed."
        );
    } else {
        println!(
            "Installed binary only. In a macOS graphical session, run `make install` again to migrate state and resume managed synchronization."
        );
    }
    Ok(())
}

fn validate_managed_state_selection(selected: &Path, managed: Option<PathBuf>) -> Result<()> {
    if managed
        .as_deref()
        .is_some_and(|managed| managed != selected)
    {
        bail!(
            "FLOCAL_STATE_DIR does not match the existing managed installation; unset it or use the managed state path before upgrading"
        )
    }
    Ok(())
}

fn wait_for_daemon_ready(state_dir: &Path, deadline: std::time::Instant) -> Result<()> {
    let socket = state_dir.join("run/daemon.sock");
    loop {
        if matches!(
            daemon_request_inner(&socket, &DaemonRequest::List { cursor: None }),
            Ok(DaemonResponse::List { .. })
        ) {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            bail!("daemon control socket did not answer a list request")
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn start_and_verify_daemon(
    service: &DaemonService,
    state_dir: &Path,
    deadline: std::time::Instant,
) -> Result<()> {
    if let Err(error) = start_daemon_service(service) {
        if let Err(stop) = stop_started_daemon_service(service) {
            bail!(
                "the managed daemon start failed: {error:#}; stopping the partially started service also failed: {stop:#}; inspect the user-service logs and rerun `make install`"
            )
        }
        bail!(
            "the managed daemon start failed: {error:#}; the service was stopped; inspect the user-service logs and rerun `make install`"
        )
    }
    if let Err(error) = wait_for_daemon_ready(state_dir, deadline) {
        if let Err(stop) = stop_started_daemon_service(service) {
            bail!(
                "the service manager accepted the candidate but its daemon did not become ready: {error:#}; stopping the failed service also failed: {stop:#}; inspect the user-service logs and rerun `make install`"
            )
        }
        bail!(
            "the service manager accepted the candidate but its daemon did not become ready: {error:#}; the service was stopped; inspect the user-service logs and rerun `make install`"
        )
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn restore_before_upgrade(
    service: &DaemonService,
    state_dir: &Path,
    error: anyhow::Error,
) -> Result<()> {
    if let Err(cleanup) = State::remove_upgrade_pending(state_dir) {
        bail!(
            "upgrade failed before executable replacement: {error:#}; the upgrade marker could not be cleared, so the old service was not restarted: {cleanup:#}"
        )
    }
    if let Err(restoration) = restore_daemon_service(service) {
        bail!(
            "upgrade failed before executable replacement: {error:#}; the old service could not be restarted: {restoration:#}"
        )
    }
    Err(error)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn fail_before_publication(
    service: &DaemonService,
    state_dir: &Path,
    preserve_pending: bool,
    error: anyhow::Error,
) -> Result<()> {
    if preserve_pending {
        return Err(error.context(
            "an earlier upgrade is still pending; the service remains stopped; rerun `make install`",
        ));
    }
    restore_before_upgrade(service, state_dir, error)
}

fn complete_upgrade_marker(
    state_dir: &Path,
    managed_install_complete: bool,
    preserve_pending: bool,
) -> Result<()> {
    if managed_install_complete || !preserve_pending {
        State::remove_upgrade_pending(state_dir)?;
    }
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn install_daemon(_: &Path) -> Result<()> {
    bail!("managed sync is supported on Linux and macOS only")
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn quiesce_for_upgrade(
    state_dir: &Path,
) -> Result<(
    flocal::state::LegacyUpgradeLocks,
    flocal::state::UpgradeBarrier,
)> {
    quiesce_for_upgrade_until(
        state_dir,
        std::time::Instant::now() + Duration::from_secs(12),
    )
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn quiesce_for_upgrade_until(
    state_dir: &Path,
    deadline: std::time::Instant,
) -> Result<(
    flocal::state::LegacyUpgradeLocks,
    flocal::state::UpgradeBarrier,
)> {
    let mut reported = None;
    loop {
        match State::try_acquire_legacy_upgrade_locks(state_dir)? {
            UpgradeLockAttempt::Acquired(locks) => {
                if let Some(barrier) = State::try_acquire_upgrade_barrier(state_dir)? {
                    return Ok((locks, barrier));
                }
            }
            UpgradeLockAttempt::Busy(path) => {
                if reported.as_ref() != Some(&path) {
                    println!(
                        "Waiting for synchronization of {} to finish before upgrade",
                        escaped(&path.to_string_lossy())
                    );
                    reported = Some(path);
                }
            }
        }
        if std::time::Instant::now() >= deadline {
            let path = reported.unwrap_or_else(|| state_dir.to_path_buf());
            bail!(
                "could not safely stop synchronization of {} before upgrade; stop any foreground `flocal watch` or `flocal sync` using that path, upgrade the connector first, and retry `make install`",
                escaped(&path.to_string_lossy())
            )
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[cfg(target_os = "linux")]
struct DaemonService {
    unit: PathBuf,
    content: String,
    running: bool,
}

#[cfg(target_os = "linux")]
fn prepare_daemon_service(state_dir: &Path, executable: &Path) -> Result<DaemonService> {
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
    if !config.is_absolute() || !state_dir.is_absolute() {
        bail!("daemon service paths must be absolute")
    }
    let unit = config.join("systemd/user/flocal-daemon.service");
    preflight_text_file(&unit)?;
    let content = systemd_unit_content(executable, state_dir)?;
    let active_state = Command::new("systemctl")
        .args([
            "--user",
            "show",
            "--property=ActiveState",
            "--value",
            "flocal-daemon.service",
        ])
        .output()?;
    if !active_state.status.success() {
        bail!("cannot query whether flocal-daemon.service is active")
    }
    let running = match String::from_utf8(active_state.stdout)?.trim() {
        "active" | "activating" | "reloading" | "deactivating" => true,
        "inactive" | "failed" => false,
        state => bail!("systemd returned an unknown active state: {state}"),
    };
    Ok(DaemonService {
        unit,
        content,
        running,
    })
}

#[cfg(target_os = "linux")]
fn stop_daemon_service(service: &DaemonService) -> Result<()> {
    if service.running {
        run_manager("systemctl", &["--user", "stop", "flocal-daemon.service"])?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn publish_daemon_service(service: &DaemonService, state_dir: &Path) -> Result<()> {
    install_managed_state_marker(state_dir)?;
    install_text_file(&service.unit, &service.content)?;
    run_manager("systemctl", &["--user", "daemon-reload"])?;
    Ok(())
}

#[cfg(target_os = "linux")]
fn start_daemon_service(_: &DaemonService) -> Result<()> {
    run_manager(
        "systemctl",
        &["--user", "enable", "--now", "flocal-daemon.service"],
    )?;
    Ok(())
}

#[cfg(target_os = "linux")]
fn stop_started_daemon_service(_: &DaemonService) -> Result<()> {
    run_manager("systemctl", &["--user", "stop", "flocal-daemon.service"])
}

#[cfg(target_os = "linux")]
fn restore_daemon_service(service: &DaemonService) -> Result<()> {
    if service.running {
        run_manager("systemctl", &["--user", "start", "flocal-daemon.service"])?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn service_managed(_: &DaemonService) -> bool {
    true
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
struct DaemonService {
    domain: String,
    plist: PathBuf,
    content: String,
    available: bool,
    loaded: bool,
}

#[cfg(target_os = "macos")]
fn prepare_daemon_service(state_dir: &Path, executable: &Path) -> Result<DaemonService> {
    let target = launchd_target()?;
    let domain = target
        .trim_end_matches("/local.file-local.flocal-daemon")
        .to_owned();
    let available = Command::new("launchctl")
        .args(["print", &domain])
        .status()?
        .success();
    let loaded = available
        && Command::new("launchctl")
            .args(["print", &target])
            .status()?
            .success();
    let home = std::env::var_os("HOME").context("could not determine home directory")?;
    let plist =
        PathBuf::from(home).join("Library/LaunchAgents/local.file-local.flocal-daemon.plist");
    if available {
        preflight_text_file(&plist)?;
    }
    let executable = executable
        .to_str()
        .context("launchd executable path must be valid UTF-8")?;
    let state_dir = state_dir
        .to_str()
        .context("launchd state path must be valid UTF-8")?;
    plist
        .to_str()
        .context("launchd service path must be valid UTF-8")?;
    let content = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict><key>Label</key><string>local.file-local.flocal-daemon</string><key>ProgramArguments</key><array><string>{}</string><string>daemon</string><string>run</string></array><key>EnvironmentVariables</key><dict><key>FLOCAL_STATE_DIR</key><string>{}</string></dict><key>RunAtLoad</key><true/><key>KeepAlive</key><true/></dict></plist>\n",
        xml_escape(executable)?,
        xml_escape(state_dir)?,
    );
    Ok(DaemonService {
        domain,
        plist,
        content,
        available,
        loaded,
    })
}

#[cfg(target_os = "macos")]
fn stop_daemon_service(service: &DaemonService) -> Result<()> {
    if service.loaded {
        let plist = service
            .plist
            .to_str()
            .context("launchd service path must be valid UTF-8")?;
        run_manager("launchctl", &["bootout", &service.domain, plist])?;
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn publish_daemon_service(service: &DaemonService, state_dir: &Path) -> Result<()> {
    install_managed_state_marker(state_dir)?;
    install_text_file(&service.plist, &service.content)
}

#[cfg(target_os = "macos")]
fn start_daemon_service(service: &DaemonService) -> Result<()> {
    if service.available {
        let plist = service
            .plist
            .to_str()
            .context("launchd service path must be valid UTF-8")?;
        run_manager("launchctl", &["bootstrap", &service.domain, plist])?;
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn stop_started_daemon_service(service: &DaemonService) -> Result<()> {
    let plist = service
        .plist
        .to_str()
        .context("launchd service path must be valid UTF-8")?;
    run_manager("launchctl", &["bootout", &service.domain, plist])
}

#[cfg(target_os = "macos")]
fn restore_daemon_service(service: &DaemonService) -> Result<()> {
    if service.loaded {
        start_daemon_service(service)?;
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn service_managed(service: &DaemonService) -> bool {
    service.available
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

fn install_text_file(path: &Path, content: &str) -> Result<()> {
    use cap_std::fs::{OpenOptions as CapOpenOptions, Permissions, PermissionsExt};

    let parent = path
        .parent()
        .context("service definition has no parent directory")?;
    ensure_private_service_directory(parent)?;
    let sync_directory = File::open(parent)?;
    let directory = cap_std::fs::Dir::from_std_file(sync_directory.try_clone()?);
    let destination = Path::new(
        path.file_name()
            .context("service definition has no file name")?,
    );
    validate_private_install_file(&directory, destination)?;
    let temporary = PathBuf::from(format!(
        ".{}-{}",
        path.file_name().unwrap().to_string_lossy(),
        ShareId::generate().0
    ));
    let result = (|| {
        let options = CapOpenOptions::new()
            .create_new(true)
            .write(true)
            .to_owned();
        let mut file = directory.open_with(&temporary, &options)?;
        directory.set_permissions(&temporary, Permissions::from_mode(0o600))?;
        file.write_all(content.as_bytes())?;
        flocal::durability::sync_file(&file)?;
        directory.rename(&temporary, &directory, destination)?;
        sync_directory.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = directory.remove_file(&temporary);
    }
    result
}

fn preflight_text_file(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .context("service definition has no parent directory")?;
    ensure_private_service_directory(parent)?;
    let directory = cap_std::fs::Dir::from_std_file(File::open(parent)?);
    validate_private_install_file(
        &directory,
        Path::new(
            path.file_name()
                .context("service definition has no file name")?,
        ),
    )
}

fn validate_private_install_file(directory: &cap_std::fs::Dir, path: &Path) -> Result<()> {
    let metadata = match directory.symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    #[cfg(unix)]
    {
        use cap_std::fs::MetadataExt;
        if !metadata.is_file()
            || metadata.file_type().is_symlink()
            || metadata.uid() != rustix::process::geteuid().as_raw()
            || metadata.mode() & 0o077 != 0
        {
            bail!("service asset is not an owner-only regular file")
        }
    }
    Ok(())
}

fn managed_state_marker_path() -> Result<PathBuf> {
    let home = std::env::var_os("HOME").context("could not determine home directory")?;
    Ok(PathBuf::from(home).join(".config/file.local/managed-state"))
}

fn preflight_managed_state_marker() -> Result<()> {
    preflight_text_file(&managed_state_marker_path()?)
}

fn install_managed_state_marker(state_dir: &Path) -> Result<()> {
    let marker = managed_state_marker_path()?;
    let state_dir = state_dir
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

#[cfg(unix)]
fn validate_trusted_executable_file(file: &File) -> Result<()> {
    use std::os::unix::fs::MetadataExt;

    let metadata = file.metadata()?;
    let uid = rustix::process::geteuid().as_raw();
    if !metadata.is_file()
        || (metadata.uid() != uid && metadata.uid() != 0)
        || metadata.mode() & 0o022 != 0
        || metadata.mode() & 0o111 == 0
    {
        bail!("candidate is not a trusted executable file");
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_trusted_executable(_: &Path) -> Result<()> {
    Ok(())
}

#[cfg(not(unix))]
fn validate_trusted_executable_file(_: &File) -> Result<()> {
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
    let lifecycle = Arc::new(Mutex::new(()));
    let (events_tx, events_rx) = std::sync::mpsc::channel();
    let mut recovery_rx = spawn_daemon_install_recovery(&mut state)?;
    let mut recovery_complete = false;
    let clients = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let shutdown = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGTERM, shutdown.clone())?;
    reconcile_watches(&mut state, &workers, &events_tx)?;
    let mut next_reconcile = std::time::Instant::now() + Duration::from_secs(1);
    let mut shutdown_started = None;
    loop {
        if !recovery_complete {
            match recovery_rx.try_recv() {
                Ok(Ok(())) => {
                    recovery_complete = true;
                }
                Ok(Err(error)) => return Err(error),
                Err(std::sync::mpsc::TryRecvError::Empty) => {}
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    bail!("daemon install recovery stopped unexpectedly")
                }
            }
        }
        while let Ok(event) = events_rx.try_recv() {
            apply_worker_event(&state, &workers, event)?;
        }
        if recovery_complete && std::time::Instant::now() >= next_reconcile {
            let _lifecycle = lifecycle
                .lock()
                .map_err(|_| anyhow::anyhow!("daemon lifecycle state is poisoned"))?;
            if state.unclassified_install_intents()?.is_empty() {
                reconcile_watches(&mut state, &workers, &events_tx)?;
            } else {
                recovery_rx = spawn_daemon_install_recovery(&mut state)?;
                recovery_complete = false;
            }
            next_reconcile = std::time::Instant::now() + Duration::from_secs(1);
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
                let lifecycle = lifecycle.clone();
                let clients = clients.clone();
                std::thread::spawn(move || {
                    if let Ok(mut state) = State::open(state_dir) {
                        let _ = handle_daemon_request(
                            &mut state, &workers, &events, &lifecycle, stream,
                        );
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

fn reconcile_watches(
    state: &mut State,
    workers: &Arc<Mutex<std::collections::HashMap<ShareId, DaemonWorker>>>,
    events: &std::sync::mpsc::Sender<WorkerEvent>,
) -> Result<()> {
    for share in state.managed_shares()? {
        if matches!(
            &share.binding,
            EndpointBinding::Connector(peer) if peer.peer_id.is_some()
        ) && share.initial_complete
            && share.watch_enabled
            && share.blocked_diagnostic.is_none()
            && share.removing_relationship.is_none()
        {
            if workers
                .lock()
                .map_err(|_| anyhow::anyhow!("daemon worker state is poisoned"))?
                .contains_key(&share.id)
            {
                continue;
            }
            let generation = state.watch_intent_generation(&share.id)?;
            let request =
                state.enqueue_sync(Some(&share.id), SyncOperation::Watch, Some(generation))?;
            start_worker(state, workers, events, share.id, Some(request))?;
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
    lifecycle: &Arc<Mutex<()>>,
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
        Ok(DaemonRequest::Start { share }) => {
            let _lifecycle = lifecycle
                .lock()
                .map_err(|_| anyhow::anyhow!("daemon lifecycle state is poisoned"));
            let share = ShareId(share);
            match _lifecycle.and_then(|_| start_managed_share(state, workers, events, share)) {
                Ok(()) => DaemonResponse::Ok,
                Err(error) => DaemonResponse::Error {
                    message: format!("{error:#}"),
                },
            }
        }
        Ok(DaemonRequest::Stop { share }) => {
            let _lifecycle = lifecycle
                .lock()
                .map_err(|_| anyhow::anyhow!("daemon lifecycle state is poisoned"));
            let share = ShareId(share);
            match _lifecycle.and_then(|_| stop_managed_share(state, workers, share)) {
                Ok(()) => DaemonResponse::Ok,
                Err(error) => DaemonResponse::Error {
                    message: format!("{error:#}"),
                },
            }
        }
        Ok(DaemonRequest::PrepareRemove {
            share,
            expected_binding,
        }) => {
            let _lifecycle = lifecycle
                .lock()
                .map_err(|_| anyhow::anyhow!("daemon lifecycle state is poisoned"));
            let share = ShareId(share);
            match _lifecycle
                .and_then(|_| prepare_managed_removal(state, workers, &share, &expected_binding))
            {
                Ok(removal) => DaemonResponse::Prepared { removal },
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

fn prepare_managed_removal(
    state: &mut State,
    workers: &Arc<Mutex<std::collections::HashMap<ShareId, DaemonWorker>>>,
    share: &ShareId,
    expected_binding: &EndpointBinding,
) -> Result<PreparedRemoval> {
    let prepared = state.prepare_removal(share, expected_binding)?;
    let ownership = stop_worker_and_wait(workers, share)
        .and_then(|()| state.lock_share_session(share).map(drop));
    if let Err(error) = ownership {
        let _ = state.set_removal_diagnostic(share, &prepared.relationship, &format!("{error:#}"));
        return Err(error);
    }
    Ok(prepared)
}

fn daemon_syncs(
    state: &mut State,
    workers: &Arc<Mutex<std::collections::HashMap<ShareId, DaemonWorker>>>,
) -> Result<Vec<DaemonSync>> {
    let scheduling_snapshot = state.scheduling_snapshot()?;
    let workers = workers
        .lock()
        .map_err(|_| anyhow::anyhow!("daemon worker state is poisoned"))?;
    let mut syncs = Vec::new();
    for share in state.managed_shares()? {
        let (role, registration_pending) = match &share.binding {
            EndpointBinding::Connector(peer) => ("connector", peer.peer_id.is_none()),
            EndpointBinding::Responder { .. } => ("responder", false),
            EndpointBinding::Unpaired => continue,
        };
        let removal_pending = share.removing_relationship.is_some();
        let connection_state = if removal_pending {
            "removing"
        } else if registration_pending {
            "registering"
        } else if share.blocked_diagnostic.is_some() {
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
        let scheduling = share_scheduling_view(state, &scheduling_snapshot, &share.id)?;
        let state_name = if scheduling.state == "queued" {
            "queued"
        } else {
            connection_state
        };
        syncs.push(DaemonSync {
            share: share.id.0.clone(),
            root: daemon_path(&path_bytes(&share.root)),
            host: match &share.binding {
                EndpointBinding::Connector(peer) => Some(peer.host.clone()),
                EndpointBinding::Responder { .. } | EndpointBinding::Unpaired => None,
            },
            remote_path: match &share.binding {
                EndpointBinding::Connector(peer) => Some(daemon_path(&peer.remote_path)),
                EndpointBinding::Responder { .. } | EndpointBinding::Unpaired => None,
            },
            enabled: share.watch_enabled,
            initial_complete: share.initial_complete,
            state: state_name.into(),
            connection_state: connection_state.into(),
            scheduling: scheduling.state.into(),
            waiting_on: scheduling.blocker.as_ref().map(|blocker| match blocker {
                SchedulingBlocker::Local(_) => "local".to_owned(),
                SchedulingBlocker::Peer => "peer".to_owned(),
            }),
            operation: scheduling.operation,
            queue_position: scheduling.queue_position,
            waiting_root: scheduling
                .blocker
                .as_ref()
                .and_then(|blocker| match blocker {
                    SchedulingBlocker::Local(Some(root)) => Some(daemon_path(&path_bytes(root))),
                    SchedulingBlocker::Local(None) | SchedulingBlocker::Peer => None,
                }),
            active_share: scheduling.active_share.map(|share| share.0),
            active_root: scheduling
                .active_root
                .as_ref()
                .map(|root| daemon_path(&path_bytes(root))),
            active_operation: scheduling.active_operation,
            diagnostic: share.blocked_diagnostic,
            unsettled: state.unsettled_paths(&share.id)?.len(),
            role: role.into(),
            registration_pending,
            removal_pending,
        });
    }
    Ok(syncs)
}

fn daemon_sync_page(
    state: &mut State,
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
) -> Result<()> {
    let managed = state.managed_share(&share)?;
    state.ensure_not_removing(&share)?;
    let EndpointBinding::Connector(peer) = &managed.binding else {
        bail!("this machine is responder-only for the selected share")
    };
    peer.completed_peer_id()?;
    if !managed.initial_complete {
        bail!("initial synchronization is incomplete; rerun `flocal sync start PATH` to review it");
    }
    let retrying_install = state.install_intent_failure(&share)?.is_some();
    if retrying_install {
        stop_worker_and_wait(workers, &share)?;
    } else {
        let mut workers = workers
            .lock()
            .map_err(|_| anyhow::anyhow!("daemon worker state is poisoned"))?;
        if let Some(worker) = workers.get(&share) {
            if worker.finished.load(Ordering::Relaxed) {
                workers.remove(&share);
            } else {
                if worker.stopping.load(Ordering::Relaxed) {
                    bail!(
                        "sync is still stopping; retry once it disappears from `flocal sync list`"
                    );
                }
                return Ok(());
            }
        }
    }
    state.validate_root_identity(&share)?;
    state.begin_install_intent_retry(&share)?;
    state.clear_blocked(&share)?;
    let request = if managed.watch_enabled {
        let generation = state.watch_intent_generation(&share)?;
        state.enqueue_sync(Some(&share), SyncOperation::Watch, Some(generation))?
    } else {
        let generation = state.watch_intent_generation(&share)?;
        state.enable_and_enqueue_managed_sync(&share, generation)?
    };
    start_worker(state, workers, events, share, Some(request))
}

fn stop_managed_share(
    state: &mut State,
    workers: &Arc<Mutex<std::collections::HashMap<ShareId, DaemonWorker>>>,
    share: ShareId,
) -> Result<()> {
    state.managed_share(&share)?;
    state.stop_and_cancel_managed_sync(&share)?;
    state.clear_blocked(&share)?;
    stop_worker_and_wait(workers, &share)
}

fn stop_worker_and_wait(
    workers: &Arc<Mutex<std::collections::HashMap<ShareId, DaemonWorker>>>,
    share: &ShareId,
) -> Result<()> {
    let worker = {
        let workers = workers
            .lock()
            .map_err(|_| anyhow::anyhow!("daemon worker state is poisoned"))?;
        workers.get(share).map(|worker| {
            worker.stopping.store(true, Ordering::Relaxed);
            worker.stop.store(true, Ordering::Relaxed);
            (worker.id, worker.finished.clone(), worker.child.clone())
        })
    };
    if let Some((worker_id, finished, child)) = worker {
        let deadline = std::time::Instant::now() + Duration::from_secs(9);
        while !finished.load(Ordering::Relaxed) && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(25));
        }
        if !finished.load(Ordering::Relaxed) {
            let mut child = child
                .lock()
                .map_err(|_| anyhow::anyhow!("daemon worker child state is poisoned"))?;
            if let Some(mut child) = child.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
            drop(child);
            let deadline = std::time::Instant::now() + Duration::from_secs(1);
            while !finished.load(Ordering::Relaxed) && std::time::Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(25));
            }
            if !finished.load(Ordering::Relaxed) {
                bail!("sync stop could not end the active worker; it remains disabled");
            }
        }
        let mut workers = workers
            .lock()
            .map_err(|_| anyhow::anyhow!("daemon worker state is poisoned"))?;
        if workers
            .get(share)
            .is_some_and(|worker| worker.id == worker_id)
        {
            workers.remove(share);
        }
    }
    Ok(())
}

fn start_worker(
    state: &State,
    workers: &Arc<Mutex<std::collections::HashMap<ShareId, DaemonWorker>>>,
    events: &std::sync::mpsc::Sender<WorkerEvent>,
    share: ShareId,
    initial_request: Option<flocal::state::QueueRequest>,
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
        let result = run_daemon_worker(&state_dir, &share, &stop, &live, child, initial_request);
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
    mut initial_request: Option<flocal::state::QueueRequest>,
) -> Result<()> {
    let mut state = State::open(state_dir)?;
    if !state.managed_share(share)?.watch_enabled {
        return Ok(());
    }
    state.validate_root_identity(share)?;
    let root = state.root_for(share)?;
    let _session_lock = state.lock_share_session(share)?;
    state.ensure_not_removing(share)?;
    #[cfg(feature = "e2e-test-hooks")]
    e2e_stop_before_reservation(&state)?;
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
        &mut initial_request,
    )
}

#[cfg(feature = "e2e-test-hooks")]
fn e2e_stop_before_reservation(state: &State) -> Result<()> {
    if !e2e_claim_reservation_stop(state)? {
        return Ok(());
    }
    e2e_publish_reservation_stop_pid(state)?;
    signal_hook::low_level::raise(signal_hook::consts::SIGSTOP)?;
    Ok(())
}

#[cfg(feature = "e2e-test-hooks")]
fn e2e_claim_reservation_stop(state: &State) -> Result<bool> {
    let marker = state.dir.join(".e2e-stop-before-reservation");
    let claimed = state.dir.join(".e2e-stop-before-reservation-claimed");
    match std::fs::rename(&marker, &claimed) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error).context("claiming E2E reservation stop marker"),
    }
    let metadata = std::fs::symlink_metadata(&claimed)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        bail!("E2E reservation stop marker is not a regular file");
    }
    std::fs::remove_file(&claimed)?;
    Ok(true)
}

#[cfg(feature = "e2e-test-hooks")]
fn e2e_publish_reservation_stop_pid(state: &State) -> Result<()> {
    let pidfile = state.dir.join(".e2e-reservation-stop.pid");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(pidfile)
        .context("publishing E2E stopped reservation pid")?;
    write!(file, "{}", std::process::id())?;
    file.sync_all()?;
    Ok(())
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

fn recover_installs_locked(state: &mut State) -> Result<()> {
    sync::recover_installs_locked(state)
}

fn recover_daemon_installs_request(request: QueueRequest) -> Result<()> {
    request
        .wait_with_prepare(|| false, |_| Ok(()), recover_installs_locked)?
        .finish()
}

#[cfg(feature = "e2e-test-hooks")]
fn e2e_hold_installation(state: &mut State, path: &Path) -> Result<()> {
    let (share, _) = state.find_share(path)?;
    let permit = state
        .enqueue_sync(Some(&share), SyncOperation::Maintenance, None)?
        .wait(|| false, |_| Ok(()))?;
    signal_hook::low_level::raise(signal_hook::consts::SIGSTOP)?;
    permit.finish()
}

fn spawn_daemon_install_recovery(
    state: &mut State,
) -> Result<std::sync::mpsc::Receiver<Result<()>>> {
    let request = state.enqueue_sync(None, SyncOperation::Recovery, None)?;
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let _ = sender.send(recover_daemon_installs_request(request));
    });
    Ok(receiver)
}

fn recover_daemon_share_install(state: &mut State, share: &ShareId) -> Result<()> {
    state.begin_install_intent_retry(share)?;
    let request = state.enqueue_sync(Some(share), SyncOperation::Recovery, None)?;
    request
        .wait_with_prepare(
            || false,
            |_| Ok(()),
            |permit_state| {
                recover_installs_locked(permit_state)?;
                ensure_install_recovery_ready(permit_state, share)
            },
        )?
        .finish()
}

fn ensure_install_recovery_ready(state: &State, share: &ShareId) -> Result<()> {
    if let Some(failure) = state.install_intent_failure(share)? {
        return Err(InstallRecoveryBlocked(failure.diagnostic).into());
    }
    Ok(())
}

#[derive(Debug)]
struct InstallRecoveryBlocked(String);

impl std::fmt::Display for InstallRecoveryBlocked {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "install recovery is blocked: {}",
            escaped(&self.0)
        )
    }
}

impl std::error::Error for InstallRecoveryBlocked {}

fn add_peer(state: &mut State, path: &Path, host: &str, remote_path: &Path) -> Result<()> {
    validate_host(host)?;
    if !remote_path.is_absolute() {
        bail!("--remote-path must be absolute");
    }
    let (share, _) = state.find_share(path)?;
    let executable = discover_executable(host)?;
    let expected = state.endpoint_binding(&share)?;
    let registration = wait_for_installation(state, &share, SyncOperation::Registration, None)?;
    let prepared = state.prepare_connector_registration_locked(
        &share,
        &expected,
        host,
        &path_bytes(remote_path),
        &executable,
    )?;
    registration.finish()?;
    let relationship = prepared
        .relationship
        .clone()
        .context("prepared connector is missing its relationship identity")?;
    let remote = match RelationshipRemote::spawn(
        &prepared.host,
        &prepared.executable,
        Duration::from_secs(30),
    ) {
        Ok(remote) => remote,
        Err(error) => {
            let _ = state.set_blocked(&share, &format!("{error:#}"));
            bail!(
                "connector registration remains prepared; rerun `flocal sync add` after correcting the SSH launch failure: {error:#}"
            )
        }
    };
    let request = RelationshipRequest::RegisterRelationship {
        registration_protocol: sync::RELATIONSHIP_REGISTRATION_PROTOCOL_VERSION,
        share: share.clone(),
        peer: state.peer_id()?,
        root: prepared.remote_path.clone(),
        relationship: relationship.clone(),
    };
    let exchange = (|| -> Result<RegisterRelationshipResponse> {
        sync::write_relationship_request_until(
            remote.input.as_ref().context("ssh stdin unavailable")?,
            &request,
            remote.deadline,
        )?;
        sync::read_register_relationship_response_until(
            remote.output.as_ref().context("ssh stdout unavailable")?,
            remote.deadline,
        )
    })();
    let response = match exchange {
        Ok(response) => response,
        Err(error) => {
            let error = remote.finish_after_error(error);
            let _ = state.set_blocked(&share, &format!("{error:#}"));
            bail!(
                "connector registration remains prepared; ensure both peers are upgraded and rerun `flocal sync add`: {error:#}"
            )
        }
    };
    let (peer_id, prior_share) = match response {
        RegisterRelationshipResponse::Registered {
            registration_protocol,
            share: accepted_share,
            peer,
            relationship: accepted_relationship,
            prior_share,
        } if registration_protocol == sync::RELATIONSHIP_REGISTRATION_PROTOCOL_VERSION
            && accepted_share == share
            && accepted_relationship == relationship =>
        {
            (peer, prior_share)
        }
        RegisterRelationshipResponse::Error { message } => {
            remote.finish()?;
            state.set_blocked(&share, &message)?;
            bail!("remote rejected pairing: {}", escaped(&message))
        }
        _ => {
            return Err(remote.finish_after_error(anyhow::anyhow!(
                "remote returned a mismatched relationship-registration response"
            )));
        }
    };
    remote.finish()?;
    let registration = wait_for_installation(state, &share, SyncOperation::Registration, None)?;
    state.complete_connector_registration_locked(&share, &prepared, &peer_id)?;
    registration.finish()?;
    state.clear_blocked(&share)?;
    println!(
        "Connected {} to {}:{}",
        escaped(&share.0),
        escaped(&prepared.host),
        escaped(&bytes_path(&prepared.remote_path).to_string_lossy())
    );
    if let Some(prior_share) = prior_share {
        println!(
            "Responder remapped retained root from {} to {}.",
            escaped(&prior_share.0),
            escaped(&share.0)
        );
    }
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
        println!(
            "{}:{}{}",
            escaped(&peer.host),
            escaped(&bytes_path(&peer.remote_path).to_string_lossy()),
            if peer.peer_id.is_none() {
                " (registration pending)"
            } else {
                ""
            }
        );
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
    initial_applied: bool,
}

fn wait_for_installation(
    state: &mut State,
    share: &ShareId,
    operation: SyncOperation,
    generation: Option<i64>,
) -> Result<InstallationPermit> {
    let root = state.root_for(share)?;
    let permit = wait_for_installation_request(state, Some(share), operation, generation)?;
    if permit.1 {
        watch_log(
            &mut io::stdout(),
            &format!(
                "Synchronization slot acquired for {}",
                escaped(&root.to_string_lossy())
            ),
        )?;
    }
    Ok(permit.0)
}

fn wait_for_installation_request(
    state: &mut State,
    share: Option<&ShareId>,
    operation: SyncOperation,
    generation: Option<i64>,
) -> Result<(InstallationPermit, bool)> {
    let request = state.enqueue_sync(share, operation, generation)?;
    let mut waited = false;
    let mut last_position = None;
    let permit = request.wait_with_prepare(
        || false,
        |position| {
            waited = true;
            if last_position.as_ref() != Some(&position) {
                report_installation_wait(state, &position, &mut io::stdout())?;
                last_position = Some(position);
            }
            Ok(())
        },
        |permit_state| {
            recover_installs_locked(permit_state)?;
            if let Some(share) = share {
                ensure_install_recovery_ready(permit_state, share)?;
            }
            Ok(())
        },
    )?;
    Ok((permit, waited))
}

fn report_installation_wait(
    state: &State,
    position: &QueuePosition,
    output: &mut impl Write,
) -> Result<()> {
    let message = match position
        .active
        .as_ref()
        .and_then(|active| active.share.as_ref())
    {
        Some(active) => {
            let root = state.root_for(active)?;
            format!(
                "Waiting for {} sync to finish (queue position {})",
                escaped(&root.to_string_lossy()),
                position.position
            )
        }
        None => format!(
            "Waiting for the installation synchronization slot (queue position {})",
            position.position
        ),
    };
    watch_log(output, &message)
}

struct ValidatedSyncBinding {
    remote_peer: PeerId,
    relationship: RelationshipId,
    order: std::cmp::Ordering,
}

fn legacy_relationship_id(share: &ShareId) -> Result<RelationshipId> {
    const DOMAIN: &[u8] = b"file.local legacy relationship v1";
    let mut hash = blake3::Hasher::new();
    hash.update(DOMAIN);
    hash.update(share.0.as_bytes());
    RelationshipId::parse(format!(
        "{}{}",
        sync::LEGACY_RELATIONSHIP_PREFIX,
        hash.finalize().to_hex()
    ))
}

fn explicit_sync_relationship(relationship: &RelationshipId) -> Result<&RelationshipId> {
    if relationship.0.starts_with(sync::LEGACY_RELATIONSHIP_PREFIX) {
        bail!("stored relationship ID uses the reserved legacy namespace");
    }
    Ok(relationship)
}

fn validate_sync_binding(
    state: &State,
    share: &ShareId,
    remote_peer: &PeerId,
    relationship: &RelationshipId,
) -> Result<ValidatedSyncBinding> {
    let managed = state
        .managed_share(share)
        .context("peer identity mismatch")?;
    if managed.removing_relationship.is_some() {
        bail!("relationship removal is pending");
    }
    let matches = match &managed.binding {
        EndpointBinding::Connector(peer) if peer.peer_id.as_ref() == Some(remote_peer) => {
            match &peer.relationship {
                Some(stored) => explicit_sync_relationship(stored)? == relationship,
                None => legacy_relationship_id(share)? == *relationship,
            }
        }
        EndpointBinding::Responder {
            peer,
            relationship: bound_relationship,
        } if peer == remote_peer => match bound_relationship {
            Some(stored) => explicit_sync_relationship(stored)? == relationship,
            None => legacy_relationship_id(share)? == *relationship,
        },
        EndpointBinding::Connector(_) | EndpointBinding::Responder { .. } => false,
        EndpointBinding::Unpaired => false,
    };
    if !matches {
        bail!("peer or relationship binding does not match");
    }
    state.validate_root_identity(share)?;
    let local_peer = state.peer_id()?;
    let order = sync::peer_order(&local_peer, remote_peer)?;
    Ok(ValidatedSyncBinding {
        remote_peer: remote_peer.clone(),
        relationship: relationship.clone(),
        order,
    })
}

fn connector_sync_binding(
    state: &State,
    share: &ShareId,
    peer: &PeerConfig,
) -> Result<ValidatedSyncBinding> {
    let remote = peer.completed_peer_id()?;
    let relationship = match &peer.relationship {
        Some(relationship) => relationship.clone(),
        None => legacy_relationship_id(share)?,
    };
    validate_sync_binding(state, share, remote, &relationship)
}

#[derive(Debug)]
enum ReservationFrame {
    PendingAuthority(sync::PendingAuthority),
    ProxyIssue(sync::ProxyIssue),
    ProxyAck(sync::ProxyAck),
    Queued(sync::SyncQueued),
    PairPrepare(sync::PairCheckpoint),
    PairPrepareAck(sync::PairCheckpoint),
    PairReset(sync::PairCheckpoint),
    PairResetAck(sync::PairCheckpoint),
    PairCommit(sync::PairCheckpoint),
    PairCommitAck(sync::PairCheckpoint),
    SyncReserved(sync::Reservation),
    SyncStart(sync::SyncStart),
    SyncAccepted(sync::Reservation),
}

trait ReservationWire {
    fn send_reservation(
        &mut self,
        frame: ReservationFrame,
        deadline: std::time::Instant,
    ) -> Result<()>;
    fn recv_reservation(&mut self, deadline: std::time::Instant) -> Result<ReservationFrame>;
}

fn reservation_from_v1(frame: Message) -> Result<ReservationFrame> {
    match frame {
        Message::PendingAuthority(value) => Ok(ReservationFrame::PendingAuthority(value)),
        Message::ProxyIssue(value) => Ok(ReservationFrame::ProxyIssue(value)),
        Message::ProxyAck(value) => Ok(ReservationFrame::ProxyAck(value)),
        Message::SyncQueued(value) => Ok(ReservationFrame::Queued(value)),
        Message::PairPrepare(value) => Ok(ReservationFrame::PairPrepare(value)),
        Message::PairPrepareAck(value) => Ok(ReservationFrame::PairPrepareAck(value)),
        Message::PairReset(value) => Ok(ReservationFrame::PairReset(value)),
        Message::PairResetAck(value) => Ok(ReservationFrame::PairResetAck(value)),
        Message::PairCommit(value) => Ok(ReservationFrame::PairCommit(value)),
        Message::PairCommitAck(value) => Ok(ReservationFrame::PairCommitAck(value)),
        Message::SyncReserved(value) => Ok(ReservationFrame::SyncReserved(value)),
        Message::SyncStart(value) => Ok(ReservationFrame::SyncStart(value)),
        Message::SyncAccepted(value) => Ok(ReservationFrame::SyncAccepted(value)),
        Message::Error { message } => bail!("remote rejected sync: {}", escaped(&message)),
        other => bail!("unexpected synchronization reservation frame: {other:?}"),
    }
}

fn reservation_into_v1(frame: ReservationFrame) -> Message {
    match frame {
        ReservationFrame::PendingAuthority(value) => Message::PendingAuthority(value),
        ReservationFrame::ProxyIssue(value) => Message::ProxyIssue(value),
        ReservationFrame::ProxyAck(value) => Message::ProxyAck(value),
        ReservationFrame::Queued(value) => Message::SyncQueued(value),
        ReservationFrame::PairPrepare(value) => Message::PairPrepare(value),
        ReservationFrame::PairPrepareAck(value) => Message::PairPrepareAck(value),
        ReservationFrame::PairReset(value) => Message::PairReset(value),
        ReservationFrame::PairResetAck(value) => Message::PairResetAck(value),
        ReservationFrame::PairCommit(value) => Message::PairCommit(value),
        ReservationFrame::PairCommitAck(value) => Message::PairCommitAck(value),
        ReservationFrame::SyncReserved(value) => Message::SyncReserved(value),
        ReservationFrame::SyncStart(value) => Message::SyncStart(value),
        ReservationFrame::SyncAccepted(value) => Message::SyncAccepted(value),
    }
}

struct V1ReservationWire<'a, I, O> {
    input: &'a I,
    output: &'a O,
}

impl<I: AsFd, O: AsFd> ReservationWire for V1ReservationWire<'_, I, O> {
    fn send_reservation(
        &mut self,
        frame: ReservationFrame,
        deadline: std::time::Instant,
    ) -> Result<()> {
        sync::write_v1_message_until(self.output, &reservation_into_v1(frame), deadline)
    }

    fn recv_reservation(&mut self, deadline: std::time::Instant) -> Result<ReservationFrame> {
        reservation_from_v1(sync::read_v1_message_until(self.input, deadline)?)
    }
}

fn reservation_from_v2(frame: V2RoundFrame) -> Result<ReservationFrame> {
    match frame {
        V2RoundFrame::PendingAuthority(value) => Ok(ReservationFrame::PendingAuthority(value)),
        V2RoundFrame::ProxyIssue(value) => Ok(ReservationFrame::ProxyIssue(value)),
        V2RoundFrame::ProxyAck(value) => Ok(ReservationFrame::ProxyAck(value)),
        V2RoundFrame::SyncQueued(value) => Ok(ReservationFrame::Queued(value)),
        V2RoundFrame::PairPrepare(value) => Ok(ReservationFrame::PairPrepare(value)),
        V2RoundFrame::PairPrepareAck(value) => Ok(ReservationFrame::PairPrepareAck(value)),
        V2RoundFrame::PairReset(value) => Ok(ReservationFrame::PairReset(value)),
        V2RoundFrame::PairResetAck(value) => Ok(ReservationFrame::PairResetAck(value)),
        V2RoundFrame::PairCommit(value) => Ok(ReservationFrame::PairCommit(value)),
        V2RoundFrame::PairCommitAck(value) => Ok(ReservationFrame::PairCommitAck(value)),
        V2RoundFrame::SyncReserved(value) => Ok(ReservationFrame::SyncReserved(value)),
        V2RoundFrame::SyncStart(value) => Ok(ReservationFrame::SyncStart(value)),
        V2RoundFrame::SyncAccepted(value) => Ok(ReservationFrame::SyncAccepted(value)),
        V2RoundFrame::SyncFailed { retryable, message } => {
            Err(RemoteWatchError { retryable, message }.into())
        }
        other => bail!("unexpected persistent reservation frame: {other:?}"),
    }
}

fn reservation_into_v2(frame: ReservationFrame) -> V2RoundFrame {
    match frame {
        ReservationFrame::PendingAuthority(value) => V2RoundFrame::PendingAuthority(value),
        ReservationFrame::ProxyIssue(value) => V2RoundFrame::ProxyIssue(value),
        ReservationFrame::ProxyAck(value) => V2RoundFrame::ProxyAck(value),
        ReservationFrame::Queued(value) => V2RoundFrame::SyncQueued(value),
        ReservationFrame::PairPrepare(value) => V2RoundFrame::PairPrepare(value),
        ReservationFrame::PairPrepareAck(value) => V2RoundFrame::PairPrepareAck(value),
        ReservationFrame::PairReset(value) => V2RoundFrame::PairReset(value),
        ReservationFrame::PairResetAck(value) => V2RoundFrame::PairResetAck(value),
        ReservationFrame::PairCommit(value) => V2RoundFrame::PairCommit(value),
        ReservationFrame::PairCommitAck(value) => V2RoundFrame::PairCommitAck(value),
        ReservationFrame::SyncReserved(value) => V2RoundFrame::SyncReserved(value),
        ReservationFrame::SyncStart(value) => V2RoundFrame::SyncStart(value),
        ReservationFrame::SyncAccepted(value) => V2RoundFrame::SyncAccepted(value),
    }
}

struct V2ReservationWire<'a, I, O> {
    round: u64,
    input: &'a I,
    output: &'a O,
    pending_remote_generation: Option<&'a mut u64>,
    prefetched: Option<ReservationFrame>,
}

impl<I: AsFd, O: AsFd> ReservationWire for V2ReservationWire<'_, I, O> {
    fn send_reservation(
        &mut self,
        frame: ReservationFrame,
        deadline: std::time::Instant,
    ) -> Result<()> {
        sync::write_v2_envelope_until(
            self.output,
            &V2Envelope::Round {
                round: self.round,
                frame: reservation_into_v2(frame),
            },
            deadline,
        )
    }

    fn recv_reservation(&mut self, deadline: std::time::Instant) -> Result<ReservationFrame> {
        if let Some(frame) = self.prefetched.take() {
            return Ok(frame);
        }
        loop {
            match sync::read_v2_envelope_until(self.input, deadline)? {
                V2Envelope::Round { round, frame } if round == self.round => {
                    return reservation_from_v2(frame);
                }
                V2Envelope::Session {
                    frame: V2SessionFrame::Changed { generation },
                } if self.pending_remote_generation.is_some() => {
                    let pending = self
                        .pending_remote_generation
                        .as_deref_mut()
                        .expect("checked above");
                    *pending = (*pending).max(generation);
                }
                V2Envelope::Session {
                    frame: V2SessionFrame::Pong { .. },
                } if self.pending_remote_generation.is_some() => {}
                V2Envelope::Session {
                    frame: V2SessionFrame::Error { retryable, message },
                } => return Err(RemoteWatchError { retryable, message }.into()),
                other => {
                    watch_protocol_bail!("unexpected persistent reservation envelope: {other:?}")
                }
            }
        }
    }
}

fn empty_predecessor() -> sync::PredecessorFingerprint {
    sync::PredecessorFingerprint::from_blake3(blake3::hash(&[]))
}

fn scheduling_deadline() -> std::time::Instant {
    std::time::Instant::now() + sync::default_frame_deadline()
}

const MAX_PAIR_RESETS: usize = 8;

fn note_pair_reset(pair_resets: &mut usize) -> Result<()> {
    *pair_resets += 1;
    if *pair_resets > MAX_PAIR_RESETS {
        bail!("peer synchronization reservation changed too many times");
    }
    Ok(())
}

fn relationship_retry_ns() -> i64 {
    let retry = std::time::SystemTime::now()
        .checked_add(sync::reservation_lease())
        .unwrap_or(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(i64::MAX as u64));
    retry
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(i64::MAX as u128) as i64
}

fn report_peer_queue(queued: sync::SyncQueued, output: &mut impl Write) -> Result<()> {
    watch_log(
        output,
        &format!(
            "Waiting for the peer to finish another synchronization (queue position {})",
            queued.position.get()
        ),
    )
}

fn recv_reservation_ignoring_progress(
    wire: &mut impl ReservationWire,
    deadline: Option<std::time::Instant>,
    output: &mut impl Write,
) -> Result<ReservationFrame> {
    let mut progress_window = std::time::Instant::now();
    let mut progress_frames = 0u8;
    let mut last_report = None;
    let mut last_reported_at = None;
    loop {
        let frame = wire.recv_reservation(deadline.unwrap_or_else(scheduling_deadline))?;
        match frame {
            ReservationFrame::Queued(queued) => {
                let now = std::time::Instant::now();
                if now.duration_since(progress_window) >= Duration::from_secs(1) {
                    progress_window = now;
                    progress_frames = 0;
                }
                progress_frames = progress_frames.saturating_add(1);
                if progress_frames > 8 {
                    bail!("peer sent synchronization progress too frequently");
                }
                let changed = last_report != Some(queued);
                let report_allowed = last_reported_at
                    .is_none_or(|last| now.duration_since(last) >= Duration::from_secs(10));
                if changed && report_allowed {
                    report_peer_queue(queued, output)?;
                    last_report = Some(queued);
                    last_reported_at = Some(now);
                }
            }
            frame => return Ok(frame),
        }
    }
}

fn send_local_queue_position(
    state: &mut State,
    request: &QueueRequest,
    wire: &mut impl ReservationWire,
    deadline: std::time::Instant,
) -> Result<()> {
    let position = state.paired_queue_position(request.token())?;
    wire.send_reservation(
        ReservationFrame::Queued(sync::SyncQueued {
            waiting_on: sync::WaitingOn::Local,
            position: sync::CoarseQueuePosition::from_exact(position.position.max(1))?,
        }),
        deadline,
    )
}

struct LocalPredecessor {
    stored: String,
    wire: sync::PredecessorFingerprint,
}

fn prepare_local_pair(
    state: &mut State,
    request: &QueueRequest,
    relationship: &RelationshipId,
    network_authority: &PeerId,
    order: sync::NetworkOrder,
    nonce: &sync::SchedulingNonce,
    wire: &mut impl ReservationWire,
) -> Result<LocalPredecessor> {
    let mut next_progress = std::time::Instant::now();
    loop {
        if let Some(predecessor) = state.prepare_paired_sync(
            request.token(),
            relationship,
            network_authority,
            order.get() as i64,
            nonce.as_str(),
        )? {
            let hash = predecessor
                .parse::<blake3::Hash>()
                .context("stored predecessor fingerprint is invalid")?;
            return Ok(LocalPredecessor {
                stored: predecessor,
                wire: sync::PredecessorFingerprint::from_blake3(hash),
            });
        }
        if std::time::Instant::now() >= next_progress {
            send_local_queue_position(state, request, wire, scheduling_deadline())?;
            next_progress = std::time::Instant::now() + Duration::from_secs(10);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn wait_for_paired_permit(
    request: QueueRequest,
    share: &ShareId,
    wire: &mut impl ReservationWire,
    lease_deadline: Option<std::time::Instant>,
) -> Result<InstallationPermit> {
    request.wait_with_prepare(
        || lease_deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline),
        |position| {
            wire.send_reservation(
                ReservationFrame::Queued(sync::SyncQueued {
                    waiting_on: sync::WaitingOn::Local,
                    position: sync::CoarseQueuePosition::from_exact(position.position.max(1))?,
                }),
                lease_deadline.unwrap_or_else(scheduling_deadline),
            )
        },
        |permit_state| ensure_install_recovery_ready(permit_state, share),
    )
}

fn recover_before_pair(state: &mut State, wire: &mut impl ReservationWire) -> Result<()> {
    recover_before_pair_with_interval(state, wire, Duration::from_secs(10))
}

fn recover_before_pair_with_interval(
    state: &mut State,
    wire: &mut impl ReservationWire,
    progress_interval: Duration,
) -> Result<()> {
    let request = state.enqueue_sync(None, SyncOperation::Recovery, None)?;
    let (completed_tx, completed_rx) = std::sync::mpsc::sync_channel(1);
    let canceled = Arc::new(AtomicBool::new(false));
    let recovery_canceled = canceled.clone();
    let recovery = std::thread::spawn(move || {
        let result = request
            .wait_with_prepare(
                || recovery_canceled.load(Ordering::Relaxed),
                |_| Ok(()),
                recover_installs_locked,
            )?
            .finish();
        let _ = completed_tx.send(());
        result
    });
    loop {
        match completed_rx.recv_timeout(progress_interval) {
            Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                return recovery
                    .join()
                    .map_err(|_| anyhow::anyhow!("install recovery thread panicked"))?;
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
        }
        if let Err(wire_error) = wire.send_reservation(
            ReservationFrame::Queued(sync::SyncQueued {
                waiting_on: sync::WaitingOn::Local,
                position: sync::CoarseQueuePosition::from_exact(1)?,
            }),
            scheduling_deadline(),
        ) {
            canceled.store(true, Ordering::Relaxed);
            let _ = recovery.join();
            return Err(wire_error);
        }
    }
}

fn validate_final_sync_binding(
    state: &State,
    share: &ShareId,
    binding: &ValidatedSyncBinding,
    expected_intent_generation: Option<i64>,
) -> Result<()> {
    validate_sync_binding(state, share, &binding.remote_peer, &binding.relationship)?;
    if let Some(generation) = expected_intent_generation
        && state.watch_intent_generation(share)? != generation
    {
        bail!("synchronization intent changed while reserving both installations");
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn reserve_as_authority(
    state: &mut State,
    share: &ShareId,
    binding: &ValidatedSyncBinding,
    operation: SyncOperation,
    intent_generation: Option<i64>,
    connector_generation: u64,
    responder_generation: u64,
    id: sync::SchedulingId,
    existing: Option<QueueRequest>,
    wire: &mut impl ReservationWire,
    watch_output: &mut impl Write,
) -> Result<(InstallationPermit, std::fs::File, sync::Reservation)> {
    let relationship = &binding.relationship;
    let nonce = sync::SchedulingNonce::generate();
    let placeholder = empty_predecessor();
    let network_authority = state.peer_id()?;
    let (request, network_order) = match existing {
        Some(request) => {
            let order = state
                .convert_managed_to_authoritative_parked(
                    request.token(),
                    share,
                    relationship,
                    operation,
                    intent_generation,
                    &network_authority,
                    nonce.as_str(),
                    placeholder.as_str(),
                )?
                .context("managed synchronization changed before authority reservation")?;
            (request, order)
        }
        None => state.enqueue_authoritative_sync(
            share,
            relationship,
            operation,
            intent_generation,
            &network_authority,
            nonce.as_str(),
            placeholder.as_str(),
        )?,
    };
    let network_order = sync::NetworkOrder::new(network_order as u64)?;
    let mut next_progress = std::time::Instant::now();
    loop {
        let next = state
            .next_unacknowledged_proxy()?
            .context("authoritative proxy issue disappeared before publication")?;
        if next.token == request.token() {
            break;
        }
        if std::time::Instant::now() >= next_progress {
            send_local_queue_position(state, &request, wire, scheduling_deadline())?;
            next_progress = std::time::Instant::now() + Duration::from_secs(10);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let issue = sync::ProxyIssue {
        id: id.clone(),
        network_order,
        nonce: nonce.clone(),
        connector_generation,
        responder_generation,
    };
    wire.send_reservation(
        ReservationFrame::ProxyIssue(issue.clone()),
        scheduling_deadline(),
    )?;
    match recv_reservation_ignoring_progress(wire, None, watch_output)? {
        ReservationFrame::ProxyAck(ack) if ack.id == id && ack.network_order == network_order => {}
        other => bail!("expected exact proxy acknowledgment, got {other:?}"),
    }
    if !state.acknowledge_proxy_issue(
        request.token(),
        relationship,
        &network_authority,
        network_order.get() as i64,
    )? {
        bail!("authoritative synchronization changed before proxy acknowledgment");
    }

    let mut pair_resets = 0;
    let local_checkpoint = loop {
        let local_predecessor = prepare_local_pair(
            state,
            &request,
            relationship,
            &network_authority,
            network_order,
            &nonce,
            wire,
        )?;
        let local_checkpoint = sync::PairCheckpoint {
            id: id.clone(),
            network_order,
            nonce: nonce.clone(),
            predecessor: local_predecessor.wire.clone(),
        };
        wire.send_reservation(
            ReservationFrame::PairPrepare(local_checkpoint.clone()),
            scheduling_deadline(),
        )?;
        match recv_reservation_ignoring_progress(wire, None, watch_output)? {
            ReservationFrame::PairPrepareAck(remote)
                if remote.id == id
                    && remote.network_order == network_order
                    && remote.nonce == nonce => {}
            other => bail!("expected exact pair prepare acknowledgment, got {other:?}"),
        }
        recover_before_pair(state, wire)?;
        if state.commit_paired_sync(
            request.token(),
            relationship,
            &network_authority,
            network_order.get() as i64,
            nonce.as_str(),
            &local_predecessor.stored,
        )? {
            break local_checkpoint;
        }
        note_pair_reset(&mut pair_resets)?;
        wire.send_reservation(
            ReservationFrame::PairReset(local_checkpoint.clone()),
            scheduling_deadline(),
        )?;
        match recv_reservation_ignoring_progress(wire, None, watch_output)? {
            ReservationFrame::PairResetAck(remote) if remote == local_checkpoint => {}
            other => bail!("expected exact pair reset acknowledgment, got {other:?}"),
        }
    };

    let committed = (|| -> Result<(InstallationPermit, std::fs::File, sync::Reservation)> {
        let lease_deadline = std::time::Instant::now() + sync::reservation_lease();
        wire.send_reservation(
            ReservationFrame::PairCommit(local_checkpoint),
            lease_deadline,
        )?;
        match recv_reservation_ignoring_progress(wire, Some(lease_deadline), watch_output)? {
            ReservationFrame::PairCommitAck(remote)
                if remote.id == id
                    && remote.network_order == network_order
                    && remote.nonce == nonce => {}
            other => bail!("expected exact pair commit acknowledgment, got {other:?}"),
        }
        let installation = wait_for_paired_permit(request, share, wire, Some(lease_deadline))?;
        let reservation = sync::Reservation {
            id: id.clone(),
            network_order,
            nonce: nonce.clone(),
        };
        wire.send_reservation(
            ReservationFrame::SyncReserved(reservation.clone()),
            lease_deadline,
        )?;
        let start =
            match recv_reservation_ignoring_progress(wire, Some(lease_deadline), watch_output)? {
                ReservationFrame::SyncStart(start) => start,
                other => bail!("expected exact synchronization start, got {other:?}"),
            };
        if start.reservation() != reservation
            || start.connector_generation != connector_generation
            || start.responder_generation != responder_generation
        {
            bail!("peer synchronization start does not match the committed reservation");
        }
        let share_lock = state.lock_share(share)?;
        validate_final_sync_binding(state, share, binding, intent_generation)?;
        wire.send_reservation(
            ReservationFrame::SyncAccepted(reservation.clone()),
            lease_deadline,
        )?;
        Ok((installation, share_lock, reservation))
    })();
    if committed.is_err() {
        state.record_relationship_yield(relationship, relationship_retry_ns())?;
    }
    committed
}

#[allow(clippy::too_many_arguments)]
fn reserve_as_higher_peer(
    state: &mut State,
    share: &ShareId,
    binding: &ValidatedSyncBinding,
    operation: SyncOperation,
    intent_generation: Option<i64>,
    connector_generation: u64,
    responder_generation: u64,
    connector_origin: bool,
    existing: Option<QueueRequest>,
    wire: &mut impl ReservationWire,
    watch_output: &mut impl Write,
) -> Result<(InstallationPermit, std::fs::File, sync::Reservation)> {
    let remote_peer = &binding.remote_peer;
    let relationship = &binding.relationship;
    let id = sync::SchedulingId::generate();
    let mut pending = if connector_origin {
        let request = match existing {
            Some(request) => {
                if !state.convert_managed_to_pending_authority(
                    request.token(),
                    share,
                    relationship,
                    operation,
                    intent_generation,
                )? {
                    bail!("managed synchronization changed before authority submission");
                }
                request
            }
            None => state.enqueue_pending_authority(
                share,
                relationship,
                operation,
                intent_generation,
            )?,
        };
        wire.send_reservation(
            ReservationFrame::PendingAuthority(sync::PendingAuthority {
                id: id.clone(),
                connector_generation,
                responder_generation,
            }),
            scheduling_deadline(),
        )?;
        Some(request)
    } else {
        None
    };
    let issue = match recv_reservation_ignoring_progress(wire, None, watch_output)? {
        ReservationFrame::ProxyIssue(issue) if !connector_origin || issue.id == id => issue,
        other => bail!("expected authoritative proxy issue, got {other:?}"),
    };
    if issue.connector_generation != connector_generation
        || issue.responder_generation != responder_generation
    {
        bail!("proxy issue generation does not match the submitted synchronization");
    }
    let placeholder = empty_predecessor();
    let request = if let Some(request) = pending.take() {
        if !state.convert_pending_authority_to_parked(
            request.token(),
            share,
            relationship,
            operation,
            intent_generation,
            remote_peer,
            issue.network_order.get() as i64,
            issue.nonce.as_str(),
            placeholder.as_str(),
        )? {
            bail!("pending synchronization changed before proxy issue");
        }
        request
    } else {
        state.enqueue_parked_proxy(
            share,
            relationship,
            operation,
            intent_generation,
            remote_peer,
            issue.network_order.get() as i64,
            issue.nonce.as_str(),
            placeholder.as_str(),
        )?
    };
    wire.send_reservation(
        ReservationFrame::ProxyAck(sync::ProxyAck {
            id: issue.id.clone(),
            network_order: issue.network_order,
        }),
        scheduling_deadline(),
    )?;
    let mut pair_resets = 0;
    let (local_predecessor, local_checkpoint) = loop {
        let lower_prepare = match recv_reservation_ignoring_progress(wire, None, watch_output)? {
            ReservationFrame::PairPrepare(checkpoint)
                if checkpoint.id == issue.id
                    && checkpoint.network_order == issue.network_order
                    && checkpoint.nonce == issue.nonce =>
            {
                checkpoint
            }
            other => bail!("expected exact pair prepare, got {other:?}"),
        };
        let local_predecessor = prepare_local_pair(
            state,
            &request,
            relationship,
            remote_peer,
            issue.network_order,
            &issue.nonce,
            wire,
        )?;
        recover_before_pair(state, wire)?;
        let local_checkpoint = sync::PairCheckpoint {
            id: issue.id.clone(),
            network_order: issue.network_order,
            nonce: issue.nonce.clone(),
            predecessor: local_predecessor.wire.clone(),
        };
        wire.send_reservation(
            ReservationFrame::PairPrepareAck(local_checkpoint.clone()),
            scheduling_deadline(),
        )?;
        match recv_reservation_ignoring_progress(wire, None, watch_output)? {
            ReservationFrame::PairCommit(checkpoint) if checkpoint == lower_prepare => {
                break (local_predecessor, local_checkpoint);
            }
            ReservationFrame::PairReset(checkpoint) if checkpoint == lower_prepare => {
                note_pair_reset(&mut pair_resets)?;
                if !state.park_paired_sync(
                    request.token(),
                    relationship,
                    remote_peer,
                    issue.network_order.get() as i64,
                    issue.nonce.as_str(),
                )? {
                    bail!("local synchronization changed before pair reset");
                }
                wire.send_reservation(
                    ReservationFrame::PairResetAck(checkpoint),
                    scheduling_deadline(),
                )?;
            }
            other => bail!("expected exact pair commit or reset, got {other:?}"),
        }
    };
    let local_commit = state.commit_paired_sync(
        request.token(),
        relationship,
        remote_peer,
        issue.network_order.get() as i64,
        issue.nonce.as_str(),
        &local_predecessor.stored,
    );
    match local_commit {
        Ok(true) => {}
        Ok(false) => {
            state.record_relationship_yield(relationship, relationship_retry_ns())?;
            bail!("local synchronization reservation was invalidated before commit");
        }
        Err(error) => {
            state.record_relationship_yield(relationship, relationship_retry_ns())?;
            return Err(error);
        }
    }

    let committed = (|| -> Result<(InstallationPermit, std::fs::File, sync::Reservation)> {
        let lease_deadline = std::time::Instant::now() + sync::reservation_lease();
        wire.send_reservation(
            ReservationFrame::PairCommitAck(local_checkpoint),
            lease_deadline,
        )?;
        let reservation =
            match recv_reservation_ignoring_progress(wire, Some(lease_deadline), watch_output)? {
                ReservationFrame::SyncReserved(reservation)
                    if reservation.id == issue.id
                        && reservation.network_order == issue.network_order
                        && reservation.nonce == issue.nonce =>
                {
                    reservation
                }
                other => bail!("expected exact synchronization reservation, got {other:?}"),
            };
        let installation = wait_for_paired_permit(request, share, wire, Some(lease_deadline))?;
        let share_lock = state.lock_share(share)?;
        validate_final_sync_binding(state, share, binding, intent_generation)?;
        wire.send_reservation(
            ReservationFrame::SyncStart(sync::SyncStart {
                id: reservation.id.clone(),
                network_order: reservation.network_order,
                nonce: reservation.nonce.clone(),
                connector_generation,
                responder_generation,
            }),
            lease_deadline,
        )?;
        match recv_reservation_ignoring_progress(wire, Some(lease_deadline), watch_output)? {
            ReservationFrame::SyncAccepted(accepted) if accepted == reservation => {}
            other => bail!("expected exact synchronization acceptance, got {other:?}"),
        }
        Ok((installation, share_lock, reservation))
    })();
    if committed.is_err() {
        state.record_relationship_yield(relationship, relationship_retry_ns())?;
    }
    committed
}

enum SyncAttempt {
    Complete(SyncCompletion),
    Preview(blake3::Hash),
}

fn sync_plan_fingerprint(
    local: &[flocal::model::Record],
    remote: &[flocal::model::Record],
    plan: &flocal::reconcile::Plan,
) -> Result<blake3::Hash> {
    Ok(blake3::hash(&serde_json::to_vec(&(local, remote, plan))?))
}

fn run_sync(
    state: &mut State,
    path: &Path,
    dry_run: bool,
    yes: bool,
    json: bool,
    report: PlanReport,
    managed_initial_generation: Option<i64>,
) -> Result<SyncCompletion> {
    let (share, _) = state.find_share(path)?;
    let requires_confirmation = !dry_run && !yes && !state.initial_complete(&share)?;
    if !requires_confirmation {
        return match run_sync_attempt(
            state,
            path,
            dry_run,
            None,
            json,
            report,
            managed_initial_generation,
        )? {
            SyncAttempt::Complete(completion) => Ok(completion),
            SyncAttempt::Preview(_) if dry_run => Ok(SyncCompletion::default()),
            SyncAttempt::Preview(_) => unreachable!("only a dry run previews without comparison"),
        };
    }

    let mut expected = match run_sync_attempt(
        state,
        path,
        true,
        None,
        json,
        report,
        managed_initial_generation,
    )? {
        SyncAttempt::Preview(fingerprint) => fingerprint,
        SyncAttempt::Complete(_) => unreachable!("initial preview must not apply"),
    };
    loop {
        if !confirm("Apply this initial plan?")? {
            return Ok(SyncCompletion::default());
        }
        match run_sync_attempt(
            state,
            path,
            false,
            Some(expected),
            json,
            report,
            managed_initial_generation,
        )? {
            SyncAttempt::Complete(completion) => return Ok(completion),
            SyncAttempt::Preview(fingerprint) => expected = fingerprint,
        }
    }
}

fn run_sync_attempt(
    state: &mut State,
    path: &Path,
    dry_run: bool,
    expected_plan: Option<blake3::Hash>,
    json: bool,
    report: PlanReport,
    managed_initial_generation: Option<i64>,
) -> Result<SyncAttempt> {
    let (share, _) = state.find_share(path)?;
    let session_lock = state.lock_share_session(&share)?;
    state.begin_install_intent_retry(&share)?;
    let operation = if state.initial_complete(&share)? {
        SyncOperation::Sync
    } else {
        SyncOperation::Initial
    };
    let peer = state
        .peer(&share)?
        .context("no peer configured; run `flocal peer add`")?;
    let binding = connector_sync_binding(state, &share, &peer)?;
    let mut remote = Remote::spawn(&peer.host, &peer.executable)?;
    sync::write_message(
        &mut remote.input,
        &Message::Sync {
            protocol: sync::PROTOCOL_VERSION,
            share: share.clone(),
            peer: state.peer_id()?,
            relationship: binding.relationship.clone(),
            dry_run,
        },
    )?;
    let (installation, _share_lock, _) = {
        let mut wire = V1ReservationWire {
            input: &remote.output,
            output: &remote.input,
        };
        match binding.order {
            std::cmp::Ordering::Less => reserve_as_authority(
                state,
                &share,
                &binding,
                operation,
                None,
                0,
                0,
                sync::SchedulingId::generate(),
                None,
                &mut wire,
                &mut io::stdout(),
            )?,
            std::cmp::Ordering::Greater => reserve_as_higher_peer(
                state,
                &share,
                &binding,
                operation,
                None,
                0,
                0,
                true,
                None,
                &mut wire,
                &mut io::stdout(),
            )?,
            std::cmp::Ordering::Equal => unreachable!("peer ordering rejects equality"),
        }
    };
    validate_final_sync_binding(state, &share, &binding, None)?;
    state.clear_pending_objects(&share)?;
    state.prune_unreferenced_objects()?;
    let local = if dry_run {
        sync::preview_refresh(state, &share)?
    } else {
        sync::refresh(state, &share)?
    };
    let remote_records = sync::read_snapshot(&mut remote.output)?;
    state.validate_remote_records(&share, &local, &remote_records)?;
    let mut plan = sync::plan(&local, &remote_records);
    let fingerprint = sync_plan_fingerprint(&local, &remote_records, &plan)?;
    if !dry_run {
        ensure_connector_recovery_limits(state, &share, &[])?;
    }

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
                    return Err(remote.finish_after_error(anyhow::anyhow!(
                        "inbound object transfer exceeds session byte limit"
                    )));
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

    let plan_changed = expected_plan.is_some_and(|expected| expected != fingerprint);
    if dry_run || plan_changed {
        let mut preview = plan.clone();
        sync::preview_merges(state, &mut preview)?;
        if report == PlanReport::Full {
            print_plan(&local, &remote_records, &preview, json, PlanReport::Preview)?;
        }
    }
    if dry_run || plan_changed {
        sync::write_message(&mut remote.input, &Message::Cancel)?;
        state.clear_pending_objects(&share)?;
        state.prune_unreferenced_objects()?;
        remote.finish()?;
        installation.finish()?;
        return Ok(SyncAttempt::Preview(fingerprint));
    }
    sync::materialize_merges(state, &share, &mut plan)?;
    ensure_connector_recovery_limits(state, &share, &plan.conflicts)?;
    if report == PlanReport::Full {
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
        Message::Error { message } => {
            bail!("remote rejected recovery plan: {}", escaped(&message))
        }
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
            return Err(remote.finish_after_error(anyhow::anyhow!(
                "outbound object transfer exceeds session byte limit"
            )));
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
    let managed_request = if operation == SyncOperation::Initial {
        if let Some(generation) = managed_initial_generation {
            Some(sync::apply_complete_plan_and_enable_managed(
                state, &share, &plan, generation,
            )?)
        } else {
            sync::apply_complete_plan(state, &share, &plan)?;
            None
        }
    } else {
        sync::apply_complete_plan(state, &share, &plan)?;
        None
    };
    let post_commit = (|| -> Result<(Option<Vec<u8>>, Result<()>)> {
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
        Ok((committed_report, finalization))
    })();
    let installation = installation.finish();
    drop(session_lock);
    let managed_handoff = if let Some(request) = managed_request {
        let result = daemon_request(
            state,
            DaemonRequest::Start {
                share: share.0.clone(),
            },
        );
        request.release_for_reclaim();
        result.map(|_| ())
    } else {
        Ok(())
    };
    let (committed_report, finalization) = post_commit?;
    installation?;
    managed_handoff?;
    if report == PlanReport::Watch {
        Ok(SyncAttempt::Complete(SyncCompletion {
            watch_report: committed_report,
            post_commit_error: finalization.err(),
            initial_applied: operation == SyncOperation::Initial,
        }))
    } else {
        finalization?;
        Ok(SyncAttempt::Complete(SyncCompletion {
            initial_applied: operation == SyncOperation::Initial,
            ..SyncCompletion::default()
        }))
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
            relationship,
        } => serve_watch_open(state, protocol, share, peer, relationship, &stdin, &stdout),
        initial => {
            let mut input = TimedReader::new(DirectReader(&stdin));
            let mut output = DirectWriter(&stdout);
            serve_initial(state, initial, &mut input, &mut output)
        }
    }
}

fn serve_relationship() -> Result<()> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let request = sync::read_relationship_request_until(
        &stdin,
        std::time::Instant::now() + Duration::from_secs(30),
    )?;
    let mut state = State::open_default()?;
    match handle_relationship_request(&mut state, request)? {
        RelationshipResponse::Register(response) => {
            sync::write_register_relationship_response_until(
                &stdout,
                &response,
                std::time::Instant::now() + Duration::from_secs(30),
            )
        }
        RelationshipResponse::Remove(response) => sync::write_remove_relationship_response_until(
            &stdout,
            &response,
            std::time::Instant::now() + sync::default_frame_deadline(),
        ),
    }
}

enum RelationshipResponse {
    Register(RegisterRelationshipResponse),
    Remove(RemoveRelationshipResponse),
}

fn handle_relationship_request(
    state: &mut State,
    request: RelationshipRequest,
) -> Result<RelationshipResponse> {
    match request {
        RelationshipRequest::RegisterRelationship {
            registration_protocol,
            share,
            peer,
            root,
            relationship,
        } => {
            let response = if registration_protocol
                != sync::RELATIONSHIP_REGISTRATION_PROTOCOL_VERSION
            {
                RegisterRelationshipResponse::Error {
                    message: bounded_relationship_error(
                        "unsupported relationship-registration protocol version",
                    ),
                }
            } else {
                let (permit, _) =
                    wait_for_installation_request(state, None, SyncOperation::Registration, None)?;
                let registration = state.register_relationship_locked(
                    &share,
                    &bytes_path(&root),
                    &peer,
                    &relationship,
                );
                permit.finish()?;
                match registration {
                    Ok(RegistrationOutcome { prior_share }) => {
                        RegisterRelationshipResponse::Registered {
                            registration_protocol: sync::RELATIONSHIP_REGISTRATION_PROTOCOL_VERSION,
                            share,
                            peer: state.peer_id()?,
                            relationship,
                            prior_share,
                        }
                    }
                    Err(error) => RegisterRelationshipResponse::Error {
                        message: bounded_relationship_error(&format!("{error:#}")),
                    },
                }
            };
            Ok(RelationshipResponse::Register(response))
        }
        RelationshipRequest::RemoveRelationship {
            removal_protocol,
            share,
            peer,
            expected_peer,
            relationship,
        } => {
            let local_peer = state.peer_id()?;
            let response = if removal_protocol != sync::RELATIONSHIP_REMOVAL_PROTOCOL_VERSION
                || expected_peer != local_peer
            {
                RemoveRelationshipResponse::Error {
                    message: bounded_relationship_error("relationship binding does not match"),
                }
            } else {
                let (permit, _) = wait_for_installation_request(
                    state,
                    Some(&share),
                    SyncOperation::Removal,
                    None,
                )?;
                let removal = match state.prepare_incoming_removal_locked(
                    &share,
                    &peer,
                    &relationship,
                ) {
                    Ok(IncomingRemoval::Absent) => RemoveRelationshipResponse::Absent {
                        removal_protocol: sync::RELATIONSHIP_REMOVAL_PROTOCOL_VERSION,
                        share,
                        peer: local_peer,
                        relationship,
                    },
                    Ok(IncomingRemoval::Prepared(prepared)) => {
                        let removal = state.detach_incoming_relationship_locked(&prepared);
                        match removal {
                            Ok(detached) => {
                                if let Some(warning) = detached.cleanup_warning {
                                    eprintln!(
                                        "flocal: relationship removed; object cleanup remains pending: {}",
                                        escaped(&warning)
                                    );
                                }
                                RemoveRelationshipResponse::Absent {
                                    removal_protocol: sync::RELATIONSHIP_REMOVAL_PROTOCOL_VERSION,
                                    share,
                                    peer: local_peer,
                                    relationship,
                                }
                            }
                            Err(error) => {
                                let _ = state.set_removal_diagnostic(
                                    &share,
                                    &prepared.relationship,
                                    &format!("{error:#}"),
                                );
                                RemoveRelationshipResponse::Error {
                                    message: bounded_relationship_error(&format!("{error:#}")),
                                }
                            }
                        }
                    }
                    Err(_) => RemoveRelationshipResponse::Error {
                        message: bounded_relationship_error("relationship binding does not match"),
                    },
                };
                permit.finish()?;
                removal
            };
            Ok(RelationshipResponse::Remove(response))
        }
    }
}

fn bounded_relationship_error(message: &str) -> String {
    let mut bytes = 0usize;
    message
        .chars()
        .map(|character| {
            if matches!(character, '\n' | '\r') {
                ' '
            } else {
                character
            }
        })
        .take_while(|character| {
            let next = bytes.saturating_add(character.len_utf8());
            if next > sync::MAX_RELATIONSHIP_ERROR_BYTES {
                false
            } else {
                bytes = next;
                true
            }
        })
        .collect()
}

#[cfg(test)]
fn serve_io(
    state: &mut State,
    mut input: &mut (impl Read + Send),
    output: &mut impl Write,
) -> Result<()> {
    let initial = sync::read_initial_message(&mut input)?;
    let mut remaining = Vec::new();
    input.read_to_end(&mut remaining)?;
    let (mut client, server) = UnixStream::pair()?;
    client.write_all(&remaining)?;
    client.shutdown(std::net::Shutdown::Write)?;
    let mut server_input = server.try_clone()?;
    let mut server_output = server;
    let result = serve_initial(state, initial, &mut server_input, &mut server_output);
    drop(server_input);
    drop(server_output);
    let mut response = Vec::new();
    client.read_to_end(&mut response)?;
    output.write_all(&response)?;
    result
}

fn serve_initial(
    state: &mut State,
    initial: InitialMessage,
    mut input: &mut (impl Read + AsFd),
    mut output: &mut (impl Write + AsFd),
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
            if let Err(error) =
                state.acknowledge_legacy_registration(&share, &bytes_path(&root), &peer)
            {
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
            relationship,
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
            let binding = match validate_sync_binding(state, &share, &peer, &relationship) {
                Ok(binding) => binding,
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
            let _session_lock = match state.lock_share_session(&share) {
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
            let (installation, _share_lock, _) = {
                let mut wire = V1ReservationWire {
                    input: &*input,
                    output: &*output,
                };
                match binding.order {
                    std::cmp::Ordering::Less => {
                        let pending = match recv_reservation_ignoring_progress(
                            &mut wire,
                            None,
                            &mut io::sink(),
                        )? {
                            ReservationFrame::PendingAuthority(pending)
                                if pending.connector_generation == 0
                                    && pending.responder_generation == 0 =>
                            {
                                pending
                            }
                            other => {
                                bail!("expected pending authority submission, got {other:?}")
                            }
                        };
                        reserve_as_authority(
                            state,
                            &share,
                            &binding,
                            SyncOperation::Sync,
                            None,
                            0,
                            0,
                            pending.id,
                            None,
                            &mut wire,
                            &mut io::sink(),
                        )?
                    }
                    std::cmp::Ordering::Greater => reserve_as_higher_peer(
                        state,
                        &share,
                        &binding,
                        SyncOperation::Sync,
                        None,
                        0,
                        0,
                        false,
                        None,
                        &mut wire,
                        &mut io::sink(),
                    )?,
                    std::cmp::Ordering::Equal => unreachable!("peer ordering rejects equality"),
                }
            };
            if let Err(error) = validate_final_sync_binding(state, &share, &binding, None) {
                sync::write_message(
                    &mut output,
                    &Message::Error {
                        message: format!("{error:#}"),
                    },
                )?;
                return Ok(());
            }
            state.clear_pending_objects(&share)?;
            state.prune_unreferenced_objects()?;
            let records = if dry_run {
                sync::preview_refresh(state, &share)?
            } else {
                sync::refresh(state, &share)?
            };
            sync::write_snapshot(&mut output, &records)?;
            serve_sync(
                state,
                &share,
                &binding.remote_peer,
                &records,
                &mut input,
                &mut output,
            )?;
            installation.finish()?;
            sync::write_message(&mut output, &Message::Done)?;
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
    relationship: RelationshipId,
    input: &impl AsFd,
    output: &impl AsFd,
) -> Result<()> {
    let startup_deadline = std::time::Instant::now() + sync::default_phase_deadline();
    if protocol != sync::WATCH_PROTOCOL_VERSION {
        return write_v2_session(
            output,
            V2SessionFrame::Error {
                retryable: true,
                message: format!(
                    "unsupported persistent watch protocol version {protocol}; upgrade flocal on both peers"
                ),
            },
        );
    }
    let binding = match validate_sync_binding(state, &share, &peer, &relationship) {
        Ok(binding) => binding,
        Err(error) => {
            return write_v2_session(
                output,
                V2SessionFrame::Error {
                    retryable: false,
                    message: format!("{error:#}"),
                },
            );
        }
    };
    let _session_lock = match state.lock_share_session(&share) {
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
    if state.bound_peer(&share)?.as_ref() != Some(&binding.remote_peer) {
        return write_v2_session(
            output,
            V2SessionFrame::Error {
                retryable: false,
                message: "peer identity mismatch".into(),
            },
        );
    }
    if let Err(error) = state.ensure_not_removing(&share) {
        return write_v2_session(
            output,
            V2SessionFrame::Error {
                retryable: false,
                message: format!("{error:#}"),
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
    if let Err(error) = read_watch_ready(state, &share, input, startup_deadline) {
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
        if state.upgrade_pending()? {
            return Ok(());
        }
        if let Err(error) = state.ensure_not_removing(&share) {
            return write_v2_session(
                output,
                V2SessionFrame::Error {
                    retryable: false,
                    message: format!("{error:#}"),
                },
            );
        }
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
                frame: frame @ (V2RoundFrame::PendingAuthority(_) | V2RoundFrame::ProxyIssue(_)),
            } if incoming == round + 1 => {
                round = incoming;
                let served = serve_v2_round(
                    state,
                    &share,
                    &binding,
                    &share_root,
                    round,
                    frame,
                    input,
                    output,
                    &invalidation_cycle.deferred,
                );
                if let Err(error) = served {
                    let retryable = error.downcast_ref::<WatchProtocolError>().is_none()
                        && error.downcast_ref::<sync::RootIdentityChanged>().is_none()
                        && error
                            .downcast_ref::<flocal::state::RecoveryLimitExceeded>()
                            .is_none();
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

fn read_watch_ready(
    state: &mut State,
    share: &ShareId,
    input: &impl AsFd,
    phase_deadline: std::time::Instant,
) -> Result<()> {
    let mut unsettled = Vec::new();
    let mut budget = sync::RoundBudget::new(phase_deadline);
    loop {
        match sync::read_v2_envelope_in_phase(input, budget.phase_deadline()?)? {
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
    match sync::read_v2_envelope_in_phase(input, budget.phase_deadline()?)? {
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
    binding: &ValidatedSyncBinding,
    root: &sync::ShareRoot,
    round: u64,
    initial: V2RoundFrame,
    input: &impl AsFd,
    output: &impl AsFd,
    deferred: &std::collections::HashSet<Vec<u8>>,
) -> Result<ServedRound> {
    let first = reservation_from_v2(initial)?;
    let mut wire = V2ReservationWire {
        round,
        input,
        output,
        pending_remote_generation: None,
        prefetched: Some(first),
    };
    let (installation, _share_lock, _) = match binding.order {
        std::cmp::Ordering::Less => {
            let pending =
                match recv_reservation_ignoring_progress(&mut wire, None, &mut io::sink())? {
                    ReservationFrame::PendingAuthority(pending) => pending,
                    other => bail!("expected pending authority submission, got {other:?}"),
                };
            reserve_as_authority(
                state,
                share,
                binding,
                SyncOperation::Watch,
                None,
                pending.connector_generation,
                pending.responder_generation,
                pending.id,
                None,
                &mut wire,
                &mut io::sink(),
            )?
        }
        std::cmp::Ordering::Greater => {
            let (connector_generation, responder_generation) = match &wire.prefetched {
                Some(ReservationFrame::ProxyIssue(issue)) => {
                    (issue.connector_generation, issue.responder_generation)
                }
                other => bail!("expected authoritative proxy issue, got {other:?}"),
            };
            reserve_as_higher_peer(
                state,
                share,
                binding,
                SyncOperation::Watch,
                None,
                connector_generation,
                responder_generation,
                false,
                None,
                &mut wire,
                &mut io::sink(),
            )?
        }
        std::cmp::Ordering::Equal => unreachable!("peer ordering rejects equality"),
    };
    let mut installation = Some(installation);
    validate_final_sync_binding(state, share, binding, None)?;
    state.clear_pending_objects(share)?;
    state.prune_unreferenced_objects()?;
    let mut budget =
        sync::RoundBudget::new(std::time::Instant::now() + sync::default_phase_deadline());
    budget.check()?;
    let advertised = sync::refresh_with_root(state, share, root)?;
    budget.check()?;
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
    sync::validate_materialized_plan_shape(&plan, &expected, &binding.remote_peer)
        .map_err(|error| watch_protocol_error(format!("{error:#}")))?;
    let applied_plan = effective_plan(&advertised, &peer_records, &plan, deferred);
    let connector_plan = effective_plan(&peer_records, &advertised, &plan, deferred);
    state.ensure_recovery_limits(share, &applied_plan.plan.conflicts)?;
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
            installation
                .take()
                .expect("installation permit exists")
                .finish()?;
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
                installation
                    .take()
                    .expect("installation permit exists")
                    .finish()?;
                return Ok(ServedRound::Completed);
            }
            V2RoundFrame::RoundInvalidated { path }
                if accepts_invalidation(&connector_plan, &path) =>
            {
                state.remember_unsettled_path(share, &path)?;
                installation
                    .take()
                    .expect("installation permit exists")
                    .finish()?;
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
                if let Err(error) = state.ensure_recovery_limits(share, &pending.conflicts) {
                    sync::write_message(
                        output,
                        &Message::Error {
                            message: format!("{error:#}"),
                        },
                    )?;
                    return Ok(());
                }
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
            Message::Cancel if !plan_ready => {
                break;
            }
            other => bail!("unexpected sync message: {other:?}"),
        }
    }
    Ok(())
}

fn ensure_connector_recovery_limits(
    state: &State,
    share: &ShareId,
    conflicts: &[flocal::reconcile::Conflict],
) -> Result<()> {
    match state.ensure_recovery_limits(share, conflicts) {
        Ok(()) => Ok(()),
        Err(error) => {
            let cleanup = state.clear_pending_objects(share).and_then(|()| {
                state
                    .prune_unreferenced_objects()
                    .context("collecting candidate objects after recovery limit failure")
            });
            match cleanup {
                Ok(()) => Err(error),
                Err(cleanup) => {
                    Err(error.context(format!("candidate-object cleanup also failed: {cleanup:#}")))
                }
            }
        }
    }
}

fn status(state: &mut State, path: &Path, json: bool) -> Result<()> {
    let (share, root) = state.find_share(path)?;
    let managed = state.managed_share(&share)?;
    let (relationship_state, bound_peer) = if managed.removing_relationship.is_some() {
        (
            "removing",
            match &managed.binding {
                EndpointBinding::Connector(peer) => peer.peer_id.clone(),
                EndpointBinding::Responder { peer, .. } => Some(peer.clone()),
                EndpointBinding::Unpaired => None,
            },
        )
    } else {
        match &managed.binding {
            EndpointBinding::Connector(peer) if peer.peer_id.is_none() => ("registering", None),
            EndpointBinding::Connector(peer) => ("connector", peer.peer_id.clone()),
            EndpointBinding::Responder { peer, .. } => ("responder", Some(peer.clone())),
            EndpointBinding::Unpaired => ("unpaired", None),
        }
    };
    let removal_pending = managed.removing_relationship.is_some();
    let removal_error = removal_pending
        .then(|| managed.blocked_diagnostic.clone())
        .flatten();
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
    let recovery = state.recovery_usage(&share)?;
    let scheduling_snapshot = state.scheduling_snapshot()?;
    let scheduling_view = share_scheduling_view(state, &scheduling_snapshot, &share)?;
    let waiting_on = scheduling_view
        .blocker
        .as_ref()
        .map(|blocker| match blocker {
            SchedulingBlocker::Local(_) => "local",
            SchedulingBlocker::Peer => "peer",
        });
    let waiting_root = scheduling_view
        .blocker
        .as_ref()
        .and_then(|blocker| match blocker {
            SchedulingBlocker::Local(root) => {
                root.as_ref().map(|root| daemon_path(&path_bytes(root)))
            }
            SchedulingBlocker::Peer => None,
        });
    let scheduling = serde_json::json!({
        "state": scheduling_view.state,
        "waiting_on": waiting_on,
        "waiting_root": waiting_root,
        "operation": scheduling_view.operation,
        "queue_position": scheduling_view.queue_position,
        "active_share": scheduling_view.active_share.as_ref().map(|active| &active.0),
        "active_root": scheduling_view.active_root.as_ref().map(|root| daemon_path(&path_bytes(root))),
        "active_operation": scheduling_view.active_operation,
    });
    let peer = match &managed.binding {
        EndpointBinding::Connector(peer) => Some(peer),
        EndpointBinding::Responder { .. } | EndpointBinding::Unpaired => None,
    };
    if json {
        println!(
            "{}",
            serde_json::json!({"schema":6,"share":share.0,"root":root,"peer":peer,"bound_peer":bound_peer,"relationship_state":relationship_state,"removal_pending":removal_pending,"removal_error":removal_error,"entries":entries,"tombstones":tombstones,"initial_complete":state.initial_complete(&share)?,"view":"last_persisted_scan","pending_install":pending_install,"unsettled":unsettled,"recovery":recovery,"scheduling":scheduling})
        );
    } else {
        println!("Share: {}", escaped(&share.0));
        println!("Root:  {}", escaped(&root.to_string_lossy()));
        println!(
            "Peer:  {}",
            peer.map(|p| escaped(&p.host))
                .unwrap_or_else(|| "not configured".into())
        );
        println!("Relationship: {relationship_state}");
        if let Some(peer) = bound_peer {
            println!("Bound peer: {}", escaped(&peer.0));
        }
        println!("Entries: {entries}");
        println!("Tombstones: {tombstones}");
        println!(
            "Recovery: {} conflicts, {} / {}",
            recovery.conflicts,
            format_bytes(recovery.used_bytes),
            format_bytes(recovery.budget_bytes)
        );
        println!(
            "Reclaimable now: {}",
            format_bytes(recovery.reclaimable_bytes)
        );
        if recovery.used_bytes >= recovery.budget_bytes {
            println!(
                "Warning: recovery storage is at its limit; use `flocal conflicts prune PATH` or `flocal conflicts budget PATH SIZE` for the root shown above"
            );
        }
        if recovery.over_conflict_limit || recovery.over_metadata_limit {
            println!(
                "Warning: fixed recovery record limits are exceeded; use `flocal conflicts prune PATH` for the root shown above"
            );
        }
        println!(
            "View:  last persisted scan (use `flocal sync PATH --dry-run` to preview this root)"
        );
        if let Some(blocker) = &scheduling_view.blocker {
            let position = scheduling_view.queue_position.unwrap_or(1);
            match blocker {
                SchedulingBlocker::Local(Some(root)) => println!(
                    "Waiting for {} sync to finish (queue position {})",
                    escaped(&root.to_string_lossy()),
                    position
                ),
                SchedulingBlocker::Local(None) => println!(
                    "Waiting for the installation synchronization slot (queue position {})",
                    position
                ),
                SchedulingBlocker::Peer => println!(
                    "Waiting for the peer to finish another synchronization (queue position {})",
                    position
                ),
            }
        }
        if pending_install {
            println!("Warning: an interrupted install will be recovered by the next sync/watch");
        }
        if removal_pending {
            println!(
                "Removal is pending; rerun `flocal sync remove --share {}`.",
                escaped(&share.0)
            );
            if let Some(error) = removal_error {
                println!("Last removal error: {}", escaped(&error));
            }
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

fn format_bytes(bytes: u64) -> String {
    if bytes == 0 {
        return "0 bytes".into();
    }
    const UNITS: [(&str, u64); 4] = [
        ("GiB", 1024 * 1024 * 1024),
        ("MiB", 1024 * 1024),
        ("KiB", 1024),
        ("bytes", 1),
    ];
    let (unit, divisor) = UNITS
        .into_iter()
        .find(|(_, divisor)| bytes >= *divisor)
        .expect("bytes unit always matches");
    if divisor == 1 {
        format!("{bytes} {unit}")
    } else {
        format!("{:.1} {unit}", bytes as f64 / divisor as f64)
    }
}

fn conflicts(state: &mut State, command: ConflictCommand) -> Result<()> {
    match command {
        ConflictCommand::List {
            path,
            json,
            ids,
            limit,
            after,
        } => {
            let (share, _) = state.find_share(&path)?;
            if ids {
                let limit = limit.unwrap_or(100);
                let mut conflicts = state.conflict_ids_page(&share, after.as_deref(), limit)?;
                let next_after = (conflicts.len() > limit).then(|| conflicts[limit - 1].id.clone());
                conflicts.truncate(limit);
                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&serde_json::json!({
                            "schema": 1,
                            "conflicts": conflicts,
                            "next_after": next_after,
                        }))?
                    );
                } else {
                    for conflict in conflicts {
                        println!("{}  {}", conflict.id, conflict.path.display());
                    }
                    if let Some(next_after) = next_after {
                        println!(
                            "More conflicts remain. Rerun with `--ids --after {} --limit {}`.",
                            next_after, limit
                        );
                    }
                }
                return Ok(());
            }
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
        ConflictCommand::Prune {
            path,
            conflict_ids,
            selection,
            yes,
            json,
        } => {
            let (share, root) = state.find_share(&path)?;
            if yes && selection.is_none() {
                bail!("--yes requires the --selection token from a prune preview");
            }
            if !yes && selection.is_some() {
                bail!("--selection is only valid with --yes");
            }
            if yes {
                let permit =
                    wait_for_installation(state, &share, SyncOperation::Maintenance, None)?;
                let outcome = state.prune_recovery_locked(
                    &share,
                    &conflict_ids,
                    selection.as_deref().expect("checked above"),
                )?;
                permit.finish()?;
                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&serde_json::json!({
                            "schema": 1,
                            "applied": true,
                            "selection_token": outcome.plan.selection_token,
                            "conflicts": outcome.plan.conflicts,
                            "released_bytes": outcome.plan.released_bytes,
                            "reclaimable_bytes": outcome.plan.reclaimable_bytes,
                            "collection_pending": outcome.collection_pending,
                        }))?
                    );
                } else {
                    println!(
                        "Pruned {} recovery conflicts; released {} of this share's recovery allowance.",
                        outcome.plan.conflicts.len(),
                        format_bytes(outcome.plan.released_bytes)
                    );
                    println!(
                        "Physical objects reclaimable now: {}",
                        format_bytes(outcome.plan.reclaimable_bytes)
                    );
                    println!("Pruning is local; preview and prune separately on the peer.");
                }
                restart_after_recovery(state, &share, &root, true)?;
                if outcome.collection_pending {
                    bail!(
                        "conflicts were pruned, but object collection is pending; rerun pruning or synchronization"
                    );
                }
            } else {
                let permit =
                    wait_for_installation(state, &share, SyncOperation::Maintenance, None)?;
                let plan = state.recovery_prune_plan_locked_with_objects(&share, &conflict_ids)?;
                permit.finish()?;
                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&serde_json::json!({
                            "schema": 1,
                            "applied": false,
                            "selection_token": plan.selection_token,
                            "conflicts": plan.conflicts,
                            "released_bytes": plan.released_bytes,
                            "reclaimable_bytes": plan.reclaimable_bytes,
                        }))?
                    );
                } else {
                    println!("Recovery conflicts selected for pruning:");
                    for conflict in &plan.conflicts {
                        println!("  {}  {}", conflict.id, conflict.path.display());
                    }
                    println!(
                        "Share allowance released: {}",
                        format_bytes(plan.released_bytes)
                    );
                    println!(
                        "Physical objects reclaimable now: {}",
                        format_bytes(plan.reclaimable_bytes)
                    );
                    println!("Selection token: {}", plan.selection_token);
                    println!(
                        "Apply by rerunning this preview command with `--selection {} --yes`.",
                        plan.selection_token
                    );
                    println!("Pruning is local; preview and prune separately on the peer.");
                }
            }
        }
        ConflictCommand::Budget {
            path_or_size,
            size,
            share,
            peer,
            json,
        } => {
            let (share_id, root, size_text) = match share {
                Some(share) => {
                    if peer || size.is_some() {
                        bail!("--share cannot be combined with --peer or a PATH");
                    }
                    validate_share_id(&share)?;
                    let share_id = ShareId(share);
                    let root = state.managed_share(&share_id)?.root;
                    (share_id, root, path_or_size)
                }
                None => {
                    let size = size.context("budget requires PATH SIZE")?;
                    let (share, root) = state.find_share(Path::new(&path_or_size))?;
                    (share, root, size)
                }
            };
            let budget = parse_recovery_size(&size_text)?;
            if peer {
                let previous = raise_peer_recovery_budget(state, &share_id, budget)?;
                if json {
                    println!(
                        "{}",
                        serde_json::json!({"schema":1,"target":"peer","previous_bytes":previous,"budget_bytes":budget})
                    );
                } else {
                    println!(
                        "Raised peer recovery budget from {} to {}.",
                        format_bytes(previous),
                        format_bytes(budget)
                    );
                }
                restart_after_recovery(state, &share_id, &root, false)?;
            } else {
                let previous = state.raise_recovery_budget(&share_id, budget)?;
                if json {
                    println!(
                        "{}",
                        serde_json::json!({"schema":1,"target":"local","previous_bytes":previous,"budget_bytes":budget})
                    );
                } else {
                    println!(
                        "Raised local recovery budget from {} to {}.",
                        format_bytes(previous),
                        format_bytes(budget)
                    );
                }
                restart_after_recovery(state, &share_id, &root, false)?;
            }
        }
    }
    Ok(())
}

fn parse_recovery_size(value: &str) -> Result<u64> {
    let (digits, multiplier) = [
        ("KiB", 1024u64),
        ("MiB", 1024 * 1024),
        ("GiB", 1024 * 1024 * 1024),
    ]
    .into_iter()
    .find_map(|(suffix, multiplier)| {
        value
            .strip_suffix(suffix)
            .map(|digits| (digits, multiplier))
    })
    .unwrap_or((value, 1));
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("SIZE must be an integer byte count or use KiB, MiB, or GiB");
    }
    let amount = digits.parse::<u64>()?;
    let bytes = amount.checked_mul(multiplier).context("SIZE overflows")?;
    if bytes == 0 || bytes > i64::MAX as u64 {
        bail!("SIZE must be between 1 and {} bytes", i64::MAX);
    }
    Ok(bytes)
}

fn validate_share_id(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        bail!("share ID contains unsafe characters");
    }
    Ok(())
}

fn restart_after_recovery(
    state: &State,
    share: &ShareId,
    root: &Path,
    any_recovery_limit: bool,
) -> Result<()> {
    let managed = state.managed_share(share)?;
    let blocked = managed
        .blocked_diagnostic
        .as_deref()
        .is_some_and(|diagnostic| {
            diagnostic.contains("recovery storage budget exceeded")
                || (any_recovery_limit
                    && (diagnostic.contains("recovery conflict count exceeded")
                        || diagnostic.contains("recovery metadata limit exceeded")))
        });
    if !blocked {
        return Ok(());
    }
    if managed.watch_enabled && matches!(&managed.binding, EndpointBinding::Connector(_)) {
        let restart = || -> Result<()> {
            ensure_daemon(state)?;
            daemon_request(
                state,
                DaemonRequest::Start {
                    share: share.0.clone(),
                },
            )?;
            Ok(())
        };
        restart().with_context(|| {
            format!(
                "automatic restart failed for {}; run `flocal sync start PATH` with that root path",
                root.display()
            )
        })?;
        eprintln!("flocal: restarted the managed synchronization worker");
    } else if !matches!(&managed.binding, EndpointBinding::Connector(_)) {
        eprintln!(
            "flocal: this installation is the responder; on the connector, run `flocal sync start PATH` with root {}",
            root.display()
        );
    }
    Ok(())
}

#[derive(serde::Deserialize)]
struct AdminBudgetResponse {
    schema: u32,
    target: String,
    previous_bytes: u64,
    budget_bytes: u64,
}

struct AdminChild(std::process::Child);

impl Drop for AdminChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn raise_peer_recovery_budget(state: &State, share: &ShareId, budget: u64) -> Result<u64> {
    validate_share_id(&share.0)?;
    let peer = state
        .peer(share)?
        .context("share has no configured connector peer")?;
    validate_host(&peer.host)?;
    validate_executable(&peer.executable)?;
    let command = format!(
        "{} conflicts budget --share {} {} --json",
        peer.executable, share.0, budget
    );
    let mut child = AdminChild(
        Command::new("ssh")
            .args([
                "-T",
                "-o",
                "ConnectTimeout=10",
                "-o",
                "ServerAliveInterval=15",
                "-o",
                "ServerAliveCountMax=2",
                &peer.host,
                &command,
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?,
    );
    let stdout = child.0.stdout.take().context("ssh stdout unavailable")?;
    let stderr = child.0.stderr.take().context("ssh stderr unavailable")?;
    let stdout = std::thread::spawn(move || read_bounded_admin_output(stdout));
    let stderr = std::thread::spawn(move || read_bounded_admin_output(stderr));
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.0.try_wait()? {
            break status;
        }
        if std::time::Instant::now() >= deadline {
            child.0.kill()?;
            let _ = child.0.wait();
            let _ = stdout.join();
            let _ = stderr.join();
            bail!("peer recovery budget command exceeded its 30 second deadline");
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let (stdout, stdout_overflow) = stdout
        .join()
        .map_err(|_| anyhow::anyhow!("peer stdout reader panicked"))??;
    let (stderr, stderr_overflow) = stderr
        .join()
        .map_err(|_| anyhow::anyhow!("peer stderr reader panicked"))??;
    if stdout_overflow || stderr_overflow {
        bail!("peer recovery budget command exceeded its 64 KiB output limit");
    }
    if !status.success() {
        bail!(
            "peer recovery budget command failed: {}",
            escaped(&String::from_utf8_lossy(&stderr))
        );
    }
    let response: AdminBudgetResponse = serde_json::from_slice(&stdout)
        .context("peer recovery budget command returned invalid JSON")?;
    if response.schema != 1 || response.target != "local" || response.budget_bytes != budget {
        bail!("peer recovery budget command returned an invalid response");
    }
    Ok(response.previous_bytes)
}

fn read_bounded_admin_output(mut reader: impl Read) -> io::Result<(Vec<u8>, bool)> {
    let mut kept = Vec::new();
    let mut overflow = false;
    let mut buffer = [0u8; 8192];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            return Ok((kept, overflow));
        }
        let remaining = (64 * 1024usize).saturating_sub(kept.len());
        kept.extend_from_slice(&buffer[..count.min(remaining)]);
        overflow |= count > remaining;
    }
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
    let _object_lock = state.lock_objects()?;
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
    let _session_lock = state.lock_share_session(&share)?;
    state.ensure_not_removing(&share)?;
    state.begin_install_intent_retry(&share)?;
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
    let mut initial_request = None;
    persistent_watch_loop_control(
        state,
        share,
        root_path,
        out,
        err,
        None,
        None,
        Arc::new(Mutex::new(None)),
        &mut initial_request,
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
    initial_request: &mut Option<flocal::state::QueueRequest>,
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
        if stop.is_some() && state.upgrade_pending()? {
            return Ok(());
        }
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
            initial_request,
        );
        match outcome {
            Ok(()) if stop.is_some_and(|stop| stop.load(Ordering::Relaxed)) => return Ok(()),
            Ok(()) => bail!("persistent watch session ended unexpectedly"),
            Err(error)
                if (stop.is_some() && error.downcast_ref::<UpgradePending>().is_some())
                    || (stop.is_some()
                        && error.chain().any(|cause| {
                            cause
                                .downcast_ref::<flocal::state::QueueCancelled>()
                                .is_some()
                        })) =>
            {
                return Ok(());
            }
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
    if error
        .downcast_ref::<flocal::state::RecoveryLimitExceeded>()
        .is_some()
    {
        return true;
    }
    if error.downcast_ref::<InstallRecoveryBlocked>().is_some() {
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
    initial_request: &mut Option<flocal::state::QueueRequest>,
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
        initial_request,
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
    initial_request: &mut Option<flocal::state::QueueRequest>,
) -> Result<()> {
    let startup_deadline = std::time::Instant::now() + sync::default_phase_deadline();
    let binding = connector_sync_binding(state, share, peer)?;
    sync::write_initial_message_until(
        remote_output,
        &InitialMessage::WatchOpen {
            protocol: sync::WATCH_PROTOCOL_VERSION,
            share: share.clone(),
            peer: state.peer_id()?,
            relationship: binding.relationship.clone(),
        },
        std::time::Instant::now() + sync::default_frame_deadline(),
    )?;
    let accepted = sync::read_v2_envelope_in_phase(remote_input, startup_deadline)?;
    match accepted {
        V2Envelope::Session {
            frame:
                V2SessionFrame::WatchAccepted {
                    protocol,
                    peer: actual,
                },
        } if protocol == sync::WATCH_PROTOCOL_VERSION && actual == binding.remote_peer => {}
        V2Envelope::Session {
            frame: V2SessionFrame::Error { retryable, message },
        } => {
            return Err(RemoteWatchError { retryable, message }.into());
        }
        other => {
            return Err(watch_protocol_error(format!(
                "remote does not support persistent watch protocol version {}: {other:?}; upgrade flocal on both peers",
                sync::WATCH_PROTOCOL_VERSION
            )));
        }
    }
    read_watch_ready(state, share, remote_input, startup_deadline)?;
    write_watch_ready(remote_output, 0, &state.unsettled_paths(share)?)?;

    let mut remote_generation = 0u64;
    let mut round = 1u64;
    let mut completed_local = 0u64;
    let intent_generation = worker_state
        .map(|_| state.watch_intent_generation(share))
        .transpose()?;
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
        initial_request,
        intent_generation,
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
        if stop.is_some() && state.upgrade_pending()? {
            return Err(UpgradePending.into());
        }
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
                initial_request,
                intent_generation,
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
    initial_request: &mut Option<flocal::state::QueueRequest>,
    intent_generation: Option<i64>,
) -> Result<Vec<u8>> {
    let mut invalidation_cycle = InvalidationCycle::default();
    loop {
        let request = initial_request.take();
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
            watch_output,
            request,
            intent_generation,
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
    watch_output: &mut impl Write,
    existing: Option<QueueRequest>,
    intent_generation: Option<i64>,
) -> Result<ConnectorRound> {
    let peer = state
        .peer(share)?
        .context("no peer configured; run `flocal peer add`")?;
    let binding = connector_sync_binding(state, share, &peer)?;
    let mut wire = V2ReservationWire {
        round,
        input,
        output,
        pending_remote_generation: Some(pending_remote_generation),
        prefetched: None,
    };
    let (installation, _share_lock, _) = match binding.order {
        std::cmp::Ordering::Less => reserve_as_authority(
            state,
            share,
            &binding,
            SyncOperation::Watch,
            intent_generation,
            connector_generation,
            responder_generation,
            sync::SchedulingId::generate(),
            existing,
            &mut wire,
            watch_output,
        )?,
        std::cmp::Ordering::Greater => reserve_as_higher_peer(
            state,
            share,
            &binding,
            SyncOperation::Watch,
            intent_generation,
            connector_generation,
            responder_generation,
            true,
            existing,
            &mut wire,
            watch_output,
        )?,
        std::cmp::Ordering::Equal => unreachable!("peer ordering rejects equality"),
    };
    drop(wire);
    let mut installation = Some(installation);
    validate_final_sync_binding(state, share, &binding, intent_generation)?;
    state.clear_pending_objects(share)?;
    state.prune_unreferenced_objects()?;
    let mut budget =
        sync::RoundBudget::new(std::time::Instant::now() + sync::default_phase_deadline());
    budget.check()?;
    let local = sync::refresh_with_root(state, share, root)?;
    budget.check()?;
    let remote_records =
        read_connector_snapshot(input, round, &mut budget, pending_remote_generation)?;
    state
        .validate_remote_records(share, &local, &remote_records)
        .map_err(|error| watch_protocol_error(format!("{error:#}")))?;
    let mut plan = sync::plan(&local, &remote_records);
    ensure_connector_recovery_limits(state, share, &[])?;
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
    ensure_connector_recovery_limits(state, share, &applied_plan.plan.conflicts)?;

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
                installation
                    .take()
                    .expect("installation permit exists")
                    .finish()?;
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
            installation
                .take()
                .expect("installation permit exists")
                .finish()?;
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
    installation
        .take()
        .expect("installation permit exists")
        .finish()?;
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
        match sync::read_v2_envelope_in_phase(input, budget.phase_deadline()?)? {
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
    input: ChildStdin,
    output: TimedReader<ChildStdout>,
    stderr: Option<std::thread::JoinHandle<Vec<u8>>>,
    finished: bool,
}

struct RelationshipRemote {
    child: Option<Child>,
    input: Option<ChildStdin>,
    output: Option<ChildStdout>,
    stderr: Option<std::thread::JoinHandle<Vec<u8>>>,
    deadline: std::time::Instant,
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

struct SshProtocolChild {
    child: Child,
    input: ChildStdin,
    output: ChildStdout,
    stderr: std::thread::JoinHandle<Vec<u8>>,
}

fn spawn_ssh_protocol(host: &str, executable: &str, protocol: &str) -> Result<SshProtocolChild> {
    validate_host(host)?;
    validate_executable(executable)?;
    let command = format!("{executable} protocol {protocol}");
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
    let stderr = drain_bounded_stderr(child.stderr.take().context("ssh stderr unavailable")?);
    Ok(SshProtocolChild {
        child,
        input,
        output,
        stderr,
    })
}

fn drain_bounded_stderr(mut child_stderr: ChildStderr) -> std::thread::JoinHandle<Vec<u8>> {
    std::thread::spawn(move || {
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
    })
}

impl PersistentRemote {
    fn spawn(host: &str, executable: &str, child_slot: Arc<Mutex<Option<Child>>>) -> Result<Self> {
        let SshProtocolChild {
            child,
            input,
            output,
            stderr,
        } = spawn_ssh_protocol(host, executable, "serve")?;
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
        let SshProtocolChild {
            child,
            input,
            output,
            stderr,
        } = spawn_ssh_protocol(host, executable, "serve")?;
        Ok(Self {
            child,
            input,
            output: TimedReader::new(output),
            stderr: Some(stderr),
            finished: false,
        })
    }
    fn finish(mut self) -> Result<()> {
        match sync::read_v1_message_until(&self.output, self.output.session_deadline()?)? {
            Message::Done => {}
            other => bail!("expected synchronization completion, got {other:?}"),
        }
        let status = wait_protocol_child(
            &mut self.child,
            std::time::Instant::now() + Duration::from_secs(10),
            "ssh protocol process exceeded its exit deadline",
        )?;
        self.finished = true;
        let stderr = self
            .stderr
            .take()
            .and_then(|thread| thread.join().ok())
            .unwrap_or_default();
        if !status.success() {
            bail!(
                "ssh exited with {status}: {}",
                escaped(&String::from_utf8_lossy(&stderr))
            );
        }
        Ok(())
    }

    fn finish_after_error(mut self, error: anyhow::Error) -> anyhow::Error {
        let _ = self.child.kill();
        let status = self.child.wait();
        if status.is_ok() {
            self.finished = true;
        } else {
            return anyhow::anyhow!("{error:#}; failed to reap remote protocol process");
        }
        let stderr = self
            .stderr
            .take()
            .and_then(|thread| thread.join().ok())
            .unwrap_or_default();
        anyhow::anyhow!(
            "{error:#}; remote exited with {:?}: {}",
            status.ok(),
            escaped(&String::from_utf8_lossy(&stderr))
        )
    }
}

impl Drop for Remote {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        if let Some(stderr) = self.stderr.take() {
            let _ = stderr.join();
        }
    }
}

fn wait_protocol_child(
    child: &mut Child,
    deadline: std::time::Instant,
    deadline_error: &str,
) -> Result<std::process::ExitStatus> {
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Ok(status),
            Ok(None) => {}
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(error).context("checking protocol process status");
            }
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            bail!("{deadline_error}")
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

impl RelationshipRemote {
    fn spawn(host: &str, executable: &str, timeout: Duration) -> Result<Self> {
        let SshProtocolChild {
            child,
            input,
            output,
            stderr,
        } = spawn_ssh_protocol(host, executable, "relationship")?;
        Ok(Self {
            child: Some(child),
            input: Some(input),
            output: Some(output),
            stderr: Some(stderr),
            deadline: std::time::Instant::now() + timeout,
        })
    }

    fn finish(mut self) -> Result<()> {
        self.input.take();
        self.output.take();
        let mut child = self.child.take().context("ssh child is unavailable")?;
        let status = wait_protocol_child(
            &mut child,
            self.deadline,
            "relationship protocol process exceeded its deadline",
        )?;
        let stderr = self
            .stderr
            .take()
            .and_then(|thread| thread.join().ok())
            .unwrap_or_default();
        if !status.success() {
            bail!(
                "ssh exited with {status}: {}",
                escaped(&String::from_utf8_lossy(&stderr))
            )
        }
        Ok(())
    }

    fn finish_after_error(mut self, error: anyhow::Error) -> anyhow::Error {
        self.input.take();
        self.output.take();
        let status = self.child.take().map(|mut child| {
            let _ = child.kill();
            child.wait().ok()
        });
        let stderr = self
            .stderr
            .take()
            .and_then(|thread| thread.join().ok())
            .unwrap_or_default();
        anyhow::anyhow!(
            "{error:#}; remote exited with {:?}: {}",
            status.flatten(),
            escaped(&String::from_utf8_lossy(&stderr))
        )
    }
}

impl Drop for RelationshipRemote {
    fn drop(&mut self) {
        self.input.take();
        self.output.take();
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(stderr) = self.stderr.take() {
            let _ = stderr.join();
        }
    }
}

struct TimedReader<R> {
    inner: R,
    started: std::time::Instant,
}

struct DirectReader<'a, R: AsFd>(&'a R);

impl<R: AsFd> AsFd for DirectReader<'_, R> {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        self.0.as_fd()
    }
}

impl<R: AsFd> Read for DirectReader<'_, R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        loop {
            match rustix::io::read(self.0, &mut *buffer) {
                Ok(count) => return Ok(count),
                Err(rustix::io::Errno::INTR) => {}
                Err(error) => return Err(io::Error::from(error)),
            }
        }
    }
}

struct DirectWriter<'a, W: AsFd>(&'a W);

impl<W: AsFd> AsFd for DirectWriter<'_, W> {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        self.0.as_fd()
    }
}

impl<W: AsFd> Write for DirectWriter<'_, W> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        loop {
            match rustix::io::write(self.0, buffer) {
                Ok(count) => return Ok(count),
                Err(rustix::io::Errno::INTR) => {}
                Err(error) => return Err(io::Error::from(error)),
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<R> TimedReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            started: std::time::Instant::now(),
        }
    }

    fn session_deadline(&self) -> Result<std::time::Instant> {
        self.started
            .checked_add(max_peer_session_duration())
            .context("configured peer protocol duration exceeds the supported range")
    }
}

impl<R: AsFd> AsFd for TimedReader<R> {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        self.inner.as_fd()
    }
}

impl<R: Read + AsFd> Read for TimedReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        use rustix::event::{PollFd, PollFlags, Timespec, poll};
        let total = max_peer_session_duration();
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

fn max_peer_session_duration() -> Duration {
    Duration::from_secs(
        std::env::var("FLOCAL_MAX_SESSION_SECONDS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(60 * 60),
    )
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

    #[cfg(feature = "e2e-test-hooks")]
    #[test]
    fn e2e_recovery_delay_claim_survives_a_killed_process() -> Result<()> {
        let temp = tempdir()?;
        let state = State::open(temp.path().join("state"))?;
        assert!(sync::e2e_claim_install_recovery_delay(&state)?.is_none());

        let marker = state.dir.join(".e2e-delay-install-recovery");
        let claimed = state.dir.join(".e2e-delay-install-recovery-claimed");
        std::fs::write(&marker, b"")?;
        assert_eq!(
            sync::e2e_claim_install_recovery_delay(&state)?,
            Some(claimed.clone())
        );
        assert!(!marker.exists());
        assert!(claimed.exists());
        assert_eq!(
            sync::e2e_claim_install_recovery_delay(&state)?,
            Some(claimed.clone())
        );

        std::fs::remove_file(&claimed)?;
        std::fs::write(&marker, b"duration")?;
        assert!(sync::e2e_claim_install_recovery_delay(&state).is_err());
        std::fs::remove_file(&claimed)?;
        std::fs::create_dir(&marker)?;
        assert!(sync::e2e_claim_install_recovery_delay(&state).is_err());
        Ok(())
    }

    #[cfg(all(feature = "e2e-test-hooks", unix))]
    #[test]
    fn e2e_reservation_stop_files_are_one_shot_and_no_follow() -> Result<()> {
        let temp = tempdir()?;
        let state = State::open(temp.path().join("state"))?;
        let marker = state.dir.join(".e2e-stop-before-reservation");
        let pidfile = state.dir.join(".e2e-reservation-stop.pid");
        assert!(!e2e_claim_reservation_stop(&state)?);

        std::fs::write(&marker, b"")?;
        assert!(e2e_claim_reservation_stop(&state)?);
        assert!(!e2e_claim_reservation_stop(&state)?);

        std::os::unix::fs::symlink(temp.path().join("missing"), &marker)?;
        assert!(e2e_claim_reservation_stop(&state).is_err());
        std::fs::remove_file(state.dir.join(".e2e-stop-before-reservation-claimed"))?;

        e2e_publish_reservation_stop_pid(&state)?;
        assert!(e2e_publish_reservation_stop_pid(&state).is_err());
        std::fs::remove_file(&pidfile)?;
        std::os::unix::fs::symlink(temp.path().join("pid-target"), &pidfile)?;
        assert!(e2e_publish_reservation_stop_pid(&state).is_err());
        Ok(())
    }

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

    fn test_connector(peer: &str) -> PeerConfig {
        PeerConfig {
            peer_id: Some(flocal::model::PeerId(peer.into())),
            relationship: None,
            host: "test-peer".into(),
            remote_path: b"/remote".to_vec(),
            executable: "/bin/false".into(),
        }
    }

    fn test_state_with_peer_id(path: &Path, peer: &str) -> Result<State> {
        let state = State::open(path)?;
        state.peer_id()?;
        drop(state);
        let database = rusqlite::Connection::open(path.join("state.sqlite3"))?;
        database.execute(
            "UPDATE installation SET peer_id=?1 WHERE singleton=1",
            [peer],
        )?;
        drop(database);
        State::open(path)
    }

    #[test]
    fn legacy_relationship_derivation_is_stable_and_valid() -> Result<()> {
        let first = legacy_relationship_id(&ShareId("share-fixed-vector".into()))?;
        assert_eq!(
            first.0,
            "legacy-77f944f03349560bdb455f84f8416400735f0c06b2b7af8e2682f1fc870066a6"
        );
        first.validate()?;
        assert_eq!(
            first,
            legacy_relationship_id(&ShareId("share-fixed-vector".into()))?
        );
        assert_ne!(
            first,
            legacy_relationship_id(&ShareId("share-other".into()))?
        );
        Ok(())
    }

    #[test]
    fn sync_binding_distinguishes_legacy_from_incomplete_and_reserved_state() -> Result<()> {
        fn connector_state(
            path: &Path,
            relationship: Option<RelationshipId>,
            completed: bool,
        ) -> Result<(State, ShareId, PeerConfig)> {
            let root = path.join("root");
            std::fs::create_dir_all(&root)?;
            let mut state = test_state_with_peer_id(&path.join("state"), "local")?;
            let share = state.init_share(&root)?;
            let mut peer = test_connector("remote");
            peer.relationship = relationship;
            if !completed {
                peer.peer_id = None;
            }
            state.set_peer(&share, &peer)?;
            Ok((state, share, peer))
        }

        let temp = tempdir()?;
        let (legacy_state, legacy_share, legacy_peer) =
            connector_state(&temp.path().join("legacy"), None, true)?;
        let derived = legacy_relationship_id(&legacy_share)?;
        let binding = connector_sync_binding(&legacy_state, &legacy_share, &legacy_peer)?;
        assert_eq!(binding.remote_peer, PeerId("remote".into()));
        assert_eq!(binding.relationship, derived);
        assert!(
            validate_sync_binding(
                &legacy_state,
                &legacy_share,
                &PeerId("wrong".into()),
                &derived,
            )
            .is_err()
        );
        assert!(
            validate_sync_binding(
                &legacy_state,
                &legacy_share,
                &PeerId("remote".into()),
                &RelationshipId::parse("legacy-arbitrary".into())?,
            )
            .is_err()
        );

        let explicit = RelationshipId::generate();
        let (explicit_state, explicit_share, explicit_peer) =
            connector_state(&temp.path().join("explicit"), Some(explicit.clone()), true)?;
        assert_eq!(
            connector_sync_binding(&explicit_state, &explicit_share, &explicit_peer)?.relationship,
            explicit
        );
        assert!(
            validate_sync_binding(
                &explicit_state,
                &explicit_share,
                &PeerId("remote".into()),
                &legacy_relationship_id(&explicit_share)?,
            )
            .is_err()
        );

        let (prepared_state, prepared_share, prepared_peer) = connector_state(
            &temp.path().join("prepared"),
            Some(RelationshipId::generate()),
            false,
        )?;
        assert!(connector_sync_binding(&prepared_state, &prepared_share, &prepared_peer).is_err());

        let (reserved_state, reserved_share, reserved_peer) = connector_state(
            &temp.path().join("reserved"),
            Some(RelationshipId::parse("legacy-hostile-state".into())?),
            true,
        )?;
        assert!(connector_sync_binding(&reserved_state, &reserved_share, &reserved_peer).is_err());

        let unpaired_root = temp.path().join("unpaired-root");
        std::fs::create_dir(&unpaired_root)?;
        let unpaired = State::open(temp.path().join("unpaired-state"))?;
        let unpaired_share = unpaired.init_share(&unpaired_root)?;
        assert!(
            validate_sync_binding(
                &unpaired,
                &unpaired_share,
                &PeerId("remote".into()),
                &legacy_relationship_id(&unpaired_share)?,
            )
            .is_err()
        );

        let responder_root = temp.path().join("responder-root");
        let responder_state_path = temp.path().join("responder-state");
        std::fs::create_dir(&responder_root)?;
        let mut responder = test_state_with_peer_id(&responder_state_path, "responder")?;
        let responder_share = responder.init_share(&responder_root)?;
        let registered = RelationshipId::generate();
        responder.register_relationship(
            &responder_share,
            &responder_root,
            &PeerId("connector".into()),
            &registered,
        )?;
        assert_eq!(
            validate_sync_binding(
                &responder,
                &responder_share,
                &PeerId("connector".into()),
                &registered,
            )?
            .relationship,
            registered
        );
        assert!(
            validate_sync_binding(
                &responder,
                &responder_share,
                &PeerId("connector".into()),
                &legacy_relationship_id(&responder_share)?,
            )
            .is_err()
        );
        drop(responder);
        let database = rusqlite::Connection::open(responder_state_path.join("state.sqlite3"))?;
        database.execute(
            "UPDATE shares SET bound_relationship='legacy-hostile-state' WHERE share_id=?1",
            [&responder_share.0],
        )?;
        drop(database);
        let responder = State::open(&responder_state_path)?;
        assert!(
            validate_sync_binding(
                &responder,
                &responder_share,
                &PeerId("connector".into()),
                &RelationshipId::parse("legacy-hostile-state".into())?,
            )
            .is_err()
        );
        drop(responder);
        let database = rusqlite::Connection::open(responder_state_path.join("state.sqlite3"))?;
        database.execute(
            "UPDATE shares SET bound_relationship=NULL WHERE share_id=?1",
            [&responder_share.0],
        )?;
        drop(database);
        let mut responder = State::open(&responder_state_path)?;
        let responder_legacy = legacy_relationship_id(&responder_share)?;
        assert!(
            validate_sync_binding(
                &responder,
                &responder_share,
                &PeerId("connector".into()),
                &RelationshipId::parse("legacy-arbitrary".into())?,
            )
            .is_err()
        );
        let binding = validate_sync_binding(
            &responder,
            &responder_share,
            &PeerId("connector".into()),
            &responder_legacy,
        )?;
        assert_eq!(binding.remote_peer, PeerId("connector".into()));
        assert_eq!(binding.relationship, responder_legacy);

        let original_root = temp.path().join("responder-root-original");
        std::fs::rename(&responder_root, &original_root)?;
        std::fs::create_dir(&responder_root)?;
        assert!(
            validate_sync_binding(
                &responder,
                &responder_share,
                &PeerId("connector".into()),
                &responder_legacy,
            )
            .is_err()
        );
        std::fs::remove_dir(&responder_root)?;
        std::fs::rename(&original_root, &responder_root)?;

        assert!(matches!(
            responder.prepare_incoming_removal(
                &responder_share,
                &PeerId("connector".into()),
                &responder_legacy,
            )?,
            flocal::state::IncomingRemoval::Prepared(_)
        ));
        assert!(
            validate_sync_binding(
                &responder,
                &responder_share,
                &PeerId("connector".into()),
                &responder_legacy,
            )
            .is_err()
        );
        Ok(())
    }

    #[derive(Default)]
    struct TestReservationWire {
        incoming: std::collections::VecDeque<ReservationFrame>,
        outgoing: Vec<ReservationFrame>,
    }

    impl ReservationWire for TestReservationWire {
        fn send_reservation(
            &mut self,
            frame: ReservationFrame,
            _deadline: std::time::Instant,
        ) -> Result<()> {
            self.outgoing.push(frame);
            Ok(())
        }

        fn recv_reservation(&mut self, _deadline: std::time::Instant) -> Result<ReservationFrame> {
            self.incoming.pop_front().context("test wire is empty")
        }
    }

    struct FailingSendWire;

    impl ReservationWire for FailingSendWire {
        fn send_reservation(
            &mut self,
            _frame: ReservationFrame,
            _deadline: std::time::Instant,
        ) -> Result<()> {
            bail!("test wire closed")
        }

        fn recv_reservation(&mut self, _deadline: std::time::Instant) -> Result<ReservationFrame> {
            bail!("test wire closed")
        }
    }

    #[test]
    fn scheduling_views_distinguish_local_predecessors_active_owners_and_peer_waits() -> Result<()>
    {
        let temp = tempdir()?;
        let mut state = State::open(temp.path().join("state"))?;
        let roots = [
            temp.path().join("one"),
            temp.path().join("two"),
            temp.path().join("three"),
        ];
        for root in &roots {
            std::fs::create_dir(root)?;
        }
        let roots = [
            roots[0].canonicalize()?,
            roots[1].canonicalize()?,
            roots[2].canonicalize()?,
        ];
        let first = state.init_share(&roots[0])?;
        let second = state.init_share(&roots[1])?;
        let third = state.init_share(&roots[2])?;
        let mut owner = state.enqueue_sync(Some(&first), SyncOperation::Sync, None)?;
        let owner = owner
            .try_activate()?
            .context("first request did not activate")?;
        let second_request = state.enqueue_sync(Some(&second), SyncOperation::Watch, None)?;
        let snapshot = state.scheduling_snapshot()?;
        let waiting = share_scheduling_view(&state, &snapshot, &second)?;
        assert_eq!(waiting.state, "queued");
        assert!(
            matches!(waiting.blocker, Some(SchedulingBlocker::Local(Some(ref root))) if root == &roots[0])
        );
        assert_eq!(
            share_scheduling_view(&state, &snapshot, &first)?.state,
            "active"
        );
        owner.finish()?;

        let third_request = state.enqueue_sync(Some(&third), SyncOperation::Sync, None)?;
        let snapshot = state.scheduling_snapshot()?;
        let waiting = share_scheduling_view(&state, &snapshot, &third)?;
        assert!(
            matches!(waiting.blocker, Some(SchedulingBlocker::Local(Some(ref root))) if root == &roots[1])
        );
        second_request.cancel()?;
        third_request.cancel()?;

        let peer_wait = state.enqueue_pending_authority(
            &third,
            &RelationshipId::generate(),
            SyncOperation::Sync,
            None,
        )?;
        let snapshot = state.scheduling_snapshot()?;
        let waiting = share_scheduling_view(&state, &snapshot, &third)?;
        assert!(matches!(waiting.blocker, Some(SchedulingBlocker::Peer)));
        drop(peer_wait);
        Ok(())
    }

    #[test]
    fn remote_cleanup_is_bounded_and_drop_reaps_unfinished_children() -> Result<()> {
        let mut timed = Command::new("sh").arg("-c").arg("sleep 30").spawn()?;
        let started = std::time::Instant::now();
        let error = wait_protocol_child(
            &mut timed,
            std::time::Instant::now() + Duration::from_millis(20),
            "ssh protocol process exceeded its exit deadline",
        )
        .expect_err("an unresponsive protocol child must exceed its exit deadline");
        assert!(error.to_string().contains("exit deadline"));
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(timed.try_wait()?.is_some());

        let mut child = Command::new("sh")
            .arg("-c")
            .arg("sleep 30")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let pid = child.id();
        let input = child.stdin.take().context("test child stdin")?;
        let output = child.stdout.take().context("test child stdout")?;
        let stderr = drain_bounded_stderr(child.stderr.take().context("test child stderr")?);
        drop(Remote {
            child,
            input,
            output: TimedReader::new(output),
            stderr: Some(stderr),
            finished: false,
        });
        let status = Command::new("kill")
            .args(["-0", &pid.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()?;
        assert!(!status.success(), "Remote::drop left child {pid} alive");
        Ok(())
    }

    #[test]
    fn reservation_helpers_bound_progress_and_activate_a_prepared_pair() -> Result<()> {
        let mut resets = 0;
        for _ in 0..MAX_PAIR_RESETS {
            note_pair_reset(&mut resets)?;
        }
        assert!(note_pair_reset(&mut resets).is_err());

        let queued = |position| {
            ReservationFrame::Queued(sync::SyncQueued {
                waiting_on: sync::WaitingOn::Peer,
                position: sync::CoarseQueuePosition::from_exact(position).unwrap(),
            })
        };
        let mut wire = TestReservationWire {
            incoming: [
                queued(1),
                queued(1),
                queued(2),
                ReservationFrame::PendingAuthority(sync::PendingAuthority {
                    id: sync::SchedulingId::generate(),
                    connector_generation: 1,
                    responder_generation: 2,
                }),
            ]
            .into(),
            outgoing: Vec::new(),
        };
        let mut output = Vec::new();
        assert!(matches!(
            recv_reservation_ignoring_progress(&mut wire, None, &mut output)?,
            ReservationFrame::PendingAuthority(_)
        ));
        assert_eq!(
            String::from_utf8(output)?
                .matches("Waiting for the peer")
                .count(),
            1
        );

        let mut flooding = TestReservationWire {
            incoming: (0..9).map(|_| queued(1)).collect(),
            outgoing: Vec::new(),
        };
        assert!(recv_reservation_ignoring_progress(&mut flooding, None, &mut Vec::new()).is_err());

        let temp = tempdir()?;
        let mut state = State::open(temp.path().join("state"))?;
        let share = ShareId::generate();
        let relationship = RelationshipId::generate();
        let authority = state.peer_id()?;
        let nonce = sync::SchedulingNonce::generate();
        let predecessor = empty_predecessor();
        let (request, order) = state.enqueue_authoritative_sync(
            &share,
            &relationship,
            SyncOperation::Sync,
            None,
            &authority,
            nonce.as_str(),
            predecessor.as_str(),
        )?;
        let order = sync::NetworkOrder::new(order as u64)?;
        let mut wire = TestReservationWire::default();
        send_local_queue_position(&mut state, &request, &mut wire, scheduling_deadline())?;
        let prepared = prepare_local_pair(
            &mut state,
            &request,
            &relationship,
            &authority,
            order,
            &nonce,
            &mut wire,
        )?;
        assert!(state.commit_paired_sync(
            request.token(),
            &relationship,
            &authority,
            order.get() as i64,
            nonce.as_str(),
            &prepared.stored,
        )?);
        wait_for_paired_permit(request, &share, &mut wire, None)?.finish()?;
        recover_before_pair(&mut state, &mut wire)?;
        assert!(matches!(
            wire.outgoing.as_slice(),
            [ReservationFrame::Queued(_)]
        ));

        let mut blocker = state.enqueue_sync(None, SyncOperation::Maintenance, None)?;
        let blocker = blocker
            .try_activate()?
            .context("test blocker did not activate")?;
        let error = recover_before_pair_with_interval(
            &mut state,
            &mut FailingSendWire,
            Duration::from_millis(1),
        )
        .expect_err("wire failure must stop queued recovery");
        assert!(error.to_string().contains("test wire closed"));
        assert!(state.scheduling_snapshot()?.queued.is_empty());
        blocker.finish()?;
        Ok(())
    }

    #[cfg(unix)]
    fn serve_test_daemon(
        state_dir: &Path,
        requests: usize,
    ) -> Result<std::thread::JoinHandle<Result<()>>> {
        use std::os::unix::fs::PermissionsExt;

        let run = state_dir.join("run");
        std::fs::create_dir_all(&run)?;
        std::fs::set_permissions(&run, std::fs::Permissions::from_mode(0o700))?;
        let socket = run.join("daemon.sock");
        match std::fs::remove_file(&socket) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let listener = std::os::unix::net::UnixListener::bind(socket)?;
        let state_dir = state_dir.to_path_buf();
        Ok(std::thread::spawn(move || {
            let mut state = State::open(state_dir)?;
            let workers = Arc::new(Mutex::new(std::collections::HashMap::new()));
            let lifecycle = Arc::new(Mutex::new(()));
            let (events, _event_rx) = std::sync::mpsc::channel();
            for stream in listener.incoming().take(requests) {
                handle_daemon_request(&mut state, &workers, &events, &lifecycle, stream?)?;
            }
            Ok(())
        }))
    }

    #[test]
    fn relationship_handler_is_versioned_bound_and_idempotent() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path().join("remote-root");
        let mut state = State::open(temp.path().join("state"))?;
        let share = ShareId("share-relationship-handler".into());
        let connector = flocal::model::PeerId("peer-connector".into());
        let relationship = flocal::model::RelationshipId::generate();

        let register = |protocol| RelationshipRequest::RegisterRelationship {
            registration_protocol: protocol,
            share: share.clone(),
            peer: connector.clone(),
            root: path_bytes(&root),
            relationship: relationship.clone(),
        };
        assert!(matches!(
            handle_relationship_request(
                &mut state,
                register(sync::RELATIONSHIP_REGISTRATION_PROTOCOL_VERSION + 1)
            )?,
            RelationshipResponse::Register(RegisterRelationshipResponse::Error { .. })
        ));
        assert!(matches!(
            handle_relationship_request(
                &mut state,
                register(sync::RELATIONSHIP_REGISTRATION_PROTOCOL_VERSION)
            )?,
            RelationshipResponse::Register(RegisterRelationshipResponse::Registered { .. })
        ));
        assert!(matches!(
            handle_relationship_request(
                &mut state,
                register(sync::RELATIONSHIP_REGISTRATION_PROTOCOL_VERSION)
            )?,
            RelationshipResponse::Register(RegisterRelationshipResponse::Registered { .. })
        ));

        let local_peer = state.peer_id()?;
        let remove = |expected_peer, peer| RelationshipRequest::RemoveRelationship {
            removal_protocol: sync::RELATIONSHIP_REMOVAL_PROTOCOL_VERSION,
            share: share.clone(),
            peer,
            expected_peer,
            relationship: relationship.clone(),
        };
        assert!(matches!(
            handle_relationship_request(
                &mut state,
                remove(
                    flocal::model::PeerId("wrong-endpoint".into()),
                    connector.clone()
                )
            )?,
            RelationshipResponse::Remove(RemoveRelationshipResponse::Error { .. })
        ));
        assert!(matches!(
            handle_relationship_request(
                &mut state,
                remove(
                    local_peer.clone(),
                    flocal::model::PeerId("wrong-connector".into())
                )
            )?,
            RelationshipResponse::Remove(RemoveRelationshipResponse::Error { .. })
        ));
        assert!(matches!(
            handle_relationship_request(&mut state, remove(local_peer.clone(), connector.clone()))?,
            RelationshipResponse::Remove(RemoveRelationshipResponse::Absent { .. })
        ));
        assert_eq!(state.endpoint_binding(&share)?, EndpointBinding::Unpaired);
        assert!(matches!(
            handle_relationship_request(&mut state, remove(local_peer, connector))?,
            RelationshipResponse::Remove(RemoveRelationshipResponse::Absent { .. })
        ));

        let bounded = bounded_relationship_error(&"é".repeat(5000));
        assert_eq!(bounded.len(), sync::MAX_RELATIONSHIP_ERROR_BYTES);
        assert_eq!(
            bounded_relationship_error("first\nsecond\rthird"),
            "first second third"
        );
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn relationship_cli_selectors_and_status_cover_every_durable_role() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path().join("root");
        let nested = root.join("nested");
        std::fs::create_dir_all(&nested)?;
        let mut state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;

        assert_eq!(select_share_for_removal(&state, Some(&root), None)?, share);
        assert_eq!(
            select_share_for_removal(&state, None, Some(&share.0))?,
            share
        );
        assert!(select_share_for_removal(&state, None, None).is_err());
        assert!(select_share_for_removal(&state, Some(&root), Some(&share.0)).is_err());
        assert!(select_share_for_removal(&state, Some(&nested), None).is_err());
        assert!(select_share_for_removal(&state, None, Some("bad share")).is_err());
        let link = temp.path().join("root-link");
        std::os::unix::fs::symlink(&root, &link)?;
        assert!(select_share_for_removal(&state, Some(&link), None).is_err());

        status(&mut state, &root, false)?;
        status(&mut state, &root, true)?;
        remove_sync_relationship(&mut state, Some(&root), None, false, true)?;

        let prepared = state.prepare_connector_registration(
            &share,
            &EndpointBinding::Unpaired,
            "host",
            b"/remote",
            "/flocal",
        )?;
        status(&mut state, &root, false)?;
        assert!(remove_sync_relationship(&mut state, Some(&root), None, false, true).is_err());
        assert!(prepared.peer_id.is_none());
        let daemon = serve_test_daemon(&state.dir, 2)?;
        remove_sync_relationship(&mut state, Some(&root), None, true, true)?;
        daemon.join().expect("test daemon joins")?;

        let responder = flocal::model::PeerId("peer-responder-view".into());
        state.register_relationship(
            &share,
            &root,
            &responder,
            &flocal::model::RelationshipId::generate(),
        )?;
        status(&mut state, &root, false)?;
        assert!(remove_sync_relationship(&mut state, None, Some(&share.0), false, true).is_err());
        let binding = state.endpoint_binding(&share)?;
        let removal = state.prepare_removal(&share, &binding)?;
        state.set_removal_diagnostic(&share, &removal.relationship, "offline")?;
        state.set_install_intent(&share, &[])?;
        status(&mut state, &root, false)?;
        status(&mut state, &root, true)?;
        let syncs = daemon_syncs(
            &mut state,
            &Arc::new(Mutex::new(std::collections::HashMap::new())),
        )?;
        assert_eq!(syncs[0].state, "removing");
        let daemon = serve_test_daemon(&state.dir, 2)?;
        remove_sync_relationship(&mut state, None, Some(&share.0), true, true)?;
        daemon.join().expect("test daemon joins")?;
        Ok(())
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
        let error = read_watch_ready(
            &mut state,
            &share,
            &reader,
            std::time::Instant::now() + sync::default_frame_deadline(),
        )
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
        let error = read_watch_ready(
            &mut state,
            &share,
            &reader,
            std::time::Instant::now() + sync::default_frame_deadline(),
        )
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
        assert!(messages.is_empty());
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
    fn recovery_size_and_command_grammar_are_strict() -> Result<()> {
        assert_eq!(format_bytes(0), "0 bytes");
        assert_eq!(format_bytes(1024), "1.0 KiB");
        assert_eq!(parse_recovery_size("1KiB")?, 1024);
        assert_eq!(parse_recovery_size("2MiB")?, 2 * 1024 * 1024);
        assert_eq!(parse_recovery_size("3GiB")?, 3 * 1024 * 1024 * 1024);
        assert_eq!(parse_recovery_size("42")?, 42);
        for invalid in ["", "0", "1KB", "1.5GiB", " 1", "1 ", "-1"] {
            assert!(
                parse_recovery_size(invalid).is_err(),
                "accepted {invalid:?}"
            );
        }
        assert!(validate_share_id("share-safe_1").is_ok());
        assert!(validate_share_id("bad;command").is_err());
        assert!(
            Cli::try_parse_from(["flocal", "conflicts", "budget", "/tmp/root", "11GiB"]).is_ok()
        );
        assert!(
            Cli::try_parse_from([
                "flocal",
                "conflicts",
                "budget",
                "--share",
                "share-safe",
                "11GiB",
                "--json"
            ])
            .is_ok()
        );
        assert!(
            Cli::try_parse_from([
                "flocal",
                "conflicts",
                "prune",
                "/tmp/root",
                "c-123",
                "--selection",
                "abc",
                "--yes"
            ])
            .is_ok()
        );
        Ok(())
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
        assert!(is_terminal_watch_error(
            &flocal::state::RecoveryLimitExceeded {
                kind: flocal::state::RecoveryLimitKind::BudgetBytes,
                current: 0,
                projected: 2,
                limit: 1,
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
    #[cfg(feature = "e2e-test-hooks")]
    fn recovery_cleanup_failure_preserves_the_terminal_limit_type() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path().join("root");
        std::fs::create_dir(&root)?;
        let state_dir = temp.path().join("state");
        let state = State::open(&state_dir)?;
        let share = state.init_share(&root)?;
        let hash = flocal::model::ObjectHash::from_blake3(blake3::hash(b"large"));
        let conflict = flocal::reconcile::Conflict::whole_file(
            test_record(
                b"large",
                "winner",
                Entry::File {
                    hash,
                    size: flocal::state::DEFAULT_RECOVERY_BUDGET_BYTES,
                    executable: false,
                },
            ),
            test_record(b"large", "loser", Entry::Tombstone),
            flocal::merge::FallbackReason::AbsentBase,
        );
        std::fs::write(state_dir.join(".e2e-collector-fail"), b"1")?;
        let error = ensure_connector_recovery_limits(&state, &share, &[conflict])
            .expect_err("recovery admission must fail");
        assert!(is_terminal_watch_error(&error));
        assert!(
            error
                .downcast_ref::<flocal::state::RecoveryLimitExceeded>()
                .is_some()
        );
        assert!(error.to_string().contains("cleanup also failed"));
        Ok(())
    }

    #[test]
    fn persistent_responder_rejects_setup_failures_with_typed_frames() -> Result<()> {
        use std::os::unix::net::UnixStream;

        let temp = tempdir()?;
        let root = temp.path().join("watch-root");
        std::fs::create_dir(&root)?;
        let mut state = State::open(temp.path().join("watch-state"))?;
        let share = ShareId("share-watch-setup".into());
        let bound_peer = flocal::model::PeerId("bound-peer".into());
        let relationship = flocal::model::RelationshipId::generate();
        state.register_relationship(&share, &root, &bound_peer, &relationship)?;

        let reject = |state: &mut State, protocol, peer| -> Result<V2SessionFrame> {
            let (server_input, _client_output) = UnixStream::pair()?;
            let (server_output, client_input) = UnixStream::pair()?;
            serve_watch_open(
                state,
                protocol,
                share.clone(),
                peer,
                relationship.clone(),
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
        let share_lock = state.lock_share_session(&share)?;
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
        let share = ShareId("share-idle".into());
        let peer = flocal::model::PeerId("idle-peer".into());
        let relationship = flocal::model::RelationshipId::generate();
        state.register_relationship(&share, &root, &peer, &relationship)?;
        let (client_output, server_input) = UnixStream::pair()?;
        let (server_output, client_input) = UnixStream::pair()?;
        let responder = std::thread::spawn(move || {
            serve_watch_open(
                &mut state,
                sync::WATCH_PROTOCOL_VERSION,
                share,
                peer,
                relationship,
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
        for (case, connector_id, responder_id) in [
            ("connector-authority", "peer-a", "peer-z"),
            ("responder-authority", "peer-z", "peer-a"),
        ] {
            let case_root = temp.path().join(case);
            let local_root = case_root.join("local");
            let remote_root = case_root.join("remote");
            std::fs::create_dir_all(&local_root)?;
            std::fs::create_dir_all(&remote_root)?;
            std::fs::write(local_root.join("from-local"), b"local")?;
            std::fs::write(remote_root.join("from-remote"), b"remote")?;
            let mut local_state =
                test_state_with_peer_id(&case_root.join("local-state"), connector_id)?;
            let mut remote_state =
                test_state_with_peer_id(&case_root.join("remote-state"), responder_id)?;
            let local_share = local_state.init_share(&local_root)?;
            let remote_share = local_share.clone();
            let connector_peer = local_state.peer_id()?;
            let remote_peer = remote_state.peer_id()?;
            let relationship = RelationshipId::generate();
            remote_state.register_relationship(
                &remote_share,
                &remote_root,
                &connector_peer,
                &relationship,
            )?;
            let mut peer = test_connector(&remote_peer.0);
            peer.relationship = Some(relationship.clone());
            local_state.set_peer(&local_share, &peer)?;
            let local_cap = sync::ShareRoot::open(&local_state, &local_share)?;
            let remote_cap = sync::ShareRoot::open(&remote_state, &remote_share)?;

            let (connector_stream, responder_reader) = UnixStream::pair()?;
            let (responder_stream, connector_reader) = UnixStream::pair()?;
            let responder = std::thread::spawn(move || -> Result<()> {
                let first = sync::read_v2_envelope_until(
                    &responder_reader,
                    std::time::Instant::now() + sync::default_frame_deadline(),
                )?;
                let V2Envelope::Round { round: 1, frame } = first else {
                    bail!("expected first reservation frame");
                };
                let binding = validate_sync_binding(
                    &remote_state,
                    &remote_share,
                    &connector_peer,
                    &relationship,
                )?;
                serve_v2_round(
                    &mut remote_state,
                    &remote_share,
                    &binding,
                    &remote_cap,
                    1,
                    frame,
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
                &mut Vec::new(),
                None,
                None,
            );
            if report.is_err() {
                drop(connector_stream);
                drop(connector_reader);
            }
            let responder_result = responder.join().expect("responder joins");
            let report = report.with_context(|| {
                format!(
                    "connector failed; responder result: {:?}",
                    responder_result.as_ref().err()
                )
            })?;
            responder_result?;

            assert_eq!(std::fs::read(local_root.join("from-remote"))?, b"remote");
            assert_eq!(std::fs::read(remote_root.join("from-local"))?, b"local");
            let ConnectorRound::Completed(report) = report else {
                bail!("test round unexpectedly invalidated");
            };
            let report = String::from_utf8(report)?;
            assert!(report.contains("UPLOAD from-local"), "{report}");
            assert!(report.contains("DOWNLOAD from-remote"), "{report}");
        }
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
        let relationship = RelationshipId::generate();
        let mut peer = test_connector("zzzz-remote");
        peer.relationship = Some(relationship);
        state.set_peer(&share, &peer)?;
        let root = sync::ShareRoot::open(&state, &share)?;
        let mut first = test_record(b"duplicate", "foreign", Entry::Directory);
        let mut second = first.clone();
        first.version.sequence = 8;
        second.version.sequence = 9;

        let (connector_stream, responder_reader) = UnixStream::pair()?;
        let (responder_stream, connector_reader) = UnixStream::pair()?;
        let responder = std::thread::spawn(move || -> Result<()> {
            let budget = sync::RoundBudget::new(std::time::Instant::now() + Duration::from_secs(1));
            let issue =
                match sync::read_v2_envelope_until(&responder_reader, budget.frame_deadline()?)? {
                    V2Envelope::Round {
                        round: 1,
                        frame: V2RoundFrame::ProxyIssue(issue),
                    } => issue,
                    other => bail!("expected proxy issue, got {other:?}"),
                };
            write_v2_round(
                &responder_stream,
                1,
                V2RoundFrame::ProxyAck(sync::ProxyAck {
                    id: issue.id.clone(),
                    network_order: issue.network_order,
                }),
                &budget,
            )?;
            let prepare =
                match sync::read_v2_envelope_until(&responder_reader, budget.frame_deadline()?)? {
                    V2Envelope::Round {
                        round: 1,
                        frame: V2RoundFrame::PairPrepare(checkpoint),
                    } => checkpoint,
                    other => bail!("expected pair prepare, got {other:?}"),
                };
            write_v2_round(
                &responder_stream,
                1,
                V2RoundFrame::PairPrepareAck(prepare.clone()),
                &budget,
            )?;
            let commit = sync::read_v2_envelope_until(&responder_reader, budget.frame_deadline()?)?;
            assert!(matches!(
                commit,
                V2Envelope::Round {
                    round: 1,
                    frame: V2RoundFrame::PairCommit(ref checkpoint),
                } if checkpoint == &prepare
            ));
            write_v2_round(
                &responder_stream,
                1,
                V2RoundFrame::PairCommitAck(prepare),
                &budget,
            )?;
            let reservation =
                match sync::read_v2_envelope_until(&responder_reader, budget.frame_deadline()?)? {
                    V2Envelope::Round {
                        round: 1,
                        frame: V2RoundFrame::SyncReserved(reservation),
                    } => reservation,
                    other => bail!("expected sync reservation, got {other:?}"),
                };
            write_v2_round(
                &responder_stream,
                1,
                V2RoundFrame::SyncStart(sync::SyncStart {
                    id: reservation.id.clone(),
                    network_order: reservation.network_order,
                    nonce: reservation.nonce.clone(),
                    connector_generation: 0,
                    responder_generation: 0,
                }),
                &budget,
            )?;
            assert!(matches!(
                sync::read_v2_envelope_until(&responder_reader, budget.frame_deadline()?)?,
                V2Envelope::Round {
                    round: 1,
                    frame: V2RoundFrame::SyncAccepted(accepted),
                } if accepted == reservation
            ));
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
            &mut Vec::new(),
            None,
            None,
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
        let remote_share = local_share.clone();
        let remote_peer = remote_state.peer_id()?;
        let connector_peer = local_state.peer_id()?;
        let relationship = RelationshipId::generate();
        remote_state.register_relationship(
            &remote_share,
            &remote_root,
            &connector_peer,
            &relationship,
        )?;
        let mut configured_peer = test_connector(&remote_peer.0);
        configured_peer.relationship = Some(relationship.clone());
        local_state.set_peer(&local_share, &configured_peer)?;
        let local_cap = sync::ShareRoot::open(&local_state, &local_share)?;
        let remote_cap = sync::ShareRoot::open(&remote_state, &remote_share)?;
        let expected_remote_peer = remote_peer.clone();
        let local_seed = flocal::model::RelativePath::from_bytes(b"local-seed".to_vec())?;
        let remote_seed = flocal::model::RelativePath::from_bytes(b"remote-seed".to_vec())?;
        local_state.remember_unsettled_path(&local_share, &local_seed)?;
        let expected_local_seed = local_seed.clone();
        let expected_remote_seed = remote_seed.clone();
        let responder_relationship = relationship.clone();

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
            read_watch_ready(
                &mut remote_state,
                &remote_share,
                &responder_input,
                std::time::Instant::now() + sync::default_phase_deadline(),
            )?;
            let unsettled = remote_state.unsettled_paths(&remote_share)?;
            assert!(unsettled.contains(&expected_local_seed));
            assert!(unsettled.contains(&expected_remote_seed));
            let first = sync::read_v2_envelope_until(
                &responder_input,
                std::time::Instant::now() + sync::default_frame_deadline(),
            )?;
            let V2Envelope::Round { round: 1, frame } = first else {
                bail!("expected first reservation frame");
            };
            let binding = validate_sync_binding(
                &remote_state,
                &remote_share,
                &connector_peer,
                &responder_relationship,
            )?;
            serve_v2_round(
                &mut remote_state,
                &remote_share,
                &binding,
                &remote_cap,
                1,
                frame,
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
            peer_id: Some(expected_remote_peer),
            relationship: Some(relationship),
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
            &mut None,
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
            Message::Error { .. }
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
                relationship: RelationshipId::generate(),
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
            relationship: RelationshipId::generate(),
            dry_run: true,
        })?;
        result?;
        assert!(matches!(
            sync::read_message(&mut output.as_slice())?,
            Message::Error { .. }
        ));
        assert!(initial_message(Message::Done)?.0.is_err());
        Ok(())
    }

    #[test]
    fn initial_protocol_runs_bound_dry_sync() -> Result<()> {
        use std::os::unix::net::UnixStream;

        let temp = tempdir()?;
        let root = temp.path().join("root");
        std::fs::create_dir_all(&root)?;
        let mut state = State::open(temp.path().join("state"))?;
        let share = ShareId("share-bound".into());
        let peer = flocal::model::PeerId("zzzz-peer-bound".into());
        let relationship = flocal::model::RelationshipId::generate();
        state.register_relationship(&share, &root, &peer, &relationship)?;
        let (mut client, server) = UnixStream::pair()?;
        let server_input = server.try_clone()?;
        let server_thread = std::thread::spawn(move || {
            let mut server_input = server_input;
            let mut server_output = server;
            serve_initial(
                &mut state,
                InitialMessage::Sync {
                    protocol: sync::PROTOCOL_VERSION,
                    share,
                    peer,
                    relationship,
                    dry_run: true,
                },
                &mut server_input,
                &mut server_output,
            )
        });
        let deadline = || std::time::Instant::now() + sync::default_frame_deadline();
        let id = sync::SchedulingId::generate();
        sync::write_v1_message_until(
            &client,
            &Message::PendingAuthority(sync::PendingAuthority {
                id: id.clone(),
                connector_generation: 0,
                responder_generation: 0,
            }),
            deadline(),
        )?;
        let issue = match sync::read_v1_message_until(&client, deadline())? {
            Message::ProxyIssue(issue) if issue.id == id => issue,
            other => bail!("expected proxy issue, got {other:?}"),
        };
        sync::write_v1_message_until(
            &client,
            &Message::ProxyAck(sync::ProxyAck {
                id: issue.id.clone(),
                network_order: issue.network_order,
            }),
            deadline(),
        )?;
        let checkpoint = match sync::read_v1_message_until(&client, deadline())? {
            Message::PairPrepare(checkpoint) => checkpoint,
            other => bail!("expected pair prepare, got {other:?}"),
        };
        sync::write_v1_message_until(
            &client,
            &Message::PairPrepareAck(checkpoint.clone()),
            deadline(),
        )?;
        assert!(matches!(
            sync::read_v1_message_until(&client, deadline())?,
            Message::PairCommit(ref commit) if commit == &checkpoint
        ));
        sync::write_v1_message_until(&client, &Message::PairCommitAck(checkpoint), deadline())?;
        let reservation = match sync::read_v1_message_until(&client, deadline())? {
            Message::SyncReserved(reservation) => reservation,
            other => bail!("expected synchronization reservation, got {other:?}"),
        };
        sync::write_v1_message_until(
            &client,
            &Message::SyncStart(sync::SyncStart {
                id: reservation.id.clone(),
                network_order: reservation.network_order,
                nonce: reservation.nonce.clone(),
                connector_generation: 0,
                responder_generation: 0,
            }),
            deadline(),
        )?;
        assert!(matches!(
            sync::read_v1_message_until(&client, deadline())?,
            Message::SyncAccepted(accepted) if accepted == reservation
        ));
        assert!(sync::read_snapshot(&mut client)?.is_empty());
        sync::write_message(&mut client, &Message::Cancel)?;
        assert!(matches!(
            sync::read_v1_message_until(&client, deadline())?,
            Message::Done
        ));
        server_thread.join().expect("server joins")?;
        Ok(())
    }

    #[test]
    fn legacy_registration_rejects_creation_and_rebinding() -> Result<()> {
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
            Message::Error { .. }
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
        let lifecycle = Arc::new(Mutex::new(()));
        let (events, _event_rx) = std::sync::mpsc::channel();
        let worker_copy = workers.clone();
        let thread = std::thread::spawn(move || {
            handle_daemon_request(&mut state, &worker_copy, &events, &lifecycle, server)
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
        state.set_peer(&share, &test_connector("peer-list"))?;
        state.set_watch_enabled(&share, true)?;
        state.set_blocked(&share, "repair required")?;
        let (server, mut client) = UnixStream::pair()?;
        let workers = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let lifecycle = Arc::new(Mutex::new(()));
        let (events, _event_rx) = std::sync::mpsc::channel();
        let worker_copy = workers.clone();
        let thread = std::thread::spawn(move || {
            handle_daemon_request(&mut state, &worker_copy, &events, &lifecycle, server)
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

    #[cfg(unix)]
    #[test]
    fn daemon_lifecycle_serializes_start_with_relationship_removal() -> Result<()> {
        fn concurrent_request(
            state_dir: PathBuf,
            workers: Arc<Mutex<std::collections::HashMap<ShareId, DaemonWorker>>>,
            events: std::sync::mpsc::Sender<WorkerEvent>,
            lifecycle: Arc<Mutex<()>>,
            barrier: Arc<std::sync::Barrier>,
            request: DaemonRequest,
        ) -> Result<DaemonResponse> {
            let mut state = State::open(state_dir)?;
            let (server, mut client) = UnixStream::pair()?;
            serde_json::to_writer(&mut client, &request)?;
            client.write_all(b"\n")?;
            barrier.wait();
            handle_daemon_request(&mut state, &workers, &events, &lifecycle, server)?;
            serde_json::from_slice(&read_daemon_message(&mut client)?).map_err(Into::into)
        }

        let temp = tempdir()?;
        let state_dir = temp.path().join("state");
        let root = temp.path().join("root");
        std::fs::create_dir(&root)?;
        let mut state = State::open(&state_dir)?;
        let share = state.init_share(&root)?;
        let relationship = RelationshipId::generate();
        let mut peer = test_connector("peer-lifecycle");
        peer.relationship = Some(relationship);
        state.set_peer(&share, &peer)?;
        state.set_initial_complete(&share)?;
        let expected_binding = state.managed_share(&share)?.binding;

        let workers = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let lifecycle = Arc::new(Mutex::new(()));
        let held = lifecycle.lock().unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let (events, _event_rx) = std::sync::mpsc::channel();
        let start = {
            let state_dir = state_dir.clone();
            let workers = workers.clone();
            let lifecycle = lifecycle.clone();
            let barrier = barrier.clone();
            let events = events.clone();
            let share = share.clone();
            std::thread::spawn(move || {
                concurrent_request(
                    state_dir,
                    workers,
                    events,
                    lifecycle,
                    barrier,
                    DaemonRequest::Start { share: share.0 },
                )
            })
        };
        let remove = {
            let state_dir = state_dir.clone();
            let workers = workers.clone();
            let lifecycle = lifecycle.clone();
            let barrier = barrier.clone();
            let events = events.clone();
            let share = share.clone();
            std::thread::spawn(move || {
                concurrent_request(
                    state_dir,
                    workers,
                    events,
                    lifecycle,
                    barrier,
                    DaemonRequest::PrepareRemove {
                        share: share.0,
                        expected_binding,
                    },
                )
            })
        };
        barrier.wait();
        drop(held);

        let start = start.join().expect("start request panicked")?;
        let remove = remove.join().expect("remove request panicked")?;
        assert!(matches!(
            start,
            DaemonResponse::Ok | DaemonResponse::Error { .. }
        ));
        assert!(matches!(remove, DaemonResponse::Prepared { .. }));
        assert!(workers.lock().unwrap().is_empty());
        assert!(
            State::open(&state_dir)?
                .managed_share(&share)?
                .removing_relationship
                .is_some()
        );
        Ok(())
    }

    #[test]
    fn daemon_list_pages_large_valid_state() -> Result<()> {
        let temp = tempdir()?;
        let mut state = State::open(temp.path().join("state"))?;
        for index in 0..20 {
            let root = temp.path().join(format!("root-{index}"));
            std::fs::create_dir(&root)?;
            let share = state.init_share(&root)?;
            state.set_peer(&share, &test_connector(&format!("peer-{index}")))?;
            state.set_blocked(&share, &"x".repeat(4096))?;
        }
        let workers = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let mut cursor = None;
        let mut count = 0;
        loop {
            let DaemonResponse::List { syncs, next } =
                daemon_sync_page(&mut state, &workers, cursor)?
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
        assert!(daemon_sync_page(&mut state, &workers, Some("missing".into())).is_err());
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
    fn removal_owns_the_share_and_blocks_every_connector_round() -> Result<()> {
        use std::os::unix::net::UnixStream;

        let temp = tempdir()?;
        let root = temp.path().join("root");
        std::fs::create_dir(&root)?;
        let state_dir = temp.path().join("state");
        let mut state = State::open(&state_dir)?;
        let share = state.init_share(&root)?;
        state.set_install_intent(&share, &[])?;

        let foreground_owner = state.lock_share(&share)?;
        let mut competing = State::open(&state_dir)?;
        competing.begin_install_intent_retry(&share)?;
        let request = competing.enqueue_sync(Some(&share), SyncOperation::Recovery, None)?;
        let (waiting, waiting_rx) = std::sync::mpsc::sync_channel(1);
        let recovery = std::thread::spawn(move || {
            request
                .wait_with_prepare(
                    || false,
                    |_| {
                        let _ = waiting.try_send(());
                        Ok(())
                    },
                    recover_installs_locked,
                )?
                .finish()
        });
        waiting_rx.recv_timeout(Duration::from_secs(3))?;
        assert!(state.install_intent_failure(&share)?.is_none());
        drop(foreground_owner);
        recovery.join().expect("recovery thread panicked")?;
        assert!(state.install_intent(&share)?.is_none());

        let mut peer = test_connector("peer-removing");
        peer.relationship = Some(RelationshipId::generate());
        state.set_peer(&share, &peer)?;
        let root = sync::ShareRoot::open(&state, &share)?;
        let binding = state.endpoint_binding(&share)?;
        state.prepare_removal(&share, &binding)?;
        let (local, _remote) = UnixStream::pair()?;
        let mut pending_remote_generation = 0;
        let error = match connector_v2_round(
            &mut state,
            &share,
            &root,
            1,
            0,
            0,
            &local,
            &local,
            &mut pending_remote_generation,
            &Default::default(),
            &mut Vec::new(),
            None,
            None,
        ) {
            Ok(_) => bail!("a removal-pending connector began another round"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("relationship removal is pending")
        );
        Ok(())
    }

    #[test]
    fn foreground_watch_rechecks_removal_after_owning_the_session() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path().join("root");
        std::fs::create_dir(&root)?;
        let mut state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        state.set_initial_complete(&share)?;
        let connector = test_connector("peer-removing");
        state.set_peer(&share, &connector)?;
        state.prepare_removal(&share, &EndpointBinding::Connector(connector))?;

        let error = watch(&mut state, &root).expect_err("removal must prevent a new watch");
        assert!(
            error
                .to_string()
                .contains("relationship removal is pending")
        );
        Ok(())
    }

    #[test]
    fn daemon_recovery_failure_updates_a_pending_removal_diagnostic() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path().join("root");
        std::fs::create_dir(&root)?;
        let mut state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        let connector = test_connector("peer-removing");
        state.set_peer(&share, &connector)?;
        let removal = state.prepare_removal(&share, &EndpointBinding::Connector(connector))?;
        state.set_install_intent(&share, &[])?;
        std::fs::rename(&root, temp.path().join("root-moved"))?;

        spawn_daemon_install_recovery(&mut state)?.recv_timeout(Duration::from_secs(3))??;

        let managed = state.managed_share(&share)?;
        assert_eq!(managed.removing_relationship, Some(removal.relationship));
        assert!(
            managed
                .blocked_diagnostic
                .is_some_and(|message| message.contains("configured root"))
        );
        Ok(())
    }

    #[test]
    fn remote_failure_classifies_local_removal_state_truthfully() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path().join("root");
        std::fs::create_dir(&root)?;
        let state_dir = temp.path().join("state");
        let mut state = State::open(&state_dir)?;
        let share = state.init_share(&root)?;
        let connector = test_connector("peer-concurrent-removal");
        state.set_peer(&share, &connector)?;
        let removal = state.prepare_removal(&share, &EndpointBinding::Connector(connector))?;
        let remote_error = anyhow::anyhow!("broken pipe");

        let pending = report_remote_removal_failure(&mut state, &removal, &remote_error)
            .expect_err("a failed two-sided removal remains pending");
        assert!(pending.to_string().contains("pending and disabled"));
        assert_eq!(
            state.managed_share(&share)?.blocked_diagnostic.as_deref(),
            Some("broken pipe")
        );

        let mut concurrent = State::open(&state_dir)?;
        concurrent.finalize_local_removal(&removal)?;
        let finalized = report_remote_removal_failure(&mut state, &removal, &remote_error)
            .expect_err("unconfirmed remote state remains an error");
        let message = finalized.to_string();
        assert!(message.contains("local relationship was removed concurrently"));
        assert!(message.contains("removal from peer"));
        assert!(message.contains("--local-only"));
        assert!(message.contains("broken pipe"));
        assert!(!message.contains("pending and disabled"));

        let mut replacement = test_connector("peer-replacement");
        replacement.host = "replacement-peer".into();
        concurrent.set_peer(&share, &replacement)?;
        let changed = report_remote_removal_failure(&mut state, &removal, &remote_error)
            .expect_err("a changed local binding cannot absorb the old failure");
        let message = changed.to_string();
        assert!(message.contains("local relationship changed"));
        assert!(message.contains("remote state is unconfirmed"));
        assert!(message.contains("test-peer"));
        assert!(message.contains("broken pipe"));
        assert!(message.contains("--local-only"));
        Ok(())
    }

    #[test]
    fn install_recovery_failure_classifies_local_removal_state_truthfully() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path().join("root");
        std::fs::create_dir(&root)?;
        let state_dir = temp.path().join("state");
        let mut state = State::open(&state_dir)?;
        let share = state.init_share(&root)?;
        let connector = test_connector("peer-concurrent-recovery");
        state.set_peer(&share, &connector)?;
        let removal = state.prepare_removal(&share, &EndpointBinding::Connector(connector))?;
        let recovery_error = anyhow::anyhow!("configured root identity changed");

        let pending = report_install_recovery_failure(&mut state, &removal, &recovery_error)
            .expect_err("the exact removal remains pending");
        assert!(pending.to_string().contains("pending and disabled as of"));

        let mut concurrent = State::open(&state_dir)?;
        concurrent.finalize_local_removal(&removal)?;
        let finalized = report_install_recovery_failure(&mut state, &removal, &recovery_error)
            .expect_err("the recovery error remains visible after concurrent finalization");
        let message = finalized.to_string();
        assert!(message.contains("local relationship was removed concurrently"));
        assert!(message.contains("no local removal retry remained"));
        assert!(message.contains("configured root identity changed"));
        assert!(!message.contains("pending and disabled"));

        let replacement = test_connector("peer-replacement-after-recovery");
        concurrent.set_peer(&share, &replacement)?;
        let changed = report_install_recovery_failure(&mut state, &removal, &recovery_error)
            .expect_err("a changed binding cannot absorb the old recovery failure");
        let message = changed.to_string();
        assert!(message.contains("install recovery failed"));
        assert!(message.contains("local relationship changed"));
        assert!(message.contains("flocal sync list"));
        assert!(message.contains("configured root identity changed"));
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
            start_managed_share(&mut state, &workers, &events, share.clone())
                .expect_err("responder-only share must not start")
                .to_string()
                .contains("responder-only")
        );

        state.set_peer(
            &share,
            &PeerConfig {
                peer_id: Some(flocal::model::PeerId("peer-test".into())),
                relationship: None,
                host: "example.invalid".into(),
                remote_path: path_bytes(Path::new("/remote")),
                executable: "/bin/false".into(),
            },
        )?;
        assert!(
            start_managed_share(&mut state, &workers, &events, share.clone())
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
        stop_managed_share(&mut state, &workers, share.clone())?;
        assert!(!state.managed_share(&share)?.watch_enabled);
        assert!(stop.load(Ordering::Relaxed));
        assert!(stopping.load(Ordering::Relaxed));
        assert!(!workers.lock().unwrap().contains_key(&share));
        Ok(())
    }

    #[test]
    fn daemon_reconciliation_claims_a_durable_managed_request_after_client_handoff() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path().join("root");
        std::fs::create_dir(&root)?;
        let mut state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        state.set_peer(
            &share,
            &PeerConfig {
                peer_id: Some(flocal::model::PeerId("peer-test".into())),
                relationship: None,
                host: "127.0.0.1".into(),
                remote_path: path_bytes(Path::new("/remote")),
                executable: "/bin/false".into(),
            },
        )?;
        state.set_initial_complete(&share)?;
        state.set_watch_enabled(&share, true)?;
        let held = state
            .enqueue_sync(Some(&share), SyncOperation::Maintenance, None)?
            .wait(|| false, |_| Ok(()))?;
        let generation = state.watch_intent_generation(&share)?;
        let request = state.enqueue_sync(Some(&share), SyncOperation::Watch, Some(generation))?;
        let token = request.token().to_owned();
        let ticket = request.ticket();
        let workers = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let (events, _events_rx) = std::sync::mpsc::channel();

        reconcile_watches(&mut state, &workers, &events)?;
        assert!(workers.lock().unwrap().contains_key(&share));
        let snapshot = state.scheduling_snapshot()?;
        let durable = snapshot
            .queued
            .iter()
            .find(|request| request.token == token)
            .context("managed request disappeared during handoff cleanup")?;
        assert_eq!(durable.ticket, ticket);
        request.release_for_reclaim();
        let snapshot = state.scheduling_snapshot()?;
        assert!(
            snapshot
                .queued
                .iter()
                .any(|request| request.token == token && request.ticket == ticket)
        );

        held.finish()?;
        stop_managed_share(&mut state, &workers, share)?;
        Ok(())
    }

    #[test]
    fn interrupted_managed_initial_recovery_preserves_enablement_and_queue_intent() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path().join("root");
        std::fs::create_dir(&root)?;
        let mut state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        state.set_peer(
            &share,
            &PeerConfig {
                peer_id: Some(flocal::model::PeerId("peer-test".into())),
                relationship: None,
                host: "127.0.0.1".into(),
                remote_path: path_bytes(Path::new("/remote")),
                executable: "/bin/false".into(),
            },
        )?;
        state.set_managed_plan_install_intent(&share, &[], &[], 0)?;

        spawn_daemon_install_recovery(&mut state)?.recv_timeout(Duration::from_secs(3))??;

        let managed = state.managed_share(&share)?;
        assert!(managed.initial_complete);
        assert!(managed.watch_enabled);
        assert!(state.install_intent(&share)?.is_none());
        let held = state
            .enqueue_sync(Some(&share), SyncOperation::Maintenance, None)?
            .wait(|| false, |_| Ok(()))?;
        let workers = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let (events, _events_rx) = std::sync::mpsc::channel();
        reconcile_watches(&mut state, &workers, &events)?;
        let queued = state.scheduling_snapshot()?.queued;
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].share.as_ref(), Some(&share));
        assert_eq!(queued[0].operation, SyncOperation::Watch);
        held.finish()?;
        stop_managed_share(&mut state, &workers, share)?;
        Ok(())
    }

    #[test]
    fn explicit_start_replaces_a_worker_blocked_by_classified_install_recovery() -> Result<()> {
        let temp = tempdir()?;
        let root = temp.path().join("root");
        std::fs::create_dir(&root)?;
        let mut state = State::open(temp.path().join("state"))?;
        let share = state.init_share(&root)?;
        state.set_peer(
            &share,
            &PeerConfig {
                peer_id: Some(flocal::model::PeerId("peer-test".into())),
                relationship: None,
                host: "127.0.0.1".into(),
                remote_path: path_bytes(Path::new("/remote")),
                executable: "/bin/false".into(),
            },
        )?;
        state.set_initial_complete(&share)?;
        state.set_watch_enabled(&share, true)?;
        state.set_install_intent(&share, &[])?;
        let intent = state
            .unclassified_install_intents()?
            .into_iter()
            .find(|intent| intent.share == share)
            .context("missing recovery intent")?;
        assert!(state.classify_install_intent_failure(
            &share,
            &intent.fingerprint,
            "operator must explicitly retry",
        )?);

        let workers = Arc::new(Mutex::new(std::collections::HashMap::new()));
        workers.lock().unwrap().insert(
            share.clone(),
            DaemonWorker {
                id: 7,
                stop: Arc::new(AtomicBool::new(false)),
                state: Arc::new(std::sync::atomic::AtomicU8::new(WORKER_WATCHING)),
                stopping: Arc::new(AtomicBool::new(false)),
                finished: Arc::new(AtomicBool::new(true)),
                child: Arc::new(Mutex::new(None)),
            },
        );
        let (events, _event_rx) = std::sync::mpsc::channel();

        start_managed_share(&mut state, &workers, &events, share.clone())?;
        assert!(state.install_intent_failure(&share)?.is_none());
        assert_ne!(workers.lock().unwrap()[&share].id, 7);

        stop_managed_share(&mut state, &workers, share.clone())?;
        assert!(!workers.lock().unwrap().contains_key(&share));
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
                peer_id: Some(flocal::model::PeerId("peer-test".into())),
                relationship: None,
                host: "127.0.0.1".into(),
                remote_path: path_bytes(Path::new("/remote")),
                executable: "/bin/false".into(),
            },
        )?;
        state.set_initial_complete(&share)?;
        let workers = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let (events, event_rx) = std::sync::mpsc::channel();
        start_managed_share(&mut state, &workers, &events, share.clone())?;
        assert!(workers.lock().unwrap().contains_key(&share));
        stop_managed_share(&mut state, &workers, share.clone())?;
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
        start_worker(&state, &workers, &events, share.clone(), None)?;
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
            let lifecycle = Arc::new(Mutex::new(()));
            std::thread::scope(|scope| {
                scope.spawn(|| handle_daemon_request(state, &workers, &events, &lifecycle, server));
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
        state.set_peer(&share, &test_connector("peer-live"))?;
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
            "#!/bin/sh\nif [ \"$2\" = show-environment ]; then printf 'HOME=%s\\n' \"$FLOCAL_TEST_MANAGER_HOME\"; elif [ \"$2\" = show ]; then if [ -e \"$FLOCAL_TEST_MANAGER_HOME/show-fail\" ]; then exit 1; elif [ -e \"$FLOCAL_TEST_MANAGER_HOME/active-state\" ]; then cat \"$FLOCAL_TEST_MANAGER_HOME/active-state\"; else printf 'inactive\\n'; fi; fi\nexit 0\n",
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
            .env("FLOCAL_STATE_DIR", temp.path().join("state"))
            .env("FLOCAL_TEST_MANAGER_HOME", &manager_home)
            .output()?;
        assert!(output.status.success(), "{:?}", output);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn assert_systemd_install(root: &Path) -> Result<()> {
        let manager_home = root.join("manager-home");
        let client_home = root.join("client-home");
        let state_dir = root.join("state");
        flocal::state::ensure_private_directory(&state_dir)?;
        let executable = std::env::current_exe()?.canonicalize()?;
        prepare_daemon_service(&state_dir, &executable)?;
        use std::os::unix::ffi::OsStringExt;
        let invalid_state = root.join(std::ffi::OsString::from_vec(b"state-\xff".to_vec()));
        assert!(systemd_unit_content(&executable, &invalid_state).is_err());
        assert!(
            !manager_home
                .join(".config/systemd/user/flocal-daemon.service")
                .exists()
        );
        assert!(systemd_quote(Path::new("/tmp/invalid%path")).is_err());
        assert!(run_manager("/bin/false", &[]).is_err());
        let state = State::open(&state_dir)?;
        assert!(ensure_daemon(&state).is_err());
        drop(state);
        let destination = client_home.join(".local/bin/flocal");
        let ready = serve_one_daemon_list(&state_dir)?;
        let old_umask = rustix::process::umask(rustix::fs::Mode::empty());
        let installed = install_daemon(&destination);
        rustix::process::umask(old_umask);
        installed?;
        ready
            .join()
            .map_err(|_| anyhow::anyhow!("test daemon socket panicked"))??;
        assert_eq!(std::fs::read(&destination)?, std::fs::read(&executable)?);
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(state_dir.join("installer.lock"))?
                .permissions()
                .mode()
                & 0o077,
            0
        );
        assert!(
            manager_home
                .join(".config/systemd/user/flocal-daemon.service")
                .is_file()
        );
        assert_eq!(
            std::fs::read_to_string(client_home.join(".config/file.local/managed-state"))?,
            format!("{}\n", state_dir.display())
        );

        let installer = std::fs::OpenOptions::new()
            .append(true)
            .open(state_dir.join("installer.lock"))?;
        fs2::FileExt::try_lock_exclusive(&installer)?;
        assert!(install_daemon(&client_home.join(".local/bin/other")).is_err());
        drop(installer);

        State::create_upgrade_pending(&state_dir)?;
        let service = prepare_daemon_service(&state_dir, &destination)?;
        let pending = fail_before_publication(
            &service,
            &state_dir,
            true,
            anyhow::anyhow!("synthetic retry failure"),
        )
        .expect_err("an inherited marker keeps the retry on the candidate path");
        assert!(
            pending
                .to_string()
                .contains("earlier upgrade is still pending")
        );
        assert!(State::upgrade_pending_at(&state_dir)?);
        let refused = restore_before_upgrade(
            &service,
            &state_dir,
            anyhow::anyhow!("synthetic safe refusal"),
        )
        .expect_err("restoration returns the original refusal");
        assert!(refused.to_string().contains("synthetic safe refusal"));

        std::fs::write(manager_home.join("active-state"), "active\n")?;
        let running = prepare_daemon_service(&state_dir, &destination)?;
        assert!(running.running);
        stop_daemon_service(&running)?;
        let readiness = start_and_verify_daemon(&running, &state_dir, std::time::Instant::now())
            .expect_err("manager acceptance without a daemon socket must fail");
        assert!(readiness.to_string().contains("did not become ready"));
        std::fs::write(manager_home.join("show-fail"), "")?;
        assert!(prepare_daemon_service(&state_dir, &destination).is_err());
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn serve_one_daemon_list(state_dir: &Path) -> Result<std::thread::JoinHandle<Result<()>>> {
        let run = state_dir.join("run");
        flocal::state::ensure_private_directory(&run)?;
        let listener = UnixListener::bind(run.join("daemon.sock"))?;
        Ok(std::thread::spawn(move || {
            let (mut stream, _) = listener.accept()?;
            let _ = read_daemon_message(&mut stream)?;
            serde_json::to_writer(
                &mut stream,
                &DaemonResponse::List {
                    syncs: Vec::new(),
                    next: None,
                },
            )?;
            stream.write_all(b"\n")?;
            Ok(())
        }))
    }

    #[test]
    fn installer_filesystem_helpers_reject_unsafe_paths_and_cleanup_staging() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir()?;
        let candidate = std::env::current_exe()?.canonicalize()?;
        assert!(StagedExecutable::prepare(&candidate, Path::new("relative/flocal")).is_err());
        assert!(StagedExecutable::prepare(&candidate, Path::new("/")).is_err());

        let destination = temp.path().join("bin/flocal");
        let staged = StagedExecutable::prepare(&candidate, &destination)?;
        let temporary = destination.parent().unwrap().join(staged.temporary.clone());
        assert!(temporary.is_file());
        drop(staged);
        assert!(!temporary.exists());

        let parent = destination.parent().unwrap();
        let directory = cap_std::fs::Dir::from_std_file(File::open(parent)?);
        std::os::unix::fs::symlink(&candidate, &destination)?;
        assert!(validate_install_destination(&directory, Path::new("flocal")).is_err());
        std::fs::remove_file(&destination)?;

        let service_asset = parent.join("service");
        std::fs::write(&service_asset, "safe")?;
        std::fs::set_permissions(&service_asset, std::fs::Permissions::from_mode(0o600))?;
        preflight_text_file(&service_asset)?;
        std::fs::set_permissions(&service_asset, std::fs::Permissions::from_mode(0o644))?;
        assert!(preflight_text_file(&service_asset).is_err());

        let untrusted = parent.join("untrusted");
        std::fs::write(&untrusted, "not executable")?;
        let untrusted = File::open(untrusted)?;
        assert!(validate_trusted_executable_file(&untrusted).is_err());
        Ok(())
    }

    #[test]
    fn binary_only_retry_preserves_an_inherited_upgrade_marker() -> Result<()> {
        let temp = tempdir()?;
        let state_dir = temp.path().join("state");
        State::create_upgrade_pending(&state_dir)?;

        complete_upgrade_marker(&state_dir, false, false)?;
        assert!(!State::upgrade_pending_at(&state_dir)?);

        State::create_upgrade_pending(&state_dir)?;
        complete_upgrade_marker(&state_dir, false, true)?;
        assert!(State::upgrade_pending_at(&state_dir)?);

        complete_upgrade_marker(&state_dir, true, true)?;
        assert!(!State::upgrade_pending_at(&state_dir)?);
        Ok(())
    }

    #[test]
    fn managed_install_refuses_a_different_selected_state_directory() -> Result<()> {
        let temp = tempdir()?;
        let selected = temp.path().join("selected");
        let managed = temp.path().join("managed");
        validate_managed_state_selection(&selected, Some(selected.clone()))?;
        validate_managed_state_selection(&selected, None)?;
        let error = validate_managed_state_selection(&selected, Some(managed))
            .expect_err("a managed installation cannot be silently relocated");
        assert!(error.to_string().contains("does not match"));
        Ok(())
    }

    #[test]
    fn upgrade_wait_names_the_busy_root_and_is_bounded() -> Result<()> {
        let temp = tempdir()?;
        let state_dir = temp.path().join("state");
        let root = temp.path().join("root");
        std::fs::create_dir(&root)?;
        let state = State::open(&state_dir)?;
        let share = state.init_share(&root)?;
        let _session = state.lock_share_session(&share)?;
        let error = match quiesce_for_upgrade_until(&state_dir, std::time::Instant::now()) {
            Ok(_) => bail!("active foreground session did not block upgrade"),
            Err(error) => error,
        };
        assert!(error.to_string().contains(&root.display().to_string()));
        Ok(())
    }

    #[test]
    fn upgrade_wait_escapes_control_bytes_in_a_busy_root() -> Result<()> {
        let temp = tempdir()?;
        let state_dir = temp.path().join("state");
        let root = temp.path().join("root\n\u{1b}[31m");
        std::fs::create_dir(&root)?;
        let state = State::open(&state_dir)?;
        let share = state.init_share(&root)?;
        let _session = state.lock_share_session(&share)?;
        let error = match quiesce_for_upgrade_until(&state_dir, std::time::Instant::now()) {
            Ok(_) => bail!("active foreground session did not block upgrade"),
            Err(error) => error,
        };
        let message = error.to_string();
        assert!(message.contains("\\n"));
        assert!(message.contains("\\u{1b}"));
        assert!(!message.contains('\n'));
        assert!(!message.contains('\u{1b}'));
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
