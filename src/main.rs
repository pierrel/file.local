use std::io::{self, BufReader, BufWriter, Read, Write};
use std::os::fd::AsFd;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
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
    Sync {
        path: PathBuf,
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        yes: bool,
        #[arg(long)]
        json: bool,
    },
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
    Protocol {
        #[command(subcommand)]
        command: ProtocolCommand,
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
        Commands::Sync {
            path,
            dry_run,
            yes,
            json,
        } => {
            if !dry_run {
                recover_installs(&mut state)?;
            }
            run_sync(&mut state, &path, dry_run, yes, json, PlanReport::Full)?;
        }
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
        Commands::Protocol {
            command: ProtocolCommand::Serve,
        } => {
            recover_installs(&mut state)?;
            serve(&mut state)?
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
    let _global_lock = match state.lock_global_sync() {
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
    let _global_lock = state.lock_global_sync()?;
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
        if !connecting_logged {
            watch_log(out, "Connecting to peer")?;
            connecting_logged = true;
        }
        let root = match sync::ShareRoot::open(state, share) {
            Ok(root) => root,
            Err(error) if !root_validation_retryable(&error) => return Err(error),
            Err(error) => {
                if let Some(event) = failures.failed(
                    &error,
                    std::time::Instant::now(),
                    WATCH_FAILURE_REPORT_INTERVAL,
                ) {
                    write_watch_event(err, event)?;
                }
                let delay = backoff.failed(&retry_policy, rand::random_range(-2_000..=2_000));
                std::thread::sleep(delay);
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
                std::thread::sleep(delay);
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
        );
        match outcome {
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
                std::thread::sleep(delay);
            }
        }
    }
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
) -> Result<()> {
    while events_rx.try_recv().is_ok() {}
    let remote = PersistentRemote::spawn(&peer.host, &peer.executable)?;
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
    child: Child,
    input: ChildStdin,
    output: ChildStdout,
    stderr: Option<std::thread::JoinHandle<Vec<u8>>>,
}

impl PersistentRemote {
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
        Ok(Self {
            child,
            input,
            output,
            stderr: Some(stderr),
        })
    }
}

impl Drop for PersistentRemote {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
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
        let global_lock = state.lock_global_sync()?;
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
        drop(global_lock);
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
}
