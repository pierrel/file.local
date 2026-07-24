use std::io::{self, BufReader, BufWriter, Read, Write};
use std::os::fd::AsFd;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
use flocal::model::{Entry, PeerConfig, ShareId};
use flocal::state::State;
use flocal::sync::{self, Message};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};

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
    let mut input = BufReader::new(TimedReader::new(stdin.lock()));
    let mut output = BufWriter::new(stdout.lock());
    serve_io(state, &mut input, &mut output)
}

fn serve_io(
    state: &mut State,
    mut input: &mut impl Read,
    mut output: &mut impl Write,
) -> Result<()> {
    match sync::read_message(&mut input)? {
        Message::Register {
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
        Message::Sync {
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
        other => bail!("unexpected initial message: {other:?}"),
    }
    Ok(())
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
    let (tx, rx) = std::sync::mpsc::channel();
    let mut watcher: RecommendedWatcher = notify::recommended_watcher(move |event| {
        let _ = tx.send(event);
    })?;
    watcher.watch(&root, RecursiveMode::Recursive)?;
    let rescan = watch_rescan_interval(std::env::var("FLOCAL_WATCH_RESCAN_SECONDS").ok());
    watch_loop(
        state,
        path,
        &root,
        &rx,
        rescan,
        &mut io::stdout(),
        &mut io::stderr(),
    )
}

/// The interval after which `watch` rescans and attempts a sync even
/// without a filesystem event: `FLOCAL_WATCH_RESCAN_SECONDS`, default 30.
/// Unlike the unclamped `FLOCAL_MAX_*` budget variables, zero (or an
/// unparseable value) falls back to the default instead of being honored —
/// a zero interval would hot-loop full rescans and connection attempts,
/// and nothing needs it.
fn watch_rescan_interval(configured: Option<String>) -> Duration {
    Duration::from_secs(
        configured
            .and_then(|value| value.parse().ok())
            .filter(|&seconds| seconds != 0)
            .unwrap_or(30),
    )
}

/// The reconciliation loop, taking an already-set-up event receiver and the
/// destinations for its status lines, so both the control flow and the
/// exact text it prints can be driven and asserted deterministically in
/// tests, without a live filesystem watcher or terminal. A disconnected
/// receiver ends the loop the same way tearing down the real watcher
/// would, via `rx.recv_timeout`'s own `Disconnected` error.
fn watch_loop(
    state: &mut State,
    path: &Path,
    root: &Path,
    rx: &std::sync::mpsc::Receiver<notify::Result<notify::Event>>,
    rescan: Duration,
    out: &mut impl Write,
    err: &mut impl Write,
) -> Result<()> {
    watch_log(out, &format!("Watching {}", root.display()))?;
    let mut failures = WatchFailures::default();
    watch_cycle(state, path, out, err, &mut failures)?;
    loop {
        match rx.recv_timeout(rescan) {
            Ok(Ok(_)) => {
                std::thread::sleep(Duration::from_millis(250));
                while rx.try_recv().is_ok() {}
            }
            Ok(Err(error)) => watch_log(err, &format!("watch error, rescanning: {error}"))?,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(error) => return Err(error.into()),
        }
        watch_cycle(state, path, out, err, &mut failures)?;
    }
}

const WATCH_FAILURE_REPORT_INTERVAL: Duration = Duration::from_secs(5 * 60);

#[derive(Default)]
struct WatchFailures {
    count: u64,
    last_reported: Option<std::time::Instant>,
}

enum WatchEvent {
    First { error: String },
    Periodic { count: u64, error: String },
    Recovered { count: u64 },
}

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

fn watch_cycle(
    state: &mut State,
    path: &Path,
    out: &mut impl Write,
    err: &mut impl Write,
    failures: &mut WatchFailures,
) -> Result<()> {
    match run_sync(state, path, false, true, false, PlanReport::Watch) {
        Ok(completion) => {
            handle_watch_completion(completion, out, err, failures)?;
        }
        Err(error) => {
            if let Some(event) = failures.failed(
                &error,
                std::time::Instant::now(),
                WATCH_FAILURE_REPORT_INTERVAL,
            ) {
                write_watch_event(err, event)?;
            }
        }
    }
    Ok(())
}

fn handle_watch_completion(
    completion: SyncCompletion,
    out: &mut impl Write,
    err: &mut impl Write,
    failures: &mut WatchFailures,
) -> Result<()> {
    if let Some(report) = completion.watch_report {
        out.write_all(&report)?;
    }
    if let Some(error) = completion.post_commit_error {
        if let Some(event) = failures.failed(
            &error,
            std::time::Instant::now(),
            WATCH_FAILURE_REPORT_INTERVAL,
        ) {
            write_watch_event(err, event)?;
        }
    } else if let Some(event) = failures.succeeded() {
        write_watch_event(err, event)?;
    }
    Ok(())
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
    fn committed_watch_report_survives_finalization_error() -> Result<()> {
        let mut out = Vec::new();
        let mut err = Vec::new();
        let mut failures = WatchFailures::default();
        handle_watch_completion(
            SyncCompletion {
                watch_report: Some(b"UPLOAD applied.txt\n".to_vec()),
                post_commit_error: Some(anyhow::anyhow!("final ssh teardown failed")),
            },
            &mut out,
            &mut err,
            &mut failures,
        )
        .expect("post-commit outcome is writable");
        assert_eq!(out, b"UPLOAD applied.txt\n");
        assert!(String::from_utf8(err)?.contains("final ssh teardown failed"));
        assert_eq!(failures.count, 1);
        handle_watch_completion(
            SyncCompletion::default(),
            &mut Vec::new(),
            &mut Vec::new(),
            &mut failures,
        )
        .expect("recovery outcome is writable");
        assert_eq!(failures.count, 0);
        Ok(())
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
    fn watch_rescan_interval_defaults_to_30_and_refuses_zero() {
        assert_eq!(watch_rescan_interval(None), Duration::from_secs(30));
        assert_eq!(
            watch_rescan_interval(Some("1".into())),
            Duration::from_secs(1)
        );
        // Zero would hot-loop, so it falls back rather than being honored;
        // negatives and non-numbers fail the u64 parse and fall back too.
        assert_eq!(
            watch_rescan_interval(Some("0".into())),
            Duration::from_secs(30)
        );
        assert_eq!(
            watch_rescan_interval(Some("-5".into())),
            Duration::from_secs(30)
        );
        assert_eq!(
            watch_rescan_interval(Some("soon".into())),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn watch_loop_retries_offline_and_exits_once_the_watcher_disconnects() -> Result<()> {
        // No dependency on a live filesystem watcher or network: the share
        // has no peer configured, so every run_sync attempt inside the loop
        // fails fast, in-process; one queued event drives the loop through
        // both its match arms before the sender is dropped, which ends the
        // loop deterministically via Disconnected exactly as tearing down a
        // real watcher would. Capturing both writers pins the exact printed
        // text, not just that the loop ran — a message swapped between the
        // pre-loop and in-loop offline cases would fail this test.
        let temp = tempdir()?;
        let root = temp.path().join("root");
        std::fs::create_dir_all(&root)?;
        let mut state = State::open(temp.path().join("state"))?;
        state.init_share(&root)?;
        let (tx, rx) = std::sync::mpsc::channel();
        tx.send(Ok(notify::Event::default()))?;
        drop(tx);
        let mut out = Vec::new();
        let mut err = Vec::new();
        let rescan = watch_rescan_interval(None);
        let error = watch_loop(&mut state, &root, &root, &rx, rescan, &mut out, &mut err)
            .expect_err("disconnect ends the loop");
        assert!(
            error
                .downcast_ref::<std::sync::mpsc::RecvTimeoutError>()
                .is_some()
        );

        let out = String::from_utf8(out)?;
        let (timestamp, rest) = out
            .trim_end()
            .split_once(' ')
            .context("expected a timestamped watch startup line")?;
        assert_eq!(timestamp.len(), 20, "{timestamp:?}");
        assert_eq!(rest, format!("Watching {}", root.display()));

        let err = String::from_utf8(err)?;
        let err_lines: Vec<&str> = err.lines().collect();
        // The pre-loop attempt fails (no peer configured) and reports it;
        // the immediate in-loop retry is rate-limited.
        assert_eq!(err_lines.len(), 1, "{err:?}");
        let (_, rest) = err_lines[0].split_once(' ').context("timestamped line")?;
        assert_eq!(
            rest,
            "synchronization failed; retrying in background: no peer configured; \
             run `flocal peer add`"
        );
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
