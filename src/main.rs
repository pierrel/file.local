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
            run_sync(&mut state, &path, dry_run, yes, json)?;
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

fn run_sync(state: &mut State, path: &Path, dry_run: bool, yes: bool, json: bool) -> Result<()> {
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
    print_plan(&local, &remote_records, &plan, json)?;
    if dry_run {
        sync::write_message(&mut remote.input, &Message::Cancel)?;
        return remote.finish();
    }
    if !state.initial_complete(&share)? && !yes && !confirm("Apply this initial plan?")? {
        sync::write_message(&mut remote.input, &Message::Cancel)?;
        return remote.finish();
    }

    let root = state.root_for(&share)?;
    let mut required_records = plan.records.clone();
    for conflict in &plan.conflicts {
        if flocal::scan::is_ignored(&root, &conflict.path)? {
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
    sync::write_message(&mut remote.input, &Message::Done)?;
    remote.finish()?;
    Ok(())
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
    let pending_install = state
        .install_intents()?
        .iter()
        .any(|(pending, _)| pending == &share);
    if json {
        println!(
            "{}",
            serde_json::json!({"schema":1,"share":share.0,"root":root,"peer":peer,"entries":records.len(),"initial_complete":state.initial_complete(&share)?,"view":"last_persisted_scan","pending_install":pending_install})
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
        println!("Entries: {}", records.len());
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
    if destination.exists() && !force {
        bail!("destination exists; use --force to replace it");
    }
    if destination.is_dir() {
        bail!("refusing to replace a directory");
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
    println!("Watching {}", root.display());
    let mut offline = false;
    if let Err(error) = run_sync(state, path, false, true, false) {
        eprintln!("peer offline; watching locally and retrying: {error:#}");
        offline = true;
    }
    loop {
        match rx.recv_timeout(Duration::from_secs(30)) {
            Ok(Ok(_)) => {
                std::thread::sleep(Duration::from_millis(250));
                while rx.try_recv().is_ok() {}
            }
            Ok(Err(error)) => eprintln!("watch error, rescanning: {error}"),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(error) => return Err(error.into()),
        }
        match run_sync(state, path, false, true, false) {
            Ok(()) if offline => {
                eprintln!("peer reconnected; synchronization resumed");
                offline = false;
            }
            Ok(()) => {}
            Err(error) if !offline => {
                eprintln!("peer offline; retrying in background: {error:#}");
                offline = true;
            }
            Err(_) => {}
        }
    }
}

fn print_plan(
    local: &[flocal::model::Record],
    remote: &[flocal::model::Record],
    plan: &flocal::reconcile::Plan,
    json: bool,
) -> Result<()> {
    if json {
        println!("{}", serde_json::json!({"schema": 1, "plan": plan}));
        return Ok(());
    }
    let local_by_path: std::collections::HashMap<_, _> =
        local.iter().map(|r| (r.path.as_bytes(), r)).collect();
    let remote_by_path: std::collections::HashMap<_, _> =
        remote.iter().map(|r| (r.path.as_bytes(), r)).collect();
    for record in &plan.records {
        let local_record = local_by_path.get(record.path.as_bytes());
        let remote_record = remote_by_path.get(record.path.as_bytes());
        let action = match (&record.version.entry, local_record, remote_record) {
            (_, Some(local), Some(remote))
                if local.version.id() == remote.version.id()
                    && local.version.entry == remote.version.entry =>
            {
                "KEEP  "
            }
            (Entry::Tombstone, _, _) => "DELETE",
            (_, Some(_), Some(_)) => "MERGE ",
            (_, Some(_), None) => "UPLOAD",
            (_, None, Some(_)) => "DOWNLOAD",
            _ => "KEEP  ",
        };
        println!("{action} {}", record.path.display());
    }
    for conflict in &plan.conflicts {
        println!("CONFLICT {}", conflict.path.display());
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
            "command -v flocal",
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
        )?;
        print_plan(
            &[],
            &[],
            &flocal::reconcile::Plan {
                records: Vec::new(),
                conflicts: Vec::new(),
            },
            true,
        )?;
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
        assert!(
            initial_message(Message::Sync {
                protocol: sync::PROTOCOL_VERSION,
                share,
                peer,
                dry_run: true,
            })?
            .0
            .is_err()
        );
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
