//! A local collector and a separately runnable read-only explorer.

mod attribution;
mod cpu;
mod store;
mod web;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Receive bounded local datagrams and retain summaries and compressed detail.
    Collect {
        #[arg(long)]
        store: PathBuf,
        #[arg(long)]
        socket: PathBuf,
        /// All profiler data must also live inside a filesystem/project quota of this size.
        #[arg(long, default_value_t = 100_000_000_000)]
        budget_bytes: u64,
    },
    /// Read-only localhost explorer. Use SSH forwarding for remote access.
    Serve {
        #[arg(long)]
        store: PathBuf,
        #[arg(long, default_value_t = 8787)]
        port: u16,
    },
    /// Emit a bounded daily JSON report to stdout.
    Report {
        #[arg(long)]
        store: PathBuf,
        #[arg(long)]
        run: Option<String>,
    },
    /// Exclude a known contaminated recording from timing statistics, retaining its evidence.
    Exclude {
        #[arg(long)]
        store: PathBuf,
        #[arg(long)]
        run: String,
        #[arg(long)]
        attempt: u64,
        #[arg(long)]
        reason: String,
    },
    /// Backfill a proven startup boundary for a recording made before automatic readiness.
    StartupBoundary {
        #[arg(long)]
        store: PathBuf,
        #[arg(long)]
        run: String,
        /// First eligible router-entry offset, in microseconds from the run start.
        #[arg(long)]
        ready_us: u64,
    },
    /// Attach Git-verified provenance to a legacy recording. Existing provenance is immutable.
    Source {
        #[arg(long)]
        store: PathBuf,
        #[arg(long)]
        run: String,
        #[arg(long)]
        expected_build: String,
        #[arg(long)]
        base_commit: String,
        #[arg(long)]
        commit: String,
    },
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::print_stdout)]
async fn main() -> Result<()> {
    match Args::parse().command {
        Command::Collect {
            store,
            socket,
            budget_bytes,
        } => {
            let stopping = Arc::new(AtomicBool::new(false));
            let worker_stop = stopping.clone();
            let mut worker = tokio::task::spawn_blocking(move || {
                collect(store, socket, budget_bytes, worker_stop)
            });
            #[cfg(unix)]
            {
                let mut term =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
                tokio::select! { result = &mut worker => { return result?; }, _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
            }
            #[cfg(not(unix))]
            tokio::signal::ctrl_c().await?;
            stopping.store(true, Ordering::Release);
            worker.await?
        }
        Command::Serve { store, port } => web::serve(store, port).await,
        Command::Report { store, run } => {
            let reader = store::Reader::open(&store)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&reader.home(run.as_deref(), "semantic")?)?
            );
            Ok(())
        }
        Command::Exclude {
            store,
            run,
            attempt,
            reason,
        } => store::exclude_timing(&store, &run, attempt, &reason),
        Command::StartupBoundary {
            store,
            run,
            ready_us,
        } => store::startup_boundary(&store, &run, ready_us),
        Command::Source {
            store,
            run,
            expected_build,
            base_commit,
            commit,
        } => store::source(
            &store,
            &run,
            &expected_build,
            zakura_jsonl_trace::block_profile::Source {
                base_commit,
                commit,
            },
        ),
    }
}

#[cfg(unix)]
fn collect(
    path: PathBuf,
    socket_path: PathBuf,
    budget: u64,
    stopping: Arc<AtomicBool>,
) -> Result<()> {
    use std::os::unix::{fs::PermissionsExt, net::UnixDatagram};
    let mut store = store::Store::open(&path, budget)?;
    use fs2::FileExt;
    let lock_path = socket_path.with_extension("lock");
    let prior_lock = lock_path.exists();
    let socket_lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)?;
    socket_lock
        .try_lock_exclusive()
        .context("profile socket belongs to an active collector")?;
    if let Ok(metadata) = std::fs::symlink_metadata(&socket_path) {
        use std::os::unix::fs::FileTypeExt;
        anyhow::ensure!(
            metadata.file_type().is_socket() && prior_lock,
            "refusing to remove an unrecognized socket path"
        );
        // Only our prior socket, protected by the same exclusive per-socket lock.
        std::fs::remove_file(&socket_path)?;
    }
    let socket = UnixDatagram::bind(&socket_path).context("bind profile socket")?;
    std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o660))?;
    socket.set_read_timeout(Some(Duration::from_millis(200)))?;
    let mut bytes = [0u8; 8193];
    let mut tick = std::time::Instant::now();
    let mut maintenance = std::time::Instant::now();
    let mut errors = 0u64;
    while !stopping.load(Ordering::Acquire) {
        let mut frames = Vec::with_capacity(store::MAX_INGEST_BATCH);
        let mut decode = |bytes: &[u8], n| {
            if n <= 8192 {
                match serde_json::from_slice(&bytes[..n]) {
                    Ok(frame) => frames.push(frame),
                    Err(_) => errors = errors.saturating_add(1),
                }
            } else {
                errors = errors.saturating_add(1);
            }
        };
        // Wait for the first datagram, then drain only the already queued burst.
        // Neither the receive batch nor the SQLite transaction can grow without bound.
        match socket.recv(&mut bytes) {
            Ok(n) => {
                decode(&bytes, n);
                socket.set_nonblocking(true)?;
                for _ in 1..store::MAX_INGEST_BATCH {
                    match socket.recv(&mut bytes) {
                        Ok(n) => decode(&bytes, n),
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                        Err(error) => return Err(error.into()),
                    }
                }
                socket.set_nonblocking(false)?;
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(error) => return Err(error.into()),
        }
        if !frames.is_empty() {
            errors = errors.saturating_add(store.ingest_batch(frames)?);
        }
        if tick.elapsed() >= Duration::from_secs(1) {
            if store.flush().is_err() {
                errors = errors.saturating_add(1);
            }
            store.health(errors)?;
            tick = std::time::Instant::now();
        }
        if maintenance.elapsed() >= Duration::from_secs(10) {
            store.prune()?;
            maintenance = std::time::Instant::now();
        }
    }
    store.flush()?;
    store.health(errors)?;
    store.checkpoint()?;
    std::fs::remove_file(socket_path)?;
    Ok(())
}

#[cfg(not(unix))]
fn collect(_: PathBuf, _: PathBuf, _: u64, _: Arc<AtomicBool>) -> Result<()> {
    anyhow::bail!("the local datagram collector requires Unix")
}
