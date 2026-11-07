//! The Tokio runtime for server commands.
//!
//! The tokio runtime is used for:
//! - non-blocking async tasks, via [`Future`]s and
//! - blocking network and file tasks, via [`spawn_blocking`](tokio::task::spawn_blocking).
//!
//! The rayon thread pool is used for:
//! - long-running CPU-bound tasks like cryptography, via [`rayon::spawn_fifo`].

use std::{future::Future, time::Duration};

use eyre::Report;
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

/// When Zebra is shutting down, wait this long for tokio tasks to finish.
const TOKIO_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(20);

/// Tokio's default blocking-thread limit. Blocking work other than state reads keeps it.
const DEFAULT_MAX_BLOCKING_THREADS: usize = 512;

/// Returns a new multi-threaded Tokio runtime with all drivers enabled.
///
/// The blocking pool adds `read_workers` threads to Tokio's default, so admitted state
/// reads cannot starve other blocking work. Tokio starts blocking threads on demand, so
/// idle read threads cost nothing.
pub(crate) fn new_runtime(read_workers: usize) -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .max_blocking_threads(DEFAULT_MAX_BLOCKING_THREADS.saturating_add(read_workers))
        .enable_all()
        .build()
        .expect("runtime building should not fail")
}

/// Zakurad's graceful shutdown function, blocks until one of the supported
/// shutdown signals is received.
async fn shutdown() {
    imp::shutdown().await;
}

/// Extension trait for running the server command on the Tokio runtime.
pub(crate) trait RuntimeRun {
    /// Runs the future returned by `run` until it finishes or a shutdown signal arrives,
    /// then stops the runtime, waiting up to [`TOKIO_SHUTDOWN_TIMEOUT`] for its tasks.
    ///
    /// Returns the future's result, or `Ok` if a signal arrived during startup.
    fn run_with_graceful_shutdown<F>(
        self,
        run: impl FnOnce(CancellationToken, CancellationToken) -> F,
    ) -> Result<(), Report>
    where
        F: Future<Output = Result<(), Report>>;
}

impl RuntimeRun for Runtime {
    fn run_with_graceful_shutdown<F>(
        self,
        run: impl FnOnce(CancellationToken, CancellationToken) -> F,
    ) -> Result<(), Report>
    where
        F: Future<Output = Result<(), Report>>,
    {
        let shutdown_token = CancellationToken::new();
        // cancelled means shutdown must await the root future's cleanup.
        let shutdown_cleanup_required = CancellationToken::new();
        let fut = run(shutdown_token.clone(), shutdown_cleanup_required.clone());
        let result = self.block_on(run_until_shutdown(
            fut,
            shutdown(),
            shutdown_token,
            shutdown_cleanup_required,
        ));

        finish_runtime(self, result)
    }
}

pub(crate) async fn run_until_shutdown(
    fut: impl Future<Output = Result<(), Report>>,
    shutdown_signal: impl Future<Output = ()>,
    shutdown_token: CancellationToken,
    shutdown_cleanup_required: CancellationToken,
) -> Result<(), Report> {
    tokio::pin!(fut);
    tokio::pin!(shutdown_signal);

    tokio::select! {
        biased;
        _ = &mut shutdown_signal => {
            shutdown_token.cancel();
            if shutdown_cleanup_required.is_cancelled() {
                fut.await
            } else {
                Ok(())
            }
        }
        result = &mut fut => result,
    }
}

fn finish_runtime(runtime: Runtime, result: Result<(), Report>) -> Result<(), Report> {
    // Don't wait for long blocking tasks before shutting down
    info!(
        ?TOKIO_SHUTDOWN_TIMEOUT,
        "waiting for async tokio tasks to shut down"
    );
    runtime.shutdown_timeout(TOKIO_SHUTDOWN_TIMEOUT);

    match &result {
        Ok(()) => {
            info!("shutting down Zakura");
        }
        Err(error) => {
            warn!(?error, "shutting down Zakura due to an error");
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use std::future;

    use tokio::sync::oneshot;

    use super::*;

    /// Admitted state reads leave Tokio's default blocking threads to other work.
    #[test]
    fn saturated_reads_leave_blocking_threads_for_other_work() {
        let limit = Duration::from_secs(5);
        // The largest limit that a 512-thread pool shared with other work admitted.
        let state = zakura_state::Config {
            read_concurrency_limit: std::num::NonZeroU16::new(508).expect("508 is nonzero"),
            ..Default::default()
        };
        let read_workers = state.read_worker_count();
        let runtime = new_runtime(read_workers);

        runtime.block_on(async {
            let (started, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
            let mut releases = Vec::with_capacity(read_workers);
            // Admitted reads run as `spawn_blocking` jobs, one per worker slot.
            for _ in 0..read_workers {
                let (release, released) = oneshot::channel::<()>();
                releases.push(release);
                let started = started.clone();
                tokio::task::spawn_blocking(move || {
                    started
                        .send(())
                        .expect("the test waits for every read to start");
                    // Dropping the sender ends the read.
                    let _ = released.blocking_recv();
                });
            }
            for _ in 0..read_workers {
                tokio::time::timeout(limit, started_rx.recv())
                    .await
                    .expect("every admitted read gets a blocking thread")
                    .expect("read jobs hold a sender until they start");
            }

            tokio::time::timeout(limit, tokio::task::spawn_blocking(|| ()))
                .await
                .expect("other blocking work finishes while reads fill their slots")
                .expect("the unrelated job does not panic");
            drop(releases);
        });
    }

    #[tokio::test]
    async fn graceful_shutdown_waits_for_root_future_cleanup() {
        let shutdown = CancellationToken::new();
        let future_shutdown = shutdown.clone();
        let shutdown_cleanup_required = CancellationToken::new();
        shutdown_cleanup_required.cancel();
        let (cleanup_tx, cleanup_rx) = oneshot::channel();

        let fut = async move {
            future_shutdown.cancelled().await;
            cleanup_tx.send(()).expect("cleanup receiver is open");
            Ok(())
        };

        run_until_shutdown(fut, future::ready(()), shutdown, shutdown_cleanup_required)
            .await
            .expect("shutdown should succeed");
        cleanup_rx.await.expect("root future ran cleanup");
    }

    #[tokio::test]
    async fn shutdown_during_startup_does_not_wait_for_root_future() {
        let shutdown = CancellationToken::new();
        let shutdown_cleanup_required = CancellationToken::new();

        run_until_shutdown(
            future::pending(),
            future::ready(()),
            shutdown.clone(),
            shutdown_cleanup_required,
        )
        .await
        .expect("shutdown should succeed");

        assert!(shutdown.is_cancelled());
    }
}

#[cfg(unix)]
mod imp {
    use tokio::signal::unix::{signal, SignalKind};

    pub(super) async fn shutdown() {
        // If both signals are received, select! chooses one of them at random.
        tokio::select! {
            // SIGINT  - Terminal interrupt signal. Typically generated by shells in response to Ctrl-C.
            _ = sig(SignalKind::interrupt(), "SIGINT") => {}
            // SIGTERM - Standard shutdown signal used by process launchers.
            _ = sig(SignalKind::terminate(), "SIGTERM") => {}
        };
    }

    #[instrument]
    async fn sig(kind: SignalKind, name: &'static str) {
        // Create a Future that completes the first
        // time the process receives 'sig'.
        signal(kind)
            .expect("Failed to register signal handler")
            .recv()
            .await;

        zakura_chain::shutdown::set_shutting_down();

        info!(
            // use target to remove 'imp' from output
            target: "zakurad::signal",
            "received {}, starting shutdown",
            name,
        );
    }
}

#[cfg(not(unix))]
mod imp {

    pub(super) async fn shutdown() {
        //  Wait for Ctrl-C in Windows terminals.
        // (Zebra doesn't support NT Service control messages. Use a service wrapper for long-running instances.)
        tokio::signal::ctrl_c()
            .await
            .expect("listening for ctrl-c signal should never fail");

        zakura_chain::shutdown::set_shutting_down();

        info!(
            // use target to remove 'imp' from output
            target: "zakurad::signal",
            "received Ctrl-C, starting shutdown",
        );
    }
}
