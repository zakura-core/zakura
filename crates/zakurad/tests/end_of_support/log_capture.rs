//! Isolated log capture for the end-of-support integration tests.
//!
//! Each capture has its own subscriber and ignores `RUST_LOG`. Synchronous
//! tests scope the returned dispatch to their thread; asynchronous tests use
//! [`tracing::instrument::WithSubscriber`] to scope it to each future poll.

use std::{
    io::{self, Write},
    sync::{Arc, Mutex},
};

/// Log output from one subscriber, independent of concurrent tests.
pub(super) struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl CapturedLogs {
    pub(super) fn contains(&self, needle: &str) -> bool {
        let bytes = self
            .0
            .lock()
            .expect("no code panics while holding the log buffer lock");
        String::from_utf8_lossy(&bytes).contains(needle)
    }
}

/// Creates a fresh buffer and a subscriber with deterministic filtering.
pub(super) fn capture_logs() -> (CapturedLogs, tracing::Dispatch) {
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let writer_bytes = Arc::clone(&bytes);
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_ansi(false)
        .without_time()
        .with_writer(move || LogWriter(Arc::clone(&writer_bytes)))
        .finish();

    (CapturedLogs(bytes), tracing::Dispatch::new(subscriber))
}

struct LogWriter(Arc<Mutex<Vec<u8>>>);

impl Write for LogWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .expect("no code panics while holding the log buffer lock")
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn captures_are_isolated_across_threads() {
    let (logs, dispatch) = capture_logs();
    tracing::dispatcher::with_default(&dispatch, || {
        tracing::info!("first capture message");
        std::thread::spawn(|| {
            let (other_logs, other_dispatch) = capture_logs();
            tracing::dispatcher::with_default(&other_dispatch, || {
                tracing::warn!("second capture warning");
            });
            assert!(other_logs.contains("second capture warning"));
            assert!(!other_logs.contains("first capture message"));
        })
        .join()
        .expect("the other capture's assertions passed");
    });

    assert!(logs.contains("first capture message"));
    assert!(!logs.contains("second capture warning"));
}

#[tokio::test]
async fn captures_are_isolated_across_future_polls() {
    use tracing::instrument::WithSubscriber;

    let (first_logs, first_dispatch) = capture_logs();
    let (second_logs, second_dispatch) = capture_logs();
    let first = async {
        tracing::info!("first future before yield");
        tokio::task::yield_now().await;
        tracing::info!("first future after yield");
    }
    .with_subscriber(first_dispatch);
    let second = async {
        tracing::warn!("second future before yield");
        tokio::task::yield_now().await;
        tracing::warn!("second future after yield");
    }
    .with_subscriber(second_dispatch);

    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(first, second);
    })
    .await
    .expect("both capture futures finish after yielding once");

    assert!(first_logs.contains("first future before yield"));
    assert!(first_logs.contains("first future after yield"));
    assert!(!first_logs.contains("second future"));
    assert!(second_logs.contains("second future before yield"));
    assert!(second_logs.contains("second future after yield"));
    assert!(!second_logs.contains("first future"));
}
