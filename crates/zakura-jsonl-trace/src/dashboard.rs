//! Opt-in local dashboard events. Delivery never waits for the consumer.
//!
//! Set `DASHBOARD_EVENT_SOCKET` before startup to a Unix datagram socket owned
//! by the dashboard. Events are lossy under pressure. Sequence gaps expose loss
//! to the consumer, which must never present incomplete spans as complete ones.

use serde::Serialize;
mod batch;
mod block;
pub use batch::BatchObservation;
pub use block::BlockStage;

use std::{
    io::{self, Write},
    sync::{
        atomic::{AtomicU64, Ordering},
        OnceLock,
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

const MAX_EVENT_BYTES: usize = 8192;
static SINK: OnceLock<Option<DashboardSink>> = OnceLock::new();

/// Emit a small public telemetry event when the dashboard transport is enabled.
///
/// The closure is not evaluated when disabled. Callers must use bounded fields
/// and omit peer addresses, transaction contents, credentials, and raw errors.
/// Delivery failures are ignored so dashboard availability cannot stop the node.
pub fn emit<T: Serialize>(build: impl FnOnce() -> T) {
    if let Some(sink) = sink() {
        let _ = sink.send(&build());
    }
}

/// Whether the optional local event transport was configured successfully.
pub fn enabled() -> bool {
    sink().is_some()
}

fn sink() -> Option<&'static DashboardSink> {
    SINK.get_or_init(|| {
        let path = std::env::var_os("DASHBOARD_EVENT_SOCKET")?;
        DashboardSink::new(path.into()).ok()
    })
    .as_ref()
}

struct DashboardSink {
    #[cfg(unix)]
    socket: std::os::unix::net::UnixDatagram,
    path: std::path::PathBuf,
    started: Instant,
    sequence: AtomicU64,
    send_failures: AtomicU64,
}

#[derive(Serialize)]
struct Envelope<'a, T> {
    version: u8,
    process: &'static str,
    sequence: u64,
    monotonic_ns: u64,
    send_failures: u64,
    unix_ms: u64,
    event: &'a T,
}

impl DashboardSink {
    #[cfg(unix)]
    fn new(path: std::path::PathBuf) -> io::Result<Self> {
        let socket = std::os::unix::net::UnixDatagram::unbound()?;
        socket.set_nonblocking(true)?;
        Ok(Self {
            socket,
            path,
            started: Instant::now(),
            sequence: AtomicU64::new(0),
            send_failures: AtomicU64::new(0),
        })
    }

    #[cfg(not(unix))]
    fn new(_path: std::path::PathBuf) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "dashboard events require Unix sockets",
        ))
    }

    fn send<T: Serialize>(&self, event: &T) -> io::Result<()> {
        let result = self.send_inner(event);
        if result.is_err() {
            self.send_failures.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    fn send_inner<T: Serialize>(&self, event: &T) -> io::Result<()> {
        let envelope = Envelope {
            version: 1,
            process: crate::process_trace_id(),
            sequence: self.sequence.fetch_add(1, Ordering::Relaxed),
            send_failures: self.send_failures.load(Ordering::Relaxed),
            monotonic_ns: u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            unix_ms: u64::try_from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis(),
            )
            .unwrap_or(u64::MAX),
            event,
        };
        let mut bytes = BoundedBytes(Vec::with_capacity(512));
        serde_json::to_writer(&mut bytes, &envelope)?;
        #[cfg(unix)]
        self.socket.send_to(&bytes.0, &self.path)?;
        Ok(())
    }
}

struct BoundedBytes(Vec<u8>);
impl Write for BoundedBytes {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_EVENT_BYTES.saturating_sub(self.0.len()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "dashboard event too large",
            ));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::{os::unix::net::UnixDatagram, time::Duration};

    #[test]
    fn delivery_survives_missing_receiver_and_exposes_lost_events() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("events.sock");
        let sink = DashboardSink::new(path.clone()).unwrap();
        assert!(sink.send(&"lost").is_err());
        let receiver = UnixDatagram::bind(&path).unwrap();
        receiver
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        sink.send(&"received").unwrap();
        let mut buffer = [0; MAX_EVENT_BYTES];
        let size = receiver.recv(&mut buffer).unwrap();
        let event: serde_json::Value = serde_json::from_slice(&buffer[..size]).unwrap();
        assert_eq!(event["sequence"], 1);
        assert_eq!(event["send_failures"], 1);
        assert_eq!(event["event"], "received");
        assert_eq!(event["process"], crate::process_trace_id());
        assert!(event["unix_ms"].as_u64().unwrap() > 0);
    }

    #[test]
    fn oversized_events_do_not_allocate_an_unbounded_output() {
        let mut output = BoundedBytes(Vec::new());
        assert!(output.write(&[0; MAX_EVENT_BYTES + 1]).is_err());
        assert!(output.0.is_empty());
    }

    #[test]
    fn full_receiver_returns_instead_of_waiting() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("events.sock");
        let _receiver = UnixDatagram::bind(&path).unwrap();
        let sink = DashboardSink::new(path).unwrap();
        let mut saw_backpressure = false;
        for _ in 0..100_000 {
            if sink.send(&vec![0_u8; 128]).is_err() {
                saw_backpressure = true;
                break;
            }
        }
        assert!(saw_backpressure);
    }
}
