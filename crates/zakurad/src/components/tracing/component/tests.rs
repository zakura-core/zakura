//! Tests for shutting down the log writer and the OpenTelemetry exporter.

use std::{
    io,
    panic::{self, AssertUnwindSafe},
    sync::{Arc, Mutex},
};

use opentelemetry_sdk::{
    error::OTelSdkResult,
    trace::{SdkTracerProvider, SpanData, SpanExporter},
};

use super::*;
use crate::components::tracing::non_blocking::{non_blocking, NonBlocking};

/// What happened during shutdown, in order.
type Events = Arc<Mutex<Vec<&'static str>>>;

fn record(events: &Events, event: &'static str) {
    events
        .lock()
        .expect("events are only recorded in code that doesn't panic")
        .push(event);
}

/// An exporter that records its shutdown, and logs a line through the log writer while
/// it shuts down, like the real exporter logs its errors.
#[derive(Debug)]
struct RecordingExporter {
    events: Events,
    log: Mutex<NonBlocking>,
}

// Test coordination failures must panic.
#[allow(clippy::unwrap_in_result)]
impl SpanExporter for RecordingExporter {
    async fn export(&self, _batch: Vec<SpanData>) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(&self, _timeout: std::time::Duration) -> OTelSdkResult {
        record(&self.events, "exporter shut down");
        let _ = self
            .log
            .lock()
            .expect("the log writer is only locked here")
            .write_all(b"exporter shutdown log line\n");
        Ok(())
    }
}

/// The log output, which records when the background log writer drops it.
struct RecordingOutput {
    events: Events,
    bytes: Arc<Mutex<Vec<u8>>>,
}

// Test coordination failures must panic: the log writer discards output errors.
#[allow(clippy::unwrap_in_result)]
impl Write for RecordingOutput {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes
            .lock()
            .expect("output is only recorded in code that doesn't panic")
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for RecordingOutput {
    fn drop(&mut self) {
        record(&self.events, "log writer stopped");
    }
}

/// What a test checks after `Tracing` has shut down.
struct Recorded {
    /// A second handle to the exporter's provider, like the one the global subscriber keeps.
    subscriber_provider: SdkTracerProvider,
    events: Events,
    output: Arc<Mutex<Vec<u8>>>,
}

/// Returns a `Tracing` with a recording exporter and log output.
fn recording_tracing() -> (Tracing, Recorded) {
    let events = Events::default();
    let output = Arc::new(Mutex::new(Vec::new()));
    let (writer, guard) = non_blocking(
        RecordingOutput {
            events: events.clone(),
            bytes: output.clone(),
        },
        100,
    )
    .expect("the log writer thread starts");
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(RecordingExporter {
            events: events.clone(),
            log: Mutex::new(writer),
        })
        .build();

    let tracing = Tracing {
        otel_provider: Some(provider.clone()),
        _guard: Some(guard),
    };
    let recorded = Recorded {
        subscriber_provider: provider,
        events,
        output,
    };
    (tracing, recorded)
}

impl Recorded {
    /// Checks that `Tracing` shut the exporter down exactly once, before the log writer
    /// stopped, and that the line the exporter logged while shutting down was written.
    fn assert_shut_down_once(self) {
        let expected = ["exporter shut down", "log writer stopped"];
        assert_eq!(
            *self.events.lock().expect("no event recorder panicked"),
            expected,
        );

        // Dropping the last provider handle must not shut the exporter down again.
        drop(self.subscriber_provider);
        assert_eq!(
            *self.events.lock().expect("no event recorder panicked"),
            expected,
        );

        let output = self.output.lock().expect("no output recorder panicked");
        assert_eq!(
            String::from_utf8_lossy(&output),
            "exporter shutdown log line\n"
        );
    }
}

#[test]
fn dropping_tracing_shuts_down_the_exporter_once() {
    let (tracing, recorded) = recording_tracing();

    drop(tracing);

    recorded.assert_shut_down_once();
}

#[test]
fn unwinding_past_tracing_shuts_down_the_exporter_once() {
    let (tracing, recorded) = recording_tracing();

    let result = panic::catch_unwind(AssertUnwindSafe(move || {
        let _tracing = tracing;
        panic!("a command panics while tracing is active");
    }));
    assert!(result.is_err(), "the command panicked");

    recorded.assert_shut_down_once();
}

#[test]
fn explicit_shutdown_then_drop_shuts_down_the_exporter_once() {
    let (mut tracing, recorded) = recording_tracing();

    tracing.shutdown();
    tracing.shutdown();
    drop(tracing);

    recorded.assert_shut_down_once();
}
