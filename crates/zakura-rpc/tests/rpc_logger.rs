//! Tests that RPC payload logging only does payload work for enabled TRACE events.
//!
//! These tests use their own binary, so they can install an allocation tracker
//! and thread-scoped subscribers without affecting the other RPC tests.

use std::{
    borrow::Cow,
    collections::VecDeque,
    future::{ready, Future},
    io,
    sync::{Arc, Mutex, PoisonError},
};

use futures::executor::block_on;
use jsonrpsee::{
    core::middleware::{Batch, Notification},
    server::middleware::rpc::RpcServiceT,
    types::{Id, Request},
    MethodResponse, ResponsePayload,
};
use serde_json::value::to_raw_value;
use tracing::level_filters::{LevelFilter, STATIC_MAX_LEVEL};
use tracing_subscriber::{fmt::MakeWriter, EnvFilter};

use zakura_rpc::server::{rpc_batch::RpcBatchMiddleware, rpc_logger::RpcLoggerMiddleware};
use zakura_test::allocations::{measure, AllocationStats, TrackingAllocator};

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator;

/// Characters in each large request or response payload.
const PAYLOAD_CHARS: usize = 1 << 20;

/// Largest single allocation the logger may make while handling a large
/// payload, which is far smaller than a copy of the payload.
const MAX_LOGGER_ALLOCATION: usize = 64 * 1024;

/// Characters of each payload the logger writes.
const MAX_LOG_CHARS: usize = 1024;

/// An inner RPC service that returns prebuilt responses in order.
///
/// Responses are moved out rather than cloned, so returning one doesn't
/// allocate during a measurement.
#[derive(Clone)]
struct PrebuiltResponses(Arc<Mutex<VecDeque<MethodResponse>>>);

impl PrebuiltResponses {
    /// Returns a service that answers calls and batches with `responses`.
    fn new(responses: impl IntoIterator<Item = MethodResponse>) -> Self {
        Self(Arc::new(Mutex::new(responses.into_iter().collect())))
    }

    /// Returns the next prebuilt response.
    fn next(&self) -> MethodResponse {
        self.0
            .lock()
            .expect("test service lock is never poisoned")
            .pop_front()
            .expect("tests prebuild a response for every call and batch")
    }
}

impl RpcServiceT for PrebuiltResponses {
    type MethodResponse = MethodResponse;
    type NotificationResponse = MethodResponse;
    type BatchResponse = MethodResponse;

    fn call<'a>(&self, _request: Request<'a>) -> impl Future<Output = MethodResponse> + Send + 'a {
        ready(self.next())
    }

    fn batch<'a>(&self, _batch: Batch<'a>) -> impl Future<Output = MethodResponse> + Send + 'a {
        ready(self.next())
    }

    fn notification<'a>(
        &self,
        _notification: Notification<'a>,
    ) -> impl Future<Output = MethodResponse> + Send + 'a {
        ready(MethodResponse::notification())
    }
}

/// Log output written by a test subscriber.
#[derive(Clone)]
struct Output(Arc<Mutex<Vec<u8>>>);

impl Output {
    /// Returns an empty output with room for a few log lines, so writing them
    /// doesn't make a large allocation during a measurement.
    fn new() -> Self {
        Self(Arc::new(Mutex::new(Vec::with_capacity(
            MAX_LOGGER_ALLOCATION / 2,
        ))))
    }

    /// Returns every line written so far.
    fn lines(&self) -> Vec<String> {
        let output = self.0.lock().expect("test output lock is never poisoned");
        String::from_utf8_lossy(&output)
            .lines()
            .map(str::to_owned)
            .collect()
    }

    /// Discards the output written so far, keeping its capacity.
    fn clear(&self) {
        self.0
            .lock()
            .expect("test output lock is never poisoned")
            .clear();
    }
}

impl io::Write for Output {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .map_err(|_| io::Error::other("test output lock was poisoned"))?
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for Output {
    type Writer = Self;

    fn make_writer(&'a self) -> Self {
        self.clone()
    }
}

/// Held while a test's subscriber is installed, so the tests in this binary run
/// one at a time.
///
/// Tracing caches whether each callsite is enabled across every live
/// subscriber in the process, so a subscriber installed by a concurrent test
/// can leave another test's events disabled.
static SUBSCRIBER_LOCK: Mutex<()> = Mutex::new(());

/// Runs `test` with a thread-scoped subscriber that writes events matching
/// `filter` to `output`.
fn with_subscriber<T>(filter: &str, output: &Output, test: impl FnOnce() -> T) -> T {
    let _serial = SUBSCRIBER_LOCK
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(filter))
        .with_writer(output.clone())
        .with_ansi(false)
        .without_time()
        .finish();
    tracing::subscriber::with_default(subscriber, test)
}

/// Returns whether this build can emit TRACE events at all.
///
/// Release builds with `release_max_level_info` compile them out.
fn trace_is_compiled_in() -> bool {
    STATIC_MAX_LEVEL >= LevelFilter::TRACE
}

/// Returns a `probe` request whose params are a string of `chars` copies of `c`.
fn request(c: char, chars: usize) -> Request<'static> {
    let params = to_raw_value(&[c.to_string().repeat(chars)]).expect("a string serializes");
    Request::owned("probe".to_owned(), Some(params), Id::Number(1))
}

/// Returns a `probe` notification whose params are a string of `chars` copies of `c`.
fn notification(c: char, chars: usize) -> Notification<'static> {
    let params = to_raw_value(&[c.to_string().repeat(chars)]).expect("a string serializes");
    Notification::new(Cow::Borrowed("probe"), Some(Cow::Owned(params)))
}

/// Returns a successful response whose result is a string of `chars` copies of `c`.
fn response(c: char, chars: usize) -> MethodResponse {
    MethodResponse::response(
        Id::Number(1),
        ResponsePayload::success(c.to_string().repeat(chars)),
        usize::MAX,
    )
}

/// Returns a batch containing one large request.
fn batch() -> Batch<'static> {
    let mut batch = Batch::new();
    batch.push(request('x', PAYLOAD_CHARS));
    batch
}

/// Calls `logger` once with small payloads, so first-use allocations by the
/// executor, tracing and the subscriber happen before any measurement.
fn warm_up(output: &Output) {
    let logger = RpcLoggerMiddleware::new(
        PrebuiltResponses::new([response('w', 1), response('w', 1)]),
        MAX_LOG_CHARS,
    );
    block_on(logger.call(request('w', 1)));
    block_on(logger.notification(notification('w', 1)));
    let mut small_batch = Batch::new();
    small_batch.push(request('w', 1));
    block_on(logger.batch(small_batch));
    output.clear();
}

/// Asserts that a measured operation made no allocation as large as a payload copy.
#[track_caller]
fn assert_no_payload_copy(operation: &str, stats: AllocationStats) {
    assert!(
        stats.largest_request < MAX_LOGGER_ALLOCATION,
        "{operation} allocated {} bytes at once, which means it copied a payload",
        stats.largest_request,
    );
}

/// Returns the payload text of the only output line containing `message`.
#[track_caller]
fn logged_payload(lines: &[String], message: &str) -> String {
    let matching: Vec<_> = lines
        .iter()
        .filter_map(|line| line.split_once(message).map(|(_, payload)| payload))
        .collect();
    assert_eq!(
        matching.len(),
        1,
        "expected one `{message}` line in {lines:#?}"
    );
    matching[0].to_owned()
}

/// With TRACE disabled, the logger forwards large payloads without serializing
/// or copying them, and writes nothing.
#[test]
fn disabled_trace_logging_does_no_payload_work() {
    let output = Output::new();
    with_subscriber("info", &output, || {
        warm_up(&output);

        let expected = response('y', PAYLOAD_CHARS).into_json();
        let logger = RpcLoggerMiddleware::new(
            PrebuiltResponses::new([response('y', PAYLOAD_CHARS), response('y', PAYLOAD_CHARS)]),
            MAX_LOG_CHARS,
        );

        let call = request('x', PAYLOAD_CHARS);
        let (call_response, stats) = measure(|| block_on(logger.call(call)));
        assert_no_payload_copy("a call", stats);
        assert_eq!(call_response.as_json().get(), expected.get());

        let notification = notification('x', PAYLOAD_CHARS);
        let (notification_response, stats) =
            measure(|| block_on(logger.notification(notification)));
        assert_no_payload_copy("a notification", stats);
        assert!(notification_response.is_notification());

        let batch = batch();
        let (batch_response, stats) = measure(|| block_on(logger.batch(batch)));
        assert_no_payload_copy("a batch", stats);
        assert_eq!(batch_response.as_json().get(), expected.get());
    });

    assert_eq!(output.lines(), Vec::<String>::new());
}

/// With TRACE enabled, the logger writes every payload truncated at a character
/// boundary inside its span, and logs responses without copying them.
#[test]
fn enabled_trace_logging_writes_truncated_payloads() {
    if !trace_is_compiled_in() {
        return;
    }

    let output = Output::new();
    with_subscriber("jsonrpsee=trace", &output, || {
        warm_up(&output);

        let logger = RpcLoggerMiddleware::new(
            PrebuiltResponses::new([response('é', PAYLOAD_CHARS), response('é', PAYLOAD_CHARS)]),
            MAX_LOG_CHARS,
        );

        // Serializing an enabled request is expected, so only the response is measured.
        let call = logger.call(request('é', PAYLOAD_CHARS));
        let (_, stats) = measure(|| block_on(call));
        assert_no_payload_copy("logging a response", stats);

        block_on(logger.notification(notification('é', PAYLOAD_CHARS)));

        let batch = logger.batch(batch());
        let (_, stats) = measure(|| block_on(batch));
        assert_no_payload_copy("logging a batch response", stats);
    });

    let lines = output.lines();
    for message in [
        "request = ",
        "response = ",
        "notification request = ",
        "batch request = ",
        "batch response = ",
    ] {
        let payload = logged_payload(&lines, &format!("jsonrpsee: {message}"));
        assert_eq!(
            payload.chars().count(),
            MAX_LOG_CHARS,
            "`{message}` should be truncated to {MAX_LOG_CHARS} characters"
        );
    }
    for message in ["jsonrpsee: request = ", "jsonrpsee: response = "] {
        assert!(
            lines.iter().any(
                |line| line.contains("method_call{method=\"probe\"}") && line.contains(message)
            ),
            "`{message}` should be logged inside the call's span: {lines:#?}"
        );
    }
}

/// A filter that only enables TRACE inside the logger's call span still logs
/// that call's request and response.
#[test]
fn span_scoped_trace_filter_logs_payloads() {
    if !trace_is_compiled_in() {
        return;
    }

    let output = Output::new();
    with_subscriber("warn,[method_call]=trace", &output, || {
        let logger =
            RpcLoggerMiddleware::new(PrebuiltResponses::new([response('y', 10)]), MAX_LOG_CHARS);
        block_on(logger.call(request('x', 10)));
    });

    let lines = output.lines();
    logged_payload(&lines, "jsonrpsee: request = ");
    logged_payload(&lines, "jsonrpsee: response = ");
}

/// Behind the batch middleware, each batched call is logged once, and the batch
/// isn't also logged as a whole.
#[test]
fn batched_calls_are_logged_once_each() {
    if !trace_is_compiled_in() {
        return;
    }

    let output = Output::new();
    with_subscriber("jsonrpsee=trace", &output, || {
        let service = RpcBatchMiddleware::new(
            RpcLoggerMiddleware::new(
                PrebuiltResponses::new([response('a', 10), response('b', 10)]),
                MAX_LOG_CHARS,
            ),
            usize::MAX,
        );
        let mut batch = Batch::new();
        batch.push(request('x', 10));
        batch.push(request('z', 10));
        block_on(service.batch(batch));
    });

    let lines = output.lines();
    let count = |message: &str| lines.iter().filter(|line| line.contains(message)).count();
    assert_eq!(count("jsonrpsee: request = "), 2, "{lines:#?}");
    assert_eq!(count("jsonrpsee: response = "), 2, "{lines:#?}");
    assert_eq!(count("batch"), 0, "{lines:#?}");
}
