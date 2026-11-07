//! Unconfirmed-row accounting through the public API.
//!
//! Every scenario reads the process-wide [`trace_rows_unconfirmed`] count, so they run in
//! sequence inside one test.

use std::{path::Path, time::Duration};

use serde::Serialize;
use serde_json::Value;
use tokio::sync::mpsc;
use zakura_jsonl_trace::{
    impl_jsonl_trace_event, trace_rows_unconfirmed, JsonlEventEmitter, JsonlTraceConfig,
    JsonlTraceSendError, JsonlTraceTable, JsonlTracer, JsonlWriteEvent,
};

const TABLE: JsonlTraceTable = JsonlTraceTable::new("unconfirmed", "unconfirmed.jsonl");

#[derive(Serialize)]
struct Row {
    event: &'static str,
}

impl_jsonl_trace_event!(Row, TABLE);

fn row(event: &'static str) -> impl FnOnce() -> Row {
    move || Row { event }
}

fn raw_row() -> JsonlWriteEvent {
    JsonlWriteEvent {
        table: TABLE.table(),
        file_name: TABLE.file_name(),
        line: br#"{"event":"raw"}"#.to_vec(),
    }
}

fn written_rows(trace_dir: &Path) -> Vec<Value> {
    std::fs::read_to_string(trace_dir.join(TABLE.file_name()))
        .expect("the writer created the table file")
        .lines()
        .map(|line| serde_json::from_str(line).expect("every written line is JSON"))
        .collect()
}

async fn wait_until_writer_stops(tracer: &JsonlTracer) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while tracer.is_enabled() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("a writer whose only table failed stops");
}

#[tokio::test]
async fn unconfirmed_rows_are_counted_only_while_tracing_is_on() {
    disabled_tracing_counts_nothing();
    full_and_closed_queues_count_each_refusal_once();
    a_refused_send_can_be_retried_so_the_count_is_not_a_loss_count();
    write_failure_counts_the_failed_row_and_the_rows_left_queued().await;
    loss_after_the_last_written_row_is_counted_but_not_reported_in_band().await;
}

fn disabled_tracing_counts_nothing() {
    let before = trace_rows_unconfirmed();

    JsonlEventEmitter::noop().emit_event(row("disabled"));
    JsonlEventEmitter::noop().emit_with(TABLE, |_| {});
    let _ = JsonlTracer::noop().try_send(raw_row());

    assert_eq!(trace_rows_unconfirmed(), before);
}

fn full_and_closed_queues_count_each_refusal_once() {
    let (full_tx, _full_rx) = mpsc::channel(1);
    let full = JsonlEventEmitter::new(JsonlTracer::new(full_tx), "full");
    full.emit_event(row("fills the queue"));

    let before = trace_rows_unconfirmed();
    full.emit_event(row("refused by a full queue"));
    assert_eq!(trace_rows_unconfirmed(), before + 1);
    let _ = full.tracer().try_send(raw_row());
    assert_eq!(trace_rows_unconfirmed(), before + 2);

    let (closed_tx, closed_rx) = mpsc::channel(1);
    drop(closed_rx);
    let closed = JsonlEventEmitter::new(JsonlTracer::new(closed_tx), "closed");

    let before = trace_rows_unconfirmed();
    closed.emit_event(row("refused by a closed queue"));
    closed.emit_with(TABLE, |_| {});
    assert_eq!(trace_rows_unconfirmed(), before + 2);
}

fn a_refused_send_can_be_retried_so_the_count_is_not_a_loss_count() {
    let (tx, mut rx) = mpsc::channel(1);
    let tracer = JsonlTracer::new(tx);
    tracer
        .try_send(raw_row())
        .expect("the queue has room for one row");

    let before = trace_rows_unconfirmed();
    let refused = match tracer.try_send(raw_row()) {
        Err(JsonlTraceSendError::Full(refused)) => refused,
        other => panic!("a full queue refuses the row, got {other:?}"),
    };
    let refused = match tracer.try_send(refused) {
        Err(JsonlTraceSendError::Full(refused)) => refused,
        other => panic!("a full queue refuses the retry too, got {other:?}"),
    };
    assert_eq!(
        trace_rows_unconfirmed(),
        before + 2,
        "each refusal of the same row counts"
    );

    rx.try_recv().expect("the first row is still queued");
    tracer
        .try_send(refused)
        .expect("the retry fits once the queue has room");
    assert_eq!(
        rx.try_recv().expect("the retried row was delivered"),
        raw_row()
    );
    assert_eq!(
        trace_rows_unconfirmed(),
        before + 2,
        "the row was delivered after all, but its two refusals stay counted"
    );
}

async fn write_failure_counts_the_failed_row_and_the_rows_left_queued() {
    let dir = tempfile::tempdir().expect("tempdir");
    let blocker = dir.path().join("not-a-directory");
    std::fs::write(&blocker, b"").expect("a regular file blocks the trace directory");

    // One row per batch, so the writer stops while two rows are still queued.
    let config = JsonlTraceConfig {
        max_batch_events: 1,
        ..JsonlTraceConfig::default()
    };
    let tracer = JsonlTracer::spawn_with_config(blocker.join("traces"), config);
    let emitter = JsonlEventEmitter::new(tracer.clone(), "write-failure");

    // The test runtime is single-threaded, so the writer runs only after all three rows
    // are queued.
    let before = trace_rows_unconfirmed();
    for _ in 0..3 {
        emitter.emit_event(row("discarded by the failed table"));
    }
    wait_until_writer_stops(&tracer).await;
    assert_eq!(trace_rows_unconfirmed(), before + 3);

    emitter.emit_event(row("refused by the stopped writer"));
    assert_eq!(trace_rows_unconfirmed(), before + 4);
}

async fn loss_after_the_last_written_row_is_counted_but_not_reported_in_band() {
    let dir = tempfile::tempdir().expect("tempdir");
    let trace_dir = dir.path().join("traces");
    let guard = JsonlTracer::spawn_guard(trace_dir.clone());
    let emitter = JsonlEventEmitter::new(guard.tracer(), "tail-loss");

    emitter.emit_event(row("last written row"));
    guard.shutdown().await;

    let before = trace_rows_unconfirmed();
    emitter.emit_event(row("refused after the last written row"));
    assert_eq!(
        trace_rows_unconfirmed(),
        before + 1,
        "the refusal is counted"
    );

    let rows = written_rows(&trace_dir);
    assert_eq!(rows.len(), 1);
    assert!(
        rows.iter().all(|row| {
            row["trace_rows_unconfirmed"]
                .as_u64()
                .expect("every row reports the unconfirmed count")
                <= before
        }),
        "no written row can report a refusal that happened after it"
    );
}
