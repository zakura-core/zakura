//! Tests for per-message P2P tracing.

use std::sync::Arc;

use zakura_chain::{block::Block, serialization::ZcashDeserializeInto};

use super::*;

/// Read a CSV table written by the trace writer into header plus rows.
fn read_csv(path: &std::path::Path) -> (Vec<String>, Vec<Vec<String>>) {
    let contents = std::fs::read_to_string(path).expect("trace table is written");
    let mut lines = contents.lines();
    let header = split_csv(lines.next().expect("table has a header"));
    let rows = lines.map(split_csv).collect();
    (header, rows)
}

/// Split one CSV line into fields, honouring RFC 4180 quoting.
fn split_csv(line: &str) -> Vec<String> {
    let mut fields = vec![String::new()];
    let mut quoted = false;
    let mut characters = line.chars().peekable();

    while let Some(character) = characters.next() {
        match character {
            '"' if quoted && characters.peek() == Some(&'"') => {
                characters.next();
                fields.last_mut().expect("a field is always open").push('"');
            }
            '"' => quoted = !quoted,
            ',' if !quoted => fields.push(String::new()),
            character => fields
                .last_mut()
                .expect("a field is always open")
                .push(character),
        }
    }

    fields
}

fn column<'a>(header: &[String], row: &'a [String], name: &str) -> &'a str {
    let index = header
        .iter()
        .position(|column| column == name)
        .unwrap_or_else(|| panic!("table has a {name} column: {header:?}"));
    &row[index]
}

fn test_block() -> Arc<Block> {
    Arc::new(
        zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .expect("block test vector deserializes"),
    )
}

#[tokio::test]
async fn traces_a_block_message_with_the_schema_the_analyzers_read() {
    let dir = tempfile::tempdir().expect("temporary trace directory");
    let guard = JsonlTracer::spawn_guard(dir.path().to_path_buf());
    let tracer = P2pTracer::from_emitter(JsonlEventEmitter::new(guard.tracer(), "node-1"));
    let seq = AtomicU64::new(0);

    let block = test_block();
    let hash = block.hash();
    let size = block.zcash_serialized_size();

    tracer.trace_msg("send", &Message::Block(block), "10.0.0.1:8233", 7, &seq);

    drop(tracer);
    guard.shutdown().await;

    let (header, rows) = read_csv(&dir.path().join(PEER_MESSAGE_TABLE.file_name()));
    assert_eq!(rows.len(), 1, "one message produces one row");
    let row = &rows[0];

    assert_eq!(column(&header, row, "node"), "node-1");
    assert_eq!(column(&header, row, "dir"), "send");
    assert_eq!(column(&header, row, "msg"), "block");
    assert_eq!(column(&header, row, "peer"), "10.0.0.1:8233");
    assert_eq!(column(&header, row, "conn"), "7");
    assert_eq!(column(&header, row, "summary.height"), "1");
    assert_eq!(column(&header, row, "summary.body_bytes"), size.to_string());

    // Every analyzer recovers the block hash as `substr(mid, 7)`, i.e. the text
    // after `block:`. This is the join key for the whole campaign.
    let mid = column(&header, row, "mid");
    assert_eq!(mid, format!("block:{hash}"));
    assert_eq!(&mid["block:".len()..], hash.to_string());

    // Cross-node correlation needs an absolute clock, not the per-emitter
    // monotonic one.
    let wall_ts = column(&header, row, "wall_ts");
    chrono::DateTime::parse_from_rfc3339(wall_ts).expect("wall_ts is RFC 3339");
    assert!(
        column(&header, row, "extra").is_empty(),
        "the declared header covers every field of a block row"
    );
}

#[tokio::test]
async fn traces_both_directions_and_bounds_inv_hashes() {
    let dir = tempfile::tempdir().expect("temporary trace directory");
    let guard = JsonlTracer::spawn_guard(dir.path().to_path_buf());
    let tracer = P2pTracer::from_emitter(JsonlEventEmitter::new(guard.tracer(), "node-1"));
    let seq = AtomicU64::new(0);

    let hashes: Vec<_> = (0..50)
        .map(|byte| InventoryHash::Block(block::Hash([byte; 32])))
        .collect();

    tracer.trace_msg("recv", &Message::Inv(hashes.clone()), "peer", 1, &seq);
    tracer.trace_msg("send", &Message::GetData(hashes), "peer", 1, &seq);

    drop(tracer);
    guard.shutdown().await;

    let (header, rows) = read_csv(&dir.path().join(PEER_MESSAGE_TABLE.file_name()));
    assert_eq!(rows.len(), 2);

    assert_eq!(column(&header, &rows[0], "dir"), "recv");
    assert_eq!(column(&header, &rows[0], "msg"), "inv");
    assert_eq!(column(&header, &rows[1], "dir"), "send");
    assert_eq!(column(&header, &rows[1], "msg"), "getdata");

    // The full count is recorded, but only a bounded prefix of the hashes: an
    // `inv` can carry thousands, and recording them all would make the trace
    // larger than the traffic it describes.
    assert_eq!(column(&header, &rows[0], "summary.count"), "50");
    let hashes: Vec<String> =
        serde_json::from_str(column(&header, &rows[0], "summary.hashes")).expect("hashes are JSON");
    assert_eq!(hashes.len(), MAX_SUMMARY_HASHES);
}

#[test]
fn counts_every_row_that_does_not_reach_the_table() {
    // A capacity-1 queue with no reader: the first row takes the slot, and
    // every row after it is dropped. Once the queue is full the sampler is at
    // maximum pressure, so drops land in both counters — what matters is that
    // none go unrecorded, since an uncounted drop is indistinguishable from a
    // message that never happened.
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let tracer = P2pTracer::from_emitter(JsonlEventEmitter::new(JsonlTracer::new(tx), "node-1"));
    let seq = AtomicU64::new(0);

    for _ in 0..8 {
        tracer.trace_msg("send", &Message::Verack, "peer", 1, &seq);
    }

    let runtime = tracer.runtime.as_ref().expect("tracer is enabled");
    let queue_full = runtime.queue_full_drops.load(Ordering::Relaxed);
    let sampled = runtime.sampled_drops.load(Ordering::Relaxed);

    assert_eq!(
        queue_full + sampled,
        7,
        "8 messages, 1 queue slot: every row after the first is accounted for \
         ({queue_full} queue-full, {sampled} sampled)"
    );
}

#[test]
fn sampling_pressure_is_measured_against_the_real_queue_size() {
    // The sample rate is a function of how full the queue is, so it has to read
    // the queue's own capacity. Taking the denominator from a default config
    // instead would report maximum pressure on an empty non-default queue.
    let (tx, _rx) = tokio::sync::mpsc::channel(64);
    let tracer = P2pTracer::from_emitter(JsonlEventEmitter::new(JsonlTracer::new(tx), "node-1"));
    let runtime = tracer.runtime.as_ref().expect("tracer is enabled");

    assert_eq!(
        runtime.adaptive_sample_rate(),
        1,
        "an empty queue keeps every row"
    );

    let seq = AtomicU64::new(0);
    for _ in 0..64 {
        tracer.trace_msg("send", &Message::Verack, "peer", 1, &seq);
    }

    // Shedding starts before the queue is full and keeps it off the floor, so
    // the rate escalates rather than pinning to the highest tier.
    assert!(
        runtime.adaptive_sample_rate() >= TRACE_SAMPLE_RATE_LOW_PRESSURE,
        "a queue under pressure sheds rows"
    );
}

#[tokio::test]
async fn writes_a_trace_dropped_row_after_the_queue_drains() {
    let dir = tempfile::tempdir().expect("temporary trace directory");
    let guard = JsonlTracer::spawn_guard(dir.path().to_path_buf());
    let tracer = P2pTracer::from_emitter(JsonlEventEmitter::new(guard.tracer(), "node-1"));
    let seq = AtomicU64::new(0);

    let runtime = tracer.runtime.as_ref().expect("tracer is enabled");
    runtime.record_drop(TraceDropReason::QueueFull);
    runtime.record_drop(TraceDropReason::Sampled);

    // The next successful trace flushes the pending counts.
    tracer.trace_msg("send", &Message::Verack, "peer", 1, &seq);

    drop(tracer);
    guard.shutdown().await;

    let (header, rows) = read_csv(&dir.path().join(TRACE_DROPPED_TABLE.file_name()));
    assert_eq!(rows.len(), 1);
    assert_eq!(column(&header, &rows[0], "table"), "peer_message");
    assert_eq!(column(&header, &rows[0], "queue_full_dropped"), "1");
    assert_eq!(column(&header, &rows[0], "sampled_dropped"), "1");
}

#[test]
fn a_disabled_tracer_never_builds_a_row() {
    let tracer = P2pTracer::noop();
    let seq = AtomicU64::new(0);

    tracer.trace_msg("send", &Message::Block(test_block()), "peer", 1, &seq);

    assert!(!tracer.is_enabled());
    assert_eq!(
        seq.load(Ordering::Relaxed),
        0,
        "a no-op tracer does no per-message work"
    );
}

#[test]
fn every_message_type_has_a_stable_label() {
    // The analyzers select on `msg`, so these labels are part of the schema.
    let cases = [
        (Message::Verack, "verack"),
        (Message::GetAddr, "getaddr"),
        (Message::Mempool, "mempool"),
        (Message::Ping(Nonce(1)), "ping"),
        (Message::Pong(Nonce(1)), "pong"),
        (Message::Addr(Vec::new()), "addr"),
        (Message::Inv(Vec::new()), "inv"),
        (Message::GetData(Vec::new()), "getdata"),
        (Message::NotFound(Vec::new()), "notfound"),
        (Message::Headers(Vec::new()), "headers"),
        (Message::Block(test_block()), "block"),
    ];

    for (message, expected) in cases {
        let (label, _) = summarize_message(&message);
        assert_eq!(label, expected);
    }
}
