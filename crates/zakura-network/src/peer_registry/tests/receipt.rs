//! A block's transfer time and predictions belong to the receipt, even if verification is delayed.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use zakura_chain::{
    block::{self, Block},
    serialization::{CompactSize64, ZcashDeserializeInto, ZcashSerialize},
    transparent::Script,
};

use crate::{
    peer::RequestSlot,
    peer_registry::{DeliverySampleKind, DeliveryTrace, PeerRegistry},
    InventoryResponse::{Available, Missing},
    Response,
};

const BLOCK_BYTES: u64 = 1_500_000;

/// Size the fixture's first output script so the receipt measures exactly 1.5 MB.
fn large_block() -> Arc<Block> {
    let mut block: Block = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let overhead = block.zcash_serialized_size()
        - block.transactions[0].outputs()[0]
            .lock_script
            .zcash_serialized_size()
        + CompactSize64::from(BLOCK_BYTES).zcash_serialized_size();
    let size = usize::try_from(BLOCK_BYTES).unwrap();
    Arc::make_mut(&mut block.transactions[0]).outputs_mut()[0].lock_script =
        Script::new(&vec![0; size - overhead]);
    assert_eq!(block.zcash_serialized_size(), size);
    Arc::new(block)
}

#[tokio::test]
async fn verified_large_replies_use_the_prediction_at_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let trace = zakura_jsonl_trace::JsonlTracer::spawn_guard(dir.path().to_path_buf());
    let emitter = zakura_jsonl_trace::JsonlEventEmitter::new(trace.tracer(), "receipt-test");
    let registry = PeerRegistry::default();
    let block = large_block();
    let start = Instant::now();
    let received_at = start + Duration::from_secs(1);
    let next_ping = received_at + Duration::from_secs(1);
    let mut results = Vec::new();

    // The first two peers have identical measurements. Only the order of verified credit differs.
    // The third peer gets its first round-trip measurement after the block arrives.
    for (id, prior_ping, verify_first) in [(1u16, true, true), (2, true, false), (3, false, false)]
    {
        let (_guard, updater) = registry.register_legacy(super::peer(id));
        let updater = updater.with_delivery_trace(DeliveryTrace::new(emitter.clone(), id.into()));
        let ping = |millis, at| {
            updater.record_delivery_sample(
                DeliverySampleKind::Ping,
                Duration::from_millis(millis),
                at,
            )
        };
        if prior_ping {
            ping(100, start);
        }
        let mut response = Response::Blocks(vec![Available((block.clone().into(), None))]);
        response.observe_block_delivery(
            &RequestSlot::default(),
            Some(updater.clone()),
            Duration::from_millis(670),
            received_at,
        );
        let Response::Blocks(blocks) = response else {
            unreachable!()
        };
        let (received, _) = blocks[0].available().unwrap();
        assert_eq!(updater.delivery_estimate().large_block_slowdown(), None);
        if verify_first {
            received.record_verified();
        }
        ping(300, next_ping);
        // A second credit on the first peer must leave only one sample in the trace.
        received.record_verified();
        let estimate = updater.delivery_estimate();
        results.push((
            estimate.large_block_slowdown(),
            estimate.expected(Some(BLOCK_BYTES)),
        ));
    }
    assert_eq!(results[0], results[1]);
    assert!(results[0].0.is_some());
    assert!(results[0].1.unwrap() > Duration::from_secs(1));
    assert_eq!(
        results[2].0, None,
        "a later ping cannot supply a missing baseline"
    );

    drop(emitter);
    trace.shutdown().await;
    let rows = std::fs::read_to_string(dir.path().join("delivery_score.jsonl")).unwrap();
    let predictions: Vec<_> = rows
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .filter(|row| row["kind"] == "large_block")
        .map(|row| {
            (
                row["connection_id"].as_u64().unwrap(),
                row["expected_ms"].as_u64(),
            )
        })
        .collect();
    assert_eq!(predictions, [(1, Some(669)), (2, Some(669)), (3, None)]);
}

/// A reply with several blocks is one transfer. Its size is the sum of the blocks it delivered,
/// which decides what it measures, and it earns one sample however many of its blocks verify.
#[tokio::test]
async fn a_batch_reply_is_one_sample_of_the_bytes_it_delivered() {
    let dir = tempfile::tempdir().unwrap();
    let trace = zakura_jsonl_trace::JsonlTracer::spawn_guard(dir.path().to_path_buf());
    let emitter = zakura_jsonl_trace::JsonlEventEmitter::new(trace.tracer(), "receipt-test");
    let registry = PeerRegistry::default();
    let small: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let small_bytes = u64::try_from(small.zcash_serialized_size()).unwrap();
    let mut expected = Vec::new();

    for (id, second, kind, wire_bytes) in [
        (1u16, small.clone(), "block", 2 * small_bytes),
        (2, large_block(), "large_block", small_bytes + BLOCK_BYTES),
    ] {
        let (_guard, updater) = registry.register_legacy(super::peer(id));
        let updater = updater.with_delivery_trace(DeliveryTrace::new(emitter.clone(), id.into()));
        let mut response = Response::Blocks(vec![
            Available((small.clone().into(), None)),
            Missing(block::Hash([7; 32])),
            Available((second.into(), None)),
        ]);
        response.observe_block_delivery(
            &RequestSlot::default(),
            Some(updater),
            Duration::from_millis(670),
            Instant::now(),
        );
        let Response::Blocks(blocks) = response else {
            unreachable!()
        };
        for entry in &blocks {
            if let Some((received, _)) = entry.available() {
                received.record_verified();
            }
        }
        expected.push((u64::from(id), kind.to_string(), wire_bytes));
    }

    drop(emitter);
    trace.shutdown().await;
    let rows = std::fs::read_to_string(dir.path().join("delivery_score.jsonl")).unwrap();
    let samples: Vec<_> = rows
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .map(|row| {
            (
                row["connection_id"].as_u64().unwrap(),
                row["kind"].as_str().unwrap().to_string(),
                row["wire_bytes"].as_u64().unwrap(),
            )
        })
        .collect();
    assert_eq!(samples, expected, "one sample for each whole reply");
}
