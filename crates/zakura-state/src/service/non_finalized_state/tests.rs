#![allow(clippy::unwrap_in_result)]

mod prop;
mod vectors;

use std::sync::{Arc, Mutex};

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use zakura_chain::{
    block::Block,
    serialization::ZcashDeserializeInto,
    transaction::{self, LockTime, Transaction},
    transparent,
};
use zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES;

use super::{block_has_transparent_spends, ContextualMetrics};

#[test]
fn coinbase_inputs_do_not_require_an_unspent_utxo_snapshot() {
    let mut block = BLOCK_MAINNET_GENESIS_BYTES
        .zcash_deserialize_into::<Block>()
        .expect("the mainnet genesis block deserializes");

    assert!(block.transactions[0].is_coinbase());
    assert!(!block_has_transparent_spends(&block));

    block.transactions.push(Arc::new(Transaction::V1 {
        inputs: vec![transparent::Input::PrevOut {
            outpoint: transparent::OutPoint {
                hash: transaction::Hash([0x11; 32]),
                index: 0,
            },
            unlock_script: transparent::Script::new(&[]),
            sequence: u32::MAX,
        }],
        outputs: Vec::new(),
        lock_time: LockTime::unlocked(),
    }));

    assert!(block_has_transparent_spends(&block));
}

#[derive(Default)]
struct MetricNameRecorder {
    histogram_names: Mutex<Vec<String>>,
}

impl Recorder for MetricNameRecorder {
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn register_counter(&self, _key: &Key, _metadata: &Metadata<'_>) -> Counter {
        Counter::noop()
    }

    fn register_gauge(&self, _key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        self.histogram_names
            .lock()
            .expect("the metric test does not poison its recorder")
            .push(key.name().to_owned());
        Histogram::noop()
    }
}

#[test]
fn contextual_metrics_record_only_selected_series() {
    let recorder = MetricNameRecorder::default();

    assert_eq!(
        ContextualMetrics::for_commit(false),
        ContextualMetrics::AllBlocks
    );
    assert_eq!(
        ContextualMetrics::for_commit(true),
        ContextualMetrics::Mined
    );

    metrics::with_local_recorder(&recorder, || {
        ContextualMetrics::Disabled.record_duration(
            "test.contextual.disabled.all",
            "test.contextual.disabled.mined",
            std::time::Duration::ZERO,
        );
        ContextualMetrics::AllBlocks.record_duration(
            "test.contextual.all.all",
            "test.contextual.all.mined",
            std::time::Duration::ZERO,
        );
        ContextualMetrics::Mined.record_duration(
            "test.contextual.mined.all",
            "test.contextual.mined.mined",
            std::time::Duration::ZERO,
        );
    });

    assert_eq!(
        *recorder
            .histogram_names
            .lock()
            .expect("the metric test does not poison its recorder"),
        [
            "test.contextual.all.all",
            "test.contextual.mined.all",
            "test.contextual.mined.mined",
        ]
    );
}
