//! Guard the allocation-sensitive state span against formatting referenced scripts.

use std::{
    fmt::{self, Write},
    sync::{Arc, Mutex},
};

use tracing::{field::Visit, span, Metadata, Subscriber};
use zakura_chain::{
    block::{Block, Height},
    parameters::Network,
    serialization::ZcashDeserializeInto,
    transaction, transparent,
    value_balance::ValueBalance,
};

use crate::{
    arbitrary::Prepare,
    service::non_finalized_state::{Chain, ContextualMetrics, NonFinalizedState},
};

struct FormattedBytes(usize);

impl fmt::Write for FormattedBytes {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        self.0 += value.len();
        Ok(())
    }
}
impl Visit for FormattedBytes {
    fn record_debug(&mut self, _field: &tracing::field::Field, value: &dyn fmt::Debug) {
        write!(self, "{value:?}").expect("the counting formatter cannot fail");
    }
}

struct SpanSizes(Arc<Mutex<Vec<usize>>>);

impl Subscriber for SpanSizes {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.name() == "validate_and_update_parallel"
    }
    fn new_span(&self, attributes: &span::Attributes<'_>) -> span::Id {
        let fields = attributes.metadata().fields();
        assert!(fields.field("height").is_some(), "keep the block height");
        assert!(fields.field("hash").is_some(), "keep the block hash");
        let mut size = FormattedBytes(0);
        attributes.record(&mut size);
        self.0
            .lock()
            .expect("test collector is not poisoned")
            .push(size.0);
        span::Id::from_u64(1)
    }
    fn record(&self, _: &span::Id, _: &span::Record<'_>) {}
    fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}
    fn event(&self, _: &tracing::Event<'_>) {}
    fn enter(&self, _: &span::Id) {}
    fn exit(&self, _: &span::Id) {}
}

#[test]
fn contextual_span_size_does_not_grow_with_referenced_scripts() {
    let sizes = Arc::new(Mutex::new(Vec::new()));
    for count in [1, 100] {
        let block = zakura_test::vectors::BLOCK_MAINNET_434873_BYTES
            .zcash_deserialize_into::<Arc<Block>>()
            .expect("the historical block vector deserializes");
        let mut contextual = block.prepare().test_with_zero_spent_utxos();
        // These unrelated entries are permitted by the contextual container's
        // contract. They exercise its Debug representation without changing the
        // block transactions, value balance or validation result.
        contextual.spent_outputs = Arc::new(
            (0..count)
                .map(|index| {
                    (
                        transparent::OutPoint {
                            hash: transaction::Hash([7; 32]),
                            index,
                        },
                        transparent::OrderedUtxo::new(
                            transparent::Output {
                                value: 1.try_into().expect("one is a valid amount"),
                                lock_script: transparent::Script::new(&[7; 10_000]),
                            },
                            Height(0),
                            1,
                        ),
                    )
                })
                .collect(),
        );
        let chain = Chain::new(
            &Network::Mainnet,
            Height(0),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            ValueBalance::fake_populated_pool(),
        );
        tracing::subscriber::with_default(SpanSizes(sizes.clone()), || {
            NonFinalizedState::validate_and_update_parallel(
                Arc::new(chain),
                contextual,
                Default::default(),
                ContextualMetrics::Disabled,
            )
            .expect("the historical fixture passes the parallel state checks");
        });
    }
    let sizes = sizes.lock().expect("test collector is not poisoned");
    assert_eq!(
        sizes.len(),
        2,
        "both actual state-validation spans were observed"
    );
    assert!(
        sizes[0] > 0 && sizes[0] <= 256,
        "span fields must stay small"
    );
    assert_eq!(
        sizes[0], sizes[1],
        "referenced context must not expand tracing fields"
    );
}
