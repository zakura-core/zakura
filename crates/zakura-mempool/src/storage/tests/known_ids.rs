//! Known-ID removal regressions.

use std::collections::HashSet;

use zakura_chain::transparent::OutPoint;

use super::{
    super::{
        config::Config, fixtures::TxFactory, EffectsRejection, SameEffectsChainRejectionError,
    },
    Storage,
};

#[test]
fn mined_parent_keeps_unmined_child_and_ignores_unknown_mined_ids() {
    let mut factory = TxFactory::new();
    let mut storage = Storage::new(&Config::default());
    let parent = factory.tx_with(10_000, 1, 0);
    let child = factory.tx(10_000);
    let parent_id = parent.transaction.id();
    let child_id = child.transaction.id();
    let missing = factory.tx(10_000).transaction.id().mined_id();
    storage
        .insert(
            parent,
            Vec::new(),
            None,
            crate::Instant::now(),
            chrono::Utc::now(),
        )
        .unwrap();
    storage
        .insert(
            child,
            vec![OutPoint::from_usize(parent_id.mined_id(), 0)],
            None,
            crate::Instant::now(),
            chrono::Utc::now(),
        )
        .unwrap();
    let mined = HashSet::from([parent_id.mined_id(), missing]);
    let removed = storage.mine(&mined, crate::Instant::now());
    assert_eq!(removed, HashSet::from([parent_id]));
    assert_eq!(
        storage.tx_ids().collect::<HashSet<_>>(),
        HashSet::from([child_id])
    );
    for id in mined {
        assert_eq!(
            storage.rejected_effects(&id, crate::Instant::now()),
            Some(EffectsRejection::Chain(
                SameEffectsChainRejectionError::Mined
            ))
        );
    }
    storage.verified.assert_dependency_groups_are_bounded();
}
