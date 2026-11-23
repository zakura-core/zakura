//! How deliveries share receipts, and how a claim reaches them.

use super::*;

#[test]
fn a_body_keeps_its_place_when_a_rival_and_announcement_arrive_during_registration() {
    let body: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let body = BlockHandle::from(body);
    let rival: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let rival = BlockHandle::from(rival);
    for source in [
        ClaimSource::Inv,
        ClaimSource::BlockMessage,
        ClaimSource::Header,
    ] {
        for delay in [Duration::ZERO, Duration::from_millis(1)] {
            let claims = AnnouncementClaims::default();
            let registry = ReceiptRegistry::new(claims.clone());
            let first = BodyArrival::now();
            let later = first.time + delay;
            let competing = registry.register_at(rival.hash(), &rival, arrival(later));
            match source {
                ClaimSource::Inv => {
                    claims.note_announcement_at(body.hash(), block::Height(0), later)
                }
                ClaimSource::BlockMessage => {
                    claims.note_block_header_at(body.hash(), block::Height(0), later)
                }
                ClaimSource::Header => claims
                    .begin_at([(body.hash(), block::Height(1))], block::Height(0), later)
                    .commit(),
            }
            let (receipt, counted) = registry.draw(body.hash(), &body, first);
            assert!(
                receipt.order < competing.order,
                "the first body keeps priority over its rival"
            );
            assert_eq!(receipt.order, first.order);
            assert_eq!(counted, Some(ClaimOutcome::Unclaimed));
        }
    }
}

#[test]
fn a_claim_timestamp_after_body_arrival_is_ineligible_even_with_an_earlier_order() {
    let now = Instant::now();
    let later = now + Duration::from_nanos(1);
    let claims = AnnouncementClaims::default();
    announce(&claims, hash(1), later);
    claim(&claims, hash(2), later);
    for hash in [hash(1), hash(2)] {
        assert_eq!(
            claims.resolve(hash, Some(CLAIMED), arrival(now)),
            ClaimOutcome::Unclaimed
        );
    }
}

#[test]
fn a_claim_is_shared_by_every_body_delivered_under_its_header() {
    let claims = AnnouncementClaims::default();
    let registry = ReceiptRegistry::new(claims.clone());
    let now = Instant::now();
    let block: Block = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .unwrap();
    claims
        .begin_at([(block.hash(), block::Height(1))], block::Height(0), now)
        .commit();
    let claimed = claims
        .claimed_order(block.hash())
        .expect("the header was claimed");
    let variant = |count: usize| {
        let mut variant = block.clone();
        for _ in 0..count {
            variant.transactions.push(block.transactions[0].clone());
        }
        assert_eq!(variant.hash(), block.hash());
        BlockHandle::from(Arc::new(variant))
    };

    // Junk bodies fill the tracked slots and spill into untracked receipts. None of them
    // uses the claim up, so the real body still finds it.
    let receipts: Vec<_> = (0..=MAX_BODIES_PER_HASH + 1)
        .map(|count| registry.register_at(block.hash(), &variant(count), arrival(now)))
        .collect();
    assert!(receipts[MAX_BODIES_PER_HASH].registered.is_none());
    assert!(receipts.iter().all(|receipt| receipt.order == claimed));

    // A delivery after every earlier one finished is still inside the deadline.
    drop(receipts);
    let again = registry.register_at(
        block.hash(),
        &variant(0),
        arrival(now + CLAIM_BODY_DEADLINE),
    );
    assert_eq!(again.order, claimed);
    let late = now + CLAIM_BODY_DEADLINE + Duration::from_millis(1);
    drop(again);
    assert!(
        registry
            .register_at(block.hash(), &variant(0), arrival(late))
            .order
            > claimed
    );
}

#[test]
fn a_claimed_body_shares_its_order_with_overlapping_deliveries() {
    let claims = AnnouncementClaims::default();
    let registry = ReceiptRegistry::new(claims.clone());
    let now = Instant::now();
    let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .unwrap();
    claims
        .begin_at([(block.hash(), block::Height(1))], block::Height(0), now)
        .commit();
    let handle = BlockHandle::from(block.clone());
    let first = registry.register_at(block.hash(), &handle, arrival(now));
    assert_eq!(Some(first.order), claims.claimed_order(block.hash()));

    // An overlapping delivery shares the receipt even after the claim's deadline.
    let late = now + CLAIM_BODY_DEADLINE + Duration::from_millis(1);
    let duplicate = BlockHandle::from(Arc::new(block.as_ref().clone()));
    assert_eq!(
        registry
            .register_at(block.hash(), &duplicate, arrival(late))
            .order,
        first.order
    );
}

#[test]
fn receipts_release_bodies_when_the_last_caller_finishes() {
    let registry = ReceiptRegistry::default();
    let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let weak = Arc::downgrade(&block);
    let receipt = register(&registry, block);
    assert!(weak.upgrade().is_some());
    assert_eq!(registry.active.lock().unwrap().len(), 1);
    drop(receipt);
    assert!(weak.upgrade().is_none());
    assert!(registry.active.lock().unwrap().is_empty());
}

#[test]
fn bodies_past_the_per_hash_bound_get_untracked_receipts() {
    let registry = ReceiptRegistry::default();
    let block: Block = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let variant = |count: usize| {
        let mut variant = block.clone();
        variant.transactions.truncate(1);
        for _ in 0..count {
            variant.transactions.push(block.transactions[0].clone());
        }
        assert_eq!(variant.hash(), block.hash());
        Arc::new(variant)
    };

    let tracked: Vec<_> = (0..MAX_BODIES_PER_HASH)
        .map(|count| register(&registry, variant(count)))
        .collect();
    let untracked = register(&registry, variant(MAX_BODIES_PER_HASH));
    let untracked_again = register(&registry, variant(MAX_BODIES_PER_HASH));
    assert!(untracked.registered.is_none());
    assert!(tracked
        .iter()
        .all(|receipt| receipt.order < untracked.order));
    assert!(untracked.order < untracked_again.order);
    assert_eq!(
        register(&registry, variant(0)).order,
        tracked[0].order,
        "a tracked body still shares its receipt past the bound",
    );
    assert_eq!(
        registry.active.lock().unwrap()[&block.hash()].len(),
        MAX_BODIES_PER_HASH
    );

    drop((tracked, untracked, untracked_again));
    assert!(registry.active.lock().unwrap().is_empty());
}

#[test]
fn authorizing_variants_share_a_header_but_not_a_receipt() {
    let registry = ReceiptRegistry::default();
    let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1687107_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let original = register(&registry, block.clone());
    let mut receipts = Vec::new();
    let mut orders = std::collections::HashSet::from([original.order]);
    // The original and these variants fill the per-hash bound, so every body is tracked.
    let variants = u8::try_from(MAX_BODIES_PER_HASH - 1).expect("the bound is small");
    for tag in 0..variants {
        let mut changed = block.clone();
        let coinbase = Arc::make_mut(&mut Arc::make_mut(&mut changed).transactions[0]);
        let zakura_chain::transparent::Input::Coinbase { data, .. } = &mut coinbase.inputs_mut()[0]
        else {
            panic!("the first transaction is coinbase");
        };
        data.push(tag);
        assert_eq!(block.hash(), changed.hash());
        assert_eq!(block.transactions[0].hash(), changed.transactions[0].hash());
        assert_ne!(block.auth_data_root(), changed.auth_data_root());
        let receipt = register(&registry, changed.clone());
        assert!(orders.insert(receipt.order));
        let duplicate = register(&registry, changed);
        assert_eq!(duplicate.order, receipt.order);
        receipts.push((receipt, duplicate));
    }
    drop(receipts);
    assert_eq!(register(&registry, block).order, original.order);
    drop(original);
    assert!(registry.active.lock().unwrap().is_empty());
}

#[test]
fn concurrent_identical_bodies_share_receipt_lifetime() {
    let registry = ReceiptRegistry::default();
    let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let barrier = std::sync::Barrier::new(8);
    let receipts = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    barrier.wait();
                    register(&registry, Arc::new(block.as_ref().clone()))
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });
    let order = receipts[0].order;
    assert!(receipts.iter().all(|receipt| receipt.order == order));
    drop(receipts);
    assert!(registry.active.lock().unwrap().is_empty());
    assert!(register(&registry, block).order > order);
}

#[test]
fn overlapping_receipts_keep_priority_until_the_last_caller_finishes() {
    let registry = ReceiptRegistry::default();
    let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let first = register(&registry, block.clone());
    let original_order = first.order;
    let duplicate = register(&registry, Arc::new(block.as_ref().clone()));
    assert_eq!(duplicate.order, original_order);

    let mut malformed = block.as_ref().clone();
    malformed.transactions.clear();
    assert_eq!(malformed.hash(), block.hash());
    let malformed = register(&registry, Arc::new(malformed));
    assert!(malformed.order > original_order);

    drop((first, malformed));
    assert_eq!(register(&registry, block.clone()).order, original_order);
    drop(duplicate);
    assert!(registry.active.lock().unwrap().is_empty());
    assert_eq!(Arc::strong_count(&block), 1);
    assert!(register(&registry, block).order > original_order);
}

#[test]
fn claim_outcomes_are_counted_once_per_tracked_receipt() {
    let claims = AnnouncementClaims::default();
    let registry = ReceiptRegistry::new(claims.clone());
    let now = Instant::now();
    let block: Block = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .unwrap();
    claims
        .begin_at([(block.hash(), block::Height(1))], block::Height(0), now)
        .commit();
    let variant = |count: usize| {
        let mut variant = block.clone();
        for _ in 0..count {
            variant.transactions.push(block.transactions[0].clone());
        }
        BlockHandle::from(Arc::new(variant))
    };
    let counted = |body: &BlockHandle| {
        let (receipt, counted) = registry.draw(block.hash(), body, arrival(now));
        (receipt, counted.is_some())
    };

    let first = variant(0);
    let (first_receipt, first_counted) = counted(&first);
    assert!(first_counted, "a newly tracked receipt is counted");
    let (shared, shared_counted) = counted(&variant(0));
    assert_eq!(shared.order, first_receipt.order);
    assert!(
        !shared_counted,
        "a shared receipt was counted when it was drawn"
    );

    let tracked: Vec<_> = (1..MAX_BODIES_PER_HASH)
        .map(|count| counted(&variant(count)))
        .collect();
    assert!(tracked.iter().all(|(_, counted)| *counted));
    let (untracked, untracked_counted) = counted(&variant(MAX_BODIES_PER_HASH));
    assert!(untracked.registered.is_none());
    assert!(
        !untracked_counted,
        "a body past the per-hash bound is not counted"
    );
}
