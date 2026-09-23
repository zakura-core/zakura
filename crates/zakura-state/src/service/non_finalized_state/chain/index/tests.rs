//! Snapshot isolation checks and a release-mode clone microbenchmark.

use std::{collections::HashMap, hint::black_box, sync::Arc, time::Instant};

use zakura_chain::{
    amount::NonNegative,
    block::Height,
    parameters::{Network, NetworkKind},
    transaction, transparent,
    value_balance::ValueBalance,
};

use super::{super::Chain, TransparentTransfers};

#[test]
fn cloned_chain_keeps_transparent_address_history_isolated() {
    let address = transparent::Address::PayToPublicKeyHash {
        network_kind: NetworkKind::Mainnet,
        pub_key_hash: [1; 20],
    };
    let existing = transaction::Hash([2; 32]);
    let added = transaction::Hash([3; 32]);
    let mut transfers = TransparentTransfers::default();
    transfers.tx_ids.insert(existing);

    let mut chain = Chain::new(
        &Network::Mainnet,
        Height(0),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        ValueBalance::<NonNegative>::zero(),
    );
    chain
        .partial_transparent_transfers
        .insert(address, Arc::new(transfers));
    let original = Arc::new(chain);
    let mut candidate = Arc::unwrap_or_clone(Arc::clone(&original));

    let original_transfers = &original.partial_transparent_transfers[&address];
    let candidate_transfers = &candidate.partial_transparent_transfers[&address];
    assert!(Arc::ptr_eq(original_transfers, candidate_transfers));

    let candidate_transfers = candidate
        .partial_transparent_transfers
        .get_mut(&address)
        .expect("the address was inserted above");
    Arc::make_mut(candidate_transfers).tx_ids.insert(added);

    assert!(candidate.partial_transparent_transfers[&address]
        .tx_ids
        .contains(&added));
    assert!(!original.partial_transparent_transfers[&address]
        .tx_ids
        .contains(&added));
    assert!(original.partial_transparent_transfers[&address]
        .tx_ids
        .contains(&existing));
}

#[test]
#[ignore = "release-mode clone benchmark"]
fn compare_address_index_clone() {
    // Isolate the address index; other chain indexes are empty. This does not
    // reproduce a full block replay or its thread scheduling.
    const ADDRESSES: usize = 10_000;
    const TRANSACTIONS_PER_ADDRESS: usize = 16;
    const SAMPLES: usize = 50;

    let old_index: HashMap<_, _> = (0..ADDRESSES)
        .map(|address_index| {
            let mut address_hash = [0; 20];
            let address_number =
                u64::try_from(address_index).expect("the benchmark address count fits in u64");
            address_hash[..8].copy_from_slice(&address_number.to_le_bytes());
            let address = transparent::Address::PayToPublicKeyHash {
                network_kind: NetworkKind::Mainnet,
                pub_key_hash: address_hash,
            };
            let mut transfers = TransparentTransfers::default();
            for transaction_index in 0..TRANSACTIONS_PER_ADDRESS {
                let unique = address_index * TRANSACTIONS_PER_ADDRESS + transaction_index;
                let mut hash = [0; 32];
                let unique =
                    u64::try_from(unique).expect("the benchmark transaction count fits in u64");
                hash[..8].copy_from_slice(&unique.to_le_bytes());
                transfers.tx_ids.insert(transaction::Hash(hash));
            }
            (address, transfers)
        })
        .collect();

    let mut chain = Chain::new(
        &Network::Mainnet,
        Height(0),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        ValueBalance::<NonNegative>::zero(),
    );
    chain.partial_transparent_transfers = old_index
        .iter()
        .map(|(address, transfers)| (*address, Arc::new(transfers.clone())))
        .collect();
    let chain = Arc::new(chain);

    let measure = |mut operation: Box<dyn FnMut()>| {
        let mut times = Vec::with_capacity(SAMPLES);
        for _ in 0..SAMPLES {
            let start = Instant::now();
            operation();
            times.push(start.elapsed().as_nanos());
        }
        times.sort_unstable();
        times[SAMPLES / 2]
    };

    let old = measure(Box::new(|| {
        black_box(black_box(&old_index).clone());
    }));
    let shared = measure(Box::new(|| {
        black_box(black_box(&chain.partial_transparent_transfers).clone());
    }));
    let unwrap = measure(Box::new(|| {
        let candidate = Arc::clone(&chain);
        black_box(Arc::unwrap_or_clone(candidate));
    }));
    let make_mut = measure(Box::new(|| {
        let mut candidate = Arc::clone(&chain);
        black_box(Arc::make_mut(&mut candidate));
    }));
    let touched = (0..100)
        .map(|address_index| {
            let mut address_hash = [0; 20];
            let address_number =
                u64::try_from(address_index).expect("the benchmark address count fits in u64");
            address_hash[..8].copy_from_slice(&address_number.to_le_bytes());
            transparent::Address::PayToPublicKeyHash {
                network_kind: NetworkKind::Mainnet,
                pub_key_hash: address_hash,
            }
        })
        .collect::<Vec<_>>();
    let added = transaction::Hash([255; 32]);
    let old_clone_and_update = measure(Box::new(|| {
        let mut candidate = old_index.clone();
        for address in &touched {
            candidate
                .get_mut(address)
                .expect("the address was inserted above")
                .tx_ids
                .insert(added);
        }
        black_box(candidate);
    }));
    let shared_clone_and_update = measure(Box::new(|| {
        let mut candidate = Arc::unwrap_or_clone(Arc::clone(&chain));
        for address in &touched {
            Arc::make_mut(
                candidate
                    .partial_transparent_transfers
                    .get_mut(address)
                    .expect("the address was inserted above"),
            )
            .tx_ids
            .insert(added);
        }
        black_box(candidate);
    }));
    println!(
        "addresses={ADDRESSES} tx_per_address={TRANSACTIONS_PER_ADDRESS} median_ns old_index={old} shared_index={shared} unwrap_or_clone={unwrap} make_mut={make_mut} old_clone_and_update={old_clone_and_update} shared_clone_and_update={shared_clone_and_update}"
    );
}
