//! Snapshot isolation checks and a release-mode clone microbenchmark.

use std::{collections::HashMap, hint::black_box, sync::Arc, time::Instant};

use zakura_chain::{
    amount::NonNegative,
    block::Height,
    parameters::{Network, NetworkKind},
    transaction, transparent,
    value_balance::ValueBalance,
};

use super::{
    super::{Chain, RevertPosition, UpdateWith},
    TransparentTransfers,
};

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
        #[cfg(zcash_unstable = "nutachyon")]
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        ValueBalance::<NonNegative>::zero(),
    );
    *chain
        .partial_transparent_transfers
        .get_or_insert_mut(address) = transfers;
    let original = Arc::new(chain);
    let mut candidate = Arc::unwrap_or_clone(Arc::clone(&original));
    let history = |chain: &Chain| {
        chain
            .partial_transparent_transfers
            .get(&address)
            .expect("the address was inserted above")
            .clone()
    };

    assert!(std::ptr::eq(
        original
            .partial_transparent_transfers
            .get(&address)
            .unwrap(),
        candidate
            .partial_transparent_transfers
            .get(&address)
            .unwrap(),
    ));

    candidate
        .partial_transparent_transfers
        .get_mut(&address)
        .expect("the address was inserted above")
        .tx_ids
        .insert(added);

    assert!(history(&candidate).tx_ids.contains(&added));
    assert!(!history(&original).tx_ids.contains(&added));
    assert!(history(&original).tx_ids.contains(&existing));
}

/// Records a coinbase output paid to the address at `height`.
fn receive_coinbase(
    transfers: &mut TransparentTransfers,
    height: u32,
) -> (transparent::OutPoint, transparent::OrderedUtxo) {
    let outpoint = transparent::OutPoint {
        hash: transaction::Hash([u8::try_from(height).expect("test heights fit in u8"); 32]),
        index: 0,
    };
    let output = transparent::Output::new(
        zakura_chain::amount::Amount::try_from(1_000).expect("the amount is in range"),
        transparent::Script::new(&[0x51; 25]),
    );
    let utxo = transparent::OrderedUtxo::new(output, Height(height), 0);
    transfers
        .update_chain_tip_with(&(&outpoint, &utxo))
        .expect("a new output is accepted");
    (outpoint, utxo)
}

#[test]
fn appending_an_output_shares_earlier_heights_and_reverts_cleanly() {
    let mut original = TransparentTransfers::default();
    for height in 1..=3 {
        receive_coinbase(&mut original, height);
    }

    // A snapshot taken before the next block keeps the earlier outputs without copying them.
    let mut appended = original.clone();
    let (outpoint, utxo) = receive_coinbase(&mut appended, 4);

    for height in 1..=3 {
        assert!(Arc::ptr_eq(
            &original.created_utxos[&Height(height)],
            &appended.created_utxos[&Height(height)],
        ));
    }
    assert_eq!(original.created_utxos().count(), 3);
    assert_eq!(appended.created_utxos().count(), 4);
    assert_eq!(appended.received(), 4_000);

    // Outputs are listed in chain order across heights.
    let heights: Vec<_> = appended
        .created_utxos()
        .map(|(location, _)| location.height())
        .collect();
    assert_eq!(heights, [Height(1), Height(2), Height(3), Height(4)]);

    // Reverting the block removes its whole height and leaves no empty group behind.
    appended.revert_chain_with(&(&outpoint, &utxo), RevertPosition::Tip);
    assert_eq!(appended, original);
    assert!(!appended.created_utxos.contains_key(&Height(4)));
}

#[test]
fn skipped_transparent_address_index_stays_empty_through_commit_and_revert() {
    for skip in [false, true] {
        let mut chain = Chain::new(
            &Network::Mainnet,
            Height(0),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            #[cfg(zcash_unstable = "nutachyon")]
            Default::default(),
            ValueBalance::<NonNegative>::zero(),
        )
        .skipping_transparent_address_index(skip);

        // A pay-to-public-key-hash output, so it has an address to index.
        let mut lock_script = vec![0x76, 0xa9, 0x14];
        lock_script.extend([4; 20]);
        lock_script.extend([0x88, 0xac]);
        let output = transparent::Output::new(
            zakura_chain::amount::Amount::try_from(1_000).expect("the amount is in range"),
            transparent::Script::new(&lock_script),
        );
        assert!(output.address(&Network::Mainnet).is_some());

        let creating_tx = transaction::Hash([5; 32]);
        let outpoint = transparent::OutPoint {
            hash: creating_tx,
            index: 0,
        };
        let outputs = vec![output.clone()];
        let created = HashMap::from([(
            outpoint,
            transparent::OrderedUtxo::new(output, Height(1), 1),
        )]);

        let spending_tx = transaction::Hash([6; 32]);
        let inputs = vec![transparent::Input::PrevOut {
            outpoint,
            unlock_script: transparent::Script::new(&[]),
            sequence: 0,
        }];

        chain
            .update_chain_tip_with(&(&outputs, &creating_tx, &created))
            .expect("a new output is accepted");
        chain
            .update_chain_tip_with(&(&inputs, &spending_tx, &created))
            .expect("spending the new output is accepted");

        // The chain's own UTXO set is maintained either way; only the address index is skipped.
        assert!(chain.created_utxos.contains_key(&outpoint));
        assert!(chain.spent_utxos.contains_key(&outpoint));
        assert_eq!(
            chain.partial_transparent_transfers.len(),
            usize::from(!skip)
        );

        chain.revert_chain_with(&(&inputs, &spending_tx, &created), RevertPosition::Tip);
        chain.revert_chain_with(&(&outputs, &creating_tx, &created), RevertPosition::Tip);

        assert!(!chain.created_utxos.contains_key(&outpoint));
        assert!(!chain.spent_utxos.contains_key(&outpoint));
        assert_eq!(chain.partial_transparent_transfers.len(), 0);
    }
}

#[test]
#[ignore = "release-mode clone benchmark"]
#[allow(clippy::print_stdout)]
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
        #[cfg(zcash_unstable = "nutachyon")]
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        ValueBalance::<NonNegative>::zero(),
    );
    for (address, transfers) in &old_index {
        *chain
            .partial_transparent_transfers
            .get_or_insert_mut(*address) = transfers.clone();
    }
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
            candidate
                .partial_transparent_transfers
                .get_mut(address)
                .expect("the address was inserted above")
                .tx_ids
                .insert(added);
        }
        black_box(candidate);
    }));
    println!(
        "addresses={ADDRESSES} tx_per_address={TRANSACTIONS_PER_ADDRESS} median_ns old_index={old} shared_index={shared} unwrap_or_clone={unwrap} make_mut={make_mut} old_clone_and_update={old_clone_and_update} shared_clone_and_update={shared_clone_and_update}"
    );
}
