//! Tests for types and functions for the `getblocktemplate` RPC.

mod nsm_fees;

use anyhow::anyhow;
use std::iter;
use zakura_chain::amount::Amount;

use zcash_keys::address::Address;

use zakura_chain::parameters::testnet::ConfiguredFundingStreamRecipient;

use zakura_chain::{
    block::Height,
    local_genesis::generate_local_testnet_with_funded_keys,
    parameters::{
        subsidy::FundingStreamReceiver::{Deferred, Ecc, MajorGrants, ZcashFoundation},
        testnet::{self, ConfiguredActivationHeights, ConfiguredFundingStreams},
        Network, NetworkUpgrade,
    },
    serialization::{ZcashDeserializeInto, ZcashSerialize},
    transaction::Transaction,
    transparent,
};
use zakura_script::Sigops;

use crate::client::TransactionTemplate;
use crate::config::mining::{default_miner_address, MinerAddressType};

use super::{MinerParams, TemplatePreparationQueue};

#[test]
fn template_rejection_targets_work_and_ignores_old_parents() {
    let parent = zakura_chain::block::Hash([1; 32]);
    let next_parent = zakura_chain::block::Hash([2; 32]);
    let mut state = super::TemplateRejections::default();
    state.set_parent(parent);
    state.mark_prepared(parent, "new");
    assert!(state.reject(parent, "old"));
    assert!(state.contains("old"));
    assert!(!state.contains("new"));
    assert!(state.is_prepared("new"));
    assert!(!state.is_prepared("unknown"));
    assert!(!state.withdrawn("new"));
    assert!(state.withdrawn("unknown"));
    assert!(!state.reject(parent, "old"));
    assert_eq!(state.revision, 1);
    state.set_parent(next_parent);
    assert!(!state.needs_fallback());
    assert!(!state.is_prepared("new"));
    assert!(!state.reject(parent, "late"));
    assert_eq!(state.revision, 1);
    assert!(state.reject(next_parent, "new"));
    assert_eq!(state.revision, 2);
}

#[test]
fn template_rejection_storage_fails_closed_at_capacity() {
    let parent = zakura_chain::block::Hash([1; 32]);
    let mut state = super::TemplateRejections::default();
    state.set_parent(parent);
    for id in 0..100 {
        state.reject(parent, &id.to_string());
    }
    assert_eq!(state.rejected.len(), 64);
    assert!(state.contains("unknown"));
    assert!(state.needs_fallback());
}

#[test]
fn prepared_template_tracking_keeps_new_recovery_work_at_capacity() {
    let parent = zakura_chain::block::Hash([1; 32]);
    let mut state = super::TemplateRejections::default();
    state.set_parent(parent);
    state.reject(parent, "invalid");
    for id in 0..100 {
        state.mark_prepared(parent, &id.to_string());
    }
    assert_eq!(state.prepared.len(), 64);
    assert!(!state.withdrawn("99"));
    assert!(state.withdrawn("0"));
}

#[tokio::test]
async fn template_rejection_retains_notifications_for_late_subscribers() {
    let parent = zakura_chain::block::Hash([1; 32]);
    let mut state = super::TemplateRejections::default();
    state.set_parent(parent);
    let sender = tokio::sync::watch::channel(state).0;
    let mut early = sender.subscribe();
    sender.send_if_modified(|state| state.reject(parent, "work"));
    tokio::time::timeout(std::time::Duration::from_secs(1), early.changed())
        .await
        .unwrap()
        .unwrap();
    assert!(early.borrow_and_update().contains("work"));
    assert!(sender.subscribe().borrow().contains("work"));
}

#[test]
fn template_preparation_queue_keeps_the_latest_pending_template() {
    let queue = TemplatePreparationQueue::<u8>::default();

    assert_eq!(queue.enqueue(1), Some(1));
    assert_eq!(queue.enqueue(2), None);
    assert_eq!(queue.enqueue(3), None);
    assert_eq!(queue.next_or_finish(), Some(3));
    assert_eq!(queue.next_or_finish(), None);
}

/// Tests transparent coinbase generation at every configured Sapling-and-later
/// network upgrade activation.
#[test]
fn transparent_coinbase() -> anyhow::Result<()> {
    let regtest = testnet::Parameters::build()
        .with_slow_start_interval(Height::MIN)
        .with_activation_heights(ConfiguredActivationHeights {
            overwinter: Some(1),
            sapling: Some(2),
            blossom: Some(3),
            heartwood: Some(4),
            canopy: Some(5),
            nu5: Some(6),
            nu6: Some(7),
            nu6_1: Some(8),
            nu6_3: Some(9),
            ..Default::default()
        })?
        .with_funding_streams(vec![
            ConfiguredFundingStreams {
                height_range: Some(Height(1)..Height(100)),
                recipients: Some(vec![
                    ConfiguredFundingStreamRecipient::new_for(Ecc),
                    ConfiguredFundingStreamRecipient::new_for(ZcashFoundation),
                    ConfiguredFundingStreamRecipient::new_for(MajorGrants),
                ]),
            },
            ConfiguredFundingStreams {
                height_range: Some(Height(1)..Height(100)),
                recipients: Some(vec![
                    ConfiguredFundingStreamRecipient::new_for(MajorGrants),
                    ConfiguredFundingStreamRecipient {
                        receiver: Deferred,
                        numerator: 12,
                        addresses: None,
                    },
                ]),
            },
        ])
        .to_network()?;

    for net in Network::iter().chain(iter::once(regtest)) {
        let miner_params = MinerParams::from(
            Address::decode(
                &net,
                default_miner_address(net.kind(), &MinerAddressType::Transparent),
            )
            .ok_or(anyhow!("hard-coded transparent address must be valid"))?,
        );

        for nu in NetworkUpgrade::iter().filter(|nu| nu >= &NetworkUpgrade::Sapling) {
            if let Some(height) = nu.activation_height(&net) {
                let transaction = coinbase_transaction(&net, height, &miner_params)?;
                assert_coinbase_resource_usage(&net, height, &miner_params, &transaction)?;
                assert!(transaction.sapling_outputs().next().is_none());
                assert!(transaction.orchard_shielded_data().is_none());
                assert!(transaction.ironwood_shielded_data().is_none());
            }
        }
    }

    Ok(())
}

/// The coinbase built for a local testnet's activation block must include the
/// network's configured zero-value lockbox marker output, so a mining node
/// can produce a block that satisfies the one-time ZIP-271 disbursement rule.
#[test]
fn local_genesis_activation_coinbase_includes_lockbox_marker() -> anyhow::Result<()> {
    let generated = generate_local_testnet_with_funded_keys(
        vec!["alice".to_string(), "bob".to_string()],
        Default::default(),
    )
    .map_err(|error| anyhow!(error.to_string()))?;
    let net = generated.network;
    let height = NetworkUpgrade::Nu6_3
        .activation_height(&net)
        .expect("the default local network activates NU6.3");
    let miner_params = MinerParams::from(
        Address::decode(
            &net,
            default_miner_address(net.kind(), &MinerAddressType::Transparent),
        )
        .ok_or(anyhow!("hard-coded address must be valid"))?,
    );
    let transaction =
        TransactionTemplate::new_coinbase(&net, height, &miner_params, Amount::zero(), None)?
            .data()
            .as_ref()
            .zcash_deserialize_into::<Transaction>()?;
    let lockbox_disbursements = net.lockbox_disbursements(height);
    let [(lockbox_address, lockbox_amount)] = lockbox_disbursements.as_slice() else {
        return Err(anyhow!("local network must have one lockbox marker"));
    };
    let lockbox_output = transparent::Output::new(*lockbox_amount, lockbox_address.script());

    assert!(transaction.outputs().contains(&lockbox_output));

    Ok(())
}

/// The Zakura marker is always prepended, and `extra_coinbase_data` can't exceed
/// the limit.
#[test]
fn coinbase_tag_and_limit() {
    use zcash_address::ZcashAddress;
    use zcash_transparent::coinbase::{MAX_COINBASE_HEIGHT_LEN, MAX_COINBASE_SCRIPT_LEN};

    use crate::config::mining::{
        Config, ExtraCoinbaseData, MAX_USER_COINBASE_DATA_LEN, ZAKURA_COINBASE_MARKER,
        ZAKURA_COINBASE_SEPARATOR,
    };

    // `ExtraCoinbaseData` accepts data up to the limit and rejects one byte over. Its
    // `Deserialize` impl delegates here, so an oversized `mining.extra_coinbase_data`
    // makes the config fail to load and the node refuse to start.
    assert!(ExtraCoinbaseData::try_from("x".repeat(MAX_USER_COINBASE_DATA_LEN)).is_ok());
    assert!(ExtraCoinbaseData::try_from("x".repeat(MAX_USER_COINBASE_DATA_LEN + 1)).is_err());
    assert_eq!(
        MAX_USER_COINBASE_DATA_LEN
            + ZAKURA_COINBASE_MARKER.len()
            + ZAKURA_COINBASE_SEPARATOR.len()
            + 2
            + MAX_COINBASE_HEIGHT_LEN,
        MAX_COINBASE_SCRIPT_LEN,
        "the configured data limit must reserve the worst-case height and OP_PUSHDATA1 bytes"
    );

    let net = Network::Mainnet;
    let addr: ZcashAddress = default_miner_address(net.kind(), &MinerAddressType::Transparent)
        .parse()
        .expect("default miner address parses");

    let params = |extra: Option<ExtraCoinbaseData>| {
        MinerParams::new(
            &net,
            Config {
                miner_address: Some(addr.clone()),
                extra_coinbase_data: extra,
                ..Default::default()
            },
        )
    };

    // The marker is prepended whether or not `extra_coinbase_data` is set, so every
    // block Zakura builds is tagged. Without extra data, the coinbase data is exactly
    // the marker.
    let untagged = params(None).expect("valid config");
    let untagged = untagged.data().as_ref().expect("marker is always present");
    assert_eq!(
        untagged.value().as_slice(),
        ZAKURA_COINBASE_MARKER.as_bytes()
    );

    // With extra data, the marker and separator precede it.
    let tag = ExtraCoinbaseData::try_from("/pool/".to_string()).expect("within the limit");
    let tagged = params(Some(tag)).expect("valid config");
    let tagged = tagged.data().as_ref().expect("marker is always present");
    assert_eq!(
        tagged.value().as_slice(),
        [ZAKURA_COINBASE_MARKER, ZAKURA_COINBASE_SEPARATOR, "/pool/"]
            .concat()
            .as_bytes()
    );

    // Exercise the invariant behind `MinerParams::new`'s `expect` through the
    // real coinbase builder at the maximum supported Zakura height.
    let max_tag = ExtraCoinbaseData::try_from("x".repeat(MAX_USER_COINBASE_DATA_LEN))
        .expect("maximum-length tag is valid");
    let max_params = params(Some(max_tag)).expect("maximum-length tag fits miner params");
    let max_coinbase =
        TransactionTemplate::new_coinbase(&net, Height::MAX, &max_params, Amount::zero(), None)
            .expect("maximum-length tag fits a coinbase transaction")
            .data()
            .as_ref()
            .zcash_deserialize_into::<Transaction>()
            .expect("maximum-length coinbase transaction deserializes");
    let coinbase_script = max_coinbase.inputs()[0]
        .coinbase_script()
        .expect("built coinbase input has a canonical script");
    assert!(
        coinbase_script.len() <= MAX_COINBASE_SCRIPT_LEN,
        "maximum-length configured tag must keep the coinbase script within consensus limits"
    );
}

/// Tests each distinct shielded coinbase construction and routing path.
///
/// The exhaustive [`transparent_coinbase`] test does not need shielded proofs.
/// This test limits real proof generation to the paths where the address type or
/// network upgrade changes the selected shielded pool or circuit.
///
/// Run this test with `--release` because it generates five real proofs.
#[test]
#[ignore]
fn shielded_coinbase_paths() -> anyhow::Result<()> {
    let net = shielded_coinbase_testnet();
    let sapling_height = NetworkUpgrade::Sapling
        .activation_height(&net)
        .expect("Sapling activation height is configured");
    let canopy_height = NetworkUpgrade::Canopy
        .activation_height(&net)
        .expect("Canopy activation height is configured");
    let nu5_height = NetworkUpgrade::Nu5
        .activation_height(&net)
        .expect("NU5 activation height is configured");
    let nu6_2_height = NetworkUpgrade::Nu6_2
        .activation_height(&net)
        .expect("NU6.2 activation height is configured");
    let nu6_3_height = NetworkUpgrade::Nu6_3
        .activation_height(&net)
        .expect("NU6.3 activation height is configured");
    let sapling_params = MinerParams::from(
        Address::decode(
            &net,
            default_miner_address(net.kind(), &MinerAddressType::Sapling),
        )
        .expect("hard-coded Sapling miner address is valid"),
    );
    let unified_params = MinerParams::from(
        Address::decode(
            &net,
            default_miner_address(net.kind(), &MinerAddressType::Unified),
        )
        .expect("hard-coded unified miner address is valid"),
    );

    let sapling_tx = coinbase_transaction(&net, sapling_height, &sapling_params)?;
    assert_coinbase_resource_usage(&net, sapling_height, &sapling_params, &sapling_tx)?;
    assert!(
        sapling_tx.sapling_outputs().next().is_some(),
        "a Sapling miner address should receive a Sapling output"
    );
    assert!(
        sapling_tx.orchard_shielded_data().is_none(),
        "a Sapling miner address should not receive an Orchard output"
    );
    assert!(
        sapling_tx.ironwood_shielded_data().is_none(),
        "a Sapling miner address should not receive an Ironwood output"
    );

    let pre_nu5_tx = coinbase_transaction(&net, canopy_height, &unified_params)?;
    assert_coinbase_resource_usage(&net, canopy_height, &unified_params, &pre_nu5_tx)?;
    assert!(
        pre_nu5_tx.sapling_outputs().next().is_some(),
        "a pre-NU5 unified address should fall back to its Sapling receiver"
    );
    assert!(
        pre_nu5_tx.orchard_shielded_data().is_none(),
        "a pre-NU5 coinbase cannot contain an Orchard output"
    );
    assert!(
        pre_nu5_tx.ironwood_shielded_data().is_none(),
        "a pre-NU5 coinbase cannot contain an Ironwood output"
    );

    let pre_nu6_2_tx = coinbase_transaction(&net, nu5_height, &unified_params)?;
    assert_coinbase_resource_usage(&net, nu5_height, &unified_params, &pre_nu6_2_tx)?;
    assert!(
        pre_nu6_2_tx.orchard_shielded_data().is_some(),
        "an NU5 unified address should prefer its Orchard receiver"
    );
    assert!(
        pre_nu6_2_tx.sapling_outputs().next().is_none(),
        "an NU5 unified address should prefer Orchard over Sapling"
    );
    assert!(
        pre_nu6_2_tx.ironwood_shielded_data().is_none(),
        "an NU5 coinbase cannot contain an Ironwood output"
    );

    let nu6_2_tx = coinbase_transaction(&net, nu6_2_height, &unified_params)?;
    assert_coinbase_resource_usage(&net, nu6_2_height, &unified_params, &nu6_2_tx)?;
    assert!(
        nu6_2_tx.orchard_shielded_data().is_some(),
        "an NU6.2 unified address should receive an Orchard output"
    );
    assert!(
        nu6_2_tx.sapling_outputs().next().is_none(),
        "an NU6.2 unified address should prefer Orchard over Sapling"
    );
    assert!(
        nu6_2_tx.ironwood_shielded_data().is_none(),
        "an NU6.2 coinbase cannot contain an Ironwood output"
    );

    let nu6_3_tx = coinbase_transaction(&net, nu6_3_height, &unified_params)?;
    assert_coinbase_resource_usage(&net, nu6_3_height, &unified_params, &nu6_3_tx)?;
    assert!(
        nu6_3_tx.ironwood_shielded_data().is_some(),
        "an NU6.3 unified address should receive an Ironwood output"
    );
    assert!(
        nu6_3_tx.sapling_outputs().next().is_none(),
        "an NU6.3 unified address should prefer Ironwood over Sapling"
    );
    assert!(
        nu6_3_tx.orchard_shielded_data().is_none(),
        "an NU6.3 coinbase should not contain an Orchard output"
    );

    Ok(())
}

fn shielded_coinbase_testnet() -> Network {
    testnet::Parameters::build()
        .with_activation_heights(ConfiguredActivationHeights {
            overwinter: Some(1),
            sapling: Some(2),
            blossom: Some(3),
            heartwood: Some(4),
            canopy: Some(5),
            nu5: Some(6),
            nu6: Some(7),
            nu6_1: Some(8),
            nu6_2: Some(9),
            nu6_3: Some(10),
            ..Default::default()
        })
        .expect("configured activation heights are valid")
        .clear_funding_streams()
        .to_network()
        .expect("configured network is valid")
}

fn coinbase_transaction(
    net: &Network,
    height: Height,
    miner_params: &MinerParams,
) -> anyhow::Result<Transaction> {
    Ok(
        TransactionTemplate::new_coinbase(net, height, miner_params, Amount::zero(), None)?
            .data()
            .as_ref()
            // Deserialization contains checks for elementary consensus rules,
            // which must pass.
            .zcash_deserialize_into::<Transaction>()?,
    )
}

fn assert_coinbase_resource_usage(
    net: &Network,
    height: Height,
    miner_params: &MinerParams,
    transaction: &Transaction,
) -> anyhow::Result<()> {
    use zcash_transparent::coinbase::MAX_COINBASE_SCRIPT_LEN;

    let resources = TransactionTemplate::coinbase_resource_usage(net, height, miner_params, None)?;
    let coinbase_script_len = transaction.inputs()[0]
        .coinbase_script()
        .expect("generated coinbase input has a canonical script")
        .len();

    assert_eq!(
        resources.max_serialized_size,
        transaction.zcash_serialized_size() + MAX_COINBASE_SCRIPT_LEN - coinbase_script_len,
    );
    assert_eq!(resources.sigops, transaction.sigops()?);
    assert_eq!(
        resources.shielded_action_counts,
        transaction.shielded_action_counts(),
    );

    Ok(())
}

/// Tests that the coinbase cache reuses a previously built coinbase for the same height and fees,
/// so a short-polling miner doesn't re-run the shielded-coinbase proof on every request.
#[test]
fn coinbase_cache_reuses_built_coinbase() {
    use super::CoinbaseCache;

    let net = Network::Mainnet;
    let height = NetworkUpgrade::Nu5
        .activation_height(&net)
        .expect("Nu5 is active on Mainnet");
    let miner_params = MinerParams::from(
        Address::decode(
            &net,
            default_miner_address(net.kind(), &MinerAddressType::Sapling),
        )
        .expect("hard-coded Sapling address is valid"),
    );
    let fee = Amount::zero();

    let build = || {
        TransactionTemplate::new_coinbase(&net, height, &miner_params, fee, None)
            .expect("valid coinbase tx")
    };

    // A shielded coinbase carries a randomized proof, so two fresh builds differ. Identical bytes
    // therefore prove the cache returned a reused transaction rather than rebuilding it.
    let coinbase = build();
    assert_ne!(
        build(),
        coinbase,
        "fresh shielded coinbases differ (randomized proof)"
    );

    let cache = CoinbaseCache::default();
    assert!(cache.get(height, fee).is_none(), "an empty cache misses");

    cache.store(height, fee, coinbase.clone());
    assert_eq!(
        cache.get(height, fee),
        Some(coinbase.clone()),
        "a cache hit reuses the stored coinbase",
    );

    // A different height key misses, so the next request rebuilds.
    let next_height = height.next().expect("height is below Height::MAX");
    assert!(
        cache.get(next_height, fee).is_none(),
        "a different height misses"
    );
}

/// Verifies the fix for #10907: the multi-entry coinbase cache retains both the zero-fee fake
/// coinbase (used for ZIP-317 weight sizing) and the real-fee coinbase simultaneously, so
/// `getblocktemplate` doesn't rebuild shielded proofs on every short-poll.
#[test]
fn coinbase_cache_retains_both_fake_and_real_fee_entries() {
    use super::CoinbaseCache;

    let height = Height(1_000_000);
    let zero_fee = Amount::zero();
    let real_fee: Amount<zakura_chain::amount::NonNegative> =
        Amount::try_from(10_000).expect("valid amount");

    let cache = CoinbaseCache::default();

    // Simulate what getblocktemplate does: store a fake coinbase at zero fee (ZIP-317 sizing),
    // then store the real coinbase at the actual fee.
    let fake_coinbase = TransactionTemplate::new_coinbase(
        &Network::Mainnet,
        height,
        &MinerParams::from(
            Address::decode(
                &Network::Mainnet,
                default_miner_address(
                    zakura_chain::parameters::NetworkKind::Mainnet,
                    &MinerAddressType::Sapling,
                ),
            )
            .unwrap(),
        ),
        zero_fee,
        None,
    )
    .unwrap();

    let real_coinbase = TransactionTemplate::new_coinbase(
        &Network::Mainnet,
        height,
        &MinerParams::from(
            Address::decode(
                &Network::Mainnet,
                default_miner_address(
                    zakura_chain::parameters::NetworkKind::Mainnet,
                    &MinerAddressType::Sapling,
                ),
            )
            .unwrap(),
        ),
        real_fee,
        None,
    )
    .unwrap();

    cache.store(height, zero_fee, fake_coinbase.clone());
    cache.store(height, real_fee, real_coinbase.clone());

    // Both entries coexist — the zero-fee sizing coinbase survives the real-fee store.
    assert_eq!(
        cache.get(height, zero_fee),
        Some(fake_coinbase),
        "zero-fee fake coinbase should still be cached after storing real-fee coinbase"
    );
    assert_eq!(
        cache.get(height, real_fee),
        Some(real_coinbase),
        "real-fee coinbase should be cached"
    );

    // Height transition: storing at a new height evicts the stale entries.
    let next_height = Height(height.0 + 1);
    let next_coinbase = TransactionTemplate::new_coinbase(
        &Network::Mainnet,
        next_height,
        &MinerParams::from(
            Address::decode(
                &Network::Mainnet,
                default_miner_address(
                    zakura_chain::parameters::NetworkKind::Mainnet,
                    &MinerAddressType::Sapling,
                ),
            )
            .unwrap(),
        ),
        zero_fee,
        None,
    )
    .unwrap();

    cache.store(next_height, zero_fee, next_coinbase.clone());
    assert_eq!(
        cache.get(next_height, zero_fee),
        Some(next_coinbase),
        "new-height entry should be cached"
    );
    assert!(
        cache.get(height, zero_fee).is_none(),
        "old-height entry should be evicted"
    );
}

/// Verifies that fee churn beyond the cache cap (4 entries) evicts stale nonzero-fee entries
/// while preserving the zero-fee sizing coinbase. Without this, the cap would clear the
/// entire map — including the zero-fee entry — recreating the original #10907 churn.
#[test]
fn coinbase_cache_preserves_zero_fee_entry_at_capacity() {
    use super::CoinbaseCache;

    let height = Height(2_000_000);
    let zero_fee = Amount::zero();
    let cache = CoinbaseCache::default();

    let miner_params = MinerParams::from(
        Address::decode(
            &Network::Mainnet,
            default_miner_address(
                zakura_chain::parameters::NetworkKind::Mainnet,
                &MinerAddressType::Sapling,
            ),
        )
        .unwrap(),
    );

    let make_coinbase = |fee: Amount<zakura_chain::amount::NonNegative>| {
        TransactionTemplate::new_coinbase(&Network::Mainnet, height, &miner_params, fee, None)
            .unwrap()
    };

    // Store the zero-fee sizing coinbase first.
    let fake_coinbase = make_coinbase(zero_fee);
    cache.store(height, zero_fee, fake_coinbase.clone());

    // Fill to capacity with distinct fee values (simulating mempool fee churn).
    for i in 1..=5u64 {
        let fee = Amount::try_from(i * 1_000).expect("valid amount");
        cache.store(height, fee, make_coinbase(fee));
    }

    // The zero-fee entry must survive eviction at capacity.
    assert_eq!(
        cache.get(height, zero_fee),
        Some(fake_coinbase.clone()),
        "zero-fee sizing coinbase must survive fee churn at capacity"
    );

    // Updating an existing key at capacity should not trigger eviction.
    let fee_1k: Amount<zakura_chain::amount::NonNegative> =
        Amount::try_from(1_000).expect("valid amount");
    let updated_coinbase = make_coinbase(fee_1k);
    cache.store(height, fee_1k, updated_coinbase.clone());
    assert_eq!(
        cache.get(height, fee_1k),
        Some(updated_coinbase),
        "updating an existing key should replace in place"
    );
    assert_eq!(
        cache.get(height, zero_fee),
        Some(fake_coinbase),
        "zero-fee entry must still be present after in-place update"
    );
}

/// A randomized miner clone must not read or overwrite the original miner's caches.
#[test]
fn coinbase_cache_detaches_when_miner_data_changes() {
    use zakura_chain::{block, chain_sync_status::MockSyncStatus};
    use zakura_node_services::BoxError;
    use zakura_test::mock_service::MockService;

    let net = Network::Mainnet;
    let verifier: MockService<zakura_consensus::Request, block::Hash, _, BoxError> =
        MockService::build().for_unit_tests();
    let handler = super::GetBlockTemplateHandler::new_with_pending_blocks(
        &net,
        crate::config::mining::Config {
            miner_address: Some(
                default_miner_address(net.kind(), &MinerAddressType::Transparent)
                    .parse()
                    .unwrap(),
            ),
            ..Default::default()
        },
        verifier,
        MockSyncStatus::default(),
        None,
        Default::default(),
    );
    let height = NetworkUpgrade::Nu5.activation_height(&net).unwrap();
    let fee = Amount::zero();
    let original =
        TransactionTemplate::new_coinbase(&net, height, handler.miner_params().unwrap(), fee, None)
            .unwrap();
    handler.coinbase_cache.store(height, fee, original.clone());

    let mut randomized = handler.clone();
    randomized.randomize_coinbase_data();
    assert!(randomized.template_cache().is_none());
    assert!(randomized.coinbase_cache.get(height, fee).is_none());
    assert!(handler.template_cache().is_some());
    assert_eq!(
        handler.coinbase_cache.get(height, fee),
        Some(original.clone())
    );

    let replacement = TransactionTemplate::new_coinbase(
        &net,
        height,
        randomized.miner_params().unwrap(),
        fee,
        None,
    )
    .unwrap();
    assert_ne!(replacement, original);
    randomized.coinbase_cache.store(height, fee, replacement);
    assert_eq!(handler.coinbase_cache.get(height, fee), Some(original));
}
