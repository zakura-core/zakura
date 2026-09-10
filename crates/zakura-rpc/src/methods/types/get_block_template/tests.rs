//! Tests for types and functions for the `getblocktemplate` RPC.

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
    serialization::ZcashDeserializeInto,
    transaction::Transaction,
    transparent,
};

use crate::client::TransactionTemplate;
use crate::config::mining::{default_miner_address, MinerAddressType};

use super::{MinerParams, TemplatePreparationQueue};

#[test]
fn template_rejection_follows_the_work_across_parents() {
    let parent = zakura_chain::block::Hash([1; 32]);
    let next_parent = zakura_chain::block::Hash([2; 32]);
    let mut state = super::TemplateRejections::default();
    assert!(state.track_parent(parent, Height(1)));
    let prepared = state.scope_work_id("prepared");
    let rejected = state.scope_work_id("rejected");
    let late = state.scope_work_id("late");
    state.mark_prepared(parent, &prepared);
    assert!(state.reject(parent, &rejected));
    assert!(!state.reject(parent, &rejected));
    assert!(state.withdrawn(&rejected));
    assert!(!state.withdrawn(&prepared));
    assert!(state.track_parent(next_parent, Height(2)));
    let next_revision = state.current_revision();
    let next_work = state.scope_work_id("next");
    assert!(state.reject(parent, &late));
    assert_eq!(state.current_revision(), next_revision);
    assert!(state.withdrawn(&late));
    assert!(!state.withdrawn(&next_work));
    assert!(!state.withdrawn(&prepared));
    assert!(state.track_parent(parent, Height(1)));
    assert!(state.needs_fallback());
    assert!(state.is_prepared(&prepared));
    assert!(state.withdrawn(&rejected));
    assert!(!state.track_parent(parent, Height(1)));
}

#[test]
fn template_rejection_storage_fails_closed_at_capacity() {
    let parent = zakura_chain::block::Hash([1; 32]);
    let mut state = super::TemplateRejections::default();
    state.track_parent(parent, Height(1));
    for id in 0..100 {
        state.reject(parent, &state.scope_work_id(&id.to_string()));
    }
    assert_eq!(state.entry(parent).unwrap().rejected.len(), 64);
    assert!(state.contains(&state.scope_work_id("unknown")));
    assert!(state.needs_fallback());
}

#[test]
fn a_saturated_parent_does_not_withdraw_another_parents_work() {
    let saturated = zakura_chain::block::Hash([1; 32]);
    let current = zakura_chain::block::Hash([2; 32]);
    let mut state = super::TemplateRejections::default();
    state.track_parent(saturated, Height(1));
    let old_work = state.scope_work_id("old");
    for id in 0..=super::MAX_REJECTED_WORK_IDS {
        state.reject(saturated, &state.scope_work_id(&id.to_string()));
    }
    assert!(state.entry(saturated).unwrap().saturated);
    state.track_parent(current, Height(2));
    let work = state.scope_work_id("work");
    assert!(!state.withdrawn(&work));
    state.track_parent(saturated, Height(1));
    assert!(state.withdrawn(&old_work));
    assert!(!state.withdrawn(&work));
}

#[test]
fn parent_eviction_retires_work_and_requires_recovery_on_return() {
    let first = zakura_chain::block::Hash([0; 32]);
    let mut state = super::TemplateRejections::default();
    state.track_parent(first, Height(0));
    let rejected = state.scope_work_id("condemned");
    let late = state.scope_work_id("late");
    assert!(state.reject(first, &rejected));
    let old_revision = state.current_revision();
    for id in 1..=super::MAX_TRACKED_PARENTS {
        state.track_parent(
            zakura_chain::block::Hash([u8::try_from(id).unwrap(); 32]),
            Height(u32::try_from(id).unwrap()),
        );
    }
    assert!(state.withdrawn(&rejected));
    assert!(state.withdrawn(&late));
    assert!(!state.reject(first, &late));
    state.track_parent(first, Height(0));
    assert!(state.needs_fallback());
    assert!(state.current_revision() > old_revision);
    let revision = state.current_revision();
    assert!(!state.reject(first, &late));
    assert_eq!(state.current_revision(), revision);
    let recovery = state.scope_work_id("recovery");
    assert!(state.withdrawn(&recovery));
    state.mark_prepared(first, &recovery);
    assert!(!state.withdrawn(&recovery));
    assert!(state.withdrawn(&rejected));
    assert!(state.withdrawn(&late));
    assert_eq!(state.parents.len(), super::MAX_TRACKED_PARENTS);
}

#[test]
fn unrelated_eviction_preserves_retained_work() {
    let retained = zakura_chain::block::Hash([0; 32]);
    let mut state = super::TemplateRejections::default();
    state.track_parent(retained, Height(0));
    let work = state.scope_work_id("valid");
    for id in 1..=super::MAX_TRACKED_PARENTS * 2 {
        let parent = zakura_chain::block::Hash([u8::try_from(id).unwrap(); 32]);
        state.track_parent(parent, Height(u32::try_from(id).unwrap()));
        state.reject(parent, &state.scope_work_id("rejected"));
        state.track_parent(retained, Height(0));
        assert!(!state.withdrawn(&work));
    }
    assert_eq!(state.parents.len(), super::MAX_TRACKED_PARENTS);
}

#[test]
fn prepared_eviction_advances_the_withdrawal_revision() {
    let parent = zakura_chain::block::Hash([1; 32]);
    let mut state = super::TemplateRejections::default();
    state.track_parent(parent, Height(1));
    state.reject(parent, &state.scope_work_id("invalid"));
    let oldest = state.scope_work_id("0");
    for id in 0..super::MAX_PREPARED_WORK_IDS {
        assert!(!state.mark_prepared(parent, &state.scope_work_id(&id.to_string())));
    }
    assert!(!state.withdrawn(&oldest));
    let previous_revision = state.current_revision();
    let newest = state.scope_work_id("newest");
    assert!(state.mark_prepared(parent, &newest));
    assert!(state.current_revision() > previous_revision);
    assert_eq!(
        state.entry(parent).unwrap().prepared.len(),
        super::MAX_PREPARED_WORK_IDS
    );
    assert!(state.withdrawn(&oldest));
    assert!(!state.withdrawn(&newest));
}

#[tokio::test]
async fn template_rejection_retains_notifications_for_late_subscribers() {
    let parent = zakura_chain::block::Hash([1; 32]);
    let mut state = super::TemplateRejections::default();
    state.track_parent(parent, Height(1));
    let work = state.scope_work_id("work");
    let sender = tokio::sync::watch::channel(state).0;
    let mut early = sender.subscribe();
    sender.send_if_modified(|state| state.reject(parent, &work));
    tokio::time::timeout(std::time::Duration::from_secs(1), early.changed())
        .await
        .unwrap()
        .unwrap();
    assert!(early.borrow_and_update().withdrawn(&work));
    assert!(sender.subscribe().borrow().withdrawn(&work));
}

#[test]
fn template_preparation_queue_keeps_the_latest_pending_template() {
    let queue = TemplatePreparationQueue::<u8>::default();

    let (first, mut worker) = queue.enqueue(1).expect("an idle queue admits a worker");
    assert_eq!(first, 1);
    assert!(queue.enqueue(2).is_none());
    assert!(queue.enqueue(3).is_none());
    assert_eq!(worker.next(), Some(3));
    assert_eq!(worker.next(), None);
}

/// A loop that ends without taking its next template still releases the worker slot.
///
/// Every exit releases it, including a `break` or a panic: `running` staying set would stop the
/// node ever preparing another template.
#[test]
fn a_dropped_preparation_worker_releases_the_queue() {
    let queue = TemplatePreparationQueue::<u8>::default();

    let (_first, worker) = queue.enqueue(1).expect("an idle queue admits a worker");
    assert!(queue.enqueue(2).is_none());
    drop(worker);

    let (next, _worker) = queue
        .enqueue(3)
        .expect("a released queue admits the next worker");
    assert_eq!(next, 3, "the discarded loop's pending template is not run");
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
        TransactionTemplate::new_coinbase(&net, height, &miner_params, Amount::zero())?
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
        TransactionTemplate::new_coinbase(&net, Height::MAX, &max_params, Amount::zero())
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
        TransactionTemplate::new_coinbase(net, height, miner_params, Amount::zero())?
            .data()
            .as_ref()
            // Deserialization contains checks for elementary consensus rules,
            // which must pass.
            .zcash_deserialize_into::<Transaction>()?,
    )
}
