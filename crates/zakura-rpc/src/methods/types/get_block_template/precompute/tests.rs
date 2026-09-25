//! Fixed test vectors for the precomputed block template cache.

use std::time::Duration;

use zcash_keys::address::Address;

use zakura_chain::{
    block,
    parameters::{Network, NetworkUpgrade},
    serialization::{DateTime32, Duration32},
    work::difficulty::{CompactDifficulty, ExpandedDifficulty, U256},
};
use zakura_state::GetBlockTemplateChainInfo;

use crate::{
    config::mining::{default_miner_address, MinerAddressType},
    methods::tests::utils::fake_history_tree,
};

use super::*;

/// Returns a template with an explicit timestamp boundary.
fn template_with_max_time(net: &Network, max_time: DateTime32) -> BlockTemplateResponse {
    let tip_height = NetworkUpgrade::Nu5
        .activation_height(net)
        .expect("Nu5 is active on the test network");

    let miner_params = MinerParams::from(
        Address::decode(
            net,
            default_miner_address(net.kind(), &MinerAddressType::Transparent),
        )
        .expect("hard-coded transparent address is valid"),
    );

    let chain_info = GetBlockTemplateChainInfo {
        expected_difficulty: CompactDifficulty::from(ExpandedDifficulty::from(U256::one())),
        tip_height,
        tip_hash: block::Hash([0xab; 32]),
        value_pools: Default::default(),
        cur_time: DateTime32::from(1654008617),
        min_time: DateTime32::from(1654008606),
        max_time,
        chain_history_root: fake_history_tree(net).hash(),
    };

    let long_poll_id = LongPollInput::new(
        chain_info.tip_height,
        chain_info.tip_hash,
        chain_info.max_time,
        std::iter::empty(),
    )
    .generate_id();

    BlockTemplateResponse::new_internal(
        net,
        &CoinbaseCache::default(),
        &miner_params,
        &chain_info,
        long_poll_id,
        vec![],
        None,
    )
    .expect("test parameters produce a valid template")
}

/// Returns a template for the notification and coinbase tests.
fn template() -> BlockTemplateResponse {
    template_with_max_time(&Network::Mainnet, DateTime32::from(1654008719))
}

/// Testnet difficulty changes after the state's minimum-difficulty switchover; other networks and
/// minimum difficulty remain usable even when wall time is beyond the template's timestamp range.
#[test]
fn only_current_work_is_served() {
    let _init_guard = zakura_test::init();
    let network = Network::new_default_testnet();
    let max_time = DateTime32::from(1654008719);
    let after_max_time = max_time.saturating_add(Duration32::from_seconds(1));
    let cache = TemplateCache::default();
    let current = template_with_max_time(&network, max_time);
    let tip_hash = current.previous_block_hash;
    // The state switches to minimum difficulty two target spacings before this abbreviated
    // time range ends.
    let switchover = max_time.saturating_sub(
        Duration32::try_from(
            NetworkUpgrade::target_spacing_for_height(&network, Height(current.height))
                * EXTRA_SPACINGS_TO_MINE_A_BLOCK,
        )
        .expect("two target spacings fit in a Duration32"),
    );

    assert!(cache
        .template_for_tip(tip_hash, &network, switchover)
        .is_none());
    cache.publish(current);
    assert!(
        cache
            .template_for_tip(tip_hash, &network, switchover)
            .is_some(),
        "the switchover time is inclusive",
    );
    assert!(
        cache
            .template_for_tip(block::Hash([0xff; 32]), &network, switchover)
            .is_none(),
        "a template for another tip must never be served",
    );
    assert!(
        cache
            .template_for_tip(tip_hash, &network, after_max_time)
            .is_none(),
        "standard Testnet difficulty must be refreshed after the boundary",
    );

    // If the 90-minute median-time cap is reached before Testnet's difficulty boundary,
    // even a newly built template still uses standard difficulty and this same time range.
    let mut median_capped = template_with_max_time(&network, max_time);
    median_capped.min_time = max_time
        .saturating_sub(Duration32::from_minutes(90))
        .saturating_add(Duration32::from_seconds(1));
    cache.publish(median_capped);
    assert!(
        cache
            .template_for_tip(tip_hash, &network, after_max_time)
            .is_some(),
        "rebuilding cannot change difficulty beyond the median-time cap",
    );

    let mut minimum_difficulty = template_with_max_time(&network, max_time);
    minimum_difficulty.bits = network.target_difficulty_limit().to_compact();
    minimum_difficulty.target = network.target_difficulty_limit();
    cache.publish(minimum_difficulty);
    assert!(
        cache
            .template_for_tip(tip_hash, &network, after_max_time)
            .is_some(),
        "difficulty cannot become easier than the minimum",
    );

    let regtest = Network::new_regtest(
        zakura_chain::parameters::testnet::ConfiguredActivationHeights {
            nu5: Some(100),
            ..Default::default()
        }
        .into(),
    );
    for network in [Network::Mainnet, regtest] {
        cache.publish(template_with_max_time(&network, max_time));
        assert!(
            cache
                .template_for_tip(tip_hash, &network, after_max_time)
                .is_some(),
            "{network:?} does not change difficulty with wall time",
        );
    }
}

/// Checks that a subscription taken before a template is published still reports it.
///
/// `getblocktemplate` reads the cache, decides the client already has that template, and only then
/// waits. A subscription taken at the point of waiting would mark anything published in between as
/// seen, and the caller's other wake conditions are a chain tip change and `max_time`, neither of
/// which fires when the mempool alone changes: it would sit on a template it had already been told
/// about until the updater's backstop.
#[tokio::test]
async fn a_subscription_reports_a_template_published_before_the_wait() {
    let _init_guard = zakura_test::init();

    let cache = TemplateCache::default();

    // The order the RPC uses: subscribe, read, then wait.
    let mut changes = cache.subscribe();
    assert!(cache.is_empty(), "nothing is published yet");

    cache.publish(template());

    tokio::time::timeout(Duration::from_secs(10), changes.changed())
        .await
        .expect("a template published before the wait should still end it");
}

/// Checks that a subscription reports each later publish, so a long poll that loops keeps waiting
/// on templates it hasn't seen rather than on the one it just read.
#[tokio::test]
async fn a_subscription_reports_each_later_publish() {
    let _init_guard = zakura_test::init();

    let cache = TemplateCache::default();
    let mut changes = cache.subscribe();

    for _ in 0..3 {
        cache.publish(template());

        tokio::time::timeout(Duration::from_secs(10), changes.changed())
            .await
            .expect("each publish should end a wait");
    }

    // With nothing published since, the next wait doesn't return.
    assert!(
        tokio::time::timeout(Duration::from_millis(100), changes.changed())
            .await
            .is_err(),
        "a subscription that has seen every publish should keep waiting",
    );
}

/// A reorg must not detach a proof that can still finish and be reused at its original height.
#[tokio::test]
async fn in_flight_coinbase_is_retained_across_height_changes() {
    let _init_guard = zakura_test::init();
    let net = Network::Mainnet;
    let miner_params = MinerParams::from(
        Address::decode(
            &net,
            default_miner_address(net.kind(), &MinerAddressType::Transparent),
        )
        .expect("hard-coded transparent address is valid"),
    );
    let template = template();
    let height = Height(template.height);
    let other_height = height.next().expect("test height is below the maximum");
    let coinbase = template.coinbase_txn;
    let expected_coinbase = coinbase.clone();
    let cache = CoinbaseCache::default();
    let (release_proof, proof_released) = tokio::sync::oneshot::channel();
    let mut next_coinbase = Some((
        height,
        tokio::task::spawn_blocking(move || {
            proof_released
                .blocking_recv()
                .expect("the test releases the proof before awaiting it");
            coinbase
        }),
    ));

    tokio::time::timeout(Duration::from_secs(10), async {
        // Neither consuming at a different height nor starting the next proof may detach this one.
        store_precomputed_coinbase(&mut next_coinbase, other_height, &cache).await;
        start_precomputing_coinbase(
            &mut next_coinbase,
            &net,
            &miner_params,
            other_height,
            Arc::new(Semaphore::new(1)),
        );
        release_proof
            .send(())
            .expect("the in-flight proof is still waiting");
        store_precomputed_coinbase(&mut next_coinbase, height, &cache).await;

        assert_eq!(
            cache.get(height, Amount::zero(), None),
            Some(expected_coinbase),
            "returning to the original height must reuse the tracked proof"
        );
        assert!(
            cache.get(other_height, Amount::zero(), None).is_none(),
            "a proof must not be stored under a different height"
        );
    })
    .await
    .expect("the retained proof should complete once released");
}

/// Finished work for the wrong height must not enter the cache or prevent the next proof.
#[tokio::test]
async fn completed_coinbase_is_replaced_without_caching_the_wrong_height() {
    let _init_guard = zakura_test::init();
    let net = Network::Mainnet;
    let miner_params = MinerParams::from(
        Address::decode(
            &net,
            default_miner_address(net.kind(), &MinerAddressType::Transparent),
        )
        .expect("hard-coded transparent address is valid"),
    );
    let height = Height(template().height);
    let other_height = height.next().expect("test height is below the maximum");
    let cache = CoinbaseCache::default();
    let mut next_coinbase = None;
    start_precomputing_coinbase(
        &mut next_coinbase,
        &net,
        &miner_params,
        height,
        Arc::new(Semaphore::new(1)),
    );

    tokio::time::timeout(Duration::from_secs(10), async {
        while !next_coinbase
            .as_ref()
            .expect("a proof was started")
            .1
            .is_finished()
        {
            tokio::task::yield_now().await;
        }

        store_precomputed_coinbase(&mut next_coinbase, other_height, &cache).await;
        assert!(cache.get(height, Amount::zero(), None).is_none());
        assert!(cache.get(other_height, Amount::zero(), None).is_none());

        start_precomputing_coinbase(
            &mut next_coinbase,
            &net,
            &miner_params,
            other_height,
            Arc::new(Semaphore::new(1)),
        );
        store_precomputed_coinbase(&mut next_coinbase, other_height, &cache).await;
        assert_eq!(
            cache.get(other_height, Amount::zero(), None),
            Some(
                TransactionTemplate::new_coinbase(
                    &net,
                    other_height,
                    &miner_params,
                    Amount::zero(),
                    None,
                )
                .expect("test parameters produce a valid coinbase")
            ),
            "completed stale work must not prevent a proof at the new height"
        );
    })
    .await
    .expect("the replacement proof should complete");
}

/// Checks that a shielded miner address waits longer for a precomputed template than a transparent
/// one, because falling back to an on-demand build runs a coinbase proof per request.
#[test]
fn shielded_miner_addresses_wait_longer_for_a_template() {
    let _init_guard = zakura_test::init();

    let net = Network::new_default_testnet();

    let miner_params = |addr_type| {
        MinerParams::from(
            Address::decode(&net, default_miner_address(net.kind(), &addr_type))
                .expect("hard-coded miner address is valid"),
        )
    };

    let height = Height(template().height);
    let transparent = miner_params(MinerAddressType::Transparent);
    assert!(!transparent.has_shielded_component(&net, height));
    assert_eq!(new_tip_timeout(&transparent, &net, height), NEW_TIP_TIMEOUT);

    for (name, addr_type) in [
        ("a Sapling address", MinerAddressType::Sapling),
        ("a unified address", MinerAddressType::Unified),
    ] {
        let shielded = miner_params(addr_type);
        assert!(
            shielded.has_shielded_component(&net, height),
            "{name} pays a shielded coinbase output"
        );
        assert_eq!(
            new_tip_timeout(&shielded, &net, height),
            SHIELDED_NEW_TIP_TIMEOUT,
            "{name} should wait for the updater instead of proving per request"
        );
    }
}

/// Before NU5, the coinbase pays a unified address's Sapling or transparent receiver, never its
/// Orchard receiver. A unified address without a Sapling receiver has no proof to wait for then.
#[test]
fn orchard_receivers_are_only_shielded_from_nu5() {
    use zcash_keys::address::UnifiedAddress;

    let _init_guard = zakura_test::init();

    let net = Network::Mainnet;
    let Address::Unified(unified) = Address::decode(
        &net,
        default_miner_address(net.kind(), &MinerAddressType::Unified),
    )
    .expect("hard-coded miner address is valid") else {
        panic!("the default unified miner address is unified");
    };
    let orchard_and_transparent = UnifiedAddress::from_receivers(
        unified.orchard().copied(),
        None,
        Some(
            *unified
                .transparent()
                .expect("the default unified miner address has a transparent receiver"),
        ),
    )
    .expect("an Orchard receiver makes a valid unified address");
    let miner_params = MinerParams::from(Address::Unified(orchard_and_transparent));

    let nu5 = NetworkUpgrade::Nu5
        .activation_height(&net)
        .expect("NU5 is active on Mainnet");
    let before_nu5 = nu5.previous().expect("NU5 activates above genesis");

    assert!(!miner_params.has_shielded_component(&net, before_nu5));
    assert_eq!(
        new_tip_timeout(&miner_params, &net, before_nu5),
        NEW_TIP_TIMEOUT
    );
    assert!(miner_params.has_shielded_component(&net, nu5));
    assert_eq!(
        new_tip_timeout(&miner_params, &net, nu5),
        SHIELDED_NEW_TIP_TIMEOUT
    );
}

/// Returns a standard-difficulty Testnet template for the block at `height`, whose time range the
/// state shortened to end before minimum difficulty applies.
fn abbreviated_testnet_template(
    net: &Network,
    height: Height,
    max_time: DateTime32,
) -> BlockTemplateResponse {
    let mut template = template_with_max_time(net, max_time);
    template.height = height.0;
    template.min_time = max_time.saturating_sub(Duration32::from_minutes(30));
    template
}

/// The minimum-difficulty rule applies at the candidate block's height, so a template for the
/// first minimum-difficulty height expires like any later one.
#[test]
fn cached_testnet_work_expires_at_the_first_minimum_difficulty_height() {
    let _init_guard = zakura_test::init();
    let net = Network::new_default_testnet();
    let max_time = DateTime32::from(1654008719);
    let after_max_time = max_time.saturating_add(Duration32::from_seconds(1));
    // The first Testnet height where the minimum-difficulty rule applies.
    let first_height = Height(299_188);
    let cache = TemplateCache::default();
    let template = abbreviated_testnet_template(&net, first_height, max_time);
    let tip_hash = template.previous_block_hash;
    cache.publish(template);

    assert!(
        cache
            .template_for_tip(tip_hash, &net, after_max_time)
            .is_none(),
        "standard difficulty ends at the first minimum-difficulty height",
    );

    let before = abbreviated_testnet_template(
        &net,
        first_height.previous().expect("above genesis"),
        max_time,
    );
    cache.publish(before);
    assert!(
        cache
            .template_for_tip(tip_hash, &net, after_max_time)
            .is_some(),
        "the rule does not apply below its start height",
    );
}

/// The state switches a fresh Testnet template to minimum difficulty two target spacings before
/// the standard-difficulty `max_time`. The cache must stop serving the harder work then too.
#[test]
fn cached_testnet_work_follows_the_minimum_difficulty_switchover() {
    let _init_guard = zakura_test::init();
    let net = Network::new_default_testnet();
    let max_time = DateTime32::from(1654008719);
    let cache = TemplateCache::default();
    let template = template_with_max_time(&net, max_time);
    let height = Height(template.height);
    let tip_hash = template.previous_block_hash;
    cache.publish(abbreviated_testnet_template(&net, height, max_time));

    let extra_time = Duration32::try_from(
        NetworkUpgrade::target_spacing_for_height(&net, height) * EXTRA_SPACINGS_TO_MINE_A_BLOCK,
    )
    .expect("two target spacings fit in a Duration32");
    let switchover = max_time.saturating_sub(extra_time);

    assert!(
        cache.template_for_tip(tip_hash, &net, switchover).is_some(),
        "the state keeps standard difficulty up to the switchover",
    );
    assert!(
        cache
            .template_for_tip(
                tip_hash,
                &net,
                switchover.saturating_add(Duration32::from_seconds(1)),
            )
            .is_none(),
        "the state builds minimum-difficulty work after the switchover",
    );
}

/// Speculative proofs must leave occupied construction capacity to the active build.
#[tokio::test]
async fn next_coinbase_respects_shared_proof_capacity() {
    let net = Network::Mainnet;
    let miner = MinerParams::from(
        Address::decode(
            &net,
            default_miner_address(net.kind(), &MinerAddressType::Transparent),
        )
        .unwrap(),
    );
    let slots = Arc::new(Semaphore::new(1));
    let permit = slots.clone().acquire_owned().await.unwrap();
    let mut next = None;
    let height = Height(template().height);
    start_precomputing_coinbase(&mut next, &net, &miner, height, slots.clone());
    assert!(next.is_none());
    drop(permit);
    start_precomputing_coinbase(&mut next, &net, &miner, height, slots);
    let (_, proof) = next.expect("released capacity permits precomputation");
    proof.await.unwrap();
}

/// ZIP 234 needs the future parent's balance, which speculative proofs cannot know.
#[tokio::test]
async fn next_coinbase_skips_zip234() {
    use zakura_chain::parameters::testnet::{ConfiguredActivationHeights, RegtestParameters};
    let net = Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu6_3: Some(1),
            nu7: Some(5),
            ..Default::default()
        },
        test_nsm_reissuance_height: Some(Height(10)),
        ..Default::default()
    });
    let miner = MinerParams::from(
        Address::decode(
            &net,
            default_miner_address(net.kind(), &MinerAddressType::Transparent),
        )
        .unwrap(),
    );
    let mut next = None;
    start_precomputing_coinbase(
        &mut next,
        &net,
        &miner,
        Height(10),
        Arc::new(Semaphore::new(1)),
    );
    assert!(next.is_none());
}
