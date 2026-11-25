//! Dial rounds with a ranking of preferred gateways: which gateways the
//! round holds places for, which it dials, and what it leaves to the
//! ordinary pass.
//!
//! Each test drives [`CandidateDialerIO::admit_round`] over a test node with
//! 92 connection slots, so the slot checks hold up to ten places, and passes
//! the registered peers itself. No worker starts, so nothing is dialed.

use std::cmp::Ordering;

use zakura_chain::miner_mesh::GatewayId;
use zakura_rt::sync::watch;

use super::super::preferred::MUTUAL_DIAL_GRACE;
use super::*;
use crate::zakura::{testkit::ZakuraTestNode, HeldPlaces};

/// The test node's connection limit, the production default, with which the
/// slot checks may hold ten places.
const MAX_CONNECTIONS: usize = 92;

/// A test node with [`MAX_CONNECTIONS`] slots, and a dialer over it that
/// reads its ranking from the returned sender.
pub(super) async fn dialer(
    seed: u64,
) -> (
    ZakuraTestNode,
    CandidateDialerIO,
    watch::Sender<MinerGatewayRanking>,
) {
    dialer_with_places(seed, 10).await
}

async fn dialer_with_places(
    seed: u64,
    places: usize,
) -> (
    ZakuraTestNode,
    CandidateDialerIO,
    watch::Sender<MinerGatewayRanking>,
) {
    let mut limits = ZakuraLocalLimits::from_config(&crate::Config::default());
    limits.max_connections = MAX_CONNECTIONS;
    limits.max_preferred_connections = places;
    let node = ZakuraTestNode::builder(seed)
        .limits(limits)
        .spawn()
        .await
        .expect("the test node spawns");
    let (ranking_tx, ranking_rx) = watch::channel(MinerGatewayRanking::default());
    let endpoint = node.endpoint();
    let discovery = node.discovery();
    let io = CandidateDialerIO {
        preferred: PreferredPass::new(
            Some(ranking_rx),
            endpoint.local_node_id(),
            discovery.max_concurrent_dials().await,
        ),
        trace: endpoint.trace(),
        limits: node.limits().clone(),
        sought_services: Vec::new(),
        workers: FuturesUnordered::new(),
        endpoint,
        discovery,
    };
    (node, io, ranking_tx)
}

/// `count` gateways `node`'s discovery book can dial, each on its own IP, in
/// rank order: the first has the most blocks. Their IDs sort above the
/// node's own, so the preferred pass dials them without waiting a round.
async fn known_gateways(node: &ZakuraTestNode, count: usize) -> Vec<NodeId> {
    known_gateways_beside(node, count, Ordering::Greater).await
}

/// Like [`known_gateways`], with IDs that sort on `side` of the node's own.
async fn known_gateways_beside(node: &ZakuraTestNode, count: usize, side: Ordering) -> Vec<NodeId> {
    let local = node.endpoint().local_node_id();
    let gateways: Vec<NodeId> = (1..=u8::MAX)
        .map(node_id)
        .filter(|gateway| gateway.as_bytes().cmp(local.as_bytes()) == side)
        .take(count)
        .collect();
    assert_eq!(
        gateways.len(),
        count,
        "the seed leaves enough IDs on that side"
    );
    for gateway in &gateways {
        node.discovery()
            .insert_static_candidate(NodeAddr::with_addrs(
                *gateway,
                [SocketAddr::new(gateway_ip(*gateway), 8233)],
            ))
            .await
            .expect("a static candidate inserts");
    }
    gateways
}

/// The one IP the discovery book lists for `gateway`, a gateway made by
/// [`node_id`] from one byte: 198.51.100 and that byte.
fn gateway_ip(gateway: NodeId) -> IpAddr {
    let byte = (1..=u8::MAX)
        .find(|byte| node_id(*byte) == gateway)
        .expect("test gateways come from one byte");
    IpAddr::from([198, 51, 100, byte])
}

/// A ranking of `gateways` in the given order.
fn ranking(gateways: &[NodeId]) -> MinerGatewayRanking {
    let count = u32::try_from(gateways.len()).expect("a test ranks few gateways");
    MinerGatewayRanking {
        gateways: gateways
            .iter()
            .zip((1..=count).rev())
            .map(|(gateway, blocks)| MinerGateway {
                gateway_id: GatewayId::from_node_id(gateway.as_bytes()),
                blocks,
            })
            .collect(),
    }
}

fn dialed(attempts: &[(DialAttemptKey, NodeAddr)]) -> Vec<NodeId> {
    attempts
        .iter()
        .map(|(attempt, _)| attempt.node_id)
        .collect()
}

fn held(endpoint: &ZakuraEndpoint) -> HeldPlaces {
    endpoint.held_places()
}

// "Test ranking, failover": every ranked gateway is kept, the ten best are
// held, and the concurrent-dial limit spreads their dials over rounds.
#[tokio::test(start_paused = true)]
async fn the_ten_best_of_thirty_one_gateways_are_held_and_dialed_in_rank_order() {
    let (node, mut io, ranking_tx) = dialer(9230).await;
    let gateways = known_gateways(&node, 31).await;
    ranking_tx.send_replace(ranking(&gateways));
    let mut state = CandidateDialerState::new(DIAL_BACKOFF);

    let first = io.admit_round(&mut state, &[], Instant::now()).await;
    assert_eq!(held(&node.endpoint()).members, gateways[..10]);
    assert_eq!(dialed(&first.preferred), gateways[..4]);
    assert!(
        first.ordinary.is_empty(),
        "the preferred dials take every concurrent dial"
    );

    // The first four register, so the next round dials the next four.
    for (attempt, _) in &first.preferred {
        assert!(state.complete(
            *attempt,
            DiscoveryDialResult::Registered,
            None,
            Instant::now()
        ));
    }
    let _authenticated: Vec<_> = gateways[..4]
        .iter()
        .copied()
        .map(|id| node.endpoint().hold_inbound_gateway(id))
        .collect();
    let second = io
        .admit_round(&mut state, &gateways[..4], Instant::now())
        .await;
    assert_eq!(held(&node.endpoint()).members, gateways[..10]);
    assert_eq!(dialed(&second.preferred), gateways[4..8]);
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_gateway_in_backoff_yields_its_place_until_it_connects_or_the_backoff_ends() {
    let (node, mut io, ranking_tx) = dialer(9239).await;
    let discovery = node.discovery();
    let gateways = known_gateways(&node, 11).await;
    ranking_tx.send_replace(ranking(&gateways));
    let third = gateways[2];
    discovery.mark_dial_attempt(&third).await;
    discovery.mark_dial_failure(&third).await;
    let without_third: Vec<_> = gateways.iter().copied().filter(|g| *g != third).collect();

    let mut state = CandidateDialerState::new(DIAL_BACKOFF);
    let round = io.admit_round(&mut state, &[], Instant::now()).await;
    assert_eq!(held(&node.endpoint()).members, without_third);
    assert!(!dialed(&round.preferred).contains(&third));
    assert!(!dialed(&round.ordinary).contains(&third));

    // Calling in ends the wait at once: a connected gateway may hold a place.
    let mut state = CandidateDialerState::new(DIAL_BACKOFF);
    let authenticated: Vec<_> = [third]
        .into_iter()
        .map(|id| node.endpoint().hold_inbound_gateway(id))
        .collect();
    io.admit_round(&mut state, &[third], Instant::now()).await;
    assert_eq!(held(&node.endpoint()).members, gateways[..10]);

    drop(authenticated);
    // So does the end of the backoff, which a success also brings.
    discovery.mark_dial_success(&third, None).await;
    let mut state = CandidateDialerState::new(DIAL_BACKOFF);
    let round = io.admit_round(&mut state, &[], Instant::now()).await;
    assert_eq!(held(&node.endpoint()).members, gateways[..10]);
    assert!(dialed(&round.preferred).contains(&third));
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_connected_or_dialing_member_holds_one_place_and_gets_no_dial() {
    let (node, mut io, ranking_tx) = dialer(9232).await;
    let gateways = known_gateways(&node, 3).await;
    ranking_tx.send_replace(ranking(&gateways));
    let mut state = CandidateDialerState::new(DIAL_BACKOFF);
    // The best gateway called in while a dial to it is still in flight; the
    // second is connected; the third is not.
    state
        .record_attempt(gateways[0], vec![gateway_ip(gateways[0])])
        .expect("an idle dialer records the attempt");

    let _authenticated: Vec<_> = gateways[..2]
        .iter()
        .copied()
        .map(|id| node.endpoint().hold_inbound_gateway(id))
        .collect();
    let round = io
        .admit_round(&mut state, &gateways[..2], Instant::now())
        .await;

    let held = held(&node.endpoint());
    assert_eq!(held.members, gateways);
    assert_eq!(dialed(&round.preferred), gateways[2..]);
    assert!(round.ordinary.is_empty());
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn unknown_gateways_and_the_node_itself_are_neither_held_nor_dialed() {
    let (node, mut io, ranking_tx) = dialer(9233).await;
    let known = known_gateways(&node, 1).await;
    let unknown = node_id(200);
    let own = node.endpoint().local_node_id();
    ranking_tx.send_replace(ranking(&[own, unknown, known[0]]));
    let mut state = CandidateDialerState::new(DIAL_BACKOFF);

    let round = io.admit_round(&mut state, &[], Instant::now()).await;

    let held = held(&node.endpoint());
    assert_eq!(held.members, known);
    assert_eq!(dialed(&round.preferred), known);
    assert_eq!(state.in_flight_node_ids(), known);
    node.shutdown().await;
}

/// A template node whose coinbases name its pool's relay ranks the relay like any other
/// gateway: only the node's own ID is left out.
#[tokio::test(start_paused = true)]
async fn the_relay_a_node_names_is_held_and_dialed_but_its_own_id_is_not() {
    let (node, mut io, ranking_tx) = dialer(9240).await;
    let relay = known_gateways(&node, 1).await;
    let own = node.endpoint().local_node_id();
    // The node's own blocks name the relay, so the relay ranks first.
    ranking_tx.send_replace(ranking(&[relay[0], own]));
    let mut state = CandidateDialerState::new(DIAL_BACKOFF);

    let round = io.admit_round(&mut state, &[], Instant::now()).await;

    assert_eq!(held(&node.endpoint()).members, relay);
    assert_eq!(dialed(&round.preferred), relay);
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_node_at_ordinary_capacity_can_dial_a_funded_gateway() {
    let (node, mut io, ranking_tx) = dialer(9234).await;
    let endpoint = node.endpoint();
    let slots = endpoint.hold_connection_slots(MAX_CONNECTIONS, false);
    let gateways = known_gateways(&node, 1).await;
    ranking_tx.send_replace(ranking(&gateways));
    let mut state = CandidateDialerState::new(DIAL_BACKOFF);

    let round = io.admit_round(&mut state, &[], Instant::now()).await;
    assert_eq!(held(&endpoint).members, gateways);
    assert_eq!(dialed(&round.preferred), gateways);
    assert!(round.ordinary.is_empty());
    drop(slots);
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn an_empty_ranking_leaves_the_round_to_the_ordinary_pass() {
    let (node, mut io, _ranking_tx) = dialer(9235).await;
    let candidates = known_gateways(&node, 3).await;
    let mut state = CandidateDialerState::new(DIAL_BACKOFF);

    let round = io.admit_round(&mut state, &[], Instant::now()).await;

    assert_eq!(held(&node.endpoint()), HeldPlaces::default());
    assert!(round.preferred.is_empty());
    let mut ordinary = dialed(&round.ordinary);
    ordinary.sort_unstable_by_key(|node| *node.as_bytes());
    let mut expected = candidates;
    expected.sort_unstable_by_key(|node| *node.as_bytes());
    assert_eq!(ordinary, expected);
    node.shutdown().await;
}

// A member whose only IP is in this dialer's own backoff can't be dialed, so
// it gives its place to the next gateway until that backoff ends.
#[tokio::test(start_paused = true)]
async fn a_member_the_dialer_backed_off_gives_way_until_its_backoff_ends() {
    let (node, mut io, ranking_tx) = dialer(9236).await;
    let gateways = known_gateways(&node, 11).await;
    ranking_tx.send_replace(ranking(&gateways));
    let backed_off = gateways[2];
    let start = Instant::now();
    let mut state = CandidateDialerState::new(DIAL_BACKOFF);
    let attempt = state
        .record_attempt(backed_off, vec![gateway_ip(backed_off)])
        .expect("an idle dialer records the attempt");
    assert!(state.complete(attempt, DiscoveryDialResult::Failed, None, start));

    let first = io.admit_round(&mut state, &[], start).await;
    let without: Vec<_> = gateways
        .iter()
        .copied()
        .filter(|gateway| *gateway != backed_off)
        .collect();
    assert_eq!(held(&node.endpoint()).members, without);
    assert!(!dialed(&first.preferred).contains(&backed_off));

    // The first round's dials register, and the backoff ends.
    let connected = dialed(&first.preferred);
    for (attempt, _) in &first.preferred {
        assert!(state.complete(*attempt, DiscoveryDialResult::Registered, None, start));
    }
    let _authenticated: Vec<_> = connected
        .iter()
        .copied()
        .map(|id| node.endpoint().hold_inbound_gateway(id))
        .collect();
    let later = start + DIAL_BACKOFF.0;
    let second = io.admit_round(&mut state, &connected, later).await;
    assert_eq!(held(&node.endpoint()).members, gateways[..10]);
    assert!(dialed(&second.preferred).contains(&backed_off));
    node.shutdown().await;
}

// Two gateways that prefer each other would each refuse the other's call if
// both dialed at once, so the one with the higher ID waits out the grace.
#[tokio::test(start_paused = true)]
async fn a_member_below_this_node_waits_out_the_grace_and_one_above_is_dialed_at_once() {
    let (node, mut io, ranking_tx) = dialer(9237).await;
    let below = known_gateways_beside(&node, 1, Ordering::Less).await[0];
    let above = known_gateways(&node, 1).await[0];
    ranking_tx.send_replace(ranking(&[below, above]));
    let mut state = CandidateDialerState::new(DIAL_BACKOFF);
    let start = Instant::now();

    let first = io.admit_round(&mut state, &[], start).await;
    assert_eq!(held(&node.endpoint()).members, vec![below, above]);
    assert_eq!(dialed(&first.preferred), vec![above]);
    assert!(
        first.ordinary.is_empty(),
        "the ordinary pass leaves the waiting member alone"
    );

    let almost = start + MUTUAL_DIAL_GRACE - Duration::from_millis(1);
    let before = io.admit_round(&mut state, &[], almost).await;
    assert!(before.preferred.is_empty() && before.ordinary.is_empty());

    let after = io
        .admit_round(&mut state, &[], start + MUTUAL_DIAL_GRACE)
        .await;
    assert_eq!(dialed(&after.preferred), vec![below]);
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_waiting_member_that_calls_in_gets_no_dial() {
    let (node, mut io, ranking_tx) = dialer(9238).await;
    let below = known_gateways_beside(&node, 1, Ordering::Less).await;
    ranking_tx.send_replace(ranking(&below));
    let mut state = CandidateDialerState::new(DIAL_BACKOFF);
    let start = Instant::now();

    let first = io.admit_round(&mut state, &[], start).await;
    assert!(first.preferred.is_empty());
    // It calls in during the grace, and stays connected past it.
    let authenticated: Vec<_> = below
        .iter()
        .copied()
        .map(|id| node.endpoint().hold_inbound_gateway(id))
        .collect();
    let connected = io
        .admit_round(&mut state, &below, start + MUTUAL_DIAL_GRACE)
        .await;
    assert_eq!(held(&node.endpoint()).members, below);
    assert!(connected.preferred.is_empty());
    drop(authenticated);
    // Once it disconnects, it waits out a whole grace again.
    let gone = start + MUTUAL_DIAL_GRACE * 2;
    let third = io.admit_round(&mut state, &[], gone).await;
    assert!(third.preferred.is_empty());
    let fourth = io
        .admit_round(&mut state, &[], gone + MUTUAL_DIAL_GRACE)
        .await;
    assert_eq!(dialed(&fourth.preferred), below);
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn moved_gateway_yields_the_last_place_until_rollback_can_be_checked() {
    let (node, mut io, ranks) = dialer_with_places(9234, 1).await;
    let gateways = known_gateways(&node, 2).await;
    let (earlier, standby) = (gateways[0], gateways[1]);
    let current = node_id(0);
    let endpoint = node.endpoint();
    let mut slots = endpoint.hold_connection_slots(io.discovery.ordinary_connection_cap, false);
    ranks.send_replace(ranking(&gateways));
    node.supervisor().learn_move(earlier, current);
    let now = Instant::now();
    let due = now + crate::zakura::DEFAULT_ZAKURA_REDIAL_MAX_BACKOFF;

    for (at, expected) in [(now, standby), (due - Duration::from_nanos(1), standby)] {
        tokio::time::advance(at - Instant::now()).await;
        let mut state = CandidateDialerState::new(DIAL_BACKOFF);
        let round = io.admit_round(&mut state, &[], at).await;
        assert_eq!(dialed(&round.preferred), [expected]);
        assert_eq!(state.in_flight_node_ids(), [expected]);
        assert!(!state.is_backed_off(earlier, gateway_ip(earlier), at));
    }
    slots.pop();
    let round = io
        .admit_round(
            &mut CandidateDialerState::new(DIAL_BACKOFF),
            &[],
            Instant::now(),
        )
        .await;
    assert_eq!(
        dialed(&round.preferred),
        [earlier],
        "a free ordinary slot needs no rollback probe"
    );
    slots.extend(endpoint.hold_connection_slots(1, false));
    tokio::time::advance(due - Instant::now()).await;
    let round = io
        .admit_round(&mut CandidateDialerState::new(DIAL_BACKOFF), &[], due)
        .await;
    assert_eq!(
        dialed(&round.preferred),
        [earlier],
        "the due gateway can use the held place"
    );
    drop(slots);
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn chained_gateway_moves_yield_both_places_to_standbys() {
    let (node, mut io, ranks) = dialer_with_places(9234, 2).await;
    let gateways = known_gateways(&node, 4).await;
    ranks.send_replace(ranking(&gateways));
    node.supervisor().learn_move(gateways[0], gateways[1]);
    node.supervisor().learn_move(gateways[1], node_id(0));
    let slots = node
        .endpoint()
        .hold_connection_slots(MAX_CONNECTIONS, false);

    let mut state = CandidateDialerState::new(DIAL_BACKOFF);
    let round = io.admit_round(&mut state, &[], Instant::now()).await;
    assert_eq!(held(&node.endpoint()).members, gateways[2..]);
    assert_eq!(dialed(&round.preferred), gateways[2..]);

    drop(slots);
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn moved_gateway_ranked_under_both_ids_gets_one_dial_per_round() {
    let (node, mut io, ranks) = dialer(9234).await;
    let gateways = known_gateways(&node, 2).await;
    let (earlier, current) = (gateways[0], gateways[1]);
    ranks.send_replace(ranking(&gateways));
    node.supervisor().learn_move(earlier, current);
    let mut state = CandidateDialerState::new(DIAL_BACKOFF);
    let round = io.admit_round(&mut state, &[], Instant::now()).await;
    assert_eq!(dialed(&round.preferred), [earlier]);
    assert!(round.ordinary.is_empty());
    let next = io.admit_round(&mut state, &[], Instant::now()).await;
    assert!(next.preferred.is_empty() && next.ordinary.is_empty());
    node.shutdown().await;
}

#[tokio::test]
async fn moved_gateway_report_survives_eviction_of_the_earlier_record(
) -> Result<(), crate::BoxError> {
    use crate::zakura::{
        testkit::{LocalEndpointFactory, TEST_NET_TIMEOUT},
        GatewayPlace, ZakuraDiscoveryConfig, ZakuraDiscoveryLocalConfig, ZakuraHandshakeConfig,
    };
    let (node, mut io, ranks) = dialer(9340).await;
    let peer = ZakuraTestNode::builder(9341)
        .extended_credential()
        .spawn()
        .await?;
    let earlier = peer.earlier_node_addr().await;
    let current = peer.node_addr().await.id;
    let handshake = ZakuraHandshakeConfig::for_network(&zakura_chain::parameters::Network::Mainnet);
    let book = |seed| {
        let local = ZakuraDiscoveryLocalConfig {
            secret_key: LocalEndpointFactory::secret_key(seed),
            direct_addrs: vec![SocketAddr::from(([93, 184, 216, 34], 8233))],
            services: vec![ZakuraServiceId::discovery()],
            zakura_protocol_min: handshake.zakura_protocol_min,
            zakura_protocol_max: handshake.zakura_protocol_max,
            network_id: handshake.network_id,
            chain_id: handshake.chain_id,
            last_authored_sequence: None,
        };
        let mut config = ZakuraDiscoveryConfig::default();
        config.book_limits.max_records = 1;
        ZakuraDiscoveryHandle::new(local, config, node.supervisor().subscribe())
    };
    io.discovery = book(9340)?;
    let old_record = (*book(9341)?.current_self_record()).clone();
    assert_eq!(old_record.body.node_id(), earlier.id);
    io.discovery
        .import_peer_records([old_record], Some(node_id(9)))
        .await?;
    assert!(io.discovery.record_for(earlier.id).await.is_some());
    node.connect_native_to_addr(earlier.clone(), TEST_NET_TIMEOUT)
        .await?;
    let replacement = (*book(9342)?.current_self_record()).clone();
    let replacement_id = replacement.body.node_id();
    io.discovery
        .import_connected_peer_record(
            replacement,
            replacement_id,
            Some(IpAddr::from([93, 184, 216, 34])),
        )
        .await?;
    assert!(
        io.discovery.record_for(earlier.id).await.is_none(),
        "the single-entry book evicts the relayed record"
    );
    ranks.send_replace(ranking(&[earlier.id]));
    let state = CandidateDialerState::new(DIAL_BACKOFF);
    io.publish_preferred_gateways(&state, &[current], Instant::now())
        .await;
    let report = node.endpoint().connection_places();
    // The peer's handshake proved the earlier ID the blocks name, so the
    // peer fills that gateway's held place under the ID it has now.
    assert_eq!(
        (report.gateways[0].place, report.gateways[0].connected),
        (GatewayPlace::Preferred, true)
    );
    assert_eq!(report.peers.len(), 1);
    assert_eq!(report.peers[0].node_id, *current.as_bytes());
    assert!(report.peers[0].preferred);
    assert_eq!((report.slots.ordinary, report.slots.preferred), (0, 1));
    peer.shutdown().await;
    tokio::time::timeout(
        TEST_NET_TIMEOUT,
        node.supervisor().subscribe().wait_for(Vec::is_empty),
    )
    .await??;
    io.publish_preferred_gateways(&state, &[], Instant::now())
        .await;
    let report = node.endpoint().connection_places();
    assert_eq!(
        (report.gateways[0].place, report.gateways[0].connected),
        (GatewayPlace::Unmatched, false)
    );
    node.shutdown().await;
    Ok(())
}
