//! Connection places held for preferred mining gateways.
//!
//! The tests publish a ranking into the slot counters and drive the
//! handler's own claim and admission checks, as dials and inbound
//! connections reach them, without a network. Discovery dials, bootstrap
//! redials and legacy-upgrade dials all claim an outbound slot for their
//! target's node ID the same way, so an outbound claim stands for each.

use super::*;
use zakura_chain::miner_mesh::GatewayId;

use crate::zakura::{
    GatewayPlace, GatewayStanding, ProvenNode, RankedGateway, SlotSplit, MAX_RANKED_GATEWAYS,
};

fn handler_from_config(config: &Config) -> ZakuraProtocolHandler {
    let limits = ZakuraLocalLimits::from_config(config);
    ZakuraProtocolHandler::new(
        ZakuraSupervisorHandle::new(DEFAULT_ZAKURA_MAX_CONNS_PER_IP),
        Network::Mainnet,
        ZakuraHandshakeConfig::for_network(&Network::Mainnet),
        limits,
    )
}

fn handler(ordinary: usize, extra: usize) -> ZakuraProtocolHandler {
    let mut config = Config::default();
    config.zakura.max_connections = ordinary;
    config.zakura.peer_memory_budget_bytes = u64::try_from(config.zakura.quic_memory_bytes(
        &config.network,
        ordinary + extra,
        &[],
    ))
    .expect("test capacities fit in u64");
    handler_from_config(&config)
}

/// A fresh node ID. Slots and rankings key on its 32 bytes alone, so no key
/// is generated: deriving a node key pair is slow enough in debug builds for
/// the wall-clock handshake cadence to refill a token in the middle of a test.
fn new_node() -> NodeId {
    let mut bytes = [0; 32];
    rand_10::Rng::fill_bytes(&mut zakura_rt::rand::rng(), &mut bytes);
    NodeId::from_bytes(&bytes)
}

/// Publishes `ranking`, highest first, with the hold limit of the
/// handler's connection limit.
fn publish(handler: &ZakuraProtocolHandler, ranking: &[(NodeId, GatewayStanding)]) {
    let count = u32::try_from(ranking.len()).expect("a test ranking is short");
    let ranked = ranking
        .iter()
        .zip((1..=count).rev())
        .map(|(&(node, standing), blocks)| RankedGateway::resolved(node, blocks, standing));
    handler.publish_gateway_ranking(ranked);
}

/// `count` gateways, each in `standing`.
fn gateways(count: usize, standing: GatewayStanding) -> Vec<(NodeId, GatewayStanding)> {
    (0..count).map(|_| (new_node(), standing)).collect()
}

/// Claims `count` slots for unrelated nodes in one direction.
fn fill(
    handler: &ZakuraProtocolHandler,
    count: usize,
    inbound: bool,
) -> Vec<ConnectionAdmissionGuard> {
    (0..count)
        .map(|_| {
            handler
                .claim_connection(new_node(), inbound)
                .expect("the test leaves room for its ordinary peers")
        })
        .collect()
}

fn ordinary(handler: &ZakuraProtocolHandler, inbound: bool) -> Result<(), SlotRefusal> {
    handler.claim_connection(new_node(), inbound).map(drop)
}

fn split(handler: &ZakuraProtocolHandler) -> SlotSplit {
    handler.supervisor.ip_slots.counts().slot_split()
}

/// Each ranked gateway's place, in rank order.
fn places(handler: &ZakuraProtocolHandler) -> Vec<GatewayPlace> {
    let counts = handler.supervisor.ip_slots.counts();
    let places = counts.preferred_gateways().places();
    places.map(|(_, place)| place).collect()
}

fn preferred_set(handler: &ZakuraProtocolHandler) -> Vec<NodeId> {
    handler
        .supervisor
        .ip_slots
        .counts()
        .preferred_gateways()
        .set()
        .to_vec()
}

/// Whether the acceptor admits a caller before its node ID is known. An
/// accepted attempt hands its transport reservation back, as an attempt
/// whose claim is refused after TLS does.
fn admits_a_caller(handler: &ZakuraProtocolHandler, last_byte: u8) -> bool {
    let incoming = IncomingInfo {
        remote: SocketAddr::new(IpAddr::from([198, 51, 100, last_byte]), 8234),
        validated: true,
        pending_total: 0,
        pending_from_ip: 0,
    };
    let admitted = handler.admit(&incoming) == Admit::Accept;
    if admitted {
        handler.finish_admission(false);
    }
    admitted
}

/// With no ranking, or a ranking whose gateways v2 doesn't know, nothing is
/// held: 69 inbound peers, 23 dials, 92 in all, as without held places.
/// (A ranked gateway v2 doesn't know is still matched when it calls in, by
/// design; see the unknown-caller test.)
#[test]
fn an_empty_or_unmatched_ranking_keeps_todays_limits() {
    for ranking in [Vec::new(), gateways(3, GatewayStanding::Unmatched)] {
        let handler = handler(92, 10);
        publish(&handler, &ranking);
        assert_eq!(
            handler
                .supervisor
                .ip_slots
                .counts()
                .preferred_gateways()
                .hold(),
            0
        );
        let inbound = fill(&handler, 69, true);
        assert_eq!(ordinary(&handler, true), Err(SlotRefusal::InboundShare));
        assert!(!admits_a_caller(&handler, 1));
        assert_eq!(handler.free_connection_slots(), 23);
        let dials = fill(&handler, 23, false);
        assert_eq!(ordinary(&handler, false), Err(SlotRefusal::Full));
        assert_eq!(handler.free_connection_slots(), 0);
        drop((inbound, dials));
        assert!(admits_a_caller(&handler, 2));
    }
}

/// Below ten matched gateways the hold is the matched count, and ordinary
/// dials retain their configured capacity.
#[test]
fn the_hold_equals_the_matched_gateways_below_ten() {
    let handler = handler(92, 10);
    let ranked = gateways(3, GatewayStanding::Dialable);
    publish(&handler, &ranked);
    assert_eq!(handler.free_connection_slots(), 92);
    let _dials = fill(&handler, 92, false);
    assert_eq!(handler.free_connection_slots(), 0);
    assert_eq!(ordinary(&handler, false), Err(SlotRefusal::Hold));
    let _gateways: Vec<_> = ranked
        .iter()
        .map(|&(node, _)| {
            handler
                .claim_connection(node, false)
                .expect("a gateway takes its held place")
        })
        .collect();
    assert_eq!(
        split(&handler),
        SlotSplit {
            preferred: 3,
            ordinary: 92,
            ordinary_inbound: 0,
        }
    );
}

/// Memory for one additional gateway leaves the ordinary inbound share intact.
#[test]
fn a_small_limit_shrinks_the_hold_and_keeps_the_inbound_share() {
    let handler = handler(40, 1);
    let ranked = gateways(12, GatewayStanding::Dialable);
    publish(&handler, &ranked);
    assert_eq!(
        handler
            .supervisor
            .ip_slots
            .counts()
            .preferred_gateways()
            .hold(),
        1
    );
    let _inbound = fill(&handler, 30, true);
    assert_eq!(ordinary(&handler, true), Err(SlotRefusal::InboundShare));
    let _dials = fill(&handler, 10, false);
    assert_eq!(ordinary(&handler, false), Err(SlotRefusal::Hold));
    let _gateway = handler
        .claim_connection(ranked[0].0, false)
        .expect("the top gateway takes the one held place");
    assert_eq!(
        preferred_set(&handler),
        [ranked[0].0],
        "only the top gateway is preferred"
    );
}

/// At the ordinary limit a dial to a gateway is granted, and an ordinary
/// dial (discovery, a bootstrap redial or a legacy upgrade) is refused.
#[test]
fn at_the_ordinary_limit_only_a_gateway_dial_is_granted() {
    let handler = handler(92, 10);
    let ranked = gateways(10, GatewayStanding::Dialable);
    publish(&handler, &ranked);
    let _dials = fill(&handler, 92, false);
    assert_eq!(handler.free_connection_slots(), 0);
    assert_eq!(ordinary(&handler, false), Err(SlotRefusal::Hold));
    let _gateway = handler
        .claim_connection(ranked[9].0, false)
        .expect("the lowest member still has its held place");
    assert_eq!(split(&handler).preferred, 1);
}

/// With the inbound share and the ordinary limit both full, a gateway that
/// calls in is admitted and granted, and an ordinary caller is refused.
#[test]
fn an_inbound_gateway_passes_a_full_share_and_ordinary_limit() {
    let handler = handler(92, 10);
    let ranked = gateways(10, GatewayStanding::Dialable);
    publish(&handler, &ranked);
    let _inbound = fill(&handler, 69, true);
    let _dials = fill(&handler, 23, false);
    assert!(
        admits_a_caller(&handler, 1),
        "a held place is free before the caller's ID is known"
    );
    let _gateway = handler
        .claim_connection(ranked[0].0, true)
        .expect("a gateway calls into its held place");
    assert_eq!(ordinary(&handler, true), Err(SlotRefusal::Hold));
    assert_eq!(
        split(&handler),
        SlotSplit {
            preferred: 1,
            ordinary: 92,
            ordinary_inbound: 69,
        },
        "the gateway sits above the inbound share"
    );
}

/// A ranked gateway v2 never matched is granted when it calls into a free
/// held place. Once no held place is free and the inbound share is full,
/// the next caller is refused before its ID is read.
#[test]
fn an_unknown_ranked_caller_takes_a_free_place_or_is_refused_before_its_id() {
    let handler = handler(40, 1);
    let unknown = new_node();
    let member = new_node();
    publish(
        &handler,
        &[
            (unknown, GatewayStanding::Unmatched),
            (new_node(), GatewayStanding::Unmatched),
            (member, GatewayStanding::Dialable),
        ],
    );
    assert_eq!(preferred_set(&handler), [member]);
    let _inbound = fill(&handler, 30, true);
    assert!(admits_a_caller(&handler, 1));
    let _unknown = handler
        .claim_connection(unknown, true)
        .expect("the caller outranks the member, so it takes the held place");
    assert_eq!(preferred_set(&handler), [unknown]);
    assert!(
        !admits_a_caller(&handler, 2),
        "no held place is free and the inbound share is full"
    );
    assert_eq!(ordinary(&handler, true), Err(SlotRefusal::InboundShare));
}

/// A gateway waiting out a failed dial is granted when it calls in. When
/// its claim is refused, the preferred set stays as it was.
#[test]
fn a_caller_in_backoff_is_granted() {
    let handler = handler(40, 1);
    let waiting = new_node();
    let member = new_node();
    publish(
        &handler,
        &[
            (waiting, GatewayStanding::Backoff),
            (member, GatewayStanding::Dialable),
        ],
    );
    assert_eq!(preferred_set(&handler), [member]);

    let _dials = fill(&handler, 40, false);
    let member_slot = handler.claim_connection(member, false).expect("held");
    assert_eq!(
        handler.claim_connection(waiting, true).map(drop),
        Err(SlotRefusal::Full)
    );
    assert_eq!(
        preferred_set(&handler),
        [member],
        "a refusal changes nothing"
    );
    drop(member_slot);

    let _waiting = handler
        .claim_connection(waiting, true)
        .expect("the caller's backoff is ignored");
    assert_eq!(preferred_set(&handler), [waiting]);
    assert_eq!(split(&handler).preferred, 1);
}

/// A full ordinary pool can immediately admit every funded gateway.
#[test]
fn default_and_explicit_memory_budgets_fund_gateways_after_ordinary_peers() {
    for (budget, extra) in [(None, 10), (Some(15_u64 << 30), 1), (Some(14_u64 << 30), 0)] {
        let mut config = Config::default();
        if let Some(bytes) = budget {
            config.zakura.peer_memory_budget_bytes = bytes;
        }
        let handler = handler_from_config(&config);
        let limits = &handler.limits;
        assert_eq!(limits.max_preferred_connections, extra);
        let ordinary_cap = limits.max_connections;
        if extra > 0 {
            assert_eq!(ordinary_cap, 92);
        }
        let bind = limits.quic_bind_config(Vec::new());
        assert_eq!(
            bind.max_connections,
            ordinary_cap + extra + limits.max_pending_handshakes
        );
        assert!(
            config
                .zakura
                .quic_memory_bytes(&config.network, ordinary_cap + extra, &[])
                <= u128::from(config.zakura.peer_memory_budget_bytes)
        );
        let inbound = limits.max_inbound_connections();
        let _inbound = fill(&handler, inbound, true);
        let _dials = fill(&handler, ordinary_cap - inbound, false);
        let ranked = gateways(11, GatewayStanding::Dialable);
        publish(&handler, &ranked);
        assert_eq!(hold(&handler), extra);
        assert_eq!(handler.free_connection_slots(), 0);
        assert!(ordinary(&handler, false).is_err());
        let mut guards = Vec::new();
        for &(node, _) in &ranked[..extra] {
            assert!(admits_a_caller(
                &handler,
                u8::try_from(guards.len() + 1).unwrap()
            ));
            guards.push(
                handler
                    .claim_connection(node, true)
                    .expect("funded gateway place"),
            );
        }
        assert_eq!(split(&handler).ordinary, ordinary_cap);
        assert_eq!(split(&handler).preferred, extra);
        assert!(!admits_a_caller(&handler, 2));
        assert_eq!(
            handler.claim_connection(ranked[extra].0, false).map(drop),
            Err(SlotRefusal::Full)
        );
    }
}

/// A set change recounts the slots already held, with no claim or release:
/// a gateway that leaves the set stays connected and counts as ordinary.
#[test]
fn a_set_change_only_recounts_held_slots() {
    let handler = handler(92, 10);
    let ranked = gateways(11, GatewayStanding::Dialable);
    publish(&handler, &ranked[..10]);
    let _gateways: Vec<_> = ranked[..10]
        .iter()
        .map(|&(node, _)| handler.claim_connection(node, true).expect("held"))
        .collect();
    let _dials = fill(&handler, 92, false);
    assert_eq!(
        (split(&handler).preferred, split(&handler).ordinary),
        (10, 92)
    );

    let mut changed = ranked[1..].to_vec();
    changed[9].1 = GatewayStanding::Dialable;
    publish(&handler, &changed);
    assert_eq!(
        (split(&handler).preferred, split(&handler).ordinary),
        (9, 93)
    );
    assert_eq!(handler.supervisor.ip_slots.counts().connections(), 102);
}

/// A member's second slot is ordinary, so at the ordinary limit it is
/// refused.
#[test]
fn a_second_slot_of_a_member_is_ordinary() {
    let handler = handler(92, 10);
    let ranked = gateways(10, GatewayStanding::Dialable);
    publish(&handler, &ranked);
    let gateway = ranked[0].0;
    let _first = handler.claim_connection(gateway, false).expect("held");
    let _dials = fill(&handler, 92, false);
    assert_eq!(
        handler.claim_connection(gateway, true).map(drop),
        Err(SlotRefusal::Hold)
    );
    assert_eq!(split(&handler).preferred, 1);
}

/// A caller admitted only for a held place pays its handshake tokens even
/// though its claim is refused once its ID is known, so the hold gives
/// anonymous callers no free retries: an address that keeps calling runs
/// out of its cadence.
#[test]
fn a_caller_refused_by_the_hold_keeps_its_cadence_charge() {
    let handler = handler(92, 10);
    publish(&handler, &gateways(1, GatewayStanding::Dialable));
    let _dials = fill(&handler, 92, false);
    let incoming = IncomingInfo {
        remote: SocketAddr::new(IpAddr::from([203, 0, 113, 7]), 8234),
        validated: true,
        pending_total: 0,
        pending_from_ip: 0,
    };
    let burst = DEFAULT_ZAKURA_MAX_CONNS_PER_IP;
    for _ in 0..burst {
        assert_eq!(handler.admit(&incoming), Admit::Accept);
        assert_eq!(ordinary(&handler, true), Err(SlotRefusal::Hold));
        handler.finish_admission(false);
    }
    assert_eq!(
        handler.admit(&incoming),
        Admit::Refuse,
        "the address spent its burst on refused claims"
    );
}

/// Claims racing from several threads never take ordinary slots past the
/// ordinary limit, or preferred ones past the hold.
#[test]
fn simultaneous_claims_never_pass_the_ordinary_limit() {
    let handler = handler(92, 10);
    let ranked = gateways(10, GatewayStanding::Dialable);
    publish(&handler, &ranked);
    let granted: Vec<ConnectionAdmissionGuard> = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..8)
            .map(|worker| {
                let handler = &handler;
                let ranked = &ranked;
                scope.spawn(move || {
                    let mut held = Vec::new();
                    for claim in 0..30 {
                        let node = if claim % 3 == 0 {
                            ranked[(worker + claim) % ranked.len()].0
                        } else {
                            new_node()
                        };
                        if let Ok(guard) = handler.claim_connection(node, claim % 2 == 0) {
                            held.push(guard);
                        }
                    }
                    held
                })
            })
            .collect();
        workers
            .into_iter()
            .flat_map(|worker| worker.join().expect("a claiming thread finishes"))
            .collect()
    });
    let split = split(&handler);
    assert!(split.preferred <= 10, "{split:?}");
    assert!(split.ordinary <= 92, "{split:?}");
    assert!(split.ordinary_inbound <= 69, "{split:?}");
    assert_eq!(split.preferred + split.ordinary, granted.len());
}

fn publish_tied_gateways(handler: &ZakuraProtocolHandler, first: NodeId, second: NodeId) {
    let ranking =
        [first, second].map(|node| RankedGateway::resolved(node, 1, GatewayStanding::Dialable));
    handler.publish_gateway_ranking(ranking);
}

/// Between ranking updates, the connected incumbent keeps the tied place.
#[test]
fn equally_ranked_gateways_call_in_between_ranking_updates() {
    let handler = handler(40, 1);
    let first = new_node();
    let second = new_node();
    publish_tied_gateways(&handler, first, second);

    assert!(admits_a_caller(&handler, 1));
    let incumbent = handler.claim_connection(first, true).expect("held place");
    assert!(admits_a_caller(&handler, 2));
    let _second = handler
        .claim_connection(second, true)
        .expect("ordinary place");
    assert_eq!(preferred_set(&handler), [first]);
    assert_eq!(
        split(&handler),
        SlotSplit {
            preferred: 1,
            ordinary: 1,
            ordinary_inbound: 1
        }
    );

    drop(incumbent);
    assert_eq!(preferred_set(&handler), [second]);
}

/// A dial gets the connected tie-break only after the transport authenticates it.
#[test]
fn outbound_gateway_keeps_a_tied_place_after_authentication() {
    for authenticated in [false, true] {
        let handler = handler(40, 1);
        let target = new_node();
        let caller = new_node();
        publish_tied_gateways(&handler, target, caller);
        let mut dial = handler.claim_connection(target, false).expect("held place");
        if authenticated {
            dial.outbound_connected(target).expect("the same node");
        }
        let _inbound = handler
            .claim_connection(caller, true)
            .expect("room for caller");
        assert_eq!(
            preferred_set(&handler),
            [if authenticated { target } else { caller }]
        );
    }
}

/// Snapshot order cannot erase a TLS result or displace an equally ranked live incumbent.
#[test]
fn ranking_publication_reconciles_authenticated_slots() {
    for inbound in [false, true] {
        let handler = handler(40, 1);
        let incumbent = new_node();
        let challenger = new_node();
        publish_tied_gateways(&handler, incumbent, challenger);
        let stale = [challenger, incumbent]
            .map(|node| RankedGateway::resolved(node, 1, GatewayStanding::Dialable));
        let mut slot = handler.claim_connection(incumbent, inbound).unwrap();
        if !inbound {
            slot.outbound_connected(incumbent).expect("the same node");
        }
        handler.publish_gateway_ranking(stale);
        assert_eq!(preferred_set(&handler), [incumbent]);

        let _challenger = handler.claim_connection(challenger, true).unwrap();
        handler.publish_gateway_ranking(stale);
        assert_eq!(preferred_set(&handler), [incumbent]);

        // Keep an unauthenticated dial to the same identity after its TLS owner leaves.
        let _pending = handler.claim_connection(incumbent, false).unwrap();
        drop(slot);
        assert_eq!(preferred_set(&handler), [challenger]);
        let mut stale_connected = stale;
        stale_connected[1] = stale_connected[1].with_standing(GatewayStanding::Connected);
        handler.publish_gateway_ranking(stale_connected);
        assert_eq!(preferred_set(&handler), [challenger]);
    }
}

/// Each outbound TLS owner keeps its own proof until its guard drops.
#[test]
fn a_stale_connected_snapshot_cannot_restore_a_closed_dial() {
    let handler = handler(40, 1);
    let gateway = new_node();
    let other = new_node();
    let snapshot = [
        (gateway, GatewayStanding::Connected),
        (other, GatewayStanding::Dialable),
    ];
    let mut first = handler.claim_connection(gateway, false).unwrap();
    let mut second = handler.claim_connection(gateway, false).unwrap();
    first.outbound_connected(gateway).expect("the same node");
    second.outbound_connected(gateway).expect("the same node");
    drop(first);
    publish(&handler, &snapshot);
    assert_eq!(preferred_set(&handler), [gateway]);
    drop(second);
    publish(&handler, &snapshot);
    assert_eq!(preferred_set(&handler), [other]);
}

/// Blocks name a gateway by the node ID it had before it added a key. A dial
/// to that ID reaches the node under the ID it has now, which proved the
/// earlier one. The slot the dial took in the gateway's held place moves to
/// that ID and stays a preferred one, with the ordinary slots full, and
/// releases from there.
#[test]
fn a_dial_that_reached_a_moved_gateway_keeps_its_held_place() {
    let handler = handler(40, 1);
    let earlier = new_node();
    let current = new_node();
    let slots = |node: &NodeId| {
        let counts = handler.supervisor.ip_slots.counts();
        counts.node_connections(node).total()
    };
    publish(&handler, &[(earlier, GatewayStanding::Dialable)]);
    let _peers = fill(&handler, 40, false);
    let held = SlotSplit {
        preferred: 1,
        ordinary: 40,
        ordinary_inbound: 0,
    };
    let mut dial = handler
        .claim_connection(earlier, false)
        .expect("held place");
    assert_eq!(split(&handler), held);

    dial.outbound_connected(ProvenNode::new(current, &[earlier]))
        .expect("the node keeps the gateway's place");
    assert_eq!((slots(&earlier), slots(&current)), (0, 1));
    assert_eq!(split(&handler), held);
    assert_eq!(preferred_set(&handler), [current]);
    assert_eq!(places(&handler), [GatewayPlace::Preferred]);
    // The next ranking still names the earlier ID. The proof outlives it.
    publish(&handler, &[(earlier, GatewayStanding::Connected)]);
    assert_eq!(preferred_set(&handler), [current]);
    assert_eq!(split(&handler), held);

    drop(dial);
    assert_eq!((slots(&earlier), slots(&current)), (0, 0));
    assert_eq!(split(&handler).preferred, 0);
    assert!(preferred_set(&handler).is_empty());
    // Nothing proves the earlier ID any more, so a ranking that still calls
    // it connected holds the place for nobody.
    publish(&handler, &[(earlier, GatewayStanding::Connected)]);
    assert!(preferred_set(&handler).is_empty());
    assert_eq!(places(&handler), [GatewayPlace::Backoff]);
}

/// A dial's slot is charged to the ID its node last answered with. When
/// another node answers, one that doesn't prove that ID, the slot the dial
/// took in the gateway's held place moves to the node's own ID, where it is
/// an ordinary one, and releases from there.
#[test]
fn a_dial_answered_by_another_node_gives_its_place_back() {
    let handler = handler(40, 1);
    let earlier = new_node();
    let current = new_node();
    let slots = |node: &NodeId| {
        let counts = handler.supervisor.ip_slots.counts();
        counts.node_connections(node).total()
    };
    publish(&handler, &[(earlier, GatewayStanding::Dialable)]);
    let mut dial = handler
        .claim_connection(earlier, false)
        .expect("held place");
    assert_eq!(split(&handler).preferred, 1);

    dial.outbound_connected(current)
        .expect("an ordinary slot is free");
    assert_eq!((slots(&earlier), slots(&current)), (0, 1));
    assert_eq!(
        split(&handler),
        SlotSplit {
            preferred: 0,
            ordinary: 1,
            ordinary_inbound: 0,
        }
    );

    drop(dial);
    assert_eq!((slots(&earlier), slots(&current)), (0, 0));
    assert_eq!(split(&handler), SlotSplit::default());
}

/// A node that answers a dial without proving the ID the dial's slot was
/// claimed for loses the held place the dial claimed, and with the ordinary
/// slots full its slot has nowhere to go. The move is refused and the slot
/// leaves with the dial. Were it allowed, one gateway could answer each dial
/// under a new ID and turn every held place into an ordinary slot of its
/// own, above the ordinary limit. Proving some other earlier ID changes
/// nothing: only the gateway's own ID keeps its place.
#[test]
fn a_node_that_does_not_prove_the_gateways_id_takes_no_slot_above_the_ordinary_limit() {
    let handler = handler(40, 2);
    let earlier = new_node();
    let other = new_node();
    let ranking = [
        (earlier, GatewayStanding::Dialable),
        (other, GatewayStanding::Dialable),
    ];
    let mut peers = fill(&handler, 40, false);
    let full = SlotSplit {
        preferred: 0,
        ordinary: 40,
        ordinary_inbound: 0,
    };

    for unrelated in [&[][..], &[new_node()]] {
        publish(&handler, &ranking);
        let mut dial = handler
            .claim_connection(earlier, false)
            .expect("held place");
        assert_eq!(
            dial.outbound_connected(ProvenNode::new(new_node(), unrelated)),
            Err(SlotRefusal::Hold),
            "no ordinary slot is free for the ID the node answers with"
        );
        assert_eq!(
            split(&handler),
            SlotSplit {
                preferred: 1,
                ..full
            },
            "a refused move leaves the slot where the dial claimed it"
        );
        assert_eq!(preferred_set(&handler), [earlier, other]);
        drop(dial);
        assert_eq!(split(&handler), full);
    }

    // With an ordinary slot free the node connects under its own ID, and
    // the other gateway still finds its held place.
    peers.pop();
    publish(&handler, &ranking);
    let mut dial = handler
        .claim_connection(earlier, false)
        .expect("held place");
    dial.outbound_connected(new_node())
        .expect("an ordinary slot is free");
    assert_eq!(split(&handler), full);
    let _other = handler.claim_connection(other, false).expect("held place");
    assert_eq!(
        split(&handler),
        SlotSplit {
            preferred: 1,
            ..full
        }
    );
}

/// One key holder answers dials to its gateway's earlier ID under a new ID
/// each time, and every one of them proves the earlier ID. The first fills
/// the gateway's one place. A second connected node makes the gateway
/// ambiguous, so it fills no place and needs an ordinary slot, and it is
/// refused while that would put the ordinary slots above their limit. The
/// key holder never fills more than the one place, and the other gateway
/// keeps its own.
#[test]
fn a_gateway_that_answers_under_successive_ids_holds_one_place_at_most() {
    let handler = handler(40, 2);
    let (earlier, other) = (new_node(), new_node());
    let (first, second) = (new_node(), new_node());
    let ranking = [
        (earlier, GatewayStanding::Dialable),
        (other, GatewayStanding::Dialable),
    ];
    publish(&handler, &ranking);
    let mut peers = fill(&handler, 40, false);
    let one_held = SlotSplit {
        preferred: 1,
        ordinary: 40,
        ordinary_inbound: 0,
    };

    let mut dial = handler
        .claim_connection(earlier, false)
        .expect("held place");
    dial.outbound_connected(ProvenNode::new(first, &[earlier]))
        .expect("the first node keeps the gateway's place");
    assert_eq!(split(&handler), one_held);
    assert_eq!(preferred_set(&handler), [first, other]);

    // The next dial is charged to the earlier ID or to the ID the node
    // last answered with. Neither is a free held place any more.
    for charged in [earlier, first] {
        assert_eq!(
            handler.claim_connection(charged, false).map(drop),
            Err(SlotRefusal::Hold)
        );
    }

    // With one ordinary slot free the dial starts, and the node answers
    // under a second ID. That makes the gateway ambiguous, which would turn
    // the first node's slot into a 41st ordinary one.
    peers.pop();
    let mut again = handler
        .claim_connection(first, false)
        .expect("an ordinary slot is free");
    assert_eq!(
        again.outbound_connected(ProvenNode::new(second, &[earlier])),
        Err(SlotRefusal::Hold)
    );
    assert_eq!(split(&handler), one_held, "the refusal changed nothing");
    assert_eq!(preferred_set(&handler), [first, other]);
    drop(again);

    // With two free, both nodes hold ordinary slots and the gateway holds
    // no place. The slots never pass the ordinary limit.
    peers.pop();
    let mut again = handler
        .claim_connection(first, false)
        .expect("an ordinary slot is free");
    again
        .outbound_connected(ProvenNode::new(second, &[earlier]))
        .expect("both nodes fit in the ordinary slots");
    let both_ordinary = SlotSplit {
        preferred: 0,
        ordinary: 40,
        ordinary_inbound: 0,
    };
    assert_eq!(split(&handler), both_ordinary);
    assert_eq!(preferred_set(&handler), [other]);
    assert_eq!(
        places(&handler),
        [GatewayPlace::Ambiguous, GatewayPlace::Preferred]
    );
    // A new ranking reads the same two connections the same way.
    publish(&handler, &ranking);
    assert_eq!(split(&handler), both_ordinary);
    assert_eq!(preferred_set(&handler), [other]);
    let _other = handler.claim_connection(other, false).expect("held place");
    assert_eq!(
        split(&handler),
        SlotSplit {
            preferred: 1,
            ..both_ordinary
        }
    );

    // Once the first node leaves, the second is the gateway's one node.
    drop(dial);
    publish(&handler, &ranking);
    assert_eq!(preferred_set(&handler), [second, other]);
    assert_eq!(
        split(&handler),
        SlotSplit {
            preferred: 2,
            ordinary: 38,
            ordinary_inbound: 0,
        }
    );
}

/// A gateway that added a key calls in under its new ID, which proves the
/// ID blocks still name it by. It takes the gateway's held place past a full
/// inbound share and ordinary limit, as a caller with the exact ID does. A
/// caller that proves another ID, or none, is an ordinary one.
#[test]
fn a_moved_gateway_calls_in_to_its_held_place() {
    let rankings: [fn(NodeId) -> RankedGateway; 2] = [
        |earlier| RankedGateway::resolved(earlier, 1, GatewayStanding::Dialable),
        |earlier| RankedGateway::unresolved(GatewayId::from_node_id(earlier.as_bytes()), 1),
    ];
    for ranked in rankings {
        let handler = handler(40, 1);
        let (earlier, current) = (new_node(), new_node());
        handler.publish_gateway_ranking([ranked(earlier)]);
        let _inbound = fill(&handler, 30, true);
        let _dials = fill(&handler, 10, false);
        assert!(ordinary(&handler, true).is_err());

        for unproven in [
            ProvenNode::from(current),
            ProvenNode::new(current, &[new_node()]),
        ] {
            assert!(handler.claim_slot(unproven, true).is_err());
            assert!(preferred_set(&handler).iter().all(|node| *node == earlier));
        }
        let caller = handler
            .claim_slot(ProvenNode::new(current, &[earlier]), true)
            .expect("the gateway's held place");
        assert_eq!(
            split(&handler),
            SlotSplit {
                preferred: 1,
                ordinary: 40,
                ordinary_inbound: 30,
            }
        );
        assert_eq!(preferred_set(&handler), [current]);

        drop(caller);
        assert_eq!(split(&handler).preferred, 0);
        assert!(preferred_set(&handler).is_empty());
    }
}

/// A dial to a gateway's earlier ID is in flight and holds the gateway's
/// place when the gateway calls in under its new ID. The call is a second
/// slot for the one gateway, so it needs an ordinary slot, as a call from
/// the exact ID would. When the dial lands, the node holds the place with
/// one slot and an ordinary slot with the other.
#[test]
fn a_moved_gateway_that_calls_in_during_a_dial_to_it_takes_no_second_place() {
    let handler = handler(40, 1);
    let (earlier, current) = (new_node(), new_node());
    let proved = [earlier];
    let caller = ProvenNode::new(current, &proved);
    publish(&handler, &[(earlier, GatewayStanding::Dialable)]);
    let mut peers = fill(&handler, 40, false);
    let mut dial = handler
        .claim_connection(earlier, false)
        .expect("held place");

    assert!(
        handler.claim_slot(caller, true).is_err(),
        "the dial holds the gateway's one place"
    );
    assert_eq!(preferred_set(&handler), [earlier]);
    peers.pop();
    let _call = handler
        .claim_slot(caller, true)
        .expect("an ordinary slot is free");
    let one_held = SlotSplit {
        preferred: 1,
        ordinary: 40,
        ordinary_inbound: 1,
    };
    assert_eq!(split(&handler), one_held);
    assert_eq!(preferred_set(&handler), [earlier]);
    // A new ranking leaves the place with the dial too.
    publish(&handler, &[(earlier, GatewayStanding::Dialing)]);
    assert_eq!(preferred_set(&handler), [earlier]);
    assert_eq!(split(&handler), one_held);

    dial.outbound_connected(caller)
        .expect("the node keeps the place its dial held");
    assert_eq!(preferred_set(&handler), [current]);
    assert_eq!(
        split(&handler),
        SlotSplit {
            preferred: 1,
            ordinary: 40,
            ordinary_inbound: 0,
        }
    );
}

/// Cancelling the old-ID dial gives its place to the already connected
/// gateway before discovery publishes another ranking.
#[test]
fn cancelling_a_dial_gives_its_place_to_the_connected_moved_gateway() {
    let handler = handler(40, 1);
    let (earlier, current) = (new_node(), new_node());
    publish(&handler, &[(earlier, GatewayStanding::Dialable)]);
    let dial = handler
        .claim_connection(earlier, false)
        .expect("held place");
    let _inbound = handler
        .claim_slot(ProvenNode::new(current, &[earlier]), true)
        .expect("the gateway fits in an ordinary slot while the dial holds its place");
    assert_eq!(preferred_set(&handler), [earlier]);
    assert_eq!(split(&handler).ordinary_inbound, 1);

    drop(dial);

    assert_eq!(preferred_set(&handler), [current]);
    assert_eq!(
        split(&handler),
        SlotSplit {
            preferred: 1,
            ordinary: 0,
            ordinary_inbound: 0,
        }
    );
}

/// The node with a gateway's exact ID and a node that proved that ID as an
/// earlier one are both connected, in either order: one key holder runs two
/// nodes. The second is refused while the ordinary slots are full, and the
/// first keeps the place. With room for it the gateway is ambiguous and
/// holds no place, so the key holder never fills more than one.
#[test]
fn the_exact_id_and_a_node_that_proved_it_share_one_place_at_most() {
    for exact_first in [true, false] {
        let handler = handler(40, 1);
        let (earlier, current) = (new_node(), new_node());
        let exact = ProvenNode::from(earlier);
        let proved = [earlier];
        let moved = ProvenNode::new(current, &proved);
        let (first, second) = if exact_first {
            (exact, moved)
        } else {
            (moved, exact)
        };
        publish(&handler, &[(earlier, GatewayStanding::Dialable)]);
        let mut peers = fill(&handler, 40, false);

        let _first = handler.claim_slot(first, true).expect("held place");
        assert_eq!(preferred_set(&handler), [first.id()]);
        assert!(
            handler.claim_slot(second, true).is_err(),
            "the second node needs an ordinary slot"
        );
        assert_eq!(
            preferred_set(&handler),
            [first.id()],
            "the refusal restored the set"
        );
        assert_eq!(split(&handler).preferred, 1);

        // One free slot is not enough: the first node's slot becomes an
        // ordinary one too.
        peers.pop();
        assert!(handler.claim_slot(second, true).is_err());
        assert_eq!(preferred_set(&handler), [first.id()]);
        peers.pop();
        let _second = handler
            .claim_slot(second, true)
            .expect("both fit in the ordinary slots");
        assert!(preferred_set(&handler).is_empty());
        assert_eq!(places(&handler), [GatewayPlace::Ambiguous]);
        assert_eq!(
            split(&handler),
            SlotSplit {
                preferred: 0,
                ordinary: 40,
                ordinary_inbound: 2,
            }
        );
        publish(&handler, &[(earlier, GatewayStanding::Connected)]);
        assert!(preferred_set(&handler).is_empty());
        assert_eq!(split(&handler).preferred, 0);
    }
}

/// One key holder runs the node with a gateway's exact ID and a second node
/// that proved that ID, and blocks name the second node's own gateway ID as
/// well. The second node calls in: it fills its own gateway's place, and its
/// proof makes the first gateway ambiguous, which would leave the first
/// node's slot an ordinary one above the limit. The call is refused and
/// nothing changes, as a dial that reached that node would be. With an
/// ordinary slot free for the first node, the call is granted.
#[test]
fn a_caller_that_makes_another_gateway_ambiguous_adds_no_slot_above_the_ordinary_limit() {
    assert_disputed_gateway_limit(40, false, SlotRefusal::Hold);
}

/// Reclassifying an existing inbound gateway must also leave the outbound reserve free.
#[test]
fn a_gateway_proof_preserves_the_ordinary_inbound_share() {
    assert_disputed_gateway_limit(
        without_outbound_reserve(40),
        true,
        SlotRefusal::InboundShare,
    );
}

fn assert_disputed_gateway_limit(peer_count: usize, inbound: bool, refusal: SlotRefusal) {
    let handler = handler(40, 2);
    let (exact, second, other) = (new_node(), new_node(), new_node());
    let proved = [exact];
    let caller = ProvenNode::new(second, &proved);
    let ranking = [
        (exact, GatewayStanding::Dialable),
        (second, GatewayStanding::Dialable),
        (other, GatewayStanding::Dialable),
    ];
    publish(&handler, &ranking);
    let mut peers = fill(&handler, peer_count, inbound);
    let _exact = handler.claim_connection(exact, true).expect("held place");
    let one_held = SlotSplit {
        preferred: 1,
        ordinary: peer_count,
        ordinary_inbound: if inbound { peer_count } else { 0 },
    };
    assert_eq!(split(&handler), one_held);
    assert_eq!(preferred_set(&handler), [exact, second]);

    assert_eq!(handler.claim_slot(caller, true).map(drop), Err(refusal));
    assert_eq!(split(&handler), one_held, "the refusal changed nothing");
    assert_eq!(preferred_set(&handler), [exact, second]);
    // A dial that reached the second node meets the same limit.
    let mut dial = handler
        .claim_connection(second, false)
        .expect("the second gateway's held place");
    assert_eq!(dial.outbound_connected(caller), Err(refusal));
    assert_eq!(preferred_set(&handler), [exact, second]);
    drop(dial);
    assert_eq!(split(&handler), one_held);
    assert_eq!(
        preferred_set(&handler),
        [exact, other],
        "the closed dial enters backoff"
    );

    peers.pop();
    let _second = handler
        .claim_slot(caller, true)
        .expect("the first node's slot fits in the ordinary slots");
    assert_eq!(
        split(&handler),
        SlotSplit {
            preferred: 1,
            ordinary: peer_count,
            ordinary_inbound: if inbound { peer_count } else { 1 },
        }
    );
    assert_eq!(preferred_set(&handler), [second, other]);
    assert_eq!(
        places(&handler),
        [
            GatewayPlace::Ambiguous,
            GatewayPlace::Preferred,
            GatewayPlace::Preferred
        ]
    );
}

/// While blocks name a node by its earlier ID and by the ID it has now, two
/// ranked gateways name the one connected node. It fills one place with its
/// one slot, and the slot counts never disagree with the slots held.
#[test]
fn a_node_that_two_ranked_gateways_name_fills_one_place() {
    let handler = handler(40, 3);
    let (earlier, current, other) = (new_node(), new_node(), new_node());
    let ranking = [
        (earlier, GatewayStanding::Dialable),
        (current, GatewayStanding::Dialable),
        (other, GatewayStanding::Dialable),
    ];
    publish(&handler, &ranking);
    assert_eq!(hold(&handler), 3);
    let _peers = fill(&handler, 40, false);

    let _caller = handler
        .claim_slot(ProvenNode::new(current, &[earlier]), true)
        .expect("held place");
    assert_eq!(
        hold(&handler),
        2,
        "one place for the node, one for the other"
    );
    assert_eq!(preferred_set(&handler), [current, other]);
    assert_eq!(
        split(&handler),
        SlotSplit {
            preferred: 1,
            ordinary: 40,
            ordinary_inbound: 0,
        }
    );
    publish(&handler, &ranking);
    assert_eq!(preferred_set(&handler), [current, other]);
    assert_eq!(split(&handler).preferred, 1);
    assert_eq!(ordinary(&handler, false), Err(SlotRefusal::Hold));
    let _other = handler.claim_connection(other, false).expect("held place");
    assert_eq!(ordinary(&handler, false), Err(SlotRefusal::Full));
}

/// A ranking is formed from discovery's snapshot, which knows a moved
/// gateway by its earlier ID. Publication resolves the gateway to the
/// connected node that proved that ID, whatever standing the snapshot gave
/// it, and to nobody once that node's proved slot is gone.
#[test]
fn publication_resolves_a_gateway_to_the_owner_that_proved_its_earlier_id() {
    for inbound in [true, false] {
        for standing in [
            GatewayStanding::Connected,
            GatewayStanding::Dialable,
            GatewayStanding::Backoff,
        ] {
            let handler = handler(40, 1);
            let (earlier, current) = (new_node(), new_node());
            let proved = [earlier];
            let node = ProvenNode::new(current, &proved);
            let slot = if inbound {
                handler.claim_slot(node, true).expect("an ordinary slot")
            } else {
                let mut dial = handler.claim_connection(current, false).expect("a slot");
                dial.outbound_connected(node).expect("the same node");
                dial
            };
            assert!(preferred_set(&handler).is_empty(), "no gateway is ranked");

            publish(&handler, &[(earlier, standing)]);
            assert_eq!(preferred_set(&handler), [current]);
            assert_eq!(split(&handler).preferred, 1);

            // An unproved dial to the node holds no proof of its own.
            let _pending = handler.claim_connection(current, false).expect("a slot");
            drop(slot);
            publish(&handler, &[(earlier, standing)]);
            assert!(!preferred_set(&handler).contains(&current));
            assert_eq!(split(&handler).preferred, 0);
        }
    }
}

/// An authenticated owner at the truncation boundary beats its equally scored peers,
/// while a lower-scoring owner stays outside the retained ranking.
#[test]
fn publication_checks_live_owners_at_the_ranking_cutoff() {
    let handler = handler(40, 1);
    let mut nodes: Vec<_> = (0..258).map(|_| new_node()).collect();
    nodes.sort_by_key(|node| *node.as_bytes());
    let tied = nodes[256];
    let lower = nodes[257];
    let _lower = handler.claim_connection(lower, true).unwrap();
    let _tied = handler.claim_connection(tied, true).unwrap();
    handler.publish_gateway_ranking(nodes.into_iter().map(|node| {
        RankedGateway::resolved(
            node,
            if node == lower { 1 } else { 2 },
            GatewayStanding::Dialable,
        )
    }));
    assert_eq!(preferred_set(&handler), [tied]);
}

/// A ranked caller that outranks a member already holding a slot takes that
/// member's place: the caller's slot is preferred and the member's stays
/// connected as ordinary, one over the ordinary limit until a slot frees.
#[test]
fn a_ranked_caller_displaces_a_connected_member() {
    // The budget funds two additional gateways.
    let handler = handler(44, 2);
    let top = new_node();
    let caller = new_node();
    let displaced = new_node();
    publish(
        &handler,
        &[
            (top, GatewayStanding::Dialable),
            (caller, GatewayStanding::Backoff),
            (displaced, GatewayStanding::Dialable),
        ],
    );
    assert_eq!(preferred_set(&handler), [top, displaced]);
    let _displaced = handler.claim_connection(displaced, false).expect("held");
    let _dials = fill(&handler, 44, false);

    let _caller = handler
        .claim_connection(caller, true)
        .expect("the caller takes the place it outranks");
    assert_eq!(preferred_set(&handler), [top, caller]);
    assert_eq!(
        split(&handler),
        SlotSplit {
            preferred: 1,
            ordinary: 45,
            ordinary_inbound: 0,
        }
    );
    assert_eq!(handler.supervisor.ip_slots.counts().connections(), 46);
}

/// A promotion can displace a connected inbound gateway only when its
/// connection fits in the ordinary inbound share.
#[test]
fn gateway_promotion_preserves_the_ordinary_inbound_share() {
    let ranked_id = new_node();
    let moved_id = new_node();
    let earlier = [ranked_id];
    for caller in [ranked_id.into(), ProvenNode::new(moved_id, &earlier)] {
        let handler = handler(4, 1);
        let incumbent = new_node();
        publish(
            &handler,
            &[
                (ranked_id, GatewayStanding::Backoff),
                (incumbent, GatewayStanding::Dialable),
            ],
        );
        let _incumbent = handler
            .claim_connection(incumbent, true)
            .expect("held place");
        let mut peers = fill(&handler, 3, true);
        let before = split(&handler);

        assert_eq!(
            handler.claim_slot(caller, true).map(drop),
            Err(SlotRefusal::InboundShare)
        );
        assert_eq!(split(&handler), before);
        assert_eq!(preferred_set(&handler), [incumbent]);

        peers.pop();
        let _caller = handler
            .claim_slot(caller, true)
            .expect("the incumbent fits in the inbound share");
        assert_eq!(preferred_set(&handler), [caller.id()]);
        assert_eq!(split(&handler), before);
    }
}

/// The higher-ranked gateway can authenticate and stay connected outbound
/// while an inbound incumbent needs the held place. A ranking publication
/// and a ranked dial's cancellation must respect the same inbound share.
#[test]
fn a_connected_gateway_waits_for_room_to_promote_its_outbound_slot() {
    let handler = handler(8, 1);
    let (higher, incumbent, pending) = (new_node(), new_node(), new_node());
    let ranking = [
        (higher, GatewayStanding::Backoff),
        (incumbent, GatewayStanding::Dialable),
        (pending, GatewayStanding::Dialable),
    ];
    publish(&handler, &ranking);
    let _incumbent = handler.claim_connection(incumbent, true).unwrap();
    let mut peers = fill(&handler, 6, true);
    let pending_dial = handler.claim_connection(pending, false).unwrap();
    let current = new_node();
    let mut dial = handler.claim_connection(higher, false).unwrap();
    dial.outbound_connected(ProvenNode::new(current, &[higher]))
        .expect("the gateway can keep its ordinary outbound slot");
    let check = || {
        assert_eq!(preferred_set(&handler), [incumbent]);
        assert_eq!(split(&handler).ordinary_inbound, 6);
        assert_eq!(
            handler
                .supervisor
                .ip_slots
                .counts()
                .node_connections(&current)
                .outbound,
            1
        );
    };
    check();
    publish(&handler, &ranking);
    check();
    drop(pending_dial);
    check();

    peers.pop();
    assert_eq!(preferred_set(&handler), [current]);
    assert_eq!(split(&handler).ordinary_inbound, 6);
}

/// A ranked caller that already holds a slot re-forms the set, but its
/// second slot is ordinary; when the ordinary slots are full the claim is
/// refused for the hold and the set is restored.
#[test]
fn the_set_is_restored_after_a_hold_refusal() {
    let handler = handler(44, 2);
    let top = new_node();
    let caller = new_node();
    let member = new_node();
    publish(
        &handler,
        &[
            (top, GatewayStanding::Dialable),
            (caller, GatewayStanding::Backoff),
            (member, GatewayStanding::Dialable),
        ],
    );
    let _member = handler.claim_connection(member, false).expect("held");
    let _caller_dial = handler
        .claim_connection(caller, false)
        .expect("the caller's first slot is ordinary");
    let _dials = fill(&handler, 43, false);
    let before = split(&handler);

    assert_eq!(
        handler.claim_connection(caller, true).map(drop),
        Err(SlotRefusal::Hold)
    );
    assert_eq!(preferred_set(&handler), [top, member]);
    assert_eq!(split(&handler), before);
}

fn hold(handler: &ZakuraProtocolHandler) -> usize {
    handler
        .supervisor
        .ip_slots
        .counts()
        .preferred_gateways()
        .hold()
}

/// A ranked caller granted a place, then dropped before any new ranking,
/// leaves the set: its place stays held and the next gateway in rank takes
/// it.
#[test]
fn a_dropped_caller_leaves_the_set() {
    let handler = handler(40, 1);
    let caller = new_node();
    let next = new_node();
    publish(
        &handler,
        &[
            (caller, GatewayStanding::Backoff),
            (next, GatewayStanding::Dialable),
        ],
    );
    let slot = handler.claim_connection(caller, true).expect("held");
    assert_eq!(preferred_set(&handler), [caller]);

    drop(slot);
    assert_eq!(preferred_set(&handler), [next]);
    assert_eq!(hold(&handler), 1);
}

/// A connected member leaves the set when its last slot is
/// released, and a member with a slot left stays.
#[test]
fn a_connected_member_leaves_the_set_with_its_last_slot() {
    let handler = handler(40, 1);
    let member = new_node();
    let next = new_node();
    publish(
        &handler,
        &[
            (member, GatewayStanding::Dialable),
            (next, GatewayStanding::Dialable),
        ],
    );
    let first = handler.claim_connection(member, false).expect("held");
    let second = handler
        .claim_connection(member, true)
        .expect("an ordinary slot is free");

    drop(first);
    assert_eq!(
        preferred_set(&handler),
        [member],
        "it still holds a slot, so it stays connected"
    );
    drop(second);
    assert_eq!(preferred_set(&handler), [next]);
    assert_eq!(hold(&handler), 1);
}

/// A member published as being dialed, or as dialable, that holds a slot
/// and then loses it leaves the set like a connected one.
#[test]
fn a_dialing_or_dialable_member_leaves_the_set_with_its_last_slot() {
    for (standing, inbound) in [
        (GatewayStanding::Dialing, false),
        (GatewayStanding::Dialable, true),
    ] {
        let handler = handler(40, 1);
        let member = new_node();
        let next = new_node();
        publish(
            &handler,
            &[(member, standing), (next, GatewayStanding::Dialable)],
        );
        let slot = handler.claim_connection(member, inbound).expect("held");
        assert_eq!(preferred_set(&handler), [member]);

        drop(slot);
        assert_eq!(preferred_set(&handler), [next], "{standing:?}");
        assert_eq!(hold(&handler), 1);
    }
}

/// A node ID whose first 20 bytes are `prefix` and the rest `last`, so two
/// such IDs can share a gateway ID.
fn node_with_prefix(prefix: u8, last: u8) -> NodeId {
    let mut bytes = [last; 32];
    bytes[..20].fill(prefix);
    NodeId::from_bytes(&bytes)
}

/// A node whose connection was proved after discovery's snapshot found no
/// node with the gateway ID: publication resolves the gateway to it, and it
/// takes the held place.
#[test]
fn publication_resolves_a_gateway_to_an_owner_proved_after_the_snapshot() {
    let handler = handler(40, 1);
    let gateway = node_with_prefix(1, 0x00);
    let _inbound = handler
        .claim_connection(gateway, true)
        .expect("room for an ordinary inbound peer");

    handler.publish_gateway_ranking([RankedGateway::unresolved(
        GatewayId::from_node_id(gateway.as_bytes()),
        1,
    )]);

    assert_eq!(preferred_set(&handler), [gateway]);
    assert_eq!(hold(&handler), 1);
}

/// A gateway tied at the ranking cutoff that discovery's snapshot could not
/// resolve joins at publication once a proved owner has its gateway ID, like
/// a gateway above the cutoff.
#[test]
fn publication_resolves_a_tied_gateway_to_an_owner_proved_after_the_snapshot() {
    let handler = handler(40, 1);
    // The highest gateway ID sorts after the others, so it is the one tied
    // entry left past the cutoff.
    let gateway = node_with_prefix(0xff, 0x00);
    let _inbound = handler
        .claim_connection(gateway, true)
        .expect("room for an ordinary inbound peer");

    let others = (0..MAX_RANKED_GATEWAYS).map(|index| {
        let mut id = [0; 20];
        // The ranking holds at most 256 gateways, so the index fits in two bytes.
        id[18..].copy_from_slice(&(index as u16).to_be_bytes());
        RankedGateway::unresolved(GatewayId::from_bytes(id), 1)
    });
    let tied = RankedGateway::unresolved(GatewayId::from_node_id(gateway.as_bytes()), 1);
    handler.publish_gateway_ranking(others.chain([tied]).collect::<Vec<_>>());

    assert_eq!(preferred_set(&handler), [gateway]);
    assert_eq!(hold(&handler), 1);
}

/// A proved owner that shares the gateway ID of the node discovery's
/// snapshot named makes the gateway ambiguous: neither node is preferred and
/// no place is held.
#[test]
fn publication_fails_closed_when_a_proved_owner_shares_a_resolved_gateway_id() {
    let handler = handler(40, 1);
    let named = node_with_prefix(1, 0x00);
    let sharing = node_with_prefix(1, 0xff);
    let _inbound = handler
        .claim_connection(sharing, true)
        .expect("room for an ordinary inbound peer");

    handler.publish_gateway_ranking([RankedGateway::resolved(named, 1, GatewayStanding::Dialable)]);

    assert!(preferred_set(&handler).is_empty());
    assert_eq!(hold(&handler), 0);
}
