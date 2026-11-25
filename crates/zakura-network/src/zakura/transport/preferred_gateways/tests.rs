use proptest::prelude::*;
use zakura_quic::NodeSecretKey;

use super::*;

fn node(seed: u8) -> NodeId {
    NodeSecretKey::from_bytes(&[seed; 32]).public()
}

fn gateway(seed: u8, blocks: u32, standing: GatewayStanding) -> RankedGateway {
    RankedGateway::resolved(node(seed), blocks, standing)
}

/// A node ID made of `prefix` in its first 20 bytes and `last` in the rest,
/// so two such IDs can share a gateway ID.
fn raw_node(prefix: u8, last: u8) -> NodeId {
    let mut bytes = [last; 32];
    bytes[..20].fill(prefix);
    NodeId::from_bytes(&bytes)
}

/// The ranked entry for `node`'s gateway ID before any node is known.
fn unresolved(node: NodeId, blocks: u32) -> RankedGateway {
    RankedGateway::unresolved(GatewayId::from_node_id(node.as_bytes()), blocks)
}

/// Each ranked gateway's place, in rank order: with one place held, a
/// gateway in backoff counts towards the hold but leaves the set to the best
/// dialable gateway, the next dialable gateway waits on standby, and an
/// unmatched gateway takes no place.
#[test]
fn places_read_each_standing_against_the_set() {
    let formed = PreferredGateways::form(
        [
            gateway(1, 4, GatewayStanding::Backoff),
            gateway(2, 3, GatewayStanding::Dialable),
            gateway(3, 2, GatewayStanding::Dialable),
            gateway(4, 1, GatewayStanding::Unmatched),
        ],
        1,
    );

    let places: Vec<_> = formed
        .places()
        .map(|(gateway, place)| (gateway.node(), place))
        .collect();
    assert_eq!(
        places,
        vec![
            (Some(node(1)), GatewayPlace::Backoff),
            (Some(node(2)), GatewayPlace::Preferred),
            (Some(node(3)), GatewayPlace::Standby),
            (Some(node(4)), GatewayPlace::Unmatched),
        ]
    );
    assert_eq!(formed.hold(), 1);
}

#[test]
fn an_empty_ranking_holds_nothing() {
    let empty = PreferredGateways::form([], MAX_PREFERRED_GATEWAYS);
    assert_eq!(empty.hold(), 0);
    assert_eq!(PreferredGateways::default().hold(), 0);
    assert!(empty.set().is_empty());
}

/// Below the ceiling, the hold is the matched gateways, and one in backoff
/// counts toward the shared hold while another available gateway may fill it.
#[test]
fn the_hold_counts_matched_gateways_and_the_set_skips_backoff() {
    let formed = PreferredGateways::form(
        [
            gateway(1, 9, GatewayStanding::Backoff),
            gateway(2, 8, GatewayStanding::Unmatched),
            gateway(3, 7, GatewayStanding::Dialable),
            gateway(4, 6, GatewayStanding::Connected),
        ],
        MAX_PREFERRED_GATEWAYS,
    );
    assert_eq!(formed.hold(), 3);
    assert_eq!(formed.set(), [node(3), node(4)]);

    let limited = PreferredGateways::form(formed.ranking.clone(), 1);
    assert_eq!(limited.hold(), 1);
    assert_eq!(limited.set(), [node(3)]);
}

/// A gateway listed twice is ranked once, so it can fill one place only.
#[test]
fn a_duplicate_gateway_is_ranked_once() {
    let formed = PreferredGateways::form(
        [
            gateway(1, 9, GatewayStanding::Dialable),
            gateway(1, 9, GatewayStanding::Dialable),
            gateway(2, 8, GatewayStanding::Dialable),
        ],
        MAX_PREFERRED_GATEWAYS,
    );
    assert_eq!(formed.hold(), 2);
    assert_eq!(formed.set(), [node(1), node(2)]);
}

/// At the cutoff, the gateway already connected keeps its place whichever
/// order the next round lists the tied gateways in.
#[test]
fn a_tie_at_the_cutoff_is_stable_across_rounds() {
    let first = PreferredGateways::form(
        [
            gateway(1, 5, GatewayStanding::Dialable),
            gateway(2, 3, GatewayStanding::Dialable),
            gateway(3, 3, GatewayStanding::Dialable),
        ],
        2,
    );
    assert_eq!(first.set(), [node(1), node(2)]);

    // The chosen gateway connects, and the next ranking lists the tie the
    // other way round.
    let second = PreferredGateways::form(
        [
            gateway(1, 5, GatewayStanding::Dialable),
            gateway(3, 3, GatewayStanding::Dialable),
            gateway(2, 3, GatewayStanding::Connected),
        ],
        2,
    );
    assert_eq!(second.set(), [node(1), node(2)]);
}

/// A ranked caller re-forms the set as if connected: its backoff is
/// ignored, an ID v2 did not know is matched, and an outranked caller
/// leaves the preferred set unchanged.
#[test]
fn a_ranked_caller_takes_the_place_it_ranks_for() {
    let formed = PreferredGateways::form(
        [
            gateway(1, 9, GatewayStanding::Backoff),
            gateway(2, 8, GatewayStanding::Dialable),
            gateway(3, 7, GatewayStanding::Unmatched),
            gateway(4, 1, GatewayStanding::Dialable),
        ],
        2,
    );
    assert_eq!(formed.hold(), 2);
    assert_eq!(formed.set(), [node(2), node(4)]);

    let backoff = formed.with_connected(&node(1)).expect("ranked first");
    assert_eq!(backoff.set(), [node(1), node(2)]);

    let unmatched = PreferredGateways::form(formed.ranking.clone(), 3)
        .with_connected(&node(3))
        .expect("ranked above a member");
    assert_eq!(unmatched.hold(), 3);
    assert_eq!(unmatched.set(), [node(2), node(3), node(4)]);

    let member = formed.with_connected(&node(2)).expect("member connected");
    assert_eq!(member.set(), formed.set());
    assert_eq!(member.with_connected(&node(2)), None, "already connected");
    assert_eq!(formed.with_connected(&node(9)), None, "not ranked");
    let full = PreferredGateways::form(
        [
            gateway(1, 9, GatewayStanding::Connected),
            gateway(2, 8, GatewayStanding::Connected),
            gateway(3, 1, GatewayStanding::Backoff),
        ],
        2,
    );
    let outranked = full.with_connected(&node(3)).expect("caller connected");
    assert_eq!(outranked.set(), full.set());
}

fn standing() -> impl Strategy<Value = GatewayStanding> {
    prop_oneof![
        Just(GatewayStanding::Unmatched),
        Just(GatewayStanding::Backoff),
        Just(GatewayStanding::Dialable),
        Just(GatewayStanding::Dialing),
        Just(GatewayStanding::Connected),
    ]
}

proptest! {
    /// The hold is the gateways that count toward it, up to the ceiling. The
    /// set is the available nodes among the first `hold` places in rank
    /// order, where an available gateway takes a place and an always-preferred
    /// ID that is not ambiguous takes one whether or not it is available.
    /// Without always-preferred IDs, that is the first `hold` available
    /// gateways, and the hold is at most the matched ones.
    #[test]
    fn the_set_is_a_prefix_of_the_available_gateways(
        entries in proptest::collection::vec(
            (0u8..40, 0u32..4, standing(), proptest::bool::weighted(0.2)),
            0..40,
        ),
        hold_limit in 0usize..16,
    ) {
        let ranking: Vec<_> = entries
            .iter()
            .map(|&(seed, blocks, standing, always)| {
                let ranked = gateway(seed, blocks, standing);
                if always {
                    RankedGateway {
                        always_preferred: true,
                        ..ranked
                    }
                } else {
                    ranked
                }
            })
            .collect();
        let any_always_preferred = ranking.iter().any(RankedGateway::is_always_preferred);
        let formed = PreferredGateways::form(ranking, hold_limit);

        let counted = formed.ranking.iter().filter(|g| g.counts_toward_hold()).count();
        prop_assert_eq!(formed.hold(), hold_limit.min(MAX_PREFERRED_GATEWAYS).min(counted));
        prop_assert!(formed.set().len() <= formed.hold());

        let mut places = 0;
        let mut expected = Vec::new();
        for gateway in &formed.ranking {
            if places == formed.hold() {
                break;
            }
            let node = gateway.available_node();
            if node.is_some() || gateway.keeps_its_place() {
                places += 1;
            }
            expected.extend(node);
        }
        prop_assert_eq!(formed.set(), &expected[..]);

        if !any_always_preferred {
            let matched = formed.ranking.iter().filter(|g| g.is_matched()).count();
            prop_assert!(formed.hold() <= matched);
            let available: Vec<NodeId> = formed
                .ranking
                .iter()
                .filter_map(RankedGateway::available_node)
                .collect();
            prop_assert_eq!(formed.set(), &available[..formed.hold().min(available.len())]);
        }
        prop_assert!(formed
            .ranking
            .windows(2)
            .all(|pair| pair[0].rank() >= pair[1].rank()));
    }
}

/// A ranking longer than the bound keeps its highest counts.
#[test]
fn a_long_ranking_keeps_its_highest_counts() {
    let ranking = (0..300u32).map(|index| {
        RankedGateway::resolved(
            NodeSecretKey::generate().public(),
            index,
            GatewayStanding::Dialable,
        )
    });
    let formed = PreferredGateways::form(ranking, MAX_PREFERRED_GATEWAYS);
    assert_eq!(formed.ranking.len(), MAX_RANKED_GATEWAYS);
    assert_eq!(
        formed.ranking.last().map(|gateway| gateway.blocks()),
        Some(44)
    );
    assert_eq!(formed.hold(), MAX_PREFERRED_GATEWAYS);
}

proptest! {
    /// A connection gives the same ranking as updating the standing and publishing again.
    #[test]
    fn a_caller_gets_what_re_forming_gives(
        entries in proptest::collection::vec((0u8..24, 0u32..4, standing()), 0..24),
        hold_limit in 0usize..4,
        caller in 0u8..24,
    ) {
        let ranking: Vec<_> = entries
            .iter()
            .map(|&(seed, blocks, standing)| gateway(seed, blocks, standing))
            .collect();
        let formed = PreferredGateways::form(ranking, hold_limit);
        let caller = node(caller);
        let mut ranking = formed.ranking.clone();
        if let Some(entry) = ranking.iter_mut().find(|entry| entry.node() == Some(caller)) {
            *entry = entry.with_standing(GatewayStanding::Connected);
        }
        let expected = PreferredGateways::form(ranking, hold_limit);
        let actual = formed.with_connected(&caller).unwrap_or(formed);
        prop_assert_eq!(actual, expected);
    }
}

/// A node that connects with a ranked gateway ID nobody knew yet fills the
/// gateway's place: the gateway now names it.
#[test]
fn a_connecting_node_resolves_its_gateway_id() {
    let caller = raw_node(1, 0x00);
    let formed = PreferredGateways::form([unresolved(caller, 5)], 1);
    assert_eq!(formed.hold(), 0);

    let connected = formed
        .with_connected(&caller)
        .expect("the caller's gateway ID is ranked");

    assert_eq!(connected.set(), &[caller]);
    assert_eq!(connected.hold(), 1);
    assert_eq!(
        connected
            .places()
            .map(|(gateway, place)| (gateway.node(), place))
            .collect::<Vec<_>>(),
        vec![(Some(caller), GatewayPlace::Preferred)]
    );
}

/// A node whose ID starts with no ranked gateway ID changes nothing.
#[test]
fn a_node_with_another_gateway_id_is_ignored() {
    let member = raw_node(1, 0x00);
    let formed = PreferredGateways::form(
        [RankedGateway::resolved(
            member,
            5,
            GatewayStanding::Connected,
        )],
        1,
    );

    assert_eq!(formed.with_connected(&raw_node(2, 0x00)), None);
}

/// A second node with a gateway's ID makes the gateway ambiguous: the member
/// it named leaves the set, and the gateway holds no place.
#[test]
fn a_second_node_with_one_gateway_id_fails_closed() {
    let member = raw_node(1, 0x00);
    let sharing = raw_node(1, 0xff);
    let formed = PreferredGateways::form(
        [RankedGateway::resolved(
            member,
            5,
            GatewayStanding::Connected,
        )],
        1,
    );
    assert_eq!(formed.set(), &[member]);

    let shared = formed
        .with_connected(&sharing)
        .expect("a second node with the gateway ID changes the gateway");

    assert!(shared.set().is_empty());
    assert_eq!(shared.hold(), 0);
    assert_eq!(
        shared
            .places()
            .map(|(gateway, place)| (gateway.node(), place))
            .collect::<Vec<_>>(),
        vec![(None, GatewayPlace::Ambiguous)]
    );
    assert_eq!(
        shared.with_connected(&member),
        None,
        "an ambiguous gateway stays ambiguous"
    );
}

/// Only a proved connection resolves a gateway: a node that keeps an
/// unproved outbound slot changes no gateway that does not already name it.
#[test]
fn an_unproved_dial_resolves_no_gateway() {
    let dialed = raw_node(1, 0x00);
    let sharing = raw_node(1, 0xff);

    let unknown = PreferredGateways::form([unresolved(dialed, 5)], 1);
    assert_eq!(unknown.with_dialing(&dialed), None);

    let named = PreferredGateways::form(
        [RankedGateway::resolved(
            dialed,
            5,
            GatewayStanding::Connected,
        )],
        1,
    );
    assert_eq!(named.with_dialing(&sharing), None);

    let dialing = named
        .with_dialing(&dialed)
        .expect("the gateway already names the dialed node");
    assert_eq!(
        dialing
            .places()
            .map(|(gateway, _)| gateway.resolution())
            .collect::<Vec<_>>(),
        vec![GatewayMatch::One((dialed, GatewayStanding::Dialing))]
    );
}

/// The ID the node always prefers for `node`, before any node is known.
fn always_preferred(node: NodeId) -> RankedGateway {
    RankedGateway::always_preferred(GatewayId::from_node_id(node.as_bytes()), 0)
}

/// An always-preferred ID ranks ahead of every gateway the blocks name, and
/// keeps its place while v2 networking cannot reach it: a lower gateway does
/// not fill it, and the ID takes it as soon as it calls in.
#[test]
fn an_unreachable_always_preferred_id_keeps_its_place() {
    let configured = raw_node(1, 0x00);
    let formed = PreferredGateways::form(
        [
            gateway(2, 50, GatewayStanding::Dialable),
            gateway(3, 40, GatewayStanding::Dialable),
            always_preferred(configured),
        ],
        2,
    );

    assert_eq!(formed.hold(), 2);
    assert_eq!(
        formed.set(),
        &[node(2)],
        "the second place waits for the always-preferred ID"
    );
    assert_eq!(
        formed
            .places()
            .map(|(gateway, place)| (gateway.is_always_preferred(), place))
            .collect::<Vec<_>>(),
        vec![
            (true, GatewayPlace::Unmatched),
            (false, GatewayPlace::Preferred),
            (false, GatewayPlace::Standby),
        ]
    );

    let connected = formed
        .with_connected(&configured)
        .expect("the caller's gateway ID is ranked");
    assert_eq!(connected.set(), &[configured, node(2)]);
    assert_eq!(connected.hold(), 2);
}

/// Unreachable always-preferred IDs cost one place each, and the gateways the
/// blocks name fill the rest.
#[test]
fn always_preferred_ids_take_one_place_each_and_gateways_fill_the_rest() {
    let formed = PreferredGateways::form(
        [
            always_preferred(raw_node(1, 0x00)),
            always_preferred(raw_node(2, 0x00)),
            gateway(3, 50, GatewayStanding::Connected),
            gateway(4, 40, GatewayStanding::Dialable),
            gateway(5, 30, GatewayStanding::Dialable),
        ],
        3,
    );

    assert_eq!(formed.hold(), 3);
    assert_eq!(formed.set(), &[node(3)]);
}

/// An always-preferred ID that two known nodes share fails closed like any
/// other gateway: it holds no place.
#[test]
fn an_ambiguous_always_preferred_id_holds_nothing() {
    let configured = raw_node(1, 0x00);
    let formed = PreferredGateways::form([always_preferred(configured)], 1)
        .with_connected(&configured)
        .expect("the caller's gateway ID is ranked")
        .with_connected(&raw_node(1, 0xff))
        .expect("a second node with the gateway ID changes the gateway");

    assert!(formed.set().is_empty());
    assert_eq!(formed.hold(), 0);
}

/// An always-preferred ID that disconnects goes into backoff but keeps its
/// place: the higher gateway keeps the other place, and the lower dialable
/// gateway stays on standby rather than taking the waiting one.
#[test]
fn an_always_preferred_id_that_disconnects_keeps_its_place() {
    let configured = raw_node(1, 0x00);
    let formed = PreferredGateways::form(
        [
            always_preferred(configured),
            gateway(2, 50, GatewayStanding::Dialable),
            gateway(3, 40, GatewayStanding::Dialable),
        ],
        2,
    )
    .with_connected(&configured)
    .expect("the caller's gateway ID is ranked");
    assert_eq!(formed.set(), &[configured, node(2)]);

    let released = formed
        .with_released(&configured)
        .expect("the connected ID loses its last slot");

    assert_eq!(released.set(), &[node(2)]);
    assert_eq!(released.hold(), 2);
    assert_eq!(
        released
            .places()
            .map(|(gateway, place)| (gateway.node(), place))
            .collect::<Vec<_>>(),
        vec![
            (Some(configured), GatewayPlace::Backoff),
            (Some(node(2)), GatewayPlace::Preferred),
            (Some(node(3)), GatewayPlace::Standby),
        ]
    );
}

/// With fewer places than always-preferred IDs, the IDs take every place,
/// and while they are unreachable the set stays empty.
#[test]
fn always_preferred_ids_can_take_every_place() {
    let formed = PreferredGateways::form(
        [
            always_preferred(raw_node(1, 0x00)),
            always_preferred(raw_node(2, 0x00)),
            gateway(3, 50, GatewayStanding::Connected),
        ],
        1,
    );

    assert_eq!(formed.hold(), 1);
    assert!(formed.set().is_empty());
}
