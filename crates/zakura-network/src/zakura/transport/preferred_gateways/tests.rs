use proptest::prelude::*;
use zakura_quic::NodeSecretKey;

use super::*;

fn node(seed: u8) -> NodeId {
    NodeSecretKey::from_bytes(&[seed; 32]).public()
}

fn gateway(seed: u8, blocks: u32, standing: GatewayStanding) -> RankedGateway {
    RankedGateway {
        node: node(seed),
        blocks,
        standing,
    }
}

/// The hold fits beside the bootstrap places in the dial quarter of the
/// limit, and is at most ten.
#[test]
fn the_hold_limit_fits_in_the_dial_quarter() {
    for (max_connections, headroom, hold) in [
        (92, 9, 10),
        (75, 9, 9),
        (40, 9, 1),
        (36, 9, 0),
        (20, 4, 1),
        (16, 4, 0),
        (0, 0, 0),
        (1, 0, 0),
    ] {
        assert_eq!(
            preferred_hold_limit(max_connections, headroom),
            hold,
            "{max_connections} connections, {headroom} bootstrap places"
        );
    }
}

#[test]
fn an_empty_ranking_holds_nothing() {
    let empty = PreferredGateways::form([], MAX_PREFERRED_GATEWAYS);
    assert_eq!(empty.hold(), 0);
    assert_eq!(PreferredGateways::default().hold(), 0);
    assert!(empty.set().is_empty());
}

/// Below the ceiling, the hold is the matched gateways, and one in backoff
/// keeps its place without filling it.
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
    /// The set is the first `hold` available gateways in rank order, and
    /// the hold never passes the ceiling, ten or the matched gateways.
    #[test]
    fn the_set_is_a_prefix_of_the_available_gateways(
        entries in proptest::collection::vec((0u8..40, 0u32..4, standing()), 0..40),
        hold_limit in 0usize..16,
    ) {
        let ranking: Vec<_> = entries
            .iter()
            .map(|&(seed, blocks, standing)| gateway(seed, blocks, standing))
            .collect();
        let formed = PreferredGateways::form(ranking, hold_limit);
        let matched = formed.ranking.iter().filter(|g| g.standing.matched()).count();
        prop_assert!(formed.hold() <= hold_limit.min(MAX_PREFERRED_GATEWAYS));
        prop_assert!(formed.hold() <= matched);
        prop_assert!(formed.set().len() <= formed.hold());
        let available: Vec<NodeId> = formed
            .ranking
            .iter()
            .filter(|g| g.standing.available())
            .map(|g| g.node)
            .collect();
        prop_assert_eq!(formed.set(), &available[..formed.hold().min(available.len())]);
        prop_assert!(formed
            .ranking
            .windows(2)
            .all(|pair| pair[0].blocks >= pair[1].blocks));
    }
}

/// A ranking longer than the bound keeps its highest counts.
#[test]
fn a_long_ranking_keeps_its_highest_counts() {
    let ranking = (0..300u32).map(|index| RankedGateway {
        node: NodeSecretKey::generate().public(),
        blocks: index,
        standing: GatewayStanding::Dialable,
    });
    let formed = PreferredGateways::form(ranking, MAX_PREFERRED_GATEWAYS);
    assert_eq!(formed.ranking.len(), MAX_RANKED_GATEWAYS);
    assert_eq!(
        formed.ranking.last().map(|gateway| gateway.blocks),
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
        if let Some(entry) = ranking.iter_mut().find(|entry| entry.node == caller) {
            entry.standing = GatewayStanding::Connected;
        }
        let expected = PreferredGateways::form(ranking, hold_limit);
        let actual = formed.with_connected(&caller).unwrap_or(formed);
        prop_assert_eq!(actual, expected);
    }
}
