//! How the recent miner set counts, forgets and reports blocks.

use std::collections::BTreeMap;

use proptest::{collection::vec, prelude::*};

use crate::{
    block::{self, Height},
    miner_mesh::{GatewayBlocks, MinerMeshCommitment, RecentMinerSet},
};

use super::network_with_nu7;

/// A tip whose window is the 2,304 heights from 7,597 to 9,900.
const TIP: Height = Height(10_000);
const OLDEST: Height = Height(7_597);
const NEWEST: Height = Height(9_900);

/// A set on a 75-second network with its tip at [`TIP`].
fn set_at_tip() -> RecentMinerSet {
    let mut set = RecentMinerSet::new(network_with_nu7(None));
    set.set_tip(TIP, hash(0));
    set
}

fn hash(byte: u8) -> block::Hash {
    block::Hash([byte; 32])
}

fn gateway(byte: u8) -> Option<MinerMeshCommitment> {
    Some(MinerMeshCommitment::new([byte; 32]))
}

/// The gateways in `set`, as (first ID byte, blocks) in rank order.
fn ranking(set: &RecentMinerSet) -> Vec<(u8, u32)> {
    set.snapshot()
        .gateways
        .iter()
        .map(|gateway| (gateway.gateway_id[0], gateway.blocks))
        .collect()
}

#[test]
fn a_height_counts_once_however_often_it_is_read() {
    let mut set = set_at_tip();

    set.record(Height(9_000), hash(1), gateway(7));
    assert_eq!(ranking(&set), [(7, 1)]);

    set.record(Height(9_001), hash(2), gateway(7));
    assert_eq!(ranking(&set), [(7, 2)]);

    set.record(Height(9_000), hash(1), gateway(7));
    set.record(Height(9_001), hash(2), gateway(7));
    assert_eq!(ranking(&set), [(7, 2)]);
    assert_eq!(set.snapshot().checked, 2);
}

#[test]
fn a_block_that_names_no_gateway_is_checked_and_adds_nothing() {
    let mut set = set_at_tip();

    set.record(Height(9_000), hash(1), None);

    assert_eq!(ranking(&set), []);
    assert_eq!(set.snapshot().checked, 1);
}

#[test]
fn a_height_outside_the_window_is_ignored() {
    let mut without_tip = RecentMinerSet::new(network_with_nu7(None));
    without_tip.record(Height(9_000), hash(1), gateway(7));
    assert_eq!(without_tip.snapshot().checked, 0);

    let mut set = set_at_tip();
    set.record(Height(OLDEST.0 - 1), hash(1), gateway(7));
    set.record(Height(NEWEST.0 + 1), hash(2), gateway(7));
    assert_eq!(set.snapshot().checked, 0);
    assert_eq!(ranking(&set), []);

    set.record(OLDEST, hash(3), gateway(7));
    set.record(NEWEST, hash(4), gateway(7));
    assert_eq!(ranking(&set), [(7, 2)]);
}

#[test]
fn a_block_stops_counting_when_it_ages_out() {
    let mut set = set_at_tip();
    set.record(OLDEST, hash(1), gateway(7));
    set.record(Height(OLDEST.0 + 1), hash(2), gateway(8));

    set.set_tip(Height(TIP.0 + 1), hash(0));

    // Coverage is still partial, and the oldest block's count is gone.
    assert!(!set.snapshot().is_complete());
    assert_eq!(ranking(&set), [(8, 1)]);
}

#[test]
fn a_tip_that_moves_back_stops_newer_blocks_counting() {
    let mut set = set_at_tip();
    set.record(NEWEST, hash(1), gateway(7));
    set.record(Height(NEWEST.0 - 10), hash(2), gateway(8));

    set.set_tip(Height(TIP.0 - 5), hash(0));
    assert_eq!(ranking(&set), [(8, 1)]);

    // The block does not count again until it is read again.
    set.set_tip(TIP, hash(0));
    assert_eq!(ranking(&set), [(8, 1)]);
    assert_eq!(set.unchecked_heights().next(), Some(NEWEST));
}

#[test]
fn forgetting_above_a_height_removes_the_displaced_blocks() {
    let mut set = set_at_tip();
    set.record(Height(9_000), hash(1), gateway(7));
    set.record(Height(9_100), hash(2), gateway(7));
    set.record(Height(9_200), hash(3), gateway(8));

    set.forget_above(Height(9_000));

    assert_eq!(ranking(&set), [(7, 1)]);
    assert_eq!(
        set.checked_blocks().collect::<Vec<_>>(),
        [(Height(9_000), hash(1))],
    );
    assert!(set
        .unchecked_heights()
        .any(|height| height == Height(9_100)));
}

#[test]
fn checked_blocks_are_listed_newest_first() {
    let mut set = set_at_tip();
    set.record(Height(9_100), hash(2), None);
    set.record(Height(9_000), hash(1), gateway(7));
    set.record(Height(9_200), hash(3), gateway(8));

    assert_eq!(
        set.checked_blocks().collect::<Vec<_>>(),
        [
            (Height(9_200), hash(3)),
            (Height(9_100), hash(2)),
            (Height(9_000), hash(1)),
        ],
    );
}

#[test]
fn gateways_with_equal_counts_are_ordered_from_the_first_id_byte() {
    let mut set = set_at_tip();
    // The first ID is lower in its first byte and higher in its last.
    let mut low_first_byte = [5; 32];
    low_first_byte[0] = 1;
    low_first_byte[31] = 9;
    let mut high_first_byte = [5; 32];
    high_first_byte[0] = 9;
    high_first_byte[31] = 1;

    set.record(
        Height(9_000),
        hash(1),
        Some(MinerMeshCommitment::new(high_first_byte)),
    );
    set.record(
        Height(9_001),
        hash(2),
        Some(MinerMeshCommitment::new(low_first_byte)),
    );

    let ids: Vec<[u8; 32]> = set
        .snapshot()
        .gateways
        .iter()
        .map(|gateway| gateway.gateway_id)
        .collect();
    assert_eq!(ids, [low_first_byte, high_first_byte]);
}

#[test]
fn reading_a_different_block_at_a_height_replaces_the_old_one() {
    let mut set = set_at_tip();
    set.record(Height(9_000), hash(1), gateway(7));

    set.record(Height(9_000), hash(2), gateway(8));

    assert_eq!(ranking(&set), [(8, 1)]);
    assert_eq!(
        set.checked_blocks().collect::<Vec<_>>(),
        [(Height(9_000), hash(2))],
    );
}

#[test]
fn every_gateway_is_kept_and_ranked_by_blocks_then_id() {
    let mut set = set_at_tip();
    let mut height = OLDEST.0;
    // Gateway `n` is named by `n / 2 + 1` blocks, so neighbours tie.
    for id in (0..31u8).rev() {
        for _ in 0..=id / 2 {
            set.record(Height(height), hash(id), gateway(id));
            height += 1;
        }
    }

    let gateways = set.snapshot().gateways;

    assert_eq!(gateways.len(), 31);
    let mut expected: Vec<GatewayBlocks> = (0..31u8)
        .map(|id| GatewayBlocks {
            gateway_id: [id; 32],
            blocks: u32::from(id / 2) + 1,
        })
        .collect();
    expected.sort_by_key(|gateway| (std::cmp::Reverse(gateway.blocks), gateway.gateway_id));
    assert_eq!(gateways, expected);
    assert_eq!((gateways[0].gateway_id[0], gateways[0].blocks), (30, 16));
    assert_eq!((gateways[1].gateway_id[0], gateways[1].blocks), (28, 15));
    assert_eq!((gateways[2].gateway_id[0], gateways[2].blocks), (29, 15));
}

#[test]
fn coverage_is_complete_only_when_every_height_is_read() {
    let mut set = RecentMinerSet::new(network_with_nu7(None));

    // No tip: the set has not looked at the chain yet.
    assert!(!set.snapshot().is_complete());
    assert_eq!(set.snapshot().window_len(), 0);

    // A tip below height 100 has nothing to read.
    set.set_tip(Height(99), hash(0));
    assert!(set.snapshot().is_complete());
    assert_eq!(set.snapshot().window_len(), 0);

    // The window is the six heights from genesis to 5.
    set.set_tip(Height(105), hash(0));
    for height in 0..5 {
        set.record(Height(height), hash(1), None);
    }
    let partial = set.snapshot();
    assert_eq!((partial.checked, partial.window_len()), (5, 6));
    assert!(!partial.is_complete());
    assert_eq!(partial.gateways, []);

    set.record(Height(5), hash(1), None);
    let complete = set.snapshot();
    assert!(complete.is_complete());
    assert_eq!(complete.gateways, []);
    assert_eq!(complete.tip, Some((Height(105), hash(0))));
}

#[test]
fn unread_heights_are_listed_newest_first() {
    let mut set = RecentMinerSet::new(network_with_nu7(None));
    assert_eq!(set.unchecked_heights().next(), None);

    set.set_tip(Height(105), hash(0));
    set.record(Height(4), hash(1), None);
    set.record(Height(1), hash(1), None);

    assert_eq!(
        set.unchecked_heights().collect::<Vec<_>>(),
        [Height(5), Height(3), Height(2), Height(0)],
    );
}

/// One step a set's owner can take.
#[derive(Clone, Debug)]
enum Step {
    SetTip(u32),
    Record(u32, Option<u8>),
    ForgetAbove(u32),
}

fn steps() -> impl Strategy<Value = Vec<Step>> {
    // Tips up to 3,000 make windows that both reach genesis and leave old
    // heights behind.
    let step = prop_oneof![
        (0..3_000u32).prop_map(Step::SetTip),
        (0..3_000u32, proptest::option::of(0..4u8))
            .prop_map(|(height, gateway)| Step::Record(height, gateway)),
        (0..3_000u32).prop_map(Step::ForgetAbove),
    ];
    vec(step, 0..60)
}

proptest! {
    /// After any steps, the counts equal a recount of the blocks that were
    /// read and still count.
    #[test]
    fn counts_match_a_recount_after_any_steps(steps in steps()) {
        let network = network_with_nu7(None);
        let mut set = RecentMinerSet::new(network);
        // The blocks an owner would expect to count, by height.
        let mut model: BTreeMap<u32, Option<u8>> = BTreeMap::new();
        let mut tip = None;

        for step in steps {
            match step {
                Step::SetTip(height) => {
                    set.set_tip(Height(height), hash(0));
                    tip = Some(height);
                }
                Step::Record(height, gateway_byte) => {
                    set.record(Height(height), hash(1), gateway_byte.and_then(gateway));
                    model.insert(height, gateway_byte);
                }
                Step::ForgetAbove(height) => {
                    set.forget_above(Height(height));
                    model.retain(|checked, _| *checked <= height);
                }
            }
            // A height counts from 100 below the tip back 2,304 blocks.
            let counts = |height: u32| {
                tip.and_then(|tip: u32| tip.checked_sub(100))
                    .is_some_and(|newest| height <= newest && newest - height < 2_304)
            };
            model.retain(|height, _| counts(*height));

            let mut expected: BTreeMap<u8, u32> = BTreeMap::new();
            for gateway_byte in model.values().flatten() {
                *expected.entry(*gateway_byte).or_default() += 1;
            }
            let snapshot = set.snapshot();
            let counted: BTreeMap<u8, u32> = snapshot
                .gateways
                .iter()
                .map(|gateway| (gateway.gateway_id[0], gateway.blocks))
                .collect();

            prop_assert_eq!(counted, expected);
            prop_assert_eq!(snapshot.checked, model.len());
            }
    }
}
