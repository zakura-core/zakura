//! Decoder v1 for fill filtering, funding and commit-window geometry.
//!
//! [`replay_block_fill_bytes`] is a machine target (see
//! [`machine_test`](crate::zakura::machine_test)). Each bounded case varies the
//! probed floor, the items another peer took, and retry facts.
//! [`expected_retry_run`] recomputes the retry filter's run the slow, obvious
//! way. Funding runs through the production queue's take, retained-range and
//! admission path, and the checks compare its charge with the run the test
//! retained. Takes hold up to 30 items, so this driver exercises multi-item
//! behavior that the one-block production cap does not reach.

use std::sync::Arc;

use zakura_chain::block;

use super::{
    model::{expected_retry_run, fund_through_queue, Funding},
    plan_floor, select_retry_run, FillLimits, RetryFact, RetryRun, TakePlan,
};
use crate::zakura::{
    block_sync::{
        admission::{floor_rescue_high, AdmissionSnapshot, RequestPriority},
        config::MIN_BS_CHECKPOINT_SUBMITTED_BLOCK_APPLIES,
        request::BlockSizeEstimate,
        work_queue::{WorkItem, WorkQueue},
        ZakuraBlockSyncConfig,
    },
    machine_test::MAX_HISTORY_BYTES,
    transport::ByteBudget,
};

const RETRY: &str = "block/fill-retry-run";
const FUNDING: &str = "block/fill-retained-funding";
const WINDOW: &str = "block/fill-commit-window";

/// Applicable transitions in one decoder-v1 fill replay.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct BlockFillReplaySummary {
    /// Takes checked against the retry model.
    pub takes: usize,
    /// Takes whose retry filter retained a run.
    pub retained: usize,
    /// Retained runs above the floor-rescue window.
    pub above_floor: usize,
    /// Floor grants checked at the commit-window boundary.
    pub window_grants: usize,
}

/// One case's estimate for the item selected by `byte`.
fn estimate(byte: u8) -> BlockSizeEstimate {
    BlockSizeEstimate::Confirmed(u32::from(byte) * 1024)
}

/// Fund `keep` of the items above `floor + raced` through the production queue.
/// A separate exhausted-budget queue reports whether the run may overdraft.
fn fund_retained(
    floor: u32,
    raced: u32,
    bytes: &[u8],
    keep: std::ops::Range<usize>,
    budget_bytes: u64,
) -> Option<(Vec<(block::Height, WorkItem)>, Funding)> {
    let work = Arc::new(WorkQueue::new(
        block::Height(floor),
        ByteBudget::new(budget_bytes),
    ));
    let first = floor + 1;
    let rival: Vec<_> = (first..first + raced)
        .map(|height| (block::Height(height), estimate(0)))
        .collect();
    let wanted: Vec<_> = bytes
        .iter()
        .zip(first + raced..)
        .map(|(byte, height)| (block::Height(height), estimate(*byte)))
        .collect();
    fund_through_queue(&work, block::Height(floor), &rival, &wanted, keep)
}

/// Replay at most [`MAX_HISTORY_BYTES`] bytes in cases of at most 32 bytes.
///
/// A case's first byte selects the floor. The second selects how many heights
/// another peer already took. Remaining bytes select retry facts and estimates.
/// A disagreement panics with its property ID.
pub fn replay_block_fill_bytes(data: &[u8]) -> BlockFillReplaySummary {
    replay(data, |_, _| {}, |_| {}, |_| {})
}

fn replay(
    data: &[u8],
    mut after_filter: impl FnMut(&mut RetryRun, usize),
    mut after_funding: impl FnMut(&mut Funding),
    mut after_plan: impl FnMut(&mut TakePlan),
) -> BlockFillReplaySummary {
    let mut summary = BlockFillReplaySummary::default();
    for case in data[..data.len().min(MAX_HISTORY_BYTES)].chunks(32) {
        let floor = u32::from(case[0]);
        let raced = u32::from(case.get(1).copied().unwrap_or(0) & 3);
        let facts: Vec<_> = case
            .get(2..)
            .unwrap_or_default()
            .iter()
            .map(|byte| match byte % 3 {
                0 => RetryFact::Allowed,
                1 => RetryFact::ProtocolBlocked,
                _ => RetryFact::Avoided,
            })
            .collect();
        let expected = expected_retry_run(&facts);
        let mut reads = Vec::new();
        let indexes: Vec<_> = (0..facts.len()).collect();
        let mut actual = select_retry_run(&indexes, |index| {
            reads.push(*index);
            facts[*index]
        });
        after_filter(&mut actual, facts.len());
        assert_eq!(actual.keep, expected.keep, "{RETRY}: contiguous first run");
        assert_eq!(
            actual.timed_rejection, expected.timed_rejection,
            "{RETRY}: visited avoidance"
        );
        assert_eq!(
            reads,
            (0..expected.visited).collect::<Vec<_>>(),
            "{RETRY}: each visited item once"
        );
        summary.takes += 1;
        if let Some(run) = actual.keep {
            let start =
                floor + 1 + raced + u32::try_from(run.start).expect("a case has at most 30 items");
            let (items, mut funding) =
                fund_retained(floor, raced, &case[2..], run.clone(), u64::MAX / 2)
                    .expect("an ample budget funds any retained run");
            after_funding(&mut funding);
            // Only a run that starts in the floor-rescue window is floor work,
            // wherever the probe or the rival take left the floor.
            let floor_priority = block::Height(start) <= floor_rescue_high(block::Height(floor));
            assert_eq!(
                fund_retained(floor, raced, &case[2..], run.clone(), 0).is_some(),
                floor_priority,
                "{FUNDING}: actual retained start"
            );
            assert_eq!(
                funding.priority,
                if floor_priority {
                    RequestPriority::Floor
                } else {
                    RequestPriority::AboveFloor
                },
                "{FUNDING}: priority"
            );
            assert_eq!(
                items.first().map(|(height, _)| height.0),
                Some(start),
                "{FUNDING}: retained run"
            );
            assert_eq!(items.len(), run.len(), "{FUNDING}: retained run");
            assert_eq!(
                funding.reserved_bytes,
                items
                    .iter()
                    .map(|(_, item)| item.estimated_bytes)
                    .sum::<u64>(),
                "{FUNDING}: retained estimates"
            );
            summary.retained += 1;
            summary.above_floor += usize::from(!floor_priority);
        }
        let span = u32::try_from(MIN_BS_CHECKPOINT_SUBMITTED_BLOCK_APPLIES)
            .expect("the consensus checkpoint span fits a height");
        let ceiling = floor + span;
        let limits = FillLimits {
            servable_low: block::Height(floor + 1),
            servable_high: block::Height(ceiling + 100),
            floor_bypass_only: false,
            max_count: 32,
            response_byte_cap: 4096,
            floor_cwnd_bytes: Some(2048),
            above_floor_cwnd_bytes: Some(1024),
        };
        let snapshot = AdmissionSnapshot {
            download_floor: block::Height(floor),
            verified_block_tip: block::Height(floor),
            reorder_buffered_bytes: 0,
            reorder_buffered_blocks: 0,
            applying_buffered_bytes: 0,
            applying_buffered_blocks: 0,
            sequencer_input_queued_bytes: 0,
            in_flight_submission_bytes: 0,
            decoded_resident_bytes: 0,
            reserved_above_floor_bytes: 0,
            reserved_above_floor_blocks: 0,
            budget_available: 4096,
        };
        let mut grant = plan_floor(
            &ZakuraBlockSyncConfig::default(),
            &limits,
            snapshot,
            Some(block::Height(floor + 1)),
        )
        .expect("an empty pipeline admits floor work")
        .expect("the floor exists");
        after_plan(&mut grant);
        assert_eq!(
            grant.high,
            block::Height(ceiling),
            "{WINDOW}: exempt take ceiling"
        );
        assert_eq!(grant.max_bytes, 2048, "{WINDOW}: congestion headroom");
        summary.window_grants += 1;
    }
    summary
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zakura::machine_test::machine_target_tests;

    // A peer took the floor; retry avoidance then removes another item.
    const WITNESS: &[u8] = &[10, 1, 2, 3, 6, 1, 0];

    machine_target_tests! {
        replay: replay_block_fill_bytes,
        target: "block_fill",
        decoder: 1,
        witness: WITNESS => BlockFillReplaySummary {
            takes: 1,
            retained: 1,
            above_floor: 1,
            window_grants: 1,
        },
    }

    /// A floor run that no peer raced funds as floor work.
    #[test]
    fn an_unraced_floor_run_funds_as_floor_work() {
        assert_eq!(
            replay_block_fill_bytes(&[10, 0, 255, 255]),
            BlockFillReplaySummary {
                takes: 1,
                retained: 1,
                above_floor: 0,
                window_grants: 1,
            }
        );
    }

    #[test]
    #[should_panic(expected = "block/fill-retry-run")]
    fn fill_witness_rejects_crossing_a_retry_hole() {
        replay(
            WITNESS,
            |run, len| {
                if let Some(keep) = &mut run.keep {
                    keep.end = len;
                }
            },
            |_| {},
            |_| {},
        );
    }

    #[test]
    #[should_panic(expected = "block/fill-retained-funding")]
    fn fill_witness_rejects_funding_from_the_probed_floor() {
        replay(
            WITNESS,
            |_, _| {},
            |funding| funding.priority = RequestPriority::Floor,
            |_| {},
        );
    }

    #[test]
    #[should_panic(expected = "block/fill-commit-window")]
    fn fill_witness_rejects_taking_past_the_commit_window() {
        replay(WITNESS, |_, _| {}, |_| {}, |grant| grant.high.0 += 1);
    }
}
