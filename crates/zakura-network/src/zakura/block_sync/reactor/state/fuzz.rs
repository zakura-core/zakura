//! Decoder v1 for query dispatch and completion histories.
//!
//! The model stores accepted identities independently. Refused proposals cannot
//! replace them, and completions must match both query ID and authority.

use std::{num::NonZeroU64, time::Duration};

use zakura_chain::block;
use zakura_rt::time::Instant;

use zakura_header_chain::{
    BodyWorkAuthority, BodyWorkEpoch, BranchId, HeaderGeneration, HeaderWorkAuthority,
    VerifiedGeneration,
};

use super::{ActionRefusal, QueryPlan, QueryRequest, QueryStart, QueryState, WorkFacts};

const DISPATCH: &str = "block/query-dispatch";
const COMPLETION: &str = "block/query-completion";

/// Applicable transitions in one decoder-v1 query history.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct BlockQueryReplaySummary {
    /// Proposals accepted by the action receiver.
    pub accepted: usize,
    /// Proposals refused while a previous query remained pending.
    pub refused_replacements: usize,
    /// Replies that named a different query or authority.
    pub stale_completions: usize,
    /// Replies that retired the exact pending query.
    pub matched_completions: usize,
    /// Explicit resets of pending work.
    pub resets: usize,
}

/// Replay at most 512 bytes through production query transitions.
///
/// The low three bits choose accept, refuse, succeed, fail, reset, retry expiry,
/// authority change or time advance. High bits choose the proposal's hash or a
/// previously accepted identity. A disagreement panics with its property ID.
pub fn replay_block_query_bytes(data: &[u8]) -> BlockQueryReplaySummary {
    replay(data, |_, _| {})
}

fn authority(epoch: u64) -> BodyWorkAuthority {
    BodyWorkAuthority {
        header: HeaderWorkAuthority {
            header_generation: HeaderGeneration::new(epoch + 1),
            branch: BranchId::new(block::Hash([4; 32]), block::Hash([5; 32])),
        },
        verified_generation: VerifiedGeneration::new(1),
        body_work_epoch: BodyWorkEpoch::default(),
    }
}

fn replay(data: &[u8], mut after_step: impl FnMut(&mut QueryState, u8)) -> BlockQueryReplaySummary {
    let origin = Instant::now();
    let mut now = 0u64;
    let mut state = QueryState::new();
    // (query ID, authority generation, header hash discriminator).
    let mut pending: Option<(u64, u64, u8)> = None;
    let mut history = Vec::new();
    let mut next_id = 1;
    let mut epoch = 0;
    let mut retry_at = None;
    let mut summary = BlockQueryReplaySummary::default();
    for &byte in data.iter().take(512) {
        let op = byte & 7;
        let choice = byte >> 3;
        let property = if op == 2 || op == 3 {
            COMPLETION
        } else {
            DISPATCH
        };
        match op {
            0 | 1 => {
                let proposal = state.propose(QueryRequest {
                    force: true,
                    best_header_tip: block::Height(100),
                    best_header_hash: block::Hash([choice; 32]),
                    scope: Some(authority(epoch)),
                    diverged: false,
                    start: QueryStart::Work(WorkFacts {
                        first_unclaimed: Some(block::Height(1)),
                        backlog: 0,
                        low_water: 5,
                    }),
                });
                if pending
                    .is_some_and(|(_, generation, hash)| generation == epoch && hash == choice)
                {
                    assert_eq!(
                        proposal,
                        QueryPlan::Covered,
                        "{DISPATCH}: duplicate proposal"
                    );
                } else {
                    let QueryPlan::Dispatch(proposal) = proposal else {
                        panic!("{DISPATCH}: distinct proposal was not dispatched")
                    };
                    assert_eq!(
                        proposal.query.query_id.get(),
                        next_id,
                        "{DISPATCH}: proposed ID"
                    );
                    assert_eq!(proposal.query.from, block::Height(1), "{DISPATCH}: start");
                    assert_eq!(proposal.query.limit, 10, "{DISPATCH}: take");
                    if op == 0 {
                        state.accepted(proposal);
                        pending = Some((next_id, epoch, choice));
                        history.push((next_id, epoch));
                        next_id += 1;
                        retry_at = None;
                        summary.accepted += 1;
                    } else {
                        state.refused(
                            proposal,
                            ActionRefusal::Full,
                            origin + Duration::from_millis(now),
                        );
                        retry_at = Some(now + 250);
                        summary.refused_replacements += usize::from(pending.is_some());
                    }
                }
            }
            2 | 3 => {
                let (id, generation) = if choice & 16 != 0 {
                    // A valid query ID paired with a different authority.
                    (pending.map_or(next_id, |p| p.0), epoch + 100)
                } else {
                    history
                        .get(usize::from(choice))
                        .copied()
                        .unwrap_or((next_id + 1, epoch))
                };
                let matches = pending.is_some_and(|p| p.0 == id && p.1 == generation);
                let id = NonZeroU64::new(id).expect("query identities start at one");
                let actual = if op == 2 {
                    state.complete(id, authority(generation)).is_some()
                } else {
                    state.fail(
                        id,
                        authority(generation),
                        origin + Duration::from_millis(now),
                    )
                };
                assert_eq!(actual, matches, "{COMPLETION}: exact identity");
                if matches {
                    pending = None;
                    retry_at = (op == 3).then_some(now + 250);
                    summary.matched_completions += 1;
                } else {
                    summary.stale_completions += 1;
                }
            }
            4 => {
                state.clear();
                pending = None;
                retry_at = None;
                summary.resets += 1;
            }
            5 => {
                state.retry_elapsed();
                retry_at = None;
            }
            6 => epoch += 1,
            _ => now += u64::from(choice),
        }
        after_step(&mut state, op);
        let actual = state.pending.map(|p| {
            (
                p.query_id.get(),
                p.scope.header.header_generation.get() - 1,
                p.best_header_hash.0[0],
            )
        });
        assert_eq!(actual, pending, "{property}: pending identity");
        assert_eq!(
            state.next_id.map(NonZeroU64::get),
            Some(next_id),
            "{DISPATCH}: next ID"
        );
        assert_eq!(
            state
                .retry_at
                .map(|at| at.duration_since(origin).as_millis()),
            retry_at.map(u128::from),
            "{property}: retry deadline"
        );
    }
    summary
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const WITNESS: &[u8] = &[0, 9, 8, 2, 130, 10, 16, 19, 18, 4];

    #[test]
    #[allow(
        clippy::print_stdout,
        reason = "witnesses print applicability counters"
    )]
    fn query_witness_exercises_refusal_stale_identity_and_reset() {
        println!(
            "tier=Machine target=block_query decoder=1 {:?}",
            replay_block_query_bytes(WITNESS)
        );
        assert_eq!(
            replay_block_query_bytes(WITNESS),
            BlockQueryReplaySummary {
                accepted: 3,
                refused_replacements: 1,
                stale_completions: 3,
                matched_completions: 2,
                resets: 1,
            }
        );
        assert_eq!(
            replay_block_query_bytes(&[]),
            BlockQueryReplaySummary::default()
        );
    }

    #[test]
    #[should_panic(expected = "block/query-dispatch")]
    fn query_witness_rejects_refusal_clearing_pending_work() {
        replay(WITNESS, |state, op| {
            if op == 1 {
                state.pending = None;
            }
        });
    }

    #[test]
    #[should_panic(expected = "block/query-completion")]
    fn query_witness_rejects_stale_completion_clearing_pending_work() {
        replay(WITNESS, |state, op| {
            if op == 2 {
                state.pending = None;
            }
        });
    }

    proptest! {
        #[test]
        fn query_histories_match_the_identity_ledger(bytes in prop::collection::vec(any::<u8>(), 0..=512)) {
            replay_block_query_bytes(&bytes);
        }
    }
}
