//! Decoder v1 for query dispatch and completion histories.
//!
//! [`replay_block_query_bytes`] is a machine target (see
//! [`machine_test`](crate::zakura::machine_test)). It drives [`QueryState`]
//! alone and checks [`QueryState::assert_invariants`] after every input. Its
//! ledger records the queries the channel accepted, so a refused proposal
//! cannot replace the pending one, and a completion must name both the
//! pending query's ID and its authority.

use std::{num::NonZeroU64, time::Duration};

use zakura_chain::block;
use zakura_rt::time::Instant;

use zakura_header_chain::{
    BodyWorkAuthority, BodyWorkEpoch, BranchId, HeaderGeneration, HeaderWorkAuthority,
    VerifiedGeneration,
};

use super::{
    ActionRefusal, QueryPlan, QueryRequest, QueryStart, QueryState, WorkFacts,
    NEEDED_BLOCK_QUERY_RETRY_DELAY,
};
use crate::zakura::machine_test::MAX_HISTORY_BYTES;

const DISPATCH: &str = "block/query-dispatch";
const COMPLETION: &str = "block/query-completion";

/// The header tip every proposal asks about.
const HEADER_TIP: u32 = 100;

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

/// Replay at most [`MAX_HISTORY_BYTES`] bytes through production query
/// transitions.
///
/// Decoder v1: the low three bits of each byte choose accept, refuse, succeed,
/// fail, reset, retry expiry, authority change or time advance. The high bits
/// choose the proposal's hash or a previously accepted identity. A broken
/// property panics with its ID.
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
    // The ledger: the pending query's (ID, authority generation, header hash
    // byte), every accepted (ID, generation), and the retry deadline.
    let mut pending: Option<(u64, u64, u8)> = None;
    let mut history = Vec::new();
    let mut next_id = 1;
    let mut epoch = 0;
    let mut retry_at: Option<u64> = None;
    let retry_delay =
        u64::try_from(NEEDED_BLOCK_QUERY_RETRY_DELAY.as_millis()).expect("a short retry delay");
    let mut summary = BlockQueryReplaySummary::default();
    for &byte in data.iter().take(MAX_HISTORY_BYTES) {
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
                    best_header_tip: block::Height(HEADER_TIP),
                    best_header_hash: block::Hash([choice; 32]),
                    scope: Some(authority(epoch)),
                    diverged: false,
                    start: QueryStart::Work(WorkFacts {
                        first_unclaimed: Some(block::Height(1)),
                        backlog: None,
                        low_water: 5,
                    }),
                });
                // The test sends the pending query's own request again.
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
                    let query = proposal.query;
                    assert_eq!(query.query_id.get(), next_id, "{DISPATCH}: proposed ID");
                    assert_eq!(
                        (query.from, query.scope, query.best_header_hash),
                        (
                            block::Height(1),
                            authority(epoch),
                            block::Hash([choice; 32])
                        ),
                        "{DISPATCH}: the request it was sent"
                    );
                    assert!(
                        (1..=HEADER_TIP).contains(&query.limit),
                        "{DISPATCH}: take {}",
                        query.limit
                    );
                    if op == 0 {
                        let _ = state.accepted(proposal);
                        pending = Some((next_id, epoch, choice));
                        history.push((next_id, epoch));
                        next_id += 1;
                        retry_at = None;
                        summary.accepted += 1;
                    } else {
                        let _ = state.refused(
                            proposal,
                            ActionRefusal::Full,
                            origin + Duration::from_millis(now),
                        );
                        retry_at = Some(now + retry_delay);
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
                    state
                        .fail(
                            id,
                            authority(generation),
                            origin + Duration::from_millis(now),
                        )
                        .retired_failed_query()
                };
                assert_eq!(actual, matches, "{COMPLETION}: exact identity");
                if matches {
                    pending = None;
                    retry_at = (op == 3).then_some(now + retry_delay);
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
                let _ = state.retry_elapsed();
                retry_at = None;
            }
            6 => epoch += 1,
            _ => now += u64::from(choice),
        }
        after_step(&mut state, op);
        state.assert_invariants();
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
    use crate::zakura::machine_test::machine_target_tests;

    const WITNESS: &[u8] = &[0, 9, 8, 2, 130, 10, 16, 19, 18, 4];

    machine_target_tests! {
        replay: replay_block_query_bytes,
        target: "block_query",
        decoder: 1,
        witness: WITNESS => BlockQueryReplaySummary {
            accepted: 3,
            refused_replacements: 1,
            stale_completions: 3,
            matched_completions: 2,
            resets: 1,
        },
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
}
