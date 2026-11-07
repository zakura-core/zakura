//! The reference shared by the fill proptests and the `block_fill` driver.
//!
//! [`expected_retry_run`] is an independent model of the retry filter: it calls
//! no planner and no admission helper. [`fund_through_queue`] is the environment
//! that charges a retained run through the production queue.

use std::{num::NonZeroU64, ops::Range, sync::Arc};

use zakura_chain::block;
use zakura_rt::sync::CancellationToken;

use super::RetryFact;
use crate::zakura::block_sync::{
    admission::RequestPriority,
    request::BlockSizeEstimate,
    test_work_scope,
    work_queue::{WorkItem, WorkQueue},
};

/// What the retry filter must do with one take.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ExpectedRetryRun {
    /// The first contiguous allowed run.
    pub(super) keep: Option<Range<usize>>,
    /// How many leading items the filter reads, once each. It stops after the run.
    pub(super) visited: usize,
    /// Whether a visited item is under a timed avoidance.
    pub(super) timed_rejection: bool,
}

pub(super) fn expected_retry_run(facts: &[RetryFact]) -> ExpectedRetryRun {
    let keep = facts
        .iter()
        .position(|fact| *fact == RetryFact::Allowed)
        .map(|start| {
            let count = facts[start..]
                .iter()
                .take_while(|fact| **fact == RetryFact::Allowed)
                .count();
            start..start + count
        });
    let visited = keep
        .as_ref()
        .map_or(facts.len(), |run| (run.end + 1).min(facts.len()));
    ExpectedRetryRun {
        keep,
        visited,
        timed_rejection: facts[..visited].contains(&RetryFact::Avoided),
    }
}

/// What queue admission charged a retained run.
#[derive(Copy, Clone, Debug)]
pub(super) struct Funding {
    pub(super) priority: RequestPriority,
    pub(super) reserved_bytes: u64,
}

/// Load `work` with `raced` and `wanted`, let a rival take `raced`, take `wanted`,
/// keep `keep`, and admit the run against `download_floor`. Returns the retained
/// items and their funding, or `None` when admission refuses.
pub(super) fn fund_through_queue(
    work: &Arc<WorkQueue>,
    download_floor: block::Height,
    raced: &[(block::Height, BlockSizeEstimate)],
    wanted: &[(block::Height, BlockSizeEstimate)],
    keep: Range<usize>,
) -> Option<(Vec<(block::Height, WorkItem)>, Funding)> {
    work.extend(
        test_work_scope(),
        raced
            .iter()
            .chain(wanted)
            .map(|(height, size)| (*height, block::Hash([7; 32]), *size)),
    );
    let id = |id| NonZeroU64::new(id).expect("request IDs start at one");
    let rival = raced
        .first()
        .zip(raced.last())
        .map(|(low, high)| work.take_request(low.0, high.0, raced.len(), u64::MAX, 1, id(1)));
    let low = raced.first().or(wanted.first())?.0;
    let high = wanted.last()?.0;
    let mut take = work.take_request(low, high, wanted.len(), u64::MAX, 2, id(2));
    take.retain_range(keep);
    let items = take.items().to_vec();
    let funded = take.admit(download_floor, CancellationToken::new()).ok()?;
    let funding = Funding {
        priority: funded.priority(),
        reserved_bytes: funded.bytes(),
    };
    drop((funded, rival));
    Some((items, funding))
}
