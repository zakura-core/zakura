//! Machine tests for the pure fill planners.
//!
//! The proptest runs one attempt's selection, retry filter, and funding in the
//! production order against a simple work-queue environment. Another routine
//! can take the probed floor height between the probe and the take. An
//! independent model checks each decision. It never calls the planners or the
//! admission helpers they use.
//!
//! Production requests one block at a time (`BS_BLOCKS_PER_RESPONSE`), so a
//! production take holds one item. The tests prefixed `multi_item_` exercise
//! planner behavior for longer takes, which the wire protocol permits but no
//! current production path reaches. They do not claim production reachability.

use std::collections::BTreeMap;

use proptest::prelude::*;

use super::*;
use crate::zakura::{
    block_sync::{
        config::{BS_BLOCKS_PER_RESPONSE, MIN_BS_CHECKPOINT_SUBMITTED_BLOCK_APPLIES},
        request::BlockSizeEstimate,
        test_work_scope,
        work_queue::WorkQueue,
    },
    transport::ByteBudget,
};

fn reference_first_allowed_run(allowed: &[bool]) -> Option<std::ops::Range<usize>> {
    let start = allowed.iter().position(|allowed| *allowed)?;
    let len = allowed[start..]
        .iter()
        .take_while(|allowed| **allowed)
        .count();
    Some(start..start + len)
}

#[test]
fn retry_filter_retains_each_small_allowed_run() {
    for len in 0..=6 {
        for mask in 0..(1usize << len) {
            let allowed: Vec<_> = (0..len)
                .map(|index| mask & (1usize << index) != 0)
                .collect();
            let expected = reference_first_allowed_run(&allowed);
            let items: Vec<_> = (0..len).collect();
            let mut calls = vec![0usize; len];

            let actual = first_allowed_run(&items, |item| {
                calls[*item] += 1;
                allowed[*item]
            });

            assert_eq!(actual, expected, "len={len}, mask={mask:#08b}");

            let visited_len = match &expected {
                Some(range) if range.end < len => range.end + 1,
                Some(range) => range.end,
                None => len,
            };
            for (index, calls) in calls.into_iter().enumerate() {
                assert_eq!(
                    calls,
                    usize::from(index < visited_len),
                    "len={len}, mask={mask:#08b}, index={index}"
                );
            }
        }
    }
}

/// Highest raw height any generated input touches.
const MAX_RAW: u32 = 12;

/// The first height above the commit window when the verified tip is zero.
fn above_commit_window() -> u32 {
    u32::try_from(MIN_BS_CHECKPOINT_SUBMITTED_BLOCK_APPLIES).unwrap() + 1
}

/// One attempt's inputs. Raw heights are offsets from `base`.
#[derive(Clone, Debug)]
struct Scenario {
    /// Whether every height sits above the commit window, so the resident
    /// look-ahead gate applies.
    above_window: bool,
    floor: u32,
    servable_low: u32,
    servable_high: u32,
    /// Pending heights and their size estimates.
    pending: BTreeMap<u32, u64>,
    floor_bypass_only: bool,
    max_count: usize,
    response_byte_cap: u64,
    floor_cwnd_bytes: Option<u64>,
    above_floor_cwnd_bytes: Option<u64>,
    budget_available: u64,
    lookahead_full: bool,
    /// Whether a preferred carrier should serve the probed floor height.
    floor_deferred: bool,
    /// Whether another routine takes the probed floor height before the take.
    floor_raced: bool,
    /// Retry facts by raw height.
    facts: BTreeMap<u32, RetryFact>,
}

impl Scenario {
    fn base(&self) -> u32 {
        if self.above_window {
            above_commit_window()
        } else {
            0
        }
    }

    fn height(&self, raw: u32) -> block::Height {
        block::Height(self.base() + raw)
    }

    fn raw(&self, height: block::Height) -> u32 {
        height.0 - self.base()
    }

    fn limits(&self) -> FillLimits {
        FillLimits {
            servable_low: self.height(self.servable_low),
            servable_high: self.height(self.servable_high),
            floor_bypass_only: self.floor_bypass_only,
            max_count: self.max_count,
            response_byte_cap: self.response_byte_cap,
            floor_cwnd_bytes: self.floor_cwnd_bytes,
            above_floor_cwnd_bytes: self.above_floor_cwnd_bytes,
        }
    }

    fn snapshot(&self) -> AdmissionSnapshot {
        AdmissionSnapshot {
            download_floor: self.height(self.floor),
            verified_block_tip: block::Height(0),
            reorder_buffered_bytes: 0,
            reorder_buffered_blocks: 0,
            applying_buffered_bytes: 0,
            applying_buffered_blocks: 0,
            sequencer_input_queued_bytes: 0,
            in_flight_submission_bytes: 0,
            decoded_resident_bytes: if self.lookahead_full { u64::MAX } else { 0 },
            reserved_above_floor_bytes: 0,
            reserved_above_floor_blocks: 0,
            budget_available: self.budget_available,
        }
    }
}

fn retry_fact() -> impl Strategy<Value = RetryFact> {
    prop_oneof![
        3 => Just(RetryFact::Allowed),
        1 => Just(RetryFact::ProtocolBlocked),
        1 => Just(RetryFact::Avoided),
    ]
}

fn scenario() -> impl Strategy<Value = Scenario> {
    let shape = (
        any::<bool>(),
        0..=MAX_RAW,
        0..=MAX_RAW,
        0..=MAX_RAW,
        prop::collection::btree_map(0..=MAX_RAW, 1u64..=4_000, 0..=8),
        any::<bool>(),
        1usize..=6,
        1u64..=8_000,
    );
    let capacity = (
        prop::option::of(0u64..=8_000),
        prop::option::of(0u64..=8_000),
        prop_oneof![1 => Just(0u64), 4 => 1u64..=8_000],
        any::<bool>(),
        any::<bool>(),
        any::<bool>(),
        prop::collection::btree_map(0..=MAX_RAW, retry_fact(), 0..=MAX_RAW as usize),
    );
    (shape, capacity).prop_map(
        |(
            (above_window, floor, low, high, pending, floor_bypass_only, max_count, cap),
            (floor_cwnd, above_cwnd, budget, lookahead_full, deferred, raced, facts),
        )| Scenario {
            above_window,
            floor,
            servable_low: low.min(high),
            servable_high: low.max(high),
            pending,
            floor_bypass_only,
            max_count,
            response_byte_cap: cap,
            floor_cwnd_bytes: floor_cwnd,
            above_floor_cwnd_bytes: above_cwnd,
            budget_available: budget,
            lookahead_full,
            floor_deferred: deferred,
            floor_raced: raced,
            facts,
        },
    )
}

/// The work-queue environment: pending heights and their estimates.
struct Queue(BTreeMap<u32, u64>);

impl Queue {
    fn first_pending(&self, low: u32, high: u32) -> Option<u32> {
        (low <= high)
            .then(|| self.0.range(low..=high).next().map(|(raw, _)| *raw))
            .flatten()
    }

    /// A contiguous ascending take that always admits its first item.
    fn take(&mut self, scenario: &Scenario, plan: TakePlan) -> Vec<(u32, u64)> {
        let (low, high) = (scenario.raw(plan.low), scenario.raw(plan.high));
        let mut taken: Vec<(u32, u64)> = Vec::new();
        let mut bytes = 0u64;
        for (raw, estimate) in self.0.range(low..=high) {
            if taken.last().is_some_and(|(last, _)| raw - last != 1)
                || taken.len() >= plan.max_count
                || (!taken.is_empty() && bytes + estimate > plan.max_bytes)
            {
                break;
            }
            bytes += estimate;
            taken.push((*raw, *estimate));
        }
        for (raw, _) in &taken {
            self.0.remove(raw);
        }
        taken
    }
}

/// Which lane produced the take.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Lane {
    Floor,
    AboveFloor,
}

/// One attempt through selection, retry filtering, and funding.
#[derive(Clone, Debug)]
struct Attempt {
    floor_plan: Result<Option<TakePlan>, FillStop>,
    above_floor_plan: Option<Result<TakePlan, FillStop>>,
    above_floor_start: Option<u32>,
    selected: Result<(Lane, Vec<(u32, u64)>), FillStop>,
    retry: Option<RetryRun>,
    visits: Vec<u32>,
    funding: Option<Funded>,
}

/// What the real queue charged a retained run.
#[derive(Copy, Clone, Debug)]
struct Funded {
    priority: RequestPriority,
    reserved_bytes: u64,
    /// Whether the run would be funded by an already exhausted budget.
    overdrafts: bool,
}

/// Take `kept` from a real queue and admit it against `budget_bytes`.
fn admit_kept(
    download_floor: block::Height,
    kept: &[(block::Height, u64)],
    budget_bytes: u64,
) -> Option<Funded> {
    let queue = std::sync::Arc::new(WorkQueue::new(
        block::Height(0),
        ByteBudget::new(budget_bytes),
    ));
    queue.set_estimate_floor_for_tests(1);
    // The queue owns only heights above genesis, and raw scenario heights can be
    // zero. Shifting the floor and the run together keeps their priority.
    let shifted = |height: block::Height| block::Height(height.0 + 1);
    queue.extend(
        test_work_scope(),
        kept.iter().map(|(height, estimate)| {
            (
                shifted(*height),
                block::Hash([7; 32]),
                BlockSizeEstimate::Confirmed(u32::try_from(*estimate).unwrap()),
            )
        }),
    );
    let (low, high) = (shifted(kept.first()?.0), shifted(kept.last()?.0));
    let request_id = std::num::NonZeroU64::MIN;
    let take = queue.take_request(low, high, kept.len(), u64::MAX, 1, request_id);
    let cancel = zakura_rt::sync::CancellationToken::new();
    let funded = take.admit(shifted(download_floor), cancel).ok()?;
    Some(Funded {
        priority: funded.priority(),
        reserved_bytes: funded.bytes(),
        overdrafts: false,
    })
}

/// Fund a retained run the way `PeerRoutine::fund` does.
fn fund_retained(download_floor: block::Height, kept: &[(block::Height, u64)]) -> Funded {
    let funded = admit_kept(download_floor, kept, u64::MAX / 2)
        .expect("a run with an ample budget always funds");
    Funded {
        overdrafts: admit_kept(download_floor, kept, 0).is_some(),
        ..funded
    }
}

/// Run the planners in the order `PeerRoutine::select_work`, `filter_retry`,
/// and `try_issue_one` call them.
fn attempt(config: &ZakuraBlockSyncConfig, scenario: &Scenario) -> Attempt {
    let limits = scenario.limits();
    let snapshot = scenario.snapshot();
    let mut queue = Queue(scenario.pending.clone());
    let probe = queue.first_pending(
        scenario.servable_low,
        scenario.servable_high.min(scenario.floor + 1),
    );
    let floor_start = probe
        .filter(|_| !scenario.floor_deferred)
        .map(|raw| scenario.height(raw));
    if let (Some(raw), true) = (probe, scenario.floor_raced) {
        queue.0.remove(&raw);
    }

    let mut attempt = Attempt {
        floor_plan: plan_floor(config, &limits, snapshot, floor_start),
        above_floor_plan: None,
        above_floor_start: None,
        selected: Err(FillStop::NoWork),
        retry: None,
        visits: Vec::new(),
        funding: None,
    };
    attempt.selected = (|| {
        if let Some(plan) = attempt.floor_plan? {
            let items = queue.take(scenario, plan);
            if !items.is_empty() {
                return Ok((Lane::Floor, items));
            }
        }
        limits.above_floor_slot()?;
        attempt.above_floor_start =
            queue.first_pending(scenario.servable_low, scenario.servable_high);
        let start = attempt.above_floor_start.map(|raw| scenario.height(raw));
        let plan = plan_above_floor(config, &limits, snapshot, start);
        attempt.above_floor_plan = Some(plan);
        let items = queue.take(scenario, plan?);
        if items.is_empty() {
            return Err(FillStop::NoWork);
        }
        Ok((Lane::AboveFloor, items))
    })();

    let Ok((_, items)) = &attempt.selected else {
        return attempt;
    };
    let run = select_retry_run(items, |(raw, _)| {
        attempt.visits.push(*raw);
        scenario
            .facts
            .get(raw)
            .copied()
            .unwrap_or(RetryFact::Allowed)
    });
    if let Some(keep) = run.keep.clone() {
        let kept = &items[keep];
        let kept: Vec<_> = kept
            .iter()
            .map(|(raw, estimate)| (scenario.height(*raw), *estimate))
            .collect();
        attempt.funding = Some(fund_retained(scenario.height(scenario.floor), &kept));
    }
    attempt.retry = Some(run);
    attempt
}

/// Check one attempt against the model.
fn check(scenario: &Scenario, attempt: &Attempt) {
    let floor_rescue_high = scenario.floor + 1;
    // Lookahead refusal needs a start above the commit window and a full gate.
    let gated = scenario.above_window && scenario.lookahead_full;
    let min_cap = |cwnd: Option<u64>| {
        scenario
            .response_byte_cap
            .min(scenario.budget_available)
            .min(cwnd.unwrap_or(u64::MAX))
    };

    // The floor lane plans only for an undeferred floor height this peer can reach.
    let floor_high = scenario.servable_high.min(floor_rescue_high);
    let floor_start = (scenario.servable_low <= floor_high && !scenario.floor_deferred)
        .then(|| {
            scenario
                .pending
                .range(scenario.servable_low..=floor_high)
                .next()
                .map(|(raw, _)| *raw)
        })
        .flatten();
    match (&attempt.floor_plan, floor_start) {
        (Ok(None), None) => {}
        (Ok(None), Some(_)) => panic!("an undeferred floor height must be planned"),
        (Ok(Some(_)) | Err(_), None) => panic!("no floor height may be planned"),
        (Ok(Some(plan)), Some(start)) => {
            assert_eq!(plan.low, scenario.height(scenario.servable_low));
            assert!(scenario.height(start) <= plan.high);
            assert!(plan.high <= scenario.height(scenario.servable_high));
            assert_eq!(plan.max_count, scenario.max_count);
            assert!(plan.max_bytes >= 1, "a floor take always funds one body");
            assert!(plan.max_bytes <= min_cap(scenario.floor_cwnd_bytes).max(1));
        }
        (Err(stop), Some(_)) => {
            assert_eq!(*stop, FillStop::LookaheadCap);
            assert!(gated, "the commit window always admits floor work");
        }
    }

    // The above-floor lane never runs on a floor-bypass slot, and never takes
    // a floor-priority start.
    if let Some(plan) = attempt.above_floor_plan {
        assert!(
            !scenario.floor_bypass_only,
            "the floor bypass took above-floor work"
        );
        let start = attempt.above_floor_start;
        match plan {
            Ok(plan) => {
                let start = start.expect("a plan has a start");
                assert!(start > floor_rescue_high);
                assert!(!gated);
                assert_eq!(plan.low, scenario.height(scenario.servable_low));
                assert!(scenario.height(start) <= plan.high);
                assert!(plan.high <= scenario.height(scenario.servable_high));
                assert!(plan.max_bytes <= min_cap(scenario.above_floor_cwnd_bytes));
            }
            Err(FillStop::NoWork) => {
                assert!(start.is_none_or(|start| start <= floor_rescue_high));
            }
            Err(FillStop::LookaheadCap) => assert!(gated),
            Err(FillStop::InflightBudget) => {
                assert!(start.is_some_and(|start| start > floor_rescue_high));
                assert_eq!(scenario.budget_available, 0);
            }
            Err(stop) => panic!("unexpected above-floor refusal {stop:?}"),
        }
    }
    match &attempt.selected {
        Ok((Lane::AboveFloor, _)) => assert!(!scenario.floor_bypass_only),
        Err(FillStop::CwndSaturated) => assert!(scenario.floor_bypass_only),
        _ => {}
    }

    let Ok((_, items)) = &attempt.selected else {
        assert!(attempt.retry.is_none() && attempt.funding.is_none());
        return;
    };
    assert!(!items.is_empty());
    assert!(
        items.windows(2).all(|pair| pair[1].0 == pair[0].0 + 1),
        "a take is one contiguous run"
    );

    // The retry filter keeps the first allowed contiguous run and reads each
    // visited item once, stopping after that run.
    let fact = |raw: &u32| {
        scenario
            .facts
            .get(raw)
            .copied()
            .unwrap_or(RetryFact::Allowed)
    };
    let first = items
        .iter()
        .position(|(raw, _)| fact(raw) == RetryFact::Allowed);
    let expected_keep = first.map(|first| {
        let len = items[first..]
            .iter()
            .take_while(|(raw, _)| fact(raw) == RetryFact::Allowed)
            .count();
        first..first + len
    });
    let visited = match &expected_keep {
        Some(keep) => (keep.end + 1).min(items.len()),
        None => items.len(),
    };
    let expected_visits: Vec<_> = items[..visited].iter().map(|(raw, _)| *raw).collect();
    let retry = attempt.retry.as_ref().expect("a selected take is filtered");
    assert_eq!(retry.keep, expected_keep);
    assert_eq!(attempt.visits, expected_visits);
    assert_eq!(
        retry.timed_rejection,
        expected_visits
            .iter()
            .any(|raw| fact(raw) == RetryFact::Avoided)
    );

    // Funding follows the retained run's first height, never the lane or the probe.
    match (&expected_keep, attempt.funding) {
        (None, None) => {}
        (Some(keep), Some(funding)) => {
            let kept = &items[keep.clone()];
            let floor_work = kept[0].0 <= floor_rescue_high;
            assert_eq!(funding.priority == RequestPriority::Floor, floor_work);
            assert_eq!(funding.overdrafts, floor_work);
            assert_eq!(
                funding.reserved_bytes,
                kept.iter().map(|(_, estimate)| estimate).sum::<u64>()
            );
        }
        (keep, funding) => panic!("kept {keep:?} but funded {funding:?}"),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2048))]

    /// Every production take: one block, because the production request cap
    /// is `BS_BLOCKS_PER_RESPONSE`.
    #[test]
    fn one_block_fill_attempt_decisions_match_an_independent_model(
        mut scenario in scenario(),
    ) {
        scenario.max_count = usize::try_from(BS_BLOCKS_PER_RESPONSE).unwrap();
        let attempt = attempt(&ZakuraBlockSyncConfig::default(), &scenario);
        check(&scenario, &attempt);
        if let Ok((_, items)) = &attempt.selected {
            prop_assert_eq!(items.len(), 1);
        }
    }

    /// Lane selection, retry filtering, and funding match an independent model,
    /// including when another routine takes the probed floor height first.
    #[test]
    fn multi_item_fill_attempt_decisions_match_an_independent_model(scenario in scenario()) {
        let config = ZakuraBlockSyncConfig::default();
        check(&scenario, &attempt(&config, &scenario));
    }

    /// The retry wake is the earliest timed avoidance, or now when a timed
    /// rejection left no deadline, bounded by the requester's retirement.
    #[test]
    fn retry_wake_is_the_earliest_exclusion_deadline(
        timed_rejection in any::<bool>(),
        offsets in prop::array::uniform4(prop::option::of(0u64..100)),
    ) {
        let now = Instant::now();
        let at = |offset: Option<u64>| offset.map(|ms| now + std::time::Duration::from_millis(ms));
        let deadlines = RetryDeadlines {
            local: at(offsets[0]),
            floor_watchdog: at(offsets[1]),
            body_retry: at(offsets[2]),
            retirement: at(offsets[3]),
        };
        let timed = offsets[..3].iter().flatten().min().copied();
        let timed = timed.or(timed_rejection.then_some(0));
        let expected = [timed, offsets[3]].into_iter().flatten().min();
        prop_assert_eq!(
            retry_wake_deadline(now, timed_rejection, deadlines),
            at(expected)
        );
    }
}

/// A scenario with floor 0, a servable range of 1..=10, and room everywhere.
fn worked_scenario(pending: &[u32]) -> Scenario {
    Scenario {
        above_window: false,
        floor: 0,
        servable_low: 1,
        servable_high: 10,
        pending: pending.iter().map(|raw| (*raw, 1_000)).collect(),
        floor_bypass_only: false,
        max_count: 4,
        response_byte_cap: 8_000,
        floor_cwnd_bytes: None,
        above_floor_cwnd_bytes: None,
        budget_available: 8_000,
        lookahead_full: false,
        floor_deferred: false,
        floor_raced: false,
        facts: BTreeMap::new(),
    }
}

/// The floor lane probes height 1, and another routine takes it before the
/// take. The floor lane's take gets 3..=5, which funds as above-floor work.
#[test]
fn multi_item_a_raced_floor_probe_funds_the_run_it_actually_took() {
    let config = ZakuraBlockSyncConfig::default();
    let mut scenario = worked_scenario(&[1, 3, 4, 5]);
    scenario.floor_raced = true;
    let attempt = attempt(&config, &scenario);
    check(&scenario, &attempt);
    let Ok((lane, items)) = &attempt.selected else {
        panic!("the floor lane takes the remaining run");
    };
    assert_eq!(*lane, Lane::Floor);
    assert_eq!(items.first().map(|(raw, _)| *raw), Some(3));
    let funding = attempt.funding.unwrap();
    assert_eq!(funding.priority, RequestPriority::AboveFloor);
    assert!(!funding.overdrafts);
}

/// The retry filter removes the floor item, so the retained run starts above
/// the floor-rescue window and funds as above-floor work.
#[test]
fn multi_item_a_retry_filter_that_removes_the_floor_item_funds_above_floor() {
    let config = ZakuraBlockSyncConfig::default();
    let mut scenario = worked_scenario(&[1, 2, 3, 4]);
    scenario.facts = [(1, RetryFact::Avoided), (2, RetryFact::ProtocolBlocked)].into();
    let attempt = attempt(&config, &scenario);
    check(&scenario, &attempt);
    let retry = attempt.retry.as_ref().unwrap();
    assert_eq!(retry.keep, Some(2..4));
    assert!(retry.timed_rejection);
    let funding = attempt.funding.unwrap();
    assert_eq!(funding.priority, RequestPriority::AboveFloor);
    assert_eq!(funding.reserved_bytes, 2_000);
}

/// A floor-bypass slot cannot fall back to above-floor work when the floor
/// lane defers to a preferred carrier.
#[test]
fn a_floor_bypass_slot_never_takes_above_floor_work() {
    let config = ZakuraBlockSyncConfig::default();
    let mut scenario = worked_scenario(&[1, 2, 3, 4]);
    scenario.floor_bypass_only = true;
    scenario.floor_deferred = true;
    let attempt = attempt(&config, &scenario);
    check(&scenario, &attempt);
    assert_eq!(attempt.selected, Err(FillStop::CwndSaturated));
    assert!(attempt.above_floor_plan.is_none());
}
