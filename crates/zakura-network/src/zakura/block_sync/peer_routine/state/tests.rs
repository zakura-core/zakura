//! The request lifecycle of [`PeerState`].
//!
//! [`replay_request_lifecycle_bytes`] is a machine target (see
//! [`machine_test`](crate::zakura::machine_test)). The harness plays the
//! routine's part: it settles writes, removes finished requests, and feeds
//! bodies, endings, expiry, and resets. A ledger records the requests the test
//! issued, the writes it started, the bodies it delivered, and the requests
//! sent since the last delivered body. After every input, the test checks
//! [`PeerState::assert_invariants`], request isolation, the next wake, and
//! fault attribution against that ledger. Named cases pin the ending rules.

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU64,
    time::Duration,
};

use zakura_chain::block;
use zakura_rt::time::Instant;

use super::{
    super::super::{
        regulated::wire::Outcome,
        request::{BlockRangeRequest, ExpectedBlock},
        state::{OutstandingBlockRange, ReceivedBlockTracker, RequestKey, WriteOutcome},
        test_work_scope, ZakuraBlockSyncConfig,
    },
    BodyMatch, Disposition, PeerState, SharedDeadlines,
};
#[cfg(test)]
use crate::zakura::machine_test::machine_target_tests;
use crate::zakura::machine_test::MAX_HISTORY_BYTES;

const GENERATION: u64 = 9;
const LIVENESS_TIMEOUT: Duration = Duration::from_secs(5);
/// Requests cover heights `1..=MAX_HEIGHT`.
const MAX_HEIGHT: u32 = 6;

#[derive(Clone, Debug)]
enum Input {
    /// Issue a request for `count` heights from `start`, due `due_ms` from now.
    Issue { start: u32, count: u32, due_ms: u64 },
    /// Transport starts writing the frame of the `pick`th live request.
    WriteStarted { pick: usize },
    /// The peer sends the body at `height`, with the requested hash or another.
    Body { height: u32, tampered: bool },
    /// The peer ends the `pick`th live request, with the download floor at `floor`.
    Ending {
        pick: usize,
        unavailable: bool,
        floor: u32,
    },
    /// A second ending for a range whose request already left the window.
    LateEnding { start: u32 },
    /// Time passes, then due requests expire.
    Expire { advance_ms: u64 },
    /// A destructive reset returns every request.
    Reset,
}

fn hash(height: u32, tampered: bool) -> block::Hash {
    let byte = u8::try_from(height).expect("test heights fit in u8");
    block::Hash([if tampered { byte | 0x80 } else { byte }; 32])
}

/// One live request as the test issued it.
#[derive(Debug)]
struct ModelRequest {
    start: u32,
    count: u32,
    deadline: Instant,
    written: bool,
    received: BTreeSet<u32>,
}

impl ModelRequest {
    fn heights(&self) -> std::ops::Range<u32> {
        self.start..self.start + self.count
    }
}

/// The ledger: what the test issued, wrote, and delivered. Requests are keyed
/// by issue sequence, which is also the request ID in their work owner.
#[derive(Debug)]
struct Model {
    now: Instant,
    requests: BTreeMap<u64, ModelRequest>,
    next_sequence: u64,
    /// Requests sent since the last delivered body, less those whose frame
    /// never reached the wire.
    streak: u32,
    /// Live requests sent since the last delivered body.
    charged: BTreeSet<u64>,
}

impl Model {
    fn new(now: Instant) -> Self {
        Self {
            now,
            requests: BTreeMap::new(),
            next_sequence: 1,
            streak: 0,
            charged: BTreeSet::new(),
        }
    }

    fn covering(&self, height: u32) -> Option<u64> {
        self.requests
            .iter()
            .find(|(_, request)| request.heights().contains(&height))
            .map(|(sequence, _)| *sequence)
    }

    fn pick(&self, pick: usize, written: bool) -> Option<u64> {
        let live: Vec<_> = self
            .requests
            .iter()
            .filter(|(_, request)| !written || request.written)
            .map(|(sequence, _)| *sequence)
            .collect();
        (!live.is_empty()).then(|| live[pick % live.len()])
    }

    fn remove(&mut self, sequence: u64) -> ModelRequest {
        self.charged.remove(&sequence);
        self.requests
            .remove(&sequence)
            .expect("the ledger removes only live requests")
    }

    /// A delivered body proves progress: no sent request counts toward a stall.
    fn progress(&mut self) {
        self.streak = 0;
        self.charged.clear();
    }
}

/// The routine's side of the test: the state, the live write handles, and
/// the keys the window returned.
struct Harness {
    state: PeerState,
    keys: BTreeMap<u64, RequestKey>,
    written: BTreeSet<u64>,
}

impl Harness {
    fn new() -> Self {
        Self {
            state: PeerState::new(&ZakuraBlockSyncConfig::default(), true, 0, 0),
            keys: BTreeMap::new(),
            written: BTreeSet::new(),
        }
    }

    fn owner(sequence: u64) -> zakura_header_chain::BodyWorkOwner {
        test_work_scope().bind(
            GENERATION,
            NonZeroU64::new(sequence).expect("sequences start at one"),
        )
    }

    fn sequence(&self, key: RequestKey) -> u64 {
        *self
            .keys
            .iter()
            .find(|(_, live)| **live == key)
            .expect("every window key belongs to an issued request")
            .0
    }

    /// Settle a write as `RequestWrites::settle` does: a frame that never
    /// started expires, and a started one stays with transport.
    fn settle(&mut self, sequence: u64) -> WriteOutcome {
        self.keys.remove(&sequence);
        if self.written.remove(&sequence) {
            WriteOutcome::Started
        } else {
            WriteOutcome::Skipped
        }
    }

    fn issue(&mut self, sequence: u64, start: u32, count: u32, now: Instant, deadline: Instant) {
        let window = &mut self.state.window;
        let key = window.outstanding.push(OutstandingBlockRange {
            request: BlockRangeRequest {
                owner: Self::owner(sequence),
                start_height: block::Height(start),
                count,
                anchor_hash: hash(start, false),
                estimated_bytes: u64::from(count),
                expected_blocks: (start..start + count)
                    .map(|height| ExpectedBlock {
                        height: block::Height(height),
                        hash: hash(height, false),
                        estimated_bytes: 1,
                    })
                    .collect(),
            },
            queued_at: now,
            deadline,
            delivery_snapshot: window.delivery_snapshot(now),
            delivered_bytes: 0,
            received: ReceivedBlockTracker::default(),
        });
        window.arm_liveness(now, LIVENESS_TIMEOUT);
        self.keys.insert(sequence, key);
    }

    fn finish(&mut self, key: RequestKey, disposition: Disposition) {
        let sequence = self.sequence(key);
        self.state
            .window
            .outstanding
            .remove(key)
            .expect("a finished request is in the window");
        self.settle(sequence);
        if disposition == Disposition::Satisfied {
            self.state.window.disarm_liveness_after_progress_if_idle();
        }
    }
}

/// Apply `input` to both sides and check the decisions that it produced.
fn step(model: &mut Model, harness: &mut Harness, input: Input) {
    match input {
        Input::Issue {
            start,
            count,
            due_ms,
        } => {
            let end = start + count - 1;
            // The work queue gives each height one owner at a time.
            if end > MAX_HEIGHT || (start..=end).any(|height| model.covering(height).is_some()) {
                return;
            }
            let sequence = model.next_sequence;
            model.next_sequence += 1;
            let deadline = model.now + Duration::from_millis(due_ms);
            harness.issue(sequence, start, count, model.now, deadline);
            model.requests.insert(
                sequence,
                ModelRequest {
                    start,
                    count,
                    deadline,
                    written: false,
                    received: BTreeSet::new(),
                },
            );
            model.streak += 1;
            model.charged.insert(sequence);
        }
        Input::WriteStarted { pick } => {
            let Some(sequence) = model.pick(pick, false) else {
                return;
            };
            model
                .requests
                .get_mut(&sequence)
                .expect("picked requests are live")
                .written = true;
            harness.written.insert(sequence);
        }
        Input::Body { height, tampered } => {
            let covering = model.covering(height);
            // A peer can answer only a request whose frame it received.
            if covering.is_some_and(|sequence| !model.requests[&sequence].written) {
                return;
            }
            let decision = harness
                .state
                .match_body(block::Height(height), hash(height, tampered));
            match covering {
                None => {
                    assert!(
                        matches!(decision, BodyMatch::Unmatched),
                        "{PROPERTY}: {decision:?}"
                    );
                    // The work still wants an untampered body.
                    if !tampered {
                        harness.state.record_late_body(
                            block::Height(height),
                            hash(height, false),
                            block::Height(0),
                            model.now,
                            LIVENESS_TIMEOUT,
                        );
                        model.progress();
                    }
                }
                // A repeated height is a duplicate before its hash is compared.
                Some(sequence) if model.requests[&sequence].received.contains(&height) => {
                    assert!(
                        matches!(decision, BodyMatch::Duplicate),
                        "{PROPERTY}: {decision:?}"
                    );
                }
                Some(sequence) if tampered => {
                    let BodyMatch::PayloadMismatch { owner, requested } = decision else {
                        panic!("{PROPERTY}: {decision:?}");
                    };
                    // The fault names the exact request the body answered.
                    assert_eq!(owner, Harness::owner(sequence), "{PROPERTY}");
                    assert_eq!(requested, hash(height, false), "{PROPERTY}");
                }
                Some(sequence) => {
                    let BodyMatch::Matched(matched) = decision else {
                        panic!("{PROPERTY}: {decision:?}");
                    };
                    assert_eq!(matched.key, harness.keys[&sequence], "{PROPERTY}");
                    assert_eq!(matched.owner, Harness::owner(sequence), "{PROPERTY}");
                    let complete = harness.state.record_body(
                        matched.key,
                        block::Height(height),
                        100,
                        model.now,
                        LIVENESS_TIMEOUT,
                    );
                    model.progress();
                    let request = model.requests.get_mut(&sequence).expect("live");
                    request.received.insert(height);
                    let model_complete = request.received.len() == request.count as usize;
                    assert_eq!(complete, model_complete, "{PROPERTY}");
                    if complete {
                        harness.finish(matched.key, Disposition::Satisfied);
                        model.remove(sequence);
                    }
                }
            }
        }
        Input::Ending {
            pick,
            unavailable,
            floor,
        } => {
            let Some(sequence) = model.pick(pick, true) else {
                return;
            };
            let outcome = if unavailable {
                Outcome::Unavailable
            } else {
                Outcome::Done
            };
            let start = model.requests[&sequence].start;
            let ending = harness
                .state
                .on_ending(block::Height(start), outcome, block::Height(floor))
                .expect("a live request accepts its ending");
            let request = &model.requests[&sequence];
            // The heights the test neither delivered nor left above the floor.
            let stale: Vec<_> = request
                .heights()
                .filter(|height| *height <= floor && !request.received.contains(height))
                .map(block::Height)
                .collect();
            let missing = request
                .heights()
                .any(|height| height > floor && !request.received.contains(&height));
            assert_eq!(ending.key, harness.keys[&sequence], "{PROPERTY}");
            assert_eq!(ending.owner, Harness::owner(sequence), "{PROPERTY}");
            assert_eq!(&ending.stale_heights, &stale, "{PROPERTY}");
            // Only an ending that leaves no height missing above the floor is
            // satisfied, and only `RangeUnavailable` retries the original range.
            assert_eq!(
                ending.disposition == Disposition::Satisfied,
                !missing,
                "{PROPERTY}: {:?} with heights missing: {missing}",
                ending.disposition
            );
            assert!(
                ending.disposition != Disposition::RetryOriginal || unavailable,
                "{PROPERTY}: a short `BlocksDone` retried the original range"
            );
            harness.finish(ending.key, ending.disposition);
            model.remove(sequence);
        }
        Input::LateEnding { start } => {
            if model
                .requests
                .values()
                .any(|request| request.start == start)
            {
                return;
            }
            let ending =
                harness
                    .state
                    .on_ending(block::Height(start), Outcome::Done, block::Height(0));
            assert!(ending.is_none(), "{PROPERTY}: {ending:?}");
        }
        Input::Expire { advance_ms } => {
            model.now += Duration::from_millis(advance_ms);
            let due: Vec<_> = harness
                .state
                .due_requests(model.now)
                .into_iter()
                .map(|key| {
                    let sequence = harness.sequence(key);
                    (key, harness.settle(sequence))
                })
                .collect();
            let model_due: Vec<_> = model
                .requests
                .iter()
                .filter(|(_, request)| request.deadline <= model.now)
                .map(|(sequence, request)| (*sequence, request.written))
                .collect();
            let expired = harness.state.expire_requests(due, block::Height(0));
            let owners = |requests: &[OutstandingBlockRange]| -> Vec<_> {
                requests
                    .iter()
                    .map(|request| request.request.owner)
                    .collect()
            };
            let expect = |written: bool| -> Vec<_> {
                model_due
                    .iter()
                    .filter(|(_, started)| *started == written)
                    .map(|(sequence, _)| Harness::owner(*sequence))
                    .collect()
            };
            // Only a started write charges the peer with a timeout.
            assert_eq!(owners(&expired.skipped), expect(false), "{PROPERTY}");
            assert_eq!(owners(&expired.timed_out), expect(true), "{PROPERTY}");
            // Skipped requests retire first, each refunding its own probe.
            for (sequence, _) in model_due.iter().filter(|(_, started)| !started) {
                if model.charged.contains(sequence) {
                    model.streak = model.streak.saturating_sub(1);
                }
                model.remove(*sequence);
            }
            for (sequence, _) in model_due.iter().filter(|(_, started)| *started) {
                model.remove(*sequence);
            }
        }
        Input::Reset => {
            for (key, _) in harness.state.window.outstanding.drain() {
                let sequence = harness.sequence(key);
                harness.settle(sequence);
            }
            harness.state.note_locally_returned_requests();
            model.requests.clear();
            model.charged.clear();
            model.streak = 0;
        }
    }
    check(model, harness)
}

/// Check the state's invariants, and compare every live request and the
/// no-progress accounting with the ledger.
fn check(model: &Model, harness: &Harness) {
    harness.state.assert_invariants();
    let window = &harness.state.window;
    // Request isolation: each live request holds exactly its own facts.
    let live: Vec<_> = window
        .outstanding
        .entries()
        .map(|(key, request)| {
            let received: BTreeSet<_> = request
                .request
                .expected_blocks
                .iter()
                .filter(|expected| request.has_received(expected.height))
                .map(|expected| expected.height.0)
                .collect();
            (
                harness.sequence(key),
                request.request.owner,
                request.request.start_height.0,
                request.deadline,
                received,
            )
        })
        .collect();
    let expected: Vec<_> = model
        .requests
        .iter()
        .map(|(sequence, request)| {
            (
                *sequence,
                Harness::owner(*sequence),
                request.start,
                request.deadline,
                request.received.clone(),
            )
        })
        .collect();
    assert_eq!(live, expected, "{PROPERTY}");
    // Fault attribution: only requests the peer saw count toward its stall.
    assert_eq!(
        window.requests_without_block_progress, model.streak,
        "{PROPERTY}"
    );
    // Deadline choice: the next wake is the earliest owed deadline, found here
    // by scanning every request.
    let earliest = window
        .outstanding
        .entries()
        .map(|(_, request)| request.deadline)
        .chain(window.block_liveness_deadline)
        .min();
    assert_eq!(
        harness.state.next_wake(SharedDeadlines::default()),
        earliest,
        "{PROPERTY}"
    );
    let retry_filter = model.now + Duration::from_millis(1);
    assert_eq!(
        harness.state.next_wake(SharedDeadlines {
            retry_filter: Some(retry_filter),
            ..SharedDeadlines::default()
        }),
        Some(earliest.map_or(retry_filter, |earliest| earliest.min(retry_filter))),
        "{PROPERTY}"
    );
    assert_eq!(
        harness.state.deadlines_due(model.now),
        earliest.is_some_and(|earliest| earliest <= model.now),
        "{PROPERTY}"
    );
}

const PROPERTY: &str = "block/request-lifecycle";

/// Applicable request operations in a decoder-v1 history.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct RequestLifecycleReplaySummary {
    /// Inputs checked.
    pub transitions: usize,
    /// Nonoverlapping requests issued.
    pub issued: u64,
    /// Started writes that expired.
    pub written_timeouts: usize,
    /// Unstarted writes that expired without charging the peer.
    pub skipped_writes: usize,
    /// New valid bodies matched to a written request.
    pub matched_bodies: usize,
    /// Endings without a live request at that start.
    pub stale_endings: usize,
    /// Resets checked.
    pub resets: usize,
}

/// Replay at most [`MAX_HISTORY_BYTES`] bytes of decoder-v1 quadruples
/// through [`PeerState`]. Requests and logical time stay bounded by the decoder.
///
/// The state keeps its invariants after every input. A body matches exactly
/// the written request that covers its height, and a tampered body names that
/// request's owner. Only a started write that expires charges the peer with a
/// timeout, and only requests the peer saw count toward its stall.
pub fn replay_request_lifecycle_bytes(data: &[u8]) -> RequestLifecycleReplaySummary {
    run(decode(data), |_| {})
}

/// Decoder v1: each four bytes are one input. The first byte chooses the input;
/// the others choose its arguments.
fn decode(data: &[u8]) -> impl Iterator<Item = Input> + '_ {
    data[..data.len().min(MAX_HISTORY_BYTES)]
        .chunks(4)
        .map(|chunk| {
            let a = chunk[0];
            let b = chunk.get(1).copied().unwrap_or(0);
            let c = chunk.get(2).copied().unwrap_or(0);
            let d = chunk.get(3).copied().unwrap_or(0);
            let height = u32::from(b) % MAX_HEIGHT + 1;
            match a % 7 {
                0 => Input::Issue {
                    start: height,
                    count: u32::from(c % 2) + 1,
                    due_ms: u64::from(d) * 10 + 1,
                },
                1 => Input::WriteStarted {
                    pick: usize::from(b),
                },
                2 => Input::Body {
                    height,
                    tampered: c & 1 != 0,
                },
                3 => Input::Ending {
                    pick: usize::from(b),
                    unavailable: c & 1 != 0,
                    floor: u32::from(d) % (MAX_HEIGHT + 1),
                },
                4 => Input::LateEnding { start: height },
                5 => Input::Expire {
                    advance_ms: u64::from(b) * 10 + 1,
                },
                _ => Input::Reset,
            }
        })
}

fn run(
    inputs: impl IntoIterator<Item = Input>,
    mut after_step: impl FnMut(&mut Harness),
) -> RequestLifecycleReplaySummary {
    let mut model = Model::new(Instant::now());
    let mut harness = Harness::new();
    let mut summary = RequestLifecycleReplaySummary::default();
    for input in inputs {
        summary.transitions += 1;
        match &input {
            Input::Body {
                height,
                tampered: false,
            } => {
                summary.matched_bodies +=
                    usize::from(model.covering(*height).is_some_and(|sequence| {
                        let request = &model.requests[&sequence];
                        request.written && !request.received.contains(height)
                    }));
            }
            Input::LateEnding { start } => {
                summary.stale_endings += usize::from(
                    !model
                        .requests
                        .values()
                        .any(|request| request.start == *start),
                );
            }
            Input::Expire { advance_ms } => {
                let now = model.now + Duration::from_millis(*advance_ms);
                for request in model
                    .requests
                    .values()
                    .filter(|request| request.deadline <= now)
                {
                    if request.written {
                        summary.written_timeouts += 1;
                    } else {
                        summary.skipped_writes += 1;
                    }
                }
            }
            Input::Reset => summary.resets += 1,
            _ => {}
        }
        step(&mut model, &mut harness, input);
        after_step(&mut harness);
        check(&model, &harness);
    }
    summary.issued = model.next_sequence - 1;
    summary
}

/// Expires a written request and an unwritten one, matches a replacement
/// request, refuses a stale ending, and resets the remaining window.
#[cfg(test)]
const WITNESS: &[u8] = &[
    0, 0, 0, 0, 1, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 4, 0, 0, 0, 0, 1, 0, 0,
    5, 0, 0, 0, 0, 2, 0, 0, 6, 0, 0, 0,
];

#[cfg(test)]
machine_target_tests! {
    replay: replay_request_lifecycle_bytes,
    target: "block_request_lifecycle",
    decoder: 1,
    witness: WITNESS => RequestLifecycleReplaySummary {
        transitions: 11,
        issued: 4,
        written_timeouts: 1,
        skipped_writes: 1,
        matched_bodies: 1,
        stale_endings: 1,
        resets: 1,
    },
}

/// An ending retries the original range only for `RangeUnavailable` above the
/// floor. A short `BlocksDone` retries the missing heights, and so does an
/// ending the floor partly covered. An ending the floor and the received
/// bodies cover is satisfied, and its unreceived heights at or below the floor
/// return without a retry.
#[test]
fn an_ending_retries_by_its_outcome_and_the_floor() {
    use Disposition::{RetryMissing, RetryOriginal, Satisfied};
    // (start, count, received, outcome is unavailable, floor, disposition, stale)
    let cases: [(u32, u32, &[u32], bool, u32, Disposition, &[u32]); 8] = [
        (3, 2, &[], true, 0, RetryOriginal, &[]),
        (3, 2, &[], false, 0, RetryMissing, &[]),
        (3, 2, &[3], true, 0, RetryOriginal, &[]),
        (3, 2, &[], true, 3, RetryMissing, &[3]),
        (3, 2, &[], false, 3, RetryMissing, &[3]),
        (3, 2, &[], true, 4, Satisfied, &[3, 4]),
        (3, 2, &[4], false, 3, Satisfied, &[3]),
        (3, 1, &[], false, 3, Satisfied, &[3]),
    ];
    for (start, count, received, unavailable, floor, disposition, stale) in cases {
        let mut harness = Harness::new();
        let now = Instant::now();
        harness.issue(1, start, count, now, now + LIVENESS_TIMEOUT);
        let key = harness.keys[&1];
        for height in received {
            harness
                .state
                .record_body(key, block::Height(*height), 1, now, LIVENESS_TIMEOUT);
        }
        let outcome = if unavailable {
            Outcome::Unavailable
        } else {
            Outcome::Done
        };
        let ending = harness
            .state
            .on_ending(block::Height(start), outcome, block::Height(floor))
            .expect("a live request accepts its ending");
        let case = format!("{start}+{count}, received {received:?}, {outcome:?}, floor {floor}");
        assert_eq!(ending.disposition, disposition, "{case}");
        assert_eq!(
            ending.stale_heights,
            stale.iter().copied().map(block::Height).collect::<Vec<_>>(),
            "{case}"
        );
    }
}

/// A body for a height its request already received is a duplicate, whether
/// or not its hash matches: the repeat is checked before the hash.
#[test]
fn a_repeated_height_is_a_duplicate_before_its_hash_is_compared() {
    for tampered in [false, true] {
        let summary = run(
            [
                Input::Issue {
                    start: 1,
                    count: 2,
                    due_ms: 1_000,
                },
                Input::WriteStarted { pick: 0 },
                Input::Body {
                    height: 1,
                    tampered: false,
                },
                Input::Body {
                    height: 1,
                    tampered,
                },
            ],
            |_| {},
        );
        assert_eq!(summary.matched_bodies, 1, "tampered repeat: {tampered}");
    }
}

/// The liveness deadline runs from the first request since the last body, so
/// later requests cannot push it out. A body restarts it while work is
/// outstanding, and a body that leaves the window idle disarms it.
#[test]
fn the_liveness_deadline_runs_from_the_first_unanswered_request() {
    let start = Instant::now();
    let far = start + Duration::from_secs(3_600);
    let later = start + Duration::from_secs(1);
    let mut harness = Harness::new();
    harness.issue(1, 1, 1, start, far);
    harness.issue(2, 2, 1, later, far);
    assert_eq!(
        harness.state.window.block_liveness_deadline,
        Some(start + LIVENESS_TIMEOUT)
    );

    let key = harness.keys[&1];
    assert!(harness
        .state
        .record_body(key, block::Height(1), 1, later, LIVENESS_TIMEOUT));
    harness.finish(key, Disposition::Satisfied);
    assert_eq!(
        harness.state.window.block_liveness_deadline,
        Some(later + LIVENESS_TIMEOUT)
    );

    let key = harness.keys[&2];
    assert!(harness
        .state
        .record_body(key, block::Height(2), 1, later, LIVENESS_TIMEOUT));
    harness.finish(key, Disposition::Satisfied);
    assert_eq!(harness.state.window.block_liveness_deadline, None);
}

#[test]
#[should_panic(expected = "block/request-lifecycle")]
fn lifecycle_witness_rejects_lost_no_progress_charge() {
    run(decode(WITNESS), |harness| {
        harness.state.window.requests_without_block_progress = 0;
    });
}

#[test]
#[should_panic(expected = "outstanding work keeps a liveness deadline")]
fn lifecycle_witness_rejects_lost_liveness_deadline() {
    run(decode(WITNESS), |harness| {
        harness.state.window.block_liveness_deadline = None;
    });
}

/// A body that arrives after its request timed out matches the replacement
/// request for the same height, never the expired one.
#[test]
fn a_late_body_matches_only_the_replacement_request() {
    let mut model = Model::new(Instant::now());
    let mut harness = Harness::new();
    let inputs = [
        Input::Issue {
            start: 3,
            count: 1,
            due_ms: 10,
        },
        Input::WriteStarted { pick: 0 },
        Input::Expire { advance_ms: 10 },
        Input::Issue {
            start: 3,
            count: 1,
            due_ms: 10,
        },
        Input::WriteStarted { pick: 0 },
        Input::Body {
            height: 3,
            tampered: false,
        },
        Input::Body {
            height: 3,
            tampered: false,
        },
    ];
    for input in inputs {
        step(&mut model, &mut harness, input);
    }
    assert!(model.requests.is_empty());
    assert_eq!(model.next_sequence, 3);
}

/// A peer that briefly drains its outbound queue cannot renew the congestion
/// grace. Block progress and disarm each start a new episode with a fresh grace.
#[test]
fn liveness_grace_is_granted_once_per_no_progress_episode() {
    let request_timeout = Duration::from_secs(2);
    let start = Instant::now();
    let far = start + Duration::from_secs(3_600);
    let mut harness = Harness::new();
    harness.issue(1, 1, 1, start, far);

    // Congestion at the deadline earns the episode's one grace.
    let deadline = start + LIVENESS_TIMEOUT;
    harness.state.note_outbound_capacity(false, deadline);
    assert_eq!(
        harness
            .state
            .check_block_liveness(deadline, true, request_timeout),
        super::LivenessDecision::Continue,
    );

    // A brief free slot starts a fresh full interval but no fresh grace.
    let drained = deadline + Duration::from_millis(1);
    harness.state.note_outbound_capacity(true, drained);
    harness.state.note_outbound_capacity(false, drained);
    let extended = deadline + request_timeout;
    assert_eq!(
        harness
            .state
            .check_block_liveness(extended, true, request_timeout),
        super::LivenessDecision::Stall,
        "a renewed full interval must not grant a second grace",
    );

    // Block progress with work still outstanding starts a new episode.
    harness
        .state
        .window
        .note_block_progress(extended, LIVENESS_TIMEOUT);
    let next_deadline = extended + LIVENESS_TIMEOUT;
    harness.state.note_outbound_capacity(true, next_deadline);
    harness.state.note_outbound_capacity(false, next_deadline);
    assert_eq!(
        harness
            .state
            .check_block_liveness(next_deadline, true, request_timeout),
        super::LivenessDecision::Continue,
        "block progress restores the grace",
    );

    // Disarming an idle window also starts a new episode.
    harness.state.window.outstanding.drain();
    harness.state.window.clear_liveness_if_idle();
    let reissued = next_deadline + Duration::from_millis(1);
    harness.issue(2, 2, 1, reissued, far);
    let last_deadline = reissued + LIVENESS_TIMEOUT;
    harness.state.note_outbound_capacity(true, last_deadline);
    harness.state.note_outbound_capacity(false, last_deadline);
    assert_eq!(
        harness
            .state
            .check_block_liveness(last_deadline, true, request_timeout),
        super::LivenessDecision::Continue,
        "disarm restores the grace",
    );
}

/// A peer at its no-progress request cap takes no request, so its slot
/// publication shows no free slot, whatever its window allows.
#[test]
fn publish_slots_shows_no_free_slot_at_the_no_progress_cap() {
    let config = ZakuraBlockSyncConfig::default();
    let now = Instant::now();
    let mut harness = Harness::new();
    harness.state.received_status = true;
    harness.state.servable_high = block::Height(MAX_HEIGHT);
    harness.issue(1, 1, 1, now, now + LIVENESS_TIMEOUT);

    assert!(
        harness.state.window.available_slots_at(now) > 0,
        "the window still has room, so only the no-progress cap refuses work",
    );
    assert_eq!(
        harness.state.fill_limits(&config, now).err(),
        Some(super::FillStop::InitialBlockProbeRequestCap),
    );
    let slots = harness.state.publish_slots(false, false, now);
    assert_eq!(slots.available_slots, 0);
    assert!(!harness.state.published_free_slots);
}

/// A full outbound queue takes no request, so the slot publication shows no
/// free slot, whatever the window allows.
#[test]
fn publish_slots_shows_no_free_slot_while_the_outbound_queue_is_full() {
    let now = Instant::now();
    let mut harness = Harness::new();
    assert!(harness.state.window.available_slots_at(now) > 0);
    let slots = harness.state.publish_slots(false, true, now);
    assert_eq!(slots.available_slots, 0);
    assert!(!harness.state.published_free_slots);
    assert!(harness.state.published_outbound_full);
}

/// A late body offsets the timeout its own request charged, once per request.
/// A second body of the same request, a repeated body, a body with another
/// hash, a body for a height this session never timed out, and a body at or
/// below the download floor earn no credit, though each still proves progress.
#[test]
fn a_late_body_credits_only_its_own_timeout_once() {
    let mut harness = Harness::new();
    let start = Instant::now();
    harness.issue(1, 3, 3, start, start + Duration::from_millis(10));
    harness.written.insert(1);
    let now = start + Duration::from_millis(10);
    let due: Vec<_> = harness
        .state
        .due_requests(now)
        .into_iter()
        .map(|key| (key, harness.settle(1)))
        .collect();
    let expired = harness.state.expire_requests(due, block::Height(3));
    assert_eq!(expired.timed_out.len(), 1);
    assert_eq!(
        harness
            .state
            .uncredited_timeouts
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![block::Height(4), block::Height(5)],
        "only the unreceived heights above the floor can offset the timeout"
    );
    let after_timeout = harness.state.window.reliability_factor();
    let late = |harness: &mut Harness, height: u32, tampered: bool| {
        harness.state.record_late_body(
            block::Height(height),
            hash(height, tampered),
            block::Height(3),
            now,
            LIVENESS_TIMEOUT,
        )
    };

    assert!(!late(&mut harness, 9, false));
    assert!(
        !late(&mut harness, 4, true),
        "a body with another hash answers another request"
    );
    assert_eq!(harness.state.window.reliability_factor(), after_timeout);

    assert!(late(&mut harness, 4, false));
    let credited = harness.state.window.reliability_factor();
    assert!(credited > after_timeout);

    assert!(!late(&mut harness, 4, false));
    assert!(
        !late(&mut harness, 5, false),
        "one timed-out request earns one credit"
    );
    assert_eq!(
        harness.state.window.reliability_factor(),
        credited,
        "later late bodies earn no second credit"
    );
    assert!(harness.state.uncredited_timeouts.is_empty());
    assert!(harness.state.window.has_block_progress());
}

/// The download floor retires uncredited timeouts, so the map stays within
/// the window above the floor.
#[test]
fn the_download_floor_retires_uncredited_timeouts() {
    let mut harness = Harness::new();
    let start = Instant::now();
    harness.issue(1, 3, 3, start, start + Duration::from_millis(10));
    harness.written.insert(1);
    let now = start + Duration::from_millis(10);
    let due: Vec<_> = harness
        .state
        .due_requests(now)
        .into_iter()
        .map(|key| (key, harness.settle(1)))
        .collect();
    harness.state.expire_requests(due, block::Height(0));
    assert_eq!(harness.state.uncredited_timeouts.len(), 3);

    assert!(!harness.state.record_late_body(
        block::Height(4),
        hash(4, false),
        block::Height(4),
        now,
        LIVENESS_TIMEOUT
    ));
    assert_eq!(
        harness
            .state
            .uncredited_timeouts
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![block::Height(5)]
    );
}
