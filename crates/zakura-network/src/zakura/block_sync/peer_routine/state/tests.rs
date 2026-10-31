//! The request lifecycle of [`PeerState`] checked against an independent model.
//!
//! The harness plays the routine's part: it settles writes, removes finished
//! requests, and feeds bodies, endings, expiry, and resets. The model tracks
//! the same requests and the no-progress accounting on its own. After every
//! input, the test checks request isolation, deadline choice, and fault
//! attribution.

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU64,
    time::Duration,
};

#[cfg(test)]
use proptest::prelude::*;
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

#[cfg(test)]
fn input() -> impl Strategy<Value = Input> {
    prop_oneof![
        3 => (1..=MAX_HEIGHT, 1u32..=2, 1u64..3_000)
            .prop_map(|(start, count, due_ms)| Input::Issue { start, count, due_ms }),
        3 => any::<usize>().prop_map(|pick| Input::WriteStarted { pick }),
        3 => (1..=MAX_HEIGHT, prop::bool::weighted(0.2))
            .prop_map(|(height, tampered)| Input::Body { height, tampered }),
        2 => (any::<usize>(), any::<bool>(), 0..=MAX_HEIGHT)
            .prop_map(|(pick, unavailable, floor)| Input::Ending { pick, unavailable, floor }),
        1 => (1..=MAX_HEIGHT).prop_map(|start| Input::LateEnding { start }),
        2 => (0u64..4_000).prop_map(|advance_ms| Input::Expire { advance_ms }),
        1 => Just(Input::Reset),
    ]
}

fn hash(height: u32, tampered: bool) -> block::Hash {
    let byte = u8::try_from(height).expect("test heights fit in u8");
    block::Hash([if tampered { byte | 0x80 } else { byte }; 32])
}

/// One live request as the model sees it.
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

/// The model. Requests are keyed by issue sequence, which is also the
/// request ID in their work owner.
#[derive(Debug)]
struct Model {
    now: Instant,
    requests: BTreeMap<u64, ModelRequest>,
    next_sequence: u64,
    /// Requests sent since the last accepted body.
    streak: u32,
    /// Requests that still count toward `streak`.
    charged: BTreeSet<u64>,
    last_request_at: Option<Instant>,
    last_block_at: Option<Instant>,
    liveness: Option<Instant>,
}

impl Model {
    fn new(now: Instant) -> Self {
        Self {
            now,
            requests: BTreeMap::new(),
            next_sequence: 1,
            streak: 0,
            charged: BTreeSet::new(),
            last_request_at: None,
            last_block_at: None,
            liveness: None,
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
            .expect("the model removes only live requests")
    }

    fn progress(&mut self) {
        self.last_block_at = Some(self.now);
        self.streak = 0;
        self.charged.clear();
        self.liveness = (!self.requests.is_empty()).then(|| self.now + LIVENESS_TIMEOUT);
    }

    /// A satisfied request leaving an idle window disarms liveness when the
    /// last body came after the last request.
    fn disarm_if_idle_after_progress(&mut self) {
        if self.requests.is_empty()
            && matches!((self.last_request_at, self.last_block_at), (Some(request), Some(body)) if body >= request)
        {
            self.liveness = None;
        }
    }

    fn earliest_deadline(&self) -> Option<Instant> {
        self.requests
            .values()
            .map(|request| request.deadline)
            .chain(self.liveness)
            .min()
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
            model.last_request_at = Some(model.now);
            model.liveness.get_or_insert(model.now + LIVENESS_TIMEOUT);
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
                        harness.state.record_late_body(model.now, LIVENESS_TIMEOUT);
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
                        model.disarm_if_idle_after_progress();
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
            let stale: Vec<_> = request
                .heights()
                .filter(|height| *height <= floor && !request.received.contains(height))
                .map(block::Height)
                .collect();
            let expected = if start <= floor {
                if request
                    .heights()
                    .all(|height| height <= floor || request.received.contains(&height))
                {
                    Disposition::Satisfied
                } else {
                    Disposition::RetryMissing
                }
            } else if unavailable {
                Disposition::RetryOriginal
            } else {
                Disposition::RetryMissing
            };
            assert_eq!(ending.key, harness.keys[&sequence], "{PROPERTY}");
            assert_eq!(ending.owner, Harness::owner(sequence), "{PROPERTY}");
            assert_eq!(&ending.stale_heights, &stale, "{PROPERTY}");
            assert_eq!(ending.disposition, expected, "{PROPERTY}");
            harness.finish(ending.key, ending.disposition);
            model.remove(sequence);
            if expected == Disposition::Satisfied {
                model.disarm_if_idle_after_progress();
            }
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
            let expired = harness.state.expire_requests(due);
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
                if model.streak == 0 && model.requests.is_empty() {
                    model.liveness = None;
                }
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
            model.liveness = None;
        }
    }
    check(model, harness)
}

/// Compare every live request and the no-progress accounting with the model.
fn check(model: &Model, harness: &Harness) {
    let window = &harness.state.window;
    window.outstanding.assert_consistent();
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
    assert_eq!(window.block_liveness_deadline, model.liveness, "{PROPERTY}");
    // Deadline choice: the next wake is the earliest owed deadline.
    let earliest = model.earliest_deadline();
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
    /// Inputs checked against the independent model.
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

/// Replay decoder-v1 quadruples against the existing request lifecycle model.
/// Read at most 512 bytes. Requests and logical time stay bounded by the decoder.
pub fn replay_request_lifecycle_bytes(data: &[u8]) -> RequestLifecycleReplaySummary {
    run(decode(data), |_| {})
}

fn decode(data: &[u8]) -> impl Iterator<Item = Input> + '_ {
    data[..data.len().min(512)].chunks(4).map(|chunk| {
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

#[cfg(test)]
proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Writes, bodies, endings, expiry, resets, and replacements keep each
    /// request's facts, the next deadline, and the stall accounting equal to
    /// the model's.
    #[test]
    fn request_lifecycle_matches_the_model(inputs in prop::collection::vec(input(), 1..80)) {
        run(inputs, |_| {});
    }
    #[test]
    fn lifecycle_bytes_match_the_same_model(bytes in prop::collection::vec(any::<u8>(), 0..=512)) {
        replay_request_lifecycle_bytes(&bytes);
    }
}

#[cfg(test)]
const WITNESS: &[u8] = &[
    0, 0, 0, 0, 1, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 4, 0, 0, 0, 0, 1, 0, 0,
    5, 0, 0, 0, 0, 2, 0, 0, 6, 0, 0, 0,
];

#[test]
#[allow(
    clippy::print_stdout,
    reason = "witnesses print applicability counters"
)]
fn lifecycle_witness_exercises_replacement_timeout_and_skipped_write() {
    let summary = replay_request_lifecycle_bytes(WITNESS);
    assert_eq!(
        summary,
        RequestLifecycleReplaySummary {
            transitions: 11,
            issued: 4,
            written_timeouts: 1,
            skipped_writes: 1,
            matched_bodies: 1,
            stale_endings: 1,
            resets: 1,
        }
    );
    assert_eq!(
        replay_request_lifecycle_bytes(&[]),
        RequestLifecycleReplaySummary::default()
    );
    println!("tier=Machine target=block_request_lifecycle decoder=1 {summary:?}");
}

#[test]
#[should_panic(expected = "block/request-lifecycle")]
fn lifecycle_witness_rejects_lost_no_progress_charge() {
    run(decode(WITNESS), |harness| {
        harness.state.window.requests_without_block_progress = 0;
    });
}

#[test]
#[should_panic(expected = "block/request-lifecycle")]
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
