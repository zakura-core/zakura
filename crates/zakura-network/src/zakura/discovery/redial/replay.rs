//! The `discovery_redial` machine target: decoder-v2 histories through
//! [`RedialState`], shared by its proptest and its cargo-fuzz target.
//!
//! [`replay_redial_bytes`] drives the production state as the redial loop
//! does (see [`machine_test`](crate::zakura::machine_test)). It checks the
//! state's invariants after every attempt, and compares each next step only
//! with facts that follow from the attempts it chose: how many counted
//! failures it sent since the last healthy attempt, the policy's bounds, and
//! the waits the state returned before. It never re-derives the backoff.
//! Named scenarios in `tests.rs` pin the rules.

use std::time::Duration;

use super::state::{RedialNext, RedialStart, RedialState};
use super::{DialResult, RedialPolicy};
use crate::zakura::{machine_test::MAX_HISTORY_BYTES, ZAKURA_ALPN_MISMATCH_BACKOFF};

const PROPERTY: &str = "discovery/redial-policy";

/// A policy production builds: [`RedialPolicy::maintain`] when `attempts` is
/// `None`, and [`RedialPolicy::maintain_bounded`] otherwise.
#[derive(Copy, Clone, Debug)]
pub(super) struct Policy {
    pub(super) attempts: Option<usize>,
    pub(super) initial: Duration,
    pub(super) max: Duration,
}

impl Policy {
    fn build(self) -> RedialPolicy {
        match self.attempts {
            None => RedialPolicy::maintain(self.initial, self.max),
            Some(attempts) => RedialPolicy::maintain_bounded(self.initial, self.max, attempts),
        }
    }
}

#[derive(Clone, Debug)]
pub(super) enum RedialOp {
    /// Start an attempt. A registered peer's registration lasts `lasted`
    /// seconds.
    Start {
        registered: bool,
        lasted: Option<u64>,
    },
    /// A dial returns `result`, and the peer's registration then outlives it
    /// by `lagging` seconds.
    Dial {
        result: DialResult,
        lagging: Option<u64>,
    },
}

/// Applicable transitions in a decoder-v2 redial history.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct RedialReplaySummary {
    /// Attempts whose result reached the policy.
    pub scored: usize,
    /// Local limits that must preserve the remote failure budget.
    pub local_limits: usize,
    /// Healthy attempts that reset the policy.
    pub resets: usize,
    /// Connections that outlived a failed dial and upgraded its outcome.
    pub lagging_upgrades: usize,
    /// Terminal policy decisions.
    pub stops: usize,
}

/// Replay at most [`MAX_HISTORY_BYTES`] bytes through [`RedialState`].
///
/// Decoder v2. Three header bytes select the policy and its backoff bounds:
/// the first byte's low bit chooses a maintained or a bounded policy, and its
/// other bits the bounded policy's attempts. Each following triple selects a
/// start or a dial result, registration presence and its lifetime. A broken
/// property panics with `discovery/redial-policy`.
pub fn replay_redial_bytes(data: &[u8]) -> RedialReplaySummary {
    let data = &data[..data.len().min(MAX_HISTORY_BYTES)];
    let Some(header) = data.get(..3) else {
        return RedialReplaySummary::default();
    };
    let policy = Policy {
        attempts: (header[0] & 1 == 1).then_some(usize::from(header[0] / 2 % 5) + 1),
        initial: Duration::from_secs(u64::from(header[1] % 30) + 1),
        max: Duration::from_secs(u64::from(header[2] % 120) + 1),
    };
    let ops = data[3..].chunks(3).map(|chunk| {
        let first = chunk[0];
        let lasted = (chunk.get(1).copied().unwrap_or(0) & 1 != 0)
            .then_some(u64::from(chunk.get(2).copied().unwrap_or(0) % 120));
        if first & 1 == 0 {
            RedialOp::Start {
                registered: first & 2 != 0,
                lasted,
            }
        } else {
            let result = match (first >> 1) % 4 {
                0 => DialResult::Healthy,
                1 => DialResult::Failed,
                2 => DialResult::AlpnMismatch,
                _ => DialResult::LocalLimit,
            };
            RedialOp::Dial {
                result,
                lagging: lasted,
            }
        }
    });
    run(policy, ops, |_, _| {})
}

/// Drives `ops` through a fresh [`RedialState`] under `policy`, and calls
/// `after_attempt` after each recorded attempt, so a test can plant a fault.
pub(super) fn run(
    policy: Policy,
    ops: impl IntoIterator<Item = RedialOp>,
    mut after_attempt: impl FnMut(&mut RedialState, DialResult),
) -> RedialReplaySummary {
    let mut state = RedialState::new(policy.build());
    // Counted failures this history sent since its last healthy attempt.
    let mut failures = 0;
    // The wait after the last counted failure since the last healthy attempt.
    let mut last_wait: Option<Duration> = None;
    let mut summary = RedialReplaySummary::default();
    for op in ops {
        let (registered, attempt, lasted) = match op {
            RedialOp::Start { registered, lasted } => (registered, None, lasted),
            RedialOp::Dial { result, lagging } => (false, Some(result), lagging),
        };
        let start = state.start(registered);
        let attempt = match start {
            RedialStart::Dial => {
                assert!(!registered, "{PROPERTY}: a registered peer is dialed");
                let attempt = attempt.unwrap_or(DialResult::Failed);
                // The loop waits out a registration that outlives the dial,
                // and rescores the dial with it.
                match lasted {
                    Some(lasted) if state.waits_for_lagging_deregistration(attempt) => {
                        assert_ne!(
                            attempt,
                            DialResult::AlpnMismatch,
                            "{PROPERTY}: an ALPN mismatch waits for a registration"
                        );
                        let rescored =
                            attempt.with_lagging_registration(Duration::from_secs(lasted));
                        assert!(
                            rescored == attempt || rescored == DialResult::Healthy,
                            "{PROPERTY}: a lagging registration only upgrades an attempt"
                        );
                        summary.lagging_upgrades += usize::from(rescored != attempt);
                        rescored
                    }
                    _ => attempt,
                }
            }
            RedialStart::ScoreRegistration => {
                assert!(registered, "{PROPERTY}: an unregistered peer is scored");
                let scored = DialResult::lasting(Duration::from_secs(lasted.unwrap_or(0)));
                assert!(
                    matches!(scored, DialResult::Healthy | DialResult::Failed),
                    "{PROPERTY}: a registration scores as healthy or failed"
                );
                scored
            }
            RedialStart::AwaitRegisteredSettle(_) => {
                panic!("{PROPERTY}: a maintained policy awaits a registration's settle")
            }
        };

        let next = state.record(attempt);
        after_attempt(&mut state, attempt);
        state.assert_invariants();
        summary.scored += 1;
        match attempt {
            DialResult::Healthy => {
                failures = 0;
                last_wait = None;
                summary.resets += 1;
                assert_eq!(
                    next,
                    RedialNext::RedialNow,
                    "{PROPERTY}: a maintained policy redials after a healthy attempt"
                );
            }
            DialResult::LocalLimit => {
                summary.local_limits += 1;
                assert_eq!(
                    next,
                    RedialNext::RetryAfter(policy.initial),
                    "{PROPERTY}: a local limit retries after the initial backoff"
                );
            }
            DialResult::Failed | DialResult::AlpnMismatch => failures += 1,
        }

        // A bounded policy stops at its first ALPN mismatch or its last
        // counted failure. A maintained policy never stops.
        let stops = policy.attempts.is_some_and(|max| {
            attempt == DialResult::AlpnMismatch
                || (attempt == DialResult::Failed && failures >= max)
        });
        assert_eq!(
            next == RedialNext::Stop,
            stops,
            "{PROPERTY}: stop after {failures} counted failures and {attempt:?}"
        );
        match next {
            RedialNext::Stop => {
                summary.stops += 1;
                break;
            }
            RedialNext::RedialNow => {}
            RedialNext::RetryAfter(wait) if attempt == DialResult::Failed => {
                // The first counted failure waits the initial backoff. Each
                // later one waits longer than the failure before it, up to
                // the maximum.
                if failures == 1 {
                    assert_eq!(wait, policy.initial, "{PROPERTY}: first backoff");
                }
                if let Some(last) = last_wait {
                    assert!(
                        wait > last || wait == policy.max,
                        "{PROPERTY}: backoff {wait:?} after {last:?}"
                    );
                }
                assert!(
                    policy.initial.min(policy.max) <= wait
                        && wait <= policy.initial.max(policy.max),
                    "{PROPERTY}: backoff {wait:?} outside the policy's bounds"
                );
                last_wait = Some(wait);
            }
            RedialNext::RetryAfter(_) => {}
            RedialNext::WaitOut(wait) => {
                assert_eq!(attempt, DialResult::AlpnMismatch, "{PROPERTY}: wait-out");
                assert!(
                    wait >= ZAKURA_ALPN_MISMATCH_BACKOFF,
                    "{PROPERTY}: ALPN backoff floor"
                );
            }
        }
    }
    summary
}
