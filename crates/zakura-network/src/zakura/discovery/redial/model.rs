//! The independent model of [`RedialState`](super::state::RedialState), shared by
//! its proptest and its cargo-fuzz target. The model restates the documented rules
//! and never calls the code it checks.

use super::state::{RedialNext, RedialStart, RedialState};
use super::{DialResult, RedialPolicy};
use crate::zakura::ZAKURA_ALPN_MISMATCH_BACKOFF;
use std::time::Duration;

pub(super) const HEALTHY: Duration = Duration::from_secs(60);

#[derive(Copy, Clone, Debug)]
pub(super) enum ModelMode {
    ConnectOnce { attempts: usize, settle: Duration },
    Maintain,
    MaintainBounded { attempts: usize },
}

#[derive(Copy, Clone, Debug)]
pub(super) struct ModelPolicy {
    pub(super) mode: ModelMode,
    pub(super) initial: Duration,
    pub(super) max: Duration,
}

impl ModelPolicy {
    fn build(self) -> RedialPolicy {
        match self.mode {
            ModelMode::ConnectOnce { attempts, settle } => RedialPolicy {
                initial_backoff: self.initial,
                max_backoff: self.max,
                max_attempts: Some(attempts),
                registered_settle_timeout: settle,
                redial_after_drop: false,
            },
            ModelMode::Maintain => RedialPolicy::maintain(self.initial, self.max),
            ModelMode::MaintainBounded { attempts } => {
                RedialPolicy::maintain_bounded(self.initial, self.max, attempts)
            }
        }
    }

    fn maintains(self) -> bool {
        !matches!(self.mode, ModelMode::ConnectOnce { .. })
    }

    fn max_attempts(self) -> Option<usize> {
        match self.mode {
            ModelMode::ConnectOnce { attempts, .. } | ModelMode::MaintainBounded { attempts } => {
                Some(attempts)
            }
            ModelMode::Maintain => None,
        }
    }
}

#[derive(Clone, Debug)]
pub(super) enum RedialOp {
    /// Start an attempt. A registered peer's registration lasts `lasted`
    /// seconds, or changes while a connect-once loop waits when `lasted` is
    /// `None`.
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

/// The documented redial rules.
#[derive(Debug)]
struct RedialModel {
    policy: ModelPolicy,
    /// Counted failures since the last reset.
    failures: usize,
}

impl RedialModel {
    fn score(lasted: u64) -> DialResult {
        if Duration::from_secs(lasted) >= HEALTHY {
            DialResult::Healthy
        } else {
            DialResult::Failed
        }
    }

    fn record(&mut self, attempt: DialResult) -> RedialNext {
        match attempt {
            DialResult::Healthy if self.policy.maintains() => {
                self.failures = 0;
                RedialNext::RedialNow
            }
            DialResult::Healthy => RedialNext::Stop,
            DialResult::LocalLimit => RedialNext::RetryAfter(self.policy.initial),
            DialResult::Failed | DialResult::AlpnMismatch => {
                let doublings = self.failures;
                self.failures += 1;
                if self
                    .policy
                    .max_attempts()
                    .is_some_and(|max| self.failures >= max)
                {
                    return RedialNext::Stop;
                }
                let backoff = if doublings == 0 {
                    self.policy.initial
                } else {
                    let factor = 2u32.saturating_pow(u32::try_from(doublings).unwrap_or(u32::MAX));
                    self.policy
                        .initial
                        .saturating_mul(factor)
                        .min(self.policy.max)
                };
                let wait = if attempt == DialResult::AlpnMismatch {
                    backoff.max(ZAKURA_ALPN_MISMATCH_BACKOFF)
                } else {
                    backoff
                };
                RedialNext::RetryAfter(wait)
            }
        }
    }
}

const PROPERTY: &str = "discovery/redial-policy";

/// Applicable transitions in a decoder-v1 redial history.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct RedialReplaySummary {
    /// Attempts whose result reached the policy.
    pub scored: usize,
    /// Local limits that must preserve the remote failure budget.
    pub local_limits: usize,
    /// Registrations or healthy attempts that reset the policy.
    pub resets: usize,
    /// Connections that outlived a failed dial and upgraded its outcome.
    pub lagging_upgrades: usize,
    /// Terminal policy decisions.
    pub stops: usize,
}

/// Replay at most 512 bytes against the existing independent redial model.
///
/// Three header bytes select policy and backoff bounds. Each following triple
/// selects a start or dial result, registration presence and its lifetime.
/// Disagreements panic with `discovery/redial-policy`.
pub fn replay_redial_bytes(data: &[u8]) -> RedialReplaySummary {
    let Some(header) = data.get(..3) else {
        return RedialReplaySummary::default();
    };
    let attempts = usize::from(header[0] / 3 % 5) + 1;
    let policy = ModelPolicy {
        mode: match header[0] % 3 {
            0 => ModelMode::ConnectOnce {
                attempts,
                settle: Duration::from_secs(60),
            },
            1 => ModelMode::Maintain,
            _ => ModelMode::MaintainBounded { attempts },
        },
        initial: Duration::from_secs(u64::from(header[1] % 30) + 1),
        max: Duration::from_secs(u64::from(header[2] % 120) + 1),
    };
    let ops = data[3..data.len().min(512)].chunks(3).map(|chunk| {
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

pub(super) fn run(
    policy: ModelPolicy,
    ops: impl IntoIterator<Item = RedialOp>,
    mut after_attempt: impl FnMut(&mut RedialState, DialResult),
) -> RedialReplaySummary {
    let mut state = RedialState::new(policy.build());
    let mut model = RedialModel {
        policy,
        failures: 0,
    };
    let mut summary = RedialReplaySummary::default();
    for op in ops {
        let (registered, attempt, lagging) = match op {
            RedialOp::Start { registered, lasted } => (registered, None, lasted),
            RedialOp::Dial { result, lagging } => (false, Some(result), lagging),
        };
        let start = state.start(registered);
        let attempt = match (start, attempt) {
            (RedialStart::Dial, attempt) => {
                assert!(!registered, "{PROPERTY}: duplicate dial");
                attempt.unwrap_or(DialResult::Failed)
            }
            (RedialStart::ScoreRegistration, _) => {
                assert!(
                    registered && policy.maintains(),
                    "{PROPERTY}: registration scoring"
                );
                RedialModel::score(lagging.unwrap_or(0))
            }
            (RedialStart::AwaitRegisteredSettle(settle), _) => {
                let ModelMode::ConnectOnce {
                    settle: expected, ..
                } = policy.mode
                else {
                    panic!("{PROPERTY}: only connect-once awaits settle");
                };
                assert!(registered, "{PROPERTY}: missing registration");
                assert_eq!(settle, expected, "{PROPERTY}: settle deadline");
                if lagging.is_none() {
                    state.registration_changed();
                    model.failures = 0;
                    summary.resets += 1;
                    continue;
                }
                summary.stops += 1;
                break;
            }
        };
        assert_eq!(
            state.waits_for_lagging_deregistration(),
            policy.maintains(),
            "{PROPERTY}: lagging registration wait"
        );
        let attempt = match lagging {
            Some(lasted) if start == RedialStart::Dial && policy.maintains() => {
                let rescored = attempt.with_lagging_registration(Duration::from_secs(lasted));
                let expected = if RedialModel::score(lasted) == DialResult::Healthy {
                    DialResult::Healthy
                } else {
                    attempt
                };
                assert_eq!(rescored, expected, "{PROPERTY}: registration upgrade");
                summary.lagging_upgrades += usize::from(rescored != attempt);
                rescored
            }
            _ => attempt,
        };
        let next = state.record(attempt);
        assert_eq!(next, model.record(attempt), "{PROPERTY}: attempt outcome");
        summary.scored += 1;
        summary.local_limits += usize::from(attempt == DialResult::LocalLimit);
        summary.resets += usize::from(attempt == DialResult::Healthy && policy.maintains());
        if let Some(max) = policy.max_attempts() {
            assert!(model.failures <= max, "{PROPERTY}: bounded attempts");
        }
        after_attempt(&mut state, attempt);
        match next {
            RedialNext::Stop => {
                summary.stops += 1;
                break;
            }
            RedialNext::RedialNow => {}
            RedialNext::RetryAfter(wait) => assert!(
                wait >= policy.initial.min(policy.max),
                "{PROPERTY}: backoff floor"
            ),
        }
    }
    summary
}
