//! Machine tests for [`RedialState`]: worked scenarios and a proptest against
//! an independent model.
//!
//! The model restates the documented rules. It never calls the code it
//! checks.

use std::time::Duration;

use proptest::prelude::*;

use super::state::{RedialNext, RedialStart, RedialState};
use super::{DialResult, RedialPolicy};
use crate::zakura::ZAKURA_ALPN_MISMATCH_BACKOFF;

const HEALTHY: Duration = Duration::from_secs(60);

// Worked scenario: local limits retry at the initial backoff and leave the
// remote-failure budget and the backoff growth untouched.
#[test]
fn repeated_local_limits_keep_the_remote_failure_budget() {
    let initial = Duration::from_secs(1);
    let mut state = RedialState::new(RedialPolicy::maintain_bounded(
        initial,
        Duration::from_secs(30),
        2,
    ));

    for _ in 0..10 {
        assert_eq!(state.start(false), RedialStart::Dial);
        assert_eq!(
            state.record(DialResult::LocalLimit),
            RedialNext::RetryAfter(initial)
        );
    }
    // The first counted failure still waits the initial backoff.
    assert_eq!(
        state.record(DialResult::Failed),
        RedialNext::RetryAfter(initial)
    );
    assert_eq!(
        state.record(DialResult::LocalLimit),
        RedialNext::RetryAfter(initial)
    );
    // The second counted failure exhausts the budget of two.
    assert_eq!(state.record(DialResult::Failed), RedialNext::Stop);
}

// A healthy connection keeps its outcome while its deregistration lags, and
// a lasting registration upgrades a failed dial.
#[test]
fn lagging_registration_only_upgrades_an_attempt() {
    let brief = Duration::from_millis(1);
    assert_eq!(
        DialResult::Healthy.with_lagging_registration(brief),
        DialResult::Healthy
    );
    assert_eq!(
        DialResult::Failed.with_lagging_registration(brief),
        DialResult::Failed
    );
    assert_eq!(
        DialResult::Failed.with_lagging_registration(HEALTHY),
        DialResult::Healthy
    );
    assert_eq!(
        DialResult::AlpnMismatch.with_lagging_registration(HEALTHY - brief),
        DialResult::AlpnMismatch
    );
}

#[derive(Copy, Clone, Debug)]
enum ModelMode {
    ConnectOnce { attempts: usize, settle: Duration },
    Maintain,
    MaintainBounded { attempts: usize },
}

#[derive(Copy, Clone, Debug)]
struct ModelPolicy {
    mode: ModelMode,
    initial: Duration,
    max: Duration,
}

impl ModelPolicy {
    fn build(self) -> RedialPolicy {
        match self.mode {
            ModelMode::ConnectOnce { attempts, settle } => {
                RedialPolicy::connect_once_with_registered_settle(
                    self.initial,
                    self.max,
                    attempts,
                    settle,
                )
            }
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

fn model_policy() -> impl Strategy<Value = ModelPolicy> {
    let mode = prop_oneof![
        (1usize..5, 1u64..120).prop_map(|(attempts, settle)| ModelMode::ConnectOnce {
            attempts,
            settle: Duration::from_secs(settle),
        }),
        Just(ModelMode::Maintain),
        (1usize..5).prop_map(|attempts| ModelMode::MaintainBounded { attempts }),
    ];
    (mode, 1u64..30, 1u64..120).prop_map(|(mode, initial, max)| ModelPolicy {
        mode,
        initial: Duration::from_secs(initial),
        max: Duration::from_secs(max),
    })
}

#[derive(Clone, Debug)]
enum RedialOp {
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

fn redial_op() -> impl Strategy<Value = RedialOp> {
    let result = prop_oneof![
        Just(DialResult::Healthy),
        Just(DialResult::Failed),
        Just(DialResult::AlpnMismatch),
        Just(DialResult::LocalLimit),
    ];
    let lasted = prop::option::of(0u64..120);
    prop_oneof![
        (any::<bool>(), lasted.clone())
            .prop_map(|(registered, lasted)| RedialOp::Start { registered, lasted }),
        (result, lasted).prop_map(|(result, lagging)| RedialOp::Dial { result, lagging }),
    ]
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

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Every step matches the model: start choices, healthy-duration scoring,
    // lagging deregistration, the attempt budget, the backoff, and the ALPN
    // floor. Local limits never consume the budget.
    #[test]
    fn redial_state_matches_independent_model(
        policy in model_policy(),
        ops in prop::collection::vec(redial_op(), 1..64),
    ) {
        let mut state = RedialState::new(policy.build());
        let mut model = RedialModel { policy, failures: 0 };

        for op in ops {
            let (registered, attempt, lagging) = match op {
                RedialOp::Start { registered, lasted } => (registered, None, lasted),
                RedialOp::Dial { result, lagging } => (false, Some(result), lagging),
            };
            let start = state.start(registered);
            let attempt = match (start, attempt) {
                (RedialStart::Dial, attempt) => {
                    prop_assert!(!registered);
                    attempt.unwrap_or(DialResult::Failed)
                }
                (RedialStart::ScoreRegistration, _) => {
                    prop_assert!(registered && policy.maintains());
                    // A registration that lasted `None` never ends here; use
                    // a brief one.
                    RedialModel::score(lagging.unwrap_or(0))
                }
                (RedialStart::AwaitRegisteredSettle(settle), _) => {
                    let ModelMode::ConnectOnce { settle: expected, .. } = policy.mode else {
                        return Err(TestCaseError::fail("only connect-once awaits settle"));
                    };
                    prop_assert!(registered);
                    prop_assert_eq!(settle, expected);
                    if lagging.is_none() {
                        // The registration changed: the policy restarts.
                        state.registration_changed();
                        model.failures = 0;
                        continue;
                    }
                    // The registration settled: the loop stops.
                    break;
                }
            };

            prop_assert_eq!(state.waits_for_lagging_deregistration(), policy.maintains());
            let attempt = match lagging {
                Some(lasted) if start == RedialStart::Dial && policy.maintains() => {
                    let rescored = attempt.with_lagging_registration(Duration::from_secs(lasted));
                    let expected = if RedialModel::score(lasted) == DialResult::Healthy {
                        DialResult::Healthy
                    } else {
                        attempt
                    };
                    prop_assert_eq!(rescored, expected);
                    rescored
                }
                _ => attempt,
            };

            let next = state.record(attempt);
            prop_assert_eq!(next, model.record(attempt));
            if let Some(max) = policy.max_attempts() {
                prop_assert!(model.failures <= max, "the budget bounds counted failures");
            }
            match next {
                RedialNext::Stop => break,
                RedialNext::RedialNow => {}
                RedialNext::RetryAfter(wait) => prop_assert!(wait >= policy.initial.min(policy.max)),
            }
        }
    }
}
