//! Redial policy for one supervised peer: its consecutive failures, its
//! backoff, and the next step after each attempt.
//!
//! The redial loop does the IO: it dials, watches the supervisor's
//! registrations, and sleeps. These transitions take the observed
//! registration and the attempt result as arguments.

use std::time::Duration;

use super::{DialResult, RedialPolicy, ZAKURA_REDIAL_HEALTHY_CONNECTION};
use crate::zakura::ZAKURA_ALPN_MISMATCH_BACKOFF;

/// What the redial loop does before its next attempt.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum RedialStart {
    /// Dial the peer.
    Dial,
    /// The peer is already registered and the policy connects once. Wait up
    /// to this long for the registration to change, and stop if it doesn't.
    AwaitRegisteredSettle(Duration),
    /// The peer is already registered and the policy maintains it. Wait for
    /// the peer to deregister, then score the registration as an attempt.
    ScoreRegistration,
}

/// What the redial loop does after an attempt.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum RedialNext {
    /// Stop supervising the peer.
    Stop,
    /// Start the next attempt at once.
    RedialNow,
    /// Sleep this long, waking early if the peer registers from another
    /// connection, then start the next attempt.
    RetryAfter(Duration),
}

impl DialResult {
    /// Scores a connection or registration that lasted `lasted`: healthy once
    /// it reaches [`ZAKURA_REDIAL_HEALTHY_CONNECTION`], failed before that.
    pub(super) fn lasting(lasted: Duration) -> Self {
        if lasted >= ZAKURA_REDIAL_HEALTHY_CONNECTION {
            Self::Healthy
        } else {
            Self::Failed
        }
    }

    /// Rescores this attempt after the peer's registration outlived it.
    ///
    /// A registration that lasted `lasted` long enough to be healthy upgrades
    /// the attempt to healthy; a shorter one leaves the attempt's result.
    pub(super) fn with_lagging_registration(self, lasted: Duration) -> Self {
        match Self::lasting(lasted) {
            Self::Healthy => Self::Healthy,
            _ => self,
        }
    }
}

/// One supervised peer's redial state.
#[derive(Debug)]
pub(super) struct RedialState {
    policy: RedialPolicy,
    /// The wait before the next redial after a failure.
    backoff: Duration,
    /// Consecutive failed attempts, counted against `policy.max_attempts`.
    failures: usize,
}

impl RedialState {
    /// Builds the state of a peer that has not been dialed yet.
    pub(super) fn new(policy: RedialPolicy) -> Self {
        Self {
            policy,
            backoff: policy.initial_backoff,
            failures: 0,
        }
    }

    /// Chooses the next attempt, given whether the peer is already registered
    /// (it may have dialed us first).
    ///
    /// Skipping a registered peer's dial keeps the two directions from
    /// churning duplicate connections.
    pub(super) fn start(&self, peer_registered: bool) -> RedialStart {
        if !peer_registered {
            RedialStart::Dial
        } else if self.policy.redial_after_drop {
            RedialStart::ScoreRegistration
        } else {
            RedialStart::AwaitRegisteredSettle(self.policy.registered_settle_timeout)
        }
    }

    /// Restarts the policy after a connect-once peer's registration changed
    /// while the loop waited for it to settle, so the next dial runs promptly
    /// with the full attempt budget.
    pub(super) fn registration_changed(&mut self) {
        self.reset();
    }

    /// Whether the loop waits for a registration that outlives an attempt to
    /// end before it scores the attempt.
    ///
    /// The connection deregistration can land after the dial future returns.
    /// Waiting keeps the residual registration from becoming a second attempt.
    pub(super) fn waits_for_lagging_deregistration(&self) -> bool {
        self.policy.redial_after_drop
    }

    /// Applies one attempt's result and chooses the next step.
    ///
    /// A healthy connection stops a connect-once policy and resets a
    /// maintained one. A local limit retries after the initial backoff
    /// without counting an attempt or growing the backoff. A failure or an
    /// ALPN mismatch counts an attempt, stops at the policy's maximum, and
    /// otherwise waits out the backoff, which then doubles up to its maximum.
    /// An ALPN mismatch waits at least [`ZAKURA_ALPN_MISMATCH_BACKOFF`].
    pub(super) fn record(&mut self, attempt: DialResult) -> RedialNext {
        match attempt {
            DialResult::Healthy => {
                if !self.policy.redial_after_drop {
                    return RedialNext::Stop;
                }
                self.reset();
                return RedialNext::RedialNow;
            }
            DialResult::LocalLimit => return RedialNext::RetryAfter(self.policy.initial_backoff),
            DialResult::Failed | DialResult::AlpnMismatch => {}
        }

        self.failures += 1;
        if self
            .policy
            .max_attempts
            .is_some_and(|max| self.failures >= max)
        {
            return RedialNext::Stop;
        }

        let wait = if attempt == DialResult::AlpnMismatch {
            self.backoff.max(ZAKURA_ALPN_MISMATCH_BACKOFF)
        } else {
            self.backoff
        };
        self.backoff = self.backoff.saturating_mul(2).min(self.policy.max_backoff);
        RedialNext::RetryAfter(wait)
    }

    fn reset(&mut self) {
        self.backoff = self.policy.initial_backoff;
        self.failures = 0;
    }
}
