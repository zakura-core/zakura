//! The action-rate limiter that `BoundedAdversary` assumes (M5-Q9).
//!
//! Each adversarial peer may take at most [`ADVERSARY_RATE`] actions in each
//! whole virtual second of the run, counted when the `Plan` picks one.
//! [`super::PeerControl`] stops offering an exhausted peer's actions and
//! publishes the next second as a boundary, so time can move there. Since
//! a peer holds at most one queued action, no backlog can pass the budget.

use std::time::Duration;

use zakura_scheduler::SimInstant;

/// The actions an adversarial peer may take per virtual second: `r` in
/// `BoundedAdversary{r}`, which `header-sync/wire` declares.
pub const ADVERSARY_RATE: u32 = 2;

/// One peer's budget in the current second.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RateLimit {
    rate: u32,
    /// The whole second of the run the last action fell in.
    second: u64,
    /// Actions taken in it.
    taken: u32,
}

/// The whole second of the run `now` falls in.
fn second(now: SimInstant) -> u64 {
    now.since_start().as_secs()
}

impl RateLimit {
    /// A budget of `rate` actions per second.
    pub(crate) fn new(rate: u32) -> Self {
        RateLimit {
            rate,
            second: 0,
            taken: 0,
        }
    }

    /// Whether the peer may act at `now`.
    pub(crate) fn allows(&self, now: SimInstant) -> bool {
        second(now) != self.second || self.taken < self.rate
    }

    /// When an exhausted budget opens again: the next whole second.
    pub(crate) fn opens(&self) -> SimInstant {
        SimInstant::from_start(Duration::from_secs(self.second.saturating_add(1)))
    }

    /// Charges an action picked at `now`.
    pub(crate) fn charge(&mut self, now: SimInstant) {
        let at = second(now);
        if at != self.second {
            self.second = at;
            self.taken = 0;
        }
        self.taken += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `ms` milliseconds into the run.
    fn at(ms: u64) -> SimInstant {
        SimInstant::from_start(Duration::from_millis(ms))
    }

    /// Two actions fill a second, and the next opens at the following
    /// whole second, however late in the second the first came.
    #[test]
    fn a_second_holds_the_rate() {
        let mut limit = RateLimit::new(2);
        assert!(limit.allows(at(1_900)));
        limit.charge(at(1_900));
        limit.charge(at(1_950));
        assert!(!limit.allows(at(1_999)));
        assert_eq!(limit.opens(), at(2_000));
        assert!(limit.allows(at(2_000)));
        limit.charge(at(2_000));
        assert!(limit.allows(at(2_000)));
    }
}
