//! When to send a second request for a gossiped block that a first request has not delivered.
//!
//! The first request for a block goes to whichever idle peer announced it first, which from a
//! node far from most miners is usually a far peer. A closer peer often announces the same block
//! a few tens of milliseconds later. Asking it as well wins if its expected answer time, from
//! now, is clearly shorter than what is left of the first request's. Otherwise the second copy
//! would only cost bandwidth, because a legacy block request cannot be cancelled.
//!
//! [`FirstBlockRequests`] remembers who each first request went to and how fast that peer was
//! expected to answer, and [`second_request_reason`] applies the rule.

use std::{
    collections::HashMap,
    hash::Hash,
    time::{Duration, Instant},
};

use zakura_chain::block;

/// The least a candidate must be expected to beat the first request by, so that small errors
/// in the expected answer times do not send second requests that lose.
const MIN_MARGIN: Duration = Duration::from_millis(20);

/// The share of the first peer's expected answer time a candidate must also beat it by.
const MARGIN_SHARE: u32 = 4;

/// How long past its expected answer time a first request may run before it counts as stalled.
const STALL_SLACK: Duration = Duration::from_millis(500);

/// The expected answer time assumed for a first peer whose answers have not been measured: a
/// typical download from a far peer. Assuming nothing would let any measured announcer, however
/// far, replace it.
const UNSCORED_FIRST_SCORE: Duration = Duration::from_millis(250);

/// How long a first request is remembered. Block downloads time out well before this.
const FIRST_REQUEST_LIFETIME: Duration = Duration::from_secs(60);

/// The most first requests remembered at once. The block downloader limits how many gossiped
/// blocks it fetches at a time to far fewer.
const MAX_FIRST_REQUESTS: usize = 64;

/// Why a second request was sent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SecondRequestReason {
    /// Another announcer was expected to deliver the block clearly before the first request.
    Predicted,
    /// The first request ran well past its expected answer time.
    Stalled,
}

impl SecondRequestReason {
    /// The name written to the legacy peer trace.
    pub(super) fn label(self) -> &'static str {
        match self {
            SecondRequestReason::Predicted => "predicted",
            SecondRequestReason::Stalled => "stalled",
        }
    }
}

/// Decides whether to send a second request for a block.
///
/// - `first_score` is the first peer's expected answer time when it was asked, if measured, or
///   else [`UNSCORED_FIRST_SCORE`];
/// - `waited` is how long the first request has been outstanding;
/// - `best_candidate_score` is the expected answer time of the best other idle announcer, if
///   there is one and it has been measured.
///
/// A candidate is compared with what is left of the first peer's expected time, or, once the first
/// request is overdue, with all of it (see the comment in the body for why).
///
/// Returns `None` when the first request is still expected to arrive sooner, or when no other
/// announcer is known to be faster and the first request has not stalled.
pub(super) fn second_request_reason(
    first_score: Option<Duration>,
    waited: Duration,
    best_candidate_score: Option<Duration>,
) -> Option<SecondRequestReason> {
    let first_score = first_score.unwrap_or(UNSCORED_FIRST_SCORE);
    let stall_after = (first_score * 2).max(first_score + STALL_SLACK);
    if waited >= stall_after {
        return Some(SecondRequestReason::Stalled);
    }

    let candidate = best_candidate_score?;
    // An overdue first request is probably fetching a large block. A legacy `inv` carries no
    // block size, and the first peer's score comes from pings, transaction replies and small
    // blocks, so it covers the peer's distance but not the time to transfer a large block.
    // Counting down to zero would then refuse every candidate until the stall point.
    //
    // This came from a load test on the asia-scored mainnet node (Singapore), with blocks of
    // about 1.5 MB. At heights 3,586,323, 3,586,329 and 3,586,330 the first peer was expected
    // to answer in 242 ms but took 388-605 ms. Same-city peers expected to answer in 2-5 ms
    // announced 193-196 ms after the first request, when less than the margin was left, so no
    // second request was sent. Once the first request is overdue, a candidate is compared with
    // the first peer's whole expected time instead, as if the first request were starting over.
    let remaining = if waited >= first_score {
        first_score
    } else {
        first_score - waited
    };
    let margin = MIN_MARGIN.max(first_score / MARGIN_SHARE);
    (candidate + margin < remaining).then_some(SecondRequestReason::Predicted)
}

/// A first request for a gossiped block.
#[derive(Clone, Copy, Debug)]
pub(super) struct FirstBlockRequest<K> {
    /// The peer it went to.
    pub(super) peer: K,
    /// That peer's expected answer time when it was asked, if measured.
    pub(super) score: Option<Duration>,
    /// When it was sent.
    sent_at: Instant,
    /// Whether a second request has already been sent for the same block.
    pub(super) second_sent: bool,
}

/// The recent first requests for gossiped blocks, by block hash.
#[derive(Debug)]
pub(super) struct FirstBlockRequests<K> {
    requests: HashMap<block::Hash, FirstBlockRequest<K>>,
}

impl<K> Default for FirstBlockRequests<K> {
    fn default() -> Self {
        Self {
            requests: HashMap::new(),
        }
    }
}

impl<K: Copy + Eq + Hash> FirstBlockRequests<K> {
    /// Remembers that `hash` was just requested from `peer`, replacing any earlier request for
    /// the same block. Forgets requests older than [`FIRST_REQUEST_LIFETIME`], and the oldest
    /// one if [`MAX_FIRST_REQUESTS`] are still remembered.
    pub(super) fn insert(
        &mut self,
        hash: block::Hash,
        peer: K,
        score: Option<Duration>,
        now: Instant,
    ) {
        self.requests.retain(|_, request| {
            now.saturating_duration_since(request.sent_at) < FIRST_REQUEST_LIFETIME
        });
        if self.requests.len() >= MAX_FIRST_REQUESTS && !self.requests.contains_key(&hash) {
            let oldest = self
                .requests
                .iter()
                .min_by_key(|(_, request)| request.sent_at)
                .map(|(hash, _)| *hash);
            if let Some(oldest) = oldest {
                self.requests.remove(&oldest);
            }
        }
        self.requests.insert(
            hash,
            FirstBlockRequest {
                peer,
                score,
                sent_at: now,
                second_sent: false,
            },
        );
    }

    /// The first request for `hash`, if it is still remembered.
    pub(super) fn get_mut(&mut self, hash: &block::Hash) -> Option<&mut FirstBlockRequest<K>> {
        self.requests.get_mut(hash)
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.requests.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    #[test]
    fn a_clearly_faster_announcer_is_asked() {
        // The first peer should answer in 230 ms; 70 ms in, a peer expected to take 50 ms
        // would deliver about 110 ms sooner.
        assert_eq!(
            second_request_reason(Some(ms(230)), ms(70), Some(ms(50))),
            Some(SecondRequestReason::Predicted)
        );
    }

    #[test]
    fn an_announcer_that_is_not_clearly_faster_is_not_asked() {
        // 160 ms are left, and the candidate's 120 ms plus the 57 ms margin is more.
        assert_eq!(
            second_request_reason(Some(ms(230)), ms(70), Some(ms(120))),
            None
        );
        // A fast first peer leaves little to win.
        assert_eq!(
            second_request_reason(Some(ms(40)), ms(20), Some(ms(5))),
            None
        );
    }

    #[test]
    fn the_margin_is_at_least_twenty_milliseconds() {
        // A quarter of 40 ms is 10 ms, so the 20 ms minimum applies: 5 + 20 is not under the
        // 25 ms left after 15 ms, but is under the 26 ms left after 14 ms.
        assert_eq!(
            second_request_reason(Some(ms(40)), ms(15), Some(ms(5))),
            None
        );
        assert_eq!(
            second_request_reason(Some(ms(40)), ms(14), Some(ms(5))),
            Some(SecondRequestReason::Predicted)
        );
    }

    #[test]
    fn an_overdue_first_request_is_compared_with_its_whole_expected_time() {
        // From the asia-scored load test: a far peer expected in 242 ms was still sending a
        // 1.5 MB block when same-city peers expected in 3 ms announced.
        assert_eq!(
            second_request_reason(Some(ms(242)), ms(195), Some(ms(3))),
            None,
            "before it is overdue, only 47 ms are left, less than the 3 ms plus a 60 ms margin"
        );
        assert_eq!(
            second_request_reason(Some(ms(242)), ms(242), Some(ms(3))),
            Some(SecondRequestReason::Predicted),
            "once overdue, 3 ms plus the margin is well under the whole 242 ms"
        );
        assert_eq!(
            second_request_reason(Some(ms(242)), ms(300), Some(ms(190))),
            None,
            "a candidate about as far away is still not asked"
        );
    }

    #[test]
    fn without_a_measured_candidate_only_a_stall_sends_a_second_request() {
        assert_eq!(second_request_reason(Some(ms(230)), ms(100), None), None);
        // A stall is the larger of twice the expected time and the expected time plus 500 ms.
        assert_eq!(second_request_reason(Some(ms(230)), ms(729), None), None);
        assert_eq!(
            second_request_reason(Some(ms(230)), ms(730), None),
            Some(SecondRequestReason::Stalled)
        );
        assert_eq!(
            second_request_reason(Some(ms(800)), ms(1600), None),
            Some(SecondRequestReason::Stalled)
        );
    }

    #[test]
    fn an_unmeasured_first_peer_is_treated_as_a_typical_far_peer() {
        // It loses to a clearly faster measured announcer...
        assert_eq!(
            second_request_reason(None, ms(10), Some(ms(50))),
            Some(SecondRequestReason::Predicted)
        );
        // ...but not to one about as far away.
        assert_eq!(second_request_reason(None, ms(10), Some(ms(190))), None);
        assert_eq!(second_request_reason(None, ms(10), None), None);
        // Its stall point is that of a 250 ms peer: 250 ms plus 500 ms.
        assert_eq!(second_request_reason(None, ms(749), None), None);
        assert_eq!(
            second_request_reason(None, ms(750), None),
            Some(SecondRequestReason::Stalled)
        );
    }

    #[test]
    fn first_requests_are_forgotten_when_old_or_too_many() {
        let start = Instant::now();
        let hash = |n: u8| block::Hash([n; 32]);
        let mut requests = FirstBlockRequests::default();

        requests.insert(hash(0), 0u8, None, start);
        requests.insert(hash(1), 1u8, None, start + FIRST_REQUEST_LIFETIME);
        assert!(
            requests.get_mut(&hash(0)).is_none(),
            "a request older than its lifetime"
        );

        let later = start + FIRST_REQUEST_LIFETIME;
        for n in 2..=(MAX_FIRST_REQUESTS as u8 + 1) {
            requests.insert(hash(n), n, None, later + Duration::from_millis(n.into()));
        }
        assert_eq!(requests.len(), MAX_FIRST_REQUESTS);
        assert!(
            requests.get_mut(&hash(1)).is_none(),
            "the oldest request makes room"
        );
        assert_eq!(
            requests.get_mut(&hash(2)).map(|request| request.peer),
            Some(2)
        );
    }
}
