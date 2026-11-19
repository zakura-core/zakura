//! When to send a second request for a gossiped block that a first request has not delivered.
//!
//! The first request for a block goes to whichever idle peer announced it first, which from a
//! node far from most miners is usually a far peer. A closer peer often announces the same block
//! a few tens of milliseconds later. Asking it as well wins if its expected answer time, from
//! now, is clearly shorter than what is left of the first request's. Otherwise the second copy
//! would only cost bandwidth, because a legacy block request cannot be cancelled.
//!
//! The downloader shares the first dispatch with the peer set for each individual attempt, and
//! asks for a second request once. The peer set holds that request (see
//! [`ParkedSecondRequest`]) and decides again only when an input to the decision changes:
//!
//! - another peer announces the block;
//! - an announcer becomes a better source: its connection is free again, an unanswered or
//!   missed block mark clears, or its expected answer time is measured again;
//! - the first request becomes overdue, and later stalls (see [`decision_times`]).
//!
//! Between those events a candidate can only look worse, because the time left on the first
//! request only shrinks.

use std::{future::Future, pin::Pin, task::Context, time::Duration};

use futures::future::BoxFuture;
use tokio::{
    sync::oneshot,
    time::{sleep_until, Instant, Sleep},
};
use zakura_chain::block;

use crate::{BlockHedge, BoxError, Response};

/// The least a candidate must be expected to beat the first request by, so that small errors
/// in the expected answer times do not send second requests that lose.
const MIN_MARGIN: Duration = Duration::from_millis(20);

/// The share of the first peer's expected answer time a candidate must also beat it by.
const MARGIN_SHARE: u32 = 4;

/// How long past its expected answer time a first request may run before it counts as stalled.
///
/// Measured from the expected time rather than from the request, so a far peer's normal download
/// is not half a stall. A candidate clearly faster than the first peer is already asked by the
/// prediction, so a stall only catches a request that has gone wrong. On the mainnet experiment
/// nodes, first requests passed this point on 3.0% of blocks in Singapore and 0.1-0.3% in
/// Amsterdam and New York; twice the expected time would have caught 2.6-13% of them, mostly
/// nearby peers a few milliseconds late.
const STALL_SLACK: Duration = Duration::from_millis(500);

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

/// How long after the first request was sent waiting alone changes the decision: when the
/// first request becomes overdue, and when it counts as stalled.
///
/// Before the first, the time left on the first request only shrinks, so a candidate that does
/// not qualify now will not qualify by waiting. Between the two, nothing changes with time.
fn decision_times(first_score: Duration) -> [Duration; 2] {
    [first_score, first_score + STALL_SLACK]
}

/// Decides whether to send a second request for a block.
///
/// - `first_score` is the first peer's expected answer time when it was asked. A first peer
///   with no measured answers gets no second request, because there is nothing to compare a
///   candidate with;
/// - `waited` is how long the first request has been outstanding;
/// - `best_candidate_score` is the expected answer time of the fastest eligible, measured second announcer.
///
/// A candidate is compared with what is left of the first peer's expected time, or, once the first
/// request is overdue, with all of it (see the comment in the body for why).
///
/// Returns `None` when the first request is still expected to arrive sooner, or when no other
/// announcer is known to be faster and the first request has not stalled.
pub(super) fn second_request_reason(
    first_score: Duration,
    waited: Duration,
    best_candidate_score: Duration,
) -> Option<SecondRequestReason> {
    let [overdue_after, stalled_after] = decision_times(first_score);
    if waited >= stalled_after {
        return Some(SecondRequestReason::Stalled);
    }

    // An overdue first request is probably fetching a block larger than the ones its score was
    // measured on. A legacy `inv` carries no block size, and the first peer's score comes mostly
    // from pings, so it covers the peer's distance but not the time to transfer a large block.
    // Counting down to zero would then refuse every candidate until the stall point.
    //
    // This came from a load test on the asia-scored mainnet node (Singapore), with blocks of
    // about 1.5 MB. At heights 3,586,323, 3,586,329 and 3,586,330 the first peer was expected
    // to answer in 242 ms but took 388-605 ms. Same-city peers expected to answer in 2-5 ms
    // announced 193-196 ms after the first request, when less than the margin was left, so no
    // second request was sent. Once the first request is overdue, a candidate is compared with
    // the first peer's whole expected time instead, as if the first request were starting over.
    let remaining = if waited >= overdue_after {
        first_score
    } else {
        first_score - waited
    };
    let margin = MIN_MARGIN.max(first_score / MARGIN_SHARE);
    (best_candidate_score + margin < remaining).then_some(SecondRequestReason::Predicted)
}

/// The response to a second request that was sent to a peer.
pub(super) type SecondResponse = BoxFuture<'static, Result<Response, BoxError>>;

/// A download's request for a second copy of its block, held by the peer set until an announcer
/// qualifies or the download stops accepting a second request.
pub(super) struct ParkedSecondRequest {
    hash: block::Hash,
    attempt: BlockHedge,
    /// Receives the response of the second request, once one is sent. Dropping it tells the
    /// download that no peer was asked.
    reply: oneshot::Sender<SecondResponse>,
    /// Fires at the next time waiting alone changes the decision, or when the window ends.
    timer: Pin<Box<Sleep>>,
}

impl ParkedSecondRequest {
    /// Holds a second request for `hash`, and returns it with the receiver of its response.
    pub(super) fn new(
        hash: block::Hash,
        attempt: BlockHedge,
    ) -> (Self, oneshot::Receiver<SecondResponse>) {
        let (reply, response) = oneshot::channel();
        let timer = Box::pin(sleep_until(attempt.window_end()));
        let parked = Self {
            hash,
            attempt,
            reply,
            timer,
        };
        (parked, response)
    }

    /// The block to request.
    pub(super) fn hash(&self) -> block::Hash {
        self.hash
    }

    /// The download this request belongs to.
    pub(super) fn attempt(&self) -> &BlockHedge {
        &self.attempt
    }

    /// Returns whether the download still accepts a second request: its window is open and it
    /// is still waiting for the response.
    ///
    /// While it does, registers the task polling with `cx` to be woken when the download stops
    /// waiting.
    pub(super) fn poll_open(&mut self, cx: &mut Context<'_>) -> bool {
        self.attempt.elapsed_if_open().is_some() && self.reply.poll_closed(cx).is_pending()
    }

    /// Hands the download the response of the second request that was just sent.
    pub(super) fn send(self, response: SecondResponse) {
        // A download that stopped waiting since `poll_open` no longer needs the response.
        let _ = self.reply.send(response);
    }

    /// Registers the task polling with `cx` to be woken at the next time waiting alone changes
    /// the decision (see [`decision_times`]), or when the window ends.
    pub(super) fn poll_next_decision(&mut self, cx: &mut Context<'_>) {
        self.timer.as_mut().reset(next_decision_at(&self.attempt));
        if self.timer.as_mut().poll(cx).is_ready() {
            // That time arrived while the request was being considered, so consider it again.
            cx.waker().wake_by_ref();
        }
    }
}

/// The next time waiting alone changes the decision for `attempt`, or the end of its window
/// when no such time is left, or the first request has not been sent yet.
fn next_decision_at(attempt: &BlockHedge) -> Instant {
    let window_end = attempt.window_end();
    let Some(first) = attempt.first_dispatch() else {
        return window_end;
    };
    let Some(first_score) = first.score else {
        return window_end;
    };
    let now = Instant::now();
    decision_times(first_score)
        .into_iter()
        .map(|after| first.at + after)
        .find(|at| *at > now)
        .map_or(window_end, |at| at.min(window_end))
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
            second_request_reason(ms(230), ms(70), ms(50)),
            Some(SecondRequestReason::Predicted)
        );
    }

    #[test]
    fn an_announcer_that_is_not_clearly_faster_is_not_asked() {
        // 160 ms are left, and the candidate's 120 ms plus the 57 ms margin is more.
        assert_eq!(second_request_reason(ms(230), ms(70), ms(120)), None);
        // A fast first peer leaves little to win.
        assert_eq!(second_request_reason(ms(40), ms(20), ms(5)), None);
    }

    #[test]
    fn the_margin_is_at_least_twenty_milliseconds() {
        // A quarter of 40 ms is 10 ms, so the 20 ms minimum applies: 5 + 20 is not under the
        // 25 ms left after 15 ms, but is under the 26 ms left after 14 ms.
        assert_eq!(second_request_reason(ms(40), ms(15), ms(5)), None);
        assert_eq!(
            second_request_reason(ms(40), ms(14), ms(5)),
            Some(SecondRequestReason::Predicted)
        );
    }

    #[test]
    fn an_overdue_first_request_is_compared_with_its_whole_expected_time() {
        // From the asia-scored load test: a far peer expected in 242 ms was still sending a
        // 1.5 MB block when same-city peers expected in 3 ms announced.
        assert_eq!(
            second_request_reason(ms(242), ms(195), ms(3)),
            None,
            "before it is overdue, only 47 ms are left, less than the 3 ms plus a 60 ms margin"
        );
        assert_eq!(
            second_request_reason(ms(242), ms(242), ms(3)),
            Some(SecondRequestReason::Predicted),
            "once overdue, 3 ms plus the margin is well under the whole 242 ms"
        );
        assert_eq!(
            second_request_reason(ms(242), ms(300), ms(190)),
            None,
            "a candidate about as far away is still not asked"
        );
    }

    /// A held second request is reconsidered at the decision times instead of on a short
    /// interval, so waiting must not turn a refusal into a request at any other time.
    #[test]
    fn waiting_alone_changes_the_decision_only_at_the_decision_times() {
        let first_score = ms(242);
        assert_eq!(decision_times(first_score), [ms(242), ms(742)]);

        for candidate in [3, 50, 120, 190, 400] {
            let mut previous = second_request_reason(first_score, ms(0), ms(candidate));
            for waited in 1..=800 {
                let decision = second_request_reason(first_score, ms(waited), ms(candidate));
                if previous.is_none() && decision.is_some() {
                    assert!(
                        decision_times(first_score).contains(&ms(waited)),
                        "a {candidate} ms candidate became eligible after {waited} ms"
                    );
                }
                previous = decision;
            }
        }
    }

    #[test]
    fn stall_threshold_is_inclusive_for_measured_candidates() {
        // A stall is 500 ms past the expected time, however fast or slow the first peer.
        assert_eq!(second_request_reason(ms(230), ms(729), ms(900)), None);
        assert_eq!(
            second_request_reason(ms(230), ms(730), ms(900)),
            Some(SecondRequestReason::Stalled)
        );
        assert_eq!(second_request_reason(ms(5), ms(504), ms(900)), None);
        assert_eq!(
            second_request_reason(ms(5), ms(505), ms(900)),
            Some(SecondRequestReason::Stalled)
        );
        assert_eq!(second_request_reason(ms(800), ms(1299), ms(900)), None);
        assert_eq!(
            second_request_reason(ms(800), ms(1300), ms(900)),
            Some(SecondRequestReason::Stalled)
        );
    }
}
