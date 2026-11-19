//! When to send a second request for a gossiped block that a first request has not delivered.
//!
//! The first request for a block goes to whichever idle peer announced it first, which from a
//! node far from most miners is usually a far peer. A closer peer often announces the same block
//! a few tens of milliseconds later. Asking it as well wins if its expected answer time, from
//! now, is clearly shorter than what is left of the first request's. Otherwise the second copy
//! would only cost bandwidth, because a legacy block request cannot be cancelled.
//!
//! A legacy `inv` carries no block size, so when the first request is sent every expected answer
//! time is a peer's round trip. The size is learned from the first peer's reply: the header of
//! its `block` message states the length, one round trip after the request and before the body
//! has arrived. From then on the expected answer times compared here are the ones for a block
//! of that size, from each peer's round trip and its record on large blocks. The decision
//! itself does not change.
//!
//! The downloader shares the first dispatch with the peer set for each individual attempt, and
//! asks for a second request once. The peer set holds that request (see
//! [`HeldSecondRequest`]) and decides again only when an input to the decision changes:
//!
//! - the first reply reveals the requested block's size;
//! - another peer announces the block;
//! - an announcer becomes a better source: its connection is free again, which is also when
//!   an unanswered block mark clears, a missed block mark clears or expires, or its expected
//!   answer time is measured again;
//! - the first request becomes overdue, and later stalls (see [`decision_times`]).
//!
//! Between those events a candidate can only look worse, because the time left on the first
//! request only shrinks.

use std::{task::Context, time::Duration};

use futures::future::BoxFuture;
use tokio::{sync::oneshot, time::Instant};
use zakura_chain::block;

use crate::{BlockHedge, BoxError, NoFasterBlockSource, Response};

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
/// - `first_score` is the first peer's expected answer time for the block, from its measurements
///   when it was asked. A first peer with no measured answers gets no second request, because
///   there is nothing to compare a candidate with;
/// - `waited` is how long the first request has been outstanding;
/// - `best_candidate_score` is the expected answer time, for the same block, of the fastest
///   eligible, measured second announcer.
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
    // measured on. A legacy `inv` carries no block size, and until the reply's header arrives
    // the first peer's score is its round trip, so it covers the peer's distance but not the
    // time to transfer a large block. Counting down to zero would then refuse every candidate
    // until the stall point.
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

/// The result a download awaits for its second request.
pub(super) type SecondResponse = BoxFuture<'static, Result<Response, BoxError>>;

/// A download's request for a second copy of its block, held by the peer set until an announcer
/// qualifies or the download stops accepting a second request.
pub(super) struct HeldSecondRequest {
    hash: block::Hash,
    attempt: BlockHedge,
    /// Hands the peer's response future to the download once the request is sent.
    reply: oneshot::Sender<SecondResponse>,
}

impl HeldSecondRequest {
    /// Returns the held request and a future that resolves to the peer's response, or
    /// `NoFasterBlockSource` if the request is dropped before dispatch.
    pub(super) fn new(hash: block::Hash, attempt: BlockHedge) -> (Self, SecondResponse) {
        let (reply, response) = oneshot::channel();
        let held = Self {
            hash,
            attempt,
            reply,
        };
        let response = Box::pin(async move {
            let dispatched = response.await.map_err(|_| NoFasterBlockSource)?;
            dispatched.await
        });
        (held, response)
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
}

/// The first request's next overdue or stall deadline after `considered_at`, capped by the
/// window end. An announcer's miss can bring the next decision forward.
pub(super) fn next_decision_after(attempt: &BlockHedge, considered_at: Instant) -> Instant {
    let window_end = attempt.window_end();
    let Some(first) = attempt.first_dispatch() else {
        return window_end;
    };
    let Some(first_score) = first.score else {
        return window_end;
    };
    decision_times(first_score)
        .into_iter()
        .map(|after| first.at + after)
        .find(|at| *at > considered_at)
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

    /// The timer is armed after the decision, so it must count from the time of the decision.
    /// Counted from the time of arming, a first request that became overdue in between would
    /// not be considered again until it stalled.
    #[tokio::test(start_paused = true)]
    async fn a_decision_time_that_arrives_while_deciding_is_not_skipped() {
        let attempt = BlockHedge::new(Instant::now());
        let sent_at = Instant::now();
        attempt
            .dispatch_if_needed(None, Some(ms(242)), || ())
            .expect("an open download accepts its first request");

        let considered_at = sent_at + ms(241);
        tokio::time::advance(ms(243)).await;
        assert_eq!(
            next_decision_after(&attempt, considered_at),
            sent_at + ms(242),
            "the overdue time passed after the decision, so it is still due"
        );
        assert_eq!(
            next_decision_after(&attempt, sent_at + ms(242)),
            sent_at + ms(742)
        );
        assert_eq!(
            next_decision_after(&attempt, sent_at + ms(742)),
            attempt.window_end()
        );
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
