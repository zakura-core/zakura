//! When to send a second request for a gossiped block that a first request has not delivered.
//!
//! The first request for a block goes to whichever idle peer announced it first, which from a
//! node far from most miners is usually a far peer. A closer peer often announces the same block
//! a few tens of milliseconds later. Asking it as well wins if its expected answer time, from
//! now, is clearly shorter than what is left of the first request's. Otherwise the second copy
//! would only cost bandwidth, because a legacy block request cannot be cancelled.
//!
//! A legacy `inv` carries no block size, so when the first request is sent every expected answer
//! time is a peer's round trip. The message header states the body length. Once enough of the
//! body arrives to identify the requested block by its header hash, routing uses that length
//! to compare peers using their round trips and their records on large blocks.
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
//! - the first request becomes overdue, and later stalls (see [`FirstRequestTimes`]).
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

/// How long before the window for a second request closes a first request is checked for a
/// stall one last time (see [`FirstRequestTimes::new`]). It leaves the peer set's task time to
/// wake and send the request while the window is still open.
pub(super) const LAST_STALL_CHECK: Duration = Duration::from_millis(250);

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

/// The times, counted from when the first request was sent, at which waiting alone changes the
/// decision: when the first request becomes overdue, and when it counts as stalled.
///
/// Before it is overdue, the time left on the first request only shrinks, so a candidate that
/// does not qualify now will not qualify by waiting. Once it is overdue, only a stall changes
/// the decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct FirstRequestTimes {
    /// The first peer's expected answer time for the block. Past it, the request is overdue.
    expected: Duration,
    /// How long the request may run before it counts as stalled.
    stalled_after: Duration,
}

impl FirstRequestTimes {
    /// - `round_trip` is the first peer's round trip, from its measurements when it was asked;
    /// - `expected` is its expected answer time for the block: the round trip, or, once its
    ///   reply has started and declared the block's size, the longer time expected for that
    ///   size;
    /// - `window` is how long after the first request was sent a second one can still be sent.
    ///
    /// A request stalls [`STALL_SLACK`] after its expected time.
    ///
    /// A declared size is the first peer's own claim, which the node cannot check until the
    /// whole block has arrived. A peer that declared the largest block and then sent nothing
    /// more could move the stall past the window, where no second request is sent at all. So a
    /// declared size delays the stall no later than [`LAST_STALL_CHECK`] before the window
    /// closes. A stall that the round trip alone already puts later than that stays where it
    /// is.
    pub(super) fn new(round_trip: Duration, expected: Duration, window: Duration) -> Self {
        let stalled_by_round_trip = round_trip.saturating_add(STALL_SLACK);
        let last_check = window.saturating_sub(LAST_STALL_CHECK);
        Self {
            expected,
            stalled_after: expected
                .saturating_add(STALL_SLACK)
                .min(stalled_by_round_trip.max(last_check)),
        }
    }

    /// The first request's next overdue or stall deadline after `considered_at`, or
    /// `window_end` when neither is left inside the window. An announcer's miss can bring the
    /// next decision forward.
    pub(super) fn next_decision_after(
        self,
        sent_at: Instant,
        considered_at: Instant,
        window_end: Instant,
    ) -> Instant {
        // A stall capped by the window can come before the overdue time.
        [self.expected, self.stalled_after]
            .into_iter()
            .filter(|after| *after < window_end.saturating_duration_since(sent_at))
            .map(|after| sent_at + after)
            .filter(|at| *at > considered_at)
            .min()
            .unwrap_or(window_end)
    }
}

/// Decides whether to send a second request for a block.
///
/// - `first` holds the first peer's expected answer time for the block and when its request
///   stalls. A first peer with no measured answers gets no second request, because there is
///   nothing to compare a candidate with;
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
    first: FirstRequestTimes,
    waited: Duration,
    best_candidate_score: Duration,
) -> Option<SecondRequestReason> {
    let first_score = first.expected;
    if waited >= first.stalled_after {
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
    let remaining = if waited >= first_score {
        first_score
    } else {
        first_score - waited
    };
    let margin = MIN_MARGIN.max(first_score / MARGIN_SHARE);
    (best_candidate_score.saturating_add(margin) < remaining)
        .then_some(SecondRequestReason::Predicted)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    /// A first request whose block size is not known, so its expected time is its round trip.
    fn unsized_first(round_trip: Duration) -> FirstRequestTimes {
        FirstRequestTimes::new(round_trip, round_trip, BlockHedge::WINDOW)
    }

    /// Decides for a first request whose block size is not known.
    fn reason(
        round_trip: Duration,
        waited: Duration,
        candidate: Duration,
    ) -> Option<SecondRequestReason> {
        second_request_reason(unsized_first(round_trip), waited, candidate)
    }

    #[test]
    fn a_clearly_faster_announcer_is_asked() {
        // The first peer should answer in 230 ms; 70 ms in, a peer expected to take 50 ms
        // would deliver about 110 ms sooner.
        assert_eq!(
            reason(ms(230), ms(70), ms(50)),
            Some(SecondRequestReason::Predicted)
        );
    }

    #[test]
    fn an_announcer_that_is_not_clearly_faster_is_not_asked() {
        // 160 ms are left, and the candidate's 120 ms plus the 57 ms margin is more.
        assert_eq!(reason(ms(230), ms(70), ms(120)), None);
        // A fast first peer leaves little to win.
        assert_eq!(reason(ms(40), ms(20), ms(5)), None);
    }

    #[test]
    fn the_margin_is_at_least_twenty_milliseconds() {
        // A quarter of 40 ms is 10 ms, so the 20 ms minimum applies: 5 + 20 is not under the
        // 25 ms left after 15 ms, but is under the 26 ms left after 14 ms.
        assert_eq!(reason(ms(40), ms(15), ms(5)), None);
        assert_eq!(
            reason(ms(40), ms(14), ms(5)),
            Some(SecondRequestReason::Predicted)
        );
    }

    #[test]
    fn an_overdue_first_request_is_compared_with_its_whole_expected_time() {
        // From the asia-scored load test: a far peer expected in 242 ms was still sending a
        // 1.5 MB block when same-city peers expected in 3 ms announced.
        assert_eq!(
            reason(ms(242), ms(195), ms(3)),
            None,
            "before it is overdue, only 47 ms are left, less than the 3 ms plus a 60 ms margin"
        );
        assert_eq!(
            reason(ms(242), ms(242), ms(3)),
            Some(SecondRequestReason::Predicted),
            "once overdue, 3 ms plus the margin is well under the whole 242 ms"
        );
        assert_eq!(
            reason(ms(242), ms(300), ms(190)),
            None,
            "a candidate about as far away is still not asked"
        );
    }

    /// A held second request is reconsidered at the decision times instead of on a short
    /// interval, so waiting must not turn a refusal into a request at any other time.
    #[test]
    fn waiting_alone_changes_the_decision_only_at_the_decision_times() {
        let first_score = ms(242);
        let times = unsized_first(first_score);
        assert_eq!([times.expected, times.stalled_after], [ms(242), ms(742)]);

        for candidate in [3, 50, 120, 190, 400] {
            let mut previous = reason(first_score, ms(0), ms(candidate));
            for waited in 1..=800 {
                let decision = reason(first_score, ms(waited), ms(candidate));
                if previous.is_none() && decision.is_some() {
                    assert!(
                        [times.expected, times.stalled_after].contains(&ms(waited)),
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
        let sent_at = Instant::now();
        let window_end = sent_at + BlockHedge::WINDOW;
        let next = |at| unsized_first(ms(242)).next_decision_after(sent_at, at, window_end);

        let considered_at = sent_at + ms(241);
        tokio::time::advance(ms(243)).await;
        assert_eq!(
            next(considered_at),
            sent_at + ms(242),
            "the overdue time passed after the decision, so it is still due"
        );
        assert_eq!(next(sent_at + ms(242)), sent_at + ms(742));
        assert_eq!(next(sent_at + ms(742)), window_end);
    }

    #[test]
    fn stall_threshold_is_inclusive_for_measured_candidates() {
        // A stall is 500 ms past the expected time, however fast or slow the first peer.
        assert_eq!(reason(ms(230), ms(729), ms(900)), None);
        assert_eq!(
            reason(ms(230), ms(730), ms(900)),
            Some(SecondRequestReason::Stalled)
        );
        assert_eq!(reason(ms(5), ms(504), ms(900)), None);
        assert_eq!(
            reason(ms(5), ms(505), ms(900)),
            Some(SecondRequestReason::Stalled)
        );
        assert_eq!(reason(ms(800), ms(1299), ms(900)), None);
        assert_eq!(
            reason(ms(800), ms(1300), ms(900)),
            Some(SecondRequestReason::Stalled)
        );
    }

    /// A 1,000 ms peer and the largest block: about 7.1 round trips on a new connection.
    const DECLARED_LARGEST: Duration = Duration::from_millis(7_100);

    /// The first peer's reply declares its size before the node can check it. A peer that
    /// declares the largest block and sends nothing more must still be noticed while a second
    /// request can be sent.
    #[test]
    fn a_declared_size_cannot_move_the_stall_past_the_window() {
        let window = BlockHedge::WINDOW;
        let first = FirstRequestTimes::new(ms(1_000), DECLARED_LARGEST, window);
        let last_check = window - LAST_STALL_CHECK;
        assert_eq!(first.stalled_after, last_check);
        assert!(
            unsized_first(ms(1_000)).stalled_after < window,
            "without the declared size the request would have stalled inside the window"
        );

        // Another announcer just as far away is expected to need as long for that size.
        assert_eq!(
            second_request_reason(first, last_check - ms(1), DECLARED_LARGEST),
            None
        );
        assert_eq!(
            second_request_reason(first, last_check, DECLARED_LARGEST),
            Some(SecondRequestReason::Stalled)
        );
    }

    #[test]
    fn a_declared_size_delays_a_stall_that_still_fits_the_window() {
        // 1.5 MB from a 250 ms peer is expected in about 1,670 ms.
        let first = FirstRequestTimes::new(ms(250), ms(1_670), BlockHedge::WINDOW);
        assert_eq!(first.stalled_after, ms(2_170));
        assert_eq!(second_request_reason(first, ms(2_169), ms(1_610)), None);
        assert_eq!(
            second_request_reason(first, ms(2_170), ms(1_610)),
            Some(SecondRequestReason::Stalled)
        );
    }

    /// The cap only limits how far a declared size can delay a stall. A first peer too far away
    /// to stall inside the window on its round trip alone is not treated as stalled early.
    #[test]
    fn a_stall_the_round_trip_puts_outside_the_window_is_not_brought_forward() {
        let window = BlockHedge::WINDOW;
        let round_trip = window - ms(400);
        for expected in [round_trip, round_trip * 7] {
            let first = FirstRequestTimes::new(round_trip, expected, window);
            assert_eq!(first.stalled_after, round_trip + STALL_SLACK);
        }

        // A first request sent late leaves less of the window than the last check needs.
        let late = FirstRequestTimes::new(ms(100), ms(700), ms(200));
        assert_eq!(late.stalled_after, ms(600));
    }

    /// The timer must wake for a capped stall, which can come before the overdue time and
    /// while the overdue time is outside the window.
    #[tokio::test(start_paused = true)]
    async fn the_next_decision_is_the_capped_stall_when_it_comes_first() {
        let sent_at = Instant::now();
        let window = BlockHedge::WINDOW;
        let window_end = sent_at + window;
        let last_check = sent_at + window - LAST_STALL_CHECK;

        let overdue_outside = FirstRequestTimes::new(ms(1_000), DECLARED_LARGEST, window);
        assert_eq!(
            overdue_outside.next_decision_after(sent_at, sent_at, window_end),
            last_check
        );
        assert_eq!(
            overdue_outside.next_decision_after(sent_at, last_check, window_end),
            window_end
        );

        // Overdue 100 ms before the window ends, after the last stall check.
        let overdue_inside = FirstRequestTimes::new(ms(1_000), window - ms(100), window);
        assert_eq!(
            overdue_inside.next_decision_after(sent_at, sent_at, window_end),
            last_check
        );
        assert_eq!(
            overdue_inside.next_decision_after(sent_at, last_check, window_end),
            window_end - ms(100)
        );
    }
}
