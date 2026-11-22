//! Event-driven hedging and arrival estimation for gossiped legacy block downloads.
//!
//! When a legacy block download starts, the peer set dispatches the primary request to the
//! selected announcer and holds one optional second request internally while the download is
//! open. The peer set re-evaluates held requests only when an input changes:
//!
//! - another peer announces the block;
//! - an announcer becomes free, records a new heartbeat RTT, or finishes a block transfer;
//! - an announcer's missed-block cooldown or slow-rate entry expires;
//! - the first request becomes overdue, stalls, or its hedge window closes.
//!
//! Dropping the caller's response future (for example, on dual-stack fallback to native peers or
//! download cancellation) closes the internal oneshot receiver and withdraws the held request.

use std::{task::Context, time::Duration};

use futures::{future::BoxFuture, FutureExt};
use tokio::{sync::oneshot, time::Instant};
use zakura_chain::block;

use crate::{BoxError, InventoryResponse, PeerSocketAddr, Response};

/// How long after dispatching a primary request the peer set may send a second request.
pub(super) const HEDGE_WINDOW: Duration = Duration::from_secs(5);

/// Minimum lead a faster announcer must promise over the first request to justify a second copy.
const MIN_MARGIN: Duration = Duration::from_millis(20);

/// Share of the first peer's expected time that a faster announcer must also beat it by.
const MARGIN_SHARE: u32 = 4;

/// How long past its expected arrival time a first request may run before it counts as stalled.
const STALL_SLACK: Duration = Duration::from_millis(500);

/// Minimum block size that always updates a peer's observed transfer rate.
const MIN_RATE_SAMPLE_BYTES: u64 = 64 * 1024;

/// Minimum transfer duration beyond RTT that updates a peer's transfer rate even on small blocks,
/// catching peers that answer pings quickly but stall on block bodies.
const MIN_RATE_SAMPLE_TRANSFER: Duration = Duration::from_millis(10);

/// Why a second request was sent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SecondRequestReason {
    /// Another announcer was expected to deliver the block clearly before the first request.
    Predicted,
    /// The first request ran well past its expected arrival time.
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

/// Elapsed times after primary dispatch at which waiting alone can flip a refusal into a hedge:
/// when the first request becomes overdue, and when it stalls.
fn decision_times(first_score: Duration) -> [Duration; 2] {
    [first_score, first_score + STALL_SLACK]
}

/// Returns true once the first request has run past its stall threshold.
pub(super) fn is_stalled(first_score: Duration, waited: Duration) -> bool {
    waited >= first_score + STALL_SLACK
}

/// Decides whether to send a second request to a measured candidate announcer.
///
/// Before `first_score`, the candidate must beat the remaining expected time by at least
/// `max(20 ms, first_score / 4)`. Once overdue (`waited >= first_score`), the block is likely
/// larger than expected or congested, so the candidate is compared against the full `first_score`
/// plus margin. At `first_score + 500 ms` the first request counts as stalled and any candidate
/// qualifies.
pub(super) fn second_request_reason(
    first_score: Duration,
    waited: Duration,
    best_candidate_score: Duration,
) -> Option<SecondRequestReason> {
    let [overdue_after, stalled_after] = decision_times(first_score);
    if waited >= stalled_after {
        return Some(SecondRequestReason::Stalled);
    }

    let remaining = if waited >= overdue_after {
        first_score
    } else {
        first_score - waited
    };
    let margin = MIN_MARGIN.max(first_score / MARGIN_SHARE);
    (best_candidate_score + margin < remaining).then_some(SecondRequestReason::Predicted)
}

/// Computes `rtt + ceil(bytes / rate_bps)` in nanoseconds without overflow.
///
/// When `bytes == 0` or no transfer rate is available, returns pure `rtt`.
pub(super) fn arrival_duration(rtt: Duration, bytes: u64, rate_bps: Option<u64>) -> Duration {
    let Some(rate_bps) = rate_bps.filter(|&r| r > 0 && bytes > 0) else {
        return rtt;
    };
    let transfer_ns = u128::from(bytes)
        .saturating_mul(1_000_000_000)
        .div_ceil(u128::from(rate_bps));
    let total_ns = rtt.as_nanos().saturating_add(transfer_ns);
    Duration::from_nanos(u64::try_from(total_ns).unwrap_or(u64::MAX))
}

/// Derives an incoming transfer rate (bytes/sec) from an idle-dispatched block delivery,
/// ignoring tiny fast blocks whose jitter would distort bandwidth estimates.
pub(super) fn sample_rate_bps(block_bytes: u64, elapsed: Duration, rtt: Duration) -> Option<u64> {
    if block_bytes == 0 {
        return None;
    }
    let transfer = elapsed.saturating_sub(rtt);
    if block_bytes < MIN_RATE_SAMPLE_BYTES && transfer < MIN_RATE_SAMPLE_TRANSFER {
        return None;
    }
    let transfer_ns = transfer.max(Duration::from_millis(1)).as_nanos();
    let rate = u128::from(block_bytes).saturating_mul(1_000_000_000) / transfer_ns;
    Some(u64::try_from(rate).unwrap_or(u64::MAX).max(1))
}

/// Overflow-safe median of `vals`.
pub(super) fn median_u128(vals: &mut [u128]) -> Option<u128> {
    if vals.is_empty() {
        return None;
    }
    vals.sort_unstable();
    let mid = vals.len() / 2;
    if vals.len().is_multiple_of(2) {
        Some(vals[mid - 1] / 2 + vals[mid] / 2 + (vals[mid - 1] % 2 + vals[mid] % 2) / 2)
    } else {
        Some(vals[mid])
    }
}

/// The response future of one block request.
pub(super) type SecondResponse = BoxFuture<'static, Result<Response, BoxError>>;

/// An internal second block request held by the peer set while its primary is outstanding.
pub(super) struct HeldSecondRequest {
    pub(super) hash: block::Hash,
    pub(super) first_peer: PeerSocketAddr,
    pub(super) first_score: Duration,
    pub(super) dispatched_at: Instant,
    window_end: Instant,
    reply: oneshot::Sender<SecondResponse>,
    pub(super) span: tracing::Span,
}

impl HeldSecondRequest {
    /// Creates a held second request paired with the receiver awaited by [`race_block_request`].
    pub(super) fn new(
        hash: block::Hash,
        first_peer: PeerSocketAddr,
        first_score: Duration,
        dispatched_at: Instant,
    ) -> (Self, oneshot::Receiver<SecondResponse>) {
        let (reply, receiver) = oneshot::channel();
        let held = Self {
            hash,
            first_peer,
            first_score,
            dispatched_at,
            window_end: dispatched_at + HEDGE_WINDOW,
            reply,
            span: tracing::Span::current(),
        };
        (held, receiver)
    }

    /// Returns whether the download still accepts a second request, registering `cx` to wake if
    /// the primary finishes or the caller drops the download future.
    pub(super) fn poll_open(&mut self, cx: &mut Context<'_>, now: Instant) -> bool {
        now < self.window_end && self.reply.poll_closed(cx).is_pending()
    }

    /// The next overdue, stall, or window-close deadline strictly after `considered_at`.
    pub(super) fn next_decision_after(&self, considered_at: Instant) -> Instant {
        decision_times(self.first_score)
            .into_iter()
            .map(|after| self.dispatched_at + after)
            .find(|at| *at > considered_at)
            .map_or(self.window_end, |at| at.min(self.window_end))
    }

    /// Hands the dispatched second request's response future to the racing primary future.
    pub(super) fn send(self, response: SecondResponse) {
        let _ = self.reply.send(response);
    }
}

/// Returns true when `result` carries the single requested block `hash`.
pub(super) fn is_usable_block(hash: block::Hash, result: &Result<Response, BoxError>) -> bool {
    matches!(
        result,
        Ok(Response::Blocks(blocks))
            if matches!(
                blocks.as_slice(),
                [InventoryResponse::Available((block, _))] if block.hash() == hash
            )
    )
}

/// Races a primary block request against an optional second request dispatched by the peer set.
///
/// The first reply that delivers the requested block wins. Dropping this future (including on
/// dual-stack timeout or download cancellation) drops `hedge`, withdrawing any unsent hedge.
pub(super) fn race_block_request(
    hash: block::Hash,
    mut first: SecondResponse,
    mut hedge: oneshot::Receiver<SecondResponse>,
) -> SecondResponse {
    async move {
        let mut second = tokio::select! {
            biased;
            first_result = &mut first => {
                if is_usable_block(hash, &first_result) {
                    if let Ok(second) = hedge.try_recv() {
                        tokio::spawn(async move {
                            let _ = second.await;
                        });
                    }
                    return first_result;
                }
                match hedge.try_recv() {
                    Ok(second) => {
                        let second_result = second.await;
                        if is_usable_block(hash, &second_result) {
                            metrics::counter!("gossip.hedge.won.count").increment(1);
                            return second_result;
                        }
                        return first_result;
                    }
                    Err(_) => return first_result,
                }
            }
            dispatched = &mut hedge => match dispatched {
                Ok(second) => second,
                Err(_) => return first.await,
            },
        };

        tokio::select! {
            biased;
            first_result = &mut first => {
                if is_usable_block(hash, &first_result) {
                    tokio::spawn(async move {
                        let _ = second.await;
                    });
                    return first_result;
                }
                let second_result = second.await;
                if is_usable_block(hash, &second_result) {
                    metrics::counter!("gossip.hedge.won.count").increment(1);
                    second_result
                } else {
                    first_result
                }
            }
            second_result = &mut second => {
                if is_usable_block(hash, &second_result) {
                    metrics::counter!("gossip.hedge.won.count").increment(1);
                    tokio::spawn(async move {
                        let _ = first.await;
                    });
                    return second_result;
                }
                first.await
            }
        }
    }
    .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    #[test]
    fn a_clearly_faster_announcer_is_asked() {
        assert_eq!(
            second_request_reason(ms(230), ms(70), ms(50)),
            Some(SecondRequestReason::Predicted)
        );
    }

    #[test]
    fn an_announcer_that_is_not_clearly_faster_is_not_asked() {
        assert_eq!(second_request_reason(ms(230), ms(70), ms(120)), None);
        assert_eq!(second_request_reason(ms(40), ms(20), ms(5)), None);
    }

    #[test]
    fn the_margin_is_at_least_twenty_milliseconds() {
        assert_eq!(second_request_reason(ms(40), ms(15), ms(5)), None);
        assert_eq!(
            second_request_reason(ms(40), ms(14), ms(5)),
            Some(SecondRequestReason::Predicted)
        );
    }

    #[test]
    fn an_overdue_first_request_is_compared_with_its_whole_expected_time() {
        assert_eq!(second_request_reason(ms(242), ms(195), ms(3)), None);
        assert_eq!(
            second_request_reason(ms(242), ms(242), ms(3)),
            Some(SecondRequestReason::Predicted)
        );
        assert_eq!(second_request_reason(ms(242), ms(300), ms(190)), None);
    }

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

    #[tokio::test(start_paused = true)]
    async fn a_decision_time_that_arrives_while_deciding_is_not_skipped() {
        let sent_at = Instant::now();
        let (held, _rx) = HeldSecondRequest::new(
            block::Hash([1; 32]),
            "127.0.0.1:8233".parse().unwrap(),
            ms(242),
            sent_at,
        );
        let considered_at = sent_at + ms(241);
        tokio::time::advance(ms(243)).await;
        assert_eq!(held.next_decision_after(considered_at), sent_at + ms(242));
        assert_eq!(
            held.next_decision_after(sent_at + ms(242)),
            sent_at + ms(742)
        );
        assert_eq!(
            held.next_decision_after(sent_at + ms(742)),
            sent_at + HEDGE_WINDOW
        );
    }

    #[test]
    fn stall_threshold_is_inclusive_for_measured_candidates() {
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
    }

    #[test]
    fn arrival_separates_rtt_and_transfer_time() {
        // 242 ms RTT + 1.5 MB at 5 MB/s = 242 ms + 300 ms = 542 ms.
        assert_eq!(
            arrival_duration(ms(242), 1_500_000, Some(5_000_000)),
            ms(542)
        );
        // Without a block size or rate, arrival equals pure RTT.
        assert_eq!(arrival_duration(ms(242), 0, Some(5_000_000)), ms(242));
        assert_eq!(arrival_duration(ms(242), 1_500_000, None), ms(242));
    }

    #[test]
    fn rate_sampling_skips_tiny_fast_blocks_and_catches_throttled_peers() {
        // Small fast block: 10 KB in 244 ms on a 242 ms peer -> ignored.
        assert_eq!(sample_rate_bps(10_000, ms(244), ms(242)), None);
        // Large block: 1.5 MB in 542 ms on a 242 ms peer -> 5 MB/s.
        assert_eq!(
            sample_rate_bps(1_500_000, ms(542), ms(242)),
            Some(5_000_000)
        );
        // Small block served slowly by a low-RTT peer: 10 KB in 405 ms on a 5 ms peer -> 25 KB/s.
        assert_eq!(sample_rate_bps(10_000, ms(405), ms(5)), Some(25_000));
    }
}
