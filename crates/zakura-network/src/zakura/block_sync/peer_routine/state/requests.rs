//! Request lifecycle decisions: deadlines, expiry, and retry avoidance.

use std::time::{Duration, Instant};

use zakura_chain::block;

use super::{
    super::super::{
        regulated::wire::Outcome,
        state::{DeliverySnapshot, OutstandingBlockRange, RequestKey, WriteOutcome},
    },
    PeerState,
};

/// How a finished request returns its work.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(in super::super) enum Disposition {
    /// Every requested height arrived or fell below the download floor.
    Satisfied,
    /// `RangeUnavailable`: retry the original range.
    RetryOriginal,
    /// A short `BlocksDone` or a lost claim: retry the missing heights.
    RetryMissing,
}

impl Disposition {
    pub(in super::super) fn trace_label(self) -> &'static str {
        match self {
            Self::Satisfied => "satisfied",
            Self::RetryOriginal => "retry_original",
            Self::RetryMissing => "retry_missing",
        }
    }
}

/// How an authorized, decoded body relates to this session's window.
///
/// Authorization already proved that the peer owed this body. A mismatch
/// against the request is therefore the peer's fault, while an unmatched body
/// means only that local work moved on.
#[derive(Copy, Clone, Debug)]
pub(in super::super) enum BodyMatch {
    /// No outstanding request covers the height. Local retirement, a reset, or
    /// floor GC removed it, so the body may still be wanted elsewhere.
    Unmatched,
    /// The covering request already received this height.
    Duplicate,
    /// The covering request expects another block at this height.
    PayloadMismatch {
        owner: zakura_header_chain::BodyWorkOwner,
        requested: block::Hash,
    },
    /// The covering request lists no block at this height.
    NotRequested,
    /// The body answers the covering request.
    Matched(MatchedBody),
}

/// The request that a matched body answers.
#[derive(Copy, Clone, Debug)]
pub(in super::super) struct MatchedBody {
    pub(in super::super) key: RequestKey,
    pub(in super::super) owner: zakura_header_chain::BodyWorkOwner,
    /// The size estimate this height reserved.
    pub(in super::super) estimated_bytes: u64,
    pub(in super::super) start_height: block::Height,
    pub(in super::super) count: u32,
    pub(in super::super) queued_at: Instant,
    pub(in super::super) delivery_snapshot: DeliverySnapshot,
}

/// How an ending finishes its request.
#[derive(Debug, Eq, PartialEq)]
pub(in super::super) struct Ending {
    pub(in super::super) key: RequestKey,
    pub(in super::super) owner: zakura_header_chain::BodyWorkOwner,
    /// The request's block count and queue time, for its trace.
    pub(in super::super) count: u32,
    pub(in super::super) queued_at: Instant,
    /// Unreceived heights at or below the download floor. Their bodies are no
    /// longer needed, so their reservations return without a retry.
    pub(in super::super) stale_heights: Vec<block::Height>,
    pub(in super::super) disposition: Disposition,
}

/// Requests that reached their deadline, split by how their write settled.
#[derive(Debug, Default)]
pub(in super::super) struct ExpiredRequests {
    /// Requests whose frame never reached the wire. They end without a fault.
    pub(in super::super) skipped: Vec<OutstandingBlockRange>,
    /// Started requests that the peer left unanswered.
    pub(in super::super) timed_out: Vec<OutstandingBlockRange>,
}

impl ExpiredRequests {
    pub(in super::super) fn is_empty(&self) -> bool {
        self.skipped.is_empty() && self.timed_out.is_empty()
    }
}

/// Wake deadlines that shared owners hold for this session.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub(in super::super) struct SharedDeadlines {
    /// When the live requester abandons an exchange that still owes its ending.
    pub(in super::super) requester_retirement: Option<Instant>,
    /// When the floor watchdog stops excluding this peer from a height.
    pub(in super::super) floor_watchdog_avoid: Option<Instant>,
    /// When a body retry alarm stops excluding this peer from a body.
    pub(in super::super) body_retry_avoid: Option<Instant>,
    /// When the last fill pass's retry filter allows its returned work again.
    pub(in super::super) retry_filter: Option<Instant>,
}

impl PeerState {
    /// Match an authorized body for `height` with hash `hash` to the oldest
    /// outstanding request that covers the height.
    pub(in super::super) fn match_body(
        &self,
        height: block::Height,
        hash: block::Hash,
    ) -> BodyMatch {
        let Some(key) = self.window.outstanding_key_for_height(height) else {
            return BodyMatch::Unmatched;
        };
        let outstanding = self
            .window
            .outstanding
            .get(key)
            .expect("the key was just found in the window");
        if outstanding.has_received(height) {
            return BodyMatch::Duplicate;
        }
        match outstanding.request.expected_hash(height) {
            Some(requested) if requested != hash => BodyMatch::PayloadMismatch {
                owner: outstanding.request.owner,
                requested,
            },
            Some(_) => BodyMatch::Matched(MatchedBody {
                key,
                owner: outstanding.request.owner,
                estimated_bytes: outstanding.estimated_bytes_for_height(height).unwrap_or(0),
                start_height: outstanding.request.start_height,
                count: outstanding.request.count,
                queued_at: outstanding.queued_at,
                delivery_snapshot: outstanding.delivery_snapshot,
            }),
            None => BodyMatch::NotRequested,
        }
    }

    /// Record a matched body of `body_bytes` at `height` as block progress.
    /// Returns whether request `key` has now received every height.
    pub(in super::super) fn record_body(
        &mut self,
        key: RequestKey,
        height: block::Height,
        body_bytes: u64,
        now: Instant,
        liveness_timeout: Duration,
    ) -> bool {
        self.window.note_block_progress(now, liveness_timeout);
        self.window.outstanding.record_body_bytes(key, body_bytes);
        self.window.outstanding.mark_received(key, height);
        self.window
            .outstanding
            .get(key)
            .is_some_and(OutstandingBlockRange::is_complete)
    }

    /// Record a body at `height` whose reservation another receipt, the floor
    /// watchdog, or floor GC already released. The body still proves progress.
    /// Returns whether request `key` has now received every height, or `None`
    /// when the request left the window.
    pub(in super::super) fn record_settled_body(
        &mut self,
        key: RequestKey,
        height: block::Height,
        now: Instant,
        liveness_timeout: Duration,
    ) -> Option<bool> {
        self.window.note_block_progress(now, liveness_timeout);
        self.window.outstanding.mark_received(key, height);
        self.window
            .outstanding
            .get(key)
            .map(OutstandingBlockRange::is_complete)
    }

    /// Record a wanted body that matches no outstanding request, typically one
    /// that arrived just after its request timed out. It proves progress and
    /// offsets that timeout's reliability charge. It feeds no delivery sample,
    /// because its request's send time is gone.
    pub(in super::super) fn record_late_body(&mut self, now: Instant, liveness_timeout: Duration) {
        self.window.note_block_progress(now, liveness_timeout);
        self.window.credit_late_delivery();
    }

    /// Decide how an ending finishes the request that starts at its range's
    /// start. Both endings close the whole request. A short `BlocksDone`
    /// retries its missing suffix; `RangeUnavailable` retries the original
    /// range. A prefix that the download floor passed counts as received.
    /// Returns `None` when local work already retired the request.
    pub(in super::super) fn on_ending(
        &mut self,
        start_height: block::Height,
        outcome: Outcome,
        download_floor: block::Height,
    ) -> Option<Ending> {
        let key = self.window.outstanding_key_for_start(start_height)?;
        let mut disposition = match outcome {
            Outcome::Done => Disposition::RetryMissing,
            Outcome::Unavailable => Disposition::RetryOriginal,
        };
        let outstanding = self.window.outstanding.get(key)?;
        let (owner, count, queued_at) = (
            outstanding.request.owner,
            outstanding.request.count,
            outstanding.queued_at,
        );
        let mut stale_heights = Vec::new();
        if outstanding.request.start_height <= download_floor {
            stale_heights = unreceived_heights_through(outstanding, download_floor).collect();
            let _ = self
                .window
                .outstanding
                .mark_received_through(key, download_floor);
            disposition = if self
                .window
                .outstanding
                .get(key)
                .is_some_and(OutstandingBlockRange::is_complete)
            {
                Disposition::Satisfied
            } else {
                Disposition::RetryMissing
            };
        }
        // A short response is one goodput failure per request, like a timeout.
        // A floor that covered the shortfall is not the peer's fault.
        if disposition != Disposition::Satisfied {
            let missing = self
                .window
                .outstanding
                .get(key)
                .map(|outstanding| unreceived_heights(outstanding).count())
                .unwrap_or(0);
            self.window.penalize_short_response(missing);
        }
        Some(Ending {
            key,
            owner,
            count,
            queued_at,
            stale_heights,
            disposition,
        })
    }

    /// Whether a request deadline or the block-liveness deadline is due at `now`.
    pub(in super::super) fn deadlines_due(&self, now: Instant) -> bool {
        self.window
            .outstanding
            .earliest_deadline()
            .is_some_and(|deadline| deadline <= now)
            || self
                .window
                .block_liveness_deadline
                .is_some_and(|deadline| deadline <= now)
    }

    /// Requests whose deadline is at or before `now`, in issue order.
    pub(in super::super) fn due_requests(&self, now: Instant) -> Vec<RequestKey> {
        self.window.outstanding.due_at(now)
    }

    /// Retire due requests by how their write settled. Only a started write
    /// charges the peer with a timeout; a skipped write refunds its probe.
    pub(in super::super) fn expire_requests(
        &mut self,
        due: Vec<(RequestKey, WriteOutcome)>,
    ) -> ExpiredRequests {
        let (skipped, timed_out): (Vec<_>, Vec<_>) = due
            .into_iter()
            .partition(|(_, write)| *write == WriteOutcome::Skipped);
        // Retire skipped requests while the timed-out ones still occupy the
        // window, so their liveness deadline cannot be cleared as idle.
        let skipped: Vec<_> = skipped
            .into_iter()
            .filter_map(|(key, write)| self.window.retire_locally(key, write))
            .collect();
        let timed_out: Vec<_> = timed_out
            .into_iter()
            .filter_map(|(key, _)| self.window.outstanding.remove(key))
            .collect();
        if !timed_out.is_empty() {
            self.window.record_timeout(timed_out.len());
        }
        ExpiredRequests { skipped, timed_out }
    }

    /// Avoid re-taking `heights` until `until`, so another peer can contest
    /// them first. The heights stay pending for every other peer.
    pub(in super::super) fn avoid_retry(
        &mut self,
        heights: impl IntoIterator<Item = block::Height>,
        until: Instant,
    ) {
        for height in heights {
            self.retry_avoid.insert(height, until);
        }
    }

    /// Let this routine re-take heights whose avoidance ended by `now`.
    pub(in super::super) fn prune_retry_avoid(&mut self, now: Instant) {
        self.retry_avoid.retain(|_, until| *until > now);
    }

    pub(in super::super) fn is_retry_avoided(&self, height: block::Height) -> bool {
        self.retry_avoid.contains_key(&height)
    }

    /// When the earliest local retry avoidance ends.
    pub(in super::super) fn retry_avoid_expiry(&self) -> Option<Instant> {
        self.retry_avoid.values().min().copied()
    }

    /// The earliest instant at which this session must run again without an
    /// external event: a request or liveness deadline, a retry-avoid expiry, a
    /// deferred status, or a deadline that a shared owner holds.
    pub(in super::super) fn next_wake(&self, shared: SharedDeadlines) -> Option<Instant> {
        [
            self.window.outstanding.earliest_deadline(),
            self.window.block_liveness_deadline,
            shared.requester_retirement,
            self.retry_avoid_expiry(),
            shared.floor_watchdog_avoid,
            shared.body_retry_avoid,
            shared.retry_filter,
            self.pending_status
                .map(|_| self.inbound_status_meter.next_allowed),
        ]
        .into_iter()
        .flatten()
        .min()
    }
}

/// The still-unreceived heights of an outstanding request (the ones that return
/// to `pending` on retry/timeout — never the received-and-buffered ones, which
/// stay claimed in `work.in_flight`).
pub(in super::super) fn unreceived_heights(
    outstanding: &OutstandingBlockRange,
) -> impl Iterator<Item = block::Height> + '_ {
    outstanding
        .request
        .expected_blocks
        .iter()
        .filter(move |expected| !outstanding.has_received(expected.height))
        .map(|expected| expected.height)
}

/// The still-unreceived heights of an outstanding request at or below `tip`.
fn unreceived_heights_through(
    outstanding: &OutstandingBlockRange,
    tip: block::Height,
) -> impl Iterator<Item = block::Height> + '_ {
    outstanding
        .request
        .expected_blocks
        .iter()
        .filter(move |expected| {
            expected.height <= tip && !outstanding.has_received(expected.height)
        })
        .map(|expected| expected.height)
}
