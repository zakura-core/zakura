//! Failure matrix for one fill attempt.
//!
//! Each case stops an attempt at one boundary with the production guards and
//! checks that the stop settles every resource the attempt acquired: work,
//! byte budget, requester pool entries, response authorization, and the
//! outstanding record. Fence refusal and a transport close during publication
//! need a concurrent writer, so the simulator covers them.

use std::time::{Duration, Instant};

use zakura_chain::block;
use zakura_rt::sync::CancellationToken;

use super::{super::tests::status_test_routine_with, FillStop, PeerRoutine};
use crate::zakura::{
    block_sync::{request::BlockSizeEstimate, test_work_scope, BlockSyncStatus},
    regulation::ReservationPool,
    FramedRecv,
};

/// A routine that serves `1..=10` with `max_blocks` per response, a two-entry
/// pool, and pending work at `heights`.
struct Fixture {
    routine: PeerRoutine,
    outbound: FramedRecv,
    pool: ReservationPool,
    connection: CancellationToken,
    heights: Vec<u32>,
}

fn fixture(max_blocks: u32, heights: &[u32]) -> Fixture {
    let pool = ReservationPool::new(2).unwrap();
    let connection = CancellationToken::new();
    let (mut routine, outbound, _events) = status_test_routine_with(pool.clone(), &connection);
    routine.handle_status(BlockSyncStatus {
        servable_low: block::Height(1),
        servable_high: block::Height(10),
        max_blocks_per_response: max_blocks,
        ..BlockSyncStatus::default()
    });
    routine.io.work.extend(
        test_work_scope(),
        heights.iter().map(|raw| {
            (
                block::Height(*raw),
                block::Hash([u8::try_from(*raw).unwrap(); 32]),
                BlockSizeEstimate::Confirmed(1_000),
            )
        }),
    );
    Fixture {
        routine,
        outbound,
        pool,
        connection,
        heights: heights.to_vec(),
    }
}

impl Fixture {
    fn next_request_id(&self) -> u64 {
        self.routine.state.next_request_id.unwrap().get()
    }

    /// The stopped attempt left no work, charge, entry, authorization, or record.
    fn assert_settled(&mut self, budget_reserved: u64) {
        let routine = &self.routine;
        for raw in &self.heights {
            assert!(routine.io.work.pending_contains(block::Height(*raw)));
        }
        assert_eq!(routine.io.work.in_flight_len(), 0);
        assert_eq!(routine.io.budget.reserved(), budget_reserved);
        assert_eq!(self.pool.held(), 0);
        assert_eq!(routine.io.requester.occupancy().len, 0);
        assert!(routine.window.outstanding.is_empty());
        assert!(self.outbound.try_recv().is_err(), "nothing was sent");
    }
}

#[tokio::test]
async fn a_peer_without_status_acquires_nothing() {
    let mut fixture = fixture(1, &[1]);
    fixture.routine.state.received_status = false;
    let before = fixture.next_request_id();

    let report = fixture.routine.try_fill();

    assert_eq!(report.stop, FillStop::NoStatus);
    assert_eq!(fixture.next_request_id(), before);
    fixture.assert_settled(0);
}

#[tokio::test]
async fn a_full_requester_pool_waits_for_an_entry() {
    let mut fixture = fixture(1, &[1]);
    let held = fixture.pool.try_entry().unwrap();
    let other = fixture.pool.try_entry().unwrap();
    let before = fixture.next_request_id();

    let report = fixture.routine.try_fill();

    assert_eq!(report.stop, FillStop::Budget);
    assert!(fixture.routine.request_pool_waiting);
    assert_eq!(fixture.next_request_id(), before);
    drop((held, other));
    fixture.assert_settled(0);
}

#[tokio::test]
async fn a_retired_fence_releases_the_pool_entry() {
    let mut fixture = fixture(1, &[1]);
    fixture.connection.cancel();
    let before = fixture.next_request_id();

    let report = fixture.routine.try_fill();

    assert_eq!(report.stop, FillStop::SendError);
    assert_eq!(fixture.next_request_id(), before);
    fixture.assert_settled(0);
}

#[tokio::test]
async fn a_closed_transport_releases_the_exchange_and_cancels_the_session() {
    let mut fixture = fixture(1, &[1]);
    fixture.outbound.close();
    let before = fixture.next_request_id();

    let report = fixture.routine.try_fill();

    assert_eq!(report.stop, FillStop::SendError);
    assert!(fixture.routine.io.session.cancel_token().is_cancelled());
    assert_eq!(fixture.next_request_id(), before);
    fixture.assert_settled(0);
}

#[tokio::test]
async fn exhausted_request_ids_release_acquired_capacity() {
    let mut fixture = fixture(1, &[1]);
    fixture.routine.state.next_request_id = None;

    let report = fixture.routine.try_fill();

    assert_eq!(report.stop, FillStop::Internal);
    fixture.assert_settled(0);
}

#[tokio::test]
async fn an_attempt_without_work_still_consumes_its_request_id() {
    let mut fixture = fixture(1, &[]);
    let before = fixture.next_request_id();

    let report = fixture.routine.try_fill();

    assert_eq!(report.stop, FillStop::NoWork);
    assert_eq!(fixture.next_request_id(), before + 1);
    fixture.assert_settled(0);
}

#[tokio::test]
async fn avoided_work_returns_quietly_with_its_retry_deadline() {
    let mut fixture = fixture(1, &[1]);
    let until = Instant::now() + Duration::from_secs(60);
    fixture.routine.retry_avoid.insert(block::Height(1), until);
    let before = fixture.next_request_id();

    let report = fixture.routine.try_fill();

    assert_eq!(report.stop, FillStop::RetryAvoid);
    assert_eq!(report.retry_at, Some(until));
    assert_eq!(fixture.next_request_id(), before + 1);
    fixture.assert_settled(0);
}

#[tokio::test]
async fn refused_above_floor_funding_returns_the_run_without_a_charge() {
    let mut fixture = fixture(1, &[3]);
    // Admission funds 500 bytes, but the take always admits its first body.
    let mut budget = fixture.routine.io.budget.clone();
    let reserved = budget.available() - 500;
    assert!(budget.try_reserve(reserved));
    let before = fixture.next_request_id();

    let report = fixture.routine.try_fill();

    assert_eq!(report.stop, FillStop::Budget);
    assert_eq!(fixture.next_request_id(), before + 1);
    fixture.assert_settled(reserved);
}

#[tokio::test]
async fn a_revoked_claim_retires_its_authorization_and_returns_its_funding() {
    let mut fixture = fixture(1, &[3]);
    fixture.routine.io.session.cancel_token().cancel();
    let before = fixture.next_request_id();

    let report = fixture.routine.try_fill();

    assert_eq!(report.stop, FillStop::Internal);
    assert_eq!(fixture.next_request_id(), before + 1);
    fixture.assert_settled(0);
}

#[tokio::test]
async fn a_pass_that_published_keeps_its_retry_deadline() {
    let mut fixture = fixture(1, &[1, 2]);
    let until = Instant::now() + Duration::from_secs(60);
    fixture.routine.retry_avoid.insert(block::Height(2), until);
    // A proven peer may send more than its single cold-start probe.
    let grace = fixture.routine.config.effective_liveness_timeout();
    fixture
        .routine
        .window
        .note_block_progress(Instant::now(), grace);

    let report = fixture.routine.try_fill();

    assert_eq!(report.sent, 1);
    assert_eq!(report.stop, FillStop::RetryAvoid);
    assert_eq!(report.retry_at, Some(until));
    let routine = &fixture.routine;
    assert!(routine.io.work.in_flight_contains(block::Height(1)));
    assert!(routine.io.work.pending_contains(block::Height(2)));
    assert!(routine.io.work.reserved_bytes() > 0);
    assert_eq!(
        routine.io.budget.reserved(),
        routine.io.work.reserved_bytes()
    );
    assert_eq!(fixture.pool.held(), 1);
    assert_eq!(routine.io.requester.occupancy().len, 1);
    assert_eq!(routine.window.outstanding.len(), 1);
    assert!(fixture.outbound.try_recv().is_ok(), "the request was sent");
}
