//! Failure matrix for one fill attempt.
//!
//! Each case stops an attempt at one boundary with the production guards and
//! checks that the stop settles every resource the attempt acquired: work,
//! byte budget, requester pool entries, response authorization, and the
//! outstanding record. [`publish_after`] runs the attempt's phases and lets a
//! case change shared state at the exact point another actor can, which covers
//! publication races without a concurrent scheduler. `GetBlocks` encoding cannot
//! fail for a valid range, so the encode stop has no case here.

use std::time::Duration;

use zakura_chain::block;
use zakura_rt::{sync::CancellationToken, time::Instant};

use super::{
    super::tests::status_test_routine_with, block_range_request, FillStop, PeerRoutine,
    PublishOutcome, TakenWork,
};
use crate::zakura::{
    block_sync::{
        request::BlockSizeEstimate, test_work_scope, trace::request_owner, BlockSyncStatus,
    },
    regulation::ReservationPool,
    testkit::{TraceCapture, TraceValue},
    trace::{block_sync_trace as bs_trace, ZakuraTrace},
    transport::BudgetClosure,
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
        assert!(routine.state.window.outstanding.is_empty());
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
    assert!(fixture.routine.state.request_pool_waiting);
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
    fixture
        .routine
        .state
        .retry_avoid
        .insert(block::Height(1), until);
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
    let budget = fixture.routine.io.work.budget_for_tests();
    let reserved = budget.available() - 500;
    let unrelated = budget.reserve_owned(reserved).unwrap();
    let before = fixture.next_request_id();

    let report = fixture.routine.try_fill();

    assert_eq!(report.stop, FillStop::Budget);
    assert_eq!(fixture.next_request_id(), before + 1);
    fixture.assert_settled(reserved);
    drop(unrelated);
}

#[tokio::test]
async fn session_cancellation_before_funding_returns_the_take_without_a_charge() {
    let mut fixture = fixture(1, &[3]);
    fixture.routine.io.session.cancel_token().cancel();
    let before = fixture.next_request_id();

    let report = fixture.routine.try_fill();

    assert_eq!(report.stop, FillStop::Cancelled);
    assert_eq!(fixture.next_request_id(), before + 1);
    fixture.assert_settled(0);
}

#[tokio::test]
async fn a_pass_that_published_keeps_its_retry_deadline() {
    let mut fixture = fixture(1, &[1, 2]);
    let until = Instant::now() + Duration::from_secs(60);
    fixture
        .routine
        .state
        .retry_avoid
        .insert(block::Height(2), until);
    // A proven peer may send more than its single cold-start probe.
    let grace = fixture.routine.config.effective_liveness_timeout();
    fixture
        .routine
        .state
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
    assert_eq!(routine.state.window.outstanding.len(), 1);
    assert!(fixture.outbound.try_recv().is_ok(), "the request was sent");
}

/// Run one attempt's phases up to publication, let `interfere` change shared
/// state, and publish. The caller settles the requester as `try_issue_one` does.
fn publish_after(
    fixture: &mut Fixture,
    interfere: impl FnOnce(&PeerRoutine, &mut FramedRecv, &CancellationToken),
) -> PublishOutcome {
    let sender = fixture.routine.io.session.request_sender();
    let routine = &mut fixture.routine;
    let now = Instant::now();
    let limits = routine.state.fill_limits(&routine.config, now).unwrap();
    let capacity = routine.acquire_request_capacity(&sender).unwrap();
    let request_id = routine.state.allocate_request_id().unwrap();
    let TakenWork {
        download_floor,
        request,
    } = routine.select_work(&limits, request_id, now).unwrap();
    let unpublished = routine.fund(request, download_floor).unwrap();
    assert!(routine.io.budget.reserved() > 0, "funding took its charge");
    let request = block_range_request(&unpublished).unwrap();
    let writer = routine
        .authorize_response(&request, capacity.entry, capacity.exchange)
        .unwrap();
    let frame = routine.encode_request(&request).unwrap();
    interfere(routine, &mut fixture.outbound, &fixture.connection);
    routine.publish_request(
        unpublished,
        request,
        capacity.slot,
        frame,
        &writer,
        now,
        now + Duration::from_secs(60),
    )
}

/// A closed budget stops the pass before it acquires anything.
#[tokio::test]
async fn closed_accounting_stops_before_any_acquisition() {
    let mut fixture = fixture(1, &[1]);
    fixture
        .routine
        .io
        .work
        .budget_for_tests()
        .close(BudgetClosure::QueuePoisoned);
    let before = fixture.next_request_id();

    let report = fixture.routine.try_fill();

    assert_eq!(report.stop, FillStop::AccountingClosed);
    assert_eq!(fixture.next_request_id(), before);
    fixture.assert_settled(0);
}

/// Funding refuses a take that a reset retired after selection. The reset
/// already removed the work, so no later cleanup can restore or charge it.
#[tokio::test]
async fn a_reset_after_selection_refuses_funding() {
    let mut fixture = fixture(1, &[1]);
    let routine = &mut fixture.routine;
    let now = Instant::now();
    let limits = routine.state.fill_limits(&routine.config, now).unwrap();
    let request_id = routine.state.allocate_request_id().unwrap();
    let taken = routine.select_work(&limits, request_id, now).unwrap();
    routine.io.work.reset_above(block::Height(0));

    let refusal = routine.fund(taken.request, taken.download_floor);

    assert_eq!(refusal.unwrap_err(), FillStop::StaleWork);
    assert_eq!(routine.io.budget.reserved(), 0);
    assert_eq!(routine.io.work.in_flight_len(), 0);
    assert!(!routine.io.work.pending_contains(block::Height(1)));
}

/// A refused authorization drops the funded request, which returns its run and
/// its charge, and cancels the routine.
#[tokio::test]
async fn refused_authorization_rolls_back_funding() {
    let mut fixture = fixture(1, &[1]);
    let sender = fixture.routine.io.session.request_sender();
    let routine = &mut fixture.routine;
    let now = Instant::now();
    let limits = routine.state.fill_limits(&routine.config, now).unwrap();
    let held = routine.acquire_request_capacity(&sender).unwrap();
    let request_id = routine.state.allocate_request_id().unwrap();
    let taken = routine.select_work(&limits, request_id, now).unwrap();
    let unpublished = routine.fund(taken.request, taken.download_floor).unwrap();
    let request = block_range_request(&unpublished).unwrap();
    // An exchange that still owns this range refuses the overlapping reservation.
    let earlier = routine
        .authorize_response(&request, held.entry, held.exchange)
        .unwrap();
    let entry = routine.io.requester.pool.try_entry().unwrap();
    let exchange = routine.io.requester.open().unwrap();

    let refusal = routine.authorize_response(&request, entry, exchange);

    assert_eq!(refusal.err(), Some(FillStop::Internal));
    assert!(routine.io.cancel.is_cancelled());
    drop(unpublished);
    assert_eq!(routine.io.budget.reserved(), 0);
    assert!(routine.io.work.pending_contains(block::Height(1)));
    routine.io.requester.retire(block::Height(1), true);
    drop((earlier, held.slot));
    fixture.assert_settled(0);
}

/// The fence retires between authorization and publication, so the claim never
/// runs and nothing reaches the outstanding window.
#[tokio::test]
async fn fence_retirement_before_publication_rolls_back_funding() {
    let mut fixture = fixture(1, &[1]);

    let outcome = publish_after(&mut fixture, |_, _, connection| connection.cancel());

    assert!(matches!(outcome, PublishOutcome::FenceRefused));
    fixture.routine.io.requester.retire(block::Height(1), true);
    fixture.assert_settled(0);
}

/// Session cancellation wins after funding: the work claim refuses publication
/// under the queue lock.
#[tokio::test]
async fn session_cancellation_before_publication_refuses_the_claim() {
    let mut fixture = fixture(1, &[1]);

    let outcome = publish_after(&mut fixture, |routine, _, _| {
        routine.io.session.cancel_token().cancel();
    });

    assert!(matches!(outcome, PublishOutcome::ClaimRefused));
    fixture.routine.io.requester.retire(block::Height(1), true);
    fixture.assert_settled(0);
}

/// A reset wins after funding. The reset owns the work, so publication returns
/// only the funding and cannot restore the retired height.
#[tokio::test]
async fn a_reset_before_publication_refuses_the_claim() {
    let mut fixture = fixture(1, &[1]);

    let outcome = publish_after(&mut fixture, |routine, _, _| {
        routine.io.work.reset_above(block::Height(0));
    });

    assert!(matches!(outcome, PublishOutcome::ClaimRefused));
    let routine = &fixture.routine;
    assert_eq!(routine.io.budget.reserved(), 0);
    assert_eq!(routine.io.work.in_flight_len(), 0);
    assert!(!routine.io.work.pending_contains(block::Height(1)));
    assert!(routine.state.window.outstanding.is_empty());
}

/// Accounting closes after funding. Publication refuses the claim and the
/// provisional funding returns.
#[tokio::test]
async fn accounting_closure_before_publication_refuses_the_claim() {
    let mut fixture = fixture(1, &[1]);

    let outcome = publish_after(&mut fixture, |routine, _, _| {
        routine
            .io
            .work
            .budget_for_tests()
            .close(BudgetClosure::QueuePoisoned);
    });

    assert!(matches!(outcome, PublishOutcome::ClaimRefused));
    fixture.routine.io.requester.retire(block::Height(1), true);
    fixture.assert_settled(0);
}

/// The queue's receiver closes after the attempt reserved its slot. The claim
/// published, so the record exists until the routine collects the skipped write.
#[tokio::test]
async fn a_transport_close_during_publication_settles_the_claim() {
    let mut fixture = fixture(1, &[1]);

    let outcome = publish_after(&mut fixture, |_, outbound, _| outbound.close());

    let PublishOutcome::TransportClosed(claim) = outcome else {
        panic!("the send found a closed queue: {outcome:?}");
    };
    assert_eq!(fixture.routine.state.window.outstanding.len(), 1);
    claim.delivery_failed();
    fixture.routine.io.requester.retire(block::Height(1), true);
    drop(claim);
    assert!(fixture.routine.io.session.cancel_token().is_cancelled());
    assert_eq!(fixture.routine.io.budget.reserved(), 0);
    assert!(fixture.routine.io.work.pending_contains(block::Height(1)));

    fixture.routine.gc_skipped_outstanding();

    fixture.assert_settled(0);
}

/// A race-free publication hands the charge to the queue ledger.
#[tokio::test]
async fn published_funding_moves_to_the_queue_ledger() {
    let mut fixture = fixture(1, &[1]);

    let outcome = publish_after(&mut fixture, |_, _, _| {});

    assert!(matches!(outcome, PublishOutcome::Published(_)));
    let routine = &fixture.routine;
    assert_eq!(routine.io.budget.reserved(), 1_024);
    assert_eq!(routine.io.work.reserved_bytes(), 1_024);
    assert!(routine.io.work.in_flight_contains(block::Height(1)));
}

/// The retry filter reports each height it returned, and the report keeps the
/// wake that gives those heights another attempt.
#[tokio::test]
async fn filtered_work_stays_visible_in_the_fill_report() {
    let mut fixture = fixture(1, &[1]);
    let until = Instant::now() + Duration::from_secs(60);
    fixture
        .routine
        .state
        .retry_avoid
        .insert(block::Height(1), until);

    let report = fixture.routine.try_fill();

    assert_eq!(report.stop, FillStop::RetryAvoid);
    assert_eq!(report.sent, 0);
    assert_eq!(report.filtered_heights, 1);
    assert_eq!(report.retry_at, Some(until));
}

/// One funded and published request carries one exact owner through both rows.
#[tokio::test]
async fn funding_and_publication_trace_the_exact_owner() {
    let mut capture = TraceCapture::for_test("funding_and_publication_trace_the_exact_owner")
        .expect("trace capture initializes");
    let mut fixture = fixture(1, &[1]);
    fixture.routine.io.trace = ZakuraTrace::new(capture.tracer(), "01");

    let report = fixture.routine.try_fill();

    assert_eq!(report.sent, 1);
    let owner = fixture
        .routine
        .state
        .window
        .outstanding
        .entries()
        .next()
        .unwrap()
        .1
        .request
        .owner;
    let owner = request_owner(&owner);
    capture.flush().await;
    let reader = capture.reader().expect("trace rows load");
    let table = reader.table("block_sync");
    table.assert_row(
        bs_trace::BLOCK_REQUEST_FUNDED,
        &[
            ("request_owner", TraceValue::Str(&owner)),
            ("request_priority", TraceValue::Str("floor")),
            ("range_start", TraceValue::U64(1)),
            ("range_count", TraceValue::U64(1)),
            ("estimated_bytes", TraceValue::U64(1_024)),
        ],
    );
    table.assert_row(
        bs_trace::BLOCK_GET_BLOCKS_SENT,
        &[
            ("request_owner", TraceValue::Str(&owner)),
            ("range_start", TraceValue::U64(1)),
            ("estimated_bytes", TraceValue::U64(1_024)),
        ],
    );
}
