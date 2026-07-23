use std::{
    future::Future,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use color_eyre::eyre::{eyre, Report};
use tokio::{pin, select, sync::mpsc, task::JoinSet};
use tower::{Service, ServiceExt};
use tracing::{debug, warn};

use zakura_chain::{
    block::{self},
    chain_tip::ChainTip,
    parallel::commitment_aux::BlockCommitmentRoots,
};
use zakura_network::zakura::{
    commit_state_trace as cs_trace, BlockSyncFrontiers, Frontier, FrontierChange,
    HeaderRootAuthState, HeaderRootAuthenticationFailureKind, HeaderSyncAction,
    HeaderSyncCommitFailureKind, HeaderSyncEvent, HeaderSyncFrontiers, HeaderSyncOperationIdentity,
    ZakuraEndpoint, ZakuraHeaderSyncDriverStartup, ZakuraTrace, DEFAULT_HS_RANGE,
};
use zakura_state::MappedRequest;

#[cfg(test)]
use zakura_network::zakura::{BlockSyncEvent, BlockSyncHandle};

use super::{
    block_verify_error_is_duplicate, emit_commit_state, insert_cs_frontiers, insert_cs_hash,
    insert_cs_height, insert_cs_peer, insert_cs_str, insert_cs_u64, verified_block_tip_from_state,
};

const ROOT_AUTH_STATE_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_ROOT_AUTH_STATE_TASKS: usize = 1;

pub(crate) async fn zakura_header_sync_driver_startup(
    read_state: zakura_state::ReadStateService,
    network: &zakura_chain::parameters::Network,
) -> Result<ZakuraHeaderSyncDriverStartup, Report> {
    let header_root_auth = read_state
        .subscribe_header_root_auth()
        .borrow()
        .map(header_root_auth_state);
    let best_header_tip = match read_state
        .clone()
        .oneshot(zakura_state::ReadRequest::BestHeaderTip)
        .await
        .map_err(|error| eyre!("{error}"))?
    {
        zakura_state::ReadResponse::BestHeaderTip(tip) => tip,
        response => Err(eyre!("unexpected BestHeaderTip response: {response:?}"))?,
    };

    let finalized_tip = match read_state
        .clone()
        .oneshot(zakura_state::ReadRequest::FinalizedTip)
        .await
        .map_err(|error| eyre!("{error}"))?
    {
        zakura_state::ReadResponse::FinalizedTip(tip) => tip,
        response => Err(eyre!("unexpected FinalizedTip response: {response:?}"))?,
    };

    let verified_block_tip = match read_state
        .clone()
        .oneshot(zakura_state::ReadRequest::Tip)
        .await
        .map_err(|error| eyre!("{error}"))?
    {
        zakura_state::ReadResponse::Tip(tip) => tip,
        response => Err(eyre!("unexpected Tip response: {response:?}"))?,
    };

    let empty_state_tip = (block::Height(0), network.genesis_hash());
    let finalized_height = finalized_tip.map_or(block::Height(0), |(height, _)| height);
    let verified_block_tip =
        verified_block_tip_from_state(finalized_tip, verified_block_tip, empty_state_tip);
    let best_header_tip = best_header_tip.unwrap_or(empty_state_tip);

    Ok(ZakuraHeaderSyncDriverStartup {
        frontiers: HeaderSyncFrontiers {
            finalized_height,
            verified_block_tip: verified_block_tip.0,
            verified_block_hash: verified_block_tip.1,
        },
        best_header_tip: Some(best_header_tip),
        verified_block_tip_hash: verified_block_tip.1,
        header_root_auth,
    })
}

fn header_root_auth_state(state: zakura_state::HeaderRootAuthState) -> HeaderRootAuthState {
    HeaderRootAuthState {
        authenticated_height: state.authenticated_height,
        authenticated_hash: state.authenticated_hash,
        completed_checkpoint_height: state.completed_checkpoint_height,
        completed_checkpoint_hash: state.completed_checkpoint_hash,
    }
}

fn state_header_root_auth_state(state: HeaderRootAuthState) -> zakura_state::HeaderRootAuthState {
    zakura_state::HeaderRootAuthState {
        authenticated_height: state.authenticated_height,
        authenticated_hash: state.authenticated_hash,
        completed_checkpoint_height: state.completed_checkpoint_height,
        completed_checkpoint_hash: state.completed_checkpoint_hash,
    }
}

pub(crate) async fn drive_header_root_auth_updates(
    read_state: zakura_state::ReadStateService,
    header_sync: zakura_network::zakura::HeaderSyncHandle,
    shutdown: impl Future<Output = ()> + Send + 'static,
) {
    let updates = read_state.subscribe_header_root_auth();
    drive_header_root_auth_watch_updates(updates, shutdown, move |state| {
        let header_sync = header_sync.clone();
        async move {
            header_sync
                .send(HeaderSyncEvent::HeaderRootAuthStateChanged(state))
                .await
                .is_ok()
        }
    })
    .await;
}

async fn drive_header_root_auth_watch_updates<Deliver, Delivery>(
    mut updates: tokio::sync::watch::Receiver<Option<zakura_state::HeaderRootAuthState>>,
    shutdown: impl Future<Output = ()> + Send + 'static,
    mut deliver: Deliver,
) where
    Deliver: FnMut(Option<HeaderRootAuthState>) -> Delivery,
    Delivery: Future<Output = bool>,
{
    pin!(shutdown);
    let initial = updates.borrow_and_update().map(header_root_auth_state);
    if !deliver(initial).await {
        return;
    }
    loop {
        select! {
            _ = &mut shutdown => return,
            changed = updates.changed() => {
                if changed.is_err() {
                    return;
                }
                let state = updates.borrow_and_update().map(header_root_auth_state);
                if !deliver(state).await {
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
async fn root_covered_best_header_tip_or_verified<ReadState>(
    read_state: ReadState,
    best_header_tip: (block::Height, block::Hash),
    verified_block_tip: (block::Height, block::Hash),
) -> Result<(block::Height, block::Hash), Report>
where
    ReadState: Service<
            zakura_state::ReadRequest,
            Response = zakura_state::ReadResponse,
            Error = zakura_state::BoxError,
        > + Send
        + 'static,
    ReadState::Future: Send + 'static,
{
    if best_header_tip.0 <= verified_block_tip.0 {
        return Ok(best_header_tip);
    }

    let Ok(start_height) = verified_block_tip.0.next() else {
        return Ok(verified_block_tip);
    };
    let best_header_height = best_header_tip.0;
    let verified_block_height = verified_block_tip.0;
    let count = best_header_height
        .0
        .checked_sub(verified_block_height.0)
        .ok_or_else(|| eyre!("best header tip is unexpectedly below verified block tip"))?;
    let roots = match read_state
        .oneshot(zakura_state::ReadRequest::BlockRoots {
            start_height,
            count,
        })
        .await
        .map_err(|error| eyre!("{error}"))?
    {
        zakura_state::ReadResponse::BlockRoots(roots) => roots,
        response => Err(eyre!("unexpected BlockRoots response: {response:?}"))?,
    };

    if block_roots_cover_range(start_height, count, &roots) {
        Ok(best_header_tip)
    } else {
        Ok(verified_block_tip)
    }
}

#[cfg(test)]
pub(crate) async fn root_covered_query_best_header_tip<ReadState>(
    read_state: ReadState,
    best_header_tip: (block::Height, block::Hash),
) -> Result<(block::Height, block::Hash), Report>
where
    ReadState: Service<
            zakura_state::ReadRequest,
            Response = zakura_state::ReadResponse,
            Error = zakura_state::BoxError,
        > + Clone
        + Send
        + 'static,
    ReadState::Future: Send + 'static,
{
    let verified_block_tip = match read_state
        .clone()
        .oneshot(zakura_state::ReadRequest::Tip)
        .await
        .map_err(|error| eyre!("{error}"))?
    {
        zakura_state::ReadResponse::Tip(Some(tip)) => tip,
        zakura_state::ReadResponse::Tip(None) => return Ok(best_header_tip),
        response => Err(eyre!("unexpected Tip response: {response:?}"))?,
    };

    root_covered_best_header_tip_or_verified(read_state, best_header_tip, verified_block_tip).await
}

#[cfg(test)]
pub(crate) fn block_roots_cover_range(
    start_height: block::Height,
    count: u32,
    roots: &[BlockCommitmentRoots],
) -> bool {
    if roots.len() != usize::try_from(count).unwrap_or(usize::MAX) {
        return false;
    }

    roots.iter().enumerate().all(|(offset, roots)| {
        let Ok(offset) = u32::try_from(offset) else {
            return false;
        };
        start_height
            .0
            .checked_add(offset)
            .is_some_and(|height| roots.height == block::Height(height))
    })
}

fn header_range_committed(
    operation: HeaderSyncOperationIdentity,
    tip_hash: block::Hash,
) -> HeaderSyncEvent {
    HeaderSyncEvent::HeaderRangeOperationCompleted {
        operation,
        tip_hash,
    }
}

fn header_range_commit_failed(
    operation: HeaderSyncOperationIdentity,
    kind: HeaderSyncCommitFailureKind,
) -> HeaderSyncEvent {
    HeaderSyncEvent::HeaderRangeOperationFailed { operation, kind }
}

fn header_root_authentication_completed(operation: HeaderSyncOperationIdentity) -> HeaderSyncEvent {
    HeaderSyncEvent::HeaderRootAuthenticationCompleted { operation }
}

fn header_root_authentication_failed(
    operation: HeaderSyncOperationIdentity,
    kind: HeaderRootAuthenticationFailureKind,
) -> HeaderSyncEvent {
    HeaderSyncEvent::HeaderRootAuthenticationFailed { operation, kind }
}

/// Convert a finished root-auth JoinSet entry into the reactor settlement event.
///
/// A panicked (non-cancelled) task must still settle the reactor's pending
/// `AuthenticateRoots` op; otherwise both admission gates stay blocked forever.
fn settle_root_auth_task_join(
    joined: Option<Result<HeaderSyncEvent, tokio::task::JoinError>>,
    in_flight: &mut Option<HeaderSyncOperationIdentity>,
) -> Option<HeaderSyncEvent> {
    match joined {
        Some(Ok(event)) => {
            let _ = in_flight.take();
            Some(event)
        }
        Some(Err(error)) if !error.is_cancelled() => {
            let Some(operation) = in_flight.take() else {
                warn!(
                    ?error,
                    "header-root authentication task failed without a tracked operation"
                );
                return None;
            };
            warn!(
                ?error,
                ?operation,
                "header-root authentication task failed; synthesizing local failure"
            );
            Some(header_root_authentication_failed(
                operation,
                HeaderRootAuthenticationFailureKind::Local,
            ))
        }
        Some(Err(_)) | None => {
            let _ = in_flight.take();
            None
        }
    }
}

fn header_root_authentication_failure_kind(
    error: &(dyn std::error::Error + Send + Sync + 'static),
) -> HeaderRootAuthenticationFailureKind {
    error
        .downcast_ref::<zakura_state::AuthenticateHeaderRootsError>()
        .map_or(
            HeaderRootAuthenticationFailureKind::Local,
            |error| match error {
                zakura_state::AuthenticateHeaderRootsError::NonCanonicalHeader { height } => {
                    HeaderRootAuthenticationFailureKind::CanonicalMismatch { height: *height }
                }
                zakura_state::AuthenticateHeaderRootsError::Verification { .. } => {
                    HeaderRootAuthenticationFailureKind::InvalidPeerRange
                }
                zakura_state::AuthenticateHeaderRootsError::StaleState { .. }
                | zakura_state::AuthenticateHeaderRootsError::AnchorMismatch { .. }
                | zakura_state::AuthenticateHeaderRootsError::StartMismatch { .. }
                | zakura_state::AuthenticateHeaderRootsError::WitnessAboveCompletedCheckpoint {
                    ..
                } => HeaderRootAuthenticationFailureKind::Stale,
                zakura_state::AuthenticateHeaderRootsError::CountMismatch { .. }
                | zakura_state::AuthenticateHeaderRootsError::MissingSuccessorWitness { .. }
                | zakura_state::AuthenticateHeaderRootsError::NonContiguous { .. }
                | zakura_state::AuthenticateHeaderRootsError::HeightOverflow
                | zakura_state::AuthenticateHeaderRootsError::Frontier(_) => {
                    HeaderRootAuthenticationFailureKind::Local
                }
            },
        )
}

#[cfg(test)]
mod operation_identity_tests {
    use super::*;
    use zakura_network::zakura::{
        HeaderSyncOperationKind, HeaderSyncRequestId, HeaderSyncWireRequestIdentity, ZakuraPeerId,
    };

    fn operation() -> HeaderSyncOperationIdentity {
        HeaderSyncOperationIdentity {
            wire_request: HeaderSyncWireRequestIdentity {
                peer: ZakuraPeerId::new(vec![1; 32]).expect("test peer ID is valid"),
                session_id: 7,
                request_id: HeaderSyncRequestId::new(9).expect("test request ID is non-zero"),
            },
            op_kind: HeaderSyncOperationKind::CommitHeaders,
        }
    }

    #[test]
    fn commit_completion_events_echo_exact_operation_identity() {
        let operation = operation();
        let tip_hash = block::Hash([3; 32]);
        assert!(matches!(
            header_range_committed(operation.clone(), tip_hash),
            HeaderSyncEvent::HeaderRangeOperationCompleted {
                operation: echoed,
                tip_hash: echoed_hash,
            } if echoed == operation && echoed_hash == tip_hash
        ));

        for kind in [
            HeaderSyncCommitFailureKind::InvalidPeerRange,
            HeaderSyncCommitFailureKind::Local,
        ] {
            assert!(matches!(
                header_range_commit_failed(operation.clone(), kind),
                HeaderSyncEvent::HeaderRangeOperationFailed {
                    operation: echoed,
                    kind: echoed_kind,
                } if echoed == operation && echoed_kind == kind
            ));
        }
    }

    #[test]
    fn root_auth_events_echo_exact_operation_identity() {
        let mut operation = operation();
        operation.op_kind = HeaderSyncOperationKind::AuthenticateRoots;

        assert!(matches!(
            header_root_authentication_completed(operation.clone()),
            HeaderSyncEvent::HeaderRootAuthenticationCompleted { operation: echoed }
                if echoed == operation
        ));
        for kind in [
            HeaderRootAuthenticationFailureKind::Stale,
            HeaderRootAuthenticationFailureKind::InvalidPeerRange,
            HeaderRootAuthenticationFailureKind::Local,
        ] {
            assert!(matches!(
                header_root_authentication_failed(operation.clone(), kind),
                HeaderSyncEvent::HeaderRootAuthenticationFailed {
                    operation: echoed,
                    kind: echoed_kind,
                } if echoed == operation && echoed_kind == kind
            ));
        }
    }

    #[test]
    fn root_auth_error_classes_preserve_peer_attribution_policy() {
        let state = zakura_state::HeaderRootAuthState {
            authenticated_height: block::Height(1),
            authenticated_hash: block::Hash([1; 32]),
            completed_checkpoint_height: block::Height(3),
            completed_checkpoint_hash: block::Hash([3; 32]),
        };
        let stale = zakura_state::AuthenticateHeaderRootsError::StaleState {
            expected: state,
            current: state,
        };
        let invalid = zakura_state::AuthenticateHeaderRootsError::CountMismatch {
            headers: 2,
            roots: 1,
        };
        let canonical_mismatch = zakura_state::AuthenticateHeaderRootsError::NonCanonicalHeader {
            height: block::Height(2),
        };
        let local = std::io::Error::other("local state service failure");

        assert_eq!(
            header_root_authentication_failure_kind(&stale),
            HeaderRootAuthenticationFailureKind::Stale
        );
        assert_eq!(
            header_root_authentication_failure_kind(&invalid),
            HeaderRootAuthenticationFailureKind::Local
        );
        assert_eq!(
            header_root_authentication_failure_kind(&canonical_mismatch),
            HeaderRootAuthenticationFailureKind::CanonicalMismatch {
                height: block::Height(2)
            }
        );
        assert_eq!(
            header_root_authentication_failure_kind(&local),
            HeaderRootAuthenticationFailureKind::Local
        );
    }

    #[tokio::test]
    async fn panicked_root_auth_join_synthesizes_local_failure() {
        let operation = {
            let mut operation = operation();
            operation.op_kind = HeaderSyncOperationKind::AuthenticateRoots;
            operation
        };
        let mut in_flight = Some(operation.clone());
        let panic_join = tokio::spawn(async {
            panic!("simulated root-auth task panic");
        })
        .await
        .expect_err("join must surface the panic");

        let event = settle_root_auth_task_join(Some(Err(panic_join)), &mut in_flight);
        assert!(
            matches!(
                event,
                Some(HeaderSyncEvent::HeaderRootAuthenticationFailed {
                    operation: ref echoed,
                    kind: HeaderRootAuthenticationFailureKind::Local,
                }) if *echoed == operation
            ),
            "unexpected settlement event: {event:?}"
        );
        assert!(
            in_flight.is_none(),
            "panic settlement must clear the tracked operation"
        );
    }

    #[tokio::test]
    async fn cancelled_root_auth_join_does_not_synthesize_failure() {
        let operation = {
            let mut operation = operation();
            operation.op_kind = HeaderSyncOperationKind::AuthenticateRoots;
            operation
        };
        let mut in_flight = Some(operation);
        let handle = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        handle.abort();
        let cancelled_join = handle
            .await
            .expect_err("aborted join must surface cancellation");
        assert!(cancelled_join.is_cancelled());

        let event = settle_root_auth_task_join(Some(Err(cancelled_join)), &mut in_flight);
        assert!(event.is_none(), "cancelled joins must not settle as Local");
        assert!(
            in_flight.is_none(),
            "cancelled joins still clear the tracked operation"
        );
    }

    #[tokio::test]
    async fn successful_root_auth_join_forwards_event_and_clears_tracker() {
        let operation = {
            let mut operation = operation();
            operation.op_kind = HeaderSyncOperationKind::AuthenticateRoots;
            operation
        };
        let mut in_flight = Some(operation.clone());
        let completed = header_root_authentication_completed(operation.clone());

        let event = settle_root_auth_task_join(Some(Ok(completed)), &mut in_flight);
        assert!(
            matches!(
                event,
                Some(HeaderSyncEvent::HeaderRootAuthenticationCompleted {
                    operation: ref echoed,
                }) if *echoed == operation
            ),
            "unexpected settlement event: {event:?}"
        );
        assert!(in_flight.is_none());
    }

    #[tokio::test]
    async fn root_auth_watch_emits_initial_value_before_waiting() {
        let state = zakura_state::HeaderRootAuthState {
            authenticated_height: block::Height(7),
            authenticated_hash: block::Hash([7; 32]),
            completed_checkpoint_height: block::Height(9),
            completed_checkpoint_hash: block::Hash([9; 32]),
        };
        let (_sender, receiver) = tokio::sync::watch::channel(Some(state));
        let (delivered_tx, mut delivered_rx) = mpsc::channel(1);
        let task = tokio::spawn(drive_header_root_auth_watch_updates(
            receiver,
            std::future::pending(),
            move |state| {
                let delivered_tx = delivered_tx.clone();
                async move { delivered_tx.send(state).await.is_ok() }
            },
        ));

        let delivered = tokio::time::timeout(Duration::from_secs(1), delivered_rx.recv())
            .await
            .expect("initial watch value is delivered without a change")
            .expect("delivery channel stays open");
        assert_eq!(delivered, Some(header_root_auth_state(state)));
        task.abort();
    }
}

#[derive(Clone)]
pub(crate) struct ZakuraHeaderSyncDriverHandles {
    pub(crate) endpoint: ZakuraEndpoint,
    pub(crate) header_sync: zakura_network::zakura::HeaderSyncHandle,
}

pub(crate) async fn drive_vct_root_repairs(
    read_state: zakura_state::ReadStateService,
    header_sync: zakura_network::zakura::HeaderSyncHandle,
    shutdown: impl Future<Output = ()> + Send + 'static,
) {
    let repairs = read_state.subscribe_vct_root_repairs();
    drive_vct_root_repair_updates(repairs, shutdown, move |status| {
        let read_state = read_state.clone();
        let header_sync = header_sync.clone();
        async move {
            match status.state {
                zakura_state::VctRootRepairState::Idle => header_sync
                    .send(HeaderSyncEvent::VctRootRepairResolved {
                        generation: status.generation,
                    })
                    .await
                    .is_ok(),
                zakura_state::VctRootRepairState::Unavailable { height } => {
                    let Some(event) =
                        vct_root_repair_event(read_state, height, status.generation).await
                    else {
                        return false;
                    };
                    header_sync.send(event).await.is_ok()
                }
            }
        }
    })
    .await;
}

async fn drive_vct_root_repair_updates<Deliver, Delivery>(
    mut repairs: tokio::sync::watch::Receiver<zakura_state::VctRootRepairStatus>,
    shutdown: impl Future<Output = ()>,
    mut deliver: Deliver,
) where
    Deliver: FnMut(zakura_state::VctRootRepairStatus) -> Delivery,
    Delivery: Future<Output = bool>,
{
    const RETRY_DELAY: Duration = Duration::from_millis(500);
    /// One warning per this many consecutive failed deliveries (~30s at
    /// [`RETRY_DELAY`]), so a permanently undeliverable repair status is
    /// visible to operators instead of an invisible silent retry loop.
    const RETRY_WARN_EVERY: u32 = 60;

    pin!(shutdown);
    let mut status = *repairs.borrow_and_update();
    // A repair can already be pending when this driver subscribes. Idle is not
    // sent initially because there cannot yet be a driver-owned repair to clear.
    let mut delivery_pending = matches!(
        status.state,
        zakura_state::VctRootRepairState::Unavailable { .. }
    );
    let mut consecutive_failures: u32 = 0;

    loop {
        if delivery_pending {
            delivery_pending = !deliver(status).await;
            if delivery_pending {
                consecutive_failures = consecutive_failures.saturating_add(1);
                if consecutive_failures.is_multiple_of(RETRY_WARN_EVERY) {
                    tracing::warn!(
                        ?status,
                        consecutive_failures,
                        "VCT root repair status could not be delivered to header sync \
                         (state read or event send keeps failing); still retrying"
                    );
                }
            } else {
                consecutive_failures = 0;
            }
        }

        select! {
            _ = &mut shutdown => return,
            changed = repairs.changed() => {
                if changed.is_err() {
                    return;
                }
                status = *repairs.borrow_and_update();
                delivery_pending = true;
                consecutive_failures = 0;
            }
            _ = tokio::time::sleep(RETRY_DELAY), if delivery_pending => {}
        }
    }
}

async fn vct_root_repair_event(
    read_state: zakura_state::ReadStateService,
    height: block::Height,
    generation: u64,
) -> Option<HeaderSyncEvent> {
    let anchor_height = height.0.checked_sub(1).map(block::Height)?;
    let response = read_state
        .oneshot(zakura_state::ReadRequest::HeadersByHeightRange {
            start: anchor_height,
            count: 3,
        })
        .await
        .ok()?;
    let zakura_state::ReadResponse::Headers(headers) = response else {
        return None;
    };
    if headers.len() < 2 {
        return None;
    }

    let (stored_anchor_height, anchor_hash, anchor_header) = &headers[0];
    if *stored_anchor_height != anchor_height {
        return None;
    }
    let mut expected_hashes: Vec<(block::Height, block::Hash)> = Vec::new();
    for (expected_offset, (candidate_height, candidate_hash, candidate_header)) in
        headers.iter().skip(1).take(2).enumerate()
    {
        let expected_height = height.0.checked_add(u32::try_from(expected_offset).ok()?)?;
        if *candidate_height != block::Height(expected_height) {
            return None;
        }
        let expected_parent = if expected_offset == 0 {
            *anchor_hash
        } else {
            expected_hashes.last().map(|(_, hash)| *hash)?
        };
        if candidate_header.previous_block_hash != expected_parent {
            return None;
        }
        expected_hashes.push((*candidate_height, *candidate_hash));
    }
    if expected_hashes.is_empty() || block::Hash::from(anchor_header.as_ref()) != *anchor_hash {
        return None;
    }

    Some(HeaderSyncEvent::VctRootRepairRequested {
        height,
        generation,
        anchor_hash: *anchor_hash,
        expected_hashes,
    })
}

#[cfg(test)]
mod vct_root_repair_driver_tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use super::*;

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn retries_transient_delivery_failure_without_another_watch_change() {
        let status = zakura_state::VctRootRepairStatus {
            state: zakura_state::VctRootRepairState::Unavailable {
                height: block::Height(42),
            },
            generation: 1,
        };
        let (_repairs_tx, repairs_rx) = tokio::sync::watch::channel(status);
        let attempts = Arc::new(AtomicUsize::new(0));
        let delivery_attempts = attempts.clone();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        let driver = tokio::spawn(drive_vct_root_repair_updates(
            repairs_rx,
            async move {
                let _ = shutdown_rx.await;
            },
            move |delivered_status| {
                assert_eq!(delivered_status, status);
                let attempt = delivery_attempts.fetch_add(1, Ordering::SeqCst);
                async move { attempt > 0 }
            },
        ));

        tokio::task::yield_now().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 1);

        tokio::time::advance(Duration::from_millis(500)).await;
        tokio::task::yield_now().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 2);

        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            2,
            "a successful delivery must stop timer retries"
        );

        shutdown_tx.send(()).expect("driver is still running");
        driver.await.expect("driver shuts down cleanly");
    }
}

pub(crate) async fn drive_zakura_header_sync_actions<State, ReadState, BlockVerifier>(
    mut actions: mpsc::Receiver<HeaderSyncAction>,
    handles: ZakuraHeaderSyncDriverHandles,
    state: State,
    read_state: ReadState,
    block_verifier: BlockVerifier,
    trace: ZakuraTrace,
    shutdown: impl Future<Output = ()> + Send + 'static,
) where
    State: Service<
            zakura_state::Request,
            Response = zakura_state::Response,
            Error = zakura_state::BoxError,
        > + Clone
        + Send
        + 'static,
    State::Future: Send + 'static,
    ReadState: Service<
            zakura_state::ReadRequest,
            Response = zakura_state::ReadResponse,
            Error = zakura_state::BoxError,
        > + Clone
        + Send
        + 'static,
    ReadState::Future: Send + 'static,
    BlockVerifier:
        Service<zakura_consensus::Request, Response = block::Hash> + Clone + Send + 'static,
    BlockVerifier::Error: std::fmt::Debug + Send + Sync + 'static,
    BlockVerifier::Future: Send + 'static,
{
    pin!(shutdown);
    let mut root_auth_tasks = JoinSet::new();
    let mut in_flight_root_auth = None;
    loop {
        let action = select! {
            _ = &mut shutdown => {
                root_auth_tasks.abort_all();
                while root_auth_tasks.join_next().await.is_some() {}
                return;
            },
            completed = root_auth_tasks.join_next(), if !root_auth_tasks.is_empty() => {
                if let Some(event) =
                    settle_root_auth_task_join(completed, &mut in_flight_root_auth)
                {
                    let _ = handles.header_sync.send(event).await;
                }
                continue;
            },
            action = actions.recv() => {
                let Some(action) = action else {
                    root_auth_tasks.abort_all();
                    while root_auth_tasks.join_next().await.is_some() {}
                    return;
                };
                action
            }
        };

        trace_header_driver_action(&trace, &action);
        match action {
            HeaderSyncAction::Misbehavior { peer, reason } => {
                // Record-only: peer scoring no longer drives disconnects.
                debug!(?peer, ?reason, "recorded Zakura header-sync peer violation");
            }
            HeaderSyncAction::NewBlockReceived {
                peer,
                height,
                hash,
                block,
            } => {
                emit_commit_state(
                    &trace,
                    cs_trace::COMMIT_START,
                    "header_sync_driver",
                    |row| {
                        insert_cs_str(row, cs_trace::ACTION, "new_block");
                        insert_cs_peer(row, cs_trace::PEER, &peer);
                        insert_cs_height(row, cs_trace::HEIGHT, height);
                        insert_cs_hash(row, cs_trace::HASH, hash);
                    },
                );
                let started = Instant::now();
                match block_verifier
                    .clone()
                    .oneshot(zakura_consensus::Request::Commit(block.clone()))
                    .await
                {
                    Ok(committed_hash) if committed_hash == hash => {
                        // A contextually valid block also commits when it does
                        // not land on the best chain, but only a best-chain
                        // block may advance the header/verified frontiers or be
                        // forwarded to peers: gossiping non-best-chain blocks
                        // makes the whole Zakura layer follow a losing branch
                        // while the node's own chain stays honest, stranding
                        // zakura-only peers.
                        let on_best_chain =
                            new_block_is_on_best_chain(read_state.clone(), hash).await;
                        let result_label = if on_best_chain {
                            "accepted"
                        } else {
                            "accepted_non_best_chain"
                        };
                        trace_header_commit_finish(
                            &trace,
                            "new_block",
                            &peer,
                            height,
                            hash,
                            result_label,
                            started,
                        );
                        trace_header_reactor_event(
                            &trace,
                            if on_best_chain {
                                "new_block_accepted"
                            } else {
                                "new_block_accepted_non_best_chain"
                            },
                            Some(&peer),
                            height,
                            hash,
                            1,
                        );
                        let event = if on_best_chain {
                            HeaderSyncEvent::NewBlockAccepted {
                                peer,
                                height,
                                hash,
                                block,
                            }
                        } else {
                            debug!(
                                ?peer,
                                ?height,
                                ?hash,
                                "Zakura NewBlock did not land on the best chain; \
                                 not advancing frontiers or forwarding"
                            );
                            HeaderSyncEvent::NewBlockAcceptedNonBestChain { peer, height, hash }
                        };
                        let _ = handles.header_sync.send(event).await;
                    }
                    Ok(committed_hash) => {
                        trace_header_commit_finish(
                            &trace,
                            "new_block",
                            &peer,
                            height,
                            hash,
                            "rejected",
                            started,
                        );
                        warn!(
                            ?peer,
                            ?hash,
                            ?committed_hash,
                            "Zakura NewBlock verifier returned an unexpected hash"
                        );
                        trace_header_reactor_event(
                            &trace,
                            "new_block_rejected",
                            Some(&peer),
                            height,
                            hash,
                            1,
                        );
                        let _ = handles
                            .header_sync
                            .send(HeaderSyncEvent::NewBlockRejected { peer, hash })
                            .await;
                    }
                    Err(error) => {
                        if block_verify_error_is_duplicate(&error) {
                            trace_header_commit_finish(
                                &trace,
                                "new_block",
                                &peer,
                                height,
                                hash,
                                "duplicate",
                                started,
                            );
                            debug!(
                                ?peer,
                                ?height,
                                ?hash,
                                ?error,
                                "Zakura NewBlock was already known by the block verifier"
                            );
                            trace_header_reactor_event(
                                &trace,
                                "new_block_duplicate",
                                Some(&peer),
                                height,
                                hash,
                                1,
                            );
                            let _ = handles
                                .header_sync
                                .send(HeaderSyncEvent::NewBlockDuplicate { peer, height, hash })
                                .await;
                            continue;
                        }

                        trace_header_commit_finish(
                            &trace,
                            "new_block",
                            &peer,
                            height,
                            hash,
                            "rejected",
                            started,
                        );
                        debug!(
                            ?peer,
                            ?hash,
                            ?error,
                            "Zakura NewBlock rejected by block verifier"
                        );
                        trace_header_reactor_event(
                            &trace,
                            "new_block_rejected",
                            Some(&peer),
                            height,
                            hash,
                            1,
                        );
                        let _ = handles
                            .header_sync
                            .send(HeaderSyncEvent::NewBlockRejected { peer, hash })
                            .await;
                    }
                }
            }
            HeaderSyncAction::QueryHeadersByHeightRange {
                peer,
                session_id,
                request_id,
                start,
                count,
                want_tree_aux_roots,
            } => {
                trace_state_read_start(
                    &trace,
                    "query_headers_by_height_range",
                    Some(&peer),
                    start,
                    count,
                );
                let started = Instant::now();
                match read_state
                    .clone()
                    .oneshot(zakura_state::ReadRequest::HeadersByHeightRange { start, count })
                    .await
                {
                    Ok(zakura_state::ReadResponse::Headers(headers)) => {
                        emit_commit_state(
                            &trace,
                            cs_trace::STATE_READ_SUCCESS,
                            "header_sync_driver",
                            |row| {
                                insert_cs_str(
                                    row,
                                    cs_trace::ACTION,
                                    "query_headers_by_height_range",
                                );
                                insert_cs_peer(row, cs_trace::PEER, &peer);
                                insert_cs_height(row, cs_trace::RANGE_START, start);
                                insert_cs_u64(row, cs_trace::RANGE_COUNT, headers.len() as u64);
                                insert_cs_u64(row, cs_trace::ELAPSED_MS, elapsed_ms(started));
                            },
                        );
                        trace_state_read_start(
                            &trace,
                            "block_size_hints",
                            Some(&peer),
                            start,
                            count,
                        );
                        let body_size_hints = match read_state
                            .clone()
                            .oneshot(zakura_state::ReadRequest::BlockSizeHints {
                                from: start,
                                count,
                            })
                            .await
                        {
                            Ok(zakura_state::ReadResponse::BlockSizeHints(hints)) => hints,
                            Ok(response) => {
                                trace_state_read_error(
                                    &trace,
                                    "block_size_hints",
                                    Some(&peer),
                                    start,
                                    count,
                                    "unexpected_response",
                                    started,
                                );
                                warn!(?peer, ?response, "unexpected BlockSizeHints response");
                                Vec::new()
                            }
                            Err(error) => {
                                trace_state_read_error(
                                    &trace,
                                    "block_size_hints",
                                    Some(&peer),
                                    start,
                                    count,
                                    &format!("{error}"),
                                    started,
                                );
                                warn!(
                                    ?peer,
                                    ?error,
                                    "failed to read Zakura BlockSizeHints response from state"
                                );
                                Vec::new()
                            }
                        };
                        let block_roots = if want_tree_aux_roots {
                            trace_state_read_start(
                                &trace,
                                "block_roots",
                                Some(&peer),
                                start,
                                count,
                            );
                            match read_state
                                .clone()
                                .oneshot(zakura_state::ReadRequest::BlockRoots {
                                    start_height: start,
                                    count,
                                })
                                .await
                            {
                                Ok(zakura_state::ReadResponse::BlockRoots(roots)) => roots,
                                Ok(response) => {
                                    trace_state_read_error(
                                        &trace,
                                        "block_roots",
                                        Some(&peer),
                                        start,
                                        count,
                                        "unexpected_response",
                                        started,
                                    );
                                    warn!(?peer, ?response, "unexpected BlockRoots response");
                                    Vec::new()
                                }
                                Err(error) => {
                                    trace_state_read_error(
                                        &trace,
                                        "block_roots",
                                        Some(&peer),
                                        start,
                                        count,
                                        &format!("{error}"),
                                        started,
                                    );
                                    warn!(
                                        ?peer,
                                        ?error,
                                        "failed to read Zakura BlockRoots response from state"
                                    );
                                    Vec::new()
                                }
                            }
                        } else {
                            Vec::new()
                        };
                        let header_heights: Vec<_> =
                            headers.iter().map(|(height, _, _)| *height).collect();
                        let tree_aux_roots = if want_tree_aux_roots {
                            tree_aux_roots_for_served_header_range(
                                start,
                                header_heights.iter().copied(),
                                &block_roots,
                            )
                            .unwrap_or_else(|error| {
                                metrics::counter!("sync.header.tree_aux.sender_alignment_failure")
                                    .increment(1);
                                static ALIGNMENT_FAILURES: AtomicU64 = AtomicU64::new(0);
                                let occurrences =
                                    ALIGNMENT_FAILURES.fetch_add(1, Ordering::Relaxed) + 1;
                                if occurrences.is_power_of_two() {
                                    warn!(
                                        ?peer,
                                        ?start,
                                        requested_count = count,
                                        occurrences,
                                        ?error,
                                        "serving header range without tree aux roots"
                                    );
                                }

                                Vec::new()
                            })
                        } else {
                            Vec::new()
                        };
                        let body_sizes = body_sizes_for_served_header_range(
                            start,
                            header_heights.iter().copied(),
                            &body_size_hints,
                        );
                        let headers = headers
                            .into_iter()
                            .map(|(_height, _hash, header)| header)
                            .collect();
                        trace_header_reactor_event(
                            &trace,
                            "header_range_response_ready",
                            Some(&peer),
                            start,
                            block::Hash([0; 32]),
                            count,
                        );
                        let _ = handles
                            .header_sync
                            .send(HeaderSyncEvent::HeaderRangeResponseReady {
                                peer,
                                session_id,
                                request_id,
                                start_height: start,
                                requested_count: count,
                                want_tree_aux_roots,
                                headers,
                                body_sizes,
                                tree_aux_roots,
                            })
                            .await;
                    }
                    Ok(response) => {
                        trace_state_read_error(
                            &trace,
                            "query_headers_by_height_range",
                            Some(&peer),
                            start,
                            count,
                            "unexpected_response",
                            started,
                        );
                        warn!(?peer, ?response, "unexpected HeadersByHeightRange response");
                        trace_header_range_finished(&trace, &peer, start, count, 0);
                        let _ = handles
                            .header_sync
                            .send(HeaderSyncEvent::HeaderRangeResponseFinished {
                                peer,
                                session_id,
                                request_id,
                                start_height: start,
                                requested_count: count,
                                returned_count: 0,
                            })
                            .await;
                    }
                    Err(error) => {
                        trace_state_read_error(
                            &trace,
                            "query_headers_by_height_range",
                            Some(&peer),
                            start,
                            count,
                            &format!("{error}"),
                            started,
                        );
                        warn!(
                            ?peer,
                            ?error,
                            "failed to read Zakura Headers response from state"
                        );
                        trace_header_range_finished(&trace, &peer, start, count, 0);
                        let _ = handles
                            .header_sync
                            .send(HeaderSyncEvent::HeaderRangeResponseFinished {
                                peer,
                                session_id,
                                request_id,
                                start_height: start,
                                requested_count: count,
                                returned_count: 0,
                            })
                            .await;
                    }
                }
            }
            HeaderSyncAction::CommitHeaderRange {
                operation,
                anchor,
                payload,
                finalized: _finalized,
            } => {
                let peer = operation.wire_request.peer.clone();
                let range = payload.range();
                let start_height = range.start();
                let count = range.count();
                let tree_aux_roots_len = payload
                    .tree_aux_roots()
                    .map_or(0, |roots| u32::try_from(roots.len()).unwrap_or(u32::MAX));
                let (_range, headers, body_sizes, tree_aux_roots) = payload.into_parts();
                let tree_aux_roots = tree_aux_roots.unwrap_or_default();
                emit_commit_state(
                    &trace,
                    cs_trace::COMMIT_START,
                    "header_sync_driver",
                    |row| {
                        insert_cs_str(row, cs_trace::ACTION, "commit_header_range");
                        insert_cs_peer(row, cs_trace::PEER, &peer);
                        insert_cs_height(row, cs_trace::RANGE_START, start_height);
                        insert_cs_u64(row, cs_trace::RANGE_COUNT, u64::from(count));
                        insert_cs_u64(
                            row,
                            cs_trace::TREE_AUX_ROOTS_LEN,
                            u64::from(tree_aux_roots_len),
                        );
                        insert_cs_hash(row, cs_trace::HASH, anchor);
                    },
                );
                let started = Instant::now();
                match state
                    .clone()
                    .oneshot(zakura_state::Request::CommitHeaderRange {
                        anchor,
                        headers,
                        body_sizes,
                        tree_aux_roots,
                    })
                    .await
                {
                    Ok(zakura_state::Response::Committed(tip_hash)) => {
                        emit_commit_state(
                            &trace,
                            cs_trace::COMMIT_FINISH,
                            "header_sync_driver",
                            |row| {
                                insert_cs_str(row, cs_trace::ACTION, "commit_header_range");
                                insert_cs_peer(row, cs_trace::PEER, &peer);
                                insert_cs_height(row, cs_trace::RANGE_START, start_height);
                                insert_cs_u64(row, cs_trace::RANGE_COUNT, u64::from(count));
                                insert_cs_u64(
                                    row,
                                    cs_trace::TREE_AUX_ROOTS_LEN,
                                    u64::from(tree_aux_roots_len),
                                );
                                insert_cs_str(row, cs_trace::RESULT, "committed");
                                insert_cs_u64(row, cs_trace::ELAPSED_MS, elapsed_ms(started));
                            },
                        );
                        let tip_height =
                            block::Height(start_height.0.saturating_add(count.saturating_sub(1)));
                        let _ = handles
                            .header_sync
                            .send(header_range_committed(operation, tip_hash))
                            .await;
                        trace_header_reactor_event(
                            &trace,
                            "header_range_committed",
                            None,
                            tip_height,
                            tip_hash,
                            count,
                        );
                    }
                    Ok(response) => {
                        emit_commit_state(
                            &trace,
                            cs_trace::COMMIT_FINISH,
                            "header_sync_driver",
                            |row| {
                                insert_cs_str(row, cs_trace::ACTION, "commit_header_range");
                                insert_cs_peer(row, cs_trace::PEER, &peer);
                                insert_cs_height(row, cs_trace::RANGE_START, start_height);
                                insert_cs_u64(row, cs_trace::RANGE_COUNT, u64::from(count));
                                insert_cs_str(row, cs_trace::RESULT, "unexpected_response");
                                insert_cs_u64(row, cs_trace::ELAPSED_MS, elapsed_ms(started));
                            },
                        );
                        warn!(?peer, ?response, "unexpected CommitHeaderRange response");
                        trace_header_reactor_event(
                            &trace,
                            "header_range_commit_failed",
                            Some(&peer),
                            start_height,
                            block::Hash([0; 32]),
                            count,
                        );
                        let _ = handles
                            .header_sync
                            .send(header_range_commit_failed(
                                operation,
                                HeaderSyncCommitFailureKind::Local,
                            ))
                            .await;
                    }
                    Err(error) => {
                        let kind = header_range_commit_failure_kind(error.as_ref());
                        emit_commit_state(
                            &trace,
                            cs_trace::COMMIT_FINISH,
                            "header_sync_driver",
                            |row| {
                                insert_cs_str(row, cs_trace::ACTION, "commit_header_range");
                                insert_cs_peer(row, cs_trace::PEER, &peer);
                                insert_cs_height(row, cs_trace::RANGE_START, start_height);
                                insert_cs_u64(row, cs_trace::RANGE_COUNT, u64::from(count));
                                insert_cs_str(
                                    row,
                                    cs_trace::RESULT,
                                    commit_failure_result_label(kind),
                                );
                                insert_cs_hash(row, cs_trace::HASH, anchor);
                                insert_cs_str(
                                    row,
                                    cs_trace::ERROR_VARIANT,
                                    header_range_commit_error_label(error.as_ref()),
                                );
                                insert_cs_str(
                                    row,
                                    cs_trace::ERROR_DEBUG,
                                    &header_range_commit_error_debug(error.as_ref()),
                                );
                                insert_cs_u64(row, cs_trace::ELAPSED_MS, elapsed_ms(started));
                            },
                        );
                        debug!(
                            ?peer,
                            ?start_height,
                            ?count,
                            ?kind,
                            ?error,
                            "Zakura header range commit failed"
                        );
                        trace_header_reactor_event(
                            &trace,
                            "header_range_commit_failed",
                            Some(&peer),
                            start_height,
                            block::Hash([0; 32]),
                            count,
                        );
                        let _ = handles
                            .header_sync
                            .send(header_range_commit_failed(operation, kind))
                            .await;
                    }
                }
            }
            HeaderSyncAction::AuthenticateHeaderRoots {
                operation,
                expected_state,
                anchor,
                payload,
            } => {
                if root_auth_tasks.len() >= MAX_ROOT_AUTH_STATE_TASKS {
                    if handles
                        .header_sync
                        .send(header_root_authentication_failed(
                            operation,
                            HeaderRootAuthenticationFailureKind::Stale,
                        ))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    continue;
                }
                let peer = operation.wire_request.peer.clone();
                let start = payload.range().start();
                let (_range, headers, _body_sizes, roots) = payload.into_parts();
                let request = zakura_state::AuthenticateHeaderRootsRequest {
                    expected_state: state_header_root_auth_state(expected_state),
                    anchor,
                    start,
                    headers,
                    roots: roots.unwrap_or_default(),
                };
                let state = state.clone();
                debug_assert!(
                    in_flight_root_auth.is_none(),
                    "at most one root-auth task is admitted"
                );
                in_flight_root_auth = Some(operation.clone());
                root_auth_tasks.spawn(async move {
                    match tokio::time::timeout(
                        ROOT_AUTH_STATE_TIMEOUT,
                        state.oneshot(request.map_request()),
                    )
                    .await
                    {
                        Ok(Ok(zakura_state::Response::AuthenticatedHeaderRoots(_))) => {
                            header_root_authentication_completed(operation)
                        }
                        Ok(Ok(response)) => {
                            warn!(
                                ?peer,
                                ?response,
                                "unexpected AuthenticateHeaderRoots response"
                            );
                            header_root_authentication_failed(
                                operation,
                                HeaderRootAuthenticationFailureKind::Local,
                            )
                        }
                        Ok(Err(error)) => {
                            let kind = header_root_authentication_failure_kind(error.as_ref());
                            if kind == HeaderRootAuthenticationFailureKind::Local {
                                warn!(
                                    ?peer,
                                    ?start,
                                    ?error,
                                    "local header-root authentication failure"
                                );
                            } else {
                                debug!(
                                    ?peer,
                                    ?start,
                                    ?kind,
                                    ?error,
                                    "header-root authentication rejected"
                                );
                            }
                            header_root_authentication_failed(operation, kind)
                        }
                        Err(_) => {
                            warn!(
                                ?peer,
                                ?start,
                                "header-root authentication state request timed out"
                            );
                            header_root_authentication_failed(
                                operation,
                                HeaderRootAuthenticationFailureKind::Local,
                            )
                        }
                    }
                });
            }
            HeaderSyncAction::QueryBestHeaderTip => {
                emit_commit_state(
                    &trace,
                    cs_trace::STATE_READ_START,
                    "header_sync_driver",
                    |row| {
                        insert_cs_str(row, cs_trace::ACTION, "query_best_header_tip");
                    },
                );
                match read_state
                    .clone()
                    .oneshot(zakura_state::ReadRequest::BestHeaderTip)
                    .await
                {
                    Ok(zakura_state::ReadResponse::BestHeaderTip(Some((tip_height, tip_hash)))) => {
                        emit_commit_state(
                            &trace,
                            cs_trace::STATE_READ_SUCCESS,
                            "header_sync_driver",
                            |row| {
                                insert_cs_str(row, cs_trace::ACTION, "query_best_header_tip");
                                insert_cs_height(row, cs_trace::BEST_HEADER_TIP, tip_height);
                                insert_cs_hash(row, cs_trace::HASH, tip_hash);
                            },
                        );
                        let _ = handles
                            .header_sync
                            .send(HeaderSyncEvent::BestHeaderTipLoaded {
                                tip_height,
                                tip_hash,
                            })
                            .await;
                    }
                    Ok(zakura_state::ReadResponse::BestHeaderTip(None)) => {}
                    Ok(response) => {
                        trace_state_read_error(
                            &trace,
                            "query_best_header_tip",
                            None,
                            block::Height(0),
                            0,
                            "unexpected_response",
                            Instant::now(),
                        );
                        warn!(?response, "unexpected BestHeaderTip response")
                    }
                    Err(error) => {
                        trace_state_read_error(
                            &trace,
                            "query_best_header_tip",
                            None,
                            block::Height(0),
                            0,
                            &format!("{error}"),
                            Instant::now(),
                        );
                        warn!(?error, "failed to query Zakura best header tip")
                    }
                }
            }
            HeaderSyncAction::QueryMissingBlockBodies { from, limit } => {
                log_missing_block_bodies(read_state.clone(), from, limit, &trace).await;
            }
            HeaderSyncAction::BodyGaps { from, to } => {
                let limit =
                    to.0.saturating_sub(from.0)
                        .saturating_add(1)
                        .min(DEFAULT_HS_RANGE);
                log_missing_block_bodies(read_state.clone(), from, limit, &trace).await;
            }
            HeaderSyncAction::HeaderAdvanced { height, hash } => {
                publish_header_frontier(
                    &handles.endpoint,
                    height,
                    hash,
                    FrontierChange::HeaderAdvanced,
                    &trace,
                );
            }
            HeaderSyncAction::HeaderReanchored { old: _, new } => {
                publish_header_frontier(
                    &handles.endpoint,
                    new.0,
                    new.1,
                    FrontierChange::HeaderReanchored,
                    &trace,
                );
            }
        }
    }
}

pub(crate) fn publish_header_frontier(
    endpoint: &ZakuraEndpoint,
    height: block::Height,
    hash: block::Hash,
    change: FrontierChange,
    trace: &ZakuraTrace,
) {
    let Some(mut update) = endpoint.current_sync_frontier() else {
        return;
    };

    update.frontier.best_header = Frontier::new(height, hash);
    update.change = change;
    endpoint.publish_sync_frontier_from(update, "header_sync_driver");
    emit_commit_state(
        trace,
        cs_trace::BLOCK_SYNC_NOTIFY_SENT,
        "header_sync_driver",
        |row| {
            insert_cs_height(row, cs_trace::HEIGHT, height);
            insert_cs_hash(row, cs_trace::HASH, hash);
        },
    );
}

#[cfg(test)]
pub(crate) async fn notify_block_sync_header_tip(
    block_sync: Option<&BlockSyncHandle>,
    height: block::Height,
    hash: block::Hash,
    trace: &ZakuraTrace,
) {
    if let Some(block_sync) = block_sync {
        let _ = block_sync
            .send(BlockSyncEvent::HeaderTipChanged { height, hash })
            .await;
        emit_commit_state(
            trace,
            cs_trace::BLOCK_SYNC_NOTIFY_SENT,
            "header_sync_driver",
            |row| {
                insert_cs_height(row, cs_trace::HEIGHT, height);
                insert_cs_hash(row, cs_trace::HASH, hash);
            },
        );
    }
}

pub(crate) fn body_sizes_for_served_header_range(
    start: block::Height,
    header_heights: impl IntoIterator<Item = block::Height>,
    body_size_hints: &[(block::Height, Option<u32>)],
) -> Vec<u32> {
    header_heights
        .into_iter()
        .map(|height| {
            if height < start {
                return 0;
            }

            let Some(offset) = usize::try_from(height - start).ok() else {
                return 0;
            };

            body_size_hints
                .get(offset)
                .and_then(|(hint_height, size)| {
                    (*hint_height == height).then_some(size.unwrap_or(0))
                })
                .unwrap_or(0)
        })
        .collect()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TreeAuxRootsForServedHeaderRangeError {
    HeaderBeforeStart {
        start: block::Height,
        height: block::Height,
    },
    OffsetOutOfRange {
        start: block::Height,
        height: block::Height,
    },
    MissingRoot {
        height: block::Height,
        offset: usize,
    },
    RootHeightMismatch {
        expected_height: block::Height,
        actual_height: block::Height,
        offset: usize,
    },
}

pub(crate) fn tree_aux_roots_for_served_header_range(
    start: block::Height,
    header_heights: impl IntoIterator<Item = block::Height>,
    block_roots: &[BlockCommitmentRoots],
) -> Result<Vec<BlockCommitmentRoots>, TreeAuxRootsForServedHeaderRangeError> {
    let mut roots = Vec::new();

    for height in header_heights {
        if height < start {
            return Err(TreeAuxRootsForServedHeaderRangeError::HeaderBeforeStart { start, height });
        }

        let Some(offset) = usize::try_from(height - start).ok() else {
            return Err(TreeAuxRootsForServedHeaderRangeError::OffsetOutOfRange { start, height });
        };

        let Some(root) = block_roots.get(offset) else {
            return Err(TreeAuxRootsForServedHeaderRangeError::MissingRoot { height, offset });
        };

        if root.height != height {
            return Err(TreeAuxRootsForServedHeaderRangeError::RootHeightMismatch {
                expected_height: height,
                actual_height: root.height,
                offset,
            });
        }

        roots.push(root.clone());
    }

    Ok(roots)
}

async fn log_missing_block_bodies<ReadState>(
    read_state: ReadState,
    from: block::Height,
    limit: u32,
    trace: &ZakuraTrace,
) where
    ReadState: Service<
            zakura_state::ReadRequest,
            Response = zakura_state::ReadResponse,
            Error = zakura_state::BoxError,
        > + Send
        + 'static,
    ReadState::Future: Send + 'static,
{
    trace_state_read_start(trace, "missing_block_bodies", None, from, limit);
    let started = Instant::now();
    match read_state
        .oneshot(zakura_state::ReadRequest::MissingBlockBodies { from, limit })
        .await
    {
        Ok(zakura_state::ReadResponse::MissingBlockBodies(heights)) => {
            emit_commit_state(
                trace,
                cs_trace::STATE_READ_SUCCESS,
                "header_sync_driver",
                |row| {
                    insert_cs_str(row, cs_trace::ACTION, "missing_block_bodies");
                    insert_cs_height(row, cs_trace::RANGE_START, from);
                    insert_cs_u64(row, cs_trace::RANGE_COUNT, heights.len() as u64);
                    insert_cs_u64(row, cs_trace::ELAPSED_MS, elapsed_ms(started));
                },
            );
            let first = heights.first().copied();
            let last = heights.last().copied();
            let count = heights.len();
            debug!(
                ?from,
                ?limit,
                ?count,
                ?first,
                ?last,
                "Zakura header-known body gaps from state"
            );
        }
        Ok(response) => {
            trace_state_read_error(
                trace,
                "missing_block_bodies",
                None,
                from,
                limit,
                "unexpected_response",
                started,
            );
            warn!(?response, "unexpected MissingBlockBodies response")
        }
        Err(error) => {
            trace_state_read_error(
                trace,
                "missing_block_bodies",
                None,
                from,
                limit,
                &format!("{error}"),
                started,
            );
            warn!(?error, "failed to query Zakura missing block bodies")
        }
    }
}

/// Returns whether a just-committed `NewBlock` landed on the best chain.
///
/// `ReadRequest::Depth` returns `Some` only for best-chain blocks, so it
/// distinguishes a best-chain extension (or a reorg the block just won) from a
/// side-chain commit. Read failures are treated as *not* best-chain: the
/// node's own frontier still advances through the chain-tip mirror, so the
/// only cost of a false negative is skipping one gossip forward, while a
/// false positive would gossip a possibly losing branch.
async fn new_block_is_on_best_chain<ReadState>(read_state: ReadState, hash: block::Hash) -> bool
where
    ReadState: Service<
            zakura_state::ReadRequest,
            Response = zakura_state::ReadResponse,
            Error = zakura_state::BoxError,
        > + Send
        + 'static,
    ReadState::Future: Send + 'static,
{
    match read_state
        .oneshot(zakura_state::ReadRequest::Depth(hash))
        .await
    {
        Ok(zakura_state::ReadResponse::Depth(depth)) => depth.is_some(),
        Ok(response) => {
            warn!(?response, "unexpected Depth response for Zakura NewBlock");
            false
        }
        Err(error) => {
            warn!(
                ?hash,
                ?error,
                "failed to read Zakura NewBlock depth from state"
            );
            false
        }
    }
}

pub(crate) fn header_range_commit_failure_kind(
    error: &(dyn std::error::Error + Send + Sync + 'static),
) -> HeaderSyncCommitFailureKind {
    let Some(error) = error.downcast_ref::<zakura_state::CommitHeaderRangeError>() else {
        return HeaderSyncCommitFailureKind::Local;
    };

    match error {
        zakura_state::CommitHeaderRangeError::StorageWriteError { .. }
        | zakura_state::CommitHeaderRangeError::MissingGenesisAnchor { .. }
        | zakura_state::CommitHeaderRangeError::SendCommitRequestFailed
        // A lower-work conflicting range is individually valid (each header passed
        // PoW, difficulty, and contextual checks); the peer simply offered a worse
        // fork. Treat it as non-scoring so this stays a liveness/correctness guard,
        // not peer punishment.
        | zakura_state::CommitHeaderRangeError::LowerWorkConflict { .. }
        // The reactor already validates every peer response against the requested
        // anchor and for internal continuity (`validate_header_range_links`) and
        // scores linkage failures there, then commits with that same anchor. So the
        // store's own linkage check failing means the local anchor/response pairing
        // went wrong, not that the peer misbehaved.
        | zakura_state::CommitHeaderRangeError::UnlinkedRange { .. }
        // Store incoherence is by definition a local storage fault: the range was
        // rejected because our own header rows failed a linkage/bijection check
        // while reading validation context, not because the peer's range was shown
        // invalid. Scoring peers for it recreates the disconnect-honest-peers
        // failure mode.
        | zakura_state::CommitHeaderRangeError::StoreIncoherent(_)
        | zakura_state::CommitHeaderRangeError::CommitResponseDropped => {
            HeaderSyncCommitFailureKind::Local
        }
        zakura_state::CommitHeaderRangeError::EmptyRange
        | zakura_state::CommitHeaderRangeError::RangeTooLong { .. }
        | zakura_state::CommitHeaderRangeError::BodySizeCountMismatch { .. }
        | zakura_state::CommitHeaderRangeError::TreeAuxRootCountMismatch { .. }
        | zakura_state::CommitHeaderRangeError::TreeAuxRootHeightMismatch { .. }
        | zakura_state::CommitHeaderRangeError::UnknownAnchor { .. }
        | zakura_state::CommitHeaderRangeError::HeightOverflow
        | zakura_state::CommitHeaderRangeError::ImmutableConflict { .. }
        | zakura_state::CommitHeaderRangeError::ReorgTooDeep { .. }
        | zakura_state::CommitHeaderRangeError::CheckpointConflict { .. }
        | zakura_state::CommitHeaderRangeError::ConflictingFullBlockHeader { .. }
        | zakura_state::CommitHeaderRangeError::ValidateContextError(_) => {
            HeaderSyncCommitFailureKind::InvalidPeerRange
        }
        _ => HeaderSyncCommitFailureKind::Local,
    }
}

pub(crate) fn header_range_commit_error_label(
    error: &(dyn std::error::Error + Send + Sync + 'static),
) -> &'static str {
    let Some(error) = error.downcast_ref::<zakura_state::CommitHeaderRangeError>() else {
        return "non_commit_header_range_error";
    };

    match error {
        zakura_state::CommitHeaderRangeError::EmptyRange => "empty_range",
        zakura_state::CommitHeaderRangeError::RangeTooLong { .. } => "range_too_long",
        zakura_state::CommitHeaderRangeError::BodySizeCountMismatch { .. } => {
            "body_size_count_mismatch"
        }
        zakura_state::CommitHeaderRangeError::TreeAuxRootCountMismatch { .. } => {
            "tree_aux_root_count_mismatch"
        }
        zakura_state::CommitHeaderRangeError::TreeAuxRootHeightMismatch { .. } => {
            "tree_aux_root_height_mismatch"
        }
        zakura_state::CommitHeaderRangeError::UnknownAnchor { .. } => "unknown_anchor",
        zakura_state::CommitHeaderRangeError::MissingGenesisAnchor { .. } => {
            "missing_genesis_anchor"
        }
        zakura_state::CommitHeaderRangeError::HeightOverflow => "height_overflow",
        zakura_state::CommitHeaderRangeError::ImmutableConflict { .. } => "immutable_conflict",
        zakura_state::CommitHeaderRangeError::ReorgTooDeep { .. } => "reorg_too_deep",
        zakura_state::CommitHeaderRangeError::LowerWorkConflict { .. } => "lower_work_conflict",
        zakura_state::CommitHeaderRangeError::CheckpointConflict { .. } => "checkpoint_conflict",
        zakura_state::CommitHeaderRangeError::ConflictingFullBlockHeader { .. } => {
            "conflicting_full_block_header"
        }
        zakura_state::CommitHeaderRangeError::ValidateContextError(error) => {
            validate_context_error_label(error)
        }
        zakura_state::CommitHeaderRangeError::StorageWriteError { .. } => "storage_write_error",
        zakura_state::CommitHeaderRangeError::SendCommitRequestFailed => {
            "send_commit_request_failed"
        }
        zakura_state::CommitHeaderRangeError::CommitResponseDropped => "commit_response_dropped",
        _ => "unknown_commit_header_range_error",
    }
}

fn validate_context_error_label(error: &zakura_state::ValidateContextError) -> &'static str {
    match error {
        zakura_state::ValidateContextError::BlockPreviouslyInvalidated { .. } => {
            "validate_context_error.block_previously_invalidated"
        }
        zakura_state::ValidateContextError::VctSuppliedRootUnavailable { .. } => {
            "validate_context_error.vct_supplied_root_unavailable"
        }
        zakura_state::ValidateContextError::VctSuppliedRootAwaitingSuccessor { .. } => {
            "validate_context_error.vct_supplied_root_awaiting_successor"
        }
        zakura_state::ValidateContextError::OrphanedBlock { .. } => {
            "validate_context_error.orphaned_block"
        }
        zakura_state::ValidateContextError::NonSequentialBlock { .. } => {
            "validate_context_error.non_sequential_block"
        }
        zakura_state::ValidateContextError::TimeTooEarly { .. } => {
            "validate_context_error.time_too_early"
        }
        zakura_state::ValidateContextError::TimeTooLate { .. } => {
            "validate_context_error.time_too_late"
        }
        zakura_state::ValidateContextError::InvalidDifficultyThreshold { .. } => {
            "validate_context_error.invalid_difficulty_threshold"
        }
        _ => "validate_context_error.other",
    }
}

fn header_range_commit_error_debug(
    error: &(dyn std::error::Error + Send + Sync + 'static),
) -> String {
    error
        .downcast_ref::<zakura_state::CommitHeaderRangeError>()
        .map(|error| format!("{error:?}"))
        .unwrap_or_else(|| error.to_string())
}

pub(crate) async fn mirror_zakura_full_block_commits<ReadState>(
    mut chain_tip_change: zakura_state::ChainTipChange,
    latest_chain_tip: zakura_state::LatestChainTip,
    read_state: ReadState,
    header_sync: zakura_network::zakura::HeaderSyncHandle,
    endpoint: ZakuraEndpoint,
    trace: ZakuraTrace,
    shutdown: impl Future<Output = ()> + Send + 'static,
) where
    ReadState: Service<
            zakura_state::ReadRequest,
            Response = zakura_state::ReadResponse,
            Error = zakura_state::BoxError,
        > + Clone
        + Send
        + 'static,
    ReadState::Future: Send + 'static,
{
    pin!(shutdown);
    loop {
        let action = select! {
            _ = &mut shutdown => return,
            action = chain_tip_change.wait_for_tip_change() => {
                let Ok(action) = action else {
                    return;
                };
                action
            }
        };
        let height = action.best_tip_height();
        let hash = action.best_tip_hash();
        emit_commit_state(
            &trace,
            cs_trace::CHAIN_TIP_ACTION,
            "chain_tip_mirror",
            |row| {
                insert_cs_str(row, cs_trace::ACTION, tip_action_label(&action));
                insert_cs_height(row, cs_trace::HEIGHT, height);
                insert_cs_hash(row, cs_trace::HASH, hash);
            },
        );

        let finalized_tip = match read_state
            .clone()
            .oneshot(zakura_state::ReadRequest::FinalizedTip)
            .await
        {
            Ok(zakura_state::ReadResponse::FinalizedTip(tip)) => tip,
            Ok(response) => {
                warn!(?response, "unexpected FinalizedTip response");
                None
            }
            Err(error) => {
                warn!(?error, "failed to query Zakura finalized frontier");
                None
            }
        };
        let finalized_height = finalized_tip.map_or(block::Height(0), |(height, _)| height);
        emit_commit_state(
            &trace,
            cs_trace::STATE_READ_SUCCESS,
            "chain_tip_mirror",
            |row| {
                insert_cs_str(row, cs_trace::ACTION, "finalized_tip");
                insert_cs_height(row, cs_trace::FINALIZED_HEIGHT, finalized_height);
            },
        );
        let action_tip = Some((height, hash));
        let verified_block_tip =
            verified_block_tip_from_state(finalized_tip, action_tip, (height, hash));
        let verified_block_tip = verified_block_tip_from_state(
            Some(verified_block_tip),
            latest_chain_tip.best_tip_height_and_hash(),
            verified_block_tip,
        );

        emit_commit_state(
            &trace,
            cs_trace::FRONTIER_DERIVED,
            "chain_tip_mirror",
            |row| {
                insert_cs_str(row, cs_trace::ACTION, "sync_exchange_frontier_derived");
                insert_cs_height(row, cs_trace::FINALIZED_HEIGHT, finalized_height);
                insert_cs_height(row, cs_trace::VERIFIED_BLOCK_TIP, verified_block_tip.0);
                insert_cs_hash(row, cs_trace::VERIFIED_BLOCK_HASH, verified_block_tip.1);
            },
        );
        if let Some(mut update) = endpoint.current_sync_frontier() {
            let previous_verified_body = update.frontier.verified_body.height;
            if let Some((finalized_height, finalized_hash)) = finalized_tip {
                update.frontier.finalized = Frontier::new(finalized_height, finalized_hash);
            }
            update.frontier.verified_body =
                Frontier::new(verified_block_tip.0, verified_block_tip.1);
            update.change = chain_tip_mirror_frontier_change(
                &action,
                previous_verified_body,
                verified_block_tip.0,
            );
            endpoint.publish_sync_frontier_from(update, "chain_tip_mirror");
            emit_commit_state(
                &trace,
                cs_trace::FRONTIER_DERIVED,
                "chain_tip_mirror",
                |row| {
                    let frontiers = BlockSyncFrontiers {
                        finalized_height,
                        verified_block_tip: verified_block_tip.0,
                        verified_block_hash: verified_block_tip.1,
                    };
                    insert_cs_str(row, cs_trace::ACTION, "sync_exchange_frontier_sent");
                    insert_cs_frontiers(row, &frontiers);
                },
            );
        }

        emit_commit_state(
            &trace,
            cs_trace::STATE_READ_START,
            "chain_tip_mirror",
            |row| {
                insert_cs_str(row, cs_trace::ACTION, "committed_tip_block");
                insert_cs_height(row, cs_trace::HEIGHT, height);
                insert_cs_hash(row, cs_trace::HASH, hash);
            },
        );
        match read_state
            .clone()
            .oneshot(zakura_state::ReadRequest::Block(hash.into()))
            .await
        {
            Ok(zakura_state::ReadResponse::Block(Some(_))) => {
                emit_commit_state(
                    &trace,
                    cs_trace::STATE_READ_SUCCESS,
                    "chain_tip_mirror",
                    |row| {
                        insert_cs_str(row, cs_trace::ACTION, "committed_tip_block");
                        insert_cs_height(row, cs_trace::HEIGHT, height);
                        insert_cs_hash(row, cs_trace::HASH, hash);
                        insert_cs_str(row, cs_trace::RESULT, "found");
                    },
                );
                let _ = header_sync
                    .send(HeaderSyncEvent::FullBlockCommitted { height, hash })
                    .await;
                emit_commit_state(
                    &trace,
                    cs_trace::REACTOR_EVENT_SENT,
                    "chain_tip_mirror",
                    |row| {
                        insert_cs_str(row, cs_trace::ACTION, "full_block_committed");
                        insert_cs_height(row, cs_trace::HEIGHT, height);
                        insert_cs_hash(row, cs_trace::HASH, hash);
                    },
                );
            }
            Ok(zakura_state::ReadResponse::Block(None)) => {
                emit_commit_state(
                    &trace,
                    cs_trace::STATE_READ_SUCCESS,
                    "chain_tip_mirror",
                    |row| {
                        insert_cs_str(row, cs_trace::ACTION, "committed_tip_block");
                        insert_cs_height(row, cs_trace::HEIGHT, height);
                        insert_cs_hash(row, cs_trace::HASH, hash);
                        insert_cs_str(row, cs_trace::RESULT, "missing");
                    },
                );
                debug!(
                    ?height,
                    ?hash,
                    "Zakura full-block mirror could not find committed tip block"
                );
            }
            Ok(response) => {
                emit_commit_state(
                    &trace,
                    cs_trace::STATE_READ_ERROR,
                    "chain_tip_mirror",
                    |row| {
                        insert_cs_str(row, cs_trace::ACTION, "committed_tip_block");
                        insert_cs_str(row, cs_trace::REASON, "unexpected_response");
                    },
                );
                warn!(?response, "unexpected block lookup response")
            }
            Err(error) => {
                emit_commit_state(
                    &trace,
                    cs_trace::STATE_READ_ERROR,
                    "chain_tip_mirror",
                    |row| {
                        insert_cs_str(row, cs_trace::ACTION, "committed_tip_block");
                        insert_cs_str(row, cs_trace::REASON, &format!("{error}"));
                    },
                );
                warn!(?error, "failed to mirror Zakura full-block commit")
            }
        }
    }
}

#[cfg(test)]
pub(crate) fn block_sync_chain_tip_event(
    action: &zakura_state::TipAction,
    frontiers: BlockSyncFrontiers,
) -> BlockSyncEvent {
    match action {
        zakura_state::TipAction::Grow { .. } => BlockSyncEvent::ChainTipGrow(frontiers),
        zakura_state::TipAction::Reset { .. } => BlockSyncEvent::ChainTipReset(frontiers),
    }
}

pub(crate) fn chain_tip_mirror_frontier_change(
    action: &zakura_state::TipAction,
    previous_verified_body: block::Height,
    verified_block_tip: block::Height,
) -> FrontierChange {
    match action {
        zakura_state::TipAction::Grow { .. } => FrontierChange::VerifiedGrow,
        zakura_state::TipAction::Reset { .. } if verified_block_tip > previous_verified_body => {
            FrontierChange::VerifiedGrow
        }
        zakura_state::TipAction::Reset { .. } => FrontierChange::VerifiedReset,
    }
}

fn trace_header_driver_action(trace: &ZakuraTrace, action: &HeaderSyncAction) {
    emit_commit_state(
        trace,
        cs_trace::ACTION_RECEIVED,
        "header_sync_driver",
        |row| match action {
            HeaderSyncAction::CommitHeaderRange {
                operation, payload, ..
            } => {
                insert_cs_str(row, cs_trace::ACTION, "commit_header_range");
                insert_cs_peer(row, cs_trace::PEER, &operation.wire_request.peer);
                insert_cs_height(row, cs_trace::RANGE_START, payload.range().start());
                insert_cs_u64(
                    row,
                    cs_trace::RANGE_COUNT,
                    u64::from(payload.range().count()),
                );
            }
            HeaderSyncAction::AuthenticateHeaderRoots {
                operation, payload, ..
            } => {
                insert_cs_str(row, cs_trace::ACTION, "authenticate_header_roots");
                insert_cs_peer(row, cs_trace::PEER, &operation.wire_request.peer);
                insert_cs_height(row, cs_trace::RANGE_START, payload.range().start());
                insert_cs_u64(
                    row,
                    cs_trace::RANGE_COUNT,
                    u64::from(payload.range().count()),
                );
            }
            HeaderSyncAction::QueryBestHeaderTip => {
                insert_cs_str(row, cs_trace::ACTION, "query_best_header_tip");
            }
            HeaderSyncAction::QueryHeadersByHeightRange {
                peer, start, count, ..
            } => {
                insert_cs_str(row, cs_trace::ACTION, "query_headers_by_height_range");
                insert_cs_peer(row, cs_trace::PEER, peer);
                insert_cs_height(row, cs_trace::RANGE_START, *start);
                insert_cs_u64(row, cs_trace::RANGE_COUNT, u64::from(*count));
            }
            HeaderSyncAction::QueryMissingBlockBodies { from, limit } => {
                insert_cs_str(row, cs_trace::ACTION, "query_missing_block_bodies");
                insert_cs_height(row, cs_trace::RANGE_START, *from);
                insert_cs_u64(row, cs_trace::RANGE_COUNT, u64::from(*limit));
            }
            HeaderSyncAction::Misbehavior { peer, reason } => {
                insert_cs_str(row, cs_trace::ACTION, "misbehavior");
                insert_cs_peer(row, cs_trace::PEER, peer);
                insert_cs_str(row, cs_trace::REASON, header_misbehavior_label(*reason));
            }
            HeaderSyncAction::BodyGaps { from, to } => {
                insert_cs_str(row, cs_trace::ACTION, "body_gaps");
                insert_cs_height(row, cs_trace::RANGE_START, *from);
                insert_cs_u64(
                    row,
                    cs_trace::RANGE_COUNT,
                    u64::from(to.0.saturating_sub(from.0).saturating_add(1)),
                );
            }
            HeaderSyncAction::HeaderAdvanced { height, hash } => {
                insert_cs_str(row, cs_trace::ACTION, "header_advanced");
                insert_cs_height(row, cs_trace::HEIGHT, *height);
                insert_cs_hash(row, cs_trace::HASH, *hash);
            }
            HeaderSyncAction::HeaderReanchored { old, new } => {
                insert_cs_str(row, cs_trace::ACTION, "header_reanchored");
                insert_cs_height(row, cs_trace::BEST_HEADER_TIP, old.0);
                insert_cs_height(row, cs_trace::HEIGHT, new.0);
                insert_cs_hash(row, cs_trace::HASH, new.1);
            }
            HeaderSyncAction::NewBlockReceived {
                peer, height, hash, ..
            } => {
                insert_cs_str(row, cs_trace::ACTION, "new_block_received");
                insert_cs_peer(row, cs_trace::PEER, peer);
                insert_cs_height(row, cs_trace::HEIGHT, *height);
                insert_cs_hash(row, cs_trace::HASH, *hash);
            }
        },
    );
}

fn trace_header_commit_finish(
    trace: &ZakuraTrace,
    action: &'static str,
    peer: &zakura_network::zakura::ZakuraPeerId,
    height: block::Height,
    hash: block::Hash,
    result: &'static str,
    started: Instant,
) {
    emit_commit_state(
        trace,
        cs_trace::COMMIT_FINISH,
        "header_sync_driver",
        |row| {
            insert_cs_str(row, cs_trace::ACTION, action);
            insert_cs_peer(row, cs_trace::PEER, peer);
            insert_cs_height(row, cs_trace::HEIGHT, height);
            insert_cs_hash(row, cs_trace::HASH, hash);
            insert_cs_str(row, cs_trace::RESULT, result);
            insert_cs_u64(row, cs_trace::ELAPSED_MS, elapsed_ms(started));
        },
    );
}

fn trace_header_reactor_event(
    trace: &ZakuraTrace,
    action: &'static str,
    peer: Option<&zakura_network::zakura::ZakuraPeerId>,
    height: block::Height,
    hash: block::Hash,
    count: u32,
) {
    emit_commit_state(
        trace,
        cs_trace::REACTOR_EVENT_SENT,
        "header_sync_driver",
        |row| {
            insert_cs_str(row, cs_trace::ACTION, action);
            if let Some(peer) = peer {
                insert_cs_peer(row, cs_trace::PEER, peer);
            }
            insert_cs_height(row, cs_trace::HEIGHT, height);
            insert_cs_hash(row, cs_trace::HASH, hash);
            insert_cs_u64(row, cs_trace::RANGE_COUNT, u64::from(count));
        },
    );
}

fn trace_header_range_finished(
    trace: &ZakuraTrace,
    peer: &zakura_network::zakura::ZakuraPeerId,
    start: block::Height,
    requested_count: u32,
    returned_count: u32,
) {
    emit_commit_state(
        trace,
        cs_trace::REACTOR_EVENT_SENT,
        "header_sync_driver",
        |row| {
            insert_cs_str(row, cs_trace::ACTION, "header_range_response_finished");
            insert_cs_peer(row, cs_trace::PEER, peer);
            insert_cs_height(row, cs_trace::RANGE_START, start);
            insert_cs_u64(row, cs_trace::RANGE_COUNT, u64::from(returned_count));
            insert_cs_u64(row, "requested_count", u64::from(requested_count));
        },
    );
}

fn trace_state_read_start(
    trace: &ZakuraTrace,
    action: &'static str,
    peer: Option<&zakura_network::zakura::ZakuraPeerId>,
    start: block::Height,
    count: u32,
) {
    emit_commit_state(
        trace,
        cs_trace::STATE_READ_START,
        "header_sync_driver",
        |row| {
            insert_cs_str(row, cs_trace::ACTION, action);
            if let Some(peer) = peer {
                insert_cs_peer(row, cs_trace::PEER, peer);
            }
            insert_cs_height(row, cs_trace::RANGE_START, start);
            insert_cs_u64(row, cs_trace::RANGE_COUNT, u64::from(count));
        },
    );
}

fn trace_state_read_error(
    trace: &ZakuraTrace,
    action: &'static str,
    peer: Option<&zakura_network::zakura::ZakuraPeerId>,
    start: block::Height,
    count: u32,
    reason: &str,
    started: Instant,
) {
    emit_commit_state(
        trace,
        cs_trace::STATE_READ_ERROR,
        "header_sync_driver",
        |row| {
            insert_cs_str(row, cs_trace::ACTION, action);
            if let Some(peer) = peer {
                insert_cs_peer(row, cs_trace::PEER, peer);
            }
            insert_cs_height(row, cs_trace::RANGE_START, start);
            insert_cs_u64(row, cs_trace::RANGE_COUNT, u64::from(count));
            insert_cs_str(row, cs_trace::REASON, reason);
            insert_cs_u64(row, cs_trace::ELAPSED_MS, elapsed_ms(started));
        },
    );
}

fn commit_failure_result_label(kind: HeaderSyncCommitFailureKind) -> &'static str {
    match kind {
        HeaderSyncCommitFailureKind::InvalidPeerRange => "invalid_peer_range",
        HeaderSyncCommitFailureKind::Local => "local_error",
    }
}

fn header_misbehavior_label(reason: zakura_network::zakura::HeaderSyncMisbehavior) -> &'static str {
    match reason {
        zakura_network::zakura::HeaderSyncMisbehavior::InvalidStatus => "invalid_status",
        zakura_network::zakura::HeaderSyncMisbehavior::UnsolicitedHeaders => "unsolicited_headers",
        zakura_network::zakura::HeaderSyncMisbehavior::EmptyHeaders => "empty_headers",
        zakura_network::zakura::HeaderSyncMisbehavior::ResponseTooLong => "response_too_long",
        zakura_network::zakura::HeaderSyncMisbehavior::InvalidRange => "invalid_range",
        zakura_network::zakura::HeaderSyncMisbehavior::MalformedMessage => "malformed_message",
        zakura_network::zakura::HeaderSyncMisbehavior::StatusSpam => "status_spam",
        zakura_network::zakura::HeaderSyncMisbehavior::NewBlockSpam => "new_block_spam",
        zakura_network::zakura::HeaderSyncMisbehavior::GetHeadersSpam => "get_headers_spam",
        zakura_network::zakura::HeaderSyncMisbehavior::GetHeadersTooLong => "get_headers_too_long",
        zakura_network::zakura::HeaderSyncMisbehavior::UnknownPeer => "unknown_peer",
        zakura_network::zakura::HeaderSyncMisbehavior::InvalidNewBlock => "invalid_new_block",
    }
}

fn tip_action_label(action: &zakura_state::TipAction) -> &'static str {
    match action {
        zakura_state::TipAction::Grow { .. } => "grow",
        zakura_state::TipAction::Reset { .. } => "reset",
    }
}

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}
