use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    sync::Arc,
    time::Instant,
};

use futures::{
    future::BoxFuture,
    stream::{FuturesUnordered, StreamExt},
    FutureExt,
};
use sha2::{Digest, Sha256};
use tokio::{pin, select, sync::mpsc};
use tower::{util::BoxCloneService, Service, ServiceExt};
use tracing::{debug, error, warn};

use zakura_chain::{block, chain_tip::ChainTip};
use zakura_network::zakura::{
    BlockApplyOutcome, BlockApplyResult, BlockApplyToken, BlockSizeEstimate, BlockSyncAction,
    BlockSyncBlockMeta, BlockSyncEvent, BlockSyncHandle, ZakuraEndpoint, ZakuraTrace,
};

use crate::components::sync;

use super::{
    block_verify_error_class, block_verify_error_diagnostic,
    trace::block_driver::BlockDriverTraceExt, BlocksyncThroughputProbe,
    ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT,
};

#[cfg(test)]
pub(crate) const ZAKURA_BLOCK_SYNC_MISSING_BODY_WINDOW: u32 =
    zakura_state::constants::MAX_HEADER_SYNC_HEIGHT_RANGE;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum BlockApplyClass {
    Checkpoint,
    Full,
}

#[derive(Debug)]
struct PendingBlockApply {
    owner: zakura_header_chain::BodyWorkOwner,
    source: zakura_header_chain::SourceId,
    token: BlockApplyToken,
    class: BlockApplyClass,
    block: Arc<block::Block>,
    operation: Option<super::BlockApplyOperation>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct BlockApplyCompletion {
    class: BlockApplyClass,
    result: BlockApplyResult,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn drive_block_sync_actions<ReadState, BlockVerifier>(
    mut actions: mpsc::Receiver<BlockSyncAction>,
    // Retained so the disconnect capability stays wired into the driver, even
    // though peer scoring no longer drives disconnects (misbehavior is record-only).
    _supervisor: zakura_network::zakura::ZakuraSupervisorHandle,
    endpoint: Option<ZakuraEndpoint>,
    block_sync: BlockSyncHandle,
    latest_chain_tip: impl ChainTip + Clone + Send + Sync + 'static,
    read_state: ReadState,
    header_chain_write: Option<
        BoxCloneService<zakura_state::Request, zakura_state::Response, zakura_state::BoxError>,
    >,
    body_evidence_authority: Option<zakura_state::HeaderChainBodyEvidenceAuthority>,
    block_verifier: BlockVerifier,
    max_checkpoint_height: block::Height,
    checkpoint_apply_limit: usize,
    full_apply_limit: usize,
    combined_apply_limit: usize,
    trace: ZakuraTrace,
    throughput_probe: Option<BlocksyncThroughputProbe>,
    block_sync_handoff: std::sync::Arc<super::SyncCoordinator>,
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
    BlockVerifier:
        Service<zakura_consensus::Request, Response = block::Hash> + Clone + Send + 'static,
    BlockVerifier::Error: std::fmt::Debug + Send + Sync + 'static,
    BlockVerifier::Future: Send + 'static,
{
    pin!(shutdown);
    const {
        assert!(
            sync::MIN_CHECKPOINT_CONCURRENCY_LIMIT <= zakura_consensus::MAX_CHECKPOINT_HEIGHT_GAP
        );
    }
    let checkpoint_apply_limit = checkpoint_apply_limit.clamp(
        sync::MIN_CHECKPOINT_CONCURRENCY_LIMIT,
        zakura_consensus::MAX_CHECKPOINT_HEIGHT_GAP,
    );
    let full_apply_limit = full_apply_limit.max(sync::MIN_CONCURRENCY_LIMIT);
    let combined_apply_limit = combined_apply_limit.max(sync::MIN_CONCURRENCY_LIMIT);
    let mut pending_applies = VecDeque::new();
    let mut pending_probe_applies = BTreeMap::new();
    let mut in_flight_applies: FuturesUnordered<BoxFuture<'static, BlockApplyCompletion>> =
        FuturesUnordered::new();
    let mut checkpoint_in_flight = 0usize;
    let mut full_in_flight = 0usize;
    let mut deferred_actions = VecDeque::new();
    let mut shutting_down = false;
    let mut apply_phase = block_sync_handoff.subscribe_apply_phase();

    loop {
        if block_sync_handoff.is_yielded_to_legacy() {
            release_pending_applies(&block_sync, &mut pending_applies, &trace);
            release_pending_probe_applies(&block_sync, &mut pending_probe_applies, &trace);
        } else if block_sync_handoff.zakura_owns_applies() && !pending_applies.is_empty() {
            drain_pending_block_applies(
                &block_sync_handoff,
                &mut pending_applies,
                &mut in_flight_applies,
                &mut checkpoint_in_flight,
                &mut full_in_flight,
                checkpoint_apply_limit,
                full_apply_limit,
                combined_apply_limit,
                latest_chain_tip.clone(),
                endpoint.clone(),
                read_state.clone(),
                block_verifier.clone(),
                block_sync.clone(),
                trace.clone(),
                throughput_probe.clone(),
            );
        }

        if !shutting_down && shutdown.as_mut().now_or_never().is_some() {
            shutting_down = true;
            block_sync_handoff.request_apply_shutdown();
            release_pending_applies(&block_sync, &mut pending_applies, &trace);
            release_pending_probe_applies(&block_sync, &mut pending_probe_applies, &trace);
            deferred_actions.clear();
        }

        if shutting_down {
            if let Some(completed) = in_flight_applies.next().await {
                handle_completed_block_apply(
                    &block_sync_handoff,
                    completed,
                    &mut pending_applies,
                    &mut in_flight_applies,
                    &mut checkpoint_in_flight,
                    &mut full_in_flight,
                    checkpoint_apply_limit,
                    full_apply_limit,
                    combined_apply_limit,
                    latest_chain_tip.clone(),
                    endpoint.clone(),
                    read_state.clone(),
                    block_verifier.clone(),
                    block_sync.clone(),
                    trace.clone(),
                    throughput_probe.clone(),
                );
                continue;
            }

            return;
        }

        if !in_flight_applies.is_empty() {
            if let Some(Some(completed)) = in_flight_applies.next().now_or_never() {
                handle_completed_block_apply(
                    &block_sync_handoff,
                    completed,
                    &mut pending_applies,
                    &mut in_flight_applies,
                    &mut checkpoint_in_flight,
                    &mut full_in_flight,
                    checkpoint_apply_limit,
                    full_apply_limit,
                    combined_apply_limit,
                    latest_chain_tip.clone(),
                    endpoint.clone(),
                    read_state.clone(),
                    block_verifier.clone(),
                    block_sync.clone(),
                    trace.clone(),
                    throughput_probe.clone(),
                );
                continue;
            }
        }

        let action = if let Some(action) =
            coalesce_ready_needed_block_queries(&mut actions, &mut deferred_actions)
        {
            action
        } else if let Some(action) = deferred_actions.pop_front() {
            action
        } else {
            select! {
                _ = &mut shutdown => {
                    shutting_down = true;
                    block_sync_handoff.request_apply_shutdown();
                    release_pending_applies(&block_sync, &mut pending_applies, &trace);
                    release_pending_probe_applies(&block_sync, &mut pending_probe_applies, &trace);
                    deferred_actions.clear();
                    continue;
                },
                _ = block_sync_handoff.wait_for_zakura_ownership(),
                    if !block_sync_handoff.zakura_owns_applies()
                        && !block_sync_handoff.is_yielded_to_legacy() =>
                {
                    continue;
                }
                changed = apply_phase.changed() => {
                    if changed.is_err() {
                        shutting_down = true;
                        block_sync_handoff.request_apply_shutdown();
                    }
                    continue;
                }
                completed = in_flight_applies.next(), if !in_flight_applies.is_empty() => {
                    let Some(completed) = completed else {
                        continue;
                    };
                    handle_completed_block_apply(
                        &block_sync_handoff,
                        completed,
                        &mut pending_applies,
                        &mut in_flight_applies,
                        &mut checkpoint_in_flight,
                        &mut full_in_flight,
                        checkpoint_apply_limit,
                        full_apply_limit,
                        combined_apply_limit,
                        latest_chain_tip.clone(),
                        endpoint.clone(),
                        read_state.clone(),
                        block_verifier.clone(),
                        block_sync.clone(),
                        trace.clone(),
                        throughput_probe.clone(),
                    );
                    continue;
                }
                action = actions.recv() => {
                    let Some(action) = action else {
                        shutting_down = true;
                        block_sync_handoff.request_apply_shutdown();
                        release_pending_applies(&block_sync, &mut pending_applies, &trace);
                        release_pending_probe_applies(
                            &block_sync,
                            &mut pending_probe_applies,
                            &trace,
                        );
                        continue;
                    };
                    action
                }
            }
        };
        let action =
            coalesce_stale_needed_block_queries(action, &mut actions, &mut deferred_actions);

        trace.trace_block_action_received(&action);
        match action {
            BlockSyncAction::RecordBodyUnavailable {
                expected_version,
                failure,
            } => {
                let Some(writer) = header_chain_write.as_ref() else {
                    debug!(
                        ?failure,
                        "header-chain body retry persistence is not wired in this harness"
                    );
                    continue;
                };
                let Some(authority) = body_evidence_authority.as_ref() else {
                    debug!(
                        ?failure,
                        "header-chain body evidence authority is not wired"
                    );
                    continue;
                };
                match tokio::time::timeout(
                    ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT,
                    writer.clone().oneshot(
                        zakura_state::Request::RecordHeaderChainBodyUnavailable {
                            prepared: authority.from_registered_attempt(expected_version, failure),
                        },
                    ),
                )
                .await
                {
                    Ok(Ok(zakura_state::Response::HeaderChainBodyUnavailableRecorded(_))) => {}
                    Ok(Ok(response)) => warn!(
                        ?response,
                        "unexpected header-chain body retry persistence response"
                    ),
                    Ok(Err(error)) => debug!(
                        ?error,
                        "header-chain body retry persistence was stale or unavailable"
                    ),
                    Err(_) => warn!("timed out persisting header-chain body retry evidence"),
                }
            }
            BlockSyncAction::RecordBodyInvalid {
                expected_version,
                invalid,
            } => {
                let Some(writer) = header_chain_write.as_ref() else {
                    debug!(
                        ?invalid,
                        "header-chain invalid-body persistence is not wired in this harness"
                    );
                    continue;
                };
                let Some(authority) = body_evidence_authority.as_ref() else {
                    debug!(
                        ?invalid,
                        "header-chain body evidence authority is not wired"
                    );
                    continue;
                };
                match persist_consensus_body_invalid(
                    writer.clone(),
                    authority,
                    expected_version,
                    invalid,
                )
                .await
                {
                    BodyInvalidPersistOutcome::Recorded => {}
                    BodyInvalidPersistOutcome::FailedClosed { reason, invalid } => {
                        error!(
                            ?invalid,
                            reason, "failing closed after losing consensus-invalid body evidence"
                        );
                        metrics::counter!(
                            "sync.block.body_invalid.persist.fail_closed",
                            "reason" => reason
                        )
                        .increment(1);
                        shutting_down = true;
                        block_sync_handoff.request_apply_shutdown();
                        release_pending_applies(&block_sync, &mut pending_applies, &trace);
                        release_pending_probe_applies(
                            &block_sync,
                            &mut pending_probe_applies,
                            &trace,
                        );
                        deferred_actions.clear();
                    }
                }
            }
            BlockSyncAction::RestartBodyAvailability {
                expected_version,
                discovery,
            } => {
                let Some(writer) = header_chain_write.as_ref() else {
                    debug!(
                        ?discovery,
                        "header-chain body retry restart is not wired in this harness"
                    );
                    continue;
                };
                let Some(authority) = body_evidence_authority.as_ref() else {
                    debug!(
                        ?discovery,
                        "header-chain body evidence authority is not wired"
                    );
                    continue;
                };
                match tokio::time::timeout(
                    ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT,
                    writer.clone().oneshot(
                        zakura_state::Request::RestartHeaderChainBodyAvailability {
                            prepared: authority
                                .from_registered_supplier(expected_version, discovery),
                        },
                    ),
                )
                .await
                {
                    Ok(Ok(zakura_state::Response::HeaderChainBodyAvailabilityRestarted(_))) => {}
                    Ok(Ok(response)) => warn!(
                        ?response,
                        "unexpected header-chain body retry restart response"
                    ),
                    Ok(Err(error)) => debug!(
                        ?error,
                        "header-chain body retry restart was stale or unavailable"
                    ),
                    Err(_) => warn!("timed out restarting header-chain body availability"),
                }
            }
            BlockSyncAction::RetryBodyAvailability {
                expected_version,
                retry,
            } => {
                let Some(writer) = header_chain_write.as_ref() else {
                    debug!(
                        ?retry,
                        "header-chain operator body retry is not wired in this harness"
                    );
                    continue;
                };
                let Some(authority) = body_evidence_authority.as_ref() else {
                    debug!(
                        ?retry,
                        "header-chain retry authority is not wired in this harness"
                    );
                    continue;
                };
                let prepared = authority.from_registered_retry(expected_version, retry);
                match tokio::time::timeout(
                    ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT,
                    writer.clone().oneshot(
                        zakura_state::Request::RetryHeaderChainBodyAvailability { prepared },
                    ),
                )
                .await
                {
                    Ok(Ok(zakura_state::Response::HeaderChainBodyAvailabilityRetried(_))) => {}
                    Ok(Ok(response)) => warn!(
                        ?response,
                        "unexpected header-chain operator body retry response"
                    ),
                    Ok(Err(error)) => debug!(
                        ?error,
                        "header-chain operator body retry was stale or unavailable"
                    ),
                    Err(_) => warn!("timed out retrying header-chain body availability"),
                }
            }
            BlockSyncAction::Misbehavior { peer, reason } => {
                // Record-only: peer scoring no longer drives disconnects.
                debug!(?peer, ?reason, "recorded Zakura block-sync peer violation");
            }
            BlockSyncAction::QueryNeededBlocks {
                query_id,
                from,
                limit,
                best_header_tip,
                scope,
            } => {
                trace.trace_needed_blocks_query_started(from, limit, best_header_tip);
                let started = Instant::now();
                match query_block_sync_needed_blocks(read_state.clone(), from, limit).await {
                    Ok((body_anchor, blocks)) => {
                        trace.trace_needed_blocks_query_succeeded(blocks.len(), started);
                        if block_sync
                            .send_control(BlockSyncEvent::ScopedNeededBlocks {
                                query_id,
                                scope,
                                body_anchor,
                                blocks,
                            })
                            .is_err()
                        {
                            error!("block-sync reactor closed before needed-body query completion");
                            return;
                        }
                        trace.trace_block_reactor_event("needed_blocks");
                    }
                    Err(error) => {
                        trace.trace_needed_blocks_query_failed(&format!("{error}"), started);
                        if block_sync
                            .send_needed_blocks_query_failure(query_id, scope)
                            .is_err()
                        {
                            error!("block-sync reactor closed before needed-body query failure");
                            return;
                        }
                        trace.trace_block_reactor_event("needed_blocks_query_failed");
                        warn!(
                            ?from,
                            ?limit,
                            ?best_header_tip,
                            ?error,
                            "failed to query Zakura block-sync needed blocks"
                        );
                    }
                }
            }
            BlockSyncAction::QueryBlocksByHeightRange { peer, start, count } => {
                trace.trace_block_range_query_started(&peer, start, count);
                let started = Instant::now();
                match tokio::time::timeout(
                    ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT,
                    read_state
                        .clone()
                        .oneshot(zakura_state::ReadRequest::BlocksByHeightRange { start, count }),
                )
                .await
                {
                    Ok(Ok(zakura_state::ReadResponse::Blocks(blocks))) => {
                        trace.trace_block_range_query_succeeded(
                            &peer,
                            start,
                            blocks.len(),
                            started,
                        );
                        trace.trace_block_range_event(
                            "block_range_response_ready",
                            &peer,
                            start,
                            count,
                        );
                        let _ = block_sync.send_control(BlockSyncEvent::BlockRangeResponseReady {
                            peer,
                            start_height: start,
                            requested_count: count,
                            blocks,
                        });
                    }
                    Ok(Ok(response)) => {
                        trace.trace_block_range_query_failed(
                            &peer,
                            start,
                            count,
                            "unexpected_response",
                            started,
                        );
                        warn!(?peer, ?response, "unexpected BlocksByHeightRange response");
                        trace.trace_block_range_finished(&peer, start, count, 0);
                        let _ =
                            block_sync.send_control(BlockSyncEvent::BlockRangeResponseFinished {
                                peer,
                                start_height: start,
                                requested_count: count,
                                returned_count: 0,
                            });
                    }
                    Ok(Err(error)) => {
                        trace.trace_block_range_query_failed(
                            &peer,
                            start,
                            count,
                            &format!("{error}"),
                            started,
                        );
                        warn!(
                            ?peer,
                            ?error,
                            "failed to read Zakura Blocks response from state"
                        );
                        trace.trace_block_range_finished(&peer, start, count, 0);
                        let _ =
                            block_sync.send_control(BlockSyncEvent::BlockRangeResponseFinished {
                                peer,
                                start_height: start,
                                requested_count: count,
                                returned_count: 0,
                            });
                    }
                    Err(_elapsed) => {
                        trace.trace_block_range_query_timed_out(&peer, start, count, started);
                        warn!(?peer, "timed out reading Zakura block-sync serving range");
                        trace.trace_block_range_finished(&peer, start, count, 0);
                        let _ =
                            block_sync.send_control(BlockSyncEvent::BlockRangeResponseFinished {
                                peer,
                                start_height: start,
                                requested_count: count,
                                returned_count: 0,
                            });
                    }
                }
            }
            BlockSyncAction::SubmitBlock {
                owner,
                source,
                token,
                block,
            } => {
                let class = block_apply_class(block.as_ref(), max_checkpoint_height);
                let height = block.coinbase_height();
                if block_sync_handoff.is_yielded_to_legacy() {
                    abandon_block_apply(&block_sync, owner, source, token, block.as_ref(), &trace);
                    continue;
                }
                trace.trace_block_submit_queued(
                    token,
                    class,
                    block.as_ref(),
                    if throughput_probe.is_some() {
                        pending_probe_applies.len()
                    } else {
                        pending_applies.len()
                    },
                    checkpoint_in_flight.saturating_add(full_in_flight),
                );
                if let Some(probe) = throughput_probe.clone() {
                    let pending = PendingBlockApply {
                        owner,
                        source,
                        token,
                        class,
                        block,
                        operation: None,
                    };
                    if let Some(height) = height {
                        pending_probe_applies.insert(height, pending);
                        drain_ordered_probe_applies(
                            &mut pending_probe_applies,
                            latest_chain_tip.clone(),
                            endpoint.clone(),
                            read_state.clone(),
                            block_verifier.clone(),
                            block_sync.clone(),
                            trace.clone(),
                            probe,
                        )
                        .await;
                    } else {
                        let _completed = apply_probe_block_sync_body(
                            latest_chain_tip.clone(),
                            endpoint.clone(),
                            read_state.clone(),
                            block_verifier.clone(),
                            block_sync.clone(),
                            trace.clone(),
                            probe,
                            pending,
                        )
                        .await;
                    }
                    continue;
                }
                let Some(operation) = block_sync_handoff.queue_apply() else {
                    abandon_block_apply(&block_sync, owner, source, token, block.as_ref(), &trace);
                    continue;
                };
                debug!(operation_id = ?operation.id(), token, "queued native block apply operation");
                pending_applies.push_back(PendingBlockApply {
                    owner,
                    source,
                    token,
                    class,
                    block,
                    operation: Some(operation),
                });
                drain_pending_block_applies(
                    &block_sync_handoff,
                    &mut pending_applies,
                    &mut in_flight_applies,
                    &mut checkpoint_in_flight,
                    &mut full_in_flight,
                    checkpoint_apply_limit,
                    full_apply_limit,
                    combined_apply_limit,
                    latest_chain_tip.clone(),
                    endpoint.clone(),
                    read_state.clone(),
                    block_verifier.clone(),
                    block_sync.clone(),
                    trace.clone(),
                    throughput_probe.clone(),
                );
            }
        }
    }
}

fn abandon_block_apply(
    block_sync: &BlockSyncHandle,
    owner: zakura_header_chain::BodyWorkOwner,
    source: zakura_header_chain::SourceId,
    token: BlockApplyToken,
    block: &block::Block,
    trace: &ZakuraTrace,
) -> BlockApplyResult {
    let Some((height, expected_hash, result, event)) =
        abandoned_block_apply_finished_event(owner, source, token, block)
    else {
        warn!(
            expected_hash = ?block.hash(),
            "dropping abandoned Zakura block-sync body without coinbase height"
        );
        return BlockApplyResult::Rejected;
    };

    let _ = block_sync.send_control(event);
    trace.trace_block_apply_finished(token, height, expected_hash, result, false);
    result
}

pub(crate) fn abandoned_block_apply_finished_event(
    owner: zakura_header_chain::BodyWorkOwner,
    source: zakura_header_chain::SourceId,
    token: BlockApplyToken,
    block: &block::Block,
) -> Option<(block::Height, block::Hash, BlockApplyResult, BlockSyncEvent)> {
    let height = block.coinbase_height()?;
    let hash = block.hash();
    let outcome = retryable_body_outcome(
        owner,
        source,
        hash,
        zakura_header_chain::TransientBodyFailureKind::Canceled,
    );
    let result = outcome.result();

    Some((
        height,
        hash,
        result,
        BlockSyncEvent::BlockApplyFinished {
            owner,
            source,
            token,
            height,
            hash,
            outcome,
        },
    ))
}

fn abandoned_pending_apply_finished_events(
    pending_applies: &mut VecDeque<PendingBlockApply>,
) -> Vec<(block::Height, block::Hash, BlockApplyResult, BlockSyncEvent)> {
    let mut events = Vec::new();
    while let Some(mut pending) = pending_applies.pop_front() {
        if let Some(operation) = pending.operation.take() {
            operation.cancel();
        }
        if let Some(event) = abandoned_block_apply_finished_event(
            pending.owner,
            pending.source,
            pending.token,
            pending.block.as_ref(),
        ) {
            events.push(event);
        } else {
            warn!(
                expected_hash = ?pending.block.hash(),
                "dropping abandoned Zakura block-sync body without coinbase height"
            );
        }
    }
    events
}

/// Bound on refreshing a stale expected version while persisting consensus-invalid body evidence.
const BODY_INVALID_STALE_REFRESH_LIMIT: u8 = 3;

#[derive(Debug)]
enum BodyInvalidPersistOutcome {
    Recorded,
    FailedClosed {
        reason: &'static str,
        invalid: zakura_header_chain::ConsensusBodyInvalid,
    },
}

/// Persist deterministic body invalidity, refreshing a stale version a bounded number of times.
///
/// Any non-durable outcome fails closed: silent discard would leave a body-invalid branch selected.
async fn persist_consensus_body_invalid(
    writer: BoxCloneService<zakura_state::Request, zakura_state::Response, zakura_state::BoxError>,
    authority: &zakura_state::HeaderChainBodyEvidenceAuthority,
    mut expected_version: zakura_header_chain::StateVersion,
    invalid: zakura_header_chain::ConsensusBodyInvalid,
) -> BodyInvalidPersistOutcome {
    let mut stale_refreshes = 0u8;
    loop {
        match tokio::time::timeout(
            ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT,
            writer
                .clone()
                .oneshot(zakura_state::Request::RecordHeaderChainBodyInvalid {
                    prepared: authority.from_full_verifier(expected_version, invalid.clone()),
                }),
        )
        .await
        {
            Ok(Ok(zakura_state::Response::HeaderChainBodyInvalidRecorded(
                zakura_header_chain::ApplyResult::Committed
                | zakura_header_chain::ApplyResult::NoChange(_),
            ))) => return BodyInvalidPersistOutcome::Recorded,
            Ok(Ok(zakura_state::Response::HeaderChainBodyInvalidRecorded(
                zakura_header_chain::ApplyResult::Stale(receipt),
            ))) => {
                if stale_refreshes >= BODY_INVALID_STALE_REFRESH_LIMIT {
                    error!(
                        ?invalid,
                        ?receipt,
                        refreshes = stale_refreshes,
                        "exhausted stale refreshes while persisting consensus-invalid body evidence"
                    );
                    return BodyInvalidPersistOutcome::FailedClosed {
                        reason: "stale_refresh_exhausted",
                        invalid,
                    };
                }
                stale_refreshes = stale_refreshes.saturating_add(1);
                warn!(
                    ?invalid,
                    previous_version = ?expected_version,
                    current_version = ?receipt.current_version,
                    refreshes = stale_refreshes,
                    "refreshing expected version for consensus-invalid body evidence"
                );
                expected_version = receipt.current_version;
            }
            Ok(Ok(zakura_state::Response::HeaderChainBodyInvalidRecorded(
                zakura_header_chain::ApplyResult::ResourceStalled(receipt),
            ))) => {
                error!(
                    ?invalid,
                    ?receipt,
                    "resource-stalled while persisting consensus-invalid body evidence"
                );
                return BodyInvalidPersistOutcome::FailedClosed {
                    reason: "resource_stalled",
                    invalid,
                };
            }
            Ok(Ok(response)) => {
                error!(
                    ?invalid,
                    ?response,
                    "unexpected response while persisting consensus-invalid body evidence"
                );
                return BodyInvalidPersistOutcome::FailedClosed {
                    reason: "unexpected_response",
                    invalid,
                };
            }
            Ok(Err(error)) => {
                error!(
                    ?invalid,
                    ?error,
                    "state error while persisting consensus-invalid body evidence"
                );
                return BodyInvalidPersistOutcome::FailedClosed {
                    reason: "state_error",
                    invalid,
                };
            }
            Err(_) => {
                error!(
                    ?invalid,
                    "timed out persisting consensus-invalid body evidence"
                );
                return BodyInvalidPersistOutcome::FailedClosed {
                    reason: "timeout",
                    invalid,
                };
            }
        }
    }
}

pub(crate) fn coalesce_ready_needed_block_queries(
    actions: &mut mpsc::Receiver<BlockSyncAction>,
    deferred_actions: &mut VecDeque<BlockSyncAction>,
) -> Option<BlockSyncAction> {
    let mut latest_query = None;
    let mut retained = VecDeque::new();
    while let Some(action) = deferred_actions.pop_front() {
        match action {
            BlockSyncAction::QueryNeededBlocks {
                query_id,
                from,
                limit,
                best_header_tip,
                scope,
            } => {
                latest_query = Some((query_id, from, limit, best_header_tip, scope));
            }
            action => retained.push_back(action),
        }
    }
    *deferred_actions = retained;

    if !deferred_actions.is_empty() {
        if let Some((query_id, from, limit, best_header_tip, scope)) = latest_query {
            deferred_actions.push_back(BlockSyncAction::QueryNeededBlocks {
                query_id,
                from,
                limit,
                best_header_tip,
                scope,
            });
        }
        return None;
    }

    while let Ok(action) = actions.try_recv() {
        match action {
            BlockSyncAction::QueryNeededBlocks {
                query_id,
                from,
                limit,
                best_header_tip,
                scope,
            } => {
                latest_query = Some((query_id, from, limit, best_header_tip, scope));
            }
            action => deferred_actions.push_back(action),
        }
    }

    let latest_query = latest_query.map(|(query_id, from, limit, best_header_tip, scope)| {
        BlockSyncAction::QueryNeededBlocks {
            query_id,
            from,
            limit,
            best_header_tip,
            scope,
        }
    });

    if !deferred_actions.is_empty() {
        if let Some(query) = latest_query {
            deferred_actions.push_back(query);
        }
        return None;
    }

    latest_query
}

pub(crate) fn coalesce_stale_needed_block_queries(
    action: BlockSyncAction,
    actions: &mut mpsc::Receiver<BlockSyncAction>,
    deferred_actions: &mut VecDeque<BlockSyncAction>,
) -> BlockSyncAction {
    let BlockSyncAction::QueryNeededBlocks {
        mut query_id,
        mut from,
        mut limit,
        mut best_header_tip,
        mut scope,
    } = action
    else {
        return action;
    };

    let mut coalesced_count = 0u64;
    while let Ok(action) = actions.try_recv() {
        match action {
            BlockSyncAction::QueryNeededBlocks {
                query_id: latest_query_id,
                from: latest_from,
                limit: latest_limit,
                best_header_tip: latest_best_header_tip,
                scope: latest_scope,
            } => {
                query_id = latest_query_id;
                from = latest_from;
                limit = latest_limit;
                best_header_tip = latest_best_header_tip;
                scope = latest_scope;
                coalesced_count = coalesced_count.saturating_add(1);
            }
            action => deferred_actions.push_back(action),
        }
    }

    if coalesced_count > 0 {
        metrics::counter!("sync.block.needed_query.coalesced").increment(coalesced_count);
    }

    BlockSyncAction::QueryNeededBlocks {
        query_id,
        from,
        limit,
        best_header_tip,
        scope,
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_completed_block_apply<ReadState, BlockVerifier>(
    handoff: &std::sync::Arc<super::SyncCoordinator>,
    completed: BlockApplyCompletion,
    pending_applies: &mut VecDeque<PendingBlockApply>,
    in_flight_applies: &mut FuturesUnordered<BoxFuture<'static, BlockApplyCompletion>>,
    checkpoint_in_flight: &mut usize,
    full_in_flight: &mut usize,
    checkpoint_apply_limit: usize,
    full_apply_limit: usize,
    combined_apply_limit: usize,
    latest_chain_tip: impl ChainTip + Clone + Send + Sync + 'static,
    endpoint: Option<ZakuraEndpoint>,
    read_state: ReadState,
    block_verifier: BlockVerifier,
    block_sync: BlockSyncHandle,
    trace: ZakuraTrace,
    throughput_probe: Option<BlocksyncThroughputProbe>,
) where
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
    decrement_in_flight_apply_count(completed.class, checkpoint_in_flight, full_in_flight);

    drain_pending_block_applies(
        handoff,
        pending_applies,
        in_flight_applies,
        checkpoint_in_flight,
        full_in_flight,
        checkpoint_apply_limit,
        full_apply_limit,
        combined_apply_limit,
        latest_chain_tip,
        endpoint,
        read_state,
        block_verifier,
        block_sync,
        trace,
        throughput_probe,
    );
}

#[allow(clippy::too_many_arguments)]
fn drain_pending_block_applies<ReadState, BlockVerifier>(
    handoff: &std::sync::Arc<super::SyncCoordinator>,
    pending_applies: &mut VecDeque<PendingBlockApply>,
    in_flight_applies: &mut FuturesUnordered<BoxFuture<'static, BlockApplyCompletion>>,
    checkpoint_in_flight: &mut usize,
    full_in_flight: &mut usize,
    checkpoint_apply_limit: usize,
    full_apply_limit: usize,
    combined_apply_limit: usize,
    latest_chain_tip: impl ChainTip + Clone + Send + Sync + 'static,
    endpoint: Option<ZakuraEndpoint>,
    read_state: ReadState,
    block_verifier: BlockVerifier,
    block_sync: BlockSyncHandle,
    trace: ZakuraTrace,
    throughput_probe: Option<BlocksyncThroughputProbe>,
) where
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
    // Once legacy fallback owns body commits, start no new Zakura applies. The
    // loop releases queued bodies outside the apply-start path.
    if !handoff.zakura_owns_applies() {
        return;
    }

    // The checkpoint verifier can hold a complete range until its checkpoint is
    // reached. Keep room for the current range and the next complete range.
    let checkpoint_pipeline_apply_limit = checkpoint_apply_limit.saturating_mul(2);
    let checkpoint_combined_apply_limit = combined_apply_limit.max(checkpoint_pipeline_apply_limit);
    while let Some(index) = pending_applies
        .iter()
        .position(|pending| match pending.class {
            BlockApplyClass::Checkpoint => {
                *checkpoint_in_flight + *full_in_flight < checkpoint_combined_apply_limit
                    && *checkpoint_in_flight < checkpoint_pipeline_apply_limit
            }
            BlockApplyClass::Full => {
                *checkpoint_in_flight + *full_in_flight < combined_apply_limit
                    && *full_in_flight < full_apply_limit
            }
        })
    {
        let pending = pending_applies
            .remove(index)
            .expect("pending apply index was found in queue");

        match pending.class {
            BlockApplyClass::Checkpoint => {
                *checkpoint_in_flight = checkpoint_in_flight.saturating_add(1);
            }
            BlockApplyClass::Full => {
                *full_in_flight = full_in_flight.saturating_add(1);
            }
        }

        let class = pending.class;
        let operation = pending
            .operation
            .expect("ordinary pending applies own a registered operation");
        let Some(accepted) = operation.accept() else {
            decrement_in_flight_apply_count(class, checkpoint_in_flight, full_in_flight);
            abandon_block_apply(
                &block_sync,
                pending.owner,
                pending.source,
                pending.token,
                pending.block.as_ref(),
                &trace,
            );
            continue;
        };
        debug!(operation_id = ?accepted.id(), token = pending.token, "accepted native block apply operation");
        let transfer_handoff = handoff.clone();
        let transfer_block = pending.block.clone();
        let transfer_owner = pending.owner;
        let transfer_source = pending.source;
        let transfer_token = pending.token;
        let transfer_block_sync = block_sync.clone();
        let transfer_trace = trace.clone();
        let apply = apply_block_sync_body(
            block_verifier.clone(),
            latest_chain_tip.clone(),
            endpoint.clone(),
            read_state.clone(),
            block_sync.clone(),
            pending.owner,
            pending.source,
            pending.token,
            pending.block,
            class,
            trace.clone(),
            throughput_probe.clone(),
        );
        in_flight_applies.push(
            async move {
                tokio::pin!(apply);
                let mut accepted = Some(accepted);
                tokio::select! {
                    biased;
                    completed = &mut apply => {
                        let terminal = match completed.result {
                            BlockApplyResult::Committed | BlockApplyResult::Duplicate => {
                                super::BlockApplyTerminal::Committed
                            }
                            BlockApplyResult::Rejected
                            | BlockApplyResult::Unavailable
                            | BlockApplyResult::TimedOut => super::BlockApplyTerminal::Rejected,
                        };
                        accepted
                            .take()
                            .expect("accepted operation has one terminal result")
                            .complete(terminal);
                        completed
                    }
                    _ = transfer_handoff.wait_for_legacy_yield(),
                        if class == BlockApplyClass::Checkpoint =>
                    {
                        // The checkpoint verifier owns transactional range commits after it
                        // accepts a request. A partial range cannot commit until another request
                        // supplies every missing body. Legacy fallback uses the same verifier, so
                        // it can complete the range after this driver transfers completion
                        // responsibility.
                        let result = abandon_block_apply(
                            &transfer_block_sync,
                            transfer_owner,
                            transfer_source,
                            transfer_token,
                            transfer_block.as_ref(),
                            &transfer_trace,
                        );
                        accepted
                            .take()
                            .expect("accepted operation has one terminal result")
                            .complete(super::BlockApplyTerminal::TransferredToLegacy);
                        metrics::counter!(
                            "sync.zakura.apply.checkpoint_transferred_to_legacy"
                        )
                        .increment(1);
                        BlockApplyCompletion { class, result }
                    }
                }
            }
            .boxed(),
        );
    }
}

fn release_pending_applies(
    block_sync: &BlockSyncHandle,
    pending_applies: &mut VecDeque<PendingBlockApply>,
    trace: &ZakuraTrace,
) {
    for (height, expected_hash, result, event) in
        abandoned_pending_apply_finished_events(pending_applies)
    {
        let token = match &event {
            BlockSyncEvent::BlockApplyFinished { token, .. } => *token,
            _ => unreachable!("abandoned apply release only builds BlockApplyFinished events"),
        };

        let _ = block_sync.send_control(event);
        trace.trace_block_apply_finished(token, height, expected_hash, result, false);
    }
}

fn release_pending_probe_applies(
    block_sync: &BlockSyncHandle,
    pending_probe_applies: &mut BTreeMap<block::Height, PendingBlockApply>,
    trace: &ZakuraTrace,
) {
    let pending = std::mem::take(pending_probe_applies);
    for pending in pending.into_values() {
        abandon_block_apply(
            block_sync,
            pending.owner,
            pending.source,
            pending.token,
            pending.block.as_ref(),
            trace,
        );
    }
}

fn decrement_in_flight_apply_count(
    class: BlockApplyClass,
    checkpoint_in_flight: &mut usize,
    full_in_flight: &mut usize,
) {
    match class {
        BlockApplyClass::Checkpoint => {
            *checkpoint_in_flight = checkpoint_in_flight.saturating_sub(1);
        }
        BlockApplyClass::Full => {
            *full_in_flight = full_in_flight.saturating_sub(1);
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn drain_ordered_probe_applies<ReadState, BlockVerifier>(
    pending_probe_applies: &mut BTreeMap<block::Height, PendingBlockApply>,
    latest_chain_tip: impl ChainTip + Clone + Send + Sync + 'static,
    endpoint: Option<ZakuraEndpoint>,
    read_state: ReadState,
    block_verifier: BlockVerifier,
    block_sync: BlockSyncHandle,
    trace: ZakuraTrace,
    throughput_probe: BlocksyncThroughputProbe,
) where
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
    while let Ok(expected_height) = throughput_probe.verified_tip().next() {
        let Some(pending) = pending_probe_applies.remove(&expected_height) else {
            break;
        };
        let _completed = apply_probe_block_sync_body(
            latest_chain_tip.clone(),
            endpoint.clone(),
            read_state.clone(),
            block_verifier.clone(),
            block_sync.clone(),
            trace.clone(),
            throughput_probe.clone(),
            pending,
        )
        .await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn apply_probe_block_sync_body<ReadState, BlockVerifier>(
    latest_chain_tip: impl ChainTip + Clone + Send + Sync + 'static,
    endpoint: Option<ZakuraEndpoint>,
    read_state: ReadState,
    block_verifier: BlockVerifier,
    block_sync: BlockSyncHandle,
    trace: ZakuraTrace,
    throughput_probe: BlocksyncThroughputProbe,
    pending: PendingBlockApply,
) -> BlockApplyCompletion
where
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
    apply_block_sync_body(
        block_verifier,
        latest_chain_tip,
        endpoint,
        read_state,
        block_sync,
        pending.owner,
        pending.source,
        pending.token,
        pending.block,
        pending.class,
        trace,
        Some(throughput_probe),
    )
    .await
}

pub(crate) fn block_apply_class(
    block: &block::Block,
    max_checkpoint_height: block::Height,
) -> BlockApplyClass {
    if block
        .coinbase_height()
        .is_some_and(|height| height <= max_checkpoint_height)
    {
        BlockApplyClass::Checkpoint
    } else {
        BlockApplyClass::Full
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn apply_block_sync_body<BlockVerifier, ReadState>(
    block_verifier: BlockVerifier,
    _latest_chain_tip: impl ChainTip + Clone + Send + Sync + 'static,
    _endpoint: Option<ZakuraEndpoint>,
    _read_state: ReadState,
    block_sync: BlockSyncHandle,
    owner: zakura_header_chain::BodyWorkOwner,
    source: zakura_header_chain::SourceId,
    token: BlockApplyToken,
    block: Arc<block::Block>,
    class: BlockApplyClass,
    trace: ZakuraTrace,
    throughput_probe: Option<BlocksyncThroughputProbe>,
) -> BlockApplyCompletion
where
    BlockVerifier:
        Service<zakura_consensus::Request, Response = block::Hash> + Clone + Send + 'static,
    BlockVerifier::Error: std::fmt::Debug + Send + Sync + 'static,
    BlockVerifier::Future: Send + 'static,
    ReadState: Service<
            zakura_state::ReadRequest,
            Response = zakura_state::ReadResponse,
            Error = zakura_state::BoxError,
        > + Clone
        + Send
        + 'static,
    ReadState::Future: Send + 'static,
{
    let expected_hash = block.hash();
    let Some(height) = block.coinbase_height() else {
        warn!(
            ?expected_hash,
            "Zakura block sync cannot apply body without coinbase height"
        );
        return BlockApplyCompletion {
            class,
            result: BlockApplyResult::Rejected,
        };
    };

    trace.trace_block_commit_started(token, class, height, expected_hash);
    let started = Instant::now();
    // Throughput-probe mode (debug only): skip consensus verify+commit and
    // advance its in-memory synthetic frontier, discarding the body.
    let outcome = match throughput_probe.as_ref() {
        Some(probe) => {
            let (result, _) = probe.apply_block(block.as_ref());
            probe_body_outcome(owner, source, expected_hash, result)
        }
        None => {
            commit_block_sync_body_with_stall_trace(
                block_verifier.clone(),
                block,
                class,
                &trace,
                owner,
                source,
                token,
                height,
                expected_hash,
            )
            .await
        }
    };
    let result = outcome.result();
    trace.trace_block_commit_finished(token, class, height, expected_hash, result, started);
    let _ = block_sync.send_control(BlockSyncEvent::BlockApplyFinished {
        owner,
        source,
        token,
        height,
        hash: expected_hash,
        outcome,
    });
    trace.trace_block_apply_finished(token, height, expected_hash, result, false);

    BlockApplyCompletion { class, result }
}

#[cfg(test)]
pub(crate) async fn commit_block_sync_body<BlockVerifier>(
    block_verifier: BlockVerifier,
    owner: zakura_header_chain::BodyWorkOwner,
    source: zakura_header_chain::SourceId,
    block: Arc<block::Block>,
    _class: BlockApplyClass,
) -> BlockApplyOutcome
where
    BlockVerifier:
        Service<zakura_consensus::Request, Response = block::Hash> + Clone + Send + 'static,
    BlockVerifier::Error: std::fmt::Debug + Send + Sync + 'static,
    BlockVerifier::Future: Send + 'static,
{
    let expected_hash = block.hash();
    let height = block.coinbase_height();
    let commit = block_verifier
        .clone()
        .oneshot(zakura_consensus::Request::Commit(block));
    block_commit_outcome(owner, source, height, expected_hash, commit.await)
}

#[allow(clippy::too_many_arguments)]
async fn commit_block_sync_body_with_stall_trace<BlockVerifier>(
    block_verifier: BlockVerifier,
    block: Arc<block::Block>,
    class: BlockApplyClass,
    trace: &ZakuraTrace,
    owner: zakura_header_chain::BodyWorkOwner,
    source: zakura_header_chain::SourceId,
    token: BlockApplyToken,
    height: block::Height,
    expected_hash: block::Hash,
) -> BlockApplyOutcome
where
    BlockVerifier:
        Service<zakura_consensus::Request, Response = block::Hash> + Clone + Send + 'static,
    BlockVerifier::Error: std::fmt::Debug + Send + Sync + 'static,
    BlockVerifier::Future: Send + 'static,
{
    let commit = block_verifier
        .clone()
        .oneshot(zakura_consensus::Request::Commit(block));

    tokio::pin!(commit);
    tokio::select! {
        outcome = &mut commit => block_commit_outcome(owner, source, Some(height), expected_hash, outcome),
        _ = tokio::time::sleep(ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT) => {
            trace.trace_block_commit_stalled(
                token,
                class,
                height,
                expected_hash,
                ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT,
            );
            block_commit_outcome(owner, source, Some(height), expected_hash, commit.await)
        }
    }
}

fn block_commit_outcome<E>(
    owner: zakura_header_chain::BodyWorkOwner,
    source: zakura_header_chain::SourceId,
    height: Option<block::Height>,
    expected_hash: block::Hash,
    outcome: Result<block::Hash, E>,
) -> BlockApplyOutcome
where
    E: std::fmt::Debug + Send + Sync + 'static,
{
    match outcome {
        Ok(committed_hash) if committed_hash == expected_hash => {
            debug!(
                ?height,
                ?committed_hash,
                "Zakura block sync committed block body through verifier"
            );
            BlockApplyOutcome::committed(zakura_header_chain::VerifiedBodyEvidence {
                hash: expected_hash,
                evidence: body_outcome_evidence(b"committed", owner, source, expected_hash, &[]),
            })
        }
        Ok(committed_hash) => {
            warn!(
                ?height,
                ?expected_hash,
                ?committed_hash,
                "Zakura block-sync verifier returned an unexpected hash"
            );
            BlockApplyOutcome::retryable(zakura_header_chain::TransientBodyFailure {
                hash: expected_hash,
                evidence: body_outcome_evidence(
                    b"verifier-unexpected-hash",
                    owner,
                    source,
                    expected_hash,
                    &committed_hash.0,
                ),
                kind: zakura_header_chain::TransientBodyFailureKind::VerifierUnavailable,
                availability: zakura_header_chain::BodyUnavailableSummary::default(),
            })
        }
        Err(error) => {
            use zakura_header_chain::BodyVerificationClass;

            let class = block_verify_error_class(&error);
            debug!(
                ?height,
                ?expected_hash,
                ?class,
                ?error,
                "Zakura block-sync verifier classified a body result"
            );
            match class {
                BodyVerificationClass::Duplicate => {
                    BlockApplyOutcome::duplicate(zakura_header_chain::VerifiedBodyEvidence {
                        hash: expected_hash,
                        evidence: body_outcome_evidence(
                            b"duplicate",
                            owner,
                            source,
                            expected_hash,
                            &[],
                        ),
                    })
                }
                BodyVerificationClass::PayloadMismatch(kind) => {
                    BlockApplyOutcome::payload_mismatch(zakura_header_chain::BodyPayloadMismatch {
                        evidence: body_outcome_evidence(
                            b"payload-mismatch",
                            owner,
                            source,
                            expected_hash,
                            body_commitment_kind_label(kind).as_bytes(),
                        ),
                        requested: expected_hash,
                        delivered: expected_hash,
                        kind,
                        source,
                    })
                }
                BodyVerificationClass::ConsensusInvalid(rule) => {
                    BlockApplyOutcome::consensus_invalid(
                        zakura_header_chain::ConsensusBodyInvalid {
                            hash: expected_hash,
                            evidence: intrinsic_body_invalid_evidence(expected_hash, &rule),
                            rule,
                            source,
                        },
                    )
                }
                BodyVerificationClass::Retryable(kind) => {
                    warn!(
                        ?height,
                        ?expected_hash,
                        ?kind,
                        diagnostic = block_verify_error_diagnostic(&error).as_deref(),
                        "Zakura block-sync verifier could not apply a body"
                    );
                    retryable_body_outcome(owner, source, expected_hash, kind)
                }
            }
        }
    }
}

fn retryable_body_outcome(
    owner: zakura_header_chain::BodyWorkOwner,
    source: zakura_header_chain::SourceId,
    hash: block::Hash,
    kind: zakura_header_chain::TransientBodyFailureKind,
) -> BlockApplyOutcome {
    BlockApplyOutcome::retryable(zakura_header_chain::TransientBodyFailure {
        hash,
        evidence: body_outcome_evidence(
            b"retryable",
            owner,
            source,
            hash,
            transient_failure_kind_label(kind).as_bytes(),
        ),
        kind,
        availability: zakura_header_chain::BodyUnavailableSummary::default(),
    })
}

fn probe_body_outcome(
    owner: zakura_header_chain::BodyWorkOwner,
    source: zakura_header_chain::SourceId,
    hash: block::Hash,
    result: BlockApplyResult,
) -> BlockApplyOutcome {
    match result {
        BlockApplyResult::Committed => {
            BlockApplyOutcome::committed(zakura_header_chain::VerifiedBodyEvidence {
                hash,
                evidence: body_outcome_evidence(b"probe-committed", owner, source, hash, &[]),
            })
        }
        BlockApplyResult::Duplicate => {
            BlockApplyOutcome::duplicate(zakura_header_chain::VerifiedBodyEvidence {
                hash,
                evidence: body_outcome_evidence(b"probe-duplicate", owner, source, hash, &[]),
            })
        }
        BlockApplyResult::Rejected => retryable_body_outcome(
            owner,
            source,
            hash,
            zakura_header_chain::TransientBodyFailureKind::MissingContext,
        ),
        BlockApplyResult::Unavailable => retryable_body_outcome(
            owner,
            source,
            hash,
            zakura_header_chain::TransientBodyFailureKind::VerifierUnavailable,
        ),
        BlockApplyResult::TimedOut => retryable_body_outcome(
            owner,
            source,
            hash,
            zakura_header_chain::TransientBodyFailureKind::Timeout,
        ),
    }
}

fn body_outcome_evidence(
    kind: &[u8],
    owner: zakura_header_chain::BodyWorkOwner,
    source: zakura_header_chain::SourceId,
    hash: block::Hash,
    detail: &[u8],
) -> zakura_header_chain::EvidenceId {
    let mut hasher = Sha256::new();
    hasher.update(b"zakura-body-apply-outcome-v1");
    hash_bytes(&mut hasher, kind);
    hasher.update(owner.header_generation.get().to_le_bytes());
    hasher.update(owner.verified_generation.get().to_le_bytes());
    hasher.update(owner.branch.anchor_hash.0);
    hasher.update(owner.branch.target_tip_hash.0);
    hasher.update(owner.session_id.to_le_bytes());
    hasher.update(owner.request_id.get().to_le_bytes());
    hasher.update(source.digest());
    hasher.update(hash.0);
    hash_bytes(&mut hasher, detail);
    zakura_header_chain::EvidenceId::from_digest(hasher.finalize().into())
}

fn intrinsic_body_invalid_evidence(
    hash: block::Hash,
    rule: &zakura_header_chain::BodyRuleId,
) -> zakura_header_chain::EvidenceId {
    let mut hasher = Sha256::new();
    hasher.update(b"zakura-consensus-body-invalid-v1");
    hasher.update(hash.0);
    hash_bytes(&mut hasher, rule.as_str().as_bytes());
    zakura_header_chain::EvidenceId::from_digest(hasher.finalize().into())
}

fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    let length = u64::try_from(bytes.len())
        .expect("slice length fits in u64 on every supported Zakura target");
    hasher.update(length.to_le_bytes());
    hasher.update(bytes);
}

fn body_commitment_kind_label(kind: zakura_header_chain::BodyCommitmentKind) -> &'static str {
    match kind {
        zakura_header_chain::BodyCommitmentKind::HeaderHash => "header_hash",
        zakura_header_chain::BodyCommitmentKind::TransactionMerkleRoot => "transaction_merkle_root",
        zakura_header_chain::BodyCommitmentKind::AuthDataRoot => "auth_data_root",
        zakura_header_chain::BodyCommitmentKind::Other(label) => label,
    }
}

fn transient_failure_kind_label(
    kind: zakura_header_chain::TransientBodyFailureKind,
) -> &'static str {
    match kind {
        zakura_header_chain::TransientBodyFailureKind::MissingContext => "missing_context",
        zakura_header_chain::TransientBodyFailureKind::Canceled => "canceled",
        zakura_header_chain::TransientBodyFailureKind::Storage => "storage",
        zakura_header_chain::TransientBodyFailureKind::VerifierUnavailable => {
            "verifier_unavailable"
        }
        zakura_header_chain::TransientBodyFailureKind::Timeout => "timeout",
        zakura_header_chain::TransientBodyFailureKind::ResourceExhausted => "resource_exhausted",
    }
}

pub(crate) async fn query_block_sync_needed_blocks<ReadState>(
    read_state: ReadState,
    from: block::Height,
    limit: u32,
) -> Result<(zakura_header_chain::Frontier, Vec<BlockSyncBlockMeta>), zakura_state::BoxError>
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
    if limit == 0 {
        return Err(
            std::io::Error::other("block-sync needed-body query limit must be nonzero").into(),
        );
    }

    let mut needed = Vec::new();
    let mut body_anchor = None;
    let mut next_from = from;
    let mut remaining = limit;

    while remaining > 0 {
        let chunk_limit = remaining.min(zakura_state::constants::MAX_HEADER_SYNC_HEIGHT_RANGE);
        let chunk =
            query_block_sync_needed_blocks_chunk(read_state.clone(), next_from, chunk_limit)
                .await?;
        if body_anchor
            .replace(chunk.anchor)
            .is_some_and(|anchor| anchor != chunk.anchor)
        {
            return Err(std::io::Error::other(
                "block-sync body anchor changed across one chunked state query",
            )
            .into());
        }
        needed.extend(block_sync_needed_blocks_from_state(chunk.blocks));

        remaining = remaining.saturating_sub(chunk_limit);
        let Some(after_chunk) = next_from.0.checked_add(chunk_limit).map(block::Height) else {
            break;
        };
        next_from = after_chunk;
    }

    let body_anchor = body_anchor
        .ok_or_else(|| std::io::Error::other("block-sync needed-body query returned no anchor"))?;
    Ok((body_anchor, needed))
}

async fn query_block_sync_needed_blocks_chunk<ReadState>(
    read_state: ReadState,
    from: block::Height,
    limit: u32,
) -> Result<zakura_state::BlockSyncBodyMetadata, zakura_state::BoxError>
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
    let metadata = match tokio::time::timeout(
        ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT,
        read_state.oneshot(zakura_state::ReadRequest::MissingBlockBodyMetadata { from, limit }),
    )
    .await
    {
        Ok(Ok(zakura_state::ReadResponse::MissingBlockBodyMetadata(metadata))) => metadata,
        Ok(Ok(response)) => {
            warn!(?response, "unexpected MissingBlockBodyMetadata response");
            return Err(
                std::io::Error::other("unexpected MissingBlockBodyMetadata response").into(),
            );
        }
        Ok(Err(error)) => return Err(error),
        Err(elapsed) => return Err(Box::new(elapsed)),
    };

    Ok(metadata)
}

#[cfg(test)]
pub(crate) fn block_sync_missing_body_window(
    from: block::Height,
    best_header_tip: block::Height,
    limit: u32,
) -> Option<(block::Height, u32)> {
    if best_header_tip < from || limit == 0 {
        return None;
    }

    let available = best_header_tip
        .0
        .saturating_sub(from.0)
        .saturating_add(1)
        .clamp(1, ZAKURA_BLOCK_SYNC_MISSING_BODY_WINDOW);
    Some((from, available.min(limit)))
}

pub(crate) fn block_sync_needed_blocks_from_state(
    metadata: Vec<(block::Height, block::Hash, Option<u32>)>,
) -> Vec<BlockSyncBlockMeta> {
    metadata
        .into_iter()
        .map(|(height, hash, size)| {
            let size = size
                .filter(|size| *size > 0)
                .map(BlockSizeEstimate::Advertised)
                .unwrap_or(BlockSizeEstimate::Unknown);

            BlockSyncBlockMeta { height, hash, size }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::super::SyncCoordinator;
    use super::*;

    use std::{
        future::{ready, Ready},
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Poll},
        time::Duration,
    };

    use futures::FutureExt;
    use tokio::sync::{mpsc, oneshot, watch};
    use tower::{service_fn, Service};
    use zakura_chain::serialization::ZcashDeserializeInto;
    use zakura_network::zakura::{
        testkit::{TraceCapture, TraceValue},
        BlockSyncFrontiers, COMMIT_STATE_TABLE,
    };
    use zakura_test::vectors::{BLOCK_MAINNET_1_BYTES, BLOCK_MAINNET_2_BYTES};

    #[derive(Clone, Debug)]
    struct BackpressuredVerifier {
        ready_polls: Arc<AtomicUsize>,
        calls: Arc<AtomicUsize>,
    }

    impl Service<zakura_consensus::Request> for BackpressuredVerifier {
        type Response = block::Hash;
        type Error = zakura_consensus::BoxError;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.ready_polls.fetch_add(1, Ordering::SeqCst);
            Poll::Pending
        }

        fn call(&mut self, request: zakura_consensus::Request) -> Self::Future {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let zakura_consensus::Request::Commit(block) = request else {
                panic!("unexpected consensus request: {request:?}");
            };
            ready(Ok(block.hash()))
        }
    }

    struct TestDriver {
        actions: mpsc::Sender<BlockSyncAction>,
        shutdown: oneshot::Sender<()>,
        driver_task: tokio::task::JoinHandle<()>,
        reactor_task: tokio::task::JoinHandle<()>,
    }

    impl TestDriver {
        async fn shutdown(self) {
            let _ = self.shutdown.send(());
            self.driver_task
                .await
                .expect("block-sync driver exits cleanly");
            self.reactor_task.abort();
        }
    }

    fn spawn_checkpoint_driver<BlockVerifier>(
        block_verifier: BlockVerifier,
        handoff: Arc<SyncCoordinator>,
        trace: ZakuraTrace,
    ) -> TestDriver
    where
        BlockVerifier:
            Service<zakura_consensus::Request, Response = block::Hash> + Clone + Send + 'static,
        BlockVerifier::Error: std::fmt::Debug + Send + Sync + 'static,
        BlockVerifier::Future: Send + 'static,
    {
        let (tip_tx, tip_rx) = watch::channel((block::Height(0), block::Hash([0; 32])));
        drop(tip_tx);
        let startup = zakura_network::zakura::BlockSyncStartup::new(
            BlockSyncFrontiers {
                finalized_height: block::Height(0),
                verified_block_tip: block::Height(0),
                verified_block_hash: block::Hash([0; 32]),
            },
            (block::Height(0), block::Hash([0; 32])),
            tip_rx,
            zakura_network::zakura::ZakuraBlockSyncConfig::default(),
        );
        let (block_sync, _reactor_actions, reactor_task) =
            zakura_network::zakura::spawn_block_sync_reactor(startup);
        let read_state = service_fn(|request: zakura_state::ReadRequest| async move {
            panic!("unexpected read request: {request:?}");
            #[allow(unreachable_code)]
            Ok::<_, zakura_state::BoxError>(zakura_state::ReadResponse::Tip(None))
        });
        let (actions, action_rx) = mpsc::channel(8);
        let (shutdown, shutdown_rx) = oneshot::channel();
        let driver_task = tokio::spawn(drive_block_sync_actions(
            action_rx,
            zakura_network::zakura::ZakuraSupervisorHandle::new(1),
            None,
            block_sync,
            zakura_chain::chain_tip::NoChainTip,
            read_state,
            None,
            None,
            block_verifier,
            block::Height::MAX,
            sync::MIN_CHECKPOINT_CONCURRENCY_LIMIT,
            sync::MIN_CONCURRENCY_LIMIT,
            sync::DEFAULT_ZAKURA_BLOCK_APPLY_CONCURRENCY_LIMIT,
            trace,
            None,
            handoff,
            async move {
                let _ = shutdown_rx.await;
            },
        ));

        TestDriver {
            actions,
            shutdown,
            driver_task,
            reactor_task,
        }
    }

    async fn submit_checkpoint(driver: &TestDriver, token: BlockApplyToken) {
        driver
            .actions
            .send(BlockSyncAction::SubmitBlock {
                owner: test_owner(),
                source: test_source(),
                token,
                block: mainnet_block(&BLOCK_MAINNET_1_BYTES),
            })
            .await
            .expect("block-sync driver action channel stays open");
    }

    async fn wait_for_count(counter: &AtomicUsize, expected: usize, context: &str) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while counter.load(Ordering::SeqCst) < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {context}"));
    }

    fn mainnet_block(bytes: &[u8]) -> Arc<block::Block> {
        Arc::new(bytes.zcash_deserialize_into().expect("block vector parses"))
    }

    fn test_owner() -> zakura_header_chain::BodyWorkOwner {
        zakura_header_chain::BodyWorkAuthority {
            header: zakura_header_chain::HeaderWorkAuthority {
                header_generation: zakura_header_chain::HeaderGeneration::new(1),
                branch: zakura_header_chain::BranchId::new(
                    block::Hash([0; 32]),
                    block::Hash([1; 32]),
                ),
            },
            verified_generation: zakura_header_chain::VerifiedGeneration::new(1),
            body_work_epoch: zakura_header_chain::BodyWorkEpoch::default(),
        }
        .bind(
            1,
            std::num::NonZeroU64::new(1).expect("test request ID is nonzero"),
        )
    }

    fn test_source() -> zakura_header_chain::SourceId {
        zakura_header_chain::SourceId::from_digest([2; 32])
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fallback_transfers_checkpoint_apply_before_verifier_admission() {
        let ready_polls = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let verifier = BackpressuredVerifier {
            ready_polls: ready_polls.clone(),
            calls: calls.clone(),
        };
        let handoff = SyncCoordinator::new();
        let driver = spawn_checkpoint_driver(verifier, handoff.clone(), ZakuraTrace::noop());

        submit_checkpoint(&driver, 1).await;
        wait_for_count(
            ready_polls.as_ref(),
            1,
            "the checkpoint verifier readiness poll",
        )
        .await;

        let lease = tokio::time::timeout(
            Duration::from_secs(1),
            handoff.acquire_legacy_fallback(Duration::from_secs(1)),
        )
        .await
        .expect("backpressured checkpoint apply transfers without blocking fallback")
        .expect("fallback acquires the apply lease after transfer");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "transfer must not wait for the verifier to admit the request"
        );

        drop(lease);
        driver.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn completed_checkpoint_apply_wins_over_ready_fallback_transfer() {
        let (release_tx, release_rx) = watch::channel(false);
        let calls = Arc::new(AtomicUsize::new(0));
        let verifier_calls = calls.clone();
        let verifier = service_fn(move |request: zakura_consensus::Request| {
            let mut release_rx = release_rx.clone();
            let verifier_calls = verifier_calls.clone();
            async move {
                let zakura_consensus::Request::Commit(block) = request else {
                    panic!("unexpected consensus request: {request:?}");
                };
                verifier_calls.fetch_add(1, Ordering::SeqCst);
                while !*release_rx.borrow() {
                    release_rx
                        .changed()
                        .await
                        .expect("commit release sender stays open");
                }
                Ok::<_, zakura_consensus::BoxError>(block.hash())
            }
        });
        let mut capture =
            TraceCapture::for_test("completed_checkpoint_apply_wins_over_ready_fallback_transfer")
                .expect("test trace capture initializes");
        let trace = ZakuraTrace::new(capture.tracer(), "01");
        let handoff = SyncCoordinator::new();
        let driver = spawn_checkpoint_driver(verifier, handoff.clone(), trace);

        submit_checkpoint(&driver, 2).await;
        wait_for_count(calls.as_ref(), 1, "the checkpoint verifier call").await;

        let fallback = handoff.acquire_legacy_fallback(Duration::from_secs(1));
        tokio::pin!(fallback);
        assert!(
            fallback.as_mut().now_or_never().is_none(),
            "fallback starts draining while the checkpoint apply is pending"
        );
        assert!(handoff.is_yielded_to_legacy());

        release_tx
            .send(true)
            .expect("commit release receiver stays open");
        let lease = tokio::time::timeout(Duration::from_secs(1), &mut fallback)
            .await
            .expect("completed checkpoint apply releases the fallback drain")
            .expect("fallback acquires the lease after native completion");

        capture.flush().await;
        let reader = capture.reader().expect("test trace is readable");
        reader.table(COMMIT_STATE_TABLE.table()).assert_row(
            "reactor_event_sent",
            &[
                ("apply_token", TraceValue::U64(2)),
                ("result", TraceValue::Str("committed")),
            ],
        );

        drop(lease);
        driver.shutdown().await;
    }

    #[test]
    fn body_apply_evidence_is_canonical_or_attempt_scoped_by_outcome_kind() {
        let owner = test_owner();
        let source = test_source();
        let hash = block::Hash([3; 32]);
        let detail = b"storage";

        let attempt = body_outcome_evidence(b"retryable", owner, source, hash, detail);
        assert_eq!(
            attempt,
            body_outcome_evidence(b"retryable", owner, source, hash, detail),
            "the same attempt and result must produce stable evidence"
        );

        let mut other_owner = owner;
        other_owner.request_id = std::num::NonZeroU64::new(2).expect("test request ID is nonzero");
        assert_ne!(
            attempt,
            body_outcome_evidence(b"retryable", other_owner, source, hash, detail),
            "different requests must not share transient-attempt evidence"
        );
        assert_ne!(
            attempt,
            body_outcome_evidence(
                b"retryable",
                owner,
                zakura_header_chain::SourceId::from_digest([4; 32]),
                hash,
                detail,
            ),
            "different suppliers must not share transient-attempt evidence"
        );

        let rule = zakura_header_chain::BodyRuleId::new("block.no_transactions");
        assert_eq!(
            intrinsic_body_invalid_evidence(hash, &rule),
            intrinsic_body_invalid_evidence(hash, &rule),
            "intrinsic invalidity must be independent of delivery order and supplier"
        );
        assert_ne!(
            intrinsic_body_invalid_evidence(hash, &rule),
            intrinsic_body_invalid_evidence(
                hash,
                &zakura_header_chain::BodyRuleId::new("block.bad_coinbase"),
            ),
            "different consensus rules must not share evidence"
        );

        let invalid = || zakura_consensus::VerifyBlockError::Block {
            source: zakura_consensus::BlockError::NoTransactions,
        };
        let first_invalid =
            block_commit_outcome(owner, source, None, hash, Err::<block::Hash, _>(invalid()));
        let second_source = zakura_header_chain::SourceId::from_digest([4; 32]);
        let second_invalid = block_commit_outcome(
            other_owner,
            second_source,
            None,
            hash,
            Err::<block::Hash, _>(invalid()),
        );
        assert_eq!(
            first_invalid.evidence(),
            second_invalid.evidence(),
            "intrinsic consensus evidence must not depend on request or supplier"
        );
        assert!(matches!(
            second_invalid.verification(),
            zakura_header_chain::BodyVerificationOutcome::ConsensusInvalid(
                zakura_header_chain::ConsensusBodyInvalid {
                    source: actual_source,
                    ..
                }
            ) if *actual_source == second_source
        ));
    }

    #[test]
    fn unexpected_verifier_hash_is_retryable_without_supplier_blame() {
        let expected_hash = block::Hash([5; 32]);
        let delivered_hash = block::Hash([6; 32]);
        let outcome = block_commit_outcome::<std::convert::Infallible>(
            test_owner(),
            test_source(),
            None,
            expected_hash,
            Ok(delivered_hash),
        );

        assert!(matches!(
            outcome.verification(),
            zakura_header_chain::BodyVerificationOutcome::Retryable(
                zakura_header_chain::TransientBodyFailure {
                    hash,
                    kind: zakura_header_chain::TransientBodyFailureKind::VerifierUnavailable,
                    ..
                }
            ) if *hash == expected_hash
        ));
        assert_eq!(outcome.result(), BlockApplyResult::Unavailable);
    }

    #[test]
    fn abandoned_pending_apply_events_drain_queued_blocks() {
        let block1 = mainnet_block(&BLOCK_MAINNET_1_BYTES);
        let block2 = mainnet_block(&BLOCK_MAINNET_2_BYTES);
        let block1_height = block1.coinbase_height().expect("test block has height");
        let block2_height = block2.coinbase_height().expect("test block has height");
        let block1_hash = block1.hash();
        let block2_hash = block2.hash();
        let mut pending_applies = VecDeque::from([
            PendingBlockApply {
                owner: test_owner(),
                source: test_source(),
                token: 11,
                class: BlockApplyClass::Full,
                block: block1,
                operation: None,
            },
            PendingBlockApply {
                owner: test_owner(),
                source: test_source(),
                token: 12,
                class: BlockApplyClass::Full,
                block: block2,
                operation: None,
            },
        ]);

        let events = abandoned_pending_apply_finished_events(&mut pending_applies);

        assert!(
            pending_applies.is_empty(),
            "abandoned pending applies must be drained and dropped"
        );
        assert_eq!(events.len(), 2);
        assert!(matches!(
            &events[0],
            (
                height,
                hash,
                BlockApplyResult::Unavailable,
                BlockSyncEvent::BlockApplyFinished {
                    token: 11,
                    height: event_height,
                    hash: event_hash,
                    outcome,
                    ..
                },
            ) if *height == block1_height
                && *hash == block1_hash
                && *event_height == block1_height
                && *event_hash == block1_hash
                && matches!(
                    outcome.verification(),
                    zakura_header_chain::BodyVerificationOutcome::Retryable(
                        zakura_header_chain::TransientBodyFailure {
                            kind: zakura_header_chain::TransientBodyFailureKind::Canceled,
                            ..
                        }
                    )
                )
        ));
        assert!(matches!(
            &events[1],
            (
                height,
                hash,
                BlockApplyResult::Unavailable,
                BlockSyncEvent::BlockApplyFinished {
                    token: 12,
                    height: event_height,
                    hash: event_hash,
                    outcome,
                    ..
                },
            ) if *height == block2_height
                && *hash == block2_hash
                && *event_height == block2_height
                && *event_hash == block2_hash
                && matches!(
                    outcome.verification(),
                    zakura_header_chain::BodyVerificationOutcome::Retryable(
                        zakura_header_chain::TransientBodyFailure {
                            kind: zakura_header_chain::TransientBodyFailureKind::Canceled,
                            ..
                        }
                    )
                )
        ));
    }
}
