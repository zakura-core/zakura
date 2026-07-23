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
use tracing::{debug, warn};

use zakura_chain::{block, chain_tip::ChainTip};
use zakura_network::zakura::{
    commit_state_trace as cs_trace, BlockApplyOutcome, BlockApplyResult, BlockApplyToken,
    BlockSizeEstimate, BlockSyncAction, BlockSyncBlockMeta, BlockSyncEvent, BlockSyncHandle,
    BlockSyncMisbehavior, ZakuraEndpoint, ZakuraTrace,
};

use crate::components::sync;

use super::{
    block_apply_result_label, block_verify_error_class, emit_commit_state, insert_cs_hash,
    insert_cs_height, insert_cs_peer, insert_cs_str, insert_cs_u64, BlocksyncThroughputProbe,
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

#[derive(Clone, Debug)]
struct PendingBlockApply {
    owner: zakura_header_chain::WorkOwner,
    source: zakura_header_chain::SourceId,
    token: BlockApplyToken,
    class: BlockApplyClass,
    block: Arc<block::Block>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct BlockApplyCompletion {
    class: BlockApplyClass,
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
    block_verifier: BlockVerifier,
    max_checkpoint_height: block::Height,
    checkpoint_apply_limit: usize,
    full_apply_limit: usize,
    combined_apply_limit: usize,
    trace: ZakuraTrace,
    throughput_probe: Option<BlocksyncThroughputProbe>,
    block_sync_handoff: std::sync::Arc<super::BlockSyncHandoff>,
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

    loop {
        if block_sync_handoff.is_yielded_to_legacy() {
            release_pending_applies(&block_sync, &mut pending_applies, &trace);
            release_pending_probe_applies(&block_sync, &mut pending_probe_applies, &trace);
        }

        if !shutting_down && shutdown.as_mut().now_or_never().is_some() {
            shutting_down = true;
            pending_applies.clear();
            pending_probe_applies.clear();
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
                    pending_applies.clear();
                    pending_probe_applies.clear();
                    deferred_actions.clear();
                    continue;
                },
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
                        return;
                    };
                    action
                }
            }
        };
        let action =
            coalesce_stale_needed_block_queries(action, &mut actions, &mut deferred_actions);

        trace_block_driver_action(&trace, &action);
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
                match tokio::time::timeout(
                    ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT,
                    writer.clone().oneshot(
                        zakura_state::Request::RecordHeaderChainBodyUnavailable {
                            expected_version,
                            failure,
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
                match tokio::time::timeout(
                    ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT,
                    writer
                        .clone()
                        .oneshot(zakura_state::Request::RecordHeaderChainBodyInvalid {
                            expected_version,
                            invalid,
                        }),
                )
                .await
                {
                    Ok(Ok(zakura_state::Response::HeaderChainBodyInvalidRecorded(_))) => {}
                    Ok(Ok(response)) => warn!(
                        ?response,
                        "unexpected header-chain invalid-body persistence response"
                    ),
                    Ok(Err(error)) => debug!(
                        ?error,
                        "header-chain invalid-body persistence was stale or unavailable"
                    ),
                    Err(_) => warn!("timed out persisting header-chain invalid-body evidence"),
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
                match tokio::time::timeout(
                    ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT,
                    writer.clone().oneshot(
                        zakura_state::Request::RestartHeaderChainBodyAvailability {
                            expected_version,
                            discovery,
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
                match tokio::time::timeout(
                    ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT,
                    writer.clone().oneshot(
                        zakura_state::Request::RetryHeaderChainBodyAvailability {
                            expected_version,
                            retry,
                        },
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
                emit_commit_state(
                    &trace,
                    cs_trace::STATE_READ_START,
                    "block_sync_driver",
                    |row| {
                        insert_cs_str(row, cs_trace::ACTION, "query_needed_blocks");
                        insert_cs_height(row, cs_trace::RANGE_START, from);
                        insert_cs_u64(row, cs_trace::RANGE_COUNT, u64::from(limit));
                        insert_cs_height(row, cs_trace::BEST_HEADER_TIP, best_header_tip);
                    },
                );
                let started = Instant::now();
                match query_block_sync_needed_blocks(read_state.clone(), from, limit).await {
                    Ok(blocks) => {
                        emit_commit_state(
                            &trace,
                            cs_trace::STATE_READ_SUCCESS,
                            "block_sync_driver",
                            |row| {
                                insert_cs_str(row, cs_trace::ACTION, "query_needed_blocks");
                                insert_cs_u64(row, cs_trace::RANGE_COUNT, blocks.len() as u64);
                                insert_cs_u64(row, cs_trace::ELAPSED_MS, elapsed_ms(started));
                            },
                        );
                        let _ = block_sync.send_control(BlockSyncEvent::ScopedNeededBlocks {
                            query_id,
                            scope,
                            blocks,
                        });
                        emit_commit_state(
                            &trace,
                            cs_trace::REACTOR_EVENT_SENT,
                            "block_sync_driver",
                            |row| {
                                insert_cs_str(row, cs_trace::ACTION, "needed_blocks");
                            },
                        );
                    }
                    Err(error) => {
                        emit_commit_state(
                            &trace,
                            cs_trace::STATE_READ_ERROR,
                            "block_sync_driver",
                            |row| {
                                insert_cs_str(row, cs_trace::ACTION, "query_needed_blocks");
                                insert_cs_str(row, cs_trace::RESULT, "error");
                                insert_cs_str(row, cs_trace::REASON, &format!("{error}"));
                                insert_cs_u64(row, cs_trace::ELAPSED_MS, elapsed_ms(started));
                            },
                        );
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
                emit_commit_state(
                    &trace,
                    cs_trace::STATE_READ_START,
                    "block_sync_driver",
                    |row| {
                        insert_cs_str(row, cs_trace::ACTION, "query_blocks_by_height_range");
                        insert_cs_peer(row, cs_trace::PEER, &peer);
                        insert_cs_height(row, cs_trace::RANGE_START, start);
                        insert_cs_u64(row, cs_trace::RANGE_COUNT, u64::from(count));
                    },
                );
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
                        emit_commit_state(
                            &trace,
                            cs_trace::STATE_READ_SUCCESS,
                            "block_sync_driver",
                            |row| {
                                insert_cs_str(
                                    row,
                                    cs_trace::ACTION,
                                    "query_blocks_by_height_range",
                                );
                                insert_cs_peer(row, cs_trace::PEER, &peer);
                                insert_cs_height(row, cs_trace::RANGE_START, start);
                                insert_cs_u64(row, cs_trace::RANGE_COUNT, blocks.len() as u64);
                                insert_cs_u64(row, cs_trace::ELAPSED_MS, elapsed_ms(started));
                            },
                        );
                        emit_commit_state(
                            &trace,
                            cs_trace::REACTOR_EVENT_SENT,
                            "block_sync_driver",
                            |row| {
                                insert_cs_str(row, cs_trace::ACTION, "block_range_response_ready");
                                insert_cs_peer(row, cs_trace::PEER, &peer);
                                insert_cs_height(row, cs_trace::RANGE_START, start);
                                insert_cs_u64(row, cs_trace::RANGE_COUNT, u64::from(count));
                            },
                        );
                        let _ = block_sync.send_control(BlockSyncEvent::BlockRangeResponseReady {
                            peer,
                            start_height: start,
                            requested_count: count,
                            blocks,
                        });
                    }
                    Ok(Ok(response)) => {
                        trace_block_range_error(
                            &trace,
                            &peer,
                            start,
                            count,
                            "unexpected_response",
                            started,
                        );
                        warn!(?peer, ?response, "unexpected BlocksByHeightRange response");
                        trace_block_range_finished(&trace, &peer, start, count, 0);
                        let _ =
                            block_sync.send_control(BlockSyncEvent::BlockRangeResponseFinished {
                                peer,
                                start_height: start,
                                requested_count: count,
                                returned_count: 0,
                            });
                    }
                    Ok(Err(error)) => {
                        trace_block_range_error(
                            &trace,
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
                        trace_block_range_finished(&trace, &peer, start, count, 0);
                        let _ =
                            block_sync.send_control(BlockSyncEvent::BlockRangeResponseFinished {
                                peer,
                                start_height: start,
                                requested_count: count,
                                returned_count: 0,
                            });
                    }
                    Err(_elapsed) => {
                        emit_commit_state(
                            &trace,
                            cs_trace::STATE_READ_TIMEOUT,
                            "block_sync_driver",
                            |row| {
                                insert_cs_str(
                                    row,
                                    cs_trace::ACTION,
                                    "query_blocks_by_height_range",
                                );
                                insert_cs_peer(row, cs_trace::PEER, &peer);
                                insert_cs_height(row, cs_trace::RANGE_START, start);
                                insert_cs_u64(row, cs_trace::RANGE_COUNT, u64::from(count));
                                insert_cs_u64(row, cs_trace::ELAPSED_MS, elapsed_ms(started));
                            },
                        );
                        warn!(?peer, "timed out reading Zakura block-sync serving range");
                        trace_block_range_finished(&trace, &peer, start, count, 0);
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
                emit_commit_state(
                    &trace,
                    cs_trace::BLOCK_SUBMIT_QUEUED,
                    "block_sync_driver",
                    |row| {
                        insert_cs_u64(row, cs_trace::APPLY_TOKEN, token);
                        insert_cs_str(row, cs_trace::APPLY_CLASS, block_apply_class_label(class));
                        insert_cs_hash(row, cs_trace::HASH, block.hash());
                        if let Some(height) = height {
                            insert_cs_height(row, cs_trace::HEIGHT, height);
                        }
                        let queue_len = if throughput_probe.is_some() {
                            pending_probe_applies.len()
                        } else {
                            pending_applies.len()
                        };
                        insert_cs_u64(row, cs_trace::QUEUE_LEN, queue_len as u64);
                        insert_cs_u64(
                            row,
                            cs_trace::IN_FLIGHT_COUNT,
                            (checkpoint_in_flight.saturating_add(full_in_flight)) as u64,
                        );
                    },
                );
                if let Some(probe) = throughput_probe.clone() {
                    let pending = PendingBlockApply {
                        owner,
                        source,
                        token,
                        class,
                        block,
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
                pending_applies.push_back(PendingBlockApply {
                    owner,
                    source,
                    token,
                    class,
                    block,
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
    owner: zakura_header_chain::WorkOwner,
    source: zakura_header_chain::SourceId,
    token: BlockApplyToken,
    block: &block::Block,
    trace: &ZakuraTrace,
) {
    let Some((height, expected_hash, result, event)) =
        abandoned_block_apply_finished_event(owner, source, token, block)
    else {
        warn!(
            expected_hash = ?block.hash(),
            "dropping abandoned Zakura block-sync body without coinbase height"
        );
        return;
    };

    let _ = block_sync.send_control(event);
    emit_commit_state(
        trace,
        cs_trace::REACTOR_EVENT_SENT,
        "block_sync_driver",
        |row| {
            insert_cs_str(row, cs_trace::ACTION, "block_apply_finished");
            insert_cs_u64(row, cs_trace::APPLY_TOKEN, token);
            insert_cs_height(row, cs_trace::HEIGHT, height);
            insert_cs_hash(row, cs_trace::HASH, expected_hash);
            insert_cs_str(row, cs_trace::RESULT, block_apply_result_label(result));
        },
    );
}

pub(crate) fn abandoned_block_apply_finished_event(
    owner: zakura_header_chain::WorkOwner,
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
    while let Some(pending) = pending_applies.pop_front() {
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
    handoff: &std::sync::Arc<super::BlockSyncHandoff>,
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
    handoff: &std::sync::Arc<super::BlockSyncHandoff>,
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
    if handoff.is_yielded_to_legacy() {
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
        let Some(permit) = handoff.begin_apply() else {
            decrement_in_flight_apply_count(class, checkpoint_in_flight, full_in_flight);
            pending_applies.push_front(pending);
            return;
        };
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
                // Hold the gate slot for the whole apply, so fallback observes
                // this work until it has finished.
                let _permit = permit;
                apply.await
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
        emit_commit_state(
            trace,
            cs_trace::REACTOR_EVENT_SENT,
            "block_sync_driver",
            |row| {
                insert_cs_str(row, cs_trace::ACTION, "block_apply_finished");
                insert_cs_u64(row, cs_trace::APPLY_TOKEN, token);
                insert_cs_height(row, cs_trace::HEIGHT, height);
                insert_cs_hash(row, cs_trace::HASH, expected_hash);
                insert_cs_str(row, cs_trace::RESULT, block_apply_result_label(result));
            },
        );
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
    owner: zakura_header_chain::WorkOwner,
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
        return BlockApplyCompletion { class };
    };

    emit_commit_state(&trace, cs_trace::COMMIT_START, "block_sync_driver", |row| {
        insert_cs_u64(row, cs_trace::APPLY_TOKEN, token);
        insert_cs_str(row, cs_trace::APPLY_CLASS, block_apply_class_label(class));
        insert_cs_height(row, cs_trace::HEIGHT, height);
        insert_cs_hash(row, cs_trace::HASH, expected_hash);
    });
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
    emit_commit_state(
        &trace,
        cs_trace::COMMIT_FINISH,
        "block_sync_driver",
        |row| {
            insert_cs_u64(row, cs_trace::APPLY_TOKEN, token);
            insert_cs_str(row, cs_trace::APPLY_CLASS, block_apply_class_label(class));
            insert_cs_height(row, cs_trace::HEIGHT, height);
            insert_cs_hash(row, cs_trace::HASH, expected_hash);
            insert_cs_str(row, cs_trace::RESULT, block_apply_result_label(result));
            insert_cs_u64(row, cs_trace::ELAPSED_MS, elapsed_ms(started));
        },
    );
    let _ = block_sync.send_control(BlockSyncEvent::BlockApplyFinished {
        owner,
        source,
        token,
        height,
        hash: expected_hash,
        outcome,
    });
    emit_commit_state(
        &trace,
        cs_trace::REACTOR_EVENT_SENT,
        "block_sync_driver",
        |row| {
            insert_cs_str(row, cs_trace::ACTION, "block_apply_finished");
            insert_cs_u64(row, cs_trace::APPLY_TOKEN, token);
            insert_cs_height(row, cs_trace::HEIGHT, height);
            insert_cs_hash(row, cs_trace::HASH, expected_hash);
            insert_cs_str(row, cs_trace::RESULT, block_apply_result_label(result));
        },
    );

    BlockApplyCompletion { class }
}

#[cfg(test)]
pub(crate) async fn commit_block_sync_body<BlockVerifier>(
    block_verifier: BlockVerifier,
    owner: zakura_header_chain::WorkOwner,
    source: zakura_header_chain::SourceId,
    block: Arc<block::Block>,
    class: BlockApplyClass,
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
    match class {
        BlockApplyClass::Checkpoint => {
            block_commit_outcome(owner, source, height, expected_hash, commit.await)
        }
        BlockApplyClass::Full => {
            match tokio::time::timeout(ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT, commit).await {
                Ok(outcome) => block_commit_outcome(owner, source, height, expected_hash, outcome),
                Err(_elapsed) => block_commit_timed_out(owner, source, height, expected_hash),
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn commit_block_sync_body_with_stall_trace<BlockVerifier>(
    block_verifier: BlockVerifier,
    block: Arc<block::Block>,
    class: BlockApplyClass,
    trace: &ZakuraTrace,
    owner: zakura_header_chain::WorkOwner,
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

    match class {
        BlockApplyClass::Checkpoint => {
            tokio::pin!(commit);
            tokio::select! {
                outcome = &mut commit => block_commit_outcome(owner, source, Some(height), expected_hash, outcome),
                _ = tokio::time::sleep(ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT) => {
                    emit_commit_state(
                        trace,
                        cs_trace::COMMIT_STALLED,
                        "block_sync_driver",
                        |row| {
                            insert_cs_u64(row, cs_trace::APPLY_TOKEN, token);
                            insert_cs_str(row, cs_trace::APPLY_CLASS, block_apply_class_label(class));
                            insert_cs_height(row, cs_trace::HEIGHT, height);
                            insert_cs_hash(row, cs_trace::HASH, expected_hash);
                            insert_cs_u64(
                                row,
                                cs_trace::ELAPSED_MS,
                                ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT.as_millis().try_into().unwrap_or(u64::MAX),
                            );
                        },
                    );
                    block_commit_outcome(owner, source, Some(height), expected_hash, commit.await)
                }
            }
        }
        BlockApplyClass::Full => {
            match tokio::time::timeout(ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT, commit).await {
                Ok(outcome) => {
                    block_commit_outcome(owner, source, Some(height), expected_hash, outcome)
                }
                Err(_elapsed) => block_commit_timed_out(owner, source, Some(height), expected_hash),
            }
        }
    }
}

fn block_commit_outcome<E>(
    owner: zakura_header_chain::WorkOwner,
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
                    retryable_body_outcome(owner, source, expected_hash, kind)
                }
            }
        }
    }
}

fn block_commit_timed_out(
    owner: zakura_header_chain::WorkOwner,
    source: zakura_header_chain::SourceId,
    height: Option<block::Height>,
    expected_hash: block::Hash,
) -> BlockApplyOutcome {
    warn!(
        ?height,
        ?expected_hash,
        "timed out committing Zakura block-sync body"
    );
    retryable_body_outcome(
        owner,
        source,
        expected_hash,
        zakura_header_chain::TransientBodyFailureKind::Timeout,
    )
}

fn retryable_body_outcome(
    owner: zakura_header_chain::WorkOwner,
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
    owner: zakura_header_chain::WorkOwner,
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
    owner: zakura_header_chain::WorkOwner,
    source: zakura_header_chain::SourceId,
    hash: block::Hash,
    detail: &[u8],
) -> zakura_header_chain::EvidenceId {
    let mut hasher = Sha256::new();
    hasher.update(b"zakura-body-apply-outcome-v1");
    hash_bytes(&mut hasher, kind);
    hasher.update(owner.state_version.get().to_le_bytes());
    hasher.update(owner.header_generation.get().to_le_bytes());
    match owner.verified_generation {
        Some(generation) => {
            hasher.update([1]);
            hasher.update(generation.get().to_le_bytes());
        }
        None => hasher.update([0]),
    }
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
) -> Result<Vec<BlockSyncBlockMeta>, zakura_state::BoxError>
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
        return Ok(Vec::new());
    }

    let mut needed = Vec::new();
    let mut next_from = from;
    let mut remaining = limit;

    while remaining > 0 {
        let chunk_limit = remaining.min(zakura_state::constants::MAX_HEADER_SYNC_HEIGHT_RANGE);
        needed.extend(
            query_block_sync_needed_blocks_chunk(read_state.clone(), next_from, chunk_limit)
                .await?,
        );

        remaining = remaining.saturating_sub(chunk_limit);
        let Some(after_chunk) = next_from.0.checked_add(chunk_limit).map(block::Height) else {
            break;
        };
        next_from = after_chunk;
    }

    Ok(needed)
}

async fn query_block_sync_needed_blocks_chunk<ReadState>(
    read_state: ReadState,
    from: block::Height,
    limit: u32,
) -> Result<Vec<BlockSyncBlockMeta>, zakura_state::BoxError>
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
            return Ok(Vec::new());
        }
        Ok(Err(error)) => return Err(error),
        Err(elapsed) => return Err(Box::new(elapsed)),
    };

    Ok(block_sync_needed_blocks_from_state(metadata))
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

fn trace_block_driver_action(trace: &ZakuraTrace, action: &BlockSyncAction) {
    emit_commit_state(
        trace,
        cs_trace::ACTION_RECEIVED,
        "block_sync_driver",
        |row| match action {
            BlockSyncAction::Misbehavior { peer, reason } => {
                insert_cs_str(row, cs_trace::ACTION, "misbehavior");
                insert_cs_peer(row, cs_trace::PEER, peer);
                insert_cs_str(row, cs_trace::REASON, block_sync_misbehavior_label(*reason));
            }
            BlockSyncAction::QueryNeededBlocks {
                from,
                limit,
                best_header_tip,
                ..
            } => {
                insert_cs_str(row, cs_trace::ACTION, "query_needed_blocks");
                insert_cs_height(row, cs_trace::RANGE_START, *from);
                insert_cs_u64(row, cs_trace::RANGE_COUNT, u64::from(*limit));
                insert_cs_height(row, cs_trace::BEST_HEADER_TIP, *best_header_tip);
            }
            BlockSyncAction::QueryBlocksByHeightRange { peer, start, count } => {
                insert_cs_str(row, cs_trace::ACTION, "query_blocks_by_height_range");
                insert_cs_peer(row, cs_trace::PEER, peer);
                insert_cs_height(row, cs_trace::RANGE_START, *start);
                insert_cs_u64(row, cs_trace::RANGE_COUNT, u64::from(*count));
            }
            BlockSyncAction::SubmitBlock { token, block, .. } => {
                insert_cs_str(row, cs_trace::ACTION, "submit_block");
                insert_cs_u64(row, cs_trace::APPLY_TOKEN, *token);
                insert_cs_hash(row, cs_trace::HASH, block.hash());
                if let Some(height) = block.coinbase_height() {
                    insert_cs_height(row, cs_trace::HEIGHT, height);
                }
            }
            BlockSyncAction::RecordBodyUnavailable { .. } => {
                insert_cs_str(row, cs_trace::ACTION, "record_body_unavailable");
            }
            BlockSyncAction::RecordBodyInvalid { .. } => {
                insert_cs_str(row, cs_trace::ACTION, "record_body_invalid");
            }
            BlockSyncAction::RestartBodyAvailability { .. } => {
                insert_cs_str(row, cs_trace::ACTION, "restart_body_availability");
            }
            BlockSyncAction::RetryBodyAvailability { .. } => {
                insert_cs_str(row, cs_trace::ACTION, "retry_body_availability");
            }
        },
    );
}

fn trace_block_range_error(
    trace: &ZakuraTrace,
    peer: &zakura_network::zakura::ZakuraPeerId,
    start: block::Height,
    count: u32,
    reason: &str,
    started: Instant,
) {
    emit_commit_state(
        trace,
        cs_trace::STATE_READ_ERROR,
        "block_sync_driver",
        |row| {
            insert_cs_str(row, cs_trace::ACTION, "query_blocks_by_height_range");
            insert_cs_peer(row, cs_trace::PEER, peer);
            insert_cs_height(row, cs_trace::RANGE_START, start);
            insert_cs_u64(row, cs_trace::RANGE_COUNT, u64::from(count));
            insert_cs_str(row, cs_trace::RESULT, "error");
            insert_cs_str(row, cs_trace::REASON, reason);
            insert_cs_u64(row, cs_trace::ELAPSED_MS, elapsed_ms(started));
        },
    );
}

fn trace_block_range_finished(
    trace: &ZakuraTrace,
    peer: &zakura_network::zakura::ZakuraPeerId,
    start: block::Height,
    requested_count: u32,
    returned_count: u32,
) {
    emit_commit_state(
        trace,
        cs_trace::REACTOR_EVENT_SENT,
        "block_sync_driver",
        |row| {
            insert_cs_str(row, cs_trace::ACTION, "block_range_response_finished");
            insert_cs_peer(row, cs_trace::PEER, peer);
            insert_cs_height(row, cs_trace::RANGE_START, start);
            insert_cs_u64(row, cs_trace::RANGE_COUNT, u64::from(returned_count));
            insert_cs_u64(row, "requested_count", u64::from(requested_count));
        },
    );
}

fn block_apply_class_label(class: BlockApplyClass) -> &'static str {
    match class {
        BlockApplyClass::Checkpoint => "checkpoint",
        BlockApplyClass::Full => "full",
    }
}

fn block_sync_misbehavior_label(reason: BlockSyncMisbehavior) -> &'static str {
    match reason {
        BlockSyncMisbehavior::MalformedMessage => "malformed_message",
        BlockSyncMisbehavior::UnsolicitedBlock => "unsolicited_block",
        BlockSyncMisbehavior::GetBlocksTooLong => "get_blocks_too_long",
        BlockSyncMisbehavior::GetBlocksSpam => "get_blocks_spam",
        BlockSyncMisbehavior::InvalidBlock => "invalid_block",
        BlockSyncMisbehavior::SizeMismatch => "size_mismatch",
        BlockSyncMisbehavior::InvalidStatus => "invalid_status",
        BlockSyncMisbehavior::UnsolicitedDone => "unsolicited_done",
        BlockSyncMisbehavior::RangeUnavailable => "range_unavailable",
        BlockSyncMisbehavior::StatusSpam => "status_spam",
    }
}

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    use zakura_chain::serialization::ZcashDeserializeInto;
    use zakura_test::vectors::{BLOCK_MAINNET_1_BYTES, BLOCK_MAINNET_2_BYTES};

    fn mainnet_block(bytes: &[u8]) -> Arc<block::Block> {
        Arc::new(bytes.zcash_deserialize_into().expect("block vector parses"))
    }

    fn test_owner() -> zakura_header_chain::WorkOwner {
        zakura_header_chain::WorkScope {
            state_version: zakura_header_chain::StateVersion::new(1),
            header_generation: zakura_header_chain::HeaderGeneration::new(1),
            verified_generation: Some(zakura_header_chain::VerifiedGeneration::new(1)),
            branch: zakura_header_chain::BranchId::new(block::Hash([0; 32]), block::Hash([1; 32])),
        }
        .bind(
            1,
            std::num::NonZeroU64::new(1).expect("test request ID is nonzero"),
        )
    }

    fn test_source() -> zakura_header_chain::SourceId {
        zakura_header_chain::SourceId::from_digest([2; 32])
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
            },
            PendingBlockApply {
                owner: test_owner(),
                source: test_source(),
                token: 12,
                class: BlockApplyClass::Full,
                block: block2,
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
