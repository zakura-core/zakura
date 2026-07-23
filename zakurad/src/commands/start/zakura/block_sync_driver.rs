use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use futures::{
    future::BoxFuture,
    stream::{FuturesUnordered, StreamExt},
    FutureExt,
};
use tokio::time::Instant as TokioInstant;
use tokio::{pin, select, sync::mpsc};
use tower::{Service, ServiceExt};
use tracing::{debug, warn};

use zakura_chain::{block, chain_tip::ChainTip};
use zakura_network::zakura::{
    commit_state_trace as cs_trace, BlockApplyResult, BlockApplyToken, BlockSizeEstimate,
    BlockSyncAction, BlockSyncBlockMeta, BlockSyncEvent, BlockSyncHandle, BlockSyncMisbehavior,
    Frontier, FrontierChange, ZakuraEndpoint, ZakuraTrace,
};

use crate::components::sync;

use super::{
    block_apply_result_label, block_verify_error_class, emit_commit_state, insert_cs_bool,
    insert_cs_frontiers, insert_cs_hash, insert_cs_height, insert_cs_peer, insert_cs_str,
    insert_cs_u64, query_block_sync_frontiers, BlocksyncThroughputProbe,
    ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT,
};

pub(crate) const ZAKURA_BLOCK_SYNC_CHECKPOINT_FRONTIER_REFRESH_INTERVAL: Duration =
    Duration::from_millis(200);
const ZAKURA_BLOCK_SYNC_CHECKPOINT_FRONTIER_REFRESH_ATTEMPTS: usize = 24;

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
    checkpoint_refresh_floor: Option<block::Height>,
}

#[derive(Clone, Debug, Default)]
struct CheckpointFrontierRefresh {
    highest_sent: Option<block::Height>,
    attempts_remaining: usize,
    next_attempt_at: Option<TokioInstant>,
}

impl CheckpointFrontierRefresh {
    fn observe_checkpoint_commit(&mut self, highest_observed_at_apply: block::Height) {
        self.highest_sent = Some(
            self.highest_sent
                .map(|height| height.max(highest_observed_at_apply))
                .unwrap_or(highest_observed_at_apply),
        );
        self.attempts_remaining = ZAKURA_BLOCK_SYNC_CHECKPOINT_FRONTIER_REFRESH_ATTEMPTS;
        if self.next_attempt_at.is_none() {
            self.next_attempt_at =
                Some(TokioInstant::now() + ZAKURA_BLOCK_SYNC_CHECKPOINT_FRONTIER_REFRESH_INTERVAL);
        }
    }

    fn next_attempt_at(&self) -> Option<TokioInstant> {
        (self.attempts_remaining > 0)
            .then_some(self.next_attempt_at)
            .flatten()
    }

    fn finish_attempt(&mut self, highest_sent: block::Height) {
        self.highest_sent = Some(highest_sent);
        self.attempts_remaining = self.attempts_remaining.saturating_sub(1);
        self.next_attempt_at = (self.attempts_remaining > 0).then_some(
            TokioInstant::now() + ZAKURA_BLOCK_SYNC_CHECKPOINT_FRONTIER_REFRESH_INTERVAL,
        );
    }
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
    let mut checkpoint_frontier_refresh = CheckpointFrontierRefresh::default();
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
                    &mut checkpoint_frontier_refresh,
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
                    &mut checkpoint_frontier_refresh,
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
                        &mut checkpoint_frontier_refresh,
                    );
                    continue;
                }
                _ = async {
                    match checkpoint_frontier_refresh.next_attempt_at() {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending().await,
                    }
                }, if checkpoint_frontier_refresh.next_attempt_at().is_some() => {
                    refresh_block_sync_frontiers_for_checkpoint_window(
                        read_state.clone(),
                        latest_chain_tip.clone(),
                        endpoint.clone(),
                        Some(block_sync.clone()),
                        trace.clone(),
                        &mut checkpoint_frontier_refresh,
                    ).await;
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
                            &mut checkpoint_frontier_refresh,
                        )
                        .await;
                    } else {
                        let completed = apply_probe_block_sync_body(
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
                        observe_block_apply_completion(completed, &mut checkpoint_frontier_refresh);
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
            insert_cs_bool(row, cs_trace::LOCAL_FRONTIER, false);
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
    let result = BlockApplyResult::TimedOut;

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
            result,
            local_frontier: None,
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
    checkpoint_frontier_refresh: &mut CheckpointFrontierRefresh,
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
    observe_block_apply_completion(completed, checkpoint_frontier_refresh);

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
                insert_cs_bool(row, cs_trace::LOCAL_FRONTIER, false);
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

fn observe_block_apply_completion(
    completed: BlockApplyCompletion,
    checkpoint_frontier_refresh: &mut CheckpointFrontierRefresh,
) {
    if let Some(highest_observed_at_apply) = completed.checkpoint_refresh_floor {
        checkpoint_frontier_refresh.observe_checkpoint_commit(highest_observed_at_apply);
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
    checkpoint_frontier_refresh: &mut CheckpointFrontierRefresh,
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
        let completed = apply_probe_block_sync_body(
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
        observe_block_apply_completion(completed, checkpoint_frontier_refresh);
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
    latest_chain_tip: impl ChainTip + Clone + Send + Sync + 'static,
    endpoint: Option<ZakuraEndpoint>,
    read_state: ReadState,
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
        return BlockApplyCompletion {
            class,
            checkpoint_refresh_floor: None,
        };
    };

    emit_commit_state(&trace, cs_trace::COMMIT_START, "block_sync_driver", |row| {
        insert_cs_u64(row, cs_trace::APPLY_TOKEN, token);
        insert_cs_str(row, cs_trace::APPLY_CLASS, block_apply_class_label(class));
        insert_cs_height(row, cs_trace::HEIGHT, height);
        insert_cs_hash(row, cs_trace::HASH, expected_hash);
    });
    let started = Instant::now();
    // Throughput-probe mode (debug only): skip consensus verify+commit and
    // advance an in-memory synthetic frontier instead, discarding the body. In
    // normal mode the frontier comes from re-reading committed state below.
    let (result, probe_frontier) = match throughput_probe.as_ref() {
        Some(probe) => probe.apply_block(block.as_ref()),
        None => (
            commit_block_sync_body_with_stall_trace(
                block_verifier.clone(),
                block,
                class,
                &trace,
                token,
                height,
                expected_hash,
            )
            .await,
            None,
        ),
    };
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
    emit_commit_state(
        &trace,
        cs_trace::FRONTIER_QUERY_START,
        "block_sync_driver",
        |row| {
            insert_cs_u64(row, cs_trace::APPLY_TOKEN, token);
            insert_cs_height(row, cs_trace::HEIGHT, height);
            insert_cs_hash(row, cs_trace::HASH, expected_hash);
        },
    );
    let local_frontier = match throughput_probe.as_ref() {
        Some(_) => probe_frontier,
        None => query_block_sync_frontiers(read_state.clone(), latest_chain_tip.clone()).await,
    };
    if let Some(frontiers) = local_frontier {
        let change =
            if result == BlockApplyResult::Committed || result == BlockApplyResult::Duplicate {
                FrontierChange::VerifiedGrow
            } else {
                FrontierChange::Snapshot
            };
        if class == BlockApplyClass::Full || change != FrontierChange::VerifiedGrow {
            publish_body_frontier(endpoint.as_ref(), frontiers, change);
        }
    }
    emit_commit_state(
        &trace,
        cs_trace::FRONTIER_QUERY_FINISH,
        "block_sync_driver",
        |row| {
            insert_cs_u64(row, cs_trace::APPLY_TOKEN, token);
            insert_cs_height(row, cs_trace::HEIGHT, height);
            insert_cs_hash(row, cs_trace::HASH, expected_hash);
            insert_cs_bool(row, cs_trace::LOCAL_FRONTIER, local_frontier.is_some());
            if let Some(frontiers) = &local_frontier {
                insert_cs_frontiers(row, frontiers);
            }
        },
    );

    let _ = block_sync.send_control(BlockSyncEvent::BlockApplyFinished {
        owner,
        source,
        token,
        height,
        hash: expected_hash,
        result,
        local_frontier,
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
            insert_cs_bool(row, cs_trace::LOCAL_FRONTIER, local_frontier.is_some());
        },
    );

    BlockApplyCompletion {
        class,
        // Probe applies never reach state, so a delayed refresh only adds state reads and makes
        // probe tests race the refresh timer.
        checkpoint_refresh_floor: (throughput_probe.is_none()
            && class == BlockApplyClass::Checkpoint
            && result == BlockApplyResult::Committed)
            .then(|| {
                local_frontier
                    .map(|frontiers| frontiers.verified_block_tip)
                    .unwrap_or_else(|| height.previous().unwrap_or(height))
            }),
    }
}

#[cfg(test)]
pub(crate) async fn commit_block_sync_body<BlockVerifier>(
    block_verifier: BlockVerifier,
    block: Arc<block::Block>,
    class: BlockApplyClass,
) -> BlockApplyResult
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
        BlockApplyClass::Checkpoint => block_commit_result(height, expected_hash, commit.await),
        BlockApplyClass::Full => {
            match tokio::time::timeout(ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT, commit).await {
                Ok(outcome) => block_commit_result(height, expected_hash, outcome),
                Err(_elapsed) => block_commit_timed_out(height, expected_hash),
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
    token: BlockApplyToken,
    height: block::Height,
    expected_hash: block::Hash,
) -> BlockApplyResult
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
                outcome = &mut commit => block_commit_result(Some(height), expected_hash, outcome),
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
                    block_commit_result(Some(height), expected_hash, commit.await)
                }
            }
        }
        BlockApplyClass::Full => {
            match tokio::time::timeout(ZAKURA_BLOCK_SYNC_DRIVER_TIMEOUT, commit).await {
                Ok(outcome) => block_commit_result(Some(height), expected_hash, outcome),
                Err(_elapsed) => block_commit_timed_out(Some(height), expected_hash),
            }
        }
    }
}

fn block_commit_result<E>(
    height: Option<block::Height>,
    expected_hash: block::Hash,
    outcome: Result<block::Hash, E>,
) -> BlockApplyResult
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
            BlockApplyResult::Committed
        }
        Ok(committed_hash) => {
            warn!(
                ?height,
                ?expected_hash,
                ?committed_hash,
                "Zakura block-sync verifier returned an unexpected hash"
            );
            BlockApplyResult::Rejected
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
                BodyVerificationClass::Duplicate => BlockApplyResult::Duplicate,
                BodyVerificationClass::PayloadMismatch(_)
                | BodyVerificationClass::ConsensusInvalid(_) => BlockApplyResult::Rejected,
                BodyVerificationClass::Retryable(_) => BlockApplyResult::Unavailable,
            }
        }
    }
}

fn block_commit_timed_out(
    height: Option<block::Height>,
    expected_hash: block::Hash,
) -> BlockApplyResult {
    warn!(
        ?height,
        ?expected_hash,
        "timed out committing Zakura block-sync body"
    );
    BlockApplyResult::TimedOut
}

async fn refresh_block_sync_frontiers_for_checkpoint_window<ReadState>(
    read_state: ReadState,
    latest_chain_tip: impl ChainTip + Clone + Send + Sync + 'static,
    endpoint: Option<ZakuraEndpoint>,
    block_sync: Option<BlockSyncHandle>,
    trace: ZakuraTrace,
    refresh: &mut CheckpointFrontierRefresh,
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
    let Some(mut highest_sent) = refresh.highest_sent else {
        return;
    };

    emit_commit_state(
        &trace,
        cs_trace::CHECKPOINT_REFRESH_ATTEMPT,
        "block_sync_driver",
        |row| {
            insert_cs_u64(row, "attempts_remaining", refresh.attempts_remaining as u64);
            insert_cs_height(row, cs_trace::VERIFIED_BLOCK_TIP, highest_sent);
        },
    );
    let Some(frontiers) =
        query_block_sync_frontiers(read_state.clone(), latest_chain_tip.clone()).await
    else {
        refresh.finish_attempt(highest_sent);
        return;
    };

    if frontiers.verified_block_tip <= highest_sent {
        refresh.finish_attempt(highest_sent);
        return;
    }

    highest_sent = frontiers.verified_block_tip;
    publish_body_frontier(endpoint.as_ref(), frontiers, FrontierChange::VerifiedGrow);
    if let Some(block_sync) = &block_sync {
        let _ = block_sync.send_control(BlockSyncEvent::ChainTipGrow(frontiers));
    }
    emit_commit_state(
        &trace,
        cs_trace::CHECKPOINT_REFRESH_SENT,
        "block_sync_driver",
        |row| {
            insert_cs_frontiers(row, &frontiers);
        },
    );
    refresh.finish_attempt(highest_sent);
}

fn publish_body_frontier(
    endpoint: Option<&ZakuraEndpoint>,
    frontiers: zakura_network::zakura::BlockSyncFrontiers,
    change: FrontierChange,
) {
    let Some(endpoint) = endpoint else {
        return;
    };
    let Some(mut update) = endpoint.current_sync_frontier() else {
        return;
    };
    if frontiers.finalized_height == frontiers.verified_block_tip {
        update.frontier.finalized =
            Frontier::new(frontiers.finalized_height, frontiers.verified_block_hash);
    }
    update.frontier.verified_body =
        Frontier::new(frontiers.verified_block_tip, frontiers.verified_block_hash);
    update.change = change;
    endpoint.publish_sync_frontier_from(update, "block_sync_driver");
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
            events[0],
            (
                height,
                hash,
                BlockApplyResult::TimedOut,
                BlockSyncEvent::BlockApplyFinished {
                    token: 11,
                    height: event_height,
                    hash: event_hash,
                    result: BlockApplyResult::TimedOut,
                    local_frontier: None,
                    ..
                },
            ) if height == block1_height
                && hash == block1_hash
                && event_height == block1_height
                && event_hash == block1_hash
        ));
        assert!(matches!(
            events[1],
            (
                height,
                hash,
                BlockApplyResult::TimedOut,
                BlockSyncEvent::BlockApplyFinished {
                    token: 12,
                    height: event_height,
                    hash: event_hash,
                    result: BlockApplyResult::TimedOut,
                    local_frontier: None,
                    ..
                },
            ) if height == block2_height
                && hash == block2_hash
                && event_height == block2_height
                && event_hash == block2_hash
        ));
    }
}
