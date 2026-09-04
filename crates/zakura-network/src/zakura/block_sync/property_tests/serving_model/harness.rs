//! Adapter between abstract serving histories and the real block-sync stack.
//!
//! Operations enter through `BlockSyncService`, framed peer streams, or the
//! public driver handle. Observations come from real reactor actions, outbound
//! frames, peer snapshots, and cancellation tokens. Only expected behavior is
//! computed by the reference model.

use std::{collections::BTreeMap, fmt::Display, future::Future, sync::Arc};

use tokio::{
    runtime::Builder,
    sync::{mpsc, watch},
    time::{timeout, Duration},
};
use zakura_chain::block;

use super::super::super::super::{
    spawn_block_sync_reactor, BlockRangeRequestId, BlockSyncAction, BlockSyncEvent,
    BlockSyncFrontiers, BlockSyncHandle, BlockSyncMessage, BlockSyncStartup, BlockSyncStatus,
    ZakuraBlockSyncConfig,
};
use super::{
    model::{ReadyBlock, ReferenceModel},
    ByteCap, CompletionKind, ExpectedAction, ExpectedObservation, PendingQuery, ServingAction,
    ServingCase, ServingCoverage, ServingFrame, ServingObservation, ServingOp, ServingStep,
    SessionKey, StatusValidity,
};
use crate::zakura::{
    testkit::{
        SyntheticBlockCorpus, SyntheticBlockShape, SyntheticBlockSyncPeer, SyntheticBlockSyncPeers,
    },
    ServicePeerDirection, ServicePeerLimits, ServicePeerSnapshot, ZakuraPeerId,
};

const PEER_QUEUE_DEPTH: usize = 1_024;
const CANCEL_TEARDOWN_TIMEOUT: Duration = Duration::from_secs(1);
const SETTLEMENT_BARRIER_TIMEOUT: Duration = Duration::from_secs(1);

/// Run one materialized case in an isolated paused runtime.
///
/// A fresh runtime also prevents tasks or virtual time from leaking between
/// proptest cases and makes repeated execution of the same case deterministic.
pub(super) fn replay_serving_case(case: &ServingCase) -> Result<ServingCoverage, String> {
    Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .map_err(|error| format!("failed to build serving-model runtime: {error}"))?
        .block_on(replay_serving_case_inner(case))
}

/// Assemble the real service, peer harness, corpus, and reference model, then
/// compare their behavior until every step has settled.
async fn replay_serving_case_inner(case: &ServingCase) -> Result<ServingCoverage, String> {
    let target_block_bytes = match case.byte_cap {
        ByteCap::All => None,
        ByteCap::ExactlyFirst | ByteCap::ExactlyFirstTwo => Some(
            usize::try_from(block::MAX_BLOCK_BYTES / 2 + 64 * 1024)
                .expect("the synthetic block target fits usize"),
        ),
    };
    let corpus = SyntheticBlockCorpus::generate(
        case.tip,
        case.corpus_seed,
        SyntheticBlockShape { target_block_bytes },
    );
    let first_size = corpus
        .size_at(block::Height(1))
        .and_then(|size| u32::try_from(size).ok())
        .ok_or_else(|| "synthetic block 1 size must fit u32".to_string())?;
    let second_size = corpus
        .size_at(block::Height(2))
        .and_then(|size| u32::try_from(size).ok())
        .ok_or_else(|| "synthetic block 2 size must fit u32".to_string())?;
    if !matches!(case.byte_cap, ByteCap::All)
        && u64::from(first_size).saturating_add(u64::from(second_size)) <= block::MAX_BLOCK_BYTES
    {
        return Err("byte-boundary corpus must exceed one maximum block across two bodies".into());
    }
    let response_byte_cap = case.byte_cap.resolve(first_size, second_size);

    let (max_inbound_peers, max_outbound_peers) = match case.direction {
        ServicePeerDirection::Inbound => (case.max_peers, 0),
        ServicePeerDirection::Outbound => (0, case.max_peers),
    };
    let peer_limits = ServicePeerLimits {
        max_inbound_peers,
        max_outbound_peers,
        ..ServicePeerLimits::default()
    };
    let config = ZakuraBlockSyncConfig {
        max_blocks_per_response: case.max_blocks,
        max_inflight_requests: case.max_inflight,
        max_response_bytes: response_byte_cap,
        peer_limits,
        ..ZakuraBlockSyncConfig::default()
    };
    let tip = (corpus.target_height(), corpus.tip_hash());
    let (_tip_tx, tip_rx) = watch::channel(tip);
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: tip.0,
            verified_block_tip: tip.0,
            verified_block_hash: tip.1,
        },
        tip,
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) = spawn_block_sync_reactor(startup);
    let peers = SyntheticBlockSyncPeers::new(config, handle.clone(), PEER_QUEUE_DEPTH);
    let mut sessions = BTreeMap::new();
    let mut model = ReferenceModel::new(
        case.max_inflight,
        case.max_blocks,
        response_byte_cap,
        case.direction,
        case.max_peers,
        tip.0,
    );

    settle_runtime(&handle, &sessions).await?;
    let startup_observation = observe(&mut sessions, &mut actions, handle.peer_snapshot()).await?;
    if !startup_observation.actions.is_empty() || !startup_observation.frames.is_empty() {
        reactor_task.abort();
        return Err(format!(
            "unexpected externally visible startup output: {startup_observation:?}"
        ));
    }

    let steps = case.steps_with_prelude();
    let result = replay_steps(
        case,
        &steps,
        &corpus,
        &peers,
        &handle,
        &mut actions,
        &mut sessions,
        &mut model,
    )
    .await;
    if reactor_task.is_finished() && result.is_ok() {
        let outcome = reactor_task.await;
        return Err(format!(
            "block-sync reactor stopped during replay: {outcome:?}"
        ));
    }
    reactor_task.abort();
    let _ = reactor_task.await;
    result.map(|()| model.into_coverage())
}

#[allow(clippy::too_many_arguments)]
/// Replay each step with indexed failure context so a shrunk history is useful
/// without additional instrumentation.
async fn replay_steps(
    case: &ServingCase,
    steps: &[ServingStep],
    corpus: &SyntheticBlockCorpus,
    peers: &SyntheticBlockSyncPeers,
    handle: &BlockSyncHandle,
    actions: &mut mpsc::Receiver<BlockSyncAction>,
    sessions: &mut BTreeMap<SessionKey, SyntheticBlockSyncPeer>,
    model: &mut ReferenceModel,
) -> Result<(), String> {
    for (step_index, step) in steps.iter().enumerate() {
        if !(1..=3).contains(&step.operations.len()) {
            return Err(format!(
                "serving step {step_index} has {} operations; expected 1..=3",
                step.operations.len()
            ));
        }
        model.record_step(step.operations.len());
        let result = replay_step(step, corpus, peers, handle, actions, sessions, model).await;
        if let Err(error) = result {
            return Err(format!(
                "GetBlocks serving-model failure at step {step_index}\ncase: {case:#?}\nsteps: {steps:#?}\nerror: {error}"
            ));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
/// Issue all operations in one step without yielding, then compare the single
/// settled observation with the combined model prediction.
async fn replay_step(
    step: &ServingStep,
    corpus: &SyntheticBlockCorpus,
    peers: &SyntheticBlockSyncPeers,
    handle: &BlockSyncHandle,
    actions: &mut mpsc::Receiver<BlockSyncAction>,
    sessions: &mut BTreeMap<SessionKey, SyntheticBlockSyncPeer>,
    model: &mut ReferenceModel,
) -> Result<(), String> {
    let mut expected = ExpectedObservation::default();
    let mut pending_queries = Vec::new();
    let mut connected_sessions = Vec::new();

    for operation in &step.operations {
        let issued = issue_operation(operation, corpus, peers, handle, sessions, model)?;
        expected.append(issued.expected);
        pending_queries.extend(issued.pending_query);
        connected_sessions.extend(issued.connected_session);
    }

    expected.required_status_sessions.extend(
        connected_sessions
            .into_iter()
            .filter(|session| model.session_is_current_and_admitted(*session)),
    );

    verify_observation(model, sessions, actions, handle, expected, pending_queries).await
}

/// Model output and deferred bindings produced while issuing one operation.
struct IssuedOperation {
    expected: ExpectedObservation,
    pending_query: Option<PendingQuery>,
    connected_session: Option<SessionKey>,
}

/// Apply one total model operation at the corresponding real system boundary.
///
/// This function deliberately does not wait. Scheduling overlap is created by
/// issuing every operation in the step before [`verify_observation`] settles.
fn issue_operation(
    operation: &ServingOp,
    corpus: &SyntheticBlockCorpus,
    peers: &SyntheticBlockSyncPeers,
    handle: &BlockSyncHandle,
    sessions: &mut BTreeMap<SessionKey, SyntheticBlockSyncPeer>,
    model: &mut ReferenceModel,
) -> Result<IssuedOperation, String> {
    let (expected, pending_query, connected_session) = match *operation {
        ServingOp::Connect { peer } => {
            let (session, _admitted) = model.connect(peer);
            let synthetic_peer = peers
                .connect_peer(
                    logical_peer(session.peer),
                    session.conn_id,
                    model.direction(),
                )
                .map_err(|error| format!("connect failed: {error}"))?;
            sessions.insert(session, synthetic_peer);
            (ExpectedObservation::default(), None, Some(session))
        }
        ServingOp::Disconnect { peer, which } => {
            if let Some(conn_id) = model.disconnect(peer, which) {
                peers.remove_peer(&logical_peer(peer), conn_id);
            }
            (ExpectedObservation::default(), None, None)
        }
        ServingOp::Cancel { peer } => {
            if let Some(session) = model.cancel(peer) {
                session_peer(sessions, session)?.cancel();
            }
            (ExpectedObservation::default(), None, None)
        }
        ServingOp::Status { peer, validity } => {
            let (session, expected) = model.status(peer, validity);
            if let Some(session) = session {
                let status = status_for(validity, corpus);
                session_peer(sessions, session)?
                    .try_send(BlockSyncMessage::Status(status))
                    .map_err(|error| format!("Status send failed: {error}"))?;
            }
            (expected, None, None)
        }
        ServingOp::GetBlocks { peer, start, count } => {
            let (session, expected, pending) = model.get_blocks(peer, start, count);
            if let Some(session) = session {
                session_peer(sessions, session)?
                    .try_send(BlockSyncMessage::GetBlocks {
                        start_height: block::Height(start),
                        count,
                    })
                    .map_err(|error| format!("GetBlocks send failed: {error}"))?;
            }
            (expected, pending, None)
        }
        ServingOp::Complete { query, kind } => {
            let Some(target) = model.completion_target(query) else {
                return Ok(IssuedOperation {
                    expected: ExpectedObservation::default(),
                    pending_query: None,
                    connected_session: None,
                });
            };
            let blocks = ready_blocks(corpus, target.start, target.requested, kind);
            let expected_blocks: Vec<_> = blocks
                .iter()
                .map(|(height, body, size)| ReadyBlock {
                    height: *height,
                    hash: body.hash(),
                    size: *size,
                })
                .collect();
            let expected = model.complete(&target, kind, &expected_blocks);
            let event = match kind {
                CompletionKind::Ready
                | CompletionKind::ReadyOverlong
                | CompletionKind::ReadyPrefix(_)
                | CompletionKind::ReadyWithGap => BlockSyncEvent::BlockRangeResponseReady {
                    request_id: target.request_id,
                    peer: logical_peer(target.peer),
                    start_height: target.start,
                    requested_count: target.requested,
                    blocks,
                },
                CompletionKind::FinishedUnavailable => BlockSyncEvent::BlockRangeResponseFinished {
                    request_id: target.request_id,
                    peer: logical_peer(target.peer),
                    start_height: target.start,
                    requested_count: target.requested,
                    returned_count: 0,
                },
            };
            handle
                .send_control(event)
                .map_err(|error| format!("completion event send failed: {error}"))?;
            (expected, None, None)
        }
    };

    Ok(IssuedOperation {
        expected,
        pending_query,
        connected_session,
    })
}

/// Settle the runtime, drain visible output, bind production request IDs, and
/// compare all modeled frames, actions, sessions, and invariants.
async fn verify_observation(
    model: &mut ReferenceModel,
    sessions: &mut BTreeMap<SessionKey, SyntheticBlockSyncPeer>,
    actions: &mut mpsc::Receiver<BlockSyncAction>,
    handle: &BlockSyncHandle,
    expected: ExpectedObservation,
    pending_queries: Vec<PendingQuery>,
) -> Result<(), String> {
    settle_runtime(handle, sessions).await?;
    wait_for_cancelled_session_teardown(handle, sessions, model.snapshot()).await?;
    let actual = observe(sessions, actions, handle.peer_snapshot()).await?;

    let request_ids = compare_actions(&expected.actions, &actual.actions)?;
    if let Some(session) = expected
        .required_status_sessions
        .iter()
        .find(|session| !actual.status_frames_by_session.contains_key(session))
    {
        return Err(format!(
            "admitted session {session:?} did not receive its initial Status; observed Status frames by session: {:#?}",
            actual.status_frames_by_session
        ));
    }
    if actual.frames != expected.frames {
        return Err(format!(
            "frame mismatch\nexpected: {:#?}\nactual: {:#?}",
            expected.frames, actual.frames
        ));
    }
    if actual.snapshot != model.snapshot() {
        return Err(format!(
            "peer accounting mismatch\nexpected: {:?}\nactual: {:?}",
            model.snapshot(),
            actual.snapshot
        ));
    }
    if actual.cancelled != model.cancellations() {
        return Err(format!(
            "GB-SM-01/GB-SM-02/GB-SM-12 session cancellation mismatch\nexpected: {:#?}\nactual: {:#?}",
            model.cancellations(),
            actual.cancelled
        ));
    }

    if pending_queries.len() != request_ids.len() {
        return Err(format!(
            "pending query count {} did not match observed request ID count {}",
            pending_queries.len(),
            request_ids.len()
        ));
    }
    for (pending, request_id) in pending_queries.into_iter().zip(request_ids) {
        model.bind_query(pending, request_id)?;
    }

    let coverage = model.coverage_mut();
    let status_frames = actual
        .status_frames_by_session
        .values()
        .copied()
        .fold(0u64, u64::saturating_add);
    coverage.status_frames = coverage.status_frames.saturating_add(status_frames);
    for frames in actual.frames.values() {
        for frame in frames {
            match frame {
                ServingFrame::Block(_) => {
                    coverage.blocks_observed = coverage.blocks_observed.saturating_add(1)
                }
                ServingFrame::BlocksDone { .. } | ServingFrame::RangeUnavailable { .. } => {
                    coverage.terminal_frames = coverage.terminal_frames.saturating_add(1)
                }
                ServingFrame::Unexpected(_) => {}
            }
        }
    }
    model.assert_invariants()?;
    model.commit_verified_step();
    Ok(())
}

/// Wait for token-driven routine teardown to reach the reactor before reading
/// the peer snapshot. Reactor barriers alone cannot order a disconnect event
/// that the canceled routine has not queued yet.
async fn wait_for_cancelled_session_teardown(
    handle: &BlockSyncHandle,
    sessions: &BTreeMap<SessionKey, SyntheticBlockSyncPeer>,
    expected_snapshot: ServicePeerSnapshot,
) -> Result<(), String> {
    if !sessions
        .values()
        .any(|peer| peer.cancel_token().is_cancelled())
    {
        return Ok(());
    }

    let mut snapshots = handle.subscribe_peer_snapshot();
    let wait_for_expected_snapshot = async {
        loop {
            let observed = *snapshots.borrow_and_update();
            if observed == expected_snapshot {
                return Ok::<(), &'static str>(());
            }
            snapshots
                .changed()
                .await
                .map_err(|_| "peer snapshot channel closed during canceled-session teardown")?;
        }
    };

    timeout(CANCEL_TEARDOWN_TIMEOUT, wait_for_expected_snapshot)
        .await
        .map_err(|_| {
            format!(
                "canceled-session teardown did not publish {expected_snapshot:?}; observed {:?}",
                *snapshots.borrow()
            )
        })??;

    await_settlement_barrier(
        "reactor canceled-session barrier",
        handle.barrier_for_test(),
    )
    .await
}

/// Compare ordered actions while extracting opaque request IDs from matching
/// production queries for later model binding.
fn compare_actions(
    expected: &[ExpectedAction],
    actual: &[ServingAction],
) -> Result<Vec<BlockRangeRequestId>, String> {
    if expected.len() != actual.len() {
        return Err(format!(
            "action mismatch\nexpected: {expected:#?}\nactual: {actual:#?}"
        ));
    }

    let mut request_ids = Vec::new();
    for (expected_action, actual_action) in expected.iter().zip(actual) {
        match (expected_action, actual_action) {
            (
                ExpectedAction::Query { peer, start, count },
                ServingAction::Query {
                    request_id,
                    peer: actual_peer,
                    start: actual_start,
                    count: actual_count,
                },
            ) if peer == actual_peer && start == actual_start && count == actual_count => {
                request_ids.push(*request_id);
            }
            (
                ExpectedAction::Misbehavior { peer, reason },
                ServingAction::Misbehavior {
                    peer: actual_peer,
                    reason: actual_reason,
                },
            ) if peer == actual_peer && reason == actual_reason => {}
            _ => {
                return Err(format!(
                    "action mismatch\nexpected: {expected:#?}\nactual: {actual:#?}"
                ));
            }
        }
    }
    Ok(request_ids)
}

/// Drain all currently available driver actions and node-to-peer frames into a
/// normalized observation, retaining each frame's owning session.
async fn observe(
    sessions: &mut BTreeMap<SessionKey, SyntheticBlockSyncPeer>,
    actions: &mut mpsc::Receiver<BlockSyncAction>,
    snapshot: crate::zakura::ServicePeerSnapshot,
) -> Result<ServingObservation, String> {
    let mut observation = ServingObservation {
        snapshot,
        ..ServingObservation::default()
    };

    for (session, peer) in sessions.iter() {
        observation
            .cancelled
            .insert(*session, peer.cancel_token().is_cancelled());
    }

    while let Ok(action) = actions.try_recv() {
        match action {
            BlockSyncAction::QueryBlocksByHeightRange {
                request_id,
                peer,
                start,
                count,
            } => observation.actions.push(ServingAction::Query {
                request_id,
                peer: peer_slot(&peer)?,
                start,
                count,
            }),
            BlockSyncAction::Misbehavior { peer, reason } => {
                observation.actions.push(ServingAction::Misbehavior {
                    peer: peer_slot(&peer)?,
                    reason,
                });
            }
            BlockSyncAction::QueryNeededBlocks { .. } => {}
            action => observation
                .actions
                .push(ServingAction::Unexpected(format!("{action:?}"))),
        }
    }

    for (session, peer) in sessions.iter_mut() {
        loop {
            let message = peer
                .recv_timeout(Duration::from_nanos(1))
                .await
                .map_err(|error| format!("outbound frame decode failed: {error}"))?;
            let Some(message) = message else {
                break;
            };
            match message {
                BlockSyncMessage::Status(_) => {
                    let count = observation
                        .status_frames_by_session
                        .entry(*session)
                        .or_default();
                    *count = count.saturating_add(1);
                }
                BlockSyncMessage::Block(body) => observation
                    .frames
                    .entry(*session)
                    .or_default()
                    .push(ServingFrame::Block(body.hash())),
                BlockSyncMessage::BlocksDone {
                    start_height,
                    returned,
                } => {
                    observation
                        .frames
                        .entry(*session)
                        .or_default()
                        .push(ServingFrame::BlocksDone {
                            start: start_height,
                            returned,
                        })
                }
                BlockSyncMessage::RangeUnavailable {
                    start_height,
                    count,
                } => observation.frames.entry(*session).or_default().push(
                    ServingFrame::RangeUnavailable {
                        start: start_height,
                        count,
                    },
                ),
                message => observation
                    .frames
                    .entry(*session)
                    .or_default()
                    .push(ServingFrame::Unexpected(message.message_type())),
            }
        }
    }

    Ok(observation)
}

/// Establish a deterministic happens-before boundary across every input path
/// used by the serving model.
async fn settle_runtime(
    handle: &BlockSyncHandle,
    sessions: &BTreeMap<SessionKey, SyntheticBlockSyncPeer>,
) -> Result<(), String> {
    await_settlement_barrier("reactor pre-frame barrier", handle.barrier_for_test()).await?;
    for (session, peer) in sessions {
        if peer.cancel_token().is_cancelled() {
            continue;
        }
        await_settlement_barrier(
            &format!("peer routine barrier for {session:?}"),
            peer.barrier_for_test(),
        )
        .await?;
    }
    await_settlement_barrier("reactor post-frame barrier", handle.barrier_for_test()).await
}

/// Bound one harness barrier so a scheduling regression produces a replayable
/// error instead of hanging the property-test process.
async fn await_settlement_barrier<E>(
    phase: &str,
    barrier: impl Future<Output = Result<(), E>>,
) -> Result<(), String>
where
    E: Display,
{
    match timeout(SETTLEMENT_BARRIER_TIMEOUT, barrier).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(format!("{phase} failed: {error}")),
        Err(_) => Err(format!(
            "{phase} timed out after {SETTLEMENT_BARRIER_TIMEOUT:?}"
        )),
    }
}

/// Build the synthetic state response selected by a completion operation.
fn ready_blocks(
    corpus: &SyntheticBlockCorpus,
    start: block::Height,
    requested: u32,
    kind: CompletionKind,
) -> Vec<(block::Height, Arc<block::Block>, usize)> {
    let offsets: Vec<_> = match kind {
        CompletionKind::Ready => (0..requested).collect(),
        CompletionKind::ReadyOverlong => (0..requested.saturating_add(1)).collect(),
        CompletionKind::ReadyPrefix(ordinal) => {
            let returned = u32::from(ordinal) % requested.saturating_add(1);
            (0..returned).collect()
        }
        CompletionKind::ReadyWithGap => {
            if requested > 1 {
                vec![0, 2]
            } else {
                vec![1]
            }
        }
        CompletionKind::FinishedUnavailable => Vec::new(),
    };
    offsets
        .into_iter()
        .filter_map(|offset| {
            let height = start.0.checked_add(offset).map(block::Height)?;
            Some((height, corpus.block_at(height)?, corpus.size_at(height)?))
        })
        .collect()
}

/// Build either a contract-valid Status or the modeled invalid-range class.
fn status_for(validity: StatusValidity, corpus: &SyntheticBlockCorpus) -> BlockSyncStatus {
    let (servable_low, servable_high) = match validity {
        StatusValidity::Valid => (block::Height(1), corpus.target_height()),
        StatusValidity::InvalidRange => (corpus.target_height(), block::Height(1)),
    };
    BlockSyncStatus {
        servable_low,
        servable_high,
        tip_hash: corpus.tip_hash(),
        max_blocks_per_response: 128,
        max_inflight_requests: 8,
        max_response_bytes: super::super::super::super::MAX_BS_RESPONSE_BYTES,
    }
}

/// Resolve a modeled session to the exact synthetic connection that owns it.
fn session_peer(
    sessions: &BTreeMap<SessionKey, SyntheticBlockSyncPeer>,
    session: SessionKey,
) -> Result<&SyntheticBlockSyncPeer, String> {
    sessions
        .get(&session)
        .ok_or_else(|| format!("model selected missing synthetic session {session:?}"))
}

/// Map a small model slot to a stable authenticated peer identity.
fn logical_peer(slot: u8) -> ZakuraPeerId {
    ZakuraPeerId::new(vec![0xa0u8.saturating_add(slot); 32])
        .expect("logical property-test peer IDs are bounded")
}

/// Map an observed authenticated identity back to its model slot.
fn peer_slot(peer: &ZakuraPeerId) -> Result<u8, String> {
    (0..super::LOGICAL_PEER_COUNT)
        .find(|slot| logical_peer(*slot) == *peer)
        .ok_or_else(|| format!("reactor emitted action for unknown peer {peer:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn settlement_barrier_timeout_names_the_stuck_phase() {
        let error = await_settlement_barrier(
            "stuck test barrier",
            std::future::pending::<Result<(), &'static str>>(),
        )
        .await
        .expect_err("a pending barrier must time out");

        assert_eq!(
            error, "stuck test barrier timed out after 1s",
            "timeout diagnostics identify the settlement phase"
        );
    }
}
