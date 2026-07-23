use std::{future::Future, time::Instant};

use color_eyre::eyre::{eyre, Report};
use tokio::{pin, select, sync::mpsc};
use tower::{Service, ServiceExt};
use tracing::{debug, warn};

use zakura_chain::{
    block::{self},
    chain_tip::ChainTip,
    parallel::commitment_aux::BlockCommitmentRoots,
};
use zakura_network::zakura::{
    commit_state_trace as cs_trace, BlockSyncFrontiers, Frontier, FrontierChange,
    FullStateFrontiers, HeaderSyncAction, HeaderSyncEvent, ZakuraEndpoint,
    ZakuraHeaderSyncDriverStartup, ZakuraTrace, DEFAULT_HS_RANGE,
};

#[cfg(test)]
use zakura_network::zakura::{BlockSyncEvent, BlockSyncHandle};

use super::{
    emit_commit_state, insert_cs_frontiers, insert_cs_hash, insert_cs_height, insert_cs_peer,
    insert_cs_str, insert_cs_u64, verified_block_tip_from_state,
};

pub(crate) async fn zakura_header_sync_driver_startup(
    read_state: zakura_state::ReadStateService,
    network: &zakura_chain::parameters::Network,
) -> Result<ZakuraHeaderSyncDriverStartup, Report> {
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
    let committed_snapshots = read_state.subscribe_header_chain_snapshots();
    let best_header_tip = root_covered_best_header_tip_or_verified(
        read_state,
        best_header_tip.unwrap_or(empty_state_tip),
        verified_block_tip,
    )
    .await?;

    Ok(ZakuraHeaderSyncDriverStartup {
        frontiers: FullStateFrontiers {
            finalized_height,
            verified_block_tip: verified_block_tip.0,
            verified_block_hash: verified_block_tip.1,
        },
        best_header_tip: Some(best_header_tip),
        verified_block_tip_hash: verified_block_tip.1,
        committed_snapshots: Some(committed_snapshots),
    })
}

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

#[derive(Clone)]
pub(crate) struct ZakuraHeaderSyncDriverHandles {
    pub(crate) endpoint: ZakuraEndpoint,
    pub(crate) header_sync: zakura_network::zakura::HeaderSyncHandle,
}

pub(crate) async fn drive_zakura_header_sync_actions<State, ReadState, BlockVerifier>(
    mut actions: mpsc::Receiver<HeaderSyncAction>,
    handles: ZakuraHeaderSyncDriverHandles,
    _state: State,
    read_state: ReadState,
    _block_verifier: BlockVerifier,
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
    loop {
        let action = select! {
            _ = &mut shutdown => return,
            action = actions.recv() => {
                let Some(action) = action else {
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
            HeaderSyncAction::QueryHeaderLocator {
                peer,
                session_id,
                target_tip_hash,
            } => {
                let locator = match read_state
                    .clone()
                    .oneshot(zakura_state::ReadRequest::HeaderLocator)
                    .await
                {
                    Ok(zakura_state::ReadResponse::HeaderLocator(locator)) => locator,
                    Ok(response) => {
                        warn!(?peer, ?response, "unexpected HeaderLocator response");
                        None
                    }
                    Err(error) => {
                        warn!(?peer, ?error, "failed to query exact header locator");
                        None
                    }
                };
                let _ = handles
                    .header_sync
                    .send(HeaderSyncEvent::HeaderLocatorReady {
                        peer,
                        session_id,
                        target_tip_hash,
                        locator,
                    })
                    .await;
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
            HeaderSyncAction::QueryHeaderLocator {
                peer,
                target_tip_hash,
                ..
            } => {
                insert_cs_str(row, cs_trace::ACTION, "query_header_locator");
                insert_cs_peer(row, cs_trace::PEER, peer);
                insert_cs_hash(row, cs_trace::HASH, *target_tip_hash);
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

fn header_misbehavior_label(reason: zakura_network::zakura::HeaderSyncMisbehavior) -> &'static str {
    match reason {
        zakura_network::zakura::HeaderSyncMisbehavior::MalformedMessage => "malformed_message",
        zakura_network::zakura::HeaderSyncMisbehavior::InvalidHeader => "invalid_header",
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
