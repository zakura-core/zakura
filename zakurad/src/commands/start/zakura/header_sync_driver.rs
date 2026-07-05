use std::{future::Future, time::Instant};

use color_eyre::eyre::{eyre, Report};
use tokio::{pin, select, sync::mpsc};
use tower::{Service, ServiceExt};
use tracing::{debug, info, warn};

use zakura_chain::{
    block::{self},
    chain_tip::ChainTip,
    parallel::commitment_aux::BlockCommitmentRoots,
};
use zakura_network::zakura::{
    commit_state_trace as cs_trace, BlockSyncFrontiers, Frontier, FrontierChange, HeaderSyncAction,
    HeaderSyncCommitFailureKind, HeaderSyncEvent, HeaderSyncFrontiers, ZakuraEndpoint,
    ZakuraHeaderSyncDriverStartup, ZakuraTrace, DEFAULT_HS_RANGE,
};

#[cfg(test)]
use zakura_network::zakura::{BlockSyncEvent, BlockSyncHandle};

use super::{
    block_verify_error_is_duplicate, emit_commit_state, insert_cs_frontiers, insert_cs_hash,
    insert_cs_height, insert_cs_peer, insert_cs_str, insert_cs_u64, verified_block_tip_from_state,
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
    let durable_best_header_tip = best_header_tip.unwrap_or(empty_state_tip);

    // Rebuild the ZIP-221 history tree so post-Heartwood header-sync root verification can resume
    // immediately after a restart. The read returns the tree positioned at the highest *contiguous*
    // confirmed header-root frontier, and that frontier's `(height, hash)` — the single authoritative
    // value we use both to anchor overlap and to pick the resume height.
    let (best_header_history_tree, (frontier_height, frontier_hash)) = match read_state
        .clone()
        .oneshot(zakura_state::ReadRequest::BestHeaderHistoryTree {
            verified_block_tip: verified_block_tip.0,
            best_header_tip: durable_best_header_tip.0,
        })
        .await
        .map_err(|error| eyre!("{error}"))?
    {
        zakura_state::ReadResponse::BestHeaderHistoryTree { tree, frontier } => (tree, frontier),
        response => Err(eyre!(
            "unexpected BestHeaderHistoryTree response: {response:?}"
        ))?,
    };

    // Resume one block above the contiguous frontier (where the reconstructed tree sits), anchored at
    // it, so the first forward range re-validates from there. If the persisted roots have a one-block
    // gap (a header-tip advance that never overlapped), this resumes from the gap instead of capping
    // all the way back to the verified tip. With no header lead there is nothing to resume, so the
    // durable tip is kept and no overlap anchor is set.
    let (best_header_tip, best_header_parent_hash) =
        if durable_best_header_tip.0 > verified_block_tip.0 {
            let resume_height = frontier_height
                .next()
                .map_err(|_| eyre!("header frontier height overflow"))?;
            // The common (no-gap) frontier is one below the durable tip, so its successor is the durable
            // tip and we already hold that hash; only a gap needs a lookup of the durable resume header.
            let resume_hash = if resume_height == durable_best_header_tip.0 {
                durable_best_header_tip.1
            } else {
                match read_state
                    .oneshot(zakura_state::ReadRequest::HeadersByHeightRange {
                        start: resume_height,
                        count: 1,
                    })
                    .await
                    .map_err(|error| eyre!("{error}"))?
                {
                    zakura_state::ReadResponse::Headers(headers) => headers
                        .first()
                        .map(|(_height, hash, _header)| *hash)
                        .ok_or_else(|| {
                            eyre!("missing durable header at resume height {resume_height:?}")
                        })?,
                    response => Err(eyre!(
                        "unexpected HeadersByHeightRange response: {response:?}"
                    ))?,
                }
            };
            ((resume_height, resume_hash), Some(frontier_hash))
        } else {
            (durable_best_header_tip, None)
        };

    info!(
        verified = ?verified_block_tip.0,
        durable_header_tip = ?durable_best_header_tip.0,
        frontier = ?frontier_height,
        resume_tip = ?best_header_tip.0,
        overlap = best_header_parent_hash.is_some(),
        "Zakura header-sync startup: reconstructed history tree at frontier, resuming header sync"
    );

    Ok(ZakuraHeaderSyncDriverStartup {
        frontiers: HeaderSyncFrontiers {
            finalized_height,
            verified_block_tip: verified_block_tip.0,
            verified_block_hash: verified_block_tip.1,
        },
        best_header_tip: Some(best_header_tip),
        best_header_parent_hash,
        best_header_history_tree,
        verified_block_tip_hash: verified_block_tip.1,
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
                    Ok(zakura_state::ReadResponse::Headers(mut headers)) => {
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
                        // The header store leads the roots CF by one block: a committed range's tip
                        // root is only persisted once the next range confirms it (the one-block
                        // confirmation lag). When roots are requested, serve only the contiguous
                        // prefix that has roots so the response never carries a header without its
                        // root — the requester rejects a root-count mismatch wholesale, which would
                        // otherwise make us unservable for any range reaching our header tip. The
                        // requester re-fetches the tip through its own overlapping forward range.
                        if want_tree_aux_roots && block_roots.len() < headers.len() {
                            headers.truncate(block_roots.len());
                        }
                        let header_heights: Vec<_> =
                            headers.iter().map(|(height, _, _)| *height).collect();
                        let tree_aux_roots = if want_tree_aux_roots {
                            tree_aux_roots_for_served_header_range(
                                start,
                                header_heights.iter().copied(),
                                &block_roots,
                            )
                            .unwrap_or_else(|error| {
                                debug!(
                                    ?peer,
                                    ?start,
                                    requested_count = count,
                                    ?error,
                                    "serving header range without tree aux roots"
                                );

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
                                start_height: start,
                                requested_count: count,
                                returned_count: 0,
                            })
                            .await;
                    }
                }
            }
            HeaderSyncAction::CommitHeaderRange {
                peer,
                anchor,
                start_height,
                headers,
                body_sizes,
                verified_roots,
                finalized: _finalized,
            } => {
                let count = u32::try_from(headers.len()).unwrap_or(u32::MAX);
                // Persist only the header-authenticated confirmed prefix. The range tip's root is
                // unconfirmed until the next overlapping range delivers its successor header, so it
                // is deliberately excluded here; the state writes exactly what it is given.
                let committed_roots = verified_roots
                    .as_deref()
                    .map_or_else(Vec::new, |verified_roots| {
                        verified_roots.confirmed_roots().to_vec()
                    });
                let tree_aux_roots_len = u32::try_from(committed_roots.len()).unwrap_or(u32::MAX);
                let tip_parent_hash = header_range_tip_parent_hash(anchor, start_height, &headers);
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
                        tree_aux_roots: committed_roots,
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
                            .send(HeaderSyncEvent::HeaderRangeCommitted {
                                start_height,
                                tip_height,
                                tip_hash,
                                tip_parent_hash,
                            })
                            .await;
                        trace_header_reactor_event(
                            &trace,
                            "header_range_committed",
                            None,
                            tip_height,
                            tip_hash,
                            count,
                        );
                        publish_header_frontier(
                            &handles.endpoint,
                            tip_height,
                            tip_hash,
                            FrontierChange::HeaderAdvanced,
                            &trace,
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
                            .send(HeaderSyncEvent::HeaderRangeCommitFailed {
                                peer,
                                start_height,
                                count,
                                kind: HeaderSyncCommitFailureKind::Local,
                            })
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
                            .send(HeaderSyncEvent::HeaderRangeCommitFailed {
                                peer,
                                start_height,
                                count,
                                kind,
                            })
                            .await;
                    }
                }
            }
            HeaderSyncAction::QueryBestHeaderHistoryTree {
                verified_block_tip,
                best_header_tip,
            } => {
                // Reconstruct the header-frontier tree at the current frontier from durable roots.
                // Always report completion back (`Some` on success, `None` on failure) so the reactor
                // clears its in-flight rebuild guard even when the read errors — otherwise the guard
                // would wedge and suppress all future rebuilds, stranding the stale tree.
                let history_tree = match read_state
                    .clone()
                    .oneshot(zakura_state::ReadRequest::BestHeaderHistoryTree {
                        verified_block_tip,
                        best_header_tip,
                    })
                    .await
                {
                    Ok(zakura_state::ReadResponse::BestHeaderHistoryTree { tree, .. }) => Some(tree),
                    Ok(response) => {
                        warn!(?response, "unexpected BestHeaderHistoryTree response");
                        None
                    }
                    Err(error) => {
                        warn!(?error, "failed to rebuild Zakura best header history tree");
                        None
                    }
                };
                let _ = handles
                    .header_sync
                    .send(HeaderSyncEvent::BestHeaderHistoryTreeLoaded {
                        best_header_tip,
                        history_tree,
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
            Ok(zakura_state::ReadResponse::Block(Some(block))) => {
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
                    .send(HeaderSyncEvent::FullBlockCommitted {
                        height,
                        hash,
                        header: block.header.clone(),
                    })
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
                peer,
                start_height,
                headers,
                ..
            } => {
                insert_cs_str(row, cs_trace::ACTION, "commit_header_range");
                insert_cs_peer(row, cs_trace::PEER, peer);
                insert_cs_height(row, cs_trace::RANGE_START, *start_height);
                insert_cs_u64(row, cs_trace::RANGE_COUNT, headers.len() as u64);
            }
            HeaderSyncAction::QueryHeadersByHeightRange {
                peer, start, count, ..
            } => {
                insert_cs_str(row, cs_trace::ACTION, "query_headers_by_height_range");
                insert_cs_peer(row, cs_trace::PEER, peer);
                insert_cs_height(row, cs_trace::RANGE_START, *start);
                insert_cs_u64(row, cs_trace::RANGE_COUNT, u64::from(*count));
            }
            HeaderSyncAction::QueryBestHeaderHistoryTree {
                verified_block_tip,
                best_header_tip,
            } => {
                insert_cs_str(row, cs_trace::ACTION, "query_best_header_history_tree");
                insert_cs_height(row, cs_trace::VERIFIED_BLOCK_TIP, *verified_block_tip);
                insert_cs_height(row, cs_trace::BEST_HEADER_TIP, *best_header_tip);
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

fn header_range_tip_parent_hash(
    anchor: block::Hash,
    start_height: block::Height,
    headers: &[std::sync::Arc<block::Header>],
) -> Option<block::Hash> {
    if headers.is_empty() {
        return None;
    }

    if headers.len() == 1 {
        return (start_height > block::Height(0)).then_some(anchor);
    }

    headers
        .get(headers.len().saturating_sub(2))
        .map(|header| block::Hash::from(header.as_ref()))
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
