use std::{future::Future, sync::Arc};

use color_eyre::eyre::{eyre, Report};
use sha2::{Digest, Sha256};
use tokio::{pin, select, sync::mpsc};
use tower::{Service, ServiceExt};
use tracing::{debug, warn};

use zakura_chain::{
    block::{self},
    parallel::commitment_aux::BlockCommitmentRoots,
};
use zakura_network::zakura::{
    commit_state_trace as cs_trace, FullStateFrontiers, HeaderEntry, HeaderPathLease,
    HeaderPathLeaseResult, HeaderPathPage, HeaderPathPageResult, HeaderSyncAction, HeaderSyncEvent,
    HeaderTargetAdmissionResult, HeaderTargetPreparationResult, HeadersOutcomeCode,
    ZakuraHeaderSyncDriverStartup, ZakuraPeerId, ZakuraTrace,
};

use super::{
    emit_commit_state, insert_cs_hash, insert_cs_height, insert_cs_peer, insert_cs_str,
    insert_cs_u64, verified_block_tip_from_state,
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
    let vct_root_repairs = read_state.subscribe_vct_root_repairs();
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
        committed_snapshots,
        vct_root_repairs: Some(vct_root_repairs),
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
    pub(crate) header_sync: zakura_network::zakura::HeaderSyncHandle,
}

pub(crate) async fn drive_zakura_header_sync_actions<State, ReadState, BlockVerifier>(
    mut actions: mpsc::Receiver<HeaderSyncAction>,
    handles: ZakuraHeaderSyncDriverHandles,
    state: State,
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
                scope,
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
                        scope,
                        locator,
                    })
                    .await;
            }
            HeaderSyncAction::QueryVctRepairContext { owner, height } => {
                let result = match read_state
                    .clone()
                    .oneshot(zakura_state::ReadRequest::VctRepairContext { owner, height })
                    .await
                {
                    Ok(zakura_state::ReadResponse::VctRepairContext(Some(context))) => {
                        zakura_network::zakura::VctRepairContextResult::Resolved(context)
                    }
                    Ok(zakura_state::ReadResponse::VctRepairContext(None)) => {
                        zakura_network::zakura::VctRepairContextResult::Stale
                    }
                    Ok(response) => {
                        warn!(?owner, ?response, "unexpected VctRepairContext response");
                        zakura_network::zakura::VctRepairContextResult::Unavailable
                    }
                    Err(error) => {
                        warn!(?owner, ?error, "failed to query exact VCT repair context");
                        zakura_network::zakura::VctRepairContextResult::Unavailable
                    }
                };
                let _ = handles
                    .header_sync
                    .send(HeaderSyncEvent::VctRepairContextReady { owner, result })
                    .await;
            }
            HeaderSyncAction::AcquireHeaderPath {
                peer,
                session_id,
                scope,
                request,
            } => {
                let result =
                    acquire_header_path(read_state.clone(), &peer, session_id, scope, &request)
                        .await;
                let _ = handles
                    .header_sync
                    .send(HeaderSyncEvent::HeaderPathLeaseReady {
                        peer,
                        session_id,
                        scope,
                        request,
                        result,
                    })
                    .await;
            }
            HeaderSyncAction::ReadHeaderPath {
                peer,
                session_id,
                lease_id,
                scope,
                request_id,
                target_tip_hash,
                after_hash,
                max_header_count,
            } => {
                let result = read_header_path(
                    read_state.clone(),
                    &peer,
                    session_id,
                    lease_id,
                    scope,
                    after_hash,
                    max_header_count,
                )
                .await;
                let _ = handles
                    .header_sync
                    .send(HeaderSyncEvent::HeaderPathPageReady {
                        peer,
                        session_id,
                        scope,
                        request_id,
                        target_tip_hash,
                        result,
                    })
                    .await;
            }
            HeaderSyncAction::ReleaseHeaderPath {
                peer,
                session_id,
                lease_id,
                scope,
            } => {
                release_header_path(read_state.clone(), &peer, session_id, lease_id, scope).await;
            }
            HeaderSyncAction::PrepareHeaderTarget {
                peer,
                source,
                network,
                owner,
                common_ancestor,
                target,
                entries,
            } => {
                let result = prepare_header_target(
                    read_state.clone(),
                    &peer,
                    source,
                    network,
                    owner,
                    common_ancestor,
                    target,
                    entries,
                    zakura_header_chain::TargetCompletion::TargetComplete { common_ancestor },
                )
                .await;
                let _ = handles
                    .header_sync
                    .send(HeaderSyncEvent::HeaderTargetPrepared {
                        peer,
                        source,
                        owner,
                        result,
                    })
                    .await;
            }
            HeaderSyncAction::PrepareVctRepair {
                peer,
                source,
                network,
                owner,
                context,
                entry,
            } => {
                let common_ancestor = context.locator.entries().first().copied();
                let result = match common_ancestor {
                    Some(common_ancestor) if context.locator.entries().len() == 1 => {
                        prepare_header_target(
                            read_state.clone(),
                            &peer,
                            source,
                            network,
                            owner,
                            common_ancestor,
                            context.target,
                            vec![entry],
                            zakura_header_chain::TargetCompletion::SelectedAuxiliaryRepair {
                                common_ancestor,
                                selected_target: context.target,
                            },
                        )
                        .await
                    }
                    _ => typed_preparation_failure(
                        zakura_header_chain::HeaderChainError::stale_target(
                            zakura_header_chain::ErrorSubject::Branch(owner.branch),
                        ),
                    ),
                };
                let _ = handles
                    .header_sync
                    .send(HeaderSyncEvent::VctRepairPrepared {
                        peer,
                        source,
                        owner,
                        result,
                    })
                    .await;
            }
            HeaderSyncAction::ApplyHeaderTarget {
                peer,
                source,
                owner,
                insert,
            } => {
                let result = apply_header_target(state.clone(), &peer, owner, insert).await;
                let _ = handles
                    .header_sync
                    .send(HeaderSyncEvent::HeaderTargetAdmissionReady {
                        peer,
                        source,
                        owner,
                        result,
                    })
                    .await;
            }
            HeaderSyncAction::ApplyVctRepair {
                peer,
                source,
                owner,
                insert,
            } => {
                let result = apply_header_target(state.clone(), &peer, owner, insert).await;
                let _ = handles
                    .header_sync
                    .send(HeaderSyncEvent::VctRepairAdmissionReady {
                        peer,
                        source,
                        owner,
                        result,
                    })
                    .await;
            }
        }
    }
}

fn typed_preparation_failure(
    error: zakura_header_chain::HeaderChainError,
) -> HeaderTargetPreparationResult {
    HeaderTargetPreparationResult::Failed(Arc::new(error))
}

fn typed_admission_failure(
    error: zakura_header_chain::HeaderChainError,
) -> HeaderTargetAdmissionResult {
    HeaderTargetAdmissionResult::Failed(Arc::new(error))
}

fn header_failure_evidence(
    source: zakura_header_chain::SourceId,
    owner: zakura_header_chain::WorkOwner,
    hash: block::Hash,
    rule: zakura_header_chain::RuleId,
) -> zakura_header_chain::EvidenceId {
    let mut hasher = Sha256::new();
    hasher.update(b"zakura-header-validation-failure-v1");
    hasher.update(source.digest());
    hasher.update(owner.session_id.to_le_bytes());
    hasher.update(owner.request_id.get().to_le_bytes());
    hasher.update(hash.0);
    hasher.update(rule.as_str().as_bytes());
    zakura_header_chain::EvidenceId::from_digest(hasher.finalize().into())
}

fn classify_header_preparation_failure(
    error: zakura_header_chain::HeaderFailure,
    entries: &[HeaderEntry],
    source: zakura_header_chain::SourceId,
    owner: zakura_header_chain::WorkOwner,
) -> zakura_header_chain::HeaderChainError {
    match error {
        zakura_header_chain::HeaderFailure::Invalid {
            offset,
            rule,
            reason,
        } => {
            let hash = entries
                .get(offset)
                .expect("the validation failure offset comes from this exact header batch")
                .header
                .hash();
            let rule_id = rule
                .rule_ids()
                .first()
                .copied()
                .expect("every validation stage has normative rule ownership");
            zakura_header_chain::HeaderChainError::invalid_header(
                zakura_header_chain::ErrorSubject::Header(zakura_header_chain::HeaderId::new(hash)),
                rule_id,
                header_failure_evidence(source, owner, hash, rule_id),
                source,
                Some(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    reason,
                ))),
            )
        }
        zakura_header_chain::HeaderFailure::Empty => {
            zakura_header_chain::HeaderChainError::malformed_protocol(
                zakura_header_chain::ErrorSubject::Request {
                    source,
                    request_id: owner.request_id,
                },
                zakura_header_chain::RuleId::new("LC-WIRE-08"),
                source,
                None,
            )
        }
        zakura_header_chain::HeaderFailure::InvalidLease => {
            zakura_header_chain::HeaderChainError::stale_target(
                zakura_header_chain::ErrorSubject::Branch(owner.branch),
            )
        }
        zakura_header_chain::HeaderFailure::ClockRange => {
            zakura_header_chain::HeaderChainError::local_resource(
                zakura_header_chain::ErrorSubject::Branch(owner.branch),
                Some(Box::new(zakura_header_chain::HeaderFailure::ClockRange)),
            )
        }
    }
}

fn classify_body_size_hint_failure(
    error: zakura_header_chain::TransitionTypeError,
    hash: block::Hash,
    source: zakura_header_chain::SourceId,
) -> zakura_header_chain::HeaderChainError {
    zakura_header_chain::HeaderChainError::malformed_protocol(
        zakura_header_chain::ErrorSubject::Header(zakura_header_chain::HeaderId::new(hash)),
        zakura_header_chain::RuleId::new("LC-WIRE-13"),
        source,
        Some(Box::new(error)),
    )
}

#[allow(clippy::too_many_arguments)]
async fn prepare_header_target<ReadState>(
    read_state: ReadState,
    peer: &ZakuraPeerId,
    source: zakura_header_chain::SourceId,
    network: zakura_chain::parameters::Network,
    owner: zakura_header_chain::WorkOwner,
    common_ancestor: zakura_header_chain::Frontier,
    target: zakura_header_chain::Frontier,
    entries: Vec<HeaderEntry>,
    completion: zakura_header_chain::TargetCompletion,
) -> HeaderTargetPreparationResult
where
    ReadState: Service<
            zakura_state::ReadRequest,
            Response = zakura_state::ReadResponse,
            Error = zakura_state::BoxError,
        > + Send
        + 'static,
    ReadState::Future: Send + 'static,
{
    let lease = match read_state
        .oneshot(zakura_state::ReadRequest::HeaderValidationLease {
            parent_hash: common_ancestor.hash,
        })
        .await
    {
        Ok(zakura_state::ReadResponse::HeaderValidationLease(Some(lease)))
            if lease.parent == common_ancestor =>
        {
            lease
        }
        Ok(zakura_state::ReadResponse::HeaderValidationLease(_)) => {
            return typed_preparation_failure(zakura_header_chain::HeaderChainError::stale_target(
                zakura_header_chain::ErrorSubject::Branch(owner.branch),
            ));
        }
        Ok(response) => {
            warn!(
                ?peer,
                ?response,
                "unexpected header validation lease response"
            );
            return typed_preparation_failure(
                zakura_header_chain::HeaderChainError::local_resource(
                    zakura_header_chain::ErrorSubject::Branch(owner.branch),
                    None,
                ),
            );
        }
        Err(error) => {
            warn!(?peer, ?error, "failed to acquire header validation lease");
            return typed_preparation_failure(
                zakura_header_chain::HeaderChainError::local_resource(
                    zakura_header_chain::ErrorSubject::Branch(owner.branch),
                    Some(error),
                ),
            );
        }
    };

    let prepared = tokio::task::spawn_blocking(move || {
        let rules = zakura_header_chain::HeaderRules::for_validation_lease(network, &lease)
            .map_err(|error| {
                typed_preparation_failure(zakura_header_chain::HeaderChainError::unknown_anchor(
                    zakura_header_chain::ErrorSubject::Branch(owner.branch),
                    Some(Box::new(error)),
                ))
            })?;
        let headers: Vec<_> = entries.iter().map(|entry| entry.header.clone()).collect();
        let batch = zakura_header_chain::prepare_headers(
            zakura_header_chain::HeaderBatchInput::new(&headers),
            &lease,
            &rules,
            &zakura_header_chain::SystemClock,
        )
        .map_err(|error| {
            typed_preparation_failure(classify_header_preparation_failure(
                error, &entries, source, owner,
            ))
        })?;
        let mut aux = Vec::with_capacity(entries.len());
        for (entry, prepared) in entries.iter().zip(batch.headers()) {
            let body_size =
                zakura_header_chain::BodySizeHint::new(entry.body_size).map_err(|error| {
                    typed_preparation_failure(classify_body_size_hint_failure(
                        error,
                        prepared.hash,
                        source,
                    ))
                })?;
            let mut hasher = Sha256::new();
            hasher.update(b"zakura-header-aux-delivery-v1");
            hasher.update(source.digest());
            hasher.update(owner.session_id.to_le_bytes());
            hasher.update(owner.request_id.get().to_le_bytes());
            hasher.update(prepared.hash.0);
            aux.push(zakura_header_chain::AuxDelivery {
                delivery_id: zakura_header_chain::EvidenceId::from_digest(hasher.finalize().into()),
                header_hash: prepared.hash,
                source,
                owner,
                body_size,
                tree_aux: entry.tree_aux,
                authentication: zakura_header_chain::AuxAuthentication::Unauthenticated,
            });
        }
        Ok::<_, HeaderTargetPreparationResult>((batch, aux))
    })
    .await;
    let (batch, aux) = match prepared {
        Ok(Ok(prepared)) => prepared,
        Ok(Err(result)) => return result,
        Err(error) => {
            warn!(?peer, ?error, "header target preparation task failed");
            return typed_preparation_failure(
                zakura_header_chain::HeaderChainError::local_resource(
                    zakura_header_chain::ErrorSubject::Branch(owner.branch),
                    Some(Box::new(error)),
                ),
            );
        }
    };

    HeaderTargetPreparationResult::Prepared(Box::new(zakura_header_chain::InsertHeaders {
        owner,
        source,
        parent_hash: common_ancestor.hash,
        target_tip_hash: target.hash,
        completion,
        batch,
        aux,
    }))
}

async fn apply_header_target<State>(
    state: State,
    peer: &ZakuraPeerId,
    owner: zakura_header_chain::WorkOwner,
    insert: Box<zakura_header_chain::InsertHeaders>,
) -> HeaderTargetAdmissionResult
where
    State: Service<
            zakura_state::Request,
            Response = zakura_state::Response,
            Error = zakura_state::BoxError,
        > + Send
        + 'static,
    State::Future: Send + 'static,
{
    match state
        .oneshot(zakura_state::Request::ApplyHeaderChainInsert {
            expected_version: owner.state_version,
            insert,
        })
        .await
    {
        Ok(zakura_state::Response::HeaderChainInsertApplied(
            zakura_header_chain::ApplyResult::Committed(_)
            | zakura_header_chain::ApplyResult::NoChange(_),
        )) => HeaderTargetAdmissionResult::Applied,
        Ok(zakura_state::Response::HeaderChainInsertApplied(
            zakura_header_chain::ApplyResult::Stale(_),
        )) => typed_admission_failure(zakura_header_chain::HeaderChainError::stale_target(
            zakura_header_chain::ErrorSubject::Branch(owner.branch),
        )),
        Ok(response) => {
            warn!(?peer, ?response, "unexpected header insertion response");
            typed_admission_failure(zakura_header_chain::HeaderChainError::local_resource(
                zakura_header_chain::ErrorSubject::Branch(owner.branch),
                None,
            ))
        }
        Err(error) => {
            warn!(?peer, ?error, "failed to atomically admit header target");
            typed_admission_failure(zakura_header_chain::HeaderChainError::local_resource(
                zakura_header_chain::ErrorSubject::Branch(owner.branch),
                Some(error),
            ))
        }
    }
}

async fn acquire_header_path<ReadState>(
    read_state: ReadState,
    peer: &ZakuraPeerId,
    session_id: u64,
    scope: zakura_header_chain::WorkScope,
    request: &zakura_network::zakura::GetHeaders,
) -> HeaderPathLeaseResult
where
    ReadState: Service<
            zakura_state::ReadRequest,
            Response = zakura_state::ReadResponse,
            Error = zakura_state::BoxError,
        > + Send
        + 'static,
    ReadState::Future: Send + 'static,
{
    let Some(source) = source_id(peer) else {
        return HeaderPathLeaseResult::Outcome(HeadersOutcomeCode::Busy);
    };
    match read_state
        .oneshot(zakura_state::ReadRequest::AcquireRetainedHeaderPath {
            peer: source,
            session_id,
            target_tip_hash: request.target_tip_hash,
            scope,
            locator_hashes: request.locator_hashes.clone(),
        })
        .await
    {
        Ok(zakura_state::ReadResponse::RetainedHeaderPathLease(outcome)) => match outcome {
            zakura_state::RetainedPathLeaseOutcome::Acquired(lease) => {
                HeaderPathLeaseResult::Acquired(HeaderPathLease {
                    lease_id: lease.lease_id,
                    common_ancestor: lease.common_ancestor,
                    target: lease.target,
                    scope: lease.scope,
                })
            }
            zakura_state::RetainedPathLeaseOutcome::TargetNotRetained => {
                HeaderPathLeaseResult::Outcome(HeadersOutcomeCode::TargetNotRetained)
            }
            zakura_state::RetainedPathLeaseOutcome::NoLocatorIntersection => {
                HeaderPathLeaseResult::Outcome(HeadersOutcomeCode::NoLocatorIntersection)
            }
            zakura_state::RetainedPathLeaseOutcome::HistoryPruned => {
                HeaderPathLeaseResult::Outcome(HeadersOutcomeCode::HistoryPruned)
            }
            zakura_state::RetainedPathLeaseOutcome::Busy => {
                HeaderPathLeaseResult::Outcome(HeadersOutcomeCode::Busy)
            }
        },
        Ok(response) => {
            warn!(
                ?peer,
                ?response,
                "unexpected retained header path lease response"
            );
            HeaderPathLeaseResult::Outcome(HeadersOutcomeCode::Busy)
        }
        Err(error) => {
            warn!(?peer, ?error, "failed to acquire retained header path");
            HeaderPathLeaseResult::Outcome(HeadersOutcomeCode::Busy)
        }
    }
}

async fn read_header_path<ReadState>(
    read_state: ReadState,
    peer: &ZakuraPeerId,
    session_id: u64,
    lease_id: u64,
    scope: zakura_header_chain::WorkScope,
    after_hash: block::Hash,
    max_header_count: u32,
) -> HeaderPathPageResult
where
    ReadState: Service<
            zakura_state::ReadRequest,
            Response = zakura_state::ReadResponse,
            Error = zakura_state::BoxError,
        > + Send
        + 'static,
    ReadState::Future: Send + 'static,
{
    let Some(source) = source_id(peer) else {
        return HeaderPathPageResult::Unavailable;
    };
    match read_state
        .oneshot(zakura_state::ReadRequest::ReadRetainedHeaderPath {
            peer: source,
            session_id,
            lease_id,
            scope,
            after_hash,
            max_count: max_header_count,
        })
        .await
    {
        Ok(zakura_state::ReadResponse::RetainedHeaderPathPage(
            zakura_state::RetainedPathReadOutcome::Page(page),
        )) => HeaderPathPageResult::Page(Box::new(HeaderPathPage {
            lease_id: page.lease_id,
            common_ancestor: page.common_ancestor,
            target: page.target,
            scope: page.scope,
            entries: page
                .nodes
                .into_iter()
                .map(|node| HeaderEntry {
                    header: node.header,
                    body_size: 0,
                    tree_aux: None,
                })
                .collect(),
            complete: page.complete,
        })),
        Ok(zakura_state::ReadResponse::RetainedHeaderPathPage(
            zakura_state::RetainedPathReadOutcome::Unavailable,
        )) => HeaderPathPageResult::Unavailable,
        Ok(response) => {
            warn!(
                ?peer,
                ?response,
                "unexpected retained header path page response"
            );
            HeaderPathPageResult::Unavailable
        }
        Err(error) => {
            warn!(?peer, ?error, "failed to read retained header path page");
            HeaderPathPageResult::Unavailable
        }
    }
}

async fn release_header_path<ReadState>(
    read_state: ReadState,
    peer: &ZakuraPeerId,
    session_id: u64,
    lease_id: u64,
    scope: zakura_header_chain::WorkScope,
) where
    ReadState: Service<
            zakura_state::ReadRequest,
            Response = zakura_state::ReadResponse,
            Error = zakura_state::BoxError,
        > + Send
        + 'static,
    ReadState::Future: Send + 'static,
{
    let Some(source) = source_id(peer) else {
        return;
    };
    if let Err(error) = read_state
        .oneshot(zakura_state::ReadRequest::ReleaseRetainedHeaderPath {
            peer: source,
            session_id,
            lease_id,
            scope,
        })
        .await
    {
        warn!(?peer, ?error, "failed to release retained header path");
    }
}

fn source_id(peer: &ZakuraPeerId) -> Option<zakura_header_chain::SourceId> {
    let digest = <[u8; 32]>::try_from(peer.as_bytes()).ok()?;
    Some(zakura_header_chain::SourceId::from_digest(digest))
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
            HeaderSyncAction::QueryVctRepairContext { owner, height } => {
                insert_cs_str(row, cs_trace::ACTION, "query_vct_repair_context");
                insert_cs_height(row, cs_trace::HEIGHT, *height);
                insert_cs_hash(row, cs_trace::HASH, owner.branch.target_tip_hash);
            }
            HeaderSyncAction::AcquireHeaderPath { peer, request, .. } => {
                insert_cs_str(row, cs_trace::ACTION, "acquire_header_path");
                insert_cs_peer(row, cs_trace::PEER, peer);
                insert_cs_hash(row, cs_trace::HASH, request.target_tip_hash);
            }
            HeaderSyncAction::ReadHeaderPath {
                peer,
                target_tip_hash,
                max_header_count,
                ..
            } => {
                insert_cs_str(row, cs_trace::ACTION, "read_header_path");
                insert_cs_peer(row, cs_trace::PEER, peer);
                insert_cs_hash(row, cs_trace::HASH, *target_tip_hash);
                insert_cs_u64(row, cs_trace::RANGE_COUNT, u64::from(*max_header_count));
            }
            HeaderSyncAction::ReleaseHeaderPath { peer, .. } => {
                insert_cs_str(row, cs_trace::ACTION, "release_header_path");
                insert_cs_peer(row, cs_trace::PEER, peer);
            }
            HeaderSyncAction::PrepareHeaderTarget { peer, target, .. } => {
                insert_cs_str(row, cs_trace::ACTION, "prepare_header_target");
                insert_cs_peer(row, cs_trace::PEER, peer);
                insert_cs_height(row, cs_trace::HEIGHT, target.height);
                insert_cs_hash(row, cs_trace::HASH, target.hash);
            }
            HeaderSyncAction::PrepareVctRepair { peer, context, .. } => {
                insert_cs_str(row, cs_trace::ACTION, "prepare_vct_repair");
                insert_cs_peer(row, cs_trace::PEER, peer);
                insert_cs_height(row, cs_trace::HEIGHT, context.target.height);
                insert_cs_hash(row, cs_trace::HASH, context.target.hash);
            }
            HeaderSyncAction::ApplyHeaderTarget { peer, insert, .. } => {
                insert_cs_str(row, cs_trace::ACTION, "apply_header_target");
                insert_cs_peer(row, cs_trace::PEER, peer);
                insert_cs_hash(row, cs_trace::HASH, insert.target_tip_hash);
            }
            HeaderSyncAction::ApplyVctRepair { peer, insert, .. } => {
                insert_cs_str(row, cs_trace::ACTION, "apply_vct_repair");
                insert_cs_peer(row, cs_trace::PEER, peer);
                insert_cs_hash(row, cs_trace::HASH, insert.target_tip_hash);
            }
            HeaderSyncAction::Misbehavior { peer, reason } => {
                insert_cs_str(row, cs_trace::ACTION, "misbehavior");
                insert_cs_peer(row, cs_trace::PEER, peer);
                insert_cs_str(row, cs_trace::REASON, header_misbehavior_label(*reason));
            }
        },
    );
}

fn header_misbehavior_label(reason: zakura_network::zakura::HeaderSyncMisbehavior) -> &'static str {
    match reason {
        zakura_network::zakura::HeaderSyncMisbehavior::MalformedMessage => "malformed_message",
        zakura_network::zakura::HeaderSyncMisbehavior::InvalidHeader => "invalid_header",
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use zakura_chain::block::genesis::regtest_genesis_block;

    use super::*;

    fn owner() -> zakura_header_chain::WorkOwner {
        zakura_header_chain::WorkScope {
            state_version: zakura_header_chain::StateVersion::new(1),
            header_generation: zakura_header_chain::HeaderGeneration::new(2),
            verified_generation: None,
            branch: zakura_header_chain::BranchId::new(block::Hash([1; 32]), block::Hash([2; 32])),
        }
        .bind(
            3,
            NonZeroU64::new(4).expect("the fixture request ID is nonzero"),
        )
    }

    #[test]
    fn driver_preserves_every_header_preparation_failure_category() {
        let source = zakura_header_chain::SourceId::from_digest([5; 32]);
        let owner = owner();
        let header = regtest_genesis_block().header.clone();
        let entries = [HeaderEntry {
            header: header.clone(),
            body_size: 0,
            tree_aux: None,
        }];

        let invalid = classify_header_preparation_failure(
            zakura_header_chain::HeaderFailure::Invalid {
                offset: 0,
                rule: zakura_header_chain::HeaderRule::ParentLink,
                reason: "wrong parent".to_owned(),
            },
            &entries,
            source,
            owner,
        );
        assert_eq!(
            invalid.category,
            zakura_header_chain::ErrorCategory::InvalidHeader
        );
        assert_eq!(
            invalid.subject,
            zakura_header_chain::ErrorSubject::Header(zakura_header_chain::HeaderId::new(
                header.hash()
            ))
        );
        assert_eq!(
            invalid.rule,
            Some(zakura_header_chain::RuleId::new("LC-VAL-03"))
        );
        assert!(invalid.evidence.is_some());
        assert_eq!(
            invalid.attribution,
            zakura_header_chain::Attribution::HeaderPeer(source)
        );

        for (failure, expected_category, expected_attribution) in [
            (
                zakura_header_chain::HeaderFailure::Empty,
                zakura_header_chain::ErrorCategory::MalformedProtocol,
                zakura_header_chain::Attribution::HeaderPeer(source),
            ),
            (
                zakura_header_chain::HeaderFailure::InvalidLease,
                zakura_header_chain::ErrorCategory::StaleTargetOrGeneration,
                zakura_header_chain::Attribution::None,
            ),
            (
                zakura_header_chain::HeaderFailure::ClockRange,
                zakura_header_chain::ErrorCategory::LocalResourceOrStorage,
                zakura_header_chain::Attribution::None,
            ),
        ] {
            let error = classify_header_preparation_failure(failure, &entries, source, owner);
            assert_eq!(error.category, expected_category);
            assert_eq!(error.attribution, expected_attribution);
        }
    }

    #[test]
    fn oversized_body_hint_is_malformed_metadata_not_an_invalid_header() {
        let source = zakura_header_chain::SourceId::from_digest([6; 32]);
        let hash = block::Hash([7; 32]);
        let error = classify_body_size_hint_failure(
            zakura_header_chain::BodySizeHint::new(2_000_001)
                .expect_err("the fixture exceeds the canonical body-size hint limit"),
            hash,
            source,
        );

        assert_eq!(
            error.category,
            zakura_header_chain::ErrorCategory::MalformedProtocol
        );
        assert_ne!(
            error.category,
            zakura_header_chain::ErrorCategory::InvalidHeader
        );
        assert_eq!(
            error.subject,
            zakura_header_chain::ErrorSubject::Header(zakura_header_chain::HeaderId::new(hash))
        );
        assert_eq!(
            error.rule,
            Some(zakura_header_chain::RuleId::new("LC-WIRE-13"))
        );
        assert_eq!(
            error.attribution,
            zakura_header_chain::Attribution::HeaderPeer(source)
        );
    }
}
