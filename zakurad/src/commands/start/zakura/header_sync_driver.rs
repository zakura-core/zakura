use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use color_eyre::eyre::{eyre, Report};
use sha2::{Digest, Sha256};
use tower::{Service, ServiceExt};

use zakura_chain::{
    block::{self},
    parallel::commitment_aux::BlockCommitmentRoots,
};
#[cfg(test)]
use zakura_network::zakura::{AuxSchema, HeaderEntry, HeaderPathPage, ZakuraPeerId};
use zakura_network::zakura::{FullStateFrontiers, ZakuraHeaderSyncDriverStartup};
use zakura_node_services::header_chain::{
    self as port, HeaderChainFuture, HeaderChainPort, HeaderChainPortError,
};

use super::{verified_block_tip_from_state, ZAKURA_HEADER_SYNC_DRIVER_TIMEOUT};

pub(crate) async fn zakura_header_sync_driver_startup<State>(
    state: State,
    read_state: zakura_state::ReadStateService,
    network: &zakura_chain::parameters::Network,
) -> Result<ZakuraHeaderSyncDriverStartup, Report>
where
    State: Service<
            zakura_state::Request,
            Response = zakura_state::Response,
            Error = zakura_state::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    State::Future: Send + 'static,
{
    let best_header_tip = match tokio::time::timeout(
        ZAKURA_HEADER_SYNC_DRIVER_TIMEOUT,
        read_state
            .clone()
            .oneshot(zakura_state::ReadRequest::BestHeaderTip),
    )
    .await
    .map_err(|_| eyre!("timed out reading BestHeaderTip"))?
    .map_err(|error| eyre!("{error}"))?
    {
        zakura_state::ReadResponse::BestHeaderTip(tip) => tip,
        response => Err(eyre!("unexpected BestHeaderTip response: {response:?}"))?,
    };

    let finalized_tip = match tokio::time::timeout(
        ZAKURA_HEADER_SYNC_DRIVER_TIMEOUT,
        read_state
            .clone()
            .oneshot(zakura_state::ReadRequest::FinalizedTip),
    )
    .await
    .map_err(|_| eyre!("timed out reading FinalizedTip"))?
    .map_err(|error| eyre!("{error}"))?
    {
        zakura_state::ReadResponse::FinalizedTip(tip) => tip,
        response => Err(eyre!("unexpected FinalizedTip response: {response:?}"))?,
    };

    let verified_block_tip = match tokio::time::timeout(
        ZAKURA_HEADER_SYNC_DRIVER_TIMEOUT,
        read_state.clone().oneshot(zakura_state::ReadRequest::Tip),
    )
    .await
    .map_err(|_| eyre!("timed out reading Tip"))?
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
        read_state.clone(),
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
        header_chain_port: Arc::new(HeaderChainServicePort::new(state, read_state)),
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
    let roots = match tokio::time::timeout(
        ZAKURA_HEADER_SYNC_DRIVER_TIMEOUT,
        read_state.oneshot(zakura_state::ReadRequest::BlockRoots {
            start_height,
            count,
        }),
    )
    .await
    .map_err(|_| eyre!("timed out reading BlockRoots"))?
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
        + Sync
        + 'static,
    ReadState::Future: Send + 'static,
{
    let verified_block_tip = match tokio::time::timeout(
        ZAKURA_HEADER_SYNC_DRIVER_TIMEOUT,
        read_state.clone().oneshot(zakura_state::ReadRequest::Tip),
    )
    .await
    .map_err(|_| eyre!("timed out reading Tip"))?
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

#[derive(Clone, Debug)]
pub(crate) struct HeaderChainServicePort<State, ReadState> {
    state: State,
    read_state: ReadState,
    next_path_token: Arc<AtomicU64>,
    retained_path_ids: Arc<Mutex<HashMap<port::HeaderPathToken, u64>>>,
}

impl<State, ReadState> HeaderChainServicePort<State, ReadState> {
    pub(crate) fn new(state: State, read_state: ReadState) -> Self {
        Self {
            state,
            read_state,
            next_path_token: Arc::new(AtomicU64::new(1)),
            retained_path_ids: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<State, ReadState> HeaderChainPort for HeaderChainServicePort<State, ReadState>
where
    State: Service<
            zakura_state::Request,
            Response = zakura_state::Response,
            Error = zakura_state::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    State::Future: Send + 'static,
    ReadState: Service<
            zakura_state::ReadRequest,
            Response = zakura_state::ReadResponse,
            Error = zakura_state::BoxError,
        > + Clone
        + Send
        + Sync
        + 'static,
    ReadState::Future: Send + 'static,
{
    fn continuation_locator(
        &self,
    ) -> HeaderChainFuture<
        '_,
        Result<Option<zakura_header_chain::HeaderLocator>, HeaderChainPortError>,
    > {
        let read_state = self.read_state.clone();
        Box::pin(async move {
            match tokio::time::timeout(
                ZAKURA_HEADER_SYNC_DRIVER_TIMEOUT,
                read_state.oneshot(zakura_state::ReadRequest::HeaderLocator),
            )
            .await
            {
                Ok(Ok(zakura_state::ReadResponse::HeaderLocator(locator))) => Ok(locator),
                Ok(Ok(_)) => Err(HeaderChainPortError::Unavailable { source: None }),
                Ok(Err(error)) => Err(HeaderChainPortError::Unavailable {
                    source: Some(Arc::from(error)),
                }),
                Err(_) => Err(HeaderChainPortError::Timeout),
            }
        })
    }

    fn vct_repair_context(
        &self,
        owner: zakura_header_chain::WorkOwner,
        height: block::Height,
    ) -> HeaderChainFuture<'_, Result<port::VctRepairContextReply, HeaderChainPortError>> {
        let read_state = self.read_state.clone();
        Box::pin(async move {
            match tokio::time::timeout(
                ZAKURA_HEADER_SYNC_DRIVER_TIMEOUT,
                read_state.oneshot(zakura_state::ReadRequest::VctRepairContext { owner, height }),
            )
            .await
            {
                Ok(Ok(zakura_state::ReadResponse::VctRepairContext(Some(context)))) => {
                    Ok(port::VctRepairContextReply::Resolved(context))
                }
                Ok(Ok(zakura_state::ReadResponse::VctRepairContext(None))) => {
                    Ok(port::VctRepairContextReply::Stale)
                }
                Ok(Ok(_)) => Err(HeaderChainPortError::Unavailable { source: None }),
                Ok(Err(error)) => Err(HeaderChainPortError::Unavailable {
                    source: Some(Arc::from(error)),
                }),
                Err(_) => Err(HeaderChainPortError::Timeout),
            }
        })
    }

    fn acquire_header_path(
        &self,
        request: port::AcquireHeaderPath,
    ) -> HeaderChainFuture<'_, Result<port::AcquireHeaderPathReply, HeaderChainPortError>> {
        let read_state = self.read_state.clone();
        let token = port::HeaderPathToken::from_adapter_id(
            self.next_path_token.fetch_add(1, Ordering::Relaxed),
        );
        let retained_path_ids = self.retained_path_ids.clone();
        Box::pin(
            async move { acquire_header_path(read_state, request, token, retained_path_ids).await },
        )
    }

    fn read_header_path(
        &self,
        path: port::RetainedHeaderPath,
        request: port::ReadHeaderPath,
    ) -> HeaderChainFuture<'_, Result<port::ReadHeaderPathReply, HeaderChainPortError>> {
        let read_state = self.read_state.clone();
        let retained_path_ids = self.retained_path_ids.clone();
        Box::pin(
            async move { read_header_path(read_state, path, request, retained_path_ids).await },
        )
    }

    fn release_header_path(
        &self,
        path: port::RetainedHeaderPath,
    ) -> HeaderChainFuture<'_, Result<(), HeaderChainPortError>> {
        let read_state = self.read_state.clone();
        let retained_path_ids = self.retained_path_ids.clone();
        Box::pin(async move { release_header_path(read_state, path, retained_path_ids).await })
    }

    fn prepare_header_target(
        &self,
        request: port::PrepareHeaderTarget,
    ) -> HeaderChainFuture<'_, port::PrepareHeaderTargetReply> {
        let read_state = self.read_state.clone();
        Box::pin(async move { prepare_header_target(read_state, request).await })
    }

    fn apply_header_target(
        &self,
        target: port::PreparedHeaderTarget,
    ) -> HeaderChainFuture<'_, port::ApplyHeaderTargetReply> {
        let state = self.state.clone();
        Box::pin(async move { apply_header_target(state, target).await })
    }
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
    entries: &[port::HeaderTargetEntry],
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
    request: port::PrepareHeaderTarget,
) -> port::PrepareHeaderTargetReply
where
    ReadState: Service<
            zakura_state::ReadRequest,
            Response = zakura_state::ReadResponse,
            Error = zakura_state::BoxError,
        > + Send
        + 'static,
    ReadState::Future: Send + 'static,
{
    let port::PrepareHeaderTarget {
        source,
        network,
        owner,
        common_ancestor,
        target,
        entries,
        completion,
    } = request;
    let lease = match tokio::time::timeout(
        ZAKURA_HEADER_SYNC_DRIVER_TIMEOUT,
        read_state.oneshot(zakura_state::ReadRequest::HeaderValidationLease {
            parent_hash: common_ancestor.hash,
        }),
    )
    .await
    {
        Ok(Ok(zakura_state::ReadResponse::HeaderValidationLease(Some(lease))))
            if lease.parent() == common_ancestor =>
        {
            lease
        }
        Ok(Ok(zakura_state::ReadResponse::HeaderValidationLease(_))) => {
            return Err(Arc::new(
                zakura_header_chain::HeaderChainError::stale_target(
                    zakura_header_chain::ErrorSubject::Branch(owner.branch),
                ),
            ));
        }
        Ok(Ok(_)) => {
            return Err(Arc::new(
                zakura_header_chain::HeaderChainError::local_resource(
                    zakura_header_chain::ErrorSubject::Branch(owner.branch),
                    None,
                ),
            ));
        }
        Ok(Err(error)) => {
            return Err(Arc::new(
                zakura_header_chain::HeaderChainError::local_resource(
                    zakura_header_chain::ErrorSubject::Branch(owner.branch),
                    Some(error),
                ),
            ));
        }
        Err(_) => {
            return Err(Arc::new(
                zakura_header_chain::HeaderChainError::local_resource(
                    zakura_header_chain::ErrorSubject::Branch(owner.branch),
                    None,
                ),
            ));
        }
    };

    let prepared = tokio::task::spawn_blocking(move || {
        let rules = zakura_header_chain::HeaderRules::for_validation_lease(network, &lease)
            .map_err(|error| {
                Arc::new(zakura_header_chain::HeaderChainError::unknown_anchor(
                    zakura_header_chain::ErrorSubject::Branch(owner.branch),
                    Some(Box::new(error)),
                ))
            })?;
        let headers: Vec<_> = entries.iter().map(|entry| entry.header.clone()).collect();
        let batch = zakura_header_chain::prepare_context_free_headers(
            zakura_header_chain::HeaderBatchInput::new(&headers),
            common_ancestor,
            &rules,
            &zakura_header_chain::SystemClock,
        )
        .map_err(|error| {
            Arc::new(classify_header_preparation_failure(
                error, &entries, source, owner,
            ))
        })?;
        let mut aux = Vec::with_capacity(entries.len());
        for (entry, prepared) in entries.iter().zip(batch.headers()) {
            let body_size =
                zakura_header_chain::BodySizeHint::new(entry.body_size).map_err(|error| {
                    Arc::new(classify_body_size_hint_failure(
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
        Ok::<_, Arc<zakura_header_chain::HeaderChainError>>((batch, aux))
    })
    .await;
    let (batch, aux) = match prepared {
        Ok(Ok(prepared)) => prepared,
        Ok(Err(error)) => return Err(error),
        Err(error) => {
            return Err(Arc::new(
                zakura_header_chain::HeaderChainError::local_resource(
                    zakura_header_chain::ErrorSubject::Branch(owner.branch),
                    Some(Box::new(error)),
                ),
            ));
        }
    };

    Ok(port::PreparedHeaderTarget::from_insert(Box::new(
        zakura_header_chain::InsertHeaders {
            owner,
            source,
            parent_hash: common_ancestor.hash,
            target_tip_hash: target.hash,
            completion,
            batch,
            aux,
        },
    )))
}

async fn apply_header_target<State>(
    state: State,
    target: port::PreparedHeaderTarget,
) -> port::ApplyHeaderTargetReply
where
    State: Service<
            zakura_state::Request,
            Response = zakura_state::Response,
            Error = zakura_state::BoxError,
        > + Send
        + 'static,
    State::Future: Send + 'static,
{
    let insert = target.into_insert();
    let owner = insert.owner;
    match tokio::time::timeout(
        ZAKURA_HEADER_SYNC_DRIVER_TIMEOUT,
        state.oneshot(zakura_state::Request::ApplyHeaderChainInsert {
            expected_version: owner.state_version,
            insert,
        }),
    )
    .await
    {
        Ok(Ok(zakura_state::Response::HeaderChainInsertApplied(
            zakura_header_chain::ApplyResult::Committed
            | zakura_header_chain::ApplyResult::NoChange(_),
        ))) => Ok(()),
        Ok(Ok(zakura_state::Response::HeaderChainInsertApplied(
            zakura_header_chain::ApplyResult::Stale(_),
        ))) => Err(Arc::new(
            zakura_header_chain::HeaderChainError::stale_target(
                zakura_header_chain::ErrorSubject::Branch(owner.branch),
            ),
        )),
        Ok(Ok(_)) => Err(Arc::new(
            zakura_header_chain::HeaderChainError::local_resource(
                zakura_header_chain::ErrorSubject::Branch(owner.branch),
                None,
            ),
        )),
        Ok(Err(error)) => Err(Arc::new(
            zakura_header_chain::HeaderChainError::local_resource(
                zakura_header_chain::ErrorSubject::Branch(owner.branch),
                Some(error),
            ),
        )),
        Err(_) => Err(Arc::new(
            zakura_header_chain::HeaderChainError::local_resource(
                zakura_header_chain::ErrorSubject::Branch(owner.branch),
                None,
            ),
        )),
    }
}

async fn acquire_header_path<ReadState>(
    read_state: ReadState,
    request: port::AcquireHeaderPath,
    token: port::HeaderPathToken,
    retained_path_ids: Arc<Mutex<HashMap<port::HeaderPathToken, u64>>>,
) -> Result<port::AcquireHeaderPathReply, HeaderChainPortError>
where
    ReadState: Service<
            zakura_state::ReadRequest,
            Response = zakura_state::ReadResponse,
            Error = zakura_state::BoxError,
        > + Send
        + 'static,
    ReadState::Future: Send + 'static,
{
    let port::AcquireHeaderPath {
        source,
        session_id,
        scope,
        target_tip_hash,
        locator_hashes,
    } = request;
    match tokio::time::timeout(
        ZAKURA_HEADER_SYNC_DRIVER_TIMEOUT,
        read_state.oneshot(zakura_state::ReadRequest::AcquireRetainedHeaderPath {
            peer: source,
            session_id,
            target_tip_hash,
            scope,
            locator_hashes,
        }),
    )
    .await
    {
        Ok(Ok(zakura_state::ReadResponse::RetainedHeaderPathLease(outcome))) => match outcome {
            zakura_state::RetainedPathLeaseOutcome::Acquired(lease) => {
                retained_path_ids
                    .lock()
                    .map_err(|_| HeaderChainPortError::Unavailable { source: None })?
                    .insert(token, lease.lease_id);
                Ok(port::AcquireHeaderPathReply::Acquired(Box::new(
                    port::RetainedHeaderPath::from_adapter(
                        token,
                        source,
                        session_id,
                        lease.common_ancestor,
                        lease.target,
                        lease.scope,
                    ),
                )))
            }
            zakura_state::RetainedPathLeaseOutcome::TargetNotRetained => {
                Ok(port::AcquireHeaderPathReply::TargetNotRetained)
            }
            zakura_state::RetainedPathLeaseOutcome::NoLocatorIntersection => {
                Ok(port::AcquireHeaderPathReply::NoLocatorIntersection)
            }
            zakura_state::RetainedPathLeaseOutcome::HistoryPruned => {
                Ok(port::AcquireHeaderPathReply::HistoryPruned)
            }
            zakura_state::RetainedPathLeaseOutcome::Busy => Ok(port::AcquireHeaderPathReply::Busy),
        },
        Ok(Ok(_)) => Err(HeaderChainPortError::Unavailable { source: None }),
        Ok(Err(error)) => Err(HeaderChainPortError::Unavailable {
            source: Some(Arc::from(error)),
        }),
        Err(_) => Err(HeaderChainPortError::Timeout),
    }
}

async fn read_header_path<ReadState>(
    read_state: ReadState,
    path: port::RetainedHeaderPath,
    request: port::ReadHeaderPath,
    retained_path_ids: Arc<Mutex<HashMap<port::HeaderPathToken, u64>>>,
) -> Result<port::ReadHeaderPathReply, HeaderChainPortError>
where
    ReadState: Service<
            zakura_state::ReadRequest,
            Response = zakura_state::ReadResponse,
            Error = zakura_state::BoxError,
        > + Send
        + 'static,
    ReadState::Future: Send + 'static,
{
    let (token, source, session_id) = path.adapter_identity();
    let lease_id = retained_path_ids
        .lock()
        .map_err(|_| HeaderChainPortError::Unavailable { source: None })?
        .get(&token)
        .copied()
        .ok_or(HeaderChainPortError::Unavailable { source: None })?;
    let port::ReadHeaderPath {
        after_hash,
        max_header_count,
    } = request;
    match tokio::time::timeout(
        ZAKURA_HEADER_SYNC_DRIVER_TIMEOUT,
        read_state.oneshot(zakura_state::ReadRequest::ReadRetainedHeaderPath {
            peer: source,
            session_id,
            lease_id,
            scope: path.scope,
            after_hash,
            max_count: max_header_count,
        }),
    )
    .await
    {
        Ok(Ok(zakura_state::ReadResponse::RetainedHeaderPathPage(
            zakura_state::RetainedPathReadOutcome::Page(page),
        ))) => Ok(port::ReadHeaderPathReply::Page(Box::new(
            port::RetainedHeaderPathPage {
                common_ancestor: page.common_ancestor,
                target: page.target,
                scope: page.scope,
                nodes: page.nodes,
                aux_deliveries: page.aux_deliveries,
                complete: page.complete,
            },
        ))),
        Ok(Ok(zakura_state::ReadResponse::RetainedHeaderPathPage(
            zakura_state::RetainedPathReadOutcome::Unavailable,
        ))) => Ok(port::ReadHeaderPathReply::Unavailable),
        Ok(Ok(_)) => Err(HeaderChainPortError::Unavailable { source: None }),
        Ok(Err(error)) => Err(HeaderChainPortError::Unavailable {
            source: Some(Arc::from(error)),
        }),
        Err(_) => Err(HeaderChainPortError::Timeout),
    }
}

#[cfg(test)]
fn assemble_header_path_page(
    lease_id: u64,
    page: port::RetainedHeaderPathPage,
    requested_schema: AuxSchema,
) -> Option<HeaderPathPage> {
    if page.nodes.len() != page.aux_deliveries.len() {
        return None;
    }

    let tree_aux_schema = if requested_schema == AuxSchema::V1
        && page
            .aux_deliveries
            .iter()
            .all(|deliveries| selected_aux_delivery(deliveries, AuxSchema::V1).is_some())
    {
        AuxSchema::V1
    } else {
        AuxSchema::None
    };
    let entries = page
        .nodes
        .into_iter()
        .zip(page.aux_deliveries)
        .map(|(node, deliveries)| {
            let delivery = selected_aux_delivery(&deliveries, tree_aux_schema);
            HeaderEntry {
                header: node.header,
                body_size: delivery.map_or(0, |delivery| match delivery.body_size {
                    zakura_header_chain::BodySizeHint::Unknown => 0,
                    zakura_header_chain::BodySizeHint::Known(size) => size.get(),
                }),
                tree_aux: (tree_aux_schema == AuxSchema::V1)
                    .then(|| delivery.and_then(|delivery| delivery.tree_aux))
                    .flatten(),
            }
        })
        .collect();

    Some(HeaderPathPage {
        lease_id,
        common_ancestor: page.common_ancestor,
        target: page.target,
        scope: page.scope,
        tree_aux_schema,
        entries,
        complete: page.complete,
    })
}

#[cfg(test)]
fn selected_aux_delivery(
    deliveries: &[zakura_header_chain::AuxDelivery],
    schema: AuxSchema,
) -> Option<zakura_header_chain::AuxDelivery> {
    deliveries
        .iter()
        .copied()
        .filter(|delivery| {
            !matches!(
                delivery.authentication,
                zakura_header_chain::AuxAuthentication::Rejected { .. }
            ) && match schema {
                AuxSchema::None => {
                    matches!(
                        delivery.body_size,
                        zakura_header_chain::BodySizeHint::Known(_)
                    )
                }
                AuxSchema::V1 => delivery.tree_aux.is_some(),
            }
        })
        .min_by_key(|delivery| {
            (
                !matches!(
                    delivery.authentication,
                    zakura_header_chain::AuxAuthentication::Authenticated { .. }
                ),
                delivery.delivery_id,
            )
        })
}

async fn release_header_path<ReadState>(
    read_state: ReadState,
    path: port::RetainedHeaderPath,
    retained_path_ids: Arc<Mutex<HashMap<port::HeaderPathToken, u64>>>,
) -> Result<(), HeaderChainPortError>
where
    ReadState: Service<
            zakura_state::ReadRequest,
            Response = zakura_state::ReadResponse,
            Error = zakura_state::BoxError,
        > + Send
        + 'static,
    ReadState::Future: Send + 'static,
{
    let (token, source, session_id) = path.adapter_identity();
    let lease_id = retained_path_ids
        .lock()
        .map_err(|_| HeaderChainPortError::Unavailable { source: None })?
        .remove(&token)
        .ok_or(HeaderChainPortError::Unavailable { source: None })?;
    match tokio::time::timeout(
        ZAKURA_HEADER_SYNC_DRIVER_TIMEOUT,
        read_state.oneshot(zakura_state::ReadRequest::ReleaseRetainedHeaderPath {
            peer: source,
            session_id,
            lease_id,
            scope: path.scope,
        }),
    )
    .await
    {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(error)) => Err(HeaderChainPortError::Unavailable {
            source: Some(Arc::from(error)),
        }),
        Err(_) => Err(HeaderChainPortError::Timeout),
    }
}

#[cfg(test)]
fn source_id(peer: &ZakuraPeerId) -> zakura_header_chain::SourceId {
    zakura_header_chain::SourceId::from_digest(peer.digest())
}

#[cfg(test)]
mod tests {
    use std::{
        future::pending,
        num::{NonZeroU32, NonZeroU64},
    };

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

    fn pending_read_state() -> tower::util::BoxCloneService<
        zakura_state::ReadRequest,
        zakura_state::ReadResponse,
        zakura_state::BoxError,
    > {
        tower::service_fn(|_: zakura_state::ReadRequest| async {
            pending::<Result<zakura_state::ReadResponse, zakura_state::BoxError>>().await
        })
        .boxed_clone()
    }

    #[test]
    fn served_aux_selection_is_deterministic_and_excludes_rejected_evidence() {
        let owner = owner();
        let source = zakura_header_chain::SourceId::from_digest([5; 32]);
        let header_hash = block::Hash([6; 32]);
        let tree_aux = zakura_header_chain::TreeAuxRecordV1 {
            height: block::Height(1),
            sapling_root: Default::default(),
            orchard_root: Default::default(),
            ironwood_root: Default::default(),
            sapling_tx_count: 0,
            orchard_tx_count: 0,
            ironwood_tx_count: 0,
            auth_data_root: [0; 32].into(),
        };
        let delivery =
            |marker, body_size, tree_aux, authentication| zakura_header_chain::AuxDelivery {
                delivery_id: zakura_header_chain::EvidenceId::from_digest([marker; 32]),
                header_hash,
                source,
                owner,
                body_size,
                tree_aux,
                authentication,
            };
        let rejected = delivery(
            1,
            zakura_header_chain::BodySizeHint::Known(NonZeroU32::new(10).expect("ten is nonzero")),
            Some(tree_aux),
            zakura_header_chain::AuxAuthentication::Rejected {
                evidence: zakura_header_chain::EvidenceId::from_digest([7; 32]),
            },
        );
        let unauthenticated = delivery(
            2,
            zakura_header_chain::BodySizeHint::Known(
                NonZeroU32::new(20).expect("twenty is nonzero"),
            ),
            Some(tree_aux),
            zakura_header_chain::AuxAuthentication::Unauthenticated,
        );
        let authenticated = delivery(
            3,
            zakura_header_chain::BodySizeHint::Known(
                NonZeroU32::new(30).expect("thirty is nonzero"),
            ),
            Some(tree_aux),
            zakura_header_chain::AuxAuthentication::Authenticated {
                evidence: zakura_header_chain::EvidenceId::from_digest([8; 32]),
                boundary_hash: block::Hash([9; 32]),
            },
        );
        let deliveries = [rejected, unauthenticated, authenticated];

        assert_eq!(
            selected_aux_delivery(&deliveries, AuxSchema::V1),
            Some(authenticated)
        );
        assert_eq!(
            selected_aux_delivery(&deliveries, AuxSchema::None),
            Some(authenticated)
        );
        assert_eq!(selected_aux_delivery(&[rejected], AuxSchema::V1), None);
    }

    #[test]
    fn retained_page_uses_v1_only_when_every_record_is_available() {
        let header = regtest_genesis_block().header.clone();
        let hash = header.hash();
        let work = header
            .difficulty_threshold
            .to_work()
            .expect("the genesis target has defined work");
        let node = zakura_header_chain::HeaderNode::from_durable_parts(
            header,
            hash,
            regtest_genesis_block().header.previous_block_hash,
            block::Height(0),
            work,
            zakura_header_chain::WorkCoordinate::new(hash, work.as_u256()),
            zakura_header_chain::HeaderValidationState::Valid,
            Default::default(),
            Default::default(),
            Vec::new(),
        )
        .expect("the canonical genesis fields form a durable node");
        let frontier = zakura_header_chain::Frontier::new(block::Height(0), hash);
        let mut page = port::RetainedHeaderPathPage {
            common_ancestor: frontier,
            target: frontier,
            scope: owner().scope(),
            nodes: vec![node],
            aux_deliveries: vec![Vec::new()],
            complete: true,
        };

        let fallback = assemble_header_path_page(1, page.clone(), AuxSchema::V1)
            .expect("the coherent parallel page assembles");
        assert_eq!(fallback.tree_aux_schema, AuxSchema::None);
        assert_eq!(fallback.entries[0].body_size, 0);
        assert_eq!(fallback.entries[0].tree_aux, None);

        let tree_aux = zakura_header_chain::TreeAuxRecordV1 {
            height: block::Height(0),
            sapling_root: Default::default(),
            orchard_root: Default::default(),
            ironwood_root: Default::default(),
            sapling_tx_count: 0,
            orchard_tx_count: 0,
            ironwood_tx_count: 0,
            auth_data_root: [0; 32].into(),
        };
        page.aux_deliveries[0].push(zakura_header_chain::AuxDelivery {
            delivery_id: zakura_header_chain::EvidenceId::from_digest([10; 32]),
            header_hash: hash,
            source: zakura_header_chain::SourceId::from_digest([11; 32]),
            owner: owner(),
            body_size: zakura_header_chain::BodySizeHint::Known(
                NonZeroU32::new(321).expect("321 is nonzero"),
            ),
            tree_aux: Some(tree_aux),
            authentication: zakura_header_chain::AuxAuthentication::Unauthenticated,
        });
        let no_aux = assemble_header_path_page(1, page.clone(), AuxSchema::None)
            .expect("the coherent parallel page assembles");
        assert_eq!(no_aux.tree_aux_schema, AuxSchema::None);
        assert_eq!(no_aux.entries[0].body_size, 321);
        assert_eq!(no_aux.entries[0].tree_aux, None);

        let served = assemble_header_path_page(1, page, AuxSchema::V1)
            .expect("the coherent parallel page assembles");
        assert_eq!(served.tree_aux_schema, AuxSchema::V1);
        assert_eq!(served.entries[0].body_size, 321);
        assert_eq!(served.entries[0].tree_aux, Some(tree_aux));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn wedged_state_read_returns_busy_at_the_driver_deadline() {
        let owner = owner();
        let peer = ZakuraPeerId::new(vec![7; 32]).expect("the peer identity has canonical width");
        let request = port::AcquireHeaderPath {
            source: source_id(&peer),
            session_id: owner.session_id,
            scope: owner.scope(),
            target_tip_hash: owner.branch.target_tip_hash,
            locator_hashes: vec![owner.branch.anchor_hash],
        };
        let started = tokio::time::Instant::now();

        let result = acquire_header_path(
            pending_read_state(),
            request,
            port::HeaderPathToken::from_adapter_id(1),
            Arc::new(Mutex::new(HashMap::new())),
        )
        .await;

        assert!(matches!(result, Err(HeaderChainPortError::Timeout)));
        assert_eq!(
            tokio::time::Instant::now().duration_since(started),
            ZAKURA_HEADER_SYNC_DRIVER_TIMEOUT
        );
    }

    #[test]
    fn driver_preserves_every_header_preparation_failure_category() {
        let source = zakura_header_chain::SourceId::from_digest([5; 32]);
        let owner = owner();
        let header = regtest_genesis_block().header.clone();
        let entries = [port::HeaderTargetEntry {
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
