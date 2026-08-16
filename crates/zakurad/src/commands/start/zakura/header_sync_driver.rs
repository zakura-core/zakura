use std::{future::Future, sync::Arc};

use color_eyre::eyre::{eyre, Report};
use sha2::{Digest, Sha256};
use tower::{Service, ServiceExt};

use zakura_chain::block::{self};
use zakura_chain::parallel::commitment_aux::BlockCommitmentRoots;
#[cfg(test)]
use zakura_network::zakura::{AuxSchema, HeaderEntry, HeaderPathPage, ZakuraPeerId};
use zakura_network::zakura::{FullStateFrontiers, ZakuraHeaderSyncDriverStartup};
use zakura_node_services::header_chain::{self as port, HeaderChainFuture, Port, PortError};

use super::{verified_block_tip_from_state, ZAKURA_HEADER_SYNC_DRIVER_TIMEOUT};

pub(crate) async fn zakura_header_sync_driver_startup<State>(
    state: State,
    read_state: zakura_state::ReadStateService,
    header_chain_authority: zakura_state::HeaderChainBodyEvidenceAuthority,
    network: &zakura_chain::parameters::Network,
    coordinator: &std::sync::Arc<super::SyncCoordinator>,
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
    let mut header_runtime_status = read_state.subscribe_header_runtime_status();
    wait_for_header_runtime(&mut header_runtime_status).await?;
    coordinator
        .observe_header_runtime(&header_runtime_status.borrow())
        .map_err(|error| eyre!("coordinator rejected header runtime status: {error}"))?;
    if header_runtime_status.borrow().is_ready() && committed_snapshots.borrow().is_none() {
        return Err(eyre!(
            "header runtime reported ready before publishing its committed snapshot"
        ));
    }
    let vct_root_repairs = read_state.subscribe_vct_root_repairs();
    let best_header_tip = committed_snapshots
        .borrow()
        .as_ref()
        .map(|snapshot| {
            (
                snapshot.frontiers.header_best.height,
                snapshot.frontiers.header_best.hash,
            )
        })
        .or(best_header_tip)
        .unwrap_or(empty_state_tip);

    Ok(ZakuraHeaderSyncDriverStartup {
        frontiers: FullStateFrontiers {
            finalized_height,
            verified_block_tip: verified_block_tip.0,
            verified_block_hash: verified_block_tip.1,
        },
        best_header_tip: Some(best_header_tip),
        verified_block_tip_hash: verified_block_tip.1,
        committed_snapshots,
        service_demand: coordinator.subscribe_service_demand(),
        vct_root_repairs: Some(vct_root_repairs),
        header_chain_port: Arc::new(HeaderChainServicePort::new(
            state,
            read_state,
            header_chain_authority,
            network.clone(),
        )),
    })
}

async fn wait_for_header_runtime(
    status: &mut tokio::sync::watch::Receiver<
        zakura_node_services::sync_lifecycle::HeaderRuntimeStatus,
    >,
) -> Result<(), Report> {
    use zakura_node_services::sync_lifecycle::HeaderRuntimeStatus;

    const RECONSTRUCTION_DIAGNOSTIC_INTERVAL: std::time::Duration =
        std::time::Duration::from_secs(30);

    let mut latest_progress = None;
    let mut last_progress_at = tokio::time::Instant::now();
    let mut attachment_pending_since = None;
    loop {
        let current = status.borrow().clone();
        match current {
            HeaderRuntimeStatus::Detached {
                reason:
                    zakura_node_services::sync_lifecycle::HeaderRuntimeDetachedReason::AwaitingSemanticHandoff,
                ..
            }
            | HeaderRuntimeStatus::Ready { .. } => return Ok(()),
            HeaderRuntimeStatus::Detached {
                epoch,
                reason:
                    zakura_node_services::sync_lifecycle::HeaderRuntimeDetachedReason::AttachmentPending,
                ..
            } => {
                let pending_since = attachment_pending_since.get_or_insert_with(tokio::time::Instant::now);
                match tokio::time::timeout(
                    RECONSTRUCTION_DIAGNOSTIC_INTERVAL,
                    status.changed(),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) => {
                        return Err(eyre!(
                            "header-runtime lifecycle publisher closed before attachment started"
                        ))
                    }
                    Err(_) => tracing::warn!(
                        header_runtime_epoch = epoch.get(),
                        attachment_pending_for = ?pending_since.elapsed(),
                        "header runtime attachment is still pending"
                    ),
                }
            }
            HeaderRuntimeStatus::Failed { error, .. } => {
                return Err(eyre!("header runtime attachment failed: {error}"))
            }
            HeaderRuntimeStatus::Reconstructing { epoch, progress } => {
                attachment_pending_since = None;
                if latest_progress != Some((epoch, progress)) {
                    latest_progress = Some((epoch, progress));
                    last_progress_at = tokio::time::Instant::now();
                }
                match tokio::time::timeout(
                    RECONSTRUCTION_DIAGNOSTIC_INTERVAL,
                    status.changed(),
                )
                .await {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) => {
                        return Err(eyre!(
                            "header-runtime lifecycle publisher closed during startup"
                        ))
                    }
                    Err(_) => tracing::warn!(
                        header_runtime_epoch = epoch.get(),
                        ?progress,
                        progress_stale_for = ?last_progress_at.elapsed(),
                        "header runtime reconstruction is still active"
                    ),
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
    authority: zakura_state::HeaderChainBodyEvidenceAuthority,
    network: zakura_chain::parameters::Network,
    adapter_key: port::AdapterKey,
}

impl<State, ReadState> HeaderChainServicePort<State, ReadState> {
    pub(crate) fn new(
        state: State,
        read_state: ReadState,
        authority: zakura_state::HeaderChainBodyEvidenceAuthority,
        network: zakura_chain::parameters::Network,
    ) -> Self {
        Self {
            state,
            read_state,
            authority,
            network,
            adapter_key: port::AdapterKey::new(),
        }
    }
}

impl<State, ReadState> Port for HeaderChainServicePort<State, ReadState>
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
    ) -> HeaderChainFuture<'_, Result<Option<zakura_header_chain::HeaderLocator>, PortError>> {
        let read_state = self.read_state.clone();
        Box::pin(async move {
            match tokio::time::timeout(
                ZAKURA_HEADER_SYNC_DRIVER_TIMEOUT,
                read_state.oneshot(zakura_state::ReadRequest::HeaderLocator),
            )
            .await
            {
                Ok(Ok(zakura_state::ReadResponse::HeaderLocator(locator))) => Ok(locator),
                Ok(Ok(_)) => Err(PortError::Unavailable { source: None }),
                Ok(Err(error)) => Err(PortError::Unavailable {
                    source: Some(Arc::from(error)),
                }),
                Err(_) => Err(PortError::Timeout),
            }
        })
    }

    fn vct_repair_context(
        &self,
        owner: zakura_header_chain::BodyWorkOwner,
        height: block::Height,
    ) -> HeaderChainFuture<'_, Result<port::VctRepairContextReply, PortError>> {
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
                Ok(Ok(_)) => Err(PortError::Unavailable { source: None }),
                Ok(Err(error)) => Err(PortError::Unavailable {
                    source: Some(Arc::from(error)),
                }),
                Err(_) => Err(PortError::Timeout),
            }
        })
    }

    fn acquire_header_path(
        &self,
        request: port::AcquirePath,
    ) -> HeaderChainFuture<'_, Result<port::AcquirePathReply, PortError>> {
        let read_state = self.read_state.clone();
        let adapter_key = self.adapter_key.clone();
        Box::pin(async move { acquire_header_path(read_state, adapter_key, request).await })
    }

    fn read_header_path(
        &self,
        path: port::RetainedHeaderPath,
        request: port::ReadPath,
    ) -> HeaderChainFuture<'_, Result<port::ReadPathReply, PortError>> {
        let read_state = self.read_state.clone();
        let adapter_key = self.adapter_key.clone();
        let network = self.network.clone();
        Box::pin(
            async move { read_header_path(read_state, adapter_key, network, path, request).await },
        )
    }

    fn release_header_path(
        &self,
        path: port::RetainedHeaderPath,
    ) -> HeaderChainFuture<'_, Result<(), PortError>> {
        let read_state = self.read_state.clone();
        let adapter_key = self.adapter_key.clone();
        Box::pin(async move { release_header_path(read_state, adapter_key, path).await })
    }

    fn prepare_header_target(
        &self,
        request: port::PrepareHeaderTarget,
    ) -> HeaderChainFuture<'_, port::PrepareHeaderTargetReply> {
        let read_state = self.read_state.clone();
        let adapter_key = self.adapter_key.clone();
        Box::pin(async move { prepare_header_target(read_state, adapter_key, request).await })
    }

    fn apply_header_target(
        &self,
        target: port::PreparedHeaderTarget,
    ) -> HeaderChainFuture<'_, port::ApplyHeaderTargetReply> {
        let state = self.state.clone();
        let authority = self.authority.clone();
        let adapter_key = self.adapter_key.clone();
        Box::pin(async move { apply_header_target(state, authority, adapter_key, target).await })
    }
}

fn header_failure_evidence(
    source: zakura_header_chain::SourceId,
    owner: zakura_header_chain::HeaderSyncWorkOwner,
    hash: block::Hash,
    rule: zakura_header_chain::RuleId,
) -> zakura_header_chain::EvidenceId {
    let mut hasher = Sha256::new();
    hasher.update(b"zakura-header-validation-failure-v1");
    hasher.update(source.digest());
    hasher.update(owner.session_id().to_le_bytes());
    hasher.update(owner.request_id().get().to_le_bytes());
    hasher.update(hash.0);
    hasher.update(rule.as_str().as_bytes());
    zakura_header_chain::EvidenceId::from_digest(hasher.finalize().into())
}

/// Returns a deterministic, domain-separated delivery ID for the source, owner, and header.
fn header_aux_delivery_id(
    source: zakura_header_chain::SourceId,
    owner: zakura_header_chain::HeaderSyncWorkOwner,
    hash: block::Hash,
) -> zakura_header_chain::EvidenceId {
    let mut hasher = Sha256::new();
    hasher.update(b"zakura-header-aux-delivery-v1");
    hasher.update(source.digest());
    hasher.update(owner.session_id().to_le_bytes());
    hasher.update(owner.request_id().get().to_le_bytes());
    hasher.update(hash.0);
    zakura_header_chain::EvidenceId::from_digest(hasher.finalize().into())
}

fn classify_header_preparation_failure(
    error: zakura_header_chain::HeaderFailure,
    entries: &[port::TargetEntry],
    source: zakura_header_chain::SourceId,
    owner: zakura_header_chain::HeaderSyncWorkOwner,
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
        zakura_header_chain::HeaderFailure::Empty
        | zakura_header_chain::HeaderFailure::Oversized { .. } => {
            zakura_header_chain::HeaderChainError::malformed_protocol(
                zakura_header_chain::ErrorSubject::Request {
                    source,
                    request_id: owner.request_id(),
                },
                zakura_header_chain::RuleId::new("LC-WIRE-08"),
                source,
                None,
            )
        }
        zakura_header_chain::HeaderFailure::InvalidLease => {
            zakura_header_chain::HeaderChainError::stale_target(
                zakura_header_chain::ErrorSubject::Branch(owner.header_authority().branch),
            )
        }
        zakura_header_chain::HeaderFailure::ClockRange => {
            zakura_header_chain::HeaderChainError::local_resource(
                zakura_header_chain::ErrorSubject::Branch(owner.header_authority().branch),
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
    adapter_key: port::AdapterKey,
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
        owner,
        common_ancestor,
        target,
        entries,
        completion,
    } = request;
    let entries = entries.into_vec();
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
                    zakura_header_chain::ErrorSubject::Branch(owner.header_authority().branch),
                ),
            ));
        }
        Ok(Ok(_)) => {
            return Err(Arc::new(
                zakura_header_chain::HeaderChainError::local_resource(
                    zakura_header_chain::ErrorSubject::Branch(owner.header_authority().branch),
                    None,
                ),
            ));
        }
        Ok(Err(error)) => {
            return Err(Arc::new(
                zakura_header_chain::HeaderChainError::local_resource(
                    zakura_header_chain::ErrorSubject::Branch(owner.header_authority().branch),
                    Some(error),
                ),
            ));
        }
        Err(_) => {
            return Err(Arc::new(
                zakura_header_chain::HeaderChainError::local_resource(
                    zakura_header_chain::ErrorSubject::Branch(owner.header_authority().branch),
                    None,
                ),
            ));
        }
    };

    let prepared = tokio::task::spawn_blocking(move || {
        let rules =
            zakura_header_chain::HeaderRules::for_validation_lease(&lease).map_err(|error| {
                Arc::new(zakura_header_chain::HeaderChainError::unknown_anchor(
                    zakura_header_chain::ErrorSubject::Branch(owner.header_authority().branch),
                    Some(Box::new(error)),
                ))
            })?;
        let headers: Vec<_> = entries.iter().map(|entry| entry.header.clone()).collect();
        let batch = zakura_header_chain::prepare_headers(
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
            aux.push(zakura_header_chain::AuxDelivery {
                delivery_id: header_aux_delivery_id(source, owner, prepared.hash),
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
                    zakura_header_chain::ErrorSubject::Branch(owner.header_authority().branch),
                    Some(Box::new(error)),
                ),
            ));
        }
    };

    Ok(port::PreparedHeaderTarget::from_insert(
        &adapter_key,
        Box::new(zakura_header_chain::InsertHeaders {
            owner,
            source,
            parent_hash: common_ancestor.hash,
            target_tip_hash: target.hash,
            completion,
            batch,
            aux,
        }),
    ))
}

async fn apply_header_target<State>(
    state: State,
    authority: zakura_state::HeaderChainBodyEvidenceAuthority,
    adapter_key: port::AdapterKey,
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
    let owner = target.owner();
    let insert = target.into_insert(&adapter_key).map_err(|_| {
        Arc::new(zakura_header_chain::HeaderChainError::stale_target(
            zakura_header_chain::ErrorSubject::Branch(owner.header_authority().branch),
        ))
    })?;
    let owner = insert.owner;
    let prepared = authority.from_registered_header_attempt(insert);
    match wait_for_header_target_apply(
        owner,
        state.oneshot(zakura_state::Request::ApplyHeaderChainInsert { prepared }),
    )
    .await
    {
        Ok(zakura_state::Response::HeaderChainInsertApplied(
            zakura_header_chain::ApplyResult::Committed
            | zakura_header_chain::ApplyResult::NoChange(_),
        )) => Ok(port::ApplyHeaderTargetOutcome::Applied),
        Ok(zakura_state::Response::HeaderChainInsertApplied(
            zakura_header_chain::ApplyResult::ResourceStalled(receipt),
        )) => Ok(port::ApplyHeaderTargetOutcome::ResourceStalled(receipt)),
        Ok(zakura_state::Response::HeaderChainInsertApplied(
            zakura_header_chain::ApplyResult::Stale(_),
        )) => Err(Arc::new(
            zakura_header_chain::HeaderChainError::stale_target(
                zakura_header_chain::ErrorSubject::Branch(owner.header_authority().branch),
            ),
        )),
        Ok(response) => Err(header_target_apply_failure(
            owner,
            "unexpected_response",
            Some(Box::new(std::io::Error::other(format!(
                "unexpected state response: {response:?}"
            )))),
        )),
        Err(error) => Err(header_target_apply_failure(
            owner,
            "state_error",
            Some(error),
        )),
    }
}

async fn wait_for_header_target_apply<F>(
    owner: zakura_header_chain::HeaderSyncWorkOwner,
    apply: F,
) -> Result<zakura_state::Response, zakura_state::BoxError>
where
    F: Future<Output = Result<zakura_state::Response, zakura_state::BoxError>>,
{
    let started = tokio::time::Instant::now();
    tokio::pin!(apply);
    loop {
        match tokio::time::timeout(ZAKURA_HEADER_SYNC_DRIVER_TIMEOUT, &mut apply).await {
            Ok(result) => return result,
            Err(_) => {
                let header_owner = owner.header_authority();
                metrics::counter!(
                    "sync.header.port.stall.total",
                    "operation" => "apply_target",
                )
                .increment(1);
                tracing::warn!(
                    operation = "apply_target",
                    session_id = owner.session_id(),
                    request_id = owner.request_id().get(),
                    header_generation = header_owner.header_generation.get(),
                    branch = ?header_owner.branch,
                    elapsed = ?started.elapsed(),
                    "header target apply remains pending after a diagnostic interval"
                );
            }
        }
    }
}

fn header_target_apply_failure(
    owner: zakura_header_chain::HeaderSyncWorkOwner,
    reason: &'static str,
    source: Option<zakura_state::BoxError>,
) -> Arc<zakura_header_chain::HeaderChainError> {
    let branch = owner.header_authority().branch;
    metrics::counter!(
        "sync.header.port.failure.total",
        "operation" => "apply_target",
        "reason" => reason,
    )
    .increment(1);
    tracing::warn!(
        reason,
        ?branch,
        error = ?source.as_deref(),
        "failed to apply a prepared header target through state"
    );
    Arc::new(zakura_header_chain::HeaderChainError::local_resource(
        zakura_header_chain::ErrorSubject::Branch(branch),
        source,
    ))
}

async fn acquire_header_path<ReadState>(
    read_state: ReadState,
    adapter_key: port::AdapterKey,
    request: port::AcquirePath,
) -> Result<port::AcquirePathReply, PortError>
where
    ReadState: Service<
            zakura_state::ReadRequest,
            Response = zakura_state::ReadResponse,
            Error = zakura_state::BoxError,
        > + Send
        + 'static,
    ReadState::Future: Send + 'static,
{
    let port::AcquirePath {
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
            zakura_state::RetainedPathLeaseOutcome::Acquired(lease) => Ok(
                port::AcquirePathReply::Acquired(Box::new(port::RetainedHeaderPath::from_adapter(
                    &adapter_key,
                    lease.lease_id,
                    source,
                    session_id,
                    lease.common_ancestor,
                    lease.target,
                    lease.scope,
                ))),
            ),
            zakura_state::RetainedPathLeaseOutcome::TargetNotRetained => {
                Ok(port::AcquirePathReply::TargetNotRetained)
            }
            zakura_state::RetainedPathLeaseOutcome::NoLocatorIntersection => {
                Ok(port::AcquirePathReply::NoLocatorIntersection)
            }
            zakura_state::RetainedPathLeaseOutcome::HistoryPruned => {
                Ok(port::AcquirePathReply::HistoryPruned)
            }
            zakura_state::RetainedPathLeaseOutcome::Busy => Ok(port::AcquirePathReply::Busy),
        },
        Ok(Ok(_)) => Err(PortError::Unavailable { source: None }),
        Ok(Err(error)) => Err(PortError::Unavailable {
            source: Some(Arc::from(error)),
        }),
        Err(_) => Err(PortError::Timeout),
    }
}

async fn read_header_path<ReadState>(
    read_state: ReadState,
    adapter_key: port::AdapterKey,
    network: zakura_chain::parameters::Network,
    path: port::RetainedHeaderPath,
    request: port::ReadPath,
) -> Result<port::ReadPathReply, PortError>
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
    let Some((lease_id, source, session_id)) = path.adapter_identity(&adapter_key) else {
        return Err(PortError::Unavailable { source: None });
    };
    let port::ReadPath {
        after_hash,
        max_header_count,
        want_tree_aux,
    } = request;
    match tokio::time::timeout(
        ZAKURA_HEADER_SYNC_DRIVER_TIMEOUT,
        read_state
            .clone()
            .oneshot(zakura_state::ReadRequest::ReadRetainedHeaderPath {
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
        ))) => {
            let finalized_tree_aux = if want_tree_aux {
                finalized_tree_aux_for_page(
                    read_state,
                    &network,
                    page.common_ancestor,
                    page.headers.len(),
                )
                .await?
            } else {
                vec![None; page.headers.len()]
            };
            Ok(port::ReadPathReply::Page(Box::new(
                port::RetainedHeaderPathPage {
                    common_ancestor: page.common_ancestor,
                    target: page.target,
                    scope: page.scope,
                    headers: page.headers,
                    aux_deliveries: page.aux_deliveries,
                    finalized_tree_aux,
                    complete: page.complete,
                },
            )))
        }
        Ok(Ok(zakura_state::ReadResponse::RetainedHeaderPathPage(
            zakura_state::RetainedPathReadOutcome::Unavailable,
        ))) => Ok(port::ReadPathReply::Unavailable),
        Ok(Ok(_)) => Err(PortError::Unavailable { source: None }),
        Ok(Err(error)) => Err(PortError::Unavailable {
            source: Some(Arc::from(error)),
        }),
        Err(_) => Err(PortError::Timeout),
    }
}

async fn finalized_tree_aux_for_page<ReadState>(
    read_state: ReadState,
    network: &zakura_chain::parameters::Network,
    common_ancestor: zakura_header_chain::Frontier,
    header_count: usize,
) -> Result<Vec<Option<zakura_header_chain::TreeAuxRecordV1>>, PortError>
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
    let empty = || vec![None; header_count];
    let Ok(count) = u32::try_from(header_count) else {
        return Ok(empty());
    };
    if count == 0 {
        return Ok(Vec::new());
    }
    let Ok(start_height) = common_ancestor.height.next() else {
        return Ok(empty());
    };
    let Some(end_height) = start_height + i64::from(count.saturating_sub(1)) else {
        return Ok(empty());
    };

    let finalized_tip = match tokio::time::timeout(
        ZAKURA_HEADER_SYNC_DRIVER_TIMEOUT,
        read_state
            .clone()
            .oneshot(zakura_state::ReadRequest::FinalizedTip),
    )
    .await
    {
        Ok(Ok(zakura_state::ReadResponse::FinalizedTip(tip))) => tip,
        Ok(Ok(_)) => return Err(PortError::Unavailable { source: None }),
        Ok(Err(error)) => {
            return Err(PortError::Unavailable {
                source: Some(Arc::from(error)),
            })
        }
        Err(_) => return Err(PortError::Timeout),
    };
    if finalized_tip.is_none_or(|(height, _)| end_height > height) {
        return Ok(empty());
    }

    let roots = match tokio::time::timeout(
        ZAKURA_HEADER_SYNC_DRIVER_TIMEOUT,
        read_state.oneshot(zakura_state::ReadRequest::BlockRoots {
            start_height,
            count,
        }),
    )
    .await
    {
        Ok(Ok(zakura_state::ReadResponse::BlockRoots(roots))) => roots,
        Ok(Ok(_)) => return Err(PortError::Unavailable { source: None }),
        Ok(Err(error)) => {
            return Err(PortError::Unavailable {
                source: Some(Arc::from(error)),
            })
        }
        Err(_) => return Err(PortError::Timeout),
    };
    if !block_roots_cover_range(start_height, count, &roots) {
        return Ok(empty());
    }

    Ok(roots
        .into_iter()
        .map(|roots| Some(finalized_tree_aux_record(roots, network)))
        .collect())
}

fn finalized_tree_aux_record(
    roots: BlockCommitmentRoots,
    network: &zakura_chain::parameters::Network,
) -> zakura_header_chain::TreeAuxRecordV1 {
    use zakura_chain::parameters::NetworkUpgrade;

    let nu5_active = NetworkUpgrade::Nu5
        .activation_height(network)
        .is_some_and(|height| roots.height >= height);
    let nu6_3_active = NetworkUpgrade::Nu6_3
        .activation_height(network)
        .is_some_and(|height| roots.height >= height);

    zakura_header_chain::TreeAuxRecordV1 {
        height: roots.height,
        sapling_root: roots.sapling_root,
        orchard_root: if nu5_active {
            roots.orchard_root
        } else {
            zakura_chain::orchard::tree::NoteCommitmentTree::default().root()
        },
        ironwood_root: if nu6_3_active {
            roots.ironwood_root
        } else {
            zakura_chain::ironwood::tree::NoteCommitmentTree::default().root()
        },
        sapling_tx_count: roots.sapling_tx,
        orchard_tx_count: if nu5_active { roots.orchard_tx } else { 0 },
        ironwood_tx_count: if nu6_3_active { roots.ironwood_tx } else { 0 },
        auth_data_root: if nu5_active {
            roots.auth_data_root
        } else {
            [0; 32].into()
        },
    }
}

#[cfg(test)]
fn assemble_header_path_page(
    lease_id: u64,
    page: port::RetainedHeaderPathPage,
    requested_schema: AuxSchema,
) -> Option<HeaderPathPage> {
    if page.headers.len() != page.aux_deliveries.len()
        || page.headers.len() != page.finalized_tree_aux.len()
    {
        return None;
    }

    let tree_aux_schema = if requested_schema == AuxSchema::V1
        && page
            .aux_deliveries
            .iter()
            .zip(&page.finalized_tree_aux)
            .all(|(deliveries, finalized_tree_aux)| {
                finalized_tree_aux.is_some()
                    || selected_aux_delivery(deliveries, AuxSchema::V1).is_some()
            }) {
        AuxSchema::V1
    } else {
        AuxSchema::None
    };
    let entries = page
        .headers
        .into_iter()
        .zip(page.aux_deliveries)
        .zip(page.finalized_tree_aux)
        .map(|((header, deliveries), finalized_tree_aux)| {
            let delivery_schema =
                if tree_aux_schema == AuxSchema::V1 && finalized_tree_aux.is_none() {
                    AuxSchema::V1
                } else {
                    AuxSchema::None
                };
            let delivery = selected_aux_delivery(&deliveries, delivery_schema);
            HeaderEntry {
                header,
                body_size: delivery.map_or(0, |delivery| match delivery.body_size {
                    zakura_header_chain::BodySizeHint::Unknown => 0,
                    zakura_header_chain::BodySizeHint::Known(size) => size.get(),
                }),
                tree_aux: (tree_aux_schema == AuxSchema::V1)
                    .then(|| finalized_tree_aux.or_else(|| delivery.and_then(|item| item.tree_aux)))
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
    adapter_key: port::AdapterKey,
    path: port::RetainedHeaderPath,
) -> Result<(), PortError>
where
    ReadState: Service<
            zakura_state::ReadRequest,
            Response = zakura_state::ReadResponse,
            Error = zakura_state::BoxError,
        > + Send
        + 'static,
    ReadState::Future: Send + 'static,
{
    let Some((lease_id, source, session_id)) = path.adapter_identity(&adapter_key) else {
        return Err(PortError::Unavailable { source: None });
    };
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
        Ok(Err(error)) => Err(PortError::Unavailable {
            source: Some(Arc::from(error)),
        }),
        Err(_) => Err(PortError::Timeout),
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

    fn owner() -> zakura_header_chain::HeaderSyncWorkOwner {
        zakura_header_chain::HeaderWorkAuthority {
            header_generation: zakura_header_chain::HeaderGeneration::new(2),
            branch: zakura_header_chain::BranchId::new(block::Hash([1; 32]), block::Hash([2; 32])),
        }
        .bind(
            3,
            NonZeroU64::new(4).expect("the fixture request ID is nonzero"),
        )
        .into()
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

    #[tokio::test]
    async fn runtime_wait_uses_explicit_attachment_state_without_height_inference() {
        use zakura_node_services::sync_lifecycle::{
            HeaderRuntimeDetachedReason, HeaderRuntimeStatus, LifecycleEpoch,
        };

        let (_detached_tx, mut detached) =
            tokio::sync::watch::channel(HeaderRuntimeStatus::Detached {
                epoch: LifecycleEpoch::INITIAL,
                reason: HeaderRuntimeDetachedReason::AwaitingSemanticHandoff,
            });
        wait_for_header_runtime(&mut detached)
            .await
            .expect("detached checkpoint bootstrap starts without waiting");

        let (pending_tx, mut pending) =
            tokio::sync::watch::channel(HeaderRuntimeStatus::Detached {
                epoch: LifecycleEpoch::INITIAL,
                reason: HeaderRuntimeDetachedReason::AttachmentPending,
            });
        let pending_wait = wait_for_header_runtime(&mut pending);
        tokio::pin!(pending_wait);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut pending_wait)
                .await
                .is_err(),
            "a durable restart must not pass startup while attachment remains pending",
        );
        pending_tx
            .send(HeaderRuntimeStatus::Ready {
                epoch: LifecycleEpoch::new(1),
            })
            .expect("the pending runtime status receiver remains live");
        pending_wait
            .await
            .expect("the durable restart proceeds after explicit readiness");

        let (_failed_tx, mut failed) = tokio::sync::watch::channel(HeaderRuntimeStatus::Failed {
            epoch: LifecycleEpoch::new(1),
            error: "fixture reconstruction failed".into(),
        });
        assert!(wait_for_header_runtime(&mut failed).await.is_err());

        let (_ready_tx, mut ready) = tokio::sync::watch::channel(HeaderRuntimeStatus::Ready {
            epoch: LifecycleEpoch::new(1),
        });
        wait_for_header_runtime(&mut ready)
            .await
            .expect("an already-ready restart never waits");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn durable_snapshot_startup_wait_outlives_driver_request_deadline() {
        let (_status_sender, mut status) = tokio::sync::watch::channel(
            zakura_node_services::sync_lifecycle::HeaderRuntimeStatus::Reconstructing {
                epoch: zakura_node_services::sync_lifecycle::LifecycleEpoch::new(1),
                progress:
                    zakura_node_services::sync_lifecycle::HeaderReconstructionProgress::STARTING,
            },
        );

        let early_result = tokio::time::timeout(
            ZAKURA_HEADER_SYNC_DRIVER_TIMEOUT + std::time::Duration::from_secs(1),
            wait_for_header_runtime(&mut status),
        )
        .await;

        assert!(
            early_result.is_err(),
            "full-state reconstruction must not inherit the ordinary driver request deadline"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn attachment_pending_has_no_completion_deadline() {
        use zakura_node_services::sync_lifecycle::{
            HeaderRuntimeDetachedReason, HeaderRuntimeStatus, LifecycleEpoch,
        };

        let (status_sender, mut status) =
            tokio::sync::watch::channel(HeaderRuntimeStatus::Detached {
                epoch: LifecycleEpoch::new(1),
                reason: HeaderRuntimeDetachedReason::AttachmentPending,
            });
        let waiter = tokio::spawn(async move { wait_for_header_runtime(&mut status).await });

        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_secs(2 * 30 + 1)).await;
        assert!(
            !waiter.is_finished(),
            "periodic attachment diagnostics must not turn the startup wait into a failure"
        );
        status_sender
            .send(HeaderRuntimeStatus::Ready {
                epoch: LifecycleEpoch::new(1),
            })
            .expect("the attachment waiter remains subscribed");
        waiter
            .await
            .expect("the waiter task remains live")
            .expect("readiness completes the unbounded attachment wait");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn reconstruction_has_no_completion_deadline() {
        use zakura_node_services::sync_lifecycle::{HeaderRuntimeStatus, LifecycleEpoch};

        let (status_sender, mut status) =
            tokio::sync::watch::channel(HeaderRuntimeStatus::Reconstructing {
                epoch: LifecycleEpoch::new(1),
                progress:
                    zakura_node_services::sync_lifecycle::HeaderReconstructionProgress::STARTING,
            });
        let waiter = tokio::spawn(async move { wait_for_header_runtime(&mut status).await });

        tokio::time::advance(std::time::Duration::from_secs(16 * 60)).await;
        assert!(
            !waiter.is_finished(),
            "diagnostics must never abort reconstruction"
        );
        status_sender
            .send(HeaderRuntimeStatus::Ready {
                epoch: LifecycleEpoch::new(1),
            })
            .expect("the reconstruction waiter remains subscribed");
        waiter
            .await
            .expect("the waiter task remains live")
            .expect("readiness completes the unbounded wait");
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
            scope: owner().header_authority(),
            headers: vec![node.header],
            aux_deliveries: vec![Vec::new()],
            finalized_tree_aux: vec![None],
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
        page.finalized_tree_aux[0] = Some(tree_aux);
        let served_from_finalized_state = assemble_header_path_page(1, page.clone(), AuxSchema::V1)
            .expect("the coherent finalized-state page assembles");
        assert_eq!(served_from_finalized_state.tree_aux_schema, AuxSchema::V1);
        assert_eq!(served_from_finalized_state.entries[0].body_size, 0);
        assert_eq!(
            served_from_finalized_state.entries[0].tree_aux,
            Some(tree_aux)
        );
        page.finalized_tree_aux[0] = None;

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

    #[tokio::test]
    async fn finalized_pages_load_contiguous_tree_aux_from_state() {
        let roots = |height| BlockCommitmentRoots {
            height,
            sapling_root: Default::default(),
            orchard_root: Default::default(),
            ironwood_root: Default::default(),
            sapling_tx: u64::from(height.0),
            orchard_tx: 0,
            ironwood_tx: 0,
            auth_data_root: [9; 32].into(),
        };
        let read_state = tower::service_fn(move |request| async move {
            Ok::<_, zakura_state::BoxError>(match request {
                zakura_state::ReadRequest::FinalizedTip => {
                    zakura_state::ReadResponse::FinalizedTip(Some((
                        block::Height(2),
                        block::Hash([2; 32]),
                    )))
                }
                zakura_state::ReadRequest::BlockRoots {
                    start_height,
                    count,
                } => {
                    assert_eq!(start_height, block::Height(1));
                    assert_eq!(count, 2);
                    zakura_state::ReadResponse::BlockRoots(vec![
                        roots(block::Height(1)),
                        roots(block::Height(2)),
                    ])
                }
                request => panic!("unexpected finalized-page state request: {request:?}"),
            })
        });

        let records = finalized_tree_aux_for_page(
            read_state,
            &zakura_chain::parameters::Network::Mainnet,
            zakura_header_chain::Frontier::new(block::Height(0), block::Hash([0; 32])),
            2,
        )
        .await
        .expect("the finalized roots are available");

        assert_eq!(records.len(), 2);
        assert_eq!(
            records[0]
                .expect("height one has a finalized root record")
                .height,
            block::Height(1)
        );
        assert_eq!(
            records[1]
                .expect("height two has a finalized root record")
                .sapling_tx_count,
            2
        );
    }

    #[test]
    fn finalized_tree_aux_uses_empty_tree_roots_before_activation() {
        let orchard_root = zakura_chain::orchard::tree::NoteCommitmentTree::default().root();
        let ironwood_root = zakura_chain::ironwood::tree::NoteCommitmentTree::default().root();
        assert_ne!(orchard_root, Default::default());
        assert_ne!(ironwood_root, Default::default());

        let record = finalized_tree_aux_record(
            BlockCommitmentRoots {
                height: block::Height(1),
                sapling_root: Default::default(),
                orchard_root,
                ironwood_root,
                sapling_tx: 3,
                orchard_tx: 4,
                ironwood_tx: 5,
                auth_data_root: [9; 32].into(),
            },
            &zakura_chain::parameters::Network::Mainnet,
        );

        assert_eq!(record.sapling_tx_count, 3);
        assert_eq!(record.orchard_root, orchard_root);
        assert_eq!(record.orchard_tx_count, 0);
        assert_eq!(record.ironwood_root, ironwood_root);
        assert_eq!(record.ironwood_tx_count, 0);
        assert_eq!(record.auth_data_root, [0; 32].into());
        zakura_chain::parallel::commitment_aux_verify::verify_supplied_orchard_root_below_nu5(
            &zakura_chain::parameters::Network::Mainnet,
            record.height,
            &record.orchard_root,
        )
        .expect("the served pre-NU5 Orchard root passes native VCT verification");
        zakura_chain::parallel::commitment_aux_verify::verify_supplied_ironwood_root_below_nu6_3(
            &zakura_chain::parameters::Network::Mainnet,
            record.height,
            &record.ironwood_root,
        )
        .expect("the served pre-NU6.3 Ironwood root passes native VCT verification");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn wedged_state_read_returns_busy_at_the_driver_deadline() {
        let owner = owner();
        let peer = ZakuraPeerId::new(vec![7; 32]).expect("the peer identity has canonical width");
        let request = port::AcquirePath {
            source: source_id(&peer),
            session_id: owner.session_id(),
            scope: owner.header_authority(),
            target_tip_hash: owner.header_authority().branch.target_tip_hash,
            locator_hashes: vec![owner.header_authority().branch.anchor_hash],
        };
        let started = tokio::time::Instant::now();

        let result =
            acquire_header_path(pending_read_state(), port::AdapterKey::new(), request).await;

        assert!(matches!(result, Err(PortError::Timeout)));
        assert_eq!(
            tokio::time::Instant::now().duration_since(started),
            ZAKURA_HEADER_SYNC_DRIVER_TIMEOUT
        );
    }

    #[tokio::test]
    async fn retained_path_capability_authenticates_the_state_lease_id() {
        let lease_id = 42;
        let owner = owner();
        let source = zakura_header_chain::SourceId::from_digest([7; 32]);
        let target = zakura_header_chain::Frontier::new(
            block::Height(2),
            owner.header_authority().branch.target_tip_hash,
        );
        let common_ancestor = zakura_header_chain::Frontier::new(
            block::Height(1),
            owner.header_authority().branch.anchor_hash,
        );
        let acquire_request = port::AcquirePath {
            source,
            session_id: owner.session_id(),
            scope: owner.header_authority(),
            target_tip_hash: target.hash,
            locator_hashes: vec![common_ancestor.hash],
        };
        let adapter_key = port::AdapterKey::new();
        let acquired = acquire_header_path(
            tower::service_fn(move |request| {
                assert!(matches!(
                    request,
                    zakura_state::ReadRequest::AcquireRetainedHeaderPath { .. }
                ));
                let lease = zakura_state::RetainedPathLease {
                    lease_id,
                    peer: source,
                    session_id: owner.session_id(),
                    target,
                    common_ancestor,
                    scope: owner.header_authority(),
                    idle_deadline: tokio::time::Instant::now(),
                };
                async move {
                    Ok::<_, zakura_state::BoxError>(
                        zakura_state::ReadResponse::RetainedHeaderPathLease(
                            zakura_state::RetainedPathLeaseOutcome::Acquired(Box::new(lease)),
                        ),
                    )
                }
            }),
            adapter_key.clone(),
            acquire_request,
        )
        .await
        .expect("the fixture state service acquires a retained path");
        let port::AcquirePathReply::Acquired(path) = acquired else {
            panic!("the fixture state service grants the retained path");
        };
        assert_eq!(
            path.adapter_identity(&adapter_key),
            Some((lease_id, source, owner.session_id()))
        );

        let foreign_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls = foreign_calls.clone();
        let foreign_read = read_header_path(
            tower::service_fn(move |_: zakura_state::ReadRequest| {
                calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                async move {
                    Ok::<_, zakura_state::BoxError>(zakura_state::ReadResponse::HeaderLocator(None))
                }
            }),
            port::AdapterKey::new(),
            zakura_chain::parameters::Network::Mainnet,
            (*path).clone(),
            port::ReadPath {
                after_hash: common_ancestor.hash,
                max_header_count: 1,
                want_tree_aux: true,
            },
        )
        .await;
        assert!(matches!(
            foreign_read,
            Err(PortError::Unavailable { source: None })
        ));
        assert_eq!(
            foreign_calls.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "a foreign retained-path capability is rejected before a state read"
        );

        let read_path = (*path).clone();
        let read = read_header_path(
            tower::service_fn(move |request| {
                let zakura_state::ReadRequest::ReadRetainedHeaderPath {
                    lease_id: requested_lease_id,
                    ..
                } = request
                else {
                    panic!("the adapter reads through the retained-path request");
                };
                assert_eq!(requested_lease_id, lease_id);
                async move {
                    Ok::<_, zakura_state::BoxError>(
                        zakura_state::ReadResponse::RetainedHeaderPathPage(
                            zakura_state::RetainedPathReadOutcome::Unavailable,
                        ),
                    )
                }
            }),
            adapter_key.clone(),
            zakura_chain::parameters::Network::Mainnet,
            read_path,
            port::ReadPath {
                after_hash: common_ancestor.hash,
                max_header_count: 1,
                want_tree_aux: true,
            },
        )
        .await
        .expect("the fixture state service reads through the retained path");
        assert!(matches!(read, port::ReadPathReply::Unavailable));

        release_header_path(
            tower::service_fn(move |request| {
                let zakura_state::ReadRequest::ReleaseRetainedHeaderPath {
                    lease_id: released_lease_id,
                    ..
                } = request
                else {
                    panic!("the adapter releases through the retained-path request");
                };
                assert_eq!(released_lease_id, lease_id);
                async move {
                    Ok::<_, zakura_state::BoxError>(zakura_state::ReadResponse::HeaderLocator(None))
                }
            }),
            adapter_key,
            *path,
        )
        .await
        .expect("the fixture state service releases the retained path");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn header_target_apply_waits_past_diagnostic_intervals_for_terminal_result() {
        let started = tokio::time::Instant::now();
        let response = wait_for_header_target_apply(owner(), async {
            tokio::time::sleep(
                ZAKURA_HEADER_SYNC_DRIVER_TIMEOUT * 2 + std::time::Duration::from_secs(1),
            )
            .await;
            Ok(zakura_state::Response::HeaderChainInsertApplied(
                zakura_header_chain::ApplyResult::Committed,
            ))
        })
        .await
        .expect("the accepted apply returns its terminal result after two stall intervals");

        assert!(matches!(
            response,
            zakura_state::Response::HeaderChainInsertApplied(
                zakura_header_chain::ApplyResult::Committed
            )
        ));
        assert_eq!(
            tokio::time::Instant::now().duration_since(started),
            ZAKURA_HEADER_SYNC_DRIVER_TIMEOUT * 2 + std::time::Duration::from_secs(1)
        );
    }

    #[test]
    fn header_target_apply_failure_preserves_a_state_service_error() {
        let error = header_target_apply_failure(
            owner(),
            "state_error",
            Some("fixture state apply failure".into()),
        );

        assert_eq!(
            error.category,
            zakura_header_chain::ErrorCategory::LocalResourceOrStorage
        );
        assert_eq!(
            error
                .source
                .as_ref()
                .expect("the state source is retained")
                .to_string(),
            "fixture state apply failure"
        );
    }

    #[test]
    fn driver_preserves_every_header_preparation_failure_category() {
        let source = zakura_header_chain::SourceId::from_digest([5; 32]);
        let owner = owner();
        let header = regtest_genesis_block().header.clone();
        let entries = [port::TargetEntry {
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
