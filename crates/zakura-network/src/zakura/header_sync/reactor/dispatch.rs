//! Starting header storage calls and reporting how they end.

use super::*;

pub(super) type PendingPortOperation =
    Pin<Box<dyn Future<Output = StorageOutcome> + Send + 'static>>;

/// The result of one direct header storage operation, with the identity that
/// correlates it to the reactor's pending work.
#[derive(Debug)]
pub(super) enum StorageCompletion {
    /// A continuation-locator read settled.
    Locator {
        key: LocatorKey,
        /// The locator, or none when the read failed or timed out.
        locator: Option<zakura_header_chain::HeaderLocator>,
    },
    /// An external driver's continuation-locator read settled.
    ///
    /// The driver answers each `QueryHeaderLocator` action by its exact
    /// session, target tip, and scope. It bypasses [`state::LocatorQueries`] so that
    /// every request still reaches the driver as its own action.
    #[cfg(any(test, feature = "zakura-testkit"))]
    ExternalLocator {
        peer: ZakuraPeerId,
        session_id: u64,
        target_tip_hash: block::Hash,
        scope: zakura_header_chain::HeaderWorkAuthority,
        locator: Option<zakura_header_chain::HeaderLocator>,
    },
    /// A VCT repair context read settled.
    VctRepairContext {
        owner: zakura_header_chain::BodyWorkOwner,
        result: VctRepairContextResult,
    },
    /// Target preparation settled.
    TargetPrepared {
        peer: ZakuraPeerId,
        source: zakura_header_chain::SourceId,
        owner: zakura_header_chain::HeaderSyncWorkOwner,
        result: HeaderTargetPreparationResult,
    },
    /// Target application settled.
    TargetAdmission {
        peer: ZakuraPeerId,
        source: zakura_header_chain::SourceId,
        owner: zakura_header_chain::HeaderSyncWorkOwner,
        result: HeaderTargetAdmissionResult,
    },
}

/// How one direct header storage future ended.
pub(super) enum StorageOutcome {
    /// The operation returned a result.
    Completed {
        /// The accepted call this result settles.
        call: StorageCall,
        completion: Box<StorageCompletion>,
        /// Header capacity the operation holds until the reactor handles its result.
        retained: Option<RetainedHeaders>,
    },
    /// The operation panicked.
    Panicked(Box<PortPanicContext>),
}

/// How the reactor handled a request to run one header storage operation.
#[must_use]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum StorageDispatch {
    /// This call started, or entered the external driver's queue.
    Started(StorageCall),
    /// This locator read in flight also serves the request.
    Joined(StorageCall),
    /// Locator capacity refused this call, so the peer's offer waits for a permit.
    Deferred(StorageCall),
    /// The operation needs no storage work, so the reactor only recorded it.
    Recorded,
    /// This call cannot run. A record-only action has no call.
    Refused(Option<StorageCall>),
}

impl StorageDispatch {
    /// Whether the operation cannot run, so its caller must retire the work.
    pub(super) fn is_refused(self) -> bool {
        matches!(self, Self::Refused(_))
    }
}

#[derive(Clone, Debug)]
pub(super) struct PortPanicContext {
    operation: &'static str,
    peer: Option<ZakuraPeerId>,
    session_id: Option<u64>,
    session: Option<PeerSession>,
    owner: Option<zakura_header_chain::HeaderSyncWorkOwner>,
    target_tip_hash: Option<block::Hash>,
    /// The pending locator read that a panicked locator operation leaves behind.
    locator: Option<LocatorKey>,
    /// The accepted call that panicked.
    pub(super) call: StorageCall,
}

/// Record that `retained`, if a call holds any, ends as `end`.
pub(super) fn end_retained(retained: &mut Option<RetainedHeaders>, end: HeaderWorkEnd) {
    if let Some(retained) = retained {
        retained.end_as(end);
    }
}

impl HeaderSyncReactor {
    pub(super) fn dispatch_action(&mut self, action: HeaderPortOperation) -> StorageDispatch {
        self.dispatch_holding(action, None)
    }

    /// Dispatch `action`. A prepared target's apply takes `held` from its prepare.
    /// Any other prepare or apply retains its peer's current round.
    pub(super) fn dispatch_holding(
        &mut self,
        action: HeaderPortOperation,
        held: Option<RetainedHeaders>,
    ) -> StorageDispatch {
        match self.startup.port_dispatch {
            PortDispatch::Direct => self.dispatch_direct_port_operation(action, held),
            #[cfg(any(test, feature = "zakura-testkit"))]
            PortDispatch::External => {
                // An external driver's calls hold no lease, and its results carry no
                // retained headers, so `held` is always empty here.
                debug_assert!(held.is_none(), "an external prepare retains no headers");
                self.dispatch_external_port_operation(action)
            }
        }
    }

    pub(super) fn dispatch_direct_port_operation(
        &mut self,
        action: HeaderPortOperation,
        mut held: Option<RetainedHeaders>,
    ) -> StorageDispatch {
        use zakura_node_services::header_chain as port;

        let Some(kind) = StorageCallKind::of(&action) else {
            match action {
                HeaderPortOperation::Misbehavior { peer, reason } => {
                    tracing::debug!(?peer, ?reason, "recorded Zakura header-sync peer violation");
                }
                HeaderPortOperation::DropPeer {
                    peer,
                    session_id,
                    reason,
                } => {
                    tracing::debug!(
                        ?peer,
                        session_id,
                        reason,
                        "dropped an unproductive Zakura header-sync peer"
                    );
                }
                _ => {}
            }
            return StorageDispatch::Recorded;
        };
        let vct_local_operation = VctLocalOperation::from_action(&action, Instant::now());
        if vct_local_operation.is_some() && self.state.vct_local_operation.is_some() {
            tracing::error!(
                current = ?self.state.vct_local_operation,
                attempted = ?vct_local_operation,
                "refused to replace an owned VCT local operation"
            );
            let call = self.state.calls.propose(kind);
            observe!(
                self,
                ReactorEvent::StorageRefused {
                    call: call.number,
                    kind,
                    cause: observe::RefusalCause::LocalOperationBusy,
                }
            );
            end_retained(&mut held, HeaderWorkEnd::Refused);
            return StorageDispatch::Refused(Some(call));
        }
        let (locator_key, locator_permit) = if let HeaderPortOperation::QueryHeaderLocator {
            peer,
            scope,
            #[cfg(any(test, zakura_sim))]
            session_id,
            #[cfg(any(test, zakura_sim))]
            target_tip_hash,
            ..
        } = &action
        {
            let key = match self
                .state
                .locators
                .propose(peer, *scope, &mut self.state.calls)
            {
                LocatorProposal::Start(key) => key,
                LocatorProposal::Joined(call) => {
                    observe!(self, ReactorEvent::StorageJoined { call: call.number });
                    return StorageDispatch::Joined(call);
                }
            };
            let Ok(permit) = self.locator_capacity.clone().try_acquire_owned() else {
                self.state.refuse_locator(&key);
                self.locator_capacity_waiting = true;
                observe!(
                    self,
                    ReactorEvent::StorageRefused {
                        call: key.call.number,
                        kind,
                        cause: observe::RefusalCause::LocatorCapacity,
                    }
                );
                observe!(
                    self,
                    ReactorEvent::OfferWaiting {
                        peer: peer.clone(),
                        session_id: *session_id,
                        target_tip_hash: *target_tip_hash,
                        cause: observe::WaitCause::LocatorCapacity,
                    }
                );
                self.peer_work_queue.remove_unstarted(peer);
                return StorageDispatch::Deferred(key.call);
            };
            (Some(key), Some(port::LocatorReadPermit::new(permit)))
        } else {
            (None, None)
        };
        let call = locator_key
            .as_ref()
            .map_or_else(|| self.state.calls.propose(kind), |key| key.call);
        let mut retained = match &action {
            HeaderPortOperation::PrepareHeaderTarget { peer, .. }
            | HeaderPortOperation::ApplyHeaderTarget { peer, .. } => {
                held.or_else(|| self.peer_work_queue.retain_headers(peer))
            }
            _ => None,
        };
        #[cfg(any(test, zakura_sim))]
        if let Some(retained) = &mut retained {
            retained.observed_by(&self.startup.observer);
        }
        let panic_context = self.port_panic_context(&action, locator_key.clone(), call);
        #[cfg(any(test, zakura_sim))]
        let (tap, dispatched) = self.begin_port_call(&action, call);
        #[cfg(any(test, zakura_sim))]
        let (panic_tap, pending_tap) = (tap.clone(), tap.clone());
        let header_chain = self.startup.header_chain_port.clone();
        let request_timeout = self.startup.request_timeout;
        let operation: Pin<Box<dyn Future<Output = StorageCompletion> + Send + 'static>> =
            match action {
                HeaderPortOperation::Misbehavior { .. } | HeaderPortOperation::DropPeer { .. } => {
                    unreachable!("a record-only action returned before its storage call")
                }
                HeaderPortOperation::QueryHeaderLocator {
                    peer,
                    session_id: _,
                    target_tip_hash: _,
                    scope: _,
                } => Box::pin(async move {
                    #[cfg(any(test, zakura_sim))]
                    let end;
                    let locator = match zakura_rt::time::timeout(
                        request_timeout,
                        header_chain.continuation_locator(
                            locator_permit
                                .as_ref()
                                .expect("locator dispatch acquired capacity")
                                .clone(),
                        ),
                    )
                    .await
                    {
                        Ok(Ok(locator)) => {
                            #[cfg(any(test, zakura_sim))]
                            {
                                end = observe::CallEnd::Replied;
                            }
                            locator
                        }
                        Ok(Err(error)) => {
                            #[cfg(any(test, zakura_sim))]
                            {
                                end = observe::CallEnd::PortError;
                            }
                            tracing::debug!(?peer, ?error, "header locator unavailable");
                            None
                        }
                        Err(_) => {
                            #[cfg(any(test, zakura_sim))]
                            {
                                end = observe::CallEnd::TimedOut;
                            }
                            tracing::debug!(?peer, "header locator query timed out");
                            None
                        }
                    };
                    #[cfg(any(test, zakura_sim))]
                    tap.end(end);
                    drop(locator_permit);
                    StorageCompletion::Locator {
                        key: locator_key.expect("locator dispatch recorded its read"),
                        locator,
                    }
                }),
                HeaderPortOperation::QueryVctRepairContext { owner, height } => {
                    Box::pin(async move {
                        #[cfg(any(test, zakura_sim))]
                        let end;
                        let result = match zakura_rt::time::timeout(
                            request_timeout,
                            header_chain.vct_repair_context(owner, height),
                        )
                        .await
                        {
                            Ok(Ok(port::VctRepairContextReply::Resolved(context))) => {
                                #[cfg(any(test, zakura_sim))]
                                {
                                    end = observe::CallEnd::Replied;
                                }
                                VctRepairContextResult::Resolved(context)
                            }
                            Ok(Ok(port::VctRepairContextReply::Stale)) => {
                                #[cfg(any(test, zakura_sim))]
                                {
                                    end = observe::CallEnd::Replied;
                                }
                                VctRepairContextResult::Stale
                            }
                            Ok(Err(error)) => {
                                #[cfg(any(test, zakura_sim))]
                                {
                                    end = observe::CallEnd::PortError;
                                }
                                tracing::debug!(?owner, ?error, "VCT repair context unavailable");
                                VctRepairContextResult::Unavailable
                            }
                            Err(_) => {
                                #[cfg(any(test, zakura_sim))]
                                {
                                    end = observe::CallEnd::TimedOut;
                                }
                                tracing::debug!(?owner, "VCT repair context timed out");
                                VctRepairContextResult::Unavailable
                            }
                        };
                        #[cfg(any(test, zakura_sim))]
                        tap.end(end);
                        StorageCompletion::VctRepairContext { owner, result }
                    })
                }
                HeaderPortOperation::PrepareHeaderTarget {
                    purpose,
                    peer,
                    source,
                    owner,
                    common_ancestor,
                    target,
                    completion,
                    entries,
                } => Box::pin(async move {
                    if matches!(
                        &purpose,
                        HeaderTargetPurpose::SelectedAuxiliaryRepair { .. }
                    ) && !matches!(
                        (&purpose, completion),
                        (
                            HeaderTargetPurpose::SelectedAuxiliaryRepair {
                                selected_target: purpose_target,
                                ..
                            },
                            zakura_header_chain::TargetCompletion::SelectedAuxiliaryRepair {
                                selected_target,
                                ..
                            },
                        ) if *purpose_target == target
                            && selected_target == target
                            && !entries.is_empty()
                            && entries.iter().all(|entry| entry.tree_aux.is_some())
                            && entries.last().is_some_and(|entry| entry.header.hash() == target.hash)
                    ) {
                        let result = HeaderTargetPreparationResult::Failed(std::sync::Arc::new(
                            zakura_header_chain::HeaderChainError::stale_target(
                                zakura_header_chain::ErrorSubject::Branch(
                                    owner.header_authority().branch,
                                ),
                            ),
                        ));
                        #[cfg(any(test, zakura_sim))]
                        tap.end(observe::CallEnd::NotSubmitted);
                        return StorageCompletion::TargetPrepared {
                            peer,
                            source,
                            owner,
                            result,
                        };
                    }
                    let entries = match port::TargetEntries::try_from(
                        entries
                            .into_iter()
                            .map(port_header_entry)
                            .collect::<Vec<_>>(),
                    ) {
                        Ok(entries) => entries,
                        Err(error) => {
                            let result =
                                HeaderTargetPreparationResult::Failed(std::sync::Arc::new(
                                    zakura_header_chain::HeaderChainError::malformed_protocol(
                                        zakura_header_chain::ErrorSubject::Request {
                                            source,
                                            request_id: owner.request_id(),
                                        },
                                        zakura_header_chain::RuleId::new("LC-WIRE-08"),
                                        source,
                                        Some(Box::new(error)),
                                    ),
                                ));
                            #[cfg(any(test, zakura_sim))]
                            tap.end(observe::CallEnd::NotSubmitted);
                            return StorageCompletion::TargetPrepared {
                                peer,
                                source,
                                owner,
                                result,
                            };
                        }
                    };
                    let result = header_chain
                        .prepare_header_target(port::PrepareHeaderTarget {
                            source,
                            owner,
                            common_ancestor,
                            target,
                            entries,
                            completion,
                        })
                        .await
                        .map(HeaderTargetPreparationResult::Prepared)
                        .unwrap_or_else(HeaderTargetPreparationResult::Failed);
                    #[cfg(any(test, zakura_sim))]
                    tap.end(observe::CallEnd::Replied);
                    StorageCompletion::TargetPrepared {
                        peer,
                        source,
                        owner,
                        result,
                    }
                }),
                HeaderPortOperation::ApplyHeaderTarget {
                    purpose: _,
                    peer,
                    source,
                    owner,
                    target,
                } => Box::pin(async move {
                    let result = header_chain
                        .apply_header_target(target)
                        .await
                        .map(|outcome| match outcome {
                            port::ApplyHeaderTargetOutcome::Applied => {
                                HeaderTargetAdmissionResult::Applied
                            }
                            port::ApplyHeaderTargetOutcome::ResourceStalled(receipt) => {
                                HeaderTargetAdmissionResult::ResourceStalled(receipt)
                            }
                        })
                        .unwrap_or_else(HeaderTargetAdmissionResult::Failed);
                    #[cfg(any(test, zakura_sim))]
                    tap.end(observe::CallEnd::Replied);
                    StorageCompletion::TargetAdmission {
                        peer,
                        source,
                        owner,
                        result,
                    }
                }),
            };
        #[cfg(any(test, zakura_sim))]
        if let Some(op) = dispatched {
            observe!(
                self,
                ReactorEvent::Dispatched {
                    call: call.number,
                    op
                }
            );
        }
        if let Some(retained) = &mut retained {
            retained.enter_call(call);
            let work = retained.work().expect("the call was just entered");
            let peer = retained.peer().clone();
            self.peer_work_queue.bind_call(&peer, work);
        }
        let operation = AssertUnwindSafe(operation)
            .catch_unwind()
            .map(move |result| {
                #[cfg(any(test, zakura_sim))]
                let result = observe::rethrow_observer_panic(result);
                match result {
                    Ok(completion) => StorageOutcome::Completed {
                        call,
                        completion: Box::new(completion),
                        retained,
                    },
                    Err(_) => {
                        // Report the panic before this closure drops `retained`,
                        // whose last lease release can reach a scheduling point.
                        #[cfg(any(test, zakura_sim))]
                        panic_tap.end_after_catch(observe::CallEnd::PortPanicked);
                        end_retained(&mut retained, HeaderWorkEnd::Panicked);
                        drop(retained);
                        StorageOutcome::Panicked(Box::new(panic_context))
                    }
                }
            });
        if let Some(operation) = vct_local_operation {
            self.state.vct_local_operation = Some(operation);
        }
        self.state.calls.accept(call);
        #[cfg(any(test, zakura_sim))]
        let operation = observe::PendingCall::new(operation, pending_tap);
        self.pending_port_operations.push(Box::pin(operation));
        StorageDispatch::Started(call)
    }

    /// Returns the tap `call`'s future reports through and, when an observer is
    /// installed, the call for its `Dispatched` event.
    #[cfg(any(test, zakura_sim))]
    pub(super) fn begin_port_call(
        &self,
        action: &HeaderPortOperation,
        call: StorageCall,
    ) -> (observe::PortTap, Option<observe::PortCall>) {
        let dispatched = self
            .startup
            .observer
            .0
            .as_ref()
            .and_then(|_| observe::PortCall::of(action));
        (
            observe::PortTap::new(call.number, &self.startup.observer),
            dispatched,
        )
    }

    #[cfg(any(test, feature = "zakura-testkit"))]
    pub(super) fn dispatch_external_port_operation(
        &mut self,
        action: HeaderPortOperation,
    ) -> StorageDispatch {
        let call = StorageCallKind::of(&action).map(|kind| self.state.calls.propose(kind));
        match self.actions.try_send(action) {
            Ok(()) => call.map_or(StorageDispatch::Recorded, StorageDispatch::Started),
            Err(error) => {
                tracing::debug!(?error, "header-sync action queue unavailable");
                #[cfg(any(test, zakura_sim))]
                if let Some(call) = call {
                    observe!(
                        self,
                        ReactorEvent::StorageRefused {
                            call: call.number,
                            kind: call.kind,
                            cause: observe::RefusalCause::ExternalQueue,
                        }
                    );
                }
                StorageDispatch::Refused(call)
            }
        }
    }

    pub(super) fn port_panic_context(
        &self,
        action: &HeaderPortOperation,
        locator: Option<LocatorKey>,
        call: StorageCall,
    ) -> PortPanicContext {
        let (operation, peer, session_id, owner, target_tip_hash) = match action {
            HeaderPortOperation::QueryHeaderLocator {
                peer,
                session_id,
                target_tip_hash,
                ..
            } => (
                "query_header_locator",
                Some(peer.clone()),
                Some(*session_id),
                None,
                Some(*target_tip_hash),
            ),
            HeaderPortOperation::QueryVctRepairContext { owner, .. } => (
                "vct_repair_context",
                None,
                None,
                Some((*owner).into()),
                None,
            ),
            HeaderPortOperation::PrepareHeaderTarget {
                peer,
                owner,
                target,
                ..
            } => (
                "prepare_header_target",
                Some(peer.clone()),
                Some(owner.session_id()),
                Some(*owner),
                Some(target.hash),
            ),
            HeaderPortOperation::ApplyHeaderTarget { peer, owner, .. } => (
                "apply_header_target",
                Some(peer.clone()),
                Some(owner.session_id()),
                Some(*owner),
                Some(owner.header_authority().branch.target_tip_hash),
            ),
            HeaderPortOperation::Misbehavior { peer, .. } => {
                ("misbehavior", Some(peer.clone()), None, None, None)
            }
            HeaderPortOperation::DropPeer {
                peer, session_id, ..
            } => (
                "drop_peer",
                Some(peer.clone()),
                Some(*session_id),
                None,
                None,
            ),
        };
        let session = peer.as_ref().and_then(|peer| {
            self.sessions.get(peer).and_then(|handles| {
                (session_id == Some(handles.session.session_id())).then(|| handles.session.clone())
            })
        });
        PortPanicContext {
            operation,
            peer,
            session_id,
            session,
            owner,
            target_tip_hash,
            locator,
            call,
        }
    }

    pub(super) fn handle_port_panic(&mut self, context: PortPanicContext) {
        if let Some(owner) = context.owner {
            if let Some(phase) = self
                .state
                .vct_local_operation
                .as_ref()
                .filter(|operation| operation.owner == owner)
                .map(|operation| operation.phase)
            {
                self.finish_vct_local_operation(owner, phase);
            }
        }
        metrics::counter!(
            "sync.header.port.panicked",
            "operation" => context.operation
        )
        .increment(1);

        self.startup.trace.emit_with(HEADER_SYNC_TABLE, |row| {
            row.insert(
                hs_trace::EVENT.into(),
                hs_trace::HEADER_PEER_VIOLATION.into(),
            );
            row.insert(
                hs_trace::PEER.into(),
                context
                    .peer
                    .as_ref()
                    .map_or(serde_json::Value::Null, |peer| {
                        trace_peer_label(peer).into()
                    }),
            );
            row.insert(
                hs_trace::SESSION_ID.into(),
                context
                    .session_id
                    .map_or(serde_json::Value::Null, Into::into),
            );
            row.insert(
                hs_trace::DIRECTION.into(),
                context
                    .session
                    .as_ref()
                    .map(|session| header_direction_label(session.direction()))
                    .map_or(serde_json::Value::Null, Into::into),
            );
            row.insert(hs_trace::REASON.into(), "port_future_panic".into());
            row.insert(hs_trace::BOUNDARY.into(), "port".into());
            row.insert(
                hs_trace::DISPOSITION.into(),
                if context.session.is_some() {
                    "disconnect"
                } else {
                    "record"
                }
                .into(),
            );
            row.insert(hs_trace::OPERATION.into(), context.operation.into());
        });

        if let Some(key) = context.locator.as_ref() {
            let _ = self.state.locators.remove_exact(key);
        }

        if let Some(session) = context.session.as_ref() {
            session.disconnect_for_port_panic();
        }

        if context.operation == "vct_repair_context" {
            if let Some(owner) = context.owner {
                if let Some(owner) = owner.body_owner() {
                    self.handle_vct_repair_context_ready(
                        owner,
                        VctRepairContextResult::Unavailable,
                    );
                }
            }
            return;
        }

        let Some(peer) = context.peer.as_ref() else {
            return;
        };
        let source = source_id_from_peer(peer);
        let subject = context.owner.map_or_else(
            || {
                context.target_tip_hash.map_or(
                    zakura_header_chain::ErrorSubject::Local("header_sync_port"),
                    |hash| {
                        zakura_header_chain::ErrorSubject::Header(
                            zakura_header_chain::HeaderId::new(hash),
                        )
                    },
                )
            },
            |owner| zakura_header_chain::ErrorSubject::Branch(owner.header_authority().branch),
        );
        let failure = zakura_header_chain::HeaderChainError::local_resource(subject, None);
        self.handle_typed_failure(peer.clone(), source, None, &failure);

        if context.operation == "query_header_locator" {
            self.peer_work_queue.remove_unstarted(peer);
        }

        if let Some(owner) = context.owner {
            let matching = self
                .peer_work_queue
                .active(peer)
                .is_some_and(|active| active.owner == owner);
            if matching {
                let repair = self.peer_work_queue.active(peer).and_then(|active| {
                    matches!(
                        active.purpose,
                        HeaderTargetPurpose::SelectedAuxiliaryRepair { .. }
                    )
                    .then(|| {
                        (
                            active
                                .owner
                                .body_owner()
                                .expect("an auxiliary repair has body authority"),
                            active.source,
                        )
                    })
                });
                if let Some((owner, source)) = repair {
                    self.retry_vct_repair(
                        owner,
                        VctRepairRetry::local(source, HeaderRequestTerminal::LocalError),
                    );
                } else {
                    self.retire_peer_work(peer, HeaderRequestTerminal::LocalError);
                }
            }
        }
    }
}
