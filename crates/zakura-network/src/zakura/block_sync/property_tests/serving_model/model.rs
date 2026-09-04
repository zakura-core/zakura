//! Independent logical model for block-range serving.
//!
//! The model tracks only contract-visible peer sessions, Status admission,
//! request ownership, completions, and expected output. It does not call the
//! production reactor, registry, serving ledger, or wire codec.

use std::collections::{BTreeMap, BTreeSet};

use super::super::super::super::{
    BlockRangeRequestId, BlockSyncMisbehavior, MAX_BS_RESPONSE_BYTES,
};
use super::{
    ByteCap, CompletionKind, CompletionTarget, DisconnectWhich, ExpectedAction,
    ExpectedObservation, PendingQuery, QueryClass, QuerySelector, ServingCoverage, ServingEvidence,
    ServingFrame, SessionKey, StatusValidity, LOGICAL_PEER_COUNT,
};
use crate::zakura::{ServicePeerDirection, ServicePeerLimits, ServicePeerSnapshot};
use zakura_chain::block;

/// Contract state owned by one logical peer identity.
#[derive(Clone, Debug)]
struct PeerModel {
    next_conn_id: u64,
    current: Option<SessionKey>,
    received_status: bool,
    ledger: Vec<usize>,
}

impl Default for PeerModel {
    fn default() -> Self {
        Self {
            next_conn_id: 1,
            current: None,
            received_status: false,
            ledger: Vec::new(),
        }
    }
}

/// Admission and cancellation state retained for every created connection.
#[derive(Clone, Debug)]
struct SessionModel {
    cancelled: bool,
    admitted: bool,
}

/// Whether a query can still affect serving output.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum QueryState {
    Live,
    Retired,
    Orphaned,
}

/// Model-owned request metadata, with an ID bound from production later.
#[derive(Clone, Debug)]
struct QueryModel {
    id: Option<BlockRangeRequestId>,
    session: SessionKey,
    peer: u8,
    start: block::Height,
    requested: u32,
    state: QueryState,
    finished_unavailable: bool,
}

/// State-driver block metadata needed to predict response framing.
#[derive(Clone, Debug)]
pub(super) struct ReadyBlock {
    pub(super) height: block::Height,
    pub(super) hash: block::Hash,
    pub(super) size: usize,
}

/// Minimal executable specification for GetBlocks serving behavior.
///
/// Transitions are deliberately independent of production helpers. Each one
/// predicts only externally visible output and updates coverage for the input
/// class it exercised.
#[derive(Debug)]
pub(super) struct ReferenceModel {
    peers: Vec<PeerModel>,
    sessions: BTreeMap<SessionKey, SessionModel>,
    queries: Vec<QueryModel>,
    max_inflight: usize,
    max_blocks: u32,
    max_response_bytes: u64,
    direction: ServicePeerDirection,
    max_peers: usize,
    servable_high: block::Height,
    overlapping_reconnect_status: Vec<Option<bool>>,
    step_evidence: BTreeSet<ServingEvidence>,
    coverage: ServingCoverage,
}

impl ReferenceModel {
    /// Initialize an empty logical node with the generated serving limits.
    pub(super) fn new(
        max_inflight: u32,
        max_blocks: u32,
        max_response_bytes: u32,
        direction: ServicePeerDirection,
        max_peers: usize,
        servable_high: block::Height,
    ) -> Self {
        Self {
            peers: (0..LOGICAL_PEER_COUNT)
                .map(|_| PeerModel::default())
                .collect(),
            sessions: BTreeMap::new(),
            queries: Vec::new(),
            max_inflight: usize::try_from(max_inflight)
                .expect("the configured u32 serving cap fits usize"),
            max_blocks,
            max_response_bytes: u64::from(max_response_bytes),
            direction,
            max_peers,
            servable_high,
            overlapping_reconnect_status: vec![None; usize::from(LOGICAL_PEER_COUNT)],
            step_evidence: BTreeSet::new(),
            coverage: ServingCoverage {
                cases: 1,
                ..ServingCoverage::default()
            },
        }
    }

    /// Map arbitrary generated peer bytes into the fixed logical peer set.
    fn peer_index(peer: u8) -> usize {
        usize::from(peer % LOGICAL_PEER_COUNT)
    }

    /// Start a new settled interval and record whether it contains overlap.
    pub(super) fn record_step(&mut self, operation_count: usize) {
        self.overlapping_reconnect_status.fill(None);
        self.step_evidence.clear();
        self.coverage.steps = self.coverage.steps.saturating_add(1);
        self.coverage.operations = self
            .coverage
            .operations
            .saturating_add(u64::try_from(operation_count).unwrap_or(u64::MAX));
        if operation_count > 1 {
            self.coverage.multi_operation_steps =
                self.coverage.multi_operation_steps.saturating_add(1);
        }
    }

    /// Create the next connection generation, applying replacement and peer-cap
    /// rules before returning its stable session key.
    pub(super) fn connect(&mut self, peer: u8) -> (SessionKey, bool) {
        let peer = peer % LOGICAL_PEER_COUNT;
        let index = Self::peer_index(peer);
        let conn_id = self.peers[index].next_conn_id;
        self.peers[index].next_conn_id = conn_id.saturating_add(1);
        let key = SessionKey { peer, conn_id };

        let replacement = self.peers[index].current;
        let reconnect_after_unsettled_disconnect =
            self.overlapping_reconnect_status[index].is_some();
        let replacement_received_status = (replacement.is_some()
            && self.peers[index].received_status)
            || self.overlapping_reconnect_status[index]
                .take()
                .unwrap_or(false);
        if let Some(previous) = replacement {
            self.coverage.replacement_sessions =
                self.coverage.replacement_sessions.saturating_add(1);
            if let Some(session) = self.sessions.get_mut(&previous) {
                session.cancelled = true;
                session.admitted = false;
            }
            self.orphan_peer_queries(peer);
        }

        let current_count = self
            .peers
            .iter()
            .filter(|peer| peer.current.is_some())
            .count();
        let admitted = replacement.is_some() || current_count < self.max_peers;
        self.sessions.insert(
            key,
            SessionModel {
                cancelled: !admitted,
                admitted,
            },
        );
        if admitted {
            self.peers[index].current = Some(key);
            self.peers[index].received_status = replacement_received_status;
            self.peers[index].ledger.clear();
            self.coverage.connected_sessions = self.coverage.connected_sessions.saturating_add(1);
        } else {
            self.coverage.rejected_sessions = self.coverage.rejected_sessions.saturating_add(1);
            if self.direction == ServicePeerDirection::Inbound {
                self.mark_evidence(ServingEvidence::InboundCapRejected);
            }
        }

        if replacement.is_some() {
            self.mark_evidence(ServingEvidence::ReplacementCancelled);
        }
        if reconnect_after_unsettled_disconnect {
            self.mark_evidence(ServingEvidence::ReactorStaleDisconnectIgnored);
        }

        (key, admitted)
    }

    /// Apply a current or stale transport removal and return the connection ID
    /// that the real service should receive.
    pub(super) fn disconnect(&mut self, peer: u8, which: DisconnectWhich) -> Option<u64> {
        let peer = peer % LOGICAL_PEER_COUNT;
        let index = Self::peer_index(peer);
        match which {
            DisconnectWhich::Current => {
                let current = self.peers[index].current.take()?;
                self.overlapping_reconnect_status[index] = Some(self.peers[index].received_status);
                self.coverage.current_disconnects =
                    self.coverage.current_disconnects.saturating_add(1);
                self.peers[index].received_status = false;
                self.peers[index].ledger.clear();
                if let Some(session) = self.sessions.get_mut(&current) {
                    session.cancelled = true;
                    session.admitted = false;
                }
                self.orphan_peer_queries(peer);
                Some(current.conn_id)
            }
            DisconnectWhich::Stale => {
                let stale = self
                    .sessions
                    .keys()
                    .rev()
                    .find(|session| {
                        session.peer == peer && Some(**session) != self.peers[index].current
                    })
                    .map(|session| session.conn_id)
                    .or_else(|| {
                        self.peers[index]
                            .current
                            .map(|session| session.conn_id.saturating_sub(1))
                    });
                if stale.is_some() {
                    self.coverage.stale_disconnects =
                        self.coverage.stale_disconnects.saturating_add(1);
                    self.mark_evidence(ServingEvidence::ServiceStaleDisconnectIgnored);
                }
                stale
            }
        }
    }

    /// Cancel the current session token, modeling teardown-driven disconnect.
    pub(super) fn cancel(&mut self, peer: u8) -> Option<SessionKey> {
        let peer = peer % LOGICAL_PEER_COUNT;
        let index = Self::peer_index(peer);
        let current = self.peers[index].current.take()?;
        self.overlapping_reconnect_status[index] = Some(self.peers[index].received_status);
        self.coverage.token_cancellations = self.coverage.token_cancellations.saturating_add(1);
        self.peers[index].received_status = false;
        self.peers[index].ledger.clear();
        if let Some(session) = self.sessions.get_mut(&current) {
            session.cancelled = true;
            session.admitted = false;
        }
        self.orphan_peer_queries(peer);
        Some(current)
    }

    /// Return whether a newly connected session should receive initial Status.
    pub(super) fn session_is_current_and_admitted(&self, session: SessionKey) -> bool {
        self.peers[Self::peer_index(session.peer)].current == Some(session)
            && self
                .sessions
                .get(&session)
                .is_some_and(|state| state.admitted)
    }

    /// Model Status handling and any expected misbehavior report.
    pub(super) fn status(
        &mut self,
        peer: u8,
        validity: StatusValidity,
    ) -> (Option<SessionKey>, ExpectedObservation) {
        let peer = peer % LOGICAL_PEER_COUNT;
        let Some(session) = self.peers[Self::peer_index(peer)].current else {
            return (None, ExpectedObservation::default());
        };

        let mut expected = ExpectedObservation::default();
        match validity {
            StatusValidity::Valid => {
                self.peers[Self::peer_index(peer)].received_status = true;
                self.coverage.valid_statuses = self.coverage.valid_statuses.saturating_add(1);
            }
            StatusValidity::InvalidRange => {
                expected.actions.push(ExpectedAction::Misbehavior {
                    peer,
                    reason: BlockSyncMisbehavior::InvalidStatus,
                });
                self.coverage.invalid_statuses = self.coverage.invalid_statuses.saturating_add(1);
            }
        }

        (Some(session), expected)
    }

    /// Model request admission, per-peer saturation, range clamping, and the
    /// expected query or terminal response.
    pub(super) fn get_blocks(
        &mut self,
        peer: u8,
        start: u32,
        count: u32,
    ) -> (
        Option<SessionKey>,
        ExpectedObservation,
        Option<PendingQuery>,
    ) {
        let peer = peer % LOGICAL_PEER_COUNT;
        let index = Self::peer_index(peer);
        let Some(session) = self.peers[index].current else {
            return (None, ExpectedObservation::default(), None);
        };
        let start = block::Height(start);
        let mut expected = ExpectedObservation::default();

        if !self.peers[index].received_status {
            expected.actions.push(ExpectedAction::Misbehavior {
                peer,
                reason: BlockSyncMisbehavior::GetBlocksSpam,
            });
            self.coverage.no_status_requests = self.coverage.no_status_requests.saturating_add(1);
            self.mark_evidence(ServingEvidence::MissingStatusRejected);
            return (Some(session), expected, None);
        }

        if self.peers[index].ledger.len() >= self.max_inflight {
            Self::push_frame(
                &mut expected,
                session,
                ServingFrame::RangeUnavailable {
                    start,
                    count: count.min(self.max_blocks).max(1),
                },
            );
            self.coverage.cap_rejections = self.coverage.cap_rejections.saturating_add(1);
            self.mark_evidence(ServingEvidence::SaturatedLedgerRejected);
            return (Some(session), expected, None);
        }

        let available = self
            .servable_high
            .0
            .checked_sub(start.0)
            .and_then(|difference| difference.checked_add(1))
            .unwrap_or(0);
        let requested = count.min(self.max_blocks).min(available);
        if requested == 0 {
            Self::push_frame(
                &mut expected,
                session,
                ServingFrame::RangeUnavailable {
                    start,
                    count: count.min(self.max_blocks).max(1),
                },
            );
            self.coverage.above_tip_rejections =
                self.coverage.above_tip_rejections.saturating_add(1);
            self.mark_evidence(ServingEvidence::AboveTipRejected);
            return (Some(session), expected, None);
        }

        if count < self.max_blocks && count < available {
            self.mark_evidence(ServingEvidence::WireCountBound);
        }
        if self.max_blocks < count && self.max_blocks < available {
            self.mark_evidence(ServingEvidence::LocalCountBound);
        }
        if available < count && available < self.max_blocks {
            self.mark_evidence(ServingEvidence::AvailableRangeBound);
        }

        expected.actions.push(ExpectedAction::Query {
            peer,
            start,
            count: requested,
        });
        if self.queries.iter().any(|query| {
            query.session == session
                && query.state == QueryState::Retired
                && query.finished_unavailable
        }) {
            self.mark_evidence(ServingEvidence::RequestAcceptedAfterLiveUnavailableCompletion);
        }
        let query_index = self.queries.len();
        self.queries.push(QueryModel {
            id: None,
            session,
            peer,
            start,
            requested,
            state: QueryState::Live,
            finished_unavailable: false,
        });
        if self.peers.iter().enumerate().any(|(other_index, peer)| {
            other_index != index && peer.ledger.len() >= self.max_inflight
        }) {
            self.coverage.cross_peer_progress = self.coverage.cross_peer_progress.saturating_add(1);
            self.mark_evidence(ServingEvidence::SaturatedPeerAllowedOtherPeerProgress);
        }
        self.peers[index].ledger.push(query_index);
        self.coverage.accepted_requests = self.coverage.accepted_requests.saturating_add(1);
        self.coverage.max_peer_ledger = self
            .coverage
            .max_peer_ledger
            .max(self.peers[index].ledger.len());
        (
            Some(session),
            expected,
            Some(PendingQuery {
                query_index,
                session,
                peer,
                start,
                requested,
            }),
        )
    }

    /// Bind an accepted model query to the opaque ID observed on the real action.
    /// Duplicate IDs or a changed pending query fail the replay.
    pub(super) fn bind_query(
        &mut self,
        pending: PendingQuery,
        request_id: BlockRangeRequestId,
    ) -> Result<(), String> {
        if self
            .queries
            .iter()
            .any(|query| query.id == Some(request_id))
        {
            return Err(format!(
                "GB-SM-08: reactor reused serving request identity {request_id}"
            ));
        }
        let has_previous_request_id = self.queries.iter().any(|query| query.id.is_some());
        let query = self
            .queries
            .get_mut(pending.query_index)
            .ok_or_else(|| format!("missing pending query {}", pending.query_index))?;
        if query.id.is_some()
            || query.session != pending.session
            || query.peer != pending.peer
            || query.start != pending.start
            || query.requested != pending.requested
        {
            return Err(format!(
                "pending query {} no longer matches its model entry",
                pending.query_index
            ));
        }
        query.id = Some(request_id);
        self.coverage.captured_request_ids = self.coverage.captured_request_ids.saturating_add(1);
        if has_previous_request_id {
            self.mark_evidence(ServingEvidence::MultipleRequestIdsValidated);
        }
        Ok(())
    }

    /// Resolve a generated selector to live, retired, orphaned, unknown, or
    /// mismatched completion metadata.
    pub(super) fn completion_target(&self, selector: QuerySelector) -> Option<CompletionTarget> {
        let (class, ordinal) = match selector {
            QuerySelector::Live(ordinal) => (QueryClass::Live, ordinal),
            QuerySelector::Retired(ordinal) => (QueryClass::Retired, ordinal),
            QuerySelector::Orphaned(ordinal) => (QueryClass::Orphaned, ordinal),
            QuerySelector::Unknown(ordinal) => (QueryClass::Unknown, ordinal),
            QuerySelector::MismatchedStart(ordinal) => (QueryClass::MismatchedStart, ordinal),
            QuerySelector::MismatchedPeer(ordinal) => (QueryClass::MismatchedPeer, ordinal),
        };
        let wanted_state = match class {
            QueryClass::Live
            | QueryClass::Unknown
            | QueryClass::MismatchedStart
            | QueryClass::MismatchedPeer => None,
            QueryClass::Retired => Some(QueryState::Retired),
            QueryClass::Orphaned => Some(QueryState::Orphaned),
        };
        let candidates: Vec<_> = self
            .queries
            .iter()
            .enumerate()
            .filter(|(_, query)| {
                query.id.is_some()
                    && wanted_state.map_or(query.state == QueryState::Live, |state| {
                        query.state == state
                    })
            })
            .collect();
        if candidates.is_empty() {
            return None;
        }
        let (query_index, query) = candidates.get(usize::from(ordinal) % candidates.len())?;
        let request_id = query.id?;

        if !matches!(
            class,
            QueryClass::Unknown | QueryClass::MismatchedStart | QueryClass::MismatchedPeer
        ) {
            return Some(CompletionTarget {
                class,
                request_id,
                session: query.session,
                peer: query.peer,
                start: query.start,
                requested: query.requested,
                query_index: Some(*query_index),
            });
        }

        if class == QueryClass::Unknown {
            let request_id = self.unused_request_id(request_id);
            Some(CompletionTarget {
                class,
                request_id,
                session: query.session,
                peer: query.peer,
                start: query.start,
                requested: query.requested,
                query_index: None,
            })
        } else if class == QueryClass::MismatchedStart {
            Some(CompletionTarget {
                class,
                request_id,
                session: query.session,
                peer: query.peer,
                start: block::Height(query.start.0.saturating_add(1)),
                requested: query.requested,
                query_index: None,
            })
        } else {
            let (peer, session) = self
                .peers
                .iter()
                .enumerate()
                .filter_map(|(peer, state)| {
                    let peer = u8::try_from(peer).ok()?;
                    (peer != query.peer).then_some((peer, state.current?))
                })
                .find(|(_, session)| {
                    self.sessions
                        .get(session)
                        .is_some_and(|state| state.admitted && !state.cancelled)
                })?;
            Some(CompletionTarget {
                class,
                request_id,
                session,
                peer,
                start: query.start,
                requested: query.requested,
                query_index: None,
            })
        }
    }

    /// Apply a state-driver completion and predict frames only when the exact
    /// live request and session still own it.
    pub(super) fn complete(
        &mut self,
        target: &CompletionTarget,
        kind: CompletionKind,
        blocks: &[ReadyBlock],
    ) -> ExpectedObservation {
        let retired_with_live_sibling = target.class == QueryClass::Retired
            && self.peers[Self::peer_index(target.peer)].ledger.len() >= self.max_inflight
            && self.peers[Self::peer_index(target.peer)]
                .ledger
                .iter()
                .any(|query_index| self.queries[*query_index].state == QueryState::Live);
        let orphaned_with_live_replacement = target.class == QueryClass::Orphaned
            && self.peers[Self::peer_index(target.peer)]
                .current
                .is_some_and(|session| session != target.session);
        let orphaned_without_replacement = target.class == QueryClass::Orphaned
            && self.peers[Self::peer_index(target.peer)].current.is_none();

        match target.class {
            QueryClass::Live => {
                self.coverage.live_completions = self.coverage.live_completions.saturating_add(1)
            }
            QueryClass::Retired => {
                self.coverage.retired_completions =
                    self.coverage.retired_completions.saturating_add(1);
                self.mark_evidence(ServingEvidence::RetiredCompletionIgnored);
                if retired_with_live_sibling {
                    self.mark_evidence(ServingEvidence::RetiredCompletionWithLiveSiblingIgnored);
                }
            }
            QueryClass::Orphaned => {
                self.coverage.orphaned_completions =
                    self.coverage.orphaned_completions.saturating_add(1);
                self.mark_evidence(ServingEvidence::OrphanedCompletionIgnored);
                if orphaned_without_replacement {
                    self.mark_evidence(
                        ServingEvidence::OrphanedCompletionWithoutReplacementIgnored,
                    );
                }
                if orphaned_with_live_replacement {
                    self.mark_evidence(
                        ServingEvidence::OrphanedCompletionWithLiveReplacementIgnored,
                    );
                }
            }
            QueryClass::Unknown => {
                self.coverage.unknown_completions =
                    self.coverage.unknown_completions.saturating_add(1);
                self.mark_evidence(ServingEvidence::UnknownCompletionIgnored);
            }
            QueryClass::MismatchedStart => {
                self.coverage.mismatched_completions =
                    self.coverage.mismatched_completions.saturating_add(1);
                self.mark_evidence(ServingEvidence::MismatchedStartCompletionIgnored);
            }
            QueryClass::MismatchedPeer => {
                self.coverage.mismatched_completions =
                    self.coverage.mismatched_completions.saturating_add(1);
                self.mark_evidence(ServingEvidence::MismatchedPeerCompletionIgnored);
            }
        }

        let Some(query_index) = target.query_index else {
            return ExpectedObservation::default();
        };
        if self.queries[query_index].state != QueryState::Live
            || self.peers[Self::peer_index(target.peer)].current != Some(target.session)
        {
            return ExpectedObservation::default();
        }

        self.queries[query_index].state = QueryState::Retired;
        self.queries[query_index].finished_unavailable =
            matches!(kind, CompletionKind::FinishedUnavailable);
        let peer = &mut self.peers[Self::peer_index(target.peer)];
        if let Some(position) = peer
            .ledger
            .iter()
            .position(|candidate| *candidate == query_index)
        {
            peer.ledger.remove(position);
        }

        let mut expected = ExpectedObservation::default();
        match kind {
            CompletionKind::FinishedUnavailable => {
                self.mark_evidence(ServingEvidence::LiveUnavailableResponseTerminated);
                Self::push_frame(
                    &mut expected,
                    target.session,
                    ServingFrame::RangeUnavailable {
                        start: target.start,
                        count: target.requested.max(1),
                    },
                );
            }
            CompletionKind::Ready
            | CompletionKind::ReadyOverlong
            | CompletionKind::ReadyPrefix(_)
            | CompletionKind::ReadyWithGap => {
                self.mark_evidence(ServingEvidence::ReadyResponseCompleted);
                if self.direction == ServicePeerDirection::Inbound {
                    self.mark_evidence(ServingEvidence::InboundServingCompleted);
                }
                if blocks.is_empty() {
                    self.coverage.empty_ready_completions =
                        self.coverage.empty_ready_completions.saturating_add(1);
                } else if blocks.len()
                    < usize::try_from(target.requested)
                        .expect("the requested u32 block count fits usize")
                {
                    self.coverage.short_ready_completions =
                        self.coverage.short_ready_completions.saturating_add(1);
                }
                let mut sent_bytes = 0u64;
                let mut sent = 0u32;
                for block in blocks {
                    if sent >= target.requested {
                        self.mark_evidence(
                            ServingEvidence::OverlongResponseStoppedAtRequestedCount,
                        );
                        break;
                    }
                    let expected_height = target.start.0.checked_add(sent).map(block::Height);
                    let size = u64::try_from(block.size).unwrap_or(u64::MAX);
                    let Some(next_bytes) = sent_bytes.checked_add(size) else {
                        break;
                    };
                    if expected_height != Some(block.height) {
                        self.mark_evidence(ServingEvidence::NonContiguousResponseStopped);
                        break;
                    }
                    if next_bytes > self.max_response_bytes {
                        self.coverage.byte_cap_stops =
                            self.coverage.byte_cap_stops.saturating_add(1);
                        self.mark_evidence(ServingEvidence::ByteCapStoppedPrefix);
                        break;
                    }
                    Self::push_frame(
                        &mut expected,
                        target.session,
                        ServingFrame::Block(block.hash),
                    );
                    sent = sent.saturating_add(1);
                    sent_bytes = next_bytes;
                }
                if target.start == block::Height::MIN && sent > 0 {
                    self.mark_evidence(ServingEvidence::GenesisReadyResponseCompleted);
                }
                if sent > 0 && sent_bytes == self.max_response_bytes {
                    self.coverage.exact_cap_endings =
                        self.coverage.exact_cap_endings.saturating_add(1);
                }
                if sent == 0 {
                    self.mark_evidence(ServingEvidence::EmptyReadyResponseTerminated);
                    Self::push_frame(
                        &mut expected,
                        target.session,
                        ServingFrame::RangeUnavailable {
                            start: target.start,
                            count: target.requested.max(1),
                        },
                    );
                } else {
                    self.mark_evidence(ServingEvidence::NonEmptyReadyResponseTerminated);
                    Self::push_frame(
                        &mut expected,
                        target.session,
                        ServingFrame::BlocksDone {
                            start: target.start,
                            returned: sent,
                        },
                    );
                }
            }
        }
        expected
    }

    /// Produce the peer-slot snapshot expected from all admitted sessions.
    pub(super) fn snapshot(&self) -> ServicePeerSnapshot {
        let admitted = self
            .sessions
            .values()
            .filter(|session| session.admitted)
            .count();
        let (inbound, outbound, max_inbound_peers, max_outbound_peers) = match self.direction {
            ServicePeerDirection::Inbound => (admitted, 0, self.max_peers, 0),
            ServicePeerDirection::Outbound => (0, admitted, 0, self.max_peers),
        };
        ServicePeerSnapshot::new(
            inbound,
            outbound,
            ServicePeerLimits {
                max_inbound_peers,
                max_outbound_peers,
                ..ServicePeerLimits::default()
            },
        )
    }

    /// Return the connection direction selected for every peer in this case.
    pub(super) fn direction(&self) -> ServicePeerDirection {
        self.direction
    }

    /// Report cancellation state for every connection created by the history.
    pub(super) fn cancellations(&self) -> BTreeMap<SessionKey, bool> {
        self.sessions
            .iter()
            .map(|(key, session)| (*key, session.cancelled))
            .collect()
    }

    /// Borrow coverage so the harness can add observations unavailable to the model.
    pub(super) fn coverage_mut(&mut self) -> &mut ServingCoverage {
        &mut self.coverage
    }

    /// Commit this step's evidence after every production observation matched.
    pub(super) fn commit_verified_step(&mut self) {
        self.coverage
            .commit_verified_step(std::mem::take(&mut self.step_evidence));
    }

    /// Finish a replay and return all accumulated reachability evidence.
    pub(super) fn into_coverage(self) -> ServingCoverage {
        self.coverage
    }

    /// Check model-internal ledger bounds and live-query ownership after a step.
    pub(super) fn assert_invariants(&self) -> Result<(), String> {
        for (index, peer) in self.peers.iter().enumerate() {
            if peer.ledger.len() > self.max_inflight {
                return Err(format!(
                    "GB-SM-04: peer {index} ledger length {} exceeded cap {}",
                    peer.ledger.len(),
                    self.max_inflight
                ));
            }
            for query_index in &peer.ledger {
                let query = self
                    .queries
                    .get(*query_index)
                    .ok_or_else(|| format!("ledger references missing query {query_index}"))?;
                if query.state != QueryState::Live {
                    return Err(format!(
                        "ledger references non-live query {} ({:?})",
                        query
                            .id
                            .map_or_else(|| "pending".to_string(), |id| id.to_string()),
                        query.state
                    ));
                }
            }
        }
        Ok(())
    }

    /// Retire every live query whose output session no longer exists.
    fn orphan_peer_queries(&mut self, peer: u8) {
        for query in &mut self.queries {
            if query.peer == peer && query.state == QueryState::Live {
                query.state = QueryState::Orphaned;
            }
        }
    }

    /// Mark an exact requirement precondition for commit after step verification.
    fn mark_evidence(&mut self, evidence: ServingEvidence) {
        self.step_evidence.insert(evidence);
    }

    /// Derive a valid nonzero identity that no modeled request owns.
    fn unused_request_id(&self, base: BlockRangeRequestId) -> BlockRangeRequestId {
        let mut candidate = base.get().checked_add(1).unwrap_or(1);
        loop {
            let request_id = BlockRangeRequestId::new(candidate).expect("candidate is nonzero");
            if self
                .queries
                .iter()
                .all(|query| query.id != Some(request_id))
            {
                return request_id;
            }
            candidate = candidate.checked_add(1).unwrap_or(1);
        }
    }

    /// Append an expected frame while retaining the session that must receive it.
    fn push_frame(expected: &mut ExpectedObservation, session: SessionKey, frame: ServingFrame) {
        expected.frames.entry(session).or_default().push(frame);
    }
}

impl ByteCap {
    /// Resolve a symbolic boundary against actual generated block sizes.
    pub(super) fn resolve(self, first: u32, second: u32) -> u32 {
        let minimum = u32::try_from(block::MAX_BLOCK_BYTES)
            .expect("maximum block bytes fit the response-limit wire field");
        match self {
            Self::All => MAX_BS_RESPONSE_BYTES,
            Self::ExactlyFirst => first.max(minimum),
            Self::ExactlyFirstTwo => first.saturating_add(second).max(minimum),
        }
    }
}
