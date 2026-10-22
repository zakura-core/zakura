//! Discovery's shared logical state, owned under the discovery handle's mutex.

use std::{net::SocketAddr, time::Duration};

use thiserror::Error;
use zakura_quic::NodeId;
use zakura_rt::collections::{hash_map::RandomState, HashMap, HashSet};

use super::protocol::{
    advertised_services, entry_is_expired, hello_verdict, normalize_direct_addrs,
    normalize_services, push_unique_service, DialCandidateExclusions, DiscoveryBookError,
    DiscoveryRecordValidationContext, DiscoveryWireError, ImportBatchOutcome, ImportOutcome,
    RecordVerdict, Services, ZakuraCachedLiveServiceSummary, ZakuraDiscoveryBook,
    ZakuraDiscoveryBookLimits, ZakuraDiscoveryConfig, ZakuraDiscoveryDialCandidate,
    ZakuraDiscoveryEntry, ZakuraDiscoveryLocalConfig, ZakuraDiscoveryPersistedEntry,
    ZakuraNodeRecord, ZakuraNodeRecordBody, ZakuraServiceId, DEFAULT_LIVE_SERVICE_SUMMARY_TTL,
    MAX_DISCOVERY_EXCLUDED_NODE_IDS,
};
use super::verified::VerifiedRecord;
use crate::zakura::{
    ServiceAdmissionDecision, ServicePeerDirection, ServicePeerSnapshot, ZakuraConnId,
    ZakuraNetworkId, ZakuraPeerId,
};

/// The identity of one discovery stream session: its peer, its connection,
/// and the stream session on that connection.
///
/// A reopened stream on the same connection gets a new `session_id`, so the
/// key tells a replacement from the session it replaced.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct DiscoverySessionKey {
    /// Authenticated peer identity.
    pub(super) peer_id: ZakuraPeerId,
    /// Transport connection that carries the stream.
    pub(super) conn_id: ZakuraConnId,
    /// Stream session on that connection.
    pub(super) session_id: u64,
}

impl DiscoverySessionKey {
    /// Builds the key of `session_id` on `conn_id` to `peer_id`.
    pub(super) fn new(peer_id: ZakuraPeerId, conn_id: ZakuraConnId, session_id: u64) -> Self {
        Self {
            peer_id,
            conn_id,
            session_id,
        }
    }
}

/// A connected peer's own record that a session failed to install.
#[derive(Debug, Error)]
pub(super) enum SessionRecordError {
    /// A newer session replaced the session that delivered the record.
    #[error("Zakura discovery session was replaced before its record was installed")]
    Superseded,

    /// The record failed its import.
    #[error(transparent)]
    Import(#[from] DiscoveryBookError),
}

impl SessionRecordError {
    /// What the failure means for the peer that sent the record.
    ///
    /// A superseded session's record only drops: the session did nothing wrong.
    pub(super) fn verdict(&self) -> RecordVerdict {
        match self {
            Self::Superseded => RecordVerdict::Drop,
            Self::Import(error) => hello_verdict(error),
        }
    }
}

/// Discovery's shared logical state: the record book, the connected peers'
/// service hints, the admitted sessions, and the local record's sequence.
///
/// The discovery handle holds it under one mutex and does the IO: clocks,
/// randomness, signing, record verification, and watch publication. These
/// transitions take the current time and any random source as arguments.
///
/// Every time argument is Unix seconds. Signed record expiry and the local
/// deadlines (the live-summary TTL and the dial backoffs) share that clock,
/// and persisted entries store it.
///
/// First-party imports, a peer's own `Hello` and `Services`, install only
/// while the session that delivered them is admitted. A record verified for a
/// replaced session therefore changes nothing. Relayed `Peers` records carry
/// their relay's node id as provenance, not a session.
#[derive(Debug)]
pub(super) struct DiscoveryState {
    book: ZakuraDiscoveryBook,
    active_services: HashMap<NodeId, ZakuraActiveServiceEntry>,
    admitted_peers: HashMap<ZakuraPeerId, ZakuraDiscoveryAdmittedPeer>,
    local: LocalRecordState,
    config: ZakuraDiscoveryConfig,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct ZakuraDiscoveryAdmittedPeer {
    conn_id: ZakuraConnId,
    session_id: u64,
    direction: ServicePeerDirection,
}

impl ZakuraDiscoveryAdmittedPeer {
    fn is(&self, session: &DiscoverySessionKey) -> bool {
        (self.conn_id, self.session_id) == (session.conn_id, session.session_id)
    }
}

/// A connected peer's service hints: its own latest record and its live
/// summaries.
#[derive(Clone, Debug, Eq, PartialEq)]
struct ZakuraActiveServiceEntry {
    record: Option<VerifiedRecord>,
    services: Vec<ZakuraServiceId>,
    live_summaries: Vec<ZakuraCachedLiveServiceSummary>,
}

impl ZakuraActiveServiceEntry {
    fn from_record(record: VerifiedRecord, services: Vec<ZakuraServiceId>) -> Self {
        Self {
            record: Some(record),
            services,
            live_summaries: Vec::new(),
        }
    }

    fn from_live_summaries(summaries: Vec<ZakuraCachedLiveServiceSummary>) -> Self {
        let mut services = Vec::new();
        for summary in &summaries {
            push_unique_service(&mut services, summary.service_id.clone());
        }
        Self {
            record: None,
            services,
            live_summaries: summaries,
        }
    }

    fn update_record(&mut self, record: VerifiedRecord, services: Vec<ZakuraServiceId>) {
        self.record = Some(record);
        self.services = services;
    }

    fn update_live_summaries(&mut self, summaries: Vec<ZakuraCachedLiveServiceSummary>) {
        self.live_summaries.retain(|cached| {
            !summaries.iter().any(|summary| {
                summary.service_id == cached.service_id && summary.summary_tag == cached.summary_tag
            })
        });
        for summary in summaries {
            self.live_summaries.push(summary);
        }
        if self.record.is_none() {
            self.refresh_live_derived_services();
        }
    }

    fn prune_expired_live_summaries(&mut self, now_unix_secs: u64) {
        self.live_summaries
            .retain(|summary| summary.expires_at_unix_secs > now_unix_secs);
        if self.record.is_none() {
            self.refresh_live_derived_services();
        }
    }

    fn refresh_live_derived_services(&mut self) {
        let mut services = Vec::new();
        for summary in &self.live_summaries {
            push_unique_service(&mut services, summary.service_id.clone());
        }
        self.services = services;
    }

    fn is_empty_live_only(&self) -> bool {
        self.record.is_none() && self.live_summaries.is_empty()
    }
}

/// The locally authored record's contents and next sequence.
///
/// The discovery handle holds the signing key and signs each body this state
/// proposes.
#[derive(Clone, Debug)]
pub(super) struct LocalRecordState {
    node_id: NodeId,
    direct_addrs: Vec<SocketAddr>,
    services: Vec<ZakuraServiceId>,
    zakura_protocol_min: u16,
    zakura_protocol_max: u16,
    network_id: ZakuraNetworkId,
    chain_id: [u8; 32],
    next_sequence: u64,
}

impl LocalRecordState {
    /// Builds the local record state from `config`.
    ///
    /// The first sequence is the later of the persisted next sequence and
    /// `wall_clock_sequence`.
    pub(super) fn new(
        config: &ZakuraDiscoveryLocalConfig,
        wall_clock_sequence: u64,
    ) -> Result<Self, DiscoveryWireError> {
        let persisted_next_sequence = config
            .last_authored_sequence
            .map(|sequence| sequence.saturating_add(1))
            .unwrap_or(0);
        Ok(Self {
            node_id: config.secret_key.public(),
            direct_addrs: normalize_direct_addrs(config.direct_addrs.clone()),
            services: advertised_services(config.services.clone())?,
            zakura_protocol_min: config.zakura_protocol_min,
            zakura_protocol_max: config.zakura_protocol_max,
            network_id: config.network_id,
            chain_id: config.chain_id,
            next_sequence: persisted_next_sequence.max(wall_clock_sequence),
        })
    }

    /// Allocates the next sequence and returns the record body to sign.
    ///
    /// The sequence is the later of the next unused sequence and
    /// `wall_clock_sequence`, so it strictly increases.
    pub(super) fn next_body(
        &mut self,
        now_unix_secs: u64,
        wall_clock_sequence: u64,
        record_ttl: Duration,
    ) -> ZakuraNodeRecordBody {
        let sequence = self.next_sequence.max(wall_clock_sequence);
        self.next_sequence = sequence.saturating_add(1);
        ZakuraNodeRecordBody {
            node_id: self.node_id,
            direct_addrs: self.direct_addrs.clone(),
            services: self.services.clone(),
            zakura_protocol_min: self.zakura_protocol_min,
            zakura_protocol_max: self.zakura_protocol_max,
            network_id: self.network_id,
            chain_id: self.chain_id,
            sequence,
            expires_at_unix_secs: now_unix_secs.saturating_add(record_ttl.as_secs()),
            extensions: Vec::new(),
        }
    }
}

impl DiscoveryState {
    /// Builds an empty state for `local`, whose book places sample windows with
    /// `disclosure_key`.
    pub(super) fn new(
        local: LocalRecordState,
        config: ZakuraDiscoveryConfig,
        disclosure_key: RandomState,
    ) -> Self {
        Self {
            book: ZakuraDiscoveryBook::with_disclosure_key(
                config.book_limits,
                Some(local.node_id),
                disclosure_key,
            ),
            active_services: HashMap::new(),
            admitted_peers: HashMap::new(),
            local,
            config,
        }
    }

    /// Returns the runtime configuration.
    pub(super) fn config(&self) -> &ZakuraDiscoveryConfig {
        &self.config
    }

    /// Returns the record book.
    pub(super) fn book(&self) -> &ZakuraDiscoveryBook {
        &self.book
    }

    /// Returns the record book for transitions that need no other state.
    pub(super) fn book_mut(&mut self) -> &mut ZakuraDiscoveryBook {
        &mut self.book
    }

    /// Returns the validation context for imports at `now`.
    pub(super) fn validation_context(&self, now: u64) -> DiscoveryRecordValidationContext {
        DiscoveryRecordValidationContext {
            expected_network_id: self.local.network_id,
            expected_chain_id: self.local.chain_id,
            current_unix_secs: now,
            supported_protocol_min: self.local.zakura_protocol_min,
            supported_protocol_max: self.local.zakura_protocol_max,
            max_record_ttl: self.config.max_record_ttl,
            clock_skew_tolerance: self.config.clock_skew_tolerance,
        }
    }

    /// Whether `current` stays valid through the next refresh interval at `now`.
    pub(super) fn self_record_is_fresh(&self, current: &ZakuraNodeRecord, now: u64) -> bool {
        current.body.expires_at_unix_secs
            > now.saturating_add(self.config.refresh_interval.as_secs())
    }

    /// Allocates the next self-record sequence and returns the body to sign.
    pub(super) fn next_self_record_body(
        &mut self,
        now: u64,
        wall_clock_sequence: u64,
    ) -> ZakuraNodeRecordBody {
        self.local
            .next_body(now, wall_clock_sequence, self.config.record_ttl)
    }

    /// Admits `session` in `direction`.
    ///
    /// A peer's admitted session is replaced only by a later
    /// `(connection, session)`. A new peer is admitted while its direction has
    /// a free slot.
    pub(super) fn admit_session(
        &mut self,
        session: DiscoverySessionKey,
        direction: ServicePeerDirection,
    ) -> ServiceAdmissionDecision {
        let admitted = ZakuraDiscoveryAdmittedPeer {
            conn_id: session.conn_id,
            session_id: session.session_id,
            direction,
        };
        if let Some(peer) = self.admitted_peers.get_mut(&session.peer_id) {
            if (session.conn_id, session.session_id) <= (peer.conn_id, peer.session_id) {
                return ServiceAdmissionDecision::RejectNotUseful;
            }
            *peer = admitted;
            return ServiceAdmissionDecision::Admit;
        }

        let cap = match direction {
            ServicePeerDirection::Inbound => self.config.peer_limits.max_inbound_peers,
            ServicePeerDirection::Outbound => self.config.peer_limits.max_outbound_peers,
        };
        if self.admitted_in(direction) >= cap {
            return ServiceAdmissionDecision::RejectFull;
        }
        self.admitted_peers.insert(session.peer_id, admitted);
        ServiceAdmissionDecision::Admit
    }

    /// Removes `peer_id`'s admitted session if it runs on `conn_id`.
    pub(super) fn remove_connection(&mut self, peer_id: &ZakuraPeerId, conn_id: ZakuraConnId) {
        if self
            .admitted_peers
            .get(peer_id)
            .is_some_and(|peer| peer.conn_id == conn_id)
        {
            self.admitted_peers.remove(peer_id);
        }
    }

    /// Removes exactly `session`. A replacement session stays admitted.
    pub(super) fn remove_session(&mut self, session: &DiscoverySessionKey) {
        if self.is_current_session(session) {
            self.admitted_peers.remove(&session.peer_id);
        }
    }

    /// Whether `session` is its peer's admitted session.
    pub(super) fn is_current_session(&self, session: &DiscoverySessionKey) -> bool {
        self.admitted_peers
            .get(&session.peer_id)
            .is_some_and(|peer| peer.is(session))
    }

    /// Returns the admitted sessions' slot snapshot.
    pub(super) fn peer_snapshot(&self) -> ServicePeerSnapshot {
        ServicePeerSnapshot::new(
            self.admitted_in(ServicePeerDirection::Inbound),
            self.admitted_in(ServicePeerDirection::Outbound),
            self.config.peer_limits,
        )
    }

    fn admitted_in(&self, direction: ServicePeerDirection) -> usize {
        self.admitted_peers
            .values()
            .filter(|peer| peer.direction == direction)
            .count()
    }

    /// Returns bounded node ids to exclude from this node's peer-sample request.
    ///
    /// Exclusions include the local node, the `connected` peers, and recently
    /// seen records. The list holds raw node ids in byte order, as the wire
    /// requires.
    pub(super) fn peer_sample_exclusions(&self, connected: &[NodeId]) -> Vec<[u8; 32]> {
        let max = self
            .config
            .max_excluded_node_ids
            .min(MAX_DISCOVERY_EXCLUDED_NODE_IDS);
        let mut excluded = Vec::with_capacity(max);
        push_excluded_node_id(&mut excluded, self.local.node_id, max);
        for node_id in connected {
            push_excluded_node_id(&mut excluded, *node_id, max);
        }
        for node_id in self
            .book
            .recent_node_ids(max.saturating_sub(excluded.len()))
        {
            push_excluded_node_id(&mut excluded, node_id, max);
        }
        excluded.sort_unstable();
        excluded
    }

    /// Returns the stored evidence for `record` if the book or the service
    /// hints hold exactly that record.
    ///
    /// A stored record passed its author checks when it was stored, so an
    /// identical record needs no new verification.
    pub(super) fn stored_evidence(&self, record: &ZakuraNodeRecord) -> Option<VerifiedRecord> {
        let node_id = &record.body.node_id;
        if let Some(entry) = self
            .book
            .get(node_id)
            .filter(|entry| entry.record() == record)
        {
            return Some(entry.verified_record().clone());
        }
        self.active_services
            .get(node_id)
            .and_then(|active| active.record.as_ref())
            .filter(|stored| **stored == *record)
            .cloned()
    }

    /// Installs relayed records from `source` into the book, up to the
    /// per-response import limit, and counts each result in `outcome`.
    ///
    /// Expiry, staleness, address, storage, and per-relay policy each only
    /// drop their record.
    pub(super) fn install_relayed(
        &mut self,
        records: Vec<VerifiedRecord>,
        source: Option<NodeId>,
        now: u64,
        outcome: &mut ImportBatchOutcome,
    ) {
        let max_records = self.config.book_limits.max_imported_records_per_response;
        let context = self.validation_context(now);
        for record in records {
            if outcome.attempted >= max_records {
                outcome.dropped_for_limit += 1;
                continue;
            }
            outcome.attempted += 1;
            match self
                .book
                .import_verified_record(record, source, now, &context)
            {
                Ok(import_outcome) => outcome.record_success(import_outcome),
                Err(_) => outcome.rejected += 1,
            }
        }
    }

    /// Installs a connected peer's own record from `session` and updates its
    /// service hints, as [`Self::install_connected_peer_record`] does.
    ///
    /// A replaced `session` changes nothing and fails as
    /// [`SessionRecordError::Superseded`].
    pub(super) fn install_session_record(
        &mut self,
        session: &DiscoverySessionKey,
        record: VerifiedRecord,
        now: u64,
    ) -> Result<ImportOutcome, SessionRecordError> {
        if !self.is_current_session(session) {
            return Err(SessionRecordError::Superseded);
        }
        Ok(self.install_connected_peer_record(record, now)?)
    }

    /// Installs a connected peer's own record and updates its service hints.
    ///
    /// Addressless or locally non-dialable records still update the service
    /// hints, because the live connection already exists; the book still
    /// rejects them.
    pub(super) fn install_connected_peer_record(
        &mut self,
        record: VerifiedRecord,
        now: u64,
    ) -> Result<ImportOutcome, DiscoveryBookError> {
        let max_services = self.config.book_limits.max_services_per_record;
        if record.body.services.len() > max_services {
            return Err(DiscoveryBookError::Limit {
                field: "service",
                actual: record.body.services.len(),
                max: max_services,
            });
        }

        let peer_node_id = record.body.node_id;
        let services = normalize_services(record.body.services.clone());
        let context = self.validation_context(now);
        let import_result =
            self.book
                .import_verified_record(record.clone(), Some(peer_node_id), now, &context);
        if import_result.is_ok()
            || matches!(import_result, Err(ref error) if is_direct_address_import_error(error))
        {
            self.update_active_services_from_connected_record(
                peer_node_id,
                record,
                services,
                &import_result,
                now,
            );
        }
        import_result
    }

    /// Caches a connected peer's own live summaries, received on `session`,
    /// as [`Self::receive_live_summaries`] does.
    ///
    /// A replaced `session` caches nothing.
    pub(super) fn receive_session_live_summaries(
        &mut self,
        session: &DiscoverySessionKey,
        peer_node_id: NodeId,
        services: Services,
        connected: &[NodeId],
        now: u64,
    ) -> Result<(), DiscoveryWireError> {
        if !self.is_current_session(session) {
            return check_services_author(&services, peer_node_id);
        }
        self.receive_live_summaries(peer_node_id, services, connected, now)
    }

    /// Caches a connected peer's own live summaries.
    ///
    /// The summaries describe `peer_node_id`, which must be `connected`. Their
    /// local expiry is the earlier of the declared expiry and the local
    /// live-summary TTL.
    pub(super) fn receive_live_summaries(
        &mut self,
        peer_node_id: NodeId,
        services: Services,
        connected: &[NodeId],
        now: u64,
    ) -> Result<(), DiscoveryWireError> {
        check_services_author(&services, peer_node_id)?;

        self.sync_active_services(connected, now);
        if !connected.contains(&peer_node_id) {
            return Ok(());
        }

        let Some(expires_at_unix_secs) = effective_live_summary_expiry(
            services.expires_at_unix_secs,
            now,
            self.config.clock_skew_tolerance,
        ) else {
            if let Some(entry) = self.active_services.get_mut(&peer_node_id) {
                entry.live_summaries.clear();
                if entry.record.is_none() {
                    entry.refresh_live_derived_services();
                }
            }
            self.active_services
                .retain(|_, entry| !entry.is_empty_live_only());
            return Ok(());
        };

        let mut live_summaries = Vec::with_capacity(services.summaries.len());
        for envelope in &services.summaries {
            live_summaries.push(ZakuraCachedLiveServiceSummary::from_envelope(
                envelope,
                now,
                expires_at_unix_secs,
            )?);
        }

        self.active_services
            .entry(peer_node_id)
            .and_modify(|entry| entry.update_live_summaries(live_summaries.clone()))
            .or_insert_with(|| ZakuraActiveServiceEntry::from_live_summaries(live_summaries));
        Ok(())
    }

    /// Returns a `connected` peer's advertised services.
    pub(super) fn active_services(
        &mut self,
        node_id: NodeId,
        connected: &[NodeId],
        now: u64,
    ) -> Option<Vec<ZakuraServiceId>> {
        self.sync_active_services(connected, now);
        if !connected.contains(&node_id) {
            return None;
        }
        self.active_services
            .get(&node_id)
            .map(|entry| entry.services.clone())
    }

    /// Returns a `connected` peer's fresh live summaries.
    pub(super) fn live_service_summaries(
        &mut self,
        node_id: NodeId,
        connected: &[NodeId],
        now: u64,
    ) -> Option<Vec<ZakuraCachedLiveServiceSummary>> {
        self.sync_active_services(connected, now);
        if !connected.contains(&node_id) {
            return None;
        }
        self.active_services
            .get(&node_id)
            .map(|entry| entry.live_summaries.clone())
    }

    /// Imports one trusted static signed record.
    pub(super) fn import_static_record(
        &mut self,
        record: ZakuraNodeRecord,
        now: u64,
    ) -> Result<ImportOutcome, DiscoveryBookError> {
        let context = self.validation_context(now);
        self.book.import_static_record(record, now, &context)
    }

    /// Re-validates and imports entries loaded from persistent storage.
    pub(super) fn import_persisted_entries(
        &mut self,
        entries: impl IntoIterator<Item = ZakuraDiscoveryPersistedEntry>,
        now: u64,
    ) -> ImportBatchOutcome {
        let context = self.validation_context(now);
        self.book.import_persisted_entries(entries, now, &context)
    }

    /// Returns a bounded peer sample for `requester`, at most the configured
    /// sample limit.
    pub(super) fn sample_peers<R: rand_10::Rng + ?Sized>(
        &mut self,
        requester: NodeId,
        limit: usize,
        wanted_services: &[ZakuraServiceId],
        exclude_node_ids: &[[u8; 32]],
        now: u64,
        rng: &mut R,
    ) -> Vec<ZakuraNodeRecord> {
        let limit = limit.min(self.config.peer_sample_limit);
        self.book.sample_peers_with_rng(
            limit,
            wanted_services,
            exclude_node_ids,
            now,
            requester,
            rng,
        )
    }

    /// Returns how many discovery dials may start with `connected` connections
    /// and `in_flight` dials.
    pub(super) fn dial_slot_limit(&self, connected: usize, in_flight: usize) -> usize {
        super::protocol::discovery_dial_slot_limit(
            connected,
            in_flight,
            self.config.max_zakura_connections,
            self.config.discovery_connection_headroom,
            self.config.max_concurrent_discovery_dials,
        )
    }

    /// Returns at most `limit` dial candidates.
    ///
    /// Candidates advertise every `required_services` entry, bounded to one
    /// record's service count. Candidates advertising any `preferred_services`
    /// entry come first. `rng` breaks ties between signed records.
    pub(super) fn dial_candidates<R: rand_10::Rng + ?Sized>(
        &self,
        limit: usize,
        required_services: &[ZakuraServiceId],
        preferred_services: &[ZakuraServiceId],
        exclusions: DialCandidateExclusions<'_>,
        now: u64,
        rng: &mut R,
    ) -> Vec<ZakuraDiscoveryDialCandidate> {
        let required_services = bounded_services(required_services, self.config.book_limits);
        self.book.dial_candidates(
            limit,
            &required_services,
            preferred_services,
            exclusions,
            now,
            (self.config.dial_backoff_base, self.config.dial_backoff_max),
            rng,
        )
    }

    /// Drops service hints of peers not in `connected`, expired live
    /// summaries, and record hints past their signed expiry.
    fn sync_active_services(&mut self, connected: &[NodeId], now_unix_secs: u64) {
        let connected: HashSet<_> = connected.iter().copied().collect();
        self.active_services
            .retain(|node_id, _| connected.contains(node_id));
        for entry in self.active_services.values_mut() {
            entry.prune_expired_live_summaries(now_unix_secs);
            if entry
                .record
                .as_ref()
                .is_some_and(|record| record.body.expires_at_unix_secs < now_unix_secs)
            {
                entry.record = None;
                entry.refresh_live_derived_services();
            }
        }
        self.active_services
            .retain(|_, entry| !entry.is_empty_live_only());
    }

    fn update_active_services_from_connected_record(
        &mut self,
        node_id: NodeId,
        record: VerifiedRecord,
        services: Vec<ZakuraServiceId>,
        import_result: &Result<ImportOutcome, DiscoveryBookError>,
        now: u64,
    ) {
        // An expired hint no longer orders the peer's records.
        if self.active_services.get(&node_id).is_some_and(|active| {
            active.record.as_ref().is_some_and(|active_record| {
                active_record.body.expires_at_unix_secs >= now
                    && record.body.sequence <= active_record.body.sequence
            })
        }) {
            return;
        }

        let should_update = match import_result {
            Ok(ImportOutcome::Added | ImportOutcome::Updated) => true,
            Ok(ImportOutcome::MetadataUpdated) => self
                .book
                .get(&node_id)
                .is_some_and(|entry| entry.record() == &*record),
            Ok(ImportOutcome::IgnoredOlder) => false,
            Err(error) if is_direct_address_import_error(error) => {
                self.book.get(&node_id).is_none_or(|entry| {
                    connected_record_is_fresh_for_stored_record(&record, entry, now)
                })
            }
            Err(_) => false,
        };

        if should_update {
            self.active_services
                .entry(node_id)
                .and_modify(|entry| entry.update_record(record.clone(), services.clone()))
                .or_insert_with(|| ZakuraActiveServiceEntry::from_record(record, services));
        }
    }
}

/// Checks that `services` describe `peer_node_id`, the peer that sent them.
fn check_services_author(
    services: &Services,
    peer_node_id: NodeId,
) -> Result<(), DiscoveryWireError> {
    if services.node_id != peer_node_id {
        return Err(DiscoveryWireError::MismatchedNodeId {
            field: "services node id",
        });
    }
    Ok(())
}

fn connected_record_is_fresh_for_stored_record(
    record: &ZakuraNodeRecord,
    entry: &ZakuraDiscoveryEntry,
    now: u64,
) -> bool {
    // An expired record no longer orders the peer's records.
    if entry_is_expired(entry, now) {
        return true;
    }
    match record.body.sequence.cmp(&entry.record().body.sequence) {
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Equal => entry.record() == record,
        std::cmp::Ordering::Greater => true,
    }
}

fn is_direct_address_import_error(error: &DiscoveryBookError) -> bool {
    matches!(
        error,
        DiscoveryBookError::NoUsableDirectAddress
            | DiscoveryBookError::NonDialableDirectAddress { .. }
    )
}

fn effective_live_summary_expiry(
    declared_expires_at: u64,
    now_unix_secs: u64,
    clock_skew_tolerance: Duration,
) -> Option<u64> {
    let local_expiry = now_unix_secs.saturating_add(DEFAULT_LIVE_SERVICE_SUMMARY_TTL.as_secs());
    if declared_expires_at > now_unix_secs {
        return Some(declared_expires_at.min(local_expiry));
    }
    if declared_expires_at.saturating_add(clock_skew_tolerance.as_secs()) <= now_unix_secs {
        return None;
    }
    // This response arrived on the authenticated live stream but is already expired locally. Use
    // a bounded receipt TTL only inside the accepted skew window; future expiries still honor a
    // peer's intentionally shorter lifetime.
    Some(local_expiry)
}

fn bounded_services(
    services: &[ZakuraServiceId],
    limits: ZakuraDiscoveryBookLimits,
) -> Vec<ZakuraServiceId> {
    services
        .iter()
        .take(limits.max_services_per_record)
        .cloned()
        .collect()
}

fn push_excluded_node_id(excluded: &mut Vec<[u8; 32]>, node_id: NodeId, max_node_ids: usize) {
    let node_id = *node_id.as_bytes();
    if excluded.len() < max_node_ids && !excluded.contains(&node_id) {
        excluded.push(node_id);
    }
}
