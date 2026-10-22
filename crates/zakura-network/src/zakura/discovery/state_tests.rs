//! Machine tests for discovery's shared state: the record book and
//! [`DiscoveryState`], each checked against an independent model.
//!
//! The models restate the documented rules. They never call the code they
//! check.

use std::{
    cmp::Reverse,
    collections::{HashMap, HashSet},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use proptest::prelude::*;
use rand_10::{rngs::StdRng, SeedableRng};
use zakura_quic::{NodeId, NodeSecretKey};
use zakura_rt::collections::hash_map::RandomState;

use super::protocol::{
    DiscoveryBookError, DiscoveryRecordError, DiscoveryRecordValidationContext,
    DiscoveryServiceSummary, ImportOutcome, ServiceSummaryEnvelope, Services, ZakuraDiscoveryBook,
    ZakuraDiscoveryBookLimits, ZakuraDiscoveryConfig, ZakuraDiscoveryLocalConfig, ZakuraNodeRecord,
    ZakuraNodeRecordBody, ZakuraServiceId,
};
use super::state::{DiscoverySessionKey, DiscoveryState, LocalRecordState};
use super::verified::VerifiedRecord;
use crate::zakura::{
    ServiceAdmissionDecision, ServicePeerDirection, ServicePeerLimits, ServicePeerSnapshot,
    ZakuraConnId, ZakuraNetworkId, ZakuraPeerId, ZAKURA_PROTOCOL_VERSION_CURRENT,
};

const START: u64 = 1_700_000_000;
const CHAIN_ID: [u8; 32] = [7; 32];
const SKEW: u64 = 300;
const LIVE_SUMMARY_TTL: u64 = 30;
const DISCLOSURE_EPOCH: u64 = 600;

const BOOK_MAX_RECORDS: usize = 4;
const BOOK_MAX_PER_RELAY: usize = 2;
const SAMPLE_WINDOW: usize = 3;
const HIGH_FAILURES: u32 = 3;

const AUTHORS: usize = 6;
const RELAYS: usize = 3;
const PEERS: usize = 3;
const PEER_CAP: usize = 2;

fn secret(seed: u8) -> NodeSecretKey {
    NodeSecretKey::from_bytes(&[seed; 32])
}

fn author(index: usize) -> NodeSecretKey {
    secret(20 + u8::try_from(index).expect("test indexes are small"))
}

fn relay(index: usize) -> NodeId {
    secret(40 + u8::try_from(index).expect("test indexes are small")).public()
}

fn peer_secret(index: usize) -> NodeSecretKey {
    secret(60 + u8::try_from(index).expect("test indexes are small"))
}

fn peer_id(node_id: NodeId) -> ZakuraPeerId {
    ZakuraPeerId::new(node_id.as_bytes().to_vec()).expect("a node id is a valid peer id")
}

fn context(now: u64) -> DiscoveryRecordValidationContext {
    DiscoveryRecordValidationContext {
        expected_network_id: ZakuraNetworkId::Regtest,
        expected_chain_id: CHAIN_ID,
        current_unix_secs: now,
        supported_protocol_min: ZAKURA_PROTOCOL_VERSION_CURRENT,
        supported_protocol_max: ZAKURA_PROTOCOL_VERSION_CURRENT,
        max_record_ttl: Duration::from_secs(24 * 60 * 60),
        clock_skew_tolerance: Duration::from_secs(SKEW),
    }
}

/// A discovery record by `signer` with one public address.
fn record(signer: &NodeSecretKey, address: u8, sequence: u64, expires: u64) -> ZakuraNodeRecord {
    record_with_services(
        signer,
        address,
        sequence,
        expires,
        vec![ZakuraServiceId::discovery()],
    )
}

/// A record by `signer` with one public address that advertises `services`.
fn record_with_services(
    signer: &NodeSecretKey,
    address: u8,
    sequence: u64,
    expires: u64,
    services: Vec<ZakuraServiceId>,
) -> ZakuraNodeRecord {
    ZakuraNodeRecord::sign(
        ZakuraNodeRecordBody {
            node_id: signer.public(),
            direct_addrs: vec![SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(45, 33, 30, address)),
                8233,
            )],
            services,
            zakura_protocol_min: ZAKURA_PROTOCOL_VERSION_CURRENT,
            zakura_protocol_max: ZAKURA_PROTOCOL_VERSION_CURRENT,
            network_id: ZakuraNetworkId::Regtest,
            chain_id: CHAIN_ID,
            sequence,
            expires_at_unix_secs: expires,
            extensions: Vec::new(),
        },
        signer,
    )
    .expect("the signer authors its own record")
}

// Book model.

/// Who supplied a record.
#[derive(Copy, Clone, Debug)]
enum Supplier {
    /// The record's author, as in its own `Hello`.
    Author,
    /// A third party relaying the record in its `Peers`.
    Relay(usize),
    /// No peer, as in a test import.
    Nobody,
}

#[derive(Clone, Debug)]
enum BookOp {
    Import {
        author: usize,
        sequence: u64,
        /// Expiry relative to now. Below `-SKEW` the record is already expired.
        expires_in: i64,
        supplier: Supplier,
    },
    DialSuccess(usize),
    DialFailure(usize),
    Sample {
        requester: usize,
        limit: usize,
    },
    Advance(u64),
}

fn supplier() -> impl Strategy<Value = Supplier> {
    prop_oneof![
        2 => Just(Supplier::Author),
        3 => (0..RELAYS).prop_map(Supplier::Relay),
        1 => Just(Supplier::Nobody),
    ]
}

fn book_op() -> impl Strategy<Value = BookOp> {
    prop_oneof![
        6 => (0..AUTHORS, 0u64..4, -400i64..120, supplier()).prop_map(
            |(author, sequence, expires_in, supplier)| BookOp::Import {
                author,
                sequence,
                expires_in,
                supplier,
            }
        ),
        1 => (0..AUTHORS).prop_map(BookOp::DialSuccess),
        1 => (0..AUTHORS).prop_map(BookOp::DialFailure),
        2 => (0..RELAYS, 0usize..5).prop_map(|(requester, limit)| BookOp::Sample { requester, limit }),
        2 => prop_oneof![0u64..60, 500u64..700].prop_map(BookOp::Advance),
    ]
}

/// The model's view of one stored entry.
#[derive(Clone, Debug, Eq, PartialEq)]
struct ModelEntry {
    sequence: u64,
    expires: u64,
    source: Option<NodeId>,
    confirmed: bool,
    failures: u32,
    last_seen: u64,
    last_success: Option<u64>,
}

/// What the model expects one import to return.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Expected {
    Outcome(ImportOutcome),
    Expired,
    RelayFull,
    BookFull,
}

/// An independent restatement of the book's documented rules.
#[derive(Default)]
struct BookModel {
    entries: HashMap<NodeId, ModelEntry>,
}

impl BookModel {
    fn relay_of(source: Option<NodeId>, node_id: NodeId) -> Option<NodeId> {
        source.filter(|source| *source != node_id)
    }

    fn relayed_by(&self, relay: NodeId) -> usize {
        self.entries
            .iter()
            .filter(|(node_id, entry)| Self::relay_of(entry.source, **node_id) == Some(relay))
            .count()
    }

    fn import(
        &mut self,
        node_id: NodeId,
        sequence: u64,
        expires: u64,
        source: Option<NodeId>,
        now: u64,
    ) -> Expected {
        if expires.saturating_add(SKEW) < now {
            return Expected::Expired;
        }
        let first_party = source == Some(node_id);
        let relay = Self::relay_of(source, node_id);
        let relay_full = relay.is_some_and(|relay| self.relayed_by(relay) >= BOOK_MAX_PER_RELAY);

        if let Some(entry) = self.entries.get_mut(&node_id) {
            // An expired record no longer orders its node's records.
            let stored_orders = entry.expires >= now;
            if stored_orders && sequence < entry.sequence {
                return Expected::Outcome(ImportOutcome::IgnoredOlder);
            }
            let same_sequence = stored_orders && sequence == entry.sequence;
            // A full relay cannot take over an entry.
            let incoming = if relay_full { entry.source } else { source };
            // A same-sequence relay copy keeps the author's own provenance.
            let keeps_author =
                same_sequence && entry.source == Some(node_id) && incoming != Some(node_id);
            if !keeps_author {
                entry.source = incoming;
            }
            entry.last_seen = now;
            entry.confirmed |= first_party;
            if same_sequence {
                return Expected::Outcome(ImportOutcome::MetadataUpdated);
            }
            entry.sequence = sequence;
            entry.expires = expires;
            return Expected::Outcome(ImportOutcome::Updated);
        }

        if let Some(relay) = relay.filter(|_| relay_full) {
            let expired: Vec<NodeId> = self
                .entries
                .iter()
                .filter(|(id, entry)| {
                    Self::relay_of(entry.source, **id) == Some(relay) && entry.expires < now
                })
                .map(|(id, _)| *id)
                .collect();
            if expired.is_empty() {
                return Expected::RelayFull;
            }
            for id in expired {
                self.entries.remove(&id);
            }
        }
        self.entries.insert(
            node_id,
            ModelEntry {
                sequence,
                expires,
                source,
                confirmed: first_party,
                failures: 0,
                last_seen: now,
                last_success: None,
            },
        );
        if self.entries.len() > BOOK_MAX_RECORDS {
            let victim = self.eviction_victim(now);
            self.entries.remove(&victim);
            if victim == node_id {
                return Expected::BookFull;
            }
        }
        Expected::Outcome(ImportOutcome::Added)
    }

    /// The documented eviction order: the earliest-expiring expired entry,
    /// then the most-failed never-dialed entry, then the least valuable entry
    /// with unconfirmed entries first.
    fn eviction_victim(&self, now: u64) -> NodeId {
        let entries = || self.entries.iter();
        entries()
            .filter(|(_, entry)| entry.expires < now)
            .min_by_key(|(id, entry)| (entry.expires, *id.as_bytes()))
            .or_else(|| {
                entries()
                    .filter(|(_, entry)| {
                        entry.last_success.is_none() && entry.failures >= HIGH_FAILURES
                    })
                    .max_by_key(|(id, entry)| {
                        (
                            entry.failures,
                            Reverse(entry.last_seen),
                            Reverse(*id.as_bytes()),
                        )
                    })
            })
            .or_else(|| {
                entries().min_by_key(|(id, entry)| {
                    (
                        entry.confirmed,
                        entry.last_success.unwrap_or(0),
                        entry.last_seen,
                        *id.as_bytes(),
                    )
                })
            })
            .map(|(id, _)| *id)
            .expect("an over-full book has an entry")
    }

    fn dial_success(&mut self, node_id: NodeId, now: u64) {
        if let Some(entry) = self.entries.get_mut(&node_id) {
            entry.last_success = Some(now);
            entry.confirmed = true;
            entry.failures = 0;
        }
    }

    fn dial_failure(&mut self, node_id: NodeId) {
        if let Some(entry) = self.entries.get_mut(&node_id) {
            entry.failures = entry.failures.saturating_add(1);
        }
    }

    fn is_sample_eligible(&self, node_id: NodeId, now: u64) -> bool {
        self.entries
            .get(&node_id)
            .is_some_and(|entry| entry.confirmed && entry.expires >= now)
    }
}

fn import_result(result: Result<ImportOutcome, DiscoveryBookError>) -> Expected {
    match result {
        Ok(outcome) => Expected::Outcome(outcome),
        Err(DiscoveryBookError::Record(DiscoveryRecordError::Expired)) => Expected::Expired,
        Err(DiscoveryBookError::Limit {
            field: "records per relay",
            ..
        }) => Expected::RelayFull,
        Err(DiscoveryBookError::Limit {
            field: "records", ..
        }) => Expected::BookFull,
        Err(error) => panic!("the generated records only fail on expiry or limits: {error}"),
    }
}

fn observed_entry(book: &ZakuraDiscoveryBook, node_id: NodeId) -> Option<ModelEntry> {
    book.get(&node_id).map(|entry| ModelEntry {
        sequence: entry.record().body.sequence,
        expires: entry.record().body.expires_at_unix_secs,
        source: entry.source(),
        confirmed: entry.last_confirmed().is_some(),
        failures: entry.failure_count(),
        last_seen: entry.last_seen(),
        last_success: entry.last_success(),
    })
}

proptest! {
    // Each import signs and verifies a record, which is slow in debug builds.
    #![proptest_config(ProptestConfig::with_cases(48))]

    /// After every step the book holds exactly the model's entries, with the
    /// model's sequence, expiry, provenance, confirmation, and dial metadata.
    /// Each import returns the model's outcome, so the per-relay quota and
    /// the record bound hold. Every sample discloses only confirmed unexpired
    /// records, and one requester learns at most one window of node ids per
    /// disclosure epoch.
    #[test]
    fn book_matches_independent_model(
        seed in any::<u64>(),
        ops in prop::collection::vec(book_op(), 1..64),
    ) {
        let local = secret(1).public();
        let limits = ZakuraDiscoveryBookLimits {
            max_records: BOOK_MAX_RECORDS,
            max_records_per_source: BOOK_MAX_PER_RELAY,
            max_imported_records_per_response: SAMPLE_WINDOW,
            ..ZakuraDiscoveryBookLimits::default()
        };
        let mut book = ZakuraDiscoveryBook::with_local_node_id(limits, local);
        let mut model = BookModel::default();
        let mut rng = StdRng::seed_from_u64(seed);
        let mut disclosed: HashMap<(usize, u64), HashSet<NodeId>> = HashMap::new();
        let authors: Vec<NodeSecretKey> = (0..AUTHORS).map(author).collect();
        let mut now = START;

        for op in ops {
            match op {
                BookOp::Import { author, sequence, expires_in, supplier } => {
                    let signer = &authors[author];
                    let node_id = signer.public();
                    let expires = now.saturating_add_signed(expires_in);
                    let address = 10 + u8::try_from(author).expect("test indexes are small");
                    let record = record(signer, address, sequence, expires);
                    let context = context(now);
                    let (source, result) = match supplier {
                        Supplier::Author => {
                            (Some(node_id), book.import_record(record, Some(node_id), now, &context))
                        }
                        Supplier::Relay(index) => {
                            // Relayed records take the production path: verify, then install.
                            let verified = VerifiedRecord::verify(record, &context)
                                .expect("generated records are authentic");
                            let source = Some(relay(index));
                            (source, book.import_verified_record(verified, source, now, &context))
                        }
                        Supplier::Nobody => (None, book.import_record(record, None, now, &context)),
                    };
                    let expected = model.import(node_id, sequence, expires, source, now);
                    prop_assert_eq!(import_result(result), expected);
                }
                BookOp::DialSuccess(author) => {
                    let node_id = authors[author].public();
                    book.mark_dial_success(&node_id, now);
                    model.dial_success(node_id, now);
                }
                BookOp::DialFailure(author) => {
                    let node_id = authors[author].public();
                    book.mark_dial_failure(&node_id, now);
                    model.dial_failure(node_id);
                }
                BookOp::Sample { requester, limit } => {
                    let sample = book.sample_peers(limit, &[], &[], now, relay(requester), &mut rng);
                    prop_assert!(sample.len() <= limit.min(SAMPLE_WINDOW));
                    let ids: HashSet<NodeId> = sample.iter().map(|record| record.body.node_id).collect();
                    prop_assert_eq!(ids.len(), sample.len(), "a sample repeats no node");
                    for record in &sample {
                        let node_id = record.body.node_id;
                        prop_assert!(model.is_sample_eligible(node_id, now), "disclosed {:?}", node_id);
                        prop_assert_eq!(
                            Some(record.body.sequence),
                            model.entries.get(&node_id).map(|entry| entry.sequence)
                        );
                    }
                    let window = disclosed.entry((requester, now / DISCLOSURE_EPOCH)).or_default();
                    window.extend(ids);
                    prop_assert!(window.len() <= SAMPLE_WINDOW, "one requester saw {} ids in an epoch", window.len());
                }
                BookOp::Advance(secs) => now += secs,
            }

            prop_assert_eq!(book.discovered_len(), model.entries.len());
            prop_assert!(model.entries.len() <= BOOK_MAX_RECORDS);
            for signer in &authors {
                let node_id = signer.public();
                prop_assert_eq!(observed_entry(&book, node_id), model.entries.get(&node_id).cloned());
            }
            for index in 0..RELAYS {
                prop_assert!(model.relayed_by(relay(index)) <= BOOK_MAX_PER_RELAY);
            }
        }
    }
}

// DiscoveryState model.

fn local_config() -> ZakuraDiscoveryLocalConfig {
    ZakuraDiscoveryLocalConfig {
        secret_key: secret(1),
        direct_addrs: Vec::new(),
        services: vec![ZakuraServiceId::discovery()],
        zakura_protocol_min: ZAKURA_PROTOCOL_VERSION_CURRENT,
        zakura_protocol_max: ZAKURA_PROTOCOL_VERSION_CURRENT,
        network_id: ZakuraNetworkId::Regtest,
        chain_id: CHAIN_ID,
        last_authored_sequence: None,
    }
}

fn peer_limits() -> ServicePeerLimits {
    ServicePeerLimits {
        max_inbound_peers: PEER_CAP,
        max_outbound_peers: PEER_CAP,
        ..ServicePeerLimits::default()
    }
}

fn discovery_state() -> DiscoveryState {
    let local = LocalRecordState::new(&local_config(), 0).expect("the local config is valid");
    let config = ZakuraDiscoveryConfig {
        peer_limits: peer_limits(),
        ..ZakuraDiscoveryConfig::default()
    };
    DiscoveryState::new(local, config, RandomState::new())
}

/// A peer's own `Services`, declared to expire at `expires`.
fn services(node_id: NodeId, expires: u64) -> Services {
    Services {
        node_id,
        expires_at_unix_secs: expires,
        summaries: vec![ServiceSummaryEnvelope::discovery(&DiscoveryServiceSummary {
            peer_exchange_slots_free: 1,
            max_records_per_response: 32,
            expected_disconnect_after_exchange: true,
        })
        .expect("the summary encodes")],
    }
}

/// A peer's own `Hello` record, valid for the whole test.
fn hello(peer: usize) -> ZakuraNodeRecord {
    let address = 100 + u8::try_from(peer).expect("test indexes are small");
    record(&peer_secret(peer), address, 1, START + 50_000)
}

fn session(peer: usize, conn_id: ZakuraConnId, session_id: u64) -> DiscoverySessionKey {
    DiscoverySessionKey::new(peer_id(peer_secret(peer).public()), conn_id, session_id)
}

fn direction(outbound: bool) -> ServicePeerDirection {
    if outbound {
        ServicePeerDirection::Outbound
    } else {
        ServicePeerDirection::Inbound
    }
}

#[derive(Clone, Debug)]
enum StateOp {
    Admit {
        peer: usize,
        conn_id: ZakuraConnId,
        session_id: u64,
        outbound: bool,
    },
    RemoveSession {
        peer: usize,
        conn_id: ZakuraConnId,
        session_id: u64,
    },
    RemoveConnection {
        peer: usize,
        conn_id: ZakuraConnId,
    },
    SetConnected {
        peer: usize,
        connected: bool,
    },
    /// Verifies a `Hello` for a session. Its install is a later `FinishHello`.
    StartHello {
        peer: usize,
        conn_id: ZakuraConnId,
        session_id: u64,
    },
    FinishHello(usize),
    ReceiveServices {
        peer: usize,
        conn_id: ZakuraConnId,
        session_id: u64,
        ttl: u64,
    },
    Advance(u64),
}

fn state_op() -> impl Strategy<Value = StateOp> {
    let key = || (0..PEERS, 0u64..3, 0u64..3);
    prop_oneof![
        3 => (key(), any::<bool>()).prop_map(|((peer, conn_id, session_id), outbound)| StateOp::Admit {
            peer,
            conn_id,
            session_id,
            outbound,
        }),
        1 => key().prop_map(|(peer, conn_id, session_id)| StateOp::RemoveSession {
            peer,
            conn_id,
            session_id,
        }),
        1 => (0..PEERS, 0u64..3).prop_map(|(peer, conn_id)| StateOp::RemoveConnection { peer, conn_id }),
        2 => (0..PEERS, any::<bool>()).prop_map(|(peer, connected)| StateOp::SetConnected { peer, connected }),
        2 => key().prop_map(|(peer, conn_id, session_id)| StateOp::StartHello {
            peer,
            conn_id,
            session_id,
        }),
        2 => (0usize..8).prop_map(StateOp::FinishHello),
        3 => (key(), 1u64..60).prop_map(|((peer, conn_id, session_id), ttl)| StateOp::ReceiveServices {
            peer,
            conn_id,
            session_id,
            ttl,
        }),
        2 => (0u64..40).prop_map(StateOp::Advance),
    ]
}

/// The model's view of one peer.
#[derive(Clone, Debug, Default)]
struct PeerModel {
    /// The admitted `(connection, session, outbound)`.
    admitted: Option<(ZakuraConnId, u64, bool)>,
    connected: bool,
    /// Whether the book holds the peer's `Hello` record.
    in_book: bool,
    /// Whether the peer's `Hello` backs a service hint.
    hinted: bool,
    /// Local expiry of the peer's live summaries.
    live_until: Option<u64>,
}

/// An independent restatement of the session and service-hint rules.
struct StateModel {
    peers: Vec<PeerModel>,
}

impl StateModel {
    fn new() -> Self {
        Self {
            peers: vec![PeerModel::default(); PEERS],
        }
    }

    fn is_current(&self, peer: usize, conn_id: ZakuraConnId, session_id: u64) -> bool {
        self.peers[peer]
            .admitted
            .is_some_and(|(conn, session, _)| (conn, session) == (conn_id, session_id))
    }

    fn admitted_in(&self, outbound: bool) -> usize {
        self.peers
            .iter()
            .filter(|peer| peer.admitted.is_some_and(|(_, _, out)| out == outbound))
            .count()
    }

    fn admit(
        &mut self,
        peer: usize,
        conn_id: ZakuraConnId,
        session_id: u64,
        outbound: bool,
    ) -> ServiceAdmissionDecision {
        if let Some((conn, session, _)) = self.peers[peer].admitted {
            // Only a later (connection, session) replaces an admitted session.
            if (conn_id, session_id) <= (conn, session) {
                return ServiceAdmissionDecision::RejectNotUseful;
            }
        } else if self.admitted_in(outbound) >= PEER_CAP {
            return ServiceAdmissionDecision::RejectFull;
        }
        self.peers[peer].admitted = Some((conn_id, session_id, outbound));
        ServiceAdmissionDecision::Admit
    }

    /// Drops what a disconnected peer or an expired summary no longer backs.
    fn sync(&mut self, now: u64) {
        for peer in &mut self.peers {
            if !peer.connected {
                peer.hinted = false;
                peer.live_until = None;
            }
            if peer.live_until.is_some_and(|until| until <= now) {
                peer.live_until = None;
            }
        }
    }

    fn connected_node_ids(&self) -> Vec<NodeId> {
        (0..PEERS)
            .filter(|peer| self.peers[*peer].connected)
            .map(|peer| peer_secret(peer).public())
            .collect()
    }
}

proptest! {
    /// After every step, admitted sessions, slot counts, and each peer's
    /// service hints and live summaries match the model. A `Hello` or
    /// `Services` from a session that is no longer admitted installs nothing,
    /// so a late verification result cannot act for its replacement.
    #[test]
    fn discovery_state_matches_independent_model(ops in prop::collection::vec(state_op(), 1..64)) {
        let mut state = discovery_state();
        let mut model = StateModel::new();
        let mut pending: Vec<(usize, ZakuraConnId, u64, VerifiedRecord)> = Vec::new();
        // Each peer's `Hello` verifies once; every start reuses the evidence.
        let hellos: Vec<VerifiedRecord> = (0..PEERS)
            .map(|peer| VerifiedRecord::verify(hello(peer), &context(START)).expect("generated records are authentic"))
            .collect();
        let mut now = START;

        for op in ops {
            match op {
                StateOp::Admit { peer, conn_id, session_id, outbound } => {
                    let decision = state.admit_session(session(peer, conn_id, session_id), direction(outbound));
                    prop_assert_eq!(decision, model.admit(peer, conn_id, session_id, outbound));
                }
                StateOp::RemoveSession { peer, conn_id, session_id } => {
                    state.remove_session(&session(peer, conn_id, session_id));
                    if model.is_current(peer, conn_id, session_id) {
                        model.peers[peer].admitted = None;
                    }
                }
                StateOp::RemoveConnection { peer, conn_id } => {
                    state.remove_connection(&session(peer, conn_id, 0).peer_id, conn_id);
                    if model.peers[peer].admitted.is_some_and(|(conn, _, _)| conn == conn_id) {
                        model.peers[peer].admitted = None;
                    }
                }
                StateOp::SetConnected { peer, connected } => model.peers[peer].connected = connected,
                StateOp::StartHello { peer, conn_id, session_id } => {
                    pending.push((peer, conn_id, session_id, hellos[peer].clone()));
                }
                StateOp::FinishHello(index) => {
                    if pending.is_empty() {
                        continue;
                    }
                    let (peer, conn_id, session_id, verified) = pending.remove(index % pending.len());
                    let result = state.install_connected_peer_record(
                        &session(peer, conn_id, session_id),
                        verified,
                        now,
                    );
                    if model.is_current(peer, conn_id, session_id) {
                        let expected = if model.peers[peer].in_book {
                            ImportOutcome::MetadataUpdated
                        } else {
                            ImportOutcome::Added
                        };
                        prop_assert_eq!(result.ok(), Some(expected));
                        model.peers[peer].in_book = true;
                        model.peers[peer].hinted = true;
                    } else {
                        prop_assert!(matches!(result, Err(DiscoveryBookError::SupersededSession)));
                    }
                }
                StateOp::ReceiveServices { peer, conn_id, session_id, ttl } => {
                    let node_id = peer_secret(peer).public();
                    let connected = model.connected_node_ids();
                    state
                        .receive_live_summaries(
                            &session(peer, conn_id, session_id),
                            node_id,
                            services(node_id, now + ttl),
                            &connected,
                            now,
                        )
                        .expect("generated summaries decode");
                    model.sync(now);
                    if model.peers[peer].connected && model.is_current(peer, conn_id, session_id) {
                        model.peers[peer].live_until = Some((now + ttl).min(now + LIVE_SUMMARY_TTL));
                    }
                }
                StateOp::Advance(secs) => now += secs,
            }

            let connected = model.connected_node_ids();
            model.sync(now);
            for (index, peer) in model.peers.iter().enumerate() {
                let node_id = peer_secret(index).public();
                let has_entry = peer.hinted || peer.live_until.is_some();
                prop_assert_eq!(
                    state.active_services(node_id, &connected, now).is_some(),
                    peer.connected && has_entry,
                    "peer {} service hints", index
                );
                prop_assert_eq!(
                    state
                        .live_service_summaries(node_id, &connected, now)
                        .map(|summaries| !summaries.is_empty()),
                    (peer.connected && has_entry).then_some(peer.live_until.is_some()),
                    "peer {} live summaries", index
                );
                if let Some((conn_id, session_id, _)) = peer.admitted {
                    prop_assert!(state.is_current_session(&session(index, conn_id, session_id)));
                }
            }
            prop_assert_eq!(
                state.peer_snapshot(),
                ServicePeerSnapshot::new(model.admitted_in(false), model.admitted_in(true), peer_limits())
            );
        }
    }
}

// Worked scenarios.

/// Session A's `Hello` verifies, B replaces A, and then A's verification
/// completes. A installs no record, no service hint, and no live summary, and
/// B stays admitted and serves normally.
#[test]
fn late_verification_for_a_replaced_session_changes_nothing() {
    let mut state = discovery_state();
    let node_id = peer_secret(0).public();
    let connected = [node_id];
    let a = session(0, 1, 1);
    let b = session(0, 1, 2);
    let now = START;

    assert_eq!(
        state.admit_session(a.clone(), ServicePeerDirection::Inbound),
        ServiceAdmissionDecision::Admit
    );
    let verified_for_a =
        VerifiedRecord::verify(hello(0), &context(now)).expect("the hello is authentic");
    assert_eq!(
        state.admit_session(b.clone(), ServicePeerDirection::Inbound),
        ServiceAdmissionDecision::Admit
    );

    assert!(matches!(
        state.install_connected_peer_record(&a, verified_for_a, now),
        Err(DiscoveryBookError::SupersededSession)
    ));
    state
        .receive_live_summaries(&a, node_id, services(node_id, now + 10), &connected, now)
        .expect("the summaries decode");
    assert_eq!(state.book().get(&node_id), None);
    assert_eq!(state.active_services(node_id, &connected, now), None);
    assert_eq!(state.live_service_summaries(node_id, &connected, now), None);
    assert!(
        state.is_current_session(&b),
        "A's late result keeps B admitted"
    );
    assert_eq!(state.peer_snapshot().inbound_peers, 1);

    // A's late disconnect does not remove B either.
    state.remove_session(&a);
    assert!(state.is_current_session(&b));

    let verified_for_b =
        VerifiedRecord::verify(hello(0), &context(now)).expect("the hello is authentic");
    assert_eq!(
        state
            .install_connected_peer_record(&b, verified_for_b, now)
            .ok(),
        Some(ImportOutcome::Added)
    );
    state
        .receive_live_summaries(&b, node_id, services(node_id, now + 10), &connected, now)
        .expect("the summaries decode");
    assert_eq!(
        state
            .live_service_summaries(node_id, &connected, now)
            .map(|summaries| summaries.len()),
        Some(1)
    );
}

/// A peer's record hint ends at its signed expiry, and its live summaries end
/// at their local TTL. Moving the time past the signed expiry while the local
/// TTL has not passed drops the record hint and keeps the live summaries.
///
/// Both deadlines are Unix seconds today; the test pins that they are two
/// separate deadlines.
#[test]
fn signed_record_expiry_does_not_expire_the_live_summary_ttl() {
    let mut state = discovery_state();
    let node_id = peer_secret(0).public();
    let connected = [node_id];
    let a = session(0, 1, 1);
    let now = START;
    let signed_expiry = now + 5;

    state.admit_session(a.clone(), ServicePeerDirection::Inbound);
    let short_lived = record_with_services(
        &peer_secret(0),
        100,
        1,
        signed_expiry,
        vec![ZakuraServiceId::discovery(), ZakuraServiceId::block_sync()],
    );
    let verified = VerifiedRecord::verify(short_lived, &context(now)).expect("authentic");
    state
        .install_connected_peer_record(&a, verified, now)
        .expect("the record installs");
    state
        .receive_live_summaries(&a, node_id, services(node_id, u64::MAX), &connected, now)
        .expect("the summaries decode");
    assert_eq!(
        state.active_services(node_id, &connected, now),
        Some(vec![
            ZakuraServiceId::block_sync(),
            ZakuraServiceId::discovery()
        ]),
        "the record hint lists its signed services"
    );

    let after_signed_expiry = signed_expiry + 1;
    assert_eq!(
        state.active_services(node_id, &connected, after_signed_expiry),
        Some(vec![ZakuraServiceId::discovery()]),
        "only the live summaries back the hint after the signed expiry"
    );
    let live = state
        .live_service_summaries(node_id, &connected, after_signed_expiry)
        .expect("the peer keeps an entry");
    assert_eq!(live.len(), 1, "the local TTL has not passed");
    assert_eq!(live[0].expires_at_unix_secs, now + LIVE_SUMMARY_TTL);

    assert_eq!(
        state.live_service_summaries(node_id, &connected, now + LIVE_SUMMARY_TTL),
        None,
        "the local TTL has passed"
    );
}

/// The self-record sequence starts after the persisted sequence or at the
/// wall-clock seed, whichever is later, and strictly increases.
#[test]
fn self_record_sequence_resumes_after_the_persisted_sequence() {
    let restarted = ZakuraDiscoveryLocalConfig {
        last_authored_sequence: Some(100),
        ..local_config()
    };
    let mut local = LocalRecordState::new(&restarted, 5).expect("valid config");
    let ttl = Duration::from_secs(60);
    assert_eq!(local.next_body(START, 5, ttl).sequence, 101);
    assert_eq!(local.next_body(START, 5, ttl).sequence, 102);
    assert_eq!(local.next_body(START, 1_000, ttl).sequence, 1_000);
    assert_eq!(
        local.next_body(START, 1_000, ttl).expires_at_unix_secs,
        START + 60
    );

    let mut state = discovery_state();
    let body = state.next_self_record_body(START, 7);
    let current = ZakuraNodeRecord::sign(body, &secret(1)).expect("the local key signs");
    let refresh = ZakuraDiscoveryConfig::default().refresh_interval.as_secs();
    let expires = current.body.expires_at_unix_secs;
    assert!(state.self_record_is_fresh(&current, expires - refresh - 1));
    assert!(!state.self_record_is_fresh(&current, expires - refresh));
}
