//! Machine models and drivers for discovery's shared state: the record book and
//! [`DiscoveryState`], each checked against an independent model.
//!
//! The models restate the documented rules. They never call the code they
//! check. `zakura-sim` replays these drivers through [`trace_discovery_book_bytes`]
//! and [`trace_discovery_state_bytes`].

use std::{
    any::Any,
    cmp::Reverse,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    panic::{catch_unwind, AssertUnwindSafe},
    time::Duration,
};

use rand_10::{rngs::StdRng, SeedableRng};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use zakura_quic::{NodeId, NodeSecretKey};
use zakura_rt::collections::{HashMap, HashSet};

use super::protocol::{
    BookChange, DisclosureKey, DiscoveryBookError, DiscoveryRecordError,
    DiscoveryRecordValidationContext, DiscoveryServiceSummary, EvictionCause, ImportOutcome,
    ServiceSummaryEnvelope, Services, ZakuraDiscoveryBook, ZakuraDiscoveryBookLimits,
    ZakuraDiscoveryConfig, ZakuraDiscoveryLocalConfig, ZakuraNodeRecord, ZakuraNodeRecordBody,
    ZakuraServiceId,
};
use super::state::{DiscoverySessionKey, DiscoveryState, LocalRecordState, SessionRecordError};
use super::verified::VerifiedRecord;
use crate::zakura::{
    ServiceAdmissionDecision, ServicePeerDirection, ServicePeerLimits, ServicePeerSnapshot,
    ZakuraConnId, ZakuraNetworkId, ZakuraPeerId, ZAKURA_PROTOCOL_VERSION_CURRENT,
};

#[cfg(test)]
mod state_tests;

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

/// One import's result in a machine step.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ImportFact {
    /// A new entry stored the record.
    Added,
    /// The record replaced an entry's older record.
    Updated,
    /// An equal sequence refreshed the entry's metadata.
    MetadataUpdated,
    /// The stored record's sequence is later.
    IgnoredOlder,
    /// The record expired before it arrived.
    Expired,
    /// The supplier's relay quota refused the record.
    RelayFull,
    /// The book evicted the new record at its record limit.
    BookFull,
}

impl From<ImportOutcome> for ImportFact {
    fn from(outcome: ImportOutcome) -> Self {
        match outcome {
            ImportOutcome::Added => Self::Added,
            ImportOutcome::Updated => Self::Updated,
            ImportOutcome::MetadataUpdated => Self::MetadataUpdated,
            ImportOutcome::IgnoredOlder => Self::IgnoredOlder,
        }
    }
}

/// An independent restatement of the book's documented rules.
#[derive(Default)]
struct BookModel {
    entries: HashMap<NodeId, ModelEntry>,
    /// The record changes the current operation made.
    changes: Vec<BookChange>,
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

    /// Each relay's attributed entries, in node id order.
    fn relayed_counts(&self) -> Vec<(String, usize)> {
        let mut counts: HashMap<NodeId, usize> = HashMap::new();
        for (node_id, entry) in &self.entries {
            if let Some(relay) = Self::relay_of(entry.source, *node_id) {
                *counts.entry(relay).or_default() += 1;
            }
        }
        let mut counts: Vec<_> = counts
            .into_iter()
            .map(|(relay, count)| (hex::encode(relay.as_bytes()), count))
            .collect();
        counts.sort_unstable();
        counts
    }

    fn import(
        &mut self,
        node_id: NodeId,
        sequence: u64,
        expires: u64,
        source: Option<NodeId>,
        now: u64,
    ) -> ImportFact {
        if expires.saturating_add(SKEW) < now {
            return ImportFact::Expired;
        }
        let first_party = source == Some(node_id);
        let relay = Self::relay_of(source, node_id);
        let relay_full = relay.is_some_and(|relay| self.relayed_by(relay) >= BOOK_MAX_PER_RELAY);

        if let Some(entry) = self.entries.get_mut(&node_id) {
            // An expired record no longer orders its node's records.
            let stored_orders = entry.expires >= now;
            if stored_orders && sequence < entry.sequence {
                return ImportFact::IgnoredOlder;
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
                return ImportFact::MetadataUpdated;
            }
            self.changes.push(BookChange::Replaced {
                node_id,
                previous_sequence: entry.sequence,
                sequence,
                source: entry.source,
            });
            entry.sequence = sequence;
            entry.expires = expires;
            return ImportFact::Updated;
        }

        if let Some(relay) = relay.filter(|_| relay_full) {
            let mut expired: Vec<NodeId> = self
                .entries
                .iter()
                .filter(|(id, entry)| {
                    Self::relay_of(entry.source, **id) == Some(relay) && entry.expires < now
                })
                .map(|(id, _)| *id)
                .collect();
            if expired.is_empty() {
                return ImportFact::RelayFull;
            }
            expired.sort_unstable_by_key(|id| *id.as_bytes());
            for id in expired {
                let entry = self.entries.remove(&id).expect("the filter found this id");
                self.changes.push(BookChange::Evicted {
                    node_id: id,
                    sequence: entry.sequence,
                    cause: EvictionCause::RelayQuota,
                });
            }
        }
        self.changes.push(BookChange::Installed {
            node_id,
            sequence,
            source,
        });
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
            let entry = self.entries.remove(&victim).expect("the victim is stored");
            self.changes.push(BookChange::Evicted {
                node_id: victim,
                sequence: entry.sequence,
                cause: EvictionCause::BookLimit,
            });
            if victim == node_id {
                return ImportFact::BookFull;
            }
        }
        ImportFact::Added
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

fn import_result(result: Result<ImportOutcome, DiscoveryBookError>) -> ImportFact {
    match result {
        Ok(outcome) => outcome.into(),
        Err(DiscoveryBookError::Record(DiscoveryRecordError::Expired)) => ImportFact::Expired,
        Err(DiscoveryBookError::Limit {
            field: "records per relay",
            ..
        }) => ImportFact::RelayFull,
        Err(DiscoveryBookError::Limit {
            field: "records", ..
        }) => ImportFact::BookFull,
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

const BOOK_PROPERTY: &str = "discovery/record-book";

/// The real book and its model at one point in a history.
struct BookDriver {
    book: ZakuraDiscoveryBook,
    model: BookModel,
    rng: StdRng,
    disclosed: HashMap<(usize, u64), HashSet<NodeId>>,
    authors: Vec<NodeSecretKey>,
    now: u64,
    /// The record changes the book reported for the current operation.
    changes: Vec<BookChange>,
    /// The current import's result, if the operation imported a record.
    result: Option<ImportFact>,
    /// What the model predicts for the current import.
    expected_result: Option<ImportFact>,
    /// The current sample's signed records, as they would go on the wire.
    sample: Vec<Vec<u8>>,
}

impl BookDriver {
    /// A driver whose controlled disclosure key and RNG both come from `seed`.
    fn new(seed: u64) -> Self {
        let local = secret(1).public();
        let limits = ZakuraDiscoveryBookLimits {
            max_records: BOOK_MAX_RECORDS,
            max_records_per_source: BOOK_MAX_PER_RELAY,
            max_imported_records_per_response: SAMPLE_WINDOW,
            ..ZakuraDiscoveryBookLimits::default()
        };
        let mut key = [0; 32];
        for part in key.as_chunks_mut::<8>().0 {
            *part = seed.to_le_bytes();
        }
        Self {
            book: ZakuraDiscoveryBook::with_disclosure_key(
                limits,
                Some(local),
                DisclosureKey::new(key),
            ),
            model: BookModel::default(),
            rng: StdRng::seed_from_u64(seed),
            disclosed: HashMap::new(),
            authors: (0..AUTHORS).map(author).collect(),
            now: START,
            changes: Vec::new(),
            result: None,
            expected_result: None,
            sample: Vec::new(),
        }
    }

    fn apply(&mut self, op: BookOp) {
        self.changes.clear();
        self.model.changes.clear();
        self.result = None;
        self.expected_result = None;
        self.sample.clear();
        let now = self.now;
        match op {
            BookOp::Import {
                author,
                sequence,
                expires_in,
                supplier,
            } => {
                let signer = &self.authors[author];
                let node_id = signer.public();
                let expires = now.saturating_add_signed(expires_in);
                let address = 10 + u8::try_from(author).expect("test indexes are small");
                let record = record(signer, address, sequence, expires);
                let context = context(now);
                let source = match supplier {
                    Supplier::Author => Some(node_id),
                    Supplier::Relay(index) => Some(relay(index)),
                    Supplier::Nobody => None,
                };
                // Every supplier's record takes the production path: verify, then install.
                let result = match VerifiedRecord::verify(record, &context) {
                    Ok(verified) => self.book.import_verified_record(
                        verified,
                        source,
                        now,
                        &context,
                        &mut self.changes,
                    ),
                    Err(error) => Err(error.into()),
                };
                self.expected_result =
                    Some(self.model.import(node_id, sequence, expires, source, now));
                self.result = Some(import_result(result));
            }
            BookOp::DialSuccess(author) => {
                let node_id = self.authors[author].public();
                self.book.mark_dial_success(&node_id, now);
                self.model.dial_success(node_id, now);
            }
            BookOp::DialFailure(author) => {
                let node_id = self.authors[author].public();
                self.book.mark_dial_failure(&node_id, now);
                self.model.dial_failure(node_id);
            }
            BookOp::Sample { requester, limit } => {
                let sample = self.book.sample_peers_with_rng(
                    limit,
                    &[],
                    &[],
                    now,
                    relay(requester),
                    &mut self.rng,
                );
                self.sample = sample.iter().map(signed_bytes).collect();
                assert!(sample.len() <= limit.min(SAMPLE_WINDOW), "{BOOK_PROPERTY}");
                let ids: HashSet<NodeId> =
                    sample.iter().map(|record| record.body.node_id).collect();
                assert_eq!(
                    ids.len(),
                    sample.len(),
                    "{BOOK_PROPERTY}: a sample repeats no node"
                );
                for record in &sample {
                    let node_id = record.body.node_id;
                    assert!(
                        self.model.is_sample_eligible(node_id, now),
                        "{BOOK_PROPERTY}: disclosed {:?}",
                        node_id
                    );
                    assert_eq!(
                        Some(record.body.sequence),
                        self.model.entries.get(&node_id).map(|entry| entry.sequence),
                        "{BOOK_PROPERTY}"
                    );
                }
                let window = self
                    .disclosed
                    .entry((requester, now / DISCLOSURE_EPOCH))
                    .or_default();
                window.extend(ids);
                assert!(
                    window.len() <= SAMPLE_WINDOW,
                    "{BOOK_PROPERTY}: one requester saw {} ids in an epoch",
                    window.len()
                );
            }
            BookOp::Advance(secs) => {
                self.now += secs;
            }
        }
    }

    fn check(&mut self, after_step: &mut impl FnMut(&mut ZakuraDiscoveryBook, u64)) {
        after_step(&mut self.book, self.now);
        assert_view_matches(BOOK_PROPERTY, &self.actual_view(), &self.expected_view());
        assert!(
            self.model.entries.len() <= BOOK_MAX_RECORDS,
            "{BOOK_PROPERTY}"
        );
        assert!(
            self.book.sample_snapshot_len() <= BOOK_MAX_RECORDS.max(SAMPLE_WINDOW),
            "{BOOK_PROPERTY}: the sample snapshot exceeds the book"
        );
        for index in 0..RELAYS {
            assert!(
                self.model.relayed_by(relay(index)) <= BOOK_MAX_PER_RELAY,
                "{BOOK_PROPERTY}"
            );
        }
    }

    fn actual_view(&self) -> BookView {
        let mut entries: Vec<_> = self
            .authors
            .iter()
            .filter_map(|signer| {
                observed_entry(&self.book, signer.public())
                    .map(|entry| entry_fact(signer.public(), &entry))
            })
            .collect();
        entries.sort_unstable_by(|left, right| left.node.cmp(&right.node));
        BookView {
            now: self.now,
            discovered: self.book.discovered_len(),
            entries,
            relayed: self
                .book
                .relayed_counts()
                .into_iter()
                .map(|(relay, count)| (hex::encode(relay.as_bytes()), count))
                .collect(),
            changes: self.changes.iter().map(BookFact::from).collect(),
            result: self.result,
        }
    }

    fn expected_view(&self) -> BookView {
        let mut entries: Vec<_> = self
            .model
            .entries
            .iter()
            .map(|(node_id, entry)| entry_fact(*node_id, entry))
            .collect();
        entries.sort_unstable_by(|left, right| left.node.cmp(&right.node));
        BookView {
            now: self.now,
            discovered: self.model.entries.len(),
            entries,
            relayed: self.model.relayed_counts(),
            changes: self.model.changes.iter().map(BookFact::from).collect(),
            result: self.expected_result,
        }
    }
}

/// `record`'s bytes as `Peers` carries them.
fn signed_bytes(record: &ZakuraNodeRecord) -> Vec<u8> {
    let mut bytes = record
        .body
        .encode_for_signature()
        .expect("a stored record encodes");
    bytes.extend_from_slice(&record.signature.to_bytes());
    bytes
}

/// One stored entry in a machine step, by node id in hex.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BookEntryFact {
    /// The record's author.
    pub node: String,
    /// The stored record's sequence.
    pub sequence: u64,
    /// The stored record's signed expiry.
    pub expires: u64,
    /// The peer that supplied the record.
    pub source: Option<String>,
    /// Whether this node vouches for the entry in peer samples.
    pub confirmed: bool,
    /// Consecutive dial failures.
    pub failures: u32,
    /// When a peer last supplied the record.
    pub last_seen: u64,
    /// The last successful dial.
    pub last_success: Option<u64>,
}

fn entry_fact(node_id: NodeId, entry: &ModelEntry) -> BookEntryFact {
    BookEntryFact {
        node: hex::encode(node_id.as_bytes()),
        sequence: entry.sequence,
        expires: entry.expires,
        source: entry.source.map(|source| hex::encode(source.as_bytes())),
        confirmed: entry.confirmed,
        failures: entry.failures,
        last_seen: entry.last_seen,
        last_success: entry.last_success,
    }
}

/// One change to the book's stored records in a machine step.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "change", rename_all = "kebab-case", deny_unknown_fields)]
pub enum BookFact {
    /// A new entry stored its first record.
    Installed {
        /// The record's author.
        node: String,
        /// The record's sequence.
        sequence: u64,
        /// The peer that supplied the record.
        source: Option<String>,
    },
    /// An entry's record replaced its old one.
    Replaced {
        /// The record's author.
        node: String,
        /// The old record's sequence.
        previous_sequence: u64,
        /// The new record's sequence.
        sequence: u64,
        /// The entry's source after the replacement.
        source: Option<String>,
    },
    /// An entry left the book.
    Evicted {
        /// The record's author.
        node: String,
        /// The removed record's sequence.
        sequence: u64,
        /// Why the entry left.
        cause: EvictionFact,
    },
}

/// Why an entry left the book in a machine step.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EvictionFact {
    /// An expired entry gave way to its relay's next record.
    RelayQuota,
    /// The book exceeded its record limit.
    BookLimit,
}

impl From<&BookChange> for BookFact {
    fn from(change: &BookChange) -> Self {
        let source_hex = |source: &Option<NodeId>| source.map(|id| hex::encode(id.as_bytes()));
        match change {
            BookChange::Installed {
                node_id,
                sequence,
                source,
            } => Self::Installed {
                node: hex::encode(node_id.as_bytes()),
                sequence: *sequence,
                source: source_hex(source),
            },
            BookChange::Replaced {
                node_id,
                previous_sequence,
                sequence,
                source,
            } => Self::Replaced {
                node: hex::encode(node_id.as_bytes()),
                previous_sequence: *previous_sequence,
                sequence: *sequence,
                source: source_hex(source),
            },
            BookChange::Evicted {
                node_id,
                sequence,
                cause,
            } => Self::Evicted {
                node: hex::encode(node_id.as_bytes()),
                sequence: *sequence,
                cause: match cause {
                    EvictionCause::RelayQuota => EvictionFact::RelayQuota,
                    EvictionCause::BookLimit => EvictionFact::BookLimit,
                },
            },
        }
    }
}

/// The book's state after one operation, read from the owner or derived by its model.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BookView {
    /// The explicit fixture time, in Unix seconds.
    pub now: u64,
    /// Discovered records in the book.
    pub discovered: usize,
    /// Stored entries in node id order.
    pub entries: Vec<BookEntryFact>,
    /// Each relay's attributed entries, in node id order.
    pub relayed: Vec<(String, usize)>,
    /// The record changes this operation made, in order.
    pub changes: Vec<BookFact>,
    /// The import's result, for an import operation.
    pub result: Option<ImportFact>,
}

/// One decoder-v2 record-book operation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BookStep {
    /// The operation's number, from 1.
    pub step: usize,
    /// The original causal bytes.
    pub input: Vec<u8>,
    /// State read from the real book.
    pub actual: BookView,
    /// State derived independently by the model.
    pub expected: BookView,
    /// The sample's signed records in hex, for a sample operation.
    pub sample: Vec<String>,
    /// How many node ids the current sample snapshot retains.
    pub snapshot_ids: usize,
    /// The first failure, if this operation stopped the run.
    pub failure: Option<DriverFailure>,
}

/// Why a machine driver stopped. A driver panic cannot become a property detection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DriverFailure {
    /// Whether the independent check named its property.
    pub property_failure: bool,
    /// The panic payload, without the panic hook's source location.
    pub message: String,
}

/// Fails `property` unless the owner's view matches the model's.
///
/// The panic names only the first differing field. The step record keeps both
/// views, so a shrink can delete an unrelated operation and keep the same failure.
fn assert_view_matches<V: Eq + Serialize>(property: &str, actual: &V, expected: &V) {
    if actual == expected {
        return;
    }
    let values = (serde_json::to_value(actual), serde_json::to_value(expected));
    let field = match &values {
        (Ok(Value::Object(actual)), Ok(expected)) => actual
            .iter()
            .find(|(name, value)| expected.get(name.as_str()) != Some(*value))
            .map(|(name, _)| name.as_str()),
        _ => None,
    };
    panic!(
        "{property}: {} differs from the model",
        field.unwrap_or("the view")
    );
}

fn driver_failure(payload: &(dyn Any + Send), property: &str) -> DriverFailure {
    let message = payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            payload
                .downcast_ref::<&str>()
                .map(|text| (*text).to_owned())
        })
        .unwrap_or_else(|| "non-string panic payload".to_owned());
    DriverFailure {
        property_failure: message.contains(property),
        message,
    }
}

/// The causal bytes of 1-based operation `step`.
fn step_input(input: &[u8], step: usize) -> Vec<u8> {
    input[(step - 1) * 4..(step * 4).min(input.len())].to_vec()
}

/// Runs record-book decoder-v2 operations under a fixed disclosure key and fixture
/// clock, and captures each operation. The history stops at the first failure.
pub fn trace_discovery_book_bytes(data: &[u8]) -> Vec<BookStep> {
    let input = &data[..data.len().min(512)];
    let mut driver = BookDriver::new(0);
    let mut steps = Vec::new();
    for (index, op) in decode_book_v2(input).enumerate() {
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            driver.apply(op);
            driver.check(&mut |_, _| {});
        }));
        let failure = outcome
            .err()
            .map(|payload| driver_failure(payload.as_ref(), BOOK_PROPERTY));
        let stop = failure.is_some();
        steps.push(BookStep {
            step: index + 1,
            input: step_input(input, index + 1),
            actual: driver.actual_view(),
            expected: driver.expected_view(),
            sample: driver.sample.iter().map(hex::encode).collect(),
            snapshot_ids: driver.book.sample_snapshot_len(),
            failure,
        });
        if stop {
            break;
        }
    }
    steps
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
    DiscoveryState::new(local, config, DisclosureKey::new([0; 32]))
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
    /// Reads each peer's hints, as a service query does.
    ReadHints,
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
        match self.peers[peer].admitted {
            // Only a later (connection, session) replaces an admitted session.
            Some((conn, session, _)) if (conn_id, session_id) <= (conn, session) => {
                return ServiceAdmissionDecision::RejectNotUseful;
            }
            // A same-direction replacement keeps its slot.
            Some((_, _, out)) if out == outbound => {}
            // A new peer or a direction change needs a free destination slot.
            _ if self.admitted_in(outbound) >= PEER_CAP => {
                return ServiceAdmissionDecision::RejectFull;
            }
            _ => {}
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

const STATE_PROPERTY: &str = "discovery/session-installation";

/// The real state and its model at one point in a history.
struct StateDriver {
    state: DiscoveryState,
    model: StateModel,
    pending: Vec<(usize, ZakuraConnId, u64, VerifiedRecord)>,
    hellos: Vec<VerifiedRecord>,
    now: u64,
    /// The current operation's installation result.
    result: Option<InstallFact>,
    /// What the model predicts for the current operation's installation.
    expected_result: Option<InstallFact>,
}

impl StateDriver {
    fn new() -> Self {
        Self {
            state: discovery_state(),
            model: StateModel::new(),
            pending: Vec::new(),
            // Each peer's `Hello` verifies once; every start reuses the evidence.
            hellos: (0..PEERS)
                .map(|peer| {
                    VerifiedRecord::verify(hello(peer), &context(START))
                        .expect("generated records are authentic")
                })
                .collect(),
            now: START,
            result: None,
            expected_result: None,
        }
    }

    fn apply(&mut self, op: StateOp) {
        self.result = None;
        self.expected_result = None;
        let now = self.now;
        match op {
            StateOp::Admit {
                peer,
                conn_id,
                session_id,
                outbound,
            } => {
                let decision = self
                    .state
                    .admit_session(session(peer, conn_id, session_id), direction(outbound));
                assert_eq!(
                    decision,
                    self.model.admit(peer, conn_id, session_id, outbound),
                    "{STATE_PROPERTY}"
                );
            }
            StateOp::RemoveSession {
                peer,
                conn_id,
                session_id,
            } => {
                self.state
                    .remove_session(&session(peer, conn_id, session_id));
                if self.model.is_current(peer, conn_id, session_id) {
                    self.model.peers[peer].admitted = None;
                }
            }
            StateOp::RemoveConnection { peer, conn_id } => {
                self.state
                    .remove_connection(&session(peer, conn_id, 0).peer_id, conn_id);
                if self.model.peers[peer]
                    .admitted
                    .is_some_and(|(conn, _, _)| conn == conn_id)
                {
                    self.model.peers[peer].admitted = None;
                }
            }
            StateOp::SetConnected { peer, connected } => {
                self.model.peers[peer].connected = connected;
            }
            StateOp::StartHello {
                peer,
                conn_id,
                session_id,
            } => {
                self.pending
                    .push((peer, conn_id, session_id, self.hellos[peer].clone()));
            }
            StateOp::FinishHello(index) => {
                if self.pending.is_empty() {
                    return;
                }
                let (peer, conn_id, session_id, verified) =
                    self.pending.remove(index % self.pending.len());
                let connected = self.model.connected_node_ids();
                let result = self.state.install_session_record(
                    &session(peer, conn_id, session_id),
                    verified,
                    &connected,
                    now,
                    &mut Vec::new(),
                );
                self.result = Some(install_fact(&result));
                if self.model.is_current(peer, conn_id, session_id) {
                    let expected = if self.model.peers[peer].in_book {
                        ImportFact::MetadataUpdated
                    } else {
                        ImportFact::Added
                    };
                    self.expected_result = Some(InstallFact::Installed(expected));
                    // An install first drops what disconnected peers no longer back.
                    self.model.sync(now);
                    self.model.peers[peer].in_book = true;
                    self.model.peers[peer].hinted = true;
                } else {
                    self.expected_result = Some(InstallFact::Superseded);
                }
            }
            StateOp::ReceiveServices {
                peer,
                conn_id,
                session_id,
                ttl,
            } => {
                let node_id = peer_secret(peer).public();
                let connected = self.model.connected_node_ids();
                self.state
                    .receive_session_live_summaries(
                        &session(peer, conn_id, session_id),
                        node_id,
                        services(node_id, now + ttl),
                        &connected,
                        now,
                    )
                    .expect("generated summaries decode");
                if self.model.is_current(peer, conn_id, session_id) {
                    self.model.sync(now);
                    if self.model.peers[peer].connected {
                        self.model.peers[peer].live_until =
                            Some((now + ttl).min(now + LIVE_SUMMARY_TTL));
                    }
                }
            }
            StateOp::Advance(secs) => {
                self.now += secs;
            }
            StateOp::ReadHints => self.check_reads(),
        }
    }

    /// Reads each peer's hints the way production does, which drops stale ones.
    fn check_reads(&mut self) {
        let now = self.now;
        let connected = self.model.connected_node_ids();
        self.model.sync(now);
        for (index, peer) in self.model.peers.iter().enumerate() {
            let node_id = peer_secret(index).public();
            let has_entry = peer.hinted || peer.live_until.is_some();
            assert_eq!(
                self.state
                    .active_services(node_id, &connected, now)
                    .is_some(),
                peer.connected && has_entry,
                "{STATE_PROPERTY}: peer {} service hints",
                index
            );
            assert_eq!(
                self.state
                    .live_service_summaries(node_id, &connected, now)
                    .map(|summaries| !summaries.is_empty()),
                (peer.connected && has_entry).then_some(peer.live_until.is_some()),
                "{STATE_PROPERTY}: peer {} live summaries",
                index
            );
        }
    }

    fn check_sessions(&self) {
        // The caps hold on the real state alone, whatever the model says.
        let snapshot = self.state.peer_snapshot();
        assert!(
            snapshot.inbound_peers <= PEER_CAP && snapshot.outbound_peers <= PEER_CAP,
            "{STATE_PROPERTY}: admitted sessions exceed a direction cap: {snapshot:?}"
        );
        for (index, peer) in self.model.peers.iter().enumerate() {
            if let Some((conn_id, session_id, _)) = peer.admitted {
                assert!(
                    self.state
                        .is_current_session(&session(index, conn_id, session_id)),
                    "{STATE_PROPERTY}"
                );
            }
        }
        assert_eq!(
            self.state.peer_snapshot(),
            ServicePeerSnapshot::new(
                self.model.admitted_in(false),
                self.model.admitted_in(true),
                peer_limits()
            ),
            "{STATE_PROPERTY}"
        );
        assert_view_matches(STATE_PROPERTY, &self.actual_view(), &self.expected_view());
    }

    fn actual_view(&self) -> SessionView {
        SessionView {
            now: self.now,
            admitted: self
                .state
                .admitted_sessions()
                .into_iter()
                .map(|(peer, conn_id, session_id, outbound)| AdmittedFact {
                    peer: hex::encode(peer.as_bytes()),
                    conn_id,
                    session_id,
                    outbound,
                })
                .collect(),
            hints: self
                .state
                .retained_hints()
                .into_iter()
                .map(|(node_id, record, live_summaries)| HintFact {
                    node: hex::encode(node_id.as_bytes()),
                    record,
                    live_summaries,
                })
                .collect(),
            discovered: self.state.book().discovered_len(),
            installation: self.result.clone(),
        }
    }

    fn expected_view(&self) -> SessionView {
        let mut admitted: Vec<_> = self
            .model
            .peers
            .iter()
            .enumerate()
            .filter_map(|(index, peer)| {
                peer.admitted
                    .map(|(conn_id, session_id, outbound)| AdmittedFact {
                        peer: hex::encode(peer_secret(index).public().as_bytes()),
                        conn_id,
                        session_id,
                        outbound,
                    })
            })
            .collect();
        admitted.sort_unstable_by(|left, right| left.peer.cmp(&right.peer));
        let mut hints: Vec<_> = self
            .model
            .peers
            .iter()
            .enumerate()
            .filter(|(_, peer)| peer.hinted || peer.live_until.is_some())
            .map(|(index, peer)| HintFact {
                node: hex::encode(peer_secret(index).public().as_bytes()),
                record: peer.hinted,
                live_summaries: usize::from(peer.live_until.is_some()),
            })
            .collect();
        hints.sort_unstable_by(|left, right| left.node.cmp(&right.node));
        SessionView {
            now: self.now,
            admitted,
            hints,
            discovered: self.model.peers.iter().filter(|peer| peer.in_book).count(),
            installation: self.expected_result.clone(),
        }
    }
}

/// One verification result's installation in a machine step.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum InstallFact {
    /// The session was current, and the book took the record.
    Installed(ImportFact),
    /// A later session replaced the session that delivered the record.
    Superseded,
    /// The book refused the record, with this error.
    Failed(String),
}

fn install_fact(result: &Result<ImportOutcome, SessionRecordError>) -> InstallFact {
    match result {
        Ok(outcome) => InstallFact::Installed((*outcome).into()),
        Err(SessionRecordError::Superseded) => InstallFact::Superseded,
        Err(SessionRecordError::Import(error)) => InstallFact::Failed(error.to_string()),
    }
}

/// One admitted session in a machine step.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmittedFact {
    /// The peer, in hex.
    pub peer: String,
    /// The connection that carries the session.
    pub conn_id: u64,
    /// The stream session on that connection.
    pub session_id: u64,
    /// Whether this node dialed the connection.
    pub outbound: bool,
}

/// One retained service hint in a machine step, before any read drops stale hints.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HintFact {
    /// The peer's node id, in hex.
    pub node: String,
    /// Whether the peer's own record backs the hint.
    pub record: bool,
    /// How many live summaries the hint retains.
    pub live_summaries: usize,
}

/// Session and hint state after one operation, read from the owner or derived by its model.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionView {
    /// The explicit fixture time, in Unix seconds.
    pub now: u64,
    /// Admitted sessions, in peer order.
    pub admitted: Vec<AdmittedFact>,
    /// Retained service hints, in node id order.
    pub hints: Vec<HintFact>,
    /// Discovered records in the book.
    pub discovered: usize,
    /// The installation's result, for an operation that finished a verification.
    pub installation: Option<InstallFact>,
}

/// One decoder-v2 session-installation operation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscoverySessionStep {
    /// The operation's number, from 1.
    pub step: usize,
    /// The original causal bytes.
    pub input: Vec<u8>,
    /// State read from the real owner.
    pub actual: SessionView,
    /// State derived independently by the model.
    pub expected: SessionView,
    /// The first failure, if this operation stopped the run.
    pub failure: Option<DriverFailure>,
}

/// Runs session-installation decoder-v2 operations under a fixture clock, and captures
/// each operation. Reads happen only at explicit read operations, so each view
/// shows what the owner retains between them.
pub fn trace_discovery_state_bytes(data: &[u8]) -> Vec<DiscoverySessionStep> {
    let input = &data[..data.len().min(512)];
    let mut driver = StateDriver::new();
    let mut steps = Vec::new();
    for (index, op) in decode_state_v2(input).enumerate() {
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            driver.apply(op);
            driver.check_sessions();
        }));
        let failure = outcome
            .err()
            .map(|payload| driver_failure(payload.as_ref(), STATE_PROPERTY));
        let stop = failure.is_some();
        steps.push(DiscoverySessionStep {
            step: index + 1,
            input: step_input(input, index + 1),
            actual: driver.actual_view(),
            expected: driver.expected_view(),
            failure,
        });
        if stop {
            break;
        }
    }
    steps
}

/// Decodes four bytes into each session-installation operation. Generator 2 names
/// this decoder in saved scenarios.
fn decode_state_v2(data: &[u8]) -> impl Iterator<Item = StateOp> + '_ {
    data.chunks(4).map(|chunk| {
        let b = chunk.get(1).copied().unwrap_or(0);
        let c = chunk.get(2).copied().unwrap_or(0);
        let d = chunk.get(3).copied().unwrap_or(0);
        let peer = usize::from(b) % PEERS;
        let conn_id = u64::from(c % 3);
        let session_id = u64::from(d % 3);
        match chunk[0] % 9 {
            0 => StateOp::Admit {
                peer,
                conn_id,
                session_id,
                outbound: b & 128 != 0,
            },
            1 => StateOp::RemoveSession {
                peer,
                conn_id,
                session_id,
            },
            2 => StateOp::RemoveConnection { peer, conn_id },
            3 => StateOp::SetConnected {
                peer,
                connected: c & 1 == 0,
            },
            4 => StateOp::StartHello {
                peer,
                conn_id,
                session_id,
            },
            5 => StateOp::FinishHello(usize::from(b)),
            6 => StateOp::ReceiveServices {
                peer,
                conn_id,
                session_id,
                ttl: u64::from(d) + 1,
            },
            7 => StateOp::Advance(u64::from(d)),
            _ => StateOp::ReadHints,
        }
    })
}

/// Decodes four bytes into each record-book operation. Generator 2 names this
/// decoder in saved scenarios.
fn decode_book_v2(data: &[u8]) -> impl Iterator<Item = BookOp> + '_ {
    data.chunks(4).map(|chunk| {
        let a = chunk[0];
        let b = chunk.get(1).copied().unwrap_or(0);
        let c = chunk.get(2).copied().unwrap_or(0);
        let d = chunk.get(3).copied().unwrap_or(0);
        let author = usize::from(b) % AUTHORS;
        match a % 7 {
            0..=2 => BookOp::Import {
                author,
                sequence: u64::from(c % 4),
                expires_in: i64::from(d) * 2 - 400,
                supplier: match a % 3 {
                    0 => Supplier::Author,
                    1 => Supplier::Relay(usize::from(b) / AUTHORS % RELAYS),
                    _ => Supplier::Nobody,
                },
            },
            3 => BookOp::DialSuccess(author),
            4 => BookOp::DialFailure(author),
            5 => BookOp::Advance(u64::from(d) * 3),
            _ => BookOp::Sample {
                requester: usize::from(b) % RELAYS,
                limit: usize::from(c) % 5,
            },
        }
    })
}
