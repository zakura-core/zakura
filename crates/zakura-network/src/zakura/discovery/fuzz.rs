//! Machine drivers for discovery's shared state: the record book and
//! [`DiscoveryState`].
//!
//! Each driver runs the real owner and checks its invariants after every
//! operation. It compares results and reported changes only with what the
//! driver sent and the owner reported earlier: which records it imported,
//! from whom and when they expire, which dials it reported, which sessions it
//! admitted and removed, and which peers it marked connected. It never
//! re-derives an eviction victim, an ordering or a sample. Named scenarios in
//! `fuzz/state_tests.rs` and `protocol.rs` pin those rules. `zakura-sim`
//! replays these drivers through [`trace_discovery_book_bytes`] and
//! [`trace_discovery_state_bytes`].

use std::{
    any::Any,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    panic::{catch_unwind, AssertUnwindSafe},
    time::Duration,
};

use rand_10::{rngs::StdRng, SeedableRng};
use serde::{Deserialize, Serialize};
use zakura_quic::{NodeId, NodeSecretKey};
use zakura_rt::collections::{HashMap, HashSet};

use super::protocol::{
    BookChange, DisclosureKey, DiscoveryBookError, DiscoveryRecordError,
    DiscoveryRecordValidationContext, DiscoveryServiceSummary, EvictionCause, ImportOutcome,
    ServiceSummaryEnvelope, Services, ZakuraDiscoveryBook, ZakuraDiscoveryBookLimits,
    ZakuraDiscoveryConfig, ZakuraDiscoveryEntry, ZakuraDiscoveryLocalConfig, ZakuraNodeRecord,
    ZakuraNodeRecordBody, ZakuraServiceId,
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

/// The last octet of book author `index`'s one public address.
fn author_address(index: usize) -> u8 {
    10 + u8::try_from(index).expect("test indexes are small")
}

/// Book author `index`'s one public address, which its records list.
fn author_socket(index: usize) -> SocketAddr {
    socket(author_address(index))
}

/// The public address with last octet `address` that test records list.
fn socket(address: u8) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(45, 33, 30, address)), 8233)
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
    // A replay rebuilds its records, so their bytes may not depend on a
    // random hedge.
    ZakuraNodeRecord::sign_with_randomness(
        ZakuraNodeRecordBody {
            keys: signer.key_list().clone(),
            direct_addrs: vec![socket(address)],
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
        [0; 32],
    )
    .expect("the signer authors its own record")
}

// Book driver.

/// Who supplied a record.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Supplier {
    /// The record's author, as in its own `Hello`.
    Author,
    /// A third party relaying the record in its `Peers`.
    Relay(usize),
    /// No peer, as in a test import.
    Nobody,
}

#[derive(Clone, Debug, Eq, PartialEq)]
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

const BOOK_PROPERTY: &str = "discovery/record-book";

/// What the driver knows of one stored entry: the changes the book reported,
/// and the operations the driver chose since the entry's install.
#[derive(Clone, Debug)]
struct Tracked {
    sequence: u64,
    /// The signed expiry of the record the driver imported.
    expires: u64,
    /// Dial failures since the install or the last successful dial.
    failures: u32,
    /// The last import that refreshed the entry.
    last_seen: u64,
    /// The last successful dial since the install.
    last_success: Option<u64>,
    /// Whether the author's own `Hello` or a successful dial vouched for the
    /// entry since its install. Only a vouched entry may be confirmed.
    vouched: bool,
}

impl Tracked {
    fn expired(&self, now: u64) -> bool {
        self.expires < now
    }

    /// Never dialed, and failed at least the eviction threshold.
    fn failing(&self) -> bool {
        self.last_success.is_none() && self.failures >= HIGH_FAILURES
    }
}

/// Checks that a record-limit eviction took its victim from the first
/// nonempty tier among `candidates`: expired entries, then never-dialed
/// failing entries, then unconfirmed entries.
fn check_eviction_tier(
    victim: &Tracked,
    victim_confirmed: bool,
    candidates: &HashMap<NodeId, Tracked>,
    now: u64,
    confirmed: impl Fn(&NodeId) -> bool,
) {
    if candidates.values().any(|candidate| candidate.expired(now)) {
        assert!(
            victim.expired(now),
            "{BOOK_PROPERTY}: evicted a live entry beside an expired one"
        );
    } else if candidates.values().any(Tracked::failing) {
        assert!(
            victim.failing(),
            "{BOOK_PROPERTY}: evicted a working entry beside a failing one"
        );
    } else {
        assert!(
            !victim_confirmed || candidates.keys().all(confirmed),
            "{BOOK_PROPERTY}: evicted a confirmed entry beside an unconfirmed one"
        );
    }
}

/// The book's entries just before an import.
struct Before {
    tracked: HashMap<NodeId, Tracked>,
    sources: HashMap<NodeId, Option<NodeId>>,
    confirmed: HashSet<NodeId>,
    relayed: HashMap<NodeId, usize>,
}

/// The real book at one point in a history, and what the driver sent it.
struct BookDriver {
    book: ZakuraDiscoveryBook,
    rng: StdRng,
    disclosed: HashMap<(usize, u64), HashSet<NodeId>>,
    authors: Vec<NodeSecretKey>,
    now: u64,
    tracked: HashMap<NodeId, Tracked>,
    /// The epoch and length of the sample snapshot after the last sample.
    snapshot: Option<(u64, usize)>,
    /// The record changes the book reported for the current operation.
    changes: Vec<BookChange>,
    /// The current import's result, if the operation imported a record.
    result: Option<ImportFact>,
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
            rng: StdRng::seed_from_u64(seed),
            disclosed: HashMap::new(),
            authors: (0..AUTHORS).map(author).collect(),
            now: START,
            tracked: HashMap::new(),
            snapshot: None,
            changes: Vec::new(),
            result: None,
            sample: Vec::new(),
        }
    }

    fn before(&self) -> Before {
        let mut sources = HashMap::new();
        let mut confirmed = HashSet::new();
        for signer in &self.authors {
            let node_id = signer.public();
            if let Some(entry) = self.book.get(&node_id) {
                sources.insert(node_id, entry.source());
                if entry.is_confirmed() {
                    confirmed.insert(node_id);
                }
            }
        }
        Before {
            tracked: self.tracked.clone(),
            sources,
            confirmed,
            relayed: self.book.relayed_counts().into_iter().collect(),
        }
    }

    fn apply(&mut self, op: BookOp) {
        self.changes.clear();
        self.result = None;
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
                let record = record(signer, author_address(author), sequence, expires);
                let context = context(now);
                // The author's own `Hello` arrives on a connection from its IP.
                let (source, authenticated_ip) = match supplier {
                    Supplier::Author => (Some(node_id), Some(author_socket(author).ip())),
                    Supplier::Relay(index) => (Some(relay(index)), None),
                    Supplier::Nobody => (None, None),
                };
                let before = self.before();
                // Every supplier's record takes the production path: verify, then install.
                let result = match VerifiedRecord::verify(record, &context) {
                    Ok(verified) => self.book.import_verified_record(
                        verified,
                        source,
                        authenticated_ip,
                        now,
                        &context,
                        &mut self.changes,
                    ),
                    Err(error) => Err(error.into()),
                };
                self.result = Some(import_result(result));
                self.check_import(node_id, sequence, expires, expires_in, supplier, &before);
            }
            BookOp::DialSuccess(author) => {
                let node_id = self.authors[author].public();
                self.book
                    .mark_dial_success(&node_id, Some(author_socket(author)), now);
                if let Some(tracked) = self.tracked.get_mut(&node_id) {
                    tracked.failures = 0;
                    tracked.last_success = Some(now);
                    tracked.vouched = true;
                    assert!(
                        self.book
                            .get(&node_id)
                            .is_some_and(|entry| entry.is_confirmed()),
                        "{BOOK_PROPERTY}: a successful dial left its entry unconfirmed"
                    );
                }
            }
            BookOp::DialFailure(author) => {
                let node_id = self.authors[author].public();
                self.book.mark_dial_failure(&node_id, now);
                if let Some(tracked) = self.tracked.get_mut(&node_id) {
                    tracked.failures = tracked.failures.saturating_add(1);
                }
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
                    sample.iter().map(|record| record.body.node_id()).collect();
                assert_eq!(
                    ids.len(),
                    sample.len(),
                    "{BOOK_PROPERTY}: a sample repeats no node"
                );
                // A sample discloses only live, vouched records, as stored.
                for record in &sample {
                    let node_id = record.body.node_id();
                    let tracked = self
                        .tracked
                        .get(&node_id)
                        .unwrap_or_else(|| panic!("{BOOK_PROPERTY}: disclosed an unknown node"));
                    assert!(
                        tracked.vouched && !tracked.expired(now),
                        "{BOOK_PROPERTY}: disclosed {node_id:?}"
                    );
                    assert_eq!(record.body.sequence, tracked.sequence, "{BOOK_PROPERTY}");
                }
                let epoch = now / DISCLOSURE_EPOCH;
                let window = self.disclosed.entry((requester, epoch)).or_default();
                window.extend(ids);
                assert!(
                    window.len() <= SAMPLE_WINDOW,
                    "{BOOK_PROPERTY}: one requester saw {} ids in an epoch",
                    window.len()
                );
                // Within an epoch the snapshot grows only while one window
                // covers it.
                let len = self.book.sample_snapshot_len();
                if let Some((last_epoch, last_len)) = self.snapshot {
                    assert!(
                        last_epoch != epoch || len <= last_len || last_len < SAMPLE_WINDOW,
                        "{BOOK_PROPERTY}: the snapshot grew from {last_len} to {len} past a window"
                    );
                }
                self.snapshot = Some((epoch, len));
            }
            BookOp::Advance(secs) => {
                self.now += secs;
            }
        }
    }

    /// Checks one import's result and changes against the entries before it,
    /// and applies the changes to the tracked entries.
    fn check_import(
        &mut self,
        node_id: NodeId,
        sequence: u64,
        expires: u64,
        expires_in: i64,
        supplier: Supplier,
        before: &Before,
    ) {
        let now = self.now;
        let result = self.result.expect("an import has a result");
        let supplied = match supplier {
            Supplier::Author => Some(node_id),
            Supplier::Relay(index) => Some(relay(index)),
            Supplier::Nobody => None,
        };
        let relay_count = |relay: NodeId| before.relayed.get(&relay).copied().unwrap_or(0);
        assert_eq!(
            result == ImportFact::Expired,
            expires_in == EXPIRED_ON_ARRIVAL,
            "{BOOK_PROPERTY}: {result:?} for a record that expires in {expires_in}s"
        );

        let mut installed = false;
        let mut replaced = false;
        for change in &self.changes {
            match *change {
                BookChange::Installed {
                    node_id: id,
                    sequence: installed_sequence,
                    source,
                } => {
                    assert_eq!(
                        (id, installed_sequence, source),
                        (node_id, sequence, supplied),
                        "{BOOK_PROPERTY}: installed another record"
                    );
                    assert!(
                        self.tracked
                            .insert(
                                id,
                                Tracked {
                                    sequence,
                                    expires,
                                    failures: 0,
                                    last_seen: now,
                                    last_success: None,
                                    vouched: false,
                                },
                            )
                            .is_none(),
                        "{BOOK_PROPERTY}: installed a stored node"
                    );
                    installed = true;
                }
                BookChange::Replaced {
                    node_id: id,
                    previous_sequence,
                    sequence: replaced_sequence,
                    source,
                } => {
                    assert_eq!(
                        (id, replaced_sequence),
                        (node_id, sequence),
                        "{BOOK_PROPERTY}: replaced with another record"
                    );
                    let tracked = self
                        .tracked
                        .get_mut(&id)
                        .unwrap_or_else(|| panic!("{BOOK_PROPERTY}: replaced an unknown node"));
                    assert_eq!(tracked.sequence, previous_sequence, "{BOOK_PROPERTY}");
                    // A later sequence replaces a live record. An expired
                    // record no longer orders its node's records.
                    assert!(
                        sequence > previous_sequence || tracked.expired(now),
                        "{BOOK_PROPERTY}: sequence {sequence} replaced live {previous_sequence}"
                    );
                    tracked.sequence = sequence;
                    tracked.expires = expires;
                    assert_eq!(
                        Some(source),
                        self.book.get(&id).map(|entry| entry.source()),
                        "{BOOK_PROPERTY}: the replacement reported another source"
                    );
                    replaced = true;
                }
                BookChange::Evicted {
                    node_id: id,
                    sequence: evicted_sequence,
                    cause,
                } => {
                    let candidates = self.tracked.clone();
                    let victim = self
                        .tracked
                        .remove(&id)
                        .unwrap_or_else(|| panic!("{BOOK_PROPERTY}: evicted an unknown node"));
                    assert_eq!(victim.sequence, evicted_sequence, "{BOOK_PROPERTY}");
                    match cause {
                        EvictionCause::RelayQuota => {
                            // Only a full relay's next new record evicts its
                            // expired entries.
                            let relay = supplied.filter(|_| matches!(supplier, Supplier::Relay(_)));
                            assert!(
                                relay.is_some_and(|relay| relay_count(relay) == BOOK_MAX_PER_RELAY)
                                    && !before.tracked.contains_key(&node_id)
                                    && victim.expired(now)
                                    && before.sources.get(&id).copied().flatten() == relay,
                                "{BOOK_PROPERTY}: an unjustified relay-quota eviction"
                            );
                        }
                        EvictionCause::BookLimit => {
                            assert_eq!(
                                candidates.len(),
                                BOOK_MAX_RECORDS + 1,
                                "{BOOK_PROPERTY}: evicted below the record limit"
                            );
                            let confirmed = |id: &NodeId| {
                                before.confirmed.contains(id)
                                    || (*id == node_id && supplier == Supplier::Author)
                            };
                            check_eviction_tier(
                                &victim,
                                confirmed(&id),
                                &candidates,
                                now,
                                confirmed,
                            );
                        }
                    }
                }
            }
        }

        let stored = before.tracked.get(&node_id);
        match result {
            ImportFact::Added | ImportFact::BookFull => assert!(
                stored.is_none() && installed,
                "{BOOK_PROPERTY}: {result:?} without an install"
            ),
            ImportFact::Updated => assert!(
                stored.is_some() && replaced,
                "{BOOK_PROPERTY}: an update without a replacement"
            ),
            ImportFact::MetadataUpdated => assert!(
                stored.is_some_and(|stored| stored.sequence == sequence && !stored.expired(now)),
                "{BOOK_PROPERTY}: a metadata update of another record"
            ),
            ImportFact::IgnoredOlder => assert!(
                stored.is_some_and(|stored| sequence < stored.sequence && !stored.expired(now)),
                "{BOOK_PROPERTY}: ignored a record that orders after the stored one"
            ),
            ImportFact::RelayFull => {
                let relay = supplied
                    .filter(|_| matches!(supplier, Supplier::Relay(_)))
                    .unwrap_or_else(|| panic!("{BOOK_PROPERTY}: no relay was full"));
                assert!(
                    stored.is_none() && relay_count(relay) == BOOK_MAX_PER_RELAY,
                    "{BOOK_PROPERTY}: refused below the relay quota"
                );
                assert!(
                    before.tracked.iter().all(|(id, tracked)| {
                        before.sources.get(id).copied().flatten() != Some(relay)
                            || !tracked.expired(now)
                    }),
                    "{BOOK_PROPERTY}: a full relay kept an expired entry"
                );
            }
            ImportFact::Expired => {}
        }
        if !matches!(
            result,
            ImportFact::Added | ImportFact::BookFull | ImportFact::Updated
        ) {
            assert!(
                self.changes.is_empty(),
                "{BOOK_PROPERTY}: {result:?} changed records"
            );
        }
        if !matches!(
            result,
            ImportFact::Added | ImportFact::Updated | ImportFact::MetadataUpdated
        ) {
            return;
        }
        let tracked = self
            .tracked
            .get_mut(&node_id)
            .expect("a stored import is tracked");
        tracked.last_seen = now;
        let entry = self
            .book
            .get(&node_id)
            .expect("a stored import is in the book");
        if supplier == Supplier::Author {
            tracked.vouched = true;
            assert!(
                entry.is_confirmed(),
                "{BOOK_PROPERTY}: the author's own record left its entry unconfirmed"
            );
        }
        // A relay's copy of the author's own record keeps the author's
        // provenance. Otherwise a relay with room takes the entry over.
        let author_kept = result == ImportFact::MetadataUpdated
            && before.sources.get(&node_id) == Some(&Some(node_id));
        if author_kept {
            assert_eq!(
                entry.source(),
                Some(node_id),
                "{BOOK_PROPERTY}: lost the author's provenance"
            );
        } else if let Supplier::Relay(index) = supplier {
            if relay_count(relay(index)) < BOOK_MAX_PER_RELAY {
                assert_eq!(
                    entry.source(),
                    Some(relay(index)),
                    "{BOOK_PROPERTY}: a relay with room did not take the entry"
                );
            }
        }
    }

    fn check(&mut self, after_step: &mut impl FnMut(&mut ZakuraDiscoveryBook, u64)) {
        after_step(&mut self.book, self.now);
        self.book.assert_invariants();
        assert_eq!(
            self.book.discovered_len(),
            self.tracked.len(),
            "{BOOK_PROPERTY}: the book's entries disagree with its reported changes"
        );
        for signer in &self.authors {
            let node_id = signer.public();
            let entry = self.book.get(&node_id);
            let Some(tracked) = self.tracked.get(&node_id) else {
                assert!(entry.is_none(), "{BOOK_PROPERTY}: an unreported entry");
                continue;
            };
            let entry = entry.unwrap_or_else(|| panic!("{BOOK_PROPERTY}: an unreported removal"));
            assert_eq!(
                (
                    entry.record().body.sequence,
                    entry.record().body.expires_at_unix_secs
                ),
                (tracked.sequence, tracked.expires),
                "{BOOK_PROPERTY}: the stored record"
            );
            assert_eq!(
                (
                    entry.failure_count(),
                    entry.last_success(),
                    entry.last_seen()
                ),
                (tracked.failures, tracked.last_success, tracked.last_seen),
                "{BOOK_PROPERTY}: dial metadata"
            );
            assert!(
                !entry.is_confirmed() || tracked.vouched,
                "{BOOK_PROPERTY}: an unearned confirmation"
            );
        }
    }

    fn view(&self) -> BookView {
        let mut entries: Vec<_> = self
            .authors
            .iter()
            .filter_map(|signer| {
                let node_id = signer.public();
                self.book
                    .get(&node_id)
                    .map(|entry| entry_fact(node_id, entry))
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
}

/// `record`'s bytes as `Peers` carries them.
fn signed_bytes(record: &ZakuraNodeRecord) -> Vec<u8> {
    let mut bytes = record
        .body
        .encode_for_signature()
        .expect("a stored record encodes");
    bytes.extend_from_slice(&record.signature);
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

fn entry_fact(node_id: NodeId, entry: &ZakuraDiscoveryEntry) -> BookEntryFact {
    BookEntryFact {
        node: hex::encode(node_id.as_bytes()),
        sequence: entry.record().body.sequence,
        expires: entry.record().body.expires_at_unix_secs,
        source: entry.source().map(|source| hex::encode(source.as_bytes())),
        confirmed: entry.is_confirmed(),
        failures: entry.failure_count(),
        last_seen: entry.last_seen(),
        last_success: entry.last_success(),
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

/// The book's state after one operation, read from the book.
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

/// One decoder-v3 record-book operation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BookStep {
    /// The operation's number, from 1.
    pub step: usize,
    /// The original causal bytes.
    pub input: Vec<u8>,
    /// State read from the real book.
    pub view: BookView,
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
    /// Whether the failed check named its property.
    pub property_failure: bool,
    /// The panic payload, without the panic hook's source location.
    pub message: String,
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

/// The causal bytes of 1-based operation `step`, when each operation reads `width` bytes.
fn step_input(input: &[u8], step: usize, width: usize) -> Vec<u8> {
    input[(step - 1) * width..(step * width).min(input.len())].to_vec()
}

/// Runs record-book decoder-v3 operations under a fixed disclosure key and fixture
/// clock, and captures each operation. The history stops at the first failure.
pub fn trace_discovery_book_bytes(data: &[u8]) -> Vec<BookStep> {
    let input = &data[..data.len().min(512)];
    let mut driver = BookDriver::new(0);
    let mut steps = Vec::new();
    for (index, op) in decode_book_v3(input).enumerate() {
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
            input: step_input(input, index + 1, BOOK_OP_BYTES),
            view: driver.view(),
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

// DiscoveryState driver.

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

/// The last octet of state peer `peer`'s one public address.
fn hello_address(peer: usize) -> u8 {
    100 + u8::try_from(peer).expect("test indexes are small")
}

/// A peer's own `Hello` record, valid for the whole test.
fn hello(peer: usize) -> ZakuraNodeRecord {
    record(&peer_secret(peer), hello_address(peer), 1, START + 50_000)
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

const STATE_PROPERTY: &str = "discovery/session-installation";

/// What the driver sent the state, by peer index.
#[derive(Debug)]
struct Sessions {
    /// Each peer's admitted `(connection, session, outbound)`, as the
    /// admissions and removals the driver sent leave it.
    admitted: [Option<(ZakuraConnId, u64, bool)>; PEERS],
    connected: [bool; PEERS],
    /// Whether a current installation put the peer's `Hello` in the book.
    in_book: [bool; PEERS],
    /// When the last current `Services` arrived, and its declared TTL.
    received: [Option<(u64, u64)>; PEERS],
}

impl Sessions {
    fn is_current(&self, peer: usize, conn_id: ZakuraConnId, session_id: u64) -> bool {
        self.admitted[peer]
            .is_some_and(|(conn, session, _)| (conn, session) == (conn_id, session_id))
    }

    fn admitted_in(&self, outbound: bool) -> usize {
        self.admitted
            .iter()
            .filter(|admitted| admitted.is_some_and(|(_, _, out)| out == outbound))
            .count()
    }

    fn connected_node_ids(&self) -> Vec<NodeId> {
        (0..PEERS)
            .filter(|peer| self.connected[*peer])
            .map(|peer| peer_secret(peer).public())
            .collect()
    }
}

/// The index of the state peer with `node_id`.
fn peer_index(node_id: NodeId) -> usize {
    (0..PEERS)
        .find(|peer| peer_secret(*peer).public() == node_id)
        .expect("the state holds only the driver's peers")
}

/// The real state at one point in a history, and what the driver sent it.
struct StateDriver {
    state: DiscoveryState,
    sent: Sessions,
    pending: Vec<(usize, ZakuraConnId, u64, VerifiedRecord)>,
    hellos: Vec<VerifiedRecord>,
    now: u64,
    /// The current operation's installation result.
    result: Option<InstallFact>,
}

impl StateDriver {
    fn new() -> Self {
        Self {
            state: discovery_state(),
            sent: Sessions {
                admitted: [None; PEERS],
                connected: [false; PEERS],
                in_book: [false; PEERS],
                received: [None; PEERS],
            },
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
        }
    }

    fn apply(&mut self, op: StateOp) {
        self.result = None;
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
                let current = self.sent.admitted[peer];
                match decision {
                    ServiceAdmissionDecision::Admit => {
                        // Only a later (connection, session) replaces an
                        // admitted session.
                        assert!(
                            current.is_none_or(|(conn, session, _)| {
                                (conn_id, session_id) > (conn, session)
                            }),
                            "{STATE_PROPERTY}: admitted an older session over {current:?}"
                        );
                        self.sent.admitted[peer] = Some((conn_id, session_id, outbound));
                    }
                    ServiceAdmissionDecision::RejectNotUseful => assert!(
                        current.is_some_and(|(conn, session, _)| {
                            (conn_id, session_id) <= (conn, session)
                        }),
                        "{STATE_PROPERTY}: refused a later session as not useful"
                    ),
                    ServiceAdmissionDecision::RejectFull => assert!(
                        current.is_none_or(|(_, _, out)| out != outbound)
                            && self.sent.admitted_in(outbound) == PEER_CAP,
                        "{STATE_PROPERTY}: refused a session as full below its cap"
                    ),
                }
            }
            StateOp::RemoveSession {
                peer,
                conn_id,
                session_id,
            } => {
                self.state
                    .remove_session(&session(peer, conn_id, session_id));
                if self.sent.is_current(peer, conn_id, session_id) {
                    self.sent.admitted[peer] = None;
                }
            }
            StateOp::RemoveConnection { peer, conn_id } => {
                self.state
                    .remove_connection(&session(peer, conn_id, 0).peer_id, conn_id);
                if self.sent.admitted[peer].is_some_and(|(conn, _, _)| conn == conn_id) {
                    self.sent.admitted[peer] = None;
                }
            }
            StateOp::SetConnected { peer, connected } => {
                self.sent.connected[peer] = connected;
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
                let connected = self.sent.connected_node_ids();
                let hints = self.state.retained_hints();
                let discovered = self.state.book().discovered_len();
                let result = self.state.install_session_record(
                    &session(peer, conn_id, session_id),
                    verified,
                    Some(socket(hello_address(peer)).ip()),
                    &connected,
                    now,
                    &mut Vec::new(),
                );
                let result = install_fact(&result);
                let current = self.sent.is_current(peer, conn_id, session_id);
                match &result {
                    InstallFact::Superseded => {
                        assert!(
                            !current,
                            "{STATE_PROPERTY}: refused the current session's result"
                        );
                        assert_eq!(
                            (
                                self.state.retained_hints(),
                                self.state.book().discovered_len()
                            ),
                            (hints, discovered),
                            "{STATE_PROPERTY}: a superseded result changed the state"
                        );
                    }
                    InstallFact::Installed(outcome) => {
                        assert!(current, "{STATE_PROPERTY}: installed a superseded result");
                        let expected = if self.sent.in_book[peer] {
                            ImportFact::MetadataUpdated
                        } else {
                            ImportFact::Added
                        };
                        assert_eq!(*outcome, expected, "{STATE_PROPERTY}");
                        self.sent.in_book[peer] = true;
                        // The install backs the peer's hint, and drops the
                        // hints of disconnected peers.
                        let node_id = peer_secret(peer).public();
                        let hints = self.state.retained_hints();
                        assert!(
                            hints
                                .iter()
                                .any(|(id, record, _)| *id == node_id && *record),
                            "{STATE_PROPERTY}: an install left no record hint"
                        );
                        assert!(
                            hints.iter().all(|(id, _, _)| {
                                *id == node_id || self.sent.connected[peer_index(*id)]
                            }),
                            "{STATE_PROPERTY}: an install kept a disconnected peer's hint"
                        );
                    }
                    InstallFact::Failed(error) => {
                        panic!("{STATE_PROPERTY}: the generated Hello failed: {error}")
                    }
                }
                self.result = Some(result);
            }
            StateOp::ReceiveServices {
                peer,
                conn_id,
                session_id,
                ttl,
            } => {
                let node_id = peer_secret(peer).public();
                let connected = self.sent.connected_node_ids();
                let hints = self.state.retained_hints();
                self.state
                    .receive_session_live_summaries(
                        &session(peer, conn_id, session_id),
                        node_id,
                        services(node_id, now + ttl),
                        &connected,
                        now,
                    )
                    .expect("generated summaries decode");
                let after = self.state.retained_hints();
                if !self.sent.is_current(peer, conn_id, session_id) {
                    assert_eq!(
                        after, hints,
                        "{STATE_PROPERTY}: a superseded session's summaries changed the hints"
                    );
                    return;
                }
                assert!(
                    after
                        .iter()
                        .all(|(id, _, _)| self.sent.connected[peer_index(*id)]),
                    "{STATE_PROPERTY}: summaries kept a disconnected peer's hint"
                );
                if self.sent.connected[peer] {
                    assert!(
                        after
                            .iter()
                            .any(|(id, _, live)| *id == node_id && *live == 1),
                        "{STATE_PROPERTY}: a connected peer's summary was not kept"
                    );
                    self.sent.received[peer] = Some((now, ttl));
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
        let connected = self.sent.connected_node_ids();
        for peer in 0..PEERS {
            let node_id = peer_secret(peer).public();
            let active = self
                .state
                .active_services(node_id, &connected, now)
                .is_some();
            let live = self
                .state
                .live_service_summaries(node_id, &connected, now)
                .is_some_and(|summaries| !summaries.is_empty());
            assert!(
                !active || self.sent.connected[peer],
                "{STATE_PROPERTY}: peer {peer} serves hints while disconnected"
            );
            // A live summary lasts no longer than its declared TTL, and no
            // longer than the local cap.
            if live {
                let (received, ttl) = self.sent.received[peer].unwrap_or_else(|| {
                    panic!("{STATE_PROPERTY}: peer {peer} live without summaries")
                });
                assert!(
                    now < received + ttl && now < received + LIVE_SUMMARY_TTL,
                    "{STATE_PROPERTY}: peer {peer} live summaries outlived their TTL"
                );
            }
        }
    }

    fn check_sessions(&self) {
        self.state.assert_invariants();
        for (peer, admitted) in self.sent.admitted.iter().enumerate() {
            if let Some((conn_id, session_id, _)) = admitted {
                assert!(
                    self.state
                        .is_current_session(&session(peer, *conn_id, *session_id)),
                    "{STATE_PROPERTY}: lost peer {peer}'s admitted session"
                );
            }
        }
        assert_eq!(
            self.state.peer_snapshot(),
            ServicePeerSnapshot::new(
                self.sent.admitted_in(false),
                self.sent.admitted_in(true),
                peer_limits()
            ),
            "{STATE_PROPERTY}: admitted sessions"
        );
        assert_eq!(
            self.state.book().discovered_len(),
            self.sent.in_book.iter().filter(|in_book| **in_book).count(),
            "{STATE_PROPERTY}: the book holds a Hello no current session installed"
        );
        // Each record hint comes from an installed `Hello`, and each live
        // summary from a current `Services`. A peer's summaries replace its
        // last ones.
        for (node_id, record, live) in self.state.retained_hints() {
            let peer = peer_index(node_id);
            assert!(
                !record || self.sent.in_book[peer],
                "{STATE_PROPERTY}: peer {peer} has an unearned record hint"
            );
            assert!(
                live <= 1,
                "{STATE_PROPERTY}: peer {peer} keeps {live} live summaries"
            );
        }
    }

    fn view(&self) -> SessionView {
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

/// Session and hint state after one operation, read from the owner.
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
    pub view: SessionView,
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
            input: step_input(input, index + 1, STATE_OP_BYTES),
            view: driver.view(),
            failure,
        });
        if stop {
            break;
        }
    }
    steps
}

/// The bytes each session-installation operation reads.
const STATE_OP_BYTES: usize = 4;

/// Decodes four bytes into each session-installation operation. Generator 2 names
/// this decoder in saved scenarios.
fn decode_state_v2(data: &[u8]) -> impl Iterator<Item = StateOp> + '_ {
    data.chunks(STATE_OP_BYTES).map(|chunk| {
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

/// An import expiry, relative to now, past the clock-skew tolerance, so the
/// record expired before it arrived. Decoder v3 chooses it.
const EXPIRED_ON_ARRIVAL: i64 = -400;

/// The bytes each record-book operation reads.
const BOOK_OP_BYTES: usize = 2;

/// Decodes two bytes into each record-book operation. Generator 3 names this
/// decoder in saved scenarios.
///
/// The first byte's low four bits choose the operation: 11 of 16 values import,
/// 3 sample, 1 dials and 1 advances the clock. An import's record mostly lives
/// for 900 or 3,600 seconds, past a typical advance of under a minute. A
/// quarter of imports carry a record that has already lapsed, so it orders
/// nothing and its relay's quota can evict it. A few of those expired before
/// they arrived.
fn decode_book_v3(data: &[u8]) -> impl Iterator<Item = BookOp> + '_ {
    data.chunks(BOOK_OP_BYTES).map(|chunk| {
        let a = chunk[0];
        let b = usize::from(chunk.get(1).copied().unwrap_or(0));
        let author = b % AUTHORS;
        let relay = b / AUTHORS % RELAYS;
        // 0..=14, from the bits the author and relay leave.
        let rest = b / (AUTHORS * RELAYS);
        match a % 16 {
            0..=10 => BookOp::Import {
                author,
                sequence: (rest % 4) as u64,
                expires_in: match a >> 6 {
                    // Past the clock-skew tolerance, so the record expired before it arrived.
                    0 if rest == 0 => EXPIRED_ON_ARRIVAL,
                    // Lapsed, but inside the clock-skew tolerance.
                    0 => -100,
                    1 | 2 => 900,
                    _ => 3_600,
                },
                supplier: match a >> 4 & 3 {
                    0 | 1 => Supplier::Author,
                    2 => Supplier::Relay(relay),
                    _ => Supplier::Nobody,
                },
            },
            11..=13 => BookOp::Sample {
                requester: b % RELAYS,
                limit: (b >> 2) % 5,
            },
            14 if b < 128 => BookOp::DialSuccess(author),
            14 => BookOp::DialFailure(author),
            _ => BookOp::Advance(if b < 192 {
                (b % 64) as u64
            } else {
                DISCLOSURE_EPOCH
            }),
        }
    })
}

/// The two bytes that [`decode_book_v3`] decodes to `op`.
///
/// # Panics
///
/// If no two bytes decode to `op`.
fn encode_book_v3(op: &BookOp) -> [u8; BOOK_OP_BYTES] {
    (0..=u16::MAX)
        .map(u16::to_be_bytes)
        .find(|bytes| decode_book_v3(bytes).next().as_ref() == Some(op))
        .unwrap_or_else(|| panic!("decoder v3 has no bytes for {op:?}"))
}

/// The record-book witness, `zakura-sim`'s `discovery-record-book` seed,
/// under the current decoder.
///
/// Its steps reach each planted bypass's trigger:
/// - step 6 evicts at the record limit;
/// - step 7 samples four confirmed records under a window of three;
/// - step 13 evicts a relay's lapsed records on its quota;
/// - step 14 moves a record from one relay to another;
/// - step 18 overfills a book of confirmed records and one unconfirmed record.
pub fn discovery_book_witness_bytes() -> Vec<u8> {
    use BookOp::{Advance, DialSuccess, Import, Sample};
    let import = |author, sequence, expires_in, supplier| Import {
        author,
        sequence,
        expires_in,
        supplier,
    };
    [
        // Authors 0..=3 import their own records, and a dial confirms author 0.
        import(0, 1, 900, Supplier::Author),
        import(1, 1, 900, Supplier::Author),
        import(2, 1, 900, Supplier::Author),
        import(3, 1, 900, Supplier::Author),
        DialSuccess(0),
        // Author 4 evicts one of the records no dial confirmed.
        import(4, 1, 900, Supplier::Author),
        // Sample four, under a window of three.
        Sample {
            requester: 0,
            limit: 4,
        },
        // Author 0 replaces its record with a later sequence.
        import(0, 3, 900, Supplier::Author),
        // Two epochs pass, so every record expires.
        Advance(DISCLOSURE_EPOCH),
        Advance(DISCLOSURE_EPOCH),
        // Relay 0 takes authors 0 and 1 with lapsed records.
        import(0, 2, -100, Supplier::Relay(0)),
        import(1, 2, -100, Supplier::Relay(0)),
        // Relay 0's next new record, from author 5, evicts them on its quota.
        import(5, 2, 3_600, Supplier::Relay(0)),
        // Relay 1 supplies author 5's later sequence, so the attribution moves.
        import(5, 3, 3_600, Supplier::Relay(1)),
        // Authors 0..=2 import their own records, which evicts the lapsed ones.
        import(0, 3, 3_600, Supplier::Author),
        import(1, 3, 3_600, Supplier::Author),
        import(2, 3, 3_600, Supplier::Author),
        // Author 3 overfills a book of confirmed records and author 5's unconfirmed one.
        import(3, 3, 3_600, Supplier::Author),
    ]
    .iter()
    .flat_map(encode_book_v3)
    .collect()
}

/// The session-installation witness, `zakura-sim`'s
/// `discovery-session-installation` seed: it installs a current result,
/// rejects a stale result, retains a hint, and reads it.
pub const DISCOVERY_STATE_WITNESS: &[u8] = &[
    0, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 0, 0, 1, 1, 5, 0, 0, 0, 4, 0, 1, 1, 5, 0, 0, 0, 6, 0, 1, 1,
    7, 0, 0, 40, 8, 0, 0, 0, // Peer 0 disconnects while peer 1's `Hello` installs.
    3, 0, 1, 0, 0, 1, 0, 0, 3, 1, 0, 0, 4, 1, 0, 0, 5, 0, 0, 0, 8, 0, 0, 0,
];
