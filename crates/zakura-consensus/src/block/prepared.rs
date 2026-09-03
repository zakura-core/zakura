use std::{
    collections::{HashMap, HashSet, VecDeque},
    mem::size_of,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use blake2b_simd::Params;
use chrono::{DateTime, Utc};
use zakura_chain::{
    block::{Block, Header},
    parameters::Network,
    serialization::ZcashSerialize,
    work::equihash::Solution,
};
use zakura_state::SemanticallyVerifiedBlock;

use super::PreparedCandidateSource;

const SERVER_MAX_ENTRIES: usize = 24;
// One 219 ms preparation at a time can produce fewer than 2,800 aliases per TTL.
const SERVER_MAX_WORK_IDS: usize = 3_072;
const SERVER_MAX_BYTES: usize = 48 * 1024 * 1024;
const PROPOSAL_MAX_ENTRIES: usize = 8;
const PROPOSAL_MAX_WORK_IDS: usize = 1_024;
const PROPOSAL_MAX_BYTES: usize = 16 * 1024 * 1024;
const ENTRY_TTL: Duration = Duration::from_secs(10 * 60);

type EntryId = u64;

#[derive(Clone, Default)]
pub(super) struct PreparedCandidateCache(Arc<Mutex<CacheInner>>);

impl std::fmt::Debug for PreparedCandidateCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        f.debug_struct("PreparedCandidateCache")
            .field("server_entries", &inner.server.entries.len())
            .field("server_work_ids", &inner.server.work_ids.len())
            .field("server_bytes", &inner.server.bytes)
            .field("proposal_entries", &inner.proposals.entries.len())
            .field("proposal_work_ids", &inner.proposals.work_ids.len())
            .field("proposal_bytes", &inner.proposals.bytes)
            .finish()
    }
}

/// Resolves compact mined-block submissions from the consensus candidate cache.
#[derive(Clone, Default)]
pub struct PreparedCandidateResolver(Arc<Mutex<CacheInner>>);

impl std::fmt::Debug for PreparedCandidateResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        f.debug_struct("PreparedCandidateResolver")
            .field("server_entries", &inner.server.entries.len())
            .field("server_work_ids", &inner.server.work_ids.len())
            .field("proposal_entries", &inner.proposals.entries.len())
            .field("proposal_work_ids", &inner.proposals.work_ids.len())
            .finish()
    }
}

/// An error returned when a compact submission cannot use a prepared candidate.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ResolvePreparedCandidateError {
    /// The cache no longer contains the supplied `workid`.
    #[error("the prepared candidate is no longer available")]
    StaleWork,

    /// The solved header changed a field that compact submission must preserve.
    #[error("the solved header does not match the prepared candidate")]
    CandidateMismatch,
}

#[derive(Default)]
struct CacheInner {
    server: Partition,
    proposals: Partition,
}

#[derive(Default)]
struct Partition {
    entries: VecDeque<Entry>,
    work_ids: HashMap<String, WorkIdAlias>,
    work_id_order: VecDeque<String>,
    bytes: usize,
    next_entry_id: EntryId,
}

struct Entry {
    id: EntryId,
    fingerprint: [u8; 32],
    prepared: SemanticallyVerifiedBlock,
    size: usize,
    expires_at: Instant,
}

struct WorkIdAlias {
    entry_id: EntryId,
    size: usize,
    expires_at: Instant,
}

pub(super) struct CachedPreparedCandidate {
    pub source: PreparedCandidateSource,
    pub prepared: Arc<SemanticallyVerifiedBlock>,
}

impl PreparedCandidateResolver {
    /// Reconstructs a mined block from a cached candidate and a solved header.
    pub fn resolve(
        &self,
        work_id: &str,
        solved_header: Header,
    ) -> Result<Arc<Block>, ResolvePreparedCandidateError> {
        let mut inner = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        inner.prune_expired();

        let entry = inner
            .find_work_id(work_id)
            .ok_or(ResolvePreparedCandidateError::StaleWork)?;
        if !preserved_header_fields_match(&entry.prepared.block.header, &solved_header) {
            return Err(ResolvePreparedCandidateError::CandidateMismatch);
        }

        Ok(Arc::new(Block {
            header: Arc::new(solved_header),
            transactions: entry.prepared.block.transactions.clone(),
        }))
    }

    /// Inserts a candidate for cross-crate tests.
    #[cfg(any(test, feature = "proptest-impl"))]
    #[doc(hidden)]
    pub fn insert_for_test(
        &self,
        block: Arc<Block>,
        work_id: &str,
        source: PreparedCandidateSource,
        network: &Network,
    ) {
        PreparedCandidateCache(self.0.clone()).insert(
            &block,
            Some(work_id),
            source,
            SemanticallyVerifiedBlock::from(block.clone()),
            network,
        );
    }
}

impl PreparedCandidateCache {
    pub(super) fn resolver(&self) -> PreparedCandidateResolver {
        PreparedCandidateResolver(self.0.clone())
    }

    pub(super) fn lookup(
        &self,
        block: &Block,
        work_id: Option<&str>,
        network: &Network,
    ) -> Option<CachedPreparedCandidate> {
        let mut inner = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        inner.prune_expired();

        if inner.is_empty() {
            metrics::counter!("mining.prepared_cache.misses").increment(1);
            return None;
        }

        if let Some(work_id) = work_id {
            if let Some((source, entry)) = inner.find_work_id_with_source(work_id) {
                if candidates_match(&entry.prepared.block, block) {
                    metrics::counter!("mining.prepared_cache.hits").increment(1);
                    return Some(CachedPreparedCandidate {
                        source,
                        prepared: Arc::new(entry.prepared.clone()),
                    });
                }

                metrics::counter!("mining.prepared_cache.mismatches").increment(1);
            }
        }

        let fingerprint = candidate_fingerprint(block, network);
        if let Some((source, entry)) = inner.find_candidate(fingerprint, block) {
            metrics::counter!("mining.prepared_cache.hits").increment(1);
            return Some(CachedPreparedCandidate {
                source,
                prepared: Arc::new(entry.prepared.clone()),
            });
        }

        metrics::counter!("mining.prepared_cache.misses").increment(1);
        None
    }

    pub(super) fn insert(
        &self,
        block: &Block,
        work_id: Option<&str>,
        source: PreparedCandidateSource,
        prepared: SemanticallyVerifiedBlock,
        network: &Network,
    ) {
        let prepared_size = prepared_candidate_size(&prepared);
        let alias_size = work_id.map(work_id_alias_size).unwrap_or(0);
        let limits = source.limits();
        if prepared_size > limits.max_bytes
            || prepared_size.saturating_add(alias_size) > limits.max_bytes
        {
            return;
        }

        let fingerprint = candidate_fingerprint(block, network);
        let mut inner = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        inner.prune_expired();

        if let Some(work_id) = work_id {
            if let Some((existing_source, entry)) = inner.find_work_id_with_source(work_id) {
                if candidates_match(&entry.prepared.block, block) {
                    if source != PreparedCandidateSource::ServerTemplate
                        || existing_source == PreparedCandidateSource::ServerTemplate
                    {
                        return;
                    }
                } else {
                    metrics::counter!(
                        "mining.prepared_cache.work_id_conflicts",
                        "source" => source.metric_label()
                    )
                    .increment(1);
                    let server_reclaims_proposal = source
                        == PreparedCandidateSource::ServerTemplate
                        && existing_source == PreparedCandidateSource::ClientProposal;
                    if !server_reclaims_proposal {
                        return;
                    }
                }

                inner.partition(existing_source).remove_work_id(work_id);
            }
        }

        let partition = inner.partition(source);
        let existing_entry = partition
            .entries
            .iter()
            .find(|entry| {
                entry.fingerprint == fingerprint && candidates_match(&entry.prepared.block, block)
            })
            .map(|entry| entry.id);
        let entry_id = match existing_entry {
            Some(entry_id) => {
                partition.refresh_candidate(entry_id, prepared, limits);
                Some(entry_id)
            }
            None => partition.insert_candidate(fingerprint, prepared, source, limits),
        };

        if let (Some(entry_id), Some(work_id)) = (entry_id, work_id) {
            partition.insert_work_id(work_id, entry_id, source, limits);
        }
    }
}

impl CacheInner {
    fn prune_expired(&mut self) {
        self.server.prune_expired();
        self.proposals.prune_expired();
    }

    fn is_empty(&self) -> bool {
        self.server.entries.is_empty() && self.proposals.entries.is_empty()
    }

    fn partition(&mut self, source: PreparedCandidateSource) -> &mut Partition {
        match source {
            PreparedCandidateSource::ServerTemplate => &mut self.server,
            PreparedCandidateSource::ClientProposal => &mut self.proposals,
        }
    }

    fn find_work_id(&self, work_id: &str) -> Option<&Entry> {
        self.server
            .entry_for_work_id(work_id)
            .or_else(|| self.proposals.entry_for_work_id(work_id))
    }

    fn find_work_id_with_source(&self, work_id: &str) -> Option<(PreparedCandidateSource, &Entry)> {
        self.server
            .entry_for_work_id(work_id)
            .map(|entry| (PreparedCandidateSource::ServerTemplate, entry))
            .or_else(|| {
                self.proposals
                    .entry_for_work_id(work_id)
                    .map(|entry| (PreparedCandidateSource::ClientProposal, entry))
            })
    }

    fn find_candidate(
        &self,
        fingerprint: [u8; 32],
        block: &Block,
    ) -> Option<(PreparedCandidateSource, &Entry)> {
        self.server
            .find_candidate(fingerprint, block)
            .map(|entry| (PreparedCandidateSource::ServerTemplate, entry))
            .or_else(|| {
                self.proposals
                    .find_candidate(fingerprint, block)
                    .map(|entry| (PreparedCandidateSource::ClientProposal, entry))
            })
    }
}

impl Partition {
    fn entry_for_work_id(&self, work_id: &str) -> Option<&Entry> {
        let entry_id = self.work_ids.get(work_id)?.entry_id;
        self.entries.iter().find(|entry| entry.id == entry_id)
    }

    fn find_candidate(&self, fingerprint: [u8; 32], block: &Block) -> Option<&Entry> {
        self.entries.iter().find(|entry| {
            entry.fingerprint == fingerprint && candidates_match(&entry.prepared.block, block)
        })
    }

    fn prune_expired(&mut self) {
        let now = Instant::now();
        let expired_entries: Vec<_> = self
            .entries
            .iter()
            .filter(|entry| entry.expires_at <= now)
            .map(|entry| entry.id)
            .collect();
        for entry_id in expired_entries {
            self.remove_entry(entry_id);
        }

        let expired_work_ids: Vec<_> = self
            .work_ids
            .iter()
            .filter(|(_, alias)| alias.expires_at <= now)
            .map(|(work_id, _)| work_id.clone())
            .collect();
        for work_id in expired_work_ids {
            self.remove_work_id(&work_id);
        }
    }

    fn insert_candidate(
        &mut self,
        fingerprint: [u8; 32],
        prepared: SemanticallyVerifiedBlock,
        source: PreparedCandidateSource,
        limits: PartitionLimits,
    ) -> Option<EntryId> {
        let size = prepared_candidate_size(&prepared);
        if size > limits.max_bytes {
            return None;
        }

        while self.entries.len() >= limits.max_entries
            || self.bytes.saturating_add(size) > limits.max_bytes
        {
            if !self.evict_oldest_entry(source) {
                return None;
            }
        }

        let id = self.next_entry_id;
        self.next_entry_id = self.next_entry_id.wrapping_add(1);
        self.bytes = self.bytes.saturating_add(size);
        self.entries.push_back(Entry {
            id,
            fingerprint,
            prepared,
            size,
            expires_at: Instant::now() + ENTRY_TTL,
        });
        Some(id)
    }

    fn refresh_candidate(
        &mut self,
        entry_id: EntryId,
        prepared: SemanticallyVerifiedBlock,
        limits: PartitionLimits,
    ) {
        let Some(index) = self.entries.iter().position(|entry| entry.id == entry_id) else {
            return;
        };
        let mut entry = self
            .entries
            .remove(index)
            .expect("entry exists because its index came from the same deque");
        let retained_bytes = self.bytes.saturating_sub(entry.size);
        let replacement_size = prepared_candidate_size(&prepared);
        if retained_bytes.saturating_add(replacement_size) <= limits.max_bytes {
            self.bytes = retained_bytes.saturating_add(replacement_size);
            entry.size = replacement_size;
            entry.prepared = prepared;
        }
        entry.expires_at = Instant::now() + ENTRY_TTL;
        self.entries.push_back(entry);
    }

    fn insert_work_id(
        &mut self,
        work_id: &str,
        entry_id: EntryId,
        source: PreparedCandidateSource,
        limits: PartitionLimits,
    ) {
        if self.work_ids.contains_key(work_id) {
            return;
        }

        let size = work_id_alias_size(work_id);
        while self.work_ids.len() >= limits.max_work_ids
            || self.bytes.saturating_add(size) > limits.max_bytes
        {
            if !self.evict_oldest_work_id(source) {
                return;
            }
        }

        self.bytes = self.bytes.saturating_add(size);
        self.work_id_order.push_back(work_id.to_owned());
        self.work_ids.insert(
            work_id.to_owned(),
            WorkIdAlias {
                entry_id,
                size,
                expires_at: Instant::now() + ENTRY_TTL,
            },
        );
    }

    fn remove_entry(&mut self, entry_id: EntryId) -> bool {
        let Some(index) = self.entries.iter().position(|entry| entry.id == entry_id) else {
            return false;
        };
        let entry = self
            .entries
            .remove(index)
            .expect("entry exists because its index came from the same deque");
        self.bytes = self.bytes.saturating_sub(entry.size);

        let work_ids: HashSet<_> = self
            .work_ids
            .iter()
            .filter(|(_, alias)| alias.entry_id == entry_id)
            .map(|(work_id, _)| work_id.clone())
            .collect();
        for work_id in &work_ids {
            if let Some(alias) = self.work_ids.remove(work_id) {
                self.bytes = self.bytes.saturating_sub(alias.size);
            }
        }
        self.work_id_order
            .retain(|work_id| !work_ids.contains(work_id));
        true
    }

    fn remove_work_id(&mut self, work_id: &str) -> bool {
        let Some(alias) = self.work_ids.remove(work_id) else {
            return false;
        };
        self.bytes = self.bytes.saturating_sub(alias.size);
        if let Some(index) = self
            .work_id_order
            .iter()
            .position(|candidate| candidate == work_id)
        {
            self.work_id_order.remove(index);
        }
        true
    }

    fn evict_oldest_entry(&mut self, source: PreparedCandidateSource) -> bool {
        let Some(entry_id) = self.entries.front().map(|entry| entry.id) else {
            return false;
        };
        let removed = self.remove_entry(entry_id);
        if removed {
            metrics::counter!(
                "mining.prepared_cache.evictions",
                "source" => source.metric_label()
            )
            .increment(1);
        }
        removed
    }

    fn evict_oldest_work_id(&mut self, source: PreparedCandidateSource) -> bool {
        let Some(work_id) = self.work_id_order.front().cloned() else {
            return false;
        };
        let removed = self.remove_work_id(&work_id);
        if removed {
            metrics::counter!(
                "mining.prepared_cache.work_id_evictions",
                "source" => source.metric_label()
            )
            .increment(1);
        }
        removed
    }
}

#[derive(Clone, Copy)]
struct PartitionLimits {
    max_entries: usize,
    max_work_ids: usize,
    max_bytes: usize,
}

impl PreparedCandidateSource {
    fn limits(self) -> PartitionLimits {
        match self {
            Self::ServerTemplate => PartitionLimits {
                max_entries: SERVER_MAX_ENTRIES,
                max_work_ids: SERVER_MAX_WORK_IDS,
                max_bytes: SERVER_MAX_BYTES,
            },
            Self::ClientProposal => PartitionLimits {
                max_entries: PROPOSAL_MAX_ENTRIES,
                max_work_ids: PROPOSAL_MAX_WORK_IDS,
                max_bytes: PROPOSAL_MAX_BYTES,
            },
        }
    }

    fn metric_label(self) -> &'static str {
        match self {
            Self::ServerTemplate => "server_template",
            Self::ClientProposal => "client_proposal",
        }
    }
}

fn candidates_match(cached: &Block, submitted: &Block) -> bool {
    preserved_header_fields_match(&cached.header, &submitted.header)
        && cached.transactions == submitted.transactions
}

fn preserved_header_fields_match(cached: &Header, submitted: &Header) -> bool {
    cached.version == submitted.version
        && cached.previous_block_hash == submitted.previous_block_hash
        && cached.merkle_root == submitted.merkle_root
        && cached.commitment_bytes == submitted.commitment_bytes
        && cached.difficulty_threshold == submitted.difficulty_threshold
}

fn candidate_fingerprint(block: &Block, network: &Network) -> [u8; 32] {
    let mut header: Header = *block.header;
    header.time =
        DateTime::<Utc>::from_timestamp(0, 0).expect("the Unix epoch is a valid UTC timestamp");
    header.nonce = [0; 32].into();
    header.solution = Solution::for_proposal_for_network(network);

    let bytes = Block {
        header: Arc::new(header),
        transactions: block.transactions.clone(),
    }
    .zcash_serialize_to_vec()
    .expect("serialization to memory cannot fail");
    let hash = Params::new().hash_length(32).hash(&bytes);
    let mut fingerprint = [0; 32];
    fingerprint.copy_from_slice(hash.as_bytes());
    fingerprint
}

fn prepared_candidate_size(prepared: &SemanticallyVerifiedBlock) -> usize {
    let block_size =
        usize::try_from(prepared.block.attributed_memory_size_bytes()).unwrap_or(usize::MAX);
    let transaction_hashes = prepared
        .transaction_hashes
        .len()
        .saturating_mul(size_of::<zakura_chain::transaction::Hash>());
    let new_outputs = prepared.new_outputs.capacity().saturating_mul(
        size_of::<zakura_chain::transparent::OutPoint>()
            .saturating_add(size_of::<zakura_chain::transparent::OrderedUtxo>()),
    );
    // The output map owns cloned scripts in addition to the scripts stored in the block.
    let new_output_scripts = prepared
        .new_outputs
        .values()
        .map(|output| output.utxo.output.lock_script.as_raw_bytes().len())
        .fold(0usize, usize::saturating_add);

    size_of::<Entry>()
        .saturating_add(block_size)
        .saturating_add(transaction_hashes)
        .saturating_add(new_outputs)
        .saturating_add(new_output_scripts)
}

fn work_id_alias_size(work_id: &str) -> usize {
    size_of::<WorkIdAlias>()
        .saturating_add(size_of::<String>().saturating_mul(2))
        .saturating_add(work_id.len().saturating_mul(2))
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::{
        block::Hash, serialization::ZcashDeserialize, work::difficulty::INVALID_COMPACT_DIFFICULTY,
    };

    fn test_block() -> Block {
        Block::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES[..])
            .expect("the genesis test vector is valid")
    }

    fn distinct_block(index: u8) -> Block {
        let mut block = test_block();
        Arc::make_mut(&mut block.header).previous_block_hash = Hash([index; 32]);
        block
    }

    fn insert(
        cache: &PreparedCandidateCache,
        block: &Block,
        work_id: &str,
        source: PreparedCandidateSource,
    ) {
        cache.insert(
            block,
            Some(work_id),
            source,
            SemanticallyVerifiedBlock::from(Arc::new(block.clone())),
            &Network::Mainnet,
        );
    }

    #[test]
    fn solved_header_fields_reuse_prepared_candidate() {
        let network = Network::Mainnet;
        let original = test_block();
        let cache = PreparedCandidateCache::default();
        insert(
            &cache,
            &original,
            "work",
            PreparedCandidateSource::ServerTemplate,
        );

        let mut solved = original;
        let header = Arc::make_mut(&mut solved.header);
        header.nonce = [7; 32].into();
        header.solution = Solution::for_proposal_for_network(&network);
        header.time += chrono::Duration::seconds(1);

        assert!(cache.lookup(&solved, Some("work"), &network).is_some());
        assert!(cache.lookup(&solved, None, &network).is_some());
    }

    #[test]
    fn preserved_candidate_changes_do_not_reuse_work_id() {
        let network = Network::Mainnet;
        let original = test_block();
        let cache = PreparedCandidateCache::default();
        insert(
            &cache,
            &original,
            "work",
            PreparedCandidateSource::ServerTemplate,
        );

        let mut changed_parent = original.clone();
        Arc::make_mut(&mut changed_parent.header).previous_block_hash = Hash([1; 32]);
        assert!(cache
            .lookup(&changed_parent, Some("work"), &network)
            .is_none());

        let mut changed_header = original.clone();
        Arc::make_mut(&mut changed_header.header).version ^= 1;
        assert!(cache
            .lookup(&changed_header, Some("work"), &network)
            .is_none());

        let mut changed_commitment = original.clone();
        Arc::make_mut(&mut changed_commitment.header).commitment_bytes[0] ^= 1;
        assert!(cache
            .lookup(&changed_commitment, Some("work"), &network)
            .is_none());

        let mut changed_difficulty = original.clone();
        Arc::make_mut(&mut changed_difficulty.header).difficulty_threshold =
            INVALID_COMPACT_DIFFICULTY;
        assert!(cache
            .lookup(&changed_difficulty, Some("work"), &network)
            .is_none());

        let mut changed_transactions = original;
        changed_transactions
            .transactions
            .push(changed_transactions.transactions[0].clone());
        assert!(cache
            .lookup(&changed_transactions, Some("work"), &network)
            .is_none());
    }

    #[test]
    fn repeated_candidates_keep_every_same_source_work_id() {
        let block = test_block();
        let cache = PreparedCandidateCache::default();
        insert(
            &cache,
            &block,
            "first",
            PreparedCandidateSource::ServerTemplate,
        );
        insert(
            &cache,
            &block,
            "second",
            PreparedCandidateSource::ServerTemplate,
        );

        assert!(cache.resolver().resolve("first", *block.header).is_ok());
        assert!(cache.resolver().resolve("second", *block.header).is_ok());
        let inner = cache
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(inner.server.entries.len(), 1);
        assert_eq!(inner.server.work_ids.len(), 2);
    }

    #[test]
    fn resolver_reconstructs_only_solved_header_fields() {
        let block = test_block();
        let cache = PreparedCandidateCache::default();
        insert(
            &cache,
            &block,
            "work",
            PreparedCandidateSource::ServerTemplate,
        );

        let mut solved_header = *block.header;
        solved_header.time += chrono::Duration::seconds(1);
        solved_header.nonce = [9; 32].into();
        solved_header.solution = Solution::for_proposal_for_network(&Network::Mainnet);
        let solved = cache
            .resolver()
            .resolve("work", solved_header)
            .expect("the solved header preserves the prepared candidate");

        assert_eq!(*solved.header, solved_header);
        assert_eq!(solved.transactions, block.transactions);

        solved_header.merkle_root = Default::default();
        assert_eq!(
            cache.resolver().resolve("work", solved_header),
            Err(ResolvePreparedCandidateError::CandidateMismatch)
        );
        assert_eq!(
            cache.resolver().resolve("missing", *block.header),
            Err(ResolvePreparedCandidateError::StaleWork)
        );
    }

    #[test]
    fn proposal_alias_eviction_does_not_evict_server_aliases() {
        let block = test_block();
        let cache = PreparedCandidateCache::default();
        insert(
            &cache,
            &block,
            "server",
            PreparedCandidateSource::ServerTemplate,
        );
        for index in 0..=PROPOSAL_MAX_WORK_IDS {
            insert(
                &cache,
                &block,
                &format!("proposal-{index}"),
                PreparedCandidateSource::ClientProposal,
            );
        }

        let inner = cache
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(inner.proposals.work_ids.len(), PROPOSAL_MAX_WORK_IDS);
        assert!(!inner.proposals.work_ids.contains_key("proposal-0"));
        assert!(inner
            .proposals
            .work_ids
            .contains_key(&format!("proposal-{PROPOSAL_MAX_WORK_IDS}")));
        assert!(inner.server.work_ids.contains_key("server"));
        assert!(inner.server.bytes <= SERVER_MAX_BYTES);
        assert!(inner.proposals.bytes <= PROPOSAL_MAX_BYTES);
    }

    #[test]
    fn server_alias_eviction_does_not_evict_proposal_aliases() {
        let block = test_block();
        let cache = PreparedCandidateCache::default();
        insert(
            &cache,
            &block,
            "proposal",
            PreparedCandidateSource::ClientProposal,
        );
        for index in 0..=SERVER_MAX_WORK_IDS {
            insert(
                &cache,
                &block,
                &format!("server-{index}"),
                PreparedCandidateSource::ServerTemplate,
            );
        }

        let inner = cache
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(inner.server.work_ids.len(), SERVER_MAX_WORK_IDS);
        assert!(!inner.server.work_ids.contains_key("server-0"));
        assert!(inner.proposals.work_ids.contains_key("proposal"));
    }

    #[test]
    fn candidate_partitions_evict_only_their_oldest_candidate() {
        let cache = PreparedCandidateCache::default();
        let proposal = distinct_block(100);
        insert(
            &cache,
            &proposal,
            "proposal",
            PreparedCandidateSource::ClientProposal,
        );
        let servers: Vec<_> = (0..=SERVER_MAX_ENTRIES)
            .map(|index| distinct_block(index as u8))
            .collect();
        for (index, block) in servers.iter().enumerate() {
            insert(
                &cache,
                block,
                &format!("server-{index}"),
                PreparedCandidateSource::ServerTemplate,
            );
        }
        assert_eq!(
            cache.resolver().resolve("server-0", *servers[0].header),
            Err(ResolvePreparedCandidateError::StaleWork)
        );
        assert!(cache
            .resolver()
            .resolve("server-1", *servers[1].header)
            .is_ok());
        assert!(cache
            .resolver()
            .resolve("proposal", *proposal.header)
            .is_ok());

        let server = distinct_block(101);
        insert(
            &cache,
            &server,
            "server-stable",
            PreparedCandidateSource::ServerTemplate,
        );
        let proposals: Vec<_> = (110..=110 + PROPOSAL_MAX_ENTRIES)
            .map(|index| distinct_block(index as u8))
            .collect();
        for (index, block) in proposals.iter().enumerate() {
            insert(
                &cache,
                block,
                &format!("proposal-churn-{index}"),
                PreparedCandidateSource::ClientProposal,
            );
        }
        assert_eq!(
            cache
                .resolver()
                .resolve("proposal-churn-0", *proposals[0].header),
            Err(ResolvePreparedCandidateError::StaleWork)
        );
        assert!(cache
            .resolver()
            .resolve("proposal-churn-1", *proposals[1].header)
            .is_ok());
        assert!(cache
            .resolver()
            .resolve("server-stable", *server.header)
            .is_ok());

        let inner = cache
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(inner.server.entries.len() <= SERVER_MAX_ENTRIES);
        assert!(inner.proposals.entries.len() <= PROPOSAL_MAX_ENTRIES);
        assert!(inner.server.work_ids.len() <= SERVER_MAX_WORK_IDS);
        assert!(inner.proposals.work_ids.len() <= PROPOSAL_MAX_WORK_IDS);
        assert!(inner.server.bytes <= SERVER_MAX_BYTES);
        assert!(inner.proposals.bytes <= PROPOSAL_MAX_BYTES);
    }

    #[test]
    fn proposal_cannot_replace_a_server_work_id() {
        let cache = PreparedCandidateCache::default();
        let server = distinct_block(1);
        let proposal = distinct_block(2);
        insert(
            &cache,
            &server,
            "shared",
            PreparedCandidateSource::ServerTemplate,
        );
        insert(
            &cache,
            &proposal,
            "shared",
            PreparedCandidateSource::ClientProposal,
        );

        assert!(cache.resolver().resolve("shared", *server.header).is_ok());
        assert_eq!(
            cache.resolver().resolve("shared", *proposal.header),
            Err(ResolvePreparedCandidateError::CandidateMismatch)
        );
    }

    #[test]
    fn same_source_reused_work_id_cannot_replace_its_candidate() {
        let cache = PreparedCandidateCache::default();
        let original = distinct_block(1);
        let replacement = distinct_block(2);
        insert(
            &cache,
            &original,
            "shared",
            PreparedCandidateSource::ServerTemplate,
        );
        insert(
            &cache,
            &replacement,
            "shared",
            PreparedCandidateSource::ServerTemplate,
        );

        assert!(cache.resolver().resolve("shared", *original.header).is_ok());
        assert_eq!(
            cache.resolver().resolve("shared", *replacement.header),
            Err(ResolvePreparedCandidateError::CandidateMismatch)
        );
    }

    #[test]
    fn server_reclaims_its_work_id_from_a_proposal() {
        let cache = PreparedCandidateCache::default();
        let proposal = distinct_block(1);
        let server = distinct_block(2);
        insert(
            &cache,
            &proposal,
            "shared",
            PreparedCandidateSource::ClientProposal,
        );
        insert(
            &cache,
            &server,
            "shared",
            PreparedCandidateSource::ServerTemplate,
        );

        assert!(cache.resolver().resolve("shared", *server.header).is_ok());
        assert_eq!(
            cache.resolver().resolve("shared", *proposal.header),
            Err(ResolvePreparedCandidateError::CandidateMismatch)
        );
    }

    #[test]
    fn each_source_retains_the_same_candidate_under_its_own_work_id() {
        let block = test_block();
        let cache = PreparedCandidateCache::default();
        insert(
            &cache,
            &block,
            "server",
            PreparedCandidateSource::ServerTemplate,
        );
        insert(
            &cache,
            &block,
            "proposal",
            PreparedCandidateSource::ClientProposal,
        );

        assert!(cache.resolver().resolve("server", *block.header).is_ok());
        assert!(cache.resolver().resolve("proposal", *block.header).is_ok());
        let inner = cache
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(inner.server.entries.len(), 1);
        assert_eq!(inner.proposals.entries.len(), 1);
    }

    #[test]
    fn server_template_promotes_an_identical_proposal_work_id() {
        let block = test_block();
        let cache = PreparedCandidateCache::default();
        insert(
            &cache,
            &block,
            "shared",
            PreparedCandidateSource::ClientProposal,
        );
        insert(
            &cache,
            &block,
            "shared",
            PreparedCandidateSource::ServerTemplate,
        );

        let inner = cache
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(inner.server.work_ids.contains_key("shared"));
        assert!(!inner.proposals.work_ids.contains_key("shared"));
    }

    #[test]
    fn oversized_entries_do_not_exceed_partition_byte_limits() {
        let block = test_block();
        let cache = PreparedCandidateCache::default();
        let proposal_work_id = "p".repeat(PROPOSAL_MAX_BYTES);
        insert(
            &cache,
            &block,
            &proposal_work_id,
            PreparedCandidateSource::ClientProposal,
        );
        let server_work_id = "s".repeat(SERVER_MAX_BYTES);
        insert(
            &cache,
            &block,
            &server_work_id,
            PreparedCandidateSource::ServerTemplate,
        );

        let inner = cache
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(inner.server.entries.is_empty());
        assert!(inner.proposals.entries.is_empty());
        assert_eq!(inner.server.bytes, 0);
        assert_eq!(inner.proposals.bytes, 0);
    }

    #[test]
    fn candidate_expiry_removes_its_work_ids() {
        let block = test_block();
        let cache = PreparedCandidateCache::default();
        insert(
            &cache,
            &block,
            "work",
            PreparedCandidateSource::ServerTemplate,
        );
        {
            let mut inner = cache
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            inner
                .server
                .entries
                .front_mut()
                .expect("the inserted candidate exists")
                .expires_at = Instant::now();
        }

        assert_eq!(
            cache.resolver().resolve("work", *block.header),
            Err(ResolvePreparedCandidateError::StaleWork)
        );
        let inner = cache
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(inner.server.entries.is_empty());
        assert!(inner.server.work_ids.is_empty());
        assert_eq!(inner.server.bytes, 0);
    }

    #[test]
    fn candidate_size_counts_cloned_output_scripts() {
        let block = Arc::new(test_block());
        let mut prepared = SemanticallyVerifiedBlock::from(block);
        let original_size = prepared_candidate_size(&prepared);
        let output = prepared
            .new_outputs
            .values_mut()
            .next()
            .expect("the genesis block creates a transparent output");
        let original_script_len = output.utxo.output.lock_script.as_raw_bytes().len();
        let replacement_script = [0; 4_096];
        let replacement_script_len = replacement_script.len();
        output.utxo.output.lock_script =
            zakura_chain::transparent::Script::new(&replacement_script);

        assert_eq!(
            prepared_candidate_size(&prepared).saturating_sub(original_size),
            replacement_script_len.saturating_sub(original_script_len),
        );
    }
}
