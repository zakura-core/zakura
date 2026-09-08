use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, MutexGuard},
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
const SERVER_MAX_BYTES: usize = 48 * 1024 * 1024;
const PROPOSAL_MAX_ENTRIES: usize = 8;
const PROPOSAL_MAX_BYTES: usize = 16 * 1024 * 1024;
const ENTRY_TTL: Duration = Duration::from_secs(10 * 60);

/// Identifies a mining candidate by the block content a solution cannot change.
///
/// Two candidates share an identity exactly when a miner can turn one into the other by solving
/// it, so a cache hit means the stored semantic verification covers the looked-up block.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) struct CandidateId([u8; 32]);

impl CandidateId {
    /// Blake2b-256 of `block` with the header fields a solution sets normalized to fixed values.
    pub(super) fn of(block: &Block, network: &Network) -> Self {
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
        let mut id = [0; 32];
        id.copy_from_slice(hash.as_bytes());
        Self(id)
    }
}

/// A prepared candidate the verifier can reuse.
pub(super) struct CacheHit {
    /// Who supplied the candidate this hit came from.
    pub source: PreparedCandidateSource,
    /// The semantic verification result for the candidate content.
    pub prepared: Arc<SemanticallyVerifiedBlock>,
}

/// What an insertion did with the candidate it was given.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum InsertOutcome {
    /// The candidate is now cached.
    Inserted,
    /// The partition already holds this candidate content.
    AlreadyPresent,
    /// The candidate alone exceeds its partition's byte budget.
    Oversized,
}

struct Entry {
    id: CandidateId,
    prepared: Arc<SemanticallyVerifiedBlock>,
    size: usize,
    expires_at: Instant,
}

/// One source's entries, bounded independently of the other source's.
struct Partition {
    source: PreparedCandidateSource,
    max_entries: usize,
    max_bytes: usize,
    entries: VecDeque<Entry>,
    bytes: usize,
}

#[derive(Clone)]
pub(super) struct PreparedCandidateCache(Arc<Mutex<[Partition; 2]>>);

impl Default for PreparedCandidateCache {
    fn default() -> Self {
        Self::with_limits(
            (SERVER_MAX_ENTRIES, SERVER_MAX_BYTES),
            (PROPOSAL_MAX_ENTRIES, PROPOSAL_MAX_BYTES),
        )
    }
}

impl std::fmt::Debug for PreparedCandidateCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cache = self.lock();
        f.debug_struct("PreparedCandidateCache")
            .field("server_entries", &cache[0].entries.len())
            .field("server_bytes", &cache[0].bytes)
            .field("proposal_entries", &cache[1].entries.len())
            .field("proposal_bytes", &cache[1].bytes)
            .finish()
    }
}

impl PreparedCandidateCache {
    fn with_limits(server: (usize, usize), proposals: (usize, usize)) -> Self {
        // The array order is the lookup order: a candidate the server prepared authorizes relay,
        // so it must win over the same content proposed by a client.
        Self(Arc::new(Mutex::new([
            Partition::new(PreparedCandidateSource::ServerTemplate, server),
            Partition::new(PreparedCandidateSource::ClientProposal, proposals),
        ])))
    }

    fn lock(&self) -> MutexGuard<'_, [Partition; 2]> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Returns the prepared verification for `block`'s content, if some source prepared it.
    pub(super) fn lookup(&self, block: &Block, network: &Network) -> Option<CacheHit> {
        // Deriving the candidate id costs a full block serialization, so skip it when the cache
        // holds no entry that could match.
        if self.lock().iter().all(|partition| partition.is_empty()) {
            metrics::counter!("mining.prepared_cache.misses").increment(1);
            return None;
        }

        let id = CandidateId::of(block, network);
        let mut cache = self.lock();
        cache.iter_mut().for_each(Partition::prune_expired);
        let hit = cache.iter().find_map(|partition| {
            partition.get(id).map(|entry| CacheHit {
                source: partition.source,
                prepared: Arc::clone(&entry.prepared),
            })
        });

        metrics::counter!(if hit.is_some() {
            "mining.prepared_cache.hits"
        } else {
            "mining.prepared_cache.misses"
        })
        .increment(1);
        hit
    }

    /// Stores `prepared` under `id` in `source`'s partition.
    pub(super) fn insert(
        &self,
        id: CandidateId,
        source: PreparedCandidateSource,
        prepared: SemanticallyVerifiedBlock,
    ) -> InsertOutcome {
        let size = retained_size(&prepared);
        let mut cache = self.lock();
        let partition = &mut cache[source.index()];
        partition.prune_expired();

        if size > partition.max_bytes {
            return InsertOutcome::Oversized;
        }
        if partition.get(id).is_some() {
            return InsertOutcome::AlreadyPresent;
        }

        let evicted = partition.make_room(size);
        if evicted > 0 {
            metrics::counter!(
                "mining.prepared_cache.evictions",
                "source" => source.metric_label()
            )
            .increment(evicted);
        }
        partition.push(Entry {
            id,
            prepared: Arc::new(prepared),
            size,
            expires_at: Instant::now() + ENTRY_TTL,
        });

        InsertOutcome::Inserted
    }
}

impl Partition {
    fn new(source: PreparedCandidateSource, (max_entries, max_bytes): (usize, usize)) -> Self {
        Self {
            source,
            max_entries,
            max_bytes,
            entries: VecDeque::new(),
            bytes: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn get(&self, id: CandidateId) -> Option<&Entry> {
        self.entries.iter().find(|entry| entry.id == id)
    }

    fn prune_expired(&mut self) {
        let now = Instant::now();
        while self
            .entries
            .front()
            .is_some_and(|entry| entry.expires_at <= now)
        {
            self.pop_oldest();
        }
    }

    /// Evicts oldest-first until `size` fits within both bounds, returning how many entries went.
    fn make_room(&mut self, size: usize) -> u64 {
        let mut evicted = 0;
        while self.entries.len() >= self.max_entries
            || self.bytes.saturating_add(size) > self.max_bytes
        {
            if self.pop_oldest().is_none() {
                break;
            }
            evicted += 1;
        }
        evicted
    }

    fn pop_oldest(&mut self) -> Option<Entry> {
        let entry = self.entries.pop_front()?;
        self.bytes = self.bytes.saturating_sub(entry.size);
        Some(entry)
    }

    fn push(&mut self, entry: Entry) {
        self.bytes = self.bytes.saturating_add(entry.size);
        self.entries.push_back(entry);
    }
}

impl PreparedCandidateSource {
    /// This source's partition index, which is also the lookup order.
    fn index(self) -> usize {
        match self {
            Self::ServerTemplate => 0,
            Self::ClientProposal => 1,
        }
    }

    fn metric_label(self) -> &'static str {
        match self {
            Self::ServerTemplate => "server_template",
            Self::ClientProposal => "client_proposal",
        }
    }
}

fn retained_size(prepared: &SemanticallyVerifiedBlock) -> usize {
    // Charge two serialized copies for the decoded block and its cloned output scripts. Add the
    // derived map's allocated buckets and the transaction-hash array. This conservative cost
    // prevents output-heavy proposals from bypassing the byte budget.
    prepared
        .block
        .zcash_serialized_size()
        .saturating_mul(2)
        .saturating_add(
            prepared.new_outputs.capacity().saturating_mul(
                std::mem::size_of::<zakura_chain::transparent::OutPoint>()
                    .saturating_add(std::mem::size_of::<zakura_chain::transparent::OrderedUtxo>())
                    .saturating_add(1),
            ),
        )
        .saturating_add(
            prepared
                .transaction_hashes
                .len()
                .saturating_mul(std::mem::size_of::<zakura_chain::transaction::Hash>()),
        )
        .saturating_add(std::mem::size_of::<Entry>())
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
        source: PreparedCandidateSource,
        network: &Network,
    ) -> InsertOutcome {
        cache.insert(
            CandidateId::of(block, network),
            source,
            SemanticallyVerifiedBlock::from(Arc::new(block.clone())),
        )
    }

    #[test]
    fn solved_header_fields_reuse_prepared_candidate() {
        let network = Network::Mainnet;
        let original = test_block();
        let cache = PreparedCandidateCache::default();
        insert(
            &cache,
            &original,
            PreparedCandidateSource::ServerTemplate,
            &network,
        );

        let mut solved = original;
        let header = Arc::make_mut(&mut solved.header);
        header.nonce = [7; 32].into();
        header.solution = Solution::for_proposal_for_network(&network);
        header.time += chrono::Duration::seconds(1);

        assert!(cache.lookup(&solved, &network).is_some());
    }

    #[test]
    fn immutable_candidate_changes_miss_the_cache() {
        let network = Network::Mainnet;
        let original = test_block();
        let cache = PreparedCandidateCache::default();
        insert(
            &cache,
            &original,
            PreparedCandidateSource::ServerTemplate,
            &network,
        );

        let mut changed_parent = original.clone();
        Arc::make_mut(&mut changed_parent.header).previous_block_hash = Hash([1; 32]);
        assert!(cache.lookup(&changed_parent, &network).is_none());

        let mut changed_header = original.clone();
        Arc::make_mut(&mut changed_header.header).version ^= 1;
        assert!(cache.lookup(&changed_header, &network).is_none());

        let mut changed_commitment = original.clone();
        Arc::make_mut(&mut changed_commitment.header).commitment_bytes[0] ^= 1;
        assert!(cache.lookup(&changed_commitment, &network).is_none());

        let mut changed_difficulty = original.clone();
        Arc::make_mut(&mut changed_difficulty.header).difficulty_threshold =
            INVALID_COMPACT_DIFFICULTY;
        assert!(cache.lookup(&changed_difficulty, &network).is_none());

        let mut changed_merkle_root = original.clone();
        Arc::make_mut(&mut changed_merkle_root.header).merkle_root.0[0] ^= 1;
        assert!(cache.lookup(&changed_merkle_root, &network).is_none());

        let mut changed_transactions = original;
        changed_transactions
            .transactions
            .push(changed_transactions.transactions[0].clone());
        assert!(cache.lookup(&changed_transactions, &network).is_none());
    }

    /// Filling one partition past its entry bound must not evict the other partition's entries.
    fn partition_eviction_is_independent(filled: PreparedCandidateSource) {
        let network = Network::Mainnet;
        let cache = PreparedCandidateCache::default();
        let other = match filled {
            PreparedCandidateSource::ServerTemplate => PreparedCandidateSource::ClientProposal,
            PreparedCandidateSource::ClientProposal => PreparedCandidateSource::ServerTemplate,
        };
        let max_entries = cache.lock()[filled.index()].max_entries;

        let retained = distinct_block(100);
        insert(&cache, &retained, other, &network);

        let candidates: Vec<_> = (0..=max_entries)
            .map(|index| distinct_block(u8::try_from(index).expect("the bounds fit in a byte")))
            .collect();
        for candidate in &candidates {
            insert(&cache, candidate, filled, &network);
        }

        assert!(
            cache.lookup(&candidates[0], &network).is_none(),
            "the oldest entry of a full partition is evicted",
        );
        assert!(cache.lookup(&candidates[1], &network).is_some());
        assert!(
            cache.lookup(&retained, &network).is_some(),
            "the other partition keeps its entry",
        );

        let cache = cache.lock();
        assert_eq!(cache[filled.index()].entries.len(), max_entries);
        assert_eq!(cache[other.index()].entries.len(), 1);
        for partition in cache.iter() {
            assert!(partition.bytes <= partition.max_bytes);
        }
    }

    #[test]
    fn proposal_eviction_does_not_evict_server_candidates() {
        partition_eviction_is_independent(PreparedCandidateSource::ClientProposal);
    }

    #[test]
    fn server_eviction_does_not_evict_proposals() {
        partition_eviction_is_independent(PreparedCandidateSource::ServerTemplate);
    }

    #[test]
    fn duplicate_content_is_not_reinserted() {
        let network = Network::Mainnet;
        let cache = PreparedCandidateCache::default();
        let candidate = test_block();

        assert_eq!(
            insert(
                &cache,
                &candidate,
                PreparedCandidateSource::ServerTemplate,
                &network
            ),
            InsertOutcome::Inserted
        );
        assert_eq!(
            insert(
                &cache,
                &candidate,
                PreparedCandidateSource::ServerTemplate,
                &network
            ),
            InsertOutcome::AlreadyPresent
        );
        assert_eq!(cache.lock()[0].entries.len(), 1);
    }

    /// The same content prepared by both sources is served from the server partition.
    #[test]
    fn a_server_candidate_wins_a_content_lookup() {
        let network = Network::Mainnet;
        let cache = PreparedCandidateCache::default();
        let candidate = test_block();
        insert(
            &cache,
            &candidate,
            PreparedCandidateSource::ClientProposal,
            &network,
        );
        insert(
            &cache,
            &candidate,
            PreparedCandidateSource::ServerTemplate,
            &network,
        );

        assert_eq!(
            cache
                .lookup(&candidate, &network)
                .expect("both partitions hold the content")
                .source,
            PreparedCandidateSource::ServerTemplate
        );
    }

    #[test]
    fn oversized_entries_are_refused() {
        let network = Network::Mainnet;
        let cache = PreparedCandidateCache::with_limits((24, 1), (8, 1));
        let candidate = test_block();

        assert_eq!(
            insert(
                &cache,
                &candidate,
                PreparedCandidateSource::ServerTemplate,
                &network
            ),
            InsertOutcome::Oversized
        );
        assert_eq!(
            insert(
                &cache,
                &candidate,
                PreparedCandidateSource::ClientProposal,
                &network
            ),
            InsertOutcome::Oversized
        );

        let cache = cache.lock();
        for partition in cache.iter() {
            assert!(partition.entries.is_empty());
            assert_eq!(partition.bytes, 0);
        }
    }
}
