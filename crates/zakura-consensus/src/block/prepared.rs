use std::{
    collections::VecDeque,
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
const SERVER_MAX_BYTES: usize = 48 * 1024 * 1024;
const PROPOSAL_MAX_ENTRIES: usize = 8;
const PROPOSAL_MAX_BYTES: usize = 16 * 1024 * 1024;
const ENTRY_TTL: Duration = Duration::from_secs(10 * 60);

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
            .field("server_bytes", &inner.server.bytes)
            .field("proposal_entries", &inner.proposals.entries.len())
            .field("proposal_bytes", &inner.proposals.bytes)
            .finish()
    }
}

#[derive(Default)]
struct CacheInner {
    server: Partition,
    proposals: Partition,
}

#[derive(Default)]
struct Partition {
    entries: VecDeque<Entry>,
    bytes: usize,
}

struct Entry {
    work_id: Option<String>,
    fingerprint: [u8; 32],
    immutable_bytes: Vec<u8>,
    prepared: Arc<SemanticallyVerifiedBlock>,
    size: usize,
    expires_at: Instant,
}

pub(super) struct CachedPreparedCandidate {
    pub source: PreparedCandidateSource,
    pub prepared: Arc<SemanticallyVerifiedBlock>,
}

impl PreparedCandidateCache {
    pub(super) fn lookup(
        &self,
        block: &Block,
        work_id: Option<&str>,
        network: &Network,
    ) -> Option<CachedPreparedCandidate> {
        // Deriving the candidate bytes costs a full block serialization, so skip it when the
        // cache holds no entry that could match.
        {
            let mut inner = self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            inner.prune_expired();
            if inner.is_empty() {
                metrics::counter!("mining.prepared_cache.misses").increment(1);
                return None;
            }
        }

        let immutable_bytes = immutable_candidate_bytes(block, network);
        let fingerprint = fingerprint(&immutable_bytes);
        let mut inner = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        inner.prune_expired();

        if let Some(work_id) = work_id {
            if let Some((source, entry)) =
                inner.find_work_id_candidate(work_id, fingerprint, &immutable_bytes)
            {
                let prepared = Arc::clone(&entry.prepared);
                drop(inner);
                metrics::counter!("mining.prepared_cache.hits").increment(1);
                return Some(CachedPreparedCandidate { source, prepared });
            }
            if inner.contains_work_id(work_id) {
                metrics::counter!("mining.prepared_cache.mismatches").increment(1);
            }
        }

        if let Some((source, entry)) = inner.find_candidate(fingerprint, &immutable_bytes) {
            let prepared = Arc::clone(&entry.prepared);
            drop(inner);
            metrics::counter!("mining.prepared_cache.hits").increment(1);
            return Some(CachedPreparedCandidate { source, prepared });
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
        let immutable_bytes = immutable_candidate_bytes(block, network);
        let fingerprint = fingerprint(&immutable_bytes);

        let mut inner = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        inner.prune_expired();

        if let Some(work_id) = work_id {
            if let Some((existing_source, entry)) = inner.find_work_id_with_source(work_id) {
                if entry.immutable_bytes == immutable_bytes {
                    if source != PreparedCandidateSource::ServerTemplate
                        || existing_source == PreparedCandidateSource::ServerTemplate
                    {
                        return;
                    }

                    inner.proposals.remove_work_id(work_id);
                } else {
                    metrics::counter!(
                        "mining.prepared_cache.work_id_conflicts",
                        "source" => source.metric_label()
                    )
                    .increment(1);
                    let server_reclaims_proposal = source
                        == PreparedCandidateSource::ServerTemplate
                        && existing_source == PreparedCandidateSource::ClientProposal;
                    if server_reclaims_proposal {
                        inner.proposals.remove_work_id(work_id);
                    } else if source == PreparedCandidateSource::ServerTemplate {
                        return;
                    }
                }
            }
        }

        let partition = inner.partition(source);
        let existing_work_id = if work_id.is_none() {
            partition
                .entries
                .iter()
                .find(|entry| {
                    entry.fingerprint == fingerprint && entry.immutable_bytes == immutable_bytes
                })
                .and_then(|entry| entry.work_id.clone())
        } else {
            None
        };
        let size = retained_size(
            immutable_bytes.len(),
            work_id
                .map(str::len)
                .or_else(|| existing_work_id.as_ref().map(String::len))
                .unwrap_or(0),
            &prepared,
        );
        let (max_entries, max_bytes) = source.limits();
        if size > max_bytes {
            return;
        }
        let work_id = work_id.map(ToOwned::to_owned).or(existing_work_id);

        if let Some(work_id) = work_id.as_deref() {
            if source == PreparedCandidateSource::ServerTemplate
                && inner
                    .proposals
                    .entries
                    .iter()
                    .any(|entry| entry.work_id.as_deref() == Some(work_id))
            {
                inner.proposals.remove_work_id(work_id);
            }
        }

        let partition = inner.partition(source);
        partition.remove_matching(
            (source == PreparedCandidateSource::ServerTemplate)
                .then_some(work_id.as_deref())
                .flatten(),
            fingerprint,
            &immutable_bytes,
        );

        while partition.entries.len() >= max_entries
            || partition.bytes.saturating_add(size) > max_bytes
        {
            if !partition.evict_oldest(source) {
                break;
            }
        }

        partition.bytes = partition.bytes.saturating_add(size);
        partition.entries.push_back(Entry {
            work_id,
            fingerprint,
            immutable_bytes,
            prepared: Arc::new(prepared),
            size,
            expires_at: Instant::now() + ENTRY_TTL,
        });
    }
}

impl CacheInner {
    fn prune_expired(&mut self) {
        self.server
            .prune_expired(PreparedCandidateSource::ServerTemplate);
        self.proposals
            .prune_expired(PreparedCandidateSource::ClientProposal);
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

    fn contains_work_id(&self, work_id: &str) -> bool {
        self.server
            .entries
            .iter()
            .chain(self.proposals.entries.iter())
            .any(|entry| entry.work_id.as_deref() == Some(work_id))
    }

    fn find_work_id_candidate(
        &self,
        work_id: &str,
        fingerprint: [u8; 32],
        immutable_bytes: &[u8],
    ) -> Option<(PreparedCandidateSource, &Entry)> {
        self.server
            .entries
            .iter()
            .find(|entry| {
                entry.work_id.as_deref() == Some(work_id)
                    && entry.fingerprint == fingerprint
                    && entry.immutable_bytes == immutable_bytes
            })
            .map(|entry| (PreparedCandidateSource::ServerTemplate, entry))
            .or_else(|| {
                self.proposals
                    .entries
                    .iter()
                    .find(|entry| {
                        entry.work_id.as_deref() == Some(work_id)
                            && entry.fingerprint == fingerprint
                            && entry.immutable_bytes == immutable_bytes
                    })
                    .map(|entry| (PreparedCandidateSource::ClientProposal, entry))
            })
    }

    fn find_work_id_with_source(&self, work_id: &str) -> Option<(PreparedCandidateSource, &Entry)> {
        self.server
            .entries
            .iter()
            .find(|entry| entry.work_id.as_deref() == Some(work_id))
            .map(|entry| (PreparedCandidateSource::ServerTemplate, entry))
            .or_else(|| {
                self.proposals
                    .entries
                    .iter()
                    .find(|entry| entry.work_id.as_deref() == Some(work_id))
                    .map(|entry| (PreparedCandidateSource::ClientProposal, entry))
            })
    }

    fn find_candidate(
        &self,
        fingerprint: [u8; 32],
        immutable_bytes: &[u8],
    ) -> Option<(PreparedCandidateSource, &Entry)> {
        self.server
            .entries
            .iter()
            .find(|entry| {
                entry.fingerprint == fingerprint && entry.immutable_bytes == immutable_bytes
            })
            .map(|entry| (PreparedCandidateSource::ServerTemplate, entry))
            .or_else(|| {
                self.proposals
                    .entries
                    .iter()
                    .find(|entry| {
                        entry.fingerprint == fingerprint && entry.immutable_bytes == immutable_bytes
                    })
                    .map(|entry| (PreparedCandidateSource::ClientProposal, entry))
            })
    }
}

impl Partition {
    fn prune_expired(&mut self, source: PreparedCandidateSource) {
        let now = Instant::now();
        while self
            .entries
            .front()
            .is_some_and(|entry| entry.expires_at <= now)
        {
            self.evict_oldest(source);
        }
    }

    fn remove_matching(
        &mut self,
        work_id: Option<&str>,
        fingerprint: [u8; 32],
        immutable_bytes: &[u8],
    ) {
        while let Some(index) = self.entries.iter().position(|entry| {
            work_id.is_some_and(|work_id| entry.work_id.as_deref() == Some(work_id))
                || (entry.fingerprint == fingerprint && entry.immutable_bytes == immutable_bytes)
        }) {
            let entry = self
                .entries
                .remove(index)
                .expect("entry exists because its index came from the same deque");
            self.bytes = self.bytes.saturating_sub(entry.size);
        }
    }

    fn remove_work_id(&mut self, work_id: &str) {
        let Some(index) = self
            .entries
            .iter()
            .position(|entry| entry.work_id.as_deref() == Some(work_id))
        else {
            return;
        };
        let entry = self
            .entries
            .remove(index)
            .expect("entry exists because its index came from the same deque");
        self.bytes = self.bytes.saturating_sub(entry.size);
    }

    fn evict_oldest(&mut self, source: PreparedCandidateSource) -> bool {
        let Some(entry) = self.entries.pop_front() else {
            return false;
        };
        self.bytes = self.bytes.saturating_sub(entry.size);
        metrics::counter!(
            "mining.prepared_cache.evictions",
            "source" => source.metric_label()
        )
        .increment(1);
        true
    }
}

impl PreparedCandidateSource {
    fn limits(self) -> (usize, usize) {
        match self {
            Self::ServerTemplate => (SERVER_MAX_ENTRIES, SERVER_MAX_BYTES),
            Self::ClientProposal => (PROPOSAL_MAX_ENTRIES, PROPOSAL_MAX_BYTES),
        }
    }

    fn metric_label(self) -> &'static str {
        match self {
            Self::ServerTemplate => "server_template",
            Self::ClientProposal => "client_proposal",
        }
    }
}

fn retained_size(
    immutable_bytes: usize,
    work_id: usize,
    prepared: &SemanticallyVerifiedBlock,
) -> usize {
    // Charge three serialized copies for the normalized bytes, the decoded block, and cloned
    // output scripts. Add the derived map's allocated buckets and the transaction-hash array.
    // This conservative cost prevents output-heavy proposals from bypassing the byte budget.
    immutable_bytes
        .saturating_mul(3)
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
        .saturating_add(work_id)
        .saturating_add(std::mem::size_of::<Entry>())
}

fn immutable_candidate_bytes(block: &Block, network: &Network) -> Vec<u8> {
    let mut header: Header = *block.header;
    header.time =
        DateTime::<Utc>::from_timestamp(0, 0).expect("the Unix epoch is a valid UTC timestamp");
    header.nonce = [0; 32].into();
    header.solution = Solution::for_proposal_for_network(network);

    Block {
        header: Arc::new(header),
        transactions: block.transactions.clone(),
    }
    .zcash_serialize_to_vec()
    .expect("serialization to memory cannot fail")
}

fn fingerprint(bytes: &[u8]) -> [u8; 32] {
    let hash = Params::new().hash_length(32).hash(bytes);
    let mut fingerprint = [0; 32];
    fingerprint.copy_from_slice(hash.as_bytes());
    fingerprint
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
        network: &Network,
    ) {
        cache.insert(
            block,
            Some(work_id),
            source,
            SemanticallyVerifiedBlock::from(Arc::new(block.clone())),
            network,
        );
    }

    #[test]
    fn solved_header_fields_reuse_prepared_candidate() {
        let network = Network::Mainnet;
        let original = test_block();
        let prepared = SemanticallyVerifiedBlock::from(Arc::new(original.clone()));
        let cache = PreparedCandidateCache::default();
        cache.insert(
            &original,
            Some("work"),
            PreparedCandidateSource::ServerTemplate,
            prepared,
            &network,
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
    fn immutable_candidate_changes_do_not_reuse_work_id() {
        let network = Network::Mainnet;
        let original = test_block();
        let prepared = SemanticallyVerifiedBlock::from(Arc::new(original.clone()));
        let cache = PreparedCandidateCache::default();
        cache.insert(
            &original,
            Some("work"),
            PreparedCandidateSource::ServerTemplate,
            prepared,
            &network,
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

        let mut changed_merkle_root = original.clone();
        Arc::make_mut(&mut changed_merkle_root.header).merkle_root.0[0] ^= 1;
        assert!(cache
            .lookup(&changed_merkle_root, Some("work"), &network)
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
    fn proposal_work_id_conflict_falls_back_to_candidate_content() {
        let network = Network::Mainnet;
        let original = test_block();
        let mut replacement = original.clone();
        Arc::make_mut(&mut replacement.header).version ^= 1;
        let cache = PreparedCandidateCache::default();

        cache.insert(
            &original,
            Some("work"),
            PreparedCandidateSource::ServerTemplate,
            SemanticallyVerifiedBlock::from(Arc::new(original.clone())),
            &network,
        );
        cache.insert(
            &replacement,
            Some("work"),
            PreparedCandidateSource::ClientProposal,
            SemanticallyVerifiedBlock::from(Arc::new(replacement.clone())),
            &network,
        );

        assert!(cache.lookup(&replacement, Some("work"), &network).is_some());
        assert!(cache.lookup(&original, Some("work"), &network).is_some());
        let inner = cache
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(inner.server.entries.len(), 1);
        assert_eq!(inner.proposals.entries.len(), 1);
    }

    #[test]
    fn proposal_eviction_does_not_evict_server_candidates() {
        let network = Network::Mainnet;
        let cache = PreparedCandidateCache::default();
        let server = distinct_block(100);
        insert(
            &cache,
            &server,
            "server",
            PreparedCandidateSource::ServerTemplate,
            &network,
        );

        let proposals: Vec<_> = (0..=PROPOSAL_MAX_ENTRIES)
            .map(|index| distinct_block(index as u8))
            .collect();
        for (index, proposal) in proposals.iter().enumerate() {
            insert(
                &cache,
                proposal,
                &format!("proposal-{index}"),
                PreparedCandidateSource::ClientProposal,
                &network,
            );
        }

        assert!(cache
            .lookup(&proposals[0], Some("proposal-0"), &network)
            .is_none());
        assert!(cache
            .lookup(&proposals[1], Some("proposal-1"), &network)
            .is_some());
        assert!(cache.lookup(&server, Some("server"), &network).is_some());

        let inner = cache
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(inner.server.entries.len(), 1);
        assert_eq!(inner.proposals.entries.len(), PROPOSAL_MAX_ENTRIES);
        assert!(inner.server.bytes <= SERVER_MAX_BYTES);
        assert!(inner.proposals.bytes <= PROPOSAL_MAX_BYTES);
    }

    #[test]
    fn server_eviction_does_not_evict_proposals() {
        let network = Network::Mainnet;
        let cache = PreparedCandidateCache::default();
        let proposal = distinct_block(100);
        insert(
            &cache,
            &proposal,
            "proposal",
            PreparedCandidateSource::ClientProposal,
            &network,
        );

        let servers: Vec<_> = (0..=SERVER_MAX_ENTRIES)
            .map(|index| distinct_block(index as u8))
            .collect();
        for (index, server) in servers.iter().enumerate() {
            insert(
                &cache,
                server,
                &format!("server-{index}"),
                PreparedCandidateSource::ServerTemplate,
                &network,
            );
        }

        assert!(cache
            .lookup(&servers[0], Some("server-0"), &network)
            .is_none());
        assert!(cache
            .lookup(&servers[1], Some("server-1"), &network)
            .is_some());
        assert!(cache
            .lookup(&proposal, Some("proposal"), &network)
            .is_some());

        let inner = cache
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(inner.server.entries.len(), SERVER_MAX_ENTRIES);
        assert_eq!(inner.proposals.entries.len(), 1);
        assert!(inner.server.bytes <= SERVER_MAX_BYTES);
        assert!(inner.proposals.bytes <= PROPOSAL_MAX_BYTES);
    }

    #[test]
    fn proposal_work_id_conflict_does_not_replace_server_candidate() {
        let network = Network::Mainnet;
        let cache = PreparedCandidateCache::default();
        let server = distinct_block(1);
        let proposal = distinct_block(2);
        insert(
            &cache,
            &server,
            "shared",
            PreparedCandidateSource::ServerTemplate,
            &network,
        );
        insert(
            &cache,
            &proposal,
            "shared",
            PreparedCandidateSource::ClientProposal,
            &network,
        );

        assert!(cache.lookup(&server, Some("shared"), &network).is_some());
        let proposal_hit = cache
            .lookup(&proposal, Some("shared"), &network)
            .expect("content lookup finds the conflicting proposal");
        assert_eq!(proposal_hit.source, PreparedCandidateSource::ClientProposal);
        let inner = cache
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(inner.server.entries.len(), 1);
        assert_eq!(inner.proposals.entries.len(), 1);
    }

    #[test]
    fn server_reclaims_its_work_id_from_a_proposal() {
        let network = Network::Mainnet;
        let cache = PreparedCandidateCache::default();
        let proposal = distinct_block(1);
        let server = distinct_block(2);
        insert(
            &cache,
            &proposal,
            "shared",
            PreparedCandidateSource::ClientProposal,
            &network,
        );
        insert(
            &cache,
            &server,
            "shared",
            PreparedCandidateSource::ServerTemplate,
            &network,
        );

        assert!(cache.lookup(&server, Some("shared"), &network).is_some());
        assert!(cache.lookup(&proposal, Some("shared"), &network).is_none());
        let inner = cache
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(inner.server.entries.len(), 1);
        assert!(inner.proposals.entries.is_empty());
    }

    #[test]
    fn each_source_retains_the_same_candidate_under_its_own_work_id() {
        let network = Network::Mainnet;
        let cache = PreparedCandidateCache::default();
        let candidate = test_block();
        insert(
            &cache,
            &candidate,
            "server",
            PreparedCandidateSource::ServerTemplate,
            &network,
        );
        insert(
            &cache,
            &candidate,
            "proposal",
            PreparedCandidateSource::ClientProposal,
            &network,
        );

        assert!(cache.lookup(&candidate, Some("server"), &network).is_some());
        assert!(cache
            .lookup(&candidate, Some("proposal"), &network)
            .is_some());
        let inner = cache
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(inner.server.entries.len(), 1);
        assert_eq!(inner.proposals.entries.len(), 1);
    }

    #[test]
    fn identical_server_candidate_promotes_the_proposal_mapping() {
        let network = Network::Mainnet;
        let cache = PreparedCandidateCache::default();
        let candidate = test_block();
        insert(
            &cache,
            &candidate,
            "shared",
            PreparedCandidateSource::ClientProposal,
            &network,
        );
        insert(
            &cache,
            &candidate,
            "shared",
            PreparedCandidateSource::ServerTemplate,
            &network,
        );

        let inner = cache
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(inner.server.entries.len(), 1);
        assert!(inner.proposals.entries.is_empty());
    }

    #[test]
    fn oversized_entries_do_not_exceed_partition_byte_limits() {
        let network = Network::Mainnet;
        let cache = PreparedCandidateCache::default();
        let candidate = test_block();
        let oversized_proposal_id = "p".repeat(PROPOSAL_MAX_BYTES);
        insert(
            &cache,
            &candidate,
            &oversized_proposal_id,
            PreparedCandidateSource::ClientProposal,
            &network,
        );
        let oversized_server_id = "s".repeat(SERVER_MAX_BYTES);
        insert(
            &cache,
            &candidate,
            &oversized_server_id,
            PreparedCandidateSource::ServerTemplate,
            &network,
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
}
