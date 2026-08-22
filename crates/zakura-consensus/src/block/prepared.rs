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

const MAX_ENTRIES: usize = 32;
const MAX_BYTES: usize = 64 * 1024 * 1024;
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
            .field("entries", &inner.entries.len())
            .field("bytes", &inner.bytes)
            .finish()
    }
}

#[derive(Default)]
struct CacheInner {
    entries: VecDeque<Entry>,
    bytes: usize,
}

struct Entry {
    work_id: Option<String>,
    fingerprint: [u8; 32],
    immutable_bytes: Vec<u8>,
    prepared: SemanticallyVerifiedBlock,
    size: usize,
    expires_at: Instant,
}

impl PreparedCandidateCache {
    pub(super) fn lookup(
        &self,
        block: &Block,
        work_id: Option<&str>,
        network: &Network,
    ) -> Option<SemanticallyVerifiedBlock> {
        let immutable_bytes = immutable_candidate_bytes(block, network);
        let fingerprint = fingerprint(&immutable_bytes);
        let mut inner = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        inner.prune_expired();

        if let Some(work_id) = work_id {
            if let Some(entry) = inner
                .entries
                .iter()
                .find(|entry| entry.work_id.as_deref() == Some(work_id))
            {
                if entry.immutable_bytes == immutable_bytes {
                    metrics::counter!("mining.prepared_cache.hits").increment(1);
                    return Some(entry.prepared.clone());
                }

                metrics::counter!("mining.prepared_cache.mismatches").increment(1);
                return None;
            }
        }

        if let Some(entry) = inner.entries.iter().find(|entry| {
            entry.fingerprint == fingerprint && entry.immutable_bytes == immutable_bytes
        }) {
            metrics::counter!("mining.prepared_cache.hits").increment(1);
            return Some(entry.prepared.clone());
        }

        metrics::counter!("mining.prepared_cache.misses").increment(1);
        None
    }

    pub(super) fn insert(
        &self,
        block: &Block,
        work_id: Option<&str>,
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
        let existing_work_id = if work_id.is_none() {
            inner
                .entries
                .iter()
                .find(|entry| {
                    entry.fingerprint == fingerprint && entry.immutable_bytes == immutable_bytes
                })
                .and_then(|entry| entry.work_id.clone())
        } else {
            None
        };
        // Count the canonical candidate, derived verification inputs, and caller-supplied work ID.
        let size = immutable_bytes.len().saturating_mul(2).saturating_add(
            work_id
                .map(str::len)
                .or_else(|| existing_work_id.as_ref().map(String::len))
                .unwrap_or(0),
        );
        if size > MAX_BYTES {
            return;
        }
        let work_id = work_id.map(ToOwned::to_owned).or(existing_work_id);
        inner.remove_matching(work_id.as_deref(), fingerprint, &immutable_bytes);

        while inner.entries.len() >= MAX_ENTRIES || inner.bytes.saturating_add(size) > MAX_BYTES {
            if !inner.evict_oldest() {
                break;
            }
        }

        inner.bytes = inner.bytes.saturating_add(size);
        inner.entries.push_back(Entry {
            work_id,
            fingerprint,
            immutable_bytes,
            prepared,
            size,
            expires_at: Instant::now() + ENTRY_TTL,
        });
    }
}

impl CacheInner {
    fn prune_expired(&mut self) {
        let now = Instant::now();
        while self
            .entries
            .front()
            .is_some_and(|entry| entry.expires_at <= now)
        {
            self.evict_oldest();
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

    fn evict_oldest(&mut self) -> bool {
        let Some(entry) = self.entries.pop_front() else {
            return false;
        };
        self.bytes = self.bytes.saturating_sub(entry.size);
        metrics::counter!("mining.prepared_cache.evictions").increment(1);
        true
    }
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

    #[test]
    fn solved_header_fields_reuse_prepared_candidate() {
        let network = Network::Mainnet;
        let original = test_block();
        let prepared = SemanticallyVerifiedBlock::from(Arc::new(original.clone()));
        let cache = PreparedCandidateCache::default();
        cache.insert(&original, Some("work"), prepared, &network);

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
        cache.insert(&original, Some("work"), prepared, &network);

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
    fn inserting_a_reused_work_id_replaces_the_old_candidate() {
        let network = Network::Mainnet;
        let original = test_block();
        let mut replacement = original.clone();
        Arc::make_mut(&mut replacement.header).version ^= 1;
        let cache = PreparedCandidateCache::default();

        cache.insert(
            &original,
            Some("work"),
            SemanticallyVerifiedBlock::from(Arc::new(original.clone())),
            &network,
        );
        cache.insert(
            &replacement,
            Some("work"),
            SemanticallyVerifiedBlock::from(Arc::new(replacement.clone())),
            &network,
        );

        assert!(cache.lookup(&replacement, Some("work"), &network).is_some());
        assert!(cache.lookup(&original, Some("work"), &network).is_none());
        assert_eq!(
            cache
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entries
                .len(),
            1
        );
    }
}
