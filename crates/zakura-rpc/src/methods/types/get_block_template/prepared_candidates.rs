//! Bounded work-ID lookup for reconstructing compact mined-block submissions.
//!
//! Entries retain block content only. Resolving a work ID does not establish validity or relay
//! authorization; the reconstructed block still goes through the consensus verifier.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    mem::size_of,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use zakura_chain::{
    block::{Block, Header},
    parameters::Network,
};
use zakura_consensus::PreparedCandidateSource;

const SERVER_MAX_ENTRIES: usize = 24;
const SERVER_MAX_WORK_IDS: usize = 3_072;
const SERVER_MAX_BYTES: usize = 48 * 1024 * 1024;
const PROPOSAL_MAX_ENTRIES: usize = 8;
const PROPOSAL_MAX_WORK_IDS: usize = 1_024;
const PROPOSAL_MAX_BYTES: usize = 16 * 1024 * 1024;
const ENTRY_TTL: Duration = Duration::from_secs(10 * 60);

type EntryId = u64;

/// Resolves compact submissions using canonical blocks and independently bounded work-ID aliases.
#[derive(Clone, Default)]
pub(crate) struct PreparedCandidateResolver(Arc<Mutex<CacheInner>>);

impl std::fmt::Debug for PreparedCandidateResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.lock();
        f.debug_struct("PreparedCandidateResolver")
            .field("server_entries", &inner.server.entries.len())
            .field("server_work_ids", &inner.server.work_ids.len())
            .field("server_bytes", &inner.server.bytes)
            .field("proposal_entries", &inner.proposals.entries.len())
            .field("proposal_work_ids", &inner.proposals.work_ids.len())
            .field("proposal_bytes", &inner.proposals.bytes)
            .finish()
    }
}

/// Why a compact submission could not be reconstructed.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum ResolvePreparedCandidateError {
    /// The cache no longer contains the supplied `workid`.
    #[error("the prepared candidate is no longer available")]
    StaleWork,

    /// The solved header changed a field that compact submission must preserve.
    #[error("the solved header does not match the prepared candidate")]
    CandidateMismatch,
}

struct CacheInner {
    server: Partition,
    proposals: Partition,
}

impl Default for CacheInner {
    fn default() -> Self {
        Self {
            server: Partition::new(PartitionLimits {
                max_entries: SERVER_MAX_ENTRIES,
                max_work_ids: SERVER_MAX_WORK_IDS,
                max_bytes: SERVER_MAX_BYTES,
            }),
            proposals: Partition::new(PartitionLimits {
                max_entries: PROPOSAL_MAX_ENTRIES,
                max_work_ids: PROPOSAL_MAX_WORK_IDS,
                max_bytes: PROPOSAL_MAX_BYTES,
            }),
        }
    }
}

#[derive(Clone, Copy)]
struct PartitionLimits {
    max_entries: usize,
    max_work_ids: usize,
    max_bytes: usize,
}

struct Partition {
    limits: PartitionLimits,
    entries: VecDeque<Entry>,
    work_ids: HashMap<String, WorkIdAlias>,
    work_id_order: VecDeque<String>,
    bytes: usize,
    next_entry_id: EntryId,
}

struct Entry {
    id: EntryId,
    block: Arc<Block>,
    size: usize,
    expires_at: Instant,
}

struct WorkIdAlias {
    entry_id: EntryId,
    size: usize,
    expires_at: Instant,
}

impl PreparedCandidateResolver {
    fn lock(&self) -> MutexGuard<'_, CacheInner> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Retains a candidate and its work ID without deriving consensus verification state.
    ///
    /// Equivalent candidates share one block within each source. An existing alias cannot be
    /// rebound, except that a server template takes precedence over a client proposal. Comparing
    /// the bounded candidate set directly avoids serializing or retaining another block copy.
    pub(crate) fn insert(
        &self,
        block: Arc<Block>,
        work_id: &str,
        source: PreparedCandidateSource,
        _network: &Network,
    ) {
        let size = candidate_size(&block);
        let alias_size = work_id_alias_size(work_id);
        let now = Instant::now();
        let mut inner = self.lock();
        inner.prune_expired(now);

        let limits = inner.partition(source).limits;
        if limits.max_entries == 0
            || limits.max_work_ids == 0
            || size.saturating_add(alias_size) > limits.max_bytes
        {
            return;
        }

        let reclaim_proposal = if let Some((existing_source, _)) = inner.find_work_id(work_id) {
            let server_reclaims_proposal = source == PreparedCandidateSource::ServerTemplate
                && existing_source == PreparedCandidateSource::ClientProposal;
            if !server_reclaims_proposal {
                return;
            }
            true
        } else {
            false
        };

        let existing_entry = inner
            .partition(source)
            .entries
            .iter()
            .find(|entry| candidates_match(&entry.block, &block))
            .map(|entry| (entry.id, entry.size));
        // Equivalent blocks can have different allocation capacities. Check the retained block
        // before removing a proposal alias that a successful server insertion will replace.
        if existing_entry
            .is_some_and(|(_, size)| size.saturating_add(alias_size) > limits.max_bytes)
        {
            return;
        }
        if reclaim_proposal {
            inner.proposals.remove_work_id(work_id);
        }

        let partition = inner.partition(source);
        let entry_id = if let Some((entry_id, _)) = existing_entry {
            partition.touch_candidate(entry_id, now);
            entry_id
        } else {
            let Some(entry_id) = partition.insert_candidate(block, size, alias_size, now) else {
                return;
            };
            entry_id
        };
        partition.insert_work_id(work_id, entry_id, now);
    }

    /// Reconstructs a block while allowing changes only to time, nonce, and Equihash solution.
    pub(crate) fn resolve(
        &self,
        work_id: &str,
        solved_header: Header,
    ) -> Result<Arc<Block>, ResolvePreparedCandidateError> {
        let mut inner = self.lock();
        inner.prune_expired(Instant::now());
        let (_, entry) = inner
            .find_work_id(work_id)
            .ok_or(ResolvePreparedCandidateError::StaleWork)?;
        if !preserved_header_fields_match(&entry.block.header, &solved_header) {
            return Err(ResolvePreparedCandidateError::CandidateMismatch);
        }
        Ok(Arc::new(Block {
            header: Arc::new(solved_header),
            transactions: entry.block.transactions.clone(),
        }))
    }
}

impl CacheInner {
    fn prune_expired(&mut self, now: Instant) {
        self.server.prune_expired(now);
        self.proposals.prune_expired(now);
    }

    fn partition(&mut self, source: PreparedCandidateSource) -> &mut Partition {
        match source {
            PreparedCandidateSource::ServerTemplate => &mut self.server,
            PreparedCandidateSource::ClientProposal => &mut self.proposals,
        }
    }

    fn find_work_id(&self, work_id: &str) -> Option<(PreparedCandidateSource, &Entry)> {
        self.server
            .entry_for_work_id(work_id)
            .map(|entry| (PreparedCandidateSource::ServerTemplate, entry))
            .or_else(|| {
                self.proposals
                    .entry_for_work_id(work_id)
                    .map(|entry| (PreparedCandidateSource::ClientProposal, entry))
            })
    }
}

impl Partition {
    fn new(limits: PartitionLimits) -> Self {
        Self {
            limits,
            entries: VecDeque::new(),
            work_ids: HashMap::new(),
            work_id_order: VecDeque::new(),
            bytes: 0,
            next_entry_id: 0,
        }
    }

    fn entry_for_work_id(&self, work_id: &str) -> Option<&Entry> {
        let entry_id = self.work_ids.get(work_id)?.entry_id;
        self.entries.iter().find(|entry| entry.id == entry_id)
    }

    fn prune_expired(&mut self, now: Instant) {
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
        block: Arc<Block>,
        size: usize,
        alias_size: usize,
        now: Instant,
    ) -> Option<EntryId> {
        // Reserve the alias together with the block so an insertion cannot strand a new block
        // with no room for the work ID that makes it usable.
        let required = size.saturating_add(alias_size);
        while self.entries.len() >= self.limits.max_entries
            || self.bytes.saturating_add(required) > self.limits.max_bytes
        {
            let entry_id = self.entries.front()?.id;
            self.remove_entry(entry_id);
        }
        let id = self.next_entry_id;
        self.next_entry_id = self.next_entry_id.wrapping_add(1);
        self.bytes = self.bytes.saturating_add(size);
        self.entries.push_back(Entry {
            id,
            block,
            size,
            expires_at: now + ENTRY_TTL,
        });
        Some(id)
    }

    fn touch_candidate(&mut self, entry_id: EntryId, now: Instant) {
        let index = self
            .entries
            .iter()
            .position(|entry| entry.id == entry_id)
            .expect("the matching candidate is still in this locked partition");
        let mut entry = self
            .entries
            .remove(index)
            .expect("the index came from the same candidate deque");
        entry.expires_at = now + ENTRY_TTL;
        self.entries.push_back(entry);
    }

    fn insert_work_id(&mut self, work_id: &str, entry_id: EntryId, now: Instant) {
        let size = work_id_alias_size(work_id);
        while self.work_ids.len() >= self.limits.max_work_ids
            || self.bytes.saturating_add(size) > self.limits.max_bytes
        {
            if let Some(oldest) = self.work_id_order.front().cloned() {
                self.remove_work_id(&oldest);
            } else if let Some(other_id) = self
                .entries
                .iter()
                .find(|entry| entry.id != entry_id)
                .map(|entry| entry.id)
            {
                self.remove_entry(other_id);
            } else {
                // The caller checks that this candidate and alias fit together.
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
                expires_at: now + ENTRY_TTL,
            },
        );
    }

    fn remove_entry(&mut self, entry_id: EntryId) {
        let Some(index) = self.entries.iter().position(|entry| entry.id == entry_id) else {
            return;
        };
        let entry = self
            .entries
            .remove(index)
            .expect("the index came from the same candidate deque");
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
    }

    fn remove_work_id(&mut self, work_id: &str) {
        let Some(alias) = self.work_ids.remove(work_id) else {
            return;
        };
        self.bytes = self.bytes.saturating_sub(alias.size);
        if let Some(index) = self
            .work_id_order
            .iter()
            .position(|candidate| candidate == work_id)
        {
            self.work_id_order.remove(index);
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

fn candidate_size(block: &Block) -> usize {
    size_of::<Entry>()
        .saturating_add(usize::try_from(block.attributed_memory_size_bytes()).unwrap_or(usize::MAX))
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
        block::Hash,
        serialization::ZcashDeserialize,
        work::{difficulty::INVALID_COMPACT_DIFFICULTY, equihash::Solution},
    };
    use PreparedCandidateSource::{ClientProposal, ServerTemplate};
    use ResolvePreparedCandidateError::{CandidateMismatch, StaleWork};

    fn test_block() -> Arc<Block> {
        Arc::new(
            Block::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES[..])
                .expect("the genesis test vector is valid"),
        )
    }

    fn distinct_block(index: usize) -> Arc<Block> {
        let mut block = test_block();
        Arc::make_mut(&mut Arc::make_mut(&mut block).header).previous_block_hash =
            Hash([u8::try_from(index).expect("test indices fit in a byte"); 32]);
        block
    }

    fn insert(
        cache: &PreparedCandidateResolver,
        block: &Arc<Block>,
        work_id: &str,
        source: PreparedCandidateSource,
    ) {
        cache.insert(block.clone(), work_id, source, &Network::Mainnet);
    }

    fn with_limits(limits: PartitionLimits) -> PreparedCandidateResolver {
        PreparedCandidateResolver(Arc::new(Mutex::new(CacheInner {
            server: Partition::new(limits),
            proposals: Partition::new(limits),
        })))
    }

    fn assert_accounting(partition: &Partition) {
        assert_eq!(
            partition.bytes,
            partition
                .entries
                .iter()
                .map(|entry| entry.size)
                .sum::<usize>()
                + partition
                    .work_ids
                    .values()
                    .map(|alias| alias.size)
                    .sum::<usize>()
        );
        assert!(partition.bytes <= partition.limits.max_bytes);
        assert!(partition.entries.len() <= partition.limits.max_entries);
        assert!(partition.work_ids.len() <= partition.limits.max_work_ids);
        assert_eq!(partition.work_id_order.len(), partition.work_ids.len());
        for work_id in &partition.work_id_order {
            assert!(partition.entry_for_work_id(work_id).is_some());
        }
    }

    #[test]
    fn solved_header_fields_reconstruct_original_transactions() {
        let block = test_block();
        let cache = PreparedCandidateResolver::default();
        insert(&cache, &block, "work", ServerTemplate);
        let mut header = *block.header;
        header.time += chrono::Duration::seconds(1);
        header.nonce = [9; 32].into();
        header.solution = Solution::for_proposal_for_network(&Network::Mainnet);
        let solved = cache
            .resolve("work", header)
            .expect("mutable fields can change");
        assert_eq!(*solved.header, header);
        assert_eq!(solved.transactions, block.transactions);
        assert!(Arc::ptr_eq(&solved.transactions[0], &block.transactions[0]));
        assert_eq!(cache.resolve("missing", header), Err(StaleWork));
    }

    #[test]
    fn every_preserved_header_field_rejects_mismatch() {
        let block = test_block();
        let cache = PreparedCandidateResolver::default();
        insert(&cache, &block, "work", ServerTemplate);
        let mut changed = vec![*block.header; 5];
        changed[0].version ^= 1;
        changed[1].previous_block_hash = Hash([1; 32]);
        changed[2].merkle_root = Default::default();
        changed[3].commitment_bytes[0] ^= 1;
        changed[4].difficulty_threshold = INVALID_COMPACT_DIFFICULTY;
        for header in changed {
            assert_eq!(cache.resolve("work", header), Err(CandidateMismatch));
        }
        assert!(cache.resolve("work", *block.header).is_ok());
    }

    #[test]
    fn equivalent_candidates_share_one_canonical_block() {
        let original = test_block();
        let mut changed = original.clone();
        let header = Arc::make_mut(&mut Arc::make_mut(&mut changed).header);
        header.time += chrono::Duration::seconds(1);
        header.nonce = [7; 32].into();
        header.solution = Solution::for_proposal_for_network(&Network::Mainnet);
        let cache = PreparedCandidateResolver::default();
        insert(&cache, &original, "first", ServerTemplate);
        insert(&cache, &changed, "second", ServerTemplate);
        assert!(cache.resolve("first", *original.header).is_ok());
        assert!(cache.resolve("second", *changed.header).is_ok());
        let inner = cache.lock();
        assert_eq!(inner.server.entries.len(), 1);
        assert_eq!(inner.server.work_ids.len(), 2);
        assert!(Arc::ptr_eq(&inner.server.entries[0].block, &original));
        assert_accounting(&inner.server);
    }

    #[test]
    fn identical_headers_with_different_transactions_are_distinct_candidates() {
        let original = test_block();
        let mut changed = original.clone();
        Arc::make_mut(&mut changed)
            .transactions
            .push(original.transactions[0].clone());
        let cache = PreparedCandidateResolver::default();
        insert(&cache, &original, "first", ServerTemplate);
        insert(&cache, &changed, "second", ServerTemplate);
        assert_eq!(cache.lock().server.entries.len(), 2);
        assert_eq!(
            cache
                .resolve("first", *original.header)
                .unwrap()
                .transactions,
            original.transactions
        );
        assert_eq!(
            cache
                .resolve("second", *changed.header)
                .unwrap()
                .transactions,
            changed.transactions
        );
    }

    #[test]
    fn a_reused_work_id_cannot_replace_its_same_source_candidate() {
        for source in [ServerTemplate, ClientProposal] {
            let cache = PreparedCandidateResolver::default();
            let original = distinct_block(1);
            let replacement = distinct_block(2);
            insert(&cache, &original, "shared", source);
            insert(&cache, &replacement, "shared", source);
            assert!(cache.resolve("shared", *original.header).is_ok());
            assert_eq!(
                cache.resolve("shared", *replacement.header),
                Err(CandidateMismatch)
            );
            let mut inner = cache.lock();
            assert_eq!(inner.partition(source).entries.len(), 1);
            assert_accounting(inner.partition(source));
        }
    }

    #[test]
    fn server_alias_wins_collisions_in_either_insertion_order() {
        for server_first in [true, false] {
            for same_candidate in [true, false] {
                let cache = PreparedCandidateResolver::default();
                let server = distinct_block(1);
                let proposal = distinct_block(if same_candidate { 1 } else { 2 });
                let mut candidates = [(ServerTemplate, &server), (ClientProposal, &proposal)];
                if !server_first {
                    candidates.reverse();
                }
                for (source, block) in candidates {
                    insert(&cache, block, "shared", source);
                }
                assert!(cache.resolve("shared", *server.header).is_ok());
                if !same_candidate {
                    assert_eq!(
                        cache.resolve("shared", *proposal.header),
                        Err(CandidateMismatch)
                    );
                }
                let inner = cache.lock();
                assert!(inner.server.work_ids.contains_key("shared"));
                assert!(!inner.proposals.work_ids.contains_key("shared"));
                assert_accounting(&inner.server);
                assert_accounting(&inner.proposals);
            }
        }
    }

    #[test]
    fn same_content_has_independently_retained_source_aliases() {
        let cache = PreparedCandidateResolver::default();
        let block = test_block();
        insert(&cache, &block, "server", ServerTemplate);
        insert(&cache, &block, "proposal", ClientProposal);
        assert!(cache.resolve("server", *block.header).is_ok());
        assert!(cache.resolve("proposal", *block.header).is_ok());
        let inner = cache.lock();
        assert_eq!(inner.server.entries.len(), 1);
        assert_eq!(inner.proposals.entries.len(), 1);
        assert_accounting(&inner.server);
        assert_accounting(&inner.proposals);
    }

    #[test]
    fn alias_caps_evict_only_the_same_sources_oldest_alias() {
        let block = test_block();
        for (source, other, cap) in [
            (ServerTemplate, ClientProposal, SERVER_MAX_WORK_IDS),
            (ClientProposal, ServerTemplate, PROPOSAL_MAX_WORK_IDS),
        ] {
            let cache = PreparedCandidateResolver::default();
            insert(&cache, &block, "other", other);
            for index in 0..=cap {
                insert(&cache, &block, &format!("work-{index}"), source);
            }
            assert_eq!(cache.resolve("work-0", *block.header), Err(StaleWork));
            assert!(cache.resolve("work-1", *block.header).is_ok());
            assert!(cache.resolve(&format!("work-{cap}"), *block.header).is_ok());
            assert!(cache.resolve("other", *block.header).is_ok());
            let mut inner = cache.lock();
            let partition = inner.partition(source);
            assert_eq!(partition.entries.len(), 1);
            assert_eq!(partition.work_ids.len(), cap);
            assert_accounting(&inner.server);
            assert_accounting(&inner.proposals);
        }
    }

    #[test]
    fn candidate_caps_evict_only_the_same_sources_oldest_candidate_and_aliases() {
        for (source, other, cap) in [
            (ServerTemplate, ClientProposal, SERVER_MAX_ENTRIES),
            (ClientProposal, ServerTemplate, PROPOSAL_MAX_ENTRIES),
        ] {
            let cache = PreparedCandidateResolver::default();
            let other_block = distinct_block(100);
            insert(&cache, &other_block, "other", other);
            for index in 0..=cap {
                insert(
                    &cache,
                    &distinct_block(index),
                    &format!("work-{index}"),
                    source,
                );
                if index == 0 {
                    insert(&cache, &distinct_block(index), "old-alias", source);
                }
            }
            assert_eq!(
                cache.resolve("work-0", *distinct_block(0).header),
                Err(StaleWork)
            );
            assert_eq!(
                cache.resolve("old-alias", *distinct_block(0).header),
                Err(StaleWork)
            );
            assert!(cache.resolve("work-1", *distinct_block(1).header).is_ok());
            assert!(cache.resolve("other", *other_block.header).is_ok());
            let mut inner = cache.lock();
            assert_eq!(inner.partition(source).entries.len(), cap);
            assert_accounting(&inner.server);
            assert_accounting(&inner.proposals);
        }
    }

    #[test]
    fn resolved_body_outlives_rpc_candidate_eviction() {
        let cache = with_limits(PartitionLimits {
            max_entries: 1,
            max_work_ids: 2,
            max_bytes: usize::MAX,
        });
        let original = distinct_block(1);
        insert(&cache, &original, "original", ServerTemplate);
        let resolved = cache
            .resolve("original", *original.header)
            .expect("the original candidate resolves before eviction");
        insert(&cache, &distinct_block(2), "replacement", ServerTemplate);
        assert_eq!(cache.resolve("original", *original.header), Err(StaleWork));
        drop(cache);
        assert_eq!(resolved, original);
        assert!(Arc::ptr_eq(
            &resolved.transactions[0],
            &original.transactions[0]
        ));
    }

    #[test]
    fn fresh_alias_refreshes_only_its_canonical_candidate() {
        let cache = with_limits(PartitionLimits {
            max_entries: 2,
            max_work_ids: 4,
            max_bytes: SERVER_MAX_BYTES,
        });
        let first = distinct_block(1);
        let second = distinct_block(2);
        let third = distinct_block(3);
        insert(&cache, &first, "first", ServerTemplate);
        insert(&cache, &second, "second", ServerTemplate);
        let old_expiry = Instant::now() + ENTRY_TTL / 2;
        {
            let mut inner = cache.lock();
            inner.server.entries[0].expires_at = old_expiry;
            inner.server.work_ids.get_mut("first").unwrap().expires_at = old_expiry;
        }
        insert(&cache, &first, "fresh", ServerTemplate);
        insert(&cache, &third, "third", ServerTemplate);
        assert!(cache.resolve("first", *first.header).is_ok());
        assert!(cache.resolve("fresh", *first.header).is_ok());
        assert_eq!(cache.resolve("second", *second.header), Err(StaleWork));
        let inner = cache.lock();
        assert!(inner.server.entry_for_work_id("first").unwrap().expires_at > old_expiry);
        assert_eq!(inner.server.work_ids["first"].expires_at, old_expiry);
        assert_accounting(&inner.server);
    }

    #[test]
    fn candidate_expiry_removes_all_its_aliases_and_bytes() {
        for source in [ServerTemplate, ClientProposal] {
            let cache = PreparedCandidateResolver::default();
            let block = test_block();
            insert(&cache, &block, "first", source);
            insert(&cache, &block, "second", source);
            cache.lock().partition(source).entries[0].expires_at = Instant::now();
            assert_eq!(cache.resolve("first", *block.header), Err(StaleWork));
            assert_eq!(cache.resolve("second", *block.header), Err(StaleWork));
            let mut inner = cache.lock();
            let partition = inner.partition(source);
            assert!(partition.entries.is_empty());
            assert!(partition.work_ids.is_empty());
            assert_eq!(partition.bytes, 0);
            assert_accounting(partition);
        }
    }

    #[test]
    fn alias_expiry_does_not_expire_other_aliases_or_renew_on_lookup() {
        let cache = PreparedCandidateResolver::default();
        let block = test_block();
        insert(&cache, &block, "old", ServerTemplate);
        insert(&cache, &block, "fresh", ServerTemplate);
        let (candidate_expiry, fresh_expiry) = {
            let mut inner = cache.lock();
            inner.server.work_ids.get_mut("old").unwrap().expires_at = Instant::now();
            (
                inner.server.entries[0].expires_at,
                inner.server.work_ids["fresh"].expires_at,
            )
        };
        assert_eq!(cache.resolve("old", *block.header), Err(StaleWork));
        assert!(cache.resolve("fresh", *block.header).is_ok());
        let inner = cache.lock();
        assert_eq!(inner.server.entries[0].expires_at, candidate_expiry);
        assert_eq!(inner.server.work_ids["fresh"].expires_at, fresh_expiry);
        assert_eq!(inner.server.work_ids.len(), 1);
        assert_accounting(&inner.server);
    }

    #[test]
    fn expiry_is_ten_minutes_and_expired_aliases_can_be_reused() {
        assert_eq!(ENTRY_TTL, Duration::from_secs(600));
        let cache = PreparedCandidateResolver::default();
        let first = distinct_block(1);
        let second = distinct_block(2);
        let before = Instant::now();
        insert(&cache, &first, "work", ServerTemplate);
        let after = Instant::now();
        {
            let mut inner = cache.lock();
            let expiry = inner.server.work_ids["work"].expires_at;
            assert!(expiry >= before + ENTRY_TTL && expiry <= after + ENTRY_TTL);
            inner.server.work_ids.get_mut("work").unwrap().expires_at = before;
        }
        insert(&cache, &second, "work", ServerTemplate);
        assert!(cache.resolve("work", *second.header).is_ok());
        assert_eq!(cache.resolve("work", *first.header), Err(CandidateMismatch));
        assert_accounting(&cache.lock().server);
    }

    #[test]
    fn byte_limits_reject_oversized_blocks_and_aliases_without_eviction() {
        let block = test_block();
        let budget = candidate_size(&block) + work_id_alias_size("work");
        let cache = with_limits(PartitionLimits {
            max_entries: 4,
            max_work_ids: 4,
            max_bytes: budget,
        });
        for source in [ServerTemplate, ClientProposal] {
            let work_id = if source == ServerTemplate {
                "work"
            } else {
                "prop"
            };
            insert(&cache, &block, work_id, source);
            insert(&cache, &block, &"x".repeat(budget), source);
            let mut oversized = distinct_block(1);
            Arc::make_mut(&mut oversized).transactions.reserve(budget);
            insert(&cache, &oversized, "huge", source);
            assert!(cache.resolve(work_id, *block.header).is_ok());
            assert_eq!(cache.resolve("huge", *oversized.header), Err(StaleWork));
        }
        let inner = cache.lock();
        assert_eq!(inner.server.bytes, budget);
        assert_eq!(inner.proposals.bytes, budget);
        assert_accounting(&inner.server);
        assert_accounting(&inner.proposals);
    }

    #[test]
    fn candidate_byte_pressure_evicts_the_oldest_candidate() {
        let first = distinct_block(1);
        let budget = 2 * (candidate_size(&first) + work_id_alias_size("work-1"));
        let cache = with_limits(PartitionLimits {
            max_entries: 8,
            max_work_ids: 8,
            max_bytes: budget,
        });
        for index in 1..=3 {
            insert(
                &cache,
                &distinct_block(index),
                &format!("work-{index}"),
                ServerTemplate,
            );
        }
        assert_eq!(cache.resolve("work-1", *first.header), Err(StaleWork));
        assert!(cache.resolve("work-2", *distinct_block(2).header).is_ok());
        assert!(cache.resolve("work-3", *distinct_block(3).header).is_ok());
        let inner = cache.lock();
        assert_eq!(inner.server.entries.len(), 2);
        assert_accounting(&inner.server);
    }

    #[test]
    fn alias_byte_pressure_evicts_old_aliases_without_duplicating_the_block() {
        let block = test_block();
        let budget = candidate_size(&block) + 2 * work_id_alias_size("work-1");
        let cache = with_limits(PartitionLimits {
            max_entries: 8,
            max_work_ids: 8,
            max_bytes: budget,
        });
        for index in 1..=3 {
            insert(&cache, &block, &format!("work-{index}"), ServerTemplate);
        }
        assert_eq!(cache.resolve("work-1", *block.header), Err(StaleWork));
        assert!(cache.resolve("work-2", *block.header).is_ok());
        assert!(cache.resolve("work-3", *block.header).is_ok());
        let inner = cache.lock();
        assert_eq!(inner.server.entries.len(), 1);
        assert_eq!(inner.server.bytes, budget);
        assert_accounting(&inner.server);
    }

    #[test]
    fn byte_accounting_counts_block_capacity_and_both_owned_alias_strings() {
        let block = test_block();
        let mut reserved = block.clone();
        Arc::make_mut(&mut reserved).transactions.reserve(100);
        let extra_capacity = reserved.transactions.capacity() - block.transactions.capacity();
        assert_eq!(
            candidate_size(&reserved) - candidate_size(&block),
            extra_capacity * size_of::<Arc<zakura_chain::transaction::Transaction>>()
        );
        assert_eq!(
            work_id_alias_size("longer") - work_id_alias_size("x"),
            2 * ("longer".len() - "x".len())
        );
    }

    #[test]
    fn production_limits_bound_both_sources_independently() {
        let cache = PreparedCandidateResolver::default();
        let inner = cache.lock();
        assert_eq!(inner.server.limits.max_entries, 24);
        assert_eq!(inner.server.limits.max_work_ids, 3_072);
        assert_eq!(inner.server.limits.max_bytes, 48 * 1024 * 1024);
        assert_eq!(inner.proposals.limits.max_entries, 8);
        assert_eq!(inner.proposals.limits.max_work_ids, 1_024);
        assert_eq!(inner.proposals.limits.max_bytes, 16 * 1024 * 1024);
    }

    #[test]
    fn oversized_alias_for_a_retained_candidate_does_not_remove_a_proposal_alias() {
        let compact = test_block();
        let mut reserved = compact.clone();
        Arc::make_mut(&mut reserved).transactions.reserve(100);
        let budget = candidate_size(&reserved) + work_id_alias_size("a");
        let cache = with_limits(PartitionLimits {
            max_entries: 4,
            max_work_ids: 4,
            max_bytes: budget,
        });
        insert(&cache, &reserved, "a", ServerTemplate);
        let proposal = distinct_block(1);
        let shared = "shared";
        insert(&cache, &proposal, shared, ClientProposal);
        insert(&cache, &compact, shared, ServerTemplate);
        assert!(cache.resolve("a", *compact.header).is_ok());
        assert!(cache.resolve(shared, *proposal.header).is_ok());
        let inner = cache.lock();
        assert!(!inner.server.work_ids.contains_key(shared));
        assert_accounting(&inner.server);
        assert_accounting(&inner.proposals);
    }

    #[test]
    fn fresh_alias_can_evict_unreferenced_candidates_under_byte_pressure() {
        let first = distinct_block(1);
        let second = distinct_block(2);
        let budget = 2 * (candidate_size(&first) + work_id_alias_size("a"));
        let cache = with_limits(PartitionLimits {
            max_entries: 4,
            max_work_ids: 4,
            max_bytes: budget,
        });
        insert(&cache, &first, "a", ServerTemplate);
        insert(&cache, &second, "b", ServerTemplate);
        {
            let mut inner = cache.lock();
            inner.server.remove_work_id("a");
            inner.server.remove_work_id("b");
        }
        let large_alias = "x".repeat(work_id_alias_size("a") + 1);
        insert(&cache, &second, &large_alias, ServerTemplate);
        assert!(cache.resolve(&large_alias, *second.header).is_ok());
        let inner = cache.lock();
        assert_eq!(inner.server.entries.len(), 1);
        assert_accounting(&inner.server);
    }

    #[test]
    fn clones_share_aliases_and_debug_reports_only_cache_counts() {
        let cache = PreparedCandidateResolver::default();
        let cloned = cache.clone();
        let block = test_block();
        insert(&cache, &block, "secret-work-id", ServerTemplate);
        assert!(cloned.resolve("secret-work-id", *block.header).is_ok());
        let debug = format!("{cache:?}");
        assert!(debug.contains("server_entries: 1"));
        assert!(!debug.contains("secret-work-id"));
    }
}
