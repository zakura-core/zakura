//! Handler-scoped FIFO retention, reserving the eventual JSON cost before first use.

use super::{Payload, TransactionData};
use std::{
    collections::{HashMap, VecDeque},
    mem::size_of,
    sync::{Arc, Mutex},
};
use zakura_chain::{
    serialization::ZcashSerialize,
    transaction::{UnminedTx, UnminedTxId},
};

const MAX_BYTES: usize = 16 * 1024 * 1024;
const MAX_ENTRIES: usize = 4_096;

#[derive(Clone)]
pub(crate) struct TransactionDataCache(Arc<Mutex<Cache>>);

struct Entry {
    data: TransactionData,
    reserved_bytes: usize,
}

struct Cache {
    entries: HashMap<UnminedTxId, Entry>,
    order: VecDeque<UnminedTxId>,
    overhead: usize,
    reserved_bytes: usize,
    max_bytes: usize,
    max_entries: usize,
}

impl Default for TransactionDataCache {
    fn default() -> Self {
        Self::new(MAX_BYTES, MAX_ENTRIES)
    }
}

impl TransactionDataCache {
    fn new(max_bytes: usize, max_entries: usize) -> Self {
        let entries = HashMap::with_capacity(max_entries);
        let order = VecDeque::with_capacity(max_entries);
        // Conservative bucket/control-byte allowance plus the fixed FIFO and owner allocation.
        let overhead = entries.capacity() * 2 * (size_of::<(UnminedTxId, Entry)>() + 1)
            + order.capacity() * size_of::<UnminedTxId>()
            + size_of::<Cache>()
            + 2 * size_of::<usize>();
        Self(Arc::new(Mutex::new(Cache {
            entries,
            order,
            overhead,
            reserved_bytes: overhead,
            max_bytes,
            max_entries,
        })))
    }

    /// Shares exact payloads across template builds; witnessed IDs include authorizing data.
    /// Byte encoding is outside the lock, and JSON stays lazy until a response uses it.
    pub(crate) fn get(&self, tx: &UnminedTx) -> TransactionData {
        let id = tx.id();
        if let Some(data) = self
            .0
            .lock()
            .expect("cache holders only update in-memory entries")
            .entries
            .get(&id)
            .map(|entry| entry.data.clone())
        {
            return data;
        }
        let bytes = tx
            .transaction()
            .zcash_serialize_to_vec()
            .expect("writing to a Vec cannot fail");
        let reserved_bytes = bytes
            .capacity()
            .saturating_add(bytes.len().saturating_mul(2).saturating_add(2))
            .saturating_add(size_of::<Payload>() + 2 * size_of::<usize>());
        let data = TransactionData::from(bytes);
        let mut cache = self
            .0
            .lock()
            .expect("cache holders only update in-memory entries");
        if let Some(existing) = cache.entries.get(&id) {
            return existing.data.clone();
        }
        if cache.overhead.saturating_add(reserved_bytes) > cache.max_bytes {
            return data;
        }
        while !cache.entries.is_empty()
            && (cache.reserved_bytes.saturating_add(reserved_bytes) > cache.max_bytes
                || cache.entries.len() >= cache.max_entries)
        {
            let oldest = cache
                .order
                .pop_front()
                .expect("each retained entry has a FIFO key");
            let removed = cache
                .entries
                .remove(&oldest)
                .expect("FIFO keys have retained entries");
            cache.reserved_bytes -= removed.reserved_bytes;
        }
        if cache.reserved_bytes.saturating_add(reserved_bytes) <= cache.max_bytes
            && cache.entries.len() < cache.max_entries
        {
            cache.reserved_bytes += reserved_bytes;
            cache.order.push_back(id);
            cache.entries.insert(
                id,
                Entry {
                    data: data.clone(),
                    reserved_bytes,
                },
            );
        }
        data
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::{
        block::Height,
        parameters::NetworkUpgrade,
        transaction::{self, LockTime, Transaction},
        transparent::{Input, OutPoint, Output, Script},
    };

    fn transaction(index: u8, signature: u8) -> UnminedTx {
        Transaction::V6 {
            network_upgrade: NetworkUpgrade::Nu7,
            lock_time: LockTime::unlocked(),
            expiry_height: Height(100),
            inputs: vec![Input::PrevOut {
                outpoint: OutPoint {
                    hash: transaction::Hash([index; 32]),
                    index: 0,
                },
                unlock_script: Script::new(&[signature]),
                sequence: 0,
            }],
            outputs: vec![Output::new(1u64.try_into().unwrap(), Script::new(&[]))],
            sapling_shielded_data: None,
            orchard_shielded_data: None,
            ironwood_shielded_data: None,
        }
        .into()
    }

    #[test]
    fn separate_builds_share_lazy_json_and_full_authorizing_identity() {
        let cache = TransactionDataCache::default();
        let first = transaction(1, 0);
        let second = transaction(1, 1);
        assert_eq!(first.id().mined_id(), second.id().mined_id());
        assert_ne!(first.id(), second.id());
        let original = cache.get(&first);
        assert!(original.0.json.get().is_none());
        original.json();
        let reused = cache.get(&first);
        assert!(Arc::ptr_eq(&original.0, &reused.0));
        assert!(!Arc::ptr_eq(&original.0, &cache.get(&second).0));
    }

    #[test]
    fn eviction_releases_cache_ownership_without_invalidating_readers() {
        let cache = TransactionDataCache::new(MAX_BYTES, 1);
        let original = cache.get(&transaction(1, 0));
        let weak = Arc::downgrade(&original.0);
        original.json();
        cache.get(&transaction(2, 0));
        assert!(weak.upgrade().is_some());
        drop(original);
        assert!(weak.upgrade().is_none());
        assert!(cache.0.lock().unwrap().reserved_bytes <= MAX_BYTES);
    }

    #[test]
    fn reservation_covers_first_use_json_and_oversized_entries_bypass_retention() {
        let cache = TransactionDataCache::new(MAX_BYTES, 1);
        let data = cache.get(&transaction(1, 0));
        let before = cache.0.lock().unwrap().reserved_bytes;
        data.json();
        assert_eq!(cache.0.lock().unwrap().reserved_bytes, before);
        let cache = TransactionDataCache::new(0, 1);
        cache.get(&transaction(1, 0)).json();
        assert!(cache.0.lock().unwrap().entries.is_empty());
    }
}
