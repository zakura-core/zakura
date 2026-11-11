//! Shared immutable mining payloads and bounded retention across template builds.

use std::{
    collections::{HashMap, VecDeque},
    mem::size_of,
    sync::{Arc, Mutex},
};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use zakura_chain::{
    serialization::ZcashSerialize,
    transaction::{SerializedTransaction, UnminedTx, UnminedTxId},
};

const MAX_BYTES: usize = 16 * 1024 * 1024;
const MAX_ENTRIES: usize = 4_096;

/// Only immutable transaction bytes and their lowercase hex, never template metadata.
#[derive(Clone)]
pub(crate) struct TransactionData(Arc<Payload>);

impl std::fmt::Debug for TransactionData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.bytes.fmt(f)
    }
}

impl PartialEq for TransactionData {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for TransactionData {}

#[derive(Debug, Eq, PartialEq)]
struct Payload {
    bytes: SerializedTransaction,
    hex: String,
    allocation_bytes: usize,
}

impl TransactionData {
    pub(crate) fn bytes(&self) -> &SerializedTransaction {
        &self.0.bytes
    }

    fn retained_bytes(&self) -> usize {
        self.0.allocation_bytes + size_of::<Payload>() + 2 * size_of::<usize>()
    }
}

impl From<Vec<u8>> for TransactionData {
    fn from(bytes: Vec<u8>) -> Self {
        let hex = crate::methods::hex_data::encode(&bytes);
        let allocation_bytes = bytes.capacity().saturating_add(hex.capacity());
        Self(Arc::new(Payload {
            bytes: bytes.into(),
            hex,
            allocation_bytes,
        }))
    }
}

impl From<SerializedTransaction> for TransactionData {
    fn from(bytes: SerializedTransaction) -> Self {
        // SerializedTransaction hides its Vec capacity. Own a known allocation so retention
        // accounting includes spare capacity as well as the bytes visible on the wire.
        bytes.as_ref().to_vec().into()
    }
}

impl AsRef<[u8]> for TransactionData {
    fn as_ref(&self) -> &[u8] {
        self.0.bytes.as_ref()
    }
}

impl Serialize for TransactionData {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.hex)
    }
}

impl<'de> Deserialize<'de> for TransactionData {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        crate::methods::hex_data::deserialize::<D, Vec<u8>>(deserializer).map(Self::from)
    }
}

/// FIFO retention shared by clones of one mining RPC handler.
#[derive(Clone)]
pub(crate) struct TransactionDataCache(Arc<Mutex<Cache>>);

impl std::fmt::Debug for TransactionDataCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cache = self.0.lock().map_err(|_| std::fmt::Error)?;
        f.debug_struct("TransactionDataCache")
            .field("entries", &cache.entries.len())
            .field("accounted_bytes", &cache.bytes)
            .finish()
    }
}

struct Cache {
    entries: HashMap<UnminedTxId, TransactionData>,
    order: VecDeque<UnminedTxId>,
    bytes: usize,
    allocation_overhead: usize,
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
        // Reserve conservatively for hash buckets, control bytes, and the FIFO allocation.
        // Allocator bookkeeping is implementation-dependent and is measured separately.
        let bytes = entries.capacity() * 2 * (size_of::<(UnminedTxId, TransactionData)>() + 1)
            + order.capacity() * size_of::<UnminedTxId>()
            + size_of::<Cache>()
            + 2 * size_of::<usize>();
        Self(Arc::new(Mutex::new(Cache {
            entries,
            order,
            bytes,
            allocation_overhead: bytes,
            max_bytes,
            max_entries,
        })))
    }

    /// Reuses the full unmined identity, including authorizing bytes for v5/v6 transactions.
    /// Encoding stays outside the lock; concurrent misses may do duplicate cold work.
    pub(crate) fn get(&self, tx: &UnminedTx) -> TransactionData {
        let id = tx.id();
        if let Some(data) = self
            .0
            .lock()
            .expect("transaction payload cache lock is not poisoned")
            .entries
            .get(&id)
            .cloned()
        {
            return data;
        }

        let data = TransactionData::from(
            tx.transaction()
                .zcash_serialize_to_vec()
                .expect("writing a transaction to a Vec cannot fail"),
        );
        let mut cache = self
            .0
            .lock()
            .expect("transaction payload cache lock is not poisoned");
        if let Some(existing) = cache.entries.get(&id) {
            return existing.clone();
        }
        let size = data.retained_bytes();
        if cache.allocation_overhead.saturating_add(size) > cache.max_bytes {
            return data;
        }
        while !cache.entries.is_empty()
            && (cache.bytes.saturating_add(size) > cache.max_bytes
                || cache.entries.len() >= cache.max_entries)
        {
            let oldest = cache.order.pop_front().expect("each entry has a FIFO key");
            let removed = cache
                .entries
                .remove(&oldest)
                .expect("FIFO keys have entries");
            cache.bytes -= removed.retained_bytes();
        }
        if cache.bytes.saturating_add(size) <= cache.max_bytes
            && cache.entries.len() < cache.max_entries
        {
            cache.bytes += size;
            cache.order.push_back(id);
            cache.entries.insert(id, data.clone());
        }
        data
    }
}

#[cfg(test)]
mod tests;
