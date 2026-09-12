//! Connection and node ownership of retained response metadata allocations.

use crate::zakura::transport::ByteBudget;

/// Requested heap size of an Arc, including its reference counts and padding.
pub(crate) fn shared_allocation_bytes<T>() -> u64 {
    let (layout, _) = std::alloc::Layout::new::<[usize; 2]>()
        .extend(std::alloc::Layout::new::<T>())
        .expect("fixed shared metadata fits an allocation");
    u64::try_from(layout.pad_to_align().size())
        .expect("shared allocation size fits the byte counter")
}

/// Bound a collection allocation before constructing its elements.
pub(crate) fn collection_allocation_bytes<T>(count: usize) -> Option<u64> {
    u64::try_from(std::alloc::Layout::array::<T>(count).ok()?.size()).ok()
}

/// Retained response metadata is separate from body, decoder, and worker budgets.
const NODE_RESPONSE_METADATA_BYTES: u64 = 128 * 1024 * 1024;
const CONNECTION_RESPONSE_METADATA_BYTES: u64 = 16 * 1024 * 1024;

/// One endpoint's pool, shared across all messages and connections.
#[derive(Clone, Debug)]
pub(crate) struct ResponseMemory {
    node: ByteBudget,
    connection_limit: u64,
}

/// One connection's share. Service fanout and session replacement clone this handle.
#[derive(Clone, Debug)]
pub(crate) struct ConnectionResponseMemory {
    node: ByteBudget,
    connection: ByteBudget,
}

/// Retain this owner with the allocation, including after an exchange ends.
#[derive(Debug)]
pub(crate) struct ResponseMemoryPermit {
    memory: ConnectionResponseMemory,
    bytes: u64,
}

impl Default for ResponseMemory {
    fn default() -> Self {
        Self::new(
            NODE_RESPONSE_METADATA_BYTES,
            CONNECTION_RESPONSE_METADATA_BYTES,
        )
    }
}

impl ResponseMemory {
    pub(crate) fn new(node_bytes: u64, connection_bytes: u64) -> Self {
        Self {
            node: ByteBudget::new(node_bytes),
            connection_limit: connection_bytes,
        }
    }

    pub(crate) fn connection(&self) -> ConnectionResponseMemory {
        ConnectionResponseMemory {
            node: self.node.clone(),
            connection: ByteBudget::new(self.connection_limit),
        }
    }
}

impl ConnectionResponseMemory {
    /// Charge both domains before allocating. Failure leaves neither domain charged.
    pub(crate) fn try_reserve(&self, bytes: u64) -> Option<ResponseMemoryPermit> {
        if bytes == 0 || bytes > self.node.available() || bytes > self.connection.available() {
            return None;
        }
        let mut memory = self.clone();
        if !memory.connection.try_reserve(bytes) {
            return None;
        }
        if !memory.node.try_reserve(bytes) {
            memory.connection.release(bytes);
            // A smaller waiter can now fit the connection even though node
            // bytes did not change. Stable exhaustion returns before reserving.
            memory.node.subscribe_capacity().notify_waiters();
            return None;
        }
        Some(ResponseMemoryPermit { memory, bytes })
    }

    /// Releases in either domain release node bytes and wake all affected sessions.
    pub(crate) fn subscribe_capacity(&self) -> &tokio::sync::Notify {
        self.node.subscribe_capacity()
    }
}

impl Drop for ResponseMemoryPermit {
    fn drop(&mut self) {
        // Make connection capacity visible before the shared node notification.
        self.memory.connection.release(self.bytes);
        self.memory.node.release(self.bytes);
    }
}

#[cfg(test)]
mod tests;
