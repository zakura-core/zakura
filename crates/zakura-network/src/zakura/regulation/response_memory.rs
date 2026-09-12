//! Connection and node ownership of retained response metadata allocations.

use std::sync::Arc;

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
const NODE_SETUP_BYTES: u64 = 512;
// Covers fixed context, counter, and first-use notification/cancellation storage.
// Allocation probes check this allowance independently of per-request charges.
const CONNECTION_SETUP_BYTES: u64 = 4096;

/// One endpoint's pool, shared across all messages and connections.
#[derive(Clone, Debug)]
pub(crate) struct ResponseMemory {
    node: ByteBudget,
    connection_limit: u64,
    _setup: Arc<NodeSetup>,
}

#[derive(Debug)]
struct NodeSetup {
    node: ByteBudget,
}

/// One connection's share. Service fanout and session replacement clone this handle.
#[derive(Clone, Debug)]
pub(crate) struct ConnectionResponseMemory(Arc<ConnectionMemory>);

#[derive(Debug)]
struct ConnectionMemory {
    pool: ResponseMemory,
    connection: ByteBudget,
    _setup: ConnectionSetup,
}

#[derive(Debug)]
struct ConnectionSetup {
    pool: ResponseMemory,
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
        assert!(
            node_bytes >= NODE_SETUP_BYTES,
            "the node limit funds pool accounting"
        );
        let mut node = ByteBudget::new(node_bytes);
        assert!(node.try_reserve(NODE_SETUP_BYTES));
        // The shared notification can allocate its mutex on first use. Its
        // storage belongs to the pool, which can outlive every connection.
        node.subscribe_capacity().notify_waiters();
        Self {
            _setup: Arc::new(NodeSetup { node: node.clone() }),
            node,
            connection_limit: connection_bytes,
        }
    }

    /// Fund fixed connection accounting before allocating its shared context.
    pub(crate) fn try_connection(&self) -> Option<ConnectionResponseMemory> {
        if self.connection_limit < CONNECTION_SETUP_BYTES {
            return None;
        }
        let mut node = self.node.clone();
        if !node.try_reserve(CONNECTION_SETUP_BYTES) {
            return None;
        }
        let setup = ConnectionSetup { pool: self.clone() };
        let mut connection = ByteBudget::new(self.connection_limit);
        assert!(connection.try_reserve(CONNECTION_SETUP_BYTES));
        connection.subscribe_capacity().notify_waiters();
        Some(ConnectionResponseMemory(Arc::new(ConnectionMemory {
            pool: self.clone(),
            connection,
            _setup: setup,
        })))
    }
}

impl ConnectionResponseMemory {
    /// Charge both domains before allocating. Failure leaves neither domain charged.
    pub(crate) fn try_reserve(&self, bytes: u64) -> Option<ResponseMemoryPermit> {
        if bytes == 0
            || bytes > self.0.pool.node.available()
            || bytes > self.0.connection.available()
        {
            return None;
        }
        let memory = self.clone();
        if !memory.0.connection.clone().try_reserve(bytes) {
            return None;
        }
        if !memory.0.pool.node.clone().try_reserve(bytes) {
            memory.0.connection.clone().release(bytes);
            // A smaller waiter can now fit the connection even though node
            // bytes did not change. Stable exhaustion returns before reserving.
            memory.0.pool.node.subscribe_capacity().notify_waiters();
            return None;
        }
        Some(ResponseMemoryPermit { memory, bytes })
    }

    /// Releases in either domain release node bytes and wake all affected sessions.
    pub(crate) fn subscribe_capacity(&self) -> &tokio::sync::Notify {
        self.0.pool.node.subscribe_capacity()
    }
}

impl ResponseMemoryPermit {
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Transfer part of one admission to storage with a different lifetime.
    pub(crate) fn split_off(&mut self, bytes: u64) -> Self {
        self.bytes = self
            .bytes
            .checked_sub(bytes)
            .expect("split memory is part of the original reservation");
        Self {
            memory: self.memory.clone(),
            bytes,
        }
    }
}

impl Drop for ResponseMemoryPermit {
    fn drop(&mut self) {
        // Make connection capacity visible before the shared node notification.
        self.memory.0.connection.clone().release(self.bytes);
        self.memory.0.pool.node.clone().release(self.bytes);
    }
}

impl Drop for ConnectionSetup {
    fn drop(&mut self) {
        self.pool.node.release(CONNECTION_SETUP_BYTES);
    }
}

impl Drop for NodeSetup {
    fn drop(&mut self) {
        self.node.release(NODE_SETUP_BYTES);
    }
}

#[cfg(test)]
mod tests;
