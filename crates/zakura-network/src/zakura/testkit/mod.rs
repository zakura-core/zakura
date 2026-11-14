//! Test tooling for the default-off Zakura Iroh/QUIC stack.
//!
//! The simulator build, `cfg(zakura_sim)`, compiles only [`regulation`]
//! and [`discovery`]: its tests drive the production regulation tools
//! through the first, and its discovery node starts production's discovery
//! through the second. The rest needs the `zakura-testkit` feature or a
//! test build, and builds only outside the simulator: it runs on tokio's
//! types, which the simulator build replaces.

#[cfg(zakura_sim)]
pub mod discovery;
#[cfg(zakura_sim)]
pub mod regulation;

/// Applies the full test stack's gate to each item.
macro_rules! test_stack {
    ($($item:item)*) => {
        $(
            #[cfg(all(any(test, feature = "zakura-testkit"), not(zakura_sim)))]
            $item
        )*
    };
}

test_stack! {
    mod block_sync_peer;
    #[cfg(test)]
    mod blocksync_fuzz;
    mod clock;
    mod cluster;
    mod endpoint;
    mod hostile;
    #[cfg(test)]
    mod mock_blocksync;
    mod node;
    mod recorder;
    #[cfg(test)]
    pub(crate) mod stream_conformance;
    mod trace_capture;
    mod trace_reader;
    mod wait;

    pub use block_sync_peer::{SyntheticBlockSyncPeer, SyntheticBlockSyncPeers};
    pub use clock::{Clock, RealClock, TestClock};
    pub use cluster::{ClusterTopology, ZakuraTestCluster};
    pub use endpoint::LocalEndpointFactory;
    pub use hostile::HostilePeer;
    #[cfg(test)]
    pub(crate) use node::startup_committed_view;
    pub use node::{ZakuraTestNode, ZakuraTestNodeBuilder};
    pub use recorder::{InboundRecorder, RecordedInbound};
    pub use trace_capture::TraceCapture;
    pub use trace_reader::{TraceQuery, TraceReader, TraceValue};
    pub use wait::{await_until, WaitError, TEST_NET_TIMEOUT};
    pub use zakura_jsonl_trace::JsonlTracer;
}
