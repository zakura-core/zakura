//! Test tooling for the default-off Zakura Iroh/QUIC stack.

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
pub use node::{ZakuraTestNode, ZakuraTestNodeBuilder};
pub use recorder::{InboundRecorder, RecordedInbound};
pub use trace_capture::TraceCapture;
pub use trace_reader::{TraceQuery, TraceReader, TraceValue};
pub use wait::{await_until, WaitError, TEST_NET_TIMEOUT};
pub use zakura_jsonl_trace::JsonlTracer;
