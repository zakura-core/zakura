//! The node's composed resource bounds, checked once at startup.
//!
//! Each family keeps its own owners and its own constructor checks. This value
//! only joins their bounds, so a configuration that no owner can fund fails before
//! the node binds a socket. It coordinates nothing at runtime. Zero limits never reach
//! it: `QuicConfig::validate` refuses zero windows, `ZakuraLocalLimits::from_config`
//! clamps connections and handshakes, handshake constants fund frames and streams, and
//! `budgeted_max_connections_for_node` refuses a budget below one connection.
//!
//! Every byte bound derives from the reactors' shares (`ReactorShares`): each admitted
//! connection holds the handshake window, every reactor's receive share and the send
//! window in QUIC, and each kind's raw and output gates hold that kind's shares of
//! frames. Handshakes hold the handshake and send windows, and each bound socket its
//! datagram queue. The peer memory budget is the ceiling on the QUIC part. An
//! embedder's custom stream kinds take the `other` share, as on a live connection.
//!
//! A held raw frame's bytes sit both in its connection's receive window and in its
//! kind's raw gate, so the retained bound counts them twice. The second count funds a
//! payload buffer that the reader allocates before its bytes arrive. TCP decoders
//! hold their incomplete messages under a connection limit of their own. Decoded
//! blocks stay a separate allowance: a count across every holder, and a byte limit
//! only on what the block pipeline still holds.
//!
//! Block sync, header sync and discovery reserve a serving response's whole cap
//! before producing it, and hold it until the last frame is written. Their frames
//! are guarded, so they skip the transport output gate, and the retained bound adds
//! the serving bound once. Each family's budget funds one session per connection
//! that can serve at once, not per configured session slot.

use std::num::NonZeroU64;

use thiserror::Error;

use crate::{
    config::Config,
    zakura::{
        block_sync::getblocks_serving_limits, discovery::discovery_serving_output_bytes,
        header_sync, transport::TransportLimits, OwnerBound, ResourceEnvelope, ZakuraLocalLimits,
    },
};

#[cfg(test)]
mod comparison;
#[cfg(test)]
mod tests;

/// Why a node's limits cannot fund its owners.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum NodeEnvelopeError {
    /// The stream owner bound does not fit its type.
    #[error("the Zakura stream owner bound overflows")]
    OwnerOverflow,
    /// The block-sync limits cannot size the decoded block pool.
    #[error("the decoded block allowance is invalid: {0}")]
    DecodedAllowance(&'static str),
    /// The connections' QUIC windows and the sockets' queues exceed the peer
    /// memory budget.
    #[error(
        "the Zakura QUIC envelope of {quic_bytes} bytes exceeds \
         [network.zakura] peer_memory_budget_bytes ({budget_bytes} bytes)"
    )]
    PeerMemoryBudget {
        /// The connections' QUIC windows and the sockets' queues.
        quic_bytes: u128,
        /// The configured budget.
        budget_bytes: u64,
    },
}

/// The bounds of every owner family on one node.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct NodeEnvelope {
    /// The most handshake and stream owners, across every generation.
    pub(crate) owners: OwnerBound,
    /// Request-stream executions that open streams allow at once.
    pub(crate) request_executions: usize,
    /// The most frame bytes that transport owners retain.
    pub(crate) transport_bytes: u128,
    /// The most bytes in QUIC's buffers and the sockets' queues: every admitted
    /// connection's windows, the handshakes' windows, and the socket queues.
    pub(crate) quic_bytes: u128,
    /// The most bytes that TCP decoders hold for incomplete messages.
    pub(crate) tcp_ingress_bytes: u64,
    /// The most blocks that the decoded pool admits within its limit.
    pub(crate) decoded_blocks: u64,
    /// The most decoded bytes that the block pipeline holds within its limit.
    pub(crate) decoded_pipeline_bytes: u64,
    /// The most unsent response bytes that serving reactors reserve together: each
    /// family's node output budget, summed.
    pub(crate) serve_output_bytes: u128,
}

impl NodeEnvelope {
    /// Check these limits and compose their bounds.
    pub(crate) fn new(
        transport: TransportLimits,
        quic_bytes: u128,
        tcp_ingress_bytes: u64,
        decoded_blocks: NonZeroU64,
        decoded_pipeline_bytes: u64,
        serve_output_bytes: u128,
    ) -> Result<Self, NodeEnvelopeError> {
        Ok(Self {
            owners: OwnerBound::new(
                transport.max_pending_handshakes,
                transport.max_connections,
                transport.max_open_streams,
            )
            .ok_or(NodeEnvelopeError::OwnerOverflow)?,
            request_executions: ResourceEnvelope::new(transport).request_executions,
            transport_bytes: ResourceEnvelope::new(transport).retained_frame_bytes(),
            quic_bytes,
            tcp_ingress_bytes,
            decoded_blocks: decoded_blocks.get(),
            decoded_pipeline_bytes,
            serve_output_bytes,
        })
    }

    /// The envelope of a node with this configuration that also serves
    /// `custom_kinds`, or an error if the peer memory budget can't fund its
    /// connections.
    pub(crate) fn for_node(
        config: &Config,
        limits: &ZakuraLocalLimits,
        custom_kinds: &[u16],
        decoded_backend_blocks: u64,
    ) -> Result<Self, NodeEnvelopeError> {
        let block_sync = &config.zakura.block_sync;
        let quic_bytes =
            config
                .zakura
                .quic_memory_bytes(&config.network, limits.max_connections, custom_kinds);
        let budget_bytes = config.zakura.peer_memory_budget_bytes;
        if quic_bytes > u128::from(budget_bytes) {
            return Err(NodeEnvelopeError::PeerMemoryBudget {
                quic_bytes,
                budget_bytes,
            });
        }
        Self::new(
            limits.transport_limits_for_kinds(
                limits
                    .reactor_shares()
                    .kinds
                    .iter()
                    .map(|(kind, _)| *kind)
                    .chain(custom_kinds.iter().copied()),
            ),
            quic_bytes,
            config.tcp_ingress_bound_bytes(),
            block_sync
                .decoded_block_allowance(decoded_backend_blocks)
                .map_err(NodeEnvelopeError::DecodedAllowance)?,
            block_sync.decoded_pipeline_byte_bound(),
            serve_output_bytes(config, limits),
        )
    }

    /// The most bytes that this envelope's byte owners retain together.
    pub(crate) fn retained_bytes_bound(&self) -> u128 {
        self.transport_bytes
            + self.quic_bytes
            + u128::from(self.tcp_ingress_bytes)
            + self.serve_output_bytes
    }
}

/// The node output budgets of every serving family on a node with this
/// configuration and `limits`' admitted connections, summed.
fn serve_output_bytes(config: &Config, limits: &ZakuraLocalLimits) -> u128 {
    let connections = limits.max_connections;
    let block_sync = config
        .zakura
        .block_sync
        .clone()
        .for_node_connections(connections);
    let headers = config
        .zakura
        .header_sync
        .clone()
        .for_node_connections(connections);
    [
        getblocks_serving_limits(&block_sync).node_output_bytes,
        header_sync::v9::serving_limits(
            header_sync::v9::SERVE_PEER_OUTPUT_BYTES,
            headers.serving_sessions(connections),
            headers.node_serve_output_bytes,
        )
        .node_output_bytes,
        discovery_serving_output_bytes(limits.max_connections),
    ]
    .into_iter()
    .map(u128::from)
    .sum()
}
