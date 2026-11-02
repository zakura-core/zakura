//! The node's composed resource bounds, checked once at startup.
//!
//! Each family keeps its own owners and its own constructor checks. This value
//! only joins their bounds, so a configuration that no owner can fund fails before
//! the node binds a socket. It coordinates nothing at runtime. Zero limits never reach
//! it: `QuicConfig::validate` refuses zero windows, `ZakuraLocalLimits::from_config`
//! clamps connections and handshakes, handshake constants fund frames and streams, and
//! `budgeted_max_connections_for_node` refuses a budget below one connection.
//!
//! The byte bounds add up without double counting. A frame's bytes belong to QUIC's
//! buffers until a reader takes them, and to a raw grant afterwards. The peer memory
//! budget funds the former and the transport envelope funds the latter. TCP decoders
//! hold their incomplete messages under a connection limit of their own. Decoded
//! blocks stay a separate allowance: a count across every holder, and a byte limit
//! only on what the block pipeline still holds.

use std::num::NonZeroU64;

use thiserror::Error;

use crate::{
    config::Config,
    zakura::{transport::TransportLimits, OwnerBound, ResourceEnvelope, ZakuraLocalLimits},
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
}

/// The bounds of every owner family on one node.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct NodeEnvelope {
    /// The most handshake and stream owners, across every generation.
    pub(crate) owners: OwnerBound,
    /// The most frame bytes that transport owners retain.
    pub(crate) transport_bytes: u128,
    /// The most bytes in QUIC's buffers and the sockets' queues.
    pub(crate) quic_bytes: u64,
    /// The most bytes that TCP decoders hold for incomplete messages.
    pub(crate) tcp_ingress_bytes: u64,
    /// The most blocks that the decoded pool admits within its limit.
    pub(crate) decoded_blocks: u64,
    /// The most decoded bytes that the block pipeline holds within its limit.
    pub(crate) decoded_pipeline_bytes: u64,
}

impl NodeEnvelope {
    /// Check these limits and compose their bounds.
    pub(crate) fn new(
        transport: TransportLimits,
        quic_bytes: u64,
        tcp_ingress_bytes: u64,
        decoded_blocks: NonZeroU64,
        decoded_pipeline_bytes: u64,
    ) -> Result<Self, NodeEnvelopeError> {
        Ok(Self {
            owners: OwnerBound::new(
                transport.max_pending_handshakes,
                transport.max_connections,
                transport.max_open_streams,
            )
            .ok_or(NodeEnvelopeError::OwnerOverflow)?,
            transport_bytes: ResourceEnvelope::new(transport).retained_frame_bytes(),
            quic_bytes,
            tcp_ingress_bytes,
            decoded_blocks: decoded_blocks.get(),
            decoded_pipeline_bytes,
        })
    }

    /// The envelope of a node with this configuration.
    pub(crate) fn for_node(
        config: &Config,
        limits: &ZakuraLocalLimits,
        decoded_backend_blocks: u64,
    ) -> Result<Self, NodeEnvelopeError> {
        let block_sync = &config.zakura.block_sync;
        Self::new(
            limits.transport_limits(),
            config.zakura.peer_memory_budget_bytes,
            config.tcp_ingress_bound_bytes(),
            block_sync
                .decoded_block_allowance(decoded_backend_blocks)
                .map_err(NodeEnvelopeError::DecodedAllowance)?,
            block_sync.decoded_pipeline_byte_bound(),
        )
    }

    /// The most bytes that this envelope's byte owners retain together.
    pub(crate) fn retained_bytes_bound(&self) -> u128 {
        self.transport_bytes + u128::from(self.quic_bytes) + u128::from(self.tcp_ingress_bytes)
    }
}
