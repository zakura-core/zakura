//! The node's composed resource bounds, checked once at startup.
//!
//! Each family keeps its own owners and its own constructor checks. This value
//! only joins their bounds, so a configuration that no owner can fund fails before
//! the node binds a socket. It coordinates nothing at runtime.
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
    protocol::external::MAX_INCOMPLETE_MESSAGE_BYTES,
    zakura::{OwnerBound, ResourceEnvelope, ZakuraBlockSyncConfig, ZakuraLocalLimits},
};

#[cfg(test)]
mod comparison;
#[cfg(test)]
mod tests;

/// Why a node's limits cannot fund its owners.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum NodeEnvelopeError {
    /// A limit leaves an owner without capacity for one legal unit of work.
    #[error("the Zakura limit {limit} must be positive to fund control progress")]
    ZeroLimit {
        /// The limit's name.
        limit: &'static str,
    },
    /// A composed bound does not fit its type.
    #[error("the Zakura resource bound {bound} overflows")]
    Overflow {
        /// The bound's name.
        bound: &'static str,
    },
    /// The block-sync limits cannot size the decoded block pool.
    #[error("the decoded block allowance is invalid: {0}")]
    DecodedAllowance(&'static str),
}

/// The inputs of a [`NodeEnvelope`], taken from each family's own configuration.
#[derive(Copy, Clone, Debug)]
pub(crate) struct NodeEnvelopeInputs {
    pub(crate) transport: ResourceEnvelope,
    pub(crate) receive_window_bytes: u64,
    pub(crate) send_window_bytes: u64,
    pub(crate) max_connections: usize,
    pub(crate) max_pending_handshakes: usize,
    pub(crate) max_open_streams: u16,
    pub(crate) max_frame_bytes: u32,
    pub(crate) peer_memory_budget_bytes: u64,
    pub(crate) tcp_connections: usize,
    pub(crate) decoded_pipeline_bytes: u64,
}

/// The bounds of every owner family on one node.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct NodeEnvelope {
    /// The most handshake and stream owners, across every generation.
    pub(crate) owners: OwnerBound,
    /// The most frame bytes that transport owners retain.
    pub(crate) transport_bytes: u128,
    /// The most request-stream executions that open streams allow at once.
    pub(crate) request_executions: usize,
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
        inputs: NodeEnvelopeInputs,
        decoded_blocks: NonZeroU64,
    ) -> Result<Self, NodeEnvelopeError> {
        // Each limit funds at least one handshake, stream, or frame. A zero here
        // would strand control progress behind a budget that no release can refill.
        for (limit, value) in [
            ("receive_window_bytes", inputs.receive_window_bytes),
            ("send_window_bytes", inputs.send_window_bytes),
            ("max_frame_bytes", u64::from(inputs.max_frame_bytes)),
            ("max_open_streams", u64::from(inputs.max_open_streams)),
            ("peer_memory_budget_bytes", inputs.peer_memory_budget_bytes),
        ] {
            if value == 0 {
                return Err(NodeEnvelopeError::ZeroLimit { limit });
            }
        }
        if inputs.max_connections == 0 {
            return Err(NodeEnvelopeError::ZeroLimit {
                limit: "max_connections",
            });
        }
        if inputs.max_pending_handshakes == 0 {
            return Err(NodeEnvelopeError::ZeroLimit {
                limit: "max_pending_handshakes",
            });
        }

        let owners = OwnerBound::new(
            inputs.max_pending_handshakes,
            inputs.max_connections,
            inputs.max_open_streams,
        );
        // `OwnerBound::new` saturates, so the maximum means that it overflowed.
        if owners.streams == usize::MAX {
            return Err(NodeEnvelopeError::Overflow {
                bound: "stream owners",
            });
        }
        // Widening usize to u64 is lossless on supported targets.
        let tcp_ingress_bytes = (inputs.tcp_connections as u64)
            .checked_mul(MAX_INCOMPLETE_MESSAGE_BYTES as u64)
            .ok_or(NodeEnvelopeError::Overflow {
                bound: "tcp ingress bytes",
            })?;

        Ok(Self {
            owners,
            transport_bytes: inputs.transport.retained_frame_bytes(),
            request_executions: inputs.transport.request_executions,
            quic_bytes: inputs.peer_memory_budget_bytes,
            tcp_ingress_bytes,
            decoded_blocks: decoded_blocks.get(),
            decoded_pipeline_bytes: inputs.decoded_pipeline_bytes,
        })
    }

    /// The envelope of a node with this configuration.
    pub(crate) fn for_node(
        config: &Config,
        limits: &ZakuraLocalLimits,
        decoded_backend_blocks: u64,
    ) -> Result<Self, NodeEnvelopeError> {
        Self::new(
            NodeEnvelopeInputs {
                transport: limits.resource_envelope(),
                receive_window_bytes: u64::from(limits.quic.receive_window_bytes),
                send_window_bytes: limits.quic.send_window_bytes,
                max_connections: limits.max_connections,
                max_pending_handshakes: limits.max_pending_handshakes,
                max_open_streams: limits.max_open_streams,
                max_frame_bytes: limits.max_frame_bytes,
                peer_memory_budget_bytes: config.zakura.peer_memory_budget_bytes,
                tcp_connections: config.peerset_total_connection_limit(),
                decoded_pipeline_bytes: config.zakura.block_sync.decoded_pipeline_byte_bound(),
            },
            decoded_allowance(&config.zakura.block_sync, decoded_backend_blocks)?,
        )
    }

    /// The most bytes that this envelope's byte owners retain together.
    pub(crate) fn retained_bytes_bound(&self) -> u128 {
        self.transport_bytes + u128::from(self.quic_bytes) + u128::from(self.tcp_ingress_bytes)
    }
}

fn decoded_allowance(
    block_sync: &ZakuraBlockSyncConfig,
    backend_blocks: u64,
) -> Result<NonZeroU64, NodeEnvelopeError> {
    block_sync
        .decoded_block_allowance(backend_blocks)
        .map_err(NodeEnvelopeError::DecodedAllowance)
}
