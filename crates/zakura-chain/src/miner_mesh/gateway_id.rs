//! The ID a coinbase commitment names a gateway by.

use std::{fmt, str::FromStr};

/// The bytes in a [`GatewayId`].
pub(super) const GATEWAY_ID_LEN: usize = 20;

/// The bytes in a v2 node ID, which a [`GatewayId`] is cut from.
const NODE_ID_LEN: usize = 32;

/// The ID a [`MinerMeshCommitment`](super::MinerMeshCommitment) names a
/// gateway by: the first 20 bytes of the gateway's 32-byte v2 node ID.
///
/// Finding a node key whose ID starts with a given gateway ID takes about
/// 2^160 work. Finding two node keys whose IDs share some gateway ID takes
/// about 2^80 work.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GatewayId([u8; GATEWAY_ID_LEN]);

impl GatewayId {
    /// Returns the gateway ID made of exactly these bytes.
    pub const fn from_bytes(bytes: [u8; GATEWAY_ID_LEN]) -> Self {
        Self(bytes)
    }

    /// Returns the gateway ID of the node whose v2 node ID is `node_id`: its
    /// first 20 bytes.
    pub fn from_node_id(node_id: &[u8; NODE_ID_LEN]) -> Self {
        let (prefix, _rest) = node_id.split_at(GATEWAY_ID_LEN);
        Self(
            prefix
                .try_into()
                .expect("a node ID is longer than a gateway ID"),
        )
    }

    /// Returns whether this is the gateway ID of the node whose v2 node ID is
    /// `node_id`.
    pub fn is_prefix_of(&self, node_id: &[u8; NODE_ID_LEN]) -> bool {
        node_id.starts_with(&self.0)
    }

    /// Returns the lowest and the highest node ID this gateway ID is a prefix
    /// of. In byte order, the node IDs it is a prefix of are exactly those
    /// from the first to the second.
    pub fn node_id_bounds(&self) -> ([u8; NODE_ID_LEN], [u8; NODE_ID_LEN]) {
        let mut lowest = [0x00; NODE_ID_LEN];
        let mut highest = [0xff; NODE_ID_LEN];
        lowest[..GATEWAY_ID_LEN].copy_from_slice(&self.0);
        highest[..GATEWAY_ID_LEN].copy_from_slice(&self.0);
        (lowest, highest)
    }

    /// Returns the gateway ID's bytes.
    pub fn as_bytes(&self) -> &[u8; GATEWAY_ID_LEN] {
        &self.0
    }
}

/// The error returned when text names no gateway ID.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum ParseGatewayIdError {
    /// The text is neither a v2 node ID nor a gateway ID long.
    #[error(
        "expected 64 hex digits (a v2 node ID) or 40 hex digits (a gateway ID), \
         found {0} characters"
    )]
    Length(usize),
    /// The text has the length of an ID but is not hex.
    #[error("not hex: {0}")]
    NotHex(#[from] hex::FromHexError),
}

impl FromStr for GatewayId {
    type Err = ParseGatewayIdError;

    /// Reads a gateway ID from hex: either a whole 32-byte v2 node ID, which
    /// names the gateway whose node ID it is, or the 20-byte gateway ID
    /// itself.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text.len() == NODE_ID_LEN * 2 {
            let mut node_id = [0; NODE_ID_LEN];
            hex::decode_to_slice(text, &mut node_id)?;
            Ok(Self::from_node_id(&node_id))
        } else if text.len() == GATEWAY_ID_LEN * 2 {
            let mut gateway_id = [0; GATEWAY_ID_LEN];
            hex::decode_to_slice(text, &mut gateway_id)?;
            Ok(Self(gateway_id))
        } else {
            Err(ParseGatewayIdError::Length(text.chars().count()))
        }
    }
}

impl fmt::Display for GatewayId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

impl fmt::Debug for GatewayId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("GatewayId")
            .field(&hex::encode(self.0))
            .finish()
    }
}
