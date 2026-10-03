//! Configure network rules once, then reuse them for each complete payload.

use crate::{
    parameters::{Network, NetworkKind},
    work::equihash::{REGTEST_SOLUTION_SIZE, SOLUTION_SIZE},
};

use super::{SerializationError, ZcashDeserialize, ZcashReader};

/// Decodes complete payloads using the node's configured network rules.
///
/// Regtest headers are smaller than Mainnet and Testnet headers. Using Regtest's
/// minimum everywhere would let a Mainnet message reserve space for more headers
/// than its bytes can contain. Configure this decoder once at the message
/// boundary. Its readers carry the rules through nested values and read limits.
///
/// ```
/// use zakura_chain::{parameters::Network, serialization::ZcashDecoder};
///
/// let decoder = ZcashDecoder::for_network(&Network::Mainnet);
/// let mut payload = &[2, 10, 20][..];
/// let bytes: Vec<u8> = decoder.decode(&mut payload)?;
/// assert_eq!(bytes, [10, 20]);
/// assert!(payload.is_empty());
/// # Ok::<(), zakura_chain::serialization::SerializationError>(())
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ZcashDecoder {
    network: Option<NetworkKind>,
}

impl ZcashDecoder {
    /// Select rules from local configuration, never from a peer's payload.
    pub fn for_network(network: &Network) -> Self {
        Self {
            network: Some(network.kind()),
        }
    }

    /// Accept encodings from any network for offline data and existing generic
    /// decoding APIs. Live message decoders should use [`Self::for_network`].
    pub const fn any_network() -> Self {
        Self { network: None }
    }

    /// The configured network, or `None` when decoding data from any network.
    pub fn network_kind(self) -> Option<NetworkKind> {
        self.network
    }

    /// Decode one value, leaving the slice pointing to any unread bytes.
    pub fn decode<T: ZcashDeserialize>(self, bytes: &mut &[u8]) -> Result<T, SerializationError> {
        self.reader(bytes).read_value()
    }

    /// Start a reader for one payload. Nested reads inherit these rules without
    /// requiring a network argument on each decoding call.
    pub fn reader<'a, 'b>(self, bytes: &'a mut &'b [u8]) -> ZcashReader<&'a mut &'b [u8]> {
        ZcashReader::with_decoder(bytes, self)
    }

    pub(crate) fn minimum_equihash_solution_size(self) -> usize {
        match self.network {
            Some(NetworkKind::Mainnet | NetworkKind::Testnet) => SOLUTION_SIZE,
            Some(NetworkKind::Regtest) | None => REGTEST_SOLUTION_SIZE,
        }
    }

    pub(crate) fn accepts_equihash_solution_size(self, size: usize) -> bool {
        match self.network {
            Some(_) => size == self.minimum_equihash_solution_size(),
            None => matches!(size, SOLUTION_SIZE | REGTEST_SOLUTION_SIZE),
        }
    }
}
