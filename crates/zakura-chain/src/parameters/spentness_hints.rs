//! External terminal-UTXO membership artifacts authenticated by release commitments.

use std::io::{self, Read};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Fixed header length, excluding membership bits.
pub const HEADER_LEN: usize = 86;
/// Maximum artifact length accepted before allocation (512 MiB).
pub const MAX_ARTIFACT_LEN: u64 = 512 * 1024 * 1024;
/// Current Zakura artifact version.
pub const FORMAT_VERSION: u16 = 1;
const MAGIC: &[u8; 8] = b"ZKSHINT\0";

mod commitments;
pub use commitments::MAINNET_COMMITMENTS;

/// Release-reviewed identity. Artifact bytes remain outside the executable.
///
/// A caller must resolve this descriptor against its release authority before use.
/// Parsing a descriptor received from a peer does not establish that authority.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Commitment {
    /// Actual genesis block hash in serialized byte order.
    pub chain_identity: [u8; 32],
    /// Exact terminal checkpoint height.
    pub terminal_height: u32,
    /// Terminal block hash in serialized byte order.
    pub terminal_block_hash: [u8; 32],
    /// Artifact encoding version.
    pub format_version: u16,
    /// Number of transparent outputs, including genesis.
    pub output_count: u64,
    /// Complete artifact length.
    pub byte_len: u64,
    /// SHA-256 of the complete artifact.
    pub sha256: [u8; 32],
}

/// Artifact parsing or authentication failed.
#[derive(Debug, Error)]
pub enum Error {
    /// The source could not be read.
    #[error("reading spentness artifact: {0}")]
    Io(#[from] io::Error),
    /// The encoding violates a format bound.
    #[error("invalid spentness artifact: {0}")]
    Format(&'static str),
    /// The artifact differs from the expected release commitment.
    #[error("spentness artifact does not match its trusted commitment")]
    CommitmentMismatch,
    /// The caller requested an output outside the artifact.
    #[error("spentness output ordinal is out of bounds")]
    Ordinal,
}

/// Calculate the exact file size without overflowing or allocating.
pub fn artifact_len(output_count: u64) -> Result<u64, Error> {
    let len = output_count
        .checked_add(7)
        .and_then(|count| count.checked_div(8))
        .and_then(|bytes| bytes.checked_add(86))
        .filter(|len| *len <= MAX_ARTIFACT_LEN)
        .ok_or(Error::Format("output count exceeds artifact limit"))?;
    Ok(len)
}

impl Commitment {
    /// Check the descriptor before using its size to bound a read.
    pub fn validate(&self) -> Result<(), Error> {
        if self.format_version != FORMAT_VERSION
            || artifact_len(self.output_count)? != self.byte_len
        {
            return Err(Error::Format("invalid commitment version or length"));
        }
        Ok(())
    }
}

/// Structurally valid owned bytes. This type deliberately exposes no membership bits.
#[derive(Debug)]
pub struct ParsedArtifact {
    bytes: Vec<u8>,
    commitment: Commitment,
}

impl ParsedArtifact {
    /// Read a bounded artifact, rejecting truncation and trailing bytes.
    pub fn read(mut source: impl Read) -> Result<Self, Error> {
        let mut header = [0; HEADER_LEN];
        source.read_exact(&mut header)?;
        if &header[..8] != MAGIC || u16::from_le_bytes([header[8], header[9]]) != FORMAT_VERSION {
            return Err(Error::Format("unknown magic or version"));
        }
        let mut identity = [0; 32];
        identity.copy_from_slice(&header[10..42]);
        let mut height = [0; 4];
        height.copy_from_slice(&header[42..46]);
        let mut hash = [0; 32];
        hash.copy_from_slice(&header[46..78]);
        let mut count = [0; 8];
        count.copy_from_slice(&header[78..86]);
        let output_count = u64::from_le_bytes(count);
        let byte_len = artifact_len(output_count)?;
        let len = usize::try_from(byte_len)
            .map_err(|_| Error::Format("artifact cannot fit in memory"))?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(len)
            .map_err(|_| Error::Format("artifact allocation failed"))?;
        bytes.extend_from_slice(&header);
        bytes.resize(len, 0);
        source.read_exact(&mut bytes[HEADER_LEN..])?;
        if source.read(&mut [0; 1])? != 0 {
            return Err(Error::Format("trailing bytes"));
        }
        let used_bits = output_count % 8;
        if used_bits != 0 && bytes[len - 1] >> used_bits != 0 {
            return Err(Error::Format("nonzero padding bits"));
        }
        // The ordinary writer never inserts the genesis coinbase output.
        if output_count != 0 && bytes[HEADER_LEN] & 1 != 0 {
            return Err(Error::Format("genesis output must be absent"));
        }
        let commitment = Commitment {
            chain_identity: identity,
            terminal_height: u32::from_le_bytes(height),
            terminal_block_hash: hash,
            format_version: FORMAT_VERSION,
            output_count,
            byte_len,
            sha256: Sha256::digest(&bytes).into(),
        };
        Ok(Self { bytes, commitment })
    }

    /// Describe generated bytes for review; this does not authorize their use.
    pub fn commitment(&self) -> &Commitment {
        &self.commitment
    }

    /// Authenticate the exact owned bytes against a trusted descriptor.
    pub fn verify(self, expected: &Commitment) -> Result<VerifiedArtifact, Error> {
        expected.validate()?;
        if &self.commitment != expected {
            return Err(Error::CommitmentMismatch);
        }
        Ok(VerifiedArtifact(self))
    }
}

/// Authenticated owned bytes. No later reads of a mutable source file occur.
#[derive(Debug)]
pub struct VerifiedArtifact(ParsedArtifact);

impl VerifiedArtifact {
    /// Read with the trusted size as an additional bound, then authenticate.
    pub fn read(source: impl Read, expected: &Commitment) -> Result<Self, Error> {
        expected.validate()?;
        // Read at most one extra byte so trailing data cannot be silently accepted.
        let mut bytes = Vec::new();
        source.take(expected.byte_len + 1).read_to_end(&mut bytes)?;
        if u64::try_from(bytes.len()).ok() != Some(expected.byte_len) {
            return Err(Error::CommitmentMismatch);
        }
        ParsedArtifact::read(bytes.as_slice())?.verify(expected)
    }

    /// The authenticated descriptor.
    pub fn commitment(&self) -> &Commitment {
        &self.0.commitment
    }

    /// Exact authenticated file bytes for caching and bounded peer serving.
    pub fn bytes(&self) -> &[u8] {
        &self.0.bytes
    }

    /// Whether the ordinary UTXO set retains this output at the terminal height.
    pub fn retains(&self, ordinal: u64) -> Result<bool, Error> {
        if ordinal >= self.0.commitment.output_count {
            return Err(Error::Ordinal);
        }
        let byte = usize::try_from(ordinal / 8).map_err(|_| Error::Ordinal)?;
        Ok(self.0.bytes[HEADER_LEN + byte] & (1 << (ordinal % 8)) != 0)
    }
}

/// Encode generator membership in canonical output order.
pub fn encode(
    chain_identity: [u8; 32],
    terminal_height: u32,
    terminal_block_hash: [u8; 32],
    membership: impl IntoIterator<Item = bool>,
) -> Result<Vec<u8>, Error> {
    let mut encoder = Encoder::new(chain_identity, terminal_height, terminal_block_hash);
    for bit in membership {
        encoder.push(bit)?;
    }
    Ok(encoder.finish())
}

/// Bounded generator buffer; stores one bit per enumerated output.
#[derive(Debug)]
pub struct Encoder {
    bytes: Vec<u8>,
    count: u64,
}

impl Encoder {
    /// Begin an artifact at the exact generation boundary.
    pub fn new(
        chain_identity: [u8; 32],
        terminal_height: u32,
        terminal_block_hash: [u8; 32],
    ) -> Self {
        let mut bytes = Vec::from(MAGIC.as_slice());
        bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&chain_identity);
        bytes.extend_from_slice(&terminal_height.to_le_bytes());
        bytes.extend_from_slice(&terminal_block_hash);
        bytes.extend_from_slice(&0u64.to_le_bytes());
        Self { bytes, count: 0 }
    }

    /// Append the next output's membership.
    pub fn push(&mut self, retained: bool) -> Result<(), Error> {
        let Self { bytes, count } = self;
        if *count == 0 && retained {
            return Err(Error::Format("genesis output must be absent"));
        }
        let next = count.checked_add(1).ok_or(Error::Ordinal)?;
        artifact_len(next)?;
        if *count % 8 == 0 {
            bytes.push(0);
        }
        if retained {
            let last = bytes.len() - 1;
            bytes[last] |= 1 << (*count % 8);
        }
        *count = next;
        Ok(())
    }

    /// Finish the untrusted generated artifact.
    pub fn finish(mut self) -> Vec<u8> {
        self.bytes[78..86].copy_from_slice(&self.count.to_le_bytes());
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_commitments_match_checkpoint_authority() {
        let checkpoints = crate::parameters::Network::Mainnet.checkpoint_list();
        let mut previous = None;
        for pin in MAINNET_COMMITMENTS {
            pin.validate().unwrap();
            assert_eq!(
                checkpoints.hash(crate::block::Height(0)).unwrap().0,
                pin.chain_identity
            );
            assert_eq!(
                checkpoints
                    .hash(crate::block::Height(pin.terminal_height))
                    .unwrap()
                    .0,
                pin.terminal_block_hash
            );
            assert!(previous.is_none_or(|height| pin.terminal_height > height));
            previous = Some(pin.terminal_height);
        }
        if let Some(pin) = MAINNET_COMMITMENTS.last() {
            assert_eq!(pin.terminal_height, checkpoints.max_height().0);
        }
    }

    #[test]
    fn boundaries_and_owned_verification() {
        for count in [0, 1, 7, 8, 9, 17] {
            let bits: Vec<_> = (0..count).map(|n| n != 0 && n % 3 != 0).collect();
            let mut source = encode([1; 32], 9, [2; 32], bits.clone()).unwrap();
            let parsed = ParsedArtifact::read(source.as_slice()).unwrap();
            let pin = parsed.commitment().clone();
            let verified = parsed.verify(&pin).unwrap();
            source.fill(0);
            for (n, bit) in bits.into_iter().enumerate() {
                assert_eq!(verified.retains(u64::try_from(n).unwrap()).unwrap(), bit);
            }
            assert!(verified.retains(count).is_err());
            assert_eq!(pin.byte_len, artifact_len(count).unwrap());
        }
    }

    #[test]
    fn rejects_malformed_and_untrusted_artifacts() {
        let bytes = encode([1; 32], 9, [2; 32], [false, true, true]).unwrap();
        assert_eq!(bytes[HEADER_LEN], 6);
        let pin = ParsedArtifact::read(bytes.as_slice())
            .unwrap()
            .commitment()
            .clone();
        for end in 0..bytes.len() {
            assert!(ParsedArtifact::read(&bytes[..end]).is_err());
        }
        for index in [0, 8, 10, 42, 46, 78, 86] {
            let mut changed = bytes.clone();
            changed[index] ^= 1;
            assert!(VerifiedArtifact::read(changed.as_slice(), &pin).is_err());
        }
        let mut padded = bytes.clone();
        padded[HEADER_LEN] |= 0x80;
        assert!(ParsedArtifact::read(padded.as_slice()).is_err());
        let mut trailing = bytes;
        trailing.push(0);
        assert!(ParsedArtifact::read(trailing.as_slice()).is_err());
        assert!(artifact_len(u64::MAX).is_err());
        assert!(artifact_len(MAX_ARTIFACT_LEN * 8).is_err());
        assert!(encode([1; 32], 9, [2; 32], [true]).is_err());
    }
}
