//! External terminal-UTXO membership artifacts authenticated by release commitments.

use std::{
    io::{self, Read},
    mem::size_of,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

const HASH_LEN: usize = 32;
const BITS_PER_BYTE: u64 = 8;
const MAGIC: &[u8; 8] = b"ZKSHINT\0";
const VERSION_OFFSET: usize = MAGIC.len();
const CHAIN_IDENTITY_OFFSET: usize = VERSION_OFFSET + size_of::<u16>();
const TERMINAL_HEIGHT_OFFSET: usize = CHAIN_IDENTITY_OFFSET + HASH_LEN;
const TERMINAL_BLOCK_HASH_OFFSET: usize = TERMINAL_HEIGHT_OFFSET + size_of::<u32>();
const OUTPUT_COUNT_OFFSET: usize = TERMINAL_BLOCK_HASH_OFFSET + HASH_LEN;
/// Fixed header length, excluding membership bits.
pub const HEADER_LEN: usize = OUTPUT_COUNT_OFFSET + size_of::<u64>();
/// Maximum artifact length accepted before allocation (512 MiB).
pub const MAX_ARTIFACT_LEN: u64 = 512 * 1024 * 1024;
/// Current Zakura artifact version.
pub const FORMAT_VERSION: u16 = 1;

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
    let header_len =
        u64::try_from(HEADER_LEN).map_err(|_| Error::Format("artifact header is too large"))?;
    let len = output_count
        .div_ceil(BITS_PER_BYTE)
        .checked_add(header_len)
        .filter(|len| *len <= MAX_ARTIFACT_LEN)
        .ok_or(Error::Format("output count exceeds artifact limit"))?;
    Ok(len)
}

fn header_field<const N: usize>(bytes: &[u8; HEADER_LEN], offset: usize) -> Result<[u8; N], Error> {
    bytes
        .get(offset..offset + N)
        .ok_or(Error::Format("truncated artifact header"))?
        .try_into()
        .map_err(|_| Error::Format("invalid artifact header field"))
}

#[derive(Debug)]
struct Header {
    chain_identity: [u8; HASH_LEN],
    terminal_height: u32,
    terminal_block_hash: [u8; HASH_LEN],
    output_count: u64,
}

impl Header {
    fn parse(bytes: &[u8; HEADER_LEN]) -> Result<Self, Error> {
        if &bytes[..MAGIC.len()] != MAGIC
            || u16::from_le_bytes(header_field(bytes, VERSION_OFFSET)?) != FORMAT_VERSION
        {
            return Err(Error::Format("unknown magic or version"));
        }

        Ok(Self {
            chain_identity: header_field(bytes, CHAIN_IDENTITY_OFFSET)?,
            terminal_height: u32::from_le_bytes(header_field(bytes, TERMINAL_HEIGHT_OFFSET)?),
            terminal_block_hash: header_field(bytes, TERMINAL_BLOCK_HASH_OFFSET)?,
            output_count: u64::from_le_bytes(header_field(bytes, OUTPUT_COUNT_OFFSET)?),
        })
    }

    fn encode(
        chain_identity: [u8; HASH_LEN],
        terminal_height: u32,
        terminal_block_hash: [u8; HASH_LEN],
    ) -> [u8; HEADER_LEN] {
        let mut bytes = [0; HEADER_LEN];
        bytes[..MAGIC.len()].copy_from_slice(MAGIC);
        bytes[VERSION_OFFSET..CHAIN_IDENTITY_OFFSET].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        bytes[CHAIN_IDENTITY_OFFSET..TERMINAL_HEIGHT_OFFSET].copy_from_slice(&chain_identity);
        bytes[TERMINAL_HEIGHT_OFFSET..TERMINAL_BLOCK_HASH_OFFSET]
            .copy_from_slice(&terminal_height.to_le_bytes());
        bytes[TERMINAL_BLOCK_HASH_OFFSET..OUTPUT_COUNT_OFFSET]
            .copy_from_slice(&terminal_block_hash);
        bytes
    }
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
        let parsed_header = Header::parse(&header)?;
        let output_count = parsed_header.output_count;
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
        let used_bits = output_count % BITS_PER_BYTE;
        if used_bits != 0 && bytes[len - 1] >> used_bits != 0 {
            return Err(Error::Format("nonzero padding bits"));
        }
        // The ordinary writer never inserts the genesis coinbase output.
        if output_count != 0 && bytes[HEADER_LEN] & 1 != 0 {
            return Err(Error::Format("genesis output must be absent"));
        }
        let commitment = Commitment {
            chain_identity: parsed_header.chain_identity,
            terminal_height: parsed_header.terminal_height,
            terminal_block_hash: parsed_header.terminal_block_hash,
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
        let byte = usize::try_from(ordinal / BITS_PER_BYTE).map_err(|_| Error::Ordinal)?;
        Ok(self.0.bytes[HEADER_LEN + byte] & (1 << (ordinal % BITS_PER_BYTE)) != 0)
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
        let bytes = Vec::from(Header::encode(
            chain_identity,
            terminal_height,
            terminal_block_hash,
        ));
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
        if count.is_multiple_of(BITS_PER_BYTE) {
            bytes.push(0);
        }
        if retained {
            let last = bytes.len() - 1;
            bytes[last] |= 1 << (*count % BITS_PER_BYTE);
        }
        *count = next;
        Ok(())
    }

    /// Finish the untrusted generated artifact.
    pub fn finish(mut self) -> Vec<u8> {
        self.bytes[OUTPUT_COUNT_OFFSET..HEADER_LEN].copy_from_slice(&self.count.to_le_bytes());
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
