//! Note and value commitments.

use std::io;

use hex::{FromHex, FromHexError, ToHex};

use crate::serialization::{SerializationError, ZcashDeserialize, ZcashSerialize};

#[cfg(test)]
mod test_vectors;

/// The randomness used in the Pedersen Hash for note commitment.
///
/// Equivalent to `sapling_crypto::note::CommitmentRandomness`,
/// but we can't use it directly as it is not public.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct CommitmentRandomness(jubjub::Fr);

/// A Sapling value commitment, stored as its 32-byte compressed encoding.
///
/// The commitment is a Jubjub point, but decompressing it needs a field square
/// root, which is expensive. Since the note-commitment tree uses `cm_u` rather
/// than `cv`, we keep the raw bytes and decompress lazily via
/// [`ValueCommitment::commitment`], keeping point decompression off the
/// checkpoint-sync hot path.
///
/// # Consensus
///
/// Deserialization only checks the byte length; it does not prove the bytes are
/// a canonical, non-small-order point. The not-small-order check is deferred to
/// the semantic verifier and mempool, which call
/// [`ValueCommitment::is_valid_not_small_order`] (via
/// [`Transaction::sapling_point_encodings_are_valid`]) and also convert the
/// transaction through librustzcash, whose `read_value_commitment` rejects a
/// small-order `cv`. The checkpoint verifier trusts block hashes and skips the
/// check. Covered by
/// `sapling_small_order_cv_epk_deferred_but_caught_by_librustzcash`.
///
/// [`Transaction::sapling_point_encodings_are_valid`]: crate::transaction::Transaction::sapling_point_encodings_are_valid
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ValueCommitment(pub(crate) [u8; 32]);

impl ValueCommitment {
    /// Decompresses and returns the underlying `sapling_crypto` value
    /// commitment, or `None` if the stored bytes are not a canonical,
    /// non-small-order Jubjub point.
    ///
    /// This is the point decompression that deserialization defers, so it is
    /// fallible: callers must handle an invalid commitment rather than assume the
    /// stored bytes are valid.
    pub fn commitment(&self) -> Option<sapling_crypto::value::ValueCommitment> {
        sapling_crypto::value::ValueCommitment::from_bytes_not_small_order(&self.0).into_option()
    }

    /// Return the stored 32-byte (little-endian) compressed encoding.
    pub fn to_bytes(&self) -> [u8; 32] {
        self.0
    }

    /// Returns true if the stored encoding is a canonical, non-small-order
    /// Jubjub point, i.e. a valid value commitment per the consensus rules.
    ///
    /// This is the not-small-order check deferred from deserialization, run by
    /// the semantic verifier (not the checkpoint verifier) on untrusted
    /// transactions.
    ///
    /// To stay in consensus with the rest of the network, this must accept
    /// exactly the `cv` encodings librustzcash accepts, so it calls the same
    /// `from_bytes_not_small_order` that `read_value_commitment` uses. Do not
    /// swap in a different decoder. Equivalence is pinned by
    /// `sapling_point_checks_match_librustzcash_predicates`.
    pub fn is_valid_not_small_order(&self) -> bool {
        bool::from(
            sapling_crypto::value::ValueCommitment::from_bytes_not_small_order(&self.0).is_some(),
        )
    }

    /// Return the hash bytes in big-endian byte-order suitable for printing out byte by byte.
    ///
    /// Zebra displays commitment value in big-endian byte-order,
    /// following the convention set by zcashd.
    pub fn bytes_in_display_order(&self) -> [u8; 32] {
        let mut reversed_bytes = self.0;
        reversed_bytes.reverse();
        reversed_bytes
    }
}

impl ToHex for &ValueCommitment {
    fn encode_hex<T: FromIterator<char>>(&self) -> T {
        self.bytes_in_display_order().encode_hex()
    }

    fn encode_hex_upper<T: FromIterator<char>>(&self) -> T {
        self.bytes_in_display_order().encode_hex_upper()
    }
}

impl FromHex for ValueCommitment {
    type Error = FromHexError;

    fn from_hex<T: AsRef<[u8]>>(hex: T) -> Result<Self, Self::Error> {
        // Parse hex string to 32 bytes
        let mut bytes = <[u8; 32]>::from_hex(hex)?;
        // Convert from big-endian (display) to little-endian (internal)
        bytes.reverse();

        Self::zcash_deserialize(io::Cursor::new(&bytes))
            .map_err(|_| FromHexError::InvalidStringLength)
    }
}

#[cfg(any(test, feature = "proptest-impl"))]
impl From<jubjub::ExtendedPoint> for ValueCommitment {
    /// Convert a Jubjub point into a ValueCommitment.
    ///
    /// # Panics
    ///
    /// Panics if the given point does not correspond to a valid ValueCommitment.
    fn from(extended_point: jubjub::ExtendedPoint) -> Self {
        ValueCommitment(jubjub::AffinePoint::from(extended_point).to_bytes())
    }
}

impl ZcashDeserialize for sapling_crypto::value::ValueCommitment {
    fn zcash_deserialize<R: io::Read>(mut reader: R) -> Result<Self, SerializationError> {
        let mut buf = [0u8; 32];
        reader.read_exact(&mut buf)?;

        let value_commitment: Option<sapling_crypto::value::ValueCommitment> =
            sapling_crypto::value::ValueCommitment::from_bytes_not_small_order(&buf).into_option();

        value_commitment.ok_or(SerializationError::Parse("invalid ValueCommitment bytes"))
    }
}

impl ZcashDeserialize for ValueCommitment {
    fn zcash_deserialize<R: io::Read>(mut reader: R) -> Result<Self, SerializationError> {
        // Store the raw bytes without decompressing the Jubjub point; the point
        // and its not-small-order check are recovered lazily in
        // `ValueCommitment::commitment`.
        let mut bytes = [0u8; 32];
        reader.read_exact(&mut bytes)?;
        Ok(Self(bytes))
    }
}

impl ZcashSerialize for ValueCommitment {
    fn zcash_serialize<W: io::Write>(&self, mut writer: W) -> Result<(), io::Error> {
        writer.write_all(&self.0)?;
        Ok(())
    }
}

impl ZcashDeserialize for sapling_crypto::note::ExtractedNoteCommitment {
    fn zcash_deserialize<R: io::Read>(mut reader: R) -> Result<Self, SerializationError> {
        let mut buf = [0u8; 32];
        reader.read_exact(&mut buf)?;

        let extracted_note_commitment: Option<sapling_crypto::note::ExtractedNoteCommitment> =
            sapling_crypto::note::ExtractedNoteCommitment::from_bytes(&buf).into_option();

        extracted_note_commitment.ok_or(SerializationError::Parse(
            "invalid ExtractedNoteCommitment bytes",
        ))
    }
}
