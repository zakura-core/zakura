use crate::serialization::ZcashReader;
use std::io;

use halo2::pasta::pallas;
use reddsa::orchard::SpendAuth;

use crate::serialization::{
    serde_helpers, ReadZcashExt, SerializationError, ZcashDeserialize, ZcashSerialize,
};

use super::{
    commitment::{self, ValueCommitment},
    keys,
    note::{self, Nullifier},
};

/// An Action description, as described in the [Zcash specification §7.3][actiondesc].
///
/// Action transfers can optionally perform a spend, and optionally perform an
/// output.  Action descriptions are data included in a transaction that
/// describe Action transfers.
///
/// [actiondesc]: https://zips.z.cash/protocol/nu5.pdf#actiondesc
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Action {
    /// A value commitment to net value of the input note minus the output note
    pub cv: commitment::ValueCommitment,
    /// The nullifier of the input note being spent.
    pub nullifier: note::Nullifier,
    /// The randomized validating key for spendAuthSig,
    pub rk: reddsa::VerificationKeyBytes<SpendAuth>,
    /// The x-coordinate of the note commitment for the output note.
    #[serde(with = "serde_helpers::Base")]
    pub cm_x: pallas::Base,
    /// An encoding of an ephemeral Pallas public key corresponding to the
    /// encrypted private key in `out_ciphertext`.
    pub ephemeral_key: keys::EphemeralPublicKey,
    /// A ciphertext component for the encrypted output note.
    pub enc_ciphertext: note::EncryptedNote,
    /// A ciphertext component that allows the holder of a full viewing key to
    /// recover the recipient diversified transmission key and the ephemeral
    /// private key (and therefore the entire note plaintext).
    pub out_ciphertext: note::WrappedNoteKey,
}

impl ZcashSerialize for Action {
    fn zcash_serialize<W: io::Write + ?Sized>(&self, writer: &mut W) -> Result<(), io::Error> {
        self.cv.zcash_serialize(writer)?;
        writer.write_all(&<[u8; 32]>::from(self.nullifier)[..])?;
        writer.write_all(&<[u8; 32]>::from(self.rk)[..])?;
        writer.write_all(&<[u8; 32]>::from(self.cm_x)[..])?;
        self.ephemeral_key.zcash_serialize(writer)?;
        self.enc_ciphertext.zcash_serialize(writer)?;
        self.out_ciphertext.zcash_serialize(writer)?;
        Ok(())
    }
}

/// An [`Action`] as read, with its `cv` and `epk` points still compressed.
///
/// Every other field is validated when it is read. Decompressing a bundle's
/// points together with [`ActionEncoding::decompress`] is faster than decoding
/// each point as it is read.
#[derive(Clone, Debug)]
pub(crate) struct ActionEncoding {
    cv: [u8; 32],
    nullifier: note::Nullifier,
    rk: reddsa::VerificationKeyBytes<SpendAuth>,
    cm_x: pallas::Base,
    ephemeral_key: [u8; 32],
    enc_ciphertext: note::EncryptedNote,
    out_ciphertext: note::WrappedNoteKey,
}

impl ActionEncoding {
    /// Decompresses the `cv` and `epk` points of every action, returning the
    /// actions in order, or an error if any point is not a valid encoding.
    ///
    /// Each point is validated exactly as [`ValueCommitment::try_from`] and
    /// [`keys::EphemeralPublicKey::try_from`] validate it.
    pub(crate) fn decompress(encodings: Vec<Self>) -> Result<Vec<Action>, SerializationError> {
        let points = pallas::Affine::batch_from_bytes(
            encodings
                .iter()
                .flat_map(|encoding| [encoding.cv, encoding.ephemeral_key]),
        );

        let (point_pairs, _) = points.as_chunks::<2>();

        encodings
            .into_iter()
            .zip(point_pairs)
            .map(|(encoding, [cv, ephemeral_key])| {
                Ok(Action {
                    cv: ValueCommitment::from_decoded((*cv).into())
                        .map_err(SerializationError::Parse)?,
                    nullifier: encoding.nullifier,
                    rk: encoding.rk,
                    cm_x: encoding.cm_x,
                    ephemeral_key: keys::EphemeralPublicKey::from_decoded((*ephemeral_key).into())
                        .map_err(SerializationError::Parse)?,
                    enc_ciphertext: encoding.enc_ciphertext,
                    out_ciphertext: encoding.out_ciphertext,
                })
            })
            .collect()
    }
}

impl ZcashDeserialize for Action {
    fn zcash_deserialize_from<R: io::Read>(
        reader: &mut ZcashReader<R>,
    ) -> Result<Self, SerializationError> {
        let encoding = reader.read_value::<ActionEncoding>()?;
        let mut actions = ActionEncoding::decompress(vec![encoding])?;
        Ok(actions
            .pop()
            .expect("decompressing one encoding returns one action"))
    }
}

impl ZcashDeserialize for ActionEncoding {
    fn zcash_deserialize_from<R: io::Read>(
        reader: &mut ZcashReader<R>,
    ) -> Result<Self, SerializationError> {
        // # Consensus
        //
        // > Elements of an Action description MUST be canonical encodings of the types given above.
        //
        // https://zips.z.cash/protocol/protocol.pdf#actiondesc
        //
        // > LEOS2IP_{256}(cmx) MUST be less than 𝑞_ℙ.
        //
        // https://zips.z.cash/protocol/protocol.pdf#actionencodingandconsensus
        //
        // See comments below for each specific type.
        Ok(ActionEncoding {
            // Type is ValueCommit^{Orchard}.Output, i.e. ℙ.
            // https://zips.z.cash/protocol/protocol.pdf#abstractcommit
            // Decompressed and validated by [`ActionEncoding::decompress`].
            cv: reader.read_32_bytes()?,
            // Type is `{0 .. 𝑞_ℙ − 1}`. See [`Nullifier::try_from`].
            nullifier: Nullifier::try_from(reader.read_32_bytes()?)?,
            // Type is SpendAuthSig^{Orchard}.Public, i.e. ℙ.
            // https://zips.z.cash/protocol/protocol.pdf#concretespendauthsig
            // https://zips.z.cash/protocol/protocol.pdf#concretereddsa
            // This only reads the 32-byte buffer. The type is enforced
            // on signature verification; see [`reddsa::batch`]
            rk: {
                let rk_bytes = reader.read_32_bytes()?;
                if rk_bytes == [0u8; 32] {
                    return Err(SerializationError::Parse(
                        "Orchard action rk must not be the identity point",
                    ));
                }
                rk_bytes.into()
            },
            // Type is `{0 .. 𝑞_ℙ − 1}`. Note that the second rule quoted above
            // is also enforced here and it is technically redundant with the first.
            // See [`pallas::Base::zcash_deserialize`].
            cm_x: reader.read_value::<pallas::Base>()?,
            // Denoted by `epk` in the spec. Type is KA^{Orchard}.Public, i.e. ℙ^*.
            // https://zips.z.cash/protocol/protocol.pdf#concreteorchardkeyagreement
            // Decompressed and validated by [`ActionEncoding::decompress`].
            ephemeral_key: reader.read_32_bytes()?,
            // Type is `Sym.C`, i.e. `𝔹^Y^{\[N\]}`, i.e. arbitrary-sized byte arrays
            // https://zips.z.cash/protocol/protocol.pdf#concretesym but fixed to
            // 580 bytes in https://zips.z.cash/protocol/protocol.pdf#outputencodingandconsensus
            // See [`note::EncryptedNote::zcash_deserialize`].
            enc_ciphertext: reader.read_value::<note::EncryptedNote>()?,
            // Type is `Sym.C`, i.e. `𝔹^Y^{\[N\]}`, i.e. arbitrary-sized byte arrays
            // https://zips.z.cash/protocol/protocol.pdf#concretesym but fixed to
            // 80 bytes in https://zips.z.cash/protocol/protocol.pdf#outputencodingandconsensus
            // See [`note::WrappedNoteKey::zcash_deserialize`].
            out_ciphertext: reader.read_value::<note::WrappedNoteKey>()?,
        })
    }
}
