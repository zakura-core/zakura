use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{
    amount::{self, Amount, NegativeAllowed},
    fmt::HexDebug,
    memory::{vec_capacity_bytes, AttributedMemorySize},
    primitives::ZkSnarkProof,
    sprout::{self, JoinSplit, Nullifier},
};

/// A bundle of [`JoinSplit`] descriptions and signature data.
///
/// JoinSplit descriptions are optional, but Zcash transactions must include a
/// JoinSplit signature and verification key if and only if there is at least one
/// JoinSplit description. This wrapper type bundles at least one JoinSplit
/// description with the required signature data, so that an
/// `Option<JoinSplitData>` correctly models the presence or absence of any
/// JoinSplit data.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinSplitData<P: ZkSnarkProof> {
    /// The first JoinSplit description in the transaction,
    /// using proofs of type `P`.
    ///
    /// Storing this separately from `rest` ensures that it is impossible
    /// to construct an invalid `JoinSplitData` with no `JoinSplit`s.
    ///
    /// However, it's not necessary to access or process `first` and `rest`
    /// separately, as the [`JoinSplitData::joinsplits`] method provides an
    /// iterator over all of the `JoinSplit`s.
    #[serde(bound(
        serialize = "JoinSplit<P>: Serialize",
        deserialize = "JoinSplit<P>: Deserialize<'de>"
    ))]
    pub first: JoinSplit<P>,
    /// The rest of the JoinSplit descriptions, using proofs of type `P`,
    /// in the order they appear in the transaction.
    ///
    /// The [`JoinSplitData::joinsplits`] method provides an iterator over
    /// all `JoinSplit`s.
    #[serde(bound(
        serialize = "JoinSplit<P>: Serialize",
        deserialize = "JoinSplit<P>: Deserialize<'de>"
    ))]
    pub rest: Vec<JoinSplit<P>>,
    /// The public key for the JoinSplit signature, denoted as `joinSplitPubKey` in the spec.
    pub pub_key: JoinSplitPubKey,
    /// The JoinSplit signature, denoted as `joinSplitSig` in the spec.
    pub sig: JoinSplitSig,
}

/// A transaction's `joinSplitPubKey`: the encoded Ed25519 validating key that its
/// `joinSplitSig` is checked against.
///
/// These are the bytes as the transaction encodes them. Nothing checks that they are a
/// valid key: `zakura-consensus` does that when it verifies the signature.
///
/// Serializes like `ed25519_zebra::VerificationKeyBytes`, the type this replaces.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename = "VerificationKeyBytes")]
pub struct JoinSplitPubKey(pub [u8; 32]);

impl fmt::Debug for JoinSplitPubKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("JoinSplitPubKey")
            .field(&HexDebug(&self.0))
            .finish()
    }
}

impl From<[u8; 32]> for JoinSplitPubKey {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl From<JoinSplitPubKey> for [u8; 32] {
    fn from(key: JoinSplitPubKey) -> Self {
        key.0
    }
}

impl AsRef<[u8]> for JoinSplitPubKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// A transaction's `joinSplitSig`: the encoded Ed25519 signature over its JoinSplit data.
///
/// These are the bytes as the transaction encodes them. Nothing checks them:
/// `zakura-consensus` verifies the signature.
///
/// Serializes like `ed25519::Signature`, the type this replaces: a tuple of 64 bytes.
#[derive(Copy, Clone, PartialEq, Eq)]
pub struct JoinSplitSig(pub [u8; 64]);

impl JoinSplitSig {
    /// The length of an encoded signature.
    pub const BYTE_SIZE: usize = 64;
}

impl fmt::Debug for JoinSplitSig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("JoinSplitSig")
            .field(&HexDebug(&self.0))
            .finish()
    }
}

impl From<[u8; 64]> for JoinSplitSig {
    fn from(bytes: [u8; 64]) -> Self {
        Self(bytes)
    }
}

impl From<JoinSplitSig> for [u8; 64] {
    fn from(sig: JoinSplitSig) -> Self {
        sig.0
    }
}

impl AsRef<[u8]> for JoinSplitSig {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Serialize for JoinSplitSig {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeTuple as _;

        let mut tuple = serializer.serialize_tuple(Self::BYTE_SIZE)?;
        for byte in &self.0 {
            tuple.serialize_element(byte)?;
        }
        tuple.end()
    }
}

impl<'de> Deserialize<'de> for JoinSplitSig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct BytesVisitor;

        impl<'de> serde::de::Visitor<'de> for BytesVisitor {
            type Value = JoinSplitSig;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a tuple of 64 bytes")
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<JoinSplitSig, A::Error> {
                let mut bytes = [0u8; JoinSplitSig::BYTE_SIZE];
                for (index, byte) in bytes.iter_mut().enumerate() {
                    *byte = seq
                        .next_element()?
                        .ok_or_else(|| serde::de::Error::invalid_length(index, &self))?;
                }
                Ok(JoinSplitSig(bytes))
            }
        }

        deserializer.deserialize_tuple(Self::BYTE_SIZE, BytesVisitor)
    }
}

impl<P: ZkSnarkProof> AttributedMemorySize for JoinSplitData<P> {
    fn attributed_memory_size_bytes(&self) -> u64 {
        vec_capacity_bytes(&self.rest)
    }
}

impl<P: ZkSnarkProof> fmt::Debug for JoinSplitData<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JoinSplitData")
            .field("first", &self.first)
            .field("rest", &self.rest)
            .field("pub_key", &self.pub_key)
            .field("sig", &self.sig)
            .finish()
    }
}

impl<P: ZkSnarkProof> fmt::Display for JoinSplitData<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut fmter =
            f.debug_struct(format!("JoinSplitData<{}>", std::any::type_name::<P>()).as_str());

        fmter.field("joinsplits", &self.joinsplits().count());
        fmter.field("value_balance", &self.value_balance());

        fmter.finish()
    }
}

impl<P: ZkSnarkProof> JoinSplitData<P> {
    /// Iterate over the [`JoinSplit`]s in `self`, in the order they appear
    /// in the transaction.
    pub fn joinsplits(&self) -> impl Iterator<Item = &JoinSplit<P>> {
        std::iter::once(&self.first).chain(self.rest.iter())
    }

    /// Modify the [`JoinSplit`]s in `self`,
    /// in the order they appear in the transaction.
    #[cfg(any(test, feature = "proptest-impl"))]
    pub fn joinsplits_mut(&mut self) -> impl Iterator<Item = &mut JoinSplit<P>> {
        std::iter::once(&mut self.first).chain(self.rest.iter_mut())
    }

    /// Iterate over the [`Nullifier`]s in `self`.
    pub fn nullifiers(&self) -> impl Iterator<Item = &Nullifier> {
        self.joinsplits()
            .flat_map(|joinsplit| joinsplit.nullifiers.iter())
    }

    /// Return the sprout value balance,
    /// the change in the transaction value pool due to sprout [`JoinSplit`]s.
    ///
    /// <https://zebra.zfnd.org/dev/rfcs/0012-value-pools.html#definitions>
    ///
    /// See [`sprout_value_balance`][svb] for details.
    ///
    /// [svb]: crate::transaction::Transaction::sprout_value_balance
    pub fn value_balance(&self) -> Result<Amount<NegativeAllowed>, amount::Error> {
        self.joinsplit_value_balances().sum()
    }

    /// Return a list of sprout value balances,
    /// the changes in the transaction value pool due to each sprout [`JoinSplit`].
    ///
    /// See [`sprout_value_balance`][svb] for details.
    ///
    /// [svb]: crate::transaction::Transaction::sprout_value_balance
    pub fn joinsplit_value_balances(
        &self,
    ) -> Box<dyn Iterator<Item = Amount<NegativeAllowed>> + '_> {
        Box::new(self.joinsplits().map(JoinSplit::value_balance))
    }

    /// Collect the Sprout note commitments  for this transaction, if it contains `Output`s,
    /// in the order they appear in the transaction.
    pub fn note_commitments(&self) -> impl Iterator<Item = &sprout::commitment::NoteCommitment> {
        self.joinsplits()
            .flat_map(|joinsplit| &joinsplit.commitments)
    }
}
