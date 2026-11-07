//! Fails closed if the coinbase builder requests a Sapling proof.

use std::convert::Infallible;

use rand_10::Rng;
use sapling_crypto::{
    bundle::GrothProofBytes,
    circuit,
    keys::EphemeralSecretKey,
    prover::{OutputProver, SpendProver},
    value::{NoteValue, ValueCommitTrapdoor},
    Diversifier, MerklePath, PaymentAddress, ProofGenerationKey, Rseed,
};

/// Satisfies the transaction builder's prover bounds without proving parameters.
///
/// The coinbase plan only adds transparent or Ironwood outputs, so the builder
/// must never invoke these methods. The uninhabited proof type prevents this
/// adapter from returning a placeholder proof if that invariant changes.
pub(super) struct NoSaplingProver;

impl SpendProver for NoSaplingProver {
    type Proof = Infallible;

    fn prepare_circuit(
        _proof_generation_key: ProofGenerationKey,
        _diversifier: Diversifier,
        _rseed: Rseed,
        _value: NoteValue,
        _alpha: jubjub::Fr,
        _rcv: ValueCommitTrapdoor,
        _anchor: bls12_381::Scalar,
        _merkle_path: MerklePath,
    ) -> Option<circuit::Spend> {
        unreachable!("coinbase construction never adds Sapling spends")
    }

    fn create_proof<R: Rng>(&self, _circuit: circuit::Spend, _rng: &mut R) -> Self::Proof {
        unreachable!("coinbase construction never adds Sapling spends")
    }

    fn encode_proof(proof: Self::Proof) -> GrothProofBytes {
        match proof {}
    }
}

impl OutputProver for NoSaplingProver {
    type Proof = Infallible;

    fn prepare_circuit(
        _esk: &EphemeralSecretKey,
        _payment_address: PaymentAddress,
        _rcm: jubjub::Fr,
        _value: NoteValue,
        _rcv: ValueCommitTrapdoor,
    ) -> circuit::Output {
        unreachable!("coinbase construction never adds Sapling outputs")
    }

    fn create_proof<R: Rng>(&self, _circuit: circuit::Output, _rng: &mut R) -> Self::Proof {
        unreachable!("coinbase construction never adds Sapling outputs")
    }

    fn encode_proof(proof: Self::Proof) -> GrothProofBytes {
        match proof {}
    }
}
