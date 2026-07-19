//! Native ZIP-143 and ZIP-243 signature hashing for V3 and V4 transactions.

use std::io;

use blake2b_simd::{Hash as Blake2bHash, Params, State};

use super::SigHash;
use crate::{
    parameters::{NetworkUpgrade, OVERWINTER_VERSION_GROUP_ID, SAPLING_VERSION_GROUP_ID},
    primitives::ZkSnarkProof,
    sapling,
    serialization::ZcashSerialize,
    transaction::{JoinSplitData, Transaction},
    transparent,
};

const ZCASH_SIGHASH_PERSONALIZATION_PREFIX: &[u8; 12] = b"ZcashSigHash";
const ZCASH_PREVOUTS_HASH_PERSONALIZATION: &[u8; 16] = b"ZcashPrevoutHash";
const ZCASH_SEQUENCE_HASH_PERSONALIZATION: &[u8; 16] = b"ZcashSequencHash";
const ZCASH_OUTPUTS_HASH_PERSONALIZATION: &[u8; 16] = b"ZcashOutputsHash";
const ZCASH_JOINSPLITS_HASH_PERSONALIZATION: &[u8; 16] = b"ZcashJSplitsHash";
const ZCASH_SHIELDED_SPENDS_HASH_PERSONALIZATION: &[u8; 16] = b"ZcashSSpendsHash";
const ZCASH_SHIELDED_OUTPUTS_HASH_PERSONALIZATION: &[u8; 16] = b"ZcashSOutputHash";

const OVERWINTERED_FLAG: u32 = 1 << 31;
const V3_VERSION: u32 = 3;
const V4_VERSION: u32 = 4;

const SIGHASH_MASK: u8 = 0x1f;
const SIGHASH_ALL: u8 = 0x01;
const SIGHASH_NONE: u8 = 0x02;
const SIGHASH_SINGLE: u8 = 0x03;
const SIGHASH_ANYONECANPAY: u8 = 0x80;
const ZERO_DIGEST: [u8; 32] = [0; 32];

fn hasher(personalization: &[u8; 16]) -> State {
    Params::new()
        .hash_length(32)
        .personal(personalization)
        .to_state()
}

struct HashWriter<'a>(&'a mut State);

impl io::Write for HashWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn update_serialized<T: ZcashSerialize>(state: &mut State, value: &T) {
    value
        .zcash_serialize(HashWriter(state))
        .expect("writing to a BLAKE2b state is infallible");
}

fn update_outpoint(state: &mut State, input: &transparent::Input) {
    match input {
        transparent::Input::PrevOut { outpoint, .. } => update_serialized(state, outpoint),
        transparent::Input::Coinbase { .. } => {
            state.update(&[0; 32]);
            state.update(&u32::MAX.to_le_bytes());
        }
    }
}

fn prevouts_hash(inputs: &[transparent::Input]) -> Blake2bHash {
    let mut state = hasher(ZCASH_PREVOUTS_HASH_PERSONALIZATION);
    for input in inputs {
        update_outpoint(&mut state, input);
    }
    state.finalize()
}

fn sequence_hash(inputs: &[transparent::Input]) -> Blake2bHash {
    let mut state = hasher(ZCASH_SEQUENCE_HASH_PERSONALIZATION);
    for input in inputs {
        state.update(&input.sequence().to_le_bytes());
    }
    state.finalize()
}

fn output_hash(output: &transparent::Output) -> Blake2bHash {
    let mut state = hasher(ZCASH_OUTPUTS_HASH_PERSONALIZATION);
    update_serialized(&mut state, output);
    state.finalize()
}

fn outputs_hash(outputs: &[transparent::Output]) -> Blake2bHash {
    let mut state = hasher(ZCASH_OUTPUTS_HASH_PERSONALIZATION);
    for output in outputs {
        update_serialized(&mut state, output);
    }
    state.finalize()
}

fn joinsplits_hash<P: ZkSnarkProof>(
    joinsplit_data: Option<&JoinSplitData<P>>,
) -> Option<Blake2bHash> {
    let joinsplit_data = joinsplit_data?;
    let mut state = hasher(ZCASH_JOINSPLITS_HASH_PERSONALIZATION);
    for joinsplit in joinsplit_data.joinsplits() {
        update_serialized(&mut state, joinsplit);
    }
    state.update(joinsplit_data.pub_key.as_ref());
    Some(state.finalize())
}

fn sapling_spends_hash(
    shielded_data: &sapling::ShieldedData<sapling::PerSpendAnchor>,
) -> Option<Blake2bHash> {
    shielded_data.spends().next()?;

    let mut state = hasher(ZCASH_SHIELDED_SPENDS_HASH_PERSONALIZATION);
    for spend in shielded_data.spends() {
        state.update(&spend.cv.to_bytes());
        update_serialized(&mut state, &spend.per_spend_anchor);
        state.update(&<[u8; 32]>::from(spend.nullifier));
        state.update(&<[u8; 32]>::from(spend.rk.clone()));
        update_serialized(&mut state, &spend.zkproof);
    }
    Some(state.finalize())
}

fn sapling_outputs_hash(
    shielded_data: &sapling::ShieldedData<sapling::PerSpendAnchor>,
) -> Option<Blake2bHash> {
    shielded_data.outputs().next()?;

    let mut state = hasher(ZCASH_SHIELDED_OUTPUTS_HASH_PERSONALIZATION);
    for output in shielded_data.outputs() {
        update_serialized(&mut state, &sapling::OutputInTransactionV4(output.clone()));
    }
    Some(state.finalize())
}

#[derive(Clone, Copy, Debug)]
enum LegacyVersion {
    V3,
    V4 {
        value_balance: i64,
        sapling_spends: Option<Blake2bHash>,
        sapling_outputs: Option<Blake2bHash>,
    },
}

impl LegacyVersion {
    fn header(self) -> u32 {
        OVERWINTERED_FLAG
            | match self {
                Self::V3 => V3_VERSION,
                Self::V4 { .. } => V4_VERSION,
            }
    }

    fn version_group_id(self) -> u32 {
        match self {
            Self::V3 => OVERWINTER_VERSION_GROUP_ID,
            Self::V4 { .. } => SAPLING_VERSION_GROUP_ID,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct InputTail {
    outpoint: Option<transparent::OutPoint>,
    sequence: u32,
}

/// Transaction-wide ZIP-143/243 data reused by every signature hash.
#[derive(Clone, Debug)]
pub(super) struct LegacySighash {
    version: LegacyVersion,
    consensus_branch_id: u32,
    lock_time: u32,
    expiry_height: u32,
    inputs: Vec<InputTail>,
    previous_output_values: Vec<i64>,
    prevouts: Blake2bHash,
    sequence: Blake2bHash,
    outputs: Blake2bHash,
    single_outputs: Vec<Blake2bHash>,
    joinsplits: Option<Blake2bHash>,
}

impl LegacySighash {
    /// Returns precomputed data for V3/V4 transactions, and `None` for other versions.
    pub(super) fn new(
        transaction: &Transaction,
        network_upgrade: NetworkUpgrade,
        all_previous_outputs: &[transparent::Output],
    ) -> Option<Self> {
        let consensus_branch_id = u32::from(network_upgrade.branch_id()?);

        match transaction {
            Transaction::V3 {
                inputs,
                outputs,
                expiry_height,
                joinsplit_data,
                ..
            } => Some(Self::from_parts(
                LegacyVersion::V3,
                consensus_branch_id,
                transaction.raw_lock_time(),
                expiry_height.0,
                inputs,
                outputs,
                joinsplits_hash(joinsplit_data.as_ref()),
                all_previous_outputs,
            )),
            Transaction::V4 {
                inputs,
                outputs,
                expiry_height,
                joinsplit_data,
                sapling_shielded_data,
                ..
            } => {
                let value_balance = sapling_shielded_data
                    .as_ref()
                    .map_or(0, |data| data.value_balance.zatoshis());
                let sapling_spends = sapling_shielded_data.as_ref().and_then(sapling_spends_hash);
                let sapling_outputs = sapling_shielded_data
                    .as_ref()
                    .and_then(sapling_outputs_hash);

                Some(Self::from_parts(
                    LegacyVersion::V4 {
                        value_balance,
                        sapling_spends,
                        sapling_outputs,
                    },
                    consensus_branch_id,
                    transaction.raw_lock_time(),
                    expiry_height.0,
                    inputs,
                    outputs,
                    joinsplits_hash(joinsplit_data.as_ref()),
                    all_previous_outputs,
                ))
            }
            _ => None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn from_parts(
        version: LegacyVersion,
        consensus_branch_id: u32,
        lock_time: u32,
        expiry_height: u32,
        inputs: &[transparent::Input],
        outputs: &[transparent::Output],
        joinsplits: Option<Blake2bHash>,
        all_previous_outputs: &[transparent::Output],
    ) -> Self {
        Self {
            version,
            consensus_branch_id,
            lock_time,
            expiry_height,
            inputs: inputs
                .iter()
                .map(|input| InputTail {
                    outpoint: input.outpoint(),
                    sequence: input.sequence(),
                })
                .collect(),
            previous_output_values: all_previous_outputs
                .iter()
                .map(|output| output.value.zatoshis())
                .collect(),
            prevouts: prevouts_hash(inputs),
            sequence: sequence_hash(inputs),
            outputs: outputs_hash(outputs),
            single_outputs: outputs.iter().map(output_hash).collect(),
            joinsplits,
        }
    }

    /// Computes a signature hash using the raw hash-type byte from the signature.
    ///
    /// `input_index_script_code` is `None` for shielded signatures, which always
    /// use `SIGHASH_ALL`.
    pub(super) fn signature_hash(
        &self,
        raw_hash_type: u8,
        input_index_script_code: Option<(usize, &[u8])>,
    ) -> Option<SigHash> {
        let transparent_input = match input_index_script_code {
            Some((index, script_code)) => Some((
                self.inputs.get(index)?,
                *self.previous_output_values.get(index)?,
                index,
                script_code,
            )),
            None => None,
        };
        let raw_hash_type = if transparent_input.is_some() {
            raw_hash_type
        } else {
            SIGHASH_ALL
        };
        let output_mode = raw_hash_type & SIGHASH_MASK;

        let mut personalization = [0; 16];
        personalization[..12].copy_from_slice(ZCASH_SIGHASH_PERSONALIZATION_PREFIX);
        personalization[12..].copy_from_slice(&self.consensus_branch_id.to_le_bytes());
        let mut state = hasher(&personalization);

        state.update(&self.version.header().to_le_bytes());
        state.update(&self.version.version_group_id().to_le_bytes());

        if raw_hash_type & SIGHASH_ANYONECANPAY == 0 {
            state.update(self.prevouts.as_bytes());
        } else {
            state.update(&ZERO_DIGEST);
        }

        if raw_hash_type & SIGHASH_ANYONECANPAY == 0
            && output_mode != SIGHASH_SINGLE
            && output_mode != SIGHASH_NONE
        {
            state.update(self.sequence.as_bytes());
        } else {
            state.update(&ZERO_DIGEST);
        }

        if output_mode != SIGHASH_SINGLE && output_mode != SIGHASH_NONE {
            state.update(self.outputs.as_bytes());
        } else if output_mode == SIGHASH_SINGLE {
            match transparent_input
                .as_ref()
                .and_then(|(_, _, index, _)| self.single_outputs.get(*index))
            {
                Some(output) => state.update(output.as_bytes()),
                None => state.update(&ZERO_DIGEST),
            };
        } else {
            state.update(&ZERO_DIGEST);
        }

        state.update(
            self.joinsplits
                .as_ref()
                .map_or(ZERO_DIGEST.as_slice(), Blake2bHash::as_bytes),
        );

        if let LegacyVersion::V4 {
            value_balance,
            sapling_spends,
            sapling_outputs,
        } = self.version
        {
            state.update(
                sapling_spends
                    .as_ref()
                    .map_or(ZERO_DIGEST.as_slice(), Blake2bHash::as_bytes),
            );
            state.update(
                sapling_outputs
                    .as_ref()
                    .map_or(ZERO_DIGEST.as_slice(), Blake2bHash::as_bytes),
            );
            state.update(&self.lock_time.to_le_bytes());
            state.update(&self.expiry_height.to_le_bytes());
            state.update(&value_balance.to_le_bytes());
        } else {
            state.update(&self.lock_time.to_le_bytes());
            state.update(&self.expiry_height.to_le_bytes());
        }

        state.update(&u32::from(raw_hash_type).to_le_bytes());

        if let Some((input, value, _, script_code)) = transparent_input {
            match input.outpoint {
                Some(outpoint) => update_serialized(&mut state, &outpoint),
                None => {
                    state.update(&[0; 32]);
                    state.update(&u32::MAX.to_le_bytes());
                }
            }
            update_serialized(&mut state, &transparent::Script::new(script_code));
            state.update(&value.to_le_bytes());
            state.update(&input.sequence.to_le_bytes());
        }

        let digest = state.finalize();
        Some(SigHash(
            digest
                .as_bytes()
                .try_into()
                .expect("BLAKE2b output length is configured to 32 bytes"),
        ))
    }
}
