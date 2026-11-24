//! The coinbase output that carries a gateway ID.

use crate::{
    amount::Amount,
    transparent::{Output, Script},
};

/// The opcodes before the payload: `OP_RETURN`, which makes the output
/// unspendable, and the opcode that pushes the next 35 bytes as data.
///
/// Opcodes `0x01` to `0x4b` push that many bytes, and the payload is 35 bytes.
const SCRIPT_OPCODES: [u8; 2] = [0x6a, 0x23];

/// The start of every payload: the tag `MR`, then the version this node
/// writes and reads.
const PAYLOAD_PREFIX: [u8; 3] = [b'M', b'R', 0x01];

/// The bytes in a gateway ID.
const GATEWAY_ID_LEN: usize = 32;

/// The bytes in a payload: the tag, the version and the gateway ID.
const PAYLOAD_LEN: usize = PAYLOAD_PREFIX.len() + GATEWAY_ID_LEN;

/// A miner's statement of the gateway node other miners can connect to.
///
/// It is a claim, not a proof: nothing ties the named gateway to the miner.
///
/// On chain it is a zero-value transparent coinbase output whose lock script
/// is exactly these 37 bytes:
///
/// ```text
/// 6a 23 || "MR" || 01 || gateway_id[32]
/// ```
///
/// An output with another version byte, another length or a nonzero value is
/// not a commitment.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct MinerMeshCommitment {
    gateway_id: [u8; GATEWAY_ID_LEN],
}

impl MinerMeshCommitment {
    /// Returns a commitment to `gateway_id`.
    ///
    /// The bytes are not checked: an ID that names no node is still a
    /// commitment.
    pub fn new(gateway_id: [u8; GATEWAY_ID_LEN]) -> Self {
        Self { gateway_id }
    }

    /// Returns the gateway ID exactly as it is written in the output.
    pub fn gateway_id(&self) -> [u8; GATEWAY_ID_LEN] {
        self.gateway_id
    }

    /// Returns the data the lock script pushes: the tag, the version and the
    /// gateway ID.
    pub fn payload(&self) -> [u8; PAYLOAD_LEN] {
        let mut payload = [0; PAYLOAD_LEN];
        let (prefix, gateway_id) = payload.split_at_mut(PAYLOAD_PREFIX.len());
        prefix.copy_from_slice(&PAYLOAD_PREFIX);
        gateway_id.copy_from_slice(&self.gateway_id);
        payload
    }

    /// Returns the coinbase output that carries this commitment.
    pub fn to_output(&self) -> Output {
        let script = [SCRIPT_OPCODES.as_slice(), &self.payload()].concat();
        Output::new(Amount::zero(), Script::new(&script))
    }

    /// Returns the commitment in a coinbase transaction's `outputs`.
    ///
    /// Returns `None` when no output is a commitment, and when two or more
    /// are, because the miner's choice is then ambiguous.
    pub fn from_coinbase_outputs(outputs: &[Output]) -> Option<Self> {
        let mut commitments = outputs.iter().filter_map(Self::from_output);
        let commitment = commitments.next()?;
        commitments.next().is_none().then_some(commitment)
    }

    /// Returns the commitment `output` carries, if it is one.
    fn from_output(output: &Output) -> Option<Self> {
        if !output.value.is_zero() {
            return None;
        }
        let gateway_id = output
            .lock_script
            .as_raw_bytes()
            .strip_prefix(&SCRIPT_OPCODES)?
            .strip_prefix(&PAYLOAD_PREFIX)?;
        Some(Self {
            // Anything but exactly 32 bytes after the prefix is not a commitment.
            gateway_id: gateway_id.try_into().ok()?,
        })
    }
}
