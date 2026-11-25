//! Randomised checks of the miner mesh commitment's reader.

use proptest::{collection::vec, prelude::*};

use crate::{
    amount::Amount,
    miner_mesh::{GatewayId, MinerMeshCommitment},
    transparent::{Output, Script},
};

/// The bytes in a commitment's lock script.
const SCRIPT_LEN: usize = 25;

/// The lock script bytes before the gateway ID: two opcodes, the tag and the
/// version.
const HEADER_LEN: usize = 5;

/// Outputs that are not commitments, because a commitment's script is never
/// shorter than [`SCRIPT_LEN`].
fn other_outputs() -> impl Strategy<Value = Vec<Output>> {
    vec(
        (any::<Amount<_>>(), vec(any::<u8>(), 0..SCRIPT_LEN))
            .prop_map(|(value, script)| Output::new(value, Script::new(&script))),
        0..8,
    )
}

/// Any gateway ID.
fn gateway_id() -> impl Strategy<Value = GatewayId> {
    any::<[u8; 20]>().prop_map(GatewayId::from_bytes)
}

proptest! {
    /// Any 20 bytes come back as written, wherever the commitment sits among
    /// other outputs.
    #[test]
    fn any_gateway_id_round_trips(
        gateway_id in gateway_id(),
        mut outputs in other_outputs(),
        position in any::<prop::sample::Index>(),
    ) {
        let commitment = MinerMeshCommitment::new(gateway_id);
        outputs.insert(position.index(outputs.len() + 1), commitment.to_output());

        let read = MinerMeshCommitment::from_coinbase_outputs(&outputs);

        prop_assert_eq!(read, Some(commitment));
        prop_assert_eq!(read.map(|commitment| commitment.gateway_id()), Some(gateway_id));
    }

    /// Changing any byte before the gateway ID leaves an output that names no
    /// gateway.
    #[test]
    fn a_changed_header_byte_is_not_a_commitment(
        gateway_id in gateway_id(),
        index in 0..HEADER_LEN,
        change in 1..=u8::MAX,
    ) {
        let mut script = MinerMeshCommitment::new(gateway_id)
            .lock_script()
            .as_raw_bytes()
            .to_vec();
        script[index] ^= change;
        let output = Output::new(Amount::zero(), Script::new(&script));

        prop_assert_eq!(MinerMeshCommitment::from_coinbase_outputs(&[output]), None);
    }

    /// Two commitments name no gateway, whatever they commit to.
    #[test]
    fn two_commitments_name_no_gateway(
        first in gateway_id(),
        second in gateway_id(),
        mut outputs in other_outputs(),
    ) {
        outputs.push(MinerMeshCommitment::new(first).to_output());
        outputs.insert(0, MinerMeshCommitment::new(second).to_output());

        prop_assert_eq!(MinerMeshCommitment::from_coinbase_outputs(&outputs), None);
    }

    /// A gateway ID is the start of the node ID it was cut from, and of no
    /// node ID that differs in its first 20 bytes.
    #[test]
    fn a_gateway_id_matches_exactly_the_node_ids_it_starts(
        node_id in any::<[u8; 32]>(),
        other in any::<[u8; 32]>(),
    ) {
        let gateway_id = GatewayId::from_node_id(&node_id);

        prop_assert!(gateway_id.is_prefix_of(&node_id));
        prop_assert_eq!(gateway_id.is_prefix_of(&other), node_id[..20] == other[..20]);
        prop_assert_eq!(GatewayId::from_node_id(&other) == gateway_id, node_id[..20] == other[..20]);

        let (lowest, highest) = gateway_id.node_id_bounds();
        prop_assert!(lowest <= node_id && node_id <= highest);
        prop_assert_eq!(lowest <= other && other <= highest, gateway_id.is_prefix_of(&other));
    }
}
