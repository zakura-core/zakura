//! Fixed cases for the miner mesh commitment's bytes and its reader.

use crate::{
    amount::{Amount, NonNegative},
    miner_mesh::MinerMeshCommitment,
    serialization::ZcashSerialize,
    transparent::{Output, Script},
};

/// A gateway ID with a different value in every byte.
const GATEWAY_ID: [u8; 32] = [
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
    0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
];

/// The lock script of a commitment to [`GATEWAY_ID`].
const SCRIPT_HEX: &str = "6a234d5201\
                      000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

fn commitment() -> MinerMeshCommitment {
    MinerMeshCommitment::new(GATEWAY_ID)
}

/// A typical miner reward output.
fn reward_output() -> Output {
    let pay_to_public_key_hash = hex::decode("76a914000000000000000000000000000000000000000088ac")
        .expect("the script is valid hex");
    Output::new(
        Amount::try_from(125_000_000).expect("the amount is within the money range"),
        Script::new(&pay_to_public_key_hash),
    )
}

/// A zero-value output with lock script `script`.
fn zero_value_output(script: &[u8]) -> Output {
    Output::new(Amount::zero(), Script::new(script))
}

fn commitment_script() -> Vec<u8> {
    commitment().to_output().lock_script.as_raw_bytes().to_vec()
}

#[test]
fn output_has_the_documented_bytes() {
    let output = commitment().to_output();

    assert_eq!(hex::encode(output.lock_script.as_raw_bytes()), SCRIPT_HEX);
    assert_eq!(output.value, Amount::<NonNegative>::zero());
    assert_eq!(
        commitment().payload().as_slice(),
        &output.lock_script.as_raw_bytes()[2..],
    );
    assert_eq!(commitment().payload().len(), 35);
    assert_eq!(
        output
            .zcash_serialize_to_vec()
            .expect("serializing to a vector never fails")
            .len(),
        46,
        "the output costs 46 bytes: an 8-byte value, a 1-byte length and the 37-byte script",
    );
}

#[test]
fn one_commitment_is_read_wherever_it_sits() {
    for outputs in [
        vec![commitment().to_output()],
        vec![commitment().to_output(), reward_output()],
        vec![reward_output(), commitment().to_output(), reward_output()],
        vec![reward_output(), reward_output(), commitment().to_output()],
    ] {
        assert_eq!(
            MinerMeshCommitment::from_coinbase_outputs(&outputs),
            Some(commitment()),
        );
    }
}

#[test]
fn outputs_without_a_commitment_name_no_gateway() {
    assert_eq!(MinerMeshCommitment::from_coinbase_outputs(&[]), None);
    assert_eq!(
        MinerMeshCommitment::from_coinbase_outputs(&[reward_output(), reward_output()]),
        None,
    );
}

#[test]
fn two_commitments_are_ambiguous() {
    let other = MinerMeshCommitment::new([0xff; 32]);

    for outputs in [
        vec![commitment().to_output(), other.to_output()],
        vec![commitment().to_output(), commitment().to_output()],
        vec![
            commitment().to_output(),
            reward_output(),
            other.to_output(),
            commitment().to_output(),
        ],
    ] {
        assert_eq!(MinerMeshCommitment::from_coinbase_outputs(&outputs), None);
    }
}

#[test]
fn another_version_is_not_a_commitment() {
    for version in [0x00, 0x02, 0xff] {
        let mut script = commitment_script();
        script[4] = version;

        assert_eq!(
            MinerMeshCommitment::from_coinbase_outputs(&[zero_value_output(&script)]),
            None,
        );
    }
}

#[test]
fn a_nonzero_value_is_not_a_commitment() {
    let output = Output::new(
        Amount::try_from(1).expect("one zatoshi is within the money range"),
        Script::new(&commitment_script()),
    );

    assert_eq!(MinerMeshCommitment::from_coinbase_outputs(&[output]), None);
}

#[test]
fn another_script_shape_is_not_a_commitment() {
    let script = commitment_script();

    let mut wrong_tag = script.clone();
    wrong_tag[2] = b'm';

    let mut not_op_return = script.clone();
    not_op_return[0] = 0x00;

    // The same payload behind OP_PUSHDATA1, a second way to push 35 bytes.
    let mut long_form_push = vec![0x6a, 0x4c, 0x23];
    long_form_push.extend(&script[2..]);

    let mut trailing_byte = script.clone();
    trailing_byte.push(0x00);

    let truncated = &script[..script.len() - 1];

    for script in [
        wrong_tag.as_slice(),
        not_op_return.as_slice(),
        long_form_push.as_slice(),
        trailing_byte.as_slice(),
        truncated,
        &[],
    ] {
        assert_eq!(
            MinerMeshCommitment::from_coinbase_outputs(&[zero_value_output(script)]),
            None,
        );
    }
}

#[test]
fn outputs_that_are_not_commitments_do_not_make_one_ambiguous() {
    let mut other_version = commitment_script();
    other_version[4] = 0x02;

    let nonzero_value = Output::new(
        Amount::try_from(1).expect("one zatoshi is within the money range"),
        Script::new(&commitment_script()),
    );

    let outputs = [
        zero_value_output(&other_version),
        commitment().to_output(),
        nonzero_value,
    ];

    assert_eq!(
        MinerMeshCommitment::from_coinbase_outputs(&outputs),
        Some(commitment()),
    );
}
