use hex::FromHex;
use proptest::prelude::*;

use super::{decode, HexData};

/// The `hex` crate's result, which `decode` must match exactly.
fn reference(input: &[u8]) -> Result<Vec<u8>, hex::FromHexError> {
    Vec::<u8>::from_hex(input)
}

#[test]
fn decode_matches_hex_crate_on_edge_cases() {
    let cases: &[&[u8]] = &[
        b"",
        b"0",
        b"00ff",
        b"ABCDEF",
        b"aBcDeF09",
        b"0g",
        b"g0",
        b"gg",
        b"00 0",
        b"0000zz",
        "00é0".as_bytes(),
        b"\xff\xff",
        b"0123456789abcdefABCDEF",
    ];
    for &case in cases {
        assert_eq!(decode(case), reference(case), "input {case:?}");
    }
}

#[test]
fn hex_data_round_trips_through_json() {
    let data = HexData(vec![0x00, 0x7f, 0x80, 0xff]);
    let json = serde_json::to_string(&data).expect("hex data serializes");
    assert_eq!(json, "\"007f80ff\"");
    assert_eq!(
        serde_json::from_str::<HexData>(&json).expect("hex data deserializes"),
        data
    );
    assert!(serde_json::from_str::<HexData>("\"0g\"").is_err());
    assert!(serde_json::from_str::<HexData>("\"000\"").is_err());
    assert!(serde_json::from_str::<HexData>("12").is_err());
}

proptest! {
    #[test]
    fn decode_matches_hex_crate(input in prop::collection::vec(
        prop::sample::select(b"0123456789abcdefABCDEFgG \xff".to_vec()),
        0..64,
    )) {
        prop_assert_eq!(decode(&input), reference(&input));
    }

    #[test]
    fn decode_inverts_encode(bytes in prop::collection::vec(any::<u8>(), 0..64)) {
        prop_assert_eq!(decode(hex::encode(&bytes).as_bytes()), Ok(bytes.clone()));
        prop_assert_eq!(decode(hex::encode_upper(&bytes).as_bytes()), Ok(bytes));
    }
}
