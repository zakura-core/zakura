use hex::FromHex;
use proptest::prelude::*;

use super::{decode, encode, HexData};
use crate::server::jsonrpc::{Id, MethodResponse};

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
        b"0x",
        b"0X",
        b"0x0",
        b"0x00",
        b"0X00",
        b"x0",
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

/// Hex written into a response as finished JSON still counts toward the response size limit.
#[test]
fn response_hex_counts_toward_the_response_size_limit() {
    let data = HexData(vec![0xab; 100]);
    assert!(!MethodResponse::response(Id::Number(1), &Ok(&data), 300).is_error());
    assert!(MethodResponse::response(Id::Number(1), &Ok(&data), 200).is_error());
}

proptest! {
    /// A method's response carries the same hex string that other serializers write.
    #[test]
    fn response_hex_matches_the_plain_string(bytes in prop::collection::vec(any::<u8>(), 0..300)) {
        let data = HexData(bytes);
        let response = MethodResponse::response(Id::Number(1), &Ok(&data), usize::MAX);
        let plain = serde_json::to_string(&data).expect("hex data serializes");

        prop_assert_eq!(&plain, &format!("\"{}\"", hex::encode(&data.0)));
        prop_assert_eq!(
            response.as_json().get(),
            format!(r#"{{"jsonrpc":"2.0","id":1,"result":{plain}}}"#)
        );
    }

    #[test]
    fn decode_matches_hex_crate(input in prop::collection::vec(
        prop::sample::select(b"0123456789abcdefABCDEFgG \xff".to_vec()),
        0..64,
    )) {
        prop_assert_eq!(decode(&input), reference(&input));
    }

    /// Long inputs reach the SIMD chunks, so an invalid byte must be found in any chunk.
    #[test]
    fn decode_matches_hex_crate_on_long_inputs(
        bytes in prop::collection::vec(any::<u8>(), 0..300),
        corruption in prop::option::of((any::<prop::sample::Index>(), prop::sample::select(b"gGxX \xff/:@`".to_vec()))),
    ) {
        let mut input = hex::encode(&bytes).into_bytes();
        if let Some((index, byte)) = corruption {
            if !input.is_empty() {
                let index = index.index(input.len());
                input[index] = byte;
            }
        }
        prop_assert_eq!(decode(&input), reference(&input));
    }

    #[test]
    fn decode_inverts_encode(bytes in prop::collection::vec(any::<u8>(), 0..300)) {
        prop_assert_eq!(decode(hex::encode(&bytes).as_bytes()), Ok(bytes.clone()));
        prop_assert_eq!(decode(hex::encode_upper(&bytes).as_bytes()), Ok(bytes.clone()));
        prop_assert_eq!(encode(&bytes), hex::encode(&bytes));
    }
}
