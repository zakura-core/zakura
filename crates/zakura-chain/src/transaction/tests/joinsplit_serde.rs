//! The JoinSplit key and signature types serialize exactly like the `ed25519-zebra` types
//! they replaced, so stored and exported data keeps its format.
//!
//! The vectors were recorded from `ed25519_zebra::VerificationKeyBytes` and
//! `ed25519::Signature` before #2171 replaced them. Each pairs the value's bytes, which are
//! also its `bincode_format` encoding, with its `serde_json` form.

use crate::serialization::bincode_format;

use super::super::{JoinSplitPubKey, JoinSplitSig};

/// `(bytes, serde_json)` recorded from `ed25519_zebra::VerificationKeyBytes`.
const RECORDED_PUB_KEYS: [(&str, &str); 2] = [
    (
        "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
        "[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31]",
    ),
    (
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        "[255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]",
    ),
];

/// `(bytes, serde_json)` recorded from `ed25519::Signature`.
const RECORDED_SIGS: [(&str, &str); 2] = [
    (
        "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
        "[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63]",
    ),
    (
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        "[255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]",
    ),
];

#[test]
fn pub_key_serializes_like_ed25519_zebra() {
    for (bytes, json) in RECORDED_PUB_KEYS {
        let key = JoinSplitPubKey(hex::decode(bytes).unwrap().try_into().unwrap());

        assert_eq!(serde_json::to_string(&key).unwrap(), json);
        assert_eq!(hex::encode(bincode_format::serialize(&key).unwrap()), bytes);

        assert_eq!(serde_json::from_str::<JoinSplitPubKey>(json).unwrap(), key);
        let encoded = hex::decode(bytes).unwrap();
        assert_eq!(
            bincode_format::deserialize::<JoinSplitPubKey>(&encoded).unwrap(),
            key
        );
    }
}

#[test]
fn sig_serializes_like_ed25519() {
    for (bytes, json) in RECORDED_SIGS {
        let sig = JoinSplitSig(hex::decode(bytes).unwrap().try_into().unwrap());

        assert_eq!(serde_json::to_string(&sig).unwrap(), json);
        assert_eq!(hex::encode(bincode_format::serialize(&sig).unwrap()), bytes);

        assert_eq!(serde_json::from_str::<JoinSplitSig>(json).unwrap(), sig);
        let encoded = hex::decode(bytes).unwrap();
        assert_eq!(
            bincode_format::deserialize::<JoinSplitSig>(&encoded).unwrap(),
            sig
        );
    }
}
