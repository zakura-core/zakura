//! The JoinSplit key and signature types serialize exactly like the `ed25519-zebra` types
//! they replaced, so stored and exported data keeps its format.

use proptest::prelude::*;

use crate::serialization::bincode_format;

use super::super::{JoinSplitPubKey, JoinSplitSig};

proptest! {
    #[test]
    fn pub_key_serializes_like_ed25519_zebra(bytes in any::<[u8; 32]>()) {
        let new = JoinSplitPubKey(bytes);
        let old = ed25519_zebra::VerificationKeyBytes::from(bytes);

        prop_assert_eq!(serde_json::to_string(&new)?, serde_json::to_string(&old)?);
        prop_assert_eq!(
            bincode_format::serialize(&new).expect("a key serializes"),
            bincode_format::serialize(&old).expect("a key serializes"),
        );

        let json = serde_json::to_string(&old)?;
        prop_assert_eq!(serde_json::from_str::<JoinSplitPubKey>(&json)?, new);
        let encoded = bincode_format::serialize(&old).expect("a key serializes");
        prop_assert_eq!(
            bincode_format::deserialize::<JoinSplitPubKey>(&encoded).expect("a key deserializes"),
            new,
        );
    }

    #[test]
    fn sig_serializes_like_ed25519(bytes in proptest::collection::vec(any::<u8>(), 64)) {
        let bytes: [u8; 64] = bytes.try_into().expect("the strategy makes 64 bytes");
        let new = JoinSplitSig(bytes);
        let old = ed25519_zebra::Signature::from(bytes);

        prop_assert_eq!(serde_json::to_string(&new)?, serde_json::to_string(&old)?);
        prop_assert_eq!(
            bincode_format::serialize(&new).expect("a signature serializes"),
            bincode_format::serialize(&old).expect("a signature serializes"),
        );

        let json = serde_json::to_string(&old)?;
        prop_assert_eq!(serde_json::from_str::<JoinSplitSig>(&json)?, new);
        let encoded = bincode_format::serialize(&old).expect("a signature serializes");
        prop_assert_eq!(
            bincode_format::deserialize::<JoinSplitSig>(&encoded).expect("a signature deserializes"),
            new,
        );
    }
}
