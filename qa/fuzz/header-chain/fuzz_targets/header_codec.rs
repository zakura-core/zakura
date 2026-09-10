#![no_main]

use libfuzzer_sys::fuzz_target;
use zakura_chain::parameters::Network;
use zakura_network::zakura::{
    AuxSchema, HeaderSyncCodec, HeaderSyncDecodeContext, MAX_HS_MESSAGE_BYTES, MAX_HS_RANGE,
    MSG_HS_GET_HEADERS, MSG_HS_HEADERS, MSG_HS_HEADERS_OUTCOME, MSG_HS_STATUS,
};

fuzz_target!(|bytes: &[u8]| {
    let codec = HeaderSyncCodec::new(
        Network::Mainnet,
        u32::try_from(MAX_HS_MESSAGE_BYTES).expect("protocol byte bound fits in u32"),
        MAX_HS_RANGE,
        1,
    );
    let context = HeaderSyncDecodeContext {
        max_header_count: MAX_HS_RANGE,
        requested_tree_aux_schema: AuxSchema::V1,
    };

    if let Ok(message) = codec.decode(bytes, Some(context)) {
        let canonical = codec
            .encode(&message)
            .expect("every decoded message must have a canonical encoding");
        let decoded = codec
            .decode(&canonical, Some(context))
            .expect("a canonical encoding must decode under the same bounds");
        assert_eq!(decoded, message);
    }

    if let Some(discriminant) = bytes.first().copied() {
        if ![
            MSG_HS_STATUS,
            MSG_HS_GET_HEADERS,
            MSG_HS_HEADERS,
            MSG_HS_HEADERS_OUTCOME,
        ]
        .contains(&discriminant)
        {
            assert!(
                codec.decode(bytes, Some(context)).is_err(),
                "a discriminator outside the fixed message set must fail"
            );
        }
    }
});
