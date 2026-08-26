#![no_main]

use libfuzzer_sys::fuzz_target;
use zakura_network::zakura::{
    control_payload_length_is_admissible, ZakuraControlAck, ZakuraControlHello,
    MAX_CONTROL_PAYLOAD_BYTES,
};

fuzz_target!(|bytes: &[u8]| {
    let Some(prefix) = bytes.get(..4) else {
        return;
    };
    let declared = u32::from_le_bytes(
        prefix
            .try_into()
            .expect("the four-byte control prefix has a fixed width"),
    );
    let admissible = control_payload_length_is_admissible(declared, u32::MAX);
    let declared_usize = usize::try_from(declared).expect("u32 lengths fit usize");
    assert_eq!(
        admissible,
        declared != 0 && declared_usize <= MAX_CONTROL_PAYLOAD_BYTES
    );

    let Some(payload) = bytes.get(4..4usize.saturating_add(declared_usize)) else {
        return;
    };

    if let Ok(hello) = ZakuraControlHello::decode(payload) {
        let canonical = hello
            .encode()
            .expect("every decoded hello has a canonical encoding");
        assert_eq!(canonical, payload);
        assert_eq!(
            ZakuraControlHello::decode(&canonical)
                .expect("a canonical hello decodes"),
            hello
        );
    }
    if let Ok(ack) = ZakuraControlAck::decode(payload) {
        let canonical = ack
            .encode()
            .expect("every decoded ack has a canonical encoding");
        assert_eq!(canonical, payload);
        assert_eq!(
            ZakuraControlAck::decode(&canonical).expect("a canonical ack decodes"),
            ack
        );
    }
});
