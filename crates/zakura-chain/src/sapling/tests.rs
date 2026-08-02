#![allow(clippy::unwrap_in_result)]

mod preallocate;
mod prop;

#[test]
fn transmission_key_rejects_off_curve_bytes() {
    let _init_guard = zakura_test::init();

    assert!(
        super::keys::TransmissionKey::try_from([0xffu8; 32]).is_err(),
        "off-curve Sapling transmission keys must return an error rather than panic",
    );
}
