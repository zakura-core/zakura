//! Sapling verification keys without the proving parameters.

use std::io::Read;

use once_cell::sync::Lazy;
use sapling_crypto::circuit::{
    OutputParameters, OutputVerifyingKey, SpendParameters, SpendVerifyingKey,
};

// The Sapling key wrappers have no public deserialization constructor. Their
// parameter reader accepts a verifying key followed by five proving-query
// vectors. Encode those vectors as empty so only the verifying key is loaded.
// No proving-query points are bundled or retained.
const EMPTY_PROVING_QUERIES: [u8; 5 * std::mem::size_of::<u32>()] = [0; 20];

static VERIFYING_KEYS: Lazy<(SpendVerifyingKey, OutputVerifyingKey)> = Lazy::new(|| {
    let spend = SpendParameters::read(
        include_bytes!("spend.vk")
            .as_slice()
            .chain(EMPTY_PROVING_QUERIES.as_slice()),
        true,
    )
    .expect("the embedded Sapling spend verifying key has a valid encoding")
    .verifying_key();
    let output = OutputParameters::read(
        include_bytes!("output.vk")
            .as_slice()
            .chain(EMPTY_PROVING_QUERIES.as_slice()),
        true,
    )
    .expect("the embedded Sapling output verifying key has a valid encoding")
    .verifying_key();
    (spend, output)
});

/// Returns the process-wide Sapling spend and output verifying keys.
pub(super) fn verifying_keys() -> &'static (SpendVerifyingKey, OutputVerifyingKey) {
    &VERIFYING_KEYS
}
