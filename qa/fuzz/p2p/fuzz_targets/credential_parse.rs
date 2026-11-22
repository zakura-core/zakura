#![no_main]

use libfuzzer_sys::fuzz_target;
use zakura_p2p_fuzz::{largest_allocation, MeteredAllocator};
use zakura_quic::fuzz::replay_credential_bytes;

#[global_allocator]
static ALLOCATOR: MeteredAllocator = MeteredAllocator;

fuzz_target!(|data: &[u8]| {
    replay_credential_bytes(data, &mut largest_allocation);
});
