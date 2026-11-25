#![no_main]

use libfuzzer_sys::fuzz_target;
use zakura_network::zakura::bpv1::fuzz::replay_bpv1_wire_bytes;
use zakura_p2p_fuzz::{largest_allocation, MeteredAllocator};

#[global_allocator]
static ALLOCATOR: MeteredAllocator = MeteredAllocator;

// Every BPv1 frame decoder on one arbitrary frame: the family decode stays
// within its heap bound and re-encodes canonically, the stream decoders
// close with the row the frame breaks, and only honest parts prove.
fuzz_target!(|data: &[u8]| {
    replay_bpv1_wire_bytes(data, &mut largest_allocation);
});
