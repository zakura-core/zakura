#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| {
    let first = zakura_header_chain::replay_fork_transition_bytes(bytes);
    let second = zakura_header_chain::replay_fork_transition_bytes(bytes);
    assert_eq!(first, second, "structured transition replay must be deterministic");
});
