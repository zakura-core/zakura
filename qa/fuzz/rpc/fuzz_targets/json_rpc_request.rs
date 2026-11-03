#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    zakura_rpc::server::fuzz::json_rpc_request(input);
});
