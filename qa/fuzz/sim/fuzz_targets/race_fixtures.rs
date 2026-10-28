#![no_main]

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    zakura_sim_fuzz::fuzz_fixtures(input);
});
