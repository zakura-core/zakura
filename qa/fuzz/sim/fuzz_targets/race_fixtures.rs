#![no_main]

libfuzzer_sys::fuzz_target!(init: zakura_sim_fuzz::unwind_task_panics(), |input: &[u8]| {
    zakura_sim_fuzz::fuzz_fixtures(input);
});
