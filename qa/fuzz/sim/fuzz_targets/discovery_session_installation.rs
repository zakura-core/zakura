#![no_main]

static ENTRY: zakura_sim::harness::HarnessEntry = zakura_sim::harness::discovery_machine::STATE_ENTRY;

libfuzzer_sys::fuzz_target!(init: zakura_sim_fuzz::unwind_task_panics(), |input: &[u8]| {
    zakura_sim_fuzz::fuzz_one(&ENTRY, input);
});
