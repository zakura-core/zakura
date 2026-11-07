//! The shared driver cases on the controlled block service (storage.md
//! ST4.6). zakurad runs the same cases on its real driver.

use zakura_storage::block::cases::{cases, run_controlled};

/// ST4.6: the controlled service sees what each case states, step by step.
#[test]
fn the_controlled_service_passes_every_shared_case() {
    for case in cases() {
        let observed = run_controlled(&case);
        for (index, ((step, seen), observed)) in case.steps.iter().zip(&observed).enumerate() {
            assert_eq!(
                &observed.summary(),
                seen,
                "{}: step {index}, {step:?}",
                case.name
            );
        }
    }
}
