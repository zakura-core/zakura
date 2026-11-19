//! Shared scaffolding for pure-machine histories.
//!
//! A pure machine is the one encoding of its rules, so its tests drive it
//! directly. Each machine target is one `replay_*_bytes` function that:
//!
//! 1. decodes at most [`MAX_HISTORY_BYTES`] fuzz bytes into a history of
//!    inputs, with a decoder version named in its doc;
//! 2. applies each input to the production machine;
//! 3. checks the machine's invariants after every input, and checks that a
//!    drain returns every resource at the end;
//! 4. compares outputs only with facts that follow from the inputs it chose:
//!    what it sent, how many, in what order;
//! 5. panics with the broken property's ID, and otherwise returns counters
//!    that record which transitions the history reached.
//!
//! The target never re-derives a decision the machine makes. Named scenarios
//! pin each rule. Proptest and `qa/fuzz/p2p` call the same function, so one
//! decoder serves both, and [`machine_target_tests!`] generates the tests every
//! target shares.

/// Most input bytes one history reads. A proptest case and a fuzz input cost
/// the same.
pub const MAX_HISTORY_BYTES: usize = 512;

/// Run `check`, a machine's own invariant check, and prefix any panic it
/// raises with `property`, so the simulator reads it as that property's
/// failure. Wrap only the invariant call: a panic in the driver around it
/// stays a driver panic.
pub(crate) fn with_property(property: &str, check: impl FnOnce()) {
    if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(check)) {
        let message = panic_text(payload.as_ref());
        if message.starts_with(property) {
            std::panic::resume_unwind(payload);
        }
        std::panic::resume_unwind(Box::new(format!("{property}: {message}")));
    }
}

/// A panic payload's text.
pub(crate) fn panic_text(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            payload
                .downcast_ref::<&str>()
                .map(|text| (*text).to_owned())
        })
        .unwrap_or_else(|| "non-string panic payload".to_owned())
}

/// The tests every machine target shares: its witness reaches the expected
/// counters, empty input reaches nothing, and random histories keep every
/// property.
///
/// ```ignore
/// machine_target_tests! {
///     replay: replay_block_query_bytes,
///     target: "block_query",
///     decoder: 1,
///     witness: WITNESS => BlockQueryReplaySummary { accepted: 3, .. },
/// }
/// ```
#[cfg(test)]
macro_rules! machine_target_tests {
    (
        replay: $replay:path,
        target: $target:literal,
        decoder: $decoder:literal,
        witness: $witness:expr => $expected:expr $(,)?
    ) => {
        #[test]
        #[allow(
            clippy::print_stdout,
            reason = "witnesses print applicability counters"
        )]
        fn witness_reaches_its_transitions() {
            let counters = $replay($witness);
            println!(
                "tier=Machine target={} decoder={} {:?}",
                $target, $decoder, counters
            );
            assert_eq!(counters, $expected);
        }

        #[test]
        fn empty_input_reaches_nothing() {
            assert_eq!($replay(&[]), Default::default());
        }

        proptest::proptest! {
            #[test]
            fn random_histories_keep_every_property(
                bytes in proptest::collection::vec(
                    proptest::prelude::any::<u8>(),
                    0..=$crate::zakura::machine_test::MAX_HISTORY_BYTES,
                )
            ) {
                $replay(&bytes);
            }
        }
    };
}

#[cfg(test)]
pub(crate) use machine_target_tests;
