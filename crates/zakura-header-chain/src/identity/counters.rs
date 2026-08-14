//! Monotonic counters that bind committed state and asynchronous generations.

use thiserror::Error;

/// A version or generation counter reached `u64::MAX`.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
#[error("header-chain {counter} counter is exhausted at u64::MAX")]
pub struct CounterExhausted {
    counter: &'static str,
}

macro_rules! counter_type {
    ($name:ident, $label:literal, $docs:literal) => {
        #[doc = $docs]
        #[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(u64);

        impl $name {
            /// Construct a counter from its durable integer representation.
            pub const fn new(value: u64) -> Self {
                Self(value)
            }

            /// Return the durable integer representation.
            pub const fn get(self) -> u64 {
                self.0
            }

            /// Return the next counter value, failing closed at `u64::MAX`.
            pub fn checked_next(self) -> Result<Self, CounterExhausted> {
                self.0
                    .checked_add(1)
                    .map(Self)
                    .ok_or(CounterExhausted { counter: $label })
            }
        }
    };
}

counter_type!(
    StateVersion,
    "state version",
    "Monotonic version of the complete durable header-chain state."
);
counter_type!(
    HeaderGeneration,
    "header generation",
    "Generation that owns selected-header forward work."
);
counter_type!(
    VerifiedGeneration,
    "verified generation",
    "Generation that owns verified-body forward work."
);
counter_type!(
    FinalityEpoch,
    "finality epoch",
    "Monotonic epoch of irreversible finality changes."
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_counters_fail_closed_at_exhaustion() {
        assert_eq!(
            StateVersion::new(8).checked_next(),
            Ok(StateVersion::new(9))
        );
        assert_eq!(
            HeaderGeneration::new(u64::MAX).checked_next(),
            Err(CounterExhausted {
                counter: "header generation"
            })
        );
        assert_eq!(
            VerifiedGeneration::new(u64::MAX).checked_next(),
            Err(CounterExhausted {
                counter: "verified generation"
            })
        );
        assert_eq!(
            FinalityEpoch::new(u64::MAX).checked_next(),
            Err(CounterExhausted {
                counter: "finality epoch"
            })
        );
    }
}
