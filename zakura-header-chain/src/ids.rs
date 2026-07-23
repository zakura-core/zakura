//! Stable identities and generation counters.

use std::{fmt, num::NonZeroU64};

use thiserror::Error;
use zakura_chain::block;

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

/// Hash-qualified identity of one admitted header.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct HeaderId(block::Hash);

impl HeaderId {
    /// Construct an identity from the header's raw internal hash.
    pub const fn new(hash: block::Hash) -> Self {
        Self(hash)
    }

    /// Return the identified header hash.
    pub const fn hash(self) -> block::Hash {
        self.0
    }
}

impl From<block::Hash> for HeaderId {
    fn from(hash: block::Hash) -> Self {
        Self::new(hash)
    }
}

/// Stable identifier for deduplicated validation or operator evidence.
#[derive(Copy, Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EvidenceId([u8; 32]);

impl EvidenceId {
    /// Construct an ID from a domain-separated evidence digest.
    pub const fn from_digest(digest: [u8; 32]) -> Self {
        Self(digest)
    }

    /// Return the opaque digest bytes.
    pub const fn digest(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for EvidenceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("EvidenceId").field(&self.0).finish()
    }
}

/// Stable identifier for one independently removable operator invalidation.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OperatorInvalidationId([u8; 16]);

impl OperatorInvalidationId {
    /// Construct an ID from its stable opaque bytes.
    pub const fn new(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Return the stable opaque bytes.
    pub const fn bytes(self) -> [u8; 16] {
        self.0
    }
}

/// Opaque stable digest of a peer identity and connection domain.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SourceId([u8; 32]);

impl SourceId {
    /// Construct a source ID from its stable digest.
    pub const fn from_digest(digest: [u8; 32]) -> Self {
        Self(digest)
    }

    /// Return the opaque digest bytes.
    pub const fn digest(self) -> [u8; 32] {
        self.0
    }
}

/// Exact branch identity, qualified by both anchor and target hashes.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct BranchId {
    /// Immutable branch anchor hash.
    pub anchor_hash: block::Hash,
    /// Exact target tip hash.
    pub target_tip_hash: block::Hash,
}

impl BranchId {
    /// Construct an exact branch identity.
    pub const fn new(anchor_hash: block::Hash, target_tip_hash: block::Hash) -> Self {
        Self {
            anchor_hash,
            target_tip_hash,
        }
    }
}

/// Durable generation and branch coordinates captured before asynchronous work is scheduled.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct WorkScope {
    /// Durable state version that scheduled the work.
    pub state_version: StateVersion,
    /// Selected-header generation that scheduled the work.
    pub header_generation: HeaderGeneration,
    /// Verified-body generation, when the work can affect verified state.
    pub verified_generation: Option<VerifiedGeneration>,
    /// Exact anchor/target branch identity.
    pub branch: BranchId,
}

impl WorkScope {
    /// Capture body-affecting work coordinates from one atomic committed snapshot.
    pub fn for_body_work(snapshot: &crate::EngineSnapshot) -> Self {
        Self {
            state_version: snapshot.state_version,
            header_generation: snapshot.header_generation,
            verified_generation: Some(snapshot.verified_generation),
            branch: BranchId::new(
                snapshot.frontiers.finalized.hash,
                snapshot.frontiers.header_best.hash,
            ),
        }
    }

    /// Bind these durable coordinates to the exact transport session and request.
    pub const fn bind(self, session_id: u64, request_id: NonZeroU64) -> WorkOwner {
        WorkOwner {
            state_version: self.state_version,
            header_generation: self.header_generation,
            verified_generation: self.verified_generation,
            branch: self.branch,
            session_id,
            request_id,
        }
    }
}

/// Complete ownership token attached to every asynchronous or staged work item.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct WorkOwner {
    /// Durable state version that scheduled the work.
    pub state_version: StateVersion,
    /// Selected-header generation that scheduled the work.
    pub header_generation: HeaderGeneration,
    /// Verified-body generation, when the work can affect verified state.
    pub verified_generation: Option<VerifiedGeneration>,
    /// Exact anchor/target branch identity.
    pub branch: BranchId,
    /// Transport session that owns the work.
    pub session_id: u64,
    /// Nonzero request identifier within that session.
    pub request_id: NonZeroU64,
}

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

    #[test]
    fn work_scope_binds_transport_identity_without_changing_durable_coordinates() {
        let scope = WorkScope {
            state_version: StateVersion::new(1),
            header_generation: HeaderGeneration::new(2),
            verified_generation: Some(VerifiedGeneration::new(3)),
            branch: BranchId::new(block::Hash([4; 32]), block::Hash([5; 32])),
        };
        let request_id = NonZeroU64::new(7).expect("seven is nonzero");
        let owner = scope.bind(6, request_id);
        assert_eq!(owner.state_version, scope.state_version);
        assert_eq!(owner.header_generation, scope.header_generation);
        assert_eq!(owner.verified_generation, scope.verified_generation);
        assert_eq!(owner.branch, scope.branch);
        assert_eq!(owner.session_id, 6);
        assert_eq!(owner.request_id, request_id);
    }
}
