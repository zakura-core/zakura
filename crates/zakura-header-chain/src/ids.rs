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

/// Header-generation and branch authority captured before asynchronous header work.
///
/// Global state versions and verified-body generations are deliberately absent:
/// neither can authorize or stale pure header work.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct HeaderWorkAuthority {
    /// Selected-header generation that scheduled the work.
    pub header_generation: HeaderGeneration,
    /// Exact anchor/target branch identity.
    pub branch: BranchId,
}

impl HeaderWorkAuthority {
    /// Capture authority for one exact advertised header target.
    pub fn for_target(snapshot: &crate::EngineSnapshot, target_tip_hash: block::Hash) -> Self {
        Self {
            header_generation: snapshot.header_generation,
            branch: BranchId::new(snapshot.frontiers.finalized.hash, target_tip_hash),
        }
    }

    /// Bind this authority to the exact transport session and request.
    pub const fn bind(self, session_id: u64, request_id: NonZeroU64) -> HeaderWorkOwner {
        HeaderWorkOwner {
            authority: self,
            session_id,
            request_id,
        }
    }
}

/// Header authority plus the verified generation required by body-affecting work.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct BodyWorkAuthority {
    /// Header branch on which the body work is valid.
    pub header: HeaderWorkAuthority,
    /// Verified-body generation that scheduled the work.
    pub verified_generation: VerifiedGeneration,
}

impl BodyWorkAuthority {
    /// Capture body-affecting authority from one atomic committed snapshot.
    pub fn for_snapshot(snapshot: &crate::EngineSnapshot) -> Self {
        Self {
            header: HeaderWorkAuthority {
                header_generation: snapshot.header_generation,
                branch: BranchId::new(
                    snapshot.frontiers.finalized.hash,
                    snapshot.frontiers.header_best.hash,
                ),
            },
            verified_generation: snapshot.verified_generation,
        }
    }

    /// Bind this authority to the exact transport session and request.
    pub const fn bind(self, session_id: u64, request_id: NonZeroU64) -> BodyWorkOwner {
        BodyWorkOwner {
            authority: self,
            session_id,
            request_id,
        }
    }
}

/// Exact owner of pure header work.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct HeaderWorkOwner {
    /// Durable authority for this attempt.
    pub authority: HeaderWorkAuthority,
    /// Transport session that owns the work.
    pub session_id: u64,
    /// Nonzero request identifier within that session.
    pub request_id: NonZeroU64,
}

impl HeaderWorkOwner {
    /// Return the durable authority shared by every item in this request.
    pub const fn authority(self) -> HeaderWorkAuthority {
        self.authority
    }

    /// Return the exact transport session.
    pub const fn session_id(self) -> u64 {
        self.session_id
    }

    /// Return the exact request identity.
    pub const fn request_id(self) -> NonZeroU64 {
        self.request_id
    }
}

impl std::ops::Deref for HeaderWorkOwner {
    type Target = HeaderWorkAuthority;

    fn deref(&self) -> &Self::Target {
        &self.authority
    }
}

/// Exact owner of body-affecting work.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct BodyWorkOwner {
    /// Durable authority for this attempt.
    pub authority: BodyWorkAuthority,
    /// Transport session that owns the work.
    pub session_id: u64,
    /// Nonzero request identifier within that session.
    pub request_id: NonZeroU64,
}

impl BodyWorkOwner {
    /// Return the durable authority shared by every item in this request.
    pub const fn authority(self) -> BodyWorkAuthority {
        self.authority
    }

    /// Return the header authority embedded in this body owner.
    pub const fn header_authority(self) -> HeaderWorkAuthority {
        self.authority.header
    }

    /// Return the exact transport session.
    pub const fn session_id(self) -> u64 {
        self.session_id
    }

    /// Return the exact request identity.
    pub const fn request_id(self) -> NonZeroU64 {
        self.request_id
    }
}

impl std::ops::Deref for BodyWorkOwner {
    type Target = BodyWorkAuthority;

    fn deref(&self) -> &Self::Target {
        &self.authority
    }
}

impl std::ops::Deref for BodyWorkAuthority {
    type Target = HeaderWorkAuthority;

    fn deref(&self) -> &Self::Target {
        &self.header
    }
}

/// Domain-discriminated owner used only where header sync can carry either a
/// normal header target or a verified-generation-bound auxiliary repair.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub enum HeaderSyncWorkOwner {
    /// Ordinary header target work.
    Header(HeaderWorkOwner),
    /// Body-authorized selected-header auxiliary repair.
    BodyRepair(BodyWorkOwner),
}

impl HeaderSyncWorkOwner {
    /// Return the shared header authority.
    pub const fn header_authority(self) -> HeaderWorkAuthority {
        match self {
            Self::Header(owner) => owner.authority,
            Self::BodyRepair(owner) => owner.authority.header,
        }
    }

    /// Return verified authority only for a body repair.
    pub const fn body_authority(self) -> Option<BodyWorkAuthority> {
        match self {
            Self::Header(_) => None,
            Self::BodyRepair(owner) => Some(owner.authority),
        }
    }

    /// Return the header owner when this is ordinary target work.
    pub const fn header_owner(self) -> Option<HeaderWorkOwner> {
        match self {
            Self::Header(owner) => Some(owner),
            Self::BodyRepair(_) => None,
        }
    }

    /// Return the body owner when this is an auxiliary repair.
    pub const fn body_owner(self) -> Option<BodyWorkOwner> {
        match self {
            Self::Header(_) => None,
            Self::BodyRepair(owner) => Some(owner),
        }
    }

    /// Return the exact transport session.
    pub const fn session_id(self) -> u64 {
        match self {
            Self::Header(owner) => owner.session_id,
            Self::BodyRepair(owner) => owner.session_id,
        }
    }

    /// Return the exact request identity.
    pub const fn request_id(self) -> NonZeroU64 {
        match self {
            Self::Header(owner) => owner.request_id,
            Self::BodyRepair(owner) => owner.request_id,
        }
    }

    /// Rebind ordinary header work to state-proven current authority.
    ///
    /// Body-affecting repair authority is deliberately non-rebasable.
    pub(crate) const fn rebase_header(self, authority: HeaderWorkAuthority) -> Option<Self> {
        match self {
            Self::Header(owner) => Some(Self::Header(HeaderWorkOwner {
                authority,
                session_id: owner.session_id,
                request_id: owner.request_id,
            })),
            Self::BodyRepair(_) => None,
        }
    }
}

impl From<HeaderWorkOwner> for HeaderSyncWorkOwner {
    fn from(owner: HeaderWorkOwner) -> Self {
        Self::Header(owner)
    }
}

impl From<BodyWorkOwner> for HeaderSyncWorkOwner {
    fn from(owner: BodyWorkOwner) -> Self {
        Self::BodyRepair(owner)
    }
}
