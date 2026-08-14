//! Stable identities for headers, evidence, operators, peers, and branches.

use std::fmt;

use zakura_chain::block;

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
