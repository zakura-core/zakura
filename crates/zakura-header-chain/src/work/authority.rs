//! Snapshot-bound authority and exact transport ownership for asynchronous work.

use std::num::NonZeroU64;

use crate::{BranchId, HeaderGeneration, VerifiedGeneration};

/// Header-generation and branch authority captured before asynchronous header work.
///
/// Header work authority omits global state versions and verified-body generations.
/// Neither value can authorize or stale pure header work.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct HeaderWorkAuthority {
    /// Selected-header generation that scheduled the work.
    pub header_generation: HeaderGeneration,
    /// Exact anchor/target branch identity.
    pub branch: BranchId,
}

impl HeaderWorkAuthority {
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
    /// The engine never rebases body-affecting repair authority.
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

#[cfg(test)]
mod tests {
    use zakura_chain::block;

    use super::*;

    #[test]
    fn domain_authority_binds_transport_identity_without_irrelevant_coordinates() {
        let authority = BodyWorkAuthority {
            header: HeaderWorkAuthority {
                header_generation: HeaderGeneration::new(2),
                branch: BranchId::new(block::Hash([4; 32]), block::Hash([5; 32])),
            },
            verified_generation: VerifiedGeneration::new(3),
        };
        let request_id = NonZeroU64::new(7).expect("seven is nonzero");
        let owner = authority.bind(6, request_id);
        assert_eq!(owner.authority, authority);
        assert_eq!(owner.header_generation, authority.header_generation);
        assert_eq!(owner.verified_generation, authority.verified_generation);
        assert_eq!(owner.branch, authority.branch);
        assert_eq!(owner.session_id, 6);
        assert_eq!(owner.request_id, request_id);
    }
}
