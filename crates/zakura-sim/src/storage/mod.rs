//! Storage services as action sources.
//!
//! Each source offers its service's pending operations as
//! `storage:<service>:<id>:<outcome>` actions, one per outcome the service's
//! contract allows now, so the `Plan` picks when and how each completes. At
//! every observation it drains the service's facts, checks them and the
//! service's actual owner state against the service's owner model, and logs
//! both. [`HeaderStorage`] serves header sync's port, and [`BlockStorage`]
//! serves block sync's action channel and range reads.

use std::time::Duration;

use zakura_header_chain::{BodyWorkOwner, Frontier, HeaderWorkAuthority, SourceId};
use zakura_scheduler::SimInstant;

use crate::observe::{StorageBranch, StoragePoint, WorkOwner, WorkScope};

mod block;
mod header;

pub use block::BlockStorage;
pub(crate) use header::decode;
pub use header::{request, work_owner, HeaderStorage};

/// `t` plus `d`, saturating at the largest instant.
fn after(t: SimInstant, d: Duration) -> SimInstant {
    t.checked_add(d)
        .unwrap_or(SimInstant::from_start(Duration::MAX))
}

/// `bytes` as lowercase hex.
pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// `hash` as hex, in its byte order.
pub(crate) fn hash(hash: zakura_chain::block::Hash) -> String {
    hex(&hash.0)
}

/// `peer` as hex.
fn peer(peer: SourceId) -> String {
    hex(&peer.digest())
}

/// `frontier` in plain fields.
pub(crate) fn point(frontier: Frontier) -> StoragePoint {
    StoragePoint {
        height: frontier.height.0,
        hash: hash(frontier.hash),
    }
}

/// `authority` in plain fields.
pub(crate) fn scope(authority: HeaderWorkAuthority) -> WorkScope {
    WorkScope {
        header_generation: authority.header_generation.get(),
        branch: StorageBranch {
            anchor: hash(authority.branch.anchor_hash),
            target: hash(authority.branch.target_tip_hash),
        },
    }
}

/// A body owner in plain fields.
pub(crate) fn body_owner(owner: BodyWorkOwner) -> WorkOwner {
    WorkOwner {
        kind: "body-repair".to_owned(),
        scope: scope(owner.authority.header),
        verified_generation: Some(owner.authority.verified_generation.get()),
        body_work_epoch: Some(owner.authority.body_work_epoch.get()),
        session_id: owner.session_id,
        request_id: owner.request_id.get(),
    }
}
