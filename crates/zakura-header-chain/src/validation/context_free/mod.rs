//! Context-free observable-header validation primitives.

mod commitment;
mod encoding;
mod future_time;
mod hash_filter;
mod height;
mod link;
mod pow_policy;
mod target;
mod trusted_anchor;

pub use commitment::validate_commitment_structure;
pub use encoding::{validate_encoding_version_hash, HeaderEncodingError};
pub use future_time::validate_future_time;
pub use hash_filter::{validate_hash_filter, HashFilterError};
pub use height::{infer_height, HeaderHeightError};
pub use link::{validate_link, HeaderLinkError};
pub use pow_policy::{PowPolicy, PowPolicyError};
pub use target::{validate_compact_target, CompactTargetError};
pub(crate) use trusted_anchor::validate_trusted_anchor_observables;

#[cfg(test)]
mod tests;
