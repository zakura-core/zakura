//! The Tachyon shielded data carried by V7 transactions.

use zcash_tachyon::TachyonBundle;

use crate::memory::{inline_size_bytes, vec_capacity_bytes, AttributedMemorySize};

/// A non-empty Tachyon bundle.
///
/// `TachyonBundle::NoBundle` is represented by `None` on [`super::Transaction::V7`].
#[derive(Clone, Debug)]
pub struct TachyonShieldedData(pub TachyonBundle);

impl TachyonShieldedData {
    /// Returns the bundle's actions.
    pub fn actions(&self) -> &[zcash_tachyon::Action] {
        match &self.0 {
            TachyonBundle::NoBundle => &[],
            TachyonBundle::Proven(bundle) => &bundle.actions,
            TachyonBundle::Adjunct(bundle) => &bundle.actions,
        }
    }

    /// Returns the bundle's value balance.
    pub fn value_balance(&self) -> zcash_tachyon::value::Balance {
        match &self.0 {
            TachyonBundle::NoBundle => zcash_tachyon::value::Balance::ZERO,
            TachyonBundle::Proven(bundle) => bundle.value_balance,
            TachyonBundle::Adjunct(bundle) => bundle.value_balance,
        }
    }
}

impl From<TachyonBundle> for TachyonShieldedData {
    fn from(bundle: TachyonBundle) -> Self {
        Self(bundle)
    }
}

impl PartialEq for TachyonShieldedData {
    fn eq(&self, other: &Self) -> bool {
        let mut self_bytes = Vec::new();
        let mut other_bytes = Vec::new();
        self.0
            .write(&mut self_bytes)
            .expect("serializing a Tachyon bundle into a Vec is infallible");
        other
            .0
            .write(&mut other_bytes)
            .expect("serializing a Tachyon bundle into a Vec is infallible");
        self_bytes == other_bytes
    }
}

impl Eq for TachyonShieldedData {}

impl AttributedMemorySize for TachyonShieldedData {
    fn attributed_memory_size_bytes(&self) -> u64 {
        let bundle = match &self.0 {
            TachyonBundle::NoBundle => return 0,
            TachyonBundle::Proven(bundle) => bundle,
            TachyonBundle::Adjunct(bundle) => {
                return vec_capacity_bytes(&bundle.actions)
                    .saturating_add(vec_capacity_bytes(&bundle.memo));
            }
        };

        vec_capacity_bytes(&bundle.actions)
            .saturating_add(vec_capacity_bytes(&bundle.memo))
            .saturating_add(
                u64::try_from(bundle.stamp.tachygrams.len())
                    .unwrap_or(u64::MAX)
                    .saturating_mul(inline_size_bytes::<zcash_tachyon::Tachygram>()),
            )
            .saturating_add(
                u64::try_from(std::mem::size_of_val(bundle.stamp.proof.as_ref()))
                    .unwrap_or(u64::MAX),
            )
    }
}

#[cfg(any(test, feature = "proptest-impl"))]
impl serde::Serialize for TachyonShieldedData {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::Error as _;

        let mut bytes = Vec::new();
        self.0.write(&mut bytes).map_err(S::Error::custom)?;
        serde::Serialize::serialize(&bytes, serializer)
    }
}
