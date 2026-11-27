//! The `mValuePoolDeltas` of a v7 transaction, by registered bundle.

use crate::amount::{Amount, NegativeAllowed};

use super::{
    fields::ValueDeltas,
    registry::{self, BundleId, REGISTRY},
    V7Ref,
};

/// Every value pool delta of a v7 transaction, zero for an absent entry.
///
/// A [`super::super::Transaction::V7`] keeps the Sapling, Orchard, and Ironwood deltas as
/// those bundles' value balances, and the others in [`ValueDeltas`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct PoolDeltas {
    pub(super) unbundled: ValueDeltas,
    pub(super) sapling: Amount<NegativeAllowed>,
    pub(super) orchard: Amount<NegativeAllowed>,
    pub(super) ironwood: Amount<NegativeAllowed>,
}

impl PoolDeltas {
    /// Pairs each registered bundle, in registry order, with the field that holds its
    /// delta. This is the only mapping from bundles to delta fields, and its length is
    /// the registry's.
    pub(super) fn entries_mut(
        &mut self,
    ) -> [(BundleId, &mut Amount<NegativeAllowed>); REGISTRY.len()] {
        [
            (registry::TRANSPARENT, &mut self.unbundled.transparent),
            (registry::COINBASE, &mut self.unbundled.coinbase),
            (registry::SAPLING, &mut self.sapling),
            (registry::ORCHARD, &mut self.orchard),
            (registry::IRONWOOD, &mut self.ironwood),
            (registry::FEE, &mut self.unbundled.fee),
            (registry::NSM, &mut self.unbundled.nsm),
        ]
    }

    /// Returns the field that holds `bundle`'s delta, or `None` if it has none.
    pub(super) fn get_mut(&mut self, bundle: BundleId) -> Option<&mut Amount<NegativeAllowed>> {
        self.entries_mut()
            .into_iter()
            .find(|(registered, _)| *registered == bundle)
            .map(|(_, delta)| delta)
    }

    /// Returns `mValuePoolDeltas` in encoding order, which omits zero deltas.
    pub(super) fn nonzero(mut self) -> Vec<(BundleId, Amount<NegativeAllowed>)> {
        self.entries_mut()
            .into_iter()
            .map(|(bundle, delta)| (bundle, *delta))
            .filter(|(_, delta)| *delta != Amount::<NegativeAllowed>::zero())
            .collect()
    }
}

impl V7Ref<'_> {
    /// Returns every value pool delta of the transaction.
    pub(super) fn pool_deltas(&self) -> PoolDeltas {
        let balance = |bundle: Option<&crate::orchard::ShieldedData>| {
            bundle.map_or(Amount::zero(), |bundle| bundle.value_balance)
        };
        PoolDeltas {
            unbundled: self.fields.value_deltas,
            sapling: self
                .sapling
                .map_or(Amount::zero(), |sapling| sapling.value_balance),
            orchard: balance(self.orchard),
            ironwood: balance(self.ironwood),
        }
    }
}
