//! The v7 bundle type registry of ZIP 248, for the pairs this node represents.

/// A `(bundleType, bundleVariant)` pair.
pub(super) type BundleId = (u32, u32);

/// The transparent bundle.
pub(super) const TRANSPARENT: BundleId = (0, 0);
/// The coinbase bundle.
pub(super) const COINBASE: BundleId = (1, 0);
/// The Sapling bundle.
pub(super) const SAPLING: BundleId = (2, 0);
/// The Orchard bundle, acting on the Orchard pool.
pub(super) const ORCHARD: BundleId = (3, 0);
/// The Ironwood bundle, acting on the Ironwood pool.
pub(super) const IRONWOOD: BundleId = (4, 0);
/// The transaction fee bundle of ZIP 2002.
pub(super) const FEE: BundleId = (5, 0);
/// The NSM field of ZIP 233.
pub(super) const NSM: BundleId = (6, 0);

/// A bundle that ZIP 248 registers for v7 transactions, in bundle type order.
///
/// Registration alone does not activate a bundle: full validators check that each bundle
/// a transaction contains is active in the consensus epoch of its block.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(
    any(test, feature = "proptest-impl"),
    derive(proptest_derive::Arbitrary)
)]
pub enum BundleKind {
    /// The transparent bundle.
    Transparent,
    /// The coinbase bundle.
    Coinbase,
    /// The Sapling bundle.
    Sapling,
    /// The Orchard bundle.
    Orchard,
    /// The Ironwood bundle.
    Ironwood,
    /// The transaction fee bundle of ZIP 2002.
    Fee,
    /// The NSM field of ZIP 233.
    Nsm,
}

impl BundleKind {
    /// Every registered bundle, in bundle type order.
    pub(super) const ALL: [BundleKind; 7] = [
        BundleKind::Transparent,
        BundleKind::Coinbase,
        BundleKind::Sapling,
        BundleKind::Orchard,
        BundleKind::Ironwood,
        BundleKind::Fee,
        BundleKind::Nsm,
    ];

    /// Returns the `(bundleType, bundleVariant)` pair that identifies this bundle.
    pub fn id(self) -> (u32, u32) {
        match self {
            BundleKind::Transparent => TRANSPARENT,
            BundleKind::Coinbase => COINBASE,
            BundleKind::Sapling => SAPLING,
            BundleKind::Orchard => ORCHARD,
            BundleKind::Ironwood => IRONWOOD,
            BundleKind::Fee => FEE,
            BundleKind::Nsm => NSM,
        }
    }

    /// Returns the registered bundle that `id` identifies.
    pub(super) fn of(id: BundleId) -> Option<BundleKind> {
        BundleKind::ALL.into_iter().find(|kind| kind.id() == id)
    }
}

/// Which maps of a v7 transaction may contain an entry for a registered pair.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Registration {
    pub(super) value_pool_delta: bool,
    pub(super) effecting: bool,
    pub(super) authorizing: bool,
}

const ALL_MAPS: Registration = Registration {
    value_pool_delta: true,
    effecting: true,
    authorizing: true,
};

const DELTA_AND_EFFECTS: Registration = Registration {
    authorizing: false,
    ..ALL_MAPS
};

const DELTA_ONLY: Registration = Registration {
    value_pool_delta: true,
    effecting: false,
    authorizing: false,
};

/// The registry: every registered pair, in bundle type order, with the maps it may
/// appear in. Each pair has its own bundle type.
pub(super) const REGISTRY: [(BundleId, Registration); 7] = [
    (TRANSPARENT, ALL_MAPS),
    (COINBASE, DELTA_AND_EFFECTS),
    (SAPLING, ALL_MAPS),
    (ORCHARD, ALL_MAPS),
    (IRONWOOD, ALL_MAPS),
    (FEE, DELTA_ONLY),
    (NSM, DELTA_ONLY),
];

/// The most entries a decodable map can have: at most one per bundle type, and only
/// registered types decode.
// `usize` to `u64` is lossless on every target Zakura builds for.
pub(super) const MAX_MAP_ENTRIES: u64 = REGISTRY.len() as u64;

/// Returns the registry entry for `bundle`, or `None` if the pair is not registered for
/// v7.
///
/// Registration alone does not activate a pair: consensus checks that separately.
pub(super) fn registration(bundle: BundleId) -> Option<Registration> {
    REGISTRY
        .iter()
        .find(|(registered, _)| *registered == bundle)
        .map(|(_, registration)| *registration)
}
