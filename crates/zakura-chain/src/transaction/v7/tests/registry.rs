//! The registry table is the one source of which pairs decode, where, and with which
//! value pool delta.

use super::super::{
    deltas::PoolDeltas,
    registry::{registration, BundleId, REGISTRY},
};

#[test]
fn registration_reads_the_registry_table() {
    for (bundle, expected) in REGISTRY {
        assert_eq!(registration(bundle), Some(expected), "{bundle:?}");
    }
    for unregistered in [(7, 0), (0, 1), (5, 1), (u32::MAX, 0)] {
        assert_eq!(registration(unregistered), None, "{unregistered:?}");
    }
}

#[test]
fn delta_fields_match_the_registry_exactly() {
    let with_deltas: Vec<BundleId> = REGISTRY
        .iter()
        .filter(|(_, registration)| registration.value_pool_delta)
        .map(|(bundle, _)| *bundle)
        .collect();
    let fields: Vec<BundleId> = PoolDeltas::default()
        .entries_mut()
        .into_iter()
        .map(|(bundle, _)| bundle)
        .collect();
    assert_eq!(fields, with_deltas);

    // Each field is distinct: setting one delta shows up under its own bundle only.
    for (index, bundle) in with_deltas.iter().enumerate() {
        let mut deltas = PoolDeltas::default();
        let value = i64::try_from(index + 1).expect("few bundles");
        *deltas.get_mut(*bundle).expect("registered") = value.try_into().expect("in range");
        let nonzero = deltas.nonzero();
        assert_eq!(nonzero.len(), 1, "{bundle:?}");
        assert_eq!(nonzero[0].0, *bundle);
        assert_eq!(i64::from(nonzero[0].1), value);
    }
}
