//! Provider-aware UTXOs cached from blocks that have not reached state yet.

use std::collections::{hash_map::Entry, HashMap};

use zakura_chain::{block, transparent};

/// Caches each known UTXO together with every block that currently provides it.
///
/// Most outpoints have one provider, so [`UtxoProviders::Single`] avoids a separate
/// allocation for the common case. Competing blocks that contain the same transaction
/// promote the entry to [`UtxoProviders::Multiple`].
///
/// The single-provider representation retains its block hash so a later collision can
/// preserve both providers' metadata and cleanup can expose the exact surviving value.
/// A reference count paired with one value could not recover that value after its
/// provider was removed.
#[derive(Debug, Default)]
pub(super) struct UtxoProviderCache {
    providers_by_outpoint: HashMap<transparent::OutPoint, UtxoProviders>,
}

#[derive(Debug)]
enum UtxoProviders {
    Single {
        block_hash: block::Hash,
        utxo: transparent::Utxo,
    },
    Multiple(HashMap<block::Hash, transparent::Utxo>),
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum ProviderRemoval {
    Missing,
    ProvidersRemain,
    LastProviderRemoved,
}

impl UtxoProviderCache {
    /// Inserts or replaces the UTXO supplied by `block_hash` at `outpoint`.
    pub(super) fn insert(
        &mut self,
        block_hash: block::Hash,
        outpoint: transparent::OutPoint,
        utxo: transparent::Utxo,
    ) {
        match self.providers_by_outpoint.entry(outpoint) {
            Entry::Vacant(entry) => {
                entry.insert(UtxoProviders::Single { block_hash, utxo });
            }
            Entry::Occupied(mut entry) => entry.get_mut().insert(block_hash, utxo),
        }
    }

    /// Removes only the UTXO supplied by `block_hash` at `outpoint`.
    ///
    /// The outpoint remains cached while any other provider remains.
    pub(super) fn remove_provider(
        &mut self,
        block_hash: &block::Hash,
        outpoint: &transparent::OutPoint,
    ) {
        debug_assert!(
            self.providers_by_outpoint
                .get(outpoint)
                .is_some_and(|providers| providers.contains(block_hash)),
            "provider ownership exists because callers remove outputs from the same registered block that inserted them"
        );

        let Entry::Occupied(mut entry) = self.providers_by_outpoint.entry(*outpoint) else {
            return;
        };

        match entry.get_mut().remove(block_hash) {
            ProviderRemoval::Missing | ProviderRemoval::ProvidersRemain => {}
            ProviderRemoval::LastProviderRemoved => {
                entry.remove();
            }
        }
    }

    /// Returns a UTXO supplied by any live provider for `outpoint`.
    ///
    /// When multiple providers remain, provider selection is arbitrary and callers
    /// must not depend on which provider's metadata is returned.
    pub(super) fn get(&self, outpoint: &transparent::OutPoint) -> Option<&transparent::Utxo> {
        self.providers_by_outpoint
            .get(outpoint)
            .map(UtxoProviders::get)
    }

    /// Returns the number of distinct cached outpoints.
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.providers_by_outpoint.len()
    }

    /// Returns `true` when there are no cached outpoints.
    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.providers_by_outpoint.is_empty()
    }

    /// Removes every cached outpoint and provider.
    pub(super) fn clear(&mut self) {
        self.providers_by_outpoint.clear();
    }

    /// Shrinks the distinct-outpoint index to fit its current contents.
    pub(super) fn shrink_to_fit(&mut self) {
        self.providers_by_outpoint.shrink_to_fit();
    }
}

impl UtxoProviders {
    fn contains(&self, block_hash: &block::Hash) -> bool {
        match self {
            Self::Single {
                block_hash: current_hash,
                ..
            } => current_hash == block_hash,
            Self::Multiple(providers) => providers.contains_key(block_hash),
        }
    }

    fn insert(&mut self, block_hash: block::Hash, utxo: transparent::Utxo) {
        match self {
            Self::Single {
                block_hash: current_hash,
                utxo: current_utxo,
            } if *current_hash == block_hash => {
                *current_utxo = utxo;
            }
            Self::Single {
                block_hash: current_hash,
                utxo: current_utxo,
            } => {
                *self = Self::Multiple(HashMap::from([
                    (*current_hash, current_utxo.clone()),
                    (block_hash, utxo),
                ]));
            }
            Self::Multiple(providers) => {
                providers.insert(block_hash, utxo);
            }
        }
    }

    /// Removes `block_hash` and reports the resulting provider state.
    fn remove(&mut self, block_hash: &block::Hash) -> ProviderRemoval {
        match self {
            Self::Single {
                block_hash: current_hash,
                ..
            } if current_hash == block_hash => ProviderRemoval::LastProviderRemoved,
            Self::Single { .. } => ProviderRemoval::Missing,
            Self::Multiple(providers) => {
                if providers.remove(block_hash).is_none() {
                    return ProviderRemoval::Missing;
                }

                if providers.len() == 1 {
                    let (remaining_hash, remaining_utxo) = providers
                        .drain()
                        .next()
                        .expect("one UTXO provider remains after checking the provider count");
                    *self = Self::Single {
                        block_hash: remaining_hash,
                        utxo: remaining_utxo,
                    };
                }

                ProviderRemoval::ProvidersRemain
            }
        }
    }

    fn get(&self) -> &transparent::Utxo {
        match self {
            Self::Single { utxo, .. } => utxo,
            Self::Multiple(providers) => providers
                .values()
                .next()
                .expect("multiple UTXO providers is never empty"),
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(debug_assertions)]
    use std::panic::{catch_unwind, AssertUnwindSafe};

    use zakura_chain::{amount::Amount, transaction};

    use super::*;

    #[test]
    fn three_providers_promote_and_collapse_provider_storage() {
        let outpoint = transparent::OutPoint {
            hash: transaction::Hash([0x49; 32]),
            index: 0,
        };
        let output = transparent::Output {
            value: Amount::zero(),
            lock_script: transparent::Script::new(&[]),
        };
        let provider_hashes = [
            block::Hash([1; 32]),
            block::Hash([2; 32]),
            block::Hash([3; 32]),
        ];
        let provider_utxos = [
            transparent::Utxo::new(output.clone(), block::Height(1), false),
            transparent::Utxo::new(output.clone(), block::Height(2), false),
            transparent::Utxo::new(output, block::Height(3), false),
        ];
        let mut cache = UtxoProviderCache::default();

        cache.insert(provider_hashes[0], outpoint, provider_utxos[0].clone());
        assert!(matches!(
            cache.providers_by_outpoint.get(&outpoint),
            Some(UtxoProviders::Single { block_hash, utxo })
                if *block_hash == provider_hashes[0] && *utxo == provider_utxos[0]
        ));

        cache.insert(provider_hashes[1], outpoint, provider_utxos[1].clone());
        cache.insert(provider_hashes[2], outpoint, provider_utxos[2].clone());
        let Some(UtxoProviders::Multiple(providers)) = cache.providers_by_outpoint.get(&outpoint)
        else {
            panic!("three UTXO providers use the multiple-provider representation");
        };
        assert_eq!(providers.len(), 3);
        for (hash, utxo) in provider_hashes.iter().zip(provider_utxos.iter()) {
            assert_eq!(providers.get(hash), Some(utxo));
        }

        cache.remove_provider(&provider_hashes[1], &outpoint);
        let Some(UtxoProviders::Multiple(providers)) = cache.providers_by_outpoint.get(&outpoint)
        else {
            panic!("two UTXO providers keep the multiple-provider representation");
        };
        assert_eq!(providers.len(), 2);
        assert_eq!(providers.get(&provider_hashes[0]), Some(&provider_utxos[0]));
        assert_eq!(providers.get(&provider_hashes[2]), Some(&provider_utxos[2]));

        cache.remove_provider(&provider_hashes[0], &outpoint);
        assert!(matches!(
            cache.providers_by_outpoint.get(&outpoint),
            Some(UtxoProviders::Single { block_hash, utxo })
                if *block_hash == provider_hashes[2] && *utxo == provider_utxos[2]
        ));

        cache.remove_provider(&provider_hashes[2], &outpoint);
        assert!(!cache.providers_by_outpoint.contains_key(&outpoint));
    }

    #[cfg(debug_assertions)]
    #[test]
    fn removing_unknown_provider_panics_without_changing_existing_providers() {
        let outpoint = transparent::OutPoint {
            hash: transaction::Hash([0x49; 32]),
            index: 0,
        };
        let output = transparent::Output {
            value: Amount::zero(),
            lock_script: transparent::Script::new(&[]),
        };
        let first_hash = block::Hash([1; 32]);
        let second_hash = block::Hash([2; 32]);
        let unknown_hash = block::Hash([3; 32]);
        let first_utxo = transparent::Utxo::new(output.clone(), block::Height(1), false);
        let second_utxo = transparent::Utxo::new(output, block::Height(2), false);
        let mut cache = UtxoProviderCache::default();
        cache.insert(first_hash, outpoint, first_utxo.clone());
        cache.insert(second_hash, outpoint, second_utxo.clone());

        let removal = catch_unwind(AssertUnwindSafe(|| {
            cache.remove_provider(&unknown_hash, &outpoint);
        }));

        assert!(removal.is_err());
        let Some(UtxoProviders::Multiple(providers)) = cache.providers_by_outpoint.get(&outpoint)
        else {
            panic!("the failed removal leaves both existing UTXO providers unchanged");
        };
        assert_eq!(providers.len(), 2);
        assert_eq!(providers.get(&first_hash), Some(&first_utxo));
        assert_eq!(providers.get(&second_hash), Some(&second_utxo));
    }

    #[cfg(debug_assertions)]
    #[test]
    fn removing_unknown_outpoint_panics_without_changing_existing_outpoints() {
        let existing_outpoint = transparent::OutPoint {
            hash: transaction::Hash([0x49; 32]),
            index: 0,
        };
        let unknown_outpoint = transparent::OutPoint {
            hash: transaction::Hash([0x50; 32]),
            index: 0,
        };
        let provider_hash = block::Hash([1; 32]);
        let utxo = transparent::Utxo::new(
            transparent::Output {
                value: Amount::zero(),
                lock_script: transparent::Script::new(&[]),
            },
            block::Height(1),
            false,
        );
        let mut cache = UtxoProviderCache::default();
        cache.insert(provider_hash, existing_outpoint, utxo.clone());

        let removal = catch_unwind(AssertUnwindSafe(|| {
            cache.remove_provider(&provider_hash, &unknown_outpoint);
        }));

        assert!(removal.is_err());
        assert_eq!(cache.get(&existing_outpoint), Some(&utxo));
        assert!(cache.get(&unknown_outpoint).is_none());
    }

    #[cfg(not(debug_assertions))]
    #[test]
    fn removing_unknown_ownership_is_a_release_no_op() {
        let existing_outpoint = transparent::OutPoint {
            hash: transaction::Hash([0x49; 32]),
            index: 0,
        };
        let unknown_outpoint = transparent::OutPoint {
            hash: transaction::Hash([0x50; 32]),
            index: 0,
        };
        let provider_hash = block::Hash([1; 32]);
        let unknown_hash = block::Hash([2; 32]);
        let utxo = transparent::Utxo::new(
            transparent::Output {
                value: Amount::zero(),
                lock_script: transparent::Script::new(&[]),
            },
            block::Height(1),
            false,
        );
        let mut cache = UtxoProviderCache::default();
        cache.insert(provider_hash, existing_outpoint, utxo.clone());

        cache.remove_provider(&unknown_hash, &existing_outpoint);
        cache.remove_provider(&provider_hash, &unknown_outpoint);

        assert_eq!(cache.get(&existing_outpoint), Some(&utxo));
        assert!(cache.get(&unknown_outpoint).is_none());
    }
}
