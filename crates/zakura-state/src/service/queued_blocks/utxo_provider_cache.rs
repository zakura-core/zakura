//! Provider-aware UTXOs cached from blocks that have not reached state yet.

use std::collections::{hash_map::Entry, HashMap};

use zakura_chain::{block, transparent};

/// Caches each known UTXO together with every block that currently provides it.
///
/// Most outpoints have one provider, so [`UtxoProviders::Single`] avoids a separate
/// allocation for the common case. Competing blocks that contain the same transaction
/// promote the entry to [`UtxoProviders::Multiple`].
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
        let Entry::Occupied(mut entry) = self.providers_by_outpoint.entry(*outpoint) else {
            return;
        };

        if entry.get_mut().remove(block_hash) {
            entry.remove();
        }
    }

    /// Returns a UTXO supplied by any live provider for `outpoint`.
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

    /// Removes `block_hash` and returns `true` when no provider remains.
    fn remove(&mut self, block_hash: &block::Hash) -> bool {
        match self {
            Self::Single {
                block_hash: current_hash,
                ..
            } => current_hash == block_hash,
            Self::Multiple(providers) => {
                if providers.remove(block_hash).is_none() {
                    return false;
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

                false
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
}
