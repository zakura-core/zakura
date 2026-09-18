//! Blocks and block-related structures (heights, headers, etc.)

use std::{collections::HashMap, fmt, ops::Neg, sync::Arc};

use halo2::pasta::pallas;

use crate::{
    amount::{Amount, DeferredPoolBalanceChange, NegativeAllowed},
    block::merkle::{auth_digest_or_placeholder, AuthDataRoot},
    fmt::DisplayToDebug,
    ironwood,
    memory::{inline_size_bytes, vec_capacity_bytes, AttributedMemorySize},
    orchard,
    parameters::{subsidy::halving_block_subsidy, Network, NetworkUpgrade},
    sapling,
    serialization::TrustedPreallocate,
    sprout,
    transaction::Transaction,
    transparent,
    value_balance::{ValueBalance, ValueBalanceError},
};

mod commitment;
mod error;
mod hash;
mod header;
mod height;
mod serialize;

pub mod genesis;
pub mod merkle;

#[cfg(any(test, feature = "proptest-impl"))]
pub mod arbitrary;
#[cfg(any(test, feature = "bench", feature = "proptest-impl"))]
pub mod tests;

pub use commitment::{
    ChainHistoryBlockTxAuthCommitmentHash, ChainHistoryMmrRootHash, Commitment, CommitmentError,
    CHAIN_HISTORY_ACTIVATION_RESERVED,
};
pub use hash::Hash;
pub use header::{BlockTimeError, CountedHeader, Header, ZCASH_BLOCK_VERSION};
pub use height::{Height, HeightDiff, TryIntoHeight};
pub use serialize::{SerializedBlock, MAX_BLOCK_BYTES};

/// Re-assert the signed Zcash block-header version rule on an in-memory header.
///
/// Canonical deserialization already applies this check.
/// Observable header validators repeat the check for locally constructed headers.
pub fn validate_header_version(version: u32) -> Result<(), &'static str> {
    serialize::validate_header_version(version)
}

#[cfg(any(test, feature = "proptest-impl"))]
pub use arbitrary::LedgerState;

/// A Zcash block, containing a header and a list of transactions.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(any(test, feature = "proptest-impl"), derive(Serialize))]
pub struct Block {
    /// The block header, containing block metadata.
    pub header: Arc<Header>,
    /// The block transactions.
    pub transactions: Vec<Arc<Transaction>>,
}

impl fmt::Display for Block {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut fmter = f.debug_struct("Block");

        if let Some(height) = self.coinbase_height() {
            fmter.field("height", &height);
        }
        fmter.field("transactions", &self.transactions.len());
        fmter.field("hash", &DisplayToDebug(self.hash()));

        fmter.finish()
    }
}

impl Block {
    /// Returns a deterministic attributed size for this decoded block's
    /// Rust-visible object-graph payload.
    ///
    /// This includes the inline [`Block`], the [`Header`] pointee, the transaction
    /// vector's capacity, each [`Transaction`] pointee, and all nested heap
    /// allocation capacities. Shared `Arc` pointees are charged in full per
    /// logical block/reference, so this is not a unique-allocation or exact RSS
    /// measurement.
    ///
    /// This excludes allocator metadata and rounding, fragmentation, `Arc`
    /// control blocks, temporary allocations, external library/runtime
    /// overhead, and allocations made by downstream verifiers.
    pub fn attributed_memory_size_bytes(&self) -> u64 {
        self.transactions
            .iter()
            .map(|transaction| {
                inline_size_bytes::<Transaction>()
                    .saturating_add(transaction.attributed_memory_size_bytes())
            })
            .fold(
                inline_size_bytes::<Self>()
                    .saturating_add(inline_size_bytes::<Header>())
                    .saturating_add(vec_capacity_bytes(&self.transactions)),
                u64::saturating_add,
            )
    }

    /// Return the block height reported in the coinbase transaction, if any.
    ///
    /// Note
    ///
    /// Verified blocks have a valid height.
    pub fn coinbase_height(&self) -> Option<Height> {
        self.transactions
            .first()
            .and_then(|tx| tx.inputs().first())
            .and_then(|input| match input {
                transparent::Input::Coinbase { ref height, .. } => Some(*height),
                _ => None,
            })
    }

    /// Compute the hash of this block.
    pub fn hash(&self) -> Hash {
        Hash::from(self)
    }

    /// Get the parsed block [`Commitment`] for this block.
    ///
    /// The interpretation of the commitment depends on the
    /// configured `network`, and this block's height.
    ///
    /// Returns an error if this block does not have a block height,
    /// or if the commitment value is structurally invalid.
    pub fn commitment(&self, network: &Network) -> Result<Commitment, CommitmentError> {
        match self.coinbase_height() {
            None => Err(CommitmentError::MissingBlockHeight {
                block_hash: self.hash(),
            }),
            Some(height) => Commitment::from_bytes(*self.header.commitment_bytes, network, height),
        }
    }

    /// Check if the `network_upgrade` fields from each transaction in the block matches
    /// the network upgrade calculated from the `network` and block height.
    ///
    /// # Consensus
    ///
    /// > [NU5 onward] The nConsensusBranchId field MUST match the consensus branch ID used
    /// > for SIGHASH transaction hashes, as specified in [ZIP-244].
    ///
    /// <https://zips.z.cash/protocol/protocol.pdf#txnconsensus>
    ///
    /// [ZIP-244]: https://zips.z.cash/zip-0244
    #[allow(clippy::unwrap_in_result)]
    pub fn check_transaction_network_upgrade_consistency(
        &self,
        network: &Network,
    ) -> Result<(), error::BlockError> {
        let block_nu =
            NetworkUpgrade::current(network, self.coinbase_height().expect("a valid height"));

        if self
            .transactions
            .iter()
            .filter_map(|trans| trans.as_ref().network_upgrade())
            .any(|trans_nu| trans_nu != block_nu)
        {
            return Err(error::BlockError::WrongTransactionConsensusBranchId);
        }

        Ok(())
    }

    /// Access the [`sprout::Nullifier`]s from all transactions in this block.
    pub fn sprout_nullifiers(&self) -> impl Iterator<Item = &sprout::Nullifier> {
        self.transactions
            .iter()
            .flat_map(|transaction| transaction.sprout_nullifiers())
    }

    /// Access the [`sapling::Nullifier`]s from all transactions in this block.
    pub fn sapling_nullifiers(&self) -> impl Iterator<Item = &sapling::Nullifier> {
        self.transactions
            .iter()
            .flat_map(|transaction| transaction.sapling_nullifiers())
    }

    /// Access the [`orchard::Nullifier`]s from all transactions in this block.
    pub fn orchard_nullifiers(&self) -> impl Iterator<Item = &orchard::Nullifier> {
        self.transactions
            .iter()
            .flat_map(|transaction| transaction.orchard_nullifiers())
    }

    /// Access the [`ironwood::Nullifier`]s from all transactions in this block.
    pub fn ironwood_nullifiers(&self) -> impl Iterator<Item = &ironwood::Nullifier> {
        self.transactions
            .iter()
            .flat_map(|transaction| transaction.ironwood_nullifiers())
    }

    /// Access the [`sprout::NoteCommitment`]s from all transactions in this block.
    pub fn sprout_note_commitments(&self) -> impl Iterator<Item = &sprout::NoteCommitment> {
        self.transactions
            .iter()
            .flat_map(|transaction| transaction.sprout_note_commitments())
    }

    /// Access the [sapling note commitments](`sapling_crypto::note::ExtractedNoteCommitment`)
    /// from all transactions in this block.
    pub fn sapling_note_commitments(
        &self,
    ) -> impl Iterator<Item = &sapling_crypto::note::ExtractedNoteCommitment> {
        self.transactions
            .iter()
            .flat_map(|transaction| transaction.sapling_note_commitments())
    }

    /// Access the [orchard note commitments](pallas::Base) from all transactions in this block.
    pub fn orchard_note_commitments(&self) -> impl Iterator<Item = &pallas::Base> {
        self.transactions
            .iter()
            .flat_map(|transaction| transaction.orchard_note_commitments())
    }

    /// Access the Ironwood note commitments from all transactions in this block.
    pub fn ironwood_note_commitments(&self) -> impl Iterator<Item = &pallas::Base> {
        self.transactions
            .iter()
            .flat_map(|transaction| transaction.ironwood_note_commitments())
    }

    /// Count how many Sapling transactions exist in a block,
    /// i.e. transactions "where either of vSpendsSapling or vOutputsSapling is non-empty"
    /// <https://zips.z.cash/zip-0221#tree-node-specification>.
    pub fn sapling_transactions_count(&self) -> u64 {
        self.transactions
            .iter()
            .filter(|tx| tx.has_sapling_shielded_data())
            .count()
            .try_into()
            .expect("number of transactions must fit u64")
    }

    /// Count how many Orchard transactions exist in a block,
    /// i.e. transactions "where vActionsOrchard is non-empty."
    /// <https://zips.z.cash/zip-0221#tree-node-specification>.
    pub fn orchard_transactions_count(&self) -> u64 {
        self.transactions
            .iter()
            .filter(|tx| tx.has_orchard_shielded_data())
            .count()
            .try_into()
            .expect("number of transactions must fit u64")
    }

    /// Count how many Ironwood transactions exist in a block,
    /// i.e. transactions containing Ironwood shielded data.
    /// <https://zips.z.cash/zip-0221#tree-node-specification>.
    pub fn ironwood_transactions_count(&self) -> u64 {
        self.transactions
            .iter()
            .filter(|tx| tx.has_ironwood_shielded_data())
            .count()
            .try_into()
            .expect("number of transactions must fit u64")
    }

    /// Returns the overall chain value pool change in this block---the negative sum of the
    /// transaction value balances in this block.
    ///
    /// These are the changes in the transparent, Sprout, Sapling, Orchard, and
    /// Deferred chain value pools, as a result of this block.
    ///
    /// Positive values are added to the corresponding chain value pool and negative values are
    /// removed from the corresponding pool.
    ///
    /// <https://zebra.zfnd.org/dev/rfcs/0012-value-pools.html#definitions>
    ///
    /// The given `utxos` must contain the [`transparent::Utxo`]s of every input in this block,
    /// including UTXOs created by earlier transactions in this block. It can also contain unrelated
    /// UTXOs, which are ignored.
    ///
    /// Note that the chain value pool has the opposite sign to the transaction value pool.
    pub fn chain_value_pool_change(
        &self,
        network: &Network,
        utxos: &HashMap<transparent::OutPoint, transparent::Utxo>,
        deferred_pool_balance_change: Option<DeferredPoolBalanceChange>,
    ) -> Result<ValueBalance<NegativeAllowed>, ValueBalanceError> {
        self.chain_value_pool_change_from_utxos(
            network,
            deferred_pool_balance_change,
            |transaction| transaction.value_balance(utxos),
        )
    }

    /// Returns the overall chain value pool change using borrowed ordered UTXOs.
    ///
    /// The given `utxos` must contain the [`transparent::OrderedUtxo`]s of every
    /// input in this block. This includes UTXOs created by earlier transactions
    /// in the same block. The map can also contain unrelated UTXOs, which this
    /// method ignores.
    ///
    /// # Panics
    ///
    /// This method panics if `utxos` omits a transparent input's UTXO.
    pub fn chain_value_pool_change_from_ordered_utxos(
        &self,
        network: &Network,
        utxos: &HashMap<transparent::OutPoint, transparent::OrderedUtxo>,
        deferred_pool_balance_change: Option<DeferredPoolBalanceChange>,
    ) -> Result<ValueBalance<NegativeAllowed>, ValueBalanceError> {
        self.chain_value_pool_change_from_utxos(
            network,
            deferred_pool_balance_change,
            |transaction| transaction.value_balance_from_ordered_utxos(utxos),
        )
    }

    fn chain_value_pool_change_from_utxos<F>(
        &self,
        network: &Network,
        deferred_pool_balance_change: Option<DeferredPoolBalanceChange>,
        mut transaction_value_balance: F,
    ) -> Result<ValueBalance<NegativeAllowed>, ValueBalanceError>
    where
        F: FnMut(&Transaction) -> Result<ValueBalance<NegativeAllowed>, ValueBalanceError>,
    {
        // `Result<T, E>` implements `IntoIterator`, so a `flat_map(|t| t.value_balance(utxos))`
        // would silently drop transactions whose value balance returns `Err`. Use `try_fold`
        // to propagate the first error instead.
        let tx_pool_sum = self
            .transactions
            .iter()
            .try_fold(ValueBalance::<NegativeAllowed>::zero(), |acc, tx| {
                acc + transaction_value_balance(tx)?
            })?;

        let mut change = *tx_pool_sum.neg().set_deferred_amount(
            deferred_pool_balance_change
                .map(DeferredPoolBalanceChange::value)
                .unwrap_or_default(),
        );

        change.set_issuance_deficit_amount(self.issuance_deficit_change(network, &change)?);

        Ok(change)
    }

    /// Returns scheduled issuance minus issued value, starting at NU7 with no seed.
    ///
    /// Should historical unclaimed subsidy and fees seed this balance? We need guidance before including those funds. Update the baseline
    /// in `zakura-state/src/service/finalized_state/disk_format/upgrade/issuance_deficit_pool.rs`
    /// together with this rule and its accounting tests.
    fn issuance_deficit_change(
        &self,
        network: &Network,
        change: &ValueBalance<NegativeAllowed>,
    ) -> Result<Amount<NegativeAllowed>, ValueBalanceError> {
        let height = self
            .coinbase_height()
            .ok_or(ValueBalanceError::MissingCoinbaseHeight)?;

        if !NetworkUpgrade::Nu7
            .activation_height(network)
            .is_some_and(|start| height >= start)
        {
            return Ok(Amount::zero());
        }

        // Genesis contributes no issuance, including on networks without slow start.
        let scheduled = if height == Height(0) {
            Amount::zero()
        } else {
            halving_block_subsidy(height, network)
                .map_err(ValueBalanceError::ScheduledIssuance)?
                .constrain::<NegativeAllowed>()
                .map_err(ValueBalanceError::IssuanceDeficit)?
        };

        let issued = change.total().map_err(ValueBalanceError::IssuanceDeficit)?;

        (scheduled - issued).map_err(ValueBalanceError::IssuanceDeficit)
    }

    /// Compute the root of the authorizing data Merkle tree,
    /// as defined in [ZIP-244].
    ///
    /// [ZIP-244]: https://zips.z.cash/zip-0244
    pub fn auth_data_root(&self) -> AuthDataRoot {
        use rayon::prelude::*;

        // Compute each transaction's auth digest in parallel, and collect into a
        // Vec with the same ordering.
        self.transactions
            .par_iter()
            .map(|tx| auth_digest_or_placeholder(tx))
            .collect::<Vec<_>>()
            .into_iter()
            .collect::<AuthDataRoot>()
    }
}

impl<'a> From<&'a Block> for Hash {
    fn from(block: &'a Block) -> Hash {
        block.header.as_ref().into()
    }
}

/// The maximum number of `block::Hash` entries Zebra will preallocate for in
/// a single peer-deserialized vector.
///
/// In the P2P protocol, `Vec<block::Hash>` appears as the `known_blocks` block
/// locator in `getblocks` and `getheaders` messages. The Bitcoin/Zcash
/// convention encodes locators with exponentially-spaced heights (1, 2, 3, …,
/// 10, 20, 40, …, genesis), giving `~log2(N) + 10` entries for chain length N.
/// For current Zcash chain heights (~3M blocks) a legitimate locator has ~32
/// entries.
///
/// We cap at 101 to match Bitcoin Core's `MAX_LOCATOR_SZ` constant
/// (`net_processing.cpp`), which zcashd inherits. This avoids any risk of
/// rejecting legitimate locators sent by compatible nodes that follow the
/// existing Bitcoin/Zcash protocol convention.
///
/// Without this cap, `Hash::max_allocation` was previously derived from
/// `MAX_PROTOCOL_MESSAGE_LEN / 32 = 65,535`, which allowed a remote peer to
/// force ~2 MiB heap preallocation per crafted `getblocks`/`getheaders` message
/// before any payload was read. This is the same class as
/// GHSA-xr93-pcq3-pxf8 (`addr_limit`), fixed for AddrV1/V2 in PR #10494.
pub const MAX_BLOCK_LOCATOR_LENGTH: u64 = 101;

impl TrustedPreallocate for Hash {
    fn max_allocation() -> u64 {
        MAX_BLOCK_LOCATOR_LENGTH
    }
}

#[cfg(test)]
mod issuance_deficit_properties {
    use super::*;
    use crate::{
        parameters::testnet::{ConfiguredActivationHeights, RegtestParameters},
        transparent::Input,
    };
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(std::env::var("NSM_ARITHMETIC_CASES").ok().and_then(|value| value.parse().ok()).unwrap_or(1024)))]

        #[test]
        fn deficit_ignores_transfers_and_tracks_removed_value(
            height in 1u32..20,
            removed in 0i64..1_000_000_000,
            transfer in 0i64..1_000_000_000,
            pool in 0usize..5,
        ) {
            let network = Network::new_regtest(RegtestParameters {
                activation_heights: ConfiguredActivationHeights { nu7: Some(3), ..Default::default() },
                ..Default::default()
            });
            let mut block = (*genesis::regtest_genesis_block()).clone();
            let transaction = Arc::make_mut(&mut block.transactions[0]);
            let Input::Coinbase { height: coinbase_height, .. } = &mut transaction.inputs_mut()[0] else {
                panic!("genesis has a coinbase input");
            };
            *coinbase_height = Height(height);
            let amount = Amount::<NegativeAllowed>::try_from(transfer).unwrap();
            let destination = match pool {
                0 => ValueBalance::from_sprout_amount(amount),
                1 => ValueBalance::from_sapling_amount(amount),
                2 => ValueBalance::from_orchard_amount(amount),
                3 => ValueBalance::from_ironwood_amount(amount),
                _ => { let mut pools = ValueBalance::zero(); pools.set_deferred_amount(amount); pools },
            };
            let change = (destination + ValueBalance::from_transparent_amount(Amount::try_from(-transfer - removed).unwrap())).unwrap();
            let actual = block.issuance_deficit_change(&network, &change).unwrap();
            let expected = if height < 3 { 0 } else {
                i64::from(halving_block_subsidy(Height(height), &network).unwrap()) + removed
            };
            prop_assert_eq!(i64::from(actual), expected);
            let reverse = -change;
            prop_assert_eq!((change + reverse).unwrap(), ValueBalance::<NegativeAllowed>::zero());
        }
    }
    #[test]
    fn issuance_accounting_activation_genesis_and_sign_boundaries() {
        for activation in [None, Some(1), Some(3)] {
            let network = Network::new_regtest(RegtestParameters {
                activation_heights: ConfiguredActivationHeights {
                    nu7: activation,
                    ..Default::default()
                },
                ..Default::default()
            });
            for height in 0..5 {
                let mut block = (*genesis::regtest_genesis_block()).clone();
                let transaction = Arc::make_mut(&mut block.transactions[0]);
                let Input::Coinbase { height: h, .. } = &mut transaction.inputs_mut()[0] else {
                    unreachable!()
                };
                *h = Height(height);
                let scheduled = if height == 0 {
                    0
                } else {
                    i64::from(halving_block_subsidy(Height(height), &network).unwrap())
                };
                for issued in [-1, 0, 1, scheduled, scheduled + 1] {
                    let change =
                        ValueBalance::from_transparent_amount(Amount::try_from(issued).unwrap());
                    let expected = if activation.is_some_and(|start| height >= start) {
                        scheduled - issued
                    } else {
                        0
                    };
                    assert_eq!(
                        i64::from(block.issuance_deficit_change(&network, &change).unwrap()),
                        expected,
                        "activation {activation:?}, height {height}, issued {issued}"
                    );
                }
            }
        }
    }
}
