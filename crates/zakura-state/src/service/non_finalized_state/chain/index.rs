//! Transparent address indexes for non-finalized chains.

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::RangeInclusive,
    sync::Arc,
};

use zakura_chain::{
    amount::{Amount, NegativeAllowed},
    block::Height,
    transaction, transparent,
};

use crate::ordered_map::collect_ordered_map;
use crate::{OutputLocation, TransactionLocation, ValidateContextError};

use super::{counted_set::CountedSet, RevertPosition, SharedMap, UpdateWith};

/// Share historical height groups while bounding the group copied by a write.
const HEIGHTS_PER_OUTPUT_CHUNK: u32 = 32;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransparentTransfers {
    /// The partial chain balance for a transparent address.
    balance: Amount<NegativeAllowed>,

    /// The partial list of transactions that spent or received UTXOs to a transparent address.
    ///
    /// Since transactions can only be added to this set, it does not need
    /// special handling for
    /// [`ReadStateService`](crate::service::ReadStateService) response
    /// inconsistencies.
    ///
    /// The `getaddresstxids` RPC needs these transaction IDs to be sorted in chain order.
    tx_ids: CountedSet<transaction::Hash>,

    /// The partial list of UTXOs received by a transparent address.
    ///
    /// The `getaddressutxos` RPC doesn't need these transaction IDs to be sorted in chain order,
    /// but it might in future. So Zebra does it anyway.
    ///
    /// Outputs are grouped by height, then by 32-height chunks. Snapshots share chunks;
    /// copying a chunk copies at most 32 height entries, sharing their output payloads.
    /// A single chunk is stored directly, avoiding a second map allocation for short
    /// histories. Both levels are sparse; occupied chunks and height groups are nonempty.
    created_utxos: CreatedUtxos,

    /// The partial list of UTXOs spent by a transparent address.
    ///
    /// The `getaddressutxos` RPC doesn't need these transaction IDs to be sorted in chain order,
    /// but it might in future. So Zebra does it anyway.
    ///
    /// Optional TODO:
    /// - store spent `Utxo`s by location in the chain, use the chain spent UTXOs to filter,
    ///   and stop storing spent UTXOs by address
    spent_utxos: BTreeSet<OutputLocation>,
}

/// The outputs one block created for an address, in chain order.
type CreatedOutputs = BTreeMap<OutputLocation, transparent::Output>;

/// The occupied heights within one chunk, in chain order.
type CreatedOutputHeights = BTreeMap<Height, Arc<CreatedOutputs>>;

/// Keep short histories in one map; share chunks only when a history spans multiple chunks.
#[derive(Clone, Debug, Eq, PartialEq)]
enum CreatedUtxos {
    /// Empty, or containing heights from just one chunk.
    Single(CreatedOutputHeights),
    /// At least two nonempty chunks, ordered by their height range.
    Chunked(BTreeMap<u32, Arc<CreatedOutputHeights>>),
}

impl Default for CreatedUtxos {
    fn default() -> Self {
        Self::Single(BTreeMap::new())
    }
}

impl CreatedUtxos {
    /// Insert an output, promoting the single map only when another chunk is needed.
    fn insert(
        &mut self,
        location: OutputLocation,
        output: transparent::Output,
    ) -> Option<transparent::Output> {
        let height = location.height();
        let chunk_id = height.0 / HEIGHTS_PER_OUTPUT_CHUNK;
        if let Self::Single(heights) = self {
            if let Some((&first_height, _)) = heights.first_key_value() {
                let existing_id = first_height.0 / HEIGHTS_PER_OUTPUT_CHUNK;
                if existing_id != chunk_id {
                    let existing = std::mem::take(heights);
                    *self = Self::Chunked(BTreeMap::from([(existing_id, Arc::new(existing))]));
                }
            }
        }
        let heights = match self {
            Self::Single(heights) => heights,
            Self::Chunked(chunks) => Arc::make_mut(chunks.entry(chunk_id).or_default()),
        };
        Arc::make_mut(heights.entry(height).or_default()).insert(location, output)
    }

    /// Remove an output and collapse back to one map when only one chunk remains.
    fn remove(&mut self, location: &OutputLocation) -> bool {
        let height = location.height();
        let chunk_id = height.0 / HEIGHTS_PER_OUTPUT_CHUNK;
        let heights =
            match self {
                Self::Single(heights) => heights,
                Self::Chunked(chunks) => Arc::make_mut(chunks.get_mut(&chunk_id).expect(
                    "created output chunk exists because the block was previously applied",
                )),
            };
        let outputs = heights
            .get_mut(&height)
            .expect("created output height exists because the block was previously applied");
        // Dropping the last output needs no copy of the shared output group.
        let removed = if outputs.len() == 1 && outputs.contains_key(location) {
            heights.remove(&height);
            true
        } else {
            Arc::make_mut(outputs).remove(location).is_some()
        };
        let empty = heights.is_empty();
        if let Self::Chunked(chunks) = self {
            if empty {
                chunks.remove(&chunk_id);
                if chunks.len() == 1 {
                    let (_, remaining) = chunks
                        .pop_first()
                        .expect("one chunk remains after removing an empty chunk");
                    *self = Self::Single(Arc::unwrap_or_clone(remaining));
                }
            }
        }
        removed
    }

    /// Iterate occupied heights in chain order in either representation.
    fn heights(&self) -> impl Iterator<Item = (&Height, &Arc<CreatedOutputs>)> {
        let (single, chunks) = match self {
            Self::Single(heights) => (Some(heights), None),
            Self::Chunked(chunks) => (None, Some(chunks)),
        };
        single
            .into_iter()
            .chain(
                chunks
                    .into_iter()
                    .flat_map(|chunks| chunks.values().map(Arc::as_ref)),
            )
            .flat_map(|heights| heights.iter())
    }

    fn is_empty(&self) -> bool {
        matches!(self, Self::Single(heights) if heights.is_empty())
    }
}

// A created UTXO
//
// TODO: replace arguments with a struct
impl
    UpdateWith<(
        // The location of the UTXO
        &transparent::OutPoint,
        // The UTXO data
        // Includes the location of the transaction that created the output
        &transparent::OrderedUtxo,
    )> for TransparentTransfers
{
    #[allow(clippy::unwrap_in_result)]
    fn update_chain_tip_with(
        &mut self,
        &(outpoint, created_utxo): &(&transparent::OutPoint, &transparent::OrderedUtxo),
    ) -> Result<(), ValidateContextError> {
        self.balance = (self.balance
            + created_utxo
                .utxo
                .output
                .value()
                .constrain()
                .expect("NonNegative values are always valid NegativeAllowed values"))
        .expect("total UTXO value has already been checked");

        let transaction_location = transaction_location(created_utxo);
        let output_location = OutputLocation::from_outpoint(transaction_location, outpoint);

        let previous_entry = self
            .created_utxos
            .insert(output_location, created_utxo.utxo.output.clone());
        assert_eq!(
            previous_entry, None,
            "unexpected created output: duplicate update or duplicate UTXO",
        );

        self.tx_ids.insert(outpoint.hash);

        Ok(())
    }

    fn revert_chain_with(
        &mut self,
        &(outpoint, created_utxo): &(&transparent::OutPoint, &transparent::OrderedUtxo),
        _position: RevertPosition,
    ) {
        self.balance = (self.balance
            - created_utxo
                .utxo
                .output
                .value()
                .constrain()
                .expect("NonNegative values are always valid NegativeAllowed values"))
        .expect("reversing previous balance changes is always valid");

        let transaction_location = transaction_location(created_utxo);
        let output_location = OutputLocation::from_outpoint(transaction_location, outpoint);

        let output_was_removed = self.created_utxos.remove(&output_location);
        assert!(
            output_was_removed,
            "unexpected revert of created output: duplicate update or duplicate UTXO",
        );

        let tx_id_was_removed = self.tx_ids.remove(&outpoint.hash);
        assert!(
            tx_id_was_removed,
            "unexpected revert of created output transaction: \
             duplicate revert, or revert of an output that was never updated",
        );
    }
}

// A transparent input
//
// TODO: replace arguments with a struct
impl
    UpdateWith<(
        // The transparent input data
        &transparent::Input,
        // The hash of the transaction the input is from
        // (not the transaction the spent output was created by)
        &transaction::Hash,
        // The output spent by the input
        // Includes the location of the transaction that created the output
        &transparent::OrderedUtxo,
    )> for TransparentTransfers
{
    #[allow(clippy::unwrap_in_result)]
    fn update_chain_tip_with(
        &mut self,
        &(spending_input, spending_tx_hash, spent_output): &(
            &transparent::Input,
            &transaction::Hash,
            &transparent::OrderedUtxo,
        ),
    ) -> Result<(), ValidateContextError> {
        // Spending a UTXO subtracts value from the balance
        self.balance = (self.balance
            - spent_output
                .utxo
                .output
                .value()
                .constrain()
                .expect("NonNegative values are always valid NegativeAllowed values"))
        .expect("total UTXO value has already been checked");

        let spent_outpoint = spending_input.outpoint().expect("checked by caller");

        let spent_output_tx_loc = transaction_location(spent_output);
        let output_location = OutputLocation::from_outpoint(spent_output_tx_loc, &spent_outpoint);
        let spend_was_inserted = self.spent_utxos.insert(output_location);
        assert!(
            spend_was_inserted,
            "unexpected spent output: duplicate update or duplicate spend",
        );

        self.tx_ids.insert(*spending_tx_hash);

        Ok(())
    }

    fn revert_chain_with(
        &mut self,
        &(spending_input, spending_tx_hash, spent_output): &(
            &transparent::Input,
            &transaction::Hash,
            &transparent::OrderedUtxo,
        ),
        _position: RevertPosition,
    ) {
        self.balance = (self.balance
            + spent_output
                .utxo
                .output
                .value()
                .constrain()
                .expect("NonNegative values are always valid NegativeAllowed values"))
        .expect("reversing previous balance changes is always valid");

        let spent_outpoint = spending_input.outpoint().expect("checked by caller");

        let spent_output_tx_loc = transaction_location(spent_output);
        let output_location = OutputLocation::from_outpoint(spent_output_tx_loc, &spent_outpoint);
        let spend_was_removed = self.spent_utxos.remove(&output_location);
        assert!(
            spend_was_removed,
            "unexpected revert of spent output: \
             duplicate revert, or revert of a spent output that was never updated",
        );

        let tx_id_was_removed = self.tx_ids.remove(spending_tx_hash);
        assert!(
            tx_id_was_removed,
            "unexpected revert of spending input transaction: \
             duplicate revert, or revert of an input that was never updated",
        );
    }
}

impl TransparentTransfers {
    /// Returns true if there are no transfers for this address.
    pub fn is_empty(&self) -> bool {
        self.balance == Amount::<NegativeAllowed>::zero()
            && self.tx_ids.is_empty()
            && self.created_utxos.is_empty()
            && self.spent_utxos.is_empty()
    }

    /// Returns the partial balance for this address.
    #[allow(dead_code)]
    pub fn balance(&self) -> Amount<NegativeAllowed> {
        self.balance
    }

    /// Returns the partial received balance for this address.
    pub fn received(&self) -> u64 {
        self.created_utxos()
            .map(|(_, output)| u64::from(output.value()))
            .sum()
    }

    /// Returns the [`transaction::Hash`]es of the transactions that sent or
    /// received transparent transfers to this address, in this partial chain,
    /// filtered by `query_height_range`.
    ///
    /// The transactions are returned in chain order.
    ///
    /// `chain_tx_loc_by_hash` should be the `tx_loc_by_hash` field from the
    /// [`Chain`][1] containing this index.
    ///
    /// # Panics
    ///
    /// If `chain_tx_loc_by_hash` is missing some transaction hashes from this
    /// index.
    ///
    /// [1]: super::super::Chain
    pub(crate) fn tx_ids(
        &self,
        chain_tx_loc_by_hash: &SharedMap<transaction::Hash, TransactionLocation>,
        query_height_range: RangeInclusive<Height>,
    ) -> BTreeMap<TransactionLocation, transaction::Hash> {
        collect_ordered_map(self.tx_ids.distinct_values().filter_map(|tx_hash| {
            let tx_loc = *chain_tx_loc_by_hash
                .get(tx_hash)
                .expect("all hashes are indexed");

            if query_height_range.contains(&tx_loc.height) {
                Some((tx_loc, *tx_hash))
            } else {
                None
            }
        }))
    }

    /// Returns the new transparent outputs sent to this address,
    /// in this partial chain, in chain order.
    ///
    /// Some of these outputs might already be spent.
    /// [`TransparentTransfers::spent_utxos`] returns spent UTXOs.
    #[allow(dead_code)]
    pub fn created_utxos(&self) -> impl Iterator<Item = (&OutputLocation, &transparent::Output)> {
        self.created_utxos
            .heights()
            .flat_map(|(_, outputs)| outputs.iter())
    }

    /// Returns the [`OutputLocation`]s of the spent transparent outputs sent to this address,
    /// in this partial chain, in chain order.
    #[allow(dead_code)]
    pub fn spent_utxos(&self) -> &BTreeSet<OutputLocation> {
        &self.spent_utxos
    }
}

impl Default for TransparentTransfers {
    fn default() -> Self {
        Self {
            balance: Amount::zero(),
            tx_ids: Default::default(),
            created_utxos: Default::default(),
            spent_utxos: Default::default(),
        }
    }
}

/// Returns the transaction location for an [`transparent::OrderedUtxo`].
pub fn transaction_location(ordered_utxo: &transparent::OrderedUtxo) -> TransactionLocation {
    TransactionLocation::from_usize(ordered_utxo.utxo.height, ordered_utxo.tx_index_in_block)
}

#[cfg(test)]
mod tests;
