//! Transparent address indexes for non-finalized blocks.

use std::{
    collections::HashMap,
    hash::{BuildHasher, BuildHasherDefault, Hash, Hasher, RandomState},
    sync::OnceLock,
};

use zakura_chain::{
    amount::{Amount, NegativeAllowed},
    block::BlockRef,
    parameters::Network,
    transparent,
};

use crate::{
    request::ContextuallyVerifiedBlock, OutputLocation, TransactionIndex, TransactionLocation,
};

/// The transparent transfers of one non-finalized block, by address.
///
/// Built once when the block is pushed. Every chain containing the block shares it, so
/// finalizing or forking never copies another chain's address index.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct BlockTransparentTransfers {
    by_address: HashMap<AddressKey, AddressBlockTransfers, BuildHasherDefault<KeyHasher>>,
}

/// A transparent address with its hash, so a query over many blocks hashes it once.
///
/// The hash is keyed by a random per-process seed, so block contents can't choose
/// colliding addresses.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AddressKey {
    hash: u64,
    address: transparent::Address,
}

impl AddressKey {
    /// Hashes `address` for lookups in any block's index.
    pub(crate) fn new(address: transparent::Address) -> Self {
        static SEED: OnceLock<RandomState> = OnceLock::new();
        Self {
            hash: SEED.get_or_init(RandomState::new).hash_one(address),
            address,
        }
    }
}

impl Hash for AddressKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

/// Passes an [`AddressKey`]'s precomputed hash through unchanged.
#[derive(Default)]
pub(crate) struct KeyHasher(u64);

impl Hasher for KeyHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, _bytes: &[u8]) {
        unreachable!("address keys only write their precomputed hash")
    }

    fn write_u64(&mut self, hash: u64) {
        self.0 = hash;
    }
}

/// One address's transparent transfers in one block.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct AddressBlockTransfers {
    /// The block's change to the address balance.
    balance: Amount<NegativeAllowed>,

    /// The total value the block sent to the address.
    received: u64,

    /// The outputs the block sent to the address, in block order.
    created: Vec<OutputLocation>,

    /// The outputs sent to the address that the block spends, with the spending
    /// transaction's index, in block order.
    spent: Vec<(TransactionIndex, OutputLocation)>,
}

impl BlockTransparentTransfers {
    /// Indexes the transparent outputs `block` creates and spends, by address.
    ///
    /// # Panics
    ///
    /// Outside tests, if `block.spent_outputs` omits an output a transparent input spends.
    pub(crate) fn new(block: &ContextuallyVerifiedBlock, network: &Network) -> Self {
        let mut by_address: HashMap<_, AddressBlockTransfers, _> = HashMap::default();

        for (transaction_index, transaction) in block.block.view().transactions().enumerate() {
            let transaction_location =
                TransactionLocation::from_usize(block.height, transaction_index);

            for (output_index, output) in transaction.outputs().iter().enumerate() {
                let Some(address) = output.address(network) else {
                    continue;
                };
                let value = output.value();
                let transfers = by_address.entry(AddressKey::new(address)).or_default();
                transfers.balance = (transfers.balance
                    + value
                        .constrain()
                        .expect("NonNegative values are always valid NegativeAllowed values"))
                .expect("total UTXO value has already been checked");
                transfers.received += u64::from(value);
                transfers.created.push(OutputLocation::from_usize(
                    block.height,
                    transaction_index,
                    output_index,
                ));
            }

            for outpoint in transaction
                .inputs()
                .iter()
                .filter_map(transparent::Input::outpoint)
            {
                // TODO: fix tests to supply correct spent outputs, then turn this into an expect()
                let Some(spent_output) = block.spent_outputs.get(&outpoint) else {
                    if !cfg!(test) {
                        panic!(
                            "unexpected missing spent output: all spent outputs must be indexed"
                        );
                    }
                    continue;
                };
                let Some(address) = spent_output.utxo.output.address(network) else {
                    continue;
                };
                let transfers = by_address.entry(AddressKey::new(address)).or_default();
                transfers.balance = (transfers.balance
                    - spent_output
                        .utxo
                        .output
                        .value()
                        .constrain()
                        .expect("NonNegative values are always valid NegativeAllowed values"))
                .expect("total UTXO value has already been checked");
                transfers.spent.push((
                    transaction_location.index,
                    OutputLocation::from_outpoint(
                        TransactionLocation::from_usize(
                            spent_output.utxo.height,
                            spent_output.tx_index_in_block,
                        ),
                        &outpoint,
                    ),
                ));
            }
        }

        Self { by_address }
    }

    /// Returns true if the block has no transparent transfers to indexed addresses.
    pub(crate) fn is_empty(&self) -> bool {
        self.by_address.is_empty()
    }

    /// Returns `address`'s transfers in this block, if it has any.
    pub(crate) fn get(&self, address: &AddressKey) -> Option<&AddressBlockTransfers> {
        self.by_address.get(address)
    }
}

impl AddressBlockTransfers {
    /// Returns the block's change to the address balance.
    pub(crate) fn balance(&self) -> Amount<NegativeAllowed> {
        self.balance
    }

    /// Returns the total value the block sent to the address.
    pub(crate) fn received(&self) -> u64 {
        self.received
    }

    /// Returns the outputs `block` sent to the address, in block order.
    ///
    /// `block` must be the block this index was built from. Some of the outputs might be
    /// spent later in the chain.
    pub(crate) fn created<'a>(
        &'a self,
        block: &'a BlockRef,
    ) -> impl Iterator<Item = (OutputLocation, &'a transparent::Output)> + 'a {
        self.created.iter().map(move |location| {
            let transaction = block
                .transaction_at(location.transaction_index().as_usize())
                .expect("address index locations refer to the block's transactions");
            let output = usize::try_from(location.output_index().index())
                .ok()
                .and_then(|index| transaction.outputs().get(index))
                .expect("address index locations refer to the block's outputs");
            (*location, output)
        })
    }

    /// Returns the locations of the outputs the block sent to the address, in block order.
    #[cfg(test)]
    pub(crate) fn created_locations(&self) -> &[OutputLocation] {
        &self.created
    }

    /// Returns the outputs sent to the address that the block spends.
    pub(crate) fn spent(&self) -> impl Iterator<Item = &OutputLocation> {
        self.spent.iter().map(|(_, spent)| spent)
    }

    /// Returns the indexes of the block's transactions that send to or spend from the
    /// address. An index repeats when a transaction has several such transfers.
    pub(crate) fn transaction_indexes(&self) -> impl Iterator<Item = TransactionIndex> + '_ {
        self.created
            .iter()
            .map(OutputLocation::transaction_index)
            .chain(self.spent.iter().map(|(spending, _)| *spending))
    }
}

#[cfg(test)]
mod tests;
