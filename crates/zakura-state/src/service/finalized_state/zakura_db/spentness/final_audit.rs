//! Check rebuilt address indexes before completion is published.
//!
//! The rebuild already establishes terminal membership: each replayed spend consumed a
//! distinct omitted output, and no omitted output remains. This audit compares the
//! address indexes with the fixed UTXO set. It scans each column family once, in key
//! order, and never changes consensus UTXOs. A mismatch stops the writer without
//! blaming a peer.

use rayon::prelude::*;
use zakura_chain::{
    amount::{Amount, NonNegative},
    parameters::Network,
    transparent,
};

use super::SpentnessError;
use crate::service::finalized_state::{
    disk_format::{
        transparent::{AddressBalanceLocation, AddressLocation, AddressUnspentOutput},
        RawBytes,
    },
    zakura_db::{transparent::UTXO_LOC_BY_TRANSPARENT_ADDR_LOC, ZakuraDb},
};

/// Rows read between writer yields.
const AUDIT_CHUNK: usize = 4_096;

/// Address UTXO index entries that share one address location.
struct Group {
    location: AddressLocation,
    address: transparent::Address,
    balance: Amount<NonNegative>,
}

/// Totals from the address UTXO index scan.
#[derive(Default)]
struct IndexTotals {
    entries: u64,
    funded_addresses: u64,
}

impl ZakuraDb {
    /// Check the address UTXO index and address balances against the terminal UTXO set.
    ///
    /// Each index entry must name a survivor paid to its group's address. That
    /// address's balance row must name the same location and hold the group's sum.
    /// The index must hold one entry per survivor with an address, and every
    /// nonzero balance must belong to a group.
    pub(super) fn audit_address_indexes(
        &self,
        yield_control: &mut impl FnMut() -> Result<(), SpentnessError>,
    ) -> Result<(), SpentnessError> {
        let network = self.network();
        let totals = self.audit_address_utxo_index(&network, yield_control)?;
        if totals.entries != self.count_address_survivors(&network, yield_control)? {
            return Err(SpentnessError::Mismatch(
                "address UTXO index differs from the terminal survivors",
            ));
        }
        if totals.funded_addresses != self.count_funded_balances(yield_control)? {
            return Err(SpentnessError::Mismatch(
                "an address balance has no indexed UTXOs",
            ));
        }
        Ok(())
    }

    /// Scan the address UTXO index in key order, which groups entries by address location.
    fn audit_address_utxo_index(
        &self,
        network: &Network,
        yield_control: &mut impl FnMut() -> Result<(), SpentnessError>,
    ) -> Result<IndexTotals, SpentnessError> {
        let address_utxos = self
            .db
            .cf_handle(UTXO_LOC_BY_TRANSPARENT_ADDR_LOC)
            .expect("address UTXO column family is declared");
        let mut keys = self
            .db
            .zs_forward_range_iter::<_, AddressUnspentOutput, (), _>(&address_utxos, ..)
            .map(|(key, ())| key)
            .peekable();
        let mut totals = IndexTotals::default();
        let mut current: Option<Group> = None;
        while keys.peek().is_some() {
            yield_control()?;
            let chunk: Vec<AddressUnspentOutput> = keys.by_ref().take(AUDIT_CHUNK).collect();
            let utxos: Vec<Option<transparent::OrderedUtxo>> = chunk
                .par_iter()
                .map(|key| self.utxo_by_location(key.unspent_output_location()))
                .collect();
            let mut finished = Vec::new();
            for (key, utxo) in chunk.into_iter().zip(utxos) {
                let output = utxo
                    .ok_or(SpentnessError::Mismatch(
                        "address index contains a spent output",
                    ))?
                    .utxo
                    .output;
                let address = output.address(network).ok_or(SpentnessError::Mismatch(
                    "address index contains a non-address script",
                ))?;
                totals.entries += 1;
                match &mut current {
                    Some(group) if group.location == key.address_location() => {
                        if group.address != address {
                            return Err(SpentnessError::Mismatch(
                                "address index groups outputs of different addresses",
                            ));
                        }
                        group.balance = (group.balance + output.value)?;
                    }
                    _ => {
                        finished.extend(current.replace(Group {
                            location: key.address_location(),
                            address,
                            balance: output.value,
                        }));
                    }
                }
            }
            totals.funded_addresses += self.audit_groups(finished)?;
        }
        totals.funded_addresses += self.audit_groups(current.into_iter().collect())?;
        Ok(totals)
    }

    /// Check each group against its address balance, and count the funded groups.
    fn audit_groups(&self, groups: Vec<Group>) -> Result<u64, SpentnessError> {
        groups
            .into_par_iter()
            .map(|group| {
                let row = self.address_balance_location(&group.address);
                if row.map(|row| row.address_location()) != Some(group.location) {
                    return Err(SpentnessError::Mismatch(
                        "address index assigns an output to the wrong address",
                    ));
                }
                if row.map(|row| row.balance()) != Some(group.balance) {
                    return Err(SpentnessError::Mismatch(
                        "rebuilt address balance differs from its indexed UTXOs",
                    ));
                }
                Ok(u64::from(group.balance != Amount::<NonNegative>::zero()))
            })
            .sum()
    }

    /// Count the survivors whose script has an address.
    fn count_address_survivors(
        &self,
        network: &Network,
        yield_control: &mut impl FnMut() -> Result<(), SpentnessError>,
    ) -> Result<u64, SpentnessError> {
        let mut count = 0;
        for (index, (_, utxo)) in self.utxos_by_location().enumerate() {
            if index % AUDIT_CHUNK == 0 {
                yield_control()?;
            }
            count += u64::from(utxo.utxo.output.address(network).is_some());
        }
        Ok(count)
    }

    /// Count the address balances above zero.
    fn count_funded_balances(
        &self,
        yield_control: &mut impl FnMut() -> Result<(), SpentnessError>,
    ) -> Result<u64, SpentnessError> {
        let mut count = 0;
        for (index, (_, balance)) in self
            .db
            .zs_forward_range_iter::<_, RawBytes, AddressBalanceLocation, _>(
                self.address_balance_cf(),
                ..,
            )
            .enumerate()
        {
            if index % AUDIT_CHUNK == 0 {
                yield_control()?;
            }
            count += u64::from(balance.balance() != Amount::<NonNegative>::zero());
        }
        Ok(count)
    }
}
