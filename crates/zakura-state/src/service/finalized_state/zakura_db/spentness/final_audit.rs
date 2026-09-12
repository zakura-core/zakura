//! Check rebuilt indexes against retained history before completion is published.
//!
//! The audit compares complete survivor entries with retained-body replay. It also
//! checks address ownership, address balances, and the output count. It never
//! changes consensus UTXOs. A mismatch stops the writer without blaming a peer.

use zakura_chain::{
    amount::{Amount, NonNegative},
    block::Height,
    parameters::{spentness_hints::Commitment, Network},
    transparent,
};

use super::{outputs_in_order, rebuild::ReplayCache, SpentnessError};
use crate::service::finalized_state::{
    disk_db::ReadDisk,
    disk_format::{
        transparent::{AddressBalanceLocation, AddressUnspentOutput},
        IntoDisk, OutputLocation, RawBytes,
    },
    zakura_db::{transparent::UTXO_LOC_BY_TRANSPARENT_ADDR_LOC, ZakuraDb},
};

/// UTXO rows scanned between writer yields.
const YIELD_INTERVAL: u64 = 1_024;

impl ZakuraDb {
    /// Audit the rebuilt state at the commitment's terminal height.
    pub(super) fn audit_rebuilt_state(
        &self,
        commitment: &Commitment,
        cache: &mut ReplayCache,
        yield_control: &mut impl FnMut() -> Result<(), SpentnessError>,
    ) -> Result<(), SpentnessError> {
        let (outputs, survivors) = self.audit_terminal_membership(commitment, yield_control)?;
        if outputs != commitment.output_count {
            return Err(SpentnessError::Mismatch(
                "retained output count differs from the commitment",
            ));
        }
        if self.count_utxos(yield_control)? != survivors {
            return Err(SpentnessError::Mismatch(
                "terminal UTXO set contains extra locations",
            ));
        }
        self.audit_address_utxo_index(yield_control)?;
        self.audit_address_balances(cache, yield_control)
    }

    /// Check every output's presence and entry, and return the output and survivor counts.
    fn audit_terminal_membership(
        &self,
        commitment: &Commitment,
        yield_control: &mut impl FnMut() -> Result<(), SpentnessError>,
    ) -> Result<(u64, u64), SpentnessError> {
        let network = self.network();
        let mut outputs = 0;
        let mut survivors = 0;
        for height in (0..=commitment.terminal_height).map(Height) {
            yield_control()?;
            let block = self
                .block(height.into())
                .ok_or(SpentnessError::Mismatch("audit block is missing"))?;
            for (location, transaction, output) in outputs_in_order(&block, height) {
                outputs += 1;
                let actual = self.utxo_by_location(location);
                if !self.unspent_after_rebuild(location) {
                    if actual.is_some() {
                        return Err(SpentnessError::Mismatch(
                            "terminal UTXO set retains a spent or excluded output",
                        ));
                    }
                    continue;
                }
                let expected =
                    transparent::Utxo::new(output.clone(), height, transaction.is_coinbase());
                if actual.map(|ordered| ordered.utxo) != Some(expected) {
                    return Err(SpentnessError::Mismatch(
                        "terminal UTXO entry differs from retained-body replay",
                    ));
                }
                self.audit_address_entry(&network, location, output)?;
                survivors += 1;
            }
        }
        Ok((outputs, survivors))
    }

    /// Whether the rebuild left `location` unspent. Genesis outputs are never unspent.
    fn unspent_after_rebuild(&self, location: OutputLocation) -> bool {
        !location.height().is_min()
            && self
                .tx_location_by_spent_output_location(&location)
                .is_none()
    }

    /// A survivor with an address must appear in that address's UTXO index.
    fn audit_address_entry(
        &self,
        network: &Network,
        location: OutputLocation,
        output: &transparent::Output,
    ) -> Result<(), SpentnessError> {
        let Some(address) = output.address(network) else {
            return Ok(());
        };
        let balance = self
            .address_balance_location(&address)
            .ok_or(SpentnessError::Mismatch("survivor has no rebuilt address"))?;
        let key = AddressUnspentOutput::new(balance.address_location(), location);
        if self
            .db
            .zs_get::<_, _, ()>(&self.address_utxo_cf(), &key)
            .is_none()
        {
            return Err(SpentnessError::Mismatch(
                "survivor is missing from its address index",
            ));
        }
        Ok(())
    }

    fn count_utxos(
        &self,
        yield_control: &mut impl FnMut() -> Result<(), SpentnessError>,
    ) -> Result<u64, SpentnessError> {
        let mut count = 0;
        for _ in self.utxos_by_location() {
            if count % YIELD_INTERVAL == 0 {
                yield_control()?;
            }
            count += 1;
        }
        Ok(count)
    }

    /// Every address index entry must name an unspent output owned by that address.
    fn audit_address_utxo_index(
        &self,
        yield_control: &mut impl FnMut() -> Result<(), SpentnessError>,
    ) -> Result<(), SpentnessError> {
        let network = self.network();
        for (key, ()) in self
            .db
            .zs_forward_range_iter::<_, AddressUnspentOutput, (), _>(&self.address_utxo_cf(), ..)
        {
            yield_control()?;
            let utxo = self.utxo_by_location(key.unspent_output_location()).ok_or(
                SpentnessError::Mismatch("address index contains a spent output"),
            )?;
            let address = utxo
                .utxo
                .output
                .address(&network)
                .ok_or(SpentnessError::Mismatch(
                    "address index contains a non-address script",
                ))?;
            let owner = self
                .address_balance_location(&address)
                .map(|balance| balance.address_location());
            if owner != Some(key.address_location()) {
                return Err(SpentnessError::Mismatch(
                    "address index assigns an output to the wrong address",
                ));
            }
        }
        Ok(())
    }

    /// Each balance's first-receive location and total must match its indexed UTXOs.
    fn audit_address_balances(
        &self,
        cache: &mut ReplayCache,
        yield_control: &mut impl FnMut() -> Result<(), SpentnessError>,
    ) -> Result<(), SpentnessError> {
        let network = self.network();
        for (address_key, balance) in self
            .db
            .zs_forward_range_iter::<_, RawBytes, AddressBalanceLocation, _>(
                self.address_balance_cf(),
                ..,
            )
        {
            yield_control()?;
            let first_receipt = cache.utxo(self, balance.address_location())?;
            let first_receipt_address = first_receipt
                .output
                .address(&network)
                .map(|address| address.as_bytes().to_vec());
            if first_receipt_address != Some(address_key.as_bytes()) {
                return Err(SpentnessError::Mismatch(
                    "address first-receive location has another script",
                ));
            }
            if self.indexed_balance(&balance, yield_control)? != balance.balance() {
                return Err(SpentnessError::Mismatch(
                    "rebuilt address balance differs from its consensus UTXOs",
                ));
            }
        }
        Ok(())
    }

    /// Sum the UTXOs the address index assigns to `balance`'s address.
    fn indexed_balance(
        &self,
        balance: &AddressBalanceLocation,
        yield_control: &mut impl FnMut() -> Result<(), SpentnessError>,
    ) -> Result<Amount<NonNegative>, SpentnessError> {
        let address_location = balance.address_location();
        let start = AddressUnspentOutput::address_iterator_start(address_location);
        let mut sum = Amount::<NonNegative>::zero();
        for (key, ()) in self
            .db
            .zs_forward_range_iter::<_, AddressUnspentOutput, (), _>(
                &self.address_utxo_cf(),
                start..,
            )
            .take_while(|(key, ())| key.address_location() == address_location)
        {
            yield_control()?;
            let output = self
                .utxo_by_location(key.unspent_output_location())
                .ok_or(SpentnessError::Mismatch("indexed UTXO is missing"))?;
            sum = (sum + output.utxo.output.value)?;
        }
        Ok(sum)
    }

    fn address_utxo_cf(&self) -> impl rocksdb::AsColumnFamilyRef + '_ {
        self.db
            .cf_handle(UTXO_LOC_BY_TRANSPARENT_ADDR_LOC)
            .expect("address UTXO column family is declared")
    }
}
