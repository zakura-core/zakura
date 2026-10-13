//! Omitted outputs that no block has spent yet.
//!
//! [`LiveOutputs`] keeps them in memory, so most spends resolve without disk reads.
//! The journal keeps each block's omitted outputs on disk, keyed by creation height.
//! A spend that misses the map, after an eviction or a restart, reads the journal.

use std::collections::{BTreeMap, HashMap};

use zakura_chain::{
    amount::{Amount, NonNegative},
    block::Height,
    parameters::Network,
    transparent::{self, Address},
};

use super::SpentnessError;
use crate::service::finalized_state::disk_format::{
    OutputLocation, TransactionIndex, TransactionLocation,
};

/// Entries above this count trigger an eviction. Each entry takes about 100 bytes.
#[cfg(not(test))]
pub(super) const LIVE_OUTPUT_CAPACITY: usize = 10_000_000;
/// Tests use a small capacity, so their spends also exercise the journal.
#[cfg(test)]
pub(super) const LIVE_OUTPUT_CAPACITY: usize = 8;

/// Eviction drops whole buckets of this many creation heights, oldest first.
const BUCKET_HEIGHTS: u32 = 1_000;

/// The fields of an omitted output that the ordinary writer needs when a block spends it.
///
/// It holds the output's address instead of its script. The writer only derives the
/// address from a spent output's script.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct OmittedOutput {
    pub(super) location: OutputLocation,
    value: Amount<NonNegative>,
    address: Option<Address>,
    from_coinbase: bool,
}

impl OmittedOutput {
    pub(super) fn new(
        location: OutputLocation,
        output: &transparent::Output,
        from_coinbase: bool,
        network: &Network,
    ) -> Self {
        Self {
            location,
            value: output.value,
            address: output.address(network),
            from_coinbase,
        }
    }

    /// The spent UTXO, with the standard script for its address.
    pub(super) fn utxo(&self) -> transparent::Utxo {
        let lock_script = self
            .address
            .as_ref()
            .map_or_else(|| transparent::Script::new(&[]), Address::script);
        transparent::Utxo::new(
            transparent::Output {
                value: self.value,
                lock_script,
            },
            self.location.height(),
            self.from_coinbase,
        )
    }
}

/// Omitted outputs in memory, with a bounded entry count.
#[derive(Debug)]
pub(super) struct LiveOutputs {
    entries: HashMap<transparent::OutPoint, OmittedOutput>,
    /// Entry counts by creation bucket, so eviction can pick a cutoff without a scan.
    buckets: BTreeMap<u32, usize>,
    capacity: usize,
}

impl LiveOutputs {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            buckets: BTreeMap::new(),
            capacity,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(super) fn insert(&mut self, outpoint: transparent::OutPoint, output: OmittedOutput) {
        let bucket = bucket(&output);
        // A retried block inserts its outputs again, so count only new entries.
        if self.entries.insert(outpoint, output).is_none() {
            *self.buckets.entry(bucket).or_default() += 1;
        }
    }

    pub(super) fn take(&mut self, outpoint: &transparent::OutPoint) -> Option<OmittedOutput> {
        let output = self.entries.remove(outpoint)?;
        let bucket = bucket(&output);
        if let Some(count) = self.buckets.get_mut(&bucket) {
            *count -= 1;
            if *count == 0 {
                self.buckets.remove(&bucket);
            }
        }
        Some(output)
    }

    /// Drop the oldest buckets until a quarter of the capacity is free.
    ///
    /// Returns the number of evicted entries. The journal still holds them.
    pub(super) fn evict_if_full(&mut self) -> usize {
        let before = self.entries.len();
        if before <= self.capacity {
            return 0;
        }
        let excess = before - (self.capacity - self.capacity / 4);
        let mut freed = 0;
        let mut cutoff = 0;
        for (bucket, count) in &self.buckets {
            if freed >= excess {
                break;
            }
            freed += count;
            cutoff = bucket + 1;
        }
        self.entries.retain(|_, output| bucket(output) >= cutoff);
        self.buckets = self.buckets.split_off(&cutoff);
        before - self.entries.len()
    }
}

fn bucket(output: &OmittedOutput) -> u32 {
    output.location.height().0 / BUCKET_HEIGHTS
}

/// Journal record flags.
const COINBASE: u8 = 1;
const PAY_TO_PUBLIC_KEY_HASH: u8 = 2;
const PAY_TO_SCRIPT_HASH: u8 = 4;

/// Encode one block's omitted outputs as journal records.
///
/// Each record holds the transaction index (2 bytes), output index (4), value (8),
/// flags (1), and, for an address output, the 20-byte address hash. Integers are
/// big-endian.
pub(super) fn encode_journal<'a>(outputs: impl IntoIterator<Item = &'a OmittedOutput>) -> Vec<u8> {
    let mut bytes = Vec::new();
    for output in outputs {
        bytes.extend_from_slice(&output.location.transaction_index().index().to_be_bytes());
        bytes.extend_from_slice(&output.location.output_index().index().to_be_bytes());
        bytes.extend_from_slice(&u64::from(output.value).to_be_bytes());
        let mut flags = if output.from_coinbase { COINBASE } else { 0 };
        match &output.address {
            Some(address @ Address::PayToPublicKeyHash { .. }) => {
                flags |= PAY_TO_PUBLIC_KEY_HASH;
                bytes.push(flags);
                bytes.extend_from_slice(&address.hash_bytes());
            }
            Some(address @ Address::PayToScriptHash { .. }) => {
                flags |= PAY_TO_SCRIPT_HASH;
                bytes.push(flags);
                bytes.extend_from_slice(&address.hash_bytes());
            }
            // Output scripts never decode to TEX addresses.
            Some(Address::Tex { .. }) | None => bytes.push(flags),
        }
    }
    bytes
}

/// Find the output at `transaction_index` and `output_index` in a block's journal records.
pub(super) fn find_in_journal(
    bytes: &[u8],
    height: Height,
    transaction_index: TransactionIndex,
    output_index: u32,
    network: &Network,
) -> Result<Option<OmittedOutput>, SpentnessError> {
    const TRUNCATED: SpentnessError = SpentnessError::Inconsistent("truncated journal record");
    let mut rest = bytes;
    while !rest.is_empty() {
        let (tx, tail) = rest.split_first_chunk::<2>().ok_or(TRUNCATED)?;
        let (index, tail) = tail.split_first_chunk::<4>().ok_or(TRUNCATED)?;
        let (value, tail) = tail.split_first_chunk::<8>().ok_or(TRUNCATED)?;
        let (&flags, tail) = tail.split_first().ok_or(TRUNCATED)?;
        let (tx, index, value) = (
            u16::from_be_bytes(*tx),
            u32::from_be_bytes(*index),
            u64::from_be_bytes(*value),
        );
        let (address, tail) = match flags & (PAY_TO_PUBLIC_KEY_HASH | PAY_TO_SCRIPT_HASH) {
            0 => (None, tail),
            kind => {
                let (hash, tail) = tail.split_first_chunk::<20>().ok_or(TRUNCATED)?;
                let address = if kind == PAY_TO_PUBLIC_KEY_HASH {
                    Address::from_pub_key_hash(network.t_addr_kind(), *hash)
                } else {
                    Address::from_script_hash(network.t_addr_kind(), *hash)
                };
                (Some(address), tail)
            }
        };
        rest = tail;
        if tx == transaction_index.index() && index == output_index {
            return Ok(Some(OmittedOutput {
                location: OutputLocation::from_output_index(
                    TransactionLocation::from_parts(height, transaction_index),
                    index,
                ),
                value: Amount::try_from(value)?,
                address,
                from_coinbase: flags & COINBASE != 0,
            }));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use zakura_chain::transparent::Script;

    use super::*;

    fn output(height: u32, address: Option<Address>) -> OmittedOutput {
        let lock_script = address
            .as_ref()
            .map_or_else(|| Script::new(&[]), Address::script);
        OmittedOutput::new(
            OutputLocation::from_usize(Height(height), 1, 2),
            &transparent::Output {
                value: Amount::try_from(7).unwrap(),
                lock_script,
            },
            height % 2 == 0,
            &Network::Mainnet,
        )
    }

    fn outpoint(n: u8) -> transparent::OutPoint {
        transparent::OutPoint {
            hash: zakura_chain::transaction::Hash([n; 32]),
            index: 0,
        }
    }

    #[test]
    fn journal_round_trips_every_address_kind() {
        let kind = Network::Mainnet.t_addr_kind();
        let outputs = [
            output(4, Some(Address::from_pub_key_hash(kind, [3; 20]))),
            output(4, Some(Address::from_script_hash(kind, [5; 20]))),
            output(4, None),
        ];
        let outputs: Vec<_> = outputs
            .into_iter()
            .enumerate()
            .map(|(index, mut output)| {
                output.location = OutputLocation::from_usize(Height(4), 1, index);
                output
            })
            .collect();
        let bytes = encode_journal(&outputs);
        for (index, expected) in outputs.iter().enumerate() {
            let found = find_in_journal(
                &bytes,
                Height(4),
                TransactionIndex::from_usize(1),
                u32::try_from(index).unwrap(),
                &Network::Mainnet,
            )
            .unwrap();
            assert_eq!(found.as_ref(), Some(expected));
        }
        assert!(find_in_journal(
            &bytes,
            Height(4),
            TransactionIndex::from_usize(2),
            0,
            &Network::Mainnet
        )
        .unwrap()
        .is_none());
        assert!(find_in_journal(
            &bytes[..bytes.len() - 1],
            Height(4),
            TransactionIndex::from_usize(9),
            0,
            &Network::Mainnet
        )
        .is_err());
    }

    #[test]
    fn spent_utxo_keeps_the_address() {
        let address = Address::from_pub_key_hash(Network::Mainnet.t_addr_kind(), [9; 20]);
        let utxo = output(6, Some(address)).utxo();
        assert_eq!(utxo.output.address(&Network::Mainnet), Some(address));
        assert_eq!(output(6, None).utxo().output.lock_script, Script::new(&[]));
    }

    #[test]
    fn eviction_drops_the_oldest_buckets() {
        let mut live = LiveOutputs::new(4);
        for (n, height) in [(1, 10), (2, 1_500), (3, 2_500), (4, 3_500), (5, 3_600)] {
            live.insert(outpoint(n), output(height, None));
        }
        live.insert(outpoint(5), output(3_600, None));
        assert_eq!(live.len(), 5);
        assert_eq!(live.evict_if_full(), 2);
        assert!(live.take(&outpoint(1)).is_none());
        assert!(live.take(&outpoint(2)).is_none());
        assert!(live.take(&outpoint(3)).is_some());
        assert_eq!(live.evict_if_full(), 0);
        assert_eq!(live.len(), 2);
    }
}
