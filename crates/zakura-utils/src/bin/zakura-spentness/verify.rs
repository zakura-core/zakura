//! Check an artifact against an independent transparent replay.
//!
//! The oracle builds its own outpoint-keyed UTXO set in scratch storage. It does not
//! call the ordinary state writer or the generator's merge. It rejects missing,
//! duplicate, future, and immature coinbase spends, then compares complete terminal
//! entries with the ordinary state.
//!
//! The bit audit then checks every membership bit. It also compares salted sums of
//! spent-output outpoints and input outpoints, so every absent output must be spent
//! exactly once. The source node's full validation remains responsible for
//! signatures, shielded proofs, and other consensus rules.

use std::{
    fs::{self, File},
    mem::size_of,
    path::{Path, PathBuf},
};

use color_eyre::eyre::{ensure, eyre, Context, Result};
use rand::RngCore;
use sha2::{Digest, Sha256};
use zakura_chain::{
    block::{self, Height},
    parameters::spentness_hints::{Commitment, VerifiedArtifact},
    serialization::{ZcashDeserializeInto, ZcashSerialize},
    transaction::Transaction,
    transparent::{Input, OrderedUtxo, OutPoint, MIN_TRANSPARENT_COINBASE_MATURITY},
};
use zakura_state::{Config, ZakuraDb};

use super::{canonical_block, exact_boundary, open, write};

const ORACLE_WRITE_BUFFER_BYTES: usize = 16 * 1024 * 1024;
const ORACLE_WRITE_BUFFER_COUNT: i32 = 2;
const SALTED_HASH_DOMAIN: &[u8] = b"Zakura spentness audit v1\0";
const REPORT_SCHEMA_VERSION: u8 = 1;
/// Oracle identity recorded in the report and checked by the release importer.
const ORACLE_NAME: &str = "transparent-replay-v1";

/// Verify the artifact against `db`, whose tip must be the artifact's terminal block.
///
/// Returns the survivor count.
pub(super) fn verify(db: &ZakuraDb, artifact: &VerifiedArtifact) -> Result<u64> {
    let commitment = artifact.commitment();
    let terminal_hash = block::Hash(commitment.terminal_block_hash);
    let genesis = exact_boundary(db, commitment.terminal_height, terminal_hash)?;
    ensure!(
        genesis == commitment.chain_identity,
        "chain identity mismatch"
    );

    let oracle = Oracle::open()?;
    oracle.replay_chain(db, commitment.terminal_height)?;
    oracle.compare_with(db)?;

    let mut audit = BitAudit::new(db, artifact);
    for h in 0..=commitment.terminal_height {
        let height = Height(h);
        let block = canonical_block(db, height)?;
        for (tx_index, tx) in block.transactions.iter().enumerate() {
            audit.check_transaction(height, tx_index, tx)?;
        }
    }
    let survivors = audit.finish()?;
    ensure!(
        exact_boundary(db, commitment.terminal_height, terminal_hash)? == genesis,
        "archive changed during verification"
    );
    Ok(survivors)
}

/// Verify an artifact file and optionally write machine-readable evidence.
pub(super) fn run(
    state: &Path,
    artifact_path: &Path,
    commitment_path: &Path,
    report_path: Option<PathBuf>,
) -> Result<()> {
    let commitment: Commitment = serde_json::from_reader(File::open(commitment_path)?)?;
    let artifact = VerifiedArtifact::read(File::open(artifact_path)?, &commitment)
        .wrap_err("authenticating artifact")?;
    let survivors = verify(&open(state)?, &artifact)?;
    if let Some(report_path) = report_path {
        let report = serde_json::json!({
            "schema_version": REPORT_SCHEMA_VERSION,
            "commitment": commitment,
            "survivor_count": survivors,
            "oracle": ORACLE_NAME,
            "complete_entries": true,
            "salted_multiset": true,
        });
        write(report_path, &serde_json::to_vec_pretty(&report)?)?;
    }
    println!(
        "verified outputs={} survivors={survivors} sha256={}",
        commitment.output_count,
        commitment.digest_hex()
    );
    Ok(())
}

/// An outpoint-keyed transparent UTXO set in a scratch RocksDB.
///
/// Each value holds the creation height (`u32` little-endian), a coinbase flag byte,
/// and the serialized output.
struct Oracle {
    // Declared before `_scratch` so the database closes before its directory is removed.
    db: rocksdb::DB,
    _scratch: tempfile::TempDir,
}

impl Oracle {
    /// Open a fresh oracle under `$TMPDIR`, or under Zakura's cache when it is unset.
    fn open() -> Result<Self> {
        let scratch_root = std::env::var_os("TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| Config::default().cache_dir.join("spentness-audit"));
        fs::create_dir_all(&scratch_root)?;
        let scratch = tempfile::Builder::new()
            .prefix("spentness-audit-")
            .tempdir_in(scratch_root)?;
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options.set_write_buffer_size(ORACLE_WRITE_BUFFER_BYTES);
        options.set_max_write_buffer_number(ORACLE_WRITE_BUFFER_COUNT);
        let db = rocksdb::DB::open(&options, scratch.path())?;
        Ok(Self {
            db,
            _scratch: scratch,
        })
    }

    /// Replay blocks 1 through `height`. Genesis outputs are unspendable and never enter.
    fn replay_chain(&self, db: &ZakuraDb, height: u32) -> Result<()> {
        for h in 1..=height {
            let block = canonical_block(db, Height(h))?;
            for (tx_index, tx) in block.transactions.iter().enumerate() {
                self.apply_transaction(Height(h), tx_index, tx)?;
            }
        }
        Ok(())
    }

    /// Spend every transparent input, then create every output.
    fn apply_transaction(&self, height: Height, tx_index: usize, tx: &Transaction) -> Result<()> {
        for outpoint in tx.inputs().iter().filter_map(Input::outpoint) {
            self.spend(height, outpoint)?;
        }
        let hash = tx.hash();
        for (index, output) in tx.outputs().iter().enumerate() {
            let key = OutPoint {
                hash,
                index: u32::try_from(index)?,
            }
            .zcash_serialize_to_vec()?;
            ensure!(
                self.db.get(&key)?.is_none(),
                "oracle found a duplicate output"
            );
            let entry = OrderedUtxo::new(output.clone(), height, tx_index);
            self.db.put(key, encode_entry(&entry)?)?;
        }
        Ok(())
    }

    fn spend(&self, height: Height, outpoint: OutPoint) -> Result<()> {
        let key = outpoint.zcash_serialize_to_vec()?;
        let entry = self.db.get(&key)?.ok_or_else(|| {
            eyre!("oracle found a missing, future, or duplicate spend: {outpoint:?}")
        })?;
        let (creation_height, from_coinbase) = decode_entry_origin(&entry)?;
        let mature = creation_height
            .checked_add(MIN_TRANSPARENT_COINBASE_MATURITY)
            .is_some_and(|mature| height.0 >= mature);
        ensure!(
            !from_coinbase || mature,
            "oracle found an immature coinbase spend"
        );
        self.db.delete(key)?;
        Ok(())
    }

    /// Require the ordinary state to hold exactly the oracle's complete entries.
    fn compare_with(&self, db: &ZakuraDb) -> Result<()> {
        let mut count = 0;
        for row in self.db.iterator(rocksdb::IteratorMode::Start) {
            let (key, expected) = row?;
            let outpoint = key.as_ref().zcash_deserialize_into::<OutPoint>()?;
            let entry = db
                .utxo(&outpoint)
                .ok_or_else(|| eyre!("ordinary state omitted an oracle survivor"))?;
            ensure!(
                encode_entry(&entry)?.as_slice() == expected.as_ref(),
                "ordinary state differs from complete oracle UTXO entry"
            );
            count += 1;
        }
        ensure!(
            db.utxos_by_location().count() == count,
            "ordinary state has unmatched UTXOs"
        );
        Ok(())
    }
}

fn encode_entry(entry: &OrderedUtxo) -> Result<Vec<u8>> {
    let mut bytes = entry.utxo.height.0.to_le_bytes().to_vec();
    bytes.push(u8::from(entry.utxo.from_coinbase));
    bytes.extend_from_slice(&entry.utxo.output.zcash_serialize_to_vec()?);
    Ok(bytes)
}

/// Decode the creation height and coinbase flag from an oracle entry.
fn decode_entry_origin(bytes: &[u8]) -> Result<(u32, bool)> {
    let (height, rest) = bytes
        .split_at_checked(size_of::<u32>())
        .ok_or_else(|| eyre!("oracle UTXO entry has a truncated height"))?;
    let (&coinbase, _) = rest
        .split_first()
        .ok_or_else(|| eyre!("oracle UTXO entry has no coinbase flag"))?;
    ensure!(
        coinbase <= 1,
        "oracle UTXO entry has an invalid coinbase flag"
    );
    Ok((u32::from_le_bytes(height.try_into()?), coinbase == 1))
}

/// A sum of salted outpoint hashes modulo 2^256.
///
/// Unlike XOR, addition preserves duplicate multiplicity.
#[derive(Debug, Default, Eq, PartialEq)]
struct SaltedSum([u8; 32]);

impl SaltedSum {
    fn add(&mut self, value: [u8; 32]) {
        let mut carry = 0u16;
        for (byte, value) in self.0.iter_mut().zip(value) {
            let [low, high] = (u16::from(*byte) + u16::from(value) + carry).to_le_bytes();
            *byte = low;
            carry = u16::from(high);
        }
    }
}

/// Checks each membership bit against the ordinary UTXO set.
struct BitAudit<'a> {
    db: &'a ZakuraDb,
    artifact: &'a VerifiedArtifact,
    salt: [u8; 32],
    /// Salted outpoints of non-genesis outputs absent from the terminal UTXO set.
    spent_outputs: SaltedSum,
    /// Salted outpoints of every transparent input.
    inputs: SaltedSum,
    ordinal: u64,
    survivors: u64,
}

impl<'a> BitAudit<'a> {
    fn new(db: &'a ZakuraDb, artifact: &'a VerifiedArtifact) -> Self {
        let mut salt = [0; 32];
        rand::rngs::OsRng.fill_bytes(&mut salt);
        Self {
            db,
            artifact,
            salt,
            spent_outputs: SaltedSum::default(),
            inputs: SaltedSum::default(),
            ordinal: 0,
            survivors: 0,
        }
    }

    fn salted_hash(&self, outpoint: OutPoint) -> Result<[u8; 32]> {
        let mut hash = Sha256::new();
        hash.update(SALTED_HASH_DOMAIN);
        hash.update(self.salt);
        hash.update(outpoint.zcash_serialize_to_vec()?);
        Ok(hash.finalize().into())
    }

    fn check_transaction(
        &mut self,
        height: Height,
        tx_index: usize,
        tx: &Transaction,
    ) -> Result<()> {
        for outpoint in tx.inputs().iter().filter_map(Input::outpoint) {
            let hash = self.salted_hash(outpoint)?;
            self.inputs.add(hash);
        }
        let tx_hash = tx.hash();
        for (index, output) in tx.outputs().iter().enumerate() {
            let outpoint = OutPoint {
                hash: tx_hash,
                index: u32::try_from(index)?,
            };
            let expected = OrderedUtxo::new(output.clone(), height, tx_index);
            self.check_output(outpoint, expected)?;
        }
        Ok(())
    }

    fn check_output(&mut self, outpoint: OutPoint, expected: OrderedUtxo) -> Result<()> {
        let ordinal = self.ordinal;
        let retained = self.artifact.retains(ordinal)?;
        self.ordinal += 1;

        let genesis = expected.utxo.height.0 == 0;
        match self.db.utxo(&outpoint) {
            Some(entry) => {
                ensure!(retained, "wrong survivor bit at ordinal {ordinal}");
                ensure!(
                    !genesis && entry == expected,
                    "oracle UTXO entry differs at {outpoint:?}"
                );
                self.survivors += 1;
            }
            None => {
                ensure!(!retained, "wrong survivor bit at ordinal {ordinal}");
                // Genesis outputs are unspendable, so no input can match them.
                if !genesis {
                    let hash = self.salted_hash(outpoint)?;
                    self.spent_outputs.add(hash);
                }
            }
        }
        Ok(())
    }

    /// Check totals after every output, and return the survivor count.
    fn finish(self) -> Result<u64> {
        ensure!(
            self.ordinal == self.artifact.commitment().output_count,
            "output count differs from artifact"
        );
        ensure!(
            u64::try_from(self.db.utxos_by_location().count())? == self.survivors,
            "oracle contains unmatched UTXOs"
        );
        ensure!(
            self.spent_outputs == self.inputs,
            "salted spent-output/input multiset mismatch"
        );
        Ok(self.survivors)
    }
}

#[cfg(test)]
mod tests {
    use zakura_chain::transaction::LockTime;
    use zakura_chain::transparent::{Output, Script};

    use super::*;

    fn spend_of(outpoint: OutPoint, outputs: Vec<Output>) -> Transaction {
        Transaction::V1 {
            inputs: vec![Input::PrevOut {
                outpoint,
                unlock_script: Script::new(&[]),
                sequence: 0,
            }],
            outputs,
            lock_time: LockTime::unlocked(),
        }
    }

    #[test]
    fn oracle_enforces_spend_order_maturity_and_complete_entries() -> Result<()> {
        let oracle = Oracle::open()?;
        let output = Output::new(1u64.try_into()?, Script::new(&[0x6a]));
        let funding = Transaction::V1 {
            inputs: vec![Input::Coinbase {
                height: Height(1),
                data: vec![],
                sequence: 0,
            }],
            outputs: vec![output.clone()],
            lock_time: LockTime::unlocked(),
        };
        let funding_outpoint = OutPoint {
            hash: funding.hash(),
            index: 0,
        };
        let spend = spend_of(funding_outpoint, vec![output.clone()]);

        ensure!(
            oracle.apply_transaction(Height(101), 1, &spend).is_err(),
            "future output was accepted"
        );
        oracle.apply_transaction(Height(1), 0, &funding)?;
        ensure!(
            oracle
                .db
                .get(funding_outpoint.zcash_serialize_to_vec()?)?
                .is_some(),
            "non-address scripts must remain in the UTXO set"
        );
        ensure!(
            oracle.apply_transaction(Height(100), 1, &spend).is_err(),
            "immature coinbase was accepted"
        );
        oracle.apply_transaction(Height(101), 1, &spend)?;
        ensure!(
            oracle.apply_transaction(Height(101), 2, &spend).is_err(),
            "duplicate spend was accepted"
        );

        let spend_outpoint = OutPoint {
            hash: spend.hash(),
            index: 0,
        };
        let entry = oracle
            .db
            .get(spend_outpoint.zcash_serialize_to_vec()?)?
            .ok_or_else(|| eyre!("survivor missing"))?;
        ensure!(
            entry == encode_entry(&OrderedUtxo::new(output, Height(101), 1))?,
            "value, script, creation height, or coinbase flag differs"
        );

        oracle.apply_transaction(Height(101), 2, &spend_of(spend_outpoint, vec![]))?;
        ensure!(
            oracle
                .db
                .iterator(rocksdb::IteratorMode::Start)
                .next()
                .is_none(),
            "same-block spend left an output"
        );
        Ok(())
    }

    #[test]
    fn salted_sum_preserves_multiplicity_and_wraps() {
        let mut one = [0; 32];
        one[0] = 1;
        let mut sum = SaltedSum([255; 32]);
        sum.add(one);
        assert_eq!(sum, SaltedSum::default());
        sum.add(one);
        sum.add(one);
        assert_eq!(sum.0[0], 2);
    }
}
