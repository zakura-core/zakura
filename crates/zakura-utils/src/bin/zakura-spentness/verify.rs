//! Check an artifact against an independent transparent replay.
//!
//! One pass over the canonical blocks does two things:
//!
//! - The oracle builds its own outpoint-keyed UTXO set in scratch storage. It does not
//!   call the ordinary state writer or the generator's merge. It rejects missing,
//!   duplicate, future, and immature coinbase spends.
//! - The bit audit reads the ordinary state at each output location. It requires the
//!   output's bit to equal its presence, and a present entry to equal the creating output.
//!
//! After the pass, every oracle survivor must have a set bit, and the oracle, the set
//! bits, and the ordinary state must hold the same number of UTXOs. The source node's
//! full validation remains responsible for signatures, shielded proofs, and other
//! consensus rules.

use std::{
    collections::HashMap,
    fs::{self, File},
    path::{Path, PathBuf},
};

use color_eyre::eyre::{ensure, eyre, Context, Result};
use zakura_chain::{
    block::{self, Height},
    parameters::spentness_hints::{Commitment, VerifiedArtifact},
    serialization::ZcashSerialize,
    transparent::{Input, OrderedUtxo, OutPoint, MIN_TRANSPARENT_COINBASE_MATURITY},
};
use zakura_state::{Config, OutputLocation, ZakuraDb};

use super::{exact_boundary, for_each_block, open, write, CanonicalBlock};

/// The oracle's memtable size. Large memtables reduce flushes and level-zero files.
const ORACLE_WRITE_BUFFER_BYTES: usize = 128 * 1024 * 1024;
const ORACLE_WRITE_BUFFER_COUNT: i32 = 4;
/// Bloom filter bits per key. Most oracle reads are duplicate-output checks that miss.
const ORACLE_BLOOM_BITS_PER_KEY: f64 = 10.0;
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

    let mut audit = Audit {
        db,
        artifact,
        oracle: Oracle::open()?,
        ordinal: 0,
        survivors: 0,
    };
    for_each_block(db, 0..=commitment.terminal_height, "verified", |block| {
        audit.check_block(&block)
    })?;
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
        write(report_path, &report(&commitment, survivors)?)?;
    }
    println!(
        "verified outputs={} survivors={survivors} sha256={}",
        commitment.output_count,
        commitment.digest_hex()
    );
    Ok(())
}

/// The machine-readable verification evidence that the release importer checks.
pub(super) fn report(commitment: &Commitment, survivors: u64) -> Result<Vec<u8>> {
    let report = serde_json::json!({
        "schema_version": REPORT_SCHEMA_VERSION,
        "commitment": commitment,
        "survivor_count": survivors,
        "oracle": ORACLE_NAME,
        "complete_entries": true,
    });
    Ok(serde_json::to_vec_pretty(&report)?)
}

/// The single verification pass: the oracle replay and the bit audit.
struct Audit<'a> {
    db: &'a ZakuraDb,
    artifact: &'a VerifiedArtifact,
    oracle: Oracle,
    /// The artifact ordinal of the next output.
    ordinal: u64,
    /// Outputs whose bit is set and whose entry the ordinary state holds.
    survivors: u64,
}

impl Audit<'_> {
    /// Apply one block to the oracle and check its output bits.
    ///
    /// Genesis outputs are unspendable, so they never enter the oracle.
    fn check_block(&mut self, canonical: &CanonicalBlock) -> Result<()> {
        let height = canonical.height;
        for (tx_index, (tx, hash)) in canonical
            .block
            .transactions
            .iter()
            .zip(&canonical.tx_hashes)
            .enumerate()
        {
            if height.0 != 0 {
                for outpoint in tx.inputs().iter().filter_map(Input::outpoint) {
                    self.oracle.spend(height, outpoint)?;
                }
            }
            for (index, output) in tx.outputs().iter().enumerate() {
                let ordinal = self.ordinal;
                self.ordinal += 1;
                let retained = self.artifact.retains(ordinal)?;
                let location = OutputLocation::from_usize(height, tx_index, index);
                match self.db.utxo_by_location(location) {
                    Some(entry) => {
                        ensure!(retained, "wrong survivor bit at ordinal {ordinal}");
                        ensure!(
                            height.0 != 0
                                && entry == OrderedUtxo::new(output.clone(), height, tx_index),
                            "ordinary UTXO entry differs at {location:?}"
                        );
                        self.survivors += 1;
                    }
                    None => ensure!(!retained, "wrong survivor bit at ordinal {ordinal}"),
                }
                if height.0 != 0 {
                    let outpoint = OutPoint {
                        hash: *hash,
                        index: u32::try_from(index)?,
                    };
                    self.oracle.create(
                        outpoint,
                        OracleEntry {
                            height,
                            from_coinbase: tx.is_coinbase(),
                            ordinal,
                        },
                    )?;
                }
            }
        }
        self.oracle.flush()
    }

    /// Check that the oracle, the artifact, and the ordinary state agree on every
    /// survivor, and return the survivor count.
    fn finish(self) -> Result<u64> {
        ensure!(
            self.ordinal == self.artifact.commitment().output_count,
            "output count differs from artifact"
        );
        // Oracle survivors have distinct ordinals, so equal counts make the sets equal.
        let mut oracle_survivors = 0;
        for entry in self.oracle.survivors() {
            let ordinal = entry?.ordinal;
            ensure!(
                self.artifact.retains(ordinal)?,
                "artifact omits oracle survivor at ordinal {ordinal}"
            );
            oracle_survivors += 1;
        }
        ensure!(
            oracle_survivors == self.survivors,
            "artifact retains outputs that the oracle spent"
        );
        ensure!(
            u64::try_from(self.db.utxos_by_location().count())? == self.survivors,
            "ordinary state has UTXOs outside canonical outputs"
        );
        Ok(self.survivors)
    }
}

/// An oracle UTXO: its creation height, coinbase flag, and artifact ordinal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OracleEntry {
    height: Height,
    from_coinbase: bool,
    ordinal: u64,
}

impl OracleEntry {
    const LEN: usize = 13;

    fn encode(self) -> [u8; Self::LEN] {
        let mut bytes = [0; Self::LEN];
        bytes[..4].copy_from_slice(&self.height.0.to_le_bytes());
        bytes[4] = u8::from(self.from_coinbase);
        bytes[5..].copy_from_slice(&self.ordinal.to_le_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let bytes: &[u8; Self::LEN] = bytes
            .try_into()
            .map_err(|_| eyre!("oracle UTXO entry has the wrong length"))?;
        let (height, rest) = bytes.split_at(4);
        let (&coinbase, ordinal) = rest
            .split_first()
            .ok_or_else(|| eyre!("oracle UTXO entry has no coinbase flag"))?;
        ensure!(
            coinbase <= 1,
            "oracle UTXO entry has an invalid coinbase flag"
        );
        Ok(Self {
            height: Height(u32::from_le_bytes(height.try_into()?)),
            from_coinbase: coinbase == 1,
            ordinal: u64::from_le_bytes(ordinal.try_into()?),
        })
    }
}

/// An outpoint-keyed transparent UTXO set in a scratch RocksDB.
///
/// Changes collect in memory until [`Oracle::flush`] writes them as one batch.
/// The database is disposable, so writes skip the write-ahead log.
struct Oracle {
    // Declared before `_scratch` so the database closes before its directory is removed.
    db: rocksdb::DB,
    _scratch: tempfile::TempDir,
    /// Unflushed changes: `Some` creates an entry and `None` deletes it.
    pending: HashMap<Vec<u8>, Option<OracleEntry>>,
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
        let mut table = rocksdb::BlockBasedOptions::default();
        table.set_bloom_filter(ORACLE_BLOOM_BITS_PER_KEY, false);
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options.set_block_based_table_factory(&table);
        options.set_write_buffer_size(ORACLE_WRITE_BUFFER_BYTES);
        options.set_max_write_buffer_number(ORACLE_WRITE_BUFFER_COUNT);
        let db = rocksdb::DB::open(&options, scratch.path())?;
        Ok(Self {
            db,
            _scratch: scratch,
            pending: HashMap::new(),
        })
    }

    fn get(&self, key: &[u8]) -> Result<Option<OracleEntry>> {
        if let Some(change) = self.pending.get(key) {
            return Ok(*change);
        }
        self.db
            .get_pinned(key)?
            .map(|bytes| OracleEntry::decode(&bytes))
            .transpose()
    }

    fn create(&mut self, outpoint: OutPoint, entry: OracleEntry) -> Result<()> {
        let key = outpoint.zcash_serialize_to_vec()?;
        ensure!(self.get(&key)?.is_none(), "oracle found a duplicate output");
        self.pending.insert(key, Some(entry));
        Ok(())
    }

    fn spend(&mut self, height: Height, outpoint: OutPoint) -> Result<()> {
        let key = outpoint.zcash_serialize_to_vec()?;
        let entry = self.get(&key)?.ok_or_else(|| {
            eyre!("oracle found a missing, future, or duplicate spend: {outpoint:?}")
        })?;
        let mature = entry
            .height
            .0
            .checked_add(MIN_TRANSPARENT_COINBASE_MATURITY)
            .is_some_and(|mature| height.0 >= mature);
        ensure!(
            !entry.from_coinbase || mature,
            "oracle found an immature coinbase spend"
        );
        self.pending.insert(key, None);
        Ok(())
    }

    /// Write the pending changes.
    fn flush(&mut self) -> Result<()> {
        let mut batch = rocksdb::WriteBatch::default();
        for (key, change) in self.pending.drain() {
            match change {
                Some(entry) => batch.put(key, entry.encode()),
                None => batch.delete(key),
            }
        }
        let mut options = rocksdb::WriteOptions::default();
        options.disable_wal(true);
        self.db.write_opt(batch, &options)?;
        Ok(())
    }

    /// Every flushed entry, in key order.
    fn survivors(&self) -> impl Iterator<Item = Result<OracleEntry>> + '_ {
        self.db
            .iterator(rocksdb::IteratorMode::Start)
            .map(|row| OracleEntry::decode(&row?.1))
    }
}

#[cfg(test)]
mod tests {
    use zakura_chain::{
        transaction::{self, LockTime, Transaction},
        transparent::{Output, Script},
    };

    use super::*;

    fn spend_of(outpoint: OutPoint) -> Transaction {
        Transaction::V1 {
            inputs: vec![Input::PrevOut {
                outpoint,
                unlock_script: Script::new(&[]),
                sequence: 0,
            }],
            outputs: vec![Output::new(1u64.try_into().unwrap(), Script::new(&[0x6a]))],
            lock_time: LockTime::unlocked(),
        }
    }

    fn entry(height: u32, from_coinbase: bool, ordinal: u64) -> OracleEntry {
        OracleEntry {
            height: Height(height),
            from_coinbase,
            ordinal,
        }
    }

    #[test]
    fn oracle_enforces_spend_order_and_maturity() -> Result<()> {
        let mut oracle = Oracle::open()?;
        let funding = OutPoint {
            hash: spend_of(OutPoint {
                hash: transaction::Hash([1; 32]),
                index: 0,
            })
            .hash(),
            index: 0,
        };

        ensure!(
            oracle.spend(Height(101), funding).is_err(),
            "future output was accepted"
        );
        oracle.create(funding, entry(1, true, 7))?;
        oracle.flush()?;
        ensure!(
            oracle.create(funding, entry(2, false, 8)).is_err(),
            "duplicate output was accepted"
        );
        ensure!(
            oracle.spend(Height(100), funding).is_err(),
            "immature coinbase was accepted"
        );
        oracle.spend(Height(101), funding)?;
        ensure!(
            oracle.spend(Height(101), funding).is_err(),
            "duplicate spend in one block was accepted"
        );
        oracle.flush()?;
        ensure!(
            oracle.spend(Height(102), funding).is_err(),
            "duplicate spend across blocks was accepted"
        );

        // An output created and spent in one block never reaches the database.
        let same_block = OutPoint {
            hash: transaction::Hash([2; 32]),
            index: 3,
        };
        oracle.create(same_block, entry(102, false, 9))?;
        oracle.spend(Height(102), same_block)?;
        oracle.flush()?;
        ensure!(
            oracle.survivors().next().is_none(),
            "spent outputs remained in the oracle"
        );

        let survivor = entry(u32::MAX, false, u64::MAX);
        oracle.create(same_block, survivor)?;
        oracle.flush()?;
        ensure!(
            oracle.survivors().collect::<Result<Vec<_>>>()? == vec![survivor],
            "oracle entry encoding does not round-trip"
        );
        Ok(())
    }
}
