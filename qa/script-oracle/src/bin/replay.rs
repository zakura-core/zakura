//! Replays every transaction in a Zakura finalized state through both script adapters.
//!
//! ```text
//! replay <state-dir> [--network mainnet|testnet] [--from H] [--to H] [--checkpoint FILE]
//! ```
//!
//! `<state-dir>` is the RocksDB directory, such as `.../state/v30/mainnet`. The tool opens it
//! read-only and reads the `tx_by_loc` column family directly, so it works with any database
//! version that stores transactions in their consensus serialization under 3-byte big-endian
//! heights and 2-byte big-endian indexes.
//!
//! Previous outputs come from an in-memory map of unspent transparent outputs, which the walk
//! maintains from genesis. Below `--from`, the walk only maintains the map. A checkpoint file
//! records the next height and the totals so far; a restart fast-forwards the map to it.

use std::{
    collections::HashMap,
    fmt::Write as _,
    fs,
    panic::{self, AssertUnwindSafe},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering::Relaxed},
        Arc, Mutex,
    },
    time::Instant,
};

use rayon::prelude::*;
use rocksdb::{IteratorMode, Options, DB};
use zakura_chain::{
    block::Height,
    parameters::{Network, NetworkUpgrade},
    serialization::ZcashDeserializeInto as _,
    transaction::Transaction,
    transparent::{self, OutPoint},
};
use zakura_script_oracle::{check_script, check_script_sigops, check_transaction, Verdict};

/// Blocks read, deserialized, and verified per batch.
const BATCH_BLOCKS: u32 = 2_000;

/// zcashd's `MAX_BLOCK_SIGOPS`.
const MAX_BLOCK_SIGOPS: u64 = 20_000;

/// Returns the `tx_by_loc` key prefix of `height`: its low 3 bytes, big-endian.
fn height_key(height: u32) -> [u8; 3] {
    let [_, a, b, c] = height.to_be_bytes();
    [a, b, c]
}

/// Parses a `tx_by_loc` key into its height.
fn key_height(key: &[u8]) -> u32 {
    u32::from_be_bytes([0, key[0], key[1], key[2]])
}

/// Totals over the verified range. A checkpoint stores them as `name=value` lines.
#[derive(Default)]
struct Totals {
    blocks: AtomicU64,
    transactions: AtomicU64,
    inputs: AtomicU64,
    script_only_inputs: AtomicU64,
    outputs: AtomicU64,
    legacy_sigops: AtomicU64,
    p2sh_sigops: AtomicU64,
    max_block_sigops: AtomicU64,
    findings: AtomicU64,
}

impl Totals {
    fn fields(&self) -> [(&'static str, &AtomicU64); 9] {
        [
            ("blocks", &self.blocks),
            ("transactions", &self.transactions),
            ("inputs", &self.inputs),
            ("script_only_inputs", &self.script_only_inputs),
            ("outputs", &self.outputs),
            ("legacy_sigops", &self.legacy_sigops),
            ("p2sh_sigops", &self.p2sh_sigops),
            ("max_block_sigops", &self.max_block_sigops),
            ("findings", &self.findings),
        ]
    }

    fn render(&self) -> String {
        self.fields()
            .iter()
            .fold(String::new(), |mut out, (name, value)| {
                let _ = write!(out, "{name}={} ", value.load(Relaxed));
                out
            })
    }
}

/// The checkpoint: the next height to verify and the totals before it.
fn write_checkpoint(path: &Path, next: u32, totals: &Totals) {
    let mut text = format!("next={next}\n");
    for (name, value) in totals.fields() {
        let _ = writeln!(text, "{name}={}", value.load(Relaxed));
    }
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, text).expect("the checkpoint directory is writable");
    fs::rename(&tmp, path).expect("the checkpoint directory is writable");
}

fn read_checkpoint(path: &Path, totals: &Totals) -> Option<u32> {
    let text = fs::read_to_string(path).ok()?;
    let values: HashMap<&str, u64> = text
        .lines()
        .filter_map(|line| line.split_once('='))
        .map(|(name, value)| (name, value.parse().expect("checkpoint values are integers")))
        .collect();
    for (name, value) in totals.fields() {
        value.store(values[name], Relaxed);
    }
    Some(u32::try_from(values["next"]).expect("checkpoint heights fit in u32"))
}

/// One transaction with its height and the outputs its inputs spend.
struct Job {
    height: u32,
    index: usize,
    transaction: Arc<Transaction>,
    previous_outputs: Vec<transparent::Output>,
}

/// Spends `transaction`'s inputs from `utxos` and adds its outputs.
///
/// Returns the spent outputs in input order. Coinbase transactions spend nothing.
fn apply(
    utxos: &mut HashMap<OutPoint, transparent::Output>,
    transaction: &Transaction,
) -> Vec<transparent::Output> {
    let spent = transaction
        .inputs()
        .iter()
        .filter_map(transparent::Input::outpoint)
        .map(|outpoint| {
            utxos
                .remove(&outpoint)
                .unwrap_or_else(|| panic!("{outpoint:?} is unspent in a valid chain"))
        })
        .collect();
    let hash = transaction.hash();
    for (index, output) in transaction.outputs().iter().enumerate() {
        let index = u32::try_from(index).expect("output counts fit in u32");
        utxos.insert(OutPoint { hash, index }, output.clone());
    }
    spent
}

/// Verifies one transaction with both adapters and returns the number of inputs that only had
/// their scripts compared.
///
/// Neither adapter can compute a pre-Overwinter (V1 or V2) sighash, so both refuse to prepare
/// those transactions. Their inputs are evaluated by both interpreters with a failing sighash
/// callback instead, which compares parsing and evaluation but not signature success.
///
/// Any rejection of a non-coinbase input is a finding: every input in a finalized chain is valid.
fn verify(job: &Job, network: &Network) -> Result<u64, String> {
    let tx = &job.transaction;
    for input in tx.inputs() {
        if let transparent::Input::PrevOut { unlock_script, .. } = input {
            check_script_sigops(unlock_script.as_raw_bytes());
        }
    }
    for output in tx.outputs() {
        check_script_sigops(output.lock_script.as_raw_bytes());
    }
    // Most shielded transactions have no transparent inputs. Preparing them would only convert
    // the whole transaction twice to compare no input, so compare their legacy sigops directly.
    if job.previous_outputs.is_empty() && !tx.is_coinbase() {
        let baseline = zakura_script_oracle::baseline::Sigops::sigops(tx.as_ref())
            .expect("the C++ adapter counts sigops");
        if zakura_script::Sigops::sigops(tx.as_ref()) != baseline {
            return Err("legacy sigops differ".into());
        }
        return Ok(0);
    }
    let nu = NetworkUpgrade::current(network, Height(job.height));
    let verdicts = check_transaction(tx.clone(), Arc::new(job.previous_outputs.clone()), nu);
    match verdicts {
        _ if tx.is_coinbase() => Ok(0),
        Some(verdicts) => match verdicts.iter().position(|v| *v == Verdict::Rejected) {
            Some(input) => Err(format!("both adapters reject input {input}")),
            None => Ok(0),
        },
        None if tx.version() < 3 => {
            for (input, output) in tx.inputs().iter().zip(&job.previous_outputs) {
                if let transparent::Input::PrevOut { unlock_script, .. } = input {
                    let raw = zcash_script::script::Raw::from_raw_parts(
                        unlock_script.as_raw_bytes().to_vec(),
                        output.lock_script.as_raw_bytes().to_vec(),
                    );
                    let is_final = input.sequence() == u32::MAX;
                    check_script(&raw, tx.raw_lock_time(), is_final, &|_, _| None);
                }
            }
            // `usize` to `u64` is lossless on supported targets.
            Ok(job.previous_outputs.len() as u64)
        }
        None => Err("both adapters refuse to prepare the transaction".into()),
    }
}

/// Returns the candidate's legacy plus P2SH sigops for one transaction.
fn sigops(job: &Job) -> (u64, u64) {
    let tx = &job.transaction;
    let legacy = zakura_script::Sigops::sigops(tx.as_ref());
    let p2sh = if tx.is_coinbase() {
        0
    } else {
        zakura_script::p2sh_sigop_count(tx, &job.previous_outputs)
    };
    (legacy.into(), p2sh.into())
}

struct Args {
    state: PathBuf,
    network: Network,
    from: u32,
    to: u32,
    checkpoint: Option<PathBuf>,
}

fn parse_args() -> Args {
    let mut args = std::env::args().skip(1);
    let usage = "usage: replay <state-dir> [--network mainnet|testnet] [--from H] [--to H] [--checkpoint FILE]";
    let mut parsed = Args {
        state: args.next().expect(usage).into(),
        network: Network::Mainnet,
        from: 0,
        to: u32::MAX,
        checkpoint: None,
    };
    while let Some(flag) = args.next() {
        let value = args.next().expect(usage);
        match flag.as_str() {
            "--network" if value == "mainnet" => parsed.network = Network::Mainnet,
            "--network" if value == "testnet" => parsed.network = Network::new_default_testnet(),
            "--from" => parsed.from = value.parse().expect(usage),
            "--to" => parsed.to = value.parse().expect(usage),
            "--checkpoint" => parsed.checkpoint = Some(value.into()),
            _ => panic!("{usage}"),
        }
    }
    parsed
}

fn main() {
    let args = parse_args();
    let (totals, findings) = run(&args);
    println!("SUMMARY network={} {}", args.network, totals.render());
    for finding in findings {
        println!("{finding}");
    }
}

/// Replays `args.from..=args.to`, or from the checkpoint, and returns the totals and findings.
fn run(args: &Args) -> (Totals, Vec<String>) {
    let totals = Totals::default();
    let from = args
        .checkpoint
        .as_deref()
        .and_then(|path| read_checkpoint(path, &totals))
        .unwrap_or(args.from);

    let cfs = DB::list_cf(&Options::default(), &args.state)
        .expect("the state directory is a RocksDB database");
    let db = DB::open_cf_for_read_only(&Options::default(), &args.state, cfs, false)
        .expect("the database opens read-only");
    let tx_by_loc = db
        .cf_handle("tx_by_loc")
        .expect("the state has a tx_by_loc column family");
    let tip = db
        .iterator_cf(tx_by_loc, IteratorMode::End)
        .next()
        .map(|item| key_height(&item.expect("the database is readable").0))
        .expect("the state has transactions");
    let to = args.to.min(tip);
    eprintln!("replaying {from}..={to} (tip {tip}); maintaining outputs from genesis");

    let findings = Mutex::new(Vec::new());
    let record = |finding: String| {
        eprintln!("{finding}");
        totals.findings.fetch_add(1, Relaxed);
        findings.lock().unwrap().push(finding);
    };
    let mut utxos = HashMap::new();
    let start = Instant::now();
    let mut batch_start = 0;
    while batch_start <= to {
        let batch_end = batch_start.saturating_add(BATCH_BLOCKS - 1).min(to);
        let raw: Vec<(u32, Vec<u8>)> = db
            .iterator_cf(
                tx_by_loc,
                IteratorMode::From(&height_key(batch_start), rocksdb::Direction::Forward),
            )
            .map(|item| item.expect("the database is readable"))
            .map(|(key, value)| (key_height(&key), value.into_vec()))
            .take_while(|(height, _)| *height <= batch_end)
            .collect();
        let transactions: Vec<(u32, Arc<Transaction>)> = raw
            .into_par_iter()
            .map(|(height, bytes)| {
                let tx = bytes
                    .zcash_deserialize_into()
                    .unwrap_or_else(|e| panic!("transaction at height {height} deserializes: {e}"));
                (height, Arc::new(tx))
            })
            .collect();

        let mut jobs = Vec::with_capacity(transactions.len());
        let mut index = 0;
        let mut last_height = None;
        for (height, transaction) in transactions {
            index = if last_height == Some(height) {
                index + 1
            } else {
                0
            };
            last_height = Some(height);
            let previous_outputs = apply(&mut utxos, &transaction);
            if height >= from {
                jobs.push(Job {
                    height,
                    index,
                    transaction,
                    previous_outputs,
                });
            }
        }

        if !jobs.is_empty() {
            let block_sigops: Mutex<HashMap<u32, u64>> = Mutex::default();
            jobs.par_iter().for_each(|job| {
                let tx = &job.transaction;
                let result = panic::catch_unwind(AssertUnwindSafe(|| verify(job, &args.network)))
                    .unwrap_or_else(|panic| {
                        Err(panic
                            .downcast_ref::<String>()
                            .cloned()
                            .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                            .unwrap_or_else(|| "panic".into()))
                    });
                match result {
                    Ok(script_only) => {
                        totals.script_only_inputs.fetch_add(script_only, Relaxed);
                    }
                    Err(reason) => {
                        record(format!(
                            "FINDING height={} index={} txid={} version={}: {reason}",
                            job.height,
                            job.index,
                            tx.hash(),
                            tx.version()
                        ));
                    }
                }
                let (legacy, p2sh) = sigops(job);
                *block_sigops.lock().unwrap().entry(job.height).or_default() += legacy + p2sh;
                let inputs = tx
                    .inputs()
                    .iter()
                    .filter(|i| i.outpoint().is_some())
                    .count();
                totals.transactions.fetch_add(1, Relaxed);
                totals.inputs.fetch_add(inputs as u64, Relaxed);
                totals.outputs.fetch_add(tx.outputs().len() as u64, Relaxed);
                totals.legacy_sigops.fetch_add(legacy, Relaxed);
                totals.p2sh_sigops.fetch_add(p2sh, Relaxed);
            });
            let block_sigops = block_sigops.into_inner().unwrap();
            // `usize` to `u64` is lossless on supported targets.
            totals.blocks.fetch_add(block_sigops.len() as u64, Relaxed);
            for (height, sigops) in block_sigops {
                totals.max_block_sigops.fetch_max(sigops, Relaxed);
                if sigops > MAX_BLOCK_SIGOPS {
                    record(format!(
                        "FINDING height={height}: {sigops} sigops exceed MAX_BLOCK_SIGOPS"
                    ));
                }
            }
            if let Some(path) = &args.checkpoint {
                write_checkpoint(path, batch_end + 1, &totals);
            }
        }
        eprintln!(
            "height={batch_end} utxos={} elapsed={:.0}s {}",
            utxos.len(),
            start.elapsed().as_secs_f64(),
            totals.render()
        );
        batch_start = batch_end + 1;
    }
    eprintln!(
        "heights={from}..={to} elapsed={:.0}s",
        start.elapsed().as_secs_f64()
    );
    (totals, findings.into_inner().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::block::Block;

    #[test]
    fn height_keys_round_trip_and_sort() {
        for height in [0, 1, 255, 256, 347_500, 3_490_665, 0x00ff_ffff] {
            let key = [&height_key(height)[..], &[0, 7]].concat();
            assert_eq!(key_height(&key), height);
        }
        assert!(height_key(255) < height_key(256));
        assert!(height_key(65_535) < height_key(65_536));
    }

    #[test]
    fn checkpoint_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("checkpoint");
        let totals = Totals::default();
        for (value, (_, field)) in totals.fields().iter().enumerate() {
            field.store(value as u64 + 1, Relaxed);
        }
        write_checkpoint(&path, 42, &totals);
        let restored = Totals::default();
        assert_eq!(read_checkpoint(&path, &restored), Some(42));
        assert_eq!(restored.render(), totals.render());
    }

    fn mainnet_block(bytes: &[u8]) -> Block {
        bytes.zcash_deserialize_into().unwrap()
    }

    #[test]
    fn apply_spends_and_creates_outputs() {
        let coinbase =
            mainnet_block(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES).transactions[0].clone();
        let mut utxos = HashMap::new();
        assert!(apply(&mut utxos, &coinbase).is_empty());
        assert_eq!(utxos.len(), coinbase.outputs().len());

        let spend = Transaction::V4 {
            inputs: vec![transparent::Input::PrevOut {
                outpoint: OutPoint {
                    hash: coinbase.hash(),
                    index: 0,
                },
                unlock_script: transparent::Script::new(&[]),
                sequence: u32::MAX,
            }],
            outputs: vec![],
            lock_time: zakura_chain::transaction::LockTime::unlocked(),
            expiry_height: Height(0),
            joinsplit_data: None,
            sapling_shielded_data: None,
        };
        assert_eq!(
            apply(&mut utxos, &spend),
            vec![coinbase.outputs()[0].clone()]
        );
        assert_eq!(utxos.len(), coinbase.outputs().len() - 1);
    }

    /// Replays Mainnet blocks 0 to 10 from a database with the state's `tx_by_loc` layout.
    #[test]
    fn replays_a_tx_by_loc_database() {
        use zakura_test::vectors::*;
        let dir = tempfile::tempdir().unwrap();
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        {
            let db = DB::open_cf(&options, dir.path(), ["tx_by_loc"]).unwrap();
            let cf = db.cf_handle("tx_by_loc").unwrap();
            let blocks = [
                &BLOCK_MAINNET_GENESIS_BYTES[..],
                &BLOCK_MAINNET_1_BYTES,
                &BLOCK_MAINNET_2_BYTES,
                &BLOCK_MAINNET_3_BYTES,
                &BLOCK_MAINNET_4_BYTES,
                &BLOCK_MAINNET_5_BYTES,
                &BLOCK_MAINNET_6_BYTES,
                &BLOCK_MAINNET_7_BYTES,
                &BLOCK_MAINNET_8_BYTES,
                &BLOCK_MAINNET_9_BYTES,
                &BLOCK_MAINNET_10_BYTES,
            ];
            for (height, bytes) in blocks.into_iter().enumerate() {
                let height = u32::try_from(height).unwrap();
                for (index, tx) in mainnet_block(bytes).transactions.iter().enumerate() {
                    let key = [
                        &height_key(height)[..],
                        &u16::try_from(index).unwrap().to_be_bytes(),
                    ]
                    .concat();
                    let value =
                        zakura_chain::serialization::ZcashSerialize::zcash_serialize_to_vec(
                            tx.as_ref(),
                        )
                        .unwrap();
                    db.put_cf(cf, key, value).unwrap();
                }
            }
        }
        let checkpoint = dir.path().join("checkpoint");
        let args = Args {
            state: dir.path().into(),
            network: Network::Mainnet,
            from: 3,
            to: u32::MAX,
            checkpoint: Some(checkpoint.clone()),
        };
        let (totals, findings) = run(&args);
        assert!(findings.is_empty(), "{findings:?}");
        assert_eq!(totals.blocks.load(Relaxed), 8);
        assert_eq!(totals.transactions.load(Relaxed), 8);
        assert_eq!(read_checkpoint(&checkpoint, &Totals::default()), Some(11));
    }
}
