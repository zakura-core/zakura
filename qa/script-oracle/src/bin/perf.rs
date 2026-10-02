//! Times both script adapters on the transactions in a Zakura finalized state.
//!
//! ```text
//! perf <state-dir> [--from H] [--to H] [--sample N]
//! ```
//!
//! The walk maintains unspent transparent outputs from genesis like `replay`, and times every
//! `N`th batch of blocks in `--from..=--to`. For each transaction with transparent inputs, one
//! thread runs both adapters back to back, in alternating order, on the same prepared inputs:
//!
//! - script verification: `is_valid` for every input, plus `p2sh_sigops`;
//! - legacy sigop counting: `Sigops::sigops`, which the node also runs on every other
//!   transaction.
//!
//! Both adapters prepare the same `SigHasher`, so the tool times preparation once, for context.

use std::{
    collections::{BTreeMap, HashMap},
    hint::black_box,
    path::PathBuf,
    sync::{Arc, Mutex},
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
use zakura_script_oracle::baseline;

/// Blocks read and deserialized per batch.
const BATCH_BLOCKS: u32 = 2_000;

fn height_key(height: u32) -> [u8; 3] {
    let [_, a, b, c] = height.to_be_bytes();
    [a, b, c]
}

fn key_height(key: &[u8]) -> u32 {
    u32::from_be_bytes([0, key[0], key[1], key[2]])
}

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

/// Elapsed nanoseconds of `f`.
fn time<T>(f: impl FnOnce() -> T) -> u64 {
    let start = Instant::now();
    black_box(f());
    // A timed call takes far less than 584 years.
    start.elapsed().as_nanos() as u64
}

/// The spent output kinds of a transaction's inputs.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Kind {
    P2pkh,
    P2sh,
    Mixed,
}

fn kind(previous_outputs: &[transparent::Output]) -> Kind {
    let is_p2pkh = |script: &[u8]| {
        script.len() == 25
            && script.starts_with(&[0x76, 0xa9, 0x14])
            && script.ends_with(&[0x88, 0xac])
    };
    let is_p2sh = |script: &[u8]| script.len() == 23 && script[0] == 0xa9 && script[22] == 0x87;
    let scripts = || {
        previous_outputs
            .iter()
            .map(|o| o.lock_script.as_raw_bytes())
    };
    if scripts().all(is_p2pkh) {
        Kind::P2pkh
    } else if scripts().all(is_p2sh) {
        Kind::P2sh
    } else {
        Kind::Mixed
    }
}

/// One timed transaction.
struct Sample {
    version: u32,
    kind: Kind,
    inputs: u32,
    prepare: u64,
    rust_verify: u64,
    cxx_verify: u64,
    rust_sigops: u64,
    cxx_sigops: u64,
}

/// Times one transaction with transparent inputs. Returns `None` if neither adapter prepares it.
fn measure(
    tx: &Arc<Transaction>,
    previous_outputs: Vec<transparent::Output>,
    nu: NetworkUpgrade,
    rust_first: bool,
) -> Option<Sample> {
    let previous_outputs = Arc::new(previous_outputs);
    let start = Instant::now();
    let rust = zakura_script::CachedFfiTransaction::new(tx.clone(), previous_outputs.clone(), nu);
    // A preparation takes far less than 584 years.
    let prepare = start.elapsed().as_nanos() as u64;
    let cxx = baseline::CachedFfiTransaction::new(tx.clone(), previous_outputs.clone(), nu);
    let (rust, cxx) = match (rust, cxx) {
        (Ok(rust), Ok(cxx)) => (rust, cxx),
        (Err(_), Err(_)) => return None,
        _ => panic!("the adapters disagree on preparing {}", tx.hash()),
    };
    let inputs = tx.inputs().len();
    let run_rust = || {
        time(|| {
            for input in 0..inputs {
                assert!(rust.is_valid(input).is_ok(), "{} input {input}", tx.hash());
            }
            rust.p2sh_sigops()
        })
    };
    let run_cxx = || {
        time(|| {
            for input in 0..inputs {
                assert!(cxx.is_valid(input).is_ok(), "{} input {input}", tx.hash());
            }
            cxx.p2sh_sigops()
        })
    };
    let rust_sigops = || time(|| zakura_script::Sigops::sigops(tx.as_ref()));
    let cxx_sigops = || time(|| baseline::Sigops::sigops(tx.as_ref()));
    let (rust_verify, cxx_verify, rust_sigops, cxx_sigops) = if rust_first {
        let (a, b) = (run_rust(), run_cxx());
        let (c, d) = (rust_sigops(), cxx_sigops());
        (a, b, c, d)
    } else {
        let (b, a) = (run_cxx(), run_rust());
        let (d, c) = (cxx_sigops(), rust_sigops());
        (a, b, c, d)
    };
    Some(Sample {
        version: tx.version(),
        kind: kind(&previous_outputs),
        inputs: u32::try_from(inputs).expect("input counts fit in u32"),
        prepare,
        rust_verify,
        cxx_verify,
        rust_sigops,
        cxx_sigops,
    })
}

/// Times legacy sigop counting on a transaction without transparent inputs.
fn measure_sigops(tx: &Transaction, rust_first: bool) -> (u64, u64) {
    let rust = || time(|| zakura_script::Sigops::sigops(tx));
    let cxx = || time(|| baseline::Sigops::sigops(tx));
    if rust_first {
        let a = rust();
        (a, cxx())
    } else {
        let b = cxx();
        (rust(), b)
    }
}

#[derive(Default)]
struct Totals {
    txs: u64,
    inputs: u64,
    prepare: u64,
    rust_verify: u64,
    cxx_verify: u64,
    rust_sigops: u64,
    cxx_sigops: u64,
}

impl Totals {
    fn add(&mut self, s: &Sample) {
        self.txs += 1;
        self.inputs += u64::from(s.inputs);
        self.prepare += s.prepare;
        self.rust_verify += s.rust_verify;
        self.cxx_verify += s.cxx_verify;
        self.rust_sigops += s.rust_sigops;
        self.cxx_sigops += s.cxx_sigops;
    }
}

fn ratio(rust: u64, cxx: u64) -> f64 {
    rust as f64 / cxx.max(1) as f64
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let index = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[index]
}

fn report(samples: &[Sample], sigops_only: (u64, u64, u64)) {
    let mut groups: BTreeMap<String, Totals> = BTreeMap::new();
    let mut all = Totals::default();
    for s in samples {
        all.add(s);
        groups
            .entry(format!("V{} {:?}", s.version, s.kind))
            .or_default()
            .add(s);
    }
    println!("| Group | Txs | Inputs | Rust µs/input | C++ µs/input | Rust/C++ | Prepare µs/tx |");
    println!("| --- | --- | --- | --- | --- | --- | --- |");
    let row = |name: &str, t: &Totals| {
        let per_input = |ns: u64| ns as f64 / 1e3 / t.inputs.max(1) as f64;
        println!(
            "| {name} | {} | {} | {:.2} | {:.2} | {:.3} | {:.2} |",
            t.txs,
            t.inputs,
            per_input(t.rust_verify),
            per_input(t.cxx_verify),
            ratio(t.rust_verify, t.cxx_verify),
            t.prepare as f64 / 1e3 / t.txs.max(1) as f64,
        );
    };
    for (name, t) in &groups {
        row(name, t);
    }
    row("All", &all);

    let mut ratios: Vec<f64> = samples
        .iter()
        .map(|s| ratio(s.rust_verify, s.cxx_verify))
        .collect();
    ratios.sort_by(f64::total_cmp);
    println!();
    println!(
        "Per-transaction Rust/C++ verify ratio: p1={:.3} p10={:.3} median={:.3} p90={:.3} p99={:.3}",
        percentile(&ratios, 0.01),
        percentile(&ratios, 0.10),
        percentile(&ratios, 0.50),
        percentile(&ratios, 0.90),
        percentile(&ratios, 0.99),
    );

    let (count, rust, cxx) = sigops_only;
    let rust = rust + all.rust_sigops;
    let cxx = cxx + all.cxx_sigops;
    let count = count + all.txs;
    println!(
        "Legacy sigops over {count} transactions: Rust {:.0} ns/tx, C++ {:.0} ns/tx, Rust/C++ {:.3}",
        rust as f64 / count.max(1) as f64,
        cxx as f64 / count.max(1) as f64,
        ratio(rust, cxx),
    );
    println!(
        "Totals: Rust verify {:.1} s, C++ verify {:.1} s, shared preparation {:.1} s",
        all.rust_verify as f64 / 1e9,
        all.cxx_verify as f64 / 1e9,
        all.prepare as f64 / 1e9,
    );
}

fn main() {
    let usage = "usage: perf <state-dir> [--from H] [--to H] [--sample N]";
    let mut args = std::env::args().skip(1);
    let state: PathBuf = args.next().expect(usage).into();
    let (mut from, mut to, mut sample) = (0u32, u32::MAX, 1u32);
    while let Some(flag) = args.next() {
        let value: u32 = args.next().expect(usage).parse().expect(usage);
        match flag.as_str() {
            "--from" => from = value,
            "--to" => to = value,
            "--sample" => sample = value.max(1),
            _ => panic!("{usage}"),
        }
    }
    let network = Network::Mainnet;

    let cfs = DB::list_cf(&Options::default(), &state).expect("the state is a RocksDB database");
    let db = DB::open_cf_for_read_only(&Options::default(), &state, cfs, false)
        .expect("the database opens read-only");
    let tx_by_loc = db.cf_handle("tx_by_loc").expect("the state has tx_by_loc");
    let tip = db
        .iterator_cf(tx_by_loc, IteratorMode::End)
        .next()
        .map(|item| key_height(&item.expect("the database is readable").0))
        .expect("the state has transactions");
    let to = to.min(tip);
    eprintln!("timing {from}..={to}, every {sample} batch(es) of {BATCH_BLOCKS} blocks");

    let samples: Mutex<Vec<Sample>> = Mutex::default();
    let sigops_only = Mutex::new((0u64, 0u64, 0u64));
    let mut utxos = HashMap::new();
    let start = Instant::now();
    let mut batch_start = 0;
    let mut batch_number = 0u32;
    while batch_start <= to {
        let batch_end = batch_start.saturating_add(BATCH_BLOCKS - 1).min(to);
        let timed = batch_end >= from && batch_number % sample == 0;
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
        let mut jobs = Vec::new();
        for (height, transaction) in transactions {
            let previous_outputs = apply(&mut utxos, &transaction);
            if timed && height >= from && !transaction.is_coinbase() {
                jobs.push((height, transaction, previous_outputs));
            }
        }
        if timed {
            // Jobs run on the rayon pool, but each one times both adapters on its own thread.
            jobs.into_par_iter()
                .enumerate()
                .for_each(|(n, (height, tx, previous_outputs))| {
                    let rust_first = n % 2 == 0;
                    if previous_outputs.is_empty() {
                        let (rust, cxx) = measure_sigops(&tx, rust_first);
                        let mut totals = sigops_only.lock().unwrap();
                        totals.0 += 1;
                        totals.1 += rust;
                        totals.2 += cxx;
                        return;
                    }
                    let nu = NetworkUpgrade::current(&network, Height(height));
                    if let Some(s) = measure(&tx, previous_outputs, nu, rust_first) {
                        samples.lock().unwrap().push(s);
                    }
                });
            let samples = samples.lock().unwrap();
            let rust: u64 = samples.iter().map(|s| s.rust_verify).sum();
            let cxx: u64 = samples.iter().map(|s| s.cxx_verify).sum();
            eprintln!(
                "height={batch_end} elapsed={:.0}s txs={} rust/cxx={:.3}",
                start.elapsed().as_secs_f64(),
                samples.len(),
                ratio(rust, cxx)
            );
        }
        batch_start = batch_end + 1;
        batch_number += 1;
    }
    let samples = samples.into_inner().unwrap();
    println!(
        "heights {from}..={to}, every {sample} batch(es), {} timed transactions with transparent inputs",
        samples.len()
    );
    report(&samples, sigops_only.into_inner().unwrap());
}
