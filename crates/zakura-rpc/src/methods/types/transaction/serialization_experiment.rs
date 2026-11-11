//! Opt-in isolation of JSON escaping, copying, and complete-response validation.
//!
//! This module is a test-only experiment. Raw fragments never enter generic RPC types.
//! Run alone in release mode with TEMPLATE_PAYLOAD_FIXTURE and --test-threads=1.

use super::TransactionTemplate;
use crate::server::jsonrpc::{Id, MethodResponse};
use serde::Serialize;
use serde_json::value::RawValue;
use std::{
    hint::black_box,
    io,
    sync::{Arc, OnceLock},
    time::Instant,
};
use zakura_chain::{
    amount::{Amount, NonNegative},
    serialization::{ZcashDeserializeInto, ZcashSerialize},
    transaction::{self, Transaction, VerifiedUnminedTx},
};

/// Lazy ownership follows the exact immutable transaction; there is no ID lookup cache.
struct LazyJsonData {
    transaction: Arc<Transaction>,
    json: OnceLock<Box<RawValue>>,
}

impl LazyJsonData {
    fn new(transaction: Arc<Transaction>) -> Self {
        Self {
            transaction,
            json: OnceLock::new(),
        }
    }

    fn json(&self) -> &RawValue {
        self.json.get_or_init(|| {
            let bytes = self.transaction.zcash_serialize_to_vec().unwrap();
            checked_hex_json(&bytes)
        })
    }
}

impl Serialize for LazyJsonData {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.json().serialize(serializer)
    }
}

/// Checked JSON constructed exclusively from internally encoded lowercase hex.
fn checked_hex_json(bytes: &[u8]) -> Box<RawValue> {
    let hex = crate::methods::hex_data::encode(bytes);
    let mut json = String::with_capacity(hex.len() + 2);
    json.push('"');
    json.push_str(&hex);
    json.push('"');
    RawValue::from_string(json).unwrap()
}

/// Metadata is rebuilt independently of the data representation in every experiment.
#[derive(Serialize)]
struct Entry<D: Serialize> {
    data: D,
    #[serde(with = "hex")]
    hash: transaction::Hash,
    #[serde(rename = "authdigest", with = "hex")]
    auth_digest: transaction::AuthDigest,
    depends: Vec<u16>,
    fee: Amount<NonNegative>,
    sigops: u32,
    required: bool,
}

fn entry<D: Serialize>(tx: &VerifiedUnminedTx, data: D) -> Entry<D> {
    Entry {
        data,
        hash: tx.transaction.id().mined_id(),
        auth_digest: tx.transaction.id().auth_digest().unwrap(),
        depends: Vec::new(),
        fee: tx.miner_fee,
        sigops: tx.block_sigop_count(),
        required: false,
    }
}

fn lazy_entries(txs: &[VerifiedUnminedTx]) -> Vec<Entry<Arc<LazyJsonData>>> {
    txs.iter()
        .map(|tx| {
            entry(
                tx,
                Arc::new(LazyJsonData::new(tx.transaction.transaction().clone())),
            )
        })
        .collect()
}

fn response<T: Serialize>(entries: &T) -> MethodResponse {
    MethodResponse::response(Id::Number(1), &Ok(entries), 16 * 1024 * 1024)
}

fn fixture() -> Vec<VerifiedUnminedTx> {
    let path = std::env::var("TEMPLATE_PAYLOAD_FIXTURE").unwrap();
    let input: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let transactions: Vec<_> = input["transactions"]
        .as_array()
        .unwrap()
        .iter()
        .take(247)
        .map(|record| {
            let bytes =
                crate::methods::hex_data::decode(record["data"].as_str().unwrap().as_bytes())
                    .unwrap();
            let tx: Transaction = bytes.as_slice().zcash_deserialize_into().unwrap();
            assert_eq!(tx.version(), 6);
            assert_eq!(tx.shielded_action_counts().ironwood_actions, 2);
            VerifiedUnminedTx::new(
                tx.into(),
                800u64.try_into().unwrap(),
                0,
                0,
                Arc::new(vec![]),
            )
            .unwrap()
        })
        .collect();
    assert_eq!(transactions.len(), 247);
    transactions
}

fn measure(name: &str, iterations: usize, mut run: impl FnMut()) -> serde_json::Value {
    let start = Instant::now();
    for _ in 0..iterations {
        run();
    }
    let elapsed = start.elapsed();
    serde_json::json!({"stage": name, "iterations": iterations, "total_ns": elapsed.as_nanos()})
}

#[derive(Default)]
struct CountingWriter(usize);

impl io::Write for CountingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self.0.checked_add(bytes.len()).unwrap();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn count_json_bytes(value: &impl Serialize) -> usize {
    let mut writer = CountingWriter::default();
    serde_json::to_writer(&mut writer, black_box(value)).unwrap();
    black_box(writer.0)
}

#[test]
fn checked_fragments_preserve_hex_and_metadata() {
    for bytes in [vec![], vec![0, 0x22, 0x5c, 0xff], (0..=255).collect()] {
        let json = checked_hex_json(&bytes);
        assert_eq!(
            serde_json::from_str::<String>(json.get()).unwrap(),
            hex::encode(bytes)
        );
    }
    // Ordinary unrelated strings still escape through their normal serializer.
    let escaped = serde_json::json!({"data": checked_hex_json(&[0xff]), "text": "\"\\\n"});
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&escaped.to_string()).unwrap(),
        escaped
    );
}

#[test]
#[ignore = "requires the real replay fixture on an isolated benchmark host"]
#[allow(clippy::print_stdout)]
fn benchmark_json_data_serialization() {
    let txs = fixture();
    let selected = &txs[..165];
    let legacy: Vec<_> = selected.iter().map(TransactionTemplate::from).collect();
    let hex: Vec<_> = legacy
        .iter()
        .map(|tx| crate::methods::hex_data::encode(tx.data.as_ref()))
        .collect();
    let raw: Vec<_> = legacy
        .iter()
        .map(|tx| checked_hex_json(tx.data.as_ref()))
        .collect();
    let strings: Vec<_> = selected
        .iter()
        .zip(&hex)
        .map(|(tx, data)| entry(tx, data.as_str()))
        .collect();
    let raw_entries: Vec<_> = selected
        .iter()
        .zip(&raw)
        .map(|(tx, data)| entry(tx, data.as_ref()))
        .collect();
    let lazy = lazy_entries(selected);
    let expected = response(&legacy);
    let expected_json = expected.as_json().get();
    assert_eq!(response(&strings).as_json().get(), expected_json);
    assert_eq!(response(&raw_entries).as_json().get(), expected_json);
    assert_eq!(response(&lazy).as_json().get(), expected_json);
    let unserved = lazy_entries(selected);
    assert!(unserved.iter().all(|entry| entry.data.json.get().is_none()));
    drop(unserved);
    let concurrent = LazyJsonData::new(txs[0].transaction.transaction().clone());
    let barrier = std::sync::Barrier::new(8);
    std::thread::scope(|scope| {
        let readers: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    barrier.wait();
                    concurrent.json()
                })
            })
            .collect();
        for reader in readers {
            assert!(std::ptr::eq(reader.join().unwrap(), concurrent.json()));
        }
    });
    // The complete bounded response, including its envelope, is checked on first use and reuse.
    let cold = lazy_entries(selected);
    for _ in 0..2 {
        let limit = expected_json.len();
        let exact = MethodResponse::response(Id::Number(1), &Ok(&cold), limit);
        assert_eq!(exact.as_json().get(), expected_json);
        let too_small = MethodResponse::response(Id::Number(1), &Ok(&cold), limit - 1);
        assert!(too_small.is_error());
    }
    let mut updated = entry(&txs[0], &lazy[0].data);
    updated.depends = vec![1, 3];
    updated.fee = 1_600u64.try_into().unwrap();
    let updated_json = serde_json::to_value(updated).unwrap();
    assert_eq!(updated_json["depends"], serde_json::json!([1, 3]));
    assert_eq!(updated_json["fee"], 1_600);
    assert_eq!(updated_json["data"], hex[0]);

    let mut output = Vec::with_capacity(expected_json.len());
    // No measurements include fixture loading, compatibility checks, or code-page warm-up.
    for _ in 0..10 {
        black_box(response(&legacy));
        black_box(response(&lazy));
    }
    let mut stages = Vec::new();
    for iteration in 0..3 {
        // Alternate the order within each process as well as between independent processes.
        let order = if iteration % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        };
        for is_raw in order {
            if is_raw {
                stages.push(measure("raw_sink", 100, || {
                    black_box(count_json_bytes(&raw));
                }));
                stages.push(measure("raw_reused_buffer", 100, || {
                    output.clear();
                    serde_json::to_writer(&mut output, &raw_entries).unwrap();
                    black_box(&output);
                }));
                stages.push(measure("raw_response", 100, || {
                    black_box(response(&raw_entries));
                }));
                stages.push(measure("lazy_warm_response", 100, || {
                    black_box(response(&lazy));
                }));
                stages.push(measure("lazy_first_build_and_response", 30, || {
                    black_box(response(&lazy_entries(selected)));
                }));
                stages.push(measure("lazy_built_never_served", 100, || {
                    let unserved = lazy_entries(selected);
                    black_box(unserved);
                }));
                stages.push(measure("lazy_two_builds_one_response", 30, || {
                    let payloads: Vec<_> = txs
                        .iter()
                        .map(|tx| Arc::new(LazyJsonData::new(tx.transaction.transaction().clone())))
                        .collect();
                    let unserved: Vec<_> = selected
                        .iter()
                        .zip(&payloads)
                        .map(|(tx, data)| entry(tx, data.clone()))
                        .collect();
                    black_box(unserved);
                    let served: Vec<_> = txs[82..247]
                        .iter()
                        .zip(&payloads[82..247])
                        .map(|(tx, data)| entry(tx, data.clone()))
                        .collect();
                    black_box(response(&served));
                }));
            } else {
                stages.push(measure("string_sink", 100, || {
                    black_box(count_json_bytes(&hex));
                }));
                stages.push(measure("string_reused_buffer", 100, || {
                    output.clear();
                    serde_json::to_writer(&mut output, &strings).unwrap();
                    black_box(&output);
                }));
                stages.push(measure("string_response", 100, || {
                    black_box(response(&strings));
                }));
                stages.push(measure("legacy_warm_response", 100, || {
                    black_box(response(&legacy));
                }));
                stages.push(measure("legacy_first_build_and_response", 30, || {
                    let entries: Vec<_> = selected.iter().map(TransactionTemplate::from).collect();
                    black_box(response(&entries));
                }));
                stages.push(measure("legacy_built_never_served", 100, || {
                    black_box(
                        selected
                            .iter()
                            .map(TransactionTemplate::from)
                            .collect::<Vec<_>>(),
                    );
                }));
                stages.push(measure("legacy_two_builds_one_response", 30, || {
                    black_box(
                        selected
                            .iter()
                            .map(TransactionTemplate::from)
                            .collect::<Vec<_>>(),
                    );
                    black_box(response(
                        &txs[82..247]
                            .iter()
                            .map(TransactionTemplate::from)
                            .collect::<Vec<_>>(),
                    ));
                }));
            }
        }
    }
    stages.push(measure("complete_response_validation_only", 100, || {
        black_box(serde_json::from_str::<&RawValue>(black_box(expected_json)).unwrap());
    }));
    stages.push(measure("utf8_validation_only", 100, || {
        black_box(std::str::from_utf8(black_box(expected_json.as_bytes())).unwrap());
    }));
    let raw_retained: usize = raw.iter().map(|json| json.get().len()).sum();
    println!(
        "JSON_DATA_BENCH {}",
        serde_json::json!({"response_bytes": expected_json.len(), "transactions":165, "stages":stages, "raw_retained_bytes":raw_retained})
    );
}

#[test]
#[ignore = "requires the real replay fixture on an isolated benchmark host"]
#[allow(clippy::print_stdout)]
fn benchmark_escape_scanning() {
    let transactions = fixture();
    let bytes: Vec<_> = transactions[..165]
        .iter()
        .map(|tx| {
            tx.transaction
                .transaction()
                .zcash_serialize_to_vec()
                .unwrap()
        })
        .collect();
    let strings: Vec<_> = bytes.iter().map(crate::methods::hex_data::encode).collect();
    let raw: Vec<_> = bytes.iter().map(|bytes| checked_hex_json(bytes)).collect();
    assert_eq!(count_json_bytes(&strings), count_json_bytes(&raw));
    let mut stages = Vec::new();
    for iteration in 0..3 {
        if iteration % 2 == 0 {
            stages.push(measure("string_counting_sink", 100, || {
                black_box(count_json_bytes(&strings));
            }));
            stages.push(measure("raw_counting_sink", 100, || {
                black_box(count_json_bytes(&raw));
            }));
        } else {
            stages.push(measure("raw_counting_sink", 100, || {
                black_box(count_json_bytes(&raw));
            }));
            stages.push(measure("string_counting_sink", 100, || {
                black_box(count_json_bytes(&strings));
            }));
        }
    }
    println!(
        "JSON_SCAN_BENCH {}",
        serde_json::json!({"stages": stages, "output_bytes": count_json_bytes(&raw)})
    );
}
