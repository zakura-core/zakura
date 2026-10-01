use serde::Deserialize;
use std::{
    env,
    fs::File,
    io::{BufRead, BufReader},
    sync::Arc,
};
use zakura_chain::{
    parameters::NetworkUpgrade,
    serialization::ZcashDeserializeInto,
    transaction::Transaction,
    transparent::{Output, Script},
};
use zakura_script_oracle::{cxx_transaction_sigops, CachedFfiTransaction, Sigops};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    transaction: String,
    network_upgrade: NetworkUpgrade,
    previous_outputs: Vec<PreviousOutput>,
    source: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PreviousOutput {
    amount: u64,
    script_pubkey: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = env::args()
        .nth(1)
        .ok_or("usage: replay <transactions.jsonl>")?;
    let mut transactions = 0u64;
    let mut inputs = 0u64;
    let mut legacy_sigops = 0u64;
    let mut p2sh_sigops = 0u64;
    for (line, row) in BufReader::new(File::open(&path)?).lines().enumerate() {
        let record: Record = serde_json::from_str(&row?)?;
        let bytes = hex::decode(record.transaction)?;
        let tx = Arc::new(bytes.zcash_deserialize_into::<Transaction>()?);
        let outputs: Vec<Output> = record
            .previous_outputs
            .into_iter()
            .map(|output| {
                Ok(Output {
                    value: output.amount.try_into()?,
                    lock_script: Script::new(&hex::decode(output.script_pubkey)?),
                })
            })
            .collect::<Result<_, Box<dyn std::error::Error>>>()?;
        let legacy = tx.sigops()?;
        assert_eq!(
            legacy,
            cxx_transaction_sigops(&tx)?,
            "line {}: legacy count differs",
            line + 1
        );
        legacy_sigops += u64::from(legacy);
        if !tx.is_coinbase() {
            let verifier =
                CachedFfiTransaction::new(tx.clone(), Arc::new(outputs), record.network_upgrade)?;
            p2sh_sigops += u64::from(verifier.p2sh_sigops());
            for index in 0..tx.inputs().len() {
                // The wrapper fails on disagreement. Both rejecting remains a recorded result.
                let result = verifier.is_valid(index);
                println!(
                    "line={} source={:?} input={} accepted={} result={:?}",
                    line + 1,
                    record.source,
                    index,
                    result.is_ok(),
                    result
                );
                inputs += 1;
            }
        }
        transactions += 1;
    }
    println!("file={path:?} transactions={transactions} inputs={inputs} legacy_sigops={legacy_sigops} p2sh_sigops={p2sh_sigops}");
    Ok(())
}
