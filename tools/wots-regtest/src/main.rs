//! Starts an isolated node and runs post-quantum account spends against it: a first spend,
//! a chained spend of a late deposit, several attempts within one step, and forged spends.

use ripemd::Ripemd160;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    error::Error,
    fs::File,
    net::TcpListener,
    path::PathBuf,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};
use zakura_wots_regtest::{Account, BOOTSTRAP_REDEEM, Coin, Unlock, build, output};
use zcash_primitives::transaction::Transaction;
use zcash_script::script::{Code, Evaluable};
use zcash_transparent::{
    address::{Script, TransparentAddress},
    bundle::{OutPoint, TxOut},
};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
const ACTIVATION: u32 = 200;
const SEED: &[u8] = b"zakura wots regtest account seed";
const COIN: u64 = 100_000_000;

#[derive(Debug, Clone)]
struct RpcRejection {
    code: i64,
    message: String,
}
impl std::fmt::Display for RpcRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RPC {}: {}", self.code, self.message)
    }
}
impl Error for RpcRejection {}

/// Requires the node itself to have rejected the transaction, with `reason` in its message
/// when given. Transport failures never count as rejections.
fn require_rejection(result: Result<String>, reason: Option<&str>) -> Result<RpcRejection> {
    let error = result.err().ok_or("node accepted an invalid transaction")?;
    let Some(rejection) = error.downcast_ref::<RpcRejection>() else {
        return Err(error);
    };
    if let Some(reason) = reason
        && (rejection.code != -25 || !rejection.message.to_ascii_lowercase().contains(reason))
    {
        return Err(format!("expected a rejection for `{reason}`, got {rejection}").into());
    }
    Ok(rejection.clone())
}

struct Node {
    child: Child,
    client: reqwest::blocking::Client,
    url: String,
    bootstrap: Script,
    /// The next block whose coinbase output has not been used.
    next_coinbase: u64,
    _directory: tempfile::TempDir,
}
impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
impl Node {
    fn rpc(&self, method: &str, params: Value) -> Result<Value> {
        let response: Value = self
            .client
            .post(&self.url)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
            .send()?
            .json()?;
        if !response["error"].is_null() {
            return Err(RpcRejection {
                code: response["error"]["code"]
                    .as_i64()
                    .ok_or("RPC error code is missing")?,
                message: response["error"]["message"]
                    .as_str()
                    .ok_or("RPC error message is missing")?
                    .to_owned(),
            }
            .into());
        }
        Ok(response["result"].clone())
    }
    fn mine(&self, count: u32) -> Result<()> {
        self.rpc("generate", json!([count]))?;
        Ok(())
    }
    fn tip(&self) -> Result<u32> {
        Ok(u32::try_from(
            self.rpc("getblockcount", json!([]))?
                .as_u64()
                .ok_or("missing height")?,
        )?)
    }
    /// An expiry height well past anything the scenario mines.
    fn expiry(&self) -> Result<u32> {
        Ok(self.tip()? + 100)
    }
    fn send(&self, tx: &Transaction) -> Result<String> {
        let mut bytes = vec![];
        tx.write(&mut bytes)?;
        Ok(self
            .rpc("sendrawtransaction", json!([hex::encode(bytes)]))?
            .as_str()
            .ok_or("missing transaction ID")?
            .to_owned())
    }
    fn accepts(&self, tx: &Transaction, case: &str) -> Result<()> {
        let txid = self.send(tx)?;
        println!("accepted {case}: {txid}");
        Ok(())
    }
    fn rejects(&self, tx: &Transaction, case: &str, reason: Option<&str>) -> Result<()> {
        let rejection = require_rejection(self.send(tx), reason)?;
        // A transport failure is never evidence of validation, including on the follow-up.
        self.rpc("getblockcount", json!([]))?;
        println!("rejected {case}: {rejection}");
        Ok(())
    }
    fn confirmations(&self, tx: &Transaction) -> Result<u64> {
        let txid = tx.txid().to_string();
        Ok(
            self.rpc("getrawtransaction", json!([txid, 1]))?["confirmations"]
                .as_u64()
                .unwrap_or(0),
        )
    }
    fn require_mined(&self, txs: &[&Transaction], case: &str) -> Result<()> {
        for tx in txs {
            if self.confirmations(tx)? < 1 {
                return Err(format!("{case}: {} was not mined", tx.txid()).into());
            }
        }
        Ok(())
    }
    /// The next unused mature coinbase output paying the bootstrap address.
    fn coinbase(&mut self) -> Result<Coin> {
        let height = self.next_coinbase;
        self.next_coinbase += 1;
        let hash = self.rpc("getblockhash", json!([height]))?;
        let block = self.rpc("getblock", json!([hash, 2]))?;
        let coinbase = &block["tx"][0];
        let coin = coinbase["vout"]
            .as_array()
            .ok_or("missing coinbase outputs")?
            .iter()
            .find(|v| v["scriptPubKey"]["hex"] == hex::encode(self.bootstrap.0.to_bytes()))
            .ok_or("missing miner output")?;
        let mut hash: [u8; 32] =
            hex::decode(coinbase["txid"].as_str().ok_or("missing coinbase txid")?)?
                .try_into()
                .map_err(|_| "txid length")?;
        hash.reverse();
        Ok(Coin {
            outpoint: OutPoint::new(
                hash,
                u32::try_from(coin["n"].as_u64().ok_or("missing output index")?)?,
            ),
            output: output(zatoshis(&coin["value"])?, self.bootstrap.clone()),
        })
    }
}

/// Output `index` of `tx`.
fn coin(tx: &Transaction, index: usize) -> Result<Coin> {
    let bundle = tx
        .transparent_bundle()
        .ok_or("missing transparent bundle")?;
    Ok(Coin {
        outpoint: OutPoint::new(*tx.txid().as_ref(), u32::try_from(index)?),
        output: bundle.vout.get(index).ok_or("missing output")?.clone(),
    })
}

/// The last output of `tx`, where [`build`] puts the change.
fn change(tx: &Transaction) -> Result<Coin> {
    let outputs = tx
        .transparent_bundle()
        .ok_or("missing transparent bundle")?
        .vout
        .len();
    coin(tx, outputs - 1)
}

/// The latest unspent control output and the latest change deposit that pays its fees.
struct State {
    step: u32,
    control: Coin,
    change: Coin,
}

fn main() -> Result<()> {
    let binary = PathBuf::from(
        std::env::args_os()
            .nth(1)
            .ok_or("usage: zakura-wots-regtest /path/to/zakurad")?,
    )
    .canonicalize()?;
    let directory = tempfile::tempdir()?;
    let port = TcpListener::bind("127.0.0.1:0")?.local_addr()?.port();
    let bootstrap =
        TransparentAddress::ScriptHash(Ripemd160::digest(Sha256::digest(BOOTSTRAP_REDEEM)).into());
    let config = format!(
        r#"
[network]
network = "Regtest"
listen_addr = "127.0.0.1:0"
initial_testnet_peers = []
cache_dir = false
[network.testnet_parameters]
experimental_wots_activation_height = {ACTIVATION}
[network.testnet_parameters.activation_heights]
NU5 = 1
[state]
ephemeral = true
[consensus]
checkpoint_sync = false
[mempool]
debug_enable_at_height = 0
[rpc]
listen_addr = "127.0.0.1:{port}"
enable_cookie_auth = false
[mining]
miner_address = "{}"
internal_miner = false
"#,
        zcash_address_string(&bootstrap)
    );
    let config_path = directory.path().join("zakura.toml");
    std::fs::write(&config_path, config)?;
    let log = directory.path().join("node.log");
    println!("isolated node log: {}", log.display());
    let child = Command::new(binary)
        .args(["-c"])
        .arg(&config_path)
        .arg("start")
        .stdout(Stdio::from(File::create(&log)?))
        .stderr(Stdio::from(File::options().append(true).open(&log)?))
        .spawn()?;
    let mut node = Node {
        child,
        // A spend of an output that is already spent waits 60 seconds for the output to appear
        // in the mempool before the node rejects it.
        client: reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(90))
            .build()?,
        url: format!("http://127.0.0.1:{port}"),
        bootstrap: Script(Code(bootstrap.script().to_bytes())),
        next_coinbase: 1,
        _directory: directory,
    };
    let start = Instant::now();
    while node.rpc("getblockcount", json!([])).is_err() {
        if let Some(status) = node.child.try_wait()? {
            return Err(format!("node exited {status}: {}", std::fs::read_to_string(&log)?).into());
        }
        if start.elapsed() > Duration::from_secs(120) {
            return Err(
                format!("node startup timed out: {}", std::fs::read_to_string(&log)?).into(),
            );
        }
        thread::sleep(Duration::from_millis(100));
    }
    node.mine(101)?;
    let result = scenario(&mut node);
    if result.is_err() {
        eprintln!("node log tail:");
        let text = std::fs::read_to_string(&log).unwrap_or_default();
        for line in text
            .lines()
            .rev()
            .take(40)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
        {
            eprintln!("  {line}");
        }
    }
    result
}

fn scenario(node: &mut Node) -> Result<()> {
    let account = Account::new(SEED, 0);
    let deposit = account.address_script();
    let payee = node.bootstrap.clone();
    let ec_key = secp256k1::SecretKey::from_secret_bytes([7; 32])?;
    let ec_script = Script(Code(
        TransparentAddress::PublicKeyHash(
            Ripemd160::digest(Sha256::digest(
                secp256k1::PublicKey::from_secret_key(&ec_key).serialize(),
            ))
            .into(),
        )
        .script()
        .to_bytes(),
    ));

    // Fund two deposits to the account, two ordinary EC outputs, and a bootstrap coin.
    let funding = build(
        &[(node.coinbase()?, Unlock::Bootstrap)],
        vec![
            output(COIN, deposit.clone()),
            output(COIN, deposit.clone()),
            output(COIN, ec_script.clone()),
            output(COIN, ec_script.clone()),
            output(COIN, payee.clone()),
        ],
        Some(payee.clone()),
        node.expiry()?,
        |_| {},
    );
    node.accepts(&funding, "funding")?;
    node.mine(1)?;
    node.require_mined(&[&funding], "funding")?;
    let deposits = [coin(&funding, 0)?, coin(&funding, 1)?];
    let ec_coins = [coin(&funding, 2)?, coin(&funding, 3)?];
    let loose = coin(&funding, 4)?;

    let ec_spend = |coin: &Coin, node: &Node| -> Result<()> {
        let tx = build(
            &[(coin.clone(), Unlock::Ec(&ec_key))],
            vec![],
            Some(payee.clone()),
            node.expiry()?,
            |_| {},
        );
        node.accepts(&tx, "ordinary EC spend")?;
        node.mine(1)?;
        node.require_mined(&[&tx], "ordinary EC spend")
    };
    ec_spend(&ec_coins[0], node)?;

    // 1. The first spend is signed now but only valid once the account rules activate.
    let first = build(
        &[
            (
                deposits[0].clone(),
                Unlock::Authorize {
                    account: &account,
                    step: 0,
                    spare: 0,
                },
            ),
            (deposits[1].clone(), Unlock::Covered(&account)),
        ],
        vec![output(COIN / 10, payee.clone()), account.control(1)],
        Some(deposit.clone()),
        ACTIVATION + 100,
        |_| {},
    );
    node.rejects(&first, "first spend before activation", None)?;
    node.mine(ACTIVATION - 1 - node.tip()?)?;

    // 2. First spend: a signing deposit and a covered deposit, creating step 1.
    node.accepts(&first, "first spend (step 0, spare 0)")?;

    // 3-4. A late deposit, spent with the step-1 key while both it and step 1 are unmined.
    let late_funding = build(
        &[(node.coinbase()?, Unlock::Bootstrap)],
        vec![output(COIN / 2, deposit.clone())],
        Some(payee.clone()),
        node.expiry()?,
        |_| {},
    );
    node.accepts(&late_funding, "late deposit")?;
    let chained = build(
        &[
            (
                coin(&first, 1)?,
                Unlock::Authorize {
                    account: &account,
                    step: 1,
                    spare: 1,
                },
            ),
            (coin(&late_funding, 0)?, Unlock::Covered(&account)),
            (change(&first)?, Unlock::Covered(&account)),
        ],
        vec![output(COIN / 10, payee.clone()), account.control(2)],
        Some(deposit.clone()),
        node.expiry()?,
        |_| {},
    );
    node.accepts(
        &chained,
        "chained spend of the late deposit (step 1, spare 1)",
    )?;
    if node.confirmations(&first)? != 0 {
        return Err("the first spend was mined before the chained spend was accepted".into());
    }
    node.mine(1)?;
    node.require_mined(&[&first, &late_funding, &chained], "chained spend")?;
    let mut state = State {
        step: 2,
        control: coin(&chained, 1)?,
        change: change(&chained)?,
    };

    // Every attempt at one step spends the same control output.
    let attempt = |state: &State,
                   spare: u8,
                   paid: u64,
                   expiry: u32,
                   tamper: &dyn Fn(&mut zcash_transparent::wots::Authorization)| {
        build(
            &[
                (
                    state.control.clone(),
                    Unlock::Authorize {
                        account: &account,
                        step: state.step,
                        spare,
                    },
                ),
                (state.change.clone(), Unlock::Covered(&account)),
            ],
            vec![output(paid, payee.clone()), account.control(state.step + 1)],
            Some(deposit.clone()),
            expiry,
            |authorization| tamper(authorization),
        )
    };
    let advance = |state: &mut State, tx: &Transaction| -> Result<()> {
        state.step += 1;
        state.control = coin(tx, 1)?;
        state.change = change(tx)?;
        Ok(())
    };

    // 5a. Conflict at step 2: the chain accepts one attempt, before and after it is mined.
    let a = attempt(&state, 0, COIN / 10, node.expiry()?, &|_| {});
    let b = attempt(&state, 1, COIN / 5, node.expiry()?, &|_| {});
    node.accepts(&a, "attempt A (step 2, spare 0)")?;
    node.rejects(&b, "attempt B (spare 1) while A is pending", None)?;
    node.mine(1)?;
    node.require_mined(&[&a], "attempt A")?;
    node.rejects(&b, "attempt B (spare 1) after A is mined", None)?;
    advance(&mut state, &a)?;

    // 5b. Retry after expiry at step 3: C is signed but lost, then expires; D retries.
    let c = attempt(&state, 0, COIN / 10, node.tip()? + 1, &|_| {});
    node.mine(2)?;
    node.rejects(&c, "attempt C (step 3, spare 0) after expiry", None)?;
    let d = attempt(&state, 1, COIN / 10, node.expiry()?, &|_| {});
    node.accepts(&d, "retry D (step 3, spare 1)")?;
    node.mine(1)?;
    node.require_mined(&[&d], "retry D")?;
    advance(&mut state, &d)?;

    // 5c. Edge spares at step 4: an index past the tree, then the last spare.
    let outside = attempt(&state, 2, COIN / 10, node.expiry()?, &|authorization| {
        authorization.spare = 4;
    });
    node.rejects(&outside, "spare index 4", Some("spare index outside"))?;
    let last = attempt(&state, 3, COIN / 10, node.expiry()?, &|_| {});
    node.accepts(&last, "last spare (step 4, spare 3)")?;
    node.mine(1)?;
    node.require_mined(&[&last], "last spare")?;
    advance(&mut state, &last)?;

    // 6. Forged spends at step 5, none of which may be mined.
    let step = state.step;
    let authorize = |spare: u8| Unlock::Authorize {
        account: &account,
        step,
        spare,
    };
    let forged = |inputs: Vec<(Coin, Unlock<'_>)>,
                  pay: Vec<TxOut>,
                  change: &Script,
                  tamper: &dyn Fn(&mut zcash_transparent::wots::Authorization)|
     -> Result<Transaction> {
        Ok(build(
            &inputs,
            pay,
            Some(change.clone()),
            node.expiry()?,
            |authorization| tamper(authorization),
        ))
    };
    let control_and_change = |spare: u8| {
        vec![
            (state.control.clone(), authorize(spare)),
            (state.change.clone(), Unlock::Covered(&account)),
        ]
    };
    node.rejects(
        &forged(
            vec![(loose.clone(), Unlock::Bootstrap)],
            vec![account.control(step + 1)],
            &payee,
            &|_| {},
        )?,
        "control output without authorization",
        Some("without the account's authorization"),
    )?;
    node.rejects(
        &forged(
            control_and_change(0),
            vec![account.control(step + 2)],
            &deposit,
            &|_| {},
        )?,
        "successor at the wrong step",
        Some("exactly one control output"),
    )?;
    node.rejects(
        &forged(control_and_change(1), vec![], &deposit, &|_| {})?,
        "missing successor",
        Some("exactly one control output"),
    )?;
    node.rejects(
        &forged(
            control_and_change(2),
            vec![account.control(step + 1), account.control(step + 1)],
            &deposit,
            &|_| {},
        )?,
        "two successors",
        Some("exactly one control output"),
    )?;
    node.rejects(
        &forged(
            vec![(state.change.clone(), Unlock::Covered(&account))],
            vec![],
            &payee,
            &|_| {},
        )?,
        "covered deposit without authorization",
        Some("exactly one authorization"),
    )?;
    // The next two sign the same transaction with the same spare, so they reveal nothing new.
    let successor = || vec![account.control(step + 1)];
    node.rejects(
        &forged(
            control_and_change(3),
            successor(),
            &deposit,
            &|authorization| {
                authorization.signature[3] ^= 1;
            },
        )?,
        "altered signature",
        Some("does not sign this transaction"),
    )?;
    node.rejects(
        &forged(
            control_and_change(3),
            successor(),
            &deposit,
            &|authorization| {
                authorization.path[0][0] ^= 1;
            },
        )?,
        "altered spare path",
        Some("does not sign this transaction"),
    )?;
    node.rejects(
        &forged(
            vec![
                (
                    state.control.clone(),
                    Unlock::Authorize {
                        account: &account,
                        step: step + 1,
                        spare: 0,
                    },
                ),
                (state.change.clone(), Unlock::Covered(&account)),
            ],
            successor(),
            &deposit,
            &|_| {},
        )?,
        "step-5 control output signed by a step-6 key",
        Some("does not sign this transaction"),
    )?;

    ec_spend(&ec_coins[1], node)?;
    println!(
        "PASS: first spend after activation, chained late deposit, one accepted attempt per step, \
         retry after expiry, edge spares, forged spends rejected, and ordinary EC spends unchanged"
    );
    Ok(())
}

fn zatoshis(value: &Value) -> Result<u64> {
    let number = value.to_string();
    let (whole, fractional) = number.split_once('.').unwrap_or((&number, ""));
    if fractional.len() > 8 || !fractional.bytes().all(|b| b.is_ascii_digit()) {
        return Err("invalid monetary amount".into());
    }
    let fraction = if fractional.is_empty() {
        0
    } else {
        fractional
            .parse::<u64>()?
            .checked_mul(10u64.pow(u32::try_from(8 - fractional.len())?))
            .ok_or("amount overflow")?
    };
    whole
        .parse::<u64>()?
        .checked_mul(100_000_000)
        .and_then(|v| v.checked_add(fraction))
        .ok_or_else(|| "amount overflow".into())
}

fn zcash_address_string(address: &TransparentAddress) -> String {
    use zcash_address::ToAddress;
    match address {
        TransparentAddress::ScriptHash(hash) => zcash_address::ZcashAddress::from_transparent_p2sh(
            zcash_protocol::consensus::NetworkType::Regtest,
            *hash,
        )
        .to_string(),
        _ => unreachable!("the bootstrap address is P2SH"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejection_requires_the_node_and_the_expected_reason() {
        let transport: Result<String> =
            Err(std::io::Error::from(std::io::ErrorKind::ConnectionRefused).into());
        assert!(require_rejection(transport, None).is_err());
        assert!(require_rejection(Ok("accepted".into()), None).is_err());
        let rejection = |code, message: &str| -> Result<String> {
            Err(RpcRejection {
                code,
                message: message.into(),
            }
            .into())
        };
        assert!(require_rejection(rejection(-26, "conflict"), None).is_ok());
        assert!(require_rejection(rejection(-25, "insufficient fee"), Some("spare")).is_err());
        assert!(require_rejection(rejection(-32603, "spare index"), Some("spare")).is_err());
        assert!(require_rejection(rejection(-25, "Spare index outside"), Some("spare")).is_ok());
    }
}
