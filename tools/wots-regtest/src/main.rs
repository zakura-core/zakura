//! Starts an isolated node, funds disposable addresses, and mines separate ordinary EC and WOTS-only outputs.

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
use zakura_wots_regtest::{digest, ec_digest, fee, output, spend_wots, transaction, with_script};
use zcash_script::script::{Code, Evaluable};
use zcash_transparent::{
    address::{Script, TransparentAddress},
    bundle::OutPoint,
    wots::{RedeemScript, SigningKey, WOTS_INPUT_SIZE},
};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
const ACTIVATION: u64 = 200;

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

fn require_script_rejection(result: Result<String>) -> Result<RpcRejection> {
    let error = result
        .err()
        .ok_or("node accepted an invalid authorization")?;
    let Some(rejection) = error.downcast_ref::<RpcRejection>() else {
        return Err(error);
    };
    if rejection.code != -25 || !rejection.message.to_ascii_lowercase().contains("script") {
        return Err(format!("unexpected rejection category: {rejection}").into());
    }
    Ok(rejection.clone())
}

struct Node {
    child: Child,
    client: reqwest::blocking::Client,
    url: String,
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
    fn mine(&self, count: u64) -> Result<()> {
        self.rpc("generate", json!([count]))?;
        Ok(())
    }
    fn send(&self, tx: &zcash_primitives::transaction::Transaction) -> Result<String> {
        let mut bytes = vec![];
        tx.write(&mut bytes)?;
        Ok(self
            .rpc("sendrawtransaction", json!([hex::encode(bytes)]))?
            .as_str()
            .ok_or("missing transaction ID")?
            .to_owned())
    }
    fn rejects(&self, tx: &zcash_primitives::transaction::Transaction, case: &str) -> Result<()> {
        let rejection = require_script_rejection(self.send(tx))?;
        // A transport failure is never evidence of validation, including on the follow-up.
        self.rpc("getblockcount", json!([]))?;
        println!("rejected {case}: {rejection}");
        Ok(())
    }
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
        TransparentAddress::ScriptHash(Ripemd160::digest(Sha256::digest([0x51])).into());
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
        client: reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()?,
        url: format!("http://127.0.0.1:{port}"),
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
    let bootstrap_script = Script(Code(bootstrap.script().to_bytes()));

    // Each WOTS output has its own disposable key; EC outputs use the ordinary P2PKH path.
    for mode in ["ec-before", "wots", "ec-after"] {
        let key =
            SigningKey::generate(&mut getrandom::SysRng).map_err(|_| "system randomness failed")?;
        let mut secret = [0u8; 32];
        getrandom::fill(&mut secret)?;
        let ec_key = secp256k1::SecretKey::from_secret_bytes(secret)?;
        let ec_public_key = secp256k1::PublicKey::from_secret_key(&ec_key);
        let public_key = ec_public_key.serialize();
        let redeem = RedeemScript::new(&key.verifying_key());
        let funded_script = if mode == "wots" {
            Script(Code(redeem.address().script().to_bytes()))
        } else {
            Script(Code(
                TransparentAddress::PublicKeyHash(
                    Ripemd160::digest(Sha256::digest(public_key)).into(),
                )
                .script()
                .to_bytes(),
            ))
        };
        let previous_hash = node.rpc(
            "getblockhash",
            json!([if mode == "ec-before" {
                1
            } else if mode == "wots" {
                2
            } else {
                3
            }]),
        )?;
        let block = node.rpc("getblock", json!([previous_hash, 2]))?;
        let coinbase = &block["tx"][0];
        let coin = coinbase["vout"]
            .as_array()
            .ok_or("missing coinbase outputs")?
            .iter()
            .find(|v| v["scriptPubKey"]["hex"] == hex::encode(bootstrap_script.0.to_bytes()))
            .ok_or("missing miner output")?;
        let mut hash: [u8; 32] =
            hex::decode(coinbase["txid"].as_str().ok_or("missing coinbase txid")?)?
                .try_into()
                .map_err(|_| "txid length")?;
        hash.reverse();
        let outpoint = OutPoint::new(
            hash,
            u32::try_from(coin["n"].as_u64().ok_or("missing output index")?)?,
        );
        let amount = zatoshis(&coin["value"])?;
        let funded_amount = amount
            .checked_sub(10_000)
            .ok_or("funding fee exceeds value")?;
        let funded = output(funded_amount, funded_script);
        let funding = transaction(outpoint, funded.clone(), Script(Code(vec![1, 0x51])));
        node.send(&funding)?;
        node.mine(1)?;
        let outpoint = OutPoint::new(*funding.txid().as_ref(), 0);
        let spend_fee = fee(if mode == "wots" { WOTS_INPUT_SIZE } else { 148 });
        let destination = output(
            funded_amount
                .checked_sub(spend_fee)
                .ok_or("spend fee exceeds value")?,
            bootstrap_script.clone(),
        );
        let ec_attack_digest = if mode == "wots" {
            digest(
                outpoint.clone(),
                funded.clone(),
                destination.clone(),
                redeem.as_bytes(),
            )
        } else {
            [0; 32]
        };
        let tx = if mode == "wots" {
            spend_wots(outpoint, funded, destination, &redeem, key)?
        } else {
            drop(key);
            let hash = ec_digest(outpoint.clone(), funded, destination.clone(), ec_public_key);
            let signature = secp256k1::ecdsa::sign(secp256k1::Message::from_digest(hash), &ec_key);
            let mut signature = signature.serialize_der().to_vec();
            signature.push(1);
            transaction(
                outpoint,
                destination,
                Script(Code(
                    [
                        vec![u8::try_from(signature.len()).expect("DER signatures fit one byte")],
                        signature,
                        vec![33],
                        public_key.to_vec(),
                    ]
                    .concat(),
                )),
            )
        };
        if mode == "wots" {
            node.rejects(&tx, "WOTS before activation")?;
            let height = node
                .rpc("getblockcount", json!([]))?
                .as_u64()
                .ok_or("missing height")?;
            node.mine(
                ACTIVATION
                    .saturating_sub(1)
                    .checked_sub(height)
                    .ok_or("unexpected chain height")?,
            )?;
            let script = tx.transparent_bundle().ok_or("missing input")?.vin[0]
                .script_sig()
                .clone();
            let ec_hash = ec_attack_digest;
            let ec_signature =
                secp256k1::ecdsa::sign(secp256k1::Message::from_digest(ec_hash), &ec_key);
            let mut ec_bytes = ec_signature.serialize_der().to_vec();
            ec_bytes.push(1);
            let mut ec_attempt = vec![u8::try_from(ec_bytes.len())?];
            ec_attempt.extend(ec_bytes);
            ec_attempt.push(33);
            ec_attempt.extend(public_key);
            ec_attempt.push(34);
            ec_attempt.extend(redeem.as_bytes());
            node.rejects(
                &with_script(&tx, Script(Code(ec_attempt))),
                "EC signature supplied to WOTS-only output",
            )?;
            let mut changed = script.clone();
            changed.0.0[3] ^= 1;
            node.rejects(&with_script(&tx, changed), "altered WOTS signature")?;
            let mut changed_seed = script.clone();
            // Four 512-byte pushes and a 96-byte push occupy 2,158 bytes;
            // the next byte pushes the 32-byte public seed.
            changed_seed.0.0[2159] ^= 1;
            node.rejects(&with_script(&tx, changed_seed), "altered WOTS public seed")?;
            if script.0.0.len() != zcash_transparent::wots::WOTS_SCRIPT_SIG_SIZE {
                return Err(
                    "the WOTS spend must contain only signature chunks, seed, and redeem script"
                        .into(),
                );
            }
            println!(
                "WOTS authorization: {} bytes; serialized input: {} bytes; no public-key endpoints",
                script.0.0.len(),
                zcash_transparent::wots::WOTS_INPUT_SIZE
            );
            let mut malformed = script;
            malformed.0.0[1] ^= 1;
            node.rejects(&with_script(&tx, malformed), "malformed signature chunk")?;
            let bundle = tx.transparent_bundle().ok_or("missing input")?;
            let changed_output = output(
                bundle.vout[0]
                    .value()
                    .into_u64()
                    .checked_sub(1)
                    .ok_or("empty output")?,
                bootstrap_script.clone(),
            );
            node.rejects(
                &transaction(
                    bundle.vin[0].prevout().clone(),
                    changed_output,
                    bundle.vin[0].script_sig().clone(),
                ),
                "altered signed destination amount",
            )?;
        }
        let txid = node.send(&tx)?;
        node.mine(1)?;
        let mined = node.rpc("getrawtransaction", json!([txid, 1]))?;
        if mined["confirmations"].as_u64().unwrap_or(0) < 1 {
            return Err("spend was not mined".into());
        }
        println!("mined {mode}: {txid}; fee={spend_fee} zatoshis");
    }
    println!(
        "PASS: ordinary EC before activation, WOTS-only after activation, ordinary EC after activation, and invalid authorizations rejected"
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
    fn rejection_requires_a_script_validation_error_from_the_node() {
        let transport: Result<String> =
            Err(std::io::Error::from(std::io::ErrorKind::ConnectionRefused).into());
        assert!(require_script_rejection(transport).is_err());
        assert!(require_script_rejection(Ok("accepted".into())).is_err());
        for (code, message) in [
            (-32603, "script service unavailable"),
            (-25, "insufficient fee"),
        ] {
            assert!(
                require_script_rejection(Err(RpcRejection {
                    code,
                    message: message.into()
                }
                .into()))
                .is_err()
            );
        }
        assert!(
            require_script_rejection(Err(RpcRejection {
                code: -25,
                message: "transaction script verification failed".into()
            }
            .into()))
            .is_ok()
        );
    }
}
