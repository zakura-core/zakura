//! Drives fee-bearing transactions onto the NU7 fork testnet.
//!
//! NU7 recycles 60% of a block's aggregate fees into the ZIP 234 NSM balance and lets the
//! miner claim the subsidy plus the remaining 40%. A fork whose every block is coinbase-only
//! has no fees, so none of that engages. This tool creates the fees.
//!
//! The only spendable value on the fork is the miner's own coinbase, and consensus requires
//! that a transaction spending transparent coinbase have **no transparent outputs**:
//!
//! > A transaction with one or more transparent inputs from coinbase transactions
//! > MUST have no transparent outputs (i.e. `tx_out_count` MUST be 0).
//!
//! So each transaction shields a matured coinbase output into the Orchard pool and leaves a
//! deliberate gap between the input value and the note value. That gap is the fee.
//!
//! The Orchard bundle is output-only (`Flags::SPENDS_DISABLED`), which is why it needs no
//! real anchor or witness: with spends disabled every spent note is constrained to be a
//! dummy, so the empty-tree anchor is correct.

use clap::Parser;
use color_eyre::eyre::{bail, eyre, Result, WrapErr};

use orchard::{
    builder::{Builder, BundleType},
    bundle::{BundleVersion, Flags},
    circuit::{OrchardCircuitVersion, ProvingKey},
    keys::{FullViewingKey, Scope, SpendingKey},
    tree::Anchor,
    value::NoteValue,
    Bundle,
};

use zakura_chain::{
    amount::{Amount, NonNegative},
    block::Height,
    orchard as zorchard,
    parameters::{Network, NetworkUpgrade},
    primitives::{Address, Halo2Proof},
    serialization::{AtLeastOne, ZcashDeserialize, ZcashSerialize},
    transaction::{HashType, LockTime, Transaction},
    transparent,
};
use zcash_address::ZcashAddress;

use zakura_node_services::rpc_client::RpcRequestClient;

/// The value pool this tool shields into, and the circuit era that pool requires.
///
/// Orchard is closed to new value after NU6.3 -- consensus rejects a transaction that adds
/// to it with `adding to the orchard pool is disabled after NU6.3` -- so value has to land
/// in Ironwood instead. Ironwood reuses Orchard's action shape and proof system, so only
/// the bundle version, the circuit era and the carrying transaction version change:
/// `ironwood_v3` is `ProtocolVersion::V3`, whose circuit is `PostNu6_3`, and Ironwood
/// bundles ride in V6 transactions.
fn bundle_version() -> BundleVersion {
    BundleVersion::ironwood_v3()
}

const CIRCUIT_VERSION: OrchardCircuitVersion = OrchardCircuitVersion::PostNu6_3;

fn faucet_recipient(encoded: &str, network: &Network) -> Result<orchard::Address> {
    let parsed: ZcashAddress = encoded.parse().wrap_err("recipient address is invalid")?;
    let converted: Address = parsed
        .convert_if_network(network.kind().into())
        .map_err(|error| eyre!("recipient is not on this fork's Testnet: {error}"))?;
    match converted {
        Address::Unified {
            orchard: Some(receiver),
            ..
        } => Ok(receiver),
        _ => bail!("recipient must be a Unified Address with an Orchard receiver"),
    }
}

fn change_recipient(secret: &secp256k1::SecretKey) -> Result<orchard::Address> {
    let mut params = blake2b_simd::Params::new();
    params.hash_length(32).personal(b"ZkFaucetChange01");
    for counter in 0..=u8::MAX {
        let mut input = secret.secret_bytes().to_vec();
        input.push(counter);
        let digest = params.hash(&input);
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(digest.as_bytes());
        if let Some(key) = SpendingKey::from_bytes(bytes).into_option() {
            return Ok(FullViewingKey::from(&key).address_at(0u32, Scope::External));
        }
    }
    bail!("could not derive a valid shielded change key")
}

fn throwaway_recipient() -> Result<orchard::Address> {
    let key = SpendingKey::from_bytes([17; 32])
        .into_option()
        .ok_or_else(|| eyre!("the hard-coded recipient key bytes are not a valid spending key"))?;
    Ok(FullViewingKey::from(&key).address_at(0u32, Scope::External))
}

#[derive(Parser, Debug)]
#[command(about, long_about = None)]
struct Args {
    /// The fork node's JSON-RPC address.
    #[arg(long, env = "ZAKURA_FORK_RPC", default_value = "127.0.0.1:18232")]
    rpc: String,

    /// The node's rendered config, read to recover the exact fork `Network`.
    #[arg(long, default_value = "/etc/zakura/zakura.toml")]
    config: std::path::PathBuf,

    /// The transparent address whose coinbase outputs are shielded.
    #[arg(long)]
    address: String,

    /// The WIF-less secret key for `address`, hex encoded (32 bytes).
    #[arg(long, env = "ZAKURA_FORK_KEY", conflicts_with = "secret_key_file")]
    secret_key: Option<String>,

    /// File containing the hex-encoded miner key, for unattended operation.
    #[arg(long, conflicts_with = "secret_key")]
    secret_key_file: Option<std::path::PathBuf>,

    /// NU7 Testnet Unified Address with an Orchard receiver; paid through Ironwood.
    #[arg(long, requires = "amount_zat")]
    recipient: Option<String>,

    /// Exact zatoshi payout when --recipient is set. Change returns to a key
    /// deterministically derived from the miner key.
    #[arg(long, requires = "recipient")]
    amount_zat: Option<u64>,

    /// Fee to leave behind in each transaction, in zatoshis.
    #[arg(long, default_value_t = 10_000)]
    fee: u64,

    /// Stop after this many transactions. 0 sends until interrupted.
    #[arg(long, default_value_t = 1)]
    count: u32,
}

/// Recovers the fork's `Network` by deserializing the node's own config section.
///
/// Parsing the node's config keeps this tool's consensus parameters identical to the
/// node's, rather than restating the fork's activation heights where they could drift.
fn network_from_config(path: &std::path::Path) -> Result<Network> {
    let text = std::fs::read_to_string(path)
        .wrap_err_with(|| format!("could not read the node config at {}", path.display()))?;
    let value: toml::Value = toml::from_str(&text).wrap_err("the node config is not valid TOML")?;
    let section = value
        .get("network")
        .ok_or_else(|| eyre!("the node config has no [network] section"))?
        .clone();
    let config: zakura_network::Config = section
        .try_into()
        .wrap_err("the [network] section did not deserialize into a network config")?;
    Ok(config.network)
}

/// One matured coinbase output belonging to the miner address.
struct Utxo {
    outpoint: transparent::OutPoint,
    script: transparent::Script,
    value: Amount<NonNegative>,
    height: Height,
}

/// Returns the outpoints already spent by transactions sitting in the mempool.
async fn mempool_spent_outpoints(
    client: &RpcRequestClient,
) -> Result<std::collections::HashSet<(String, u32)>> {
    let txids: Vec<String> = client
        .json_result_from_call("getrawmempool", "[]".to_string())
        .await
        .map_err(|error| eyre!("getrawmempool failed: {error}"))?;

    let mut spent = std::collections::HashSet::new();
    for txid in txids {
        let tx: serde_json::Value = client
            .json_result_from_call("getrawtransaction", format!(r#"["{txid}", 1]"#))
            .await
            .map_err(|error| eyre!("getrawtransaction failed for {txid}: {error}"))?;
        for input in tx["vin"].as_array().into_iter().flatten() {
            if let (Some(hash), Some(index)) = (input["txid"].as_str(), input["vout"].as_u64()) {
                spent.insert((hash.to_string(), index as u32));
            }
        }
    }
    Ok(spent)
}

async fn newest_spendable_utxo(
    client: &RpcRequestClient,
    address: &str,
    tip: Height,
) -> Result<Utxo> {
    let mempool_spent = mempool_spent_outpoints(client).await?;
    let params = serde_json::json!([{ "addresses": [address] }]).to_string();
    let utxos: serde_json::Value = client
        .json_result_from_call("getaddressutxos", params)
        .await
        .map_err(|error| eyre!("getaddressutxos failed: {error}"))?;

    let utxos = utxos
        .as_array()
        .ok_or_else(|| eyre!("getaddressutxos did not return an array"))?;

    // Coinbase outputs need MIN_TRANSPARENT_COINBASE_MATURITY confirmations. Spending is
    // checked against the height the transaction lands at, so leave a margin for the
    // blocks mined while this transaction sits in the mempool.
    let maturity = transparent::MIN_TRANSPARENT_COINBASE_MATURITY;
    let highest_spendable = tip.0.saturating_sub(maturity + 2);

    let mut best: Option<Utxo> = None;
    for entry in utxos {
        let height = Height(
            u32::try_from(
                entry["height"]
                    .as_u64()
                    .ok_or_else(|| eyre!("a UTXO has no height"))?,
            )
            .wrap_err("a UTXO height does not fit in a block height")?,
        );
        if height.0 > highest_spendable {
            continue;
        }
        let value = Amount::try_from(
            i64::try_from(
                entry["satoshis"]
                    .as_u64()
                    .ok_or_else(|| eyre!("a UTXO has no satoshis"))?,
            )
            .wrap_err("a UTXO value does not fit in an amount")?,
        )?;

        let txid_text = entry["txid"]
            .as_str()
            .ok_or_else(|| eyre!("a UTXO has no txid"))?;
        let entry_index = u32::try_from(
            entry["outputIndex"]
                .as_u64()
                .ok_or_else(|| eyre!("a UTXO has no outputIndex"))?,
        )?;
        // Already spent by a mempool transaction: selecting it would double spend.
        if mempool_spent.contains(&(txid_text.to_string(), entry_index)) {
            continue;
        }

        let txid: zakura_chain::transaction::Hash =
            txid_text.parse().wrap_err("a UTXO txid did not parse")?;
        let index = u32::try_from(
            entry["outputIndex"]
                .as_u64()
                .ok_or_else(|| eyre!("a UTXO has no outputIndex"))?,
        )?;
        let script = transparent::Script::new(
            &hex::decode(
                entry["script"]
                    .as_str()
                    .ok_or_else(|| eyre!("a UTXO has no script"))?,
            )
            .wrap_err("a UTXO script is not hex")?,
        );

        // Prefer the largest mature output, so one transaction can carry a visible fee.
        if best.as_ref().is_none_or(|current| value > current.value) {
            best = Some(Utxo {
                outpoint: transparent::OutPoint { hash: txid, index },
                script,
                value,
                height,
            });
        }
    }

    best.ok_or_else(|| {
        eyre!("no matured coinbase output for {address} at or below height {highest_spendable}")
    })
}

/// Builds and proves an output-only Orchard bundle carrying `value`.
///
/// Returns the unauthorized bundle; its signatures are applied later, once the surrounding
/// transaction's sighash is known. The bundle's contents are fixed before that point, which
/// is what makes the two-phase assembly sound: the sighash commits to the actions, value
/// balance and anchor, but not to the signatures over it.
fn prove_shielding_bundle(
    pk: &ProvingKey,
    value: u64,
    recipient: orchard::Address,
    change: Option<(orchard::Address, u64)>,
) -> Result<
    orchard::bundle::Bundle<
        orchard::builder::InProgress<orchard::circuit::Proof, orchard::builder::Unauthorized>,
        i64,
    >,
> {
    let mut rng = rand_10::rng();

    let mut builder = Builder::new(
        BundleType::DEFAULT,
        bundle_version(),
        Flags::SPENDS_DISABLED,
        Anchor::empty_tree(),
    )
    .map_err(|error| eyre!("the shielding bundle builder rejected its flags: {error:?}"))?;

    builder
        .add_output(None, recipient, NoteValue::from_raw(value), [0u8; 512])
        .map_err(|error| eyre!("adding the shielded output failed: {error:?}"))?;
    if let Some((address, amount)) = change {
        builder
            .add_output(None, address, NoteValue::from_raw(amount), [0u8; 512])
            .map_err(|error| eyre!("adding the change output failed: {error:?}"))?;
    }

    let (unauthorized, _meta) = builder
        .build::<i64>(&mut rng)
        .map_err(|error| eyre!("building the shielding bundle failed: {error:?}"))?
        .ok_or_else(|| eyre!("the shielding bundle came out empty despite having an output"))?;

    unauthorized
        .create_proof(pk, &mut rng)
        .map_err(|error| eyre!("proving the shielding bundle failed: {error:?}"))
}

/// Serialises one Orchard action in the canonical layout and parses it back as Zakura's
/// action type.
///
/// Every field of `zakura_chain::orchard::Action` is `ZcashDeserialize`-able, so a byte
/// round trip is both shorter and safer than converting field by field: the parser applies
/// the same canonicity rules consensus does, so a malformed action fails here rather than
/// at the node.
fn zakura_action(
    action: &orchard::Action<orchard::primitives::redpallas::Signature<reddsa::orchard::SpendAuth>>,
) -> Result<zorchard::Action> {
    let note = action.encrypted_note();

    let mut bytes = Vec::new();
    bytes.extend_from_slice(&action.cv_net().to_bytes());
    bytes.extend_from_slice(&action.nullifier().to_bytes());
    bytes.extend_from_slice(&<[u8; 32]>::from(action.rk()));
    bytes.extend_from_slice(&action.cmx().to_bytes());
    bytes.extend_from_slice(&note.epk_bytes);
    bytes.extend_from_slice(&note.enc_ciphertext);
    bytes.extend_from_slice(&note.out_ciphertext);

    zorchard::Action::zcash_deserialize(&bytes[..])
        .wrap_err("the Orchard action did not round-trip into Zakura's action type")
}

/// Converts a proved and signed Orchard bundle into Zakura's transaction-level type.
fn shielded_data_from_bundle(
    bundle: &Bundle<orchard::bundle::Authorized, i64>,
) -> Result<zorchard::ShieldedData> {
    let mut actions = Vec::new();
    for action in bundle.actions() {
        // The orchard crate and zakura-chain link separate `reddsa` instances, so the
        // signature crosses that boundary as raw bytes rather than by type.
        actions.push(zorchard::AuthorizedAction::from_parts(
            zakura_action(action)?,
            reddsa::Signature::from(<[u8; 64]>::from(action.authorization())),
        ));
    }

    let flags =
        zorchard::Flags::from_bits(bundle.flags().to_byte(bundle_version()).ok_or_else(|| {
            eyre!("the bundle flags are not representable in this bundle version")
        })?)
        .ok_or_else(|| eyre!("the bundle flags did not round-trip into Zakura's flag type"))?;

    Ok(zorchard::ShieldedData {
        flags,
        value_balance: Amount::try_from(*bundle.value_balance())?,
        shared_anchor: zorchard::tree::Root::try_from(bundle.anchor().to_bytes())
            .map_err(|error| eyre!("the bundle anchor did not convert: {error:?}"))?,
        proof: Halo2Proof(bundle.authorization().proof().as_ref().to_vec()),
        actions: AtLeastOne::try_from(actions)
            .map_err(|error| eyre!("the bundle has no actions: {error:?}"))?,
        binding_sig: reddsa::Signature::from(<[u8; 64]>::from(
            bundle.authorization().binding_signature(),
        )),
    })
}

#[tokio::main]
#[allow(clippy::print_stdout)]
async fn main() -> Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    let network = network_from_config(&args.config)?;
    let client = RpcRequestClient::new(
        args.rpc
            .parse()
            .wrap_err_with(|| format!("{} is not a socket address", args.rpc))?,
    );

    let nu7 = NetworkUpgrade::Nu7
        .activation_height(&network)
        .ok_or_else(|| eyre!("{network} has no NU7 activation height"))?;

    let secret_text = match (&args.secret_key, &args.secret_key_file) {
        (Some(key), None) => key.clone(),
        (None, Some(path)) => std::fs::read_to_string(path)
            .wrap_err_with(|| format!("could not read key file at {}", path.display()))?,
        _ => bail!("supply exactly one of --secret-key and --secret-key-file"),
    };
    let secret = secp256k1::SecretKey::from_slice(
        &hex::decode(secret_text.trim()).wrap_err("the secret key is not hex")?,
    )
    .wrap_err("the secret key is not a valid secp256k1 key")?;
    let recipient = args
        .recipient
        .as_deref()
        .map(|encoded| faucet_recipient(encoded, &network))
        .transpose()?;
    if args.amount_zat == Some(0) {
        bail!("the faucet payout must be positive");
    }

    tracing::info!(%network, nu7 = nu7.0, "building the proving key");
    let pk = ProvingKey::build(CIRCUIT_VERSION);
    tracing::info!("proving key ready");

    let mut sent = 0u32;
    loop {
        let tip: u32 = client
            .json_result_from_call("getblockcount", "[]".to_string())
            .await
            .map_err(|error| eyre!("getblockcount failed: {error}"))?;
        let tip = Height(tip);

        if tip < nu7 {
            bail!("the fork is at {tip:?}, below NU7 activation {nu7:?}; fees would not recycle");
        }

        let utxo = newest_spendable_utxo(&client, &args.address, tip).await?;
        let spendable_value = u64::try_from(i64::from(utxo.value))
            .wrap_err("a UTXO value does not fit in a note value")?
            .checked_sub(args.fee)
            .ok_or_else(|| eyre!("the fee exceeds the UTXO value"))?;
        let (shielded_value, change) = if let Some(amount) = args.amount_zat {
            let change_value = spendable_value
                .checked_sub(amount)
                .ok_or_else(|| eyre!("the UTXO does not cover the payout and fee"))?;
            let change = (change_value > 0)
                .then(|| change_recipient(&secret).map(|address| (address, change_value)))
                .transpose()?;
            (amount, change)
        } else {
            (spendable_value, None)
        };

        tracing::info!(
            outpoint = ?utxo.outpoint,
            utxo_value = i64::from(utxo.value),
            utxo_height = utxo.height.0,
            fee = args.fee,
            shielded_value,
            "shielding a matured coinbase output"
        );

        let destination = if let Some(address) = recipient {
            address
        } else {
            throwaway_recipient()?
        };
        let proved = prove_shielding_bundle(&pk, shielded_value, destination, change)?;

        // Two-phase assembly: the sighash commits to the bundle's contents but not to the
        // signatures over it, so the transaction is assembled once with a placeholder
        // authorization to derive the sighash, then rebuilt with the real signatures.
        let mut rng = rand_10::rng();
        let placeholder = proved
            .clone()
            .apply_signatures(&mut rng, [0u8; 32], &[])
            .map_err(|error| eyre!("placeholder signing failed: {error:?}"))?;

        let expiry_height = Height(tip.0.saturating_add(40));
        let build = |data: zorchard::ShieldedData| Transaction::V6 {
            network_upgrade: NetworkUpgrade::Nu7,
            lock_time: LockTime::unlocked(),
            expiry_height,
            inputs: vec![transparent::Input::PrevOut {
                outpoint: utxo.outpoint,
                unlock_script: transparent::Script::new(&[]),
                sequence: u32::MAX,
            }],
            // Mandatory: a transaction spending transparent coinbase must have no
            // transparent outputs.
            outputs: vec![],
            sapling_shielded_data: None,
            orchard_shielded_data: None,
            ironwood_shielded_data: Some(data),
        };

        // A V5 signature digest commits to the value and script of every spent output,
        // so the sighash cannot be computed from the transaction alone.
        let spent = std::sync::Arc::new(vec![transparent::Output {
            value: utxo.value,
            lock_script: utxo.script.clone(),
        }]);

        let draft = build(shielded_data_from_bundle(&placeholder)?);
        let draft_hasher = draft
            .sighasher(NetworkUpgrade::Nu7, spent.clone())
            .wrap_err("could not build a sighasher for the draft transaction")?;

        // The shielded bundle commits to the whole-transaction digest, with no transparent
        // input selected.
        let sighash: [u8; 32] = draft_hasher.sighash(HashType::ALL, None).0;

        // The transparent input commits to its own digest, which additionally binds this
        // input's prevout, amount and script.
        let transparent_sighash: [u8; 32] = draft_hasher
            .sighash(
                HashType::ALL,
                Some((0, utxo.script.as_raw_bytes().to_vec())),
            )
            .0;

        let authorized = proved
            .apply_signatures(&mut rng, sighash, &[])
            .map_err(|error| eyre!("signing the shielding bundle failed: {error:?}"))?;

        // Verify the proof here, against the same circuit era the node uses, so a proving
        // failure is distinguishable from a serialization failure at the node.
        match authorized.verify_proof(&orchard::circuit::VerifyingKey::build(CIRCUIT_VERSION)) {
            Ok(()) => tracing::info!("local proof verification passed"),
            Err(error) => bail!("local proof verification FAILED: {error:?}"),
        }

        // P2PKH scriptSig: <DER signature || hash type> <compressed public key>.
        let message = secp256k1::Message::from_digest(transparent_sighash);
        let signature = secp256k1::SECP256K1.sign_ecdsa(&message, &secret);
        let mut unlock = Vec::new();
        let mut der = signature.serialize_der().to_vec();
        der.push(HashType::ALL.bits() as u8);
        unlock.push(u8::try_from(der.len())?);
        unlock.extend_from_slice(&der);
        let public_key = secp256k1::PublicKey::from_secret_key_global(&secret).serialize();
        unlock.push(u8::try_from(public_key.len())?);
        unlock.extend_from_slice(&public_key);

        let mut tx = build(shielded_data_from_bundle(&authorized)?);
        if let Transaction::V6 { inputs, .. } = &mut tx {
            if let transparent::Input::PrevOut { unlock_script, .. } = &mut inputs[0] {
                *unlock_script = transparent::Script::new(&unlock);
            }
        }

        // The node verifies the bundle it parses back out of the wire bytes, not the one
        // built here, so check that path before submitting.
        let wire = tx.zcash_serialize_to_vec()?;
        let parsed = Transaction::zcash_deserialize(&wire[..])
            .wrap_err("the serialized transaction did not parse back")?;
        let reparsed_bundle = parsed
            .sighasher(NetworkUpgrade::Nu7, spent.clone())
            .wrap_err("could not build a sighasher for the parsed transaction")?
            .ironwood_bundle()
            .ok_or_else(|| eyre!("the parsed transaction has no Ironwood bundle"))?;

        // The sighash was taken from the draft, whose transparent input carries an empty
        // unlock script. If filling that script in changes the digest, every signature over
        // the draft sighash is over the wrong message.
        let final_sighash: [u8; 32] = parsed
            .sighasher(NetworkUpgrade::Nu7, spent.clone())
            .wrap_err("could not build a sighasher for the parsed transaction")?
            .sighash(HashType::ALL, None)
            .0;
        if final_sighash != sighash {
            bail!(
                "sighash changed once the unlock script was filled in: signed {} but the                  wire transaction hashes to {}",
                hex::encode(sighash),
                hex::encode(final_sighash),
            );
        }
        tracing::info!("sighash is stable across unlock-script insertion");

        let vk = orchard::circuit::VerifyingKey::build(CIRCUIT_VERSION);
        match reparsed_bundle.verify_proof(&vk) {
            Ok(()) => tracing::info!("re-parsed proof verification passed"),
            Err(error) => bail!(
                "re-parsed proof verification FAILED: {error:?}; the bundle changed across \
                 serialization (flags {:?} -> {:?}, anchor {} -> {})",
                authorized.flags(),
                reparsed_bundle.flags(),
                hex::encode(authorized.anchor().to_bytes()),
                hex::encode(reparsed_bundle.anchor().to_bytes()),
            ),
        }

        let raw = hex::encode(&wire);
        let txid: String = client
            .json_result_from_call("sendrawtransaction", format!(r#"["{raw}"]"#))
            .await
            .map_err(|error| eyre!("sendrawtransaction failed: {error}"))?;

        sent = sent.saturating_add(1);
        tracing::info!(%txid, sent, fee = args.fee, "transaction accepted into the mempool");
        if recipient.is_some() {
            println!("FAUCET_TXID={txid}");
        }

        if args.count != 0 && sent >= args.count {
            return Ok(());
        }
    }
}
