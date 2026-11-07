use std::sync::Arc;

use crate::{
    block::{Block, Height},
    parameters::Network,
    serialization::{SerializationError, ZcashDeserialize, ZcashDeserializeInto},
    transaction,
    transparent::Input,
};
use hex::FromHex;
use zcash_script::{opcode::Evaluable, pattern};

use zakura_test::prelude::*;

/// Builds a serialized transparent input wire-format with a null prevout (coinbase) and the given
/// `script_sig` bytes. Used to exercise [`Input::zcash_deserialize`] directly.
fn build_coinbase_wire(script_sig: &[u8]) -> Vec<u8> {
    let mut buf = vec![0u8; 32];
    buf.extend_from_slice(&0xffff_ffffu32.to_le_bytes());
    let len = script_sig.len();
    if len < 0xfd {
        buf.push(len as u8);
    } else if len <= 0xffff {
        buf.push(0xfd);
        buf.extend_from_slice(&(len as u16).to_le_bytes());
    } else {
        buf.push(0xfe);
        buf.extend_from_slice(&(len as u32).to_le_bytes());
    }
    buf.extend_from_slice(script_sig);
    buf.extend_from_slice(&0u32.to_le_bytes());
    buf
}

/// Returns the canonical BIP-34 height-prefix bytes for the given height, computed via the
/// upstream encoder used by `zcash_transparent::bundle::TxIn::coinbase`.
fn canonical_height_prefix(height: Height) -> Vec<u8> {
    pattern::push_num(i64::from(height.0)).to_bytes()
}

#[test]
fn coinbase_deserialize_rejects_non_minimal_heights() {
    let _init_guard = zakura_test::init();

    // Each of these is a non-canonical encoding of a valid height. They must all be rejected.
    // The trailing byte makes the script ≥ 2 bytes (the consensus minimum); without it the length
    // check would reject before the height check, masking the bug we are guarding against.
    let cases: &[(&str, &[u8])] = &[
        // Height 1: minimal is OP_1 (0x51). Length-prefixed forms are non-canonical.
        ("h1_via_01_01", &[0x01, 0x01, 0x00]),
        ("h1_via_02", &[0x02, 0x01, 0x00, 0x00]),
        ("h1_via_03", &[0x03, 0x01, 0x00, 0x00, 0x00]),
        ("h1_via_04", &[0x04, 0x01, 0x00, 0x00, 0x00, 0x00]),
        // Height 16: minimal is OP_16 (0x60). 1-byte push form is non-canonical.
        ("h16_via_01_10", &[0x01, 0x10, 0x00]),
        // Height 17: minimal is `[0x01, 0x11]`. Wider forms are non-canonical.
        ("h17_via_02", &[0x02, 0x11, 0x00, 0x00]),
        ("h17_via_03", &[0x03, 0x11, 0x00, 0x00, 0x00]),
    ];

    for (name, sig) in cases {
        let wire = build_coinbase_wire(sig);
        let result = Input::zcash_deserialize(&wire[..]);
        assert!(
            result.is_err(),
            "{name}: must reject non-canonical encoding {sig:02x?}, got {result:?}",
        );
    }
}

#[test]
fn coinbase_deserialize_accepts_canonical_heights() {
    let _init_guard = zakura_test::init();

    // Boundary heights spanning every valid prefix length, plus Height::MAX.
    let heights = [
        1,
        16,
        17,
        127,
        128,
        32_767,
        32_768,
        8_388_607,
        8_388_608,
        Height::MAX.0,
    ];

    for h in heights {
        let height = Height(h);
        let mut script_sig = canonical_height_prefix(height);
        // Pad to ≥ MIN_COINBASE_SCRIPT_LEN (= 2 bytes) so the length check passes.
        if script_sig.len() < 2 {
            script_sig.push(0x00);
        }
        let wire = build_coinbase_wire(&script_sig);
        let parsed = Input::zcash_deserialize(&wire[..])
            .unwrap_or_else(|e| panic!("must accept canonical height {h}: {e:?}"));
        match parsed {
            Input::Coinbase {
                height: parsed_h, ..
            } => assert_eq!(parsed_h, height, "round-trip mismatch for height {h}"),
            _ => panic!("expected Coinbase input for height {h}"),
        }
    }
}

/// Build a coinbase `Input` byte sequence: 32 zero bytes for the null outpoint
/// hash, the coinbase index `0xffffffff` little-endian, and a coinbase-script
/// CompactSize length followed by `payload`.
///
/// Used by the regression test below to confirm that `Input::zcash_deserialize`
/// rejects attacker-controlled coinbase script lengths *before* allocating or
/// reading the script bytes.
fn coinbase_input_bytes(compactsize_len: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(32 + 4 + compactsize_len.len() + payload.len());
    bytes.extend_from_slice(&[0u8; 32]);
    bytes.extend_from_slice(&0xffff_ffffu32.to_le_bytes());
    bytes.extend_from_slice(compactsize_len);
    bytes.extend_from_slice(payload);
    bytes
}

/// Coinbase scripts longer than the consensus maximum (100 bytes) must be
/// rejected at length-decode time, not after allocating the bytes.
///
/// Regression test: encoding a CompactSize length of 101 with no payload would
/// have made the previous implementation try to `read_exact(101)` and surface
/// an `io::Error`; the fix returns the consensus error string before the read.
#[test]
fn coinbase_script_oversize_rejected_before_allocation() {
    let _init_guard = zakura_test::init();

    let bytes = coinbase_input_bytes(&[101u8], &[]);
    let result = Input::zcash_deserialize(std::io::Cursor::new(&bytes));
    assert!(
        matches!(
            result,
            Err(SerializationError::Parse("Coinbase script is too long"))
        ),
        "expected `Coinbase script is too long`, got {result:?}",
    );
}

#[test]
fn coinbase_deserialize_rejects_out_of_range_heights() {
    let _init_guard = zakura_test::init();

    // Sign-bit set on the top byte: would decode as negative under signed-LE.
    let sig = [0x01, 0x80, 0x00];
    Input::zcash_deserialize(&build_coinbase_wire(&sig)[..])
        .expect_err("must reject negative-signed height encoding");

    // 6-byte push (n = 6 invalid; spec restricts heightBytes length to {1..=5}).
    let sig = [0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
    Input::zcash_deserialize(&build_coinbase_wire(&sig)[..])
        .expect_err("must reject 6-byte height push");

    // Height > Height::MAX. Height::MAX = 0x7FFF_FFFF, so 0x8000_0000 encoded with sign-bit clear
    // would need 5 bytes: [0x05, 0x00, 0x00, 0x00, 0x80, 0x00].
    let sig = [0x05, 0x00, 0x00, 0x00, 0x80, 0x00];
    Input::zcash_deserialize(&build_coinbase_wire(&sig)[..])
        .expect_err("must reject height > Height::MAX");
}

proptest! {
    #[test]
    fn coinbase_canonical_round_trip(h in 1u32..=Height::MAX.0) {
        let height = Height(h);
        let mut script_sig = canonical_height_prefix(height);
        if script_sig.len() < 2 {
            script_sig.push(0x00);
        }
        let wire = build_coinbase_wire(&script_sig);
        let parsed = Input::zcash_deserialize(&wire[..]).expect("canonical encoding must parse");
        match parsed {
            Input::Coinbase { height: parsed_h, .. } => prop_assert_eq!(parsed_h, height),
            _ => prop_assert!(false, "expected Coinbase input"),
        }
    }
}

#[test]
fn get_transparent_output_address() -> Result<()> {
    let _init_guard = zakura_test::init();

    let script_tx: Vec<u8> = <Vec<u8>>::from_hex("0400008085202f8901fcaf44919d4a17f6181a02a7ebe0420be6f7dad1ef86755b81d5a9567456653c010000006a473044022035224ed7276e61affd53315eca059c92876bc2df61d84277cafd7af61d4dbf4002203ed72ea497a9f6b38eb29df08e830d99e32377edb8a574b8a289024f0241d7c40121031f54b095eae066d96b2557c1f99e40e967978a5fd117465dbec0986ca74201a6feffffff020050d6dc0100000017a9141b8a9bda4b62cd0d0582b55455d0778c86f8628f870d03c812030000001976a914e4ff5512ffafe9287992a1cd177ca6e408e0300388ac62070d0095070d000000000000000000000000")
    .expect("Block bytes are in valid hex representation");

    let transaction = script_tx.zcash_deserialize_into::<Arc<transaction::Transaction>>()?;

    // Hashes were extracted from the transaction (parsed with zakura-chain,
    // then manually extracted from lock_script).
    // Final expected values were generated with https://secretscan.org/PrivateKeyHex,
    // by filling field 4 with the prefix followed by the address hash.
    // Refer to <https://zips.z.cash/protocol/protocol.pdf#transparentaddrencoding>
    // for the prefixes.

    // Script hash 1b8a9bda4b62cd0d0582b55455d0778c86f8628f
    let addr = transaction.outputs()[0]
        .address(&Network::Mainnet)
        .expect("should return address");
    assert_eq!(addr.to_string(), "t3M5FDmPfWNRG3HRLddbicsuSCvKuk9hxzZ");
    let addr = transaction.outputs()[0]
        .address(&Network::new_default_testnet())
        .expect("should return address");
    assert_eq!(addr.to_string(), "t294SGSVoNq2daz15ZNbmAW65KQZ5e3nN5G");
    // Public key hash e4ff5512ffafe9287992a1cd177ca6e408e03003
    let addr = transaction.outputs()[1]
        .address(&Network::Mainnet)
        .expect("should return address");
    assert_eq!(addr.to_string(), "t1ekRwsd4LaSsd6NXgsx66q2HxQWTLCF44y");
    let addr = transaction.outputs()[1]
        .address(&Network::new_default_testnet())
        .expect("should return address");
    assert_eq!(addr.to_string(), "tmWbBGi7TjExNmLZyMcFpxVh3ZPbGrpbX3H");

    Ok(())
}

#[test]
fn get_transparent_output_address_with_blocks() {
    let _init_guard = zakura_test::init();
    for network in Network::iter() {
        get_transparent_output_address_with_blocks_for_network(network);
    }
}

/// Test that the block test vector indexes match the heights in the block data,
/// and that each post-sapling block has a corresponding final sapling root.
fn get_transparent_output_address_with_blocks_for_network(network: Network) {
    let block_iter = network.block_iter();

    let mut valid_addresses = 0;

    for (&height, block_bytes) in block_iter.skip(1) {
        let block = block_bytes
            .zcash_deserialize_into::<Block>()
            .expect("block is structurally valid");

        for (idx, tx) in block.transactions.iter().enumerate() {
            for output in tx.outputs() {
                let addr = output.address(&network);
                if addr.is_none() && idx == 0 && output.lock_script.as_raw_bytes()[0] == 0x21 {
                    // There are a bunch of coinbase transactions with pay-to-pubkey scripts
                    // which we don't support; skip them
                    continue;
                }
                assert!(
                    addr.is_some(),
                    "address of {output:?}; block #{height}; tx #{idx}; must not be None",
                );
                valid_addresses += 1;
            }
        }
    }
    // Make sure we didn't accidentally skip all vectors
    assert!(valid_addresses > 0);
}

/// The address `Output::address` returned when it went through librustzcash's script solver.
fn solver_address(
    output: &crate::transparent::Output,
    network: &Network,
) -> Option<crate::transparent::Address> {
    use zcash_transparent::{address::TransparentAddress, bundle::TxOut};

    match TxOut::try_from(output).ok()?.recipient_address()? {
        TransparentAddress::PublicKeyHash(pkh) => Some(
            crate::transparent::Address::from_pub_key_hash(network.t_addr_kind(), pkh),
        ),
        TransparentAddress::ScriptHash(sh) => Some(crate::transparent::Address::from_script_hash(
            network.t_addr_kind(),
            sh,
        )),
    }
}

/// Wraps script bytes in an output with a valid amount.
fn output_with_script(script: &[u8]) -> crate::transparent::Output {
    crate::transparent::Output::new(
        crate::amount::Amount::try_from(1_000).expect("1000 zatoshis is a valid amount"),
        crate::transparent::Script::new(script),
    )
}

/// Script shapes next to P2PKH and P2SH: wrong hash lengths, non-direct pushes,
/// trailing bytes, and truncated scripts must not yield an address.
#[test]
fn output_address_matches_solver_on_near_standard_scripts() {
    let _init_guard = zakura_test::init();
    let hash = [0x42u8; 20];

    let p2pkh = [&[0x76, 0xa9, 0x14][..], &hash, &[0x88, 0xac]].concat();
    let p2sh = [&[0xa9, 0x14][..], &hash, &[0x87]].concat();
    let scripts = [
        p2pkh.clone(),
        p2sh.clone(),
        // PUSHDATA1 encoding of the same 20-byte hash.
        [&[0x76, 0xa9, 0x4c, 0x14][..], &hash, &[0x88, 0xac]].concat(),
        [&[0xa9, 0x4c, 0x14][..], &hash, &[0x87]].concat(),
        // 19- and 21-byte hashes with matching push lengths.
        [&[0x76, 0xa9, 0x13][..], &hash[..19], &[0x88, 0xac]].concat(),
        [&[0xa9, 0x15][..], &hash, &[0x00, 0x87]].concat(),
        // Push length that disagrees with the hash length.
        [&[0x76, 0xa9, 0x14][..], &hash, &[0x00, 0x88, 0xac]].concat(),
        // Trailing and missing bytes.
        [&p2pkh[..], &[0x00]].concat(),
        [&p2sh[..], &[0x87]].concat(),
        p2pkh[..p2pkh.len() - 1].to_vec(),
        p2sh[1..].to_vec(),
        Vec::new(),
    ];

    for network in Network::iter() {
        for script in &scripts {
            let output = output_with_script(script);
            assert_eq!(
                output.address(&network),
                solver_address(&output, &network),
                "script {}",
                hex::encode(script),
            );
        }
    }
}

proptest! {
    /// Random scripts, including ones that start with a standard prefix, get the
    /// same address as the solver gives them.
    #[test]
    fn output_address_matches_solver(
        prefix in prop::sample::select(vec![vec![], vec![0x76, 0xa9, 0x14], vec![0xa9, 0x14]]),
        body in prop::collection::vec(any::<u8>(), 0..30),
        suffix in prop::sample::select(vec![vec![], vec![0x88, 0xac], vec![0x87]]),
    ) {
        let script = [prefix, body, suffix].concat();
        let output = output_with_script(&script);
        for network in Network::iter() {
            prop_assert_eq!(output.address(&network), solver_address(&output, &network));
        }
    }
}
