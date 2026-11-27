//! Stack-1 (`bpv1-core`) tests: deterministic vectors, tool-5 property tests,
//! generated message/frame suites, layout validation, and
//! FIFO stream-writer invariants.

use std::sync::Arc;

use proptest::prelude::*;
use zakura_chain::{
    amount::Amount,
    block::{
        merkle::{AuthDataRoot, Root},
        Block, ChainHistoryBlockTxAuthCommitmentHash, ChainHistoryMmrRootHash, Height,
    },
    miner_mesh::{GatewayId, MinerMeshCommitment},
    parameters::Network,
    serialization::ZcashSerialize,
    transparent::{Output, Script},
};
use zakura_quic::{NodeId, NodeSecretKey};

use super::{
    test_support::{build_v6_coinbase, build_v6_tx, node_id, regtest_nu7},
    wire::{HAVE_BITMAP_MAX_BYTES, HAVE_FLAG_BITMAP},
    *,
};
use crate::zakura::{
    check_frame_filter,
    wire_codec::{
        decode_frame, encode_frame,
        message_suite::{check_layout_carries_family, message_suite, MessageSample, Violation},
        WireError,
    },
    Frame,
};

/// A complete post-NU7 block with a committed v6 coinbase.
struct Committed {
    network: Network,
    height: Height,
    block: Block,
    encoded: EncodedBody,
    binding: CoinbaseBinding,
    fields: CommitmentFields,
}

impl Committed {
    /// Verifies `binding` against this block's header and commitment fields.
    fn verify(&self, binding: &CoinbaseBinding) -> Result<(), CommitmentError> {
        verify_commitment(
            &self.network,
            self.height,
            &self.block.header,
            binding,
            &self.fields,
        )
        .map(|_| ())
    }

    /// Each stripe's `k` source payloads, as `assemble_block` takes them.
    fn source_parts(&self) -> Vec<Vec<Arc<[u8]>>> {
        let coding = &self.encoded.coding;
        self.encoded
            .parts
            .chunks(coding.n as usize)
            .map(|stripe| {
                stripe[..coding.k as usize]
                    .iter()
                    .map(VerifiedPart::payload_arc)
                    .collect()
            })
            .collect()
    }
}

/// [`Committed`] with `extra_tx_count` non-coinbase transactions.
fn build_committed_nu7_block(profile: &Profile, extra_tx_count: usize) -> Committed {
    build_committed_nu7_block_with_outputs(profile, extra_tx_count, Vec::new())
}

/// [`build_committed_nu7_block`] whose coinbase also pays `coinbase_outputs`.
fn build_committed_nu7_block_with_outputs(
    profile: &Profile,
    extra_tx_count: usize,
    coinbase_outputs: Vec<Output>,
) -> Committed {
    let network = regtest_nu7();
    let height = Height(10);
    let mut non_cb_txs = Vec::with_capacity(extra_tx_count);
    let mut body_bytes = Vec::new();
    for i in 0..extra_tx_count {
        let tx = build_v6_tx(height, (i as u8).wrapping_add(1));
        tx.zcash_serialize(&mut body_bytes)
            .expect("tx serialization succeeds");
        non_cb_txs.push(Arc::new(tx));
    }

    let tx_count = u32::try_from(1 + extra_tx_count).expect("tx_count fits u32");
    let encoded = encode_body(profile, &body_bytes, tx_count).expect("encode_body succeeds");

    let chain_id = network.genesis_hash().0;
    let fields = CommitmentFields {
        chain_id,
        profile_id: W2_PROFILE_ID,
        coding: encoded.coding,
        part_root: encoded.part_root,
        proposer_node: node_id(1),
    };
    let script = commitment_script(commit_hash(&fields));
    let coinbase = build_v6_coinbase(height, &script, 0, coinbase_outputs, &[]);

    let mut txs = Vec::with_capacity(1 + extra_tx_count);
    txs.push(Arc::new(coinbase));
    txs.extend(non_cb_txs);

    let merkle_root: Root = txs.iter().map(|t| t.as_ref()).collect();
    let auth_root: AuthDataRoot = txs.iter().map(|t| t.as_ref()).collect();
    let light_client_root = [0x42u8; 32];
    let block_commitment = ChainHistoryBlockTxAuthCommitmentHash::from_commitments(
        &ChainHistoryMmrRootHash::from(light_client_root),
        &auth_root,
    );

    let mut header = *zakura_chain::block::genesis::regtest_genesis_block().header;
    header.merkle_root = merkle_root;
    *header.commitment_bytes = <[u8; 32]>::from(block_commitment);

    let block = Block {
        header: Arc::new(header),
        transactions: txs,
    };
    let paths = coinbase_paths(&block).expect("the block has a coinbase");
    let binding = CoinbaseBinding {
        coinbase: paths.coinbase,
        txid_siblings: paths.txid_siblings,
        auth_siblings: paths.auth_siblings,
        light_client_root,
    };

    Committed {
        network,
        height,
        block,
        encoded,
        binding,
        fields,
    }
}

/// decision-18: the part size table in 01 §4, the multi-stripe switch at
/// `STRIPE_K × S_MAX`, and `wire_len(i)` trimming at the body's end.
#[test]
fn w2_part_size_follows_the_body() {
    for (body, s, k, n) in [
        (4_000, 4_096, 1, 2),
        (40_000, 4_096, 10, 14),
        (200_000, 4_096, 49, 66),
        // S = round_up_64(ceil(1_990_000 / 64)).
        (1_990_000, 31_104, 64, 86),
        (2_000_000, 31_296, 64, 86),
    ] {
        let coding = Profile::W2.derive_coding(body, 2).expect("valid body");
        assert_eq!(
            (coding.part_bytes, coding.stripes, coding.k, coding.n),
            (s, 1, k, n),
            "body {body}"
        );
        // Source parts end where the body ends; parity carries all `S` bytes.
        let trimmed: u64 = (0..coding.k)
            .map(|i| u64::from(coding.wire_len(i).expect("in range")))
            .sum();
        assert_eq!(trimmed, body, "body {body}");
        assert_eq!(coding.wire_len(coding.k), Some(s));
        assert_eq!(coding.wire_len(coding.n), None);
    }

    // A near-2MB body has tree depth 7. Overflowing the 2,000,000-byte block
    // limit fails checked arithmetic before allocation.
    let near_limit = Profile::W2.derive_coding(1_990_000, 100).unwrap();
    assert_eq!(
        (near_limit.tree_depth(), near_limit.stripe_tree_depth()),
        (7, 7)
    );
    assert!(Profile::W2.validate_coding(&near_limit, 1487, 300).is_ok());
    assert!(matches!(
        Profile::W2.validate_coding(&near_limit, 1487, 10_000),
        Err(LayoutError::BlockTooLarge { .. })
    ));

    // Above `STRIPE_K × S_MAX` (4 MiB) the part size stays at `S_MAX` and stripes multiply.
    let big = Profile::W2.derive_coding(4 * 1024 * 1024 + 1, 2).unwrap();
    assert_eq!((big.part_bytes, big.stripes, big.k), (65_536, 2, 33));
    // `stripes × k = 66 > K = 65`: the last source slot holds only padding.
    let last_source = big.wire_index(1, big.k - 1).unwrap();
    assert!(big.is_padding(last_source));
    assert_eq!(big.wire_len(big.wire_index(1, big.k - 2).unwrap()), Some(1));
    assert_eq!(
        (0..big.total_parts().unwrap())
            .filter(|&i| big.is_padding(i))
            .count(),
        1
    );
}

/// decision-18: a padding source part needs no entry in `held`. Decode treats
/// it as zeros, and the re-encode check covers it with the stripe root.
#[test]
fn padding_part_decodes_as_zeros() {
    let body: Vec<u8> = (0..160u8).collect();
    let encoded = encode_body(&Profile::TINY, &body, 2).unwrap();
    let coding = encoded.coding;
    let n = coding.n as usize;
    assert!(coding.is_padding(coding.wire_index(1, 2).unwrap()));
    assert_eq!(encoded.parts[n + 2].wire_payload(), &[] as &[u8]);
    // Stripe 1: source slot 1 and the parity slot, with slot 0 missing.
    let mut held: Vec<Option<VerifiedPart>> = vec![None; n];
    held[1] = Some(encoded.parts[n + 1].clone());
    held[3] = Some(encoded.parts[n + 3].clone());
    let checked = decode_and_check_stripe(&coding, &encoded.encoding_id, 1, &held)
        .expect("two real parts and the padding reach k");
    assert_eq!(checked, encoded.parts[n..2 * n].to_vec());
}

#[test]
fn deterministic_codec_tree_and_v6_commitment_vectors() {
    // Vector 1: TINY multi-stripe block with odd tx count (5 txs -> depth 3).
    let committed = build_committed_nu7_block(&Profile::TINY, 4);
    let Committed {
        block,
        encoded,
        binding,
        ..
    } = &committed;

    assert!(
        encoded.coding.stripes >= 2,
        "TINY test block spans multiple stripes"
    );
    let enc_id = encoded.encoding_id;
    let part_root = encoded.part_root;

    // Every encoded part verifies against `part_root` and matches its `stripe_root`.
    for part in &encoded.parts {
        let verified = verify_part_proof(
            &encoded.coding,
            &enc_id,
            &part_root,
            part.index(),
            part.siblings().to_vec(),
            part.wire_payload(),
        )
        .expect("encoded part proof verifies");
        let (stripe, _) = encoded.coding.split_index(part.index()).unwrap();
        assert_eq!(
            verified.stripe_root(),
            encoded.parts[(stripe * encoded.coding.n) as usize].stripe_root()
        );
    }

    // Reassemble from every stripe's source parts and verify byte-exact equality with `block`.
    let (raw, assembled) = assemble_block(
        &Profile::TINY,
        &block.header,
        &encoded.coding,
        &binding.coinbase,
        &committed.source_parts(),
    )
    .expect("canonical block assembly succeeds");
    assert_eq!(&assembled, block);
    assert_eq!(raw, block.zcash_serialize_to_vec().unwrap());
}

/// bpv1-68: a coinbase far above a few KB, which consensus accepts, still
/// takes the fast path. Its `HeaderMeta` encodes, decodes, passes the layout
/// check, and verifies (decision-23).
#[test]
fn large_coinbase_keeps_the_fast_path() {
    let outputs: Vec<Output> = (0..40u8)
        .map(|i| {
            Output::new(
                Amount::try_from(0).expect("valid test zatoshi"),
                Script::new(&[i; 1_000]),
            )
        })
        .collect();
    let committed = build_committed_nu7_block_with_outputs(&Profile::W2, 2, outputs);
    let Committed {
        block,
        encoded,
        binding,
        ..
    } = &committed;
    assert!(binding.coinbase.len() > 40_000);
    let header = block.header.zcash_serialize_to_vec().unwrap();
    Profile::W2
        .validate_coding(&encoded.coding, header.len(), binding.coinbase.len())
        .expect("the layout admits a large coinbase");

    let meta = Bpv1Message::HeaderMeta {
        header,
        binding: binding.clone(),
        coding: encoded.coding,
        part_root: encoded.part_root,
        proposer_node: node_id(1),
    };
    let frame = encode_frame(&meta).unwrap();
    assert_eq!(decode_frame::<Bpv1Message>(&frame).unwrap(), meta);
    committed
        .verify(binding)
        .expect("a large coinbase verifies");
}

#[test]
fn commitment_rejects_auth_data_mutation_and_coinbase_edge_cases() {
    // Odd tx counts 1, 2, 3, 5 all verify cleanly.
    for extra in [0usize, 1, 2, 4] {
        let committed = build_committed_nu7_block(&Profile::TINY, extra);
        committed
            .verify(&committed.binding)
            .expect("odd and even tx counts verify");
    }

    // Named scenario (05 §3): Coinbase input script altered by a relay -> check-3 fails!
    // Under ZIP 244, mutating the coinbase input script does NOT change txid,
    // so check-2 passes and check-3 catches the tamper!
    let committed = build_committed_nu7_block(&Profile::TINY, 2);
    let Committed {
        height,
        block,
        binding,
        fields,
        ..
    } = &committed;
    let c = commit_hash(fields);
    let altered_cb = build_v6_coinbase(*height, &commitment_script(c), 0, Vec::new(), &[0xFF]);
    assert_eq!(
        altered_cb.hash(),
        block.transactions[0].hash(),
        "ZIP 244 txid ignores coinbase input script"
    );
    assert_ne!(
        altered_cb.auth_digest(),
        block.transactions[0].auth_digest(),
        "ZIP 244 auth digest covers coinbase input script"
    );
    let mut altered_binding = binding.clone();
    altered_binding.coinbase = altered_cb.zcash_serialize_to_vec().unwrap();
    assert_eq!(
        committed.verify(&altered_binding),
        Err(CommitmentError::BlockCommitmentMismatch)
    );

    // Pre-NU7 height is rejected (decision-9).
    let pre_nu7_net = Network::new_regtest(
        zakura_chain::parameters::testnet::ConfiguredActivationHeights {
            nu7: Some(100),
            ..Default::default()
        }
        .into(),
    );
    assert_eq!(
        verify_commitment(&pre_nu7_net, *height, &block.header, binding, fields),
        Err(CommitmentError::UnsupportedNetworkUpgrade)
    );

    // Wrong path depth is rejected before parsing.
    let mut bad_depth = binding.clone();
    bad_depth.txid_siblings.push([0u8; 32]);
    assert!(matches!(
        committed.verify(&bad_depth),
        Err(CommitmentError::WrongPathDepth { .. })
    ));

    // Trailing coinbase bytes rejected.
    let mut trailing_cb = binding.clone();
    trailing_cb.coinbase.push(0x00);
    assert_eq!(
        committed.verify(&trailing_cb),
        Err(CommitmentError::TrailingCoinbaseBytes)
    );
}

/// bpv1-70 (decision-25): only an exact zero-value `6a 23 "BP" 01 C`
/// output is a commitment. Readers ignore every other output, and a coinbase
/// with two commitments has no binding.
#[test]
fn commitment_ignores_unusual_outputs_and_needs_exactly_one() {
    let zero = |script: &[u8]| Output::new(Amount::zero(), Script::new(script));
    let c = commit_hash(&build_committed_nu7_block(&Profile::TINY, 2).fields);
    let verify = |outputs: Vec<Output>| {
        let committed = build_committed_nu7_block_with_outputs(&Profile::TINY, 2, outputs);
        committed.verify(&committed.binding)
    };

    // `BP 01` outputs of another version, length, push encoding, or value.
    let script = commitment_script(c);
    let mut version_02 = script;
    version_02[4] = 0x02;
    let mut long = script.to_vec();
    long.push(0x00);
    let short = &script[..script.len() - 1];
    let mut pushdata1 = vec![0x6a, 0x4c, 0x23];
    pushdata1.extend_from_slice(&script[2..]);
    let mut junk = script[..4].to_vec();
    junk.extend_from_slice(&[0xee; 40]);
    let unusual = vec![
        zero(&version_02),
        zero(&long),
        zero(short),
        zero(&pushdata1),
        zero(&junk),
        Output::new(Amount::try_from(1).unwrap(), Script::new(&script)),
    ];
    for output in unusual.clone() {
        assert_eq!(verify(vec![output]), Ok(()), "an unusual output is ignored");
    }
    assert_eq!(verify(unusual), Ok(()), "all unusual outputs are ignored");

    // Two valid `BP 01` outputs: no binding.
    assert_eq!(
        verify(vec![zero(&script)]),
        Err(CommitmentError::MultipleCommitmentOutputs)
    );

    // Only unusual outputs: no binding.
    let only_unusual = build_v6_coinbase(Height(10), &version_02, 0, vec![], &[]);
    assert_eq!(
        extract_single_commitment(&only_unusual),
        Err(CommitmentError::MissingCommitmentOutput)
    );
}

/// A coinbase can carry both a BPv1 commitment and a miner mesh output. Their
/// tags `BP` and `MR` differ, so each reader finds its own output and ignores
/// the other.
#[test]
fn commitment_and_miner_mesh_outputs_stay_distinct_in_one_coinbase() {
    let mesh = MinerMeshCommitment::new(GatewayId::from_bytes([0x77; 20]));
    let committed =
        build_committed_nu7_block_with_outputs(&Profile::TINY, 2, vec![mesh.to_output()]);
    assert_eq!(committed.verify(&committed.binding), Ok(()));

    let coinbase = &committed.block.transactions[0];
    let [bpv1, mesh_output] = coinbase.outputs() else {
        panic!("the coinbase holds the BPv1 and miner mesh outputs only");
    };
    assert_eq!(
        extract_single_commitment(coinbase),
        Ok(commit_hash(&committed.fields))
    );
    assert_eq!(
        MinerMeshCommitment::from_coinbase_outputs(coinbase.outputs()),
        Some(mesh)
    );

    // Each reader ignores the other's output on its own.
    assert_eq!(
        MinerMeshCommitment::from_coinbase_outputs(std::slice::from_ref(bpv1)),
        None
    );
    let mesh_only = build_v6_coinbase(
        Height(10),
        mesh_output.lock_script.as_raw_bytes(),
        0,
        vec![],
        &[],
    );
    assert_eq!(
        extract_single_commitment(&mesh_only),
        Err(CommitmentError::MissingCommitmentOutput)
    );

    // Both scripts are `OP_RETURN` and one push, and differ in the tag.
    let bpv1_script = bpv1.lock_script.as_raw_bytes();
    let mesh_script = mesh_output.lock_script.as_raw_bytes();
    assert_eq!(bpv1_script[0], mesh_script[0]);
    assert_eq!(&bpv1_script[2..4], b"BP");
    assert_eq!(&mesh_script[2..4], b"MR");
}

/// From NU7.1 a block's coinbase can be a ZIP 248 v7 transaction, which has no coinbase
/// input. Its txid and auth digest bind it to the header like a v6 coinbase, and its
/// transparent outputs carry the commitment.
#[test]
fn commitment_binds_a_v7_coinbase() {
    use zakura_chain::{
        parameters::{testnet::ConfiguredActivationHeights, NetworkUpgrade},
        transaction::{
            v7::{CoinbaseBundle, V7Fields, ValueDeltas},
            LockTime, Transaction,
        },
    };

    let network = Network::new_regtest(
        ConfiguredActivationHeights {
            nu7: Some(1),
            nu7_1: Some(2),
            ..Default::default()
        }
        .into(),
    );
    let height = Height(10);
    let fields = build_committed_nu7_block(&Profile::TINY, 0).fields;
    let paid = Amount::try_from(1_000_000).unwrap();
    let v7_coinbase = |coinbase: Option<CoinbaseBundle>| Transaction::V7 {
        network_upgrade: NetworkUpgrade::Nu7_1,
        lock_time: LockTime::unlocked(),
        expiry_height: height,
        inputs: Vec::new(),
        outputs: vec![
            Output::new(paid.constrain().unwrap(), Script::new(&[0x51])),
            Output::new(
                Amount::zero(),
                Script::new(&commitment_script(commit_hash(&fields))),
            ),
        ],
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: None,
        v7_fields: Box::new(V7Fields {
            value_deltas: ValueDeltas {
                transparent: -paid,
                coinbase: paid,
                ..ValueDeltas::default()
            },
            coinbase,
        }),
    };
    let bundle = CoinbaseBundle::new(
        height,
        paid.constrain().unwrap(),
        Amount::zero(),
        b"miner".to_vec(),
        None,
    )
    .unwrap();
    let coinbase = v7_coinbase(Some(bundle));

    let block = super::fuzz::chain::block_of([0x42; 32], vec![Arc::new(coinbase.clone())]);
    let paths = coinbase_paths(&block).expect("the block has a coinbase");
    let binding = CoinbaseBinding {
        coinbase: paths.coinbase,
        txid_siblings: paths.txid_siblings,
        auth_siblings: paths.auth_siblings,
        light_client_root: [0x42; 32],
    };
    assert_eq!(
        verify_commitment(&network, height, &block.header, &binding, &fields),
        Ok(coinbase)
    );

    // A v7 transaction without a coinbase bundle is not a coinbase.
    let not_coinbase = v7_coinbase(None).zcash_serialize_to_vec().unwrap();
    assert_eq!(
        commitment::decode_coinbase(&not_coinbase),
        Err(CommitmentError::NotCoinbase)
    );
}

#[test]
fn corrupted_parity_under_valid_proofs_fails_reencode_check() {
    // Named scenario (05 §3): proposer commits valid Merkle proofs over an
    // invalid RS codeword (a parity shard was tampered BEFORE building the tree).
    let body = vec![0x5Au8; 100];
    let encoded = encode_body(&Profile::TINY, &body, 2).expect("encode succeeds");
    let coding = encoded.coding;
    let k = coding.k as usize;
    let n = coding.n as usize;

    // Tamper with stripe 0's first parity shard BEFORE building the Merkle tree.
    let mut raw_parts: Vec<Vec<u8>> = encoded.parts.iter().map(|p| p.payload().to_vec()).collect();
    raw_parts[k][0] ^= 0xFF;

    let bad_enc_id = encoding_id(&coding);
    let payloads = raw_parts.iter().map(|p| Arc::from(p.as_slice())).collect();
    let (bad_root, bad_parts) = build_part_tree(&coding, &bad_enc_id, payloads)
        .expect("malicious proposer builds valid tree");

    // Receiver verifies proofs (all valid against `bad_root`) for slots 1..=k
    // (using the tampered parity shard k instead of source shard 0).
    let mut held: Vec<Option<VerifiedPart>> = vec![None; n];
    for slot in 1..=k {
        let idx = slot as u32;
        let proof = bad_parts[slot].siblings().to_vec();
        let verified = verify_part_proof(
            &coding,
            &bad_enc_id,
            &bad_root,
            idx,
            proof,
            bad_parts[slot].wire_payload(),
        )
        .expect("proof is valid against malicious proposer's part_root");
        held[slot] = Some(verified);
    }

    // Decoding + re-encoding detects the invalid codeword (bpv1-41).
    let result = decode_and_check_stripe(&coding, &bad_enc_id, 0, &held);
    assert_eq!(result.unwrap_err(), CodecError::CodewordCheckFailed);
}

#[test]
fn restored_source_part_with_a_nonzero_tail_fails_reencode_check() {
    // A malicious proposer commits a consistent codeword whose trimmed source
    // part has nonzero bytes past its wire length. That part never verifies on
    // the wire, so every node restores it from parity.
    let body = vec![0x5Au8; 10];
    let encoded = encode_body(&Profile::TINY, &body, 2).expect("encode succeeds");
    let coding = encoded.coding;
    assert_eq!(coding.stripes, 1);
    let k = coding.k as usize;
    let n = coding.n as usize;
    // Widening usize to u32 is lossless: `slot < k`.
    let trimmed = (0..k)
        .find(|&slot| coding.wire_len(slot as u32) < Some(coding.part_bytes))
        .expect("the body does not fill its last source part");

    let mut sources: Vec<Vec<u8>> = encoded.parts[..k]
        .iter()
        .map(|p| p.payload().to_vec())
        .collect();
    *sources[trimmed].last_mut().expect("parts are not empty") = 0xFF;
    let parity = reed_solomon_simd::encode(k, n - k, &sources).expect("encode succeeds");
    let payloads = sources
        .into_iter()
        .chain(parity)
        .map(|p| Arc::from(p.as_slice()))
        .collect();
    let enc_id = encoding_id(&coding);
    let (root, parts) =
        build_part_tree(&coding, &enc_id, payloads).expect("malicious proposer builds valid tree");

    let mut held: Vec<Option<VerifiedPart>> = vec![None; n];
    for (slot, part) in parts.iter().enumerate() {
        let index = part.index();
        let proof = part.siblings().to_vec();
        let verified =
            verify_part_proof(&coding, &enc_id, &root, index, proof, part.wire_payload());
        if slot == trimmed {
            assert_eq!(verified.unwrap_err(), ProofError::RootMismatch);
        } else {
            held[slot] = Some(verified.expect("proof is valid against the committed root"));
        }
    }

    let result = decode_and_check_stripe(&coding, &enc_id, 0, &held);
    assert_eq!(result.unwrap_err(), CodecError::CodewordCheckFailed);
}

#[test]
fn assemble_rejects_nonzero_padding() {
    let committed = build_committed_nu7_block(&Profile::TINY, 1);
    let Committed {
        block,
        encoded,
        binding,
        ..
    } = &committed;
    let mut stripe_sources = committed.source_parts();
    // Flip the very last padding byte of the last source part.
    let last_stripe = stripe_sources.last_mut().unwrap();
    let mut last_part = last_stripe.last().unwrap().to_vec();
    *last_part.last_mut().unwrap() ^= 0x01;
    *last_stripe.last_mut().unwrap() = Arc::from(last_part);

    assert_eq!(
        assemble_block(
            &Profile::TINY,
            &block.header,
            &encoded.coding,
            &binding.coinbase,
            &stripe_sources,
        ),
        Err(AssembleError::NonZeroPadding)
    );
}

/// An empty `B'` has no parts: `K = stripes = k = n = 0`. Its part tree is
/// one pad leaf, so `part_root = T("pad", encoding_id || u32(0))`, and the
/// block assembles from the header and the coinbase alone.
#[test]
fn empty_body_has_no_parts_a_pad_root_and_assembles_from_header_meta() {
    for profile in [Profile::W2, Profile::TINY] {
        let coding = profile.derive_coding(0, 1).unwrap();
        assert_eq!(
            (coding.stripes, coding.k, coding.n, coding.part_bytes),
            (0, 0, 0, profile.s_min)
        );
        assert_eq!(coding.total_parts(), Some(0));
        assert_eq!(coding.tree_depth(), 0);
        assert_eq!(coding.split_index(0), None);
        assert_eq!(coding.wire_len(0), None);

        let encoded = encode_body(&profile, &[], 1).unwrap();
        assert_eq!(encoded.coding, coding);
        assert!(encoded.parts.is_empty());
        assert_eq!(encoded.part_root, pad_hash(&encoding_id(&coding), 0));
        assert_eq!(
            verify_part_proof(
                &coding,
                &encoded.encoding_id,
                &encoded.part_root,
                0,
                Vec::new(),
                &[]
            ),
            Err(ProofError::IndexOutOfRange)
        );
    }

    let Committed {
        block,
        encoded,
        binding,
        ..
    } = build_committed_nu7_block(&Profile::TINY, 0);
    assert_eq!(encoded.coding.stripes, 0);
    let header = block.header.zcash_serialize_to_vec().unwrap();
    Profile::TINY
        .validate_coding(&encoded.coding, header.len(), binding.coinbase.len())
        .expect("an empty body's zero-part layout is valid");
    let (raw, assembled) = assemble_block(
        &Profile::TINY,
        &block.header,
        &encoded.coding,
        &binding.coinbase,
        &[],
    )
    .expect("a coinbase-only block assembles with no stripes");
    assert_eq!(assembled, block);
    assert_eq!(raw, block.zcash_serialize_to_vec().unwrap());
}

/// A receiver accepts zero stripes only with zero body bytes (bpv1-10).
#[test]
fn layout_rejects_zero_parts_with_body_bytes_and_parts_without_body_bytes() {
    let profile = Profile::TINY;
    let validate = |coding: &Coding| profile.validate_coding(coding, 80, 100);
    let empty = profile.derive_coding(0, 1).unwrap();
    validate(&empty).expect("the derived empty layout is valid");

    // Zero parts with body bytes.
    let non_empty = profile.derive_coding(100, 2).unwrap();
    let zero_parts = Coding {
        stripes: 0,
        k: 0,
        n: 0,
        ..non_empty
    };
    assert_eq!(validate(&zero_parts), Err(LayoutError::CodingMismatch));

    // Parts with no body bytes: the old one-stripe empty layout.
    let one_stripe = Coding {
        stripes: 1,
        k: 1,
        n: 2,
        ..empty
    };
    assert_eq!(validate(&one_stripe), Err(LayoutError::CodingMismatch));
}

/// A padding part (`wire_len = 0`) exists only when `stripes * k > K >= 1`.
/// Fewer than `stripes` parts are padding, and every stripe's slot 0 carries
/// body bytes, so no stripe is all padding (decision-18).
#[test]
fn no_stripe_is_all_padding() {
    let cases = (1..=2_048u64)
        .map(|len| (Profile::TINY, len))
        .chain([1, 4_096, 262_144, 262_145, 2_000_000].map(|len| (Profile::W2, len)));
    for (profile, len) in cases {
        let coding = profile.derive_coding(len, 2).unwrap();
        let padding = (0..coding.total_parts().unwrap())
            .filter(|&index| coding.is_padding(index))
            .count();
        // Widening u32 to usize is lossless on supported targets.
        assert!(
            padding < coding.stripes as usize,
            "{len}: {padding} padding parts"
        );
        for stripe in 0..coding.stripes {
            let first = coding.wire_index(stripe, 0).unwrap();
            assert!(
                !coding.is_padding(first),
                "{len}: stripe {stripe} is all padding"
            );
        }
    }
}

#[test]
fn fifo_queue_keeps_every_item_and_cancels_parts_but_never_ends() {
    // A conformant peer's 65 served 64 KiB parts and a `Have` exceed
    // the old 4 MiB cap; the queue keeps all of them, in order.
    let block = zakura_chain::block::Hash([1u8; 32]);
    let encoded = encode_body(&Profile::TINY, &[7u8; 64], 2).expect("TINY body encodes");
    let part = encoded.parts[0].clone();
    let mut queue = Bpv1FifoQueue::default();
    for _ in 0..65 {
        queue.push_back(DataQueueItem::Part {
            block_id: block,
            stripe: 0,
            part: part.clone(),
            push: false,
        });
    }
    queue.push_back(DataQueueItem::Have {
        block_id: block,
        flags: HaveFlags::Plain,
        form: HaveForm::List,
        indices: vec![0],
    });
    queue.push_back(DataQueueItem::WantEnd {
        want_id: 1,
        sent: 65,
        reason: WantEndReason::Served,
    });
    assert_eq!(queue.len(), 67);
    assert_eq!(queue.part_bytes(), 65 * part.payload().len());

    // Cancelling stripe 0 removes the parts but keeps the `Have` and the
    // `WantEnd` behind them (bpv1-29, bpv1-55).
    assert_eq!(queue.cancel_stripe(block, 0), 65);
    assert_eq!(queue.part_bytes(), 0);
    assert!(matches!(
        queue.pop_front(),
        Some(DataQueueItem::Have { .. })
    ));
    assert!(matches!(
        queue.pop_front(),
        Some(DataQueueItem::WantEnd { want_id: 1, .. })
    ));
}

#[test]
fn fifo_queue_cancels_only_the_named_holder_push() {
    // 03 §5.4: the peer announced or sent part 0 meanwhile, so its queued
    // push is skipped; a served copy and other pushes stay.
    let block = zakura_chain::block::Hash([1u8; 32]);
    let encoded = encode_body(&Profile::TINY, &[7u8; 64], 2).expect("TINY body encodes");
    let part = |index: usize| encoded.parts[index].clone();
    let mut queue = Bpv1FifoQueue::default();
    for (index, push) in [(0, true), (0, false), (1, true)] {
        queue.push_back(DataQueueItem::Part {
            block_id: block,
            stripe: 0,
            part: part(index),
            push,
        });
    }
    assert_eq!(queue.cancel_push(block, 0), 1);
    assert_eq!(queue.cancel_push(block, 0), 0);
    assert!(matches!(
        queue.pop_front(),
        Some(DataQueueItem::Part { .. })
    ));
    assert!(matches!(
        queue.pop_front(),
        Some(DataQueueItem::Part { push: true, ref part, .. }) if part.index() == 1
    ));
}

#[test]
fn bpv1_layout_passes_frame_suite_and_matches_family() {
    check_frame_filter(&BPV1_STREAMS);
    check_layout_carries_family::<Bpv1Message>(&BPV1_STREAMS);
}

proptest! {
    /// Tool-5: any `k`-subset of a stripe decodes to the exact source parts and
    /// passes the re-encode check across both single-stripe and multi-stripe blocks.
    #[test]
    fn any_k_subset_decodes_and_passes_reencode_check(
        body in prop::collection::vec(any::<u8>(), 1..=512),
        seed in any::<u64>(),
    ) {
        let encoded = encode_body(&Profile::TINY, &body, 2).expect("encode succeeds");
        let coding = encoded.coding;
        let enc_id = encoded.encoding_id;
        let k = coding.k as usize;
        let n = coding.n as usize;

        for stripe in 0..coding.stripes {
            let base = (stripe * coding.n) as usize;
            // Deterministically select a k-subset mixing source and parity parts.
            let mut slots: Vec<usize> = (0..n).collect();
            let mut s = seed.wrapping_add(u64::from(stripe));
            for i in (1..n).rev() {
                s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                let j = (s as usize) % (i + 1);
                slots.swap(i, j);
            }
            let mut held: Vec<Option<VerifiedPart>> = vec![None; n];
            for &slot in slots.iter().take(k) {
                held[slot] = Some(encoded.parts[base + slot].clone());
            }
            let checked = decode_and_check_stripe(&coding, &enc_id, stripe, &held)
                .expect("any k verified parts decode and pass re-encode check");
            for (part, source) in checked.iter().zip(&encoded.parts[base..base + k]) {
                prop_assert_eq!(part.payload(), source.payload());
            }
            // Regenerated parts carry valid full Merkle proofs to `part_root`.
            for part in &checked {
                let re_verified = verify_part_proof(
                    &coding,
                    &enc_id,
                    &encoded.part_root,
                    part.index(),
                    part.siblings().to_vec(),
                    part.wire_payload(),
                );
                prop_assert!(re_verified.is_ok());
            }
        }
    }
}

// --- Generated wire message suite (`message_suite!`) ---

fn sample_coding() -> Coding {
    Profile::W2.derive_coding(65_536, 2).unwrap()
}

fn sample_want_bitmap() -> WantBitmap {
    let mut b = WantBitmap::EMPTY;
    b.set(0);
    b.set(3);
    b
}

impl MessageSample for Bpv1Message {
    fn samples() -> Vec<Self> {
        let min_header_meta = Bpv1Message::HeaderMeta {
            header: vec![0x11],
            binding: CoinbaseBinding {
                coinbase: vec![0x22],
                txid_siblings: Vec::new(),
                auth_siblings: Vec::new(),
                light_client_root: [0x33; 32],
            },
            coding: sample_coding(),
            part_root: [0x44; 32],
            proposer_node: node_id(1),
        };
        let max_header_meta = Bpv1Message::HeaderMeta {
            header: vec![0xAA; profile::W2_MAX_HEADER_BYTES],
            binding: CoinbaseBinding {
                coinbase: vec![0xBB; profile::W2_MAX_COINBASE_BYTES],
                txid_siblings: vec![[0xCC; 32]; profile::W2_MAX_COMMITMENT_PATH_DEPTH],
                auth_siblings: vec![[0xDD; 32]; profile::W2_MAX_COMMITMENT_PATH_DEPTH],
                light_client_root: [0xEE; 32],
            },
            coding: sample_coding(),
            part_root: [0x55; 32],
            proposer_node: NodeSecretKey::from_bytes(&[0u8; 32]).public(),
        };

        let want = Bpv1Message::Want {
            want_id: 1,
            block_id: zakura_chain::block::Hash([1u8; 32]),
            stripe: 0,
            requested: sample_want_bitmap(),
        };
        let stripe_done = Bpv1Message::StripeDone {
            block_id: zakura_chain::block::Hash([2u8; 32]),
            stripe: 3,
        };
        let block_done = Bpv1Message::BlockDone {
            block_id: zakura_chain::block::Hash([3u8; 32]),
        };
        let min_part = Bpv1Message::Part {
            block_id: zakura_chain::block::Hash([4u8; 32]),
            index: 0,
            siblings: Vec::new(),
            payload: Vec::new(),
        };
        let max_part = Bpv1Message::Part {
            block_id: zakura_chain::block::Hash([4u8; 32]),
            index: 1,
            siblings: vec![[0x66; 32]; profile::W2_MAX_PROOF_DEPTH],
            payload: vec![0x77; profile::W2_S_MAX as usize],
        };
        // The shortest `Have` is a one-byte bitmap (decision-33).
        let min_have = Bpv1Message::Have {
            block_id: zakura_chain::block::Hash([5u8; 32]),
            flags: HaveFlags::Plain,
            form: HaveForm::Bitmap { len: 1 },
            indices: vec![0],
        };
        let list_have = Bpv1Message::Have {
            block_id: zakura_chain::block::Hash([5u8; 32]),
            flags: HaveFlags::Push,
            form: HaveForm::List,
            indices: vec![0, 43_000],
        };
        // A 2 MB block's 86 parts as an 11-byte bitmap.
        let w2_bitmap_have = Bpv1Message::Have {
            block_id: zakura_chain::block::Hash([5u8; 32]),
            flags: HaveFlags::Relay,
            form: HaveForm::Bitmap { len: 11 },
            indices: (0..profile::W2_MAX_N).collect(),
        };
        // The longest `Have` is a `u16` list of `MAX_HAVE_INDICES`.
        let max_have = Bpv1Message::Have {
            block_id: zakura_chain::block::Hash([5u8; 32]),
            flags: HaveFlags::Relay,
            form: HaveForm::List,
            indices: (0..profile::W2_MAX_HAVE_INDICES as u32).collect(),
        };
        let want_end = Bpv1Message::WantEnd {
            want_id: 1,
            sent: 2,
            reason: WantEndReason::Served,
        };

        vec![
            min_header_meta,
            max_header_meta,
            want,
            stripe_done,
            block_done,
            min_part,
            max_part,
            min_have,
            list_have,
            w2_bitmap_have,
            max_have,
            want_end,
        ]
    }

    fn opaque_types() -> &'static [u16] {
        // A part payload is the rest of the frame, `wire_len(i)` bytes
        // (decision-18). The codec bounds it by `S_MAX`; `PeerState` checks the
        // exact length against the admitted coding (bpv1-66). A `Have`
        // bitmap is also the rest of the frame (decision-33): the codec
        // bounds it by `ceil(MAX_PARTS / 8)`, and `PeerState` checks the
        // exact length against the admitted coding (close-16).
        &[
            super::wire::message_type::PART,
            super::wire::message_type::HAVE,
        ]
    }

    fn opaque_head_len(&self) -> usize {
        match self {
            // `block_id || index || depth || siblings`, then the payload.
            Bpv1Message::Part { siblings, .. } => 32 + 4 + 1 + 32 * siblings.len(),
            // `block_id || flags`, then the bitmap. A bitmap sample's first
            // byte is nonzero, so every longer prefix still names an index.
            Bpv1Message::Have {
                form: HaveForm::Bitmap { .. },
                ..
            } => 32 + 1 + 1,
            // A list's count fixes its length.
            Bpv1Message::Have {
                form: HaveForm::List,
                ..
            } => usize::MAX,
            _ => 0,
        }
    }

    fn arbitrary_valid() -> BoxedStrategy<Self> {
        prop_oneof![
            Just(Bpv1Message::HeaderMeta {
                header: vec![0x11; 80],
                binding: CoinbaseBinding {
                    coinbase: vec![0x22; 64],
                    txid_siblings: vec![[0x33; 32]],
                    auth_siblings: vec![[0x44; 32]],
                    light_client_root: [0x55; 32],
                },
                coding: sample_coding(),
                part_root: [0x66; 32],
                proposer_node: node_id(1),
            }),
            (
                1u32..1000,
                any::<[u8; 32]>(),
                0u32..512,
                0u32..profile::W2_MAX_N
            )
                .prop_map(|(want_id, hash, stripe, bit)| {
                    let mut requested = WantBitmap::EMPTY;
                    requested.set(bit);
                    Bpv1Message::Want {
                        want_id,
                        block_id: zakura_chain::block::Hash(hash),
                        stripe,
                        requested,
                    }
                }),
            (any::<[u8; 32]>(), 0u32..512).prop_map(|(hash, stripe)| Bpv1Message::StripeDone {
                block_id: zakura_chain::block::Hash(hash),
                stripe,
            }),
            any::<[u8; 32]>().prop_map(|hash| Bpv1Message::BlockDone {
                block_id: zakura_chain::block::Hash(hash),
            }),
            (any::<[u8; 32]>(), 0u32..100, 0usize..=6).prop_map(|(hash, index, depth)| {
                Bpv1Message::Part {
                    block_id: zakura_chain::block::Hash(hash),
                    index,
                    siblings: vec![[0xAB; 32]; depth],
                    payload: vec![0xCD; profile::W2_S_MIN as usize],
                }
            }),
            (
                any::<[u8; 32]>(),
                1usize..=8,
                prop_oneof![
                    Just(HaveFlags::Plain),
                    Just(HaveFlags::Push),
                    Just(HaveFlags::Relay)
                ],
                prop::option::of(0u16..4),
            )
                .prop_map(|(hash, count, flags, bitmap_slack)| {
                    // `count <= 8`, so the cast keeps every value.
                    let indices: Vec<u32> = (0..count as u32).map(|i| i * 3).collect();
                    let covered = indices[count - 1] / 8 + 1;
                    Bpv1Message::Have {
                        block_id: zakura_chain::block::Hash(hash),
                        flags,
                        // `covered <= 3`, so the cast keeps every value.
                        form: bitmap_slack.map_or(HaveForm::List, |slack| HaveForm::Bitmap {
                            len: covered as u16 + slack,
                        }),
                        indices,
                    }
                }),
            (1u32..1000, 0u16..86, 0u8..=2).prop_map(|(want_id, sent, reason)| {
                Bpv1Message::WantEnd {
                    want_id,
                    sent,
                    reason: WantEndReason::from_u8(reason).unwrap(),
                }
            }),
        ]
        .boxed()
    }

    fn violations() -> Vec<Violation<Self>> {
        let want = || {
            encode_frame(&Bpv1Message::Want {
                want_id: 1,
                block_id: zakura_chain::block::Hash([1u8; 32]),
                stripe: 0,
                requested: sample_want_bitmap(),
            })
            .unwrap()
        };
        let relay_have = || {
            encode_frame(&Bpv1Message::Have {
                block_id: zakura_chain::block::Hash([2u8; 32]),
                flags: HaveFlags::Relay,
                form: HaveForm::List,
                indices: vec![1],
            })
            .unwrap()
        };
        let mut zero_want_id = want();
        zero_want_id.payload[0..4].copy_from_slice(&0u32.to_le_bytes());

        let mut empty_bitmap = want();
        let len = empty_bitmap.payload.len();
        empty_bitmap.payload[len - 11..].fill(0);

        let mut high_bit_bitmap = want();
        *high_bit_bitmap.payload.last_mut().unwrap() |= 0x80;

        let mut unsorted_have = encode_frame(&Bpv1Message::Have {
            block_id: zakura_chain::block::Hash([2u8; 32]),
            flags: HaveFlags::Plain,
            form: HaveForm::List,
            indices: vec![1, 2],
        })
        .unwrap();
        // Set the second `u16` index equal to the first (not strictly
        // increasing).
        unsorted_have.payload[37..39].copy_from_slice(&1u16.to_le_bytes());

        // decision-24, decision-33: bits 0, 1 and 2 are the only defined
        // `Have` flags.
        let mut reserved_have_flag = relay_have();
        reserved_have_flag.payload[32] |= 0x08;

        // `block_id || flags || body` as a raw `Have` frame.
        let have_frame = |flags: u8, body: &[u8]| {
            let mut payload = vec![2u8; 32];
            payload.push(flags);
            payload.extend_from_slice(body);
            Frame {
                message_type: super::wire::message_type::HAVE,
                flags: 0,
                payload,
            }
        };
        let empty_list = have_frame(0, &0u16.to_le_bytes());
        // The count check runs before the indices are read.
        let long_list = have_frame(0, &4_097u16.to_le_bytes());
        let short_list = have_frame(0, &[2, 0, 1, 0]);
        let empty_have_bitmap = have_frame(HAVE_FLAG_BITMAP, &[0; 11]);
        // 513 full bytes name 4,104 indices, past `MAX_HAVE_INDICES`.
        let full_bitmap = have_frame(HAVE_FLAG_BITMAP, &[0xFF; 513]);
        let long_bitmap = have_frame(HAVE_FLAG_BITMAP, &[0x01; HAVE_BITMAP_MAX_BYTES + 1]);

        // decision-24: relay is valid only beside push.
        let mut relay_without_push = relay_have();
        relay_without_push.payload[32] = 0x02;

        let mut bad_want_end_reason = encode_frame(&Bpv1Message::WantEnd {
            want_id: 1,
            sent: 0,
            reason: WantEndReason::Served,
        })
        .unwrap();
        *bad_want_end_reason.payload.last_mut().unwrap() = 3;

        vec![
            Violation {
                name: "want_id == 0",
                frame: zero_want_id,
                rejected_by: |e| matches!(e, WireError::OutOfRange(_)),
            },
            Violation {
                name: "empty want bitmap",
                frame: empty_bitmap,
                rejected_by: |e| matches!(e, WireError::OutOfRange(_)),
            },
            Violation {
                name: "want bitmap high bit >= MAX_N",
                frame: high_bit_bitmap,
                rejected_by: |e| matches!(e, WireError::OutOfRange(_)),
            },
            Violation {
                name: "have indices not strictly increasing",
                frame: unsorted_have,
                rejected_by: |e| matches!(e, WireError::OutOfRange(_)),
            },
            Violation {
                name: "unknown want_end reason",
                frame: bad_want_end_reason,
                rejected_by: |e| matches!(e, WireError::OutOfRange(_)),
            },
            Violation {
                name: "have flag bit other than push, relay, and bitmap",
                frame: reserved_have_flag,
                rejected_by: |e| matches!(e, WireError::ReservedFlags(0x0B)),
            },
            Violation {
                name: "have list with no index",
                frame: empty_list,
                rejected_by: |e| matches!(e, WireError::OutOfRange("have index count")),
            },
            Violation {
                name: "have list past MAX_HAVE_INDICES",
                frame: long_list,
                rejected_by: |e| matches!(e, WireError::OutOfRange("have index count")),
            },
            Violation {
                name: "have list shorter than its count",
                frame: short_list,
                rejected_by: |e| matches!(e, WireError::Truncated(_)),
            },
            Violation {
                name: "have bitmap with no index",
                frame: empty_have_bitmap,
                rejected_by: |e| matches!(e, WireError::OutOfRange("have index count")),
            },
            Violation {
                name: "have bitmap past MAX_HAVE_INDICES",
                frame: full_bitmap,
                rejected_by: |e| matches!(e, WireError::OutOfRange("have index count")),
            },
            Violation {
                name: "have bitmap past ceil(MAX_PARTS / 8)",
                frame: long_bitmap,
                rejected_by: |e| matches!(e, WireError::OutOfRange("have bitmap length")),
            },
            Violation {
                name: "have relay flag without push",
                frame: relay_without_push,
                rejected_by: |e| matches!(e, WireError::ReservedFlags(0x02)),
            },
        ]
    }
}

message_suite!(bpv1_wire_suite, Bpv1Message);

/// Fixed external golden test vectors covering W2 size discontinuities
/// (`0`, `1_000`, `262_144`, `262_145` bytes: the last body at `S_MIN` and the
/// first above it, decision-18), BLAKE3 KDF domain separation,
/// post-NU7 coinbase commitment script (`6a 23 BP 01 C`), and the exact
/// 51-byte `Want` wire frame (`verification.md` Level 1–3).
#[test]
fn golden_external_hex_vectors_for_w2_discontinuities_and_wire() {
    // 1. BLAKE3 KDF with context "Zakura BPv1 test" over b"abc".
    let kdf = Bpv1Hash::new("test").hash(&[b"abc"]);
    assert_eq!(
        kdf,
        *blake3::Hasher::new_derive_key("Zakura BPv1 test")
            .update(b"abc")
            .finalize()
            .as_bytes(),
        "decision-25: hash contexts are \"Zakura BPv1 \" + name"
    );
    let kdf_hex = hex::encode(kdf);
    assert_eq!(
        kdf_hex, "442e548781a8590f5162ea5ac226d3b62c7b50360e27c37823cb3ae48834af5a",
        "frozen golden BLAKE3 KDF vector"
    );

    // 2. W2 part roots at the size discontinuities (0, 1_000, 262_144, 262_145 bytes).
    let roots: Vec<String> = [0usize, 1_000, 262_144, 262_145]
        .into_iter()
        .map(|len| {
            let body: Vec<u8> = (0..len)
                .map(|i| ((i.wrapping_mul(37) ^ (i >> 7)) & 0xff) as u8)
                .collect();
            hex::encode(
                encode_body(&Profile::W2, &body, 2)
                    .expect("W2 encode")
                    .part_root,
            )
        })
        .collect();
    assert_eq!(
        roots,
        vec![
            "867a48fed38c1fd10347ce78052b9470e8e638f991ed4afd758048cb565f7e41",
            "f7973c8ae9454d059d1e03c0acd2a892900e3687446d917758cbcf9e26404ac9",
            "7444bbf749682d991c2f4c3ed43ebaa4fb96fbbb2507b5730bb12f2a0b79683c",
            "8a9f13f478a32e7af28ffc3b81bb2466e6ba2cfbc1a32813ae868c79f75b88af",
        ],
        "frozen golden W2 part_root vectors at 0, 1000, 262144, and 262145 bytes"
    );

    // 3. Frozen 37-byte `OP_RETURN BP 01 C` script, 35-byte payload, 46-byte
    //    output, and 51-byte `Want` frame hex.
    let commit = commit_hash(&CommitmentFields {
        chain_id: [0x11; 32],
        profile_id: W2_PROFILE_ID,
        coding: Profile::W2.derive_coding(1_000, 2).unwrap(),
        part_root: [0x22; 32],
        // Raw bytes, so the vector pins BPv1's hash and not the transport's key derivation.
        proposer_node: NodeId::from_bytes(&[0x33; 32]),
    });
    assert_eq!(
        hex::encode(commit),
        "ab516914bd83aa8fe57e6a301ae72069af0ffb1776573615c3c1602b79db2888",
        "frozen golden commitment hash C"
    );
    let script = commitment_script(commit);
    assert_eq!(
        hex::encode(script),
        "6a23425001ab516914bd83aa8fe57e6a301ae72069af0ffb1776573615c3c1602b79db2888",
        "frozen golden commitment script"
    );
    assert_eq!(commitment_payload(commit), script[2..]);
    // A zero value, the script length, then the script.
    let output = Output::new(Amount::zero(), Script::new(&script))
        .zcash_serialize_to_vec()
        .expect("an output always serializes");
    assert_eq!(output.len(), 46);
    assert_eq!(output, [&[0u8; 8][..], &[0x25], &script].concat());

    let mut bm = WantBitmap::EMPTY;
    bm.set(0);
    bm.set(7);
    let want = Bpv1Message::Want {
        want_id: 1,
        block_id: zakura_chain::block::Hash([0xAA; 32]),
        stripe: 0,
        requested: bm,
    };
    let encoded_want = encode_frame(&want).expect("encode Want");
    assert_eq!(encoded_want.message_type, super::wire::message_type::WANT);
    assert_eq!(encoded_want.payload.len(), 51);
    assert_eq!(
        hex::encode(&encoded_want.payload),
        "01000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa000000008100000000000000000000"
    );

    // 4. Frozen `Have` frames in both forms (decision-33): a plain list of
    // indices 1 and 300, then a push and relay bitmap of 0, 9 and 85 at 86
    // parts.
    let list_have = encode_frame(&Bpv1Message::Have {
        block_id: zakura_chain::block::Hash([0xAA; 32]),
        flags: HaveFlags::Plain,
        form: HaveForm::List,
        indices: vec![1, 300],
    })
    .expect("encode list Have");
    assert_eq!(
        hex::encode(&list_have.payload),
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa00020001002c01"
    );
    let bitmap_have = encode_frame(&Bpv1Message::Have {
        block_id: zakura_chain::block::Hash([0xAA; 32]),
        flags: HaveFlags::Relay,
        form: HaveForm::Bitmap { len: 11 },
        indices: vec![0, 9, 85],
    })
    .expect("encode bitmap Have");
    assert_eq!(
        hex::encode(&bitmap_have.payload),
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa070102000000000000000020"
    );
}

/// decision-33: both `Have` forms round trip, through the message codec and
/// through the data queue's borrowed encoder, to the same frame.
#[test]
fn decision33_have_round_trips_in_both_forms() {
    let cases = [
        (HaveForm::List, vec![0]),
        (HaveForm::List, vec![3, 40_000, 44_031]),
        (HaveForm::List, (0..4_096).collect()),
        (HaveForm::Bitmap { len: 1 }, vec![7]),
        (HaveForm::Bitmap { len: 11 }, (0..86).collect()),
        (HaveForm::Bitmap { len: 11 }, vec![85]),
        (
            HaveForm::Bitmap {
                len: HAVE_BITMAP_MAX_BYTES as u16,
            },
            vec![0, 44_031],
        ),
    ];
    for (form, indices) in cases {
        for flags in [HaveFlags::Plain, HaveFlags::Push, HaveFlags::Relay] {
            let message = Bpv1Message::Have {
                block_id: zakura_chain::block::Hash([0x3C; 32]),
                flags,
                form,
                indices: indices.clone(),
            };
            let frame = encode_frame(&message).expect("a valid Have encodes");
            assert_eq!(
                decode_frame::<Bpv1Message>(&frame).expect("its frame decodes"),
                message
            );
            let queued = DataQueueItem::Have {
                block_id: zakura_chain::block::Hash([0x3C; 32]),
                flags,
                form,
                indices: indices.clone(),
            };
            assert_eq!(queued.encode(), Some(frame), "{form:?} {flags:?}");
        }
    }
    // The encoder refuses what the decoder would refuse.
    for (form, indices) in [
        (HaveForm::List, vec![]),
        (HaveForm::List, vec![2, 2]),
        (HaveForm::List, vec![65_536]),
        (HaveForm::Bitmap { len: 1 }, vec![8]),
        (HaveForm::Bitmap { len: 0 }, vec![0]),
        (
            HaveForm::Bitmap {
                len: HAVE_BITMAP_MAX_BYTES as u16 + 1,
            },
            vec![0],
        ),
    ] {
        assert!(
            encode_have_frame(
                zakura_chain::block::Hash([0; 32]),
                HaveFlags::Plain,
                form,
                &indices
            )
            .is_err(),
            "{form:?} {indices:?}"
        );
    }
}

/// decision-33: the encoder picks the smaller form, and a tie keeps the
/// list. At 86 parts the bitmap takes 11 bytes, so four indices (10 bytes)
/// stay a list and five (12 bytes) become a bitmap.
#[test]
fn decision33_encoder_picks_the_smaller_form_at_the_crossover() {
    assert_eq!(HaveForm::smaller(86, 4), HaveForm::List);
    assert_eq!(HaveForm::smaller(86, 5), HaveForm::Bitmap { len: 11 });
    // At 80 parts four indices tie the 10-byte bitmap.
    assert_eq!(HaveForm::smaller(80, 4), HaveForm::List);
    assert_eq!(HaveForm::smaller(80, 5), HaveForm::Bitmap { len: 10 });
    // An empty layout has no bitmap.
    assert_eq!(HaveForm::smaller(0, 1), HaveForm::List);
    let max_parts = profile::W2_MAX_STRIPES * profile::W2_MAX_N;
    assert_eq!(
        HaveForm::smaller(max_parts, 4_096),
        HaveForm::Bitmap {
            len: HAVE_BITMAP_MAX_BYTES as u16
        }
    );
    for total in [1, 2, 8, 9, 86, 172, 1_000, max_parts] {
        for count in 1..=profile::W2_MAX_HAVE_INDICES {
            let form = HaveForm::smaller(total, count);
            let list = HaveForm::List.indices_len(count);
            let bitmap = total.div_ceil(8) as usize;
            assert_eq!(form.indices_len(count), list.min(bitmap), "{total} {count}");
            assert!(form.fits(total));
        }
    }
}

/// decision-33 against #2745's table: one index takes a 4-byte list, and a
/// 2 MB block's 86 indices take an 11-byte bitmap, where the `u32` list took
/// 6 and 346 bytes.
#[test]
fn decision33_have_sizes_match_the_issue_table() {
    let have_len = |total: u32, indices: Vec<u32>| {
        let form = HaveForm::smaller(total, indices.len());
        encode_have_frame(
            zakura_chain::block::Hash([0; 32]),
            HaveFlags::Plain,
            form,
            &indices,
        )
        .expect("a valid Have encodes")
        .payload
        .len()
    };
    // `block_id` and the flags byte precede the indices.
    let header = 32 + 1;
    assert_eq!(have_len(86, vec![17]), header + 4);
    assert_eq!(have_len(86, (0..86).collect()), header + 11);
    // A full proposer seed list at 86 parts.
    assert_eq!(have_len(86, (0..86).step_by(2).collect()), header + 11);
}
