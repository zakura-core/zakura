//! `SeededInputs`: replay, independence between sources, the X25519 group,
//! the CID generator and the TLS slot pool.

use quinn_proto::crypto::{HandshakeTokenKey, HmacKey};
use ring::agreement;

use super::*;

const SEED: [u8; 32] = [7; 32];

/// One sample of `source`, taken through the input it feeds.
fn sample(inputs: &SeededInputs, source: Source) -> Vec<u8> {
    let streams = &inputs.streams;
    match source {
        Source::RngSeed => inputs.rng_seed.to_vec(),
        Source::ResetKey => {
            let mut tag = vec![0; inputs.reset_key.signature_len()];
            HmacKey::sign(&inputs.reset_key, b"probe", &mut tag);
            tag
        }
        Source::Cid => {
            let mut generator = SeededCidGenerator::new(streams.clone());
            [
                generator.generate_cid().to_vec(),
                generator.generate_cid().to_vec(),
            ]
            .concat()
        }
        Source::InitialDstCid => streams
            .draw_array::<INITIAL_DST_CID_LEN>(Source::InitialDstCid)
            .to_vec(),
        Source::TokenKey => {
            let mut sealed = b"probe".to_vec();
            inputs
                .token_key
                .aead_from_hkdf(b"retry")
                .seal(&mut sealed, b"")
                .unwrap();
            sealed
        }
        Source::TlsRandom => {
            let mut random = vec![0; 48];
            streams.slot.fill(&mut random).unwrap();
            random
        }
        Source::KeyExchange => streams.slot.start().unwrap().pub_key().to_vec(),
    }
}

/// One sample of every source, in [`Source::ALL`] order.
fn transcript(inputs: &SeededInputs) -> Vec<Vec<u8>> {
    Source::ALL
        .into_iter()
        .map(|source| sample(inputs, source))
        .collect()
}

#[test]
fn the_same_seed_and_node_give_the_same_inputs() {
    let first = SeededInputs::new(SEED, 3);
    let second = SeededInputs::new(SEED, 3);
    assert_eq!(transcript(&first), transcript(&second));
    assert_eq!(first.record(), second.record());
    assert_eq!(
        SeededInputs::node_key(SEED, 3).public(),
        SeededInputs::node_key(SEED, 3).public()
    );
}

#[test]
fn another_node_or_seed_changes_every_source() {
    let base = transcript(&SeededInputs::new(SEED, 3));
    for other in [SeededInputs::new(SEED, 4), SeededInputs::new([8; 32], 3)] {
        let other_transcript = transcript(&other);
        for (index, source) in Source::ALL.into_iter().enumerate() {
            assert_ne!(base[index], other_transcript[index], "{source:?}");
        }
    }
    let key = SeededInputs::node_key(SEED, 3).public();
    assert_ne!(key, SeededInputs::node_key(SEED, 4).public());
    assert_ne!(key, SeededInputs::node_key([8; 32], 3).public());
}

#[test]
fn extra_draws_on_one_source_leave_the_others_alone() {
    let base = transcript(&SeededInputs::new(SEED, 3));
    for source in Source::ALL {
        let inputs = SeededInputs::new(SEED, 3);
        for _ in 0..3 {
            sample(&inputs, source);
        }
        let shifted = transcript(&inputs);
        for (index, other) in Source::ALL.into_iter().enumerate() {
            if other != source {
                assert_eq!(base[index], shifted[index], "{source:?} shifted {other:?}");
            }
        }
    }
}

#[test]
fn the_record_counts_each_sources_draws() {
    let inputs = SeededInputs::new(SEED, 9);
    let record = inputs.record();
    assert_eq!((record.seed, record.node), (SEED, 9));
    let drawn_at_start = [Source::RngSeed, Source::ResetKey, Source::TokenKey];
    for source in Source::ALL {
        let expected = u64::from(drawn_at_start.contains(&source));
        assert_eq!(record.draws[&source], expected, "{source:?}");
    }

    // A generator draws its key, then one nonce per ID. A 48-byte fill is
    // one draw of two blocks.
    sample(&inputs, Source::Cid);
    sample(&inputs, Source::TlsRandom);
    sample(&inputs, Source::KeyExchange);
    let draws = inputs.record().draws;
    assert_eq!(draws[&Source::Cid], 3);
    assert_eq!(draws[&Source::TlsRandom], 1);
    assert_eq!(draws[&Source::KeyExchange], 1);
    assert_eq!(draws[&Source::InitialDstCid], 0);
}

#[test]
fn the_seeded_group_agrees_with_rings_x25519() {
    let inputs = SeededInputs::new(SEED, 1);
    let ours = inputs.streams.slot.start().unwrap();
    let ring_key = agreement::EphemeralPrivateKey::generate(
        &agreement::X25519,
        &ring::rand::SystemRandom::new(),
    )
    .unwrap();
    let ring_public = ring_key.compute_public_key().unwrap();
    let theirs = agreement::agree_ephemeral(
        ring_key,
        &agreement::UnparsedPublicKey::new(&agreement::X25519, ours.pub_key()),
        |secret| secret.to_vec(),
    )
    .unwrap();
    let shared = ours.complete(ring_public.as_ref()).unwrap();
    assert_eq!(shared.secret_bytes(), &theirs[..]);
    assert_eq!(inputs.streams.slot.name(), NamedGroup::X25519);
}

#[test]
fn the_seeded_group_refuses_bad_key_shares() {
    let inputs = SeededInputs::new(SEED, 1);
    let slot = inputs.streams.slot;
    assert!(slot.start().unwrap().complete(&[9; 31]).is_err());
    // The identity point gives the all-zero secret.
    assert!(slot.start().unwrap().complete(&[0; 32]).is_err());
}

#[test]
fn seeded_cids_have_the_hashed_generator_shape() {
    let inputs = SeededInputs::new(SEED, 1);
    let mut generator = SeededCidGenerator::new(inputs.streams.clone());
    assert_eq!(generator.cid_len(), 8);
    assert_eq!(generator.cid_lifetime(), None);
    let cid = generator.generate_cid();
    assert_eq!(cid.len(), 8);
    generator.validate(&cid).unwrap();

    let mut forged = cid.to_vec();
    forged[0] ^= 1;
    assert!(generator.validate(&ConnectionId::new(&forged)).is_err());
    assert!(generator.validate(&ConnectionId::new(&cid[..7])).is_err());
    // Another generator has another key.
    let other = SeededCidGenerator::new(inputs.streams.clone());
    assert!(other.validate(&cid).is_err());
}

#[test]
fn tls_slots_are_reused_once_free() {
    static POOL: SlotPool = SlotPool::new();

    let first = SeededInputs::with_pool(SEED, 0, &POOL);
    let slot = first.streams.slot;
    let inputs = first.endpoint_inputs();
    drop(first);
    // The endpoint inputs still hold the slot.
    let second = SeededInputs::with_pool(SEED, 1, &POOL);
    assert!(!std::ptr::eq(second.streams.slot, slot));
    drop((inputs, second));

    // Sequential runs reuse the free slots, and each new holder starts at
    // block 0.
    for node in 0..50 {
        let inputs = SeededInputs::with_pool(SEED, node, &POOL);
        assert_eq!(
            sample(&inputs, Source::TlsRandom),
            sample(&SeededInputs::new(SEED, node), Source::TlsRandom)
        );
    }
    assert_eq!(POOL.leaked(), 2);

    // Nodes alive at once each hold their own.
    let nodes: Vec<_> = (0..3)
        .map(|node| SeededInputs::with_pool(SEED, node, &POOL))
        .collect();
    assert_eq!(POOL.leaked(), 3);
    drop(nodes);
    let _again = SeededInputs::with_pool(SEED, 0, &POOL);
    assert_eq!(POOL.leaked(), 3);
}

#[test]
fn the_endpoint_inputs_set_every_source() {
    let inputs = SeededInputs::new(SEED, 2).endpoint_inputs();
    assert_eq!(
        format!("{inputs:?}"),
        "EndpointInputs { rng_seed: true, reset_key: true, cid_generator: true, \
         initial_dst_cid: true, token_key: true, tls_random: true, kx_groups: Some(1) }"
    );
}

/// Endpoint inputs from `node` with both CID callbacks replaced, so only
/// the TLS inputs still come from the seed.
fn tls_only_inputs(node: &SeededInputs) -> EndpointInputs {
    node.endpoint_inputs()
        .cid_generator(Arc::new(|| {
            Box::new(quinn_proto::HashedConnectionIdGenerator::from_key(1))
                as Box<dyn ConnectionIdGenerator>
        }))
        .initial_dst_cid(Arc::new(|| ConnectionId::new(&[1; 8])))
}

/// node 0's first TLS random draw under `SEED`.
fn first_tls_random() -> Vec<u8> {
    sample(&SeededInputs::new(SEED, 0), Source::TlsRandom)
}

#[test]
fn a_live_tls_config_keeps_its_slot_after_the_inputs_drop() {
    static POOL: SlotPool = SlotPool::new();

    let node = SeededInputs::with_pool(SEED, 0, &POOL);
    let slot = node.streams.slot;
    let inputs = tls_only_inputs(&node);
    let tls =
        crate::tls::TlsConfig::with_inputs(&SeededInputs::node_key(SEED, 0), &inputs).unwrap();
    drop((node, inputs));

    // The TLS config still draws through the slot, from node 0's stream.
    let mut random = vec![0; 48];
    slot.fill(&mut random)
        .expect("a live TLS user keeps the lease");
    assert_eq!(random, first_tls_random());
    assert!(slot.start().is_ok());
    drop(tls);
}

#[test]
fn a_slot_with_a_live_tls_config_isnt_reused() {
    static POOL: SlotPool = SlotPool::new();

    let node = SeededInputs::with_pool(SEED, 0, &POOL);
    let slot = node.streams.slot;
    let inputs = tls_only_inputs(&node);
    let tls =
        crate::tls::TlsConfig::with_inputs(&SeededInputs::node_key(SEED, 0), &inputs).unwrap();
    drop((node, inputs));

    let other = SeededInputs::with_pool(SEED, 1, &POOL);
    assert!(
        !std::ptr::eq(other.streams.slot, slot),
        "node 1 took a used slot"
    );
    let mut random = vec![0; 48];
    slot.fill(&mut random).unwrap();
    assert_eq!(random, first_tls_random(), "node 0 read another stream");

    // Once the last TLS user is gone, the slot is free again.
    drop(tls);
    let _third = SeededInputs::with_pool(SEED, 2, &POOL);
    assert_eq!(POOL.leaked(), 2);
}
