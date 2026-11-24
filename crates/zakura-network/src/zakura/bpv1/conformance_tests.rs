//! Level-8 conformance suite over real BPv1 QUIC streams.

use std::{num::NonZeroU64, sync::Arc};

use zakura_chain::{
    block::{self, Height},
    parameters::Network,
};
use zakura_node_services::block_memory::DecodedBlockPool;
use zakura_quic::NodeSecretKey;
use zakura_rt::sync::CancellationToken;

use super::{
    commitment::CoinbaseBinding,
    ports::MemoryBodyStore,
    profile::{Profile, W2_S_MAX},
    service::Bpv1Ports,
    test_support::{
        build_large_v6_tx, connect, node_id, regtest_nu7, solved_block_of, start,
        AcceptingHeaderAdmission,
    },
    wire::{
        message_type, Bpv1Message, HaveFlags, WantBitmap, WantEndReason, BPV1_STREAMS,
        ZAKURA_CAP_BPV1,
    },
};
use crate::{
    zakura::{
        testkit::{
            await_until,
            stream_conformance::{
                stream_conformance_suite, RawLayoutPeer, StreamConformance, SubscriptionUpdate,
            },
            LocalEndpointFactory, TEST_NET_TIMEOUT,
        },
        wire_codec::{decode_frame, encode_frame},
        MessageRule, Service, ServiceRegistry, ZakuraHandshakeConfig, ZakuraLocalLimits,
        ZakuraProtocolHandler, ZakuraSupervisorHandle,
    },
    BoxError, Config,
};

#[derive(Debug)]
struct Bpv1Conformance;

fn block_for_exchange(exchange: u32) -> block::Hash {
    let mut hash = [0u8; 32];
    hash[..4].copy_from_slice(&exchange.to_le_bytes());
    block::Hash(hash)
}

fn exchange_of_block(hash: &block::Hash) -> u32 {
    u32::from_le_bytes([hash.0[0], hash.0[1], hash.0[2], hash.0[3]])
}

impl StreamConformance for Bpv1Conformance {
    type Message = Bpv1Message;

    fn message(row: &MessageRule, exchange: u32) -> Bpv1Message {
        match row.message_type {
            message_type::HEADER_META => Bpv1Message::HeaderMeta {
                header: vec![0xAA; 140],
                binding: CoinbaseBinding {
                    coinbase: vec![0xBB; 64],
                    txid_siblings: Vec::new(),
                    auth_siblings: Vec::new(),
                    light_client_root: [0xCC; 32],
                },
                coding: Profile::W2.derive_coding(64, 2).expect("valid W2 coding"),
                part_root: [0x11; 32],
                proposer_node: NodeSecretKey::from_bytes(&[7; 32]).public(),
            },
            message_type::WANT => {
                let mut requested = WantBitmap::EMPTY;
                requested.set(0);
                Bpv1Message::Want {
                    want_id: exchange.wrapping_add(1),
                    block_id: block_for_exchange(exchange),
                    stripe: 0,
                    requested,
                }
            }
            message_type::STRIPE_DONE => Bpv1Message::StripeDone {
                block_id: block_for_exchange(exchange),
                stripe: 0,
            },
            message_type::BLOCK_DONE => Bpv1Message::BlockDone {
                block_id: block_for_exchange(exchange),
            },
            message_type::PART => Bpv1Message::Part {
                block_id: block_for_exchange(exchange),
                index: 0,
                siblings: Vec::new(),
                payload: vec![0x42; W2_S_MAX as usize],
            },
            message_type::HAVE => Bpv1Message::Have {
                block_id: block_for_exchange(exchange),
                flags: HaveFlags::Plain,
                indices: vec![0],
            },
            message_type::WANT_END => Bpv1Message::WantEnd {
                want_id: exchange.wrapping_add(1),
                sent: 1,
                reason: WantEndReason::Served,
            },
            other => unreachable!("bpv1 declares seven message kinds, got {other}"),
        }
    }

    fn exchange(message: &Bpv1Message) -> u32 {
        match message {
            Bpv1Message::Want { want_id, .. } | Bpv1Message::WantEnd { want_id, .. } => {
                want_id.wrapping_sub(1)
            }
            Bpv1Message::Part { block_id, .. }
            | Bpv1Message::Have { block_id, .. }
            | Bpv1Message::StripeDone { block_id, .. }
            | Bpv1Message::BlockDone { block_id } => exchange_of_block(block_id),
            Bpv1Message::HeaderMeta { .. } => 0,
        }
    }

    fn update(_: &MessageRule, _: SubscriptionUpdate) -> Bpv1Message {
        unreachable!("bpv1 uses seeded pull (decision-13) and declares no subscription rows")
    }

    fn read_update(_: &Bpv1Message) -> Option<SubscriptionUpdate> {
        None
    }

    fn page(_: &MessageRule, _: u32, _: u32) -> Bpv1Message {
        unreachable!("bpv1 declares no subscription rows")
    }

    fn read_page(_: &Bpv1Message) -> Option<(u32, u32)> {
        None
    }
}

stream_conformance_suite!(bpv1_streams, BPV1_STREAMS, Bpv1Conformance);

/// The BPv1 control and data members, in `BPV1_STREAMS` order.
const CONTROL: usize = 0;
const DATA: usize = 1;

/// A raw peer's receive window for each stream: four W2 parts.
const RAW_STREAM_WINDOW: u32 = 4 * 65_536;

/// cov-3 (`bpv1-54`, `bpv1-55`, acceptance criterion 4, level-8): over real
/// QUIC, a peer stops reading BPv1 data while a proposer serves it a 2 MB
/// `Want`, far beyond its stream window. The proposer's control messages
/// still reach it, it still reads the peer's next `Want`, and once the peer
/// reads data again each `WantEnd` arrives behind exactly the parts it ends.
#[tokio::test]
async fn bpv1_54_55_control_progresses_and_ends_follow_parts_while_data_is_paused(
) -> Result<(), BoxError> {
    let limits = ZakuraLocalLimits::from_config(&Config::default());
    let shutdown = CancellationToken::new();
    let ports = || Bpv1Ports {
        header_admission: Arc::new(AcceptingHeaderAdmission(20)),
        body_store: Arc::new(MemoryBodyStore::new(DecodedBlockPool::for_block_count(
            NonZeroU64::new(4).unwrap(),
        ))),
        ..Default::default()
    };

    // The proposer serves BPv1 over real QUIC.
    let endpoint = LocalEndpointFactory::with_limits(&limits)
        .endpoint(901)
        .await?;
    let id_v = endpoint.local_id();
    let (svc_v, handle_v) = start(Profile::W2, regtest_nu7(), id_v, ports(), &shutdown);
    let handler = ZakuraProtocolHandler::new_with_registry(
        ZakuraSupervisorHandle::new(16),
        Network::Mainnet,
        ZakuraHandshakeConfig::for_network(&Network::Mainnet),
        limits.clone(),
        Arc::new(ServiceRegistry::new(vec![
            svc_v.clone() as Arc<dyn Service>
        ])?),
    )
    .with_local_node_id(id_v);
    endpoint.serve(handler)?;

    // An in-memory neighbour shares the seeds, so the raw peer pulls most
    // of the parts it gets.
    let id_b = node_id(83);
    let (svc_b, _handle_b) = start(Profile::W2, regtest_nu7(), id_b, ports(), &shutdown);
    connect(&svc_v, id_v, &svc_b, id_b, &shutdown);
    let height = Height(20);
    let big = solved_block_of(
        &handle_v,
        height,
        (0..32u8)
            .map(|i| build_large_v6_tx(height, i + 1, 61_000))
            .collect(),
    );
    let small = solved_block_of(&handle_v, height, vec![build_large_v6_tx(height, 99, 100)]);

    let mut raw_limits = limits.clone();
    raw_limits.quic.stream_receive_window_bytes = RAW_STREAM_WINDOW;
    let mut raw = RawLayoutPeer::connect(
        LocalEndpointFactory::node_addr(&endpoint).await,
        &raw_limits,
        902,
        ZAKURA_CAP_BPV1,
    )
    .await?;
    raw.open_layout(&BPV1_STREAMS).await?;
    await_until("both sessions", TEST_NET_TIMEOUT, || {
        handle_v.live_sessions() == 2
    })
    .await?;

    // The raw peer pulls all of the big block's parts and reads no data.
    assert!(handle_v.publish_solved_block(&big, height.0));
    let big_k = header_meta_coding_k(&mut raw, big.hash()).await?;
    assert!(
        u64::from(big_k) * 65_536 > 4 * u64::from(RAW_STREAM_WINDOW),
        "the parts of one Want overflow the paused stream window"
    );
    send_want(&mut raw, 1, big.hash(), 0..big_k).await?;

    // bpv1-54: with data paused, the next block's metadata still arrives on
    // control, and the proposer still reads the next `Want`.
    assert!(handle_v.publish_solved_block(&small, height.0));
    let small_k = header_meta_coding_k(&mut raw, small.hash()).await?;
    send_want(&mut raw, 2, small.hash(), 0..small_k).await?;

    // bpv1-55: reading data again, each end follows exactly its parts. The
    // proposer flags the raw peer's seeds in `Have`s ahead of them
    // (decision-19) and serves a `Want` only the parts it has not seeded
    // (bpv1-17).
    let mut seeds: [Vec<u32>; 2] = [Vec::new(), Vec::new()];
    let mut parts = [(big.hash(), 0u16), (small.hash(), 0u16)];
    let mut seeded = [0u32; 2];
    let mut ended = [None, None];
    while ended.iter().any(Option::is_none)
        || seeded[0] < u32::try_from(seeds[0].len())?
        || seeded[1] < u32::try_from(seeds[1].len())?
    {
        match decode_frame::<Bpv1Message>(&raw.recv(DATA).await?)? {
            Bpv1Message::Part {
                block_id, index, ..
            } => {
                let (i, _) = parts
                    .iter()
                    .enumerate()
                    .find(|(_, (hash, _))| *hash == block_id)
                    .ok_or("a part of an unknown block")?;
                if seeds[i].contains(&index) {
                    seeded[i] += 1;
                    continue;
                }
                assert!(ended[i].is_none(), "a part arrived after its WantEnd");
                parts[i].1 += 1;
            }
            Bpv1Message::WantEnd { want_id, sent, .. } => {
                let i = usize::try_from(want_id - 1)?;
                assert_eq!(sent, parts[i].1, "WantEnd {want_id} follows its parts");
                ended[i] = Some(sent);
            }
            Bpv1Message::Have {
                block_id,
                flags,
                indices,
            } => {
                assert!(
                    flags.push(),
                    "the proposer sends no plain Have (decision-20)"
                );
                let (i, _) = parts
                    .iter()
                    .enumerate()
                    .find(|(_, (hash, _))| *hash == block_id)
                    .ok_or("a Have of an unknown block")?;
                seeds[i].extend(indices);
            }
            other => return Err(format!("unexpected data frame {other:?}").into()),
        }
    }
    let in_want = |list: &[u32], k: u32| -> u32 {
        // A stripe-0 slot below `k` is a wanted index; the count fits u32.
        list.iter().filter(|&&i| i < k).count() as u32
    };
    assert_eq!(
        ended.map(|sent| sent.map(u32::from)),
        [
            Some(big_k - in_want(&seeds[0], big_k)),
            Some(small_k - in_want(&seeds[1], small_k)),
        ],
        "every wanted part arrived once"
    );
    assert!(!raw.is_closed(), "the conformant peer stays connected");

    raw.shutdown().await;
    shutdown.cancel();
    Ok(())
}

/// Reads control until `block_id`'s `HeaderMeta` and returns its `k`.
async fn header_meta_coding_k(
    raw: &mut RawLayoutPeer,
    block_id: block::Hash,
) -> Result<u32, BoxError> {
    loop {
        if let Bpv1Message::HeaderMeta { header, coding, .. } =
            decode_frame::<Bpv1Message>(&raw.recv(CONTROL).await?)?
        {
            let header: block::Header =
                zakura_chain::serialization::ZcashDeserialize::zcash_deserialize(&header[..])?;
            if header.hash() == block_id {
                return Ok(coding.k);
            }
        }
    }
}

/// Sends `Want(want_id)` for `slots` of `block_id`'s first stripe.
async fn send_want(
    raw: &mut RawLayoutPeer,
    want_id: u32,
    block_id: block::Hash,
    slots: std::ops::Range<u32>,
) -> Result<(), BoxError> {
    let mut requested = WantBitmap::EMPTY;
    for slot in slots {
        requested.set(slot);
    }
    let want = Bpv1Message::Want {
        want_id,
        block_id,
        stripe: 0,
        requested,
    };
    raw.send(CONTROL, &encode_frame(&want)?).await
}
