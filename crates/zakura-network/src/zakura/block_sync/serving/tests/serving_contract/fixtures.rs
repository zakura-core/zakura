//! Supply stored block bytes and observe each actual blocking read.
//!
//! Every read decodes fresh objects. Weak references let the tests observe when
//! those objects are released without keeping them alive themselves. The probe
//! can hold jobs open so admission and reconnect checks do not depend on timing.

use super::super::super::super::tests::fake_blocks_in_range;
use super::*;
use std::{
    collections::BTreeMap,
    sync::{atomic::AtomicU64, Weak},
};

#[derive(Debug)]
pub(super) struct ControlledSource {
    pub(super) encoded: Arc<BTreeMap<block::Height, Vec<u8>>>,
    pub(super) probe: Arc<ExecutionProbe>,
    pub(super) decoded: Arc<Mutex<Vec<Weak<block::Block>>>>,
    pub(super) peak_decoded: Arc<AtomicU64>,
    pub(super) fail: bool,
}

impl ControlledSource {
    pub(super) fn new(start: u32, count: u32, large: bool, blocked: bool) -> Arc<Self> {
        let mut bodies = fake_blocks_in_range(start, start + count - 1);
        if large {
            for body in &mut bodies {
                let body = Arc::make_mut(body);
                let tx = Arc::make_mut(&mut body.transactions[0]);
                let outputs = match tx {
                    zakura_chain::transaction::Transaction::V1 { outputs, .. }
                    | zakura_chain::transaction::Transaction::V2 { outputs, .. }
                    | zakura_chain::transaction::Transaction::V3 { outputs, .. }
                    | zakura_chain::transaction::Transaction::V4 { outputs, .. }
                    | zakura_chain::transaction::Transaction::V5 { outputs, .. }
                    | zakura_chain::transaction::Transaction::V6 { outputs, .. } => outputs,
                };
                outputs[0].lock_script =
                    zakura_chain::transparent::Script::new(&vec![0; 1_900_000]);
                Arc::make_mut(&mut body.header).merkle_root = body.transactions.iter().collect();
            }
        }
        Arc::new(Self {
            encoded: Arc::new(
                bodies
                    .into_iter()
                    .map(|body| {
                        (
                            body.coinbase_height().unwrap(),
                            body.zcash_serialize_to_vec().unwrap(),
                        )
                    })
                    .collect(),
            ),
            probe: ExecutionProbe::new(blocked, false),
            decoded: Arc::new(Mutex::new(Vec::new())),
            peak_decoded: Arc::new(AtomicU64::new(0)),
            fail: false,
        })
    }

    pub(super) fn live_decoded_bytes(&self) -> u64 {
        self.decoded
            .lock()
            .unwrap()
            .iter()
            .filter_map(Weak::upgrade)
            .map(|body| body.attributed_memory_size_bytes())
            .sum()
    }

    pub(super) fn fixture(self: &Arc<Self>, workers: usize, depth: usize, peer: u8) -> Fixture {
        let config = config(workers);
        let regulator = GetBlocksServingRegulator::new(config.clone());
        self.shared_fixture(
            depth,
            peer,
            config,
            regulator,
            Arc::new(PeerRegistry::new()),
        )
    }

    pub(super) fn shared_fixture(
        self: &Arc<Self>,
        depth: usize,
        peer: u8,
        config: ZakuraBlockSyncConfig,
        regulator: GetBlocksServingRegulator,
        registry: Arc<PeerRegistry>,
    ) -> Fixture {
        let f = Fixture::with_resources(self.clone(), depth, config, regulator, registry, peer);
        f.status.send_modify(|status| {
            status.servable_low = *self.encoded.first_key_value().unwrap().0;
            status.servable_high = *self.encoded.last_key_value().unwrap().0;
        });
        f.session.mark_status_received();
        f
    }
}

impl BlockRangeSource for ControlledSource {
    fn read_range(
        &self,
        request: BlockRangeRead,
    ) -> BoxFuture<'static, Result<BlockRangeReadResult, crate::BoxError>> {
        let encoded = self.encoded.clone();
        let probe = self.probe.clone();
        let fail = self.fail;
        let decoded = self.decoded.clone();
        let peak_decoded = self.peak_decoded.clone();
        // The source stores bytes like a database. Each returned body is decoded
        // into a distinct allocation rather than cloning a cached Arc fixture.
        let job = Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let (start, count, cap, lease) = request.into_parts();
                assert!(lease.try_start());
                let operation = probe.start();
                if fail {
                    return Err(io::Error::other("controlled storage failure").into());
                }
                let (blocks, allocations) = measure(|| {
                    let mut blocks = Vec::new();
                    let mut bytes = 0usize;
                    for offset in 0..count {
                        if lease.is_cancelled() {
                            break;
                        }
                        let Some(height) = start.0.checked_add(offset).map(block::Height) else {
                            break;
                        };
                        let Some(encoded) = encoded.get(&height) else {
                            break;
                        };
                        // Decode the one lookahead body before discovering it
                        // does not fit, as a real state read is allowed to do.
                        let body =
                            Arc::new(block::Block::zcash_deserialize(encoded.as_slice()).unwrap());
                        let next = bytes + encoded.len();
                        if next > usize::try_from(cap).unwrap() {
                            break;
                        }
                        blocks.push((height, body, encoded.len()));
                        bytes = next;
                    }
                    blocks
                });
                probe.allocations(allocations);
                {
                    let mut live = decoded.lock().unwrap();
                    live.retain(|body| body.strong_count() > 0);
                    live.extend(blocks.iter().map(|(_, body, _)| Arc::downgrade(body)));
                    let bytes: u64 = live
                        .iter()
                        .filter_map(Weak::upgrade)
                        .map(|body| body.attributed_memory_size_bytes())
                        .sum();
                    peak_decoded.fetch_max(bytes, Ordering::Relaxed);
                }
                operation.finish();
                Ok(BlockRangeReadResult::new(blocks, lease))
            })
            .await?
        });
        job
    }
}

pub(super) fn config(workers: usize) -> ZakuraBlockSyncConfig {
    let mut config = ZakuraBlockSyncConfig {
        max_blocks_per_response: 128,
        max_inflight_requests: 16,
        ..ZakuraBlockSyncConfig::default()
    };
    config.get_blocks_regulation.node_active_requests = workers;
    config.peer_limits.max_inbound_peers = 8;
    config.peer_limits.max_outbound_peers = 8;
    config
}

pub(super) async fn request(f: &Fixture, start: u32, count: u32) {
    time::timeout(
        DEADLINE,
        f.requests.send(
            BlockSyncMessage::GetBlocks {
                start_height: block::Height(start),
                count,
            }
            .encode_frame()
            .unwrap(),
        ),
    )
    .await
    .unwrap()
    .unwrap();
}

pub(super) async fn response(
    f: &mut Fixture,
    source: &ControlledSource,
    start: u32,
    requested: u32,
    cap: u32,
    count_cap: u32,
) -> usize {
    let encoded = source.encoded.clone();
    let fail = source.fail;
    // Decoding large expected responses must not block the task polling other
    // peers' frame deadlines when these checkers run together with join_all.
    let expected = tokio::task::spawn_blocking(move || {
        let mut expected = Vec::new();
        let mut bytes = 0usize;
        for offset in 0..requested.min(count_cap) {
            let Some(encoded) = encoded.get(&block::Height(start + offset)) else {
                break;
            };
            if fail || bytes + encoded.len() > usize::try_from(cap).unwrap() {
                break;
            }
            bytes += encoded.len();
            expected.push(
                block::Block::zcash_deserialize(encoded.as_slice())
                    .unwrap()
                    .hash(),
            );
        }
        expected
    })
    .await
    .unwrap();
    for (offset, hash) in expected.iter().enumerate() {
        let BlockSyncMessage::Block(body) = f.next().await else {
            panic!("C07 missing expected prefix body");
        };
        assert_eq!(
            body.coinbase_height().unwrap().0,
            start + u32::try_from(offset).unwrap()
        );
        assert_eq!(body.hash(), *hash);
    }
    let terminal = f.next().await;
    if expected.is_empty() {
        assert_eq!(
            terminal,
            BlockSyncMessage::RangeUnavailable {
                start_height: block::Height(start),
                count: requested
            }
        );
    } else {
        assert_eq!(
            terminal,
            BlockSyncMessage::BlocksDone {
                start_height: block::Height(start),
                returned: u32::try_from(expected.len()).unwrap()
            }
        );
    }
    expected.len()
}
