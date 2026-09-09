//! Multi-node in-process Zakura harness.

use std::time::Duration;

use super::{await_until, TraceCapture, ZakuraTestNode};
use crate::{zakura::ZakuraPeerId, BoxError};

/// Supported deterministic topologies.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ClusterTopology {
    /// Every node dials every higher-indexed peer.
    FullMesh,
    /// Node `n` dials node `n + 1`.
    Line,
}

/// In-process collection of Zakura nodes.
#[derive(Debug, Default)]
pub struct ZakuraTestCluster {
    nodes: Vec<ZakuraTestNode>,
}

impl ZakuraTestCluster {
    /// Create an empty cluster.
    pub fn new() -> Self {
        Self::default()
    }

    /// Spawn one node and append it to the cluster.
    pub async fn spawn_node(&mut self, seed: u64) -> Result<usize, BoxError> {
        let node = ZakuraTestNode::builder(seed).spawn().await?;
        self.nodes.push(node);
        Ok(self.nodes.len() - 1)
    }

    /// Spawn one preconfigured node and append it to the cluster.
    #[cfg(test)]
    pub(crate) async fn spawn_node_with_builder(
        &mut self,
        builder: super::ZakuraTestNodeBuilder,
    ) -> Result<usize, BoxError> {
        let node = builder.spawn().await?;
        self.nodes.push(node);
        Ok(self.nodes.len() - 1)
    }

    /// Spawn one node with a per-node JSONL trace directory.
    pub async fn spawn_traced_node(
        &mut self,
        seed: u64,
        trace: &mut TraceCapture,
    ) -> Result<usize, BoxError> {
        let node = ZakuraTestNode::builder(seed)
            .tracer(trace.tracer_for_node(seed))
            .spawn()
            .await?;
        self.nodes.push(node);
        Ok(self.nodes.len() - 1)
    }

    /// Spawn all nodes in `seeds`.
    pub async fn spawn_nodes(
        &mut self,
        seeds: impl IntoIterator<Item = u64>,
    ) -> Result<(), BoxError> {
        for seed in seeds {
            self.spawn_node(seed).await?;
        }
        Ok(())
    }

    /// Borrow all nodes.
    pub fn nodes(&self) -> &[ZakuraTestNode] {
        &self.nodes
    }

    /// Borrow one node by index.
    pub fn node(&self, index: usize) -> &ZakuraTestNode {
        &self.nodes[index]
    }

    /// Connect nodes according to `topology`.
    pub async fn connect_topology(
        &self,
        topology: ClusterTopology,
        timeout: Duration,
    ) -> Result<(), BoxError> {
        match topology {
            ClusterTopology::FullMesh => self.connect_full_mesh(timeout).await,
            ClusterTopology::Line => {
                for pair in self.nodes.windows(2) {
                    pair[0].connect_native(&pair[1], timeout).await?;
                }
                Ok(())
            }
        }
    }

    /// Connect every pair in the cluster.
    pub async fn connect_full_mesh(&self, timeout: Duration) -> Result<(), BoxError> {
        for left in 0..self.nodes.len() {
            for right in (left + 1)..self.nodes.len() {
                self.nodes[left]
                    .connect_native(&self.nodes[right], timeout)
                    .await?;
            }
        }
        Ok(())
    }

    /// Wait until every node has registered all expected peers.
    pub async fn await_all_connected(&self, timeout: Duration) -> Result<(), BoxError> {
        let mut ids = Vec::with_capacity(self.nodes.len());
        for node in &self.nodes {
            ids.push(node.node_addr().await.node_id.as_bytes().to_vec());
        }

        for node in &self.nodes {
            let own_id = node.node_addr().await.node_id.as_bytes().to_vec();
            let expected_peers: Vec<Vec<u8>> =
                ids.iter().filter(|id| **id != own_id).cloned().collect();
            let registered = node.supervisor().subscribe();
            await_until("cluster peer set", timeout, || {
                expected_peers
                    .iter()
                    .all(|expected| contains_peer(&registered.borrow(), expected))
            })
            .await?;
        }
        Ok(())
    }

    /// Shut down all nodes.
    pub async fn shutdown(&self) {
        for node in &self.nodes {
            node.shutdown().await;
        }
    }
}

fn contains_peer(peers: &[ZakuraPeerId], expected: &[u8]) -> bool {
    peers.iter().any(|peer| peer.as_bytes() == expected)
}

#[cfg(test)]
mod tests {
    use super::super::{HostilePeer, TraceValue};
    use super::*;
    use crate::{
        zakura::trace::block_sync_trace as bs_trace,
        zakura::{
            block_sync::{MAX_BS_FRAME_BYTES, ZAKURA_CAP_BLOCK_SYNC, ZAKURA_STREAM_BLOCK_SYNC},
            BlockApplyResult, BlockSizeEstimate, BlockSyncAction, BlockSyncBlockMeta,
            BlockSyncEvent, BlockSyncFrontiers, BlockSyncMessage, BlockSyncStatus,
            DiscoveryMessage, Event, Frame, FramedSend, FullStateFrontiers, HeaderEntry,
            HeaderSyncAction, HeaderSyncMessage, Headers, Peer, Service, ServicePeerLimits, Status,
            Stream, StreamMode, ZakuraBlockSyncConfig, ZakuraConnId, ZakuraLocalLimits,
            FRAME_HEADER_BYTES, MAX_BS_RESPONSE_BYTES, ZAKURA_CAP_DISCOVERY,
            ZAKURA_CAP_HEADER_SYNC, ZAKURA_CAP_LEGACY_GOSSIP, ZAKURA_STREAM_DISCOVERY,
            ZAKURA_STREAM_GOSSIP, ZAKURA_STREAM_HEADER_SYNC,
        },
        Config,
    };
    use std::{
        collections::{BTreeMap, HashMap},
        sync::{Arc, Mutex as StdMutex},
    };
    use tokio::{
        sync::{mpsc, Mutex},
        task::JoinHandle,
    };
    use zakura_chain::{
        block,
        parameters::{
            testnet::{
                ConfiguredActivationHeights, ConfiguredCheckpoints, Parameters as TestnetParameters,
            },
            Network,
        },
        serialization::{ZcashDeserializeInto, ZcashSerialize},
        work::difficulty::U256,
    };

    #[tokio::test]
    async fn protocol_v8_status_and_correlated_response_cross_real_wire() -> Result<(), BoxError> {
        let anchor = (block::Height(0), mainnet_genesis_hash());
        let block_one = mainnet_block(&BLOCK_MAINNET_1_BYTES);
        let target = (block::Height(1), block_one.hash());
        let victim = ZakuraTestNode::builder(70)
            .header_sync_driver(
                Network::Mainnet,
                anchor,
                FullStateFrontiers {
                    finalized_height: anchor.0,
                    verified_block_tip: anchor.0,
                    verified_block_hash: anchor.1,
                },
                Some(anchor),
            )
            .spawn()
            .await?;
        let mut actions = victim
            .take_header_sync_actions()
            .await
            .expect("the external header driver is enabled");
        let header_sync = victim
            .header_sync()
            .expect("the real header service is enabled");
        let codec = header_sync.codec();
        let hostile =
            HostilePeer::connect_native_with_capabilities(&victim, 71, ZAKURA_CAP_HEADER_SYNC)
                .await?;
        let hostile_id = hostile.id()?;

        assert!(matches!(
            tokio::time::timeout(
                Duration::from_secs(5),
                hostile.recv_header_sync_message(&codec)
            )
            .await
            .map_err(|_| -> BoxError { "timed out waiting for initial v8 Status".into() })??,
            HeaderSyncMessage::Status(_)
        ));

        hostile
            .send_header_sync_message(
                &codec,
                &HeaderSyncMessage::Status(Status {
                    work_anchor_height: anchor.0,
                    work_anchor_hash: anchor.1,
                    selected_tip_height: target.0,
                    selected_tip_hash: target.1,
                    suffix_cumulative_work: U256::from(1_u8),
                    oldest_retained_height: anchor.0,
                    max_headers_per_response: 16,
                    max_inflight_requests: 1,
                    max_message_bytes: 1_000_000,
                    tree_aux_schema_mask: 0,
                }),
            )
            .await?;

        let (session_id, scope) = match tokio::time::timeout(Duration::from_secs(5), actions.recv())
            .await
            .map_err(|_| -> BoxError { "timed out waiting for QueryHeaderLocator".into() })?
            .ok_or("header action channel closed")?
        {
            HeaderSyncAction::QueryHeaderLocator {
                peer,
                session_id,
                target_tip_hash,
                scope,
            } => {
                assert_eq!(peer, hostile_id);
                assert_eq!(target_tip_hash, target.1);
                (session_id, scope)
            }
            action => return Err(format!("unexpected header action: {action:?}").into()),
        };
        header_sync
            .send(Event::HeaderLocatorReady {
                peer: hostile_id.clone(),
                session_id,
                target_tip_hash: target.1,
                scope,
                locator: Some(zakura_header_chain::HeaderLocator::for_continuation(
                    zakura_header_chain::Frontier::new(anchor.0, anchor.1),
                )),
            })
            .await?;

        let request = match tokio::time::timeout(
            Duration::from_secs(5),
            hostile.recv_header_sync_message(&codec),
        )
        .await
        .map_err(|_| -> BoxError { "timed out waiting for v8 GetHeaders".into() })??
        {
            HeaderSyncMessage::GetHeaders(request) => request,
            message => return Err(format!("unexpected header message: {message:?}").into()),
        };
        assert_ne!(request.request_id, 0);
        assert_eq!(request.target_tip_hash, target.1);
        assert_eq!(request.locator_hashes.first(), Some(&anchor.1));

        hostile
            .send_header_sync_message(
                &codec,
                &HeaderSyncMessage::Headers(Headers {
                    request_id: request.request_id,
                    target_tip_hash: target.1,
                    common_ancestor_height: anchor.0,
                    common_ancestor_hash: anchor.1,
                    complete: true,
                    tree_aux_schema: crate::zakura::AuxSchema::None,
                    entries: vec![HeaderEntry {
                        header: block_one.header.clone(),
                        body_size: u32::try_from(block_one.zcash_serialized_size())
                            .unwrap_or(u32::MAX),
                        tree_aux: None,
                    }],
                }),
            )
            .await?;

        match tokio::time::timeout(Duration::from_secs(5), actions.recv())
            .await
            .map_err(|_| -> BoxError { "timed out waiting for PrepareHeaderTarget".into() })?
            .ok_or("header action channel closed")?
        {
            HeaderSyncAction::PrepareHeaderTarget {
                peer,
                owner,
                common_ancestor,
                target: prepared_target,
                entries,
                ..
            } => {
                assert_eq!(peer, hostile_id);
                assert_eq!(owner.session_id(), session_id);
                assert_eq!(owner.header_authority(), scope);
                assert_eq!(
                    common_ancestor,
                    zakura_header_chain::Frontier::new(anchor.0, anchor.1)
                );
                assert_eq!(
                    prepared_target,
                    zakura_header_chain::Frontier::new(target.0, target.1)
                );
                assert_eq!(entries.len(), 1);
            }
            action => return Err(format!("unexpected header action: {action:?}").into()),
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(100), actions.recv())
                .await
                .is_err(),
            "one correlated response schedules exactly one preparation",
        );
        assert!(victim
            .supervisor()
            .registered_ids()
            .await
            .contains(&hostile_id));

        hostile.shutdown().await;
        victim.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn protocol_v8_hostile_frames_disconnect_with_boundary_attribution(
    ) -> Result<(), BoxError> {
        let mut capture = TraceCapture::for_test_with_keep_override(
            "protocol_v8_hostile_frames_disconnect",
            false,
        )?;
        let anchor = (block::Height(0), mainnet_genesis_hash());
        let victim = ZakuraTestNode::builder(72)
            .max_connections_per_ip(8)
            .tracer(capture.tracer_for_node(72))
            .header_sync_driver(
                Network::Mainnet,
                anchor,
                FullStateFrontiers {
                    finalized_height: anchor.0,
                    verified_block_tip: anchor.0,
                    verified_block_hash: anchor.1,
                },
                Some(anchor),
            )
            .spawn()
            .await?;
        let header_sync = victim
            .header_sync()
            .expect("the real header service is enabled");
        let codec = header_sync.codec();
        let mut actions = victim
            .take_header_sync_actions()
            .await
            .expect("the external header driver is enabled");
        let control =
            HostilePeer::connect_native_with_capabilities(&victim, 73, ZAKURA_CAP_HEADER_SYNC)
                .await?;
        let control_id = control.id()?;

        for (seed, case) in [(74, "unknown"), (75, "unsolicited"), (76, "oversize")] {
            let hostile = HostilePeer::connect_native_with_capabilities(
                &victim,
                seed,
                ZAKURA_CAP_HEADER_SYNC,
            )
            .await?;
            let hostile_id = hostile.id()?;
            match case {
                "unknown" => {
                    hostile
                        .send_raw_frame(
                            ZAKURA_STREAM_HEADER_SYNC,
                            Frame {
                                message_type: 99,
                                flags: 0,
                                payload: vec![99],
                            },
                        )
                        .await?;
                }
                "unsolicited" => {
                    hostile
                        .send_header_sync_message(
                            &codec,
                            &HeaderSyncMessage::Headers(Headers {
                                request_id: 999,
                                target_tip_hash: anchor.1,
                                common_ancestor_height: anchor.0,
                                common_ancestor_hash: anchor.1,
                                complete: true,
                                tree_aux_schema: crate::zakura::AuxSchema::None,
                                entries: Vec::new(),
                            }),
                        )
                        .await?;
                }
                "oversize" => {
                    hostile
                        .oversize_frame_declared_len(ZAKURA_STREAM_HEADER_SYNC)
                        .await?;
                }
                _ => unreachable!("the hostile matrix cases are fixed"),
            }
            hostile
                .wait_for_connection_close(Duration::from_secs(5))
                .await?;
            await_until(
                "hostile header peer removed",
                Duration::from_secs(5),
                || {
                    !victim
                        .supervisor()
                        .subscribe()
                        .borrow()
                        .contains(&hostile_id)
                },
            )
            .await?;
            assert!(
                victim
                    .supervisor()
                    .registered_ids()
                    .await
                    .contains(&control_id),
                "the normal control peer remains connected after {case}"
            );
            hostile.shutdown().await;
        }
        assert!(
            actions.try_recv().is_err(),
            "hostile frames never schedule target preparation or admission"
        );

        control.shutdown().await;
        victim.shutdown().await;
        capture.flush().await;
        let trace = capture.reader()?;
        let header_rows = trace.node("72").table("header_sync");
        header_rows.assert_row(
            "header_peer_violation",
            &[
                ("boundary", TraceValue::Str("pipe")),
                ("reason", TraceValue::Str("unknown_message_type")),
                ("disposition", TraceValue::Str("disconnect")),
            ],
        );
        header_rows.assert_row(
            "header_peer_violation",
            &[
                ("boundary", TraceValue::Str("pipe")),
                ("reason", TraceValue::Str("unsolicited_response")),
                ("disposition", TraceValue::Str("disconnect")),
            ],
        );
        assert!(capture.finish().await?.is_none());
        Ok(())
    }
    use zakura_test::vectors::{
        BLOCK_MAINNET_1_BYTES, BLOCK_MAINNET_2_BYTES, BLOCK_MAINNET_3_BYTES, BLOCK_MAINNET_4_BYTES,
        BLOCK_MAINNET_5_BYTES, BLOCK_MAINNET_GENESIS_BYTES,
    };

    const CUSTOM_FRAME_CAP_STREAM_KIND: u16 = 42;
    const CUSTOM_FRAME_CAP_CAPABILITY: u64 = 1 << 20;
    const CUSTOM_FRAME_CAP_BYTES: u32 = 64;
    const CUSTOM_FRAME_CAP_STREAMS: [Stream; 1] = [Stream {
        kind: CUSTOM_FRAME_CAP_STREAM_KIND,
        version: 1,
        frame_cap: CUSTOM_FRAME_CAP_BYTES,
        capability: CUSTOM_FRAME_CAP_CAPABILITY,
        mode: StreamMode::Ordered,
    }];

    #[derive(Debug, Default)]
    struct OrderedSourceProbeService {
        senders: Arc<Mutex<HashMap<ZakuraPeerId, FramedSend>>>,
    }

    impl OrderedSourceProbeService {
        async fn contains_peer(&self, peer: &ZakuraPeerId) -> bool {
            self.senders.lock().await.contains_key(peer)
        }

        async fn send_payload(
            &self,
            peer: &ZakuraPeerId,
            payload: Vec<u8>,
        ) -> Result<(), BoxError> {
            let sender = {
                let senders = self.senders.lock().await;
                senders.get(peer).cloned()
            };
            let Some(sender) = sender else {
                return Err("source probe peer sender missing".into());
            };
            sender
                .send(Frame {
                    message_type: 77,
                    flags: 0,
                    payload,
                })
                .await
                .map_err(|_| -> BoxError { "source probe sender closed".into() })
        }
    }

    impl Service for OrderedSourceProbeService {
        fn name(&self) -> &'static str {
            "ordered-source-probe"
        }

        fn streams(&self) -> &[Stream] {
            crate::zakura::legacy_gossip_streams()
        }

        fn add_peer(&self, mut peer: Peer) {
            let peer_id = peer.id.clone();
            let Some((mut recv, send)) = peer.take_stream(ZAKURA_STREAM_GOSSIP) else {
                return;
            };
            let cancel_token = peer.cancel_token();
            let senders = self.senders.clone();
            tokio::spawn(async move {
                senders.lock().await.insert(peer_id.clone(), send);
                loop {
                    tokio::select! {
                        _ = cancel_token.cancelled() => break,
                        frame = recv.recv() => {
                            if frame.is_none() {
                                break;
                            }
                        }
                    }
                }
                senders.lock().await.remove(&peer_id);
            });
        }

        fn remove_peer(&self, peer: &ZakuraPeerId, _conn_id: ZakuraConnId) {
            let senders = self.senders.clone();
            let peer = peer.clone();
            tokio::spawn(async move {
                senders.lock().await.remove(&peer);
            });
        }
    }

    #[derive(Debug, Default)]
    struct CustomFrameCapProbeService {
        senders: Arc<StdMutex<HashMap<ZakuraPeerId, FramedSend>>>,
        received: Arc<StdMutex<Vec<(ZakuraPeerId, Vec<u8>)>>>,
    }

    impl CustomFrameCapProbeService {
        fn contains_peer(&self, peer: &ZakuraPeerId) -> bool {
            self.senders
                .lock()
                .expect("custom frame-cap sender map is never poisoned")
                .contains_key(peer)
        }

        fn contains_payload(&self, peer: &ZakuraPeerId, payload: &[u8]) -> bool {
            self.received
                .lock()
                .expect("custom frame-cap receive list is never poisoned")
                .iter()
                .any(|(sender, received)| sender == peer && received == payload)
        }

        async fn send_payload(
            &self,
            peer: &ZakuraPeerId,
            payload: Vec<u8>,
        ) -> Result<(), BoxError> {
            let sender = self
                .senders
                .lock()
                .map_err(|_| -> BoxError { "custom frame-cap sender map is poisoned".into() })?
                .get(peer)
                .cloned()
                .ok_or_else(|| -> BoxError { "custom frame-cap sender is missing".into() })?;

            sender
                .send(Frame {
                    message_type: 1,
                    flags: 0,
                    payload,
                })
                .await
                .map_err(|_| "custom frame-cap sender closed".into())
        }
    }

    impl Service for CustomFrameCapProbeService {
        fn name(&self) -> &'static str {
            "custom-frame-cap-probe"
        }

        fn streams(&self) -> &[Stream] {
            &CUSTOM_FRAME_CAP_STREAMS
        }

        fn add_peer(&self, mut peer: Peer) {
            let peer_id = peer.id.clone();
            let Some((mut recv, send)) = peer.take_stream(CUSTOM_FRAME_CAP_STREAM_KIND) else {
                return;
            };
            self.senders
                .lock()
                .expect("custom frame-cap sender map is never poisoned")
                .insert(peer_id.clone(), send);

            let cancel_token = peer.cancel_token();
            let received = self.received.clone();
            tokio::spawn(async move {
                loop {
                    let frame = tokio::select! {
                        _ = cancel_token.cancelled() => return,
                        frame = recv.recv() => {
                            let Some(frame) = frame else {
                                return;
                            };
                            frame
                        }
                    };

                    received
                        .lock()
                        .expect("custom frame-cap receive list is never poisoned")
                        .push((peer_id.clone(), frame.payload));
                }
            });
        }

        fn remove_peer(&self, peer: &ZakuraPeerId, _conn_id: ZakuraConnId) {
            self.senders
                .lock()
                .expect("custom frame-cap sender map is never poisoned")
                .remove(peer);
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum TaskExitProbeEvent {
        Added(ZakuraPeerId),
        SinkExited(ZakuraPeerId),
        SourceExited(ZakuraPeerId),
        Removed(ZakuraPeerId),
    }

    #[derive(Debug)]
    struct TaskExitProbeService {
        events: mpsc::UnboundedSender<TaskExitProbeEvent>,
    }

    impl TaskExitProbeService {
        fn new(events: mpsc::UnboundedSender<TaskExitProbeEvent>) -> Arc<Self> {
            Arc::new(Self { events })
        }
    }

    impl Service for TaskExitProbeService {
        fn name(&self) -> &'static str {
            "task-exit-probe"
        }

        fn streams(&self) -> &[Stream] {
            crate::zakura::legacy_gossip_streams()
        }

        fn add_peer(&self, mut peer: Peer) {
            let peer_id = peer.id.clone();
            let _ = self.events.send(TaskExitProbeEvent::Added(peer_id.clone()));
            let Some((mut recv, send)) = peer.take_stream(ZAKURA_STREAM_GOSSIP) else {
                return;
            };

            let cancel_token = peer.cancel_token();
            let sink_events = self.events.clone();
            let sink_peer = peer_id.clone();
            let sink_cancel = cancel_token.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = sink_cancel.cancelled() => {
                            let _ = sink_events.send(TaskExitProbeEvent::SinkExited(sink_peer));
                            return;
                        }
                        frame = recv.recv() => {
                            if frame.is_none() {
                                let _ = sink_events.send(TaskExitProbeEvent::SinkExited(sink_peer));
                                return;
                            }
                        }
                    }
                }
            });

            let source_events = self.events.clone();
            tokio::spawn(async move {
                let _send = send;
                cancel_token.cancelled().await;
                let _ = source_events.send(TaskExitProbeEvent::SourceExited(peer_id));
            });
        }

        fn remove_peer(&self, peer: &ZakuraPeerId, _conn_id: ZakuraConnId) {
            let _ = self.events.send(TaskExitProbeEvent::Removed(peer.clone()));
        }
    }

    async fn wait_for_probe_event(
        events: &mut mpsc::UnboundedReceiver<TaskExitProbeEvent>,
        label: &'static str,
        mut matches: impl FnMut(&TaskExitProbeEvent) -> bool,
    ) -> Result<TaskExitProbeEvent, BoxError> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let event = events.recv().await.ok_or_else(|| -> BoxError {
                    format!("task-exit probe closed before {label}").into()
                })?;
                if matches(&event) {
                    return Ok(event);
                }
            }
        })
        .await
        .map_err(|_| -> BoxError { format!("timed out waiting for {label}").into() })?
    }

    fn mainnet_block(bytes: &[u8]) -> Arc<block::Block> {
        Arc::new(bytes.zcash_deserialize_into().expect("block vector parses"))
    }

    fn block_size(block: &block::Block) -> u32 {
        u32::try_from(
            block
                .zcash_serialize_to_vec()
                .expect("test block serializes")
                .len(),
        )
        .expect("test block size fits u32")
    }

    async fn drive_native_block_sync_actions(
        node: &ZakuraTestNode,
        blocks: Vec<Arc<block::Block>>,
        submitted: Arc<StdMutex<Vec<block::Height>>>,
    ) -> JoinHandle<()> {
        let endpoint = node.endpoint();
        let mut actions = node
            .take_block_sync_actions()
            .await
            .expect("block-sync action receiver is enabled");
        let by_height: BTreeMap<_, _> = blocks
            .into_iter()
            .map(|block| {
                (
                    block.coinbase_height().expect("test block has height"),
                    block,
                )
            })
            .collect();

        tokio::spawn(async move {
            while let Some(action) = actions.recv().await {
                let Some(handle) = endpoint.block_sync() else {
                    continue;
                };
                match action {
                    BlockSyncAction::QueryNeededBlocks {
                        query_id,
                        from,
                        limit,
                        best_header_tip,
                        scope,
                    } => {
                        let metas = if limit == 0 {
                            Vec::new()
                        } else {
                            let end = (from + i64::from(limit.saturating_sub(1)))
                                .unwrap_or(block::Height::MAX)
                                .min(best_header_tip);
                            by_height
                                .range(from..=end)
                                .map(|(height, block)| BlockSyncBlockMeta {
                                    height: *height,
                                    hash: block.hash(),
                                    size: BlockSizeEstimate::Advertised(block_size(block)),
                                })
                                .collect()
                        };
                        let _ = handle
                            .send(BlockSyncEvent::ScopedNeededBlocks {
                                query_id,
                                scope,
                                body_anchor: {
                                    let height = block::Height(from.0.saturating_sub(1));
                                    let hash = by_height
                                        .get(&height)
                                        .map(|block| block.hash())
                                        .unwrap_or_else(mainnet_genesis_hash);
                                    zakura_header_chain::Frontier::new(height, hash)
                                },
                                blocks: metas,
                            })
                            .await;
                    }
                    BlockSyncAction::SubmitBlock {
                        owner,
                        source,
                        token,
                        block,
                    } => {
                        let height = block.coinbase_height().expect("submitted block has height");
                        submitted
                            .lock()
                            .expect("submitted list mutex is not poisoned")
                            .push(height);
                        let _ = handle
                            .send(BlockSyncEvent::BlockApplyFinished {
                                owner,
                                source,
                                token,
                                height,
                                hash: block.hash(),
                                outcome: crate::zakura::block_sync::test_block_apply_outcome(
                                    BlockApplyResult::Committed,
                                ),
                            })
                            .await;
                        let _ = handle
                            .send(BlockSyncEvent::ChainTipGrow(BlockSyncFrontiers {
                                finalized_height: height,
                                verified_block_tip: height,
                                verified_block_hash: block.hash(),
                            }))
                            .await;
                    }
                    BlockSyncAction::QueryBlocksByHeightRange { peer, start, count } => {
                        let _ = handle
                            .send(BlockSyncEvent::BlockRangeResponseFinished {
                                peer,
                                start_height: start,
                                requested_count: count,
                                returned_count: 0,
                            })
                            .await;
                    }
                    BlockSyncAction::RecordBodyUnavailable { .. }
                    | BlockSyncAction::RecordBodyInvalid { .. }
                    | BlockSyncAction::RestartBodyAvailability { .. }
                    | BlockSyncAction::RetryBodyAvailability { .. } => {}
                    BlockSyncAction::Misbehavior { .. } => {}
                }
            }
        })
    }

    fn block_bytes(height: u32) -> &'static [u8] {
        match height {
            1 => &BLOCK_MAINNET_1_BYTES,
            2 => &BLOCK_MAINNET_2_BYTES,
            3 => &BLOCK_MAINNET_3_BYTES,
            4 => &BLOCK_MAINNET_4_BYTES,
            5 => &BLOCK_MAINNET_5_BYTES,
            _ => panic!("missing test vector for height {height}"),
        }
    }

    fn mainnet_genesis_hash() -> block::Hash {
        mainnet_block(&BLOCK_MAINNET_GENESIS_BYTES).hash()
    }

    fn e2e_network(checkpoints: impl IntoIterator<Item = u32>) -> Network {
        let checkpoints = std::iter::once((block::Height(0), mainnet_genesis_hash()))
            .chain(checkpoints.into_iter().map(|height| {
                (
                    block::Height(height),
                    mainnet_block(block_bytes(height)).hash(),
                )
            }))
            .collect();

        TestnetParameters::build()
            .with_genesis_hash(mainnet_genesis_hash())
            .expect("mainnet genesis vector hash parses")
            .with_activation_heights(ConfiguredActivationHeights {
                before_overwinter: None,
                overwinter: Some(1),
                sapling: Some(1),
                blossom: Some(1),
                heartwood: Some(1),
                canopy: Some(1),
                nu5: None,
                nu6: None,
                nu6_1: None,
                nu6_2: None,
                nu6_3: None,
                nu7: None,
                #[cfg(zcash_unstable = "nutachyon")]
                nu_tachyon: None,
                #[cfg(zcash_unstable = "zfuture")]
                zfuture: None,
            })
            .expect("height-1 activation set is valid")
            .with_funding_streams(Vec::new())
            .with_checkpoints(ConfiguredCheckpoints::HeightsAndHashes(checkpoints))
            .expect("e2e checkpoints use valid header hashes")
            .to_network()
            .expect("e2e network has enough checkpoint coverage")
    }

    #[tokio::test]
    #[ignore = "native handler mesh smoke is exercised by the zakura-integration nextest profile once dial scheduling is made deterministic"]
    async fn cluster_forms_native_two_node_mesh() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let mut cluster = ZakuraTestCluster::new();
        cluster.spawn_nodes([1, 2]).await?;

        cluster.connect_full_mesh(Duration::from_secs(5)).await?;
        cluster.await_all_connected(Duration::from_secs(5)).await?;
        cluster.shutdown().await;

        Ok(())
    }

    #[tokio::test]
    async fn traced_node_records_native_handshake_and_ratelimit_events() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let mut capture = TraceCapture::for_test_with_keep_override(
            "traced_node_records_native_handshake_and_ratelimit_events",
            false,
        )?;
        let mut cluster = ZakuraTestCluster::new();
        let victim_idx = cluster.spawn_traced_node(1, &mut capture).await?;
        let victim = cluster.node(victim_idx);
        let hostile =
            HostilePeer::connect_native_with_capabilities(victim, 2, ZAKURA_CAP_LEGACY_GOSSIP)
                .await?;

        tokio::time::sleep(Duration::from_millis(200)).await;
        hostile.oversize_frame_declared_len(2).await?;
        tokio::time::sleep(Duration::from_millis(300)).await;
        hostile.shutdown().await;
        cluster.shutdown().await;
        capture.flush().await;

        let reader = capture.reader()?;
        reader
            .node("01")
            .table("handshake")
            .assert_sequence(&["control.started", "control.succeeded"]);
        assert!(reader.node("01").table("conn").count("accepted") >= 1);
        assert!(reader.node("01").table("stream").count("accepted") >= 1);
        assert!(reader.node("01").table("ratelimit").count("frame.oversize") >= 1);

        assert!(capture.finish().await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn native_stream6_oversize_frame_is_traceable_over_real_connection(
    ) -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let mut capture = TraceCapture::for_test_with_keep_override(
            "native_stream6_oversize_frame_is_traceable_over_real_connection",
            false,
        )?;
        let mut cluster = ZakuraTestCluster::new();
        let victim_idx = cluster.spawn_traced_node(1, &mut capture).await?;
        let victim = cluster.node(victim_idx);
        let hostile =
            HostilePeer::connect_native_with_capabilities(victim, 2, ZAKURA_CAP_BLOCK_SYNC).await?;
        let hostile_peer = hostile.id()?;
        let peer_set = victim.supervisor().subscribe();

        await_until("block-sync peer registered", Duration::from_secs(5), || {
            peer_set.borrow().contains(&hostile_peer)
        })
        .await?;
        assert!(
            MAX_BS_FRAME_BYTES < victim.limits().max_frame_bytes,
            "test payload must fit the negotiated connection cap but exceed stream-6's cap"
        );
        hostile
            .send_frame_header_with_declared_payload_len(
                ZAKURA_STREAM_BLOCK_SYNC,
                MAX_BS_FRAME_BYTES,
            )
            .await?;

        await_until("stream-6 oversize trace", Duration::from_secs(5), || {
            capture.reader().is_ok_and(|reader| {
                reader
                    .node("01")
                    .table("ratelimit")
                    .rows()
                    .iter()
                    .any(|row| {
                        row.get("event").and_then(serde_json::Value::as_str)
                            == Some("frame.oversize")
                            && row.get("stream_kind").and_then(serde_json::Value::as_str)
                                == Some("block_sync")
                    })
            })
        })
        .await?;
        await_until(
            "oversized stream-6 frame disconnects peer",
            Duration::from_secs(5),
            || !peer_set.borrow().contains(&hostile_peer),
        )
        .await?;

        hostile.shutdown().await;
        cluster.shutdown().await;
        assert!(capture.finish().await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn native_stream6_declared_payload_above_old_cap_is_not_raw_oversize(
    ) -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let mut capture = TraceCapture::for_test_with_keep_override(
            "native_stream6_declared_payload_above_old_cap_is_not_raw_oversize",
            false,
        )?;
        let mut cluster = ZakuraTestCluster::new();
        let victim_idx = cluster.spawn_traced_node(1, &mut capture).await?;
        let victim = cluster.node(victim_idx);
        let hostile =
            HostilePeer::connect_native_with_capabilities(victim, 2, ZAKURA_CAP_BLOCK_SYNC).await?;
        let hostile_peer = hostile.id()?;
        let peer_set = victim.supervisor().subscribe();

        await_until("block-sync peer registered", Duration::from_secs(5), || {
            peer_set.borrow().contains(&hostile_peer)
        })
        .await?;

        let old_max_bs_message_bytes =
            u32::try_from(block::MAX_BLOCK_BYTES).expect("max block bytes fits in u32") + 1;
        let regression_payload_len = old_max_bs_message_bytes + 1;
        assert!(
            regression_payload_len < MAX_BS_FRAME_BYTES,
            "test payload must exceed the old stream-6 cap but fit the new one"
        );

        hostile
            .send_frame_header_with_declared_payload_len(
                ZAKURA_STREAM_BLOCK_SYNC,
                regression_payload_len,
            )
            .await?;

        await_until(
            "incomplete stream-6 frame disconnects peer",
            Duration::from_secs(5),
            || !peer_set.borrow().contains(&hostile_peer),
        )
        .await?;

        let reader = capture.reader()?;
        let raw_oversize = reader
            .node("01")
            .table("ratelimit")
            .rows()
            .iter()
            .any(|row| {
                row.get("event").and_then(serde_json::Value::as_str) == Some("frame.oversize")
                    && row.get("stream_kind").and_then(serde_json::Value::as_str)
                        == Some("block_sync")
            });
        assert!(
            !raw_oversize,
            "payloads above the old stream-6 cap but below the new cap must reach \
             the stream payload reader instead of being dropped as raw frame.oversize"
        );

        hostile.shutdown().await;
        cluster.shutdown().await;
        assert!(capture.finish().await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn native_block_sync_getblocks_flushes_before_hostile_peer_sends_bodies(
    ) -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let mut capture = TraceCapture::for_test_with_keep_override(
            "native_block_sync_getblocks_flushes_before_hostile_peer_sends_bodies",
            false,
        )?;
        let blocks = vec![
            mainnet_block(&BLOCK_MAINNET_1_BYTES),
            mainnet_block(&BLOCK_MAINNET_2_BYTES),
            mainnet_block(&BLOCK_MAINNET_3_BYTES),
        ];

        let mut limits = ZakuraLocalLimits::from_config(&Config::default());
        limits.max_connections = 16;
        limits.max_pending_handshakes = 8;
        limits.max_open_streams = 16;
        limits.max_inbound_queue_depth = 8;
        limits.message_rate_per_second = 64;
        limits.stream_open_rate_per_second = 64;

        let block_sync_config = ZakuraBlockSyncConfig {
            max_blocks_per_response: 3,
            max_inflight_block_bytes: u64::MAX,
            request_timeout: Duration::from_secs(300),
            peer_limits: ServicePeerLimits {
                inbound_queue_depth: 1,
                outbound_queue_depth: 1,
                ..ServicePeerLimits::default()
            },
            ..ZakuraBlockSyncConfig::default()
        };

        let anchor = (block::Height(0), mainnet_genesis_hash());
        let mut cluster = ZakuraTestCluster::new();
        let victim = ZakuraTestNode::builder(60)
            .limits(limits)
            .tracer(capture.tracer_for_node(60))
            .header_sync_driver(
                e2e_network([3]),
                anchor,
                FullStateFrontiers {
                    finalized_height: block::Height(0),
                    verified_block_tip: block::Height(0),
                    verified_block_hash: anchor.1,
                },
                Some((block::Height(3), blocks[2].hash())),
            )
            .block_sync_config(block_sync_config)
            .spawn()
            .await?;
        cluster.nodes.push(victim);
        let victim = cluster.node(0);
        assert!(
            victim.block_sync().is_some(),
            "block-sync handle should be enabled with the header-sync test driver"
        );

        let submitted = Arc::new(StdMutex::new(Vec::new()));
        let driver =
            drive_native_block_sync_actions(victim, blocks.clone(), submitted.clone()).await;
        let hostile =
            HostilePeer::connect_native_with_capabilities(victim, 61, ZAKURA_CAP_BLOCK_SYNC)
                .await?;
        let hostile_peer = hostile.id()?;
        let peer_set = victim.supervisor().subscribe();
        await_until("block-sync peer registered", Duration::from_secs(5), || {
            peer_set.borrow().contains(&hostile_peer)
        })
        .await?;

        hostile
            .send_raw_frame(
                ZAKURA_STREAM_BLOCK_SYNC,
                BlockSyncMessage::Status(BlockSyncStatus {
                    servable_low: block::Height(1),
                    servable_high: block::Height(3),
                    tip_hash: blocks[2].hash(),
                    max_blocks_per_response: 3,
                    max_inflight_requests: 1,
                    max_response_bytes: MAX_BS_RESPONSE_BYTES,
                })
                .encode_frame()?,
            )
            .await?;

        let (start_height, count) = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let frame = hostile.recv_ordered_frame(ZAKURA_STREAM_BLOCK_SYNC).await?;
                match BlockSyncMessage::decode_frame(frame)
                    .map_err(|error| -> BoxError { Box::new(error) })?
                {
                    BlockSyncMessage::GetBlocks {
                        start_height,
                        count,
                    } => return Ok::<_, BoxError>((start_height, count)),
                    BlockSyncMessage::Status(_) => {}
                    msg => {
                        return Err(format!("unexpected native block-sync message: {msg:?}").into())
                    }
                }
            }
        })
        .await
        .map_err(|_| -> BoxError {
            "timed out waiting for physical stream-6 GetBlocks frame".into()
        })??;

        assert_eq!(start_height, block::Height(1));
        assert_eq!(count, 3);
        assert!(
            submitted
                .lock()
                .expect("submitted list mutex is not poisoned")
                .is_empty(),
            "the test-side responder must not send bodies or trigger submissions before it has \
             physically read GetBlocks from stream 6"
        );

        let end_height = start_height
            .0
            .checked_add(count)
            .expect("test request height range fits u32");
        for height in start_height.0..end_height {
            let block = blocks
                .iter()
                .find(|block| block.coinbase_height() == Some(block::Height(height)))
                .expect("requested test block exists")
                .clone();
            hostile
                .send_raw_frame(
                    ZAKURA_STREAM_BLOCK_SYNC,
                    BlockSyncMessage::Block(block).encode_frame()?,
                )
                .await?;
        }
        hostile
            .send_raw_frame(
                ZAKURA_STREAM_BLOCK_SYNC,
                BlockSyncMessage::BlocksDone {
                    start_height,
                    returned: count,
                }
                .encode_frame()?,
            )
            .await?;

        let expected: Vec<_> = (start_height.0..end_height).map(block::Height).collect();
        await_until(
            "native block-sync submitted requested bodies",
            Duration::from_secs(5),
            || {
                let mut actual = submitted
                    .lock()
                    .expect("submitted list mutex is not poisoned")
                    .clone();
                actual.sort_unstable();
                actual.dedup();
                expected.iter().all(|height| actual.contains(height))
            },
        )
        .await?;

        await_until(
            "native block-sync submitted trace rows",
            Duration::from_secs(5),
            || {
                capture.reader().is_ok_and(|reader| {
                    let rows = reader.node("60").table("block_sync").rows();
                    expected.iter().all(|height| {
                        rows.iter().any(|row| {
                            row.get(bs_trace::EVENT).and_then(serde_json::Value::as_str)
                                == Some(bs_trace::BLOCK_BODY_SUBMITTED)
                                && row
                                    .get(bs_trace::HEIGHT)
                                    .and_then(serde_json::Value::as_u64)
                                    == Some(u64::from(height.0))
                        })
                    })
                })
            },
        )
        .await?;

        driver.abort();
        hostile.shutdown().await;
        cluster.shutdown().await;
        assert!(capture.finish().await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn unknown_stream_kind_is_reset_and_never_delivered() -> Result<(), BoxError> {
        // FLUP-015: reset an unknown stream kind before its frame reaches the inbound sink.
        // Continue delivering known stream kinds on the same connection.
        // Assert the recorder state instead of metrics.
        let _guard = zakura_test::init();
        let mut cluster = ZakuraTestCluster::new();
        let victim_idx = cluster.spawn_node(1).await?;
        let victim = cluster.node(victim_idx);
        let recorder = victim.recorder();
        let hostile =
            HostilePeer::connect_native_with_capabilities(victim, 2, ZAKURA_CAP_LEGACY_GOSSIP)
                .await?;

        let known_payload = b"known-kind-frame".to_vec();
        let unknown_payload = b"unknown-kind-frame".to_vec();
        // Reset and drop unknown kind 9.
        hostile.send_frame(9, unknown_payload.clone()).await?;
        // Deliver known gossip kind 2.
        hostile.send_frame(2, known_payload.clone()).await?;

        await_until("known-kind frame delivered", Duration::from_secs(5), || {
            recorder.contains_payload(2, &known_payload)
        })
        .await?;

        // The recorder received the known frame.
        // It must not receive the unknown frame under any kind label.
        let delivered = recorder.drain();
        assert!(
            delivered
                .iter()
                .any(|m| m.stream_kind == 2 && m.frame.payload == known_payload),
            "known-kind frame must be delivered"
        );
        assert!(
            !delivered.iter().any(|m| m.frame.payload == unknown_payload),
            "unknown-kind frame must be reset before delivery, got {delivered:?}"
        );

        hostile.shutdown().await;
        cluster.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn custom_service_frame_cap_is_enforced_end_to_end() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let service_a = Arc::new(CustomFrameCapProbeService::default());
        let service_b = Arc::new(CustomFrameCapProbeService::default());
        let node_a = ZakuraTestNode::builder(54)
            .service(service_a.clone())
            .supported_capabilities(CUSTOM_FRAME_CAP_CAPABILITY)
            .spawn()
            .await?;
        let node_b = ZakuraTestNode::builder(55)
            .service(service_b.clone())
            .supported_capabilities(CUSTOM_FRAME_CAP_CAPABILITY)
            .spawn()
            .await?;
        let peer_a = ZakuraPeerId::new(node_a.node_addr().await.node_id.as_bytes().to_vec())?;
        let peer_b = ZakuraPeerId::new(node_b.node_addr().await.node_id.as_bytes().to_vec())?;

        node_a
            .connect_native(&node_b, Duration::from_secs(5))
            .await?;
        await_until(
            "custom frame-cap stream admitted",
            Duration::from_secs(5),
            || service_a.contains_peer(&peer_b) && service_b.contains_peer(&peer_a),
        )
        .await?;

        let max_payload_bytes = usize::try_from(CUSTOM_FRAME_CAP_BYTES)?
            .checked_sub(FRAME_HEADER_BYTES)
            .expect("custom frame cap includes the frame header");
        let at_cap_from_a = vec![0xa5; max_payload_bytes];
        service_a
            .send_payload(&peer_b, at_cap_from_a.clone())
            .await?;
        await_until(
            "at-cap custom frame delivered from opener",
            Duration::from_secs(5),
            || service_b.contains_payload(&peer_a, &at_cap_from_a),
        )
        .await?;

        let at_cap_from_b = vec![0x5a; max_payload_bytes];
        service_b
            .send_payload(&peer_a, at_cap_from_b.clone())
            .await?;
        await_until(
            "at-cap custom frame delivered from accepting peer",
            Duration::from_secs(5),
            || service_a.contains_payload(&peer_b, &at_cap_from_b),
        )
        .await?;

        let peers_a = node_a.supervisor().subscribe();
        let peers_b = node_b.supervisor().subscribe();
        let over_cap = vec![0xff; max_payload_bytes + 1];
        service_b.send_payload(&peer_a, over_cap.clone()).await?;

        await_until(
            "over-cap custom frame disconnects the peer",
            Duration::from_secs(5),
            || {
                !contains_peer(&peers_a.borrow(), peer_b.as_bytes())
                    && !contains_peer(&peers_b.borrow(), peer_a.as_bytes())
            },
        )
        .await?;
        assert!(
            !service_a.contains_payload(&peer_b, &over_cap),
            "a custom frame over the declared cap must not reach the peer service"
        );

        node_a.shutdown().await;
        node_b.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn unsupported_stream_version_is_reset_and_never_delivered() -> Result<(), BoxError> {
        // FLUP-015: reject a known stream kind at an unsupported version.
        let _guard = zakura_test::init();
        let mut cluster = ZakuraTestCluster::new();
        let victim_idx = cluster.spawn_node(3).await?;
        let victim = cluster.node(victim_idx);
        let recorder = victim.recorder();
        let hostile =
            HostilePeer::connect_native_with_capabilities(victim, 4, ZAKURA_CAP_LEGACY_GOSSIP)
                .await?;

        let bad_version = b"kind-2-version-99".to_vec();
        let good = b"kind-2-version-1".to_vec();
        hostile
            .send_frame_with_version(2, 99, bad_version.clone())
            .await?;
        hostile.send_frame(2, good.clone()).await?;

        await_until("version-1 frame delivered", Duration::from_secs(5), || {
            recorder.contains_payload(2, &good)
        })
        .await?;

        let delivered = recorder.drain();
        assert!(
            !delivered.iter().any(|m| m.frame.payload == bad_version),
            "unsupported-version frame must be reset before delivery, got {delivered:?}"
        );

        hostile.shutdown().await;

        // Header sync supports one version.
        // Reset the stream for retired version 6.
        // Deliver the current version on the same connection.
        let victim = cluster.node(victim_idx);
        let recorder = victim.recorder();
        let hostile = HostilePeer::connect_native_with_capabilities(
            victim,
            5,
            ZAKURA_CAP_HEADER_SYNC | ZAKURA_CAP_LEGACY_GOSSIP,
        )
        .await?;

        let retired_header_sync_v6 = b"header-sync-kind-5-version-6".to_vec();
        let current_header_sync = b"header-sync-kind-5-version-7".to_vec();
        hostile
            .send_frame_with_version(ZAKURA_STREAM_HEADER_SYNC, 6, retired_header_sync_v6.clone())
            .await?;
        hostile
            .send_frame(ZAKURA_STREAM_HEADER_SYNC, current_header_sync.clone())
            .await?;

        await_until(
            "current header-sync frame delivered",
            Duration::from_secs(5),
            || recorder.contains_payload(ZAKURA_STREAM_HEADER_SYNC, &current_header_sync),
        )
        .await?;

        let delivered = recorder.drain();
        assert!(
            !delivered
                .iter()
                .any(|m| m.frame.payload == retired_header_sync_v6),
            "retired header-sync v6 frame must be reset before delivery, got {delivered:?}"
        );
        assert!(
            delivered
                .iter()
                .any(|m| m.stream_kind == ZAKURA_STREAM_HEADER_SYNC
                    && m.frame.payload == current_header_sync),
            "current header-sync frame must be delivered, got {delivered:?}"
        );

        hostile.shutdown().await;
        cluster.shutdown().await;
        Ok(())
    }

    /// Builds a discovery `GetPeers` request frame on the native wire.
    fn discovery_get_peers_frame() -> Frame {
        Frame {
            message_type: 1,
            flags: 0,
            payload: DiscoveryMessage::GetPeers {
                limit: 8,
                wanted_services: Vec::new(),
                exclude_node_ids: Vec::new(),
            }
            .encode()
            .expect("empty GetPeers encodes"),
        }
    }

    #[tokio::test]
    async fn recorder_transport_survives_malformed_header_sync_frame() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let mut cluster = ZakuraTestCluster::new();
        let victim_idx = cluster.spawn_node(5).await?;
        let victim = cluster.node(victim_idx);
        let recorder = victim.recorder();
        let hostile = HostilePeer::connect_native(victim, 6).await?;

        let before = b"before-header-sync-error".to_vec();
        let bad_header_sync_payload = vec![99];
        hostile
            .send_frame(ZAKURA_STREAM_HEADER_SYNC, bad_header_sync_payload.clone())
            .await?;
        hostile.send_frame(2, before.clone()).await?;
        await_until("pre-error gossip delivered", Duration::from_secs(5), || {
            recorder.contains_payload(2, &before)
        })
        .await?;

        let after = b"after-header-sync-error".to_vec();
        hostile.send_frame(2, after.clone()).await?;
        await_until(
            "post-header-sync gossip delivered",
            Duration::from_secs(5),
            || recorder.contains_payload(2, &after),
        )
        .await?;

        let delivered = recorder.drain();
        assert!(
            delivered
                .iter()
                .any(|m| m.stream_kind == 2 && m.frame.payload == before),
            "pre-error gossip frame must be delivered"
        );
        assert!(
            delivered
                .iter()
                .any(|m| m.stream_kind == ZAKURA_STREAM_HEADER_SYNC
                    && m.frame.payload == bad_header_sync_payload),
            "recorder nodes assert transport routing only; production header-sync owners decode header-sync frames and reject malformed payloads, got {delivered:?}"
        );
        assert!(
            delivered.iter().any(|m| m.frame.payload == after),
            "generic transport must not close on header-sync payload decode, got {delivered:?}"
        );

        hostile.shutdown().await;
        cluster.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn unnegotiated_header_sync_stream_is_rejected_before_delivery() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let mut cluster = ZakuraTestCluster::new();
        let victim_idx = cluster.spawn_node(7).await?;
        let victim = cluster.node(victim_idx);
        let recorder = victim.recorder();

        let zero_cap_peer = HostilePeer::connect_native_with_capabilities(victim, 8, 0).await?;
        let rejected_payload = b"unnegotiated-header-sync".to_vec();
        zero_cap_peer
            .send_frame(ZAKURA_STREAM_HEADER_SYNC, rejected_payload.clone())
            .await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !recorder.contains_payload(ZAKURA_STREAM_HEADER_SYNC, &rejected_payload),
            "header-sync stream kind 5 from a zero-capability peer must be rejected before delivery"
        );

        let header_cap_peer =
            HostilePeer::connect_native_with_capabilities(victim, 9, ZAKURA_CAP_HEADER_SYNC)
                .await?;
        let admitted_payload = b"negotiated-header-sync".to_vec();
        header_cap_peer
            .send_frame(ZAKURA_STREAM_HEADER_SYNC, admitted_payload.clone())
            .await?;
        await_until(
            "negotiated header-sync stream delivered",
            Duration::from_secs(5),
            || recorder.contains_payload(ZAKURA_STREAM_HEADER_SYNC, &admitted_payload),
        )
        .await?;

        zero_cap_peer.shutdown().await;
        header_cap_peer.shutdown().await;
        cluster.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn discovery_stream_requires_negotiated_capability_and_responds() -> Result<(), BoxError>
    {
        let _guard = zakura_test::init();
        let victim = ZakuraTestNode::builder(26).spawn().await?;
        let victim_node_id = victim.node_addr().await.node_id;

        // A peer that did not negotiate the discovery capability cannot open a
        // discovery stream and receives no service response.
        let zero_cap_peer = HostilePeer::connect_native_with_capabilities(&victim, 27, 0).await?;
        zero_cap_peer
            .send_raw_frame(ZAKURA_STREAM_DISCOVERY, discovery_get_peers_frame())
            .await?;
        let rejected = tokio::time::timeout(
            Duration::from_millis(200),
            zero_cap_peer.recv_ordered_frame(ZAKURA_STREAM_DISCOVERY),
        )
        .await;
        assert!(
            rejected.is_err() || rejected.is_ok_and(|result| result.is_err()),
            "unnegotiated discovery stream must not receive a service response"
        );

        // A peer that negotiated discovery exchanges native discovery messages:
        // the victim gossips its own signed self-record (Hello) and answers our
        // GetPeers with a Peers response (empty, since it knows no other peers).
        let discovery_peer =
            HostilePeer::connect_native_with_capabilities(&victim, 28, ZAKURA_CAP_DISCOVERY)
                .await?;
        discovery_peer
            .send_raw_frame(ZAKURA_STREAM_DISCOVERY, discovery_get_peers_frame())
            .await?;

        let mut saw_hello = false;
        let mut saw_peers = false;
        for _ in 0..8 {
            if saw_hello && saw_peers {
                break;
            }
            let frame = tokio::time::timeout(
                Duration::from_secs(5),
                discovery_peer.recv_ordered_frame(ZAKURA_STREAM_DISCOVERY),
            )
            .await??;
            assert_eq!(frame.message_type, 1);
            assert_eq!(frame.flags, 0);
            match DiscoveryMessage::decode(&frame.payload)? {
                DiscoveryMessage::Hello { record } => {
                    assert_eq!(record.body.node_id, victim_node_id);
                    saw_hello = true;
                }
                DiscoveryMessage::Peers { records } => {
                    assert!(records.is_empty());
                    saw_peers = true;
                }
                // The victim's own discovery source also asks us for peers.
                DiscoveryMessage::GetPeers { .. } => {}
                DiscoveryMessage::GetServices(_) => {}
                other => panic!("unexpected discovery message: {other:?}"),
            }
        }
        assert!(saw_hello, "victim gossips its signed self-record");
        assert!(saw_peers, "victim answers GetPeers with a Peers response");

        zero_cap_peer.shutdown().await;
        discovery_peer.shutdown().await;
        victim.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn discovery_candidate_dialer_connects_static_candidate() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let dialer = ZakuraTestNode::builder(50).spawn().await?;
        let target = ZakuraTestNode::builder(51).spawn().await?;

        // Seed `target` as a trusted static candidate (loopback allowed) and let
        // the book-driven candidate dialer connect it.
        let target_id = dialer.insert_static_discovery_candidate(&target).await?;
        let _dialer_task = dialer.spawn_discovery_dialer();

        let peer_set = dialer.supervisor().subscribe();
        await_until(
            "discovery dialer connects the static candidate",
            Duration::from_secs(10),
            || contains_peer(&peer_set.borrow(), target_id.as_bytes()),
        )
        .await?;

        dialer.shutdown().await;
        target.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn connected_peers_import_each_others_signed_records() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        // Advertise dialable (non-loopback) addresses so the gossiped records are
        // kept in the dialable book rather than dropped as locally non-dialable.
        let addr_a = "45.33.30.10:9"
            .parse::<std::net::SocketAddr>()
            .expect("valid test addr");
        let addr_b = "45.33.30.11:9"
            .parse::<std::net::SocketAddr>()
            .expect("valid test addr");
        let a = ZakuraTestNode::builder(52)
            .discovery_direct_addrs(vec![addr_a])
            .spawn()
            .await?;
        let b = ZakuraTestNode::builder(53)
            .discovery_direct_addrs(vec![addr_b])
            .spawn()
            .await?;
        let b_id = b.node_addr().await.node_id;

        a.connect_native(&b, Duration::from_secs(5)).await?;

        let mut learned = false;
        for _ in 0..100 {
            if a.discovery().record_for(b_id).await.is_some() {
                learned = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(learned, "node a imports node b's gossiped self-record");

        a.shutdown().await;
        b.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn invalid_discovery_frame_disconnects_negotiated_peer() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let victim = ZakuraTestNode::builder(32).spawn().await?;
        let peer_set = victim.supervisor().subscribe();

        let discovery_peer =
            HostilePeer::connect_native_with_capabilities(&victim, 33, ZAKURA_CAP_DISCOVERY)
                .await?;
        let peer_id = discovery_peer.id()?;

        await_until("discovery peer registered", Duration::from_secs(5), || {
            contains_peer(&peer_set.borrow(), peer_id.as_bytes())
        })
        .await?;

        discovery_peer
            .send_raw_frame(
                ZAKURA_STREAM_DISCOVERY,
                Frame {
                    message_type: 99,
                    flags: 0,
                    payload: Vec::new(),
                },
            )
            .await?;

        await_until(
            "protocol-invalid discovery peer deregistered",
            Duration::from_secs(5),
            || !contains_peer(&peer_set.borrow(), peer_id.as_bytes()),
        )
        .await?;

        discovery_peer.shutdown().await;
        victim.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn discovery_stream_uses_transport_rate_and_oversize_bounds() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let mut capture = TraceCapture::for_test_with_keep_override(
            "discovery_stream_uses_transport_rate_and_oversize_bounds",
            false,
        )?;
        let mut limits = ZakuraLocalLimits::from_config(&Config::default());
        limits.max_connections = 16;
        limits.max_pending_handshakes = 8;
        limits.max_open_streams = 16;
        limits.max_inbound_queue_depth = 256;
        limits.message_rate_per_second = 1;
        limits.stream_open_rate_per_second = 64;
        let victim = ZakuraTestNode::builder(29)
            .limits(limits)
            .tracer(capture.tracer_for_node(29))
            .spawn()
            .await?;

        let flooding =
            HostilePeer::connect_native_with_capabilities(&victim, 30, ZAKURA_CAP_DISCOVERY)
                .await?;
        // The transport traces a per-kind rate violation before it disconnects the stream.
        flooding
            .flood_stream(ZAKURA_STREAM_DISCOVERY, 'd', 16)
            .await?;
        await_until(
            "discovery throttling traced",
            Duration::from_secs(5),
            || {
                capture.reader().is_ok_and(|reader| {
                    reader
                        .node("29")
                        .table("ratelimit")
                        .rows()
                        .iter()
                        .any(|row| {
                            row.get("event").and_then(serde_json::Value::as_str)
                                == Some("message.throttled")
                                && row.get("stream_kind").and_then(serde_json::Value::as_str)
                                    == Some("discovery")
                        })
                })
            },
        )
        .await?;
        flooding.shutdown().await;

        let oversized =
            HostilePeer::connect_native_with_capabilities(&victim, 31, ZAKURA_CAP_DISCOVERY)
                .await?;
        oversized
            .oversize_frame_declared_len(ZAKURA_STREAM_DISCOVERY)
            .await?;
        await_until("discovery oversize traced", Duration::from_secs(5), || {
            capture.reader().is_ok_and(|reader| {
                reader
                    .node("29")
                    .table("ratelimit")
                    .rows()
                    .iter()
                    .any(|row| {
                        row.get("event").and_then(serde_json::Value::as_str)
                            == Some("frame.oversize")
                            && row.get("stream_kind").and_then(serde_json::Value::as_str)
                                == Some("discovery")
                    })
            })
        })
        .await?;

        oversized.shutdown().await;
        victim.shutdown().await;
        assert!(capture.finish().await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn persistent_ordered_stream_uses_message_budget() -> Result<(), BoxError> {
        // P2: a long-lived ordered stream spends the per-kind message budget at the transport.
        // The transport disconnects a peer that exceeds the budget.
        // The service therefore receives at most approximately one budget of frames.
        let _guard = zakura_test::init();
        let mut capture = TraceCapture::for_test_with_keep_override(
            "persistent_ordered_stream_uses_message_budget",
            false,
        )?;

        // Use a small deterministic message budget to expose the aggregate cap.
        let mut limits = ZakuraLocalLimits::from_config(&Config::default());
        limits.max_connections = 16;
        limits.max_pending_handshakes = 8;
        limits.max_open_streams = 16;
        limits.max_inbound_queue_depth = 256;
        limits.message_rate_per_second = 4;
        // Allow stream opens because a separate limiter controls their rate.
        limits.stream_open_rate_per_second = 64;
        let message_budget = limits.message_rate_per_second as usize;

        let victim = ZakuraTestNode::builder(5)
            .limits(limits)
            .tracer(capture.tracer_for_node(5))
            .spawn()
            .await?;
        let recorder = victim.recorder();
        let hostile =
            HostilePeer::connect_native_with_capabilities(&victim, 6, ZAKURA_CAP_LEGACY_GOSSIP)
                .await?;

        let sent = message_budget * 8;
        for index in 0..sent {
            // The victim disconnects after the sender exceeds the budget.
            // Later sends can race the teardown and return an error.
            if hostile
                .send_frame(2, format!("a-{index}").into_bytes())
                .await
                .is_err()
            {
                break;
            }
        }

        // Wait until rate limiting has clearly engaged (more frames sent than one
        // budget, so the bucket must have emptied at least once).
        await_until("rate limiting engaged", Duration::from_secs(5), || {
            capture.reader().is_ok_and(|reader| {
                reader
                    .node("05")
                    .table("ratelimit")
                    .rows()
                    .iter()
                    .any(|row| {
                        row.get("event").and_then(serde_json::Value::as_str)
                            == Some("message.throttled")
                            && row.get("stream_kind").and_then(serde_json::Value::as_str)
                                == Some("gossip")
                    })
            })
        })
        .await?;
        // Brief deterministic settle to let one refill window pass. A correct
        // per-kind bucket should remain close to the initial burst plus one
        // refill, not merely below the much larger flood size.
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Total ever delivered = retained + dropped-by-recorder (the recorder is
        // a bounded tap). The bucket caps the burst near one budget even though
        // the peer sent many frames on the persistent stream.
        let delivered_total = recorder.len() + recorder.dropped_count();
        assert!(
            delivered_total <= message_budget * 2,
            "persistent stream flood delivered {delivered_total} of {sent} frames; \
             the per-kind message bucket must throttle the peer"
        );

        hostile.shutdown().await;
        victim.shutdown().await;
        assert!(capture.finish().await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn persistent_ordered_stream_delivers_frames_in_order() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let victim = ZakuraTestNode::builder(16).spawn().await?;
        let recorder = victim.recorder();
        let hostile =
            HostilePeer::connect_native_with_capabilities(&victim, 17, ZAKURA_CAP_LEGACY_GOSSIP)
                .await?;
        let payloads: Vec<Vec<u8>> = (0..4)
            .map(|index| format!("ordered-{index}").into_bytes())
            .collect();

        for payload in &payloads {
            hostile
                .send_frame(ZAKURA_STREAM_GOSSIP, payload.clone())
                .await?;
        }

        await_until(
            "ordered gossip burst delivered",
            Duration::from_secs(5),
            || recorder.len() >= payloads.len(),
        )
        .await?;
        let delivered: Vec<_> = recorder
            .drain()
            .into_iter()
            .filter(|message| message.stream_kind == ZAKURA_STREAM_GOSSIP)
            .map(|message| message.frame.payload)
            .collect();

        assert_eq!(delivered, payloads);

        hostile.shutdown().await;
        victim.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn service_owned_source_sends_multiple_ordered_frames() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let service = Arc::new(OrderedSourceProbeService::default());
        let victim = ZakuraTestNode::builder(24)
            .service(service.clone())
            .spawn()
            .await?;
        let hostile =
            HostilePeer::connect_native_with_capabilities(&victim, 25, ZAKURA_CAP_LEGACY_GOSSIP)
                .await?;
        let peer_id = hostile.id()?;

        hostile
            .send_frame(ZAKURA_STREAM_GOSSIP, b"open-source-stream".to_vec())
            .await?;
        tokio::time::timeout(Duration::from_secs(5), async {
            while !service.contains_peer(&peer_id).await {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| -> BoxError { "source probe peer registration timed out".into() })?;

        let first = b"source-one".to_vec();
        let second = b"source-two".to_vec();
        service.send_payload(&peer_id, first.clone()).await?;
        service.send_payload(&peer_id, second.clone()).await?;

        let received_first = tokio::time::timeout(
            Duration::from_secs(5),
            hostile.recv_ordered_frame(ZAKURA_STREAM_GOSSIP),
        )
        .await??;
        let received_second = tokio::time::timeout(
            Duration::from_secs(5),
            hostile.recv_ordered_frame(ZAKURA_STREAM_GOSSIP),
        )
        .await??;

        assert_eq!(received_first.payload, first);
        assert_eq!(received_second.payload, second);

        hostile.shutdown().await;
        victim.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn single_peer_disconnect_cancels_service_stream_tasks() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let victim = ZakuraTestNode::builder(21)
            .service(TaskExitProbeService::new(events_tx))
            .spawn()
            .await?;
        let hostile =
            HostilePeer::connect_native_with_capabilities(&victim, 22, ZAKURA_CAP_LEGACY_GOSSIP)
                .await?;
        let peer_id = hostile.id()?;

        hostile
            .send_frame(ZAKURA_STREAM_GOSSIP, b"start-probe".to_vec())
            .await?;
        wait_for_probe_event(
            &mut events_rx,
            "service add",
            |event| matches!(event, TaskExitProbeEvent::Added(peer) if peer == &peer_id),
        )
        .await?;

        assert!(
            victim.supervisor().disconnect_peer(&peer_id).await,
            "the hostile peer should be registered before disconnect"
        );

        let mut sink_exited = false;
        let mut source_exited = false;
        let mut removed = false;
        while !sink_exited || !source_exited || !removed {
            match wait_for_probe_event(&mut events_rx, "service task exit", |event| {
                matches!(
                    event,
                    TaskExitProbeEvent::SinkExited(peer)
                        | TaskExitProbeEvent::SourceExited(peer)
                        | TaskExitProbeEvent::Removed(peer)
                        if peer == &peer_id
                )
            })
            .await?
            {
                TaskExitProbeEvent::SinkExited(peer) if peer == peer_id => sink_exited = true,
                TaskExitProbeEvent::SourceExited(peer) if peer == peer_id => source_exited = true,
                TaskExitProbeEvent::Removed(peer) if peer == peer_id => removed = true,
                _ => {}
            }
        }

        let second =
            HostilePeer::connect_native_with_capabilities(&victim, 23, ZAKURA_CAP_LEGACY_GOSSIP)
                .await?;
        second.shutdown().await;
        hostile.shutdown().await;
        victim.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn impossible_ordered_stream_limits_do_not_leave_registered_peer() -> Result<(), BoxError>
    {
        let _guard = zakura_test::init();
        let mut limits = ZakuraLocalLimits::from_config(&Config::default());
        limits.max_connections = 4;
        limits.max_pending_handshakes = 4;
        limits.max_open_streams = 16;
        limits.max_inbound_queue_depth = 1;
        let victim = ZakuraTestNode::builder(18).limits(limits).spawn().await?;
        let peer_set = victim.supervisor().subscribe();

        let first = HostilePeer::connect_native(&victim, 19).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            peer_set.borrow().is_empty(),
            "peer rejected before registration must not remain in the supervisor peer set"
        );
        if let Ok(first) = first {
            first.shutdown().await;
        }

        let second = HostilePeer::connect_native(&victim, 20).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            peer_set.borrow().is_empty(),
            "a later peer must not be rejected because stale registration state was leaked"
        );
        if let Ok(second) = second {
            second.shutdown().await;
        }
        victim.shutdown().await;
        Ok(())
    }
}
