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
            ids.push(node.node_addr().await.id.as_bytes().to_vec());
        }

        for node in &self.nodes {
            let own_id = node.node_addr().await.id.as_bytes().to_vec();
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
    use super::super::{HostilePeer, LocalEndpointFactory, TraceValue, ZakuraTestNodeBuilder};
    use super::*;
    use crate::{
        zakura::handler::i_open_collision_winner,
        zakura::header_sync::v9::{
            CommonMessage as HeaderSyncMessage, Cursor, FakePort, Operation, SubscribeHeaders,
        },
        zakura::trace::block_sync_trace as bs_trace,
        zakura::{
            block_sync::{
                MAX_BS_FRAME_BYTES, ZAKURA_CAP_BLOCK_SYNC_REGULATED,
                ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION, ZAKURA_STREAM_BLOCK_SYNC,
            },
            BlockApplyResult, BlockSizeEstimate, BlockSyncAction, BlockSyncBlockMeta,
            BlockSyncEvent, BlockSyncFrontiers, BlockSyncMessage, BlockSyncStatus, Credit,
            DiscoveryMessage, Event, Frame, FramedSend, FullStateFrontiers, GetPeers, HeaderEntry,
            HeaderSyncAction, Headers, Peer, Service, ServicePeerLimits, Status, Stream,
            ZakuraBlockSyncConfig, ZakuraConnId, ZakuraLocalLimits, FRAME_HEADER_BYTES,
            MAX_BS_RESPONSE_BYTES, ZAKURA_CAP_DISCOVERY, ZAKURA_CAP_HEADER_SYNC,
            ZAKURA_CAP_LEGACY_GOSSIP, ZAKURA_STREAM_DISCOVERY, ZAKURA_STREAM_GOSSIP,
            ZAKURA_STREAM_HEADER_SYNC,
        },
        Config,
    };
    use std::{
        collections::{BTreeMap, HashMap},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex as StdMutex,
        },
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
    use zakura_quic::QuicConfig;

    /// Over real QUIC, a peer's Status leads to an Open, and a page that
    /// reaches the target prepares it once and closes the subscription.
    #[tokio::test]
    async fn protocol_v9_status_and_subscription_cross_real_wire() -> Result<(), BoxError> {
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
        let hostile =
            HostilePeer::connect_native_with_capabilities(&victim, 71, ZAKURA_CAP_HEADER_SYNC)
                .await?;
        let hostile_id = hostile.id()?;

        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(5), hostile.recv_header_sync_message())
                .await
                .map_err(|_| -> BoxError { "timed out waiting for initial Status".into() })??,
            HeaderSyncMessage::Status(_)
        ));

        hostile
            .send_header_sync_message(&HeaderSyncMessage::Status(Status {
                work_anchor_height: anchor.0,
                work_anchor_hash: anchor.1,
                selected_tip_height: target.0,
                selected_tip_hash: target.1,
                suffix_cumulative_work: U256::from(1_u8),
                oldest_retained_height: anchor.0,
                max_headers_per_response: 16,
                max_subscriptions: 1,
                max_message_bytes: 1_000_000,
                tree_aux_schema_mask: 0,
            }))
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

        let open =
            match tokio::time::timeout(Duration::from_secs(5), hostile.recv_header_sync_message())
                .await
                .map_err(|_| -> BoxError { "timed out waiting for Open".into() })??
            {
                HeaderSyncMessage::SubscribeHeaders(open) => open,
                message => return Err(format!("unexpected header message: {message:?}").into()),
            };
        assert_eq!(open.operation, Operation::Open);
        assert_eq!(open.initial_target_tip_hash, target.1);
        assert_eq!(open.locator_hashes.first(), Some(&anchor.1));

        hostile
            .send_header_sync_message(&HeaderSyncMessage::Headers(Headers {
                subscription_id: open.subscription_id,
                initial_target_tip_hash: target.1,
                common_ancestor_height: anchor.0,
                common_ancestor_hash: anchor.1,
                reaches_initial_target: true,
                tree_aux_schema: crate::zakura::AuxSchema::None,
                entries: vec![HeaderEntry {
                    header: block_one.header.clone(),
                    body_size: u32::try_from(block_one.zcash_serialized_size()).unwrap_or(u32::MAX),
                    tree_aux: None,
                }],
            }))
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
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(5), hostile.recv_header_sync_message())
                .await
                .map_err(|_| -> BoxError { "timed out waiting for Close".into() })??,
            HeaderSyncMessage::SubscribeHeaders(SubscribeHeaders {
                operation: Operation::Close,
                ..
            })
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(100), actions.recv())
                .await
                .is_err(),
            "one page that reaches the target schedules exactly one preparation",
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

    /// Spawn a Regtest node whose header-sync publisher serves `port`, with
    /// its committed tip at `tip` and every body below it verified.
    async fn spawn_header_publisher(
        seed: u64,
        port: Arc<FakePort>,
        tip: zakura_header_chain::Frontier,
    ) -> Result<(ZakuraTestNode, mpsc::Receiver<HeaderSyncAction>), BoxError> {
        let network = Network::new_regtest(Default::default());
        let anchor = (block::Height(0), network.genesis_hash());
        let node = ZakuraTestNode::builder(seed)
            .header_sync_driver(
                network,
                anchor,
                FullStateFrontiers {
                    finalized_height: anchor.0,
                    verified_block_tip: tip.height,
                    verified_block_hash: anchor.1,
                },
                Some((tip.height, tip.hash)),
            )
            .header_sync_port(port)
            .spawn()
            .await?;
        let actions = node
            .take_header_sync_actions()
            .await
            .expect("the external header driver is enabled");
        Ok((node, actions))
    }

    /// Wait for `node`'s next action that is not a status-driven locator
    /// query for another target. A misbehavior or drop fails the test.
    async fn next_header_action(
        actions: &mut mpsc::Receiver<HeaderSyncAction>,
    ) -> Result<HeaderSyncAction, BoxError> {
        let action = tokio::time::timeout(Duration::from_secs(20), actions.recv())
            .await
            .map_err(|_| -> BoxError { "timed out waiting for a header action".into() })?
            .ok_or("header action channel closed")?;
        if matches!(
            action,
            HeaderSyncAction::Misbehavior { .. } | HeaderSyncAction::DropPeer { .. }
        ) {
            return Err(format!("conformant sessions faulted: {action:?}").into());
        }
        Ok(action)
    }

    /// Answer `node`'s locator query for `target` with genesis.
    async fn answer_locator(
        node: &ZakuraTestNode,
        actions: &mut mpsc::Receiver<HeaderSyncAction>,
        target: block::Hash,
    ) -> Result<(), BoxError> {
        let genesis = zakura_header_chain::Frontier::new(
            block::Height(0),
            Network::new_regtest(Default::default()).genesis_hash(),
        );
        loop {
            if let HeaderSyncAction::QueryHeaderLocator {
                peer,
                session_id,
                target_tip_hash,
                scope,
            } = next_header_action(actions).await?
            {
                if target_tip_hash != target {
                    continue;
                }
                node.header_sync()
                    .expect("the header service is enabled")
                    .send(Event::HeaderLocatorReady {
                        peer,
                        session_id,
                        target_tip_hash,
                        scope,
                        locator: Some(zakura_header_chain::HeaderLocator::for_continuation(
                            genesis,
                        )),
                    })
                    .await?;
                return Ok(());
            }
        }
    }

    /// Two nodes subscribe to each other on one connection while A's header
    /// validation stalls.
    ///
    /// A never finishes preparing, so it accepts no page and stops granting
    /// once its staging budget is full. B's publisher must stop at A's credit
    /// window without a fault. A's session reader must still apply B's
    /// Grants and Close while A's own work waits: B receives A's whole chain
    /// across several grants, and A's terminal outcome after B's Close lets
    /// B open again.
    #[tokio::test]
    async fn header_sync_v9_subscriptions_cross_while_one_side_stalls() -> Result<(), BoxError> {
        let a_port = FakePort::new(64);
        let a_tip = *a_port.grow(600).last().expect("A grows a chain");
        let b_port = FakePort::new(64);
        let b_tip = *b_port.grow(6_000).last().expect("B grows a chain");
        let (a, mut a_actions) = spawn_header_publisher(80, a_port, a_tip).await?;
        let (b, mut b_actions) = spawn_header_publisher(81, b_port, b_tip).await?;
        a.connect_native(&b, Duration::from_secs(5)).await?;
        answer_locator(&a, &mut a_actions, b_tip.hash).await?;
        answer_locator(&b, &mut b_actions, a_tip.hash).await?;

        let HeaderSyncAction::PrepareHeaderTarget {
            entries,
            completion,
            ..
        } = next_header_action(&mut a_actions).await?
        else {
            return Err("A prepares the prefix its staging budget holds".into());
        };
        assert_eq!(
            entries.len(),
            usize::try_from(crate::zakura::MAX_HS_RANGE)?,
            "A grants until its staging budget is full, across many grants"
        );
        assert!(matches!(
            completion,
            zakura_header_chain::TargetCompletion::TargetPrefix { .. }
        ));

        let HeaderSyncAction::PrepareHeaderTarget {
            source,
            owner,
            target,
            entries,
            ..
        } = next_header_action(&mut b_actions).await?
        else {
            return Err("B prepares A's whole chain".into());
        };
        assert_eq!(target, a_tip);
        assert_eq!(entries.len(), 600, "A applied every Grant while stalled");

        // A never answers its preparation. A page beyond A's credit would
        // close the connection.
        tokio::time::sleep(Duration::from_millis(500)).await;
        for (node, other) in [(&a, &b), (&b, &a)] {
            assert!(node
                .supervisor()
                .registered_ids()
                .await
                .contains(&peer_id_of(other).await?));
        }
        assert!(
            a_actions.try_recv().is_err(),
            "A's stalled work starts nothing new"
        );

        // B's subscription closed at its target. Only A's terminal outcome
        // lets B's next status open another.
        b.header_sync()
            .expect("the header service is enabled")
            .send(Event::HeaderTargetPrepared {
                peer: peer_id_of(&a).await?,
                source,
                owner,
                result: crate::zakura::HeaderTargetPreparationResult::Failed(Arc::new(
                    zakura_header_chain::HeaderChainError::stale_target(
                        zakura_header_chain::ErrorSubject::Branch(owner.header_authority().branch),
                    ),
                )),
            })
            .await?;
        answer_locator(&b, &mut b_actions, a_tip.hash).await?;

        a.shutdown().await;
        b.shutdown().await;
        Ok(())
    }

    /// The peer ID other nodes see for `node`.
    async fn peer_id_of(node: &ZakuraTestNode) -> Result<ZakuraPeerId, BoxError> {
        Ok(ZakuraPeerId::new(
            node.node_addr().await.id.as_bytes().to_vec(),
        )?)
    }

    /// Each hostile header sync frame closes only its sender's connection,
    /// with the pipe's violation reason in the trace.
    #[tokio::test]
    async fn protocol_v9_hostile_frames_disconnect_with_boundary_attribution(
    ) -> Result<(), BoxError> {
        let mut capture = TraceCapture::for_test_with_keep_override(
            "protocol_v9_hostile_frames_disconnect",
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
        let mut actions = victim
            .take_header_sync_actions()
            .await
            .expect("the external header driver is enabled");
        let control =
            HostilePeer::connect_native_with_capabilities(&victim, 73, ZAKURA_CAP_HEADER_SYNC)
                .await?;
        let control_id = control.id()?;
        let valid_status = Status {
            work_anchor_height: anchor.0,
            work_anchor_hash: anchor.1,
            selected_tip_height: anchor.0,
            selected_tip_hash: anchor.1,
            suffix_cumulative_work: U256::from(1_u8),
            oldest_retained_height: anchor.0,
            max_headers_per_response: 16,
            max_subscriptions: 1,
            max_message_bytes: 1_000_000,
            tree_aux_schema_mask: 0,
        };

        for (seed, case) in [
            (74, "unknown"),
            (75, "unsolicited"),
            (76, "oversize"),
            (77, "malformed"),
            (78, "update"),
        ] {
            let hostile = HostilePeer::connect_native_with_capabilities(
                &victim,
                seed,
                ZAKURA_CAP_HEADER_SYNC,
            )
            .await?;
            let hostile_id = hostile.id()?;
            match case {
                // The frame filter rejects an unknown type before its payload.
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
                // The subscription precheck rejects a page nobody opened.
                "unsolicited" => {
                    let block_one = mainnet_block(&BLOCK_MAINNET_1_BYTES);
                    hostile
                        .send_header_sync_message(&HeaderSyncMessage::Headers(Headers {
                            subscription_id: 999,
                            initial_target_tip_hash: block_one.hash(),
                            common_ancestor_height: anchor.0,
                            common_ancestor_hash: anchor.1,
                            reaches_initial_target: true,
                            tree_aux_schema: crate::zakura::AuxSchema::None,
                            entries: vec![HeaderEntry {
                                header: block_one.header.clone(),
                                body_size: 0,
                                tree_aux: None,
                            }],
                        }))
                        .await?;
                }
                "oversize" => {
                    hostile
                        .oversize_frame_declared_len(ZAKURA_STREAM_HEADER_SYNC)
                        .await?;
                }
                // A Status advertising two subscriptions fails decode.
                "malformed" => {
                    let mut frame = crate::zakura::wire_codec::encode_frame(
                        &HeaderSyncMessage::Status(valid_status.clone()),
                    )?;
                    frame.payload[112] = 2;
                    hostile
                        .send_raw_frame(ZAKURA_STREAM_HEADER_SYNC, frame)
                        .await?;
                }
                // A Grant for no subscription breaks the publisher's rules.
                "update" => {
                    hostile
                        .send_header_sync_message(&HeaderSyncMessage::SubscribeHeaders(
                            SubscribeHeaders {
                                operation: Operation::Grant,
                                subscription_id: 999,
                                update_sequence: 1,
                                initial_target_tip_hash: anchor.1,
                                acknowledged: Cursor {
                                    height: anchor.0,
                                    hash: anchor.1,
                                    headers: 0,
                                    bytes: 0,
                                },
                                locator_hashes: Vec::new(),
                                added_credit: Credit {
                                    objects: 1,
                                    bytes: 0,
                                },
                                tree_aux_schema: crate::zakura::AuxSchema::None,
                            },
                        ))
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
        for reason in ["malformed_message", "subscription_update"] {
            header_rows.assert_row(
                "header_peer_violation",
                &[
                    ("boundary", TraceValue::Str("pipe")),
                    ("reason", TraceValue::Str(reason)),
                    ("disposition", TraceValue::Str("disconnect")),
                ],
            );
        }
        assert!(capture.finish().await?.is_none());
        Ok(())
    }
    use zakura_test::vectors::{
        BLOCK_MAINNET_1046401_BYTES, BLOCK_MAINNET_1_BYTES, BLOCK_MAINNET_2_BYTES,
        BLOCK_MAINNET_3_BYTES, BLOCK_MAINNET_4_BYTES, BLOCK_MAINNET_5_BYTES,
        BLOCK_MAINNET_GENESIS_BYTES,
    };

    const CUSTOM_FRAME_CAP_STREAM_KIND: u16 = 42;
    const CUSTOM_FRAME_CAP_CAPABILITY: u64 = 1 << 20;
    const CUSTOM_FRAME_CAP_BYTES: u32 = 64;
    const CUSTOM_FRAME_CAP_STREAMS: [Stream; 1] = [Stream {
        kind: CUSTOM_FRAME_CAP_STREAM_KIND,
        version: 1,
        frame_cap: CUSTOM_FRAME_CAP_BYTES,
        capability: CUSTOM_FRAME_CAP_CAPABILITY,
        ..Stream::PERSISTENT
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
        let hostile = HostilePeer::connect_native_with_capabilities(
            victim,
            2,
            ZAKURA_CAP_BLOCK_SYNC_REGULATED,
        )
        .await?;
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
        let hostile = HostilePeer::connect_native_with_capabilities(
            victim,
            2,
            ZAKURA_CAP_BLOCK_SYNC_REGULATED,
        )
        .await?;
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
        native_getblocks_exchange(false).await
    }

    /// A remotely opened regulated session can consume the last available slot.
    #[tokio::test]
    async fn regulated_getblocks_admits_the_first_remote_session_at_a_limit_of_one(
    ) -> Result<(), BoxError> {
        native_getblocks_exchange(true).await
    }

    /// A requester that stops reading stalls the served response at its QUIC
    /// window. Queued output then reaches the peer's unsent-byte bound and new
    /// production stops; reading resumes it. Progress waits assume local
    /// loopback and an otherwise idle runtime, and are bounded at 5 s each.
    #[tokio::test]
    async fn regulated_serving_stops_at_the_output_bound_and_resumes_on_read(
    ) -> Result<(), BoxError> {
        use crate::zakura::{
            regulation::sizing, ZAKURA_CAP_BLOCK_SYNC_REGULATED,
            ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION,
        };
        const RESPONSE_BYTES: u32 = 256 * 1024;
        const REQUESTS: u32 = 400;
        let _guard = zakura_test::init();
        let blocks = [
            mainnet_block(&BLOCK_MAINNET_1_BYTES),
            mainnet_block(&BLOCK_MAINNET_2_BYTES),
            mainnet_block(&BLOCK_MAINNET_3_BYTES),
        ];
        // The first request asks for 3 bodies. Serving answers with one, which
        // is larger than the requester's stream window. Every later request is
        // for an unavailable height.
        let large = mainnet_block(&BLOCK_MAINNET_1046401_BYTES);
        let source = FixtureBlockSource::new(
            (1..=3)
                .map(|height| (block::Height(height), large.clone()))
                .collect(),
        );
        let reads = source.reads.clone();
        // Each queued response reserves output for its whole cap, at least
        // RESPONSE_BYTES, until written. So at most `bound` can be produced.
        let bound = usize::try_from(sizing::peer_output_bytes() / u64::from(RESPONSE_BYTES))?;
        assert!(bound < usize::try_from(REQUESTS)?);

        let mut cluster = ZakuraTestCluster::new();
        let victim = cluster
            .spawn_node_with_builder(
                getblocks_victim(
                    getblocks_limits(),
                    Some((block::Height(3), blocks[2].hash())),
                )
                .block_sync_config(ZakuraBlockSyncConfig {
                    max_response_bytes: RESPONSE_BYTES,
                    ..ZakuraBlockSyncConfig::default()
                })
                .block_range_source(Arc::new(source)),
            )
            .await?;
        let victim = cluster.node(victim);

        let quic = QuicConfig {
            stream_receive_window_bytes: 64 * 1024,
            receive_window_bytes: 128 * 1024,
            ..victim.limits().quic.clone()
        };
        assert!(block_size(&large) > quic.stream_receive_window_bytes);
        let peer = HostilePeer::connect_native_with_transport(
            victim,
            collision_winning_seeds(1)[0],
            ZAKURA_CAP_BLOCK_SYNC_REGULATED,
            quic,
        )
        .await?;
        let version = ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION;
        for request in 0..REQUESTS {
            let (start_height, count) = if request == 0 {
                (block::Height(1), 3)
            } else {
                (block::Height(request + 3), 1)
            };
            peer.send_ordered_raw_frame_with_version(
                ZAKURA_STREAM_BLOCK_SYNC,
                version,
                BlockSyncMessage::GetBlocks {
                    start_height,
                    count,
                }
                .encode_frame()?,
            )
            .await?;
        }

        // Production must approach the bound, then stop there without reads.
        // Frame overhead adds a few bytes per cap, so it settles just below.
        await_until(
            "serving approaches its output bound",
            Duration::from_secs(5),
            || reads.load(Ordering::SeqCst) * 10 >= bound * 9,
        )
        .await?;
        let mut settled = reads.load(Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut unchanged = 0;
            while unchanged < 10 {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let now = reads.load(Ordering::SeqCst);
                unchanged = if now == settled { unchanged + 1 } else { 0 };
                settled = now;
            }
        })
        .await
        .map_err(|_| -> BoxError { "serving production did not settle".into() })?;
        assert!(
            settled < usize::try_from(REQUESTS)?,
            "an unread response stops new production: {settled} reads"
        );

        // Reading releases output, so every request completes exactly once.
        // GetBlocks writes responses as they are ready, so empty responses
        // that finish before the large one are written ahead of it.
        let mut early = None;
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut bodies = 0;
            let mut ended = vec![false; usize::try_from(REQUESTS)?];
            let mut endings = 0;
            while endings < REQUESTS {
                let frame = peer
                    .recv_ordered_frame_with_version(ZAKURA_STREAM_BLOCK_SYNC, version)
                    .await?;
                let request = match BlockSyncMessage::decode_frame(frame)? {
                    BlockSyncMessage::Status(_) => continue,
                    BlockSyncMessage::Block(_) => {
                        early.get_or_insert(endings);
                        bodies += 1;
                        continue;
                    }
                    BlockSyncMessage::BlocksDone {
                        start_height: block::Height(1),
                        returned: 1,
                    } => {
                        assert_eq!(bodies, 1, "the large response ends after its body");
                        0
                    }
                    BlockSyncMessage::RangeUnavailable {
                        start_height,
                        count: 1,
                    } => start_height.0 - 3,
                    message => return Err(format!("unexpected message {message:?}").into()),
                };
                let request = usize::try_from(request)?;
                assert!(!std::mem::replace(&mut ended[request], true));
                endings += 1;
            }
            assert_eq!(bodies, 1, "a three-block request gets a one-block prefix");
            Ok::<_, BoxError>(())
        })
        .await
        .map_err(|_| -> BoxError { "reading did not resume serving".into() })??;
        // Only responses written ahead of the stalled body released their
        // output before reading resumed, so the rest stayed within the bound.
        let early = usize::try_from(early.expect("the large response has bodies"))?;
        assert!(
            settled - early <= bound,
            "an unread response stops new production: {settled} reads, \
             {early} written before the stall, bound {bound}"
        );
        assert_eq!(reads.load(Ordering::SeqCst), usize::try_from(REQUESTS)?);
        Ok(())
    }

    /// Serves the same serialized body at every requested height, within the
    /// byte budget.
    #[derive(Debug)]
    struct EndlessBlockSource(Vec<u8>);

    impl EndlessBlockSource {
        /// Creates a source that serves the serialization of `block`.
        fn new(block: &block::Block) -> Self {
            Self(
                block
                    .zcash_serialize_to_vec()
                    .expect("test block serializes"),
            )
        }
    }

    impl crate::zakura::BlockRangeSource for EndlessBlockSource {
        fn read(
            &self,
            request: crate::zakura::BlockRangeRead,
        ) -> futures::future::BoxFuture<
            'static,
            Result<crate::zakura::BlockRangeReadResult, BoxError>,
        > {
            let mut selected = Vec::new();
            if request.lease.try_start() {
                let size = u32::try_from(self.0.len()).expect("test block size fits u32");
                let fits = request.max_body_bytes / size.max(1);
                for offset in 0..request.count.min(fits) {
                    let height = block::Height(request.start_height.0 + offset);
                    selected.push((height, self.0.clone()));
                }
            }
            Box::pin(async move {
                Ok(crate::zakura::BlockRangeReadResult {
                    blocks: selected,
                    lease: request.lease,
                })
            })
        }
    }

    /// Manual evidence for regulated GetBlocks throughput over native QUIC.
    ///
    /// One requester pipelines GetBlocks against a production regulated server
    /// for each body size, blocks per request and in-flight depth, and reports
    /// body throughput. Production advertises one block per response, so small
    /// bodies show whether the advertised in-flight limit caps throughput. Add
    /// round-trip delay outside the test, for example with
    /// `tc qdisc add dev lo root netem delay 250ms`, and pass the resulting RTT
    /// in `ZAKURA_PROBE_RTT_MS` so the report can show the in-flight ceiling.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "manual throughput evidence; requires ZAKURA_RTT_PROBE=1"]
    #[allow(clippy::print_stderr)]
    async fn regulated_getblocks_throughput_probe() -> Result<(), BoxError> {
        use crate::zakura::{
            ZAKURA_CAP_BLOCK_SYNC_REGULATED, ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION,
        };
        use tokio::sync::Semaphore;
        if std::env::var_os("ZAKURA_RTT_PROBE").is_none() {
            return Ok(());
        }
        let env = |name: &str, default: u64| -> u64 {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(default)
        };
        let seconds = env("ZAKURA_PROBE_SECS", 15);
        let rtt_ms = env("ZAKURA_PROBE_RTT_MS", 0);
        let small = mainnet_block(&BLOCK_MAINNET_1_BYTES);
        let large = mainnet_block(&BLOCK_MAINNET_1046401_BYTES);
        let version = ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION;
        // `u32::MAX` means the server's advertised in-flight limit.
        let cases = [
            (&small, 1_u32, 64_u32),
            (&small, 1, 512),
            (&small, 1, u32::MAX),
            (&large, 1, 64),
            (&large, 1, 512),
            (&large, 1, u32::MAX),
        ];
        let seeds = collision_winning_seeds(cases.len());
        for (case, (body, count, depth)) in cases.into_iter().enumerate() {
            // Production limits, apart from the test node's connection cap.
            let limits = ZakuraLocalLimits {
                max_connections: 16,
                ..ZakuraLocalLimits::from_config(&Config::default())
            };
            let mut cluster = ZakuraTestCluster::new();
            let victim = cluster
                .spawn_node_with_builder(
                    getblocks_victim(limits, None)
                        .block_range_source(Arc::new(EndlessBlockSource::new(body))),
                )
                .await?;
            let victim = cluster.node(victim);
            let peer = HostilePeer::connect_native_with_capabilities(
                victim,
                seeds[case],
                ZAKURA_CAP_BLOCK_SYNC_REGULATED,
            )
            .await?;
            let max_frame = peer.max_frame_bytes();
            let (mut send, mut recv) = peer
                .split_ordered_stream(ZAKURA_STREAM_BLOCK_SYNC, version)
                .await?;
            let advertised = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let frame = HostilePeer::read_frame(&mut recv, max_frame).await?;
                    if let BlockSyncMessage::Status(status) = BlockSyncMessage::decode_frame(frame)?
                    {
                        return Ok::<_, BoxError>(status);
                    }
                }
            })
            .await
            .map_err(|_| -> BoxError { "no Status from the server".into() })??;
            let count = count.min(advertised.max_blocks_per_response);
            let depth = depth.min(advertised.max_inflight_requests);
            let window = Arc::new(Semaphore::new(usize::try_from(depth)?));
            let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let writer = {
                let (window, stop) = (window.clone(), stop.clone());
                tokio::spawn(async move {
                    let mut start = 1_u32;
                    while !stop.load(Ordering::Relaxed) {
                        window.acquire().await?.forget();
                        let frame = BlockSyncMessage::GetBlocks {
                            start_height: block::Height(start),
                            count,
                        }
                        .encode_frame()?;
                        send.write_all(&frame.encode(max_frame)?).await?;
                        start += count;
                    }
                    Ok::<_, BoxError>(())
                })
            };
            // Count only after a warm-up, so slow start does not dominate.
            let warmup = tokio::time::Instant::now() + Duration::from_secs(2);
            let deadline = warmup + Duration::from_secs(seconds);
            let (mut bodies, mut body_bytes, mut endings) = (0_u64, 0_u64, 0_u64);
            let mut cpu_start = None;
            while tokio::time::Instant::now() < deadline {
                let Ok(frame) = tokio::time::timeout_at(
                    deadline,
                    HostilePeer::read_frame(&mut recv, max_frame),
                )
                .await
                else {
                    break;
                };
                let frame = frame?;
                let counted = tokio::time::Instant::now() >= warmup;
                if counted && cpu_start.is_none() {
                    cpu_start = Some(zakura_test::process_stats::cpu_seconds());
                }
                match frame.message_type {
                    3 if counted => {
                        bodies += 1;
                        body_bytes += u64::try_from(frame.payload.len())?;
                    }
                    4 | 5 => {
                        window.add_permits(1);
                        endings += u64::from(counted);
                    }
                    _ => {}
                }
            }
            stop.store(true, Ordering::Relaxed);
            window.add_permits(1);
            let cpu = match (
                cpu_start.flatten(),
                zakura_test::process_stats::cpu_seconds(),
            ) {
                (Some(start), Some(end)) => format!("{:.2}", end - start),
                _ => "n/a".into(),
            };
            // Float conversions are lossy only far beyond these counts.
            let mb_per_second = body_bytes as f64 / 1e6 / seconds as f64;
            // The in-flight ceiling: depth responses per round trip.
            let ceiling = if rtt_ms == 0 || endings == 0 {
                "n/a".into()
            } else {
                let per_response = body_bytes as f64 / endings as f64;
                format!(
                    "{:.1}",
                    f64::from(depth) * per_response / (rtt_ms as f64 / 1000.0) / 1e6
                )
            };
            eprintln!(
                "getblocks_probe rtt_ms={rtt_ms} body_bytes={} blocks_per_request={count} depth={depth} \
                 advertised_inflight={} seconds={seconds} responses={endings} bodies={bodies} \
                 body_mb_per_s={mb_per_second:.2} inflight_ceiling_mb_per_s={ceiling} cpu_s={cpu}",
                block_size(body),
                advertised.max_inflight_requests
            );
            writer.abort();
        }
        Ok(())
    }

    /// A regulated Status burst, including one past the receiver's cadence
    /// capacity, is recorded rather than treated as a violation. The 5 s bounds
    /// assume a local loopback connection and an otherwise idle runtime.
    #[tokio::test]
    async fn regulated_status_bursts_keep_the_connection_serving() -> Result<(), BoxError> {
        use crate::zakura::{
            ZAKURA_CAP_BLOCK_SYNC_REGULATED, ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION,
        };
        let _guard = zakura_test::init();
        let blocks = [
            mainnet_block(&BLOCK_MAINNET_1_BYTES),
            mainnet_block(&BLOCK_MAINNET_2_BYTES),
            mainnet_block(&BLOCK_MAINNET_3_BYTES),
        ];
        let served = blocks
            .iter()
            .zip(1..)
            .map(|(block, height)| (block::Height(height), block.clone()))
            .collect();
        let limits = ZakuraLocalLimits {
            message_rate_per_second: 64,
            stream_open_rate_per_second: 64,
            ..getblocks_limits()
        };
        let mut cluster = ZakuraTestCluster::new();
        let victim = cluster
            .spawn_node_with_builder(
                getblocks_victim(limits, Some((block::Height(3), blocks[2].hash())))
                    .block_range_source(Arc::new(FixtureBlockSource::new(served))),
            )
            .await?;
        let victim = cluster.node(victim);
        let peer = HostilePeer::connect_native_with_capabilities(
            victim,
            collision_winning_seeds(1)[0],
            ZAKURA_CAP_BLOCK_SYNC_REGULATED,
        )
        .await?;
        let version = ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION;

        // 22 is the receiver capacity: 20 buffered over an outage, the initial
        // Status and one of jitter. The second burst exhausts it.
        for burst in [22, 8] {
            for _ in 0..burst {
                peer.send_ordered_raw_frame_with_version(
                    ZAKURA_STREAM_BLOCK_SYNC,
                    version,
                    BlockSyncMessage::Status(BlockSyncStatus::default()).encode_frame()?,
                )
                .await?;
            }
            peer.send_ordered_raw_frame_with_version(
                ZAKURA_STREAM_BLOCK_SYNC,
                version,
                BlockSyncMessage::GetBlocks {
                    start_height: block::Height(1),
                    count: 3,
                }
                .encode_frame()?,
            )
            .await?;
            let (hashes, ending) = read_getblocks_response(
                &peer,
                version,
                &format!("the response after a {burst}-Status burst"),
            )
            .await?;
            // Serving advertises one block per response, so a three-block
            // request gets a one-block prefix.
            assert_eq!(hashes, vec![blocks[0].hash()]);
            assert_eq!(
                ending,
                BlockSyncMessage::BlocksDone {
                    start_height: block::Height(1),
                    returned: 1,
                }
            );
        }
        Ok(())
    }

    /// Serves fixed bodies at fixed heights, without checking that they match.
    #[derive(Debug)]
    struct FixtureBlockSource {
        blocks: Vec<(block::Height, Vec<u8>)>,
        /// Counts the reads that serving starts, after it reserves output capacity.
        reads: Arc<AtomicUsize>,
    }

    impl FixtureBlockSource {
        /// Creates a source serving the serialization of `blocks`, with a zero
        /// read count.
        fn new(blocks: Vec<(block::Height, Arc<block::Block>)>) -> Self {
            Self {
                blocks: blocks
                    .into_iter()
                    .map(|(height, block)| {
                        let bytes = block
                            .zcash_serialize_to_vec()
                            .expect("test block serializes");
                        (height, bytes)
                    })
                    .collect(),
                reads: Arc::default(),
            }
        }
    }

    impl crate::zakura::BlockRangeSource for FixtureBlockSource {
        fn read(
            &self,
            request: crate::zakura::BlockRangeRead,
        ) -> futures::future::BoxFuture<
            'static,
            Result<crate::zakura::BlockRangeReadResult, BoxError>,
        > {
            self.reads.fetch_add(1, Ordering::SeqCst);
            let mut selected = Vec::new();
            let mut remaining = request.max_body_bytes;
            if request.lease.try_start() {
                for (height, bytes) in &self.blocks {
                    let height = *height;
                    if height < request.start_height {
                        continue;
                    }
                    if selected.len() >= usize::try_from(request.count).unwrap() {
                        break;
                    }
                    let size = u32::try_from(bytes.len()).expect("test block size fits u32");
                    let Some(left) = remaining.checked_sub(size) else {
                        break;
                    };
                    remaining = left;
                    selected.push((height, bytes.clone()));
                }
            }
            Box::pin(async move {
                Ok(crate::zakura::BlockRangeReadResult {
                    blocks: selected,
                    lease: request.lease,
                })
            })
        }
    }

    /// The first `count` seeds whose node ids win the ordered-stream collision
    /// against the seed-60 victim, so the victim adopts their streams.
    fn collision_winning_seeds(count: usize) -> Vec<u64> {
        let victim_id = LocalEndpointFactory::secret_key(60).public();
        (61..)
            .filter(|seed| {
                i_open_collision_winner(
                    &LocalEndpointFactory::secret_key(*seed).public(),
                    &victim_id,
                )
            })
            .take(count)
            .collect()
    }

    /// Test-node limits with the small inbound queue the GetBlocks tests use.
    fn getblocks_limits() -> ZakuraLocalLimits {
        ZakuraLocalLimits {
            max_connections: 16,
            max_pending_handshakes: 8,
            max_open_streams: 16,
            max_inbound_queue_depth: 8,
            ..ZakuraLocalLimits::from_config(&Config::default())
        }
    }

    /// Builds the seed-60 GetBlocks victim at genesis, with header tip `target`.
    fn getblocks_victim(
        limits: ZakuraLocalLimits,
        target: Option<(block::Height, block::Hash)>,
    ) -> ZakuraTestNodeBuilder {
        let anchor = (block::Height(0), mainnet_genesis_hash());
        ZakuraTestNode::builder(60)
            .limits(limits)
            .header_sync_driver(
                e2e_network([3]),
                anchor,
                FullStateFrontiers {
                    finalized_height: block::Height(0),
                    verified_block_tip: block::Height(0),
                    verified_block_hash: anchor.1,
                },
                target,
            )
    }

    /// Reads `peer`'s block-sync frames until a response ending, skipping
    /// Status, and returns the body hashes and the ending. Bounded at 5 s.
    async fn read_getblocks_response(
        peer: &HostilePeer,
        version: u16,
        label: &str,
    ) -> Result<(Vec<block::Hash>, BlockSyncMessage), BoxError> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut hashes = Vec::new();
            loop {
                let frame = peer
                    .recv_ordered_frame_with_version(ZAKURA_STREAM_BLOCK_SYNC, version)
                    .await?;
                match BlockSyncMessage::decode_frame(frame)? {
                    BlockSyncMessage::Status(_) => {}
                    BlockSyncMessage::Block(block) => hashes.push(block.hash()),
                    ending @ (BlockSyncMessage::BlocksDone { .. }
                    | BlockSyncMessage::RangeUnavailable { .. }) => return Ok((hashes, ending)),
                    message => {
                        return Err(format!("unexpected message in {label}: {message:?}").into())
                    }
                }
            }
        })
        .await
        .map_err(|_| -> BoxError { format!("timed out reading {label}").into() })?
    }

    async fn native_getblocks_exchange(one_session_slot: bool) -> Result<(), BoxError> {
        let version = ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION;
        let _guard = zakura_test::init();
        let mut capture =
            TraceCapture::for_test_with_keep_override("native_getblocks_exchange", false)?;
        let blocks = vec![
            mainnet_block(&BLOCK_MAINNET_1_BYTES),
            mainnet_block(&BLOCK_MAINNET_2_BYTES),
            mainnet_block(&BLOCK_MAINNET_3_BYTES),
        ];
        // The victim serves bodies larger than zakura-quic's 64 KiB minimum
        // window, so an unread response stalls its writer. The raw requester
        // does not validate them, so one large block fills heights 1 to 3.
        let large = mainnet_block(&BLOCK_MAINNET_1046401_BYTES);
        let served_blocks: Vec<_> = (1..=3)
            .map(|height| (block::Height(height), large.clone()))
            .collect();
        // Serving answers the hostile peer's three-block request with one block.
        let served_hashes = vec![large.hash()];

        let limits = ZakuraLocalLimits {
            message_rate_per_second: 64,
            stream_open_rate_per_second: 64,
            ..getblocks_limits()
        };

        let block_sync_config = ZakuraBlockSyncConfig {
            max_inflight_block_bytes: u64::MAX,
            request_timeout: Duration::from_secs(300),
            peer_limits: ServicePeerLimits {
                max_inbound_peers: if one_session_slot {
                    1
                } else {
                    ServicePeerLimits::default().max_inbound_peers
                },
                max_outbound_peers: if one_session_slot {
                    1
                } else {
                    ServicePeerLimits::default().max_outbound_peers
                },
                inbound_queue_depth: 1,
                outbound_queue_depth: 1,
                ..ServicePeerLimits::default()
            },
            ..ZakuraBlockSyncConfig::default()
        };

        let mut cluster = ZakuraTestCluster::new();
        let builder = getblocks_victim(limits.clone(), Some((block::Height(3), blocks[2].hash())))
            .tracer(capture.tracer_for_node(60))
            .block_sync_config(block_sync_config)
            .block_range_source(Arc::new(FixtureBlockSource::new(served_blocks)))
            .quic_config(QuicConfig {
                send_window_bytes: 64 * 1024,
                ..limits.quic.clone()
            });
        let victim = cluster.spawn_node_with_builder(builder).await?;
        let victim = cluster.node(victim);
        assert!(
            victim.block_sync().is_some(),
            "block-sync handle should be enabled with the header-sync test driver"
        );

        let submitted = Arc::new(StdMutex::new(Vec::new()));
        let driver =
            drive_native_block_sync_actions(victim, blocks.clone(), submitted.clone()).await;
        // The victim also opens stream 6 for its own demand, so every peer
        // needs a collision-winning seed.
        let seeds = collision_winning_seeds(6);
        let hostile_seed = seeds[0];
        // Each served block exceeds this receive window. Leaving the response
        // unread must stall the serving writer while downloads still progress.
        let quic = QuicConfig {
            stream_receive_window_bytes: 64 * 1024,
            receive_window_bytes: 128 * 1024,
            ..victim.limits().quic.clone()
        };
        assert!(block_size(&large) > quic.stream_receive_window_bytes);
        let hostile = HostilePeer::connect_native_with_transport(
            victim,
            hostile_seed,
            ZAKURA_CAP_BLOCK_SYNC_REGULATED,
            quic,
        )
        .await?;
        let hostile_peer = hostile.id()?;
        let peer_set = victim.supervisor().subscribe();
        await_until("block-sync peer registered", Duration::from_secs(5), || {
            peer_set.borrow().contains(&hostile_peer)
        })
        .await?;
        hostile
            .send_ordered_raw_frame_with_version(
                ZAKURA_STREAM_BLOCK_SYNC,
                version,
                // Serve only height 1, so the victim's single request is the
                // only one interleaved ahead of its unread serving response.
                BlockSyncMessage::Status(BlockSyncStatus {
                    servable_low: block::Height(1),
                    servable_high: block::Height(1),
                    tip_hash: blocks[0].hash(),
                    max_blocks_per_response: 3,
                    max_inflight_requests: 1,
                    max_response_bytes: MAX_BS_RESPONSE_BYTES,
                })
                .encode_frame()?,
            )
            .await?;

        let (start_height, count) = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let frame = hostile
                    .recv_ordered_frame_with_version(ZAKURA_STREAM_BLOCK_SYNC, version)
                    .await?;
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
        assert_eq!(
            count, 1,
            "the peer's three-block advertisement still gets one"
        );
        assert!(
            submitted
                .lock()
                .expect("submitted list mutex is not poisoned")
                .is_empty(),
            "the test-side responder must not send bodies or trigger submissions before it has \
             physically read GetBlocks from stream 6"
        );

        // Keep both directions active with bounded application queues.
        // Do not consume the served response until our own bodies reach the driver.
        hostile
            .send_ordered_raw_frame_with_version(
                ZAKURA_STREAM_BLOCK_SYNC,
                version,
                BlockSyncMessage::GetBlocks {
                    start_height,
                    count: 3,
                }
                .encode_frame()?,
            )
            .await?;

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
                .send_ordered_raw_frame_with_version(
                    ZAKURA_STREAM_BLOCK_SYNC,
                    version,
                    BlockSyncMessage::Block(block).encode_frame()?,
                )
                .await?;
        }
        hostile
            .send_ordered_raw_frame_with_version(
                ZAKURA_STREAM_BLOCK_SYNC,
                version,
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

        let (served, ending) =
            read_getblocks_response(&hostile, version, "the serving response").await?;
        assert_eq!(served, served_hashes);
        assert_eq!(
            ending,
            BlockSyncMessage::BlocksDone {
                start_height,
                returned: 1,
            }
        );

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
        let peer_a = ZakuraPeerId::new(node_a.node_addr().await.id.as_bytes().to_vec())?;
        let peer_b = ZakuraPeerId::new(node_b.node_addr().await.id.as_bytes().to_vec())?;

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
        // Reset the stream for retired version 8.
        // Deliver the current version on the same connection.
        let victim = cluster.node(victim_idx);
        let recorder = victim.recorder();
        let hostile = HostilePeer::connect_native_with_capabilities(
            victim,
            5,
            ZAKURA_CAP_HEADER_SYNC | ZAKURA_CAP_LEGACY_GOSSIP,
        )
        .await?;

        let retired_header_sync_v8 = b"header-sync-kind-5-version-8".to_vec();
        let current_header_sync = b"header-sync-kind-5-version-9".to_vec();
        hostile
            .send_frame_with_version(ZAKURA_STREAM_HEADER_SYNC, 8, retired_header_sync_v8.clone())
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
                .any(|m| m.frame.payload == retired_header_sync_v8),
            "retired header-sync v8 frame must be reset before delivery, got {delivered:?}"
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
        crate::zakura::wire_codec::encode_frame(&DiscoveryMessage::GetPeers(GetPeers {
            limit: 8,
            wanted_services: Vec::new(),
            exclude_node_ids: Vec::new(),
        }))
        .expect("empty GetPeers encodes")
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

    /// A peer that offers only header sync version 8 negotiates no header
    /// sync stream, and its connection stays usable.
    #[tokio::test]
    async fn a_version_8_peer_negotiates_no_header_sync_and_keeps_its_connection(
    ) -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let mut cluster = ZakuraTestCluster::new();
        let victim_idx = cluster.spawn_node(10).await?;
        let victim = cluster.node(victim_idx);
        let recorder = victim.recorder();
        // Bit 5 identified header sync version 8.
        let v8_peer = HostilePeer::connect_native_with_capabilities(
            victim,
            11,
            (1 << 5) | ZAKURA_CAP_LEGACY_GOSSIP,
        )
        .await?;

        let header_sync = b"header-sync-version-8".to_vec();
        v8_peer
            .send_frame_with_version(ZAKURA_STREAM_HEADER_SYNC, 8, header_sync.clone())
            .await?;
        let gossip = b"gossip-after-header-sync-version-8".to_vec();
        v8_peer
            .send_frame(ZAKURA_STREAM_GOSSIP, gossip.clone())
            .await?;
        await_until(
            "the version 8 peer's gossip is delivered",
            Duration::from_secs(5),
            || recorder.contains_payload(ZAKURA_STREAM_GOSSIP, &gossip),
        )
        .await?;
        assert!(
            !recorder.contains_payload(ZAKURA_STREAM_HEADER_SYNC, &header_sync),
            "a version 8 header-sync stream must be refused before delivery"
        );
        assert!(victim
            .supervisor()
            .registered_ids()
            .await
            .contains(&v8_peer.id()?));

        v8_peer.shutdown().await;
        cluster.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn discovery_stream_requires_negotiated_capability_and_responds() -> Result<(), BoxError>
    {
        let _guard = zakura_test::init();
        let victim = ZakuraTestNode::builder(26).spawn().await?;
        let victim_node_id = victim.node_addr().await.id;

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
            match crate::zakura::wire_codec::decode_frame::<DiscoveryMessage>(&frame)? {
                DiscoveryMessage::Hello { record } => {
                    assert_eq!(record.body.node_id, victim_node_id);
                    saw_hello = true;
                }
                DiscoveryMessage::Peers { records } => {
                    assert!(records.is_empty());
                    saw_peers = true;
                }
                // The victim's own discovery source also asks us for peers.
                DiscoveryMessage::GetPeers(_) => {}
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
        let b_id = b.node_addr().await.id;

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
    async fn discovery_stream_uses_transport_oversize_bound() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let mut capture = TraceCapture::for_test_with_keep_override(
            "discovery_stream_uses_transport_oversize_bound",
            false,
        )?;
        let mut limits = ZakuraLocalLimits::from_config(&Config::default());
        limits.max_connections = 16;
        limits.max_pending_handshakes = 8;
        limits.max_open_streams = 16;
        limits.max_inbound_queue_depth = 256;
        limits.stream_open_rate_per_second = 64;
        let victim = ZakuraTestNode::builder(29)
            .limits(limits)
            .tracer(capture.tracer_for_node(29))
            .spawn()
            .await?;

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
