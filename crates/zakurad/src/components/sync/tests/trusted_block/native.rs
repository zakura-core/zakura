//! Native block-sync enforcement, separate from the HTTP compatibility relay path.

use super::*;
use zakura_network::zakura::{
    testkit::ZakuraTestNode, BlockRangeRead, BlockRangeReadResult, BlockRangeSource,
    FullStateFrontiers,
};

#[derive(Debug)]
struct BodySource(Vec<u8>);

impl BlockRangeSource for BodySource {
    fn read(
        &self,
        request: BlockRangeRead,
    ) -> futures::future::BoxFuture<'static, Result<BlockRangeReadResult, BoxError>> {
        let blocks = if request.lease.try_start()
            && request.start_height == Height(1)
            && request.count > 0
            && self.0.len() <= usize::try_from(request.max_body_bytes).unwrap()
        {
            vec![(Height(1), self.0.clone())]
        } else {
            vec![]
        };
        Box::pin(async move {
            Ok(BlockRangeReadResult {
                blocks,
                lease: request.lease,
            })
        })
    }
}

/// Admit the target through the same durable header port used by header sync.
async fn select_header(node: &Node, block: &Block, body_size: usize) {
    use zakura_header_chain::{
        Frontier, HeaderWorkAuthority, SourceId, TargetCompletion, TreeAuxRecordV1,
    };
    use zakura_node_services::header_chain::{PrepareHeaderTarget, TargetEntry};
    let startup = node.native_startup.as_ref().unwrap();
    let snapshot = startup
        .header_chain_port
        .selected_chain()
        .borrow()
        .clone()
        .unwrap();
    let common_ancestor = Frontier::new(Height(0), node.network.genesis_hash());
    let target = Frontier::new(Height(1), block.hash());
    let owner = HeaderWorkAuthority::for_target(&snapshot, block.hash())
        .bind(1, std::num::NonZeroU64::new(1).unwrap());
    let prepared = startup
        .header_chain_port
        .prepare_header_target(PrepareHeaderTarget {
            source: SourceId::from_digest([42; 32]),
            owner: owner.into(),
            common_ancestor,
            target,
            entries: vec![TargetEntry {
                header: block.header.clone(),
                body_size: u32::try_from(body_size).unwrap(),
                tree_aux: Some(TreeAuxRecordV1 {
                    height: Height(1),
                    sapling_root: zakura_chain::sapling::tree::NoteCommitmentTree::default().root(),
                    orchard_root: zakura_chain::orchard::tree::NoteCommitmentTree::default().root(),
                    ironwood_root: zakura_chain::ironwood::tree::NoteCommitmentTree::default()
                        .root(),
                    sapling_tx_count: 0,
                    orchard_tx_count: 0,
                    ironwood_tx_count: 0,
                    auth_data_root: block.auth_data_root(),
                }),
            }]
            .try_into()
            .unwrap(),
            completion: TargetCompletion::TargetComplete { common_ancestor },
        })
        .await
        .unwrap();
    startup
        .header_chain_port
        .apply_header_target(prepared)
        .await
        .unwrap();
}

async fn native_invalid_body(tampered: bool) {
    let _init = zakura_test::init();
    let mut receiver = Node::with_stack(None, P2pStack::Dual).await;
    let valid = receiver.template().await;
    let mut invalid = faulty_body(&valid);
    if !tampered {
        Arc::make_mut(&mut invalid.header).merkle_root = invalid.transactions.iter().collect();
    }
    let header_block = if tampered { &valid } else { &invalid };
    let bytes = invalid.zcash_serialize_to_vec().unwrap();
    select_header(&receiver, header_block, bytes.len()).await;
    let supplier = ZakuraTestNode::builder(930)
        .header_sync_driver(
            receiver.network.clone(),
            (Height(0), receiver.network.genesis_hash()),
            FullStateFrontiers {
                finalized_height: Height(0),
                verified_block_tip: Height(1),
                verified_block_hash: header_block.hash(),
            },
            Some((Height(1), header_block.hash())),
        )
        .block_range_source(Arc::new(BodySource(bytes)))
        .spawn()
        .await
        .unwrap();
    // One dial avoids a maintained dialer's automatic reconnection masking closure.
    let endpoint = receiver.endpoint.as_ref().unwrap().clone();
    let dial_endpoint = endpoint.clone();
    let addr = supplier.node_addr().await;
    let limits = supplier.limits().clone();
    let dial = tokio::spawn(async move { dial_endpoint.dial(addr, &limits).await });
    let mut peers = endpoint.supervisor().subscribe();
    timeout(Duration::from_secs(20), async {
        while peers.borrow_and_update().is_empty() {
            peers.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    let violation = timeout(Duration::from_secs(20), receiver.native_violations.recv())
        .await
        .unwrap()
        .unwrap();
    if tampered {
        assert!(matches!(
            violation,
            zakura_network::zakura::BlockSyncMisbehavior::InvalidBlock
                | zakura_network::zakura::BlockSyncMisbehavior::BodyPayloadMismatch(_)
        ));
        timeout(Duration::from_secs(20), async {
            while !peers.borrow_and_update().is_empty() {
                peers.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        timeout(Duration::from_secs(20), dial)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            receiver.gate.entered.try_recv().is_err(),
            "tampering is rejected before consensus admission"
        );
    } else {
        let result = timeout(Duration::from_secs(20), receiver.gate.finished.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result, (invalid.hash(), false));
        assert!(
            matches!(
                violation,
                zakura_network::zakura::BlockSyncMisbehavior::ConsensusBodyInvalid(_)
            ),
            "consensus rejection is not tampered-body evidence"
        );
        assert_eq!(endpoint.supervisor().registered_ids().await.len(), 1);
        assert!(
            !dial.is_finished(),
            "consensus rejection must retain the connection"
        );
        dial.abort();
    }
    assert_eq!(receiver.tip.best_tip_height(), Some(Height(0)));
    assert!(!receiver.bans.is_banned(receiver.p2p.ip()));
    supplier.shutdown().await;
    endpoint.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn native_tampered_body_disconnects_supplier_without_legacy_ban() {
    native_invalid_body(true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn native_consensus_invalid_body_records_violation_without_disconnect_or_ban() {
    native_invalid_body(false).await;
}
