use super::*;

#[tokio::test]
async fn tachyon_sync_rpc_distinguishes_invalid_unknown_and_unavailable_data() {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let mempool: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
        let state: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
        let mut read_state: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
        let (_tx, rx) = tokio::sync::watch::channel(None);
        let (rpc, queue) = mock_rpc(Mainnet, mempool, state, read_state.clone(), NoChainTip, rx);
        assert_eq!(
            rpc.get_tachyon_block("not-a-block".into())
                .await
                .unwrap_err()
                .code(),
            -8
        );

        let rpc = Arc::new(rpc);
        let task = {
            let rpc = rpc.clone();
            tokio::spawn(async move { rpc.get_tachyon_block("10".into()).await })
        };
        read_state
            .expect_request(ReadRequest::TachyonBlock(Height(10).into()))
            .await
            .respond(ReadResponse::TachyonBlock(None));
        assert_eq!(task.await.unwrap().unwrap_err().code(), -8);

        let task = tokio::spawn(async move { rpc.get_tachyon_block("10".into()).await });
        read_state
            .expect_request(ReadRequest::TachyonBlock(Height(10).into()))
            .await
            .respond_error("Tachyon block data unavailable: body pruned".into());
        let error = task.await.unwrap().unwrap_err();
        assert_eq!(error.code(), -1);
        assert!(error.message().contains("pruned"));
        queue.abort();
    })
    .await
    .expect("RPC tests finish within ten seconds");
}

#[tokio::test]
async fn tachyon_sync_rpc_returns_empty_block_data_from_one_state_request() {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let mempool: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
        let state: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
        let mut read_state: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
        let (_tx, rx) = tokio::sync::watch::channel(None);
        let (rpc, queue) = mock_rpc(Mainnet, mempool, state, read_state.clone(), NoChainTip, rx);
        let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
            .zcash_deserialize_into()
            .unwrap();
        let hash = block.hash();
        let anchor = zakura_chain::tachyon::Anchor::from(zcash_tachyon::Anchor::default());
        let task = tokio::spawn(async move { rpc.get_tachyon_block(hash.to_string()).await });
        read_state
            .expect_request(ReadRequest::TachyonBlock(hash.into()))
            .await
            .respond(ReadResponse::TachyonBlock(Some(
                zakura_state::TachyonBlock {
                    block,
                    height: Height(10),
                    activation_height: Height(10),
                    finalized: false,
                    anchor_before: anchor,
                    anchor_after: anchor,
                },
            )));
        let reply = task.await.unwrap().unwrap();
        assert_eq!(reply.hash, hash);
        assert_eq!(reply.pool_height, 0);
        assert!(!reply.finalized);
        assert!(reply.stamps.is_empty());
        assert_eq!(reply.anchor_after, anchor.0);
        let json = serde_json::to_value(reply).unwrap();
        assert_eq!(json["hash"], hash.to_string());
        assert_eq!(json["previousBlockHash"], "0".repeat(64));
        queue.abort();
    })
    .await
    .expect("RPC tests finish within ten seconds");
}
