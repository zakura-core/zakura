//! Tests for block body delivery rows written by the peer message sender.

use zakura_jsonl_trace::JsonlTraceTables;

use crate::lifecycle_trace::{tests::capture, LifecycleTrace};

/// Upload progress is only sampled for the `p2p` table, so a node that does not record it never
/// duplicates a socket descriptor or starts a watch.
#[test]
fn only_the_p2p_table_consumes_upload_progress() {
    assert!(!LifecycleTrace::noop().records_p2p());
    let (gossip_only, _rx) = capture(JsonlTraceTables::from_names(["block_gossip"]).unwrap());
    assert!(!gossip_only.records_p2p());
    let (p2p, _rx) = capture(JsonlTraceTables::from_names(["p2p"]).unwrap());
    assert!(p2p.records_p2p());
}

#[cfg(target_os = "linux")]
mod linux {
    use std::{sync::Arc, time::Duration};

    use futures::StreamExt;
    use tokio::net::{TcpListener, TcpStream};
    use tokio_util::codec::Framed;
    use zakura_chain::{block, serialization::ZcashDeserializeInto};
    use zakura_jsonl_trace::{tables, JsonlTraceTables};

    use crate::{
        lifecycle_trace::{
            tests::{capture, drain, rows},
            ConnectionTrace,
        },
        peer::{
            connection::peer_tx::PeerTx,
            tcp_progress::{TcpSocket, UploadProgress},
        },
        protocol::external::{Codec, Message},
    };

    async fn connected_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("listener address");
        let (sender, accepted) = tokio::join!(TcpStream::connect(addr), listener.accept());
        (sender.expect("connect"), accepted.expect("accept").0)
    }

    fn sender(stream: TcpStream, trace: ConnectionTrace) -> PeerTx<Framed<TcpStream, Codec>> {
        let progress = TcpSocket::from_transport(&stream).map(UploadProgress::new);
        assert!(progress.is_some(), "a TCP stream on Linux has a socket");
        let mut codec = Codec::builder().finish();
        codec.reconfigure_full_body_len();
        let mut peer_tx = PeerTx::new(Framed::new(stream, codec), trace);
        peer_tx.set_upload_progress(progress);
        peer_tx
    }

    /// A block body send records the kernel's TCP state on its `send_done` row, and a
    /// `body_acked` row with the same `seq` once the peer acknowledged it. Other messages get
    /// neither.
    // Decoding a block uses `block_in_place`, which needs the multi-threaded runtime.
    #[tokio::test(flavor = "multi_thread")]
    async fn block_body_sends_record_tcp_state_and_acknowledgement() {
        let (trace, mut rx) = capture(JsonlTraceTables::from_names(["p2p"]).unwrap());
        let (stream, receiver) = connected_pair().await;
        let mut peer_tx = sender(stream, trace.connection(false, "uploader"));
        let mut codec = Codec::builder().finish();
        codec.reconfigure_full_body_len();
        let mut receiver = Framed::new(receiver, codec);

        let block: Arc<block::Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .expect("test vector is a valid block");
        peer_tx
            .send(Message::GetAddr)
            .await
            .expect("the send succeeds");
        peer_tx
            .send(Message::Block(block))
            .await
            .expect("the send succeeds");
        for _ in 0..2 {
            receiver
                .next()
                .await
                .expect("the receiver gets both messages")
                .expect("valid message");
        }

        let body_row = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let rows = rows(&drain(&mut rx), tables::P2P);
                if let Some(row) = rows.into_iter().find(|row| row["event"] == "body_acked") {
                    return row;
                }
            }
        })
        .await
        .expect("the body is acknowledged");
        assert_eq!(body_row["seq"], "1");
        assert_eq!(body_row["msg"], "block");
        assert_eq!(body_row["result"], "acked");
        assert!(!body_row["ack_us"].is_empty());
        assert!(!body_row["tcp_rtt_us"].is_empty());
        assert!(body_row["acked_bytes"].parse::<u64>().expect("integer") > 24);
        assert!(body_row["samples"].parse::<u64>().expect("integer") >= 1);
        drop(peer_tx);
    }

    /// The `send_done` rows: only the block body has TCP columns.
    #[tokio::test]
    async fn only_block_body_send_done_rows_have_tcp_columns() {
        let (trace, mut rx) = capture(JsonlTraceTables::from_names(["p2p"]).unwrap());
        let (stream, _receiver) = connected_pair().await;
        let mut peer_tx = sender(stream, trace.connection(false, "uploader"));
        let block: Arc<block::Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .expect("test vector is a valid block");

        peer_tx.send(Message::GetAddr).await.expect("send");
        peer_tx.send(Message::Block(block)).await.expect("send");

        let rows = rows(&drain(&mut rx), tables::P2P);
        let done: Vec<_> = rows
            .iter()
            .filter(|row| row["event"] == "send_done")
            .collect();
        assert_eq!(done.len(), 2);
        assert_eq!(done[0]["msg"], "getaddr");
        assert!(done[0]["tcp_cwnd"].is_empty());
        assert_eq!(done[1]["msg"], "block");
        for column in ["tcp_unacked", "tcp_cwnd", "tcp_rtt_us", "tcp_total_retrans"] {
            assert!(!done[1][column].is_empty(), "{column} is recorded");
        }
        assert!(done[1]["ack_us"].is_empty());
    }

    /// A sender whose trace does not record the `p2p` table never starts a watch, even with
    /// upload progress attached.
    #[tokio::test]
    async fn untraced_senders_start_no_watch() {
        let (stream, _receiver) = connected_pair().await;
        let mut peer_tx = sender(stream, ConnectionTrace::untraced());
        let block: Arc<block::Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .expect("test vector is a valid block");

        peer_tx.send(Message::Block(block)).await.expect("send");
        let progress = peer_tx.upload_progress.as_ref().expect("attached");
        assert_eq!(progress.watching(), 0);
    }
}
