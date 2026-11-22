//! A pool can withhold the end of a trusted upload until its primary accepts the solution.

use std::path::Path;

use rustls::pki_types::{pem::PemObject, CertificateDer, ServerName};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio_rustls::{client::TlsStream, TlsConnector};
use zakura_rpc::config::rpc::TlsConfig;

use super::*;

type Client = BufReader<TlsStream<TcpStream>>;

async fn connect(relay: &Node, tls: &TlsConfig) -> Client {
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(CertificateDer::from_pem_file(&tls.cert_file).unwrap())
        .unwrap();
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    let tcp = TcpStream::connect(relay.rpc).await.unwrap();
    BufReader::new(
        TlsConnector::from(Arc::new(config))
            .connect(ServerName::try_from("localhost").unwrap(), tcp)
            .await
            .unwrap(),
    )
}

/// Consume exactly one fixed-length HTTP response so the socket can serve the next request.
async fn response(client: &mut Client) -> Vec<u8> {
    let mut line = String::new();
    client.read_line(&mut line).await.unwrap();
    assert_eq!(line, "HTTP/1.1 200 OK\r\n");
    let mut length = None;
    loop {
        line.clear();
        assert_ne!(client.read_line(&mut line).await.unwrap(), 0);
        if line == "\r\n" {
            break;
        }
        let (name, value) = line.split_once(':').unwrap();
        if name.eq_ignore_ascii_case("content-length") {
            length = Some(value.trim().parse::<usize>().unwrap());
        }
    }
    let mut body = vec![0; length.expect("these RPC responses have a known length")];
    client.read_exact(&mut body).await.unwrap();
    body
}

async fn height(client: &mut Client, auth: &str) -> u64 {
    let body = r#"{"jsonrpc":"2.0","method":"getblockcount","params":[],"id":1}"#;
    let request = format!(
        "POST / HTTP/1.1\r\nHost: localhost\r\nAuthorization: {auth}\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    client.write_all(request.as_bytes()).await.unwrap();
    client.flush().await.unwrap();
    let result: serde_json::Value = serde_json::from_slice(&response(client).await).unwrap();
    result["result"].as_u64().unwrap()
}

async fn upload(client: &mut Client, auth: &str, bytes: &[u8]) {
    // In this test the interim response synchronizes with admission on the server.
    let head = format!(
        "POST /submittrustedblock HTTP/1.1\r\nHost: localhost\r\nAuthorization: {auth}\r\n\
         Content-Type: application/octet-stream\r\nTransfer-Encoding: chunked\r\n\
         Expect: 100-continue\r\n\r\n"
    );
    client.write_all(head.as_bytes()).await.unwrap();
    client.flush().await.unwrap();
    const CONTINUE: &[u8] = b"HTTP/1.1 100 Continue\r\n\r\n";
    let mut interim = [0; CONTINUE.len()];
    client.read_exact(&mut interim).await.unwrap();
    assert_eq!(&interim, CONTINUE);
    client
        .write_all(format!("{:x}\r\n", bytes.len()).as_bytes())
        .await
        .unwrap();
    client.write_all(bytes).await.unwrap();
    client.write_all(b"\r\n").await.unwrap();
    client.flush().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn tls_upload_waits_for_release_and_can_retry_after_disconnect() {
    let _init = zakura_test::init();
    timeout(Duration::from_secs(90), upload_and_release())
        .await
        .expect("the staged TLS upload and its retry finish");
}

async fn upload_and_release() {
    let primary = Node::new(None).await;
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("../zakura-rpc/src/server/tests");
    let tls = TlsConfig {
        cert_file: fixtures.join("handshake-cert.pem"),
        key_file: fixtures.join("handshake-key.pem"),
    };
    let mut relay = Node::with_network(
        None,
        P2pStack::Legacy,
        primary.network.clone(),
        zakura_chain::block::genesis::regtest_genesis_block(),
        Some(tls.clone()),
    )
    .await;
    let mut observer = WirePeer::connect(&relay).await;
    let template = primary.block_template().await;
    let mut block = proposal_block_from_template(
        &template,
        BlockTemplateTimeSource::MinTime,
        &primary.network,
    )
    .unwrap();
    // Precomputed (48,5) proof for height one at the template's minimum time, paying [7; 20].
    let header = Arc::make_mut(&mut block.header);
    header.nonce.0[0] = 5;
    header.solution = zakura_chain::work::equihash::Solution::from_bytes(
        &hex::decode("04ea63fcd28fd8b5a308304b7f43f5a5732c20deeffde4f4b64726339b0e37c3e58b43b7")
            .unwrap(),
    )
    .unwrap();
    header.solution.check(header, &primary.network).unwrap();
    let solution = serde_json::json!({"jsonrpc":"2.0","id":1,"method":"submitsolution","params":[{
        "workid": template.work_id(), "header": hex::encode(header.zcash_serialize_to_vec().unwrap())
    }]});
    let bytes = block.zcash_serialize_to_vec().unwrap();
    let hash = block.hash();
    relay.gate.hold();
    relay.hold_parent_lookup.send_replace(true);

    let mut aborted = connect(&relay, &tls).await;
    upload(&mut aborted, &relay.auth, &bytes).await;
    aborted.shutdown().await.unwrap();
    drop(aborted);
    assert!(fetch_from_peer(&relay, hash).await.is_none());

    let mut client = connect(&relay, &tls).await;
    assert_eq!(height(&mut client, &relay.auth).await, 0);
    upload(&mut client, &relay.auth, &bytes).await;
    let (status, verdict) = http(
        primary.rpc,
        &primary.auth,
        "/",
        "application/json",
        serde_json::to_vec(&solution).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let verdict: serde_json::Value = serde_json::from_slice(&verdict).unwrap();
    assert_eq!(
        verdict.get("result"),
        Some(&serde_json::Value::Null),
        "{verdict}"
    );
    assert_eq!(primary.tip.best_tip_hash(), Some(hash));
    assert!(fetch_from_peer(&relay, hash).await.is_none());
    assert!(relay.parent_lookups.try_recv().is_err());
    assert!(relay.gate.entered.try_recv().is_err());
    assert!(relay.pending.get(hash).is_none());
    assert_eq!(observer.count(hash, "inv"), 0);

    relay.hold_parent_lookup.send_replace(false);
    client.write_all(b"0\r\n\r\n").await.unwrap();
    client.flush().await.unwrap();
    assert_eq!(relay.gate.entered().await, hash);
    observer.announced(hash).await;
    observer.fetch(&block).await;
    relay.gate.release();
    assert_eq!(response(&mut client).await, b"accepted");
    assert_eq!(height(&mut client, &relay.auth).await, 1);
    assert_eq!(relay.tip.best_tip_hash(), Some(hash));
}
