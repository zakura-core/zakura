//! Golden tests for the exact HTTP responses of the RPC server.
//!
//! Each case sends raw HTTP/1.1 bytes to a running server, and the snapshots record the status
//! line, headers (except `date`), and body that come back. They cover JSON-RPC 1.0 and 2.0
//! requests, parameter decoding, notifications, batches, malformed input, HTTP methods and
//! content types, response size limits, and cookie authentication.

use std::{
    fmt::Write as _,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    time::Duration,
};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::watch,
    time::timeout,
};
use tower::buffer::Buffer;

use zakura_chain::{
    block::MAX_BLOCK_BYTES, chain_sync_status::MockSyncStatus, chain_tip::NoChainTip,
    parameters::Network,
};
use zakura_network::address_book_peers::MockAddressBookPeers;
use zakura_node_services::BoxError;
use zakura_test::mock_service::MockService;

use crate::{config::rpc::Config, methods::RpcImpl};

use super::super::RpcServer;

/// The largest request body the server accepts.
const MAX_REQUEST_BODY_SIZE: usize = MAX_BLOCK_BYTES as usize * 2 + 1024;

/// Larger response bodies are recorded as their length and hash.
const MAX_LOGGED_BODY_SIZE: usize = 2_000;

/// How long to wait for a whole response.
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);

/// An HTTP request to send.
struct Case {
    name: &'static str,
    method: &'static str,
    content_type: Option<&'static str>,
    body: String,
}

impl Case {
    fn post(name: &'static str, body: impl Into<String>) -> Self {
        Self {
            name,
            method: "POST",
            content_type: Some("application/json"),
            body: body.into(),
        }
    }

    fn content_type(mut self, content_type: Option<&'static str>) -> Self {
        self.content_type = content_type;
        self
    }

    fn method(mut self, method: &'static str) -> Self {
        self.method = method;
        self
    }
}

/// Starts a Regtest RPC server with mock services, returning its address.
async fn start_server(conf: Config) -> SocketAddr {
    let mempool: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
    let state: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
    let read_state: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
    let block_verifier_router: MockService<_, _, _, BoxError> =
        MockService::build().for_unit_tests();
    let (_tx, rx) = watch::channel(None);
    let (rpc_impl, _rpc_queue_task) = RpcImpl::new(
        Network::new_regtest(Default::default()),
        Default::default(),
        false,
        "RPC wire test",
        "RPC wire test",
        Buffer::new(mempool, 1),
        Buffer::new(state, 1),
        Buffer::new(read_state, 1),
        Buffer::new(block_verifier_router, 1),
        MockSyncStatus::default(),
        NoChainTip,
        MockAddressBookPeers::default(),
        rx,
        None,
    );

    let listen_addr = conf.listen_addr.expect("tests set a listen address");
    let server_task = RpcServer::start(rpc_impl, conf)
        .await
        .expect("RPC server should start");
    // The server runs until the test runtime shuts down.
    std::mem::forget(server_task);

    listen_addr
}

fn local_config() -> Config {
    let port = zakura_test::net::random_known_port();

    Config {
        listen_addr: Some(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port).into()),
        enable_cookie_auth: false,
        ..Config::default()
    }
}

/// Sends `case` on a new connection, returning the response without its `date` header, or
/// `<connection closed>` if the server closed the connection without responding.
async fn send(addr: SocketAddr, case: &Case, authorization: Option<&str>) -> String {
    let mut request = format!(
        "{} / HTTP/1.1\r\nhost: {addr}\r\nconnection: close\r\ncontent-length: {}\r\n",
        case.method,
        case.body.len()
    );
    if let Some(content_type) = case.content_type {
        writeln!(request, "content-type: {content_type}\r").expect("writing to a String works");
    }
    if let Some(authorization) = authorization {
        writeln!(request, "authorization: {authorization}\r").expect("writing to a String works");
    }
    request.push_str("\r\n");
    request.push_str(&case.body);

    let mut stream = TcpStream::connect(addr)
        .await
        .expect("test server should accept connections");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("test server should read requests");

    let mut response = Vec::new();
    timeout(RESPONSE_TIMEOUT, stream.read_to_end(&mut response))
        .await
        .unwrap_or_else(|_| panic!("{}: the response should arrive in time", case.name))
        .unwrap_or_else(|error| panic!("{}: the response should be readable: {error}", case.name));

    if response.is_empty() {
        return "<connection closed>".to_string();
    }

    let response = String::from_utf8(response).expect("responses are UTF-8");
    let (head, body) = response
        .split_once("\r\n\r\n")
        .expect("responses have a header section");

    let mut lines = head.lines();
    let mut output = lines
        .next()
        .expect("responses have a status line")
        .to_string();
    for header in lines.filter(|line| !line.to_ascii_lowercase().starts_with("date:")) {
        output.push('\n');
        output.push_str(&header.to_ascii_lowercase());
    }
    output.push_str("\n\n");
    if body.len() > MAX_LOGGED_BODY_SIZE {
        write!(
            output,
            "<{} bytes, FNV-1a {:016x}>",
            body.len(),
            fnv1a(body)
        )
        .expect("writing to a String works");
    } else {
        output.push_str(body);
    }

    output
}

/// Sends every case, returning a transcript of the requests and responses.
async fn transcript(addr: SocketAddr, cases: &[Case], authorization: Option<&str>) -> String {
    let mut output = String::new();

    for case in cases {
        let response = send(addr, case, authorization).await;
        let body = if case.body.len() > 200 {
            format!("<{} bytes>", case.body.len())
        } else {
            case.body.clone()
        };
        write!(
            output,
            "=== {}\n{} content-type={:?}\n{body}\n---\n{response}\n\n",
            case.name, case.method, case.content_type
        )
        .expect("writing to a String works");
    }

    output
}

/// The 64-bit FNV-1a hash of `text`.
fn fnv1a(text: &str) -> u64 {
    text.bytes().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// A `getstandardfee` call padded with an ignored parameter to exactly `length` bytes.
fn call_with_length(length: usize) -> String {
    let prefix = r#"{"jsonrpc":"2.0","method":"getstandardfee","params":[""#;
    let suffix = r#""],"id":1}"#;

    format!(
        "{prefix}{}{suffix}",
        "0".repeat(length - prefix.len() - suffix.len())
    )
}

fn call(method: &str, params: &str, id: &str) -> String {
    format!(r#"{{"jsonrpc":"2.0","method":"{method}","params":{params},"id":{id}}}"#)
}

#[tokio::test]
async fn json_rpc_requests() {
    let _init_guard = zakura_test::init();
    let addr = start_server(local_config()).await;

    let cases = [
        // Versions
        Case::post("2.0 call", call("getstandardfee", "[]", "1")),
        Case::post(
            "2.0 call without params",
            r#"{"jsonrpc":"2.0","method":"getstandardfee","id":1}"#,
        ),
        Case::post(
            "bitcoind 1.0 call",
            r#"{"method":"getstandardfee","params":[],"id":1}"#,
        ),
        Case::post(
            "lightwalletd 1.0 call",
            r#"{"jsonrpc":"1.0","method":"getstandardfee","params":[],"id":"lwd"}"#,
        ),
        Case::post(
            "1.0 call without params",
            r#"{"jsonrpc":"1.0","method":"getstandardfee","id":1}"#,
        ),
        Case::post(
            "bitcoind call without params",
            r#"{"method":"getstandardfee","id":1}"#,
        ),
        Case::post(
            "unknown version",
            r#"{"jsonrpc":"3.0","method":"getstandardfee","params":[],"id":1}"#,
        ),
        Case::post(
            "bitcoind error",
            r#"{"method":"getblockcount","params":[],"id":1}"#,
        ),
        Case::post(
            "lightwalletd error",
            r#"{"jsonrpc":"1.0","method":"getblockcount","params":[],"id":1}"#,
        ),
        // Request IDs
        Case::post("string id", call("getstandardfee", "[]", r#""abc""#)),
        Case::post(
            "escaped string id",
            call("getstandardfee", "[]", r#""a\"bc""#),
        ),
        Case::post("null id", call("getstandardfee", "[]", "null")),
        Case::post("zero id", call("getstandardfee", "[]", "0")),
        Case::post(
            "max id",
            call("getstandardfee", "[]", "18446744073709551615"),
        ),
        Case::post(
            "too large id",
            call("getstandardfee", "[]", "18446744073709551616"),
        ),
        Case::post("negative id", call("getstandardfee", "[]", "-1")),
        Case::post("fractional id", call("getstandardfee", "[]", "1.5")),
        Case::post("exponent id", call("getstandardfee", "[]", "1e2")),
        Case::post("boolean id", call("getstandardfee", "[]", "true")),
        Case::post("object id", call("getstandardfee", "[]", "{}")),
        Case::post("array id", call("getstandardfee", "[]", "[1]")),
        // Notifications are not run.
        Case::post(
            "notification",
            r#"{"jsonrpc":"2.0","method":"getstandardfee","params":[]}"#,
        ),
        Case::post(
            "notification for unknown method",
            r#"{"jsonrpc":"2.0","method":"nosuchmethod"}"#,
        ),
        Case::post(
            "bitcoind notification",
            r#"{"method":"getstandardfee","params":[]}"#,
        ),
        // Methods
        Case::post("unknown method", call("nosuchmethod", "[]", "1")),
        Case::post("empty method", call("", "[]", "1")),
        Case::post("escaped method", call(r"getstandardfee", "[]", "1")),
        Case::post("method case", call("GetStandardFee", "[]", "1")),
        Case::post(
            "numeric method",
            r#"{"jsonrpc":"2.0","method":1,"params":[],"id":1}"#,
        ),
        Case::post("missing method", r#"{"jsonrpc":"2.0","params":[],"id":1}"#),
        Case::post(
            "unknown field",
            r#"{"jsonrpc":"2.0","method":"getstandardfee","params":[],"id":1,"extra":true}"#,
        ),
        Case::post(
            "duplicate field",
            r#"{"jsonrpc":"2.0","method":"getstandardfee","method":"getblockcount","id":1}"#,
        ),
        Case::post("sync method", call("getblockcount", "[]", "1")),
        Case::post("sync method with result", call("rpc.discover", "[]", "1")),
        // Parameters
        Case::post(
            "extra params are ignored",
            call("getstandardfee", "[1, 2]", "1"),
        ),
        Case::post(
            "object params with no args",
            call("getstandardfee", "{}", "1"),
        ),
        Case::post(
            "object params with unknown key",
            call("getstandardfee", r#"{"a":1}"#, "1"),
        ),
        Case::post(
            "string params with no args",
            call("getstandardfee", r#""x""#, "1"),
        ),
        Case::post(
            "null params with no args",
            call("getstandardfee", "null", "1"),
        ),
        Case::post(
            "number params with no args",
            call("getstandardfee", "7", "1"),
        ),
        Case::post(
            "positional param",
            call(
                "validateaddress",
                r#"["tmRGc4CD1UyUdbSJmTUzcB6oDqk4qUaHnnh"]"#,
                "1",
            ),
        ),
        Case::post(
            "invalid address",
            call("validateaddress", r#"["abc"]"#, "1"),
        ),
        Case::post(
            "named param",
            call("validateaddress", r#"{"address":"abc"}"#, "1"),
        ),
        Case::post(
            "named param with impl name",
            call("validateaddress", r#"{"raw_address":"abc"}"#, "1"),
        ),
        Case::post(
            "named param with extra key",
            call("validateaddress", r#"{"address":"abc","x":1}"#, "1"),
        ),
        Case::post(
            "named param duplicated",
            call(
                "validateaddress",
                r#"{"address":"abc","address":"def"}"#,
                "1",
            ),
        ),
        Case::post("missing param", call("validateaddress", "[]", "1")),
        Case::post(
            "missing params member",
            r#"{"jsonrpc":"2.0","method":"validateaddress","id":1}"#,
        ),
        Case::post("null params", call("validateaddress", "null", "1")),
        Case::post("wrong param type", call("validateaddress", "[1]", "1")),
        Case::post(
            "null required param",
            call("validateaddress", "[null]", "1"),
        ),
        Case::post(
            "extra positional param",
            call("validateaddress", r#"["abc", 2]"#, "1"),
        ),
        Case::post("string params", call("validateaddress", r#""abc""#, "1")),
        Case::post("number params", call("validateaddress", "5", "1")),
        Case::post("empty named params", call("validateaddress", "{}", "1")),
        Case::post(
            "spaced params",
            r#"{"jsonrpc":"2.0","method":"validateaddress","params":[ "abc" , 1 ],"id":1}"#,
        ),
        Case::post(
            "bitcoind spaced empty params",
            r#"{"method":"getblocksubsidy","params":[ ],"id":1}"#,
        ),
        Case::post(
            "2.0 spaced empty params",
            r#"{"jsonrpc":"2.0","method":"getblocksubsidy","params":[ ],"id":1}"#,
        ),
        Case::post(
            "unknown-version spaced empty params",
            r#"{"jsonrpc":"2.0","method":"getblocksubsidy","params":[ ],"id":[]}"#,
        ),
        Case::post("optional param omitted", call("getblocksubsidy", "[]", "1")),
        Case::post(
            "optional param null",
            call("getblocksubsidy", "[null]", "1"),
        ),
        Case::post("optional param given", call("getblocksubsidy", "[1]", "1")),
        Case::post(
            "optional param wrong type",
            call("getblocksubsidy", r#"["1"]"#, "1"),
        ),
        Case::post(
            "optional param negative",
            call("getblocksubsidy", "[-1]", "1"),
        ),
        Case::post(
            "optional named params empty",
            call("getblocksubsidy", "{}", "1"),
        ),
        Case::post(
            "optional named param",
            call("getblocksubsidy", r#"{"height":1}"#, "1"),
        ),
        Case::post(
            "optional named param null",
            call("getblocksubsidy", r#"{"height":null}"#, "1"),
        ),
        Case::post(
            "method invalid params error",
            call("z_listunifiedreceivers", r#"["abc"]"#, "1"),
        ),
        Case::post(
            "two optional params",
            call("getnetworksolps", r#"[1, "x"]"#, "1"),
        ),
        Case::post(
            "camelCase named param",
            call("getaddresstxids", r#"{"request":{"addresses":[]}}"#, "1"),
        ),
        Case::post(
            "named param with wrong case",
            call("validateaddress", r#"{"Address":"abc"}"#, "1"),
        ),
        Case::post(
            "lower camel case param",
            call("getnetworksolps", r#"{"numBlocks":"x"}"#, "1"),
        ),
        Case::post(
            "snake case param",
            call("getnetworksolps", r#"{"num_blocks":"x"}"#, "1"),
        ),
        Case::post(
            "param under unknown name and invalid param",
            call("getnetworksolps", r#"{"numblocks":"x","height":"y"}"#, "1"),
        ),
        Case::post(
            "param under two aliases",
            call("getnetworksolps", r#"{"num_blocks":1,"numBlocks":2}"#, "1"),
        ),
        Case::post(
            "underscore param by name",
            call(
                "sendrawtransaction",
                r#"{"raw_transaction_hex":"zz","_allow_high_fees":"bad"}"#,
                "1",
            ),
        ),
        Case::post(
            "underscore param in snake case",
            call(
                "sendrawtransaction",
                r#"{"rawTransactionHex":"zz","allow_high_fees":"bad"}"#,
                "1",
            ),
        ),
        Case::post(
            "underscore param in lower camel case",
            call(
                "sendrawtransaction",
                r#"{"raw_transaction_hex":"zz","allowHighFees":"bad"}"#,
                "1",
            ),
        ),
        Case::post(
            "escaped param name",
            call("getnetworksolps", r#"{"num\u0042locks":"x"}"#, "1"),
        ),
        Case::post(
            "invalid second positional param",
            call("getblock", r#"["1", "x"]"#, "1"),
        ),
        Case::post(
            "positional params with trailing garbage",
            r#"{"jsonrpc":"2.0","method":"getblock","params":["1", 1 x],"id":1}"#,
        ),
        Case::post(
            "unicode string id",
            call("getstandardfee", "[]", r#""\u00e9t\u00e9""#),
        ),
        // Batches
        Case::post(
            "batch",
            format!(
                "[{},{},{}]",
                call("getstandardfee", "[]", "1"),
                call("nosuchmethod", "[]", "2"),
                call("validateaddress", "[]", "3"),
            ),
        ),
        Case::post(
            "batch with notification and invalid entries",
            format!(
                r#"[{},{{"jsonrpc":"2.0","method":"getstandardfee"}},1,{{"id":5}},{{"jsonrpc":"1.0","method":"getstandardfee","id":6}},[],{}]"#,
                call("getstandardfee", "[]", "1"),
                call("getblockcount", "[]", r#""last""#),
            ),
        ),
        Case::post("empty batch", "[]"),
        Case::post(
            "batch of notifications",
            r#"[{"jsonrpc":"2.0","method":"getstandardfee"},{"jsonrpc":"2.0","method":"x"}]"#,
        ),
        Case::post(
            "bitcoind batch",
            r#"[{"method":"getstandardfee","params":[],"id":1}]"#,
        ),
        Case::post("batch of a number", "[1]"),
        Case::post(
            "largest batch",
            format!(
                "[{}]",
                vec![call("getstandardfee", "[]", "1"); 1_000].join(",")
            ),
        ),
        Case::post(
            "too large batch",
            format!("[{}]", vec!["1"; 1_001].join(",")),
        ),
        // Malformed JSON
        Case::post("empty body", ""),
        Case::post("whitespace body", "  \n "),
        Case::post("truncated object", r#"{"jsonrpc":"2.0""#),
        Case::post("truncated batch", "[1,"),
        Case::post("not json", "abc"),
        Case::post("json string", r#""abc""#),
        Case::post("json null", "null"),
        Case::post(
            "leading whitespace",
            format!("{}{}", " ".repeat(10), call("getstandardfee", "[]", "1")),
        ),
        Case::post(
            "long leading whitespace",
            format!("{}{}", " ".repeat(200), call("getstandardfee", "[]", "1")),
        ),
        Case::post(
            "trailing data",
            format!("{} x", call("getstandardfee", "[]", "1")),
        ),
        Case::post(
            "two objects",
            format!("{0}{0}", call("getstandardfee", "[]", "1")),
        ),
        Case::post("object without fields", "{}"),
        Case::post("invalid request with id", r#"{"id":7}"#),
        Case::post("invalid request with string id", r#"{"id":"x","method":5}"#),
        Case::post("invalid request with bad id", r#"{"id":{},"method":5}"#),
        // HTTP
        Case::post("GET", call("getstandardfee", "[]", "1")).method("GET"),
        Case::post("PUT", call("getstandardfee", "[]", "1")).method("PUT"),
        Case::post("OPTIONS", "").method("OPTIONS"),
        Case::post("no content type", call("getstandardfee", "[]", "1")).content_type(None),
        Case::post("text/plain", call("getstandardfee", "[]", "1"))
            .content_type(Some("text/plain")),
        Case::post("text/plain charset", call("getstandardfee", "[]", "1"))
            .content_type(Some("text/plain; charset=utf-8")),
        Case::post(
            "application/json charset",
            call("getstandardfee", "[]", "1"),
        )
        .content_type(Some("application/json; charset=utf-8")),
        Case::post(
            "application/json uppercase",
            call("getstandardfee", "[]", "1"),
        )
        .content_type(Some("Application/JSON")),
        Case::post("application/json-rpc", call("getstandardfee", "[]", "1"))
            .content_type(Some("application/json-rpc")),
        Case::post(
            "application/json with spaces",
            call("getstandardfee", "[]", "1"),
        )
        .content_type(Some("application/json ; charset=utf-8")),
        Case::post("form content type", call("getstandardfee", "[]", "1"))
            .content_type(Some("application/x-www-form-urlencoded")),
        Case::post("xml content type", call("getstandardfee", "[]", "1"))
            .content_type(Some("text/xml")),
        Case::post("largest request", call_with_length(MAX_REQUEST_BODY_SIZE)),
        Case::post(
            "too large request",
            call_with_length(MAX_REQUEST_BODY_SIZE + 1),
        ),
    ];

    insta::assert_snapshot!(transcript(addr, &cases, None).await);
}

#[tokio::test]
async fn response_size_limits_and_cookie_authentication() {
    let _init_guard = zakura_test::init();

    let cookie_dir = tempfile::tempdir().expect("temporary cookie directory should be created");
    let conf = Config {
        enable_cookie_auth: true,
        cookie_dir: cookie_dir.path().to_path_buf(),
        max_response_body_size: 300,
        ..local_config()
    };
    let addr = start_server(conf.clone()).await;

    let cookie = std::fs::read_to_string(cookie_dir.path().join(&conf.cookie_file_name))
        .expect("the server writes its cookie");
    let authorization = format!("Basic {}", STANDARD.encode(cookie.trim()));

    let cases = [
        Case::post("small response", call("getstandardfee", "[]", "1")),
        Case::post("oversized response", call("rpc.discover", "[]", "1")),
        Case::post(
            "oversized bitcoind response",
            r#"{"method":"rpc.discover","params":[],"id":1}"#,
        ),
        Case::post(
            "batch under the limit",
            format!(
                "[{},{}]",
                call("getstandardfee", "[]", "1"),
                call("getstandardfee", "[]", "2")
            ),
        ),
        Case::post(
            "oversized batch response",
            format!(
                "[{}]",
                vec![call("getstandardfee", "[]", "1"); 10].join(",")
            ),
        ),
        Case::post(
            "batch with an oversized entry",
            format!(
                "[{},{}]",
                call("getstandardfee", "[]", "1"),
                call("rpc.discover", "[]", "2")
            ),
        ),
    ];

    let mut output = transcript(addr, &cases, Some(&authorization)).await;

    let unauthenticated = [Case::post(
        "no credentials",
        call("getstandardfee", "[]", "1"),
    )];
    output.push_str(&transcript(addr, &unauthenticated, None).await);

    let wrong_credentials = [Case::post(
        "wrong password",
        call("getstandardfee", "[]", "1"),
    )];
    output.push_str(
        &transcript(
            addr,
            &wrong_credentials,
            Some(&format!("Basic {}", STANDARD.encode("__cookie__:wrong"))),
        )
        .await,
    );

    insta::assert_snapshot!(output);
}
