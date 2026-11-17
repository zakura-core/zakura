use std::path::Path;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::*;

/// One parsed HTTP/1.1 response.
#[derive(Debug)]
struct Reply {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Reply {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

struct Server {
    address: SocketAddr,
    shutdown: CancellationToken,
    task: JoinHandle<()>,
}

impl Server {
    async fn start(directory: &Path) -> Self {
        let shutdown = CancellationToken::new();
        let (task, address) = start(
            "127.0.0.1:0".parse().unwrap(),
            directory.to_owned(),
            // Each test owns its pool, so parallel tests never see 503.
            FsThreads::new(crate::components::tracing::raw_endpoint::fs_threads::MAX_FS_THREADS),
            shutdown.clone(),
        )
        .await
        .unwrap();
        Self {
            address,
            shutdown,
            task,
        }
    }

    /// Send `method path` with `headers` and read the whole response.
    async fn send(&self, method: &str, path: &str, headers: &[(&str, &str)]) -> Reply {
        let mut request = format!("{method} {path} HTTP/1.1\r\nHost: {}\r\n", self.address);
        for (name, value) in headers {
            request.push_str(&format!("{name}: {value}\r\n"));
        }
        request.push_str("\r\n");
        let mut socket = TcpStream::connect(self.address).await.unwrap();
        socket.write_all(request.as_bytes()).await.unwrap();
        let mut raw = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), socket.read_to_end(&mut raw))
            .await
            .unwrap()
            .unwrap();
        parse_reply(&raw)
    }

    async fn get(&self, path: &str, headers: &[(&str, &str)]) -> Reply {
        self.send("GET", path, headers).await
    }

    async fn stop(self) {
        self.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(2), self.task)
            .await
            .unwrap()
            .unwrap();
    }
}

fn parse_reply(raw: &[u8]) -> Reply {
    let split = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("response has a header block");
    let head = std::str::from_utf8(&raw[..split]).unwrap();
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .unwrap()
        .split(' ')
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let headers = lines
        .map(|line| {
            let (name, value) = line.split_once(": ").unwrap();
            (name.to_owned(), value.to_owned())
        })
        .collect();
    Reply {
        status,
        headers,
        body: raw[split + 4..].to_vec(),
    }
}

fn append(path: &Path, bytes: &[u8]) {
    use std::io::Write;
    std::fs::OpenOptions::new()
        .append(true)
        .open(path)
        .unwrap()
        .write_all(bytes)
        .unwrap();
}

#[tokio::test]
async fn whole_files_heads_and_the_index() {
    let dir = tempfile::tempdir().unwrap();
    let data = b"{\"event\":1}\npartial";
    std::fs::write(dir.path().join("block_sync.jsonl"), data).unwrap();
    std::fs::write(dir.path().join("conn.jsonl"), b"").unwrap();
    let server = Server::start(dir.path()).await;

    let whole = server.get("/traces/block_sync", &[]).await;
    assert_eq!(whole.status, 200);
    assert_eq!(whole.body, data);
    assert_eq!(whole.header("content-type"), Some("application/x-ndjson"));
    assert_eq!(whole.header("cache-control"), Some("no-store"));
    assert_eq!(whole.header("accept-ranges"), Some("bytes"));
    let etag = whole.header("etag").unwrap().to_owned();

    let head = server.send("HEAD", "/traces/block_sync", &[]).await;
    assert_eq!(head.status, 200);
    assert_eq!(head.header("content-length"), Some("19"));
    assert_eq!(head.header("etag"), Some(etag.as_str()));
    assert!(head.body.is_empty());

    let empty = server.get("/traces/conn", &[]).await;
    assert_eq!((empty.status, empty.body.len()), (200, 0));
    assert_eq!(server.get("/traces/stream", &[]).await.status, 404);

    let index = server.get("/traces", &[]).await;
    assert_eq!(index.status, 200);
    assert_eq!(index.header("content-type"), Some("application/json"));
    let index: serde_json::Value = serde_json::from_slice(&index.body).unwrap();
    assert_eq!(
        index,
        serde_json::json!({ "tables": [
            { "name": "block_sync", "bytes": 19, "etag": etag },
            { "name": "conn", "bytes": 0, "etag": empty.header("etag").unwrap() },
        ]})
    );
    server.stop().await;
}

#[tokio::test]
async fn a_follower_reads_only_appended_bytes_and_notices_replacement() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("conn.jsonl");
    std::fs::write(&path, b"first\n").unwrap();
    let server = Server::start(dir.path()).await;

    let first = server.get("/traces/conn", &[]).await;
    let etag = first.header("etag").unwrap().to_owned();
    let mut cursor = first.body.len();

    // Caught up: 416 reports the current length.
    let range = format!("bytes={cursor}-");
    let resume = [("range", range.as_str()), ("if-range", etag.as_str())];
    let idle = server.get("/traces/conn", &resume).await;
    assert_eq!(idle.status, 416);
    assert_eq!(idle.header("content-range"), Some("bytes */6"));
    assert_eq!(idle.header("etag"), Some(etag.as_str()));

    append(&path, b"second\n");
    let grown = server.get("/traces/conn", &resume).await;
    assert_eq!(grown.status, 206);
    assert_eq!(grown.body, b"second\n");
    assert_eq!(grown.header("content-range"), Some("bytes 6-12/13"));
    assert_eq!(grown.header("etag"), Some(etag.as_str()));
    cursor += grown.body.len();

    // A replaced file fails If-Range, so the response is the whole new file.
    std::fs::rename(&path, dir.path().join("old.jsonl")).unwrap();
    std::fs::write(&path, b"new file\n").unwrap();
    let range = format!("bytes={cursor}-");
    let resume = [("range", range.as_str()), ("if-range", etag.as_str())];
    let replaced = server.get("/traces/conn", &resume).await;
    assert_eq!(replaced.status, 200);
    assert_eq!(replaced.body, b"new file\n");
    assert_ne!(replaced.header("etag"), Some(etag.as_str()));

    // If-Match refuses instead.
    let strict = server
        .get(
            "/traces/conn",
            &[("range", "bytes=0-"), ("if-match", &etag)],
        )
        .await;
    assert_eq!(strict.status, 412);
    assert_eq!(strict.header("etag"), replaced.header("etag"));
    server.stop().await;
}

#[tokio::test]
async fn large_files_and_ranges_stream_without_a_size_cap() {
    let dir = tempfile::tempdir().unwrap();
    let data: Vec<u8> = (0..5 * 1024 * 1024 + 3).map(|i| (i % 253) as u8).collect();
    std::fs::write(dir.path().join("conn.jsonl"), &data).unwrap();
    let server = Server::start(dir.path()).await;
    assert_eq!(server.get("/traces/conn", &[]).await.body, data);
    let tail = server
        .get("/traces/conn", &[("range", "bytes=1000-")])
        .await;
    assert_eq!(tail.status, 206);
    assert_eq!(tail.body, data[1000..]);
    let suffix = server.get("/traces/conn", &[("range", "bytes=-5")]).await;
    assert_eq!(suffix.body, data[data.len() - 5..]);
    server.stop().await;
}

#[tokio::test]
async fn the_node_writer_appends_while_a_response_streams() {
    use zakura_jsonl_trace::{JsonlTraceConfig, JsonlTracer, JsonlWriteEvent};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("conn.jsonl");
    let guard = JsonlTracer::spawn_guard_with_config(
        dir.path().to_owned(),
        JsonlTraceConfig {
            buffer_flush_bytes: 1,
            max_batch_events: 1,
            ..JsonlTraceConfig::default()
        },
    );
    let tracer = guard.tracer();
    let send = |value: usize| {
        tracer
            .try_send(JsonlWriteEvent {
                table: "conn",
                file_name: "conn.jsonl",
                line: format!("{{\"value\":{value}}}").into_bytes(),
            })
            .unwrap()
    };
    let wait_for_len = |len: u64| {
        let path = path.clone();
        async move {
            tokio::time::timeout(Duration::from_secs(2), async {
                while std::fs::metadata(&path).map_or(0, |meta| meta.len()) < len {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
        }
    };
    send(1);
    wait_for_len(12).await;
    let server = Server::start(dir.path()).await;
    let first = server.get("/traces/conn", &[]).await;
    assert_eq!(first.body, b"{\"value\":1}\n");
    let etag = first.header("etag").unwrap().to_owned();
    send(2);
    wait_for_len(24).await;
    let next = server
        .get(
            "/traces/conn",
            &[("range", "bytes=12-"), ("if-range", &etag)],
        )
        .await;
    assert_eq!(next.status, 206);
    assert_eq!(next.body, b"{\"value\":2}\n");
    server.stop().await;
    tokio::time::timeout(Duration::from_secs(2), guard.shutdown())
        .await
        .unwrap();
}

#[tokio::test]
async fn stalled_filesystem_threads_refuse_requests_until_released() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("conn.jsonl"), b"row\n").unwrap();
    let endpoint = Endpoint {
        directory: dir.path().to_owned(),
        authority: "127.0.0.1:9000".to_owned(),
        fs_threads: FsThreads::new(1),
    };
    let (release, blocked) = std::sync::mpsc::sync_channel::<()>(0);
    let (started, entered) = oneshot::channel();
    endpoint
        .fs_threads
        .try_spawn(move || {
            started.send(()).unwrap();
            let _ = blocked.recv();
        })
        .unwrap();
    entered.await.unwrap();
    let request = |path: &str| {
        Request::builder()
            .uri(path)
            .header(hyper::header::HOST, "127.0.0.1:9000")
            .body(())
            .unwrap()
    };
    for path in ["/traces/conn", "/traces"] {
        for _ in 0..50 {
            let response = endpoint.respond(request(path)).await;
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        }
    }
    drop(release);
    tokio::time::timeout(Duration::from_secs(2), async {
        while endpoint.respond(request("/traces/conn")).await.status() != StatusCode::OK {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn excess_connections_wait_and_shutdown_closes_open_ones() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("conn.jsonl"), b"row\n").unwrap();
    let server = Server::start(dir.path()).await;
    let mut idle = Vec::new();
    for _ in 0..MAX_CONNECTIONS {
        let mut socket = TcpStream::connect(server.address).await.unwrap();
        socket
            .write_all(b"GET /traces/conn HTTP/1.1\r\n")
            .await
            .unwrap();
        idle.push(socket);
    }
    let mut waiting = TcpStream::connect(server.address).await.unwrap();
    waiting
        .write_all(
            format!(
                "GET /traces/conn HTTP/1.1\r\nHost: {}\r\n\r\n",
                server.address
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let mut probe = [0u8; 1];
    assert!(
        tokio::time::timeout(Duration::from_millis(100), waiting.read(&mut probe))
            .await
            .is_err(),
        "a fifth connection must wait for a slot"
    );
    drop(idle.pop());
    let mut raw = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), waiting.read_to_end(&mut raw))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(parse_reply(&raw).body, b"row\n");

    server.shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), server.task)
        .await
        .unwrap()
        .unwrap();
    for mut socket in idle {
        let mut rest = Vec::new();
        let closed = tokio::time::timeout(Duration::from_secs(2), socket.read_to_end(&mut rest))
            .await
            .unwrap();
        assert!(
            closed.is_err() || rest.is_empty(),
            "shutdown closes idle connections"
        );
    }
}

#[tokio::test]
async fn malformed_requests_leave_the_listener_usable() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("conn.jsonl"), b"row\n").unwrap();
    let server = Server::start(dir.path()).await;
    let address = server.address;
    for request in [
        "GET /traces/conn HTTP/1.1\r\nHost: attacker.example\r\n\r\n".to_owned(),
        format!("GET /traces/conn HTTP/1.1\r\nHost: {address}\r\nOrigin: http://a.example\r\n\r\n"),
        format!("GET /traces/%2e%2e%2fsecret HTTP/1.1\r\nHost: {address}\r\n\r\n"),
        format!("GET /traces/conn HTTP/1.1\r\nHost: {address}\r\nRange: bytes=18446744073709551616-\r\n\r\n"),
        format!("GET /traces/conn HTTP/1.1\r\nHost: {address}\r\nX-Big: {}\r\n\r\n", "x".repeat(16 * 1024)),
        "not http at all\r\n\r\n".to_owned(),
    ] {
        let mut socket = TcpStream::connect(address).await.unwrap();
        socket.write_all(request.as_bytes()).await.unwrap();
        let mut raw = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(2), socket.read_to_end(&mut raw))
            .await
            .unwrap();
        assert!(!raw.starts_with(b"HTTP/1.1 2"), "{request:.60}");
        assert!(!raw.ends_with(b"row\n"), "{request:.60}");
    }
    let refused = server
        .get("/traces/conn", &[("origin", "http://a.example")])
        .await;
    assert_eq!(refused.status, 403);
    assert_eq!(
        refused.header("content-type"),
        Some("text/plain; charset=utf-8")
    );
    assert!(!refused.body.is_empty());
    assert_eq!(server.get("/traces/conn", &[]).await.body, b"row\n");
    server.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_stalled_reader_fails_the_write_and_progress_resets_the_deadline() {
    let (client, server) = tokio::io::duplex(64);
    let mut writer = WriteTimeout::new(server);
    let mut reader = client;
    // Fill the pipe, then keep reading a little before each deadline.
    writer.write_all(&[0; 64]).await.unwrap();
    for _ in 0..3 {
        let write = writer.write_all(&[0; 8]);
        tokio::pin!(write);
        assert!(
            tokio::time::timeout(IDLE_TIMEOUT - Duration::from_secs(1), &mut write)
                .await
                .is_err()
        );
        let mut buffer = [0; 8];
        reader.read_exact(&mut buffer).await.unwrap();
        write.await.unwrap();
    }
    let started = tokio::time::Instant::now();
    let error = writer.write_all(&[0; 8]).await.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert_eq!(started.elapsed(), IDLE_TIMEOUT);
}

#[tokio::test(start_paused = true)]
async fn transient_accept_errors_retry_after_backoff() {
    let mut attempts = 0;
    let started = tokio::time::Instant::now();
    let value = accept_with_backoff(
        || {
            attempts += 1;
            std::future::ready(if attempts == 1 {
                Err(io::Error::from(io::ErrorKind::Interrupted))
            } else {
                Ok(7)
            })
        },
        &CancellationToken::new(),
    )
    .await;
    assert_eq!((value, attempts), (Some(7), 2));
    assert_eq!(started.elapsed(), ACCEPT_BACKOFF);
}

#[tokio::test(start_paused = true)]
async fn shutdown_interrupts_accept_backoff() {
    let shutdown = CancellationToken::new();
    let cancel = shutdown.clone();
    let cancellation = tokio::spawn(async move {
        tokio::time::sleep(ACCEPT_BACKOFF / 2).await;
        cancel.cancel();
    });
    let started = tokio::time::Instant::now();
    let value = accept_with_backoff(
        || std::future::ready(Err::<(), _>(io::Error::from(io::ErrorKind::Other))),
        &shutdown,
    )
    .await;
    assert!(value.is_none());
    assert_eq!(started.elapsed(), ACCEPT_BACKOFF / 2);
    cancellation.await.unwrap();
}
