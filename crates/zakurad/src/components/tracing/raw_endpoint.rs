//! Opt-in, local-only streams and bounded byte ranges of structured trace files.

use std::{
    convert::Infallible,
    io,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use bytes::Bytes;
use futures::stream;
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Full, StreamBody};
use hyper::{
    header::{CACHE_CONTROL, CONTENT_TYPE, HOST, ORIGIN},
    server::conn::http1,
    service::service_fn,
    Method, Request, Response, StatusCode,
};
use hyper_util::rt::TokioIo;
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt},
    net::TcpListener,
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

const MAX_RANGE_BYTES: u64 = 1024 * 1024;
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);
// Explicit export allowlist, independent of other files in the trace directory.
const TABLES: &[&str] = &[
    "handshake",
    "conn",
    "stream",
    "discovery",
    "ratelimit",
    "header_sync",
    "legacy_request",
    "block_sync",
    "commit_state",
    "quic_conn",
    "queue_send",
    "legacy_peer_request",
    "legacy_sync",
];

/// Bind the configured local endpoint, or do nothing when disabled.
/// Invalid configuration and bind errors fail node startup rather than silently
/// exposing a different address. The task stops with the node's shutdown token.
pub(crate) async fn init(
    address: Option<SocketAddr>,
    directory: Option<PathBuf>,
    shutdown: CancellationToken,
) -> io::Result<Option<(JoinHandle<()>, SocketAddr)>> {
    let Some(address) = address else {
        return Ok(None);
    };
    if !address.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "raw trace endpoint must bind a loopback IP",
        ));
    }
    if !cfg!(unix) {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "raw trace endpoint requires Unix no-follow file opens",
        ));
    }
    let directory = directory.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "raw trace endpoint requires network.zakura.trace_dir",
        )
    })?;
    let listener = TcpListener::bind(address).await?;
    let bound_address = listener.local_addr()?;
    let authority = bound_address.to_string();
    info!(%authority, "raw trace endpoint enabled; local users can read sensitive trace data");
    let task = tokio::spawn(async move {
        // One active request bounds disk work, memory and slow-client resources.
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                accepted = listener.accept() => {
                    let (stream, _) = match accepted {
                        Ok(connection) => connection,
                        Err(error) => {
                            warn!(?error, "raw trace endpoint failed to accept a connection");
                            break;
                        }
                    };
                    let service = service_fn(|request| handle(request, &directory, &authority));
                    let connection = async {
                        let _ = http1::Builder::new()
                            .keep_alive(false)
                            .max_buf_size(8192)
                            .serve_connection(TokioIo::new(stream), service)
                            .await;
                    };
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        _ = tokio::time::timeout(CONNECTION_TIMEOUT, connection) => {}
                    }
                }
            }
        }
    });
    Ok(Some((task, bound_address)))
}

/// Reject browser access and nonliteral authorities before resolving a table.
fn requested_table<'a, B>(request: &'a Request<B>, authority: &str) -> Result<&'a str, StatusCode> {
    if request.headers().contains_key(ORIGIN)
        || request
            .headers()
            .get(HOST)
            .and_then(|value| value.to_str().ok())
            != Some(authority)
        || request.uri().authority().is_some()
    {
        return Err(StatusCode::FORBIDDEN);
    }
    if request.method() != Method::GET {
        return Err(StatusCode::METHOD_NOT_ALLOWED);
    }
    if request.uri().query().is_some() {
        return Err(StatusCode::BAD_REQUEST);
    }
    request
        .uri()
        .path()
        .strip_prefix("/traces/")
        .filter(|table| TABLES.contains(table))
        .ok_or(StatusCode::NOT_FOUND)
}

type Body = UnsyncBoxBody<Bytes, io::Error>;

async fn handle<B>(
    request: Request<B>,
    directory: &Path,
    authority: &str,
) -> Result<Response<Body>, Infallible> {
    let result = match requested_table(&request, authority) {
        Ok(table) => export(directory, table, request.headers()).await,
        Err(status) => Err(status),
    };
    let mut response = result.unwrap_or_else(|status| {
        let body = Full::new(Bytes::new())
            .map_err(|never| match never {})
            .boxed_unsync();
        let mut response = Response::new(body);
        *response.status_mut() = status;
        response
    });
    response.headers_mut().insert(
        CONTENT_TYPE,
        "application/x-ndjson"
            .parse()
            .expect("static header is valid"),
    );
    response.headers_mut().insert(
        CACHE_CONTROL,
        "no-store".parse().expect("static header is valid"),
    );
    response.headers_mut().insert(
        "x-content-type-options",
        "nosniff".parse().expect("static header is valid"),
    );
    Ok(response)
}

/// Parse one inclusive byte range. Open-ended ranges are capped at one MiB.
fn byte_range(value: &str, length: u64) -> Result<(u64, u64), StatusCode> {
    let invalid = StatusCode::RANGE_NOT_SATISFIABLE;
    let (start, end) = value
        .strip_prefix("bytes=")
        .and_then(|range| range.split_once('-'))
        .ok_or(invalid)?;
    let start: u64 = start.parse().map_err(|_| invalid)?;
    if start >= length {
        return Err(invalid);
    }
    let limit = MAX_RANGE_BYTES;
    let end = if end.is_empty() {
        start.saturating_add(limit - 1).min(length - 1)
    } else {
        end.parse::<u64>().map_err(|_| invalid)?.min(length - 1)
    };
    if end < start || end - start >= limit {
        return Err(invalid);
    }
    Ok((start, end + 1))
}

/// A fixed prefix of an append-only trace file. This is not a content hash.
/// External copy-truncate, in-place edits and inode reuse are unsupported.
struct TracePrefix {
    device: u64,
    inode: u64,
    length: u64,
}

impl TracePrefix {
    fn capture(metadata: &std::fs::Metadata) -> io::Result<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            Ok(Self {
                device: metadata.dev(),
                inode: metadata.ino(),
                length: metadata.len(),
            })
        }
        #[cfg(not(unix))]
        {
            let _ = metadata;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "raw trace export requires Unix",
            ))
        }
    }

    fn parse(value: &str) -> Option<Self> {
        let mut fields = value.split(':');
        let prefix = Self {
            device: fields.next()?.parse().ok()?,
            inode: fields.next()?.parse().ok()?,
            length: fields.next()?.parse().ok()?,
        };
        fields.next().is_none().then_some(prefix)
    }

    fn token(&self) -> String {
        format!("{}:{}:{}", self.device, self.inode, self.length)
    }

    /// Appends preserve the prefix. Reject replacement and observed truncation.
    /// A truncate followed by regrowth between checks is not detectable here.
    fn validate(&self, metadata: &std::fs::Metadata) -> io::Result<()> {
        let current = Self::capture(metadata)?;
        if current.device != self.device
            || current.inode != self.inode
            || current.length < self.length
        {
            return Err(io::Error::other(
                "trace replaced or truncated; start a new prefix",
            ));
        }
        Ok(())
    }
}

/// Open a fixed table and stream only the file length observed at open time.
/// Range responses preserve raw bytes, including partial JSONL records.
async fn export(
    directory: &Path,
    table: &str,
    headers: &hyper::HeaderMap,
) -> Result<Response<Body>, StatusCode> {
    let unavailable = StatusCode::SERVICE_UNAVAILABLE;
    let mut options = tokio::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let mut file = options
        .open(directory.join(format!("{table}.jsonl")))
        .await
        .map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                StatusCode::NOT_FOUND
            } else {
                unavailable
            }
        })?;
    let metadata = file.metadata().await.map_err(|_| unavailable)?;
    if !metadata.is_file() {
        return Err(unavailable);
    }
    let prefix = if let Some(expected) = headers.get("x-trace-file-id") {
        let prefix = expected
            .to_str()
            .ok()
            .and_then(TracePrefix::parse)
            .ok_or(StatusCode::BAD_REQUEST)?;
        prefix
            .validate(&metadata)
            .map_err(|_| StatusCode::CONFLICT)?;
        prefix
    } else {
        TracePrefix::capture(&metadata).map_err(|_| unavailable)?
    };
    let identity = prefix.token();
    let length = prefix.length;
    let (start, end, status) = if let Some(range) = headers.get(hyper::header::RANGE) {
        let (start, end) =
            byte_range(range.to_str().map_err(|_| StatusCode::BAD_REQUEST)?, length)?;
        if start > 0 && !headers.contains_key("x-trace-file-id") {
            return Err(StatusCode::PRECONDITION_REQUIRED);
        }
        (start, end, StatusCode::PARTIAL_CONTENT)
    } else {
        (0, length, StatusCode::OK)
    };
    file.seek(io::SeekFrom::Start(start))
        .await
        .map_err(|_| unavailable)?;
    let count = end - start;
    let chunks = stream::try_unfold(
        (file, count, prefix),
        |(mut file, remaining, prefix)| async move {
            if remaining == 0 {
                return Ok::<_, io::Error>(None);
            }
            let size = usize::try_from(remaining.min(64 * 1024)).expect("64 KiB fits usize");
            let mut buffer = vec![0; size];
            let read = file.read(&mut buffer).await?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "trace truncated during export",
                ));
            }
            // The producer only appends. Check the captured lower length bound
            // after each read, allowing growth without changing this response.
            prefix.validate(&file.metadata().await?)?;
            buffer.truncate(read);
            let remaining = remaining - u64::try_from(read).expect("read size fits u64");
            Ok(Some((
                hyper::body::Frame::data(Bytes::from(buffer)),
                (file, remaining, prefix),
            )))
        },
    );
    let mut response = Response::new(StreamBody::new(chunks).boxed_unsync());
    *response.status_mut() = status;
    let response_headers = response.headers_mut();
    response_headers.insert(hyper::header::CONTENT_LENGTH, count.into());
    response_headers.insert(
        hyper::header::ACCEPT_RANGES,
        "bytes".parse().expect("static header is valid"),
    );
    response_headers.insert(
        "x-trace-file-id",
        identity
            .parse()
            .expect("numeric identity is a valid header"),
    );
    if status == StatusCode::PARTIAL_CONTENT {
        response_headers.insert(
            hyper::header::CONTENT_RANGE,
            format!("bytes {start}-{}/{length}", end - 1, length = length)
                .parse()
                .expect("numeric range is a valid header"),
        );
    }
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn disabled_and_unsafe_configurations() {
        assert!(init(None, None, CancellationToken::new())
            .await
            .unwrap()
            .is_none());
        assert!(init(
            Some("0.0.0.0:0".parse().unwrap()),
            Some(PathBuf::from(".")),
            CancellationToken::new()
        )
        .await
        .is_err());
        assert!(init(
            Some("127.0.0.1:0".parse().unwrap()),
            None,
            CancellationToken::new()
        )
        .await
        .is_err());
        assert!(crate::components::tracing::Config::default()
            .raw_endpoint_addr
            .is_none());
    }

    #[test]
    fn request_boundary_rejects_browser_and_path_attacks() {
        let authority = "127.0.0.1:9000";
        let request = |path: &str| {
            Request::builder()
                .uri(path)
                .header(HOST, authority)
                .body(())
                .unwrap()
        };
        assert_eq!(
            requested_table(&request("/traces/block_sync"), authority),
            Ok("block_sync")
        );
        for path in [
            "/traces/../secret",
            "/traces/%2e%2e%2fsecret",
            "/traces/unknown",
            "/traces/block_sync?offset=0",
        ] {
            assert!(requested_table(&request(path), authority).is_err());
        }
        assert_eq!(
            requested_table(
                &request("http://127.0.0.1:9000/traces/block_sync"),
                authority
            ),
            Err(StatusCode::FORBIDDEN)
        );
        let mut req = request("/traces/block_sync");
        req.headers_mut()
            .insert(ORIGIN, "http://example.com".parse().unwrap());
        assert_eq!(requested_table(&req, authority), Err(StatusCode::FORBIDDEN));
        req.headers_mut().remove(ORIGIN);
        req.headers_mut()
            .insert(HOST, "attacker.example:9000".parse().unwrap());
        assert_eq!(requested_table(&req, authority), Err(StatusCode::FORBIDDEN));
        req.headers_mut().insert(HOST, authority.parse().unwrap());
        *req.method_mut() = Method::POST;
        assert_eq!(
            requested_table(&req, authority),
            Err(StatusCode::METHOD_NOT_ALLOWED)
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stream_and_resume_preserve_exact_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("block_sync.jsonl");
        let data = b"{\"event\":1}\npartial";
        tokio::fs::write(&path, data).await.unwrap();
        let response = export(dir.path(), "block_sync", &hyper::HeaderMap::new())
            .await
            .unwrap();
        let identity = response.headers()["x-trace-file-id"].clone();
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            &data[..]
        );
        let mut headers = hyper::HeaderMap::new();
        headers.insert(hyper::header::RANGE, "bytes=4-8".parse().unwrap());
        assert_eq!(
            export(dir.path(), "block_sync", &headers).await.err(),
            Some(StatusCode::PRECONDITION_REQUIRED)
        );
        headers.insert("x-trace-file-id", identity);
        let response = export(dir.path(), "block_sync", &headers).await.unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            &data[4..9]
        );
        tokio::fs::write(&path, b"short\n").await.unwrap();
        assert_eq!(
            export(dir.path(), "block_sync", &headers).await.err(),
            Some(StatusCode::CONFLICT)
        );
        let current = export(dir.path(), "block_sync", &hyper::HeaderMap::new())
            .await
            .unwrap();
        headers.insert(
            "x-trace-file-id",
            current.headers()["x-trace-file-id"].clone(),
        );
        tokio::fs::rename(&path, dir.path().join("old.jsonl"))
            .await
            .unwrap();
        tokio::fs::write(&path, data).await.unwrap();
        assert_eq!(
            export(dir.path(), "block_sync", &headers).await.err(),
            Some(StatusCode::CONFLICT)
        );
        assert_eq!(
            current.into_body().collect().await.unwrap().to_bytes(),
            b"short\n"[..]
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stream_allows_appends_and_reports_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("conn.jsonl");
        tokio::fs::write(&path, b"first\n").await.unwrap();
        let response = export(dir.path(), "conn", &hyper::HeaderMap::new())
            .await
            .unwrap();
        let mut writer = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .await
            .unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut writer, b"second\n")
            .await
            .unwrap();
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            b"first\n"[..]
        );
        let response = export(dir.path(), "conn", &hyper::HeaderMap::new())
            .await
            .unwrap();
        writer.set_len(0).await.unwrap();
        assert!(response.into_body().collect().await.is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn resume_keeps_original_prefix_while_table_grows() {
        use tokio::io::AsyncWriteExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("conn.jsonl");
        let original = vec![b'x'; 2 * 1024 * 1024];
        tokio::fs::write(&path, &original).await.unwrap();
        let mut headers = hyper::HeaderMap::new();
        headers.insert(hyper::header::RANGE, "bytes=0-".parse().unwrap());
        let first = export(dir.path(), "conn", &headers).await.unwrap();
        let identity = first.headers()["x-trace-file-id"].clone();
        let mut writer = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .await
            .unwrap();
        let mut first_body = first.into_body();
        let head = first_body
            .frame()
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap();
        writer.write_all(b"new rows\n").await.unwrap();
        let tail = first_body.collect().await.unwrap().to_bytes();
        let first_bytes = [head.as_ref(), tail.as_ref()].concat();
        assert_eq!(first_bytes.len(), 1024 * 1024);
        headers.insert("x-trace-file-id", identity);
        headers.insert(hyper::header::RANGE, "bytes=1048576-".parse().unwrap());
        let second = export(dir.path(), "conn", &headers).await.unwrap();
        assert_eq!(
            second.headers()[hyper::header::CONTENT_RANGE],
            "bytes 1048576-2097151/2097152"
        );
        writer.write_all(b"more rows\n").await.unwrap();
        let second_bytes = second.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            [first_bytes.as_slice(), second_bytes.as_ref()].concat(),
            original
        );
        headers.insert(hyper::header::RANGE, "bytes=2097152-".parse().unwrap());
        assert_eq!(
            export(dir.path(), "conn", &headers).await.err(),
            Some(StatusCode::RANGE_NOT_SATISFIABLE)
        );
        let fresh = export(dir.path(), "conn", &hyper::HeaderMap::new())
            .await
            .unwrap();
        assert_eq!(
            fresh.into_body().collect().await.unwrap().to_bytes().len(),
            original.len() + 19
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn active_node_writer_preserves_exported_prefix() {
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
        let send = |value| {
            tracer
                .try_send(JsonlWriteEvent {
                    table: "conn",
                    file_name: "conn.jsonl",
                    line: format!("{{\"value\":{value}}}").into_bytes(),
                })
                .unwrap()
        };
        send(1);
        tokio::time::timeout(Duration::from_secs(2), async {
            while tokio::fs::metadata(&path)
                .await
                .map_or(true, |meta| meta.len() == 0)
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let first = export(dir.path(), "conn", &hyper::HeaderMap::new())
            .await
            .unwrap();
        let identity = first.headers()["x-trace-file-id"].clone();
        send(2);
        tokio::time::timeout(Duration::from_secs(2), async {
            while tokio::fs::metadata(&path).await.unwrap().len() <= 12 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            first.into_body().collect().await.unwrap().to_bytes(),
            b"{\"value\":1}\n"[..]
        );
        let mut headers = hyper::HeaderMap::new();
        headers.insert("x-trace-file-id", identity);
        headers.insert(hyper::header::RANGE, "bytes=1-".parse().unwrap());
        let resumed = export(dir.path(), "conn", &headers).await.unwrap();
        assert_eq!(
            resumed.into_body().collect().await.unwrap().to_bytes(),
            b"\"value\":1}\n"[..]
        );
        tokio::time::timeout(Duration::from_secs(2), guard.shutdown())
            .await
            .unwrap();
    }

    #[test]
    fn prefix_tokens_reject_malformed_or_overflowing_fields() {
        for token in ["", "1:2", "1:2:3:4", "x:2:3", "1:2:18446744073709551616"] {
            assert!(TracePrefix::parse(token).is_none());
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn http_server_serializes_clients_and_stops_on_shutdown() {
        use tokio::io::AsyncWriteExt;
        let dir = tempfile::tempdir().unwrap();
        tokio::fs::write(dir.path().join("conn.jsonl"), b"row\n")
            .await
            .unwrap();
        let shutdown = CancellationToken::new();
        let (task, address) = init(
            Some("127.0.0.1:0".parse().unwrap()),
            Some(dir.path().to_owned()),
            shutdown.clone(),
        )
        .await
        .unwrap()
        .unwrap();
        let mut slow = tokio::net::TcpStream::connect(address).await.unwrap();
        slow.write_all(b"GET /traces/conn HTTP/1.1\r\n")
            .await
            .unwrap();
        let mut next = tokio::net::TcpStream::connect(address).await.unwrap();
        next.write_all(format!("GET /traces/conn HTTP/1.1\r\nHost: {address}\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut probe = [0u8; 1];
        assert!(
            tokio::time::timeout(Duration::from_millis(50), next.read(&mut probe))
                .await
                .is_err()
        );
        tokio::time::pause();
        tokio::time::advance(CONNECTION_TIMEOUT + Duration::from_secs(1)).await;
        tokio::time::resume();
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), next.read_to_end(&mut response))
            .await
            .unwrap()
            .unwrap();
        let response = String::from_utf8(response).unwrap();
        assert!(response.starts_with("HTTP/1.1 200"));
        assert!(response.contains("cache-control: no-store"));
        assert!(response.ends_with("row\n"));
        let _idle = tokio::net::TcpStream::connect(address).await.unwrap();
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
    }

    #[test]
    fn ranges_are_bounded_and_checked() {
        assert_eq!(byte_range("bytes=0-", 2_000_000), Ok((0, 1_048_576)));
        assert_eq!(byte_range("bytes=2-4", 4), Ok((2, 4)));
        for range in [
            "bytes=-1",
            "bytes=4-3",
            "bytes=0-9999999",
            "bytes=0-1,3-4",
            "bytes=18446744073709551616-",
        ] {
            assert!(byte_range(range, 2_000_000).is_err());
        }
        assert!(byte_range("bytes=0-", 0).is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn export_rejects_symlinks_and_nonregular_files() {
        let dir = tempfile::tempdir().unwrap();
        let secret = dir.path().join("secret");
        std::fs::write(&secret, b"secret\n").unwrap();
        std::os::unix::fs::symlink(secret, dir.path().join("block_sync.jsonl")).unwrap();
        assert!(export(dir.path(), "block_sync", &hyper::HeaderMap::new())
            .await
            .is_err());
        let _socket =
            std::os::unix::net::UnixListener::bind(dir.path().join("stream.jsonl")).unwrap();
        assert!(tokio::time::timeout(
            Duration::from_secs(1),
            export(dir.path(), "stream", &hyper::HeaderMap::new())
        )
        .await
        .unwrap()
        .is_err());
        std::fs::create_dir(dir.path().join("conn.jsonl")).unwrap();
        assert!(export(dir.path(), "conn", &hyper::HeaderMap::new())
            .await
            .is_err());
    }
}
