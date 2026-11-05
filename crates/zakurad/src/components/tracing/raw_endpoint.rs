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

const MAX_BYTES: usize = 1024 * 1024;
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
) -> io::Result<Option<JoinHandle<()>>> {
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
    let authority = listener.local_addr()?.to_string();
    info!(%authority, "raw trace endpoint enabled; local users can read sensitive trace data");
    Ok(Some(tokio::spawn(async move {
        // One active request bounds disk work, memory and slow-client resources.
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                accepted = listener.accept() => {
                    let Ok((stream, _)) = accepted else { break };
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
    })))
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
    let limit = u64::try_from(MAX_BYTES).expect("one MiB fits u64");
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
    #[cfg(unix)]
    let identity = {
        use std::os::unix::fs::MetadataExt;
        format!("{}:{}", metadata.dev(), metadata.ino())
    };
    #[cfg(not(unix))]
    let identity = String::new(); // init rejects this platform before opening a listener.
    if let Some(expected) = headers.get("x-trace-file-id") {
        if expected.to_str().ok() != Some(identity.as_str()) {
            return Err(StatusCode::CONFLICT);
        }
    }
    let (start, end, status) = if let Some(range) = headers.get(hyper::header::RANGE) {
        let (start, end) = byte_range(
            range.to_str().map_err(|_| StatusCode::BAD_REQUEST)?,
            metadata.len(),
        )?;
        if start > 0 && !headers.contains_key("x-trace-file-id") {
            return Err(StatusCode::PRECONDITION_REQUIRED);
        }
        (start, end, StatusCode::PARTIAL_CONTENT)
    } else {
        (0, metadata.len(), StatusCode::OK)
    };
    file.seek(io::SeekFrom::Start(start))
        .await
        .map_err(|_| unavailable)?;
    let count = end - start;
    let chunks = stream::try_unfold((file, count), |(mut file, remaining)| async move {
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
        buffer.truncate(read);
        let remaining = remaining - u64::try_from(read).expect("read size fits u64");
        Ok(Some((
            hyper::body::Frame::data(Bytes::from(buffer)),
            (file, remaining),
        )))
    });
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
            format!(
                "bytes {start}-{}/{length}",
                end - 1,
                length = metadata.len()
            )
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
        tokio::fs::rename(&path, dir.path().join("old.jsonl"))
            .await
            .unwrap();
        tokio::fs::write(&path, data).await.unwrap();
        assert_eq!(
            export(dir.path(), "block_sync", &headers).await.err(),
            Some(StatusCode::CONFLICT)
        );
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
        std::fs::create_dir(dir.path().join("conn.jsonl")).unwrap();
        assert!(export(dir.path(), "conn", &hyper::HeaderMap::new())
            .await
            .is_err());
    }
}
