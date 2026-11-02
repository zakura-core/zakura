//! Tests for the plain HTTP listener's connection handling.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use hyper::Response;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};

use super::super::{http::HttpBody, run_listener, run_listener_with_limits};

/// A request that keeps its connection open after the response.
const KEEP_ALIVE_REQUEST: &[u8] =
    b"POST / HTTP/1.1\r\nhost: localhost\r\ncontent-length: 0\r\n\r\n";

/// A service that answers every request with `ok` after a delay.
#[derive(Clone)]
struct DelayedOk(Duration);

impl tower::Service<hyper::Request<hyper::body::Incoming>> for DelayedOk {
    type Response = Response<HttpBody>;
    type Error = tower::BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Response<HttpBody>, tower::BoxError>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), tower::BoxError>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _request: hyper::Request<hyper::body::Incoming>) -> Self::Future {
        let delay = self.0;
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            Ok(Response::new(HttpBody::from("ok")))
        })
    }
}

/// Sends a request on `stream` and checks that it gets a `200 OK` response.
async fn expect_ok_response(stream: &mut TcpStream) {
    stream.write_all(KEEP_ALIVE_REQUEST).await.unwrap();
    let mut response = [0; 1024];
    let read = timeout(Duration::from_secs(10), stream.read(&mut response))
        .await
        .expect("the server answers the request")
        .unwrap();
    assert!(
        response[..read].starts_with(b"HTTP/1.1 200 OK"),
        "unexpected response: {:?}",
        String::from_utf8_lossy(&response[..read])
    );
}

/// Waits for the server to close `stream` without sending anything.
async fn expect_closed(stream: &mut TcpStream) {
    let mut response = [0; 1024];
    let read = timeout(Duration::from_secs(10), stream.read(&mut response))
        .await
        .expect("the server closes the connection")
        .unwrap_or(0);
    assert_eq!(read, 0, "the connection is closed without a response");
}

/// Stopping the listener closes keep-alive connections that are still open, as `jsonrpsee` did.
#[tokio::test]
async fn stopping_the_listener_closes_open_connections() {
    let _init_guard = zakura_test::init();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let service = tower::service_fn(|_request| async {
        Ok::<_, tower::BoxError>(Response::new(HttpBody::from("ok")))
    });
    let listener_task = tokio::spawn(run_listener(listener, service));

    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(b"POST / HTTP/1.1\r\nhost: localhost\r\ncontent-length: 0\r\n\r\n")
        .await
        .unwrap();
    let mut response = [0; 1024];
    let read = timeout(Duration::from_secs(10), stream.read(&mut response))
        .await
        .expect("the server answers the first request")
        .unwrap();
    assert!(response[..read].starts_with(b"HTTP/1.1 200 OK"));

    listener_task.abort();

    let read = timeout(Duration::from_secs(10), stream.read(&mut response))
        .await
        .expect("the server closes the idle keep-alive connection when it stops");
    assert_eq!(
        read.unwrap(),
        0,
        "the connection is closed without a response"
    );
}

/// Connections over the listener's limit are closed, and closing a connection frees its slot.
#[tokio::test]
async fn connections_over_the_limit_are_closed() {
    let _init_guard = zakura_test::init();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let listener_task = tokio::spawn(run_listener_with_limits(
        listener,
        DelayedOk(Duration::ZERO),
        1,
        Duration::from_secs(60),
    ));

    let mut first = TcpStream::connect(addr).await.unwrap();
    expect_ok_response(&mut first).await;

    let mut second = TcpStream::connect(addr).await.unwrap();
    expect_closed(&mut second).await;

    // The server frees the slot once it sees the first connection close.
    drop(first);
    let served = timeout(Duration::from_secs(10), async {
        loop {
            let mut next = TcpStream::connect(addr).await.unwrap();
            if next.write_all(KEEP_ALIVE_REQUEST).await.is_ok() {
                let mut response = [0; 1024];
                if let Ok(read) = next.read(&mut response).await {
                    if response[..read].starts_with(b"HTTP/1.1 200 OK") {
                        return;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        served.is_ok(),
        "a new connection is served after one closes"
    );

    listener_task.abort();
}

/// Connections are closed after the idle timeout, both before their first request and
/// between keep-alive requests.
#[tokio::test]
async fn idle_connections_are_closed() {
    let _init_guard = zakura_test::init();

    let idle_timeout = Duration::from_millis(200);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let listener_task = tokio::spawn(run_listener_with_limits(
        listener,
        DelayedOk(Duration::ZERO),
        16,
        idle_timeout,
    ));

    let start = std::time::Instant::now();
    let mut silent = TcpStream::connect(addr).await.unwrap();
    expect_closed(&mut silent).await;
    assert!(
        start.elapsed() >= idle_timeout,
        "closed before the idle timeout"
    );

    let mut keep_alive = TcpStream::connect(addr).await.unwrap();
    expect_ok_response(&mut keep_alive).await;
    let answered = std::time::Instant::now();
    expect_closed(&mut keep_alive).await;
    assert!(
        answered.elapsed() >= idle_timeout,
        "closed before the idle timeout"
    );

    listener_task.abort();
}

/// A request that takes longer than the idle timeout keeps its connection open.
#[tokio::test]
async fn requests_in_progress_keep_connections_open() {
    let _init_guard = zakura_test::init();

    let idle_timeout = Duration::from_millis(200);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let listener_task = tokio::spawn(run_listener_with_limits(
        listener,
        DelayedOk(idle_timeout * 4),
        16,
        idle_timeout,
    ));

    let mut stream = TcpStream::connect(addr).await.unwrap();
    expect_ok_response(&mut stream).await;

    listener_task.abort();
}

/// Starts a listener that answers every request with a body much larger than the socket
/// buffers, so sending it waits for the client to read.
async fn large_response_listener(
    response_len: usize,
    idle_timeout: Duration,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let service = tower::service_fn(move |_request| async move {
        Ok::<_, tower::BoxError>(Response::new(HttpBody::from(vec![b'x'; response_len])))
    });
    let listener_task = tokio::spawn(run_listener_with_limits(
        listener,
        service,
        16,
        idle_timeout,
    ));
    (addr, listener_task)
}

/// A response that takes longer than the idle timeout to read is sent in full, as long as the
/// client keeps reading.
#[tokio::test]
async fn slowly_read_responses_are_sent_in_full() {
    let _init_guard = zakura_test::init();

    const RESPONSE_LEN: usize = 32 * 1024 * 1024;
    let idle_timeout = Duration::from_millis(200);
    let (addr, listener_task) = large_response_listener(RESPONSE_LEN, idle_timeout).await;

    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(KEEP_ALIVE_REQUEST).await.unwrap();

    // Reading takes several idle timeouts in total, but never pauses for one.
    let start = std::time::Instant::now();
    let mut received = Vec::new();
    let mut chunk = vec![0; 256 * 1024];
    loop {
        let read = timeout(Duration::from_secs(10), stream.read(&mut chunk))
            .await
            .expect("the server keeps sending or closes the connection")
            .unwrap();
        if read == 0 {
            break;
        }
        received.extend_from_slice(&chunk[..read]);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(start.elapsed() > idle_timeout * 2, "the read is slow");
    assert!(received.starts_with(b"HTTP/1.1 200 OK"));
    assert!(
        received.ends_with(&vec![b'x'; RESPONSE_LEN]),
        "the whole response body arrives"
    );

    listener_task.abort();
}

/// A client that stops reading a response for the idle timeout is disconnected.
#[tokio::test]
async fn stalled_response_reads_close_connections() {
    let _init_guard = zakura_test::init();

    const RESPONSE_LEN: usize = 32 * 1024 * 1024;
    let idle_timeout = Duration::from_millis(200);
    let (addr, listener_task) = large_response_listener(RESPONSE_LEN, idle_timeout).await;

    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(KEEP_ALIVE_REQUEST).await.unwrap();
    tokio::time::sleep(idle_timeout * 5).await;

    let mut received = Vec::new();
    // The connection may be reset rather than closed, because unread data was discarded.
    let _ = timeout(Duration::from_secs(10), stream.read_to_end(&mut received))
        .await
        .expect("the connection is closed");
    assert!(
        received.len() < RESPONSE_LEN,
        "the response is cut off when the client stops reading"
    );

    listener_task.abort();
}

/// Requests over the concurrency limit get `429 Too Many Requests` without reaching the
/// inner service, which is where request bodies are read.
#[tokio::test]
async fn requests_over_the_limit_are_rejected_before_reaching_the_inner_service() {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use tokio::sync::Notify;
    use tower::{Service as _, ServiceExt as _};

    use super::super::http::{HttpRequest, HttpResponse, RequestLimit};

    let _init_guard = zakura_test::init();

    // The inner service stays busy until released, like one receiving a slow body.
    let release = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let inner = {
        let release = release.clone();
        let calls = calls.clone();
        tower::service_fn(move |_request: HttpRequest| {
            let release = release.clone();
            calls.fetch_add(1, Ordering::SeqCst);
            async move {
                release.notified().await;
                Ok::<_, tower::BoxError>(HttpResponse::new(HttpBody::from("ok")))
            }
        })
    };
    let mut limit = RequestLimit::new(inner, 1);

    let first = limit
        .ready()
        .await
        .unwrap()
        .call(HttpRequest::new(HttpBody::default()));
    let first = tokio::spawn(first);
    while calls.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }

    let second = limit
        .ready()
        .await
        .unwrap()
        .call(HttpRequest::new(HttpBody::default()));
    let second = timeout(Duration::from_secs(10), second)
        .await
        .expect("excess requests are answered immediately")
        .unwrap();
    assert_eq!(second.status(), hyper::StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the excess request isn't passed on"
    );

    release.notify_one();
    let first = first.await.unwrap().unwrap();
    assert_eq!(first.status(), hyper::StatusCode::OK);

    // The permit is returned when the first request finishes.
    let third = limit
        .ready()
        .await
        .unwrap()
        .call(HttpRequest::new(HttpBody::default()));
    release.notify_one();
    assert_eq!(third.await.unwrap().status(), hyper::StatusCode::OK);
}

/// HTTP/2 connections are closed after the idle timeout following their last request.
#[tokio::test]
async fn idle_http2_connections_are_closed() {
    use hyper_util::rt::{TokioExecutor, TokioIo};

    let _init_guard = zakura_test::init();

    let idle_timeout = Duration::from_millis(200);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let listener_task = tokio::spawn(run_listener_with_limits(
        listener,
        DelayedOk(Duration::ZERO),
        16,
        idle_timeout,
    ));

    let stream = TcpStream::connect(addr).await.unwrap();
    let (mut sender, connection) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
            .await
            .unwrap();
    let connection = tokio::spawn(connection);

    let response = sender
        .send_request(
            hyper::Request::post("http://localhost/")
                .body(HttpBody::default())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), hyper::StatusCode::OK);
    let answered = std::time::Instant::now();

    // The client's connection task finishes when the server closes the connection.
    let _ = timeout(Duration::from_secs(10), connection)
        .await
        .expect("the server closes the idle HTTP/2 connection");
    assert!(
        answered.elapsed() >= idle_timeout,
        "closed before the idle timeout"
    );

    listener_task.abort();
}
