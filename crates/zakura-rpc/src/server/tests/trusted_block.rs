//! HTTP admission tests for binary submissions, before any consensus or relay work.

use base64::{engine::general_purpose::STANDARD, Engine};
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::{
    body::{Bytes, Frame},
    Request, StatusCode,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use super::super::{
    cookie::{self, Cookie},
    http::HttpResponse,
    trusted_block::{Submit, TrustedBlockHttpService},
};
use crate::{config::rpc::Config, methods::types::submit_block::SubmitBlockResponse};

/// Calls the service the way hyper does when it parses a request head.
fn send<S, B>(
    service: TrustedBlockHttpService<S>,
    request: Request<B>,
) -> <TrustedBlockHttpService<S> as hyper::service::Service<Request<B>>>::Future
where
    TrustedBlockHttpService<S>: hyper::service::Service<Request<B>>,
{
    hyper::service::Service::call(&service, request)
}

fn credentials() -> (Cookie, String) {
    let cookie = Cookie::default();
    let directory = tempfile::tempdir().unwrap();
    cookie::write_to_disk(&cookie, directory.path(), None).unwrap();
    let credentials = std::fs::read(directory.path().join(".cookie")).unwrap();
    (cookie, format!("Basic {}", STANDARD.encode(credentials)))
}

fn request(auth: &str) -> hyper::http::request::Builder {
    Request::post("/submittrustedblock")
        .header("Authorization", auth)
        .header("Content-Type", "application/octet-stream")
}

#[test]
fn trusted_submission_requires_a_primary_listener_with_either_auth_policy() {
    let mut config = Config::default();
    assert!(!config.enable_trusted_block_submission);
    config.enable_trusted_block_submission = true;
    assert!(config.validate().is_err());
    config.listen_addr = Some("127.0.0.1:0".parse().unwrap());
    assert!(config.validate().is_ok());
    config.enable_cookie_auth = false;
    assert!(config.validate().is_ok());
    config.listen_addr = None;
    assert!(config.validate().is_err());
}

#[tokio::test]
async fn trusted_http_authenticates_before_polling_the_body() {
    let (cookie, auth) = credentials();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let submit: Submit = Arc::new(move |_| {
        count.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(SubmitBlockResponse::Accepted) })
    });
    let json = tower::service_fn(|_| async { Ok::<_, tower::BoxError>(HttpResponse::default()) });
    let service = TrustedBlockHttpService::new(json, Some(cookie), Some(submit));
    for (authorization, origin, status) in [
        (None, false, StatusCode::UNAUTHORIZED),
        (Some("invalid"), false, StatusCode::UNAUTHORIZED),
        (Some(auth.as_str()), true, StatusCode::FORBIDDEN),
    ] {
        let body = StreamBody::new(futures::stream::poll_fn(
            |_| -> std::task::Poll<Option<Result<Frame<Bytes>, std::io::Error>>> {
                panic!("unauthorized request body must not be polled")
            },
        ));
        let mut builder =
            Request::post("/submittrustedblock").header("Content-Type", "application/octet-stream");
        if let Some(authorization) = authorization {
            builder = builder.header("Authorization", authorization);
        }
        if origin {
            builder = builder.header("Origin", "https://example.invalid");
        }
        let response = send(service.clone(), builder.body(body).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), status);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn trusted_http_bounds_streams_and_routes_without_json_conversion() {
    let (cookie, auth) = credentials();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let submit: Submit = Arc::new(move |bytes| {
        count.fetch_add(1, Ordering::SeqCst);
        assert_eq!(bytes.as_ref(), &[0, 255, 128]);
        Box::pin(async { Ok(SubmitBlockResponse::Accepted) })
    });
    let json = tower::service_fn(|_| async { Ok::<_, tower::BoxError>(HttpResponse::default()) });
    let service = TrustedBlockHttpService::new(json, Some(cookie.clone()), Some(submit));
    let make_body = |bytes| Full::new(Bytes::from(bytes));
    let response = send(
        service.clone(),
        request(&auth).body(make_body(vec![0, 255, 128])).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "accepted"
    );
    let response = send(
        service.clone(),
        request(&auth)
            .method("GET")
            .body(make_body(vec![]))
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    let response = send(
        service.clone(),
        Request::post("/submittrustedblock")
            .header("Authorization", &auth)
            .header("Content-Type", "application/json")
            .body(make_body(vec![]))
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    let length = usize::try_from(zakura_chain::block::MAX_BLOCK_BYTES).unwrap() + 1;
    let response = send(
        service.clone(),
        request(&auth).body(make_body(vec![0; length])).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let response = send(
        service.clone(),
        request(&auth)
            .header("Content-Length", length.to_string())
            .body(make_body(vec![]))
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::PAYLOAD_TOO_LARGE,
        "a declared oversized body is refused before it is read"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let json = tower::service_fn(|_| async { Ok::<_, tower::BoxError>(HttpResponse::default()) });
    let disabled = TrustedBlockHttpService::new(json, Some(cookie), None);
    assert_eq!(
        send(disabled, request(&auth).body(make_body(vec![])).unwrap())
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
}

type TestBody =
    StreamBody<futures::stream::BoxStream<'static, Result<Frame<Bytes>, std::io::Error>>>;

/// A body fed by the test, like hyper's sender. Dropping the sender ends the body cleanly;
/// hyper reports a connection lost mid-body by sending an error instead.
fn channel_body() -> (
    futures::channel::mpsc::UnboundedSender<Result<Frame<Bytes>, std::io::Error>>,
    TestBody,
) {
    use futures::StreamExt;
    let (sender, receiver) = futures::channel::mpsc::unbounded();
    (sender, StreamBody::new(receiver.boxed()))
}

#[tokio::test]
async fn trusted_http_capacity_is_released_when_incomplete_clients_disconnect() {
    let (cookie, auth) = credentials();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let submit: Submit = Arc::new(move |_| {
        count.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(SubmitBlockResponse::Accepted) })
    });
    let json = tower::service_fn(|_| async { Ok::<_, tower::BoxError>(HttpResponse::default()) });
    let service = TrustedBlockHttpService::new(json, Some(cookie), Some(submit));
    let mut incomplete = vec![];
    for _ in 0..16 {
        let (sender, body) = channel_body();
        sender
            .unbounded_send(Ok(Frame::data(Bytes::from_static(&[1]))))
            .unwrap();
        let mut future = Box::pin(send(service.clone(), request(&auth).body(body).unwrap()));
        assert!(futures::poll!(&mut future).is_pending());
        incomplete.push((sender, future));
    }
    let (sender, body) = channel_body();
    drop(sender);
    let response = send(service.clone(), request(&auth).body(body).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    for (sender, future) in incomplete {
        sender
            .unbounded_send(Err(std::io::Error::other("connection lost mid-body")))
            .unwrap();
        // The detached receiver answers even though no client would read it, then frees its slot.
        assert_eq!(future.await.unwrap().status(), StatusCode::BAD_REQUEST);
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "a truncated body is never submitted"
    );
    let (sender, body) = channel_body();
    drop(sender);
    let response = send(service, request(&auth).body(body).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

/// Hyper reads a body only after its handler polls it, then can queue every frame and drop the
/// handler on the client's close before polling it again. The submission must not depend on
/// that second poll.
#[tokio::test]
async fn trusted_http_submits_a_body_completed_after_the_handler_is_dropped() {
    let (cookie, auth) = credentials();
    let (submitted_tx, mut submitted) = tokio::sync::mpsc::unbounded_channel();
    let submit: Submit = Arc::new(move |bytes| {
        submitted_tx.send(bytes).unwrap();
        Box::pin(async { Ok(SubmitBlockResponse::Accepted) })
    });
    let json = tower::service_fn(|_| async { Ok::<_, tower::BoxError>(HttpResponse::default()) });
    let service = TrustedBlockHttpService::new(json, Some(cookie), Some(submit));
    let (sender, body) = channel_body();
    let mut handler = Box::pin(send(service, request(&auth).body(body).unwrap()));
    assert!(futures::poll!(&mut handler).is_pending());
    for frame in [&[1, 2][..], &[3]] {
        sender
            .unbounded_send(Ok(Frame::data(Bytes::copy_from_slice(frame))))
            .unwrap();
    }
    drop(sender);
    drop(handler);
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(5), submitted.recv())
            .await
            .expect("the dropped handler's body is still submitted")
            .unwrap()
            .as_ref(),
        &[1, 2, 3],
        "the complete body is submitted after its connection is gone"
    );
}

#[tokio::test]
async fn trusted_http_without_cookie_accepts_requests_without_credentials() {
    let submit: Submit = Arc::new(|bytes| {
        assert_eq!(bytes.as_ref(), &[0, 255, 128]);
        Box::pin(async { Ok(SubmitBlockResponse::Accepted) })
    });
    let json = tower::service_fn(|_| async { Ok::<_, tower::BoxError>(HttpResponse::default()) });
    let service = TrustedBlockHttpService::new(json, None, Some(submit));
    let response = send(
        service,
        Request::post("/submittrustedblock")
            .header("Content-Type", "application/octet-stream")
            .body(Full::new(Bytes::from_static(&[0, 255, 128])))
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "accepted"
    );
}

#[tokio::test]
async fn trusted_http_without_cookie_rejects_browser_requests_before_reading_body() {
    let submit: Submit = Arc::new(|_| panic!("browser requests must not be submitted"));
    let json = tower::service_fn(|_| async { Ok::<_, tower::BoxError>(HttpResponse::default()) });
    let service = TrustedBlockHttpService::new(json, None, Some(submit));
    let body = StreamBody::new(futures::stream::poll_fn(
        |_| -> std::task::Poll<Option<Result<Frame<Bytes>, std::io::Error>>> {
            panic!("browser request body must not be polled")
        },
    ));
    assert_eq!(
        send(
            service,
            Request::post("/submittrustedblock")
                .header("Content-Type", "application/octet-stream")
                .header("Origin", "https://example.invalid")
                .body(body)
                .unwrap(),
        )
        .await
        .unwrap()
        .status(),
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn trusted_http_without_cookie_still_requires_explicit_enablement() {
    let json = tower::service_fn(|_| async { Ok::<_, tower::BoxError>(HttpResponse::default()) });
    let service = TrustedBlockHttpService::new(json, None, None);
    assert_eq!(
        send(service, request("").body(Full::new(Bytes::new())).unwrap())
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test(start_paused = true)]
async fn trusted_http_body_timeout_is_bounded() {
    let (cookie, auth) = credentials();
    let submit: Submit = Arc::new(|_| panic!("an incomplete body cannot be submitted"));
    let json = tower::service_fn(|_| async { Ok::<_, tower::BoxError>(HttpResponse::default()) });
    let service = TrustedBlockHttpService::new(json, Some(cookie), Some(submit));
    let body = StreamBody::new(futures::stream::pending::<
        Result<Frame<Bytes>, std::io::Error>,
    >());
    let response = send(service, request(&auth).body(body).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
}

/// Hyper drops the connection future when a client disconnects. Once the whole body has
/// arrived, the submission must still run to completion, and it keeps its body slot until it
/// finishes so disconnected clients stay within the same bound as connected ones.
#[tokio::test]
async fn trusted_http_completes_a_fully_received_submission_after_client_disconnect() {
    use futures::StreamExt;
    use tokio::sync::{mpsc, oneshot};

    let (cookie, auth) = credentials();
    let (started_tx, mut started) = mpsc::unbounded_channel();
    let (release_tx, release) = oneshot::channel::<()>();
    let release = Arc::new(std::sync::Mutex::new(Some(release)));
    let (finished_tx, finished) = oneshot::channel();
    let finished_tx = Arc::new(std::sync::Mutex::new(Some(finished_tx)));
    let submit: Submit = Arc::new(move |bytes| {
        let started_tx = started_tx.clone();
        let release = release.lock().unwrap().take();
        let finished_tx = finished_tx.lock().unwrap().take();
        Box::pin(async move {
            started_tx.send(bytes).unwrap();
            if let Some(release) = release {
                release.await.unwrap();
            }
            if let Some(finished_tx) = finished_tx {
                finished_tx.send(()).unwrap();
            }
            Ok(SubmitBlockResponse::Accepted)
        })
    });
    let json = tower::service_fn(|_| async { Ok::<_, tower::BoxError>(HttpResponse::default()) });
    let service = TrustedBlockHttpService::new(json, Some(cookie), Some(submit));

    let complete = StreamBody::new(
        futures::stream::iter([Ok::<_, std::io::Error>(Frame::data(Bytes::from_static(&[
            1, 2, 3,
        ])))])
        .boxed(),
    );
    let connection = tokio::spawn(send(
        service.clone(),
        request(&auth).body(complete).unwrap(),
    ));
    assert_eq!(started.recv().await.unwrap().as_ref(), &[1, 2, 3]);
    connection.abort();
    assert!(connection.await.unwrap_err().is_cancelled());

    // The detached submission still owns one slot: 15 incomplete bodies fill the rest.
    let mut incomplete = vec![];
    for _ in 0..15 {
        let body = StreamBody::new(
            futures::stream::pending::<Result<Frame<Bytes>, std::io::Error>>().boxed(),
        );
        let mut future = Box::pin(send(service.clone(), request(&auth).body(body).unwrap()));
        assert!(futures::poll!(&mut future).is_pending());
        incomplete.push(future);
    }
    let empty = || {
        request(&auth)
            .body(StreamBody::new(
                futures::stream::empty::<Result<Frame<Bytes>, std::io::Error>>().boxed(),
            ))
            .unwrap()
    };
    assert_eq!(
        send(service.clone(), empty()).await.unwrap().status(),
        StatusCode::TOO_MANY_REQUESTS
    );

    release_tx.send(()).unwrap();
    finished
        .await
        .expect("the disconnected client's submission runs to completion");
    // On this single-threaded runtime the submission task runs from that send to its end,
    // releasing its slot, before this test resumes.
    assert_eq!(
        send(service, empty()).await.unwrap().status(),
        StatusCode::OK
    );
    drop(incomplete);
}

/// A client that sends a complete request and closes at once. Hyper can read the whole body,
/// see the close and fail the connection before it polls the handler, so the submission must
/// already own the body when hyper calls the service. Returns the submitted body length.
async fn submitted_after_send_then_close(tls: bool, body_len: usize) -> Option<usize> {
    use tokio::io::{AsyncWrite, AsyncWriteExt};

    let (cookie, auth) = credentials();
    let (submitted_tx, mut submitted) = tokio::sync::mpsc::unbounded_channel();
    let submit: Submit = Arc::new(move |bytes: Bytes| {
        submitted_tx.send(bytes.len()).unwrap();
        Box::pin(async { Ok(SubmitBlockResponse::Accepted) })
    });
    let json = tower::service_fn(|_: Request<hyper::body::Incoming>| async {
        Ok::<_, tower::BoxError>(HttpResponse::default())
    });
    let service = TrustedBlockHttpService::new(json, Some(cookie), Some(submit));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    // Driven in this task rather than spawned: the connection future's `Send` proof hits a
    // higher-ranked lifetime limitation for this test-local service.
    let server: std::pin::Pin<Box<dyn std::future::Future<Output = ()>>> = if tls {
        Box::pin(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let stream = super::tls::test_acceptor().accept(socket).await.unwrap();
            let _ = super::super::serve_tls_connection(
                stream,
                service,
                std::future::pending(),
                super::super::TLS_HEADER_READ_TIMEOUT,
            )
            .await;
        })
    } else {
        Box::pin(super::super::run_listener(listener, service))
    };

    let mut request = format!(
        "POST /submittrustedblock HTTP/1.1\r\nHost: localhost\r\nAuthorization: {auth}\r\n\
         Content-Type: application/octet-stream\r\nContent-Length: {body_len}\r\n\r\n"
    )
    .into_bytes();
    request.resize(request.len() + body_len, 7);
    async fn send_and_close(mut stream: impl AsyncWrite + Unpin, request: &[u8]) {
        stream.write_all(request).await.unwrap();
        stream.shutdown().await.unwrap();
    }
    let client = async {
        let socket = tokio::net::TcpStream::connect(address).await.unwrap();
        if tls {
            let stream = super::http_client::tls_connector()
                .connect(
                    rustls::pki_types::ServerName::try_from("localhost").unwrap(),
                    socket,
                )
                .await
                .unwrap();
            send_and_close(stream, &request).await;
        } else {
            send_and_close(socket, &request).await;
        }
        tokio::time::timeout(std::time::Duration::from_secs(5), submitted.recv())
            .await
            .ok()
            .flatten()
    };
    tokio::select! {
        submitted = client => submitted,
        () = async { server.await; std::future::pending().await } => unreachable!(),
    }
}

#[tokio::test]
async fn trusted_http_submits_a_single_read_request_whose_client_closed_at_once() {
    assert_eq!(submitted_after_send_then_close(false, 300).await, Some(300));
}

#[tokio::test]
async fn trusted_http_submits_a_multi_frame_body_whose_client_closed_at_once() {
    assert_eq!(
        submitted_after_send_then_close(false, 64 * 1024).await,
        Some(64 * 1024)
    );
}

#[tokio::test]
async fn trusted_https_submits_a_single_read_request_whose_client_closed_at_once() {
    assert_eq!(submitted_after_send_then_close(true, 300).await, Some(300));
}

#[tokio::test]
async fn trusted_https_submits_a_multi_frame_body_whose_client_closed_at_once() {
    assert_eq!(
        submitted_after_send_then_close(true, 64 * 1024).await,
        Some(64 * 1024)
    );
}

/// The plain listener also serves HTTP/2, which calls the same service per stream.
#[tokio::test]
async fn trusted_and_json_routes_answer_over_http2() {
    let (cookie, auth) = credentials();
    let submit: Submit = Arc::new(|bytes: Bytes| {
        assert_eq!(bytes.as_ref(), &[1, 2, 3]);
        Box::pin(async { Ok(SubmitBlockResponse::Accepted) })
    });
    let json = tower::service_fn(|_: Request<hyper::body::Incoming>| async {
        Ok::<_, tower::BoxError>(HttpResponse::new(super::super::http::HttpBody::from(
            "json",
        )))
    });
    let service = TrustedBlockHttpService::new(json, Some(cookie), Some(submit));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(super::super::run_listener(listener, service));

    let stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let (mut client, connection) = hyper::client::conn::http2::handshake(
        hyper_util::rt::TokioExecutor::new(),
        hyper_util::rt::TokioIo::new(stream),
    )
    .await
    .unwrap();
    let connection = tokio::spawn(connection);
    for (path, body, expected) in [
        ("/submittrustedblock", vec![1, 2, 3], "accepted"),
        ("/", vec![], "json"),
    ] {
        let response = client
            .send_request(
                request(&auth)
                    .uri(format!("http://{address}{path}"))
                    .body(Full::new(Bytes::from(body)))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.version(), hyper::Version::HTTP_2);
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            expected
        );
    }
    connection.abort();
    server.abort();
}
