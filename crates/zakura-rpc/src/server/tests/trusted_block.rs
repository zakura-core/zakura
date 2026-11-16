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
use tower::ServiceExt;

use super::super::{
    cookie::{self, Cookie},
    http::HttpResponse,
    trusted_block::{Submit, TrustedBlockHttpService},
};
use crate::{config::rpc::Config, methods::types::submit_block::SubmitBlockResponse};

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
fn trusted_submission_requires_an_authenticated_primary_listener() {
    let mut config = Config::default();
    assert!(!config.enable_trusted_block_submission);
    config.enable_trusted_block_submission = true;
    assert!(config.validate().is_err());
    config.listen_addr = Some("127.0.0.1:0".parse().unwrap());
    assert!(config.validate().is_ok());
    config.enable_cookie_auth = false;
    config.allow_unauthenticated_admin = true;
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
        ("invalid", false, StatusCode::UNAUTHORIZED),
        (auth.as_str(), true, StatusCode::FORBIDDEN),
    ] {
        let body = StreamBody::new(futures::stream::poll_fn(
            |_| -> std::task::Poll<Option<Result<Frame<Bytes>, std::io::Error>>> {
                panic!("unauthorized request body must not be polled")
            },
        ));
        let mut builder = request(authorization);
        if origin {
            builder = builder.header("Origin", "https://example.invalid");
        }
        let response = service
            .clone()
            .oneshot(builder.body(body).unwrap())
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
    let response = service
        .clone()
        .oneshot(request(&auth).body(make_body(vec![0, 255, 128])).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "accepted"
    );
    let response = service
        .clone()
        .oneshot(
            request(&auth)
                .method("GET")
                .body(make_body(vec![]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    let response = service
        .clone()
        .oneshot(
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
    let response = service
        .clone()
        .oneshot(request(&auth).body(make_body(vec![0; length])).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let json = tower::service_fn(|_| async { Ok::<_, tower::BoxError>(HttpResponse::default()) });
    let disabled = TrustedBlockHttpService::new(json, Some(cookie), None);
    assert_eq!(
        disabled
            .oneshot(request(&auth).body(make_body(vec![])).unwrap())
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn trusted_http_capacity_is_released_when_incomplete_clients_disconnect() {
    use futures::StreamExt;
    let (cookie, auth) = credentials();
    let submit: Submit = Arc::new(|_| Box::pin(async { Ok(SubmitBlockResponse::Accepted) }));
    let json = tower::service_fn(|_| async { Ok::<_, tower::BoxError>(HttpResponse::default()) });
    let service = TrustedBlockHttpService::new(json, Some(cookie), Some(submit));
    let mut pending = vec![];
    for _ in 0..16 {
        let body = StreamBody::new(
            futures::stream::pending::<Result<Frame<Bytes>, std::io::Error>>().boxed(),
        );
        let mut future = Box::pin(service.clone().oneshot(request(&auth).body(body).unwrap()));
        assert!(futures::poll!(&mut future).is_pending());
        pending.push(future);
    }
    let response = service
        .clone()
        .oneshot(
            request(&auth)
                .body(StreamBody::new(
                    futures::stream::empty::<Result<Frame<Bytes>, std::io::Error>>().boxed(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    drop(pending);
    let response = service
        .oneshot(
            request(&auth)
                .body(StreamBody::new(
                    futures::stream::empty::<Result<Frame<Bytes>, std::io::Error>>().boxed(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn trusted_http_cannot_enable_without_a_cookie() {
    let submit: Submit = Arc::new(|_| panic!("missing credentials must disable the handler"));
    let json = tower::service_fn(|_| async { Ok::<_, tower::BoxError>(HttpResponse::default()) });
    let service = TrustedBlockHttpService::new(json, None, Some(submit));
    assert_eq!(
        service
            .oneshot(request("").body(Full::new(Bytes::new())).unwrap())
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
    let response = service
        .oneshot(request(&auth).body(body).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
}
