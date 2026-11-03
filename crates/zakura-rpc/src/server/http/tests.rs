//! Admission and permit lifetime at the HTTP service boundary.

use std::sync::atomic::{AtomicUsize, Ordering};

use http_body_util::StreamBody;
use tokio::sync::Notify;

use super::*;
use crate::server::jsonrpc::{Notification, Request};

#[derive(Clone, Default)]
struct HeldRpc {
    calls: Arc<AtomicUsize>,
    release: Arc<Notify>,
}

impl RpcServiceT for HeldRpc {
    fn call<'a>(&self, request: Request<'a>) -> impl Future<Output = MethodResponse> + Send + 'a {
        let this = self.clone();
        async move {
            this.calls.fetch_add(1, Ordering::SeqCst);
            this.release.notified().await;
            MethodResponse::response(request.id, &Ok::<_, ErrorObject>(()), 1024)
        }
    }

    fn notification<'a>(
        &self,
        _: Notification<'a>,
    ) -> impl Future<Output = MethodResponse> + Send + 'a {
        std::future::ready(MethodResponse::notification())
    }
}

fn collected(body: &'static str) -> HttpRequest {
    let bytes = Bytes::from_static(body.as_bytes());
    let mut request = HttpRequest::builder()
        .method("POST")
        .header(header::CONTENT_TYPE, "application/json")
        .body(HttpBody::from(bytes.clone()))
        .unwrap();
    request.extensions_mut().insert(CollectedRequestBody(bytes));
    request
}

#[tokio::test]
async fn saturated_admission_precedes_compatibility_errors_and_dispatch() {
    let rpc = HeldRpc::default();
    let service = JsonRpcHttpService::new(rpc.clone(), 1000, 1024);
    let _permits = service
        .request_permits
        .clone()
        .try_acquire_many_owned(u32::try_from(MAX_CONCURRENT_REQUESTS).unwrap())
        .unwrap();
    let response = service.clone().call(collected("not JSON")).await.unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(rpc.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cancellation_releases_the_admitted_request_permit() {
    let rpc = HeldRpc::default();
    let mut service = JsonRpcHttpService::new(rpc.clone(), 1000, 1024);
    let mut response = service.call(collected(r#"{"method":"getinfo","params":[],"id":1}"#));
    assert!(futures::poll!(&mut response).is_pending());
    assert_eq!(rpc.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        service.request_permits.available_permits(),
        MAX_CONCURRENT_REQUESTS - 1
    );
    drop(response);
    assert_eq!(
        service.request_permits.available_permits(),
        MAX_CONCURRENT_REQUESTS
    );
}

#[tokio::test]
async fn completion_formats_the_legacy_response_and_releases_the_permit() {
    let rpc = HeldRpc::default();
    let mut service = JsonRpcHttpService::new(rpc.clone(), 1000, 1024);
    let mut response = service.call(collected(r#"{"method":"getinfo","params":[],"id":1}"#));
    assert!(futures::poll!(&mut response).is_pending());
    rpc.release.notify_one();
    let response = response.await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        r#"{"id":1,"result":null,"error":null}"#
    );
    assert_eq!(
        service.request_permits.available_permits(),
        MAX_CONCURRENT_REQUESTS
    );
}

#[tokio::test]
async fn malformed_and_failed_body_reads_release_the_permit() {
    let mut service = JsonRpcHttpService::new(HeldRpc::default(), 1000, 1024);
    assert_eq!(
        service.call(collected("not JSON")).await.unwrap().status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        service.request_permits.available_permits(),
        MAX_CONCURRENT_REQUESTS
    );

    let mut request = collected(r#"{"method":"getinfo","params":[],"id":1}"#);
    request
        .headers_mut()
        .insert(header::CONTENT_LENGTH, "11000000".parse().unwrap());
    let response = service.call(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        r#"{"id":null,"result":null,"error":{"code":-32007,"message":"Request is too big","data":"Exceeded max limit of 10485760"}}"#
    );
    assert_eq!(
        service.request_permits.available_permits(),
        MAX_CONCURRENT_REQUESTS
    );

    let body = StreamBody::new(futures::stream::iter([Err::<
        http_body::Frame<Bytes>,
        BoxError,
    >("connection reset".into())]));
    let request = HttpRequest::builder()
        .method("POST")
        .header(header::CONTENT_TYPE, "application/json")
        .body(body)
        .unwrap();
    assert_eq!(
        service.call(request).await.unwrap().status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        service.request_permits.available_permits(),
        MAX_CONCURRENT_REQUESTS
    );
}
