//! Tests for RPC metrics labels.

use std::{
    collections::BTreeSet,
    future::{ready, Future},
    sync::Mutex,
};

use futures::executor::block_on;
use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};

use crate::server::{
    jsonrpc::{Id, MethodResponse, Notification, Request},
    rpc_metrics::RpcMetricsMiddleware,
    rpc_service::RpcServiceT,
};

/// A metrics recorder that keeps the key of every metric registered with it.
#[derive(Default)]
struct KeyRecorder(Mutex<Vec<Key>>);

impl KeyRecorder {
    /// Records the key of a newly registered metric.
    fn register(&self, key: &Key) {
        self.0
            .lock()
            .expect("test recorder lock is never poisoned")
            .push(key.clone());
    }

    /// Returns every distinct `method` label value registered so far.
    fn method_labels(&self) -> BTreeSet<String> {
        self.0
            .lock()
            .expect("test recorder lock is never poisoned")
            .iter()
            .flat_map(Key::labels)
            .filter(|label| label.key() == "method")
            .map(|label| label.value().to_owned())
            .collect()
    }
}

impl Recorder for KeyRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        self.register(key);
        Counter::noop()
    }

    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        self.register(key);
        Gauge::noop()
    }

    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        self.register(key);
        Histogram::noop()
    }
}

/// An inner RPC service that answers every call with a successful `null` result.
#[derive(Clone)]
struct NullResult;

impl RpcServiceT for NullResult {
    fn call<'a>(&self, request: Request<'a>) -> impl Future<Output = MethodResponse> + Send + 'a {
        ready(MethodResponse::response(request.id, &Ok(()), usize::MAX))
    }

    fn notification<'a>(
        &self,
        _notification: Notification<'a>,
    ) -> impl Future<Output = MethodResponse> + Send + 'a {
        ready(MethodResponse::notification())
    }
}

/// Calls to unregistered method names share the `unknown` metrics label, so a
/// client can't create new metric series by sending arbitrary names.
#[test]
fn unregistered_methods_share_one_metrics_label() {
    let recorder = KeyRecorder::default();
    let service = RpcMetricsMiddleware::new(NullResult);
    let long_name = "x".repeat(1 << 20);

    metrics::with_local_recorder(&recorder, || {
        for method in ["getblockcount", "made-up-method", long_name.as_str()] {
            block_on(service.call(Request::new(method, None, Id::Number(1))));
        }
    });

    assert_eq!(
        recorder.method_labels(),
        BTreeSet::from(["getblockcount".to_owned(), "unknown".to_owned()]),
    );
}
