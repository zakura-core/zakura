//! The serving ownership suite, run against a scripted blocking service.

pub(crate) mod kit;

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use tokio::sync::{Mutex, OwnedMutexGuard};

use super::*;
use crate::zakura::Frame;
use kit::{serving_ownership_suite, ServingUnderTest};

/// A service whose work runs on a blocking thread behind a test-held gate.
#[derive(Debug, Default)]
pub(super) struct ScriptedServe {
    gate: Arc<Mutex<()>>,
    fail_next: AtomicBool,
}

const SCRIPTED_RESPONSE_CAP: u32 = 64;

impl Serve for ScriptedServe {
    type Request = u32;

    fn response_cap(&self, _request: &u32) -> u32 {
        SCRIPTED_RESPONSE_CAP
    }

    async fn produce(
        &self,
        request: u32,
        lease: WorkLease,
        sink: ResponseSink,
    ) -> Result<Responded, ServeEnd> {
        let gate = self.gate.clone();
        let fail = self.fail_next.swap(false, Ordering::SeqCst);
        // The blocking task owns a lease clone, like a real storage query.
        tokio::task::spawn_blocking(move || {
            let _lease = lease;
            drop(gate.blocking_lock());
        })
        .await
        .map_err(|error| ServeEnd::LocalFault(error.to_string()))?;
        if fail {
            return Err(ServeEnd::LocalFault("scripted failure".into()));
        }
        sink.respond(Frame {
            message_type: 1,
            flags: 0,
            payload: request.to_le_bytes().to_vec(),
        })
    }
}

/// Adapter for [`ScriptedServe`].
#[derive(Debug, Default)]
struct ScriptedServeUnderTest {
    serve: Arc<ScriptedServe>,
}

impl ServingUnderTest for ScriptedServeUnderTest {
    type Serve = ScriptedServe;
    type Stall = OwnedMutexGuard<()>;

    async fn new() -> Self {
        Self::default()
    }

    fn serve(&self) -> Arc<ScriptedServe> {
        self.serve.clone()
    }

    fn request(&self, seq: u32) -> u32 {
        seq
    }

    fn max_response_bytes(&self) -> u32 {
        SCRIPTED_RESPONSE_CAP
    }

    async fn stall(&self) -> Self::Stall {
        self.serve.gate.clone().lock_owned().await
    }

    fn fail_next(&self) {
        self.serve.fail_next.store(true, Ordering::SeqCst);
    }

    fn assert_response(&self, frame: &Frame, seq: u32) {
        assert_eq!(frame.payload, seq.to_le_bytes());
    }
}

serving_ownership_suite!(scripted_serve_ownership, ScriptedServeUnderTest);

#[test]
fn a_response_above_its_bound_is_a_local_fault() {
    let sink = ResponseSink::new(8);
    let frame = Frame {
        message_type: 1,
        flags: 0,
        payload: vec![0; 1],
    };
    assert!(matches!(sink.respond(frame), Err(ServeEnd::LocalFault(_))));
}
