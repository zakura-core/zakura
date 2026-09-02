use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

use turmoil_net::{
    fixture::ClientServer,
    shim::tokio::net::{TcpListener, TcpStream},
    Latency,
};

use crate::zakura::{
    run_native_initiator_control, run_native_responder_control, NativeHandshakeNegotiated,
    TokioControlHandshakeClock, ZakuraHandlerError,
};

use super::{
    runner::{report, RunReport, TokioRead, TokioWrite},
    scenario::{FaultAction, HandshakeScenario},
    trace::TraceRecorder,
};

type RoleResult = Result<NativeHandshakeNegotiated, ZakuraHandlerError>;

pub(super) fn run_turmoil(scenario: &HandshakeScenario) -> RunReport {
    let (initiator_config, initiator_limits, responder_config, responder_limits) =
        scenario.policies();
    let initiator_id = scenario.initiator_id();
    let initiator_local_id = initiator_id.clone();
    let responder_result = Arc::new(Mutex::new(None::<RoleResult>));
    let server_result = responder_result.clone();
    let responder_done = Arc::new(AtomicBool::new(false));
    let server_done = responder_done.clone();
    let client_done = responder_done;
    let recorder = TraceRecorder::for_scenario(scenario);
    let server_recorder = recorder.clone();
    let client_recorder = recorder.clone();
    let server_scenario = scenario.clone();
    let client_scenario = scenario.clone();

    let initiator = ClientServer::new()
        .server("server", async move {
            let listener = TcpListener::bind("0.0.0.0:32000")
                .await
                .expect("the Turmoil responder binds");
            let (stream, _) = listener
                .accept()
                .await
                .expect("the Turmoil responder accepts");
            let (recv, send) = stream.into_split();
            let mut recv = TokioRead(recv);
            let mut send = TokioWrite::new(send, &server_scenario, false);
            let result = run_native_responder_control(
                &mut send,
                &mut recv,
                &responder_limits,
                &responder_config,
                &initiator_id,
                [2; 32],
                &TokioControlHandshakeClock,
                &server_recorder,
            )
            .await;
            *server_result
                .lock()
                .expect("the Turmoil responder result mutex is not poisoned") = Some(result);
            server_done.store(true, Ordering::Release);
        })
        .run("client", async move {
            if let FaultAction::DelayMillis(delay) = client_scenario.fault {
                turmoil_net::rule(Latency::fixed(
                    client_scenario
                        .timeout()
                        .min(std::time::Duration::from_millis(u64::from(delay))),
                ))
                .forget();
            }
            let stream = TcpStream::connect("server:32000")
                .await
                .expect("the Turmoil initiator connects");
            let (recv, send) = stream.into_split();
            let mut recv = TokioRead(recv);
            let mut send = TokioWrite::new(send, &client_scenario, true);
            let result = run_native_initiator_control(
                &mut send,
                &mut recv,
                &initiator_limits,
                &initiator_config,
                &initiator_local_id,
                [1; 32],
                &TokioControlHandshakeClock,
                &client_recorder,
            )
            .await;
            while !client_done.load(Ordering::Acquire) {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            result
        });

    let responder = responder_result
        .lock()
        .expect("the Turmoil responder result mutex is not poisoned")
        .take()
        .unwrap_or(Err(ZakuraHandlerError::Closed));
    let trace = recorder.snapshot();
    report(initiator, responder, trace, None)
}
