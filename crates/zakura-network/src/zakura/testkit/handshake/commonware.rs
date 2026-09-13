use std::{future::Future, io, net::SocketAddr, time::Duration};

use bytes::Bytes;
use commonware_runtime::{deterministic, Clock, Listener, Network, Runner, Sink, Stream};
use rand::RngCore;

use crate::zakura::{
    run_native_initiator_control, run_native_responder_control, ControlHandshakeClock, ControlRead,
    ControlWrite, NativeHandshakeNegotiated, ZakuraHandlerError,
};

use super::{
    runner::{frame_payload, report, RunReport},
    scenario::{FaultAction, HandshakeScenario},
    trace::TraceRecorder,
};

struct CommonwareRead<R>(R);

impl<R: Stream> ControlRead for CommonwareRead<R> {
    async fn read_exact(&mut self, bytes: &mut [u8]) -> Result<(), ZakuraHandlerError> {
        self.0.recv(&mut *bytes).await.map_err(commonware_error)
    }
}

struct CommonwareWrite<W> {
    inner: W,
    clock: deterministic::Context,
    scenario: HandshakeScenario,
    from_initiator: bool,
    has_length: bool,
}

impl<W> CommonwareWrite<W> {
    fn new(
        inner: W,
        clock: deterministic::Context,
        scenario: &HandshakeScenario,
        from_initiator: bool,
    ) -> Self {
        Self {
            inner,
            clock,
            scenario: scenario.clone(),
            from_initiator,
            has_length: false,
        }
    }
}

impl<W: Sink> ControlWrite for CommonwareWrite<W> {
    async fn write_all(&mut self, bytes: &[u8]) -> Result<(), ZakuraHandlerError> {
        if !self.has_length {
            self.has_length = true;
            return Ok(());
        }
        if matches!(
            (self.from_initiator, self.scenario.fault),
            (true, FaultAction::StallBeforeHello) | (false, FaultAction::StallBeforeAck)
        ) {
            std::future::pending::<()>().await;
        }
        if matches!(
            (self.from_initiator, self.scenario.fault),
            (true, FaultAction::CloseBeforeHello) | (false, FaultAction::CloseBeforeAck)
        ) {
            return Err(ZakuraHandlerError::Closed);
        }
        if matches!(
            (self.from_initiator, self.scenario.fault),
            (true, FaultAction::CancelBeforeHello) | (false, FaultAction::CancelBeforeAck)
        ) {
            return Err(ZakuraHandlerError::Io(io::Error::new(
                io::ErrorKind::Interrupted,
                "injected handshake cancellation",
            )));
        }
        if let FaultAction::DelayMillis(delay) = self.scenario.fault {
            Clock::sleep(&self.clock, Duration::from_millis(u64::from(delay))).await;
        }
        let framed = frame_payload(&self.scenario, self.from_initiator, bytes);
        let chunk_bytes = match self.scenario.fault {
            FaultAction::ShortWrites(chunk_bytes) => usize::from(chunk_bytes),
            _ => framed.len().max(1),
        };
        for chunk in framed.chunks(chunk_bytes) {
            self.inner
                .send(Bytes::copy_from_slice(chunk))
                .await
                .map_err(commonware_error)?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<(), ZakuraHandlerError> {
        Ok(())
    }
}

impl ControlHandshakeClock for deterministic::Context {
    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send {
        Clock::sleep(self, duration)
    }
}

fn commonware_error(error: commonware_runtime::Error) -> ZakuraHandlerError {
    ZakuraHandlerError::Io(io::Error::other(error.to_string()))
}

fn nonce(context: &mut deterministic::Context) -> [u8; 32] {
    let mut nonce = [0; 32];
    context.fill_bytes(&mut nonce);
    nonce
}

pub(super) fn run_commonware(scenario: &HandshakeScenario) -> RunReport {
    let scenario = scenario.clone();
    deterministic::Runner::seeded(scenario.seed).start(|context| async move {
        let address: SocketAddr = "127.0.0.1:31000"
            .parse()
            .expect("the fixed Commonware address is valid");
        let mut listener = context
            .bind(address)
            .await
            .expect("the deterministic listener binds");
        let auditor = context.auditor();
        let recorder = TraceRecorder::for_scenario(&scenario);
        let (initiator_config, initiator_limits, responder_config, responder_limits) =
            scenario.policies();
        let initiator_id = scenario.initiator_id();
        let initiator_local_id = initiator_id.clone();

        let mut responder_context = context.clone();
        let responder_recorder = recorder.clone();
        let responder_scenario = scenario.clone();
        let responder = async move {
            let (_, sink, stream) = listener
                .accept()
                .await
                .expect("the deterministic responder accepts");
            let mut send =
                CommonwareWrite::new(sink, responder_context.clone(), &responder_scenario, false);
            let mut recv = CommonwareRead(stream);
            let local_nonce = nonce(&mut responder_context);
            run_native_responder_control(
                &mut send,
                &mut recv,
                &responder_limits,
                &responder_config,
                &initiator_id,
                local_nonce,
                &responder_context,
                &responder_recorder,
            )
            .await
        };

        let mut initiator_context = context;
        let initiator_recorder = recorder.clone();
        let initiator_scenario = scenario.clone();
        let initiator = async move {
            let (sink, stream) = initiator_context
                .dial(address)
                .await
                .expect("the deterministic initiator dials");
            let mut send =
                CommonwareWrite::new(sink, initiator_context.clone(), &initiator_scenario, true);
            let mut recv = CommonwareRead(stream);
            let local_nonce = nonce(&mut initiator_context);
            run_native_initiator_control(
                &mut send,
                &mut recv,
                &initiator_limits,
                &initiator_config,
                &initiator_local_id,
                local_nonce,
                &initiator_context,
                &initiator_recorder,
            )
            .await
        };

        let (responder, initiator): (
            Result<NativeHandshakeNegotiated, ZakuraHandlerError>,
            Result<NativeHandshakeNegotiated, ZakuraHandlerError>,
        ) = futures::join!(responder, initiator);
        let trace = recorder.snapshot();
        report(initiator, responder, trace, Some(auditor.state()))
    })
}
