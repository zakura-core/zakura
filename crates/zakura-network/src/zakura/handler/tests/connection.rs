//! Native connection setup shared by paired transport fixtures.

use super::*;
use tokio_util::task::AbortOnDropHandle;

impl ZakuraProtocolHandler {
    /// Drive both setup phases directly in tests that isolate admission rules.
    pub(in crate::zakura::handler) async fn admit_bi_stream(
        &self,
        send: SendStream,
        recv: RecvStream,
        admission: &mut StreamAdmission<'_>,
        queue_depth: usize,
        exits: mpsc::UnboundedSender<SessionExit>,
        pending: &mut PendingSessions,
    ) -> Option<AdmittedSession> {
        let incoming = self.begin_bi_stream_setup(send, recv, admission)?.await?;
        self.finish_bi_stream_setup(incoming, admission, queue_depth, exits, pending)
    }
}

/// Connect `client` to `address`, take `held` of the peer's stream credits,
/// and serve the connection with `handler`. Holding the returned streams
/// without writing to them withholds those credits from the handler's own
/// opens.
pub(in crate::zakura::handler) async fn connect_holding_credits_and_serve(
    client: &QuicEndpoint,
    address: NodeAddr,
    handler: ZakuraProtocolHandler,
    limits: ZakuraLocalLimits,
    alpn: &[u8],
    deadline: Duration,
    held: usize,
) -> Result<
    (
        Connection,
        Vec<(SendStream, RecvStream)>,
        AbortOnDropHandle<Result<(), ZakuraHandlerError>>,
    ),
    BoxError,
> {
    let remote_id = address.id;
    let local_id = client.local_id();
    let connection = timeout(deadline, client.connect(address, &[alpn])).await??;
    let local_peer = ZakuraPeerId::new(local_id.as_bytes().to_vec())?;
    let remote_peer = ZakuraPeerId::new(remote_id.as_bytes().to_vec())?;
    let conn = ZakuraConnTrace::without_peer(1);
    let negotiated = timeout(
        deadline,
        run_native_initiator_handshake(
            &connection,
            &limits,
            &handler.current_handshake_config(),
            &local_peer,
            &ZakuraTrace::noop(),
            &conn,
        ),
    )
    .await??;
    let mut held_streams = Vec::with_capacity(held);
    for _ in 0..held {
        held_streams.push(timeout(deadline, connection.open_bi()).await??);
    }
    let remote_ip = Some(connection.admitted_ip());
    let serving_connection = connection.clone();
    let transport = AbortOnDropHandle::new(tokio::spawn(async move {
        handler
            .register_and_serve(
                serving_connection,
                remote_peer,
                remote_ip,
                ConnectionServeContext {
                    limits: limits.clamp(&negotiated.limits),
                    accepted_capabilities: negotiated.accepted_capabilities,
                    role: "initiator",
                    direction: ServicePeerDirection::Outbound,
                    transcript_hash: native_connection_transcript_hash(
                        ServicePeerDirection::Outbound,
                        &local_id,
                        &remote_id,
                    ),
                    i_open_collision_winner: i_open_collision_winner(&local_id, &remote_id),
                    conn,
                    control_handshake: None,
                    registration_report: None,
                },
            )
            .await
    }));
    Ok((connection, held_streams, transport))
}
