//! Native connection setup shared by paired transport fixtures.

use super::*;
use tokio_util::task::AbortOnDropHandle;

pub(in crate::zakura::handler) async fn connect_and_serve(
    client: &Endpoint,
    address: NodeAddr,
    handler: ZakuraProtocolHandler,
    limits: ZakuraLocalLimits,
    alpn: &[u8],
    deadline: Duration,
) -> Result<
    (
        Connection,
        AbortOnDropHandle<Result<(), ZakuraHandlerError>>,
    ),
    BoxError,
> {
    let remote_id = address.node_id;
    let local_id = client.node_id();
    let connection = timeout(deadline, client.connect(address, alpn)).await??;
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
    let serving_connection = connection.clone();
    let transport = AbortOnDropHandle::new(tokio::spawn(async move {
        handler
            .register_and_serve(
                serving_connection,
                remote_peer,
                None,
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
                },
            )
            .await
    }));
    Ok((connection, transport))
}
