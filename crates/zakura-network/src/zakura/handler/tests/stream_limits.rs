//! Check that peers are offered only the streams the transport can open.
//!
//! Negotiation must preserve lower local or remote limits. A raw QUIC peer
//! checks the actual stream capacity in both directions, including its boundary.

use super::{connection::RawConnection, *};
use proptest::prelude::*;

fn check_negotiation(configured: u16, remote: u16, expected_local: u16) {
    let mut local = ZakuraLocalLimits::from_config(&Config::default());
    local.max_open_streams = configured;
    let handler = ZakuraProtocolHandler::new_with_registry(
        ZakuraSupervisorHandle::new(1),
        Network::Mainnet,
        ZakuraHandshakeConfig::for_network(&Network::Mainnet),
        local.clone(),
        Arc::new(ServiceRegistry::new(vec![Arc::new(NoopService)]).unwrap()),
    );
    assert_eq!(local.initial_limits().max_open_streams, expected_local);
    let proposed = ZakuraInitialLimits {
        max_open_streams: remote,
        ..local.initial_limits()
    };
    let accepted = handler.accepted_limits_for(&proposed);
    assert_eq!(accepted.max_open_streams, expected_local.min(remote));
    assert_eq!(
        local.clamp(&accepted).max_open_streams,
        expected_local.min(remote)
    );
    // The final local guard also protects a caller passing unclamped peer limits.
    assert_eq!(
        local.clamp(&proposed).max_open_streams,
        expected_local.min(remote)
    );
}

#[test]
fn stream_limit_negotiation_boundaries() {
    for (configured, expected) in [
        (0, 1),
        (1, 1),
        (3, 3),
        (15, 15),
        (16, 16),
        (17, 16),
        (u16::MAX, 16),
    ] {
        for remote in [1, 3, 15, 16, 17, u16::MAX] {
            check_negotiation(configured, remote, expected);
        }
    }
}

proptest! {
    #[test]
    fn generated_stream_limits_preserve_lower_peer_and_local_bounds(
        configured in any::<u16>(), remote in 1..=u16::MAX,
    ) {
        let expected = match configured {
            0 => 1,
            1..=16 => configured,
            _ => 16,
        };
        check_negotiation(configured, remote, expected);
    }
}

#[tokio::test]
async fn advertised_stream_limit_matches_quic_capacity_in_both_directions() -> Result<(), BoxError>
{
    let _guard = zakura_test::init();
    const ALPN: &[u8] = b"/zakura/test/stream-limits/1";
    const DEADLINE: Duration = Duration::from_secs(5);
    for configured in [0, 3, 16, 17, u16::MAX] {
        let mut local = ZakuraLocalLimits::from_config(&Config::default());
        local.max_open_streams = configured;
        let server = LocalEndpointFactory::with_transport_config(local.transport_config())
            .endpoint(92351)
            .await?;
        let client = LocalEndpointFactory::with_transport_config(local.transport_config())
            .endpoint(92352)
            .await?;
        let (accepted, mut connections) = mpsc::channel(1);
        let router = Router::builder(server)
            .accept(ALPN, RawConnection(accepted))
            .spawn();
        let address = LocalEndpointFactory::node_addr(router.endpoint()).await;
        let connection = timeout(DEADLINE, client.connect(address, ALPN)).await??;
        let remote = timeout(DEADLINE, connections.recv())
            .await?
            .ok_or("missing connection")?;
        let mut held = Vec::new();
        for (opener, receiver) in [(&connection, &remote), (&remote, &connection)] {
            for _ in 0..local.initial_limits().max_open_streams {
                let (mut send, recv) = timeout(DEADLINE, opener.open_bi()).await??;
                // Sending a byte makes the new stream visible to the other peer.
                timeout(DEADLINE, send.write_all(&[42])).await??;
                let (remote_send, mut remote_recv) =
                    timeout(DEADLINE, receiver.accept_bi()).await??;
                let mut byte = [0];
                timeout(DEADLINE, remote_recv.read_exact(&mut byte)).await??;
                assert_eq!(byte, [42]);
                held.push((send, recv, remote_send, remote_recv));
            }
            // Keep every stream open so none can return credit for one more.
            assert!(
                timeout(Duration::from_millis(100), opener.open_bi())
                    .await
                    .is_err(),
                "QUIC must stop at the advertised limit for configured={configured}"
            );
        }
        connection.close(0u32.into(), b"test complete");
        drop(held);
        timeout(DEADLINE, client.close()).await?;
        timeout(DEADLINE, router.shutdown()).await??;
    }
    Ok(())
}
