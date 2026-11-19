//! Handshake admission: one deadline from admission (conn-5), a TLS start
//! rate per address and per node (conn-7), and room for an honest peer while
//! other addresses hold their handshake share (conn-30).
//!
//! The tests drive `Acceptor::admit`, `cancel_admission` and
//! `finish_admission` as the QUIC endpoint does. An accepted attempt that
//! isn't cancelled is a TLS entry.

use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use crate::zakura::{
    handshake_cadence, node_handshake_cadence, Cadence, TransportEvent, TransportObserver,
};

/// Asserts `entries` ≤ capacity + one token per whole refill interval.
fn assert_within(entries: usize, cadence: Cadence, elapsed: Duration, what: &str) {
    let refills = elapsed.as_nanos() / cadence.refill_interval.as_nanos();
    let bound = u128::from(cadence.capacity) + refills;
    assert!(
        u128::try_from(entries).expect("a count fits u128") <= bound,
        "{what}: {entries} TLS entries exceed {bound} after {elapsed:?}"
    );
}

fn handler_with_defaults() -> ZakuraProtocolHandler {
    ZakuraProtocolHandler::new(
        ZakuraSupervisorHandle::new(DEFAULT_ZAKURA_MAX_CONNS_PER_IP),
        Network::Mainnet,
        ZakuraHandshakeConfig::for_network(&Network::Mainnet),
        ZakuraLocalLimits::from_config(&Config::default()),
    )
}

/// The QUIC endpoint's view of the attempts it is running.
#[derive(Default)]
struct Endpoint {
    in_flight: HashMap<IpAddr, usize>,
    /// Attempts that end at the next tick, and whether QUIC cancels them
    /// before TLS.
    ending: Vec<(IncomingInfo, bool)>,
    /// TLS entries per (address, validated) and per validation state.
    entries: HashMap<(IpAddr, bool), usize>,
    node_entries: [usize; 2],
    attempts: usize,
}

impl Endpoint {
    fn info(&self, ip: IpAddr, validated: bool) -> IncomingInfo {
        IncomingInfo {
            remote: SocketAddr::new(ip, 8234),
            validated,
            pending_total: self.in_flight.values().sum(),
            pending_from_ip: self.in_flight.get(&ip).copied().unwrap_or_default(),
        }
    }

    /// Starts an attempt from `ip`. A Retry makes the attacker answer with a
    /// validated Initial at once. Every fifth accepted attempt is cancelled
    /// at the next tick, after a token boundary may have passed.
    fn attempt(&mut self, handler: &ZakuraProtocolHandler, ip: IpAddr) -> Admit {
        let mut info = self.info(ip, false);
        let mut admitted = handler.admit(&info);
        if admitted == Admit::Retry {
            info = self.info(ip, true);
            admitted = handler.admit(&info);
        }
        if admitted == Admit::Accept {
            self.attempts += 1;
            *self.in_flight.entry(ip).or_default() += 1;
            *self.entries.entry((ip, info.validated)).or_default() += 1;
            self.node_entries[usize::from(info.validated)] += 1;
            self.ending.push((info, self.attempts.is_multiple_of(5)));
        }
        admitted
    }

    /// Ends last tick's attempts: QUIC cancels some before TLS, and TLS
    /// fails for the rest, so each address may reopen at once.
    fn end_attempts(&mut self, handler: &ZakuraProtocolHandler) {
        for (info, cancelled) in std::mem::take(&mut self.ending) {
            let ip = info.remote.ip();
            if cancelled {
                handler.cancel_admission(&info);
                *self
                    .entries
                    .get_mut(&(ip, info.validated))
                    .expect("a cancelled attempt was accepted") -= 1;
                self.node_entries[usize::from(info.validated)] -= 1;
            } else {
                handler.finish_admission(false);
            }
            let count = self
                .in_flight
                .get_mut(&ip)
                .expect("an ending attempt is in flight");
            *count -= 1;
            if *count == 0 {
                self.in_flight.remove(&ip);
            }
        }
    }

    fn assert_within_cadence(&self, elapsed: Duration) {
        for (&(ip, validated), &entries) in &self.entries {
            let what = format!("address {ip}, validated {validated}");
            assert_within(
                entries,
                handshake_cadence(DEFAULT_ZAKURA_MAX_CONNS_PER_IP),
                elapsed,
                &what,
            );
        }
        for (validated, &entries) in self.node_entries.iter().enumerate() {
            assert_within(
                entries,
                node_handshake_cadence(),
                elapsed,
                &format!("node, validated {validated}"),
            );
        }
    }
}

/// Forty addresses each open three simultaneous attempts every millisecond
/// for three seconds on a paused clock, answer every Retry with a validated
/// Initial, and reopen as soon as TLS fails. TLS entries stay within the
/// cadence of each address and of the node, in both validation states, and
/// the node runs at its full rate.
#[tokio::test(start_paused = true)]
async fn tls_entries_stay_within_the_address_and_node_cadence() {
    let handler = handler_with_defaults();
    let mut endpoint = Endpoint::default();
    let started = Instant::now();
    for _ in 0..3_000 {
        endpoint.end_attempts(&handler);
        for host in 0..40u8 {
            for _ in 0..3 {
                endpoint.attempt(&handler, IpAddr::from([198, 51, 100, host]));
            }
        }
        endpoint.assert_within_cadence(started.elapsed());
        tokio::time::advance(Duration::from_millis(1)).await;
    }
    // The flood keeps both node buckets empty, so each admits about its rate.
    let rate = node_handshake_cadence().capacity;
    for entries in endpoint.node_entries {
        assert!(
            entries >= usize::try_from(3 * rate).expect("the rate fits usize"),
            "the node admitted {entries} TLS entries in 3 s"
        );
    }
}

/// One address floods as above. An honest address's first attempt is still
/// admitted every 100 ms, because one address's rate is below the node's.
#[tokio::test(start_paused = true)]
async fn a_fresh_address_admits_while_another_is_saturated() {
    let handler = handler_with_defaults();
    let mut endpoint = Endpoint::default();
    let attacker = IpAddr::from([203, 0, 113, 1]);
    let started = Instant::now();
    let mut refused = 0;
    for tick in 0..3_000u32 {
        endpoint.end_attempts(&handler);
        for _ in 0..3 {
            if endpoint.attempt(&handler, attacker) != Admit::Accept {
                refused += 1;
            }
        }
        if tick % 100 == 50 {
            let honest = IpAddr::from([192, 0, 2, u8::try_from(tick / 100).expect("30 ticks")]);
            assert_eq!(
                handler.admit(&endpoint.info(honest, false)),
                Admit::Accept,
                "an honest first attempt at {tick} ms"
            );
            handler.finish_admission(false);
        }
        endpoint.assert_within_cadence(started.elapsed());
        tokio::time::advance(Duration::from_millis(1)).await;
    }
    assert!(
        refused > 8_000,
        "the attacker is saturated: {refused} refusals"
    );
}

/// conn-30: five addresses fill their whole handshake share, 4 each, and an
/// honest address is still admitted. A sixth address fills the inbound share
/// of 24, and the honest address waits until an attempt ends, which the
/// handshake deadline bounds at `control_timeout` from admission.
#[test]
fn an_honest_address_admits_while_five_addresses_hold_their_share() {
    let handler = handler_with_defaults();
    let limits = ZakuraLocalLimits::from_config(&Config::default());
    let share = limits.max_pending_handshakes / PENDING_HANDSHAKE_SHARES_PER_IP;
    assert_eq!(
        (share, limits.max_inbound_handshakes()),
        (4, 24),
        "the figures below follow from these defaults"
    );
    let mut endpoint = Endpoint::default();
    let honest = IpAddr::from([192, 0, 2, 1]);
    for host in 0..6u8 {
        let ip = IpAddr::from([198, 51, 100, host]);
        for _ in 0..share {
            assert_eq!(endpoint.attempt(&handler, ip), Admit::Accept);
        }
        assert_ne!(endpoint.attempt(&handler, ip), Admit::Accept);
        let admitted = handler.admit(&endpoint.info(honest, false));
        if host < 5 {
            assert_eq!(admitted, Admit::Accept, "{} addresses hold slots", host + 1);
            handler.finish_admission(false);
        } else {
            assert_eq!(admitted, Admit::Retry, "six addresses fill the share");
        }
    }
    // One stalled attempt reaches its deadline and ends.
    let (info, _) = endpoint.ending.remove(0);
    handler.finish_admission(false);
    *endpoint
        .in_flight
        .get_mut(&info.remote.ip())
        .expect("the attempt is in flight") -= 1;
    assert_eq!(handler.admit(&endpoint.info(honest, false)), Admit::Accept);
    handler.finish_admission(false);
}

/// The control exchange gets only what TLS and the claim left of
/// `control_timeout`, measured from QUIC admission (conn-5).
#[tokio::test]
async fn the_control_exchange_gets_what_remains_of_the_admission_deadline() -> Result<(), BoxError>
{
    let _guard = zakura_test::init();
    let mut limits = ZakuraLocalLimits::from_config(&Config::default());
    limits.control_timeout = Duration::from_secs(1);
    let handler = ZakuraProtocolHandler::new(
        ZakuraSupervisorHandle::new(64),
        Network::Mainnet,
        ZakuraHandshakeConfig::for_network(&Network::Mainnet),
        limits,
    );
    let server = LocalEndpointFactory::new().endpoint(0x2_5001).await?;
    let (connection_tx, mut connection_rx) = mpsc::channel(1);
    let (stream_tx, _stream_rx) = mpsc::channel(1);
    server.serve(CaptureConnection {
        alpn: P2P_V2_ALPN,
        connection_tx,
        stream_tx,
    })?;
    let client = LocalEndpointFactory::new().endpoint(0x2_5002).await?;
    let _client_connection = timeout(
        Duration::from_secs(10),
        client.connect(
            LocalEndpointFactory::node_addr(&server).await,
            &[P2P_V2_ALPN],
        ),
    )
    .await??;
    let inbound = timeout(Duration::from_secs(10), connection_rx.recv())
        .await?
        .expect("the capture acceptor sends the accepted connection");

    // TLS and a slow claim use 600 ms of the deadline. The client never
    // opens the control stream.
    zakura_rt::time::sleep(Duration::from_millis(600)).await;
    let peer = ZakuraPeerId::new(inbound.remote_id().as_bytes().to_vec())?;
    let conn = ZakuraConnTrace::new(&handler.trace, trace_conn_id(&inbound), &peer);
    let control_started = Instant::now();
    let result = handler
        .run_native_responder_handshake_with_permit(
            &inbound,
            &peer,
            &conn,
            handler
                .claim_pending_handshake(true)
                .expect("the inbound handshake share has room"),
        )
        .await;
    let control = control_started.elapsed();
    assert!(
        matches!(
            result,
            Err(ZakuraHandlerError::Timeout("control handshake"))
        ),
        "got {result:?}"
    );
    assert!(inbound.admitted_at().elapsed() >= Duration::from_secs(1));
    assert!(
        control < Duration::from_millis(800),
        "the control exchange ran {control:?}, past what admission left"
    );
    client.shutdown().await;
    server.shutdown().await;
    Ok(())
}

/// Counts the cadence's charges and refunds.
#[derive(Debug, Default)]
struct CadenceActions {
    charges: AtomicUsize,
    refunds: AtomicUsize,
}

impl TransportObserver for CadenceActions {
    fn observe(&self, event: TransportEvent) {
        if let TransportEvent::HandshakeCadence { action, .. } = event {
            match action {
                "admitted" => self.charges.fetch_add(1, Ordering::Relaxed),
                "exhausted" | "node-exhausted" | "capacity" => 0,
                _ => self.refunds.fetch_add(1, Ordering::Relaxed),
            };
        }
    }
}

/// A peer that registers, closes and reconnects at once over real QUIC pays
/// one cadence token for each TLS handshake: registration refunds nothing,
/// so the address cadence bounds reconnects too (conn-7).
#[tokio::test]
async fn registered_reconnects_keep_their_cadence_charge() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let limits = ZakuraLocalLimits::from_config(&Config::default());
    let server = LocalEndpointFactory::with_limits(&limits)
        .endpoint(0x2_5003)
        .await?;
    let actions = Arc::new(CadenceActions::default());
    let handler = ZakuraProtocolHandler::new(
        ZakuraSupervisorHandle::new(DEFAULT_ZAKURA_MAX_CONNS_PER_IP),
        Network::Mainnet,
        ZakuraHandshakeConfig::for_network(&Network::Mainnet),
        limits.clone(),
    )
    .with_local_node_id(server.local_id())
    .with_transport_observer(actions.clone());
    server.serve(handler)?;
    let address = LocalEndpointFactory::node_addr(&server).await;
    let client = LocalEndpointFactory::with_limits(&limits)
        .endpoint(0x2_5004)
        .await?;
    let peer = ZakuraPeerId::new(client.local_id().as_bytes().to_vec())?;
    let config = ZakuraHandshakeConfig::for_network(&Network::Mainnet);

    let started = Instant::now();
    let mut entries = 0;
    for _ in 0..20 {
        match timeout(
            Duration::from_secs(10),
            client.connect(address.clone(), &[P2P_V2_ALPN]),
        )
        .await?
        {
            Ok(connection) => {
                entries += 1;
                run_native_initiator_handshake_without_trace(&connection, &limits, &config, &peer)
                    .await?;
                connection.close(0u32.into(), b"reconnect");
            }
            Err(zakura_quic::ConnectError::Refused) => {}
            Err(error) => return Err(error.into()),
        }
    }
    assert!(entries > 0);
    assert_eq!(actions.charges.load(Ordering::Relaxed), entries);
    assert_eq!(actions.refunds.load(Ordering::Relaxed), 0);
    // An unvalidated and a validated ledger may each charge the address.
    let cadence = handshake_cadence(DEFAULT_ZAKURA_MAX_CONNS_PER_IP);
    let refills = started.elapsed().as_nanos() / cadence.refill_interval.as_nanos();
    let bound = 2 * (u128::from(cadence.capacity) + refills);
    assert!(
        u128::try_from(entries)? <= bound,
        "{entries} TLS entries exceed {bound}"
    );
    client.shutdown().await;
    server.shutdown().await;
    Ok(())
}
