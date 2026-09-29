//! The suite's properties, generic over the adapter and the layout.
//!
//! Each property runs real nodes over loopback QUIC. A property returns an
//! error, never panics mid-way, so the test reports what it was waiting for.

use std::{
    collections::HashSet,
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

use super::{
    node::production_limits, LayoutNode, LayoutPlan, RawLayoutPeer, RequestPlan, StreamConformance,
    CONFORMANCE_DEADLINE, SIBLINGS,
};
use crate::{
    zakura::{
        regulation::UNFINISHED_EXCHANGE,
        testkit::await_until,
        wire_codec::{decode_frame, encode_frame},
        Frame, MessageRole, MessageRule, Stream, DEFAULT_ZAKURA_RECEIVE_WINDOW,
        DEFAULT_ZAKURA_STREAM_RECEIVE_WINDOW, FRAME_HEADER_BYTES,
    },
    BoxError,
};

/// Endpoint seeds for one property. Each property uses its own range, and
/// each layout its own offset, so parallel runs never share an identity.
fn seeds(property: u64, layout: &[Stream]) -> u64 {
    // Widening usize to u64 is lossless on supported targets.
    95_000 + property * 1_000 + layout.len() as u64 * 100
}

/// Every capability a raw peer offers: the layout's and the siblings'.
fn capabilities(layout: &[Stream]) -> u64 {
    layout
        .iter()
        .chain(&SIBLINGS)
        .fold(0, |capabilities, stream| capabilities | stream.capability)
}

fn encode<A: StreamConformance>(row: &MessageRule, exchange: u32) -> Result<Frame, BoxError> {
    encode_frame(&A::message(row, exchange)).map_err(|error| error.to_string().into())
}

/// Send one request as a raw peer and read its response to the ending.
async fn raw_exchange<A: StreamConformance>(
    raw: &mut RawLayoutPeer,
    plan: &RequestPlan,
    exchange: u32,
) -> Result<(), BoxError> {
    raw.send(plan.stream, &encode::<A>(plan.row, exchange)?)
        .await?;
    read_ending::<A>(raw, plan).await.map(drop)
}

/// Read a raw peer's responses until the next ending, and return the
/// ending's exchange.
async fn read_ending<A: StreamConformance>(
    raw: &mut RawLayoutPeer,
    plan: &RequestPlan,
) -> Result<u32, BoxError> {
    loop {
        let frame = raw.recv(plan.response_stream).await?;
        let message = decode_frame::<A::Message>(&frame).map_err(|error| error.to_string())?;
        if frame.message_type == plan.end.message_type {
            return Ok(A::exchange(&message));
        }
    }
}

/// A raw peer connected to `victim` with the layout open, once the victim
/// holds the session.
async fn raw_peer<A: StreamConformance>(
    victim: &LayoutNode<A>,
    layout: &'static [Stream],
    seed: u64,
) -> Result<(RawLayoutPeer, u64), BoxError> {
    let mut raw = RawLayoutPeer::connect(
        victim.addr().await,
        &victim.limits,
        seed,
        capabilities(layout),
    )
    .await?;
    raw.open_layout(layout).await?;
    let session = victim.wait_session(&raw.id()).await?;
    Ok((raw, session.key.session_id))
}

fn ensure(condition: bool, failure: impl FnOnce() -> String) -> Result<(), BoxError> {
    if condition {
        Ok(())
    } else {
        Err(failure().into())
    }
}

/// P1: an unsolicited response disconnects its sender before any frame of it
/// reaches the handler, and a control peer keeps working.
pub(crate) async fn unsolicited_response_disconnects<A: StreamConformance>(
    layout: &'static [Stream],
) -> Result<(), BoxError> {
    let seed = seeds(1, layout);
    let victim = LayoutNode::<A>::spawn(seed, layout).await?;
    let control = LayoutNode::<A>::spawn(seed + 1, layout).await?;
    victim.connect(&control).await?;
    let control_session = victim.wait_session(&control.id()).await?.key;
    let plan = LayoutPlan::new(layout);
    for (offset, &(row, member)) in (2..).zip(&plan.responses) {
        let (mut raw, _) = raw_peer(&victim, layout, seed + offset).await?;
        let handled = victim.shared().handled.load(Ordering::Relaxed);
        raw.send(member, &encode::<A>(row, 7)?).await?;
        raw.closed().await?;
        victim.wait_disconnected(&raw.id()).await?;
        ensure(
            victim.shared().handled.load(Ordering::Relaxed) == handled,
            || {
                format!(
                    "row {} reached the handler before its refusal",
                    row.message_type
                )
            },
        )?;
        raw.shutdown().await;
    }
    ensure(
        victim.shared().table.get(&control.id()).map(|(key, _)| key) == Some(control_session),
        || "the control peer's session changed".to_string(),
    )?;
    control.download(&victim.id(), 0..1).await?;
    victim.download(&control.id(), 0..1).await?;
    victim.siblings[0].probe(&control.id()).await?;
    victim.shutdown().await;
    control.shutdown().await;
    Ok(())
}

/// P2: `limit` requests re-sent the moment each ending arrives never fault;
/// `2 × limit` requests sent at once are all served and never fault; request
/// `2 × limit + 1` disconnects.
pub(crate) async fn request_margin<A: StreamConformance>(
    layout: &'static [Stream],
) -> Result<(), BoxError> {
    let seed = seeds(2, layout);
    let victim = LayoutNode::<A>::spawn(seed, layout).await?;
    for (index, plan) in victim.shared().plan.requests.clone().iter().enumerate() {
        let seed = seed + 10 * u64::try_from(index)?;
        let limit = plan.max_in_flight;
        let serving = &victim.shared().serving[index];

        // Exactly `limit` open, each re-sent at its ending, for many rounds.
        let (mut raw, _) = raw_peer(&victim, layout, seed + 1).await?;
        for exchange in 0..limit {
            raw.send(plan.stream, &encode::<A>(plan.row, exchange)?)
                .await?;
        }
        for exchange in limit..limit * 32 {
            read_ending::<A>(&mut raw, plan).await?;
            raw.send(plan.stream, &encode::<A>(plan.row, exchange)?)
                .await?;
        }
        for _ in 0..limit {
            read_ending::<A>(&mut raw, plan).await?;
        }
        ensure(!raw.is_closed(), || {
            "a conformant requester was disconnected".into()
        })?;
        ensure(serving.over_limit_count() == 0, || {
            "a conformant requester was counted over its limit".into()
        })?;
        raw.shutdown().await;

        // `2 × limit` at once, while serving waits: served and traced.
        let (mut raw, _) = raw_peer(&victim, layout, seed + 2).await?;
        let session = victim.wait_session(&raw.id()).await?;
        let hold = serving.hold_node_for_test();
        for exchange in 0..2 * limit {
            raw.send(plan.stream, &encode::<A>(plan.row, exchange)?)
                .await?;
        }
        await_until("every request admitted", CONFORMANCE_DEADLINE, || {
            session.serving_open(victim.shared(), plan.row.message_type) == 2 * limit
        })
        .await?;
        ensure(serving.over_limit_count() == u64::from(limit), || {
            format!(
                "{} requests counted over the limit",
                serving.over_limit_count()
            )
        })?;
        drop(hold);
        let mut ended = HashSet::new();
        for _ in 0..2 * limit {
            ensure(
                ended.insert(read_ending::<A>(&mut raw, plan).await?),
                || "an exchange ended twice".into(),
            )?;
        }
        ensure(!raw.is_closed(), || {
            "requests within the margin were refused".into()
        })?;
        raw.shutdown().await;

        // One more than the margin disconnects.
        let (mut raw, _) = raw_peer(&victim, layout, seed + 3).await?;
        let hold = serving.hold_node_for_test();
        for exchange in 0..=2 * limit {
            // The victim may close before the last request is written.
            if raw
                .send(plan.stream, &encode::<A>(plan.row, exchange)?)
                .await
                .is_err()
            {
                break;
            }
        }
        raw.closed().await?;
        drop(hold);
        let violations = victim.shared().violations();
        ensure(
            violations
                .iter()
                .any(|violation| violation.contains("open requests")),
            || format!("the disconnect was not the margin's: {violations:?}"),
        )?;
        raw.shutdown().await;
    }
    victim.shutdown().await;
    Ok(())
}

/// P3: two nodes that download from each other at once, with every request
/// row's full window, never fault.
pub(crate) async fn sustained_exchanges<A: StreamConformance>(
    layout: &'static [Stream],
) -> Result<(), BoxError> {
    let seed = seeds(3, layout);
    let a = LayoutNode::<A>::spawn(seed, layout).await?;
    let b = LayoutNode::<A>::spawn(seed + 1, layout).await?;
    a.connect(&b).await?;
    let (a_id, b_id) = (a.id(), b.id());
    let (from_b, from_a) = tokio::join!(a.download(&b_id, 0..256), b.download(&a_id, 0..256));
    from_b?;
    from_a?;
    for (node, peer) in [(&a.service.shared, b.id()), (&b.service.shared, a.id())] {
        ensure(node.violations().is_empty(), || {
            format!("a conformant peer faulted: {:?}", node.violations())
        })?;
        ensure(
            node.serving
                .iter()
                .all(|serving| serving.over_limit_count() == 0),
            || "a conformant peer was counted over its limit".into(),
        )?;
        ensure(node.table.get(&peer).is_some(), || "a session ended".into())?;
    }
    ensure(a.connected(&b.id()) && b.connected(&a.id()), || {
        "the connection closed".into()
    })?;
    a.shutdown().await;
    b.shutdown().await;
    Ok(())
}

/// P4: two nodes whose serving is saturated, each by the other's requests,
/// both finish their downloads once capacity returns, and the siblings keep
/// working meanwhile.
pub(crate) async fn mutual_saturation<A: StreamConformance>(
    layout: &'static [Stream],
) -> Result<(), BoxError> {
    let seed = seeds(4, layout);
    let a = LayoutNode::<A>::spawn(seed, layout).await?;
    let b = LayoutNode::<A>::spawn(seed + 1, layout).await?;
    a.connect(&b).await?;
    let limit = a.shared().plan.requests[0].max_in_flight;
    let holds = [
        a.shared().serving[0].hold_node_for_test(),
        b.shared().serving[0].hold_node_for_test(),
    ];
    let saturate = async {
        await_until("both nodes' serving waits", CONFORMANCE_DEADLINE, || {
            a.shared().serving[0].active_and_waiting().1 >= 1
                && b.shared().serving[0].active_and_waiting().1 >= 1
        })
        .await?;
        a.siblings[0].probe(&b.id()).await?;
        b.siblings[1].probe(&a.id()).await?;
        drop(holds);
        Ok::<_, BoxError>(())
    };
    let (a_id, b_id) = (a.id(), b.id());
    let (from_b, from_a, saturated) = tokio::join!(
        a.download(&b_id, 0..limit * 4),
        b.download(&a_id, 0..limit * 4),
        saturate,
    );
    saturated?;
    from_b?;
    from_a?;
    ensure(a.connected(&b.id()) && b.connected(&a.id()), || {
        "the connection closed".into()
    })?;
    a.shutdown().await;
    b.shutdown().await;
    Ok(())
}

/// P5: ending one member retires the whole session, other services on the
/// connection keep working, and the layout reopens. With a started exchange
/// and no ending, the fence closes the connection instead.
pub(crate) async fn member_retirement<A: StreamConformance>(
    layout: &'static [Stream],
) -> Result<(), BoxError> {
    let seed = seeds(5, layout);
    let victim = LayoutNode::<A>::spawn(seed, layout).await?;
    let plan = victim.shared().plan.requests[0];
    for member in 0..layout.len() {
        let seed = seed + 10 * u64::try_from(member)?;

        // No started exchange: only the session retires.
        let (mut raw, old) = raw_peer(&victim, layout, seed + 1).await?;
        raw.reset(member)?;
        for other in (0..layout.len()).filter(|&other| other != member) {
            raw.ended(other).await?;
        }
        raw.probe_sibling(&SIBLINGS[0]).await?;
        raw.open_layout(layout).await?;
        victim.wait_session_after(&raw.id(), Some(old)).await?;
        raw_exchange::<A>(&mut raw, &plan, 1).await?;
        ensure(!raw.is_closed(), || {
            "retiring a member closed the connection".into()
        })?;
        raw.shutdown().await;

        // A started exchange: the fence closes the connection.
        let (mut raw, _) = raw_peer(&victim, layout, seed + 2).await?;
        let session = victim.wait_session(&raw.id()).await?;
        victim
            .service
            .request(&raw.id(), plan.row.message_type, 5)
            .await?;
        // The request's bytes arrived, so its write started.
        raw.recv(plan.stream).await?;
        raw.reset(member)?;
        raw.closed().await?;
        ensure(
            session.close_cause.get_or("unset") == UNFINISHED_EXCHANGE,
            || format!("closed for {}", session.close_cause.get_or("unset")),
        )?;
        raw.shutdown().await;
    }
    victim.shutdown().await;
    Ok(())
}

/// Paused sibling streams the connection window supports: a paused stream
/// holds at most one stream window, and the connection's credit update
/// waits until an eighth of the connection window is consumed (#981).
fn supported_paused_siblings() -> u32 {
    let connection = DEFAULT_ZAKURA_RECEIVE_WINDOW;
    (connection - connection / 8) / DEFAULT_ZAKURA_STREAM_RECEIVE_WINDOW
}

/// Wait until `sibling`'s flood has queued a full stream window.
///
/// The paused receiver then holds most of its stream window. A lower fill
/// only weakens the property; it cannot fail it.
async fn wait_filled(sibling: &super::SiblingService) -> Result<(), BoxError> {
    let window = u64::from(DEFAULT_ZAKURA_STREAM_RECEIVE_WINDOW);
    await_until("a sibling filled its window", CONFORMANCE_DEADLINE, || {
        sibling.filled() >= window
    })
    .await?;
    Ok(())
}

/// P6: paused siblings up to the supported count do not stop the layout.
/// One more exhausts the connection's credit; once the siblings resume, the
/// exchange that was waiting completes.
pub(crate) async fn paused_siblings<A: StreamConformance>(
    layout: &'static [Stream],
) -> Result<(), BoxError> {
    let supported = usize::try_from(supported_paused_siblings())?;
    ensure(SIBLINGS.len() > supported, || {
        "the harness needs one sibling more than the supported count".into()
    })?;
    let seed = seeds(6, layout);
    let a = LayoutNode::<A>::spawn(seed, layout).await?;
    let b = LayoutNode::<A>::spawn(seed + 1, layout).await?;
    a.connect(&b).await?;
    let limit = a.shared().plan.requests[0].max_in_flight;

    let mut floods = Vec::new();
    for sibling in 0..supported {
        a.siblings[sibling].pause(true);
        floods.push(b.siblings[sibling].flood(&a.id())?);
        wait_filled(&b.siblings[sibling]).await?;
    }
    a.download(&b.id(), 0..limit * 4).await?;
    b.download(&a.id(), 0..limit * 4).await?;

    a.siblings[supported].pause(true);
    floods.push(b.siblings[supported].flood(&a.id())?);
    wait_filled(&b.siblings[supported]).await?;
    let handled = b.shared().handled.load(Ordering::Relaxed);
    let resume = async {
        // Let the request reach the peer before credit returns, so the
        // response is the part that waits.
        await_until(
            "the stalled exchange's request",
            CONFORMANCE_DEADLINE,
            || b.shared().handled.load(Ordering::Relaxed) > handled,
        )
        .await?;
        floods.clear();
        for sibling in &a.siblings {
            sibling.pause(false);
        }
        Ok::<_, BoxError>(())
    };
    let b_id = b.id();
    let (downloaded, resumed) = tokio::join!(a.download(&b_id, 1_000..1_001), resume);
    resumed?;
    downloaded?;
    for sibling in &a.siblings {
        sibling.probe(&b.id()).await?;
    }
    ensure(a.connected(&b.id()), || "the connection closed".into())?;
    a.shutdown().await;
    b.shutdown().await;
    Ok(())
}

/// P7: a stream at a version the peers did not negotiate is refused, and the
/// connection stays usable.
pub(crate) async fn unnegotiated_version<A: StreamConformance>(
    layout: &'static [Stream],
) -> Result<(), BoxError> {
    let seed = seeds(7, layout);
    let victim = LayoutNode::<A>::spawn(seed, layout).await?;
    let plan = victim.shared().plan.requests[0];
    let mut raw = RawLayoutPeer::connect(
        victim.addr().await,
        &victim.limits,
        seed + 1,
        capabilities(layout),
    )
    .await?;
    let member = &layout[plan.stream];
    let (mut send, mut recv) = raw.open_stream(member.kind, member.version + 1).await?;
    let request = encode::<A>(plan.row, 3)?;
    // The victim may reset the stream before the frame is written.
    let _ = send
        .write_all(&request.encode(victim.limits.max_frame_bytes)?)
        .await;
    tokio::time::timeout(CONFORMANCE_DEADLINE, async {
        let mut buffer = [0; 64];
        while let Ok(Some(_)) = recv.read(&mut buffer).await {}
    })
    .await
    .map_err(|_| "the victim did not refuse the stream")?;
    ensure(!raw.is_closed(), || {
        "the refusal closed the connection".into()
    })?;
    raw.open_layout(layout).await?;
    victim.wait_session(&raw.id()).await?;
    raw_exchange::<A>(&mut raw, &plan, 4).await?;
    raw.probe_sibling(&SIBLINGS[0]).await?;
    raw.shutdown().await;
    victim.shutdown().await;
    Ok(())
}

/// The first row on `member` that a raw peer may send unsolicited: any row
/// but a response.
fn unsolicited_row(layout: &[Stream], member: usize) -> Option<&'static MessageRule> {
    layout[member]
        .messages?
        .iter()
        .find(|row| !matches!(row.role, MessageRole::Response { .. }))
}

/// P8: on every member, a frame cut off by a reset retires only the
/// session. A frame cut off by a FIN inside its payload closes the
/// connection; inside its header, the transport reads the FIN as the
/// stream's end and retires only the session. No cut frame reaches the
/// handler. A frame that stalls part-way ends at the read deadline, even
/// while the connection stays active.
pub(crate) async fn partial_frames<A: StreamConformance>(
    layout: &'static [Stream],
) -> Result<(), BoxError> {
    let seed = seeds(8, layout);
    let victim = LayoutNode::<A>::spawn(seed, layout).await?;
    let mut offset = 1;
    for member in 0..layout.len() {
        let Some(row) = unsolicited_row(layout, member) else {
            // A member that carries only responses cannot be written to
            // without a reservation, and the header check refuses it first.
            continue;
        };
        let bytes = encode::<A>(row, 9)?.encode(victim.limits.max_frame_bytes)?;
        for split in [1, bytes.len() - 1] {
            for reset in [true, false] {
                let (mut raw, old) = raw_peer(&victim, layout, seed + offset).await?;
                offset += 1;
                let handled = victim.shared().handled.load(Ordering::Relaxed);
                raw.write(member, &bytes[..split]).await?;
                if reset {
                    raw.reset(member)?;
                } else {
                    raw.finish(member)?;
                }
                if !reset && split >= FRAME_HEADER_BYTES {
                    raw.closed().await?;
                } else {
                    await_until("the session's retirement", CONFORMANCE_DEADLINE, || {
                        victim
                            .shared()
                            .table
                            .get(&raw.id())
                            .is_none_or(|(key, _)| key.session_id != old)
                    })
                    .await?;
                    raw.probe_sibling(&SIBLINGS[0]).await?;
                    ensure(!raw.is_closed(), || {
                        format!("a cut at byte {split} closed the connection")
                    })?;
                }
                ensure(
                    victim.shared().handled.load(Ordering::Relaxed) == handled,
                    || format!("a frame cut at byte {split} reached the handler"),
                )?;
                raw.shutdown().await;
            }
        }
    }
    victim.shutdown().await;

    // A stalled frame, while a sibling keeps the connection active.
    let mut limits = production_limits();
    limits.quic_idle_timeout = Duration::from_secs(4);
    limits.keep_alive_interval = Duration::from_secs(1);
    let victim = LayoutNode::<A>::spawn_with_limits(seed + 50, layout, limits.clone()).await?;
    let (member, row) = (0..layout.len())
        .find_map(|member| unsolicited_row(layout, member).map(|row| (member, row)))
        .ok_or("the layout has no row a raw peer may send")?;
    let bytes = encode::<A>(row, 9)?.encode(limits.max_frame_bytes)?;
    let mut raw = RawLayoutPeer::connect(
        victim.addr().await,
        &limits,
        seed + 51,
        capabilities(layout),
    )
    .await?;
    raw.open_layout(layout).await?;
    victim.wait_session(&raw.id()).await?;
    let started = Instant::now();
    raw.write(member, &bytes[..bytes.len() - 1]).await?;
    while !raw.is_closed() {
        ensure(started.elapsed() < CONFORMANCE_DEADLINE, || {
            "a stalled frame never ended".into()
        })?;
        // A failed probe means the connection is closing.
        if raw.probe_sibling(&SIBLINGS[0]).await.is_err() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    raw.closed().await?;
    ensure(started.elapsed() >= limits.quic_idle_timeout / 2, || {
        "the stalled frame ended before its read deadline".into()
    })?;
    raw.shutdown().await;
    victim.shutdown().await;
    Ok(())
}
