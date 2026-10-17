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
    node::production_limits, service::encode_update, LayoutNode, LayoutPlan, LayoutSession,
    RawLayoutPeer, RequestPlan, StreamConformance, SubscriptionPlan, UpdateOp,
    CONFORMANCE_DEADLINE, SIBLINGS,
};
use crate::{
    zakura::{
        regulation::{ServeCapacity, Update, UNFINISHED_EXCHANGE},
        testkit::await_until,
        wire_codec::{decode_frame, encode_frame},
        Credit, Frame, MessageRole, MessageRule, Stream, DEFAULT_ZAKURA_RECEIVE_WINDOW,
        DEFAULT_ZAKURA_STREAM_RECEIVE_WINDOW, FRAME_HEADER_BYTES,
    },
    BoxError,
};

/// Endpoint seeds for one property: its own range, offset by the layout's
/// member count. Layouts with the same member count share seeds, which is
/// harmless because each test's endpoints dial each other's loopback addresses
/// directly.
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

/// Credit that adds nothing, as `Close` carries.
const NO_CREDIT: Credit = Credit {
    objects: 0,
    bytes: 0,
};

/// An `op` update of subscription `key` on `subscription`'s row that
/// acknowledges the start, as a raw peer that accepts nothing sends it.
fn update_frame<A: StreamConformance>(
    subscription: &SubscriptionPlan,
    op: UpdateOp,
    key: u32,
    sequence: u32,
    added: Credit,
) -> Result<Frame, BoxError> {
    let acknowledged = 0;
    let update = Update {
        sequence,
        acknowledged,
        added,
    };
    encode_update::<A>(subscription.row, op, key, &update)
}

/// Page `cursor` of subscription 0 on `subscription`'s page row.
fn page_frame<A: StreamConformance>(
    subscription: &SubscriptionPlan,
    cursor: u32,
) -> Result<Frame, BoxError> {
    encode_frame(&A::page(subscription.page, 0, cursor)).map_err(|error| error.to_string().into())
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

/// Open subscription `key` as a raw peer with the row's whole window, read
/// its first page, then close it and read to its ending.
async fn raw_subscription<A: StreamConformance>(
    raw: &mut RawLayoutPeer,
    subscription: &SubscriptionPlan,
    key: u32,
) -> Result<(), BoxError> {
    let credit = subscription.limits.credit;
    raw.send(
        subscription.stream,
        &update_frame::<A>(subscription, UpdateOp::Open, key, 0, credit)?,
    )
    .await?;
    let frame = raw.recv(subscription.response_stream).await?;
    let message = decode_frame::<A::Message>(&frame).map_err(|error| error.to_string())?;
    ensure(A::read_page(&message) == Some((key, 1)), || {
        format!("expected page 1 of {key}, got row {}", frame.message_type)
    })?;
    raw.send(
        subscription.stream,
        &update_frame::<A>(subscription, UpdateOp::Close, key, 1, NO_CREDIT)?,
    )
    .await?;
    let ended = read_subscription_ending::<A>(raw, subscription).await?;
    ensure(ended == key, || {
        format!("subscription {key} ended as {ended}")
    })
}

/// One raw-peer exchange on each kind of row the layout has: request
/// `exchange` of the first request row, then subscription `exchange` of the
/// first subscription row.
async fn raw_exchanges<A: StreamConformance>(
    raw: &mut RawLayoutPeer,
    plan: &LayoutPlan,
    exchange: u32,
) -> Result<(), BoxError> {
    if let Some(request) = plan.requests.first() {
        raw_exchange::<A>(raw, request, exchange).await?;
    }
    if let Some(subscription) = plan.subscriptions.first() {
        raw_subscription::<A>(raw, subscription, exchange).await?;
    }
    Ok(())
}

/// Read a raw peer's frames on `member` until the next `end` row, and return
/// its exchange.
async fn read_until<A: StreamConformance>(
    raw: &mut RawLayoutPeer,
    member: usize,
    end: &MessageRule,
) -> Result<u32, BoxError> {
    loop {
        let frame = raw.recv(member).await?;
        let message = decode_frame::<A::Message>(&frame).map_err(|error| error.to_string())?;
        if frame.message_type == end.message_type {
            return Ok(A::exchange(&message));
        }
    }
}

/// Read a raw peer's responses until the next ending, and return the
/// ending's exchange.
async fn read_ending<A: StreamConformance>(
    raw: &mut RawLayoutPeer,
    plan: &RequestPlan,
) -> Result<u32, BoxError> {
    read_until::<A>(raw, plan.response_stream, plan.end).await
}

/// Read a raw peer's pages until the next ending, and return the ended
/// subscription's key.
async fn read_subscription_ending<A: StreamConformance>(
    raw: &mut RawLayoutPeer,
    subscription: &SubscriptionPlan,
) -> Result<u32, BoxError> {
    read_until::<A>(raw, subscription.response_stream, subscription.end).await
}

/// Four windows of each kind of row: exchanges of the first request row and
/// pages of the first subscription row. A missing row gets zero.
fn four_windows(plan: &LayoutPlan) -> (u32, u32) {
    let exchanges = plan
        .requests
        .first()
        .map_or(0, |request| request.max_in_flight.saturating_mul(4));
    let pages = plan.subscriptions.first().map_or(0, |subscription| {
        subscription.limits.credit.objects.saturating_mul(4)
    });
    (exchanges, pages)
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
/// reaches the handler, and a control peer keeps working. So does a page
/// beyond a live subscription's credit, after the pages within it reached the
/// handler.
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
    for (offset, subscription) in (50..).zip(&plan.subscriptions) {
        let (mut raw, _) = raw_peer(&victim, layout, seed + offset).await?;
        let session = victim.wait_session(&raw.id()).await?;
        let credit = subscription.limits.credit;
        victim
            .service
            .subscribe(&raw.id(), subscription.row.message_type, 0, credit)
            .await?;
        // The victim's `Open`.
        raw.recv(subscription.stream).await?;
        let pages = pages_within::<A>(subscription, credit)?;
        let handled = victim.shared().handled.load(Ordering::Relaxed) + u64::from(pages);
        for cursor in 1..=pages {
            raw.send(
                subscription.response_stream,
                &page_frame::<A>(subscription, cursor)?,
            )
            .await?;
        }
        // `handled` counts a frame before the handler spends its credit, so
        // wait for the accepted pages. Otherwise the reader could still see
        // credit for the extra page.
        await_until("the pages within the credit", CONFORMANCE_DEADLINE, || {
            *session.pages.borrow() == u64::from(pages)
        })
        .await?;
        // The victim may close before the page is written.
        let _ = raw
            .send(
                subscription.response_stream,
                &page_frame::<A>(subscription, pages + 1)?,
            )
            .await;
        raw.closed().await?;
        victim.wait_disconnected(&raw.id()).await?;
        ensure(
            victim.shared().handled.load(Ordering::Relaxed) == handled,
            || "a page beyond its credit reached the handler".into(),
        )?;
        raw.shutdown().await;
    }
    ensure(
        victim.shared().table.get(&control.id()).map(|(key, _)| key) == Some(control_session),
        || "the control peer's session changed".to_string(),
    )?;
    control.exchange(&victim.id(), 0..1, 1).await?;
    victim.exchange(&control.id(), 0..1, 1).await?;
    victim.siblings[0].probe(&control.id()).await?;
    victim.shutdown().await;
    control.shutdown().await;
    Ok(())
}

/// P2: `limit` requests re-sent the moment each ending arrives never fault;
/// `2 × limit` requests sent at once are all served and never fault; request
/// `2 × limit + 1` disconnects. Each subscription row keeps the same margin
/// over `max_live`.
pub(crate) async fn request_margin<A: StreamConformance>(
    layout: &'static [Stream],
) -> Result<(), BoxError> {
    let seed = seeds(2, layout);
    let victim = LayoutNode::<A>::spawn(seed, layout).await?;
    for (index, plan) in victim.shared().plan.requests.clone().iter().enumerate() {
        let seed = seed + 10 * u64::try_from(index)?;
        let limit = plan.max_in_flight;
        let serving = &victim.shared().serving[index];

        // Refill at every ending for at least one complete window. Small
        // windows exercise more rounds without turning large limits into load tests.
        let exchanges = (limit * 2).max(256).min(limit * 32);
        let (mut raw, _) = raw_peer(&victim, layout, seed + 1).await?;
        for exchange in 0..limit {
            raw.send(plan.stream, &encode::<A>(plan.row, exchange)?)
                .await?;
        }
        for exchange in limit..exchanges {
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
    for (index, subscription) in victim
        .shared()
        .plan
        .subscriptions
        .clone()
        .iter()
        .enumerate()
    {
        subscription_margin::<A>(
            &victim,
            layout,
            subscription,
            seed + 50 + 10 * u64::try_from(index)?,
        )
        .await?;
    }
    victim.shutdown().await;
    Ok(())
}

/// P2 for one subscription row: `max_live` subscriptions re-opened at each
/// ending never fault; `2 × max_live` opened at once are all admitted, counted
/// over the limit, and ended after their `Close`; one more disconnects.
async fn subscription_margin<A: StreamConformance>(
    victim: &LayoutNode<A>,
    layout: &'static [Stream],
    subscription: &SubscriptionPlan,
    seed: u64,
) -> Result<(), BoxError> {
    let shared = victim.shared();
    let row = subscription.row.message_type;
    let stream = subscription.stream;
    let limit = subscription.limits.max_live;
    let credit = subscription.limits.credit;
    let open = |key| update_frame::<A>(subscription, UpdateOp::Open, key, 0, credit);
    let close = |key| update_frame::<A>(subscription, UpdateOp::Close, key, 1, NO_CREDIT);

    // Open again at every ending, for at least sixteen subscriptions.
    let subscriptions = (limit * 2).max(16).min(limit * 32);
    let (mut raw, _) = raw_peer(victim, layout, seed + 1).await?;
    let session = victim.wait_session(&raw.id()).await?;
    for key in 0..subscriptions {
        if key >= limit {
            read_subscription_ending::<A>(&mut raw, subscription).await?;
        }
        raw.send(stream, &open(key)?).await?;
        raw.send(stream, &close(key)?).await?;
    }
    for _ in 0..limit {
        read_subscription_ending::<A>(&mut raw, subscription).await?;
    }
    ensure(!raw.is_closed(), || {
        "a conformant subscriber was disconnected".into()
    })?;
    ensure(session.publishing(shared, row).1 == 0, || {
        "a conformant subscriber was counted over its limit".into()
    })?;
    raw.shutdown().await;

    // `2 × max_live` at once: admitted and counted.
    let (mut raw, _) = raw_peer(victim, layout, seed + 2).await?;
    let session = victim.wait_session(&raw.id()).await?;
    for key in 0..2 * limit {
        raw.send(stream, &open(key)?).await?;
    }
    let held = usize::try_from(2 * limit)?;
    await_until("every subscription admitted", CONFORMANCE_DEADLINE, || {
        session.publishing(shared, row).0 == held
    })
    .await?;
    let over_limit = session.publishing(shared, row).1;
    ensure(over_limit == u64::from(limit), || {
        format!("{over_limit} subscriptions counted over the limit")
    })?;
    for key in 0..2 * limit {
        raw.send(stream, &close(key)?).await?;
    }
    let mut ended = HashSet::new();
    for _ in 0..2 * limit {
        ensure(
            ended.insert(read_subscription_ending::<A>(&mut raw, subscription).await?),
            || "a subscription ended twice".into(),
        )?;
    }
    ensure(!raw.is_closed(), || {
        "subscriptions within the margin were refused".into()
    })?;
    raw.shutdown().await;

    // One more than the margin disconnects.
    let (mut raw, _) = raw_peer(victim, layout, seed + 3).await?;
    for key in 0..=2 * limit {
        // The victim may close before the last `Open` is written.
        if raw.send(stream, &open(key)?).await.is_err() {
            break;
        }
    }
    raw.closed().await?;
    let violations = shared.violations();
    ensure(
        violations
            .iter()
            .any(|violation| violation.contains("exceed twice the limit")),
        || format!("the disconnect was not the subscription margin's: {violations:?}"),
    )?;
    raw.shutdown().await;
    Ok(())
}

/// P3: two nodes that download from each other at once, with the first
/// request row's full window and a subscription that renews its credit
/// across at least four windows, never fault.
pub(crate) async fn sustained_exchanges<A: StreamConformance>(
    layout: &'static [Stream],
) -> Result<(), BoxError> {
    let seed = seeds(3, layout);
    let a = LayoutNode::<A>::spawn(seed, layout).await?;
    let b = LayoutNode::<A>::spawn(seed + 1, layout).await?;
    a.connect(&b).await?;
    let (a_id, b_id) = (a.id(), b.id());
    let pages = four_windows(&a.shared().plan).1.max(256);
    let (from_b, from_a) = tokio::join!(
        a.exchange(&b_id, 0..256, pages),
        b.exchange(&a_id, 0..256, pages)
    );
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
        let (_, session) = node.table.get(&peer).ok_or("a session ended")?;
        ensure(
            node.plan
                .subscriptions
                .iter()
                .all(|plan| session.publishing(node, plan.row.message_type).1 == 0),
            || "a conformant subscriber was counted over its limit".into(),
        )?;
    }
    ensure(a.connected(&b.id()) && b.connected(&a.id()), || {
        "the connection closed".into()
    })?;
    a.shutdown().await;
    b.shutdown().await;
    Ok(())
}

/// P4: two nodes whose serving and pushing are saturated, each by the other's
/// requests and subscription, both finish their downloads once capacity
/// returns, and the siblings keep working meanwhile.
pub(crate) async fn mutual_saturation<A: StreamConformance>(
    layout: &'static [Stream],
) -> Result<(), BoxError> {
    let seed = seeds(4, layout);
    let a = LayoutNode::<A>::spawn(seed, layout).await?;
    let b = LayoutNode::<A>::spawn(seed + 1, layout).await?;
    a.connect(&b).await?;
    let (exchanges, pages) = four_windows(&a.shared().plan);
    // The first request and subscription rows' capacity, on both nodes.
    let holds: Vec<_> = [&a, &b]
        .into_iter()
        .flat_map(|node| {
            let shared = node.shared();
            shared
                .serving
                .first()
                .into_iter()
                .chain(shared.pushing.first())
        })
        .map(ServeCapacity::hold_node_for_test)
        .collect();
    let saturate = async {
        await_until(
            "both nodes' serving and pushing wait",
            CONFORMANCE_DEADLINE,
            || {
                [&a, &b].into_iter().all(|node| {
                    let shared = node.shared();
                    let serving = shared
                        .serving
                        .first()
                        .is_none_or(|serving| serving.active_and_waiting().1 >= 1);
                    let pushing =
                        shared.pushing.is_empty() || shared.push_waits.load(Ordering::Relaxed) >= 1;
                    serving && pushing
                })
            },
        )
        .await?;
        a.siblings[0].probe(&b.id()).await?;
        b.siblings[1].probe(&a.id()).await?;
        drop(holds);
        Ok::<_, BoxError>(())
    };
    let (a_id, b_id) = (a.id(), b.id());
    let (from_b, from_a, saturated) = tokio::join!(
        a.exchange(&b_id, 0..exchanges, pages),
        b.exchange(&a_id, 0..exchanges, pages),
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
/// or subscription and no ending, the fence closes the connection instead.
pub(crate) async fn member_retirement<A: StreamConformance>(
    layout: &'static [Stream],
) -> Result<(), BoxError> {
    let seed = seeds(5, layout);
    let victim = LayoutNode::<A>::spawn(seed, layout).await?;
    let plan = LayoutPlan::new(layout);
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
        raw_exchanges::<A>(&mut raw, &plan, 1).await?;
        ensure(!raw.is_closed(), || {
            "retiring a member closed the connection".into()
        })?;
        raw.shutdown().await;

        // A started exchange: the fence closes the connection.
        if let Some(request) = plan.requests.first() {
            let (mut raw, _) = raw_peer(&victim, layout, seed + 2).await?;
            let session = victim.wait_session(&raw.id()).await?;
            victim
                .service
                .request(&raw.id(), request.row.message_type, 5)
                .await?;
            // The request's bytes arrived, so its write started.
            raw.recv(request.stream).await?;
            closes_unfinished(raw, &session, member).await?;
        }
        // A started subscription closes it the same way.
        if let Some(subscription) = plan.subscriptions.first() {
            let (mut raw, _) = raw_peer(&victim, layout, seed + 3).await?;
            let session = victim.wait_session(&raw.id()).await?;
            victim
                .service
                .subscribe(
                    &raw.id(),
                    subscription.row.message_type,
                    5,
                    subscription.limits.credit,
                )
                .await?;
            // The `Open`'s bytes arrived, so its write started.
            raw.recv(subscription.stream).await?;
            closes_unfinished(raw, &session, member).await?;
        }
    }
    victim.shutdown().await;
    Ok(())
}

/// Reset `member` while `session` has a started exchange, and check that the
/// fence closes the connection.
async fn closes_unfinished<A: StreamConformance>(
    mut raw: RawLayoutPeer,
    session: &LayoutSession<A>,
    member: usize,
) -> Result<(), BoxError> {
    raw.reset(member)?;
    raw.closed().await?;
    ensure(
        session.close_cause.get_or("unset") == UNFINISHED_EXCHANGE,
        || format!("closed for {}", session.close_cause.get_or("unset")),
    )?;
    raw.shutdown().await;
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
/// exchanges that were waiting complete.
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
    let (exchanges, pages) = four_windows(&a.shared().plan);

    let mut floods = Vec::new();
    for sibling in 0..supported {
        a.siblings[sibling].pause(true);
        floods.push(b.siblings[sibling].flood(&a.id())?);
        wait_filled(&b.siblings[sibling]).await?;
    }
    a.exchange(&b.id(), 0..exchanges, pages).await?;
    b.exchange(&a.id(), 0..exchanges, pages).await?;

    a.siblings[supported].pause(true);
    floods.push(b.siblings[supported].flood(&a.id())?);
    wait_filled(&b.siblings[supported]).await?;
    // The stalled exchanges' rows: the first request row and the first
    // subscription row, with the frames the peer handled on each so far.
    let plan = &a.shared().plan;
    let stalled: Vec<_> = plan
        .requests
        .first()
        .map(|request| request.row.message_type)
        .into_iter()
        .chain(
            plan.subscriptions
                .first()
                .map(|subscription| subscription.row.message_type),
        )
        .map(|row| (row, b.shared().handled_of(row)))
        .collect();
    let resume = async {
        // Let the request and the `Open` both reach the peer before credit
        // returns, so the responses are the part that waits. A
        // subscription's later updates follow its `Open`, so a new frame on
        // its row means the `Open` arrived.
        await_until(
            "the stalled exchanges' first frames",
            CONFORMANCE_DEADLINE,
            || {
                stalled
                    .iter()
                    .all(|&(row, before)| b.shared().handled_of(row) > before)
            },
        )
        .await?;
        floods.clear();
        for sibling in &a.siblings {
            sibling.pause(false);
        }
        Ok::<_, BoxError>(())
    };
    let b_id = b.id();
    let (downloaded, resumed) = tokio::join!(a.exchange(&b_id, 1_000..1_001, 1), resume);
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
    let plan = LayoutPlan::new(layout);
    // The first frame of an exchange: a request, or else an `Open`.
    let (stream, first) = match (plan.requests.first(), plan.subscriptions.first()) {
        (Some(request), _) => (request.stream, encode::<A>(request.row, 3)?),
        (None, Some(subscription)) => (
            subscription.stream,
            update_frame::<A>(
                subscription,
                UpdateOp::Open,
                3,
                0,
                subscription.limits.credit,
            )?,
        ),
        (None, None) => unreachable!("the plan requires a request or subscription row"),
    };
    let mut raw = RawLayoutPeer::connect(
        victim.addr().await,
        &victim.limits,
        seed + 1,
        capabilities(layout),
    )
    .await?;
    let member = &layout[stream];
    let (mut send, mut recv) = raw.open_stream(member.kind, member.version + 1).await?;
    // The victim may reset the stream before the frame is written.
    let _ = send
        .write_all(&first.encode(victim.limits.max_frame_bytes)?)
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
    raw_exchanges::<A>(&mut raw, &plan, 4).await?;
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

/// The pages of subscription 0 that `credit` covers, pushed greedily from
/// cursor 1 as the harness's publisher pushes them.
fn pages_within<A: StreamConformance>(
    subscription: &SubscriptionPlan,
    credit: Credit,
) -> Result<u32, BoxError> {
    let mut bytes = 0u64;
    let mut pages = 0;
    while pages < credit.objects {
        let frame = page_frame::<A>(subscription, pages + 1)?;
        // Widening usize to u64 is lossless on supported targets.
        bytes += frame.payload.len() as u64;
        if bytes > u64::from(credit.bytes) {
            break;
        }
        pages += 1;
    }
    Ok(pages)
}

/// P9: `Close` ends a subscription while its pages sit unread, every
/// execution slot and output byte is held, and a page waits for them. Other
/// control messages still progress on the connection. Once the peer reads
/// again, it finds every page in order, then the ending.
pub(crate) async fn close_progresses<A: StreamConformance>(
    layout: &'static [Stream],
) -> Result<(), BoxError> {
    close_progresses_with_wait::<A>(layout, false).await?;
    let send_waits = close_progresses_with_wait::<A>(layout, true).await?;
    ensure(
        LayoutPlan::new(layout).subscriptions.is_empty()
            || send_waits.is_some_and(|waits| waits > 0),
        || "P9 did not exercise Close while a page waited for send capacity".into(),
    )
}

/// Hold the response allowance while preserving the transport slot needed by Close.
async fn hold_response_allowance(
    send: &crate::zakura::FramedSend,
) -> Result<Vec<crate::zakura::transport::ResponseFrameSlot<'_>>, BoxError> {
    ensure(send.max_capacity() >= 2, || {
        "P9 allowance blocking requires a response slot and an independent control slot".to_owned()
    })?;
    let mut slots = Vec::new();
    for _ in 0..send.max_capacity() - 1 {
        slots.push(
            tokio::time::timeout(CONFORMANCE_DEADLINE, send.reserve_response_guarded())
                .await?
                .map_err(|error| format!("response slot: {error:?}"))?,
        );
    }
    Ok(slots)
}

/// Return the observed send waits after Close completes, or None for no subscriptions.
async fn close_progresses_with_wait<A: StreamConformance>(
    layout: &'static [Stream],
    allowance_only: bool,
) -> Result<Option<u64>, BoxError> {
    let plan = LayoutPlan::new(layout);
    let Some(subscription) = plan.subscriptions.first().copied() else {
        return Ok(None);
    };
    let credit = subscription.limits.credit;
    let update = |op, sequence, added| update_frame::<A>(&subscription, op, 0, sequence, added);
    // Open with all but one page of the window; a grant adds that page later.
    let largest = u32::try_from(subscription.page.payload.max())?;
    let one_page = Credit {
        objects: 1,
        bytes: largest,
    };
    let opening = (credit.objects >= 2 && credit.bytes >= 2 * largest).then(|| Credit {
        objects: credit.objects - one_page.objects,
        bytes: credit.bytes - one_page.bytes,
    });
    let pages = if allowance_only {
        u32::from(opening.is_some())
    } else {
        pages_within::<A>(&subscription, opening.unwrap_or(credit))?
    };
    let seed = seeds(9, layout);
    let victim = LayoutNode::<A>::spawn(seed, layout).await?;
    let control = LayoutNode::<A>::spawn(seed + 1, layout).await?;
    victim.connect(&control).await?;
    let (mut raw, _) = raw_peer(&victim, layout, seed + 2).await?;
    let shared = victim.shared();
    let session = victim.wait_session(&raw.id()).await?;
    let send = &session.sends[subscription.response_stream];
    let mut response_slots = Vec::new();
    if allowance_only && pages == 0 {
        // A one-page credit window cannot also leave an earlier page unread.
        // Block its first page instead of silently skipping allowance coverage.
        response_slots = hold_response_allowance(send).await?;
    }

    // Open, and read nothing.
    raw.send(
        subscription.stream,
        &update(
            UpdateOp::Open,
            0,
            if allowance_only {
                one_page
            } else {
                opening.unwrap_or(credit)
            },
        )?,
    )
    .await?;
    await_until(
        "the victim pushes every page the credit covers",
        CONFORMANCE_DEADLINE,
        || u64::from(pages) == shared.pushed.load(Ordering::Relaxed),
    )
    .await?;

    if allowance_only && pages > 0 {
        // An earlier page is already queued on QUIC. The next page must wait
        // without spending credit, so Close can cancel it and follow the first.
        response_slots = hold_response_allowance(send).await?;
    }

    // Hold every execution slot and output byte. A grant makes the next page
    // wait for them.
    let holds: Vec<_> = shared
        .serving
        .iter()
        .chain(&shared.pushing)
        .filter(|_| !allowance_only)
        .map(ServeCapacity::hold_node_for_test)
        .collect();
    let mut sequence = 1;
    if opening.is_some() {
        raw.send(
            subscription.stream,
            &update(UpdateOp::Grant, sequence, one_page)?,
        )
        .await?;
        sequence += 1;
    }
    if opening.is_some() || allowance_only {
        await_until("a page waits for capacity", CONFORMANCE_DEADLINE, || {
            if allowance_only {
                shared.push_send_waits.load(Ordering::Relaxed) > 0
            } else {
                shared.push_waits.load(Ordering::Relaxed) >= 1
            }
        })
        .await?;
    }
    raw.send(
        subscription.stream,
        &update(UpdateOp::Close, sequence, NO_CREDIT)?,
    )
    .await?;
    await_until(
        "the victim ends the subscription",
        CONFORMANCE_DEADLINE,
        || shared.published_ended.load(Ordering::Relaxed) == 1,
    )
    .await?;

    // A request is still admitted, and a sibling still answers.
    let request = plan.requests.first().copied();
    if let Some(request) = request {
        let handled = shared.handled.load(Ordering::Relaxed);
        raw.send(request.stream, &encode::<A>(request.row, 9)?)
            .await?;
        await_until("the victim admits a request", CONFORMANCE_DEADLINE, || {
            shared.handled.load(Ordering::Relaxed) > handled
        })
        .await?;
    }
    victim.siblings[0].probe(&control.id()).await?;

    // The peer reads again: every page in order, then the ending.
    for cursor in 1..=pages {
        let frame = raw.recv(subscription.response_stream).await?;
        let message = decode_frame::<A::Message>(&frame).map_err(|error| error.to_string())?;
        ensure(
            frame.message_type == subscription.page.message_type && A::exchange(&message) == cursor,
            || format!("expected page {cursor}, got row {}", frame.message_type),
        )?;
    }
    let frame = raw.recv(subscription.response_stream).await?;
    ensure(frame.message_type == subscription.end.message_type, || {
        format!("expected the ending, got row {}", frame.message_type)
    })?;

    // Serving resumes once capacity returns.
    drop(holds);
    drop(response_slots);
    if let Some(request) = request {
        let exchange = read_ending::<A>(&mut raw, &request).await?;
        ensure(exchange == 9, || format!("request 9 ended as {exchange}"))?;
    }
    ensure(shared.violations().is_empty(), || {
        format!(
            "the victim faulted a conformant peer: {:?}",
            shared.violations()
        )
    })?;
    ensure(victim.connected(&raw.id()), || {
        "the connection closed".into()
    })?;
    let send_waits = shared.push_send_waits.load(Ordering::Relaxed);
    raw.shutdown().await;
    victim.shutdown().await;
    control.shutdown().await;
    Ok(Some(send_waits))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A one-slot queue cannot prove independent control headroom, and must fail promptly.
    #[tokio::test]
    async fn p9_rejects_a_one_slot_allowance_without_waiting() {
        let (send, _recv) = crate::zakura::framed_channel(1);
        let error =
            tokio::time::timeout(Duration::from_millis(100), hold_response_allowance(&send))
                .await
                .expect("an unsupported queue is rejected before reserving it")
                .unwrap_err();
        assert!(error.to_string().contains("independent control slot"));
        assert_eq!(send.capacity(), 1);
    }
}
