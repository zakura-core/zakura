//! Evidence for the header credit, cursor ring, and grant minimum, and a load
//! test across many sessions on the production serving capacity.
//!
//! Every test runs on a paused clock in one thread. Progress bounds count
//! scheduler passes, so a wait on a timer fails them instead of advancing
//! time.

use std::sync::atomic::{AtomicUsize, Ordering};

use zakura_test::allocations::AllocationStats;

use super::*;
use crate::zakura::{
    header_sync::{
        config::COMMON_HEADER_BYTES,
        v9::{
            self, serving_capacity, HS_SENT_CURSOR_RING, MAX_HS_PUSH_CREDIT_BYTES,
            MAX_HS_PUSH_CREDIT_HEADERS, NODE_EXECUTION, PEER_EXECUTION,
        },
        wire::{DEFAULT_HS_RANGE, MAX_HS_MESSAGE_BYTES},
    },
    regulation::{sizing, WriterFence},
    CloseCause,
};

/// The production window: 4,000 headers and 8 MiB.
const WINDOW: Credit = Credit {
    objects: MAX_HS_PUSH_CREDIT_HEADERS,
    bytes: MAX_HS_PUSH_CREDIT_BYTES,
};

/// The page caps a default node advertises: 1,000 headers and 2 MiB.
fn production_pages() -> PageLimits {
    PageLimits {
        max_headers: DEFAULT_HS_RANGE,
        // The 2 MiB protocol cap fits u32.
        max_bytes: MAX_HS_MESSAGE_BYTES as u32,
        aux_schema_mask: AuxSchema::V1.mask_bit(),
    }
}

/// Header credit binds before byte credit on every network. A page's fixed
/// fields cost the most per header on one-header pages with records, and even
/// then a full header window spends less than the byte window, 6.9 MB of
/// 8 MiB on Mainnet. The cursor ring covers a full window of such pages.
#[test]
fn header_credit_binds_before_byte_credit() {
    for header_bytes in [COMMON_HEADER_BYTES, HB] {
        // Widening usize to u64 is lossless on supported targets.
        let page = smallest_page(header_bytes, AuxSchema::V1) as u64;
        assert!(u64::from(WINDOW.objects) * page <= u64::from(WINDOW.bytes));
    }
    const { assert!(HS_SENT_CURSOR_RING >= WINDOW.objects) };
}

/// A full window of one-header pages fits the cursor ring. The publisher
/// stops at the header credit, and a one-header `Grant` that acknowledges
/// the newest page renews it.
#[tokio::test(start_paused = true)]
async fn a_full_window_of_one_header_pages_fits_the_cursor_ring() {
    let port = FakePort::new(Semaphore::MAX_PERMITS);
    let tip = *port
        .grow(WINDOW.objects + 1)
        .last()
        .expect("the chain grew");
    let one_header = PageLimits {
        max_headers: 1,
        ..limits()
    };
    let mut harness = Harness::advertising(port.clone(), serve_limits(), one_header);
    let id = harness
        .open(tip.hash, vec![port.genesis], WINDOW, AuxSchema::None)
        .await;
    let sent = pages(&harness.deliver_all().await);
    assert_eq!(sent.len(), 4_000);
    assert_eq!(harness.publisher.unacknowledged(id), Some(4_000));
    let grant = harness.subscriber.grant(1, 0).expect("the window has room");
    assert_eq!(harness.apply(grant).await, Ok(Applied::Live));
    let last = pages(&harness.deliver_all().await);
    assert_eq!(last, [(block::Height(4_000), tip.height, true)]);
    assert_eq!(harness.publisher.unacknowledged(id), Some(1));
    harness.check_quiet().await;
}

/// A subscriber that never acknowledges sends the most `Open` and `Grant`
/// updates the window allows, each at the minimum, while page output is
/// blocked. Every update applies, the publisher spends credit only on the
/// page its transport slot holds, and the updates allocate nothing.
#[tokio::test(start_paused = true)]
#[allow(clippy::print_stderr)]
async fn minimum_grant_floods_apply_while_output_is_blocked() {
    let port = FakePort::new(Semaphore::MAX_PERMITS);
    let tip = *port.grow(64).last().expect("the chain grew");
    let capacity = ServeCapacity::new("header_sync_v9", &SUBSCRIBE_HEADERS, serve_limits())
        .expect("the test budgets are valid");
    // A one-frame transport queue that nobody reads.
    let (publisher, _recv, _cancel, _task) = session(&port, &capacity, &peer(7), 1, 1);
    let mut subscriber = Subscriber::new();
    let p = p::<HB>();
    let first = Credit {
        objects: 1,
        bytes: p,
    };
    let open = subscriber.open(tip.hash, vec![port.genesis], first, AuxSchema::None);
    let mut grants = Vec::new();
    while let Some(grant) = subscriber.grant(1, 0).or_else(|| subscriber.grant(0, p)) {
        grants.push(grant);
    }
    // `Open` adds one header and `P` bytes, then each `Grant` adds one or
    // the other until the window is full.
    let updates = u32::try_from(grants.len() + 1).expect("fits u32");
    assert_eq!(updates, WINDOW.objects + WINDOW.bytes / p - 1);
    // The first page takes the only page slot, and the second waits for it.
    assert_eq!(publisher.apply(&open).await, Ok(Applied::Live));
    assert_eq!(publisher.apply(&grants[0]).await, Ok(Applied::Live));
    tokio::time::sleep(QUIET).await;
    assert_eq!(publisher.unacknowledged(open.subscription_id), Some(1));
    let started = std::time::Instant::now();
    let ((), allocated) = zakura_test::allocations::measure(|| {
        for grant in &grants[1..] {
            let applied = lock(&publisher.shared).apply(grant, limits());
            assert_eq!(applied, Ok(Applied::Live));
        }
    });
    let elapsed = started.elapsed();
    tokio::time::sleep(QUIET).await;
    assert_eq!(publisher.unacknowledged(open.subscription_id), Some(1));
    assert_eq!(
        allocated,
        AllocationStats::default(),
        "the updates allocate nothing"
    );
    eprintln!(
        "grant_flood updates={updates} measured={} elapsed={elapsed:?} ns_per_update={} {allocated:?}",
        grants.len() - 1,
        elapsed.as_nanos() / u128::try_from(grants.len() - 1).expect("fits u128"),
    );
}

/// The real subscriber and publisher over a link with a fixed one-way delay
/// and no bandwidth limit, with production pages and a full staging budget.
/// Half-window grants keep one window in flight per round trip, so header
/// credit alone sets the rate: 4,000 headers per round trip, 8,000 per
/// second at the 500 ms target. Validation time is excluded.
#[tokio::test(start_paused = true)]
#[allow(clippy::print_stderr)]
async fn header_credit_sets_throughput_at_the_target_round_trip() {
    const HEADERS: u32 = 5 * MAX_HS_PUSH_CREDIT_HEADERS;
    for rtt in [Duration::from_millis(100), sizing::TARGET_RTT] {
        let port = FakePort::new(Semaphore::MAX_PERMITS);
        let tip = *port.grow(HEADERS).last().expect("the chain grew");
        let caps = production_pages();
        let mut harness = Harness::advertising(port.clone(), serve_limits(), caps);
        let status = v9::Status {
            max_headers_per_response: caps.max_headers,
            max_message_bytes: caps.max_bytes,
            ..v9::test_support::status::<HB>(tip)
        };
        let fence = WriterFence::new(CancellationToken::new(), CloseCause::default());
        let mut subscriber = v9::Subscriber::new(HB, fence, &framed_channel(1).1);
        let open = subscriber
            .open(&status, &[port.genesis], AuxSchema::None, WINDOW.objects)
            .expect("the Open is recorded")
            .0;
        let (start, one_way) = (tokio::time::Instant::now(), rtt / 2);
        let mut updates = VecDeque::from([(start + one_way, open)]);
        let mut frames: VecDeque<(tokio::time::Instant, Frame)> = VecDeque::new();
        let mut received = 0;
        // Five round trips at the credit ceiling; a stall fails at a hundred.
        let deadline = start + rtt * 100;
        while received < HEADERS {
            let now = tokio::time::Instant::now();
            assert!(now < deadline, "the subscription stopped making progress");
            let due = [
                updates.front().map(|(at, _)| *at),
                frames.front().map(|(at, _)| *at),
            ];
            let next = due.into_iter().flatten().min().unwrap_or(deadline);
            tokio::select! {
                biased;
                frame = harness.recv.recv() => {
                    let frame = frame.expect("the session is open");
                    frames.push_back((tokio::time::Instant::now() + one_way, frame));
                }
                () = tokio::time::sleep_until(next) => {
                    while updates.front().is_some_and(|(at, _)| *at <= next) {
                        let (_, update) = updates.pop_front().expect("an update is due");
                        harness.publisher.apply(&update).await.expect("grants apply");
                    }
                    while frames.front().is_some_and(|(at, _)| *at <= next) {
                        let (_, frame) = frames.pop_front().expect("a frame is due");
                        subscriber.admit(&frame).expect("the frame header passes");
                        let Message::Headers(page) = decode_frame::<RegtestMessage>(&frame)
                            .expect("the publisher writes valid frames")
                        else {
                            panic!("the publisher sends only pages here");
                        };
                        let admitted = subscriber
                            .receive_page(&page, frame.payload.len())
                            .expect("the page passes the subscriber");
                        received += u32::try_from(page.entries.len()).expect("fits u32");
                        let grant = subscriber
                            .accept_through(admitted.id, &admitted.cursor, WINDOW.objects)
                            .expect("a received page is accepted");
                        updates.extend(grant.map(|grant| (next + one_way, grant)));
                    }
                }
            }
        }
        let elapsed = tokio::time::Instant::now() - start;
        // Widening u32 to f64 is lossless.
        let rate = f64::from(HEADERS) / elapsed.as_secs_f64();
        let ceiling = f64::from(WINDOW.objects) / rtt.as_secs_f64();
        eprintln!("throughput rtt={rtt:?} elapsed={elapsed:?} headers_per_s={rate:.0}");
        assert!(
            rate <= ceiling && rate >= 0.99 * ceiling,
            "{rate} of {ceiling}"
        );
    }
}

/// What one load-test session does.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Role {
    /// Opens at the tip and receives each header pushed above it.
    Follower,
    /// Opens at genesis and reads full pages to the tip.
    FromEmpty,
    /// Opens at the tip with the minimum credit, then sends 3,999 one-header
    /// grants.
    GrantFlood,
    /// Opens at genesis with a full window and never reads.
    NonReading,
    /// Opens at genesis, reads one page, disconnects, and reconnects.
    Churn,
}

/// One load-test session and its subscriber.
struct Load {
    role: Role,
    peer: ZakuraPeerId,
    publisher: Arc<Publisher>,
    recv: FramedRecv,
    cancel: CancellationToken,
    subscriber: Subscriber,
    /// Headers received on this session.
    headers: u32,
    /// Reconnects left for a churning session.
    reconnects: u32,
}

/// Run `peers` sessions on the production serving capacity, one fifth in
/// each [`Role`], and check aggregate bounds and progress.
///
/// Reads first wait on a closed gate, so running reads must stop at the
/// node and peer execution limits. After the gate opens, every reading
/// session must finish within a bound on scheduler passes, so a wait on a
/// timer fails instead of advancing the paused clock. Cursor history,
/// execution, and output stay within their bounds after every pass. Pages
/// carry Regtest headers, so output bytes stay far below production limits
/// and this checks counts and slots, not byte budgets.
#[allow(clippy::print_stderr)]
async fn run_load(peers: usize) {
    const TIP: u32 = 2 * DEFAULT_HS_RANGE;
    const PUSHED: u32 = 8;
    // Two page slots, fewer than a window of production pages, so
    // non-reading sessions block on output.
    const QUEUE: usize = 3;
    const ROLES: [Role; 5] = [
        Role::Follower,
        Role::FromEmpty,
        Role::GrantFlood,
        Role::NonReading,
        Role::Churn,
    ];
    let port = FakePort::new(0);
    let tip = *port.grow(TIP).last().expect("the chain grew");
    let capacity = serving_capacity();
    let (started, wall) = (tokio::time::Instant::now(), std::time::Instant::now());
    // Session `id` of peer `index`, after its `Open` applies.
    let connect = |index: usize, id: u64| {
        let mut identity = vec![201; 32];
        identity[..8].copy_from_slice(&u64::try_from(index).expect("fits u64").to_le_bytes());
        let peer = ZakuraPeerId::new(identity).expect("a 32-byte peer ID is valid");
        let (publisher, recv, cancel, _task) =
            session_advertising(&port, &capacity, &peer, id, QUEUE, production_pages());
        let role = ROLES[index % ROLES.len()];
        let (locator, credit) = match role {
            Role::Follower => (tip, WINDOW),
            Role::GrantFlood => (
                tip,
                Credit {
                    objects: 1,
                    bytes: p::<HB>(),
                },
            ),
            Role::FromEmpty | Role::NonReading | Role::Churn => (port.genesis, WINDOW),
        };
        let mut subscriber = Subscriber::new();
        let open = subscriber.open(tip.hash, vec![locator], credit, AuxSchema::None);
        let publisher = Arc::new(publisher);
        async move {
            assert_eq!(publisher.apply(&open).await, Ok(Applied::Live));
            Load {
                role,
                peer,
                publisher,
                recv,
                cancel,
                subscriber,
                headers: 0,
                reconnects: 3,
            }
        }
    };
    let mut sessions = Vec::new();
    for index in 0..peers {
        sessions.push(connect(index, 0).await);
    }

    // Sessions with a page to read wait on the closed gate.
    for _ in 0..10_000 {
        tokio::task::yield_now().await;
    }
    assert_eq!(port.store().reads_running, NODE_EXECUTION);
    assert_eq!(capacity.node_execution_held(), NODE_EXECUTION);
    for session in &sessions {
        assert!(capacity.peer_held(&session.peer).0 <= PEER_EXECUTION);
    }

    // Floods run beside the readers. Each grant yields once its budget
    // runs out.
    let flooded = Arc::new(AtomicUsize::new(0));
    let floods = sessions
        .iter()
        .filter(|s| s.role == Role::GrantFlood)
        .count();
    for session in sessions.iter_mut().filter(|s| s.role == Role::GrantFlood) {
        let grants: Vec<_> = std::iter::from_fn(|| session.subscriber.grant(1, 0)).collect();
        assert_eq!(grants.len(), 3_999);
        let (publisher, flooded) = (session.publisher.clone(), flooded.clone());
        tokio::spawn(async move {
            for grant in grants {
                assert_eq!(publisher.apply(&grant).await, Ok(Applied::Live));
            }
            flooded.fetch_add(1, Ordering::Relaxed);
        });
    }
    port.gate.add_permits(Semaphore::MAX_PERMITS);
    port.grow(PUSHED);

    let mut next_id = 1;
    let mut passes = 0;
    for pass in 0.. {
        passes = pass;
        assert!(
            pass < 1_000_000,
            "the sessions did not finish within the bound"
        );
        for (index, session) in sessions.iter_mut().enumerate() {
            if session.role == Role::NonReading {
                continue;
            }
            while let Ok(frame) = session.recv.try_recv() {
                let Received::Page { parent, last, .. } = session.subscriber.receive(&frame) else {
                    panic!("no subscription ends before it closes");
                };
                session.headers += last.height.0 - parent.height.0;
                // A flood's own task sends its grants.
                if session.role != Role::GrantFlood {
                    if let Some(grant) = session.subscriber.grant(u32::MAX, u32::MAX) {
                        assert_eq!(session.publisher.apply(&grant).await, Ok(Applied::Live));
                    }
                }
            }
            if session.role == Role::Churn && session.headers > 0 && session.reconnects > 0 {
                session.cancel.cancel();
                let reconnects = session.reconnects - 1;
                *session = connect(index, next_id).await;
                session.reconnects = reconnects;
                next_id += 1;
            }
        }
        for session in &sessions {
            if let Some(opened) = &session.subscriber.live {
                let held = session.publisher.unacknowledged(opened.id).unwrap_or(0);
                assert!(held <= 4_000, "the cursor ring never fills");
            }
        }
        assert!(capacity.node_execution_held() <= NODE_EXECUTION);
        assert!(capacity.node_output_held() <= sizing::node_output_bytes());
        let finished = sessions.iter().all(|session| match session.role {
            Role::Follower => session.headers == PUSHED,
            Role::FromEmpty => session.headers == TIP + PUSHED,
            Role::Churn => session.reconnects == 0 && session.headers > 0,
            Role::GrantFlood | Role::NonReading => true,
        });
        if finished && flooded.load(Ordering::Relaxed) == floods {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(port.store().most_reads_running, NODE_EXECUTION);
    eprintln!(
        "header_load sessions={peers} passes={passes} wall={:?} rss_kib={:?}",
        wall.elapsed(),
        zakura_test::process_stats::memory_kib(),
    );
    assert_eq!(
        tokio::time::Instant::now(),
        started,
        "no session waited on a timer"
    );

    // A non-reading session queues pages in every slot but the one kept for
    // control, and the next page waits without spending credit.
    for session in sessions.iter_mut().filter(|s| s.role == Role::NonReading) {
        let id = session.subscriber.live.as_ref().expect("still open").id;
        assert_eq!(session.publisher.unacknowledged(id), Some(QUEUE - 1));
        for _ in 1..QUEUE {
            session
                .recv
                .try_recv()
                .expect("each committed page is queued");
        }
        assert!(session.recv.try_recv().is_err());
    }
    for session in &sessions {
        session.cancel.cancel();
    }
    for _ in 0..10_000 {
        tokio::task::yield_now().await;
    }
    assert_eq!(capacity.node_execution_held(), 0);
    assert!(port.store().leases.is_empty());
}

/// Forty sessions, eight in each role, on every pull request.
#[tokio::test(start_paused = true)]
async fn many_sessions_keep_header_serving_bounded() {
    run_load(40).await;
}

/// Every default header-sync session slot, 256 inbound and 256 outbound, in
/// full-coverage CI runs.
#[tokio::test(start_paused = true)]
#[ignore = "broad load run; full-coverage CI passes --run-ignored"]
async fn maximum_sessions_keep_header_serving_bounded() {
    run_load(512).await;
}
