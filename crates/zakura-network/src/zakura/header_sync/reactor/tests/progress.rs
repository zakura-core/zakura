//! Progress under adversaries (hsync-22, hsync-23).
//!
//! One honest peer serves a known chain while k adversaries hold sessions
//! beside it. The reactor runs its own loop on a paused clock and reaches
//! storage through the typed port, as production does. The port prepares
//! pages with the production header rules and publishes each applied target
//! as a new committed snapshot. A run holds when that snapshot reaches the
//! honest tip by the deadline [`progress_deadline`] derives, and is violated
//! otherwise. The oracle reads only the committed snapshot, the clock and
//! each peer's session and connection tokens. It never picks a supplier.

use std::time::Duration;

use zakura_chain::work::difficulty::INVALID_COMPACT_DIFFICULTY;
use zakura_node_services::header_chain as port;

use super::*;
use crate::zakura::{CloseCause, FramedRecv};

/// Headers the honest chain has above genesis.
const HONEST_LEN: u32 = 4;

/// How long the port takes to read a locator. Offers that arrive meanwhile
/// meet a staged target that hasn't started.
const LOCATOR_READ: Duration = Duration::from_millis(1);

/// Why a progress deadline has no figure.
#[derive(Debug, Eq, PartialEq)]
enum MissingBound {
    /// `max_unproductive_header_requests` is zero, so no stall ever ends an
    /// adversary's session.
    NoStallRule,
    /// Adversaries reopen sessions, after their cooldown or under fresh
    /// identities. Only the per-IP handshake rate bounds how many sessions
    /// they open, and the scheduler doesn't prefer the honest offer over a
    /// reopened one, so no figure follows from the header-sync constants.
    Reopening,
}

/// The time by which a node must adopt the honest tip against `adversaries`
/// peers that each hold one session.
///
/// A session holds a target for at most `max_unproductive_header_requests`
/// request timeouts before the stall rule ends it. Only one peer owns a
/// branch target at a time, so in the worst case the adversaries hold the
/// honest branch one after another. The honest round then finishes within
/// one more request timeout. Each turn can wait one cached-pass refill
/// before the next offer stages.
fn progress_deadline(
    startup: &HeaderSyncStartup,
    adversaries: u32,
    reopening: bool,
) -> Result<Duration, MissingBound> {
    if reopening {
        return Err(MissingBound::Reopening);
    }
    let strikes = startup.config.max_unproductive_header_requests;
    if strikes == 0 {
        return Err(MissingBound::NoStallRule);
    }
    let turn = startup.request_timeout * strikes + CACHED_PASS_REFILL;
    Ok(turn * adversaries.saturating_add(1))
}

/// Genesis and the headers above it, each linked to the one before.
struct Chain(Vec<Arc<block::Header>>);

impl Chain {
    /// The honest chain: `len` headers above genesis, each a second after
    /// its parent.
    fn honest(len: u32) -> Self {
        let genesis = regtest_genesis_block().header.clone();
        let mut headers = vec![genesis.clone()];
        for seconds in 1..=i64::from(len) {
            let parent = headers.last().expect("the chain starts at genesis");
            let mut header = *genesis;
            header.previous_block_hash = parent.hash();
            header.time += chrono::Duration::seconds(seconds);
            headers.push(Arc::new(header));
        }
        Self(headers)
    }

    /// Branch `branch` off genesis: the honest chain with an unencodable
    /// compact target (LC-VAL-05) in its first header. The branch index
    /// changes the first header's time, so each branch has its own tip.
    fn invalid(len: u32, branch: u8) -> Self {
        let mut headers = vec![regtest_genesis_block().header.clone()];
        for (index, honest) in Self::honest(len).0.iter().enumerate().skip(1) {
            let parent = headers.last().expect("the chain starts at genesis");
            let mut header = **honest;
            header.previous_block_hash = parent.hash();
            if index == 1 {
                header.difficulty_threshold = INVALID_COMPACT_DIFFICULTY;
                header.time += chrono::Duration::seconds(i64::from(branch));
            }
            headers.push(Arc::new(header));
        }
        Self(headers)
    }

    fn frontier(&self, height: u32) -> zakura_header_chain::Frontier {
        // A test chain has a few headers, so every height fits in `usize`.
        zakura_header_chain::Frontier::new(block::Height(height), self.0[height as usize].hash())
    }

    fn tip(&self) -> zakura_header_chain::Frontier {
        let height = u32::try_from(self.0.len() - 1).expect("a test chain is short");
        self.frontier(height)
    }

    fn height_of(&self, hash: block::Hash) -> Option<u32> {
        self.0
            .iter()
            .position(|header| header.hash() == hash)
            .and_then(|index| u32::try_from(index).ok())
    }

    /// The page from the header after `cursor` through at most `objects`
    /// headers, toward `tip`.
    fn page(&self, subscription_id: u64, cursor: u32, objects: u32) -> Headers {
        let tip = self.tip();
        let last = cursor.saturating_add(objects).min(tip.height.0);
        Headers {
            subscription_id,
            initial_target_tip_hash: tip.hash,
            common_ancestor_height: block::Height(cursor),
            common_ancestor_hash: self.frontier(cursor).hash,
            reaches_initial_target: last == tip.height.0,
            tree_aux_schema: AuxSchema::None,
            // Heights in a test chain fit in `usize`.
            entries: self.0[cursor as usize + 1..=last as usize]
                .iter()
                .map(|header| HeaderEntry {
                    header: header.clone(),
                    body_size: 0,
                    tree_aux: None,
                })
                .collect(),
        }
    }
}

/// Committed state behind the typed port: production header rules over the
/// honest chain, and one committed snapshot per applied target.
struct ChainPort {
    honest: Arc<Chain>,
    network: Network,
    key: port::AdapterKey,
    snapshots: watch::Sender<Option<zakura_header_chain::EngineSnapshot>>,
}

impl ChainPort {
    fn committed(&self) -> zakura_header_chain::EngineSnapshot {
        self.snapshots
            .borrow()
            .clone()
            .expect("the port always holds a snapshot")
    }
}

impl port::Port for ChainPort {
    fn continuation_locator(
        &self,
        _permit: port::LocatorReadPermit,
    ) -> port::HeaderChainFuture<
        '_,
        Result<Option<zakura_header_chain::HeaderLocator>, port::PortError>,
    > {
        let best = self.committed().frontiers.header_best;
        Box::pin(async move {
            time::sleep(LOCATOR_READ).await;
            Ok(Some(zakura_header_chain::HeaderLocator::for_continuation(
                best,
            )))
        })
    }

    fn vct_repair_context(
        &self,
        _owner: zakura_header_chain::BodyWorkOwner,
        _height: block::Height,
    ) -> port::HeaderChainFuture<'_, Result<port::VctRepairContextReply, port::PortError>> {
        Box::pin(async { Ok(port::VctRepairContextReply::Stale) })
    }

    fn acquire_header_path(
        &self,
        _request: port::AcquirePath,
    ) -> port::HeaderChainFuture<'_, Result<port::AcquirePathReply, port::PortError>> {
        Box::pin(async { Ok(port::AcquirePathReply::TargetNotRetained) })
    }

    fn read_header_path(
        &self,
        _path: port::RetainedHeaderPath,
        _request: port::ReadPath,
    ) -> port::HeaderChainFuture<'_, Result<port::ReadPathReply, port::PortError>> {
        Box::pin(async { Ok(port::ReadPathReply::Unavailable) })
    }

    fn release_header_path(
        &self,
        _path: port::RetainedHeaderPath,
    ) -> port::HeaderChainFuture<'_, Result<(), port::PortError>> {
        Box::pin(async { Ok(()) })
    }

    fn prepare_header_target(
        &self,
        request: port::PrepareHeaderTarget,
    ) -> port::HeaderChainFuture<'_, port::PrepareHeaderTargetReply> {
        let ancestor = request.common_ancestor;
        let parent = self
            .honest
            .height_of(ancestor.hash)
            .map(|height| self.honest.0[height as usize].clone())
            .expect("every locator names a committed honest header");
        let headers: Vec<_> = request
            .entries
            .as_slice()
            .iter()
            .map(|entry| entry.header.clone())
            .collect();
        let lease = zakura_header_chain::ValidationLease::new(
            ancestor,
            vec![zakura_header_chain::HeaderContextFact {
                frontier: ancestor,
                header: parent,
            }],
            self.network.clone(),
            [9; 32],
        );
        let rules = zakura_header_chain::HeaderRules::for_validation_lease(&lease)
            .expect("the Regtest policy is valid");
        let reply = match zakura_header_chain::prepare_headers(
            zakura_header_chain::HeaderBatchInput::new(&headers),
            lease.parent(),
            &rules,
            &zakura_header_chain::SystemClock,
        ) {
            Ok(batch) => Ok(port::PreparedHeaderTarget::from_insert(
                &self.key,
                Box::new(zakura_header_chain::InsertHeaders {
                    owner: request.owner,
                    source: request.source,
                    parent_hash: ancestor.hash,
                    target_tip_hash: request.target.hash,
                    completion: request.completion,
                    batch,
                    aux: Vec::new(),
                }),
            )),
            Err(_) => Err(invalid_header_failure(request.source, request.owner)),
        };
        Box::pin(async move { reply })
    }

    fn apply_header_target(
        &self,
        target: port::PreparedHeaderTarget,
    ) -> port::HeaderChainFuture<'_, port::ApplyHeaderTargetReply> {
        let tip = target.target_tip_hash();
        let owner = target.owner();
        let reply = match self.honest.height_of(tip) {
            Some(height) => {
                let mut snapshot = self.committed();
                snapshot.state_version =
                    zakura_header_chain::StateVersion::new(snapshot.state_version.get() + 1);
                snapshot.header_generation = zakura_header_chain::HeaderGeneration::new(
                    snapshot.header_generation.get() + 1,
                );
                snapshot.frontiers.header_best = self.honest.frontier(height);
                snapshot.header_best_score = zakura_header_chain::ChainScore::new(
                    zakura_header_chain::SuffixWork::new(U256::from(height)),
                    tip,
                );
                self.snapshots.send_replace(Some(snapshot));
                Ok(port::ApplyHeaderTargetOutcome::Applied)
            }
            None => Err(local_failure(owner)),
        };
        Box::pin(async move { reply })
    }

    fn selected_chain_contains(
        &self,
        _hash: block::Hash,
    ) -> port::HeaderChainFuture<'_, Result<port::SelectedChainMembership, port::PortError>> {
        Box::pin(async { Ok(port::SelectedChainMembership::NotRetained) })
    }
}

type U256 = zakura_chain::work::difficulty::U256;

/// What one peer does.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Role {
    /// Advertises the honest tip and serves the honest chain.
    Honest,
    /// Advertises the honest tip and never answers a subscription.
    Silent,
    /// Advertises tip `n` of its own, with more work than the honest tip, and
    /// never answers a subscription.
    FalseTip(u8),
    /// Advertises the honest tip and sends one valid header just before each
    /// request deadline, never reaching the target.
    Intermittent,
    /// Advertises tip `n` of an invalid branch with more work than the honest
    /// tip, and serves that branch.
    Invalid(u8),
}

impl Role {
    fn peer(self, index: usize) -> ZakuraPeerId {
        // Adversaries sort before the honest peer.
        let byte = match self {
            Role::Honest => 0xF0,
            _ => 0x10 + u8::try_from(index).expect("a test runs few peers"),
        };
        ZakuraPeerId::new(vec![byte; 32]).expect("the test peer ID has the required length")
    }

    /// The chain this peer serves, if it serves one.
    fn chain(self, honest: &Arc<Chain>) -> Option<Arc<Chain>> {
        match self {
            Role::Honest | Role::Intermittent => Some(honest.clone()),
            Role::Invalid(branch) => Some(Arc::new(Chain::invalid(HONEST_LEN, branch))),
            Role::Silent | Role::FalseTip(_) => None,
        }
    }

    fn status(self, honest: &Chain) -> Status {
        let honest_tip = honest.tip();
        let (tip, work) = match self {
            Role::Honest | Role::Silent | Role::Intermittent => {
                (honest_tip, U256::from(HONEST_LEN))
            }
            Role::FalseTip(n) => (
                zakura_header_chain::Frontier::new(
                    block::Height(HONEST_LEN + 1),
                    block::Hash([n; 32]),
                ),
                U256::from(1_000 + u32::from(n)),
            ),
            Role::Invalid(n) => (
                Chain::invalid(HONEST_LEN, n).tip(),
                U256::from(2_000 + u32::from(n)),
            ),
        };
        Status {
            work_anchor_height: block::Height(0),
            work_anchor_hash: honest.frontier(0).hash,
            selected_tip_height: tip.height,
            selected_tip_hash: tip.hash,
            suffix_cumulative_work: work,
            oldest_retained_height: block::Height(0),
            max_headers_per_response: HONEST_LEN,
            max_subscriptions: 1,
            max_message_bytes: 2_000_000,
            tree_aux_schema_mask: 0,
        }
    }
}

/// Plays `role` on `session` until the session ends.
async fn play(
    role: Role,
    session: PeerSession,
    mut outbound: FramedRecv,
    handle: HeaderSyncHandle,
    chain: Option<Arc<Chain>>,
    request_timeout: Duration,
) {
    let cancelled = session.cancel_token();
    loop {
        let frame = tokio::select! {
            _ = cancelled.cancelled() => return,
            frame = outbound.recv() => frame,
        };
        let Some(frame) = frame else { return };
        let Ok(v9::Message::SubscribeHeaders(update)) = decode_frame::<v9::RegtestMessage>(&frame)
        else {
            continue;
        };
        let Some(chain) = chain.as_ref() else {
            continue;
        };
        let event = match update.operation {
            v9::Operation::Close => outcome_event(
                &session,
                HeadersOutcome {
                    subscription_id: update.subscription_id,
                    initial_target_tip_hash: update.initial_target_tip_hash,
                    outcome: HeadersOutcomeCode::SubscriptionClosed,
                },
            ),
            v9::Operation::Open | v9::Operation::Grant => {
                let cursor = chain
                    .height_of(update.acknowledged.hash)
                    .expect("the node acknowledges a header of the served chain");
                let objects = if role == Role::Intermittent {
                    // Keep one header back so the page never reaches the target.
                    if cursor + 1 >= chain.tip().height.0 {
                        continue;
                    }
                    time::sleep(request_timeout.saturating_sub(Duration::from_secs(1))).await;
                    1
                } else {
                    update.added_credit.objects
                };
                if objects == 0 {
                    continue;
                }
                page_event(
                    &session,
                    chain.page(update.subscription_id, cursor, objects),
                )
            }
        };
        if handle.send(event).await.is_err() {
            return;
        }
    }
}

/// How one run ended.
#[derive(Debug, Eq, PartialEq)]
enum Verdict {
    /// The node adopted the honest tip after this long.
    Held(Duration),
    /// The node hadn't adopted the honest tip by the deadline.
    Violated,
}

/// Runs `roles` (exactly one honest) against one node until it adopts the
/// honest tip or `deadline` passes. Peers connect and advertise in order.
async fn run(roles: &[Role]) -> Run {
    assert_eq!(
        roles.iter().filter(|role| **role == Role::Honest).count(),
        1,
        "a run has exactly one honest peer"
    );
    let shutdown = CancellationToken::new();
    let mut startup = startup(shutdown.clone());
    let adversaries = u32::try_from(roles.len() - 1).expect("a test runs few peers");
    // Each turn also waits for the port's own locator read.
    let deadline = progress_deadline(&startup, adversaries, false)
        .expect("one-session adversaries have a bounded deadline")
        + LOCATOR_READ * adversaries.saturating_add(1);
    let honest = Arc::new(Chain::honest(HONEST_LEN));
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let (snapshots, snapshots_rx) = watch::channel(Some(committed_snapshot(anchor)));
    let mut adopted = snapshots_rx.clone();
    startup.committed_snapshots = Some(snapshots_rx);
    startup.header_chain_port = Arc::new(ChainPort {
        honest: honest.clone(),
        network: startup.network.clone(),
        key: port::AdapterKey::new(),
        snapshots,
    });
    startup.use_direct_port();
    let request_timeout = startup.request_timeout;
    let (handle, mut actions, task) =
        spawn_header_sync_reactor(startup).expect("the progress fixture starts");
    // The direct port takes every storage call. Nothing else reads the action channel.
    tokio::spawn(async move { while actions.recv().await.is_some() {} });

    let start = Instant::now();
    let mut honest_tokens = None;
    let mut adversaries = Vec::new();
    for (index, role) in roles.iter().copied().enumerate() {
        let peer = role.peer(index);
        let (send, outbound) = framed_channel(8);
        // As in production, the session's token is a child of its connection's.
        let connection = CancellationToken::new();
        let session = PeerSession::from_parts_with_connection(
            peer.clone(),
            1,
            send,
            connection.child_token(),
            connection.clone(),
            CloseCause::new(),
        );
        if role == Role::Honest {
            honest_tokens = Some((session.cancel_token(), connection));
        } else {
            adversaries.push((role, session.cancel_token()));
        }
        handle
            .send(Event::PeerConnected(session.clone()))
            .await
            .expect("the reactor runs");
        handle
            .send(Event::Status {
                peer,
                session_id: 1,
                status: role.status(&honest),
            })
            .await
            .expect("the reactor runs");
        tokio::spawn(play(
            role,
            session,
            outbound,
            handle.clone(),
            role.chain(&honest),
            request_timeout,
        ));
    }

    let tip = honest.tip();
    let verdict = match tokio::time::timeout_at(
        start + deadline,
        adopted.wait_for(|snapshot| {
            snapshot
                .as_ref()
                .is_some_and(|snapshot| snapshot.frontiers.header_best == tip)
        }),
    )
    .await
    {
        Ok(_) => Verdict::Held(start.elapsed()),
        Err(_) => Verdict::Violated,
    };

    let (session, connection) = honest_tokens.expect("a run has an honest peer");
    assert!(!session.is_cancelled(), "the honest session stays open");
    assert!(
        !connection.is_cancelled(),
        "the honest connection stays open"
    );
    let ended = adversaries
        .into_iter()
        .map(|(role, session)| (role, session.is_cancelled()))
        .collect();
    shutdown.cancel();
    task.await.expect("the reactor exits cleanly");
    Run { verdict, ended }
}

/// What one run produced.
#[derive(Debug)]
struct Run {
    verdict: Verdict,
    /// Each adversary, and whether the node had ended its session by the
    /// verdict.
    ended: Vec<(Role, bool)>,
}

impl Run {
    /// Asserts that the run held, and returns how long it took.
    fn held(self) -> Duration {
        let Verdict::Held(elapsed) = self.verdict else {
            panic!("the node missed the honest tip's deadline: {self:?}");
        };
        elapsed
    }

    /// Asserts that the run held and that the node ended every adversary's
    /// session on the way, so each adversary held work. Returns how long
    /// the run took.
    fn held_past_every_adversary(self) -> Duration {
        let Verdict::Held(elapsed) = self.verdict else {
            panic!("the node missed the honest tip's deadline: {self:?}");
        };
        assert!(
            self.ended.iter().all(|(_, ended)| *ended),
            "every adversary held work and lost its session: {self:?}"
        );
        elapsed
    }
}

#[test]
fn the_deadline_follows_the_request_timeout_and_stall_rule() {
    let mut startup = startup(CancellationToken::new());
    let turn = startup.request_timeout + CACHED_PASS_REFILL;
    assert_eq!(progress_deadline(&startup, 0, false), Ok(turn));
    assert_eq!(progress_deadline(&startup, 16, false), Ok(turn * 17));
    assert_eq!(
        progress_deadline(&startup, 1, true),
        Err(MissingBound::Reopening)
    );
    startup.config.max_unproductive_header_requests = 0;
    assert_eq!(
        progress_deadline(&startup, 1, false),
        Err(MissingBound::NoStallRule)
    );
}

/// hsync-22: alone, the honest peer's tip is adopted within one turn.
#[tokio::test(start_paused = true)]
async fn an_honest_peer_alone_is_adopted_by_the_deadline() {
    let roles = [Role::Honest];
    run(&roles).await.held_past_every_adversary();
}

/// hsync-23: silent servers of the honest tip each hold its branch for at
/// most one request timeout.
#[tokio::test(start_paused = true)]
async fn silent_servers_cannot_hold_the_honest_tip_past_the_deadline() {
    let roles = [Role::Silent, Role::Silent, Role::Silent, Role::Honest];
    run(&roles).await.held_past_every_adversary();
}

/// hsync-23: 16 false tips with more work than the honest tip take every
/// staged slot, then stall out together.
#[tokio::test(start_paused = true)]
async fn sixteen_false_tips_cannot_hold_the_honest_tip_past_the_deadline() {
    let mut roles: Vec<_> = (1..=16).map(Role::FalseTip).collect();
    roles.push(Role::Honest);
    run(&roles).await.held_past_every_adversary();
}

/// hsync-23, #1517: 16 false tips with more work arrive while the honest
/// offer waits for its locator read. They don't strand it.
#[tokio::test(start_paused = true)]
async fn sixteen_later_false_tips_cannot_strand_the_honest_offer() {
    let mut roles = vec![Role::Honest];
    roles.extend((1..=16).map(Role::FalseTip));
    run(&roles).await.held();
}

/// hsync-23: a peer that drips one valid header per request timeout stalls,
/// because progress counts only once a page is prepared.
#[tokio::test(start_paused = true)]
async fn intermittent_pages_cannot_hold_the_honest_tip_past_the_deadline() {
    let roles = [Role::Intermittent, Role::Intermittent, Role::Honest];
    run(&roles).await.held_past_every_adversary();
}

/// hsync-23: 16 branches that fail header validation take every staged slot
/// first. Each failure ends its session and frees its slot for the honest
/// peer.
#[tokio::test(start_paused = true)]
async fn invalid_branches_cannot_hold_the_honest_tip_past_the_deadline() {
    let mut roles: Vec<_> = (1..=16).map(Role::Invalid).collect();
    roles.push(Role::Honest);
    run(&roles).await.held_past_every_adversary();
}

/// hsync-23: every adversary class at once.
#[tokio::test(start_paused = true)]
async fn mixed_adversaries_cannot_hold_the_honest_tip_past_the_deadline() {
    let mut roles: Vec<_> = (1..=16).map(Role::FalseTip).collect();
    roles.extend([
        Role::Silent,
        Role::Intermittent,
        Role::Invalid(1),
        Role::Honest,
    ]);
    run(&roles).await.held_past_every_adversary();
}
