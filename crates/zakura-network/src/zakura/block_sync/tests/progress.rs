//! Progress under adversaries (bsync-20, bsync-21).
//!
//! One honest peer serves every body while k adversaries hold sessions beside
//! it. The reactor and its peer routines run on a paused clock, and each peer
//! speaks through the service's streams. The test plays state: it answers each
//! needed-block query, commits each submitted body, and grows the verified tip.
//! A run holds when every body commits by the deadline [`progress_deadline`]
//! derives, and is violated otherwise. The oracle reads only the reactor's
//! actions, the clock and each peer's streams. It never picks a supplier.

use std::sync::Mutex as StdMutex;

use super::*;

/// Bodies the honest chain has above genesis: more than one session's
/// no-progress request cap, so a liar at that cap can't hold them all.
const BLOCKS: u32 = DEFAULT_BS_MAX_REQUESTS_WITHOUT_BLOCK_PROGRESS + 16;

/// Why a progress deadline has no figure.
#[derive(Debug, Eq, PartialEq)]
enum MissingBound {
    /// The deadline outlasts the park cooldown, so a parked adversary can
    /// return within it. Only the per-IP handshake rate bounds how often it
    /// returns, so no figure follows from the block-sync constants.
    ParkEnds,
}

/// The time by which every body must commit against `adversaries` peers that
/// each hold one session, when the honest peer must move `bytes`.
///
/// A session that delivers no body is parked by its liveness deadline: the
/// allowance, its next owed body at the minimum rate, and at most one
/// local-write grace of `request_timeout`. An adversary owes at most every
/// needed body, so its deadline is at most `liveness(bytes)`. In the worst
/// case the adversaries hold work one after another. The honest peer then
/// moves every body at the minimum rate within one more deadline. The figure
/// holds only while no parked adversary returns, so it must end before the
/// park cooldown does.
fn progress_deadline(
    config: &ZakuraBlockSyncConfig,
    adversaries: u32,
    bytes: u64,
) -> Result<Duration, MissingBound> {
    let liveness = config.effective_liveness_timeout()
        + admission::min_rate_transfer_time(bytes)
        + config.request_timeout;
    let deadline = liveness * adversaries.saturating_add(1);
    if deadline > config.effective_no_progress_peer_cooldown() {
        return Err(MissingBound::ParkEnds);
    }
    Ok(deadline)
}

/// What one peer does.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Role {
    /// Serves every requested body and ends each request.
    Honest,
    /// Advertises every body and never answers.
    Silent,
    /// Advertises every body and answers `RangeUnavailable` to each request.
    Unavailable,
    /// Serves its requests' bodies but never sends their endings.
    Withholder,
    /// Serves its first request to prove itself, then never answers again,
    /// with the protocol's most in-flight requests.
    Liar,
}

impl Role {
    fn peer(self, index: usize) -> u8 {
        // Adversaries sort before the honest peer.
        match self {
            Role::Honest => 0xF0,
            _ => 0x10 + u8::try_from(index).expect("a test runs few peers"),
        }
    }

    fn status(self, tip: block::Hash) -> BlockSyncStatus {
        BlockSyncStatus {
            servable_low: block::Height(1),
            servable_high: block::Height(BLOCKS),
            tip_hash: tip,
            max_blocks_per_response: 1,
            max_inflight_requests: match self {
                Role::Liar => MAX_BS_INFLIGHT_REQUESTS,
                _ => 4,
            },
            max_response_bytes: MAX_BS_RESPONSE_BYTES,
        }
    }
}

/// What one peer saw.
#[derive(Debug, Default)]
struct Seen {
    /// Requests received since the peer last served a body.
    unserved: u32,
    /// The most requests in a row the peer left without a body.
    most_unserved: u32,
    /// When the node ended the peer's session, if it did.
    ended_at: Option<Duration>,
}

/// Plays `role` until the node ends the session.
async fn play(
    role: Role,
    blocks: Arc<Vec<Arc<block::Block>>>,
    inbound: FramedSend,
    mut outbound: FramedRecv,
    start: Instant,
    seen: Arc<StdMutex<Seen>>,
) {
    let mut proven = false;
    while let Some(frame) = outbound.recv().await {
        let Ok(BlockSyncMessage::GetBlocks {
            start_height,
            count,
        }) = BlockSyncMessage::decode_frame(frame)
        else {
            continue;
        };
        let serve = match role {
            Role::Honest | Role::Withholder => true,
            Role::Liar => !std::mem::replace(&mut proven, true),
            Role::Silent | Role::Unavailable => false,
        };
        {
            let mut seen = seen.lock().expect("unpoisoned");
            seen.unserved = if serve { 0 } else { seen.unserved + 1 };
            seen.most_unserved = seen.most_unserved.max(seen.unserved);
        }
        let mut replies = Vec::new();
        if serve {
            for height in start_height.0..start_height.0.saturating_add(count) {
                // Heights start at 1 and stay within the test chain.
                replies.push(BlockSyncMessage::Block(blocks[height as usize - 1].clone()));
            }
        }
        match role {
            Role::Honest | Role::Liar if serve => replies.push(BlockSyncMessage::BlocksDone {
                start_height,
                returned: count,
            }),
            Role::Unavailable => replies.push(BlockSyncMessage::RangeUnavailable {
                start_height,
                count,
            }),
            _ => {}
        }
        for reply in replies {
            let frame = reply.encode_frame().expect("a test reply encodes");
            if inbound.send(frame).await.is_err() {
                break;
            }
        }
    }
    seen.lock().expect("unpoisoned").ended_at = Some(start.elapsed());
}

/// How one run ended.
#[derive(Debug)]
struct Run {
    /// When the last body committed, or `None` if it hadn't by the deadline.
    committed_at: Option<Duration>,
    /// What each adversary saw.
    adversaries: Vec<(Role, Arc<StdMutex<Seen>>)>,
}

impl Run {
    /// Asserts that every body committed by the deadline and that, by the
    /// deadline, the node ended every adversary's session. Returns how long
    /// the commits took.
    fn held_past_every_adversary(&self) -> Duration {
        let Some(elapsed) = self.committed_at else {
            panic!("the node missed the honest tip's deadline: {self:?}");
        };
        for (_, seen) in &self.adversaries {
            assert!(
                seen.lock().expect("unpoisoned").ended_at.is_some(),
                "the node ends every adversary's session by the deadline: {self:?}"
            );
        }
        elapsed
    }
}

/// Runs `roles` (exactly one honest) against one node until every body
/// commits or the deadline passes. Peers connect in order.
async fn run(roles: &[Role]) -> Run {
    assert_eq!(
        roles.iter().filter(|role| **role == Role::Honest).count(),
        1,
        "a run has exactly one honest peer"
    );
    let config = immediate_body_download_config();
    let blocks = Arc::new(fake_sequential_blocks(BLOCKS));
    let tip = blocks.last().expect("the chain has bodies").hash();
    let metas: Vec<_> = blocks.iter().map(block_meta).collect();
    let bytes = blocks
        .iter()
        .map(|block| u64::from(block_size(block)))
        .sum();
    let adversaries = u32::try_from(roles.len() - 1).expect("a test runs few peers");
    let deadline = progress_deadline(&config, adversaries, bytes)
        .expect("a few one-session adversaries finish within the park cooldown");

    let (_tip_tx, tip_rx) = watch::channel((block::Height(BLOCKS), tip));
    let startup = header_tip_startup(
        genesis_frontiers(),
        (block::Height(BLOCKS), tip),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, reactor_task) =
        spawn_block_sync_reactor(startup).expect("valid block-sync config");
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());

    let start = Instant::now();
    let mut adversary_seen = Vec::new();
    let mut honest_connection = None;
    for (index, role) in roles.iter().copied().enumerate() {
        let peer = peer(role.peer(index));
        let (inbound_tx, inbound_rx) = framed_channel(16);
        let (outbound_tx, outbound_rx) = framed_channel(16);
        let streams = HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (inbound_rx, outbound_tx))]);
        let connection = CancellationToken::new();
        service.add_peer(Peer::new_with_direction(
            peer,
            None,
            ZAKURA_CAP_BLOCK_SYNC_REGULATED,
            ServicePeerDirection::Outbound,
            streams,
            connection.clone(),
        ));
        inbound_tx
            .send(
                BlockSyncMessage::Status(role.status(tip))
                    .encode_frame()
                    .expect("status encodes"),
            )
            .await
            .expect("the peer's stream is open");
        let seen = Arc::new(StdMutex::new(Seen::default()));
        if role == Role::Honest {
            honest_connection = Some((connection, seen.clone()));
        } else {
            adversary_seen.push((role, seen.clone()));
        }
        tokio::spawn(play(
            role,
            blocks.clone(),
            inbound_tx,
            outbound_rx,
            start,
            seen,
        ));
    }

    handle
        .send(BlockSyncEvent::NeededBlocks(metas.clone()))
        .await
        .expect("the reactor runs");
    // State keeps answering after the last commit, so stalled sessions reach
    // their park by the deadline.
    let (committed_tx, committed_rx) = tokio::sync::oneshot::channel();
    let state_handle = handle.clone();
    let state = tokio::spawn(async move {
        let handle = state_handle;
        let mut committed_tx = Some(committed_tx);
        let mut verified = 0;
        while let Some(action) = actions.recv().await {
            match action {
                BlockSyncAction::QueryNeededBlocks { .. } => {
                    // Heights start at 1, so height `h` is at index `h - 1`.
                    // The verified tip never passes the chain's last height.
                    let needed = metas[verified as usize..].to_vec();
                    handle
                        .send(BlockSyncEvent::NeededBlocks(needed))
                        .await
                        .expect("the reactor runs");
                }
                BlockSyncAction::SubmitBlock {
                    owner,
                    source,
                    token,
                    block,
                } => {
                    let height = block.coinbase_height().expect("a test body has a height");
                    handle
                        .send(BlockSyncEvent::BlockApplyFinished {
                            owner,
                            source,
                            token,
                            height,
                            hash: block.hash(),
                            outcome: test_block_apply_outcome(BlockApplyResult::Committed),
                        })
                        .await
                        .expect("the reactor runs");
                    if height.0 == verified + 1 {
                        verified = height.0;
                        if verified == BLOCKS {
                            if let Some(committed) = committed_tx.take() {
                                let _ = committed.send(start.elapsed());
                            }
                        }
                        handle
                            .send(BlockSyncEvent::ChainTipGrow(BlockSyncFrontiers {
                                finalized_height: block::Height(0),
                                verified_block_tip: height,
                                verified_block_hash: block.hash(),
                            }))
                            .await
                            .expect("the reactor runs");
                    }
                }
                _ => {}
            }
        }
    });
    let committed = time::timeout_at(start + deadline, committed_rx)
        .await
        .ok()
        .map(|committed| committed.expect("state runs until the reactor stops"));
    time::sleep_until(start + deadline).await;

    let (connection, seen) = honest_connection.expect("a run has an honest peer");
    assert!(
        !connection.is_cancelled(),
        "the honest connection stays open"
    );
    assert!(
        seen.lock().expect("unpoisoned").ended_at.is_none(),
        "the honest session stays open"
    );
    state.abort();
    reactor_task.abort();
    Run {
        committed_at: committed,
        adversaries: adversary_seen,
    }
}

#[test]
fn the_deadline_follows_the_liveness_rule_and_park_cooldown() {
    let config = ZakuraBlockSyncConfig::default();
    let bytes = 2_000_000;
    let liveness = config.effective_liveness_timeout()
        + admission::min_rate_transfer_time(bytes)
        + config.request_timeout;
    assert_eq!(progress_deadline(&config, 0, bytes), Ok(liveness));
    assert_eq!(progress_deadline(&config, 2, bytes), Ok(liveness * 3));
    // Three adversaries in a row outlast the 180 s park, so the first can
    // return before the honest peer's turn.
    assert_eq!(
        progress_deadline(&config, 3, bytes),
        Err(MissingBound::ParkEnds)
    );
}

/// bsync-20: alone, the honest peer commits every body within one deadline.
#[tokio::test(start_paused = true)]
async fn an_honest_peer_alone_commits_by_the_deadline() {
    run(&[Role::Honest]).await.held_past_every_adversary();
}

/// bsync-20: silent peers are parked, and their heights return to the
/// honest peer.
#[tokio::test(start_paused = true)]
async fn silent_peers_cannot_hold_bodies_past_the_deadline() {
    run(&[Role::Silent, Role::Silent, Role::Honest])
        .await
        .held_past_every_adversary();
}

/// bsync-20: peers that answer every request `RangeUnavailable` can't hold
/// bodies.
#[tokio::test(start_paused = true)]
async fn unavailable_ranges_cannot_hold_bodies_past_the_deadline() {
    run(&[Role::Unavailable, Role::Unavailable, Role::Honest])
        .await
        .held_past_every_adversary();
}

/// bsync-20, #1438: a peer that serves bodies but withholds every ending
/// can't hold the rest of the chain.
#[tokio::test(start_paused = true)]
async fn withheld_endings_cannot_hold_bodies_past_the_deadline() {
    run(&[Role::Withholder, Role::Honest])
        .await
        .held_past_every_adversary();
}

/// bsync-21: a proven liar at the protocol's request cap holds at most its
/// no-progress cap of requests, is parked by its liveness deadline, and
/// strands none of its heights.
#[tokio::test(start_paused = true)]
async fn a_proven_liar_at_the_request_cap_is_bounded_and_parked() {
    let run = run(&[Role::Liar, Role::Honest]).await;
    run.held_past_every_adversary();
    let config = ZakuraBlockSyncConfig::default();
    let seen = run.adversaries[0].1.lock().expect("unpoisoned");
    let cap = config
        .max_requests_without_block_progress
        .min(MAX_BS_INFLIGHT_REQUESTS);
    assert!(
        seen.most_unserved <= cap,
        "the liar held {} requests, more than {cap}",
        seen.most_unserved
    );
    let parked = seen.ended_at.expect("the liar is parked");
    let bytes = BLOCKS * block_size(&fake_sequential_blocks(1)[0]);
    assert!(
        parked
            <= config.effective_liveness_timeout()
                + admission::min_rate_transfer_time(u64::from(bytes))
                + config.request_timeout,
        "the liar was parked after {parked:?}"
    );
}

/// bsync-20: every adversary class at once.
#[tokio::test(start_paused = true)]
async fn mixed_adversaries_cannot_hold_bodies_past_the_deadline() {
    run(&[
        Role::Silent,
        Role::Unavailable,
        Role::Withholder,
        Role::Honest,
    ])
    .await
    .held_past_every_adversary();
}
