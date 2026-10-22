//! Machine test for the floor watchdog's settlement against the shared owners.
//!
//! The proptest runs [`cancel_expired_floor_claims`] on a real registry, work
//! queue, and byte budget while sessions take work, get replaced, and tear
//! down. An independent model tracks each height's holder, each peer's
//! published claim, and the reserved bytes. It never calls the code it checks.

use std::{collections::BTreeMap, time::Duration};

use proptest::prelude::*;

use super::*;
use crate::zakura::{
    block_sync::{test_work_scope, work_queue::RequestWrite},
    transport::FrameWriteClaim,
};

/// Heights the test schedules. Height 1 is the floor watchdog's height.
const HEIGHTS: [u32; 2] = [1, 2];
const BYTES: u64 = 100;
const PEERS: u8 = 2;

fn test_peer(index: u8) -> ZakuraPeerId {
    ZakuraPeerId::new(vec![index + 1; 32]).expect("the test peer ID has the required length")
}

#[derive(Clone, Debug)]
enum Input {
    /// Admit a new session for the peer, replacing its registry entry.
    Admit(u8),
    /// The peer's current session takes `height` and publishes a written
    /// request with a deadline `deadline_ms` from the start.
    Take {
        peer: u8,
        height: u32,
        deadline_ms: u64,
    },
    /// The session holding `height` tears down if a replacement superseded it.
    TeardownReplaced { height: u32 },
    /// Run the watchdog at `now_ms`.
    Watchdog { now_ms: u64 },
}

fn input() -> impl Strategy<Value = Input> {
    prop_oneof![
        2 => (0..PEERS).prop_map(Input::Admit),
        4 => (0..PEERS, prop::sample::select(HEIGHTS.to_vec()), 0..100_u64)
            .prop_map(|(peer, height, deadline_ms)| Input::Take { peer, height, deadline_ms }),
        2 => prop::sample::select(HEIGHTS.to_vec())
            .prop_map(|height| Input::TeardownReplaced { height }),
        3 => (0..120_u64).prop_map(|now_ms| Input::Watchdog { now_ms }),
    ]
}

/// The session that holds one height.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct Holder {
    peer: u8,
    generation: u64,
    owner: zakura_header_chain::BodyWorkOwner,
}

#[derive(Debug, Default)]
struct Model {
    /// Current registry generation of each admitted peer.
    generations: BTreeMap<u8, u64>,
    holders: BTreeMap<u32, Holder>,
    /// Published claims in each peer's current registry entry: height to deadline.
    claims: BTreeMap<(u8, u32), u64>,
    reserved: u64,
}

impl Model {
    fn admit(&mut self, peer: u8, generation: u64) {
        self.generations.insert(peer, generation);
        // The replacement entry starts with no published claims.
        self.claims.retain(|(claimant, _), _| *claimant != peer);
    }

    /// Claims the watchdog cancels at `now_ms`.
    fn watchdog(&mut self, now_ms: u64) -> Vec<(u8, u32)> {
        let floor = HEIGHTS[0];
        let expired: Vec<_> = self
            .claims
            .iter()
            .filter(|((_, height), deadline)| *height == floor && **deadline <= now_ms)
            .map(|(key, _)| *key)
            .collect();
        for key in &expired {
            self.claims.remove(key);
            self.holders.remove(&key.1);
            self.reserved -= BYTES;
        }
        expired
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// The watchdog cancels exactly the expired floor claims that a current
    /// session published. A replaced session's work stays with it until its
    /// own teardown, and every cancellation releases that owner's charge once.
    #[test]
    fn floor_watchdog_cancels_only_current_expired_claims(
        inputs in prop::collection::vec(input(), 1..40),
    ) {
        let config = ZakuraBlockSyncConfig::default();
        let start = Instant::now();
        let registry = PeerRegistry::new();
        let work = Arc::new(WorkQueue::new(block::Height(0)));
        work.set_estimate_floor_for_tests(1);
        let _ = work.extend(
            test_work_scope(),
            HEIGHTS.map(|height| {
                (
                    block::Height(height),
                    block::Hash([u8::try_from(height).expect("small test height"); 32]),
                    BlockSizeEstimate::Confirmed(u32::try_from(BYTES).expect("small test size")),
                )
            }),
        );
        let mut budget = ByteBudget::new(10 * BYTES);
        let mut model = Model::default();
        let mut next_request = 1_u64;

        for input in inputs {
            match input {
                Input::Admit(peer) => {
                    let generation = registry
                        .admit_session(&test_peer(peer), ServicePeerDirection::Outbound, &config, 0, start)
                        .generation();
                    model.admit(peer, generation);
                }
                Input::Take { peer, height, deadline_ms } => {
                    let Some(generation) = model.generations.get(&peer).copied() else {
                        continue;
                    };
                    if model.holders.contains_key(&height) {
                        continue;
                    }
                    let request_id = NonZeroU64::new(next_request).expect("request IDs start at one");
                    next_request += 1;
                    let items = work.take_for_request(
                        block::Height(height),
                        block::Height(height),
                        1,
                        BYTES,
                        generation,
                        request_id,
                    );
                    prop_assert_eq!(items.len(), 1);
                    let owner = items[0].1.owner.expect("a taken item has an owner");
                    prop_assert!(budget.try_reserve(BYTES));
                    let write = RequestWrite::new(
                        owner,
                        items,
                        work.clone(),
                        budget.clone(),
                        CancellationToken::new(),
                    );
                    let deadline = start + Duration::from_millis(deadline_ms);
                    let published = write.publish(|| {
                        registry.insert_outstanding(
                            &test_peer(peer),
                            generation,
                            [(
                                block::Height(height),
                                OutstandingMeta {
                                    owner,
                                    hash: block::Hash([u8::try_from(height).expect("small test height"); 32]),
                                    estimated_bytes: BYTES,
                                    queued_at: start,
                                    deadline,
                                },
                            )],
                        );
                    });
                    prop_assert!(published);
                    prop_assert!(write.try_start());
                    write.written();
                    model.holders.insert(height, Holder { peer, generation, owner });
                    model.claims.insert((peer, height), deadline_ms);
                    model.reserved += BYTES;
                }
                Input::TeardownReplaced { height } => {
                    let Some(holder) = model.holders.get(&height).copied() else {
                        continue;
                    };
                    if model.generations.get(&holder.peer) == Some(&holder.generation) {
                        continue;
                    }
                    let returned = work.revoke_for_owner(holder.owner, [block::Height(height)]);
                    budget.release(returned.released_bytes);
                    prop_assert_eq!(returned.released_bytes, BYTES);
                    model.holders.remove(&height);
                    model.reserved -= BYTES;
                }
                Input::Watchdog { now_ms } => {
                    let mut cancelled = Vec::new();
                    cancel_expired_floor_claims(
                        &registry,
                        &work,
                        &mut budget,
                        block::Height(HEIGHTS[0]),
                        start + Duration::from_millis(now_ms),
                        Duration::from_secs(1),
                        |claim, released| cancelled.push((claim.peer.clone(), claim.height, released)),
                    );
                    let expected = model.watchdog(now_ms);
                    prop_assert_eq!(cancelled.len(), expected.len());
                    for ((peer, height, released), (model_peer, model_height)) in
                        cancelled.iter().zip(&expected)
                    {
                        prop_assert_eq!(peer, &test_peer(*model_peer));
                        prop_assert_eq!(height.0, *model_height);
                        prop_assert_eq!(released.released_bytes, BYTES);
                        prop_assert_eq!(released.returned_count, 1);
                    }
                }
            }

            prop_assert_eq!(budget.reserved(), model.reserved);
            prop_assert_eq!(work.reserved_bytes(), model.reserved);
            prop_assert_eq!(registry.total_unreceived(), model.claims.len());
            for height in HEIGHTS {
                let held = model.holders.contains_key(&height);
                prop_assert_eq!(work.in_flight_contains(block::Height(height)), held);
                prop_assert_eq!(work.pending_contains(block::Height(height)), !held);
            }
        }
    }
}
