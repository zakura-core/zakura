//! Fixed test vectors for the inventory registry.

use std::{cmp::min, net::SocketAddr, sync::Arc};

use tokio::sync::{watch, Semaphore};

use zakura_chain::{block, serialization::AtLeastOne, transaction};

use crate::{
    peer_set::inventory_registry::{
        tests::new_inv_registry, InventoryChange, InventoryMarker, InventoryRegistry,
        InventoryStatus, MAX_COMPACT_SOURCES, MAX_INV_PER_MAP, MAX_PEERS_PER_INV,
    },
    protocol::external::InventoryHash,
    PeerSocketAddr,
};

/// Check an empty inventory registry works as expected.
#[tokio::test(start_paused = true)]
async fn inv_registry_empty_ok() {
    let fake_hash = InventoryHash::Error;

    let (mut inv_registry, _inv_stream_tx) = new_inv_registry();

    inv_registry
        .update()
        .await
        .expect("unexpected dropped registry sender channel");

    assert_eq!(inv_registry.advertising_peers(fake_hash).count(), 0);
    assert_eq!(inv_registry.missing_peers(fake_hash).count(), 0);
}

/// Check inventory registration for one advertised hash/peer.
#[tokio::test]
async fn inv_registry_one_advertised_ok() {
    let test_hash = InventoryHash::Block(block::Hash([0; 32]));
    let test_peer = "1.1.1.1:1"
        .parse()
        .expect("unexpected invalid peer address");
    let test_change = InventoryStatus::new_available(test_hash, test_peer);

    let (mut inv_registry, inv_stream_tx) = new_inv_registry();

    let receiver_count = inv_stream_tx
        .send(test_change)
        .expect("unexpected failed inventory status send");
    assert_eq!(receiver_count, 1);

    inv_registry
        .update()
        .await
        .expect("unexpected dropped registry sender channel");

    assert_eq!(
        inv_registry.advertising_peers(test_hash).next(),
        Some(&test_peer),
    );
    assert_eq!(inv_registry.advertising_peers(test_hash).count(), 1);
    assert_eq!(inv_registry.missing_peers(test_hash).count(), 0);
}

/// Check inventory registration for one missing hash/peer.
#[tokio::test]
async fn inv_registry_one_missing_ok() {
    let test_hash = InventoryHash::Block(block::Hash([0; 32]));
    let test_peer = "1.1.1.1:1"
        .parse()
        .expect("unexpected invalid peer address");
    let test_change = InventoryStatus::new_missing(test_hash, test_peer);

    let (mut inv_registry, inv_stream_tx) = new_inv_registry();

    let receiver_count = inv_stream_tx
        .send(test_change)
        .expect("unexpected failed inventory status send");
    assert_eq!(receiver_count, 1);

    inv_registry
        .update()
        .await
        .expect("unexpected dropped registry sender channel");

    assert_eq!(inv_registry.advertising_peers(test_hash).count(), 0);
    assert_eq!(
        inv_registry.missing_peers(test_hash).next(),
        Some(&test_peer),
    );
    assert_eq!(inv_registry.missing_peers(test_hash).count(), 1);
}

/// Check inventory registration for one hash/peer prefers missing over advertised.
#[tokio::test]
async fn inv_registry_prefer_missing_ok() {
    inv_registry_prefer_missing_order(true).await;
    inv_registry_prefer_missing_order(false).await;
}

async fn inv_registry_prefer_missing_order(missing_first: bool) {
    let test_hash = InventoryHash::Block(block::Hash([0; 32]));
    let test_peer = "1.1.1.1:1"
        .parse()
        .expect("unexpected invalid peer address");

    let missing_change = InventoryStatus::new_missing(test_hash, test_peer);
    let advertised_change = InventoryStatus::new_available(test_hash, test_peer);

    let (mut inv_registry, inv_stream_tx) = new_inv_registry();

    let changes = if missing_first {
        [missing_change, advertised_change]
    } else {
        [advertised_change, missing_change]
    };

    for change in changes {
        let receiver_count = inv_stream_tx
            .send(change)
            .expect("unexpected failed inventory status send");
        assert_eq!(receiver_count, 1);
    }

    // TODO: also test with updates after each change
    inv_registry
        .update()
        .await
        .expect("unexpected dropped registry sender channel");

    assert_eq!(inv_registry.advertising_peers(test_hash).count(), 0);
    assert_eq!(
        inv_registry.missing_peers(test_hash).next(),
        Some(&test_peer),
    );
    assert_eq!(inv_registry.missing_peers(test_hash).count(), 1);
}

/// Check inventory registration for one hash/peer prefers current over previous.
#[tokio::test]
async fn inv_registry_prefer_current_ok() {
    inv_registry_prefer_current_order(true).await;
    inv_registry_prefer_current_order(false).await;
}

async fn inv_registry_prefer_current_order(missing_current: bool) {
    let test_hash = InventoryHash::Block(block::Hash([0; 32]));
    let test_peer = "1.1.1.1:1"
        .parse()
        .expect("unexpected invalid peer address");

    let missing_change = InventoryStatus::new_missing(test_hash, test_peer);
    let advertised_change = InventoryStatus::new_available(test_hash, test_peer);

    let (mut inv_registry, inv_stream_tx) = new_inv_registry();

    let changes = if missing_current {
        [advertised_change, missing_change]
    } else {
        [missing_change, advertised_change]
    };

    for change in changes {
        // This rotation has no effect in the first loop iteration, because the registry is empty.
        inv_registry.rotate();

        let receiver_count = inv_stream_tx
            .send(change)
            .expect("unexpected failed inventory status send");
        assert_eq!(receiver_count, 1);

        // We must update after each change, so the rotation puts the first change in `prev`.
        inv_registry
            .update()
            .await
            .expect("unexpected dropped registry sender channel");
    }

    if missing_current {
        assert_eq!(inv_registry.advertising_peers(test_hash).count(), 0);
        assert_eq!(
            inv_registry.missing_peers(test_hash).next(),
            Some(&test_peer),
        );
        assert_eq!(inv_registry.missing_peers(test_hash).count(), 1);
    } else {
        assert_eq!(
            inv_registry.advertising_peers(test_hash).next(),
            Some(&test_peer),
        );
        assert_eq!(inv_registry.advertising_peers(test_hash).count(), 1);
        assert_eq!(inv_registry.missing_peers(test_hash).count(), 0);
    }
}

/// Check inventory registration limits.
#[tokio::test]
async fn inv_registry_limit() {
    inv_registry_limit_for(InventoryMarker::Available(())).await;
    inv_registry_limit_for(InventoryMarker::Missing(())).await;
}

/// Check the inventory registration limit for `status`.
async fn inv_registry_limit_for(status: InventoryMarker) {
    let single_test_hash = InventoryHash::Block(block::Hash([0xbb; 32]));
    let single_test_peer = "1.1.1.1:1"
        .parse()
        .expect("unexpected invalid peer address");

    let (mut inv_registry, inv_stream_tx) = new_inv_registry();

    // Check hash limit
    for hash_count in 0..(MAX_INV_PER_MAP + 10) {
        let mut test_hash = hash_count.to_ne_bytes().to_vec();
        test_hash.resize(32, 0);
        let test_hash = InventoryHash::Tx(transaction::Hash(test_hash.try_into().unwrap()));

        let test_change = status.map(|()| {
            let at_least_one = AtLeastOne::from_vec(vec![test_hash]);

            match at_least_one {
                Ok(at_least_one) => (at_least_one, single_test_peer),
                Err(_) => {
                    panic!("failed to create AtLeastOne")
                }
            }
        });

        let receiver_count = inv_stream_tx
            .send(test_change)
            .expect("unexpected failed inventory status send");

        assert_eq!(receiver_count, 1);

        inv_registry
            .update()
            .await
            .expect("unexpected dropped registry sender channel");

        if status.is_available() {
            assert_eq!(inv_registry.advertising_peers(test_hash).count(), 1);
            assert_eq!(inv_registry.missing_peers(test_hash).count(), 0);
        } else {
            assert_eq!(inv_registry.advertising_peers(test_hash).count(), 0);
            assert_eq!(inv_registry.missing_peers(test_hash).count(), 1);
        }

        // SECURITY: limit inventory memory usage
        assert_eq!(
            inv_registry.status_hashes().count(),
            min(hash_count + 1, MAX_INV_PER_MAP),
        );
    }

    // Check peer address per hash limit
    let (mut inv_registry, inv_stream_tx) = new_inv_registry();

    for peer_count in 0..(MAX_PEERS_PER_INV + 10) {
        let test_peer: PeerSocketAddr = SocketAddr::new(
            "2.2.2.2".parse().unwrap(),
            peer_count.try_into().expect("fits in u16"),
        )
        .into();

        let test_change = status.map(|()| {
            let at_least_one = AtLeastOne::from_vec(vec![single_test_hash]);

            match at_least_one {
                Ok(at_least_one) => (at_least_one, test_peer),
                Err(_) => {
                    panic!("failed to create AtLeastOne")
                }
            }
        });

        let receiver_count = inv_stream_tx
            .send(test_change)
            .expect("unexpected failed inventory status send");

        assert_eq!(receiver_count, 1);

        inv_registry
            .update()
            .await
            .expect("unexpected dropped registry sender channel");

        assert_eq!(inv_registry.status_hashes().count(), 1);

        let limited_count = min(peer_count + 1, MAX_PEERS_PER_INV);

        // SECURITY: limit inventory memory usage
        if status.is_available() {
            assert_eq!(
                inv_registry.advertising_peers(single_test_hash).count(),
                limited_count,
            );
            assert_eq!(inv_registry.missing_peers(single_test_hash).count(), 0);
        } else {
            assert_eq!(inv_registry.advertising_peers(single_test_hash).count(), 0);
            assert_eq!(
                inv_registry.missing_peers(single_test_hash).count(),
                limited_count,
            );
        }
    }
}

/// An oversized multi-hash change keeps only [`MAX_INV_PER_MAP`] hashes, and does not pin
/// the allocation for the rest.
#[test]
fn multi_change_caps_before_collecting() {
    let peer: PeerSocketAddr = "127.0.0.1:8233".parse().unwrap();
    let hashes: Vec<InventoryHash> = (0..50_000u32)
        .map(|i| {
            let mut bytes = [0; 32];
            bytes[..4].copy_from_slice(&i.to_le_bytes());
            InventoryHash::Tx(transaction::Hash(bytes))
        })
        .collect();

    for change in [
        InventoryChange::new_available_multi(&hashes, peer),
        InventoryChange::new_missing_multi(&hashes, peer),
    ] {
        let (InventoryStatus::Available((kept, _)) | InventoryStatus::Missing((kept, _))) =
            change.expect("the change has hashes");
        assert_eq!(kept.len(), MAX_INV_PER_MAP);
        assert!(kept.as_vec().capacity() <= 2 * MAX_INV_PER_MAP);
    }
}

/// Registers `count` compact block sources, and returns their peers and source watches.
fn add_compact_sources(
    registry: &mut InventoryRegistry,
    count: u8,
) -> (
    Vec<PeerSocketAddr>,
    Vec<watch::Receiver<Option<Arc<Semaphore>>>>,
) {
    (1..=count)
        .map(|ip| {
            let peer: PeerSocketAddr = SocketAddr::from(([127, 0, 0, ip], 8233)).into();
            let (sender, receiver) = watch::channel(None);
            registry.add_compact_source(peer, sender);
            (peer, receiver)
        })
        .unzip()
}

/// Returns which sources are selected as high-bandwidth sources.
fn selected(sources: &[watch::Receiver<Option<Arc<Semaphore>>>]) -> Vec<bool> {
    sources
        .iter()
        .map(|source| source.borrow().is_some())
        .collect()
}

/// Registers an `Available` status for the block `hash` from `peer`.
fn announce(registry: &mut InventoryRegistry, hash: block::Hash, peer: PeerSocketAddr) {
    registry.register(InventoryStatus::new_available(
        InventoryHash::Block(hash),
        peer,
    ));
}

/// The first three sources are selected in registration order, and share one set of three
/// slots.
#[tokio::test]
async fn compact_sources_are_capped_at_three() {
    let (mut registry, _inv_stream_tx) = new_inv_registry();
    let (_peers, sources) = add_compact_sources(&mut registry, 6);

    registry.select_compact_sources(None, |_| true);

    assert_eq!(selected(&sources), [true, true, true, false, false, false]);
    let slots: Vec<Arc<Semaphore>> = sources[..3]
        .iter()
        .map(|source| source.borrow().clone().expect("the source is selected"))
        .collect();
    assert!(slots.iter().all(|slot| Arc::ptr_eq(slot, &slots[0])));
    assert_eq!(slots[0].available_permits(), MAX_COMPACT_SOURCES);
}

/// The first peer to announce a new best tip moves to the front, and the last selected source
/// is demoted.
#[tokio::test]
async fn first_announcer_of_new_tip_moves_to_front() {
    let (mut registry, _inv_stream_tx) = new_inv_registry();
    let (peers, sources) = add_compact_sources(&mut registry, 4);
    registry.select_compact_sources(None, |_| true);

    let tip = block::Hash([1; 32]);
    announce(&mut registry, tip, peers[3]);
    announce(&mut registry, tip, peers[1]);
    registry.select_compact_sources(Some(tip), |_| true);

    assert_eq!(selected(&sources), [true, true, false, true]);
}

/// The previous interval holds earlier announcements, so it is searched before the current one.
#[tokio::test]
async fn first_announcer_is_searched_in_the_previous_interval_first() {
    let (mut registry, _inv_stream_tx) = new_inv_registry();
    let (peers, sources) = add_compact_sources(&mut registry, 5);
    registry.select_compact_sources(None, |_| true);

    let tip = block::Hash([1; 32]);
    announce(&mut registry, tip, peers[3]);
    registry.rotate();
    announce(&mut registry, tip, peers[4]);
    registry.select_compact_sources(Some(tip), |_| true);

    assert_eq!(selected(&sources), [true, true, false, true, false]);
}

/// A peer whose announcement became `Missing` gets no credit, and a `Missing` status in the
/// previous interval does not hide an announcement in the current one.
#[tokio::test]
async fn missing_status_is_not_an_announcement() {
    let (mut registry, _inv_stream_tx) = new_inv_registry();
    let (peers, sources) = add_compact_sources(&mut registry, 5);
    registry.select_compact_sources(None, |_| true);

    let tip = block::Hash([1; 32]);
    registry.register(InventoryStatus::new_missing(
        InventoryHash::Block(tip),
        peers[0],
    ));
    announce(&mut registry, tip, peers[3]);
    registry.register(InventoryStatus::new_missing(
        InventoryHash::Block(tip),
        peers[3],
    ));
    registry.rotate();
    announce(&mut registry, tip, peers[4]);
    registry.select_compact_sources(Some(tip), |_| true);

    assert_eq!(selected(&sources), [true, true, false, false, true]);
}

/// Selection with an unchanged best tip keeps the order, even if another peer is now the first
/// announcer of that tip.
#[tokio::test]
async fn unchanged_tip_keeps_the_order() {
    let (mut registry, _inv_stream_tx) = new_inv_registry();
    let (peers, sources) = add_compact_sources(&mut registry, 5);

    let tip = block::Hash([1; 32]);
    announce(&mut registry, tip, peers[3]);
    registry.select_compact_sources(Some(tip), |_| true);
    assert_eq!(selected(&sources), [true, true, false, true, false]);

    // Two rotations forget the first announcement.
    registry.rotate();
    registry.rotate();
    announce(&mut registry, tip, peers[4]);
    registry.select_compact_sources(Some(tip), |_| true);

    assert_eq!(selected(&sources), [true, true, false, true, false]);
}

/// A disconnected peer loses its selection and its candidacy, and the next candidate is
/// selected.
#[tokio::test]
async fn disconnected_source_is_demoted_and_removed() {
    let (mut registry, _inv_stream_tx) = new_inv_registry();
    let (peers, sources) = add_compact_sources(&mut registry, 4);
    registry.select_compact_sources(None, |_| true);

    let disconnected = peers[0];
    registry.select_compact_sources(None, |peer| *peer != disconnected);
    assert_eq!(selected(&sources), [false, true, true, true]);

    // The removed peer stays removed when the predicate would accept it again.
    let tip = block::Hash([1; 32]);
    announce(&mut registry, tip, disconnected);
    registry.select_compact_sources(Some(tip), |_| true);
    assert_eq!(selected(&sources), [false, true, true, true]);
}

/// A source whose connection closed its watch is removed, and the next candidate is selected.
#[tokio::test]
async fn closed_source_watch_is_removed() {
    let (mut registry, _inv_stream_tx) = new_inv_registry();
    let (_peers, mut sources) = add_compact_sources(&mut registry, 4);
    registry.select_compact_sources(None, |_| true);

    drop(sources.remove(1));
    registry.select_compact_sources(None, |_| true);

    assert_eq!(selected(&sources), [true, true, true]);
}
