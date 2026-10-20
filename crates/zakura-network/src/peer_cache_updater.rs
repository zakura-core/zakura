//! An async task that regularly updates the peer cache on disk from the current address book.

use std::{
    io,
    sync::{Arc, Mutex},
};

use chrono::Utc;
use tokio::time::sleep;

use crate::{
    constants::{DNS_LOOKUP_TIMEOUT, PEER_DISK_CACHE_UPDATE_INTERVAL},
    meta_addr::MetaAddr,
    AddressBook, BoxError, Config,
};

/// An ongoing task that regularly caches the current `address_book` to disk, based on `config`.
#[instrument(skip(config, address_book))]
pub async fn peer_cache_updater(
    config: Config,
    address_book: Arc<Mutex<AddressBook>>,
) -> Result<(), BoxError> {
    // Wait until we've queried DNS and (hopefully) sent peers to the address book.
    // Ideally we'd wait for at least one peer crawl, but that makes tests very slow.
    //
    // TODO: turn the initial sleep time into a parameter of this function,
    //       and allow it to be set in tests
    sleep(DNS_LOOKUP_TIMEOUT * 4).await;

    let mut wrote_cache = false;

    loop {
        // Ignore errors because updating the cache is optional.
        // Errors are already logged by the functions we're calling.
        wrote_cache |= update_peer_cache_once(&config, &address_book)
            .await
            .unwrap_or(false);

        // Right after a cold start, the address book can still be empty at the first attempt,
        // and the cache is only written once there are cacheable peers. Retry soon until the
        // first write, so the cache exists shortly after the node finds its first peers.
        let interval = if wrote_cache {
            PEER_DISK_CACHE_UPDATE_INTERVAL
        } else {
            DNS_LOOKUP_TIMEOUT * 4
        };

        sleep(interval).await;
    }
}

/// Caches peers from the current `address_book` to disk, based on `config`.
///
/// Returns `true` if the cache was written. If no peers are cacheable, preserves
/// previous cached peers except those whose IPs are currently banned.
pub async fn update_peer_cache_once(
    config: &Config,
    address_book: &Arc<Mutex<AddressBook>>,
) -> io::Result<bool> {
    let mut peer_list: std::collections::HashSet<_> = cacheable_peers(address_book)
        .iter()
        .map(|meta_addr| meta_addr.addr)
        .collect();
    if peer_list.is_empty() {
        // Preserve cold-start fallback peers, but never restore a known banned IP.
        peer_list = config.load_peer_cache().await?;
        let bans = address_book
            .lock()
            .expect("unexpected panic in previous thread while accessing the address book")
            .bans();
        let previous_len = peer_list.len();
        peer_list.retain(|peer| !bans.contains(peer.ip()));
        if peer_list.len() == previous_len {
            return Ok(false);
        }
    }

    config.write_peer_cache(peer_list).await?;

    Ok(true)
}

/// Returns a list of cacheable peers, blocking for as short a time as possible.
fn cacheable_peers(address_book: &Arc<Mutex<AddressBook>>) -> Vec<MetaAddr> {
    // TODO: use spawn_blocking() here, if needed to handle address book mutex load
    let now = Utc::now();

    // # Concurrency
    //
    // We return from this function immediately to make sure the address book is unlocked.
    address_book
        .lock()
        .expect("unexpected panic in previous thread while accessing the address book")
        .cacheable(now)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        constants::MAX_PEER_MISBEHAVIOR_SCORE, meta_addr::MetaAddrChange,
        protocol::types::PeerServices, CacheDir, PeerSocketAddr,
    };
    use zakura_chain::serialization::DateTime32;

    #[tokio::test]
    async fn empty_peer_cache_update_removes_banned_ips_but_preserves_safe_peers() {
        let _init_guard = zakura_test::init();
        for retain_safe_peer in [false, true] {
            let cache_dir = tempfile::tempdir().unwrap();
            let config = Config {
                cache_dir: CacheDir::custom_path(cache_dir.path()),
                ..Config::default()
            };
            let banned: PeerSocketAddr = "127.0.0.1:8233".parse().unwrap();
            let other_port: PeerSocketAddr = "127.0.0.1:8234".parse().unwrap();
            let safe: PeerSocketAddr = "127.0.0.2:8233".parse().unwrap();
            let mut cached = std::collections::HashSet::from([banned, other_port]);
            if retain_safe_peer {
                cached.insert(safe);
            }
            config.update_peer_cache(cached).await.unwrap();

            let mut book = AddressBook::new(
                "0.0.0.0:0".parse().unwrap(),
                &config.network,
                2,
                tracing::Span::current(),
            );
            book.update(
                MetaAddr::new_gossiped_meta_addr(
                    banned,
                    PeerServices::NODE_NETWORK,
                    DateTime32::now(),
                )
                .new_gossiped_change()
                .unwrap(),
            );
            book.update(MetaAddrChange::UpdateMisbehavior {
                addr: banned,
                score_increment: MAX_PEER_MISBEHAVIOR_SCORE,
            });
            assert!(book.bans().contains(banned.ip()));
            let inbound: PeerSocketAddr = "127.0.0.3:45678".parse().unwrap();
            book.update(MetaAddr::new_connected(
                inbound,
                &PeerServices::NODE_NETWORK,
                true,
            ));
            book.update(MetaAddr::new_responded(inbound, None));
            assert!(book.get(inbound).is_some());
            assert!(book.cacheable(Utc::now()).is_empty());
            let book = Arc::new(Mutex::new(book));
            update_peer_cache_once(&config, &book).await.unwrap();
            let expected = if retain_safe_peer {
                std::collections::HashSet::from([safe])
            } else {
                std::collections::HashSet::new()
            };
            assert_eq!(config.load_peer_cache().await.unwrap(), expected);
        }
    }
}
