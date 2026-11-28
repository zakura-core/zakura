//! One alternate announcer for each active legacy transaction download.

use crate::{PeerSocketAddr, PeerSource, Request, Response, SharedPeerError};
use futures::future::BoxFuture;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::sync::oneshot;
use zakura_chain::transaction::UnminedTxId;

#[derive(Clone, Default, Debug)]
pub(super) struct TxAnnouncers(Arc<Mutex<HashMap<UnminedTxId, Sources>>>);

#[derive(Debug)]
struct Sources {
    first: PeerSocketAddr,
    alternate: Option<PeerSocketAddr>,
}

impl Sources {
    fn observe(&mut self, peer: PeerSocketAddr) {
        if peer != self.first {
            self.alternate.get_or_insert(peer);
        }
    }
}

impl TxAnnouncers {
    /// Observe the wire announcement before mempool duplicate suppression.
    pub(super) fn observe(&self, request: &Request) {
        if let Request::AdvertiseTransactionIds(ids, Some(PeerSource::LegacySocket(peer))) = request
        {
            let mut active = self.0.lock().expect("announcer map lock is not poisoned");
            // Large inventory messages need only one lookup per active download.
            if active.len() < ids.len() {
                for (id, sources) in active.iter_mut() {
                    if ids.contains(id) {
                        sources.observe(*peer);
                    }
                }
            } else {
                for id in ids {
                    if let Some(sources) = active.get_mut(id) {
                        sources.observe(*peer);
                    }
                }
            }
        }
    }

    pub(super) fn begin(
        &self,
        id: UnminedTxId,
        first: PeerSocketAddr,
        source: Option<PeerSocketAddr>,
    ) -> Option<Download> {
        let mut active = self.0.lock().expect("announcer map lock is not poisoned");
        if let std::collections::hash_map::Entry::Vacant(entry) = active.entry(id) {
            entry.insert(Sources {
                first,
                alternate: source.filter(|addr| *addr != first),
            });
            Some(Download {
                active: self.clone(),
                id,
            })
        } else {
            None
        }
    }
}

/// Dropping the original caller (including on timeout) removes its entry.
pub(super) struct Download {
    active: TxAnnouncers,
    pub(super) id: UnminedTxId,
}

impl Download {
    pub(super) fn alternate(&self) -> Option<PeerSocketAddr> {
        self.active
            .0
            .lock()
            .expect("announcer map lock is not poisoned")
            .get(&self.id)?
            .alternate
    }
}

impl Drop for Download {
    fn drop(&mut self) {
        self.active
            .0
            .lock()
            .expect("announcer map lock is not poisoned")
            .remove(&self.id);
    }
}

pub(super) type PeerResponse = BoxFuture<'static, Result<Response, SharedPeerError>>;

pub(super) struct Retry {
    pub download: Download,
    pub first: PeerSocketAddr,
    pub reply: oneshot::Sender<PeerResponse>,
}
