//! Local, single-completion evidence for block discovery.

use futures::task::AtomicWaker;
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc,
};

pub(super) const PENDING: u8 = 0;
pub(super) const NEUTRAL: u8 = 1;
pub(super) const STALLED: u8 = 2;
pub(super) const VERIFIED: u8 = 3;
pub(super) const EXPIRED: u8 = 4;

#[derive(Debug)]
pub(super) struct Completion {
    pub outcome: AtomicU8,
    pub wake: Arc<AtomicWaker>,
}

/// Feedback for one selected connection's discovery response.
/// Clones share one completion. This capability never crosses the wire.
#[derive(Clone, Debug)]
pub struct DiscoveryFeedback(Arc<Consumer>);

#[derive(Debug)]
struct Consumer {
    completion: Arc<Completion>,
}
impl Drop for Consumer {
    fn drop(&mut self) {
        let _ = self.completion.outcome.compare_exchange(
            PENDING,
            NEUTRAL,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        self.completion.wake.wake();
    }
}

impl PartialEq for DiscoveryFeedback {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for DiscoveryFeedback {}

impl DiscoveryFeedback {
    pub(super) fn new(completion: Arc<Completion>) -> Self {
        Self(Arc::new(Consumer { completion }))
    }

    /// Credits a previously unknown advertised hash after its body commits.
    pub fn verified(&self) {
        self.complete(VERIFIED);
    }
    /// Reports a response with no usable candidates. This is availability evidence only.
    pub fn no_progress(&self) {
        self.complete(STALLED);
    }
    /// Releases expired evidence and requests a temporary discovery rotation, without a strike.
    pub fn expired(&self) {
        self.complete(EXPIRED);
    }
    fn complete(&self, outcome: u8) {
        let _ = self.0.completion.outcome.compare_exchange(
            PENDING,
            outcome,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        self.0.completion.wake.wake();
    }
}

/// Holds discovery evidence for one in-flight request and resolves it on cancellation.
///
/// A dropped [`DiscoveryFeedback`] completes as `NEUTRAL`, which records nothing. The syncer
/// cancels a discovery request after six seconds, long before the connection classifies a
/// silent peer as a receive timeout, so a peer that accepts `getblocks` and then stays quiet —
/// or answers with an unrelated message the handler never completes on — would stay eligible
/// and repeat that indefinitely. Cancellation instead expires the evidence, which rotates the
/// peer out for the reprobe delay without charging it a misconduct strike.
#[derive(Debug)]
pub(super) struct PendingDiscovery(Option<DiscoveryFeedback>);

impl PendingDiscovery {
    pub(super) fn new(feedback: Option<DiscoveryFeedback>) -> Self {
        Self(feedback)
    }

    /// Disarms the guard once the request produced a result, so the caller classifies it.
    pub(super) fn responded(mut self) -> Option<DiscoveryFeedback> {
        self.0.take()
    }
}

impl Drop for PendingDiscovery {
    fn drop(&mut self) {
        if let Some(feedback) = self.0.take() {
            feedback.expired();
        }
    }
}
