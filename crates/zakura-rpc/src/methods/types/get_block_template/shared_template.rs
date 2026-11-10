//! Completed mining work and ownership of one coalesced speculative build.

use super::BlockTemplateResponse;
use crate::methods::types::long_poll::LongPollId;
use orchard::circuit::ProofCancellation;
use std::sync::Arc;
use tokio::sync::watch;
use zakura_chain::block;

#[derive(Clone, Debug)]
pub(crate) struct SharedTemplate(watch::Sender<State>);

#[derive(Clone, Debug)]
struct Running {
    parent: block::Hash,
    generation: Arc<()>,
    cancellation: ProofCancellation,
}

#[derive(Clone, Debug)]
struct Ready {
    template: Arc<BlockTemplateResponse>,
    validity: u64,
}

#[derive(Debug, Default)]
struct State {
    running: Option<Running>,
    ready: Option<Ready>,
}

impl Default for SharedTemplate {
    fn default() -> Self {
        Self(watch::Sender::new(State::default()))
    }
}

impl SharedTemplate {
    pub(crate) fn subscribe(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let mut receiver = self.0.subscribe();
        async move {
            let _ = receiver.changed().await;
        }
    }

    /// Invalidates completed work and signals cancellation without releasing CPU capacity.
    pub(crate) fn invalidate(&self) {
        self.0.send_modify(|state| {
            if let Some(running) = &state.running {
                running.cancellation.cancel();
            }
            state.ready = None;
        });
    }

    /// Takes immediately available populated work from the current validity revision.
    pub(crate) fn ready(
        &self,
        parent: block::Hash,
        validity: u64,
    ) -> Option<Arc<BlockTemplateResponse>> {
        self.0
            .borrow()
            .ready
            .as_ref()
            .filter(|ready| {
                ready.validity == validity
                    && ready.template.previous_block_hash == parent
                    && !ready.template.transactions.is_empty()
            })
            .map(|ready| ready.template.clone())
    }

    pub(crate) fn claim(&self, parent: block::Hash) -> Option<SharedTemplateBuild> {
        self.claim_snapshot(parent, 0)
    }

    /// Retains completed work while refreshing; a cancelled build still owns its CPU permit.
    pub(crate) fn claim_snapshot(
        &self,
        parent: block::Hash,
        validity: u64,
    ) -> Option<SharedTemplateBuild> {
        let running = Running {
            parent,
            generation: Arc::new(()),
            cancellation: ProofCancellation::default(),
        };
        let mut claimed = false;
        self.0.send_if_modified(|state| {
            if let Some(active) = &state.running {
                if active.parent == parent && !active.cancellation.is_cancelled() {
                    return false;
                }
                active.cancellation.cancel();
            }
            state.running = Some(running.clone());
            claimed = true;
            true
        });
        claimed.then(|| SharedTemplateBuild {
            slot: self.0.clone(),
            running,
            validity,
        })
    }

    pub(crate) async fn wait_for(
        &self,
        parent: block::Hash,
        id: &LongPollId,
    ) -> Option<Arc<BlockTemplateResponse>> {
        let mut slot = self.0.subscribe();
        loop {
            {
                let state = slot.borrow_and_update();
                if let Some(ready) = &state.ready {
                    if ready.template.long_poll_id == *id {
                        return Some(ready.template.clone());
                    }
                }
                if !state.running.as_ref().is_some_and(|running| {
                    running.parent == parent && !running.cancellation.is_cancelled()
                }) {
                    return None;
                }
            }
            slot.changed().await.ok()?;
        }
    }
}

pub(crate) struct SharedTemplateBuild {
    slot: watch::Sender<State>,
    running: Running,
    validity: u64,
}

impl SharedTemplateBuild {
    pub(crate) fn cancellation(&self) -> ProofCancellation {
        self.running.cancellation.clone()
    }

    pub(crate) fn publish(self, template: BlockTemplateResponse) {
        self.slot.send_if_modified(|state| {
            if !self.owns(state) || self.running.cancellation.is_cancelled() {
                return false;
            }
            state.ready = Some(Ready {
                template: Arc::new(template),
                validity: self.validity,
            });
            state.running = None;
            true
        });
    }

    fn owns(&self, state: &State) -> bool {
        state
            .running
            .as_ref()
            .is_some_and(|active| Arc::ptr_eq(&active.generation, &self.running.generation))
    }
}

impl Drop for SharedTemplateBuild {
    fn drop(&mut self) {
        self.running.cancellation.cancel();
        self.slot.send_if_modified(|state| {
            if !self.owns(state) {
                return false;
            }
            state.running = None;
            true
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returning_to_a_parent_does_not_restore_old_build_ownership() {
        let shared = SharedTemplate::default();
        let parent = block::Hash([1; 32]);
        let old = shared.claim(parent).unwrap();
        let old_signal = old.cancellation();
        shared.invalidate();
        let replacement = shared.claim(parent).unwrap();
        assert!(old_signal.is_cancelled());
        drop(old);
        assert!(shared.claim(parent).is_none());
        assert!(!replacement.cancellation().is_cancelled());
    }

    #[test]
    fn dropping_a_build_cancels_its_cpu_work() {
        let shared = SharedTemplate::default();
        let build = shared.claim(block::Hash([1; 32])).unwrap();
        let cancellation = build.cancellation();
        drop(build);
        assert!(cancellation.is_cancelled());
    }
}
