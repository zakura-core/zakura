//! One full block template per tip, shared by every long poll that asks for the same work.

use std::sync::Arc;

use tokio::sync::watch;
use zakura_chain::block;

use super::BlockTemplateResponse;
use crate::methods::types::long_poll::LongPollId;

/// The newest full template, or the tip that a build for one is running on.
///
/// A caller takes the template only when its long poll ID matches the caller's own. The ID names
/// the tip, the time limit, the mempool transactions and the rejection revision, so a template
/// built for an older tip or an older mempool never matches. A build on a newer tip replaces the
/// slot, and a build that finishes after that is dropped.
#[derive(Clone, Debug)]
pub(crate) struct SharedTemplate(watch::Sender<SharedTemplateState>);

#[derive(Debug, Default)]
enum SharedTemplateState {
    #[default]
    Empty,
    Building(block::Hash),
    Ready(Arc<BlockTemplateResponse>),
}

impl Default for SharedTemplate {
    fn default() -> Self {
        Self(watch::Sender::new(SharedTemplateState::default()))
    }
}

impl SharedTemplate {
    /// Claims the build on `tip`, unless another build on `tip` is already running.
    pub(crate) fn claim(&self, tip: block::Hash) -> Option<SharedTemplateBuild> {
        let mut claimed = false;
        self.0.send_if_modified(|state| {
            claimed = !matches!(state, SharedTemplateState::Building(building) if *building == tip);
            if claimed {
                *state = SharedTemplateState::Building(tip);
            }
            claimed
        });
        claimed.then(|| SharedTemplateBuild {
            slot: self.0.clone(),
            tip,
        })
    }

    /// Returns the template with `long_poll_id`, waiting while a build on `tip` is running.
    ///
    /// Returns `None` when the slot holds no such template and no build on `tip` is running, so
    /// the caller builds its own.
    pub(crate) async fn wait_for(
        &self,
        tip: block::Hash,
        long_poll_id: &LongPollId,
    ) -> Option<Arc<BlockTemplateResponse>> {
        let mut slot = self.0.subscribe();
        loop {
            match &*slot.borrow_and_update() {
                SharedTemplateState::Ready(template) if template.long_poll_id == *long_poll_id => {
                    return Some(template.clone());
                }
                SharedTemplateState::Building(building) if *building == tip => {}
                _ => return None,
            }
            // Every build ends by publishing or by emptying the slot when its claim drops, and
            // `self` keeps the channel open.
            slot.changed().await.ok()?;
        }
    }
}

/// A running build's claim on the [`SharedTemplate`] slot.
///
/// Dropping the claim without publishing empties the slot, so the callers waiting for the build
/// build their own templates.
pub(crate) struct SharedTemplateBuild {
    slot: watch::Sender<SharedTemplateState>,
    tip: block::Hash,
}

impl SharedTemplateBuild {
    /// Shares `template`, unless a build on another tip has replaced this one.
    pub(crate) fn publish(self, template: BlockTemplateResponse) {
        self.replace(SharedTemplateState::Ready(Arc::new(template)));
    }

    fn replace(&self, next: SharedTemplateState) {
        self.slot.send_if_modified(|state| {
            let claimed = matches!(state, SharedTemplateState::Building(tip) if *tip == self.tip);
            if claimed {
                *state = next;
            }
            claimed
        });
    }
}

impl Drop for SharedTemplateBuild {
    fn drop(&mut self) {
        self.replace(SharedTemplateState::Empty);
    }
}
