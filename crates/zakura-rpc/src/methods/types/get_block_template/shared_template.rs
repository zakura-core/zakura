//! One full block template per tip, shared by every long poll that asks for the same work.

use std::sync::Arc;

use tokio::sync::watch;
use zakura_chain::block;

use super::BlockTemplateResponse;
use crate::methods::types::long_poll::LongPollId;

/// The newest full template, or the build for one that is running.
///
/// A caller takes the template only when its long poll ID matches the caller's own. The ID names
/// the tip, the time limit, the mempool transactions and the rejection revision, so a template
/// built for an older tip or an older mempool never matches. A caller waits only for a build of
/// its own long poll ID.
///
/// One build per tip holds the slot: a build on a newer tip replaces it, and a build that
/// finishes after that is dropped. A caller that asks for other work on the same tip while a
/// build runs builds its own template without sharing it.
#[derive(Clone, Debug)]
pub(crate) struct SharedTemplate(watch::Sender<SharedTemplateState>);

#[derive(Debug, Default)]
enum SharedTemplateState {
    #[default]
    Empty,
    Building {
        tip: block::Hash,
        long_poll_id: LongPollId,
    },
    Ready(Arc<BlockTemplateResponse>),
}

impl Default for SharedTemplate {
    fn default() -> Self {
        Self(watch::Sender::new(SharedTemplateState::default()))
    }
}

impl SharedTemplate {
    /// Claims the build of the template with `long_poll_id` on `tip`, unless another build on
    /// `tip` is already running.
    pub(crate) fn claim(
        &self,
        tip: block::Hash,
        long_poll_id: LongPollId,
    ) -> Option<SharedTemplateBuild> {
        let mut claimed = false;
        self.0.send_if_modified(|state| {
            claimed = !matches!(
                state,
                SharedTemplateState::Building { tip: building, .. } if *building == tip
            );
            if claimed {
                *state = SharedTemplateState::Building { tip, long_poll_id };
            }
            claimed
        });
        claimed.then(|| SharedTemplateBuild {
            slot: self.0.clone(),
            tip,
            long_poll_id,
        })
    }

    /// Returns the template with `long_poll_id` on `tip`, waiting while a build of it is running.
    ///
    /// Returns `None` when the slot holds no such template and no build of it is running, so the
    /// caller builds its own.
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
                SharedTemplateState::Building {
                    tip: building,
                    long_poll_id: building_id,
                } if *building == tip && building_id == long_poll_id => {}
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
    long_poll_id: LongPollId,
}

impl SharedTemplateBuild {
    /// Shares `template`, unless a build on another tip has replaced this one.
    pub(crate) fn publish(self, template: BlockTemplateResponse) {
        self.replace(SharedTemplateState::Ready(Arc::new(template)));
    }

    fn replace(&self, next: SharedTemplateState) {
        self.slot.send_if_modified(|state| {
            let claimed = matches!(
                state,
                SharedTemplateState::Building { tip, long_poll_id }
                    if *tip == self.tip && *long_poll_id == self.long_poll_id
            );
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

#[cfg(test)]
mod tests {
    use futures::FutureExt;
    use zakura_chain::{block::Height, serialization::DateTime32};

    use super::*;
    use crate::methods::types::long_poll::LongPollInput;

    /// A long poll that asks for other work on the same tip builds its own template at once,
    /// instead of waiting for a build it cannot use.
    #[test]
    fn only_a_build_of_the_same_work_is_waited_for() {
        let tip = block::Hash([1; 32]);
        let building =
            LongPollInput::new(Height(1), tip, DateTime32::from(1_654_008_728), []).generate_id();
        let mut other_work = building;
        other_work.revision += 1;

        let slot = SharedTemplate::default();
        let _build = slot.claim(tip, building).expect("the slot starts empty");

        assert!(
            slot.wait_for(tip, &building).now_or_never().is_none(),
            "a long poll for the work being built waits for it"
        );
        assert_eq!(
            slot.wait_for(tip, &other_work)
                .now_or_never()
                .map(|shared| shared.is_none()),
            Some(true),
            "a long poll for other work does not wait"
        );
        assert!(
            slot.claim(tip, other_work).is_none(),
            "a build on the same tip keeps the slot"
        );
    }
}
