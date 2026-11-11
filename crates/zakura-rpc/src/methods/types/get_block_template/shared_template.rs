//! One full block template per tip, shared by every request that asks for the same work.

use std::sync::{Arc, OnceLock};

use serde_json::value::RawValue;
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
    Ready(ReadyTemplate),
}

/// A finished template, with the JSON of the answers already given from it.
#[derive(Clone, Debug)]
pub(crate) struct ReadyTemplate {
    pub(crate) template: Arc<BlockTemplateResponse>,
    pub(crate) json: Arc<SharedTemplateJson>,
}

/// A shared template's answer as JSON, encoded at most once for each value of `submit_old`.
///
/// `submit_old` is the only field that differs between the requests that share a template. A
/// long poll's answer has it as `true` or `false`, and the answer to a request without a long
/// poll ID leaves it out, so each of the three has one encoding that every caller with that
/// value is given.
#[derive(Debug, Default)]
pub(crate) struct SharedTemplateJson {
    submit_old: OnceLock<Box<RawValue>>,
    discard_old: OnceLock<Box<RawValue>>,
    no_long_poll: OnceLock<Box<RawValue>>,
}

impl SharedTemplateJson {
    /// Returns the JSON of `template`, which must be the template this JSON is shared for with
    /// its caller's `submit_old`. Encodes it on the first call for that `submit_old`.
    ///
    /// Returns `None` if `template` cannot be encoded.
    pub(crate) fn get_or_encode(&self, template: &BlockTemplateResponse) -> Option<&RawValue> {
        let json = match template.submit_old {
            Some(true) => &self.submit_old,
            Some(false) => &self.discard_old,
            None => &self.no_long_poll,
        };
        if let Some(json) = json.get() {
            return Some(json);
        }
        let encoded = serde_json::value::to_raw_value(template).ok()?;
        Some(json.get_or_init(|| encoded))
    }
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

    /// Returns the finished template with `long_poll_id`, without waiting for a build of it.
    pub(crate) fn ready(&self, long_poll_id: &LongPollId) -> Option<ReadyTemplate> {
        match &*self.0.borrow() {
            SharedTemplateState::Ready(ready) if ready.template.long_poll_id == *long_poll_id => {
                Some(ready.clone())
            }
            _ => None,
        }
    }

    /// Returns the template with `long_poll_id` on `tip`, waiting while a build of it is running.
    ///
    /// Returns `None` when the slot holds no such template and no build of it is running, so the
    /// caller builds its own.
    pub(crate) async fn wait_for(
        &self,
        tip: block::Hash,
        long_poll_id: &LongPollId,
    ) -> Option<ReadyTemplate> {
        let mut slot = self.0.subscribe();
        loop {
            match &*slot.borrow_and_update() {
                SharedTemplateState::Ready(ready)
                    if ready.template.long_poll_id == *long_poll_id =>
                {
                    return Some(ready.clone());
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
    /// Shares `template` and returns it as stored, unless a build on another tip has replaced
    /// this one.
    pub(crate) fn publish(self, template: BlockTemplateResponse) -> Option<ReadyTemplate> {
        let ready = ReadyTemplate {
            template: Arc::new(template),
            json: Arc::default(),
        };
        self.replace(SharedTemplateState::Ready(ready.clone()))
            .then_some(ready)
    }

    /// Puts `next` in the slot if this build still holds it, and returns whether it did.
    fn replace(&self, next: SharedTemplateState) -> bool {
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
        })
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
