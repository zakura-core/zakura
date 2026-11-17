//! The ordered queue of best-chain changes, from the state writer to the pool
//! core.
//!
//! The state writer holds the [`ChainTransitionSender`]. For each change to
//! the best chain it calls [`ChainTransitionSender::begin_change`] before any
//! new state becomes readable, and [`ChainTransitionSender::finish_change`]
//! once the new tip is published. The second call moves the
//! [`GenerationCell`] to its next stable number and queues the
//! [`ChainTransition`] for the one consumer.
//!
//! The consumer subscribes through a [`ChainTransitions`] handle and reads
//! [`ChainEvents`]. The queue holds at most [`MAX_QUEUED_TRANSITION_BLOCKS`]
//! blocks. A transition that does not fit replaces the whole queue with one
//! [`ChainEvent::Resync`] at the tip it ends on. With no consumer subscribed,
//! nothing is built or queued.

use std::sync::Arc;

use zakura_rt::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex, MutexGuard, Notify, PoisonError,
};

use super::{ChainEvent, ChainTip, ChainTransition, Generation, GenerationCell, GenerationRead};

#[cfg(test)]
mod tests;

/// The most blocks the queued transitions can name together. Provisional.
pub const MAX_QUEUED_TRANSITION_BLOCKS: usize = 128;

/// Where a subscription starts: the best tip the consumer can take as applied,
/// and its stable generation. The first queued transition starts from this
/// tip, or the first event is a [`ChainEvent::Resync`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ChainStart {
    /// The best tip, or `None` before the genesis block.
    pub tip: Option<ChainTip>,
    /// The stable generation of that tip.
    pub generation: Generation,
}

/// [`ChainTransitions::subscribe`] was called while another consumer holds
/// the queue.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct AlreadySubscribed;

impl std::fmt::Display for AlreadySubscribed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the chain transition queue already has a consumer")
    }
}

impl std::error::Error for AlreadySubscribed {}

/// What the sender and the consumer share.
#[derive(Debug)]
struct Inner {
    generation: GenerationCell,
    /// Mirrors [`Queue::subscribed`], so the writer can check it without the
    /// lock. Written only under the lock.
    subscribed: AtomicBool,
    queue: Mutex<Queue>,
    notify: Notify,
}

/// The queue and the tip it ends on.
#[derive(Debug)]
struct Queue {
    /// The last published best tip.
    tip: Option<ChainTip>,
    /// Its stable generation.
    generation: Generation,
    events: std::collections::VecDeque<ChainEvent>,
    /// The blocks the queued transitions name.
    blocks: usize,
    subscribed: bool,
    /// The sender is gone.
    closed: bool,
}

impl Inner {
    /// Lock the queue. A panic while it was held leaves it consistent, because
    /// every change to it is a single assignment or push.
    fn lock(&self) -> MutexGuard<'_, Queue> {
        self.queue.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The state writer's half of the queue. There is one, and it is not
/// cloneable, because the generation has one writer.
#[derive(Debug)]
pub struct ChainTransitionSender {
    inner: Arc<Inner>,
    /// The last tip a change finished at. The writer's own copy, so checking
    /// whether a publication changes the best chain takes no lock.
    tip: Option<ChainTip>,
}

/// Create the queue, starting at `tip` and generation zero, with no consumer.
pub fn chain_transitions(tip: Option<ChainTip>) -> ChainTransitionSender {
    let generation = GenerationCell::new();
    let GenerationRead::Stable(start) = generation.read() else {
        unreachable!("a new generation cell is stable");
    };
    ChainTransitionSender {
        inner: Arc::new(Inner {
            generation,
            subscribed: AtomicBool::new(false),
            queue: Mutex::new(Queue {
                tip,
                generation: start,
                events: std::collections::VecDeque::new(),
                blocks: 0,
                subscribed: false,
                closed: false,
            }),
            notify: Notify::new(),
        }),
        tip,
    }
}

impl ChainTransitionSender {
    /// The handle readers use to read the generation and to subscribe.
    pub fn transitions(&self) -> ChainTransitions {
        ChainTransitions {
            inner: self.inner.clone(),
        }
    }

    /// The last tip a change finished at, or the starting tip. A publication
    /// whose tip differs from this is a change.
    pub fn tip(&self) -> Option<ChainTip> {
        self.tip
    }

    /// Whether a consumer is subscribed now. The writer checks this before it
    /// gathers what a transition needs, so nothing is gathered without one.
    pub fn is_subscribed(&self) -> bool {
        self.inner.subscribed.load(Ordering::Acquire)
    }

    /// Mark the best chain as changing. Call it before any new best-chain
    /// state becomes readable.
    ///
    /// # Panics
    ///
    /// If a change has already begun.
    pub fn begin_change(&self) {
        self.inner.generation.begin_change();
    }

    /// Mark the change begun by [`Self::begin_change`] as finished at `after`,
    /// once that tip is published, and return its stable generation.
    ///
    /// If a consumer is subscribed, `build` makes the transition from the
    /// previous tip and the new generation. It returns `None` when the writer
    /// does not know which blocks changed, and the consumer then gets a
    /// [`ChainEvent::Resync`]. `build` is not called without a consumer.
    ///
    /// # Panics
    ///
    /// If no change has begun.
    pub fn finish_change(
        &mut self,
        after: ChainTip,
        build: impl FnOnce(Option<ChainTip>, Generation) -> Option<ChainTransition>,
    ) -> Generation {
        self.tip = Some(after);
        let generation = self.inner.generation.finish_change();
        let mut queue = self.inner.lock();
        let before = queue.tip.replace(after);
        queue.generation = generation;
        if queue.subscribed {
            let event = match build(before, generation) {
                Some(transition) => ChainEvent::Transition(transition),
                None => ChainEvent::Resync {
                    tip: after,
                    generation,
                },
            };
            queue.push(event);
            drop(queue);
            self.inner.notify.notify_one();
        }
        generation
    }

    /// Finish a change whose result the writer does not know, such as a
    /// failed write that may have landed, and return its stable generation.
    ///
    /// `tip` is the best tip readers see now, if the writer knows it, or the
    /// tip stays the last one published. A consumer gets a
    /// [`ChainEvent::Resync`] at that tip. Before the genesis block there is
    /// no tip and nothing to resynchronize, so nothing is queued.
    ///
    /// # Panics
    ///
    /// If no change has begun.
    pub fn finish_unknown_change(&mut self, tip: Option<ChainTip>) -> Generation {
        if tip.is_some() {
            self.tip = tip;
        }
        let generation = self.inner.generation.finish_change();
        let mut queue = self.inner.lock();
        if tip.is_some() {
            queue.tip = tip;
        }
        queue.generation = generation;
        if let (true, Some(tip)) = (queue.subscribed, queue.tip) {
            queue.push(ChainEvent::Resync { tip, generation });
            drop(queue);
            self.inner.notify.notify_one();
        }
        generation
    }
}

impl Drop for ChainTransitionSender {
    fn drop(&mut self) {
        self.inner.lock().closed = true;
        self.inner.notify.notify_one();
    }
}

impl Queue {
    /// Queue `event`. A transition that does not fit beside the queued ones
    /// replaces them all with a resync at the queue's tip.
    fn push(&mut self, event: ChainEvent) {
        let fits = match &event {
            ChainEvent::Transition(transition) => {
                let blocks = self.blocks.saturating_add(transition.blocks());
                (blocks <= MAX_QUEUED_TRANSITION_BLOCKS).then_some(blocks)
            }
            ChainEvent::Resync { .. } => None,
        };
        match fits {
            Some(blocks) => {
                self.blocks = blocks;
                self.events.push_back(event);
            }
            None => {
                let tip = self
                    .tip
                    .expect("a queued event ends on a published tip, so the queue has one");
                self.events.clear();
                self.blocks = 0;
                self.events.push_back(ChainEvent::Resync {
                    tip,
                    generation: self.generation,
                });
            }
        }
    }
}

/// The readers' handle: the generation, and the one subscription. Cheap to
/// clone.
#[derive(Clone, Debug)]
pub struct ChainTransitions {
    inner: Arc<Inner>,
}

impl ChainTransitions {
    /// The generation cell readers check around their lookups. See
    /// [`GenerationCell`] for the rule.
    pub fn generation(&self) -> GenerationCell {
        self.inner.generation.clone()
    }

    /// Become the one consumer. Returns where the stream starts and the
    /// stream. Dropping the stream ends the subscription, and another
    /// consumer can subscribe after that.
    pub fn subscribe(&self) -> Result<(ChainStart, ChainEvents), AlreadySubscribed> {
        let mut queue = self.inner.lock();
        if queue.subscribed {
            return Err(AlreadySubscribed);
        }
        queue.subscribed = true;
        queue.events.clear();
        queue.blocks = 0;
        self.inner.subscribed.store(true, Ordering::Release);
        let start = ChainStart {
            tip: queue.tip,
            generation: queue.generation,
        };
        Ok((
            start,
            ChainEvents {
                inner: self.inner.clone(),
            },
        ))
    }
}

/// The consumer's half of the queue.
#[derive(Debug)]
pub struct ChainEvents {
    inner: Arc<Inner>,
}

impl ChainEvents {
    /// The next event, or `None` once the sender is gone and every queued
    /// event is read.
    pub async fn next(&mut self) -> Option<ChainEvent> {
        loop {
            let notified = {
                let mut queue = self.inner.lock();
                if let Some(event) = queue.pop() {
                    return Some(event);
                }
                if queue.closed {
                    return None;
                }
                // A notify sent after the lock is released stores a permit, so
                // this wait cannot miss it.
                self.inner.notify.notified()
            };
            notified.await;
        }
    }

    /// The next event if one is queued.
    pub fn try_next(&mut self) -> Option<ChainEvent> {
        self.inner.lock().pop()
    }
}

impl Queue {
    /// Take the oldest event.
    fn pop(&mut self) -> Option<ChainEvent> {
        let event = self.events.pop_front()?;
        if let ChainEvent::Transition(transition) = &event {
            self.blocks = self.blocks.saturating_sub(transition.blocks());
        }
        Some(event)
    }
}

impl Drop for ChainEvents {
    fn drop(&mut self) {
        let mut queue = self.inner.lock();
        queue.subscribed = false;
        queue.events.clear();
        queue.blocks = 0;
        self.inner.subscribed.store(false, Ordering::Release);
    }
}
