//! `block_gossip` rows: a block broadcast's path through the shared upload slots.
//!
//! One broadcast writes `broadcast_start`, then for each peer, in order:
//!
//! - `advert_start`: the per-peer task started. `wait_us` is the time since `broadcast_start`
//!   spent waiting for a place among the concurrently polled tasks.
//! - `advert_join`: the peer already had an upload of this block in progress, so this task waits
//!   for that upload instead of taking a slot.
//! - `slot_acquired`: the task took an upload slot. `wait_us` is the time spent waiting for one.
//! - `inv_enqueued`: the connection channel accepted the inventory request. `wait_us` is the
//!   time since `slot_acquired`.
//! - `inv_written`: the connection wrote the inventory. `wait_us` is the time since
//!   `inv_enqueued`, which covers requests queued ahead of it and the connection's current
//!   request.
//! - `slot_released`: the slot was returned. `release_reason` says why, `held_us` is the time
//!   since `slot_acquired`, and `requested_us` and `written_us` are the peer's `getdata` and the
//!   completed block write, relative to `slot_acquired`.
//! - `advert_done`: the task finished. `elapsed_us` is the time since `advert_start`.
//!
//! and finally `broadcast_done`, with the number of peers whose advertisement succeeded.
//!
//! Slot wait (`slot_acquired.wait_us`) and connection queue wait (`inv_enqueued.wait_us` plus
//! `inv_written.wait_us`) are reported separately, so a slow advertisement can be attributed to
//! older broadcasts holding slots or to a busy connection.

use std::sync::Arc;

use serde::Serialize;
use tokio::time::Instant;
use zakura_chain::block;
use zakura_jsonl_trace::{
    impl_jsonl_trace_event, saturating_count, saturating_micros, tables, JsonlDisplay,
    JsonlEventEmitter,
};

use super::{ConnectionTrace, LifecycleTrace};

/// Why a block advertisement returned its upload slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SlotRelease {
    /// The block was written, and the drain allowance after the write elapsed.
    WriteGrace,
    /// The peer did not request the block before the request deadline.
    NoRequest,
    /// The peer requested the block, but the upload did not finish before its deadline.
    UploadTimeout,
    /// The upload ended without writing the block, or the connection closed.
    UploadFailed,
    /// The connection did not write the inventory before the inventory deadline.
    InventoryNotWritten,
    /// The advertisement task was dropped while it held the slot.
    Cancelled,
}

impl SlotRelease {
    /// Returns the `release_reason` trace value, also used as a metric label.
    pub(crate) fn label(self) -> &'static str {
        match self {
            SlotRelease::WriteGrace => "write_grace",
            SlotRelease::NoRequest => "no_request",
            SlotRelease::UploadTimeout => "upload_timeout",
            SlotRelease::UploadFailed => "upload_failed",
            SlotRelease::InventoryNotWritten => "inv_not_written",
            SlotRelease::Cancelled => "cancelled",
        }
    }
}

/// One block broadcast's trace context. Empty when the tracer does not write `block_gossip`.
#[derive(Clone, Debug, Default)]
pub(crate) struct BroadcastTrace(Option<Arc<Broadcast>>);

#[derive(Debug)]
struct Broadcast {
    emitter: JsonlEventEmitter,
    id: u64,
    hash: block::Hash,
    started: Instant,
}

/// One peer's advertisement within a broadcast. Empty when the broadcast is untraced.
///
/// Dropping a started advertisement records any slot it still holds as
/// [`SlotRelease::Cancelled`] and finishes it as unsuccessful.
#[derive(Debug, Default)]
pub(crate) struct AdvertTrace(Option<Box<Advert>>);

#[derive(Debug)]
struct Advert {
    broadcast: Arc<Broadcast>,
    conn: u64,
    peer: Option<Arc<str>>,
    position: usize,
    started: Instant,
    slot_wait_started: Option<Instant>,
    slot_acquired: Option<Instant>,
    inv_enqueued: Option<Instant>,
    upload_requested: Option<Instant>,
    upload_written: Option<Instant>,
    slot_released: bool,
    finished: bool,
}

#[derive(Default, Serialize)]
struct GossipRow<'a> {
    event: &'static str,
    broadcast_id: u64,
    hash: Option<JsonlDisplay<'a, block::Hash>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    conn: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    peer: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    position: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    peers: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    concurrency: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    available_slots: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    wait_us: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    held_us: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    requested_us: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    written_us: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    elapsed_us: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    release_reason: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    succeeded: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    succeeded_peers: Option<u64>,
}

impl_jsonl_trace_event!(GossipRow<'_>, tables::BLOCK_GOSSIP);

fn micros_between(from: Instant, to: Instant) -> u64 {
    saturating_micros(to.saturating_duration_since(from))
}

impl LifecycleTrace {
    /// Starts tracing a broadcast of `hash` to `peers` peers.
    ///
    /// `mined` marks a locally mined block. `concurrency` is the configured number of shared
    /// upload slots, and `available_slots` is how many were free when the broadcast started.
    pub(crate) fn block_broadcast(
        &self,
        hash: block::Hash,
        mined: bool,
        peers: usize,
        concurrency: usize,
        available_slots: usize,
    ) -> BroadcastTrace {
        let Some(emitter) = self.emitter.as_ref().filter(|emitter| {
            emitter
                .tracer()
                .is_table_enabled(tables::BLOCK_GOSSIP.table())
        }) else {
            return BroadcastTrace::default();
        };

        let broadcast = Broadcast {
            emitter: emitter.clone(),
            id: self
                .next_broadcast_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            hash,
            started: Instant::now(),
        };
        broadcast.emitter.emit_event(|| GossipRow {
            reason: Some(if mined { "mined" } else { "relay" }),
            peers: Some(saturating_count(peers)),
            concurrency: Some(saturating_count(concurrency)),
            available_slots: Some(saturating_count(available_slots)),
            ..broadcast.row("broadcast_start")
        });

        BroadcastTrace(Some(Arc::new(broadcast)))
    }
}

impl Broadcast {
    fn row(&self, event: &'static str) -> GossipRow<'_> {
        GossipRow {
            event,
            broadcast_id: self.id,
            hash: Some(JsonlDisplay(&self.hash)),
            ..GossipRow::default()
        }
    }
}

impl BroadcastTrace {
    /// Creates the trace for the advertisement at `position` in the broadcast's peer order, sent
    /// on the connection traced by `conn`.
    pub(crate) fn advert(&self, position: usize, conn: &ConnectionTrace) -> AdvertTrace {
        AdvertTrace(self.0.as_ref().map(|broadcast| {
            Box::new(Advert {
                broadcast: broadcast.clone(),
                conn: conn.id(),
                peer: conn.gossip().map(|state| state.peer.clone()),
                position,
                started: broadcast.started,
                slot_wait_started: None,
                slot_acquired: None,
                inv_enqueued: None,
                upload_requested: None,
                upload_written: None,
                slot_released: false,
                finished: false,
            })
        }))
    }

    /// Records that every advertisement finished, `succeeded_peers` of them successfully.
    pub(crate) fn finished(&self, succeeded_peers: usize) {
        let Some(broadcast) = &self.0 else {
            return;
        };

        let elapsed_us = saturating_micros(broadcast.started.elapsed());
        broadcast.emitter.emit_event(|| GossipRow {
            elapsed_us: Some(elapsed_us),
            succeeded: Some(succeeded_peers > 0),
            succeeded_peers: Some(saturating_count(succeeded_peers)),
            ..broadcast.row("broadcast_done")
        });
    }
}

impl Advert {
    fn emit(&self, event: &'static str, build: impl FnOnce(GossipRow<'_>) -> GossipRow<'_>) {
        self.broadcast.emitter.emit_event(|| {
            build(GossipRow {
                conn: Some(self.conn),
                peer: self.peer.as_deref(),
                position: Some(saturating_count(self.position)),
                ..self.broadcast.row(event)
            })
        });
    }
}

impl AdvertTrace {
    /// Records that the advertisement task started.
    pub(crate) fn started(&mut self) {
        let Some(advert) = &mut self.0 else {
            return;
        };

        let now = Instant::now();
        let wait_us = micros_between(advert.started, now);
        advert.started = now;
        advert.emit("advert_start", |row| GossipRow {
            wait_us: Some(wait_us),
            ..row
        });
    }

    /// Records that the advertisement joined an upload already in progress on its connection.
    pub(crate) fn joined(&mut self) {
        if let Some(advert) = &self.0 {
            advert.emit("advert_join", |row| row);
        }
    }

    /// Records that the advertisement started waiting for an upload slot.
    pub(crate) fn waiting_for_slot(&mut self) {
        if let Some(advert) = &mut self.0 {
            advert.slot_wait_started = Some(Instant::now());
        }
    }

    /// Records that the advertisement acquired an upload slot, leaving `available_slots` free.
    pub(crate) fn slot_acquired(&mut self, available_slots: usize) {
        let Some(advert) = &mut self.0 else {
            return;
        };

        let now = Instant::now();
        let wait_us = micros_between(advert.slot_wait_started.unwrap_or(now), now);
        advert.slot_acquired = Some(now);
        advert.emit("slot_acquired", |row| GossipRow {
            wait_us: Some(wait_us),
            available_slots: Some(saturating_count(available_slots)),
            ..row
        });
    }

    /// Records that the connection channel accepted the inventory request.
    pub(crate) fn inv_enqueued(&mut self) {
        let Some(advert) = &mut self.0 else {
            return;
        };

        let now = Instant::now();
        let wait_us = micros_between(advert.slot_acquired.unwrap_or(now), now);
        advert.inv_enqueued = Some(now);
        advert.emit("inv_enqueued", |row| GossipRow {
            wait_us: Some(wait_us),
            ..row
        });
    }

    /// Records that the connection wrote the inventory.
    pub(crate) fn inv_written(&mut self) {
        let Some(advert) = &self.0 else {
            return;
        };

        let now = Instant::now();
        let wait_us = micros_between(advert.inv_enqueued.unwrap_or(now), now);
        advert.emit("inv_written", |row| GossipRow {
            wait_us: Some(wait_us),
            ..row
        });
    }

    /// Records the time the peer requested the block.
    pub(crate) fn upload_requested(&mut self, at: Instant) {
        if let Some(advert) = &mut self.0 {
            advert.upload_requested.get_or_insert(at);
        }
    }

    /// Records the time the connection finished writing the block.
    pub(crate) fn upload_written(&mut self, at: Instant) {
        if let Some(advert) = &mut self.0 {
            advert.upload_written.get_or_insert(at);
        }
    }

    /// Records that the advertisement returned its upload slot because of `release`.
    pub(crate) fn slot_released(&mut self, release: SlotRelease) {
        let Some(advert) = &mut self.0 else {
            return;
        };
        let Some(acquired) = advert.slot_acquired.filter(|_| !advert.slot_released) else {
            return;
        };

        advert.slot_released = true;
        let held_us = saturating_micros(acquired.elapsed());
        let relative = |at: Option<Instant>| at.map(|at| micros_between(acquired, at));
        let (requested_us, written_us) = (
            relative(advert.upload_requested),
            relative(advert.upload_written),
        );
        advert.emit("slot_released", |row| GossipRow {
            held_us: Some(held_us),
            requested_us,
            written_us,
            release_reason: Some(release.label()),
            ..row
        });
    }

    /// Records that the advertisement task finished.
    pub(crate) fn finished(&mut self, succeeded: bool) {
        let Some(advert) = &mut self.0 else {
            return;
        };
        if advert.finished {
            return;
        }

        advert.finished = true;
        let elapsed_us = saturating_micros(advert.started.elapsed());
        advert.emit("advert_done", |row| GossipRow {
            elapsed_us: Some(elapsed_us),
            succeeded: Some(succeeded),
            ..row
        });
    }
}

impl Drop for AdvertTrace {
    fn drop(&mut self) {
        if self.0.is_some() {
            self.slot_released(SlotRelease::Cancelled);
            self.finished(false);
        }
    }
}
