//! block_sync/pipe.rs — **start here** to read the Zakura block-sync subsystem.
//!
//! Block sync downloads block *bodies* over QUIC stream 6 for the heights header
//! sync has already committed, and serves those same bodies back to other peers.
//! After per-peer routines the shape is "the pipe IS the routine": each connected peer is driven
//! by one task that owns its transport read and runs the download logic inline —
//! there is no central scheduler and no reactor inbound demux.
//!
//! # Where the code lives (reading order)
//!
//! 1. [`peer_routine`](super::peer_routine) — **the core.** One per-peer routine
//!    owns its `FramedRecv`, decodes each stream-6 frame, and runs the want-work
//!    fill loop plus the inbound body/terminator handling in the same task. The
//!    download decision gates on exactly two things — the global in-flight byte
//!    budget and the peer's adaptive outbound request window (its slots) — never
//!    on how far ahead of the committed tip the fetch already is.
//! 2. [`work_queue`](super::work_queue) — the shared, sorted set of needed heights
//!    routines pull contiguous chunks from. Dedup / in-flight tracking lives here;
//!    the committed floor is garbage-collection only, never a fetch throttle.
//! 3. [`sequencer_task`](super::sequencer_task) — the single commit pipeline.
//!    Routines forward matched bodies to it; it orders them above the verified
//!    tip, submits applies, and publishes the committed `SequencerView` watch the
//!    routines read for the floor and for reset.
//! 4. [`peer_registry`](super::peer_registry) — the cross-peer facts the routines
//!    *write* (servable range, caps, outstanding heights, misbehavior) and the
//!    reactor *reads* (candidate / producer / serving). Generation-gated so a
//!    superseded routine cannot clobber a live entry.
//! 5. [`reactor`](super::reactor) — the shared-concerns hub: serving inbound
//!    `GetBlocks`, status advertisement, the needed-heights producer, and peer
//!    lifecycle. Routines reach it over `RoutineToReactor` for those concerns
//!    only; everything per-peer stays in the routine.
//!
//! # Inbound data flow (this file's pipe)
//!
//! A peer's routine applies the shared ingress [`block_sync_guard`], decodes the
//! frame, then branches on the decoded message — all inline, no demux:
//!
//! ```text
//!  recv ─▶ guard ─▶ decode ─▶ branch(msg)
//!                             ├─ Status           ─▶ local servable/caps + advertise (reactor)
//!                             ├─ GetBlocks        ─▶ serve (reactor)
//!                             ├─ Block            ─▶ local match + Sequencer accept
//!                             ├─ BlocksDone       ─▶ local finish/retry
//!                             └─ RangeUnavailable ─▶ local retry
//! ```
//!
//! A disallowed/unknown stream-6 type or a malformed payload surfaces as a
//! `MalformedMessage` misbehavior + a protocol reject from the routine's
//! decode-error path, rather than a pre-decode guard reject silently dropping the
//! signal (see BS1).

use crate::zakura::SessionGuard;

use super::wire::MAX_BS_MESSAGE_BYTES;

pub(super) fn block_sync_guard() -> SessionGuard {
    // The transport already applies the per-connection count bucket and frame
    // cap; this guard adds the same payload cap the codec enforces. Type
    // validity is left to the decode stage on purpose: a disallowed or unknown
    // stream-6 type must surface as a `MalformedMessage` misbehavior + protocol
    // reject from the routine's decode-error path (see BS1), rather than a
    // pre-decode guard reject dropping that signal. The block-sync byte budget
    // likewise stays in the routine's reserve/reorder accounting so existing
    // request/retry accounting is not double-counted.
    SessionGuard::oversize_only(MAX_BS_MESSAGE_BYTES as u32)
}
