#![allow(dead_code, unused_imports)] // activated by the first reactor adoption

//! Shared tools that regulate native Zakura message exchanges.
//!
//! - [`Serve`] admits requests without waiting, bounds execution and output,
//!   and ends every admitted request exactly once.
//! - [`Reservations`] admit a response only if this node requested it.
//! - [`CadenceBuckets`] and [`CadenceSender`] enforce and obey the rate that
//!   a row declares.
//! - [`SessionCapacity`] bounds a service's sessions from reservation through
//!   the last owner.
//! - [`SessionTable`] holds each peer's current session, and its
//!   [`WriterFence`] closes the connection rather than orphan a started
//!   exchange.
//! - [`Subscriptions`] and [`Publications`] hold the two sides of a
//!   subscription: renewable credit, ordered updates, the cursor history,
//!   and tombstones for crossed updates.
//! - [`sizing`] derives every capacity default from the throughput target.
//!
//! Every tool acts against a peer only on an unambiguous violation: an event
//! that no conformant peer could cause. Anything a conformant peer could cause
//! is traced instead. Local capacity limits make this node wait; they never
//! fault a peer.
//!
//! # Owner dispositions
//!
//! Each runtime owner has one current disposition. "Runtime" means the owner's
//! destructor or wait has effects, so pure machines refer to it only through
//! values the owner hands them.
//!
//! | Owner | Resource | Disposition |
//! | --- | --- | --- |
//! | [`Serve`], [`ServeCapacity`] | Commitments, execution slots, output bytes and responses | Runtime owner. Block sync and discovery serve through it. |
//! | [`Push`], [`PushPermit`] | The serving budgets, for pushed frames | Runtime owner. Block sync and header sync push through it. |
//! | [`WorkLease`], [`ExecutionSlots`] | Worker execution, through a blocking task's end | Runtime owner. Cancelling the waiter never releases a lease. |
//! | [`Reservations`], [`ReservationPool`] | Requested responses | Runtime owner shared by a reader's precheck and its reactor. |
//! | [`CadenceBuckets`] | Inbound cadence | Pure ledger, charged at explicit instants under the connection's mutex. |
//! | [`CadenceSender`] | One unwritten frame per row | Runtime owner. Its hold decision is a pure function of its inputs. |
//! | [`SessionCapacity`] | Direction and setup slots, through the last owner | Runtime owner. `service_session::state` never holds its reservation. |
//! | [`SessionTable`] | Each peer's current session | Value-only map, held under its reactor's lock. |
//! | [`WriterFence`] | Started exchanges | Runtime owner. It closes the connection rather than orphan one. |
//! | [`Subscriptions`] | Renewable credit and ordered updates | Value state, except each table's tap. |
//! | [`Publications`] | Live and closing slots, cursor history, tombstones | Mixed. Header subscriber work moves its permits out in delivery slot 2. |
//! | [`TerminalPermit`] | The reserved terminal queue access of one subscription | Runtime owner, held through the terminal's write. |
//! | [`SlotBudget`], [`OutputByteBudget`] | Slots and bytes | Primitive owners. A permit or grant moves with the resource it funds. |
//!
//! The `request` module is gone. An audit before its removal found no caller
//! outside its own tests, and [`Serve`] carries each behavior it tested: peer
//! capacity before node capacity, a shared peer pool across reconnects, a
//! response cap that covers every frame, and a guard held through the write.

mod cadence;
pub(crate) use cadence::{CadenceBuckets, CadenceCharge, CadenceSendError, CadenceSender, RowSend};

mod serve;
pub(crate) use serve::{
    Completion, CompletionId, Completions, ExecutionSlots, OutputOrder, Produce, Push, PushPermit,
    PushSend, Responded, ResponseCap, ResponseSink, ResponsesWritten, Serve, ServeCapacity,
    ServeConfigError, ServeEnd, ServeLimits, ServeViolation, SinkError, SinkProgress, WorkLease,
};
#[cfg(any(test, zakura_sim))]
pub(crate) use serve::{ReleaseReport, ServeStep};

mod reservations;
pub(crate) use reservations::{
    shrink_after_peak, ClaimRefused, Claimed, Ended, PoolEntry, PrecheckByRequest, PrecheckSlot,
    ReservationPool, Reservations, ReserveRefused, ResponsePrecheck, SharedReservations,
};

mod session_capacity;
pub(crate) use session_capacity::SessionCapacity;

mod session_table;
pub(crate) use session_table::{Current, Replacement, SessionKey, SessionTable};

pub(crate) mod sizing;

mod subscription;
pub(crate) use subscription::{
    within_window, Applied, CreditExceeded, PageStall, Publications, ResponseCredit,
    SharedSubscriptions, SubscribeRefused, SubscriptionEnded, SubscriptionFault,
    SubscriptionLimits, Subscriptions, TerminalPermit, Totals, Update,
};

mod slots;
pub(crate) use slots::{OutputByteBudget, OutputGrant, SlotBudget, SlotPermit};

mod writer_fence;
pub(crate) use writer_fence::{
    Exchange, ExchangeWriter, FencedSendError, WriterFence, UNFINISHED_EXCHANGE,
};

#[cfg(any(test, zakura_sim, feature = "p2p-fuzz"))]
pub(crate) mod test_family;
#[cfg(test)]
mod tests;

#[cfg(any(test, feature = "p2p-fuzz"))]
pub use cadence::{replay_cadence_bytes, CadenceReplaySummary};
