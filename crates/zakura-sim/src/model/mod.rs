//! Independent role models (properties.md, Role models).
//!
//! Each model implements one role's rules from the regulation
//! specification, and labels each frame a simulated peer sends before it is
//! sent. The models never call the production regulation tools. The
//! `model_agreement` tests drive both with the same frames and require the
//! same answers, so a mutated tool disagrees with its model.
//!
//! A frame labelled with a violation changes no model state: the node closes
//! the connection for it. Models are plain values, so a peer that wants a
//! label without committing the frame labels a clone.
//!
//! Models use `BTreeMap`, never `zakura_rt`'s `HashMap`: a checker that made
//! the run's first `HashMap` would draw a hash seed between steps.

mod cadence;
mod consequence;
mod exchange;
mod frame;
mod label;
mod subscription;

pub use cadence::{CadenceModel, Charge};
pub use consequence::{consequence, is_fault_cause, Consequence, FAULT_CAUSES};
pub use exchange::{ExchangeModel, Owed};
pub use frame::{FrameHeader, FrameModel, ReaderEnd};
pub use label::{Direction, Label, LabelOutcome, LabelledFrame, Labels};
pub use subscription::{SubscriptionModel, Totals, Update};

/// A peer session, numbered by the harness.
pub type SessionId = u64;

/// An exchange's or subscription's key within its session.
pub type ExchangeKey = u64;

/// A subscription page's cursor.
pub type Cursor = u64;
