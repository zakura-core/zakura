//! Deterministic simulation of Zakura's P2P stack.
//!
//! `zakura-sim` drives `zakura-scheduler` one step at a time. Each step
//! computes the actions enabled now, lets the `Plan` pick one, runs it, and
//! records what happened. Every choice comes from the `Plan`, so the same
//! `Plan` replays the same run.
//!
//! The crate is empty outside the simulator build,
//! `RUSTFLAGS="--cfg zakura_sim"`. The design is in the `sim.md` plan and its
//! M1 implementation plan.
#![cfg(zakura_sim)]

pub mod action;
pub mod check;
mod draws;
pub mod end;
pub mod fake;
pub mod identity;
pub mod observe;
pub mod plan;
pub mod record;
pub mod sim;
pub mod trace;

pub use action::{Action, ActionSource, Boundary, Enabled, SourceId};
pub use check::{Checker, NoChecks, PropertyOutcome, PropertyReport, StepCtx, Violation};
pub use end::{HarnessFailure, IdleStatus, Outcome, RunEnd};
pub use identity::{Category, Identity};
pub use observe::{Observation, ObservationLog};
pub use plan::{By, HexU64, Plan, PlanChoice, PlanError, PlanInput};
pub use record::{MemorySink, Record, RecordSink};
pub use sim::{RunConfig, RunReport, Sim};
