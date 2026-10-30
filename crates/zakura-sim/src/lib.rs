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
pub mod domain;
mod downstream;
mod draws;
pub mod end;
mod evidence;
pub mod explore;
pub mod fake;
pub mod harness;
pub mod identity;
pub mod model;
pub mod net;
pub mod observe;
pub mod peer;
pub mod plan;
pub mod property;
pub mod record;
pub mod seed;
mod sender;
pub mod sim;
pub mod storage;
pub mod suite;
pub mod table;
pub mod trace;
pub mod witness;

pub use action::{Action, ActionSource, Boundary, Enabled, SourceId};
pub use check::{Checker, NoChecks, PropertyOutcome, PropertyReport, StepCtx, Violation};
pub use end::{HarnessFailure, IdleStatus, Outcome, RunEnd};
pub use identity::{Category, Identity};
pub use observe::{Observation, ObservationLog};
pub use plan::{
    By, Candidate, Divergence, HexU64, Plan, PlanChoice, PlanError, PlanInput, SkipScript,
    TraceScript,
};
pub use record::{MemorySink, NullSink, Record, RecordSink};
pub use sim::{RunConfig, RunReport, Sim, TaskHold};
