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

pub mod identity;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the step loop, the decider's only user, lands in the next PR"
    )
)]
pub mod plan;

pub use identity::{Category, Identity};
pub use plan::{HexU64, Plan, PlanChoice, PlanError, PlanInput};
