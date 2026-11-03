---
name: core-rust-readability
description: >-
  Split overloaded core Rust modules into a thin facade with private children.
  For key Rust crates (zakura-chain, zakura-consensus, zakura-header-chain,
  zakura-network, zakura-rpc, zakura-script, zakura-state), confirm with the
  user during planning before applying; do not auto-apply mid-implementation.
  Also use when the user asks for readability, module decomposition, or
  facade-package refactors in those crates.
---

# Core Rust Module Packages

The readability checks in `AGENTS.md` apply to all code. This skill adds module
decomposition and Rust-specific checks for the key crates in the description.

Confirm application with the user during planning instead of auto-applying. If
you are already implementing the plan, do not auto-apply and do not interrupt
the plan.

## Module Packages

Do not grow overloaded multi-responsibility files. When a module mixes a
high-level API with lower-level helpers or phases, keep a thin facade and move
cohesive clusters into private children.

- The facade shows entry points and phase order. Children hide mechanism
  (`pub(super)` / package-private).
- Split by responsibility or phase (admit → apply → settle; load → audit →
  derive → classify), not by line count.
- Leaves and DTOs sit below planners, recovery, and engines. Do not park shared
  durable types under a single consumer.
- Group tests by behavior with shared fixtures. Do not add source-layout guards
  that break when files move.
- A pure module move keeps the external API. Visibility tightening is a separate
  change.

Reference: `crates/zakura-header-chain/src/transition/{planner,types,recovery}`.

## Rust Checks

- No `#[cfg(test)]` fields, return values, or branches sit in production data
  paths.
- Distinct states have distinct error variants.
- Several arguments that form one concept become a parameter struct.
