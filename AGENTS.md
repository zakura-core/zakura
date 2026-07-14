# Zakura — Agent Guidelines

> This file is read by AI coding agents (Claude Code, GitHub Copilot, Cursor, Devin, etc.).
> It provides project context and contribution policies.

# Working in This Repository

- Keep changes focused and avoid unrelated refactors.
- Run formatting and the checks relevant to the code you changed.
- For fixes, explain the root cause, how the solution addresses it, and how the tests exercise the affected behavior.

## Project Structure & Module Organization

Zakura is a Rust workspace. Main crates include:

- `zakurad/` (node CLI/orchestration),
- core libraries like `zakura-chain/`, `zakura-consensus/`, `zakura-network/`, `zakura-state/`, `zakura-rpc/`,
- support crates like `zakura-node-services/`, `zakura-test/`, `zakura-utils/`, `tower-batch-control/`, and `tower-fallback/`.

Code is primarily in each crate's `src/`; integration tests are in `*/tests/`; many unit/property tests are colocated in `src/**/tests/` (for example `prop.rs`, `vectors.rs`, `preallocate.rs`). Documentation is in `book/` and `docs/decisions/`. CI and policy automation live in `.github/workflows/`.

## Build, Test, and Development Commands

Choose checks that are proportional to the change. Workspace-wide commands are useful for broad or cross-crate changes:

```bash
# Optional full build check
cargo build --workspace --locked

# Formatting, linting, and workspace tests
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace

# Run a single crate's tests
cargo test -p zakura-chain
cargo test -p zakura-state

# Run a single test by name
cargo test -p zakura-chain -- test_name

# CI-like nextest profile for broad coverage
cargo nextest run --profile all-tests --locked --release --features default-release-binaries --run-ignored=all

# Run with nextest (integration profiles)
cargo nextest run --profile sync-large-checkpoints-empty
```

## Commit & Pull Request Guidelines

- PR titles must follow [conventional commits](https://www.conventionalcommits.org/en/v1.0.0/#specification) (PRs are squash-merged — the PR title becomes the commit message)
- Use `.github/pull_request_template.md`. For fixes, connect the root cause to both the solution and the test coverage.
- For user-visible changes, update `CHANGELOG.md` per `CHANGELOG_GUIDELINES.md`.

## Project Overview

Zakura is a Zcash full node implementation in Rust. It is a validator node — it excludes features not strictly needed for block validation and chain sync.

- **Rust edition**: 2021
- **MSRV**: 1.91 (unified across the library crates and the zakurad binary)
- **Database format version**: defined in `zakura-state/src/constants.rs`

## Crate Architecture

```text
zakurad (CLI orchestration)
  ├── zakura-consensus (block/transaction verification)
  │     └── zakura-script (script validation via FFI)
  ├── zakura-state (finalized + non-finalized storage)
  ├── zakura-network (P2P, peer management)
  └── zakura-rpc (JSON-RPC + gRPC)
        └── zakura-node-services (service trait aliases)
              └── zakura-chain (core data types, no async)
```

**Dependency rules**:

- Dependencies flow **downward only** — lower crates must not depend on higher ones
- `zakura-chain` is **sync-only**: no async, no tokio, no Tower services
- `zakura-node-services` defines service trait aliases used across crates
- `zakurad` orchestrates all components but contains minimal logic
- Utility crates: `tower-batch-control`, `tower-fallback`, `zakura-test`

### Per-Crate Concerns

| Crate | Key Concerns |
| --- | --- |
| `zakura-chain` | Serialization correctness, no async, consensus-critical data structures |
| `zakura-network` | Protocol correctness, peer handling, rate limiting, DoS resistance |
| `zakura-consensus` | Verification completeness, error handling, checkpoint vs semantic paths |
| `zakura-state` | Read/write separation (`ReadRequest` vs `Request`), database migrations |
| `zakura-rpc` | zcashd compatibility, error responses, timeout handling |
| `zakura-script` | FFI safety, memory management, lifetime/ownership across boundaries |

## Coding Style & Naming Conventions

- Rust 2021 conventions and `rustfmt` defaults apply across the workspace (4-space indentation).
- Naming: `snake_case` for functions/modules/files, `CamelCase` for types/traits, `SCREAMING_SNAKE_CASE` for constants.
- Respect workspace lint policy in `.cargo/config.toml` and crate-specific lint config in `clippy.toml`.
- Keep dependencies flowing downward across crates; maintain `zakura-chain` as sync-only.

## Code Patterns

### Tower Services

All services must include these bounds:

```rust
S: Service<Req, Response = Resp, Error = BoxError> + Send + Clone + 'static,
S::Future: Send + 'static,
```

- `poll_ready` must check all inner services
- Clone services before moving into async blocks

### Error Handling

- Use `thiserror` with `#[from]` / `#[source]` for error chaining
- `expect()` messages must explain **why** the invariant holds, not what happens if it fails:

  ```rust
  .expect("block hash exists because we just inserted it")  // good
  .expect("failed to get block")                            // bad
  ```

- Don't turn invariant violations into misleading `None`/default values

### Numeric Safety

- External/untrusted values: use `saturating_*` / `checked_*` arithmetic
- All `as` casts must have a comment explaining why the cast is safe

### Async & Concurrency

- CPU-heavy work (crypto, proofs): use `tokio::task::spawn_blocking`
- All external waits need timeouts (network, state, channels)
- Prefer `tokio::sync::watch` over `Mutex` for shared async state
- Prefer freshness tracking ("time since last change") to detect stalls

### Security

- Use `TrustedPreallocate` for deserializing collections from untrusted sources
- Bound all loops/allocations over attacker-controlled data
- Validate at system boundaries (network, RPC, disk)

### Performance

- Prefer existing indexed structures (maps/sets) over scanning/iterating
- Avoid unnecessary clones — structs may grow in size over time
- Use `impl Into<T>` to reduce verbose `.into()` at call sites
- Don't add unnecessary comments, docstrings, or type annotations to code you didn't change

## Testing Guidelines

- Unit/property tests: `src/*/tests/` within each crate (`prop.rs`, `vectors.rs`, `preallocate.rs`)
- Integration tests: `crate/tests/` (standard Rust layout)
- Async tests: `#[tokio::test]` with timeouts for long-running tests
- Test configs must match real network parameters (don't rely on defaults)

```bash
# Unit tests
cargo test --workspace

# Integration tests with nextest
cargo nextest run --profile sync-large-checkpoints-empty
```

## Metrics & Observability

- Metrics use dot-separated hierarchical names with existing prefixes: `checkpoint.*`, `state.*`, `sync.*`, `rpc.*`, `peer.*`, `zcash.chain.*`
- Use `#[instrument(skip(large_arg))]` for tracing spans on important operations
- Errors must be logged with context

## Changelog

- Update `CHANGELOG.md` under `[Unreleased]` for user-visible changes
- Update crate `CHANGELOG.md` for library-consumer-visible changes
- Apply the appropriate PR label (`C-feature`, `C-bug`, `C-security`, etc.)
- See `CHANGELOG_GUIDELINES.md` for detailed formatting rules

## Configuration

```rust
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// Documentation for field
    pub field: Type,
}
```

- Use `#[serde(deny_unknown_fields)]` for strict validation
- Use `#[serde(default)]` for backward compatibility
- All fields must have documentation
- Defaults must be sensible for production
