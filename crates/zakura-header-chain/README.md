# Zakura header-chain policy

`zakura-header-chain` keeps competing Zcash header forks coherent while
validation, body verification, checkpoint updates, and operator actions finish
at different times. It is a synchronous policy layer: authenticated evidence
goes in, and an invariant-checked atomic transition plan comes out.

The crate owns the retained header graph, deterministic fork choice, finality,
retention, and the projections exposed to readers. It does not own networking,
asynchronous scheduling, durable storage, full-block verification, or
publication; higher-level crates authenticate external work and perform those
effects.

## Start here

- [Header validation](src/validation/README.md) explains context-free
  preparation, retained-branch contextual admission, and which checks still
  require a full block.
- [Atomic header-chain transitions](src/transition/README.md) explains the
  engine model, freshness and replay, fork choice, finality, retention, atomic
  commit contract, failure semantics, and startup recovery.

Those documents are the detailed architecture references. The essential model
at the crate boundary is:

1. Prepare header evidence or authenticate another transition event.
2. Plan against one coherent engine snapshot.
3. Atomically commit the complete write set.
4. Install that committed transition into the in-memory engine.
5. Publish the resulting snapshot.

Planning is not a commit. Callers must not publish a planned state or partially
apply its write set.

## Integration boundary

The selected projection is the strongest eligible header path. The verified
projection is the contiguous full-state-accepted path in integrated mode; it
can legitimately differ from the selected path. Both remain anchored by
finality.

The crate is intentionally not connected to the node runtime yet. Follow-up
changes will add the durable `zakura-state` adapter, the asynchronous
`zakura-node-services` port, consensus-validator reuse, and `zakurad`
orchestration.

Engine hydration requires an audited durable state. Integrations should use the
state-backed boundary rather than treating the in-memory graph store as a
public mutation API.

## Development

Run the focused test suite from the workspace root:

```console
cargo test -p zakura-header-chain
```

The `fuzz-impl` feature exposes the deterministic replay adapter used by the
follow-up header-chain fuzz target.
