# Fork-aware header-store coherence harness

This harness replaces the model-based suite that targeted the removed
height-selected header overlay. It drives the durable fork-aware transition
writer and runs the production exhaustive audit plus an independent retained
DAG oracle after every operation.

The old operation names map to the current architecture as follows:

| Removed overlay operation | Fork-aware operation |
| --- | --- |
| `CommitHeaderRange` | sealed `prepare_headers` evidence followed by `TransitionEvent::InsertHeaders` |
| `CommitBody` | authenticated `TransitionEvent::VerifiedChainChanged` grow/reset evidence |
| `Seed` | the same authenticated verified-path transition; there is no second overlay seeding writer |
| `Finalize` | authenticated `TransitionEvent::FullStateFinalized` |
| `Reopen` | persistent `HeaderChainStore::startup` with zero repairs and byte-identical logical rows |

The fixed universe has a 60-header real-DAA trunk and four engineered forks.
One longer branch has less work than a shorter branch, another extension
reverses that ordering, and the final branch is nested. This prevents height
or insertion order from accidentally acting as fork choice.

After every operation the model checks:

- exact retained hashes, parents, heights, block work, and immutable-origin
  cumulative work;
- finality-relative suffix work and raw internal hash tie-breaking;
- selected and verified projections, verified body state, and finality
  pruning;
- durable metadata against the process publisher; and
- a clean `audit_store` result, with no reconstructible repair required.

Rejected range and full-state evidence must leave all twelve logical header
families and publication byte-identical. Separate read tests hand-corrupt
authoritative nodes and selected projections and require local incoherence
errors before those rows can become validation contexts, locators, or serving
successors.

Run the deterministic and default 64-case discovery suite with:

```text
cargo test -p zakura-state 'header_chain::coherence::' --lib
```

Increase discovery depth with `PROPTEST_CASES`; any shrunk failure should be
transcribed into a named deterministic regression in `coherence.rs`.
