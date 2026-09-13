# Serving ownership properties

A serving request occupies capacity until its last worker, queued result or
unfinished write is gone. Cancelling the caller must not let a replacement
request start while the old operation still owns that capacity.

For example, A asks us for three blocks and we start a storage read. A then
reconnects while that read is still running. The new session must wait for the
old read to finish and release its result. Otherwise reconnecting would bypass
the per-peer worker limit. These tests hold the real operations open, cancel
their callers and require replacement work to recover after release.

## Shared models

`regulation::request::properties` checks finite requests with a bounded response.
Its two checkers compare production admission with independent expected owners
and a first-in, first-out waiter list. Both GetBlocks and a test GetPeers policy
run the same checkers. Adding another finite request needs a policy and a legal
frame, not another copy of the model. The GetPeers adapter tests reuse of this
contract. It does not change production discovery handling.

`serving_regulation::properties` adds GetBlocks' real encoder and output writer.
The model predicts ownership without reading production counters. Its adapter
performs the same actions on production code, then compares outcomes and live
capacity. A lease is a shared handle that keeps a request's slot reserved.
Cloning it must not start another job, and dropping one clone must not free a slot
still owned elsewhere. A deliberately broken writer proves that the comparison
catches releasing capacity before a write finishes.

## Real operations

The serving task tests live under `serving::tests::serving_contract`.
`fixtures` supplies stored bytes and controlled blocking reads. `ownership`
holds jobs and writes open. `ranges` checks response prefixes and endings.
`failures` checks local error cleanup. The encoder probe is test-only and runs
inside the real blocking encode, with the production lease still attached.

| Requirement | Property coverage |
| --- | --- |
| C01 | Actual operation starts stay within worker capacity when more requests are waiting. |
| C02 | Retained output and unfinished writes stop read-ahead and resume after release. |
| C03 | Same-identity reconnects share peer limits. Identity churn still shares node limits. |
| C04 | Running encodes and completed results retain capacity after caller cancellation. |
| C05 serving | Storage allocations, decoded objects, encoder peaks and retained frames stay within the tested bounds. |
| C06 serving | Readiness, read, encoding and output failures clean up correctly without inventing endings or blaming the peer. |
| C07 | Generated counts from 1 to 128, fixed maximum ranges, byte caps, gaps, exact fits and changed waiting limits yield the right prefix and ending. |
| R02 serving | A held ending prevents another serving admission for overlapping and disjoint ranges. |

Generated request bursts also compare every block hash and ending while varying
output depth and cancellation. The fixed cancellation counterexample waits for
all surviving encode and write owners before expecting capacity to be free.
State properties independently check contiguous reads, byte limits, height
overflow and cancellation between storage lookups.

These tests cover serving ownership. Receiver authorization, aggregate memory
and load, and transport-wide progress need their own tests and changes. In
particular, C05 and C06 here do not claim those other layers are complete.

## Local execution and replay

```sh
PROPTEST_CASES=2048 PROPTEST_RNG_SEED=896 cargo nextest run --locked \
  -p zakura-network -p zakura-state --lib --profile serving-ownership
```

This local profile includes the relevant existing fixed regressions, finite
timeouts and no retries. No workflow trigger is added. Generated replay failures
print concrete JSON actions. The committed reconnect scenario and Proptest
regression seed remain part of the normal run.

To replay a saved ownership history:

```sh
ZAKURA_REGULATION_REPLAY=/absolute/path/scenario.json cargo test --locked \
  -p zakura-network replay_preserves_writing_ownership_across_session_replacement
```
