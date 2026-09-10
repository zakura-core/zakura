# GetBlocks audit stack

This stack replaces the combined review surface of
[PR #892](https://github.com/zakura-core/zakura/pull/892). Keep that draft open as
the reference until the replacement stack has been audited.

## Audit order

The storage PR targets `main`; each later PR targets its predecessor.
Review the incremental diffs in this order; the final draft also needs the integration review across all layers.

| PR | Boundary | Main audit question |
| --- | --- | --- |
| [#942](https://github.com/zakura-core/zakura/pull/942) | Owned storage reads | Do running jobs and undelivered results retain their charged resources? |
| [#943](https://github.com/zakura-core/zakura/pull/943) | Generic transport and resource ownership | Are complete service sessions, queue ownership, cancellation, and sibling-service progress bounded? |
| [#944](https://github.com/zakura-core/zakura/pull/944) | Outgoing request ownership and deadlines | Can publication, expiry, reset, or a partial write lose or double-release work? |
| [#945](https://github.com/zakura-core/zakura/pull/945) | Serving migration and activation | Do negotiation, per-peer and node limits, storage, serving, and session retirement compose correctly? |

The first three drafts retain the existing block-sync wire layout. The last
activates the paired layout and removes the previous serving path. Temporary
compatibility methods and unused-API allowances in the intermediate chunks are
removed by activation. The network API version bump belongs to the generic
transport draft because that is where new public error variants first appear.

The transport draft includes [#956](https://github.com/zakura-core/zakura/pull/956).
Persistent streams with one capability form a complete service session. Block
sync declares its one-slot request queue, cancellation-driven request writes,
and 32-second data-write deadline through service hooks. The transport records
remote closes and write timeouts before cancelling a session, so download policy
still parks unanswered work and disconnects repeated stalls.

The September 10 service-session update uses merge commits and normal pushes
through #943, #944, and #945. Earlier commits remain in each branch's history.

[PR #896](https://github.com/zakura-core/zakura/pull/896) remains a separate property
coverage follow-up, still based on #892. It needs restacking before merging with
this replacement series. Its changes are not included in these four drafts.

## Reference equivalence

The reference is #892 at `953b85b18bc2584218c97bc0d5530ecb1122b7d8`.
It includes both fixes accepted immediately before the split:

- A lost session-capacity reservation defers that service instead of closing
  the healthy connection. A real QUIC regression checks retry and sibling data.
- A peer resetting unanswered work still triggers the existing cooldown and
  repeated-stall disconnect. A real QUIC regression also checks that intentional
  local cancellation does not charge the peer a stall.

At creation, the completed stack at `6b060457e` matched that reference apart
from per-PR changelog packaging and this audit map. Subsequently, #941 was closed
at the user's request because the separate
[Iroh upgrade in #935](https://github.com/zakura-core/zakura/pull/935) will replace
its backport. The backport pins, audits, raw QUIC regression, and changelog fragment
were removed from every remaining draft with normal commits. The latest `main`
workflow updates were also merged. Those changes preserved the GetBlocks implementation.

The planned deployment combines this stack with the separate Iroh upgrade and
needs integration validation with that transport. No force push or history rewrite was used.

## Review corrections

The correction pass preserves paired streams and sequential serving. Reader exit
now directly cancels a blocked paired write, without adding a request-write timer.
Request/response reads enforce service allowlists before payload reads, and opener
eligibility is checked before reserving service capacity. Measured peers retain
floor rescue in both byte-count and block-count modes; guarded reservation
failures log their peer, generation, and error. Intermediate transport fixtures
no longer assume that the final BlockSync layout is active.

The latest feedback pass records write errors before a failed request owner can
cancel the session. Block sync processes buffered responses before charging a
stall and keeps local body backpressure neutral. Application closure drains every
session member before retiring shared capacity, while retained receivers and sender clones keep the
session alive. A queued request that expires returns all its remaining unsent
heights immediately. Received bodies and replacement owners survive cleanup,
and committed heights are discarded. Publication failures now include peer,
generation, and range context. The parameter ledger attributes the 32-second
data-write override to #945's service policy.

The serving-query timeout remains removed. The concrete read service is always
ready, and a running database job intentionally retains its serving permit until
that job finishes.

The bounded production-readiness pass also keeps incoming setup reads separate
from connection cancellation, expiry, and worker cleanup. A missing pair identity
cannot reserve service capacity, and an expired incomplete offer must wait one
setup interval before reserving again. If a replacement races the old workers,
only the new pair is reset; the opener can retry while sibling services continue.
Four real QUIC regressions reproduce these failures on the preceding transport
revision and pass with the fixes. Generic legacy transport findings remain outside
this refactor's scope.

## Validation

The [Iroh integration results](getblocks-refactor-results.md#iroh-integration)
record the correction pass, the tested dependency revisions, and the remaining
transport qualification. They do not authorize merging the activation draft.

The following results describe the original stack with the QUIC backport, before
its removal. They do not establish transport readiness without that backport or
with the separate Iroh upgrade.

- Reference fixes: 29 targeted tests and network Clippy passed.
- Storage: all 11 focused tests passed, including the real state API.
- Generic transport: 72 focused tests passed; all-target network Clippy and
  locked dependency metadata passed after placing the API version bump here.
- Outgoing requests: 295 existing block-sync tests passed, followed by both
  deadline regressions after moving their fixture with the policy change.
  All-target network Clippy passed.
- Final activation: all 405 focused tests and all 22 cluster integration tests
  passed without retries. Workspace all-target Clippy, formatting, Markdown lint,
  and changelog checks passed.

Nextest reported occasional process-cleanup warnings on otherwise passing tests.
Those warnings are retained in the test evidence. The standalone long-running
transport measurements are separate from the ordinary acceptance suite.

Check CI coverage against each final head. Earlier stack revisions only received
automatic documentation checks, while the September 10 service-session merge
also triggered Rust checks. Neither passing CI nor local regression checks clear
the combined transport qualification for #945.

Matched block-download acceptance is one-way.

## Merge handling

Review each PR individually. Keep #945 draft until combined qualification passes.
Merge in dependency order and update each child's base as its predecessor is
merged. The final integration result
must still be verified after any substantive review changes. A clean comparison
proves that splitting preserved the reference; it does not replace review of
the reference behavior.
