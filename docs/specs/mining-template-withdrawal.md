# Mining template withdrawal

## Problem and scope

PR #748 publishes mining templates before background proposal validation finishes.
The background worker previously logged a failure without withdrawing the template.
The internal miner could keep solving that template until a tip change or another
template-unavailability event stopped it.

The fix cancels mining work on the rejected template and supplies a validated
replacement when available.
The node keeps running while the internal miner waits for that replacement.
It does not promise that speculative mining can never start on an invalid candidate.
That stronger guarantee would require validation before publication and would delay
the fast tip-change template.

## Failure cases

| Case | What must happen | Existing protection | Withdrawal action |
| --- | --- | --- | --- |
| Transaction expiry | A snapshot includes a transaction whose expiry is below the candidate height | The mempool removes transactions when the tip reaches their expiry, including their dependents; consensus checks the candidate height | Withdraw if proposal verification nevertheless reports expiry |
| Conflicting spends or nullifiers | Template assembly or a stale snapshot includes transactions that conflict in the candidate context | Mempool admission and contextual block validation check conflicts | Withdraw the candidate after a deterministic contextual rejection |
| Missing transaction dependency | Template assembly omits a required ancestor transaction | Dependency-aware selection and contextual UTXO checks | Withdraw after a deterministic rejection |
| Invalid block totals or commitments | Template assembly produces incorrect fees, coinbase value, sigops, or commitments | Semantic and contextual proposal checks | Withdraw after a deterministic rejection |
| Parent changes during preparation | The selected tip moves while the proposal waits or validates | State requires a proposal to extend the current best tip | Cancel stale preparation; do not quarantine the new parent |
| Local service failure | A service stops, lacks context, or times out | Verification returns an error | Log the failure; do not label the candidate consensus-invalid |
| Miner changes a solved header | A miner submits an invalid timestamp, solution, or other changed data | Submission rechecks mutable fields and candidate identity | Reject that submission; do not withdraw unrelated server work |

A transaction's signature or proof does not become invalid merely because time passes.
Expiry depends on candidate height, not time spent solving at that height.
A transaction may become unsuitable for a new chain context without invalidating its
original block on its original parent.
The cases above describe possible triggers, not observed production incidents.

## Implementation

The server retains eight parent entries in least-recently-used order.
Each entry owns a random work-ID namespace, up to 64 rejected IDs, and up to 64
prepared IDs. A work ID identifies one template within that namespace.
A rejection applies to its own parent even when validation finishes after a tip change.
Saturation withdraws that parent's work and stops publication on that parent.

Evicting an entry retires its namespace. A withdrawal waiter checks the namespace
and current result together, so it detects retirement even when it subscribes after
the rejection and eviction. Another parent's retirement cannot withdraw retained work.
A late validation result cannot mutate a replacement entry for the same parent hash.

Each new parent entry receives a fresh long-poll revision. The tracker retains the
greatest parent height it has observed. A new entry at that height or below requires
foreground empty-template recovery. This rule covers forgotten-parent returns with
bounded memory. A new forward height can still use speculative preparation.
Returning to a retained parent preserves its existing policy and results.

A concrete rejection or prepared-ID eviction advances the affected parent's revision.
Long polls wake even when the parent and mempool stay unchanged. The server accepts
legacy long-poll IDs. New parent entries and withdrawal events use the revision suffix.
A response that replaces an older revision sets `submitold: false`.
External miners must cooperate with withdrawal.

The server holds the rejection-state guard through the fast publication decision.
Recovery validates an empty template, then checks the live parent and revision,
registers the prepared ID, and sets the response revision in one write closure.
Recovery returns an error if validation fails, times out, or loses its context.
A later rejection can still withdraw work that was valid at publication.

The background queue retains one running computation and one newest pending template.
A deadline classifies its cost; it does not release ownership of work still computing.
A late rejection still counts. Queued speculative work on a recovery parent is discarded.
Solved-block verification and commit use separate ownership.

## Tests

The RPC suite covers parent ABA, late rejection, parent retirement before waiter
subscription, unrelated parent eviction, and prepared-ID eviction. The long-poll
regression fills the prepared bound with real recovery responses and confirms that
eviction wakes a waiting client with `submitold: false` and the current revision.
A concurrent rejection prevents recovery publication.
The internal miner tests cover cancellation during RPC waits and refresh delays.

## Limits

Revisiting a forgotten height requires foreground empty-template recovery even when
that particular parent has never failed validation. This conservative policy avoids
an unbounded parent blacklist. Normal forward heights retain speculative preparation.

Recovery can repeat validation for concurrent requests. Candidate deduplication and
recovery admission remain performance follow-ups. Background preparation remains
bounded independently of RPC cancellation and validation latency.
