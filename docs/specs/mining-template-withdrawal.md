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

1. Retain rejected server work IDs in a watch channel scoped to the current parent.
   Retained state covers failures before subscription and between checking and waiting.
   Ignore late results for another parent.
   Bound rejection storage at 64 IDs; stop issuing templates on overflow until the
   parent changes.
2. Classify concrete consensus and contextual errors.
   Unwrap the consensus router error before classification.
   Keep missing-context, stale-parent, timeout, and service failures retryable.
   Withdraw a server template that cannot form a proposal.
3. Let internal template generation watch the active work ID during RPC requests and
   refresh delays.
   Clear the active template on rejection.
   The existing solver callback observes the cleared template and stops at its next
   cancellation check.
   Preserve internal work that already passed validation when another candidate fails
   on the same parent. Cancel unvalidated work conservatively.
4. Increment the long-poll withdrawal revision on rejection.
   Wake long polls even when the parent and mempool stay unchanged.
   Accept legacy 46-character IDs; append 16 hex digits after a withdrawal.
   External miners receive a replacement with `submitold: false`.
   External miners must cooperate; RPC cannot force them to stop.
5. Enter empty-template recovery for the affected parent.
   Validate the empty template before returning it.
   Return an error if validation fails or exceeds 30 seconds.
   Recheck the recovery context before publication.
   Do not issue further speculative transaction sets until the parent changes.
   This conservative recovery avoids an unbounded candidate fingerprint blacklist.
6. Discard queued speculative preparation during recovery.
   Drop the preparation future when its parent becomes stale or after 30 seconds.
   Keep solved-block verification and commit outside this cancellation path.

## Tests

- `zakura-consensus`: `template_rejection_distinguishes_expiry_from_service_failure`
  separates a real transaction-expiry rejection from a service failure.
- `zakura-rpc`: `template_rejection_wakes_long_poll_and_validates_recovery` and
  `template_rejection_before_long_poll_is_not_lost` cover withdrawal, validated
  recovery, and a rejection that precedes the long poll it must wake.
- `zakura-rpc`: `template_rejection_targets_work_and_ignores_old_parents`,
  `template_rejection_storage_fails_closed_at_capacity`,
  `prepared_template_tracking_keeps_new_recovery_work_at_capacity`, and
  `template_rejection_retains_notifications_for_late_subscribers` cover the
  rejection state itself.
- `zakura-rpc`: `long_poll_withdrawal_changes_id_and_disallows_old_work` checks that a
  withdrawal disallows old shares and that the revised ID round-trips.
- `zakurad`: `template_rejection_cancels_during_rpc_wait` and
  `template_rejection_cancels_during_refresh_delay` cover the internal miner's two
  cancellation points.

## Limits and follow-up measurements

Dropping an async preparation future does not preempt cryptographic work that a
buffered service, batch verifier, or blocking thread already owns.
The block verifier already returns on the first transaction error it observes.
This change does not claim that every submitted proof check stops at that instant.
Fine-grained proof-task cancellation requires a separate ownership audit.

Recovery may repeat empty-template validation for concurrent requests.
Candidate deduplication and preparation scheduling remain performance follow-ups;
they do not gate withdrawal correctness.
Measure preparation CPU, cache-hit rate, solved-block latency, and rejection-to-solver
stop latency before selecting a preparation rate limit.
The implementation counts rejected, cancelled, and timed-out preparations.
