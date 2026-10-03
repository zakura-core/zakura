The EU testnet node stopped advancing at height 4,355,419 on September 16, 2026. The logs establish that five accepted native operations prevented fallback from acquiring its apply lease. The logs do not identify the processing stage of those operations.

The observed sequence was:

- At 10:47:50 UTC, the node committed height 4,355,419.
- At 10:55:28 and 10:56:48, header transitions reported `ConflictingReplay`.
- At 10:57:56, the watchdog requested fallback after 600 seconds without progress.
- The coordinator reported five permits and five operations throughout the drain.
- At 11:27:56, the 30-minute drain deadline failed the apply lifecycle.
- Systemd restarted the node at 11:28:08 with the same binary.
- At 11:38:34, the watchdog requested fallback again.
- The second drain completed immediately. The fleet reported recovery at 11:38:54.
- At 11:40:07, the legacy round completed at height 4,355,840.

The deployed commit was `85e4f2c4fbf8`. The restart log sets `checkpoint_sync` to false and the maximum checkpoint height to 1,028,800. Blocks near the stalled tip therefore use full verification. Current main also retains the 30-minute drain deadline. An earlier comparison against an older local feature branch incorrectly suggested that current code had removed this deadline.

A deterministic state test establishes one possible dependency cycle. The state service retains a semantically verified block until its parent becomes available. The coordinator retains an accepted operation until that commit resolves. Fallback waits for the coordinator to drain before it can fetch missing blocks. A missing parent can therefore prevent the recovery mechanism from supplying that parent. This test establishes a failure mechanism, not the initial cause of the incident. `ConflictingReplay` remains a separate hypothesis.

The reactor's applying count measures its current sequencer entries. A reset can detach a submitted block from those entries while the driver retains the commit future. The coordinator's operation count measures that retained work. An applying count of zero cannot establish that state commits have drained.

The alert collector now carries coordinator operation count, permit count, oldest-operation age, and apply phase when the node exports them. These fields supplement the reactor count. The lifecycle fix supplies the additional node metrics and per-operation processing stages.

The recovery rule is that cancellation must exclude future writes before the coordinator releases ownership. State may cancel a full-verification request while it waits for its parent. State must drain a request after admitting it to the writer. The checkpoint verifier must claim an entire verified range atomically before state submission. Cancellation must leave checkpoint progress unchanged if the range is incomplete. The existing drain deadline remains the last resort for an admitted write that never resolves.

Validation must cover cancellation racing with admission, duplicate replacement, checkpoint resubmission, sequencer resets, and completion from an earlier epoch. Recovery must advance the same state service without a restart. Local tests can validate these ownership rules without replacing a fleet binary.
