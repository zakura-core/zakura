# First-received mining

## Selection

The fully verified chain with greatest cumulative work is the mining chain.
On equal work, the tip received first by the full-block verifier wins. Receipt
order is assigned before asynchronous verification, so a later block cannot gain
priority by finishing validation faster. Only valid blocks enter chain selection.
A greater-work chain still replaces an earlier equal-work winner.

[Fork retention](../specs/fork-aware-header-chain-engine.md#lc-retain-01)
keeps newly accepted parents available for extension and evicted bodies eligible
for download and validation again.

Proposals and templates do not establish receipt order. A solved submission gets
its own order even when verification reuses a prepared template. Retained blocks
keep their original order through duplicate delivery, forks, invalidation, and
reconsideration. The comparator reads the current tip block directly.
Overlapping deliveries of the same complete block share their first receipt.
Different bodies with the same header hash do not share priority. Active
registrations are removed when their last verification completes or is cancelled.
A block redelivered after that gets a new receipt, including after a transient
failure before it reached the state queue.

A full body already waiting for its parent in the state queue keeps its receipt
even if its caller times out, and an identical redelivery does not replace it.
That queue has a 1000-entry bound, but no wall-clock expiry. The body stays until
its parent arrives, an ancestor fails, finalization reaches its height and it is
pruned, or the process stops. An old receipt still cannot beat a chain with
greater work.

The state queue retains up to four distinct bodies per header while waiting for
its parent. Every body counts against the existing global queue limit. Identical
retries replace their response channel and keep their original receipt. Distinct
bodies keep separate receipts until contextual validation finds a valid body.
Rejecting one body does not reject the header or its children while another
retained body can still commit. If writer capacity rejects a group, its queued
descendants are released with a retryable error as well. A duplicate in the queue
or writer is retryable until committed state confirms the block. Native body sync
must not treat that pending write as verified evidence.

For example, A arrives before B, but B finishes verification first. Mining can
briefly use B while A is unverified. Once A passes, equal-work selection chooses
A. If B gains a child with more cumulative work, mining switches to that child.

This also applies when A arrives before its parent. B's chain can be fully
available first, but A can retain its earlier receipt while waiting in the state
queue. Once A's parent arrives and both chains validate, A can win an equal-work
tie. This preserves priority for honest out-of-order delivery. It also lets a
miner with a private lead reserve priority for a tip while withholding an
ancestor from nodes that received the tip.

zcashd assigns tie priority only once the block and its ancestor data are
available. Zakura uses the tip's full-block verifier receipt instead. Reconsidering
this policy is tracked separately in [#1127](https://github.com/zakura-core/zakura/issues/1127).

## Header sync

The header engine still uses greatest work and raw hash to select downloads.
Native block sync requests bodies on that selected header chain. If both headers
arrive before either body is handed off for verification, the higher-hash branch
can be the only one whose body reaches the verifier.
Making header selection agree with full state on equal work is tracked in
[#1137](https://github.com/zakura-core/zakura/issues/1137).

Peers also serve retained bodies on the selected header branch. A node can keep
mining on the earlier block A while serving an equal-work side-fork block B to a
peer whose header sync selected B. Each response uses one branch snapshot and
stops if the next selected body is unavailable. It never substitutes a block
from the node's mining branch at the same height.

A header switch can discard a body that is still downloading or buffered. A block
already handed off for verification is not cancelled by the switch alone.
First-received priority starts when the full-block verifier receives the block,
which can be after it waits in the apply queue.

The header engine's `header_best` may differ from the fully validated `verified_best` on equal
work. The atomic full-state transition publishes the verified choice, including
after operator invalidation and reconsideration. The planner checks that the
chosen path is eligible, fully verified, and has greatest cumulative work.

## Restart

Receipt order is local metadata and is not stored in blocks or backup files.
Restored blocks precede new receipts. Two restored blocks, or equal receipt
orders, use raw hash as a final tie-breaker. Historical arrival preference between
restored forks therefore does not survive a process restart. Startup reconciles
the restored full-state path with the durable header engine before publication.

## Trusted mirrors

[Receipt metadata](trusted-mirror-receipts.md) lets trusted secondaries use the
primary's arrival order even when their stream delivers blocks in a different
order. A process session identifies which primary assigned those orders.
Changing sessions clears old receipt metadata before replaying blocks. Same-session
reconnects and duplicate deliveries preserve the original orders.

Older servers omit receipt metadata and retain their hash-based policy. Older
clients can decode block messages but cannot reproduce the new tie preference,
so upgrade trusted secondaries alongside the primary. The stream remains
incremental. Complete fork snapshots and broader mirror recovery are separate work.

The header-chain operator-error enum gains a variant. This public API change must
be accounted for when selecting crate versions for the next release.
