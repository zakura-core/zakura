# First-received mining

## Selection

The fully verified chain with greatest cumulative work is the mining chain.
On equal work, the tip received first by the full-block verifier wins. Receipt
order is assigned before asynchronous verification, so a later block cannot gain
priority by finishing validation faster. Only valid blocks enter chain selection.
A greater-work chain still replaces an earlier equal-work winner.

Full state retains at most ten forks. At that limit, it keeps the newly accepted
branch and the nine highest-ranked other branches. This leaves the accepted
parent available for a child that could make its branch best, while preserving
the current mining winner. Evicted bodies leave the duplicate cache and can be
downloaded and validated again.

Proposals and templates do not establish receipt order. A solved submission gets
its own order even when verification reuses a prepared template. Retained blocks
keep their original order through duplicate delivery, forks, invalidation, and
reconsideration. The comparator reads the current tip block directly.
Overlapping deliveries of the same complete block share their first receipt.
Different bodies with the same header hash do not share priority. Active
registrations are removed when their last verification completes or is cancelled.
Only blocks that passed proof-of-work checks (or the network's authenticated
waiver) retain a receipt after retryable failures or cancellation. Unchecked and
PoW-invalid blocks cannot occupy the retry cache. Retried blocks retain only a
complete-body digest and receipt order, bounded to 4096 entries for one hour after
the last attempt. Each header can retain at most four body variants, including
canceled attempts. Additional variants evict that header's oldest cached variant
before they can displace unrelated receipts. Capacity eviction or expiry gives a
later retry a new receipt.
Success, already committed duplicates, and permanent rejection clear the retry
receipt. A duplicate still waiting in the commit queue preserves it because the
outstanding attempt can fail transiently.
Receipt retention has its own error policy. Missing context, local service
failures, and blocks ahead of the local clock can retry. Permanent block and
transaction failures release their receipts even when peer attribution must
remain inconclusive.

For example, A arrives before B, but B finishes verification first. Mining can
briefly use B while A is unverified. Once A passes, equal-work selection chooses
A. If B gains a child with more cumulative work, mining switches to that child.

## Header sync

The header engine still uses greatest work and raw hash to select downloads.
Its `header_best` may differ from the fully validated `verified_best` on equal
work. The atomic full-state transition publishes the verified choice, including
after operator invalidation and reconsideration. The planner checks that the
chosen path is eligible, fully verified, and has greatest cumulative work.
When full state evicts a fork, the same atomic transition clears its retained
body verification markers. Its headers stay eligible for download and validation.
This also applies when reconsideration restores a branch at the fork limit.
The header limit allows one extra candidate beyond the ten full-state forks,
so an independent header download tip can remain selected.

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
