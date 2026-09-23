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
Only blocks that passed proof-of-work checks (or the network's authenticated
waiver) and transaction Merkle root checks retain a receipt after retryable
failures or cancellation. These checks do not establish full transaction
validity. Retried blocks retain only a complete-body digest and receipt order,
bounded to 4096 entries for one hour after
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

The state queue retains up to four distinct bodies per header while waiting for
its parent. Every body counts against the existing global queue limit. Identical
retries replace their response channel and keep their original receipt. Distinct
bodies keep separate receipts until contextual validation finds a valid body.
Rejecting one body does not reject the header or its children while another
retained body can still commit.

For example, A arrives before B, but B finishes verification first. Mining can
briefly use B while A is unverified. Once A passes, equal-work selection chooses
A. If B gains a child with more cumulative work, mining switches to that child.

This also applies when A arrives before its parent. B's chain can be fully
available first, but A can retain its earlier receipt while waiting for missing
context. Once A's parent arrives and both chains validate, A can win an equal-work
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
