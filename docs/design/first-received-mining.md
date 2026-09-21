# First-received mining

## Selection

The fully verified chain with greatest cumulative work is the mining chain.
On equal work, the tip received first by the full-block verifier wins. Receipt
order is assigned before asynchronous verification, so a later block cannot gain
priority by finishing validation faster. Only valid blocks enter chain selection.
A greater-work chain still replaces an earlier equal-work winner.

Proposals and templates do not establish receipt order. A solved submission gets
its own order even when verification reuses a prepared template. Retained blocks
keep their original order through duplicate delivery, forks, invalidation, and
reconsideration. The comparator reads the current tip block directly.
Overlapping deliveries of the same complete block share their first receipt.
Different bodies with the same header hash do not share priority. Active registrations are removed when their last verification completes or is
cancelled. Retryable failures and cancellations retain only a complete-body digest
and receipt order, bounded to 4096 entries for one hour after the last attempt. Capacity eviction or
expiry gives a later retry a new receipt. Success and permanent rejection clear
the retry receipt.

For example, A arrives before B, but B finishes verification first. Mining can
briefly use B while A is unverified. Once A passes, equal-work selection chooses
A. If B gains a child with more cumulative work, mining switches to that child.

## Header sync

The header engine still uses greatest work and raw hash to select downloads.
Its `header_best` may differ from the fully validated `verified_best` on equal
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

The trusted indexer stream carries optional `receipt_order` metadata. Its
`x-zakura-receipt-session` response header identifies the primary process that
assigned those orders. A secondary returns that value as `receipt_session` on
resubscription. If it differs, the server ignores the secondary's known tips and
sends a complete snapshot. The secondary discards its old receipt-order domain
and resubscribes with empty tips whenever the response session changes, including
when an older server omits it. After the block syncer has taken over publication,
it immediately clears the published fork and refreshes the finalized tip while
the replacement stream is empty, even if a legacy server sends no messages.

Secondaries request `include_chain_snapshot` to receive a `chain_snapshot`
message after each batch of blocks. Its complete retained tip set covers changes
to already-known blocks, including invalidation and reconsideration. Snapshot
messages contain no block data. Missing tips cause a complete resubscription.
An empty tip set clears the mirror's non-finalized state. With session-aware
servers, blocks stay private until the snapshot marker reconciles the full fork
set. Only then does the mirror publish the new state and tip. A disconnected or
failed batch is discarded and resubscription uses the last published snapshot.
Legacy servers without snapshot markers retain incremental publication.
An initial empty snapshot leaves finalized-tip tracking active during checkpoint
sync. The first real block transfers that responsibility to the block syncer.

These fields are additive. Servers send snapshot messages only to clients that
request them. Older servers omit receipt metadata and retain their hash-based
policy. Older clients can decode block messages but cannot mirror the new tie
policy exactly, so upgrade trusted secondaries alongside the primary. Ordinary
block encodings and the P2P header protocol are unchanged.

The generated Rust request and response structs gain fields, which breaks
exhaustive struct literals in library consumers. The RPC crate therefore advances
to the next major version even though the wire extension is additive.
The header-chain crate also advances to the next major version because its
public operator-error enum gains a variant.
