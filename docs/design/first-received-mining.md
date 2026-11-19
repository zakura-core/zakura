# First-received mining

## Selection

The fully verified chain with greatest cumulative work is the mining chain.
On equal work, an explicit `preciousblock` preference wins. Otherwise, the tip
with the earlier receipt order wins. Receipt
order is assigned before asynchronous verification, so a later block cannot gain
priority by finishing validation faster. Only valid blocks enter chain selection.
A greater-work chain still replaces an earlier equal-work winner.

## Announcement priority

A block's receipt order is drawn when its complete body reaches the full-block
verifier. A block whose header was admitted first keeps the order reserved for
that header instead, provided its body reaches the verifier before a deadline
that grows with its size.

Without this, a block full of transactions can lose an equal-work race to an
empty block announced later, because the larger body takes longer to transfer.
That penalizes including transactions.

The reservation is called a claim. Four rules define it:

- **A claim needs a fully validated header.** Native header sync reserves the
  order and the time when it starts applying a header, and the claim takes
  effect once state has accepted that header. By then the header has passed
  Equihash, its target, parent linkage and contextual difficulty. A header
  deferred for a future timestamp gets no claim. Only headers one or two
  blocks above the verified tip are claimed, so catch-up creates none.
- **A block has one claim, and it is permanent.** Delivering the header again
  changes nothing, and reading the claim does not use it up, so an invalid
  body under the same header cannot take it. The table holds 64 claims. When
  it is full it refuses new claims and never drops old ones. A claim is
  removed only when its height falls below the verified tip, where no new
  claim can be made.
- **The body has a deadline.** It is 500 ms plus the body's serialized size at
  1 MiB per second, capped at `MAX_CLAIM_BODY_DEADLINE` (2 seconds). A
  body that arrives later draws a fresh order, as if its header had never been
  seen.
- **A node keeps the block it mined.** A locally submitted block has no claim
  and draws its order at submission. A rival's earlier claim at the same
  height is ignored from that moment. If the submitted block fails
  verification, the rival's claim is back in force.

The verifier looks a claim up by block hash, so native block sync, legacy
gossip and the legacy syncer follow the same rule. The deadline is measured
when the body reaches the verifier, after each transport's own queueing, so
the rule is the same on every path and the latency is not.

The deadline is what keeps the rule safe. A miner can announce a real header
and withhold its body. With no bound, that miner would win every equal-work
tie against blocks found while it withheld, which makes withholding profitable
at any hashrate. Because a claim belongs to the block and not to a delivery
attempt, a failed download or a repeated announcement does not restart the
deadline. Inside the deadline, withholding is indistinguishable from slow
honest delivery, so the deadline is both the transfer delay forgiven for large
blocks and the most a withholder can gain. A miner who pads a block to about
1.5 MiB gets the full 2 seconds. The cap is therefore the real bound, and the
size term only shortens the deadline for small blocks.

The constants are estimates of honest delivery, not measurements. The
`sync.block.announcement_claim` counter records how each body resolved
(`used`, `expired`, `voided` by a mined block, or `none`), and the
`sync.block.announcement_claim.age.seconds` histogram records the age of the
claim at body arrival, both labelled by block size. The
`sync.block.announcement_claim.refused` counter records claims refused by a
full table.

For example, A's header is admitted before B's, but B's smaller body arrives
and validates first. Mining uses B until A validates. If A's body arrived
before its deadline, equal-work selection then chooses A. If it arrived later,
B stays.

A claim never makes a block eligible. Mining still waits for full validation.
Some blocks have no claim and are ranked by body arrival:

- A block seen only through legacy peers, because no legacy path validates
  headers before bodies.
- A block whose header arrived before a restart. Claims are not persisted.
- A block refused by a full table. On Testnet, minimum-difficulty headers can
  fill the table at every height.

[Fork retention](../specs/fork-aware-header-chain-engine.md#lc-retain-01)
keeps newly accepted parents available for extension and evicted bodies eligible
for download and validation again.

Proposals and templates do not establish receipt order. A solved submission gets
its own order even when verification reuses a prepared template. Retained blocks
keep their original order through duplicate delivery, forks, invalidation, and
reconsideration. The comparator reads the current tip block directly.
Overlapping deliveries of the same complete block share their first receipt.
Different bodies with the same header hash do not share a receipt. Active
registrations are removed when their last verification completes or is cancelled.
A block redelivered after that gets a new receipt, including after a transient
failure before it reached the state queue. A claim does not change this
bookkeeping. It only sets the order a new receipt takes, so every body that
arrives under a claimed header before its deadline takes the claimed order.

A full body already waiting for its parent in the state queue keeps its receipt
even if its caller times out, and an identical redelivery does not replace it.
That queue has a 1000-entry bound, but no wall-clock expiry. The body stays until
its parent arrives, an ancestor fails, finalization reaches its height and it is
pruned, or the process stops. An old receipt still cannot beat a chain with
greater work.

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

The integrated header engine selects greatest work and preserves its selected
eligible tip on equal work. It restores that preference from durable metadata.
A stronger fork or loss of eligibility changes selection. Raw hash resolves ties
when no selected tip remains competitive. Headers-only mode retains raw-hash
ordering. This prevents equal-work arrivals from repeatedly replacing an active
body download. Header arrival order and full-block receipt order can still differ.

The header engine's preference has no time bound. If the selected equal-work
header's body is withheld, the engine keeps that header selected and raises the
body-unavailable alarm, and native block sync does not fetch the competing
body. Legacy gossip can still deliver the competitor. Bounding this preference
the way the body deadline bounds full-state priority is follow-up work.

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
