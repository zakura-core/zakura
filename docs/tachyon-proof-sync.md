# Tachyon proof synchronization

NuTachyon builds expose `gettachyonblock` for wallets and proof services that
construct spendability updates from public chain data. The method reads one
best-chain block and its historical anchors. It does not receive a wallet's
nullifiers or generate proofs.

```sh
curl --user "$(cat /path/to/rpc/.cookie)" \
  -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"gettachyonblock","params":["10"]}' \
  http://127.0.0.1:18232
```

The argument is a decimal height or a conventional block hash. The response has:

| Field | Meaning |
| --- | --- |
| `hash`, `previousBlockHash`, `height` | Block identity and chain continuity. |
| `activationHeight`, `poolHeight`, `epoch`, `epochLength` | Network-specific position in the Tachyon pool. |
| `finalized` | Whether the block was finalized when read. |
| `anchorBefore` | Parent's end-of-block anchor; at activation, the epoch-zero entry anchor. |
| `epochStartAnchor` | Entry anchor if this block starts an epoch, otherwise null. |
| `anchorAfter` | Consensus-computed end-of-block anchor. |
| `stamps` | Ordered proof-stamp inputs, each with `transactionIndex`, `txid`, `tachygramSet`, and `tachygrams`. |

Anchors, commitments, and tachygrams are hex strings containing canonical
Tachyon wire encodings; do not reverse these bytes. Hashes and txids use their
usual RPC display order. Positions include coinbase and non-Tachyon transactions.
Pointer/adjunct bundles contribute no additional anchor step: their tachygrams
are covered by their aggregate's proof stamp.

## Consuming the feed

1. Obtain the tip from `getbestblockheightandhash`. Fetch successive blocks from
   your saved cursor, checking each `previousBlockHash` against the saved hash.
   Also compare `anchorBefore` with the previous response's `anchorAfter`.
2. If `epochStartAnchor` is present, process the epoch crossing before the
   stamps. Activation starts at `zcash_tachyon::Anchor::default()`; the special
   predecessor used to seal epoch zero is the zero field element.
3. Process stamps in response order. Reconstruct each `TachygramSetPoly` from
   its tachygrams and check its commitment against `tachygramSet`. Fold the
   commitment into the anchor with `next_stamp(epoch, commitment)`.
4. Check the resulting anchor against `anchorAfter`. Save the block hash and
   data or constructed evidence before advancing your durable cursor.

Empty blocks have `stamps: []`. Inside an epoch they leave the anchor unchanged.
An empty block that begins an epoch still performs an epoch crossing. A completed
epoch closes at the next epoch's entry anchor, before that next block's stamps;
do not confuse this with the next block's `anchorAfter`.

Each response uses one selected non-finalized chain snapshot. Different calls
can straddle a reorganization. Recheck the saved hash at its height on startup
and when polling, even if no higher block arrives. On a mismatch, roll back to a
common ancestor and discard evidence made from replaced blocks. A sidecar can
wait for `finalized: true` before publishing durable epoch evidence, or maintain
reversible work for the live chain.

## Proof-service boundary

Run proof generation as a sidecar. It owns proof scheduling, caches, QR bucket
construction, evidence trees, and an API accepting opaque per-epoch nullifiers
and an anchor/epoch range. With the current Tachyon API, it returns a composed
`ArbitraryUnspent` PCD and the public span needed to check the response.
Give that service its own authentication, request-size/span limits, proving
queue, and rate limits. Expensive client proving must not compete with the
validator's RPC or consensus workers.

For closed epochs, build `SummarySeed`/`SummaryAdvance` or `QrStampIntakeSeed`
inputs from the feed. Use `QrEmptyIntakeSeed` for an epoch with no stamps, and
`QrBucketSeal` with the preceding epoch's final anchor. QR buckets/evidence trees
support `QrUnspentInit`, `UnspentLift`, and `UnspentFuse`. The wallet binds the
result to its own derivation with `UnspentBind` and advances its `NoteSpendable`
using `SpendableLift`. Active-epoch `AnchorChain` proofs can be built from the
same ordered stamp commitments for stamp lifting.

The [proof-consumer tests](../crates/zakura-rpc/src/methods/types/tachyon/tests.rs)
demonstrate decoding this JSON, checking stamp commitments, and building and
composing a mock-Ragu absence proof across a populated epoch and an empty epoch.

The service needs no note plaintext, spending key, nullifier derivation key,
or private spendability witness. Opaque nullifiers still expose the requested
ranges and which requests share a session; an API should avoid logging them.
Tachyon currently uses mock Ragu, so this interface must not be represented as a
production cryptographic proof service.

## Retention and errors

Archive nodes can serve old inputs after the live two-epoch tachygram index is
pruned: this RPC uses retained transaction bodies and the historical anchor
index. It does not depend on the expiring anchor-validity index. No database
migration or new retention configuration is required.

A node that has removed historical transaction bodies cannot reconstruct them
from an anchor. A sidecar must ingest them while retained and keep the evidence
and associated public witness material, or use an archive node to catch up.
`getblockchaininfo` reports the node's transaction-pruning status and prune height.

Malformed identifiers and blocks outside the selected best chain return `-8`.
Inactive Tachyon, unavailable/pruned data, missing anchors, and inconsistent
anchor reconstruction return `-1` with a descriptive message. These failures
must not be treated as empty blocks. Queries are bounded to one block and time
out after 30 seconds. The method is absent when the `nutachyon` cfg is disabled.
