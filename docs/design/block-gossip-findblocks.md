# Block gossip during legacy `FindBlocks`

Legacy Zcash peers use the same `inv` message for a `getblocks` response and
for unsolicited block gossip. A connection with a pending `FindBlocks` request
therefore historically treats the next block-only `inv` as its response.

Zakura adds a backward-compatible trailing extension to block advertisements:

```text
"ZINV" | version 1 | block-gossip kind 1 | payload length 0
```

The extension is present only on the single-block `inv` messages emitted for
`AdvertiseBlock` and `AdvertiseBlockToAll`. `FindBlocks` responses and
transaction inventory are unchanged. There is no request identifier, service
bit, handshake capability, protocol-version threshold, or `version` field.
Legacy decoders consume the standard inventory prefix and ignore the trailing
bytes.

Malformed extensions and unknown versions, kinds, or payloads are treated as
untagged. This preserves the meaning of legacy traffic rather than letting an
invalid extension change response matching.

## Handling and penalties

| Message during `FindBlocks` | Handling | Ban/disconnect behavior |
| --- | --- | --- |
| Untagged block `inv` | Existing `FindBlocks` response path | Existing scoring and bans remain possible |
| Tagged block-gossip `inv` | Existing inbound gossip path; `FindBlocks` remains pending | No mistaken sync lookahead score; no disconnect merely for sending gossip |
| Tagged gossip containing a consensus-invalid block | Existing gossip verification | Existing consensus misbehavior score and ban remain possible |
| Malformed protocol traffic or transport failure | Existing connection error paths | Existing disconnect behavior remains unchanged |

A tagged far-ahead gossip block reaches the existing inbound download path.
That path drops an out-of-range block without attributing the legacy sync
lookahead penalty. The tag does not disable verification or banning: if the
advertised block is downloaded and found consensus-invalid, its advertiser is
still scored through the normal gossip path.

Untagged responses retain the complete legacy behavior, including terminal-hash
stripping, the 50,000-block lookahead limit, and scoring for
`AboveLookaheadHeightLimit`, `InvalidHeight`, and consensus-invalid blocks.

The sync handler does not directly close a peer socket after an attributed
violation. It emits 100 misbehavior points. Network initialization batches
these updates for up to 30 seconds, the address book bans an IP when its
cumulative score reaches 100, and the peer set then drops active services for
that IP and rejects future connections.
