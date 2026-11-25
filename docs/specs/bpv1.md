# BPv1 protocol specification

Status: protocol draft, third revision (BPv1). Earlier revisions called this
protocol Dogwood. This document defines the proposed behavior, amended with
the BPv1 implementation decisions:

- **Coinbase commitment (decision-1, decision-9, decision-25):** The proposer
  commits `C` in one zero-value post-NU7 v6 coinbase `OP_RETURN` output
  (`6a 25 "BPV1" 01 C`), proven via both txid and ZIP 244 auth-data index-0
  paths plus `hashBlockCommitments`. The byte after the tag is the format
  version. Readers ignore every output in another form. `block_id` is the
  consensus block hash; Ed25519 signatures, variant identifiers, and the
  conflict rule are removed. Parts encode `B'` (transactions after the
  coinbase); `coding` carries `tx_count`.
- **Headers (decision-2):** `HeaderMeta` always carries the full header, once
  per block per connection. Header sync runs unchanged and headerchain coalesces
  validation by hash.
- **Seeded pull, holder push, and exact-index `Want` (decision-13,
  decision-14, decision-24):** BPv1 carries seven messages (`HeaderMeta`,
  `Want`, `StripeDone`, `BlockDone`, `Part`, `Have`, `WantEnd`), omitting
  `Subscribe`, `Unsubscribe`, `Refuse`, `SubscriptionEnd`, and `Advert`. The
  proposer pushes seeds to its neighbours, behind `Have`s with the push flag.
  Each neighbour pushes its relay-flagged seeds once to its own peers, behind
  a `Have` with the push flag. Every later hop pulls. `Want` carries an exact
  11-byte requested bitmap with no separate `count`.
- **All-systematic fast path & root-mismatch fallback (decision-8, decision-10):**
  A stripe holding all `k` source parts assembles without decoding; the re-encode
  check gates only regenerated parity. A body whose transaction or auth-data root
  does not match the header falls back to native block sync without marking the
  header invalid.
- **Seed plan and relay flags (decision-19):** `C` names no peer. The
  proposer builds a focused seed plan when the block is solved. It flags each
  neighbour's seeds with a `Have` on that neighbour's data stream, ahead of the
  parts; the relay flag marks the seeds the neighbour pushes onward. Each
  holder takes at most an even share of a stripe's pushed seeds. Direct copies
  fill each neighbour's idle link until feedback stops them. BPv1 has no entry
  peers and no lanes; the lane and subscription rules in sections 3, 6, and 7
  apply only to the subscription extension, which must define lanes again
  before it ships.
- **Paced `Have`s (decision-26):** A node announces parts at once to an initial
  group of peers, and to every other peer after `advert_delay`. Main's block
  gossip uses the same pacing and settings.
- **Rate samples and the 2 × BDP cap (decision-21):** When a `Want` ends with
  parts, the receiver takes one delivery-rate sample,
  `bytes / max(t_last − t_sent − min_rtt, t_last − t_first)`. A peer's `bw` is
  the windowed max of its samples and the transport's `rate_in`. A node commits
  at most `max(2 parts, 2 × bw × rtt)` of pulls to a peer.
- **The proposer's silence (decision-20):** The proposer sends `HeaderMeta`
  and `BlockDone`, never `Have`. `BlockDone` means the sender holds every
  part. Repair rounds, but not normal pulls, may ask any `BlockDone` peer.
- **`Have`s to the proposer (decision-32):** Every node announces to the
  proposer each verified part that the proposer did not send it, at once and
  unpaced. The proposer never pulls these parts. Its trace then records when
  each neighbour obtained each part.
- **Estimate-based repair (decision-22):** Repair starts after `SLOW_X` × one
  part's estimated arrival time without progress, doubling per round. The first
  repair round releases the body claim, so native block sync races.
- **Arrival-ordered pull planning & liveness (decision-3, decision-12):** Pulls
  and seed plans compare estimated arrival time `now + rtt + (ahead + bytes) / bw`
  with `SLOW_X = 2` late release.
  Unanswered `Want`s use deadline `8 s + outstanding_bytes / 100 KB/s`, parking
  the session on first stall and closing neutrally on repeat stall. A control
  frame that waits `4 × (8 s + frame_bytes / 100 KB/s)` for its stream also
  closes the session neutrally.

The [design document](../design/bpv1.md) explains the background choices.
Section 8 fixes candidate payload profile W2.

`MUST` defines a security or interoperability requirement. `SHOULD` defines the
default policy. An alternative policy must preserve every `MUST`. `TBD`
identifies a choice that blocks interoperable implementation.

Message admission follows the
[peer message regulation specification](peer-message-regulation.md). That
document defines the filter order, the admission results, reservations,
subscriptions, cadence, and capacity admission. Section 5 states the
BPv1-specific rules for each message in the same format.

## 1. Scope and integration

The protocol pushes and pulls the parts of new proof-of-work blocks over
existing authenticated, encrypted, reliable peer connections. Existing service
negotiation MUST select a common protocol profile before these messages appear.
No additional application handshake is defined here.

Headerchain MUST own header validation, contextual difficulty, fork choice, and
header recovery. The propagation service MUST submit `HeaderMeta.header` through
that admission path. It MUST NOT create a second header validator. Existing
header discovery and full-block download MUST remain available. Nodes MUST
deduplicate headers across these paths by consensus block hash.

A node MUST NOT forward metadata, open assembly state, decode parts, or update
route measurements before full header admission and metadata authentication.
Bounded parsing, verification, and the early record in section 3 are the
necessary exceptions. If parent context is missing, the node MAY retain a
bounded metadata envelope while headerchain recovers that context. It MUST apply
independent count, byte, work, and time limits to this pending state, both per
peer and node-wide. Headerchain answers a missing parent before it checks proof
of work, so a pending entry costs its sender only bytes. Pending metadata MUST
NOT take space from metadata whose parent the node holds. When the node-wide
pending limit is full, the node SHOULD drop an entry of the peer holding the
most pending metadata, not refuse a peer holding less. A dropped entry is not a
violation, and the block arrives through header sync and block download.

Header admission does not validate the block body. Reconstructed blocks MUST
enter the existing consensus block-validation path.

The [design targets](../design/bpv1.md#performance-targets) define the
planning workload and the latency goal. These estimates do not change consensus
limits or establish measured guarantees. The block interval and body size remain
required sizing inputs.

The throughput claim assumes that the honest relay network remains connected
after removing the proposer. Experiments MUST state the relay degree, the
available service, and the failures that preserve this assumption. Experiments
MUST measure normal-path completion separately from repair and full-block
download. Disconnected relay components are failure cases outside the
throughput claim. Implementations MUST still bound their resource use and
recovery attempts.

Several blocks can propagate at once. They share each connection's streams,
queues, subscriptions, and limits.

## 2. Parts, stripes, and authenticated metadata

Encode the block body, not the already transmitted header. Let `B` be the
canonical serialization of the transaction vector, including its count prefix.
The existing block serializer concatenates the header and this vector.
Reassembly MUST use the exact admitted header from `HeaderMeta`.

```text
S        = clamp(round_up(ceil(len(B) / STRIPE_K), ALIGN), S_MIN, S_MAX)
                                                // W2: S_MIN = 4,096, S_MAX = 65,536, ALIGN = 64
K        = ceil(len(B) / S)                     // source parts in the body
stripes  = ceil(K / STRIPE_K)                   // W2: STRIPE_K = 64
k        = ceil(K / stripes), or 0 if stripes = 0  // source parts per stripe
n        = k + parity_parts(k)                  // W2: parity_parts(k) = ceil(k / 3)
data     = split(zero_pad(B, stripes * k * S), S)
source(s) = data[s*k .. (s+1)*k]                // stripe s, 0 <= s < stripes
coded(s)  = systematic_encode(source(s))        // n parts; the first k equal source(s)
index(s, j) = s * n + j                         // wire index, 0 <= j < n
```

Each stripe is an independent codeword. Any `k` distinct correctly encoded parts
of a stripe reconstruct that stripe. Every stripe has the same `k` and `n`. The
last stripes carry zero padding, and `body_bytes` gives the exact body length.

Each block picks its part size `S`: the smallest multiple of `ALIGN` that fits
the body in one stripe of `STRIPE_K` source parts, between `S_MIN` and `S_MAX`.
A body above `STRIPE_K * S_MAX` uses `S_MAX` and more stripes. A small block
therefore costs about its own size per node, not `STRIPE_K` parts of `S_MAX`.
`coding.part_bytes` carries `S`, so the rule adds no field.

On the wire a source part ends where the body ends, so padding never crosses:

```text
g            = s * k + j                        // global source index of slot j in stripe s
wire_len(i)  = S                                if j >= k   (parity)
             = clamp(len(B) - g * S, 0, S)      if j < k    (source)
```

Every part payload MUST contain exactly `wire_len(i)` bytes. The receiver
appends zeros up to `S` before it hashes the leaf, decodes, or re-encodes, so
the tree, the codec, and assembly see only padded parts. A source part with
`wire_len(i) = 0` holds only padding. It exists only when `stripes * k > K >= 1`.
The receiver holds a padding part from admission, and no node announces,
requests, or sends it.

An empty body (`len(B) = 0`) has no parts: `K = stripes = k = n = 0`. The block
is the header and the coinbase, which `HeaderMeta` already carries. A node
assembles it when it admits the metadata and then sends `BlockDone`. No `Have`,
`Want`, `Part`, or `StripeDone` exists for it; one that names it names an index
or stripe outside the layout and returns `Disconnect`.

`0 < k < n` for a non-empty body, `stripes <= MAX_STRIPES`, and
`len(canonical(header)) + len(B) <= MAX_BLOCK_BYTES` MUST hold. The profile
fixes `S_MIN`, `S_MAX`, `ALIGN`, `STRIPE_K`, `parity_parts`, and `MAX_STRIPES`.
A proposer cannot choose coding parameters. Receivers MUST recompute `S`,
`stripes`, `k`, and `n` from `body_bytes` and the profile, and MUST reject a
mismatch.

### Codec

The codec is systematic Reed–Solomon over GF(2^16). The profile MUST fix the
construction bit for bit. W2's candidate is the construction that the
[`reed-solomon-simd`](https://crates.io/crates/reed-solomon-simd) crate,
version 3.1, implements: `k` original shards and `n - k` recovery shards of `S`
bytes each. The prototype measured this construction. The
[RFC 5510](https://www.rfc-editor.org/rfc/rfc5510.html#section-8) Vandermonde
construction remains an alternative. Either choice MUST publish test vectors
before implementations claim interoperability. The first draft's `k = 2`
vector belongs to the Vandermonde construction and does not apply to the W2
candidate.

### Decoding

The decoder MUST verify membership before it incorporates a part. It MUST
incorporate each distinct index at most once. It decodes a stripe once it holds
`k` distinct verified parts of that stripe. It MAY decode incrementally. It MUST
bound CPU and memory per stripe and across the node. Stripes bound one decode
job to `k` parts. Forwarding a verified part MUST NOT wait for decoding.

### Proposer preparation

The proposer MUST finish every stripe's codeword and the part root before it
publishes `HeaderMeta`. It SHOULD precompute them for the candidate body while
mining. A change to the body, including its coinbase, invalidates that cached
encoding. A header-only change does not change the encoded body or the part
root. `C` still binds the root to the final block through the coinbase.
Bare-header propagation MAY proceed through headerchain while encoding runs. It
MUST NOT authorize part processing.

A future profile MAY commit per-stripe roots in the header, so the proposer can
send each stripe as soon as it is encoded. W2 does not.

### Metadata fields

`HeaderMeta` carries all of these values, authenticated via the post-NU7 v6
coinbase commitment `C` (`decision-1`, `decision-9`, `01-wire.md` §3):

```text
HeaderMeta {
    header: ConsensusHeader,       // full canonical consensus header (decision-2)
    binding: CoinbaseBinding,      // coinbase tx + txid & auth-data index-0 paths + light_client_root
    coding: Coding,                // codec, body_bytes, tx_count, part_bytes, stripes, k, n
    part_root: Hash,               // 32-byte BLAKE3 KDF Merkle root over all stripes*n parts
    proposer_node: NodeId,         // transport identity that sends the seed parts
}
```

`body_bytes` MUST equal `len(B')` (the serialized transactions after the
coinbase, without the transaction-count prefix; `tx_count` is carried in
`Coding`). Admission MUST validate the canonical coding tuple and check
`len(canonical(header)) + len(coinbase) + compact_size(tx_count) + body_bytes <= MAX_BLOCK_BYTES`
with checked arithmetic before headerchain work or assembly state allocation.

`HeaderMeta` names no seeds and is the same on every connection. The proposer
authorizes its seeds on the data stream with flagged `Have`s (section 3).

The block identifier (`block_id`) is the consensus hash of `header` across every
BPv1 message. Because `C` is embedded in the mined coinbase inside the PoW
header, Ed25519 proposer signatures, variant identifiers, and metadata conflict
rules are unnecessary and removed in revision 3.

Coded propagation covers only recent blocks. Let `top` be the highest of this
node's verified tip height, its best header-valid height, and the height of a
block it proposed. `top` never decreases. A header that headerchain retains
but has not selected, such as a fork or a future-dated header, does not raise
`top`. A node MUST NOT admit
metadata for a block at or below `top - PROPAGATION_DEPTH` (strictly
`height + PROPAGATION_DEPTH > top`). A node also MUST NOT admit metadata for a
block more than `PROPAGATION_DEPTH` above its verified tip. While a node catches
up, header sync raises `top` far above the verified tip, and such a block cannot
commit until its ancestors arrive. Such blocks use the existing block-download
path. A node retires a block once it falls to or below `top - PROPAGATION_DEPTH`.
The verified-tip bound applies at admission only. A reorg can lower the verified
tip, and a block admitted above the new bound keeps its slot until it completes
or reaches its deadline.

The coinbase commitment binds `proposer_node` to the block. A node therefore
accepts relay flags only from the connection authenticated as `proposer_node`,
and only that connection may flag every index of a stripe for push.

### Bind the proposer to the proof of work

The proposer commits `C = T("commit", chain_id || profile_id || coding || part_root || proposer_node)`
in a single zero-value transparent `OP_RETURN` output of the post-NU7 v6
coinbase transaction before mining:

```text
6a 25 || ASCII("BPV1") || 01 || C[32]
```

`6a` is `OP_RETURN`; `25` directly pushes the following 37 bytes (total script
length 39 bytes, serialized transparent output 48 bytes). The byte after
`"BPV1"` is the commitment format version (decision-25). BPv1 defines version
`01` only; the profile lives inside `C`, not in this byte.

An output is a version-01 commitment only if its value is zero and its script
is exactly the form above. Readers MUST ignore every other output, including
another version, length, push encoding, or value. A coinbase MUST hold exactly
one version-01 commitment for a binding. With none or two, the block has no
binding: it travels by block sync, and its `HeaderMeta` returns `Disconnect`
(close-4). A later version is negotiated with the BPv1 service version. During
a transition a proposer MAY write one output per version, and each reader reads
only the versions it supports.

A miner mesh coinbase MAY also carry its own output
([#2666](https://github.com/zakura-core/zakura/pull/2666)), which names a
gateway. BPv1 ignores that output, and it grants no BPv1 rights. When one node
both builds the template and announces the gateway, `proposer_node` equals the
gateway identity.

`CoinbaseBinding` carries the canonical v6 coinbase transaction, its index-0
txid Merkle path, its index-0 ZIP 244 authorizing-data Merkle path, and
`hashLightClientRoot`. The verifier MUST recompute both the transaction Merkle
root and `hashBlockCommitments = BLAKE2b("ZcashBlockCommit", hashLightClientRoot || hashAuthDataRoot)`
using the chain implementation for the admitted height, and verify both against
the admitted header. The profile bounds coinbase bytes and path depth before
allocation. The coinbase byte bound is the consensus bound, `MAX_BLOCK_BYTES`:
consensus has no separate coinbase limit, so a tighter bound would send valid
blocks to slower relay. A large coinbase travels whole in `HeaderMeta`, so its
miner pays for it on every connection's control stream. The coinbase byte bound
also bounds the coinbase's outputs and
script bytes. Pre-NU7 blocks and pool-built coinbases
without a commitment use existing block propagation.

The verifier MUST validate the coding layout first, then submit `header` to
headerchain (checking proof of work and all contextual header rules), and only
after header admission verify the coinbase commitment and allocate assembly
state. Section 5 states how the verifier treats copies of one block's
`HeaderMeta` from other peers.

### Authenticate parts and reconstruction

The Merkle tree has `next_pow2(stripes) * next_pow2(n)` leaf positions. Wire
index `i` in stripe `s = i / n` occupies position:

```text
position(i) = s * next_pow2(n) + (i mod n)
depth       = log2(next_pow2(stripes)) + log2(next_pow2(n))
```

Real positions hold part leaves. Every other position holds a pad leaf. The
subtree over stripe `s`'s `next_pow2(n)` positions has root `stripe_root(s)`.
The construction MUST bind part position and coding parameters:

```text
encoding_id = T("encoding", coding)
leaf[i]     = T("leaf", encoding_id || u32(i) || zero_pad(payload[i], S))
pad[p]      = T("pad", encoding_id || u32(p))
parent      = T("node", left || right)
```

An empty body's tree has `next_pow2(0) * next_pow2(0) = 1` position, which
holds a pad leaf, so its `part_root = pad[0]`. Every empty body has the same
coding tuple, so every empty block has the same `part_root`.

A proof contains exactly `depth` siblings in leaf-to-root order. Bit `j` of the
position selects the orientation at level `j`: zero places the current hash on
the left. Verification MUST require the exact proof shape for index
`i < stripes * n`. `part_root` commits to every data and parity part of every
stripe. The first `log2(next_pow2(n))` levels of a verified proof yield
`stripe_root(s)`.

A valid proof establishes membership in the signed root. It does not establish
that the parts of a stripe form a valid codeword or a valid block.

After a node holds `k` distinct valid parts of stripe `s`, it MUST decode the
stripe, re-encode all `n` parts, and recompute `stripe_root(s)`. The result MUST
equal the stripe root that the verified proofs establish. The node MUST bound
this work and MUST NOT search combinations of parts after a failure. A failed
check stops coded propagation for the block. The node MUST then use the existing
block-download path directly. It MUST NOT request more parts of a failed
codeword.

After every stripe passes, the node MUST concatenate the stripes' source data,
check that the padding after `body_bytes` is zero, and assemble the admitted
header and body in the existing canonical block format. It MUST enforce the
total block-size bound and reject trailing or noncanonical body bytes.

A node MAY serve regenerated parts of a stripe only after that stripe passes its
re-encode check. A node other than the proposer MUST send `BlockDone` only after
assembly succeeds. The existing validator independently decides whether the block is valid, including
whether the body's transaction commitments match the admitted header. Coding
verification does not establish consensus validity and MUST NOT substitute for
that validation.

## 3. Seeds, push, and route state

### Seeds

A seed is a part the proposer sends a neighbour unasked. The proposer builds
its seed plan when the block is solved, from its live sessions and path
estimates (decision-19, section 7). After it queues `HeaderMeta` and
`BlockDone` to a neighbour, it writes a `Have` with the push flag on that
neighbour's data stream, then the parts it flags. The relay flag marks the
neighbour's _pushed seeds_: the deal-1 seeds the neighbour pushes onward. The
neighbour that receives a part as a pushed seed is that part's _holder_.
deal-2 cover copies and direct copies carry the push flag only. Relay-flagged
seeds go first. A flagged `Have` lists at most `MAX_HAVE_INDICES` indices, so
a longer batch goes out as several flagged `Have`s, each ahead of its own
parts. The proposer skips an index that the neighbour announced or sent, or
that the proposer already flagged or sent on the connection, and every index of
a stripe or block the neighbour finished. It sends no unflagged `Have`.

The flag travels on the same ordered stream as the parts, so it always
reaches the neighbour before them, even when the proposer's `HeaderMeta` is
still in flight on the control stream. A neighbour that admitted the block from
another peer's `HeaderMeta` therefore accepts the proposer's seeds at once.

Two hops push. The proposer pushes seeds to its neighbours, and each holder
pushes its pushed seeds to its own peers. Every later hop pulls parts that its
neighbours announce. On a full mesh the two pushed hops reach every node.
Pulls fill what they miss, such as sparse topologies and lost holders.

A neighbour holds flagged seeds as pending claims, like any flagged push
below. Each late seed is released to the other announcers once. The proposer's
session end releases its pending seeds.

### Holder push

A holder pushes each of its pushed seeds once it holds a verified copy of the
part from any source, including a part it regenerated from a checked stripe
(decision-24). It pushes the part once to each peer that meets all of these
conditions:

- the peer exchanged `HeaderMeta` for the block with the holder;
- the peer is not the block's proposer;
- the peer has not announced the part, sent it, or reported `StripeDone` for
  its stripe or `BlockDone` for the block.

The forwarding skip rules in section 6 also apply. The holder first writes a
`Have` with the push flag that lists the indices it will push to the peer. It
then writes those parts behind the flagged `Have` on the same data stream. The
flag therefore arrives before its parts, and the receiver authorizes each
pushed part from the `Part` prefix alone. The flagged `Have` also announces the
parts, so the holder sends no second `Have` for them.

A node other than the proposer MUST flag and push only its own pushed seeds,
whichever source delivered them. It MUST NOT push any other part. It MUST NOT
flag more than `n - k` indices of one stripe on one connection, and it MUST NOT
set the relay flag. It MUST NOT flag an index that it already announced on the
connection; a pushed seed learned after its unflagged `Have` went out is not
pushed to that peer. A flagged `Have` lists at most
`MAX_HAVE_INDICES` indices, so a longer push goes out as several flagged
`Have`s, each ahead of its own parts. When the peer announces or sends a part
before its queued push is written, the holder skips the write.

A receiver counts flags per stripe of an admitted block per connection when it
reads the flagged `Have`. Its _push bound_ on a connection is `n` distinct
indices per stripe from the connection authenticated as `proposer_node`, whose
seeds carry its proof of work, and `n - k` from any other connection. Relay
flags count separately, at most `n - k` per stripe, and only from
`proposer_node`. Each of these returns `Disconnect` (close-20):

- a push flag past the connection's push bound;
- a relay flag from a connection other than `proposer_node`;
- a relay flag past `n - k` in one stripe.

Any peer MAY push within its bound. Recognizing a miner mesh gateway grants
nothing more. `n - k` is the most parts one holder can lose without stalling a
stripe. At W2 with a 2 MB block, it is 22 parts, about 0.7 MB per stripe per
connection. The receiver's relay-flagged indices become its pushed seeds.

The receiver holds flagged indices as pending claims: it does not pull them
until they turn late, and it releases a late push like a late pull (section
7). Each flagged `Have` starts its own late time from its own estimated
arrival, so a later flag never delays an earlier one. A pushed part that crosses another peer's copy of the same index returns
`Drop` as surplus.

BPv1 defines no lanes. The subscription rules below use lanes and
apply only to the subscription extension.

### Subscriptions

A subscriber asks a publisher to push a lane. The scope of a subscription is
`Standing`, `Block(block_id)`, or `Proposer`. A standing subscription covers
each part of the lane in every admitted block while it lasts. A block
subscription covers the lane's parts in one admitted block. A proposer
subscription names no lane and covers every part of each admitted block whose
`proposer_node` is the publisher; rules below that name `(scope, lane)` apply to
its single key `(Proposer, all)`.

Each connection direction keeps one subscription state per `(scope, lane)`:

| Subscriber state | Entered when | Parts wanted |
| --- | --- | --- |
| `Idle` | Initially, or when the subscriber reads the subscription's `SubscriptionEnd` | No |
| `Live` | The subscriber sends `Subscribe` | Yes |
| `Draining` | The subscriber sends `Unsubscribe` | Yes |

Every subscription follows one lifecycle:

1. The subscriber sends `Subscribe` only in `Idle`. The state becomes `Live`.
2. The publisher accepts or refuses the subscription when it processes
   `Subscribe`. It writes `Refuse` on the data stream for a refusal.
3. The publisher MAY later refuse an accepted subscription with reason `Shed`.
   It then stops queueing new parts under the subscription. It sends at most one
   `Refuse` per subscription.
4. A subscriber that reads `Refuse` in `Live` MUST send `Unsubscribe`. A
   `Refuse` read in `Draining` changes nothing.
5. The subscriber sends exactly one `Unsubscribe` per `Subscribe`. The state
   becomes `Draining`.
6. The publisher answers every `Unsubscribe` with exactly one `SubscriptionEnd`
   on the data stream. `SubscriptionEnd` travels behind every part that the
   publisher queued for the subscription. The publisher then forgets the
   subscription.
7. The subscriber returns to `Idle` when it reads `SubscriptionEnd` in
   `Draining`.

Only `SubscriptionEnd` ends a subscription, and only `Unsubscribe` causes it.
Neither side ends a subscription on a timer. A publisher keeps a refused
subscription until its `Unsubscribe` arrives, so every crossing has a defined
answer. `Refuse` and `SubscriptionEnd` share the data stream, so a
subscription's `Refuse` always arrives before its `SubscriptionEnd`. Section 4
explains why this ordering keeps in-flight parts legal.

A subscriber MUST keep at most `MAX_OPEN_SUBS` subscriptions outside `Idle` at
each peer. The publisher holds each subscription from its `Subscribe` until its
`SubscriptionEnd` enters the data stream's ordered output. It then forgets the
subscription's key, before the subscriber can leave `Draining`. The publisher
therefore never holds more subscriptions than the subscriber's limit, and never
holds a key that the subscriber may subscribe to again. Each held subscription
reserves the output for its `Refuse` and its `SubscriptionEnd`. That output
reservation lasts until the write completes.

A publisher that retires a block stops pushing its parts. It keeps the block's
subscriptions until their `Unsubscribe`s arrive, and it MAY refuse them with
reason `Shed`. A subscriber that retires a block MUST send `Unsubscribe` for the
block's `Live` subscriptions. It keeps them in `Draining` until their
`SubscriptionEnd`s arrive.

### Wanted set

Each node keeps, per connection, the parts it wants from that peer. Part `i` of
admitted block `b` is wanted from peer `p` when at least one of these holds:

1. A subscription at `p` whose scope covers `b` and whose lane contains `i` is
   `Live` or `Draining`. A proposer subscription contains every lane.
2. A live `Want` sent to `p` for `b` has `i` in its allowed set and unspent
   count.
3. `p` sent a `Have` for `b` with the push flag that lists `i`. The proposer's
   seeds and direct copies arrive this way.

Each wanted part is wanted once from each peer. The receive record holds, per
peer and block, every index received from that peer. A second copy of
`(block_id, index)` from the same peer violates send-once.

A part covered by rule 1 or 3 does not spend a `Want`'s count. Otherwise it
spends the count of the one live `Want` at `p` whose allowed set contains `i`.
Section 5 requires live `Want`s at one peer to have disjoint allowed sets, so
this `Want` is unique. When the two peers' views of a subscription differ, the
receiver attributes a part to the subscription and the sender attributes it to
the `Want`. The receiver then counts fewer `Want` answers than the sender sent,
never more.

**Reservation messages.** `Subscribe`, `Unsubscribe`, `Refuse`,
`SubscriptionEnd`, `Want`, and `WantEnd` match reservation state by key, whether
or not the block they name is admitted, retired, or unknown. They never enter
the early record or the retired path. Section 5 gives the answer for an unknown
or retired block: `Refuse` with reason `UnknownBlock` for a block subscription,
and `WantEnd` with `sent = 0` for a `Want`.

**Unknown blocks.** Another message can name a `block_id` that this node has not
admitted: its metadata is still in flight or under admission, the peer runs more than `PROPAGATION_DEPTH` blocks behind, or
the peer is lying. The receiver cannot yet check such a message.

- On the data stream, the receiver holds `Part` and `Have` messages in a
  per-peer early record, with a byte bound per peer, a node-wide byte bound, and
  an age bound `EARLY_AGE`. When a peer's record is full, the receiver pauses
  reading that peer's data stream until entries leave the record. Pausing is not
  a violation.
- On the control stream, the receiver MAY hold `StripeDone`, `BlockDone`, and
  `Advert` in a separate small per-peer record. It MUST NOT pause the control
  stream for this record, because the metadata that resolves the block can sit
  behind the paused message. Messages that do not fit return `Drop`.

When the block is admitted, the receiver replays the held messages through the
ordinary checks. A replayed part that is no longer wanted returns `Drop`,
because the subscription state can change between arrival and replay. Entries
that reach `EARLY_AGE` return `Drop`.

A flagged `Have` that leaves the early record without replay, because the
record was full, the entry aged out, or the metadata was rejected, leaves its
parts behind it on the stream. The receiver remembers that it lost this
peer's flags for the block, in a per-peer set bounded like pending metadata.
After admission, a part from that peer that no push slot or `Want` authorizes
opens a push slot itself while its stripe has fewer push slots from the peer
than the connection's push bound. Past that bound it returns `Disconnect` as
an unwanted part. Until the stripe reaches the bound, the receiver sends the
peer no `Want` for it, so a pushed part never answers a `Want`. The push bound
therefore holds. A lost relay flag is not recovered, so its part does not
become this node's pushed seed.

**Retired blocks.** A node keeps a retired block's `block_id` while the block's
height is above `top - PROPAGATION_DEPTH`. When it retires a block, it replaces
each peer's records for the block with two counters: the parts not yet received
from the peer, and the indices the peer has not yet announced. A peer without
records for the block, for example on a later session, starts both counters at
`stripes * n`. Each later `Part` spends one unit of the first counter, and each
later `Have` index one unit of the second. They return `Drop` without allocation
or proof verification. A peer that exhausts either counter sent a part or index
twice on this connection, which returns `Disconnect`. Other announcements for a
retired block return `Drop`. A block that leaves the retired record is below
this node's propagation window, so this node never admits it again. A peer that
still admits it runs far behind; the early record bounds its traffic.

**The bound.** Any peer, honest or not, can send at most one copy of each part
that this node subscribed to or asked for, plus the parts the peer flagged
within its push bound: `n` per stripe from `proposer_node` and `n - k` from any
other peer, for each admitted block. Data-stream messages about unknown blocks
add at most the per-peer early-record bound per
`EARLY_AGE`. Control-stream announcements about unknown or retired blocks are
small, and their cadence in section 5 records floods. Total receive bytes are
therefore bounded by this node's own subscriptions and `Want`s over the
retained blocks, `(n - k) × S` pushed bytes per stripe per connection (`n × S`
from the proposer), and the early allowance. A byte budget adds nothing to
this bound. A per-peer byte budget per block MAY still serve as defence in
depth.

### Node state

Each node maintains these bounded structures:

```text
meta[b]                          // admitted metadata and layout
push_flags[peer][b]              // indices the peer flagged (relay flags apart), or this node flagged to it
copies[peer][b]                  // as the proposer: direct copies waiting for an idle data writer
store[b][i]                      // verified or regenerated parts
receive_record[peer][b]          // indices received from the peer
send_mark[peer][b]               // indices queued to the peer
subs_in[peer][scope][lane]       // this node subscribes: Live | Draining
subs_out[peer][scope][lane]      // this node publishes: accepted | refused
wants_out[peer][want_id]         // block, stripe, allowed set, count, spent
wants_in[peer][want_id]          // Wants this node serves
peer_view[peer][b]               // Haves, StripeDone, BlockDone, parts written
early[peer]                      // messages about unknown blocks
adverts[peer][b]                 // advertised arrival delays and wait
learner[lane]                    // parent, candidates, lags, streaks (section 7)
retired[b][peer]                 // remaining late parts and indices per peer
```

Every structure MUST have count and byte bounds. Peer-provided keys and counts
MUST NOT raise these bounds. Reservations (`subs_in`, `subs_out`, `wants_out`,
and `wants_in`) have count bounds only: their ends release them, never a timer.
The other structures also have age bounds. Session retirement discards every
per-peer structure, including subscriptions, `Want`s, send marks, and receive
records.

## 4. Service layout and admission

### Streams

BPv1 is one `zakura-network` service. Its
[service session](../design/service-sessions.md) has two persistent ordered
streams carrying seven messages (`decision-13`):

- The **control stream** carries `HeaderMeta`, `Want`, `StripeDone`, and
  `BlockDone`.
- The **data stream** carries `Part`, `Have`, and `WantEnd`.

`Subscribe`, `Unsubscribe`, `Refuse`, `SubscriptionEnd`, and `Advert` from
revision 2 are omitted in BPv1 (`decision-13`).

The control stream MUST have higher write priority than the data stream. Data
queues MUST NOT block control traffic.

The two streams impose no order on each other. The protocol places each message
by the ordering it needs:

- **Ends travel behind parts.** `WantEnd` follows, on the FIFO data stream,
  every part its `Want` exchange authorized. No part of an ended `Want` can
  follow its `WantEnd`. The receiver closes the reservation exactly when it
  reads `WantEnd`.
- **Haves travel behind parts.** A `Have` queues behind data already waiting on
  the link so announcements reflect sender queue drain.
- **Push flags travel ahead of their parts.** A `Have` with the push flag
  queues before every part it flags, so a pushed part never overtakes its
  authorization.
- **Stop signals overtake parts.** `StripeDone` and `BlockDone` travel on the
  control stream so they reach the sender before its queued parts drain.

A message can arrive before the metadata of the block it names. Section 3's
early record handles that case.

### Message table

The service declares one message table per stream, as the
[message regulation design](../design/peer-message-regulation.md#message-tables)
describes. Payload caps below are W2 values (`01-wire.md` §1).

| Discriminator | Message | Stream | Role | Answers or ends | Payload cap |
| --- | --- | --- | --- | --- | --- |
| 1 | `HeaderMeta` | control | Announcement | — | 2,006,278 |
| 4 | `Want` | control | Request | — | 51 |
| 5 | `StripeDone` | control | Announcement | — | 36 |
| 6 | `BlockDone` | control | Announcement | — | 32 |
| 8 | `Part` | data | Item | Answers `Want` or a push flag | 66,085 |
| 9 | `Have` | data | Announcement | — | 16,419 |
| 11 | `WantEnd` | data | Response | Ends `Want` | 7 |

### Results and filter order

Admission returns `Continue`, `Drop`, `Disconnect`, or `LocalFault`, with the
meanings in the regulation specification. Capacity waiting belongs to the
receive or serving loop and produces no result. The first draft's `Delay` result
no longer exists.

Every message passes the regulation filter order:

```text
frame -> cadence where declared -> reservation precheck -> bounded decode
      -> reservation match -> required stateless verification -> handler policy
```

For `Part`, the reservation precheck reads the fixed prefix
`(block_id, index)`. It checks the wanted set and the receive record before the
node reads the proof and payload or verifies anything.

A node MUST return `Disconnect` only for an event that no conformant peer can
cause. Each `Disconnect` rule in section 5 names the sender obligation that
makes the event unambiguous. Crossings, local eviction, reorganization, and
local capacity MUST NOT establish misconduct. An ambiguous event returns `Drop`
and produces a bounded diagnostic record.

## 5. Message rules

The following limits apply to every BPv1 message:

```text
S_MIN; S_MAX       = 4,096; 65,536 bytes; S is per block (section 2)
STRIPE_K           = 64
MAX_N              = 86          // STRIPE_K + ceil(STRIPE_K / 3)
MAX_STRIPES        = 512
MAX_PARTS          = 44,032      // MAX_STRIPES * MAX_N
MAX_PROOF_DEPTH    = 16          // log2(128) + log2(512)
MAX_HAVE_INDICES   = 4,096
MAX_WANTS_INFLIGHT = 64          // live Wants per connection direction
MAX_OPEN_SUBS      = 1,024       // subscriptions outside Idle per connection direction
PROPAGATION_DEPTH  = 16          // blocks below the highest admitted height
MAX_HEADER_BYTES   = 4,096
MAX_BINDING_BYTES  = 2,002,086   // coinbase up to MAX_BLOCK_BYTES
```

`StripeDone`, `BlockDone`, and `Advert` that name an unknown or retired block
charge one per-connection announcement bucket: capacity = 256 messages, refill
= 64 messages/s, on_empty = record and forward. Floods of these small messages
are therefore visible without risking an honest peer.

A scope encodes as `u8(0)` for `Standing`, `u8(1) || block_id[32]` for `Block`,
or `u8(2)` for `Proposer`. In the subscription extension a lane encodes as a 32-byte `NodeId`.
Section 8 lists every field in transmission order.

### Control stream

#### `HeaderMeta` — Announcement, discriminator 1

- **Frame**
  - payload cap = 2,006,278 bytes
- **Decode**
  - header length = 1..=4,096 bytes
  - binding length = 1..=2,002,086 bytes
  - codec = W2 codec identifier; `part_bytes = S` for `body_bytes` (section 2)
  - exact consumption
- **Verify**
  - `stripes`, `k`, and `n` equal the values recomputed from `body_bytes`
  - `stripes <= MAX_STRIPES`
  - header plus body bytes <= `MAX_BLOCK_BYTES`, with checked arithmetic
  - key-binding envelope within the profile's coinbase, script, output, and
    path bounds
- **Admission**, in this order
  - headerchain admits the header, including proof of work and contextual
    difficulty
  - the binding's txid and auth-data paths match the admitted header
  - the coinbase holds exactly one version-01 commitment, and its `C` equals
    the recomputed hash (section 2)
- **Cadence**
  - at most one `HeaderMeta` per block per connection
  - capacity = 64 messages; refill = 16 messages/s; on_empty = record and
    forward
  - the per-peer and node-wide pending-metadata bounds in section 1

A node MUST send `HeaderMeta` for a block at most once per connection. It
MUST forward only metadata it admitted, and only within the propagation window
in section 2. It SHOULD forward admitted metadata promptly to every peer, including peers without subscriptions. Forwarding MUST NOT wait
for parts or a per-hop request. It SHOULD NOT send metadata to a peer that
already sent it the block's metadata. The proposer SHOULD write `HeaderMeta` to
every peer before it writes any part.

A header that headerchain rejects returns `Disconnect` when headerchain
classifies the failure as peer-caused. A missing parent invokes bounded
recovery and is not a violation. A layout mismatch, invalid binding, or failed
commitment returns `Disconnect`: an honest relay forwards only admitted
metadata. A repeat of a block's metadata from the same peer returns `Disconnect`
while this node holds the block in admission, admitted, or retired. The sender
keeps the block retired at least until the block leaves its own window, and it
never admits a block outside that window, so it cannot send the metadata twice.

The header commits the coinbase, and the coinbase commits `C`, so a block has
one valid `HeaderMeta`. The consensus hash covers only the header, so other
peers may send different copies under one hash while the first copy is in
admission:

- The node checks each copy's layout when the copy arrives.
- A copy that fails its commitment check returns `Disconnect` for every peer
  that sent an equal copy, first or later. The check depends only on the copy
  and the admitted height, so an honest relay never forwards a copy that fails
  it.
- When the first copy fails, the node checks one differing copy if its
  metadata budget kept that copy. The kept copy gives way to any first copy
  that needs the room.
- Every other duplicate from another peer returns `Drop`, including each copy
  that arrives after the block is admitted.

Metadata for a retired block, or for a block outside the propagation window,
returns `Drop`.

`HeaderMeta` authorizes no parts. The proposer's flagged `Have`s on the data
stream authorize its seeds (section 3).

#### `Want` — Request, discriminator 4

- **Frame**
  - payload cap = 51 bytes (`want_id: u32 || block_id: [u8; 32] || stripe: u32 || requested: [u8; 11]`, `decision-14`)
- **Decode**
  - `want_id != 0`
  - `requested` bitmap = 11 bytes; bits at or above `MAX_N` are zero; popcount >= 1
  - exact consumption
- **Verify**, when the server has admitted the block
  - `stripe < stripes`
  - `requested` bits at or above `n` are zero
- **Reservation**, at the server
  - `want_id` differs from every live `Want` on this connection
  - the requested set is disjoint from every live `Want` for the same block and
    stripe on this connection
  - live `Want`s <= `MAX_WANTS_INFLIGHT`; the server serves and traces up to
    twice that limit and returns `Disconnect` beyond it
- **Capacity**
  - the serving task acquires output capacity before it produces parts
  - the reader admits the request as a commitment without waiting

A `Want` asks for the exact set of parts marked in `requested` for one stripe
(`decision-14`: no separate `count` field). The requester MUST create the
reservation before it sends the request and keep live `Want`s at one peer
disjoint for each block and stripe.

The server MUST end every admitted `Want` exactly once with `WantEnd` on the
data stream, behind the parts it sent for that `Want`. Each part MUST come from
`requested` and MUST obey send-once. It serves the parts it holds when it
processes the request, including regenerated parts of checked stripes, and MAY
send fewer than `popcount(requested)` parts, including none. A `Want` for a
block the server has not admitted or has retired ends with `sent = 0` and the
matching reason.

An out-of-range stripe or requested bit returns `Disconnect`. A reused live
`want_id`, an overlapping live requested set, or more than twice
`MAX_WANTS_INFLIGHT` live `Want`s returns `Disconnect`. At the server, a `Want`
stays live until the server writes its `WantEnd` to the transport. The
requester keeps the `Want` live until it reads that `WantEnd`, so it cannot
reuse an id or a set that the server still holds.

A requester that no longer needs the parts keeps the reservation live until
`WantEnd` or session retirement. A local pull timeout (`SLOW_X * estimated` or
hard deadline `8 s + outstanding_bytes / 100 KB/s`) MAY send another `Want` to
another server; it MUST NOT revoke the first reservation.

#### `StripeDone` — Announcement, discriminator 5

- **Frame**
  - payload cap = 36 bytes
- **Decode**
  - exact consumption
- **Verify**, when the receiver has admitted the block
  - `stripe < stripes`
- **Cadence**
  - at most once per block and stripe per connection

`StripeDone` states that the sender holds `k` distinct verified parts of the
stripe. The sender MUST send it only when this holds. It SHOULD send it at once
to every peer that knows the block. The receiver SHOULD cancel queued unsent
parts of the stripe to the sender. `StripeDone` ends no subscription and no
`Want`: parts already in flight stay wanted and return `Drop` as surplus.

An out-of-range stripe returns `Disconnect`. A repeat returns `Disconnect` while
the receiver retains the block's peer view. A `StripeDone` for an unknown or
retired block follows section 3, like `BlockDone`.

A false `StripeDone` only reduces what the sender receives. It can also attract
repair `Want`s, which the sender must still end.

#### `BlockDone` — Announcement, discriminator 6

- **Frame**
  - payload cap = 32 bytes
- **Decode**
  - exact consumption
- **Cadence**
  - at most once per block per connection

`BlockDone` states that the sender assembled the block under section 2, or that
the sender is the block's proposer, so it holds every part (decision-20). It is
terminal and advisory for the block on
the connection. The receiver MUST stop queueing parts of the block to the
sender. It MUST still process control messages and future-block demand from the
sender. `BlockDone` ends no subscription and no `Want`.

A node SHOULD send `BlockDone` to every peer that knows the block. The proposer
SHOULD send `BlockDone` right after `HeaderMeta`, so no neighbour forwards the
block's parts back to it. A repair round MAY ask a `BlockDone` peer for any part;
a normal pull asks only announcers.

A repeat returns `Disconnect` while the receiver retains the block's peer view.
A `BlockDone` for an unknown or retired block follows section 3: the receiver
MAY hold it in the control-stream early record, and otherwise returns `Drop`.

A false `BlockDone` stops only the sender's own incoming parts. It can attract
repair `Want`s, which the sender must still end. It cannot cancel another
peer's traffic or establish block validity.

_(Discriminators 2 `Subscribe`, 3 `Unsubscribe`, 7 `Advert`, 10 `Refuse`, and
12 `SubscriptionEnd` are reserved / omitted in revision-3 BPv1 seeded pull;
`decision-13`.)_

### Data stream

#### `Part` — Response, discriminator 8

- **Frame**
  - payload cap = 66,085 bytes
- **Decode**
  - sibling count <= `MAX_PROOF_DEPTH`
  - payload = the bytes after the siblings, at most `S_MAX`
  - exact consumption
- **Reservation precheck**, on the fixed prefix `(block_id, index)`
  - an unknown block goes to the data-stream early record
  - a retired block spends the peer's retired part counter and returns `Drop`;
    an exhausted counter returns `Disconnect`
  - `index < stripes * n`
  - the frame's payload length equals the fixed prefix, `depth` siblings, and
    `wire_len(index)` bytes; the frame header gives it before any payload byte
    is read
  - `index` is absent from the peer's receive record; the node records it
  - `index` is wanted from the peer; the node spends a `Want`'s count when
    section 3 attributes the part to a `Want`
- **Verify**
  - sibling count = the layout's `depth`
  - the Merkle proof verifies against `part_root`

The sender MUST send a part only when the receiver wants it: a subscription it
accepted and has not ended covers it, a live `Want` from the receiver allows it
with count left, or the sender flagged the part for push in a `Have` on this
connection. The sender MUST mark a part as sent when it queues it, across
every path. Cancelling a queued part MUST NOT clear the mark. The sender MUST
NOT send a part to a peer from which it received a valid copy. Section 6 lists
the other sends a sender skips.

An out-of-range index, an unwanted part, a repeat from the same peer, or a
failed proof returns `Disconnect`. Honest nodes forward only verified parts.
They send each part at most once per connection, and only under an
authorization that the receiver still holds when the part arrives.

A copy of a stored part from a second peer is legal and returns `Drop`. A part
of a stripe that already decoded returns `Drop` as surplus. A part of a block
this node has finished is still wanted while its authorization lasts, and
returns `Drop`. The receiver MUST bound verification concurrency. Waiting for a
verification worker pauses reading and is not a violation.

#### `Have` — Announcement, discriminator 9

- **Frame**
  - payload cap = 16,419 bytes
- **Decode**
  - `flags` = `0x00`, `0x01` (push), or `0x03` (push and relay); any other
    value returns `Disconnect` (close-1)
  - index count = 1..=4,096
  - indices strictly increasing
  - exact consumption
- **Verify**, when the receiver has admitted the block
  - every index < `stripes * n`
- **Push flags**, when the receiver has admitted the block, counted across the
  block's flagged `Have`s on this connection
  - from `proposer_node`: at most `n` flagged indices per stripe, of which at
    most `n - k` carry the relay flag
  - from any other connection: at most `n - k` flagged indices per stripe and
    no relay flag
- **Cadence**
  - each `(block_id, index)` at most once per connection

`Have` states that the sender holds verified parts. The sender MUST announce
only such parts. It SHOULD batch announcements for at most `HAVE_FLUSH_MS` or
`HAVE_FLUSH_COUNT` indices. The proposer MUST NOT send an unflagged `Have` for
its own block (decision-20, bpv1-9). The receiver uses `Have`s to choose pulls,
to skip sends, and to measure lag.

A node SHOULD send the block's proposer an unflagged `Have` for each part it
verifies, received or reconstructed, unless the proposer sent it that part
(decision-32). It sends the `Have` at once, without pacing, and also after the
proposer's `BlockDone`. It MUST NOT push or send the proposer a part. The
proposer accepts these `Have`s under the usual cadence and retired-counter
rules, records them for its trace, and never pulls for them.

A `Have` with the push flag states that the sender will push the listed parts
behind it on the data stream (section 3). A holder sets it for its own pushed
seeds. The proposer sets it for its seeds and direct copies, and adds the relay
flag to the receiver's pushed seeds. Only the proposer sets the relay flag. The
flagged `Have` is the only `Have` for those indices on the connection. A sender
MAY skip a flagged part it has not yet written, when the receiver announced
or sent it meanwhile; the receiver releases the unfilled flag when it turns
late and never closes for it.

**Pacing (decision-26).** A node SHOULD NOT announce every part to every peer
at once. When it admits a block, it picks the block's _initial group_ the way
main's block gossip does: peers in order of estimated arrival (section 7), with
every fourth slot taken by a random remaining peer. The group's size is main's
advertisement window for the block's body size: `advert_window_small_block`
at or below `advert_window_small_size_bytes`, `advert_window_full_block` at or
above `advert_window_full_size_bytes`, and linear between. The node announces
verified parts to the initial group at once. Every other peer becomes eligible
at admission plus `advert_delay`, and then receives one `Have` of the parts it
still lacks. At both times the node skips a part that the peer announced or
sent, and a stripe or block the peer finished. Flagged `Have`s, `Have`s to the
proposer, seeds, and repair are not paced.

An out-of-range index or a repeated index returns `Disconnect` while the
receiver retains the block's peer view. A push flag past the connection's
push bound, a relay flag from a connection other than `proposer_node`, or a
relay flag past `n - k` in one stripe returns `Disconnect` (close-20). A
`Have` for an unknown block enters the data-stream early record, and a `Have`
for a retired block spends the peer's retired index counter, under section 3. A false unflagged `Have` attracts `Want`s that the
sender must still end; it cannot force the receiver to accept parts. A false
flagged `Have` authorizes at most the connection's push bound per stripe, and
each part must still be a distinct index that verifies against `part_root`.

#### `WantEnd` — Response, discriminator 11

- **Frame**
  - payload cap = 7 bytes
- **Decode**
  - reason = `Served`, `UnknownBlock`, or `Retired`
  - exact consumption
- **Reservation**, at the requester
  - `want_id` names a live `Want`
  - `sent <= popcount(requested)`
  - `sent >=` the parts this node attributed to the `Want`
  - consumes the reservation

A `WantEnd` that names no live `Want` or breaks either count rule returns
`Disconnect`. The server writes one end per admitted `Want` behind its parts,
and section 3's attribution never counts more answers than the server sent.
After `WantEnd`, the requester MAY ask another server for the parts it still
lacks.

## 6. Forwarding, completion, and recovery

### Forwarding

When a node verifies a new part, it MUST store the part within assembly bounds.
If the part is one of its pushed seeds, from any source or regenerated from a
checked stripe, it SHOULD queue the part for every peer that section 3's
holder push rule names. In the subscription extension, it
SHOULD queue the part for every peer with an accepted subscription that covers
the part. In both cases it skips the peers that it MUST or SHOULD skip:

- It MUST skip a peer from which it received a valid copy of the part.
- It MUST skip a peer whose send mark already holds the part.
- It MUST skip a peer that reported `BlockDone` for the block.
- It SHOULD skip a peer that announced the part, or reported `StripeDone` for
  its stripe.
- It SHOULD skip a peer that can already decode the stripe: the parts the peer
  announced plus the parts this link wrote to it reach `k`.

The node checks these rules when it queues a part and again when it writes it.
Forwarding MUST NOT wait for decoding or for other parts. The node MAY verify
parts in bounded batches. It also announces the part to its other peers under
the pacing in section 5.

The proposer sends each neighbour the seeds its plan gives it (section 7),
parity parts as well, behind their flagged `Have`s. Its direct copies follow on
each connection once that connection's data writer is idle. It SHOULD NOT
push its own block's parts on standing subscriptions. It SHOULD push every part
of its block on its accepted proposer subscriptions, interleaved with the seed
parts, until each subscriber's `StripeDone` or `BlockDone` stops it.

### Scheduler

A connection's data stream is one FIFO shared by all blocks. The sender SHOULD
write parts in the order it queued them. The node MUST bound per-peer and
node-wide queue bytes. A full queue for one peer MUST NOT block another peer.
QUIC send windows follow the regulation design's sizing target.

### Completion and retention

When a stripe reaches `k` distinct verified parts, the node SHOULD send
`StripeDone` to every peer that knows the block. When the block assembles under
section 2, the node MUST stop issuing pulls for the block and SHOULD send
`BlockDone` to every peer that knows the block. Its outstanding `Want`s stay
live until their `WantEnd`s arrive.

After a stripe passes its re-encode check, the node MAY send the stripe's parts
it never received to accepted subscribers of their lanes. These parts obey
the ordinary skip, send-once, and wanted rules. The node SHOULD NOT send them to
the block's proposer.

A relay SHOULD retain a completed block until every neighbour reported
`BlockDone`, or until the retention bound expires. When it releases the block,
it purges queued parts, ends live `Want`s for the block with reason `Retired`,
and records the block as retired. After a node evicts a block's per-connection
history, it MUST NOT send `HeaderMeta`, `Part`, `Have`, `StripeDone`,
`BlockDone`, or `Advert` for that block on the same connection. The retired
record in section 3 enforces this rule: it keeps the block retired until the
block leaves the propagation window, and no node admits a block outside that
window. Ends of `Want`s and subscriptions remain legal.

### Recovery

Recovery follows a fixed ladder. A block's recovery state never moves backward.

1. **Normal path.** Seeds, holder pushes, and pulls on `Have` fill the
   stripes (section 7). A late seed or flagged push is released like a late
   pull.
2. **Repair.** After the stall interval without a new verified part of the
   block, the node enters repair. The stall interval is `SLOW_X` times one
   part's estimated arrival through the median path, at least 1 ms
   (decision-22). The first repair round releases the body claim once, so
   native block sync races the block; the first commit wins. For each undecoded stripe it sends `Want`s for
   `k - held - pending` parts, with the missing indices split disjointly across
   peers. A repair round plans over the peers that announced the indices and
   every peer that sent `BlockDone`, by earliest estimated arrival
   (decision-20). The proposer is reached through its `BlockDone`. Later rounds wait the stall
   interval times `STALL_BACKOFF^round`
   and, within a round, never ask a peer again for the same index. The node MAY
   use a block subscription to wait for parts a peer lacks now. Rounds repeat
   until the reconstruction deadline, which is the only bound on repair.
3. **Block download.** A monotonic reconstruction deadline starts at metadata
   admission. Partial progress MUST NOT reset it. When the deadline passes, the
   node MUST use the existing full-block download path. It MUST verify the
   result against the admitted header. A failed re-encode check goes to this
   step at once.

The node MUST record the reason and time of each step, the repair bytes, and the
terminal outcome. Reports MUST exclude block-download completion from
normal-path success. Repair MUST share transport and aggregate limits with
normal work.

Delivery requires a reachable honest source of `k` parts of every stripe, or of
the full block. Neither parity nor local route counts prove this condition.
Implementations MUST report when they cannot satisfy it.

## 7. Route learning

This section defines the baseline routing policy. Peers do not negotiate it.
Implementations MAY change it within sections 3–6. The parameter registry in
section 8 holds its values.

BPv1 chooses pull servers, holders, and direct copies by one estimate: the
arrival time of a part through each peer, from its RTT and bandwidth. Rules
below that name lanes or parents apply only to the subscription extension.

### Pull first

Every lane starts with no parent. A lane whose standing parent is the
current block's proposer also counts as having no parent. The node pulls a part
on a `Have` when all of these hold:

- the announcer is not the block's proposer;
- the node neither holds the part nor has a pending pull for it;
- the part's stripe has not decoded;
- the part is not a pending flagged push, the proposer's seeds included;
- the lane has no parent;
- the stripe's held and pending parts number fewer than `k`;
- the announcer has room in its pull window, within the node-wide pull cap;
- the parts committed to the announcer stay within
  `max(2 parts, 2 × bw × rtt)` (decision-21).

`bw` is the windowed max of the peer's delivery-rate samples and the
transport's `rate_in`. When a `Want` ends with parts, the node takes one sample:

```text
transfer = max(t_last − t_sent − min_rtt, t_last − t_first)
sample   = bytes / transfer
```

`t_sent` is when the node queued the `Want`; `t_first` and `t_last` are when the
first and last attributed parts arrived. A sample counts until a later sample,
one to two `RATE_WINDOW`s on, replaces it, or until the session ends.
`RATE_WINDOW` is `PROPAGATION_DEPTH` times the target block spacing (20 minutes),
so the estimate spans many blocks.

A pull is a `Want` whose allowed set holds the announced indices the node still
lacks. A pull is pending from when the node sends it until its `WantEnd`
arrives or `PULL_TIMEOUT_MS` passes. Every `PULL_TICK_MS`, the node stops
counting older pulls as pending. Their reservations stay live. The node MAY
ask another server for a timed-out index. For each undecoded
stripe that still needs parts, it asks the best-ranked announcer with room, or
else the holder with the fewest outstanding requests.

Parity lets a pulling node stop at `k` parts per stripe. It skips slow
neighbours instead of receiving extra bytes.

### Holes under push

A lane with a parent can still miss parts. The node pulls a missing part of
such a lane when one of these holds:

- the parent announced the part;
- the parent announced a later part of the same lane within
  `HOLE_LOOKAHEAD` positions;
- the parent kept announcing other parts for `STARVE_MS` after its last part of
  this lane;
- another neighbour announced the part at least `STARVE_MS` earlier.

Hole pulls share a node-wide cap across blocks.

### Measure lag

The node samples a part when a hash of `(block_id, index)` selects one in
`SAMPLE_EVERY`. For a sampled part, a neighbour's lag is the arrival time of its
copy or `Have` minus the part's first arrival at this node. A candidate's raw
lag for a lane is the median over at least `MIN_SAMPLES` sampled parts of
that lane in the block. The ranking lag is an EWMA of raw lags with weight
`LAG_EWMA`.

The node runs one learning step per block, `LEARN_DELAY_MS` after it assembles
the block. The step waits up to `ADV_WAIT_MS` for its candidates' `Advert`s.

### Arrival delays and cycle avoidance

A node's arrival delay for a lane in a block is the median time from the
block's creation to the first arrival of each of the lane's sampled parts.
BPv1's `HeaderMeta` carries no creation time, so the subscription extension
MUST define that reference again. Parts that the node regenerates count at
decode time. The node advertises its arrival delays and its forwarding wait in
`Advert`. The forwarding wait is its queued
bytes over an EWMA of its egress rate.

A candidate parent for a lane is a neighbour that is not the block's proposer,
not this node's child for the lane, and not banned. A candidate that is not the
current parent MUST advertise a lower arrival delay than this node's for the
learned block. Its advertised wait adds to its lag. Advertised values use each
node's own clock, so skew between nodes biases rankings. It cannot create a
cycle among nodes that pass this filter in the same block.

When a node's parent for a lane subscribes to that lane at the node, the
node unsubscribes from that parent. This rule removes the cycles that form
across blocks.

### Promote and demote

After `PROMOTE_AFTER` steps with the same leading candidate, the node promotes
the leader, provided the leader's advertised wait is below `PROMOTE_WAIT_MS`. A
step without candidates keeps the streak. Promotion sends `Subscribe` to the new
parent and `Unsubscribe` to the old one in the same step. The old lane stays
wanted while it drains.

A step is late when the parent's ranking lag exceeds the best other candidate's
raw lag plus that candidate's round-trip time plus `DEMOTE_MS`. After
`DEMOTE_AFTER` late steps, the node unsubscribes from the parent. The lane
returns to pull.

When the standing parent of a lane is the current block's proposer, the node
subscribes for this block to the best-ranked other candidate.

### Push where cheap

A publisher bounds its accepted standing subscriptions with a child budget. The
budget counts each accepted lane at its share of a block and sums the shares
across children. The subscription extension defines the share when it defines lanes. A publisher refuses a
subscription that would exceed `CHILD_BUDGET` blocks. Because lanes are shared
across proposers, the budget binds per node, not per proposer. Proposer
subscriptions do not count toward the child budget; a publisher accepts at most
`MAX_PROPOSER_SUBS` of them and SHOULD prefer peers that its operator configured
as miners.

### Seed plan

The proposer builds the seed plan when the block is solved (decision-19). The
plan builds a focused broadcast tree. Its objective is the median, the mean,
and the `t90` of the time each node takes to receive the block. It does not
weight nodes by hash power, and miner mesh status changes no deal.

A neighbour is eligible when its estimated arrival of an even share of the
body, `rtt + share / rate_out`, is within `SLOW_X` times the median. A
neighbour's _seed arrival_ is the estimated arrival of its seeds so far through
the proposer's queue, using `rate_out`. A holder's push is the second hop. It
finishes when the holder's slowest path delivers its last pushed part:

```text
finish(H)          = seed_arrival(H) + rtt(H) + pushed_bytes(H) / push_rate(H)
push_arrival(H, i) = seed_arrival(H) + rtt(H) + pushed_bytes_through(H, i) / push_rate(H)
```

`pushed_bytes(H)` counts the pushed seeds the plan has given `H`, including the
next one. `pushed_bytes_through(H, i)` counts them up to part `i` in `H`'s push
order. `push_rate(H)` is the rate at which `H` delivers one copy of each pushed
part to its slowest peer. The proposer cannot see `H`'s paths, so its own
`rate_in` from `H` stands in. Experiments decide whether holders later report
their own estimate. Whatever the source, the even-share cap below bounds what
a holder that overstates its rate can withhold.

The plan runs three deals:

1. deal-1 gives every non-padding part once, as a pushed seed, to the eligible
   neighbour with the earliest `finish(H)`. A neighbour takes at most
   `cap = min(n - k, ceil(real / eligible))` pushed seeds of a stripe, where
   `real` counts the stripe's non-padding parts and `eligible` the eligible
   neighbours. Under the cap, holders with fast paths take their share first.
   The even share means that any `floor((n - k) / cap)` holders that withhold
   their pushes take at most `n - k` parts of a stripe, so the other holders
   still push `k`, counting padding. With fewer than `ceil(real / (n - k))`
   eligible neighbours, a part left once every neighbour is at its cap goes to
   the neighbour with the earliest seed arrival with the push flag only, so
   nobody pushes it onward.
2. deal-2 copies parts, with the push flag only, to the next-earliest neighbour while
   losing any one neighbour would leave a stripe below `k` parts at the others
   (bpv1-67). Padding counts as held.
3. deal-3 lists direct copies. A neighbour may get a direct copy of any part
   it lacks while it holds fewer than `k` of the stripe's parts. A neighbour's
   copies go latest `push_arrival` first; a part nobody pushes counts as latest.
   The neighbours go closest first, by their seed arrival.

A direct copy carries the push flag only, so it never starts a second holder
push. A direct copy can cross the holder push of the same part; the receiver
drops the second copy as surplus.

The proposer queues each neighbour's deal-1 and deal-2 seeds after its
`HeaderMeta`. It releases that neighbour's direct copies once the
connection's data writer has nothing queued or in flight, so they follow the
neighbour's own seeds and wait for no other connection. Each release takes up
to `COPY_BATCH` copies behind one push-flagged `Have`. Before each copy the
proposer drops it when the neighbour announced, sent, or was sent the part,
reported `StripeDone` for its stripe or `BlockDone` for the block, or can
already decode the stripe (section 6). These checks, not a budget, stop the
copies. The writer counts a frame as written once the transport queues it, so
the transport's send buffer delays the feedback that stops copies.

### Evaluate block outcomes

For each block and node, reports MUST include the time to assembly, the
received payload bytes over the body bytes, new parts by source (push, pull,
repair), surplus parts after `k`, and the terminal step of the recovery ladder.
Across nodes, reports MUST include `t90` (the time by which 90% of non-proposer
nodes assembled the block), the busiest relay's upload over the body, and the
repair and block-download frequency.

## 8. Profile W2 and conformance

### Candidate payload profile W2

W2 is a concrete payload profile for review and conformance tests. It does not
enable a network service. Peers MUST negotiate a complete transport and chain
profile before using it.

All integers are unsigned little-endian. Concatenation has no implicit padding.
Hashes and node identities contain 32 raw bytes. Consensus block identifiers use
the chain hash bytes, not reversed display-hex bytes. The zakura frame header
carries the message discriminator and length; the payload carries no copy. A
parser MUST reject unknown discriminators, nonzero flags, unknown tags, truncated
fields, trailing bytes, and lengths above these bounds before allocation.

| W2 limit | Value |
| --- | --- |
| `S_MIN`; `S_MAX`; `ALIGN`; `STRIPE_K`; `parity_parts(k)`; codec identifier | 4,096 bytes; 65,536 bytes; 64; 64; `ceil(k / 3)`; 2 |
| `MAX_STRIPES`; `MAX_PARTS`; maximum proof depth | 512; 44,032; 16 siblings |
| Maximum body | 2 GiB (`MAX_STRIPES * STRIPE_K * S_MAX`), and never above `MAX_BLOCK_BYTES` |
| `MAX_HAVE_INDICES`; `MAX_WANTS_INFLIGHT` | 4,096; 64 |
| Pushed seeds per stripe per holder; flagged indices per stripe per connection | `min(n - k, ceil(real / eligible))`; `n` from `proposer_node`, of which `n - k` relay, and `n - k` from any other peer |
| Header bytes; key-binding bytes | 4,096; 2,002,086 |
| Coinbase bytes; coinbase Merkle siblings | `MAX_BLOCK_BYTES` (the consensus bound); 32 |
| Control-stream frame cap; data-stream frame cap | 2,006,286 bytes; 66,085 bytes |

These are syntax limits, not assembly reservations or performance guarantees.
The profile MUST impose lower admission bounds where consensus or node-wide work
budgets require them.

#### Hashes and commitments

Define `T(name, bytes)` as BLAKE3 in key-derivation mode with the context string
`"Zakura BPv1 " + name` (decision-25). Define the 30-byte coding tuple:

```text
coding = u16(2) || u64(body_bytes) || u32(tx_count) || u32(S) || u32(stripes) || u32(k) || u32(n)
```

Section 2 defines the tree and the commitment `C` over this tuple. W2 MUST
publish a Merkle-root vector together with the codec vectors.

The chain identifier is the consensus genesis block's raw 32-byte hash. Header
admission MUST supply `block_id`; a sender-supplied identifier does not
substitute for admission. The commitment does not replace body validation.

#### Field grammar

The following table lists each payload's fields in transmission order.

| Message | Fields |
| --- | --- |
| `HeaderMeta` | `u16(header_length)`, header, binding, coding tuple (30), part root (32), proposer node (32) |
| `Subscribe` | scope, `u16(count)`, lanes (32 each) |
| `Unsubscribe` | scope, `u16(count)`, lanes (32 each) |
| `Refuse` | scope, `u16(count)`, lanes (32 each), `u8(reason)` |
| `Want` | `u32(want_id)`, block identifier, `u32(stripe)`, requested bitmap (11) |
| `StripeDone` | block identifier, `u32(stripe)` |
| `BlockDone` | block identifier |
| `Advert` | block identifier, `u32(wait_us)`, `u16(count)`, items of lane (32) and `i32(delay_us)` |
| `Part` | block identifier, `u32(index)`, `u8(sibling_count)`, siblings (32 each), payload (`wire_len(index)`) |
| `Have` | block identifier, `u8(flags)`, `u16(count)`, `u32(index)` each |
| `WantEnd` | `u32(want_id)`, `u16(sent)`, `u8(reason)` |
| `SubscriptionEnd` | scope, `u16(count)`, lanes (32 each) |

The requested bitmap encodes stripe-relative index `j` in bit `j mod 8` of byte
`j / 8`. `Refuse` reasons are 0 `Capacity`, 1 `UnknownBlock`, 2 `Policy`, and
3 `Shed`.
`WantEnd` reasons are 0 `Served`, 1 `UnknownBlock`, and 2 `Retired`.
`Have` flags are `0x00`, `0x01` (push), or `0x03` (push and relay). Bit 1, the
relay flag, is valid only with bit 0. Any other value returns `Disconnect`
(close-1).

The binding contains
`u32(coinbase_length) || canonical_coinbase || u8(txid_sibling_count) || txid_siblings || u8(auth_sibling_count) || auth_siblings || light_client_root`.
The coinbase MUST be nonempty. Both paths always start at index zero.
Their hashing and transaction-id rules come from the selected chain adapter,
not the BPv1 part tree. Parsing this envelope does not establish that its
transaction, header, or commitment is valid.

### Parameter registry

This registry is authoritative for the draft's parameter meanings and reference
settings. A value marked experimental reproduces the fleet experiments. It is
not a production recommendation or evidence of conformance. A value marked
`TBD` MUST be fixed before an implementation claims conformance. Reports MUST
record all overrides. The
[design rationale](../design/bpv1.md#parameter-tuning) does not override this
registry.

#### Protocol and profile

| Parameter | Reference value | Meaning and authority |
| --- | --- | --- |
| `S_MIN`; `S_MAX`; `STRIPE_K`; `parity_parts(k)` | 4,096 bytes; 65,536 bytes; 64; `ceil(k / 3)` | W2 profile. `S` is per block (section 2). `S_MIN` keeps a part's prefix, proof, and announcement under 4% of its payload. Parity near one third let pulling nodes skip slow neighbours at 128 and 256 MiB. |
| `MAX_STRIPES`; `MAX_HAVE_INDICES` | 512; 4,096 | W2 syntax limits. |
| `MAX_WANTS_INFLIGHT` | 64 per connection direction | W2 commitment limit. The server acts only above twice this value. |
| `MAX_OPEN_SUBS` | 1,024 per connection direction | W2 limit on subscriptions outside `Idle`. The publisher acts only above twice this value. |
| Publisher accept limit | `TBD`, at most `MAX_OPEN_SUBS` | Local capacity. The publisher refuses above it. |
| `PROPAGATION_DEPTH` | 16 blocks | W2 window for coded propagation and the retired record. |
| Early record per peer; node-wide; `EARLY_AGE` | `TBD`; experiments used a 64 MiB node-wide buffer | Messages about unknown blocks. |
| Peer-view and advert retention | `TBD`; `ADV_KEEP` = 64 blocks | Learning bounds. |
| Concurrent blocks | `TBD`; experiments used 4 | Assembly bound. |
| Metadata in admission; pending metadata | 64 entries and 8 MiB each; pending also 4 entries per peer and 15 s | Separate budgets, so headers with unknown parents never refuse viable metadata. |
| Subscription cadence | capacity 64; refill 32/s | Candidate. Exhaustion is recorded, not enforced. |

#### Routing policy

| Parameter | Reference value | Meaning and authority |
| --- | --- | --- |
| `PULL_WINDOW`; node-wide pull cap | 64 parts per neighbour; 4,096 parts | Experimental. |
| `PULL_TICK_MS`; `PULL_TIMEOUT_MS` | 20 ms; 500 ms | Experimental. |
| `HOLE_LOOKAHEAD`; `STARVE_MS`; hole-pull cap | 3; 300 ms; 256 parts | Experimental. |
| `SAMPLE_EVERY`; `MIN_SAMPLES`; `LAG_EWMA` | 4; 3; 0.3 | Experimental. |
| `LEARN_DELAY_MS`; `ADV_WAIT_MS` | 100 ms; 500 ms | Experimental. |
| `PROMOTE_AFTER`; `PROMOTE_WAIT_MS` | 1 step; 50 ms | Experimental. |
| `DEMOTE_AFTER`; `DEMOTE_MS` | 3 steps; 5 ms | Experimental. |
| `BAN_STEPS` | 5 steps | Experimental. |
| `MAX_PROPOSER_SUBS` | `TBD` | Required. Accepted proposer subscriptions per node. |
| `CHILD_BUDGET` | `TBD` | Required before the subscription extension ships. The experiments capped accepted subscriptions at `1.0 * degree`, about 1.4 blocks at degree 28 and 20 entry peers. |
| `HAVE_FLUSH_MS`; `HAVE_FLUSH_COUNT` | 2 ms; 32 indices | Experimental. |
| `COPY_BATCH` | 8 direct copies | Experimental. Copies the proposer releases to an idle data writer behind one flagged `Have`. |
| `Have` initial group; `advert_delay` | main's `advert_window_full_block` (32) to `advert_window_small_block` (64) by body size; 500 ms | Main's block gossip settings (decision-26). BPv1 adds no setting. |

#### Recovery and retention

| Parameter | Reference value | Meaning and authority |
| --- | --- | --- |
| Stall interval; `STALL_BACKOFF` | `SLOW_X` × one part's estimated arrival (decision-22); 2.0 | Recovery; the reconstruction deadline is the only bound (decision-17). |
| Reconstruction deadline | 15 s | Monotonic from metadata admission. |
| Retention after completion | 5 s, extended until every neighbour reports `BlockDone`, within a 60 s bound | Experimental. |
| Verification and decode workers; CPU and memory caps | `TBD` | Required per-peer and node-wide work limits. |

The fleet experiments also fixed transport settings: a 24 MB QUIC send window
with cubic congestion control and a window floor of
`max(256 KB, 16 MiB/s * RTT)`, a 4 MB initial window, and 256 MB stream and
512 MB connection receive windows. These values are experiment inputs. The
production values follow the regulation design's derivation.

#### Profile items

The profile MUST fix these before implementations claim interoperability:

| Item | Draft choice |
| --- | --- |
| Part payload and stripes | W2 fixes `S_MIN`, `S_MAX`, `ALIGN`, `STRIPE_K`, and `parity_parts` |
| Codec construction | Candidate `reed-solomon-simd` 3.1 construction; vectors `TBD` |
| Hashing | W2 fixes BLAKE3 key derivation with BPv1 contexts; vectors `TBD` |
| Proposer authentication | W2 fixes the `BPV1 01` coinbase commitment; supported chain formats and admission adapter remain `TBD` |
| Serialization | W2 fixes payload encoding; service identifier, capability, stream kinds, and negotiation remain `TBD` |
| Resource bounds | W2 fixes syntax limits; accept limits, early-record, retention, and work bounds remain `TBD` |

A codec, parity, part-size, stripe-size, or wire-format change MUST use a
mutually selected profile before affected metadata or parts are sent. No
proposer MAY vary parity. A local-policy update MAY change routing values
within the bounds above.

### Conformance tests

The implementation MUST provide focused checks for the following properties. It
MAY use the existing unit, property, synthetic-peer, and transport test
infrastructure that the
[testing design](../design/property-testing.md) describes.

1. Every message has legal boundary cases, canonical round trips, exact
   decoding, payload caps, and decoded-allocation checks. Deterministic cases
   MUST cover every message and rule; random generation MUST NOT provide the
   only coverage.
2. Layout arithmetic, padding, the empty body, the transaction-count prefix,
   trailing bytes, the
   total block-size bound, and proofs at every depth up to 16. Body changes
   invalidate cached parity; header changes require no new encoding.
3. Header admission before any part work, missing parent context, a forged
   coinbase binding, a coinbase with no commitment or two, an unusual output
   beside one valid commitment, a layout mismatch, a failed re-encode check
   going straight to block download, and consensus-invalid bodies.
4. The wanted set. Unsolicited parts, repeats from one peer, parts after
   `SubscriptionEnd` or `WantEnd`, parts beyond a `Want`'s count, parts of
   retired blocks, retired-block counters, and early-record overflow, pausing,
   and replay. The control stream never pauses for the early record. Push
   flags past the connection's push bound, relay flags from a peer other than
   the proposer or past `n - k`, proposer seeds that arrive before the
   proposer's `HeaderMeta`, a flagged part that never arrives, and a holder
   push that crosses another peer's copy of the same part.
5. Subscription crossings. A reference model of both peers generates
   interleavings of the two streams: `Subscribe` against `Refuse`, `Unsubscribe`
   against `Refuse` with reason `Shed`, re-subscription after each end, and
   block retirement on either side. Conformant sequences MUST NOT produce
   `Disconnect`. The model also checks that the receiver never attributes more
   parts to a `Want` than the server sent.
6. Send-once across push, pull, repair, and regenerated parts, including
   cancellation after queueing.
7. Capacity. Saturated verification workers, full output queues, blocked `Want`
   serving, and connection churn pause work without a violation and release
   resources exactly once. Control progress continues while data output is
   blocked.
8. Recovery. The stall resets on progress; the reconstruction deadline does not.
   The ladder never moves backward. Block download follows the reconstruction
   deadline, a metadata conflict, or a failed re-encode check.
9. Routing. No cycles within a block, the cycle-break rule, a node that runs
   several blocks behind, two proposers at once, random proposer pairs,
   proposer changes with shared neighbours, a parent that withholds one lane,
   and proposer subscriptions.

Fleet experiments MUST report the block outcomes in section 7. They MUST vary
block size, proposer placement, concurrent proposers, and asymmetric bandwidth.
They MUST report normal-path completion separately from repair and block
download.

## Source locations

- [Peer message regulation](peer-message-regulation.md)
- [Service sessions](../design/service-sessions.md)
- [Header admission](../../crates/zakura-header-chain/src/transition/planner/event_effects/header_admission.rs)
- [Current consensus header](../../crates/zakura-chain/src/block/header.rs)
- [Current block serialization](../../crates/zakura-chain/src/block/serialize.rs)
