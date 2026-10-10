# Dogwood protocol specification

Status: protocol draft, second revision. This document defines the proposed
behavior. The [design document](../design/dogwood.md) explains the choices.
This revision replaces the first draft's part masks, immutable grants, and
byte-budgeted route controller. It adopts the mechanisms that the `flow`
prototype measured on an 80-node fleet: coded stripes, proposer roots, one
stream per root, pull-first routing, and receiver-chosen push. Section 8 fixes
candidate payload profile W2. Transport negotiation and the production chain
adapter remain open.

`MUST` defines a security or interoperability requirement. `SHOULD` defines the
default policy. An alternative policy must preserve every `MUST`. `TBD`
identifies a choice that blocks interoperable implementation.

Message admission follows the
[peer message regulation specification](peer-message-regulation.md). That
document defines the filter order, the admission results, reservations,
subscriptions, cadence, and capacity admission. Section 5 states the
Dogwood-specific rules for each message in the same format.

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
independent count, byte, work, and time limits to this pending state.

Header admission does not validate the block body. Reconstructed blocks MUST
enter the existing consensus block-validation path.

The [design targets](../design/dogwood.md#performance-targets) define the
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
S        = part payload bytes                   // W2: 65,536
K        = max(1, ceil(len(B) / S))             // source parts in the body
stripes  = ceil(K / STRIPE_K)                   // W2: STRIPE_K = 64
k        = ceil(K / stripes)                    // source parts per stripe
n        = k + parity_parts(k)                  // W2: parity_parts(k) = ceil(k / 3)
data     = split(zero_pad(B, stripes * k * S), S)
source(s) = data[s*k .. (s+1)*k]                // stripe s, 0 <= s < stripes
coded(s)  = systematic_encode(source(s))        // n parts; the first k equal source(s)
index(s, j) = s * n + j                         // wire index, 0 <= j < n
```

Each stripe is an independent codeword. Any `k` distinct correctly encoded parts
of a stripe reconstruct that stripe. Every stripe has the same `k` and `n`. The
last stripes carry zero padding, and `body_bytes` gives the exact body length.
Every part, including parity, MUST contain exactly `S` payload bytes.

`0 < k < n`, `stripes <= MAX_STRIPES`, and
`len(canonical(header)) + len(B) <= MAX_BLOCK_BYTES` MUST hold. The profile
fixes `S`, `STRIPE_K`, `parity_parts`, and `MAX_STRIPES`. A proposer cannot
choose coding parameters. Receivers MUST recompute `stripes`, `k`, and `n` from
`body_bytes` and the profile, and MUST reject a mismatch.

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
root. The signature still binds the root to the final block identifier.
Bare-header propagation MAY proceed through headerchain while encoding runs. It
MUST NOT authorize part processing.

A future profile MAY commit per-stripe roots in the header, so the proposer can
send each stripe as soon as it is encoded. W2 does not.

### Metadata fields

`HeaderMeta` MUST authenticate all of these values:

```text
HeaderMeta {
    header: ConsensusHeader,
    proposer_key: PublicKey,
    proposer_node: NodeId,    // transport identity that sends the seed parts
    key_binding: BoundedProof,
    coding: Coding,           // codec, body_bytes, part_bytes, stripes, k, n
    part_root: Hash,
    created_ms: u64,          // proposer's Unix time, in milliseconds
    roots: [NodeId],          // 1..=MAX_ROOTS proposer peers, in stream order
    signature: Signature,
}
```

`body_bytes` MUST equal `len(B)`. Admission MUST check the combined header and
body size with checked arithmetic before it allocates assembly state.

`roots` MUST be nonempty and MUST NOT repeat an identity. It MUST NOT contain
`proposer_node`. Section 3 derives each part's stream from `roots`.

`created_ms` anchors the arrival potentials in section 7. Nodes MUST NOT use it
for admission, retention, or deadlines. A skewed proposer clock shifts every
node's potentials by the same amount.

The block identifier is the consensus hash of `header`. The profile MUST define
one canonical serialization for the metadata fields and one hash `T`. The
proposer signature MUST cover a domain separator, the chain identifier, the
protocol profile, the block identifier, the proposer key, the proposer node, the
coding tuple, the part root, the creation time, and the root list. The metadata
variant identifier MUST hash these signed fields, excluding the signature and
the key-binding proof. Alternate encodings of equivalent proof or signature
evidence MUST NOT create apparent proposer equivocation.

Every Dogwood message other than `HeaderMeta` names a block by its variant
identifier, not by its consensus hash. This document calls that identifier
`block_id`. A proposer can sign two variants of one header. A peer that admitted
the other variant then sees an unknown `block_id`, never an invalid proof or
index. Headerchain still deduplicates headers by consensus hash.

Coded propagation covers only recent blocks. Let `top` be the highest block
height this node has admitted, which never decreases. A node MUST NOT admit
metadata for a block at or below `top - PROPAGATION_DEPTH`. Such a block uses
the existing block-download path.

The signature binds `proposer_node` to the block. A root therefore accepts seed
parts only from the connection authenticated as `proposer_node`.

### Bind the proposer to the proof of work

`key_binding` MUST prove that the mined header commits to `proposer_key`. A
transport identity, self-signed wrapper, or unauthenticated coinbase label does
not satisfy this rule. The key is the proposer identity for this block. It does
not establish a unique operator or a stable physical entry point.

The proposed chain adapter commits the key before mining, then signs metadata
after mining. Its candidate uses one zero-value transparent coinbase output with
this exact script for a 32-byte proposer key:

```text
6a 28 || ASCII("DOGWOOD") || 01 || proposer_key[32]
```

`6a` is `OP_RETURN`; `28` directly pushes the following 40 bytes. The verifier
MUST require exactly one matching commitment output. Alternate push encodings,
extra bytes, and a nonzero output value MUST NOT satisfy this candidate. The
candidate does not put the part root in the coinbase.

The proof carries the canonical coinbase transaction and its transaction-id
Merkle path at index zero. The verifier MUST compute the txid with the existing
chain implementation for the admitted height and transaction version. It MUST
check the path against the admitted header's transaction Merkle root. Bounded
parsing and an output key match do not replace that path check. The profile MUST
bound the coinbase bytes, script bytes, output count, and path depth before
allocation. Oversized or unsupported bindings use existing block propagation.

Under [ZIP 244](https://zips.z.cash/zip-0244), the txid commits to transparent
outputs. It excludes input scripts from that digest. A key in the coinbase input
script MUST NOT be accepted with only a txid inclusion path. An adapter that
uses authorizing data would need the separate header commitment proof for that
data. It MUST NOT silently substitute one proof for the other.

The local binding probe covers transparent-only V5 coinbases. The selected
profile still MUST fix supported transaction versions and chain-verification
work bounds. A post-Tachyon format requires its own commitment check; the V5
result does not establish that future binding.

Implementations MUST NOT assume that the current Zcash header already
authenticates this wrapper. A block without a supported key binding MUST use
existing block propagation.

The verifier MUST check the header's proof of work and all contextual header
rules before signature verification, part-root admission, or route-state
allocation. It MUST bound even invalid-header verification with per-peer and
global work limits.

A valid header does not limit how many roots its signer can sign. Nodes MUST
retain at most one admitted metadata variant per block hash. Exact duplicates
MUST NOT create additional assembly or forwarding work. On a second distinct,
fully authenticated variant, a node MUST stop coded propagation for that block,
retain bounded conflict evidence, and recover through the existing block-download
path. A metadata conflict alone MUST NOT invalidate the consensus header or
penalize an honest relay. Local variant selection is not a consensus rule.

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
leaf[i]     = T("leaf", encoding_id || u32(i) || payload[i])
pad[p]      = T("pad", encoding_id || u32(p))
parent      = T("node", left || right)
```

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

## 3. Streams and route state

### Roots and streams

A root is a peer of the proposer that `roots` lists. With `R = len(roots)`,
wire index `i` belongs to the stream of root `roots[i mod R]`. A stream is
identified by its root's `NodeId` alone. Streams are shared across proposers:
two proposers that list the same root feed the same stream.

The proposer sends each part of its block exactly once, to the part's root. The
root forwards it. Every other node obtains a stream's parts from a parent that
pushes them, or by pulling parts that its neighbours announce.

### Subscriptions

A subscriber asks a publisher to push a stream. The scope of a subscription is
`Standing` or `Block(block_id)`. A standing subscription covers each part of the
stream in every admitted block while it lasts. A block subscription covers the
stream's parts in one admitted block.

Each connection direction keeps one subscription state per `(scope, stream)`:

| Subscriber state | Entered when | Parts wanted |
| --- | --- | --- |
| `Idle` | Initially, or when the subscriber reads the subscription's `StreamEnd` | No |
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
6. The publisher answers every `Unsubscribe` with exactly one `StreamEnd` on the
   data stream. `StreamEnd` travels behind every part that the publisher queued
   for the subscription. The publisher then forgets the subscription.
7. The subscriber returns to `Idle` when it reads `StreamEnd` in `Draining`.

Only `StreamEnd` ends a subscription, and only `Unsubscribe` causes it. Neither
side ends a subscription on a timer. A publisher keeps a refused subscription
until its `Unsubscribe` arrives, so every crossing has a defined answer.
`Refuse` and `StreamEnd` share the data stream, so a subscription's `Refuse`
always arrives before its `StreamEnd`. Section 4 explains why this ordering
keeps in-flight parts legal.

A subscriber MUST keep at most `MAX_OPEN_SUBS` subscriptions outside `Idle` at
each peer. The publisher holds each subscription from its `Subscribe` until
its `StreamEnd` enters the data stream's ordered output. It then forgets the
subscription's key, before the subscriber can leave `Draining`. The publisher
therefore never holds more subscriptions than the subscriber's limit, and never
holds a key that the subscriber may subscribe to again. Each held subscription
reserves the output for its `Refuse` and its `StreamEnd`. That output
reservation lasts until the write completes.

A publisher that retires a block stops pushing its parts. It keeps the block's
subscriptions until their `Unsubscribe`s arrive, and it MAY refuse them with
reason `Shed`. A subscriber that retires a block MUST send `Unsubscribe` for the
block's `Live` subscriptions. It keeps them in `Draining` until their
`StreamEnd`s arrive.

### Wanted set

Each node keeps, per connection, the parts it wants from that peer. Part `i` of
admitted block `b` is wanted from peer `p` when at least one of these holds:

1. A subscription at `p` whose scope covers `b` and whose stream contains `i` is
   `Live` or `Draining`.
2. A live `Want` sent to `p` for `b` has `i` in its allowed set and unspent
   count.
3. `p` is `b`'s `proposer_node`, and this node is the root of `i`'s stream.
   This is the seed slot.

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

**Reservation messages.** `Subscribe`, `Unsubscribe`, `Refuse`, `StreamEnd`,
`Want`, and `WantEnd` match reservation state by key, whether or not the block
they name is admitted, retired, or unknown. They never enter the early record or
the retired path. Section 5 gives the answer for an unknown or retired block:
`Refuse` with reason `UnknownBlock` for a block subscription, and `WantEnd` with
`sent = 0` for a `Want`.

**Unknown blocks.** Another message can name a `block_id` that this node has not
admitted: its metadata is still in flight or under admission, the peer admitted
another variant, the peer runs more than `PROPAGATION_DEPTH` blocks behind, or
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
that reach `EARLY_AGE` return `Drop`. When the node retires a block's variants
after a conflict, it drops the entries held for every variant of that block.

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
that this node subscribed to, asked for, or holds a seed slot for. Data-stream
messages about unknown blocks add at most the per-peer early-record bound per
`EARLY_AGE`. Control-stream announcements about unknown or retired blocks are
small, and their cadence in section 5 records floods. Total receive bytes are
therefore bounded by this node's own subscriptions and `Want`s over the retained
blocks, plus the early allowance. A byte budget adds nothing to this bound. A
per-peer byte budget per block MAY still serve as defence in depth.

### Node state

Each node maintains these bounded structures:

```text
meta[b]                          // admitted metadata, layout, roots
store[b][i]                      // verified or regenerated parts
receive_record[peer][b]          // indices received from the peer
send_mark[peer][b]               // indices queued to the peer
subs_in[peer][scope][stream]     // this node subscribes: Live | Draining
subs_out[peer][scope][stream]    // this node publishes: accepted | refused
wants_out[peer][want_id]         // block, stripe, allowed set, count, spent
wants_in[peer][want_id]          // Wants this node serves
peer_view[peer][b]               // Haves, StripeDone, BlockDone, parts written
early[peer]                      // messages about unknown blocks
adverts[peer][b]                 // advertised potentials and wait
learner[stream]                  // parent, candidates, lags, streaks (section 7)
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

Dogwood is one zakura-network service. Its
[service session](../design/service-sessions.md) has two persistent ordered
streams:

- The **control stream** carries `HeaderMeta`, `Subscribe`, `Unsubscribe`,
  `Want`, `StripeDone`, `BlockDone`, and `Advert`.
- The **data stream** carries `Part`, `Have`, `Refuse`, `WantEnd`, and
  `StreamEnd`.

The control stream MUST have higher write priority than the data stream. Data
queues MUST NOT block control traffic.

The two streams impose no order on each other. The protocol places each message
by the ordering it needs:

- **Ends travel behind parts.** `StreamEnd` and `WantEnd` follow, on the FIFO
  data stream, every part their exchange authorized. No part of an ended
  subscription or `Want` can follow its end. The receiver can therefore close
  the authorization exactly when it reads the end.
- **Haves travel behind parts.** A `Have` queues behind the data already waiting
  on the link. Its arrival time carries the sender's queue, which the learner in
  section 7 measures.
- **Stop signals overtake parts.** `StripeDone` and `BlockDone` travel on the
  control stream so they reach the sender before its queued parts drain.
- **Refusals travel ahead of ends.** `Refuse` and `StreamEnd` share the data
  stream, so a subscriber always reads a subscription's `Refuse` before its
  `StreamEnd`. A refusal ends nothing: parts already queued stay wanted until
  the `StreamEnd` that answers the subscriber's `Unsubscribe`.

A message can arrive before the metadata of the block it names. Section 3's
early record handles that case.

### Message table

The service declares one message table per stream, as the
[message regulation design](../design/peer-message-regulation.md#message-tables)
describes. Payload caps below are W2 values.

| Discriminator | Message | Stream | Role | Answers or ends | Payload cap |
| --- | --- | --- | --- | --- | --- |
| 1 | `HeaderMeta` | control | Announcement | — | 29,868 |
| 2 | `Subscribe` | control | Subscription open | — | 8,195 |
| 3 | `Unsubscribe` | control | Subscription close | — | 8,195 |
| 4 | `Want` | control | Request | — | 53 |
| 5 | `StripeDone` | control | Announcement | — | 36 |
| 6 | `BlockDone` | control | Announcement | — | 32 |
| 7 | `Advert` | control | Announcement | — | 9,218 |
| 8 | `Part` | data | Response | Answers `Subscribe`, `Want`, or the seed slot | 66,085 |
| 9 | `Have` | data | Announcement | — | 16,418 |
| 10 | `Refuse` | data | Subscription update | Refuses `Subscribe` | 8,196 |
| 11 | `WantEnd` | data | Response | Ends `Want` | 7 |
| 12 | `StreamEnd` | data | Response | Answers `Unsubscribe`; ends the subscription | 8,195 |

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

The following limits apply to every Dogwood message:

```text
S                  = 65,536 bytes
STRIPE_K           = 64
MAX_N              = 86          // STRIPE_K + ceil(STRIPE_K / 3)
MAX_STRIPES        = 512
MAX_PARTS          = 44,032      // MAX_STRIPES * MAX_N
MAX_PROOF_DEPTH    = 16          // log2(128) + log2(512)
MAX_ROOTS          = 255
MAX_HAVE_INDICES   = 4,096
MAX_WANTS_INFLIGHT = 64          // live Wants per connection direction
MAX_OPEN_SUBS      = 1,024       // subscriptions outside Idle per connection direction
PROPAGATION_DEPTH  = 16          // blocks below the highest admitted height
MAX_HEADER_BYTES   = 4,096
MAX_BINDING_BYTES  = 17,413
```

`StripeDone`, `BlockDone`, and `Advert` that name an unknown or retired block
charge one per-connection announcement bucket: capacity = 256 messages, refill
= 64 messages/s, on_empty = record and forward. Floods of these small messages
are therefore visible without risking an honest peer.

A scope encodes as `u8(0)` for `Standing`, or `u8(1) || block_id[32]` for
`Block`. A stream encodes as its root's 32-byte `NodeId`. Section 8 lists every
field in transmission order.

### Control stream

#### `HeaderMeta` — Announcement, discriminator 1

- **Frame**
  - payload cap = 29,868 bytes
- **Decode**
  - header length = 1..=4,096 bytes
  - binding length = 1..=17,413 bytes
  - codec = W2 codec identifier; `part_bytes = S`
  - root count = 1..=255
  - roots unique; no root equals `proposer_node`
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
  - the key-binding path matches the admitted header's transaction root
  - the strict Ed25519 signature verifies over the section 8 transcript
  - at most one variant per block hash
- **Cadence**
  - at most one `HeaderMeta` per variant per connection
  - capacity = 64 messages; refill = 16 messages/s; on_empty = record and
    forward
  - headerchain's per-peer pending-header bounds

A node MUST send `HeaderMeta` for a variant at most once per connection. It
MUST forward only metadata it admitted, and only within the propagation window
in section 2. It SHOULD forward admitted metadata promptly to every peer, including peers without subscriptions. Forwarding MUST NOT wait
for parts or a per-hop request. It SHOULD NOT send metadata to a peer that
already sent it the same variant. The proposer SHOULD write `HeaderMeta` to
every peer before it writes any part.

A header that headerchain rejects returns `Disconnect` when headerchain
classifies the failure as peer-caused. A missing parent invokes bounded
recovery and is not a violation. A layout mismatch, invalid binding, or invalid
signature returns `Disconnect`: an honest relay forwards only admitted
metadata. A repeat of a variant from the same peer returns `Disconnect` while
this node keeps the variant admitted or retired. The sender keeps the variant
retired at least until the block leaves its own window, and it never admits a
block outside that window, so it cannot send the variant twice. An exact
duplicate from another peer returns `Drop`. A second distinct authenticated
variant returns `Drop` and triggers the conflict
rule in section 2; the node then retires every variant of the block. Metadata
for a retired block, or for a block outside the propagation window, returns
`Drop`.

After admission, a node that appears in `roots` wants its seed slot from
`proposer_node` under section 3.

#### `Subscribe` — Subscription open, discriminator 2

- **Frame**
  - payload cap = 8,195 bytes
- **Decode**
  - scope tag 0 or 1
  - stream count = 1..=255
  - streams unique
  - exact consumption
- **Reservation**, at the publisher
  - each `(scope, stream)` is not currently held
  - held subscriptions per connection <= `MAX_OPEN_SUBS`; the publisher serves
    and traces up to twice that limit and returns `Disconnect` beyond it
- **Capacity**
  - each held subscription reserves output for one `Refuse` and one `StreamEnd`
  - the publisher's accept limit may be lower than `MAX_OPEN_SUBS`; above it,
    the publisher refuses
- **Cadence**
  - capacity = 64 messages
  - refill = 32 messages/s
  - on_empty = record and forward, as the regulation design requires

The publisher MUST accept or refuse each listed stream when it processes the
message. It MAY refuse any stream for local policy, including its child budget
in section 7. Refusal is never a violation. It MUST refuse a block subscription
for a block it has not admitted or has retired. It answers every refused stream
in one `Refuse`.

An accepted subscription authorizes the publisher to push each part of the
stream that it holds or later verifies, once, for every covered block. The
publisher MAY skip any part. The protocol imposes no push deadline. A slow
publisher is a routing problem for the subscriber's learner, not a violation.

A `Subscribe` for a `(scope, stream)` that the publisher holds returns
`Disconnect`. The subscriber sends `Subscribe` only in `Idle`, which it reaches
only by reading `StreamEnd`. The publisher forgets a subscription when it writes
that `StreamEnd`. The same ordering bounds the held count: a conformant
subscriber never has more than `MAX_OPEN_SUBS` subscriptions that the publisher
still holds.

#### `Unsubscribe` — Subscription close, discriminator 3

- **Frame**
  - payload cap = 8,195 bytes
- **Decode**
  - same as `Subscribe`
- **Reservation**, at the publisher
  - each `(scope, stream)` is held, accepted or refused
- **Capacity**
  - processing MUST NOT wait for serving workers or data-output capacity; the
    held subscription already reserved its `StreamEnd` output

For each accepted `(scope, stream)`, the publisher MUST stop queueing the
stream's parts under that scope. It MUST cancel queued unsent parts that no
other authorization covers. Cancellation keeps their send marks. For every
listed `(scope, stream)`, accepted or refused, it MUST then write one
`StreamEnd` behind every part it already queued for the subscription. It MAY
end several subscriptions in one `StreamEnd` when it writes that message after
all their parts.

A `(scope, stream)` that the publisher does not hold returns `Disconnect`. The
subscriber sends exactly one `Unsubscribe` per `Subscribe`, and the publisher
holds every subscription until that `Unsubscribe` arrives.

#### `Want` — Request, discriminator 4

- **Frame**
  - payload cap = 53 bytes
- **Decode**
  - `want_id != 0`
  - `count >= 1`
  - allowed bitmap = 11 bytes; bits at or above `MAX_N` are zero
  - `count <= popcount(allowed)`
  - exact consumption
- **Verify**, when the server has admitted the block
  - `stripe < stripes`
  - allowed bits at or above `n` are zero
- **Reservation**, at the server
  - `want_id` differs from every live `Want` on this connection
  - the allowed set is disjoint from every live `Want` for the same block and
    stripe on this connection
  - live `Want`s <= `MAX_WANTS_INFLIGHT`; the server serves and traces up to
    twice that limit and returns `Disconnect` beyond it
- **Capacity**
  - the serving task acquires output capacity before it produces parts
  - the reader admits the request as a commitment without waiting

A `Want` asks for up to `count` parts of one stripe, chosen by the server from
the allowed set. The requester MUST create the reservation before it sends the
request. The requester MUST keep live `Want`s at one peer disjoint for each
block and stripe. It SHOULD give disjoint allowed sets to different servers, so
two servers do not send the same index.

The server MUST end every admitted `Want` exactly once with `WantEnd` on the
data stream, behind the parts it sent for that `Want`. It MUST send at most
`count` parts. Each part MUST come from the allowed set and MUST obey
send-once. It serves the parts it holds when it processes the request,
including regenerated parts of checked stripes. It does not wait for later
parts; a block subscription serves that purpose. It SHOULD send data parts
before parity parts. It SHOULD skip parts that the requester announced. It MAY
send fewer than `count` parts, including none. A `Want` for a block the server
has not admitted or has retired ends with `sent = 0` and the matching reason.

An out-of-range stripe or allowed bit returns `Disconnect`. The requester knows
the layout because it admitted the metadata. A reused live `want_id`, an
overlapping live allowed set, or more than twice `MAX_WANTS_INFLIGHT` live
`Want`s returns `Disconnect`. A conformant requester sends a new `Want` only
after it reads the previous one's end, and the server releases a `Want` when it
writes that end.

A requester that no longer needs the parts keeps the reservation live until
`WantEnd` or session retirement. A local pull timeout MAY send another `Want`
to another server. It MUST NOT revoke the first reservation.

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
the sender is the block's proposer. It is terminal and advisory for the block on
the connection. The receiver MUST stop queueing parts of the block to the
sender. It MUST still process control messages and future-block demand from the
sender. `BlockDone` ends no subscription and no `Want`.

A node SHOULD send `BlockDone` to every peer that knows the block. The proposer
SHOULD send `BlockDone` right after `HeaderMeta`, so no neighbour forwards the
block's parts back to it.

A repeat returns `Disconnect` while the receiver retains the block's peer view.
A `BlockDone` for an unknown or retired block follows section 3: the receiver
MAY hold it in the control-stream early record, and otherwise returns `Drop`.

A false `BlockDone` stops only the sender's own incoming parts. It can attract
repair `Want`s, which the sender must still end. It cannot cancel another
peer's traffic or establish block validity.

#### `Advert` — Announcement, discriminator 7

- **Frame**
  - payload cap = 9,218 bytes
- **Decode**
  - entry count = 0..=255
  - entry streams unique
  - exact consumption
- **Verify**, when the receiver has admitted the block
  - every entry stream appears in the block's `roots`
- **Cadence**
  - at most once per block per connection

`Advert` carries the sender's forwarding wait and, per stream, its arrival
potential for the block. Section 7 defines both values. The receiver keeps each
peer's adverts for the last `ADV_KEEP` blocks. The values are hints. A
receiver MUST NOT treat an advertised value as a violation.

An entry stream outside the block's roots returns `Disconnect`. A repeat returns
`Disconnect` while the receiver retains the peer's adverts for the block. An
`Advert` for an unknown or retired block follows section 3, like `BlockDone`.

### Data stream

#### `Part` — Response, discriminator 8

- **Frame**
  - payload cap = 66,085 bytes
- **Decode**
  - sibling count <= `MAX_PROOF_DEPTH`
  - payload = exactly `S` bytes after the siblings
  - exact consumption
- **Reservation precheck**, on the fixed prefix `(block_id, index)`
  - an unknown block goes to the data-stream early record
  - a retired block spends the peer's retired part counter and returns `Drop`;
    an exhausted counter returns `Disconnect`
  - `index < stripes * n`
  - `index` is absent from the peer's receive record; the node records it
  - `index` is wanted from the peer; the node spends a `Want`'s count when
    section 3 attributes the part to a `Want`
- **Verify**
  - sibling count = the layout's `depth`
  - the Merkle proof verifies against `part_root`

The sender MUST send a part only when the receiver wants it: a subscription it
accepted and has not ended covers it, a live `Want` from the receiver allows it
with count left, or the sender is the block's proposer and the receiver is the
part's root. The sender MUST mark a part as sent when it queues it, across
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
  - payload cap = 16,418 bytes
- **Decode**
  - index count = 1..=4,096
  - indices strictly increasing
  - exact consumption
- **Verify**, when the receiver has admitted the block
  - every index < `stripes * n`
- **Cadence**
  - each `(block_id, index)` at most once per connection

`Have` states that the sender holds verified parts. The sender MUST announce
only such parts. It SHOULD batch announcements for at most `HAVE_FLUSH_MS` or
`HAVE_FLUSH_COUNT` indices. It SHOULD announce each verified part to every peer
that has not reported `StripeDone` for its stripe or `BlockDone` for its block.
The receiver uses `Have`s to choose pulls, to skip sends, and to measure lag.

An out-of-range index or a repeated index returns `Disconnect` while the
receiver retains the block's peer view. A `Have` for an unknown block enters the
data-stream early record, and a `Have` for a retired block spends the peer's
retired index counter, under section 3. A false `Have` attracts `Want`s that the sender must still end; it
cannot force the receiver to accept parts.

#### `Refuse` — Subscription update, discriminator 10

- **Frame**
  - payload cap = 8,196 bytes
- **Decode**
  - scope and streams as in `Subscribe`
  - reason = `Capacity`, `UnknownBlock`, `Policy`, or `Shed`
  - exact consumption
- **Reservation**, at the subscriber
  - each `(scope, stream)` is `Live` or `Draining`
  - at most one `Refuse` per subscription

The publisher sends `Refuse` when it declines a new subscription or stops
pushing an accepted one (`Shed`). After `Refuse`, it queues no new parts under
the subscription. It still holds the subscription until `Unsubscribe` arrives.

A subscriber that reads `Refuse` in `Live` MUST send `Unsubscribe`. Parts
already queued stay wanted until the resulting `StreamEnd`.

A `(scope, stream)` in `Idle`, or a second `Refuse` for one subscription,
returns `Disconnect`. The publisher refuses only a subscription it holds, at
most once, and the subscriber stays outside `Idle` until the publisher writes
the subscription's `StreamEnd`.

After a refusal, the subscriber SHOULD avoid that publisher for the stream for
`BAN_STEPS` learning steps. A later `Subscribe` after `StreamEnd` is legal.

#### `WantEnd` — Response, discriminator 11

- **Frame**
  - payload cap = 7 bytes
- **Decode**
  - reason = `Served`, `UnknownBlock`, or `Retired`
  - exact consumption
- **Reservation**, at the requester
  - `want_id` names a live `Want`
  - `sent <= count`
  - `sent >=` the parts this node attributed to the `Want`
  - consumes the reservation

A `WantEnd` that names no live `Want` or breaks either count rule returns
`Disconnect`. The server writes one end per admitted `Want` behind its parts,
and section 3's attribution never counts more answers than the server sent.
After `WantEnd`, the requester MAY ask another server for the parts it still
lacks.

#### `StreamEnd` — Response, discriminator 12

- **Frame**
  - payload cap = 8,195 bytes
- **Decode**
  - scope and streams as in `Subscribe`
  - exact consumption
- **Reservation**, at the subscriber
  - each `(scope, stream)` is `Draining`
  - consumes the subscription: the state becomes `Idle`

A `(scope, stream)` outside `Draining` returns `Disconnect`. The publisher
writes `StreamEnd` only in answer to `Unsubscribe`, once per subscription. After
`StreamEnd`, a part of that stream under that scope is unwanted unless another
authorization covers it.

## 6. Forwarding, completion, and recovery

### Forwarding

When a node verifies a new part, it MUST store the part within assembly bounds.
It SHOULD queue the part for every peer with an accepted subscription that
covers the part, except peers that it MUST or SHOULD skip:

- It MUST skip a peer from which it received a valid copy of the part.
- It MUST skip a peer whose send mark already holds the part.
- It MUST skip a peer that reported `BlockDone` for the block.
- It SHOULD skip a peer that announced the part, or reported `StripeDone` for
  its stripe.
- It SHOULD skip a peer that can already decode the stripe: the parts the peer
  announced plus the parts this link wrote to it reach `k`.

The node checks these rules when it queues a part and again when it writes it.
Forwarding MUST NOT wait for decoding or for other parts. The node MAY verify
parts in bounded batches. It also queues a `Have` for the part to every peer
under section 5.

The proposer SHOULD send each part of its block exactly once, to the part's
root, in index order per root. It sends parity parts as well. It SHOULD
interleave its roots, so every stream starts at once. It SHOULD NOT push its own
block's parts on standing subscriptions. W2 gives the proposer no authorization
to send spare copies beyond the seed slots, subscriptions, and `Want`s. A
proposer that fills peers' missing parts from spare upload needs a future
extension that makes those parts wanted.

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
it never received to accepted subscribers of their streams. These parts obey
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

1. **Normal path.** Pushed parts and pulls on `Have` fill the stripes (section
   7).
2. **Repair.** After `STALL_MS` without a new verified part of the block, the
   node enters repair. For each undecoded stripe it sends `Want`s for
   `k - held - pending` parts, with the missing indices split disjointly across
   peers. Peers that reported `StripeDone` or `BlockDone` and delivered parts of
   the block come first. Other reporters follow, then peers that announced the
   indices, then the proposer. Later rounds wait `STALL_MS * STALL_BACKOFF^round`
   and never ask a peer again for the same index. The node MAY use a block
   subscription to wait for parts a peer lacks now. Repair ends after
   `REPAIR_ROUNDS` rounds.
3. **Block download.** A monotonic reconstruction deadline starts at metadata
   admission. Partial progress MUST NOT reset it. When the deadline passes, or
   repair ends without decoding, the node MUST use the existing full-block
   download path. It MUST verify the result against the admitted header. A
   metadata conflict or a failed re-encode check goes to this step at once.

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

### Pull first

Every stream starts with no parent. A stream whose standing parent is the
current block's proposer also counts as having no parent. The node pulls a part
on a `Have` when all of these hold:

- the announcer is not the block's proposer;
- the node neither holds the part nor has a pending pull for it;
- the part's stripe has not decoded;
- the node is not the root of the part's stream;
- the stream has no parent;
- the stripe's held and pending parts number fewer than `k`;
- the announcer has room in its pull window, within the node-wide pull cap.

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

A stream with a parent can still miss parts. The node pulls a missing part of
such a stream when one of these holds:

- the parent announced the part;
- the parent announced a later part of the same stream within
  `HOLE_LOOKAHEAD` positions;
- the parent kept announcing other parts for `STARVE_MS` after its last part of
  this stream.

Hole pulls share a node-wide cap across blocks.

### Measure lag

The node samples a part when a hash of `(block_id, index)` selects one in
`SAMPLE_EVERY`. For a sampled part, a neighbour's lag is the arrival time of its
copy or `Have` minus the part's first arrival at this node. A candidate's raw
lag for a stream is the median over at least `MIN_SAMPLES` sampled parts of
that stream in the block. The ranking lag is an EWMA of raw lags with weight
`LAG_EWMA`.

The node runs one learning step per block, `LEARN_DELAY_MS` after it assembles
the block. The step waits up to `ADV_WAIT_MS` for its candidates' `Advert`s.

### Potentials and loop freedom

A node's potential for a stream in a block is the median first-arrival delay,
after `created_ms`, of the stream's sampled parts. Parts that the node
regenerates count at decode time. The node advertises its potentials and its
forwarding wait in `Advert`. The forwarding wait is its queued bytes over an
EWMA of its egress rate.

A candidate parent for a stream is a neighbour that is not the block's
proposer, not this node's child for the stream, and not banned. A candidate that
is not the current parent MUST advertise a lower potential than this node's for
the learned block. Its advertised wait adds to its lag. Advertised values use
each node's own clock, so skew between nodes biases rankings. It cannot create a
cycle among nodes that pass this filter in the same block.

When a node's parent for a stream subscribes to that stream at the node, the
node unsubscribes from that parent. This rule removes the cycles that form
across blocks.

### Promote and demote

After `PROMOTE_AFTER` steps with the same leading candidate, the node promotes
the leader, provided the leader's advertised wait is below `PROMOTE_WAIT_MS`. A
step without candidates keeps the streak. Promotion sends `Subscribe` to the new
parent and `Unsubscribe` to the old one in the same step. The old stream stays
wanted while it drains.

A step is late when the parent's ranking lag exceeds the best other candidate's
raw lag plus that candidate's round-trip time plus `DEMOTE_MS`. After
`DEMOTE_AFTER` late steps, the node unsubscribes from the parent. The stream
returns to pull.

When the standing parent of a stream is the current block's proposer, the node
subscribes for this block to the best-ranked other candidate. Without one, it
subscribes to the root itself when the root is a neighbour.

### Push where cheap

A publisher bounds its accepted standing subscriptions with a child budget. The
budget counts each accepted stream at its share of a block: `1/R`, where `R` is
the root count of the most recent admitted block that listed the stream. It
sums the shares across children. A publisher refuses a
subscription that would exceed `CHILD_BUDGET` blocks. Because streams are shared
across proposers, the budget binds per node, not per proposer.

### Proposer roots

The proposer SHOULD choose its roots from its neighbours by measured minimum
round-trip time. It keeps the neighbours whose minimum is at most
`ROOT_RTT_X` times the median minimum of its neighbours with a sample. It drops
neighbours without a sample when any sample exists. It lists at most
`MAX_ROOTS` roots in any order. Streams are keyed by root, so the order does not
disturb learned routes.

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
| `S`; `STRIPE_K`; `parity_parts(k)`; codec identifier | 65,536 bytes; 64; `ceil(k / 3)`; 2 |
| `MAX_STRIPES`; `MAX_PARTS`; maximum proof depth | 512; 44,032; 16 siblings |
| Maximum body | 2 GiB (`MAX_STRIPES * STRIPE_K * S`), and never above `MAX_BLOCK_BYTES` |
| `MAX_ROOTS`; `MAX_HAVE_INDICES`; `MAX_WANTS_INFLIGHT` | 255; 4,096; 64 |
| Header bytes; key-binding bytes | 4,096; 17,413 |
| Coinbase bytes; coinbase Merkle siblings | 16,384; 32 |
| Control-stream frame cap; data-stream frame cap | 29,868 bytes; 66,085 bytes |

These are syntax limits, not assembly reservations or performance guarantees.
The profile MUST impose lower admission bounds where consensus or node-wide work
budgets require them.

#### Hashes, commitments, and signatures

Define `T(name, bytes)` as BLAKE3 in key-derivation mode with the context string
`"Zakura Dogwood 2 " + name`. Define the 26-byte coding tuple:

```text
coding = u16(2) || u64(body_bytes) || u32(65536) || u32(stripes) || u32(k) || u32(n)
```

Section 2 defines the tree over this tuple. W2 MUST publish a Merkle-root vector
together with the codec vectors.

Use pure Ed25519 from [RFC 8032](https://www.rfc-editor.org/rfc/rfc8032.html),
with these additional acceptance restrictions. The 32-byte public key and
signature point `R` MUST use canonical compressed Edwards encodings and MUST be
nonidentity points in the prime-order subgroup. Reject mixed-torsion and
small-order points, noncanonical field encodings, and negative zero. The 64-byte
signature consists of `R || S`; the little-endian scalar MUST satisfy
`S < 2^252 + 27742317777372353535851937790883648493`. Verify the usual Ed25519
equation after these checks. Implementations MUST test library behavior against
these restrictions. Ed25519ph and Ed25519ctx do not implement W2.

The chain identifier is the consensus genesis block's raw 32-byte hash. The
signature covers this transcript of `216 + 32 * R` bytes:

```text
ASCII("Dogwood/metadata/2") || 00 || chain_id || u16(2) ||
block_id || proposer_key || proposer_node || coding || part_root ||
u64(created_ms) || u8(R) || roots
```

Header admission MUST supply `block_id`; a sender-supplied identifier does not
substitute for admission. The metadata variant identifier is
`T("variant", transcript)`. It excludes the signature and key-binding proof. A
signature does not replace proof of the coinbase commitment or body validation.

#### Field grammar

The following table lists each payload's fields in transmission order.

| Message | Fields |
| --- | --- |
| `HeaderMeta` | `u16(header_length)`, header, proposer key (32), proposer node (32), `u16(binding_length)`, binding, coding tuple (26), part root (32), `u64(created_ms)`, `u8(root_count)`, roots (32 each), signature (64) |
| `Subscribe` | scope, `u16(count)`, streams (32 each) |
| `Unsubscribe` | scope, `u16(count)`, streams (32 each) |
| `Refuse` | scope, `u16(count)`, streams (32 each), `u8(reason)` |
| `Want` | `u32(want_id)`, block identifier, `u32(stripe)`, `u16(count)`, allowed bitmap (11) |
| `StripeDone` | block identifier, `u32(stripe)` |
| `BlockDone` | block identifier |
| `Advert` | block identifier, `u32(wait_us)`, `u16(count)`, entries of stream (32) and `u32(potential_us)` |
| `Part` | block identifier, `u32(index)`, `u8(sibling_count)`, siblings (32 each), payload (`S`) |
| `Have` | block identifier, `u16(count)`, `u32(index)` each |
| `WantEnd` | `u32(want_id)`, `u16(sent)`, `u8(reason)` |
| `StreamEnd` | scope, `u16(count)`, streams (32 each) |

The allowed bitmap encodes stripe-relative index `j` in bit `j mod 8` of byte
`j / 8`. `Refuse` reasons are 0 `Capacity`, 1 `UnknownBlock`, 2 `Policy`, and
3 `Shed`.
`WantEnd` reasons are 0 `Served`, 1 `UnknownBlock`, and 2 `Retired`.

The binding contains
`u32(coinbase_length) || canonical_coinbase || u8(sibling_count) || siblings`.
The coinbase MUST be nonempty. The transaction path always starts at index zero.
Its hashing and transaction-id rules come from the selected chain adapter, not
the Dogwood part tree. Parsing this envelope does not establish that its
transaction, header, or commitment is valid.

### Parameter registry

This registry is authoritative for the draft's parameter meanings and reference
settings. A value marked experimental reproduces the fleet experiments. It is
not a production recommendation or evidence of conformance. A value marked
`TBD` MUST be fixed before an implementation claims conformance. Reports MUST
record all overrides. The
[design rationale](../design/dogwood.md#parameter-tuning) does not override this
registry.

#### Protocol and profile

| Parameter | Reference value | Meaning and authority |
| --- | --- | --- |
| `S`; `STRIPE_K`; `parity_parts(k)` | 65,536 bytes; 64; `ceil(k / 3)` | W2 profile. Parity near one third let pulling nodes skip slow neighbours at 128 and 256 MiB. |
| `MAX_STRIPES`; `MAX_ROOTS`; `MAX_HAVE_INDICES` | 512; 255; 4,096 | W2 syntax limits. |
| `MAX_WANTS_INFLIGHT` | 64 per connection direction | W2 commitment limit. The server acts only above twice this value. |
| `MAX_OPEN_SUBS` | 1,024 per connection direction | W2 limit on subscriptions outside `Idle`. The publisher acts only above twice this value. |
| Publisher accept limit | `TBD`, at most `MAX_OPEN_SUBS` | Local capacity. The publisher refuses above it. |
| `PROPAGATION_DEPTH` | 16 blocks | W2 window for coded propagation and the retired record. |
| Early record per peer; node-wide; `EARLY_AGE` | `TBD`; experiments used a 64 MiB node-wide buffer | Messages about unknown blocks. |
| Peer-view and advert retention | `TBD`; `ADV_KEEP` = 64 blocks | Learning bounds. |
| Concurrent blocks | `TBD`; experiments used 4 | Assembly bound. |
| Subscription cadence | capacity 64; refill 32/s | Candidate. Exhaustion is recorded, not enforced. |

#### Routing policy

| Parameter | Reference value | Meaning and authority |
| --- | --- | --- |
| `ROOT_RTT_X` | 2.0 | Experimental proposer root rule. |
| `PULL_WINDOW`; node-wide pull cap | 64 parts per neighbour; 4,096 parts | Experimental. |
| `PULL_TICK_MS`; `PULL_TIMEOUT_MS` | 20 ms; 500 ms | Experimental. |
| `HOLE_LOOKAHEAD`; `STARVE_MS`; hole-pull cap | 3; 300 ms; 256 parts | Experimental. |
| `SAMPLE_EVERY`; `MIN_SAMPLES`; `LAG_EWMA` | 4; 3; 0.3 | Experimental. |
| `LEARN_DELAY_MS`; `ADV_WAIT_MS` | 100 ms; 500 ms | Experimental. |
| `PROMOTE_AFTER`; `PROMOTE_WAIT_MS` | 1 step; 50 ms | Experimental. |
| `DEMOTE_AFTER`; `DEMOTE_MS` | 3 steps; 5 ms | Experimental. |
| `BAN_STEPS` | 5 steps | Experimental. |
| `CHILD_BUDGET` | `TBD` | Required before enabling push by default. The experiments capped accepted subscriptions at `1.0 * degree`, about 1.4 blocks at degree 28 and 20 roots. |
| `HAVE_FLUSH_MS`; `HAVE_FLUSH_COUNT` | 2 ms; 32 indices | Experimental. |

#### Recovery and retention

| Parameter | Reference value | Meaning and authority |
| --- | --- | --- |
| `STALL_MS`; `STALL_BACKOFF`; `REPAIR_ROUNDS` | 2,000 ms; 2.0; 3 | Experimental. |
| Reconstruction deadline | `TBD` | Required. Monotonic from metadata admission. |
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
| Part payload and stripes | W2 fixes `S`, `STRIPE_K`, and `parity_parts` |
| Codec construction | Candidate `reed-solomon-simd` 3.1 construction; vectors `TBD` |
| Hashing | W2 fixes BLAKE3 key derivation with Dogwood contexts; vectors `TBD` |
| Proposer authentication | W2 fixes Ed25519 and the transcript; supported chain formats and admission adapter remain `TBD` |
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

1. Every message has legal boundary cases, canonical round trips, exact decoding,
   payload caps, and decoded-allocation checks. Deterministic cases MUST cover
   every message and rule; random generation MUST NOT provide the only coverage.
2. Layout arithmetic, padding, the transaction-count prefix, trailing bytes, the
   total block-size bound, and proofs at every depth up to 16. Body changes
   invalidate cached parity; header changes require a new signature but not a
   new encoding.
3. Header admission before any part work, missing parent context, forged key
   binding, signed equivocation, a layout mismatch, a failed re-encode check
   going straight to block download, and consensus-invalid bodies.
4. The wanted set. Unsolicited parts, repeats from one peer, parts after
   `StreamEnd` or `WantEnd`, parts beyond a `Want`'s count, parts of retired
   blocks, retired-block counters, and early-record overflow, pausing, and replay.
   The control stream never pauses for the early record.
5. Subscription crossings. A reference model of both peers generates
   interleavings of the two streams: `Subscribe` against `Refuse`, `Unsubscribe`
   against `Refuse` with reason `Shed`, re-subscription after each end, and
   block retirement on
   either side. Conformant sequences MUST NOT produce `Disconnect`. The model
   also checks that the receiver never attributes more parts to a `Want` than
   the server sent.
6. Send-once across push, pull, repair, and regenerated parts, including
   cancellation after queueing.
7. Capacity. Saturated verification workers, full output queues, blocked
   `Want` serving, and connection churn pause work without a violation and
   release resources exactly once. Control progress continues while data output
   is blocked.
8. Recovery. The stall resets on progress; the reconstruction deadline does
   not. The ladder never moves backward. Block download follows repair failure,
   a metadata conflict, or a failed re-encode check.
9. Routing. Loop freedom within a block, the cycle-break rule, a node that runs
   several blocks behind, two proposers at once, random proposer pairs, and
   proposer changes with shared roots.

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
