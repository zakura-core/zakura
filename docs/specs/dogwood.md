# Dogwood protocol specification

Status: protocol draft. This document defines the proposed behavior.
[design document](../design/dogwood.md) explains the choices. The five message families are
settled for this draft; the wire profile and chain binding are not.

`MUST` defines a security or interoperability requirement. `SHOULD` defines
the default policy. An alternative policy must preserve every `MUST`.
`TBD` identifies a choice that blocks interoperable implementation.

## 1. Scope and integration

The protocol pushes new proof-of-work blocks over existing authenticated,
encrypted, reliable peer connections. Existing service negotiation MUST select
a common protocol profile before these messages appear. No additional
application handshake is defined here.

Headerchain MUST own header validation, contextual difficulty, fork choice,
and header recovery. The propagation service MUST submit `HeaderMeta.header`
through that admission path. It MUST NOT create a second header validator.

The current header-sync service has `Status`, `GetHeaders`, `Headers`, and
`HeadersOutcome`; it has no `HeaderMeta` or coded-part messages. Integration
MUST negotiate the new propagation service and connect it to headerchain.
Existing header discovery and full-block download MUST remain available.
Nodes MUST deduplicate headers across these paths by consensus block hash.

A node MUST NOT forward metadata, open assembly state, decode parts, or update
proposer measurements before full header admission and metadata authentication.
Bounded parsing and verification are the necessary exceptions. If parent
context is missing, the node MAY retain a bounded metadata envelope while
headerchain recovers that context. It MUST apply independent count, byte, work,
and time limits to this pending state.

Header admission does not validate the block body. Reconstructed blocks MUST
enter the existing consensus block-validation path.

## 2. Parts and authenticated metadata

Encode the block body, not the already transmitted header. Let `B` be the
canonical serialization of the transaction vector, including its count prefix.
The existing block serializer concatenates the header and this vector.
Reassembly MUST use the exact admitted header from `HeaderMeta`.

```text
S = part payload bytes                         // default 65,536
k = ceil(len(B) / S)
n = k + ceil(k / 4)
data = split(zero_pad(B, k * S), S)
parts[0..n] = systematic_encode(data, codec)
```

The selected codec is deterministic systematic Reed–Solomon over
GF(2^16), with the property that any `k` distinct correctly encoded parts
reconstruct `B`. The construction below fixes its parity bytes.
Every part, including parity, MUST contain exactly `S` payload bytes.
`0 < k < n <= min(MAX_PARTS, 65535)` and
`len(canonical(header)) + len(B) <= MAX_BLOCK_BYTES` MUST hold.
The profile MUST fix `S` and resource bounds within these codec limits.
A peer cannot choose arbitrary coding parameters to increase receiver work.

### Codec

Use the systematic Vandermonde construction in
[RFC 5510, section 8](https://www.rfc-editor.org/rfc/rfc5510.html#section-8):

```text
field polynomial = x^16 + x^12 + x^3 + x + 1     // 0x1100b
a = x                                           // field element 0x0002
V[r,c] = a^(r*c)                                 // 0 <= r < k, 0 <= c < n
G = inverse(V[:,0..k]) * V                       // k-by-n systematic matrix
```

Arithmetic in this construction MUST use GF(2^16). Each consecutive pair of
payload bytes MUST encode one field element in little-endian order, with bit
`j` representing the coefficient of `x^j`. `S` MUST be even. For each element
offset, multiply the row vector of `k` data elements by `G` to obtain the `n`
encoded elements. Indices `0..k` are the original data parts. The remaining
indices are parity parts. The encoder MUST use one codeword for the whole body.

A codec-level test vector uses `k = 2`, `n = 3`, and `S = 65536`. Data part 0
starts with `01 00`; data part 1 starts with `02 00`. Every remaining byte is
zero. Parity part 2 MUST start with `04 00` and contain zeros afterward.
This vector tests the codec, not canonical block-body parsing.

### Incremental decoding

The baseline decoder SHOULD maintain an incremental elimination state. Part
`i` supplies coefficient row `G[:,i]` and its payload as the right-hand side.
On each verified arrival, eliminate existing pivots and retain the new pivot.
After `k` distinct parts, finish back-substitution and the checks below.
Systematic parts expose their body bytes directly, but those bytes remain
provisional until verification completes. A decoder MAY use an equivalent
algorithm that produces the same result within the same work bounds.

The decoder MUST verify membership before incorporating a part. It MUST
incorporate each distinct index at most once. It MUST bound total CPU and
memory, including elimination state, and MUST NOT restart an unbounded job on
each arrival. Received-part forwarding MUST NOT wait for decoding. Serving
reconstructed parts requires complete codeword verification.

### Proposer preparation

The proposer MUST finish the committed codeword and its Merkle root before
publishing `HeaderMeta`. It SHOULD precompute them for the candidate body while
mining. A change to the body, including its coinbase, invalidates that cached
encoding. A header-only change does not change the encoded body or part root.
The signature still binds the root to the final mined block identifier.
The block-hash-derived part-mask mapping is computed after mining.
Bare-header propagation MAY proceed through headerchain while unfinished
encoding runs; it MUST NOT authorize coded-part processing.

### Metadata fields

`HeaderMeta` MUST authenticate all of these values:

```text
HeaderMeta {
    header: ConsensusHeader,
    proposer_key: PublicKey,
    key_binding: BoundedProof,
    codec_id: CodecId,
    body_bytes: u64,
    part_bytes: u32,
    data_parts: u32,        // k
    total_parts: u32,       // n
    part_root: Hash,
    signature: Signature,
}
```

`body_bytes` MUST equal `len(B)`. Admission MUST require
`data_parts = ceil(body_bytes / part_bytes)` and the profile's parity schedule.
It MUST check the combined header/body size with checked arithmetic before
allocating assembly state.

The block identifier is the consensus hash of `header`. The profile MUST
define one canonical serialization for the metadata fields and one hash `H`.
The proposer signature MUST cover a domain separator, chain identifier,
protocol profile, block identifier, proposer key, codec, size, counts, and root.
The metadata variant identifier MUST hash these signed fields, excluding the
signature and key-binding proof. Alternate encodings of equivalent proof or
signature evidence MUST NOT create apparent proposer equivocation.

### Bind the proposer to the proof of work

`key_binding` MUST prove that the mined header commits to `proposer_key`.
A transport identity, self-signed wrapper, or unauthenticated coinbase label
does not satisfy this rule. The key is the proposer identity used for routing;
it does not establish a unique operator or a stable physical entry point.

The proposed chain adapter commits the key before mining, then signs metadata
after mining. A coinbase key commitment with a canonical transaction inclusion
proof is a candidate. Its encoding, transaction-version rules, and proof
validation are `TBD`. The adapter MUST demonstrate that the committed field is
covered by the header for every supported transaction version.

This proposal does not place the part root inside the block it encodes.
Implementations MUST NOT assume that the current Zcash header already
authenticates this wrapper. A block without a supported key binding MUST use
existing block propagation.

The verifier MUST check the header's proof of work and all contextual header
rules before signature verification, part-root admission, or proposer-state
allocation. It MUST bound even invalid-header verification with per-peer and
global work limits.

A valid header does not limit how many roots its signer can sign. Nodes MUST
retain at most one admitted metadata variant per block hash. Exact duplicates
MUST NOT create additional assembly or forwarding work. On a second distinct,
fully authenticated variant, a node MUST stop coded propagation for that block,
retain bounded conflict evidence, and recover through ordinary block download.
A metadata conflict alone MUST NOT invalidate the consensus header or penalize
an honest relay. Local variant selection is not a consensus rule.

### Authenticate parts and reconstruction

The Merkle construction MUST bind part position and coding parameters:

```text
encoding_id = H(domain_encoding || canonical(codec, body_bytes, S, k, n))
leaf[i] = H(domain_leaf || encoding_id || u32(i) || parts[i])
```

The profile MUST fix internal-node hashing, tree padding, integer encoding,
proof order, and proof depth. Verification MUST require the exact proof shape
for index `i < n`. Domains for leaves, internal nodes, and other hashes MUST
differ. `part_root` MUST commit to all data and parity parts.

A valid proof establishes membership in the signed root. It does not establish
that the parts form a valid codeword or a valid block.

After receiving `k` distinct valid parts, the node MUST reconstruct the body,
check padding and `body_bytes`, and re-encode the body.
The re-encoded root MUST equal `part_root`. The node MUST bound this work and
MUST NOT search combinations of parts after failure. Failure disables coded
propagation for that block and starts ordinary block recovery.

The node MUST assemble the admitted header and reconstructed body using the
existing canonical block format. It MUST enforce the total block-size bound
and reject trailing or noncanonical body bytes. Header-only bytes MUST NOT be
counted again as data parts.

Only after codeword and canonical-assembly checks may the node regenerate and
serve missing parts or send `FullBlock`. The existing validator independently
decides whether the block is valid, including whether the body's transaction
commitments match the admitted header. Coding verification does not establish
consensus validity and MUST NOT substitute for that validation.

## 3. Part selection and route state

A part is one encoded payload. Persistent subscriptions select parts through a
fixed-width `PartMask`; block-scoped subscriptions select exact indices.
The profile fixes `0 < P = PART_MASK_BITS <= MAX_PARTS`.
For a given block, sort all indices
`i in [0, n)` by `(H(domain_route || block_id || u32(i)), i)`.
Let `rank(i)` be the zero-based position:

```text
mask_bit(block_id, i) = rank(i) mod P
selected(mask, block_id, i) = mask[mask_bit(block_id, i)]
```

Each enabled mask bit selects either `floor(n/P)` or `ceil(n/P)` parts.
A mask bit is a selector, not a part index. This mapping distributes both data
and parity indices. Nodes MUST NOT assume this determinism prevents proposer
grinding or makes arrival times independent.
They MUST evaluate actual part placement for failure coverage.

Each node maintains these bounded structures:

```text
incoming[scope][peer, mask_bit]  // requests we sent
outgoing[scope][peer, mask_bit]  // requests peers sent
block_incoming[block][peer, i]    // temporary exact-part overrides
block_outgoing[block][peer, i]
receive_grants[peer, grant_seq]
send_grants[peer, grant_seq]
received_from[block][peer, i]
sent_to[block][peer, i]
completed_by[block][peer]
route_stats[proposer_or_default, size_class, load_class, peer_pair]
load_budget[peer]               // shared across proposers and blocks
pending_payload[peer]           // local estimate, not sender queue occupancy
```

`scope` is `Default` or `Proposer(key)` for persistent routes.
The logical wire scope also permits `Block(block_id)` for exact-part repair.
Each proposer has independent incoming and outgoing routes and route statistics.
A switch between known proposer keys MUST NOT reset these routes or statistics.
An unfamiliar key uses default routes until the receiver installs overrides.
Changing congestion or the entry point behind the same key can stale its routes.
Size and load classes are local measurement contexts, not wire selectors.
One proposer has only one installed persistent row per peer at a time.
Connection grants, pending bytes, budgets, and hard limits MUST remain shared
across these contexts. Changing proposer keys MUST NOT reset them.

For a particular peer and block, the effective subscription is the first
present row in this order:

1. The exact-part `Block(block_id)` row.
2. The `Proposer(key)` row, expanded through the part-mask mapping.
3. The `Default` row, expanded through the part-mask mapping.

An absent row inherits. An empty explicit row disables all its parts.
Changing an override MUST NOT change the row it overrides.
The initial default row is empty.
Receivers MUST remove persistent overrides with `Inherit` before retiring
their local copies. Senders MUST NOT silently evict an active persistent row.
Capacity pressure requires rejecting new state or closing the service.
Block-row retirement MUST follow the block retention rules.

`SubscribeParts` adds bits. When it first creates an override, the handler
MUST copy the currently inherited row before adding bits. The receiver MUST
account for those inherited bits when choosing credit. `UnsubscribeParts`
can remove bits or remove the entire override to restore inheritance.

A subscriber controls only traffic sent to itself on that connection.
Reciprocal subscriptions are legal. Nodes MUST NOT infer availability from a
subscription, an announced header, or reciprocity.

## 4. Messages, credit, and ordering

These are the only five application message families:

```text
Scope = Default | Proposer(PublicKey) | Block(Hash)
Selection = PartMask | PartRanges
UnsubscribeAction = Remove(Selection) | Inherit

SubscribeParts {
    control_seq: u64,
    scope: Scope,
    selection: Selection,
    first_height: u32,
    last_height: u32,
    part_credit: u32,
    byte_credit: u64,
}

UnsubscribeParts {
    control_seq: u64,
    scope: Scope,
    action: UnsubscribeAction,
}

BlockPart {
    block_id: Hash,
    grant_seq: u64,
    part_index: u32,
    payload: Bytes,
    proof: MerkleProof,
}

FullBlock {
    block_id: Hash,
}
```

Section 2 defines `HeaderMeta`. All hashes and integer encodings are fixed by
the selected profile. `PartMask` has exactly `ceil(P/8)` bytes and zero
unused bits. `PartRanges` contains sorted, disjoint, non-adjacent, non-empty
half-open ranges inside `[0,n)`. Selections MUST be non-empty.
Only `Block` scope uses `PartRanges`.

Route updates and `FullBlock` MUST share one ordered control stream per
connection direction.
Their `control_seq` starts at one and increments without gaps or wraparound.
The sequence of a `SubscribeParts` also identifies its grant.
A connection restart discards its sequence space and every grant.

The sender MUST put `HeaderMeta` before the first part on each ordered block
data stream. It MUST use at most one such stream per block per direction on a
connection. A metadata duplicate from another peer MUST NOT cause the
receiver to lose the stream's association with the admitted metadata.
Bounded transport buffering may hold parts while metadata admission completes.
Data queues MUST NOT block service of control traffic.

### Immutable grants

The receiver MUST record a receive grant before sending `SubscribeParts`.
The sender MUST reserve response work and record the matching send grant before
enabling the update. A grant covers the message's selection, scope, height
interval, part credit, and encoded-byte credit. Grants do not change when
routes change.

A default grant matches any authenticated proposer. A proposer grant matches
only that key. A block grant matches only that block and its exact indices.
The height interval MUST contain that block's admitted height.
`first_height <= last_height` MUST hold. The inclusive interval MUST be finite
and at most `MAX_GRANT_HEIGHT_SPAN`. Local relevance limits MUST bound how far
ahead a receiver issues credit.

For a new part send, the effective route MUST enable the index and one grant
MUST cover it. Both remaining counters MUST suffice. The sender consumes one
part and the complete encoded `BlockPart` byte count before queueing it.
The receiver consumes the same counters before proof verification.
Parts MUST NOT combine credit from several grants.

Every subscription MUST satisfy:

```text
0 < part_credit <= MAX_GRANT_PARTS
byte_credit = part_credit * MAX_PART_MESSAGE_BYTES
charge = byte_credit + REQUEST_OVERHEAD
```

Arithmetic MUST be checked. Senders MUST reserve outstanding response bytes
before accepting the grant. They MUST also pace actual sends within service
budgets. Receivers SHOULD preissue enough credit for several blocks; routes
with exhausted credit cannot deliver, even when their bitmap cells are enabled.

For each grant, response accounting MUST preserve:

```text
reserved_bytes = unsent_credit + queued_bytes + sent_bytes + released_bytes
```

The fixed request overhead is not refundable. Grant closure MUST release unused
credit exactly once. Queue cancellation MUST release its reservation exactly
once. A route update MUST NOT close a grant still usable by other routes.
The sender MUST NOT restore grant counters for a canceled queued part: the receiver cannot
observe that cancellation. A new grant supplies any replacement credit.

Unsubscribe stops new sends under the effective route. It cannot revoke
already consumed credit. Receivers MUST classify responses against immutable
grants, not current route bits. The same rule applies after `FullBlock`.

Nodes MUST cap grants and retire them after exhaustion, connection closure, or
the bounded retention policy for their height interval. IDs MUST never be
reused within a connection. Retirement MUST NOT turn delayed honest data into
a protocol violation. A bounded late-message path MUST drop parts referencing
retired or locally evicted grants without allocation or proof verification.
An unknown ID below the last issued control sequence may be stale; it MUST
receive the same bounded treatment. An ID never yet issued is invalid.

Nodes MUST bound stale traffic separately, so retiring credit does not create
unlimited receive work. Local capacity pressure MAY pause admission or close
the service without blaming the peer.

## 5. Message protections

Every message MUST have one declaration defining frame and field bounds,
admission filters, state changes, sender obligations, and resource costs.
The wire variants, declarations, handlers, and reference model MUST form one
closed inventory.

The common path is:

```text
frame cap -> cadence / work bound -> bounded decode
          -> authorization -> verification -> relevance -> handler
```

A fixed prefix MAY support authorization before variable-field decoding.
Decoders MUST reject non-canonical encoding, trailing bytes, arithmetic
overflow, and invalid field lengths. A frame cap does not replace field
allocation limits. Transports MUST enforce incomplete-frame deadlines.
Each peer and message kind MUST have bounded cadence accounting. The profile
MUST specify sender rates and bursts compatible with the receiver's allowance.
Response credit bounds outstanding work; service budgets bound work over time.
Neither bound replaces the other.

| Message | Required protection and effect |
| --- | --- |
| `HeaderMeta` | Bound header, key proof, signature, and verification work. Fully admit the header and authenticate metadata before assembly or forwarding. Cap candidates per height, pending parents, and recent blocks. |
| `BlockPart` | Match an admitted block and live grant. Check height, scope, index, credit, exact payload length, and proof shape. Consume credit before proof verification. Store and forward only after verification. |
| `SubscribeParts` | Check sequence, canonical selection, scope, height span, credit arithmetic, and state caps. Reserve maximum response work before atomically adding a grant and route bits. |
| `UnsubscribeParts` | Check sequence, scope, action, and cadence. Remove bits from the effective row, creating a copy if needed, or remove an override. Cancel queued parts that no longer have an effective route. |
| `FullBlock` | Require a previously announced, retained block and bounded cadence. Mark this peer complete and cancel its unsent parts for this block. Do not alter future routes. |

Additional obligations:

- A sender MUST send each `(block_id, part_index)` at most once per connection,
  across all grants and scopes. It MUST mark the send when queueing.
  Canceling a queued part MUST NOT clear that mark. Repair must use another
  peer or the existing block-download path if that part is still needed.
- A node MUST deduplicate storage by `(block_id, part_index)`. Copies from
  different peers are valid measurements and consume their respective credit.
  A repeat from the same peer while its receive record remains live violates
  the send-once rule. Local retirement instead uses the bounded stale path.
- A node MUST suppress sending a part back to a peer from which it received
  that valid part. Simultaneous crossing sends remain legal.
- A block-scoped subscription requires admitted metadata previously received
  from that peer. The handler MUST schedule retained requested parts and watch
  for parts that arrive later. It need not already possess them.
- `Inherit` is valid only for proposer or block scope. Repeated removals and
  inheritance requests are idempotent. Control sequences still advance.
- A proposer-scoped update need not follow that proposer's next block, but it
  MUST fit the receiver's bounded selector capacity. Only authenticated metadata
  can cause data or measurements to use that selector.
- A node MUST send `FullBlock` only after reconstruction and the codeword,
  padding, length, and canonical-assembly checks in section 2. This message is
  not a claim that full consensus validation has finished.
- `FullBlock` MUST be terminal for that block on the connection. The receiver
  MUST suppress later demand from that peer for the completed block. It MUST
  still process control sequencing and unrelated future-block demand. A node
  that later loses its data MUST recover through another connection or the
  existing full-block service.
- A node SHOULD send `FullBlock` to each peer that knows the metadata. If a new
  peer announces it later, the node SHOULD reply with `FullBlock`.
  Repeated completion notices are idempotent and cadence-bounded.
- `FullBlock` is an advisory availability hint. A dishonest peer can stop its
  own incoming traffic or falsely attract repair requests. It cannot cancel
  anyone else's traffic or establish block validity.

Admission returns `Continue`, `Drop`, `Delay`, `Disconnect`, or `LocalFault`.
A demonstrably invalid frame, proof, signature, or live-grant response returns
`Disconnect`. A valid duplicate, stale block, stale authorization, or local
loss of relevance returns `Drop`. A missing parent invokes bounded recovery.
Resource exhaustion returns `Delay` or closes the service as a local capacity
event. Delays MUST NOT create an unbounded secondary queue.
Admission waits MUST have a finite timeout. If a wait would indefinitely block
route cancellation or completion on that peer, the node MUST close the service
as a local capacity event and schedule recovery elsewhere.

Every disconnect rule MUST have a matching sender obligation. Reorganization,
local eviction, stream timing, or a crossed unsubscribe MUST NOT establish
peer misconduct. Nodes MUST log bounded diagnostic records for non-continue
results. A decoder, verifier, or peer-worker fault MUST remain local to that
work; it MUST NOT crash unrelated peer paths or refund consumed receive credit.

## 6. Forwarding, completion, and recovery

After admitting metadata, a node SHOULD promptly forward it once to every
eligible peer. Metadata forwarding MUST remain independent of part routes.
Its scheduling MUST avoid a per-hop header request round trip.

Upon receiving a valid new part, the node MUST store it within assembly bounds
and schedule it for each eligible subscriber. It MUST NOT wait for full-block
assembly. Queue eligibility requires admitted metadata, an enabled route,
credit, no prior send, and no `FullBlock` from that subscriber.
The node MUST suppress a send to a peer from which it has already received a
valid copy of that index. Crossed sends before either copy arrives remain legal.

A queue limit on one peer MUST NOT block another peer. The scheduler MUST bound
per-peer and global queue bytes, verification concurrency, reconstruction
concurrency, and retained assemblies. The receiver SHOULD prioritize distinct
coverage when allocating its grants. The sender cannot infer which grants
represent challenges or redundant routes: these roles are not on the wire.
The sender SHOULD use a work-conserving, byte-fair scheduler across active
blocks on each connection. It MUST bound starvation of eligible parts.
All block streams on a connection MUST share its transport and queue budgets.
After evicting a block's send-once or completion history, a node MUST NOT resume
sending that block on the same connection. Bounded retirement watermarks or a
service restart MUST enforce this rule. Locally retired inbound data uses the
bounded stale-message path.

After reconstruction checks succeed, the node MUST cancel its outstanding
block-specific receive demand with `FullBlock`. It SHOULD continue serving
retained and regenerated parts to incomplete peers for a bounded retention
interval. It MUST retain part proofs or regenerate the canonical Merkle tree.

A node MUST start a monotonic reconstruction deadline when it admits metadata.
It MUST NOT reset that deadline indefinitely on partial progress. On a stall,
peer loss, or deadline expiry, it MUST add bounded block-scoped subscriptions
to other peers that announced metadata. It SHOULD prefer peers that reported
`FullBlock`, subject to independent timeouts and diversity.

Recovery MUST first request enough distinct missing indices to make decoding
possible. Additional parity indices are valid substitutes for missing data.
It MAY add duplicate requests when expected latency justifies their cost.
No separate repair request, acknowledgement, or unavailable message is needed:
the subscription schedules current and future availability, and a local
deadline handles silence.

After a bounded number of attempts or a fixed total recovery deadline, the
node MUST use the existing full-block download path. It MUST verify that result
against the consensus header. Bad coding metadata MUST NOT suppress this path.

Delivery requires a reachable honest source of enough parts or the full block.
Neither parity nor local route counts prove this condition. Implementations
MUST report when they cannot satisfy coverage or recovery policy.

## 7. Redundancy and route control

The following byte-budgeted pairwise controller is the baseline experimental
policy. Its stability and performance have not been established. Implementations
MAY improve its estimator while preserving explicit resource, exploration,
recovery, and coverage bounds. The controller allocates subscriptions;
transport congestion control separately paces bytes.

### Core rules

The detailed rules below implement this control loop:

1. The receiver SHOULD select a bounded set of suppliers per part at startup.
   It MUST NOT enable every part merely because a peer connected. Additional
   subscriptions require a coverage, challenge, or recovery purpose.
2. The receiver SHOULD retain bounded randomized challenges for each active
   proposer. It MUST compare the same parts under comparable workload.
3. The receiver SHOULD move demand after repeated wins. It MUST install the
   replacement before removing the incumbent and preserve failure coverage.
4. The receiver MUST account for bytes across all proposers and active blocks
   sharing a connection. It SHOULD limit that connection to one unsettled move
   and collect fresh evidence before moving more demand into it.
5. The receiver SHOULD raise its assignment budget only after a loaded success
   and lower it after repeated uncanceled deadline failures. Hard grant and
   queue limits MUST remain independent of this learned budget.
6. The receiver MUST recover stalled blocks within bounded deadlines. It MUST
   send `FullBlock` after reconstruction checks and MUST NOT treat canceled
   demand as a later delivery failure.

### Distinct-part coverage

Let `A[p]` be the indices assigned to peer `p` for this block, after resolving
overrides. Count only routes backed by sufficient credit and expected service
capacity. For a set `F` of failed peers or known failure groups:

```text
coverage(F) = |union(A[p] for p not in F)|
margin(F) = coverage(F) - k
```

The receiver SHOULD maintain `coverage(F) >= k + safety_parts` for every
failure set in its configured failure model. `safety_parts` is nonnegative.
The default model SHOULD include loss of any one upstream peer. A receiver
MUST NOT describe distinct peer identities as independent physical paths.

With one supplier per index and no extra routes, tolerance of any one peer
requires `max_p |A[p]| <= n - k - safety_parts`. If a fast peer exceeds that
share, the receiver needs extra distinct coverage elsewhere. This permits many
parts per connection without silently abandoning redundancy.

The steady-state target SHOULD be one supplier per part plus bounded
challenges and any routes required by this coverage test. Unmeasured default
or proposer routes SHOULD start with two selected suppliers per part where
available. The receiver SHOULD distribute these assignments across eligible
peers, subject to byte limits, rather than assigning every part to the same
two peers. Two-peer networks may necessarily select both peers for every part.
The receiver SHOULD remove startup redundancy only after repeated successful
reconstruction observations.

A receiver MUST evaluate coverage using the actual `n` and part mapping when
metadata arrives. If it cannot meet its target, it MUST mark the block degraded
and add recovery demand. It cannot retroactively guarantee a fast path for a
new block size, topology, or proposer.

### Measure opportunity and delay

All timestamps in this section come from the receiver's monotonic clock.
The baseline MUST NOT depend on a sender timestamp, synchronized clocks, an
application RTT estimate, or an inferred bandwidth-delay product.

Let `t0[b]` be local metadata admission. Local policy MUST select a bounded
observation duration `D[b]` from the admitted block size and a configured
latency target. The deadline is `t0[b] + D[b]`. It MUST remain fixed for that
observation. Increasing load MUST NOT continually extend existing deadlines.
The controller MUST measure local verification queueing separately from arrival.
Only proof-verified arrivals may win races or establish successful delivery.

An eligible opportunity requires an enabled route and sufficient reserved
grant credit. The controller MUST exclude assignments that its own insufficient
credit, service closure, or resource exhaustion prevented from completing.
It MUST still report those events as local capacity failures.
For each eligible peer and part that remains requested until the deadline:

```text
delay[p,i] = min(valid_arrival[p,i] - t0[b], D[b])
delay[p,i] = D[b] when the part is absent at the deadline
```

The controller MUST record cancellation time when it sends `FullBlock`, removes
a route, or abandons an assembly. A still-missing copy at that time is censored,
not a deadline miss. A valid arrival before cancellation remains a delivery
sample. Two copies may still establish an arrival-order comparison before the
cutoff. A later in-flight copy MUST NOT turn canceled demand into a failure.
The receiver MUST NOT delay `FullBlock` to complete a measurement.

For uncensored eligible observations, define:

```text
yield[p] = timely_deliveries[p] / eligible_observations[p]
service[p] = timely_valid_payload_bytes[p] / fixed_observation_duration
```

Zero-opportunity peers have no sample. The receiver MUST report the censored
fraction alongside yield. Yield under early cancellation is not an unbiased
estimate of deadline reliability. `service` measures achieved delivery under
offered load, not unused capacity or sender queue occupancy.

Assignments added after metadata admission are recovery observations, not
pre-block race samples. A newly issued standing route is warming until the
receiver has observed a valid response under its grant. It SHOULD participate
in negative race evidence only on subsequent blocks. The protocol has no
subscription acknowledgement, so silence cannot prove when the sender installed
a route. Startup silence still triggers bounded recovery and route exploration.

For any smoothed metric:

```text
estimate_next = (1 - alpha) * estimate + alpha * sample
0 < alpha <= 1
```

Measurements SHOULD use this context, with fixed local bucket boundaries:

```text
size_class(b) = floor(log2(n[b]))
load_class(b) = (floor(log2(max(1, active_block_count))),
                 floor(log2(max(1, sum_active_blocks(n)))))
context(b) = (authenticated_proposer(b), size_class(b), load_class(b))
```

The receiver MUST snapshot the context at metadata admission, including `b`.
It MUST also record subsequent peak active bytes and block count. A material
change in load makes the observation unsuitable for promoting a route in the
original context; it still informs recovery and block-outcome reports.
The policy MUST define what constitutes a material change.

The receiver SHOULD use proposer-specific comparisons when enough recent
blocks exist in that context. Otherwise pooled measurements for the same size
and load classes MAY guide candidate selection, followed by randomized exploration.
Pooled measurements MUST NOT supply promotion votes for another proposer or
replace its learned routes merely because a different proposer published a block.
The receiver MUST age out stale comparisons. Idle periods supply no successful
samples.
One large block MUST NOT count as hundreds of independent block observations.
The receiver MUST cap contexts, peer pairs, and tracked proposers.

### Account for block size and concurrent blocks

Let `A[p,b]` be the effective assigned indices for peer `p` and admitted block
`b`. Each index appears once per peer even if several grants authorize it.
Redundant suppliers each incur their own cost. Define:

```text
assigned_bytes[p,b] = S[b] * |A[p,b]|
pending[p,b] = S[b] * count(assigned copies not yet received or retired)
Q[p] = sum_active_blocks(pending[p,b]) + cancellation_tail[p]
x[b,j] = S[b] * count(indices selected by mask bit j of block b)
S[b] * floor(n[b]/P) <= x[b,j] <= S[b] * ceil(n[b]/P)
```

`Q[p]` estimates outstanding requested payload, including upstream-unavailable
parts. It is not a measured queue. Removing demand SHOULD transfer its pending
estimate to a bounded cancellation tail rather than instantly crediting spare
capacity. Arrival or a fixed local tail timeout retires that estimate. Retirement
of this soft estimate MUST NOT release immutable grant reservations early.
The receiver MUST NOT count a copy in both pending bytes and the tail.

Each connection has a soft assignment budget `W[p]` and hard resource bounds.
All proposers and active blocks MUST share `W[p]`. A discretionary addition
of `x` bytes SHOULD require `Q[p] + x <= W[p]`. A loaded trial MAY exceed it
by at most the separately reserved probe allowance. Recovery uses a separate
bounded reserve and MUST NOT bypass hard limits. If no route fits, the receiver
MUST report degraded service rather than inventing capacity.

Before a block exists, the controller SHOULD evaluate additions against a
bounded workload scenario from recent block sizes and burst concurrency.
For a scenario containing part counts `n_hat[1..m]`, a conservative cost for
one additional mask bit is:

```text
x_hat = sum_r(S * ceil(n_hat[r] / P))
```

The scenario MUST count future blocks from all proposers that can share the
connection, not reserve a separate full budget for every proposer. Local policy
MUST define its finite sample history, conservative quantiles, and startup
scenario. The scenario is a forecast, not an admission or security bound.

On every metadata admission, the receiver MUST resolve actual assignments,
credit, coverage, and aggregate `Q`. Standing grants can produce `Q > W` before
control updates take effect. The receiver MUST continue to authorize honest
in-flight parts under their immutable grants. It SHOULD stop discretionary
additions to that connection and rebalance through block-specific overrides.
It MUST NOT claim that those post-header corrections preserve the zero-request
latency of a standing route.

The receiver SHOULD install standing routes for the workload it expects next.
The wire does not automatically select a different proposer row by block size
or concurrency. An implementation that requires that behavior needs a future
selector extension; it MUST NOT assume the sender knows receiver-local load.

### Challenge and move load

A challenge adds a supplier for selected parts before a future block. The
receiver SHOULD first use comparisons from existing backup subscriptions.
When it adds demand, it SHOULD choose an incumbent mask bit weighted by assigned
bytes and an alternative peer uniformly from eligible peers not serving those
parts. Unmeasured and previously slow peers MUST remain eligible for exploration
unless independent service or resource constraints exclude them.

#### Challenge frequency

The receiver MUST bound both extra authorized bytes and trial-start frequency.
A timer alone cannot set a useful rate: it churns routes during idle periods,
and the same number of trials costs more under larger or concurrent blocks.
The baseline uses one node-wide exploration balance, not one allowance per key:

```text
0 < exploration_fraction < 1
0 <= exploration_initial <= exploration_cap
E = exploration_initial
V[b] = n[b] * MAX_PART_MESSAGE_BYTES
E = min(exploration_cap, E + floor(exploration_fraction * V[b]))
    on the first reconstruction and consensus acceptance of block b
E = E - challenge_charge
    before authorizing extra challenge responses, only if E >= challenge_charge
```

The receiver MUST use checked arithmetic for balances and charges.
It MUST credit each relevant block hash at most once across peers,
proposers, reconnects, and reorganization. It MUST bound the eligible height
window and deduplication history; retired blocks cannot earn credit again.
Idle time, duplicate parts, repeated metadata, and proposer-key changes MUST
NOT replenish or reset `E`. Failed blocks earn no credit; recovery retains its
separate reserve. This balance adapts the token-bucket idea in
[RFC 3290, appendix A](https://www.rfc-editor.org/rfc/rfc3290.html#appendix-A)
to completed block traffic instead of elapsed time.

`challenge_charge` MUST cover the maximum encoded response bytes authorized
for the trial, including warming, plus bounded subscription-control bytes.
The receiver SHOULD issue dedicated finite challenge grants. If another live
grant can authorize additional responses on the trial route, the receiver MUST
charge those bytes too or isolate the trial from that grant. Forecast demand
alone is insufficient. Renewals require another charge. Cancellation MUST NOT
refund the exploration charge: in-flight responses remain authorized.
Existing backup demand needs no extra charge unless the challenge increases its
authorized work. With no refunds, cumulative challenge authorization is bounded
by `exploration_initial + exploration_fraction * sum(V[b])`.

The receiver SHOULD queue at most one pending trial per recently active proposer
and serve the queue round-robin. New keys join the tail without a fresh budget.
An eligible waiting trial retains its turn while funds accumulate, provided its
charge fits `exploration_cap`. Local policy MUST bound the active-proposer window,
queue size, and waiting time. Expired or infeasible entries leave the queue.
Sparse proposers get opportunities, not a guaranteed number of observations.

The receiver MUST space node-wide trial starts by at least
`CHALLENGE_MIN_INTERVAL >= CONTROL_INTERVAL`, with independent bounded jitter.
It MUST NOT start a new trial merely because an epoch elapsed. Admission also
requires an active proposer, an alternative peer, sufficient exploration funds,
connection capacity, and a free trial slot. The baseline SHOULD allow at most
one active trial per proposer and per challenger connection. A global
`MAX_CHALLENGES` bounds simultaneous trials and includes warming routes.

A trial SHOULD retain the same pair and selection across blocks so it can
collect repeated comparable observations. It MUST end on a decision, peer loss,
credit exhaustion, `CHALLENGE_MAX_BLOCKS` distinct admitted matching blocks, or
`CHALLENGE_MAX_AGE`, whichever comes first. The block cap includes warming and
inconclusive observations. Expiry without enough decisive blocks is inconclusive,
not a challenger loss. Removing the trial MUST preserve pre-existing backup
demand. Cancellation tails remain subject to the existing bounds. The receiver
MUST NOT delay `FullBlock` to prolong a trial.

For equal-size blocks, let `f` be the fraction of a block duplicated in one
trial observation and `rho = exploration_fraction`. Ignoring startup credit,
proof-size differences, and control bytes, each observation costs about `f/rho`
completed blocks of budget. At `rho = 1/32`, duplicating one quarter costs about
eight blocks; three observations cost about 24 blocks. Warming or inconclusive
observations increase this cost. These are accounting examples, not recommended
parameters. Smaller selections permit more observations for the same budget.
More frequent trials cannot create observations for a proposer that rarely mines.

Simulations MUST sweep the byte fraction, minimum interval, selection width,
and trial lifetime together. They MUST measure adaptation in both elapsed time
and proposer blocks, including warm-up cost and trials that expire inconclusively.
The policy MUST retain a positive exploration fraction, but MUST NOT bypass
hard limits or recovery priority to meet a nominal challenge rate.

#### Compare and promote

For the same part, a valid copy wins if it arrives at least `race_epsilon`
before the other copy or before a cutoff at which the other is still absent.
The cutoff is the earlier of the deadline and cancellation. Two absent copies
and arrivals separated by less than `race_epsilon` are ties. A win before
cancellation establishes arrival order, not a later deadline failure.

Within one block and peer pair, the receiver SHOULD aggregate eligible
same-part comparisons into one vote. A strict majority of decisive part
comparisons wins that block; an equal split or no decisive parts is a tie.
The controller MUST NOT infer independent statistical confidence from parts
that share a block, path, or queue. Define over a bounded recent context:

```text
win_rate(challenger, incumbent) = challenger_block_wins /
    (challenger_block_wins + incumbent_block_wins)
```

Promotion SHOULD require at least `MIN_RACE_BLOCKS` decisive blocks and
`win_rate >= SWITCH_THRESHOLD > 1/2`. These are policy thresholds, not a proof
of statistical significance or convergence. A small-block context MUST NOT
authorize an untested large-block or high-concurrency concentration.

Promotion MUST install the replacement before removing the incumbent. It
MUST preserve distinct-part coverage at both stages. The temporary overlap
MUST fit grants and exploration or migration reserves. The controller SHOULD
observe the winner serving the added load before pruning. It SHOULD limit
each connection to one unsettled promotion across all proposers, with at most
`MIGRATION_BYTES` additional forecast bytes. A mask-bit assignment above that limit
cannot be a routine promotion. The receiver MAY use bounded exact-part recovery
or retain the existing route until it can afford a larger trial.

After the promotion, the receiver MUST collect fresh observations at the new
load before another promotion into that connection. Old wins MUST NOT justify
an arbitrary sequence of load increases. A miss or peer loss triggers immediate
bounded additions; pruning waits for `STABLE_WINDOWS >= 2` successful windows.

### Adjust the shared byte budget

The baseline uses additive increase and multiplicative decrease on `W`, not on
a transport congestion window. Local policy fixes a monotonic control epoch
of duration `CONTROL_INTERVAL`, bounds `W_min <= W_initial <= W_max`, an
increment `Delta`, a decrease factor `0 < beta < 1`, and a utilization threshold
`0 < utilization_threshold <= 1`.

A loaded trial snapshots a cohort of concurrently outstanding assigned copies
and the current `W`. Success requires timely valid delivery of at least
`utilization_threshold * W` payload bytes from that cohort, successful observed
block reconstruction, and acceptable uncensored deadline yield. Sequential
small transfers MUST NOT count as evidence for one large outstanding budget.
The receiver MUST have actually offered the trial load. Estimated unused
capacity, canceled copies, or an idle epoch MUST NOT count as delivered trial
bytes. Sparse or unfinished samples postpone adjustment. Each completed trial
MUST support at most one increase; the receiver MUST NOT reuse its success.
Repeated failure requires deadline misses on at least `MIN_FAILURE_BLOCKS`
distinct comparable blocks with eligible uncanceled demand. Local verification
overload MUST NOT be classified as evidence against the remote peer.

```text
W_next = max(W_min, floor(beta * W))    if repeated eligible failure
W_next = min(W_max, W + Delta)         else if successful loaded trial
W_next = W                            otherwise
```

Each connection MUST update its budget at most once per epoch. The receiver
SHOULD require at least two failure blocks and use the settling rule for
increases. It MUST cap `Delta`, trial bytes, and concurrent trials globally.
Each failed block MUST count toward at most one decrease. Failure histories
MUST have a bounded age and reset after an adjustment.
Failure takes precedence over increase. Lowering `W` changes future allocation;
it does not revoke grant credit or establish peer misconduct.

Budget failure means the assigned route did not meet the delivery target. It
does not identify physical congestion: upstream waiting can produce the same
result. Proposer-specific losses SHOULD first shift that proposer's routes.
If all alternatives stall together, the receiver SHOULD freeze promotions and
budget increases, retain exploration, and invoke bounded recovery. Pairwise
comparison supplies no winning alternative in that case.

Transport congestion control MUST pace each connection. Sender queue bounds
and service budgets MUST remain active even while the controller learns.
A receiver MUST NOT allocate all parts from raw first-arrival counts or
assume random source distribution eliminates the need for exploration.

This mechanism borrows randomized comparison from
[power of two choices](https://brooker.co.za/blog/2012/01/17/two-random.html).
The [underlying survey](https://www.eecs.harvard.edu/~michaelm/postscripts/handbook2001.pdf)
studies queueing models and delayed information. Those results do not establish
this controller's behavior with persistent subscriptions, unavailable data,
heterogeneous routes, or Byzantine peers.

### Evaluate block outcomes

For a completed observation, let `u` be distinct timely valid parts, `c` all
timely valid copies, and `L = body_bytes`:

```text
decode_deficit = max(0, k - u)
duplicate_parts = c - u
payload_overhead = (received_payload_bytes / L) - 1
```

The overhead metric measures payload relative to the body, including parity,
padding, duplicates, and late parts through the observation cutoff. Wire
accounting MUST separately include proofs, headers, and metadata.
Reports MUST include reconstruction p50/p95/p99, deadline misses,
fallback frequency, and time spent degraded. A low duplicate count alone is
not success.

## 8. Profile choices and conformance

The profile MUST fix these before implementations claim interoperability:

| Item | Draft choice |
| --- | --- |
| Part payload | 64 KiB default; selected profile fixes the value |
| Codec and parity schedule | Section 2 fixes systematic Reed–Solomon over GF(2^16), little-endian elements, and `ceil(k/4)` parity parts |
| Part-mask width and hash | Balanced mapping in section 3; `P` and `H` `TBD` |
| Proposer authentication | PoW-bound key and signed metadata; chain binding and signature scheme `TBD` |
| Serialization | Message discriminators, integer encoding, Merkle proof format, and canonical key encoding `TBD` |
| Resource bounds | Frame and field caps, part count, grants, height span, candidates, selectors, queues, and retention `TBD` |
| Regulation | Negotiated sender budgets, cadence/burst allowance, response work, and incomplete-frame deadline `TBD` |

Local policy MUST define reconstruction and recovery deadlines, failure groups,
safety margin, and retention limits. Controller configuration MUST also define:

- Context history bounds, staleness, material load changes, workload forecasts,
  and the startup size/concurrency scenario.
- `CONTROL_INTERVAL`, `W_min`, `W_initial`, `W_max`, `Delta`, `beta`,
  `utilization_threshold`, and acceptable yield.
- `MIN_RACE_BLOCKS`, `MIN_FAILURE_BLOCKS`, `race_epsilon`, `SWITCH_THRESHOLD`,
  `STABLE_WINDOWS`, and the settling interval.
- `exploration_fraction`, `exploration_initial`, `exploration_cap`,
  `CHALLENGE_MIN_INTERVAL`, its jitter bound, `CHALLENGE_MAX_BLOCKS`,
  `CHALLENGE_MAX_AGE`, and `MAX_CHALLENGES`. The cap MUST fund at least one
  minimum trial with control overhead. The block and age caps SHOULD allow
  warming plus `MIN_RACE_BLOCKS` observations at the expected proposer rate.
- Active-proposer queue bounds, grant isolation, migration, recovery, and
  cancellation-tail byte/time limits, plus global limits on concurrent trials.

These controller values require simulation. The control policy is local; peers do not
negotiate a common estimator or trust each other's measurements.

The implementation MUST test:

- Canonical encoding, minimum and maximum fields, malformed proofs, arithmetic
  overflow, and allocation bounds for every message.
- Header/body separation, transaction-count prefixes, trailing bytes, total
  block-size enforcement, body changes invalidating cached parity, and header
  changes requiring a new signature but not a new body encoding.
- Header admission before any part work, missing parent context, forged key
  binding, signed equivocation, incorrect parity, and consensus-invalid bodies.
- Grant conservation, exhaustion, cancellation, renewal, retirement, stale
  responses, reordered streams, and parts crossing unsubscribe or completion.
- Proposer and block inheritance, empty overrides, reconnect cleanup, terminal
  completion, and deduplication across grants and services.
- Alternating known proposers, preserving their distinct routes and votes,
  an unfamiliar key using defaults, and changed ingress behind an existing key.
- Two-peer reciprocal waits, larger isolated cycles, arbitrary block entry
  points, cold proposers, skewed part placement, peer loss, and failed repair.
- A nearby high-capacity proposer, congestion after route concentration,
  correlated failures, withholding, false completion claims, and recovery of
  a previously slow peer.
- Mixed block sizes, same-height forks, burst arrivals, and simultaneous
  proposers sharing one connection budget. Holding block size fixed while
  varying concurrency MUST change aggregate assignment accounting.
- Small-block wins followed by large blocks, a new proposer with no samples,
  idle periods, subscription warming, credit exhaustion, and stale histories.
- Races censored by `FullBlock` or unsubscribe, local verification overload,
  correlated part arrivals within one block, and peers that perform well only
  during probes. Completion MUST NOT create artificial deadline failures.
- Challenge funding under idle time, block bursts, duplicate announcements,
  invalid bodies, reconnects, key churn, and block replay after history eviction.
  Cumulative charged authorization MUST obey the exploration bound.
- Challenge responses authorized by overlapping grants, renewal, cancellation,
  and large blocks exhausting a small trial's credit. A route label alone
  MUST NOT bypass exploration accounting.
- Unequal proposer rates, queue fairness, an unaffordable or expired trial,
  empty mask selections for small blocks, warming without acknowledgements,
  and inconclusive expiry. Timers MUST NOT generate unbounded trial starts.
- Budget growth without loaded evidence, repeated failures, simultaneous
  promotions, one mask-bit assignment exceeding the migration budget, and
  cancellation tails. No context or key change may create a fresh connection budget.
- A receiver bottleneck shared by every peer, an upstream source stall, and
  other receivers adapting concurrently. The simulator MUST include control
  delay, sender scheduling, and feedback-driven changes to upstream routes.
- Per-peer isolation under queue pressure, malformed-frame floods, bounded
  decoder/verifier faults, and continued control-stream progress.

Stateful tests MUST compare production admission with an independent reference
model. Conformant sequences MUST NOT produce misconduct disconnects.
Deterministic cases MUST cover each declared rule; random generation alone is
insufficient. Minimized failures MUST serialize into replayable scenarios.
A finite-state explorer MUST report an unfinished frontier as incomplete.

Simulation MUST set explicit delivery and resource targets before tuning the
controller. It MUST report assumptions about honest connectivity and failure
correlation. No simulation result may imply unconditional connectivity from
local subscription counts.

Controller experiments MUST compare this policy with static equal shares,
random assignment, global-best selection, and pairwise races without a byte
budget. An offline informed scheduler MAY provide a reference bound, but the
report MUST disclose the information unavailable to real receivers. Experiments
MUST vary block size, arrival rate, burst concurrency, proposer locality,
asymmetric peer bandwidth, and correlated failures independently. Reports MUST
include adaptation time, control bytes, duplicate bytes, and queue peaks as
well as the block outcomes in section 7. No finite controller can meet a fixed
latency target when offered work exceeds available service capacity.

Codec benchmarks MUST compare batch and incremental implementations of the
selected code on identical arrival traces and redundancy.
They MUST measure encoding/root latency after mining, useful work before the
last required part, the remaining decode tail, root verification, CPU time,
and peak memory. They MUST include systematic-only reception, parity-heavy
reception, withheld parts, invalid codewords, and concurrent assemblies.
Conformance tests MUST extend the vector in section 2 with nonzero high bytes,
field reduction, padding, and recovery from every `k`-part subset for small
codewords. An incremental-input API alone does not demonstrate overlapped work.

## Source locations

- [Current header-sync messages](../../crates/zakura-network/src/zakura/header_sync/wire.rs)
- [Header admission](../../crates/zakura-header-chain/src/transition/planner/event_effects/header_admission.rs)
- [Current consensus header](../../crates/zakura-chain/src/block/header.rs)
- [Current block serialization](../../crates/zakura-chain/src/block/serialize.rs)
