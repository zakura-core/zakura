# Dogwood: block propagation for Zcash

Proof of work makes the next block's entry point unpredictable. Propagation
delay increases the orphan rate. We need low latency from any proposer across
peers with unequal bandwidth.

Dogwood splits each block body into erasure-coded stripes. The proposer sends
each part once, to one of a few nearby peers called entry peers. Every part
assigned to the same entry peer forms a lane. Each node first pulls the parts
its neighbours announce, and learns from their timing which neighbour delivers
each lane first. It then asks that neighbour to push the lane, but only where
pushing is cheap. Parity lets a node stop after any `k` parts of a stripe, so it
can skip slow neighbours instead of waiting for them.

The tradeoff is state: pushed lanes avoid a request round trip, but they need
learning, cycle avoidance, and a way to stop. The
[protocol specification](../specs/dogwood.md) defines the rules. This document
explains the design. The [experiment report](dogwood-experiments.md) records
the local codec and controller experiments behind the first draft.

## Performance targets

These planning targets need network testing and do not set consensus parameters.

**Throughput:** At 2 KiB per aggregated transaction after Tachyon, 50,000 TPS
requires 819.2 Mbps (97.7 MiB/s) of block-body traffic before parity, transport
overhead, and recovery. Delivering a block accumulated over `T` seconds in `D`
seconds requires at least `819.2 × T / D` Mbps of body ingress. Relays also need
upload capacity for each forwarding copy.

**Latency:** Aim to reach 90% of nodes in about 400 ms, with a target below
500 ms. We provisionally allow 150 ms for three roughly 10,000 km fiber hops
and 250 ms for indirect routes, serialization, queueing, verification, and
reconstruction. Actual geography, topology, and block size determine feasibility.

**Robustness:** Support any proposer within the available capacity and paths.
Nodes adapt their routes as bandwidth and topology change. Pulls, repair, and
block download preserve delivery while routes adapt.

### Current evidence

A test bench ran the `flow` prototype on 80 DigitalOcean nodes with 8 vCPUs
each, spread over 11 regions, in a random 28-regular overlay. The nodes had
synchronized clocks, no chain, and no Byzantine peers. `t90` is the time by
which 90% of non-proposer nodes assembled the block.

| Workload | Router | `t90` |
| --- | --- | --- |
| 32 MiB, one block at a time, rotating proposers | Push after learning | 0.40 s |
| 64 MiB, one block at a time, rotating proposers | Push after learning | 0.59–0.61 s |
| 32 MiB blocks streamed at 110 MiB/s, one or many proposers | Push after learning | 0.40–0.48 s per block; 0.77 s for the worst-placed proposer |
| 128 MiB, random proposers, parity 1/3 | Pull only; push with a child budget | 1.03 s; 0.91 s |
| 256 MiB, random proposers, parity 1/3 | Pull only; learned push at parity 0.1 | 1.59 s; 2.16 s |
| Two 256 MiB blocks at once | Pull only; learned push at parity 0.1 | 2.73 s; 3.85 s |

Every block reached every node in every trial. At 32 MiB the fleet meets the
latency target and sustains the planning throughput. At 256 MiB, transport
queueing dominates: the busiest pushing relay uploads about four times the
block. Pulling with parity keeps each node's received bytes at
1.02–1.05 times the body and needs no repair. These results motivate this
revision's pull-first design.

## Tradeoffs

### Protocol comparison

Each node limits its forwarding peers as the network grows. Like
[Gossipsub](https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.0.md#gossipsub-the-gossiping-mesh-router),
Dogwood uses bounded local forwarding. It forwards per lane rather than per
message.

[Rotor](https://www.anza.xyz/blog/alpenglow-a-new-consensus-for-solana) uses
erasure coding and a single relay layer to reduce the proposer's upload burden
and propagation hops. Dogwood's entry peers play the role of that relay layer:
the proposer uploads each part once. Rotor's routes depend on a known proposer
and validator set; Dogwood's entry peers are simply the proposer's nearest
peers.

Celestia's Pull-Based Broadcast Tree
([PBBT](https://github.com/celestiaorg/celestia-app/blob/9c1e04d1dfd090531252f16f34293242d04b1157/specs/src/recovery.md))
discovers routes as parts propagate. It pipelines authenticated `Have` and
`Want` messages with data transfer. Dogwood starts the same way: every lane
begins in pull mode. The `Have` timing then trains a learner.

Like [DOG](https://github.com/cometbft/cometbft/issues/3263), Dogwood uses local
delivery measurements to set push routes. The receiver alone decides to push a
lane, and it returns the lane to pull when a pull would be faster. The
network therefore fades between pull and push per lane.

### Pull, push, and parity

A push saves the request round trip, but a pushing parent sends every part of
its lane, including parity the child does not need. Across a network,
learned push also concentrates load on the fastest relays. A pull costs one
round trip, but it asks only for parts the node still needs and lets the node
stop at `k` per stripe.

Parity changes that balance. With one third extra parity, a pulling node can
ignore its slowest announcers and still decode. At 256 MiB, pull only beat
learned push by about 25% and received about a fifth fewer bytes. At 128 MiB, push
with a child budget still won by about 10%. Dogwood therefore keeps push where
it is cheap: the child chose it, the parent advertises a short forwarding wait,
and the parent's budget has room. Everything else stays pull.

## Parts, stripes, and lanes

The proposer splits the block body into 64 KiB source parts. It groups them
into stripes of at most 64 parts and encodes each stripe with systematic
Reed–Solomon over GF(2¹⁶), adding `ceil(k / 3)` parity parts. Any `k` distinct
correctly encoded parts of a stripe reconstruct it. A 128 MiB body has 32
stripes of 64 source and 86 coded parts.

`HeaderMeta` wraps the consensus header with the coding parameters, a Merkle
root over every part, the proposer's entry peers, and proposer authentication.
Each `Part` carries a proof against that root. Nodes verify parts before
forwarding or decoding them. A verified proof also yields the stripe's own
subtree root, so a node can check each stripe as soon as it decodes.

The entry-peer list assigns part `i` to entry peer `entry_peers[i mod R]`. The
parts assigned to one entry peer form that peer's lane. A lane is named by its
entry peer alone, so two proposers that share an entry peer feed the same lane,
and routes learned under one proposer serve the next. In the two-proposer
experiments, keying lanes by proposer lost on every dataset.

### Block-part lifecycle

This diagram follows one part of one lane. A has already promoted the
lane to push at E; B still pulls it.

```mermaid
sequenceDiagram
    participant P as Proposer
    participant E as Entry peer
    participant A as Node A
    participant B as Node B
    A->>E: Subscribe (lane E)
    P->>E: HeaderMeta, BlockDone
    E->>A: HeaderMeta
    A->>B: HeaderMeta
    P->>E: Part (seed slot)
    E->>E: Verify part
    E->>A: Part (pushed)
    A->>A: Verify part
    A->>B: Have
    B->>A: Want
    A->>B: Part, WantEnd
    Note over A,B: Repeat for other parts and lanes
    B->>A: StripeDone (k parts held)
    B->>B: Decode, check stripes, assemble
    B->>A: BlockDone
```

`StripeDone` and `BlockDone` travel on the control stream, so they overtake
queued parts and stop the sender early. Parts already in flight still arrive and
count as surplus. Reconstruction checks do not replace consensus block
validation.

### Messages

| Message | Purpose |
| --- | --- |
| `HeaderMeta` | Announce the header, the coded body's commitment, and the entry peers. |
| `Subscribe` | Ask a peer to push a lane for all future blocks or one block, or every part of its own blocks. |
| `Unsubscribe` | Stop a pushed lane. |
| `Refuse` | Decline a subscription, or stop pushing one. |
| `Want` | Ask for up to `count` parts of one stripe from an allowed set. |
| `StripeDone` | Report `k` parts of a stripe; stop sending that stripe. |
| `BlockDone` | Report an assembled block; stop sending that block. |
| `Advert` | Share per-lane arrival delays and the forwarding wait. |
| `Part` | Send one part with its proof. |
| `Have` | Announce verified parts, in band behind queued parts. |
| `WantEnd` | End a `Want`, behind its parts. |
| `SubscriptionEnd` | Answer `Unsubscribe` and end the subscription, behind its parts. |

## Pull first, then push

Every lane starts in pull mode. When a neighbour announces a part the node
lacks, the node asks that neighbour for it, unless the stripe already has
enough parts on the way. A `Want` names a stripe, a count, and an allowed set,
so any server can answer with any parts it holds. A node that asks two servers
gives them disjoint allowed sets, so they never send the same part.

Each node samples a quarter of the parts by hash. For each sampled part, it
measures every neighbour's lag: when that neighbour's copy or `Have` arrived,
relative to the part's first arrival. `Have`s travel on the data stream behind
queued parts, so a neighbour's lag includes its queue. After each block, the
node ranks each lane's candidates by an EWMA of these lags.

When the same candidate leads a step and advertises a short forwarding wait,
the node subscribes to it. The parent then pushes the lane for every future
block. When a parent falls behind the best alternative by more than a round
trip for three steps, the node unsubscribes, and the lane returns to pull.
In the 256 MiB experiments, the first block, pulled everywhere, arrived about
twice as fast as the pushed blocks that followed.

```text
Pull:       A ··Have··> Node ──Want──> A ──Part──> Node
Promote:    Node ──Subscribe──> A
Push:       A ──Part──> Node
Demote:     Node ──Unsubscribe──> A ... A ──SubscriptionEnd──> Node
```

**Avoiding cycles.** A node accepts a parent only if the parent's advertised
arrival delay for the lane in that block, measured from the proposer's signed
`created_ms`, is lower than the node's own. Parents therefore always point
upstream, and no cycle forms within a block, even under clock skew. A node also
leaves a parent that subscribes to it for the same lane, which breaks cycles
that form across blocks.

**Falling behind.** A node that runs several blocks behind still learns. It
keeps each neighbour's adverts for the last 64 blocks and compares against the
learned block's entry. An earlier prototype kept only the newest advert; a node
that fell behind then found no candidates and kept a slow parent.

**Push where cheap.** A parent bounds its children with a budget measured in
blocks: each accepted lane costs `1/R` of a block. A parent at its budget
refuses new subscriptions, and the child keeps pulling. Because lanes are
shared across proposers, the budget binds per node. A budget of about 1.4
blocks cut the busiest relay's upload to 2.7 blocks and won at 128 MiB. The
production value is still open.

## Entry peers

Sending a whole block to every direct peer multiplies proposer upload. Dogwood
instead has the proposer send each part once, to one entry peer, and lets the
entry peers' lanes spread from there. The proposer draws its entry peers from
neighbours whose minimum round-trip time is at most twice the median. Far entry
peers delayed whole stripes in the experiments: when a European proposer sent
parts to Sydney, every other node waited for those parts to come back.

The signed metadata names the entry peers and the proposer's transport identity.
An entry peer therefore accepts seed parts only from that identity, and no relay
can rewrite which entry peer carries which lane. The proposer also sends
`BlockDone` right after `HeaderMeta`, so no neighbour forwards the block's parts
back to it.

A proposer with many distant neighbours still sends its parts far away. The
worst-placed proposer in the experiments had nine slow neighbours out of 28 and
stayed the slowest. Excluding slow peers from the entry-peer median, or capping
entry-peer round-trip time, remains untested.

## Bounding what peers can send

QUIC's receive window does not bound a dishonest peer: the node reads and drops
each unwanted part, so the window refills at once. The first draft added part
and byte credit for this. Dogwood instead keeps a wanted set per connection.
Node N wants part `i` from peer A only in these cases:

| N wants `i` from A while | Until |
| --- | --- |
| N subscribes to `i`'s lane at A | A's `SubscriptionEnd` arrives |
| N's live `Want` to A allows `i` | A's `WantEnd` arrives |
| A is the block's proposer and N is `i`'s entry peer | The part arrives |

N wants each part at most once from each peer. An unwanted part or a second
copy disconnects the peer before proof verification. A peer can therefore send
at most one copy of each part that N asked for, so a byte budget adds nothing.

Ends keep the bound exact when messages cross. A parent may queue parts before
the `Unsubscribe` arrives, so the lane stays wanted until the parent's
`SubscriptionEnd`. `SubscriptionEnd` and `WantEnd` travel on the data stream
behind every part they authorized. In this diagram, solid arrows use the control
stream and dashed arrows use the data stream.

```mermaid
sequenceDiagram
    participant N as Node N
    participant A as Parent A
    N->>A: Subscribe (lane E)
    A-->>N: Part, Part, ...
    N->>A: Unsubscribe (lane E)
    A-->>N: Part (queued earlier, still wanted)
    A-->>N: SubscriptionEnd (lane E)
    Note over N: Lane E is no longer wanted from A.<br/>Another lane-E part from A disconnects A.
```

A parent that declines or stops pushing sends `Refuse`, which asks for the
`Unsubscribe`. `Refuse` shares the data stream with `SubscriptionEnd`, so it
always arrives first. No timer ends a subscription or a `Want`, so a slow peer
never looks like a lying one.

Three rules bound traffic about blocks that a node cannot check yet:

- **Variant names.** Messages name a block by its metadata variant, not its
  consensus hash. A node that holds another variant sees an unknown block, never
  a bad proof. An equivocating proposer therefore cannot make honest peers
  disconnect each other.
- **Early record.** Messages about unknown blocks wait in a small per-peer
  record. When the record fills, the node pauses that peer's data stream. The
  control stream never pauses, because it carries the metadata that resolves
  the block.
- **Propagation window.** Coded propagation covers only the last 16 blocks, so a
  node can remember every block it retired and reject repeats.

Every message follows the [peer message regulation](peer-message-regulation.md)
model. A node disconnects only when no conformant peer could have sent the
message, and the spec names the sender obligation behind each disconnect.
Crossings and local capacity return `Drop`.

## Recovery

Parity makes the normal path forgiving: a node needs any `k` parts of each
stripe, from any mix of pushes and pulls. When no new part arrives for two
seconds, the node enters repair. It asks peers that reported `StripeDone` or
`BlockDone` for the missing parts, in bounded rounds. A monotonic deadline from
metadata admission caps the whole attempt, and the node then downloads the full
block. A failed re-encode check or conflicting metadata skips straight to block
download. Recovery never moves backward.

In the 128 and 256 MiB experiments, repair fetched 5.7–9.4% of new parts under
learned push with low parity. With a child budget it fetched 0–1.1%, and with
pull only none.

## Adversarial neighbours

Delaying a rival's block pays: a delay of `Δ` lets another miner orphan it with
probability about `1 − e^(−Δ/T)`, which is 3% for 2.5 s at a 75 s block spacing
and 10% at 25 s. Dogwood therefore has to deliver when many of the proposer's
peers withhold, in three tiers:

| Tier | Mechanism | Delivers while |
| --- | --- | --- |
| Fast path | Entry peers, lanes, pulls, and repair | Honest neighbours hold `k` parts of every stripe |
| Latency floor | The miner mesh | Miners peer with each other directly |
| Liveness floor | Header sync and full-block download | One honest path exists |

**Withholding entry peers.** A stripe decodes from any `k` of its `k(1 + p)`
parts, so it survives entry peers that withhold less than `p / (1 + p)` of them:
25% at one-third parity, 50% at parity 1 for twice the proposer upload. Pulling
nodes still stop at `k` parts per stripe, so receivers pay little for more
parity. Beyond that share, no stripe decodes until repair starts after the 2 s
stall. An attacker reaches the share cheaply by colocating with a pool to win
the round-trip ranking and by filling the pool's inbound slots. The proposer
therefore prefers outbound and configured peers, takes at most one per network
group, and picks at random among the nearby ones.

**Silent parents.** An attacker that earns a parent slot by being fast can
withhold one lane of a chosen block and serve every other block, so a node also
pulls a hole from any neighbour that announced it `STARVE_MS` earlier.

**Miner mesh.** Every second on a stale tip wastes a miner's work, and a miner
that builds on a block wants every other miner on that tip, so miners want to
send, receive, and relay blocks among themselves. A proposer subscription
carries the first hop: each miner subscribes at the miners it peers with, and a
proposer pushes every part of its own blocks to those subscribers beside its
entry peers until their `StripeDone`s stop it. Mesh subscribers then serve every
lane to their neighbours, which also covers lanes that entry peers withheld.
The entry-peer and mesh lists reveal a miner's peers, so a miner that wants to
hide its topology can propose through a relay it controls.

Open work:

- **Uneven assignment.** Give trusted entry peers the source parts and random
  ones the extra parity. `HeaderMeta` would then carry a share per entry peer.
- **Proof of mining.** Let a miner sign its transport identity with the
  coinbase-bound key of a recent block, so nodes can confirm mesh peers without
  configuration.
- **Parity.** Choose parity from the withholding share to survive, and measure
  what it costs pushed lanes.
- **Adversarial fleet.** The fleet had synchronized clocks, no chain, and no
  Byzantine peers. Measure normal delivery and fallback separately under
  realistic geography, workloads, changing capacity, and adversarial peers.

## Encoding and verification

A part's Merkle proof establishes membership in the committed codeword. After a
stripe decodes, the node re-encodes it and checks the stripe's subtree root.
After every stripe passes, it assembles the block and submits it for consensus
validation. Forwarding verified parts never waits for decoding. Changing the body
requires new parity and a new Merkle tree. Changing only the header preserves
both.

Stripes bound each decode to 64 parts, and decoding overlaps the transfer. In
the fleet traces, the tail after the last part was 115–180 ms at `p90`. Coding
a 256 MiB body as one codeword instead took about 2 seconds to decode. The
stripes' shared tree keeps one root in the header while letting each stripe be
checked alone.

The proposer still encodes the whole body before it signs the root. A future
profile could commit per-stripe roots so the proposer can send each stripe as
soon as it is encoded.

## Headerchain integration

Headerchain handles header validation, fork choice, and header recovery. The
node checks the complete header, including proof of work and contextual
difficulty, before authenticating metadata or allocating assembly state. It
then forwards `HeaderMeta` to every peer, including peers without
subscriptions.

The header does not commit to the part root, the entry peers, or a Dogwood
proposer key. The wrapper therefore needs a signature from a key bound to the
mined block; an arbitrary wrapper key would let anyone attach conflicting entry
peers to someone else's proof of work. The [candidate
binding](../specs/dogwood.md#bind-the-proposer-to-the-proof-of-work) commits a
key in a coinbase output and proves its transaction's membership in the block. A
txid proof alone cannot authenticate a key in the V5 coinbase input script. The
post-Tachyon adapter remains open.

Nodes accept at most one authenticated metadata variant per block. An
authenticated conflict stops coded propagation for that block and triggers
block download. The
[W2 payload profile](../specs/dogwood.md#candidate-payload-profile-w2) defines
candidate bytes, commitments, and signatures.

## What changed from the first draft

| First draft | This revision | Evidence |
| --- | --- | --- |
| One codeword over the whole body, 25% parity | Stripes of 64 source parts, one-third parity | One codeword takes about 2 s to decode at 256 MiB; parity 1/3 cut the 256 MiB `t90` by a quarter. |
| Receivers subscribe to the proposer; `SeedOffer` candidate | The proposer sends each part once to a signed list of entry peers | Entry peers near the proposer removed the detour through far entry peers. |
| Part masks with proposer and block scopes | Lanes keyed by entry peer, with standing and block scopes | Routes keyed by proposer lost in replay; per-entry-peer lanes carry across proposers. |
| Two suppliers per part and a coverage margin | One parent per lane, plus pulls | Pulls fill gaps at 1.02–1.05 times the body. |
| Byte-budgeted pairwise races with AIMD | Pull first, EWMA lag ranking, promote and demote | The pairwise controller did not consistently beat static allocation; pull-first reached 90% push within two to five blocks. |
| Immutable grants of part and byte credit | The wanted set, with in-band ends | Application credit windows lost throughput; the wanted set bounds dishonest peers without them. |
| `FullBlock` | `StripeDone` and `BlockDone` | Stopping per stripe avoids about 9% of bytes that were parity sent after decoding was possible. |
| `Continue`, `Drop`, `Delay`, `Disconnect`, `LocalFault` | The regulation model's four results | Capacity waits belong to the serving loop. |

The first draft's headerchain admission, key binding, variant rule, send-once
rule, no-echo rule, and block-download fallback remain unchanged.

## Parameter tuning

The [spec parameter registry](../specs/dogwood.md#parameter-registry) owns
starting values and change rules. They reproduce the fleet experiments and
still need production tuning. Three tradeoffs matter most:

- **Parity:** more parity lets pulls skip more slow neighbours but raises
  proposer upload and every push. One third helped at 256 MiB; one half added
  little.
- **Push threshold:** promoting sooner saves round trips but builds hubs.
  The child budget and the advertised wait decide where push is cheap.
- **Stripe size:** smaller stripes decode sooner and narrow the tail. They also
  deepen proofs and multiply per-stripe messages. Sixteen stripes beat eight at
  64 MiB.

Local policy can change within the spec's bounds. Record parameter versions
with results. Wire changes require negotiation.

## Open work

- **Choose the child budget.** Set its default and test it with pull-first at
  every block size.
- **Settle per-proposer state.** Lanes, parents, and pull windows are shared
  across proposers by design. Measure whether any of them should split.
- **Place entry peers for poorly placed proposers.** Test excluding slow peers
  from the entry-peer median and capping entry-peer round-trip time.
- **Pipeline the proposer.** Commit per-stripe roots so the proposer sends each
  stripe once it is encoded.
- **Complete interoperability.** Select the codec construction and vectors, the
  production chain adapter, transport negotiation, and aggregate resource
  limits. W2 payload encoding alone does not complete these choices.
