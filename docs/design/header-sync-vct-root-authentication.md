# Header-sync VCT root authentication

Status: proposed

## 1. Summary

Zakura header sync receives peer-supplied note-commitment roots and the
additional ZIP-221 leaf inputs needed by verified commitment tree (VCT) fast sync.

These values must be authenticated before they enter the authoritative
commitment-root index.

This design makes `zakura-state` the owner of a single, durable, ascending
root authentication frontier. Header discovery remains responsible for
finding and committing valid headers. Root-carrying header responses are retained
in reactor memory through the VCT handoff region until the state-owned
authentication frontier can consume them. Authentication and promotion remain gated
by completed-checkpoint coverage, so retained payloads above the current completed
checkpoint wait until that frontier advances. A root-authentication network lane
fetches an overlapping canonical range only when the retained responses have a gap
or authentication rejects a payload. State verifies each range against its current
history-tree frontier and atomically persists only the confirmed root prefix.

The central invariant is:

> Every row visible through the authoritative commitment-root index has already
> been authenticated against the selected header chain.

The design retains the strongest parts of the earlier
[PR #62](https://github.com/zakura-core/zakura/pull/62):

- a sealed `VerifiedHeaderCommitmentRoots` type;
- one-header overlap between root-carrying ranges;
- a persisted confirmed-prefix invariant;
- reconstruction from durable authenticated state;
- protection for authoritative roots produced by committed block bodies.

It deliberately does not retain PR #62's network-owned history tree, lazy rebuild, or reanchor machinery. State already owns canonical chain
selection, durable writes, and block-commit history trees, so it also owns the header-root authentication frontier.

## 2. Goals

1. Reject incorrect roots at the earliest practical point after their
header chain becomes canonical.
2. Never persist unverified peer roots in the authoritative root index.
3. Preserve the ability to identify the peer that supplied an invalid root payload.
4. Support forward header discovery, restart, and bounded header reorganization
   without duplicating chain ownership.
5. Keep failure behavior fail-closed: missing or invalid roots cannot influence VCT state.
6. Reuse the existing ZIP-221 verification implementation rather than introducing new cryptography.
7. Avoid re-fetching roots already delivered with committed headers unless their
   retained payload was lost, invalidated, or rejected.
8. Retain root-carrying payloads with committed headers through the VCT handoff so
   later checkpoint closure does not force a re-fetch; gate authentication and
   promotion on completed-checkpoint coverage.

## 3. Non-goals

- Authenticating roots above the VCT fast-sync handoff.
- Replacing ordinary block semantic verification.
- Trusting roots because they came from multiple peers.
- Persisting peer identities as consensus state.
- Making root availability a prerequisite for accepting independently valid
  headers.
- Supporting multiple durable candidate header branches.
- Fetching headers below the verified body/history-tree authentication base.
- Preserving unauthenticated retained roots across restart.

## 4. Cryptographic constraint

A block header commits to the history tree as of its parent. Consequently, roots
for height `H` are authenticated by header `H + 1`, not by header `H`.

For a contiguous delivery:

```text
headers: H ... E
roots:   H ... E
```

verification can confirm only:

```text
roots:   H ... E - 1
```

The root at `E` remains unconfirmed until a successor header is supplied.

The existing
`zakura_chain::parallel::commitment_aux_verify::verify_supplied_roots_from_parts`
implements this rule. It also performs the direct checks needed outside the
applicable ZIP-221 history-tree versions:

- Sapling roots below Heartwood;
- empty Orchard roots below NU5;
- empty Ironwood roots below NU6.3.

The wire payload already contains the remaining header-only leaf inputs:

- Sapling, Orchard, and Ironwood roots;
- shielded transaction counts;
- the current block's auth-data root.

Full block bodies are not required for header-layer authentication. They are still
required later to prove that transaction counts and the auth-data root match the
downloaded body.

## 5. Architectural decision

### 5.1 Ownership

Responsibility is divided as follows.

`zakura-chain` owns:

- the root payload types;
- ZIP-221 history-tree construction;
- header commitment verification;
- the sealed verified-root result type.

`zakura-network` owns:

- request scheduling and response correlation;
- frame and shape validation;
- peer, session, and request attribution;
- in-memory retention of root-carrying committed-header responses through the
  VCT handoff region;
- retries and peer policy;
- the one-header overlap used by the root-authentication lane.

`zakura-state` owns:

- canonical header selection;
- checkpoint and contextual header validation;
- the durable root-authentication frontier;
- atomic root promotion;
- startup restoration;
- synchronization with authoritative roots produced by committed bodies.

`zakurad` owns:

- translating network actions into state requests;
- returning typed state outcomes to the reactor;
- retaining the supplying peer identity for the duration of each request.

### 5.2 Two logical lanes

Header discovery and root authentication are related but distinct.

The header-discovery lane:

1. downloads headers;
2. validates their wire shape, proof of work, linkage, and checkpoint termination;
3. asks state to perform contextual validation and canonical chain selection;
4. writes headers and body-size hints;
5. retains aligned roots and their peer attribution in reactor memory after
   the headers commit, through the VCT handoff region even when ahead of the
   current completed checkpoint;
6. does not write peer-supplied roots to the authoritative index.

The root-authentication lane:

1. follows the state-published authenticated root frontier;
2. consumes the next contiguous overlapping range from retained forward responses;
3. submits the complete single-peer response to state while its attribution is live;
4. requests replacement overlapping ranges from peers when retained coverage is
   missing, invalidated, or rejected;
5. persists only the prefix confirmed by the successor header;
6. advances the durable frontier after the database write succeeds.

Forward responses are the primary source of roots. Header discovery may run ahead
without forcing the same roots to be downloaded again: committed root-carrying
payloads wait in reactor memory until their predecessor becomes the durable
authentication frontier and their successor is covered by a completed checkpoint.
The dedicated authentication request path is recovery for gaps and invalid
payloads, not the steady-state supply path. After restart, it is also a bounded
catch-up lane: it eagerly queues overlapping requests across the missing durable
header lead, assigns them incrementally as peer slots become available, and keeps
the network window full while state authenticates one range at a time.

During catch-up, `AuthenticateRoots` closes the gap below the durable header tip.
Forward header discovery may pause so peer slots prefer that work; pausing Forward
does not pause root recovery or discard already committed headers. The body-download
target is published separately from the durable best-header tip and never advances
past root-authenticated coverage in the VCT region.

### 5.3 Why state owns the frontier

The history tree is meaningful only relative to a particular canonical chain.
State already decides:

- whether an anchor is known;
- whether a range is contextually valid;
- whether it terminates at the expected checkpoint;
- whether a competing suffix has more work;
- whether a reorganization is permitted;
- whether a full block already made a row authoritative.

Keeping the root frontier in the reactor duplicates this ownership and creates races between an in-memory tree and durable chain changes. PR #62 required parent-hash tracking, asynchronous tree reconstruction, stale-result guards, lazy rebuilds, and tip reanchoring for this reason. Moving the frontier into state removes those cross-component synchronization paths.

## 6. Durable state

State maintains a header-root authentication frontier:

```rust
struct HeaderRootAuthFrontier {
    confirmed_height: block::Height,
    confirmed_hash: block::Hash,
    history_tree: HistoryTree,
}
```

The history tree includes authenticated roots through `confirmed_height`.
`confirmed_hash` binds it to the exact canonical header branch.

The durable frontier and newly authenticated root rows are written in the same
database transaction. The in-memory frontier is replaced only after that transaction
succeeds.

The authoritative commitment-root index contains two kinds of rows, both trusted:

1. roots authenticated by the header-root lane;
2. roots derived from semantically or checkpoint-verified block bodies.

There is no provisional peer-root row in this index.

The index is contiguous through `confirmed_height`: every height covered by the durable root-authentication frontier has exactly one authoritative row. Promotion can only append a contiguous prefix beginning at `confirmed_height + 1`, and body commit can only confirm or atomically replace an existing row. Neither path can create an interior gap.

Trust is determined by the write boundary, not by where a root originated.

A root supplied by a peer is indistinguishable from a body-derived root after it has been authenticated and promoted. The index therefore does not store per-row provenance.

### 6.1 Existing database upgrade

Before this design, header sync wrote unauthenticated peer roots into the same index as body-derived roots. Existing rows above the durable verified body history-tree tip cannot be assumed to satisfy the new invariant.

The database format upgrade performs this one-time transition before the index is served or consumed:

1. retain the root prefix through the durable verified body history-tree
tip, whose rows were replaced by the normal verified body-commit path;
2. delete every root row above that tip;
3. initialize `HeaderRootAuthFrontier` from the body-derived history tree
and its canonical tip hash;
4. record the database format upgrade;
5. re-fetch and authenticate the deleted suffix through the root-authentication lane.

If the body history tree or its canonical tip is incoherent, the upgrade does not promote or preserve a header-ahead suffix. It fails closed under
the existing state recovery policy.

After this transition, all writes use either the sealed verified-root path
or the verified body-commit path. State can consequently treat every row in
the index as authoritative without a provenance marker or a second root
index.

State may persist the frontier as a single history-tree snapshot. This avoids
replaying a long header lead on every restart. The authenticated root rows remain a
durable audit trail and can be used for defensive reconstruction.

### 6.2 Highest completed checkpoint

State also tracks in memory the highest complete canonical checkpoint bracket:

```rust
struct HighestCompletedCheckpoint {
    height: block::Height,
    hash: block::Hash,
}
```

This snapshot is the highest configured checkpoint for which state has every
canonical header in the bracket, in order, from the previous completed boundary
through `height`, and the terminal header hash equals the configured checkpoint
hash. The previous boundary is the body-derived authentication base or the preceding
completed checkpoint.

Canonical headers are the durable source of truth; there is no separate completed-
checkpoint database row. State advances the in-memory tracker only after the header
transaction that completes the bracket succeeds, then publishes the new value to
header sync. On restart, state reconstructs it from the durable canonical header
store and configured checkpoint list before publishing it.

`RangeRequest.finalized` is scheduling intent, not evidence that a bracket is
complete. A short response retains no authority from that flag. Root authentication
uses only `HighestCompletedCheckpoint` as its promotion limit.

The completed checkpoint bracket bounds the successor witness as well as the
promoted roots. A witness header above `HighestCompletedCheckpoint.height` is not
pinned by any configured checkpoint hash, so it authenticates nothing: a peer can
craft a valid-PoW successor whose commitment field matches fabricated auxiliary
inputs, and nothing below the fast-sync handoff ever semantically verifies that
field. Every promoted root and its successor witness must therefore be at or below
`HighestCompletedCheckpoint.height`. The root at the checkpoint height itself
remains unconfirmed until a later completed bracket supplies a pinned successor.

## 7. State API

Header persistence remains conceptually separate:

```rust
Request::CommitHeaderRange {
    anchor,
    headers,
    body_sizes,
}
```

Root authentication uses a dedicated request:

```rust
Request::AuthenticateHeaderRoots {
    anchor,
    start_height,
    headers,
    tree_aux_roots,
}
```

The state request does not contain a network peer type. The driver retains the peer,
session, and request ID while awaiting the result.

Header sync subscribes to both state-owned frontiers:

```rust
struct HeaderRootAuthState {
    authenticated: HeaderRootAuthFrontier,
    completed_checkpoint: HighestCompletedCheckpoint,
}
```

The root scheduler consumes retained coverage from
`authenticated.confirmed_height + 1` and requests fallback coverage from that height
only on a miss. Both every promoted root and its successor witness must be at or
below `completed_checkpoint.height`, so the highest promotable root is strictly
below that frontier. Neither frontier is inferred from a network request flag or a
successful response.

State validates all of the following before promotion:

1. `headers.len() == tree_aux_roots.len()`;
2. the range contains at least one successor witness;
3. `start_height == confirmed_height + 1`;
4. the first header links to `confirmed_hash`;
5. every supplied header hash equals the canonical stored hash at that height;
6. every root record has the expected height;
7. the range is within the checkpoint-authenticated VCT region;
8. `verify_supplied_roots_from_parts` succeeds from the durable frontier.

The persistence layer accepts a sealed verified value rather than a raw root vector:

```rust
fn prepare_authenticated_roots_batch(
    verified: VerifiedHeaderCommitmentRoots,
) -> Result<...>
```

The verified type exposes only the confirmed prefix. Raw peer roots cannot reach the
write helper.

Suggested result categories are:

```rust
enum AuthenticateHeaderRootsError {
    StaleFrontier,
    NonCanonicalHeader { height: Height },
    InvalidSuppliedRoots {
        detected_at: Height,
        source: SuppliedRootsError,
    },
    StoreIncoherent(...),
    StorageWrite(...),
}
```

Only `InvalidSuppliedRoots` is direct evidence against the supplying peer.
`StaleFrontier` and `NonCanonicalHeader` normally mean that a response raced a chain
or scheduler change. Store and write errors are local faults.

## 8. Overlapping range protocol

Assume the durable frontier is authenticated through height `F`. The next complete
single-peer delivery, retained from forward sync or fetched as fallback, covers:

```text
[F + 1 ... E + 1]
```

State verifies the full delivery and persists:

```text
[F + 1 ... E]
```

The root at `E + 1` is not promoted because it is not confirmed by that delivery.
The next root-carrying forward or fallback response starts at the overlapping
header:

```text
[E + 1 ... N + 1]
```

This confirms and persists:

```text
[E + 1 ... N]
```

The terminal root does not need a separate per-height quarantine entry: the complete
next response repeats it together with the successor witness and remains attributable
to one peer. Other committed forward responses may remain in the retained store while
authentication catches up.

For a range of 4,000 headers, one repeated boundary header adds approximately
0.025% request-count overhead. This is preferable to a durable quarantine index or
an unbounded candidate cache.

Short responses are handled using their actual delivered endpoint. If fewer than two
headers are returned, no root can be newly confirmed; the request is retried once a
successor is available.

## 9. Forward sync

### 9.1 Common path

Every valid forward response within the VCT root window carries roots with its
headers. After state commits the independently valid headers, the reactor retains
the aligned payload and its exact peer/session/request attribution. When a retained response starts at
`confirmed_height + 1` and includes a checkpoint-covered successor witness, the same
response serves both lanes:

1. state commits the independently valid headers;
2. the reactor retains the root-carrying payload;
3. state authenticates the roots against those now-canonical headers;
4. state persists the confirmed prefix;
5. the reactor advances the authentication frontier and consumes the next retained
   response.

The request's final header becomes the overlap header for the next root-carrying
range. Retained responses are never combined across peers to manufacture an
authentication witness range.

### 9.2 Header lead

Header discovery may run ahead of root authentication. Headers remain useful for body
download scheduling and checkpoint closure, so missing roots must not block their
commit.

The reactor stores root-carrying payloads from committed forward ranges in a map
keyed by contiguous start height. This retained store may extend ahead of both the
durable authentication frontier and the current completed checkpoint. Let `C` be
the VCT fast-sync handoff and maximum configured checkpoint. Retention continues
through header `C`, which is the final checkpoint-covered successor witness used to
authenticate the peer-supplied root at `C - 1`. Payloads above `C` are not requested
or retained: ordinary semantic verification rebuilds those trees from bodies.
Completed-checkpoint coverage gates authentication and promotion only.
There is no separate resident height or byte budget and no pressure eviction.
Keeping the open-bracket retained lead is simpler and cheap enough for the VCT
region, and avoids re-fetching when the next checkpoint closes. State still
authenticates exactly one range at a time in ascending order.

On each authentication-frontier or completed-checkpoint update, the reactor first
looks for retained contiguous coverage starting at `confirmed_height + 1` and
ending with a successor witness at or below the completed checkpoint. A hit is
dispatched directly to state. A miss schedules overlapping `AuthenticateRoots`
network requests. The reactor eagerly fills the bounded resident authentication
window across the gap; peer slots consume that work incrementally, and subsequent
scheduler passes refill the window. This is neither one unbounded request nor a
one-request-at-a-time retry loop. State still admits exactly one authentication
operation at a time, so prefetched responses wait buffered until the durable
frontier reaches their start. An invalid retained response is discarded, attributed
to its supplying peer, and replaced from another peer; stale or local state errors
do not prove the retained roots invalid.

The missing suffix ends at the first retained start when one exists: the fallback
range includes that height as its successor witness, then authentication reconnects
to the retained BTree entry at the same height. Forward responses committed after
restart are admitted to that BTree even when an older gap exists below them. They
are not dropped because of the gap, but cannot be consumed until the ascending
frontier reaches their exact start key.

Checkpoint coverage can advance while a root request is in flight. This does not
invalidate an unchanged authenticated height and hash: state uses the latest
checkpoint snapshot to bound the response witness, while compare-and-swap staleness
applies only to the durable root frontier. When `HighestCompletedCheckpoint`
advances, previously retained open-bracket payloads become eligible without a
network round-trip.

Authentication advancement removes retained entries at or below the new frontier.
Frontier rollback or removal, checkpoint rollback, branch replacement, peer-session
retirement, and canonical hash mismatch drop affected retained payloads. Every
payload is rechecked against the current canonical headers and checkpoint bound
before persistence.

## 10. Forward-only startup

The existing backward header-sync lane is removed before root authentication is
implemented. Header discovery and root authentication schedule only ascending work
from the durable verified body/history-tree authentication base.

The base is a one-time seed and a floor, not an ongoing coupling to body sync. A
history tree can only be extended forward from an already-trusted tree state, and
the trusted sources are the empty tree at the activation height and the tree
produced by verified block bodies. On a fresh fast-sync node the base is the
activation-height tree, so root authentication starts with no body progress at all.
On an existing database the base is the verified body tip, below which every root
row is already authoritative. Above the base, the only forward gate on promotion is
`HighestCompletedCheckpoint`, which canonical headers alone advance; body sync
never bounds the root-authentication frontier from above.

At startup, state publishes that base together with the canonical header frontier and
completed-checkpoint snapshot. Header sync resumes at the first missing height above
the base. It may reuse a contiguous canonical header lead already present in state,
but it does not schedule a range below the base or maintain a separate backfill
frontier.

A startup configuration that would require fetching headers below the verified body
history-tree base is invalid. A header-only suffix separated from the base by a gap
is not adopted as forward progress; startup recovery truncates or ignores that suffix
and resumes from the last coherent prefix.

Once state publishes an advanced `HighestCompletedCheckpoint`, every ancestor in
that forward bracket is pinned by the checkpoint hash and header linkage. The root
lane walks the bracket upward using overlapping requests. Receiving or committing a
range marked `finalized` does not by itself advance this frontier.

This forward-only checkpoint gating keeps:

- one ascending authentication history tree;
- one header-discovery direction;
- authenticated root rows free from branch rollback;
- checkpoint-backed authentication immutable.

The durable best-header tip and the body-download target are distinct during VCT
catch-up. Before publishing a higher body target, header sync first builds a
`CATCHUP_MIN_LEAD` authenticated-root lead (one checkpoint bracket by default).
Each published target is at or below the authenticated frontier, so body sync
cannot enter an unauthenticated hole. VCT remains fail-closed as a final safety
check, but its bounded two-height repair is not the normal post-restart gap closer.

## 11. Reorganizations

VCT header-root promotion is bounded to the checkpoint-authenticated region below the
fast-sync handoff. The root lane promotes only headers covered by a completed
checkpoint bracket.

Therefore ordinary unfinalized header reorganizations affect header discovery but do
not roll back the authenticated root frontier.

Explicit finalized-state rollback or startup recovery may rebase this durable
frontier and truncate the corresponding authoritative suffix. The single ascending
frontier invariant applies during ordinary forward operation, not across an operator-
requested database rollback.

A `NonCanonicalHeader` result means a network response raced a header-store change.
It is dropped without peer punishment and retried against the current canonical
hashes.

If future work extends header-root authentication above checkpoints, it must add
hash-bound frontier rollback or per-branch snapshots. That complexity is explicitly
outside this design.

## 12. Peer attribution

Each root-authentication operation uses one complete response supplied by one peer
and contains both:

- roots and transaction counts for height `H`;
- the successor header and auth-data root needed to authenticate height `H`.

The one-header overlap keeps both sides of the commitment check in a single peer
delivery. A cryptographic mismatch is therefore attributable to that response without
maintaining a long-lived provenance database.

The reactor retains:

```text
peer ID
session ID
request ID
requested range
```

until `AuthenticateHeaderRoots` completes.

### 12.1 Exact operation correlation

Request attribution remains owned by the network layer:

```rust
struct HeaderSyncRequestIdentity {
    peer: ZakuraPeerId,
    session_id: u64,
    request_id: HeaderSyncRequestId,
}

struct HeaderSyncOperationIdentity {
    request: HeaderSyncRequestIdentity,
    kind: HeaderSyncOperationKind,
}

enum HeaderSyncOperationKind {
    CommitHeaders,
    AuthenticateRoots,
}
```

One peer response may start both operations. They share the request identity but
have distinct operation identities.

The reactor preserves this identity through the outstanding request, retained or
buffered payload, driver action, state await, and completion event. State does not
receive network peer types. The driver retains the identity while awaiting the typed
state result and attaches it to the event returned to the reactor.

Every completion settles exactly the pending operation with the same identity.
Peer attribution, retries, and failure handling never infer operation identity from
peer plus range geometry or from range containment.

After exactly matching a successful header commit, the reactor may use the committed
height interval to mark redundant discovery work covered. This coverage optimization
does not settle an overlapping root-authentication operation and does not transfer
success or failure between requests.

A peer disconnect or session replacement does not allow an old completion to match
a request from the new session. A stale completion may be discarded, but it is never
reassigned.

On `InvalidSuppliedRoots`, the reactor:

1. records the failed height and peer;
2. retires the request;
3. avoids that peer for the retry;
4. requests the same canonical overlapping range from another peer;
5. applies the configured misbehavior policy.

On a local or stale result, the reactor retries without scoring the peer.

Peer attribution is intentionally ephemeral. A restart loses pending attribution,
but it also loses the untrusted response. The replacement request establishes fresh
attribution.

## 13. Restart and recovery

Only authenticated state must survive restart.

Startup performs:

1. apply the one-time existing database transition from Section 6.1, if required;
2. load the durable `HeaderRootAuthFrontier`;
3. verify that its `(height, hash)` matches the canonical header store;
4. verify that the frontier is not behind the authoritative body history tree;
5. if bodies advanced farther, rebase to the body-derived history tree;
6. load the durable best-header tip independently from the root-covered body target;
7. enter catch-up from `confirmed_height + 1`, because the pre-restart retained
   store is intentionally not restored.

No unverified candidate roots are restored.

Catch-up defines its network gap as the confirmable heights between
`confirmed_height + 1` and the minimum of the completed checkpoint, durable
best-header tip, and VCT handoff. If a post-restart retained response starts inside
that interval, its start is the reconnect boundary.

Each scheduler pass queues as many overlapping `AuthenticateRoots` batches as fit
the resident authentication budget. Free peer slots take those batches before
Forward work. Responses can therefore arrive in parallel, but durable
authentication remains serial and ascending. Later passes refill released resident
capacity until the gap closes. New Forward scheduling pauses while a confirmable
restart gap exists; in-flight Forward work can finish, and Forward resumes when a
successor header is needed or catch-up reaches its boundary.

The shared body-download target starts from root-covered state, not merely the
durable header tip. Header-root advancement publishes a new body target only after
the authenticated frontier has rebuilt `CATCHUP_MIN_LEAD`; every published target
is itself authenticated. This prevents body sync from racing into the emptied
post-restart root gap. A VCT repair may use one idle peer, but it does not stop the
scheduler from assigning catch-up work to other peers.

If the frontier snapshot is missing or fails coherence checks, state can defensively
reconstruct it from:

1. the authoritative history tree at the verified body tip;
2. contiguous authenticated root rows and canonical headers above that tip.

A reconstruction gap is not filled with guessed data. Root authentication resumes
from the last coherent frontier and re-fetches the missing range.

## 14. Interaction with block commit

Header-layer authentication proves that a root payload is the payload committed by
the canonical header chain. It does not replace body verification.

When a body arrives, state still checks:

- the body header equals the stored canonical header;
- body-derived shielded transaction counts equal the authenticated counts;
- the body-derived auth-data root equals the authenticated auth-data root;
- all ordinary consensus and semantic rules.

The block commit path treats body-derived roots as authoritative. If a header-root row
already exists at that height, the body commit may confirm or replace it only after
the normal body checks succeed.

If a late header-root request overlaps a height with a committed body, the write path
keeps the existing body-derived row.

The state writer also keeps the header-root authentication frontier synchronized when
the verified body tip overtakes it. Because both operations are state-owned and
serialized, this requires no network-side lazy rebuild.

## 15. VCT handoff

Root authentication is required only through the last checkpoint used by VCT fast
sync.

Let `C = network.checkpoint_list().max_height()`, which is also the height of the
embedded final frontier. The peer-root lane promotes through `C - 1`; checkpoint
header `C` is the final pinned successor witness. It does not attempt to authenticate
the peer-supplied root at `C`, because no later configured checkpoint can pin a
successor at `C + 1`. Instead, the embedded final frontier and the normal handoff-body
checks independently establish the authoritative Sapling, Orchard, and Ironwood
state at `C`.

Above `C`, ordinary semantic verification and tree updates resume. Header sync stops
requesting and retaining tree-aux roots above `C` unless another feature requires
them.

## 16. Serving policy

The root-serving path reads only the authoritative commitment-root index.

As a result:

- every served root is already authenticated or body-derived;
- a forwarding node introduces no additional trust;
- restart does not expose stale candidates;
- there is no distinction between provisional and verified rows for consumers.

Serving may return a contiguous prefix shorter than requested only when the request
extends beyond the local authenticated frontier or the available canonical header
tip. Every returned header has its corresponding authoritative root row.

A missing root at or below the advertised authenticated frontier is state
incoherence, not a short response. Serving fails closed without returning a partial
payload from before the gap. Startup recovery truncates to the last coherent prefix
and re-authenticates the suffix before that frontier is advertised or served.

## 17. Failure policy

Invalid peer roots:

- do not write roots;
- do not advance the frontier;
- do not remove valid canonical headers;
- are retried using another peer;
- are reported as peer-attributable misbehavior.

Missing roots:

- do not fall back to a stale tree frontier;
- prioritize a root-authentication request;
- stall the VCT body commit if it catches the root frontier;
- leave ordinary header discovery operational.

State incoherence or storage failure:

- is treated as a local error;
- does not score a peer;
- leaves the in-memory frontier unchanged unless the durable transaction succeeded.

Repeated peer failures:

- use bounded attempts and wall time;
- surface metrics and error-level logs;
- leave VCT fail-closed.

## 18. Bandwidth and memory

The selected design retains root-carrying payloads with committed headers through
the VCT handoff so authentication can consume them later without a steady-state
re-download. It does not persist untrusted payloads.

In the steady path, header discovery supplies every root payload and authentication
later consumes it from the retained store once completed-checkpoint coverage
allows. The only repeated data is the one-header boundary overlap between adjacent
forward responses.

Extra bandwidth occurs when:

- a committed range arrived without complete roots;
- a node restarted before authenticating retained responses;
- a rebase or canonical mismatch invalidated retained responses;
- authentication rejected a response and replacement data must come from another
  peer.

Retention is bounded by the VCT handoff region, not by the current completed
checkpoint. Open-bracket payloads between `HighestCompletedCheckpoint` and the
header tip remain cached until that checkpoint closes, authentication consumes
them, or they are invalidated. The store removes consumed or invalidated entries
promptly and does not pressure-evict under a memory budget. Refetch after restart is
acceptable and is preferable to making untrusted data durable.

A future roots-only recovery message could avoid retransmitting full headers, using
`(height, header_hash, roots)` records. It is not required initially and adds another
wire message, serving path, and compatibility surface.

## 19. Alternatives considered

### 19.1 Persist provisional roots and validate during body commit

Advantages:

- simplest header-sync implementation;
- no overlap or separate authentication lane;
- no root re-fetch after restart.

Disadvantages:

- faulty roots enter the authoritative index;
- peers can be served unverified roots;
- failures are detected late, potentially stalling body commit;
- original peer attribution is usually lost;
- repair becomes part of the consensus-critical commit loop.

Decision: rejected because it violates the primary trust invariant.

### 19.2 Network-owned history tree, as in PR #62

Advantages:

- earliest rejection at network ingress;
- immediate peer attribution;
- confirmed-prefix persistence and overlap are structurally strong;
- no untrusted root cache is required.

Disadvantages:

- duplicates canonical-chain ownership;
- requires startup and lazy tree reconstruction across an async state boundary;
- requires stale-result guards and tip reanchoring;
- couples scheduler progress to history-tree position;
- reorganization handling becomes split between network and state.

Decision: retain its overlap and verified-prefix ideas, but move frontier ownership
to state.

### 19.3 Bounded in-memory candidate cache

Advantages:

- untrusted roots never reach disk;
- peer provenance is easy to retain;
- no new persistent format.
- roots delivered with headers are not normally downloaded again;
- header and authentication throughput can differ without wasting forward-response
  bandwidth.

Disadvantages:

- restart requires re-fetching;
- forward sync can outrun authentication and hold retained payloads until the
  frontier catches up;
- delayed cross-range verification complicates attribution;
- another queue needs reorganization cleanup.

Decision: accepted in a constrained form. The reactor retains complete,
single-peer, root-carrying forward payloads keyed by contiguous range; it does not
cache independent per-height candidates or combine data from different peers.
State remains the only owner of authentication and durable promotion. The cache is
ephemeral, limited by the VCT handoff region (not the current completed checkpoint),
and cleared or pruned on frontier and session invalidation. It does not use a
separate memory budget or pressure eviction. Completed-checkpoint coverage gates
only when a retained payload may be authenticated.

### 19.4 Persistent quarantine index

Advantages:

- avoids re-fetch after restart;
- decouples header and authentication throughput.

Disadvantages:

- untrusted data still enters the database;
- requires schema, cleanup, limits, and migration policy;
- persisted peer provenance is operational rather than consensus data;
- consumers must never accidentally read the quarantine index.

Decision: rejected for the initial design.

### 19.5 Roots-only protocol

Advantages:

- efficient restart and gap recovery;
- avoids retransmitting known headers.

Disadvantages:

- adds wire negotiation and another request/response path;
- still needs canonical hash correlation;
- does not remove the need for overlap or a state-owned frontier.

Decision: defer until measurements show recovery bandwidth is material.

### 19.6 Authenticate every non-finalized candidate branch

Advantages:

- cryptographically earliest rejection;
- roots may be ready before checkpoint closure.

Disadvantages:

- needs branch-specific history trees;
- requires frontier rollback and root-row deletion on reorganization;
- complicates peer attribution when branches and range boundaries differ;
- provides little benefit because VCT consumes roots only below checkpoints.

Decision: reject in favor of checkpoint-gated promotion.

## 20. Implementation sequence

### Phase 0: simplify existing boundaries

Complete on `main`:

- backward header sync was removed in
  [PR #227](https://github.com/zakura-core/zakura/pull/227);
- exact operation identity was added in
  [PR #246](https://github.com/zakura-core/zakura/pull/246);
- checked, aligned range payloads were added in
  [PR #298](https://github.com/zakura-core/zakura/pull/298) and hardened in
  [PR #309](https://github.com/zakura-core/zakura/pull/309);
- commitment-root index access was centralized in
  [PR #307](https://github.com/zakura-core/zakura/pull/307);
- history-tree snapshot decoding was made fallible in
  [PR #316](https://github.com/zakura-core/zakura/pull/316).

1. Remove the backward header-sync lane, its work-queue priority, buffering paths,
   tracing, and tests.
2. Enforce the forward-only startup invariant from Section 10.
3. Carry `HeaderSyncRequestIdentity` through buffering and driver actions.
4. Assign distinct operation identities to header commit and root authentication.
5. Include the exact operation identity in every completion event.
6. Replace range-derived pending-operation removal with exact identity matching,
   retaining range coverage only as a post-success scheduling optimization.
7. Introduce checked range geometry and payload types that keep headers, body-size
   hints, and optional roots aligned while separating scheduling policy.
8. Centralize commitment-root index reads, disk-row conversions, batch writes, and
   deletion policy behind state-owned helpers.
9. Add fallible history-tree snapshot decoding and coherence errors for restart
   recovery.

### Phase 1: establish the persistence boundary

Implemented in draft
[PR #323](https://github.com/zakura-core/zakura/pull/323).

1. Add the database format transition from Section 6.1.
2. Add the minimal durable `HeaderRootAuthFrontier` representation needed by that
   transition.
3. Initialize or rebase the frontier from the verified body history tree and its
   canonical tip.
4. Restore the frontier at startup using fallible snapshot decoding and coherence
   checks.
5. Introduce or complete `VerifiedHeaderCommitmentRoots` with private fields.
6. Add a write helper that accepts only the verified type and atomically persists
   promoted roots with the frontier.
7. Stop `CommitHeaderRange` from writing raw peer roots.
8. Ensure serving reads only authenticated or body-derived rows.
9. Preserve committed-body precedence.

### Phase 2: add the state-owned frontier

Implemented in draft
[PR #323](https://github.com/zakura-core/zakura/pull/323).

1. Reuse the in-memory `HighestCompletedCheckpointTracker`.
2. Advance the completed-checkpoint snapshot only after a durable bracket-closing
   header commit.
3. Reconstruct the completed-checkpoint snapshot from the canonical header store.
4. Add defensive reconstruction for the durable authentication frontier and the
   in-memory checkpoint tracker.
5. Publish both snapshots to header sync using a watch or existing frontier event.

### Phase 3: add root authentication requests

Implemented in draft
[PR #323](https://github.com/zakura-core/zakura/pull/323).

1. Add `AuthenticateHeaderRoots`.
2. Validate canonical stored header hashes before cryptographic verification.
3. Call `verify_supplied_roots_from_parts`.
4. Persist only the confirmed prefix.
5. Return typed peer, stale, and local outcomes.

### Phase 4: schedule overlap

Implemented in draft
[PR #323](https://github.com/zakura-core/zakura/pull/323).

1. Add a root-authentication work priority or lane.
2. Request from `confirmed_height + 1`.
3. Include one successor witness.
4. Start the next request at the previous response's final header.
5. Share responses with header discovery when their frontiers align.
6. Gate promotion on the state-published `HighestCompletedCheckpoint`.
7. Require the successor witness itself to be covered by that frontier.

### Phase 5: retain forward root payloads

1. Replace the one-shot `reusable_payload` path with a reactor-owned retained
   payload store limited by the VCT handoff region.
2. Make adjacent root-carrying forward requests overlap by one header so every
   retained authentication range contains its own terminal successor witness and
   remains attributable to one peer.
3. Insert complete root-carrying payloads only after their headers commit, including
   payloads ahead of the current completed checkpoint.
4. Preserve exact peer, session, and request attribution with each payload.
5. Consume contiguous retained coverage from `confirmed_height + 1` before
   scheduling authentication network work, but only when the successor witness is
   covered by `HighestCompletedCheckpoint`.
6. Make `AuthenticateRoots` requests fallback-only for missing, invalidated, or
   rejected coverage.
7. Prune consumed payloads and invalidate affected payloads on frontier rebase,
   checkpoint rollback, canonical mismatch, and peer-session retirement.

### Phase 6: integrate VCT consumption

1. Make `PeerSource` read only authenticated roots.
2. Retain body-time checks as defense in depth.
3. Keep bounded repair for missing authenticated rows.
4. Verify the embedded final frontier against the authenticated checkpoint root.

### Phase 7: remove obsolete provisional behavior

1. Remove provisional-root terminology and reads.
2. Remove body-commit invalidation of unauthenticated database rows.
3. Simplify repair events that existed only because invalid roots were discovered
   late.
4. Update the main VCT design document and changelog.

## 21. Test plan

### Chain verifier

- Valid ranges return exactly the confirmed prefix.
- Corrupt each root, count, and auth-data root independently.
- A wrong root at `H` fails when processing `H + 1`.
- A one-header range confirms no roots.
- Upgrade activation boundaries reset and advance the history tree correctly.

### State authentication

- An existing database preserves body-derived rows and deletes every header-ahead
  root before the index becomes readable.
- The database upgrade initializes the authentication frontier at the body history
  tree tip and re-fetches the deleted suffix.
- Raw roots cannot be passed to the persistence helper.
- Invalid roots write no rows and do not advance the frontier.
- A valid range atomically writes roots and its frontier.
- A simulated storage failure advances neither durable nor in-memory state.
- Canonical header mismatch returns a stale result without peer blame.
- Existing committed-body roots are never overwritten.
- Startup restores the exact frontier snapshot.
- State publishes a completed checkpoint only after every canonical header in its
  bracket and the configured terminal hash are durably stored.
- Short or out-of-order header commits do not prematurely advance the completed
  checkpoint frontier.
- Restart reconstructs the same completed checkpoint snapshot from durable headers.
- Defensive reconstruction stops at the first gap.
- A body-tip advance safely rebases an older header-root frontier.
- Promotion and body-derived replacement preserve a contiguous authoritative index
  through the authenticated frontier.

### Network scheduling

- Consecutive root requests overlap by exactly one header.
- Adjacent root-carrying forward responses overlap by exactly one header, so each
  retained response independently carries its successor witness.
- The repeated boundary header commits idempotently and does not regress or duplicate
  canonical header progress.
- The confirmed prefix is one shorter than the delivered headers.
- Short responses use their actual final header as the next overlap.
- Serving can shorten only at the authenticated frontier or available header tip,
  never at an interior root gap.
- A missing row at or below the advertised authenticated frontier is reported as
  local state incoherence and returns no partial payload.
- Two peers requesting the same range complete independently.
- Header commit and root authentication from one response have distinct completions.
- Header success does not settle an overlapping root-authentication operation.
- An old-session completion cannot settle a new-session request for the same range.
- Invalid roots are attributed only to the exact request that supplied them.
- A response from an avoided peer is not immediately retried to that peer.
- Invalid roots are attributed to the peer that supplied the complete witness range.
- Local and stale state errors do not score peers.
- A root-carrying forward payload remains retained after header commit even when its
  range is ahead of the current authentication frontier or completed checkpoint.
- Authentication consumes retained contiguous coverage before scheduling a network
  fallback request, and only once the successor is checkpoint-covered.
- A long header lead does not re-fetch retained roots for the same heights.
- When `HighestCompletedCheckpoint` advances, previously retained open-bracket
  payloads become eligible without a fallback request.
- A retained-store miss fills the bounded authentication window with overlapping
  fallback requests through the checkpoint/header or retained reconnect boundary.
- Invalid retained roots are dropped, attributed to their original peer, and
  replaced from another peer.
- Frontier advancement prunes consumed retained payloads.
- Rebase, canonical mismatch, checkpoint rollback, and peer-session retirement drop
  affected retained payloads without promoting them.
- Retained payloads through the VCT handoff are kept until consumed or invalidated;
  there is no pressure eviction.

### Forward-only startup

- Startup schedules no header or root work below the verified body/history-tree
  authentication base.
- A configuration that requires below-base backfill is rejected.
- A disconnected header-only suffix is truncated or ignored rather than backfilled.
- A contiguous canonical header lead is reused as forward progress.
- Root authentication waits for the contiguous canonical prefix.
- A completed checkpoint bracket authenticates strictly upward.
- `RangeRequest.finalized` never authorizes root promotion.
- A successor witness above the completed checkpoint snapshot confirms no root.
- Forward header progress does not move the root frontier past a gap.

### Restart and handoff

- Restart resumes at `confirmed_height + 1`.
- The first post-restart fallback starts at `confirmed_height + 1` and includes its
  successor witness; it does not re-request `confirmed_height`.
- The reactor eagerly fills its bounded authentication request window across the
  restart gap and refills it incrementally as peer slots and resident capacity free.
- Forward tip extension pauses only while a confirmable catch-up gap exists;
  `AuthenticateRoots` refetch continues and in-flight Forward work may finish.
- No unverified tip root survives restart.
- Restart may re-fetch roots that existed only in the pre-restart retained store.
- Root payloads committed by new Forward responses after restart remain retained
  above the gap and become usable when catch-up reaches their exact start.
- The body-download target stays at or below authenticated coverage and is released
  only after the configured minimum authenticated lead has been rebuilt.
- The final peer-promoted root is at `C - 1`, confirmed by checkpoint header `C`;
  the embedded frontier and handoff-body checks establish the authoritative state at
  `C`.
- The embedded frontier must match the authenticated last-checkpoint roots.

## 22. Metrics and diagnostics

The primary health metric is `sync.header.root_auth.lead_blocks`. It is the
authenticated root height minus the verified body tip, saturated at zero. During
checkpoint sync it should normally be positive and large enough to absorb temporary
network or verification delays. A value of zero is not automatically an error at the
chain tip or during startup, but zero combined with body-sync root retries means the
authentication lane has failed to stay ahead.

The bounded retained pipeline is exposed through these gauges:

- `sync.header.root_auth.work.retained_batches`: committed forward responses held
  for future authentication;
- `sync.header.root_auth.work.retained_heights`: the sum of entries in retained
  responses, including repeated overlap witnesses;
- `sync.header.root_auth.work.pending_batches`: fallback ranges waiting for a peer;
- `sync.header.root_auth.work.in_flight_batches`: fallback requests currently
  assigned to peers;
- `sync.header.root_auth.work.buffered_batches`: fallback responses waiting for the
  durable frontier;
- `sync.header.root_auth.work.authenticating_batches`: retained or fallback ranges
  admitted to state authentication; this should never remain above one because
  durable verification is serial;
- `sync.header.root_auth.work.resident_heights`: the sum of entries in queued,
  in-flight, buffered, retained, and authenticating root ranges. Overlap witnesses
  are counted in adjacent ranges, so this measures retained lead size rather than
  unique heights.
- `sync.header.root_auth.catchup.active`: whether the reactor is closing a
  confirmable gap under the durable header tip;
- `sync.header.root_auth.catchup.hole_heights`: the remaining confirmable gap before
  the retained reconnect boundary or current checkpoint/header bound;
- `sync.header.root_auth.catchup.prefetched`: overlapping fallback batches added to
  the bounded catch-up window.

The retained-store and fallback counters are:

- `sync.header.root_auth.retain.admitted`: committed root-carrying forward responses
  added to the retained store;
- `sync.header.root_auth.retain.hit`: next-frontier authentication ranges supplied
  from retained forward responses;
- `sync.header.root_auth.retain.miss`: next-frontier ranges absent from retained
  coverage;
- `sync.header.root_auth.retain.dropped{reason=...}`: retained responses removed
  without authentication. Expected reasons include `frontier_advanced`,
  `frontier_rebased`, `checkpoint_rebased`, `canonical_mismatch`,
  `invalid_roots`, and `session_retired`;
- `sync.header.root_auth.fallback.requested{reason=...}`: overlapping network
  recovery requested because of `missing`, `invalid_roots`, or `restart`;
- `sync.header.root_auth.completed`: state operations that reported success. Durable
  progress is still accepted only from the state watch, so this counter alone does not
  prove frontier advancement.

The steady path should have a high retain-hit rate and near-zero fallback requests.
Occasional `frontier_advanced` drops are normal because a larger authenticated range
can cover retained work. Repeated `frontier_rebased`, `checkpoint_rebased`, or
`canonical_mismatch` drops without corresponding canonical chain changes indicate
scheduler/state churn. Any `invalid_roots` drop identifies a peer payload that
failed authentication.

Body commit exposes the consequences of insufficient lead:

- `state.vct.root.unavailable.count`: a body needed a root that was not present in the
  authenticated index;
- `state.vct.root.await_successor.count`: the root existed but lacked its authenticated
  successor witness;
- `state.vct.root.retry.count`: write-loop retry polls. One stall can increment this
  many times, so use it as a rate rather than an incident count;
- `state.vct.root.stalled.height`: zero normally; after a prolonged retry episode it
  contains the blocked body height;
- `state.vct.root.repair.requested`: bounded repair requests sent back to header sync;
- `state.vct.fast_path.hit` and `state.vct.fast_path.miss`: whether finalized body
  commits used the authenticated-root fast path.

A healthy checkpoint-sync dashboard should show a positive
`sync.header.root_auth.lead_blocks`, at most one authenticating batch, retained work
within the VCT handoff region, a high retain-hit rate, near-zero fallback
requests, and no sustained increase in either root-unavailable counter. The
strongest correctness regression signal is:

```text
sync.header.root_auth.lead_blocks == 0
and rate(state.vct.root.retry.count) > 0
```

When diagnosing that condition, first compare retained batches, retain hits/misses,
and fallback pending/in-flight/buffered gauges. Retained coverage with no retain hits
points to a frontier, overlap, or checkpoint-bound mismatch. Persistent misses with
no retained coverage point to lost or never-admitted payloads. Pending fallback work
with no in-flight work points to peer admission or scheduling; in-flight fallback
work without a response points to network latency or timeouts. No retained or
fallback work despite available checkpoint coverage points to a scheduler
regression.

Logs for failures should include:

- requested and delivered range;
- current authenticated frontier;
- detected failure height;
- peer, session, and request identifiers in the network layer;
- canonical header hash;
- error category;
- retry attempt and remaining wall-time budget.

Do not log complete root payloads at warning or error level.

## 23. Resulting invariants

After implementation:

1. Header validity is independent of root availability.
2. State is the sole owner of the canonical root-authentication frontier.
3. The authoritative root index contains no unverified peer rows.
4. Every promoted range is bound to canonical stored header hashes.
5. Every promoted root has a successor witness from the same attributed response.
6. A database transaction advances root rows and the frontier together.
7. Restart needs only authenticated durable state.
8. Header discovery and root authentication advance only forward from the verified
   body/history-tree authentication base.
9. Reorganizations above the checkpoint region cannot invalidate promoted roots.
10. Only state-published completed checkpoint brackets authorize root promotion.
11. The authoritative root index is contiguous through the authenticated frontier.
12. Every asynchronous state result is matched to its original request and operation;
    range geometry is never used as completion identity.
13. Root-carrying committed-header responses are retained in reactor memory through
    the VCT handoff region until authenticated, invalidated, or lost on restart,
    including payloads ahead of the current completed checkpoint.
14. Retained roots are consumed before network fallback once their successor is
    checkpoint-covered; heights already retained are not re-fetched unless coverage
    is missing, invalidated, or rejected.
15. Retained roots never enter the authoritative index without state authentication.
16. Body verification remains the final proof that authenticated auxiliary values
    match downloaded transactions.
