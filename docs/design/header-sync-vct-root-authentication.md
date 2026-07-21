# Header-sync VCT root authentication

Status: proposed

## 1. Summary

Zakura header sync receives peer-supplied note-commitment roots and the
additional ZIP-221 leaf inputs needed by verified commitment tree (VCT) fast sync.

These values must be authenticated before they enter the authoritative
commitment-root index.

This design makes `zakura-state` the owner of a single, durable, ascending
root authentication frontier. Header discovery remains responsible for
finding and committing valid headers, while a root-authentication lane
supplies overlapping canonical header ranges to state. State verifies each
range against its current history-tree frontier and atomically persists
only the confirmed root prefix.

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
7. Keep bandwidth and restart costs bounded.

## 3. Non-goals

- Authenticating roots above the VCT fast-sync handoff.
- Replacing ordinary block semantic verification.
- Trusting roots because they came from multiple peers.
- Persisting peer identities as consensus state.
- Making root availability a prerequisite for accepting independently valid
  headers.
- Supporting multiple durable candidate header branches.
- Fetching headers below the verified body/history-tree authentication base.

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
5. does not write peer-supplied roots.

The root-authentication lane:

1. follows the state-published authenticated root frontier;
2. requests a canonical, overlapping range from one peer;
3. submits the complete response to state while its peer attribution is live;
4. persists only the prefix confirmed by the successor header;
5. advances the durable frontier after the database write succeeds.

The lanes can share a network response when a newly discovered header range begins
exactly where root authentication is ready to advance. Otherwise, root
authentication can re-request already-known canonical headers.

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

### 6.2 Completed checkpoint frontier

State also owns the durable fact that a complete canonical checkpoint bracket is
stored:

```rust
struct CompletedCheckpointFrontier {
    height: block::Height,
    hash: block::Hash,
}
```

This frontier is the highest configured checkpoint for which state has every
canonical header in the bracket, in order, from the previous completed boundary
through `height`, and the terminal header hash equals the configured checkpoint
hash. The previous boundary is the body-derived authentication base or the preceding
completed checkpoint.

State advances this frontier only after the header transaction that completes the
bracket succeeds. It publishes the new value to header sync using a watch or the
shared state-frontier event. On restart, state reconstructs or checks it from the
durable canonical header store and configured checkpoint list before publishing it.

`RangeRequest.finalized` is scheduling intent, not evidence that a bracket is
complete. A short response retains no authority from that flag. Root authentication
uses only `CompletedCheckpointFrontier` as its promotion limit.

An authentication delivery may include one canonical successor header above the
completed checkpoint frontier to witness the root at the frontier height. State may
promote roots only through `CompletedCheckpointFrontier.height`; the successor's root
remains unconfirmed until a later completed bracket authorizes further promotion.

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
    completed_checkpoint: CompletedCheckpointFrontier,
}
```

The root scheduler requests from `authenticated.confirmed_height + 1` and does not
schedule promotable roots above `completed_checkpoint.height`. It may request the
single additional successor witness needed to confirm the root at that upper bound.
Neither frontier is inferred from a network request flag or a successful response.

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

Assume the durable frontier is authenticated through height `F`.

The root lane requests:

```text
[F + 1 ... E + 1]
```

State verifies the full delivery and persists:

```text
[F + 1 ... E]
```

The root at `E + 1` is deliberately discarded because it is not confirmed by that
delivery. The next request starts at the overlapping header:

```text
[E + 1 ... N + 1]
```

This confirms and persists:

```text
[E + 1 ... N]
```

No pending unverified root needs to survive between requests.

For a range of 4,000 headers, one repeated boundary header adds approximately
0.025% request-count overhead. This is preferable to a durable quarantine index or
an unbounded candidate cache.

Short responses are handled using their actual delivered endpoint. If fewer than two
headers are returned, no root can be newly confirmed; the request is retried once a
successor is available.

## 9. Forward sync

### 9.1 Common path

When the next header-discovery response starts at `confirmed_height + 1`, the same
response can serve both lanes:

1. state commits the independently valid headers;
2. state authenticates the roots against those now-canonical headers;
3. state persists the confirmed prefix;
4. the reactor advances both header scheduling and root scheduling.

The request's final header becomes the overlap header for the next root-carrying
request.

### 9.2 Header lead

Header discovery may run ahead of root authentication. Headers remain useful for body
download scheduling and checkpoint closure, so missing roots must not block their
commit.

Root authentication remains bounded by its own small number of in-flight requests.
It does not retain an unbounded queue of unverified roots. Responses that cannot
advance the current root frontier are either:

- used only for header discovery and their roots discarded; or
- requested without tree-aux roots.

When the root frontier catches up, it reuses the stored canonical headers as the
expected chain and obtains a fresh overlapping root response.

## 10. Forward-only startup

The existing backward header-sync lane is removed before root authentication is
implemented. Header discovery and root authentication schedule only ascending work
from the durable verified body/history-tree authentication base.

At startup, state publishes that base together with the canonical header and
completed-checkpoint frontiers. Header sync resumes at the first missing height above
the base. It may reuse a contiguous canonical header lead already present in state,
but it does not schedule a range below the base or maintain a separate backfill
frontier.

A startup configuration that would require fetching headers below the verified body
history-tree base is invalid. A header-only suffix separated from the base by a gap
is not adopted as forward progress; startup recovery truncates or ignores that suffix
and resumes from the last coherent prefix.

Once state publishes an advanced `CompletedCheckpointFrontier`, every ancestor in
that forward bracket is pinned by the checkpoint hash and header linkage. The root
lane walks the bracket upward using overlapping requests. Receiving or committing a
range marked `finalized` does not by itself advance this frontier.

This forward-only checkpoint gating keeps:

- one ascending authentication history tree;
- one header-discovery direction;
- authenticated root rows free from branch rollback;
- checkpoint-backed authentication immutable.

If the body pipeline reaches a missing root before authentication catches up, VCT
stays fail-closed and prioritizes the corresponding forward root request.

## 11. Reorganizations

VCT header-root promotion is bounded to the checkpoint-authenticated region below the
fast-sync handoff. The root lane promotes only headers covered by a completed
checkpoint bracket.

Therefore ordinary unfinalized header reorganizations affect header discovery but do
not roll back the authenticated root frontier.

A `NonCanonicalHeader` result means a network response raced a header-store change.
It is dropped without peer punishment and retried against the current canonical
hashes.

If future work extends header-root authentication above checkpoints, it must add
hash-bound frontier rollback or per-branch snapshots. That complexity is explicitly
outside this design.

## 12. Peer attribution

Each root-authentication request is supplied by one peer and contains both:

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

The reactor preserves this identity through the outstanding request, buffered
payload, driver action, state await, and completion event. State does not receive
network peer types. The driver retains the identity while awaiting the typed state
result and attaches it to the event returned to the reactor.

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
6. schedule the next overlapping request from `confirmed_height + 1`.

No unverified candidate roots are restored.

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

To authenticate the last checkpoint root at height `C`, the root lane requests header
`C + 1` as the successor witness. The embedded final frontier remains an independent
handoff check: its Sapling, Orchard, and Ironwood roots must equal the authenticated
roots at `C` before the frontier is written.

Above `C`, ordinary semantic verification and tree updates resume. Header sync stops
requesting tree-aux roots unless another feature requires them.

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

The selected design spends one repeated header per root range. It does not keep an
unbounded root candidate cache and does not persist untrusted payloads.

In the steady path, header discovery and root authentication share the same response,
so the only repeated data is the one-header boundary overlap.

Extra bandwidth occurs when:

- root authentication follows headers that were previously committed without roots;
- a node restarts before authenticating an in-flight response;
- an invalid response must be fetched from another peer.

This cost is bounded by the root-authentication window and is preferable to making
untrusted data durable.

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

Disadvantages:

- restart and eviction require re-fetching;
- forward sync can outrun the cache;
- delayed cross-range verification complicates attribution;
- another queue needs backpressure and reorganization cleanup.

Decision: unnecessary when overlapping requests keep verification synchronous.

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

1. Add `CompletedCheckpointFrontier`.
2. Advance the completed-checkpoint frontier only after a durable bracket-closing
   header commit.
3. Initialize or rebase the completed-checkpoint frontier from the canonical header
   store.
4. Add defensive reconstruction for both frontiers.
5. Publish both frontiers to header sync using a watch or existing frontier event.

### Phase 3: add root authentication requests

1. Add `AuthenticateHeaderRoots`.
2. Validate canonical stored header hashes before cryptographic verification.
3. Call `verify_supplied_roots_from_parts`.
4. Persist only the confirmed prefix.
5. Return typed peer, stale, and local outcomes.

### Phase 4: schedule overlap

1. Add a root-authentication work priority or lane.
2. Request from `confirmed_height + 1`.
3. Include one successor witness.
4. Start the next request at the previous response's final header.
5. Share responses with header discovery when their frontiers align.
6. Gate promotion on the state-published `CompletedCheckpointFrontier`.
7. Permit one canonical successor witness above that frontier without promoting its
   root.

### Phase 5: integrate VCT consumption

1. Make `PeerSource` read only authenticated roots.
2. Retain body-time checks as defense in depth.
3. Keep bounded repair for missing authenticated rows.
4. Verify the embedded final frontier against the authenticated checkpoint root.

### Phase 6: remove obsolete provisional behavior

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
- Restart reconstructs the same completed checkpoint frontier from durable headers.
- Defensive reconstruction stops at the first gap.
- A body-tip advance safely rebases an older header-root frontier.
- Promotion and body-derived replacement preserve a contiguous authoritative index
  through the authenticated frontier.

### Network scheduling

- Consecutive root requests overlap by exactly one header.
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

### Forward-only startup

- Startup schedules no header or root work below the verified body/history-tree
  authentication base.
- A configuration that requires below-base backfill is rejected.
- A disconnected header-only suffix is truncated or ignored rather than backfilled.
- A contiguous canonical header lead is reused as forward progress.
- Root authentication waits for the contiguous canonical prefix.
- A completed checkpoint bracket authenticates strictly upward.
- `RangeRequest.finalized` never authorizes root promotion.
- A successor witness above the completed checkpoint frontier can confirm the
  frontier root but is not itself promoted.
- Forward header progress does not move the root frontier past a gap.

### Restart and handoff

- Restart resumes at `confirmed_height + 1`.
- The first post-restart request overlaps the correct canonical header.
- No unverified tip root survives restart.
- The last checkpoint root is confirmed by its successor.
- The embedded frontier must match the authenticated last-checkpoint roots.

## 22. Metrics and diagnostics

Suggested metrics:

```text
sync.header.root_auth.frontier.height
sync.header.root_auth.requested
sync.header.root_auth.confirmed
sync.header.root_auth.rejected
sync.header.root_auth.retry
sync.header.root_auth.stale
sync.header.root_auth.local_error
sync.header.root_auth.storage_error
sync.header.root_auth.waiting_for_checkpoint
sync.header.root_auth.waiting_for_header
sync.header.root_auth.overlap_headers
```

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
13. Body verification remains the final proof that authenticated auxiliary values
    match downloaded transactions.
