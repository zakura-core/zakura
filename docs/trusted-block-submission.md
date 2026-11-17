# Trusted block submission

`POST /submittrustedblock` lets an authorized operator start block relay before
the receiving node finishes its own validation. It uses the existing
announcement-and-fetch peer protocol. The block enters the node's chain only
after normal local consensus validation and commitment succeed.

Legacy TCP peers fetch announced blocks normally. The v2 legacy-gossip
compatibility adapter announces and serves pending blocks on explicit requests.
Current Zakura receivers ignore compatibility block announcements because native
block sync owns their automatic downloads; early announcements do not make an
unvalidated block available through native block sync.

## Enable the endpoint

```toml
[rpc]
listen_addr = "127.0.0.1:8232"
enable_cookie_auth = true
enable_trusted_block_submission = true
```

The endpoint is disabled by default and follows the primary listener's
`enable_cookie_auth` setting, just like `submitblock`, including on Regtest.
When `true`, a valid RPC cookie is required. When `false`, requests need no
credentials: restrict the listener to trusted clients using a firewall or private
network. Anyone who can reach an unauthenticated listener can authorize early
relay and cause peer bans by submitting invalid blocks.

The separate admin listener does not expose this endpoint. Browser-origin
requests are rejected with either authentication setting. Use the existing RPC
TLS configuration or a protected transport when connecting across hosts.

## Recommended sequence

1. Obtain a template from the GBT (`getblocktemplate`) node.
2. Mine it and construct the final solved block, including any coinbase changes
   and the corresponding commitments.
3. Submit that final block through the GBT node's ordinary validating submission
   path and wait for successful acceptance.
4. Only after acceptance, send **the exact same complete block bytes** to the
   trusted endpoints on the other relay nodes.
5. Those nodes relay immediately and independently validate before accepting
   the block into their own chains.

The GBT node can reuse applicable template-preparation work and can propagate
through its normal path. Other relay nodes avoid waiting for repeated local
validation before starting their own relay.

A separate trusted full validator can perform step 3 instead. It needs the
appropriate network rules and parent state and must validate the final solved
block's proof of work, transactions, commitments, and contextual rules. Checking
only the template, acknowledging receipt, or recognizing the header hash is
insufficient. Preserve the validated byte buffer without mutation: equal header
hashes alone do not establish equal block bodies. Validate again after any change.

Stop fan-out on rejection, timeout, or an inconclusive upstream result. A duplicate
response alone does not prove that the exact submitted body was validated.

This sequence is an operator recommendation, not an endpoint prerequisite.
Access to the endpoint authorizes relay; it does not establish prior validation.
The endpoint does not accept or verify a validation certificate.

## Request and response

Send `Content-Type: application/octet-stream` with exactly one complete block in
Zcash binary serialization. Do not hex-encode it or wrap it in JSON. The maximum
body size is the network block-size limit; trailing bytes are rejected. Body
receipt has a 30-second deadline. Pending bodies and submissions are bounded.

Before registering the block, the relay runs two state lookups concurrently,
sharing one 100-millisecond deadline: one for the block's parent and one for the
block's own header hash. The hash lookup is the same one the verifier uses for its
duplicate check.

- If the hash is already committed to any active chain, the endpoint returns
  `duplicate` at once. It does not register the body, relay it, or validate it.
- If the parent is confirmed and the hash is confirmed new, relay starts without
  waiting for validation.
- Otherwise the block is validated without early relay and is announced only
  after it commits. This covers an unconfirmed parent, a hash that another path
  is already committing, and a lookup that fails or misses the deadline. The
  verifier still runs its own duplicate check.

A block whose parent is still missing returns `inconclusive`. In every case
except an already committed hash, the HTTP response waits for the local result.
HTTP 200 has a plain-text body:

| Result | Meaning |
| --- | --- |
| `accepted` | Locally validated and committed, possibly to a side chain. |
| `rejected` | Local validation rejected the block. |
| `duplicate` | The node already committed a block with this header hash. This does not say whether the submitted body matches it. |
| `duplicate-inconclusive` | A submission with this header hash is still in flight. |
| `inconclusive` | Acceptance could not be established; the caller may retry. |

Acceptance does not certify best-chain selection or delivery to any peer. The
eight-minute validation deadline returns `inconclusive` and withdraws temporary
relay storage; it does not roll back a state operation already handed off.

Once an authenticated request passes its header checks and takes a receipt slot,
which happens as soon as the HTTP server parses the request head, its body is
received and submitted by a task that the HTTP connection does not own. A client
disconnect from then on does not abandon the submission: receipt, decoding, the
state lookups, validation and relay continue without the client. This includes a
client that writes the whole request and closes the connection at once. The task
keeps its slot until the submission finishes, so at most 16 bodies are buffered
whether or not their clients stay connected. A body that ends early, because the
client disconnected mid-body or the 30-second receipt deadline passed, is never
submitted and frees its slot. A client that disconnects gets no response, so it
cannot learn the outcome.

Transport errors use HTTP 400 for malformed blocks, 401 for missing or invalid
credentials when cookie authentication is enabled, 403 for browser requests,
404 when disabled, 405 for a method other than POST, 408 for body receipt timeout,
413 for oversized bodies, 415 for the wrong content type, and 429 when submission
capacity is exhausted.

A commit preserves announcements already dispatched to peers. The normal committed
broadcast shares their per-connection delivery tracking, avoiding a second inventory
on healthy connections while still reaching peers that missed early relay. If the
block commits before early dispatch, only the committed path announces it.

## Invalid blocks and peer penalties

Calling the trusted endpoint before upstream validation succeeds can distribute
invalid bytes before any full validator approves them. The relay still validates
afterward, but peers may already have disconnected or banned its IP. A local
rejection or timeout removes its pending body and withdraws queued legacy TCP
announcements and body responses, including responses that fetched the body
before withdrawal; those peers receive "not found" instead. The v2 compatibility
adapter withdraws body responses the same way, checked when it encodes the
response; its announcements are not withdrawn. The final permission check is
ordered against withdrawal at the start of each local write. Writes already
started may finish; bytes already sent cannot be recalled. A corrected-body
retry has independent permission even when its header hash matches the rejected
submission. Temporary bodies compute each legacy message checksum from their exact
bytes and never read or populate the shared checksum cache. A different body with
the same header and length therefore cannot poison a corrected retry. Committed
state reads retain checksum sharing. Ordinary `submitblock` early relay, enabled by
`optimistic_block_inventory`, applies the same withdrawal when contextual
validation fails.

An invalid submission does not disable early relay for later submissions.

A pending body is never served in place of a committed block. When a peer asks for
a hash, the relay serves the committed block if state has one, and the pending body
only otherwise.

The duplicate lookup cannot close every race: the same block can commit through
another path, for example gossip from the GBT node, after the lookup and before the
trusted submission's own validation. Its validation then reports a duplicate. A
header hash does not authenticate the transactions, so the relay compares the
submitted block with the committed one:

- If they are equal, peers receive the same bytes from either, so queued
  announcements and in-flight body responses continue.
- If they differ, the pending body is withdrawn like a rejected one.
- State reports the duplicate as soon as the other copy is in its write
  channel, usually before that write finishes. The relay therefore keeps
  reading the committed block every 50 milliseconds for up to five seconds. If
  it does not appear by then, for example because the other copy was rejected,
  the bodies cannot be compared and the pending body is withdrawn too. If it did
  match, peers whose body responses were withdrawn get "not found" for a block
  the relay may later commit, with the side effect described below.

Until that comparison finishes, which can take up to five seconds, the HTTP
response and one pending-body slot are held, and body responses that fetched the
pending body can still start writing it, as they could before any rejection.

A block that was relayed early but not accepted, because it was `rejected`, its
validation timed out, or its duplicate body was withdrawn, has a known side
effect. Peers that request it after the relay withdraws it get "not found" and
treat the relay as missing that block hash for up to about two minutes. If the
same header hash is later submitted with corrected bytes, those peers ignore this
relay's new announcement until the mark expires; they can still fetch the block
from other peers that announce it. The recommended sequence avoids this except
when the committed block cannot be read to settle a duplicate.

Monitor `rpc.trusted_submission.*` metrics and rejection logs.
`rpc.trusted_submission.relay_requested_without_commit`, labelled by outcome,
counts every submission whose early relay was requested but that this submission
did not commit, with a warning log. `rpc.trusted_submission.duplicate_body`
counts duplicate verdicts by whether the body was `identical`, `differs` or
`unconfirmed`, and `rpc.trusted_submission.early_relay_skipped` counts skipped
early relay by reason. Upstream
validation reduces ban risk but cannot eliminate validator bugs or consensus
disagreements. Legacy peer penalties, v2 compatibility gossip, and native v2
block-sync policies are distinct; the endpoint does not change any of them.
