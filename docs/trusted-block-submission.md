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

When the relay already has the block's parent, relay starts without waiting for
validation. Otherwise the block is validated without early relay and is announced
only after it commits; a block whose parent is still missing returns
`inconclusive`. In both cases the HTTP response waits for the local result.
HTTP 200 has a plain-text body:

| Result | Meaning |
| --- | --- |
| `accepted` | Locally validated and committed, possibly to a side chain. |
| `rejected` | Local validation rejected the block. |
| `duplicate` | The node already committed the block identified by this header. |
| `duplicate-inconclusive` | A submission with this header hash is still in flight. |
| `inconclusive` | Acceptance could not be established; the caller may retry. |

Acceptance does not certify best-chain selection or delivery to any peer. The
eight-minute validation deadline returns `inconclusive` and withdraws temporary
relay storage; it does not roll back a state operation already handed off. An
HTTP disconnect does not abandon local validation.

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
submission. Ordinary `submitblock` early relay, enabled by
`optimistic_block_inventory`, applies the same withdrawal when contextual
validation fails.

An invalid submission does not disable early relay for later submissions.

A block that was relayed early but not accepted, because it was `rejected` or
its validation timed out, has a known side effect. Peers that request it after
the relay withdraws it get "not found" and treat the relay as missing that block
hash for up to about two minutes. If the same header hash is later submitted with
corrected bytes, those peers ignore this relay's new announcement until the mark
expires; they can still fetch the block from other peers that announce it. This
only happens when the bytes sent to the relay differ from the bytes validated
upstream, which the recommended sequence prevents.

Monitor `rpc.trusted_submission.*` metrics and rejection logs. Upstream
validation reduces ban risk but cannot eliminate validator bugs or consensus
disagreements. Legacy peer penalties, v2 compatibility gossip, and native v2
block-sync policies are distinct; the endpoint does not change any of them.
