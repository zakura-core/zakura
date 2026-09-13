# Response identity and lifetime

A written request gives the peer permission to send one specific response. Local
work decides whether we still need its blocks. These are separate facts.

## Example

We request blocks 100–102 and receive block 100. Our deadline then expires, so
another peer may take the remaining local work. The original peer can still send
blocks 101–102 or end after its one-block prefix. We keep its original hashes and
byte limit until its ending arrives or the connection closes. We cannot issue an
overlapping request on that same connection in the meantime.

`ResponseCredit` shares the item and byte arithmetic across messages. GetBlocks
owns its ordered hashes and the rules for BlocksDone and RangeUnavailable. Size
estimates help schedule work but do not create extra obligations for the peer.
Numeric Status limits remain fixed for a connection. A change closes locally
without treating the peer as malicious. Availability ranges may still change.

## Properties

The receiver fixture sends requests through the real writer queue and decodes
real frames. Tests are grouped under `peer_routine::response_contract`.

| File | Contract |
| --- | --- |
| `identity.rs` | R01 and R03–R08. Only the next requested hash is valid. Duplicate or unrelated bodies and endings cannot consume another response. |
| `lifetime.rs` | R02 and R09–R11. Deadlines, finality, reorganization and reassignment preserve the original response. Endings retain protocol slots. Connection closure cleans up unfinished work. |
| `limits.rs` | R12, F04 and C06. Count actual body bytes, reject unauthorized bodies before decoding or handler waits, and distinguish local handler failure from peer faults. A valid-body control verifies the allocation observer. |
| `terminal_counts.rs` | Generated R06–R07 request, prefix and ending counts, including the saved failing seed. |

Generated local-change histories cover counts 1–128 and prefixes from empty to
complete. Fixed witnesses cover the boundaries and the production receiver's
liveness, work ownership and connection cleanup paths.

## Local execution

The `response-lifetime` nextest profile selects these properties and affected
receiver, queue and registry regressions. It has no retries. Its longer outer
timeout accommodates generated histories that decode real blocks. Each fixture
transition still has a two-second deadline.

This layer does not claim session replacement is atomic or that all request
metadata is funded. Those are separate contracts above this layer. It also does
not claim verifier, transport or whole-stack qualification.
