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

Local retention uses actual body bytes for both active and late responses. The
body's size replaces its current work estimate in the backlog calculation. If it
does not fit, we still count the response part, but drop the body and return our
own work for retry. Work already taken by another peer stays with that peer. The
fixed checkpoint window remains exempt so verification can finish its range.

## Response lookup

`ResponseIndex` is a shared index from matching keys to request positions. GetBlocks
uses it for each range's next hash and its starting height. Finding a response
takes logarithmic work instead of checking every outstanding range. Duplicate
keys remain visible so an ambiguous response is rejected before body decoding.

Consuming a body advances only that range's hash key. Local deadlines leave the
key intact. Completing or skipping a range removes its keys. The last stored
range moves into the vacant position and updates its own keys, so removal does
not shift the entire request list. A completed body range keeps its starting
height indexed until its ending arrives.

## Properties

The receiver fixture sends requests through the real writer queue and decodes
real frames. Tests are grouped under `peer_routine::response_contract`.

| File | Contract |
| --- | --- |
| `identity.rs` | R01 and R03–R08. Only the next requested hash is valid. A duplicate or unrelated body spends one part of the earliest open response and is discarded. An unrelated ending cannot consume another response, and a body with no open response is a fault. |
| `indexed_matching.rs` | Publication installs lookup keys before writes. Different completion orders and local detachment preserve matching. Skipped writes remove keys. Ambiguous hashes fail before decoding. |
| `lifetime.rs` | R02 and R09–R11. Deadlines, finality, reorganization and reassignment preserve the original response. Endings retain protocol slots. Connection closure cleans up unfinished work. |
| `limits.rs` | R12, F04 and C06. Count actual body bytes, reject unauthorized bodies before decoding or handler waits, and distinguish local handler failure from peer faults. A valid-body control verifies the allocation observer. |
| `retention.rs` | R12. Check exact-fit and one-byte-over retention with other reservations and buffered bytes. Bursts of underestimated bodies stop entering the backlog. Local refusal preserves response counts, retries and reassigned ownership. |
| `terminal_counts.rs` | Generated R06–R07 request, prefix and ending counts, including the saved failing seed. |

Generated local-change histories cover counts 1–128 and prefixes from empty to
complete. Fixed witnesses cover the boundaries and the production receiver's
liveness, work ownership and connection cleanup paths.

The shared index properties compare generated updates and removals with an
independent owner map. A 32,768-request case counts key comparisons to catch a
return to linear lookup, and lookup allocation observations cover missing,
unique and ambiguous keys. Window properties cover advancing keys, failed byte
charges, moved entries and endings retained after the last body. These checks
measure the index, not whole receiver throughput.

## Local execution

The `response-lifetime` nextest profile selects these properties and affected
receiver, queue and registry regressions. It has no retries. Its longer outer
timeout accommodates generated histories that decode real blocks. Each fixture
transition still has a two-second deadline.

This layer does not claim session replacement is atomic or that all request
metadata is funded. Those are separate contracts above this layer. It also does
not claim verifier, transport or whole-stack qualification.
The allocation-planning layer funds the index storage before publishing a range
and retains its allowance until the index drops. Each live range adds at most one
next-hash entry and one ending entry.
