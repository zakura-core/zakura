# Session replacement fencing

A receiver must remain responsible for every request its writer starts. Checking
cancellation only before queueing is too early. The receiver can be replaced
while that request is still waiting to write.

## Example

An old GetBlocks request is queued when a new receiver replaces the old one.
Retiring the old `ResponseScope` stops that queued writer from emitting bytes.
If writing started first, the old peer can already respond. The connection must
close unless that response has ended. Finishing the request write is not the same
as receiving the response ending.

The scope shares one lock between request publication, the first write claim and
receiver retirement. An authorization owns the unfinished response. A writer
permission can publish and start but cannot finish it. The message validates its
own ending before releasing the authorization.

## Properties

| Test group | Contract |
| --- | --- |
| `response_scope::tests` | Publication completes before retirement returns. Retirement and first write have one winner. Dropping a started owner closes the connection. Validated endings permit reuse. |
| `service::tests::session_fencing` | Real GetBlocks queue and session replacement, including prepared, queued, writing and completed request writes. New connections remain usable. |
| `response_properties::lifecycle` | Generated histories independently track receiver generations, publication, writing, ending, owner drop and cancellation. Old writers remain fenced. |
| `response_properties::discovery` | A test adapter uses the real GetPeers codec to check identity, counts, bytes, one complete response, lost local interest and receiver retirement. |
| `response_properties::subscription` | A future subscription adapter checks explicit credit renewal, unchanged consumed totals, idle worker release, updates crossing Close and separate ending capacity. |

The adapters demonstrate shared rules across different message shapes. They do
not migrate production discovery or implement a production subscription. Memory
funding is the next separate layer.

## Local execution

The `response-session-fencing` nextest profile runs the scope and cross-message
properties, service session tests and request-writer regressions. It has no
retries and adds no workflow trigger.
