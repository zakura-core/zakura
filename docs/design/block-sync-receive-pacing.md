# Block-sync receive pacing

Ordinary sync can deliver a burst of block bodies and their completion messages
faster than the receiver's message allowance. Closing the connection on that
burst discards useful in-flight work and forces the downloader to recover.

The block-sync reader checks the frame and message size limits, then waits for
message credit before forwarding the frame to its service. It retains its one
current frame while waiting and performs no further reads. Existing channel
bounds and QUIC flow control therefore apply while the independent writer can
continue sending. Stream or connection cancellation ends the wait.

The allowance remains `network.zakura.message_rate_per_second`, with a burst
capacity equal to one second of credit. Streams of the same kind on a connection
share one bucket. Block-sync readers acquire credit in FIFO order, so another
reader cannot repeatedly take a waiting reader's refill. Waiting does not reserve
future tokens; cancelling a waiter releases its queue position without charging
a frame. A frame consumes exactly one token when it is admitted.

This count budget applies to incoming block-sync requests and responses. It is
separate from the serving budgets that account for GetBlocks queries and retained
response bytes. Other ordered streams retain their existing rate-rejection
behavior.

`zakura.p2p.ratelimit.message.delayed` counts frames admitted after waiting, and
`zakura.p2p.ratelimit.message.delay_seconds` measures those completed waits,
including time behind another reader. The trace event is `message.delayed`.
Cancelled waits are not completed admissions and do not increment these metrics.
The existing `message.throttled` event continues to describe the other admission
paths; it is not emitted for a successfully paced block-sync frame.
