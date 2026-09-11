# Service sessions

A service declares its stream types through `Service::streams()`. The transport
groups persistent streams with the same capability into one session layout.
The transport admits the complete layout through one `Service::add_peer()` call.
Each stream has independent readers, writers, and queues.

Request/response stream types remain in the declaration, but the transport opens
their streams per request. They do not participate in persistent session setup.
The service controls their application lifecycle.

## Declaring a session

For example, a service can declare data, requests, and events as three persistent
streams with the same capability:

```rust
const DATA: Stream = Stream {
    kind: 64,
    version: 1,
    frame_cap: 1024 * 1024,
    capability: 1 << 16,
    mode: StreamMode::Persistent,
};
const REQUESTS: Stream = Stream { kind: 65, ..DATA };
const EVENTS: Stream = Stream { kind: 66, ..DATA };
const LOOKUP: Stream = Stream {
    kind: 67,
    mode: StreamMode::RequestResponse,
    ..DATA
};

// Inside impl Service:
fn streams(&self) -> &[Stream] {
    &[DATA, REQUESTS, EVENTS, LOOKUP]
}

fn session_policy(&self) -> SessionPolicy {
    SessionPolicy {
        opening: SessionOpening::EitherSide,
        reopen: true,
    }
}
```

These identifiers illustrate the API. A production protocol must allocate its
own stream kinds and capability bit.

The service does not declare membership a second time. The transport waits for
data, requests, and events before handing their receive/send handles to the
service. It does not wait for a lookup request.

The service uses `message_types()`, `message_payload_limits()`, and
`stream_queue_depths()` to specify each stream's traffic and bounds. The protocol
defines message assignments; peers do not negotiate individual message types.
The service routes outgoing messages to the appropriate sender.

`stream_write_policy()` sets each persistent stream's write deadline. The default
is ten seconds. A service can choose another duration or `UntilCancelled`.
A service that chooses `UntilCancelled` must enforce its own progress deadline.

## Negotiating complete layouts

Each capability identifies a complete persistent layout for its service.
Alternative layouts use different capabilities. Each alternative retains the
same lowest stream kind, whose version ranks the layouts. The registry selects
the highest mutually supported version of that primary stream and includes every
member of its layout. It never mixes members from different alternatives.

For example, primary/request versions `3/1` and `2/4` select `3/1` when both
capabilities are available. The request stream's higher version in the older
layout does not override that choice.

Adding a required stream changes the protocol layout. Allocate a new capability
and advance the primary stream's version. Keep the primary kind stable.
Changing a message assignment also requires a compatible protocol transition.
The transport cannot make an old peer understand a new layout or message.

## Setup and retirement

Multi-stream sessions append the same nonzero eight-byte session identifier to
each ordinary stream prelude. This retains #943's two-stream setup encoding.
Single-stream sessions retain their existing prelude without an extra identifier.

The transport holds at most one incomplete session per service and connection.
The first complete member starts the setup deadline. Later members cannot extend
that deadline. Duplicate members, mismatched identifiers, and invalid declarations
cannot complete a session. Expiry releases every arrived member and defers another
offer through the existing cooldown.

`reserve_session()` charges service capacity once during setup.
`SessionResources::admitted()` signals complete setup.
The workers and application senders retain the shared resource owner until they
finish or drop it. Every member also consumes a transport stream slot.

Every persistent member shares a local session identity, cancellation token, and
message-rate budget. A remote close on any member retires the session.
Cancellation resets unfinished writes before a replacement can send frames.
A write deadline retires the session without closing unrelated services on the
connection. Protocol violations can still close the connection.

Dropping an application receiver stops delivery to that receiver. The transport
keeps reading within its frame and message-rate limits and discards those frames.
Retained application handles keep the session alive. Normal retirement waits for
every application handle to close and every member's queued writes to finish.

The transport records the first remote close or write timeout before cancelling
its session. Block sync settles that failure against unanswered download work,
including when cancellation wins over receiving EOF. Local cancellation alone
does not charge the peer for a stall.

The transport reports session exit after every worker and reader finishes.
Reopening follows the service's policy and demand. Ephemeral request completion
does not cancel the persistent session.

Setup readiness does not impose ordering across streams. For example, a request
can arrive before a status message on another stream. The service must handle
that ordering or perform an application handshake before processing requests.

## Migrating a pair consumer

Remove `OrderedStreamPair` and `ordered_stream_pair()`. Declare the persistent
members with one capability in `streams()`. Use the `Session*` policy, demand,
and resource APIs. The transport supplies all declared members together.

Move role-specific queue limits and write deadlines into the service hooks.
For the block-sync activation following #943, the service must declare the
one-slot request queue, the request write policy, and the 32-second data write
deadline. The transport no longer assigns those policies by role name.

Production block sync in #943 remains a single-stream protocol. This change does
not activate the later block-sync layout.
