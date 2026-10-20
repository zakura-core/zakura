# Wallet transaction submission

Status: **Draft for review**

Version: 0.2

Date: 2026-10-04

Scope: Native wallet submission sessions, node capacity limits, and wallet
behavior

This specification defines the protocol in the
[design document](../design/wallet-transaction-submission.md). A wallet opens a
short session with a Zakura node, sends signed transactions, and receives one
admission result per transaction. Wallets keep lightwalletd as a fallback.

This document is authoritative. Nothing in it is implemented or deployed.
Version 0.1 (zakura#906) was never implemented. Version 0.2 replaces its
session model, result codes, discovery, and capacity sections, and it keeps
stream version 1 because no version 1 implementation exists.

## 1. Contract

The uppercase terms MUST, MUST NOT, SHOULD, and MAY carry their
[BCP 14 meanings](https://www.rfc-editor.org/rfc/rfc8174.html).

| Term | Meaning |
| --- | --- |
| Wallet | The wallet's submission component. |
| Node | A Zakura node that serves wallet sessions. |
| Session | One wallet connection, from accept to close. |
| Request | One `Submit` stream and its `SubmitResult`. |
| Admission | The existing node pipeline's decision to place a transaction in the verified mempool. |
| Commit point | The moment a request's job takes a verification permit and starts decoding. |
| Exact identity | The unmined transaction identifier, including authorizing data where the transaction format requires it ([ZIP 239][zip-239]). |
| Endpoint list | The wallet-maintained list of nodes it may contact (section 7.1). |

`Accepted` means admission by one node. It MUST NOT be treated as proof of
propagation, durable retention, or block inclusion. The wallet confirms
inclusion through its existing chain observation path.

The node receives complete serialized transactions. Wallet keys, account
identifiers, and partially signed transactions are not request fields.

## 2. Connection

### 2.1 Registry

| Property | Value |
| --- | --- |
| ALPN | `zakura-wallet/1` |
| Capability bit | `ZAKURA_CAP_TX_SUBMIT = 1 << 7` (`0x80`) |
| Stream kind | `ZAKURA_STREAM_TX_SUBMIT = 7` |
| Stream mode | `RequestResponse` |
| Stream version | `1` |

Main uses capability bits 0, 2, 3, and 5, and retired bits 1 and 4. Main uses
stream kinds 2 through 6. Draft zakura#961 uses bit 6 and stream kind 8.
Regulated block sync reserves bit 8 for version 3 of stream kind 6.
Implementation MUST register these values centrally and recheck open
allocations before merge. Retired values MUST NOT be reused.

### 2.2 Admission before the handshake

The node MUST classify each incoming connection from the ALPN list in the
client's first QUIC Initial packet, before it starts the TLS handshake.

1. If the list is exactly `zakura-wallet/1`, the connection is a wallet
   session. The node MUST refuse it without handshake work when all `W` slots
   are in use, or when the source already holds `ceil(W / F)` slots. A source is
   an IPv4 address or an IPv6 /64. Otherwise the node reserves a slot and
   accepts with the wallet transport profile (section 2.3).
2. If the list contains `zakura-wallet/1` and any other ALPN, the node MUST
   refuse the connection.
3. If the node cannot read the ALPN list from the first Initial packet, it
   applies the full-peer profile. If the handshake then selects
   `zakura-wallet/1`, the node MUST close the connection before it reads any
   stream.
4. Under load, the node MAY require QUIC address validation before it counts a
   source, so spoofed addresses cannot fill a source share.

The wallet MUST send a ClientHello that fits in one Initial packet.

Wallet sessions MUST NOT count toward peer connection limits
(`DEFAULT_ZAKURA_MAX_CONNECTIONS`, `max_connections_per_ip`, or
`ServicePeerLimits`). Peer connections MUST NOT use wallet slots.

### 2.3 Wallet transport profile

QUIC grants flow-control credit during the handshake, and a node cannot revoke
it. The wallet profile bounds what one session can make the node buffer.

| QUIC setting | Value |
| --- | --- |
| Connection receive window | `R = Binflight + 32 KiB` |
| Stream receive window | `Btx + 64` bytes |
| Concurrent bidirectional streams | `K + 2` (control, session, `K` requests) |
| Concurrent unidirectional streams | 0 |
| Datagrams | Disabled |
| Idle timeout | At most `Tsession` |
| Keep-alive | Disabled |
| Remote NAT traversal addresses | 0 |

The 32 KiB margin covers the control exchange (`MAX_CONTROL_PAYLOAD_BYTES` is
16 KiB), stream preludes, frame headers, and `Finish`. The 64-byte margin
covers one prelude and one frame header.

### 2.4 Native handshake

The wallet MUST initiate the connection. The node MUST NOT dial a wallet.

Both sides MUST use the native control handshake. The wallet's hello MUST carry
the initiator role, the configured network and genesis chain, a fresh nonce, no
legacy upgrade transcript, a capability offer of exactly `ZAKURA_CAP_TX_SUBMIT`,
and `required_channels = 0`. The node MUST refuse any other capability offer.
The wallet MUST authenticate the node identity pinned in its endpoint list.

The node MUST exclude wallet sessions from gossip, discovery, address
advertisement, sync, keep-alive management, peer-count targets, and reconnect
scheduling.

The wallet SHOULD generate a fresh transport identity per connection. Transport
identities MUST NOT grant resources. Both sides MUST refuse submission in 0-RTT
early data.

## 3. Session

### 3.1 Streams and request IDs

The wallet opens every stream. Each stream's prelude carries a request ID.

| Request ID | Stream | Frames |
| --- | --- | --- |
| `0` | Session stream; one per connection | Node → wallet: `SessionInfo`. Wallet → node: `Finish`. |
| `1..=Nsession` | Request stream | Wallet → node: `Submit`. Node → wallet: `SubmitResult`. |

1. The wallet MUST open the session stream right after the handshake. It sends
   only the prelude, then waits.
2. The node MUST write one `SessionInfo` frame on the session stream and then
   finish its send half. The wallet's send half stays open for `Finish`.
3. The wallet MUST NOT open a request stream before it has received and
   validated `SessionInfo` (section 7.2).
4. The wallet MUST allocate request IDs consecutively from 1. Streams MAY
   arrive out of order.
5. The node MUST track request IDs in a bitmap of `Nsession` bits. It MUST treat
   each of the following as a protocol violation, checked from the prelude
   alone: a duplicate ID, an ID above `Nsession`, an ID above a received
   `Finish` value, a second session stream, and a request stream that arrives
   before the node sent `SessionInfo`.

A protocol violation closes the session. The node keeps no record of it.

The node MUST NOT open streams to a wallet. The wallet MUST reset any
node-opened stream before reading its payload.

### 3.2 Deadlines and states

| Deadline | Starts | Ends |
| --- | --- | --- |
| `T_hs` | Connection accept | `SessionInfo` sent |
| `Topen` | `SessionInfo` sent | Last moment the node accepts a new request stream |
| `Tsession` | `SessionInfo` sent | Hard close |
| `Trequest` | Request header read | Result sent |
| `Tdrain` | Draining starts | Close |

A session moves through these states:

| State | Entered when |
| --- | --- |
| `Handshaking` | The node accepts the connection. |
| `Active` | The node sends `SessionInfo`. |
| `Sealed` | The node receives `Finish`. |
| `Draining` | Every sealed ID has a result, `Nsession` requests have results, `Topen` expires with no outstanding request, or `Tsession` expires. |
| `Closed` | Draining results are sent, or `Tdrain` expires. |

No event extends a deadline. A completed request does not reset any deadline.

The node holds the session's slot from accept until both the connection has
closed and every verification job the session started has exited.

## 4. Messages

### 4.1 Framing

Each stream carries one frame in each direction at most. The sender finishes
its send half after its frame. Additional frames are a protocol violation and
create no admission work.

Frames use Zakura's existing `Frame` encoding. Its eight-byte header contains
`message_type: u16`, `flags: u16`, and `payload_len: u32`. All integers in this
service are unsigned little-endian. Byte arrays keep their defined order. Flags
MUST be zero.

| Message | `message_type` | Direction | Stream |
| --- | --- | --- | --- |
| `Submit` | `0x0002` | Wallet → node | Request |
| `Finish` | `0x0003` | Wallet → node | Session |
| `SessionInfo` | `0x8001` | Node → wallet | Session |
| `SubmitResult` | `0x8002` | Node → wallet | Request |

Message type `0x0001` (`GetInfo` in version 0.1) is reserved and MUST NOT be
reused. Parsers MUST reject unknown message types, nonzero flags, invalid enum
values, inconsistent lengths, and trailing bytes. Such errors are protocol
violations, not transaction rejections. A malformed result leaves the wallet
without a known outcome.

### 4.2 Wire limits

| Constant | Value | Meaning |
| --- | --- | --- |
| `MAX_SUBMIT_TX_BYTES` | `zakura_chain::block::MAX_BLOCK_BYTES` (currently 2,000,000) | Ceiling for serialized transaction bytes in one `Submit`. |
| `MAX_FORMATS` | 8 | Maximum entries in `SessionInfo.formats`. |
| `MAX_SUBMISSION_FRAME_BYTES` | `MAX_SUBMIT_TX_BYTES + 8` | Largest `Submit` frame. |
| `MAX_SESSION_INFO_FRAME_BYTES` | 150 | Largest `SessionInfo` frame. |
| `FINISH_FRAME_BYTES` | 16 | Size of every `Finish` frame. |
| `MAX_RESULT_FRAME_BYTES` | 113 | Largest `SubmitResult` frame. |

These are decoding ceilings. The node's effective transaction limit is `Btx`
(section 6.2), which the node advertises in `SessionInfo`. Each prelude's
`max_frame_bytes` MUST cover the largest response frame for that stream.

### 4.3 Common fields

`TipObservation` starts with a one-byte presence tag. `0` means absent. `1` is
followed by `height: u32` and `hash: [u8; 32]`. The hash uses the Zcash
block-hash wire encoding, not display-order hexadecimal. The field is at most
37 bytes. It is the node's observation, not a chain proof.

`TransactionIdentity` starts with a one-byte tag:

| Tag | Following bytes | Meaning |
| --- | --- | --- |
| `0` | None | The node did not compute the identifier. |
| `1` | 32-byte `txid` | Legacy unmined identity (versions 1–4). |
| `2` | 32-byte `txid`, then 32-byte authorizing data commitment | Witnessed unmined identity (versions 5–6). |

Identifiers MUST use the shared transaction library and its wire serialization.
A witnessed identity MUST NOT be reduced to `txid` for deduplication or result
matching.

### 4.4 SessionInfo

| Field | Encoding | Meaning |
| --- | --- | --- |
| `readiness` | `u8` | `0 = Ready`, `1 = NotReady`, `2 = Busy`. |
| `retry_after_ms` | `u32` | Retry hint for `NotReady` and `Busy`. `0` means no hint. Ignored when `Ready`. |
| `tip` | `TipObservation` | The node's best chain tip. Required when `Ready`. |
| `next_branch_id` | `u32` | Consensus branch ID at height `tip.height + 1`. Required when `Ready`; `0` otherwise. |
| `min_fee_rate` | `u64` | Minimum fee rate the node currently admits, in zatoshis per `MEMPOOL_TRANSACTION_COST_THRESHOLD` (10,000) units of [ZIP 401][zip-401] cost. `0` means no floor beyond the standard fee checks. |
| `max_tx_bytes` | `u32` | `Btx`. |
| `max_requests` | `u16` | `Nsession`. |
| `max_outstanding` | `u8` | `K`. |
| `max_outstanding_bytes` | `u32` | `Binflight`. |
| `admission_window_ms` | `u32` | `Topen`. |
| `session_deadline_ms` | `u32` | `Tsession`. |
| `request_deadline_ms` | `u32` | `Trequest`. |
| `format_count` | `u8` | Number of format entries, at most `MAX_FORMATS`. |
| `formats` | `format_count` pairs of `u32` | Serialized version header with flags, then version group ID. Group ID is zero for formats without that field. |

The largest payload is 142 bytes, so the largest frame is 150 bytes.

Format pairs MUST be unique and sorted by version header, then group ID. They
list formats the node currently admits; support does not imply validity.

A `Ready` `SessionInfo` MUST have a present tip, a nonempty format list,
`1 ≤ K ≤ Nsession`, `0 < Btx ≤ Binflight`, and `0 < Topen ≤ Tsession`. A
`NotReady` or `Busy` `SessionInfo` MAY carry zero limits and an empty format
list. After a `NotReady` or `Busy` `SessionInfo`, the node closes the session
once it has flushed the frame.

The node MUST set `min_fee_rate` to the larger of two values:

- An operator-configured floor, if one exists.
- When the verified mempool is full, the lowest package fee rate that
  [zakura#1233][fee-eviction]'s eviction rule would displace, plus
  `MARGINAL_FEE`, both in zatoshis per `MEMPOOL_TRANSACTION_COST_THRESHOLD` of
  cost.

A transaction with fee `f` and cost `c` meets the hint when
`f × 10,000 ≥ min_fee_rate × c`. The hint is advisory. The node MUST recheck
readiness, format, size, and fee policy for every request. `SessionInfo` does
not reserve capacity.

### 4.5 Submit

| Field | Encoding | Meaning |
| --- | --- | --- |
| `transaction` | Entire frame payload | One complete serialized Zcash transaction. |

`payload_len` is the transaction length, from 1 through `Btx`. The node MUST
check it against `Btx`, `K`, and `Binflight` from the frame header, before it
reads the body. A transaction above `Btx` returns `Rejected / TooLarge`. A
request that would exceed `K` or `Binflight` returns
`NotAdmitted / SessionLimit`. Neither reads the body. The decoder MUST consume the
entire payload. The node computes the exact identity. The wallet does not
supply a trusted hash.

### 4.6 Finish

| Field | Encoding | Meaning |
| --- | --- | --- |
| `last_request_id` | `u64` | The highest request ID the wallet will open. |

The wallet sends `Finish` once and then finishes the session stream's send
half. `Finish` seals the request set. It neither cancels requests nor waits for
results. A value below an ID the node has already received, or above
`Nsession`, is a protocol violation. Missing IDs up to the sealed value MAY
still arrive until `Topen` expires.

### 4.7 SubmitResult

| Field | Encoding | Meaning |
| --- | --- | --- |
| `result` | `u8` | `0 = Accepted`, `1 = Rejected`, `2 = NotAdmitted`, `3 = Indeterminate`. |
| `reason` | `u16` | Code from the table below. |
| `identity` | `TransactionIdentity` | Exact identity when computed. |
| `tip` | `TipObservation` | Chain context of the decision when available. |

Only these result and reason pairs are valid:

| Result | Reason | Meaning |
| --- | --- | --- |
| `Accepted` | `0x0000 None` | The exact transaction is in the node's verified mempool, whether this request inserted it or it was already present. |
| `Rejected` | `0x0101 InvalidEncoding` | The bytes do not decode completely. |
| `Rejected` | `0x0102 UnsupportedFormat` | The node does not admit this transaction format. |
| `Rejected` | `0x0103 TooLarge` | The transaction exceeds `Btx` or local size policy. |
| `Rejected` | `0x0104 Policy` | Other local relay policy, including fee policy. |
| `Rejected` | `0x0105 InvalidTransaction` | Validation failed against the reported context. |
| `Rejected` | `0x0106 Expired` | The transaction is expired in the reported context. |
| `Rejected` | `0x0107 AlreadyMined` | The node reports the transaction in its best chain. |
| `Rejected` | `0x0108 MissingContext` | The node lacks a required input or ancestor. This is definite for the node's context, not a validity claim. |
| `NotAdmitted` | `0x0201 Busy` | The node had no capacity, or the request expired before its commit point. |
| `NotAdmitted` | `0x0202 NotReady` | The node cannot perform normal admission. |
| `NotAdmitted` | `0x0203 SessionLimit` | The request exceeded `K`, `Binflight`, or `Topen`. |
| `Indeterminate` | `0x0301 Timeout` | `Trequest` expired after the commit point. |
| `Indeterminate` | `0x0302 ContextChanged` | A chain change prevented a definite result. |
| `Indeterminate` | `0x0303 InternalError` | A local failure prevented a definite result. |

`NotAdmitted` means verification never started. `Indeterminate` means work
started and the node cannot state the outcome. `Accepted` covers transactions
that were already present, so a result does not reveal earlier mempool
contents.

`Accepted` MUST carry an exact identity and a tip. Other results MUST carry the
identity once computed, and the tip when the decision depends on it. The wallet
MUST compare a returned identity with its own transaction before it applies the
result. Unknown result or reason codes MUST NOT be treated as success or as
`Rejected`. New codes require a new stream version.

`AlreadyMined` is a chain claim. The wallet checks it through its confirmation
path.

## 5. Admission

### 5.1 Shared operation

Define `AdmitTransaction` in `zakura-node-services` and implement it in the
existing mempool.

| Input | Contract |
| --- | --- |
| Transaction | The serialized bytes, checked against the frame limits. |
| Source | New `QueueSource::Wallet`, populated by the network task, never from request fields. |
| Deadline | Absolute monotonic deadline for this request. Queueing does not restart it. |
| Cancellation | A signal that the caller no longer needs the result. It does not stop running work. |

The operation returns one typed completion: the outcome, the exact identity,
and the associated tip. The P2P service maps the outcome to wire codes. The
mempool MUST NOT depend on transport types. `Queue`, `QueueFromPeer`, and
`AdmitTransaction` MUST share the existing verification and insertion
pipeline.

`QueueSource::Wallet` MUST NOT reach `mempool_misbehavior_score` or any ban,
cooldown, or ignore list. Wallet submissions MUST NOT acquire the RPC retry
queue's background retention.

### 5.2 Sequence and commit point

1. Check the prelude, frame header, `Btx`, `K`, and `Binflight`.
2. Read the body and queue it serialized. The node schedules queued requests
   round-robin across sessions.
3. **Commit point:** take one of `V` verification permits and decode the
   transaction.
4. Compute the exact identity. Check the verified mempool and in-flight
   verifications for the same identity.
5. Run the existing policy, consensus, and contextual validation pipeline.
6. Attempt verified mempool insertion and record the chain context.
7. Return the result. Existing gossip relays admitted transactions.

A request that expires or is cancelled before its commit point MUST release its
capacity and return `NotAdmitted / Busy` if the session still exists. After the
commit point, the job keeps its permit and its session slot until it exits,
even when the wallet disconnects. Expiry after the commit point returns
`Indeterminate / Timeout`.

The node MUST NOT report `Accepted` for queued or partly verified work.
A request whose identity matches an in-flight verification MUST wait on that
verification instead of starting another. Each waiter still counts as an
outstanding request of its own session. A different authorizing commitment is a
different transaction, even with the same `txid`.

A chain change before insertion uses the mempool's existing revalidation rules
or returns `ContextChanged`. A later reorg or eviction does not change an
earlier result. The node reports the context of its decision.

## 6. Capacity

### 6.1 Invariant

The node has one wallet limit, `W`. **If at most `W` wallet sessions exist,
wallet state MUST stay within the bounds in section 6.4.** Every bound is a
per-session constant times `W`, or a per-verification constant times `V`. No
wallet state outlives its session and its jobs.

### 6.2 Parameters

| Parameter | Meaning |
| --- | --- |
| `W` | Maximum concurrent wallet sessions, including handshaking and draining sessions and sessions with running jobs. Operator setting. |
| `F` | Per-source divisor. One source holds at most `ceil(W / F)` slots. |
| `V` | Maximum concurrent wallet verifications. Operator setting. |
| `Btx` | `min(local transaction size policy, Binflight)`. |
| `Binflight` | Maximum declared transaction bytes outstanding per session. |
| `K` | Maximum outstanding requests per session. |
| `Nsession` | Maximum requests per session. |
| `R` | Wallet connection receive window, `Binflight + 32 KiB`. |
| `C_quic` | Per-connection QUIC state under the wallet profile. Measured. |

### 6.3 Decoded transaction size

For a transaction of `S` serialized bytes, the node's memory from decoding
through sighash preparation is at most:

```text
D(S) = 11 × S + 4 KiB + U
U    = Σ over transparent inputs of (32 + spent scriptPubKey length)
```

`D` counts the serialized bytes, the decoded `Transaction`, the `UnminedTx`
wrapper, and the `SigHasher` with its `librustzcash` copy, at their peak. `U`
counts the spent outputs the verifier loads from state. Gossiped transactions
load the same outputs. Batch verifier items are bounded by the batch verifiers'
own limits and are not part of `D`.

The factor 11 comes from a heap-counting measurement on main
(`4127aa196`). The table lists peak bytes per element divided by wire bytes per
element. Each row repeats one element of a real Mainnet or Testnet transaction.

| Element | Wire bytes | Peak bytes | Ratio |
| --- | --- | --- | --- |
| Transparent output, empty script | 9 | 95 | 10.6 |
| Sapling spend (v5) | 352 | 3,011 | 8.6 |
| Sapling output (v5) | 948 | 6,165 | 6.5 |
| Sapling spend (v4) | 384 | 2,444 | 6.4 |
| Transparent input, empty script | 41 | 245 | 6.0 |
| Orchard action with its proof share | 3,156 | 18,135 | 5.7 |
| JoinSplit (v4) | 1,698 | 8,338 | 4.9 |

Orchard decoding rejects a non-canonical proof size, so an action always
carries its proof share. The largest fixed overhead at one element was
1,470 bytes above `11 × S`. Version 6 Ironwood bundles are not yet measured. A
node MUST NOT advertise a version 6 format until its elements are measured
against this formula. A change to the decoder or the sighash path MUST
re-run the measurement.

### 6.4 State bounds

| State | Bound | Freed when |
| --- | --- | --- |
| Wallet slots | `W` | Connection closed and the session's jobs exited |
| Slots per source | `ceil(W / F)` | Same as the slot |
| QUIC state and receive buffers | `W × (C_quic + R)` | Connection closed |
| Session record and request-ID bitmap | `W ×` (fixed record + `Nsession` bits) | Connection closed |
| Outstanding requests, serialized | `W × Binflight` bytes, `W × K` entries | Result sent, stream reset, or session closed before the commit point |
| Decoded transactions in verification | `V × D(Btx)` | Job exit, including after disconnect |
| In-flight identity index and waiters | `W × K` entries | Job exit |
| Results awaiting send | `W × K × 113` bytes | Sent, or `Tdrain` expiry |
| Persistent per-wallet state | None | Not applicable |

Total wallet memory is therefore at most:

```text
W × (C_quic + R + Binflight + record) + V × D(Btx) + W × K × 113
```

### 6.5 CPU

`W` does not bound CPU throughput; `V` does. One session's verification work is
bounded instead: at most `Nsession` transactions of at most `Btx` bytes.

```text
CPU per session ≤ c_hs + Nsession × c(Btx)
c(S)            = c_0 + c_byte × S
```

`c_hs` is the cost of one handshake. `c(S)` is the worst-case verification
time for an `S`-byte transaction. Benchmarks MUST measure `c_byte` for each
element type in section 6.3 and use the largest value.

### 6.6 Provisional profile

Qualification replaces these values. Implementations MUST label them
provisional.

| Parameter | Value |
| --- | --- |
| `W` | 2,048 |
| `F` | 16 (128 slots per source) |
| `V` | 32 |
| `Btx` | 250,000 (the default size policy) |
| `Binflight` | 256 KiB |
| `K` | 4 |
| `Nsession` | 16 |
| `T_hs` | 5 s |
| `Topen` | 10 s |
| `Tsession` | 30 s |
| `Trequest` | 10 s |
| `Tdrain` | 2 s |

With `C_quic` at most 64 KiB, the section 6.4 bound is about 1.3 GiB: 1.19 GiB
of session state, 84 MiB of decoded transactions, and under 1 MiB of results,
plus `V × U`.

## 7. Wallet behavior

### 7.1 Endpoint list

The wallet MUST choose nodes from a finite, wallet-maintained endpoint list.
Each entry MUST name the network, the endpoint address, and the node's identity
key. Entries SHOULD name the operator when known. The protocol does not
distribute or update the list. A list entry does not prove honesty or operator
independence.

### 7.2 SessionInfo checks

The wallet MUST bound its wait for `SessionInfo`. It MUST validate
`SessionInfo` before any upload, and close without uploading if any check
fails:

- `readiness` is not `Ready`.
- `tip.height` is below the highest anchor height the transaction uses.
- `tip.height` is more than `L` blocks below the wallet's own observed tip.
- The wallet knows the block hash at `tip.height`, and it differs from
  `tip.hash`.
- `next_branch_id` differs from the transaction's consensus branch ID.
- The transaction's nonzero expiry height is below `tip.height + 1`.
- The transaction does not meet `min_fee_rate`.
- The transaction exceeds `max_tx_bytes` or uses an unadvertised format.

The wallet MUST NOT wait for a timer or a larger batch before it dispatches an
eligible transaction.

### 7.3 Dispatch

The wallet MUST keep at most `K` requests and `Binflight` declared bytes
outstanding, open at most `Nsession` requests, and open no request after
`Topen`. It sends `Finish` after it dispatches its queue snapshot for the
session. New work starts a later session.

Each endpoint has its own queue, connection generation, outstanding-request
map, and backoff. A slow endpoint MUST NOT block another. The first `Accepted`
removes unsent copies from other queues. A later error MUST NOT override an
acceptance. A result belongs to its node, connection generation, stream, and
request ID; a result from an old connection resolves nothing.

### 7.4 Results and fallback

The wallet MUST persist the signed transaction bytes and its retry state
before it transmits them.

| Observation | Required action |
| --- | --- |
| `Accepted` with matching identity | Record acceptance, end this pass, and monitor confirmation. |
| `Rejected / UnsupportedFormat`, `TooLarge`, or `Policy` | Try another node within budget. Do not resend unchanged bytes to the same node. |
| `Rejected / InvalidEncoding`, `InvalidTransaction`, or `Expired` | Check the claim locally or through chain observation. Stop if confirmed; otherwise try another node within budget. |
| `Rejected / AlreadyMined` | Check confirmation. Stop if confirmed; otherwise try another node within budget. |
| `Rejected / MissingContext` | Send known ancestors first, or try another node. |
| `NotAdmitted` | Back off, honoring `retry_after_ms`, or try another node. |
| `Indeterminate`, timeout, stream reset, or malformed result after upload began | Record an unknown outcome. Retry only the original bytes. |
| Failure before any transaction byte was sent | Record the transaction as not transmitted. |

A remote rejection MUST NOT by itself release reserved inputs, create a
replacement payment, or mark a transaction confirmed.

Each native pass MUST have finite attempt and elapsed-time limits. Connection
setup, `SessionInfo`, and requests all count toward them. Results and node
changes MUST NOT reset them. When the native pass ends without acceptance or a
confirmed terminal reason, the wallet MUST use its configured lightwalletd
fallback with the same bytes and its own finite limits. Exhausting both routes
ends the pass in a pending or failed state that matches the evidence.

### 7.5 Defaults

Wallets MAY override these with explicit finite values. Mobile tests validate
them before release.

| Setting | Default |
| --- | --- |
| Native pass deadline | 30 s, including connection setup, `SessionInfo`, and requests. |
| Node attempts | 4 per native pass. A failed connection or retry to the same node consumes one. |
| Concurrent endpoints | 2. |
| Connection deadline | 5 s, including address resolution and handshake. |
| `SessionInfo` deadline | 3 s after the handshake. |
| Request deadline | `Trequest` from `SessionInfo`, at most 10 s. |
| Tip lag `L` | 2 blocks. |
| Same-node backoff | 1 s, doubled per retryable failure, capped at 8 s, plus up to 25% jitter. |
| Lightwalletd attempts | 2 sequential attempts, preferring different endpoints. |
| Direct lightwalletd deadline | 30 s per pass, at most 15 s per attempt. |
| Tor lightwalletd deadline | 60 s per pass, at most 30 s per attempt. |
| Background retries | After 1 minute, doubling to at most 15 minutes, plus up to 25% jitter. |

Each operation uses the smaller of its own timeout and the remaining pass
budget. App suspension MAY delay a pass and MUST NOT cause a burst of missed
passes on resume.

### 7.6 Dependencies

The wallet MUST send parents before children to a node that accepted or
already holds the parents. On failover it replays required ancestors in order.
Partial acceptance MUST survive cancellation and restart. The service provides
no atomicity across transactions.

### 7.7 Tor

Native submission is QUIC over UDP, and Tor carries only TCP. With Tor enabled,
the wallet MUST submit through its Tor route to lightwalletd. It MUST NOT open
native connections, including as a fallback or a background retry. A route
change MUST stop pending work that would violate the new route policy.

## 8. Conformance

Runtime acceptance requires the following evidence. This document does not
claim it exists.

| Area | Required cases |
| --- | --- |
| Pre-handshake admission (§2.2) | Wallet ALPN at `W` and at the source share; mixed ALPN lists; unreadable ALPN followed by a wallet handshake; peer limits unchanged by wallet load. |
| Transport profile (§2.3) | A wallet cannot make the node buffer more than `R`; a wallet cannot open more than `K + 2` streams. |
| Handshake (§2.4) | Wrong chain, extra capabilities, identity mismatch, node-opened streams. |
| Request IDs (§3.1) | Duplicate, out-of-range, post-`Finish`, and early request streams; out-of-order arrival; second session stream. |
| Deadlines (§3.2) | Every deadline expires without extension; slots stay held by running jobs after disconnect. |
| Encoding (§4) | Field order, truncated and trailing payloads, unknown types and flags, exact frame caps, and invalid result/reason pairs. |
| `SessionInfo` (§4.4) | `Ready` consistency rules; `min_fee_rate` from a full mempool and from a configured floor. |
| Identity (§4.3, §5) | Legacy and witnessed vectors; matching `txid` with different authorizing commitments; identity mismatch. |
| Admission (§5) | Expiry before and after the commit point; in-flight waiters; already-present transactions; chain changes; no misbehavior score for wallet sources. |
| Capacity (§6) | Saturation at `W` keeps peer sync and relay progressing; measured memory stays within the §6.4 bound; the §6.3 measurement holds for every advertised format. |
| Wallet (§7) | Every `SessionInfo` check skips without upload; per-endpoint queues; first acceptance; every result action; fallback; restart without retry bursts; dependency replay. |
| Tor (§7.7) | No native connection under Tor, including failures, retries, and route changes. |
| Propagation | A wallet submits a valid transaction, another node receives it through gossip, and regtest mines it in native and mixed topologies. |

### 8.1 Vectors

The session stream prelude: magic `ZKST`, stream kind 7, version 1, request
ID 0, and `max_frame_bytes` 150.

```text
5a 4b 53 54 07 00 01 00 01 00 00 00 00 00 00 00 00 96 00 00 00
```

`SessionInfo` with `NotReady`, a 1,000 ms retry hint, no tip, and zero limits
has a 42-byte payload:

```text
01 80 00 00 2a 00 00 00
01 e8 03 00 00 00 00 00 00 00 00 00 00 00 00 00
00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
00 00 00 00 00 00 00 00 00 00
```

`Finish` with `last_request_id = 3`:

```text
03 00 00 00 08 00 00 00
03 00 00 00 00 00 00 00
```

`SubmitResult` for `NotAdmitted / NotReady`, with no identity or tip:

```text
02 80 00 00 05 00 00 00
02 02 02 00 00
```

Vectors for a `Ready` `SessionInfo` and for transaction-dependent results MUST
be generated from the shared chain fixtures and checked against the existing
serializers before stream version 1 is frozen.

## 9. Readiness

Before the node enables wallet sessions:

- Implement pre-handshake classification, the wallet profile, the session
  service, and `AdmitTransaction`.
- Measure `C_quic`, the CPU constants in section 6.5, and the section 6.3
  formula for every advertised format.
- Replace the provisional profile with measured values.
- Pass the conformance cases in section 8.

Before wallets adopt native submission, validate the defaults on iOS and
Android under network changes, app suspension, slow validation, and fallback
failures. Shipped endpoint lists MUST contain at least three independently
operated nodes with a working gossip path to the wider network. Mainnet
[defaults to legacy P2P][mainnet-default], so participating nodes must enable
v2.

## 10. References

- [Design document](../design/wallet-transaction-submission.md).
- [zakura#906][pr-906]: version 0.1 of this design and specification.
- [Zakura stream prelude and frame encoding][framing].
- [Shared unmined transaction identities][identities] and
  [identifier wire serialization][identity-wire].
- [zakura#1233][fee-eviction]: fee-rate eviction, which defines the full-pool
  admission threshold behind `min_fee_rate`.
- [Peer message regulation specification][regulation-spec].
- [ZIP 239][zip-239]: witnessed transaction identity.
- [ZIP 317][zip-317]: proportional transfer fee mechanism.
- [ZIP 401][zip-401]: mempool cost and eviction.

[pr-906]: https://github.com/zakura-core/zakura/pull/906
[fee-eviction]: https://github.com/zakura-core/zakura/pull/1233
[framing]: https://github.com/zakura-core/zakura/blob/4127aa19644a5a7a4aa91f82088929624bdd7c20/crates/zakura-network/src/zakura/handshake.rs#L1197-L1255
[identities]: https://github.com/zakura-core/zakura/blob/4127aa19644a5a7a4aa91f82088929624bdd7c20/crates/zakura-chain/src/transaction/unmined.rs#L1-L13
[identity-wire]: https://github.com/zakura-core/zakura/blob/4127aa19644a5a7a4aa91f82088929624bdd7c20/crates/zakura-chain/src/transaction/hash.rs
[regulation-spec]: https://github.com/zakura-core/zakura/blob/4127aa19644a5a7a4aa91f82088929624bdd7c20/docs/specs/peer-message-regulation.md
[mainnet-default]: https://github.com/zakura-core/zakura/blob/4127aa19644a5a7a4aa91f82088929624bdd7c20/crates/zakura-network/src/config.rs
[zip-239]: https://zips.z.cash/zip-0239
[zip-317]: https://zips.z.cash/zip-0317
[zip-401]: https://zips.z.cash/zip-0401
