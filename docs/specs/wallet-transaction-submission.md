# Wallet transaction submission

Status: **Draft for review**

Version: 0.1

Date: 2026-09-06

Scope: Native Zakura P2P v2 submission and wallet fallback

This specification defines the messages and behavior proposed by the
[design document](../design/wallet-transaction-submission.md). A wallet discovers
a serving node, submits a signed transaction, and receives the node's admission
result. Wallets retain lightwalletd fallback. Tor submissions continue through
lightwalletd until the separate Tor follow-on is available.

This draft selects the wire values, admission contract, and initial wallet
defaults. These choices are specified here and are not yet deployed.
Message regulation integration and node resource budgets remain pending the
GetBlocks work. Section 9 separates that dependency from rollout requirements.

## 1. Contract

The uppercase terms MUST, MUST NOT, SHOULD, and MAY carry their
[BCP 14 meanings](https://www.rfc-editor.org/rfc/rfc8174.html). They describe the
requirements proposed by this draft, not guarantees of the current implementation.

| Term | Meaning |
| --- | --- |
| Client | The wallet's submission component. |
| Provider | A Zakura node offering the submission service. |
| Admission | The existing node pipeline's decision to place a transaction in the verified mempool. |
| Submission pass | One foreground send or scheduled background retry, with bounded attempts and elapsed time. |
| Exact identity | The unmined transaction identifier, including authorizing data where the transaction format requires it. |

`Accepted` means admission by one provider. It MUST NOT be treated as proof of
propagation, durable retention, or block inclusion. Confirmation remains the
wallet's responsibility through its existing chain observation path.

The service receives complete serialized transactions. Wallet keys, account
identifiers, and partially signed transactions are not request fields.

## 2. Negotiation

### 2.1 Service registration

| Property | Value |
| --- | --- |
| Discovery service ID | `zakura.tx_submit.v1` |
| Connection protocol | Existing `p2p-v2/1` |
| Stream mode | `RequestResponse` |
| Stream version | `1` |
| Capability bit | `ZAKURA_CAP_TX_SUBMIT = 1 << 6` (`0x40`) |
| Stream kind | `ZAKURA_STREAM_TX_SUBMIT = 7` |

These values are unused in the [shared registry][capability-registry] inspected
at `f2cfab1cf59ccc6c3e280bbe0e6ab2c98a9645af`. Implementation MUST register them
centrally and check for intervening allocations before merge. Retired values
MUST NOT be reused.

The client MUST validate the authenticated provider identity, network ID, and
genesis chain ID through the existing handshake. Submission requires a mutually
supported capability and stream version. Unsupported peers are skipped within
the client's retry budget.

Providers MUST register the service through the existing service registry and
advertise it only when the listener and admission integration are enabled.
Temporary overload is reported by the service; an advertisement is not a promise
of immediate capacity.

### 2.2 Request direction

The client opens a fresh bidirectional stream for each operation. The existing
stream prelude MUST contain a request ID. The client MUST allocate IDs from a
strictly increasing counter within each connection generation and open a new
connection before the counter would wrap.

Providers MUST correlate requests by connection and stream, retaining request-ID
state only for active operations. They MUST NOT retain a history of completed
IDs or enforce connection-wide ID uniqueness. Independent streams can arrive
out of order, so a lower ID than one previously received is not a protocol error.
Reused IDs on different streams MUST NOT combine their responses or accounting.
Transaction deduplication uses the exact transaction identity under section 5.

Wallet clients MUST reject peer-opened submission requests before decoding
transaction payloads.

The shared requester MUST associate each response with the provider, connection
generation, stream, and request ID of the operation that opened it, and validate
the expected response type. Response payloads do not repeat the request ID.
A delayed response from a replaced connection MUST NOT complete a request on
the replacement connection.

Transport framing, correlation, deadlines, and cancellation belong in the shared
requester. Submission supplies its own response decoder. The legacy response
validator MUST NOT be extended with submission-specific cases.

## 3. Discovery

Providers advertise the service in existing signed node records. Clients use
native discovery's bounded `GetPeers` request with the submission service filter.
The filter MUST recognize the new service ID; block-serving capabilities are
not prerequisites for selection.

Clients MUST validate record signatures, chain identity, protocol compatibility,
sequence, expiration, and dial addresses using the shared discovery rules.
Peer-supplied addresses MUST NOT direct a wallet to private or local network
services. An explicitly configured local node is a separate permitted input.

The default bootstrap configuration MUST contain at least three independently
operated sources.
Clients SHOULD also retain validated peer records and allow user-configured
nodes. Bootstrap sources supply candidates, not an authoritative provider list.
Cache expiration or bootstrap failure MUST trigger bounded rediscovery and then
the wallet's fallback, rather than indefinite connection attempts.

An authenticated client MUST be able to request a bounded peer sample without
publishing a reachable self-record. Provider-record validation remains unchanged.
The wallet client profile must support this exchange without starting the normal
full-node discovery publisher or requiring a public listening address.

Selection SHOULD spread submissions across network groups and discovery sources,
using independently known operator diversity where available. Different keys or
addresses MUST NOT be treated as proof of independent ownership. Fanout, candidate
counts, retained records, and discovery attempts MUST have configured bounds.

## 4. Messages

### 4.1 Framing

Each stream carries one request frame and at most one response frame. The client
finishes its send half after the request; the provider finishes its send half
after its response. Additional frames MUST NOT create additional admission work.
They are a stream protocol error and do not recall an already admitted transaction.

Use Zakura's existing `Frame` encoding. Its eight-byte header contains
`message_type: u16`, `flags: u16`, and `payload_len: u32`. All integer fields in
this service are unsigned little-endian values; byte arrays retain their defined
order. Flags MUST be zero. No compression or application fragmentation is defined.

Message types are scoped to the submission stream:

| Message | `message_type` | Direction |
| --- | --- | --- |
| `GetInfo` | `0x0001` | Client to provider |
| `Submit` | `0x0002` | Client to provider |
| `Info` | `0x8001` | Provider to client |
| `SubmitResult` | `0x8002` | Provider to client |

Fields appear in the order shown below. Parsers MUST reject unknown tags, nonzero
flags, invalid enum values, inconsistent lengths, and trailing payload bytes.
Such errors close the affected stream; they are not transaction rejection results.
A malformed response leaves the client without a known submission outcome.

### 4.2 Wire limits

| Constant | Value | Meaning |
| --- | --- | --- |
| `MAX_SUBMIT_TX_BYTES` | `zakura_chain::block::MAX_BLOCK_BYTES` (currently 2,000,000) | Shared ceiling for serialized transaction bytes in one request. |
| `MAX_FORMATS` | 8 | Maximum entries in `Info.formats`. |
| `MAX_SUBMISSION_FRAME_BYTES` | `MAX_SUBMIT_TX_BYTES + 8` (currently 2,000,008) | Largest `Submit`, including its eight-byte frame header. |
| `MAX_INFO_FRAME_BYTES` | 115 | Largest `Info`, including its frame header. |
| `MAX_RESULT_FRAME_BYTES` | 113 | Largest `SubmitResult`, including its frame header. |

The transaction ceiling MUST reuse the shared [`MAX_BLOCK_BYTES`][block-size]
constant used by the transaction decoder. The submission frame ceiling MUST be
derived from it plus the frame header. This is a decoding ceiling; a mined
transaction must also leave room for the rest of its block.

These wire bounds are separate from verification budgets and the node's local
size policy, whose inspected default is 250,000 bytes. `Info.max_tx_bytes`
advertises the provider's effective admission limit.

All frames MUST also fit the applicable negotiated transport limits. A client
MUST offer capacity for the maximum response to its requested operation. A
provider MUST reject a stream with insufficient response capacity before
dispatching admission work.

Frame lengths MUST be bounded before allocation. A provider MAY enforce a lower
inbound cap derived from its configured transaction policy. If the declared
frame exceeds that cap, it may reset the stream without receiving the body or
returning a typed rejection. A transaction received within the frame cap can
still receive `TooLarge` if local policy excludes it. Clients MUST preserve the
distinction between a response and a stream failure.

### 4.3 Common fields

`TipObservation` contains a one-byte presence tag. `0` means absent; `1` is
followed by `height: u32` and `hash: [u8; 32]`. The hash uses the existing Zcash
block-hash wire encoding, not display-order hexadecimal. Its maximum size is
37 bytes. It is the provider's observation, not a chain proof.

`TransactionIdentity` begins with a one-byte tag:

| Tag | Following bytes | Meaning |
| --- | --- | --- |
| `0` | None | Identifier was not computed. |
| `1` | 32-byte `txid` | Legacy unmined identity. |
| `2` | 32-byte `txid`, then 32-byte authorizing data commitment | Witnessed unmined identity. |

Identifiers MUST use the shared transaction library and its wire serialization.
For the current formats, versions 1–4 use legacy identities and versions 5–6 use
witnessed identities. Supporting an identity encoding does not activate a
transaction version on a network. A witnessed identity MUST NOT be reduced to
`txid` for deduplication or response matching. See section 10 for the pinned
protocol authority.

### 4.4 GetInfo and Info

`GetInfo` has an empty payload. `Info` has these fields:

| Field | Encoding | Meaning |
| --- | --- | --- |
| `readiness` | `u8` | `0 = Ready`, `1 = NotReady`, `2 = Busy`. |
| `max_tx_bytes` | `u32` | Current transaction size limit for this connection. |
| `format_count` | `u8` | Number of format entries, at most `MAX_FORMATS`. |
| `formats` | Repeated pair of `u32` values | Serialized version header, including its flags, and version group ID. Use group ID zero for formats without that field. |
| `tip` | `TipObservation` | Provider's current chain observation. |

Format pairs MUST be unique and sorted by numeric version header, then group ID.
They describe formats the provider currently supports for admission, not a
promise that a particular transaction is valid.

`Ready` requires a present tip, a nonempty format list, and a positive size limit.
The limit MUST NOT exceed local policy, `MAX_SUBMIT_TX_BYTES`, or the connection's
effective inbound `Submit` frame capacity minus eight bytes. An unavailable service
may return zero size and an empty format list.

The client MAY request `Info` to check readiness or policy before sending a
transaction. It MAY submit directly using cached policy information or handle
policy differences through the submission result. The provider MUST process
`Submit` without requiring an earlier `GetInfo`. Admission MUST check current
readiness, format, and size policy regardless of any earlier metadata response.

### 4.5 Submit

| Field | Encoding | Meaning |
| --- | --- | --- |
| `transaction` | Entire frame payload | One complete serialized Zcash transaction. |

The frame's `payload_len` is the transaction length and MUST be from 1 through
`MAX_SUBMIT_TX_BYTES`. Check this bound before allocating the payload. The
transaction decoder MUST consume the entire payload. The provider
computes the identity; the client does not supply a trusted hash alongside the
transaction. A correctly framed request containing invalid transaction encoding
receives `Rejected / InvalidEncoding` when response capacity is available.

### 4.6 SubmitResult

| Field | Encoding | Meaning |
| --- | --- | --- |
| `result` | `u8` | `0 = Accepted`, `1 = AlreadyPresent`, `2 = Rejected`, `3 = RetryLater`. |
| `reason` | `u16` | Code from the table below. |
| `identity` | `TransactionIdentity` | Computed identity when available. |
| `tip` | `TipObservation` | Chain context associated with the decision when available. |

The following result/reason combinations are permitted:

| Result | Reason | Meaning |
| --- | --- | --- |
| `Accepted`, `AlreadyPresent` | `0x0000 None` | Successful mempool observation. |
| `Rejected` | `0x0101 InvalidEncoding` | Transaction bytes cannot be decoded completely. |
| `Rejected` | `0x0102 UnsupportedFormat` | This provider does not support the transaction format. |
| `Rejected` | `0x0103 TooLarge` | Transaction exceeds the provider's current size policy. |
| `Rejected` | `0x0104 Policy` | Other local relay policy, such as fee policy. |
| `Rejected` | `0x0105 InvalidTransaction` | Validation failed against the reported context. |
| `Rejected` | `0x0106 Expired` | Transaction is expired in the reported context. |
| `Rejected` | `0x0107 AlreadyMined` | Provider reports the transaction is already in its selected chain. |
| `RetryLater` | `0x0201 Busy` | Admission capacity is unavailable. |
| `RetryLater` | `0x0202 NotReady` | Provider cannot currently perform normal admission. |
| `RetryLater` | `0x0203 MissingContext` | Required validation context or an ancestor is unavailable. |
| `RetryLater` | `0x0204 ContextChanged` | A chain change prevented a definitive admission result. |
| `RetryLater` | `0x0205 InternalError` | A local processing failure prevented a definitive result. |

All other combinations are invalid in stream version 1. Unknown result or reason
codes MUST NOT be interpreted as success or silently treated as `Rejected`.
Adding result or reason codes requires a new negotiated stream version.

`Accepted` and `AlreadyPresent` require both an exact identity and a tip.
Other results MUST include the identity if computation completed, and context
when the decision depends on it. Early rejection or overload may omit either.
A client MUST compare any returned identity with its own transaction before
applying the result. Internal exception text is not included in the wire response.

`AlreadyMined` is a chain claim, not mempool acceptance or confirmation evidence.
The wallet checks it through its existing confirmation path.

## 5. Admission

### 5.1 Shared operation

Define `AdmitTransaction` in `zakura-node-services` and implement it in the
existing mempool owner. The service checks the frame and size limits, decodes
once into the shared `UnminedTx` type, and calls this operation. Parsing remains
subject to the resource reservations in section 7.

| Input | Contract |
| --- | --- |
| Transaction | One decoded `UnminedTx`, with its serialized size and exact identity computed from the received bytes. |
| Source | Existing `QueueSource::Zakura` attribution populated from the authenticated peer, never from request fields. |
| Deadline | Absolute monotonic deadline for this admission attempt; queueing does not restart it. |
| Cancellation | A signal that the caller no longer needs completion delivery. It does not release capacity still owned by running work. |

The operation produces one typed completion containing the admission outcome,
exact identity, and associated tip under section 4.6. Immediate refusal returns
the same completion type. Queue acceptance alone is not successful completion.
The P2P adapter maps domain outcomes to the wire codes; the mempool MUST NOT
depend on transport frames or wire message types.

Connection generation, stream, and request ID remain in the calling network task
for response correlation. The mempool retains source attribution through queued
and running work. `Queue`, `QueueFromPeer`, and `AdmitTransaction` MUST share the
existing verification and insertion machinery, preserving the existing callers'
contracts. Rust type layout and future/channel mechanics are implementation
choices. Regulation permit plumbing remains deferred under section 7.2.

### 5.2 Completion

The admission sequence is:

1. Check framing, request direction, local size policy, and available capacity
   before the work each check protects.
2. Decode the transaction and compute its exact unmined identity.
3. Check for verified or pending copies and apply the duplicate rules below.
4. Run the existing policy, consensus, and contextual validation pipeline.
5. Attempt verified mempool insertion and capture the associated chain context.
6. Return the decision. Existing mempool gossip handles admitted transactions.

The service MUST NOT report `Accepted` merely because work was queued or proofs
finished. It requires successful insertion. `AlreadyPresent` requires the exact
authorized transaction to be in the verified mempool at the observation point.
A pending verification is neither result.

Pending duplicates MAY join the existing verification through bounded completion
waiters; otherwise return `RetryLater / Busy`. Sharing verification MUST NOT
remove request or response accounting for additional callers. A different
authorizing commitment remains a different candidate even when `txid` matches.

A chain change before the insertion decision MUST use the mempool's existing
revalidation rules or return `ContextChanged`. A later reorg or eviction does not
invalidate an earlier admission observation. Report the context of that
observation rather than a newer, unrelated tip.

Disconnecting or timing out cancels result delivery; it does not guarantee that
verification stopped or undo insertion. Work that continues MUST retain its
resource reservations until completion. Results MUST NOT be delivered on a
replacement connection. Public submissions do not acquire the RPC retry queue's
background retention policy.

## 6. Wallet behavior

### 6.1 Results and fallback

Wallets MUST persist authorized transaction bytes and the state needed for safe
retries before transmission. Initial sends and background retries use the same
persisted routing policy.

| Observation | Required action |
| --- | --- |
| `Accepted` or `AlreadyPresent` with matching identity | Record acceptance, end automatic retries for this pass, and monitor confirmation. |
| `Busy` or `NotReady`, including `Info` readiness | Back off before retrying the same node or select another suitable node within budget. |
| `MissingContext` or `ContextChanged` | Restore known ancestors where applicable, wait for context, or try another node within budget. |
| `InternalError` | Treat the admission outcome as uncertain and use bounded retry or failover. |
| `UnsupportedFormat`, `TooLarge`, or `Policy` | Try a suitable alternative within budget; do not repeat unchanged bytes to the same unchanged policy. |
| `InvalidEncoding`, `InvalidTransaction`, or `Expired` | Check the claim with wallet validation or chain observation. If confirmed, stop automatic retransmission. Otherwise use bounded failover. |
| `AlreadyMined` | Check confirmation through the wallet's chain observation path. Stop retransmission if confirmed; otherwise use bounded failover. |
| Timeout, stream reset, malformed response, or cancellation after transmission began | Preserve an unknown outcome. Retry only the original bytes when routing policy and authorization allow. |

A remote rejection MUST NOT by itself release reserved inputs, create a
replacement payment, or mark a transaction as confirmed. A transport failure
before any transaction bytes were sent may be recorded as not transmitted;
failures after transmission begins MUST be treated conservatively.

Each native submission pass MUST have positive, finite attempt and elapsed-time
limits. Discovery, connection setup, metadata, and transaction attempts all count
toward the native deadline. Responses and peer changes MUST NOT reset it.
Backoff MUST remain within the remaining budget.

If native attempts cannot obtain acceptance, and no terminal reason has been
confirmed, the wallet MUST use its configured lightwalletd fallback. That path
has its own finite attempt limit and deadline and sends the same signed bytes.
Exhausting both routes ends the pass with a pending or failed state appropriate
to the evidence, not a fabricated rejection. Any later background pass retains
the transaction state and rechecks authorization, expiry, and route policy.

The client's initial fanout MUST be bounded. An acceptance ends further retry
scheduling; already transmitted requests may still complete. Multiple
acknowledgments are not a consensus quorum.

### 6.2 Client defaults

The reusable client starts with the following defaults. Wallets MAY override
them with explicit finite configuration. They are initial client policy choices,
not wire constants or measured performance guarantees. Mobile tests validate
and, where necessary, tune them before release.

| Setting | Default |
| --- | --- |
| Native phase deadline | 30 seconds, including discovery, connection setup, metadata, and submissions. |
| Provider attempts | 4 per native phase; a failed connection or retry to the same provider consumes an attempt. |
| Concurrent native connections | 2 across discovery and submission; at most 2 providers receive a transaction concurrently. |
| Connection deadline | 5 seconds including address resolution and handshake. |
| `GetInfo` response deadline | 3 seconds. |
| `Submit` response deadline | 10 seconds, including provider queueing and validation. |
| Same-provider retry backoff | 1 second, doubled after each retryable failure, capped at 8 seconds; add random jitter of up to 25%. |
| Discovery work | At most 3 source connection attempts and 2 `GetPeers` queries per phase, requesting 16 records each; 3 seconds per response and 10 seconds total including connection setup. |
| Peer cache | 128 validated records, at most one current record per identity. |
| Cache refresh | Refresh on use when the last successful sample is at least 10 minutes old, or when no eligible candidate remains. |
| Cached `Info` | At most 30 seconds, scoped to provider identity and network; negotiated connection limits still apply. |
| Lightwalletd attempts | 2 sequential attempts, preferring different configured endpoints when available. |
| Direct lightwalletd deadline | 30 seconds for the phase, at most 15 seconds per attempt including connection setup. |
| Tor/lightwalletd deadline | 60 seconds for the phase, at most 30 seconds per attempt including Tor connection setup. |
| Automatic background retries | After 1 minute, doubling to at most 15 minutes between passes, with up to 25% additional jitter. |

Each operation uses the smaller of its own timeout and the remaining phase
budget. The native phase ends early when no eligible attempt remains; it does
not wait out its deadline before fallback. Under Tor, only the Tor/lightwalletd
phase runs. Background scheduling rechecks the wallet state under section 6.1;
app suspension may delay a pass and MUST NOT produce a burst of missed passes
on resume. Concurrent callers MUST share these connection and discovery bounds.

An attempt is one provider visit, including optional metadata and ordered
dependency submission. There are at most 8 `Submit` requests per native phase
across all visits, including ancestor replay. Lightwalletd likewise has at most
8 transaction submissions per phase. Larger dependency sets retain progress for
a later pass. Every request also consumes the phase deadline.

Peer records expire under the shared signed-record rules and MUST NOT have
their lifetime extended by a cache read or an unsuccessful refresh. The shared
maximum accepted future lifetime is currently 24 hours. Refresh uses the phase's
discovery budget and does not start a continuously running discovery loop.
Unsolicited records do not count as a successful requested sample. Evict expired
records first, then the least recently useful records while preserving source
diversity where possible. A policy rejection invalidates conflicting cached
`Info`; it does not reset an attempt budget.

### 6.3 Dependencies

For dependent transactions, the client MUST send parents before children to a
provider that accepted or already holds those parents. On failover it replays
required ancestors in order, allowing independently confirmed ancestors to be
omitted. Partial acceptance MUST be retained across cancellation and restarts.
The service provides no atomicity across transactions.

### 6.4 Tor routing

With Tor enabled, the wallet MUST use its existing Tor/lightwalletd route for
submission. It MUST NOT perform direct native discovery, metadata requests, or
submission as a fallback from Tor. This applies to background retries as well as
foreground sends.

An explicit route change MUST stop pending connection and retry work that would
violate the new policy. It cannot recall already transmitted transactions.
For direct native operation, clients SHOULD use ephemeral connection identities
and MUST omit wallet/account identifiers from service metadata.

## 7. Message regulation

### 7.1 Required guarantees

Submission will use the shared regulation framework once its integration is
settled. It MUST provide these guarantees:

- Bound retained transaction bytes, parsing expansion, verification work and
  concurrency, duplicate waiters, and pending response capacity.
- Reserve capacity before dispatching the work it covers, and retain ownership
  through queued work, verification, and result delivery as applicable.
- Apply aggregate submission limits across peers. Source-group and peer limits
  provide fairness; a fresh identity MUST NOT reset aggregate capacity.
- Return `RetryLater / Busy` when admission capacity is unavailable and a bounded
  response can be sent. If no response capacity exists, reject the stream within
  its deadline rather than queueing an unbounded response.
- Keep block validation and normal relay progressing during submission load.
  Peer-set policy separately bounds transient client connections and idle time.

Wire-byte limits MUST NOT be presented as bounds on total process memory or
verification CPU. A valid transaction's fee is not a substitute for admission
resource accounting.

### 7.2 Pending integration

The [message regulation specification][regulation-spec] and
[GetBlocks implementation][getblocks-work] are still being developed. This draft
does not select a regulator API, queue scheduler, cost model, or production
budget. GetBlocks serving rates MUST NOT be copied into transaction verification
defaults without independent measurements.

Codec, discovery, result handling, and wallet integration work may proceed
against explicit interfaces and controlled test doubles. The public service
MUST remain disabled until regulation is wired, its limits are specified, and
the capacity and progress tests pass. An unlimited temporary path is not an
implementation of this specification.

## 8. Conformance

Tests should use controlled peers and local fixtures. Runtime acceptance requires
the following evidence; writing this specification does not claim it exists.

| Area | Required cases |
| --- | --- |
| Negotiation (§2) | Wrong chain, unsupported capability/version, and peer-opened requests to a wallet. |
| Request correlation (§2) | Increasing client IDs without wrap; out-of-order stream arrival; reused IDs on concurrent and later streams without response or accounting crossover; no completed-ID history after operations finish; and responses associated with the wrong stream or a replaced connection. |
| Discovery (§3) | Submission service filtering, a client without a public self-record, record expiration, unsuitable addresses, bootstrap outage, and bounded fallback. |
| Encoding (§4) | Canonical field order; empty, truncated, oversized, and trailing payloads; unknown tags/flags; exact response caps; and invalid result/reason combinations. |
| Identity (§4–5) | Legacy and witnessed vectors; display/wire byte-order differences; matching `txid` with different authorizing commitments; and response identity mismatch. |
| Admission (§5) | Queued versus inserted work, pending duplicates, bounded waiters, validation failure, chain changes, already mined transactions, and eviction after acceptance. |
| Wallet routing (§6) | Submission with and without prior `GetInfo`; stale policy metadata; every result/reason action; overload and policy rejection reaching fallback; default attempt and deadline enforcement; shared connection bounds; restart without retry bursts; dependency replay; and unknown outcomes. |
| Tor (§6.4) | No native discovery or submission under Tor, including failures, background retries, and route changes. |
| Regulation (§7) | Capacity retained after timeout/disconnect, bounded combined submission work across peers, and continuing block validation and relay under controlled load. |
| Propagation (§1, §5) | A wallet-like client submits a valid transaction, another node receives it through gossip, and regtest mines it in native and mixed native/legacy topologies. |

### 8.1 Initial vectors

These vectors cover complete application frames, excluding the stream prelude.
They use the message and reason values above.

`GetInfo` has no payload:

```text
01 00 00 00 00 00 00 00
```

`SubmitResult` for `RetryLater / NotReady`, with no computed identity or tip,
has a five-byte payload. Its request ID belongs to the stream context:

```text
02 80 00 00 05 00 00 00
03 02 02 00 00
```

Transaction-dependent vectors MUST be generated from the shared chain fixtures
and checked against the existing serializers before freezing stream version 1.

## 9. Readiness

### 9.1 Pending regulation

The remaining design dependency is the regulation integration described in
section 7.2. Settle the shared API, scheduling policy, verification cost model,
and measured node budgets after the GetBlocks work. The wire values, admission
contract, and initial wallet defaults are specified above.

### 9.2 Rollout requirements

Before enabling the public service, implement the shared admission operation
and regulation, register the protocol values, and pass the conformance tests.
Before enabling wallet adoption, validate the client defaults on iOS and Android
under network changes, app suspension, slow validation, and fallback failures.

Wallet releases MUST ship at least three independently operated bootstrap
sources and verify successful submission through independently operated providers
with a working gossip path to the wider network. Discovery remains open to
additional operators.

The Tor follow-on will separately specify transport, endpoint advertisements,
bootstrap discovery, and source accounting. Those decisions are outside this
native wire specification.

## 10. References

- [Design document](../design/wallet-transaction-submission.md).
- [Zakura stream prelude and frame encoding][framing], inspected at
  `1e0d5c245c61dc31591dab30fbe41496dd020ec6`.
- [Signed discovery records][discovery].
- [Shared unmined transaction identities][identities] and
  [identifier wire serialization][identity-wire].
- [Peer message regulation specification][regulation-spec], draft at
  `44028a51893d17fe4202fee0e54a1c59ce2fc946`. Its discovery/header/block message
  inventory must be extended for submission; this document does not claim that
  its policy is already implemented for the new service.
- Zcash Protocol Specification **v2026.7.0-187-ge753a6 [NU6.3 proposal]**, pinned to
  `e753a6a301912cf77202db8f0d840f4796f5cca1`. Section 7.1.1,
  [Transaction Identifiers, PDF page 130][protocol-identities], and its
  [pinned source][protocol-source] define identity semantics. The NU6.3/v6 text
  is a proposal, not an assertion of Mainnet activation.
- [ZIP 239][zip-239], **Final** in the same snapshot, defines the witnessed
  identity as the transaction ID followed by the authorizing data commitment.

[regulation-spec]: https://github.com/zakura-core/zakura/blob/44028a51893d17fe4202fee0e54a1c59ce2fc946/docs/specs/peer-message-regulation.md
[getblocks-work]: https://github.com/zakura-core/zakura/pull/892
[framing]: https://github.com/zakura-core/zakura/blob/1e0d5c245c61dc31591dab30fbe41496dd020ec6/crates/zakura-network/src/zakura/handshake.rs#L1192-L1317
[block-size]: https://github.com/zakura-core/zakura/blob/1e0d5c245c61dc31591dab30fbe41496dd020ec6/crates/zakura-chain/src/block/serialize.rs#L18-L24
[capability-registry]: https://github.com/zakura-core/zakura/blob/f2cfab1cf59ccc6c3e280bbe0e6ab2c98a9645af/crates/zakura-network/src/zakura.rs#L58-L72
[discovery]: https://github.com/zakura-core/zakura/blob/1e0d5c245c61dc31591dab30fbe41496dd020ec6/crates/zakura-network/src/zakura/discovery/protocol.rs#L220-L299
[identities]: https://github.com/zakura-core/zakura/blob/1e0d5c245c61dc31591dab30fbe41496dd020ec6/crates/zakura-chain/src/transaction/unmined.rs#L1-L13
[identity-wire]: https://github.com/zakura-core/zakura/blob/1e0d5c245c61dc31591dab30fbe41496dd020ec6/crates/zakura-chain/src/transaction/hash.rs#L250-L283
[protocol-identities]: https://zips.z.cash/protocol/nu6_3.pdf#txnidentifiers
[protocol-source]: https://github.com/zcash/zips/blob/e753a6a301912cf77202db8f0d840f4796f5cca1/protocol/protocol.tex#L13608-L13618
[zip-239]: https://github.com/zcash/zips/blob/e753a6a301912cf77202db8f0d840f4796f5cca1/zips/zip-0239.rst#L28-L37
