# Native P2P message contracts

This is the catalog for Zakura's native P2P exchange contracts. Contract
requirements live beside this file; implementation PRs add their evidence.

The catalog groups 14 wire messages into seven exchanges. Requests and
responses remain separate wire kinds, but a request owns regulation for the
work and response frames it causes. Legacy Bitcoin-compatible gossip messages
are outside this catalog.

Read these documents first:

- [Property testing](../../design/property-testing.md) explains how requirements
  become generated tests and regressions.
- [Message-regulation design](../../design/peer-message-regulation.md) explains
  the runtime traffic controls.
- [Message-regulation specification](regulation.md)
  defines the required production behavior.

## Status

Status is tracked separately for each contract layer:

- **TBD:** requirements have not been written.
- **Draft:** an earlier proposal is preserved, but it still needs stable IDs
  and reconciliation with the production protocol.
- **Specified:** requirements exist, but implementation evidence is incomplete.
- **Partially implemented:** some requirements have evidence and the contract
  names what remains.
- **Implemented:** the contract links every requirement to passing evidence.
- **Covered by request:** response load is owned by the initiating request;
  this does not complete the response's wire or receiving-side contract.

The Discovery v2 and Header Sync v9 links preserve successor designs from the
original proposal. They do not specify the currently deployed stream versions.

## Catalog

| Stream | Exchange | Message role | Wire format | State and lifecycle | Regulation |
| --- | --- | --- | --- | --- | --- |
| Discovery (4) | Introduction | `Hello` | [Draft v2](discovery-introduction.md) | [Draft v2](discovery-introduction.md) | [Draft v2](discovery-introduction.md) |
| Discovery (4) | Peer lookup | `GetPeers` — request | [Draft v2](discovery-peer-lookup.md) | [Draft v2](discovery-peer-lookup.md) | [Draft v2](discovery-peer-lookup.md) |
|  |  | ↳ `Peers` — response | [Draft v2](discovery-peer-lookup.md) | [Draft v2](discovery-peer-lookup.md) | Covered by draft `GetPeers` |
| Discovery (4) | Service lookup | `GetServices` — request | [Draft v2](discovery-service-lookup.md) | [Draft v2](discovery-service-lookup.md) | [Draft v2](discovery-service-lookup.md) |
|  |  | ↳ `Services` — response | [Draft v2](discovery-service-lookup.md) | [Draft v2](discovery-service-lookup.md) | Covered by draft `GetServices` |
| Header sync (5) | Advertisement | `Status` | [Draft v9](header-advertisement.md) | [Draft v9](header-advertisement.md) | [Draft v9](header-advertisement.md) |
| Header sync (5) | Header lookup | `GetHeaders` — request | TBD; [v9 successor draft](header-lookup.md) | TBD; [v9 successor draft](header-lookup.md) | TBD; [v9 successor draft](header-lookup.md) |
|  |  | ↳ `Headers` — response | TBD; [v9 successor draft](header-lookup.md) | TBD; [v9 successor draft](header-lookup.md) | Covered by future request contract |
|  |  | ↳ `HeadersOutcome` — terminal response | TBD; [v9 successor draft](header-lookup.md) | TBD; [v9 successor draft](header-lookup.md) | Covered by future request contract |
| Block sync (6) | Advertisement | `Status` | [Draft](block-advertisement.md) | [Draft](block-advertisement.md) | [Draft](block-advertisement.md) |
| Block sync (6) | Block range | `GetBlocks` — request | [Implemented](block-range.md#wire-format-contract) | [Implemented](block-range.md#serving-model-contract) | [Implemented](block-range.md#regulated-load-contract) |
|  |  | ↳ `Block` — response item | [Draft](block-range.md#draft-response-receiving-contract) | [Sending implemented](block-range.md#serving-model-contract); [receiving draft](block-range.md#draft-response-receiving-contract) | [Covered by `GetBlocks`](block-range.md#regulated-load-contract) |
|  |  | ↳ `BlocksDone` — terminal response | [Draft](block-range.md#draft-response-receiving-contract) | [Sending implemented](block-range.md#serving-model-contract); [receiving draft](block-range.md#draft-response-receiving-contract) | [Covered by `GetBlocks`](block-range.md#regulated-load-contract) |
|  |  | ↳ `RangeUnavailable` — terminal response | [Draft](block-range.md#draft-response-receiving-contract) | [Sending implemented](block-range.md#serving-model-contract); [receiving draft](block-range.md#draft-response-receiving-contract) | [Covered by `GetBlocks`](block-range.md#regulated-load-contract) |

The indented rows are protocol roles, not Rust subtypes.

## Contract shape

Use the layers that apply to the exchange:

1. **Wire format:** discriminators, flags, canonical encoding, message-specific
   preallocation cap, numeric bounds, truncation, trailing bytes, and arbitrary
   input.
2. **State model:** prerequisites, emitted actions, replies, ownership,
   replacement, cancellation, retry, and concurrency behavior.
3. **Regulated load:** charge arithmetic, rate and outstanding budgets,
   pending and response queues, overload outcomes, cleanup, and useful
   progress.

Every contract defines:

- the production entry points and boundary under test;
- stable requirement IDs and their implementation status;
- peer, driver, and internal input classes;
- boundary-biased generated cases and explicit invalid cases;
- observable outcomes and required coverage;
- replay and sensitivity expectations; and
- limitations or deferred layers.

The [property-testing workflow](../../design/property-testing.md#implementation-workflow)
explains how these requirements acquire executable evidence. The first
specified example is the [block-range exchange](block-range.md).
