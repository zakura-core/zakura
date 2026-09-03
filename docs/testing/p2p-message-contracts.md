# Native P2P message contracts

This is the catalog and authoring guide for Zakura's native P2P contracts.
Requirements are specified here before implementation PRs add evidence.

The catalog groups 14 wire messages into seven exchanges. Requests and
responses remain separate wire kinds, but a request owns regulation for the
work and response frames it causes. Legacy Bitcoin-compatible gossip messages
are outside this catalog.

Read these documents first:

- [Property testing](../design/property-testing.md) explains how requirements
  become generated tests and regressions.
- [Message-regulation design](../design/peer-message-regulation.md) explains
  the runtime traffic controls.
- [Message-regulation specification](../specs/peer-message-regulation.md)
  defines the required production behavior.

## Status

Status is tracked separately for each contract layer:

- **TBD:** requirements have not been written.
- **Specified:** requirements exist, but implementation evidence is incomplete.
- **Implemented:** the contract links every requirement to passing evidence.
- **Covered by request:** response load is owned by the initiating request;
  this does not complete the response's wire or receiving-side contract.

## Catalog

| Stream | Exchange | Message role | Wire format | State and lifecycle | Regulation |
| --- | --- | --- | --- | --- | --- |
| Discovery (4) | Introduction | `Hello` | TBD | TBD | TBD |
| Discovery (4) | Peer lookup | `GetPeers` — request | TBD | TBD | TBD |
|  |  | ↳ `Peers` — response | TBD | TBD | Covered by `GetPeers` (TBD) |
| Discovery (4) | Service lookup | `GetServices` — request | TBD | TBD | TBD |
|  |  | ↳ `Services` — response | TBD | TBD | Covered by `GetServices` (TBD) |
| Header sync (5) | Advertisement | `Status` | TBD | TBD | TBD |
| Header sync (5) | Header lookup | `GetHeaders` — request | TBD | TBD | TBD |
|  |  | ↳ `Headers` — response | TBD | TBD | Covered by `GetHeaders` (TBD) |
|  |  | ↳ `HeadersOutcome` — terminal response | TBD | TBD | Covered by `GetHeaders` (TBD) |
| Block sync (6) | Advertisement | `Status` | TBD | TBD | TBD |
| Block sync (6) | Block range | `GetBlocks` — request | [Specified](get-blocks-serving-contract.md#wire-format-contract) | [Specified](get-blocks-serving-contract.md#serving-model-contract) | [Specified](get-blocks-serving-contract.md#regulated-load-contract) |
|  |  | ↳ `Block` — response item | TBD | [Sending specified](get-blocks-serving-contract.md#serving-model-contract); receiving TBD | [Covered by `GetBlocks`](get-blocks-serving-contract.md#regulated-load-contract) |
|  |  | ↳ `BlocksDone` — terminal response | TBD | [Sending specified](get-blocks-serving-contract.md#serving-model-contract); receiving TBD | [Covered by `GetBlocks`](get-blocks-serving-contract.md#regulated-load-contract) |
|  |  | ↳ `RangeUnavailable` — terminal response | TBD | [Sending specified](get-blocks-serving-contract.md#serving-model-contract); receiving TBD | [Covered by `GetBlocks`](get-blocks-serving-contract.md#regulated-load-contract) |

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

## Workflow

For every new or changed exchange:

1. Specify the applicable requirements and update this catalog.
2. Implement one ID-named test for each requirement.
3. Exercise the smallest real production path that proves the claim.
4. Run deterministic required coverage before random histories.
5. Preserve useful failures as focused regressions.
6. Confirm harness sensitivity with historical defects or controlled
   mutations.
7. Reproduce remote reachability before assigning operational severity.
8. Mark a layer implemented only after every requirement has evidence.

The first specified example is the
[GetBlocks serving exchange](get-blocks-serving-contract.md).
