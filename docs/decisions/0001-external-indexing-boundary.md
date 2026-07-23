---
status: accepted
date: 2026-07-23
---

# Keep External Indexing Outside the Node

## Context and Problem Statement

Zakura previously offered an experimental `elasticsearch` Cargo feature that
sent finalized blocks to Elasticsearch from the state commit path. This coupled
a validator's availability and dependency surface to a particular external
database client.

Indexing is valuable, but embedding a vendor-specific exporter in the node is
the wrong hardening boundary. External service latency, availability,
authentication, TLS policy, backpressure, and API changes must not affect
consensus-critical block validation or state commits.

## Decision Outcome

Zakura does not compile storage-vendor clients into the node. Indexers run as
separate processes and consume a vendor-neutral indexing API.

An indexing API should let an external consumer:

- discover the active network and stable chain identity;
- read the finalized tip and retrieve historical blocks for catch-up;
- resume from a durable height, hash, or opaque cursor;
- subscribe to ordered finalized and non-finalized chain updates;
- detect reorgs, including removed and replacement blocks;
- retrieve raw, consensus-encoded data without adopting Zakura's internal
  database schema;
- detect gaps and recover by replaying an explicit range;
- apply bounded backpressure without delaying validation or state commits; and
- authenticate and encrypt connections independently of storage credentials.

The API should make delivery and ordering guarantees explicit. Consumers own
their durable progress, retries, schema, transformations, and integration with
Elasticsearch or any other storage system.

Existing indexer RPCs are a starting point, not a commitment that their current
surface provides all of these guarantees. We will extend a general indexing API
in response to concrete consumer requirements rather than add another
storage-specific integration to the node.

## Expected Consequences

- Node builds have a smaller dependency and configuration surface.
- External database outages cannot directly block or terminate state commits.
- Indexers can evolve and deploy independently from Zakura.
- Operators using the removed experimental Elasticsearch feature need an
  external replacement.

If you use the removed feature, please contact the Zakura maintainers. We will
prioritize a better indexing API around your requirements.
