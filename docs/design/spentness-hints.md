# Spentness hints

Zakura can use reviewed terminal UTXO membership to omit spent outputs during
checkpoint construction. The ordered writer then rebuilds derived indexes at H
before ordinary commits resume. The default remains off. The compiled public
commitment list remains empty until maintainers review an artifact.

Matched sync benchmarks and public-network rollout remain separate work.
A faster initial pass alone does not establish an end-to-end sync improvement.

## Artifact and trust

The file stores an 86-byte header followed by one bit per transparent output.
Output order follows height, transaction index, and output index. Ordinal zero
includes the genesis coinbase output, whose bit must be zero. A set bit means
the ordinary state retains the output at the terminal checkpoint H.

The header encodes `ZKSHINT\0`, version 1, the serialized genesis hash, H,
the serialized terminal hash, and the output count. Integers use little-endian
encoding. Ordinal n uses bit `n % 8` of byte `n / 8`. Unused high bits must be zero.
The parser rejects trailing bytes and bounds files at 512 MiB before allocation.

`ParsedArtifact` exposes metadata for generation and review. `VerifiedArtifact`
exposes membership only after the complete file matches an expected commitment.
It owns the exact authenticated bytes. Runtime callers select commitments from
the compiled release list. A descriptor supplied by a peer cannot authorize a hint.

The compiled list starts empty because no public artifact has been reviewed.
The release importer adds descriptors and provenance. It keeps older descriptors
and their matching VCT handoff frontiers for incomplete-run recovery. The bitmap never enters source data or the
executable. Checkpoint hashes alone do not authenticate terminal UTXO membership;
maintainers must review the generation evidence before accepting a descriptor.

## Generation and verification

Build both tools:

```sh
cargo build --release -p zakura-utils --features zakura-spentness \
  --bin zakura-checkpoints --bin zakura-spentness
```

`zakura-spentness replay` reads retained blocks from an archive database and
advances a separate ordinary archive state to exactly H. It disables VCT skipping.
It refuses a destination beyond H or on a different chain. It never rolls back
the source. The source must have validated the retained chain; this replay does
not repeat signatures, proof verification, or all contextual consensus checks.

`generate` requires the archive tip to equal H/hash. It merges serialized output
order with the ordered UTXO iterator. It checks complete matched entries and
rejects unmatched UTXOs. It reports output count, survivor count, file size, and hash.
Advancing H regenerates every membership bit, including bits for older outputs.

`verify` builds an independent transparent replay database under `$TMPDIR`, or
under Zakura's cache when `$TMPDIR` is unset. This oracle uses outpoints as keys.
It rejects missing, duplicate, future, and immature coinbase spends. It compares
complete terminal entries with the ordinary state. It then checks each hint bit
and compares salted sums of spent output outpoints and input outpoints modulo
2^256. The oracle does not call the ordinary writer or generator merge helpers.
The source node's full validation remains responsible for signatures, shielded
proofs, and other consensus rules.

```sh
zakura-spentness replay --source /data/archive --destination /data/replay \
  --height "$HEIGHT" --block-hash "$BLOCK_HASH"
zakura-spentness generate --state /data/replay --height "$HEIGHT" \
  --block-hash "$BLOCK_HASH" --output /data/hints.bin \
  --commitment /data/hints.commitment.json
zakura-spentness verify --state /data/replay --artifact /data/hints.bin \
  --commitment /data/hints.commitment.json --report /data/hints.verification.json
```

The publisher also reproduces the artifact from a separately synchronized archive.
Reproducibility and independent transparent replay provide different evidence.
Record the second source's validation software and identity in the bundle.

## Peer protocol and cache

Stream kind 8, version 1, uses capability bit 6. Each request carries the digest,
offset, and requested length. A response carries availability status, digest,
offset, length, and at most 256 KiB. The capability advertises protocol support.
Status values distinguish absent (0), available (1), busy (2), insufficient
response capacity (3), and a range outside the artifact (4). Negative responses
echo the digest and offset with length zero. These statuses do not penalize peers.
The discovery service advertises availability only when startup loaded a verified
artifact. Nodes can serve newly verified bytes immediately; they advertise those
new cache entries after restart.

The server admits at most four concurrent range preparations. Each successful
preparation holds its slot for 250 ms, limiting aggregate data to 4 MiB/s.
The transport also applies its stream, frame, message, and connection limits.
Unavailable or busy servers return availability failure without a peer penalty.

The downloader selects peers that negotiated the capability. It tries at most
three sources per round, one source at a time. It rotates sources between rounds.
It reduces the requested range when the peer reports insufficient response capacity.
It retains interrupted progress under names
that bind the expected digest and peer identity. It checks every returned range
and verifies the complete file before durable cache publication. A whole-file
mismatch discards that source's partial file. It does not attribute a whole-file
mismatch to an individual chunk or disconnect the peer.

Enable artifact distribution explicitly:

```toml
[spentness]
cache_dir = "/data/zakura/spentness"
```

The distribution task waits up to 60 seconds for a capable peer, then makes bounded
acquisition attempts. It retries missing artifacts after a 60-second pause until
acquisition succeeds or the endpoint shuts down. Each source has a ten-minute
deadline. An unavailable artifact does not block ordinary sync when hinted
construction is disabled. Pre-state acquisition rotates peer cohorts for up to
one hour. Auto mode then falls back to ordinary sync; Require mode reports failure.
Supported historical cache entries also remain available for serving.

For offline or seed provisioning, use a binary that contains the reviewed pin:

```sh
zakura-spentness install --artifact /data/hints.bin --cache /data/zakura/spentness
```

Cache names use the SHA-256 digest followed by `.bin`. Startup always reverifies
cache bytes. Seed operators retain supported artifacts and load them before
rolling out a release that selects their commitments. Nodes fetch artifacts from
peers; the HTTPS bundle publisher serves release automation, not node acquisition.

## Construction and recovery

For a release with a reviewed commitment, select an explicit construction policy:

```toml
[spentness]
mode = "require" # "off" (default), "auto", or "require"
# artifact_file = "/data/hints.bin"
```

Construction requires checkpoint sync and VCT fast sync. `auto` can use ordinary
sync if the empty database cannot start with compatible hints. `require` reports
the failure. Neither policy starts hints midway through an ordinary database.
An explicit file supplies bytes only; it cannot authorize an unrecognized digest.

Before opening writable state, the node verifies a local artifact or starts a
temporary peer endpoint to acquire the selected artifact. The node shuts down
that endpoint before starting its normal endpoint. It uses the configured serving
cache, or `<state.cache_dir>/spentness`, and keeps a durable recovery copy in the
state cache. Nodes never download bitmap bytes from HTTP.

The writer persists one versioned record with each atomic block batch:

```text
Applying { commitment, height, block_hash, next_ordinal, survivor_value }
  -> Rebuilding { commitment, indexed_height, replay_accounting, survivor_value }
  -> Complete { commitment, rollback_floor }
```

Applying consumes every output bit, including genesis and non-address scripts.
It inserts terminal survivors without resolving or deleting spent input UTXOs.
It retains raw transactions and defers address indexes. It preserves shielded
and deferred accounting. The transparent balance at this stage describes the
survivors created so far, so construction gates block monetary consumers.

At H, the writer checks the exact hash, output count, and VCT handoff frontiers.
It then holds the consensus tip at H while it replays retained transactions.
The replay uses transaction-location indexes and caches at most 64 creating
transactions. It restores address balances, received totals, first-receive
locations, address UTXOs, transaction indexes, and historical value pools.
Each replay block commits its index updates and cursor in one batch.

The final audit compares complete survivor entries with retained-body replay.
It also checks address ownership, address balances, output count, and terminal
pool values. The replay and audit never change live consensus UTXOs. A mismatch
stops the writer without attributing the failure to a peer.

State access gates block pending-UTXO responses, monetary RPCs, mempool checks,
mining checks, and ordinary semantic admission during construction. Header
control messages continue during replay and the final audit. The legacy syncer
waits before starting verifier deadlines. The native stall watchdog pauses
during rebuilding. Completion lifts the gates and releases retained history
to the existing pruning backlog.

Startup resumes Applying with its original recognized commitment, even when a
new release selects a later commitment. If the artifact is missing, restore
identical bytes in the reported cache path or set `spentness.artifact_file`.
Rebuilding needs retained bodies but no bitmap. Startup finishes that replay
before exposing state. Completed databases need no bitmap for later operation.
Unknown or revoked commitments stop startup with a compatibility error.

Database format 29 gives this construction a separate major-version directory.
The existing upgrade mechanism reuses ordinary format-28 data. Older binaries
do not open format-29 data as ordinary state. Incomplete runs also require the
same database format and indexer feature when they resume.

Read-only opens, exports, offline pruning, and offline rollback reject incomplete
state. After completion, rollback cannot cross H or a stricter VCT boundary.
To diagnose a cursor without opening monetary state, stop the node and run:

```sh
zakura-spentness audit-progress --state /data/zakura
```

The audit re-enumerates retained transactions and verifies their header Merkle
roots. It prints recorded and enumerated output counts. A mismatch returns an
error without changing the record.

`state.spentness.construction_height` and `state.spentness.rebuilt_height` report
the two passes. `SpentnessStatus` and `state.spentness.usable` report consumer
availability. The writer suppresses provisional value-pool metrics until completion. The
`state.spentness.utxo.*` counters describe initial-pass reads, inserts, deletes,
and omissions. Initial-pass reads and deletes remain zero. The preparation,
commit, and rebuild-audit histograms separate those costs. Benchmark the entire
path through the first ordinary commit above H before claiming a speedup.

## Release-state schema 2

`zakura-checkpoints --mainnet-spentness-output` couples generation to its selected
checkpoint. Supply `--spentness-replay-cache` with the three treestate outputs.
`ZAKURA_SPENTNESS_BIN` selects the installed helper. The exporter writes commitment
and verification JSON sidecars beside the hint. It emits checkpoint stdout only
after generation and verification succeed. Each helper has a 48-hour deadline.

Schema 2 requires the hint, commitment, verification report, and frontier grid.
The publisher uploads data before metadata and moves `latest.json` last.
The importer checks the descriptor, format, digest, counts, genesis, shared H/hash,
and provenance before generating Rust commitments. It retains each matching VCT
frontier under the artifact digest and verifies retained frontier hashes on later
imports. It never imports bitmap bytes.
Version 2 bundles have no automatic newest-N deletion policy. Retention must cover
all supported incomplete hinted runs.

Deploy the fetcher/importer before enabling the version 2 publisher. Configure
`RELEASE_STATE_ORACLE_SOURCE`, `RELEASE_STATE_ORACLE_ID`, and
`RELEASE_STATE_GENERATOR_REVISION`. `RELEASE_STATE_DATA_DIR` holds both retained
replay states. Budget archive-state disk space and full-history replay time.
Publication fails before moving the pointer if generation or verification fails.

Before rollout, provision each seed with
`deploy/release-state/provision-spentness-seed.sh`, configure its cache, and restart
it. Test a cold client against only those seeds and confirm the verified digest in
its log. The loopback integration test exercises this same peer acquisition path.
