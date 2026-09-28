# Spentness hints

Zakura can use reviewed terminal UTXO membership to omit spent outputs during
checkpoint construction. The ordinary writer never inserts, reads, or deletes a
UTXO row for an output that dies before H. It still writes every index, balance,
and value pool, so the state at H equals ordinary state. The default remains off. The compiled public
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

`verify` makes one pass over the canonical blocks. In that pass, an independent
transparent replay oracle builds its own UTXO database under `$TMPDIR`, or under
Zakura's cache when `$TMPDIR` is unset. This oracle uses outpoints as keys. It
rejects missing, duplicate, future, and immature coinbase spends. The same pass
reads the ordinary state at each output location. Each bit must equal the
output's presence there, and each present entry must equal its creating output.
After the pass, each oracle survivor must have a set bit. The oracle, the set
bits, and the ordinary state must hold the same number of UTXOs. The oracle does
not call the ordinary writer or generator merge helpers. The source node's full
validation remains responsible for signatures, shielded proofs, and other
consensus rules.

`replay`, `generate`, and `verify` read and deserialize blocks on worker threads.
Each block must match the canonical hash at its height. Its transactions must
match the header's Merkle root and contain no duplicates.

```sh
zakura-spentness replay --source /data/archive --destination /data/replay \
  --height "$HEIGHT" --block-hash "$BLOCK_HASH"
zakura-spentness generate --state /data/replay --height "$HEIGHT" \
  --block-hash "$BLOCK_HASH" --output /data/hints.bin \
  --commitment /data/hints.commitment.json
zakura-spentness verify --state /data/replay --artifact /data/hints.bin \
  --commitment /data/hints.commitment.json --report /data/hints.verification.json
```

The publisher runs two pipelines concurrently. The primary pipeline replays the
publisher's archive and generates the published artifact. The independent pipeline
replays a separately synchronized archive, generates the artifact again, and
verifies that reproduction with `--report`. The publisher requires byte-identical
artifacts and commitments. Reproducibility and independent transparent replay
provide different evidence. Record the second source's validation software and
identity in the bundle.

The primary pipeline does not run `verify`. Identical bytes make the independent
verification cover the published artifact. Both archives hold the same canonical
blocks, because every pass checks each block's hash and Merkle root. Primary
generation requires the primary UTXO set to equal the set bits. The primary
UTXO set therefore equals the membership that the oracle verified.

## Peer protocol and cache

Stream kind 8, version 1, uses capability bit 6. Each request carries the digest,
offset, and requested length. A response carries availability status, digest,
offset, length, and at most 256 KiB. The capability advertises protocol support.
Status values distinguish absent (0), available (1), busy (2), insufficient
response capacity (3), and a range outside the artifact (4). Negative responses
echo the digest and offset with length zero. These statuses do not penalize peers.
The stream declares its message table, whose only response ends the exchange.
The requester therefore admits exactly one response. It closes the connection on
a second or missing response.

Distribution uses only supported commitments: release commitments minus
`REVOKED_COMMITMENTS`. A node never downloads, serves, or advertises a revoked
artifact. The discovery service advertises availability only when startup loaded
a verified artifact. It seeks the service only when startup found a supported
artifact missing. Nodes can serve newly verified bytes immediately; they advertise
those new cache entries after restart.

The server bounds serving with two byte token buckets. The node bucket allows
8 MiB/s across all peers. Each peer bucket allows 2 MiB/s, so one peer takes at
most a quarter of the node rate. Each bucket holds one second of its rate. A
request that either bucket cannot cover gets an immediate busy reply. The server
never delays a reply, so an idle server answers at transport speed.
The transport also applies its stream, frame, message, and connection limits.

The downloader selects peers that negotiated the capability. It tries one source
at a time and rotates sources between rounds. At most three sources per round may
transfer bytes. A source that replies absent before it transfers bytes does not
count. A busy reply pauses the same source for 125 ms to 2 s, then retries the same
range. The downloader reduces the requested range when the peer reports
insufficient response capacity.

The downloader retains interrupted progress under names that bind the expected
digest and peer identity. It keeps at most three partial files per digest and
deletes the shortest ones first. It checks every returned range and verifies the
complete file before durable cache publication. Success deletes every partial file
for that digest. Startup deletes partial files for digests that are held or
unsupported. A whole-file mismatch, a format error, or a truncated file discards
that source's partial file. A local read error keeps it. The downloader does not
attribute a whole-file mismatch to an individual chunk or disconnect the peer.

Enable artifact distribution explicitly:

```toml
[spentness]
cache_dir = "/data/zakura/spentness"
```

The distribution task waits up to 60 seconds for a capable peer, then makes bounded
acquisition attempts. It retries missing artifacts after a 60-second pause until
acquisition succeeds or the endpoint shuts down. The first failure for each
artifact logs a warning; later failures log at debug level. Each source has a
ten-minute deadline. An unavailable artifact does not block ordinary sync when hinted
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

Before opening writable state, the node checks the database that state opening
will use. That includes a previous major version that the upgrade will reuse. Only
an empty database or an interrupted hinted run needs an artifact. The node then
verifies a local artifact or starts a temporary peer endpoint to acquire the
selected artifact. The node shuts down that endpoint before starting its normal
endpoint. A slow endpoint shutdown does not discard an acquired artifact. Shutdown
during acquisition stops the node instead of falling back to ordinary sync. The
node uses the configured serving cache, or `<state.cache_dir>/spentness`, and keeps
a durable recovery copy in the state cache. Nodes never download bitmap bytes
from HTTP.

The writer persists one versioned record with each atomic block batch:

```text
Applying { commitment, height, block_hash, next_ordinal, omitted_outputs, resolved_spends }
  -> Complete { commitment, rollback_floor }
```

Construction uses the ordinary block writer. For each block, the writer reads the
bit of every created output, including genesis and non-address outputs. A set bit
marks a survivor, which the writer inserts into the UTXO set and the address UTXO
index. A clear bit marks an omitted output. The writer writes no UTXO row and no
address UTXO row for an omitted output. It still writes the output's address
transaction index entry, balance, received total, and value pool change.

The writer resolves each spend without the UTXO set:

1. A spend of an output in the same block resolves from that block.
2. Otherwise it resolves from an in-memory map of omitted outputs that no block
   has spent yet. A hit removes the entry.
3. Otherwise it reads the creating transaction's location and the omitted-output
   journal row at that height.

Each map entry and journal record holds the output's location, value, coinbase
flag, and P2PKH or P2SH hash. It holds no script. The writer rebuilds the standard
script for the address, because it reads only the address from a spent output's
script. The map evicts its oldest 1,000-height buckets when it exceeds 10 million
entries. The journal is the `spentness_omitted_outputs` column family. Each block
writes one row, keyed by height, with its omitted outputs that it does not spend
itself. A miss after an eviction or a restart reads that row. Correctness never
depends on the map; it only saves reads.

Every spent output feeds the ordinary index and pool code. Construction therefore
keeps exact address balances, received totals, first-receive locations, address
transaction indexes, spending transaction indexes, and chain value pools at every
height. That includes the NSM value balance and ZIP 234 issuance, so H may reach
or pass NU7 activation. The writer skips spent-output deletes, because every
output that a block spends before H is omitted.

Only omitted outputs enter the map and the journal, so each resolved spend proves
that the artifact omits its output. A spend that resolves nowhere, including a
spend of a survivor, stops construction. The record counts omitted outputs and
resolved spends. At H, the writer checks the exact hash, output count, and VCT
handoff frontiers, and requires the two counts to be equal. Checkpoint sync
already trusts the chain through H, so no output is spent twice. Equal counts
then mean the omitted outputs are exactly the spent outputs, and the survivors
are exactly the UTXO set at H. The terminal batch writes Complete and deletes the
journal.

A mismatch stops the writer without attributing the failure to a peer. A
restart repeats the same failure, so the error tells the operator to delete the
state and resync with hints off.

The ordinary retention plan applies during construction. Pruned mode therefore
deletes raw transactions below the retention window as construction advances.

State access gates the UTXO set during construction: pending-UTXO responses,
UTXO and address UTXO reads, spent-output checks, mempool checks, mining checks,
and ordinary semantic admission. Balances, address transaction indexes, value
pools, block info, and chain info stay available, because they are exact. Every
state request variant is classified as allowed or denied, so a new variant needs
a review decision. Synced blocks wait for completion before semantic
verification. Block proposals and `submitblock` fail immediately. Completion
lifts the gates.

Startup resumes Applying with its original recognized commitment, even when a
new release selects a later commitment. If the artifact is missing, restore
identical bytes in the reported cache path or set `spentness.artifact_file`.
Completed databases need no bitmap and no recognized commitment. Startup checks
only their chain identity and revocation. Unknown or revoked commitments stop
incomplete runs with a compatibility error.

Database format 30.0.0 gives this construction a separate major-version
directory. The existing upgrade mechanism reuses ordinary format-29 data. Older
binaries do not open format-30 data as ordinary state. Incomplete runs also
require the same database format and indexer feature when they resume.

Read-only opens, exports, offline pruning, and offline rollback reject incomplete
state. After completion, rollback cannot cross H or a stricter VCT boundary.

`state.spentness.construction_height` reports construction progress.
`SpentnessStatus` and `state.spentness.usable` report UTXO-set availability.
`state.spentness.utxo.omitted` counts omitted outputs. `state.spentness.spends.hits`
and `state.spentness.spends.misses` count spends that resolved from the map and from
the journal. `state.spentness.live.entries` and `state.spentness.live.evicted`
report the map. Benchmark the entire path through the first ordinary commit
above H before claiming a speedup.

## Release-state schema 2

The publisher runs `zakura-checkpoints` first and reads H/hash from the last line
of its checkpoint list. It then runs both spentness pipelines at H.
`ZAKURA_SPENTNESS_BIN` selects the installed helper.
`RELEASE_STATE_SPENTNESS_TIMEOUT` bounds each pipeline and defaults to 48 hours.
The helpers keep verification scratch under `$RELEASE_STATE_DATA_DIR/tmp`. The
publisher empties that directory before each run and removes it afterwards.
Publication fails before any upload if either pipeline fails or the artifacts differ.

`generated_at` records when the exporter selected H. Pipeline time therefore
counts against the fetcher's 48-hour freshness window. Both replays resume from
their previous H, so a daily run replays only new blocks. The primary pipeline
then reads the chain once, and the independent pipeline reads it twice. The two
pipelines run concurrently. The first run replays both archives from genesis and
can produce a bundle that is already too old to import.

Schema 2 requires the hint, commitment, verification report, and frontier grid.
The publisher uploads data before metadata and moves `latest.json` last.
The importer checks the descriptor, format, digest, counts, genesis, shared H/hash,
and provenance before generating Rust commitments. It retains each matching VCT
frontier under the artifact digest and verifies retained frontier hashes on later
imports. It never imports bitmap bytes.
The verification report in the bundle comes from the independent pipeline. After
a spentness descriptor is committed, the fetcher and importer reject schema 1
bundles, which carry no descriptor.
Version 2 bundles have no automatic newest-N deletion policy. Retention must cover
all supported incomplete hinted runs.

Deploy the fetcher/importer before enabling the version 2 publisher. Configure
`RELEASE_STATE_ORACLE_SOURCE`, `RELEASE_STATE_ORACLE_ID`, and
`RELEASE_STATE_GENERATOR_REVISION`. `RELEASE_STATE_DATA_DIR` holds both retained
replay states. Budget archive-state disk space and full-history replay time.
The archive deployment installs `zakura-checkpoints` and `zakura-spentness` at the
revision recorded in `EXPORTER_REVISION`.

Before rollout, provision each seed with
`deploy/release-state/provision-spentness-seed.sh`, configure its cache, and restart
it. Test a cold client against only those seeds and confirm the verified digest in
its log. The loopback integration test exercises this same peer acquisition path.
