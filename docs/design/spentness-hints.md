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
Applying { commitment, height, block_hash, next_ordinal, omitted_outputs }
  -> Rebuilding { commitment, indexed_height, transparent_value, unspent_omitted }
  -> Complete { commitment, rollback_floor }
```

Applying consumes every output bit, including genesis and non-address scripts.
It inserts terminal survivors without resolving or deleting spent input UTXOs.
It writes every other output after genesis to the temporary
`spentness_omitted_outputs` column family, keyed by output location. This column
family holds spent outputs until the rebuild, so construction needs extra disk
space for them. Applying retains raw transactions and defers address indexes.
It preserves shielded and deferred accounting. The transparent balance at this
stage describes the survivors created so far, so construction gates block
monetary consumers.

Construction cannot compute the NSM value balance or ZIP 234 issuance, because
both depend on spent output values. Ordinary state seeds the NSM balance at the
block before NU7 activation. The release authority therefore rejects a commitment
when H + 1 reaches NU7 activation on its network. The NSM leg stays zero through
H, as it does in ordinary state, and the first ordinary block after H seeds it
from exact pools.

At H, the writer checks the exact hash, output count, and VCT handoff frontiers.
It then holds the consensus tip at H while it replays retained transactions.
Each replay step takes a window of up to 1,000 blocks or 64 MiB of serialized
blocks. The step deserializes the window's bodies and resolves its spends in
parallel. Each spend reads the creating transaction's location and then the
omitted output at that location. A serial pass restores address balances,
received totals, first-receive locations, address UTXOs, transaction indexes,
and historical value pools. The replay copies every pool except transparent from
the saved block accounting. One batch commits the window's index updates, the
deletion of each consumed omitted output, and the cursor.

The replay rejects a spend of a retained output, a missing or consumed output,
an output that follows its spend, a genesis output, an immature or disallowed
coinbase spend, and a transaction whose outputs exceed its inputs. Each spend
therefore consumes a distinct omitted output. At H, the final audit requires
that no omitted output remains. The omitted outputs are then exactly the spent
outputs, so the survivors are exactly the terminal UTXO set. The audit also
requires the replayed transparent pool to equal the survivors' value.

The final audit then scans the address UTXO index once, in key order. Each
entry must name a survivor with an address. Entries that share an address
location must share an address. That address's balance row must name the same
location and hold the entries' total. The index must hold one entry per survivor
with an address, and every nonzero balance must belong to an indexed address.
The audit writes nothing, so a restart repeats it from the beginning.

The replay and audit never change live consensus UTXOs. A mismatch stops the
writer without attributing the failure to a peer. A restart repeats the same
failure, so the error tells the operator to delete the state and resync with
hints off.

State access gates block pending-UTXO responses, monetary RPCs, mempool checks,
mining checks, and ordinary semantic admission during construction. Every state
request variant is classified as allowed or denied, so a new variant needs a
review decision. Synced blocks wait for completion before semantic verification.
Block proposals and `submitblock` fail immediately. `getblockchaininfo` reports
the real tip and omits `chainSupply` and `valuePools`. Header control messages
continue between replay steps and audit chunks. The legacy syncer waits before
starting verifier deadlines. The native stall watchdog pauses during rebuilding.
Completion lifts the gates. In pruned mode, the completion batch also deletes raw
transactions below the retention window at H. Online pruning continues from that
marker.

Startup resumes Applying with its original recognized commitment, even when a
new release selects a later commitment. If the artifact is missing, restore
identical bytes in the reported cache path or set `spentness.artifact_file`.
Rebuilding needs retained bodies and omitted outputs but no bitmap. Startup
finishes that replay before exposing state. Shutdown interrupts the replay, and a
restart resumes it. Completed databases need no bitmap and no recognized
commitment. Startup checks only their chain identity and revocation. Unknown or
revoked commitments stop incomplete runs with a compatibility error.

Database format 30.0.0 gives this construction a separate major-version
directory. The existing upgrade mechanism reuses ordinary format-29 data. Older
binaries do not open format-30 data as ordinary state. Incomplete runs also
require the same database format and indexer feature when they resume.

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
availability. The writer suppresses provisional value-pool metrics until completion.
The `state.spentness.utxo.inserts` and `state.spentness.utxo.omitted` counters
describe the initial pass. The preparation, commit, and rebuild-audit histograms
separate those costs. The rebuild repeats the spent-output reads that the initial
pass skipped. Benchmark the entire path through the first ordinary commit above H
before claiming a speedup.

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
