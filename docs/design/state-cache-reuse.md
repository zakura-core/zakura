# State cache reuse

Startup selects the newest usable older major version connected to the current
format by restorable upgrades. It does not rank caches by chain height: an empty
newer cache can intentionally supersede an older one. Malformed version markers
and structurally invalid candidates are logged and skipped. If existing candidates
cannot be reused, startup reports an error rather than silently creating empty state.
An existing current-major database remains authoritative.

Reuse opens the selected source exclusively and creates a RocksDB checkpoint under
the destination-major directory. The checkpoint receives the full source format
version before publication, including versions left by interrupted upgrades.
Migrations run only on the destination, and failed migrations remain retryable.
A locked source or checkpoint failure stops startup; it does not select an older
cache. A per-network startup lock serializes publication with writable opens.

The source database and sibling network caches remain in place. On the same
filesystem the checkpoint shares immutable SST files through hard links, but
migration and later compaction can consume additional space. Restorable source
caches remain protected by the existing old-cache cleanup policy. Interrupted
`.reuse-*` staging directories are not database candidates; they can be removed
manually while nodes using that cache directory are stopped.

Ephemeral databases skip cache discovery. Each open owns one temporary directory,
and version reads and writes use that directory until it is deleted on drop.
