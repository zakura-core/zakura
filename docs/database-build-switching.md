# Upgrading database formats

Zakura selects its state database by database kind, major format version, and network:

```text
<state.cache_dir>/state/v<major>/<network>/
```

A release tag identifies software. The database format version identifies storage compatibility.
Builds with incompatible storage formats must use different major format versions, even when
they share a release tag. Compatible pool additions can use the existing upgrade mechanism.

## Upgrading to a new major format

Zakura automatically reuses the preceding database when the registered major upgrade supports
reuse and no database exists at the destination. It moves the database into the new format
directory and applies the registered upgrades. It does not retain a fallback copy.

Zakura checks the source's recorded major format before moving it. It preserves an interrupted
upgrade when every intervening major upgrade supports reuse. Otherwise, it skips reuse and
logs a warning. Zakura still accepts legacy minor-only version files and infers the major
format from the directory when no version file
exists. The existing cleanup and
non-finalized backup policies still apply.

## Unsupported older builds

Downgrading an upgraded database is not a supported workflow. Continue using a compatible
build after upgrading. Do not rename the database into an older format directory or edit its
version file. Zakura rejects a recorded major format newer than the running build before
opening RocksDB, including read-only opens.

This check only protects builds that include it. Previously released binaries cannot learn a
new check from metadata. An older binary may start a fresh sync if its old database directory
no longer exists. The existing handling of compatible minor-version differences remains in
place; this change adds no downgrade migration.

## Relationship to writer metadata and Tachyon

[PR #415](https://github.com/zakura-core/zakura/pull/415) records the last software that opened
the database writable. This record helps explain a database's history. It does not establish
compatibility, and software without that feature can leave the record stale.

[PR #795](https://github.com/zakura-core/zakura/pull/795) selects major format 29 under
`zcash_unstable="nutachyon"` and format 28 otherwise. The existing versioned directories
separate these builds without a Tachyon-specific path rule.

## Reserving the Tachyon format number

[PR #1028](https://github.com/zakura-core/zakura/pull/1028) reserves major version 29 on
`main`. Its compile-time assertion rejects an ordinary build that selects that number.
The reservation prevents two incompatible formats from sharing the same version.

When integrating Tachyon, compile that assertion only when
`zcash_unstable="nutachyon"` is absent. Tachyon intentionally selects the reserved version.
An unconditional assertion would reject the Tachyon build before the node can start.
The Tachyon constant uses the reservation constant so both refer to the same number.

| Build | Selected major | Compiler result |
| --- | --- | --- |
| Ordinary | 28 | Accept |
| Ordinary with an accidental version collision | 29 | Reject |
| Tachyon | 29 | Accept |

The reservation controls which build may use a format number. The startup checks control
whether a build may open an existing database. Both checks remain necessary.

## Building and testing Tachyon

Ordinary builds leave Tachyon disabled. Enable Tachyon across Zakura and its dependencies:

```sh
RUSTFLAGS='--cfg zcash_unstable="nutachyon"' cargo build --locked -p zakura
```

This is a build-wide Rust configuration flag. A Cargo feature on Zakura alone would not
configure the Tachyon types in its dependencies. If you supply other Rust flags, include
the Tachyon flag in the same `RUSTFLAGS` value.

Run the cross-build upgrade test:

```sh
scripts/test-tachyon-db-upgrade.sh
```

The script creates a temporary v28 database with an ordinary build. It commits real Mainnet
blocks and writes a non-finalized backup. The Tachyon build then upgrades that database to
v29, reads the backup, commits another block, and reopens the database. The test also
checks writer metadata and exchanges a serialized history snapshot between the builds.
CI runs this script and the state tests with Tachyon disabled and enabled.

Downgrading remains unsupported. The format-guard tests verify that startup rejects a
newer recorded major format in both writable and read-only modes. Release branches must
reserve distinct major versions for incompatible layouts.
