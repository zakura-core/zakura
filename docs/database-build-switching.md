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

Zakura checks the source's recorded major format before moving it. If the source format does
not match its directory, Zakura skips reuse and logs a warning. Zakura still accepts legacy
minor-only version files and infers the major format from the directory when no version file
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
`zcash_unstable="nutachyon"` and format 28 otherwise. Once #795 incorporates this policy,
the existing versioned directories separate those builds without a Tachyon-specific path rule.

Before releasing #795, test an ordinary-to-Tachyon upgrade with real blocks and existing
non-finalized backups. Verify that a build with the format check rejects a database requiring
a newer major format. Reserve incompatible major formats across release branches so two
builds never assign the same major version to incompatible layouts.
