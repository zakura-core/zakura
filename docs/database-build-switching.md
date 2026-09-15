# Switching builds and database formats

Zakura selects its state database by database kind, major format version, and network:

```text
<state.cache_dir>/state/v<major>/<network>/
```

A release tag identifies software. The database format version identifies storage compatibility.
Builds with incompatible storage formats must use different major format versions, even when
they share a release tag. Compatible pool additions can use the existing upgrade mechanism.

## Trying a build with a new major format

Zakura preserves the previous database by default. The new build syncs its own database when
its database directory does not exist. Switching back lets the previous build resume from
its preserved database. Each copy requires disk space and catches up independently.

For major upgrades that support reuse, an operator can instead set:

```toml
[state]
reuse_previous_database = true
```

This setting permits Zakura to **move** the previous database into the new format directory.
It does not create a backup. Switching back can then require a backup restore or a resync.
Zakura logs the source and destination before moving the database.

Cleanup preserves the immediately preceding major format and databases that remain candidates
for a supported major upgrade. Set `state.delete_old_database = false` to retain other old
databases too.

## Switching back

Use the previous build's own database directory. Do not rename a newer database into an older
format directory or edit its version file. Zakura rejects a recorded major format newer than
the running build before opening RocksDB, including read-only opens.

This check only protects builds that include it. Previously released binaries cannot learn a
new check from metadata. Keeping the previous database in its original directory protects
that workflow without requiring the old binary to understand new metadata.

Non-finalized block backups also use the database major format:

```text
<state.cache_dir>/non_finalized_state/v<major>/<network>/
```

Zakura leaves legacy backups at `non_finalized_state/<network>/` untouched. This build does not
restore them. The node fetches any missing recent blocks from peers.

## Relationship to writer metadata and Tachyon

[PR #415](https://github.com/zakura-core/zakura/pull/415) records the last software that opened
the database writable. This record helps explain a database's history. It does not establish
compatibility, and software without that feature can leave the record stale.

[PR #795](https://github.com/zakura-core/zakura/pull/795) selects major format 29 under
`zcash_unstable="nutachyon"` and format 28 otherwise. Once #795 incorporates this policy,
the existing versioned directories separate those builds without a Tachyon-specific path rule.
The backup path uses the same format selection.

Before releasing #795, test ordinary-to-Tachyon-to-ordinary switching with real blocks in both
builds. Test explicit reuse separately. Reserve incompatible major formats across release
branches so two builds never assign the same major version to incompatible layouts.
