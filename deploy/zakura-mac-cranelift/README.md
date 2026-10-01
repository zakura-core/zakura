# zakura-mac-cranelift

A native ARM64 Mac runs Zakura compiled with Cranelift. The existing fleet
watchdog compares its committed mainnet results against a Linux reference.
Rust performs consensus and cryptographic verification; Python observes the
results and reports disagreement.

Imported finalized snapshot history is trusted. Comparison coverage starts at
the recorded bootstrap height plus one. Matching results do not independently
audit imported UTXOs/nullifiers or eliminate bugs shared by both nodes.

## Components

| Component | Responsibility |
| --- | --- |
| `ssh_probe.py` | Bounded read-only Mac probe invoked over SSH; no daemon or listener |
| `comparison.py` | Bounded comparison invoked by the fleet watchdog |
| `../runner/mac_cranelift_status.py` | Shared field filter and atomic public-file writer; no service |
| `common.py` | Bounded RPC, canonical records and atomic state writes |
| `rotate_logs.py` | Bound Mac launchd diagnostic logs |
| `cranelift/qualify.py` | Build and test compiler acceptance candidates in CI |

The US reference host opens one SSH session per comparison cycle. A dedicated
monitoring key is restricted to the root-owned read-only probe: no interactive
shell, arbitrary command or port forwarding. The private key is generated on
Linux and never leaves it. Only its public half is installed on the Mac. The probe
uses the Mac node's existing loopback RPC on port `28232`; Linux RPC uses loopback
`8232`. The adapter HTTP service and reverse tunnel are retired.

Private connection settings and pinned host keys live under
`/etc/zakura-mac-verifier/ssh`, readable only by the monitoring account/root.
The dashboard reads the watchdog's sanitized status file; it never receives the
SSH destination or adds it to public inventory.
Deployment credentials stay in the private CI environment, and are not copied to
the reference host.

## Comparison and alerts

The fleet watchdog invokes `comparison.py once` after its existing fleet checks.
Each invocation compares at most 16 successive heights, three blocks behind the
shorter tip, using block hashes and canonical Sapling, Orchard and Ironwood
commitment-tree roots and frontiers. It re-reads mismatches to distinguish stable
differences from racing reads. A durable cursor prevents silently skipping blocks
across restarts; recent history supports rewinding up to 1,000 blocks. Reorg
search progress is saved across bounded invocations. Missing historical data
cannot establish agreement, and an exhausted reorg window records a coverage gap.

Network requests share a ten-second budget and individual two-second timeouts.
The watchdog kills the entire comparison/SSH process group after fifteen seconds.
The remote probe also has a fifteen-second lifetime and bounded request count. Other fleet
checks run first. Comparison outcomes are matching, catching up, unavailable,
chain disagreement, confirmed tree mismatch and coverage gap.

The watchdog owns notification delivery, suppression and recovery. Confirmed tree
mismatches are immediately actionable; other failures have a three-minute grace
period. Matching results recover through the existing alert lifecycle. Private
mismatch evidence remains after recovery. There is no separate notification
queue, alert-enablement script or 24-hour qualification state.

The lane is enabled with `ZAKURA_MAC_CRANELIFT_COMPARISON=1` on the existing watchdog.
Notifications remain muted by default; `ZAKURA_MAC_CRANELIFT_COMPARISON_ALERTS=1` enables
this lane through the existing watchdog channel. Monitoring deployment preserves
muted operation and does not replay historical messages.

The watchdog only publishes approved status fields. The dashboard distinguishes
comparison failure from node availability. Private receipts, diagnostics, host
addresses and peer identities never enter the public status file.

## Names and deployment compatibility

The component is named `zakura-mac-cranelift`. Its CI entry points are
`zakura-mac-cranelift.yml` (tests), `build-zakura-mac-cranelift.yml` (native
compiler acceptance), and the existing `zakura-mainnet-deploy.yml` (deployment).
The deployment helper is `deploy/deployer/zakura-mac-cranelift-manager.py`.

The installed node ID `zakura-mac-os`, `/Library/Application Support/ZakuraVerifier`,
`dev.valargroup.zakura-verifier-*` launchd labels, `zakura-mac-verifier` Linux
account/services/directories and SSH alias `mac-verifier` remain compatibility
bindings. Renaming source files does not move node data, reset comparison history,
or require a service restart. The private CI environment `mac-verifier-private`
and its encrypted `MAC_VERIFIER_*` secrets remain in place; CI maps them to
`ZAKURA_MAC_CRANELIFT_*` process variables. The existing deployment-branch variable
and PR branch remain unchanged. Historical workflow paths and candidate artifact
names remain accepted for reuse of already qualified binaries.

Dashboard and watchdog settings use the `ZAKURA_MAC_CRANELIFT_` prefix. Existing
`ZAKURA_PRIVATE_VERIFIER_STATUS`, `MAC_VERIFIER_ALERTS_MUTED`,
`ZAKURA_MAC_COMPARISON` and `ZAKURA_MAC_COMPARISON_ALERTS` settings are accepted
when their replacement is unset. Opaque receipt IDs and serialized status fields
remain stable so existing history and dashboard consumers continue to work.

## Monitoring deployment

Use `node=zakura-mac-os` and `mac_operation=dashboard` to deploy the watchdog,
comparison client, field-filter library and dashboard together. The watchdog
atomically writes `/var/lib/zakura-mac-cranelift-public/status.json`; the dashboard
reads that bounded, sanitized file. Raw comparison state and connection settings
remain in their private directories. There is no dashboard bridge service.

Deployment checks a fresh matching sample and a healthy dashboard row before
disabling the former bridge. Failure restores the prior scripts and services
without rewinding comparison history. The Mac node is not restarted. Completed
adapter/tunnel and cursor-schema migrations are no longer supported operations.

## Binary deployment

Use the existing mainnet deployment's `ref` input for a tag, branch or SHA:

```sh
gh workflow run zakura-mainnet-deploy.yml --repo zakura-core/zakura \
  --ref codex/mac-verifier-poc \
  -f ref=YOUR_RELEASE_TAG -f node=zakura-mac-os -f mac_operation=deploy
```

CI resolves the ref once, builds and tests that source on an isolated hosted
ARM64 Mac, and verifies the candidate's source, lockfile and binary hashes before
installation. The Cranelift backend and Rust nightly are pinned separately.
An optional `mac_candidate_run_id` can reuse an accepted artifact matching the
requested source and lockfile. Fresh builds support `force_rebuild`; artifact
reuse cannot be combined with it. `no_restart` is unsupported.

Installation pauses the watchdog while changing the binary and expected receipts,
then checks block progress and healthy comparison. It preserves configuration,
database, original bootstrap evidence, cursor and incident evidence. A binary
rollback does not undo database migrations. Historical comparisons are not proof
that a new version reverified old blocks.

Use `mac_operation=status` for inspection and `mac_operation=dashboard` to restore
the dashboard integration. Until this PR merges, a dashboard deployment from main
can overwrite the Mac integration.

Compiler acceptance retains the unwind and double-panic probes, eight exact
consensus cases and two network panic-containment cases. See the
[compiler README](cranelift/README.md). Build acceptance and live comparison are
separate checks; neither proves the absence of shared verifier bugs.
