# Native Mac mainnet verifier

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
| `status_bridge.py` | Allowlisted status for the public dashboard |
| `common.py` | Bounded RPC, canonical records and atomic state writes |
| `rotate_logs.py` | Bound Mac launchd diagnostic logs |
| `cranelift/qualify.py` | Build and test compiler acceptance candidates in CI |

The US reference host opens one SSH session per comparison cycle. A dedicated
monitoring key is restricted to the root-owned read-only probe: no interactive
shell, arbitrary command or port forwarding. The private key is generated on
Linux and never leaves it. CI installs only its public half on the Mac. The probe
uses the Mac node's existing loopback RPC on port `28232`; Linux RPC uses loopback
`8232`. The adapter HTTP service and reverse tunnel are retired.

Private connection settings and pinned host keys live under
`/etc/zakura-mac-verifier/ssh`, readable only by the monitoring account/root.
The dashboard reuses the resulting sample through the existing allowlisted status
bridge; it never receives the SSH destination or adds it to public inventory.
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

The lane is enabled with `ZAKURA_MAC_COMPARISON=1` on the existing watchdog.
Notifications remain muted by default; `ZAKURA_MAC_COMPARISON_ALERTS=1` enables
this lane through the existing watchdog channel. Migration preserves muted
operation and does not replay historical messages.

The bridge only publishes approved status fields. The dashboard distinguishes
comparison failure from node availability. Private receipts, diagnostics, host
addresses and peer identities never pass through the bridge.

## CI migration

The mainnet workflow supports `node=zakura-mac-os` and `mac_operation=migrate`
to replace the HTTP adapter with direct SSH on the existing watchdog deployment.
It installs the restricted monitoring key and probe, checks a copy of the existing
cursor against Linux, and compares SSH results with the still-running adapter.
Only after those checks pass does it switch the comparison client and disable
the Mac adapter and reverse-tunnel launchd jobs. It never restarts the node.

Migration succeeds only after new blocks are compared over SSH with both old
services disabled. Failure restores the prior transport and services while
retaining all comparison progress and evidence. Recovery files remain private.
The node binary, configuration, database and alert settings are unchanged.

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
