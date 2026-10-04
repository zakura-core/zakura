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
| `ssh_probe.py` | Bounded read-only Mac probe over SSH |
| `comparison.py` | Bounded comparison and atomic public-status publication |
| `common.py` | RPC, records, public fields, atomic writes and rotation |
| `remote/` | Checked-in deployment health checks and cursor receipt rebinding |
| `rotate_logs.py` | Bound Mac launchd diagnostic logs |
| `cranelift/qualify.py` | Build and test compiler acceptance candidates in CI |

The US reference host opens one SSH session per comparison cycle. A dedicated
monitoring key is restricted to the root-owned read-only probe: no interactive
shell, arbitrary command or port forwarding. The private key is generated on
Linux and never leaves it. Only its public half is installed on the Mac. The
probe
uses the Mac node's existing loopback RPC on port `28232`; Linux RPC uses
loopback
`8232`. The adapter HTTP service and reverse tunnel are retired.

Private connection settings and pinned host keys live under
`/etc/zakura-mac-verifier/ssh`, readable only by the monitoring account/root.
The dashboard reads the comparator's sanitized status file. Linux addresses
remain
visible. A root-only server-side file at
`/etc/zakura-mainnet-dashboard/private/addresses.json` protects the Mac address
if it appears in any public JSON field, including Linux peer diagnostics. This
file is populated from the CI secret, never served or embedded in JavaScript.
When protection settings are unavailable, `/data` keeps only typed fleet
observations; detailed JSON routes fail closed. The Mac is never added to the
public SSH inventory.
Deployment credentials stay in the private CI environment, and are not copied to
the reference host.

## Comparison and alerts

The fleet watchdog invokes `comparison.py` after its existing fleet checks.
Each invocation compares at most 16 successive heights, three blocks behind the
shorter tip, using block hashes and canonical Sapling, Orchard and Ironwood
commitment-tree roots and frontiers. It re-reads mismatches to distinguish
stable
differences from racing reads. A durable cursor prevents silently skipping
blocks
across restarts; recent history supports rewinding up to 1,000 blocks. Reorg
search progress is saved across bounded invocations. Missing historical data
cannot establish agreement, and an exhausted reorg window records a coverage
gap.

Network requests share a ten-second budget and individual two-second timeouts.
The watchdog kills the entire comparison/SSH process group after fifteen
seconds.
The remote probe also has a fifteen-second lifetime and bounded request count.
Other fleet
checks run first. Comparison outcomes are matching, catching up, unavailable,
chain disagreement, confirmed tree mismatch and coverage gap.

The watchdog owns notification delivery, suppression and recovery. Confirmed
tree
mismatches are emitted through a bounded process response before evidence is
written. They remain immediately actionable if evidence, private state or public
status publication fails, or if the child exits abnormally after reporting them.
Other
failures share a three-minute grace period across changes in failure reason.
Delivered incidents stay latched until matching resumes; a confirmed mismatch
escalates an existing incident once. Failed status publication cannot claim a
healthy recovery. Matching results recover through the existing alert lifecycle.
Private mismatch evidence remains after recovery. There is no separate
notification
queue, alert-enablement script or 24-hour qualification state.

The lane is enabled with `ZAKURA_MAC_CRANELIFT_COMPARISON=1` on the existing
watchdog.
Notifications remain muted by default;
`ZAKURA_MAC_CRANELIFT_COMPARISON_ALERTS=1` enables
this lane through the existing watchdog channel. Monitoring deployment preserves
the configured alert settings and does not replay historical messages.

The comparator atomically publishes approved status fields directly. `condition`
is the sole comparison-health signal; the watchdog consumes it through a bounded
process response and does not rewrite either status file. The dashboard
distinguishes
comparison failure from node availability. Private receipts, diagnostics, host
addresses and peer identities never enter the public status file. If address
redaction configuration is unavailable, `/data` retains only validated node
names, health categories, numeric progress and block hashes for fleet
monitoring.
Free-form diagnostics and detailed JSON endpoints remain unavailable until the
privacy configuration is repaired.

## Names and deployment compatibility

The component is named `zakura-mac-cranelift`. Its CI entry points are
`zakura-mac-cranelift.yml` (tests), `build-zakura-mac-cranelift.yml` (native
compiler acceptance), and the existing `zakura-mainnet-deploy.yml` (deployment).
The deployment helper is `deploy/deployer/zakura-mac-cranelift-manager.py`.
Tooling tests run for monitoring changes. Native acceptance runs for compiler,
corpus, build configuration or Rust input changes within a triggered run; manual
runs always build. PRs use their full diff so a monitoring revision cannot
bypass
an earlier failed compiler build. Monitoring-only PRs skip native acceptance;
PRs that include compiler inputs still require it. Missing history runs native
acceptance rather than skipping it.

The fleet dashboard names the node `mac-os-cranelift`; progress and incident
state
use that canonical name. The Mac probe emits the fleet's existing
`ancestor_hashes`
format. The quorum alert uses the shared ancestor lookup at the Mac tip minus
ten
blocks and still requires agreement from at least 70% of other nodes. Linux
probes
add that common height to their ancestry sample when needed; missing or racing
samples cannot establish a fork or clear an existing incident. The comparator's
pairwise chain disagreement remains distinct from fleet quorum attribution.
The installed node ID `zakura-mac-os`, `/Library/Application
Support/ZakuraVerifier`,
`dev.valargroup.zakura-verifier-*` launchd labels, `zakura-mac-verifier` Linux
account/services/directories and SSH alias `mac-verifier` remain compatibility
bindings. Renaming source files does not move node data, reset comparison
history,
or require a service restart. The private CI environment `mac-verifier-private`
and its encrypted `MAC_VERIFIER_*` secrets remain in place; CI maps them to
`ZAKURA_MAC_CRANELIFT_*` process variables. The existing deployment-branch
variable
and PR branch remain unchanged. The installed binary has an unexpired accepted
artifact from `mac-verifier.yml`
with the `mac-verifier-cranelift-` prefix. Those two historical bindings remain
accepted so that binary can be reused; unused former workflow paths are removed.

Dashboard and watchdog settings use the `ZAKURA_MAC_CRANELIFT_` prefix. The
deployed
configuration and incident state already use these names; unused aliases and
completed migrations are removed. Full private-address history scans run only
for binary deployment, rather than every status or helper update.

## Monitoring deployment

Deploy the selected ref through the canonical mainnet workflow on the Linux
reference node first. It owns installation of the dashboard, watchdog,
`comparison.py` and `common.py`, and gives the existing monitoring account write
access to the dedicated public-status directory. Existing comparison/alert and
privacy configuration stays in protected runtime files.

Then use `node=zakura-mac-os` and `mac_operation=dashboard` to update the Mac
SSH
probe and log rotation helper. This operation only reads Linux health. It checks
a fresh matching comparison with ancestor evidence and a healthy dashboard row;
failure restores the previous Mac helpers. The Mac node is not restarted and
comparison history is not rewound.

The comparator writes `/var/lib/zakura-mac-cranelift-public/status.json`. Raw
comparison state and connection settings remain in private directories. There is
no dashboard bridge service. Completed adapter/tunnel, cursor-schema and alert
enablement operations are removed.

## Binary deployment

Use the existing mainnet deployment's `ref` input for a tag, branch or SHA:

```sh
gh workflow run zakura-mainnet-deploy.yml --repo zakura-core/zakura \
  --ref codex/mac-verifier-poc \
  -f ref=YOUR_RELEASE_TAG -f node=zakura-mac-os -f mac_operation=deploy
```

CI resolves the ref once, builds and tests that source on an isolated hosted
ARM64 Mac, and verifies the candidate's source, lockfile and binary hashes
before
installation. The Cranelift backend and Rust nightly are pinned separately.
An optional `mac_candidate_run_id` can reuse an accepted artifact matching the
requested source and lockfile. Fresh builds support `force_rebuild`; artifact
reuse cannot be combined with it. `no_restart` is unsupported.

Installation pauses the watchdog while changing the binary and expected
receipts,
then checks block progress and healthy comparison. It preserves configuration,
database, original bootstrap evidence, cursor and incident evidence. A binary
rollback does not undo database migrations. Historical comparisons are not proof
that a new version reverified old blocks.

Use `mac_operation=status` for a strict health check; unhealthy or stale results
fail CI. `mac_operation=dashboard` updates only the Mac helpers after the Linux
monitoring deployment. Until this PR merges, a dashboard deployment from main
can overwrite the Mac integration.

Compiler acceptance retains the unwind and double-panic probes, eight exact
consensus cases and two network panic-containment cases. See the
[compiler README](cranelift/README.md). Build acceptance and live comparison are
separate checks; neither proves the absence of shared verifier bugs.
