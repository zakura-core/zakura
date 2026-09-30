# Native Mac mainnet verifier

This package runs the same pinned Zakura consensus source on native macOS ARM64,
with conservative verification settings, and compares live committed results
against the existing Linux x86_64 mainnet reference. Endpoint addresses are private runtime configuration.

The Mac imports trusted finalized snapshot state. Its assurance boundary starts
at the recorded restored finalized height plus one. It does not independently
audit imported UTXOs/nullifiers or eliminate bugs shared by both implementations.

## Fixed deployment

- Existing privately owned Apple Silicon Mac with at least 16 GB RAM and sufficient
  SSD headroom. No provider ordering, billing, renewal, or automatic host deletion.
- Pin consensus source `af944f5194ef2e9921bc96af017629450375013c` and Rust 1.97.1;
  record the tooling revision separately. Build with one Cargo job.
- Observe a 72-hour initial verification window; review at hour 60. Stop only verifier services
  at completion, retaining the private host and its unrelated workloads.
- Mac RPC `127.0.0.1:28232`; adapter `127.0.0.1:28233`; P2P `127.0.0.1:28234`.
  Outbound peer discovery remains enabled. Mac-to-DO reverse SSH forwards only
  the adapter to DO loopback `28233`.
- Comparator polls every 30 seconds with 10-second request timeouts, at most
  16 heights per iteration and a three-block delay. That delay is an observation
  policy, not a consensus finality guarantee.
- A bootstrap anchor above the mandatory checkpoint range allows live coverage
  to use semantic verification with checkpoint/VCT fast sync disabled.

## Secrets and access

Use the existing **Zakura snapshots** Infisical project
`c57a6889-6a7c-4d05-a54a-e4a4c0b14ee7`, environment `prod`. Create these folders
before setting their secrets; do not copy credentials from other projects:

| Folder | Credentials | Access |
| --- | --- | --- |
| `/mac-verifier` | Dedicated monitor identity JSON | Operator only |
| `/mac-verifier/provisioner` | Private deployment endpoint and SSH secrets | Operator only |
| `/mac-verifier/tunnel` | Dedicated Mac forwarding private key | Operator only |
| `/mac-verifier/monitor` | `MAC_VERIFIER_SLACK_BOT_TOKEN` | Dedicated monitor identity, read only |

For the optional dedicated DM mode, use `slack-app-manifest.json` to create a dedicated Slack bot with `im:write` and `chat:write` in workspace
`T0A80TZAXK5`. Alerts target Roman `U0A81KAPYMR`; workspace identity and DM channel
are checked before sending. Never deploy an existing broadly scoped operator bot.

For DM mode, use a dedicated Infisical Universal Auth identity restricted to read secrets in
`prod:/mac-verifier/monitor`. On DO install its client ID/secret as JSON with
keys `client_id` and `client_secret`, root-owned mode `0600`, at
`/etc/zakura-mac-verifier/identity.json`. Systemd `LoadCredential` delivers it
privately to the unprivileged runner. `identity.py create --receipt <private-path>`
creates this identity, records its resource IDs, restricts authentication to the
reference egress CIDR (`MAC_VERIFIER_REFERENCE_CIDR` at runtime), and vaults `MAC_VERIFIER_MONITOR_IDENTITY_JSON` in the operator-only root
folder. The client secret expires after 96 hours; create it when provisioning
succeeds. `identity.py revoke --receipt <same-path>` revokes only that recorded
identity. Secrets are fetched at startup and are not
written to the runtime environment file or repository. Restart the monitor after
credential rotation. Do not print CLI token/secret output.

Install Roman's public SSH key for administration. Use a separate deployment key
for Actions and a separate tunnel key; never mirror Roman's personal private key.
Do not install Roman's personal private key on either workload.

## Private deployment configuration

The manual `.github/workflows/deploy-mac-verifier.yml` workflow reads secrets only
from GitHub environment `mac-verifier-private`. It has no host/IP input. Only
`main` and the approved branch configured in repository variable
`MAC_VERIFIER_DEPLOY_BRANCH` can execute it; keep the environment branch policy
restricted to those same branches. The Mac is
never registered as a general Actions self-hosted runner.

GitHub registers a new manual workflow only after it reaches the default branch.
For the initial deployment while this PR remains draft, use the same
`private_deploy.py` operator entry point with credentials fetched at runtime from
Infisical. Record the clean tooling commit before running it; keep child output
captured privately. The environment secrets are already mirrored for subsequent
workflow execution. Do not merge solely to make deployment dispatch available.

Infisical remains the source of truth. Store deployment secrets in
`prod:/mac-verifier/provisioner`:

- `MAC_VERIFIER_HOST`, `MAC_VERIFIER_USER`, `MAC_VERIFIER_SSH_KEY`,
  `MAC_VERIFIER_KNOWN_HOSTS`: private Mac IP, admin user, dedicated deployment key,
  and authenticated pinned host keys.
- `MAC_VERIFIER_SSH_PORT`: SSH port from private runtime configuration, including
  the standard port when applicable; validate the range before connecting.
- `MAC_VERIFIER_REFERENCE_HOST`, `MAC_VERIFIER_REFERENCE_USER`,
  `MAC_VERIFIER_REFERENCE_SSH_KEY`, `MAC_VERIFIER_REFERENCE_KNOWN_HOSTS`: reference
  endpoint and dedicated deployment access. Both admin identities need
  passwordless sudo for the installer. Keep these identities operator controlled.
- `MAC_VERIFIER_TUNNEL_KEY`: a dedicated ed25519 private key, vaulted before use.
- `MAC_VERIFIER_ID`: randomly generated `verifier-<32 hexadecimal characters>`;
  no hostname, IP, hardware serial number or peer identity is used as its label.

Initialize the opaque identifier with `python3 github_secrets.py init`. When the
operator provides the IP, read it from a private local file with
`python3 github_secrets.py bind-host --host-file <private-file>`; the value is
vaulted without printing it. Run `python3 github_secrets.py sync` to propagate
vaulted values to GitHub environment secrets through stdin. Never use Actions
inputs, repository variables, workflow literals or command-line IP arguments.
The runtime monitor identity stays in `prod:/mac-verifier`, and its Slack
secret stays in the monitor folder described above.

Dispatch operations in order: `preflight`, `prepare`, `bootstrap`, `activate`,
then `status`. Preparation builds the pinned source natively and installs inactive
Mac services. Bootstrap imports and anchors state through temporary SSH forwards.
Activation installs the Linux comparator and a loopback status bridge, updates
the existing mainnet dashboard, and starts Mac
services. `stop` stops only the verifier services; it never destroys the private host.
If dedicated alert credentials are not yet available, `install.py linux-observe`
starts continuous comparison and the existing dashboard with an explicit alert
delivery incident. This mode cannot qualify or start the healthy observation
window. Channel activation uses the existing fleet webhook. Optional DM activation uses
the scoped identity. `linux-activate` removes the
observation override and restarts the comparator with Slack delivery enabled.
Provision tooling first; obtain the private IP only after review and checks.

SSH output and errors are captured privately and never relayed to Actions logs.
No remote logs or endpoint-bearing receipts are uploaded as public artifacts.
Native corpus CI uploads only the structured receipt, never raw test logs. The
existing dashboard redacts IPv4 and IPv6 literals across all public JSON responses,
in addition to the verifier field allowlist.
The `status` operation emits only the dashboard allowlist. Inspect failures over
an authenticated private SSH session. Use hashed known-host entries when possible;
verify keys through an existing trusted session, never unauthenticated keyscan.

## Native build and bootstrap

On the Mac install official Homebrew packages `python@3.12 protobuf zstd`. The
host must have Xcode command-line tools; verify its compiler is usable before building. This
package builds bundled RocksDB, avoiding an implicit runtime dependency on a
mutable system RocksDB library.

Transfer the tooling package and a source bundle made with:

```bash
git -C <clean-pinned-source> bundle create /private/operator/zakura-source.bundle HEAD
```

Clone the bundle on the Mac, detach at that SHA, and run:

```bash
bash build.sh /private/operator/source /private/operator/build
```

The corpus requires a clean pinned checkout and a native Rust host. Every selected
case must report exactly one passed test; zero matches, ignored cases, and failed
cases are errors. Source/build outputs and corpus logs live outside the tooling
checkout. `build.sh` uses one Cargo job and refuses to run alongside `zakurad`.

## Install, bootstrap, activate

1. On the Mac run `sudo python3 install.py mac-prepare --binary <built-zakurad>
   --known-hosts <authenticated-reference-known-hosts>` (supply
   `MAC_VERIFIER_REFERENCE_HOST` through the root installer environment). Obtain host keys from Roman's
   existing trusted SSH configuration or another authenticated source; an
   unauthenticated `ssh-keyscan` is insufficient. This creates a nonadmin service
   account, private state/log directories, launchd definitions, and a tunnel key.
2. Vault the newly generated forwarding private key in the tunnel folder before
   using it. Copy only its public key to DO. Copy the native corpus evidence into
   `/Library/Application Support/ZakuraVerifier/evidence/`.
3. From Roman's operator machine, open two temporary SSH sessions:
   `ssh -NT -L 127.0.0.1:28235:127.0.0.1:8232 <reference-admin>@<private-reference-host>`, then
   `ssh -NT -R 127.0.0.1:28235:127.0.0.1:28235 <Mac-admin>@<Mac-IP>`.
   Together they expose Linux's local RPC only at Mac loopback `28235`, without
   copying Roman's private key to the Mac. Verify the new Mac's SSH fingerprint
   through the existing trusted administrative session before pinning its host key.
4. Run `sudo python3 bootstrap.py --source <pinned-source> --tooling-sha <PR-head>`.
   The script pins the manifest, checks compressed size/checksum, bounds expanded
   extraction by free disk, excludes copied identity/nonfinalized state, reads
   the actual finalized height offline, and compares its hash and all three pool
   roots/frontiers against Linux. It records source/build/configuration receipts.
   Close both temporary operator forwards after anchoring.
5. Copy the Mac's nonsecret `receipt.json` to DO, then run
   `python3 install.py linux-prepare --tunnel-public-key <key.pub> --receipt
   <receipt.json> --infisical-project c57a6889-6a7c-4d05-a54a-e4a4c0b14ee7
   --fleet-dashboard-script <updated-zakura-cluster-status.py>
   --fleet-watchdog-script <updated-zakura-cluster-watchdog.py>` as root.
   Install the scoped identity file. Preparation validates SSH configuration before
   reloading SSH; it does not restart the node or its fleet watchdog.
6. Run `sudo python3 install.py mac-activate`, then on DO run
   `python3 install.py linux-activate`. Verify the node, adapter, tunnel, and log-rotation launchd jobs and the
   systemd service, tunnel reconnection, live status, and advancing comparison.

Preparation is repeatable for the same deployment. Activation requires a bootstrap
receipt. Configuration changes are explicit operations: stop the owned services,
export evidence, review the change, and establish a new recorded coverage boundary
when required. Do not overwrite the database or comparison cursor to hide a gap.
Failed bootstrap staging is preserved for inspection; remove only its explicitly
identified failed staging/download files before retrying. Existing finalized state
is never silently overwritten.

## Monitor and operate

The read-only adapter accepts only `GET /v1/status` and `GET /v1/block/<height>`.
Status reports sample time, tip, receipt, current binary/configuration digests,
architecture, disk space, node RSS, and macOS memory availability. Block records
contain hash and canonical decoded Sapling/Orchard/Ironwood roots and frontiers.
Missing activated fields, malformed hex, and changing block hashes invalidate a
sample rather than manufacture agreement. No arbitrary RPC proxy is exposed.

On DO:

```bash
sudo -u zakura-mac-verifier python3 /opt/zakura-mac-verifier/monitor.py status
journalctl -u zakura-mac-verifier
```

Cursor updates are atomic. Audit writes precede cursor advancement, so a crash can
replay comparisons but cannot skip them. Reorgs rewind to a saved common block
within 1,000 heights and replay; deeper or unavailable history requires an explicit
rebootstrap. The monitor records coverage gaps, never treating an unavailable
sample as equality. The JSON status and bounded audit logs are the reporting
interface. `status_bridge.py` serves sanitized JSON on Linux loopback port `28236` through
`zakura-mac-verifier-dashboard.service`, used only as an internal status bridge.
The existing mainnet dashboard includes `zakura-mac-os` as its thirteenth node
in the normal table, fleet totals, chain summary and node detail view. Testnet is
unaffected. The bridge supplies an opaque identifier, tip hash/height, source
commit, numeric resource samples and comparison/alert counters. It excludes raw
errors, receipts, host addresses and peer identities. The node uses the existing
loopback bridge; keep its private endpoint out of the public fleet TOML and do not
proxy the raw adapter. The installer enables this integration through a service
environment override and restarts the existing mainnet dashboard and fleet watchdog; the
managed SSH inventory, webhook and other node thresholds remain unchanged.

Alerts cover availability, missing coverage, resource samples, disk below 20 GB,
memory pressure, tip stalls, prolonged catch-up, unexpected build/configuration,
chain disagreement over three polls, and freshly confirmed tree-state mismatches.
Recovery requires two good samples. Critical incidents remain latched. Stop the
monitor, investigate and preserve evidence, then acknowledge a named incident:

```bash
sudo python3 install.py linux-stop
sudo -u zakura-mac-verifier python3 /opt/zakura-mac-verifier/monitor.py ack \
  --incident 'confirmed tree state mismatch'
sudo python3 install.py linux-activate
```

The alert outbox persists stable message IDs before delivery, retries rate limits,
and caps pending messages at 128. An overflow is exposed in status and prevents a
clean qualification; investigate instead of silently discarding incident evidence.
Slack delivery failures remain pending. Slack's handling of retried IDs is not an
exactly-once delivery guarantee. DO or Slack outages are dependencies of this verification;
there is no independent monitor of the comparator host yet.

## Qualification and trial end

Run `python3 -m unittest discover -s deploy/mac-verifier/tests -v` and `bash -n
 deploy/mac-verifier/build.sh`. CI runs tooling checks and the pinned corpus on
native Linux x86_64; the actual remote Mac produces its own ARM64 receipt.

After deployment, exercise and record:

1. Mac node outage, recovery, and receipt-preserving restart.
2. Reverse tunnel outage and automatic reconnection.
3. Comparator restart with unchanged durable cursor.
4. Controlled Mac reboot after confirming unrelated workloads allow it; verify node, adapter, and tunnel recover without login.
5. Real Slack incident and recovery channel messages; invalid-token/rate-limit behavior remains
   covered by isolated tests rather than modifying production credentials.
6. Divergence through isolated fixtures, preserving live databases.

Then collect 24 uninterrupted healthy hours and at least 100 newly observed
mainnet blocks above the first Linux reference tip. Missing samples over 90 seconds,
incidents, stale resources, or falling behind reset the qualification clock.
Record actual peak RSS, memory availability, disk headroom, coverage boundary,
source/configuration identity, and final compared height. `qualified` records that
this gate has been achieved; current health remains separately visible in status.

Export evidence regularly to the reference and review by hour 60. Run
`install.py export --output <new-directory>` locally on each host and keep exports
private. After 72 hours, stop verifier services unless explicitly extended; preserve
the host, state, and evidence. This private-host verification has no provider teardown.

`mac-uninstall` and `linux-uninstall` remove only verifier service/SSH definitions and
preserve state/evidence. Revoke the forwarding key, scoped monitor identity, and
verifier Slack credential after teardown. Do not revoke existing node credentials.

Keep the implementation PR draft. Deployment, native receipts, delivered alerts,
reboot recovery, and the qualification window must all have direct evidence before
claiming the verification complete.

## Fleet channel alerts

The existing fleet watchdog sends Mac offline and fork transitions to
`#zakura-alerts` using its existing incoming webhook. Mac downtime alerts after
three minutes; other node thresholds remain unchanged. A fork alert requires
at least 70% of all other configured mainnet nodes (nine of the current twelve)
to agree on a different hash at the same height ten blocks behind the Mac tip.
That proves at least eleven divergent blocks. Offline or missing peers remain
in the denominator. Missing or racing evidence cannot trigger a fork alert or
clear an existing one. Ordinary height lag on the same chain is not a fork.
Alerts and recovery transitions use the watchdog's persistent state and delivery
queue. The Mac endpoint, SSH credentials and peer identity are never included.
These channel alerts are independent of the comparator's dedicated Slack DM
credentials and do not establish the full comparator qualification gate.

`mac-stop` persistently disables the verifier launchd jobs before unloading them,
so they stay stopped after reboot. `mac-activate` explicitly enables them again.
Bootstrap publishes its activation receipt only after cleanup, disk headroom and
state ownership checks succeed. Failed imports require operator inspection; they
are never automatically overwritten.

## Comparator alerts through the existing channel

The default private-host activation reuses the existing `#zakura-alerts` webhook
from the root-private `/etc/zakura-fleet-watchdog/env`. It materializes only that
secret into a root-owned mode 0600 verifier credential. Systemd `LoadCredential`
delivers it to the unprivileged comparator; endpoints and credential values are
never emitted. The existing Infisical-backed fleet credential remains the source
of truth. No new bot or Universal Auth identity is required for channel mode.

For an existing installation run `install.py linux-activate --alert-webhook-env
/etc/zakura-fleet-watchdog/env` as root. This removes observation mode, enables
actual transition delivery and permits qualification after its durable queue has
drained. The optional dedicated DM setup above remains available to operators.
Webhook retries are bounded and persistent queue IDs prevent repeat transitions;
ambiguous network failures can still produce duplicate Slack messages.

Channel notifications are grouped into one detailed-incident episode and one
recovery, even when several findings overlap. The fleet watchdog exclusively
owns Mac offline/stall and quorum-fork pages. The comparator suppresses transient
coverage gaps; a live Mac with incomplete comparison coverage must persist for
three minutes before it pages. Historical observation-mode notifications are
coalesced during the migration rather than replayed as stale Slack messages.
All findings remain in the incident state and qualification still fails closed.

Operator mute: set `MAC_VERIFIER_ALERTS_MUTED=1` on the existing fleet watchdog
service to exclude Mac notifications while retaining other nodes' alerting. Run
`install.py linux-observe` and restart the comparator to disable its delivery.
The node and comparison continue; observation mode cannot qualify. Re-enable
only after explicit operator authorization; do not treat a code deployment as
permission to remove the mute.
