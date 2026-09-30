# Native Mac mainnet verifier POC

This package runs the same pinned Zakura consensus source on native macOS ARM64,
with conservative verification settings, and compares live committed results
against the Linux x86_64 mainnet node `us-east-0` (`159.65.183.89`).

The Mac imports trusted finalized snapshot state. Its assurance boundary starts
at the recorded restored finalized height plus one. It does not independently
audit imported UTXOs/nullifiers or eliminate bugs shared by both implementations.

## Fixed deployment

- Scaleway `M2-M`, `fr-par-1`, 16 GB RAM, 256 GB SSD, default stable macOS.
- Server name `zakura-mainnet-mac-verifier-poc`; Valargroup organization,
  account owner `roman@valargroup.dev`.
- Source `af944f5194ef2e9921bc96af017629450375013c`, Rust `1.97.1`, default features.
- A 72-hour trial: €0.17/hour before tax, approximately €12.24 plus deletion time.
  Check [current pricing](https://www.scaleway.com/en/pricing/apple-silicon/)
  before ordering. `duration_24h` is the mandatory initial lease; do not select
  `renewed_monthly`. Private networking and bandwidth upgrades are excluded.
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
| `/mac-verifier-poc` | Provider-returned sudo password and VNC URL | Operator only |
| `/mac-verifier-poc/provisioner` | `SCW_SECRET_KEY`, `SCW_PROJECT_ID`, `INFISICAL_PROJECT_ID` | Operator only |
| `/mac-verifier-poc/tunnel` | Dedicated Mac forwarding private key | Operator only |
| `/mac-verifier-poc/monitor` | `MAC_VERIFIER_SLACK_BOT_TOKEN` | Dedicated monitor identity, read only |

Use `slack-app-manifest.json` to create a dedicated Slack bot with `im:write` and `chat:write` in workspace
`T0A80TZAXK5`. Alerts target Roman `U0A81KAPYMR`; workspace identity and DM channel
are checked before sending. Never deploy an existing broadly scoped operator bot.

Use a dedicated Infisical Universal Auth identity restricted to read secrets in
`prod:/mac-verifier-poc/monitor`. On DO install its client ID/secret as JSON with
keys `client_id` and `client_secret`, root-owned mode `0600`, at
`/etc/zakura-mac-verifier/identity.json`. Systemd `LoadCredential` delivers it
privately to the unprivileged runner. `identity.py create --receipt <private-path>`
creates this identity, records its resource IDs, restricts authentication to the
DO IP, and vaults `MAC_VERIFIER_MONITOR_IDENTITY_JSON` in the operator-only root
folder. The client secret expires after 96 hours; create it when provisioning
succeeds. `identity.py revoke --receipt <same-path>` revokes only that recorded
identity. Secrets are fetched at startup and are not
written to the runtime environment file or repository. Restart the monitor after
credential rotation. Do not print CLI token/secret output.

Install Roman's `~/.ssh/id_ed25519.pub` through Scaleway's SSH key management.
Do not install Roman's personal private key on either workload.

## Provision and build

Run operator commands from this package. Keep inventory and evidence outside the
checkout in a private operator directory. Only nonsecret inventory is printed:

```bash
infisical run --env=prod \
  --projectId=c57a6889-6a7c-4d05-a54a-e4a4c0b14ee7 \
  --path=/mac-verifier-poc/provisioner -- \
  python3 provider.py create --inventory /private/operator/scw-inventory.json \
  --confirmed-hourly-eur 0.17
```

`create` checks stock, RAM, and stable default OS, records the creation attempt
before POST, records the resource ID before vaulting credentials, and reconciles
an uncertain creation by inventory lookup. It never automatically repeats a POST
whose outcome is unknown. If a same-named resource predates the recorded attempt,
inspect it and use `adopt --server-id <verified-id>` explicitly. Every operation
checks project, name, and machine type. Destruction also requires a recorded
creation request owned by this POC; adopted resources cannot be destroyed. `inspect` returns safe inventory; `reboot`
uses the provider API. Raw API responses include passwords and must not be logged.

On the Mac install official Homebrew packages `python@3.12 protobuf zstd`. The
provider supplies Xcode; verify its compiler is usable before building. This
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
   --known-hosts <authenticated-DO-known-hosts>`. Obtain host keys from Roman's
   existing trusted SSH configuration or another authenticated source; an
   unauthenticated `ssh-keyscan` is insufficient. This creates a nonadmin service
   account, private state/log directories, launchd definitions, and a tunnel key.
2. Vault the newly generated forwarding private key in the tunnel folder before
   using it. Copy only its public key to DO. Copy the native corpus evidence into
   `/Library/Application Support/ZakuraVerifier/evidence/`.
3. Establish a temporary operator SSH local forward on the Mac:
   `ssh -NT -L 127.0.0.1:28235:127.0.0.1:8232 root@159.65.183.89`.
   This gives bootstrap read-only access to Linux's existing local RPC.
4. Run `sudo python3 bootstrap.py --source <pinned-source> --tooling-sha <PR-head>`.
   The script pins the manifest, checks compressed size/checksum, bounds expanded
   extraction by free disk, excludes copied identity/nonfinalized state, reads
   the actual finalized height offline, and compares its hash and all three pool
   roots/frontiers against Linux. It records source/build/configuration receipts.
   Close the temporary operator forward after anchoring.
5. Copy the Mac's nonsecret `receipt.json` to DO, then run
   `python3 install.py linux-prepare --tunnel-public-key <key.pub> --receipt
   <receipt.json> --infisical-project c57a6889-6a7c-4d05-a54a-e4a4c0b14ee7` as root.
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
interface; there is no dashboard.

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
exactly-once delivery guarantee. DO or Slack outages are dependencies of this POC;
there is no independent monitor of the comparator host yet.

## Qualification and trial end

Run `python3 -m unittest discover -s deploy/mac-verifier/tests -v` and `bash -n
 deploy/mac-verifier/build.sh`. CI runs tooling checks and the pinned corpus on
native Linux x86_64; the actual remote Mac produces its own ARM64 receipt.

After deployment, exercise and record:

1. Mac node outage, recovery, and receipt-preserving restart.
2. Reverse tunnel outage and automatic reconnection.
3. Comparator restart with unchanged durable cursor.
4. Provider API Mac reboot; verify node, adapter, and tunnel recover without login.
5. Real Slack incident and recovery DMs; invalid-token/rate-limit behavior remains
   covered by isolated tests rather than modifying production credentials.
6. Divergence through isolated fixtures, preserving live databases.

Then collect 24 uninterrupted healthy hours and at least 100 newly observed
mainnet blocks above the first Linux reference tip. Missing samples over 90 seconds,
incidents, stale resources, or falling behind reset the qualification clock.
Record actual peak RSS, memory availability, disk headroom, coverage boundary,
source/configuration identity, and final compared height. `qualified` records that
this gate has been achieved; current health remains separately visible in status.

Export evidence regularly to DO and review by hour 60. `install.py export --output
<new-directory>` exports local nonsecret receipts, logs, and monitor evidence;
combine the two host exports in a private operator directory. By hour 72, unless
explicitly extended, stop the Mac/monitor services, export final evidence, then:

```bash
infisical run --env=prod \
  --projectId=c57a6889-6a7c-4d05-a54a-e4a4c0b14ee7 \
  --path=/mac-verifier-poc/provisioner -- \
  python3 provider.py destroy --inventory /private/operator/scw-inventory.json \
  --evidence-export /private/operator/final-evidence
```

Deletion requires saved receipt/status/cursor/audit files and an elapsed minimum
lease. Confirm API lookup returns 404 afterward and preserve that receipt: billing
continues until deletion completes. Power-off does not stop billing. The deadline
is an operator responsibility, not an automatic provider TTL; schedule the trial
follow-up when provisioning succeeds.

`mac-uninstall` and `linux-uninstall` remove only POC service/SSH definitions and
preserve state/evidence. Revoke the forwarding key, scoped monitor identity, and
POC Slack credential after teardown. Do not revoke existing node credentials.

Keep the implementation PR draft. Deployment, native receipts, delivered alerts,
reboot recovery, and the qualification window must all have direct evidence before
claiming the POC complete.
