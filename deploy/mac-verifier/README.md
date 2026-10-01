# Native Mac mainnet verifier

This package observes an existing native macOS ARM64 Zakura verifier and compares
its committed results against a Linux x86_64 mainnet reference. It contains the
runtime services, comparison tests and compiler acceptance evidence. Host setup,
secret provisioning, snapshot import and remote installation are outside this package. A CI
workflow builds qualified Cranelift deployment candidates on an isolated Mac.

The verifier uses pinned consensus source
`af944f5194ef2e9921bc96af017629450375013c`. Imported finalized snapshot history is
trusted. Coverage starts at the recorded bootstrap height plus one; matching live
results do not independently audit imported UTXOs/nullifiers or eliminate bugs
shared by both nodes.

## Runtime components

| Component | Location | Responsibility |
| --- | --- | --- |
| `adapter.py` | Mac | Read-only node samples, build identity and resources |
| `monitor.py` | Linux reference | Sequential comparison, durable cursor and incidents |
| `status_bridge.py` | Linux reference | Allowlisted status for the existing dashboard |
| `rotate_logs.py` | Mac | Bound launchd diagnostic logs without reopening child descriptors |
| `common.py` | Both hosts | Bounded transport, canonical records and atomic state writes |
| `alert_preflight.py` | Linux reference | Quiet alert enablement checks and historical-message archival |

The adapter accepts only `GET /v1/status` and `GET /v1/block/<height>` on Mac
loopback port `28233`; the node RPC stays on loopback `28232`. A dedicated reverse
SSH tunnel exposes only that adapter on the Linux reference's loopback. The
reference RPC stays on loopback `8232`. Requests have ten-second timeouts and
bounded JSON responses; redirects and operator HTTP proxies are refused by the
comparison transport.

The comparator checks block hashes and canonical hexadecimal bytes for the
Sapling, Orchard and Ironwood commitment-tree roots and frontiers. Missing pools,
malformed records, changed hashes during reads and stale samples invalidate the
sample. It polls every 30 seconds, compares at most 16 heights per iteration and
stays three blocks behind the shorter tip. This delay is an observation policy,
not a consensus finality guarantee.

Audit writes precede atomic cursor advancement, so a crash can replay comparisons
but cannot skip heights. A reorganization rewinds to a saved common block within
1,000 heights and replays. Deeper or unavailable history latches a coverage-gap
incident that requires operator investigation. Stable repeated tree-state
mismatches are preserved privately and remain latched until acknowledged.

## Required deployment evidence

The runtime assumes operator-managed accounts, launchd/systemd supervision and
restricted forwarding are already installed. Keep the Mac separate from general
CI runners and preserve its unrelated workloads. Use dedicated forwarding keys
and authenticated pinned host keys; never copy a personal private key onto a host.
Infisical remains the source of truth for private configuration and credentials.

The Mac runtime base is `/Library/Application Support/ZakuraVerifier`. The node
uses pruned mainnet storage with checkpoint/VCT fast sync disabled and bounded
concurrency. Its RPC and adapter remain on loopback.

The runtime consumes a private receipt containing `bootstrap_height`,
`bootstrap_record`, `deployed_at`, `binary_sha256` and `config_sha256`. The starting
anchor must agree with the Linux reference on block hash and all three tree
roots/frontiers. The Linux expected receipt at
`/etc/zakura-mac-verifier/receipt.json` must match the Mac receipt exactly.

Unexpected receipt, binary or configuration changes invalidate monitoring.
Preserve the starting anchor, cursor/history, incidents and outbox across reviewed
identity transitions; establish a fresh qualification baseline. Never overwrite
state or erase a gap to manufacture qualification.

## Private and public status

Linux comparison state lives in `/var/lib/zakura-mac-verifier`: `cursor.json`,
`status.json`, bounded fsynced `audit.jsonl` journals and private incident evidence.
Inspect these only in a private operator session:

```sh
sudo -u zakura-mac-verifier python3 /opt/zakura-mac-verifier/monitor.py status
journalctl -u zakura-mac-verifier
```

`status_bridge.py` exposes only approved fields on Linux loopback port `28236`.
The existing mainnet dashboard consumes this bridge with
`ZAKURA_PRIVATE_VERIFIER_STATUS=1` and displays the Mac as an ordinary fleet node,
using an opaque `verifier-<32 hexadecimal characters>` identifier. It includes
numeric resources, tip identity and comparison counters; it excludes raw errors,
receipts, private host addresses and peer identity. Public JSON additionally
redacts address literals. Do not put the private endpoint in the public fleet
inventory or proxy the raw adapter through the dashboard. Testnet is unaffected.

## Alerts and quiet enablement

`monitor.py observe` continues comparison without delivery or operational
qualification. `monitor.py channel` uses only the existing fleet channel webhook,
provided as a root-private systemd `LoadCredential` named `slack-webhook`.
There is no dedicated Slack bot or DM identity path.

The fleet watchdog owns Mac offline/stall and quorum-fork alerts. Mac downtime
alerts after three minutes. A fork alert requires at least 70% of all other
configured mainnet nodes to agree on a different hash at the same height ten
blocks behind the Mac tip. Offline or missing peers remain in the denominator.
Missing evidence cannot prove a fork or clear an existing incident.

The comparator groups resource, build-identity and persistent coverage findings
into one detailed-incident episode and one recovery. Transient coverage gaps do
not page immediately, but all gaps affect qualification. Recovery requires three
distinct good samples spanning at least one minute; bad evidence or a gap over
90 seconds resets that sequence. Critical incidents remain latched.

Notification IDs are persisted before delivery. Rate-limit retries are bounded;
failed delivery remains queued, and a 128-message overflow blocks qualification.
Incoming webhooks do not provide exactly-once delivery: an ambiguous failure can
produce duplicate messages. Linux reference and Slack availability are operational
dependencies; this package has no independent observer of the comparator host.

Keep `MAC_VERIFIER_ALERTS_MUTED=1` on the fleet watchdog and the comparator in
observation mode while alerts are muted. Changing code does not authorize unmuting.
After explicit operator authorization, stop the comparator and run
`alert_preflight.py` as its service user. It requires five fresh healthy samples
spanning two minutes, complete coverage, matching receipt, no actionable incident
or overflow, recognized queued messages and a matching fleet quorum. It never
contacts Slack. `--apply` archives historical messages before clearing their queue
and resets the qualification baseline. If reconciling watchdog state, stop that
service too and supply `--fleet-state`; other nodes' state must be preserved.

Only after a successful fresh preflight may the operator select channel mode and
remove the Mac mute. Never replay historical observation notifications. To
acknowledge an investigated latched incident, stop the comparator, preserve its
evidence and run `monitor.py ack --incident '<incident name>'` as the service user.
Acknowledgement does not restore qualification or repair a divergent node.

## Acceptance and qualification

Run the runtime tooling tests with:

```sh
python3 -m unittest discover -s deploy/mac-verifier/tests -v
```

Native Cranelift CI executes the eight pinned consensus cases and two actual
node panic-containment tests. Every case must execute exactly once with zero
failed or ignored tests. Compiler acceptance is described in
[`cranelift/README.md`](cranelift/README.md); a dashboard row does not prove it.

Record node/tunnel outage and recovery, comparator restart preserving its cursor,
controlled host-reboot recovery when unrelated workloads permit it, and real
channel incident/recovery delivery. Test divergence in isolated fixtures, never
by altering live databases. Evidence from a different compiler remains distinct.

Operational qualification requires 24 uninterrupted healthy hours and at least
100 newly compared blocks beyond the reference tip recorded at the start of that
healthy window, with delivery available and no pending incidents/notifications.
Missing or stale samples, resource problems and falling behind reset the window.
`qualified` records a historical achievement; current health, incidents and
coverage remain separate status fields. It must not be interpreted as perpetual
health. Muted compiler shadow observation is a separate runtime assurance gate.
