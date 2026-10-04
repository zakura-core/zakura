# Automatic NU7 public-Testnet selection

`zakura-cluster-status.py --nu7-activation-config <JSON>` serves `/v1/dashboard`
as one coherent generation. Install `nu7_activation.py` beside it and use
`nu7-activation.service` on the existing public-Testnet primary. Port 8096 is a
read-only collector endpoint: allow only the existing API gateway and localhost
in the host firewall. `nu7-activation-firewall.service` loads the dedicated
nftables table before the collector starts; it only filters port 8096. Both units
are enabled for boot. The service trusts forwarded client addresses only from
that explicit gateway IP, so visitors have separate rate-limit quotas. Add
`/v1/dashboard` to that gateway's Caddy allowlist.
The ordinary status API and staging services continue independently.

## Runtime configuration

Generate `/etc/zakura/nu7-activation.json` from verified deployment artifacts.
It is not a secret; RPC URLs must be private/allowlisted and contain no embedded
credentials. `stateFile` should be
`/var/lib/zakura-nu7-activation/selection.json`. The service's StateDirectory is
persistent and writable by its DynamicUser. Back it up before service changes;
never delete it to recover an outage.
Startup adopts intact existing selection state and writes a durable
`selection.initialized` marker. If the selection file is missing but that
marker or the legacy lock file remains, startup requires operator repair;
it cannot initialize staging again. Preserve the whole state directory.

Required shape:

```json
{
  "armed": false,
  "dispatch": false,
  "stateFile": "/var/lib/zakura-nu7-activation/selection.json",
  "staging": {
    "statusUrl": "https://api.nu7.valargroup.dev/v1/status",
    "manifest": {},
    "capabilities": {"faucet": null, "snapshot": null}
  },
  "public": {
    "identityCheckpoint": {"height": 4453700, "hash": "0011c54eccb61d4c1ec70f3f03bb8a534e7e8cb034b54384de389996a9342f38"},
    "manifest": {},
    "nodes": [
      {"name": "zakura-testnet-1", "rpcUrl": "http://127.0.0.1:18232/"},
      {"name": "zakura-testnet-eu", "rpcUrl": "http://164.92.209.78:18232/"},
      {"name": "zakura-testnet-as", "rpcUrl": "http://206.189.148.0:18232/"}
    ],
    "reference": {"name": "official-zebra7-eu", "rpcUrl": "http://164.92.209.78:28232/"},
    "capabilities": {"faucet": null, "snapshot": null}
  }
}
```

The empty manifest objects intentionally cannot run. The checkpoint above is
the verified public-Testnet checkpoint used by this deployment. Supply each full manifest with `nodeRevision`, `config`,
`configSha256`, and `network` (`name`, `magic`, `activationHeight`, `branchId`).
The public manifest uses standard Testnet joining configuration without staging
activation overrides and magic `fa1af9bf`. Verify the identity checkpoint comes
from the public chain **after staging diverged**, before activation. Both joining
configuration checksums are checked on startup.
Managed RPC endpoints must be distinct after URL normalization, and the
reference must have a distinct endpoint and source name. This rejects duplicate
configured votes; independently verify host provenance because DNS aliases or
proxies can still reach the same underlying node.

Both profiles' nodes must expose the read-only `getnetworkparameters(height)`
RPC. The feed reads current tip and next candidate rules from consensus, never
from separately maintained website constants. Rules include `effectiveHeight`,
`targetSpacingSeconds`, `daaWindowBlocks` and the nested `minimumDifficulty`
object (`gapMultiplier`, `thresholdSeconds`, `comparison`). All original
consensus export fields remain available. NSM values absent from a reference
implementation are unavailable, rather than zero.
Public selection requires two validators to agree on complete current and
next-block exports at a common pinned height, as well as reference block hashes.
Publication separately corroborates the exact displayed heights with a second
validator, so a higher-tip parameter outlier cannot supply the dashboard rules.

Capability objects are explicit: `faucet` is null or `{apiUrl, claimZat}`;
`snapshot` is null or verified immutable snapshot metadata. Do not advertise the
staging faucet/snapshot in the public profile. Snapshot is optional for normal
public-Testnet joining.

## Preparation and arming

1. Upgrade all three existing public nodes to the approved full SHA/checksum,
   validate their public identity and advancing common chain. Restrict regional
   RPC inbound traffic to the collector primary; prove other sources cannot use
   it. Staging RPC remains loopback-only: its existing status collector embeds
   optional pinned-tip consensus exports in /v1/status over HTTPS.
2. Use the isolated official Zebra reference on the existing EU host,
   `http://164.92.209.78:28232/`, through the JSON-RPC adapter.
   `nu7-reference.service` uses its own database under
   `/mnt/data/nu7-reference/cache`; do not modify the live Zakura or wallet
   databases. `nu7-reference-rpc-guard.service` persistently restricts port 28232
   to loopback and collector primary `167.99.103.111`; verify both allowed and
   rejected sources. The pinned official Zebra 7.0.0-rc.0 runtime reports
   protocol 170180 and public-Testnet NU7 pending at height 4465026, branch
   `77190ad9`. It provides an independent consensus implementation on a
   Valargroup-operated host. It does not need our parameter-export RPC.
   Verify runtime version, release provenance and schedule separately from
   chain qualification: a correct future schedule does not establish a synced
   chain. The reference must also verify checkpoint 4453700 above, agree with
   all three nodes at a current common height, remain within two blocks of
   them, and advance. Configure this reference while the collector is disarmed,
   even during catch-up: identity checks fail closed until the checkpoint is
   available, and durable qualification then starts automatically.
   Zec.rocks reported Zebra 6.3.0 during preparation; its earlier common-hash
   agreement was chain evidence only, not NU7 schedule/readiness evidence.
   Do not substitute it for the pinned NU7-capable reference.
3. Install the collector/module/service, profile manifests, and configuration
   with `armed=false`; keep dispatch disabled. Run the final tests and browser
   rehearsal before switching the website's configured endpoint.
4. Require one uninterrupted hour of node/reference observations no older than
   60 seconds, matching common hashes, lag at most two blocks, and advancement
   by every source using the enabled `nu7-public-readiness.service` on the public
   primary. Its persistent read-only receipt is
   `/var/lib/zakura-nu7-readiness/receipt.json`. It stays unqualified during
   reference catch-up, starts the hour after checkpoint and current-chain
   checks pass, and resets on source failure, lag over two blocks, a sampling
   gap over 90 seconds, or a changed public-profile/reference fingerprint.
   Check `qualified=true`, source revision, reference identity and the latest
   observation time; neither a stale receipt nor elapsed catch-up time qualifies.
   Check the coherent response, staging APIs and live/outage builds. Obtain
   required source/codeowner approvals and deploy the website before arming.
5. Set `armed=true`, restart and verify `selectionState=armed`. The selector
   polls every ten seconds. Two managed nodes plus the independent reference
   must be at least A+2, agree on activation and subsequent common hashes, and
   report active NU7. Three consecutive passes select public Testnet.
6. Use the website's `.github/workflows/nu7-activation-watch.yml` on reviewed,
   deployed `main` for automatic static publication. The watcher requests a
   ten-minute schedule, validates the collector's public selection, and dispatches
   `nu7_activated` only when the recovered `gh-pages` baseline is not public
   and no publication is already queued or running. The receiver builds
   approved `main` and requires a public response; it does not trust event
   payload configuration. Scheduled reruns retry outages and failed publication.
   There is no calendar expiry, and GitHub can delay scheduled runs. The watcher
   uses the repository-scoped short-lived `GITHUB_TOKEN` with Contents: write
   and Actions: read. No PAT or additional Infisical credential is needed.
   Keep backend `dispatch=false` for this deployed path. The browser independently
   follows the validated collector within its 30-second polling cadence.

Backend repository dispatch remains an optional alternative, not a prerequisite:
if separately chosen, provide a narrowly scoped unattended token from the correct
Infisical project/environment through `NU7_DISPATCH_TOKEN` at runtime and enable
`dispatch=true`. Never put the token in source. Missing/rejected credentials are
retried; dispatch only follows a coherent public response. The deployed scheduled
publication path does not use this credential or option.

Selection is atomic, durable, and process-locked. After selection, no outage or
restart can restore staging. Last validated public data stays dated and becomes
unavailable/degraded when observation fails. The durable state includes the
last public envelope for outage builds/recovery. A missing reference holds
selection; it is never treated as positive activation evidence.

## Final validation

Run `python3 -m unittest discover -s deploy/runner -p 'test_nu7_activation.py'`
and the existing cluster-status suite once after the prototype is integrated.
Rehearse A-1/A/A+1/A+2, three fresh passes, reference disagreement/unavailability,
one managed node down, wrong identity, stale progress, process locking,
restart after selection, failed/duplicate publication, and live/outage site
builds. Use a disposable public-Testnet faucet claim only when that capability
has been prepared. Difficulty tests belong to the node's consensus export:
450 seconds must not qualify for minimum difficulty; 451 must qualify.

Do not claim the system armed while a reference endpoint, node revision,
publication path or codeowner approval is pending. Public faucet/snapshot
capabilities may remain null when those services are not prepared. After real
activation observe at least one full 102-block post-activation window and verify
common hashes and website generation; random block intervals need not average
exactly 25 seconds.

Run read-only runtime qualification without taking the selector state lock:

```bash
ssh root@167.99.103.111 'runuser -u nu7-activation -- python3 /opt/zakura-nu7-status/nu7-qualify.py'
```

It exits nonzero until all three node sources expose the expected public NU7
schedule/export and agree with the reference at a common height. Inspect
`preparedChainAgreement`, `referenceNu7ScheduleVerified` and
`activationReferencePrepared` separately: a zero exit status establishes current
chain agreement, not the persistent one-hour gate or activation readiness alone.
`activationReferencePrepared` also requires verified reference identity/schedule,
an actual Zebra 7 runtime and protocol at least 170180. A compatible binary still
catching up remains unprepared until its trusted checkpoint is verified. The
command does not arm the selector or mutate persisted selection.

Inspect the persistent qualification separately:

```bash
ssh root@167.99.103.111 'systemctl is-active nu7-public-readiness.service; cat /var/lib/zakura-nu7-readiness/receipt.json'
```

Runtime qualification checks each getinfo.build for the approved full revision's
nine-character prefix in addition to the consensus export and chain identity.
For an approved new deployment, qualify it read-only before changing the manifest:

```bash
ssh root@167.99.103.111 'runuser -u nu7-activation -- python3 /opt/zakura-nu7-status/nu7-qualify.py --expected-revision FULL_APPROVED_SHA'
```

The override applies only to that command's in-memory configuration; it never
changes the selector or runtime manifest. Retain deployment artifact checksum
evidence as well: a short build identifier is not a full binary attestation.

Before installing the shared staging dashboard or remote miner health units, create
the dedicated system account named `zakura-nu7-dashboard` or
`zakura-nu7-miner-status`, respectively, with `useradd --system --no-create-home
--shell /usr/sbin/nologin NAME`. Stop an already running DynamicUser unit first
when that name is not in `/etc/passwd`, because its transient NSS entry otherwise
blocks user creation. Ubuntu system D-Bus can reject transient UIDs even for
read-only `systemctl is-active`; the stable unprivileged identity preserves real
service-state observations. `DynamicUser=yes` reuses the existing identity and
the remaining service sandbox still applies. Verify actual `/v1/miner`
`nodeActive`, `minerActive`, and `nodeHealthy` before accepting the cutover.
