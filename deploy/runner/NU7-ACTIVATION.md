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
    "identityCheckpoint": {"height": 0, "hash": "REPLACE_WITH_VERIFIED_POST_FORK_PUBLIC_HASH"},
    "manifest": {},
    "nodes": [
      {"name": "zakura-testnet-1", "rpcUrl": "http://127.0.0.1:18232/"},
      {"name": "zakura-testnet-eu", "rpcUrl": "http://164.92.209.78:18232/"},
      {"name": "zakura-testnet-as", "rpcUrl": "http://206.189.148.0:18232/"}
    ],
    "reference": {"name": "zecrocks-testnet", "adapter": "grpcurl", "endpoint": "testnet.zec.rocks:443", "grpcurlPath": "/usr/local/bin/grpcurl", "hashByteOrder": "little"},
    "capabilities": {"faucet": null, "snapshot": null}
  }
}
```

The empty manifest objects and placeholder checkpoint intentionally
cannot run. Supply each full manifest with `nodeRevision`, `config`,
`configSha256`, and `network` (`name`, `magic`, `activationHeight`, `branchId`).
The public manifest uses standard Testnet joining configuration without staging
activation overrides and magic `fa1af9bf`. Verify the identity checkpoint comes
from the public chain **after staging diverged**, before activation. Both joining
configuration checksums are checked on startup.

Both profiles' nodes must expose the read-only `getnetworkparameters(height)`
RPC. The feed reads current tip and next candidate rules from consensus, never
from separately maintained website constants. Rules include `effectiveHeight`,
`targetSpacingSeconds`, `daaWindowBlocks` and the nested `minimumDifficulty`
object (`gapMultiplier`, `thresholdSeconds`, `comparison`). All original
consensus export fields remain available. NSM values absent from a reference
implementation are unavailable, rather than zero.

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
2. Configure an independent NU7-compatible reference. The verified Zec.rocks
   Testnet lightwalletd endpoint uses GetLightdInfo/GetBlock through the pinned
   absolute grpcurl executable, TLS and reflection. Its CompactBlock hash is
   little endian: checkpoint 4453700 decodes to
   `0011c54eccb61d4c1ec70f3f03bb8a534e7e8cb034b54384de389996a9342f38`.
   Responses are bounded to 16 MiB and each call has a 15-second process deadline.
   Alternatively a separately operated full-node RPC can supply
   getblockchaininfo/getblockhash. Neither reference needs our parameter export.
   Revalidate reachability, current consensus branch, and checkpoint before arming.
3. Install the collector/module/service, profile manifests, and configuration
   with `armed=false`; keep dispatch disabled. Run the final tests and browser
   rehearsal before switching the website's configured endpoint.
4. Observe nodes and reference advancing/agreement for one hour. Check the
   coherent response, staging APIs and live/outage builds. Obtain required
   source/codeowner approvals and deploy website before arming.
5. Set `armed=true`, restart and verify `selectionState=armed`. The selector
   polls every ten seconds. Two managed nodes plus the independent reference
   must be at least A+2, agree on activation and subsequent common hashes, and
   report active NU7. Three consecutive passes select public Testnet.
6. For automatic static publication, merge the website's `nu7_activated`
   repository_dispatch handler first. Create a narrowly scoped unattended token
   in the correct Infisical project/environment, deliver it at runtime through
   `NU7_DISPATCH_TOKEN`, then set `dispatch=true`. Never put its value in source.
   Missing/rejected credentials keep retrying; the live browser does not depend
   on static publication. Dispatch only occurs after a coherent public response
   is available; duplicate deliveries are harmless with website concurrency.

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
schedule and consensus export; successful reference identity alone is insufficient.
The command does not arm the selector or mutate persisted selection.
