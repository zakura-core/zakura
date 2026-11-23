# End of support

Every Zakura release is supported for a limited time. On Mainnet, a release
stops running once the chain tip passes the last height it supports, so that
nodes do not keep following consensus rules that the network has moved on
from. Other networks have no end of support.

A node reports where its release stands shortly after it starts and then every
hour:

- While the release is supported, it logs the last supported height.
- In the last three days of the window, it logs a warning.
- Past the last supported height, it halts with `Zakura refuses to run` at its
  next check.

The `getdeprecationinfo` RPC and the `end_of_support.*` metrics report the
same height.

## Running past the end of support

Upgrading is the operator's decision, so a node can be told to keep running an
unsupported release:

```toml
[end_of_support]
halt = false
```

With the halt off, the node warns where it used to halt:

- At startup, that the halt is off.
- In the last three days of the window, that the release will be unsupported.
- Past the last supported height, every hour and after every restart, that the
  release is unsupported.
- From the last three days of the window, while a network upgrade that
  activates after the window is still ahead, a further warning that names the
  upgrade, its activation height and the blocks left.
- Once such an upgrade has activated, a further warning that the unsupported
  release is running past it.

### What this does not protect against

The halt exists to make an outdated node fail loudly. Without it, an
unsupported release that reaches a network upgrade it does not implement stays
on the old rules and falls off the network. Turning the halt off makes the
operator responsible for tracking network upgrades.

The warnings about network upgrades only cover upgrades whose activation
height is built into the running release. The release implements those
upgrades as they were when it was built, and later releases can change how
they are handled. An upgrade that was scheduled after the release was built is
invisible to it, and no warning or alert can report it.

### Mining nodes

Mining nodes always halt. A miner on outdated rules can build blocks that the
network rejects, or extend a chain the network has left. A node counts as a
mining node when `mining.miner_address` is set, because it can then build
block templates, or when it runs the internal miner. On Mainnet such a node
refuses to start with `end_of_support.halt = false`. Other networks have no
end of support, so the setting has no effect there and miners are not refused.

### zcashd-compat mode

The zcashd sidecar build has its own end-of-support halt disabled and relies
on Zakura for consensus safety, see [zcashd compatibility](zcashd-compat.md).
With `end_of_support.halt = false`, nothing in that stack halts on an
unsupported release.

## Monitoring

| Metric | Meaning |
| --- | --- |
| `end_of_support.enforced` | `1` when the node halts at the end of support: Mainnet with the halt on. |
| `end_of_support.last_supported_height` | The last height the release supports. Mainnet only. |
| `end_of_support.remaining_blocks` | Blocks left in the support window. Negative once the release runs unsupported. Mainnet only. |
| `end_of_support.blocks_until_next_upgrade` | Blocks left before a network upgrade that the release will reach unsupported, reported from the last three days of the window, and `0` otherwise. Mainnet only. |
| `end_of_support.upgrade_activated_unsupported` | `1` once an unsupported release has kept running through a network upgrade, and `0` otherwise. Mainnet only. |

The bundled Prometheus rules in
`docker/observability/prometheus/rules/zakura_alerts.yml` alert before the end
of support, when a release runs past it, when a release will be unsupported at
a network upgrade within three days, and when an unsupported release is
running past one. The node refreshes these metrics at
each hourly check, so they can lag the chain tip by up to an hour.
