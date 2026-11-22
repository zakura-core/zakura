# Miner mesh

A mining pool that hears about a new block late keeps mining on the old tip
until it does. The miner mesh gives pools a direct path to each other: a pool
names its gateway node in the coinbase of each block it mines, and every synced
mining node keeps direct v2 connections to the gateways that recent blocks name
most often.

This page covers the `mining.miner_mesh` setting, the `getminermeshinfo` RPC,
and what a node does for the gateways it prefers.

## Announce a gateway

```toml
[mining]
miner_address = "t1..."
miner_mesh = true

[network]
p2p_stack = "dual"

[network.zakura]
listen_addr = "0.0.0.0:8234"
```

With `miner_mesh = true`, every coinbase the node builds gets one extra
zero-value output that carries the node's v2 node ID. The output takes 48 bytes
of the block and has no effect on its validity. It is off by default.

`zakurad` refuses to start with `miner_mesh` set unless:

- v2 networking runs (`network.p2p_stack` is `"zakura"` or `"dual"`), since
  other miners find the gateway by its v2 node ID;
- its v2 node key stays the same across restarts: either
  `network.zakura_node_secret_key` is set, or the key file under
  `network.identity_dir` can be read or created.

It warns, and starts anyway, when `network.zakura.listen_addr` is unset or
loopback (other miners cannot dial the gateway) or when `mining.miner_address`
is unset (the node builds no coinbase, so it names no gateway).

## Prefer announced gateways

Every node with `mining.miner_address` set counts the gateways named in the
best chain's recent blocks, whether or not it announces one itself. The window
is two days of blocks ending 100 blocks below the tip, so it holds about 2,304
blocks at a 75-second spacing. The node reads each block as it reaches 100 deep,
reads older blocks in the window when it has nothing newer to do, and repairs
its counts after a reorg.

While its tip is recent enough to mine on, the node hands the counts to v2
networking, which ranks the gateways by block count and keeps up to ten
connection places for the highest-ranked gateways it can reach. A node that is
not synced, or does not mine, holds no places, and its connection limits are
the same as without the miner mesh.

### What a held place grants

A held place is a connection slot that only a preferred gateway may use:

- ordinary peers, inbound or dialed, cannot take it, even when every other slot
  is free;
- the node dials its preferred gateways ahead of ordinary candidates, and a
  gateway's first connection takes a held place even when the ordinary slots
  and the inbound share are full;
- a preferred gateway whose dial fails, or whose connection drops, keeps its
  place reserved while it waits out a backoff, and may still connect in;
- a ranked gateway that connects in takes a place when its rank brings it into
  the preferred set.

A held place grants nothing else: once connected, a preferred gateway is
treated like any other peer.

The places come out of the quarter of `network.zakura.max_connections` kept for
the node's own dials, after the places kept for bootstrap peers, so the hold
never shrinks the share of slots inbound peers may take. With the default limit
the node holds up to ten places.

## `getminermeshinfo`

`getminermeshinfo` takes no parameters and works on every network. It is an
admin method, because it shows the node's v2 ID and its peers' IDs: it is
served on the admin listener, on a primary listener with cookie
authentication, on Regtest, and with `rpc.allow_unauthenticated_admin`, but
not on the restricted unauthenticated listener. It reports what the node
counts, the places it holds, and its v2 peers:

```json
{
  "node_id": "1111...1111",
  "announcing": true,
  "mining": true,
  "synced": true,
  "window": {
    "tip": 3000000,
    "newest": 2999900,
    "oldest": 2997597,
    "checked": 1152,
    "coverage": 0.5
  },
  "gateways": [
    { "node_id": "2222...2222", "blocks": 40, "state": "preferred", "connected": true },
    { "node_id": "3333...3333", "blocks": 12, "state": "backoff", "connected": false },
    { "node_id": "4444...4444", "blocks": 7, "state": "standby", "connected": true },
    { "node_id": "5555...5555", "blocks": 3, "state": "unmatched", "connected": false }
  ],
  "hold": 2,
  "slots": { "limit": 92, "in_use": 3, "preferred": 1, "ordinary": 2, "inbound": 2 },
  "peers": [
    { "node_id": "2222...2222", "inbound": true, "preferred": true },
    { "node_id": "4444...4444", "inbound": false, "preferred": false },
    { "node_id": "6666...6666", "inbound": true, "preferred": false }
  ]
}
```

| Field | Meaning |
| --- | --- |
| `node_id` | This node's v2 node ID in hex, or null without v2 networking. |
| `announcing` | The coinbases this node builds name its gateway. |
| `mining` | The node has a miner address. |
| `synced` | The tip is recent enough to prefer gateways. Always true on test networks. |
| `window` | The counted heights, or null on a node that does not mine. |
| `window.tip` | The best tip height the counts follow. |
| `window.newest`, `window.oldest` | The heights that count, or null while the tip is below 100. |
| `window.checked` | Heights in the window read so far. |
| `window.coverage` | The share of the window read, from 0 to 1. Below 1 the counts are a lower bound, as on a pruned node. A tip below 100 has an empty window, which reports 1. |
| `gateways` | The gateways v2 networking ranks, best first, at most 256. Empty unless the node mines and is synced. This node's own ID, and IDs that are not valid node IDs, are left out. |
| `gateways[].state` | See below. |
| `gateways[].connected` | The gateway is a v2 peer now. |
| `hold` | The connection places held for preferred gateways. |
| `slots` | How connection slots are used, or null without v2 networking. `in_use` is `preferred` plus `ordinary`; `preferred` counts one slot per preferred gateway connected; `inbound` counts slots of connections the remote node opened. |
| `peers` | Every v2 peer connected now, ordered by node ID, at most the connection limit. A peer connected both ways counts as inbound. |

A gateway's `state` reads its standing in v2 networking together with the
preferred set:

| State | Meaning |
| --- | --- |
| `preferred` | Reachable and among the highest-ranked: it may fill a held place. |
| `standby` | Known and reachable (it can be dialed, is being dialed, or is connected), but ranked below the gateways that fill the held places. |
| `backoff` | Known, but waiting out a failed dial or a lost connection. It still counts towards the hold, so a place may stay reserved for it. |
| `unmatched` | v2 networking has no record of it: it is not connected and discovery has no address for it, so it takes no place. Gateways are usually learned through v2 peer exchange. |

The `gateways`, `hold` and `slots` fields come from the set the node formed on
its last dial round, about once a second, and change in between when a
gateway connects in or loses its last connection. The peers are read just
before the slots, so a connection that opens or closes in between can show in
one and not the other.
