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

### The gateway's node ID

The node ID a block names is worked out from the secret in the gateway's node
key file (or `network.zakura_node_secret_key`). The same secret always gives
the same ID, so the ID survives restarts; a new secret gives a new ID.

Moving v2 networking to protocol 4 gave every node a new ID. A block mined
before a gateway upgraded names its old ID, which no node has any more. Such
an ID stays unmatched: it takes no connection place, and it leaves the counts
once its blocks fall out of the two-day window. A gateway's new ID counts only
the blocks mined since it upgraded, so for up to two days the ranking can show
its old and new IDs side by side.

Only nodes on the current v2 protocol make v2 connections to each other, so a
gateway and the miners that prefer it must all run it to form a mesh.

## Prefer announced gateways

Every node with `mining.miner_address` set counts the gateways named in the
best chain's recent blocks, whether or not it announces one itself. The window
is two days of blocks ending 100 blocks below the tip, so it holds about 2,304
blocks at a 75-second spacing. The node reads each block as it reaches 100 deep,
reads older blocks in the window when it has nothing newer to do, and repairs
its counts after a reorg.

While its tip is recent enough to mine on, the node hands v2 networking the
gateways named in at least 10 counted blocks. Networking ranks them by block
count and keeps up to ten connection places for the highest-ranked gateways it
can reach. A gateway named in fewer blocks is counted but not ranked, so the
node neither dials it nor holds a place for it. Ten blocks is about 0.15% of a
two-day window at a 25-second spacing: a pool that mines that rarely does not
take a place, and a node that names another node's ID in its blocks must keep
mining to hold that node a place. When the tracker
next checks a stale tip, it publishes an empty ranking. Networking releases
the held places when it applies that update. A node without a miner address
holds no places.

### What a held place grants

A held place is a connection slot that only a preferred gateway may use:

- ordinary peers, inbound or dialed, cannot take it, even when every other slot
  is free;
- the node dials its preferred gateways ahead of ordinary candidates, and a
  gateway's first connection takes a held place even when the ordinary slots
  and the inbound share are full;
- a gateway in backoff still counts toward the number of places held. The
  next available gateway can fill a place while the first waits to retry;
- a ranked gateway that connects in takes a place when its rank brings it into
  the preferred set.

A held place grants nothing else: once connected, a preferred gateway is
treated like any other peer.

The peer memory budget funds up to ten additional gateway places above the ordinary
connection limit. Ordinary inbound and outbound peers keep their existing capacity.

## `getminermeshinfo`

`getminermeshinfo` takes no parameters and works on every network. It is an
admin method, because it shows the node's v2 ID and its peers' IDs: it is
served on the admin listener, on a primary listener with cookie
authentication, on Regtest, and with `rpc.allow_unauthenticated_admin`, but
not on the restricted unauthenticated listener. It reports what the node
counts, the places it holds, and its v2 peers. In this example the node has
40 funded connection slots in total, with one place currently held for a preferred gateway.

```json
{
  "node_id": "2840...36c7",
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
    { "node_id": "4d70...b690", "blocks": 40, "state": "preferred", "connected": true },
    { "node_id": "67bb...f112", "blocks": 12, "state": "backoff", "connected": false },
    { "node_id": "96dd...93ed", "blocks": 11, "state": "standby", "connected": true },
    { "node_id": "ccc7...d947", "blocks": 10, "state": "unmatched", "connected": false }
  ],
  "hold": 1,
  "slots": { "limit": 40, "in_use": 3, "preferred": 1, "ordinary": 2, "inbound": 2 },
  "peers": [
    { "node_id": "4d70...b690", "inbound": true, "preferred": true },
    { "node_id": "96dd...93ed", "inbound": false, "preferred": false },
    { "node_id": "ce24...93ea", "inbound": true, "preferred": false }
  ]
}
```

| Field | Meaning |
| --- | --- |
| `node_id` | This node's v2 node ID in hex, or null without v2 networking. |
| `announcing` | The coinbases this node builds name its gateway. |
| `mining` | The node has a miner address. |
| `synced` | The tip is recent enough to prefer gateways when this request is served. Always true on test networks. |
| `window` | The counted heights, or null on a node that does not mine. |
| `window.tip` | The best tip height the counts follow. |
| `window.newest`, `window.oldest` | The heights that count, or null while the tip is below 100. |
| `window.checked` | Heights in the window read so far. |
| `window.coverage` | The share of the window read, from 0 to 1. Below 1 the counts are a lower bound, as on a pruned node. A tip below 100 has an empty window, which reports 1. |
| `gateways` | The last ranking applied by v2 networking, best first, at most 256 gateways. Only gateways named in at least 10 counted blocks are ranked, and this node's own ID is left out. |
| `gateways[].state` | See below. |
| `gateways[].connected` | The gateway is a v2 peer now. |
| `hold` | The connection places held for preferred gateways. |
| `slots` | How connection slots are used, or null without v2 networking. `limit` includes all funded ordinary and gateway places. `in_use` is `preferred` plus `ordinary`; `preferred` counts one slot per preferred gateway connected; `inbound` counts slots of connections the remote node opened. |
| `peers` | Every v2 peer connected now, ordered by node ID, at most the connection limit. A peer connected both ways counts as inbound. |

A gateway's `state` reads its standing in v2 networking together with the
preferred set:

| State | Meaning |
| --- | --- |
| `preferred` | Reachable and among the highest-ranked: it may fill a held place. |
| `standby` | Known and reachable (it can be dialed, is being dialed, or is connected), but ranked below the gateways that fill the held places. |
| `backoff` | Known, but waiting out a failed dial or a lost connection. It still counts toward the shared hold and may connect inbound during backoff. |
| `unmatched` | Networking found no connection or usable dial address for this gateway when it ranked it. A missing or expired address record can produce this state. Gateways are usually learned through v2 peer exchange. |

On Mainnet, `synced` can turn false while `gateways` and `hold` still show the
previous ranking. The tracker rechecks tip freshness on progress and at
one-minute intervals while idle. Networking applies the ranking on its next
dial round and adjusts places as gateways connect and disconnect. Peers are read
just before slots, so a connection opening or closing between the two reads
can appear in only one of them.
