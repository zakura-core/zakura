# Miner mesh

A mining pool that hears about a new block late keeps mining on the old tip
until it does. The miner mesh gives pools a direct path to each other: a pool
names its gateway node in the coinbase of each block it mines, and every synced
node that prefers gateways (it mines or sets `mining.miner_mesh`) keeps direct
v2 connections to the gateways that recent blocks name most often.

This page covers the `mining.miner_mesh`, `mining.miner_mesh_gateways` and
`mining.miner_mesh_template_nodes` settings, the `getminermeshinfo` RPC, and
what a node does for the gateways it prefers.

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
zero-value output that names the node by its gateway ID: the first 20 bytes of
its v2 node ID, unless `mining.miner_mesh_gateways` names another node (see
[Name a relay node](#name-a-relay-node)). The output takes 34 bytes of the block and has no effect on its
validity. It is off by default.

The output's lock script is `OP_RETURN`, a push of 23 bytes, the tag `MR`
("Miner Record"), the version byte `01` and the 20-byte gateway ID:

```text
6a 17 || "MR" || 01 || gateway_id[20]
```

`zakurad` refuses to start with `miner_mesh` set unless both of these hold,
or `mining.miner_mesh_gateways` names another node (see
[Name a relay node](#name-a-relay-node)):

- v2 networking runs (`network.p2p_stack` is `"zakura"` or `"dual"`), since
  other miners find the gateway by its v2 node ID;
- its v2 node key stays the same across restarts: either
  `network.zakura_node_secret_key` is set, or the key file under
  `network.identity_dir` can be read or created.

With a miner address, it also refuses a mining config it cannot build
coinbases from, such as an address for another network, so the node never
reports a gateway its blocks do not name.

It warns, and starts anyway, when `network.zakura.listen_addr` is unset or
loopback, since other miners cannot dial the gateway. A node with
`miner_mesh` set and no `mining.miner_address` builds no coinbase, but it
still acts as a gateway that other coinbases can name, and it prefers other
gateways: see [Name a relay node](#name-a-relay-node).

### Name a relay node

A pool often builds its block templates on an internal node and runs separate
public nodes that other pools can reach. With `mining.miner_mesh_gateways`,
the coinbases the template node builds name a public relay node instead of
the template node itself:

```toml
# The template node.
[mining]
miner_address = "t1..."
miner_mesh = true
miner_mesh_gateways = ["<the relay's node_id>"]
```

```toml
# The relay node.
[mining]
miner_mesh = true
miner_mesh_template_nodes = ["<the template node's node_id>"]

[network]
p2p_stack = "dual"

[network.zakura]
listen_addr = "0.0.0.0:8234"
```

Each entry is the relay's v2 node ID in hex, the `node_id` its
`getminermeshinfo` reports, or the first 40 hex digits of it, its gateway ID.
For now the list takes one entry: `zakurad` refuses to start with more, or
with the node's own ID. An empty list, the default, names the node itself.

`mining.miner_mesh_template_nodes` on the relay lists the template node the
same way, by the `node_id` its `getminermeshinfo` reports or the first 40 hex
digits of it. No block names a template node, so without this line the relay
holds no place for it and refuses its connection once the relay's ordinary
inbound share is full.

The two nodes need different things:

- The relay is the gateway. It needs what any gateway needs: v2 networking,
  a node key that stays the same across restarts, and a listen address other
  pools can dial. With `miner_mesh` set and no miner address, it counts the
  gateways recent blocks name and holds places for the best of them, as a
  mining node does, so it stays connected to the other pools' gateways.
- The template node is not a gateway, so `zakurad` asks neither v2 networking
  nor a lasting node key of it, and does not warn about its listen address.
  It refuses to start with `miner_mesh_gateways` set unless `miner_mesh` is
  set too, and warns when it has no miner address, since then it builds no
  coinbase to name the relay in.

The template node ranks the relay like any other gateway: once its own blocks
name the relay in at least 10 counted blocks, it holds a place for the relay
and dials it. The relay keeps a place for each template node it lists, ahead
of the gateways it ranks and whether or not the template node is connected,
so the template node can always connect in, and the relay dials it when it
knows its address. The two stay connected with no extra peer settings. A
template node that is offline costs the relay only its own place; the ranked
gateways fill the rest. A template node that runs only the legacy stack holds
no places, so it reaches the relay only through its ordinary legacy peers. A
node only ever leaves out the gateway ID its own node ID starts with.

`zakurad` refuses to start with `miner_mesh_template_nodes` set unless
`miner_mesh` is set and the node is its own gateway (`miner_mesh_gateways` is
empty), since template nodes connect to the gateway their coinbases name. It
also refuses a list with the node's own ID, a repeated entry, or more than
four entries, which leaves at least six of the ten places the default budget
funds to the gateways recent blocks name. A smaller peer memory budget, or a
larger `network.zakura.max_connections`, funds fewer places, and the template
nodes may then take all of them: `zakurad` warns at startup when the funded
places are no more than the template nodes, and when none are funded at all,
so no template node gets a place. A gateway with a miner address may list
template nodes too.

`getminermeshinfo` on the template node lists the relay's gateway ID in
`announced_gateways`, which is how an operator confirms the setting. On the
relay, each template node comes first in `gateways`, with `template_node`
true, and `connected` says whether it is connected now.

Only a node with a miner address announces a gateway, since only it builds
coinbases. The `mining.mesh.announcing` metric reports whether this node's own
coinbases name a gateway, so it reads 0 on the relay. The
`mining.mesh.gateway.local_blocks` metric counts the blocks that name the
gateway this node stands for: the one it announces, or itself on a relay. So
on the relay it counts the template node's blocks, and on the template node it
counts the same blocks.

### Pools that build their own coinbase

The node puts the output in the coinbase it returns as `getblocktemplate`'s
`coinbasetxn`. A pool that submits `coinbasetxn`'s outputs unchanged already
has it, so it should not add the output again.

A pool that builds its own coinbase outputs, for example to pay its own
addresses, loses the output unless it copies it. For such pools
`getblocktemplate` also returns `minermeshscriptpubkey`, the output's
scriptPubKey in hex. It is the script alone, not a serialized output, so it
has no value or length prefix:

```json
"minermeshscriptpubkey": "6a174d5201<gateway_id>"
```

Add exactly one transparent output to the coinbase with value zero and that
scriptPubKey. The field names the same gateway `coinbasetxn` does, so it follows
`mining.miner_mesh_gateways`: it names the relay when one is set. A node that
does not announce a gateway leaves the field out.

A coinbase that repeats the same output, as when a pool adds it to outputs
that already have it, still names that gateway, and the block counts once. A
coinbase whose outputs name two different gateway IDs names none.

### The gateway's ID

The node ID a gateway ID is cut from is worked out from the secret in the
gateway's node key file (or `network.zakura_node_secret_key`). The same secret
always gives the same ID, so the ID survives restarts; a new secret gives a new
ID.

Networking knows nodes by their full 32-byte node ID. A ranked gateway ID
names the one node it knows (a connected peer, a dial in flight, or an address
record) whose node ID starts with those 20 bytes. Taking a specific gateway's
place would mean finding a node key whose ID starts with the same 20 bytes,
about 2^160 work. Two nodes can share a gateway ID; making such a pair on
purpose takes about 2^80 work. When networking knows two or more nodes with a
ranked gateway ID, it cannot tell which one the miners named, so it fails
closed: it neither dials nor prefers any of them, holds no place for the
gateway, and reports it as `ambiguous`. A node also never ranks the gateway ID
its own node ID starts with.

A block whose coinbase carries a marker in an earlier format names no
gateway: the node ignores the marker and counts the block as naming none.

Only nodes on the current v2 protocol make v2 connections to each other, so a
gateway and the miners that prefer it must all run it to form a mesh.

## Prefer announced gateways

Every node with `mining.miner_address` or `mining.miner_mesh` set counts the
gateways named in the best chain's recent blocks, whether or not it announces
one itself. The window is two
days of blocks ending 10 blocks below the tip, so it holds about 6,912 blocks
at a 25-second spacing (2,304 at the 75-second spacing before NU7). The node
reads each block as it reaches 10 deep, reads older blocks in the window when
it has nothing newer to do, and repairs its counts after a reorg. At 25-second
blocks a block starts counting about four minutes after it is mined. Reorgs
that reach a counted block are rare, and the repair drops the blocks they
displace. The one exception is a block read from a chain that was best only
briefly: if the node switches to that chain, more than 10 blocks deep, and back
before the tracker sees the switch, the block can stay counted until it leaves
the window or a later reorg's repair finds it.

While its tip is recent enough to mine on, the node hands v2 networking the
gateways named in at least 10 counted blocks. Networking ranks them by block
count and keeps up to ten connection places for the highest-ranked gateways it
can reach. A gateway named in fewer blocks is counted but not ranked, so the
node neither dials it nor holds a place for it. Ten blocks is about 0.15% of the
25-second window: a pool that mines that rarely does not take a place, and a node that
names another node's ID in its blocks must keep mining to hold that node a
place. When the tracker next checks a stale tip, it publishes a ranking with no
gateways, which keeps only a relay's template nodes. Networking releases the
gateways' held places when it applies that update. A node with
neither a miner address nor `miner_mesh` holds no places.

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
  the preferred set;
- a template node the relay lists in `miner_mesh_template_nodes` keeps its
  place while it is unreachable, so no other gateway fills it.

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
  "announced_gateways": ["2840...3793"],
  "mining": true,
  "synced": true,
  "window": {
    "tip": 3000000,
    "newest": 2999990,
    "oldest": 2997687,
    "checked": 1152,
    "coverage": 0.5
  },
  "gateways": [
    { "gateway_id": "4d70...c112", "blocks": 40, "state": "preferred", "template_node": false, "connected": true },
    { "gateway_id": "67bb...6446", "blocks": 12, "state": "backoff", "template_node": false, "connected": false },
    { "gateway_id": "96dd...3b7e", "blocks": 11, "state": "standby", "template_node": false, "connected": true },
    { "gateway_id": "ccc7...2885", "blocks": 10, "state": "unmatched", "template_node": false, "connected": false }
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
| `announced_gateways` | The gateway IDs in hex that the coinbases this node builds name: its own, or the relay `mining.miner_mesh_gateways` names. Empty when the node does not announce, and one entry for now. |
| `mining` | The node has a miner address. |
| `synced` | The tip is recent enough to prefer gateways when this request is served. Always true on test networks. |
| `window` | The counted heights, or null on a node that does not prefer gateways: one with neither a miner address nor `miner_mesh`. |
| `window.tip` | The best tip height the counts follow. |
| `window.newest`, `window.oldest` | The heights that count, or null while the tip is below 10. |
| `window.checked` | Heights in the window read so far. |
| `window.coverage` | The share of the window read, from 0 to 1. Below 1 the counts are a lower bound, as on a pruned node. A tip below 10 has an empty window, which reports 1. |
| `gateways` | The last ranking applied by v2 networking, best first, at most 256 gateways: the template nodes `mining.miner_mesh_template_nodes` lists, then the gateways named in at least 10 counted blocks. The gateway ID this node's own ID starts with is left out. |
| `gateways[].gateway_id` | The gateway ID in hex: the 20 bytes the blocks name, the first 20 bytes of the gateway's v2 node ID. Peers report their full 32-byte node IDs as `node_id`. |
| `gateways[].state` | See below. |
| `gateways[].template_node` | The entry is a template node `mining.miner_mesh_template_nodes` lists. Within the funded places, the node holds a place for it whatever its `state`, unless it is `ambiguous`. |
| `gateways[].connected` | The gateway is a v2 peer now. |
| `hold` | The connection places held for preferred gateways and template nodes. |
| `slots` | How connection slots are used, or null without v2 networking. `limit` includes all funded ordinary and gateway places. `in_use` is `preferred` plus `ordinary`; `preferred` counts one slot per preferred gateway connected; `inbound` counts slots of connections the remote node opened. |
| `peers` | Every v2 peer connected now, ordered by node ID, at most the connection limit. A peer connected both ways counts as inbound. |

A gateway's `state` reads its standing in v2 networking together with the
preferred set:

| State | Meaning |
| --- | --- |
| `preferred` | Reachable and among the highest-ranked: it may fill a held place. |
| `standby` | Known and reachable (it can be dialed, is being dialed, or is connected), but ranked below the gateways that fill the held places. |
| `backoff` | Known, but waiting out a failed dial or a lost connection. It still counts toward the shared hold and may connect inbound during backoff. |
| `unmatched` | Networking found no connection or usable dial address for this gateway when it ranked it. A missing or expired address record can produce this state. Gateways are usually learned through v2 peer exchange. A template node keeps its place in this state. |
| `ambiguous` | Networking knows two or more nodes whose node IDs start with this gateway ID, so it prefers none of them and holds no place for the gateway, even for a template node. |

On Mainnet, `synced` can turn false while `gateways` and `hold` still show the
previous ranking. The tracker rechecks tip freshness on progress and at
one-minute intervals while idle. Networking applies the ranking on its next
dial round and adjusts places as gateways connect and disconnect. Peers are read
just before slots, so a connection opening or closing between the two reads
can appear in only one of them.
