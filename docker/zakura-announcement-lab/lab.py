#!/usr/bin/env python3
"""LAB ONLY: controller for the announcement-window lab.

Each node runs in its own container, so `tc netem` shapes its egress. A topology names the
nodes and the edges between them. Every other pair is blackholed, so blocks travel only along
edges. Miners are always `a` and `b`.

    triangle   a - o - b                 (the original lab: one observer)
    diamond    the triangle plus r1, linked to both a and o
    line:N     a - r1 - ... - r(N-2) - b (multi-hop relays)
    ring:N     a and b opposite on a ring of N nodes

    lab.py up <inst> [--topology triangle] [--stack dual] [--stack-b legacy] [--persist a]
    lab.py down <inst>
    lab.py exp1 <inst> --sizes 0,2000000 --rtt 100 --rate 50 --idle 18 --min-valid 20 --out f.csv
    lab.py grid <inst> --rtts 20,100 --rates 10,50 --sizes 500000,2000000 [--cold] --out f.csv
    lab.py exp2 <inst> --sizes 2000000 --offsets 0,100 --windows=-1,0 --rtt 100 --rate 50 ...
    lab.py exp3 <inst> --delays 0,500 --offsets 1000 --rtt 100 --rate 50 ...
    lab.py withhold <inst> --delays 2000,10000 --windows=-1,0 --offsets 600 ...
    lab.py multihop <inst> --offsets 0,100 [--rule-off r1,r3] ...
    lab.py edge <inst> --cases mined,mined-invalid --size 2000000 --delays 1000 ...

Every trial row carries `valid` and `invalid_reason`. With `--min-valid N`, a cell repeats until
it has N valid trials or `--max-trials` trials in total.
"""

import argparse
import csv
import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
TARGET = os.environ.get("LAB_TARGET", "/root/projects/zakura/target/release")
ZAKURAD = os.environ.get("LAB_ZAKURAD", os.path.join(TARGET, "zakurad"))
LABCTL = os.path.join(TARGET, "examples", "labctl")
IMAGE = os.environ.get("LAB_IMAGE", "zakura-claim-lab")
LINE = re.compile(r"LAB (claim_drawn|claim_resolved) (.*)$")
MINERS = ("a", "b")

# Ed25519 node ids of the seeds `<byte> * 32`, so each node can name its bootstrap peers before
# they start.
NODE_IDS = {
    "01": "8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c",
    "02": "8139770ea87d175f56a35466c34c7ecccb8d8a91b4ee37a25df60f5b8fc9b394",
    "03": "ed4928c628d1c2c6eae90338905995612959273a5c63f93636c14614ac8737d1",
    "04": "ca93ac1705187071d67b83c7ff0efe8108e8ec4530575d7726879333dbdabe7c",
    "05": "6e7a1cdd29b0b78fd13af4c5598feff4ef2a97166e3ca6f2e4fbfccd80505bf1",
    "06": "8a875fff1eb38451577acd5afee405456568dd7c89e090863a0557bc7af49f17",
    "07": "ea4a6c63e29c520abef5507b132ec5f9954776aebebe7b92421eea691446d22c",
    "08": "1398f62c6d1a457c51ba6a4b5f3dbd2f69fca93216218dc8997e416bd17d93ca",
    "09": "fd1724385aa0c75b64fb78cd602fa1d991fdebf76b13c58ed702eac835e9f618",
    "0a": "43a72e714401762df66b68c26dfbdf2682aaec9f2474eca4613e424a0fbafd3c",
}
# Fixed per name, so a node keeps its address and identity in every topology. The triangle
# keeps its original seeds (o=01, a=02, b=03) and addresses (a=.11, b=.12, o=.13).
SLOTS = {"a": 2, "b": 3, "o": 1, **{f"r{i}": 3 + i for i in range(1, 8)}}
OCTETS = {"a": 11, "b": 12, "o": 13, **{f"r{i}": 13 + i for i in range(1, 8)}}


def sh(*args, check=True, capture=True):
    result = subprocess.run(args, check=check, capture_output=capture, text=True)
    return result.stdout.strip() if capture else ""


def topology(spec):
    """(nodes, edges) for a topology spec. Edges are sorted name pairs."""
    kind, _, count = spec.partition(":")
    if kind == "triangle":
        return ["a", "b", "o"], {("a", "o"), ("b", "o")}
    if kind == "diamond":
        # The triangle plus relay r1, which also hears A: O learns A's header twice.
        return ["a", "b", "o", "r1"], {("a", "o"), ("b", "o"), ("a", "r1"), ("o", "r1")}
    total = int(count)
    if kind == "fan":
        # A line from A through the relays, with legacy miner B linked to every relay: each relay
        # hears A's header one hop further away, and B's block directly.
        if total < 3:
            raise ValueError("fan needs at least 3 nodes")
        relays = [f"r{i}" for i in range(1, total - 1)]
        path = ["a"] + relays + ["b"]
        pairs = list(zip(["a"] + relays, relays)) + [("b", relay) for relay in relays]
        return path, {tuple(sorted(pair)) for pair in pairs}
    if kind == "line":
        if total < 3:
            raise ValueError("line needs at least 3 nodes")
        path = ["a"] + [f"r{i}" for i in range(1, total - 1)] + ["b"]
        pairs = list(zip(path, path[1:]))
    elif kind == "ring":
        if total < 4 or total % 2:
            raise ValueError("ring needs an even number of nodes, at least 4")
        relays = [f"r{i}" for i in range(1, total - 1)]
        half = len(relays) // 2
        path = ["a"] + relays[:half] + ["b"] + relays[half:]
        pairs = list(zip(path, path[1:] + path[:1]))
    else:
        raise ValueError(f"unknown topology {spec}")
    if total > len(NODE_IDS):
        raise ValueError(f"at most {len(NODE_IDS)} nodes")
    return path, {tuple(sorted(pair)) for pair in pairs}


def host_limit():
    """The 1-minute load above which the shared host is too busy for timing trials."""
    return (os.cpu_count() or 8) - 2


def ints(text):
    return [int(part) for part in str(text).split(",") if part != ""]


def floats(text):
    return [float(part) for part in str(text).split(",") if part != ""]


def names(text):
    return [part for part in (text or "").split(",") if part]


class Lab:
    def __init__(self, inst):
        self.inst = int(inst)
        self.net = f"claimlab{self.inst}"
        self.run = os.path.join(HERE, "run", str(self.inst))
        self.shaping = (0, 0, 0.0)
        self.offsets = {}
        layout = os.path.join(self.run, "layout.json")
        if os.path.exists(layout):
            saved = json.load(open(layout))
            self.nodes, self.edges = saved["nodes"], {tuple(edge) for edge in saved["edges"]}
            self.stacks, self.persist = saved["stacks"], set(saved.get("persist", []))
            self.faketime = set(saved.get("faketime", []))
            self.spec = saved.get("topology", "triangle")
        else:
            self.spec = "triangle"
            self.nodes, self.edges = topology(self.spec)
            self.stacks = {name: "dual" for name in self.nodes}
            self.persist = set()
            self.faketime = set()

    def ip(self, name):
        return f"10.77.{self.inst}.{OCTETS[name]}"

    def seed(self, name):
        return f"{SLOTS[name]:02x}"

    def container(self, name):
        return f"{self.net}-{name}"

    def dir(self, name):
        return os.path.join(self.run, name)

    def neighbours(self, name):
        return [other for edge in self.edges if name in edge for other in edge if other != name]

    def observers(self):
        return [name for name in self.nodes if name not in MINERS]

    def dialer(self, edge):
        """The end of an edge that dials the other. `o` keeps the original triangle's roles."""
        u, v = edge
        order = sorted(self.nodes, key=lambda name: (SLOTS[name] == 1, SLOTS[name]))
        return u if order.index(u) < order.index(v) else v

    # --- lifecycle -------------------------------------------------------------------------

    def down(self):
        for name in set(self.nodes) | set(SLOTS):
            sh("docker", "rm", "-f", self.container(name), check=False)
        sh("docker", "network", "rm", self.net, check=False)

    def up(self, spec, stack, stack_b=None, stacks=None, persist=(), faketime=()):
        """Starts `spec` with every node on `stack`, miner B on `stack_b`, and `stacks` overrides
        given as name=stack. Nodes in `persist` keep their state across restarts. Nodes in
        `faketime` read their wall-clock offset from their `faketime` knob (needs LAB_IMAGE with
        libfaketime)."""
        self.down()
        self.spec = spec
        self.nodes, self.edges = topology(spec)
        self.stacks = {name: stack for name in self.nodes}
        if stack_b:
            self.stacks["b"] = stack_b
        for override in names(stacks):
            name, _, value = override.partition("=")
            self.stacks[name] = value
        self.persist = set(persist)
        self.faketime = set(faketime)
        os.makedirs(self.run, exist_ok=True)
        json.dump({"topology": spec, "nodes": self.nodes, "edges": sorted(self.edges),
                   "stacks": self.stacks, "persist": sorted(self.persist),
                   "faketime": sorted(self.faketime)},
                  open(os.path.join(self.run, "layout.json"), "w"))
        sh("docker", "network", "create", "--subnet", f"10.77.{self.inst}.0/24", self.net)
        miner = sh(LABCTL, "address")
        template = open(os.path.join(HERE, "node.toml.tmpl")).read()
        for name in self.nodes:
            os.makedirs(os.path.join(self.dir(name), "ctl"), exist_ok=True)
            for stale in ("node.log",):
                path = os.path.join(self.dir(name), stale)
                if os.path.exists(path):
                    os.remove(path)
            sh("rm", "-rf", os.path.join(self.dir(name), "state"))
            for knob in ("window_ms", "cap_ms", "body_delay_ms", "faketime"):
                self.knob(name, knob, None)
            bootstrap = ", ".join(
                f'"{NODE_IDS[self.seed(peer)]}@{self.ip(peer)}:18234"'
                for peer in self.neighbours(name)
                if self.dialer(tuple(sorted((name, peer)))) == name)
            config = (template.replace("@STACK@", self.stacks[name]).replace("@MINER@", miner)
                      .replace("@KEY@", self.seed(name) * 32).replace("@BOOTSTRAP@", bootstrap))
            if name in self.persist:
                config = config.replace("ephemeral = true",
                                        'ephemeral = false\ncache_dir = "/lab/state"')
            if os.environ.get("LAB_DEBUG"):
                config = config.replace(
                    'filter = "warn,zakura_consensus::block::receipt=info"',
                    'filter = "info,zakura_network::zakura=debug,zakurad::commands::start::zakura=debug"')
                config = config.replace(
                    "bootstrap_peers =", 'trace_dir = "/lab/traces"\nbootstrap_peers =')
            open(os.path.join(self.dir(name), "node.toml"), "w").write(config)
            clock = []
            if name in self.faketime:
                self.knob(name, "faketime", "+0")
                # Only the wall clock moves: a monotonic jump would fire every timer at once.
                clock = ["-e", "LD_PRELOAD=/usr/lib/x86_64-linux-gnu/faketime/libfaketimeMT.so.1",
                         "-e", "FAKETIME_TIMESTAMP_FILE=/lab/ctl/faketime",
                         "-e", "FAKETIME_CACHE_DURATION=1",
                         "-e", "FAKETIME_DONT_FAKE_MONOTONIC=1"]
            sh(
                "docker", "run", "-d", "--name", self.container(name), *clock,
                "--network", self.net, "--ip", self.ip(name),
                "--cap-add", "NET_ADMIN",
                # The host is shared: keep cores 0-1 free and cap each node.
                "--cpuset-cpus", os.environ.get("LAB_CPUSET", "2-7"),
                "--cpus", os.environ.get("LAB_CPUS", "1.0"),
                "--cpu-shares", "256",
                "--memory", os.environ.get("LAB_MEMORY", "2g"),
                # As deploy/sysctl/60-zakura-network.conf sets on production nodes.
                "--sysctl", "net.ipv4.tcp_slow_start_after_idle=0",
                "-e", "CLAIM_LAB_DIR=/lab/ctl",
                "-v", f"{ZAKURAD}:/usr/local/bin/zakurad:ro",
                "-v", f"{self.dir(name)}:/lab",
                IMAGE, "sh", "-c",
                # Append, so a restart keeps the log offsets that trials measure from.
                "exec zakurad -c /lab/node.toml start >> /lab/node.log 2>&1",
            )
        for name in self.nodes:
            self.isolate(name)
        for name in self.nodes:
            self.wait_rpc(name)
        self.connect_legacy()
        # Fund the lane: the height 1 coinbase must be mature.
        for _ in range(11):
            self.rpc("a", "generate", [10])
        self.wait_same_tip(180)
        sh(LABCTL, "lane-init", f"{self.ip('a')}:18232", self.lane())
        print(f"lab {self.inst} up ({spec}) at height {self.rpc(self.nodes[-1], 'getblockcount', [])}")

    def isolate(self, name):
        """Blackholes every lab node that is not a neighbour of `name`."""
        for other in self.nodes:
            if other != name and other not in self.neighbours(name):
                sh("docker", "exec", self.container(name), "ip", "route", "replace", "blackhole",
                   self.ip(other))

    def legacy_edges(self):
        return [edge for edge in sorted(self.edges)
                if "zakura" not in (self.stacks[edge[0]], self.stacks[edge[1]])]

    def connect_legacy(self):
        """Legacy peers only meet through `addnode`: Regtest drops non-localhost initial peers."""
        for edge in self.legacy_edges():
            dialer = self.dialer(edge)
            listener = edge[0] if edge[1] == dialer else edge[1]
            try:
                self.rpc(listener, "addnode", [f"{self.ip(dialer)}:18233", "add"])
            except RuntimeError as error:
                # A second `add` is refused, but the address book still redials the peer.
                if "already present" not in str(error):
                    raise

    def restart(self, name, timeout=120):
        """Restarts one node. Its routes and shaping live in its network namespace, so both are
        rebuilt. Only nodes started with `--persist` keep their chain."""
        if name not in self.persist:
            raise RuntimeError(f"{name} has ephemeral state: start the lab with --persist {name}")
        sh("docker", "restart", "-t", "10", self.container(name))
        self.isolate(name)
        self.shape_node(name, *self.shaping)
        self.wait_rpc(name, timeout)
        if any(name in edge for edge in self.legacy_edges()):
            self.connect_legacy()

    def lane(self):
        return os.path.join(self.run, "lane-a.txt")

    # --- rpc and metrics -------------------------------------------------------------------

    def rpc(self, name, method, params, timeout=60):
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
        request = urllib.request.Request(
            f"http://{self.ip(name)}:18232",
            data=body.encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            reply = json.loads(response.read())
        if reply.get("error"):
            raise RuntimeError(f"{name} {method}: {reply['error']}")
        return reply.get("result")

    def metrics(self, name):
        """Unlabelled and labelled samples from the node's Prometheus endpoint."""
        with urllib.request.urlopen(f"http://{self.ip(name)}:19000/metrics", timeout=5) as reply:
            samples = {}
            for line in reply.read().decode().splitlines():
                if line and not line.startswith("#"):
                    key, _, value = line.rpartition(" ")
                    try:
                        samples[key] = float(value)
                    except ValueError:
                        pass
            return samples

    def metric_sum(self, name, prefix):
        """Sum of every sample of one metric, across labels."""
        try:
            samples = self.metrics(name)
        except Exception:
            return None
        return sum(value for key, value in samples.items()
                   if key == prefix or key.startswith(prefix + "{"))

    def wait_rpc(self, name, timeout=60):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                self.rpc(name, "getblockcount", [], timeout=2)
                return
            except Exception:
                time.sleep(0.5)
        raise RuntimeError(f"{name} RPC did not start: see {self.dir(name)}/node.log")

    def tip(self, name):
        return self.rpc(name, "getbestblockhash", [], timeout=10)

    def tips(self):
        return {name: self.tip(name) for name in self.nodes}

    def wait_same_tip(self, timeout):
        deadline = time.time() + timeout
        while time.time() < deadline:
            tips = set(self.tips().values())
            if len(tips) == 1:
                return tips.pop()
            time.sleep(0.2)
        raise RuntimeError(f"tips did not converge: {self.tips()}")

    # --- trial conditions ------------------------------------------------------------------

    def wait_for_host(self, max_wait=900):
        """Waits while the shared host is busy. Returns the load it started the trial at."""
        deadline = time.time() + max_wait
        while os.getloadavg()[0] > host_limit() and time.time() < deadline:
            time.sleep(5)
        return round(os.getloadavg()[0], 2)

    def throttled_ms(self):
        """CPU time the nodes' cgroups spent throttled, summed over nodes."""
        total = 0
        for name in self.nodes:
            stat = sh("docker", "exec", self.container(name), "cat", "/sys/fs/cgroup/cpu.stat",
                      check=False)
            match = re.search(r"throttled_usec (\d+)", stat)
            total += int(match.group(1)) if match else 0
        return total / 1000

    def ensure_legacy_links(self, timeout=60):
        """Every legacy-only node must hold a legacy connection before a race, or its block can
        never reach the observer. Returns (fewest legacy peers seen first, ms waited)."""
        legacy_only = [name for name in self.nodes if self.stacks[name] == "legacy"]
        if not legacy_only:
            return "", 0
        began = time.time()
        first = None
        while True:
            counts = [self.metric_sum(name, "zcash_net_peers") or 0 for name in legacy_only]
            if first is None:
                first = int(min(counts))
            if min(counts) >= 1:
                return first, round((time.time() - began) * 1000)
            if time.time() - began > timeout:
                return first, -1
            self.connect_legacy()
            time.sleep(2)

    # --- knobs and shaping -----------------------------------------------------------------

    def knob(self, name, knob, value):
        path = os.path.join(self.dir(name), "ctl", knob)
        if value is None:
            if os.path.exists(path):
                os.remove(path)
        else:
            open(path, "w").write(str(value))

    def shape(self, rtt_ms, rate_mbit, loss_pct=0.0):
        """Each node delays its egress to the other nodes by half the RTT, so every pair sees
        `rtt_ms`. Traffic to the host, which carries the controller's RPC calls, is not shaped:
        otherwise uploading a large block to `submitblock` waits on delayed ACKs."""
        self.shaping = (rtt_ms, rate_mbit, loss_pct)
        for name in self.nodes:
            self.shape_node(name, rtt_ms, rate_mbit, loss_pct)

    def shape_node(self, name, rtt_ms, rate_mbit, loss_pct=0.0):
        tc = ["docker", "exec", self.container(name), "tc"]
        sh(*tc, "qdisc", "del", "dev", "eth0", "root", check=False)
        if not (rtt_ms or rate_mbit or loss_pct):
            return
        # Band 1:1 is shaped and only the filters below send traffic to it.
        sh(*tc, "qdisc", "add", "dev", "eth0", "root", "handle", "1:", "prio", "bands", "3",
           "priomap", *["1"] * 16)
        netem = ["limit", "100000"]
        if rtt_ms:
            netem += ["delay", f"{rtt_ms / 2}ms"]
        if rate_mbit:
            netem += ["rate", f"{rate_mbit}mbit"]
        if loss_pct:
            netem += ["loss", f"{loss_pct}%"]
        sh(*tc, "qdisc", "add", "dev", "eth0", "parent", "1:1", "handle", "10:", "netem", *netem)
        # LAB_SHAPE_RPC=1 also shapes traffic to the host, as the first runs did.
        peers = [peer for peer in self.nodes if peer != name]
        targets = [self.ip(peer) for peer in peers]
        if os.environ.get("LAB_SHAPE_RPC"):
            targets.append(f"10.77.{self.inst}.1")
        for target in targets:
            sh(*tc, "filter", "add", "dev", "eth0", "parent", "1:", "protocol", "ip", "prio", "1",
               "u32", "match", "ip", "dst", f"{target}/32", "flowid", "1:1")

    # --- logs ------------------------------------------------------------------------------

    def mark(self):
        for name in self.nodes:
            path = os.path.join(self.dir(name), "node.log")
            self.offsets[name] = os.path.getsize(path) if os.path.exists(path) else 0

    def events(self, name):
        """LAB lines `name` logged since the last mark."""
        path = os.path.join(self.dir(name), "node.log")
        with open(path, errors="replace") as log:
            log.seek(self.offsets.get(name, 0))
            found = []
            for line in log:
                match = LINE.search(line)
                if match:
                    fields = dict(pair.split("=", 1) for pair in match.group(2).split())
                    fields["event"] = match.group(1)
                    found.append(fields)
            return found

    def seen(self, name, block_hash):
        """Whether `name` drew a claim for, or resolved, `block_hash` since the last mark."""
        return any(event["hash"] == block_hash for event in self.events(name))

    def resolved(self, name, block_hash):
        """The resolution of `block_hash` at `name`, with its claim's timing when it had one."""
        events = self.events(name)
        for event in events:
            if event["event"] == "claim_resolved" and event["hash"] == block_hash:
                for drawn in events:
                    if drawn["event"] == "claim_drawn" and drawn["hash"] == block_hash:
                        event["drawn_unix_ms"] = drawn["unix_ms"]
                        event["apply_ms"] = drawn.get("apply_ms", "")
                return event
        return None

    def drawn(self, name, block_hash):
        """Every claim `name` drew for `block_hash` since the last mark."""
        return [event for event in self.events(name)
                if event["event"] == "claim_drawn" and event["hash"] == block_hash]

    def wait_drawn(self, name, block_hash, timeout):
        deadline = time.time() + timeout
        while time.time() < deadline:
            found = self.drawn(name, block_hash)
            if found:
                return found[0]
            time.sleep(0.02)
        return None

    def wait_resolved(self, name, block_hash, timeout):
        deadline = time.time() + timeout
        while time.time() < deadline:
            event = self.resolved(name, block_hash)
            if event:
                return event
            time.sleep(0.02)
        return None

    # --- blocks ----------------------------------------------------------------------------

    def prepare(self, name, size, invalid=False):
        out = os.path.join(self.run, f"block-{name}.hex")
        info = json.loads(sh(LABCTL, "prepare", f"{self.ip(name)}:18232", str(size),
                             self.lane(), out, *(["--invalid"] if invalid else [])))
        info["hex"] = open(out).read()
        return info

    def submit_at(self, name, block, when, result):
        delay = when - time.time()
        if delay > 0:
            time.sleep(delay)
        result[name] = time.time()
        reply = self.rpc(name, "submitblock", [block["hex"]], timeout=120)
        if reply is not None:
            result[name + "_error"] = reply

    def reconcile(self):
        """Put every node back on one tip by extending whatever the first observer chose."""
        for name in self.nodes:
            self.knob(name, "body_delay_ms", None)
        try:
            return self.wait_same_tip(1)
        except RuntimeError:
            pass
        # Extend the observer until its chain has the most work, and hand its last blocks to any
        # node that does not follow. A miner that kept its own block against a rival header does
        # not fetch the longer chain over native sync.
        leader = self.observers()[0]
        for _ in range(4):
            self.rpc(leader, "generate", [1])
            try:
                return self.wait_same_tip(10)
            except RuntimeError:
                pass
            height = self.rpc(leader, "getblockcount", [])
            tip = self.tip(leader)
            for name in self.nodes:
                if self.tip(name) != tip:
                    for at in range(height - 5, height + 1):
                        try:
                            self.rpc(name, "submitblock",
                                     [self.rpc(leader, "getblock", [str(at), 0])])
                        except Exception:
                            pass
            try:
                return self.wait_same_tip(20)
            except RuntimeError:
                pass
        raise RuntimeError(f"tips did not converge: {self.tips()}")

    def cold_start(self, name, peer, timeout=90):
        """Restarts `name` so its next block crosses a new connection to `peer`, whose congestion
        window has not grown. Waits for `peer` to accept the new v2 connection."""
        accepted = self.metric_sum(peer, "zakura_p2p_conn_accepted") or 0
        self.restart(name)
        deadline = time.time() + timeout
        while time.time() < deadline:
            if (self.metric_sum(peer, "zakura_p2p_conn_accepted") or 0) > accepted:
                break
            time.sleep(1)
        else:
            raise RuntimeError(f"{peer} never accepted a new connection from {name}")
        self.wait_same_tip(60)


# --- trial bookkeeping ---------------------------------------------------------------------


class Conditions:
    """Host load and CPU throttling around one trial."""

    def __init__(self, lab):
        self.lab = lab
        self.load_start = lab.wait_for_host()
        self.throttled = lab.throttled_ms()

    def finish(self, row):
        load_end = round(os.getloadavg()[0], 2)
        row.update(load_start=self.load_start, load_end=load_end,
                   throttled_ms=round(self.lab.throttled_ms() - self.throttled, 1))
        return max(self.load_start, load_end) > host_limit()


CONDITION_COLUMNS = ["valid", "invalid_reason", "load_start", "load_end", "throttled_ms"]


def writer(path, columns):
    """Appends to `path`, refusing a file written with different columns."""
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    if os.path.exists(path) and os.path.getsize(path):
        with open(path, newline="") as existing:
            header = next(csv.reader(existing), None)
        if header != columns:
            raise SystemExit(f"{path} has different columns; write to a new file")
    new = not os.path.exists(path) or not os.path.getsize(path)
    handle = open(path, "a", newline="")
    out = csv.DictWriter(handle, fieldnames=columns)
    if new:
        out.writeheader()
    return handle, out


def run_cell(args, trial, emit):
    """Runs `trial(index)` until the cell has `--min-valid` valid rows (bounded by
    `--max-trials`), or exactly `--trials` rows when no minimum is set."""
    valid = total = 0
    while True:
        if args.min_valid:
            if valid >= args.min_valid or total >= args.max_trials:
                break
        elif total >= args.trials:
            break
        row = trial(total)
        emit(row)
        total += 1
        valid += row.get("valid") == 1
    if args.min_valid and valid < args.min_valid:
        print(f"short cell: {valid}/{args.min_valid} valid after {total} trials", flush=True)


def emitter(path, columns):
    handle, out = writer(path, columns)

    def emit(row):
        out.writerow(row)
        handle.flush()
        print(row, flush=True)

    return emit


# --- experiment 1: honest delivery ---------------------------------------------------------

EXP1_COLUMNS = ["exp", "stack", "rtt_ms", "rate_mbit", "loss_pct", "idle_s", "cold", "size",
                "bytes", "trial", "outcome", "age_ms", "source", "submit_to_resolved_ms",
                "window_ms", "submit_to_claim_ms", "apply_ms"] + CONDITION_COLUMNS


def delivery(lab, args, size, rtt, rate, cold, trial):
    """A releases a block, the first observer records the claim age."""
    observer = lab.observers()[0]
    lab.reconcile()
    if cold:
        lab.cold_start("a", observer)
    # After a cold start this also lets the restarted node finish its startup work, which would
    # otherwise compete with the delivery for its CPU quota.
    time.sleep(args.idle)
    conditions = Conditions(lab)
    block = lab.prepare("a", size)
    lab.mark()
    sent = {}
    lab.submit_at("a", block, time.time(), sent)
    event = lab.wait_resolved(observer, block["hash"], 60)
    row = {"exp": 1, "stack": args.label or args.stack, "rtt_ms": rtt, "rate_mbit": rate,
           "loss_pct": args.loss, "idle_s": args.idle, "cold": int(cold), "size": size,
           "bytes": block["bytes"], "trial": trial}
    if event:
        row.update(outcome=event["outcome"], age_ms=event["age_ms"], source=event["source"],
                   window_ms=event["window_ms"],
                   submit_to_resolved_ms=round(int(event["unix_ms"]) - sent["a"] * 1000))
        if "drawn_unix_ms" in event:
            row.update(apply_ms=event["apply_ms"], submit_to_claim_ms=round(
                int(event["drawn_unix_ms"]) - sent["a"] * 1000))
    else:
        row.update(outcome="timeout")
    busy = conditions.finish(row)
    reason = "a_not_seen" if not event else "host_busy" if busy else ""
    row.update(valid=int(not reason), invalid_reason=reason)
    return row


def exp1(lab, args):
    emit = emitter(args.out, EXP1_COLUMNS)
    lab.shape(args.rtt, args.rate, args.loss)
    for size in ints(args.sizes):
        run_cell(args, lambda trial: delivery(lab, args, size, args.rtt, args.rate, args.cold,
                                              trial), emit)


def grid(lab, args):
    """Experiment 1 over every rate, RTT and size: where honest blocks lose their claim."""
    emit = emitter(args.out, EXP1_COLUMNS)
    for rtt in floats(args.rtts):
        for rate in floats(args.rates):
            lab.shape(rtt, rate, args.loss)
            for size in ints(args.sizes):
                run_cell(args, lambda trial: delivery(lab, args, size, rtt, rate, args.cold,
                                                      trial), emit)


# --- races ---------------------------------------------------------------------------------

RACE_COLUMNS = ["exp", "stack", "rtt_ms", "rate_mbit", "window_ms", "cap_ms", "body_delay_ms",
                "size",
                "bytes", "offset_ms", "trial", "winner", "a_outcome", "a_age_ms", "a_order",
                "a_resolved_ms", "b_outcome", "b_age_ms", "b_order", "b_resolved_ms",
                "a_drawn_ms", "b_drawn_ms", "a_seen", "b_seen", "o_moved_ms", "o_moved_to",
                "legacy_peers", "link_wait_ms"] + CONDITION_COLUMNS


def window_knob(lab, window_ms, nodes=None):
    """Sets the deadline override. A negative value keeps the implementation's own deadline."""
    for name in nodes or [lab.observers()[0]]:
        lab.knob(name, "window_ms", None if window_ms < 0 else window_ms)


def watch_tips(lab, nodes, parent, start, stop, moved):
    """Records when each node first leaves `parent`, and for which block."""
    while not stop.is_set():
        for name in nodes:
            if name in moved:
                continue
            try:
                tip = lab.tip(name)
            except Exception:
                continue
            if tip != parent:
                moved[name] = (round((time.time() - start) * 1000), tip)
        if len(moved) == len(nodes):
            return
        stop.wait(0.2)


def release(lab, size, offset_ms, body_delay_ms, pace):
    """Common race setup: one tip, A's `size`-byte block and B's empty block on one parent.
    Returns (blocks, start, conditions, link, moved, stop, watcher) once both are submitted."""
    lab.reconcile()
    # Each trial costs every peer link about two tip announcements, and a link's Status bucket
    # refills one token per 15 s. See run_exp1.sh.
    time.sleep(pace)
    link = lab.ensure_legacy_links()
    conditions = Conditions(lab)
    lab.knob("a", "body_delay_ms", body_delay_ms)
    blocks = {"a": lab.prepare("a", size), "b": lab.prepare("b", 0)}
    lab.mark()
    sent = {}
    start = time.time() + 0.3
    moved, stop = {}, threading.Event()
    watcher = threading.Thread(target=watch_tips, args=(lab, lab.observers(),
                                                        blocks["a"]["parent"], start, stop, moved))
    threads = [
        threading.Thread(target=lab.submit_at, args=("a", blocks["a"], start, sent)),
        threading.Thread(target=lab.submit_at,
                         args=("b", blocks["b"], start + offset_ms / 1000, sent)),
    ]
    for thread in threads:
        thread.start()
    watcher.start()
    for thread in threads:
        thread.join()
    return blocks, start, conditions, link, moved, stop, watcher


def race(lab, size, offset_ms, columns_extra, pace=0, body_delay_ms=None):
    """A (size bytes) is released first, empty B `offset_ms` later. Returns the trial row."""
    observer = lab.observers()[0]
    blocks, start, conditions, link, moved, stop, watcher = release(
        lab, size, offset_ms, body_delay_ms, pace)
    block_a, block_b = blocks["a"], blocks["b"]
    event_a = lab.wait_resolved(observer, block_a["hash"], 12 + (body_delay_ms or 0) / 1000)
    event_b = lab.wait_resolved(observer, block_b["hash"], 5 if event_a else 0.1)
    time.sleep(1.0)
    stop.set()
    watcher.join()
    tip = lab.tip(observer)
    winner = "a" if tip == block_a["hash"] else "b" if tip == block_b["hash"] else "other"
    row = dict(columns_extra)
    row.update(size=size, bytes=block_a["bytes"], offset_ms=offset_ms, winner=winner,
               legacy_peers=link[0], link_wait_ms=link[1])
    for label, event in (("a", event_a), ("b", event_b)):
        if event:
            row.update({f"{label}_outcome": event["outcome"], f"{label}_age_ms": event["age_ms"],
                        f"{label}_order": event["order"],
                        f"{label}_resolved_ms": round(int(event["unix_ms"]) - start * 1000)})
            if "drawn_unix_ms" in event:
                row[f"{label}_drawn_ms"] = round(int(event["drawn_unix_ms"]) - start * 1000)
        else:
            row[f"{label}_outcome"] = "timeout"
    a_seen = lab.seen(observer, block_a["hash"])
    b_seen = lab.seen(observer, block_b["hash"])
    row.update(a_seen=int(a_seen), b_seen=int(b_seen))
    if observer in moved:
        at, to = moved[observer]
        row.update(o_moved_ms=at, o_moved_to="a" if to == block_a["hash"]
                   else "b" if to == block_b["hash"] else "other")
    busy = conditions.finish(row)
    reason = ("parent_mismatch" if block_a["parent"] != block_b["parent"]
              else "b_not_seen" if not b_seen
              else "a_not_seen" if not a_seen
              else "tip_other" if winner == "other"
              else "host_busy" if busy else "")
    row.update(valid=int(not reason), invalid_reason=reason)
    return row


def exp2(lab, args):
    """Race: full A first, empty B later, with the window on and off."""
    emit = emitter(args.out, RACE_COLUMNS)
    lab.shape(args.rtt, args.rate)
    for window in ints(args.windows):
        window_knob(lab, window)
        for size in ints(args.sizes):
            for offset in ints(args.offsets):
                run_cell(args, lambda trial: race(lab, size, offset, {
                    "exp": 2, "stack": args.label or args.stack, "rtt_ms": args.rtt,
                    "rate_mbit": args.rate, "window_ms": window, "body_delay_ms": 0,
                    "trial": trial}, pace=args.pace), emit)
    window_knob(lab, -1)


def exp3(lab, args):
    """Withholding: A announces, then sits on the body for `delay` ms."""
    args.windows = str(args.window)
    withhold(lab, args, exp=3)


def cap_knob(lab, cap_ms):
    """Replaces only the deadline's cap on every node, keeping its base and slope. -1 keeps the
    built-in cap."""
    for name in lab.nodes:
        lab.knob(name, "cap_ms", None if cap_ms < 0 else cap_ms)


def withhold(lab, args, exp=4):
    """Withholding under each deadline arm. `o_moved_ms` shows how long the observer stays on
    the old tip; comparing arms separates the rule's effect from block sync's."""
    emit = emitter(args.out, RACE_COLUMNS)
    lab.shape(args.rtt, args.rate)
    for cap in ints(args.caps):
        cap_knob(lab, cap)
        for window in ints(args.windows):
            window_knob(lab, window)
            for delay in ints(args.delays):
                for offset in ints(args.offsets):
                    run_cell(args, lambda trial: race(lab, args.size, offset, {
                        "exp": exp, "stack": args.label or args.stack, "rtt_ms": args.rtt,
                        "rate_mbit": args.rate, "window_ms": window, "cap_ms": cap,
                        "body_delay_ms": delay, "trial": trial},
                        pace=args.pace, body_delay_ms=delay), emit)
    window_knob(lab, -1)
    cap_knob(lab, -1)


# --- multi-hop and mixed adoption ----------------------------------------------------------


def multihop_columns(lab):
    per_node = []
    for name in lab.observers():
        per_node += [f"{name}_tip", f"{name}_a", f"{name}_b", f"{name}_moved_ms"]
    return (["exp", "stack", "topology", "rule_off", "cap_ms", "rtt_ms", "rate_mbit", "size",
             "offset_ms",
             "trial", "split", "majority", "a_tips", "b_tips"] + per_node
            + ["legacy_peers", "link_wait_ms"] + CONDITION_COLUMNS)


def hop_race(lab, args, size, offset_ms, rule_off, trial, cap=-1):
    """A race where every relay reports its own tip and claim outcomes."""
    blocks, start, conditions, link, moved, stop, watcher = release(
        lab, size, offset_ms, None, args.pace)
    hashes = {blocks["a"]["hash"]: "a", blocks["b"]["hash"]: "b"}
    deadline = time.time() + 30  # an 8 MB body over 4 hops can take longer than 15 s
    while time.time() < deadline:
        if all(any(lab.resolved(name, block) for block in hashes) for name in lab.observers()):
            break
        time.sleep(0.1)
    time.sleep(2.0)
    stop.set()
    watcher.join()
    row = {"exp": 5, "stack": args.label or args.stack, "topology": lab.spec,
           "rule_off": ",".join(rule_off), "cap_ms": cap, "rtt_ms": args.rtt, "rate_mbit": args.rate,
           "size": size, "offset_ms": offset_ms, "trial": trial,
           "legacy_peers": link[0], "link_wait_ms": link[1]}
    unseen = []
    for name in lab.observers():
        tip = hashes.get(lab.tip(name), "other")
        row[f"{name}_tip"] = tip
        for block, label in hashes.items():
            event = lab.resolved(name, block)
            row[f"{name}_{label}"] = (f"{event['outcome']}:{float(event['age_ms']):.0f}"
                                      if event else "seen" if lab.seen(name, block) else "")
        if not any(lab.seen(name, block) for block in hashes):
            unseen.append(name)
        if name in moved:
            row[f"{name}_moved_ms"] = moved[name][0]
    tips = [row[f"{name}_tip"] for name in lab.observers()]
    row.update(split=len(set(tips)), a_tips=tips.count("a"), b_tips=tips.count("b"),
               majority="a" if tips.count("a") > tips.count("b") else "b")
    busy = conditions.finish(row)
    # A's submission can fail outright (for example an oversized RPC request), which would
    # otherwise look like a clean B win.
    a_hash = blocks["a"]["hash"]
    a_reached = any(lab.seen(name, a_hash) or lab.resolved(name, a_hash)
                    for name in lab.observers())
    reason = ("parent_mismatch" if blocks["a"]["parent"] != blocks["b"]["parent"]
              else "a_not_seen" if not a_reached
              else f"unseen:{'+'.join(unseen)}" if unseen
              else "tip_other" if "other" in tips
              else "host_busy" if busy else "")
    row.update(valid=int(not reason), invalid_reason=reason)
    return row


def multihop(lab, args):
    """Races over the lab's topology. `--rule-off` turns the deadline to zero on some nodes, so
    their claims always expire and they rank by body arrival as before the rule."""
    emit = emitter(args.out, multihop_columns(lab))
    lab.shape(args.rtt, args.rate)
    for rule_off in [names(arm.replace("+", ",")) for arm in args.rule_off.split(";")]:
        for name in lab.nodes:
            lab.knob(name, "window_ms", 0 if name in rule_off else None)
        # Caps only matter on nodes running the rule. A cap written "f1500" is a flat deadline:
        # 1500 ms for every size, with no base or size term.
        caps = args.caps.split(",") if not set(lab.observers()) <= set(rule_off) else ["-1"]
        for cap in caps:
            flat = int(cap[1:]) if cap.startswith("f") else None
            for name in lab.nodes:
                if name not in rule_off:
                    lab.knob(name, "window_ms", flat)
            cap_knob(lab, -1 if flat is not None else int(cap))
            for size in ints(args.sizes):
                for offset in ints(args.offsets):
                    run_cell(args, lambda trial: hop_race(
                        lab, args, size, offset, rule_off, trial, cap), emit)
    for name in lab.nodes:
        lab.knob(name, "window_ms", None)
    cap_knob(lab, -1)


# --- edge cases ----------------------------------------------------------------------------

EDGE_COLUMNS = ["exp", "case", "trial", "size", "body_delay_ms", "a_drawn", "a_outcome",
                "a_age_ms", "first_drawn_to_resolved_ms", "o_tip", "detail", "expected",
                "passed"] + CONDITION_COLUMNS
EXPECTED = {
    "restart": "A resolves after O restarts; detail records whether O drew a new claim",
    "mined": "O keeps its own block and A's older claim resolves voided",
    "mined-invalid": "O's mined block fails, the watermark clears, and A's claim is used",
    "future": "O defers A's future header and draws no claim for it",
    "redelivery": "one claim for A at O, aged from that first draw",
}


def edge_trial(lab, args, case, trial):
    """One edge case. A announces and withholds its body for `--delays` ms (the first value);
    the case acts on O while A's claim is pending."""
    delay = ints(args.delays)[0]
    lab.reconcile()
    time.sleep(args.pace)
    conditions = Conditions(lab)
    block = lab.prepare("a", args.size)
    sent, detail = {}, ""
    if case == "future":
        # Regtest template times trail the wall clock by hours, so set O's clock from A's header
        # time: three hours before it, beyond the two-hour future limit.
        header_time = lab.rpc("a", "getblocktemplate", [])["curtime"]
        offset = int(header_time - time.time()) - 3 * 3600
        lab.knob("o", "faketime", f"{offset:+d}")
        time.sleep(2)
        detail = f"o_clock_offset_s={offset}"
    lab.knob("a", "body_delay_ms", delay)
    lab.mark()
    lab.submit_at("a", block, time.time(), sent)
    drawn = lab.wait_drawn("o", block["hash"], 10)
    if case == "restart" and drawn:
        restarted = time.time()
        lab.restart("o")
        detail = f"restart_ms={round((time.time() - restarted) * 1000)}"
    elif case == "mined" and drawn:
        time.sleep(0.2)
        mined = lab.rpc("o", "generate", [1])
        detail = f"mined={mined[0][:12] if mined else ''}"
    elif case == "mined-invalid" and drawn:
        time.sleep(0.2)
        bad = lab.prepare("o", 0, invalid=True)
        reply = lab.rpc("o", "submitblock", [bad["hex"]])
        detail = f"submitblock={reply}"
    event = lab.wait_resolved("o", block["hash"], delay / 1000 + 30)
    lab.knob("a", "body_delay_ms", None)
    if case == "future":
        lab.knob("o", "faketime", "+0")
    time.sleep(1.0)
    draws = lab.drawn("o", block["hash"])
    tip = lab.tip("o")
    row = {"exp": 6, "case": case, "trial": trial, "size": args.size, "body_delay_ms": delay,
           "a_drawn": len(draws), "o_tip": "a" if tip == block["hash"]
           else "parent" if tip == block["parent"] else "other",
           "detail": detail, "expected": EXPECTED[case]}
    if event:
        row.update(a_outcome=event["outcome"], a_age_ms=event["age_ms"])
        if draws:
            row["first_drawn_to_resolved_ms"] = int(event["unix_ms"]) - int(draws[0]["unix_ms"])
    else:
        row["a_outcome"] = "timeout"
    if case == "restart":
        row["detail"] += f" draws={len(draws)}"
        passed = bool(event)
    elif case == "mined":
        passed = row["a_outcome"] == "voided" and row["o_tip"] == "other"
    elif case == "mined-invalid":
        passed = row["a_outcome"] == "used" and row["o_tip"] == "a"
    elif case == "future":
        # Judged while O's clock was still back: once it is restored the header becomes valid
        # and may be claimed late, which is correct.
        passed = not drawn
    else:
        age, span = row.get("a_age_ms"), row.get("first_drawn_to_resolved_ms")
        passed = len(draws) == 1 and age not in (None, "") and span is not None \
            and abs(float(age) - span) < 50
    busy = conditions.finish(row)
    needs_draw = case not in ("future",)
    reason = ("a_not_drawn" if needs_draw and not drawn
              else "host_busy" if busy else "")
    row.update(passed=int(passed), valid=int(not reason), invalid_reason=reason)
    return row


def edge(lab, args):
    """Restart, mined-block watermark, future-timestamp and redelivery cases at O."""
    emit = emitter(args.out, EDGE_COLUMNS)
    lab.shape(args.rtt, args.rate)
    for case in names(args.cases):
        if case not in EXPECTED:
            raise SystemExit(f"unknown case {case}: one of {', '.join(EXPECTED)}")
        if case == "restart" and "o" not in lab.persist:
            raise SystemExit("restart needs the lab up with --persist o")
        if case == "future" and "o" not in lab.faketime:
            raise SystemExit("future needs the lab up with --faketime o")
        if case == "redelivery" and lab.spec != "diamond":
            raise SystemExit("redelivery needs the diamond topology")
        run_cell(args, lambda trial: edge_trial(lab, args, case, trial), emit)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["up", "down", "exp1", "grid", "exp2", "exp3",
                                            "withhold", "multihop", "edge"])
    parser.add_argument("inst")
    parser.add_argument("--topology", default="triangle")
    parser.add_argument("--stack", default="legacy")
    parser.add_argument("--stack-b", default=None)
    parser.add_argument("--stacks", default="", help="per-node overrides, e.g. r1=legacy")
    parser.add_argument("--persist", default="", help="nodes whose state survives a restart")
    parser.add_argument("--faketime", default="", help="nodes whose wall clock the lab can move")
    parser.add_argument("--cases", default="mined,mined-invalid",
                        help="edge cases: restart, mined, mined-invalid, future, redelivery")
    parser.add_argument("--sizes", default="0")
    parser.add_argument("--size", type=int, default=0)
    parser.add_argument("--rtt", type=float, default=0)
    parser.add_argument("--rate", type=float, default=0)
    parser.add_argument("--rtts", default="100")
    parser.add_argument("--rates", default="50")
    parser.add_argument("--loss", type=float, default=0)
    parser.add_argument("--idle", type=float, default=5)
    parser.add_argument("--cold", action="store_true",
                        help="restart A before each delivery, so it crosses a new connection")
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument("--min-valid", type=int, default=0)
    parser.add_argument("--max-trials", type=int, default=40)
    parser.add_argument("--offsets", default="0")
    parser.add_argument("--windows", default="-1")
    parser.add_argument("--window", type=int, default=-1)
    parser.add_argument("--rule-off", default="",
                        help="multihop arms separated by ';', each a '+'-separated node list "
                             "whose deadline is zero, e.g. ';r1+r2+r3' for rule on, then off")
    parser.add_argument("--caps", default="-1", help="deadline caps in ms; -1 keeps the built-in")
    parser.add_argument("--pace", type=float, default=35)
    parser.add_argument("--label", default=None)
    parser.add_argument("--delays", default="0")
    parser.add_argument("--out", default=os.path.join(HERE, "results", "out.csv"))
    args = parser.parse_args(argv)
    lab = Lab(args.inst)
    if args.command == "up":
        lab.up(args.topology, args.stack, args.stack_b, args.stacks, names(args.persist),
               names(args.faketime))
    elif args.command == "down":
        lab.down()
    else:
        {"exp1": exp1, "grid": grid, "exp2": exp2, "exp3": exp3, "withhold": withhold,
         "multihop": multihop, "edge": edge}[args.command](lab, args)


if __name__ == "__main__":
    sys.exit(main())
