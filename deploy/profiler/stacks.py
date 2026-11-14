"""Call stacks sampled by `perf`, as trees of CPU time.

A sample is one thread caught on a CPU: its thread name, the time, and the chain of
functions it was in. This module parses `perf script` output into samples, cleans the
symbol names, adds samples to a tree whose nodes are functions and whose values are CPU
microseconds, merges trees from several time windows, and cuts a tree down to one view
(for example block verification) for the flame graph.

A tree is made of `Node`s, each with:

- `self_us`, CPU time sampled in exactly this function at this position in the stack,
- `block_us`, the part of it sampled while a block was being processed,
- `children`, which maps a callee's name to its node.

The children of the root are thread groups, so the same function called from two kinds
of thread stays separate until a view merges it.
"""

import collections
import re
import subprocess

# perf prints user addresses below this and kernel addresses above it on x86-64.
KERNEL_ADDRESS_START = 0xFFFF800000000000
# perf prints this address for the frame where the copied stack ran out.
TRUNCATED_ADDRESS = 0xFFFFFFFFFFFFFFFF
TRUNCATED_FRAME = "[stack truncated]"
UNKNOWN_FRAME = "[unknown]"
KERNEL_PREFIX = "[k] "
THREAD_PREFIX = "thread: "
SMALL_FRAMES = "(small frames)"

SAMPLE_HEADER = re.compile(r"^(\S.*?)\s+(\d+)\s+(\d+\.\d+):")
CRATE_HASH = re.compile(r"\[[0-9a-f]{16}\]")
LEGACY_HASH = re.compile(r"::h[0-9a-f]{16}$")
LLVM_SUFFIX = re.compile(r"\.llvm\.\d+$")
# The trailing index of a pooled thread, or the separator left when Linux cut the name.
THREAD_INDEX = re.compile(r"[\s:\-]*\d*$")

# Thread names as Linux reports them (cut to 15 bytes), mapped to a readable group.
THREAD_GROUPS = {
    "tokio-rt-worker": "tokio (async tasks and blocking pool)",
    "tokio-runtime-w": "tokio (async tasks and blocking pool)",
    "rayon": "rayon (verification pool)",
    "zakurad": "zakurad (main thread and block writer)",
    "parent-input-re": "parent-input-read (state reads)",
    "commit-compute": "commit-compute (tree updates)",
    "rocksdb:low": "rocksdb (background)",
    "rocksdb:high": "rocksdb (background)",
    "metrics-endpoin": "metrics endpoint",
    "zakura-log": "log writer",
}

# What part of the node a frame belongs to. A frame's category colours it, names the
# one-click view it appears in, and decides which band of the CPU timeline a sample
# counts toward. The two count differently: a view keeps everything under the
# outermost frame of its category, while a sample's band is the category of its
# innermost categorized frame, so a view's total can exceed its band's.
# Where two patterns start at the same place in a name, the earlier entry wins, so
# narrower patterns come first.
CATEGORIES = (
    {
        "id": "propagation",
        "label": "Block propagation",
        "patterns": (
            "zakurad::components::sync::gossip::",
            "zakura_network::peer::block_gossip::",
            "send_block_advert",
            "route_block_broadcast",
            "legacy_gossip::LegacyGossipAdapter",
            "legacy_gossip::send_to_sessions",
            "legacy_gossip::try_send_advertise_block",
            "block_sync::regulated::status_sender",
            "zakura_state::service::block_announcements::",
        ),
    },
    {
        "id": "verification",
        "label": "Block verification",
        "patterns": (
            "zakura_consensus::",
            "zakura_script::",
            "zcash_script::",
            "tower_batch_control::",
            "halo2_proofs::",
            "halo2_gadgets::",
            "orchard::",
            "sapling_crypto::",
            "bellman::",
            "bls12_381::",
            "pasta_curves::",
            "jubjub::",
            "redjubjub::",
            "reddsa::",
            "ed25519_zebra::",
            "zcash_proofs::",
            "zcash_primitives::",
            "zcash_note_encryption::",
        ),
    },
    {
        "id": "commit",
        "label": "State commit",
        "patterns": (
            "zakura_state::",
            "zakura_chain::parallel::tree::",
            "incrementalmerkletree::",
            "bridgetree::",
            "rocksdb::",
            "librocksdb_sys::",
            "rocksdb_",
        ),
    },
    {
        "id": "arrival",
        "label": "Block arrival and sync",
        "patterns": (
            "zakurad::components::inbound::",
            "zakurad::components::sync::",
            "zakura_network::zakura::block_sync::",
            "zakura_network::zakura::header_sync::",
            "zakura_header_chain::",
            "block_sync_driver::",
        ),
    },
    {
        "id": "mempool",
        "label": "Mempool",
        "patterns": ("zakurad::components::mempool::",),
    },
    {
        "id": "rpc",
        "label": "RPC and mining",
        "patterns": ("zakura_rpc::", "jsonrpsee", "tonic::"),
    },
    {
        "id": "network",
        "label": "Other networking",
        "patterns": (
            "zakura_network::",
            "quinn::",
            "quinn_proto::",
            "quinn_udp::",
            "rustls::",
            "ring::",
            "aws_lc_rs::",
            "tokio_util::codec::",
        ),
    },
)
KERNEL_CATEGORY = {"id": "kernel", "label": "Kernel"}
OTHER_CATEGORY = {"id": "other", "label": "Runtime and other"}
# The order categories are listed and stacked in on the page.
LEGEND_ORDER = (
    "verification", "propagation", "arrival", "rpc", "mempool", "network", "commit",
    "kernel", "other",
)  # fmt: skip
ALL_VIEW = "all"


def category_ids():
    """Every category id, in legend order."""
    return list(LEGEND_ORDER)


def _compile_patterns():
    groups, owners = [], []
    for category in CATEGORIES:
        for pattern in category["patterns"]:
            groups.append("(" + re.escape(pattern) + ")")
            owners.append(category["id"])
    return re.compile("|".join(groups)), owners


_PATTERNS, _PATTERN_OWNERS = _compile_patterns()


def frame_category(name):
    """The category of one frame: the one whose pattern appears first in its name.

    A Rust symbol names its own function first and the types it is generic over
    after, so `<inbound::Downloads<.., zakura_consensus::Request, ..>>::poll` is block
    arrival even though it mentions the verifier. Taking the leftmost match keeps a
    wrapper from being filed under whatever it happens to carry.
    """
    match = _PATTERNS.search(name)
    if match is not None:
        return _PATTERN_OWNERS[match.lastindex - 1]
    if name.startswith(KERNEL_PREFIX):
        return KERNEL_CATEGORY["id"]
    return OTHER_CATEGORY["id"]


class Categorizer:
    """Caches each frame's category, and names the category of a whole stack."""

    def __init__(self):
        self._by_frame = {}

    def frame(self, name):
        category = self._by_frame.get(name)
        if category is None:
            category = self._by_frame[name] = frame_category(name)
        return category

    def stack(self, frames):
        """The category of the innermost frame that belongs to a node subsystem.

        `frames` runs from the outermost caller to the innermost callee. A stack with
        no subsystem frame counts as kernel time when it ended in the kernel, and as
        runtime time otherwise.
        """
        for name in reversed(frames):
            category = self.frame(name)
            if category not in (KERNEL_CATEGORY["id"], OTHER_CATEGORY["id"]):
                return category
        if frames and frames[-1].startswith(KERNEL_PREFIX):
            return KERNEL_CATEGORY["id"]
        return OTHER_CATEGORY["id"]


def thread_group(comm):
    """The readable group for a Linux thread name such as `rayon 3`."""
    base = THREAD_INDEX.sub("", comm.strip()) or comm.strip()
    return THREAD_GROUPS.get(base, base)


def parse_perf_script(lines):
    """Yield `(comm, time, raw_frames)` for each sample in `perf script` output.

    Expects the fields `comm,tid,time,ip,sym,dso`. `time` is in seconds on the clock perf
    recorded with. `raw_frames` runs from the innermost callee to the outermost caller,
    as perf prints it; each entry is `(address, symbol)`.
    """
    comm = None
    when = 0.0
    frames = []
    for line in lines:
        header = SAMPLE_HEADER.match(line)
        if header:
            if comm is not None:
                yield comm, when, frames
            comm, when, frames = header.group(1), float(header.group(3)), []
            continue
        if comm is None:
            continue
        parts = line.split(None, 1)
        if not parts:
            continue
        try:
            address = int(parts[0], 16)
        except ValueError:
            continue
        frames.append((address, frame_symbol(parts[1] if len(parts) > 1 else "")))
    if comm is not None:
        yield comm, when, frames


def frame_symbol(text):
    """The symbol of one `perf script` frame, given the text after its address.

    perf prints `symbol (path/of/library)`. A frame perf could not name is reported
    as its library instead, which tells libc from the node binary.
    """
    text = text.strip()
    symbol, separator, library = text.rpartition(" (")
    if not separator or not library.endswith(")"):
        return text or UNKNOWN_FRAME
    if symbol != UNKNOWN_FRAME:
        return symbol
    library = library[:-1].rsplit("/", 1)[-1]
    return UNKNOWN_FRAME if library in ("", UNKNOWN_FRAME) else f"[{library}]"


def clean_symbol(demangled):
    """Drop the parts of a demangled Rust symbol that differ between builds."""
    name = LLVM_SUFFIX.sub("", demangled.strip())
    name = CRATE_HASH.sub("", name)
    return LEGACY_HASH.sub("", name)


def run_cxxfilt(mangled):
    """Demangle `mangled` names with binutils' `c++filt`, one output per input."""
    result = subprocess.run(
        ["c++filt"],
        input="\n".join(mangled) + "\n",
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )
    return result.stdout.split("\n")[: len(mangled)]


class SymbolNames:
    """Readable names for the raw symbols perf prints, demangled once and remembered."""

    def __init__(self, demangle=run_cxxfilt):
        self._demangle = demangle
        self._names = {}

    def resolve(self, raw_symbols):
        """Make sure every symbol in `raw_symbols` has a readable name."""
        new = sorted(s for s in set(raw_symbols) if s not in self._names)
        mangled = [s for s in new if s.startswith(("_R", "_ZN"))]
        demangled = {}
        if mangled:
            stripped = [LLVM_SUFFIX.sub("", s) for s in mangled]
            try:
                demangled = dict(zip(mangled, self._demangle(stripped)))
            except (OSError, subprocess.SubprocessError):
                # Without the demangler the profile is still usable; names stay raw
                # and are retried on the next window.
                new = [s for s in new if s not in mangled]
        for symbol in new:
            self._names[symbol] = clean_symbol(demangled.get(symbol) or symbol)

    def frame_name(self, address, symbol):
        """The display name for one frame of a sample."""
        if address == TRUNCATED_ADDRESS:
            return TRUNCATED_FRAME
        name = self._names.get(symbol, symbol)
        if address >= KERNEL_ADDRESS_START:
            return KERNEL_PREFIX + name
        return name


class Node:
    """One function at one position in a call stack, and the functions it called."""

    __slots__ = ("self_us", "block_us", "children")

    def __init__(self, self_us=0, block_us=0):
        self.self_us = self_us
        self.block_us = block_us
        self.children = {}

    def child(self, name):
        """The callee called `name`, added empty if it is not there yet."""
        node = self.children.get(name)
        if node is None:
            node = self.children[name] = Node()
        return node

    def own_us(self, block_only=False):
        """Self time: all of it, or only the part sampled while a block was processed."""
        return self.block_us if block_only else self.self_us


def new_tree():
    """An empty tree root."""
    return Node()


def walk(root):
    """Yield `(parent, name, node)` for every node below `root`, each parent before its children."""
    stack = [(root, name, child) for name, child in root.children.items()]
    while stack:
        parent, name, node = stack.pop()
        yield parent, name, node
        stack.extend((node, child_name, child) for child_name, child in node.children.items())


def add_stack(root, path, cpu_us, block_us=0):
    """Add CPU time sampled at the end of `path`, which runs from the outermost caller."""
    node = root
    for name in path:
        node = node.child(name)
    node.self_us += cpu_us
    node.block_us += block_us


def merge_into(target, source):
    """Add every node of `source` to `target`."""
    stack = [(target, source)]
    while stack:
        into, node = stack.pop()
        into.self_us += node.self_us
        into.block_us += node.block_us
        stack.extend((into.child(name), child) for name, child in node.children.items())


def total_us(node, block_only=False):
    """CPU time in `node` and everything below it."""
    return node.own_us(block_only) + sum(child.own_us(block_only) for _, _, child in walk(node))


class FlatTree:
    """A tree written as a table of distinct names plus one list per node field.

    Nodes are listed parents first. `n` is each node's index into `names` and `p` its
    parent's position, `-1` for children of the root; the remaining lists hold the
    values named when the table is made. Storing each function name once keeps both the
    files on disk and the page's responses small.
    """

    def __init__(self, *value_keys):
        self.names = []
        self._name_ids = {}
        self.columns = {"n": [], "p": [], **{key: [] for key in value_keys}}

    def add(self, parent, name, **values):
        """Append one node under the node at position `parent`; returns its position."""
        name_id = self._name_ids.get(name)
        if name_id is None:
            name_id = self._name_ids[name] = len(self.names)
            self.names.append(name)
        self.columns["n"].append(name_id)
        self.columns["p"].append(parent)
        for key, value in values.items():
            self.columns[key].append(value)
        return len(self.columns["n"]) - 1

    def document(self):
        return {"names": self.names, **self.columns}


def encode_tree(root):
    """A JSON-ready form of a stored tree: a `FlatTree` with self time `s` and block time `b`."""
    flat = FlatTree("s", "b")
    position = {id(root): -1}
    for parent, name, node in walk(root):
        position[id(node)] = flat.add(position[id(parent)], name, s=node.self_us, b=node.block_us)
    return flat.document()


def decode_tree(doc):
    """The tree stored by `encode_tree`."""
    root = Node()
    names = doc["names"]
    nodes = []
    for name_id, parent, self_us, block_us in zip(doc["n"], doc["p"], doc["s"], doc["b"]):
        node = Node(self_us, block_us)
        owner = root if parent < 0 else nodes[parent]
        owner.children[names[name_id]] = node
        nodes.append(node)
    return root


def view_ids():
    """The views a flame graph can be asked for: the whole process, then each subsystem."""
    subsystems = {c["id"] for c in CATEGORIES}
    return [ALL_VIEW] + [c for c in LEGEND_ORDER if c in subsystems]


def focus_tree(root, matches):
    """A tree of only the work under frames for which `matches(name)` is true.

    Each subtree rooted at the outermost matching frame is kept and merged with the
    others by function name, so work that runs under many different callers (async
    tasks, thread pools) shows up as one tower per entry point. Thread groups are
    dropped.
    """
    focused = Node()
    verdicts = {}
    stack = list(root.children.values())
    while stack:
        node = stack.pop()
        for name, child in node.children.items():
            verdict = verdicts.get(name)
            if verdict is None:
                verdict = verdicts[name] = bool(matches(name))
            if verdict:
                merge_into(focused.child(name), child)
            else:
                stack.append(child)
    return focused


def view_tree(root, view, categorizer):
    """The tree for a named view: the whole process, or one category's work."""
    if view == ALL_VIEW:
        return root
    return focus_tree(root, lambda name: categorizer.frame(name) == view)


def function_tree(root, text):
    """The tree of everything under frames whose name contains `text`."""
    return focus_tree(root, lambda name: text in name)


def flame_document(root, block_only=False, min_share=0.0002, top=40):
    """The flame graph sent to the page.

    Values are CPU microseconds. Frames below `min_share` of the total are folded into
    one `(small frames)` child per parent, so the page stays small while every parent
    keeps its full width. `top` lists the functions with the most self time, summed
    over every place they appear.
    """
    # Each node's time including its callees; children are summed before their parent.
    inclusive = {}
    for node in reversed([root, *(node for _, _, node in walk(root))]):
        inclusive[id(node)] = node.own_us(block_only) + sum(
            inclusive[id(child)] for child in node.children.values()
        )
    total = inclusive[id(root)]
    threshold = max(1, total * min_share)

    flat = FlatTree("v", "s")
    self_by_name = collections.Counter()
    stack = [(-1, root)]
    while stack:
        parent_position, parent = stack.pop()
        small = 0
        children = sorted(
            parent.children.items(), key=lambda item: inclusive[id(item[1])], reverse=True
        )
        for name, child in children:
            value = inclusive[id(child)]
            if value <= 0:
                continue
            self_by_name[name] += child.own_us(block_only)
            if value < threshold:
                small += value
                # Self time below the fold still counts toward the function totals.
                for _, inner_name, inner in walk(child):
                    self_by_name[inner_name] += inner.own_us(block_only)
                continue
            position = flat.add(parent_position, name, v=value, s=child.own_us(block_only))
            stack.append((position, child))
        if small:
            flat.add(parent_position, SMALL_FRAMES, v=small, s=small)

    ranked = sorted(self_by_name.items(), key=lambda item: item[1], reverse=True)
    return {
        "total_us": total,
        **flat.document(),
        "top": [[name, self_us] for name, self_us in ranked[:top] if self_us > 0],
    }
