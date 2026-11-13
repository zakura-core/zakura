"""Call stacks sampled by `perf`, as trees of CPU time.

A sample is one thread caught on a CPU: its thread name, the time, and the chain of
functions it was in. This module parses `perf script` output into samples, cleans the
symbol names, adds samples to a tree whose nodes are functions and whose values are CPU
microseconds, merges trees from several time windows, and cuts a tree down to one view
(for example block verification) for the flame graph.

A tree node is the list `[self_us, self_block_us, children]`:

- `self_us` is CPU time sampled in exactly this function at this position in the stack,
- `self_block_us` is the part of it sampled while a block was being processed,
- `children` maps a callee's name to its node.

The children of the root are thread groups, so the same function called from two kinds
of thread stays separate until a view merges it.
"""

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
# one-click view it appears in, and (for the innermost such frame of a sample) decides
# which band of the CPU timeline the sample counts toward, so the three cannot disagree.
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


def new_tree():
    """An empty tree root."""
    return [0, 0, {}]


def add_stack(root, path, cpu_us, block_us=0):
    """Add CPU time sampled at the end of `path`, which runs from the outermost caller."""
    node = root
    for name in path:
        children = node[2]
        child = children.get(name)
        if child is None:
            child = children[name] = [0, 0, {}]
        node = child
    node[0] += cpu_us
    node[1] += block_us


def merge_into(target, source):
    """Add every node of `source` to `target`."""
    stack = [(target, source)]
    while stack:
        into, node = stack.pop()
        into[0] += node[0]
        into[1] += node[1]
        into_children = into[2]
        for name, child in node[2].items():
            match = into_children.get(name)
            if match is None:
                match = into_children[name] = [0, 0, {}]
            stack.append((match, child))


def total_us(node, block_only=False):
    """CPU time in `node` and everything it called."""
    index = 1 if block_only else 0
    total = 0
    stack = [node]
    while stack:
        current = stack.pop()
        total += current[index]
        stack.extend(current[2].values())
    return total


def encode_tree(root):
    """A JSON-ready form of a stored tree: a name table and parallel node arrays.

    Nodes are listed parents first. `p` is each node's parent index, `-1` for children
    of the root.
    """
    names, name_ids = [], {}
    n, p, s, b = [], [], [], []
    stack = [(-1, name, child) for name, child in root[2].items()]
    while stack:
        parent, name, node = stack.pop()
        name_id = name_ids.get(name)
        if name_id is None:
            name_id = name_ids[name] = len(names)
            names.append(name)
        index = len(n)
        n.append(name_id)
        p.append(parent)
        s.append(node[0])
        b.append(node[1])
        stack.extend((index, child_name, child) for child_name, child in node[2].items())
    return {"names": names, "n": n, "p": p, "s": s, "b": b}


def decode_tree(doc):
    """The tree stored by `encode_tree`."""
    root = new_tree()
    names = doc["names"]
    nodes = []
    for name_id, parent, self_us, block_us in zip(doc["n"], doc["p"], doc["s"], doc["b"]):
        node = [self_us, block_us, {}]
        owner = root if parent < 0 else nodes[parent]
        owner[2][names[name_id]] = node
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
    focused = new_tree()
    verdicts = {}
    stack = list(root[2].values())
    while stack:
        node = stack.pop()
        for name, child in node[2].items():
            verdict = verdicts.get(name)
            if verdict is None:
                verdict = verdicts[name] = bool(matches(name))
            if verdict:
                match = focused[2].get(name)
                if match is None:
                    match = focused[2][name] = [0, 0, {}]
                merge_into(match, child)
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
    index = 1 if block_only else 0
    inclusive = {}

    def measure(node):
        # Iterative post-order: a deep stack must not hit Python's recursion limit.
        order, pending = [], [node]
        while pending:
            current = pending.pop()
            order.append(current)
            pending.extend(current[2].values())
        for current in reversed(order):
            inclusive[id(current)] = current[index] + sum(
                inclusive[id(child)] for child in current[2].values()
            )

    measure(root)
    total = inclusive[id(root)]
    threshold = max(1, total * min_share)

    names, name_ids = [], {}
    n, p, v, s = [], [], [], []
    self_by_name = {}

    def name_id_of(name):
        name_id = name_ids.get(name)
        if name_id is None:
            name_id = name_ids[name] = len(names)
            names.append(name)
        return name_id

    def emit(parent, name, value, self_us):
        n.append(name_id_of(name))
        p.append(parent)
        v.append(value)
        s.append(self_us)
        return len(n) - 1

    stack = [(-1, root)]
    while stack:
        parent_index, parent = stack.pop()
        small = 0
        children = sorted(
            parent[2].items(), key=lambda item: inclusive[id(item[1])], reverse=True
        )
        for name, child in children:
            value = inclusive[id(child)]
            if value <= 0:
                continue
            self_by_name[name] = self_by_name.get(name, 0) + child[index]
            if value < threshold:
                small += value
                # Self time below the fold still counts toward the function totals.
                pending = list(child[2].items())
                while pending:
                    inner_name, inner = pending.pop()
                    self_by_name[inner_name] = self_by_name.get(inner_name, 0) + inner[index]
                    pending.extend(inner[2].items())
                continue
            stack.append((emit(parent_index, name, value, child[index]), child))
        if small:
            emit(parent_index, SMALL_FRAMES, small, small)

    ranked = sorted(self_by_name.items(), key=lambda item: item[1], reverse=True)
    return {
        "total_us": total,
        "names": names,
        "n": n,
        "p": p,
        "v": v,
        "s": s,
        "top": [[name, self_us] for name, self_us in ranked[:top] if self_us > 0],
    }
