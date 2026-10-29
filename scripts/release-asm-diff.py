#!/usr/bin/env python3
"""Compare the release code of two revisions, function by function.

It checks that a change leaves the release build alone, such as a hook that
only `cfg(any(test, zakura_sim))` builds compile. For each revision it makes
a detached worktree (see `lib/revision_worktrees.py`), emits each package's
release assembly at each codegen-units setting, and compares the two sides
after normalizing what moves without a code change.

Generic code can be instantiated only in a downstream crate, which then
holds its instructions and its data: constants, strings, pointer tables
such as vtables, and panic locations. So it also emits and compares, with
the same normalizer and masking, the assembly of every library that
`zakurad` links and that depends on a compared package, at each setting.
Every section the assembly defines is compared except debug info and
unwind tables, so this covers `__const`, `__cstring` and `__DATA_CONST`
data on macOS, or `.rodata` and `.data.rel.ro` on Linux. Last it builds
the release `zakurad` for each and compares its text section, which also
covers what link-time optimization inlines across crates. `--no-binary`
skips the downstream packages and `zakurad` for quick runs.

Both sides build one at a time at the same target path, each with its own
cache (see `side_target`), so paths that a build embeds, such as `OUT_DIR`,
match. Build metadata is made the same
too (see `build_env`), never masked.

Usage:
  release-asm-diff.py --base <rev> --head <rev>
      [--package zakura-network --package zakura-rt] [--cgu 4 --cgu 1]
      [--no-binary] [--work-dir <dir>] [--cargo <cargo>]

What the normalizer ignores, and nothing else:
  * comments, debug line directives (`.loc`, `.file`), and alignment and
    linkage directives;
  * the names of local labels and anonymous constants. Each function and
    data block renames them in order of first use, so each instruction
    stays tied to the constant it reads. A constant read at the same place
    on both sides is paired with its counterpart, and the pair's contents
    are compared too, so a changed constant fails.

The only masked difference is a source line in release data that moved
because a line was added or removed above it: the line of a
`core::panic::Location`, and the line and `event <file>:<line>` name of a
`tracing` callsite. One is masked only when `git diff base head` maps the
base line to the head line through unchanged lines of that file, so the
source line itself didn't change or move. Every masked line is printed.

Exit status: 0 when everything matches; 1 when any function or data block
differs or is on one side only, or the `zakurad` text section differs; 2
when a side of any comparison has no functions; 3 when a command fails.
"""

import argparse
import collections
import contextlib
import difflib
import json
import re
import shutil
import struct
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "lib"))
import revision_worktrees as rw  # noqa: E402

DEFAULT_PACKAGES = ["zakura-network", "zakura-rt"]
DEFAULT_CGUS = [4, 1]
BINARY_PACKAGE = "zakura"
BINARY = "zakurad"

EXIT_SAME, EXIT_DIFFERS, EXIT_EMPTY = 0, 1, 2

# How many differences to print per comparison before summarizing the rest.
MAX_PRINTED = 40

# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

LABEL_LINE = re.compile(r'^("(?:[^"\\]|\\.)+"|[A-Za-z_.$][\w.$]*):(.*)$')
IDENT = re.compile(r'(?<![\w.$"])(\.?[A-Za-z_$][\w.$]*)')
STRING = re.compile(r'"((?:[^"\\]|\\.)*)"')
# Assembler-local labels: Mach-O's `L` and `l` prefixes, ELF's `.L`, and
# Mach-O's exception tables.
LOCAL = re.compile(r"^(?:\.L|L|l|GCC_except_table\d)")
# `tracing`'s event names, `event <file>:<line>`, with `.asciz`'s NUL.
EVENT_NAME = re.compile(rb"^event (.+\.rs):(\d+)(\x00?)$")

# Directives that carry no code or data: debug lines, alignment, linkage and
# object-file bookkeeping.
DROPPED = (
    ".loc", ".file", ".p2align", ".align", ".balign", ".globl", ".global",
    ".private_extern", ".weak_definition", ".weak_def_can_be_hidden",
    ".weak_reference", ".alt_entry", ".build_version", ".macosx_version_min",
    ".subsections_via_symbols", ".type", ".size", ".local", ".hidden",
    ".protected", ".no_dead_strip", ".addrsig", ".addrsig_sym", ".ident",
)
DATA_SIZES = {
    ".byte": 1, ".short": 2, ".2byte": 2, ".hword": 2, ".long": 4, ".4byte": 4,
    ".word": 4, ".int": 4, ".quad": 8, ".8byte": 8, ".xword": 8,
}
STRING_DIRECTIVES = {".ascii": b"", ".asciz": b"\0", ".string": b"\0"}
SKIPPED_SEGMENTS = ("__DWARF", "__LD")
SKIPPED_ELF_PREFIXES = (".debug", ".note", ".comment", ".llvm_addrsig", ".eh_frame")
ESCAPES = {"n": 10, "t": 9, "r": 13, "b": 8, "f": 12, "v": 11, "a": 7, '"': 34, "\\": 92}


def strip_comment(line):
    """Drops an assembler comment (`;`, `//`, or `#` before a space) outside string literals."""
    quoted = escaped = False
    for i, ch in enumerate(line):
        if quoted:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                quoted = False
        elif ch == '"':
            quoted = True
        elif ch == ";" or line.startswith("//", i):
            return line[:i]
        elif ch == "#" and (i + 1 == len(line) or line[i + 1] in " \t"):
            return line[:i]
    return line


def decode_string(literal):
    """Decodes the inside of an assembler string literal to bytes."""
    out = bytearray()
    i = 0
    while i < len(literal):
        ch = literal[i]
        if ch != "\\":
            out += ch.encode()
            i += 1
            continue
        nxt = literal[i + 1]
        if nxt in "01234567":
            j = i + 1
            while j < len(literal) and j < i + 4 and literal[j] in "01234567":
                j += 1
            out.append(int(literal[i + 1:j], 8) & 0xFF)
            i = j
        elif nxt == "x":
            j = i + 2
            while j < len(literal) and literal[j] in "0123456789abcdefABCDEF":
                j += 1
            out.append(int(literal[i + 2:j], 16) & 0xFF)
            i = j
        else:
            out.append(ESCAPES.get(nxt, ord(nxt)))
            i += 2
    return bytes(out)


def parse_int(text):
    """The integer `text` spells, or None when it's a symbolic expression."""
    try:
        return int(text, 0)
    except ValueError:
        return None


def section_kind(line):
    """Whether a section directive switches to `text`, `data` or a `skip` section, or None."""
    if line == ".text":
        return "text"
    if line in (".data", ".const", ".const_data", ".cstring", ".bss", ".rodata", ".literal4",
                ".literal8", ".literal16"):
        return "data"
    if not line.startswith(".section"):
        return None
    name = line.split(None, 1)[1] if " " in line or "\t" in line else ""
    parts = [part.strip().strip('"') for part in name.split(",")]
    if parts[0].startswith("__") and len(parts) > 1:
        if parts[0] in SKIPPED_SEGMENTS or parts[1] in ("__eh_frame", "__compact_unwind"):
            return "skip"
        return "text" if (parts[0], parts[1]) == ("__TEXT", "__text") else "data"
    if parts[0].startswith(SKIPPED_ELF_PREFIXES):
        return "skip"
    return "text" if parts[0] == ".text" or parts[0].startswith(".text.") else "data"


def split_file(text):
    """Splits one `.s` file into functions and data blocks.

    Returns `(functions, blocks, set_names)`: functions as `(name, lines)`,
    with local code labels kept in the body as `label:` lines, data blocks
    as `(label, lines)`, and the names that `.set` defines.
    """
    kind = "text"
    functions, blocks, set_names = [], [], set()
    current = None
    for raw in text.splitlines():
        line = strip_comment(raw).strip()
        if not line:
            continue
        switched = section_kind(line)
        if switched:
            kind, current = switched, None
            continue
        if kind == "skip" or line.startswith(DROPPED):
            continue
        if line.startswith(".set"):
            set_names.add(line.split(None, 1)[1].split(",")[0].strip())
        if line.startswith((".zerofill", ".tbss", ".comm", ".lcomm")):
            fields = [field.strip() for field in line.split(None, 1)[1].split(",")]
            name = fields[2] if line.startswith(".zerofill") else fields[0]
            blocks.append((name, [line.replace(name, "SELF")]))
            current = None
            continue
        match = LABEL_LINE.match(line)
        if match:
            label, rest = match.group(1), match.group(2).strip()
            if kind == "text" and LOCAL.match(label):
                if current is not None:
                    current.append(label + ":")
            else:
                current = []
                (functions if kind == "text" else blocks).append((label, current))
            if rest and current is not None:
                current.append(rest)
            continue
        if current is not None:
            current.append(line)
    return functions, blocks, set_names


class Renamer:
    """Renames local labels in one function or data block by first use.

    A label defined in a data section becomes `DATA<n>`, and its qualified
    name goes in `refs`, in order. Any other local label becomes `LOC<n>`.
    """

    def __init__(self, local, data, qualify):
        """Takes the file's local labels, its data labels and how to qualify one."""
        self.local, self.data, self.qualify = local, data, qualify
        self.names, self.refs = {}, []

    def __call__(self, text):
        """Returns `text` with every local label renamed."""
        return IDENT.sub(self._one, text)

    def _one(self, match):
        """The new name for one identifier, or the identifier itself when it isn't local."""
        token = match.group(1)
        if token not in self.local:
            return token
        if token not in self.names:
            if token in self.data:
                self.names[token] = "DATA%d" % len(self.names)
                self.refs.append((self.names[token], self.qualify(token)))
            else:
                self.names[token] = "LOC%d" % len(self.names)
        return self.names[token]


class Block:
    """One data block: its normalized items and the local data it refers to.

    Items are `("bytes", b)`, `("ref", size, expr)`, `("len",)` for a
    string's length after a pointer to it, or `("dir", text)`.
    """

    __slots__ = ("label", "items", "refs")

    def __init__(self, label, items, refs):
        """Stores the block's label, items and `(token, qualified label)` refs."""
        self.label, self.items, self.refs = label, items, refs

    def ref(self, token):
        """The qualified label that `token`, such as `DATA0`, names, or None."""
        for name, target in self.refs:
            if name == token:
                return target
        return None

    def string(self):
        """The block's bytes when it's one string or byte run, else None."""
        if len(self.items) == 1 and self.items[0][0] == "bytes":
            return self.items[0][1]
        return None


def int_bytes(value, size):
    """`value` as `size` little-endian bytes, two's complement for negatives."""
    return (value & ((1 << (8 * size)) - 1)).to_bytes(size, "little")


def leb128(value, signed):
    """`value` in (s)LEB128."""
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        done = (value == 0 and not byte & 0x40) or (value == -1 and byte & 0x40) if signed else value == 0
        out.append(byte if done else byte | 0x80)
        if done:
            return bytes(out)


def split_operands(text):
    """Splits a directive's operands on top-level commas."""
    parts, depth, start = [], 0, 0
    for i, ch in enumerate(text):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(text[start:i].strip())
            start = i + 1
    parts.append(text[start:].strip())
    return parts


def data_items(lines, rename):
    """Turns a data block's lines into items, with adjacent literal bytes merged."""
    items = []

    def add_bytes(raw):
        """Appends literal bytes, merging them into a preceding bytes item."""
        if items and items[-1][0] == "bytes":
            items[-1] = ("bytes", items[-1][1] + raw)
        else:
            items.append(("bytes", raw))

    for line in lines:
        directive, _, operands = line.partition(" ")
        directive, operands = directive.strip(), operands.strip()
        if "\t" in directive:
            directive, _, more = directive.partition("\t")
            operands = (more + " " + operands).strip()
        if directive in STRING_DIRECTIVES:
            for literal in STRING.findall(operands):
                add_bytes(decode_string(literal) + STRING_DIRECTIVES[directive])
        elif directive in DATA_SIZES:
            size = DATA_SIZES[directive]
            for operand in split_operands(operands):
                value = parse_int(operand)
                if value is None:
                    items.append(("ref", size, rename(operand)))
                else:
                    add_bytes(int_bytes(value, size))
        elif directive in (".space", ".zero", ".skip"):
            fields = split_operands(operands)
            count, fill = parse_int(fields[0]), parse_int(fields[1]) if len(fields) > 1 else 0
            if count is None or fill is None:
                items.append(("dir", rename(line)))
            else:
                add_bytes(bytes([fill & 0xFF]) * count)
        elif directive in (".uleb128", ".sleb128"):
            value = parse_int(operands)
            if value is None:
                items.append(("ref", directive, rename(operands)))
            else:
                add_bytes(leb128(value, directive == ".sleb128"))
        else:
            items.append(("dir", rename(line)))
    return items


class Unit:
    """One package's assembly at one codegen setting, across all its files.

    `functions` and `named` map a global name to its normalized copies (one
    per codegen unit that emits it). `local` maps a file-qualified local data
    label to its block.
    """

    def __init__(self):
        """Starts empty."""
        self.functions = collections.defaultdict(list)
        self.named = collections.defaultdict(list)
        self.local = {}

    def add_file(self, index, text):
        """Parses one `.s` file's text into the unit."""
        functions, blocks, set_names = split_file(text)
        data = {label for label, _ in blocks if LOCAL.match(label)}
        local = set(data) | {n for n in set_names if LOCAL.match(n)}
        for _, lines in functions:
            for line in lines:
                if line.endswith(":") and LOCAL.match(line[:-1]):
                    local.add(line[:-1])

        def qualify(label):
            """Makes a local label unique across the unit's files."""
            return "%d:%s" % (index, label)

        for name, lines in functions:
            rename = Renamer(local, data, qualify)
            body = tuple(rename(line) for line in lines)
            self.functions[name].append((body, rename.refs))
        for label, lines in blocks:
            rename = Renamer(local, data, qualify)
            block = Block(label, data_items(lines, rename), rename.refs)
            if label in data:
                self.local[qualify(label)] = block
            else:
                self.named[label].append(block)

    def finish(self):
        """Marks each string length that follows a pointer to the string.

        A pointer and length pair to a local string becomes `("ref", ...)`
        then `("len",)`, so the length isn't compared as bytes. The string's
        own contents are still compared through the pair.
        """
        for block in self.blocks():
            items, out = block.items, []
            for item in items:
                prev = out[-1] if out else None
                if (item[0] == "bytes" and prev and prev[0] == "ref" and prev[1] == 8
                        and len(item[1]) >= 8):
                    target = self.local.get(block.ref(prev[2]))
                    text = target.string() if target else None
                    length = int.from_bytes(item[1][:8], "little")
                    if text is not None and (length == len(text) or (text.endswith(b"\0") and length == len(text) - 1)):
                        out.append(("len",))
                        if len(item[1]) > 8:
                            out.append(("bytes", item[1][8:]))
                        continue
                out.append(item)
            block.items = tuple(out)

    def blocks(self):
        """Every data block, named and local."""
        for copies in self.named.values():
            yield from copies
        yield from self.local.values()


def load_unit(paths):
    """Parses the `.s` files at `paths` into one `Unit`."""
    unit = Unit()
    for index, path in enumerate(sorted(paths)):
        unit.add_file(index, Path(path).read_text(errors="surrogateescape"))
    unit.finish()
    return unit


def load_texts(texts):
    """Parses `.s` file texts into one `Unit`, for tests."""
    unit = Unit()
    for index, text in enumerate(texts):
        unit.add_file(index, text)
    unit.finish()
    return unit


# ---------------------------------------------------------------------------
# Line mapping
# ---------------------------------------------------------------------------

HUNK = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@", re.M)


def parse_hunks(diff_text):
    """Reads a `-U0` diff's hunk headers as `(old_start, old_count, new_start, new_count)`."""
    return [
        (int(a), int(b) if b else 1, int(c), int(d) if d else 1)
        for a, b, c, d in HUNK.findall(diff_text)
    ]


def map_line(hunks, line):
    """The head line that base `line` became, or None when a hunk changed or removed it."""
    offset = 0
    for old_start, old_count, new_start, new_count in hunks:
        if old_count == 0:
            # A pure insertion after line `old_start`.
            if old_start >= line:
                break
            offset += new_count
            continue
        if line < old_start:
            break
        if line < old_start + old_count:
            return None
        offset += new_count - old_count
    return line + offset


class GitLineMap:
    """Maps a base line of a repository file to its head line through `git diff`."""

    def __init__(self, repo, base, head):
        """Takes the repository and the two commits."""
        self.repo, self.base, self.head = repo, base, head
        self.hunks = {}

    def __call__(self, path, line):
        """The head line for `path`'s base `line`, or None when it changed or the file isn't in both."""
        if path not in self.hunks:
            self.hunks[path] = self._hunks(path)
        hunks = self.hunks[path]
        return None if hunks is None else map_line(hunks, line)

    def _hunks(self, path):
        """The diff hunks for a relative repository path in both commits, else None."""
        if path.startswith("/") or ".." in Path(path).parts:
            return None
        for rev in (self.base, self.head):
            probe = subprocess.run(
                ["git", "-C", str(self.repo), "cat-file", "-e", f"{rev}:{path}"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            if probe.returncode != 0:
                return None
        diff = rw.run(
            ["git", "-C", self.repo, "diff", "-U0", "--no-ext-diff", "--no-color", "--no-renames",
             self.base, self.head, "--", path],
            capture=True,
        )
        return parse_hunks(diff)


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------


class Report:
    """The result of comparing two units."""

    def __init__(self):
        """Starts with no differences and nothing masked."""
        self.differences = []
        self.masked = set()
        self.functions = 0
        self.blocks = 0

    def differ(self, text):
        """Records one difference."""
        self.differences.append(text)


def source_path(unit, qualified):
    """The source file path that a local string block holds, or None."""
    block = unit.local.get(qualified)
    text = block.string() if block else None
    if text is None:
        return None
    text = text[:-1] if text.endswith(b"\0") else text
    if not text.endswith(b".rs") or b"\n" in text or b"\0" in text:
        return None
    try:
        return text.decode()
    except UnicodeDecodeError:
        return None


def flatten(items):
    """The block's bytes by offset, with None for symbolic bytes, or None if a size is unknown."""
    flat = []
    for item in items:
        if item[0] == "bytes":
            flat.extend(item[1])
        elif item[0] == "ref" and isinstance(item[1], int):
            flat.extend([None] * item[1])
        elif item[0] == "len":
            flat.extend([None] * 8)
        else:
            return None
    return flat


def is_location(block, unit):
    """Whether the block starts like a `core::panic::Location`: a path pointer, its length, line, column."""
    items = block.items
    return (
        len(items) >= 3 and items[0][0] == "ref" and items[0][1] == 8 and items[1] == ("len",)
        and items[2][0] == "bytes" and len(items[2][1]) >= 8
        and source_path(unit, block.ref(items[0][2])) is not None
    )


def compare_blocks(bunit, base, hunit, head, line_map):
    """Compares a data block pair, masking moved source lines.

    Returns `(reason, masked)`: `reason` is None when they match, and
    `masked` lists the `(path, base_line, head_line)` lines that matched
    only through `line_map`.
    """
    if base.items == head.items:
        return None, []
    btext, htext = base.string(), head.string()
    if btext is not None and htext is not None:
        bm, hm = EVENT_NAME.match(btext), EVENT_NAME.match(htext)
        if bm and hm and (bm.group(1), bm.group(3)) == (hm.group(1), hm.group(3)):
            path, bline, hline = bm.group(1).decode(errors="replace"), int(bm.group(2)), int(hm.group(2))
            if line_map(path, bline) == hline:
                return None, [(path, bline, hline, "callsite name")]
            return "event name line %d -> %d of %s isn't mapped by the diff" % (bline, hline, path), []
        return "contents differ", []
    if len(base.items) != len(head.items) or any(
        b != h for b, h in zip(base.items, head.items) if not (b[0] == h[0] == "bytes" and len(b[1]) == len(h[1]))
    ):
        return "layout differs", []
    paths = {source_path(bunit, target) for _, target in base.refs} - {None}
    hpaths = {source_path(hunit, target) for _, target in head.refs} - {None}
    if len(paths) != 1 or paths != hpaths:
        return "contents differ", []
    path = paths.pop()
    bflat, hflat = flatten(base.items), flatten(head.items)
    if bflat is None or hflat is None:
        return "contents differ", []
    location = is_location(base, bunit) and is_location(head, hunit)
    masked = []
    words = sorted({i - i % 4 for i, (b, h) in enumerate(zip(bflat, hflat)) if b != h})
    for word in words:
        bword, hword = bflat[word:word + 4], hflat[word:word + 4]
        if None in bword or None in hword or len(bword) < 4:
            return "contents differ", []
        # A Location's line follows its path pointer and length, at offset
        # 16. A callsite's line is an `Option<u32>`, so a word after a `Some`
        # tag of 1.
        if location:
            slot = word == 16
        else:
            slot = word >= 4 and bflat[word - 4:word] == [1, 0, 0, 0] == hflat[word - 4:word]
        if not slot:
            return "bytes differ at offset %d" % word, []
        bline, hline = int.from_bytes(bytes(bword), "little"), int.from_bytes(bytes(hword), "little")
        if line_map(path, bline) != hline:
            return "line %d -> %d of %s isn't mapped by the diff" % (bline, hline, path), []
        masked.append((path, bline, hline, "location" if location else "callsite"))
    return None, masked


def compare_units(base, head, line_map):
    """Compares two units: functions, named data, then every local block they reach."""
    report = Report()
    pairs = collections.deque()
    for name in sorted(set(base.functions) | set(head.functions)):
        bcopies = sorted(base.functions.get(name, []), key=lambda copy: copy[0])
        hcopies = sorted(head.functions.get(name, []), key=lambda copy: copy[0])
        report.functions += 1
        if not hcopies:
            report.differ("function only in base: %s" % name)
        elif not bcopies:
            report.differ("function only in head: %s" % name)
        elif [c[0] for c in bcopies] != [c[0] for c in hcopies]:
            detail = ""
            if len(bcopies) == len(hcopies):
                for (bbody, _), (hbody, _) in zip(bcopies, hcopies):
                    if bbody != hbody:
                        detail = "\n".join(
                            "      " + line for line in list(
                                difflib.unified_diff(bbody, hbody, "base", "head", lineterm="", n=2)
                            )[2:14]
                        )
                        break
            report.differ("function differs: %s%s" % (name, "\n" + detail if detail else ""))
        else:
            for (_, brefs), (_, hrefs) in zip(bcopies, hcopies):
                pairs.extend((b[1], h[1], name) for b, h in zip(brefs, hrefs))
    for name in sorted(set(base.named) | set(head.named)):
        bcopies = sorted(base.named.get(name, []), key=lambda block: repr(block.items))
        hcopies = sorted(head.named.get(name, []), key=lambda block: repr(block.items))
        report.blocks += 1
        if len(bcopies) != len(hcopies):
            report.differ("data %s: %d copies in base, %d in head" % (name, len(bcopies), len(hcopies)))
            continue
        for bblock, hblock in zip(bcopies, hcopies):
            reason, masked = compare_blocks(base, bblock, head, hblock, line_map)
            if reason:
                report.differ("data differs: %s (%s)" % (name, reason))
                break
            report.masked.update(masked)
            pairs.extend((b[1], h[1], name) for b, h in zip(bblock.refs, hblock.refs))
    seen, bseen, hseen = set(), set(), set()
    while pairs:
        bq, hq, via = pairs.popleft()
        if (bq, hq) in seen:
            continue
        seen.add((bq, hq))
        bseen.add(bq)
        hseen.add(hq)
        report.blocks += 1
        bblock, hblock = base.local.get(bq), head.local.get(hq)
        if bblock is None or hblock is None:
            report.differ("constant read by %s has no block on one side" % via)
            continue
        reason, masked = compare_blocks(base, bblock, head, hblock, line_map)
        if reason:
            report.differ("constant %s read by %s differs from head's %s (%s)" % (bq, via, hq, reason))
            continue
        report.masked.update(masked)
        pairs.extend((b[1], h[1], via) for b, h in zip(bblock.refs, hblock.refs))
    bleft = collections.Counter(repr(b.items) for q, b in base.local.items() if q not in bseen)
    hleft = collections.Counter(repr(b.items) for q, b in head.local.items() if q not in hseen)
    for items, count in (bleft - hleft).items():
        report.differ("unreferenced constant only in base (x%d): %s" % (count, items[:120]))
    for items, count in (hleft - bleft).items():
        report.differ("unreferenced constant only in head (x%d): %s" % (count, items[:120]))
    return report


# ---------------------------------------------------------------------------
# Binary text section
# ---------------------------------------------------------------------------


def text_section(path):
    """The bytes of a 64-bit little-endian Mach-O `__TEXT,__text` or ELF `.text` section."""
    data = Path(path).read_bytes()
    if data[:4] == b"\xcf\xfa\xed\xfe":
        (ncmds,) = struct.unpack_from("<I", data, 16)
        offset = 32
        for _ in range(ncmds):
            cmd, size = struct.unpack_from("<II", data, offset)
            if cmd == 0x19:  # LC_SEGMENT_64
                (nsects,) = struct.unpack_from("<I", data, offset + 64)
                for j in range(nsects):
                    sect = offset + 72 + 80 * j
                    names = (data[sect:sect + 16].rstrip(b"\0"), data[sect + 16:sect + 32].rstrip(b"\0"))
                    if names == (b"__text", b"__TEXT"):
                        _, length = struct.unpack_from("<QQ", data, sect + 32)
                        (start,) = struct.unpack_from("<I", data, sect + 48)
                        return data[start:start + length]
            offset += size
    elif data[:6] == b"\x7fELF\x02\x01":
        (shoff,) = struct.unpack_from("<Q", data, 0x28)
        entsize, count, strndx = struct.unpack_from("<HHH", data, 0x3A)

        def header(i):
            """Section header `i`'s name offset, file offset and size."""
            base = shoff + i * entsize
            (name,) = struct.unpack_from("<I", data, base)
            start, length = struct.unpack_from("<QQ", data, base + 24)
            return name, start, length

        _, strings, _ = header(strndx)
        for i in range(count):
            name, start, length = header(i)
            if data[strings + name:data.index(b"\0", strings + name)] == b".text":
                return data[start:start + length]
    raise ValueError("no text section found in %s" % path)


def compare_text(base, head):
    """None when two text sections match, else a description of the first difference."""
    if base == head:
        return None
    first = next((i for i, (b, h) in enumerate(zip(base, head)) if b != h), min(len(base), len(head)))
    return "%d bytes in base, %d in head, first difference at offset %#x" % (len(base), len(head), first)


# ---------------------------------------------------------------------------
# Building
# ---------------------------------------------------------------------------


def build_env(tree, target):
    """The environment for a build of `tree`, with build metadata made the same on both sides.

    `zakurad`'s build script embeds the revision's Git SHA, description and
    commit time (`crates/zakurad/build/metadata.rs`), which would make any
    two commits differ. Git can't find the repository from inside `tree`
    when `tree` is a ceiling directory, so the script records no Git
    metadata on either side, as for a source archive build. No data is
    masked for it.
    """
    return rw.cargo_env(target, {"GIT_CEILING_DIRECTORIES": str(tree), "SOURCE_DATE_EPOCH": "0"})


@contextlib.contextmanager
def side_target(work, side):
    """Moves `side`'s cached target directory to `<work>/target` while it builds, and back after.

    Both sides build at the same path, so paths a build embeds, such as
    `OUT_DIR` in generated code's panic locations, match. Each side keeps its
    own cache, so neither side ever counts the other's artifacts as fresh.
    """
    active, saved = work / "target", work / ("%s-target" % side)
    if active.exists():
        # An interrupted run left it, and which side built it is unknown.
        shutil.rmtree(active)
    if saved.exists():
        saved.rename(active)
    try:
        yield active
    finally:
        if active.exists():
            active.rename(saved)


def emit_asm(cargo, tree, target, package, crate, cgu, out_dir):
    """Emits `package`'s release library assembly, and moves its `.s` files to `out_dir`.

    `crate` is the library's crate name, and `cgu` the codegen-units setting.
    """
    deps = target / "release" / "deps"
    for stale in deps.glob(crate + "-*.s"):
        stale.unlink()
    print("== %s: %s, codegen-units=%d" % (tree.name, package, cgu), flush=True)
    rw.run(
        [cargo, "rustc", "--release", "--locked", "-p", package, "--lib", "--",
         "--emit=asm,link", "-C", "codegen-units=%d" % cgu],
        cwd=tree, env=build_env(tree, target),
    )
    shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True)
    for path in deps.glob(crate + "-*.s"):
        shutil.move(str(path), str(out_dir / path.name))
    return sorted(out_dir.glob("*.s"))


def downstream_packages(metadata, packages):
    """The library packages that `zakurad` links and that depend on any of `packages`.

    Reads `cargo metadata --no-deps` output. Generic code from `packages` can
    be instantiated in these, with its constants, strings and panic
    locations, so their assembly is compared too. Only normal dependencies
    count, so test-only and simulator crates are left out.
    """
    workspace = {package["name"]: package for package in metadata["packages"]}
    deps = {
        name: {dep["name"] for dep in package["dependencies"]
               if dep["name"] in workspace and dep.get("kind") is None}
        for name, package in workspace.items()
    }
    linked, todo = set(), [BINARY_PACKAGE]
    while todo:
        name = todo.pop()
        if name in workspace and name not in linked:
            linked.add(name)
            todo.extend(deps[name])

    def reaches(name, seen):
        """Whether `name` depends, directly or not, on any of `packages`."""
        if name in seen:
            return False
        seen.add(name)
        return any(dep in packages or reaches(dep, seen) for dep in deps[name])

    return sorted(
        name for name in linked
        if name not in packages and reaches(name, set())
        and any("lib" in target["kind"] for target in workspace[name]["targets"])
    )


def read_metadata(cargo, tree):
    """`cargo metadata --no-deps` for the workspace at `tree`."""
    return json.loads(rw.run(
        [cargo, "metadata", "--format-version", "1", "--no-deps", "--locked"], cwd=tree, capture=True))


def lib_crates(metadata):
    """Each workspace package's library crate name, such as `zakurad` for `zakura`."""
    return {
        package["name"]: target["name"].replace("-", "_")
        for package in metadata["packages"]
        for target in package["targets"]
        if "lib" in target["kind"]
    }


def comparisons(asm, packages, cgus, downstream):
    """The `(title, base files, head files)` comparisons to run, from emitted `asm`.

    Each of `packages`, then each `downstream` package, at each of `cgus`. A
    downstream package that one side doesn't have gets None on that side.
    """
    out = []
    for kind, names in (("", packages), ("downstream ", downstream)):
        for package in names:
            for cgu in cgus:
                out.append(("%s%s codegen-units=%d" % (kind, package, cgu),
                            asm.get(("base", package, cgu)), asm.get(("head", package, cgu))))
    return out


def run_comparisons(items, line_map):
    """Runs and prints each comparison, returning the worst exit status."""
    status = EXIT_SAME
    for title, bfiles, hfiles in items:
        if bfiles is None or hfiles is None:
            print("%s: package only in %s" % (title, "head" if bfiles is None else "base"))
            status = max(status, EXIT_DIFFERS)
            continue
        status = max(status, compare_side_files(title, bfiles, hfiles, line_map))
    return status


def build_binary(cargo, tree, target):
    """Builds the release `zakurad` in `tree`, and returns its path."""
    print("== %s: release %s" % (tree.name, BINARY), flush=True)
    rw.run(
        [cargo, "build", "--release", "--locked", "-p", BINARY_PACKAGE, "--bin", BINARY],
        cwd=tree, env=build_env(tree, target),
    )
    return target / "release" / BINARY


def print_report(title, report):
    """Prints one comparison's differences and masked lines, and returns its exit status."""
    state = "differs" if report.differences else "identical"
    print("%s: %d functions, %d data blocks compared, %d lines masked: %s"
          % (title, report.functions, report.blocks, len(report.masked), state))
    for path, bline, hline, what in sorted(report.masked):
        print("  masked %s %s:%d -> %d" % (what, path, bline, hline))
    for text in report.differences[:MAX_PRINTED]:
        print("  " + text)
    if len(report.differences) > MAX_PRINTED:
        print("  ... and %d more differences" % (len(report.differences) - MAX_PRINTED))
    return EXIT_DIFFERS if report.differences else EXIT_SAME


def compare_side_files(title, bfiles, hfiles, line_map):
    """Loads and compares one package and setting, returning its exit status."""
    if not bfiles or not hfiles:
        print("%s: no assembly on one side (base %d files, head %d)" % (title, len(bfiles), len(hfiles)))
        return EXIT_EMPTY
    return compare_loaded(title, load_unit(bfiles), load_unit(hfiles), line_map)


def compare_loaded(title, base, head, line_map):
    """Compares two loaded units and prints the result, returning its exit status."""
    if not base.functions or not head.functions:
        print("%s: no functions on one side" % title)
        return EXIT_EMPTY
    return print_report(title, compare_units(base, head, line_map))


def parse_args(argv):
    """Parses the command line."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--base", required=True, help="the base revision")
    parser.add_argument("--head", required=True, help="the head revision")
    parser.add_argument("--package", action="append", help="a library package (default: %s)"
                        % " ".join(DEFAULT_PACKAGES))
    parser.add_argument("--cgu", action="append", type=int, help="a codegen-units setting (default: 4 and 1)")
    parser.add_argument("--no-binary", action="store_true",
                        help="skip the downstream packages and the release zakurad build, for quick runs")
    parser.add_argument("--work-dir", help="keep worktrees' build caches here for reuse (default: a temporary directory)")
    parser.add_argument("--cargo", default="cargo", help="the cargo command (default: cargo)")
    parser.add_argument("--repo", default=".", help="the repository (default: the current one)")
    return parser.parse_args(argv)


def main(argv=None):
    """Runs the comparison, and returns the exit status."""
    args = parse_args(argv)
    packages = args.package or DEFAULT_PACKAGES
    cgus = args.cgu or DEFAULT_CGUS
    status = EXIT_SAME
    try:
        repo = rw.repo_root(args.repo)
        base, head = rw.resolve(repo, args.base), rw.resolve(repo, args.head)
        print("base %s, head %s" % (base, head), flush=True)
        line_map = GitLineMap(repo, base, head)
        with rw.Worktrees(repo, base, head, args.work_dir) as trees:
            asm, downstream, texts = {}, set(), {}
            for side in ("base", "head"):
                tree = trees.tree(side)
                metadata = read_metadata(args.cargo, tree)
                crates = lib_crates(metadata)
                linked = [] if args.no_binary else downstream_packages(metadata, set(packages))
                downstream.update(linked)
                with side_target(trees.work, side) as target:
                    for package in list(packages) + linked:
                        for cgu in cgus:
                            out = trees.work / "asm" / side / ("%s-cgu%d" % (package, cgu))
                            crate = crates.get(package, package.replace("-", "_"))
                            asm[side, package, cgu] = emit_asm(
                                args.cargo, tree, target, package, crate, cgu, out)
                    if not args.no_binary:
                        texts[side] = text_section(build_binary(args.cargo, tree, target))
            if not args.no_binary:
                print("downstream packages: %s" % (" ".join(sorted(downstream)) or "none"), flush=True)
            status = run_comparisons(comparisons(asm, packages, cgus, sorted(downstream)), line_map)
            if not args.no_binary:
                difference = compare_text(texts["base"], texts["head"])
                print("%s text section: %s" % (BINARY, difference or "identical (%d bytes)" % len(texts["base"])))
                if difference:
                    status = max(status, EXIT_DIFFERS)
            else:
                print("%s text section: skipped (--no-binary)" % BINARY)
    except rw.CommandFailed as error:
        print("error: %s" % error, file=sys.stderr)
        return rw.EXIT_COMMAND_FAILED
    except (OSError, ValueError) as error:
        print("error: %s" % error, file=sys.stderr)
        return rw.EXIT_COMMAND_FAILED
    return status


if __name__ == "__main__":
    sys.exit(main())
