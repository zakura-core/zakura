#!/usr/bin/env python3
"""Compare the release code of two revisions, function by function.

It checks that a change leaves the release build alone, such as a hook that
only `cfg(any(test, zakura_sim))` builds compile. For each revision it makes
a detached worktree (see `lib/revision_worktrees.py`), emits each package's
release assembly at each codegen-units setting, and compares the two sides
after normalizing what moves without a code change.

Generic code can be instantiated only in a downstream crate, which then
holds its instructions and its data: constants, strings, pointer tables
such as vtables, and panic locations. So it also builds the release
`zakurad` with every workspace crate at each setting, and compares, with
the same normalizer and masking, the binary's assembly after thin
link-time optimization, which `--emit=asm` writes for every module the
binary links. That covers all of the binary's code and data, from any
crate, including what LTO inlines across crates. Every section is compared
except debug info and unwind tables, so data in `__const`, `__cstring` and
`__DATA_CONST` on macOS, or `.rodata` and `.data.rel.ro` on Linux, is
covered, along with each item's section. It also compares the linked
binary's text section byte for byte. `--no-binary` skips the binary for
quick runs.

Both sides build one at a time at the same target path, each with its own
cache (see `side_target`), so paths that a build embeds, such as `OUT_DIR`,
match. Build metadata is made the same too (see `build_env`), never masked.

Usage:
  release-asm-diff.py --base <rev> --head <rev>
      [--package zakura-network --package zakura-rt] [--cgu 4 --cgu 1]
      [--no-binary] [--jobs <n>] [--work-dir <dir>] [--cargo <cargo>]

What the normalizer ignores, and nothing else:
  * comments, debug line directives (`.loc`, `.file`), and alignment and
    linkage directives;
  * the `.llvm.<hash>` suffix that thin LTO gives a promoted local symbol,
    whose hash covers the module's debug line info too;
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
import concurrent.futures
import contextlib
import difflib
import io
import json
import multiprocessing
import os
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
# An anonymous constant that thin LTO promoted to a global symbol.
PROMOTED_ANON = re.compile(r"^_?_anon\.[0-9a-f]+\.\d+$")
# Thin LTO's suffix on a promoted local symbol.
LLVM_SUFFIX = re.compile(r"\.llvm\.\d+\b")
# `tracing`'s event names, `event <file>:<line>`, with `.asciz`'s NUL.
EVENT_NAME = re.compile(rb"^event (.+\.rs):(\d+)(\x00?)$")
# The `Metadata` static that `tracing`'s callsite macros define, `META` inside
# `__CALLSITE`, in v0 or legacy mangling.
CALLSITE_META = re.compile(r"CALLSITE4META(?:17h[0-9a-f]{16}E)?$")

# Directives that carry no code or data: debug lines, alignment, linkage and
# object-file bookkeeping.
ALIGNMENT = (".p2align", ".align", ".balign")
DROPPED = ALIGNMENT + (
    ".loc", ".file", ".globl", ".global",
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
    as `(label, section, align, lines)`, and the names that `.set` defines.
    Each function body starts with its section directive, and each block
    keeps its own and the alignment directives before it, with the item's
    own name replaced by `SELF`, so moving code or data to another section
    or alignment is a difference. Code or data that no label precedes, such
    as a constructor table's entries, gets an empty label. Thin LTO's
    `.llvm.<hash>` suffix is dropped from labels.
    """
    kind, section = "text", ".text"
    functions, blocks, set_names = [], [], set()
    current, align = None, []
    for raw in text.splitlines():
        line = strip_comment(raw).strip()
        if not line:
            continue
        switched = section_kind(line)
        if switched:
            kind, section, current, align = switched, line, None, []
            continue
        if kind == "skip":
            continue
        if kind == "data" and line.startswith(ALIGNMENT):
            # Alignment belongs to the block that follows it.
            current = None
            align.append(line)
            continue
        if line.startswith(DROPPED):
            continue
        if line.startswith(".set"):
            set_names.add(line.split(None, 1)[1].split(",")[0].strip())
        if line.startswith((".zerofill", ".tbss", ".comm", ".lcomm")):
            fields = [field.strip() for field in line.split(None, 1)[1].split(",")]
            name = fields[2] if line.startswith(".zerofill") else fields[0]
            blocks.append((LLVM_SUFFIX.sub("", name), ".zerofill", (), [line.replace(name, "SELF")]))
            current = None
            continue
        match = LABEL_LINE.match(line)
        if match:
            label, rest = match.group(1), match.group(2).strip()
            if kind == "text" and LOCAL.match(label):
                if current is not None:
                    current.append(label + ":")
            elif kind == "text":
                current = [section.replace(label, "SELF")]
                functions.append((LLVM_SUFFIX.sub("", label), current))
            else:
                current = []
                blocks.append((LLVM_SUFFIX.sub("", label), section.replace(label, "SELF"),
                               tuple(item.replace(label, "SELF") for item in align), current))
                align = []
            if rest and current is not None:
                current.append(rest)
            continue
        if current is None:
            current = [section] if kind == "text" else []
            if kind == "text":
                functions.append(("", current))
            else:
                blocks.append(("", section, tuple(align), current))
                align = []
        current.append(line)
    return functions, blocks, set_names


class Renamer:
    """Renames local labels in one function or data block by first use.

    A label defined in a data section becomes `DATA<n>`, and its qualified
    name goes in `refs`, in order. Any other local label becomes `LOC<n>`.
    Every other symbol is kept, and goes in `symbols`, so a block's pointer
    to a global, such as an anonymous constant that thin LTO promoted, can
    still be followed.
    """

    def __init__(self, local, data, qualify):
        """Takes the file's local labels, its data labels and how to qualify one."""
        self.local, self.data, self.qualify = local, data, qualify
        self.names, self.refs, self.symbols = {}, [], set()

    def __call__(self, text):
        """Returns `text` with every local label renamed."""
        return IDENT.sub(self._one, text)

    def _one(self, match):
        """The new name for one identifier, or the identifier itself when it isn't local."""
        token = match.group(1)
        if token not in self.local:
            token = LLVM_SUFFIX.sub("", token)
            self.symbols.add(token)
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

    Items are `("bytes", b)`, `("ref", size, expr)`, `("len", d)` for a
    string's length after a pointer to it (see `Unit.finish`), or
    `("dir", text)`.
    """

    __slots__ = ("key", "section", "items", "refs", "symbols")

    def __init__(self, key, section, items, refs, symbols=()):
        """Stores the block's unit-wide key, placement, items, `(token, qualified label)` refs and global symbols.

        `section` is the block's section directive and alignment directives.
        """
        self.key, self.section, self.items, self.refs = key, section, items, refs
        self.symbols = frozenset(symbols)

    def ref(self, token):
        """The unit-wide key that `token`, such as `DATA0` or a global symbol, names, or None."""
        for name, target in self.refs:
            if name == token:
                return target
        return token if token in self.symbols else None

    def targets(self):
        """The keys of every block this block may point to: its local refs, then its global symbols."""
        return [target for _, target in self.refs] + sorted(self.symbols)

    def string(self):
        """The block's bytes when it's one string or byte run, else None."""
        if len(self.items) == 1 and self.items[0][0] == "bytes":
            return self.items[0][1]
        return None

    def shape(self):
        """The block's section and items, for comparing blocks that nothing pairs."""
        return repr((self.section, self.items))


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
        data = {label for label, _, _, _ in blocks if LOCAL.match(label)}
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
            self.functions[name or "unlabeled code in " + lines[0]].append((body, rename.refs))
        for label, section, align, lines in blocks:
            rename = Renamer(local, data, qualify)
            placement = tuple(rename(line) for line in (section,) + align)
            key = qualify(label) if label in data else label or "unlabeled data in " + section
            block = Block(key, placement, data_items(lines, rename), rename.refs, rename.symbols)
            if label in data:
                self.local[key] = block
            else:
                self.named[key].append(block)

    def finish(self):
        """Marks string lengths, then finds where `tracing` callsites keep their line.

        A pointer and length pair to a local string becomes `("ref", ...)`
        then `("len", d)`, where `d` is the string's size minus the length,
        0 or 1 for `.asciz`'s NUL. So a length that changes along with its
        string isn't compared as bytes, but one that changes alone is. The
        string's own contents are still compared through the pair.
        """
        for block in self.blocks():
            items, out = block.items, []
            for item in items:
                prev = out[-1] if out else None
                if (item[0] == "bytes" and prev and prev[0] == "ref" and prev[1] == 8
                        and len(item[1]) >= 8):
                    target = self.block(block.ref(prev[2]))
                    text = target.string() if target else None
                    length = int.from_bytes(item[1][:8], "little")
                    if text is not None and (length == len(text) or (text.endswith(b"\0") and length == len(text) - 1)):
                        out.append(("len", len(text) - length))
                        if len(item[1]) > 8:
                            out.append(("bytes", item[1][8:]))
                        continue
                out.append(item)
            block.items = tuple(out)
        self.callsite_line, self.callsite_names = find_callsite_layout(self)

    def block(self, key):
        """The block a key names: a local block, or a global one with one copy, else None."""
        if key in self.local:
            return self.local[key]
        copies = self.named.get(key, ())
        return copies[0] if len(copies) == 1 else None

    def blocks(self):
        """Every data block, named and local."""
        for copies in self.named.values():
            yield from copies
        yield from self.local.values()


def is_callsite(block):
    """Whether a block is a `tracing` callsite's `Metadata` static, by its symbol."""
    return bool(CALLSITE_META.search(block.key))


def find_callsite_layout(unit):
    """Where this unit's `tracing` callsites keep their line, from its event callsites.

    An event callsite's name is `event <file>:<line>`, so its `Some(line)`
    word can be found by value. Every event callsite must agree on that
    word's offset, which then applies to span callsites too, since they
    share the `Metadata` type. Returns `(offset, names)`: the offset, or None
    when there are no event callsites or they disagree, and the keys of the
    event name strings.
    """
    offsets, names = set(), set()
    for copies in unit.named.values():
        for block in copies:
            if not is_callsite(block):
                continue
            for target in block.targets():
                found = unit.block(target)
                text = found.string() if found else None
                match = EVENT_NAME.match(text) if text is not None else None
                if not match:
                    continue
                names.add(target)
                flat = flatten(block.items)
                want = [1, 0, 0, 0] + list(int(match.group(2)).to_bytes(4, "little"))
                found = [w for w in range(4, len(flat or []) - 3, 4) if flat[w - 4:w + 4] == want]
                offsets.update(found if len(found) == 1 else [None])
    offset = offsets.pop() if len(offsets) == 1 else None
    return offset, names


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
        self.hunks, self.texts = {}, {}

    def __call__(self, path, line):
        """The head line for `path`'s base `line`, or None when it changed or the file isn't in both."""
        if path not in self.hunks:
            self.hunks[path] = self._hunks(path)
        hunks = self.hunks[path]
        return None if hunks is None else map_line(hunks, line)

    def fits(self, path, line, column):
        """Whether `column` (1-based) is a column of base `line` of `path`, or just past its end."""
        if path not in self.texts:
            show = subprocess.run(["git", "-C", str(self.repo), "show", f"{self.base}:{path}"],
                                  stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            text = show.stdout.decode(errors="replace") if show.returncode == 0 else None
            self.texts[path] = text.split("\n") if text is not None else None
        lines = self.texts[path]
        return lines is not None and 1 <= line <= len(lines) and 1 <= column <= len(lines[line - 1]) + 1

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


def source_path(unit, key):
    """The source file path that a string block holds, or None."""
    block = unit.block(key)
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
    """Whether an anonymous block is a `core::panic::Location`: a path pointer, its length, line and column."""
    items = block.items
    return (
        (block.key in unit.local or PROMOTED_ANON.match(block.key)) and len(items) == 3
        and items[0][0] == "ref" and items[0][1] == 8 and items[1][0] == "len"
        and items[2][0] == "bytes" and len(items[2][1]) == 8
        and source_path(unit, block.ref(items[0][2])) is not None
    )


def compare_blocks(bunit, base, hunit, head, line_map):
    """Compares a data block pair, masking source lines that moved.

    Returns `(reason, masked)`: `reason` is None when they match, and
    `masked` lists the `(path, base_line, head_line, kind)` lines that
    matched only through `line_map`. Only three fields can differ: a
    `Location`'s line, a `tracing` callsite's line at its unit's learned
    offset (see `find_callsite_layout`), and the line in a callsite's
    `event <file>:<line>` name.
    """
    if base.section != head.section:
        return "placement %s -> %s" % (" ".join(base.section), " ".join(head.section)), []
    if base.items == head.items:
        return None, []
    btext, htext = base.string(), head.string()
    if btext is not None and htext is not None:
        bm, hm = EVENT_NAME.match(btext), EVENT_NAME.match(htext)
        named = base.key in bunit.callsite_names and head.key in hunit.callsite_names
        if named and bm and hm and (bm.group(1), bm.group(3)) == (hm.group(1), hm.group(3)):
            path, bline, hline = bm.group(1).decode(errors="replace"), int(bm.group(2)), int(hm.group(2))
            if line_map(path, bline) == hline:
                return None, [(path, bline, hline, "callsite name")]
            return "event name line %d -> %d of %s isn't mapped by the diff" % (bline, hline, path), []
        return "contents differ", []
    if len(base.items) != len(head.items) or any(
        b != h for b, h in zip(base.items, head.items) if not (b[0] == h[0] == "bytes" and len(b[1]) == len(h[1]))
    ):
        return "layout differs", []
    location = is_location(base, bunit) and is_location(head, hunit)
    line_offset = bunit.callsite_line if bunit.callsite_line == hunit.callsite_line else None
    callsite = is_callsite(base) and is_callsite(head) and line_offset is not None
    if not location and not callsite:
        return "contents differ", []
    paths = {source_path(bunit, target) for target in base.targets()} - {None}
    hpaths = {source_path(hunit, target) for target in head.targets()} - {None}
    if len(paths) != 1 or paths != hpaths:
        return "contents differ", []
    path = paths.pop()
    bflat, hflat = flatten(base.items), flatten(head.items)
    if bflat is None or hflat is None:
        return "contents differ", []
    masked = []
    words = sorted({i - i % 4 for i, (b, h) in enumerate(zip(bflat, hflat)) if b != h})
    for word in words:
        bword, hword = bflat[word:word + 4], hflat[word:word + 4]
        if None in bword or None in hword or len(bword) < 4:
            return "contents differ", []
        # A Location's line follows its path pointer and length, at offset
        # 16. A callsite's line is an `Option<u32>` at the learned offset,
        # after a `Some` tag of 1.
        if location:
            slot = word == 16
        else:
            slot = word == line_offset and bflat[word - 4:word] == [1, 0, 0, 0] == hflat[word - 4:word]
        if not slot:
            return "bytes differ at offset %d" % word, []
        bline, hline = int.from_bytes(bytes(bword), "little"), int.from_bytes(bytes(hword), "little")
        if line_map(path, bline) != hline:
            return "line %d -> %d of %s isn't mapped by the diff" % (bline, hline, path), []
        fits = getattr(line_map, "fits", None)
        column = int.from_bytes(bytes(bflat[20:24]), "little") if location and None not in bflat[20:24] else None
        if fits and column is not None and not fits(path, bline, column):
            return "location %s:%d:%d isn't a column of that line" % (path, bline, column), []
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
        bcopies = sorted(base.named.get(name, []), key=Block.shape)
        hcopies = sorted(head.named.get(name, []), key=Block.shape)
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
    bleft = collections.Counter(b.shape() for q, b in base.local.items() if q not in bseen)
    hleft = collections.Counter(b.shape() for q, b in head.local.items() if q not in hseen)
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


# A `git` that answers `zakurad`'s build-metadata queries
# (`crates/zakurad/build/metadata.rs`) with fixed values and passes anything
# else to the real `git`.
FAKE_GIT = """#!/bin/sh
case "$*" in
  'rev-parse --is-inside-work-tree') echo true ;;
  'rev-parse --git-dir') echo '{git_dir}' ;;
  'symbolic-ref HEAD') exit 1 ;;
  'rev-parse --abbrev-ref --symbolic-full-name HEAD') echo HEAD ;;
  'rev-parse HEAD') echo '{sha}' ;;
  'describe --always --tags --match v*.*.*') echo '{describe}' ;;
  'status --porcelain --untracked-files=no') ;;
  *) exec '{git}' "$@" ;;
esac
"""


def metadata_env(work, repo, base):
    """Build settings that give both sides the base revision's build metadata.

    `zakurad`'s build script embeds the revision's Git SHA, description and
    commit time, which would make any two commits differ. Both sides get the
    base's values instead, from a `git` placed first on `PATH` and from
    `SOURCE_DATE_EPOCH`, so the metadata is present and valid, as in a normal
    build, and the code that reads it is still compiled. No data is masked
    for it.
    """
    real = shutil.which("git")
    if real is None:
        raise rw.CommandFailed(["git"], "not on PATH")
    describe = rw.run(["git", "-C", repo, "describe", "--always", "--tags", "--match", "v*.*.*", base],
                      capture=True).strip()
    epoch = rw.run(["git", "-C", repo, "log", "-1", "--format=%ct", base], capture=True).strip()
    fake = work / "metadata-git" / "git"
    fake.parent.mkdir(parents=True, exist_ok=True)
    fake.write_text(FAKE_GIT.format(git_dir=fake.parent / "no-git-dir", sha=base, describe=describe, git=real))
    fake.chmod(0o755)
    return {"PATH": str(fake.parent) + os.pathsep + os.environ.get("PATH", ""), "SOURCE_DATE_EPOCH": epoch}


def build_env(target, inputs, extra=None):
    """The environment for a build into `target`, with `inputs` from `metadata_env` and then `extra`."""
    env = dict(inputs)
    env.update(extra or {})
    return rw.cargo_env(target, env)


# Inputs that build native code or drive builds, which the assembly doesn't
# show. A change to one means the gate can't vouch for the binary.
NATIVE_INPUTS = ["Cargo.lock", "*.c", "*.cc", "*.cpp", "*.cxx", "*.h", "*.hh", "*.hpp", "*.S",
                 ":(glob)**/build.rs", ":(glob)**/build/**"]


def native_changes(repo, base, head):
    """The files in `NATIVE_INPUTS` that differ between `base` and `head`."""
    return rw.run(["git", "-C", repo, "diff", "--name-only", base, head, "--"] + NATIVE_INPUTS,
                  capture=True).split()


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


def collect_asm(deps, prefix, out_dir):
    """Moves the `.s` files in `deps` that start with `prefix` to a fresh `out_dir`, and lists them."""
    shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True)
    for path in deps.glob(prefix + "*.s"):
        shutil.move(str(path), str(out_dir / path.name))
    return sorted(out_dir.glob("*.s"))


def emit_asm(cargo, tree, target, inputs, package, crate, cgu, out_dir):
    """Emits `package`'s release library assembly, and moves its `.s` files to `out_dir`.

    `crate` is the library's crate name, and `cgu` the codegen-units setting
    for that crate.
    """
    deps = target / "release" / "deps"
    for stale in deps.glob(crate + "-*.s"):
        stale.unlink()
    print("== %s: %s, codegen-units=%d" % (tree.name, package, cgu), flush=True)
    rw.run(
        [cargo, "rustc", "--release", "--locked", "-p", package, "--lib", "--",
         "--emit=asm,link", "-C", "codegen-units=%d" % cgu],
        cwd=tree, env=build_env(target, inputs),
    )
    return collect_asm(deps, crate + "-", out_dir)


def emit_binary(cargo, tree, target, inputs, cgu, out_dir):
    """Builds the release `zakurad` with every workspace crate at `cgu` codegen units.

    With the release profile's thin LTO, `--emit=asm` on the binary writes
    the assembly of every module after link-time optimization: all the code
    and data the binary holds, whichever crate instantiated it. Returns that
    assembly, moved to `out_dir`, and the binary's text section.
    """
    deps = target / "release" / "deps"
    for stale in deps.glob(BINARY + "-*.s"):
        stale.unlink()
    print("== %s: release %s, codegen-units=%d" % (tree.name, BINARY, cgu), flush=True)
    rw.run(
        [cargo, "rustc", "--release", "--locked", "-p", BINARY_PACKAGE, "--bin", BINARY, "--",
         "--emit=asm,link"],
        cwd=tree, env=build_env(target, inputs, {"CARGO_PROFILE_RELEASE_CODEGEN_UNITS": str(cgu)}),
    )
    text = text_section(target / "release" / BINARY)
    return collect_asm(deps, BINARY + "-", out_dir), text


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


def comparisons(asm, packages, cgus, binary):
    """The `(title, base files, head files)` comparisons to run, from emitted `asm`.

    Each of `packages` at each of `cgus`, then, with `binary`, the release
    `zakurad`'s assembly at each of `cgus`.
    """
    out = []
    names = list(packages) + ([BINARY] if binary else [])
    for name in names:
        for cgu in cgus:
            title = "%s%s codegen-units=%d" % ("binary " if name == BINARY else "", name, cgu)
            out.append((title, asm.get(("base", name, cgu), []), asm.get(("head", name, cgu), [])))
    return out


def comparison_job(item, line_map):
    """Runs one comparison, returning its exit status and what it printed."""
    title, bfiles, hfiles = item
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        status = compare_side_files(title, bfiles, hfiles, line_map)
    return status, out.getvalue()


def run_comparisons(items, line_map, jobs=1):
    """Runs each comparison, `jobs` at a time, prints them in order, and returns the worst exit status.

    Each comparison parses its own two sides, so they run in forked worker
    processes when `jobs` is more than 1.
    """
    if jobs > 1 and len(items) > 1:
        context = multiprocessing.get_context("fork")
        with concurrent.futures.ProcessPoolExecutor(min(jobs, len(items)), mp_context=context) as pool:
            results = list(pool.map(comparison_job, items, [line_map] * len(items)))
    else:
        results = [comparison_job(item, line_map) for item in items]
    status = EXIT_SAME
    for result, text in results:
        sys.stdout.write(text)
        status = max(status, result)
    sys.stdout.flush()
    return status


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
                        help="skip the release zakurad builds and their comparison, for quick runs")
    parser.add_argument("--jobs", type=int, default=min(8, os.cpu_count() or 1),
                        help="comparisons to run at once (default: up to 8)")
    parser.add_argument("--work-dir", help="keep worktrees' build caches here for reuse (default: a temporary directory)")
    parser.add_argument("--cargo", default="cargo", help="the cargo command (default: cargo)")
    parser.add_argument("--repo", default=".", help="the repository (default: the current one)")
    return parser.parse_args(argv)


def main(argv=None):
    """Runs the comparison, and returns the exit status."""
    args = parse_args(argv)
    packages = args.package or DEFAULT_PACKAGES
    cgus = args.cgu or DEFAULT_CGUS
    try:
        repo = rw.repo_root(args.repo)
        base, head = rw.resolve(repo, args.base), rw.resolve(repo, args.head)
        print("base %s, head %s" % (base, head), flush=True)
        line_map = GitLineMap(repo, base, head)
        native = native_changes(repo, base, head)
        with rw.Worktrees(repo, base, head, args.work_dir) as trees:
            inputs = metadata_env(trees.work, repo, base)
            asm, texts = {}, {}
            for side in ("base", "head"):
                tree = trees.tree(side)
                crates = lib_crates(read_metadata(args.cargo, tree))
                with side_target(trees.work, side) as target:
                    for package in packages:
                        crate = crates.get(package, package.replace("-", "_"))
                        for cgu in cgus:
                            out = trees.work / "asm" / side / ("%s-cgu%d" % (package, cgu))
                            asm[side, package, cgu] = emit_asm(
                                args.cargo, tree, target, inputs, package, crate, cgu, out)
                    if not args.no_binary:
                        for cgu in cgus:
                            out = trees.work / "asm" / side / ("%s-cgu%d" % (BINARY, cgu))
                            asm[side, BINARY, cgu], texts[side, cgu] = emit_binary(
                                args.cargo, tree, target, inputs, cgu, out)
            status = run_comparisons(comparisons(asm, packages, cgus, not args.no_binary), line_map, args.jobs)
            for cgu in cgus:
                if args.no_binary:
                    print("%s text section: skipped (--no-binary)" % BINARY)
                    break
                difference = compare_text(texts["base", cgu], texts["head", cgu])
                print("%s text section codegen-units=%d: %s" % (
                    BINARY, cgu, difference or "identical (%d bytes)" % len(texts["base", cgu])))
                if difference:
                    status = max(status, EXIT_DIFFERS)
            if native:
                print("native or build inputs changed, which the assembly can't show: %s" % " ".join(native))
                status = max(status, EXIT_DIFFERS)
    except rw.CommandFailed as error:
        print("error: %s" % error, file=sys.stderr)
        return rw.EXIT_COMMAND_FAILED
    except (OSError, ValueError) as error:
        print("error: %s" % error, file=sys.stderr)
        return rw.EXIT_COMMAND_FAILED
    return status


if __name__ == "__main__":
    sys.exit(main())
