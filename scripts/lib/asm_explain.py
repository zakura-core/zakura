"""Explain where two revisions' release assembly differs.

A diagnostic for `release-asm-diff.py`, which decides pass or fail by
comparing bytes. When the bytes differ, this says which function or data
block did. It splits rustc assembly into functions and data blocks, renames
local labels and anonymous constants by first use, so each instruction
stays tied to the constant it reads, and pairs each constant with its
counterpart by where it's read. Comments, debug line directives, linkage
directives, function alignment and thin LTO's `.llvm.<hash>` symbol
suffixes are ignored. It never decides the gate's result, so what it
ignores can't hide a change.
"""

import collections
import difflib
import re
from pathlib import Path

# How many differences to print per comparison before summarizing the rest.
MAX_PRINTED = 40

# Parsing
# ---------------------------------------------------------------------------

LABEL_LINE = re.compile(r'^("(?:[^"\\]|\\.)+"|[A-Za-z_.$][\w.$]*):(.*)$')
IDENT = re.compile(r'(?<![\w.$"])(\.?[A-Za-z_$][\w.$]*)')
STRING = re.compile(r'"((?:[^"\\]|\\.)*)"')
# Assembler-local labels: Mach-O's `L` and `l` prefixes, ELF's `.L`, and
# Mach-O's exception tables.
LOCAL = re.compile(r"^(?:\.L|L|l|GCC_except_table\d)")
# Thin LTO's suffix on a promoted local symbol.
LLVM_SUFFIX = re.compile(r"\.llvm\.\d+\b")

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
    Other symbols keep their names, without thin LTO's suffix.
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
            return LLVM_SUFFIX.sub("", token)
        if token not in self.names:
            if token in self.data:
                self.names[token] = "DATA%d" % len(self.names)
                self.refs.append((self.names[token], self.qualify(token)))
            else:
                self.names[token] = "LOC%d" % len(self.names)
        return self.names[token]


class Block:
    """One data block: its normalized items and the local data it refers to.

    Items are `("bytes", b)`, `("ref", size, expr)` or `("dir", text)`.
    """

    __slots__ = ("key", "section", "items", "refs")

    def __init__(self, key, section, items, refs):
        """Stores the block's unit-wide key, placement, items and `(token, qualified label)` refs.

        `section` is the block's section directive and alignment directives.
        """
        self.key, self.section, self.items, self.refs = key, section, items, refs

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
            block = Block(key, placement, data_items(lines, rename), rename.refs)
            if label in data:
                self.local[key] = block
            else:
                self.named[key].append(block)


def load_unit(paths):
    """Parses the `.s` files at `paths` into one `Unit`."""
    unit = Unit()
    for index, path in enumerate(sorted(paths)):
        unit.add_file(index, Path(path).read_text(errors="surrogateescape"))
    return unit


def load_texts(texts):
    """Parses `.s` file texts into one `Unit`, for tests."""
    unit = Unit()
    for index, text in enumerate(texts):
        unit.add_file(index, text)
    return unit


# ---------------------------------------------------------------------------


class Report:
    """The result of comparing two units."""

    def __init__(self):
        """Starts with no differences."""
        self.differences = []
        self.functions = 0
        self.blocks = 0

    def differ(self, text):
        """Records one difference."""
        self.differences.append(text)


def compare_blocks(base, head):
    """None when two data blocks match, with their sections and alignment, else why not."""
    if base.section != head.section:
        return "placement %s -> %s" % (" ".join(base.section), " ".join(head.section))
    if base.items != head.items:
        return "contents differ"
    return None


def compare_units(base, head):
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
            reason = compare_blocks(bblock, hblock)
            if reason:
                report.differ("data differs: %s (%s)" % (name, reason))
                break
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
        reason = compare_blocks(bblock, hblock)
        if reason:
            report.differ("constant %s read by %s differs from head's %s (%s)" % (bq, via, hq, reason))
            continue
        pairs.extend((b[1], h[1], via) for b, h in zip(bblock.refs, hblock.refs))
    bleft = collections.Counter(b.shape() for q, b in base.local.items() if q not in bseen)
    hleft = collections.Counter(b.shape() for q, b in head.local.items() if q not in hseen)
    for items, count in (bleft - hleft).items():
        report.differ("unreferenced constant only in base (x%d): %s" % (count, items[:120]))
    for items, count in (hleft - bleft).items():
        report.differ("unreferenced constant only in head (x%d): %s" % (count, items[:120]))
    return report


def explain(title, bfiles, hfiles):
    """Prints where two sides' assembly files differ. Returns True when it found a difference."""
    if not bfiles or not hfiles:
        print("  %s: no assembly on one side (base %d files, head %d)" % (title, len(bfiles), len(hfiles)))
        return True
    report = compare_units(load_unit(bfiles), load_unit(hfiles))
    print("  %s: %d functions, %d data blocks compared, %s"
          % (title, report.functions, report.blocks, "differs" if report.differences else "no difference found"))
    for text in report.differences[:MAX_PRINTED]:
        print("    " + text)
    if len(report.differences) > MAX_PRINTED:
        print("    ... and %d more differences" % (len(report.differences) - MAX_PRINTED))
    return bool(report.differences)
