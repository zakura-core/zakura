#!/usr/bin/env python3
"""Check that a change leaves the release build the same, byte for byte.

It checks a change such as a hook that only `cfg(any(test, zakura_sim))`
builds compile. For each revision it checks out a detached worktree, builds
each package's release library at each codegen-units setting, and builds the
release `zakurad` with every workspace crate at each setting. It passes only
when the two sides' objects and binaries match byte for byte:

  * each library object's sections (debug info and embedded bitcode aside),
    relocations and symbols, with relocations naming their symbols and
    symbol names without LLVM's `.llvm.<hash>` promotion suffix, whose hash
    covers the source files' checksums in debug info;
  * the binary's sections in every segment but `__LINKEDIT`, its chained
    fixups or rebase and bind data, its exports and its dylibs on macOS, or
    every allocated section, relocations included, on Linux.

The contract is "identical apart from source line numbers". Nothing is
masked. Two things are made the same on both sides first, so that only
real changes differ:

  * Line numbers. A line added or removed in a `.rs` file moves every line
    below it, and line numbers live in release data, in panic locations and
    `tracing` callsites. So for each hunk of `git diff base head`, the
    shorter side is padded with blank lines to the longer side's length.
    Every unchanged line then has the same number on both sides. A blank
    line changes no token, so each side still builds the same code. Every
    padding is printed. Padding is refused inside a string literal or block
    comment, and when a file another embeds changes only by blank lines or
    an embed can't be resolved (see `align_tree`).

    A padded side is also built as committed, and its instruction sections
    must match the padded build byte for byte (see `run_line_checks`). So a
    line number that reaches code, by any route, such as `line!` forwarded
    through a macro, fails. A line number that reaches only data, such as a
    panic location, a `tracing` callsite, or a `Location::caller().line()`
    read at run time, is outside the contract.
  * Paths and build metadata. Both sides build at `<work>/src` with the
    target directory at `<work>/target`, one side at a time, each with its
    own cache (see `side_target`), and both get the base revision's Git
    metadata (see `metadata_env`).

`--explain` also emits assembly and, for each comparison that differs, says
which functions and data blocks differ (see `lib/asm_explain.py`). It never
changes the result.

Usage:
  release-asm-diff.py --base <rev> --head <rev>
      [--package zakura-network --package zakura-rt] [--cgu 4 --cgu 1]
      [--no-binary] [--explain] [--work-dir <dir>] [--cargo <cargo>]

Exit status: 0 when everything matches; 1 when anything differs or is on
one side only, or a line number reaches code; 2 when a side built no
objects; 3 when a command fails or padding is refused.
"""

import argparse
import collections
import contextlib
import json
import os
import re
import shutil
import struct
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "lib"))
import asm_explain  # noqa: E402
import revision_worktrees as rw  # noqa: E402

DEFAULT_PACKAGES = ["zakura-network", "zakura-rt"]
DEFAULT_CGUS = [4, 1]
BINARY_PACKAGE = "zakura"
BINARY = "zakurad"

EXIT_SAME, EXIT_DIFFERS, EXIT_EMPTY = 0, 1, 2

# How many differing sections to print per comparison.
MAX_PRINTED = 20

# ---------------------------------------------------------------------------
# Line alignment
# ---------------------------------------------------------------------------

HUNK = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@", re.M)
CHAR_LITERAL = re.compile(r"'(?:\\(?:x[0-9a-fA-F]{2}|u\{[0-9a-fA-F_]{1,6}\}|.)|[^\\'\n])'")


class PaddingError(Exception):
    """Padding that would land inside a literal or block comment."""


def parse_hunks(diff_text):
    """Reads a `-U0` diff's hunk headers as `(old_start, old_count, new_start, new_count)`."""
    return [
        (int(a), int(b) if b else 1, int(c), int(d) if d else 1)
        for a, b, c, d in HUNK.findall(diff_text)
    ]


class FileHunks(dict):
    """`{path: hunks}` for the files both sides have, and in `blank_only` the ones whose every changed line is blank."""

    def __init__(self):
        """Starts empty."""
        super().__init__()
        self.blank_only = set()


def parse_file_hunks(diff_text):
    """Splits a `-U0` diff into a `FileHunks` for files that both sides have."""
    files, path = FileHunks(), None
    old = None
    for line in diff_text.splitlines():
        if line.startswith("--- "):
            old = line[4:]
        elif line.startswith("+++ "):
            new = line[4:]
            path = new[2:] if old != "/dev/null" and new != "/dev/null" else None
            if path is not None:
                files[path] = []
                files.blank_only.add(path)
        elif line.startswith("@@") and path is not None:
            files[path].extend(parse_hunks(line))
        elif line.startswith(("+", "-")) and path is not None and line[1:].strip():
            files.blank_only.discard(path)
    return files


def paddings(hunks):
    """The blank lines each side needs so every unchanged line lines up.

    Returns `(base, head)`, each a list of `(after_line, count)`: insert
    `count` blank lines after that line, 0 meaning the file's start. Each
    hunk is padded on its shorter side to the longer side's length.
    """
    base, head = [], []
    for old_start, old_count, new_start, new_count in hunks:
        size = max(old_count, new_count)
        if size > old_count:
            base.append((old_start + old_count - 1 if old_count else old_start, size - old_count))
        if size > new_count:
            head.append((new_start + new_count - 1 if new_count else new_start, size - new_count))
    return base, head


# Where code can stop being code: a comment, a string or raw string, or a
# quote that may start a char literal.
CODE_STOP = re.compile(r"//|/\*|(?<![\w])(?:b|c)?r#*\"|\"|'")
STRING_END = re.compile(r'(?:[^"\\]|\\.)*"', re.S)
BLOCK_MARK = re.compile(r"/\*|\*/")


def lex(text):
    """Splits Rust source into `(kind, text)` segments: `code`, `comment`, `block`, `string` or `raw`.

    A `string` or `raw` segment is a string or raw string literal's body,
    without its quotes, prefix or hashes. Char literals stay in `code`, and lifetimes too. An
    unterminated literal or block comment runs to the end of the text.
    """
    segments, i, start, n = [], 0, 0, len(text)
    while i < n:
        stop = CODE_STOP.search(text, i)
        if not stop:
            break
        i, mark = stop.start(), stop.group()
        if mark == "'":
            char = CHAR_LITERAL.match(text, i)
            i = char.end() if char else i + 1
            continue
        if i > start:
            segments.append(("code", text[start:i]))
        if mark == "//":
            j = text.find("\n", i)
            j = n if j < 0 else j
            segments.append(("comment", text[i:j]))
        elif mark == "/*":
            depth, j = 0, i
            for block in BLOCK_MARK.finditer(text, i):
                depth += 1 if block.group() == "/*" else -1
                j = block.end()
                if depth == 0:
                    break
            else:
                j = n
            segments.append(("block", text[i:j]))
        elif mark.endswith('"') and "r" in mark:
            close = '"' + mark[mark.index("r") + 1:-1]
            k = text.find(close, stop.end())
            k = n if k < 0 else k
            segments.append(("raw", text[stop.end():k]))
            j = min(n, k + len(close))
        else:
            body = STRING_END.match(text, stop.end())
            k = body.end() - 1 if body else n
            segments.append(("string", text[stop.end():k]))
            j = min(n, k + 1)
        i = start = j
    if start < n:
        segments.append(("code", text[start:]))
    return segments


def safe_boundaries(text):
    """The lines whose end lies in code or a line comment, so a blank line can follow them.

    Returns a set of line numbers, 0 standing for the file's start. A line
    that ends inside a string, raw string or block comment isn't safe, since
    a blank line there would change a token.
    """
    safe, line, kind = {0}, 1, "code"
    for kind, part in lex(text):
        for _ in range(part.count("\n")):
            if kind in ("code", "comment"):
                safe.add(line)
            line += 1
    if kind in ("code", "comment") or not text:
        safe.add(line)
    return safe


def pad_text(text, inserts, path="<text>"):
    """Inserts the `(after_line, count)` blank lines into `text`, refusing any outside code."""
    safe = safe_boundaries(text)
    lines = text.split("\n")
    for after, count in sorted(inserts, reverse=True):
        if after not in safe:
            raise PaddingError("padding after %s:%d would land inside a literal or block comment" % (path, after))
        lines[after:after] = [""] * count
    return "\n".join(lines)


INCLUDE_MACRO = re.compile(r"\binclude(?:_str|_bytes)?\s*!\s*[(\[{]\s*")
# A string placeholder in `code_text`'s output.
PLACEHOLDER = re.compile(r"\x01(\d+)\x02")
LITERAL_ARGUMENT = re.compile(r"\x01(\d+)\x02\s*[)\]}]")
OUT_DIR_ARGUMENT = re.compile(r"concat\s*!\s*[(\[{]\s*env\s*!\s*[(\[{]\s*\x01(\d+)\x02")


STRING_ESCAPE = re.compile(r"\\(?:x([0-7][0-9a-fA-F])|u\{([0-9a-fA-F_]{1,6})\}|\n\s*|(.))", re.S)
SIMPLE_ESCAPES = {"n": "\n", "t": "\t", "r": "\r", "0": "\0", "\\": "\\", "'": "'", '"': '"'}


def string_value(kind, body):
    """The value of a string literal's body: a raw one as is, a plain one with its escapes decoded.

    Returns None for an escape Rust doesn't accept, so a caller can refuse
    rather than guess.
    """
    if kind == "raw":
        return body
    bad = []

    def one(match):
        """One escape's value."""
        hex_code, unicode, simple = match.groups()
        if hex_code:
            return chr(int(hex_code, 16))
        if unicode:
            return chr(int(unicode.replace("_", ""), 16))
        if simple is None:
            return ""  # a line continuation
        if simple not in SIMPLE_ESCAPES:
            bad.append(simple)
        return SIMPLE_ESCAPES.get(simple, "")

    value = STRING_ESCAPE.sub(one, body)
    return None if bad else value


def code_text(text):
    """Source text with comments dropped and each string literal replaced by a numbered placeholder.

    Returns the text and the strings' values (see `string_value`), so token
    patterns can't match inside a comment or a string.
    """
    out, strings = [], []
    for kind, part in lex(text):
        if kind == "code":
            out.append(part)
        elif kind in ("string", "raw"):
            out.append("\x01%d\x02" % len(strings))
            strings.append(string_value(kind, part))
        else:
            out.append(" ")
    return "".join(out), strings


def check_includes(tree, files, padded):
    """Refuses padding when an embedded file's only change is blank lines, or an embed can't be resolved.

    `include!`, `include_str!` and `include_bytes!` make a file's text part
    of the program. A real change to that text still differs after padding,
    so the bytes catch it, but a change of only blank lines would pad to the
    same text on both sides. An argument other than a string literal, or
    `concat!(env!("OUT_DIR"), ...)` for a generated file, could name a
    padded file, so it refuses too.
    """
    for source in files:
        code, strings = code_text(source.read_bytes().decode(errors="replace"))
        for found in INCLUDE_MACRO.finditer(code):
            literal = LITERAL_ARGUMENT.match(code, found.end())
            generated = OUT_DIR_ARGUMENT.match(code, found.end())
            path = strings[int(literal.group(1))] if literal else None
            if literal and path is None:
                raise PaddingError("%s embeds a path with an escape padding can't read" % source.relative_to(tree))
            if literal:
                target = (source.parent / path).resolve()
                if target in padded:
                    raise PaddingError("%s embeds %s, whose only change is blank lines"
                                       % (source.relative_to(tree), target.relative_to(tree)))
            elif not (generated and strings[int(generated.group(1))] == "OUT_DIR"):
                raise PaddingError("%s embeds a file padding can't rule out: %s"
                                   % (source.relative_to(tree), code[found.start():found.end() + 40].strip()))


def rust_sources(tree):
    """Every `.rs` file in `tree` outside `target`."""
    return [path for path in sorted(tree.rglob("*.rs")) if path.relative_to(tree).parts[0] != "target"]


def align_tree(tree, file_hunks, side):
    """Pads `side`'s `.rs` files in `tree` so unchanged lines line up with the other side's.

    Refuses padding inside a literal (see `pad_text`), or when the
    workspace embeds a file padding could make equal (see
    `check_includes`). Returns the `(path, after_line, count)` paddings it
    made.
    """
    tree = Path(tree).resolve()
    made, blank_padded = [], set()
    blank_only = getattr(file_hunks, "blank_only", set())
    for path, hunks in sorted(file_hunks.items()):
        inserts = paddings(hunks)[0 if side == "base" else 1]
        if not inserts:
            continue
        file = tree / path
        text = file.read_bytes().decode(errors="surrogateescape")
        file.write_bytes(pad_text(text, inserts, path).encode(errors="surrogateescape"))
        made.extend((path, after, count) for after, count in inserts)
        if path in blank_only:
            blank_padded.add(file.resolve())
    if made:
        check_includes(tree, rust_sources(tree), blank_padded)
    return made


def rust_hunks(repo, base, head):
    """The `-U0` diff hunks of every `.rs` file that both revisions have."""
    diff = rw.run(["git", "-C", repo, "diff", "-U0", "--no-ext-diff", "--no-color", "--no-renames",
                   base, head, "--", "*.rs"], capture=True)
    return parse_file_hunks(diff)


# ---------------------------------------------------------------------------
# Object and binary images
# ---------------------------------------------------------------------------

MACHO_MAGIC = b"\xcf\xfa\xed\xfe"
MH_EXECUTE = 2
CPU_ARM64 = 0x0100000C
ARM64_RELOC_ADDEND = 10
LC_SEGMENT_64, LC_SYMTAB, LC_DYSYMTAB, LC_UUID, LC_CODE_SIGNATURE = 0x19, 0x2, 0xB, 0x1B, 0x1D
# Load commands whose payload is a blob in `__LINKEDIT`: function starts,
# data in code, chained fixups and the exports trie.
LC_LINKEDIT_DATA = {0x26, 0x29, 0x80000034, 0x80000033}
LC_DYLD_INFO = {0x22, 0x80000022}
S_ZEROFILL = (0x1, 0xC, 0x12)
# The suffix that LLVM gives a local symbol it promotes across codegen units,
# which a name derived from that symbol, such as a switch table's, carries
# too. Its number hashes the module, debug info included, and debug info
# holds each source file's checksum, which padding can't make equal.
LLVM_SUFFIX = re.compile(rb"\.llvm\.\d+")


def symbol_name(data, strings, offset):
    """The NUL-terminated symbol name at `strings + offset`, without LLVM's promotion suffixes."""
    start = strings + offset
    return LLVM_SUFFIX.sub(b"", data[start:data.index(b"\0", start)])


def macho_symbols(data, commands):
    """An object file's symbols in index order, as `(name, type, section, desc, value)`."""
    for cmd, body in commands:
        if cmd == LC_SYMTAB:
            symoff, nsyms, stroff, _ = struct.unpack_from("<IIII", body, 8)
            out = []
            for k in range(nsyms):
                strx, ntype, nsect, desc, value = struct.unpack_from("<IBBHQ", data, symoff + 16 * k)
                out.append((symbol_name(data, stroff, strx), ntype, nsect, desc, value))
            return out
    return []


def macho_relocations(data, reloff, nreloc, symbols, cputype):
    """A section's relocations, each external one naming its symbol rather than its index.

    A symbol is named by its name, type, section and value, so two symbols
    that drop to the same name (see `LLVM_SUFFIX`) stay apart unless they
    are the same address.
    """
    out = []
    for k in range(nreloc):
        address, packed = struct.unpack_from("<iI", data, reloff + 8 * k)
        target, extern, rtype = packed & 0xFFFFFF, packed >> 27 & 1, packed >> 28
        if extern and not (cputype == CPU_ARM64 and rtype == ARM64_RELOC_ADDEND) and target < len(symbols):
            target = symbols[target]
        out.append(b"%d %d %r" % (address, packed >> 24, target))
    return b"\n".join(out)


S_ATTR_INSTRUCTIONS = 0x80000400
SHF_EXECINSTR = 0x4


def macho_image(data, code_only=False):
    """The runtime parts of a 64-bit little-endian Mach-O file, as `{name: bytes}`.

    Skips `__DWARF` debug info, `__LLVM` embedded bitcode, `__LINKEDIT`
    itself, the UUID and the code signature. Keeps every other section's
    header and contents, each section's relocations, the dyld rebase, bind
    and export data, the linkedit blobs above, every other load command as
    is, and, in an object file, its non-debug symbols. In an object file,
    relocations name their symbols, and symbol names drop LLVM's promotion
    suffix (see `LLVM_SUFFIX`). With `code_only`, keeps only the sections
    that hold instructions, with their relocations.
    """
    image = {}
    cputype = struct.unpack_from("<I", data, 4)[0]
    filetype, ncmds = struct.unpack_from("<II", data, 12)
    commands, offset = [], 32
    for _ in range(ncmds):
        cmd, size = struct.unpack_from("<II", data, offset)
        commands.append((cmd, data[offset:offset + size]))
        offset += size
    executable = filetype == MH_EXECUTE
    symbols = [] if executable else macho_symbols(data, commands)
    for index, (cmd, body) in enumerate(commands):
        if cmd == LC_SEGMENT_64:
            segname = body[8:24].rstrip(b"\0").decode()
            nsects = struct.unpack_from("<I", body, 64)[0]
            if segname != "__LINKEDIT" and not code_only:
                image["segment %s" % segname] = body[8:24] + body[24:40] + body[56:72]
            for j in range(nsects):
                sect = body[72 + 80 * j:152 + 80 * j]
                names = "%s,%s" % (sect[16:32].rstrip(b"\0").decode(), sect[0:16].rstrip(b"\0").decode())
                if names.startswith(("__DWARF,", "__LINKEDIT,", "__LLVM,")):
                    continue
                addr, length = struct.unpack_from("<QQ", sect, 32)
                start, align, reloff, nreloc, flags = struct.unpack_from("<IIIII", sect, 48)
                if code_only and not flags & S_ATTR_INSTRUCTIONS:
                    continue
                header = struct.pack("<QQIII", addr, length, align, flags, nreloc) + sect[68:80]
                contents = b"" if flags & 0xFF in S_ZEROFILL else data[start:start + length]
                image["section %s" % names] = header + contents
                if nreloc:
                    image["relocations %s" % names] = (
                        data[reloff:reloff + 8 * nreloc] if executable
                        else macho_relocations(data, reloff, nreloc, symbols, cputype))
        elif code_only:
            continue
        elif cmd in LC_LINKEDIT_DATA:
            start, length = struct.unpack_from("<II", body, 8)
            image["linkedit 0x%x" % cmd] = data[start:start + length]
        elif cmd in LC_DYLD_INFO:
            fields = struct.unpack_from("<10I", body, 8)
            for k, name in enumerate(("rebase", "bind", "weak bind", "lazy bind", "export")):
                start, length = fields[2 * k], fields[2 * k + 1]
                image["dyld %s" % name] = data[start:start + length]
        elif cmd == LC_SYMTAB:
            if not executable:
                image["symbols"] = b"\n".join(sorted(
                    b"%s %d %d %d %d" % symbol for symbol in symbols if not symbol[1] & 0xE0))
        elif cmd not in (LC_DYSYMTAB, LC_UUID, LC_CODE_SIGNATURE):
            image["load command %d (0x%x)" % (index, cmd)] = body
    if not code_only:
        image["header"] = data[4:16] + data[24:32]
    return image


ELF_MAGIC = b"\x7fELF\x02\x01"
SHF_ALLOC = 0x2
SHT_NOBITS, SHT_SYMTAB, SHT_RELA, SHT_REL = 8, 2, 4, 9
ET_REL = 1
STT_SECTION = 3


def elf_image(data, code_only=False):
    """The runtime parts of a 64-bit little-endian ELF file, as `{name: bytes}`.

    Keeps every program header, every allocated section's header and
    contents except the build ID, and every relocation section that applies
    to one. In an object file, relocations name their symbols by name, type,
    section and value, a section symbol by its section, and the symbols are
    kept with their names without LLVM's promotion suffix (see
    `LLVM_SUFFIX`). Debug and other unallocated sections, embedded bitcode
    among them, are skipped. With `code_only`, keeps only executable
    sections and the relocations that apply to them.
    """
    image = {} if code_only else {"header": data[16:24] + data[24:32]}
    obj = struct.unpack_from("<H", data, 16)[0] == ET_REL
    shoff = struct.unpack_from("<Q", data, 0x28)[0]
    entsize, count, strndx = struct.unpack_from("<HHH", data, 0x3A)
    headers = [struct.unpack_from("<IIQQQQIIQQ", data, shoff + i * entsize) for i in range(count)]
    names_at = headers[strndx][4]

    def name(header):
        """A section's name."""
        start = names_at + header[0]
        return data[start:data.index(b"\0", start)].decode()

    phoff = struct.unpack_from("<Q", data, 0x20)[0]
    phentsize, phnum = struct.unpack_from("<HH", data, 0x36)
    for k in range(0 if code_only else phnum):
        # Every program header: type, permissions, mapping and alignment.
        image["program header %d" % k] = data[phoff + k * phentsize:phoff + (k + 1) * phentsize]
    symbols = []
    for header in headers:
        if header[1] == SHT_SYMTAB and obj:
            strings = headers[header[6]][4]
            for k in range(header[5] // 24):
                strx, info, other, shndx, value, size = struct.unpack_from("<IBBHQQ", data, header[4] + 24 * k)
                sym = symbol_name(data, strings, strx)
                if info & 0xF == STT_SECTION and shndx < count:
                    # A section symbol has no name of its own.
                    sym = b"section " + name(headers[shndx]).encode()
                symbols.append((sym, info, other, shndx, value, size))
            if not code_only:
                image["symbols"] = b"\n".join(sorted(b"%s %d %d %d %d %d" % symbol for symbol in symbols))
    # ELF allows several sections with one name, such as COMDAT groups'
    # `.text`, so each is known by its name and occurrence.
    seen, labels = collections.Counter(), []
    for header in headers:
        seen[name(header)] += 1
        labels.append(name(header) + ("" if seen[name(header)] == 1 else "#%d" % seen[name(header)]))
    for header, label in zip(headers, labels):
        sname, stype, flags, addr, start, length, link, info, align, _ = header
        key = "section " + label
        keep = flags & SHF_ALLOC and sname and name(header) != ".note.gnu.build-id"
        relocates = stype in (SHT_RELA, SHT_REL) and 0 < info < count and headers[info][2] & SHF_ALLOC
        if code_only and not (flags & SHF_EXECINSTR or relocates and headers[info][2] & SHF_EXECINSTR):
            continue
        if relocates and obj:
            width = 24 if stype == SHT_RELA else 16
            entries = []
            for k in range(length // width):
                offset, rinfo = struct.unpack_from("<QQ", data, start + width * k)
                addend = struct.unpack_from("<q", data, start + width * k + 16)[0] if width == 24 else 0
                index = rinfo >> 32
                target = symbols[index] if index < len(symbols) else index
                entries.append(b"%d %d %r %d" % (offset, rinfo & 0xFFFFFFFF, target, addend))
            image[key] = struct.pack("<IQ", stype, flags) + labels[info].encode() + b"\n" + b"\n".join(entries)
        elif keep or relocates:
            contents = b"" if stype == SHT_NOBITS else data[start:start + length]
            # A relocation section's `sh_info` names the section it relocates.
            applies = labels[info].encode() if relocates else b""
            image[key] = struct.pack("<IQQQQ", stype, flags, addr, length, align) + applies + b"\n" + contents
    return image


def runtime_image(path, code_only=False):
    """The runtime parts of a Mach-O or ELF object or binary at `path` (see `macho_image`, `elf_image`)."""
    data = Path(path).read_bytes()
    if data[:4] == MACHO_MAGIC:
        return macho_image(data, code_only)
    if data[:6] == ELF_MAGIC:
        return elf_image(data, code_only)
    raise ValueError("%s isn't a 64-bit little-endian Mach-O or ELF file" % path)


def compare_images(base, head):
    """How two runtime images differ, as a list of lines, empty when they match."""
    out = []
    for name in sorted(set(base) | set(head)):
        if name not in head:
            out.append("%s: only in base" % name)
        elif name not in base:
            out.append("%s: only in head" % name)
        elif base[name] != head[name]:
            b, h = base[name], head[name]
            first = next((i for i, (x, y) in enumerate(zip(b, h)) if x != y), min(len(b), len(h)))
            out.append("%s: %d bytes in base, %d in head, first difference at byte %d" % (name, len(b), len(h), first))
    return out


def compare_files(bfiles, hfiles, code_only=False):
    """How two sides' sets of object files or binaries differ, matched by file name.

    With `code_only`, compares only the sections that hold instructions.
    """
    bnames, hnames = {path.name: path for path in bfiles}, {path.name: path for path in hfiles}
    out = []
    for file in sorted(set(bnames) | set(hnames)):
        if file not in hnames or file not in bnames:
            out.append("%s: only in %s" % (file, "base" if file in bnames else "head"))
            continue
        out.extend("%s %s" % (file, line) for line in compare_images(runtime_image(bnames[file], code_only),
                                                                     runtime_image(hnames[file], code_only)))
    return out


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


@contextlib.contextmanager
def side_target(work, side):
    """Moves `side`'s cached target directory to `<work>/target` while it builds, and back after.

    Both sides build at the same path, so paths a build embeds, such as
    `OUT_DIR`, match. Each side keeps its own cache, so neither side ever
    counts the other's artifacts as fresh.
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


def collect(deps, prefix, suffixes, out_dir):
    """Moves the files in `deps` named `<prefix>*<suffix>` to a fresh `out_dir`, and lists them by suffix."""
    shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True)
    found = {}
    for suffix in suffixes:
        for path in deps.glob(prefix + "*" + suffix):
            shutil.move(str(path), str(out_dir / path.name))
        found[suffix] = sorted(out_dir.glob("*" + suffix))
    return found


def emit_lib(cargo, tree, target, inputs, package, crate, cgu, out_dir, asm):
    """Builds `package`'s release library at `cgu` codegen units, and returns its objects (and assembly).

    The release profile's thin LTO makes cargo build a library as bitcode for
    the final link, so this asks for machine code instead, as `--emit=asm`
    does. The binary's own comparison covers what LTO does.
    """
    deps = target / "release" / "deps"
    for suffix in (".o", ".s"):
        for stale in deps.glob(crate + "-*" + suffix):
            stale.unlink()
    print("== %s: %s, codegen-units=%d" % (tree.name, package, cgu), flush=True)
    emit = "obj,asm,link" if asm else "obj,link"
    rw.run(
        [cargo, "rustc", "--release", "--locked", "-p", package, "--lib", "--",
         "--emit=" + emit, "-C", "codegen-units=%d" % cgu, "-C", "linker-plugin-lto=no"],
        cwd=tree, env=build_env(target, inputs),
    )
    found = collect(deps, crate + "-", (".o", ".s"), out_dir)
    return found[".o"], found[".s"]


def emit_binary(cargo, tree, target, inputs, cgu, out_dir, asm):
    """Builds the release `zakurad` with every workspace crate at `cgu` codegen units.

    Returns the binary, copied to `out_dir`, and with `asm` the assembly of
    every module after thin LTO, which `--emit=asm` writes for the binary.
    """
    deps = target / "release" / "deps"
    for stale in deps.glob(BINARY + "-*.s"):
        stale.unlink()
    print("== %s: release %s, codegen-units=%d" % (tree.name, BINARY, cgu), flush=True)
    rw.run(
        [cargo, "rustc", "--release", "--locked", "-p", BINARY_PACKAGE, "--bin", BINARY, "--",
         "--emit=asm,link" if asm else "--emit=link"],
        cwd=tree, env=build_env(target, inputs, {"CARGO_PROFILE_RELEASE_CODEGEN_UNITS": str(cgu)}),
    )
    found = collect(deps, BINARY + "-", (".s",), out_dir)
    shutil.copy2(str(target / "release" / BINARY), str(out_dir / BINARY))
    return [out_dir / BINARY], found[".s"]


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


# ---------------------------------------------------------------------------
# Comparing
# ---------------------------------------------------------------------------


def comparisons(built, packages, cgus, binary):
    """The `(title, base, head)` comparisons to run, each side `(files, asm files)`, from `built`."""
    out = []
    for name in list(packages) + ([BINARY] if binary else []):
        for cgu in cgus:
            title = "%s%s codegen-units=%d" % ("binary " if name == BINARY else "", name, cgu)
            out.append((title, built.get(("base", name, cgu), ([], [])), built.get(("head", name, cgu), ([], []))))
    return out


def run_comparisons(items, explain=False):
    """Compares and prints each comparison, returning the worst exit status."""
    status = EXIT_SAME
    for title, (bfiles, basm), (hfiles, hasm) in items:
        if not bfiles or not hfiles:
            print("%s: nothing built on one side (base %d files, head %d)" % (title, len(bfiles), len(hfiles)))
            status = max(status, EXIT_EMPTY)
            continue
        differences = compare_files(bfiles, hfiles)
        print("%s: %d files: %s" % (title, len(bfiles), "differs" if differences else "identical"))
        for line in differences[:MAX_PRINTED]:
            print("  " + line)
        if len(differences) > MAX_PRINTED:
            print("  ... and %d more" % (len(differences) - MAX_PRINTED))
        if differences:
            status = max(status, EXIT_DIFFERS)
            if explain:
                asm_explain.explain("assembly", basm, hasm)
    return status


def run_line_checks(items):
    """Checks that padding a side changed none of its instructions, returning the worst exit status.

    Each item is `(title, unpadded files, padded files)`. The gate's contract
    is "identical apart from source line numbers": a line number may move
    where it only labels a panic or a log line, which lives in data. If one
    reaches an instruction, by any route, such as `line!` forwarded through
    a macro, padding changed what the program computes, and the padded
    comparison can't vouch for it.
    """
    status = EXIT_SAME
    for title, unpadded, padded in items:
        if not unpadded or not padded:
            print("%s: nothing built (unpadded %d files, padded %d)" % (title, len(unpadded), len(padded)))
            status = max(status, EXIT_EMPTY)
            continue
        differences = compare_files(unpadded, padded, code_only=True)
        print("%s: %s" % (title, "line numbers reach code" if differences else "code unchanged by padding"))
        for line in differences[:MAX_PRINTED]:
            print("  " + line)
        if differences:
            status = max(status, EXIT_DIFFERS)
    return status


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
    parser.add_argument("--explain", action="store_true",
                        help="also emit assembly, and say which functions and data differ")
    parser.add_argument("--work-dir", help="keep build caches here for reuse (default: a temporary directory)")
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
        file_hunks = rust_hunks(repo, base, head)
        with rw.work_dir(args.work_dir) as work:
            inputs = metadata_env(work, repo, base)
            built, checks = {}, []
            names = list(packages) + ([] if args.no_binary else [BINARY])
            for side, rev in (("base", base), ("head", head)):
                with rw.worktree(repo, rev, work / "src") as tree:
                    padded = align_tree(tree, file_hunks, side)
                    for path, after, count in padded:
                        print("padded %s: %d blank lines after %s:%d" % (side, count, path, after))
                    crates = lib_crates(read_metadata(args.cargo, tree))
                    with side_target(work, side) as target:
                        # The padded tree first, then, when this side was
                        # padded, the tree as committed, for the line check.
                        for variant in (("padded", "unpadded") if padded else ("padded",)):
                            if variant == "unpadded":
                                rw.run(["git", "-C", tree, "checkout", "--quiet", "--", "."])
                                # Every crate rebuilds, so each emits its objects again.
                                for source in rust_sources(tree):
                                    os.utime(source)
                            for name in names:
                                for cgu in cgus:
                                    out = work / "out" / variant / side / ("%s-cgu%d" % (name, cgu))
                                    asm = args.explain and variant == "padded"
                                    if name == BINARY:
                                        result = emit_binary(args.cargo, tree, target, inputs, cgu, out, asm)
                                    else:
                                        crate = crates.get(name, name.replace("-", "_"))
                                        result = emit_lib(args.cargo, tree, target, inputs, name, crate, cgu, out, asm)
                                    built[variant, side, name, cgu] = result
                    if padded:
                        checks.extend(
                            ("line check %s %s%s codegen-units=%d" % (side, "binary " if name == BINARY else "", name, cgu),
                             built["unpadded", side, name, cgu][0], built["padded", side, name, cgu][0])
                            for name in names for cgu in cgus)
            padded_built = {key[1:]: value for key, value in built.items() if key[0] == "padded"}
            status = run_comparisons(comparisons(padded_built, packages, cgus, not args.no_binary), args.explain)
            return max(status, run_line_checks(checks))
    except PaddingError as error:
        print("error: %s" % error, file=sys.stderr)
        return rw.EXIT_COMMAND_FAILED
    except rw.CommandFailed as error:
        print("error: %s" % error, file=sys.stderr)
        return rw.EXIT_COMMAND_FAILED
    except (OSError, ValueError) as error:
        print("error: %s" % error, file=sys.stderr)
        return rw.EXIT_COMMAND_FAILED


if __name__ == "__main__":
    sys.exit(main())
