import importlib.util
import inspect
import io
import os
import struct
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "release-asm-diff.py"
SPEC = importlib.util.spec_from_file_location("release_asm_diff", SCRIPT)
assert SPEC and SPEC.loader
asm_diff = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = asm_diff
SPEC.loader.exec_module(asm_diff)
asm_explain = asm_diff.asm_explain


def macho(sections, uuid=b"\1" * 16, fixups=b"fixups", linkedit=b"symbols"):
    """A minimal 64-bit Mach-O executable.

    `sections` lists `(segment, section, contents, flags)`. The file also
    gets an `LC_UUID`, chained fixups and a `__LINKEDIT` segment holding the
    fixups and `linkedit`, which stands in for the symbol table.
    """
    segments = {}
    for seg, sect, contents, flags in sections:
        segments.setdefault(seg, []).append((sect, contents, flags))
    commands_size = sum(72 + 80 * len(sects) for sects in segments.values()) + 72 + 24 + 16
    offset = 32 + commands_size
    commands, payload, addr = [], b"", 0x100000000
    for seg, sects in segments.items():
        headers = b""
        for sect, contents, flags in sects:
            headers += struct.pack("<16s16sQQIIIIIIII", sect.encode(), seg.encode(), addr, len(contents),
                                   offset + len(payload), 3, 0, 0, flags, 0, 0, 0)
            payload += contents
            addr += len(contents)
        commands.append(struct.pack("<II16sQQQQIIII", 0x19, 72 + 80 * len(sects), seg.encode(), 0x100000000,
                                    0x4000, 0, 0, 5, 5, len(sects), 0) + headers)
    fixups_at = offset + len(payload)
    payload += fixups + linkedit
    commands.append(struct.pack("<II16sQQQQIIII", 0x19, 72, b"__LINKEDIT", 0x200000000, 0x4000,
                                fixups_at, len(fixups) + len(linkedit), 1, 1, 0, 0))
    commands.append(struct.pack("<II16s", 0x1B, 24, uuid))
    commands.append(struct.pack("<IIII", 0x80000034, 16, fixups_at, len(fixups)))
    body = b"".join(commands)
    header = struct.pack("<IiiIIIII", 0xFEEDFACF, 0x0100000C, 0, 2, len(commands), len(body), 0, 0)
    return header + body + payload


def macho_object(symbols, relocations, bitcode=b"bc"):
    """A minimal 64-bit Mach-O object file with `__TEXT,__text`, embedded bitcode, symbols and relocations.

    `symbols` lists names, each defined in `__text`. `relocations` lists the
    index of the external symbol each relocation targets.
    """
    text = b"\x1f\x20\x03\xd5" * 4
    strings = b"\0" + b"".join(name + b"\0" for name in symbols)
    commands_size = 72 + 80 * 2 + 24
    offset = 32 + commands_size
    text_at, bitcode_at = offset, offset + len(text)
    relocs_at = bitcode_at + len(bitcode)
    relocs = b"".join(struct.pack("<iI", 4 * k, (2 << 28) | (1 << 27) | (2 << 25) | (1 << 24) | target)
                      for k, target in enumerate(relocations))
    symbols_at = relocs_at + len(relocs)
    entries, strx = b"", 1
    for name in symbols:
        entries += struct.pack("<IBBHQ", strx, 0x0F, 1, 0, 0)
        strx += len(name) + 1
    strings_at = symbols_at + len(entries)
    sections = (struct.pack("<16s16sQQIIIIIIII", b"__text", b"__TEXT", 0, len(text), text_at, 2, relocs_at,
                            len(relocations), 0x80000400, 0, 0, 0)
                + struct.pack("<16s16sQQIIIIIIII", b"__bitcode", b"__LLVM", len(text), len(bitcode), bitcode_at,
                              0, 0, 0, 0, 0, 0, 0))
    segment = struct.pack("<II16sQQQQIIII", 0x19, 72 + 160, b"", 0, len(text) + len(bitcode), text_at,
                          len(text) + len(bitcode), 7, 7, 2, 0) + sections
    symtab = struct.pack("<IIIIII", 0x2, 24, symbols_at, len(symbols), strings_at, len(strings))
    header = struct.pack("<IiiIIIII", 0xFEEDFACF, 0x0100000C, 0, 1, 2, len(segment) + len(symtab), 0, 0)
    return header + segment + symtab + text + bitcode + relocs + entries + strings


def elf(sections, elf_type=2, program_headers=()):
    """A minimal 64-bit ELF file.

    `sections` lists `(name, type, flags, contents, info, link)`, and
    `program_headers` lists `(type, flags)`.
    """
    names = b"\0" + b"".join(section[0].encode() + b"\0" for section in sections) + b".shstrtab\0"
    phdrs = b"".join(struct.pack("<IIQQQQQQ", ptype, flags, 0, 0, 0, 0, 0, 16) for ptype, flags in program_headers)
    offset = 64 + len(phdrs)
    payload, headers, name_at = b"", [b"\0" * 64], 1
    for name, stype, flags, contents, info, link in sections:
        headers.append(struct.pack("<IIQQQQIIQQ", name_at, stype, flags, 0, offset + len(payload),
                                   len(contents), link, info, 1, 24 if stype == 2 else 0))
        name_at += len(name) + 1
        payload += contents
    headers.append(struct.pack("<IIQQQQIIQQ", name_at, 3, 0, 0, offset + len(payload), len(names), 0, 0, 1, 0))
    payload += names
    header = (b"\x7fELF\x02\x01\x01" + b"\0" * 9
              + struct.pack("<HHIQQQIHHHHHH", elf_type, 0xB7, 1, 0x400000, 64 if phdrs else 0, offset + len(payload),
                            0, 64, 56, len(program_headers), 64, len(headers), len(headers) - 1))
    return header + phdrs + payload + b"".join(headers)


def elf_object(target):
    """A minimal ELF object whose one `.text` relocation targets section symbol 1 (`.data.x`) or 2 (`.data.y`)."""
    symbols = (b"\0" * 24 + struct.pack("<IBBHQQ", 0, 3, 0, 2, 0, 0) + struct.pack("<IBBHQQ", 0, 3, 0, 3, 0, 0))
    rela = struct.pack("<QQq", 0, (target << 32) | 257, 0)
    return elf([(".text", 1, 6, b"code", 0, 0), (".data.x", 1, 3, b"xxxxxxxx", 0, 0), (".data.y", 1, 3, b"yyyyyyyy", 0, 0),
                (".rela.text", 4, 0x40, rela, 1, 5), (".symtab", 2, 0, symbols, 3, 6), (".strtab", 3, 0, b"\0", 0, 0)],
               elf_type=1)


TEXT = ("__TEXT", "__text", b"\x1f\x20\x03\xd5" * 4, 0x80000400)
CSTRING = ("__TEXT", "__cstring", b"bad peer\0crates/x/src/lib.rs\0", 2)
JUMPS = ("__TEXT", "__const", b"\x00\x01\x02\x03", 0)
# A panic `Location`, whose path pointer the fixups resolve: length 19, line 10, column 5.
LOCATION = ("__DATA_CONST", "__const", struct.pack("<QQII", 0, 19, 10, 5), 0)
CONSTRUCTORS = ("__DATA", "__mod_init_func", struct.pack("<QQ", 0x1000, 0x2000), 9)
ALL = [TEXT, CSTRING, JUMPS, LOCATION, CONSTRUCTORS]


def changed(section, contents):
    """`section` with new contents."""
    return section[:2] + (contents,) + section[3:]


def swap(sections, new):
    """`sections` with the one that has `new`'s segment and name replaced by `new`."""
    return [new if section[:2] == new[:2] else section for section in sections]


def image_status(base, head):
    """Compares two crafted binaries as the gate does, returning the exit status and what it printed."""
    with tempfile.TemporaryDirectory() as tmp:
        sides = []
        for side, data in (("base", base), ("head", head)):
            path = Path(tmp) / side / "zakurad"
            path.parent.mkdir()
            path.write_bytes(data)
            sides.append(([path], []))
        out = io.StringIO()
        with redirect_stdout(out):
            status = asm_diff.run_comparisons([("binary zakurad", sides[0], sides[1])])
    return status, out.getvalue()


class ImageTests(unittest.TestCase):
    def test_identical_images_pass(self):
        """Binaries that differ only in their UUID and symbol table pass."""
        status, out = image_status(macho(ALL), macho(ALL, uuid=b"\2" * 16, linkedit=b"other symbols"))
        self.assertEqual(status, 0, out)
        self.assertIn("identical", out)

    def test_a_changed_instruction_fails(self):
        """A changed instruction fails."""
        status, out = image_status(macho(ALL), macho(swap(ALL, changed(TEXT, b"\x1f\x20\x03\xd5" * 3 + b"\xc0\x03\x5f\xd6"))))
        self.assertEqual(status, 1, out)
        self.assertIn("section __TEXT,__text", out)

    def test_a_changed_constant_fails(self):
        """A changed constant that keeps its size fails."""
        status, out = image_status(macho(ALL), macho(swap(ALL, changed(LOCATION, struct.pack("<QQII", 0, 19, 10, 6)))))
        self.assertEqual(status, 1, out)
        self.assertIn("section __DATA_CONST,__const", out)

    def test_a_missing_function_fails(self):
        """A section that only one side has fails."""
        status, out = image_status(macho(ALL), macho(ALL[:-1]))
        self.assertEqual(status, 1, out)
        self.assertIn("section __DATA,__mod_init_func: only in base", out)

    def test_an_empty_side_fails(self):
        """A side that built nothing exits 2."""
        out = io.StringIO()
        with redirect_stdout(out):
            status = asm_diff.run_comparisons([("binary zakurad", ([Path("/x")], []), ([], []))])
        self.assertEqual(status, 2, out.getvalue())

    def test_a_changed_panic_message_fails(self):
        """A message string of the same length fails."""
        status, out = image_status(macho(ALL), macho(swap(ALL, changed(CSTRING, b"bad Peer\0crates/x/src/lib.rs\0"))))
        self.assertEqual(status, 1, out)
        self.assertIn("section __TEXT,__cstring", out)

    def test_a_moved_release_line_fails(self):
        """A location whose line moved, with nothing to line it up, fails."""
        status, out = image_status(macho(ALL), macho(swap(ALL, changed(LOCATION, struct.pack("<QQII", 0, 19, 21, 5)))))
        self.assertEqual(status, 1, out)
        self.assertIn("section __DATA_CONST,__const", out)

    def test_swapped_constructor_entries_fail(self):
        """Two constructor entries that swap places fail."""
        status, out = image_status(macho(ALL), macho(swap(ALL, changed(CONSTRUCTORS, struct.pack("<QQ", 0x2000, 0x1000)))))
        self.assertEqual(status, 1, out)
        self.assertIn("section __DATA,__mod_init_func", out)

    def test_swapped_jump_table_targets_fail(self):
        """Two jump-table targets that swap places fail."""
        status, out = image_status(macho(ALL), macho(swap(ALL, changed(JUMPS, b"\x00\x02\x01\x03"))))
        self.assertEqual(status, 1, out)
        self.assertIn("section __TEXT,__const", out)

    def test_a_changed_fixup_fails(self):
        """A pointer whose fixup changes fails, though every section is the same."""
        status, out = image_status(macho(ALL), macho(ALL, fixups=b"fixupz"))
        self.assertEqual(status, 1, out)
        self.assertIn("linkedit 0x80000034", out)

    def test_object_relocations_name_their_symbols(self):
        """Object relocations and symbols compare by name, without LLVM's suffix or embedded bitcode."""
        base = macho_object([b"_f.llvm.111", b"_g"], [0])
        status, out = image_status(base, macho_object([b"_g", b"_f.llvm.222"], [1], bitcode=b"BC"))
        self.assertEqual(status, 0, out)
        status, out = image_status(base, macho_object([b"_f.llvm.111", b"_g"], [1]))
        self.assertEqual(status, 1, out)
        self.assertIn("relocations __TEXT,__text", out)

    def test_elf_compares_allocated_sections_and_relocations(self):
        """On ELF, allocated sections and the relocations for them count, and debug info and the build ID don't."""

        def build(rela=b"r1", debug=b"d1", build_id=b"b1"):
            """A crafted ELF file with one changeable section of each kind, `.rela.text` applying to `.text`."""
            return elf([(".text", 1, 6, b"code", 0, 0), (".data.rel.ro", 1, 3, b"vtable", 0, 0),
                        (".rela.text", 4, 0x40, rela, 1, 0), (".debug_info", 1, 0, debug, 0, 0),
                        (".note.gnu.build-id", 7, 2, build_id, 0, 0)])

        status, out = image_status(build(), build(debug=b"d2", build_id=b"b2"))
        self.assertEqual(status, 0, out)
        status, out = image_status(build(), build(rela=b"r2"))
        self.assertEqual(status, 1, out)
        self.assertIn("section .rela.text", out)

    def test_elf_section_symbol_targets_stay_apart(self):
        """An ELF object relocation that moves between two unnamed section symbols fails."""
        status, out = image_status(elf_object(1), elf_object(1))
        self.assertEqual(status, 0, out)
        status, out = image_status(elf_object(1), elf_object(2))
        self.assertEqual(status, 1, out)
        self.assertIn("section .rela.text", out)

    def test_elf_sections_that_share_a_name_are_each_compared(self):
        """Two `.text` sections, as COMDAT groups give, are compared one by one."""

        def build(first):
            """An ELF file with two `.text` sections."""
            return elf([(".text", 1, 6, first, 0, 0), (".text", 1, 6, b"two", 0, 0)])

        status, out = image_status(build(b"one"), build(b"One"))
        self.assertEqual(status, 1, out)
        self.assertIn("section .text:", out)

    def test_elf_relocations_keep_the_section_they_apply_to(self):
        """A relocation section that moves to another target section fails, though its entries are the same."""

        def build(target):
            """An ELF executable whose `.rela.text` applies to section `target`."""
            return elf([(".text", 1, 6, b"code", 0, 0), (".data", 1, 3, b"data", 0, 0),
                        (".rela.text", 4, 0x40, b"r1", target, 0)])

        status, out = image_status(build(1), build(2))
        self.assertEqual(status, 1, out)
        self.assertIn("section .rela.text", out)

        def twins(target):
            """An ELF executable with two `.data` sections, whose `.rela.data` applies to one of them."""
            return elf([(".data", 1, 3, b"one", 0, 0), (".data", 1, 3, b"two", 0, 0),
                        (".rela.data", 4, 0x40, b"r1", target, 0)])

        status, out = image_status(twins(1), twins(2))
        self.assertEqual(status, 1, out)
        self.assertIn("section .rela.data", out)

    def test_elf_program_headers_count(self):
        """An executable stack, which only a program header records, fails."""
        stack = 0x6474E551
        status, out = image_status(elf([], program_headers=[(stack, 6)]), elf([], program_headers=[(stack, 7)]))
        self.assertEqual(status, 1, out)
        self.assertIn("program header 0", out)

    def test_a_generic_change_shows_in_the_binary(self):
        """A change that only the binary holds fails there, though every library matches."""
        with tempfile.TemporaryDirectory() as tmp:
            built = {}
            for side, data in (("base", macho(ALL)), ("head", macho(swap(ALL, changed(JUMPS, b"\x09\x01\x02\x03"))))):
                lib = Path(tmp) / side / "lib.o"
                binary = Path(tmp) / side / "zakurad"
                lib.parent.mkdir()
                lib.write_bytes(macho(ALL))
                binary.write_bytes(data)
                built[side, "zakura-network", 4] = ([lib], [])
                built[side, "zakurad", 4] = ([binary], [])
            out = io.StringIO()
            with redirect_stdout(out):
                status = asm_diff.run_comparisons(asm_diff.comparisons(built, ["zakura-network"], [4], True))
        self.assertEqual(status, 1, out.getvalue())
        self.assertIn("zakura-network codegen-units=4: 1 files: identical", out.getvalue())
        self.assertIn("binary zakurad codegen-units=4: 1 files: differs", out.getvalue())


def aligned(base_text, head_text, extra=None):
    """Pads two versions of one `.rs` file as the gate does, through a real `git diff`.

    `extra` maps other file names to text both versions share. Returns each
    side's padded lines and the paddings each side made.
    """
    with tempfile.TemporaryDirectory() as tmp:
        repo = Path(tmp)
        git = ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@example.com"]
        subprocess.run(git + ["init", "-q"], check=True)
        # User settings that drop the a/ and b/ prefixes or merge nearby
        # hunks mustn't matter.
        subprocess.run(git + ["config", "diff.noprefix", "true"], check=True)
        subprocess.run(git + ["config", "diff.interHunkContext", "3"], check=True)
        for name, text in (extra or {}).items():
            (repo / name).write_text(text)
        revs = []
        for text in (base_text, head_text):
            (repo / "lib.rs").write_text(text)
            subprocess.run(git + ["add", "."], check=True)
            subprocess.run(git + ["commit", "-q", "--allow-empty", "-m", "x"], check=True)
            revs.append(subprocess.run(git + ["rev-parse", "HEAD"], stdout=subprocess.PIPE,
                                       text=True, check=True).stdout.strip())
        hunks = asm_diff.rust_hunks(repo, revs[0], revs[1])
        probes = asm_diff.embed_probes(repo, revs[0], revs[1], hunks)
        padded, made = [], []
        for side, text in (("base", base_text), ("head", head_text)):
            (repo / "lib.rs").write_text(text)
            made.append(asm_diff.align_tree(repo, hunks, side, probes))
            padded.append((repo / "lib.rs").read_text().split("\n"))
    return padded, made


def where(lines, text):
    """The 1-based line number of `text` in `lines`."""
    return lines.index(text) + 1


SOURCE = ["fn check(x: Option<u8>) -> u8 {", "    let a = 1;", "    x.expect(\"bad peer\")", "}",
          "fn log() {", "    tracing::info!(\"done\");", "}", ""]


class AlignmentTests(unittest.TestCase):
    def test_a_moved_panic_location_passes_and_is_listed(self):
        """A cfg-only line above an `expect` is padded on base, listed, and every unchanged line lines up."""
        head = SOURCE[:2] + ["    #[cfg(zakura_sim)]", "    let () = ();"] + SOURCE[2:]
        (base, head), made = aligned("\n".join(SOURCE), "\n".join(head))
        self.assertEqual(made, [[("lib.rs", 2, 2)], []])
        for line in ("    x.expect(\"bad peer\")", "    tracing::info!(\"done\");", "fn log() {"):
            self.assertEqual(where(base, line), where(head, line), line)

    def test_hunks_split_by_unchanged_lines_stay_apart(self):
        """A cfg-only statement moved past two unchanged lines still lines those lines up."""
        base = ["fn f() {", "    #[cfg(zakura_sim)]", "    let () = ();", "    a();", "    b();", "}", ""]
        head = ["fn f() {", "    a();", "    b();", "    #[cfg(zakura_sim)]", "    let () = ();", "}", ""]
        (base, head), _ = aligned("\n".join(base), "\n".join(head))
        for line in ("    a();", "    b();", "}"):
            self.assertEqual(where(base, line), where(head, line), line)

    def test_a_moved_release_line_stays_apart(self):
        """Release code that moved doesn't line up, so its location still differs and the bytes fail."""
        head = SOURCE[:2] + SOURCE[3:6] + [SOURCE[2]] + SOURCE[6:]
        (base, head), _ = aligned("\n".join(SOURCE), "\n".join(head))
        self.assertNotEqual(where(base, SOURCE[2]), where(head, SOURCE[2]))
        self.assertEqual(where(base, "fn check(x: Option<u8>) -> u8 {"), where(head, "fn check(x: Option<u8>) -> u8 {"))

    def test_an_empty_file_is_not_padded(self):
        """A file that was empty has no unchanged line to align, so it's left as committed, without a probe."""
        (base, head), made = aligned("", "#[cfg(test)]\nmod tests {}\n")
        self.assertEqual(made, [[], []])
        self.assertEqual((base, head), ([""], ["#[cfg(test)]", "mod tests {}", ""]))

    def test_both_sides_get_the_same_probe(self):
        """A file padded on one side ends with the same probe comment on both sides."""
        head = SOURCE[:2] + ["    #[cfg(zakura_sim)]", "    let () = ();"] + SOURCE[2:]
        (base, head), _ = aligned("\n".join(SOURCE), "\n".join(head))
        self.assertEqual(base[-1], "")
        self.assertRegex(base[-2], r"^// zakura-release-gate embed probe [0-9a-f]{32}$")
        self.assertEqual(base[-2:], head[-2:])
        self.assertEqual(len(base), len(head))

    def test_a_probe_after_a_missing_final_newline_gets_its_own_line(self):
        """A file that doesn't end in a newline gets one before its probe, so its last line stays code."""
        (base, head), made = aligned("fn a() {}\nfn b() {}", "fn a() {}\n#[cfg(test)]\nfn t() {}\nfn b() {}")
        self.assertEqual(made, [[("lib.rs", 1, 2)], []])
        self.assertEqual(base[-3], "fn b() {}")
        self.assertEqual(base[-2:], head[-2:])
        self.assertTrue(base[-2].startswith("// zakura-release-gate embed probe "), base)

    def test_a_probe_after_a_closed_comment_at_the_end_is_allowed(self):
        """A file that ends with a closed block comment and no final newline takes its probe on a new line."""
        (base, head), _ = aligned("fn a() {}\nfn b() {}\n/* done */", "fn a() {}\n#[cfg(test)]\nfn t() {}\nfn b() {}\n/* done */")
        self.assertEqual(base[-3], "/* done */")
        self.assertEqual(base[-2:], head[-2:])
        self.assertTrue(base[-2].startswith("// zakura-release-gate embed probe "), base)

    def test_a_probe_inside_a_literal_is_refused(self):
        """A file that ends inside a raw string or block comment can't take a probe."""
        for tail in ('r#"open', "/* open"):
            with self.assertRaises(asm_diff.PaddingError, msg=tail):
                aligned("fn a() {}\nfn b() {}\n" + tail, "fn a() {}\n#[cfg(test)]\nfn t() {}\nfn b() {}\n" + tail)

    def test_padding_inside_a_literal_is_refused(self):
        """A hunk that ends inside a string, raw string or block comment can't be padded."""
        for opener, closer in (('"', '"'), ('r#"', '"#'), ("/*", "*/")):
            base = "fn f() {\n    let s = %sone\ntwo%s;\n}\n" % (opener, closer)
            head = "fn f() {\n    let s = %sone\nthree\nfour\ntwo%s;\n}\n" % (opener, closer)
            with self.assertRaises(asm_diff.PaddingError, msg=opener):
                aligned(base, head)

    def test_a_forwarded_line_macro_is_left_to_the_line_check(self):
        """Padding doesn't scan for `line!`; a line number that reaches code fails the build-level line check."""
        head = SOURCE[:2] + ["    #[cfg(zakura_sim)]", "    let () = ();"] + SOURCE[2:]
        other = {"other.rs": "macro_rules! forward { ($m:ident) => { $m!() } }\nfn here() -> u32 { forward!(line) }\n"}
        _, made = aligned("\n".join(SOURCE), "\n".join(head), other)
        self.assertEqual(made, [[("lib.rs", 2, 2)], []])

    def test_literals_and_lifetimes_are_told_apart(self):
        """Char literals, lifetimes, nested comments, escaped newlines and raw strings are lexed right."""
        text = "\n".join([
            "fn f<'a>(x: &'a str) -> char {",  # 1: lifetimes, safe
            "    let q = '\"';",                 # 2: a char literal holding a quote, safe
            "    /* outer /* inner */",          # 3: still inside the outer comment
            "    still */ let s = \"a\\",        # 4: inside a string, before an escaped newline
            "b\";",                              # 5: safe
            "    br##\"x\"#",                    # 6: inside a raw byte string
            "\"##; 'x'",                         # 7: safe
            "}",                                 # 8: safe, the file's end
        ])
        self.assertEqual(sorted(asm_diff.safe_boundaries(text)), [0, 1, 2, 5, 7, 8])

    def test_line_map_hunks_parse(self):
        """Hunk headers parse with and without counts, and per file."""
        diff = "--- a/x.rs\n+++ b/x.rs\n@@ -3,0 +4,2 @@\n@@ -10 +12 @@\n--- /dev/null\n+++ b/new.rs\n@@ -0,0 +1 @@\n"
        self.assertEqual(asm_diff.parse_file_hunks(diff), {"x.rs": [(3, 0, 4, 2), (10, 1, 12, 1)]})


EXPLAIN = r"""
	.section	__TEXT,__text,regular,pure_instructions
	.globl	__RNvCs1_5crate5check
	.p2align	2
__RNvCs1_5crate5check:
	.loc	1 {line} 5
Lloh{f}0:
	adrp	x8, l_anon.{h}.3@PAGE
	ldr	x8, [x8, l_anon.{h}.3@PAGEOFF]
	cmp	x0, x8
	b.hi	LBB{f}_2
	ret
LBB{f}_2:
	{insn}
	ret

	.section	__TEXT,__literal8,8byte_literals
	.p2align	3, 0x0
l_anon.{h}.3:
	.quad	1000
"""


def explained(base, head):
    """Runs the `--explain` diagnostic on two crafted assembly files, returning what it printed."""
    with tempfile.TemporaryDirectory() as tmp:
        files = []
        for side, text in (("base", base), ("head", head)):
            path = Path(tmp) / side / "x.s"
            path.parent.mkdir()
            path.write_text(text)
            files.append([path])
        out = io.StringIO()
        with redirect_stdout(out):
            asm_explain.explain("assembly", files[0], files[1])
    return out.getvalue()


def explain_asm(h="aaaa", f="0", line=10, insn="mov\tx0, #1"):
    """Fills in the crafted assembly for the diagnostic."""
    return EXPLAIN.replace("{h}", h).replace("{f}", f).replace("{line}", str(line)).replace("{insn}", insn)


class ExplainTests(unittest.TestCase):
    def test_explain_names_the_differing_function(self):
        """The diagnostic ignores renumbered labels and moved lines, and names a changed function."""
        out = explained(explain_asm(), explain_asm(h="bbbb", f="7", line=12))
        self.assertIn("no difference found", out)
        out = explained(explain_asm(), explain_asm(insn="mov\tx0, #2"))
        self.assertIn("function differs: __RNvCs1_5crate5check", out)


# A fake `cargo`: `metadata` prints an empty workspace, and any build records
# the build metadata that Git and `SOURCE_DATE_EPOCH` give a crate directory,
# the way `zakurad`'s build script reads it. The first `FAKE_CARGO_PASSES`
# builds succeed, each leaving an empty rlib, and later ones fail with
# status 7.
FAKE_CARGO = """#!/bin/sh
if [ "$1" = metadata ]; then echo '{"packages": []}'; exit 0; fi
cd crates/x
echo "$(git rev-parse HEAD) $(git describe --always --tags --match 'v*.*.*') $SOURCE_DATE_EPOCH" >> "$FAKE_CARGO_LOG"
if [ "$(wc -l < "$FAKE_CARGO_LOG")" -le "${FAKE_CARGO_PASSES:-0}" ]; then
  while [ "$1" != "-p" ]; do shift; done
  mkdir -p "$CARGO_TARGET_DIR/release/deps"
  printf '!<arch>\\n' > "$CARGO_TARGET_DIR/release/deps/lib$(echo "$2" | tr - _)-0.rlib"
  exit 0
fi
exit 7
"""


# A fake `cargo` that "builds" the crate `-p` names, `x` or `y`, as a Mach-O
# file holding the line count of `crates/<crate>/lib.rs`, in an instruction
# section when `FAKE_LINES_IN` is `code` and in a data section when it's
# `data`, the way a line number reaches code or only data. When it's
# `embed`, the data section holds the source file itself, as
# `use std::include_str as embed; static S: &str = embed!("lib.rs");` would.
# When it's `debug`, only a debug section varies, sized by the source file,
# and only when the build asks for debug info, as rustc's does by default.
# Like cargo, it skips a crate whose source is older than its last build.
FAKE_OBJECT_CARGO = """#!/usr/bin/env python3
import json, os, struct, sys
from pathlib import Path
%s
if sys.argv[1] == "metadata":
    print(json.dumps({"packages": [{"name": name, "targets": [{"name": name, "kind": ["lib"]}]} for name in "xy"]}))
    sys.exit(0)
crate = sys.argv[sys.argv.index("-p") + 1]
source = Path("crates") / crate / "lib.rs"
deps = Path(os.environ["CARGO_TARGET_DIR"]) / "release" / "deps"
deps.mkdir(parents=True, exist_ok=True)
stamp = deps / (crate + ".stamp")
if stamp.exists() and stamp.stat().st_mtime >= source.stat().st_mtime:
    sys.exit(0)
stamp.write_text("")
lines = struct.pack("<I", source.read_text().count("\\n"))
mode = os.environ["FAKE_LINES_IN"]
text = source.read_bytes()
# The embed modes store the file whole at a place in data. `newlines`
# stands for line-number data that holds newline bytes.
code, data = {"code": (lines, b"same"), "data": (b"same", lines), "embed": (b"same", text),
              "embed-start": (b"same", text + b"tail"), "embed-middle": (b"same", b"lead" + text + b"tail"),
              "embed-end": (b"same", b"lead" + text), "embed-newline": (b"same", b"\\n" + text),
              "newlines": (b"same", b"\\n" * text.count(b"\\n")), "debug": (b"same", b"same")}[mode]
debug = [] if "debuginfo=0" in sys.argv else [("__DWARF", "__debug_line", b"d" * len(source.read_bytes()), 0)]
sections = debug + [("__TEXT", "__text", code, 0x80000400), ("__DATA_CONST", "__const", data, 0)]
member = macho(sections)
name = (crate + "-0.o").encode()
header = b"#1/%%-13d" %% len(name) + b"0" * 12 + b"0" * 6 + b"0" * 6 + b"100644  " + b"%%-10d" %% (len(name) + len(member)) + b"`\\n"
archive = b"!<arch>\\n" + header + name + member + (b"\\n" if (len(name) + len(member)) %% 2 else b"")
(deps / ("lib" + crate + "-0.rlib")).write_bytes(archive)
""" % inspect.getsource(macho)


class LineCheckTests(unittest.TestCase):
    def run_gate(self, lines_in, head=None, base=SOURCE):
        """Runs the gate on a change to `crates/x/lib.rs`, a cfg-only hook by default, with the fake object cargo."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp) / "repo"
            for crate in "xy":
                (repo / "crates" / crate).mkdir(parents=True)
            (repo / "crates" / "y" / "lib.rs").write_text("\n".join(SOURCE))
            git = ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@example.com"]
            subprocess.run(git + ["init", "-q"], check=True)
            hook = head or SOURCE[:2] + ["    #[cfg(zakura_sim)]", "    let () = ();"] + SOURCE[2:]
            for lines in (base, hook):
                (repo / "crates" / "x" / "lib.rs").write_text("\n".join(lines))
                subprocess.run(git + ["add", "."], check=True)
                subprocess.run(git + ["commit", "-q", "-m", "x"], check=True)
            cargo = Path(tmp) / "cargo"
            cargo.write_text(FAKE_OBJECT_CARGO)
            os.chmod(cargo, 0o755)
            out, err = io.StringIO(), io.StringIO()
            os.environ["FAKE_LINES_IN"] = lines_in
            try:
                with redirect_stdout(out), redirect_stderr(err):
                    status = asm_diff.main([
                        "--repo", str(repo), "--base", "HEAD~1", "--head", "HEAD", "--cargo", str(cargo),
                        "--package", "x", "--package", "y", "--cgu", "4", "--no-binary",
                        "--work-dir", str(Path(tmp) / "work"),
                    ])
            finally:
                del os.environ["FAKE_LINES_IN"]
        return status, out.getvalue() + err.getvalue()

    def test_a_line_number_that_reaches_code_fails(self):
        """A line number that padding moves into an instruction, as a forwarded `line!` would, exits 1."""
        status, output = self.run_gate("code")
        self.assertEqual(status, 1, output)
        self.assertIn("x codegen-units=4: 1 files: identical", output)
        self.assertIn("line check base x codegen-units=4: code changed by padding", output)
        self.assertIn("section __TEXT,__text", output)

    def test_a_line_number_that_reaches_only_data_passes(self):
        """A line number in data only, as a panic location's is, is outside the contract."""
        status, output = self.run_gate("data")
        self.assertEqual(status, 0, output)
        self.assertIn("line check base x codegen-units=4: code unchanged by padding", output)

    def test_an_aliased_embed_of_a_padded_file_fails(self):
        """A padded file embedded in release data, under any macro name, exits 1, though padding made the builds equal."""
        status, output = self.run_gate("embed", head=SOURCE[:2] + ["", ""] + SOURCE[2:])
        self.assertEqual(status, 1, output)
        self.assertIn("x codegen-units=4: 1 files: identical", output)
        self.assertIn("embed check base x codegen-units=4: padded files embedded: crates/x/lib.rs", output)

    def test_a_padded_file_that_is_not_embedded_passes_the_embed_check(self):
        """A padded file whose text appears in no data section passes."""
        status, output = self.run_gate("data")
        self.assertEqual(status, 0, output)
        self.assertIn("embed check base x codegen-units=4: no padded file embedded", output)

    def test_an_embed_at_any_place_in_data_fails(self):
        """A padded file embedded at the start, middle or end of a data section exits 1."""
        for mode in ("embed-start", "embed-middle", "embed-end"):
            with self.subTest(mode):
                status, output = self.run_gate(mode, head=SOURCE[:2] + ["", ""] + SOURCE[2:])
                self.assertEqual(status, 1, output)
                self.assertIn("x codegen-units=4: 1 files: identical", output)
                self.assertIn("embed check base x codegen-units=4: padded files embedded: crates/x/lib.rs", output)

    def test_an_embed_padded_at_its_start_next_to_a_newline_fails(self):
        """A file padded at its start and embedded after a newline byte, so its padded text appears in both builds, exits 1."""
        status, output = self.run_gate("embed-newline", head=[""] + SOURCE)
        self.assertEqual(status, 1, output)
        self.assertIn("x codegen-units=4: 1 files: identical", output)
        self.assertIn("embed check base x codegen-units=4: padded files embedded: crates/x/lib.rs", output)

    def test_a_whitespace_only_file_that_is_not_embedded_passes(self):
        """A padded file of blank lines isn't reported because data that line numbers change holds newline bytes."""
        status, output = self.run_gate("newlines", base=["", ""], head=["#[cfg(test)]", "mod t {}", "", ""])
        self.assertEqual(status, 0, output)
        self.assertIn("padded base: 2 blank lines after crates/x/lib.rs:0", output)
        self.assertIn("embed check base x codegen-units=4: no padded file embedded", output)

    def test_debug_info_size_alone_passes(self):
        """A change that only resizes debug info, which moves later sections' addresses, passes."""
        status, output = self.run_gate("debug")
        self.assertEqual(status, 0, output)
        self.assertIn("x codegen-units=4: 1 files: identical", output)

    def test_unpadded_crates_rebuild_for_the_line_check(self):
        """A crate with no padded file still rebuilds for the committed tree, so its line check has objects."""
        status, output = self.run_gate("data")
        self.assertEqual(status, 0, output)
        self.assertIn("line check base y codegen-units=4: code unchanged by padding", output)


class CommandTests(unittest.TestCase):
    def run_fake(self, tmp, base="HEAD", passes=0):
        """Runs the tool on a two-commit repository with the fake cargo, returning status, output and log."""
        repo = Path(tmp) / "repo"
        (repo / "crates" / "x").mkdir(parents=True)
        git = ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@example.com"]
        subprocess.run(git + ["init", "-q"], check=True)
        for text in ("x\n", "y\n"):
            (repo / "crates" / "x" / "README").write_text(text)
            subprocess.run(git + ["add", "."], check=True)
            subprocess.run(git + ["commit", "-q", "-m", text.strip()], check=True)
        cargo = Path(tmp) / "cargo"
        cargo.write_text(FAKE_CARGO)
        os.chmod(cargo, 0o755)
        log = Path(tmp) / "cargo.log"
        log.write_text("")
        out, err = io.StringIO(), io.StringIO()
        os.environ["FAKE_CARGO_LOG"] = str(log)
        os.environ["FAKE_CARGO_PASSES"] = str(passes)
        try:
            with redirect_stdout(out), redirect_stderr(err):
                status = asm_diff.main([
                    "--repo", str(repo), "--base", base, "--head", "HEAD", "--cargo", str(cargo),
                    "--no-binary", "--work-dir", str(Path(tmp) / "work"),
                ])
        finally:
            del os.environ["FAKE_CARGO_LOG"]
            del os.environ["FAKE_CARGO_PASSES"]
        return status, out.getvalue() + err.getvalue(), log.read_text()

    def test_a_failed_command_exits_nonzero(self):
        """A cargo build that fails makes the run exit 3, and the worktree is removed."""
        with tempfile.TemporaryDirectory() as tmp:
            status, output, _ = self.run_fake(tmp)
            self.assertEqual(status, 3, output)
            self.assertIn("exit status 7", output)
            self.assertFalse((Path(tmp) / "work" / "src").exists())

    def test_an_unrelated_work_dir_is_refused(self):
        """A work directory that holds files no run made is refused, and nothing in it is deleted."""
        with tempfile.TemporaryDirectory() as tmp:
            keep = Path(tmp) / "work" / "src" / "keep.txt"
            keep.parent.mkdir(parents=True)
            keep.write_text("mine\n")
            status, output, _ = self.run_fake(tmp)
            self.assertEqual(status, 3, output)
            self.assertIn("isn't empty and no earlier run made it", output)
            self.assertEqual(keep.read_text(), "mine\n")

    def test_build_overrides_are_dropped(self):
        """A caller's compiler flags and profile overrides don't reach the builds."""
        names = ["RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_PROFILE_RELEASE_CODEGEN_UNITS",
                 "CARGO_TARGET_AARCH64_APPLE_DARWIN_RUSTFLAGS", "CARGO_INCREMENTAL",
                 "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER"]
        saved = {name: os.environ.get(name) for name in names}
        try:
            for name in names:
                os.environ[name] = "--cfg zakura_sim"
            env = asm_diff.build_env(Path("/target"), {}, {"CARGO_PROFILE_RELEASE_CODEGEN_UNITS": "1"})
        finally:
            for name, value in saved.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value
        self.assertEqual([name for name in names if name in env], ["CARGO_PROFILE_RELEASE_CODEGEN_UNITS"])
        self.assertEqual(env["CARGO_PROFILE_RELEASE_CODEGEN_UNITS"], "1")
        self.assertEqual(env["CARGO_TARGET_DIR"], "/target")

    def test_both_sides_see_the_base_metadata(self):
        """Both sides' builds see the base revision's SHA, description and commit time."""
        with tempfile.TemporaryDirectory() as tmp:
            # The base side's four library builds pass, and the head side's first build fails.
            status, output, log = self.run_fake(tmp, base="HEAD~1", passes=4)
            repo = Path(tmp) / "repo"
            base = subprocess.run(["git", "-C", str(repo), "rev-parse", "HEAD~1"],
                                  stdout=subprocess.PIPE, text=True, check=True).stdout.strip()
        self.assertEqual(status, 3, output)
        rows = [tuple(line.split()) for line in log.splitlines()]
        self.assertEqual(len(rows), 5, log)
        self.assertEqual(set(rows), {(base, base[:7], rows[0][2])}, log)
        self.assertNotEqual(rows[0][2], "0", log)


if __name__ == "__main__":
    unittest.main()
