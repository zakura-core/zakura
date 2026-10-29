import importlib.util
import io
import os
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


# A crafted rustc-style aarch64 Mach-O file: `check` reads a constant limit
# and calls `expect_failed` with a message and a `core::panic::Location` at
# `crates/x/src/lib.rs:10:5`. `{h}` is the anonymous-constant hash, `{f}`
# the function index in label names, and `{line}` and `{msg}` the
# location's line and its message. The path is 19 bytes, octal `\\023`.
BASE = r"""
	.section	__TEXT,__text,regular,pure_instructions
	.build_version macos, 11, 0
	.globl	__RNvCs1_5crate5check
	.p2align	2
__RNvCs1_5crate5check:
Lfunc_begin{f}:
	.file	1 "/work/{h}" "crates/x/src/lib.rs"
	.loc	1 {line} 5 prologue_end
	.cfi_startproc
Lloh{f}0:
	adrp	x8, l_anon.{h}.3@PAGE
Lloh{f}1:
	ldr	x8, [x8, l_anon.{h}.3@PAGEOFF]
	cmp	x0, x8
	b.hi	LBB{f}_2
	ret
LBB{f}_2:
Lloh{f}2:
	adrp	x0, l_anon.{h}.0@PAGE
Lloh{f}3:
	add	x0, x0, l_anon.{h}.0@PAGEOFF
	mov	w1, #{msglen}
Lloh{f}4:
	adrp	x2, l_anon.{h}.2@PAGE
Lloh{f}5:
	add	x2, x2, l_anon.{h}.2@PAGEOFF
	bl	__RNvNtCs2_4core6option13expect_failed
	.loh AdrpAdd	Lloh{f}4, Lloh{f}5
	.loh AdrpAdd	Lloh{f}2, Lloh{f}3
	.loh AdrpLdr	Lloh{f}0, Lloh{f}1
Lfunc_end{f}:
	.cfi_endproc
{extra_function}
	.section	__TEXT,__const
l_anon.{h}.0:
	.ascii	"{msg}"

	.section	__TEXT,__cstring,cstring_literals
l_anon.{h}.1:
	.asciz	"crates/x/src/lib.rs"

	.section	__DATA,__const
	.p2align	3, 0x0
l_anon.{h}.2:
	.quad	l_anon.{h}.1
	.asciz	"\023\000\000\000\000\000\000\000{lineoctal}\000\000\000\005\000\000"

	.section	__TEXT,__literal8,8byte_literals
	.p2align	3, 0x0
l_anon.{h}.3:
	.quad	{limit}

.subsections_via_symbols
"""

SECOND_FUNCTION = r"""
	.globl	__RNvCs1_5crate4zero
	.p2align	2
__RNvCs1_5crate4zero:
	mov	x0, #0
	ret
"""

# A `tracing` event callsite at `crates/x/src/lib.rs:{line}`: its name
# string, its file string, and its metadata, whose second and third words
# are the line as `Some(line)`.
CALLSITE = r"""
	.section	__TEXT,__text,regular,pure_instructions
	.globl	__RNvCs1_5crate4emit
	.p2align	2
__RNvCs1_5crate4emit:
	adrp	x0, __RNvNvCs1_5crate4emit8CALLSITE4META@PAGE
	ret

	.section	__TEXT,__const
l_anon.{h}.5:
	.ascii	"event crates/x/src/lib.rs:{line}"
l_anon.{h}.6:
	.ascii	"crates/x/src/lib.rs"

	.section	__DATA,__const
	.globl	__RNvNvCs1_5crate4emit8CALLSITE4META
	.p2align	3, 0x0
__RNvNvCs1_5crate4emit8CALLSITE4META:
	.asciz	"\001\000\000\000\000\000\000\000\001\000\000\000{lineoctal}\000\000"
	.quad	l_anon.{h}.5
	.asciz	"{namelen}\000\000\000\000\000\000"
	.quad	l_anon.{h}.6
	.ascii	"\023\000\000\000\000\000\000\000{level}"
	.space	7
"""

# An ordinary record laid out like a callsite's first words, a count of 1,
# a flag of 1 and a limit of `{line}`, then a pointer to a source path.
CONFIG = r"""
	.section	__TEXT,__text,regular,pure_instructions
	.globl	__RNvCs1_5crate4conf
	.p2align	2
__RNvCs1_5crate4conf:
	adrp	x0, __RNvCs1_5crate6CONFIG@PAGE
	ret

	.section	__TEXT,__const
l_anon.{h}.7:
	.ascii	"crates/x/src/lib.rs"

	.section	__DATA,__const
	.globl	__RNvCs1_5crate6CONFIG
	.p2align	3, 0x0
__RNvCs1_5crate6CONFIG:
	.asciz	"\001\000\000\000\000\000\000\000\001\000\000\000{lineoctal}\000\000"
	.quad	l_anon.{h}.7
	.asciz	"\023\000\000\000\000\000\000"
"""


def render(template, h="aaaa", f="0", line=10, msg="bad peer", limit=1000, extra_function="", level=2):
    """Fills in one of the crafted templates."""
    return (
        template.replace("{h}", h).replace("{f}", f).replace("{line}", str(line))
        .replace("{lineoctal}", "\\%03o" % line).replace("{msg}", msg)
        .replace("{msglen}", str(len(msg))).replace("{limit}", str(limit))
        .replace("{namelen}", "\\%03o" % len("event crates/x/src/lib.rs:%d" % line))
        .replace("{extra_function}", extra_function).replace("{level}", "\\%03o" % level)
    )


def no_map(path, line):
    """A line map for an empty diff: every line stays where it was."""
    return line


def inserted_above(at, count=1):
    """A line map for `count` lines inserted after line `at` of `crates/x/src/lib.rs`."""
    hunks = asm_diff.parse_hunks("@@ -%d,0 +%d,%d @@\n" % (at, at + 1, count))

    def line_map(path, line):
        """Maps `crates/x/src/lib.rs` through the insertion, and any other file to itself."""
        return asm_diff.map_line(hunks, line) if path == "crates/x/src/lib.rs" else line

    return line_map


def compare(base_texts, head_texts, line_map=no_map):
    """Compares crafted files, returning the exit status and what it printed."""
    out = io.StringIO()
    with redirect_stdout(out):
        status = asm_diff.compare_loaded(
            "test", asm_diff.load_texts(base_texts), asm_diff.load_texts(head_texts), line_map)
    return status, out.getvalue()


class ComparatorTests(unittest.TestCase):
    def test_identical_asm_passes(self):
        """Renumbered labels, a new constant hash and moved `.loc` lines aren't differences."""
        status, out = compare([render(BASE)], [render(BASE, h="bbbb", f="7")])
        self.assertEqual(status, 0, out)
        self.assertIn("identical", out)

    def test_a_changed_instruction_fails(self):
        """A changed instruction fails."""
        head = render(BASE).replace("cmp\tx0, x8", "cmp\tx1, x8")
        status, out = compare([render(BASE)], [head])
        self.assertEqual(status, 1, out)
        self.assertIn("function differs: __RNvCs1_5crate5check", out)

    def test_a_changed_constant_fails(self):
        """A changed anonymous constant that the function reads fails."""
        status, out = compare([render(BASE)], [render(BASE, limit=1001)])
        self.assertEqual(status, 1, out)
        self.assertIn("constant 0:l_anon.aaaa.3 read by __RNvCs1_5crate5check", out)

    def test_a_missing_function_fails(self):
        """A function only one side has fails."""
        status, out = compare([render(BASE, extra_function=SECOND_FUNCTION)], [render(BASE)])
        self.assertEqual(status, 1, out)
        self.assertIn("function only in base: __RNvCs1_5crate4zero", out)

    def test_an_empty_side_fails(self):
        """A side with no functions exits 2."""
        empty = "\t.section\t__TEXT,__text,regular,pure_instructions\n.subsections_via_symbols\n"
        status, out = compare([render(BASE)], [empty])
        self.assertEqual(status, 2, out)

    def test_a_moved_panic_location_passes_and_is_listed(self):
        """A location that a line inserted above it moved passes, and is printed."""
        status, out = compare([render(BASE)], [render(BASE, h="bbbb", line=11)], inserted_above(7))
        self.assertEqual(status, 0, out)
        self.assertIn("masked location crates/x/src/lib.rs:10 -> 11", out)

    def test_a_changed_string_naming_llvm_fails(self):
        """A string that merely looks like a thin LTO suffix is still compared."""
        status, out = compare([render(BASE, msg="a.llvm.111")], [render(BASE, msg="a.llvm.222")])
        self.assertEqual(status, 1, out)

    def test_a_changed_constructor_entry_fails(self):
        """Unlabeled data, such as a constructor table's entry, is compared."""
        table = "\t.section\t__DATA,__mod_init_func,mod_init_funcs\n\t.p2align\t3, 0x0\n\t.quad\t%s\n"
        base = render(BASE, extra_function=SECOND_FUNCTION) + table % "__RNvCs1_5crate5check"
        head = render(BASE, extra_function=SECOND_FUNCTION) + table % "__RNvCs1_5crate4zero"
        status, out = compare([base], [head])
        self.assertEqual(status, 1, out)
        self.assertIn("unlabeled data in", out)

    def test_a_changed_alignment_fails(self):
        """A constant that keeps its contents but changes its alignment fails."""
        head = render(BASE).replace("\t.p2align\t3, 0x0\nl_anon.aaaa.3:", "\t.p2align\t5, 0x0\nl_anon.aaaa.3:")
        self.assertNotEqual(head, render(BASE))
        status, out = compare([render(BASE)], [head])
        self.assertEqual(status, 1, out)
        self.assertIn("placement", out)

    def test_a_location_column_must_fit_its_line(self):
        """A moved location whose column isn't on its source line fails."""

        class ShortLines:
            """A line map for one inserted line, where every source line is 3 columns wide."""

            def __call__(self, path, line):
                """Maps through the insertion."""
                return inserted_above(7)(path, line)

            def fits(self, path, line, column):
                """Only columns 1 to 4 fit."""
                return 1 <= column <= 4

        status, out = compare([render(BASE)], [render(BASE, line=11)], ShortLines())
        self.assertEqual(status, 1, out)
        self.assertIn("location crates/x/src/lib.rs:10:5 isn't a column of that line", out)

    def test_a_moved_promoted_location_passes(self):
        """A location and path string that thin LTO promoted to global symbols are still masked."""

        def promoted(text):
            """Renames the location and its path string as thin LTO promotes them."""
            return text.replace("l_anon.aaaa.1", "_anon.aaaa.1").replace("l_anon.aaaa.2", "_anon.aaaa.2")

        status, out = compare([promoted(render(BASE))], [promoted(render(BASE, line=11))], inserted_above(7))
        self.assertEqual(status, 0, out)
        self.assertIn("masked location crates/x/src/lib.rs:10 -> 11", out)

    def test_a_changed_panic_message_fails(self):
        """A message change fails beside a moved, masked location, and so does a callsite's other field."""
        status, out = compare([render(BASE)], [render(BASE, line=11, msg="bad Peer")], inserted_above(7))
        self.assertEqual(status, 1, out)
        self.assertIn("constant 0:l_anon.aaaa.0 read by __RNvCs1_5crate5check", out)
        status, out = compare([render(CALLSITE, line=99)], [render(CALLSITE, line=100, level=3)], inserted_above(7))
        self.assertEqual(status, 1, out)
        self.assertIn("CALLSITE4META (bytes differ at offset 48)", out)

    def test_an_ordinary_record_is_not_masked_as_a_callsite(self):
        """A record that isn't a callsite fails when a word after a 1 changes, even to a mapped line."""
        status, out = compare([render(CALLSITE, line=99) + render(CONFIG, line=10)],
                              [render(CALLSITE, line=100) + render(CONFIG, line=11)], inserted_above(7))
        self.assertEqual(status, 1, out)
        self.assertIn("data differs: __RNvCs1_5crate6CONFIG (contents differ)", out)

    def test_a_changed_string_length_fails(self):
        """A string length that changes alone fails, though the string is the same."""
        head = render(BASE).replace('"\\023\\000\\000\\000', '"\\024\\000\\000\\000', 1)
        self.assertNotEqual(head, render(BASE))
        status, out = compare([render(BASE)], [head])
        self.assertEqual(status, 1, out)

    def test_a_moved_section_fails(self):
        """Unchanged data moved to another section fails."""
        head = render(BASE).replace("__TEXT,__literal8,8byte_literals", "__DATA,__mod_init_func,mod_init_funcs")
        status, out = compare([render(BASE)], [head])
        self.assertEqual(status, 1, out)
        self.assertIn("section", out)

    def test_thinlto_suffixes_are_ignored(self):
        """Thin LTO's `.llvm.<hash>` suffixes on promoted locals aren't differences."""
        base = render(BASE).replace("__RNvNtCs2_4core6option13expect_failed", "__RNvCs1_5crate4help.llvm.111")
        head = render(BASE).replace("__RNvNtCs2_4core6option13expect_failed", "__RNvCs1_5crate4help.llvm.222")
        status, out = compare([base], [head])
        self.assertEqual(status, 0, out)

    def test_a_moved_release_line_fails(self):
        """A location whose line moved with no diff mapping fails."""
        status, out = compare([render(BASE)], [render(BASE, line=11)])
        self.assertEqual(status, 1, out)
        self.assertIn("line 10 -> 11 of crates/x/src/lib.rs isn't mapped by the diff", out)

    def test_a_moved_tracing_callsite_passes_and_is_listed(self):
        """A callsite's line and name that a line inserted above moved pass, and are printed."""
        status, out = compare([render(CALLSITE, line=99)], [render(CALLSITE, h="bbbb", line=100)], inserted_above(7))
        self.assertEqual(status, 0, out)
        self.assertIn("masked callsite crates/x/src/lib.rs:99 -> 100", out)
        self.assertIn("masked callsite name crates/x/src/lib.rs:99 -> 100", out)
        status, out = compare([render(CALLSITE, line=99)], [render(CALLSITE, line=101)], inserted_above(7))
        self.assertEqual(status, 1, out)


class BinaryTests(unittest.TestCase):
    def test_a_generic_change_shows_in_the_binary(self):
        """A constant that only a downstream instantiation holds fails in the binary's assembly."""
        with tempfile.TemporaryDirectory() as tmp:
            files = {}
            for name, text in (("lib-base", render(BASE)), ("lib-head", render(BASE, h="bbbb")),
                               ("bin-base", render(BASE)), ("bin-head", render(BASE, limit=1001))):
                path = Path(tmp) / name / "x.s"
                path.parent.mkdir()
                path.write_text(text)
                files[name] = [path]
            asm = {
                ("base", "zakura-network", 4): files["lib-base"],
                ("head", "zakura-network", 4): files["lib-head"],
                ("base", "zakurad", 4): files["bin-base"],
                ("head", "zakurad", 4): files["bin-head"],
            }
            out = io.StringIO()
            with redirect_stdout(out):
                status = asm_diff.run_comparisons(asm_diff.comparisons(asm, ["zakura-network"], [4], True), no_map)
        self.assertEqual(status, 1, out.getvalue())
        self.assertIn("zakura-network codegen-units=4: 1 functions, 4 data blocks compared, 0 lines masked: identical",
                      out.getvalue())
        self.assertIn("binary zakurad codegen-units=4: 1 functions, 4 data blocks compared, 0 lines masked: differs",
                      out.getvalue())


class LineMapTests(unittest.TestCase):
    def test_lines_map_through_unchanged_lines_only(self):
        """Lines shift past insertions and deletions, and a changed line maps to nothing."""
        hunks = asm_diff.parse_hunks("@@ -3,0 +4,2 @@\n@@ -10 +12 @@\n@@ -20,2 +21,0 @@\n")
        self.assertEqual(asm_diff.map_line(hunks, 3), 3)
        self.assertEqual(asm_diff.map_line(hunks, 4), 6)
        self.assertIsNone(asm_diff.map_line(hunks, 10))
        self.assertEqual(asm_diff.map_line(hunks, 11), 13)
        self.assertIsNone(asm_diff.map_line(hunks, 21))
        self.assertEqual(asm_diff.map_line(hunks, 19), 21)
        self.assertEqual(asm_diff.map_line(hunks, 22), 22)


# A fake `cargo`: `metadata` prints an empty workspace, and any build records
# the build metadata that Git and `SOURCE_DATE_EPOCH` give a crate directory,
# the way `zakurad`'s build script reads it. Then a base build succeeds when
# `FAKE_CARGO_PASS_BASE` is set, and any other build fails with status 7.
FAKE_CARGO = """#!/bin/sh
if [ "$1" = metadata ]; then echo '{"packages": []}'; exit 0; fi
side=$(basename "$PWD")
cd crates/x
echo "$side $(git rev-parse HEAD) $(git describe --always --tags --match 'v*.*.*') $SOURCE_DATE_EPOCH" >> "$FAKE_CARGO_LOG"
[ "$side" = base ] && [ -n "$FAKE_CARGO_PASS_BASE" ] && exit 0
exit 7
"""


class CommandTests(unittest.TestCase):
    def run_fake(self, tmp, base="HEAD", pass_base=False):
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
        if pass_base:
            os.environ["FAKE_CARGO_PASS_BASE"] = "1"
        try:
            with redirect_stdout(out), redirect_stderr(err):
                status = asm_diff.main([
                    "--repo", str(repo), "--base", base, "--head", "HEAD", "--cargo", str(cargo),
                    "--no-binary", "--work-dir", str(Path(tmp) / "work"),
                ])
        finally:
            del os.environ["FAKE_CARGO_LOG"]
            os.environ.pop("FAKE_CARGO_PASS_BASE", None)
        return status, out.getvalue() + err.getvalue(), log.read_text()

    def test_a_failed_command_exits_nonzero(self):
        """A cargo build that fails makes the run exit 3, and the worktrees are removed."""
        with tempfile.TemporaryDirectory() as tmp:
            status, output, _ = self.run_fake(tmp)
            self.assertEqual(status, 3, output)
            self.assertIn("exit status 7", output)
            self.assertFalse((Path(tmp) / "work" / "base").exists())

    def test_an_unrelated_work_dir_is_refused(self):
        """A work directory that holds files no run made is refused, and nothing in it is deleted."""
        with tempfile.TemporaryDirectory() as tmp:
            keep = Path(tmp) / "work" / "base" / "keep.txt"
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
            status, output, log = self.run_fake(tmp, base="HEAD~1", pass_base=True)
            repo = Path(tmp) / "repo"
            base = subprocess.run(["git", "-C", str(repo), "rev-parse", "HEAD~1"],
                                  stdout=subprocess.PIPE, text=True, check=True).stdout.strip()
        self.assertEqual(status, 3, output)
        rows = [line.split() for line in log.splitlines()]
        self.assertEqual(sorted({row[0] for row in rows}), ["base", "head"], log)
        self.assertEqual({tuple(row[1:]) for row in rows}, {(base, base[:7], rows[0][3])}, log)
        self.assertNotEqual(rows[0][3], "0", log)

    def test_native_input_changes_are_reported(self):
        """A change to C sources, build scripts or `Cargo.lock` is listed, and Rust sources aren't."""
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            git = ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@example.com"]
            subprocess.run(git + ["init", "-q"], check=True)
            (repo / "README").write_text("x\n")
            subprocess.run(git + ["add", "."], check=True)
            subprocess.run(git + ["commit", "-q", "-m", "x"], check=True)
            for name in ("Cargo.lock", "crates/x/build.rs", "crates/x/src/lib.rs", "vendor/y/z.c"):
                (repo / name).parent.mkdir(parents=True, exist_ok=True)
                (repo / name).write_text("1\n")
            subprocess.run(git + ["add", "."], check=True)
            subprocess.run(git + ["commit", "-q", "-m", "y"], check=True)
            changed = asm_diff.native_changes(repo, "HEAD~1", "HEAD")
        self.assertEqual(sorted(changed), ["Cargo.lock", "crates/x/build.rs", "vendor/y/z.c"])

if __name__ == "__main__":
    unittest.main()
