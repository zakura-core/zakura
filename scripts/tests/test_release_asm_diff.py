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
	.ascii	"\023\000\000\000\000\000\000\000\002"
	.space	7
"""

# A record that holds a location and its message inline.
PANIC_SITE = r"""
	.section	__TEXT,__text,regular,pure_instructions
	.globl	__RNvCs1_5crate4site
	.p2align	2
__RNvCs1_5crate4site:
	adrp	x0, __RNvCs1_5crate4SITE@PAGE
	ret

	.section	__TEXT,__cstring,cstring_literals
l_anon.{h}.7:
	.asciz	"crates/x/src/lib.rs"

	.section	__DATA,__const
	.globl	__RNvCs1_5crate4SITE
	.p2align	3, 0x0
__RNvCs1_5crate4SITE:
	.quad	l_anon.{h}.7
	.ascii	"\023\000\000\000\000\000\000\000{lineoctal}\000\000\000\005\000\000\000"
	.ascii	"{msg}"
"""


def render(template, h="aaaa", f="0", line=10, msg="bad peer", limit=1000, extra_function=""):
    """Fills in one of the crafted templates."""
    return (
        template.replace("{h}", h).replace("{f}", f).replace("{line}", str(line))
        .replace("{lineoctal}", "\\%03o" % line).replace("{msg}", msg)
        .replace("{msglen}", str(len(msg))).replace("{limit}", str(limit))
        .replace("{namelen}", "\\%03o" % len("event crates/x/src/lib.rs:%d" % line))
        .replace("{extra_function}", extra_function)
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

    def test_a_changed_panic_message_fails(self):
        """A message change fails, even beside a location that moved and is masked."""
        status, out = compare([render(BASE)], [render(BASE, line=11, msg="bad Peer")], inserted_above(7))
        self.assertEqual(status, 1, out)
        status, out = compare([render(PANIC_SITE)], [render(PANIC_SITE, line=11, msg="bad Peer")], inserted_above(7))
        self.assertEqual(status, 1, out)
        self.assertIn("data differs: __RNvCs1_5crate4SITE (bytes differ at offset 28)", out)

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


class DownstreamTests(unittest.TestCase):
    def test_a_generic_change_shows_only_downstream(self):
        """A constant that only a downstream crate's instantiation holds fails there."""
        with tempfile.TemporaryDirectory() as tmp:
            files = {}
            for name, text in (("lib-base", render(BASE)), ("lib-head", render(BASE, h="bbbb")),
                               ("down-base", render(BASE)), ("down-head", render(BASE, limit=1001))):
                path = Path(tmp) / name / "x.s"
                path.parent.mkdir()
                path.write_text(text)
                files[name] = [path]
            asm = {
                ("base", "zakura-network", 4): files["lib-base"],
                ("head", "zakura-network", 4): files["lib-head"],
                ("base", "zakura", 4): files["down-base"],
                ("head", "zakura", 4): files["down-head"],
            }
            out = io.StringIO()
            with redirect_stdout(out):
                status = asm_diff.run_comparisons(
                    asm_diff.comparisons(asm, ["zakura-network"], [4], ["zakura"]), no_map)
        self.assertEqual(status, 1, out.getvalue())
        self.assertIn("zakura-network codegen-units=4: 1 functions", out.getvalue())
        self.assertIn("downstream zakura codegen-units=4: 1 functions, 4 data blocks compared, 0 lines masked: differs",
                      out.getvalue())

    def test_downstream_packages_are_linked_dependents(self):
        """Downstream packages are libraries `zakurad` links that depend on a compared one."""

        def package(name, deps, kinds=("lib",)):
            """One `cargo metadata` package entry."""
            return {"name": name, "targets": [{"kind": [kind]} for kind in kinds],
                    "dependencies": [{"name": dep, "kind": kind} for dep, kind in deps]}

        metadata = {"packages": [
            package("zakura", [("zakura-network", None), ("zakura-rpc", None), ("zakura-sim", "dev")],
                    ("lib", "bin")),
            package("zakura-rpc", [("zakura-network", None)]),
            package("zakura-network", [("zakura-rt", None)]),
            package("zakura-rt", []),
            package("zakura-chain", []),
            package("zakura-sim", [("zakura-network", None)]),
        ]}
        self.assertEqual(asm_diff.downstream_packages(metadata, {"zakura-rt"}),
                         ["zakura", "zakura-network", "zakura-rpc"])
        self.assertEqual(asm_diff.downstream_packages(metadata, {"zakura-network", "zakura-rt"}),
                         ["zakura", "zakura-rpc"])


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


# A fake `cargo`: `metadata` prints an empty workspace, and any build
# records whether Git can find the repository from a crate directory, the
# way `zakurad`'s build script runs, then fails with status 7.
FAKE_CARGO = """#!/bin/sh
if [ "$1" = metadata ]; then echo '{"packages": []}'; exit 0; fi
(cd crates/x && git rev-parse HEAD) >> "$FAKE_CARGO_LOG" 2>&1
echo "git status $?" >> "$FAKE_CARGO_LOG"
exit 7
"""


class CommandTests(unittest.TestCase):
    def run_fake(self, tmp):
        """Runs the tool on a one-commit repository with the fake cargo, returning status, output and log."""
        repo = Path(tmp) / "repo"
        (repo / "crates" / "x").mkdir(parents=True)
        git = ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@example.com"]
        subprocess.run(git + ["init", "-q"], check=True)
        (repo / "crates" / "x" / "README").write_text("x\n")
        subprocess.run(git + ["add", "."], check=True)
        subprocess.run(git + ["commit", "-q", "-m", "x"], check=True)
        cargo = Path(tmp) / "cargo"
        cargo.write_text(FAKE_CARGO)
        os.chmod(cargo, 0o755)
        log = Path(tmp) / "cargo.log"
        log.write_text("")
        out, err = io.StringIO(), io.StringIO()
        os.environ["FAKE_CARGO_LOG"] = str(log)
        try:
            with redirect_stdout(out), redirect_stderr(err):
                status = asm_diff.main([
                    "--repo", str(repo), "--base", "HEAD", "--head", "HEAD", "--cargo", str(cargo),
                    "--no-binary", "--work-dir", str(Path(tmp) / "work"),
                ])
        finally:
            del os.environ["FAKE_CARGO_LOG"]
        return status, out.getvalue() + err.getvalue(), log.read_text()

    def test_a_failed_command_exits_nonzero(self):
        """A cargo build that fails makes the run exit 3, and the worktrees are removed."""
        with tempfile.TemporaryDirectory() as tmp:
            status, output, _ = self.run_fake(tmp)
            self.assertEqual(status, 3, output)
            self.assertIn("exit status 7", output)
            self.assertFalse((Path(tmp) / "work" / "base").exists())

    def test_builds_see_no_git_metadata(self):
        """A build can't find the revision's Git repository, so `zakurad`'s build metadata is the same on both sides."""
        with tempfile.TemporaryDirectory() as tmp:
            _, output, log = self.run_fake(tmp)
            self.assertIn("git status 128", log, output)
            self.assertIn("not a git repository", log)


if __name__ == "__main__":
    unittest.main()
