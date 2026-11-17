#!/usr/bin/env python3
"""Inventory retained mutation bindings without applying patches or running builds."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tomllib

WITNESSES = "qa/mutation-readiness/witnesses.json"
WORK_LIST = "qa/native-mutations/work-list.toml"

# Crates that hold the simulator, its storage and scheduler, and the tools
# that run it. A loop patch changes production code only, so none of these.
SIMULATOR_CRATES = {"zakura-sim", "zakura-scheduler", "zakura-storage", "xtask"}

# The mutation plan's fault classes.
FAULT_CLASSES = {
    "stale-result", "resource-lifetime", "ownership", "wakeup",
    "boundary-accounting", "request-authorization", "queue-refusal",
    "fault-isolation",
}

# The README line that names an ID a loop patch must fail, possibly
# wrapped: `Required failure: the run ends at `<id>`` or, for a patch that
# runs against several tables or instances, `Required failure for `<table>`:
# the run ends at `<id>``.
REQUIRED_FAILURE = re.compile(
    r"Required failure(?: for `[^`]+`)?: the run ends at `([^`]+)`"
)


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def typed_binding(root, patch, case):
    """Check a typed case's committed scenario and Plan bytes. No fuzz target runs one."""
    typed = case["typed"]
    errors = []
    files = {}
    for name in ("scenario", "plan"):
        path = root / typed[name]
        if path.is_file():
            files[name] = sha256(path)
        else:
            errors.append(f"missing typed {name}: {typed[name]}")
    for check in case.get("checks", []):
        if not (root / check).exists():
            errors.append(f"missing dependency path: {check}")
    return {
        "property": patch.parent.relative_to(root / "qa/planted").as_posix(),
        "input": None,
        "scenario": typed["scenario"],
        "scenario_sha256": files.get("scenario"),
        "plan": typed["plan"],
        "plan_sha256": files.get("plan"),
        "harness": typed["harness"],
        "fuzz_targets": [],
        "discovery_target": "not applicable: a typed case has no fuzz input",
        "errors": errors,
    }


def planted_binding(root, patch):
    """Check the case, committed input, witness declaration, and fuzz target mapping."""
    case = tomllib.loads(patch.with_suffix(".toml").read_text())
    if ("input" in case) == ("typed" in case):
        raise ValueError("a case names either `input` or `[typed]`")
    if "typed" in case:
        return typed_binding(root, patch, case)
    source = root / case["input"]
    sidecar = tomllib.loads(source.with_suffix(".toml").read_text())
    property_id = patch.parent.relative_to(root / "qa/planted").as_posix()
    errors = []
    if sha256(source) != sidecar["sha256"]:
        errors.append("input hash differs from sidecar")
    if not any(
        witness["property"] == property_id and witness["expect"] == "holds"
        for witness in sidecar.get("witness", [])
    ):
        errors.append("sidecar has no holds witness for the intended property")
    manifest = tomllib.loads((root / "qa/fuzz/sim/Cargo.toml").read_text())
    targets = manifest["package"]["metadata"]["zakura-sim"]["targets"]
    names = sorted(name for name, harness in targets.items() if harness == sidecar["harness"])
    binaries = {item["name"]: item for item in manifest["bin"]}
    for name in names:
        binary = binaries.get(name)
        if binary is None or not (root / "qa/fuzz/sim" / binary["path"]).is_file():
            errors.append(f"target {name} has no binary source")
    for check in case.get("checks", []):
        if not (root / check).exists():
            errors.append(f"missing dependency path: {check}")
    return {
        "property": property_id,
        "input": case["input"],
        "input_sha256": sha256(source),
        "harness": sidecar["harness"],
        "generator": sidecar["generator"],
        "fuzz_targets": names,
        "discovery_target": "mapped, not executed" if names else "missing",
        "errors": errors,
    }


def selector_errors(root, source, test, test_target=None):
    """Check that `--exact test` can name a function in `source`.

    A library test's name starts with the module path of its file, and an
    integration test's file is `tests/<test_target>.rs`. Cargo selects zero
    tests when either part is wrong, so a function-name match alone is not
    enough. This does not prove that Cargo compiles or selects the test.
    """
    path = root / source
    crate = next(parent for parent in path.parents if (parent / "Cargo.toml").is_file())
    parts = path.relative_to(crate).with_suffix("").parts
    errors = []
    if test_target is not None:
        if parts != ("tests", test_target):
            errors.append(f"{source} is not the integration test file for {test_target}")
        module = []
    elif parts[0] != "src":
        errors.append(f"{source} is outside the library source tree")
        module = []
    else:
        module = list(parts[1:])
        if module[-1] in ("lib", "mod"):
            module.pop()
    *test_module, symbol = test.split("::")
    if test_module[: len(module)] != module:
        errors.append(f"selector {test} lies outside module {'::'.join(module)}")
    if not re.search(r"\bfn\s+" + re.escape(symbol) + r"\s*\(", path.read_text()):
        errors.append(f"test function not found in {source}")
    return crate, errors


def native_binding(root, case):
    """Check a native case's package, test selector and intended assertion."""
    if "gap" in case:
        return {"errors": [case["gap"]]}
    test = case["test"]
    crate, errors = selector_errors(root, case["source"], test, case.get("test_target"))
    manifest = tomllib.loads((crate / "Cargo.toml").read_text())
    if manifest["package"]["name"] != case["package"]:
        errors.append("declared package differs from the source crate")
    if not case["required_failure"].strip():
        errors.append("missing intended assertion")
    target = ["--test", case["test_target"]] if "test_target" in case else ["--lib"]
    return {
        "package": case["package"],
        "test": test,
        "source": case["source"],
        "required_failure": case["required_failure"],
        "command": ["cargo", "test", "--locked", "-p", case["package"], *target, "--", test, "--exact"],
        "rustflags": case.get("rustflags", ""),
        "errors": errors,
    }


# The cfg atoms a release build leaves unset: the test, simulator, fuzzing
# and doc builds. A release build also leaves unset each feature whose name
# matches NON_RELEASE_FEATURE, such as `proptest-impl` or `zakura-testkit`.
# Every other atom and feature counts as set.
NON_RELEASE_CFG = {"test", "zakura_sim", "fuzzing", "doc"}
NON_RELEASE_FEATURE = re.compile(r"test|fuzz|bench|proptest|mock|sim")

CFG_TOKEN = re.compile(r'\s*(?:([A-Za-z_][A-Za-z0-9_]*)|("[^"]*")|(\S))')

# An out-of-line module declaration, `mod name;`, at the start of a line.
MOD_DECL = re.compile(r"^[ \t]*(?:pub(?:\([^)]*\))?\s+)?mod\s+(?:r#)?([A-Za-z_][A-Za-z0-9_]*)\s*;", re.M)
CFG_ATTRIBUTE = re.compile(r"^\s*#\[\s*cfg\s*\((.*)\)\s*\]\s*$")
PATH_ATTRIBUTE = re.compile(r'^\s*#\[\s*path\s*=\s*"([^"]*)"\s*\]\s*$')


def release_cfg(predicate):
    """Whether the `cfg` predicate `predicate` holds in a release build."""
    tokens = [next(group for group in match.groups() if group) for match in CFG_TOKEN.finditer(predicate)]

    def parse(at):
        name = tokens[at]
        if tokens[at + 1:at + 2] == ["("]:
            values, at = [], at + 2
            while tokens[at] != ")":
                value, at = parse(at)
                values.append(value)
                if tokens[at] == ",":
                    at += 1
            combine = {"all": all, "any": any, "not": lambda values: not values[0]}[name]
            return combine(values), at + 1
        if tokens[at + 1:at + 2] == ["="]:
            value = tokens[at + 2].strip('"')
            return not (name == "feature" and NON_RELEASE_FEATURE.search(value)), at + 3
        return name not in NON_RELEASE_CFG, at + 1

    holds, at = parse(0)
    if at != len(tokens):
        raise ValueError(f"unparsed cfg predicate: {predicate}")
    return holds


def declared_modules(source):
    """The files `source` declares as modules that a release build compiles:
    each `mod name;` whose `cfg` attributes hold in a release build
    ([`release_cfg`]), at its `path` attribute's file or its default one."""
    text = source.read_text()
    lines = text.splitlines()
    owns_directory = source.name in ("lib.rs", "main.rs", "mod.rs")
    directory = source.parent if owns_directory else source.parent / source.stem
    files = []
    for match in MOD_DECL.finditer(text):
        line = text.count("\n", 0, match.start())
        holds, path = True, None
        # The declaration's attributes and doc comments sit directly above it.
        while line > 0 and lines[line - 1].lstrip().startswith(("#[", "//")):
            line -= 1
            if cfg := CFG_ATTRIBUTE.match(lines[line]):
                holds = holds and release_cfg(cfg.group(1))
            elif attribute := PATH_ATTRIBUTE.match(lines[line]):
                path = attribute.group(1)
        if not holds:
            continue
        name = match.group(1)
        if path is not None:
            files.append(source.parent / path)
        else:
            files.extend(
                candidate for candidate in (directory / f"{name}.rs", directory / name / "mod.rs")
                if candidate.is_file()
            )
    return files


def release_files(crate):
    """The source files of `crate` a release build compiles: those its
    library and binary roots reach through `mod` declarations whose `cfg`
    holds in a release build. Simulator-only modules such as
    `transport/sim_reader.rs`, and test-only ones, are not among them. A
    module declared inside an inline module or a macro is not either."""
    roots = [crate / "src/lib.rs", crate / "src/main.rs", *(crate / "src/bin").glob("*.rs")]
    pending = [root for root in roots if root.is_file()]
    reached = set()
    while pending:
        source = pending.pop().resolve()
        if source in reached or not source.is_file():
            continue
        reached.add(source)
        pending.extend(declared_modules(source))
    return reached


def production_errors(root, paths):
    """Name each path a loop patch may not change: anything but a Rust
    source file a release build of a production crate compiles
    ([`release_files`]), so not a simulator crate, a test, or a module only
    the simulator or tests compile."""
    errors = []
    crates = {}
    for path in paths:
        parts = Path(path).parts
        if len(parts) < 3 or parts[0] != "crates" or parts[1] in SIMULATOR_CRATES:
            errors.append(f"changes a non-production file: {path}")
            continue
        crate = root / "crates" / parts[1]
        if crate not in crates:
            crates[crate] = release_files(crate)
        if (root / path).resolve() not in crates[crate]:
            errors.append(f"changes a non-production file: {path}")
    return errors


def work_list_ids(work_list):
    """Map each work-list entry ID to the property IDs it covers and the
    table prefixes it covers."""
    entries = {}
    for entry in work_list["entry"]:
        ids = {instance["property"] for instance in entry.get("instance", [])}
        tables = set()
        if "table" in entry:
            tables.add(entry["table"])
        tables.update(table["table"] for table in entry.get("tables", []))
        entries[entry["id"]] = (ids, tables)
    return entries


def witness_binding(root, witness):
    """Check one loop witness: a planted case's witness, a committed input,
    or one `zakura-sim` integration test. Returns its errors."""
    forms = [key for key in ("planted", "input", "test") if key in witness]
    if len(forms) != 1:
        return [f"a witness names exactly one of planted, input or test, not {forms}"]
    if "planted" in witness:
        path = root / witness["planted"]
        if not path.is_file():
            return [f"missing planted case: {witness['planted']}"]
        case = tomllib.loads(path.read_text())
        if ("input" in case) == ("typed" in case):
            return [f"{witness['planted']} names neither or both of input and typed"]
        return []
    if "input" in witness:
        path = root / witness["input"]
        if not path.is_file() or not path.with_suffix(".toml").is_file():
            return [f"missing input or sidecar: {witness['input']}"]
        return []
    if "test_target" not in witness:
        return ["a test witness names its test_target"]
    source = f"crates/zakura-sim/tests/{witness['test_target']}.rs"
    if not (root / source).is_file():
        return [f"missing test file: {source}"]
    _, errors = selector_errors(root, source, witness["test"], witness["test_target"])
    return errors


def loop_binding(root, patch, cases, work_list):
    """Check a loop patch: it changes production files only, its README
    names each ID it must fail, and each `cases.json` entry names one of
    those IDs, a work-list entry covering it, and one witness."""
    errors = []
    numstat = subprocess.run(
        ["git", "apply", "--numstat", str(patch)], cwd=root,
        capture_output=True, text=True, check=False,
    )
    changed = [line.split("\t", 2)[2] for line in numstat.stdout.splitlines() if line.count("\t") >= 2]
    if not changed:
        errors.append("the patch changes no file")
    errors.extend(production_errors(root, changed))
    readme = patch.parent / "README.md"
    required = []
    if readme.is_file():
        required = REQUIRED_FAILURE.findall(" ".join(readme.read_text().split()))
    else:
        errors.append("missing README.md")
    if not required:
        errors.append("the README names no required failure")
    if len(set(required)) != len(required):
        errors.append("the README names an ID twice")
    entries = work_list_ids(work_list)
    named = []
    for case in cases:
        property_id = case["property"]
        named.append(property_id)
        entry = entries.get(case.get("entry"))
        if entry is None:
            errors.append(f"{property_id}: unknown work-list entry {case.get('entry')!r}")
        else:
            ids, tables = entry
            covered = property_id in ids or any(
                property_id.startswith(table + "/") and case.get("table", table) == table
                for table in tables
            )
            if not covered:
                errors.append(f"{property_id}: entry {case['entry']} doesn't cover it")
        if "fault_class" in case and case["fault_class"] not in FAULT_CLASSES:
            errors.append(f"{property_id}: unknown fault class {case['fault_class']}")
        errors.extend(f"{property_id}: {error}" for error in witness_binding(root, case.get("witness", {})))
    if sorted(named) != sorted(required):
        errors.append(f"cases.json names {sorted(named)}, the README {sorted(required)}")
    return {
        "properties": named,
        "changed": changed,
        "witnesses": [case.get("witness") for case in cases],
        "errors": errors,
    }


def witness_errors(root):
    """Check every native selector in the witness inventory, if one exists."""
    path = root / WITNESSES
    if not path.is_file():
        return []
    errors = []
    for row in json.loads(path.read_text())["rows"]:
        for test in row.get("tests", []):
            try:
                _, found = selector_errors(root, test["source"], test["selector"])
            except (OSError, KeyError, ValueError, StopIteration) as error:
                found = [f"binding metadata: {error!r}"]
            errors.extend(f"{WITNESSES} {row['id']}: {error}" for error in found)
    return errors


def inventory(root, loop_only=False, cases_dir=None):
    """Return one row per retained patch and every static validation failure.
    With `loop_only`, only the mutation loop's patches: those whose
    `cases.json` entries name a property. With `cases_dir`, every patch of
    that directory, such as a generator's output before triage, against the
    directory's own `cases.json`, whose patch paths are relative to it; each
    patch must have a loop mapping."""
    native_dir = Path(cases_dir) if cases_dir is not None else root / "qa/native-mutations"
    native = json.loads((native_dir / "cases.json").read_text())
    work_list = (
        tomllib.loads((root / WORK_LIST).read_text())
        if (root / WORK_LIST).is_file() else {"entry": []}
    )
    mappings = {}
    loops = {}
    errors = []
    def patch_path(name):
        full = native_dir / name
        try:
            return full.relative_to(root).as_posix()
        except ValueError:
            return full.as_posix()

    for case in native:
        path = patch_path(case["patch"])
        if "property" in case:
            # A loop patch has one entry for each ID it must fail.
            if any(other["property"] == case["property"] for other in loops.get(path, [])):
                errors.append(f"duplicate loop mapping: {path} {case['property']}")
            loops.setdefault(path, []).append(case)
            continue
        if path in mappings:
            errors.append(f"duplicate native mapping: {path}")
        mappings[path] = case
    patches = sorted([
        *(root / "qa/planted").rglob("*.patch"),
        *(root / "qa/native-mutations").rglob("*.patch"),
    ])
    if cases_dir is not None:
        # Every generated patch is a loop case: one with no loop mapping, or
        # whose entry names no property, fails rather than going unchecked.
        patches = sorted(Path(cases_dir).resolve().rglob("*.patch"))
        errors.extend(
            f"patch has no loop mapping: {patch_path(patch)}"
            for patch in patches if patch_path(patch) not in loops
        )
        errors.extend(f"mapping names no property: {path}" for path in sorted(mappings))
    if loop_only or cases_dir is not None:
        patches = [patch for patch in patches if patch_path(patch) in loops]
        mappings = {}
    rows = []
    for patch in patches:
        path = patch_path(patch)
        row = {"patch": path, "patch_sha256": sha256(patch)}
        checked = subprocess.run(
            ["git", "apply", "--check", str(patch)], cwd=root,
            capture_output=True, text=True, check=False,
        )
        row["apply"] = "passed" if checked.returncode == 0 else "failed"
        row["apply_error"] = checked.stderr.strip() or None
        try:
            if path.startswith("qa/planted/"):
                row.update(planted_binding(root, patch))
            elif path in loops:
                row.update(loop_binding(root, patch, loops.pop(path), work_list))
            else:
                row.update(native_binding(root, mappings.pop(path)))
        except (OSError, KeyError, ValueError, StopIteration) as error:
            row["errors"] = [f"binding metadata: {error!r}"]
        rows.append(row)
    errors.extend(f"mapping has no patch: {path}" for path in sorted([*mappings, *loops]))
    if not loop_only and cases_dir is None:
        errors.extend(witness_errors(root))
    return {
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
        "worktree_status": subprocess.check_output(
            ["git", "status", "--porcelain", "--untracked-files=all"], cwd=root, text=True,
        ).splitlines(),
        "scope": "Static inventory only. No row has been compiled or run. Apply success and selector checks do not prove compilation, executable selection, trigger reachability, or detection.",
        "errors": errors,
        "cases": rows,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--loop", action="store_true",
        help="check only the mutation loop's patches (qa/native-mutations/PROMPT.md)",
    )
    parser.add_argument(
        "--cases", type=Path,
        help="check the loop patches of this directory's cases.json instead",
    )
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[1]
    report = inventory(root, loop_only=args.loop, cases_dir=args.cases and args.cases.resolve())
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    failures = len(report["errors"]) + sum(
        bool(row["errors"]) or row["apply"] != "passed" for row in report["cases"]
    )
    print(f"{len(report['cases'])} patches; {failures} metadata or application failures")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
