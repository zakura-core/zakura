#!/usr/bin/env python3
"""List publishable crates needing semver checks for a Git diff."""

import argparse
import json
from pathlib import Path
import subprocess
import sys


def is_publishable(package):
    publish = package.get("publish")
    return publish is None or bool(publish)


def package_graph(metadata):
    workspace_ids = set(metadata["workspace_members"])
    packages = {
        package["id"]: package
        for package in metadata["packages"]
        if package["id"] in workspace_ids
    }
    roots = {
        package_id: Path(package["manifest_path"]).parent.resolve()
        for package_id, package in packages.items()
    }
    owners = {root: package_id for package_id, root in roots.items()}
    reverse_dependencies = {package_id: set() for package_id in packages}

    for dependent_id, package in packages.items():
        for dependency in package["dependencies"]:
            dependency_path = dependency.get("path")
            if dependency.get("kind") == "dev" or dependency_path is None:
                continue

            dependency_id = owners.get(Path(dependency_path).resolve())
            if dependency_id is not None:
                reverse_dependencies[dependency_id].add(dependent_id)

    return packages, roots, reverse_dependencies


def directly_changed_packages(metadata, changed_files):
    packages, roots, _ = package_graph(metadata)
    workspace_root = Path(metadata["workspace_root"]).resolve()
    changed_paths = [Path(path) for path in changed_files]

    if Path("Cargo.toml") in changed_paths:
        return set(packages)

    roots_by_depth = sorted(
        roots.items(),
        key=lambda item: len(item[1].parts),
        reverse=True,
    )
    changed = set()

    for changed_path in changed_paths:
        absolute_path = (workspace_root / changed_path).resolve()

        for package_id, package_root in roots_by_depth:
            try:
                relative_path = absolute_path.relative_to(package_root)
            except ValueError:
                continue

            if (
                relative_path.name == "Cargo.toml"
                or relative_path == Path("build.rs")
                or relative_path.suffix == ".rs"
            ):
                changed.add(package_id)
            break

    return changed


def affected_publishable_packages(metadata, changed_files=None, check_all=False):
    packages, _, reverse_dependencies = package_graph(metadata)
    if check_all:
        directly_changed = set(packages)
    else:
        directly_changed = directly_changed_packages(metadata, changed_files or [])

    seen = set(directly_changed)
    pending = set(directly_changed)
    while pending:
        next_pending = {
            dependent_id
            for package_id in pending
            for dependent_id in reverse_dependencies[package_id]
            if dependent_id not in seen
        }
        seen.update(next_pending)
        pending = next_pending

    dependency_counts = {package_id: 0 for package_id in seen}
    for dependency_id in seen:
        for dependent_id in reverse_dependencies[dependency_id]:
            if dependent_id in seen:
                dependency_counts[dependent_id] += 1

    ordered = []
    ready = {
        package_id
        for package_id, count in dependency_counts.items()
        if count == 0
    }
    while ready:
        package_id = min(ready, key=lambda item: packages[item]["name"])
        ready.remove(package_id)
        ordered.append(package_id)

        for dependent_id in reverse_dependencies[package_id]:
            if dependent_id not in dependency_counts:
                continue
            dependency_counts[dependent_id] -= 1
            if dependency_counts[dependent_id] == 0:
                ready.add(dependent_id)

    if len(ordered) != len(seen):
        remaining = seen.difference(ordered)
        ordered.extend(sorted(remaining, key=lambda item: packages[item]["name"]))

    return [
        packages[package_id]["name"]
        for package_id in ordered
        if is_publishable(packages[package_id])
    ]


def command_output(command, cwd):
    return subprocess.run(
        command,
        cwd=cwd,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    ).stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument(
        "--all",
        action="store_true",
        help="list every publishable workspace crate",
    )
    selection.add_argument("--base", help="base Git revision")
    parser.add_argument("--head", default="HEAD", help="head Git revision")
    args = parser.parse_args()

    if args.head != "HEAD" and args.base is None:
        parser.error("--head requires --base")

    repo_root = Path(__file__).resolve().parents[3]
    metadata = json.loads(
        command_output(
            [
                "cargo",
                "metadata",
                "--format-version",
                "1",
                "--no-deps",
                "--locked",
            ],
            repo_root,
        )
    )

    changed_files = None
    if not args.all:
        changed_files = command_output(
            [
                "git",
                "diff",
                "--name-only",
                "--diff-filter=ACMRDTUXB",
                args.base,
                args.head,
                "--",
            ],
            repo_root,
        ).splitlines()

    packages = affected_publishable_packages(
        metadata,
        changed_files=changed_files,
        check_all=args.all,
    )
    sys.stdout.write("".join(f"{package}\n" for package in packages))


if __name__ == "__main__":
    main()
