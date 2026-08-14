#!/usr/bin/env python3
"""Tests for affected_semver_packages.py."""

import tempfile
import unittest
from pathlib import Path

import affected_semver_packages


class AffectedSemverPackagesTest(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name).resolve()

    def tearDown(self):
        self.temporary_directory.cleanup()

    def package(self, name, *, publish=None, dependencies=None):
        package_root = self.root / name
        package = {
            "id": name,
            "name": name,
            "manifest_path": str(package_root / "Cargo.toml"),
            "dependencies": dependencies or [],
        }
        if publish is not None:
            package["publish"] = publish
        return package

    def dependency(self, name, *, kind=None):
        return {
            "name": name,
            "kind": kind,
            "path": str(self.root / name),
        }

    def metadata(self, packages):
        return {
            "workspace_root": str(self.root),
            "workspace_members": [package["id"] for package in packages],
            "packages": packages,
        }

    def test_includes_publishable_reverse_dependencies(self):
        packages = [
            self.package("base"),
            self.package(
                "dependent",
                dependencies=[self.dependency("base")],
            ),
            self.package(
                "transitive",
                dependencies=[self.dependency("dependent", kind="build")],
            ),
            self.package(
                "dev-only",
                dependencies=[self.dependency("base", kind="dev")],
            ),
            self.package(
                "private-middle",
                publish=[],
                dependencies=[self.dependency("base")],
            ),
            self.package(
                "public-after-private",
                dependencies=[self.dependency("private-middle")],
            ),
        ]

        affected = affected_semver_packages.affected_publishable_packages(
            self.metadata(packages),
            changed_files=["base/src/lib.rs"],
        )

        self.assertEqual(
            affected,
            ["base", "dependent", "public-after-private", "transitive"],
        )

    def test_root_manifest_selects_every_publishable_package(self):
        packages = [
            self.package("z-last"),
            self.package("a-first"),
            self.package("private", publish=[]),
        ]

        affected = affected_semver_packages.affected_publishable_packages(
            self.metadata(packages),
            changed_files=["Cargo.toml"],
        )

        self.assertEqual(affected, ["a-first", "z-last"])

    def test_ignores_lockfiles_and_non_rust_package_files(self):
        packages = [self.package("base")]

        affected = affected_semver_packages.affected_publishable_packages(
            self.metadata(packages),
            changed_files=["Cargo.lock", "base/README.md"],
        )

        self.assertEqual(affected, [])

    def test_package_manifest_selects_that_package(self):
        registry_dependency = {
            "name": "registry-package",
            "kind": None,
        }
        packages = [
            self.package("base", dependencies=[registry_dependency]),
            self.package("other"),
        ]

        affected = affected_semver_packages.affected_publishable_packages(
            self.metadata(packages),
            changed_files=["base/Cargo.toml"],
        )

        self.assertEqual(affected, ["base"])


if __name__ == "__main__":
    unittest.main()
