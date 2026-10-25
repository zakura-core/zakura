"""Validate documentation coverage without compiling or contacting GitHub."""

import json
from pathlib import Path
import unittest
from unittest.mock import patch

import check_pr_docs as docs


class DocsSelectionTests(unittest.TestCase):
    """Exercise changed inputs, dependency propagation, and the Cargo boundary."""

    def setUp(self):
        """Build a workspace containing an unpublished dependency and a dev-only consumer."""
        self.root = Path(__file__).resolve().parents[3]
        self.packages = []
        for name, dependencies in [("private", []), ("consumer", [("private", None)]),
                                   ("transitive", [("consumer", "build")]),
                                   ("dev-only", [("private", "dev")])]:
            self.packages.append({
                "id": name, "name": name, "publish": [],
                "manifest_path": str(self.root / "crates" / name / "Cargo.toml"),
                "dependencies": [{"path": str(self.root / "crates" / dependency), "kind": kind}
                                 for dependency, kind in dependencies],
            })
        self.metadata = {"workspace_root": str(self.root), "packages": self.packages,
                         "workspace_members": [package["id"] for package in self.packages]}

    def test_unpublished_changes_include_transitive_consumers(self):
        """Internal crates must receive coverage even though semver checks skip them."""
        self.assertEqual(docs.affected_packages(self.metadata, ["crates/private/src/lib.rs"]),
                         ["consumer", "private", "transitive"])

    def test_owned_assets_and_docs_are_inputs(self):
        """Included Markdown and build inputs can change generated documentation."""
        for path in ["README.md", "schema.proto", "Cargo.toml", "build.rs"]:
            self.assertEqual(docs.affected_packages(self.metadata, [f"crates/consumer/{path}"]),
                             ["consumer", "transitive"])

    def test_global_and_deleted_inputs_check_every_crate(self):
        """Shared configuration and unowned Rust changes cannot silently lose coverage."""
        for path in [*docs.GLOBAL_INPUTS, "crates/deleted/src/lib.rs", "shared/input.rs",
                     ".github/actions/setup-zakura-build/action.yml"]:
            self.assertEqual(docs.affected_packages(self.metadata, [path]),
                             ["consumer", "dev-only", "private", "transitive"])

    def test_unrelated_changes_do_not_run_rustdoc(self):
        """Operator scripts and prose outside packages do not document crates."""
        self.assertEqual(docs.affected_packages(self.metadata, ["deploy/README.md"]), [])

    def test_one_invocation_covers_all_selected_crates_and_propagates_failure(self):
        """Keep flags, shared target directory, and failure status at the real process boundary."""
        with patch.object(docs.subprocess, "check_output", side_effect=[
            json.dumps(self.metadata), b"crates/private/src/lib.rs\0",
        ]), patch.object(docs.subprocess, "run") as run:
            run.return_value.returncode = 101
            self.assertEqual(docs.main(["--base", "base", "--head", "head"]), 101)
        run.assert_called_once()
        self.assertEqual(run.call_args.args[0], [
            "cargo", "doc", "--locked", "--no-deps", "--all-features", "--document-private-items",
            "--target-dir", str(self.root / "target/internal"),
            "--package", "consumer", "--package", "private", "--package", "transitive",
        ])


if __name__ == "__main__":
    unittest.main()
