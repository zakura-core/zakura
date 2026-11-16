"""Protect the single Python dependency graph and the required audit gate."""

import fnmatch
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
import tomllib
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import yaml


ROOT = Path(__file__).resolve().parents[3]
SETUP = "./.github/actions/setup-python"


def read_yaml(path):
    """Read workflow strings without YAML 1.1 converting `on` into a boolean."""
    return yaml.load(path.read_text(), Loader=yaml.BaseLoader)


class DependencyPolicyTests(unittest.TestCase):
    def test_only_root_manifests_are_allowed(self):
        paths = subprocess.check_output(
            ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
            cwd=ROOT,
        ).decode().split("\0")
        manifests = {
            "pyproject.toml", "uv.lock", "uv.toml", "Pipfile", "Pipfile.lock",
            "poetry.lock", "pdm.lock", "pylock.toml", "setup.py", "setup.cfg",
        }
        for relative in paths:
            path = Path(relative)
            if not (ROOT / path).is_file():
                continue
            is_manifest = path.name in manifests or any(
                fnmatch.fnmatch(path.name, pattern)
                for pattern in ("*requirements*.txt", "*requirements*.in", "*constraints*.txt", "*constraints*.in", "*.py.lock")
            )
            if is_manifest:
                self.assertIn(relative, {"pyproject.toml", "uv.lock"}, relative)
            if path.suffix == ".py":
                self.assertFalse(any(line.startswith("# /// script") for line in (ROOT / path).read_text().splitlines()), relative)

    def test_all_dependencies_have_reviewable_sources_and_hashes(self):
        manifest = tomllib.loads((ROOT / "pyproject.toml").read_text())
        config = manifest["tool"]["uv"]
        self.assertEqual(config["default-groups"], [])
        self.assertFalse(config["package"])
        self.assertNotIn("sources", config)
        self.assertNotIn("index", config)
        self.assertNotIn("audit", config, "Audit exclusions require an explicit policy change")
        for package in tomllib.loads((ROOT / "uv.lock").read_text())["package"]:
            if package["name"] == manifest["project"]["name"]:
                self.assertEqual(package["source"], {"virtual": "."})
                continue
            self.assertEqual(package["source"], {"registry": "https://pypi.org/simple"})
            self.assertTrue(package.get("wheels"), package["name"])
            for artifact in package["wheels"]:
                self.assertRegex(artifact["hash"], r"^sha256:[0-9a-f]{64}$")

    def test_installation_is_shared_and_locked(self):
        expected = {"lint.yml": "test", "docs-check.yml": "docs", "update-release-state.yml": "release"}
        for filename, group in expected.items():
            workflow = read_yaml(ROOT / ".github/workflows" / filename)
            steps = [step for job in workflow["jobs"].values() for step in job.get("steps", [])]
            self.assertTrue(any(step.get("uses") == SETUP and step.get("with", {}).get("group") == group for step in steps))
        for path in (ROOT / ".github").rglob("*.yml"):
            self.assertNotRegex(path.read_text(), r"\bpip(?:3)?\s+(?:[^\n]*\s)?install\b", str(path))
        setup = read_yaml(ROOT / ".github/actions/setup-python/action.yml")
        commands = "\n".join(step.get("run", "") for step in setup["runs"]["steps"])
        self.assertIn('uv sync --locked --no-build --only-group "$DEPENDENCY_GROUP"', commands)

    def test_security_gate_cannot_be_skipped(self):
        workflow = read_yaml(ROOT / ".github/workflows/tests-unit.yml")
        for restriction in ("paths", "paths-ignore", "branches", "branches-ignore"):
            self.assertNotIn(restriction, workflow["on"]["pull_request"])
        self.assertIn("merge_group", workflow["on"])
        self.assertIn("schedule", workflow["on"])
        job = workflow["jobs"]["python-security"]
        self.assertNotIn("if", job)
        self.assertNotIn("continue-on-error", job)
        self.assertEqual(job["permissions"], {"contents": "read"})
        self.assertEqual(job["timeout-minutes"], "10")
        self.assertNotIn("environment", job)
        self.assertNotIn("secrets.", json.dumps(job))
        summary = workflow["jobs"]["test-success"]
        self.assertIn("python-security", summary["needs"])
        for step in summary["steps"]:
            self.assertNotIn("python-security", step.get("with", {}).get("allowed-skips", ""))
        audit = next(step for step in job["steps"] if "check-python-dependencies.sh" in step.get("run", ""))
        self.assertEqual(audit["shell"], "bash", "Explicit bash enables pipefail for the report tee")
        self.assertNotIn("continue-on-error", audit)
        promotion = read_yaml(ROOT / ".github/workflows/update-release-state.yml")
        self.assertIn("python-security", promotion["jobs"]["promote"]["needs"])
        self.assertNotIn("environment", promotion["jobs"]["python-security"])

    def test_python_and_manifest_changes_trigger_checks(self):
        samples = ("tool.py", "scripts/tool.py", ".github/scripts/tool.py", "deploy/new/tool.py", "pyproject.toml", "uv.lock")
        for filename, events in (("tests-unit.yml", ("push",)), ("lint.yml", ("push", "pull_request"))):
            workflow = read_yaml(ROOT / ".github/workflows" / filename)
            for event in events:
                patterns = workflow["on"][event]["paths"]
                for path in samples:
                    self.assertTrue(any(fnmatch.fnmatch(path, pattern) or (pattern.startswith("**/") and fnmatch.fnmatch(path, pattern[3:])) for pattern in patterns), (filename, event, path))


class AdvisoryServer(ThreadingHTTPServer):
    """A local OSV service with deterministic clean, vulnerable, and outage replies."""

    def __init__(self, vulnerable=None, unavailable=False):
        super().__init__(("127.0.0.1", 0), AdvisoryHandler)
        self.vulnerable = vulnerable
        self.unavailable = unavailable
        self.queried = set()


class AdvisoryHandler(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def respond(self, value):
        payload = json.dumps(value).encode()
        self.send_response(503 if self.server.unavailable else 200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        results = []
        for query in body["queries"]:
            name = query["package"]["name"]
            self.server.queried.add(name)
            results.append({"vulns": [{"id": "TEST-2026-0001", "modified": "2026-01-01T00:00:00Z"}]} if name == self.server.vulnerable else {})
        self.respond({"results": results})

    def do_GET(self):
        self.respond({
            "schema_version": "1.6.0", "id": "TEST-2026-0001",
            "modified": "2026-01-01T00:00:00Z", "summary": "Test advisory",
            "affected": [{"package": {"ecosystem": "PyPI", "name": self.server.vulnerable}, "ranges": [{"type": "ECOSYSTEM", "events": [{"introduced": "0"}]}]}],
        })


class AuditIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.project = Path(self.temporary.name)
        for name in ("pyproject.toml", "uv.lock"):
            shutil.copyfile(ROOT / name, self.project / name)

    def audit(self, server, platform="x86_64-unknown-linux-gnu", *, frozen=False):
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            return subprocess.run(
                ["uv", "audit", "--frozen" if frozen else "--locked", "--no-build", "--no-cache", "--python-version", "3.12", "--python-platform", platform,
                 "--service-url", f"http://127.0.0.1:{server.server_port}/v1"],
                cwd=self.project, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                env={**os.environ, "UV_HTTP_RETRIES": "0", "UV_HTTP_TIMEOUT": "5"}, timeout=30,
            )
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_clean_graph_checks_uninstalled_groups(self):
        server = AdvisoryServer()
        result = self.audit(server)
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertTrue({"urllib3", "codespell", "pyyaml"} <= server.queried, result.stdout)
        self.assertFalse((self.project / ".venv").exists())

    def test_vulnerable_transitive_dependency_fails_without_installing_it(self):
        result = self.audit(AdvisoryServer(vulnerable="urllib3"))
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("TEST-2026-0001", result.stdout)
        self.assertFalse((self.project / ".venv").exists())

    def test_advisory_service_failure_is_not_success(self):
        server = AdvisoryServer(unavailable=True)
        result = self.audit(server)
        self.assertTrue(server.queried, result.stdout)
        self.assertNotEqual(result.returncode, 0, result.stdout)

    def test_platform_specific_group_is_audited_on_its_target(self):
        manifest = self.project / "pyproject.toml"
        requirement = tomllib.loads(manifest.read_text())["dependency-groups"]["docs"][0]
        manifest.write_text(manifest.read_text().replace(
            json.dumps(requirement), json.dumps(requirement + "; sys_platform == 'darwin'"),
        ))
        # Make an audit fixture without resolving or relying on cached PyPI
        # metadata. Frozen mode checks the supplied graph; separate tests check
        # that the production command rejects a stale lock.
        lock = self.project / "uv.lock"
        lock.write_text(lock.read_text().replace(
            '{ name = "codespell" }',
            '{ name = "codespell", marker = "sys_platform == \'darwin\'" }',
        ).replace(
            '{ name = "codespell", specifier =',
            '{ name = "codespell", marker = "sys_platform == \'darwin\'", specifier =',
        ))
        # uv 0.11.8 conservatively queries all reachable locked packages even
        # when target selectors are provided. Require coverage on both Macs;
        # do not require the audit to omit the package on Linux.
        for platform in ("x86_64-apple-darwin", "aarch64-apple-darwin"):
            with self.subTest(platform=platform):
                server = AdvisoryServer(vulnerable="codespell")
                result = self.audit(server, platform, frozen=True)
                self.assertIn("codespell", server.queried)
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn("TEST-2026-0001", result.stdout)

    def test_stale_lock_fails_the_local_security_command(self):
        manifest = self.project / "pyproject.toml"
        manifest.write_text(manifest.read_text().replace('name = "zakura-python-tools"', 'name = "changed-tools"'))
        scripts = self.project / "scripts"
        scripts.mkdir()
        shutil.copyfile(ROOT / "scripts/check-python-dependencies.sh", scripts / "check-python-dependencies.sh")
        result = subprocess.run(
            ["bash", str(scripts / "check-python-dependencies.sh")], text=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=30,
            env={**os.environ, "UV_OFFLINE": "1"},
        )
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertNotIn("Auditing Python", result.stdout)

    def test_local_command_preserves_failure_and_checks_every_target(self):
        scripts = self.project / "scripts"
        scripts.mkdir()
        shutil.copyfile(ROOT / "scripts/check-python-dependencies.sh", scripts / "check-python-dependencies.sh")
        binary = self.project / "bin"
        binary.mkdir()
        uv = binary / "uv"
        uv.write_text(
            '#!/bin/sh\nprintf "%s\\n" "$*" >> "$CALL_LOG"\n'
            'case "$*" in *x86_64-unknown-linux-gnu*) exit 1 ;; esac\n'
        )
        uv.chmod(0o755)
        log = self.project / "calls.txt"
        result = subprocess.run(
            ["bash", str(scripts / "check-python-dependencies.sh")], text=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=10,
            env={**os.environ, "PATH": str(binary) + os.pathsep + os.environ["PATH"], "CALL_LOG": str(log)},
        )
        self.assertNotEqual(result.returncode, 0, result.stdout)
        calls = log.read_text().splitlines()
        self.assertEqual(len(calls), 5, calls)
        self.assertEqual(calls[0], "lock --check --no-build")
        self.assertEqual({call.split()[-1] for call in calls[1:]}, {
            "x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu",
            "x86_64-apple-darwin", "aarch64-apple-darwin",
        })


if __name__ == "__main__":
    unittest.main()
