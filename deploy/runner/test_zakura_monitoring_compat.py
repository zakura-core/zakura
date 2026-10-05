#!/usr/bin/env python3
"""Tests for the zcashd-compat sync checker (zakura_monitoring.compat).

They run from the repository and from an installed release, so they only use
paths relative to this file and never touch real nodes, Slack or /run.
"""

from __future__ import annotations

import base64
import contextlib
import io
import json
import os
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from zakura_monitoring import compat, suppression  # noqa: E402

CHECKER = HERE / "zakura-compat-check"
SECRET_COOKIE = "__cookie__:s3cr3t-cookie-value"
SECRET_PASSWORD = "s3cr3t-password-value"


class RpcServer:
    """A tiny JSON-RPC server whose per-method replies tests can script."""

    def __init__(self, host: str = "127.0.0.1"):
        self.replies: dict[str, object] = {}
        self.auth_headers: list[str | None] = []
        self.delay = 0.0
        server = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                request = json.loads(self.rfile.read(length))
                server.auth_headers.append(self.headers.get("Authorization"))
                if server.delay:
                    time.sleep(server.delay)
                reply = server.replies.get(request["method"], {"result": 0, "error": None})
                status = 200
                if isinstance(reply, tuple):
                    status, reply = reply
                body = reply if isinstance(reply, bytes) else json.dumps(reply).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                try:
                    self.wfile.write(body)
                except OSError:
                    pass

        class Server(ThreadingHTTPServer):
            daemon_threads = True
            address_family = socket.AF_INET6 if ":" in host else socket.AF_INET

        self.httpd = Server((host, 0), Handler)
        self.port = self.httpd.server_address[1]
        host_part = f"[{host}]" if ":" in host else host
        self.url = f"http://{host_part}:{self.port}"
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


def ipv6_available() -> bool:
    try:
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as probe:
            probe.bind(("::1", 0))
        return True
    except OSError:
        return False


class CheckerCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.zakura = RpcServer()
        self.zcashd = RpcServer()
        self.addCleanup(self.zakura.close)
        self.addCleanup(self.zcashd.close)
        self.addCleanup(self.tmp.cleanup)
        self.zakura_cookie = self.dir / "zakura.cookie"
        self.zcashd_cookie = self.dir / "zcashd.cookie"
        self.zakura_cookie.write_text(SECRET_COOKIE + "\n")
        self.zcashd_cookie.write_text(SECRET_COOKIE + "\n")
        self.healthy(peers=1, zakura=2_000_000, zcashd=2_000_000)

    def healthy(self, peers=1, zakura=100, zcashd=100):
        self.zcashd.replies = {
            "getconnectioncount": {"result": peers, "error": None},
            "getblockcount": {"result": zcashd, "error": None},
        }
        self.zakura.replies = {"getblockcount": {"result": zakura, "error": None}}

    def values(self, **overrides):
        values = compat.resolve_settings({}, {}, {})
        values.update(
            zakura_rpc_url=self.zakura.url,
            zcashd_rpc_url=self.zcashd.url,
            zakura_cookie_file=str(self.zakura_cookie),
            zcashd_cookie_file=str(self.zcashd_cookie),
            deployment_suppression_file=str(self.dir / "marker"),
        )
        values.update({key: str(value) for key, value in overrides.items()})
        return values

    def config(self, **overrides):
        return compat.build_config(self.values(**overrides))

    def cycle(self, processes=(True, True), **overrides):
        answers = dict(zip(("zakurad", "zcashd"), processes))
        query = lambda pattern, _timeout: answers["zakurad" if "zakurad" in pattern else "zcashd"]
        return compat.run_cycle(self.config(**overrides), time.monotonic() + 30, query)


class PredicateParityTests(CheckerCase):
    def test_healthy_cycle_passes_with_numeric_details(self):
        outcome = self.cycle()
        self.assertTrue(outcome.passed)
        self.assertEqual(outcome.predicate, "in_sync")
        self.assertEqual(
            outcome.details,
            {"height_max_drift": 30, "zcashd_connections": 1, "zakura_height": 2_000_000,
             "zcashd_height": 2_000_000, "height_drift": 0},
        )

    def test_process_predicates_present_and_absent(self):
        self.assertEqual(self.cycle(processes=(False, True)).predicate, "zakurad_process")
        self.assertEqual(self.cycle(processes=(True, False)).predicate, "zcashd_process")
        self.assertEqual(self.cycle(processes=(False, False)).predicate, "zakurad_process")
        self.assertTrue(self.cycle(processes=(True, True)).passed)

    def test_default_process_patterns_match_the_canonical_check(self):
        config = self.config()
        self.assertEqual(config.zakurad_process_pattern, "zakurad .*--zcashd-compat")
        self.assertEqual(config.zcashd_process_pattern, "zcashd .*-connect")

    def test_peer_count_must_be_exactly_one(self):
        for peers, passed in ((0, False), (1, True), (2, False)):
            with self.subTest(peers=peers):
                self.healthy(peers=peers)
                outcome = self.cycle()
                self.assertEqual(outcome.passed, passed)
                if not passed:
                    self.assertEqual(outcome.predicate, "peer_pinning")
                    self.assertEqual(outcome.details["zcashd_connections"], peers)

    def test_drift_boundaries_in_both_directions(self):
        for drift, passed in ((9, True), (10, True), (11, False)):
            for direction in (1, -1):
                with self.subTest(drift=drift, direction=direction):
                    self.healthy(zakura=1000 + direction * drift, zcashd=1000)
                    outcome = self.cycle(height_max_drift=10)
                    self.assertEqual(outcome.passed, passed)
                    self.assertEqual(outcome.details["height_drift"], drift)
                    if not passed:
                        self.assertEqual(outcome.predicate, "height_drift")

    def test_default_drift_limit_is_current_main_value(self):
        self.assertEqual(self.config().height_max_drift, 30)
        self.healthy(zakura=1030, zcashd=1000)
        self.assertTrue(self.cycle().passed)
        self.healthy(zakura=1000, zcashd=1031)
        self.assertEqual(self.cycle().predicate, "height_drift")

    def test_rpc_order_matches_canonical_check(self):
        self.zcashd.replies["getconnectioncount"] = {"result": None, "error": {"code": -1}}
        self.assertEqual(self.cycle().predicate, "zcashd_getconnectioncount")
        self.healthy()
        self.zakura.replies["getblockcount"] = {"result": None, "error": {"code": -1}}
        self.assertEqual(self.cycle().predicate, "zakura_getblockcount")
        self.healthy()
        self.zcashd.replies["getblockcount"] = {"result": None, "error": {"code": -1}}
        self.assertEqual(self.cycle().predicate, "zcashd_getblockcount")


class RpcResultTests(CheckerCase):
    def assert_kind(self, reply, kind, method="getblockcount", server=None):
        (server or self.zakura).replies[method] = reply
        outcome = self.cycle()
        self.assertFalse(outcome.passed)
        self.assertEqual(outcome.error_kind, kind)
        return outcome

    def test_rpc_error_object(self):
        self.assert_kind({"result": 5, "error": {"code": -28, "message": "warming up"}}, "rpc_error")

    def test_http_error_status(self):
        self.assert_kind((500, {"result": None, "error": {"code": -1}}), "http_status")

    def test_malformed_json(self):
        self.assert_kind(b"{not json", "malformed_json")
        self.assert_kind(b"[1, 2]", "malformed_json")

    def test_missing_result(self):
        self.assert_kind({"error": None}, "missing_result")

    def test_numeric_results_exclude_bool_float_negative_and_strings(self):
        for value in (True, False, 5.0, -1, "100", None):
            with self.subTest(value=value):
                self.assert_kind({"result": value, "error": None}, "invalid_result")
        for value in (True, 1.0):
            with self.subTest(peers=value):
                self.healthy()
                self.assert_kind(
                    {"result": value, "error": None}, "invalid_result",
                    method="getconnectioncount", server=self.zcashd,
                )

    def test_oversized_response(self):
        self.assert_kind(b"[" + b"0," * (compat.MAX_RESPONSE_BYTES // 2 + 10) + b"0]",
                         "oversized_response")

    def test_connection_refused(self):
        with socket.socket() as unused:
            unused.bind(("127.0.0.1", 0))
            port = unused.getsockname()[1]
        outcome = self.cycle(zakura_rpc_url=f"http://127.0.0.1:{port}")
        self.assertEqual(outcome.predicate, "zakura_getblockcount")
        self.assertEqual(outcome.error_kind, "connection")

    def test_per_rpc_timeout(self):
        self.zakura.delay = 3
        started = time.monotonic()
        outcome = self.cycle(rpc_timeout=1)
        self.assertLess(time.monotonic() - started, 2.5)
        self.assertEqual(outcome.error_kind, "timeout")

    def test_cycle_deadline_bounds_every_rpc(self):
        self.zcashd.delay = 3
        started = time.monotonic()
        outcome = compat.run_cycle(
            self.config(rpc_timeout=30), time.monotonic() + 1, lambda *_: True
        )
        self.assertLess(time.monotonic() - started, 2.5)
        self.assertEqual(outcome.error_kind, "timeout")

    def test_expired_deadline_reports_deadline_predicate(self):
        outcome = compat.run_cycle(self.config(), time.monotonic() - 1, lambda *_: True)
        self.assertEqual(outcome.predicate, "deadline")


class AuthTests(CheckerCase):
    def basic(self, credentials):
        return "Basic " + base64.b64encode(credentials.encode()).decode()

    def test_cookie_auth(self):
        self.assertTrue(self.cycle().passed)
        self.assertEqual(self.zakura.auth_headers[-1], self.basic(SECRET_COOKIE))

    def test_missing_and_malformed_cookie(self):
        outcome = self.cycle(zcashd_cookie_file=self.dir / "absent")
        self.assertEqual(outcome.error_kind, "auth_unavailable")
        self.zcashd_cookie.write_text("no-colon")
        self.assertEqual(self.cycle().error_kind, "auth_malformed")

    def test_config_file_auth_and_explicit_overrides(self):
        conf = self.dir / "zcash.conf"
        conf.write_text("# comment\nrpcuser = alice\nrpcpassword=" + SECRET_PASSWORD + "\n")
        self.assertTrue(self.cycle(zcashd_cookie_file="", zcashd_rpc_conf=conf).passed)
        self.assertEqual(self.zcashd.auth_headers[-1], self.basic("alice:" + SECRET_PASSWORD))
        self.assertTrue(
            self.cycle(zcashd_cookie_file="", zcashd_rpc_conf=conf, zcashd_rpc_user="bob").passed
        )
        self.assertEqual(self.zcashd.auth_headers[-1], self.basic("bob:" + SECRET_PASSWORD))

    def test_missing_config_file(self):
        outcome = self.cycle(zcashd_cookie_file="", zcashd_rpc_conf=self.dir / "absent.conf")
        self.assertEqual(outcome.error_kind, "auth_unavailable")

    def test_explicit_user_password(self):
        self.assertTrue(
            self.cycle(zakura_cookie_file="", zakura_rpc_user="carol",
                       zakura_rpc_password=SECRET_PASSWORD).passed
        )
        self.assertEqual(self.zakura.auth_headers[-1], self.basic("carol:" + SECRET_PASSWORD))

    def test_explicitly_empty_cookie_without_other_auth_sends_none(self):
        self.assertTrue(self.cycle(zakura_cookie_file="").passed)
        self.assertIsNone(self.zakura.auth_headers[-1])

    def test_cookie_takes_precedence_over_user_password(self):
        self.assertTrue(self.cycle(zakura_rpc_user="ignored", zakura_rpc_password="ignored").passed)
        self.assertEqual(self.zakura.auth_headers[-1], self.basic(SECRET_COOKIE))

    def test_authentication_failure_is_an_http_status(self):
        self.zakura.replies["getblockcount"] = (401, b"")
        self.assertEqual(self.cycle().error_kind, "http_status")


class ConfigurationTests(unittest.TestCase):
    def test_defaults_match_the_canonical_check(self):
        values = compat.resolve_settings({}, {}, {})
        config = compat.build_config(values)
        self.assertEqual(config.zakura.url, "http://127.0.0.1:8232")
        self.assertEqual(config.zcashd.url, "http://[::1]:8232")
        self.assertEqual(config.zakura.cookie_file, "/root/.cache/zakura/.cookie")
        self.assertEqual(config.zcashd.cookie_file, "/mnt/data/runtime/zcashd/.cookie")
        self.assertEqual(
            (config.sync_check_timeout, config.sync_check_interval, config.rpc_timeout),
            (600, 15, 30),
        )
        self.assertEqual(
            config.deployment_suppression_file,
            Path("/run/zakura-watchdog/deployment-suppressed-until"),
        )
        self.assertEqual(config.max_deployment_suppression, 1200)

    def test_shell_empty_semantics(self):
        environ = {"ZAKURA_COOKIE_FILE": "", "ZAKURA_RPC_URL": "", "HEIGHT_MAX_DRIFT": ""}
        values = compat.resolve_settings({}, environ, {})
        self.assertEqual(values["zakura_cookie_file"], "")
        self.assertEqual(values["zakura_rpc_url"], "http://127.0.0.1:8232")
        self.assertEqual(values["height_max_drift"], "30")

    def test_precedence_cli_environment_env_file(self):
        values = compat.resolve_settings(
            {"height_max_drift": "1"},
            {"HEIGHT_MAX_DRIFT": "2", "SYNC_CHECK_INTERVAL": "3"},
            {"HEIGHT_MAX_DRIFT": "4", "SYNC_CHECK_INTERVAL": "5", "SYNC_CHECK_TIMEOUT": "6"},
        )
        self.assertEqual(
            (values["height_max_drift"], values["sync_check_interval"],
             values["sync_check_timeout"]),
            ("1", "3", "6"),
        )

    def test_env_file_reads_only_checker_keys(self):
        with tempfile.TemporaryDirectory() as directory:
            env = Path(directory) / "env"
            env.write_text(
                "SENTRY_DSN=https://example.invalid/1\nHEIGHT_MAX_DRIFT=12\n"
                "export ZCASHD_RPC_URL=\"http://127.0.0.1:9\"\n# HEIGHT_MAX_DRIFT=99\n"
            )
            self.assertEqual(
                compat.read_env_file(env),
                {"HEIGHT_MAX_DRIFT": "12", "ZCASHD_RPC_URL": "http://127.0.0.1:9"},
            )
            self.assertEqual(compat.read_env_file(Path(directory) / "absent"), {})

    def test_invalid_values_raise_without_echoing_them(self):
        for key, value in (
            ("height_max_drift", "-1"),
            ("sync_check_timeout", "ten"),
            ("rpc_timeout", "0"),
            ("zakura_rpc_url", "ftp://host"),
            ("zcashd_rpc_url", "http://user:" + SECRET_PASSWORD + "@host"),
            ("zakurad_process_pattern", "("),
        ):
            with self.subTest(key=key):
                values = compat.resolve_settings({}, {}, {})
                values[key] = value
                with self.assertRaises(compat.ConfigError) as raised:
                    compat.build_config(values)
                self.assertNotIn(SECRET_PASSWORD, str(raised.exception))

    def test_ipv6_loopback_default_reaches_the_server(self):
        if not ipv6_available():
            self.skipTest("IPv6 loopback unavailable")
        server = RpcServer("::1")
        self.addCleanup(server.close)
        server.replies = {"getconnectioncount": {"result": 1, "error": None}}
        endpoint = compat.Endpoint("zcashd", server.url, "", "", "", "")
        self.assertEqual(
            compat.json_rpc(endpoint, "getconnectioncount", time.monotonic() + 5, 5), 1
        )


class CheckLoopTests(unittest.TestCase):
    def config(self, timeout=600, interval=15):
        values = compat.resolve_settings({}, {}, {})
        values.update(sync_check_timeout=str(timeout), sync_check_interval=str(interval))
        return compat.build_config(values)

    def outcome(self, passed):
        status = compat.PASS if passed else compat.FAIL
        predicate = "in_sync" if passed else "peer_pinning"
        return compat.Outcome(status, predicate, "s", {}, time.time())

    def run_check(self, results, timeout=600, interval=15):
        calls = iter(results)
        sleeps = []
        out, err = io.StringIO(), io.StringIO()
        code = compat.check(
            self.config(timeout, interval),
            cycle=lambda _config, _deadline: self.outcome(next(calls)),
            sleep=sleeps.append,
            out=out,
            err=err,
        )
        return code, sleeps, out.getvalue(), err.getvalue()

    def test_passes_immediately(self):
        code, sleeps, out, _ = self.run_check([True])
        self.assertEqual((code, sleeps), (0, []))
        self.assertIn("zcashd-compat sync check passed", out)

    def test_retries_on_the_interval_until_pass(self):
        code, sleeps, _, _ = self.run_check([False, False, True])
        self.assertEqual((code, sleeps), (0, [15, 15]))

    def test_fails_when_the_deadline_expires(self):
        code, sleeps, _, err = self.run_check([False] * 3, timeout=0)
        self.assertEqual((code, sleeps), (1, []))
        self.assertIn("timed out", err)

    def test_whole_deadline_is_respected(self):
        started = time.monotonic()
        code = compat.check(
            self.config(timeout=1, interval=15),
            cycle=lambda *_: self.outcome(False),
            out=io.StringIO(),
            err=io.StringIO(),
        )
        self.assertEqual(code, 1)
        self.assertLess(time.monotonic() - started, 3)


class CommandLineTests(CheckerCase):
    """Run the real entry point the way deployments and the fleet do."""

    def run_checker(self, *args, env_overrides=None, pgrep=True):
        bin_dir = self.dir / "bin"
        bin_dir.mkdir(exist_ok=True)
        fake = bin_dir / "pgrep"
        fake.write_text("#!/bin/sh\necho 4242\nexit 0\n" if pgrep else "#!/bin/sh\nexit 1\n")
        fake.chmod(fake.stat().st_mode | stat.S_IXUSR)
        env = {
            "PATH": f"{bin_dir}:{os.environ.get('PATH', '/usr/bin:/bin')}",
            "ZAKURA_RPC_URL": self.zakura.url,
            "ZCASHD_RPC_URL": self.zcashd.url,
            "ZAKURA_COOKIE_FILE": str(self.zakura_cookie),
            "ZCASHD_COOKIE_FILE": str(self.zcashd_cookie),
            "WATCHDOG_DEPLOYMENT_SUPPRESSION_FILE": str(self.dir / "marker"),
            **(env_overrides or {}),
        }
        return subprocess.run(
            [sys.executable, "-I", str(CHECKER), *args],
            env=env, capture_output=True, text=True, timeout=60, cwd="/",
        )

    def test_probe_prints_one_valid_outcome_when_health_fails(self):
        result = self.run_checker("probe", "--nonce", "abc", pgrep=False)
        self.assertEqual(result.returncode, 0)
        outcome = json.loads(result.stdout)
        self.assertEqual(
            (outcome["schema"], outcome["status"], outcome["predicate"], outcome["nonce"]),
            (compat.SCHEMA, "fail", "zakurad_process", "abc"),
        )
        self.assertEqual(outcome["suppression"]["state"], "missing")

    def test_probe_reports_suppression_metadata(self):
        (self.dir / "marker").write_text(str(int(time.time()) + 300))
        outcome = json.loads(self.run_checker("probe").stdout)
        self.assertEqual(outcome["status"], "pass")
        self.assertTrue(outcome["suppression"]["active"])

    def test_outputs_never_contain_credentials(self):
        self.zakura.replies["getblockcount"] = {"result": None, "error": {"message": SECRET_COOKIE}}
        for mode in ("probe",):
            result = self.run_checker(mode)
            self.assertNotIn("s3cr3t", result.stdout + result.stderr)
        result = self.run_checker(
            "check", env_overrides={"SYNC_CHECK_TIMEOUT": "0",
                                    "ZAKURA_RPC_PASSWORD": SECRET_PASSWORD}
        )
        self.assertEqual(result.returncode, 1)
        self.assertNotIn("s3cr3t", result.stdout + result.stderr)

    def test_check_exit_codes(self):
        self.assertEqual(self.run_checker("check").returncode, 0)
        self.assertEqual(
            self.run_checker("check", env_overrides={"SYNC_CHECK_TIMEOUT": "0"},
                             pgrep=False).returncode,
            1,
        )
        invalid = self.run_checker("check", env_overrides={"HEIGHT_MAX_DRIFT": "x"})
        self.assertEqual(invalid.returncode, 2)
        self.assertIn("HEIGHT_MAX_DRIFT", invalid.stderr)

    def test_check_ignores_slack_and_suppression(self):
        (self.dir / "marker").write_text(str(int(time.time()) + 300))
        result = self.run_checker(
            "check", env_overrides={"SYNC_CHECK_TIMEOUT": "0"}, pgrep=False
        )
        self.assertEqual(result.returncode, 1)
        self.assertEqual(self.run_checker("check").returncode, 0)

    def test_probe_invalid_config_is_json_with_exit_two(self):
        result = self.run_checker("probe", "--nonce", "n1",
                                  env_overrides={"ZAKURA_RPC_URL": "nope"})
        self.assertEqual(result.returncode, 2)
        self.assertEqual(json.loads(result.stdout)["predicate"], "invalid_config")

    def test_env_file_configuration(self):
        env_file = self.dir / "compat.env"
        env_file.write_text("HEIGHT_MAX_DRIFT=0\n")
        self.healthy(zakura=101, zcashd=100)
        outcome = json.loads(self.run_checker("probe", "--env-file", str(env_file)).stdout)
        self.assertEqual(outcome["predicate"], "height_drift")
        self.assertEqual(outcome["details"]["height_max_drift"], 0)

    def test_shell_wrapper_runs_the_python_checker(self):
        wrapper = HERE.parent / "zcashd-compat" / "sync-check.sh"
        if not wrapper.exists():
            self.skipTest("shell wrapper is not part of an installed release")
        result = subprocess.run(
            ["bash", str(wrapper)],
            env={**os.environ, "ZAKURA_COMPAT_CHECK": str(CHECKER),
                 "ZAKURA_RPC_URL": self.zakura.url, "ZCASHD_RPC_URL": self.zcashd.url,
                 "ZAKURA_COOKIE_FILE": str(self.zakura_cookie),
                 "ZCASHD_COOKIE_FILE": str(self.zcashd_cookie),
                 "HEIGHT_MAX_DRIFT": "bad"},
            capture_output=True, text=True, timeout=60,
        )
        self.assertEqual(result.returncode, 2)


class ProcessQueryTests(unittest.TestCase):
    def test_own_process_tree_never_satisfies_a_pattern(self):
        with mock.patch.object(compat.subprocess, "run") as run:
            run.return_value = subprocess.CompletedProcess([], 0, stdout=f"{os.getpid()}\n".encode())
            self.assertFalse(compat.process_running("anything", 1))
            run.return_value = subprocess.CompletedProcess([], 0, stdout=b"999999\n")
            self.assertTrue(compat.process_running("anything", 1))
            run.return_value = subprocess.CompletedProcess([], 1, stdout=b"")
            self.assertFalse(compat.process_running("anything", 1))
            run.side_effect = subprocess.TimeoutExpired("pgrep", 1)
            self.assertFalse(compat.process_running("anything", 1))


class SuppressionMarkerTests(unittest.TestCase):
    def classify(self, content, now=1_000_000.0, max_seconds=1200):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "marker"
            if content is not None:
                path.write_bytes(content.encode() if isinstance(content, str) else content)
            return suppression.compat_suppression(path, now, max_seconds)

    def test_marker_states(self):
        now = 1_000_000
        self.assertEqual(self.classify(None).state, "missing")
        self.assertEqual(self.classify(f"{now + 600}\n").state, "active")
        self.assertEqual(self.classify(f"{now + 1200}").state, "active")
        self.assertEqual(self.classify(f"{now + 1201}").state, "excessive")
        self.assertEqual(self.classify(f"{now}").state, "expired")
        self.assertEqual(self.classify(f"{now - 5}").state, "expired")
        for malformed in ("", "soon", "1.5", "-5", "9" * 100, b"\xff"):
            with self.subTest(malformed=malformed):
                self.assertEqual(self.classify(malformed).state, "malformed")
                self.assertFalse(self.classify(malformed).active)
        self.assertFalse(self.classify(f"{now + 5000}").active)


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        unittest.main()
