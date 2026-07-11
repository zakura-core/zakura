import importlib.util
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT_DIR = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("deploy", SCRIPT_DIR / "deploy.py")
deploy = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(deploy)


class BuildCacheTests(unittest.TestCase):
    def test_build_cache_dir_uses_env_override(self):
        with mock.patch.dict(os.environ, {
            deploy.BUILD_CACHE_DIR_ENV: "/tmp/zakura-build-cache",
        }):
            self.assertEqual(deploy.build_cache_dir(), Path("/tmp/zakura-build-cache"))
            self.assertEqual(
                deploy.cached_binary("abc123"),
                Path("/tmp/zakura-build-cache/zakurad-abc123"),
            )

    def test_cached_binary_reused_without_build(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp)
            sha = "a" * 40
            target = cache_dir / f"zakurad-{sha}"
            target.write_text("cached")

            with mock.patch.dict(os.environ, {deploy.BUILD_CACHE_DIR_ENV: str(cache_dir)}), \
                    mock.patch.object(deploy, "binary_is_runnable", return_value=True), \
                    mock.patch.object(deploy, "run") as run:
                self.assertEqual(deploy.build_commit(Path(tmp), sha), target)
                run.assert_not_called()

    def test_corrupt_cached_binary_is_rebuilt(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cache_dir = root / "cache"
            cache_dir.mkdir()
            sha = "b" * 40
            target = cache_dir / f"zakurad-{sha}"
            target.write_text("corrupt")
            calls = []

            def fake_run(cmd, *, cwd=None, capture=False, check=True):
                calls.append(cmd)
                if cmd[:3] == ["git", "worktree", "add"]:
                    Path(cmd[-2]).mkdir(parents=True)
                if cmd[:2] == ["cargo", "build"]:
                    built = Path(cwd) / "target" / "release" / "zakurad"
                    built.parent.mkdir(parents=True)
                    built.write_text("rebuilt")
                return mock.Mock(returncode=0, stdout="")

            with mock.patch.dict(os.environ, {deploy.BUILD_CACHE_DIR_ENV: str(cache_dir)}), \
                    mock.patch.object(deploy, "binary_is_runnable", return_value=False), \
                    mock.patch.object(deploy, "run", side_effect=fake_run):
                self.assertEqual(deploy.build_commit(root, sha), target)

            self.assertEqual(target.read_text(), "rebuilt")
            self.assertIn(["cargo", "build", "--release", "--locked", "-p", "zakura"], calls)

    def test_force_rebuild_skips_cached_binary_check(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cache_dir = root / "cache"
            cache_dir.mkdir()
            sha = "c" * 40
            (cache_dir / f"zakurad-{sha}").write_text("cached")

            def fake_run(cmd, *, cwd=None, capture=False, check=True):
                if cmd[:3] == ["git", "worktree", "add"]:
                    Path(cmd[-2]).mkdir(parents=True)
                if cmd[:2] == ["cargo", "build"]:
                    built = Path(cwd) / "target" / "release" / "zakurad"
                    built.parent.mkdir(parents=True)
                    built.write_text("forced")
                return mock.Mock(returncode=0, stdout="")

            with mock.patch.dict(os.environ, {deploy.BUILD_CACHE_DIR_ENV: str(cache_dir)}), \
                    mock.patch.object(deploy, "binary_is_runnable") as binary_is_runnable, \
                    mock.patch.object(deploy, "run", side_effect=fake_run):
                deploy.build_commit(root, sha, force=True)

            binary_is_runnable.assert_not_called()

    def test_prune_cached_binaries_keeps_current_and_recent(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp)
            current_sha = "d" * 40
            (cache_dir / f"zakurad-{current_sha}").write_text("current")
            old = []
            for idx in range(5):
                path = cache_dir / f"zakurad-{idx:040x}"
                path.write_text(str(idx))
                os.utime(path, (100 + idx, 100 + idx))
                old.append(path)

            with mock.patch.dict(os.environ, {deploy.BUILD_CACHE_RETAIN_ENV: "3"}):
                deploy.prune_cached_binaries(cache_dir, current_sha)

            remaining = {path.name for path in cache_dir.iterdir()}
            self.assertEqual(remaining, {
                f"zakurad-{current_sha}",
                old[4].name,
                old[3].name,
            })


class MountRenderingTests(unittest.TestCase):
    def node(self, **overrides):
        data = {
            "name": "node-a",
            "ssh_string": "root@example",
            "commit": "main",
            "deploy_kind": "systemd",
            "manage_config": True,
            "service_name": "zakurad",
            "bin_path": "/usr/local/bin/zakurad",
            "config_path": "/etc/zakura/zakura.toml",
            "log_file": "/var/log/zakura/zakura.log",
            "state_cache_dir": "/var/lib/zakura",
            "network": "Testnet",
            "listen_addr": "0.0.0.0:18233",
            "identity_dir": "",
            "network_cache_dir": "",
            "rpc_listen_addr": "",
            "rpc_enable_cookie_auth": None,
            "storage_mode": "archive",
            "p2p_stack": "dual",
            "metrics_endpoint": "",
            "tracing_filter": "",
            "checkpoint_sync": True,
            "vct_fast_sync": True,
            "zakura": None,
            "working_dir": "",
            "start_command": "",
            "process_pattern": "",
            "container_name": "",
        }
        data.update(overrides)
        return deploy.Node(**data)

    def test_render_service_requires_data_mount_for_data_paths(self):
        service = deploy.render_service(self.node(
            state_cache_dir="/mnt/data/zakura-cache",
            log_file="/mnt/data/logs/zakura.log",
        ))

        self.assertIn("RequiresMountsFor=/mnt/data", service)
        self.assertIn("AssertPathIsMountPoint=/mnt/data", service)

    def test_render_service_omits_mount_for_non_data_paths(self):
        service = deploy.render_service(self.node())

        self.assertNotIn("RequiresMountsFor=/mnt/data", service)
        self.assertNotIn("AssertPathIsMountPoint=/mnt/data", service)


if __name__ == "__main__":
    unittest.main()
