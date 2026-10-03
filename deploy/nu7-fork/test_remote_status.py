"""Mining observations must not mix retained histories across a reset."""
import os
import runpy
import tempfile
import unittest
from pathlib import Path

module = runpy.run_path(str(Path(__file__).parent / 'miner/remote-status.py'))
MinedBlocks = module['MinedBlocks']

ACCEPTED = ('{time}  INFO run_mining_solver{{solver_id=0}}: zakurad::components::miner: '
            'successfully mined a new block height=Height(4420700) solver_id=0 success=Accepted\n')
REJECTED = ('{time}  INFO run_mining_solver{{solver_id=0}}: zakurad::components::miner: '
            'successfully mined a new block height=Height(4420701) solver_id=0 '
            'success=ErrorResponse(Rejected)\n')


def stamp(seconds):
    from datetime import datetime, timezone
    return datetime.fromtimestamp(seconds, timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')


class LogCase(unittest.TestCase):
    def log(self, *lines):
        handle = tempfile.NamedTemporaryFile('w', suffix='.log', delete=False)
        handle.write(''.join(lines))
        handle.close()
        self.addCleanup(Path(handle.name).unlink, missing_ok=True)
        return handle.name


class GenerationBoundaryTests(LogCase):
    def test_current_generation_excludes_previous_network_blocks(self):
        path = self.log(ACCEPTED.format(time=stamp(198_000)), ACCEPTED.format(time=stamp(199_500)))
        self.assertEqual(MinedBlocks(path, since=199_000).count_24h(now=200_000), 1)

    def test_day_window_advances_beyond_generation_start(self):
        path = self.log(ACCEPTED.format(time=stamp(113_000)), ACCEPTED.format(time=stamp(113_700)))
        self.assertEqual(MinedBlocks(path, since=100_000).count_24h(now=200_000), 1)


class MinedBlockLogTests(LogCase):
    def test_only_accepted_blocks_count(self):
        path = self.log(ACCEPTED.format(time=stamp(199_000)), REJECTED.format(time=stamp(199_001)),
                        f'{stamp(199_002)}  INFO zakurad: mining with an updated block template\n')
        self.assertEqual(MinedBlocks(path).count_24h(now=200_000), 1)

    def test_new_lines_are_read_incrementally_and_partial_lines_wait(self):
        path = self.log(ACCEPTED.format(time=stamp(199_000)))
        counter = MinedBlocks(path)
        self.assertEqual(counter.count_24h(now=200_000), 1)
        line = ACCEPTED.format(time=stamp(199_100))
        with open(path, 'a') as stream:
            stream.write(line[:20])
        self.assertEqual(counter.count_24h(now=200_000), 1)
        with open(path, 'a') as stream:
            stream.write(line[20:])
        self.assertEqual(counter.count_24h(now=200_000), 2)

    def test_a_truncated_log_is_read_from_the_start(self):
        path = self.log(ACCEPTED.format(time=stamp(199_000)), ACCEPTED.format(time=stamp(199_001)))
        counter = MinedBlocks(path)
        self.assertEqual(counter.count_24h(now=200_000), 2)
        Path(path).write_text(ACCEPTED.format(time=stamp(199_500)))
        self.assertEqual(counter.count_24h(now=200_000), 1)

    def replace(self, path, *lines):
        """Rotate: a new file takes the old name, as logrotate's create mode does."""
        fresh = self.log(*lines)
        os.replace(fresh, path)

    def test_a_rotated_log_of_equal_size_is_a_new_file(self):
        first = ACCEPTED.format(time=stamp(199_000))
        path = self.log(first)
        counter = MinedBlocks(path)
        self.assertEqual(counter.count_24h(now=200_000), 1)
        # Same length, different content: an offset check alone would keep the old count.
        self.replace(path, REJECTED.format(time=stamp(199_100))[:len(first) - 1] + "\n")
        self.assertEqual(len(Path(path).read_bytes()), len(first.encode()))
        self.assertEqual(counter.count_24h(now=200_000), 0)

    def test_a_rotated_larger_log_is_counted_from_its_start(self):
        path = self.log(ACCEPTED.format(time=stamp(199_000)))
        counter = MinedBlocks(path)
        self.assertEqual(counter.count_24h(now=200_000), 1)
        self.replace(path, ACCEPTED.format(time=stamp(199_200)), ACCEPTED.format(time=stamp(199_300)),
                     REJECTED.format(time=stamp(199_301)))
        self.assertEqual(counter.count_24h(now=200_000), 2)

    def test_a_partial_line_in_a_rotated_log_waits_for_its_newline(self):
        path = self.log(ACCEPTED.format(time=stamp(199_000)))
        counter = MinedBlocks(path)
        counter.count_24h(now=200_000)
        line = ACCEPTED.format(time=stamp(199_400))
        self.replace(path, line[:30])
        self.assertEqual(counter.count_24h(now=200_000), 0)
        with open(path, 'a') as stream:
            stream.write(line[30:])
        self.assertEqual(counter.count_24h(now=200_000), 1)

    def test_an_unreadable_log_is_unknown_not_zero(self):
        self.assertIsNone(MinedBlocks('/nonexistent/zakura.log').count_24h(now=200_000))

    def test_input_failures_are_logged_once_when_they_start_and_end(self):
        path = self.log(ACCEPTED.format(time=stamp(199_000)))
        counter = MinedBlocks(path)
        counter.count_24h(now=200_000)
        Path(path).unlink()
        with self.assertLogs(level='INFO') as logs:
            counter.count_24h(now=200_000)
            counter.count_24h(now=200_000)
            Path(path).write_text(ACCEPTED.format(time=stamp(199_500)))
            self.assertEqual(counter.count_24h(now=200_000), 1)
        self.assertEqual(len(logs.output), 2)
        self.assertIn('node log is unavailable', logs.output[0])
        self.assertIn('node log is readable again', logs.output[1])

    def test_miner_state_comes_from_the_node_config(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / 'zakura.toml'
            config.write_text('[mining]\nminer_address = "tmX"\ninternal_miner = true\n'
                              '[tracing]\nlog_file = "/var/log/zakura/zakura-fork.log"\n')
            self.assertEqual(module['node_settings'](config),
                             (True, '/var/log/zakura/zakura-fork.log'))
            config.write_text('[mining]\nminer_address = "tmX"\n')
            self.assertEqual(module['node_settings'](config), (False, None))



class EndpointHardeningTests(unittest.TestCase):
    def test_repeated_reads_share_one_node_sample(self):
        calls = []
        cached = module['CachedSample'](lambda: calls.append(1) or len(calls), ttl=5)
        self.assertEqual([cached.get(now=t) for t in (0, 1, 4.9)], [1, 1, 1])
        self.assertEqual(cached.get(now=5), 2)

    def test_concurrent_requests_are_capped(self):
        import socket
        import threading
        import time
        from http.server import BaseHTTPRequestHandler

        release, lock = threading.Event(), threading.Lock()
        state = {'active': 0, 'peak': 0}

        class Slow(BaseHTTPRequestHandler):
            def do_GET(self):
                with lock:
                    state['active'] += 1
                    state['peak'] = max(state['peak'], state['active'])
                release.wait(5)
                with lock:
                    state['active'] -= 1
                self.send_response(204)
                self.end_headers()

            def log_message(self, *args):
                pass

        server = module['BoundedHTTPServer'](('127.0.0.1', 0), Slow, max_concurrent=2)
        threading.Thread(target=server.serve_forever, daemon=True).start()

        def request():
            with socket.create_connection(server.server_address, timeout=10) as conn:
                conn.sendall(b'GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n')
                conn.recv(64)

        threads = [threading.Thread(target=request) for _ in range(6)]
        for thread in threads:
            thread.start()
        time.sleep(0.5)
        release.set()
        for thread in threads:
            thread.join(10)
        server.shutdown()
        server.server_close()
        self.assertEqual(state['peak'], 2)
        self.assertEqual(module['Handler'].timeout, module['REQUEST_TIMEOUT_SECONDS'])

    def test_units_run_unprivileged_and_restrict_the_health_port(self):
        here = Path(__file__).parent
        health = (here / 'miner/zakura-nu7-miner-status.service').read_text()
        collector = (here / 'dashboard.service').read_text()
        for unit in (health, collector):
            self.assertIn('DynamicUser=yes', unit)
            self.assertNotIn('User=root', unit)
            self.assertIn('NoNewPrivileges=true', unit)
        # The health report needs no journal at all; the collector needs only the
        # journal group, not root.
        self.assertNotIn('SupplementaryGroups', health)
        self.assertIn('SupplementaryGroups=systemd-journal', collector)
        self.assertIn('IPAddressDeny=any', health)
        self.assertIn('IPAddressAllow=localhost', health)
        allowlist = (here / 'miner/collector-allowlist.conf').read_text()
        self.assertIn('IPAddressAllow=167.99.146.155', allowlist)
        self.assertIn('--host 127.0.0.1', collector)


if __name__ == '__main__':
    unittest.main()
