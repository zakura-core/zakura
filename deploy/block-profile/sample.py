#!/usr/bin/env python3
"""Continuous bounded perf rotation with a separate decoder and explicit coverage limits."""
import argparse
import decimal
import fcntl
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import re
import resource
import signal
import shutil
import sqlite3
import subprocess
import time
import uuid

MAX_FILE = 256 * 1024 * 1024
MAX_SYMBOL_FILE = 1024 * 1024 * 1024
MAX_JSON = 12 * 1024 * 1024
MAX_SYMBOL = 65536
MAX_LINE = 256 * 1024
RAW_BUDGET = 8_000_000_000
INBOX_BUDGET = 128 * 1024 * 1024
SYMBOL_BUDGET = 2_000_000_000
MAX_PENDING = 8
HEADER = re.compile(r"^\s*(\d+)(?:/|\s+)(\d+)\s+(\d+\.\d+):\s+(?:(\d+)\s+)?(\S+:)\s*(.*)$")
FRAME = re.compile(r"^\s*([0-9a-fA-F]+)\s+(.+?)\s+\((.+)\)\s*$")
LOST = re.compile(r"(?:PERF_RECORD_LOST.*?lost\s*[:=]?\s*(\d+)|LOST\s+(\d+)(?:\s+events)?)", re.I)


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open('rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def monotonic_us():
    return time.clock_gettime_ns(time.CLOCK_MONOTONIC) // 1000


def atomic_json(path, value):
    temp = path.with_name(path.name + '.tmp')
    with temp.open('w') as out:
        json.dump(value, out, separators=(',', ':'), ensure_ascii=False)
        out.flush()
        os.fsync(out.fileno())
    temp.replace(path)


def parse_perf(lines, pid, start, end, require_period=False):
    """Keep raw symbol identity, intern repeated stacks, and count every bounded omission."""
    frames, stacks, samples, frame_ids, stack_ids = [], [], [], {}, {}
    current, size = None, 0
    stats = dict(decode_errors=0, truncated=False, lost_samples=0,
                 omitted_samples=0, omitted_frames=0, symbol_truncations=0)

    def append_sample():
        nonlocal current, size
        if current is None:
            return
        stamp, tid, period, raw_frames = current
        current = None
        if len(samples) >= 50000 or size >= MAX_JSON:
            stats['omitted_samples'] += 1
            stats['truncated'] = True
            return
        indices = []
        for ip, symbol, dso in raw_frames:
            identity = (ip, symbol, dso)
            if identity not in frame_ids:
                if len(frames) >= 50000:
                    stats['omitted_samples'] += 1
                    stats['truncated'] = True
                    return
                frame = dict(ip=ip, symbol=symbol, dso=dso)
                cost = len(json.dumps(frame, ensure_ascii=False).encode()) + 1
                if size + cost > MAX_JSON:
                    stats['omitted_samples'] += 1
                    stats['truncated'] = True
                    return
                size += cost
                frame_ids[identity] = len(frames)
                frames.append(frame)
            indices.append(frame_ids[identity])
        stack = tuple(indices)
        if stack not in stack_ids:
            if len(stacks) >= 50000:
                stats['omitted_samples'] += 1
                stats['truncated'] = True
                return
            size += len(json.dumps(indices)) + 1
            stack_ids[stack] = len(stacks)
            stacks.append(indices)
        sample = dict(mono_us=stamp, tid=tid, stack=stack_ids[stack])
        if period is not None:
            sample['cpu_period_ns'] = period
        size += len(json.dumps(sample)) + 1
        if size > MAX_JSON:
            stats['omitted_samples'] += 1
            stats['truncated'] = True
        else:
            samples.append(sample)

    for line in lines:
        if len(line.encode()) > MAX_LINE:
            stats['decode_errors'] += 1
            stats['truncated'] = True
            current = None
            continue
        if 'PERF_RECORD_LOST' in line or re.match(r'^\s*LOST\s+\d+\s+events', line):
            match = LOST.search(line)
            if match and stats['lost_samples'] is not None:
                stats['lost_samples'] += int(next(x for x in match.groups() if x))
            else:
                stats['lost_samples'] = None
                stats['decode_errors'] += 1
            continue
        match = HEADER.match(line)
        if match:
            append_sample()
            sample_pid, tid, stamp, period, event, tail = match.groups()
            stamp = int(decimal.Decimal(stamp) * 1_000_000)
            period = int(period) if period is not None else None
            if require_period and (event != 'cpu-clock:u:' or period is None or not 0 < period <= 1_000_000_000):
                stats['decode_errors'] += 1
                stats['omitted_samples'] += 1
                stats['truncated'] = True
                current = None
                continue
            current = [stamp, int(tid), period, []] if int(sample_pid) == pid and start <= stamp <= end else None
            line = tail
        elif not line.strip():
            append_sample()
            continue
        frame = FRAME.match(line)
        if frame and current is not None:
            ip, symbol, dso = frame.groups()
            # IP and full DSO remain part of identity even if an exceptional symbol exceeds the cap.
            if len(symbol.encode()) > MAX_SYMBOL:
                symbol = symbol.encode()[:MAX_SYMBOL].decode(errors='ignore')
                stats['symbol_truncations'] += 1
                stats['truncated'] = True
            if len(dso.encode()) > 4096:
                stats['omitted_frames'] += 1
                stats['truncated'] = True
                continue
            if len(current[3]) < 128:
                current[3].append((ip, symbol, dso))
            else:
                stats['omitted_frames'] += 1
                stats['truncated'] = True
        elif line.strip() and not match:
            stats['decode_errors'] += 1
    append_sample()
    return dict(frames=frames, stacks=stacks, samples=samples, **stats)


def limits():
    resource.setrlimit(resource.RLIMIT_FSIZE, (MAX_FILE, MAX_FILE))
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))


def symbol_limits():
    resource.setrlimit(resource.RLIMIT_FSIZE, (MAX_SYMBOL_FILE, MAX_SYMBOL_FILE))
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))


def identity(pid, executable):
    proc = Path(f'/proc/{pid}')
    if not os.path.samefile(proc / 'exe', executable):
        raise RuntimeError('PID does not match configured node executable')
    return int((proc / 'stat').read_text().rsplit(') ', 1)[1].split()[19])


def node_cgroup(pid, service):
    group = subprocess.check_output(['systemctl', 'show', service, '-p', 'ControlGroup', '--value'], text=True).strip()
    member = next((line[3:] for line in Path(f'/proc/{pid}/cgroup').read_text().splitlines() if line.startswith('0::')), None)
    if not group.startswith('/') or group == '/' or '..' in group.split('/') or member != group:
        raise RuntimeError('Node does not belong to the exact non-root supervised cgroup')
    return group


def current_run(store, pid, start_ticks):
    with sqlite3.connect(f'file:{store / "index.sqlite"}?mode=ro', uri=True, timeout=0.1) as db:
        rows = db.execute('SELECT metadata,seen_ms FROM runs ORDER BY utc_ms DESC LIMIT 100').fetchall()
    process_start_us = start_ticks * 1_000_000 // os.sysconf('SC_CLK_TCK')
    for metadata, seen in rows:
        run = json.loads(metadata)
        if run['pid'] == pid and run['monotonic_start_us'] is not None and run['monotonic_start_us'] >= process_start_us and time.time() * 1000 - seen < 10000:
            return run
    raise RuntimeError('No fresh profiling run matches node process')


def open_inodes(pid):
    """Fail closed if descriptor inspection is unavailable, never infer closure from file size."""
    directory = Path('/proc') / str(pid) / 'fd'
    result = set()
    for path in directory.iterdir():
        try:
            stat = path.stat()
            result.add((stat.st_dev, stat.st_ino))
        except FileNotFoundError:
            continue
    return result


def sealed_files(directory, opened):
    result = []
    for path in directory.glob('capture.data.*'):
        if not re.fullmatch(r'capture\.data\.\d+', path.name):
            continue
        try:
            stat = path.stat()
        except FileNotFoundError:
            continue
        if path.is_file() and (stat.st_dev, stat.st_ino) not in opened:
            result.append(path)
    return sorted(result, key=lambda path: path.name)


def directory_size(root):
    total = 0
    for path in root.rglob('*'):
        try:
            if path.is_file() and not path.is_symlink():
                total += path.stat().st_size
        except FileNotFoundError:
            pass
    return total


def publish_loss(store, manifest, reason):
    data = {**manifest['capture'], 'frames': [], 'stacks': [], 'samples': [],
            'decode_errors': 1, 'truncated': True, 'lost_samples': None,
            'omitted_samples': 0, 'omitted_frames': 0, 'symbol_truncations': 0}
    # Detailed errors stay private; the importer sees explicit incomplete coverage.
    atomic_json(store / 'inbox' / (manifest['key'] + '.json'), data)
    manifest.update(state='lost', error=reason[:512])
    atomic_json(Path(manifest['raw']).with_name(Path(manifest['raw']).name + '.done.json'), manifest)


def bounded_lines(file):
    while True:
        line = file.readline(MAX_LINE + 1)
        if not line:
            return
        yield line
        if len(line) > MAX_LINE and not line.endswith('\n'):
            while line and not line.endswith('\n'):
                line = file.readline(MAX_LINE + 1)


def decode_one(store, path):
    working = path.with_suffix('.working')
    try:
        path.rename(working)
    except FileNotFoundError:
        return
    manifest = json.loads(working.read_text())
    source = Path(manifest['raw'])
    decoded = source.with_suffix('.txt')
    errors_path = source.with_suffix('.decode-stderr')
    try:
        with decoded.open('wb') as out, errors_path.open('wb') as errors:
            subprocess.run(['perf', '--buildid-dir', manifest.get('symbol_dir', str(store / 'symbols')), 'script', '--no-inline', '--ns',
                            '--show-lost-events', '-F', 'pid,tid,time,period,event,ip,sym,dso', '-i', str(source)],
                           stdout=out, stderr=errors, timeout=45, preexec_fn=limits, check=True)
        capture = manifest['capture']
        with decoded.open(errors='replace') as lines:
            parsed = parse_perf(bounded_lines(lines), capture['pid'], 0, (1 << 64) - 1,
                                require_period=capture.get('schema_version', 0) >= 3)
        if parsed['samples']:
            capture['start_mono_us'] = min(capture['start_mono_us'], min(s['mono_us'] for s in parsed['samples']))
            capture['end_mono_us'] = max(capture['end_mono_us'], max(s['mono_us'] for s in parsed['samples']))
        build = subprocess.run(['perf', 'buildid-list', '-i', str(source)], capture_output=True, text=True, timeout=10, check=True)
        ids = build.stdout.splitlines()
        capture.update(parsed, build_ids=[line[:512] for line in ids[:512]])
        capture['truncated'] |= len(ids) > 512
        atomic_json(store / 'inbox' / (manifest['key'] + '.json'), capture)
        manifest.update(state='published', decode_errors=capture['decode_errors'])
        atomic_json(store / 'cpu-decoder-status.json', dict(session=capture['session'], published_through_mono_us=capture['end_mono_us']))
        atomic_json(source.with_name(source.name + '.done.json'), manifest)
    except (OSError, subprocess.SubprocessError, ValueError) as error:
        publish_loss(store, manifest, str(error))
    finally:
        decoded.unlink(missing_ok=True)
        errors_path.unlink(missing_ok=True)
        working.unlink(missing_ok=True)


def decoder(store, stop):
    os.setsid()
    os.nice(10)
    while True:
        pending = sorted((store / 'raw').glob('*/segment-*.json'))
        if not pending:
            if stop.is_set():
                return
            time.sleep(0.2)
            continue
        if directory_size(store / 'inbox') >= INBOX_BUDGET:
            if stop.is_set():
                return
            time.sleep(1)
            continue
        decode_one(store, pending[0])


def prune_raw(raw):
    """Only decoded immutable files may be pruned. Active and queued files are protected."""
    size = directory_size(raw)
    completed = sorted(raw.glob('*/capture.data*.done.json'), key=lambda p: p.stat().st_mtime)
    for index, marker in enumerate(completed):
        if size < RAW_BUDGET - 3 * MAX_FILE and len(completed) - index <= 512:
            break
        manifest = json.loads(marker.read_text())
        path = Path(manifest['raw'])
        size -= path.stat().st_size if path.exists() else 0
        path.unlink(missing_ok=True)
        size -= marker.stat().st_size
        marker.unlink()
    # Failed launches and drained sessions must not accumulate unbounded directories or logs.
    directories = sorted((p for p in raw.iterdir() if p.is_dir()), key=lambda p: p.stat().st_mtime)
    for index, directory in enumerate(directories):
        if len(directories) - index <= 128 and size < RAW_BUDGET - 3 * MAX_FILE:
            break
        if not any(directory.glob('capture.data*')) and not any(directory.glob('segment-*')):
            size -= directory_size(directory)
            shutil.rmtree(directory)
    return size


def symbol_cache(store, executable, digest, prune=False):
    """Retain required builds. Expire decoded raw evidence before evicting its symbol cache."""
    if executable.stat().st_size > MAX_SYMBOL_FILE:
        raise RuntimeError('Node executable exceeds the 1GiB symbol-file limit')
    symbols, raw = store / 'symbols', store / 'raw'
    if prune:
        symbols.mkdir(exist_ok=True)
        for staging in symbols.iterdir():
            if staging.is_dir() and not staging.is_symlink() and re.fullmatch(r'[0-9a-f]{64}\.building', staging.name):
                shutil.rmtree(staging)
    protected, references, completed = {digest}, set(), {}
    try:
        status = json.loads((store / 'cpu-status.json').read_text())
        if status.get('state') == 'recording' and time.time() * 1000 - status.get('updated_ms', 0) < 30000:
            protected.add(status.get('executable_sha256'))
    except FileNotFoundError:
        pass
    for path in raw.rglob('*'):
        if path.name.endswith(('.json', '.working', '.dropping')):
            try:
                manifest = json.loads(path.read_text())
                if 'capture' not in manifest:
                    continue
                owner = manifest['capture']['executable_sha256']
                references.add(owner)
                if path.name.endswith('.done.json'):
                    completed.setdefault(owner, []).append((path, Path(manifest['raw'])))
                else:
                    protected.add(owner)
            except FileNotFoundError:
                pass
    children = list(symbols.iterdir()) if symbols.exists() else []
    # Legacy 40-character source-revision ELF snapshots remain under the shared disk quota.
    # Only new 64-character executable generations consume this independently managed budget.
    generations = [p for p in children if p.is_dir() and not p.is_symlink() and re.fullmatch('[0-9a-f]{64}', p.name)]
    sizes = {p: directory_size(p) for p in generations}
    used = sum(sizes.values())
    target = symbols / digest
    complete = (target / '.complete').exists()
    addition = 0 if complete else executable.stat().st_size
    if not complete:
        used -= sizes.get(target, 0)  # prepare_symbols replaces this interrupted generation.
    candidates = sorted((p for p in generations if p.name not in protected),
                        key=lambda p: (p.name in references, p.stat().st_mtime))
    removals = []
    for path in candidates:
        if path.name in references and used + addition <= SYMBOL_BUDGET:
            continue
        used -= sizes[path]
        removals.append(path)
    if used + addition > SYMBOL_BUDGET:
        raise RuntimeError('Symbol cache budget exhausted by active or pending captures; deployment cannot proceed safely')
    if prune:
        for path in removals:
            for marker, payload in completed.get(path.name, []):
                payload.unlink(missing_ok=True)
                marker.unlink(missing_ok=True)
            shutil.rmtree(path)
    return target


def prepare_symbols(target, executable, digest):
    """Publish a complete verified generation atomically. Failed copies are never reused."""
    if (target / '.complete').exists():
        return
    # A previous interrupted perf invocation may leave a truncated ELF that perf would skip.
    if target.exists():
        shutil.rmtree(target)
    staging = target.with_name(target.name + '.building')
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir()
    try:
        subprocess.run(['perf', '--buildid-dir', str(staging), 'buildid-cache', '--add', str(executable)],
                       timeout=30, preexec_fn=symbol_limits, check=True)
        copies = list(staging.rglob('elf'))
        if len(copies) != 1 or sha256_file(copies[0]) != digest:
            raise RuntimeError('Cached node executable does not match its source SHA-256')
        (staging / '.complete').write_text(digest + '\n')
        staging.rename(target)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def capture(args, stopping):
    store, executable = args.store.resolve(), args.executable.resolve(strict=True)
    raw, inbox = store / 'raw', store / 'inbox'
    raw.mkdir(exist_ok=True)
    inbox.mkdir(exist_ok=True)
    lock = (raw / 'sampler.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    for interrupted in raw.glob('*/segment-*.working'):
        interrupted.rename(interrupted.with_suffix('.json'))
    pid = args.pid or int(subprocess.check_output(['systemctl', 'show', args.node_service, '-p', 'MainPID', '--value']))
    start_ticks = identity(pid, executable)
    cgroup = node_cgroup(pid, args.node_service)
    run = current_run(store, pid, start_ticks)
    digest = sha256_file(executable)
    prune_raw(raw)
    symbols = symbol_cache(store, executable, digest, prune=True)
    prepare_symbols(symbols, executable, digest)
    session = uuid.uuid4().hex
    directory = raw / session
    directory.mkdir()
    started = monotonic_us()
    state = dict(state='recording', run=run['id'], pid=pid, session=session, frequency=args.frequency,
                 executable_sha256=digest, updated_ms=int(time.time() * 1000), started_mono_us=started, sealed_through_mono_us=started,
                 published_through_mono_us=started, backlog_segments=0, dropped_segments=0, last_error=None)
    base = dict(stack_bytes=args.stack_bytes, schema_version=3, run=run['id'], pid=pid, frequency=args.frequency, clock='monotonic',
                process_start_ticks=start_ticks, executable_sha256=digest, build_ids=[], session=session,
                coverage_proven=False, decode_errors=0, truncated=False)
    context = multiprocessing.get_context('spawn')
    stop_decoder = context.Event()
    worker = context.Process(target=decoder, args=(store, stop_decoder))
    worker.start()
    error_file = (directory / 'record.stderr').open('wb')
    # At 999 Hz, one second keeps 16 busy CPUs below the raw segment limit.
    rotation_seconds = 1 if args.frequency >= 999 else 10
    command = ['perf', '--buildid-dir', str(symbols), 'record', '--no-buildid-cache', '--no-no-buildid', '--buildid-all',
               '--clockid', 'mono', '-e', 'cpu-clock:u', '-F', str(args.frequency), '--call-graph', f'dwarf,{args.stack_bytes}',
               '--mmap-pages', '128', f'--switch-output={rotation_seconds}s', '-a', '-G', cgroup.lstrip('/'), '-o', str(directory / 'capture.data')]
    try:
        process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=error_file, preexec_fn=limits, start_new_session=True)
    except Exception:
        error_file.close()
        stop_decoder.set()
        worker.join(timeout=5)
        if worker.is_alive():
            os.killpg(worker.pid, signal.SIGTERM)
            worker.join(timeout=5)
        raise
    seen, sequence, previous = set(), 0, started
    prune_at = time.monotonic() - 10
    deadline = time.monotonic() + args.duration_seconds if args.duration_seconds else float('inf')

    def seal(opened):
        nonlocal sequence, previous
        for source in sealed_files(directory, opened):
            if source.name in seen:
                continue
            seen.add(source.name)
            end = monotonic_us()
            sequence += 1
            manifest = dict(key=uuid.uuid4().hex, raw=str(source), symbol_dir=str(symbols), state='sealed', capture={**base,
                'sequence':sequence, 'start_mono_us':previous, 'end_mono_us':end,
                'uncertainty_us':max(0, end - previous)})
            # The bracket is conservative, not a promise that perf was enabled for its entirety.
            atomic_json(directory / f'segment-{sequence:08d}.json', manifest)
            previous = end
            state['sealed_through_mono_us'] = end

    try:
        while not stopping[0] and time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(f'perf exited early ({process.returncode})')
            if not worker.is_alive():
                raise RuntimeError('CPU decoder exited')
            if identity(pid, executable) != start_ticks:
                raise RuntimeError('Node process identity changed')
            if f'0::{cgroup}' not in Path(f'/proc/{pid}/cgroup').read_text().splitlines():
                raise RuntimeError('Node left its supervised cgroup')
            seal(open_inodes(process.pid))
            pending = sorted(raw.glob('*/segment-*.json'))
            for path in pending[:-MAX_PENDING]:
                try:
                    claimed = path.with_suffix('.dropping')
                    path.rename(claimed)
                    manifest = json.loads(claimed.read_text())
                    claimed.unlink()
                except FileNotFoundError:
                    continue
                if directory_size(inbox) >= INBOX_BUDGET:
                    raise RuntimeError('CPU inbox full; refusing to hide lost coverage')
                publish_loss(store, manifest, 'Decoder backlog exceeded eight sealed segments')
                state['dropped_segments'] += 1
            state['backlog_segments'] = min(len(pending), MAX_PENDING)
            active = directory / 'capture.data'
            try:
                active_bytes = active.stat().st_size
            except FileNotFoundError:
                active_bytes = 0
            if active_bytes >= 192 * 1024 * 1024:
                raise RuntimeError('Active perf segment approached its 256MiB hard limit')
            if time.monotonic() - prune_at >= 10:
                if prune_raw(raw) >= RAW_BUDGET:
                    raise RuntimeError('Raw capture budget exhausted')
                prune_at = time.monotonic()
                seen.intersection_update(p.name for p in sealed_files(directory, open_inodes(process.pid)))
            try:
                done = json.loads((store / 'cpu-decoder-status.json').read_text())
                if done['session'] == session:
                    state['published_through_mono_us'] = done['published_through_mono_us']
            except FileNotFoundError:
                pass
            state['updated_ms'] = int(time.time() * 1000)
            atomic_json(store / 'cpu-status.json', state)
            if args.once and sequence:
                break
            time.sleep(0.5)
    except Exception as error:
        state.update(state='error', last_error=str(error)[:512])
        raise
    finally:
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            state.update(state='error', last_error='perf failed to seal before timeout')
        error_file.close()
        seal(set())
        unfinished = directory / 'capture.data'
        if unfinished.exists():
            sequence += 1
            manifest = dict(key=uuid.uuid4().hex, raw=str(unfinished), symbol_dir=str(symbols), state='lost',
                            capture={**base, 'sequence': sequence, 'start_mono_us': previous,
                                     'end_mono_us': monotonic_us(), 'uncertainty_us': monotonic_us() - previous})
            publish_loss(store, manifest, 'perf exited without sealing its active segment')
        stop_decoder.set()
        worker.join(timeout=60)
        if worker.is_alive():
            os.killpg(worker.pid, signal.SIGTERM)
            worker.join(timeout=5)
            state.update(state='error', last_error='decoder drain timed out; queued segments retained')
        if state['state'] != 'error':
            state['state'] = 'stopped'
        state['updated_ms'] = int(time.time() * 1000)
        atomic_json(store / 'cpu-status.json', state)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--store', type=Path, required=True)
    parser.add_argument('--pid', type=int)
    parser.add_argument('--node-service', default='zakurad')
    parser.add_argument('--executable', type=Path, required=True)
    parser.add_argument('--stack-bytes', type=int, choices=(8192, 16384, 32768, 65528), default=8192)
    parser.add_argument('--frequency', type=int, choices=(19, 49, 99, 999), default=999)
    parser.add_argument('--duration-seconds', type=int, default=0, help='Zero records continuously.')
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--check-symbol-budget', action='store_true')
    args = parser.parse_args()
    if not 0 <= args.duration_seconds <= 7 * 86400:
        parser.error('duration must be zero (continuous) or at most seven days')
    os.umask(0o077)
    if args.check_symbol_budget:
        executable = args.executable.resolve(strict=True)
        digest = sha256_file(executable)
        symbol_cache(args.store.resolve(), executable, digest)
        return
    stopping = [False]
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda _s, _f: stopping.__setitem__(0, True))
    try:
        capture(args, stopping)
    except Exception as error:
        status_path = args.store / 'cpu-status.json'
        try:
            prior = json.loads(status_path.read_text()) if status_path.exists() else {}
            prior.update(state='error', last_error=str(error)[:512], updated_ms=int(time.time() * 1000))
            atomic_json(status_path, prior)
        except OSError:
            pass
        raise


if __name__ == '__main__':
    main()
