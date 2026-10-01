#!/usr/bin/env python3
"""Build and test a pinned candidate on an isolated native Apple Silicon host."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import resource
import signal
import subprocess
import time

RECIPE = Path(__file__).resolve().parent
UPSTREAM = '05409775adc5f87a3aae12184486301f70ca519d'
EXACT_RESULT = re.compile(r'test result: ok\. 1 passed; 0 failed; 0 ignored;')
CONTAINMENT = [
    'zakura::transport::pipe::tests::supervised_pipe_runs_teardown_on_panic',
    'zakura::transport::pipe::tests::supervised_peer_task_runs_teardown_and_disconnect_on_panic',
]


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def git(path, *args):
    return subprocess.check_output(['git', '-C', str(path), *args], text=True,
                                   timeout=30).strip()


def qualifies(exit_code, output, exact=False):
    return exit_code == 0 and (not exact or EXACT_RESULT.search(output) is not None)


def verify_backend_patch(backend, accepted):
    """Reject tracked changes outside the accepted patch, including staged changes."""
    patch = subprocess.check_output(['git', '-C', str(backend), 'diff', 'HEAD', '--'],
                                    timeout=30)
    if patch != Path(accepted).read_bytes():
        raise ValueError('backend patch differs from accepted profile')


def qualify(backend, source, output):
    backend, source, output = [Path(p).resolve() for p in (backend, source, output)]
    if (platform.system(), platform.machine()) != ('Darwin', 'arm64'):
        raise ValueError('native Apple Silicon build host required')
    if subprocess.run(['pgrep', '-x', 'zakurad'], capture_output=True,
                      timeout=30).returncode != 1:
        raise ValueError('build host must have no running zakurad')
    manifest = json.loads((RECIPE.parent / 'corpus.json').read_text())
    if git(source, 'rev-parse', 'HEAD') != manifest['source_sha'] or git(source, 'status', '--porcelain'):
        raise ValueError('clean pinned consensus source required')
    if git(backend, 'rev-parse', 'HEAD') != UPSTREAM:
        raise ValueError('unexpected backend revision')
    # Verify the exact accepted patch, not merely its presence in a checkout.
    verify_backend_patch(backend, RECIPE / 'macos-unwind.patch')
    if output == source or source in output.parents or output == backend or backend in output.parents:
        raise ValueError('output must be a fresh directory outside source checkouts')
    output.mkdir(parents=True, exist_ok=False)
    output.chmod(0o700)
    env = dict(os.environ)
    for key in ['CARGO_ENCODED_RUSTFLAGS', 'CARGO_BUILD_TARGET', 'RUSTC',
                'RUSTC_WRAPPER', 'RUSTUP_TOOLCHAIN', 'ROCKSDB_LIB_DIR',
                'ROCKSDB_INCLUDE_DIR', 'ROCKSDB_STATIC']:
        env.pop(key, None)
    env.update(CARGO_BUILD_JOBS='1', CARGO_TERM_COLOR='never',
               CARGO_TARGET_DIR=str(output / 'target'), CARGO_PROFILE_RELEASE_LTO='false',
               RUSTFLAGS='-Cpanic=unwind -Clink-arg=-Wl,-ld_classic')
    cargo = str(backend / 'dist/cargo-clif')
    receipt = dict(source_sha=manifest['source_sha'], cargo_lock_sha256=digest(source / 'Cargo.lock'),
                   patch_sha256=digest(RECIPE / 'macos-unwind.patch'),
                   backend_sha256=digest(backend / 'dist/lib/librustc_codegen_cranelift.dylib'),
                   started=time.time(), passed=False, production_ready=False, checks=[])
    receipt['toolchain'] = subprocess.check_output(
        ['rustc', '+nightly-2026-09-30', '-Vv'], text=True, timeout=30).strip()
    receipt['sdk_version'] = subprocess.check_output(
        ['xcrun', '--show-sdk-version'], text=True, timeout=30).strip()
    receipt['configuration'] = dict(panic='unwind', lto=False, build_jobs=1,
                                    linker='apple-classic', standard_library='cranelift-static')

    def save():
        temporary = output / 'receipt.tmp'
        temporary.write_text(json.dumps(receipt, indent=2) + '\n')
        temporary.chmod(0o600)
        temporary.replace(output / 'receipt.json')

    def run(name, args, cwd, exact=False, expected_exit=0, timeout=4 * 3600):
        receipt['phase'] = name
        save()
        log = output / (name + '.log')
        start = time.time()
        with log.open('w') as stream:
            result = subprocess.run(args, cwd=cwd, env=env, stdout=stream,
                                    stderr=subprocess.STDOUT, timeout=timeout,
                                    preexec_fn=lambda: resource.setrlimit(resource.RLIMIT_CORE, (0, 0)))
        passed = (result.returncode == expected_exit if expected_exit else
                  qualifies(result.returncode, log.read_text(), exact))
        receipt['checks'].append(dict(name=name, passed=passed,
                                      exit_code=result.returncode,
                                      duration_seconds=time.time() - start,
                                      log_sha256=digest(log)))
        save()
        if not passed:
            raise RuntimeError('acceptance failed; inspect private log: ' + name)

    for optimization in ['0', '2']:
        for probe in ['unwind_probe', 'unwind_extended']:
            name = probe + '-' + optimization
            executable = output / name
            run(name + '-build', [str(backend / 'dist/rustc-clif'),
                str(RECIPE / 'probes' / (probe + '.rs')), '-Cpanic=unwind',
                '-Copt-level=' + optimization, '-Clink-arg=-Wl,-ld_classic',
                '-o', str(executable)], RECIPE, timeout=120)
            run(name, [str(executable)], output, timeout=30)
            if probe == 'unwind_extended':
                run(name + '-double-panic', [str(executable), 'double-panic'], output,
                    expected_exit=-signal.SIGABRT, timeout=30)
    run('async-probe-build', [cargo, 'build', '--manifest-path',
        str(RECIPE / 'probes/async/Cargo.toml'), '--locked', '--release'], RECIPE)
    run('async-probe', [str(output / 'target/release/cranelift-async-unwind-probe')],
        output, timeout=30)
    run('native-node-build', [cargo, 'build', '--locked', '--release', '-p',
                             'zakura', '--bin', 'zakurad'], source)
    for package, cases in [('zakura-consensus', manifest['tests']), ('zakura-network', CONTAINMENT)]:
        for index, case in enumerate(cases):
            run(package + '-' + str(index), [cargo, 'test', '-p', package, '--lib',
                '--locked', '--release', case, '--', '--exact', '--nocapture'], source, exact=True)
    binary = output / 'target/release/zakurad'
    architecture = subprocess.check_output(['file', '-b', str(binary)], text=True, timeout=30).strip()
    if 'arm64' not in architecture:
        raise ValueError('candidate is not native ARM64')
    receipt.update(binary_sha256=digest(binary), binary_architecture=architecture,
                   passed=True, phase='awaiting-runtime-qualification', completed=time.time())
    save()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for argument in ['backend', 'source', 'output']:
        parser.add_argument('--' + argument, required=True)
    args = parser.parse_args()
    qualify(args.backend, args.source, args.output)
