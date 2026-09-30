#!/usr/bin/env python3
"""Prepare a fresh static standard-library source without modifying rustup's copy."""
import argparse
from pathlib import Path
import platform
import shutil
import subprocess

UPSTREAM = '05409775adc5f87a3aae12184486301f70ca519d'
TOOLCHAIN = 'nightly-2026-09-30'


def prepare(backend):
    backend = Path(backend).resolve()
    if (platform.system(), platform.machine()) != ('Darwin', 'arm64'):
        raise ValueError('native Apple Silicon build host required')
    head = subprocess.check_output(['git', '-C', str(backend), 'rev-parse', 'HEAD'], text=True).strip()
    if head != UPSTREAM:
        raise ValueError('unexpected backend source revision')
    sysroot = Path(subprocess.check_output(
        ['rustc', '+' + TOOLCHAIN, '--print', 'sysroot'], text=True).strip())
    source = sysroot / 'lib/rustlib/src/rust/library'
    target = backend / 'build/stdlib'
    target.mkdir(parents=True, exist_ok=False)
    shutil.copytree(source, target / 'library')
    subprocess.run(['git', 'init', '-q'], cwd=target, check=True, timeout=30)
    patches = sorted(p for p in (backend / 'patches').glob('*.patch')
                     if p.name.partition('-')[2].startswith('stdlib'))
    for patch in patches:
        subprocess.run(['git', 'apply', str(patch)], cwd=target, check=True, timeout=30)
    manifest = target / 'library/std/Cargo.toml'
    contents = manifest.read_text()
    old = 'crate-type = ["dylib", "rlib"]'
    if contents.count(old) != 1:
        raise ValueError('unexpected pinned standard-library manifest')
    manifest.write_text(contents.replace(old, 'crate-type = ["rlib"]'))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--backend', required=True)
    prepare(parser.parse_args().backend)
