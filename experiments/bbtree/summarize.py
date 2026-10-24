"""Summarize paired benchmark CSV without discarding outliers."""
import csv
import statistics
import sys
from collections import defaultdict


def main(argv=None):
    args = sys.argv[1:] if argv is None else argv
    rows = defaultdict(dict)
    with open(args[0], newline='', encoding='utf-8') as source:
        for row in csv.DictReader(source):
            key = row['suite'], int(row['size']), row['operation']
            rows[key].setdefault(row['backend'], []).append(row)
    print('# Measured comparison\n\n<!-- markdownlint-disable MD013 -->\n')
    print('Nine outer repetitions; no outliers removed. Ratio = bb/std median; '
          'below 1 is faster. CV = sample standard deviation / mean. '
          'Height times are ns/op; map times are ns/workload.\n')
    print('| Suite / size / operation | std median [min–max] | bb median [min–max] | Ratio | CV std/bb |')
    print('| --- | ---: | ---: | ---: | ---: |')
    for (suite, size, operation), backends in sorted(rows.items()):
        values = {b: [float(r['ns']) for r in backends[b]] for b in ['std', 'bb']}
        def cell(backend):
            v = values[backend]
            return f'{statistics.median(v):.1f} [{min(v):.1f}–{max(v):.1f}]'
        ratio = statistics.median(values['bb']) / statistics.median(values['std'])
        cv = [statistics.stdev(values[b]) / statistics.mean(values[b]) for b in ['std', 'bb']]
        print(f'| {suite}/{size}/{operation} | {cell("std")} | {cell("bb")} | '
              f'{ratio:.3f} | {cv[0]:.1%}/{cv[1]:.1%} |')
    print('\n## Allocation passes\n')
    print('Cumulative requested bytes, not resident/peak memory. Counts must agree '
          'across repetitions. Fixtures are excluded.\n')
    print('| Size / operation | std allocations / bytes | bb allocations / bytes |')
    print('| --- | ---: | ---: |')
    for (suite, size, operation), backends in sorted(rows.items()):
        if suite != 'map':
            continue
        cells = []
        for backend in ['std', 'bb']:
            counts = {(r['allocations'], r['requested_bytes']) for r in backends[backend]}
            assert len(counts) == 1, (size, operation, backend, counts)
            count, size_bytes = counts.pop()
            cells.append(f'{count} / {size_bytes}')
        print(f'| {size}/{operation} | {cells[0]} | {cells[1]} |')


if __name__ == '__main__':
    main()
