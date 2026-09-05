"""Reachability closure only. No timing, queues, or adaptive routing."""

from collections import deque
import math
import random


def closure(suppliers, source, k, n, regenerate, details=False):
    count = len(suppliers)
    subscribers = [[[] for _ in range(n)] for _ in range(count)]
    for receiver, parts in enumerate(suppliers):
        for i, peers in enumerate(parts):
            for peer in peers:
                subscribers[peer][i].append(receiver)
    have = [set() for _ in range(count)]
    have[source] = set(range(n))
    queue = deque((source, i) for i in range(n))
    sends = 0
    while queue:
        peer, i = queue.popleft()
        for receiver in subscribers[peer][i]:
            sends += 1
            if i in have[receiver]:
                continue
            have[receiver].add(i)
            queue.append((receiver, i))
            if regenerate and len(have[receiver]) == k:
                for missing in set(range(n)) - have[receiver]:
                    queue.append((receiver, missing))
                have[receiver] = set(range(n))
    return have if details else (sum(len(parts) >= k for parts in have), sends)


def header_tree_repair(suppliers, peers, source, k, n):
    """Idealized first-header tree: equal header delays, no failed or dishonest peers."""
    parents = {source: None}
    queue = deque([source])
    while queue:
        parent = queue.popleft()
        for child in sorted(peers[parent]):
            if child not in parents:
                parents[child] = parent
                queue.append(child)
    have = closure(suppliers, source, k, n, True, details=True)
    repaired = [[set(ps) for ps in parts] for parts in suppliers]
    additions = 0
    for node in range(len(suppliers)):
        deficit = max(0, k - len(have[node]))
        for i in sorted(set(range(n)) - have[node])[:deficit]:
            repaired[node][i].add(parents[node])
            additions += 1
    completed, sends = closure(repaired, source, k, n, True)
    return completed, sends, additions


def experiment(seeds=40):
    rows = []
    for count in (16, 64):
        for degree in (4, 8):
            for ratio in (.125, .25, .5, 1):
                for copies in (1, 2):
                    for seed in range(seeds):
                        rng = random.Random(seed)
                        # A ring guarantees physical connectivity before random edges.
                        peers = [{(i - 1) % count, (i + 1) % count} for i in range(count)]
                        for i in range(count):
                            while len(peers[i]) < degree:
                                j = rng.choice([j for j in range(count) if j != i and j not in peers[i]])
                                peers[i].add(j)
                                peers[j].add(i)
                        k = 32
                        n = k + math.ceil(k * ratio)
                        suppliers = [[set(rng.sample(sorted(peers[i]), copies)) for _ in range(n)]
                                     for i in range(count)]
                        source = rng.randrange(count)
                        for regenerate in (False, True):
                            completed, sends = closure(suppliers, source, k, n, regenerate)
                            rows.append(dict(nodes=count, minimum_degree=degree, parity=ratio,
                                             copies=copies, seed=seed, regenerate=regenerate,
                                             repair="none", completed=completed, sends=sends))
                        completed, sends, additions = header_tree_repair(suppliers, peers, source, k, n)
                        rows.append(dict(nodes=count, minimum_degree=degree, parity=ratio,
                                         copies=copies, seed=seed, regenerate=True, repair="header_tree",
                                         completed=completed, sends=sends, repair_grants=additions))
    return rows
