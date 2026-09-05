"""Packet-service model of subscription allocation; no consensus or transport model."""

from __future__ import annotations

import argparse
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
import heapq
import itertools
import json
import math
import random
import statistics


PART = 65536
WIRE = PART + 384  # Fixed experimental proof/framing allowance, not the wire profile.
MASK_BITS = 16


@dataclass(frozen=True)
class Config:
    name: str = "balanced"
    blocks: int = 80
    k: int = 32
    parity: float = 0.25
    subscribe_extra: float = 0.25
    interval_ms: float = 1200
    burst: int = 1
    receivers: int = 1
    rates: tuple = (20.0, 20.0, 20.0, 20.0)  # MiB/s, each supplier's shared egress.
    down_mib: float = 100
    control_ms: float = 20
    deadline_ms: float = 400
    fallback_ms: float = 1200
    queue_parts: int = 256
    proposer_mode: str = "alternating"
    locality: bool = False
    skew: bool = False
    change: str = "none"
    source_stall_ms: float = 0
    rho: float = 1 / 32
    challenge_width: int = 1
    min_interval_ms: float = 250
    trial_blocks: int = 12
    trial_age_ms: float = 20000
    min_votes: int = 3
    threshold: float = 2 / 3
    epsilon_ms: float = 1
    initial_budget_parts: int = 20
    beta: float = 0.75
    migration_parts: int = 4
    coverage: bool = True


@dataclass
class Packet:
    block: int
    receiver: int
    peer: int
    part: int
    recovery: bool = False


class Link:
    """Non-preemptive one-part service, round-robin across receiver/block queues."""

    def __init__(self, sim, rate, deliver, cancellable):
        self.sim, self.rate, self.deliver = sim, rate, deliver
        self.cancellable = cancellable
        self.queues = defaultdict(deque)
        self.order = deque()
        self.busy = False
        self.count = 0
        self.peak = 0
        self.dropped = 0
        self.sent = 0

    def put(self, packet):
        if self.count >= self.sim.cfg.queue_parts:
            self.dropped += 1
            return
        key = packet.receiver, packet.block
        if not self.queues[key]:
            self.order.append(key)
        self.queues[key].append(packet)
        self.count += 1
        self.peak = max(self.peak, self.count)
        self.pump()

    def pump(self):
        if self.busy:
            return
        while self.order:
            key = self.order.popleft()
            packet = self.queues[key].popleft()
            self.count -= 1
            if self.queues[key]:
                self.order.append(key)
            else:
                del self.queues[key]
            state = self.sim.states[key]
            if self.cancellable and self.sim.now >= state.cancel_at:
                self.sim.release(packet)
                continue
            self.busy = True
            duration = WIRE / (self.rate() * 1048576) * 1000
            self.sim.at(self.sim.now + duration, self.finish, packet)
            return

    def finish(self, packet):
        self.busy = False
        self.sent += WIRE
        self.deliver(packet)
        self.pump()


@dataclass
class State:
    block: int
    receiver: int
    proposer: int
    start: float
    k: int
    n: int
    selected: list
    assignments: dict
    context: tuple
    trial: tuple | None = None
    arrived: dict = field(default_factory=dict)
    unique: set = field(default_factory=set)
    sent_once: set = field(default_factory=set)
    released: set = field(default_factory=set)
    done: float = math.inf
    cancel_at: float = math.inf
    fallback: bool = False
    repaired: bool = False
    degraded: bool = False
    peak_active: int = 1
    standing: set = field(default_factory=set)


class Controller:
    def __init__(self, cfg, policy, rng):
        self.cfg, self.policy, self.rng = cfg, policy, rng
        self.routes = {}
        self.scores = defaultdict(dict)
        self.trials = {}
        self.pending = deque()
        self.last_start = -math.inf
        self.energy_initial = 2 * cfg.k * WIRE
        self.energy_cap = 4 * cfg.k * WIRE
        self.energy = self.energy_initial
        self.earned = self.charged = 0
        self.q = [0] * 4
        self.w = [cfg.initial_budget_parts * WIRE] * 4
        self.failures = [set() for _ in range(4)]
        self.last_adjust = [-math.inf] * 4
        self.starts = self.moves = self.expired = self.votes = self.ties = 0
        self.unfunded = self.blocked_moves = 0
        self.history = []

    def row(self, proposer):
        key = 0 if self.policy == "global_best" else proposer
        if key not in self.routes:
            self.routes[key] = [j % 4 for j in range(MASK_BITS)]
        return self.routes[key]

    def start_trial(self, proposer, n, now):
        if self.policy not in ("races", "budgeted"):
            return
        for p, trial in list(self.trials.items()):
            if now - trial["start"] >= self.cfg.trial_age_ms:
                del self.trials[p]
                self.expired += 1
        if proposer not in self.trials and proposer not in self.pending:
            self.pending.append(proposer)
        if not self.pending:
            return
        if now - self.last_start < self.cfg.min_interval_ms or len(self.trials) >= 2:
            return
        proposer = self.pending[0]
        row = self.row(proposer)
        mask = self.rng.randrange(MASK_BITS)
        incumbent = row[mask]
        occupied = {t["challenger"] for t in self.trials.values()}
        choices = [p for p in range(4) if p != incumbent and p not in occupied]
        if not choices:
            return
        challenger = self.rng.choice(choices)
        masks = [(mask + i) % MASK_BITS for i in range(MASK_BITS)
                 if row[(mask + i) % MASK_BITS] == incumbent][:self.cfg.challenge_width]
        # Reserve one future block of maximum response credit; renew at each observation.
        charge = math.ceil(n / MASK_BITS) * len(masks) * WIRE + 128
        if charge > self.energy or self.q[challenger] + charge > self.w[challenger]:
            self.unfunded += 1
            return
        self.pending.popleft()
        self.trials[proposer] = dict(incumbent=incumbent, challenger=challenger,
                                     masks=masks, start=now, blocks=0, wins=0, losses=0,
                                     context=None, warmed=False)
        self.starts += 1
        self.last_start = now + self.rng.uniform(0, self.cfg.min_interval_ms / 4)

    def assign(self, state, now):
        row = self.row(state.proposer)
        out = {}
        for i in state.selected:
            bit = state.assignments[i]  # The caller supplied the balanced part-mask mapping.
            if self.policy == "random":
                p = self.rng.randrange(4)
            elif self.policy in ("global_best", "passive"):
                score = self.scores[0 if self.policy == "global_best" else state.proposer]
                p = min(score, key=score.get) if len(score) == 4 else row[bit]
            else:
                p = row[bit]
            out[i] = {p}
            state.standing.add((p, i))
        if self.cfg.coverage:
            # Greedy minimum additions for the declared single-supplier-loss model.
            for failed in range(4):
                surviving = sum(bool(peers - {failed}) for peers in out.values())
                for i in state.selected:
                    if surviving >= state.k:
                        break
                    if out[i] == {failed}:
                        alternatives = [p for p in range(4) if p != failed]
                        p = min(alternatives, key=lambda p: (sum(p in x for x in out.values()), p))
                        out[i].add(p)
                        surviving += 1
        self.start_trial(state.proposer, state.n, now)
        trial = self.trials.get(state.proposer)
        if trial:
            if trial["blocks"] >= self.cfg.trial_blocks:
                del self.trials[state.proposer]
                self.expired += 1
            else:
                chosen = [i for i in state.selected if state.assignments[i] in trial["masks"]]
                extra = [i for i in chosen if trial["challenger"] not in out[i]]
                charge = len(extra) * WIRE + 128
                if charge <= self.energy and self.q[trial["challenger"]] + charge <= self.w[trial["challenger"]]:
                    self.energy -= charge
                    self.charged += charge
                    for i in chosen:
                        out[i].add(trial["challenger"])
                    state.trial = (trial, chosen, trial["warmed"])
                    trial["blocks"] += 1
                else:
                    self.unfunded += 1
        state.degraded = any(sum(bool(ps - {f}) for ps in out.values()) < state.k for f in range(4))
        return out

    def observe(self, state, now):
        cfg = self.cfg
        if not state.fallback:
            credit = math.floor(cfg.rho * state.n * WIRE)
            self.earned += credit
            self.energy = min(self.energy_cap, self.energy + credit)
        samples = defaultdict(list)
        for (p, i), t in state.arrived.items():
            if t <= state.done:
                samples[p].append(t - state.start)
        key = 0 if self.policy == "global_best" else state.proposer
        for p, values in samples.items():
            sample = statistics.mean(values)
            old = self.scores[key].get(p, sample)
            self.scores[key][p] = 0.75 * old + 0.25 * sample
        for p in range(4):
            assigned = sum(p in peers for peers in state.assignments.values())
            delivered = sum((p, i) in state.arrived and state.arrived[p, i] <= state.start + cfg.deadline_ms
                            for i, peers in state.assignments.items() if p in peers)
            if state.done > state.start + cfg.deadline_ms and assigned > delivered:
                self.failures[p].add(state.block)
            if self.policy != "budgeted" or now - self.last_adjust[p] < 250:
                continue
            if len(self.failures[p]) >= 2:
                self.w[p] = max(2 * WIRE, int(cfg.beta * self.w[p]))
                self.failures[p].clear()
                self.last_adjust[p] = now
            elif delivered * WIRE >= 0.8 * self.w[p] and state.done - state.start <= cfg.deadline_ms:
                self.w[p] = min(256 * WIRE, self.w[p] + WIRE)
                self.last_adjust[p] = now
        if state.trial:
            trial, chosen, warmed = state.trial
            a, b = trial["incumbent"], trial["challenger"]
            cutoff = min(state.done, state.start + cfg.deadline_ms)
            trial["warmed"] |= any((b, i) in state.arrived for i in chosen)
            # Drop stale trial references and observations whose concurrency changed.
            if self.trials.get(state.proposer) is not trial or not warmed:
                return
            if state.peak_active > state.context[1] or state.repaired:
                return
            if trial["context"] not in (None, state.context):
                return
            trial["context"] = state.context
            wins = losses = 0
            for i in chosen:
                ta = state.arrived.get((a, i), math.inf)
                tb = state.arrived.get((b, i), math.inf)
                wins += tb + cfg.epsilon_ms <= min(ta, cutoff)
                losses += ta + cfg.epsilon_ms <= min(tb, cutoff)
            if wins == losses:
                self.ties += 1
                return
            trial["wins"] += wins > losses
            trial["losses"] += losses > wins
            self.votes += 1
            decisive = trial["wins"] + trial["losses"]
            if decisive < cfg.min_votes:
                return
            if trial["wins"] / decisive >= cfg.threshold:
                added = len(chosen) * WIRE
                fits = (added <= cfg.migration_parts * WIRE and self.q[b] + added <= self.w[b])
                if self.policy == "races" or fits:
                    row = self.row(state.proposer)
                    for mask in trial["masks"]:
                        if row[mask] == a:
                            row[mask] = b
                    self.moves += 1
                    self.history.append((state.block, state.proposer, a, b, now))
                else:
                    self.blocked_moves += 1
            del self.trials[state.proposer]


class Simulation:
    def __init__(self, cfg, policy, seed):
        self.cfg, self.policy, self.seed = cfg, policy, seed
        self.now = 0.0
        self.events = []
        self.seq = itertools.count()
        self.states = {}
        self.controllers = [Controller(cfg, policy, random.Random(seed * 997 + r))
                            for r in range(cfg.receivers)]
        self.change_time = ((cfg.blocks // 2) // cfg.burst) * cfg.interval_ms
        self.peers = [Link(self, lambda p=p: self.rate(p), self.peer_deliver, True) for p in range(4)]
        self.down = [Link(self, lambda: cfg.down_mib, self.arrive, False) for _ in self.controllers]
        self.peak_q = 0
        self.control_bytes = 0
        self.recovery_bytes = 0
        self.late_bytes = 0
        self.proposer_counts = defaultdict(int)

    def at(self, time, fn, *args):
        if time < self.now - 1e-8:
            raise ValueError("event moved backward")
        heapq.heappush(self.events, (time, next(self.seq), fn, args))

    def rate(self, peer):
        rate = self.cfg.rates[peer]
        if self.now >= self.change_time and self.cfg.change == "bandwidth" and peer == 0:
            return rate / 16
        if self.now >= self.change_time and self.cfg.change == "recovery" and peer == 0:
            return rate * 16
        return rate

    def availability(self, block, proposer, peer, part, n, start):
        rng = random.Random((self.seed + 1) * 10000019 + block * 10103 + peer * 701 + part)
        near = proposer % 4
        if start >= self.change_time and self.cfg.change == "entry":
            near = (near + 2) % 4
        base = 4 if self.cfg.locality and peer == near else 35
        if self.cfg.skew:
            base += 140 if part % 4 == peer else 0
        if self.cfg.change == "withhold" and start >= self.change_time and peer < 2:
            return math.inf
        # Exogenous availability, not a recursively adapting overlay.
        return start + self.cfg.source_stall_ms + base + rng.uniform(0, 12) + part * 0.08

    def release(self, packet):
        state = self.states[packet.receiver, packet.block]
        key = packet.peer, packet.part
        if key not in state.released:
            state.released.add(key)
            self.controllers[packet.receiver].q[packet.peer] -= WIRE

    def send(self, state, peer, part, recovery=False):
        key = peer, part
        if key in state.sent_once:
            return
        state.sent_once.add(key)
        controller = self.controllers[state.receiver]
        controller.q[peer] += WIRE
        self.peak_q = max(self.peak_q, max(controller.q))
        ready = self.availability(state.block, state.proposer, peer, part, state.n, state.start)
        if math.isfinite(ready):
            warming = state.trial and state.trial[0]["challenger"] == peer and not state.trial[2]
            post_header = key not in state.standing
            self.at(max(ready, self.now + (self.cfg.control_ms if recovery or warming or post_header else 0)),
                    self.peers[peer].put, Packet(state.block, state.receiver, peer, part, recovery))

    def announce(self, block):
        cfg = self.cfg
        rng = random.Random(self.seed * 100003 + block)
        proposer = block % 4
        if cfg.proposer_mode == "sparse":
            proposer = 1 if block % 16 == 0 else 0
        if cfg.proposer_mode == "cold":
            proposer = block
        k = cfg.k * (4 if cfg.change == "size" and block >= cfg.blocks // 2 else 1)
        n = k + math.ceil(k * cfg.parity)
        indices = list(range(n))
        rng.shuffle(indices)
        mapping = {i: rank % MASK_BITS for rank, i in enumerate(indices)}
        selected = indices[:min(n, k + math.ceil(k * cfg.subscribe_extra))]
        self.proposer_counts[proposer] += 1
        for r, controller in enumerate(self.controllers):
            active = sum(s.receiver == r and not math.isfinite(s.done) for s in self.states.values()) + 1
            state = State(block, r, proposer, self.now, k, n, selected, mapping,
                          (int(math.log2(n)), active))
            self.states[r, block] = state
            for s in self.states.values():
                if s.receiver == r and not math.isfinite(s.done):
                    s.peak_active = max(s.peak_active, active)
            state.assignments = controller.assign(state, self.now)
            for i, peers in state.assignments.items():
                for p in sorted(peers):
                    self.send(state, p, i)
            self.control_bytes += 4 * 128
            if state.trial:
                self.control_bytes += 128
            self.at(self.now + cfg.deadline_ms, self.repair, state)
            self.at(self.now + cfg.fallback_ms, self.fallback, state)

    def peer_deliver(self, packet):
        self.down[packet.receiver].put(packet)

    def arrive(self, packet):
        state = self.states[packet.receiver, packet.block]
        self.release(packet)
        state.arrived[packet.peer, packet.part] = self.now
        if packet.recovery:
            self.recovery_bytes += WIRE
        if math.isfinite(state.done):
            self.late_bytes += WIRE
            return
        state.unique.add(packet.part)
        if len(state.unique) >= state.k:
            self.complete(state)

    def complete(self, state):
        state.done = self.now
        state.cancel_at = self.now + self.cfg.control_ms
        self.control_bytes += 4 * 64
        self.controllers[state.receiver].observe(state, self.now)
        # Retire withheld or dropped work only after the cancellation tail expires.
        self.at(state.cancel_at + self.cfg.fallback_ms, self.retire, state)

    def retire(self, state):
        for peer, part in state.sent_once:
            self.release(Packet(state.block, state.receiver, peer, part))

    def repair(self, state):
        if math.isfinite(state.done):
            return
        state.repaired = True
        reserve = 2 * state.k
        for i in range(state.n):
            if i in state.unique:
                continue
            options = [p for p in range(4) if (p, i) not in state.sent_once]
            if options and reserve:
                # No hidden readiness oracle: choose a deterministic rotating alternative.
                self.send(state, options[(i + state.block) % len(options)], i, True)
                reserve -= 1
                self.control_bytes += 128

    def fallback(self, state):
        if not math.isfinite(state.done):
            state.fallback = True
            self.complete(state)

    def run(self):
        for b in range(self.cfg.blocks):
            self.at((b // self.cfg.burst) * self.cfg.interval_ms, self.announce, b)
        while self.events:
            self.now, _, fn, args = heapq.heappop(self.events)
            fn(*args)
        return self.report()

    def report(self):
        states = list(self.states.values())
        complete = [s.done - s.start for s in states if not s.fallback]
        all_delays = [s.done - s.start for s in states]
        payload = sum(s.k * PART for s in states)
        copies = sum(len(s.arrived) for s in states)
        distinct = sum(len({i for _, i in s.arrived}) for s in states)
        after = [s for s in states if s.start >= self.change_time]
        controllers = self.controllers
        for c in controllers:
            assert 0 <= c.energy <= c.energy_cap
            assert c.charged <= c.energy_initial + c.earned
            assert all(x == 0 for x in c.q)
        return dict(scenario=self.cfg.name, policy=self.policy, seed=self.seed,
                    config=asdict(self.cfg), completed=len(complete), total=len(states),
                    p50_ms=quantile(complete, .5), p95_ms=quantile(complete, .95),
                    p99_ms=quantile(complete, .99), worst_terminal_ms=max(all_delays),
                    deadline_miss=sum(t > self.cfg.deadline_ms for t in all_delays) / len(states),
                    fallback=sum(s.fallback for s in states) / len(states),
                    degraded=sum(s.degraded for s in states) / len(states),
                    after_change_p95_ms=quantile([s.done - s.start for s in after if not s.fallback], .95),
                    wire_to_body=sum(x.sent for x in self.down) / payload,
                    supplier_wire_to_body=sum(x.sent for x in self.peers) / payload,
                    duplicate_bytes=(copies - distinct) * PART, late_bytes=self.late_bytes,
                    control_bytes=self.control_bytes, recovery_bytes=self.recovery_bytes,
                    peak_link_queue_bytes=max(x.peak for x in self.peers + self.down) * WIRE,
                    peak_assigned_bytes=self.peak_q,
                    completed_body_bytes=sum(s.k * PART for s in states if not s.fallback),
                    observation_end_ms=max(s.done for s in states),
                    degraded_block_ms=sum(min(s.done - s.start, self.cfg.fallback_ms)
                                          for s in states if s.degraded),
                    queue_drops=sum(x.dropped for x in self.peers + self.down),
                    trial_starts=sum(c.starts for c in controllers), moves=sum(c.moves for c in controllers),
                    expired=sum(c.expired for c in controllers), votes=sum(c.votes for c in controllers),
                    ties=sum(c.ties for c in controllers), unfunded=sum(c.unfunded for c in controllers),
                    blocked_moves=sum(c.blocked_moves for c in controllers),
                    exploration_charged=sum(c.charged for c in controllers),
                    exploration_bound=sum(c.energy_initial + c.earned for c in controllers),
                    final_w_parts=[x / WIRE for c in controllers for x in c.w],
                    moves_log=[h for c in controllers for h in c.history])


def quantile(values, q):
    return sorted(values)[max(0, math.ceil(q * len(values)) - 1)] if values else None


def scenarios():
    return [
        Config(),
        Config(name="locality", locality=True, rates=(80, 20, 10, 5)),
        Config(name="entry_shift", locality=True, change="entry"),
        Config(name="bandwidth_drop", locality=True, rates=(80, 20, 10, 5), change="bandwidth"),
        Config(name="peer_recovers", locality=True, rates=(2, 20, 10, 5), change="recovery"),
        Config(name="part_skew", skew=True, locality=True),
        Config(name="size_jump", change="size", rates=(20, 10, 5, 2)),
        Config(name="burst", burst=4, interval_ms=900, rates=(20, 10, 5, 2)),
        Config(name="shared_receiver", down_mib=5, interval_ms=300),
        Config(name="source_stall", source_stall_ms=500),
        Config(name="correlated_withhold", change="withhold", locality=True),
        Config(name="two_receivers", receivers=2, interval_ms=400, rates=(10, 8, 6, 4)),
        Config(name="overload", burst=4, interval_ms=150, down_mib=4, queue_parts=64),
        Config(name="sparse_proposer", proposer_mode="sparse", locality=True),
        Config(name="cold_proposers", proposer_mode="cold", locality=True),
    ]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--scenario", default="balanced")
    parser.add_argument("--policy", choices=("equal", "random", "global_best", "passive", "races", "budgeted"),
                        default="budgeted")
    args = parser.parse_args()
    cfg = next(c for c in scenarios() if c.name == args.scenario)
    print(json.dumps(Simulation(cfg, args.policy, args.seed).run(), sort_keys=True))


if __name__ == "__main__":
    main()
