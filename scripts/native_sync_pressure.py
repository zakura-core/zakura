"""Streaming resource-pressure analysis for native node recordings.

PSI totals are microseconds. Sample timestamps bracket several reads, so elapsed
time and pressure percentages are intervals, not exact instantaneous values.
Missing counters, restarts and long sampling gaps cannot establish zero pressure.
"""
from collections import Counter

MAX_SAMPLE_BYTES = 4 * 1024 * 1024
PRESSURE_FIELDS = {
    "memory": {"host": "pressure/memory", "cgroup": "memory.pressure"},
    "cpu": {"host": "pressure/cpu"},
    "io": {"host": "pressure/io"},
}


def pressure_kinds(resource):
    """System-wide CPU full pressure is undefined; do not interpret its zero."""
    return ("some",) if resource == "cpu" else ("some", "full")


def memory_fields(text):
    """Return the byte-valued fields in a Linux meminfo sample."""
    fields = {}
    if isinstance(text, str):
        for line in text.splitlines():
            parts = line.split()
            if len(parts) == 3 and parts[2] == "kB" and parts[1].isdigit():
                fields[parts[0].rstrip(":")] = int(parts[1]) * 1024
    return fields


def pressure_total(text, kind):
    """Return one valid cumulative PSI total, or None if it is unavailable."""
    totals = []
    if isinstance(text, str):
        for line in text.splitlines():
            parts = line.split()
            if parts and parts[0] == kind:
                totals.extend(part.removeprefix("total=") for part in parts[1:]
                              if part.startswith("total="))
    return int(totals[0]) if len(totals) == 1 and totals[0].isdigit() else None


def unsigned_counters(text):
    """Preserve missing reads; reject malformed or duplicate cgroup counters."""
    if not isinstance(text, str):
        return None
    result = {}
    for line in text.splitlines():
        fields = line.split()
        if len(fields) != 2 or not fields[1].isdigit() or fields[0] in result:
            raise ValueError("invalid cgroup counter row")
        result[fields[0]] = int(fields[1])
    return result or None


class MemoryValues:
    """Summarize scalar reads without treating unlimited or missing as zero."""

    def __init__(self):
        self.numeric_samples = 0
        self.unlimited_samples = 0
        self.unavailable_samples = 0
        self.minimum = None
        self.maximum = None

    def add(self, value, *, allow_unlimited=False):
        if not isinstance(value, str):
            self.unavailable_samples += 1
            return None
        value = value.strip()
        if allow_unlimited and value == "max":
            self.unlimited_samples += 1
            return None
        if not value.isdigit():
            raise ValueError("invalid cgroup memory value")
        value = int(value)
        self.numeric_samples += 1
        self.minimum = value if self.minimum is None else min(self.minimum, value)
        self.maximum = value if self.maximum is None else max(self.maximum, value)
        return value

    def report(self):
        return {"numeric_samples": self.numeric_samples,
                "unlimited_samples": self.unlimited_samples,
                "unavailable_samples": self.unavailable_samples,
                "minimum_bytes": self.minimum, "maximum_bytes": self.maximum}


class MemoryFootprint:
    """Keep peak compositions and event changes without inferring reclaimability."""

    EVENTS = ("low", "high", "max", "oom", "oom_kill", "oom_group_kill")

    def __init__(self):
        self.values = {name: MemoryValues() for name in
                       ("memory.current", "memory.peak", "memory.high", "memory.max", "memory.swap.current")}
        self.peak_usage = None
        self.peak_anon = None
        self.valid_usage_samples = 0
        self.missing_stat_samples = 0
        self.previous = None
        self.changes = {name: 0 for name in self.EVENTS}
        self.intervals = {name: 0 for name in self.EVENTS}
        self.excluded = {name: Counter() for name in self.EVENTS}
        self.event_observations = {name: {"numeric_samples": 0, "unavailable_samples": 0,
                                          "maximum_count": None} for name in self.EVENTS}

    def add(self, row, identity):
        group = row.get("cgroup", {})
        stat = unsigned_counters(group.get("memory.stat"))
        usage = self.values["memory.current"].add(group.get("memory.current"))
        for name in ("memory.peak", "memory.high", "memory.max", "memory.swap.current"):
            self.values[name].add(group.get(name), allow_unlimited=name in ("memory.high", "memory.max"))
        sample = {"utc_ns": row.get("utc_ns"), "sample_start_ns": row["sample_start_ns"],
                  "sample_end_ns": row["sample_end_ns"], "unit_identity": identity,
                  "boot_id": row.get("boot_id"), "memory_current_bytes": usage,
                  "memory_max": group.get("memory.max"), "memory_high": group.get("memory.high"),
                  "memory_stat": stat}
        if usage is not None:
            self.valid_usage_samples += 1
            if self.peak_usage is None or usage > self.peak_usage["memory_current_bytes"]:
                self.peak_usage = sample
        if stat is None:
            self.missing_stat_samples += 1
        elif "anon" in stat and (self.peak_anon is None
                                  or stat["anon"] > self.peak_anon["memory_stat"]["anon"]):
            self.peak_anon = sample
        events = unsigned_counters(group.get("memory.events")) or {}
        for name, observation in self.event_observations.items():
            if name in events:
                observation["numeric_samples"] += 1
                observation["maximum_count"] = max(observation["maximum_count"] or 0, events[name])
            else:
                observation["unavailable_samples"] += 1
        if self.previous is not None:
            before, old_events = self.previous
            for name in self.EVENTS:
                reason = None
                if not identity or before["unit_identity"] != identity:
                    reason = "unit_changed"
                elif before["boot_id"] != sample["boot_id"]:
                    reason = "host_changed"
                elif (sample["sample_start_ns"] <= before["sample_end_ns"]
                      or before["sample_end_ns"] < before["sample_start_ns"]
                      or sample["sample_end_ns"] < sample["sample_start_ns"]
                      or sample["sample_end_ns"] - before["sample_start_ns"] > 10_000_000_000):
                    reason = "clock_or_sampling_gap"
                elif name not in events or name not in old_events:
                    reason = "missing_counter"
                elif events[name] < old_events[name]:
                    reason = "counter_reset"
                if reason:
                    self.excluded[name][reason] += 1
                else:
                    self.changes[name] += events[name] - old_events[name]
                    self.intervals[name] += 1
        self.previous = sample, events

    def report(self):
        return {"valid_usage_samples": self.valid_usage_samples,
                "missing_stat_samples": self.missing_stat_samples,
                "value_observations": {name: values.report() for name, values in self.values.items()},
                "peak_usage_sample": self.peak_usage, "peak_anon_sample": self.peak_anon,
                "event_observations": {name: dict(value) for name, value in self.event_observations.items()},
                "event_changes": {name: {"observed_change": self.changes[name] if self.intervals[name] else None,
                                         "valid_intervals": self.intervals[name],
                                         "excluded_intervals": dict(self.excluded[name])}
                                  for name in self.EVENTS},
                "scope": "Peak fields come from the same bracketed read, not an atomic snapshot. "
                         "Memory categories overlap; do not sum file and LRU fields. "
                         "Event changes exclude the initial count and unobserved intervals; "
                         "they are not absolute lifetime totals or a headroom pass. "
                         "Event observations retain absolute counter maxima, including initial counts; "
                         "they cannot count events after the last read or sum across counter resets."}


class PressureIntervals:
    """Accumulate adjacent observations without retaining the recording."""

    def __init__(self):
        self.intervals = 0
        self.excluded = Counter()
        self.stall_us = 0
        self.minimum_ns = 0
        self.maximum_ns = 0
        self.maximum_percent_upper = None

    def add(self, before, after, resource, scope, kind):
        if scope == "cgroup" and (not before["unit_identity"]
                                  or before["unit_identity"] != after["unit_identity"]):
            self.excluded["unit_changed"] += 1
            return
        if before["boot_id"] != after["boot_id"]:
            self.excluded["host_changed"] += 1
            return
        first, last = before[resource][scope][kind], after[resource][scope][kind]
        if first is None or last is None:
            self.excluded["missing_counter"] += 1
            return
        shortest = after["start"] - before["end"]
        longest = after["end"] - before["start"]
        if (shortest <= 0 or longest > 10_000_000_000
                or before["end"] < before["start"] or after["end"] < after["start"]):
            self.excluded["clock_or_sampling_gap"] += 1
            return
        if last < first:
            self.excluded["counter_reset"] += 1
            return
        delta = last - first
        upper = delta * 100_000 / shortest
        self.intervals += 1
        self.stall_us += delta
        self.minimum_ns += shortest
        self.maximum_ns += longest
        self.maximum_percent_upper = max(self.maximum_percent_upper or 0, upper)

    def report(self):
        return {
            "intervals": self.intervals,
            "excluded_intervals": dict(self.excluded),
            "observed_stall_us": self.stall_us if self.intervals else None,
            "maximum_interval_percent_upper": self.maximum_percent_upper,
            "minimum_observed_seconds": self.minimum_ns / 1e9,
            "maximum_observed_seconds": self.maximum_ns / 1e9,
        }


class MemoryPressure:
    """Summarize one host and unit; memory use is independent of trace length."""

    def __init__(self):
        self.previous = None
        self.footprint = MemoryFootprint()
        self.samples = 0
        self.memory_samples = 0
        self.minimum_available = None
        self.peak_cached = None
        self.pressure = {
            resource: {scope: {kind: PressureIntervals() for kind in pressure_kinds(resource)}
                       for scope in fields}
            for resource, fields in PRESSURE_FIELDS.items()
        }

    def add(self, row):
        for field in ("sample_start_ns", "sample_end_ns"):
            if type(row.get(field)) is not int or row[field] < 0:
                raise ValueError(f"invalid sample timestamp: {field}")
        self.samples += 1
        memory = memory_fields(row.get("host", {}).get("meminfo"))
        if memory.get("MemTotal", 0) > 0 and "MemAvailable" in memory:
            if memory["MemAvailable"] > memory["MemTotal"]:
                raise ValueError("available memory exceeds total memory")
            available = 100 * memory["MemAvailable"] / memory["MemTotal"]
            self.minimum_available = (available if self.minimum_available is None
                                      else min(self.minimum_available, available))
            self.memory_samples += 1
        if "Cached" in memory:
            self.peak_cached = max(self.peak_cached or 0, memory["Cached"])
        unit = row.get("unit", {})
        identity = (unit.get("MainPID"), unit.get("ControlGroup"))
        if not identity[0] or identity[0] == "0" or not identity[1]:
            identity = None
        self.footprint.add(row, identity)
        current = {"start": row["sample_start_ns"], "end": row["sample_end_ns"],
                   "unit_identity": identity, "boot_id": row.get("boot_id")}
        for resource, fields in PRESSURE_FIELDS.items():
            current[resource] = {
                scope: {kind: pressure_total(row.get(scope, {}).get(field), kind)
                        for kind in pressure_kinds(resource)}
                for scope, field in fields.items()
            }
        if self.previous is not None:
            for resource, scopes in self.pressure.items():
                for scope, counters in scopes.items():
                    for kind, counter in counters.items():
                        counter.add(self.previous, current, resource, scope, kind)
        self.previous = current

    def report(self):
        return {
            "samples": self.samples,
            "host_memory_samples": self.memory_samples,
            "host_minimum_available_percent": self.minimum_available,
            "host_peak_cached_bytes": self.peak_cached,
            "cgroup_memory": self.footprint.report(),
            "memory_pressure": {scope: {kind: counter.report() for kind, counter in counters.items()}
                                for scope, counters in self.pressure["memory"].items()},
            "host_pressure": {
                resource: {kind: counter.report() for kind, counter in self.pressure[resource]["host"].items()}
                for resource in ("cpu", "io")
            },
        }
