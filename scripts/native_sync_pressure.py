"""Streaming memory-pressure analysis for native node recordings.

PSI totals are microseconds. Sample timestamps bracket several reads, so elapsed
time and pressure percentages are intervals, not exact instantaneous values.
Missing counters, restarts and long sampling gaps cannot establish zero pressure.
"""
from collections import Counter

MAX_SAMPLE_BYTES = 4 * 1024 * 1024


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


class PressureIntervals:
    """Accumulate adjacent observations without retaining the recording."""

    def __init__(self):
        self.intervals = 0
        self.excluded = Counter()
        self.stall_us = 0
        self.minimum_ns = 0
        self.maximum_ns = 0
        self.maximum_percent_upper = None

    def add(self, before, after, scope, kind):
        if scope == "cgroup" and (not before["unit_identity"]
                                  or before["unit_identity"] != after["unit_identity"]):
            self.excluded["unit_changed"] += 1
            return
        if before["boot_id"] != after["boot_id"]:
            self.excluded["host_changed"] += 1
            return
        first, last = before[scope][kind], after[scope][kind]
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
        self.samples = 0
        self.memory_samples = 0
        self.minimum_available = None
        self.peak_cached = None
        self.pressure = {scope: {kind: PressureIntervals() for kind in ("some", "full")}
                         for scope in ("host", "cgroup")}

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
        current = {"start": row["sample_start_ns"], "end": row["sample_end_ns"],
                   "unit_identity": identity, "boot_id": row.get("boot_id")}
        for scope, field in (("host", "pressure/memory"), ("cgroup", "memory.pressure")):
            current[scope] = {kind: pressure_total(row.get(scope, {}).get(field), kind)
                              for kind in ("some", "full")}
        if self.previous is not None:
            for scope, counters in self.pressure.items():
                for kind, counter in counters.items():
                    counter.add(self.previous, current, scope, kind)
        self.previous = current

    def report(self):
        return {
            "samples": self.samples,
            "host_memory_samples": self.memory_samples,
            "host_minimum_available_percent": self.minimum_available,
            "host_peak_cached_bytes": self.peak_cached,
            "memory_pressure": {scope: {kind: counter.report() for kind, counter in counters.items()}
                                for scope, counters in self.pressure.items()},
        }
