#!/usr/bin/env python3
"""Report memory pressure from one closed native sync recording."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path

from native_sync_pressure import MAX_SAMPLE_BYTES, MemoryPressure


def report_recording(path, start_utc_ns, end_utc_ns, *, allow_incomplete=False):
    """Read through the gzip footer, including rows outside the selected phase."""
    if not 0 <= start_utc_ns <= end_utc_ns:
        raise ValueError("invalid workload interval")
    pressure = MemoryPressure()
    whole_recording = MemoryPressure()
    last = None
    rows = 0
    unit_name = None
    with path.open("rb") as source:
        before = os.fstat(source.fileno())
        with gzip.GzipFile(fileobj=source) as stream:
            while line := stream.readline(MAX_SAMPLE_BYTES + 1):
                if len(line) > MAX_SAMPLE_BYTES:
                    raise ValueError("sample exceeds the recording row limit")
                row = json.loads(line)
                if (type(row.get("schema")) is not int or row["schema"] != 1
                        or row.get("clock") != "monotonic_ns"
                        or type(row.get("utc_ns")) is not int):
                    raise ValueError("unsupported recording schema or clock")
                name = row.get("unit_name")
                if rows and name != unit_name:
                    raise ValueError("recording mixes different unit names")
                unit_name = name
                rows += 1
                if (row["unit"].get("MainPID", "0") != "0"
                        and row["unit"].get("ControlGroup")):
                    whole_recording.add(row)
                    if start_utc_ns <= row["utc_ns"] <= end_utc_ns:
                        pressure.add(row)
                last = row
        source.seek(0)
        checksum = hashlib.file_digest(source, "sha256").hexdigest()
        after = path.stat()
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
                after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
            raise ValueError("recording changed while being read")
    unit = last.get("unit", {}) if last else {}
    complete = unit.get("ActiveState") in ("inactive", "failed")
    success = None
    if all(key in unit for key in ("ActiveState", "ExecMainStatus", "Result")):
        success = (unit["ActiveState"] == "inactive" and unit["ExecMainStatus"] == "0"
                   and unit["Result"] == "success")
    if not complete and not allow_incomplete:
        raise ValueError("recording does not end with a stopped node unit")
    return {
        "schema_version": 1,
        "samples_sha256": checksum,
        "recording_complete": complete,
        "unit_success_observed": success,
        "recorded_rows": rows,
        "unit_name": unit_name,
        "phase_start_unix_ns": start_utc_ns,
        "phase_end_unix_ns": end_utc_ns,
        "scope": "Selected workload phase, adjacent same-host sample bounds. "
                 "Missing counters are unknown. Complete recording does not establish sync success.",
        "whole_recording": {
            "scope": "All recorded active-node samples, including startup and drain/shutdown. "
                     "This interval differs from the selected workload phase and ends at the last successful read.",
            **whole_recording.report(),
        },
        **pressure.report(),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("recording", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--start-utc-ns", type=int, required=True)
    parser.add_argument("--end-utc-ns", type=int, required=True)
    parser.add_argument("--allow-incomplete", action="store_true",
                        help="Write a diagnostic report with recording_complete=false.")
    args = parser.parse_args()
    result = report_recording(args.recording, args.start_utc_ns, args.end_utc_ns,
                              allow_incomplete=args.allow_incomplete)
    with args.output.open("x") as output:
        json.dump(result, output, indent=2)
        output.write("\n")


if __name__ == "__main__":
    main()
