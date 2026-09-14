"""Shared bounded CSV decoder for trace validation and benchmark analysis."""

from contextlib import contextmanager
import csv
from dataclasses import dataclass
from datetime import datetime
import io
import json
import math
import os
from pathlib import Path
import stat
import time


SCHEMA_PATH = Path(__file__).parent.parent / "crates/zakura-jsonl-trace/schema.json"
if not SCHEMA_PATH.is_file():
    SCHEMA_PATH = Path(__file__).with_name("schema.json")
SCHEMA = json.loads(SCHEMA_PATH.read_text())
HEADERS = {table: tuple(SCHEMA["envelope"] + columns + ["extra"])
           for table, columns in SCHEMA["tables"].items()}
INTEGER_FIELDS = set(SCHEMA["integer_fields"])
BOOLEAN_FIELDS = set(SCHEMA["boolean_fields"])
JSON_FIELDS = set(SCHEMA["json_fields"])
TEXT_FIELDS = set().union(*HEADERS.values()) - INTEGER_FIELDS - BOOLEAN_FIELDS - JSON_FIELDS - {"extra"}
MAX_FIELD_BYTES = 64 * 1024
MAX_RECORD_BYTES = 1024 * 1024
MAX_JSON_DEPTH = 64
csv.field_size_limit(MAX_FIELD_BYTES)


class TraceInputError(ValueError):
    """The capture cannot supply trustworthy evidence."""


@dataclass
class Budget:
    bytes_left: int = 256 * 1024 * 1024
    rows_left: int = 500_000
    files_left: int = 256
    entries_left: int = 4096

    def file(self, size):
        if size > self.bytes_left or self.files_left <= 0:
            raise TraceInputError("trace byte/file budget exceeded")
        self.bytes_left -= size
        self.files_left -= 1

    def row(self):
        if self.rows_left <= 0:
            raise TraceInputError("trace row budget exceeded")
        self.rows_left -= 1


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise TraceInputError("duplicate JSON key")
        result[key] = value
    return result


def finite_float(text):
    value = float(text)
    if not math.isfinite(value):
        raise TraceInputError("nonfinite JSON number")
    return value


def invalid_constant(_):
    raise TraceInputError("nonfinite JSON number")


def load_json(text):
    depth = 0
    quoted = escaped = False
    for char in text:
        if escaped:
            escaped = False
        elif quoted and char == "\\":
            escaped = True
        elif char == '"':
            quoted = not quoted
        elif not quoted:
            if char in "[{":
                depth += 1
                if depth > MAX_JSON_DEPTH:
                    raise TraceInputError("JSON nesting budget exceeded")
            elif char in "]}":
                depth -= 1
    try:
        return json.loads(text, object_pairs_hook=unique_object, parse_float=finite_float, parse_constant=invalid_constant)
    except (ValueError, RecursionError) as error:
        raise TraceInputError("invalid or ambiguous JSON field") from error


@contextmanager
def regular_file(path):
    """Resolve every component through descriptors without following symlinks."""
    path = Path(os.path.abspath(path))
    if os.name != "posix":
        raise TraceInputError("secure trace import requires POSIX file descriptors")
    parent = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts[1:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
            os.close(parent)
            parent = child
        fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
    finally:
        os.close(parent)
    with os.fdopen(fd, "rb") as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise TraceInputError("trace entry must be a regular file")
        yield handle


@contextmanager
def locked_directory(path):
    """Read one snapshot while the node holds no directory writer lock."""
    import fcntl
    lock = Path(path) / ".trace.lock"
    if not os.path.lexists(lock):
        yield
        return
    with regular_file(lock) as handle:
        deadline = time.monotonic() + 2
        while True:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TraceInputError("trace snapshot lock timed out")
                time.sleep(0.01)
        yield


def entries(path, budget=None):
    if Path(path).is_symlink():
        raise TraceInputError("trace directory must not be a symlink")
    result = []
    with os.scandir(path) as iterator:
        for entry in iterator:
            if budget is not None:
                budget.entries_left -= 1
                if budget.entries_left < 0:
                    raise TraceInputError("trace discovery budget exceeded")
            if len(result) >= 1024:
                raise TraceInputError("trace directory entry budget exceeded")
            result.append(Path(entry.path))
    return sorted(result)


def segments(path, budget=None):
    path = Path(path)
    retained = []
    for item in entries(path.parent, budget):
        suffix = item.name.removeprefix(path.name + ".")
        if item.name.startswith(path.name + ".") and suffix.isascii() and suffix.isdigit():
            if len(suffix) > 6:
                raise TraceInputError("invalid trace segment number")
            retained.append((int(suffix), item))
    retained.sort(reverse=True)
    result = [item for _, item in retained]
    if os.path.lexists(path):
        result.append(path)
    return result


def validate_header(table, header):
    if table not in HEADERS or tuple(header or ()) != HEADERS[table]:
        raise TraceInputError(f"CSV header does not match the {table} schema")
    return HEADERS[table]


def validate_row(row):
    if not isinstance(row.get("event"), str) or not row["event"]:
        raise TraceInputError("trace event must be a nonempty string")
    for name in INTEGER_FIELDS:
        if name in row and (type(row[name]) is not int or not 0 <= row[name] <= 2**64 - 1):
            raise TraceInputError(f"{name} must be an unsigned 64-bit integer")
    for name in BOOLEAN_FIELDS:
        if name in row and type(row[name]) is not bool:
            raise TraceInputError(f"{name} must be a boolean")
    # Extra fields can carry fields declared by another table. Validate them too.
    for name in TEXT_FIELDS:
        if name in row and not isinstance(row[name], str):
            raise TraceInputError(f"{name} must be a string")
    if row.get("trace_version") != 2:
        raise TraceInputError("trace requires version 2 process-wide clocks")
    if not row.get("process_trace_id") or not row.get("node") or "ts" not in row:
        raise TraceInputError("trace envelope is incomplete")
    wall = row.get("wall_ts", "")
    try:
        timestamp = datetime.fromisoformat(wall.replace("Z", "+00:00"))
        if timestamp.utcoffset() is None:
            raise ValueError("timezone missing")
    except ValueError as error:
        raise TraceInputError("wall_ts must be an absolute RFC 3339 timestamp") from error
    for field in SCHEMA["required_event_fields"].get(row["event"], []):
        if field not in row:
            raise TraceInputError(f"{row['event']} requires {field}")
    return row


def read_segment(path, table, budget):
    try:
        with regular_file(path) as raw:
            size = os.fstat(raw.fileno()).st_size
            budget.file(size)
            # The descriptor's initial size bounds growth by a noncooperating writer.
            data = raw.read(size + 1)
            if len(data) != size:
                raise TraceInputError("trace changed while reading")
        reader = csv.DictReader(io.StringIO(data.decode("utf-8"), newline=""), strict=True)
        header = validate_header(table, reader.fieldnames)
        for record in reader:
            budget.row()
            if None in record or None in record.values():
                raise TraceInputError("CSV row field count does not match header")
            if sum(len(value.encode("utf-8")) for value in record.values()) > MAX_RECORD_BYTES:
                raise TraceInputError("trace record budget exceeded")
            row = {}
            extra = {}
            for name, value in record.items():
                if not value:
                    continue
                if name == "extra":
                    extra = load_json(value)
                else:
                    row[name] = load_json(value) if name in INTEGER_FIELDS | BOOLEAN_FIELDS | JSON_FIELDS else value
            if not isinstance(extra, dict) or set(header).intersection(extra):
                raise TraceInputError("CSV extra must be an object with no declared columns")
            row.update(extra)
            yield validate_row(row)
    except (OSError, UnicodeError, csv.Error) as error:
        raise TraceInputError(f"cannot decode trace segment {Path(path).name}: {type(error).__name__}") from error


def read_table(path, budget=None):
    path = Path(path)
    table = path.name.split(".csv", 1)[0]
    budget = budget if budget is not None else Budget()
    for segment in segments(path, budget):
        yield from read_segment(segment, table, budget)


def read_status(path, budget):
    with regular_file(path) as handle:
        size = os.fstat(handle.fileno()).st_size
        if size > MAX_FIELD_BYTES:
            raise TraceInputError("capture status budget exceeded")
        budget.file(size)
        return load_json(handle.read(size + 1).decode("utf-8"))
