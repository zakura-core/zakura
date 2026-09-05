"""Validate complete application lifetimes before exporting a workload profile.

This first profile requires completed reads and returned writes. It rejects an
entire episode containing unsupported outcomes; it never filters bad requests
out of an otherwise successful workload.
"""
from collections import Counter, defaultdict
import hashlib
import json

from getblocks_capture import (
    FINISH, MESSAGE, START, IncompleteCapture, integer, read_capture_metrics,
)

FAMILIES = {
    "get_blocks_serving": "serving_events",
    "get_blocks_frame": "frame_events",
    "get_blocks_ownership": "ownership_events",
    "get_blocks_settlement": "settlement_events",
    "get_blocks_wait": "wait_events",
    "get_blocks_query": "serving_query_events",
}


def require(condition, message):
    if not condition:
        raise IncompleteCapture(message)


def phases(rows, expected):
    require([row.get("phase") for row in rows] == expected,
            f"missing, duplicate, or unsupported phases; expected {expected}")
    times = [integer(row, "ts") for row in rows]
    require(times == sorted(times), "lifecycle clock moved backwards")
    return {row["phase"]: row for row in rows}


def consistent(rows, fields):
    values = {field: integer(rows[0], field) for field in fields}
    for row in rows[1:]:
        require(all(integer(row, field) == value for field, value in values.items()),
                "lifecycle identity or accounting values changed")
    return values


def completed_request(arrival, events):
    serving = phases(events["get_blocks_serving"], [
        "input_retained", "admission_reserved", "input_consumed", "committed", "request_bound",
    ])
    request_id = integer(serving["request_bound"], "request_id")
    pending = phases(events["get_blocks_ownership"], ["release_started", "release_finished"])
    require(all(row.get("stage") == "pending" for row in pending.values()),
            "provisional rollback is unsupported by the completed profile")
    require(serving["input_consumed"]["ts"] <= pending["release_started"]["ts"]
            <= pending["release_finished"]["ts"] <= serving["committed"]["ts"],
            "pending ownership does not precede commitment")
    query = phases(events["get_blocks_query"], ["read_started", "read_finished"])
    require(all(integer(row, "request_id") == request_id for row in query.values()),
            "query belongs to another request")
    require(serving["request_bound"]["ts"] <= query["read_started"]["ts"],
            "query started before request binding")
    settlement = phases(events["get_blocks_settlement"], ["release_started", "release_finished"])
    cost = consistent(list(settlement.values()), [
        "request_id", "request_overhead", "response_cap", "transferred", "unused_response_capacity",
    ])
    require(cost["request_id"] == request_id, "settlement belongs to another request")
    require(cost["response_cap"] == cost["transferred"] + cost["unused_response_capacity"],
            "settlement does not reconcile transferred and unused capacity")
    require(query["read_finished"]["ts"] <= settlement["release_started"]["ts"],
            "request released before its read completed")

    by_sequence = defaultdict(list)
    for row in events["get_blocks_frame"]:
        by_sequence[integer(row, "frame_sequence")].append(row)
    require(sorted(by_sequence) == list(range(len(by_sequence))), "frame sequence has gaps")
    frames = []
    for sequence in range(len(by_sequence)):
        rows = by_sequence[sequence]
        frame = phases(rows, ["queued", "write_started", "write_returned", "release_started", "release_finished"])
        identity = consistent(rows, ["request_id", "payload_bytes", "message_type"])
        require(identity["request_id"] == request_id, "frame belongs to another request")
        require(0 < identity["payload_bytes"] <= cost["response_cap"], "frame exceeds its response capacity")
        require(identity["message_type"] <= 65535, "frame message type exceeds the wire field")
        require([row.get("write_state") for row in rows] == ["queued", "writing", "returned", "returned", "returned"],
                "unsupported or inconsistent frame write state")
        require(query["read_finished"]["ts"] <= frame["queued"]["ts"] <= settlement["release_started"]["ts"],
                "frame transfer lies outside the response ownership interval")
        frames.append({
            "payload_bytes": identity["payload_bytes"], "message_type": identity["message_type"],
            "queued_us": frame["queued"]["ts"], "write_started_us": frame["write_started"]["ts"],
            "write_returned_us": frame["write_returned"]["ts"],
            "release_us": [frame["release_started"]["ts"], frame["release_finished"]["ts"]],
        })
    require(sum(frame["payload_bytes"] for frame in frames) == cost["transferred"],
            "queued frame bytes do not reconcile with settlement")

    by_sequence = defaultdict(list)
    for row in events["get_blocks_wait"]:
        by_sequence[integer(row, "wait_sequence")].append(row)
    require(sorted(by_sequence) == list(range(len(by_sequence))), "wait sequence has gaps")
    waits = []
    allowed_bounds = {
        "pending_input": {"session_pending", "node_pending"},
        "admission": {"peer_rate", "node_rate", "peer_active", "node_active", "peer_outstanding", "node_outstanding"},
        "reactor_queue": {"reactor_queue"},
    }
    for sequence in range(len(by_sequence)):
        rows = by_sequence[sequence]
        wait = phases(rows, ["started", "ready"])
        stage, bound = rows[0].get("stage"), rows[0].get("initial_bound")
        require(bound in allowed_bounds.get(stage, set()), "unsupported wait bound")
        require(all(row.get("stage") == stage and row.get("initial_bound") == bound for row in rows),
                "wait identity changed")
        waits.append({"stage": stage, "initial_bound": bound, "interval_us": [wait["started"]["ts"], wait["ready"]["ts"]]})

    return {
        **arrival, "retained_us": serving["input_retained"]["ts"],
        "pending_release_us": [pending["release_started"]["ts"], pending["release_finished"]["ts"]],
        "bound_us": serving["request_bound"]["ts"],
        "admitted_us": serving["admission_reserved"]["ts"],
        "committed_us": serving["committed"]["ts"],
        "query_us": [query["read_started"]["ts"], query["read_finished"]["ts"]],
        "settlement_us": [settlement["release_started"]["ts"], settlement["release_finished"]["ts"]],
        "request_overhead": cost["request_overhead"], "response_cap": cost["response_cap"],
        "frames": frames, "waits": waits,
    }


def import_completed_lifetimes(block_lines, query_lines, arrivals, metrics, boundary_bytes, clients_bytes):
    """Join freshly validated arrivals with the same closed file and its query table.

    The controller's stopped-client declaration is a prerequisite, not something
    inferred from successful request timings or balanced event totals.
    """
    require(arrivals.get("decode_totals_reconciled") is True, "decoded totals were not reconciled")
    boundary, clients = json.loads(boundary_bytes), json.loads(clients_bytes)
    require(isinstance(boundary, dict) and isinstance(clients, dict), "boundary must contain objects")
    require(integer(boundary, "schema_version") == 1 and integer(clients, "schema_version") == 1, "unsupported boundary version")
    require(boundary.get("quiescent_counters_verified") is True, "missing quiescent boundary")
    require(boundary.get("metrics_sha256") == hashlib.sha256(metrics).hexdigest(), "boundary metrics hash differs")
    require(boundary.get("clients_stopped_sha256") == hashlib.sha256(clients_bytes).hexdigest(), "client boundary hash differs")
    require(clients.get("no_new_clients") is True and isinstance(clients.get("clients"), list) and clients["clients"],
            "controller has not closed the client population")
    require(all(isinstance(client, dict) and type(client.get("MainPID")) is int and client["MainPID"] == 0
                and client.get("ActiveState") == "inactive" for client in clients["clients"]), "a client was not stopped")

    expected = Counter()
    owners, by_id = {}, {}
    process = None
    hashes = {}
    def count(family, **labels):
        expected[(family, tuple(sorted(labels.items())))] += 1
    for table, lines in [("block_sync", block_lines), ("commit_state", query_lines)]:
        digest = hashlib.sha256()
        for line_number, raw in enumerate(lines, 1):
            digest.update(raw)
            try:
                row = json.loads(raw)
                require(isinstance(row, dict), "trace row is not an object")
                event = row.get("event")
                if event == START:
                    require(table == "block_sync", "decode row in query table")
                    process = process or row["process_trace_id"]
                    count("sessions_started")
                elif event == FINISH:
                    require(table == "block_sync", "decode row in query table")
                    count("sessions_finished")
                elif event == MESSAGE:
                    require(table == "block_sync", "decode row in query table")
                    count("decoded_messages", kind=row["kind"])
                    if row["kind"] == "get_blocks":
                        key = (row["peer"], row["session_id"], row["message_sequence"])
                        require(key not in owners, "duplicate decoded request")
                        require(len(owners) < len(arrivals["requests"]), "extra decoded request")
                        owners[key] = {"arrival": arrivals["requests"][len(owners)], "events": defaultdict(list)}
                elif event in FAMILIES:
                    require(integer(row, "capture_version") == 1, "unsupported lifecycle version")
                    require(row.get("process_trace_id") == process, "lifecycle process changed")
                    require(isinstance(row.get("phase"), str), "missing lifecycle phase")
                    labels = {"phase": row["phase"]}
                    if event in {"get_blocks_wait", "get_blocks_ownership"}:
                        require(isinstance(row.get("stage"), str), "missing lifecycle stage")
                        labels["stage"] = row["stage"]
                    count(FAMILIES[event], **labels)
                    if event == "get_blocks_query":
                        require(table == "commit_state", "query row in decode table")
                        owner = by_id[integer(row, "request_id")]
                    else:
                        require(table == "block_sync", "serving row in query table")
                        owner = owners[(row["peer"], integer(row, "session_id"), integer(row, "message_sequence"))]
                        if event == "get_blocks_serving" and row["phase"] == "request_bound":
                            request_id = integer(row, "request_id")
                            require(request_id > 0 and request_id not in by_id, "reused reactor request ID")
                            by_id[request_id] = owner
                    require(integer(row, "ts") >= owner["arrival"]["decoded_us"], "lifecycle predates decode")
                    owner["events"][event].append(row)
                elif isinstance(event, str) and event.startswith("get_blocks_"):
                    raise IncompleteCapture("unsupported GetBlocks observation event")
            except (ValueError, KeyError, TypeError) as error:
                raise IncompleteCapture(f"{table} line {line_number}: {error}") from error
        hashes[table] = digest.hexdigest()
    require(hashes["block_sync"] == arrivals["source_sha256"], "decode source changed between validation passes")
    count("process_info", process_trace_id=process)
    recorded = read_capture_metrics(metrics, set(FAMILIES.values()) | {"process_info", "sessions_started", "sessions_finished", "decoded_messages"})
    require(dict(expected) == {key: value for key, value in recorded.items() if value}, "lifecycle rows do not match independent counters")
    require(len(owners) == len(arrivals["requests"]) == len(by_id), "missing request ownership or binding")
    requests = []
    for index, owner in enumerate(owners.values()):
        try:
            requests.append(completed_request(owner["arrival"], owner["events"]))
        except (ValueError, KeyError, TypeError) as error:
            raise IncompleteCapture(f"request {index}: {error}") from error
    return {
        "version": 1, "profile": "completed_getblocks_application_lifetimes", "time_unit": "microseconds",
        "observation_boundary": "peer_routine_decode", "source_sha256": hashes,
        "metrics_sha256": hashlib.sha256(metrics).hexdigest(),
        "boundary_sha256": hashlib.sha256(boundary_bytes).hexdigest(),
        "all_observation_counters_reconciled": True, "completed_request_profiles_verified": True,
        "instantaneous_global_balances_reconstructed": False,
        "write_return_semantics": "success_or_error_not_peer_receipt",
        "peers": arrivals["peers"], "sessions": arrivals["sessions"], "requests": requests,
    }
