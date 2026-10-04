"""Coherent NU7 feed and durable public-Testnet selection.

RPCs stay private. All reads are bounded and occur in the polling thread; HTTP
handlers only copy its completed generation. A selected public network never
falls back to staging, including after restart or a reference outage.
"""
from __future__ import annotations

import base64
import concurrent.futures
import copy
import fcntl
import hashlib
import itertools
import json
import logging
import os
import re
from pathlib import Path
import statistics
import subprocess
import tempfile
import threading
import time
import urllib.request

MAX_JSON_BYTES = 1_048_576
RPC_TIMEOUT = 5
MAX_REFERENCE_BYTES = 16 * MAX_JSON_BYTES


def read_json(url, body=None, headers=None):
    """Read at most one MiB with a five-second socket deadline."""
    request = urllib.request.Request(url, data=body, headers=headers or {})
    with urllib.request.urlopen(request, timeout=RPC_TIMEOUT) as response:
        raw = response.read(MAX_JSON_BYTES + 1)
    if len(raw) > MAX_JSON_BYTES:
        raise ValueError("JSON response exceeds limit")
    return json.loads(raw)


def rpc(url, method, params=None):
    result = read_json(url, json.dumps({"jsonrpc": "2.0", "id": 1,
                       "method": method, "params": params or []}).encode(),
                       {"Content-Type": "application/json"})
    if result.get("error") is not None:
        raise ValueError(f"RPC {method} failed")
    return result["result"]


def atomic_json(path, value):
    """Persist a complete generation, including across power loss."""
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w") as stream:
        json.dump(value, stream, allow_nan=False)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def rules(value):
    result = {**value, **value["difficulty"]}
    result["effectiveHeight"] = value["effectiveHeight"]
    result["daaWindowBlocks"] = value["difficulty"]["averagingWindowBlocks"]
    result["minimumDifficulty"] = {
        "gapMultiplier": value["difficulty"]["minimumDifficultyGapMultiplier"],
        "thresholdSeconds": value["difficulty"]["minimumDifficultyGapSeconds"],
        "comparison": "strictly-greater" if value["difficulty"]["minimumDifficultyGapSeconds"] is not None else "disabled",
    }
    return result


class ActivationFeed:
    """Poll the approved profiles and publish one locked, persistent selection.

    Construct once per service. A process lock prevents two selectors owning the
    same state. Corrupt/missing state after a previous selection must be repaired
    by an operator; state-directory deletion is never an automatic reset.
    """

    def __init__(self, config_path, staging_status, clock=time.time, rpc_call=rpc):
        self.config = json.loads(Path(config_path).read_text())
        self.clock, self.rpc = clock, rpc_call
        self.staging_status = staging_status
        self.path = Path(self.config["stateFile"])
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.validate_config()
        owner_path = self.path.with_suffix(".lock")
        previously_initialized = owner_path.exists()
        marker_path = self.path.with_suffix(".initialized")
        self.owner = owner_path.open("a")
        try:
            fcntl.flock(self.owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if not self.path.exists() and (marker_path.exists() or previously_initialized):
                raise ValueError("selection state missing from initialized directory; operator repair required")
            self.state = (json.loads(self.path.read_text()) if self.path.exists() else
                          {"selectionState": "armed" if self.config["armed"] else "staging",
                           "selectedProfile": "staging", "consecutivePasses": 0})
        except Exception:
            self.owner.close()
            raise
        if (self.state["selectedProfile"] not in ("staging", "public-testnet")
                or (self.state["selectedProfile"] == "public-testnet"
                    and self.state["selectionState"] != "selected")):
            self.owner.close()
            raise ValueError("invalid persisted selection")
        if self.state["selectedProfile"] == "staging":
            self.state["selectionState"] = "armed" if self.config["armed"] else "staging"
        # A restart always requires fresh consecutive observations.
        self.state["consecutivePasses"] = 0
        self.lock = threading.Lock()
        self.payload = self.state.get("lastPublicEnvelope")
        self.progress = {}
        self.headers = {}
        self.last_tips = {}
        atomic_json(self.path, self.state)
        # Adopt intact legacy selection safely; the marker survives loss of the
        # selection file and is never an automatic reset authorization.
        atomic_json(marker_path, {"schemaVersion": 1})

    def validate_config(self):
        public = self.config["public"]
        if len(public["nodes"]) != 3 or len({n["name"] for n in public["nodes"]}) != 3:
            raise ValueError("exactly three distinct public validators are required")
        if public["reference"].get("rpcUrl") in {n["rpcUrl"] for n in public["nodes"]}:
            raise ValueError("reference must be independent of managed validators")
        if public["manifest"]["network"]["magic"] != "fa1af9bf":
            raise ValueError("public Testnet magic mismatch")
        for profile in (self.config["staging"], public):
            manifest = profile["manifest"]
            if not re.fullmatch(r"[0-9a-f]{40}", manifest["nodeRevision"]):
                raise ValueError("manifest requires full source revision")
            if hashlib.sha256(manifest["config"].encode()).hexdigest() != manifest["configSha256"]:
                raise ValueError("joining configuration checksum mismatch")
        if not (self.config["staging"]["manifest"]["network"]["activationHeight"]
                < public["identityCheckpoint"]["height"]
                < public["manifest"]["network"]["activationHeight"]):
            raise ValueError("identity checkpoint must follow staging divergence and precede public activation")

    def observe(self, node, now, parameters=True):
        """Verify network history and retain local progress age for every source."""
        try:
            if node.get("adapter") == "grpcurl":
                return self.observe_lightwallet(node, now)
            url = node["rpcUrl"]
            info = self.rpc(url, "getblockchaininfo")
            public = self.config["public"]
            identity = public["identityCheckpoint"]
            activation = public["manifest"]["network"]["activationHeight"]
            branch = public["manifest"]["network"]["branchId"]
            upgrade = info["upgrades"].get(branch)
            if (info["chain"] != "test" or not upgrade
                    or upgrade["activationheight"] != activation
                    or self.rpc(url, "getblockhash", [identity["height"]]) != identity["hash"]):
                raise ValueError("public Testnet identity/upgrade mismatch")
            tip = (info["blocks"], info["bestblockhash"])
            previous = self.progress.get(node["name"])
            changed = now if previous is None or previous[:2] != tip else previous[2]
            self.progress[node["name"]] = (*tip, changed)
            result = {"name": node["name"], "url": url, "source": node, "height": tip[0], "hash": tip[1],
                      "observedAt": now, "fresh": True, "progressAgeSeconds": max(0, now - changed),
                      "active": upgrade["status"] == "active"}
            if parameters:
                parameter_rules = self.parameter_rules(node, tip[0])
                version = self.rpc(url, "getinfo").get("build")
                expected = public["manifest"]["nodeRevision"]
                if not isinstance(version, str) or expected[:9] not in version.lower():
                    raise ValueError("running binary revision does not match approved manifest")
                result["buildVersion"] = version
                result["sourceRevision"] = expected
                pinned = self.rpc(url, "getblockchaininfo")
                if (pinned["blocks"], pinned["bestblockhash"]) != tip:
                    raise ValueError("tip moved during parameter observation")
                result["rules"] = parameter_rules
            return result
        except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as exc:
            # Avoid echoing URLs, credentials, or RPC response bodies.
            logging.warning("NU7 source %s unavailable (%s)", node["name"], type(exc).__name__)
            return {"name": node["name"], "fresh": False, "error": "Source unavailable or mismatched"}

    def parameter_rules(self, node, height):
        """Export complete rules at specified heights with public identity checks."""
        public = self.config["public"]["manifest"]["network"]
        exports = []
        for effective_height in (height, height + 1):
            value = self.rpc(node["rpcUrl"], "getnetworkparameters", [effective_height])
            if (value["network"] != "Testnet" or value["networkMagic"] != "fa1af9bf"
                    or value["activationHeight"] != public["activationHeight"]
                    or value["nu7BranchId"] != public["branchId"]
                    or value["effectiveHeight"] != effective_height):
                raise ValueError("parameter export identity mismatch")
            exports.append(rules(value))
        return dict(zip(("atTip", "nextBlock"), exports))

    def pinned_parameter_rules(self, observation, height):
        """Reject exports if their source moved since its verified observation."""
        exported = self.parameter_rules(observation["source"], height)
        info = self.rpc(observation["url"], "getblockchaininfo")
        if (info["blocks"], info["bestblockhash"]) != (observation["height"], observation["hash"]):
            raise ValueError("tip moved during parameter agreement")
        return exported

    def lightwallet(self, node, method, request):
        """Read bounded lightwalletd metadata through a pinned grpcurl binary."""
        executable = node["grpcurlPath"]
        if not Path(executable).is_absolute():
            raise ValueError("grpcurl path must be absolute")
        with tempfile.TemporaryFile() as output:
            process = subprocess.Popen([executable, "-max-time", "12", "-max-msg-sz", str(MAX_REFERENCE_BYTES),
                "-d", json.dumps(request), node["endpoint"],
                "cash.z.wallet.sdk.rpc.CompactTxStreamer/" + method],
                stdout=output, stderr=subprocess.DEVNULL)
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
                raise OSError("reference deadline exceeded")
            if process.returncode:
                raise OSError("reference unavailable")
            output.seek(0)
            raw = output.read(MAX_REFERENCE_BYTES + 1)
        if len(raw) > MAX_REFERENCE_BYTES:
            raise ValueError("reference response exceeds limit")
        return json.loads(raw)

    def lightwallet_hash(self, node, height):
        block = self.lightwallet(node, "GetBlock", {"height": height})
        if int(block["height"]) != height:
            raise ValueError("reference returned wrong height")
        raw = base64.b64decode(block["hash"], validate=True)
        if len(raw) != 32:
            raise ValueError("reference block hash length")
        if node["hashByteOrder"] == "little":
            raw = raw[::-1]
        elif node["hashByteOrder"] != "big":
            raise ValueError("reference byte order not verified")
        return raw.hex()

    def source_hash(self, observation, height):
        source = observation["source"]
        if source.get("adapter") == "grpcurl":
            return self.lightwallet_hash(source, height)
        return self.rpc(source["rpcUrl"], "getblockhash", [height])

    def observe_lightwallet(self, node, now):
        """An independent Zebra-backed lightwalletd verifies public chain hashes."""
        info = self.lightwallet(node, "GetLightdInfo", {})
        public = self.config["public"]
        identity = public["identityCheckpoint"]
        if (info["chainName"] != "test"
                or self.lightwallet_hash(node, identity["height"]) != identity["hash"]):
            raise ValueError("reference public chain identity mismatch")
        height = int(info["blockHeight"])
        block_hash = self.lightwallet_hash(node, height)
        previous = self.progress.get(node["name"])
        tip = (height, block_hash)
        changed = now if previous is None or previous[:2] != tip else previous[2]
        self.progress[node["name"]] = (*tip, changed)
        return {"name": node["name"], "source": node, "height": height, "hash": block_hash,
                "observedAt": now, "fresh": True, "progressAgeSeconds": max(0, now - changed),
                "active": info["consensusBranchId"] == public["manifest"]["network"]["branchId"]}

    def agreement(self, nodes, reference, now):
        """Two managed nodes plus independent reference must agree through A+2."""
        activation = self.config["public"]["manifest"]["network"]["activationHeight"]
        candidates = [n for n in nodes if n.get("fresh") and n.get("active")
                      and n.get("height", 0) >= activation + 2
                      and 0 <= now - n["observedAt"] <= 60]
        if not (reference.get("fresh") and reference.get("active")
                and reference.get("height", 0) >= activation + 2
                and 0 <= now - reference["observedAt"] <= 60):
            return None
        for pair in itertools.combinations(candidates, 2):
            sources = (*pair, reference)
            common = min(n["height"] for n in sources)
            try:
                activation_hashes = [self.source_hash(n, activation) for n in sources]
                common_hashes = [self.source_hash(n, common) for n in sources]
                parameter_rules = [self.pinned_parameter_rules(n, common) for n in pair]
                if (parameter_rules[0] == parameter_rules[1]
                        and len(set(activation_hashes)) == len(set(common_hashes)) == 1
                        and all(0 <= self.clock() - n["observedAt"] <= 60 for n in sources)):
                    agreeing = [n["name"] for n in pair]
                    for candidate in candidates:
                        if candidate["name"] not in agreeing:
                            try:
                                if (self.source_hash(candidate, activation) == activation_hashes[0]
                                        and self.source_hash(candidate, common) == common_hashes[0]
                                        and self.pinned_parameter_rules(candidate, common) == parameter_rules[0]):
                                    agreeing.append(candidate["name"])
                            except (OSError, ValueError, KeyError, TypeError):
                                pass
                    return {"activationHash": activation_hashes[0], "commonHeight": common,
                            "commonHash": common_hashes[0], "nodes": agreeing}
            except (OSError, ValueError, KeyError, TypeError):
                continue
        return None

    def public_status(self, nodes, agreement, now):
        available = [n for n in nodes if n.get("rules") and n.get("fresh")]
        if not available:
            raise ValueError("public parameter source unavailable")
        if agreement:
            available = [n for n in available if n["name"] in agreement["nodes"]]
        # Corroborate the exact heights we publish, even when validators have
        # different tips. Common-height agreement alone cannot attest a higher tip.
        primary = None
        for candidate in sorted(available, key=lambda n: n["height"], reverse=True):
            for peer in available:
                if peer["name"] == candidate["name"]:
                    continue
                try:
                    if self.pinned_parameter_rules(peer, candidate["height"]) == candidate["rules"]:
                        primary = candidate
                        break
                except (OSError, ValueError, KeyError, TypeError):
                    continue
            if primary is not None:
                break
        if primary is None:
            raise ValueError("public parameter quorum unavailable")
        height = primary["height"]
        manifest = self.config["public"]["manifest"]
        activation = manifest["network"]["activationHeight"]
        start = max(min(activation, height), height - 300)
        previous = self.last_tips.get(primary["url"])
        if previous and (previous[0] > height or
                         self.rpc(primary["url"], "getblockhash", [previous[0]]) != previous[1]):
            self.headers.clear()
        self.last_tips[primary["url"]] = (height, primary["hash"])
        headers = []
        for number in range(start, height + 1):
            key = (primary["url"], number)
            if key not in self.headers or number >= height - 2:
                block_hash = self.rpc(primary["url"], "getblockhash", [number])
                self.headers[key] = self.rpc(primary["url"], "getblockheader", [block_hash, True])
            headers.append(self.headers[key])
        self.headers = {k: v for k, v in self.headers.items() if k[0] == primary["url"] and k[1] >= start}
        tip = headers[-1]
        intervals = [max(0, b["time"] - a["time"]) for a, b in zip(headers, headers[1:])]
        info = self.rpc(primary["url"], "getblockchaininfo")
        if info["blocks"] != height or info["bestblockhash"] != primary["hash"]:
            raise ValueError("tip moved during coherent response collection")
        balance = info.get("nsmValueBalanceZat")
        threshold = primary["rules"]["atTip"]["minimumDifficulty"]["thresholdSeconds"]
        gap = max(0, headers[-1]["time"] - headers[-2]["time"]) if len(headers) > 1 else None
        bits = tip.get("bits")
        limit = primary["rules"]["atTip"].get("powLimitCompact")
        status = {
            "schemaVersion": 1, "observedAt": now, "status": "live" if agreement else "degraded",
            "network": {**manifest["network"], "targetSpacingSeconds": primary["rules"]["atTip"]["targetSpacingSeconds"],
                        "daaWindowBlocks": primary["rules"]["atTip"]["daaWindowBlocks"],
                        "reissuanceHeight": primary["rules"]["atTip"].get("nsmReissuanceHeight"),
                        "reissuanceKnown": primary["rules"]["atTip"].get("nsmReissuanceHeight") is not None},
            "chain": {"height": height, "hash": primary["hash"], "blockTime": tip["time"],
                      "tipAgeSeconds": max(0, int(now - tip["time"])), "difficulty": tip.get("difficulty"),
                      "meanIntervalSeconds": round(statistics.mean(intervals), 1) if intervals else None,
                      "medianIntervalSeconds": round(statistics.median(intervals), 1) if intervals else None,
                      "intervalSampleBlocks": len(intervals),
                      "timestampGapSeconds": gap,
                      "minimumDifficultyEligible": (gap > threshold if gap is not None and threshold is not None else None),
                      "minimumDifficultyBlock": (str(bits).lower().removeprefix("0x") == str(limit).lower().removeprefix("0x")
                                                 if bits is not None and limit is not None else None)},
            "nsm": {"balanceZat": balance, "available": balance is not None, "seedZat": None},
            "observation": {"validatorsConfigured": 3, "validatorsAgree": bool(agreement and len(agreement["nodes"]) == 3),
                            "localNodesAgree": bool(agreement), "validatorsAgreeing": len(agreement["nodes"]) if agreement else 0,
                            "blocks24h": None, "reorgs24h": None, "reorgRate24h": None,
                            "since": now, "scope": "Three public-Testnet validators and independent reference"},
            "mining": {"operatorMinersActive": 0, "operatorMinersConfigured": 0, "remoteMiners": []},
            "nodes": [{"name": n["name"], "healthy": bool(agreement and n["name"] in agreement["nodes"]),
                       **({"height": n["height"], "hash": n["hash"]} if n.get("height") is not None else {})}
                      for n in nodes],
            "recentBlocks": [{"height": header["height"], "hash": header["hash"],
                              "time": header["time"], "difficulty": header.get("difficulty")}
                             for header in reversed(headers[-8:])],
        }
        return status, primary["rules"]

    def envelope(self, profile, status, parameter_rules, now):
        settings = self.config["public" if profile == "public-testnet" else "staging"]
        body = {"schemaVersion": 1, "selectedProfile": profile,
                "selectionState": self.state["selectionState"], "generatedAt": now,
                "status": status, "network": settings["manifest"], "rules": parameter_rules,
                "capabilities": settings.get("capabilities", {"faucet": None, "snapshot": None})}
        body["generation"] = hashlib.sha256(json.dumps(body, sort_keys=True, allow_nan=False).encode()).hexdigest()
        return body

    def dispatch(self):
        """Idempotent notification; failed dispatch is retried on later polls."""
        if (self.state.get("publicationDispatched") or not self.config.get("dispatch", False)
                or not self.state.get("lastPublicEnvelope")):
            return
        token = os.environ.get("NU7_DISPATCH_TOKEN")
        if not token:
            logging.warning("NU7 publication credential unavailable")
            return
        try:
            request = urllib.request.Request("https://api.github.com/repos/zakura-core/website/dispatches",
                data=json.dumps({"event_type": "nu7_activated", "client_payload": {
                    "selectedProfile": "public-testnet", "selectionId": self.state["selectionId"]}}).encode(),
                headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
                         "Content-Type": "application/json"})
            with urllib.request.urlopen(request, timeout=RPC_TIMEOUT) as response:
                if response.status != 204:
                    raise ValueError("publication dispatch rejected")
            self.state["publicationDispatched"] = True
        except (OSError, ValueError):
            logging.warning("NU7 publication dispatch failed; retry retained")

    def poll(self):
        now = self.clock()
        public = self.config["public"]
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(self.observe, node, now) for node in public["nodes"]]
            reference_future = executor.submit(self.observe, public["reference"], now, False)
            nodes = [f.result() for f in futures]
            reference = reference_future.result()
        agreement = self.agreement(nodes, reference, self.clock())
        if self.state["selectedProfile"] == "staging" and self.config["armed"]:
            self.state["selectionState"] = "armed"
            self.state["consecutivePasses"] = self.state["consecutivePasses"] + 1 if agreement else 0
            if self.state["consecutivePasses"] >= 3:
                self.state.update(selectedProfile="public-testnet", selectionState="selected",
                                  selectedAt=now, selectionEvidence=agreement,
                                  selectionId=hashlib.sha256(json.dumps(agreement, sort_keys=True).encode()).hexdigest())
        profile = self.state["selectedProfile"]
        try:
            if profile == "public-testnet":
                status, parameters = self.public_status(nodes, agreement, now)
            else:
                status = (self.staging_status.response()[1] if self.staging_status else
                          read_json(self.config["staging"]["statusUrl"]))
                height = status["chain"]["height"]
                exported = status.get("rules")
                if exported:
                    current, following = exported["atTip"], exported["nextBlock"]
                elif self.config["staging"].get("rpcUrl"):
                    current = self.rpc(self.config["staging"]["rpcUrl"], "getnetworkparameters", [height])
                    following = self.rpc(self.config["staging"]["rpcUrl"], "getnetworkparameters", [height + 1])
                else:
                    raise ValueError("staging consensus export unavailable")
                if (current["networkMagic"] != self.config["staging"]["manifest"]["network"]["magic"]
                        or current["effectiveHeight"] != height
                        or following["effectiveHeight"] != height + 1):
                    raise ValueError("staging parameter network mismatch")
                parameters = {"atTip": rules(current), "nextBlock": rules(following)}
            payload = self.envelope(profile, status, parameters, now)
        except (OSError, ValueError, KeyError, TypeError):
            previous = self.state.get("lastPublicEnvelope") if profile == "public-testnet" else None
            if previous:
                payload = copy.deepcopy(previous)
                payload["status"]["status"] = "unavailable"
                payload["status"]["error"] = "Public Testnet observation unavailable; showing last verified generation"
                payload["generation"] = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
            else:
                payload = self.envelope(profile, {"schemaVersion": 1, "status": "unavailable",
                    "observedAt": now, "error": "Selected network observation unavailable"}, None, now)
        if profile == "public-testnet":
            if payload["status"]["status"] != "unavailable":
                self.state["lastPublicEnvelope"] = payload
            self.dispatch()
        atomic_json(self.path, self.state)
        with self.lock:
            self.payload = payload

    def response(self):
        with self.lock:
            payload = copy.deepcopy(self.payload)
        if payload and self.clock() - payload["generatedAt"] > 60:
            payload["status"]["status"] = "unavailable"
            payload["status"]["error"] = "Selected network observations are stale"
            payload["generation"] = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
        return (200, payload) if payload else (503, {"schemaVersion": 1, "error": "Observation pending"})

    def loop(self):
        while True:
            started = time.monotonic()
            try:
                self.poll()
            except Exception:
                logging.exception("NU7 selector poll failed")
            time.sleep(max(0, 10 - (time.monotonic() - started)))
