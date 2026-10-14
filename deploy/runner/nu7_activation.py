"""Coherent NU7 feed and durable public-Testnet selection.

RPCs stay private. All reads are bounded and occur in the polling thread; HTTP
handlers only copy its completed generation. A selected public network never
falls back to staging, including after restart or a reference outage.
"""
from __future__ import annotations

import base64
import concurrent.futures
import copy
from decimal import Decimal, InvalidOperation
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
import urllib.parse
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


def rpc_endpoint_identity(url):
    """Normalize transport endpoints independently of credentials/query tokens.

    This detects repeated configured endpoints, not DNS aliases or independent
    hosts; source independence also requires operator provenance verification.
    """
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme.lower() not in ("http", "https") or not parsed.hostname:
        raise ValueError("RPC endpoint requires HTTP(S) host")
    port = parsed.port or (443 if parsed.scheme.lower() == "https" else 80)
    return (parsed.scheme.lower(), parsed.hostname.lower(), port, parsed.path.rstrip("/"))


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
        self.protocol_reference = self.load_protocol_reference()
        self.mainnet_protocol_reference = (self.load_protocol_reference(self.config["mainnet"]["protocolReferenceFile"])
                                           if self.config.get("mainnet") else None)
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
        if (self.state["selectedProfile"] not in ("staging", "public-testnet", "mainnet")
                or (self.state["selectedProfile"] in ("public-testnet", "mainnet")
                    and self.state["selectionState"] != "selected")):
            self.owner.close()
            raise ValueError("invalid persisted selection")
        if self.state["selectedProfile"] == "staging":
            self.state["selectionState"] = "armed" if self.config["armed"] else "staging"
        if self.state["selectedProfile"] == "mainnet" and not self.config.get("mainnet"):
            self.owner.close()
            raise ValueError("selected Mainnet configuration missing; operator repair required")
        self.state["mainnetConsecutivePasses"] = 0
        # A restart always requires fresh consecutive observations.
        self.state["consecutivePasses"] = 0
        self.lock = threading.Lock()
        self.payload = self.state.get("lastPublicEnvelope")
        if self.payload and (self.payload["selectedProfile"] != self.state["selectedProfile"]
                or self.payload["network"]["network"]["magic"] != self.settings(self.state["selectedProfile"])["manifest"]["network"]["magic"]):
            self.owner.close()
            raise ValueError("saved observation belongs to another selected network")
        self.progress = {}
        self.headers = {}
        self.last_tips = {}
        atomic_json(self.path, self.state)
        # Adopt intact legacy selection safely; the marker survives loss of the
        # selection file and is never an automatic reset authorization.
        atomic_json(marker_path, {"schemaVersion": 1})

    def validate_config(self):
        for profile, settings in (("public-testnet", self.config["public"]),
                                  ("mainnet", self.config.get("mainnet"))):
            if settings is None:
                continue
            nodes = settings["nodes"]
            if len(nodes) != 3 or len({n["name"] for n in nodes}) != 3:
                raise ValueError("exactly three distinct public validators are required")
            endpoints = [rpc_endpoint_identity(n["rpcUrl"]) for n in nodes]
            if len(set(endpoints)) != 3:
                raise ValueError("managed validators require distinct RPC endpoints")
            reference = settings["reference"]
            if reference["name"] in {n["name"] for n in nodes}:
                raise ValueError("reference requires a distinct source name")
            if reference.get("rpcUrl") and rpc_endpoint_identity(reference["rpcUrl"]) in endpoints:
                raise ValueError("reference must be independent of managed validators")
            network = settings["manifest"]["network"]
            name, magic = ("Mainnet", "24e92764") if profile == "mainnet" else ("Testnet", "fa1af9bf")
            if network["name"] != name or network["magic"] != magic or network["branchId"] != "77190ad9":
                raise ValueError("public network identity mismatch")
            if not (0 < settings["identityCheckpoint"]["height"] < network["activationHeight"]):
                raise ValueError("identity checkpoint must precede activation")
            if profile == "mainnet":
                if type(settings.get("armed", False)) is not bool:
                    raise ValueError("Mainnet armed must be a boolean")
                if not Path(settings["protocolReferenceFile"]).is_absolute():
                    raise ValueError("Mainnet protocol reference requires an absolute path")
                if settings.get("capabilities", {}).get("faucet") is not None:
                    raise ValueError("Mainnet cannot advertise a test faucet")
                if settings.get("capabilities", {}).get("snapshot") != settings["manifest"].get("snapshot"):
                    raise ValueError("snapshot capability differs from manifest")
                snapshot = settings["manifest"].get("snapshot")
                if snapshot and snapshot.get("networkId") != "mainnet":
                    raise ValueError("Mainnet snapshot identity unverified")
        profiles = [self.config["staging"], self.config["public"]]
        if self.config.get("mainnet"):
            profiles.append(self.config["mainnet"])
        for settings in profiles:
            manifest = settings["manifest"]
            if not re.fullmatch(r"[0-9a-f]{40}", manifest["nodeRevision"]):
                raise ValueError("manifest requires full source revision")
            if hashlib.sha256(manifest["config"].encode()).hexdigest() != manifest["configSha256"]:
                raise ValueError("joining configuration checksum mismatch")
            if settings is not self.config["staging"]:
                name = manifest["network"]["name"]
                if (not re.search(r'^network\s*=\s*"' + name + r'"\s*$', manifest["config"], re.M)
                        or any(key in manifest["config"] for key in ("testnet_parameters", "activation_heights", "Nu7Staging"))):
                    raise ValueError("public profile requires standard network configuration")
        if not (self.config["staging"]["manifest"]["network"]["activationHeight"]
                < self.config["public"]["identityCheckpoint"]["height"]):
            raise ValueError("identity checkpoint must follow staging divergence")

    def settings(self, profile):
        return self.config[{"public-testnet": "public", "mainnet": "mainnet", "staging": "staging"}[profile]]

    def reference_parameters(self, profile):
        return self.mainnet_protocol_reference if profile == "mainnet" else self.protocol_reference

    def observe(self, node, now, include_build=True, profile="public-testnet"):
        """Verify network history and retain local progress age for every source."""
        try:
            if node.get("adapter") == "grpcurl":
                return self.observe_lightwallet(node, now, profile)
            url = node["rpcUrl"]
            info = self.rpc(url, "getblockchaininfo")
            public = self.settings(profile)
            identity = public["identityCheckpoint"]
            activation = public["manifest"]["network"]["activationHeight"]
            branch = public["manifest"]["network"]["branchId"]
            upgrade = info["upgrades"].get(branch)
            if (info["chain"] != ("main" if profile == "mainnet" else "test") or not upgrade
                    or upgrade["activationheight"] != activation
                    or self.rpc(url, "getblockhash", [identity["height"]]) != identity["hash"]):
                raise ValueError("public Testnet identity/upgrade mismatch")
            tip = (info["blocks"], info["bestblockhash"])
            previous = self.progress.get((profile, node["name"]))
            changed = now if previous is None or previous[:2] != tip else previous[2]
            self.progress[(profile, node["name"])] = (*tip, changed)
            result = {"name": node["name"], "url": url, "source": node, "height": tip[0], "hash": tip[1],
                      "observedAt": now, "fresh": True, "progressAgeSeconds": max(0, now - changed),
                      "active": upgrade["status"] == "active",
                      "nsmValueBalanceZat": info.get("nsmValueBalanceZat"),
                      "consensus": info.get("consensus")}
            if include_build:
                try:
                    version = self.rpc(url, "getinfo").get("build")
                    if isinstance(version, str):
                        result["buildVersion"] = version
                except (OSError, ValueError, KeyError, TypeError):
                    pass
            pinned = self.rpc(url, "getblockchaininfo")
            if pinned["blocks"] < tip[0] or self.rpc(url, "getblockhash", [tip[0]]) != tip[1]:
                raise ValueError("sampled block left the current chain")
            return result
        except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as exc:
            # Avoid echoing URLs, credentials, or RPC response bodies.
            logging.warning("NU7 source %s unavailable (%s)", node["name"], type(exc).__name__)
            return {"name": node["name"], "fresh": False, "error": "Source unavailable or mismatched"}

    @staticmethod
    def load_protocol_reference(path=None):
        """Load optional, versioned protocol data without gating chain reports."""
        try:
            return json.loads((Path(path) if path else Path(__file__).with_name("nu7-protocol-reference.json")).read_text())
        except (OSError, ValueError):
            logging.warning("NU7 protocol reference unavailable; chain reporting continues")
            return None

    def consolidated_rules(self, primary, profile="public-testnet"):
        """Combine reference constants with height, branch, build and subsidy RPCs."""
        reference = self.reference_parameters(profile)
        manifest = self.settings(profile)["manifest"]["network"]
        try:
            if (not reference or reference["schemaVersion"] != 1
                    or any(reference["network"][key] != manifest[key]
                           for key in ("name", "magic", "branchId", "activationHeight"))):
                return None
            if (not isinstance(reference.get("sourceRevision"), str)
                    or len(reference["sourceRevision"]) != 40
                    or not isinstance(reference.get("sourceUrl"), str)):
                return None
            required = {"targetSpacingSeconds", "minimumProtocolVersion", "nsmReissuanceHeight",
                        "coinbaseMaturityBlocks", "fees", "difficulty", "actionLimits"}
            parameters = reference["parameters"]
            if not required.issubset(parameters):
                return None
            if (set(parameters["fees"]) != {"minerPercent", "nsmPercent", "nsmRounding"}
                    or set(parameters["actionLimits"]) != {"globalShieldedBudget", "orchardActionsPerPool", "saplingSpendsAndOutputs", "sproutJoinSplits"}
                    or set(parameters["difficulty"]) != {"averagingTimespanSeconds", "averagingWindowBlocks", "dampingFactor",
                            "maxAdjustDownPercent", "maxAdjustUpPercent", "medianTimeSpanBlocks",
                            "minimumDifficultyGapMultiplier", "minimumDifficultyGapSeconds",
                            "minimumDifficultyStrictlyGreater", "powLimit", "powLimitCompact"}):
                return None
            def positive_integer(value):
                return type(value) is int and 0 < value <= 9_007_199_254_740_991
            if (set(parameters) != required
                    or any(not positive_integer(parameters[key]) for key in
                           ("targetSpacingSeconds", "minimumProtocolVersion", "nsmReissuanceHeight", "coinbaseMaturityBlocks"))):
                return None
            difficulty = parameters["difficulty"]
            if any(not positive_integer(difficulty[key]) for key in
                   ("averagingTimespanSeconds", "averagingWindowBlocks", "dampingFactor", "medianTimeSpanBlocks")):
                return None
            if difficulty["averagingTimespanSeconds"] != parameters["targetSpacingSeconds"] * difficulty["averagingWindowBlocks"]:
                return None
            if (not re.fullmatch(r"[0-9a-f]{64}", difficulty["powLimit"])
                    or not re.fullmatch(r"[0-9a-f]{8}", difficulty["powLimitCompact"])):
                return None
            if profile == "mainnet":
                if (difficulty["minimumDifficultyGapSeconds"] is not None
                        or difficulty["minimumDifficultyGapMultiplier"] is not None
                        or difficulty["minimumDifficultyStrictlyGreater"] is not False):
                    return None
            elif (not positive_integer(difficulty["minimumDifficultyGapSeconds"])
                    or not positive_integer(difficulty["minimumDifficultyGapMultiplier"])
                    or difficulty["minimumDifficultyStrictlyGreater"] is not True):
                return None
            fees = parameters["fees"]
            percentages = [fees["minerPercent"], fees["nsmPercent"],
                           difficulty["maxAdjustDownPercent"], difficulty["maxAdjustUpPercent"]]
            if (any(type(value) is not int or not 0 <= value <= 100 for value in percentages)
                    or fees["minerPercent"] + fees["nsmPercent"] != 100
                    or fees["nsmRounding"] != "floor-on-block-aggregate"
                    or any(type(value) is not int or not 0 <= value <= 9_007_199_254_740_991
                           for value in parameters["actionLimits"].values())):
                return None
            # Reject non-finite or non-JSON additions before a durable selection.
            json.dumps(reference, allow_nan=False)
            consensus = primary.get("consensus") or {}
            branches = (consensus.get("chaintip"), consensus.get("nextblock"))
            if any(branch != reference["network"]["branchId"] for branch in branches):
                return None
            exported = {}
            for key, height, branch in zip(("atTip", "nextBlock"),
                                            (primary["height"], primary["height"] + 1), branches):
                value = copy.deepcopy(reference["parameters"])
                value.update(schemaVersion=1, network=manifest["name"], networkMagic=manifest["magic"],
                             effectiveHeight=height, buildVersion=primary.get("buildVersion"),
                             activationHeight=manifest["activationHeight"], branchId=branch,
                             nu7BranchId=manifest["branchId"], nu7Active=True, baseSubsidyZat=None)
                try:
                    subsidy = self.rpc(primary["url"], "getblocksubsidy", [height])
                    amount = Decimal(str(subsidy["totalblocksubsidy"])) * 100_000_000
                    # Before reissuance the RPC's total is the halving base subsidy.
                    # Afterward it also contains balance-dependent issuance.
                    if (height < value["nsmReissuanceHeight"] and amount.is_finite()
                            and 0 <= amount <= 21_000_000 * 100_000_000
                            and amount == amount.to_integral_value()):
                        value["baseSubsidyZat"] = int(amount)
                except (OSError, ValueError, KeyError, TypeError, InvalidOperation):
                    logging.warning("NU7 subsidy unavailable; other observations retained")
                value.update(value["difficulty"])
                value["daaWindowBlocks"] = value["difficulty"]["averagingWindowBlocks"]
                value["minimumDifficulty"] = {
                    "gapMultiplier": value["difficulty"]["minimumDifficultyGapMultiplier"],
                    "thresholdSeconds": value["difficulty"]["minimumDifficultyGapSeconds"],
                    "comparison": "strictly-greater" if value["difficulty"]["minimumDifficultyStrictlyGreater"] else "disabled",
                }
                exported[key] = value
            return exported
        except (KeyError, TypeError, ValueError):
            logging.warning("NU7 protocol reference mismatched; chain reporting continues")
            return None

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

    def observe_lightwallet(self, node, now, profile="public-testnet"):
        """An independent Zebra-backed lightwalletd verifies public chain hashes."""
        info = self.lightwallet(node, "GetLightdInfo", {})
        public = self.settings(profile)
        identity = public["identityCheckpoint"]
        if (info["chainName"] != ("main" if profile == "mainnet" else "test")
                or self.lightwallet_hash(node, identity["height"]) != identity["hash"]):
            raise ValueError("reference public chain identity mismatch")
        height = int(info["blockHeight"])
        block_hash = self.lightwallet_hash(node, height)
        previous = self.progress.get((profile, node["name"]))
        tip = (height, block_hash)
        changed = now if previous is None or previous[:2] != tip else previous[2]
        self.progress[(profile, node["name"])] = (*tip, changed)
        return {"name": node["name"], "source": node, "height": height, "hash": block_hash,
                "observedAt": now, "fresh": True, "progressAgeSeconds": max(0, now - changed),
                "active": info["consensusBranchId"] == public["manifest"]["network"]["branchId"]}

    def agreement(self, nodes, reference, now, profile="public-testnet"):
        """Two managed nodes plus independent reference must agree through A+2."""
        activation = self.settings(profile)["manifest"]["network"]["activationHeight"]
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
                if (len(set(activation_hashes)) == len(set(common_hashes)) == 1
                        and all(0 <= self.clock() - n["observedAt"] <= 60 for n in sources)):
                    agreeing = [n["name"] for n in pair]
                    for candidate in candidates:
                        if candidate["name"] not in agreeing:
                            try:
                                if (self.source_hash(candidate, activation) == activation_hashes[0]
                                        and self.source_hash(candidate, common) == common_hashes[0]):
                                    agreeing.append(candidate["name"])
                            except (OSError, ValueError, KeyError, TypeError):
                                pass
                    return {"activationHash": activation_hashes[0], "commonHeight": common,
                            "commonHash": common_hashes[0], "nodes": agreeing}
            except (OSError, ValueError, KeyError, TypeError):
                continue
        return None

    def record_public_reorgs(self, height, block_hash, url, now, profile="public-testnet"):
        """Count sampled canonical-chain replacements, never ordinary extensions.

        Persist the previous verified tip and event times in the selection state.
        A restart or a different primary must compare against that same tip.
        The first observation establishes a baseline, not a measured zero.
        These are observed events; forks between successful polls can be missed.
        """
        state_key = "mainnetReorgObservation" if profile == "mainnet" else "publicReorgObservation"
        previous = self.state.get(state_key)
        def confirm_tip():
            info = self.rpc(url, "getblockchaininfo")
            if (info["blocks"], info["bestblockhash"]) == (height, block_hash):
                return
            if info["blocks"] < height or self.rpc(url, "getblockhash", [height]) != block_hash:
                raise ValueError("tip moved to a different chain during reorg observation")
        if previous is None:
            confirm_tip()
            self.state[state_key] = {
                "startedAt": now, "lastObservedAt": now, "height": height,
                "hash": block_hash, "events": [],
            }
            return {"reorgs24h": None, "since": now}
        times = [previous["startedAt"], previous["lastObservedAt"], *previous["events"]]
        if (len(previous["events"]) > 10000
                or any(type(t) not in (int, float) or not 0 < t <= now for t in times)
                or previous["startedAt"] > previous["lastObservedAt"]
                or previous["events"] != sorted(previous["events"])
                or any(t < previous["startedAt"] or t > previous["lastObservedAt"] for t in previous["events"])
                or type(previous["height"]) is not int or previous["height"] <= 0
                or not re.fullmatch(r"[0-9a-f]{64}", previous["hash"])):
            raise ValueError("invalid persisted public reorg observations")
        cutoff = max(now - 86400, previous["startedAt"])
        events = [t for t in previous["events"] if t >= cutoff]
        changed = (height, block_hash) != (previous["height"], previous["hash"])
        if changed and height < previous["height"]:
            # A lower quorum tip can be a lagging source, not a chain replacement.
            # Keep the higher baseline until its ancestry can be checked again.
            confirm_tip()
            return {"reorgs24h": None, "since": cutoff}
        if changed:
            replaced = False
            if not replaced:
                ancestor = self.rpc(url, "getblockhash", [previous["height"]])
                if not isinstance(ancestor, str) or not re.fullmatch(r"[0-9a-f]{64}", ancestor):
                    raise ValueError("previous public tip ancestry unavailable")
                replaced = ancestor != previous["hash"]
            if replaced:
                if len(events) >= 10000:
                    raise ValueError("public reorg observation capacity exceeded")
                events.append(now)
        confirm_tip()
        self.state[state_key] = {
            "startedAt": previous["startedAt"], "lastObservedAt": now,
            "height": height, "hash": block_hash, "events": events,
        }
        return {"reorgs24h": len(events), "since": cutoff}

    def public_status(self, nodes, agreement, now, profile="public-testnet"):
        available = [n for n in nodes if n.get("fresh") and n.get("active")]
        if agreement:
            available = [n for n in available if n["name"] in agreement["nodes"]]
        # A second validator must know the exact tip we publish, not just a
        # lower common ancestor shared with an ahead or divergent node.
        primary = None
        for candidate in sorted(available, key=lambda n: n["height"], reverse=True):
            for peer in available:
                if peer["name"] == candidate["name"] or peer["height"] < candidate["height"]:
                    continue
                try:
                    if self.source_hash(peer, candidate["height"]) == candidate["hash"]:
                        primary = candidate
                        break
                except (OSError, ValueError, KeyError, TypeError):
                    continue
            if primary is not None:
                break
        if primary is None:
            raise ValueError("public chain quorum unavailable")
        parameters = self.consolidated_rules(primary, profile)
        height = primary["height"]
        manifest = self.settings(profile)["manifest"]
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
        if info["blocks"] < height or self.rpc(primary["url"], "getblockhash", [height]) != primary["hash"]:
            raise ValueError("sampled block left the current chain during collection")
        balance = primary.get("nsmValueBalanceZat")
        gap = max(0, headers[-1]["time"] - headers[-2]["time"]) if len(headers) > 1 else None
        current_rules = parameters["atTip"] if parameters else None
        threshold = current_rules["minimumDifficulty"]["thresholdSeconds"] if current_rules else None
        limit = current_rules["powLimitCompact"] if current_rules else None
        bits = tip.get("bits")
        reorgs = (self.record_public_reorgs(height, primary["hash"], primary["url"], now, profile)
                  if agreement else {"reorgs24h": None, "since": now})
        status = {
            "schemaVersion": 1, "observedAt": now, "status": "live" if agreement else "degraded",
            "network": {**manifest["network"],
                        "targetSpacingSeconds": current_rules["targetSpacingSeconds"] if current_rules else manifest["network"].get("targetSpacingSeconds"),
                        "daaWindowBlocks": current_rules["daaWindowBlocks"] if current_rules else manifest["network"].get("daaWindowBlocks"),
                        "reissuanceHeight": current_rules["nsmReissuanceHeight"] if current_rules else None,
                        "reissuanceKnown": current_rules is not None},
            "chain": {"height": height, "hash": primary["hash"], "blockTime": tip["time"],
                      "tipAgeSeconds": max(0, int(now - tip["time"])), "difficulty": tip.get("difficulty"),
                      "meanIntervalSeconds": round(statistics.mean(intervals), 1) if intervals else None,
                      "medianIntervalSeconds": round(statistics.median(intervals), 1) if intervals else None,
                      "intervalSampleBlocks": len(intervals),
                      "timestampGapSeconds": gap,
                      "minimumDifficultyEligible": (gap > threshold if threshold is not None else False) if gap is not None and current_rules else None,
                      "minimumDifficultyBlock": False if current_rules and threshold is None else str(bits).lower().removeprefix("0x") == str(limit).lower().removeprefix("0x")
                      if bits is not None and limit is not None else None},
            "nsm": {"balanceZat": balance, "available": balance is not None, "seedZat": None},
            "observation": {"validatorsConfigured": 3, "validatorsAgree": bool(agreement and len(agreement["nodes"]) == 3),
                            "localNodesAgree": bool(agreement), "validatorsAgreeing": len(agreement["nodes"]) if agreement else 0,
                            "blocks24h": None, "reorgRate24h": None, **reorgs,
                            "scope": "Three " + manifest["network"]["name"] + " validators and independent reference; reorgs are sampled canonical-chain replacements"},
            "mining": {"operatorMinersActive": 0, "operatorMinersConfigured": 0, "remoteMiners": []},
            "nodes": [{"name": n["name"], "healthy": bool(agreement and n["name"] in agreement["nodes"]),
                       **({"height": n["height"], "hash": n["hash"]} if n.get("height") is not None else {})}
                      for n in nodes],
            "recentBlocks": [{"height": header["height"], "hash": header["hash"],
                              "time": header["time"], "difficulty": header.get("difficulty")}
                             for header in reversed(headers[-8:])],
        }
        return status, parameters

    def envelope(self, profile, status, parameter_rules, now):
        settings = self.settings(profile)
        body = {"schemaVersion": 1, "selectedProfile": profile,
                "selectionState": self.state["selectionState"], "generatedAt": now,
                "status": status, "network": settings["manifest"], "rules": parameter_rules,
                "capabilities": settings.get("capabilities", {"faucet": None, "snapshot": None})}
        if profile != "staging" and parameter_rules:
            body["rulesSource"] = {
                "kind": "protocol-reference-and-node-rpcs",
                "protocolRevision": self.reference_parameters(profile)["sourceRevision"],
                "protocolUrl": self.reference_parameters(profile)["sourceUrl"],
                "liveFields": {"buildVersion": "getinfo", "baseSubsidyZat": "getblocksubsidy",
                               "branchId": "getblockchaininfo", "effectiveHeight": "getblockchaininfo"},
            }
        body["generation"] = hashlib.sha256(json.dumps(body, sort_keys=True, allow_nan=False).encode()).hexdigest()
        return body

    def dispatch(self):
        """Idempotent notification; failed dispatch is retried on later polls."""
        if (self.state["selectedProfile"] == "staging" or self.state.get("publicationDispatchedFor") == self.state["selectedProfile"] or (self.state.get("publicationDispatched") and self.state["selectedProfile"] == "public-testnet") or not self.config.get("dispatch", False)
                or not self.state.get("lastPublicEnvelope")):
            return
        token = os.environ.get("NU7_DISPATCH_TOKEN")
        if not token:
            logging.warning("NU7 publication credential unavailable")
            return
        try:
            request = urllib.request.Request("https://api.github.com/repos/zakura-core/website/dispatches",
                data=json.dumps({"event_type": "nu7_activated", "client_payload": {
                    "selectedProfile": self.state["selectedProfile"], "selectionId": self.state["selectionId"]}}).encode(),
                headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
                         "Content-Type": "application/json"})
            with urllib.request.urlopen(request, timeout=RPC_TIMEOUT) as response:
                if response.status != 204:
                    raise ValueError("publication dispatch rejected")
            self.state["publicationDispatchedFor"] = self.state["selectedProfile"]
        except (OSError, ValueError):
            logging.warning("NU7 publication dispatch failed; retry retained")

    def collect(self, profile, now):
        settings = self.settings(profile)
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(self.observe, node, now, True, profile) for node in settings["nodes"]]
            reference_future = executor.submit(self.observe, settings["reference"], now, False, profile)
            nodes = [f.result() for f in futures]
            reference = reference_future.result()
        return nodes, self.agreement(nodes, reference, self.clock(), profile)

    def select(self, profile, evidence, now):
        self.state.update(selectedProfile=profile, selectionState="selected", selectedAt=now,
                          selectionEvidence=evidence,
                          selectionId=hashlib.sha256(json.dumps({"profile": profile, "evidence": evidence}, sort_keys=True).encode()).hexdigest())
        # Observations and reorg baselines from another network must never become
        # the fallback for the newly selected network.
        self.state.pop("lastPublicEnvelope", None)
        self.headers.clear()
        self.last_tips.clear()

    def poll(self):
        now = self.clock()
        selected = self.state["selectedProfile"]
        prepared_mainnet = None
        observation_profile = "mainnet" if selected == "mainnet" else "public-testnet"
        nodes, agreement = self.collect(observation_profile, now)
        if selected == "staging" and self.config["armed"]:
            self.state["selectionState"] = "armed"
            self.state["consecutivePasses"] = self.state["consecutivePasses"] + 1 if agreement else 0
            if self.state["consecutivePasses"] >= 3:
                self.select("public-testnet", agreement, now)
        if self.state["selectedProfile"] == "public-testnet" and (self.config.get("mainnet") or {}).get("armed", False):
            mainnet_nodes, mainnet_agreement = self.collect("mainnet", now)
            ready = bool(mainnet_agreement and self.reference_parameters("mainnet"))
            # An activation switch must have the complete matching parameter table,
            # not merely a reachable Mainnet endpoint.
            if ready:
                try:
                    mainnet_status, parameters = self.public_status(mainnet_nodes, mainnet_agreement, now, "mainnet")
                    ready = parameters is not None and 0 <= self.clock() - now <= 60
                except (OSError, ValueError, KeyError, TypeError):
                    ready = False
            self.state["mainnetConsecutivePasses"] = self.state.get("mainnetConsecutivePasses", 0) + 1 if ready else 0
            if self.state["mainnetConsecutivePasses"] >= 3:
                self.select("mainnet", mainnet_agreement, now)
                nodes, agreement = mainnet_nodes, mainnet_agreement
                prepared_mainnet = (mainnet_status, parameters)
        profile = self.state["selectedProfile"]
        try:
            if profile != "staging":
                status, parameters = prepared_mainnet or self.public_status(nodes, agreement, now, profile)
            else:
                status = (self.staging_status.response()[1] if self.staging_status else
                          read_json(self.config["staging"]["statusUrl"]))
                parameters = None
            payload = self.envelope(profile, status, parameters, now)
        except (OSError, ValueError, KeyError, TypeError):
            previous = self.state.get("lastPublicEnvelope") if profile != "staging" else None
            if previous:
                payload = copy.deepcopy(previous)
                payload["status"]["status"] = "unavailable"
                payload["status"]["error"] = "Selected network observation unavailable; showing last verified generation"
                payload["generation"] = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
            else:
                payload = self.envelope(profile, {"schemaVersion": 1, "status": "unavailable",
                    "observedAt": now, "error": "Selected network observation unavailable"}, None, now)
        if profile != "staging":
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
