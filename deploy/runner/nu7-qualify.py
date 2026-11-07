"""Read-only readiness receipt; does not acquire or mutate selector state."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import sys
import time

from nu7_activation import ActivationFeed, rpc


def qualify(config, profile, expected_revision=None):
    """Check source identity, expected builds, and a common chain hash.

    Preparation can succeed before NU7 activates. Actual activation remains the
    persistent selector's decision, after its consecutive observations.
    """
    settings = config["mainnet" if profile == "mainnet" else "public"]
    revision = expected_revision or settings["manifest"]["nodeRevision"]
    # Observation does not need the state-owning constructor or its process lock.
    feed = object.__new__(ActivationFeed)
    feed.config, feed.rpc, feed.clock, feed.progress = config, rpc, time.time, {}
    nodes = [feed.observe(node, time.time(), True, profile) for node in settings["nodes"]]
    sources = list(nodes)
    receipt = {"checkedAt": time.time(), "profile": profile,
        "armed": settings["armed"] if profile == "mainnet" else config["armed"],
        "dispatch": config["dispatch"],
        "nodeSources": [{k: n[k] for k in ("name", "fresh", "active", "height", "error", "sourceRevision", "buildVersion") if k in n} for n in nodes],
        "manifestRevision": revision}
    if profile == "public-testnet":
        reference_source = settings["reference"]
        reference = feed.observe(reference_source, time.time(), False, profile)
        sources.append(reference)
        receipt.update(referenceIdentityVerified=reference.get("fresh", False),
            referenceHeight=reference.get("height"), referenceNu7Active=reference.get("active"),
            referenceNu7ScheduleVerified=False,
            referenceTransport=reference_source.get("adapter", "json-rpc"))
        if reference_source.get("adapter") == "grpcurl":
            receipt["grpcurlSha256"] = hashlib.sha256(Path(reference_source["grpcurlPath"]).read_bytes()).hexdigest()
        elif reference.get("fresh"):
            info = rpc(reference_source["rpcUrl"], "getinfo")
            build = info.get("build", info.get("subversion"))
            protocol = info.get("protocolversion")
            receipt.update(referenceBuildVersion=build, referenceProtocolVersion=protocol)
            receipt["referenceNu7ScheduleVerified"] = bool(
                re.search(r"(?:^v|/Zebra:)7\.", str(build)) and type(protocol) is int and protocol >= 170180)
    receipt["preparedChainAgreement"] = False
    if nodes and all(n.get("fresh") for n in sources) and (profile != "mainnet" or len(nodes) == 3):
        common = min(n["height"] for n in sources)
        hashes = [feed.source_hash(n, common) for n in sources]
        receipt.update(commonHeight=common, commonHash=hashes[0], preparedChainAgreement=len(set(hashes)) == 1)
    if profile == "public-testnet":
        receipt["activationReferencePrepared"] = bool(receipt["preparedChainAgreement"] and receipt["referenceNu7ScheduleVerified"])
    receipt["manifestRevisionMatches"] = bool(nodes) and all(
        revision[:9] in str(n.get("buildVersion", "")).lower() for n in nodes)
    # A rolling release can legitimately differ from the published joining build.
    # Explicit source pins retain build verification during that transition.
    receipt["sourceRevisionsMatch"] = bool(nodes) and all(
        re.fullmatch(r"[0-9a-f]{40}", expected_revision or source.get("expectedRevision", revision))
        and (expected_revision or source.get("expectedRevision", revision))[:9] in str(node.get("buildVersion", "")).lower()
        for source, node in zip(settings["nodes"], nodes))
    receipt["prepared"] = bool(receipt["preparedChainAgreement"] and
        (not expected_revision or receipt["manifestRevisionMatches"]) and
        (profile != "mainnet" or receipt["sourceRevisionsMatch"]))
    return receipt


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("/etc/zakura/nu7-activation.json"))
    parser.add_argument("--profile", choices=("public-testnet", "mainnet"), default="public-testnet")
    parser.add_argument("--expected-revision", help="approved candidate SHA to qualify before updating the runtime manifest")
    args = parser.parse_args(argv)
    if args.expected_revision and not re.fullmatch(r"[0-9a-f]{40}", args.expected_revision):
        parser.error("--expected-revision requires a full source SHA")
    try:
        receipt = qualify(json.loads(args.config.read_text()), args.profile, args.expected_revision)
    except (OSError, ValueError, KeyError, TypeError):
        print("NU7 qualification failed: configuration or source unavailable", file=sys.stderr)
        return 1
    print(json.dumps(receipt, indent=2))
    return 0 if receipt["prepared"] else 1


if __name__ == "__main__":
    sys.exit(main())
