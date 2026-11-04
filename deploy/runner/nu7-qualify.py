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
    """Check identity, reference implementation, and a common chain hash.

    Preparation can succeed before NU7 activates. Actual activation remains the
    persistent selector's decision, after its consecutive observations.
    """
    settings = config["mainnet" if profile == "mainnet" else "public"]
    revision = expected_revision or settings["manifest"]["nodeRevision"]
    # Observation does not need the state-owning constructor or its process lock.
    feed = object.__new__(ActivationFeed)
    feed.config, feed.rpc, feed.clock, feed.progress = config, rpc, time.time, {}
    reference_source = settings["reference"]
    reference = feed.observe(reference_source, time.time(), False, profile)
    nodes = [feed.observe(node, time.time(), True, profile) for node in settings["nodes"]]
    receipt = {"checkedAt": time.time(), "profile": profile,
        "armed": settings["armed"] if profile == "mainnet" else config["armed"],
        "dispatch": config["dispatch"],
        "referenceIdentityVerified": reference.get("fresh", False),
        "referenceHeight": reference.get("height"), "referenceNu7Active": reference.get("active"),
        "nodeSources": [{k: n[k] for k in ("name", "fresh", "height", "error", "sourceRevision", "buildVersion") if k in n} for n in nodes],
        "manifestRevision": revision, "referenceNu7ScheduleVerified": False,
        "referenceTransport": reference_source.get("adapter", "json-rpc")}
    if reference_source.get("adapter") == "grpcurl":
        receipt["grpcurlSha256"] = hashlib.sha256(Path(reference_source["grpcurlPath"]).read_bytes()).hexdigest()
    elif reference.get("fresh"):
        info = rpc(reference_source["rpcUrl"], "getinfo")
        build = info.get("build", info.get("subversion"))
        protocol = info.get("protocolversion")
        receipt.update(referenceBuildVersion=build, referenceProtocolVersion=protocol)
        expected_build = reference_source.get("expectedBuild")
        implementation_matches = (build == expected_build if profile == "mainnet" else
                                  bool(re.search(r"(?:^v|/Zebra:)7\.", str(build))))
        receipt["referenceNu7ScheduleVerified"] = bool(
            implementation_matches and type(protocol) is int
            and protocol >= reference_source.get("minimumProtocolVersion", 170180)
            and (profile != "mainnet" or expected_build))
    receipt["preparedChainAgreement"] = False
    if reference.get("fresh") and nodes and all(n.get("fresh") for n in nodes):
        sources = nodes + [reference]
        common = min(n["height"] for n in sources)
        hashes = [feed.source_hash(n, common) for n in sources]
        receipt.update(commonHeight=common, commonHash=hashes[0], preparedChainAgreement=len(set(hashes)) == 1)
    receipt["activationReferencePrepared"] = bool(receipt["preparedChainAgreement"] and receipt["referenceNu7ScheduleVerified"])
    receipt["manifestRevisionMatches"] = bool(nodes) and all(
        revision[:9] in str(n.get("buildVersion", "")).lower() for n in nodes)
    # A rolling release can legitimately differ from the published joining build.
    # Explicit source pins retain build verification during that transition.
    receipt["sourceRevisionsMatch"] = bool(nodes) and all(
        re.fullmatch(r"[0-9a-f]{40}", source.get("expectedRevision", revision))
        and source.get("expectedRevision", revision)[:9] in str(node.get("buildVersion", "")).lower()
        for source, node in zip(settings["nodes"], nodes))
    receipt["prepared"] = bool(receipt["preparedChainAgreement"] and
        (not expected_revision or receipt["manifestRevisionMatches"]) and
        (profile != "mainnet" or receipt["activationReferencePrepared"] and receipt["sourceRevisionsMatch"]))
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
