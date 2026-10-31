"""Resolve a release PR, review immutable Git objects, and publish saved evidence."""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

from .config import load_settings
from .contracts import ReviewError
from .engine import review
from .github import GitHub, fetch_head, publish, resolve_request
from .models import AbliteratedProvider, JsonTransport, OpenAIProvider
from .reporting import render, write_report
from .repository import Repository


def prepare(args) -> int:
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    context = resolve_request(GitHub(os.environ["GITHUB_REPOSITORY"]), os.environ["GITHUB_EVENT_NAME"], event)
    with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
        output.write(f"eligible={'true' if context else 'false'}\n")
        if context:
            output.write(f"pr_number={context['pr_number']}\nprovider={context['provider']}\n")
    if context:
        context.update(run_number=int(os.environ["GITHUB_RUN_NUMBER"]),
                       run_attempt=int(os.environ["GITHUB_RUN_ATTEMPT"]),
                       run_url=f"https://github.com/{context['repository']}/actions/runs/{os.environ['GITHUB_RUN_ID']}")
        args.output.mkdir(parents=True, exist_ok=True)
        (args.output / "context.json").write_text(json.dumps(context))
        print(f"Resolved release PR #{context['pr_number']} at {context['head_sha']}")
    else:
        print("No eligible current release PR for this event")
    return 0


def run_review(args) -> int:
    context = json.loads(args.context.read_text())
    report = {key: value for key, value in context.items() if key != "pr_body"}
    report.update(status="incomplete", findings=[], errors=[], coverage=[], synthesis_complete=False)
    start = time.monotonic()
    repository = Repository(args.repo)
    try:
        report["control_sha"] = repository.resolve("HEAD")
        write_report(args.output, report)
        fetch_head(args.repo, context)
        if repository.resolve("refs/release-review/head") != context["head_sha"]:
            raise ReviewError("PR head changed before source fetch; rerun the review")
        report.update(repository.pin(context["head_sha"], context["pr_body"], context["base_override"]))
        settings = load_settings(args.config, context["provider"], context["profile"],
                                 os.environ.get("RELEASE_REVIEW_MODELS", ""))
        key = os.environ.get("REVIEW_API_KEY", "")
        if not key:
            raise ReviewError("selected provider API key is not configured in GitHub secrets")
        provider_type = OpenAIProvider if settings.provider == "openai" else AbliteratedProvider
        report["pricing"] = {"input_usd_per_million": str(settings.input_price),
                             "output_usd_per_million": str(settings.output_price),
                             "cached_input_usd_per_million": (str(settings.cached_input_price)
                                                              if settings.cached_input_price is not None else None)}
        prior = json.loads(args.resume_report.read_text()) if args.resume_report else None
        review(repository, provider_type(settings, key, JsonTransport()), settings, report,
               lambda current: write_report(args.output, current), prior_report=prior)
    except (ReviewError, OSError, ValueError, KeyError, TypeError) as error:
        # Only our domain errors are safe to publish. Exceptions from I/O or malformed
        # external envelopes can contain credential-bearing URLs or provider payloads.
        report["errors"].append(str(error) if isinstance(error, ReviewError) else
                                f"review infrastructure failed ({type(error).__name__}); inspect configuration")
        report["status"] = "incomplete"
    finally:
        report["duration_seconds"] = round(time.monotonic() - start, 2)
        write_report(args.output, report)
    print(f"Review {report['status']}; report saved to {args.output}")
    return 0 if report["status"] == "complete" else 2


def publish_report(args) -> int:
    report_path = args.output / "report.json"
    if report_path.exists():
        report = json.loads(report_path.read_text())
    else:
        context = json.loads(args.context.read_text())
        report = {key: value for key, value in context.items() if key != "pr_body"}
        report.update(status="incomplete", findings=[], coverage=[],
                      errors=["review job stopped before producing a report"], synthesis_complete=False)
        write_report(args.output, report)
    if report["repository"] != os.environ["GITHUB_REPOSITORY"]:
        raise ReviewError("report repository does not match this workflow")
    markdown = render(report, comment=True)
    outcome = publish(GitHub(report["repository"]), report, markdown)
    write_report(args.output, report)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as summary:
            summary.write(markdown + "\nPublication: " + outcome + "\n")
            if report.get("comment_url"):
                summary.write(f"\n[Verified PR summary comment]({report['comment_url']})\n")
    print(outcome)
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["prepare", "review", "publish"])
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--config", type=Path, default=Path(".github/release-review"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--context", type=Path)
    parser.add_argument("--resume-report", type=Path,
                        help="trusted incomplete report to continue under its existing API budget")
    args = parser.parse_args(argv)
    if args.command != "prepare" and args.context is None:
        parser.error("--context is required for review and publish")
    if args.resume_report is not None and args.command != "review":
        parser.error("--resume-report is only valid for review")
    try:
        return {"prepare": prepare, "review": run_review, "publish": publish_report}[args.command](args)
    except ReviewError as error:
        print("Release review incomplete: " + str(error))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
