#!/usr/bin/env python3
"""Break recent CI failures down by root cause.

Rather than counting every red run as "flaky", this pulls the log of the
first failed job of each sampled failed run (through the ``gh`` CLI) and
files it under one category:

  no_free_workers   no worker had room for an Actor (capacity in tests)
  e2e_timeout       e2e waits that timed out; upstream connect errors
  storage_access    object-store access denied / bucket missing
  license_check     the license check reported a problem
  named_test_fail   a specific Rust test failed ("test path::name ... FAILED")
  verify_fail       another scripts/verify check failed
  log_fetch_failed  the log could not be fetched (expired, rate-limited)
  unknown           fetched but unclassified

Example:
  scripts/metrics/ci_failures.py --repo OWNER/NAME --workflow ci --sample 30 --save out.json
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from collections import Counter, defaultdict
from typing import Callable, Optional

# First match wins. Infrastructure causes come first so a run that crashed on
# capacity is not blamed on whichever test happened to print a failure;
# named failures beat the generic verify bucket because they are more useful.
PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("no_free_workers", re.compile(r"no worker has room for the actor|no free workers available", re.I)),
    ("e2e_timeout", re.compile(
        r"timed out waiting for actor response|timed out waiting for the condition"
        r"|connection timeout|upstream connect error", re.I)),
    ("storage_access", re.compile(r"AccessDenied|NoSuchBucket|storage\.objects\.get", re.I)),
    ("license_check", re.compile(r"verify/licenses\.sh.*(failed|error)|license.*not allowed|rejected license", re.I)),
    ("named_test_fail", re.compile(r"^test (\S+) \.\.\. FAILED$", re.M)),
    ("verify_fail", re.compile(r"^FAILED: scripts/verify/\S+|Verification failed", re.I | re.M)),
]
NAMED_TEST = dict(PATTERNS)["named_test_fail"]

LABELS = {
    "no_free_workers": "No free workers        (test capacity)",
    "e2e_timeout": "E2e timeout            (waits; upstream connect)",
    "storage_access": "Object store access    (credentials / IAM)",
    "license_check": "License check          (scripts/verify/licenses.sh)",
    "verify_fail": "Other verify failure   (scripts/verify/*)",
    "named_test_fail": "Named test failure     (test ... FAILED)",
    "log_fetch_failed": "Log fetch failed       (expired / rate-limited)",
    "unknown": "Unclassified",
}

ANSI = re.compile(r"\x1b\[[0-9;]*m")
TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T[\d:.]+Z ", re.M)


def clean_log(text: str) -> str:
    """Strip colour codes and per-line timestamps from a job log."""
    return TIMESTAMP.sub("", ANSI.sub("", text))


def classify(log: str) -> tuple[str, Optional[str]]:
    """(category, detail); detail is the first failing test for named_test_fail."""
    for category, pattern in PATTERNS:
        m = pattern.search(log)
        if m:
            return category, (m.group(1) if category == "named_test_fail" else None)
    return "unknown", None


class Gh:
    """Thin wrapper over the gh CLI; swapped out in tests."""

    def __init__(self, run: Callable[..., subprocess.CompletedProcess] = subprocess.run):
        self._run = run

    def json(self, *args: str):
        r = self._run(["gh", *args], capture_output=True, text=True)
        if r.returncode != 0:
            raise RuntimeError(r.stderr.strip())
        return json.loads(r.stdout)

    def text(self, *args: str) -> Optional[str]:
        r = self._run(["gh", *args], capture_output=True, text=True)
        return clean_log(r.stdout) if r.returncode == 0 else None


def analyse(gh: Gh, repo: str, workflow: str, fetch_limit: int, sample: int) -> dict:
    runs = gh.json("run", "list", "--repo", repo, "--workflow", workflow, "--limit", str(fetch_limit),
                   "--json", "databaseId,conclusion,event,headBranch,createdAt")
    failed = [r for r in runs if r.get("conclusion") == "failure"][:sample]
    print(f"{len(failed)} failed runs sampled", file=sys.stderr)

    counts: Counter = Counter()
    tests: Counter = Counter()
    by_category: dict[str, list] = defaultdict(list)
    raw = []
    for i, run in enumerate(failed, 1):
        rid, branch, event = run["databaseId"], run.get("headBranch"), run.get("event")
        print(f"  [{i}/{len(failed)}] run {rid}", file=sys.stderr)
        entry = {"run_id": rid, "branch": branch, "event": event, "job": None, "detail": None}
        try:
            jobs = gh.json("api", f"repos/{repo}/actions/runs/{rid}/jobs?per_page=100",
                           "--jq", '[.jobs[] | select(.conclusion=="failure") | {id,name}]')
        except RuntimeError:
            jobs = None
        if jobs is None:
            category = "log_fetch_failed"
        elif not jobs:
            category, entry["job"] = "unknown", "no_failed_job"
        else:
            # The first failed job is enough: one root cause usually cascades.
            job = jobs[0]
            entry["job"] = job["name"]
            log = gh.text("api", f"repos/{repo}/actions/jobs/{job['id']}/logs")
            if log is None:
                category = "log_fetch_failed"
            else:
                category, entry["detail"] = classify(log)
                if category == "named_test_fail":
                    tests.update(NAMED_TEST.findall(log))
        entry["category"] = category
        counts[category] += 1
        by_category[category].append(entry)
        raw.append(entry)

    return {
        "total_sampled": len(failed),
        "category_counts": dict(counts),
        "test_fail_counts": dict(tests),
        "by_category": dict(by_category),
        "raw": raw,
    }


def report(data: dict) -> str:
    n = data["total_sampled"] or 1
    lines = ["-" * 66, f"  CI failures by cause ({data['total_sampled']} most recent failed runs)", "-" * 66]
    for key, label in LABELS.items():
        c = data["category_counts"].get(key, 0)
        if c:
            pct = c / n * 100
            lines += [f"  {label}", f"    {c:3d} / {data['total_sampled']}  ({pct:4.0f}%)  {'#' * int(pct / 3)}"]
    lines.append("")
    if data["test_fail_counts"]:
        lines.append("  Failing tests:")
        for t, c in sorted(data["test_fail_counts"].items(), key=lambda kv: (-kv[1], kv[0])):
            lines.append(f"    {c}x  {t}")
    else:
        lines.append("  No named test failures in this sample.")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--repo", required=True, help="OWNER/NAME")
    p.add_argument("--workflow", default="ci")
    p.add_argument("--fetch-limit", type=int, default=300)
    p.add_argument("--sample", type=int, default=30, help="failed runs to fetch logs for")
    p.add_argument("--format", choices=["table", "json"], default="table")
    p.add_argument("--save", metavar="FILE")
    args = p.parse_args(argv)

    try:
        data = analyse(Gh(), args.repo, args.workflow, args.fetch_limit, args.sample)
    except RuntimeError as e:
        print(f"gh error: {e}", file=sys.stderr)
        return 1
    print(json.dumps(data, indent=2) if args.format == "json" else report(data), end="")
    if args.save:
        with open(args.save, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
