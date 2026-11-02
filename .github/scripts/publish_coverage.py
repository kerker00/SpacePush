"""Publish a Shields endpoint on a data-only branch after successful main CI."""

import argparse
import base64
import json
import os
import re
import subprocess
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path


def endpoint(metrics, source_sha, repository, run_id):
    if not re.fullmatch(r"[0-9a-f]{40}", source_sha):
        raise ValueError("Invalid source commit")
    if metrics["sourceSha"] != source_sha or metrics["otp"] != "28":
        raise ValueError("Coverage must come from the matching OTP 28 CI run")
    if not re.fullmatch(r"[\w.-]+/[\w.-]+", repository) or not run_id.isdigit():
        raise ValueError("Invalid repository or run ID")
    modules = metrics["modules"]
    if not isinstance(modules, list) or not modules:
        raise ValueError("Application module coverage is missing")
    names = set()
    covered = uncovered = 0
    for module in modules:
        name = module["module"]
        if not isinstance(name, str) or not name.startswith("spacepush_") or name in names:
            raise ValueError("Invalid or duplicate application module")
        names.add(name)
        c, u = module["coveredLines"], module["uncoveredLines"]
        if type(c) is not int or type(u) is not int or min(c, u) < 0:
            raise ValueError("Invalid coverage counts")
        covered += c
        uncovered += u
    if (covered, uncovered) != (metrics["coveredLines"], metrics["uncoveredLines"]):
        raise ValueError("Module counts do not match the total")
    total = covered + uncovered
    if not total:
        raise ValueError("No executable application lines")
    percent = (Decimal(covered) * 100 / total).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
    if percent >= 90:
        color = "brightgreen"
    elif percent >= 80:
        color = "green"
    elif percent >= 70:
        color = "yellow"
    elif percent >= 50:
        color = "orange"
    else:
        color = "red"
    return {
        "schemaVersion": 1,
        "label": "EUnit coverage",
        "message": f"{percent}%",
        "color": color,
        "sourceSha": source_sha,
        "coveredLines": covered,
        "totalLines": total,
        "otp": "28",
        "runUrl": f"https://github.com/{repository}/actions/runs/{run_id}",
    }


def api(method, path, payload=None, allow_missing=False):
    command = ["gh", "api", "--method", method, path]
    if payload is not None:
        command += ["--input", "-"]
    result = subprocess.run(command, input=json.dumps(payload) if payload is not None else None,
                            text=True, capture_output=True, check=False)
    if result.returncode:
        if allow_missing and "(HTTP 404)" in result.stderr:
            return None
        raise RuntimeError(f"GitHub {method} {path} failed: {result.stderr.strip()}")
    return json.loads(result.stdout)


def publish(document, repository, source_sha):
    root = f"repos/{repository}"
    # Never let a completed older run replace metrics for a newer main commit.
    head = api("GET", f"{root}/git/ref/heads/main")["object"]["sha"]
    if head != source_sha:
        print("Skipping coverage publication: main has advanced.")
        return
    text = json.dumps(document, indent=2) + "\n"
    branch = api("GET", f"{root}/git/ref/heads/badges", allow_missing=True)
    if branch is None:
        tree = api("POST", f"{root}/git/trees", {"tree": [
            {"path": "coverage.json", "mode": "100644", "type": "blob", "content": text}
        ]})
        commit = api("POST", f"{root}/git/commits", {
            "message": f"Coverage for {source_sha[:12]}", "tree": tree["sha"], "parents": []
        })
        api("POST", f"{root}/git/refs", {"ref": "refs/heads/badges", "sha": commit["sha"]})
    else:
        current = api("GET", f"{root}/contents/coverage.json?ref=badges", allow_missing=True)
        if current and base64.b64decode(current["content"]).decode() == text:
            print("Coverage badge is already current.")
            return
        payload = {"message": f"Coverage for {source_sha[:12]}", "branch": "badges",
                   "content": base64.b64encode(text.encode()).decode()}
        if current:
            payload["sha"] = current["sha"]
        api("PUT", f"{root}/contents/coverage.json", payload)
    print(f"Published coverage for {source_sha} on badges.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("metrics", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    repository = os.environ["GITHUB_REPOSITORY"]
    source_sha = os.environ["GITHUB_SHA"]
    document = endpoint(json.loads(args.metrics.read_text()), source_sha, repository,
                        os.environ["GITHUB_RUN_ID"])
    if args.dry_run:
        print(json.dumps(document, indent=2))
    else:
        publish(document, repository, source_sha)


if __name__ == "__main__":
    main()
