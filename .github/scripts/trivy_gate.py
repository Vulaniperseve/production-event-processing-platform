"""
Turn a Trivy JSON report into a pass/fail gate with readable output.

Each blocking finding becomes a GitHub annotation (shown on the run page)
and a row in the job summary, so you can see *what* blocked a release
without digging through logs.
"""

import json
import os
import sys


def blocking(report):
    """CRITICAL vulnerabilities that have a fixed version, one per (package, id)."""
    seen = {}
    for result in report.get("Results") or []:
        for v in result.get("Vulnerabilities") or []:
            if v.get("Severity") == "CRITICAL" and v.get("FixedVersion"):
                key = (v["PkgName"], v["VulnerabilityID"])
                seen.setdefault(key, {
                    "id": v["VulnerabilityID"], "package": v["PkgName"],
                    "installed": v.get("InstalledVersion", "?"), "fixed": v["FixedVersion"],
                    "where": result.get("Target", ""), "title": (v.get("Title") or "")[:120],
                })
    return sorted(seen.values(), key=lambda f: (f["package"], f["id"]))


def main(path):
    findings = blocking(json.load(open(path, encoding="utf-8")))
    lines = ["| Vulnerability | Package | Installed | Fixed in | Where |", "|---|---|---|---|---|"]
    for f in findings:
        print(f"::error title={f['id']} in {f['package']}::{f['package']} {f['installed']} -> "
              f"fixed in {f['fixed']} ({f['where']}) {f['title']}")
        lines.append(f"| {f['id']} | {f['package']} | {f['installed']} | {f['fixed']} | {f['where']} |")
    summary = os.getenv("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as out:
            out.write(f"### Trivy: {len(findings)} fixable CRITICAL vulnerabilities\n\n")
            if findings:
                out.write("\n".join(lines) + "\n")
    print(f"{len(findings)} fixable CRITICAL vulnerabilities")
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
