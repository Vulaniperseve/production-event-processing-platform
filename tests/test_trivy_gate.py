import importlib.util
import json
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "trivy_gate", Path(__file__).resolve().parents[1] / ".github" / "scripts" / "trivy_gate.py")
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


def vuln(vid, pkg, severity="CRITICAL", fixed="2.0"):
    return {"VulnerabilityID": vid, "PkgName": pkg, "Severity": severity,
            "InstalledVersion": "1.0", "FixedVersion": fixed, "Title": "bad"}


REPORT = {"Results": [
    {"Target": "debian", "Vulnerabilities": [
        vuln("CVE-1", "openssl"), vuln("CVE-2", "zlib", severity="HIGH"), vuln("CVE-3", "libx", fixed="")]},
    {"Target": "python-pkg", "Vulnerabilities": [vuln("CVE-1", "openssl"), vuln("CVE-4", "jinja2")]},
    {"Target": "empty", "Vulnerabilities": None},
]}


def test_only_fixable_criticals_block_once_each():
    assert [(f["package"], f["id"]) for f in gate.blocking(REPORT)] == [("jinja2", "CVE-4"), ("openssl", "CVE-1")]


def test_gate_exit_codes_and_annotations(tmp_path, capsys, monkeypatch):
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    bad, good = tmp_path / "bad.json", tmp_path / "good.json"
    bad.write_text(json.dumps(REPORT))
    good.write_text(json.dumps({"Results": []}))

    assert gate.main(str(bad)) == 1
    out = capsys.readouterr().out
    assert "::error title=CVE-4 in jinja2::" in out
    assert "| CVE-1 | openssl |" in summary.read_text()
    assert gate.main(str(good)) == 0
