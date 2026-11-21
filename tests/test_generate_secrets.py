import base64
import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "generate_secrets", Path(__file__).resolve().parents[1] / "scripts" / "generate_secrets.py")
gs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gs)

EXISTING = "POSTGRES_USER=airflow\nTWELVE_DATA_API_KEY=abc\n"


def values(text):
    return dict(line.split("=", 1) for line in text.splitlines() if "=" in line and not line.startswith("#"))


def test_adds_missing_secrets_and_keeps_everything_else():
    text, changed = gs.update_env(EXISTING)
    v = values(text)
    assert set(changed) == set(gs.GENERATORS)
    assert v["POSTGRES_USER"] == "airflow" and v["TWELVE_DATA_API_KEY"] == "abc"
    assert len(base64.urlsafe_b64decode(v["AIRFLOW_FERNET_KEY"])) == 32     # valid Fernet key
    assert len(v["AIRFLOW_JWT_SECRET"]) >= 60


def test_never_overwrites_existing_secrets_without_rotate():
    first, _ = gs.update_env(EXISTING)
    second, changed = gs.update_env(first)
    assert changed == [] and second == first


def test_rotate_replaces_only_the_secrets():
    first, _ = gs.update_env(EXISTING)
    rotated, changed = gs.update_env(first, rotate=True)
    a, b = values(first), values(rotated)
    assert set(changed) == set(gs.GENERATORS)
    assert all(a[k] != b[k] for k in gs.GENERATORS)
    assert a["TWELVE_DATA_API_KEY"] == b["TWELVE_DATA_API_KEY"]


def test_cli_prints_names_not_values(tmp_path, capsys, monkeypatch):
    env = tmp_path / ".env"
    env.write_text(EXISTING)
    monkeypatch.setattr("sys.argv", ["generate_secrets.py", "--env-file", str(env)])
    gs.main()
    out = capsys.readouterr().out
    for value in [v for k, v in values(env.read_text()).items() if k in gs.GENERATORS]:
        assert value not in out
    assert "AIRFLOW_FERNET_KEY" in out
