"""
Generate Airflow's internal secrets into airflow/.env (never committed).

    python scripts/generate_secrets.py            # add any that are missing
    python scripts/generate_secrets.py --rotate   # replace all of them

* AIRFLOW_FERNET_KEY   encrypts connections and variables Airflow stores
* AIRFLOW_SECRET_KEY   signs web sessions
* AIRFLOW_JWT_SECRET   signs REST API tokens

Only the variable names are printed, never the values.
Rotating logs everyone out and invalidates API tokens (clients log in again).
"""

import argparse
import base64
import os
import secrets
from pathlib import Path

ENV_FILE = Path(__file__).resolve().parents[1] / "airflow" / ".env"

GENERATORS = {
    # Fernet keys are 32 random bytes, url-safe base64 encoded.
    "AIRFLOW_FERNET_KEY": lambda: base64.urlsafe_b64encode(os.urandom(32)).decode(),
    "AIRFLOW_SECRET_KEY": lambda: secrets.token_urlsafe(32),
    "AIRFLOW_JWT_SECRET": lambda: secrets.token_urlsafe(48),
}


def update_env(text, rotate=False):
    """Return (new_text, changed_names). Keeps every other line as it is."""
    lines = text.splitlines()
    present = {line.split("=", 1)[0].strip() for line in lines if "=" in line and not line.lstrip().startswith("#")}
    changed = []
    out = []
    for line in lines:
        name = line.split("=", 1)[0].strip() if "=" in line else None
        if rotate and name in GENERATORS:
            out.append(f"{name}={GENERATORS[name]()}")
            changed.append(name)
        else:
            out.append(line)
    missing = [n for n in GENERATORS if n not in present]
    if missing:
        out.append("# Airflow internal secrets (scripts/generate_secrets.py)")
        for name in missing:
            out.append(f"{name}={GENERATORS[name]()}")
            changed.append(name)
    return "\n".join(out) + "\n", changed


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--rotate", action="store_true", help="replace existing secrets")
    parser.add_argument("--env-file", type=Path, default=ENV_FILE)
    args = parser.parse_args()

    text = args.env_file.read_text(encoding="utf-8") if args.env_file.exists() else ""
    new_text, changed = update_env(text, rotate=args.rotate)
    if changed:
        args.env_file.write_text(new_text, encoding="utf-8")
        print(f"{'Rotated' if args.rotate else 'Generated'} in {args.env_file}: {', '.join(changed)}")
    else:
        print("All secrets already present (use --rotate to replace them).")


if __name__ == "__main__":
    main()
