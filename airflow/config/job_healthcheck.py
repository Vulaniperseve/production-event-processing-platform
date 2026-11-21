"""
Lightweight Docker health check for Airflow's dag-processor and triggerer.

Does what `airflow jobs check --job-type <type> --hostname $HOSTNAME` does,
by asking the metadata database directly: is there a running job of this type
on this host that has sent a heartbeat recently?

Why not the Airflow CLI: booting it takes 30+ s of CPU on a laptop, and longer
than the check's timeout under load. Docker doesn't kill a timed-out check, so
the slow checks piled up (about 20 CLI processes per container) and starved the
very services they were checking. This script needs only psycopg2 and finishes
in well under a second.

Usage: python job_healthcheck.py DagProcessorJob|TriggererJob
"""

import os
import socket
import sys

import psycopg2

# Airflow heartbeats every few seconds; allow for a busy machine.
MAX_HEARTBEAT_AGE = "3 minutes"


def main(job_type):
    dsn = os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"].replace("+psycopg2", "")
    hostname = os.environ.get("HOSTNAME") or socket.getfqdn()
    with psycopg2.connect(dsn, connect_timeout=5) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM job WHERE job_type = %s AND hostname = %s AND state = 'running' "
            "AND latest_heartbeat > now() - %s::interval LIMIT 1",
            (job_type, hostname, MAX_HEARTBEAT_AGE),
        )
        alive = cur.fetchone() is not None
    print(f"{job_type} on {hostname}: {'alive' if alive else 'no recent heartbeat'}")
    return 0 if alive else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
