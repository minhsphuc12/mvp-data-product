"""
Prefect flow: run batch seed + dbt transform (demo pipeline).

Install: pip install -r requirements-orchestration.txt
Run from repo root: python orchestration/refresh_flow.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _run_make(target: str) -> None:
    subprocess.run(["make", "-C", str(ROOT), target], check=True)


def main() -> None:
    try:
        from prefect import flow, task
    except ImportError:
        print(
            "Install Prefect: pip install -r requirements-orchestration.txt",
            file=sys.stderr,
        )
        sys.exit(1)

    @task(name="seed-data")
    def task_seed() -> None:
        _run_make("seed-data")

    @task(name="transform")
    def task_transform() -> None:
        _run_make("transform")

    @flow(name="finance-demo-refresh")
    def refresh_flow() -> None:
        task_seed()
        task_transform()

    refresh_flow()


if __name__ == "__main__":
    main()
