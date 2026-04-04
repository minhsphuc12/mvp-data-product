#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ ! -f .env ]]; then
  echo "Copy .env.example to .env and adjust if needed."
  exit 1
fi

pyver="$(python3 -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")')"
case "$pyver" in
  3.11|3.12|3.13) ;;
  *)
    echo "Warning: Python $pyver detected. dbt-core officially supports 3.11–3.13; 3.14+ may fail to install/run."
    ;;
esac

python3 -m venv .venv
# shellcheck disable=SC1091
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt

if [[ ! -f dbt_project/profiles.yml ]]; then
  cp dbt_project/profiles.yml.example dbt_project/profiles.yml
  echo "Created dbt_project/profiles.yml from example."
fi

echo "Bootstrap complete. Activate venv: source .venv/bin/activate"
