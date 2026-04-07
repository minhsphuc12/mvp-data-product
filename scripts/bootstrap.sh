#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ ! -f .env ]]; then
  echo "Copy .env.example to .env and adjust if needed."
  exit 1
fi

# dbt-core 1.x is not compatible with Python 3.14+ (mashumaro / pydantic v1). Prefer 3.13 > 3.12 > 3.11.
pick_bootstrap_python() {
  if [[ -n "${BOOTSTRAP_PYTHON:-}" ]]; then
    echo "$BOOTSTRAP_PYTHON"
    return
  fi
  for cmd in python3.13 python3.12 python3.11; do
    if command -v "$cmd" >/dev/null 2>&1; then
      echo "$cmd"
      return
    fi
  done
  echo ""
}

PYBIN="$(pick_bootstrap_python)"
if [[ -z "$PYBIN" ]]; then
  echo "No Python 3.11–3.13 found in PATH (tried python3.13, python3.12, python3.11)."
  echo "dbt-core does not run on Python 3.14+ yet. Install e.g. brew install python@3.13, or set BOOTSTRAP_PYTHON to a 3.11–3.13 interpreter."
  exit 1
fi

pyver="$("$PYBIN" -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")')"
case "$pyver" in
  3.11|3.12|3.13) ;;
  *)
    echo "Refusing to create .venv with $PYBIN (Python $pyver). Use 3.11–3.13 or set BOOTSTRAP_PYTHON."
    exit 1
    ;;
esac

echo "Using $PYBIN (Python $pyver) for .venv"
"$PYBIN" -m venv .venv
# shellcheck disable=SC1091
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt

if [[ ! -f dbt_project/profiles.yml ]]; then
  cp dbt_project/profiles.yml.example dbt_project/profiles.yml
  echo "Created dbt_project/profiles.yml from example."
fi

echo "Bootstrap complete. Activate venv: source .venv/bin/activate"
