#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

MODE="${1:-all}"

if [[ -x ".venv/bin/python" ]]; then
  PYTHON_BIN=".venv/bin/python"
else
  PYTHON_BIN="${PYTHON_BIN:-python3}"
fi

run_fast() {
  PYTHONPATH=src "$PYTHON_BIN" -m unittest discover -s tests -p 'test_cache.py' -v
}

run_sample() {
  PYTHONPATH=src "$PYTHON_BIN" -m unittest discover -s tests -p 'test_pdf_regression.py' -v
}

run_corpus() {
  PYTHONPATH=src "$PYTHON_BIN" -m unittest discover -s tests -p 'test_pdf_corpus_regression.py' -v
}

run_pr() {
  run_fast
  run_sample
}

run_nightly() {
  run_fast
  run_sample
  run_corpus
}

case "$MODE" in
  fast)
    run_fast
    ;;
  pr)
    run_pr
    ;;
  sample)
    run_sample
    ;;
  corpus)
    run_corpus
    ;;
  nightly)
    run_nightly
    ;;
  all)
    run_nightly
    ;;
  *)
    echo "usage: scripts/run_test_suites.sh [fast|pr|sample|corpus|nightly|all]" >&2
    exit 1
    ;;
esac
