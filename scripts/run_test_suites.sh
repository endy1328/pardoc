#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

MODE="${1:-all}"

run_fast() {
  PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_cache.py' -v
}

run_sample() {
  PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_pdf_regression.py' -v
}

run_corpus() {
  PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_pdf_corpus_regression.py' -v
}

case "$MODE" in
  fast)
    run_fast
    ;;
  sample)
    run_sample
    ;;
  corpus)
    run_corpus
    ;;
  all)
    run_fast
    run_sample
    run_corpus
    ;;
  *)
    echo "usage: scripts/run_test_suites.sh [fast|sample|corpus|all]" >&2
    exit 1
    ;;
esac
