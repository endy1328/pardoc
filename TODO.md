# TODO

## Remaining Work

### 1. Diagram Relation Refinement

- semantic node와 routing node를 더 안정적으로 구분
- branch-heavy connector graph에서 edge 추론 정교화
- short-line `V` 패턴 외의 richer arrowhead shape 감지
- edge confidence를 segment geometry와 label 품질까지 반영하도록 보강

### 2. OCR Tuning By Document Family

- diagram box-focused OCR retry 추가
- low-confidence page에 대한 selective reprocessing threshold를 corpus 기준으로 재조정
- diagram/mixed 문서에서 label 복구용 OCR word filtering 추가 튜닝

### 3. Test / CI Operations

- corpus 규모 증가 시 CI runtime 관리
- PR / nightly에 어떤 corpus 검사를 둘지 기준 정리
- snapshot 갱신 절차를 contributor workflow 수준으로 더 구체화

### 4. Corpus Expansion

- page 1에서 실제 OCR이 강제되는 scanned-only 샘플 추가
- pure table-heavy / multilingual 샘플 추가
- known failure fixture와 regression note 추가

## Current Useful Commands

```bash
cd /home/u24/projects/pardoc
source .venv/bin/activate
python3 -m compileall src tests
PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_cache.py' -v
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_pdf_regression.py' -v
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_pdf_corpus_regression.py' -v
./scripts/run_test_suites.sh all
```

Focused PDF check:

```bash
PYTHONPATH=src .venv/bin/python -m pardoc.cli 'pdf_sample/12장 조달관리.pdf' \
  --pdf-mode hybrid \
  --pages 1-5 \
  --format markdown \
  --json-output \
  --show-analysis \
  -o /tmp/pardoc_manual_check
```
