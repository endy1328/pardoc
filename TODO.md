# TODO

## Remaining Work

### 4. Corpus Expansion

- 기존 local `pdf_sample`은 text / diagram / mixed-table / known-unlabeled-diagram 대표군을 회귀로 고정
- OCR retry가 민감한 샘플은 review list로 별도 관리
- page 1에서 실제 OCR이 강제되는 scanned-only 샘플 추가
- pure table-heavy / multilingual 샘플 추가
- 추가 corpus는 라이선스와 저장소 크기 정책을 확인한 뒤 반영

## Completed

### 1. Diagram Relation Refinement

- semantic node와 routing node 구분 로직을 보강
- branch-heavy connector graph의 edge 추론과 root 선택 규칙을 정교화
- short-line `V` 패턴 외 multi-wing arrowhead shape 감지를 추가
- edge confidence에 segment geometry, routing node 수, label 품질을 반영

### 2. OCR Tuning By Document Family

- diagram box-focused OCR retry를 추가
- low-confidence page의 selective reprocessing 기준을 조정
- diagram/mixed 문서에서 label 복구용 OCR word filtering을 라벨 친화적으로 보강

### 3. Test / CI Operations

- `scripts/run_test_suites.sh`에 `fast`, `pr`, `nightly`, `all` 정책을 반영
- `.github/workflows/tests.yml`에 push / PR / nightly CI 구조를 추가
- `README.md`에 snapshot 갱신 절차와 테스트 정책을 문서화

## Current Useful Commands

```bash
cd /home/u24/projects/pardoc
source .venv/bin/activate
python3 -m compileall src tests
./scripts/run_test_suites.sh fast
./scripts/run_test_suites.sh pr
./scripts/run_test_suites.sh nightly
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
