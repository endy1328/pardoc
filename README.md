# pardoc

`docx`, `doc`, `pdf`, `xlsx`, `xls` 같은 문서를 `html`, `text`, `markdown`, `json(debug)`으로 변환하는 CLI입니다.
현재 구현의 중심은 PDF 경로이며, 구조 추출, OCR, 캐시, diagram 힌트, regression suite까지 포함합니다.

## 지원 형식

- `.docx`: `mammoth`
- `.doc`: `LibreOffice(soffice)` 경유
- `.pdf`: `PyMuPDF` 우선, 필요 시 `pypdf` fallback
- `.xlsx`, `.xlsm`, `.xltx`, `.xltm`: `openpyxl`
- `.xls`: `xlrd`
- `.txt`, `.md`, `.csv`, `.tsv`, `.html`: 단순 입력

지원 입력 포맷 요약:

- `.docx`
- `.doc`
- `.pdf`
- `.xlsx`
- `.xlsm`
- `.xltx`
- `.xltm`
- `.xls`
- `.txt`
- `.md`
- `.csv`
- `.tsv`
- `.html`
- `.htm`

## 출력 포맷

기본 출력 포맷:

- `text` -> `.txt`
- `html` -> `.html`
- `markdown` -> `.md`

선택 출력 포맷:

- `json(debug)` -> `.json` (`--json-output`)

## PDF 기능

- `faithful`, `semantic`, `hybrid`, `reconstructed` HTML 모드
- direct Markdown 생성
- borderless table inference, multi-line cell merge, adjacent table merge
- page layout classification: `text`, `table`, `diagram`, `mixed`
- diagram box / connector / inferred edge extraction
- edge provenance (`direct` / `chain` / `branch`) and heuristic confidence
- simple arrowhead-based direction hinting
- routing-node-aware edge inference
- OCR label backfill for unlabeled diagram boxes
- OCR `auto`, `off`, `force`
- page-type-aware OCR preprocessing
- adaptive `psm` retry + early stop
- selective OCR rescue variant expansion for weak text/table results
- OCR confidence summary per page
- raster / table / OCR cache
- cache invalidation reason reporting
- `--json-output`, `--show-analysis`, `--debug-overlays`

## 설치

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

추가 요구사항:

- `.doc` 변환: `LibreOffice`와 `soffice`
- OCR: `tesseract`와 필요한 언어 데이터

## 기본 사용법

단일 파일:

```bash
pardoc sample.docx
```

디렉터리 전체:

```bash
pardoc ./documents -o ./converted
```

Markdown만:

```bash
pardoc report.pdf --format markdown
```

기본 PDF HTML:

```bash
pardoc report.pdf
```

faithful PDF HTML:

```bash
pardoc report.pdf --pdf-mode faithful
```

reconstructed PDF HTML:

```bash
pardoc report.pdf --pdf-mode reconstructed --format html
```

hybrid PDF HTML + Markdown:

```bash
pardoc report.pdf --pdf-mode hybrid --format all
```

일부 페이지만:

```bash
pardoc big.pdf --pages 1-3,8
```

강제 OCR:

```bash
pardoc scanned.pdf --ocr-mode force
```

강제 OCR + 병렬 처리 + 캐시:

```bash
pardoc scanned.pdf --ocr-mode force --ocr-workers 4 --ocr-dpi 300 --ocr-cache-dir .pardoc_cache
```

캐시 없이 강제 OCR:

```bash
pardoc scanned.pdf --ocr-mode force --no-ocr-cache
```

분석 요약과 JSON:

```bash
pardoc report.pdf --pdf-mode hybrid --pages 1-5 --format markdown --json-output --show-analysis
```

디버그 오버레이:

```bash
pardoc report.pdf --pdf-mode hybrid --debug-overlays --format html
```

기본 출력 디렉터리는 `./output`입니다.

## 출력 파일

- `sample.docx` -> `output/sample.txt`, `output/sample.html`
- `report.pdf --format markdown` -> `output/report.md`
- `report.pdf --format all` -> `output/report.txt`, `output/report.html`, `output/report.md`
- `report.pdf --json-output` -> 기존 출력 + `output/report.json`

## PDF 출력 모드 가이드

- `faithful`
  - 원본 페이지 이미지를 유지하는 HTML입니다.
  - debug overlay 확인에 가장 적합합니다.
- `semantic`
  - 텍스트, 표, diagram summary 위주의 구조화 HTML입니다.
- `hybrid`
  - faithful view와 structured content를 함께 제공합니다.
- `reconstructed`
  - 기본값입니다.
  - 배경 페이지 이미지를 쓰지 않고, 추출한 block/table을 HTML/CSS만으로 다시 배치합니다.
  - `article`, `main`, `section`, `form`, `fieldset` 같은 DOM 구조를 우선해서 원본과 유사한 문서 구조를 재생성합니다.
  - 원본과 유사한 읽기 흐름과 섹션감을 목표로 하지만, complex diagram/page art는 완전 복제하지 않습니다.

## HTML / Markdown 품질 기준

- HTML은 기본적으로 `reconstructed` 모드에서 원본과 유사한 DOM 구조, 블록 배치, 표 구조 재생성을 목표로 합니다.
- Markdown은 표준 문법 한계 때문에 레이아웃 복제보다는 읽기 순서와 구조 보존에 초점을 둡니다.
- 따라서 HTML은 이미지 배치보다 DOM 구조 재생성을 우선합니다.

## PDF 상태 / 분석 출력

진행률 상태:

- `native-text`
- `ocr-auto`
- `ocr-force`
- `cache-hit`

`--show-analysis`에는 다음 정보가 포함됩니다.

- page layout / confidence
- text, image, drawing block count
- table count
- dominant signal
- OCR 평균 confidence, low-confidence ratio, selected `psm`
- cache hit / miss / stale / write summary

`--json-output`의 `debug.pages[*]`에는 다음 구조가 포함됩니다.

- `layout`, `layout_confidence`, `dominant_signal`, `signal_scores`
- `tables`
- `diagram.boxes`, `diagram.connector_segments`, `diagram.edges`
- `diagram.edges[*].provenance`, `diagram.edges[*].direction_hint`, `diagram.edges[*].confidence`, `diagram.edges[*].routing_nodes`
- `ocr_confidence`
- `ocr_strategy`
- `cache`

## 테스트

권장 정책:

- `fast`: 캐시와 핵심 유틸리티 중심의 빠른 단위 테스트
- `pr`: `fast` + 샘플 PDF snapshot regression
- `nightly`: `pr` + `pdf_sample` corpus smoke regression

빠른 단위 테스트:

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_cache.py' -v
```

샘플 PDF snapshot regression:

```bash
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_pdf_regression.py' -v
```

`pdf_sample` corpus regression:

```bash
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_pdf_corpus_regression.py' -v
```

현재 corpus regression은 기존 `pdf_sample`만으로 text / diagram / mixed-table / known-unlabeled-diagram 대표군을 고정합니다. `AULA F87Pro 퀵 가이드.pdf`, `PMBOK 핵심요약(1부~5부).pdf`, `profile_form.pdf`, `teugijeomi onda - rei keojeuwail.pdf`는 OCR retry 경로를 수동으로 다시 보는 review list에 따로 올려 두는 편이 안전합니다.

suite helper script:

```bash
scripts/run_test_suites.sh fast
scripts/run_test_suites.sh pr
scripts/run_test_suites.sh sample
scripts/run_test_suites.sh corpus
scripts/run_test_suites.sh nightly
scripts/run_test_suites.sh all
```

GitHub Actions workflow:

- `.github/workflows/tests.yml`
- `push` / `pull_request`: `scripts/run_test_suites.sh pr`
- `schedule` / `workflow_dispatch`: `scripts/run_test_suites.sh nightly`

snapshot update workflow:

1. heuristic, parser, OCR, diagram 관련 변경 후 `scripts/run_test_suites.sh pr`를 먼저 실행합니다.
2. 의도된 출력 변화라면 `tests/snapshots/*`를 새 결과로 갱신합니다.
3. `tests/snapshots/pdf_sample_family_matrix.json`처럼 corpus family snapshot이나 review list가 영향을 받는 경우 `scripts/run_test_suites.sh nightly`까지 다시 실행해 drift가 없는지 확인합니다.
4. 변경 이유가 문서화가 필요하면 `TODO.md`의 완료 항목과 README 테스트 정책을 함께 갱신합니다.

전체 테스트:

```bash
python3 -m compileall src tests
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -v
```

## 샘플 수동 검증

```bash
PYTHONPATH=src .venv/bin/python -m pardoc.cli 'pdf_sample/sample.pdf' \
  --pdf-mode hybrid \
  --pages 1-5 \
  --format markdown \
  --json-output \
  --show-analysis \
  -o /tmp/pardoc_manual_check
```

같은 명령을 2회 실행하면 cache-hit 경로를 확인할 수 있습니다.

## GitHub 업로드 가이드

권장 업로드 범위:

- 포함: `src/`, `tests/`, `scripts/`, `.github/`, `README.md`, `TODO.md`, `pyproject.toml`, `requirements.txt`
- 제외: `.venv/`, `.pardoc_cache*/`, `out/`, `output*/`, 로컬 임시 파일

`pdf_sample/` 정책:

- 기본값은 저장소 제외를 권장합니다.
- 이유: 샘플 corpus 크기가 크고, 문서 저작권/재배포 가능 여부를 별도로 확인해야 할 수 있습니다.
- 샘플이 꼭 필요하면 Git LFS 또는 별도 private storage/repo 분리를 권장합니다.



공개 전 체크리스트:

- `.gitignore`에 캐시, 가상환경, 산출물, 로컬 샘플이 제외되어 있는지 확인
- `tests/`와 `tests/snapshots/`가 추적 대상인지 확인
- GitHub Actions workflow가 의도한 범위(`fast`, `sample_pdf`, `corpus_pdf`)로 동작하는지 확인
- 샘플 PDF를 올릴 경우 용량과 라이선스/배포 가능 여부를 확인
- 민감한 `.env`, key, local credential 파일이 없는지 재확인

## 제한 사항

- diagram edge는 direct/chain/branch provenance와 confidence를 제공하지만, arrowhead 판정은 여전히 휴리스틱입니다.
- branch-heavy diagram은 아직 그래프 최적화보다 휴리스틱 edge 생성에 가깝습니다.
- connector-only drawing이 많은 문서는 debug JSON에는 남아도 Markdown/HTML summary에서는 숨깁니다.
- OCR 품질은 문서군별로 차이가 있어 추가 tuning 여지가 있습니다.
- `.doc` 변환은 `LibreOffice` 의존입니다.
- `--debug-overlays`는 faithful 계열 HTML에서 가장 유의미합니다.
