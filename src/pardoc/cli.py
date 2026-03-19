from __future__ import annotations

import argparse
from pathlib import Path
import sys

from pardoc.converters import ConversionError, ConversionOptions, convert_file, render_json, render_markdown


SUPPORTED_EXTENSIONS = {
    ".docx",
    ".doc",
    ".pdf",
    ".xlsx",
    ".xlsm",
    ".xltx",
    ".xltm",
    ".xls",
    ".txt",
    ".md",
    ".csv",
    ".tsv",
    ".html",
    ".htm",
}


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    if args.ocr_dpi <= 0:
        raise SystemExit("--ocr-dpi must be greater than 0")
    if args.ocr_workers <= 0:
        raise SystemExit("--ocr-workers must be greater than 0")
    options = ConversionOptions(
        pdf_mode=args.pdf_mode,
        ocr_mode=args.ocr_mode,
        ocr_dpi=args.ocr_dpi,
        ocr_workers=args.ocr_workers,
        cache_dir=None if args.no_ocr_cache else Path(args.ocr_cache_dir).resolve(),
        page_numbers=_parse_page_range(args.pages),
        progress_callback=_build_progress_callback(args.show_progress),
        analysis_callback=_build_analysis_callback(args.show_analysis),
        debug_overlays=args.debug_overlays,
    )

    sources = _collect_sources(args.inputs)
    if not sources:
        print("No supported input files were found.", file=sys.stderr)
        return 1

    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    failures = 0
    reserved_outputs: set[Path] = set()
    for source in sources:
        try:
            result = convert_file(source, options)
            if args.show_analysis:
                _print_cache_analysis(result.debug_data or {})
            written = _write_outputs(
                result,
                output_dir,
                args.format,
                reserved_outputs,
                json_output=args.json_output,
            )
            joined = ", ".join(str(path) for path in written)
            print(f"[OK] {source} -> {joined}")
        except ConversionError as exc:
            failures += 1
            print(f"[ERROR] {source}: {exc}", file=sys.stderr)

    return 1 if failures else 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pardoc",
        description="Convert documents such as DOCX, PDF, XLSX/XLS into HTML, text, and Markdown.",
    )
    parser.add_argument("inputs", nargs="+", help="Input file(s) or directory path(s)")
    parser.add_argument(
        "-f",
        "--format",
        choices=["text", "html", "markdown", "both", "all"],
        default="both",
        help="Output format to write: 'both' keeps the legacy text+html pair, 'all' writes text+html+markdown",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=Path("output"),
        help="Directory where converted files will be saved",
    )
    parser.add_argument(
        "--pdf-mode",
        choices=["faithful", "semantic", "hybrid"],
        default="hybrid",
        help="PDF HTML mode: faithful keeps original page look, semantic keeps structured HTML, hybrid includes both",
    )
    parser.add_argument(
        "--ocr-mode",
        choices=["auto", "off", "force"],
        default="auto",
        help="OCR mode for PDFs: auto uses OCR when text extraction is empty, off disables OCR, force always runs OCR",
    )
    parser.add_argument(
        "--ocr-dpi",
        type=int,
        default=200,
        help="Render DPI used for OCR on PDFs",
    )
    parser.add_argument(
        "--ocr-workers",
        type=int,
        default=1,
        help="Number of worker threads for force OCR on PDFs",
    )
    parser.add_argument(
        "--ocr-cache-dir",
        default=".pardoc_cache",
        help="Directory for OCR cache files",
    )
    parser.add_argument(
        "--no-ocr-cache",
        action="store_true",
        help="Disable OCR cache reads and writes",
    )
    parser.add_argument(
        "--pages",
        help="PDF page selection, e.g. 1-3,5,8",
    )
    parser.add_argument(
        "--show-progress",
        action="store_true",
        help="Show per-page conversion progress for PDFs",
    )
    parser.add_argument(
        "--json-output",
        action="store_true",
        help="Write an additional .json file with structured debug data and rendered outputs",
    )
    parser.add_argument(
        "--show-analysis",
        action="store_true",
        help="Print per-page PDF analysis summaries to stderr",
    )
    parser.add_argument(
        "--debug-overlays",
        action="store_true",
        help="Add visual debug overlays for detected tables and OCR usage to faithful PDF HTML pages",
    )
    return parser


def _parse_page_range(spec: str | None) -> set[int] | None:
    if not spec:
        return None

    selected: set[int] = set()
    for chunk in spec.split(","):
        part = chunk.strip()
        if not part:
            continue
        if "-" in part:
            start_text, end_text = part.split("-", 1)
            start = int(start_text)
            end = int(end_text)
            if start <= 0 or end <= 0 or end < start:
                raise SystemExit(f"Invalid page range: {part}")
            selected.update(range(start, end + 1))
            continue
        page = int(part)
        if page <= 0:
            raise SystemExit(f"Invalid page number: {part}")
        selected.add(page)
    return selected


def _build_progress_callback(enabled: bool):
    if not enabled:
        return None

    def report(current_page: int, total_pages: int, processed: int, selected_total: int, status: str) -> None:
        print(
            f"[PROGRESS] page {current_page}/{total_pages} ({processed}/{selected_total} selected) [{status}]",
            file=sys.stderr,
        )

    return report


def _build_analysis_callback(enabled: bool):
    if not enabled:
        return None

    def report(page_data: dict[str, object]) -> None:
        page = page_data.get("page", "?")
        layout = page_data.get("layout", "unknown")
        layout_confidence = page_data.get("layout_confidence", "?")
        tables = page_data.get("table_count", 0)
        ocr_status = page_data.get("ocr_status", "unknown")
        ocr_confidence = page_data.get("ocr_confidence", {})
        ocr_strategy = page_data.get("ocr_strategy", {})
        text_blocks = page_data.get("text_blocks", 0)
        images = page_data.get("image_blocks", 0)
        drawings = page_data.get("drawing_blocks", 0)
        dominant_signal = page_data.get("dominant_signal", "")
        ocr_avg = ""
        if isinstance(ocr_confidence, dict) and "avg" in ocr_confidence:
            ocr_avg = f" ocr_avg={ocr_confidence['avg']}"
        ocr_low = ""
        if isinstance(ocr_confidence, dict) and "low_confidence_ratio" in ocr_confidence:
            ocr_low = f" low_ratio={ocr_confidence['low_confidence_ratio']}"
        ocr_psm = ""
        if isinstance(ocr_strategy, dict) and "psm" in ocr_strategy:
            ocr_psm = f" psm={ocr_strategy['psm']}"
        signal_note = f" dominant={dominant_signal}" if dominant_signal else ""
        print(
            f"[ANALYSIS] page {page} layout={layout} layout_conf={layout_confidence} "
            f"text_blocks={text_blocks} tables={tables} images={images} drawings={drawings}{signal_note} "
            f"ocr={ocr_status}{ocr_avg}{ocr_low}{ocr_psm}",
            file=sys.stderr,
        )

    return report


def _print_cache_analysis(debug_data: dict[str, object]) -> None:
    cache = debug_data.get("cache", {})
    if not isinstance(cache, dict) or not cache:
        return
    parts = []
    for kind in ("raster", "tables", "ocr"):
        stats = cache.get(kind, {})
        if not isinstance(stats, dict):
            continue
        fragment = (
            f"{kind}:hit={stats.get('hit', 0)}"
            f",miss={stats.get('miss', 0)}"
            f",stale={stats.get('stale', 0)}"
            f",write={stats.get('write', 0)}"
        )
        reasons = stats.get("reasons", {})
        if isinstance(reasons, dict) and reasons:
            ordered = ",".join(f"{key}={value}" for key, value in sorted(reasons.items()))
            fragment += f" [{ordered}]"
        parts.append(fragment)
    if parts:
        print(f"[ANALYSIS] cache {'; '.join(parts)}", file=sys.stderr)


def _collect_sources(inputs: list[str]) -> list[Path]:
    discovered: list[Path] = []
    for raw in inputs:
        path = Path(raw).resolve()
        if not path.exists():
            print(f"[WARN] Missing path skipped: {path}", file=sys.stderr)
            continue
        if path.is_file():
            if path.suffix.lower() in SUPPORTED_EXTENSIONS:
                discovered.append(path)
            else:
                print(f"[WARN] Unsupported file skipped: {path}", file=sys.stderr)
            continue
        if path.is_dir():
            for child in sorted(p for p in path.rglob("*") if p.is_file()):
                if child.suffix.lower() in SUPPORTED_EXTENSIONS:
                    discovered.append(child.resolve())
    return discovered


def _write_outputs(
    result,
    output_dir: Path,
    fmt: str,
    reserved_outputs: set[Path],
    *,
    json_output: bool = False,
) -> list[Path]:
    written = []

    if fmt in {"text", "both", "all"}:
        text_path = _choose_output_path(result.source, output_dir, ".txt", reserved_outputs)
        text_path.write_text(result.text, encoding="utf-8")
        written.append(text_path)

    if fmt in {"html", "both", "all"}:
        html_path = _choose_output_path(result.source, output_dir, ".html", reserved_outputs)
        html_path.write_text(result.html, encoding="utf-8")
        written.append(html_path)

    if fmt in {"markdown", "all"}:
        markdown_path = _choose_output_path(result.source, output_dir, ".md", reserved_outputs)
        markdown_path.write_text(render_markdown(result), encoding="utf-8")
        written.append(markdown_path)

    if json_output:
        json_path = _choose_output_path(result.source, output_dir, ".json", reserved_outputs)
        json_path.write_text(render_json(result), encoding="utf-8")
        written.append(json_path)

    return written


def _choose_output_path(source: Path, output_dir: Path, suffix: str, reserved_outputs: set[Path]) -> Path:
    candidates = [
        output_dir / f"{source.stem}{suffix}",
        output_dir / f"{source.stem}_{source.suffix.lstrip('.')}{suffix}",
        output_dir / f"{source.name}{suffix}",
    ]

    for candidate in candidates:
        if candidate not in reserved_outputs and not candidate.exists():
            reserved_outputs.add(candidate)
            return candidate

    counter = 2
    while True:
        candidate = output_dir / f"{source.stem}_{counter}{suffix}"
        if candidate not in reserved_outputs and not candidate.exists():
            reserved_outputs.add(candidate)
            return candidate
        counter += 1


if __name__ == "__main__":
    raise SystemExit(main())
