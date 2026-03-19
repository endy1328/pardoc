from __future__ import annotations

import importlib.util
import json
import tempfile
from pathlib import Path
import unittest

from pardoc.converters import ConversionOptions, convert_file, render_json, render_markdown


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_PDF = PROJECT_ROOT / "12장 조달관리.pdf"
if not SAMPLE_PDF.exists():
    SAMPLE_PDF = PROJECT_ROOT / "pdf_sample" / "12장 조달관리.pdf"
SNAPSHOT_DIR = PROJECT_ROOT / "tests" / "snapshots"
PDF_SAMPLE_DIR = PROJECT_ROOT / "pdf_sample"
PDF_BACKEND_AVAILABLE = bool(importlib.util.find_spec("fitz") or importlib.util.find_spec("pypdf"))


@unittest.skipUnless(PDF_BACKEND_AVAILABLE, "PDF backend dependencies are required for regression tests")
class PdfRegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.cache_dir = Path(self.temp_dir.name) / "cache"

    def test_markdown_json_and_overlay_outputs_remain_available(self) -> None:
        result = convert_file(
            SAMPLE_PDF,
            ConversionOptions(
                pdf_mode="hybrid",
                page_numbers={1, 2, 3},
                cache_dir=self.cache_dir,
                debug_overlays=True,
            ),
        )

        markdown = render_markdown(result)
        self.assertIn("## Page 1", markdown)
        self.assertIn("## Page 3", markdown)
        self.assertIn("|", markdown)

        html = result.html
        self.assertIn("pdf-debug-layer", html)
        self.assertIn("pdf-page-frame", html)
        self.assertIn("Structured Content", html)

        payload = json.loads(render_json(result))
        self.assertEqual(payload["debug"]["kind"], "pdf")
        self.assertEqual(payload["debug"]["engine"], "pymupdf")
        self.assertEqual(payload["debug"]["selected_pages"], [1, 2, 3])
        self.assertEqual(len(payload["debug"]["pages"]), 3)
        self.assertIn("cache", payload["debug"])
        self.assertIn("diagram", payload["debug"]["pages"][0])
        self.assertIn("edges", payload["debug"]["pages"][0]["diagram"])
        page_two = payload["debug"]["pages"][1]
        self.assertIn("ocr_strategy", page_two)
        self.assertIn("ocr_confidence", page_two)
        if page_two["ocr_strategy"]:
            self.assertIn("trials", page_two["ocr_strategy"])
            self.assertIn("stop_reason", page_two["ocr_strategy"])

    def test_second_run_reports_cache_hits_for_raster_and_tables(self) -> None:
        options = ConversionOptions(
            pdf_mode="hybrid",
            page_numbers={1, 2, 3},
            cache_dir=self.cache_dir,
        )

        first = convert_file(SAMPLE_PDF, options)
        first_cache = first.debug_data["cache"]
        self.assertGreaterEqual(first_cache["raster"]["write"], 1)
        self.assertGreaterEqual(first_cache["tables"]["write"], 1)

        second = convert_file(SAMPLE_PDF, options)
        second_cache = second.debug_data["cache"]
        self.assertEqual(second_cache["raster"]["miss"], 0)
        self.assertEqual(second_cache["tables"]["miss"], 0)
        self.assertGreaterEqual(second_cache["raster"]["hit"], 1)
        self.assertGreaterEqual(second_cache["tables"]["hit"], 1)

        page_two = next(page for page in second.debug_data["pages"] if page["page"] == 2)
        self.assertIn(page_two["cache"]["ocr"], {"hit", "unused", "miss"})
        self.assertIn("edges", page_two["diagram"])
        if page_two["ocr_confidence"]:
            self.assertIn("low_confidence_ratio", page_two["ocr_confidence"])
        if page_two["ocr_strategy"]:
            self.assertIn("trials", page_two["ocr_strategy"])
            self.assertIn("stop_reason", page_two["ocr_strategy"])

    def test_sample_pdf_markdown_excerpt_matches_snapshot(self) -> None:
        result = convert_file(
            SAMPLE_PDF,
            ConversionOptions(
                pdf_mode="hybrid",
                page_numbers={1, 2, 3, 4, 5},
                cache_dir=self.cache_dir,
                debug_overlays=True,
            ),
        )

        markdown = render_markdown(result)
        excerpt_lines = []
        for line in markdown.splitlines():
            if line.startswith("## Page ") or line.startswith("> Diagram hints") or line.startswith("- Flow:"):
                excerpt_lines.append(line.rstrip())
        excerpt = "\n".join(excerpt_lines).strip()
        expected = (SNAPSHOT_DIR / "sample_markdown_excerpt.txt").read_text(encoding="utf-8").strip()
        self.assertEqual(excerpt, expected)

    def test_sample_pdf_debug_summary_matches_snapshot(self) -> None:
        result = convert_file(
            SAMPLE_PDF,
            ConversionOptions(
                pdf_mode="hybrid",
                page_numbers={1, 2, 3, 4, 5},
                cache_dir=self.cache_dir,
            ),
        )

        summary = []
        for page in result.debug_data["pages"]:
            diagram = page.get("diagram", {})
            strategy = page.get("ocr_strategy") or {}
            summary.append(
                {
                    "page": page["page"],
                    "layout": page["layout"],
                    "dominant_signal": page.get("dominant_signal"),
                    "table_count": page.get("table_count"),
                    "diagram": {
                        "boxes": len(diagram.get("boxes", [])),
                        "connectors": diagram.get("connectors", 0),
                        "edges": len(diagram.get("edges", [])),
                    },
                    "ocr": {
                        "status": page.get("ocr_status"),
                        "psm": strategy.get("psm"),
                        "variant": strategy.get("variant"),
                        "stop_reason": strategy.get("stop_reason"),
                    },
                }
            )
        expected = json.loads((SNAPSHOT_DIR / "sample_page_summary.json").read_text(encoding="utf-8"))
        self.assertEqual(summary, expected)

if __name__ == "__main__":
    unittest.main()
