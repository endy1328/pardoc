from __future__ import annotations

import importlib.util
import json
import tempfile
from pathlib import Path
import unittest

from pardoc.converters import ConversionOptions, convert_file, render_markdown


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PDF_SAMPLE_DIR = PROJECT_ROOT / "pdf_sample"
SNAPSHOT_DIR = PROJECT_ROOT / "tests" / "snapshots"
PDF_BACKEND_AVAILABLE = bool(importlib.util.find_spec("fitz") or importlib.util.find_spec("pypdf"))


@unittest.skipUnless(PDF_BACKEND_AVAILABLE, "PDF backend dependencies are required for corpus regression tests")
@unittest.skipUnless(PDF_SAMPLE_DIR.exists(), "pdf_sample corpus is required for corpus regression tests")
class PdfCorpusRegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.cache_dir = Path(self.temp_dir.name) / "cache"

    @staticmethod
    def _sample_family(name: str) -> str:
        if name == "profile_form.pdf":
            return "ocr-heavy"
        if name == "AWS101_Services+0517.pdf":
            return "known-unlabeled-diagram"
        if name in {"250507_P-DX AI미팅_순번 2. BPED P&ID.pdf"}:
            return "mixed-table"
        if name in {"lecture-07-refactoring-to-microservices.pdf"}:
            return "mixed-text-diagram"
        if name in {"12장 조달관리.pdf", "en_DxO_Optics_Pro_v5_Visual_Guide.pdf", "AWS101_Intro+0517.pdf"}:
            return "diagram"
        return "text"

    def test_pdf_sample_corpus_first_page_smoke(self) -> None:
        files = sorted(PDF_SAMPLE_DIR.glob("*.pdf"))
        self.assertTrue(files)

        for path in files:
            with self.subTest(pdf=path.name):
                result = convert_file(
                    path,
                    ConversionOptions(
                        pdf_mode="hybrid",
                        page_numbers={1},
                        cache_dir=self.cache_dir / "corpus",
                    ),
                )
                markdown = render_markdown(result)
                self.assertTrue(markdown.strip())
                self.assertEqual(result.debug_data["kind"], "pdf")
                self.assertEqual(len(result.debug_data["pages"]), 1)
                page = result.debug_data["pages"][0]
                self.assertIn(page["layout"], {"text", "table", "diagram", "mixed"})
                self.assertIn("diagram", page)
                self.assertIn("edges", page["diagram"])

    def test_pdf_sample_representative_first_page_summary_matches_snapshot(self) -> None:
        expected = json.loads((SNAPSHOT_DIR / "pdf_sample_representative_summary.json").read_text(encoding="utf-8"))
        actual = []

        for item in expected:
            path = PDF_SAMPLE_DIR / item["file"]
            result = convert_file(
                path,
                ConversionOptions(
                    pdf_mode="hybrid",
                    page_numbers={1},
                    cache_dir=self.cache_dir / "representative",
                ),
            )
            page = result.debug_data["pages"][0]
            actual.append(
                {
                    "file": path.name,
                    "layout": page["layout"],
                    "dominant_signal": page.get("dominant_signal"),
                    "table_count": page.get("table_count", 0),
                    "diagram": {
                        "boxes": len(page.get("diagram", {}).get("boxes", [])),
                        "connectors": page.get("diagram", {}).get("connectors", 0),
                        "edges": len(page.get("diagram", {}).get("edges", [])),
                    },
                    "ocr_status": page.get("ocr_status"),
                    "markdown_head": "\n".join(render_markdown(result).splitlines()[:6]),
                }
            )

        self.assertEqual(actual, expected)

    def test_pdf_sample_family_matrix_matches_snapshot(self) -> None:
        files = [
            "AgilePG_KOR-UL.pdf",
            "12장 조달관리.pdf",
            "lecture-07-refactoring-to-microservices.pdf",
            "250507_P-DX AI미팅_순번 2. BPED P&ID.pdf",
            "AWS101_Services+0517.pdf",
            "en_DxO_Optics_Pro_v5_Visual_Guide.pdf",
        ]
        actual = []

        for filename in files:
            path = PDF_SAMPLE_DIR / filename
            result = convert_file(
                path,
                ConversionOptions(
                    pdf_mode="hybrid",
                    page_numbers={1},
                    cache_dir=self.cache_dir / "family-matrix",
                ),
            )
            page = result.debug_data["pages"][0]
            diagram = page.get("diagram", {})
            actual.append(
                {
                    "file": path.name,
                    "family": self._sample_family(path.name),
                    "layout": page["layout"],
                    "dominant_signal": page.get("dominant_signal"),
                    "table_count": page.get("table_count", 0),
                    "diagram": {
                        "boxes": len(diagram.get("boxes", [])),
                        "connectors": diagram.get("connectors", 0),
                        "edges": len(diagram.get("edges", [])),
                        "unlabeled_boxes": diagram.get("unlabeled_boxes", 0),
                    },
                    "ocr_status": page.get("ocr_status"),
                }
            )

        expected = json.loads((SNAPSHOT_DIR / "pdf_sample_family_matrix.json").read_text(encoding="utf-8"))
        self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()
