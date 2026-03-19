from __future__ import annotations

import tempfile
from pathlib import Path
import unittest

from pardoc import converters


class DummyPixmap:
    def __init__(self, marker: bytes) -> None:
        self.width = 8
        self.height = 6
        self._marker = marker

    def tobytes(self, fmt: str) -> bytes:
        if fmt != "png":
            raise ValueError(fmt)
        return self._marker


class DummyPage:
    def __init__(self, marker: bytes = b"png-bytes") -> None:
        self.marker = marker
        self.pixmap_calls = 0
        self.find_tables_calls = 0

    def get_pixmap(self, dpi: int, alpha: bool = False) -> DummyPixmap:
        self.pixmap_calls += 1
        return DummyPixmap(self.marker + f"-{dpi}".encode("ascii"))

    def find_tables(self):
        self.find_tables_calls += 1
        raise AssertionError("find_tables should not be called on cache hit")


class NativeTable:
    def __init__(self) -> None:
        self.bbox = (1.0, 2.0, 3.0, 4.0)

    def extract(self):
        return [["A", "B"], ["1", "2"]]


class Finder:
    def __init__(self) -> None:
        self.tables = [NativeTable()]


class TablePage(DummyPage):
    def find_tables(self):
        self.find_tables_calls += 1
        return Finder()


class DiagramPage:
    class RectPage:
        width = 420
        height = 80

    rect = RectPage()

    def get_drawings(self):
        class Rect:
            def __init__(self, x0, y0, x1, y1):
                self.x0 = x0
                self.y0 = y0
                self.x1 = x1
                self.y1 = y1

        class Point:
            def __init__(self, x, y):
                self.x = x
                self.y = y

        return [
            {"items": [("re", Rect(0, 0, 200, 80))]},
            {"items": [("re", Rect(240, 0, 420, 80))]},
            {"items": [("l", Point(200, 40), Point(240, 40))]},
        ]


class ChainedDiagramPage(DiagramPage):
    class RectPage:
        width = 460
        height = 120

    rect = RectPage()

    def get_drawings(self):
        class Rect:
            def __init__(self, x0, y0, x1, y1):
                self.x0 = x0
                self.y0 = y0
                self.x1 = x1
                self.y1 = y1

        class Point:
            def __init__(self, x, y):
                self.x = x
                self.y = y

        return [
            {"items": [("re", Rect(0, 20, 160, 100))]},
            {"items": [("re", Rect(300, 20, 460, 100))]},
            {"items": [("l", Point(160, 60), Point(220, 60))]},
            {"items": [("l", Point(220, 60), Point(300, 60))]},
        ]


class ArrowDiagramPage(DiagramPage):
    class RectPage:
        width = 420
        height = 120

    rect = RectPage()

    def get_drawings(self):
        class Rect:
            def __init__(self, x0, y0, x1, y1):
                self.x0 = x0
                self.y0 = y0
                self.x1 = x1
                self.y1 = y1

        class Point:
            def __init__(self, x, y):
                self.x = x
                self.y = y

        return [
            {"items": [("re", Rect(0, 20, 150, 100))]},
            {"items": [("re", Rect(270, 20, 420, 100))]},
            {"items": [("l", Point(150, 60), Point(270, 60))]},
            {"items": [("l", Point(270, 60), Point(255, 50))]},
            {"items": [("l", Point(270, 60), Point(255, 70))]},
        ]


class LargeRectDiagramPage(DiagramPage):
    def get_drawings(self):
        class Rect:
            def __init__(self, x0, y0, x1, y1):
                self.x0 = x0
                self.y0 = y0
                self.x1 = x1
                self.y1 = y1

        return [{"items": [("re", Rect(0, 0, 420, 80))]}]


class DummyDocument:
    page_count = 3
    metadata = {"format": "PDF 1.7", "title": "sample", "author": "tester"}


class CacheTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.cache_dir = Path(self.temp_dir.name) / ".pardoc_cache"
        self.source_path = Path(self.temp_dir.name) / "sample.pdf"
        self.source_path.write_bytes(b"pdf")
        self.document = DummyDocument()

    def test_raster_cache_reuses_rendered_page(self) -> None:
        cache_report = converters._create_cache_report()
        page = DummyPage()

        first = converters._get_cached_page_raster(
            page,
            self.source_path,
            self.document,
            1,
            144,
            self.cache_dir,
            cache_report,
        )
        second = converters._get_cached_page_raster(
            page,
            self.source_path,
            self.document,
            1,
            144,
            self.cache_dir,
            cache_report,
        )

        self.assertEqual(page.pixmap_calls, 1)
        self.assertEqual(first, second)
        self.assertEqual(cache_report["raster"]["write"], 1)
        self.assertGreaterEqual(cache_report["raster"]["hit"], 1)

    def test_table_cache_reuses_semantic_extraction(self) -> None:
        cache_report = converters._create_cache_report()
        page = TablePage()
        page_dict = {"blocks": []}

        first, first_status = converters._extract_pymupdf_tables(
            page,
            page_dict,
            self.source_path,
            self.document,
            2,
            self.cache_dir,
            cache_report,
        )
        self.assertEqual(first_status, "miss")
        self.assertEqual(page.find_tables_calls, 1)

        cached_page = DummyPage()
        second, second_status = converters._extract_pymupdf_tables(
            cached_page,
            page_dict,
            self.source_path,
            self.document,
            2,
            self.cache_dir,
            cache_report,
        )

        self.assertEqual(second_status, "hit")
        self.assertEqual(cached_page.find_tables_calls, 0)
        self.assertEqual(first, second)

    def test_cache_metadata_invalidation_reports_stale_reason(self) -> None:
        cache_report = converters._create_cache_report()
        page = DummyPage()
        converters._get_cached_page_raster(
            page,
            self.source_path,
            self.document,
            1,
            144,
            self.cache_dir,
            cache_report,
        )

        self.source_path.write_bytes(b"pdf-updated")
        invalidation_report = converters._create_cache_report()
        updated_page = DummyPage()
        converters._get_cached_page_raster(
            updated_page,
            self.source_path,
            self.document,
            1,
            144,
            self.cache_dir,
            invalidation_report,
        )

        self.assertGreaterEqual(invalidation_report["raster"]["stale"], 1)
        self.assertGreaterEqual(invalidation_report["raster"]["reasons"]["signature"], 1)
        self.assertEqual(updated_page.pixmap_calls, 1)

    def test_page_analysis_detects_mixed_layout(self) -> None:
        blocks = []
        for index in range(5):
            y0 = index * 55
            blocks.append(
                {
                    "type": 0,
                    "bbox": (0, y0, 600, y0 + 45),
                    "lines": [
                        {"spans": [{"text": "설명 텍스트 블록입니다. 도해 설명을 포함합니다.", "bbox": (10, y0 + 5, 360, y0 + 20)}]},
                        {"spans": [{"text": "추가 문장입니다.", "bbox": (10, y0 + 24, 180, y0 + 38)}]},
                    ],
                }
            )
        blocks.extend(
            [
                {"type": 1, "bbox": (0, 280, 600, 760)},
                {"type": 2, "bbox": (20, 300, 580, 740)},
            ]
        )

        analysis = converters._analyze_pdf_page(blocks, table_count=0)
        self.assertEqual(analysis.layout, "mixed")
        self.assertEqual(analysis.dominant_signal, "text+diagram")
        self.assertGreaterEqual(analysis.signal_scores["text"], 0.38)
        self.assertGreaterEqual(analysis.signal_scores["diagram"], 0.38)

    def test_page_analysis_detects_diagram_heavy_layout(self) -> None:
        blocks = [
            {"type": 1, "bbox": (0, 0, 600, 700)},
            {"type": 2, "bbox": (10, 20, 590, 680)},
            {"type": 2, "bbox": (20, 30, 580, 660)},
            {
                "type": 0,
                "bbox": (30, 710, 580, 760),
                "lines": [{"spans": [{"text": "짧은 캡션", "bbox": (30, 710, 120, 724)}]}],
            },
        ]

        analysis = converters._analyze_pdf_page(blocks, table_count=0)
        self.assertEqual(analysis.layout, "diagram")
        self.assertEqual(analysis.dominant_signal, "diagram")
        self.assertGreater(analysis.signal_scores["diagram"], analysis.signal_scores["text"])

    def test_diagram_primitives_extract_labeled_boxes_and_connectors(self) -> None:
        page_dict = {
            "blocks": [
                {
                    "type": 0,
                    "bbox": (10, 10, 190, 50),
                    "lines": [{"spans": [{"text": "Start", "bbox": (10, 10, 80, 25), "size": 12}]}],
                },
                {
                    "type": 0,
                    "bbox": (250, 10, 390, 50),
                    "lines": [{"spans": [{"text": "Finish", "bbox": (250, 10, 340, 25), "size": 12}]}],
                },
            ]
        }

        diagram = converters._extract_pymupdf_diagram_primitives(DiagramPage(), page_dict)
        self.assertEqual(diagram["connectors"], 1)
        self.assertEqual(len(diagram["boxes"]), 2)
        self.assertEqual(diagram["boxes"][0]["label"], "Start")
        self.assertEqual(diagram["boxes"][1]["label"], "Finish")
        self.assertEqual(diagram["boxes"][0]["label_source"], "native")
        self.assertEqual(
            diagram["edges"],
            [
                {
                    "from_index": 1,
                    "to_index": 2,
                    "from_label": "Start",
                    "to_label": "Finish",
                    "provenance": "direct",
                    "direction_hint": "segment",
                    "confidence": 0.9,
                    "routing_nodes": 0,
                }
            ],
        )

    def test_diagram_edge_inference_merges_chained_segments(self) -> None:
        page_dict = {
            "blocks": [
                {
                    "type": 0,
                    "bbox": (10, 30, 120, 70),
                    "lines": [{"spans": [{"text": "Input", "bbox": (10, 30, 80, 45), "size": 12}]}],
                },
                {
                    "type": 0,
                    "bbox": (320, 30, 420, 70),
                    "lines": [{"spans": [{"text": "Output", "bbox": (320, 30, 390, 45), "size": 12}]}],
                },
            ]
        }

        diagram = converters._extract_pymupdf_diagram_primitives(ChainedDiagramPage(), page_dict)
        self.assertEqual(len(diagram["edges"]), 1)
        self.assertEqual(diagram["edges"][0]["from_label"], "Input")
        self.assertEqual(diagram["edges"][0]["to_label"], "Output")
        self.assertEqual(diagram["edges"][0]["provenance"], "chain")
        self.assertEqual(diagram["edges"][0]["direction_hint"], "spatial")
        self.assertGreaterEqual(diagram["edges"][0]["confidence"], 0.7)
        self.assertEqual(diagram["edges"][0]["routing_nodes"], 0)

    def test_diagram_edge_inference_detects_arrowhead_direction(self) -> None:
        page_dict = {
            "blocks": [
                {
                    "type": 0,
                    "bbox": (10, 30, 120, 70),
                    "lines": [{"spans": [{"text": "Left", "bbox": (10, 30, 70, 45), "size": 12}]}],
                },
                {
                    "type": 0,
                    "bbox": (300, 30, 390, 70),
                    "lines": [{"spans": [{"text": "Right", "bbox": (300, 30, 360, 45), "size": 12}]}],
                },
            ]
        }

        diagram = converters._extract_pymupdf_diagram_primitives(ArrowDiagramPage(), page_dict)
        self.assertEqual(len(diagram["edges"]), 1)
        self.assertEqual(diagram["edges"][0]["from_label"], "Left")
        self.assertEqual(diagram["edges"][0]["to_label"], "Right")
        self.assertEqual(diagram["edges"][0]["direction_hint"], "arrowhead")
        self.assertGreaterEqual(diagram["edges"][0]["confidence"], 0.9)
        self.assertEqual(diagram["edges"][0]["routing_nodes"], 0)

    def test_diagram_labels_can_be_backfilled_from_ocr_words(self) -> None:
        diagram = {
            "boxes": [
                {"bbox": [0.0, 0.0, 200.0, 80.0], "label": "", "label_source": ""},
                {"bbox": [240.0, 0.0, 420.0, 80.0], "label": "", "label_source": ""},
            ],
            "connectors": 1,
            "edges": [{"from_index": 1, "to_index": 2, "from_label": "", "to_label": ""}],
            "unlabeled_boxes": 2,
        }
        ocr_payload = {
            "words": [
                {"text": "Start", "bbox": [10, 10, 80, 25], "confidence": 95.0},
                {"text": "Finish", "bbox": [250, 10, 340, 25], "confidence": 95.0},
            ],
            "image_width": 420,
            "image_height": 80,
        }

        merged = converters._merge_ocr_labels_into_diagram(diagram, ocr_payload, DiagramPage())
        self.assertEqual(merged["boxes"][0]["label"], "Start")
        self.assertEqual(merged["boxes"][0]["label_source"], "ocr")
        self.assertEqual(merged["boxes"][1]["label"], "Finish")
        self.assertEqual(merged["edges"], [{"from_index": 1, "to_index": 2, "from_label": "", "to_label": ""}])
        self.assertEqual(merged["unlabeled_boxes"], 0)

    def test_diagram_labels_can_use_fallback_low_confidence_ocr_words(self) -> None:
        diagram = {
            "boxes": [
                {"bbox": [0.0, 0.0, 200.0, 80.0], "label": "", "label_source": ""},
            ],
            "connectors": 0,
            "edges": [],
            "unlabeled_boxes": 1,
        }
        ocr_payload = {
            "words": [
                {"text": "Valve", "bbox": [20, 10, 90, 25], "confidence": 31.0},
                {"text": "A", "bbox": [95, 10, 110, 25], "confidence": 29.0},
            ],
            "image_width": 420,
            "image_height": 80,
        }

        merged = converters._merge_ocr_labels_into_diagram(diagram, ocr_payload, DiagramPage())
        self.assertEqual(merged["boxes"][0]["label"], "Valve A")
        self.assertEqual(merged["boxes"][0]["label_source"], "ocr")

    def test_diagram_fallback_ocr_labels_follow_bbox_reading_order(self) -> None:
        diagram = {
            "boxes": [
                {"bbox": [0.0, 0.0, 200.0, 80.0], "label": "", "label_source": ""},
            ],
            "connectors": 0,
            "edges": [],
            "unlabeled_boxes": 1,
        }
        ocr_payload = {
            "words": [
                {"text": "Beta", "bbox": [90, 10, 130, 25], "confidence": 35.0},
                {"text": "Alpha", "bbox": [20, 10, 80, 25], "confidence": 28.0},
            ],
            "image_width": 420,
            "image_height": 80,
        }

        merged = converters._merge_ocr_labels_into_diagram(diagram, ocr_payload, DiagramPage())
        self.assertEqual(merged["boxes"][0]["label"], "Alpha Beta")

    def test_diagram_primitives_filter_page_sized_rectangles(self) -> None:
        page_dict = {
            "blocks": [
                {
                    "type": 0,
                    "bbox": (10, 10, 190, 50),
                    "lines": [{"spans": [{"text": "Start", "bbox": (10, 10, 80, 25), "size": 12}]}],
                }
            ]
        }
        diagram = converters._extract_pymupdf_diagram_primitives(LargeRectDiagramPage(), page_dict)
        self.assertEqual(diagram["boxes"], [])

    def test_diagram_primitives_ignore_footer_like_text_bands(self) -> None:
        page_dict = {
            "blocks": [
                {
                    "type": 0,
                    "bbox": (15, 60, 180, 78),
                    "lines": [{"spans": [{"text": "Core Box", "bbox": (20, 62, 90, 74), "size": 12}]}],
                },
                {
                    "type": 0,
                    "bbox": (20, 70, 400, 79),
                    "lines": [{"spans": [{"text": "Footer disclaimer text", "bbox": (20, 71, 180, 78), "size": 10}]}],
                },
            ]
        }
        diagram = converters._extract_pymupdf_diagram_primitives(DiagramPage(), page_dict)
        self.assertEqual(diagram["boxes"][0]["label"], "Core Box")

    def test_diagram_primitives_drop_small_unlabeled_cells(self) -> None:
        page_dict = {
            "blocks": [
                {
                    "type": 0,
                    "bbox": (10, 10, 190, 50),
                    "lines": [{"spans": [{"text": "Named Box", "bbox": (10, 10, 100, 24), "size": 12}]}],
                }
            ]
        }
        diagram = converters._extract_pymupdf_diagram_primitives(DiagramPage(), page_dict)
        labels = [box["label"] for box in diagram["boxes"]]
        self.assertIn("Named Box", labels)

    def test_diagram_primitives_keep_small_unlabeled_cells_when_connected(self) -> None:
        page_dict = {
            "blocks": [
                {
                    "type": 0,
                    "bbox": (10, 10, 80, 25),
                    "lines": [{"spans": [{"text": "Start", "bbox": (10, 10, 80, 25), "size": 12}]}],
                }
            ]
        }
        diagram = converters._extract_pymupdf_diagram_primitives(DiagramPage(), page_dict)
        unlabeled = [box for box in diagram["boxes"] if not box["label"]]
        self.assertTrue(unlabeled)
        self.assertEqual(diagram["connectors"], 1)
        self.assertTrue(diagram["connector_segments"])

    def test_diagram_summary_is_suppressed_for_connector_only_noise(self) -> None:
        diagram = {"boxes": [], "connectors": 18, "edges": [], "unlabeled_boxes": 0}
        self.assertEqual(converters._render_diagram_summary_html(diagram), "")
        self.assertEqual(converters._render_diagram_summary_markdown(diagram), "")

    def test_ocr_profile_uses_page_type_specific_psm_candidates(self) -> None:
        text_profile = converters._build_ocr_profile("text", force=False)
        diagram_profile = converters._build_ocr_profile("diagram", force=False)
        table_profile = converters._build_ocr_profile("table", force=True)

        self.assertEqual(text_profile["psm_candidates"][0], 6)
        self.assertEqual(text_profile["psm_candidates"], [6, 3])
        self.assertEqual(diagram_profile["psm_candidates"][0], 11)
        self.assertEqual(table_profile["psm_candidates"][0], 6)
        self.assertTrue(table_profile["key"].startswith("force-table"))
        self.assertEqual(text_profile["variant_policy"], "fast-text")
        self.assertEqual(diagram_profile["variant_policy"], "full-diagram")
        self.assertEqual(table_profile["variant_policy"], "focused-table")
        self.assertEqual(diagram_profile["word_confidence_floor"], 25)
        variants = converters._build_ocr_profile_variants(diagram_profile)
        self.assertEqual([item["variant"] for item in variants], ["base", "soft", "strong"])
        self.assertGreater(variants[2]["threshold"], variants[0]["threshold"])
        text_variants = converters._build_ocr_profile_variants(text_profile)
        self.assertEqual([item["variant"] for item in text_variants], ["base", "soft"])

    def test_ocr_confidence_summary_includes_distribution_fields(self) -> None:
        summary = converters._summarize_ocr_confidence(
            {
                "text": ["A", "B", "C", ""],
                "conf": ["95", "40", "80", "-1"],
            }
        )

        self.assertEqual(summary["words"], 3)
        self.assertEqual(summary["confident_words"], 1)
        self.assertEqual(summary["low_confidence_words"], 1)
        self.assertAlmostEqual(summary["low_confidence_ratio"], 1 / 3, places=3)
        self.assertIn("median", summary)

    def test_ocr_trial_early_stop_rule_detects_excellent_candidate(self) -> None:
        reason = converters._should_stop_ocr_trials(
            {"avg": 95.2, "low_confidence_ratio": 0.0, "words": 3},
            95.25,
        )
        self.assertEqual(reason, "excellent-confidence")

    def test_ocr_variant_expansion_adds_rescue_variant_only_for_weak_text_results(self) -> None:
        profile = converters._build_ocr_profile("text", force=False)
        expanded = converters._should_expand_ocr_variants(
            profile,
            ["base", "soft"],
            {"avg": 70.0, "low_confidence_ratio": 0.25, "words": 1},
        )
        self.assertEqual([item["variant"] for item in expanded], ["strong"])
        stable = converters._should_expand_ocr_variants(
            profile,
            ["base", "soft"],
            {"avg": 93.0, "low_confidence_ratio": 0.0, "words": 12},
        )
        self.assertEqual(stable, [])

    def test_ocr_profile_cache_token_changes_when_policy_changes(self) -> None:
        profile = converters._build_ocr_profile("text", force=False)
        baseline = converters._ocr_profile_cache_token(profile)
        changed = converters._ocr_profile_cache_token({**profile, "psm_candidates": [6, 4]})
        self.assertNotEqual(baseline, changed)

    def test_extract_ocr_words_respects_min_confidence(self) -> None:
        words = converters._extract_ocr_words(
            {
                "text": ["keep", "drop"],
                "conf": ["28", "22"],
                "left": [0, 0],
                "top": [0, 0],
                "width": [10, 10],
                "height": [10, 10],
            },
            min_confidence=25.0,
        )
        self.assertEqual([item["text"] for item in words], ["keep"])


if __name__ == "__main__":
    unittest.main()
