from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

import validate_release


class ReleaseValidationTest(unittest.TestCase):
    def test_normalize_title_ignores_latex_punctuation(self):
        self.assertEqual(
            validate_release.normalize_title("Authenticating top-$k$ queries"),
            "authenticating top k queries",
        )

    def test_bib_parser_reads_all_records_and_required_fields(self):
        paper = validate_release.ROOT.parent / "paper"
        if not paper.is_dir():
            self.skipTest("standalone artifact has no sibling paper")
        entries = validate_release.parse_bib(paper / "references.bib")
        self.assertEqual(validate_release.EXPECTED_REFERENCES, len(entries))
        for entry in entries.values():
            self.assertTrue(entry["author"])
            self.assertTrue(entry["title"])
            self.assertTrue(entry["year"])
            self.assertTrue(entry["url"])

    def test_claim_evidence_paths_exist(self):
        result = validate_release.validate_claim_ledger()
        self.assertEqual(0, result["missing_evidence_paths"])

    def test_reference_ledgers_close_without_paper(self):
        result = validate_release.validate_references(None)
        self.assertGreaterEqual(result["references"], validate_release.MIN_REFERENCES)
        self.assertEqual(21, result["complete_main_text_records"])

    def test_frozen_results_and_negative_controls(self):
        result = validate_release.validate_results(check_reproductions=False, check_trace_inventory=False)
        self.assertEqual(220997, result["finite_instances"])
        self.assertEqual(1991, result["real_history_source_receipts_replayed"])

    def test_history_base_and_commit_projection_metadata(self):
        result = validate_release.validate_history()
        self.assertEqual(6, result["selected_commits"])
        self.assertEqual(7, result["function_assignments"])

    def test_parser_rejects_duplicate_bib_key(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "x.bib"
            path.write_text("@article{x,\n title={A}\n}\n@article{x,\n title={B}\n}\n")
            with self.assertRaises(validate_release.ValidationError):
                validate_release.parse_bib(path)


if __name__ == "__main__":
    unittest.main()
