"""Regression tests for fixed upstream history projection and query strata."""
from __future__ import annotations

import hashlib
import unittest
from pathlib import Path

from src.checker import Checker
from src.corpus import build
from src.real_history import HISTORY_INPUT, SOURCE_RELATIVE, build_real_history

ROOT = Path(__file__).resolve().parents[1]


class RealHistoryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.fixture = build_real_history(ROOT, build(ROOT))

    def test_base_and_commit_inventory(self) -> None:
        history = self.fixture["git_history"]
        source = (ROOT / SOURCE_RELATIVE).read_bytes()
        self.assertEqual(
            hashlib.sha256(source).hexdigest(), history["base"]["file_sha256"]
        )
        self.assertTrue((ROOT / HISTORY_INPUT).is_file())
        self.assertEqual("inputs/git-history/cachetools-function-history.json", history["input"])
        self.assertEqual(6, len(history["stages"]))
        self.assertEqual([0, 6, 0], [len(items) for items in self.fixture["history"]])
        self.assertEqual(
            [
                "d5c7eea7e52d18fed0ec1b575db1956b1d035782",
                "c0fdf6abab38040947d6fe2e38c507401d5e2350",
                "13bb86a55e36e501cf0b3e4c35db516ed9409fd7",
                "39b31bc9b63abe98497409945e9d382d8918c8fb",
                "ccaa8c8c882b7cb76904ea5ae21aae33cca0c2c1",
                "dd181c5a72a74fcc01045c17cc943f72cb521262",
            ],
            [item["commit"] for item in history["stages"]],
        )

    def test_function_change_projection(self) -> None:
        stages = self.fixture["git_history"]["stages"]
        self.assertEqual(
            [1, 1, 1, 1, 2, 1], [len(stage["changes"]) for stage in stages]
        )
        self.assertEqual(
            [
                ["Cache.__setitem__"],
                ["TLRUCache.__setitem__"],
                ["Cache.__init__"],
                ["Cache.__setitem__"],
                ["TLRUCache.__delitem", "TLRUCache.__setitem__"],
                ["Cache.__init__"],
            ],
            [stage["changed_functions"] for stage in stages],
        )
        for event in self.fixture["history"][1]:
            self.assertLessEqual(len(event["changes"]), 16)
            for _, body in event["changes"]:
                self.assertIn(body, self.fixture["payloads"])
                self.assertGreaterEqual(len(self.fixture["payloads"][body]["features"]), 2)

    def test_replacements_use_the_retained_identifier_namespace(self) -> None:
        # Three existing functions are replaced; only __delitem is newly added.
        # Host path separators must not turn a replacement into an insertion.
        base_ids = set().union(*(set(state) for state in self.fixture["initial"]))
        changed_ids = {ident for event in self.fixture["history"][1]
                       for ident, _ in event["changes"]}
        self.assertEqual(3, len(changed_ids & base_ids))
        self.assertEqual(
            {"cachetools/cachetools/__init__.py:TLRUCache.__delitem:1"},
            changed_ids - base_ids,
        )
        self.assertTrue(all('\\' not in ident for ident in base_ids))

    def test_query_strata_and_targeted_plans(self) -> None:
        self.assertEqual(64, self.fixture["base_query_count"])
        self.assertEqual(68, len(self.fixture["queries"]))
        targeted = self.fixture["targeted_queries"]
        self.assertEqual(4, len(targeted))
        self.assertEqual(
            [
                ["attr:getsizeof", "attr:__maxsize", "attr:__size", "attr:__currsize"],
                ["call:getsizeof", "name:diffsize", "call:popitem", "attr:popitem"],
                ["attr:removed", "call:cache_delitem", "attr:__delitem__", "name:cache_delitem"],
                ["call:__ttu", "call:_item", "call:heappush", "attr:_item"],
            ],
            [query["plan"][0] for query in targeted],
        )

    def test_oracle_changes_are_discriminating_and_bounded(self) -> None:
        checker = Checker(self.fixture["payloads"])
        previous = {
            query["id"]: checker.oracle(
                self.fixture["initial"], self.fixture["history"], [0, 0, 0], query
            )
            for query in self.fixture["queries"]
        }
        counts = []
        changed_ids = set()
        for sequence in range(1, 7):
            changed = []
            for query in self.fixture["queries"]:
                answer = checker.oracle(
                    self.fixture["initial"],
                    self.fixture["history"],
                    [0, sequence, 0],
                    query,
                )
                if answer != previous[query["id"]]:
                    changed.append(query["id"])
                    changed_ids.add(query["id"])
                previous[query["id"]] = answer
            counts.append(len(changed))
        self.assertEqual([2, 2, 1, 2, 2, 1], counts)
        self.assertEqual({"rhq000", "rhq001", "rhq002", "rhq003"}, changed_ids)


if __name__ == "__main__":
    unittest.main()
