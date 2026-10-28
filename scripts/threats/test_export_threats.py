import json
import tempfile
import unittest
from pathlib import Path

import export_threats as et

TABLE = """
Intro text.

| Threat ID | Priority | Threat | Mitigating Invariants | Notes |
|---|:--:|---|---|---|
| T-01 | High | Actor escapes \\| sandbox | Isolation | n1 |
| T-02 | Low | Replay of tokens | Expiry | |
|  | | | | |

Paragraph ends the table.

| Name | Value |
|---|---|
| not | threat |
"""


class ExtractTest(unittest.TestCase):
    def test_reads_rows_and_unescapes(self):
        got = et.extract(TABLE)
        self.assertEqual(len(got), 2)
        self.assertEqual(got[0]["threat"], "Actor escapes | sandbox")
        self.assertEqual(got[0]["priority"], "High")
        self.assertEqual(got[1]["notes"], "")
        self.assertNotIn("suggested_concrete_mitigations", got[0])

    def test_key_order_follows_columns_map(self):
        got = et.extract(TABLE)
        self.assertEqual(list(got[0]), ["threat_id", "priority", "threat", "mitigating_invariants", "notes"])

    def test_rejects_bad_and_duplicate_ids(self):
        bad = "| Threat ID | Threat |\n|---|---|\n| X1 | t |\n"
        with self.assertRaises(ValueError):
            et.extract(bad)
        dup = "| Threat ID | Threat |\n|---|---|\n| T-1 | a |\n| T-1 | b |\n"
        with self.assertRaises(ValueError):
            et.extract(dup)

    def test_main_writes_json(self):
        with tempfile.TemporaryDirectory() as d:
            md = Path(d, "tm.md")
            md.write_text(TABLE)
            out = Path(d, "t.json")
            self.assertEqual(et.main(["--md", str(md), "--out", str(out)]), 0)
            self.assertEqual(len(json.loads(out.read_text())), 2)
            self.assertTrue(out.read_text().endswith("\n"))

    def test_main_missing_file(self):
        self.assertEqual(et.main(["--md", "/nonexistent.md", "--out", "/dev/null"]), 1)


if __name__ == "__main__":
    unittest.main()
