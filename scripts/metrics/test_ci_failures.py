import json
import subprocess
import unittest

import ci_failures as cf


class ClassifyTest(unittest.TestCase):
    def test_categories(self):
        cases = [
            ("error: no worker has room for the actor", "no_free_workers", None),
            ("timed out waiting for the condition on pods", "e2e_timeout", None),
            ("AccessDenied: bucket", "storage_access", None),
            ("running 3 tests\ntest store::tests::put ... FAILED\n", "named_test_fail", "store::tests::put"),
            ("FAILED: scripts/verify/fmt.sh\n", "verify_fail", None),
            ("all fine", "unknown", None),
        ]
        for log, cat, detail in cases:
            with self.subTest(cat=cat):
                self.assertEqual(cf.classify(log), (cat, detail))

    def test_infrastructure_wins_over_named_test(self):
        log = "test a::b ... FAILED\nno free workers available\n"
        self.assertEqual(cf.classify(log)[0], "no_free_workers")

    def test_clean_log(self):
        raw = "2026-01-02T03:04:05.123Z \x1b[31mtest x ... FAILED\x1b[0m"
        self.assertEqual(cf.clean_log(raw), "test x ... FAILED")


def fake_run(responses):
    def run(cmd, capture_output, text):
        for (verb, arg), (code, out) in responses.items():
            if cmd[1] == verb and cmd[2].startswith(arg):
                return subprocess.CompletedProcess(cmd, code, out, "boom" if code else "")
        return subprocess.CompletedProcess(cmd, 1, "", "unexpected")
    return run


class AnalyseTest(unittest.TestCase):
    def test_end_to_end_with_fake_gh(self):
        runs = [{"databaseId": 1, "conclusion": "failure"}, {"databaseId": 2, "conclusion": "success"}]
        responses = {
            ("run", "list"): (0, json.dumps(runs)),
            ("api", "repos/o/r/actions/runs/"): (0, json.dumps([{"id": 9, "name": "unit"}])),
            ("api", "repos/o/r/actions/jobs/"): (0, "test a::b ... FAILED\ntest a::b ... FAILED\n"),
        }
        data = cf.analyse(cf.Gh(fake_run(responses)), "o/r", "ci", 10, 5)
        self.assertEqual(data["total_sampled"], 1)
        self.assertEqual(data["category_counts"], {"named_test_fail": 1})
        self.assertEqual(data["test_fail_counts"], {"a::b": 2})
        self.assertIn("2x  a::b", cf.report(data))

    def test_log_fetch_failure(self):
        runs = [{"databaseId": 1, "conclusion": "failure"}]
        responses = {
            ("run", "list"): (0, json.dumps(runs)),
            ("api", "repos/o/r/actions/runs/"): (0, json.dumps([{"id": 9, "name": "unit"}])),
            ("api", "repos/o/r/actions/jobs/"): (1, ""),
        }
        data = cf.analyse(cf.Gh(fake_run(responses)), "o/r", "ci", 10, 5)
        self.assertEqual(data["category_counts"], {"log_fetch_failed": 1})


if __name__ == "__main__":
    unittest.main()
