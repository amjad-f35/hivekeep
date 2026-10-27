"""Tests for sweperf_catalog.py:
python3 crates/hivekeep-bench/workloads/manifests/sweperf/test_sweperf_catalog.py
"""

import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import sweperf_catalog as cat  # noqa: E402

PINNED = "example.com/sweperf@sha256:" + "0" * 64


def catalog_file(d: pathlib.Path, text: str) -> pathlib.Path:
    path = d / "tasks.tsv"
    path.write_text(text)
    return path


class ShippedCatalogTest(unittest.TestCase):
    def test_shipped_catalog_and_template_render(self):
        # Fails when tasks.tsv or the template changed in a way deploy.sh
        # cannot render.
        tasks = cat.load_catalog(cat.CATALOG)
        self.assertTrue(tasks)
        template = cat.TEMPLATE.read_text()
        self.assertEqual(cat.check_template(template), [])
        for fname, content in cat.render(tasks, template, "example.com/repo").items():
            with self.subTest(fname=fname):
                self.assertNotIn("${SWEPERF_", content)
                # deploy.sh fills the snapshot location for every template.
                self.assertIn("${SNAPSHOT_BASE_URI}", content)


class RenderTest(unittest.TestCase):
    TEMPLATE = cat.TEMPLATE.read_text()

    def test_render_fills_task_values(self):
        out = cat.render([{"name": "django-11099", "image": PINNED, "steps": 15}], self.TEMPLATE)
        content = out["sweperf-django-11099-template.yaml"]
        self.assertIn("name: sweperf-django-11099", content)
        self.assertIn("- name: django", content)
        self.assertIn(f'image: "{PINNED}"', content)
        self.assertIn('value: "15"', content)

    def test_missing_image_uses_the_repository(self):
        out = cat.render([{"name": "sympy-13647", "image": None, "steps": 33}], self.TEMPLATE, "r.example/x/")
        self.assertIn('image: "r.example/x/sweperf:sympy-13647"', out["sweperf-sympy-13647-template.yaml"])
        with self.assertRaises(ValueError):
            cat.render([{"name": "sympy-13647", "image": None, "steps": 33}], self.TEMPLATE)


class CheckTemplateTest(unittest.TestCase):
    def test_reports_missing_and_unknown_placeholders(self):
        text = "name: sweperf-${SWEPERF_TASK}\nimage: ${SWEPERF_IMAGE}\nx: ${SWEPERF_EXTRA}\nuri: ${SNAPSHOT_BASE_URI}\n"
        problems = cat.check_template(text)
        kinds = sorted(p.split(":")[0] for p in problems)
        self.assertEqual(kinds, ["missing placeholder", "missing placeholder", "unknown placeholder"], problems)
        self.assertTrue(any("SWEPERF_EXTRA" in p for p in problems))


class LoadCatalogTest(unittest.TestCase):
    def test_parses_lines_and_skips_comments(self):
        with tempfile.TemporaryDirectory() as d:
            path = catalog_file(pathlib.Path(d), f"# header\n\nastropy-7336\t21\ndjango-11099\t15\t{PINNED}\n")
            tasks = cat.load_catalog(path, require_pinned=False)
        self.assertEqual(tasks, [
            {"name": "astropy-7336", "steps": 21, "image": None},
            {"name": "django-11099", "steps": 15, "image": PINNED},
        ])

    def test_catalog_rejects_bad_entries(self):
        cases = {
            "missing steps": ("astropy-7336\n", False),
            "zero steps": ("astropy-7336\t0\n", False),
            "word steps": ("astropy-7336\tmany\n", False),
            "bad name": ("Astropy_7336\t3\n", False),
            "too many columns": (f"astropy-7336\t3\t{PINNED}\textra\n", False),
            "unpinned image": ("a-1\t1\texample.com/sweperf:a\n", True),
            "no image when pinning": ("a-1\t1\n", True),
            "duplicate name": ("a-1\t1\na-1\t2\n", False),
        }
        for label, (text, pinned) in cases.items():
            with self.subTest(label), tempfile.TemporaryDirectory() as d:
                with self.assertRaises(ValueError):
                    cat.load_catalog(catalog_file(pathlib.Path(d), text), require_pinned=pinned)


if __name__ == "__main__":
    unittest.main()
