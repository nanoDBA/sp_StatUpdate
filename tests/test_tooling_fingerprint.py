"""Identity guarantees for the test-tree fingerprint."""
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tooling_fingerprint import tooling_fingerprint


class ToolingFingerprintTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.root = self.base / "tests"
        self.root.mkdir()
        (self.root / "suite.ps1").write_text("assert true", encoding="utf-8")
        self.external = self.base / "verdict.py"
        self.external.write_text("return pass", encoding="utf-8")
        (self.root / "fingerprint-inputs.json").write_text(
            json.dumps([{"label": "verdict", "path": "../verdict.py"}]), encoding="utf-8"
        )

    def tearDown(self):
        self.tmp.cleanup()

    def test_identical_inputs_are_stable(self):
        self.assertEqual(tooling_fingerprint(self.root), tooling_fingerprint(self.root))

    def test_external_verdict_change_changes_fingerprint(self):
        before = tooling_fingerprint(self.root)
        self.external.write_text("return fail", encoding="utf-8")
        self.assertNotEqual(before, tooling_fingerprint(self.root))

    def test_test_tree_change_changes_fingerprint(self):
        before = tooling_fingerprint(self.root)
        (self.root / "suite.ps1").write_text("assert false", encoding="utf-8")
        self.assertNotEqual(before, tooling_fingerprint(self.root))

    def test_volatile_files_are_excluded(self):
        before = tooling_fingerprint(self.root)
        volatile = self.root / "results"
        volatile.mkdir()
        (volatile / "run.json").write_text("volatile", encoding="utf-8")
        (self.root / "debug.log").write_text("volatile", encoding="utf-8")
        self.assertEqual(before, tooling_fingerprint(self.root))

    def test_missing_verdict_input_fails_closed(self):
        self.external.unlink()
        with self.assertRaises(ValueError):
            tooling_fingerprint(self.root)


if __name__ == "__main__":
    unittest.main()
