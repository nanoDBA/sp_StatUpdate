from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from release_gate import EXPECTED_RELEASE_SUITES, evaluate_gate, evaluate_version, validate_run
from results_store import write_run_list

COMMIT = "a" * 40
FINGERPRINT = "b" * 64


def valid_record(version="2022"):
    suites = [
        {
            "suite": name,
            "expected_count": 1,
            "started_at": "2026-11-12T12:00:00+00:00",
            "duration_ms": 1,
            "checks": [{"check": "sample", "result": "PASS", "duration_ms": 1}],
        }
        for name in sorted(EXPECTED_RELEASE_SUITES)
    ]
    return {
        "run_id": "run-test",
        "commit": COMMIT,
        "tooling_sha256": FINGERPRINT,
        "orchestration_id": "unit-test",
        "sql_version": version,
        "sql_build": "16.0.test",
        "started_at": "2026-11-12T12:00:00+00:00",
        "ended_at": "2026-11-12T12:01:00+00:00",
        "duration_ms": 60_000,
        "server_alive_at_end": True,
        "suites": suites,
    }


class PortableGateTests(unittest.TestCase):
    def test_single_version_accepts_complete_portable_run_without_ssms(self):
        record = valid_record()
        self.assertEqual([], validate_run(record, require_ssms=False))
        self.assertTrue(evaluate_version(COMMIT, "2022", record).would_tag)

    def test_single_version_rejects_missing_declared_suite(self):
        record = valid_record()
        record["suites"].pop()
        decision = evaluate_version(COMMIT, "2022", record)
        self.assertFalse(decision.would_tag)
        self.assertIn("missing_suite", {item.code for item in decision.refusals})

    def test_single_version_rejects_failed_check_and_wrong_identity(self):
        record = valid_record()
        record["suites"][0]["checks"][0].update(result="FAIL", reason="expected failure")
        decision = evaluate_version("c" * 40, "2019", record)
        codes = {item.code for item in decision.refusals}
        self.assertIn("fail", codes)
        self.assertIn("wrong_commit", codes)
        self.assertIn("wrong_version", codes)

    def test_release_gate_still_requires_all_versions_and_blocking_ssms_report(self):
        decision = evaluate_gate(COMMIT, [valid_record()], lambda _issue: True)
        codes = {item.code for item in decision.refusals}
        self.assertIn("missing_version", codes)
        self.assertIn("missing_ssms", codes)

    def test_local_result_file_is_written_after_validation(self):
        record = valid_record()
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "run.json"
            write_run_list(output, record, require_ssms=False)
            self.assertEqual(record, json.loads(output.read_text(encoding="utf-8")))


if __name__ == "__main__":
    unittest.main()
