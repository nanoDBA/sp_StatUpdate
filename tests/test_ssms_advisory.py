import unittest

from release_gate import EXPECTED_RELEASE_SUITES, evaluate_gate


class SsmsBlockingTests(unittest.TestCase):
    def make_runs(self, control_result="PASS"):
        commit = "a" * 40
        ssms_names = (
            "sp_StatUpdate.sql",
            "sp_StatUpdate_Diag.sql",
            "sp_StatUpdate_XE_Session.sql",
            "sp_StatUpdate_Diag.v3.11.2-pre-fix.sql",
        )
        runs = []
        for version in ("2019", "2022", "2025"):
            suites = [
                {
                    "suite": name,
                    "expected_count": 0,
                    "checks": [],
                }
                for name in sorted(EXPECTED_RELEASE_SUITES)
            ]
            checks = [
                {
                    "check": name,
                    "result": control_result if name.endswith("pre-fix.sql") else "PASS",
                    "reason": ("SSMS 22.11.3; expected pre-fix parser rejection: Msg 102 batch 4 line 895"
                               if name.endswith("pre-fix.sql") else "SSMS 22.11.3; parser result recorded"),
                    "duration_ms": 1,
                }
                for name in ssms_names
            ]
            suites.extend((
                {"suite": "SSMS", "expected_count": len(checks), "checks": checks},
                {"suite": "Coverage", "expected_count": 1, "checks": [
                    {"check": "coverage_inventory", "result": "PASS", "reason": "report-only", "duration_ms": 1}
                ]},
            ))
            runs.append({
                "run_id": version,
                "commit": commit,
                "sql_version": version,
                "sql_build": "test",
                "server_alive_at_end": True,
                "suites": suites,
            })
        return commit, runs

    def test_expected_control_passes_only_for_parser_rejection(self):
        commit, runs = self.make_runs()
        for run in runs:
            control = next(row for suite in run["suites"] if suite["suite"] == "SSMS"
                           for row in suite["checks"] if row["check"].endswith("pre-fix.sql"))
            control["reason"] = "SSMS 22.11.3; expected pre-fix parser rejection: Msg 102 batch 4 line 895"
        decision = evaluate_gate(commit, runs, lambda _: False)
        self.assertTrue(decision.would_tag)

    def test_ssms_failure_blocks_the_gate(self):
        commit, runs = self.make_runs(control_result="FAIL")
        decision = evaluate_gate(commit, runs, lambda _: False)
        self.assertFalse(decision.would_tag)
        self.assertTrue(any(refusal.code == "ssms_check" for refusal in decision.refusals))

    def test_coverage_is_report_only(self):
        commit, runs = self.make_runs()
        for run in runs:
            coverage = next(row for row in run["suites"] if row["suite"] == "Coverage")["checks"][0]
            coverage.update(result="PASS", reason="report-only: 90 coverage gaps")
        decision = evaluate_gate(commit, runs, lambda _: False)
        self.assertTrue(decision.would_tag)


if __name__ == "__main__":
    unittest.main()
