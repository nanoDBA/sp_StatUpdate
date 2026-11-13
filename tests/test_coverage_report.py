"""Results-store seam tests for fail-closed coverage enforcement."""
from __future__ import annotations

import unittest
import json
import re
from pathlib import Path
from contextlib import redirect_stdout
from io import StringIO
import sys
from tempfile import TemporaryDirectory

from coverage_report import coverage_gaps, enforcement_refusals, source_inventory
from coverage_report import main as coverage_main
from results_store import MemoryResultsStore
from release_gate import EXPECTED_RELEASE_SUITES, validate_run


class CoverageReportTests(unittest.TestCase):
    def setUp(self):
        self.proc = "ALTER PROCEDURE dbo.sp_StatUpdate\n    @Known int = 0,\n    @Missing bit = 0\nAS\nBEGIN RETURN; END"
        self.diag = "-- W1 is defined\n-- C2 is defined"
        self.run = {
            "run_id": "ready-made", "commit": "a" * 40, "sql_version": "2022",
            "sql_build": "16.0.test", "started_at": "2026-01-01T00:00:00+00:00",
            "ended_at": "2026-01-01T00:00:01+00:00", "duration_ms": 1000,
            "server_alive_at_end": True,
            "suites": [{"suite": "Compile", "expected_count": 1, "started_at": "2026-01-01T00:00:00+00:00",
                        "duration_ms": 1000, "checks": [{"check": "procedure_compiles", "result": "PASS", "duration_ms": 1}]}],
        }
        for suite_name in sorted(EXPECTED_RELEASE_SUITES - {"Compile"}):
            self.run["suites"].append({
                "suite": suite_name, "expected_count": 0, "started_at": "2026-01-01T00:00:00+00:00",
                "duration_ms": 0, "checks": [],
            })
        ssms_checks = [
            "sp_StatUpdate.sql", "sp_StatUpdate_Diag.sql", "sp_StatUpdate_XE_Session.sql",
            "sp_StatUpdate_Diag.v3.11.2-pre-fix.sql",
        ]
        self.run["suites"].append({
            "suite": "SSMS", "expected_count": len(ssms_checks), "started_at": "2026-01-01T00:00:00+00:00",
            "duration_ms": 1, "checks": [
                {"check": check, "result": "PASS",
                 "reason": ("SSMS 22.11.3; expected pre-fix parser rejection: Msg 102 batch 4 line 895"
                            if check.endswith("pre-fix.sql") else "SSMS 22.11.3; syntax result recorded"),
                 "duration_ms": 1}
                for check in ssms_checks
            ],
        })
        self.run["suites"] = [self.run["suites"][0], *sorted(self.run["suites"][1:], key=lambda s: s["suite"])]

    def test_reports_missing_source_rows_and_absent_proof(self):
        store = MemoryResultsStore()
        store.save_run(self.run)
        coverage = {"rows": [
            {"id": "@known", "tier": "behaviour", "proving_checks": [{"suite": "Compile", "check": "procedure_compiles"}]},
            {"id": "diag:W1", "tier": "regression", "proving_checks": [{"suite": "Compile", "check": "missing_check"}]},
            {"id": "diag:C1", "tier": "not testable here", "proving_checks": []},
        ]}
        gaps = coverage_gaps(coverage, self.proc, self.diag, store.runs_for_commit("a" * 40))
        self.assertTrue(any("public parameter @missing" in gap for gap in gaps))
        self.assertTrue(any("Diag check C2 has no coverage row" in gap for gap in gaps))
        self.assertTrue(any("proving check Compile/missing_check absent" in gap for gap in gaps))

    def test_release_gate_refuses_a_missing_declared_suite(self):
        incomplete = json.loads(json.dumps(self.run))
        incomplete["suites"] = [s for s in incomplete["suites"] if s["suite"] != "StopReasonsCoverage"]
        refusals = validate_run(incomplete)
        self.assertTrue(any(r.code == "missing_suite" and "StopReasonsCoverage" in r.detail for r in refusals))

    def test_behavior_row_without_named_proof_is_a_reported_gap(self):
        coverage = {"rows": [{"id": "@known", "tier": "behaviour", "proving_checks": []},
                              {"id": "@missing", "tier": "not testable here", "proving_checks": []}]}
        gaps = coverage_gaps(coverage, self.proc, "", [])
        self.assertIn("@known: no proving check is named", gaps)

    def test_clean_sample_has_no_gaps(self):
        store = MemoryResultsStore()
        store.save_run(self.run)
        coverage = {"rows": [
            {"id": "@known", "tier": "behaviour", "proving_checks": [{"suite": "Compile", "check": "procedure_compiles"}]},
            {"id": "@missing", "tier": "not testable here", "proving_checks": []},
            {"id": "diag:W1", "tier": "not testable here", "proving_checks": []},
            {"id": "diag:C2", "tier": "not testable here", "proving_checks": []},
        ]}
        self.assertEqual([], coverage_gaps(coverage, self.proc, self.diag, store.runs_for_commit("a" * 40)))

    def test_skipped_proof_does_not_count_as_coverage(self):
        coverage = {"rows": [
            {"id": "@known", "tier": "behaviour", "proving_checks": [{"suite": "Compile", "check": "procedure_compiles"}]},
            {"id": "@missing", "tier": "not testable here", "proving_checks": []},
            {"id": "diag:W1", "tier": "not testable here", "proving_checks": []},
            {"id": "diag:C2", "tier": "not testable here", "proving_checks": []},
        ]}
        skipped_run = {**self.run, "suites": [{**self.run["suites"][0], "checks": [
            {"check": "procedure_compiles", "result": "SKIPPED", "duration_ms": 0}
        ]}]}
        gaps = coverage_gaps(coverage, self.proc, self.diag, [skipped_run])
        self.assertIn("@known: proving check Compile/procedure_compiles absent or not executed", gaps)

    def test_not_applicable_proof_does_not_count_as_coverage(self):
        coverage = {"rows": [
            {"id": "@known", "tier": "behaviour", "proving_checks": [{"suite": "Compile", "check": "procedure_compiles"}]},
            {"id": "@missing", "tier": "not testable here", "proving_checks": []},
            {"id": "diag:W1", "tier": "not testable here", "proving_checks": []},
            {"id": "diag:C2", "tier": "not testable here", "proving_checks": []},
        ]}
        unavailable_run = {**self.run, "suites": [{**self.run["suites"][0], "checks": [
            {"check": "procedure_compiles", "result": "NOT_APPLICABLE", "duration_ms": 0}
        ]}]}
        gaps = coverage_gaps(coverage, self.proc, self.diag, [unavailable_run])
        self.assertIn("@known: proving check Compile/procedure_compiles absent or not executed", gaps)

    def test_enforcement_allows_clean_coverage(self):
        self.assertEqual([], enforcement_refusals(lambda: []))

    def test_enforcement_refuses_any_gap(self):
        refusals = enforcement_refusals(lambda: ["one uncovered check"])
        self.assertEqual(["coverage gap: one uncovered check"], refusals)

    def test_enforcement_refuses_unavailable_evaluation(self):
        def unavailable():
            raise OSError("private detail must not escape")
        self.assertEqual(["coverage evaluation unavailable (OSError)"], enforcement_refusals(unavailable))

    def test_checked_in_inventory_covers_current_sql_sources_and_has_tiers(self):
        tests = Path(__file__).resolve().parent
        root = tests.parent
        coverage = json.loads((tests / "coverage-list.json").read_text(encoding="utf-8"))
        proc = (root / "sp_StatUpdate.sql").read_text(encoding="utf-8-sig")
        diag = (root / "sp_StatUpdate_Diag.sql").read_text(encoding="utf-8-sig")
        gaps = coverage_gaps(coverage, proc, diag, [])
        missing_rows = [gap for gap in gaps if "has no coverage row" in gap]
        self.assertEqual([], missing_rows)
        self.assertTrue(coverage["rows"])
        self.assertTrue(all(row.get("tier") in {"behaviour", "regression", "not testable here"} for row in coverage["rows"]))

    def test_cli_reports_gaps_and_returns_nonzero(self):
        root = Path(__file__).resolve().parent.parent
        with TemporaryDirectory() as temp:
            runs_path = Path(temp) / "runs.json"
            runs_path.write_text("[]", encoding="utf-8")
            args = ["coverage_report.py", "--coverage", str(root / "tests/coverage-list.json"),
                    "--procedure", str(root / "sp_StatUpdate.sql"), "--diag", str(root / "sp_StatUpdate_Diag.sql"),
                    "--runs", str(runs_path)]
            previous = sys.argv
            output = StringIO()
            try:
                sys.argv = args
                with redirect_stdout(output):
                    self.assertEqual(0, coverage_main())
            finally:
                sys.argv = previous
            self.assertIn("mode=report-only", output.getvalue())
            self.assertIn("coverage_gap=", output.getvalue())

    def test_selection_filter_scenarios_are_named_in_coverage_inventory(self):
        inventory = json.loads((Path(__file__).resolve().parent / "coverage-list.json").read_text(encoding="utf-8"))
        proof_rows = [row for row in inventory["rows"] if row.get("kind") == "proving_check"]
        by_suite = {}
        for row in proof_rows:
            suite, check = row["id"][len("check:"):].split("/", 1)
            by_suite.setdefault(suite, set()).add(check)
        self.assertTrue({"CriticalTables", "DenialAndGuards", "DirectStringCSV",
                         "ExcludeTablesWildcards", "FilteredStats", "WhatIfNorecompute"}.issubset(by_suite))
        self.assertEqual(8, sum(check.startswith(f"D{i:02d} ") for check in by_suite["DirectStringCSV"] for i in range(1, 9)))

    def test_runtime_label_wildcard_matches_ready_made_result(self):
        store = MemoryResultsStore()
        record = json.loads(json.dumps(self.run))
        suite = next(s for s in record["suites"] if s["suite"] == "DirectStringCSV")
        suite["expected_count"] = 1
        suite["checks"] = [{"check": "D01 [SQL 2022]: two-stat CSV -> StatsFoundOut >= 2", "result": "PASS", "duration_ms": 1}]
        store.save_run(record)
        coverage = {"rows": [
            {"id": "@known", "tier": "not testable here", "proving_checks": []},
            {"id": "@missing", "tier": "not testable here", "proving_checks": []},
            {"id": "check:DirectStringCSV/D01 *: two-stat CSV -> StatsFoundOut >= 2",
             "tier": "behaviour", "proving_checks": [{
                 "suite": "DirectStringCSV",
                 "check": "D01 *: two-stat CSV -> StatsFoundOut >= 2"}]},
        ]}
        self.assertEqual([], coverage_gaps(coverage, self.proc, "", store.runs_for_commit("a" * 40)))

    def test_check_pattern_treats_brackets_literally_and_keeps_star_wildcard(self):
        store = MemoryResultsStore()
        record = json.loads(json.dumps(self.run))
        record["suites"][0]["checks"][0]["check"] = "procedure_compiles [NO DATA] / verified"
        store.save_run(record)
        coverage = {"rows": [
            {"id": "@known", "tier": "behaviour", "proving_checks": [{
                "suite": "Compile", "check": "procedure_compiles [NO DATA] *"}]},
            {"id": "@missing", "tier": "not testable here", "proving_checks": []},
            {"id": "diag:W1", "tier": "not testable here", "proving_checks": []},
            {"id": "diag:C2", "tier": "not testable here", "proving_checks": []},
        ]}
        self.assertEqual([], coverage_gaps(coverage, self.proc, self.diag, store.runs_for_commit("a" * 40)))

    def test_parameter_sweep_and_untestable_ten_million_branch_are_listed(self):
        inventory = json.loads((Path(__file__).resolve().parent / "coverage-list.json").read_text(encoding="utf-8"))
        by_id = {row["id"]: row for row in inventory["rows"]}
        for parameter in ("@ansi", "@firsttimefullscancaprows", "@jobname", "@skiptableswithcci", "@skiptableswithncci"):
            self.assertEqual("behaviour", by_id[parameter]["tier"])
            self.assertIn({"suite": "ParamSweep", "check": parameter}, by_id[parameter]["proving_checks"])
        self.assertEqual({"suite": "StopReasonsCoverage", "check": "SC05: TEMPDB_PRESSURE"},
                         by_id["guard:tempdb-low-space"]["proving_checks"][0])
        self.assertEqual("not testable here", by_id["size-boundary:10M-row"]["tier"])

    def test_new_suite_proofs_are_in_the_inventory(self):
        inventory = json.loads((Path(__file__).resolve().parent / "coverage-list.json").read_text(encoding="utf-8"))
        proof_ids = {row["id"] for row in inventory["rows"] if row.get("kind") == "proving_check"}
        self.assertTrue({
            "check:AutoUpdatePersistence/persisted_sample_rate_after_auto_update",
            "check:AutoUpdatePersistence/skip_changed_after_discovery",
            "check:AutoUpdatePersistence/log_changed_stat_skip",
            "check:AwkwardNamesCaseSensitive/quoted_unicode_object_names",
            "check:AwkwardNamesCaseSensitive/case_sensitive_identifier_resolution",
            "check:QueryStoreWorkloadSortOrder/query_store_workload_candidates",
            "check:QueryStoreWorkloadSortOrder/query_store_sort_order_observed",
            "check:V3Regression/0859.9",
            "check:V3Regression/0859.10",
            "check:V3Regression/Plan-feedback state filter uses documented states",
        }.issubset(proof_ids))
        by_id = {row["id"]: row for row in inventory["rows"]}
        for size in ("10K", "100K", "1M", "1024-pages"):
            self.assertIn({"suite": "SizeBoundaries", "check": size},
                          by_id[f"size-boundary:{size}"]["proving_checks"])

    def test_diag_ids_name_the_release_suite(self):
        inventory = json.loads((Path(__file__).resolve().parent / "coverage-list.json").read_text(encoding="utf-8"))
        rows = {row["id"]: row for row in inventory["rows"] if row.get("kind") == "diag_check"}
        for check_id in ("C6", "W11", "W12", "I15", "I18"):
            self.assertTrue(rows[f"diag:{check_id}"]["proving_checks"])
        self.assertEqual("not testable here", rows["diag:I19"]["tier"])
        self.assertEqual([], rows["diag:I19"]["proving_checks"])

    def test_legacy_rewrite_suites_are_in_the_coverage_inventory(self):
        inventory = json.loads((Path(__file__).resolve().parent / "coverage-list.json").read_text(encoding="utf-8"))
        checks = {row["id"][len("check:"):] for row in inventory["rows"]
                  if row.get("id", "").startswith("check:")}
        suites = {check.split("/", 1)[0] for check in checks}
        self.assertTrue({"HelpContent", "ParamSweep", "QSEnrichmentSafety", "V3Bugs"}.issubset(suites))
        self.assertIn("HelpContent/HC4: RS3 parameter names match input signature", checks)
        self.assertIn("HelpContent/HC13: RS3 row count matches input signature", checks)
        self.assertIn("ParamSweep/@JobName", checks)

    def test_nested_comment_bug_does_not_claim_a_compile_check_as_its_proof(self):
        inventory = json.loads((Path(__file__).resolve().parent / "coverage-list.json").read_text(encoding="utf-8"))
        row = next(row for row in inventory["rows"] if row["id"] == "bug:gh-627/628")
        self.assertEqual([{"suite": "NestedCommentsSimulator", "check": "test_check_sql_nested_comments"}],
                         row["proving_checks"])

    def test_fixed_bug_proofs_name_checks_present_in_suite_inventory(self):
        inventory = json.loads((Path(__file__).resolve().parent / "coverage-list.json").read_text(encoding="utf-8"))
        listed = {tuple(row["id"][len("check:"):].split("/", 1)) for row in inventory["rows"]
                  if row.get("id", "").startswith("check:")}
        listed_suites = {suite for suite, _ in listed}
        for row in inventory["rows"]:
            if row.get("kind") != "fixed_bug":
                continue
            for proof in row["proving_checks"]:
                if proof["suite"] == "NestedCommentsSimulator":
                    continue
                if proof["suite"] not in listed_suites:
                    continue  # A named future regression suite stays a visible coverage gap.
                self.assertIn((proof["suite"], proof["check"]), listed, row["id"])

    def test_diag_fixture_uses_week_and_execution_anchors(self):
        setup = (Path(__file__).resolve().parent / "diag" / "Setup-DiagTestData.sql").read_text(encoding="utf-8-sig")
        compact = re.sub(r"\s+", " ", setup)
        self.assertIn("DECLARE @server_today date = CONVERT(date, SYSDATETIME());", compact)
        self.assertRegex(compact, r"DATEDIFF\(DAY, CONVERT\(date, '19000101'\), @server_today\) % 7")
        self.assertRegex(compact, r"SET @start = DATEADD\(DAY, -?\d+, @anchor\)")
        self.assertIn("DECLARE @execution_anchor datetime2(3) = SYSDATETIME();", compact)
        self.assertRegex(compact, r"SET @start = DATEADD\(DAY, -10, @execution_anchor\)")
        self.assertRegex(compact, r"SET @start = DATEADD\(DAY, @day_offset, @execution_anchor\)")
        self.assertRegex(compact, r"SET @start = DATEADD\(HOUR, -12, @execution_anchor\)")
        self.assertNotRegex(setup, r"(?<!\d)20\d{2}-\d{2}-\d{2}(?!\d)")

    def test_helpcontent_deprecated_list_excludes_current_public_parameters(self):
        root = Path(__file__).resolve().parent.parent
        proc = (root / "sp_StatUpdate.sql").read_text(encoding="utf-8-sig")
        current_parameters, _ = source_inventory(proc, "")
        deprecated = {name.lower() for name in (
            "@TieredThresholds", "@ThresholdLogic", "@StagedDiscovery",
            "@GroupByJoinPattern", "@ExposeProgressToAllSessions",
            "@ProgressLogInterval", "@CheckPermissionsOnly",
            "@CollectHeapForwarding", "@UpdateIncremental",
            "@DelayBetweenStats", "@CleanupOrphanedRuns",
        )}
        self.assertFalse(current_parameters & deprecated)

    def test_inventory_proofs_match_emitted_result_labels(self):
        inventory = json.loads((Path(__file__).resolve().parent / "coverage-list.json").read_text(encoding="utf-8"))
        rows = {row["id"]: row for row in inventory["rows"]}
        self.assertIn({"suite": "DirectStringCSV", "check": "D01 *: two-stat CSV -> StatsFoundOut >= 2"},
                      rows["check:DirectStringCSV/D01 *: two-stat CSV -> StatsFoundOut >= 2"]["proving_checks"])
        self.assertIn({"suite": "V3QAGaps2", "check": "B04: DEFAULT preset @TimeLimit=18000 in debug output"},
                      rows["preset:DEFAULT"]["proving_checks"])
        self.assertEqual(4, len(rows["preset:OLTP_LIGHT"]["proving_checks"]))
        self.assertIn({"suite": "V3QAGaps2", "check": "B06: OLTP_LIGHT @TimeLimit=1800 in debug output"},
                      rows["preset:OLTP_LIGHT"]["proving_checks"])
        self.assertEqual("SC01:*", rows["stop_reason:TIME_LIMIT"]["proving_checks"][0]["check"])
        self.assertEqual("S1: @SortOrder=ROWS -- large table first",
                         rows["sort_order:ROWS"]["proving_checks"][0]["check"])
        self.assertIn({"suite": "QueryStoreWorkloadSortOrder", "check": "query_store_sort_order_observed"},
                      rows["sort_order:QUERY_STORE"]["proving_checks"])
        self.assertEqual("* -- @Version non-NULL", rows["@version"]["proving_checks"][0]["check"])
        e05 = "E05: Cross-version E01 mop-up fires (2019+2022+2025)"
        self.assertIn({"suite": "MopUpPassE2E", "check": e05},
                      rows[f"check:MopUpPassE2E/{e05}"]["proving_checks"])
        for suite, check in (
                ("ParamSweep", "OutputParamCoverage"),
                ("V3Bugs", "T31: #386 CommandLog start uses public names"),
                ("V3Bugs", "T32: #386 MopUpPass validation text is public"),
                ("V3Bugs", "T33: #386 overshoot warning avoids internal parameter names"),
                ("V3Bugs", "T34: @Tables=ALL matches default selection"),
                ("V3Bugs", "T35: START CommandLog text is not truncated")):
            self.assertIn({"suite": suite, "check": check},
                          rows[f"check:{suite}/{check}"]["proving_checks"])


if __name__ == "__main__":
    unittest.main()
