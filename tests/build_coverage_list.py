"""Regenerate the source inventory in coverage-list.json."""
from __future__ import annotations

import json
import re
from pathlib import Path

from coverage_report import source_inventory

ROOT = Path(__file__).resolve().parent.parent
OUTPUT = Path(__file__).with_name("coverage-list.json")


def row(identifier: str, kind: str, tier: str, proofs=(), note="") -> dict:
    return {"id": identifier, "kind": kind, "tier": tier,
            "proving_checks": [{"suite": suite, "check": check} for suite, check in proofs],
            "note": note}


def main() -> None:
    proc = (ROOT / "sp_StatUpdate.sql").read_text(encoding="utf-8-sig")
    diag = (ROOT / "sp_StatUpdate_Diag.sql").read_text(encoding="utf-8-sig")
    parameters, diag_checks = source_inventory(proc, diag)
    signature = re.search(r"\bALTER\s+PROCEDURE\s+(?:\[?dbo\]?\.)?\[?sp_StatUpdate\]?\b(.*?)\nAS\s*\n", proc, re.I | re.S).group(1)
    output_params = {"@" + name.lower() for name in re.findall(r"@([A-Za-z][A-Za-z0-9_]*)\s+[A-Za-z][A-Za-z0-9_]*(?:\s*\([^)]*\))?\s*=[^,\r\n]*\bOUTPUT\b", signature, re.I)}

    rows = []
    untested = set()
    output_proofs = {
        "@version": ("OutputParamCoverage", "* -- @Version non-NULL"),
        "@versiondate": ("OutputParamCoverage", "* -- @VersionDate non-NULL"),
        "@durationsecondsout": ("OutputParamCoverage", "* -- @DurationSecondsOut >= 0"),
        "@stopreasonout": ("OutputParamCoverage", "* -- @StopReasonOut non-empty"),
        "@warningsout": ("OutputParamCoverage", "P07 (@WarningsOut on warning path) -- @WarningsOut populated"),
    }
    for parameter in sorted(parameters):
        proofs = []
        tier = "behaviour"
        if parameter in untested:
            proofs.append(("ParameterBehavior", parameter))
            note = "Behaviour proof is planned; the named check is not in the current release results yet."
        elif parameter in output_params:
            if parameter in output_proofs:
                proofs.append(output_proofs[parameter])
            else:
                proofs.append(("OutputParamCoverage", "* -- count params non-NULL"))
            note = "Output behavior proof is planned; its named result is not in the current release results yet."
        else:
            proofs.append(("ParamSweep", parameter))
            note = "Parameter behavior proof is planned; its named result is not in the current release results yet."
        if parameter in output_params:
            note = "OUTPUT contract; behavior is checked across early-return paths."
        rows.append(row(parameter, "public_parameter", tier, proofs, note))

    preset_map = {
        "DEFAULT": ("balanced default; 5-hour limit", [("V3QAGaps2", "B04: DEFAULT preset @TimeLimit=18000 in debug output")]),
        "NIGHTLY": ("one-hour Query Store-prioritized run with mop-up", [
            ("V3QAGaps2", "B05: NIGHTLY preset @TimeLimit=3600 and QUERY_STORE in debug"),
            ("PresetStopByTime", "P1: NIGHTLY preset compiles"),
        ]),
        "WEEKLY_FULL": ("four-hour full-scan run", [
            ("V3QAGaps2", "B02: WEEKLY_FULL preset generates FULLSCAN/SAMPLE 100 commands"),
            ("PresetStopByTime", "P2: WEEKLY_FULL preset compiles (FULLSCAN)"),
        ]),
        "OLTP_LIGHT": ("30-minute low-impact run", [
            ("V3QAGaps2", "B03: OLTP_LIGHT preset uses auto-sample (no FULLSCAN/explicit SAMPLE)"),
            ("V3QAGaps2", "B06: OLTP_LIGHT @TimeLimit=1800 in debug output"),
            ("PresetStopByTime", "P3: OLTP_LIGHT preset compiles without error"),
            ("PresetStopByTime", "P5: Preset @TimeLimit takes priority (documented)"),
        ]),
        "WAREHOUSE": ("unbounded full-scan run", [
            ("V3QAGaps2", "B01: WAREHOUSE preset generates FULLSCAN/SAMPLE 100 commands"),
            ("PresetStopByTime", "P4: WAREHOUSE preset compiles without error"),
        ]),
    }
    for preset, (description, proofs) in preset_map.items():
        rows.append(row("preset:" + preset, "preset", "behaviour",
                        proofs, description))

    sort_match = re.search(r"@i_sort_order\s+NOT\s+IN\s*\((.*?)\)", proc, re.I | re.S)
    sort_orders = re.findall(r"N'([A-Z_]+)'", sort_match.group(1)) if sort_match else []
    sort_proofs = {
        "ROWS": ("SortOrderCoverage", "S1: @SortOrder=ROWS -- large table first"),
        "AUTO_CREATED": ("SortOrderCoverage", "S2: @SortOrder=AUTO_CREATED -- user stat before auto stat"),
        "FILTERED_DRIFT": ("SortOrderCoverage", "S3: @SortOrder=FILTERED_DRIFT -- high-drift stat first"),
        "QUERY_STORE": ("SortOrderCoverage", "S4: @SortOrder=QUERY_STORE -- QS-boosted stat first"),
        "DAYS_STALE": ("V3QAGaps2", "C04: @SortOrder=DAYS_STALE dry-run succeeds"),
    }
    for value in sort_orders:
        proof = sort_proofs.get(value, ("SortOrderCoverage", f"observable order: {value}"))
        proofs = [proof]
        if value == "QUERY_STORE":
            proofs.append(("QueryStoreWorkloadSortOrder", "query_store_sort_order_observed"))
        rows.append(row("sort_order:" + value, "sort_order", "behaviour",
                        proofs, "Observable ordering proof expected."))

    stop_reasons = set(re.findall(r"@stop_reason\s*=\s*N?'([A-Z_]+)'", proc, re.I))
    stop_reasons.update(re.findall(r"\bStopReason\s*=\s*N?'([A-Z_]+)'", proc, re.I))
    stop_reasons.update(re.findall(r"N'([A-Z_]+)'\s+AS\s+StopReason", proc, re.I))
    stop_check_numbers = {
        "TIME_LIMIT": "SC01", "BATCH_LIMIT": "SC02", "COMPLETED": "SC03",
        "NO_QUALIFYING_STATS": "SC04", "TEMPDB_PRESSURE": "SC05",
        "FAIL_FAST": "SC06", "CONSECUTIVE_FAILURES": "SC07",
        "COMPLETED_WITH_SKIPPED_DBS": "SC08", "NATURAL_END": "SC09",
        "PARAMETER_ERROR": "SC10", "ALREADY_RUNNING": "SC11",
    }
    for value in sorted(stop_reasons):
        check_id = stop_check_numbers.get(value, f"StopReason={value}")
        rows.append(row("stop_reason:" + value, "stop_reason", "behaviour",
                        [("StopReasonsCoverage", f"{check_id}:*")], "Stop-reason behavior needs an explicit result check."))

    for parameter in sorted(output_params):
        proof = output_proofs.get(parameter, ("OutputParamCoverage", "* -- count params non-NULL"))
        rows.append(row("output:" + parameter, "output", "behaviour",
                        [proof], "Value and early-return paths should be asserted."))

    for check_id in sorted(diag_checks):
        rows.append(row("diag:" + check_id, "diag_check", "behaviour",
                        [("StatUpdateDiag", check_id)], "Diag-specific assertion from the Diag release-gate suite."))

    bug_proofs = [
        ("gh-492/493", [("DirectStringCSV", "D02 *: @TargetNorecompute=N filters NORECOMPUTE stat from CSV"),
                         ("DirectStringCSV", "D03 *: @ExcludeStatistics filters stat from CSV")], "DIRECT_STRING filtering regression"),
        ("gh-451", [("OutputParamCoverage", "P03 (PARAM_ERROR) -- @Version OUTPUT param"),
                     ("OutputParamCoverage", "P03 (PARAM_ERROR) -- @StopReasonOut OUTPUT param")], "early-return OUTPUT parameters"),
        ("rse2", [("EarlyEndRows", "E04-NO_ORPHANED_STARTS")], "balanced END rows on early exits"),
        ("gh-556/rcng/mtt4", [("QSCacheKey", "Q06-schema-key"), ("QSCacheKey", "Q07-3b-schema-key"),
                               ("QSCacheKey", "Q08-boost-survives")], "Query Store cache-key regression"),
        ("btr9", [("PartitionedIncremental", "PI3: Truncated partition handling"),
                   ("PartitionedIncremental", "PI3: Truncated partition 3 excluded from ON PARTITIONS")], "truncated partition handling"),
        ("8gzw/v31y", [("ConcurrentParallel", "P1: No concurrent updates on same table (mutual exclusion)"),
                        ("ConcurrentParallel", "P2: No duplicate UPDATE_STATISTICS per stat per run"),
                        ("ConcurrentParallel", "P5: Dead worker claim released by new worker")], "parallel worker failure masking"),
        ("402-405", [("V3Fixes", "T60: #402 LongRunningThreshold override (behavioral)"),
                     ("V3Fixes", "T61: #403 No @i_ labels in debug header"),
                     ("V3Fixes", "T62: #405 START Command uses public param names"),
                     ("V3Fixes", "T63: #404 No 'triggering legacy fallback' phrase")], "debug and Query Store regression fixes"),
        ("gh-515", [("V3Coverage", "T80: gh-515 ExtendedInfo carries RunLabel")], "RunLabel persistence"),
        ("381-387", [("V3Bugs", "T20: #381 Preset threshold passthrough"), ("V3Bugs", "T21: #382 ROWS sort order"),
                      ("V3Bugs", "T22: #383 sp_getapplock param name"), ("V3Bugs", "T23: #384 LongRunningSamplePercent passthrough"),
                      ("V3Bugs", "T24: #385 @StaleHours hours vs days"), ("V3Bugs", "T30: #387 @QueryStoreTopPlans accepted")], "v3 refactor regressions"),
        ("gh-550", [("FreshStatQualification", "A: Fresh stat on populated table qualifies"),
                     ("FreshStatQualification", "B: Fresh stat on EMPTY table does NOT qualify")], "never-computed statistics qualification"),
        ("gh-549", [("AbortOnIntegrityError", "T_AIE1: @AbortOnIntegrityError=1 accepted in dry run"),
                     ("AbortOnIntegrityError", "T_AIE3: AbortOnIntegrityError=1 in START XML (default omitted)")], "integrity-error abort policy"),
        ("gh-551/gh-554", [("DenialAndGuards", "T_DAG1: ALREADY_RUNNING writes SP_STATUPDATE_DENIED"),
                            ("DenialAndGuards", "T_DAG3: FILTERED_DRIFT precedence")], "denial artifact and FILTERED_DRIFT precedence"),
        ("gh-522", [("FilteredStats", "F1: EXCLUDE - filtered absent, unfiltered present"),
                     ("FilteredStats", "F5: QualifyReason=FILTERED_DRIFT in CommandLog ExtendedInfo")], "filtered statistics behavior"),
        ("gh-533/534", [("QSForcedBaselineAndWaitCpu", "T1: WAIT_CPU accepted (dry run)"),
                         ("QSForcedBaselineAndWaitCpu", "T5: Debug baseline-capture note emitted")], "forced-plan baseline and WAIT_CPU"),
        ("Warnings contract", [("WarningsContract", "T_WC1: EMPTY_STRING_PARAM in END row WarningsCodes element"),
                               ("WarningsContract", "T_WC2: @WarningsCodesOut OUTPUT contains EMPTY_STRING_PARAM")], "stable warning codes"),
        ("jgsp", [("ExcludeTablesWildcards", "ET1: ET_TableA excluded"),
                   ("ExcludeTablesWildcards", "ET3: All ET_ tables excluded by multi-pattern")], "wildcard table exclusion"),
        ("i1ra", [("WhatIfNorecompute", "NR1: NORECOMPUTE stat has WITH NORECOMPUTE"),
                   ("WhatIfNorecompute", "NR3: NORECOMPUTE stat excluded when TargetNorecompute=N")], "WhatIf NORECOMPUTE behavior"),
        ("h74k", [("VersionTokenGating", "TG11: SQL 2019 cross-version anchor (CPU)"),
                   ("VersionTokenGating", "TG12: SQL 2025 cross-version anchor (all tokens)")], "version-token gating"),
        ("gh-375", [("LegacyFallback", "F1: Staged vs legacy produce same StatsFound count"),
                    ("LegacyFallback", "F2: Legacy mode compiles and finds stats")], "legacy/staged parity"),
        ("gh-377", [("PresetStopByTime", "P1: NIGHTLY preset compiles"),
                     ("PresetStopByTime", "P2: WEEKLY_FULL preset compiles (FULLSCAN)"),
                     ("PresetStopByTime", "P3: OLTP_LIGHT preset compiles without error"),
                     ("PresetStopByTime", "P4: WAREHOUSE preset compiles without error")], "preset and stop-by-time behavior"),
        ("QA gap cluster", [("V3QAGaps2", "A01b: Dead-worker ClaimLoginTime reclaim"),
                            ("V3QAGaps2", "A01c: QueueStatistic has 0 ghost-claimed rows (final state)")], "dead-worker reclaim and QA regressions"),
        ("gh-627/628", [("NestedCommentsSimulator", "test_check_sql_nested_comments")], "SSMS nested-comment splitter regression"),
    ]
    for bug, proofs, description in bug_proofs:
        rows.append(row("bug:" + bug, "fixed_bug", "regression", proofs, description))

    gate_candidates = ("CriticalTables", "DenialAndGuards", "DirectStringCSV",
                       "ExcludeTablesWildcards", "FilteredStats", "WhatIfNorecompute",
                       "EarlyEndRows", "QSCacheKey", "PartitionedIncremental", "ConcurrentParallel",
                       "V3Coverage", "V3Fixes", "VersionTokenGating", "LegacyFallback",
                       "StopReasonsCoverage", "OutputParamCoverage", "PresetStopByTime",
                       "LockTimeout", "MopUpPassE2E", "V3QAGaps", "V3QAGaps2",
                       "QueryStoreStates", "SortOrderCoverage", "QSForcedBaselineAndWaitCpu",
                       "AbortOnIntegrityError", "FreshStatQualification", "WarningsContract",
                       "HelpContent", "ParamSweep", "QSEnrichmentSafety", "V3Bugs",
                       "DiagChecksCoverage", "StatUpdateDiag", "DiagTargetChecks")
    for suite in gate_candidates:
        script = ROOT / "tests" / "diag" / f"Test-{suite}.ps1" if suite in ("DiagChecksCoverage", "StatUpdateDiag", "DiagTargetChecks") else ROOT / "tests" / f"Test-{suite}.ps1"
        source = script.read_text(encoding="utf-8-sig")
        labels = set(re.findall(r'Add-Result\s+"([^"\r\n]+)"', source, re.IGNORECASE))
        if suite == "StatUpdateDiag":
            labels.update(re.findall(r'Write-TestResult\s+-Name\s+"([^"\r\n]+)"', source, re.IGNORECASE))
        labels.update(re.findall(r'Record\s+\$ver\s+"([^"\r\n]+)"', source, re.IGNORECASE))
        if suite == "ParamSweep":
            labels.update(re.findall(r"(?m)^\s*'(@[A-Za-z][A-Za-z0-9_]*)'\s*=", source))
        for label in sorted(labels):
            # These display labels can include a SQL-version or path label at runtime.
            # A wildcard preserves the stable assertion text while covering that value.
            check_id = label.replace("[$label]", "*").replace("$path", "*")
            rows.append(row(f"check:{suite}/{check_id}", "proving_check", "behaviour",
                            [(suite, check_id)], "Existing scenario check; release-gate result integration is tracked separately."))

    regression_checks = (
        ("0859.9", "0859.9"),
        ("0859.10", "0859.10"),
        ("Plan-feedback state filter uses documented states", "Plan-feedback state filter uses documented states"),
    )
    for check_id, check_name in regression_checks:
        rows.append(row("check:V3Regression/" + check_id, "proving_check", "regression",
                        [("V3Regression", check_name)], "Named regression assertion in the release gate."))
        rows.append(row("bug:" + check_id, "fixed_bug", "regression",
                        [("V3Regression", check_name)], "Named regression proof for plan-feedback state filtering."))
    rows.append(row("guard:tempdb-low-space", "guard", "behaviour",
                    [("StopReasonsCoverage", "SC05: TEMPDB_PRESSURE")], "Set the free-space threshold above actual free space and assert no update executes."))
    for size in ("10K", "100K", "1M", "1024-pages"):
        rows.append(row("size-boundary:" + size, "size_boundary", "behaviour",
                        [("SizeBoundaries", size)], "Script-built base fixture boundary."))
    rows.append(row("size-boundary:10M-row", "size_boundary", "not testable here", [],
                    "The ten-million-row branch is explicitly outside the test fixture budget."))

    proving_suites = {
        "AutoUpdatePersistence": ("persisted_sample_rate_after_auto_update", "skip_changed_after_discovery", "log_changed_stat_skip"),
        "AwkwardNamesCaseSensitive": ("quoted_unicode_object_names", "case_sensitive_identifier_resolution"),
        "QueryStoreWorkloadSortOrder": ("query_store_workload_candidates", "query_store_sort_order_observed"),
    }
    for suite, checks in proving_suites.items():
        for check in checks:
            rows.append(row(f"check:{suite}/{check}", "proving_check", "behaviour",
                            [(suite, check)], "Release-gate scenario with a named persisted proof result."))

    # E05 summarizes E01 across three separate single-version invocations; each
    # individual E05 result is intentionally NOT_APPLICABLE.
    cross_version_mopup = "E05: Cross-version E01 mop-up fires (2019+2022+2025)"
    rows.append(row(f"check:MopUpPassE2E/{cross_version_mopup}", "proving_check", "behaviour",
                    [("MopUpPassE2E", cross_version_mopup)],
                    "Derived cross-version summary; E01 behavior is proven in each supported SQL Server run."))
    derived_suite_checks = (
        ("ParamSweep", "OutputParamCoverage",
         "ParamSweep verifies every current OUTPUT parameter is reported by its structured result."),
        ("V3Bugs", "T31: #386 CommandLog start uses public names",
         "The V3Bugs suite checks the actual CommandLog START payload."),
        ("V3Bugs", "T32: #386 MopUpPass validation text is public",
         "The V3Bugs suite checks user-facing validation text."),
        ("V3Bugs", "T33: #386 overshoot warning avoids internal parameter names",
         "The V3Bugs suite checks user-facing warning text; a FAIL remains a visible behavior finding."),
        ("V3Bugs", "T34: @Tables=ALL matches default selection",
         "The V3Bugs suite compares the positive StatsFoundOut result for @Tables=ALL and the default NULL selection."),
        ("V3Bugs", "T35: START CommandLog text is not truncated",
         "The V3Bugs suite reads a real START row produced by a command longer than 255 characters."),
    )
    for suite, check, note in derived_suite_checks:
        rows.append(row(f"check:{suite}/{check}", "proving_check", "behaviour",
                        [(suite, check)], note))

    OUTPUT.write_text(json.dumps({"schema_version": 1, "rows": rows}, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {len(rows)} coverage rows: {len(parameters)} parameters, {len(output_params)} outputs, {len(diag_checks)} Diag IDs")


if __name__ == "__main__":
    main()
