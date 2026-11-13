"""Pure validation and release-gate decision logic; no database or SQL Server dependency."""
from __future__ import annotations
from collections import defaultdict
from dataclasses import dataclass, asdict
import json
import re
from pathlib import Path
from typing import Callable, Iterable

RESULTS = {"PASS", "FAIL", "NOT_APPLICABLE", "SKIPPED"}
VERSIONS = {"2019", "2022", "2025"}
EXPECTED_RELEASE_SUITES = frozenset({
    "Compile", "QSCacheKey", "EarlyEndRows", "AutoUpdatePersistence",
    "AwkwardNamesCaseSensitive", "QueryStoreWorkloadSortOrder", "HelpContent",
    "ParamSweep", "QSEnrichmentSafety", "V3Bugs", "CriticalTables",
    "DenialAndGuards", "DirectStringCSV", "ExcludeTablesWildcards",
    "FilteredStats", "WhatIfNorecompute", "StopReasonsCoverage",
    "OutputParamCoverage", "PresetStopByTime", "LockTimeout", "MopUpPassE2E",
    "V3QAGaps", "V3QAGaps2", "QueryStoreStates", "SortOrderCoverage",
    "QSForcedBaselineAndWaitCpu", "AbortOnIntegrityError",
    "FreshStatQualification", "WarningsContract", "StatUpdateDiag",
    "DiagChecksCoverage", "DiagTargetChecks", "ParameterBehavior",
    "V3Regression", "SizeBoundaries",
})

@dataclass(frozen=True)
class Refusal:
    code: str
    detail: str

@dataclass(frozen=True)
class GateDecision:
    would_tag: bool
    refusals: tuple[Refusal, ...]
    def to_dict(self):
        return {"would_tag": self.would_tag, "refusals": [asdict(x) for x in self.refusals]}

def load_run_lists(paths: Iterable[str | Path]) -> list[dict]:
    records = []
    for path in paths:
        with open(path, encoding="utf-8") as f:
            records.append(json.load(f))
    return records

def validate_run(record: dict, *, require_ssms: bool = True) -> list[Refusal]:
    out: list[Refusal] = []
    for key in ("run_id", "commit", "sql_version", "sql_build", "server_alive_at_end", "suites"):
        if key not in record:
            out.append(Refusal("malformed", f"missing {key}"))
    if out:
        return out
    if not isinstance(record["commit"], str) or not re.fullmatch(r"[0-9a-fA-F]{40}", record["commit"]):
        out.append(Refusal("malformed", "commit must be a full 40-character hash"))
    version = str(record["sql_version"])
    if version not in VERSIONS:
        out.append(Refusal("malformed", "unsupported SQL version"))
    if not isinstance(record["sql_build"], str) or not record["sql_build"].strip():
        out.append(Refusal("malformed", "SQL build is required"))
    if not isinstance(record["server_alive_at_end"], bool):
        out.append(Refusal("malformed", "server_alive_at_end must be boolean"))
    suites = record["suites"]
    if not isinstance(suites, list) or not suites:
        out.append(Refusal("malformed", "suites must be a nonempty list")); return out
    suite_names = set()
    observed_release_suites = set()
    for suite in suites:
        if not isinstance(suite, dict):
            out.append(Refusal("malformed", "suite must be an object")); continue
        for key in ("suite", "expected_count", "checks"):
            if key not in suite:
                out.append(Refusal("malformed", f"suite missing {key}"))
        if not all(key in suite for key in ("suite", "expected_count", "checks")):
            continue
        name = suite["suite"]
        if not isinstance(name, str) or not name.strip():
            out.append(Refusal("malformed", "suite name is required"))
        elif name in suite_names:
            out.append(Refusal("duplicate_suite", f"duplicate suite {name}"))
        else:
            suite_names.add(name)
        base_name = re.sub(r" \(iteration [1-9][0-9]*\)$", "", name) if isinstance(name, str) else ""
        if base_name in EXPECTED_RELEASE_SUITES:
            observed_release_suites.add(base_name)
        expected = suite["expected_count"]
        checks = suite["checks"]
        if not isinstance(expected, int) or isinstance(expected, bool) or expected < 0:
            out.append(Refusal("malformed", f"{name} expected_count must be a nonnegative integer"))
        if not isinstance(checks, list):
            out.append(Refusal("malformed", f"{name} checks must be a list")); continue
        if isinstance(expected, int) and len(checks) != expected:
            out.append(Refusal("short_count", f"{name} expected {expected}, got {len(checks)}"))
        seen = set()
        for row in checks:
            if not isinstance(row, dict):
                out.append(Refusal("malformed", f"{name} check row must be an object")); continue
            cid, result = row.get("check"), row.get("result")
            if not isinstance(cid, str) or not cid:
                out.append(Refusal("malformed", f"{name} check id is required"))
            elif cid in seen:
                out.append(Refusal("duplicate_check", f"{name}/{cid} is duplicated"))
            else:
                seen.add(cid)
            if result not in RESULTS:
                out.append(Refusal("malformed", f"invalid result for {name}/{cid or 'unnamed check'}")); continue
            if result != "PASS" and not str(row.get("reason", "")).strip():
                out.append(Refusal("malformed", f"{name}/{cid} needs a reason"))
            if result == "SKIPPED" and not str(row.get("issue", "")).strip():
                out.append(Refusal("skip_without_issue", f"{name}/{cid} has no issue id"))
    missing_suites = EXPECTED_RELEASE_SUITES - observed_release_suites
    if missing_suites:
        out.append(Refusal("missing_suite", "missing declared suite(s): " + ", ".join(sorted(missing_suites))))
    ssms = [suite for suite in suites if isinstance(suite, dict) and suite.get("suite") == "SSMS"]
    expected_ssms = {
        "sp_StatUpdate.sql", "sp_StatUpdate_Diag.sql", "sp_StatUpdate_XE_Session.sql",
        "sp_StatUpdate_Diag.v3.11.2-pre-fix.sql",
    }
    if require_ssms and len(ssms) != 1:
        out.append(Refusal("missing_ssms", "exactly one blocking SSMS suite is required"))
    elif ssms and {row.get("check") for row in ssms[0].get("checks", []) if isinstance(row, dict)} != expected_ssms:
        out.append(Refusal("ssms_checks", "SSMS suite must contain all shipped files and the pre-fix control"))
    elif ssms:
        for row in ssms[0].get("checks", []):
            if row.get("result") != "PASS":
                out.append(Refusal("ssms_check", f"SSMS/{row.get('check')}: {row.get('reason', row.get('result'))}"))
            if row.get("check") == "sp_StatUpdate_Diag.v3.11.2-pre-fix.sql" and not re.search(
                r"expected pre-fix parser rejection: Msg (102|111)", str(row.get("reason", ""))
            ):
                out.append(Refusal("ssms_control", "pre-fix Diag control did not record expected Msg 102/111"))
    return out

def evaluate_version(expected_commit: str, expected_version: str, record: dict) -> GateDecision:
    """Judge one portable SQL run; SSMS and issue lookups remain maintainer-only."""
    refusals = validate_run(record, require_ssms=False)
    if record.get("commit", "").lower() != expected_commit.lower():
        refusals.append(Refusal("wrong_commit", "run belongs to another source commit"))
    if str(record.get("sql_version", "")) != expected_version:
        refusals.append(Refusal("wrong_version", "run belongs to another SQL Server version"))
    if record.get("server_alive_at_end") is not True:
        refusals.append(Refusal("server_down_at_end", "SQL Server was not alive at run end"))
    for suite in record.get("suites", []) if isinstance(record.get("suites"), list) else []:
        if not isinstance(suite, dict):
            continue
        for check in suite.get("checks", []) if isinstance(suite.get("checks"), list) else []:
            if not isinstance(check, dict):
                continue
            if check.get("result") == "FAIL":
                refusals.append(Refusal("fail", f"{suite.get('suite','suite')}/{check.get('check','?')}: {check.get('reason','')}"))
            elif check.get("result") == "SKIPPED" and not str(check.get("issue", "")).strip():
                refusals.append(Refusal("skip_without_issue", f"{suite.get('suite','suite')}/{check.get('check','?')} has no skip reason"))
    return GateDecision(not refusals, tuple(refusals))

def evaluate_gate(expected_commit: str, run_records: Iterable[dict], issue_is_open: Callable[[str], bool]) -> GateDecision:
    refusals: list[Refusal] = []
    if not re.fullmatch(r"[0-9a-fA-F]{40}", expected_commit or ""):
        return GateDecision(False, (Refusal("invalid_commit", "release commit must be a full 40-character hash"),))
    by_version = defaultdict(list)
    for record in run_records:
        refusals.extend(validate_run(record))
        if not isinstance(record, dict):
            continue
        version = str(record.get("sql_version", ""))
        by_version[version].append(record)
        if record.get("commit", "").lower() != expected_commit.lower():
            refusals.append(Refusal("wrong_commit", f"SQL {version} run belongs to another commit"))
        if record.get("server_alive_at_end") is not True:
            refusals.append(Refusal("server_down_at_end", f"SQL {version} server was not alive at end"))
        for suite in record.get("suites", []) if isinstance(record.get("suites"), list) else []:
            if not isinstance(suite, dict):
                continue
            for check in suite.get("checks", []) if isinstance(suite.get("checks"), list) else []:
                if not isinstance(check, dict):
                    continue
                result = check.get("result")
                label = f"{version}/{suite.get('suite','suite')}/{check.get('check','?')}"
                if result == "FAIL":
                    refusals.append(Refusal("fail", f"{label}: {check.get('reason','')}"))
                elif result == "SKIPPED":
                    issue = str(check.get("issue", "")).strip()
                    if issue and not issue_is_open(issue):
                        refusals.append(Refusal("skip_issue_not_open", f"{label} issue is missing or closed"))
    for version in sorted(VERSIONS - set(by_version)):
        refusals.append(Refusal("missing_version", f"no results for SQL {version}"))
    return GateDecision(not refusals, tuple(refusals))
