"""Coverage inventory checks for the release gate."""
from __future__ import annotations

import json
import fnmatch
import re
from pathlib import Path
from typing import Iterable

TIERS = {"behaviour", "regression", "not testable here"}


def source_inventory(proc_sql: str, diag_sql: str) -> tuple[set[str], set[str]]:
    """Return public procedure parameters and documented Diag check IDs."""
    proc = re.search(
        r"\bALTER\s+PROCEDURE\s+(?:\[?dbo\]?\.)?\[?sp_StatUpdate\]?\b(.*?)\nAS\s*\n",
        proc_sql,
        re.IGNORECASE | re.DOTALL,
    )
    if not proc:
        raise ValueError("could not find sp_StatUpdate signature")
    signature = re.sub(r"/\*.*?\*/|--[^\r\n]*", "", proc.group(1), flags=re.DOTALL)
    parameters = {"@" + name.lower() for name in re.findall(r"@([A-Za-z][A-Za-z0-9_]*)\s+[A-Za-z][A-Za-z0-9_]*(?:\s*\([^)]*\))?", signature)}
    checks = set()
    for family, maximum in (("W", 16), ("C", 6), ("I", 19)):
        checks.update(
            f"{family}{int(number)}"
            for number in re.findall(rf"\b{family}(\d{{1,2}})\b", diag_sql, re.IGNORECASE)
            if 1 <= int(number) <= maximum
        )
    return parameters, checks


def _check_pattern_matches(value: str, pattern: str) -> bool:
    """Match '*' and '?' as wildcards; treat other check-ID characters literally.

    Check IDs commonly contain bracketed SQL text such as '[NO DATA]'.
    fnmatch interprets brackets as character classes, so its default behavior
    rejects an exact proof for those IDs.
    """
    regex = re.escape(pattern).replace(r"\*", ".*").replace(r"\?", ".")
    return re.fullmatch(regex, value, flags=re.IGNORECASE) is not None


def coverage_gaps(coverage: dict, proc_sql: str, diag_sql: str, runs: Iterable[dict]) -> list[str]:
    rows = coverage.get("rows")
    if not isinstance(rows, list):
        return ["coverage list has no rows array"]
    gaps: list[str] = []
    row_ids: set[str] = set()
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("id"), str):
            gaps.append("coverage row is missing an id")
            continue
        row_id = row["id"]
        row_ids.add(row_id.lower())
        if row.get("tier") not in TIERS:
            gaps.append(f"{row_id}: missing or invalid tier")
        proofs = row.get("proving_checks")
        if not isinstance(proofs, list):
            gaps.append(f"{row_id}: proving_checks must be a list")
        elif not proofs and row.get("tier") in {"behaviour", "regression"}:
            gaps.append(f"{row_id}: no proving check is named")

    try:
        parameters, diag_ids = source_inventory(proc_sql, diag_sql)
    except ValueError as error:
        return gaps + [str(error)]
    for parameter in sorted(parameters - row_ids):
        gaps.append(f"public parameter {parameter} has no coverage row")
    for check_id in sorted(diag_ids):
        if ("diag:" + check_id.lower()) not in row_ids:
            gaps.append(f"Diag check {check_id} has no coverage row")

    observed: set[tuple[str, str]] = set()
    for run in runs:
        for suite in run.get("suites", []):
            suite_name = str(suite.get("suite", "")).split(" (iteration ", 1)[0]
            for check in suite.get("checks", []):
                # A skipped or unavailable check did not execute, so it cannot
                # serve as evidence that the behavior was covered.
                if str(check.get("result", "")).upper() not in {"PASS", "FAIL"}:
                    continue
                observed.add((suite_name.casefold(), str(check.get("check", "")).casefold()))
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("proving_checks"), list):
            continue
        for proof in row["proving_checks"]:
            if not isinstance(proof, dict) or not proof.get("suite") or not proof.get("check"):
                gaps.append(f"{row.get('id', '?')}: malformed proving check")
                continue
            suite_pattern = str(proof["suite"]).casefold()
            check_pattern = str(proof["check"]).casefold()
            found = any(
                fnmatch.fnmatchcase(suite, suite_pattern) and _check_pattern_matches(check, check_pattern)
                for suite, check in observed
            )
            if not found:
                gaps.append(f"{row['id']}: proving check {proof['suite']}/{proof['check']} absent or not executed")
    return gaps


def enforcement_refusals(evaluate) -> list[str]:
    """Fail closed when coverage has gaps or cannot be evaluated."""
    try:
        gaps = evaluate()
    except Exception as error:
        return [f"coverage evaluation unavailable ({type(error).__name__})"]
    return [f"coverage gap: {gap}" for gap in gaps]


def report(coverage: dict, proc_sql: str, diag_sql: str, runs: Iterable[dict]) -> list[str]:
    gaps = coverage_gaps(coverage, proc_sql, diag_sql, runs)
    print(f"coverage_report rows={len(coverage.get('rows', []))} gaps={len(gaps)} mode=report-only")
    for gap in gaps:
        print("coverage_gap=" + gap)
    return gaps


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--coverage", type=Path, required=True)
    parser.add_argument("--procedure", type=Path, required=True)
    parser.add_argument("--diag", type=Path, required=True)
    parser.add_argument("--runs", type=Path, required=True, help="JSON list of results read back from the results database")
    args = parser.parse_args()
    coverage = json.loads(args.coverage.read_text(encoding="utf-8"))
    runs = json.loads(args.runs.read_text(encoding="utf-8"))
    gaps = report(coverage, args.procedure.read_text(encoding="utf-8"), args.diag.read_text(encoding="utf-8"), runs)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
