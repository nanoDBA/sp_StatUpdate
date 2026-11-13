"""Build a run record, save it locally, and optionally persist it to a configured results database."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
from urllib.parse import unquote, urlsplit
from uuid import uuid4

from release_gate import EXPECTED_RELEASE_SUITES, evaluate_version, validate_run
from results_store import DoltResultsStore, write_run_list
from coverage_report import report as report_coverage


def read_json(path: Path):
    with path.open(encoding="utf-8") as stream:
        return json.load(stream)


def database_connection():
    database_url = os.environ.get("SPSTATUPDATE_RESULTS_DB_URL", "").strip()
    if not database_url:
        raise ValueError("SPSTATUPDATE_RESULTS_DB_URL is required when database saving is requested")
    try:
        import pymysql
    except ImportError as error:
        raise RuntimeError("install the optional PyMySQL dependency to save results to a database") from error
    parsed = urlsplit(database_url)
    if parsed.scheme not in {"mysql", "mariadb"} or not parsed.hostname or not parsed.username:
        raise ValueError("results database URL must use mysql://user:password@host/database")
    database = unquote(parsed.path.lstrip("/"))
    if not database:
        raise ValueError("results database URL must include a database name")
    connection = pymysql.connect(
        host=parsed.hostname,
        port=parsed.port or 3306,
        user=unquote(parsed.username),
        password=unquote(parsed.password or ""),
        database=database,
        connect_timeout=8,
        autocommit=False,
    )
    return connection, pymysql


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True, choices=("2019", "2022", "2025"))
    parser.add_argument("--commit", required=True)
    parser.add_argument("--run-id", default="")
    parser.add_argument("--tooling-sha256", required=True)
    parser.add_argument("--orchestration-id")
    parser.add_argument("--pass-count", type=int, default=1)
    parser.add_argument("--test-root", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--started-at", required=True)
    parser.add_argument("--ended-at", required=True)
    parser.add_argument("--ssms-result", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--save-to-database", action="store_true")
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument("--result", nargs="+", type=Path)
    inputs.add_argument("--result-manifest", type=Path)
    args = parser.parse_args()
    if not 1 <= args.pass_count <= 3:
        raise ValueError("pass count must be between 1 and 3")
    if not re.fullmatch(r"[0-9a-f]{64}", args.tooling_sha256):
        raise ValueError("tooling fingerprint must be a 64-character SHA-256")
    if args.result_manifest:
        paths = read_json(args.result_manifest)
        if not isinstance(paths, list) or not paths or not all(isinstance(path, str) for path in paths):
            raise ValueError("result manifest must contain a nonempty array of file paths")
        args.result = [Path(path) for path in paths]

    suites = []
    sql_build = None
    alive = True
    occurrences: dict[str, int] = {}
    order_by_pass: dict[int, list[str]] = {iteration: [] for iteration in range(1, args.pass_count + 1)}
    for path in args.result:
        record = read_json(path)
        if record.get("commit", "").lower() != args.commit.lower():
            raise ValueError("result commit does not match this run")
        if str(record.get("sql_version")) != args.version:
            raise ValueError("result SQL version does not match this run")
        if not record.get("server_alive_at_end"):
            alive = False
        if sql_build is None:
            sql_build = record.get("sql_build")
        elif sql_build != record.get("sql_build"):
            raise ValueError("suite SQL builds differ")
        if len(record.get("suites", [])) != 1:
            raise ValueError("each result file must contain exactly one suite")
        suite = record["suites"][0]
        name = suite["suite"]
        if name not in EXPECTED_RELEASE_SUITES:
            raise ValueError("unexpected suite in the release gate")
        occurrences[name] = occurrences.get(name, 0) + 1
        iteration = occurrences[name]
        if iteration not in order_by_pass:
            raise ValueError(f"each suite must run exactly {args.pass_count} times")
        order_by_pass[iteration].append(name)
        suite = dict(suite)
        suite["suite"] = f"{name} (iteration {iteration})"
        suites.append(suite)

    if occurrences != {name: args.pass_count for name in EXPECTED_RELEASE_SUITES}:
        missing = sorted(EXPECTED_RELEASE_SUITES - set(occurrences))
        extra = sorted(set(occurrences) - EXPECTED_RELEASE_SUITES)
        raise ValueError(f"declared suites must run exactly {args.pass_count} times; missing={missing}, extra={extra}")
    if any(set(order_by_pass[index]) != EXPECTED_RELEASE_SUITES for index in range(1, args.pass_count + 1)):
        raise ValueError("every pass must contain every declared suite")
    if args.pass_count >= 2 and order_by_pass[1] != order_by_pass[2]:
        raise ValueError("the first two suite passes must use the same order")
    if args.pass_count >= 3 and order_by_pass[3] == order_by_pass[1]:
        raise ValueError("the third suite pass must use a different shuffled order")
    if not sql_build:
        raise ValueError("SQL build is required")

    if args.ssms_result:
        ssms = read_json(args.ssms_result)
        expected_ssms = {
            "sp_StatUpdate.sql", "sp_StatUpdate_Diag.sql", "sp_StatUpdate_XE_Session.sql",
            "sp_StatUpdate_Diag.v3.11.2-pre-fix.sql",
        }
        if ssms.get("suite") != "SSMS" or {row["check"] for row in ssms.get("checks", [])} != expected_ssms:
            raise ValueError("SSMS report must contain every shipped file and the parser control")
        suites.append(ssms)

    started = datetime.fromisoformat(args.started_at.replace("Z", "+00:00"))
    ended = datetime.fromisoformat(args.ended_at.replace("Z", "+00:00"))
    duration_ms = int((ended - started).total_seconds() * 1000)
    if duration_ms < 0:
        raise ValueError("run end precedes run start")
    run = {
        "run_id": args.run_id or str(uuid4()),
        "commit": args.commit.lower(),
        "tooling_sha256": args.tooling_sha256,
        "orchestration_id": args.orchestration_id,
        "sql_version": args.version,
        "sql_build": sql_build,
        "started_at": started.astimezone(timezone.utc).isoformat(),
        "ended_at": ended.astimezone(timezone.utc).isoformat(),
        "duration_ms": duration_ms,
        "server_alive_at_end": alive,
        "suites": suites,
    }

    coverage_started = datetime.now(timezone.utc)
    try:
        repo_root = args.test_root.resolve().parent
        gaps = report_coverage(
            read_json(args.test_root / "coverage-list.json"),
            (repo_root / "sp_StatUpdate.sql").read_text(encoding="utf-8-sig"),
            (repo_root / "sp_StatUpdate_Diag.sql").read_text(encoding="utf-8-sig"),
            [run],
        )
        coverage_reason = f"report-only: {len(gaps)} coverage gap(s)"
        coverage_reason += "; first=" + gaps[0] if gaps else "; no gaps"
    except Exception as error:
        coverage_reason = f"report-only: evaluation unavailable ({type(error).__name__})"
    coverage_ended = datetime.now(timezone.utc)
    coverage_ms = int((coverage_ended - coverage_started).total_seconds() * 1000)
    run["duration_ms"] = int((coverage_ended - started.astimezone(timezone.utc)).total_seconds() * 1000)
    run["ended_at"] = coverage_ended.isoformat()
    run["suites"].append({
        "suite": "Coverage", "expected_count": 1, "started_at": coverage_started.isoformat(),
        "duration_ms": coverage_ms,
        "checks": [{"check": "coverage_inventory", "result": "PASS", "reason": coverage_reason, "duration_ms": coverage_ms}],
    })

    errors = validate_run(run, require_ssms=args.save_to_database)
    if errors:
        raise ValueError("result list validation refused: " + "; ".join(item.detail for item in errors))
    write_run_list(args.output, run, require_ssms=args.save_to_database)

    check_count = sum(len(suite["checks"]) for suite in run["suites"])
    decision = evaluate_version(args.commit, args.version, run)
    print(f"results_file={args.output.name}")
    print(f"run_id={run['run_id']} commit={run['commit']} tooling_sha256={run['tooling_sha256']}")
    print(f"sql_version={args.version} suites={len(run['suites'])} checks={check_count}")
    print("verdict=" + ("PASS" if decision.would_tag else "FAIL") + " source=local_results")
    for refusal in decision.refusals:
        print(f"refusal={refusal.code}: {refusal.detail}")
    if not decision.would_tag:
        return 1

    if args.save_to_database:
        connection, pymysql = database_connection()
        try:
            store = DoltResultsStore(connection, dict_cursor=pymysql.cursors.DictCursor)
            store.save_run(run)
            saved = next((item for item in store.runs_for_commit(args.commit, args.tooling_sha256)
                          if item["run_id"] == run["run_id"]), None)
            if saved is None:
                raise RuntimeError("database read-back returned no run")
            if saved["commit"].lower() != args.commit.lower() or saved["tooling_sha256"] != args.tooling_sha256:
                raise RuntimeError("database read-back did not preserve source identity")
            if saved["orchestration_id"] != args.orchestration_id:
                raise RuntimeError("database read-back did not preserve orchestration identity")
            if not evaluate_version(args.commit, args.version, saved).would_tag:
                raise RuntimeError("database read-back verdict differs from local results")
            print(f"database_readback=passed run_id={saved['run_id']} checks={sum(len(s['checks']) for s in saved['suites'])}")
        finally:
            connection.close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print("result_save=failed error_type=" + type(error).__name__)
        raise SystemExit(2)
