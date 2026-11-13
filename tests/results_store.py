"""Persistence seam for release-gate results; accepts any MySQL-compatible DB-API connection."""
from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Callable, Iterable, Protocol
from uuid import uuid4

from release_gate import GateDecision, Refusal, evaluate_gate, validate_run


class ResultStore(Protocol):
    def save_run(self, record: dict) -> None: ...
    def runs_for_commit(self, commit: str, tooling_sha256: str | None = None) -> list[dict]: ...


def write_run_list(path: str | Path, record: dict, *, require_ssms: bool = True) -> None:
    """Atomically write one validated run list so readers never load a partial file."""
    errors = validate_persistable_run(record, require_ssms=require_ssms)
    if errors:
        raise ValueError("invalid result list: " + "; ".join(x.detail for x in errors))
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=target.name + ".", suffix=".tmp", dir=target.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(record, stream, ensure_ascii=False, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


class DoltResultsStore:
    """Store backed by a MySQL-compatible DB-API connection (Dolt uses this protocol)."""

    def __init__(self, connection, dict_cursor=None):
        self.connection = connection
        self.dict_cursor = dict_cursor

    def _cursor(self, *, dictionary=False):
        if not dictionary:
            return self.connection.cursor()
        if self.dict_cursor is not None:
            return self.connection.cursor(self.dict_cursor)
        return self.connection.cursor(dictionary=True)

    def save_run(self, record: dict) -> None:
        fingerprint = record.get("tooling_sha256")
        if not isinstance(fingerprint, str) or not re.fullmatch(r"[0-9a-f]{64}", fingerprint):
            raise ValueError("A startup tooling fingerprint is required for new database rows")
        errors = validate_persistable_run(record)
        if errors:
            raise ValueError("invalid result list: " + "; ".join(x.detail for x in errors))
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                "INSERT INTO runs (run_id, commit_sha, tooling_sha256, orchestration_id, sql_version, sql_build, started_at, ended_at, duration_ms, server_alive_at_end) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (record["run_id"], record["commit"].lower(), fingerprint, record.get("orchestration_id"), record["sql_version"], record["sql_build"],
                 _mysql_datetime(record["started_at"]), _mysql_datetime(record["ended_at"]),
                 _run_duration_ms(record), record["server_alive_at_end"]),
            )
            for suite in record["suites"]:
                suite_run_id = str(uuid4())
                checks = suite["checks"]
                cursor.execute(
                    "INSERT INTO suite_runs (suite_run_id, run_id, suite_name, expected_count, actual_count, started_at, duration_ms) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (suite_run_id, record["run_id"], suite["suite"], suite["expected_count"], len(checks),
                     _mysql_datetime(suite["started_at"]), suite["duration_ms"]),
                )
                for check in checks:
                    cursor.execute(
                        "INSERT INTO check_results (suite_run_id, check_id, result, reason, issue_id, duration_ms) "
                        "VALUES (%s, %s, %s, %s, %s, %s)",
                        (suite_run_id, check["check"], check["result"], check.get("reason"),
                         check.get("issue"), check["duration_ms"]),
                    )
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise
        finally:
            cursor.close()

    def runs_for_commit(self, commit: str, tooling_sha256: str | None = None) -> list[dict]:
        cursor = self._cursor(dictionary=True)
        try:
            fingerprint_clause = " AND tooling_sha256 = %s" if tooling_sha256 is not None else ""
            args = (commit.lower(),) if tooling_sha256 is None else (commit.lower(), tooling_sha256)
            cursor.execute(
                "SELECT run_id, commit_sha, tooling_sha256, orchestration_id, sql_version, sql_build, started_at, ended_at, duration_ms, server_alive_at_end "
                "FROM runs WHERE commit_sha = %s" + fingerprint_clause + " ORDER BY ended_at, run_id", args
            )
            rows = cursor.fetchall()
            output = []
            for run in rows:
                cursor.execute(
                    "SELECT suite_run_id, suite_name, expected_count, actual_count, started_at, duration_ms "
                    "FROM suite_runs WHERE run_id = %s ORDER BY suite_name", (run["run_id"],)
                )
                suites = []
                for suite in cursor.fetchall():
                    cursor.execute(
                        "SELECT check_id, result, reason, issue_id, duration_ms FROM check_results "
                        "WHERE suite_run_id = %s ORDER BY check_id", (suite["suite_run_id"],)
                    )
                    checks = [
                        {"check": row["check_id"], "result": row["result"], "reason": row["reason"],
                         "issue": row["issue_id"], "duration_ms": row["duration_ms"]}
                        for row in cursor.fetchall()
                    ]
                    suites.append({"suite": suite["suite_name"], "expected_count": suite["expected_count"],
                                   "checks": checks})
                output.append({"run_id": run["run_id"], "commit": run["commit_sha"], "tooling_sha256": run["tooling_sha256"],
                               "orchestration_id": run["orchestration_id"],
                               "sql_version": str(run["sql_version"]), "sql_build": run["sql_build"],
                               "duration_ms": int(run.get("duration_ms", 0) or 0),
                               "server_alive_at_end": bool(run["server_alive_at_end"]), "suites": suites})
            return output
        finally:
            cursor.close()


def _mysql_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _run_duration_ms(record: dict) -> int:
    duration = record.get("duration_ms")
    if duration is None:
        start = datetime.fromisoformat(record["started_at"].replace("Z", "+00:00"))
        end = datetime.fromisoformat(record["ended_at"].replace("Z", "+00:00"))
        duration = int((end - start).total_seconds() * 1000)
    if isinstance(duration, bool) or not isinstance(duration, int) or duration < 0:
        raise ValueError("run duration_ms must be a nonnegative integer")
    return duration


def validate_persistable_run(record: dict, *, require_ssms: bool = True):
    errors = validate_run(record, require_ssms=require_ssms)
    if errors:
        return errors
    try:
        _run_duration_ms(record)
    except (KeyError, TypeError, ValueError):
        errors.append(Refusal("malformed", "run requires a nonnegative duration_ms or valid start/end timestamps"))
    required = ((record, "started_at", "run"), (record, "ended_at", "run"))
    for obj, key, label in required:
        if not isinstance(obj.get(key), str):
            errors.append(Refusal("malformed", f"{label} {key} must be an ISO timestamp"))
        else:
            try:
                _mysql_datetime(obj[key])
            except (TypeError, ValueError):
                errors.append(Refusal("malformed", f"{label} {key} must be an ISO timestamp"))
    for suite in record["suites"]:
        if not isinstance(suite.get("started_at"), str) or not isinstance(suite.get("duration_ms"), int) or suite["duration_ms"] < 0:
            errors.append(Refusal("malformed", f"{suite.get('suite', 'suite')} requires started_at and nonnegative duration_ms"))
        else:
            try:
                _mysql_datetime(suite["started_at"])
            except (TypeError, ValueError):
                errors.append(Refusal("malformed", f"{suite.get('suite', 'suite')} started_at must be an ISO timestamp"))
        for check in suite["checks"]:
            if not isinstance(check.get("duration_ms"), int) or isinstance(check.get("duration_ms"), bool) or check["duration_ms"] < 0:
                errors.append(Refusal("malformed", f"{suite.get('suite', 'suite')}/{check.get('check', '?')} requires nonnegative duration_ms"))
    return errors


def ingest_result_lists(store: ResultStore, paths: Iterable[str | Path]) -> int:
    count = 0
    for path in paths:
        with open(path, encoding="utf-8") as stream:
            store.save_run(json.load(stream))
        count += 1
    return count


def check_release(
    store: ResultStore, commit: str, issue_is_open: Callable[[str], bool], tooling_sha256: str | None = None
) -> GateDecision:
    if not isinstance(tooling_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", tooling_sha256):
        return GateDecision(False, (Refusal("tooling_unknown", "a known tooling fingerprint is required"),))
    return evaluate_gate(commit, store.runs_for_commit(commit, tooling_sha256), issue_is_open)


class MemoryResultsStore:
    """In-memory implementation for seam tests; does not emulate Dolt SQL behavior."""

    def __init__(self):
        self._runs: list[dict] = []

    def save_run(self, record: dict) -> None:
        errors = validate_persistable_run(record)
        if errors:
            raise ValueError("invalid result list: " + "; ".join(x.detail for x in errors))
        self._runs.append(json.loads(json.dumps(record)))

    def runs_for_commit(self, commit: str, tooling_sha256: str | None = None) -> list[dict]:
        return [json.loads(json.dumps(run)) for run in self._runs
                if run["commit"].lower() == commit.lower()
                and (tooling_sha256 is None or run.get("tooling_sha256") == tooling_sha256)]
