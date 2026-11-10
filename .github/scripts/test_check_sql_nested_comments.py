#!/usr/bin/env python3
"""Exercise the nested-comment check through its command-line interface."""
from pathlib import Path
import subprocess
import sys


SCRIPT_DIR = Path(__file__).resolve().parent
CHECKER = SCRIPT_DIR / "check_sql_nested_comments.py"
FIXTURES = SCRIPT_DIR / "fixtures"


def run_checker(name):
    return subprocess.run(
        [sys.executable, str(CHECKER), str(FIXTURES / name)],
        check=False,
        capture_output=True,
        text=True,
    )


def main():
    passing = run_checker("pass_comments.sql")
    assert passing.returncode == 0, passing.stdout + passing.stderr
    assert "OK: 1 file(s), no nested block comments" in passing.stdout

    nested = run_checker("nested_comment.sql")
    assert nested.returncode == 1, nested.stdout + nested.stderr
    assert ":2: block comment nested inside a block comment" in nested.stdout

    pre_fix = run_checker("v3_11_2_diag_nested_comment.sql")
    assert pre_fix.returncode == 1, pre_fix.stdout + pre_fix.stderr
    findings = [line for line in pre_fix.stdout.splitlines() if "block comment nested" in line]
    assert len(findings) == 2, pre_fix.stdout + pre_fix.stderr
    assert ":3: block comment nested inside a block comment" in findings[0]
    assert ":4: block comment nested inside a block comment" in findings[1]
    assert "Reword the text" in pre_fix.stdout
    print("PASS: CLI accepts valid SQL and rejects nested comments, including the v3.11.2 Diag case.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
