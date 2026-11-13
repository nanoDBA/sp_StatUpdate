"""Reject private operational details, credentials, and tracker IDs from public text."""
from __future__ import annotations

import ipaddress
from pathlib import Path
import re
import subprocess
import sys

PRIVATE_RANGES = (
    (0x0A000000, 0x0AFFFFFF),
    (0xAC100000, 0xAC1FFFFF),
    (0xC0A80000, 0xC0A8FFFF),
    (0x64400000, 0x647FFFFF),
)
IPV4 = re.compile(r"(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}(?![\d.])")
ALLOWED_WORKFLOW = ".github/workflows/sql-tests.yml"
ALLOWED_SQL_PASSWORD = "TestOnly-Passw0rd!2026"
LITERAL_CREDENTIAL = re.compile(
    r"(?i)\b(MSSQL_SA_PASSWORD|SQL_PASSWORD|SQLPassword|Password|Pwd)\s*[:=]\s*['\"]([^'\"]+)['\"]"
)
PATTERNS = {
    "private IPv4": IPV4,
    "tailnet domain": re.compile(r"(?i)\b(?:[a-z0-9-]+\.)+ts\.net\b"),
    "Windows user profile path": re.compile(r"(?i)\b[A-Z]:\\Users\\[^\\/\s]+"),
    "private Unix path": re.compile(r"(?<![A-Za-z0-9])/(?:mnt|home)/[^\s\"'<>]+"),
    "email address": re.compile(r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b"),
    "GitHub or cloud token": re.compile(r"(?i)\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}|AKIA[0-9A-Z]{16}|xox[baprs]-[A-Za-z0-9-]{10,})\b"),
    "bearer token": re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]{20,}"),
    "private key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "Beads issue id": re.compile(r"(?i)\b(?:sp_StatUpdate-[a-z0-9]{4,}|o2md\.\d{1,6})\b"),
}
TEXT_SUFFIXES = {".cs", ".json", ".md", ".ps1", ".psm1", ".py", ".sh", ".sql", ".txt", ".yml", ".yaml", ".xml"}


def _is_private_ip(value: str) -> bool:
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        return False
    number = int(address)
    return value != "127.0.0.1" and any(start <= number <= end for start, end in PRIVATE_RANGES)


def _is_allowed_workflow_password(relative_path: str, line: str, match: re.Match[str]) -> bool:
    return (
        relative_path.replace("\\", "/") == ALLOWED_WORKFLOW
        and match.group(1).upper() == "MSSQL_SA_PASSWORD"
        and match.group(2) == ALLOWED_SQL_PASSWORD
    )


def _is_environment_reference(value: str) -> bool:
    value = value.strip()
    return value.startswith("$") or (value.startswith("%") and value.endswith("%"))


def scan_text(text: str, relative_path: str) -> list[tuple[int, str]]:
    findings = []
    for line_number, line in enumerate(text.splitlines(), 1):
        credential = LITERAL_CREDENTIAL.search(line)
        if credential and not _is_environment_reference(credential.group(2)) and not _is_allowed_workflow_password(relative_path, line, credential):
            findings.append((line_number, "literal credential"))
        for category, pattern in PATTERNS.items():
            if category == "private IPv4":
                matched = any(_is_private_ip(match.group(0)) for match in pattern.finditer(line))
            else:
                matched = bool(pattern.search(line))
            if matched:
                findings.append((line_number, category))
    return findings


def public_paths(root: Path) -> list[Path]:
    result = subprocess.run(
        ["git", "-C", str(root), "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        check=True, capture_output=True,
    )
    paths = []
    for raw in result.stdout.split(b"\0"):
        if not raw:
            continue
        rel = Path(raw.decode("utf-8", errors="replace"))
        if rel.parts and rel.parts[0] == "tests-host":
            continue
        if rel.suffix.lower() in TEXT_SUFFIXES:
            path = root / rel
            if path.is_file():
                paths.append(rel)
    return paths


def scan_repo(root: Path) -> list[tuple[str, int, str]]:
    findings = []
    for rel in public_paths(root):
        try:
            text = (root / rel).read_text(encoding="utf-8-sig")
        except UnicodeDecodeError:
            continue
        for line_number, category in scan_text(text, rel.as_posix()):
            findings.append((rel.as_posix(), line_number, category))
    return findings


def main() -> int:
    root = Path(sys.argv[1]).resolve() if len(sys.argv) == 2 else Path(__file__).resolve().parents[2]
    findings = scan_repo(root)
    if findings:
        for path, line_number, category in findings:
            print(f"{path}:{line_number}: {category}")
        print(f"privacy_check=FAIL findings={len(findings)}")
        return 1
    print("privacy_check=PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
