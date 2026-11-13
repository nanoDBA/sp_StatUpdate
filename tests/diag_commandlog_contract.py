"""Check the CommandLog contract shared by the writer, Diag reader, and fixtures."""
from __future__ import annotations

import re
import sys
from pathlib import Path

TABLE_COLUMNS = {
    "id", "databasename", "schemaname", "objectname", "objecttype", "indexname",
    "indextype", "statisticsname", "statisticname", "partitionnumber", "extendedinfo",
    "command", "commandtype", "starttime", "endtime", "errornumber", "errormessage",
}
COMMAND_TYPES = re.compile(r"\b(?:SP_STATUPDATE_[A-Z_]+|UPDATE_STATISTICS)\b", re.I)
STOP_REASON_ASSIGNMENT = re.compile(
    r"(?:@stop_reason|@stopreasonout)\s*=\s*N?'([A-Z_]+)'", re.I
)
XQUERY_ELEMENT = re.compile(r"(?:/|\()((?:Parameters|Summary|ExtendedInfo))/([A-Za-z][A-Za-z0-9_]*)", re.I)


def remove_comments(sql: str) -> str:
    """Remove SQL comments while preserving quoted strings and newlines."""
    out: list[str] = []
    i = 0
    depth = 0
    quoted = False
    while i < len(sql):
        if depth:
            if sql.startswith("/*", i):
                depth += 1
                i += 2
            elif sql.startswith("*/", i):
                depth -= 1
                i += 2
            else:
                if sql[i] == "\n":
                    out.append("\n")
                i += 1
        elif quoted:
            out.append(sql[i])
            if sql[i] == "'":
                if i + 1 < len(sql) and sql[i + 1] == "'":
                    out.append("'")
                    i += 2
                    continue
                quoted = False
            i += 1
        elif sql[i] == "'":
            quoted = True
            out.append(sql[i])
            i += 1
        elif sql.startswith("--", i):
            end = sql.find("\n", i)
            if end < 0:
                break
            out.append("\n")
            i = end + 1
        elif sql.startswith("/*", i):
            depth = 1
            i += 2
        else:
            out.append(sql[i])
            i += 1
    return "".join(out)


def insert_column_sets(sql: str) -> set[str]:
    text = remove_comments(sql)
    found: set[str] = set()
    pattern = re.compile(r"\bINSERT\s+INTO\s+(?:\[?dbo\]?\s*\.\s*)?\[?CommandLog\]?\s*\(", re.I)
    for match in pattern.finditer(text):
        start = match.end()
        depth = 1
        end = start
        while end < len(text) and depth:
            if text[end] == "(":
                depth += 1
            elif text[end] == ")":
                depth -= 1
            end += 1
        if depth:
            continue
        found.update(re.sub(r"[\[\]\s]", "", item).lower()
                     for item in text[start:end - 1].split(",") if item.strip())
    return found & TABLE_COLUMNS


def command_types(sql: str) -> set[str]:
    return {value.upper() for value in COMMAND_TYPES.findall(remove_comments(sql))}


def stop_reasons(sql: str) -> set[str]:
    text = remove_comments(sql).replace("''", "'")
    found = {value.upper() for value in STOP_REASON_ASSIGNMENT.findall(text)}
    found.update(value.upper() for value in re.findall(r"<StopReason>\s*([A-Z_]+)\s*</StopReason>", text, re.I))
    found.update(value.upper() for value in re.findall(r"-StopReason\s+['\"]([A-Z_]+)['\"]", text, re.I))
    reason_rows = re.search(r"INSERT\s+INTO\s+@reasons\b.*?\bVALUES(.*?);", text, re.I | re.S)
    if reason_rows:
        found.update(value.upper() for value in re.findall(r"N?'([A-Z_]+)'", reason_rows.group(1), re.I))
    return found


def xml_elements(sql: str) -> set[str]:
    text = remove_comments(sql).replace("''", "'")
    found = {value.lower() for value in re.findall(r"<([A-Za-z][A-Za-z0-9_]*)>", text)}
    found.update(value.lower() for value in re.findall(r"\bAS\s+\[?([A-Za-z][A-Za-z0-9_]*)\]?\s*(?:,|\n|FROM|FOR XML)", text, re.I))
    found.update(value.lower() for value in re.findall(r"FOR\s+XML\s+(?:RAW|PATH)\s*\(\s*N?'([A-Za-z][A-Za-z0-9_]*)'", text, re.I))
    return found


def diag_columns(diag: str, known: set[str]) -> set[str]:
    text = remove_comments(diag).replace("''", "'")
    aliases = set(re.findall(
        r"\b(?:FROM|JOIN)\s+(?:\[?dbo\]?\s*\.\s*)?\[?CommandLog\]?\s+(?:AS\s+)?([A-Za-z][A-Za-z0-9_]*)",
        text, re.I,
    ))
    aliases = {alias.lower() for alias in aliases if alias.lower() not in {"where", "with", "on"}}
    result: set[str] = set()
    for alias in aliases:
        result.update(column.lower() for column in re.findall(rf"\b{re.escape(alias)}\.\[?([A-Za-z][A-Za-z0-9_]*)\]?", text, re.I))
    return result & known


def contract_gaps(writer: str, diag: str, fixture: str) -> list[str]:
    gaps: list[str] = []
    writer_columns = insert_column_sets(writer)
    fixture_columns = insert_column_sets(fixture)
    read_columns = diag_columns(diag, TABLE_COLUMNS)
    for column in sorted(read_columns - writer_columns):
        gaps.append(f"Diag reads CommandLog column '{column}' that the writer does not insert")
    for column in sorted(read_columns - fixture_columns):
        gaps.append(f"Diag reads CommandLog column '{column}' absent from synthetic inserts")

    writer_types = command_types(writer)
    fixture_types = command_types(fixture)
    diag_text = remove_comments(diag).replace("''", "'")
    read_types = {m.group(1).upper() for m in re.finditer(
        r"\bCommandType\s*=\s*N?'(SP_STATUPDATE_[A-Z_]+|UPDATE_STATISTICS)'", diag_text, re.I
    )}
    for value in sorted(read_types - writer_types):
        gaps.append(f"Diag reads CommandType '{value}' that the writer does not emit")
    for value in sorted(read_types - fixture_types):
        gaps.append(f"Diag reads CommandType '{value}' absent from synthetic history")

    writer_elements = xml_elements(writer)
    fixture_elements = xml_elements(fixture)
    diag_elements = {child.lower() for _, child in XQUERY_ELEMENT.findall(remove_comments(diag).replace("''", "'"))}
    for element in sorted(diag_elements - writer_elements):
        gaps.append(f"Diag reads ExtendedInfo element '{element}' that the writer does not emit")
    for element in sorted(diag_elements - fixture_elements):
        gaps.append(f"Diag reads ExtendedInfo element '{element}' absent from synthetic history")

    writer_reasons = stop_reasons(writer)
    fixture_reasons = stop_reasons(fixture)
    diag_literals = {value.upper() for value in re.findall(r"N?'([A-Z_]+)'", diag_text)}
    read_reasons = writer_reasons & diag_literals
    for value in sorted(read_reasons - writer_reasons):
        gaps.append(f"Diag expects StopReason '{value}' that the writer does not emit")
    for value in sorted(read_reasons - fixture_reasons):
        gaps.append(f"Diag-visible StopReason '{value}' absent from synthetic history")
    return gaps


def check_sources(writer: str, diag: str, fixture: str) -> list[str]:
    return contract_gaps(writer, diag, fixture)


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    writer = (root / "sp_StatUpdate.sql").read_text(encoding="utf-8-sig")
    diag = (root / "sp_StatUpdate_Diag.sql").read_text(encoding="utf-8-sig")
    fixture = "\n".join((root / path).read_text(encoding="utf-8-sig") for path in (
        "tests/diag/Setup-DiagTestData.sql",
        "tests/diag/Test-DiagChecksCoverage.ps1",
        "tests/diag/CommandLogContractFixture.sql",
    ))
    gaps = check_sources(writer, diag, fixture)
    if gaps:
        print("CommandLog contract failed:")
        for gap in gaps:
            print(f"- {gap}")
        return 1
    print("CommandLog writer, Diag reader, and synthetic history contracts agree.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
