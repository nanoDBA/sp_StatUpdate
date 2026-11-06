#!/usr/bin/env python3
"""Fail when a T-SQL block comment contains another block comment.

SQL Server nests block comments, but the batch splitter in SSMS 22.11 does not: the
first */ ends the comment for it. The rest of the comment is then read as code, so a
quote in that text can hide the next GO. The script reaches the server as one batch
and fails with Msg 102 and Msg 111, even though the server parses the comment fine.

Usage: check_sql_nested_comments.py [file.sql ...]   (default: *.sql in the current directory)
"""
import glob
import sys


def nested_comments(sql):
    """Yield the offset of each /* found inside a block comment."""
    i, n = 0, len(sql)
    while i < n:
        if sql.startswith("--", i):
            end = sql.find("\n", i)
            i = n if end < 0 else end
        elif sql.startswith("/*", i):
            start, depth = i, 1
            i += 2
            while depth:
                if i >= n:
                    raise ValueError(f"unterminated block comment at offset {start}")
                if sql.startswith("/*", i):
                    yield i
                    depth, i = depth + 1, i + 2
                elif sql.startswith("*/", i):
                    depth, i = depth - 1, i + 2
                else:
                    i += 1
        elif sql[i] in "'\"[":
            i = skip_delimited(sql, i)
        else:
            i += 1


def skip_delimited(sql, start):
    """Return the offset just past a string or quoted identifier; doubled closers escape."""
    close = "]" if sql[start] == "[" else sql[start]
    i = start + 1
    while True:
        i = sql.find(close, i)
        if i < 0:
            raise ValueError(f"unterminated {sql[start]} at offset {start}")
        if not sql.startswith(close, i + 1):
            return i + 1
        i += 2


def main(paths):
    failed = False
    for path in paths:
        with open(path, encoding="utf-8-sig", newline="") as f:
            sql = f.read()
        for offset in nested_comments(sql):
            line = sql.count("\n", 0, offset) + 1
            print(f"{path}:{line}: block comment nested inside a block comment")
            failed = True
    if failed:
        print("Reword the text so it has no /* or */. SSMS 22 does not nest comments.")
        return 1
    print(f"OK: {len(paths)} file(s), no nested block comments")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:] or sorted(glob.glob("*.sql"))))
