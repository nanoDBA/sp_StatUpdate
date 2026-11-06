#!/usr/bin/env python3
"""Fail when a T-SQL block comment is large enough to break batch splitting in SSMS.

SSMS 22.11 stops recognising GO after a block comment larger than 64 KiB, so the
script reaches the server as one batch and fails with Msg 102 and Msg 111. The limit
here is half that, to leave room. Sizes are measured with CRLF line endings because
that is how the files look when opened on Windows, whatever the repository stores.

Usage: check_sql_comment_size.py [file.sql ...]   (default: *.sql in the current directory)
"""
import glob
import sys

LIMIT = 32_000


def block_comments(sql):
    """Yield (start_offset, end_offset) for each outermost block comment."""
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
                    depth, i = depth + 1, i + 2
                elif sql.startswith("*/", i):
                    depth, i = depth - 1, i + 2
                else:
                    i += 1
            yield start, i
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


def violations(sql):
    """Yield (line_number, size) for each block comment over the limit."""
    crlf = sql.replace("\r\n", "\n").replace("\n", "\r\n")
    for start, end in block_comments(crlf):
        if end - start > LIMIT:
            yield crlf.count("\n", 0, start) + 1, end - start


def main(paths):
    failed = False
    for path in paths:
        with open(path, encoding="utf-8-sig", newline="") as f:
            for line, size in violations(f.read()):
                print(f"{path}:{line}: block comment is {size:,} chars (limit {LIMIT:,})")
                failed = True
    if failed:
        print("Split the comment into smaller /* */ blocks, keeping the text in the file.")
        return 1
    print(f"OK: {len(paths)} file(s), no block comment over {LIMIT:,} chars")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:] or sorted(glob.glob("*.sql"))))
