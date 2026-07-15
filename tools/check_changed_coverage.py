#!/usr/bin/env python3
"""Require line coverage on executable Rust lines changed from a Git base."""

import re
import subprocess
import sys
import xml.etree.ElementTree as ET


HUNK = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@")


def git_diff(*args: str) -> str:
    return subprocess.run(
        ["git", "diff", "--unified=0", "--no-ext-diff", *args],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    ).stdout


def untracked_rust_files() -> list[str]:
    output = subprocess.run(
        ["git", "ls-files", "--others", "--exclude-standard", "--", "*.rs"],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    ).stdout
    return output.splitlines()


def changed_lines(diff: str) -> dict[str, set[int]]:
    changed: dict[str, set[int]] = {}
    path = None
    for line in diff.splitlines():
        if line.startswith("+++ "):
            target = line[4:]
            path = target[2:] if target.startswith("b/") else None
        elif path is not None and (match := HUNK.match(line)):
            start = int(match.group(1))
            count = int(match.group(2) or "1")
            changed.setdefault(path, set()).update(range(start, start + count))
    return changed


def merge(into: dict[str, set[int]], other: dict[str, set[int]]) -> None:
    for path, lines in other.items():
        into.setdefault(path, set()).update(lines)


def main() -> int:
    if len(sys.argv) != 4:
        print(f"usage: {sys.argv[0]} COVERAGE_XML BASE MINIMUM_PERCENT", file=sys.stderr)
        return 2
    coverage_path, base, minimum_text = sys.argv[1:]
    minimum = float(minimum_text)
    changed = changed_lines(git_diff(f"{base}...HEAD"))
    merge(changed, changed_lines(git_diff("HEAD")))
    for path in untracked_rust_files():
        with open(path, "rb") as source:
            changed[path] = set(range(1, sum(1 for _ in source) + 1))

    executable = 0
    covered = 0
    for source in ET.parse(coverage_path).iterfind(".//class"):
        path = source.get("filename")
        if path not in changed:
            continue
        for line in source.iterfind("./lines/line"):
            if int(line.get("number", "0")) in changed[path]:
                executable += 1
                covered += int(line.get("hits", "0")) > 0

    percent = 100.0 if executable == 0 else 100.0 * covered / executable
    print(
        f"Changed executable lines: {covered}/{executable} covered "
        f"({percent:.2f}%, required {minimum:.2f}%)"
    )
    return 0 if percent + 1e-9 >= minimum else 1


if __name__ == "__main__":
    raise SystemExit(main())
