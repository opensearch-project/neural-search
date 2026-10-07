#!/usr/bin/env python3
"""Clean up the Unreleased section of a Keep-a-Changelog style CHANGELOG.

When a release is cut, the entries listed under the top "## [Unreleased ...]"
section have shipped. This script empties those entries while preserving:
  - the "## [Unreleased ...]" heading itself
  - every "### <Category>" sub-header within that section (Features,
    Enhancements, Bug Fixes, Infrastructure, Documentation, Maintenance,
    Refactoring, ...)

Everything after the Unreleased section (previous releases, footer, etc.) is
left untouched.

Usage:
    cleanup_changelog.py CHANGELOG.md
"""

import re
import sys


UNRELEASED_HEADING = re.compile(r"^## \[Unreleased", re.IGNORECASE)
SECTION_HEADING = re.compile(r"^## ")
CATEGORY_HEADING = re.compile(r"^### ")


def cleanup(text: str) -> str:
    lines = text.splitlines(keepends=True)

    # Locate the Unreleased section.
    start = None
    for i, line in enumerate(lines):
        if UNRELEASED_HEADING.match(line):
            start = i
            break

    if start is None:
        # Nothing to do; no Unreleased section.
        return text

    # Find where the Unreleased section ends: the next top-level "## " heading.
    end = len(lines)
    for i in range(start + 1, len(lines)):
        if SECTION_HEADING.match(lines[i]):
            end = i
            break

    # Rebuild the Unreleased section: keep the Unreleased heading and any
    # "### <Category>" headers, drop all other content (bullets, blank noise).
    rebuilt = [lines[start].rstrip("\n") + "\n"]  # Unreleased heading

    for line in lines[start + 1 : end]:
        if CATEGORY_HEADING.match(line):
            rebuilt.append("\n")
            rebuilt.append(line.rstrip("\n") + "\n")

    # Ensure exactly one trailing blank line before the next section so the
    # output stays tidy.
    rebuilt.append("\n")

    new_lines = lines[:start] + rebuilt + lines[end:]
    return "".join(new_lines)


def main(argv):
    if len(argv) != 2:
        sys.stderr.write("Usage: cleanup_changelog.py <CHANGELOG.md>\n")
        return 2

    path = argv[1]
    with open(path, "r", encoding="utf-8") as f:
        original = f.read()

    updated = cleanup(original)

    if updated != original:
        with open(path, "w", encoding="utf-8") as f:
            f.write(updated)
        print(f"Cleaned up Unreleased section in {path}")
    else:
        print(f"No changes made to {path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
