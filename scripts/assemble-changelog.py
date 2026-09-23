#!/usr/bin/env python3
"""Assemble the unreleased changelog fragments into a release block.

Every pull request records its changelog entry as a fragment file under
``changelog/unreleased/<section>/<topic>.md`` instead of editing
``CHANGELOG.md``, so parallel pull requests stop colliding on the same
lines. At release time this script concatenates the fragments, in the fixed
section order of the changelog, into a ``## [X.Y.Z] - YYYY-MM-DD`` block
inserted right below ``## [Unreleased]``, resets ``## [Unreleased]`` to an
empty section and deletes the fragments it consumed. Entries that still sit
under ``## [Unreleased]`` in ``CHANGELOG.md`` are carried into the release
block ahead of the fragments of the same section.

Usage:
    assemble-changelog.py --check
    assemble-changelog.py --version X.Y.Z --date YYYY-MM-DD [--dry-run]

``--check`` validates the fragment tree only (section names, list item
format, trailing newline) and exits non zero on the first problem set.
``--dry-run`` prints the assembled ``CHANGELOG.md`` to stdout and leaves
every file untouched. Only the Python standard library is used.
"""

import argparse
import datetime
import re
import sys
from pathlib import Path

SECTIONS = (
    "Features",
    "Changed",
    "Fixed",
    "Infrastructure",
    "Documentation",
    "Maintenance",
    "Testing",
)
SECTION_DIRS = {name.lower(): name for name in SECTIONS}

UNRELEASED_HEADING = "## [Unreleased]"
FRAGMENT_DIR = Path("changelog") / "unreleased"
CHANGELOG_FILE = "CHANGELOG.md"
VERSION_PATTERN = re.compile(r"^\d+\.\d+\.\d+$")


class ChangelogError(Exception):
    """A validation or layout problem the user has to fix by hand."""


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def check_fragments(fragment_dir: Path) -> list:
    """Return every problem found in the fragment tree, as human readable strings."""
    problems = []
    if not fragment_dir.is_dir():
        return [f"{fragment_dir} does not exist"]
    for entry in sorted(fragment_dir.iterdir()):
        if entry.is_dir():
            if entry.name not in SECTION_DIRS:
                problems.append(
                    f"{entry}: unknown section directory, expected one of "
                    + ", ".join(sorted(SECTION_DIRS))
                )
                continue
            for fragment in sorted(entry.iterdir()):
                problems.extend(check_fragment(fragment))
        elif entry.name != "README.md":
            problems.append(f"{entry}: fragments belong in a section directory")
    return problems


def check_fragment(fragment: Path) -> list:
    if not fragment.is_file() or fragment.suffix != ".md":
        return [f"{fragment}: only .md files are allowed in a section directory"]
    text = fragment.read_text(encoding="utf-8")
    if text.strip() == "":
        return [f"{fragment}: empty fragment"]
    problems = []
    if not text.endswith("\n"):
        problems.append(f"{fragment}: missing trailing newline")
    for number, line in enumerate(text.splitlines(), start=1):
        if line.strip() == "":
            problems.append(
                f"{fragment}:{number}: blank or whitespace only line, a fragment holds list items only"
            )
        elif number == 1 and not line.startswith("- "):
            problems.append(f"{fragment}:{number}: must start with a markdown list item ('- ')")
        elif not (line.startswith("- ") or line.startswith("  ")):
            problems.append(
                f"{fragment}:{number}: every line must start a list item ('- ') "
                "or continue one (two leading spaces)"
            )
    return problems


def read_fragments(fragment_dir: Path) -> dict:
    """Map each section name to the concatenated list items of its fragments."""
    problems = check_fragments(fragment_dir)
    if problems:
        raise ChangelogError("\n".join(problems))
    entries = {}
    for dir_name, section in SECTION_DIRS.items():
        section_dir = fragment_dir / dir_name
        if not section_dir.is_dir():
            continue
        items = []
        for fragment in sorted(section_dir.iterdir()):
            items.append(fragment.read_text(encoding="utf-8").rstrip("\n"))
        if items:
            entries[section] = items
    return entries


def fragment_files(fragment_dir: Path) -> list:
    files = []
    for dir_name in SECTION_DIRS:
        section_dir = fragment_dir / dir_name
        if section_dir.is_dir():
            files.extend(sorted(section_dir.iterdir()))
    return files


def split_changelog(text: str):
    """Split CHANGELOG.md into (head, unreleased body, tail).

    ``head`` ends with the ``## [Unreleased]`` line, ``tail`` starts at the
    next ``## `` heading (or is empty), and ``unreleased`` holds the lines in
    between.
    """
    lines = text.splitlines()
    try:
        start = lines.index(UNRELEASED_HEADING)
    except ValueError:
        raise ChangelogError(f"{CHANGELOG_FILE}: no '{UNRELEASED_HEADING}' heading") from None
    end = len(lines)
    for index in range(start + 1, len(lines)):
        if lines[index].startswith("## "):
            end = index
            break
    return lines[: start + 1], lines[start + 1 : end], lines[end:]


def parse_unreleased(body: list) -> dict:
    """Map each ``### Section`` under Unreleased to its non blank body lines."""
    entries = {}
    section = None
    for line in body:
        if line.startswith("### "):
            section = line[4:].strip()
            if section not in SECTIONS:
                raise ChangelogError(
                    f"{CHANGELOG_FILE}: unknown section '{section}' under {UNRELEASED_HEADING}"
                )
            entries.setdefault(section, [])
        elif line.strip() == "":
            continue
        elif section is None:
            raise ChangelogError(
                f"{CHANGELOG_FILE}: text under {UNRELEASED_HEADING} outside a '### ' section: {line}"
            )
        else:
            entries[section].append(line)
    return {name: lines for name, lines in entries.items() if lines}


def release_block(version: str, date: str, existing: dict, fragments: dict) -> list:
    lines = [f"## [{version}] - {date}"]
    for section in SECTIONS:
        items = list(existing.get(section, [])) + list(fragments.get(section, []))
        if not items:
            continue
        lines.append("")
        lines.append(f"### {section}")
        lines.append("")
        lines.extend(items)
    if len(lines) == 1:
        raise ChangelogError("nothing to release: no fragments and an empty Unreleased section")
    return lines


def assemble(changelog_text: str, fragments: dict, version: str, date: str) -> str:
    head, unreleased, tail = split_changelog(changelog_text)
    existing = parse_unreleased(unreleased)
    block = release_block(version, date, existing, fragments)
    lines = head + [""] + block
    if tail:
        lines += [""] + tail
    return "\n".join(lines) + "\n"


def validate_version(value: str) -> str:
    if not VERSION_PATTERN.match(value):
        raise argparse.ArgumentTypeError("expected X.Y.Z")
    return value


def validate_date(value: str) -> str:
    try:
        datetime.date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError("expected YYYY-MM-DD") from None
    return value


def parse_args(argv):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--version", type=validate_version, help="release version, X.Y.Z")
    parser.add_argument("--date", type=validate_date, help="release date, YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true", help="print the result, change nothing")
    parser.add_argument("--check", action="store_true", help="validate the fragments only")
    parser.add_argument(
        "--root",
        type=Path,
        default=repo_root(),
        help="repository root holding CHANGELOG.md and changelog/unreleased (default: the checkout)",
    )
    args = parser.parse_args(argv)
    if not args.check and (args.version is None or args.date is None):
        parser.error("--version and --date are required unless --check is given")
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    fragment_dir = args.root / FRAGMENT_DIR
    changelog = args.root / CHANGELOG_FILE
    try:
        if args.check:
            problems = check_fragments(fragment_dir)
            if problems:
                raise ChangelogError("\n".join(problems))
            count = len(fragment_files(fragment_dir))
            print(f"{count} fragment(s) under {fragment_dir} are well formed")
            return 0
        fragments = read_fragments(fragment_dir)
        result = assemble(changelog.read_text(encoding="utf-8"), fragments, args.version, args.date)
    except ChangelogError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    if args.dry_run:
        sys.stdout.write(result)
        return 0
    changelog.write_text(result, encoding="utf-8")
    consumed = fragment_files(fragment_dir)
    for fragment in consumed:
        fragment.unlink()
    print(f"wrote {changelog} and removed {len(consumed)} fragment(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
