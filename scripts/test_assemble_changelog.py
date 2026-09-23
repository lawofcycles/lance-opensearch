"""Unit tests for scripts/assemble-changelog.py.

Run from the repository root with ``python3 -m unittest scripts/test_assemble_changelog.py``.
"""

import contextlib
import importlib.util
import io
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent / "assemble-changelog.py"
spec = importlib.util.spec_from_file_location("assemble_changelog", SCRIPT)
assemble_changelog = importlib.util.module_from_spec(spec)
spec.loader.exec_module(assemble_changelog)

HEADER = """# CHANGELOG

Inspired by [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]
"""

PREVIOUS_RELEASE = """
## [0.1.0] - 2026-01-01

### Features

- First release.
"""

FEATURE_A = "- `POST /_lance/attach` accepts a `version` clause that pins a snapshot.\n"
FEATURE_B = (
    "- `lance_knn` fans out one nearest scan per shard.\n"
    "  The coordinator merge reconstructs the global top k.\n"
)
FIX_A = "- The namespace poll no longer races the catalog handle's release. (#217)\n"


class FragmentTree:
    """A temporary repository root with CHANGELOG.md and changelog/unreleased."""

    def __init__(self, changelog: str, fragments: dict):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        (self.root / "CHANGELOG.md").write_text(changelog, encoding="utf-8")
        unreleased = self.root / "changelog" / "unreleased"
        unreleased.mkdir(parents=True)
        (unreleased / "README.md").write_text("Fragments live here.\n", encoding="utf-8")
        for relative, text in fragments.items():
            path = unreleased / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")

    def cleanup(self):
        self.tmp.cleanup()

    def changelog(self) -> str:
        return (self.root / "CHANGELOG.md").read_text(encoding="utf-8")

    def fragments(self) -> list:
        unreleased = self.root / "changelog" / "unreleased"
        return sorted(
            str(path.relative_to(unreleased))
            for path in unreleased.rglob("*.md")
            if path.name != "README.md"
        )


def run(tree: FragmentTree, *argv: str):
    """Run main() against the tree, returning (exit code, stdout, stderr)."""
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = assemble_changelog.main([*argv, "--root", str(tree.root)])
    return code, out.getvalue(), err.getvalue()


class AssembleTests(unittest.TestCase):
    def setUp(self):
        self.tree = FragmentTree(
            HEADER + PREVIOUS_RELEASE,
            {
                "features/knn.md": FEATURE_B,
                "features/attach-version.md": FEATURE_A,
                "fixed/217-handle-release.md": FIX_A,
            },
        )
        self.addCleanup(self.tree.cleanup)

    def test_release_block_follows_the_fixed_section_order_and_sorted_file_names(self):
        code, out, err = run(self.tree, "--version", "0.2.0", "--date", "2026-02-01", "--dry-run")
        self.assertEqual(code, 0, err)
        expected = (
            HEADER
            + "\n## [0.2.0] - 2026-02-01\n"
            + "\n### Features\n\n"
            + FEATURE_A
            + FEATURE_B
            + "\n### Fixed\n\n"
            + FIX_A
            + PREVIOUS_RELEASE
        )
        self.assertEqual(out, expected)

    def test_dry_run_changes_nothing(self):
        before = self.tree.changelog()
        code, _, err = run(self.tree, "--version", "0.2.0", "--date", "2026-02-01", "--dry-run")
        self.assertEqual(code, 0, err)
        self.assertEqual(self.tree.changelog(), before)
        self.assertEqual(
            self.tree.fragments(),
            ["features/attach-version.md", "features/knn.md", "fixed/217-handle-release.md"],
        )

    def test_release_writes_the_changelog_and_removes_the_fragments(self):
        code, _, err = run(self.tree, "--version", "0.2.0", "--date", "2026-02-01")
        self.assertEqual(code, 0, err)
        self.assertIn("## [0.2.0] - 2026-02-01", self.tree.changelog())
        self.assertEqual(self.tree.fragments(), [])
        self.assertTrue((self.tree.root / "changelog" / "unreleased" / "README.md").exists())
        # A second release finds nothing to publish and leaves the file alone.
        after = self.tree.changelog()
        code, _, err = run(self.tree, "--version", "0.3.0", "--date", "2026-03-01")
        self.assertEqual(code, 1)
        self.assertIn("nothing to release", err)
        self.assertEqual(self.tree.changelog(), after)

    def test_existing_unreleased_entries_lead_their_section(self):
        legacy = (
            HEADER
            + "\n### Fixed\n\n- An entry written straight into CHANGELOG.md.\n"
            + "\n### Testing\n\n- A test entry.\n"
            + PREVIOUS_RELEASE
        )
        (self.tree.root / "CHANGELOG.md").write_text(legacy, encoding="utf-8")
        code, out, err = run(self.tree, "--version", "0.2.0", "--date", "2026-02-01", "--dry-run")
        self.assertEqual(code, 0, err)
        expected = (
            HEADER
            + "\n## [0.2.0] - 2026-02-01\n"
            + "\n### Features\n\n"
            + FEATURE_A
            + FEATURE_B
            + "\n### Fixed\n\n- An entry written straight into CHANGELOG.md.\n"
            + FIX_A
            + "\n### Testing\n\n- A test entry.\n"
            + PREVIOUS_RELEASE
        )
        self.assertEqual(out, expected)

    def test_unreleased_at_end_of_file_gets_no_trailing_blank_line(self):
        (self.tree.root / "CHANGELOG.md").write_text(HEADER, encoding="utf-8")
        code, out, err = run(self.tree, "--version", "0.2.0", "--date", "2026-02-01", "--dry-run")
        self.assertEqual(code, 0, err)
        self.assertTrue(out.startswith(HEADER + "\n## [0.2.0] - 2026-02-01\n"))
        self.assertTrue(out.endswith(FIX_A))

    def test_missing_unreleased_heading_is_an_error(self):
        (self.tree.root / "CHANGELOG.md").write_text("# CHANGELOG\n", encoding="utf-8")
        code, _, err = run(self.tree, "--version", "0.2.0", "--date", "2026-02-01", "--dry-run")
        self.assertEqual(code, 1)
        self.assertIn("no '## [Unreleased]' heading", err)


class CheckTests(unittest.TestCase):
    def check(self, fragments: dict):
        tree = FragmentTree(HEADER, fragments)
        self.addCleanup(tree.cleanup)
        return run(tree, "--check")

    def test_well_formed_fragments_pass(self):
        code, out, err = self.check({"features/a.md": FEATURE_A, "fixed/b.md": FIX_A})
        self.assertEqual(code, 0, err)
        self.assertIn("2 fragment(s)", out)

    def test_unknown_section_directory_fails(self):
        code, _, err = self.check({"bugfix/a.md": FIX_A})
        self.assertEqual(code, 1)
        self.assertIn("unknown section directory", err)

    def test_fragment_outside_a_section_fails(self):
        code, _, err = self.check({"stray.md": FIX_A})
        self.assertEqual(code, 1)
        self.assertIn("belong in a section directory", err)

    def test_empty_fragment_fails(self):
        code, _, err = self.check({"fixed/a.md": "\n"})
        self.assertEqual(code, 1)
        self.assertIn("empty fragment", err)

    def test_missing_list_marker_fails(self):
        code, _, err = self.check({"fixed/a.md": "Fixed a thing.\n"})
        self.assertEqual(code, 1)
        self.assertIn("must start with a markdown list item", err)

    def test_line_that_neither_starts_nor_continues_an_item_fails(self):
        code, _, err = self.check({"fixed/a.md": "- Fixed a thing.\nMore text.\n"})
        self.assertEqual(code, 1)
        self.assertIn(":2: every line must start a list item", err)

    def test_missing_trailing_newline_fails(self):
        code, _, err = self.check({"fixed/a.md": "- Fixed a thing."})
        self.assertEqual(code, 1)
        self.assertIn("missing trailing newline", err)

    def test_non_markdown_file_in_a_section_fails(self):
        code, _, err = self.check({"fixed/a.txt": FIX_A})
        self.assertEqual(code, 1)
        self.assertIn("only .md files", err)

    def test_check_reports_every_problem_at_once(self):
        code, _, err = self.check({"fixed/a.md": "Fixed.", "bugfix/b.md": FIX_A})
        self.assertEqual(code, 1)
        self.assertIn("unknown section directory", err)
        self.assertIn("missing trailing newline", err)
        self.assertIn("must start with a markdown list item", err)


class ArgumentTests(unittest.TestCase):
    def test_version_and_date_are_required_without_check(self):
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            assemble_changelog.parse_args(["--dry-run"])

    def test_malformed_version_and_date_are_rejected(self):
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            assemble_changelog.parse_args(["--version", "1.0", "--date", "2026-01-01"])
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            assemble_changelog.parse_args(["--version", "1.0.0", "--date", "2026-13-01"])


if __name__ == "__main__":
    unittest.main()
