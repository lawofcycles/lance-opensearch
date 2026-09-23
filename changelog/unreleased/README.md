# Unreleased changelog fragments

Every pull request records its changelog entry here instead of editing `CHANGELOG.md`, so parallel pull requests no longer conflict on the same lines.

- One file per pull request, at `changelog/unreleased/<section>/<topic>.md`.
- `<section>` is one of `features`, `changed`, `fixed`, `infrastructure`, `documentation`, `maintenance`, `testing` (the `CHANGELOG.md` section names in lower case).
- `<topic>` is the branch name without its `issue` prefix (`155-d4b.md`) or a short topic (`hive-catalogs.md`). A pull request that touches several sections puts one file in each.
- The file holds one or more markdown list items (`- ...`), written like the existing `CHANGELOG.md` entries: what changed, why, and what a user observes. Put issue references at the end of the item as `(#N)`. End the file with a newline.
- A pull request with nothing to record (typo fixes, CI tweaks) carries the `skip-changelog` label, or a line reading only `skip-changelog` in its description.

The `changelog` workflow fails a pull request that neither adds a fragment nor opts out, and runs `python3 scripts/assemble-changelog.py --check` over the tree.

At release time `python3 scripts/assemble-changelog.py --version X.Y.Z --date YYYY-MM-DD` concatenates the fragments in section order into a new release block of `CHANGELOG.md` and deletes them. Add `--dry-run` to preview the result.
