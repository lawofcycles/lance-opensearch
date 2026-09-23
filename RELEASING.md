- [Overview](#overview)
- [Branching](#branching)
  - [Release Branching](#release-branching)
  - [Feature Branches](#feature-branches)
- [Release Labels](#release-labels)
- [Releasing](#releasing)
  - [Assembling the changelog](#assembling-the-changelog)
  - [Release notes](#release-notes)
- [Workflows to enable after the repository moves](#workflows-to-enable-after-the-repository-moves)

## Overview

This document explains the release strategy for artifacts in this organization, and the steps that are specific to this repository.

## Branching

### Release Branching

Given the current major release of 1.0, projects in this organization maintain the following active branches.

* **main**: The next _major_ release. This is the branch where all merges take place and code moves fast.
* **1.x**: The next _minor_ release. Once a change is merged into `main`, decide whether to backport it to `1.x`.
* **1.0**: The _current_ release. In between minor releases, only hotfixes (e.g. security) are backported to `1.0`.

Label PRs with the next major version label (e.g. `2.0.0`) and merge changes into `main`. Label PRs that you believe need to be backported as `1.x` and `1.0`. Backport PRs by checking out the versioned branch, cherry-pick changes and open a PR against each target backport branch.

This repository currently has only `main`; no release branch has been cut yet.

### Feature Branches

Do not create branches in the upstream repo, use your fork, for the exception of long lasting feature branches that require active collaboration from multiple developers. Name feature branches `feature/<thing>`. Once the work is merged to `main`, please make sure to delete the feature branch.

## Release Labels

Repositories create consistent release labels, such as `v1.0.0`, `v1.1.0` and `v2.0.0`, as well as `backport`. Use release labels to target an issue or a PR for a given release. See [MAINTAINERS](MAINTAINERS.md) for more information on triaging issues.

## Releasing

The release process is standard across repositories in this org and is run by a release manager volunteering from amongst [MAINTAINERS](MAINTAINERS.md).

This repository adds two steps of its own before the release commit.

### Assembling the changelog

Unreleased changelog entries live as fragment files under [`changelog/unreleased/`](changelog/unreleased/README.md), one per pull request. Before the release commit, assemble them into `CHANGELOG.md`:

```
python3 scripts/assemble-changelog.py --version X.Y.Z --date YYYY-MM-DD
```

The script concatenates the fragments in section order into a new `## [X.Y.Z] - YYYY-MM-DD` block below `## [Unreleased]`, carries any entries still written directly under `## [Unreleased]` ahead of them, and deletes the consumed fragments. Add `--dry-run` to preview the result without writing. Commit the rewritten `CHANGELOG.md` and the removed fragments together with the version bump in `gradle.properties`.

### Release notes

Copy the new release block into `release-notes/opensearch-lance.release-notes-<version>.md`, where `<version>` is the plugin version followed by the OpenSearch compatibility digit (for example `0.1.0.0` for plugin 0.1.0 against OpenSearch 3.8.0). The file starts with the heading `## Version <version> Release Notes` and the line `Compatible with OpenSearch <opensearch version>`, then the sections of the release block. See [`release-notes/README.md`](release-notes/README.md) for the naming convention.

## Workflows to enable after the repository moves

Three of the organization's standard workflows are not in `.github/workflows/` yet, because each one needs an organization secret, a release branch, or a Maven publishing target that this repository does not have while it lives under a personal account. Add them when the repository is transferred into the opensearch-project organization, copying the current files from a sibling plugin repository such as k-NN or neural-search and replacing the plugin name.

* `backport.yml`: opens a backport pull request for a merged pull request labelled `backport <branch>`. Needs the release branches from [Release Branching](#release-branching) to exist and the organization's backport GitHub App token.
* `auto-release.yml`: creates the GitHub release from a pushed release tag using the draft release notes. Needs the organization's release token and the `release-notes/` file for the version.
* `maven-publish.yml`: publishes the plugin zip and jar to the organization's Sonatype staging repository on a push to a release branch. Needs the Sonatype credentials held as organization secrets.

At the same time, change the repository guard in `.github/workflows/delete_backport_branch.yml` from `lawofcycles/lance-opensearch` to the organization's repository name, and replace the placeholder badge lines at the top of `README.md` with the organization's build, documentation and forum badges.
