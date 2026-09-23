# Release notes

One file per release, named `opensearch-lance.release-notes-<version>.md`, where `<version>` is the plugin version followed by the OpenSearch compatibility digit, for example `opensearch-lance.release-notes-0.1.0.0.md` for plugin 0.1.0 built against OpenSearch 3.8.0. This is the naming the other plugin repositories in the opensearch-project organization use (`opensearch-knn.release-notes-3.1.0.0.md`).

Each file starts with `## Version <version> Release Notes`, then `Compatible with OpenSearch <opensearch version>`, then the sections of the matching release block of [`CHANGELOG.md`](../CHANGELOG.md), which `python3 scripts/assemble-changelog.py --version X.Y.Z --date YYYY-MM-DD` produces from the fragments under [`changelog/unreleased/`](../changelog/unreleased/README.md). The steps are in [RELEASING.md](../RELEASING.md).

There is no release yet, so this directory holds only this file. The first release adds `opensearch-lance.release-notes-0.1.0.0.md`.
