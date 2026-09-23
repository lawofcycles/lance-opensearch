[![build](https://github.com/lawofcycles/lance-opensearch/actions/workflows/CI.yml/badge.svg)](https://github.com/lawofcycles/lance-opensearch/actions/workflows/CI.yml)
<!-- Badges to add once the repository lives under opensearch-project: the CI.yml badge of the organization repository, codecov, the documentation page on opensearch.org, the forum category, and "PRs welcome". The URLs do not exist yet, so they are not written here. -->

# OpenSearch Lance

- [Welcome!](#welcome)
- [Status](#status)
- [What it does](#what-it-does)
- [Requirements](#requirements)
- [Build](#build)
- [Try it](#try-it)
- [Documentation](#documentation)
- [Project Resources](#project-resources)
- [Feedback](#feedback)
- [Code of Conduct](#code-of-conduct)
- [Security](#security)
- [License](#license)
- [Copyright](#copyright)

## Welcome!

**OpenSearch Lance** is an OpenSearch plugin that surfaces [Lance](https://github.com/lancedb/lance) tables as searchable indexes. Implementation of [RFC #22643](https://github.com/opensearch-project/OpenSearch/issues/22643).

Lance is an open columnar table format with multimodal columns, concurrent writers, and time travel via manifest versions. The format ships with vector and full text indexes, so vector search, full text search, and analytical aggregation can all run against the same table without extracting data or maintaining a parallel copy. This plugin registers a Lance table as a shardable OpenSearch index without copying data out of Lance: OpenSearch's engine treats each Lance fragment as a Lucene leaf, delegates full text and vector work to the indexes stored inside the Lance table, and follows the table forward as new manifest versions land.

## Status

Early draft. Not production ready. The read-side plumbing (namespace registration, mapping derivation, `_search` / GET / aggregation shapes, and the follow-forward refresh loop) works end to end against real Lance tables, but interfaces and settings are still shifting. Use it to explore the design or try it against your own Lance tables, not to run anything you depend on.

Built against OpenSearch 3.8.0 with Lance 12.0.0.

## What it does

- Registers a directory or a single Lance table URI and surfaces it as an OpenSearch index. Mapping derives from the Lance Arrow schema.
- Runs full text (`match`, `lance_match`, `lance_match_phrase`, `lance_multi_match`, `lance_fts_boost`, `lance_fts_bool`), vector (`lance_knn`), primary-key GET, and aggregation queries through a fragment fan-out that drives Lucene's stock aggregator machinery over per-fragment leaf readers.
- Follows the Lance manifest forward automatically. `"version": N` on attach also pins a readonly snapshot.
- Accepts per-table `storage_options` (S3, GCS, Azure) so a single JVM can address multiple buckets with different credentials.

See [docs/features.md](docs/features.md) for the full feature list and [docs/limitations.md](docs/limitations.md) for known limitations and shapes routed to the shard path.

## Requirements

- Java 21
- OpenSearch 3.8.0
- macOS/aarch64, linux/x86_64, or linux/aarch64 (Lance's Rust native library ships in `org.lance:lance-core` for these platforms)

## Build

```
./gradlew build
```

Produces `build/distributions/opensearch-lance-0.1.0.zip`.

To run the tests:

```
./gradlew test integTest multiNodeIntegTest
```

The [Developer Guide](DEVELOPER_GUIDE.md) covers the build in detail: prerequisites, the precommit checks the CI runs, running one test class, debugging, and building against another OpenSearch version.

## Try it

The end-to-end walkthrough (install into OpenSearch, prepare a Lance table, register a namespace, run every query shape) is in [docs/getting-started.md](docs/getting-started.md).

## Documentation

- [docs/getting-started.md](docs/getting-started.md) — Build, install, prepare a table, run the query shapes.
- [docs/features.md](docs/features.md) — Feature reference by concern.
- [docs/limitations.md](docs/limitations.md) — Known limitations and shard-path fall-throughs.
- [docs/architecture.md](docs/architecture.md) — How the plugin is put together: components, request paths, planner design, design intent.
- [CHANGELOG.md](CHANGELOG.md) — Release notes.

## Project Resources

* [Project Website](https://opensearch.org/)
* [Downloads](https://opensearch.org/downloads.html)
* [Project Principles](https://opensearch.org/#principles)
* [Contributing to OpenSearch Lance](CONTRIBUTING.md)
* [Developer Guide](DEVELOPER_GUIDE.md)
* [Maintainer Responsibilities](MAINTAINERS.md)
* [Release Management](RELEASING.md)
* [Admin Responsibilities](ADMINS.md)
* [Security](SECURITY.md)

## Feedback

If you try this against your own Lance data, please open an issue with what you observed, which schemas failed to map, and which queries did or did not behave as expected. Design questions belong on the RFC thread ([opensearch-project/OpenSearch#22643](https://github.com/opensearch-project/OpenSearch/issues/22643)).

## Code of Conduct

This project has adopted the [Amazon Open Source Code of Conduct](CODE_OF_CONDUCT.md). For more information see the [Code of Conduct FAQ](https://aws.github.io/code-of-conduct-faq), or contact [opensource-codeofconduct@amazon.com](mailto:opensource-codeofconduct@amazon.com) with any additional questions or comments.

## Security

If you discover a potential security issue in this project we ask that you notify OpenSearch Security directly via email to security@opensearch.org. Please do **not** create a public GitHub issue. See [SECURITY.md](SECURITY.md).

## License

This project is licensed under the [Apache v2.0 License](LICENSE.txt).

## Copyright

Copyright OpenSearch Contributors. See [NOTICE](NOTICE.txt) for details.
