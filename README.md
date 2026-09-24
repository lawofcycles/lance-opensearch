# OpenSearch Lance

OpenSearch plugin that surfaces [Lance](https://github.com/lancedb/lance) tables as OpenSearch indexes, without copying data.

[![build](https://github.com/lawofcycles/lance-opensearch/actions/workflows/CI.yml/badge.svg)](https://github.com/lawofcycles/lance-opensearch/actions/workflows/CI.yml)

## Overview

Lance is an open columnar table format with multimodal columns, concurrent writers, manifest versioned snapshots, and its own full text and vector indexes. One copy of a Lance table can serve vector search, full text search and analytical aggregation at once.

This plugin makes such a table addressable through OpenSearch's APIs. An operator attaches one table (`POST /_lance/attach`) or registers a catalog of tables (`POST /_lance/namespace`); the plugin derives an OpenSearch mapping from the table's Arrow schema and creates a read only index for it. From then on `_search`, `_count`, `GET /<index>/_doc/<id>` and the monitoring APIs answer from the Lance table directly. No documents are ingested and no Lucene copy of the data is built: every Lance fragment is adapted as a Lucene leaf reader at query time, so OpenSearch's stock aggregators, collectors and query DSL run over Lance data, and a cost based query planner folds the work Lance does better natively (full text lookups, nearest neighbour scans, filters and aggregations) into the Lance scan itself. When a writer commits a new manifest version, the index follows it without a reindex.

The plugin implements [RFC #22643](https://github.com/opensearch-project/OpenSearch/issues/22643). [docs/features.md](docs/features.md) is the full feature reference.

## Status

This plugin is in preview and is not production ready. The read side works end to end against real Lance tables; interfaces, settings and response shapes can still change between releases without a deprecation period.

Built against OpenSearch 3.8.0 with Lance 12.0.0. Every node of a cluster must run the same plugin version: rolling upgrades between plugin versions are not supported yet.

## Quick start

Requires OpenSearch 3.8.0, JDK 21, and macOS/aarch64, linux/x86_64 or linux/aarch64.

```
./gradlew build
bin/opensearch-plugin install file:///absolute/path/to/build/distributions/opensearch-lance-0.1.0.zip
```

Every node also needs two JVM options for Arrow; [docs/getting-started.md](docs/getting-started.md) has them together with the Docker and native install paths, a script that prepares a sample Lance table, and every query shape.

## Documentation

- [docs/getting-started.md](docs/getting-started.md), build, install, prepare a table, run the query shapes.
- [docs/features.md](docs/features.md), feature reference: attach and namespace surface, query shapes, aggregations, query plan, freshness, caches, memory bounds.
- [docs/limitations.md](docs/limitations.md), known limitations and refused shapes.
- [docs/query-plan.md](docs/query-plan.md), the explain endpoint, the physical operators and the cost model.
- [docs/architecture.md](docs/architecture.md), how the plugin is put together.
- [docs/dependencies.md](docs/dependencies.md), what the plugin zip bundles and why.
- [CHANGELOG.md](CHANGELOG.md), release notes; unreleased entries are fragments under [changelog/unreleased/](changelog/unreleased/README.md).
- [RFC #22643](https://github.com/opensearch-project/OpenSearch/issues/22643), the design discussion.

## Contributing

Bug reports, feature requests, documentation fixes and code are welcome. [CONTRIBUTING.md](CONTRIBUTING.md) explains how to open an issue, the pull request process and the changelog fragment every pull request adds; [DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md) covers the build, the precommit checks and the tests. Every commit must be signed off (`git commit -s`) under the Developer Certificate of Origin.

## License

This project is licensed under the [Apache v2.0 License](LICENSE.txt). Copyright the contributors; see [NOTICE.txt](NOTICE.txt).
