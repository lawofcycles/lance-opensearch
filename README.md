# OpenSearch Lance

OpenSearch plugin that surfaces [Lance](https://github.com/lancedb/lance) tables as OpenSearch indexes, without copying data.

[![build](https://github.com/lawofcycles/lance-opensearch/actions/workflows/CI.yml/badge.svg)](https://github.com/lawofcycles/lance-opensearch/actions/workflows/CI.yml)

## Overview

Lance is an open columnar table format with multimodal columns, concurrent writers, manifest versioned snapshots, and its own full text and vector indexes. One copy of a Lance table can serve vector search, full text search and analytical aggregation at once.

This plugin makes such a table addressable through OpenSearch's APIs. An operator attaches one table (`POST /_lance/attach`) or a catalog of tables (`POST /_lance/namespace`); the plugin derives a mapping from the table's Arrow schema and creates a read only index. `_search`, `_count` and `GET /<index>/_doc/<id>` then read the Lance table directly; nothing is ingested and nothing is copied.

Every request fans the table's fragments out over the data nodes. A cost based planner decides per request what Lance runs natively (filters, full text, nearest neighbour, Substrait aggregations) and what OpenSearch's stock collectors and aggregators run. New table versions need no reindex. The design is discussed in [RFC #22643](https://github.com/opensearch-project/OpenSearch/issues/22643).

## Status

This plugin is in preview and is not production ready. The read side works end to end against real Lance tables. Interfaces, settings and response shapes can change between releases without a deprecation period. It is built against OpenSearch 3.8.0 with Lance 12.0.0; a rolling upgrade may run two plugin versions at once ([docs/design/wire-format-compat.md](docs/design/wire-format-compat.md)).

## Quick start

Requires OpenSearch 3.8.0, JDK 21, and macOS/aarch64, linux/x86_64 or linux/aarch64. Build the plugin and install it on every node:

```
./gradlew build
bin/opensearch-plugin install file:///absolute/path/to/build/distributions/opensearch-lance-0.1.0.zip
```

Add the two JVM options Arrow needs to `config/jvm.options` (or set them via `OPENSEARCH_JAVA_OPTS`) on every node, then start OpenSearch:

```
--add-opens=java.base/java.nio=ALL-UNNAMED
-Darrow.allocation.manager.type=Unsafe
```

Attach a Lance table and search it ([docs/getting-started.md](docs/getting-started.md#2a-docker-single-node) does the same in Docker and prepares a sample table):

```
curl -X POST http://localhost:9200/_lance/attach -H 'Content-Type: application/json' \
  -d '{"table":"/tables/demo.lance"}'
curl -s -X POST 'http://localhost:9200/demo/_search?size=3' -H 'Content-Type: application/json' \
  -d '{"query":{"match":{"body":"hello"}}}'
```

## Documentation

- Run it for the first time, with a sample table and every query shape: [docs/getting-started.md](docs/getting-started.md).
- What it can and cannot do: [docs/features.md](docs/features.md) and [docs/limitations.md](docs/limitations.md).
- How a request is executed, and the explain endpoint: [docs/query-plan.md](docs/query-plan.md).
- How the plugin is put together: [docs/architecture.md](docs/architecture.md).
- What the plugin zip bundles and why: [docs/dependencies.md](docs/dependencies.md).
- Wire format and rolling upgrades: [docs/design/wire-format-compat.md](docs/design/wire-format-compat.md).
- How tables surfaced from a namespace stay fresh: [docs/design/namespace-freshness.md](docs/design/namespace-freshness.md).

## Contributing

Bug reports, feature requests, documentation fixes and code are welcome. [CONTRIBUTING.md](CONTRIBUTING.md) explains how to open an issue and the pull request process; [DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md) covers the build, the precommit checks and the tests. Every commit must be signed off (`git commit -s`) under the Developer Certificate of Origin.

## License

This project is licensed under the [Apache v2.0 License](LICENSE.txt). Copyright the contributors; see [NOTICE.txt](NOTICE.txt).
