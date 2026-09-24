[![build](https://github.com/lawofcycles/lance-opensearch/actions/workflows/CI.yml/badge.svg)](https://github.com/lawofcycles/lance-opensearch/actions/workflows/CI.yml)
<!-- Badges to add once the repository lives under opensearch-project: the CI.yml badge of the organization repository, codecov, the documentation page on opensearch.org, the forum category, and "PRs welcome". The URLs do not exist yet, so they are not written here. -->

# OpenSearch Lance

OpenSearch plugin that surfaces [Lance](https://github.com/lancedb/lance) tables as OpenSearch indexes, without copying data.

- [Overview](#overview)
- [Status](#status)
- [Features](#features)
- [Requirements](#requirements)
- [Install](#install)
- [Getting started](#getting-started)
- [Documentation](#documentation)
- [Releases](#releases)
- [Contributing](#contributing)
- [Project Resources](#project-resources)
- [Code of Conduct](#code-of-conduct)
- [Security](#security)
- [License](#license)
- [Copyright](#copyright)

## Overview

Lance is an open columnar table format with multimodal columns, concurrent writers, manifest versioned snapshots, and its own full text and vector indexes. One copy of a Lance table can serve vector search, full text search and analytical aggregation at once.

This plugin makes such a table addressable through OpenSearch's APIs. An operator attaches one table (`POST /_lance/attach`) or registers a catalog of tables (`POST /_lance/namespace`); the plugin derives an OpenSearch mapping from the table's Arrow schema and creates a read only index for it. From then on `_search`, `_count`, `GET /<index>/_doc/<id>` and the monitoring APIs answer from the Lance table directly. No documents are ingested and no Lucene copy of the data is built: every Lance fragment is adapted as a Lucene leaf reader at query time, so OpenSearch's stock aggregators, collectors and query DSL run over Lance data, and a cost based query planner folds the work Lance does better natively (full text lookups, nearest neighbour scans, filters and aggregations) into the Lance scan itself. When a writer commits a new manifest version, the index follows it without a reindex.

The plugin implements [RFC #22643](https://github.com/opensearch-project/OpenSearch/issues/22643).

## Status

This plugin is in preview. It is not production ready.

The read side works end to end against real Lance tables: attach and catalog registration, mapping derivation, `_search` with full text, vector, filter and aggregation shapes, GET by primary key, the explain endpoint, and the follow forward refresh. Interfaces, settings and response shapes can still change between releases without a deprecation period.

Every node of a cluster must run the same plugin version. The messages the plugin sends between nodes carry a version marker and a node running another plugin version refuses them, so a rolling upgrade between plugin versions is not possible today. Rolling upgrade support is planned and not implemented.

Built against OpenSearch 3.8.0 with Lance 12.0.0.

## Features

- Zero copy attach of a Lance table (`POST /_lance/attach`) or of a whole namespace of tables (`POST /_lance/namespace`). Attach also pins a manifest version (`"version": N`) or follows a Lance tag (`"tag": "name"`).
- Mapping derived from the Lance Arrow schema: integers, floats, booleans, dates and timestamps, Utf8 as `keyword` or (with a Lance inverted index) `lance_text`, lists of Utf8, structs as `object`, lists of structs as `nested`, fixed size float lists as `knn_vector`, binary. Per column `overrides` add `date`, `keyword`, `ip`, `wildcard`, `geo_point` and OpenSearch analyzer mode (`text_analyzer`) mappings and keyword sub fields.
- Search on the fragment executors, fanned out over every data node: full text with the stock `match`, `match_phrase`, `multi_match` and `bool` syntax or the Lance native `lance_match`, `lance_match_phrase`, `lance_multi_match`, `lance_fts_bool` and `lance_fts_boost` queries; vectors with `lance_knn` and a Lance side pre filter; scalar filters, sorted pages, `search_after`, `collapse`, `rescore` and the stock hit shape (`_source`, `fields`, `docvalue_fields`, `explain`).
- Aggregations either pushed into Lance's Substrait scan (metrics, `terms`, `histogram`, `date_histogram`, `range`, `filters`, `composite` and nested chains of them, computed by Lance's DataFusion kernel) or run by OpenSearch's stock aggregators over the per fragment leaf readers, chosen per request by a cost based planner built on Apache Calcite. Pipeline aggregations reduce on the coordinator.
- Native index builds from OpenSearch (`POST /_lance/build_indexes/{index}`): inverted indexes with a chosen tokenizer, scalar indexes (BTree, Bitmap, Zone Map, Bloom Filter, N-gram, Label List) and vector indexes (IVF_FLAT, IVF_PQ, IVF_SQ, IVF_RQ, IVF_HNSW_PQ, IVF_HNSW_SQ), with per column type selection on the attach body. `node_local` placement builds into a shallow clone on each data node when the source table is read only.
- Namespace catalog integrations: directory, Lance Namespace REST, AWS Glue Data Catalog, Iceberg REST, Apache Polaris and Unity Catalog, with credential bearing config values redacted everywhere they are read back.
- Manifest freshness driven by the node holding the shard: the table's latest manifest is checked at `lance.namespace.poll_cadence` (default 10s), the reader is swapped without a shard close, and the mapping is updated only when the schema drifted (renamed, recast or dropped columns are handled, and overrides follow a renamed column).
- Native memory admission control: before a scan or index load starts, the executor estimates what it makes Lance allocate and refuses the request with 429 `circuit_breaking_exception` when the node's available memory cannot hold it, instead of letting the kernel's OOM killer end the node. Lance's index and metadata caches and the off heap column store are bounded by `lance.native_memory.limit`.
- Fragment pruning by Lance zone map statistics before the scan, and per table `storage_options` (S3, GCS, Azure) so one JVM addresses several buckets with different credentials.
- Query plan visibility through `GET /<index>/_lance/explain`: the logical and physical plan, the cost the planner charged each operator, the traits the request demanded, the plan shipped to the data nodes and the refinements a data node could still apply.

[docs/features.md](docs/features.md) is the full feature reference and [docs/limitations.md](docs/limitations.md) lists the known limitations and the request shapes the plugin refuses.

## Requirements

- OpenSearch 3.8.0. An OpenSearch plugin zip is built against one OpenSearch version and the node refuses any other; [DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md#building-against-another-opensearch-version) describes building against another version.
- JDK 21.
- macOS/aarch64, linux/x86_64 or linux/aarch64. Lance's Rust native library ships in `org.lance:lance-core` for these platforms; Windows is not supported.
- An S3 compatible object store, GCS or Azure Blob Storage for tables that do not live on a filesystem every data node can read (optional).

## Install

Build the plugin zip (or take it from a release once one is published, see [Releases](#releases)):

```
./gradlew build
```

The artifact is `build/distributions/opensearch-lance-0.1.0.zip`. Install it into every node of the cluster with the standard plugin tool:

```
bin/opensearch-plugin install file:///absolute/path/to/opensearch-lance-0.1.0.zip
```

Two JVM options are required on every node, in `config/jvm.options` or `OPENSEARCH_JAVA_OPTS`: `--add-opens=java.base/java.nio=ALL-UNNAMED` for Arrow's C data interface and `-Darrow.allocation.manager.type=Unsafe` for the Arrow allocator that works under OpenSearch's Netty configuration. The Docker and native install paths, with the complete commands, are in [docs/getting-started.md](docs/getting-started.md#2-install-into-opensearch).

## Getting started

[docs/getting-started.md](docs/getting-started.md) is the end to end walkthrough: build and install the plugin, prepare a Lance table with a short Python script (locally or on S3), register a namespace, run every query shape, and read the explain endpoint. [docs/features.md](docs/features.md) is the reference to consult once the walkthrough has answered its first queries.

## Documentation

- [docs/getting-started.md](docs/getting-started.md), build, install, prepare a table, run the query shapes.
- [docs/features.md](docs/features.md), feature reference by concern: attach and namespace surface, query shapes, aggregations, query plan, freshness, caches, memory bounds.
- [docs/limitations.md](docs/limitations.md), known limitations and refused shapes.
- [docs/query-plan.md](docs/query-plan.md), the explain endpoint field by field, the physical operators, the cost model, refinements, traits and the wire format.
- [docs/architecture.md](docs/architecture.md), how the plugin is put together: components, request paths, planner design, memory, follow forward.
- [docs/dependencies.md](docs/dependencies.md), what the plugin zip bundles and why, the third party audit and the security policy.
- [docs/design/namespace-freshness.md](docs/design/namespace-freshness.md), where the mapping is kept in step with the table and why that work runs on the shard's node.
- [CHANGELOG.md](CHANGELOG.md), release notes; unreleased entries are fragments under [changelog/unreleased/](changelog/unreleased/README.md).

## Releases

The plugin is at version 0.1.0 and no release has been published yet. Until the first minor release every release is tagged from `main`; the release zip `opensearch-lance-<version>.zip` will be attached to the GitHub release and the release notes kept under [release-notes/](release-notes/). [RELEASING.md](RELEASING.md) describes the branching, the changelog assembly and the release steps.

## Contributing

Contributions of every kind are welcome: bug reports with the Arrow schema of the table that misbehaved, feature requests, documentation fixes and code. [CONTRIBUTING.md](CONTRIBUTING.md) explains how to open an issue, the pull request process, the changelog fragment every pull request adds, and the Developer Certificate of Origin: every commit must be signed off (`git commit -s`) under your real name. The [Developer Guide](DEVELOPER_GUIDE.md) covers the build, the precommit checks the CI runs, running one test class and debugging. The project is governed by its [maintainers](MAINTAINERS.md) and [admins](ADMINS.md) under the [opensearch-project responsibilities](https://github.com/opensearch-project/.github/blob/main/RESPONSIBILITIES.md).

Design questions belong on the RFC thread ([opensearch-project/OpenSearch#22643](https://github.com/opensearch-project/OpenSearch/issues/22643)).

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

## Code of Conduct

This project has adopted the [Amazon Open Source Code of Conduct](CODE_OF_CONDUCT.md). For more information see the [Code of Conduct FAQ](https://aws.github.io/code-of-conduct-faq), or contact [opensource-codeofconduct@amazon.com](mailto:opensource-codeofconduct@amazon.com) with any additional questions or comments.

## Security

If you discover a potential security issue in this project we ask that you notify OpenSearch Security directly via email to security@opensearch.org. Please do **not** create a public GitHub issue. See [SECURITY.md](SECURITY.md).

## License

This project is licensed under the [Apache v2.0 License](LICENSE.txt).

## Copyright

Copyright OpenSearch Contributors. See [NOTICE](NOTICE.txt) for details.
