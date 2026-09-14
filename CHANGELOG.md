# CHANGELOG

Inspired by [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Features

- Distributed search over Lance tables. Each Lance fragment is exposed as a Lucene leaf inside a shard, and full text and vector queries are rewritten to native Lance execution.
- `POST /_lance/namespace` registers a filesystem path as a Lance namespace and surfaces every `*.lance` table under it as an OpenSearch index. Uses `DirectoryNamespace` from `lance-namespace-core`.
- `POST /_lance/attach` attaches a single Lance table URI. Idempotent; a repeated call returns `already_attached: true`.
- `lance.namespace.poll_cadence` cluster setting controls how often each registered namespace is polled for new or advanced tables. Default 10s, minimum 1s.
- `index.lance.uncovered_fragment_policy` index setting (`wait` / `immediate`) controls the visibility of rows in fragments not yet covered by the current manifest.
- `index.lance.primary_key_field` index setting names the Lance column that answers `GET /<index>/_doc/<id>` lookups.
- Mapping derivation from the Lance Arrow schema for boolean, date/timestamp (all Arrow date/timestamp variants normalized to epoch millis), integer, `keyword` for Utf8 columns without an FTS index, `lance_text` for Utf8 columns with an FTS index, multi-valued `keyword` for list-of-Utf8, `knn_vector` for fixed-size-list-of-float, and `binary` for Arrow binary and large binary columns.
- Manifest version advance is picked up by a `ReferenceManager` swap under the read-only engine. No shard close, no reallocation.
- `lance_knn` DSL query issues one Lance nearest scan per shard and dispatches hits to the fragments they belong to. The coordinator merge reconstructs the global top-k.
- `GET /<index>/_doc/<id>` maps to `Dataset.newScan` with a `<field> = <key>` filter restricted to the shard's fragments. A Lance scalar index answers in log time; without one, the same call falls back to a filtered scan.
- `_source` and `_id` come from stored fields synthesised on the fly from the Lance row.
- Auto-build of missing FTS / scalar / vector indexes for tables at or under `lance.builder.max_rows` rows (default 1,000,000). `POST /_lance/build_indexes/{index}` triggers manual builds with optional `columns` and `fragment_ids` for larger tables or incremental builds.

### Infrastructure

- Bundle Lance's runtime dependencies (Arrow, Netty, questdb JAR JNI loader, Lance native library for macOS/aarch64, linux/x86_64, and linux/aarch64).
- opensearch-project standard build (`opensearch.opensearchplugin` + `opensearch.pluginzip`) with the full precommit chain (dependencyLicenses, thirdPartyAudit, licenseHeaders, testingConventions, forbiddenApis, jarHell, filepermissions, forbiddenPatterns, validatePluginZipPom, validatePom).

### Documentation

- README covers install, JVM options (`--add-opens=java.base/java.nio=ALL-UNNAMED`, `-Darrow.allocation.manager.type=Unsafe`), a Try it walkthrough for match / GET / knn against a demo Lance table, shard selection, and how to combine the plugin's index builder with external tools such as `lance-ray`.

### Maintenance

- Upgrade `org.lance:lance-core` to `11.0.0` (from `10.1.0-beta.2`). `lance-namespace-core` and `lance-namespace-apache-client` stay at `0.7.7`, matching lance-core 11.0.0's pinned transitive.
