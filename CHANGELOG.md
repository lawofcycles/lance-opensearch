# CHANGELOG

Inspired by [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Features

- Distributed search over Lance tables. Each Lance fragment is exposed as a Lucene leaf inside a shard, and full text and vector queries are rewritten to native Lance execution.
- `POST /_lance/namespace` registers a filesystem path as a Lance namespace and surfaces every `*.lance` table under it as an OpenSearch index. Uses `DirectoryNamespace` from `lance-namespace-core`.
- `POST /_lance/attach` attaches a single Lance table URI. Idempotent; a repeated call returns `already_attached: true`. Attach body accepts an optional `multi_fields` clause that declares a keyword sub-field on a Utf8 base column (`{"body": {"raw": {"type": "keyword"}}}`), a `"version": N` clause that pins a readonly snapshot, and a `storage_options` map for per-table object-store credentials.
- `POST /_lance/namespace/tables {"path": "..."}` previews the tables the poll would surface from a registered namespace. Returns 404 for unregistered paths so the caller can distinguish "not registered" from "registered but empty".
- `DELETE /_lance/namespace {"path": "..."}` stops polling a namespace. Already-surfaced indexes are left in place.
- `lance.namespace.poll_cadence` cluster setting controls how often each registered namespace is polled for new or advanced tables. Default 10s, minimum 1s.
- `lance.namespace.resurface_guard_grace` node setting (dynamic, default 1h) holds a deletion-tombstone for a Lance-backed index that was removed via `DELETE /{index}`, so the poll cycle honours the operator's intent for that window. Zero disables the guard.
- `index.lance.uncovered_fragment_policy` index setting (`wait` / `immediate`) controls the visibility of rows in fragments not yet covered by the current manifest.
- `index.lance.primary_key_field` index setting names the Lance column that answers `GET /<index>/_doc/<id>` lookups. `index.lance.primary_key_type` (`long`, `unsigned_long`, or `keyword`) carries the Arrow type family: signed integer PKs (up to 64 bits) resolve through `Long.parseLong`, UInt64 PKs resolve through `BigInteger` with a wide-decimal Lance filter and the column also surfaces as an `unsigned_long` mapping for term / range / sort / aggregation, and Utf8 PKs resolve through a SQL-quoted Lance filter with single-quote doubling for injection safety.
- `index.lance.multi_fields` index setting persists the multi-fields spec captured at attach time so the engine can rehydrate keyword sub-fields on shard open. Sub-field query resolution goes through Lucene doc values on the base column, no data duplication.
- Mapping derivation from the Lance Arrow schema for boolean, date/timestamp (all Arrow date/timestamp variants normalized to epoch millis), integer, `keyword` for Utf8 columns without an FTS index, `lance_text` for Utf8 columns with an FTS index, multi-valued `keyword` for list-of-Utf8, `knn_vector` for fixed-size-list-of-float, `binary` for Arrow binary and large binary columns, and a `fields` block per base column when the attach body carries a multi-fields declaration.
- Manifest version advance is picked up by a `ReferenceManager` swap under the read-only engine. No shard close, no reallocation.
- `lance_knn` DSL query issues one Lance nearest scan per shard and dispatches hits to the fragments they belong to. The coordinator merge reconstructs the global top-k.
- `GET /<index>/_doc/<id>` maps to `Dataset.newScan` with a `<field> = <literal>` filter restricted to the shard's fragments. Filter literal shape (integer or single-quoted string) is picked from `index.lance.primary_key_type`. A Lance scalar index answers in log time; without one, the same call falls back to a filtered scan.
- `_source` and `_id` come from stored fields synthesised on the fly from the Lance row. Tables without a declared primary key still emit unique `_id` values through a `<fragment>-<offset>` fallback so sort-by-`_id` and `_mget` dedup stay correct.
- Auto-build of missing FTS / scalar / vector indexes for tables at or under `lance.builder.max_rows` rows (default 1,000,000). `POST /_lance/build_indexes/{index}` triggers manual builds with optional `columns` and `fragment_ids` for larger tables or incremental builds.
- Fragment path is the single search implementation. Coordinator response envelope tracks per-hit scores (Lucene's 4 / 5 argument `search` / `searchAfter` overloads) so `sort` combined with `track_scores: true` preserves `_score`, and `max_score` reports the largest per-hit score in the paged window. Attach and namespace surface persist all metadata the fragment path and engine path need on shard open.

### Infrastructure

- Bundle Lance's runtime dependencies (Arrow, Netty, questdb JAR JNI loader, Lance native library for macOS/aarch64, linux/x86_64, and linux/aarch64).
- opensearch-project standard build (`opensearch.opensearchplugin` + `opensearch.pluginzip`) with the full precommit chain (dependencyLicenses, thirdPartyAudit, licenseHeaders, testingConventions, forbiddenApis, jarHell, filepermissions, forbiddenPatterns, validatePluginZipPom, validatePom).
- Full test / integTest / multiNodeIntegTest suite (200+ unit, 70+ integTest, 2 multi-node).

### Documentation

- README trimmed to overview, status, build, and pointers into `docs/`. Feature reference lives in `docs/features.md`, known limitations in `docs/limitations.md`, and the walkthrough in `docs/getting-started.md`.

### Maintenance

- Upgrade `org.lance:lance-core` to `11.0.0` (from `10.1.0-beta.2`). `lance-namespace-core` and `lance-namespace-apache-client` stay at `0.7.7`, matching lance-core 11.0.0's pinned transitive.
