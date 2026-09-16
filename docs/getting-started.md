# Getting started

Walkthrough for building the plugin, installing it into OpenSearch, and driving it through the query shapes it supports (match, phrase, multi-field match, score-composing boost / bool, GET, vector kNN, aggregation). Aimed at people evaluating the plugin against their own Lance tables and at reviewers who want to reproduce the behaviour claimed in the RFC.

## Prerequisites

- JDK 21
- Docker (only for the Docker install path; native install has no Docker requirement)
- Python 3.10+ with `pip install pylance` (only for the sample-table script; skip if you already have a Lance table)

`JAVA_HOME` should point at a JDK 21 install for the build. Every shell snippet below assumes that is done.

## 1. Build the plugin

```
cd lance-opensearch
./gradlew build
```

`build/distributions/opensearch-lance-0.1.0.zip` is the plugin artifact. It is about 280 MB; the bulk is `org.lance:lance-core:11.0.0`, which ships Lance's Rust native library.

If you only want to check tests pass:

```
./gradlew test integTest
```

`test` runs the unit tests. `integTest` boots a single-node OpenSearch test cluster with the plugin installed and runs the REST integration suite (route smoke checks plus end-to-end attach → match / GET / kNN / phrase / fuzziness / multi_match / boost / bool over a Lance table written from Java).

## 2. Install into OpenSearch

Two paths are supported. Pick one.

### 2a. Docker (single node)

The command below installs the plugin into a fresh OpenSearch 3.8.0 image and starts it on port 9200. `--add-opens=java.base/java.nio=ALL-UNNAMED` is required for Arrow's C data interface; `-Darrow.allocation.manager.type=Unsafe` picks the Arrow allocator compatible with OpenSearch's Netty configuration.

```
docker run -d --name lance-opensearch -p 9200:9200 \
  -e discovery.type=single-node \
  -e DISABLE_SECURITY_PLUGIN=true \
  -e DISABLE_INSTALL_DEMO_CONFIG=true \
  -e 'OPENSEARCH_JAVA_OPTS=-Xms1g -Xmx1g --add-opens=java.base/java.nio=ALL-UNNAMED -Darrow.allocation.manager.type=Unsafe' \
  -v "$(pwd)/build/distributions/opensearch-lance-0.1.0.zip:/tmp/plugin.zip:ro" \
  -v /absolute/path/to/your/tables:/tables \
  --entrypoint bash opensearchproject/opensearch:3.8.0 \
  -c "bin/opensearch-plugin install --batch file:///tmp/plugin.zip && ./opensearch-docker-entrypoint.sh opensearch"
```

Replace `/absolute/path/to/your/tables` with the directory that holds your `*.lance` tables. Wait for OpenSearch to come up, then confirm the plugin loaded:

```
curl -s http://localhost:9200/_cat/plugins?v
```

`opensearch-lance` should appear in the plugin list.

### 2b. Native install

Install into an existing OpenSearch 3.8.0 distribution:

```
bin/opensearch-plugin install file:///absolute/path/to/opensearch-lance-0.1.0.zip
```

Then add the required JVM options to `config/jvm.options` (or set them via `OPENSEARCH_JAVA_OPTS`):

```
--add-opens=java.base/java.nio=ALL-UNNAMED
-Darrow.allocation.manager.type=Unsafe
```

Start OpenSearch normally. Confirm the plugin loaded with the same `_cat/plugins` call.

## 3. Prepare a Lance table

You need at least one Lance table on a path the OpenSearch process can read. Two options:

### Option A: use your own table

Any Lance table works. The plugin derives the mapping from the Arrow schema. Supported column types today: int32/int64, boolean, date/timestamp, string (mapped to `lance_text` when the column has a Lance FTS index, otherwise `keyword`), fixed-size list of float (mapped to `knn_vector`), list of string (multi-valued `keyword`), and binary.

Place the table under the directory you mounted in step 2. The rest of this walkthrough assumes it is at `/tables/demo.lance` and has these columns:

- `id: int32` — used as the primary key
- `body: string` with a Lance FTS index — matched by the `match` query
- `title: string` with a Lance FTS index — used together with `body` by the `lance_multi_match` / `lance_fts_boost` / `lance_fts_bool` examples
- `embedding: fixed_size_list<float>[8]` — used by the `lance_knn` query
- `rating: int32` — used by the aggregation example

### Option B: create a sample table with Python

If you do not have a Lance table on hand, this script builds one that matches the walkthrough's schema (16 rows, deterministic content). Run it against the same directory you mounted in step 2.

```python
# save as make_demo_table.py; requires `pip install pylance pyarrow`
import pyarrow as pa
import lance

schema = pa.schema([
    pa.field("id", pa.int32()),
    pa.field("body", pa.string()),
    pa.field("title", pa.string()),
    pa.field("rating", pa.int32()),
    pa.field("embedding", pa.list_(pa.float32(), 8)),
])

rows = 16
data = {
    "id": list(range(rows)),
    "body": [
        f"hello lance {i}" if i % 2 == 0 else f"quick brown fox {i}"
        for i in range(rows)
    ],
    "title": [
        f"sunny morning {i}" if i % 2 == 0 else f"cloudy morning {i}"
        for i in range(rows)
    ],
    "rating": [(i % 5) + 1 for i in range(rows)],
    "embedding": [[float(i)] + [0.0] * 7 for i in range(rows)],
}

table = pa.table(data, schema=schema)
dataset = lance.write_dataset(table, "/absolute/path/to/tables/demo.lance", mode="create")
dataset.create_scalar_index("body", index_type="INVERTED")
dataset.create_scalar_index("title", index_type="INVERTED")
print(f"wrote {dataset.count_rows()} rows to {dataset.uri}")
```

Run it once. The plugin will pick the table up on its next poll cycle.

## 4. Register the namespace

Once per namespace path. The plugin polls the directory every ten seconds (configurable via `lance.namespace.poll_cadence`) and auto-surfaces every `*.lance` directory found underneath as an OpenSearch index.

```
curl -X POST http://localhost:9200/_lance/namespace \
  -H 'Content-Type: application/json' \
  -d '{"path":"/tables"}'
```

List registered namespaces:

```
curl -s http://localhost:9200/_lance/namespace
```

Wait for the poll cycle to fire (up to ten seconds), then confirm the index appeared:

```
curl -s http://localhost:9200/_cat/indices?v
```

The index is named after the `*.lance` directory (in this walkthrough `demo` for `demo.lance`).

If you would rather bypass the namespace poll (for a table not under a registered path, or to override the derived index name), attach a single table by URI:

```
curl -X POST http://localhost:9200/_lance/attach \
  -H 'Content-Type: application/json' \
  -d '{"table":"/tables/demo.lance"}'
```

The call is idempotent; a second attach on the same table returns `already_attached: true`.

### Point at S3, GCS, or Azure with storage_options

For tables that live in an object store, add a `storage_options` map on either the attach body or the namespace body. Keys follow Lance's Rust `object_store` names, so what you write is exactly what Lance receives.

```
curl -X POST http://localhost:9200/_lance/attach \
  -H 'Content-Type: application/json' \
  -d '{
        "table": "s3://my-bucket/tables/demo.lance",
        "storage_options": {
          "aws_region": "us-east-1",
          "aws_access_key_id": "AKIA...",
          "aws_secret_access_key": "..."
        }
      }'
```

The same shape works on `POST /_lance/namespace`; every table auto-surfaced under that namespace inherits the map. Common keys:

| Provider | Keys |
|---|---|
| AWS S3 | `aws_region`, `aws_endpoint`, `aws_access_key_id`, `aws_secret_access_key`, `aws_session_token`, `aws_virtual_hosted_style_request`, `allow_http` |
| GCS | `gcs_bucket`, `gcs_service_account_key`, `gcs_application_credentials` |
| Azure | `azure_storage_account_name`, `azure_storage_account_key`, `azure_storage_sas_token` |

Values must be strings. The plugin does not enumerate a fixed allowlist; whatever keys Lance's Rust `object_store` recognises for the URI scheme reach it verbatim. When `storage_options` is omitted, Lance falls back to its normal environment-variable path (`AWS_*` / `GCS_*` / `AZURE_*`).

Options are persisted as `index.lance.storage_options.<key>` on the created index, so a single node can address two buckets with different credentials at the same time. They are stored in plain index settings today; treat them the way you would treat any other index setting.

## 5. Verify: run the four query shapes

Each command below assumes the index name `demo` from step 4.

### Full-text search (match)

Uses the Lance INVERTED index on `body`. Half the rows say `hello lance <i>`, half say `quick brown fox <i>`.

```
curl -s -X POST 'http://localhost:9200/demo/_search?size=3' \
  -H 'Content-Type: application/json' \
  -d '{"query":{"match":{"body":"hello"}}}'
```

Expected `hits.total.value`: 8 (every even row).

### AND / OR match (lance_match)

The stock `match` above passes the whole query text through Lance as one token, so `operator: and` on OpenSearch's built-in `match` is ignored. Use `lance_match` to push AND / OR control into Lance's own FTS engine:

```
curl -s -X POST 'http://localhost:9200/demo/_search?size=3' \
  -H 'Content-Type: application/json' \
  -d '{"query":{"lance_match":{"field":"body","query":"hello lance","operator":"and"}}}'
```

Expected `hits.total.value`: 8 (every even row contains both `hello` and `lance`).

Swapping the query text to `"hello quick"` returns 16 hits with the default operator (OR) and 0 hits with `operator: and`, because no row contains both `hello` and `quick`.

### Phrase order (lance_match_phrase)

The stock `match_phrase` collapses to a single token for the same reason and ignores order. Use `lance_match_phrase`:

```
curl -s -X POST 'http://localhost:9200/demo/_search?size=3' \
  -H 'Content-Type: application/json' \
  -d '{"query":{"lance_match_phrase":{"field":"body","query":"hello lance"}}}'
```

Returns 8 hits. Reversing the phrase to `"lance hello"` returns 0. Non-zero slop lets tokens sit further apart: `{"field":"body","query":"quick fox","slop":1}` matches every `quick brown fox <i>` because `brown` sits one position between `quick` and `fox`.

### Fuzziness (lance_match)

`lance_match` accepts `fuzziness` (non-negative integer edit distance), `prefix_length`, and `max_expansions`. OpenSearch's `AUTO` fuzziness is not supported because Lance takes an explicit integer.

```
curl -s -X POST 'http://localhost:9200/demo/_search?size=3' \
  -H 'Content-Type: application/json' \
  -d '{"query":{"lance_match":{"field":"body","query":"helo","fuzziness":1}}}'
```

Expected `hits.total.value`: 8. `helo` is edit distance 1 from `hello`, so the eight even rows still match. Without `fuzziness` the same query returns 0.

### Multi-field match (lance_multi_match)

Push a single query text across multiple `lance_text` fields with optional per-field boosts. Even rows have `title = "sunny morning i"`; odd rows have `title = "cloudy morning i"`.

```
curl -s -X POST 'http://localhost:9200/demo/_search?size=3' \
  -H 'Content-Type: application/json' \
  -d '{
        "query": {
          "lance_multi_match": {
            "fields": ["body","title"],
            "query": "hello cloudy",
            "boosts": [1.0, 2.0]
          }
        }
      }'
```

Expected `hits.total.value`: 16. `hello` hits every even row on `body`; `cloudy` hits every odd row on `title`; the OR default unions them. Restricting to `["body"]` drops the odd-row matches; adding `"operator":"and"` returns 0 hits because no row contains both terms.

### Score composition (lance_fts_boost)

Compose two Lance FTS clauses so hits are defined by `positive` and hits that also match `negative` get their score multiplied by `negative_boost`. All hits still come from the positive set.

```
curl -s -X POST 'http://localhost:9200/demo/_search?size=3' \
  -H 'Content-Type: application/json' \
  -d '{
        "query": {
          "lance_fts_boost": {
            "positive": {"lance_match": {"field": "body", "query": "hello"}},
            "negative": {"lance_match": {"field": "body", "query": "lance"}},
            "negative_boost": 0.1
          }
        }
      }'
```

Expected `hits.total.value`: 8. The positive `hello` matches every even row; every even row also matches the negative `lance`, so each hit's score is multiplied by `0.1`. Compare `_score` against the plain `lance_match {"field":"body","query":"hello"}` to see the reduction. Both `positive` and `negative` must themselves be Lance FTS DSLs (`lance_match`, `lance_match_phrase`, `lance_multi_match`, or a nested `lance_fts_boost` / `lance_fts_bool`); passing a stock `match` returns 400.

### Bool composition (lance_fts_bool)

Compose FTS clauses using `must` / `should` / `must_not` lists. Every clause must itself be a Lance FTS DSL.

```
curl -s -X POST 'http://localhost:9200/demo/_search?size=3' \
  -H 'Content-Type: application/json' \
  -d '{
        "query": {
          "lance_fts_bool": {
            "must": [
              {"lance_match": {"field": "body", "query": "hello"}}
            ],
            "must_not": [
              {"lance_match": {"field": "body", "query": "lance"}}
            ]
          }
        }
      }'
```

Expected `hits.total.value`: 0. `must` on `body:hello` selects the eight even rows; `must_not` on `body:lance` removes every row that also contains `lance`, which is all of them. Replace the `must_not` with a `should` on `title:sunny` to see the intersection (`must ∩ should` = eight even rows) and score composition on Lance's side.

### Primary key lookup

```
curl -s http://localhost:9200/demo/_doc/3
```

Expected `_source.body`: `quick brown fox 3` (odd `id`).

### Vector nearest neighbour (lance_knn)

Each row `i` lives at coordinate `(i, 0, 0, ..., 0)`. The query vector `(2.4, 0, ...)` is nearest to row 2, then row 3.

```
curl -s -X POST 'http://localhost:9200/demo/_search?size=2' \
  -H 'Content-Type: application/json' \
  -d '{
        "query": {
          "lance_knn": {
            "field":"embedding",
            "vector":[2.4,0,0,0,0,0,0,0],
            "k":2
          }
        }
      }'
```

Expected `hits.hits[0]._source.id = 2`, `hits.hits[1]._source.id = 3`.

Scores are `boost / (1 + distance)`. Compare within one query, not across queries.

`lance_knn` accepts an inner `filter` clause that Lance evaluates before applying the K-nearest cutoff (a pre-filter). Any `bool` combination of `match_all`, `term`, `terms`, `exists`, and `range` clauses works; anything else returns 400.

```
curl -s -X POST 'http://localhost:9200/demo/_search?size=2' \
  -H 'Content-Type: application/json' \
  -d '{
        "query": {
          "lance_knn": {
            "field":"embedding",
            "vector":[2.4,0,0,0,0,0,0,0],
            "k":2,
            "filter":{"range":{"id":{"gte":10}}}
          }
        }
      }'
```

Expected `hits.hits[0]._source.id = 10`, `hits.hits[1]._source.id = 11` (`k=2` stays populated because the filter is applied before the cutoff).

### Aggregation combined with bool

```
curl -s -X POST 'http://localhost:9200/demo/_search?size=0' \
  -H 'Content-Type: application/json' \
  -d '{
        "query": {"bool":{"must":[{"match":{"body":"lance"}}]}},
        "aggs":  {"rating":{"terms":{"field":"rating"}}}
      }'
```

Expected: one bucket per distinct `rating` value seen in matching rows.

### Hybrid shape (bool.should combining FTS and vector)

FTS and vector sub-queries compose inside `bool.should`. Row `i` has body-token "hello" only on even `i`, and its vector coordinate on the first axis is `i`. `lance_match` on "hello" hits every even row, `lance_knn` near `(0.5, 0, ..., 0)` with `k=2` hits rows 0 and 1. The union has 9 rows; row 0 satisfies both clauses and sums to the highest `_score`.

```
curl -s -X POST 'http://localhost:9200/demo/_search?size=16' \
  -H 'Content-Type: application/json' \
  -d '{
        "query": {
          "bool": {
            "should": [
              { "lance_match": { "field": "body", "query": "hello" } },
              { "lance_knn": {
                  "field": "embedding",
                  "vector": [0.5, 0, 0, 0, 0, 0, 0, 0],
                  "k": 2
              }}
            ]
          }
        }
      }'
```

Expected `hits.total.value`: 9, with `hits[0]._source.id = 0`.

The same shape works with stock `match` on `body` in place of `lance_match`: on a `lance_text` field, `match` is rewritten to Lance FTS, so per-shard composition is identical.

For OpenSearch's dedicated `hybrid` query (per-sub-query top-K with a score-normalising search pipeline), install the [`neural-search`](https://opensearch.org/docs/latest/search-plugins/hybrid-search/) plugin alongside this one and follow its docs. Per-shard sub-query execution goes through the same Lucene `createWeight` / `Scorer` path the `bool.should` example above exercises.

## 6. Refresh behaviour when Lance moves forward

If you rewrite the table externally (Python `dataset.append`, `dataset.update`, `merge_insert`, or a Ray / Spark writer), the poll picks up the new manifest version within one cadence period and issues an internal refresh. Queries reflect the new data after the next poll fires. No `_refresh`, `_close`, or shard reallocation is needed.

Force a faster poll by lowering `lance.namespace.poll_cadence` (node-level setting, minimum 1s) in `opensearch.yml`:

```
lance.namespace.poll_cadence: 1s
```

### How the plugin thinks about indexes

Indexes on the Lance table (FTS, scalar, vector) are treated as an external concern. The recommended flow is to build them from the same writer that produced the table, using `dataset.create_index` in Python, the Lance Java SDK, or a Ray / Spark job. The plugin never creates or optimises indexes on its own poll cycle; that principle is what keeps the plugin from writing new versions to the user's Lance table behind their back.

For operators who want to trigger a build from the cluster, `POST /_lance/build_indexes/{index}` is the auxiliary path. It supports two modes.

```
POST /demo/_lance/build_indexes
{
  "columns": ["body"]
}
```

Runs FTS, scalar, or vector index builds for the requested columns.

```
POST /demo/_lance/build_indexes
{
  "optimize": true
}
```

Runs `Dataset.optimizeIndices` so every existing index folds in fragments that appended since the last build. Use this after a batch of appends, or on a schedule, to keep the fraction of uncovered fragments from growing.

### Append visibility

When Lance advances to a new version, the plugin exposes it as soon as the next poll observes the change. The appended fragments do not have to be covered by every existing index first: Lance's own scanner produces a mixed execution plan for FTS and knn (covered fragments use the existing index, uncovered fragments run a flat scan, and the results are unioned by the query engine), so an incremental append never slows down queries hitting the previously-covered fragments. Whenever uncovered fragments accumulate to the point that flat-scan latency becomes noticeable, call `POST /_lance/build_indexes/{index}` with `{"optimize": true}` to fold them into the existing indexes.

The `index.lance.uncovered_fragment_policy` setting still accepts `wait` alongside the default `immediate`. Both values currently expose the new version immediately; `wait` is reserved for a future async-optimize implementation, and setting it today logs an informational message so operators are aware that the plugin does not run auto-optimize.

### Cap Lance's native memory footprint

Lance keeps its inverted-index and metadata caches in native memory, outside the JVM heap. The plugin installs a single Lance `Session` at startup so every table on a node shares the same caches. The upper bound is set by `lance.native_memory.limit`, a node-level setting that accepts either a byte value or a percentage of the memory left after the JVM heap is subtracted from physical memory. The default is `40%`, which scales with instance size and leaves room for the k-NN plugin's own memory budget on nodes that host both plugins.

```
lance.native_memory.limit: 40%      # default; percent of (physical - heap)
lance.native_memory.limit: 10gb     # or an absolute byte value
```

The parsed value is split 6:1 between the index cache and the metadata cache, mirroring Lance's own default ratio. On startup the plugin logs the resolved sizes so the operator can confirm the split, for example `installed shared Lance Session: limit [10gb] -> index cache [8.5gb], metadata cache [1.4gb] (from lance.native_memory.limit [10gb])`. This is a static setting today, so a change requires a rolling restart to take effect.

### Circuit breaker for Lance native memory

The plugin registers a `lance_native` circuit breaker with OpenSearch's standard breaker service, so operators can watch native cache usage through `GET /_nodes/stats/breaker` alongside `fielddata`, `request`, and the other built-in breakers.

```
curl -sS localhost:9200/_nodes/stats/breaker | jq '.nodes[].breakers.lance_native'
```

The breaker's byte limit mirrors `lance.native_memory.limit`, and the plugin samples `Session.sizeBytes()` on a background scheduler (every 5 seconds by default) to keep the breaker's accounting close to the real footprint. FTS and knn query paths call the breaker before starting a native scan; if the usage has caught up to the limit the query is rejected with a `CircuitBreakingException` (HTTP 429), which stays transient because the next polling tick or an LRU eviction will let the next request through.

Two cluster settings tune this:

```
lance.native_memory.circuit_breaker.enabled: true       # default; toggle enforcement
lance.native_memory.circuit_breaker.poll_interval: 5s   # default; how fast the sampler catches up
```

Both are dynamic, so changes take effect without a restart. The byte limit itself is not directly configurable through the breaker; it always follows `lance.native_memory.limit` so operators reason about one number.

## 7. Cleanup and restart

The namespace registry is held in process memory. Restarting OpenSearch clears the registrations, and any Lance-backed indices survive as regular OpenSearch indices without a live sync loop. To resume auto-surface after a restart:

1. Delete any Lance-backed indices you want to re-surface (`curl -X DELETE http://localhost:9200/demo`).
2. `POST /_lance/namespace` again with the same path.

Alternatively, keep the surviving indices and reattach each one explicitly with `POST /_lance/attach` if you only need to reconnect the poll cycle for a specific table.

## 8. Troubleshooting

**`java.lang.OutOfMemoryError: Direct buffer memory`.** Arrow's C data interface allocates off-heap; heavy scans hit the JVM's direct-memory ceiling before the heap. Raise it with `-XX:MaxDirectMemorySize=2g` (tune to workload).

**A query on a mapped column returns zero hits when Python `dataset.to_table()` shows data.** Inspect the OpenSearch mapping (`curl -s http://localhost:9200/<index>/_mapping`). If the column is missing there, its Arrow type is not yet covered by the mapping derivation; open an issue with the schema.

## Next steps

- The plugin's design and the invariants it upholds live in the RFC: [opensearch-project/OpenSearch#22643](https://github.com/opensearch-project/OpenSearch/issues/22643).
- Known unfinished work is tracked as issues in this repository. See the README's "What is not yet implemented" list for the shape of the gaps.
