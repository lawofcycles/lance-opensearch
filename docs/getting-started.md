# Getting started

Walkthrough for building the plugin, installing it into OpenSearch, and driving it through the four query shapes it supports (match, GET, vector kNN, aggregation). Aimed at people evaluating the plugin against their own Lance tables and at reviewers who want to reproduce the behaviour claimed in the RFC.

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

`test` runs the 36 unit tests. `integTest` boots a single-node OpenSearch test cluster with the plugin installed and runs 9 REST tests (route smoke checks plus end-to-end attach → match / GET / kNN over a Lance table written from Java).

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
    "rating": [(i % 5) + 1 for i in range(rows)],
    "embedding": [[float(i)] + [0.0] * 7 for i in range(rows)],
}

table = pa.table(data, schema=schema)
dataset = lance.write_dataset(table, "/absolute/path/to/tables/demo.lance", mode="create")
dataset.create_scalar_index("body", index_type="INVERTED")
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

## 6. Refresh behaviour when Lance moves forward

If you rewrite the table externally (Python `dataset.append`, `dataset.update`, `merge_insert`, or a Ray / Spark writer), the poll picks up the new manifest version within one cadence period and issues an internal refresh. Queries reflect the new data after the next poll fires. No `_refresh`, `_close`, or shard reallocation is needed.

Force a faster poll by lowering `lance.namespace.poll_cadence` (node-level setting, minimum 1s) in `opensearch.yml`:

```
lance.namespace.poll_cadence: 1s
```

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
