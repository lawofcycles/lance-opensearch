# Getting started

Walkthrough for building the plugin, installing it into OpenSearch, and driving it through the query shapes it supports (`match`, `match_phrase`, `multi_match`, `bool`, the `lance_*` full text queries, GET by primary key, `lance_knn`, aggregations, and the explain endpoint that shows where each request runs). Aimed at people evaluating the plugin against their own Lance tables and at reviewers who want to reproduce the behaviour claimed in the RFC.

## Prerequisites

- JDK 21
- Docker (only for the Docker install path; native install has no Docker requirement)
- Python 3.10+ with `pip install pylance` (only for the sample-table script; skip if you already have a Lance table)
- AWS credentials and an S3 bucket you can write to (only for the S3 sample-table path, Option C in step 3)

`JAVA_HOME` should point at a JDK 21 install for the build. Every shell snippet below assumes that is done.

## 1. Build the plugin

```
cd lance-opensearch
./gradlew build
```

`build/distributions/opensearch-lance-0.1.0.zip` is the plugin artifact. It is about 280 MB; the bulk is `org.lance:lance-core:12.0.0`, which ships Lance's Rust native library.

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

You need at least one Lance table on a path or object store URI the OpenSearch process can read. Three options:

### Option A: use your own table

Any Lance table works. The plugin derives the mapping from the Arrow schema. Supported column types today: int8 to int64 (`byte` to `long`), float32/float64, boolean, date/timestamp (`date`), string (mapped to `lance_text` when the column has a Lance FTS index, otherwise `keyword`), fixed-size list of float32 (`lance_vector`, the type `lance_knn` queries), list of string (multi-valued `keyword`), struct (`object`), list of struct (`nested`), and binary. The full table is in [features.md](features.md#mapping-type-coverage).

Place the table under the directory you mounted in step 2. The rest of this walkthrough assumes it is at `/tables/demo.lance` and has these columns:

- `id: int32` — used as the primary key; the column carries `lance-schema:unenforced-primary-key` field metadata and is non-nullable (without that metadata the table attaches fine but `GET /<index>/_doc/<id>` returns 404)
- `body: string` with a Lance FTS index — mapped as `lance_text`; matched by the `match` / `match_phrase` and `lance_match` / `lance_match_phrase` examples
- `title: string` with a Lance FTS index — mapped as `lance_text`; used together with `body` by the `multi_match` and `bool` examples and by their `lance_multi_match` / `lance_fts_boost` / `lance_fts_bool` equivalents
- `embedding: fixed_size_list<float>[8]` — mapped as `lance_vector`; used by the `lance_knn` query
- `rating: int32` — mapped as `integer`; used by the `bool.filter` and aggregation examples

Once attached, `curl -s http://localhost:9200/demo/_mapping` shows exactly these types; the mapping is the contract the queries below rely on. Only a `lance_text` field sends `match` and its relatives to the Lance FTS index, so a string column that has no inverted index in the table (mapped as `keyword`) needs one built first (see "How the plugin thinks about indexes" in step 6).

### Option B: create a sample table with Python

If you do not have a Lance table on hand, this script builds one that matches the walkthrough's schema (16 rows, deterministic content). Run it against the same directory you mounted in step 2.

```python
# save as make_demo_table.py; requires `pip install pylance pyarrow`
import pyarrow as pa
import lance

schema = pa.schema([
    # the metadata marks `id` as the table's primary key so `GET /demo/_doc/<id>`
    # works; Lance requires the PK column to be non-nullable
    pa.field("id", pa.int32(), nullable=False,
             metadata={"lance-schema:unenforced-primary-key": "true"}),
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
# with_position stores token positions, which lance_match_phrase needs
# (pylance defaults it to False); title has no phrase example, so it is
# indexed without them
dataset.create_scalar_index("body", index_type="INVERTED", with_position=True)
dataset.create_scalar_index("title", index_type="INVERTED")
print(f"wrote {dataset.count_rows()} rows to {dataset.uri}")
```

Run it once. The plugin will pick the table up on its next poll cycle.

### Option C: create the sample table on S3

If your Lance tables live in an object store — written there by Ray, Spark, or any other Lance writer — the plugin attaches them in place; nothing is copied down to local disk. This option writes the same 16-row sample table straight to an S3 bucket so you can walk that flow end to end. The rest of the walkthrough is identical to Options A and B: the only difference is that the attach body in step 4 carries the `s3://` URI and a `storage_options` map instead of a filesystem path.

You need AWS credentials with write access to a bucket (exported as the standard `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` environment variables, plus `AWS_SESSION_TOKEN` when they are temporary), the bucket's region in `AWS_REGION`, and the same Python environment as Option B (`pip install pylance pyarrow`). If your credentials live in an AWS CLI profile, `eval "$(aws configure export-credentials --profile <profile> --format env)"` exports them. The `storage_options` keys below follow Lance's Rust `object_store` names — the same names step 4 uses on the attach body.

```python
# save as make_demo_table_s3.py; requires `pip install pylance pyarrow`
import os
import pyarrow as pa
import lance

schema = pa.schema([
    # same primary key declaration as Option B
    pa.field("id", pa.int32(), nullable=False,
             metadata={"lance-schema:unenforced-primary-key": "true"}),
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

storage_options = {
    "aws_region": os.environ["AWS_REGION"],
    "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
    "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
}
if "AWS_SESSION_TOKEN" in os.environ:
    storage_options["aws_session_token"] = os.environ["AWS_SESSION_TOKEN"]

table = pa.table(data, schema=schema)
dataset = lance.write_dataset(
    table,
    "s3://<bucket>/tables/demo.lance",
    mode="create",
    storage_options=storage_options,
)
# with_position stores token positions, which lance_match_phrase needs
# (pylance defaults it to False); title has no phrase example, so it is
# indexed without them
dataset.create_scalar_index("body", index_type="INVERTED", with_position=True)
dataset.create_scalar_index("title", index_type="INVERTED")
print(f"wrote {dataset.count_rows()} rows to {dataset.uri}")
```

Replace `<bucket>` with your bucket name and run it once. Confirm the table landed:

```
aws s3 ls s3://<bucket>/tables/demo.lance/
```

You should see the Lance table layout — `_indices/`, `_transactions/`, `_versions/`, and `data/` prefixes.

There is no directory to mount into the container and nothing for the namespace poll to watch, so in step 4 skip the namespace registration and attach the table by URI, passing `"table": "s3://<bucket>/tables/demo.lance"` together with the same `storage_options` map you gave the script (see "Point at S3, GCS, or Azure with storage_options" in step 4). From step 5 on, every query shape behaves exactly as it does against the local table.

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

For tables that live in an object store, add a `storage_options` map on either the attach body or the namespace body. If you created the sample table on S3 with Option C in step 3, this is the path that attaches it: put the script's `s3://` URI in `table` and pass the same credential keys you gave the script. Keys follow Lance's Rust `object_store` names, so what you write is exactly what Lance receives.

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

## 5. Verify: run the query shapes

Each command below assumes the index name `demo` from step 4.

Full text queries come in two forms that answer the same. The stock OpenSearch syntax (`match`, `match_phrase`, `multi_match`, and `bool` around them) is the form to start with: on a `lance_text` field the coordinator rewrites each stock clause into the plugin's `lance_*` clause with the same parameters before it plans the request, so the hits and the BM25 scores come from the Lance inverted index, `operator`, `fuzziness`, phrase order, `slop` and field boosts reach Lance, and a `bool` of several clauses with scalar `filter` / `must_not` companions runs as one Lance scan with the filter as a prefilter. The `lance_*` form (`lance_match`, `lance_match_phrase`, `lance_multi_match`, `lance_fts_bool`, `lance_fts_boost`) is the explicit spelling of the same Lance queries; it is what the rewrite produces, what `GET /<index>/_lance/explain` prints under `lance_clause`, and the form to write when a parameter has no stock counterpart (`lance_fts_boost`'s negative clause). Each subsection below shows the stock form first and the explicit form under it. [limitations.md](limitations.md#fts-query-behaviour-on-stock-match--match_phrase--multi_match) has the short list of what the stock form drops.

### Full-text search (match)

Uses the Lance INVERTED index on `body`. Half the rows say `hello lance <i>`, half say `quick brown fox <i>`.

```
curl -s -X POST 'http://localhost:9200/demo/_search?size=3' \
  -H 'Content-Type: application/json' \
  -d '{"query":{"match":{"body":"hello"}}}'
```

Expected `hits.total.value`: 8 (every even row). `_score` is Lance's BM25 for the row.

Lance's own tokenizer splits several words and the default operator ORs the terms: `{"match":{"body":"hello lance"}}` returns 8 and `{"match":{"body":"hello quick"}}` returns 16, because every row contains one of the two words. `operator: and` requires both: `{"match":{"body":{"query":"hello quick","operator":"and"}}}` returns 0 and `{"match":{"body":{"query":"hello lance","operator":"and"}}}` returns 8.

`fuzziness` is an edit distance: `{"match":{"body":{"query":"helo","fuzziness":1}}}` returns 8 (`helo` is one edit from `hello`; without `fuzziness` it returns 0). `AUTO` resolves on the length of the whole query text, 1 for `helo`. `prefix_length` and `max_expansions` reach Lance as well. `minimum_should_match` has no Lance counterpart and is ignored.

#### Explicit form: lance_match

```
curl -s -X POST 'http://localhost:9200/demo/_search?size=3' \
  -H 'Content-Type: application/json' \
  -d '{"query":{"lance_match":{"field":"body","query":"hello lance","operator":"and"}}}'
```

Expected `hits.total.value`: 8, the same hits and scores as the stock `match` with `operator: and`. `lance_match` takes `fuzziness` as a non negative integer only (`AUTO` is not accepted), `prefix_length` and `max_expansions`:

```
curl -s -X POST 'http://localhost:9200/demo/_search?size=3' \
  -H 'Content-Type: application/json' \
  -d '{"query":{"lance_match":{"field":"body","query":"helo","fuzziness":1}}}'
```

Expected `hits.total.value`: 8.

### Phrase (match_phrase)

```
curl -s -X POST 'http://localhost:9200/demo/_search?size=3' \
  -H 'Content-Type: application/json' \
  -d '{"query":{"match_phrase":{"body":"hello lance"}}}'
```

Expected `hits.total.value`: 8. Word order is enforced: `{"match_phrase":{"body":"lance hello"}}` returns 0. Non zero `slop` lets tokens sit further apart: `{"match_phrase":{"body":{"query":"quick fox","slop":1}}}` matches every `quick brown fox <i>` because `brown` sits one position between `quick` and `fox`.

A phrase needs an inverted index that stores token positions (the `with_position=True` argument in the step 3 scripts; `"with_position": true` on `POST /_lance/build_indexes/{index}` when the plugin builds the index). On an index built without positions Lance answers 400 with `position is not found but required for phrase queries`.

#### Explicit form: lance_match_phrase

```
curl -s -X POST 'http://localhost:9200/demo/_search?size=3' \
  -H 'Content-Type: application/json' \
  -d '{"query":{"lance_match_phrase":{"field":"body","query":"hello lance"}}}'
```

Returns the same 8 hits; `{"field":"body","query":"quick fox","slop":1}` is the explicit spelling of the slop example.

### Multi-field match (multi_match)

Even rows have `title = "sunny morning i"`; odd rows have `title = "cloudy morning i"`.

```
curl -s -X POST 'http://localhost:9200/demo/_search?size=3' \
  -H 'Content-Type: application/json' \
  -d '{
        "query": {
          "multi_match": {
            "query": "hello cloudy",
            "fields": ["body", "title^2"]
          }
        }
      }'
```

Expected `hits.total.value`: 16. `hello` hits every even row on `body`; `cloudy` hits every odd row on `title`; the two are unioned. The default type `best_fields` becomes one Lance `MultiMatchQuery` over both columns that keeps the best column's score, with the `^2` boost as that column's factor; `operator` applies inside each column. Restricting to `["body"]` drops the odd row matches; adding `"operator":"and"` returns 0 hits because no row contains both words in one field. The other `multi_match` types (`most_fields`, `cross_fields`, `phrase`, `phrase_prefix`, `bool_prefix`) keep their stock form: one Lance match per field, combined by Lucene.

#### Explicit form: lance_multi_match

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

Expected `hits.total.value`: 16, the same rows and scores as the stock `multi_match` above.

### Composition with bool

A `bool` composes full text clauses with each other and with scalar filters. When its `must` and `should` lists hold full text clauses only and its `filter` holds scalar clauses (`term`, `terms`, `range`, `exists`, `match_all`, `wildcard`, `regexp`, `prefix`, or a `bool` of those), the planner translates the scalar clauses to Lance SQL and fuses the full text clauses into one Lance boolean query, so the whole `bool` runs as one Lance FTS scan with the SQL as a prefilter: Lance evaluates the predicate first (through the column's scalar index when it has one), looks the inverted index up only for the selected rows, and scores the clauses as Lucene's `BooleanQuery` would (`must` clauses intersect and add their scores, `should` clauses add theirs when they match, `must_not` clauses exclude). `rating` is `(i % 5) + 1`, so the even rows carry the ratings 1, 3, 5, 2, 4, 1, 3, 5.

```
curl -s -X POST 'http://localhost:9200/demo/_search?size=3' \
  -H 'Content-Type: application/json' \
  -d '{
        "query": {
          "bool": {
            "must":   [{"match": {"body": "hello"}}],
            "filter": [{"range": {"rating": {"gte": 3}}}]
          }
        }
      }'
```

Expected `hits.total.value`: 5 (rows 2, 4, 8, 12, 14: even, and rating 3 or above). Other compositions on the same table:

- `must: [{"match":{"body":"hello"}}, {"match":{"title":"sunny"}}]` returns 8: both words sit on the even rows, each hit scored by the sum of the two clauses.
- `must: [{"match":{"body":"hello"}}], must_not: [{"match":{"body":"lance"}}]` returns 0: every row with `hello` also has `lance`.
- `should: [{"match":{"body":"hello"}}, {"match":{"title":"cloudy"}}]` returns 16, each row scored by the one clause it matches.

Shapes the planner leaves to Lucene, with the same results: a `should` next to a `filter` without a `must` (the full text clause is optional in Lucene, the filter alone selects the rows), a scalar clause in `must` or `should`, a full text clause in `filter`, a `minimum_should_match`, or a `boost` on the `bool`. Every full text clause then runs as its own Lance scan and Lucene combines them. "Where a request runs" below shows how to tell the two plans apart.

#### Explicit form: lance_fts_bool and lance_fts_boost, composition on Lance's side

`lance_fts_bool` is what the fused stock `bool` becomes: `must` / `should` / `must_not` lists of full text clauses inside one Lance query. `lance_fts_boost` defines the hits by a `positive` clause and multiplies the score of the hits that also match `negative` by `negative_boost`; it has no stock counterpart (the stock `boosting` query is not rewritten to it). Every clause inside either must itself be a `lance_*` full text query (`lance_match`, `lance_match_phrase`, `lance_multi_match`, or a nested `lance_fts_bool` / `lance_fts_boost`); a stock `match` inside either returns 400.

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

Expected `hits.total.value`: 0, as for the stock `bool` with the same clauses. Replace the `must_not` with a `should` on `title:sunny` to see the intersection (`must ∩ should` = eight even rows) with the `should` clause's score added on Lance's side.

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

Expected `hits.total.value`: 8. The positive `hello` matches every even row; every even row also matches the negative `lance`, so each hit's score is multiplied by `0.1`. Compare `_score` against the plain `lance_match {"field":"body","query":"hello"}` to see the reduction. The stock `boosting` query is not translated to this; use `lance_fts_boost` when the negative clause has to be applied inside Lance's scoring.

### Primary key lookup

```
curl -s http://localhost:9200/demo/_doc/3
```

Expected `_source.body`: `quick brown fox 3` (odd `id`).

### Vector nearest neighbour (lance_knn)

`lance_knn` is the one query without a stock counterpart in this plugin (the `knn` query name belongs to the k-NN plugin). Each row `i` lives at coordinate `(i, 0, 0, ..., 0)`. The query vector `(2.4, 0, ...)` is nearest to row 2, then row 3.

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

`lance_knn` accepts an inner `filter` clause that Lance evaluates before applying the K-nearest cutoff (a pre-filter). Any `bool` combination of `match_all`, `term`, `terms`, `exists`, `range`, `wildcard`, `regexp` and `prefix` clauses works; anything else, a `match` included, returns 400 (`[lance_knn] filter type [MatchQueryBuilder] ...`).

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

### Aggregation combined with a full text query

```
curl -s -X POST 'http://localhost:9200/demo/_search?size=0' \
  -H 'Content-Type: application/json' \
  -d '{
        "query": {"bool":{"must":[{"match":{"body":"lance"}}]}},
        "aggs":  {"rating":{"terms":{"field":"rating"}}}
      }'
```

Expected: five buckets over the eight even rows, `1` and `3` and `5` with `doc_count` 2, `2` and `4` with `doc_count` 1.

An aggregation under a full text or `lance_knn` query always runs through OpenSearch's aggregators over the fragment leaf readers, with the doc values of the aggregated column fetched for the matched rows only. The aggregation pushdown into the Lance scan (a group by evaluated inside Lance) applies to `size: 0` requests over `match_all` or a scalar filter; "Aggregations: where they run" in step 6 walks through that and through the settings that steer it.

### Hybrid shape (bool.should combining FTS and vector)

Full text and vector sub-queries compose inside `bool.should`. Row `i` has body token `hello` only on even `i`, and its vector coordinate on the first axis is `i`. `match` on `hello` hits every even row, `lance_knn` near `(0.5, 0, ..., 0)` with `k=2` hits rows 0 and 1. The union has 9 rows; row 0 satisfies both clauses and sums to the highest `_score`.

```
curl -s -X POST 'http://localhost:9200/demo/_search?size=16' \
  -H 'Content-Type: application/json' \
  -d '{
        "query": {
          "bool": {
            "should": [
              { "match": { "body": "hello" } },
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

The same shape works with `lance_match {"field":"body","query":"hello"}` in place of the stock `match`; both are a Lance FTS scan per fragment, and Lucene's `bool.should` composition over them is identical.

For OpenSearch's dedicated `hybrid` query (per-sub-query top-K with a score-normalising search pipeline), install the [`neural-search`](https://opensearch.org/docs/latest/search-plugins/hybrid-search/) plugin alongside this one and follow its docs. Per-shard sub-query execution goes through the same Lucene `createWeight` / `Scorer` path the `bool.should` example above exercises.

### Where a request runs: the explain endpoint

`GET /{index}/_lance/explain` takes a search body and answers what the coordinator would execute for it, without running the search. The plugin plans every `_search` once, on the coordinating node, through a Calcite planner: the body is translated to a logical tree over the table, the planner picks the cheapest physical form that declares the traits the request demands, and the per node part of that form ships to the data nodes with each fragment request. The explain endpoint runs the same planning entry and prints the result, so what it shows is what a search with the same body executes. [query-plan.md](query-plan.md) is the reference for every field, the operators, the cost model, the refinements and the traits; this section shows the two answers the examples above produce.

The `bool` from "Composition with bool" is a shape the translator spells. The stock `match` was rewritten to `lance_match` before planning (the `logical` text and `lance_clause` show the rewritten clause), the `range` becomes the Lance SQL `rating >= 3` and rides on the pushed full text operation as its prefilter, the page is pushed too (`PUSHED_SCAN`), and nothing is `unplanned`. The physical lines carry three terms per operator (`accuracy`, `tie_stability`, `cost`), cut here for width:

```
curl -s -X GET 'http://localhost:9200/demo/_lance/explain?pretty' \
  -H 'Content-Type: application/json' \
  -d '{"size":3,"query":{"bool":{"must":[{"match":{"body":"hello"}}],"filter":[{"range":{"rating":{"gte":3}}}]}}}'
```

```json
{
  "index" : "demo",
  "route" : "fragment",
  "logical" : "LanceHitShape(columns=[[id, body, title, rating, embedding]], source=[true], id=[true], score=[true], sortValues=[false])\n  LanceTopK(collations=[[]], fetch=[3], offset=[0])\n    LanceFtsMatch(kind=[MATCH], columns=[[body]], query=[{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\",\"boost\":1.0}}])\n      LogicalFilter(condition=[>=(CAST($3):BIGINT NOT NULL, 3)])\n        LanceTableScan(table=[[lance, demo]])\n",
  "physical" : "MergeExec(reduce=[HITS_TOP_K], ...)\n  FanOutExec(fanOut=[1], partitioning=[EQUAL_FRAGMENT_GROUPS], ...)\n    LanceTableScan(table=[[lance, demo]], pushed=[[fts{kind=MATCH, columns=[body], query={\"lance_match\":{\"field\":\"body\",\"query\":\"hello\",\"boost\":1.0}}, filter=rating >= 3}, topk{collations=[], fetch=3, offset=0, hits{columns=[id, body, title, rating, embedding], source=true, id=true, score=true, sortValues=false}}]], ...)\n",
  "fragment_plan" : {
    "kind" : "PUSHED_SCAN",
    "filter_sql" : "rating >= 3",
    "lance_clause" : "lance_match",
    "top_k" : {
      "orderings" : [ ],
      "fetch" : 3
    }
  },
  "refinements_possible" : [ ],
  "traits" : {
    "requested" : { "accuracy" : "APPROXIMATE", "tie_stability" : "NONE" },
    "declared" : { "accuracy" : "EXACT", "tie_stability" : "UNSTABLE" },
    "enforcer" : "none"
  }
}
```

A shape the planner leaves to Lucene carries no query part: the executors run Lucene's collector over the query the field type built (`LUCENE_TOPK`) and `unplanned` names the element that kept the request on the Lucene side. Here the `match` sits in `should` next to a `filter` without a `must`, so it is optional in Lucene and the filter alone selects the rows, which Lance's boolean cannot express:

```
curl -s -X GET 'http://localhost:9200/demo/_lance/explain?pretty' \
  -H 'Content-Type: application/json' \
  -d '{"size":3,"query":{"bool":{"should":[{"match":{"body":"hello"}}],"filter":[{"range":{"rating":{"gte":3}}}]}}}'
```

```json
{
  "index" : "demo",
  "route" : "fragment",
  "logical" : "LanceTableScan(table=[[lance, demo]])\n",
  "physical" : "MergeExec(reduce=[HITS_TOP_K], accuracy=[EXACT], tie_stability=[STABLE_ROWADDR], cost=[{ms=1, native_bytes=1, heap_bytes=0}], total_cost=[{ms=18, native_bytes=2, heap_bytes=0}])\n  FanOutExec(fanOut=[1], partitioning=[EQUAL_FRAGMENT_GROUPS], accuracy=[EXACT], tie_stability=[STABLE_ROWADDR], cost=[{ms=1, native_bytes=1, heap_bytes=0}])\n    LanceTableScan(table=[[lance, demo]], accuracy=[EXACT], tie_stability=[STABLE_ROWADDR], cost=[{ms=16, native_bytes=0, heap_bytes=0}])\n",
  "fragment_plan" : {
    "kind" : "LUCENE_TOPK"
  },
  "unplanned" : "full text clause in [should] next to [filter] without a [must] clause",
  "refinements_possible" : [ ],
  "traits" : {
    "requested" : { "accuracy" : "APPROXIMATE", "tie_stability" : "NONE" },
    "declared" : { "accuracy" : "EXACT", "tie_stability" : "STABLE_ROWADDR" },
    "enforcer" : "none"
  }
}
```

How to read the fields:

- `route` is `fragment` for everything the coordinator fans out to the data nodes, which is every body except one holding `suggest` or `highlight`; those answer `route: unsupported` with the refusal message under `unplanned` (a `_search` with the same body answers that message as 400), and nothing else is planned.
- `logical` is the tree the translator built and `physical` the tree the planner chose: `MergeExec` (how the per node answers combine) over `FanOutExec` (how many data nodes the request fans out to) over the per node plan, a `LanceTableScan` carrying its pushed operations (`filter{sql=...}`, `fts{...}`, `knn{...}`, `topk{...}`, `aggregate{...}`), or a `HeapTopKExec` / `LuceneAggregateExec` operator over the bare scan when Lucene's collector or aggregators run the request. Every physical line ends with the `accuracy` and `tie_stability` the operator declares and the `cost` the planner charged it; the root adds `total_cost`, the figure the candidates were compared by. Below a million rows the milliseconds are placeholders (a bare scan charges one per row, which is where the `16` above comes from); at a million rows and above an aggregation is priced by the fitted model described in [query-plan.md](query-plan.md#cost).
- `fragment_plan` is what every data node receives: `kind` (`PUSHED_SCAN`, `LUCENE_TOPK`, `LUCENE_COUNT`, `LUCENE_AGGREGATE`), the `filter_sql` of the scalar predicate when there is one, the `lance_clause` the executor builds its Lance query from, and the pushed `top_k` page or `aggregate`.
- `unplanned` is present only when some element kept the request, or the whole query, on the Lucene side, and names it (`query type [match]` for a `match` on a field that is not `lance_text`, `full text clause in [should] next to [filter] without a [must] clause`, `sort type [_geo_distance]`, `aggregation type [multi_terms]`, `size [5] (only 0 with aggregations)`, `pipeline aggregation`). It is absent when the planner's cost model chose the Lucene operator for a tree that did translate; the physical plan shows that choice.
- `refinements_possible` lists the downgrades a data node could still apply to the shipped plan for what only it knows (`security_wrapper` when a DLS / FLS reader wrapper is installed, `sort_field_type` for a page sorted by an `ip` column). `GET /_lance/stats` counts what the nodes did under `plan.refinements` and `plan.executed`, see step 6.
- `traits` is what the body demanded of the plan (`requested`: an explicit `track_total_hits` demands `EXACT` accuracy, a `search_after` cursor demands a reproducible tie order; `APPROXIMATE` and `NONE` mean no demand) against what the chosen plan declares (`declared`). The bare scan above returns rows in Lance row address order (`STABLE_ROWADDR`); a page cut in score order out of a full text or knn scan is `UNSTABLE`, which is why `search_after` over a `lance_match` page sorted by `_score` alone is refused. `enforcer` says whether the planner had to replace the cheapest plan with one meeting the demand.

The plan text format will change as the planner grows; read it, do not parse it.

## 6. Refresh behaviour when Lance moves forward

If you rewrite the table externally (Python `dataset.append`, `dataset.update`, `merge_insert`, or a Ray / Spark writer), the freshness check on the node holding the index's shard picks up the new manifest version within one cadence period and swaps the shard's reader. Queries reflect the new data after the next check fires. No `_refresh`, `_close`, or shard reallocation is needed.

Force a faster check by lowering `lance.namespace.poll_cadence` (node-level setting, minimum 1s; it is the cadence of both the catalog listing on the cluster manager and the freshness check on the shard's node) in `opensearch.yml`, or run the check now with `POST /demo/_lance/sync`:

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

When Lance advances to a new version, the plugin exposes it as soon as the next freshness check observes the change. The appended fragments do not have to be covered by every existing index first: Lance's own scanner produces a mixed execution plan for FTS and knn (covered fragments use the existing index, uncovered fragments run a flat scan, and the results are unioned by the query engine), so an incremental append never slows down queries hitting the previously-covered fragments. Whenever uncovered fragments accumulate to the point that flat-scan latency becomes noticeable, call `POST /_lance/build_indexes/{index}` with `{"optimize": true}` to fold them into the existing indexes.

The `index.lance.uncovered_fragment_policy` setting still accepts `wait` alongside the default `immediate`. Both values currently expose the new version immediately; `wait` is reserved for a future async-optimize implementation, and setting it today logs an informational message so operators are aware that the plugin does not run auto-optimize.

### Serving an object store table from local NVMe

Lance's object store layer has no read-through cache on local disk, and the plugin adds none: every byte a request needs that is not already in memory is read from the store the table was attached from. On S3 that puts the store's latency on the first request that touches an index or a column: the pages a `term` filter reads from a BTree, the token dictionaries and posting lists of a full text query, the first load of a column into the column store, the partitions a nearest scan probes, and the `_rowaddr` take behind every page of hits. On the project's 1B row benchmark table, read from S3 by four r7gd.4xlarge data nodes, `filter rating=5 + terms(category)` took 164 s the first time (the rating BTree was being read from S3) and 4.3 s once its pages were in the Lance Session cache; the same table and plugin build on one r7gd.16xlarge with the table on the instance's NVMe took 14 s cold and 4.8 s warm. A `lance_knn` (k=10, nprobes=200) on the same table went from 17.9 s cold to 0.3 s warm when read from S3 by three such nodes, and from 1.0 s to 0.2 s on the NVMe node. A full text query whose inverted index does not fit one shard of the index cache (about 52 bytes per row, so about 48 GiB at 1B rows) rebuilds its document set from the index files on every query, and that read comes from the store every time. Everything the node holds in memory (the Lance Session cache, the plugin's column store and snapshots) is gone after a restart, so a restarted node pays every cold read again. Local disk does not shorten a shape that is bound by Lance's CPU work rather than by reads: a one hit `lance_match` on the 1B row table took 30 s on S3 and on NVMe alike.

Two operational answers exist today. Neither needs a plugin setting, and the plugin has no setting for a local mirror or cache.

**Copy the table to every data node.** Run `aws s3 sync s3://<bucket>/tables/t.lance /nvme/tables/t.lance` on every data node and attach the local path:

```
curl -X POST http://localhost:9200/_lance/attach \
  -H 'Content-Type: application/json' \
  -d '{"table":"/nvme/tables/t.lance"}'
```

The project's 1B row table (750 GB) syncs to one r7gd.16xlarge in about 24 minutes (1,413 s and 1,434 s in two runs). Every data node needs the whole table, not only the fragments it happens to execute: the fragment share of a node changes with cluster membership, and index files are read on every node. Lance never rewrites a data file, an index file, a deletion file or a manifest under `_versions/`, so rerunning `aws s3 sync` after the writer commits fetches only the files the new version added, and the freshness check picks the new version up on its next cadence from the local path exactly as it would from S3; files a Lance cleanup removed from the bucket stay on disk unless the sync runs with `--delete`. What matters is that every data node holds the same versions at the same path. The coordinator plans each request against the latest manifest at the path it sees, and a data node whose copy is behind cannot open that manifest, so every request that reaches it fails with 400 naming the `_versions/<N>.manifest` it could not open until its sync catches up. Run the sync from a cron on every node or from the writer's commit hook, and keep the window in which nodes disagree short: sync everything except `_versions/` on every node first (`aws s3 sync --exclude '_versions/*' ...`), then sync `_versions/` on every node.

**Mount the bucket with a local cache.** [Mountpoint for Amazon S3](https://github.com/awslabs/mountpoint-s3) can keep a local cache of the object content it has read, so the first read of a piece of an object comes from S3 and later reads of the same piece from disk:

```
mount-s3 <bucket> /mnt/tables --cache /nvme/mp-cache --max-cache-size <MiB> --metadata-ttl minimal
curl -X POST http://localhost:9200/_lance/attach \
  -H 'Content-Type: application/json' \
  -d '{"table":"/mnt/tables/tables/t.lance"}'
```

`--cache <dir>` is the cache directory (Mountpoint creates a subdirectory in it and empties that subdirectory at mount time and at exit, so a remount starts cold), `--max-cache-size <MiB>` bounds it (the default keeps 5 percent of the file system free), and `--metadata-ttl` is how long Mountpoint trusts the file metadata it cached, which with `--cache` defaults to 60 seconds; a new manifest can stay invisible to the freshness check for that long, so set the TTL to `minimal` or to a few seconds when a writer commits to the table. The options are documented in Mountpoint's [CONFIGURATION.md](https://github.com/awslabs/mountpoint-s3/blob/main/doc/CONFIGURATION.md). This path needs no copy step and no cron, but the project has not measured it. Mountpoint documents itself as optimised for sequential reads of large objects, while the reads that dominate a cold request here are small ranges at scattered offsets (BTree pages, `_rowaddr` takes), so how much of the 164 s above the cache removes on the second request, and what the first request costs through the mount compared with reading S3 directly, is not known.

Should Lance's object store layer gain a read-through disk cache of its own (the project intends to propose one upstream), both answers reduce to one `storage_options` entry on the attach body.

`index_placement: node_local` is not a read cache: it makes every data node build indexes into a shallow clone of the table under its data path so that the source stays read only, and every data read of the source still goes to the store. See "Attach and namespace surface" in [features.md](features.md#attach-and-namespace-surface).

### Cap Lance's native memory footprint

Lance keeps its inverted-index and metadata caches in native memory, outside the JVM heap. The plugin installs a single Lance `Session` at startup so every table on a node shares the same caches. The upper bound is set by `lance.native_memory.limit`, a node-level setting that accepts either a byte value or a percentage of the memory left after the JVM heap is subtracted from physical memory. The default is `40%`, which scales with instance size and leaves room for the k-NN plugin's own memory budget on nodes that host both plugins.

```
lance.native_memory.limit: 40%      # default; percent of (physical - heap)
lance.native_memory.limit: 10gb     # or an absolute byte value
```

The parsed value is split first by `lance.cache.column_share` (default 0.4), which goes to the off-heap column store; the rest belongs to the Lance `Session` and is split 6:1 between the index cache and the metadata cache, mirroring Lance's own default ratio. On startup the plugin logs the resolved sizes so the operator can confirm the split, for example `installed shared Lance Session: limit [37gb] -> index cache [15.9gb] (shards 2, share 7.9gb per shard), metadata cache [3.1gb], column cache [14.8gb], unused [3gb] (from lance.native_memory.limit [40%], lance.cache.column_share [0.4], 16 cpus)`. This is a static setting today, so a change requires a rolling restart to take effect.

The index cache does not receive its whole 6/7 share, and the log line says why. Lance backs the index cache with a sharded cache whose shards do not borrow capacity from one another, and an entry heavier than one shard's share is refused without an error. The shard count is `min(cpus / 2, capacity / 4 GiB)` rounded down to a power of two (at least 1, at most 1024), so the share per shard is not monotonic in the capacity: on 16 CPUs a 19 GiB cache is 4 shards of 4.75 GiB, 16 GiB minus one byte is 2 shards of 8 GiB, and 32 GiB is 8 shards of 4 GiB. An inverted index is kept as one entry per full-text column (about 52 bytes per row, 4.8 GiB at 100M rows), so a cache whose share is below that reloads the index from storage on every full-text query. The plugin therefore chooses, within the 6/7 budget, the capacity whose share per shard is largest (the candidates are the budget itself and each `k * 4 GiB - 1` for `k = 2, 4, 8, ...`), hands that to Lance and leaves the difference unused; it is not given to the column store. `GET /_lance/stats` reports the chosen capacity, shard count and share under `native_memory`, and `POST /_lance/attach` logs a warning when a table's estimated inverted index entry is heavier than the share. To raise the share, raise `lance.native_memory.limit` or lower `lance.cache.column_share`; with the choice above a larger budget never yields a smaller share.

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

### Inspect the snapshot and column cache

`GET /_lance/stats` shows, per node, what the plugin holds in its caches: how many table snapshots (open Lance datasets) it keeps, how the off-heap column store is doing against its budget, and how the two add up to the `lance_native` breaker reading.

```
curl -sS localhost:9200/_lance/stats?pretty
```

```json
{
  "_nodes" : { "total" : 1, "successful" : 1, "failed" : 0 },
  "cluster_name" : "opensearch",
  "nodes" : {
    "Xc3...": {
      "name" : "node-1",
      "snapshots" : {
        "enabled" : true,
        "count" : 2,
        "retired" : 0,
        "dataset_open_count" : 3,
        "snapshot_build_count" : 3,
        "snapshot_hit_count" : 41
      },
      "column_store" : {
        "bytes" : 838860800,
        "limit_bytes" : 4294967296,
        "entries" : 96,
        "hits" : 120,
        "loads" : 12,
        "evictions" : 0,
        "budget_misses" : 0,
        "heap_fallback_bytes" : 0,
        "heap_fallback_rejections" : 0
      },
      "native_memory" : {
        "estimated_bytes" : 1258291200,
        "session_bytes" : 419430400,
        "column_store_bytes" : 838860800,
        "index_cache_capacity" : 17179869183,
        "index_cache_shards" : 2,
        "index_cache_shard_share" : 8589934591
      },
      "fts" : {
        "subset_probe_limit" : 1000000
      },
      "admission" : {
        "enabled" : true,
        "headroom_bytes" : 8589934592,
        "available_bytes" : 98784247808,
        "retained_bytes" : 0,
        "retained_scope" : "none",
        "last_estimate_bytes" : 0,
        "last_kind" : "none",
        "rejections" : {
          "fts" : 0,
          "scalar_index" : 0,
          "vector_index" : 0,
          "filter_scan" : 0,
          "aggregate_scan" : 0,
          "column_load" : 0
        }
      },
      "warm_up" : {
        "mode" : "metadata",
        "tables" : [
          {
            "index" : "perf20m",
            "table" : "s3://bucket/perf20m.lance",
            "version" : 8,
            "mode" : "metadata",
            "state" : "done",
            "started_at" : "2026-09-21T03:39:23.435Z",
            "seconds" : 5.71,
            "indexes" : [
              { "name" : "body_idx", "type" : "Inverted", "column" : "body", "state" : "done", "seconds" : 5.55 },
              { "name" : "rating_idx", "type" : "BTree", "column" : "rating", "state" : "done", "seconds" : 0.01 }
            ]
          }
        ]
      },
      "plan" : {
        "statistics" : { "tables" : 1, "collect_millis_total" : 94, "pending" : 0, "planned_without" : 1 },
        "refinements" : {
          "security_wrapper" : 0,
          "sort_field_type" : 0,
          "aggregate_resolution" : 0,
          "column_store_warm" : 0
        },
        "executed" : { "pushed_scan" : 6, "lucene" : 51 },
        "pruned" : { "fragments" : 0 }
      },
      "fetch" : {
        "take_count" : 57, "take_rows" : 570, "take_columns" : 342,
        "take_millis_total" : 121, "take_max_millis" : 9,
        "stored_fields_takes" : 51, "column_takes" : 6
      }
    }
  }
}
```

The response carries three more sections than shown (`freshness`, `indices`, `local_clones`); the ones above are the ones this walkthrough refers to.

How to read it:

- `snapshots.count` is normally the number of Lance-backed shards on the node (each shard reader holds its version's snapshot) plus any version a request is still reading. It grows by one when a table advances and the poll has not refreshed the shard yet, and comes back down when the poll retires the old version. A `retired` value that stays above zero means a reader of an old version has not closed.
- `snapshot_build_count` and `dataset_open_count` should stop growing once every table version in use has been seen; `snapshot_hit_count` grows with every `_search`. Builds that keep growing on a table that is not changing mean requests are not finding the cached version.
- `column_store.bytes` against `limit_bytes` tells you how much of `lance.cache.column_share` is in use. `loads` grows on the first request that reads a column of a fragment, `hits` on every later one. `budget_misses` above zero means requests fell back to heap loads because the store was full; raise `lance.cache.column_share` or `lance.native_memory.limit`, or reduce the number of columns aggregated or sorted on. `heap_fallback_bytes` is the heap those loads currently hold on the request circuit breaker, and `heap_fallback_rejections` counts the loads the breaker refused (HTTP 429 to the client); a rising rejection count means the columns that miss the store are too large for `indices.breaker.request.limit` on this node.
- `native_memory.estimated_bytes` is what the breaker enforces against `lance.native_memory.limit`; it lags `session_bytes + column_store_bytes` by at most one `lance.native_memory.circuit_breaker.poll_interval`. Compare it with the process RSS to see how much of the native footprint the plugin accounts for.
- `native_memory.index_cache_capacity`, `index_cache_shards` and `index_cache_shard_share` are the index cache the plugin handed Lance at startup and the shard layout Lance derives from it (see "Cap Lance's native memory footprint"). `index_cache_shard_share` is the heaviest entry the cache admits; a table whose inverted index is heavier than it (about 52 bytes per row per full-text column) is reloaded on every full-text query.
- `warm_up` is the index warm-up of the section below: `mode` is the value of `lance.attach.warm_indexes` on the node, and `tables` has one entry per Lance-backed index the node has seen since it started, with the table, the manifest version the warm-up read, the mode it ran under, its `state` (`pending`, `running`, `done`, `failed`, `skipped` for mode `none`, `cancelled` when the index was deleted first), when it started, how long it took, and one entry per Lance index (`name`, `type`, `column`, `state`, `seconds`, and a `detail` when it failed or was skipped). A table whose entry stays `running` for minutes on an object store is reading its indexes page by page; the INFO log shows one line per index as it finishes.
- `plan` is what the node did with the plans the coordinator shipped: `refinements` counts, per reason, the pushed operations the node moved to the Lucene side, and `executed` counts the fragment requests the Lance scan answered against the ones Lucene's collector and aggregators answered. "Aggregations: where they run" below explains the four reasons. `statistics` is the planner's table statistics cache on the node: `tables` (table versions held), `collect_millis_total` (time spent collecting them), `pending` (collections running in the background right now) and `planned_without` (requests the node planned without statistics because their version was not collected yet; the first request against a freshly attached table on a node that only coordinates is one, and on a table of billions of rows `pending` stays at one for the minutes the collection takes while the requests keep answering).
- `fetch` counts the `_rowaddr IN (...)` take scans the node ran for the rows behind hits (`stored_fields_takes`, one per leaf that holds a hit of a page) and for the sort or aggregation column of a small full-text or vector hit set (`column_takes`), with the rows and columns they asked for and their wall time. Nothing caches these rows, so every page pays for its own takes; `take_millis_total` set against the `took` of the requests the node served over the same interval says how much of the request time the fetch is.

The endpoint is read only. With the security plugin, grant `cluster:monitor/lance/stats`.

The response also carries a `request_cache` section (not shown above): the coordinator's result cache, which keeps the reduced answer of every `size: 0` request against one Lance backed index and serves the same body again while the table stays at the version the answer was computed from, so a dashboard refreshing the same aggregation scans the table once per table version rather than once per refresh. `hits` and `misses` tell how often that happens on the node you asked; `skipped` counts the requests the cache does not take (a body with hits, a target of several indexes, `request_cache=false`). An append to the table is a new version and a miss, `POST /<index>/_cache/clear?request=true` drops the index's entries on every node, and `GET /<index>/_lance/explain` answers `cacheable` for a body. The settings are `lance.request_cache.enabled`, `size`, `max_entry_size` and `expire`; [features.md](features.md#result-cache) has the key and the rules.

### Warm the indexes when a table is attached

Lance opens an index the first time a scan uses it: a BTree reads its page lookup and then one object store request per page that holds a matching value, a full-text index reads the token dictionary of every partition, an IVF index reads its centroids and the codes of the probed partitions. The loaded parts stay in the Lance Session cache, so on a table read from S3 the first request that uses an index pays for the load in latency bound page reads while the next one takes a fraction of a second. On a 20M row table on a local MinIO the first `match` took 9 to 13 s (63 MB of token dictionaries) and the first `lance_knn` 2.1 s, against 0.5 s and 0.1 s warm; on a 1B row table on S3 the first `term` on a BTree column took 200 to 350 s.

The plugin therefore warms the indexes of every Lance-backed index on every data node as soon as the index appears in the cluster state, which is right after `POST /_lance/attach` returns, when the namespace poll surfaces a table, and when a node applies its first cluster state after a restart. The attach response does not wait for it. The warm-up runs on the `lance_warm_up` thread pool (one thread, so tables warm one after another and the indexes of a table one after another) and issues, per Lance index, the smallest scan that makes Lance load the part named by the mode; no data column is read. A request that arrives while the warm-up runs does not wait: it loads what it needs on its own and Lance's cache reconciles the two. A warm-up that fails logs a WARN line and leaves the request path unchanged.

```
lance.attach.warm_indexes: metadata   # default; none | metadata | all; dynamic
```

- `none`: nothing is read. The stats record the index with `state: skipped`.
- `metadata`: every index is opened. BTree: the page lookup (`page_lookup.lance`, a few KB per thousand pages). Bitmap: the keys. Full-text: the token dictionaries (63 MB and 5.5 s on a 20M row column; about 3 bytes per row). IVF: the centroids and one partition. The pages, bitmaps, posting lists and doc lengths a query needs are still read by the first query that needs them, so on a large BTree the first `term` still pays for its pages; what this mode removes is the open of every index (the token dictionaries dominate) and the round trips before the first page read.
- `all`: `metadata`, plus every BTree page, every bitmap and every IVF partition (full-text indexes are opened as under `metadata`: their posting lists are only reachable by token). The read is one object store request per page, in parallel up to the CPU count, so on the 20M table it read 45,000 objects and 820 MB in 57 s. The pages are useful only while they stay in the Session index cache: when the index files of a table (full-text ones excluded) add up to more than half of `native_memory.index_cache_capacity`, the warm-up opens the indexes only, as under `metadata`, and says so in the `detail` of every index entry. Size the cache with `lance.native_memory.limit` before choosing `all` for a table whose BTrees are large; a 1B row BTree is about 4.7 GB of pages per column.

Changing the setting affects warm-ups that start after the change; re-attach the index (or restart the node) to warm an already attached table under a different mode. The `lance_warm_up` pool is a fixed pool of one thread with a queue of 1,000 tables (`thread_pool.lance_warm_up.queue_size`); `GET /_cat/thread_pool/lance_warm_up?v&h=node_name,active,queue` shows whether a warm-up is running.

### Full-text lookups on several data nodes

With more than one data node each node executes a share of the table's fragments, but a full-text query still looks the whole table up from the inverted index and keeps its own rows, because passing Lance a fragment list makes it read `_rowid` over those fragments first. Shapes that need every match (aggregations, sort by a field, post_filter, `size 0`, `track_total_hits: true`) run that lookup as a probe whose row limit is derived from the rows the node covers, through three dynamic cluster settings:

```
lance.fts.subset_probe_ratio: 0.03        # default; share of the rows the node covers that the probe may return
lance.fts.subset_probe_min_rows: 10000    # default; floor of the probe limit
lance.fts.subset_probe_limit: 1000000     # default; cap of the probe limit
```

The effective probe limit is `min(subset_probe_limit, max(subset_probe_min_rows, floor(covered rows * subset_probe_ratio)))`. When the lookup returns that many rows the node discards them and repeats the scan restricted to its fragments. The ratio is where the two paths cost the same: the whole-table lookup makes every node receive every match and drop the rows of other nodes, about 0.5 to 0.9 µs per received row, while the restricted scan makes Lance read `_rowid` over the node's fragments first, about 21 ns per covered row (139 ms for 6.7M rows at 20M rows). The two are equal when the matches are about 3 percent of the covered rows. On a 20M row table over 3 nodes the limit is 200,000, so a term with 500,000 matches takes the restricted scan and a term with a few hits stays on the index-only lookup. Raise the ratio when the restricted scan is slower than the lookup on your hardware, lower the floor or the cap when the per-node heap for the probe rows matters.

### Admission control for native scans and index loads

Lance allocates native memory the plugin's breakers never see: an inverted index document set or the matching pages of a BTree that do not fit one index cache shard, the IVF partitions a nearest scan probes, the row addresses a filtered scan materialises, the read queue and decoded batches of every scan. On a table large enough for the node any of these ends the node with a kernel OOM kill. Before each such scan starts, the executor estimates what it will make Lance allocate and answers 429 `circuit_breaking_exception` (label `lance_admission`) when the node's available physical memory (the kernel's `MemAvailable` on Linux, the free physical memory elsewhere) minus a headroom, plus the memory earlier admitted scans retained, cannot hold it, instead of letting the kernel kill the node. Three dynamic cluster settings control it:

```
lance.admission.enabled: true              # default; false admits every shape
lance.admission.headroom: 8gb              # default; available memory kept out of reach of a scan
lance.admission.bounded_shapes_gated: true # default; false admits bounded full text pages ungated and judges a bounded filter page on its limit
```

The 429 message names the kind of scan (`fts`, `scalar_index`, `vector_index`, `filter_scan`, `aggregate_scan`, `column_load`), the estimate, the available memory, the headroom and what to relax. `GET /_lance/stats` reports the decisions under `admission`, with one rejection counter per kind. The estimates are a model whose coefficients are pinned to the measurements the project has; a 429 on a table whose scan does not fit the node is the intended answer, and the shapes that never scan (`GET /_doc`, `_count` without a filter, `match_all` pages) are never gated.

### Aggregations: where they run

Every aggregation type OpenSearch ships runs on the fragment path, that is on the data nodes over the fragments each one holds, with the coordinator reducing the per node results the way `SearchPhaseController` reduces shards. No aggregation tree changes how a request routes. Within the fragment path there are two ways to compute one:

- Inside the Lance scan. A `size: 0` request over `match_all` or a scalar filter (`term`, `terms`, `range`, `exists`, `bool` of those) whose tree is metrics only (`stats`, tdigest `percentiles` and `percentile_ranks` included), a chain of up to three `terms` / `histogram` / `date_histogram` / `range` / `date_range` / `filter` / `filters` / `missing` levels with metric children, or one `composite` over `terms` / `date_histogram` sources, is translated to a Substrait `AggregateRel` and evaluated by Lance's DataFusion kernel as a group by; each executor gets one row per group and builds the same `InternalAggregation` the aggregators would. The explain endpoint shows it as `aggregate{...}` among the scan's pushed operations and `fragment_plan.kind: PUSHED_SCAN`. The shapes are listed in [features.md](features.md#aggregation-pushdown).
- Through OpenSearch's aggregators over the fragment leaf readers (`LuceneAggregateExec` in the physical plan, `fragment_plan.kind: LUCENE_AGGREGATE`). This serves every other tree: `multi_terms`, `matrix_stats`, `weighted_avg`, `median_absolute_deviation`, `variable_width_histogram`, `adjacency_matrix`, `sampler`, `nested`, the geo aggregations over a `geo_point` override column (`geo_distance`, `geohash_grid`, `geotile_grid`, `geo_centroid`, `geo_bounds`), scripted aggregations, `cardinality`, any tree under a full text or `lance_knn` query, a `post_filter`, a page (`size > 0`), or a pipeline aggregation (the bucket tree runs on the executors and the pipelines on the coordinator's final reduce). The `terms` over `rating` under `match` in step 5 is one of these.

Five types cannot run on the fragment executors and answer 400 `illegal_argument_exception` naming the builder (`aggregation type [global] on [g] is not supported for Lance-backed indices: ...`), from `_search` and from the explain endpoint alike: `global`, `top_hits`, `rare_terms`, `significant_terms`, `significant_text`. The reasons are in [limitations.md](limitations.md#aggregations-the-fragment-path-does-not-serve).

Between the two ways the planner decides by cost ([query-plan.md](query-plan.md#cost) has the model), and explain shows the decision. A tree the translator spells is planned in both forms, the pushed scan and `LuceneAggregateExec`, and each is priced by a latency model fitted to measurements of the aggregation shapes on 20M, 100M and 1B row tables across 1 to 6 node clusters: a fixed cost per request, an object store open latency for `s3://` / `gs://` / `az://` tables, the per row work divided by the fan out node count and by the thread settings below, the object store transfer of the columns read, a penalty above a million groups. On tables under a million rows the pushed scan always wins; above it the choice depends on the table size, the node count and the storage kind (a keyword `terms` over a billion rows on S3 plans as `LuceneAggregateExec` on a four node cluster, the same tree over twenty million rows on local disk as the pushed scan), and `cardinality` always loses the comparison because feeding Lance's distinct values into the sketch measured slower than the aggregator. When the cost chose the aggregators, explain shows `LuceneAggregateExec` and nothing under `unplanned`; `unplanned` appears only when the translator could not spell the tree (`aggregation type [multi_terms]`, `bucket tree deeper than 3 levels`, ...). Two settings are inputs to the same cost comparison rather than switches in front of it: `lance.aggregation.pushdown: false` prices every pushed aggregate as infinite, and `lance.aggregation.pushdown_max_groups` (node setting, default `1000000`) does the same for a tree whose group rows, estimated from the table statistics, exceed the bound.

A data node may still move a pushed aggregate (or a pushed page or full text clause) to the Lucene side for what only it can judge. There are four such reasons ([query-plan.md](query-plan.md#refinements)), counted per node under `plan.refinements` in `GET /_lance/stats`, and the two the coordinator can predict from the mapping are listed under `refinements_possible` by the explain endpoint:

- `security_wrapper`: a reader wrapper (the security plugin's DLS / FLS) is installed on the index, so a pushed aggregate, a pushed page and a pushed full text clause go to the aggregators, the collector and the Lucene composition of the query, which the wrapper filters. A pushed `lance_knn` and the filter SQL survive the wrapper.
- `sort_field_type`: the pushed page orders by a column whose Lucene sort field carries a format the scan cannot type its sort values from (an `ip` override column), so the page goes to the collector.
- `aggregate_resolution`: the pushed aggregate's fields do not resolve against the node's mapping, or the executor's own group estimate from the request shape (the product of the `terms` levels' `shard_size`, a `range` / `filters` level as its bucket count plus one) exceeds `lance.aggregation.pushdown_max_groups`; the aggregators run.
- `column_store_warm`: over an object store table, the node's off heap column store already holds every column the aggregators would read for every fragment of the request, and the aggregators over resident columns are predicted cheaper than scanning the object store again (the coordinator ships both predicted costs with the plan). Nothing warms the column store at attach, so this fires only after a Lucene side request has loaded the columns; over a local table it never fires.

`plan.executed` next to it counts, per node, the fragment requests the Lance scan answered (`pushed_scan`) and the ones Lucene's collector and aggregators answered (`lucene`, which includes every full text or `lance_knn` page), so a request whose explain says `PUSHED_SCAN` should move the first counter:

```json
"plan" : {
  "statistics" : { "tables" : 1, "collect_millis_total" : 94, "pending" : 0, "planned_without" : 1 },
  "refinements" : {
    "security_wrapper" : 0,
    "sort_field_type" : 0,
    "aggregate_resolution" : 0,
    "column_store_warm" : 0
  },
  "executed" : { "pushed_scan" : 6, "lucene" : 51 },
  "pruned" : { "fragments" : 0 }
}
```

`plan.pruned.fragments` counts the fragments the node's executor left out of its scans because the coordinator's zone map pruning excluded them (a `range` or `term` on a column with a zone map index whose zones cannot hold the value; the explain answer lists them under `fragment_plan.excluded_fragment_ids`). It stays at zero on a table without a zone map index. See [features.md](features.md#fragment-pruning).

Settings that steer the pushed scan, all dynamic cluster settings unless noted:

```
lance.aggregation.pushdown: true              # default; false prices every pushed aggregate as infinite, so the aggregators answer
lance.aggregation.pushdown_parallelism: 4     # default: half the CPUs the JVM sees (at least 1, at most 32)
lance.aggregation.pushdown_max_groups: 1000000 # node setting; bound on the estimated group rows of a pushed tree
lance.aggregation.percentiles_bins: 4096      # default; bins of a pushed down tdigest percentiles histogram (16 to 1000000)
lance.aggregation.pushdown_topk_slack: 4      # default; per scan retention of a top-k ordered terms, in multiples of shard_size (1 to 64)
```

Lance aggregates one scan on a single thread, so a node that holds many fragments cuts them into `pushdown_parallelism` contiguous groups, scans the groups at once on the `index_searcher` thread pool and merges the group rows before it builds its buckets. Set it to `1` to compare against a single scan; raise it up to the node's core count when a `terms` over many rows is slower than the same request with the pushdown priced out.

A tdigest `percentiles` is sketched from a histogram of `percentiles_bins` equal width bins over the field's range instead of from every document: the histogram is accurate to one bin width, and the TDigest built from it interpolates a little less accurately than one built from every document (see [limitations.md](limitations.md)). Raise the bin count when a percentile needs to be closer than `(max - min) / 4096`; every bin the data fills is one row the executor reads per bucket.

A single `terms` level ordered by `_count` (the default) or by one of its own single value metric children keeps only the best `shard_size * pushdown_topk_slack` groups per scan; every other group's rows stay counted in `sum_other_doc_count`, and `doc_count_error_upper_bound` keeps the meaning it has across shards. A slack that retains every distinct key gives the exact answer; raise it when high cardinality terms need tighter counts.

### Aggregations and hit pages collected on several threads

Every other aggregation, and every page of hits, runs through OpenSearch's collectors over the executor's fragments. Two dynamic cluster settings decide how many threads an executor uses for that:

```
lance.fragment_path.parallelism: 4            # default: half the CPUs the JVM sees (at least 1, at most 32)
lance.fragment_path.slices: 4                 # default: half the CPUs the JVM sees (at least 1, at most 32)
```

`parallelism` is the number of Lance scans an executor runs side by side when it reads a column into memory. `slices` is the number of slices it cuts its fragments into when it collects: each slice collects on its own `index_searcher` pool thread with its own collector, the way concurrent segment search does on a shard, and the slice results are reduced on the executor. Set `slices` to `1` to collect on one thread in fragment order (the tdigest `percentiles` and `cardinality` sketches are then built once per executor instead of once per slice); raise it towards the node's core count when an aggregation that the scan does not compute (`multi_terms`, `cardinality`, any tree under a full text or `lance_knn` query, or `terms` with `lance.aggregation.pushdown: false`) keeps one core busy while the others idle. With the log level of `org.opensearch.lance.dispatch.TransportLanceFragmentQueryAction` at `DEBUG`, each request logs the number of leaves and slices it collected.

### The coordinator thread pool

A `_search` against a Lance-backed index has two halves. The coordinator half runs on the node that received the request: it resolves the index, enumerates the table's fragments, sends one request per data node, and merges the per-node answers (sorts the hits, reduces the aggregations). The executor half runs on every data node on the `search` pool: it scans its fragments. The plugin gives the coordinator half its own fixed pool, `lance_coordinator`, so that a burst of requests cannot fill a data node's `search` queue with coordinator work, and a full `search` queue cannot stop the transport layer from delivering a fragment response. The responses themselves are received on the transport thread and only stored there; the merge is queued on `lance_coordinator` once the last node has answered. The work an executor runs beyond its own thread (collection slices, column loads, pushdown scans) is submitted to the node's `index_searcher` pool, the one concurrent segment search uses, so a request's own parallelism never occupies the `search` queue slots that admit requests.

```
thread_pool.lance_coordinator.size: 8            # default max(1, allocated processors / 2)
thread_pool.lance_coordinator.queue_size: 10000  # default
```

Both are static node settings. When the pool is full, the request fails with HTTP 429 and a `rejected_execution_exception` naming the pool, whether the rejection hit the request's entry or its merge; the plugin does not run the request through the shard engine's reader instead. `GET /_cat/thread_pool/lance_coordinator?v&h=node_name,active,queue,rejected` shows the pool per node. A `rejected` count that keeps growing means the coordinating nodes receive more concurrent requests than they can merge; spread the client's requests over more coordinating nodes or raise `queue_size`, which trades the 429s for longer queueing.

## 7. Cleanup and restart

Namespace registrations live in the cluster state and are persisted with it, so a restart of OpenSearch keeps them: the catalogs re-initialise, the surfaced indexes reopen their tables, and the catalog listing and the freshness check resume on their own. Nothing has to be registered or attached again after a restart.

To stop surfacing tables from a namespace, unregister it (`path` identifies a directory registration, `name` any other type). The indexes it surfaced stay in place until you delete them:

```
curl -X DELETE http://localhost:9200/_lance/namespace \
  -H 'Content-Type: application/json' \
  -d '{"path":"/tables"}'
curl -X DELETE http://localhost:9200/demo
```

Deleting a Lance-backed index while its namespace is still registered is honoured for `lance.namespace.resurface_guard_grace` (default one hour); after that the catalog listing recreates the index if the table is still there.

## 8. Troubleshooting

**`java.lang.OutOfMemoryError: Direct buffer memory`.** Arrow's C data interface allocates off-heap; heavy scans hit the JVM's direct-memory ceiling before the heap. Raise it with `-XX:MaxDirectMemorySize=2g` (tune to workload).

**A query on a mapped column returns zero hits when Python `dataset.to_table()` shows data.** Inspect the OpenSearch mapping (`curl -s http://localhost:9200/<index>/_mapping`). If the column is missing there, its Arrow type is not yet covered by the mapping derivation; open an issue with the schema.

**`match` returns hits `operator: and` should have excluded, or `match_phrase` ignores word order.** On a `lance_text` field at the top of the query or inside a `bool` / `dis_max`, both reach Lance (step 5); check the field's mapping type with `GET /<index>/_mapping` (a `keyword` override answers `match` as an exact term) and `GET /<index>/_lance/explain` (`lance_clause` names the rewritten clause). Inside another compound (`function_score`, `nested`, `constant_score`) the `operator` of `match` is not applied; write `lance_match` there.

**`lance_match_phrase` answers 400 `position is not found but required for phrase queries`.** The inverted index was built without token positions. Rebuild it with `with_position=True` (pylance) or `"with_position": true` on `POST /_lance/build_indexes/{index}`; positions are fixed when the index is created.

**A request is slower than expected and you want to know where it ran.** `GET /<index>/_lance/explain` with the same body (step 5) prints the plan and names what kept it on the Lucene side under `unplanned`; `GET /_lance/stats` shows under `plan` whether the data nodes executed the shipped plan or downgraded it, and why.

## Next steps

- Full feature reference: [features.md](features.md).
- The query plan and the explain endpoint in full (operators, cost model, refinements, traits, wire format): [query-plan.md](query-plan.md).
- Known limitations and refused shapes: [limitations.md](limitations.md).
- How the plugin is put together, including the planner design: [architecture.md](architecture.md).
- The plugin's design and the invariants it upholds live in the RFC: [opensearch-project/OpenSearch#22643](https://github.com/opensearch-project/OpenSearch/issues/22643).
- Known unfinished work is tracked as issues in this repository.
