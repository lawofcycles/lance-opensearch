/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.nio.file.Files;
import java.nio.file.Path;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.core.rest.RestStatus;

/**
 * Search request shapes served by the fragment dispatch path (scalar
 * filters, date ranges, unmapped fields, top-k, pagination, post_filter,
 * search_after, scripts, response envelope) and the shapes that fall through
 * to the shard path.
 */
public class LanceSearchDispatchIT extends LanceRestTestCase {

    public void testFragmentDispatchModeAnswersFilterQueries() throws Exception {
        // Fragment-path baseline: match_all + filter queries
        // (term / terms / exists / range / bool) on Lance-backed
        // indices flow through the plugin's own executor. The count
        // and hits both come from Lance via LanceKnnFilterTranslator
        // (metadata-only Dataset.countRows for the count, and
        // ScanOptions.filter for the hits' Lance scan), so
        // hits.total.value stays in sync with the number of matching
        // hits regardless of shard state.
        String suffix = "dispatch-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            String matchAllBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match_all\":{}}}"));
            int matchAllHits = extractIntPath(matchAllBody, "hits", "total", "value");
            assertEquals("fragment path match_all must return the true row count", 6, matchAllHits);
            // Default size is 10 so a 6-row table returns all six
            // hits. Each hit carries a synthesised _id in the form
            // "<fragmentId>-<offset>" and a _source rendered from
            // the Arrow batch. The LanceTableFactory fixture puts
            // "hello lance " at even offsets and "quick brown fox"
            // at odd offsets in the body column; both must show
            // up in the response.
            assertTrue("fragment path match_all must populate the hits array: " + matchAllBody, matchAllBody.contains("\"_id\":\"0-0\""));
            assertTrue(
                "fragment path match_all must render _source with the body column: " + matchAllBody,
                matchAllBody.contains("hello lance")
            );
            assertTrue(
                "fragment path match_all must render _source with the id column: " + matchAllBody,
                matchAllBody.contains("\"id\":0")
            );

            // A size=2 request returns only two hits but keeps the
            // total row count at six.
            String sizeBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match_all\":{}},\"size\":2}"));
            assertEquals("size clause must not affect total", 6, extractIntPath(sizeBody, "hits", "total", "value"));
            int returnedHits = countOccurrences(sizeBody, "\"_id\":");
            assertEquals("size=2 must return exactly two hits: " + sizeBody, 2, returnedHits);

            // Term queries on numeric columns are answered by the
            // fragment executor. The count and hit metadata both come
            // from the plugin's own path via
            // LanceKnnFilterTranslator -> Dataset.countRows(sql) +
            // ScanOptions.filter(sql).
            String termBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"term\":{\"id\":3}}}"));
            assertEquals("fragment path term must match exactly one row", 1, extractIntPath(termBody, "hits", "total", "value"));
            assertTrue("fragment path term must return the id=3 hit: " + termBody, termBody.contains("\"_id\":\"0-3\""));
            assertTrue("fragment path term must render the matching row: " + termBody, termBody.contains("\"id\":3"));

            // A numeric range covers three rows (id in {2, 3, 4}).
            String rangeBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"id\":{\"gte\":2,\"lt\":5}}}}"));
            assertEquals("fragment path range must count matching rows", 3, extractIntPath(rangeBody, "hits", "total", "value"));
            assertTrue("fragment path range must include id=2: " + rangeBody, rangeBody.contains("\"id\":2"));
            assertTrue("fragment path range must include id=4: " + rangeBody, rangeBody.contains("\"id\":4"));

            // Bool AND of two filters proves nested translation works
            // end-to-end. id >= 2 intersects id = 3, so the response
            // must count and return exactly the id=3 row.
            String boolBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"bool\":{\"filter\":[{\"range\":{\"id\":{\"gte\":2}}},{\"term\":{\"id\":3}}]}}}"
                )
            );
            assertEquals("fragment path bool must count the intersection", 1, extractIntPath(boolBody, "hits", "total", "value"));
            assertTrue("fragment path bool must return the id=3 hit: " + boolBody, boolBody.contains("\"_id\":\"0-3\""));

            // Full-text match queries also flow through the fragment
            // executor since Stage 3 widened the dispatch filter. The
            // per-node handler translates the QueryBuilder via
            // QueryShardContext.toQuery, gets a LanceFtsQuery from the
            // lance_text field mapper, and drives IndexSearcher.search
            // against the per-fragment reader.
            int matchHits = extractIntPath(
                readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match\":{\"body\":\"lance\"}}}")),
                "hits",
                "total",
                "value"
            );
            assertTrue("match query must return hits on fragment path (got " + matchHits + ")", matchHits > 0);
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAnswersDateRangeQuery() throws Exception {
        // Issue #43: a `range` query with an ISO-8601 string literal
        // ({"gte":"2024-03-01","lt":"2024-04-01"}) on a Lance
        // Timestamp column used to return 400. LanceKnnFilterTranslator
        // emitted a bare Utf8 SQL literal ('2024-03-01') and
        // DataFusion rejected the comparison against a Timestamp
        // column with "could not convert to literal of type
        // 'Timestamp(...)'". The translator now recognises ISO-8601
        // shapes and lifts them into `timestamp '...'` so the same
        // range DSL that works on shard-path date fields also works
        // when the request lands on the fragment executor.
        //
        // Four shapes exercise the fix:
        // (a) date-only literal
        // (b) datetime literal (with T and seconds)
        // (c) bool filter combining a keyword term with a date range
        // (d) date_histogram bucket aggregation consuming the date column
        String suffix = "date-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeDatedTable(scratchDir, tableName);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // (a) Date-only literal: id 2 (2024-03-10) and id 3
            // (2024-03-25) are the two March rows in the fixture, so
            // [2024-03-01, 2024-04-01) picks exactly those two.
            String dateOnly = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"ts\":{\"gte\":\"2024-03-01\",\"lt\":\"2024-04-01\"}}}}")
            );
            assertEquals(
                "date-only range must count only the two March rows: " + dateOnly,
                2,
                extractIntPath(dateOnly, "hits", "total", "value")
            );
            assertTrue("date-only range must include the id=2 hit: " + dateOnly, dateOnly.contains("\"id\":2"));
            assertTrue("date-only range must include the id=3 hit: " + dateOnly, dateOnly.contains("\"id\":3"));

            // (b) Datetime literal: id 1 (2024-02-20) and id 2
            // (2024-03-10) fall inside
            // [2024-02-01T00:00:00Z, 2024-03-15T12:00:00Z). id 3
            // (2024-03-25) sits above the upper bound and must be
            // excluded, proving the timestamp comparison respects
            // sub-day precision.
            String dateTime = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"range\":{\"ts\":{\"gte\":\"2024-02-01T00:00:00Z\",\"lt\":\"2024-03-15T12:00:00Z\"}}}}"
                )
            );
            assertEquals(
                "datetime range must count Feb + early-March rows only: " + dateTime,
                2,
                extractIntPath(dateTime, "hits", "total", "value")
            );
            assertTrue("datetime range must include the id=1 hit: " + dateTime, dateTime.contains("\"id\":1"));
            assertTrue("datetime range must include the id=2 hit: " + dateTime, dateTime.contains("\"id\":2"));

            // (c) bool filter [term category=odd, range ts]: the odd
            // subset is {1, 3, 5}, the date range keeps rows in
            // [2024-01-01, 2024-05-01), and id 5 (2024-05-30) falls
            // outside the upper bound. The intersection is exactly
            // {1, 3}. This proves nested translation still routes the
            // date literal through the timestamp path.
            String boolBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"bool\":{\"filter\":["
                        + "{\"term\":{\"category\":\"odd\"}},"
                        + "{\"range\":{\"ts\":{\"gte\":\"2024-01-01\",\"lt\":\"2024-05-01\"}}}"
                        + "]}}}"
                )
            );
            assertEquals(
                "bool filter must intersect the keyword and date-range subsets: " + boolBody,
                2,
                extractIntPath(boolBody, "hits", "total", "value")
            );
            assertTrue("bool filter must include id=1: " + boolBody, boolBody.contains("\"id\":1"));
            assertTrue("bool filter must include id=3: " + boolBody, boolBody.contains("\"id\":3"));

            // (d) date_histogram on the same column, monthly interval.
            // All six rows contribute: {Jan:1, Feb:1, March:2, April:1,
            // May:1}, so five buckets are opened and the March bucket
            // carries two docs. min_doc_count 1 keeps empty months out.
            String hist = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"aggs\":{\"per_month\":{\"date_histogram\":"
                        + "{\"field\":\"ts\",\"calendar_interval\":\"month\",\"min_doc_count\":1}}}}"
                )
            );
            assertEquals("date_histogram must see every row: " + hist, 6, extractIntPath(hist, "hits", "total", "value"));
            int monthBuckets = countOccurrences(hist, "\"doc_count\":");
            assertEquals("date_histogram must open five monthly buckets: " + hist, 5, monthBuckets);
            assertTrue("date_histogram must carry a bucket with two docs (March): " + hist, hist.contains("\"doc_count\":2"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAnswersDateRangeQueryWithEpochMillisLiterals() throws Exception {
        // Issue #48 (follow-up to #43): a `range` on a Lance
        // Timestamp column with a numeric epoch-millis literal —
        // {"gte": 1709251200000} — used to return 400 with
        // `Received literal Int64(...) and could not convert to
        // literal of type 'Timestamp(...)'` because the translator
        // emitted the number as a bare Int64. The #43 fix only
        // handled ISO-8601 strings because the translator had no
        // mapping context to know whether a numeric literal was
        // meant to be a date or a plain integer.
        //
        // The follow-up plumbs a field-type lookup through
        // {@link org.opensearch.lance.query.LanceKnnFilterTranslator#toLanceSql(QueryBuilder, Function)}
        // so both the coordinator (via
        // {@code TransportLanceCoordinatorAction.resolveTargets} +
        // {@code IndexMetadata.mapping()}) and the knn inner filter
        // ({@code LanceKnnQueryBuilder.doToQuery} via
        // {@code QueryShardContext.fieldMapper}) can tell the
        // translator that a given field is mapped as `date`. When
        // the field is a date, a numeric literal gets wrapped in
        // {@code to_timestamp_millis(...)} so DataFusion coerces
        // to whatever Timestamp unit the Lance column carries.
        //
        // Shapes exercised:
        // (a) range ts with epoch-millis literals only
        // (b) bool filter combining a keyword term with an
        // epoch-millis date range (same intersection as the
        // ISO-8601 variant in #43's IT)
        // (c) regression fence: ISO-8601 string literals still
        // resolve on the mapping-aware path
        String suffix = "dateml-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeDatedTable(scratchDir, tableName);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // Epoch-millis anchor points (UTC midnight, matching what
            // OpenSearch's date field emits from a numeric literal
            // input by default):
            // 2024-03-01T00:00:00Z = 1709251200000
            // 2024-04-01T00:00:00Z = 1711929600000
            // 2024-05-01T00:00:00Z = 1714521600000

            // (a) numeric range: id 2 (2024-03-10) and id 3
            // (2024-03-25) sit inside [1709251200000, 1711929600000),
            // matching the ISO-8601 variant of the same interval in
            // testFragmentDispatchModeAnswersDateRangeQuery.
            String numeric = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"ts\":{\"gte\":1709251200000,\"lt\":1711929600000}}}}")
            );
            assertEquals(
                "numeric range must count only the two March rows: " + numeric,
                2,
                extractIntPath(numeric, "hits", "total", "value")
            );
            assertTrue("numeric range must include id=2: " + numeric, numeric.contains("\"id\":2"));
            assertTrue("numeric range must include id=3: " + numeric, numeric.contains("\"id\":3"));

            // (b) bool + term + numeric range: category=odd narrows
            // to {1, 3, 5}, the date range keeps rows in
            // [2024-01-01, 2024-05-01), and id 5 (2024-05-30) falls
            // outside the upper bound. Intersection: {1, 3}.
            String bool = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"bool\":{\"filter\":["
                        + "{\"term\":{\"category\":\"odd\"}},"
                        + "{\"range\":{\"ts\":{\"gte\":1704067200000,\"lt\":1714521600000}}}"
                        + "]}}}"
                )
            );
            assertEquals("bool + numeric range must intersect: " + bool, 2, extractIntPath(bool, "hits", "total", "value"));
            assertTrue("bool + numeric range must include id=1: " + bool, bool.contains("\"id\":1"));
            assertTrue("bool + numeric range must include id=3: " + bool, bool.contains("\"id\":3"));

            // (c) regression fence: ISO-8601 string variant of (a)
            // must resolve to the same two rows through the
            // mapping-aware translator (the shape heuristic and the
            // mapping-driven branch converge on the same output for
            // this shape).
            String iso = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"ts\":{\"gte\":\"2024-03-01\",\"lt\":\"2024-04-01\"}}}}")
            );
            assertEquals("ISO-8601 regression fence: " + iso, 2, extractIntPath(iso, "hits", "total", "value"));
            assertTrue("ISO regression must include id=2: " + iso, iso.contains("\"id\":2"));
            assertTrue("ISO regression must include id=3: " + iso, iso.contains("\"id\":3"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAnswersQueriesAgainstUnmappedFieldsWithoutError() throws Exception {
        // Issue #50: a range / bool query against a field that
        // derive() left unmapped used to return 500 with
        // `illegal_state_exception: Rewrite first`. The exception
        // comes from RangeQueryBuilder.doToQuery reaching for a
        // MappedFieldType that is null; the shard path never hits
        // it because SearchService.parseSource calls
        // Rewriteable.rewrite before toQuery, which folds an
        // unmapped range into MatchNoneQueryBuilder via
        // RangeQueryBuilder.doRewrite. The fragment executor
        // skipped that rewrite step.
        //
        // After the fix (a) TransportLanceFragmentQueryAction
        // rewrites request.query() and request.postFilter() before
        // handing them to toQuery, and (b) the coordinator's
        // resolveFilterSql refuses to emit Lance SQL when any leaf
        // names an unmapped field so Dataset.countRows(sql) does
        // not surface a second 500 from the count path. Behaviour
        // now matches the shard path: 200 with 0 hits, no error.
        //
        // Shapes exercised:
        // (a) `range unmapped_int {gte:1}` — pure unmapped range,
        // which is the exact repro from the issue.
        // (b) `range unmapped_str {gte:"aa"}` — string-shaped
        // range against an unmapped field (regression fence
        // for the ISO-8601 shape-heuristic branch in the
        // translator's literal encoder).
        // (c) `bool must [term body="alpha", range unmapped_int]`
        // — nested case where the coordinator's
        // hasUnmappedField walker has to recurse into the
        // bool tree, and Rewriteable.rewrite on the per-node
        // side has to fold the range clause inside the bool.
        // (d) Regression fence: a range against the mapped `id`
        // column still returns the correct hit set on the
        // same index (no over-broad match-none rewrite).
        String suffix = "unmapped-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 4);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // (a) numeric range on unmapped field
            String numeric = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"unmapped_int\":{\"gte\":1}}}}"));
            assertEquals("unmapped range must return 0 hits: " + numeric, 0, extractIntPath(numeric, "hits", "total", "value"));

            // (b) string range on unmapped field. Also exercises
            // the branch in LanceKnnFilterTranslator.literal where
            // a shape-heuristic ISO-8601 detection would fire on a
            // string literal; the coordinator's hasUnmappedField
            // walker skips translation before we get there.
            String stringRange = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"unmapped_str\":{\"gte\":\"aa\"}}}}")
            );
            assertEquals(
                "unmapped string range must return 0 hits: " + stringRange,
                0,
                extractIntPath(stringRange, "hits", "total", "value")
            );

            // (c) bool must with one mapped and one unmapped
            // clause. RangeQueryBuilder.doRewrite folds the
            // unmapped range to MatchNone, then
            // BoolQueryBuilder.doRewrite collapses the whole
            // bool to a query that matches nothing. Both the
            // per-node hits path and the count path have to see
            // this or hits.total.value would collapse to only
            // the term-clause matches.
            String bool = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"bool\":{\"must\":["
                        + "{\"term\":{\"body\":\"alpha\"}},"
                        + "{\"range\":{\"unmapped_int\":{\"gte\":1}}}"
                        + "]}}}"
                )
            );
            assertEquals("bool must with unmapped clause must return 0 hits: " + bool, 0, extractIntPath(bool, "hits", "total", "value"));

            // (d) regression fence: range on the mapped `id`
            // column still resolves normally on the same index.
            // Ensures the rewrite step does not accidentally
            // treat mapped fields as MatchNone.
            String mapped = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"id\":{\"gte\":1,\"lt\":3}}},\"size\":10}")
            );
            assertEquals("mapped range must return 2 hits: " + mapped, 2, extractIntPath(mapped, "hits", "total", "value"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeCountsFtsHitsWithoutWeightMaterialisation() throws Exception {
        // triggered LanceFtsQuery's Weight to fully materialise every
        // hit's row address and score into the sparse buffer. On a 20M
        // row table with 500k matching hits QA measured 4.6 s for a
        // count-only query that pylance answered in <2 ms because
        // Lance's inverted-index scan can stream row counts when we
        // ask for zero columns. This test fences that count path:
        //
        // - lance_match size:0 must return the exact match count
        // (no clip, no over-count) for match / phrase / bool
        // - post_filter present + size:0 must fall through to the
        // Weight path (post_filter narrowing needs Lucene)
        // - non-FTS scoring shapes (knn) continue on the Weight
        // path since Lance has no countRows(NearestQuery)
        String suffix = "ftscount-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        // 20 rows: 10 with "hello lance N" (even ids), 10 with "quick
        // brown fox N" (odd ids). "lance" hits 10 rows, "hello lance"
        // as a phrase hits the same 10 rows.
        LanceTableFactory.writeTable(scratchDir, tableName, 20);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // Simple lance_match count-only: total = 10, hits empty.
            String matchBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match\":{\"body\":\"lance\"}},\"size\":0}"));
            assertEquals("match size:0 total must be 10", 10, extractIntPath(matchBody, "hits", "total", "value"));
            assertEquals("size:0 must return no hits", 0, countOccurrences(matchBody, "\"_id\":"));

            // lance_match_phrase count-only: same 10 rows contain
            // "hello lance N" so the phrase count matches the
            // simple match count.
            String phraseBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_match_phrase\":{\"field\":\"body\",\"query\":\"hello lance\"}},\"size\":0}"
                )
            );
            assertEquals("phrase size:0 total must be 10", 10, extractIntPath(phraseBody, "hits", "total", "value"));

            // Zero-match count: query never touches the fixture so
            // the count path must still return 0 (regression fence
            // against the Weight path failing when the FTS scan
            // yields empty batches).
            String zeroBody = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"match\":{\"body\":\"nonexistent\"}},\"size\":0}")
            );
            assertEquals("no-match size:0 total must be 0", 0, extractIntPath(zeroBody, "hits", "total", "value"));

            // Post-filter forces the Weight fallback because the
            // post_filter narrows below what Dataset.countRows
            // would report. The fixture pins ids 0..19 sequential,
            // so id >= 10 keeps five "hello lance" rows (10, 12, 14,
            // 16, 18) and drops the rest. When Weight-fallback works
            // correctly total = 5, hits empty (size:0).
            String postFilterBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"match\":{\"body\":\"lance\"}}," + "\"post_filter\":{\"range\":{\"id\":{\"gte\":10}}}," + "\"size\":0}"
                )
            );
            assertEquals(
                "match + post_filter size:0 must narrow to 5 via Weight fallback",
                5,
                extractIntPath(postFilterBody, "hits", "total", "value")
            );
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAppliesTopKPushdownForFtsHits() throws Exception {
        // Issue #42 Phase B: pure FTS (lance_match / lance_match_phrase
        // as the top-level query) with no sort, no aggregation, and no
        // post_filter must push size into the per-fragment Lance scan
        // as `limit(size)`. Lance's inverted-index scorer holds a
        // bounded score-sorted heap, so a size:5 request against a
        // large hit set never has to transfer or rank 4+ orders of
        // magnitude of rows the client will not look at.
        //
        // Regression fence covers: hits stay correct up to the
        // requested size, hits.total.value keeps returning the true
        // count from LanceFtsQuery's Weight (because that Weight runs
        // once per size:0 count call and the scan limit is disabled
        // there), and shapes that must NOT enable the pushdown (sort
        // by non-score field / agg) keep serving the full matched set.
        String suffix = "ftstop-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        // 20 rows: 10 with "hello lance N" (even ids), 10 with "quick
        // brown fox N" (odd ids). The "lance" match yields 10 hits.
        LanceTableFactory.writeTable(scratchDir, tableName, 20);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // Match "lance" hits 10 rows; size:3 must clip hits to 3
            // while hits.total.value stays at 10. If the clip leaked
            // into the total, the count would drop to 3.
            String matchBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match\":{\"body\":\"lance\"}},\"size\":3}"));
            assertEquals("match size:3 total must be 10", 10, extractIntPath(matchBody, "hits", "total", "value"));
            int matchReturnedHits = countOccurrences(matchBody, "\"_id\":");
            assertEquals("match size:3 must return three hits: " + matchBody, 3, matchReturnedHits);

            // Match with a size larger than the matched set must
            // return every match (no clip, no padding).
            String allMatchBody = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"match\":{\"body\":\"lance\"}},\"size\":50}")
            );
            assertEquals("match size:50 total must be 10", 10, extractIntPath(allMatchBody, "hits", "total", "value"));
            int allMatchReturnedHits = countOccurrences(allMatchBody, "\"_id\":");
            assertEquals("match size:50 must return ten hits: " + allMatchBody, 10, allMatchReturnedHits);

            // size:0 count-only: hits stays empty, total reflects the
            // full match (served by IndexSearcher.count on the FTS
            // Weight without the top-k enabled).
            String countBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match\":{\"body\":\"lance\"}},\"size\":0}"));
            assertEquals("match size:0 total must be 10", 10, extractIntPath(countBody, "hits", "total", "value"));
            int countReturnedHits = countOccurrences(countBody, "\"_id\":");
            assertEquals("size:0 must return no hits", 0, countReturnedHits);

            // Sort by a non-score field must disable the top-k
            // pushdown: even at size:3, we need every match so sort
            // by id desc can pick the largest id (the row with "hello
            // lance 18" at id=18 must come first for the "lance"
            // match on even ids).
            String sortBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"match\":{\"body\":\"lance\"}},\"size\":3,\"sort\":[{\"id\":\"desc\"}]}"
                )
            );
            assertEquals("match sort size:3 total must be 10", 10, extractIntPath(sortBody, "hits", "total", "value"));
            int sortReturnedHits = countOccurrences(sortBody, "\"_id\":");
            assertEquals("match sort size:3 must return three hits: " + sortBody, 3, sortReturnedHits);
            assertTrue("match sort desc must put id=18 first: " + sortBody, sortBody.contains("\"id\":18"));

            // Aggregation must disable the top-k pushdown: the sum of
            // even ids 0..18 is 0+2+4+6+8+10+12+14+16+18 = 90. If the
            // scan were clipped to 3, the sum would be < 90.
            String aggBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"match\":{\"body\":\"lance\"}},\"size\":3,\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}"
                )
            );
            assertEquals("match agg size:3 total must be 10", 10, extractIntPath(aggBody, "hits", "total", "value"));
            assertEquals("sum(id) over match must be 90", 90.0d, extractDoublePath(aggBody, "aggregations", "s", "value"), 0.0d);

            // lance_match_phrase should follow the same pushdown path.
            // "hello lance" matches all 10 even-id rows exactly.
            String phraseBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_match_phrase\":{\"field\":\"body\",\"query\":\"hello lance\"}},\"size\":4}"
                )
            );
            assertEquals("phrase size:4 total must be 10", 10, extractIntPath(phraseBody, "hits", "total", "value"));
            int phraseReturnedHits = countOccurrences(phraseBody, "\"_id\":");
            assertEquals("phrase size:4 must return four hits: " + phraseBody, 4, phraseReturnedHits);
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAppliesTopKPushdownForScalarFilterHits() throws Exception {
        // Issue #42 Phase A: pure scalar filter (term / range / bool
        // built from LanceKnnFilterTranslator-translatable clauses)
        // with no sort, no aggregation, and no post_filter must
        // push size into the per-fragment Lance scan as `limit(size)`.
        // The regression fence covers: hits stay correct up to the
        // requested size, hits.total.value keeps returning the true
        // count from Dataset.countRows(sql), and shapes that must
        // NOT enable the pushdown (sort / agg) keep serving the full
        // matched set.
        String suffix = "topk-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        // 20 rows so a size:5 request exercises the top-k clip while
        // still leaving enough distinct rows for the count assertion.
        LanceTableFactory.writeTable(scratchDir, tableName, 20);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // Range covers 20 rows; size:5 must clip hits to 5 while
            // hits.total.value stays at 20. If the clip leaks into
            // Dataset.countRows the total would drop to 5.
            String rangeBody = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"id\":{\"gte\":0,\"lt\":20}}},\"size\":5}")
            );
            assertEquals("range size:5 total must be 20", 20, extractIntPath(rangeBody, "hits", "total", "value"));
            int rangeReturnedHits = countOccurrences(rangeBody, "\"_id\":");
            assertEquals("range size:5 must return five hits: " + rangeBody, 5, rangeReturnedHits);

            // Term narrowing to a single row must still return that
            // row even when size:5 is more than the matched count.
            String termBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"term\":{\"id\":3}},\"size\":5}"));
            assertEquals("term id=3 total must be 1", 1, extractIntPath(termBody, "hits", "total", "value"));
            assertTrue("term id=3 must return the id=3 hit: " + termBody, termBody.contains("\"_id\":\"0-3\""));

            // size:0 count-only: hits stays empty, total reflects the
            // full match (served by Dataset.countRows(sql) directly).
            String countBody = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"id\":{\"gte\":0,\"lt\":20}}},\"size\":0}")
            );
            assertEquals("range size:0 total must be 20", 20, extractIntPath(countBody, "hits", "total", "value"));
            int countReturnedHits = countOccurrences(countBody, "\"_id\":");
            assertEquals("size:0 must return no hits", 0, countReturnedHits);

            // Sort must disable the top-k pushdown: even at size:5 we
            // need the full matched set to sort by id desc, and the
            // first hit must be id=19 (max id in the range).
            String sortBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"range\":{\"id\":{\"gte\":0,\"lt\":20}}},\"size\":5,\"sort\":[{\"id\":\"desc\"}]}"
                )
            );
            assertEquals("sort size:5 total must be 20", 20, extractIntPath(sortBody, "hits", "total", "value"));
            int sortReturnedHits = countOccurrences(sortBody, "\"_id\":");
            assertEquals("sort size:5 must return five hits: " + sortBody, 5, sortReturnedHits);
            assertTrue("sort desc must put id=19 first: " + sortBody, sortBody.contains("\"id\":19"));
            assertTrue("sort desc must put id=15 last: " + sortBody, sortBody.contains("\"id\":15"));

            // Aggregation must disable the top-k pushdown: sum over
            // the full range is 0 + 1 + ... + 19 = 190. If the scan
            // were clipped to 5, the sum would be < 190.
            String aggBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"range\":{\"id\":{\"gte\":0,\"lt\":20}}},\"size\":5,\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}"
                )
            );
            assertEquals("agg size:5 total must be 20", 20, extractIntPath(aggBody, "hits", "total", "value"));
            assertEquals("sum(id) over range must be 190", 190.0d, extractDoublePath(aggBody, "aggregations", "s", "value"), 0.0d);
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testStoredFieldsDocValueFieldsExplainFallThroughToShardPath() throws Exception {
        // stored_fields, docvalue_fields, and explain used to slip
        // past isDispatchable and produce silently wrong hit
        // envelopes (issue #37 case 4 remainder). Fragment
        // executor drops all three: stored_fields projection is
        // ignored so _source stays in hits (including when
        // "_none_" asks to hide it entirely), docvalue_fields is
        // never populated into hits.fields, and explain never
        // adds the _explanation field. Reject list now sends
        // each of these shapes to the shard path where the
        // built-in fetch phase applies the projection and
        // synthesises the explanation.
        String suffix = "s3-storedfields-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 4);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // stored_fields: "_none_" hides _source entirely on
            // the shard path. Fragment path used to always
            // materialise _source from the Lance row scan.
            String noneBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":1,\"query\":{\"match_all\":{}},\"stored_fields\":\"_none_\"}")
            );
            assertFalse("stored_fields:_none_ should suppress _source: " + noneBody, noneBody.contains("\"_source\""));

            // docvalue_fields projects doc values into hits.fields.
            // Fragment path used to omit hits.fields entirely.
            String docvalueBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":1,\"query\":{\"match_all\":{}},\"docvalue_fields\":[\"id\"]}")
            );
            assertTrue(
                "docvalue_fields should populate hits.fields on the shard path: " + docvalueBody,
                docvalueBody.contains("\"fields\":{\"id\"")
            );

            // explain: true adds a per-hit _explanation with a
            // scoring breakdown on the shard path. Fragment path
            // used to never call searcher.explain, so the field
            // was missing.
            String explainBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":1,\"query\":{\"match_all\":{}},\"explain\":true}")
            );
            assertTrue("explain:true should add _explanation on the shard path: " + explainBody, explainBody.contains("\"_explanation\""));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testMinScoreTerminateAfterTrackTotalHitsFallThroughToShardPath() throws Exception {
        // min_score, terminate_after, and track_total_hits used to
        // slip past isDispatchable and produce silently wrong
        // response envelopes (issue #37 cases 1 and 3). Fragment
        // path counts matches from Lance metadata (or Lucene
        // count()) without threading these knobs through, so the
        // request would come back with hits.total.value from the
        // full match set and terminated_early=false, even when
        // the caller asked for a tighter answer. Reject list now
        // sends each of these shapes to the shard path where the
        // built-in MinScoreCollector /
        // EarlyTerminatingCollector / total-hits-up-to gate
        // actually clip.
        String suffix = "s3-reject-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // min_score above 1.0 excludes every hit from a
            // match_all query (all hits carry score 1.0). Fragment
            // path used to return total=6; shard path clips to
            // total=0.
            String minScoreBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"match_all\":{}},\"min_score\":2.0}")
            );
            assertEquals(0, extractIntPath(minScoreBody, "hits", "total", "value"));

            // terminate_after=2 tells the collector to stop after
            // two docs per segment. Shard path signals early
            // termination through terminated_early=true; fragment
            // path omits the flag entirely because it never wired
            // the count through its scan. Shard path leaves
            // hits.total.value as the pre-terminate count, so we
            // only look at the flag rather than total.
            String terminateBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"match_all\":{}},\"terminate_after\":2}")
            );
            assertTrue(
                "terminate_after should set terminated_early=true on the shard path: " + terminateBody,
                terminateBody.contains("\"terminated_early\":true")
            );

            // track_total_hits=3 on a 6-row table produces
            // relation=gte with a value at the shard path
            // early-terminated counter. Fragment path used to
            // return relation="eq" with the Lance metadata count.
            String trackBoundBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"match_all\":{}},\"track_total_hits\":3}")
            );
            assertTrue("track_total_hits:3 should return relation=gte: " + trackBoundBody, trackBoundBody.contains("\"relation\":\"gte\""));

            // track_total_hits=false omits hits.total entirely on
            // the shard path. Fragment path used to always return
            // the exact Lance total, ignoring the flag. Assert on
            // the response envelope shape rather than the counter
            // value because the two paths disagree on the shape.
            String trackFalseBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"match_all\":{}},\"track_total_hits\":false}")
            );
            assertFalse("track_total_hits:false should omit hits.total: " + trackFalseBody, trackFalseBody.contains("\"total\":{"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeStampsIndexAndVersionEnvelope() throws Exception {
        // The response envelope should carry _index on every hit
        // regardless of what the request asked for, and _version /
        // _seq_no / _primary_term when the request opted in via
        // `version` / `seq_no_primary_term`. Fragment path used to
        // omit all four because it built SearchHit objects on the
        // per-node executor without a SearchShardTarget and without
        // per-doc version accounting; the coordinator now stamps
        // them on absorbTargetResponses.
        String suffix = "s3-env-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 4);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // Plain _search: _index must be present on every hit,
            // version / seq_no / primary_term must NOT (defaults are
            // off).
            String plain = readAll(postJson("/" + indexName + "/_search", "{\"size\":2,\"query\":{\"match_all\":{}}}"));
            assertTrue("expected _index=[" + indexName + "] on each hit: " + plain, plain.contains("\"_index\":\"" + indexName + "\""));
            assertFalse("_version must be omitted by default: " + plain, plain.contains("\"_version\""));
            assertFalse("_seq_no must be omitted by default: " + plain, plain.contains("\"_seq_no\""));
            assertFalse("_primary_term must be omitted by default: " + plain, plain.contains("\"_primary_term\""));

            // version: true opts _version in, but not seq_no /
            // primary_term. Fragment path has no per-doc version so
            // the constant value 1 is reported.
            String versioned = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":1,\"query\":{\"match_all\":{}},\"version\":true}")
            );
            assertTrue("expected _index on each hit: " + versioned, versioned.contains("\"_index\":\"" + indexName + "\""));
            assertTrue("expected _version=1 when version:true: " + versioned, versioned.contains("\"_version\":1"));
            assertFalse("_seq_no still off: " + versioned, versioned.contains("\"_seq_no\""));

            // seq_no_primary_term: true opts _seq_no and
            // _primary_term in but leaves _version off. Constants
            // seqNo=0 / primaryTerm=1 match the shard path defaults
            // for a freshly-indexed doc.
            String seqno = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":1,\"query\":{\"match_all\":{}},\"seq_no_primary_term\":true}")
            );
            assertTrue("expected _seq_no=0: " + seqno, seqno.contains("\"_seq_no\":0"));
            assertTrue("expected _primary_term=1: " + seqno, seqno.contains("\"_primary_term\":1"));
            assertFalse("_version still off: " + seqno, seqno.contains("\"_version\""));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAnswersFromPagination() throws Exception {
        // from > 0 used to fall through to the shard path because the
        // coordinator merge did not know how to skip. Now the
        // coordinator asks each per-node executor for `from + size`
        // hits and drops the leading `from` from the merged response,
        // so pagination beyond the first page runs through the
        // fragment executor end-to-end.
        String suffix = "s3-from-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // Sort by id descending, request the third page window
            // (from=2, size=2). Full descending order is [5, 4, 3,
            // 2, 1, 0]; skipping two leaves [3, 2, 1, 0] and size=2
            // clips to [3, 2].
            String body = readAll(
                postJson("/" + indexName + "/_search", "{\"from\":2,\"size\":2,\"query\":{\"match_all\":{}},\"sort\":[{\"id\":\"desc\"}]}")
            );
            assertEquals(6, extractIntPath(body, "hits", "total", "value"));
            assertTrue("expected sort value [3] on the first paged hit: " + body, body.contains("\"sort\":[3]"));
            assertTrue("expected sort value [2] on the second paged hit: " + body, body.contains("\"sort\":[2]"));
            assertFalse("hit sort value [5] must have been skipped by from=2: " + body, body.contains("\"sort\":[5]"));
            assertFalse("hit sort value [4] must have been skipped by from=2: " + body, body.contains("\"sort\":[4]"));
            assertFalse("only two hits should remain after size=2: " + body, body.contains("\"sort\":[1]"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAnswersPostFilter() throws Exception {
        // post_filter narrows hits (and hits.total.value) but leaves
        // aggregations unaffected. Fragment path runs aggregations
        // against the top-level query and applies the post_filter
        // to the hits scan on the per-node executor.
        String suffix = "s3-pf-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // Query matches all 6 rows, post_filter narrows to id
            // >= 4 (rows 4 and 5). The aggregation counts the full
            // 6 rows because post_filter must not influence it.
            String body = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":10,\"query\":{\"match_all\":{}},"
                        + "\"post_filter\":{\"range\":{\"id\":{\"gte\":4}}},"
                        + "\"aggs\":{\"total\":{\"value_count\":{\"field\":\"id\"}}}}"
                )
            );
            assertEquals(2, extractIntPath(body, "hits", "total", "value"));
            assertEquals(6, extractIntPath(body, "aggregations", "total", "value"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAnswersSearchAfter() throws Exception {
        // search_after pagination flows through the fragment path
        // when the request also carries a sort. The per-node
        // executor calls IndexSearcher.searchAfter(FieldDoc, size,
        // sort) with the coordinator-forwarded cursor; the merged
        // response contains only the hits after the cursor value.
        String suffix = "s3-sa-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // First page: sort id desc, size=2. Full descending
            // order is [5, 4, 3, 2, 1, 0]; the first page keeps 5
            // and 4.
            String first = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":2,\"query\":{\"match_all\":{}},\"sort\":[{\"id\":\"desc\"}]}")
            );
            assertTrue("expected first page to include sort value [5]: " + first, first.contains("\"sort\":[5]"));
            assertTrue("expected first page to include sort value [4]: " + first, first.contains("\"sort\":[4]"));

            // Second page via search_after: the cursor is the last
            // sort value from the first page. Expect ids 3 and 2.
            String second = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":2,\"query\":{\"match_all\":{}},\"sort\":[{\"id\":\"desc\"}],\"search_after\":[4]}"
                )
            );
            assertTrue("expected second page to include sort value [3]: " + second, second.contains("\"sort\":[3]"));
            assertTrue("expected second page to include sort value [2]: " + second, second.contains("\"sort\":[2]"));
            assertFalse("search_after cursor value [4] must be excluded: " + second, second.contains("\"sort\":[4]"));
            assertFalse("hit above the cursor must be excluded: " + second, second.contains("\"sort\":[5]"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAnswersScriptQueryAndScriptSort() throws Exception {
        // Two shapes flow through the fragment path without any
        // explicit plumbing because the per-fragment reader already
        // exposes doc values that scripts consume through the
        // standard DocValues API. If this test starts failing, the
        // shape has to move onto the isDispatchable reject list (or
        // the fragment executor has to grow the missing piece).
        //
        // collapse and rescore used to live in this test as well
        // but they are silent no-ops on the fragment path (collapse
        // returns ungrouped hits, rescore leaves first-pass scores
        // untouched), so they now sit on the isDispatchable reject
        // list and go through the standard shard path instead.
        String suffix = "s3-scq-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // id > 2 -> rows 3, 4, 5.
            String body = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":10,\"query\":{\"script\":{\"script\":{\"source\":\"doc['id'].value > 2\"}}}}"
                )
            );
            assertEquals(3, extractIntPath(body, "hits", "total", "value"));

            // Same doc value path also drives script sort. Rows
            // 5..0 in id desc order.
            String sortBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":3,\"query\":{\"match_all\":{}},"
                        + "\"sort\":[{\"_script\":{\"script\":{\"source\":\"doc['id'].value\"},\"type\":\"number\",\"order\":\"desc\"}}]}"
                )
            );
            assertEquals(6, extractIntPath(sortBody, "hits", "total", "value"));
            assertTrue("script sort first hit should carry sort value 5: " + sortBody, sortBody.contains("\"sort\":[5"));
            assertTrue("script sort second hit should carry sort value 4: " + sortBody, sortBody.contains("\"sort\":[4"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testDefaultSearchReturnsAtLeastTenHits() throws Exception {
        // Regression for the FetchPhase sequential-stored-fields path: with
        // >= 10 adjacent doc ids and no deletions the fetch phase calls
        // getSequentialStoredFieldsReader on the leaf reader. Before the
        // LanceSequentialLeafReader wrapper this threw "requires a
        // CodecReader or a SequentialStoredFieldsLeafReader", so GET
        // /demo/_search with the default size=10 returned 500. Sixteen rows
        // exercises the >= 10 hits case; every hit must materialise cleanly.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "defaultsearch")) {
            String indexName = fixture.indexName();

            Response search = client().performRequest(new Request("GET", "/" + indexName + "/_search"));
            String body = readAll(search);
            int totalHits = extractIntPath(body, "hits", "total", "value");
            assertEquals("expected 16 total hits, saw response: " + body, 16, totalHits);
            // Default size is 10; hits array must be full and each entry
            // must carry the Lance-backed _source.
            assertTrue("expected hits[0]._source in response, saw: " + body, body.contains("\"_source\""));
        }
    }
}
