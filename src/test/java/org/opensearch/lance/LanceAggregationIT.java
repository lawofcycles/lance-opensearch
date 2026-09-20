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
 * Aggregations on the fragment dispatch path: metric aggregations, terms
 * with deferred sub-aggregations, filter pushdown into column
 * materialisation, and the empty-table response shape.
 */
public class LanceAggregationIT extends LanceRestTestCase {

    public void testFragmentDispatchModeAnswersMetricAggregations() throws Exception {
        // Milestone 5-B of the shard-free dispatch prototype: setting
        // lance.dispatch.mode = fragment must let the plugin's own
        // executor answer the five metric aggregations
        // LanceMetricAggregator supports (value_count, sum, avg, min,
        // max), possibly combined with a filter query. The count,
        // hits, and aggregation branches all share the same filter
        // push-down, so a filter query narrows the aggregation the
        // same way it narrows the count. Bucket aggregations, script
        // metrics, and multi-index aggregation continue to route
        // through the standard shard fan-out.
        String suffix = "aggs-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // The fixture writes six rows with id = 0..5, so the
            // canonical aggregates are: count = 6, sum = 15,
            // avg = 2.5, min = 0, max = 5. size=0 is standard for
            // aggregation-only requests and avoids paying the
            // hits scan on the same request.
            String body = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"aggs\":{"
                        + "\"c\":{\"value_count\":{\"field\":\"id\"}},"
                        + "\"s\":{\"sum\":{\"field\":\"id\"}},"
                        + "\"a\":{\"avg\":{\"field\":\"id\"}},"
                        + "\"m\":{\"min\":{\"field\":\"id\"}},"
                        + "\"M\":{\"max\":{\"field\":\"id\"}}"
                        + "}}"
                )
            );
            assertEquals("aggregation must count matching rows via Dataset.countRows", 6, extractIntPath(body, "hits", "total", "value"));
            assertEquals("value_count on id must equal row count", 6, extractIntPath(body, "aggregations", "c", "value"));
            assertEquals("sum(id) 0..5 == 15", 15.0d, extractDoublePath(body, "aggregations", "s", "value"), 0.0d);
            assertEquals("avg(id) 0..5 == 2.5", 2.5d, extractDoublePath(body, "aggregations", "a", "value"), 0.0d);
            assertEquals("min(id) == 0", 0.0d, extractDoublePath(body, "aggregations", "m", "value"), 0.0d);
            assertEquals("max(id) == 5", 5.0d, extractDoublePath(body, "aggregations", "M", "value"), 0.0d);

            // Filter query pushes the same SQL predicate into
            // Dataset.countRows and ScanOptions.filter, so
            // sum(id where id >= 2) must equal 2+3+4+5 = 14 with
            // total = 4.
            String filteredBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"query\":{\"range\":{\"id\":{\"gte\":2}}}," + "\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}"
                )
            );
            assertEquals("filtered aggregation must count filtered rows", 4, extractIntPath(filteredBody, "hits", "total", "value"));
            assertEquals("sum(id) with id>=2 == 14", 14.0d, extractDoublePath(filteredBody, "aggregations", "s", "value"), 0.0d);

            // Hits + aggregation in the same request must both
            // come from the fragment executor: hits carry the
            // synthesised _rowaddr id, aggregations carry the
            // computed value.
            String hitsPlusAggs = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":2,\"query\":{\"match_all\":{}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}"
                )
            );
            assertEquals("total unchanged when hits also requested", 6, extractIntPath(hitsPlusAggs, "hits", "total", "value"));
            assertTrue("hits section must carry the synthesised _id: " + hitsPlusAggs, hitsPlusAggs.contains("\"_id\":\"0-0\""));
            assertEquals(
                "sum unchanged when hits also requested",
                15.0d,
                extractDoublePath(hitsPlusAggs, "aggregations", "s", "value"),
                0.0d
            );

            // Direction 1 Stage 2: terms bucket aggregation
            // now runs on the fragment executor via the stock
            // TermsAggregator against per-fragment
            // LanceFragmentLeafReaders. The response must carry
            // one bucket per distinct id value (6 rows, all
            // unique ids 0..5 = 6 buckets, doc_count=1 each).
            String bucketBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":0,\"aggs\":{\"by_id\":{\"terms\":{\"field\":\"id\",\"size\":10}}}}")
            );
            assertTrue("terms aggregation via fragment path carries buckets: " + bucketBody, bucketBody.contains("\"buckets\":"));
            assertEquals(6, extractIntPath(bucketBody, "hits", "total", "value"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAnswersTermsWithDeferredMetricSubAggregation() throws Exception {
        // Regression for issue #40: `terms` with a deferred metric
        // sub-aggregation (`avg` / `sum` / `max`) or a nested `terms`
        // used to return 500 `Already been replayed` on the fragment
        // path because the executor invoked postCollection() and
        // buildAggregations() manually after
        // ContextIndexSearcher.search had already run
        // BucketCollectorProcessor#processPostCollection. The second
        // pass drove BestBucketsDeferringCollector#prepareSelectedBuckets
        // a second time, which is what throws. Fix reads the built
        // aggregations back through Aggregator#getPostCollectionAggregation,
        // matching the shard path.
        //
        // Shapes covered: default collect_mode (breadth_first is the
        // default when a metric sub-agg is present, and it is the shape
        // that triggers the defer path). Sibling coverage without
        // pipeline aggregations (Phase 1a rejects the pipeline shape).
        String suffix = "tds-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // terms + avg (issue #40 primary reproducer). 6 rows with
            // unique ids 0..5 yields 6 buckets each holding a single
            // doc; avg over each bucket equals the id itself. Also
            // check the bucket for id=0 to confirm sub-agg values
            // actually round trip (the pre-fix path threw before
            // reaching sub-agg serialisation).
            String avgBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"aggs\":{\"by_id\":{\"terms\":{\"field\":\"id\",\"size\":10},"
                        + "\"aggs\":{\"a\":{\"avg\":{\"field\":\"id\"}}}}}}"
                )
            );
            assertEquals("terms + avg total unchanged", 6, extractIntPath(avgBody, "hits", "total", "value"));
            assertTrue("terms + avg carries buckets: " + avgBody, avgBody.contains("\"buckets\":"));
            assertTrue("terms + avg carries sub-agg value: " + avgBody, avgBody.contains("\"a\":{\"value\":0.0}"));

            // terms + sum (same defer path, different metric).
            String sumBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"aggs\":{\"by_id\":{\"terms\":{\"field\":\"id\",\"size\":10},"
                        + "\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}}}"
                )
            );
            assertEquals("terms + sum total unchanged", 6, extractIntPath(sumBody, "hits", "total", "value"));

            // terms + max (same defer path).
            String maxBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"aggs\":{\"by_id\":{\"terms\":{\"field\":\"id\",\"size\":10},"
                        + "\"aggs\":{\"m\":{\"max\":{\"field\":\"id\"}}}}}}"
                )
            );
            assertEquals("terms + max total unchanged", 6, extractIntPath(maxBody, "hits", "total", "value"));

            // terms + nested terms (also defer-driven; QA also
            // reproduced 500 with this shape).
            String nestedBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"aggs\":{\"by_id\":{\"terms\":{\"field\":\"id\",\"size\":10},"
                        + "\"aggs\":{\"nest\":{\"terms\":{\"field\":\"id\",\"size\":10}}}}}}"
                )
            );
            assertEquals("nested terms total unchanged", 6, extractIntPath(nestedBody, "hits", "total", "value"));

            // depth_first should have kept working before the fix
            // (defer is skipped). Include it so a regression that
            // breaks depth_first is caught too.
            String depthBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"aggs\":{\"by_id\":{\"terms\":{\"field\":\"id\",\"size\":10,\"collect_mode\":\"depth_first\"},"
                        + "\"aggs\":{\"a\":{\"avg\":{\"field\":\"id\"}}}}}}"
                )
            );
            assertEquals("depth_first terms + avg total unchanged", 6, extractIntPath(depthBody, "hits", "total", "value"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAppliesFilterPushdownDuringColumnMaterialisation() throws Exception {
        // Issue #42 Phase C / Step C-1: fragment path now passes the
        // top-level Lance SQL filter through to LanceFragmentLeafReader,
        // and every per-column ensureXxxLoaded scan layers the same
        // filter into ScanOptions before asking Lance for values.
        //
        // Before the fix, `filter + sum(x)` on a 20M row fragment
        // materialised every value of `x` even when the filter kept
        // 5% of rows, because the ensureNumericLoaded scan issued a
        // full-column scan and let the Weight side drop non-matching
        // rows after the fact. Correctness stayed intact but the
        // load side did the work the filter was supposed to prune.
        //
        // This test does not measure timing — it fences the correctness
        // side, so a follow-up refactor cannot silently drop matches
        // when the filter is pushed down. The bucket / metric values
        // must still equal the unfiltered aggregation restricted to
        // matching rows.
        String suffix = "fcpc-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        // Six rows: id 0..5, body alternating "hello lance i" (even)
        // and "quick brown fox i" (odd). Numeric column `id`
        // exercises numeric doc values; `body` (indexed as
        // lance_text with a keyword sub-field) exercises keyword doc
        // values and, indirectly, the terms-agg ordinal path.
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson(
                "/_lance/attach",
                // Declare body.raw as a keyword sub-field so the terms
                // aggregation cases below (b, c, d) can key on it.
                // Without the multi_fields clause the sub-field
                // resolves to nothing and the terms bucket silently
                // returns zero buckets.
                "{\"table\":\"" + tableUri + "\",\"multi_fields\":{\"body\":{\"raw\":{\"type\":\"keyword\"}}}}"
            );
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // (a) filter + metric aggs: id >= 2 keeps {2, 3, 4, 5}.
            // sum = 14, avg = 3.5, min = 2, max = 5, value_count = 4.
            // Every one of those values is served from
            // numericColumns.get("id") which was populated by a
            // scan that carried filterSql = "(id >= 2)"; the assert
            // catches any regression where the filter fails to
            // route to the leaf reader.
            String metricBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"query\":{\"range\":{\"id\":{\"gte\":2}}},\"aggs\":{"
                        + "\"c\":{\"value_count\":{\"field\":\"id\"}},"
                        + "\"s\":{\"sum\":{\"field\":\"id\"}},"
                        + "\"a\":{\"avg\":{\"field\":\"id\"}},"
                        + "\"m\":{\"min\":{\"field\":\"id\"}},"
                        + "\"M\":{\"max\":{\"field\":\"id\"}}"
                        + "}}"
                )
            );
            assertEquals("filter + metric aggs must count matching rows", 4, extractIntPath(metricBody, "hits", "total", "value"));
            assertEquals("value_count with id>=2 == 4", 4, extractIntPath(metricBody, "aggregations", "c", "value"));
            assertEquals("sum(id) with id>=2 == 14", 14.0d, extractDoublePath(metricBody, "aggregations", "s", "value"), 0.0d);
            assertEquals("avg(id) with id>=2 == 3.5", 3.5d, extractDoublePath(metricBody, "aggregations", "a", "value"), 0.0d);
            assertEquals("min(id) with id>=2 == 2", 2.0d, extractDoublePath(metricBody, "aggregations", "m", "value"), 0.0d);
            assertEquals("max(id) with id>=2 == 5", 5.0d, extractDoublePath(metricBody, "aggregations", "M", "value"), 0.0d);

            // (b) filter + terms bucket agg on the keyword sub-field
            // of a lance_text column. `id < 3` keeps {0, 1, 2}. The
            // body values on those rows are:
            // id=0 → "hello lance 0"
            // id=1 → "quick brown fox 1"
            // id=2 → "hello lance 2"
            // Terms agg on body.raw must open three buckets, each
            // with a doc count of 1. Bucket assembly walks
            // getSortedSetDocValues("body.raw") on filter-matched
            // docs; the ord dictionary the leaf reader builds must
            // contain the three matching values (and only those,
            // since ensureTextLoaded's scan was filtered).
            String termsBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"query\":{\"range\":{\"id\":{\"lt\":3}}},"
                        + "\"aggs\":{\"by_body\":{\"terms\":{\"field\":\"body.raw\",\"size\":10}}}}"
                )
            );
            assertEquals("filter + terms total must count matching rows", 3, extractIntPath(termsBody, "hits", "total", "value"));
            assertTrue("filter + terms must open a bucket for id=0's body: " + termsBody, termsBody.contains("hello lance 0"));
            assertTrue("filter + terms must open a bucket for id=1's body: " + termsBody, termsBody.contains("quick brown fox 1"));
            assertTrue("filter + terms must open a bucket for id=2's body: " + termsBody, termsBody.contains("hello lance 2"));
            // Rows outside the filter (id >= 3) must not leak into
            // the terms dictionary. If they did, the ord numbering
            // would shift and the bucket keys would be off. Pin the
            // negative case with a value from id=3 (odd row, body
            // starts with "quick brown fox 3") which the range
            // filter must exclude.
            assertFalse("filter + terms must not include rows outside the filter: " + termsBody, termsBody.contains("quick brown fox 3"));

            // (c) filter + terms with a metric sub-agg. Confirms
            // that the filter push-down still cooperates with the
            // #40 fix (getPostCollectionAggregation instead of a
            // second postCollection call). Same three buckets as
            // (b); each carries avg(id) == that row's id.
            String termsWithMetricBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"query\":{\"range\":{\"id\":{\"lt\":3}}},"
                        + "\"aggs\":{\"by_body\":{\"terms\":{\"field\":\"body.raw\",\"size\":10},"
                        + "\"aggs\":{\"a\":{\"avg\":{\"field\":\"id\"}}}}}}"
                )
            );
            assertEquals("filter + terms + sub-avg total unchanged", 3, extractIntPath(termsWithMetricBody, "hits", "total", "value"));
            // Presence of the three keys plus the avg field
            // structure inside each bucket is enough — Terms
            // regression fence for the sub-agg wire happens in the
            // dedicated Deferred sub-agg IT.
            assertTrue(
                "filter + terms + sub-avg must render the sub-agg: " + termsWithMetricBody,
                termsWithMetricBody.contains("\"a\":{\"value\":")
            );

            // (d) match query + terms agg (regression fence for the
            // FTS shape). Match queries are not translatable to
            // Lance SQL, so filterSql stays null and
            // ensureXxxLoaded falls back to the pre-Phase-C
            // unfiltered scan. Correctness must be identical to the
            // shard path: `match body:lance` hits the three even
            // rows (0, 2, 4), each with a distinct body value.
            String matchBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"query\":{\"match\":{\"body\":\"lance\"}},"
                        + "\"aggs\":{\"by_body\":{\"terms\":{\"field\":\"body.raw\",\"size\":10}}}}"
                )
            );
            assertEquals("match + terms total must count matching FTS rows", 3, extractIntPath(matchBody, "hits", "total", "value"));
            assertTrue("match + terms must open a bucket for id=0's body: " + matchBody, matchBody.contains("hello lance 0"));
            assertTrue("match + terms must open a bucket for id=4's body: " + matchBody, matchBody.contains("hello lance 4"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeReturnsAggregationsBlockForEmptyTable() throws Exception {
        // A 0-row Lance table with an aggregation request used to
        // drop the aggregations block entirely from the response.
        // Fragment path skipped the fan-out (nothing to fan out) so
        // the coordinator had no per-node InternalAggregations to
        // reduce, and the response was missing the "aggregations"
        // key that shard path would still produce. Coordinator now
        // dispatches a single empty-fragment fan-out to the primary
        // node whenever the request carries aggregations, so the
        // per-node executor runs the aggregator over zero docs and
        // returns an empty tree the coordinator can reduce.
        String suffix = "s3-empty-agg-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 0);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // max aggregation on id. Zero rows means the metric
            // has no value, but the aggregations block itself must
            // still be present with a null value.
            String metricBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"query\":{\"match_all\":{}},\"aggs\":{\"m\":{\"max\":{\"field\":\"id\"}}}}"
                )
            );
            assertEquals(0, extractIntPath(metricBody, "hits", "total", "value"));
            assertTrue("expected aggregations block for empty table max agg: " + metricBody, metricBody.contains("\"aggregations\""));
            assertTrue("expected m bucket for empty table max agg: " + metricBody, metricBody.contains("\"m\""));

            // terms aggregation: empty table produces buckets=[] but
            // the aggregations block should still be there.
            String termsBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"query\":{\"match_all\":{}},\"aggs\":{\"g\":{\"terms\":{\"field\":\"id\"}}}}"
                )
            );
            assertEquals(0, extractIntPath(termsBody, "hits", "total", "value"));
            assertTrue("expected aggregations block for empty table terms agg: " + termsBody, termsBody.contains("\"aggregations\""));
            assertTrue("expected g bucket for empty table terms agg: " + termsBody, termsBody.contains("\"g\""));
            assertTrue("expected empty buckets on empty table: " + termsBody, termsBody.contains("\"buckets\":[]"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }
}
