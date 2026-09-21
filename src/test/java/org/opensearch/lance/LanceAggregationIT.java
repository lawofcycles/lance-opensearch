/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Aggregations on the fragment dispatch path: metric aggregations, terms
 * with deferred sub-aggregations, filter pushdown into column
 * materialisation, and the empty-table response shape.
 */
public class LanceAggregationIT extends LanceRestTestCase {

    public void testFragmentDispatchModeAnswersMetricAggregations() throws Exception {
        // Metric aggregations (value_count, sum, avg, min, max) and a
        // terms bucket aggregation on the fragment dispatch path, alone
        // and combined with a filter query or with hits.
        String suffix = "aggs-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // id = 0..5: count 6, sum 15, avg 2.5, min 0, max 5.
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

            // id >= 2: total 4, sum 14.
            String filteredBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"query\":{\"range\":{\"id\":{\"gte\":2}}}," + "\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}"
                )
            );
            assertEquals("filtered aggregation must count filtered rows", 4, extractIntPath(filteredBody, "hits", "total", "value"));
            assertEquals("sum(id) with id>=2 == 14", 14.0d, extractDoublePath(filteredBody, "aggregations", "s", "value"), 0.0d);

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

            // Six distinct ids give six buckets of doc_count 1.
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
        // terms with a metric or nested terms sub-aggregation goes
        // through BestBucketsDeferringCollector under the default
        // breadth_first collect mode. The executor must read the built
        // aggregations through Aggregator#getPostCollectionAggregation,
        // as the shard path does, rather than replaying post-collection
        // itself. depth_first is included so both modes stay covered.
        String suffix = "tds-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // Six single-doc buckets; avg over each equals the id.
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

            String sumBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"aggs\":{\"by_id\":{\"terms\":{\"field\":\"id\",\"size\":10},"
                        + "\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}}}"
                )
            );
            assertEquals("terms + sum total unchanged", 6, extractIntPath(sumBody, "hits", "total", "value"));

            String maxBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"aggs\":{\"by_id\":{\"terms\":{\"field\":\"id\",\"size\":10},"
                        + "\"aggs\":{\"m\":{\"max\":{\"field\":\"id\"}}}}}}"
                )
            );
            assertEquals("terms + max total unchanged", 6, extractIntPath(maxBody, "hits", "total", "value"));

            String nestedBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"aggs\":{\"by_id\":{\"terms\":{\"field\":\"id\",\"size\":10},"
                        + "\"aggs\":{\"nest\":{\"terms\":{\"field\":\"id\",\"size\":10}}}}}}"
                )
            );
            assertEquals("nested terms total unchanged", 6, extractIntPath(nestedBody, "hits", "total", "value"));

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
        // The leaf reader layers the request's Lance SQL filter into the
        // per-column scans that back doc values. Aggregation values must
        // still equal the unfiltered aggregation restricted to the
        // matching rows, for numeric doc values and for the keyword
        // ordinal dictionary alike.
        String suffix = "fcpc-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        // id 0..5; body alternates "hello lance i" and "quick brown fox i".
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson(
                "/_lance/attach",
                // body.raw gives the terms aggregations a keyword key.
                "{\"table\":\"" + tableUri + "\",\"multi_fields\":{\"body\":{\"raw\":{\"type\":\"keyword\"}}}}"
            );
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // id >= 2 keeps {2, 3, 4, 5}: sum 14, avg 3.5, min 2, max 5.
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

            // id < 3 keeps {0, 1, 2}; terms on body.raw opens one bucket
            // per row.
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
            // A value from an excluded row must not appear as a bucket.
            assertFalse("filter + terms must not include rows outside the filter: " + termsBody, termsBody.contains("quick brown fox 3"));

            // Filter plus a deferred sub-aggregation: the same three
            // buckets, each carrying avg(id) equal to its row's id.
            String termsWithMetricBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"query\":{\"range\":{\"id\":{\"lt\":3}}},"
                        + "\"aggs\":{\"by_body\":{\"terms\":{\"field\":\"body.raw\",\"size\":10},"
                        + "\"aggs\":{\"a\":{\"avg\":{\"field\":\"id\"}}}}}}"
                )
            );
            assertEquals("filter + terms + sub-avg total unchanged", 3, extractIntPath(termsWithMetricBody, "hits", "total", "value"));
            assertTrue(
                "filter + terms + sub-avg must render the sub-agg: " + termsWithMetricBody,
                termsWithMetricBody.contains("\"a\":{\"value\":")
            );

            // A match query has no SQL form, so the column scans run
            // unfiltered; "lance" hits the three even rows.
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
        // An empty table still returns an aggregations block: the
        // coordinator dispatches to the primary even with no fragments
        // so the aggregator produces an empty tree to reduce.
        String suffix = "s3-empty-agg-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 0);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            String metricBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"query\":{\"match_all\":{}},\"aggs\":{\"m\":{\"max\":{\"field\":\"id\"}}}}"
                )
            );
            assertEquals(0, extractIntPath(metricBody, "hits", "total", "value"));
            assertTrue("expected aggregations block for empty table max agg: " + metricBody, metricBody.contains("\"aggregations\""));
            assertTrue("expected m bucket for empty table max agg: " + metricBody, metricBody.contains("\"m\""));

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

    public void testRepeatedRequestsAndDisabledCacheAgreeOnEveryShape() throws Exception {
        // The first request against a table version builds the node's
        // snapshot and loads the numeric, boolean and keyword columns it
        // reads into the off-heap column store; the second request reads
        // them from there. Both must answer the same, and so must the request
        // path that runs with lance.cache.enabled false (per request
        // dataset open, heap columns with the filter pushed into the
        // column scan). The hint fixture has 3 fragments of 200 rows with
        // nullable rating (int), flag (bool), category (keyword) and an
        // FTS body.
        try (LanceTestCluster fixture = LanceTestCluster.setUpHintFixture(3, 200, "cache-agree")) {
            String index = fixture.indexName();
            String matchAll = "{\"query\":{\"match_all\":{}}}";
            String[] shapes = new String[] {
                // hits pages
                "{\"size\":10,\"query\":{\"match_all\":{}}}",
                "{\"size\":10,\"query\":{\"match_all\":{}},\"sort\":[{\"rating\":\"desc\"},{\"id\":\"asc\"}]}",
                "{\"size\":10,\"query\":{\"term\":{\"flag\":true}},\"sort\":[{\"rating\":\"asc\"},{\"id\":\"asc\"}]}",
                "{\"size\":10,\"query\":{\"range\":{\"rating\":{\"lt\":100}}},\"sort\":[{\"id\":\"asc\"}]}",
                // numeric and boolean aggregations over every row
                "{\"size\":0,\"query\":{\"match_all\":{}},\"aggs\":{\"r\":{\"terms\":{\"field\":\"rating\",\"size\":20}}}}",
                "{\"size\":0,\"query\":{\"match_all\":{}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}},\"a\":{\"avg\":{\"field\":\"rating\"}},"
                    + "\"m\":{\"min\":{\"field\":\"rating\"}},\"M\":{\"max\":{\"field\":\"rating\"}},\"c\":{\"value_count\":{\"field\":\"rating\"}}}}",
                "{\"size\":0,\"query\":{\"match_all\":{}},\"aggs\":{\"f\":{\"terms\":{\"field\":\"flag\"}}}}",
                // a filter the coordinator translates to Lance SQL: the
                // store loads the whole column and the Weight filters
                "{\"size\":0,\"query\":{\"range\":{\"rating\":{\"gte\":500}}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}},\"c\":{\"value_count\":{\"field\":\"id\"}}}}",
                "{\"size\":0,\"query\":{\"term\":{\"flag\":false}},\"aggs\":{\"r\":{\"terms\":{\"field\":\"rating\",\"size\":20}}}}",
                // keyword aggregations and sorts over every row: the
                // dictionary and ordinals come from the store on the
                // second request
                "{\"size\":0,\"query\":{\"match_all\":{}},\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"a\":{\"avg\":{\"field\":\"rating\"}}}}}}",
                "{\"size\":0,\"query\":{\"match_all\":{}},\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\",\"size\":10}}}}",
                "{\"size\":0,\"query\":{\"match_all\":{}},\"aggs\":{\"t\":{\"terms\":{\"field\":\"tags\",\"size\":10}}}}",
                "{\"size\":0,\"query\":{\"match_all\":{}},\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"t\":{\"terms\":{\"field\":\"tags\"}}}}}}",
                "{\"size\":10,\"query\":{\"match_all\":{}},\"sort\":[{\"category\":\"asc\"},{\"id\":\"asc\"}],\"aggs\":{\"n\":{\"value_count\":{\"field\":\"id\"}}}}",
                "{\"size\":10,\"query\":{\"match_all\":{}},\"sort\":[{\"category\":\"desc\"},{\"id\":\"desc\"}]}",
                "{\"size\":0,\"query\":{\"term\":{\"category\":\"c1\"}},\"aggs\":{\"t\":{\"terms\":{\"field\":\"tags\"}}}}",
                // a translated filter keeps the keyword dictionary on the
                // request scoped heap path
                "{\"size\":0,\"query\":{\"range\":{\"rating\":{\"gte\":500}}},\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"}}}}",
                "{\"size\":10,\"query\":{\"range\":{\"rating\":{\"lt\":100}}},\"sort\":[{\"category\":\"asc\"},{\"id\":\"asc\"}],"
                    + "\"aggs\":{\"n\":{\"value_count\":{\"field\":\"id\"}}}}",
                // full-text hits with sort and aggregation (sparse take, then the store on a second clause)
                "{\"size\":10,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"grp7\"}},\"sort\":[{\"rating\":\"desc\"},{\"id\":\"asc\"}]}",
                "{\"size\":0,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}",
                "{\"size\":5,\"query\":{\"bool\":{\"should\":[{\"lance_match\":{\"field\":\"body\",\"query\":\"grp7\"}},{\"term\":{\"id\":1}}]}},"
                    + "\"sort\":[{\"rating\":\"desc\"},{\"id\":\"asc\"}]}",
                // vector hits
                "{\"size\":5,\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":[250.4,0,0,0,0,0,0,0],\"k\":5}},"
                    + "\"sort\":[{\"rating\":\"desc\"},{\"id\":\"asc\"}]}" };

            // Warm: every shape twice against the cache.
            List<String> first = new ArrayList<>();
            for (String shape : shapes) {
                first.add(withoutTook(readAll(postJson("/" + index + "/_search", shape))));
            }
            assertEquals(600, extractIntPath(readAll(postJson("/" + index + "/_search", matchAll)), "hits", "total", "value"));
            for (int i = 0; i < shapes.length; i++) {
                String second = withoutTook(readAll(postJson("/" + index + "/_search", shapes[i])));
                assertEquals("second request differs for " + shapes[i], first.get(i), second);
            }
            // Sanity on the content, not only on the equality: 480 rows
            // have a rating, ratings are (i * 37) % 1000.
            String metrics = first.get(5);
            assertEquals(480, extractIntPath(metrics, "aggregations", "c", "value"));
            assertEquals(0.0d, extractDoublePath(metrics, "aggregations", "m", "value"), 0.0d);
            // 450 rows have a category, 150 of each of c0, c1, c2.
            String categories = first.get(10);
            for (String key : new String[] { "c0", "c1", "c2" }) {
                assertTrue(key + " bucket in " + categories, categories.contains("{\"key\":\"" + key + "\",\"doc_count\":150}"));
            }
            // category asc, id asc: c0 rows in id order, skipping the null rows (id % 4 == 3).
            assertEquals(
                List.of("0-0", "0-6", "0-9", "0-12", "0-18", "0-21", "0-24", "0-30", "0-33", "0-36"),
                idsOf(hitsOf(first.get(13)))
            );

            // Disabled cache: same answers from the per request path.
            Request disable = new Request("PUT", "/_cluster/settings");
            disable.setJsonEntity("{\"transient\":{\"lance.cache.enabled\":false}}");
            client().performRequest(disable);
            try {
                for (int i = 0; i < shapes.length; i++) {
                    String uncached = withoutTook(readAll(postJson("/" + index + "/_search", shapes[i])));
                    assertEquals("uncached request differs for " + shapes[i], first.get(i), uncached);
                }
            } finally {
                Request enable = new Request("PUT", "/_cluster/settings");
                enable.setJsonEntity("{\"transient\":{\"lance.cache.enabled\":null}}");
                client().performRequest(enable);
            }
            // Back on: the snapshots were retired by the disable, so this
            // rebuilds them and must still agree.
            for (int i = 0; i < shapes.length; i++) {
                String rebuilt = withoutTook(readAll(postJson("/" + index + "/_search", shapes[i])));
                assertEquals("request after re-enable differs for " + shapes[i], first.get(i), rebuilt);
            }
        }
    }

    /** Response body with the {@code took} field removed so two runs compare on content. */
    private static String withoutTook(String body) {
        return body.replaceFirst("\"took\":\\d+,", "");
    }

    public void testKeywordTermsLoadsTheDictionaryOnceAndAgreesWithTheDisabledCache() throws Exception {
        // terms(category) size 0 through the Lucene aggregator: the first
        // request scans the keyword column once and its dictionary enters
        // the store (loads + 1, no budget miss), the second reads the
        // store (no load), and the per request path with
        // lance.cache.enabled false returns the same buckets. The
        // aggregation pushdown is turned off because a size 0 terms over
        // match_all otherwise runs inside the Lance scan and never
        // touches the store. The hint fixture has 3 fragments of 200 rows
        // with category c0, c1, c2 on three rows out of four.
        Request disablePushdown = new Request("PUT", "/_cluster/settings");
        disablePushdown.setJsonEntity("{\"transient\":{\"lance.aggregation.pushdown\":false}}");
        client().performRequest(disablePushdown);
        try (LanceTestCluster fixture = LanceTestCluster.setUpHintFixture(3, 200, "kw-once")) {
            String index = fixture.indexName();
            String terms = "{\"size\":0,\"query\":{\"match_all\":{}},\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\",\"size\":10}}}}";
            Map<String, Object> before = columnStoreStats();

            String first = withoutTook(readAll(postJson("/" + index + "/_search", terms)));
            assertEquals(600, extractIntPath(first, "hits", "total", "value"));
            assertEquals(List.of("c0=150", "c1=150", "c2=150"), bucketsOf(first, "c"));
            Map<String, Object> afterFirst = columnStoreStats();
            assertEquals("one scan filled the store", number(before.get("loads")) + 1, number(afterFirst.get("loads")));
            assertEquals("the dictionary fit the budget", before.get("budget_misses"), afterFirst.get("budget_misses"));

            String second = withoutTook(readAll(postJson("/" + index + "/_search", terms)));
            assertEquals("second request differs", first, second);
            Map<String, Object> afterSecond = columnStoreStats();
            assertEquals("no load on the second request", afterFirst.get("loads"), afterSecond.get("loads"));
            assertEquals(afterFirst.get("budget_misses"), afterSecond.get("budget_misses"));
            assertTrue("the second request read the store", number(afterSecond.get("hits")) > number(afterFirst.get("hits")));

            Request disable = new Request("PUT", "/_cluster/settings");
            disable.setJsonEntity("{\"transient\":{\"lance.cache.enabled\":false}}");
            client().performRequest(disable);
            try {
                String uncached = withoutTook(readAll(postJson("/" + index + "/_search", terms)));
                assertEquals("uncached request differs", first, uncached);
                Map<String, Object> afterUncached = columnStoreStats();
                assertEquals("the per request path never touches the store", afterSecond.get("loads"), afterUncached.get("loads"));
                assertEquals(afterSecond.get("budget_misses"), afterUncached.get("budget_misses"));
            } finally {
                Request enable = new Request("PUT", "/_cluster/settings");
                enable.setJsonEntity("{\"transient\":{\"lance.cache.enabled\":null}}");
                client().performRequest(enable);
            }
        } finally {
            Request enablePushdown = new Request("PUT", "/_cluster/settings");
            enablePushdown.setJsonEntity("{\"transient\":{\"lance.aggregation.pushdown\":null}}");
            client().performRequest(enablePushdown);
        }
    }

    /** The {@code column_store} object of the single test node from {@code GET /_lance/stats}. */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> columnStoreStats() throws IOException {
        String json = readAll(client().performRequest(new Request("GET", "/_lance/stats")));
        try (XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Map<String, Object> nodes = (Map<String, Object>) parser.map().get("nodes");
            assertEquals("single node cluster", 1, nodes.size());
            Map<String, Object> node = (Map<String, Object>) nodes.values().iterator().next();
            return (Map<String, Object>) node.get("column_store");
        }
    }

    private static int number(Object value) {
        return ((Number) value).intValue();
    }

    private static long longNumber(Object value) {
        return ((Number) value).longValue();
    }

    private static void putTransientSetting(String key, String value) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"transient\":{\"" + key + "\":" + (value == null ? "null" : "\"" + value + "\"") + "}}");
        client().performRequest(request);
    }

    public void testHeapColumnLoadIsChargedToTheRequestBreakerAndRefusedAt429() throws Exception {
        // With lance.cache.enabled false every column a request reads is
        // materialised in heap for that request, the path a column store
        // budget miss takes (lance.cache.column_share is a startup
        // setting, so the store cannot be emptied at runtime). The hint
        // fixture has 3 fragments of 200 rows: the rating column costs a
        // little over 1.6 KB per fragment as long[200] plus its presence
        // bits, about 5 KB for the request. The aggregator itself
        // reserves 5 KB on the same breaker when it is built, so a limit
        // of 8 KB lets the aggregator through and refuses the column. The
        // pushdown is off because a size 0 sum over match_all would
        // otherwise run inside the Lance scan and never touch the column.
        putTransientSetting("lance.aggregation.pushdown", "false");
        putTransientSetting("lance.cache.enabled", "false");
        String sum = "{\"size\":0,\"query\":{\"match_all\":{}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}";
        try (LanceTestCluster fixture = LanceTestCluster.setUpHintFixture(3, 200, "heap-breaker")) {
            String index = fixture.indexName();
            Map<String, Object> before = columnStoreStats();
            long rejectionsBefore = longNumber(before.get("heap_fallback_rejections"));

            String first = withoutTook(readAll(postJson("/" + index + "/_search", sum)));
            assertEquals(600, extractIntPath(first, "hits", "total", "value"));
            // 480 rows have a rating of (i * 37) % 1000.
            double expectedSum = 0d;
            for (int i = 0; i < 600; i++) {
                if (i % 5 != 4) {
                    expectedSum += (i * 37) % 1000;
                }
            }
            assertEquals(expectedSum, extractDoublePath(first, "aggregations", "s", "value"), 0d);
            Map<String, Object> afterFirst = columnStoreStats();
            assertEquals(
                "the reader closed with the request and gave the heap back",
                0L,
                longNumber(afterFirst.get("heap_fallback_bytes"))
            );
            assertEquals(rejectionsBefore, longNumber(afterFirst.get("heap_fallback_rejections")));

            putTransientSetting("indices.breaker.request.limit", "8kb");
            try {
                ResponseException refused = expectThrows(ResponseException.class, () -> postJson("/" + index + "/_search", sum));
                String body = readAll(refused.getResponse());
                assertEquals(body, RestStatus.TOO_MANY_REQUESTS.getStatus(), refused.getResponse().getStatusLine().getStatusCode());
                assertTrue(body, body.contains("circuit_breaking_exception"));
                assertTrue("the label names the column: " + body, body.contains("lance_heap_column:rating"));
                assertTrue("the message names the column: " + body, body.contains("column [rating]"));
                assertTrue("the message carries the limit: " + body, body.contains("limit of [8192/8kb]"));
                Map<String, Object> afterRefusal = columnStoreStats();
                assertEquals(rejectionsBefore + 1, longNumber(afterRefusal.get("heap_fallback_rejections")));
                assertEquals(0L, longNumber(afterRefusal.get("heap_fallback_bytes")));
            } finally {
                putTransientSetting("indices.breaker.request.limit", null);
            }

            // Back at the default limit the same request answers as before.
            String again = withoutTook(readAll(postJson("/" + index + "/_search", sum)));
            assertEquals(first, again);
            assertEquals(rejectionsBefore + 1, longNumber(columnStoreStats().get("heap_fallback_rejections")));

            // The shard path reader (explain routes there) stays open for
            // the life of the shard, so its heap column stays charged and
            // the gauge shows it until the index goes away.
            String viaShard = readAll(postJson("/" + index + "/_search", "{\"explain\":true," + sum.substring(1)));
            assertEquals(expectedSum, extractDoublePath(viaShard, "aggregations", "s", "value"), 0d);
            long held = longNumber(columnStoreStats().get("heap_fallback_bytes"));
            assertTrue(
                "the engine reader holds rating in heap: " + held,
                held >= 3 * 200 * Long.BYTES && held < 3 * 200 * Long.BYTES + 1024
            );
            client().performRequest(new Request("DELETE", "/" + index));
            assertBusy(() -> assertEquals(0L, longNumber(columnStoreStats().get("heap_fallback_bytes"))));
        } finally {
            putTransientSetting("lance.cache.enabled", null);
            putTransientSetting("lance.aggregation.pushdown", null);
        }
    }

    public void testSubstraitPushdownAnswersLikeTheAggregators() throws Exception {
        // Every size 0 shape the Substrait pushdown accepts, answered
        // once with lance.aggregation.pushdown on (the default) and once
        // with it off (Lucene aggregators over the fragment readers);
        // the two response bodies have to be identical, hits.total and
        // the terms error / other counts included. The hint fixture has
        // 3 fragments of 200 rows with nullable rating (int), flag (bool),
        // category (keyword), tags (keyword list) and an FTS body.
        try (LanceTestCluster fixture = LanceTestCluster.setUpHintFixture(3, 200, "pushdown-agree")) {
            String index = fixture.indexName();
            String[] shapes = new String[] {
                // metrics only, with and without a filter
                "{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}},\"a\":{\"avg\":{\"field\":\"rating\"}},"
                    + "\"m\":{\"min\":{\"field\":\"rating\"}},\"M\":{\"max\":{\"field\":\"rating\"}},\"c\":{\"value_count\":{\"field\":\"rating\"}}}}",
                "{\"size\":0,\"query\":{\"match_all\":{}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}",
                "{\"size\":0,\"query\":{\"range\":{\"rating\":{\"gte\":500}}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}},\"c\":{\"value_count\":{\"field\":\"id\"}}}}",
                "{\"size\":0,\"query\":{\"term\":{\"flag\":true}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}},\"f\":{\"sum\":{\"field\":\"flag\"}}}}",
                "{\"size\":0,\"aggs\":{\"k\":{\"value_count\":{\"field\":\"category\"}}}}",
                "{\"size\":0,\"aggs\":{\"fm\":{\"max\":{\"field\":\"flag\"}},\"fa\":{\"avg\":{\"field\":\"flag\"}}}}",
                // terms: keyword, integer, boolean, small size (shard_size
                // cuts the groups, so sum_other_doc_count and the error
                // bound matter), _key orders, sub-metrics
                "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"}}}}",
                "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\",\"size\":2}}}}",
                "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\",\"order\":{\"_key\":\"desc\"}}}}}",
                "{\"size\":0,\"aggs\":{\"r\":{\"terms\":{\"field\":\"rating\",\"size\":20}}}}",
                "{\"size\":0,\"aggs\":{\"r\":{\"terms\":{\"field\":\"rating\",\"size\":3,\"shard_size\":5}}}}",
                "{\"size\":0,\"aggs\":{\"r\":{\"terms\":{\"field\":\"rating\",\"size\":5,\"order\":{\"_key\":\"asc\"}}}}}",
                "{\"size\":0,\"aggs\":{\"r\":{\"terms\":{\"field\":\"rating\",\"size\":5,\"show_term_doc_count_error\":true}}}}",
                "{\"size\":0,\"aggs\":{\"f\":{\"terms\":{\"field\":\"flag\"}}}}",
                "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"a\":{\"avg\":{\"field\":\"rating\"}},"
                    + "\"s\":{\"sum\":{\"field\":\"rating\"}},\"m\":{\"min\":{\"field\":\"rating\"}},\"M\":{\"max\":{\"field\":\"rating\"}},\"n\":{\"value_count\":{\"field\":\"rating\"}}}}}}",
                "{\"size\":0,\"query\":{\"range\":{\"rating\":{\"gte\":500}}},\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"}}}}",
                "{\"size\":0,\"query\":{\"term\":{\"category\":\"c1\"}},\"aggs\":{\"r\":{\"terms\":{\"field\":\"rating\",\"size\":4}}}}",
                "{\"size\":0,\"query\":{\"bool\":{\"filter\":[{\"term\":{\"flag\":false}},{\"range\":{\"rating\":{\"lt\":300}}}]}},"
                    + "\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"a\":{\"avg\":{\"field\":\"rating\"}}}}}}",
                // histogram on the integer column
                "{\"size\":0,\"aggs\":{\"h\":{\"histogram\":{\"field\":\"rating\",\"interval\":100}}}}",
                "{\"size\":0,\"aggs\":{\"h\":{\"histogram\":{\"field\":\"rating\",\"interval\":250,\"min_doc_count\":1},\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}}}",
                "{\"size\":0,\"query\":{\"range\":{\"rating\":{\"gte\":100,\"lt\":700}}},\"aggs\":{\"h\":{\"histogram\":{\"field\":\"rating\",\"interval\":150,\"keyed\":true}}}}",
                // nested buckets: two and three levels, metrics beside
                // the nested bucket, small inner sizes so the inner
                // truncation, sum_other_doc_count and the error bound
                // matter, a histogram parent with min_doc_count 0
                "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"r\":{\"terms\":{\"field\":\"rating\",\"size\":3},"
                    + "\"aggs\":{\"a\":{\"avg\":{\"field\":\"id\"}}}}}}}}",
                "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\",\"size\":2},\"aggs\":{\"a\":{\"avg\":{\"field\":\"rating\"}},"
                    + "\"f\":{\"terms\":{\"field\":\"flag\"},\"aggs\":{\"m\":{\"max\":{\"field\":\"id\"}}}},\"s\":{\"sum\":{\"field\":\"id\"}}}}}}",
                "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\",\"order\":{\"_key\":\"desc\"}},\"aggs\":{\"f\":{\"terms\":{\"field\":\"flag\"},"
                    + "\"aggs\":{\"r\":{\"terms\":{\"field\":\"rating\",\"size\":2,\"show_term_doc_count_error\":true},\"aggs\":{\"m\":{\"min\":{\"field\":\"id\"}}}}}}}}}}",
                "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"h\":{\"histogram\":{\"field\":\"rating\",\"interval\":250},"
                    + "\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}}}}}",
                "{\"size\":0,\"aggs\":{\"h\":{\"histogram\":{\"field\":\"rating\",\"interval\":250,\"min_doc_count\":0},\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"},"
                    + "\"aggs\":{\"a\":{\"avg\":{\"field\":\"id\"}}}}}}}}",
                "{\"size\":0,\"query\":{\"range\":{\"rating\":{\"gte\":500}}},\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"},"
                    + "\"aggs\":{\"r\":{\"terms\":{\"field\":\"rating\",\"size\":4,\"order\":{\"_key\":\"desc\"}}}}}}}",
                // composite: size and after paging, a descending source,
                // a boolean source, metric children
                "{\"size\":0,\"aggs\":{\"cr\":{\"composite\":{\"sources\":[{\"c\":{\"terms\":{\"field\":\"category\"}}},{\"r\":{\"terms\":{\"field\":\"rating\"}}}]}}}}",
                "{\"size\":0,\"aggs\":{\"cr\":{\"composite\":{\"size\":3,\"sources\":[{\"c\":{\"terms\":{\"field\":\"category\"}}},{\"r\":{\"terms\":{\"field\":\"rating\"}}}]},"
                    + "\"aggs\":{\"a\":{\"avg\":{\"field\":\"id\"}}}}}}",
                "{\"size\":0,\"aggs\":{\"cr\":{\"composite\":{\"size\":4,\"sources\":[{\"c\":{\"terms\":{\"field\":\"category\"}}},{\"r\":{\"terms\":{\"field\":\"rating\"}}}],"
                    + "\"after\":{\"c\":\"c1\",\"r\":500}}}}}",
                "{\"size\":0,\"aggs\":{\"rc\":{\"composite\":{\"size\":5,\"sources\":[{\"r\":{\"terms\":{\"field\":\"rating\",\"order\":\"desc\"}}},{\"c\":{\"terms\":{\"field\":\"category\"}}}],"
                    + "\"after\":{\"r\":900,\"c\":\"c0\"}}}}}",
                "{\"size\":0,\"query\":{\"term\":{\"flag\":true}},\"aggs\":{\"fc\":{\"composite\":{\"size\":10,\"sources\":[{\"f\":{\"terms\":{\"field\":\"flag\"}}},{\"c\":{\"terms\":{\"field\":\"category\"}}}]},"
                    + "\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}},\"n\":{\"value_count\":{\"field\":\"id\"}}}}}}",
                "{\"size\":0,\"aggs\":{\"c\":{\"composite\":{\"size\":2,\"sources\":[{\"c\":{\"terms\":{\"field\":\"category\",\"order\":\"desc\"}}}]}}}}",
                // track_total_hits variants
                "{\"size\":0,\"track_total_hits\":true,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"}}}}",
                "{\"size\":0,\"track_total_hits\":false,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"}}}}",
                "{\"size\":0,\"track_total_hits\":100,\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}" };
            String[] aggregatorShapes = new String[] {
                // shapes outside the allow list stay on the aggregators
                "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"t\":{\"terms\":{\"field\":\"tags\"}}}}}}",
                "{\"size\":0,\"aggs\":{\"a\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"b\":{\"terms\":{\"field\":\"flag\"},\"aggs\":{\"c\":{\"terms\":{\"field\":\"rating\"},"
                    + "\"aggs\":{\"d\":{\"histogram\":{\"field\":\"id\",\"interval\":100}}}}}}}}}}",
                "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"f\":{\"terms\":{\"field\":\"flag\"}},\"r\":{\"terms\":{\"field\":\"rating\"}}}}}}",
                "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"r\":{\"terms\":{\"field\":\"rating\",\"order\":{\"_count\":\"asc\"}}}}}}}",
                "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\",\"size\":1000},\"aggs\":{\"r\":{\"terms\":{\"field\":\"rating\",\"size\":1000}}}}}}",
                "{\"size\":0,\"aggs\":{\"cr\":{\"composite\":{\"sources\":[{\"c\":{\"terms\":{\"field\":\"category\",\"missing_bucket\":true}}},{\"r\":{\"terms\":{\"field\":\"rating\"}}}]}}}}",
                "{\"size\":0,\"aggs\":{\"h\":{\"composite\":{\"sources\":[{\"h\":{\"histogram\":{\"field\":\"rating\",\"interval\":100}}}]}}}}",
                "{\"size\":0,\"aggs\":{\"t\":{\"composite\":{\"sources\":[{\"t\":{\"terms\":{\"field\":\"tags\"}}}]}}}}",
                "{\"size\":0,\"aggs\":{\"c\":{\"composite\":{\"sources\":[{\"c\":{\"terms\":{\"field\":\"category\"}}}]},\"aggs\":{\"r\":{\"terms\":{\"field\":\"rating\"}}}}}}",
                "{\"size\":0,\"aggs\":{\"t\":{\"terms\":{\"field\":\"tags\",\"size\":10}}}}",
                "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\",\"missing\":\"none\"}}}}",
                "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\",\"include\":\"c[01]\"}}}}",
                "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\",\"min_doc_count\":0}}}}",
                "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\",\"order\":{\"a\":\"desc\"}},\"aggs\":{\"a\":{\"avg\":{\"field\":\"rating\"}}}}}}",
                "{\"size\":0,\"aggs\":{\"h\":{\"histogram\":{\"field\":\"rating\",\"interval\":100,\"offset\":10}}}}",
                "{\"size\":0,\"aggs\":{\"h\":{\"histogram\":{\"field\":\"rating\",\"interval\":100,\"extended_bounds\":{\"min\":-200,\"max\":1200}}}}}",
                "{\"size\":0,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}",
                "{\"size\":0,\"query\":{\"match_all\":{}},\"post_filter\":{\"term\":{\"flag\":true}},\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"}}}}",
                "{\"size\":2,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"}}}}",
                "{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\",\"missing\":0}}}}",
                "{\"size\":0,\"aggs\":{\"u\":{\"sum\":{\"field\":\"unmapped\"}}}}",
                // the namespace registered this fixture without multi_fields, so body.raw is unmapped
                "{\"size\":0,\"aggs\":{\"b\":{\"terms\":{\"field\":\"body.raw\",\"size\":5}}}}" };
            assertPushdownAgreesWithAggregators(index, shapes, aggregatorShapes);
        }
    }

    public void testSubstraitPushdownAnswersDateHistogramLikeTheAggregators() throws Exception {
        // The dated fixture has six rows with a timestamp[us] column and
        // an even / odd keyword category: fixed_interval and
        // calendar_interval date histograms and date metrics take the
        // pushdown, time_zone, offset and bounds stay on the aggregators.
        // The attach adds a keyword sub-field so a terms on category.raw
        // resolves to the base column.
        String suffix = "pushdown-date-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeDatedTable(scratchDir, tableName);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson(
                "/_lance/attach",
                "{\"table\":\"" + tableUri + "\",\"multi_fields\":{\"category\":{\"raw\":{\"type\":\"keyword\"}}}}"
            );
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            String[] shapes = new String[] {
                "{\"size\":0,\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"fixed_interval\":\"30d\"}}}}",
                "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category.raw\"},\"aggs\":{\"n\":{\"value_count\":{\"field\":\"category.raw\"}}}}}}",
                "{\"size\":0,\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"fixed_interval\":\"30d\",\"min_doc_count\":1,\"keyed\":true}}}}",
                "{\"size\":0,\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"fixed_interval\":\"7d\"},"
                    + "\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}},\"last\":{\"max\":{\"field\":\"ts\"}}}}}}",
                "{\"size\":0,\"query\":{\"term\":{\"category\":\"even\"}},\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"fixed_interval\":\"30d\"}}}}",
                "{\"size\":0,\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"fixed_interval\":\"30d\",\"order\":{\"_key\":\"desc\"}}}}}",
                "{\"size\":0,\"aggs\":{\"first\":{\"min\":{\"field\":\"ts\"}},\"last\":{\"max\":{\"field\":\"ts\"}},\"n\":{\"value_count\":{\"field\":\"ts\"}}}}",
                "{\"size\":0,\"aggs\":{\"t\":{\"terms\":{\"field\":\"ts\",\"size\":10}}}}",
                "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"first\":{\"min\":{\"field\":\"ts\"}}}}}}",
                // calendar intervals: month with a metric child (the March
                // bucket holds two rows), day, week, quarter, year, the 1M
                // spelling, a filter and min_doc_count 0 for the reduce's
                // empty bucket filling on calendar rounding
                "{\"size\":0,\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"calendar_interval\":\"month\"},\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}}}",
                "{\"size\":0,\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"calendar_interval\":\"day\"}}}}",
                "{\"size\":0,\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"calendar_interval\":\"week\",\"min_doc_count\":1}}}}",
                "{\"size\":0,\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"calendar_interval\":\"quarter\",\"keyed\":true}}}}",
                "{\"size\":0,\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"calendar_interval\":\"year\"}}}}",
                "{\"size\":0,\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"calendar_interval\":\"1M\",\"order\":{\"_key\":\"desc\"}}}}}",
                "{\"size\":0,\"query\":{\"term\":{\"category\":\"odd\"}},\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"calendar_interval\":\"month\"}}}}",
                "{\"size\":0,\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"calendar_interval\":\"week\",\"min_doc_count\":0}}}}",
                // a calendar interval under terms and above terms
                "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"calendar_interval\":\"month\",\"min_doc_count\":0},"
                    + "\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}}}}}",
                "{\"size\":0,\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"calendar_interval\":\"week\"},\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"}}}}}}",
                // a date_histogram under terms (built without the
                // aggregator's prototype), terms under a date_histogram
                // with min_doc_count 0, composite with a date source in
                // both directions, raw and formatted after keys
                "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category.raw\"},\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"fixed_interval\":\"30d\"},"
                    + "\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}}}}}",
                "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"fixed_interval\":\"30d\",\"min_doc_count\":0,\"keyed\":true}}}}}}",
                "{\"size\":0,\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"fixed_interval\":\"30d\",\"min_doc_count\":0},\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"},"
                    + "\"aggs\":{\"a\":{\"avg\":{\"field\":\"id\"}}}}}}}}",
                "{\"size\":0,\"aggs\":{\"cd\":{\"composite\":{\"sources\":[{\"c\":{\"terms\":{\"field\":\"category\"}}},{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"fixed_interval\":\"30d\"}}}]},"
                    + "\"aggs\":{\"n\":{\"value_count\":{\"field\":\"id\"}}}}}}",
                "{\"size\":0,\"aggs\":{\"dc\":{\"composite\":{\"size\":2,\"sources\":[{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"fixed_interval\":\"30d\",\"order\":\"desc\"}}},"
                    + "{\"c\":{\"terms\":{\"field\":\"category\"}}}],\"after\":{\"d\":1709510400000,\"c\":\"odd\"}}}}}",
                "{\"size\":0,\"aggs\":{\"dc\":{\"composite\":{\"size\":2,\"sources\":[{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"fixed_interval\":\"30d\",\"format\":\"yyyy-MM-dd\"}}},"
                    + "{\"c\":{\"terms\":{\"field\":\"category\"}}}],\"after\":{\"d\":\"2024-01-31\",\"c\":\"odd\"}}}}}",
                "{\"size\":0,\"aggs\":{\"t\":{\"composite\":{\"size\":3,\"sources\":[{\"t\":{\"terms\":{\"field\":\"ts\"}}}],\"after\":{\"t\":1705276800000}}}}}" };
            String[] aggregatorShapes = new String[] {
                "{\"size\":0,\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"calendar_interval\":\"month\",\"time_zone\":\"+09:00\"}}}}",
                "{\"size\":0,\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"calendar_interval\":\"day\",\"offset\":\"6h\"}}}}",
                "{\"size\":0,\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"fixed_interval\":\"30d\",\"time_zone\":\"+09:00\"}}}}",
                "{\"size\":0,\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"fixed_interval\":\"30d\",\"offset\":\"1d\"}}}}",
                "{\"size\":0,\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"fixed_interval\":\"30d\","
                    + "\"extended_bounds\":{\"min\":\"2023-12-01\",\"max\":\"2024-07-01\"}}}}}",
                "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"calendar_interval\":\"month\",\"time_zone\":\"+09:00\"}}}}}}",
                "{\"size\":0,\"aggs\":{\"d\":{\"composite\":{\"sources\":[{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"calendar_interval\":\"month\"}}}]}}}}",
                "{\"size\":0,\"aggs\":{\"d\":{\"composite\":{\"sources\":[{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"fixed_interval\":\"30d\",\"time_zone\":\"+09:00\"}}}]}}}}" };
            assertPushdownAgreesWithAggregators(indexName, shapes, aggregatorShapes);
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testSubstraitPushdownWithParallelScansAnswersLikeTheAggregators() throws Exception {
        // Six fragments of 100 rows scanned in three groups per request:
        // the bodies have to equal the aggregators' exactly, terms error
        // and other counts included, and the executor's log line has to
        // say the answer came from three scans.
        try (LanceTestCluster fixture = LanceTestCluster.setUpHintFixture(6, 100, "pushdown-parallel")) {
            String index = fixture.indexName();
            String[] shapes = new String[] {
                "{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}},\"a\":{\"avg\":{\"field\":\"rating\"}},"
                    + "\"m\":{\"min\":{\"field\":\"rating\"}},\"M\":{\"max\":{\"field\":\"rating\"}},\"c\":{\"value_count\":{\"field\":\"rating\"}}}}",
                "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"}}}}",
                "{\"size\":0,\"aggs\":{\"r\":{\"terms\":{\"field\":\"rating\",\"size\":3,\"show_term_doc_count_error\":true}}}}",
                "{\"size\":0,\"aggs\":{\"r\":{\"terms\":{\"field\":\"rating\",\"size\":5,\"order\":{\"_key\":\"desc\"}}}}}",
                "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\",\"size\":2},\"aggs\":{\"a\":{\"avg\":{\"field\":\"rating\"}},"
                    + "\"m\":{\"min\":{\"field\":\"rating\"}},\"M\":{\"max\":{\"field\":\"rating\"}},\"n\":{\"value_count\":{\"field\":\"flag\"}}}}}}",
                "{\"size\":0,\"query\":{\"range\":{\"rating\":{\"gte\":500}}},\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"}}}}",
                "{\"size\":0,\"aggs\":{\"h\":{\"histogram\":{\"field\":\"rating\",\"interval\":100},\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}}}" };
            Request parallelism = new Request("PUT", "/_cluster/settings");
            parallelism.setJsonEntity("{\"transient\":{\"lance.aggregation.pushdown_parallelism\":3}}");
            client().performRequest(parallelism);
            try {
                long before = pushdownLogLines(index, "in 3 scans");
                assertPushdownAgreesWithAggregators(index, shapes, new String[0]);
                assertEquals("every pushdown answer came from three scans", before + shapes.length, pushdownLogLines(index, "in 3 scans"));
            } finally {
                Request reset = new Request("PUT", "/_cluster/settings");
                reset.setJsonEntity("{\"transient\":{\"lance.aggregation.pushdown_parallelism\":null}}");
                client().performRequest(reset);
            }
        }
    }

    public void testSubstraitPushdownOnEmptyTable() throws Exception {
        String suffix = "pushdown-empty-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 0);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            String[] shapes = new String[] {
                "{\"size\":0,\"aggs\":{\"m\":{\"max\":{\"field\":\"id\"}},\"s\":{\"sum\":{\"field\":\"id\"}},\"a\":{\"avg\":{\"field\":\"id\"}},\"c\":{\"value_count\":{\"field\":\"id\"}}}}",
                "{\"size\":0,\"aggs\":{\"g\":{\"terms\":{\"field\":\"id\"}}}}",
                "{\"size\":0,\"aggs\":{\"h\":{\"histogram\":{\"field\":\"id\",\"interval\":2}}}}" };
            assertPushdownAgreesWithAggregators(indexName, shapes, new String[0]);
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    /**
     * Runs every shape with {@code lance.aggregation.pushdown} on and
     * off and asserts the two response bodies are identical apart from
     * {@code took}. The executor logs each request the pushdown answers
     * at DEBUG, so the node log has to gain one line per shape in
     * {@code pushdownShapes} and none for {@code aggregatorShapes} or
     * for the run with the setting off. Restores the settings afterwards.
     */
    static void assertPushdownAgreesWithAggregators(String index, String[] pushdownShapes, String[] aggregatorShapes) throws Exception {
        Request debug = new Request("PUT", "/_cluster/settings");
        debug.setJsonEntity("{\"transient\":{\"logger.org.opensearch.lance.dispatch.TransportLanceFragmentQueryAction\":\"DEBUG\"}}");
        client().performRequest(debug);
        try {
            long before = pushdownLogLines(index);
            List<String> pushed = new ArrayList<>();
            for (String shape : pushdownShapes) {
                pushed.add(withoutTook(readAll(postJson("/" + index + "/_search", shape))));
            }
            assertBusy(
                () -> assertEquals("pushdown log lines after the pushdown shapes", before + pushdownShapes.length, pushdownLogLines(index))
            );
            List<String> viaAggregatorsOnly = new ArrayList<>();
            for (String shape : aggregatorShapes) {
                viaAggregatorsOnly.add(withoutTook(readAll(postJson("/" + index + "/_search", shape))));
            }
            assertEquals(
                "shapes outside the allow list must not take the pushdown",
                before + pushdownShapes.length,
                pushdownLogLines(index)
            );

            Request disable = new Request("PUT", "/_cluster/settings");
            disable.setJsonEntity("{\"transient\":{\"lance.aggregation.pushdown\":false}}");
            client().performRequest(disable);
            try {
                for (int i = 0; i < pushdownShapes.length; i++) {
                    String viaAggregators = withoutTook(readAll(postJson("/" + index + "/_search", pushdownShapes[i])));
                    assertEquals("pushdown differs from the aggregators for " + pushdownShapes[i], viaAggregators, pushed.get(i));
                }
                for (int i = 0; i < aggregatorShapes.length; i++) {
                    String again = withoutTook(readAll(postJson("/" + index + "/_search", aggregatorShapes[i])));
                    assertEquals(
                        "aggregator shape differs with the setting off for " + aggregatorShapes[i],
                        viaAggregatorsOnly.get(i),
                        again
                    );
                }
                assertEquals("the setting off must not take the pushdown", before + pushdownShapes.length, pushdownLogLines(index));
            } finally {
                Request enable = new Request("PUT", "/_cluster/settings");
                enable.setJsonEntity("{\"transient\":{\"lance.aggregation.pushdown\":null}}");
                client().performRequest(enable);
            }
        } finally {
            Request reset = new Request("PUT", "/_cluster/settings");
            reset.setJsonEntity("{\"transient\":{\"logger.org.opensearch.lance.dispatch.TransportLanceFragmentQueryAction\":null}}");
            client().performRequest(reset);
        }
    }

    /**
     * Number of executor log lines announcing a pushdown answer for
     * {@code indexName} across the node logs under
     * {@code build/testclusters/<task>-<n>/logs/<task>.log}, the sibling
     * of the shared tables directory. Only the log4j file is read; the
     * captured stdout repeats every line.
     */
    private static long pushdownLogLines(String indexName) throws IOException {
        return pushdownLogLines(indexName, "");
    }

    /**
     * As {@link #pushdownLogLines(String)}, counting only the lines that
     * also contain {@code detail} (for example {@code "in 3 scans"}).
     */
    private static long pushdownLogLines(String indexName, String detail) throws IOException {
        return logLines("lance.dispatch: aggregation pushdown for [" + indexName + "]", detail);
    }

    /** Coordinator fan-out lines for {@code indexName}: one per request that took the fragment path. */
    private static long fanOutLogLines(String indexName) throws IOException {
        return logLines("lance.dispatch: fan-out index [" + indexName + "]", "");
    }

    /**
     * Lines of the test cluster's node logs (the log4j file under
     * {@code build/testclusters/<task>-<n>/logs}, not the captured
     * stdout which repeats every line) that contain {@code marker} and
     * {@code detail}.
     */
    private static long logLines(String marker, String detail) throws IOException {
        Path clustersDir = sharedRoot().resolveSibling("testclusters");
        assertTrue("testclusters directory not found at " + clustersDir, Files.isDirectory(clustersDir));
        long count = 0;
        try (Stream<Path> files = Files.walk(clustersDir)) {
            for (Path file : files.filter(Files::isRegularFile).toList()) {
                String name = file.getFileName().toString();
                if (!name.endsWith(".log") || name.startsWith("opensearch.") || !file.getParent().getFileName().toString().equals("logs")) {
                    continue;
                }
                for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                    if (line.contains(marker) && line.contains(detail)) {
                        count++;
                    }
                }
            }
        }
        return count;
    }

    private static Map<String, Object> parse(String json) throws IOException {
        try (XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, json)) {
            return parser.map();
        }
    }

    /**
     * Run {@code shape} (a {@code _search} body without its outer braces)
     * through the fragment path and, with {@code "explain": true} added,
     * through the shard path, and assert the two responses carry the same
     * {@code hits.total} and the same {@code aggregations} block. Returns
     * the fragment path response.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> assertShardPathAgrees(String index, String shape) throws IOException {
        Map<String, Object> fragmentPath = parse(readAll(postJson("/" + index + "/_search", "{" + shape + "}")));
        Map<String, Object> shardPath = parse(
            readAll(postJson("/" + index + "/_search?request_cache=false", "{\"explain\":true," + shape + "}"))
        );
        assertEquals(
            shape,
            ((Map<String, Object>) shardPath.get("hits")).get("total"),
            ((Map<String, Object>) fragmentPath.get("hits")).get("total")
        );
        assertEquals(shape, shardPath.get("aggregations"), fragmentPath.get("aggregations"));
        return fragmentPath;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> aggregation(Map<String, Object> response, String name) {
        return (Map<String, Object>) ((Map<String, Object>) response.get("aggregations")).get(name);
    }

    private static void assertRelativeClose(String what, double expected, double actual, double tolerance) {
        double diff = Math.abs(expected - actual) / Math.max(Math.abs(expected), 1e-9);
        assertTrue(what + ": expected " + expected + " got " + actual + " (relative diff " + diff + ")", diff <= tolerance);
    }

    /**
     * Every aggregation type the allow list newly routes to the fragment
     * path answers the same as the shard path. The hint fixture has three
     * fragments of 400 rows: rating {@code (i * 37) % 1000}, null when
     * {@code i % 5 == 4}; category {@code c(i % 3)}, null when
     * {@code i % 4 == 3}; tags and flag likewise. The exact aggregations
     * (stats, extended_stats, range, missing, filter, filters, composite
     * with paging, hdr percentiles) have to match the shard path byte for
     * byte; the sketches (tdigest percentiles, cardinality) within their
     * error. Every fragment path request leaves one fan-out line in the
     * node log, which is how the test knows the requests did not fall to
     * the shard path.
     */
    @SuppressWarnings("unchecked")
    public void testWiderAllowListAnswersLikeTheShardPath() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUpHintFixture(3, 400, "allow-list")) {
            String index = fixture.indexName();
            long fanOutBefore = fanOutLogLines(index);
            String[] exact = {
                "\"size\":0,\"aggs\":{\"s\":{\"stats\":{\"field\":\"rating\"}},\"es\":{\"extended_stats\":{\"field\":\"rating\",\"sigma\":2}}}",
                "\"size\":0,\"query\":{\"term\":{\"category\":\"c1\"}},\"aggs\":{\"s\":{\"stats\":{\"field\":\"rating\"}}}",
                "\"size\":0,\"aggs\":{\"r\":{\"range\":{\"field\":\"rating\",\"keyed\":true,\"ranges\":[{\"to\":300},{\"from\":300,\"to\":700},{\"from\":700}]},\"aggs\":{\"a\":{\"avg\":{\"field\":\"id\"}}}}}",
                "\"size\":0,\"aggs\":{\"m\":{\"missing\":{\"field\":\"category\"},\"aggs\":{\"mx\":{\"max\":{\"field\":\"rating\"}}}}}",
                "\"size\":0,\"aggs\":{\"f\":{\"filter\":{\"range\":{\"rating\":{\"gte\":500}}},\"aggs\":{\"t\":{\"terms\":{\"field\":\"category\"}}}}}",
                "\"size\":0,\"aggs\":{\"f\":{\"filter\":{\"bool\":{\"filter\":[{\"exists\":{\"field\":\"category\"}}],\"must_not\":[{\"term\":{\"flag\":true}}]}}}}",
                "\"size\":0,\"aggs\":{\"fs\":{\"filters\":{\"other_bucket_key\":\"rest\",\"filters\":{\"low\":{\"range\":{\"rating\":{\"lt\":200}}},\"c0\":{\"term\":{\"category\":\"c0\"}}}},\"aggs\":{\"tags\":{\"terms\":{\"field\":\"tags\"}}}}}",
                "\"size\":0,\"aggs\":{\"fs\":{\"filters\":{\"filters\":[{\"terms\":{\"category\":[\"c0\",\"c2\"]}},{\"match_all\":{}}]}}}",
                "\"size\":0,\"aggs\":{\"c\":{\"composite\":{\"size\":7,\"sources\":[{\"cat\":{\"terms\":{\"field\":\"category\"}}},{\"r\":{\"terms\":{\"field\":\"rating\"}}}]},\"aggs\":{\"n\":{\"value_count\":{\"field\":\"id\"}}}}}",
                "\"size\":0,\"aggs\":{\"c\":{\"composite\":{\"size\":5,\"sources\":[{\"cat\":{\"terms\":{\"field\":\"category\",\"order\":\"desc\",\"missing_bucket\":true}}},{\"h\":{\"histogram\":{\"field\":\"rating\",\"interval\":250}}}]}}}",
                "\"size\":0,\"aggs\":{\"p\":{\"percentiles\":{\"field\":\"rating\",\"hdr\":{\"number_of_significant_value_digits\":3}}}}",
                "\"size\":0,\"aggs\":{\"pr\":{\"percentile_ranks\":{\"field\":\"rating\",\"values\":[250,750],\"hdr\":{\"number_of_significant_value_digits\":3}}}}",
                "\"size\":0,\"aggs\":{\"t\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"st\":{\"stats\":{\"field\":\"rating\"}},\"r\":{\"range\":{\"field\":\"rating\",\"ranges\":[{\"to\":500},{\"from\":500}]}}}}}" };
            int requests = 0;
            for (String shape : exact) {
                assertShardPathAgrees(index, shape);
                requests++;
            }

            // Composite paging: the second page starts at the after_key of
            // the first and is the same page on both paths.
            String firstPage =
                "\"size\":0,\"aggs\":{\"c\":{\"composite\":{\"size\":7,\"sources\":[{\"cat\":{\"terms\":{\"field\":\"category\"}}},{\"r\":{\"terms\":{\"field\":\"rating\"}}}]}}}";
            Map<String, Object> page = assertShardPathAgrees(index, firstPage);
            requests++;
            Map<String, Object> afterKey = (Map<String, Object>) aggregation(page, "c").get("after_key");
            assertEquals("c0", afterKey.get("cat"));
            List<String> keysSeen = new ArrayList<>();
            for (int pages = 0; pages < 4 && afterKey != null; pages++) {
                String after = "{\"cat\":\"" + afterKey.get("cat") + "\",\"r\":" + afterKey.get("r") + "}";
                String nextPage = "\"size\":0,\"aggs\":{\"c\":{\"composite\":{\"size\":7,\"after\":"
                    + after
                    + ",\"sources\":[{\"cat\":{\"terms\":{\"field\":\"category\"}}},{\"r\":{\"terms\":{\"field\":\"rating\"}}}]}}}";
                page = assertShardPathAgrees(index, nextPage);
                requests++;
                for (Map<String, Object> bucket : (List<Map<String, Object>>) aggregation(page, "c").get("buckets")) {
                    keysSeen.add(String.valueOf(bucket.get("key")));
                }
                afterKey = (Map<String, Object>) aggregation(page, "c").get("after_key");
            }
            assertEquals("four pages of seven buckets", 28, keysSeen.size());
            assertEquals("no bucket repeats across pages", keysSeen.size(), keysSeen.stream().distinct().count());

            // tdigest percentiles: one sketch per executor on the fragment
            // path, so the values are within the algorithm's error of the
            // shard path's single sketch.
            String tdigest = "{\"size\":0,\"aggs\":{\"p\":{\"percentiles\":{\"field\":\"rating\"}}}}";
            Map<String, Object> viaFragments = parse(readAll(postJson("/" + index + "/_search", tdigest)));
            requests++;
            Map<String, Object> viaShard = parse(
                readAll(postJson("/" + index + "/_search?request_cache=false", "{\"explain\":true," + tdigest.substring(1)))
            );
            Map<String, Object> fragmentValues = (Map<String, Object>) aggregation(viaFragments, "p").get("values");
            Map<String, Object> shardValues = (Map<String, Object>) aggregation(viaShard, "p").get("values");
            assertEquals(shardValues.keySet(), fragmentValues.keySet());
            for (String percentile : shardValues.keySet()) {
                assertRelativeClose(
                    "percentile " + percentile,
                    ((Number) shardValues.get(percentile)).doubleValue(),
                    ((Number) fragmentValues.get(percentile)).doubleValue(),
                    0.01d
                );
            }

            // cardinality: the true count comes from the fixture layout and
            // the default precision threshold (3000) keeps the HyperLogLog++
            // in its linear counting range for this many distinct values.
            java.util.Set<Long> distinctRatings = new java.util.HashSet<>();
            for (int i = 0; i < 1200; i++) {
                if (i % 5 != 4) {
                    distinctRatings.add((long) ((i * 37) % 1000));
                }
            }
            String cardinality =
                "{\"size\":0,\"aggs\":{\"c\":{\"cardinality\":{\"field\":\"rating\"}},\"k\":{\"cardinality\":{\"field\":\"category\"}}}}";
            Map<String, Object> counted = parse(readAll(postJson("/" + index + "/_search", cardinality)));
            requests++;
            assertRelativeClose(
                "cardinality(rating)",
                distinctRatings.size(),
                ((Number) aggregation(counted, "c").get("value")).doubleValue(),
                0.01d
            );
            assertEquals(3, ((Number) aggregation(counted, "k").get("value")).intValue());

            long fanOut = fanOutLogLines(index);
            assertEquals("every request above took the fragment path", fanOutBefore + requests, fanOut);

            // A filter bucket over a Lance query and a scripted metric stay
            // on the shard path: they answer, and leave no fan-out line.
            String ftsFilter = "{\"size\":0,\"aggs\":{\"f\":{\"filter\":{\"lance_match\":{\"field\":\"body\",\"query\":\"grp7\"}}}}}";
            Map<String, Object> viaShardOnly = parse(readAll(postJson("/" + index + "/_search", ftsFilter)));
            assertEquals(48, ((Number) aggregation(viaShardOnly, "f").get("doc_count")).intValue());
            assertEquals(fanOut, fanOutLogLines(index));
        }
    }

    /**
     * {@code date_range} and a {@code composite} over a
     * {@code date_histogram} source on a timestamp column, against the
     * interleaved fixture (ts is 2024-01-01 plus {@code id} days).
     */
    public void testDateAggregationsOnTheWiderAllowListAnswerLikeTheShardPath() throws Exception {
        String suffix = "allow-dates-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeInterleavedTable(scratchDir, tableName, 4, 30);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            long fanOutBefore = fanOutLogLines(indexName);
            String[] shapes = {
                "\"size\":0,\"aggs\":{\"d\":{\"date_range\":{\"field\":\"ts\",\"format\":\"yyyy-MM-dd\",\"ranges\":[{\"to\":\"2024-02-01\"},{\"from\":\"2024-02-01\",\"to\":\"2024-04-01\"},{\"from\":\"2024-04-01\"}]},\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"}}}}}",
                "\"size\":0,\"aggs\":{\"c\":{\"composite\":{\"size\":3,\"sources\":[{\"month\":{\"date_histogram\":{\"field\":\"ts\",\"calendar_interval\":\"month\"}}},{\"cat\":{\"terms\":{\"field\":\"category\"}}}]},\"aggs\":{\"s\":{\"stats\":{\"field\":\"id\"}}}}}",
                "\"size\":0,\"aggs\":{\"c\":{\"composite\":{\"size\":3,\"after\":{\"month\":1706745600000,\"cat\":\"c0\"},\"sources\":[{\"month\":{\"date_histogram\":{\"field\":\"ts\",\"calendar_interval\":\"month\"}}},{\"cat\":{\"terms\":{\"field\":\"category\"}}}]}}}",
                "\"size\":0,\"query\":{\"range\":{\"ts\":{\"gte\":\"2024-02-15\"}}},\"aggs\":{\"d\":{\"date_range\":{\"field\":\"ts\",\"ranges\":[{\"to\":\"2024-03-01\"},{\"from\":\"2024-03-01\"}]}}}" };
            for (String shape : shapes) {
                assertShardPathAgrees(indexName, shape);
            }
            assertEquals(fanOutBefore + shapes.length, fanOutLogLines(indexName));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }
}
