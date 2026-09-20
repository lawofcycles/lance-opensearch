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
import java.util.stream.Stream;

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
                // track_total_hits variants
                "{\"size\":0,\"track_total_hits\":true,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"}}}}",
                "{\"size\":0,\"track_total_hits\":false,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"}}}}",
                "{\"size\":0,\"track_total_hits\":100,\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}" };
            String[] aggregatorShapes = new String[] {
                // shapes outside the allow list stay on the aggregators
                "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"t\":{\"terms\":{\"field\":\"tags\"}}}}}}",
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
        // an even / odd keyword category: fixed_interval date histograms
        // and date metrics take the pushdown, calendar_interval and
        // time_zone stay on the aggregators. The attach adds a keyword
        // sub-field so a terms on category.raw resolves to the base column.
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
                "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"first\":{\"min\":{\"field\":\"ts\"}}}}}}" };
            String[] aggregatorShapes = new String[] {
                "{\"size\":0,\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"calendar_interval\":\"month\"}}}}",
                "{\"size\":0,\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"fixed_interval\":\"30d\",\"time_zone\":\"+09:00\"}}}}",
                "{\"size\":0,\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"fixed_interval\":\"30d\",\"offset\":\"1d\"}}}}",
                "{\"size\":0,\"aggs\":{\"d\":{\"date_histogram\":{\"field\":\"ts\",\"fixed_interval\":\"30d\","
                    + "\"extended_bounds\":{\"min\":\"2023-12-01\",\"max\":\"2024-07-01\"}}}}}" };
            assertPushdownAgreesWithAggregators(indexName, shapes, aggregatorShapes);
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
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
        Path clustersDir = sharedRoot().resolveSibling("testclusters");
        assertTrue("testclusters directory not found at " + clustersDir, Files.isDirectory(clustersDir));
        String marker = "lance.dispatch: aggregation pushdown for [" + indexName + "]";
        long count = 0;
        try (Stream<Path> files = Files.walk(clustersDir)) {
            for (Path file : files.filter(Files::isRegularFile).toList()) {
                String name = file.getFileName().toString();
                if (!name.endsWith(".log") || name.startsWith("opensearch.") || !file.getParent().getFileName().toString().equals("logs")) {
                    continue;
                }
                for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                    if (line.contains(marker)) {
                        count++;
                    }
                }
            }
        }
        return count;
    }
}
