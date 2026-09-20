/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.core.rest.RestStatus;

/**
 * Sorting on the fragment dispatch path: the Lance ordered-scan pushdown and
 * its agreement with the Lucene collector path, sort values, {@code
 * max_score} and {@code track_scores}, and sort combined with match / knn
 * queries.
 */
public class LanceSortIT extends LanceRestTestCase {

    public void testSortPushdownMatchesLuceneOrderAndSortValues() throws Exception {
        // Sorted scalar-filter pages without aggregations, post_filter
        // or search_after run as one ordered, limited Lance scan. The
        // scan must reproduce the Lucene collector's order, its
        // handling of `_last` / `_first`, and the typed sort values.
        // Adding a trivial aggregation forces the Lucene path, which
        // serves as the oracle: the hits of the two requests must be
        // identical.
        try (LanceTestCluster fixture = LanceTestCluster.setUpNullable("sortpush")) {
            String indexName = fixture.indexName();
            String oracleAgg = ",\"aggs\":{\"n\":{\"value_count\":{\"field\":\"id\"}}}";

            // One null row (id=5) sorts last.
            String desc = "{\"size\":12,\"sort\":[{\"count16\":\"desc\"}]";
            java.util.List<java.util.Map<String, Object>> descHits = hitsOf(readAll(postJson("/" + indexName + "/_search", desc + "}")));
            java.util.List<java.util.Map<String, Object>> descOracle = hitsOf(
                readAll(postJson("/" + indexName + "/_search", desc + oracleAgg + "}"))
            );
            assertEquals("count16 desc must match the Lucene path", descOracle, descHits);
            assertEquals(
                java.util.List.of("0-11", "0-10", "0-9", "0-8", "0-7", "0-6", "0-4", "0-3", "0-2", "0-1", "0-0", "0-5"),
                idsOf(descHits)
            );
            assertEquals(java.util.List.of(1100), sortValuesOf(descHits.get(0)));
            // count16 is mapped as short, which sorts as SortField.Type.INT,
            // so the missing sentinel is Integer.MIN_VALUE.
            assertEquals(java.util.List.of(Integer.MIN_VALUE), sortValuesOf(descHits.get(11)));

            // Boolean key with a numeric tie-break; flag is null on id=5.
            String multi = "{\"size\":12,\"sort\":[{\"flag\":\"asc\"},{\"id\":\"desc\"}]";
            java.util.List<java.util.Map<String, Object>> multiHits = hitsOf(readAll(postJson("/" + indexName + "/_search", multi + "}")));
            java.util.List<java.util.Map<String, Object>> multiOracle = hitsOf(
                readAll(postJson("/" + indexName + "/_search", multi + oracleAgg + "}"))
            );
            assertEquals("flag asc, id desc must match the Lucene path", multiOracle, multiHits);
            assertEquals(
                java.util.List.of("0-11", "0-10", "0-8", "0-7", "0-4", "0-2", "0-1", "0-9", "0-6", "0-3", "0-0", "0-5"),
                idsOf(multiHits)
            );
            assertEquals(java.util.List.of(0, 11), sortValuesOf(multiHits.get(0)));
            assertEquals(java.util.List.of(1, 9), sortValuesOf(multiHits.get(7)));

            // missing:_first flips where the null row lands.
            String first = "{\"size\":3,\"sort\":[{\"count64\":{\"order\":\"asc\",\"missing\":\"_first\"}}]";
            java.util.List<java.util.Map<String, Object>> firstHits = hitsOf(readAll(postJson("/" + indexName + "/_search", first + "}")));
            java.util.List<java.util.Map<String, Object>> firstOracle = hitsOf(
                readAll(postJson("/" + indexName + "/_search", first + oracleAgg + "}"))
            );
            assertEquals("count64 asc missing:_first must match the Lucene path", firstOracle, firstHits);
            assertEquals(java.util.List.of("0-5", "0-0", "0-1"), idsOf(firstHits));

            // Filter + sort + limit; hits.total keeps the full match count.
            String filtered = "{\"size\":3,\"query\":{\"range\":{\"id\":{\"gte\":3,\"lt\":10}}},\"sort\":[{\"count8\":\"desc\"}]";
            String filteredBody = readAll(postJson("/" + indexName + "/_search", filtered + "}"));
            java.util.List<java.util.Map<String, Object>> filteredHits = hitsOf(filteredBody);
            java.util.List<java.util.Map<String, Object>> filteredOracle = hitsOf(
                readAll(postJson("/" + indexName + "/_search", filtered + oracleAgg + "}"))
            );
            assertEquals("range + count8 desc must match the Lucene path", filteredOracle, filteredHits);
            assertEquals(java.util.List.of("0-9", "0-8", "0-7"), idsOf(filteredHits));
            assertEquals(7, extractIntPath(filteredBody, "hits", "total", "value"));

            // A literal missing value cannot be pushed down and takes the
            // Lucene path.
            String literalMissing = "{\"size\":12,\"sort\":[{\"count16\":{\"order\":\"asc\",\"missing\":250}}]}";
            java.util.List<java.util.Map<String, Object>> literalHits = hitsOf(
                readAll(postJson("/" + indexName + "/_search", literalMissing))
            );
            assertEquals(
                java.util.List.of("0-0", "0-1", "0-2", "0-5", "0-3", "0-4", "0-6", "0-7", "0-8", "0-9", "0-10", "0-11"),
                idsOf(literalHits)
            );
        }
    }

    public void testMaxScoreAndTrackScoresOnFragmentPath() throws Exception {
        // max_score is computed from the returned page, and track_scores
        // decides whether a sorted search also computes scores: without
        // it the hits carry _score null and max_score is null too.
        String suffix = "s3-trackscores-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 4);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // constant_score without sort: every hit carries the boost.
            String noSortBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":4,\"query\":{\"constant_score\":{\"filter\":{\"match_all\":{}},\"boost\":3.5}}}"
                )
            );
            assertEquals(4, extractIntPath(noSortBody, "hits", "total", "value"));
            assertEquals(3.5d, extractDoublePath(noSortBody, "hits", "max_score"), 0.0001d);
            assertEquals(3.5d, extractDoublePath(noSortBody, "hits", "hits", "0", "_score"), 0.0001d);
            assertEquals(3.5d, extractDoublePath(noSortBody, "hits", "hits", "3", "_score"), 0.0001d);

            // sort + track_scores: scores alongside sort values.
            String trackBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":4,\"query\":{\"constant_score\":{\"filter\":{\"match_all\":{}},\"boost\":3.5}},"
                        + "\"sort\":[{\"id\":\"desc\"}],\"track_scores\":true}"
                )
            );
            assertEquals(4, extractIntPath(trackBody, "hits", "total", "value"));
            assertEquals(3.5d, extractDoublePath(trackBody, "hits", "max_score"), 0.0001d);
            assertEquals(3.5d, extractDoublePath(trackBody, "hits", "hits", "0", "_score"), 0.0001d);
            assertEquals(3.5d, extractDoublePath(trackBody, "hits", "hits", "3", "_score"), 0.0001d);
            assertTrue("sort desc must start with id=3: " + trackBody, trackBody.contains("\"sort\":[3]"));

            // sort without track_scores: NaN scores serialise as null.
            String nullScoreBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":4,\"query\":{\"constant_score\":{\"filter\":{\"match_all\":{}},\"boost\":3.5}},"
                        + "\"sort\":[{\"id\":\"desc\"}]}"
                )
            );
            assertEquals(4, extractIntPath(nullScoreBody, "hits", "total", "value"));
            assertTrue(
                "sort without track_scores must produce max_score:null: " + nullScoreBody,
                nullScoreBody.contains("\"max_score\":null")
            );
            assertTrue(
                "sort without track_scores must produce per-hit _score:null: " + nullScoreBody,
                nullScoreBody.contains("\"_score\":null")
            );
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAnswersMatchKnnAndSort() throws Exception {
        // match, knn and sort by a field all return real scores and sort
        // values through the fragment dispatch path.
        String suffix = "s3-mks-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // "hello lance" is on the three even rows.
            String matchBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match\":{\"body\":\"lance\"}}}"));
            assertEquals(3, extractIntPath(matchBody, "hits", "total", "value"));
            assertTrue("match hits must carry a positive Lucene score: " + matchBody, matchBody.contains("\"_score\":"));
            assertFalse("match hits must carry real scores rather than a constant 1.0: " + matchBody, matchBody.contains("\"_score\":1.0"));

            // Row i has embedding[0]=i and zeros elsewhere, so a query on
            // the first axis ranks id=5 highest.
            String queryVector = "[0.5,0.0,0.0,0.0,0.0,0.0,0.0,0.0]";
            String knnBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":3,\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":" + queryVector + ",\"k\":3}}}"
                )
            );
            assertEquals(3, extractIntPath(knnBody, "hits", "total", "value"));
            assertTrue("knn top hit must have a positive score: " + knnBody, knnBody.contains("\"_score\":"));

            String sortBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":3,\"query\":{\"match_all\":{}},\"sort\":[{\"id\":\"desc\"}]}")
            );
            assertEquals(6, extractIntPath(sortBody, "hits", "total", "value"));
            assertTrue("sort hits must carry sort values: " + sortBody, sortBody.contains("\"sort\":[5]"));
            assertTrue("second sort hit must have sort value [4]: " + sortBody, sortBody.contains("\"sort\":[4]"));
            assertTrue("third sort hit must have sort value [3]: " + sortBody, sortBody.contains("\"sort\":[3]"));
            // No primary key: _id is the synthesised "<fragment>-<offset>".
            int firstIdx = sortBody.indexOf("\"_id\":");
            assertTrue("expected an _id in sorted response: " + sortBody, firstIdx >= 0);
            String firstIdSlice = sortBody.substring(firstIdx, Math.min(sortBody.length(), firstIdx + 20));
            assertTrue("first sorted hit must be _id = \"0-5\": " + firstIdSlice, firstIdSlice.contains("\"0-5\""));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testKeywordSortAgreesAcrossCacheStates() throws Exception {
        // A keyword sort through the Lucene comparator reads the
        // fragment's dictionary and ordinals: from the off-heap store
        // when the snapshot cache holds them (the second request), from
        // a request scoped heap dictionary when the cache is disabled.
        // The order and the sort values must not depend on the source.
        // A trivial aggregation keeps the request on the comparator path
        // rather than the ordered Lance scan pushdown; the pushdown is
        // asked too as a third oracle.
        try (LanceTestCluster fixture = LanceTestCluster.setUpHintFixture(3, 200, "kwsort")) {
            String index = fixture.indexName();
            String oracleAgg = ",\"aggs\":{\"n\":{\"value_count\":{\"field\":\"id\"}}}";
            String asc = "{\"size\":10,\"query\":{\"match_all\":{}},\"sort\":[{\"category\":\"asc\"},{\"id\":\"asc\"}]";
            String desc = "{\"size\":10,\"query\":{\"match_all\":{}},\"sort\":[{\"category\":\"desc\"},{\"id\":\"asc\"}]";
            String filtered =
                "{\"size\":10,\"query\":{\"range\":{\"rating\":{\"gte\":500}}},\"sort\":[{\"category\":\"asc\"},{\"id\":\"desc\"}]";
            String[] shapes = { asc + oracleAgg + "}", desc + oracleAgg + "}", filtered + oracleAgg + "}" };
            // category is "c" + (i % 3), null when i % 4 == 3: the c0 rows
            // in id order are 0, 6, 9, 12, 18, 21, 24, 30, 33, 36.
            List<String> ascIds = List.of("0-0", "0-6", "0-9", "0-12", "0-18", "0-21", "0-24", "0-30", "0-33", "0-36");
            List<String> descIds = List.of("0-2", "0-5", "0-8", "0-14", "0-17", "0-20", "0-26", "0-29", "0-32", "0-38");

            List<List<Map<String, Object>>> cold = new ArrayList<>();
            for (String shape : shapes) {
                cold.add(hitsOf(readAll(postJson("/" + index + "/_search", shape))));
            }
            assertEquals(ascIds, idsOf(cold.get(0)));
            assertEquals(List.of("c0", 0), sortValuesOf(cold.get(0).get(0)));
            assertEquals(descIds, idsOf(cold.get(1)));
            assertEquals(List.of("c2", 2), sortValuesOf(cold.get(1).get(0)));

            // Warm: the dictionaries now come from the store.
            for (int i = 0; i < shapes.length; i++) {
                assertEquals(
                    "warm request differs for " + shapes[i],
                    cold.get(i),
                    hitsOf(readAll(postJson("/" + index + "/_search", shapes[i])))
                );
            }
            // The ordered Lance scan pushdown agrees with the comparator.
            assertEquals(ascIds, idsOf(hitsOf(readAll(postJson("/" + index + "/_search", asc + "}")))));
            assertEquals(descIds, idsOf(hitsOf(readAll(postJson("/" + index + "/_search", desc + "}")))));

            Request disable = new Request("PUT", "/_cluster/settings");
            disable.setJsonEntity("{\"transient\":{\"lance.cache.enabled\":false}}");
            client().performRequest(disable);
            try {
                for (int i = 0; i < shapes.length; i++) {
                    assertEquals(
                        "uncached request differs for " + shapes[i],
                        cold.get(i),
                        hitsOf(readAll(postJson("/" + index + "/_search", shapes[i])))
                    );
                }
            } finally {
                Request enable = new Request("PUT", "/_cluster/settings");
                enable.setJsonEntity("{\"transient\":{\"lance.cache.enabled\":null}}");
                client().performRequest(enable);
            }
        }
    }
}
