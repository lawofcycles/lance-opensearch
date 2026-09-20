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
 * Sorting on the fragment dispatch path: the Lance ordered-scan pushdown and
 * its agreement with the Lucene collector path, sort values, {@code
 * max_score} and {@code track_scores}, and sort combined with match / knn
 * queries.
 */
public class LanceSortIT extends LanceRestTestCase {

    public void testSortPushdownMatchesLuceneOrderAndSortValues() throws Exception {
        // Sorted scalar-filter pages (match_all or a filter the
        // coordinator translated to Lance SQL, no aggregations, no
        // post_filter, no search_after) run as one Lance scan with
        // ColumnOrderings + limit instead of a Lucene TopFieldCollector
        // over every matching row. The pushdown has to reproduce
        // Lucene's order, its handling of missing values (`_last` /
        // `_first`), and the typed sort values clients feed back as
        // search_after. Adding a trivial aggregation to the same
        // request forces the Lucene path (aggregations need the full
        // match set), which gives an in-cluster oracle: the hits of
        // the two requests must be identical.
        try (LanceTestCluster fixture = LanceTestCluster.setUpNullable("sortpush")) {
            String indexName = fixture.indexName();
            String oracleAgg = ",\"aggs\":{\"n\":{\"value_count\":{\"field\":\"id\"}}}";

            // Single numeric key, descending, one null row (id=5).
            // `_last` on a descending sort puts the null at the end
            // and Lucene reports Long.MIN_VALUE as its sort value.
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
            // count16 is mapped as `short`, which OpenSearch sorts with
            // SortField.Type.INT, so the `_last` sentinel on a
            // descending sort is Integer.MIN_VALUE, not Long.MIN_VALUE.
            assertEquals(java.util.List.of(Integer.MIN_VALUE), sortValuesOf(descHits.get(11)));

            // Two keys with a boolean primary key and a numeric
            // tie-break, plus the null row (flag is null on id=5) at
            // the end.
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

            // Filter + sort + limit: Lance evaluates the filter and the
            // top-k in one pass; hits.total still reports the full
            // match count.
            String filtered = "{\"size\":3,\"query\":{\"range\":{\"id\":{\"gte\":3,\"lt\":10}}},\"sort\":[{\"count8\":\"desc\"}]";
            String filteredBody = readAll(postJson("/" + indexName + "/_search", filtered + "}"));
            java.util.List<java.util.Map<String, Object>> filteredHits = hitsOf(filteredBody);
            java.util.List<java.util.Map<String, Object>> filteredOracle = hitsOf(
                readAll(postJson("/" + indexName + "/_search", filtered + oracleAgg + "}"))
            );
            assertEquals("range + count8 desc must match the Lucene path", filteredOracle, filteredHits);
            assertEquals(java.util.List.of("0-9", "0-8", "0-7"), idsOf(filteredHits));
            assertEquals(7, extractIntPath(filteredBody, "hits", "total", "value"));

            // A literal missing value has no ColumnOrdering equivalent
            // and must keep working through the Lucene fallback.
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
        // Issue #37 case 2: the fragment executor used to write
        // hits.max_score as a hard-coded 1.0 regardless of the
        // real per-hit score, and never honoured track_scores
        // when combined with a sort. Both quirks silently
        // changed the response envelope compared to the shard
        // path. Since the case 2 fix the coordinator's
        // MergeState computes max_score from the paged window,
        // and per-node Lucene search wires track_scores through
        // to the 4 / 5 argument search / searchAfter overloads.
        // Three shapes exercise those seams:
        // 1. constant_score without sort. Scores fall out of
        // IndexSearcher.search(query, size), max_score
        // lifts to the caller-chosen boost.
        // 2. constant_score with sort by id and
        // track_scores:true. The 4 arg
        // search(query, size, sort, true) collects both
        // sort values and scores, so hits carry the boost
        // and max_score does too.
        // 3. constant_score with sort by id and no
        // track_scores. Lucene's sort collector skips
        // score computation, hits carry _score:null, and
        // max_score comes back as null after the NaN
        // guard in MergeState skips every hit.
        String suffix = "s3-trackscores-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 4);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // Shape 1: constant_score without sort. Every hit
            // carries the boost, max_score matches.
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

            // Shape 2: sort by id with track_scores:true. Score
            // stays populated alongside sort values.
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
            // Sort by id desc puts id=3 first.
            assertTrue("sort desc must start with id=3: " + trackBody, trackBody.contains("\"sort\":[3]"));

            // Shape 3: sort by id with no track_scores. Lucene
            // reports NaN for every hit, JSON encodes that as
            // null. max_score falls out to null as well because
            // MergeState skips NaN hits before picking the max.
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
        // Direction 1 Stage 3: match on lance_text, knn on
        // lance_knn, and sort now flow through the fragment
        // executor. The per-node handler translates the QueryBuilder
        // via QueryShardContext.toQuery, drives IndexSearcher.search
        // against the shared per-fragment reader, and returns real
        // Lucene scores + sort values. Before Stage 3 all three
        // shapes fell through to the shard fan-out.
        String suffix = "s3-mks-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // Match on lance_text (body column contains "hello lance"
            // for even rows, "hello world" for odd rows). Fragment
            // path drives LanceFtsQuery via IndexSearcher.search and
            // returns 3 hits (even rows) with real BM25 scores.
            String matchBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match\":{\"body\":\"lance\"}}}"));
            assertEquals(3, extractIntPath(matchBody, "hits", "total", "value"));
            assertTrue("match hits must carry a positive Lucene score: " + matchBody, matchBody.contains("\"_score\":"));
            assertFalse("Stage 3 must not report the hard-coded 1.0 score anymore: " + matchBody, matchBody.contains("\"_score\":1.0"));

            // knn on lance_knn. Table factory writes 8-dim
            // vectors where row i has embedding[0]=i and all
            // other coordinates zero (see LanceTableFactory
            // VECTOR_DIM), so a query vector concentrated on the
            // first axis ranks id=5 highest.
            String queryVector = "[0.5,0.0,0.0,0.0,0.0,0.0,0.0,0.0]";
            String knnBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":3,\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":" + queryVector + ",\"k\":3}}}"
                )
            );
            assertEquals(3, extractIntPath(knnBody, "hits", "total", "value"));
            // Cosine similarity produces a positive score for the
            // nearest neighbour.
            assertTrue("knn top hit must have a positive score: " + knnBody, knnBody.contains("\"_score\":"));

            // Sort by id descending. 6 rows -> ids 0..5, descending
            // means the first hit is id=5, then 4, 3.
            String sortBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":3,\"query\":{\"match_all\":{}},\"sort\":[{\"id\":\"desc\"}]}")
            );
            assertEquals(6, extractIntPath(sortBody, "hits", "total", "value"));
            // Every hit should carry sort values under the "sort" field.
            assertTrue("sort hits must carry sort values: " + sortBody, sortBody.contains("\"sort\":[5]"));
            assertTrue("second sort hit must have sort value [4]: " + sortBody, sortBody.contains("\"sort\":[4]"));
            assertTrue("third sort hit must have sort value [3]: " + sortBody, sortBody.contains("\"sort\":[3]"));
            // Top sorted hit is the row with id=5 (Lance offset 5
            // within fragment 0 because there's no declared PK).
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
}
