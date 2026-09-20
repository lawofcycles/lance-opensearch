/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.util.List;

import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;

/**
 * The {@code lance_knn} query: vector search, validation errors,
 * pre-filtering, and composition with full-text clauses inside a {@code
 * bool} query.
 */
public class LanceKnnQueryIT extends LanceRestTestCase {

    public void testAttachAndKnn() throws Exception {
        // Row i sits at (i, 0, 0, ...), so the nearest neighbours of
        // (2.4, 0, ...) are rows 2 and 3. Below 256 rows no vector index
        // is built and the search is a brute-force scan.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(12, "attachAndKnn")) {
            String indexName = fixture.indexName();

            String queryVector = "[2.4,0,0,0,0,0,0,0]";
            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"size\":2,\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":" + queryVector + ",\"k\":2}}}"
            );
            String body = readAll(search);
            int firstId = extractIntPath(body, "hits", "hits", "0", "_source", "id");
            int secondId = extractIntPath(body, "hits", "hits", "1", "_source", "id");
            assertEquals("expected row 2 as nearest, saw: " + body, 2, firstId);
            assertEquals("expected row 3 as second, saw: " + body, 3, secondId);
        }
    }

    public void testLanceKnnRejectsUnknownField() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "knnUnknown")) {
            String indexName = fixture.indexName();
            String queryVector = "[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]";
            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_knn\":{\"field\":\"noSuchField\",\"vector\":" + queryVector + ",\"k\":2}}}"
                )
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for unknown field, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about noSuchField, saw: " + body, body.contains("noSuchField"));
        }
    }

    public void testLanceKnnRejectsScalarField() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "knnScalar")) {
            String indexName = fixture.indexName();
            String queryVector = "[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]";
            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_knn\":{\"field\":\"id\",\"vector\":" + queryVector + ",\"k\":2}}}"
                )
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for scalar field, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about non-lance_vector, saw: " + body, body.contains("lance_vector"));
        }
    }

    public void testLanceKnnRejectsDimensionMismatch() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "knnDim")) {
            String indexName = fixture.indexName();
            // The column is FixedSizeList<Float32, 8>.
            String queryVector = "[0.1,0.2,0.3]";
            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":" + queryVector + ",\"k\":2}}}"
                )
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for dimension mismatch, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about dimension, saw: " + body, body.contains("dimension"));
        }
    }

    public void testLanceKnnAppliesFilterAsPreFilter() throws Exception {
        // The filter is applied before the top-k: with id >= 10 the two
        // nearest rows are 10 and 11. A post-filter would first pick rows
        // 2 and 3 and then drop them, returning nothing.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "knnPreFilter")) {
            String indexName = fixture.indexName();
            String queryVector = "[2.4,0,0,0,0,0,0,0]";
            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"size\":2,\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":"
                    + queryVector
                    + ",\"k\":2,\"filter\":{\"range\":{\"id\":{\"gte\":10}}}}}}"
            );
            String body = readAll(search);
            int totalHits = extractIntPath(body, "hits", "total", "value");
            assertEquals("expected 2 hits for id >= 10 with k=2, saw: " + body, 2, totalHits);
            int firstId = extractIntPath(body, "hits", "hits", "0", "_source", "id");
            int secondId = extractIntPath(body, "hits", "hits", "1", "_source", "id");
            assertEquals("expected id 10 as nearest match >= 10, saw: " + body, 10, firstId);
            assertEquals("expected id 11 as second nearest match, saw: " + body, 11, secondId);
        }
    }

    public void testLanceKnnFilterAcceptsBoolFilterClause() throws Exception {
        // Same expectation with the range nested inside bool.filter.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "knnPreFilterBool")) {
            String indexName = fixture.indexName();
            String queryVector = "[2.4,0,0,0,0,0,0,0]";
            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"size\":2,\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":"
                    + queryVector
                    + ",\"k\":2,\"filter\":{\"bool\":{\"filter\":[{\"range\":{\"id\":{\"gte\":10}}}]}}}}}"
            );
            String body = readAll(search);
            int totalHits = extractIntPath(body, "hits", "total", "value");
            assertEquals("expected 2 hits for bool filter id >= 10, saw: " + body, 2, totalHits);
            int firstId = extractIntPath(body, "hits", "hits", "0", "_source", "id");
            assertEquals("expected id 10 as nearest match, saw: " + body, 10, firstId);
        }
    }

    public void testLanceKnnRejectsUnsupportedFilterClause() throws Exception {
        // A match clause has no Lance SQL form and is rejected up front.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "knnPreFilterMatch")) {
            String indexName = fixture.indexName();
            String queryVector = "[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]";
            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":"
                        + queryVector
                        + ",\"k\":2,\"filter\":{\"match\":{\"body\":\"hello\"}}}}}"
                )
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for unsupported filter, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about MatchQueryBuilder, saw: " + body, body.contains("MatchQueryBuilder"));
        }
    }

    public void testLanceKnnWithSortAndAggregationsMatchesScalarReference() throws Exception {
        // Rows sit at (id, 0, ...), so the five nearest to 250.4 are rows
        // 248..252, all in fragment 1 (offsets 48..52). The knn Weight
        // hints the leaf with those rows and the sort and aggregation
        // columns are fetched for them alone; a terms filter on the same
        // ids is the hint-free reference. Row 249 has a null rating, so
        // the missing handling is part of the comparison.
        try (LanceTestCluster fixture = LanceTestCluster.setUpHintFixture(3, 200, "knnhintsort")) {
            String indexName = fixture.indexName();
            String knn = "{\"lance_knn\":{\"field\":\"embedding\",\"vector\":[250.4,0,0,0,0,0,0,0],\"k\":5}}";
            String reference = "{\"terms\":{\"id\":[248,249,250,251,252]}}";
            for (String sort : List.of(
                "\"sort\":[{\"rating\":\"desc\"},{\"id\":\"asc\"}]",
                "\"sort\":[{\"category\":\"asc\"},{\"id\":\"asc\"}]",
                "\"sort\":[{\"flag\":\"desc\"},{\"id\":\"asc\"}]"
            )) {
                String actual = readAll(postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":" + knn + "," + sort + "}"));
                String expected = readAll(postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":" + reference + "," + sort + "}"));
                assertEquals(5, extractIntPath(actual, "hits", "total", "value"));
                assertEquals(sort, idsAndSortValuesOf(expected), idsAndSortValuesOf(actual));
            }
            String byRating = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":10,\"query\":" + knn + ",\"sort\":[{\"rating\":\"desc\"},{\"id\":\"asc\"}]}"
                )
            );
            // 252 * 37 mod 1000 = 324, 251 -> 287, 250 -> 250, 248 -> 176, 249 -> null last.
            assertEquals(List.of("1-52", "1-51", "1-50", "1-48", "1-49"), idsOf(hitsOf(byRating)));

            String aggs = "\"aggs\":{\"by_category\":{\"terms\":{\"field\":\"category\",\"size\":10}},"
                + "\"by_tag\":{\"terms\":{\"field\":\"tags\",\"size\":10}},"
                + "\"max_rating\":{\"max\":{\"field\":\"rating\"}}}";
            for (String size : List.of("0", "3")) {
                String actual = readAll(
                    postJson("/" + indexName + "/_search", "{\"size\":" + size + ",\"query\":" + knn + "," + aggs + "}")
                );
                String expected = readAll(
                    postJson("/" + indexName + "/_search", "{\"size\":" + size + ",\"query\":" + reference + "," + aggs + "}")
                );
                assertEquals(bucketsOf(expected, "by_category"), bucketsOf(actual, "by_category"));
                assertEquals(bucketsOf(expected, "by_tag"), bucketsOf(actual, "by_tag"));
                assertEquals(324d, extractDoublePath(actual, "aggregations", "max_rating", "value"), 0d);
            }
        }
    }

    public void testBoolShouldComposesLanceMatchWithLanceKnn() throws Exception {
        // Two Lance sub-queries composed by bool.should: lance_match
        // "hello" matches the eight even rows, lance_knn near (0.5, 0,
        // ...) with k=2 returns rows 0 and 1, so the union is nine rows.
        // Compound queries such as neural-search's hybrid drive the
        // sub-queries through the same Weight / Scorer contract.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "hybridboolshould")) {
            String indexName = fixture.indexName();
            String queryVector = "[0.5,0,0,0,0,0,0,0]";
            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"size\":16,\"query\":{\"bool\":{\"should\":["
                    + "{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},"
                    + "{\"lance_knn\":{\"field\":\"embedding\",\"vector\":"
                    + queryVector
                    + ",\"k\":2}}"
                    + "]}}}"
            );
            String body = readAll(search);
            int hits = extractIntPath(body, "hits", "total", "value");
            assertEquals("expected 9 unique rows from union(match, knn): " + body, 9, hits);
            // Row 0 satisfies both clauses and scores highest.
            int topId = extractIntPath(body, "hits", "hits", "0", "_source", "id");
            assertEquals("expected row 0 (matches both sub-queries) at top of hits: " + body, 0, topId);
        }
    }

    public void testBoolShouldComposesStockMatchOnLanceTextWithLanceKnn() throws Exception {
        // The same composition with a stock match on the lance_text
        // field instead of lance_match.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "hybridstockmatchknn")) {
            String indexName = fixture.indexName();
            String queryVector = "[0.5,0,0,0,0,0,0,0]";
            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"size\":16,\"query\":{\"bool\":{\"should\":["
                    + "{\"match\":{\"body\":\"hello\"}},"
                    + "{\"lance_knn\":{\"field\":\"embedding\",\"vector\":"
                    + queryVector
                    + ",\"k\":2}}"
                    + "]}}}"
            );
            String body = readAll(search);
            int hits = extractIntPath(body, "hits", "total", "value");
            assertEquals("expected 9 unique rows from union(match, knn): " + body, 9, hits);
            int topId = extractIntPath(body, "hits", "hits", "0", "_source", "id");
            assertEquals("expected row 0 at top: " + body, 0, topId);
        }
    }
}
