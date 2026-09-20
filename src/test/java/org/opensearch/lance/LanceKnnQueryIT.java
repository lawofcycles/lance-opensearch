/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;

/**
 * The {@code lance_knn} query: vector search, validation errors,
 * pre-filtering, and composition with full-text clauses inside a {@code
 * bool} query.
 */
public class LanceKnnQueryIT extends LanceRestTestCase {

    public void testAttachAndKnn() throws Exception {
        // Row i sits at coordinate (i, 0, 0, ...) so the nearest neighbour
        // of (2.4, 0, ...) is row 2 followed by row 3. Vector index build is
        // skipped (256-row floor); the plugin falls back to a brute-force
        // scan, which is fine for a 12-row table.
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
            // The fixture writes a FixedSizeList<Float32, 8>; a 3-element
            // query vector must be rejected up front rather than reaching
            // Lance.
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
        // With row i at coordinate (i, 0, ...), the two rows nearest to
        // (2.4, 0, ...) are id 2 and id 3. A pre-filter of id >= 10 must
        // keep k=2 populated with the two nearest matches among {10..15},
        // i.e. id 10 (distance 7.6) and id 11 (distance 8.6). A post-filter
        // would pull id 2 / 3 into the top-K first, then drop them, and
        // return 0 hits.
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
        // Same expectation as testLanceKnnAppliesFilterAsPreFilter but with
        // the range clause nested inside bool.filter, so the translator's
        // bool + range path is exercised end-to-end.
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
        // `match` cannot be lowered to a Lance SQL filter safely (analysis
        // would happen server-side, not in Lance), so the translator
        // rejects it up front with 400. Doing so beats silently degrading
        // recall or returning an obscure Lance parse error.
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

    public void testBoolShouldComposesLanceMatchWithLanceKnn() throws Exception {
        // Hybrid-shape query at the shard level: two independent Lance
        // sub-queries fan out inside a bool.should. lance_match:body:hello
        // matches every even row (0, 2, 4, ..., 14; 8 rows). lance_knn
        // near (0.5, 0, ..., 0) with k=2 returns the two closest rows
        // by Euclidean distance, which are id 0 (distance 0.5) and id 1
        // (distance 0.5). The union is 9 rows: id 1 is the only knn hit
        // not already in the match set.
        //
        // This is the same per-shard plumbing neural-search's `hybrid`
        // query relies on: HybridQuery.createWeight iterates the
        // sub-queries, calls createWeight on each, and composes their
        // scorers. As long as our Lance queries honour the Lucene
        // Weight / ScorerSupplier / Scorer contract inside a compound
        // query (as this test proves for bool.should), the hybrid query
        // will drive them identically.
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
            // Row 0 satisfies both sub-queries and must score highest of
            // any single-sub-query hit, so the sort by _score lands it
            // first. This is Lucene's bool.should sum-of-child-scores
            // behaviour; hybrid replaces the sum with per-sub-query
            // top-K + coordinator-side normalisation, but the shard-side
            // requirement is the same: each sub-query yields the same
            // scored docs it would on its own.
            int topId = extractIntPath(body, "hits", "hits", "0", "_source", "id");
            assertEquals("expected row 0 (matches both sub-queries) at top of hits: " + body, 0, topId);
        }
    }

    public void testBoolShouldComposesStockMatchOnLanceTextWithLanceKnn() throws Exception {
        // Stock OpenSearch `match` on a lance_text field goes through
        // LanceTextFieldMapper.termQuery, which builds a LanceFtsQuery
        // for the single-token case. Confirm the composition works the
        // same way when the FTS clause uses the plain match DSL rather
        // than the lance_match DSL: the shard-side composition contract
        // that hybrid depends on does not vary between them.
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
