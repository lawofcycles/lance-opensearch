/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.opensearch.client.Request;
import org.opensearch.client.Response;

/**
 * The hits envelope through the planner: a sorted page folds into the
 * scan as a pushed top-k (visible in the explain output), round trips
 * {@code _id} / {@code _source} / sort values, a {@code size: 0}
 * request keeps the count path, and a {@code search_after}
 * continuation folds as a cursor bound and pages through the table in
 * agreement with the Lucene collector.
 */
public class LanceHitShapeIT extends LanceRestTestCase {

    private static String explainBody(String indexName, String body) throws IOException {
        Request request = new Request("GET", "/" + indexName + "/_lance/explain");
        request.setJsonEntity(body);
        Response response = client().performRequest(request);
        return readAll(response);
    }

    public void testSortedPageFoldsAndRoundTripsTheEnvelope() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUpNullable("hitshape")) {
            String indexName = fixture.indexName();

            // The explain output shows the fold: the logical plan is the
            // hit shape over the top-k over the scan, the physical plan
            // is the coordinator's merge and fan out over the scan
            // carrying the pushed page.
            String explained = explainBody(indexName, "{\"size\":3,\"sort\":[{\"count16\":\"desc\"}]}");
            String logical = stringPath(explained, "logical");
            assertTrue("logical plan carries the hit shape: " + logical, logical.contains("LanceHitShape"));
            assertTrue("logical plan carries the top-k: " + logical, logical.contains("LanceTopK"));
            String physical = stringPath(explained, "physical");
            assertTrue("the coordinator merge leads: " + physical, physical.startsWith("MergeExec("));
            assertTrue("the per node plan is the scan: " + physical, physical.contains("LanceTableScan("));
            assertTrue("the page is pushed: " + physical, physical.contains("topk{"));
            assertTrue("the page carries its fetch: " + physical, physical.contains("fetch=3"));

            // The page itself: _id, _source and typed sort values.
            String body = readAll(postJson("/" + indexName + "/_search", "{\"size\":3,\"sort\":[{\"count16\":\"desc\"}]}"));
            List<Map<String, Object>> hits = hitsOf(body);
            assertEquals(List.of("0-11", "0-10", "0-9"), idsOf(hits));
            assertEquals(List.of(1100), sortValuesOf(hits.get(0)));
            @SuppressWarnings("unchecked")
            Map<String, Object> source = (Map<String, Object>) hits.get(0).get("_source");
            assertEquals(11, source.get("id"));
            assertEquals(12, extractIntPath(body, "hits", "total", "value"));
        }
    }

    public void testSizeZeroKeepsTheCountPath() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUpNullable("hitszero")) {
            String indexName = fixture.indexName();
            String body = readAll(postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"range\":{\"id\":{\"gte\":6}}}}"));
            assertEquals(6, extractIntPath(body, "hits", "total", "value"));
            assertTrue("a size 0 request returns no hits: " + body, body.contains("\"hits\":[]"));
        }
    }

    public void testSearchAfterFoldsAndPagesLikeTheLuceneCollector() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUpNullable("hitsafter")) {
            String indexName = fixture.indexName();

            // The cursor page folds: the pushed page carries the strict
            // bound the scan ANDs into its filter.
            String explained = explainBody(indexName, "{\"size\":4,\"sort\":[{\"count64\":\"asc\"}],\"search_after\":[3000]}");
            String physical = stringPath(explained, "physical");
            assertTrue("the cursor page is pushed: " + physical, physical.contains("topk{"));
            assertTrue(
                "the pushed page carries the cursor bound: " + physical,
                physical.contains("cursor=(count64 > 3000 OR count64 IS NULL)")
            );

            // Page through the table with search_after and compare each
            // page with the Lucene collector, which a trivial
            // aggregation forces (the planner refuses aggregations on
            // the hits fold).
            String oracleAgg = ",\"aggs\":{\"n\":{\"value_count\":{\"field\":\"id\"}}}";
            List<String> pagedIds = new java.util.ArrayList<>();
            String cursor = null;
            for (int page = 0; page < 3; page++) {
                String shape = "{\"size\":4,\"sort\":[{\"count64\":\"asc\"}]"
                    + (cursor == null ? "" : ",\"search_after\":[" + cursor + "]");
                List<Map<String, Object>> hits = hitsOf(readAll(postJson("/" + indexName + "/_search", shape + "}")));
                List<Map<String, Object>> oracle = hitsOf(readAll(postJson("/" + indexName + "/_search", shape + oracleAgg + "}")));
                assertEquals("page " + page + " must match the Lucene collector", oracle, hits);
                for (Map<String, Object> hit : hits) {
                    pagedIds.add((String) hit.get("_id"));
                }
                if (hits.isEmpty()) {
                    break;
                }
                cursor = String.valueOf(sortValuesOf(hits.get(hits.size() - 1)).get(0));
            }
            // The union of the pages is the whole sorted table: the one
            // null row (id=5) sorts last through the missing sentinel,
            // whose cursor stays on the Lucene path and still answers.
            List<Map<String, Object>> whole = hitsOf(
                readAll(postJson("/" + indexName + "/_search", "{\"size\":12,\"sort\":[{\"count64\":\"asc\"}]}"))
            );
            assertEquals(idsOf(whole), pagedIds);
        }
    }
}
