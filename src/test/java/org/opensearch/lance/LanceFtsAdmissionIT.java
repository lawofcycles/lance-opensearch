/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.util.Map;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;

/**
 * Admission control for unbounded full-text shapes. The fixture's
 * inverted index is declared as not fitting the index cache shard
 * through {@code lance.test.index_cache_shard_share}, and the
 * headroom is raised to 1 PB so any host's free memory is below it:
 * an unbounded shape must answer 429 with the {@code lance_fts_admission}
 * label before its scan starts, a bounded top-k page must keep
 * answering 200, and disabling the gate must let the unbounded shape
 * through again. The stats block must count the rejection.
 */
public class LanceFtsAdmissionIT extends LanceRestTestCase {

    private static final String UNBOUNDED = "{\"size\":10,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},"
        + "\"sort\":[{\"id\":\"desc\"}]}";
    private static final String BOUNDED = "{\"size\":5,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}";

    public void testUnboundedShapeIsRefusedWhenTheEstimateDoesNotFitFreeMemory() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "ftsadmission")) {
            String indexName = fixture.indexName();

            // With the defaults the fixture's estimate fits the shard
            // share, so estimate 0 admits both shapes.
            String before = readAll(postJson("/" + indexName + "/_search", UNBOUNDED));
            assertEquals(8, extractIntPath(before, "hits", "total", "value"));

            updateClusterSetting("lance.test.index_cache_shard_share", "\"1b\"");
            updateClusterSetting("lance.fts.admission.headroom", "\"1pb\"");
            try {
                ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/" + indexName + "/_search", UNBOUNDED));
                int status = failure.getResponse().getStatusLine().getStatusCode();
                String body = readAll(failure.getResponse());
                assertEquals("expected 429, saw " + status + ": " + body, RestStatus.TOO_MANY_REQUESTS.getStatus(), status);
                assertTrue("expected circuit_breaking_exception: " + body, body.contains("circuit_breaking_exception"));
                assertTrue("expected the admission label: " + body, body.contains("lance_fts_admission"));
                assertTrue("expected the index name in the message: " + body, body.contains(indexName));

                // A bounded top-k page is not gated: same table, same
                // settings, 200 with the top hits.
                Response bounded = postJson("/" + indexName + "/_search", BOUNDED);
                assertEquals(RestStatus.OK.getStatus(), bounded.getStatusLine().getStatusCode());
                assertEquals(8, extractIntPath(readAll(bounded), "hits", "total", "value"));

                // The rejection is counted and the estimate recorded:
                // 16 rows at 52 bytes per row.
                Map<String, Object> admission = admissionStats();
                assertEquals(admission.toString(), true, admission.get("enabled"));
                assertEquals(admission.toString(), 1L << 50, ((Number) admission.get("headroom_bytes")).longValue());
                assertTrue(admission.toString(), ((Number) admission.get("rejections")).longValue() >= 1L);
                assertEquals(admission.toString(), 16L * 52L, ((Number) admission.get("last_estimate_bytes")).longValue());

                // Disabling the gate lets the same unbounded shape
                // through; the rejection count stays where it was.
                long rejected = ((Number) admission.get("rejections")).longValue();
                updateClusterSetting("lance.fts.admission.enabled", "false");
                String disabled = readAll(postJson("/" + indexName + "/_search", UNBOUNDED));
                assertEquals(8, extractIntPath(disabled, "hits", "total", "value"));
                Map<String, Object> after = admissionStats();
                assertEquals(after.toString(), false, after.get("enabled"));
                assertEquals(after.toString(), rejected, ((Number) after.get("rejections")).longValue());
            } finally {
                updateClusterSetting("lance.fts.admission.enabled", null);
                updateClusterSetting("lance.fts.admission.headroom", null);
                updateClusterSetting("lance.test.index_cache_shard_share", null);
            }

            // Back at the defaults the unbounded shape answers as before.
            String restored = readAll(postJson("/" + indexName + "/_search", UNBOUNDED));
            assertEquals(8, extractIntPath(restored, "hits", "total", "value"));
        }
    }

    /** The single node's {@code fts.admission} stats block. */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> admissionStats() throws IOException {
        String stats = readAll(client().performRequest(new Request("GET", "/_lance/stats")));
        Map<String, Object> nodes = (Map<String, Object>) parseJson(stats).get("nodes");
        assertEquals("single node cluster: " + stats, 1, nodes.size());
        Map<String, Object> node = (Map<String, Object>) nodes.values().iterator().next();
        Map<String, Object> fts = (Map<String, Object>) node.get("fts");
        return (Map<String, Object>) fts.get("admission");
    }

    private static void updateClusterSetting(String key, String jsonValue) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"transient\":{\"" + key + "\":" + (jsonValue == null ? "null" : jsonValue) + "}}");
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }
}
