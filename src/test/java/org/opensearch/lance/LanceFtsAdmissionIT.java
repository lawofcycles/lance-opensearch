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
 * Admission control for full-text shapes. The fixture's inverted
 * index is declared as not fitting the index cache shard through
 * {@code lance.test.index_cache_shard_share}, and the headroom is
 * raised to 1 PB so any host's free memory is below it: an unbounded
 * shape must answer 429 with the {@code lance_fts_admission} label
 * before its scan starts, a bounded top-k page must answer the same
 * 429 (it rebuilds the same document set) until
 * {@code lance.fts.admission.bounded_shapes_gated} opts it out, and
 * disabling the gate must let the unbounded shape through again. The
 * stats block must count the rejections, report the node's
 * available memory and the memory earlier admitted scans left behind,
 * and with the readings scripted through
 * {@code lance.test.fts_admission_available_memory} a repeat of an
 * admitted unbounded shape must be admitted on that credit alone.
 */
public class LanceFtsAdmissionIT extends LanceRestTestCase {

    private static final String UNBOUNDED = "{\"size\":10,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},"
        + "\"sort\":[{\"id\":\"desc\"}]}";
    private static final String BOUNDED = "{\"size\":5,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}";

    public void testUnboundedShapeIsRefusedWhenTheEstimateDoesNotFitFreeMemory() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "ftsadmission")) {
            String indexName = fixture.indexName();

            // With the defaults the fixture's estimate fits the shard
            // share, so estimate 0 admits both shapes, and the stats
            // block reports the node's available memory (MemAvailable
            // on Linux, which stays positive on a warm node whose page
            // cache has consumed its free pages) next to the 200.
            String before = readAll(postJson("/" + indexName + "/_search", UNBOUNDED));
            assertEquals(8, extractIntPath(before, "hits", "total", "value"));
            Map<String, Object> warm = admissionStats();
            assertTrue(warm.toString(), ((Number) warm.get("available_bytes")).longValue() > 0L);

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
                assertTrue("expected the retained figure in the message: " + body, body.contains("retained by earlier full text scans"));

                // The rejection is counted and the estimate recorded:
                // 16 rows at 52 bytes per row for the document set,
                // plus the scan buffer of the unbounded shape (one row
                // in ten of 16 rows assumed matched, 12 bytes per
                // returned row, doubled): 832 + 24.
                Map<String, Object> admission = admissionStats();
                assertEquals(admission.toString(), true, admission.get("enabled"));
                assertEquals(admission.toString(), 1L << 50, ((Number) admission.get("headroom_bytes")).longValue());
                assertTrue(admission.toString(), ((Number) admission.get("rejections")).longValue() >= 1L);
                assertEquals(admission.toString(), 16L * 52L + 24L, ((Number) admission.get("last_estimate_bytes")).longValue());

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

    public void testBoundedShapeIsGatedAndTheOptOutSettingRestoresThePassThrough() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "ftsadmissionb")) {
            String indexName = fixture.indexName();

            updateClusterSetting("lance.test.index_cache_shard_share", "\"1b\"");
            updateClusterSetting("lance.fts.admission.headroom", "\"1pb\"");
            try {
                // A bounded top-k page rebuilds the same document set,
                // so it is judged by the same comparison and refused.
                ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/" + indexName + "/_search", BOUNDED));
                int status = failure.getResponse().getStatusLine().getStatusCode();
                String body = readAll(failure.getResponse());
                assertEquals("expected 429, saw " + status + ": " + body, RestStatus.TOO_MANY_REQUESTS.getStatus(), status);
                assertTrue("expected the admission label: " + body, body.contains("lance_fts_admission"));
                assertTrue("expected the bounded wording: " + body, body.contains("bounded full text page"));
                assertTrue(
                    "expected the opt-out setting in the message: " + body,
                    body.contains("lance.fts.admission.bounded_shapes_gated")
                );

                // The estimate carries the page's own scan buffer: 16
                // rows at 52 bytes for the document set plus the top-k
                // page of 5 rows at 12 bytes, doubled: 832 + 120.
                Map<String, Object> admission = admissionStats();
                assertTrue(admission.toString(), ((Number) admission.get("rejections")).longValue() >= 1L);
                assertEquals(admission.toString(), 16L * 52L + 120L, ((Number) admission.get("last_estimate_bytes")).longValue());

                // Opting bounded shapes out restores the pass-through
                // for the page while the unbounded shape stays gated.
                updateClusterSetting("lance.fts.admission.bounded_shapes_gated", "false");
                Response bounded = postJson("/" + indexName + "/_search", BOUNDED);
                assertEquals(RestStatus.OK.getStatus(), bounded.getStatusLine().getStatusCode());
                assertEquals(8, extractIntPath(readAll(bounded), "hits", "total", "value"));
                ResponseException stillGated = expectThrows(
                    ResponseException.class,
                    () -> postJson("/" + indexName + "/_search", UNBOUNDED)
                );
                assertEquals(RestStatus.TOO_MANY_REQUESTS.getStatus(), stillGated.getResponse().getStatusLine().getStatusCode());
            } finally {
                updateClusterSetting("lance.fts.admission.bounded_shapes_gated", null);
                updateClusterSetting("lance.fts.admission.headroom", null);
                updateClusterSetting("lance.test.index_cache_shard_share", null);
            }

            // Back at the defaults the bounded page answers as before.
            String restored = readAll(postJson("/" + indexName + "/_search", BOUNDED));
            assertEquals(8, extractIntPath(restored, "hits", "total", "value"));
        }
    }

    public void testRepeatOfAnAdmittedUnboundedShapeIsAdmittedThroughTheRetainedCredit() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "ftsadmissionr")) {
            String indexName = fixture.indexName();
            // 16 rows at 52 bytes for the document set plus the 24 byte
            // scan buffer of the unbounded shape.
            long estimate = 16L * 52L + 24L;

            // The fixture's index is declared as not fitting, so every
            // decision carries the full estimate, and the available
            // memory readings are scripted so what a scan leaves behind
            // is known: without the script a 16 row scan leaves nothing
            // the kernel's kibibyte granularity could show.
            updateClusterSetting("lance.test.index_cache_shard_share", "\"1b\"");
            try {
                // A reading of 1000 bytes and a headroom of 500 leave 500
                // for an 856 byte estimate. Nothing has been retained
                // yet, so the shape is refused and the message says so.
                updateClusterSetting("lance.test.fts_admission_available_memory", "[\"1000b\"]");
                updateClusterSetting("lance.fts.admission.headroom", "\"500b\"");
                ResponseException refused = expectThrows(ResponseException.class, () -> postJson("/" + indexName + "/_search", UNBOUNDED));
                String body = readAll(refused.getResponse());
                assertEquals(body, RestStatus.TOO_MANY_REQUESTS.getStatus(), refused.getResponse().getStatusLine().getStatusCode());
                assertTrue(body, body.contains("[0b] retained by earlier full text scans"));
                Map<String, Object> admission = admissionStats();
                long rejected = ((Number) admission.get("rejections")).longValue();
                assertEquals(admission.toString(), 1000L, ((Number) admission.get("available_bytes")).longValue());
                assertEquals(admission.toString(), 0L, ((Number) admission.get("retained_bytes")).longValue());

                // Readings of 2000 then 1000: the next request is admitted
                // at 2000 (1500 left) and its scan completes at 1000, so
                // the pool records the 1000 byte drop clamped to the
                // estimate. The stats then read 1000 and report the
                // credit next to it.
                updateClusterSetting("lance.test.fts_admission_available_memory", "[\"2000b\",\"1000b\"]");
                String first = readAll(postJson("/" + indexName + "/_search", UNBOUNDED));
                assertEquals(8, extractIntPath(first, "hits", "total", "value"));
                admission = admissionStats();
                assertEquals(admission.toString(), rejected, ((Number) admission.get("rejections")).longValue());
                assertEquals(admission.toString(), estimate, ((Number) admission.get("last_estimate_bytes")).longValue());
                assertEquals(admission.toString(), 1000L, ((Number) admission.get("available_bytes")).longValue());
                assertEquals(admission.toString(), estimate, ((Number) admission.get("retained_bytes")).longValue());

                // The identical request now reads 1000 again: 500 left on
                // its own, the reading that was refused above, admitted
                // on the 856 byte credit. Twice, and the pool stands.
                for (int repeat = 1; repeat <= 2; repeat++) {
                    String response = readAll(postJson("/" + indexName + "/_search", UNBOUNDED));
                    assertEquals("repeat " + repeat, 8, extractIntPath(response, "hits", "total", "value"));
                    admission = admissionStats();
                    assertEquals(admission.toString(), rejected, ((Number) admission.get("rejections")).longValue());
                    assertEquals(admission.toString(), 1000L, ((Number) admission.get("available_bytes")).longValue());
                    assertEquals(admission.toString(), estimate, ((Number) admission.get("retained_bytes")).longValue());
                }

                // Recovery decays the credit: 300 of the 1000 bytes come
                // back and 556 remain credited; all of it back and nothing
                // is.
                updateClusterSetting("lance.test.fts_admission_available_memory", "[\"1300b\"]");
                admission = admissionStats();
                assertEquals(admission.toString(), 1300L, ((Number) admission.get("available_bytes")).longValue());
                assertEquals(admission.toString(), estimate - 300L, ((Number) admission.get("retained_bytes")).longValue());
                updateClusterSetting("lance.test.fts_admission_available_memory", "[\"2000b\"]");
                admission = admissionStats();
                assertEquals(admission.toString(), 0L, ((Number) admission.get("retained_bytes")).longValue());

                // With nothing retained the same 500 bytes left refuse
                // again: the admissions above were the credit's doing.
                updateClusterSetting("lance.fts.admission.headroom", "\"1500b\"");
                ResponseException refusedAgain = expectThrows(
                    ResponseException.class,
                    () -> postJson("/" + indexName + "/_search", UNBOUNDED)
                );
                String bodyAgain = readAll(refusedAgain.getResponse());
                assertEquals(
                    bodyAgain,
                    RestStatus.TOO_MANY_REQUESTS.getStatus(),
                    refusedAgain.getResponse().getStatusLine().getStatusCode()
                );
                assertTrue(bodyAgain, bodyAgain.contains("[0b] retained by earlier full text scans"));
                assertEquals(rejected + 1, ((Number) admissionStats().get("rejections")).longValue());
            } finally {
                updateClusterSetting("lance.fts.admission.headroom", null);
                updateClusterSetting("lance.test.fts_admission_available_memory", null);
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
