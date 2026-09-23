/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;

/**
 * Admission control end to end. The fixture's indexes and scans are
 * declared as not fitting the index cache shard through
 * {@code lance.test.index_cache_shard_share}, and the headroom is
 * raised to 1 PB so any host's available memory is below it: an
 * unbounded full text shape must answer 429 with the
 * {@code lance_admission} label before its scan starts, a bounded top-k
 * page must answer the same 429 (it rebuilds the same document set)
 * until {@code lance.admission.bounded_shapes_gated} opts it out, and
 * disabling the gate must let the unbounded shape through again. The
 * stats block must count the rejections per kind, report the node's
 * available memory and the memory earlier admitted scans left behind,
 * and with the readings scripted through
 * {@code lance.test.admission_available_memory} a repeat of an admitted
 * unbounded shape must be admitted on that credit alone. The other
 * kinds (a scalar index load, a filter scan, a nearest scan, a pushed
 * aggregate, a sorted page) are each refused under the same overrides
 * with their kind in the message and their counter incremented, and
 * admitted without them.
 */
public class LanceAdmissionIT extends LanceRestTestCase {

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
            updateClusterSetting("lance.admission.headroom", "\"1pb\"");
            try {
                ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/" + indexName + "/_search", UNBOUNDED));
                int status = failure.getResponse().getStatusLine().getStatusCode();
                String body = readAll(failure.getResponse());
                assertEquals("expected 429, saw " + status + ": " + body, RestStatus.TOO_MANY_REQUESTS.getStatus(), status);
                assertTrue("expected circuit_breaking_exception: " + body, body.contains("circuit_breaking_exception"));
                assertTrue("expected the admission label: " + body, body.contains("lance_admission"));
                assertTrue("expected the index name in the message: " + body, body.contains(indexName));
                assertTrue("expected the retained figure in the message: " + body, body.contains("retained by earlier admitted scans"));

                // The rejection is counted and the estimate recorded:
                // 16 rows at 52 bytes per row for the document set,
                // plus the scan buffer of the unbounded shape (one row
                // in ten of 16 rows assumed matched, 12 bytes per
                // returned row, doubled): 832 + 24.
                Map<String, Object> admission = admissionStats();
                assertEquals(admission.toString(), true, admission.get("enabled"));
                assertEquals(admission.toString(), 1L << 50, ((Number) admission.get("headroom_bytes")).longValue());
                assertTrue(admission.toString(), rejections(admission, "fts") >= 1L);
                assertEquals(admission.toString(), 16L * 52L + 24L, ((Number) admission.get("last_estimate_bytes")).longValue());
                assertEquals(admission.toString(), "fts", admission.get("last_kind"));

                // Disabling the gate lets the same unbounded shape
                // through; the rejection count stays where it was.
                long rejected = rejections(admission, "fts");
                updateClusterSetting("lance.admission.enabled", "false");
                String disabled = readAll(postJson("/" + indexName + "/_search", UNBOUNDED));
                assertEquals(8, extractIntPath(disabled, "hits", "total", "value"));
                Map<String, Object> after = admissionStats();
                assertEquals(after.toString(), false, after.get("enabled"));
                assertEquals(after.toString(), rejected, rejections(after, "fts"));
            } finally {
                updateClusterSetting("lance.admission.enabled", null);
                updateClusterSetting("lance.admission.headroom", null);
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
            updateClusterSetting("lance.admission.headroom", "\"1pb\"");
            try {
                // A bounded top-k page rebuilds the same document set,
                // so it is judged by the same comparison and refused.
                ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/" + indexName + "/_search", BOUNDED));
                int status = failure.getResponse().getStatusLine().getStatusCode();
                String body = readAll(failure.getResponse());
                assertEquals("expected 429, saw " + status + ": " + body, RestStatus.TOO_MANY_REQUESTS.getStatus(), status);
                assertTrue("expected the admission label: " + body, body.contains("lance_admission"));
                assertTrue("expected the bounded wording: " + body, body.contains("bounded full text page"));
                assertTrue("expected the opt-out setting in the message: " + body, body.contains("lance.admission.bounded_shapes_gated"));

                // The estimate carries the page's own scan buffer: 16
                // rows at 52 bytes for the document set plus the top-k
                // page of 5 rows at 12 bytes, doubled: 832 + 120.
                Map<String, Object> admission = admissionStats();
                assertTrue(admission.toString(), rejections(admission, "fts") >= 1L);
                assertEquals(admission.toString(), 16L * 52L + 120L, ((Number) admission.get("last_estimate_bytes")).longValue());

                // Opting bounded shapes out restores the pass-through
                // for the page while the unbounded shape stays gated.
                updateClusterSetting("lance.admission.bounded_shapes_gated", "false");
                Response bounded = postJson("/" + indexName + "/_search", BOUNDED);
                assertEquals(RestStatus.OK.getStatus(), bounded.getStatusLine().getStatusCode());
                assertEquals(8, extractIntPath(readAll(bounded), "hits", "total", "value"));
                ResponseException stillGated = expectThrows(
                    ResponseException.class,
                    () -> postJson("/" + indexName + "/_search", UNBOUNDED)
                );
                assertEquals(RestStatus.TOO_MANY_REQUESTS.getStatus(), stillGated.getResponse().getStatusLine().getStatusCode());
            } finally {
                updateClusterSetting("lance.admission.bounded_shapes_gated", null);
                updateClusterSetting("lance.admission.headroom", null);
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
                updateClusterSetting("lance.test.admission_available_memory", "[\"1000b\"]");
                updateClusterSetting("lance.admission.headroom", "\"500b\"");
                ResponseException refused = expectThrows(ResponseException.class, () -> postJson("/" + indexName + "/_search", UNBOUNDED));
                String body = readAll(refused.getResponse());
                assertEquals(body, RestStatus.TOO_MANY_REQUESTS.getStatus(), refused.getResponse().getStatusLine().getStatusCode());
                assertTrue(body, body.contains("[0b] retained by earlier admitted scans"));
                Map<String, Object> admission = admissionStats();
                long rejected = rejections(admission, "fts");
                assertEquals(admission.toString(), 1000L, ((Number) admission.get("available_bytes")).longValue());
                assertEquals(admission.toString(), 0L, ((Number) admission.get("retained_bytes")).longValue());

                // Readings of 2000 then 1000: the next request is admitted
                // at 2000 (1500 left) and its scan completes at 1000, so
                // the pool records the 1000 byte drop clamped to the
                // estimate. The stats then read 1000 and report the
                // credit next to it.
                updateClusterSetting("lance.test.admission_available_memory", "[\"2000b\",\"1000b\"]");
                String first = readAll(postJson("/" + indexName + "/_search", UNBOUNDED));
                assertEquals(8, extractIntPath(first, "hits", "total", "value"));
                admission = admissionStats();
                assertEquals(admission.toString(), rejected, rejections(admission, "fts"));
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
                    assertEquals(admission.toString(), rejected, rejections(admission, "fts"));
                    assertEquals(admission.toString(), 1000L, ((Number) admission.get("available_bytes")).longValue());
                    assertEquals(admission.toString(), estimate, ((Number) admission.get("retained_bytes")).longValue());
                }

                // Recovery decays the credit: 300 of the 1000 bytes come
                // back and 556 remain credited; all of it back and nothing
                // is.
                updateClusterSetting("lance.test.admission_available_memory", "[\"1300b\"]");
                admission = admissionStats();
                assertEquals(admission.toString(), 1300L, ((Number) admission.get("available_bytes")).longValue());
                assertEquals(admission.toString(), estimate - 300L, ((Number) admission.get("retained_bytes")).longValue());
                updateClusterSetting("lance.test.admission_available_memory", "[\"2000b\"]");
                admission = admissionStats();
                assertEquals(admission.toString(), 0L, ((Number) admission.get("retained_bytes")).longValue());

                // With nothing retained the same 500 bytes left refuse
                // again: the admissions above were the credit's doing.
                updateClusterSetting("lance.admission.headroom", "\"1500b\"");
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
                assertTrue(bodyAgain, bodyAgain.contains("[0b] retained by earlier admitted scans"));
                assertEquals(rejected + 1, rejections(admissionStats(), "fts"));
            } finally {
                updateClusterSetting("lance.admission.headroom", null);
                updateClusterSetting("lance.test.admission_available_memory", null);
                updateClusterSetting("lance.test.index_cache_shard_share", null);
            }

            // Back at the defaults the unbounded shape answers as before.
            String restored = readAll(postJson("/" + indexName + "/_search", UNBOUNDED));
            assertEquals(8, extractIntPath(restored, "hits", "total", "value"));
        }
    }

    /** The single node's {@code admission} stats block. */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> admissionStats() throws IOException {
        String stats = readAll(client().performRequest(new Request("GET", "/_lance/stats")));
        Map<String, Object> nodes = (Map<String, Object>) parseJson(stats).get("nodes");
        assertEquals("single node cluster: " + stats, 1, nodes.size());
        Map<String, Object> node = (Map<String, Object>) nodes.values().iterator().next();
        return (Map<String, Object>) node.get("admission");
    }

    /** The rejection counter of {@code kind} in an {@code admission} block; every kind is present. */
    @SuppressWarnings("unchecked")
    private static long rejections(Map<String, Object> admission, String kind) {
        Map<String, Object> rejections = (Map<String, Object>) admission.get("rejections");
        assertEquals(
            admission.toString(),
            List.of("fts", "scalar_index", "vector_index", "filter_scan", "aggregate_scan", "column_load"),
            List.copyOf(rejections.keySet())
        );
        return ((Number) rejections.get(kind)).longValue();
    }

    private static void updateClusterSetting(String key, String jsonValue) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"transient\":{\"" + key + "\":" + (jsonValue == null ? "null" : jsonValue) + "}}");
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    // ---- the other kinds, one shape each on the indexed fixture ----

    private static final String TERM_BITMAP = "{\"size\":0,\"query\":{\"term\":{\"category\":\"c1\"}}}";
    private static final String TERM_BTREE = "{\"size\":0,\"query\":{\"term\":{\"rating\":37}}}";
    private static final String KNN = "{\"size\":3,\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":[5,0,0,0,0,0,0,0],\"k\":3,"
        + "\"nprobes\":1}}}";
    private static final String TERMS_AGG = "{\"size\":0,\"aggs\":{\"c\":{\"terms\":{\"field\":\"category\",\"size\":10}}}}";
    private static final String SORTED_PAGE = "{\"size\":5,\"sort\":[{\"rating\":\"desc\"}]}";

    public void testEveryKindIsRefusedUnderTheOverridesAndAdmittedWithoutThem() throws Exception {
        // A table with a BTree, a bitmap, an inverted and an IVF_PQ
        // index; 300 rows so the IVF_PQ trains. Under a one byte shard
        // share every non zero estimate counts in full, and a headroom
        // of 1 PB puts any host's available memory below zero, so each
        // gated path answers 429 naming its kind; with the overrides
        // gone the same requests answer 200.
        String suffix = "admissionkinds-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeIndexedFixtureTable(scratchDir, tableName, 2, 150);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(readAll(attach), 200, attach.getStatusLine().getStatusCode());
            ensureGreen(tableName);

            // The oracle answers, so the shapes below are served by the
            // paths the gate covers.
            assertEquals(100, extractIntPath(readAll(postJson("/" + tableName + "/_search", TERM_BITMAP)), "hits", "total", "value"));
            assertEquals(1, extractIntPath(readAll(postJson("/" + tableName + "/_search", TERM_BTREE)), "hits", "total", "value"));
            assertEquals(3, extractIntPath(readAll(postJson("/" + tableName + "/_search", KNN)), "hits", "total", "value"));
            assertEquals(300, extractIntPath(readAll(postJson("/" + tableName + "/_search", TERMS_AGG)), "hits", "total", "value"));
            assertEquals(300, extractIntPath(readAll(postJson("/" + tableName + "/_search", SORTED_PAGE)), "hits", "total", "value"));

            updateClusterSetting("lance.test.index_cache_shard_share", "\"1b\"");
            updateClusterSetting("lance.admission.headroom", "\"1pb\"");
            try {
                // A term on the bitmap column: the bitmap load is the
                // first path judged and refused. The estimate is the
                // matching bitmaps held twice: the index's manifest size
                // times one third (three categories) times two.
                Map<String, Object> before = admissionStats();
                String bitmap = expectAdmissionRefusal(tableName, TERM_BITMAP, "scalar_index");
                assertTrue(bitmap, bitmap.contains("bitmap index [category_bitmap]"));
                assertTrue(bitmap, bitmap.contains("selectivity 0."));
                Map<String, Object> afterBitmap = admissionStats();
                assertEquals(rejections(before, "scalar_index") + 1, rejections(afterBitmap, "scalar_index"));
                assertEquals("scalar_index", afterBitmap.get("last_kind"));
                assertTrue(afterBitmap.toString(), ((Number) afterBitmap.get("last_estimate_bytes")).longValue() > 0L);

                // A term on the BTree column: the pages read are one
                // fifth of the index (no cardinality), refused the same.
                String btree = expectAdmissionRefusal(tableName, TERM_BTREE, "scalar_index");
                assertTrue(btree, btree.contains("btree index [rating_btree]"));
                assertTrue(btree, btree.contains("selectivity 0.2000"));

                // The nearest scan: the IVF_PQ partitions, the whole
                // index twice since the partition count is not known.
                String knn = expectAdmissionRefusal(tableName, KNN, "vector_index");
                assertTrue(knn, knn.contains("nearest scan on [embedding]"));
                assertTrue(knn, knn.contains("index [embedding_ivf]"));
                assertEquals(rejections(before, "vector_index") + 1, rejections(admissionStats(), "vector_index"));

                // The pushed aggregate: its parallel scans.
                String aggregate = expectAdmissionRefusal(tableName, TERMS_AGG, "aggregate_scan");
                assertTrue(aggregate, aggregate.contains("pushed aggregate over"));
                assertTrue(aggregate, aggregate.contains("parallel scans over 300 rows"));
                assertEquals(rejections(before, "aggregate_scan") + 1, rejections(admissionStats(), "aggregate_scan"));

                // The sorted page: a filter scan of every row for the
                // sort column.
                String sorted = expectAdmissionRefusal(tableName, SORTED_PAGE, "filter_scan");
                assertTrue(sorted, sorted.contains("sorted page scan over"));
                assertTrue(sorted, sorted.contains("300 of 300 rows read on this node"));
                assertEquals(rejections(before, "filter_scan") + 1, rejections(admissionStats(), "filter_scan"));

                // Disabling the gate lets every shape through again and
                // the counters stand.
                Map<String, Object> refused = admissionStats();
                updateClusterSetting("lance.admission.enabled", "false");
                assertEquals(100, extractIntPath(readAll(postJson("/" + tableName + "/_search", TERM_BITMAP)), "hits", "total", "value"));
                assertEquals(3, extractIntPath(readAll(postJson("/" + tableName + "/_search", KNN)), "hits", "total", "value"));
                assertEquals(300, extractIntPath(readAll(postJson("/" + tableName + "/_search", TERMS_AGG)), "hits", "total", "value"));
                assertEquals(300, extractIntPath(readAll(postJson("/" + tableName + "/_search", SORTED_PAGE)), "hits", "total", "value"));
                Map<String, Object> disabled = admissionStats();
                assertEquals(disabled.toString(), false, disabled.get("enabled"));
                assertEquals(disabled.get("rejections").toString(), refused.get("rejections"), disabled.get("rejections"));
            } finally {
                updateClusterSetting("lance.admission.enabled", null);
                updateClusterSetting("lance.admission.headroom", null);
                updateClusterSetting("lance.test.index_cache_shard_share", null);
            }

            // Back at the defaults every shape answers as before.
            assertEquals(100, extractIntPath(readAll(postJson("/" + tableName + "/_search", TERM_BITMAP)), "hits", "total", "value"));
            assertEquals(1, extractIntPath(readAll(postJson("/" + tableName + "/_search", TERM_BTREE)), "hits", "total", "value"));
            assertEquals(3, extractIntPath(readAll(postJson("/" + tableName + "/_search", KNN)), "hits", "total", "value"));
            assertEquals(300, extractIntPath(readAll(postJson("/" + tableName + "/_search", TERMS_AGG)), "hits", "total", "value"));
            assertEquals(300, extractIntPath(readAll(postJson("/" + tableName + "/_search", SORTED_PAGE)), "hits", "total", "value"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + tableName));
            } catch (Exception ignored) {
                // best-effort cleanup; the base class wipes indices too
            }
            deleteRecursively(scratchDir);
        }
    }

    public void testFilterWithoutAnIndexIsJudgedAsAFilterScanOnly() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "admissionfilter")) {
            String indexName = fixture.indexName();
            String term = "{\"size\":0,\"query\":{\"term\":{\"id\":3}}}";
            assertEquals(1, extractIntPath(readAll(postJson("/" + indexName + "/_search", term)), "hits", "total", "value"));
            updateClusterSetting("lance.test.index_cache_shard_share", "\"1b\"");
            updateClusterSetting("lance.admission.headroom", "\"1pb\"");
            try {
                // id carries no scalar index: no scalar_index decision,
                // the filter scan is refused on its row addresses (16
                // rows, one in five expected to match at 256 bytes) and
                // the batches in flight.
                Map<String, Object> before = admissionStats();
                String body = expectAdmissionRefusal(indexName, term, "filter_scan");
                assertTrue(body, body.contains("unbounded filter scan over"));
                assertTrue(body, body.contains("3 of 16 rows expected to match on this node"));
                Map<String, Object> after = admissionStats();
                assertEquals(rejections(before, "scalar_index"), rejections(after, "scalar_index"));
                assertEquals(rejections(before, "filter_scan") + 1, rejections(after, "filter_scan"));
                assertEquals("filter_scan", after.get("last_kind"));
                // A bounded page carries the same cost until the opt out
                // caps it at its limit; the limit still counts in full
                // under a one byte share.
                String page = "{\"size\":2,\"query\":{\"term\":{\"id\":3}}}";
                String bounded = expectAdmissionRefusal(indexName, page, "filter_scan");
                assertTrue(bounded, bounded.contains("bounded filter scan over"));
                assertTrue(bounded, bounded.contains("3 of 16 rows expected to match"));
                updateClusterSetting("lance.admission.bounded_shapes_gated", "false");
                String capped = expectAdmissionRefusal(indexName, page, "filter_scan");
                assertTrue(capped, capped.contains("2 of 16 rows expected to match"));
            } finally {
                updateClusterSetting("lance.admission.bounded_shapes_gated", null);
                updateClusterSetting("lance.admission.headroom", null);
                updateClusterSetting("lance.test.index_cache_shard_share", null);
            }
            assertEquals(1, extractIntPath(readAll(postJson("/" + indexName + "/_search", term)), "hits", "total", "value"));
        }
    }

    /** POST {@code body} to the index and assert the 429 of the gate names {@code kind}; the response body. */
    private static String expectAdmissionRefusal(String indexName, String body, String kind) {
        ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/" + indexName + "/_search", body));
        int status = failure.getResponse().getStatusLine().getStatusCode();
        String response;
        try {
            response = readAll(failure.getResponse());
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        assertEquals("expected 429, saw " + status + ": " + response, RestStatus.TOO_MANY_REQUESTS.getStatus(), status);
        assertTrue("expected circuit_breaking_exception: " + response, response.contains("circuit_breaking_exception"));
        assertTrue("expected the admission label and kind: " + response, response.contains("[lance_admission] " + kind + " estimate"));
        return response;
    }
}
