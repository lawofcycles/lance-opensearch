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
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Smoke checks that the plugin is installed and its node-level surfaces
 * (circuit breaker, REST routes, stats APIs) respond.
 */
public class LancePluginIT extends LanceRestTestCase {

    public void testPluginIsInstalled() throws IOException {
        Response response = client().performRequest(new Request("GET", "/_cat/plugins?format=json"));
        String body = readAll(response);
        assertTrue("expected opensearch-lance in _cat/plugins, saw: " + body, body.contains("opensearch-lance"));
    }

    public void testCircuitBreakerIsRegistered() throws IOException {
        // The lance_native breaker is registered through
        // CircuitBreakerPlugin so it shows up in the standard
        // _nodes/stats/breaker output.
        Response response = client().performRequest(new Request("GET", "/_nodes/stats/breaker"));
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
        String body = readAll(response);
        assertTrue("expected lance_native breaker in _nodes/stats/breaker, saw: " + body, body.contains("lance_native"));
    }

    public void testNamespaceEndpointIsRegistered() throws IOException {
        Response response = client().performRequest(new Request("GET", "/_lance/namespace"));
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
        String body = readAll(response);
        assertTrue("expected namespaces JSON, saw: " + body, body.contains("namespaces"));
    }

    public void testStatsAPIsSucceedForLanceIndex() throws Exception {
        // The default docStats / segmentsStats implementations walk
        // leaves through Lucene.segmentReader, which rejects Lance
        // leaves; the engine overrides must keep the stats APIs free of
        // shard failures.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "statsapi")) {
            String indexName = fixture.indexName();

            Response indexStats = client().performRequest(new Request("GET", "/" + indexName + "/_stats"));
            assertEquals(RestStatus.OK.getStatus(), indexStats.getStatusLine().getStatusCode());
            String body = readAll(indexStats);
            int failed = extractIntPath(body, "_shards", "failed");
            assertEquals("expected zero shard failures on _stats, saw: " + body, 0, failed);

            Response segments = client().performRequest(new Request("GET", "/" + indexName + "/_segments"));
            assertEquals(RestStatus.OK.getStatus(), segments.getStatusLine().getStatusCode());
            String segmentsBody = readAll(segments);
            assertEquals(
                "expected zero shard failures on _segments, saw: " + segmentsBody,
                0,
                extractIntPath(segmentsBody, "_shards", "failed")
            );

            Response nodesStats = client().performRequest(new Request("GET", "/_nodes/stats/indices/docs"));
            assertEquals(RestStatus.OK.getStatus(), nodesStats.getStatusLine().getStatusCode());
            String nodesBody = readAll(nodesStats);
            int nodesFailed = extractIntPath(nodesBody, "_nodes", "failed");
            assertEquals("expected zero node failures on _nodes/stats, saw: " + nodesBody, 0, nodesFailed);
        }
    }

    public void testCatIndicesReportsLanceRowCountAndDeletions() throws Exception {
        // docs.count / docs.deleted must follow the Lance manifest: after
        // two rows are deleted and the shard refreshes onto the new
        // version, every stats surface reports 14 live rows and 2
        // deletions, and _cat/shards agrees with _cat/indices.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "catdocs")) {
            String indexName = fixture.indexName();
            LanceTableFactory.deleteRows(fixture.tableUri(), "id IN (1, 4)");
            Response refresh = client().performRequest(new Request("POST", "/" + indexName + "/_refresh"));
            assertEquals(RestStatus.OK.getStatus(), refresh.getStatusLine().getStatusCode());

            Map<String, Object> indexRow = singleCatRow("/_cat/indices/" + indexName + "?format=json&bytes=b");
            assertEquals("docs.count in " + indexRow, "14", indexRow.get("docs.count"));
            assertEquals("docs.deleted in " + indexRow, "2", indexRow.get("docs.deleted"));
            assertEquals("health in " + indexRow, "green", indexRow.get("health"));
            assertEquals("pri in " + indexRow, "1", indexRow.get("pri"));
            assertEquals("rep in " + indexRow, "0", indexRow.get("rep"));
            assertEquals("pri.store.size in " + indexRow, indexRow.get("store.size"), indexRow.get("pri.store.size"));

            Map<String, Object> shardRow = singleCatRow("/_cat/shards/" + indexName + "?format=json&bytes=b");
            assertEquals("docs in " + shardRow, "14", shardRow.get("docs"));
            assertEquals("store in " + shardRow + " vs " + indexRow, indexRow.get("store.size"), shardRow.get("store"));
            assertEquals("prirep in " + shardRow, "p", shardRow.get("prirep"));
            assertEquals("state in " + shardRow, "STARTED", shardRow.get("state"));

            String stats = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_stats/docs,store")));
            assertEquals("docs.count in " + stats, 14, extractIntPath(stats, "_all", "primaries", "docs", "count"));
            assertEquals("docs.deleted in " + stats, 2, extractIntPath(stats, "_all", "primaries", "docs", "deleted"));
            assertEquals(
                "store.size_in_bytes in " + stats + " vs " + indexRow,
                Integer.parseInt((String) indexRow.get("store.size")),
                extractIntPath(stats, "_all", "primaries", "store", "size_in_bytes")
            );

            String health = readAll(client().performRequest(new Request("GET", "/_cluster/health/" + indexName + "?level=indices")));
            assertEquals("cluster status in " + health, "green", extractStringPath(health, "indices", indexName, "status"));
            assertEquals("active_primary_shards in " + health, 1, extractIntPath(health, "indices", indexName, "active_primary_shards"));
            assertEquals("active_shards in " + health, 1, extractIntPath(health, "indices", indexName, "active_shards"));
            assertEquals("unassigned_shards in " + health, 0, extractIntPath(health, "indices", indexName, "unassigned_shards"));
            assertEquals("initializing_shards in " + health, 0, extractIntPath(health, "indices", indexName, "initializing_shards"));

            String clusterStats = readAll(client().performRequest(new Request("GET", "/_cluster/stats")));
            assertEquals("node failures in " + clusterStats, 0, extractIntPath(clusterStats, "_nodes", "failed"));
            assertTrue("indices.docs.count in " + clusterStats, extractIntPath(clusterStats, "indices", "docs", "count") >= 14);
        }
    }

    private static Map<String, Object> singleCatRow(String path) throws IOException {
        String body = readAll(client().performRequest(new Request("GET", path)));
        try (XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, body)) {
            List<Object> rows = parser.list();
            assertEquals("expected exactly one row from " + path + ", saw: " + body, 1, rows.size());
            @SuppressWarnings("unchecked")
            Map<String, Object> row = (Map<String, Object>) rows.get(0);
            return row;
        }
    }

    private static String extractStringPath(String json, String... path) throws IOException {
        try (XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Object value = parser.map();
            for (String step : path) {
                assertTrue("cannot descend into " + value + " with step " + step, value instanceof Map);
                value = ((Map<?, ?>) value).get(step);
                assertNotNull("missing key " + step + " in path " + String.join(".", path) + ", json=" + json, value);
            }
            return String.valueOf(value);
        }
    }
}
