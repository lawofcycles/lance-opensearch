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
        Response response = client().performRequest(new Request("GET", "/_plugins/_lance/namespace"));
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
        String body = readAll(response);
        assertTrue("expected namespaces JSON, saw: " + body, body.contains("namespaces"));
    }

    public void testOldPathsAnswerWithDeprecationWarning() throws Exception {
        // The test client runs in strict deprecation mode, so a Warning
        // header fails the request unless the options expect it. The
        // node logs one "deprecated_route" message per X-Opaque-Id and
        // only a logged message becomes a Warning header, so every old
        // path request carries its own opaque id to be sure the header
        // is there even after another request already tripped the
        // deprecation on the same node.
        assertDeprecatedPath("GET", "/_lance/stats", "/_lance/stats", "/_plugins/_lance/stats");
        assertDeprecatedPath("GET", "/_lance/namespace", "/_lance/namespace", "/_plugins/_lance/namespace");

        // Attach answers from the callback of a create it issued under a
        // stashed thread context, so its 200 covers the response header
        // surviving that stash, both on the create and on the
        // already_attached answer of a second attach.
        String suffix = "oldattach-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 2);
        String attachBody = "{\"table\":\"" + scratchDir.resolve(tableName + ".lance") + "\"}";
        try {
            Response attached = assertDeprecatedPath("POST", "/_lance/attach", attachBody, "/_lance/attach", "/_plugins/_lance/attach");
            String body = readAll(attached);
            assertEquals("expected the index name in " + body, tableName, stringPath(body, "index"));
            assertFalse("expected a fresh attach in " + body, body.contains("\"already_attached\":true"));

            Response again = assertDeprecatedPath("POST", "/_lance/attach", attachBody, "/_lance/attach", "/_plugins/_lance/attach");
            String againBody = readAll(again);
            assertTrue("expected already_attached in " + againBody, againBody.contains("\"already_attached\":true"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + tableName));
            } catch (Exception ignored) {}
            deleteRecursively(scratchDir);
        }

        Response stats = client().performRequest(new Request("GET", "/_plugins/_lance/stats"));
        assertEquals(RestStatus.OK.getStatus(), stats.getStatusLine().getStatusCode());
        assertEquals("unexpected warnings on the new path: " + stats.getWarnings(), List.of(), stats.getWarnings());

        Response namespaces = client().performRequest(new Request("GET", "/_plugins/_lance/namespace"));
        assertEquals(RestStatus.OK.getStatus(), namespaces.getStatusLine().getStatusCode());
        assertEquals("unexpected warnings on the new path: " + namespaces.getWarnings(), List.of(), namespaces.getWarnings());
    }

    public void testOldStatsPathReadsNodeIdOnBothTemplates() throws IOException {
        // The old template /_lance/stats/{node_id} shares its parameter
        // name with the new /_plugins/_lance/{node_id}/stats, so a node id
        // on the old path still selects that node.
        String nodesBody = readAll(client().performRequest(new Request("GET", "/_nodes/_local")));
        Map<String, Object> nodes = parseJson(nodesBody);
        @SuppressWarnings("unchecked")
        Map<String, Object> nodeMap = (Map<String, Object>) nodes.get("nodes");
        String nodeId = nodeMap.keySet().iterator().next();

        Response oldPath = assertDeprecatedPath(
            "GET",
            "/_lance/stats/" + nodeId,
            "/_lance/stats/{node_id}",
            "/_plugins/_lance/{node_id}/stats"
        );
        String oldBody = readAll(oldPath);
        assertEquals("expected exactly one node in " + oldBody, 1, extractIntPath(oldBody, "_nodes", "total"));
        assertTrue("expected node " + nodeId + " in " + oldBody, oldBody.contains("\"" + nodeId + "\""));

        Response newPath = client().performRequest(new Request("GET", "/_plugins/_lance/" + nodeId + "/stats"));
        String newBody = readAll(newPath);
        assertEquals("expected exactly one node in " + newBody, 1, extractIntPath(newBody, "_nodes", "total"));
        assertTrue("expected node " + nodeId + " in " + newBody, newBody.contains("\"" + nodeId + "\""));
    }

    /**
     * Sends {@code method oldPath} and asserts it answers 200 with exactly
     * the deprecation warning OpenSearch's {@code RestController} builds for
     * a replaced route. The templates are the paths as registered, with
     * their {@code {param}} placeholders, because the warning quotes the
     * templates, not the resolved request path.
     */
    private static Response assertDeprecatedPath(String method, String oldPath, String oldTemplate, String newTemplate) throws IOException {
        return assertDeprecatedPath(method, oldPath, null, oldTemplate, newTemplate);
    }

    /** {@link #assertDeprecatedPath(String, String, String, String)} with a JSON body. */
    private static Response assertDeprecatedPath(String method, String oldPath, String jsonBody, String oldTemplate, String newTemplate)
        throws IOException {
        String warning = "[" + method + " " + oldTemplate + "] is deprecated! Use [" + method + " " + newTemplate + "] instead.";
        Request request = new Request(method, oldPath);
        if (jsonBody != null) {
            request.setJsonEntity(jsonBody);
        }
        request.setOptions(expectWarnings(warning).toBuilder().addHeader("X-Opaque-Id", "lance-deprecated-" + randomAlphaOfLength(12)));
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
        assertEquals("expected the deprecation warning on " + oldPath, List.of(warning), response.getWarnings());
        return response;
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
            // _cluster/stats aggregates every index on the shared cluster, so
            // compare against the value observed before the delete instead of
            // a fixed number.
            int clusterDocsBefore = extractIntPath(
                readAll(client().performRequest(new Request("GET", "/_cluster/stats"))),
                "indices",
                "docs",
                "count"
            );
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
            assertEquals(
                "indices.docs.count in " + clusterStats + " (before delete: " + clusterDocsBefore + ")",
                clusterDocsBefore - 2,
                extractIntPath(clusterStats, "indices", "docs", "count")
            );
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
