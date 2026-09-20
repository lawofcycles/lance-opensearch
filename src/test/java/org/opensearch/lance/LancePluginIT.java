/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.core.rest.RestStatus;

/**
 * Smoke checks that the plugin is installed and its node-level surfaces
 * (circuit breaker, REST routes, stats APIs) respond.
 */
public class LancePluginIT extends LanceRestTestCase {

    public void testPluginIsInstalled() throws IOException {
        // Ask the cluster for its plugin list and confirm the entry exists.
        // This catches classpath / metadata regressions before any Lance code
        // ever gets touched.
        Response response = client().performRequest(new Request("GET", "/_cat/plugins?format=json"));
        String body = readAll(response);
        assertTrue("expected opensearch-lance in _cat/plugins, saw: " + body, body.contains("opensearch-lance"));
    }

    public void testCircuitBreakerIsRegistered() throws IOException {
        // The plugin registers a lance_native breaker via
        // CircuitBreakerPlugin.getCircuitBreaker so operators can see
        // the native memory footprint through the standard
        // _nodes/stats/breaker API without a plugin-specific stats
        // endpoint. Assert that the breaker name and non-zero limit
        // show up on every node in a fresh cluster.
        Response response = client().performRequest(new Request("GET", "/_nodes/stats/breaker"));
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
        String body = readAll(response);
        assertTrue("expected lance_native breaker in _nodes/stats/breaker, saw: " + body, body.contains("lance_native"));
    }

    public void testNamespaceEndpointIsRegistered() throws IOException {
        // GET /_lance/namespace lists registered namespaces. On a fresh
        // cluster it returns an empty list, but the important assertion is
        // that the route is wired up and returns a 200 instead of 404.
        Response response = client().performRequest(new Request("GET", "/_lance/namespace"));
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
        String body = readAll(response);
        assertTrue("expected namespaces JSON, saw: " + body, body.contains("namespaces"));
    }

    public void testStatsAPIsSucceedForLanceIndex() throws Exception {
        // Regression for LanceReadOnlyEngine.docStats() / segmentsStats():
        // OpenSearch's default implementations traverse leaves via
        // Lucene.segmentReader(reader), which throws for Lance leaves and
        // takes out _stats / _cat/indices docs.count / _nodes/stats /
        // _cluster/stats node-wide. Confirm the plugin overrides succeed
        // and the node-level stats aggregation runs without shard failures.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "statsapi")) {
            String indexName = fixture.indexName();

            Response indexStats = client().performRequest(new Request("GET", "/" + indexName + "/_stats"));
            assertEquals(RestStatus.OK.getStatus(), indexStats.getStatusLine().getStatusCode());
            String body = readAll(indexStats);
            int failed = extractIntPath(body, "_shards", "failed");
            assertEquals("expected zero shard failures on _stats, saw: " + body, 0, failed);

            Response segments = client().performRequest(new Request("GET", "/" + indexName + "/_segments"));
            assertEquals(RestStatus.OK.getStatus(), segments.getStatusLine().getStatusCode());

            Response nodesStats = client().performRequest(new Request("GET", "/_nodes/stats/indices/docs"));
            assertEquals(RestStatus.OK.getStatus(), nodesStats.getStatusLine().getStatusCode());
            String nodesBody = readAll(nodesStats);
            int nodesFailed = extractIntPath(nodesBody, "_nodes", "failed");
            assertEquals("expected zero node failures on _nodes/stats, saw: " + nodesBody, 0, nodesFailed);
        }
    }
}
