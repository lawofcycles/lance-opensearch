/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.core.rest.RestStatus;

/**
 * The {@code index.lance.tag} setting is Dynamic: an operator repointing
 * the tag with {@code PUT /{index}/_settings} on a running index takes
 * effect on the next namespace poll cycle without a detach or reattach.
 * Attaches an index following tag {@code v1} (at manifest version A),
 * then rewrites the setting to tag {@code v2} (at manifest version B),
 * and asserts that both the fragment path and the shard engine reader
 * follow the new tag.
 */
public class LanceNamespaceTagDynamicIT extends LanceRestTestCase {

    public void testDynamicTagRepointRefreshesReader() throws Exception {
        String suffix = "tagdyn-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        // Version A has six rows; append lifts the manifest to version
        // B with ten. Tags v1 -> A, v2 -> B.
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        long versionA = LanceTableFactory.currentVersion(tableUri);
        LanceTableFactory.appendRows(tableUri, 6, 4);
        long versionB = LanceTableFactory.currentVersion(tableUri);
        assertTrue("append must advance the manifest", versionB > versionA);
        LanceTableFactory.createTag(tableUri, "v1", versionA);
        LanceTableFactory.createTag(tableUri, "v2", versionB);

        String indexName = tableName + "-dyntag";
        String matchAllBody = "{\"query\":{\"match_all\":{}},\"size\":0}";
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\",\"name\":\"" + indexName + "\",\"tag\":\"v1\"}");
            String attachBody = readAll(attach);
            assertEquals("attach with tag v1 failed: " + attachBody, RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            assertEquals("attach must report v1's version", (int) versionA, extractIntPath(attachBody, "version"));

            ensureGreen(indexName);
            assertEquals("shard engine must serve v1 before the repoint", 6, engineDocCount(indexName));
            assertEquals(
                "fragment path must serve v1 before the repoint",
                6,
                extractIntPath(readAll(postJson("/" + indexName + "/_search", matchAllBody)), "hits", "total", "value")
            );

            // Rewrite the tag on a running index: the setting is
            // Dynamic, so PUT /_settings updates cluster state without
            // a detach.
            Request updateSettings = new Request("PUT", "/" + indexName + "/_settings");
            updateSettings.setJsonEntity("{\"index.lance.tag\":\"v2\"}");
            updateSettings.setOptions(updateSettings.getOptions().toBuilder().addHeader("Content-Type", "application/json"));
            Response updateResponse = client().performRequest(updateSettings);
            assertEquals(
                "PUT /_settings for index.lance.tag=v2 failed: " + readAll(updateResponse),
                RestStatus.OK.getStatus(),
                updateResponse.getStatusLine().getStatusCode()
            );

            // The setting is visible in _settings straight away.
            String settingsBody = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_settings")));
            assertTrue(
                "expected index.lance.tag=v2 to persist after PUT /_settings: " + settingsBody,
                settingsBody.contains("\"tag\":\"v2\"")
            );

            // Fragment path re-reads the tag per request and swaps
            // immediately; the shard engine reader follows on the
            // next namespace poll.
            assertEquals(
                "fragment path must follow the new tag at once",
                10,
                extractIntPath(readAll(postJson("/" + indexName + "/_search", matchAllBody)), "hits", "total", "value")
            );
            assertBusy(
                () -> { assertEquals("shard engine must follow the new tag on the next poll", 10, engineDocCount(indexName)); },
                60,
                java.util.concurrent.TimeUnit.SECONDS
            );

            // Repointing back is a move too: the poll compares for
            // inequality, not for a forward advance.
            Request repointBack = new Request("PUT", "/" + indexName + "/_settings");
            repointBack.setJsonEntity("{\"index.lance.tag\":\"v1\"}");
            repointBack.setOptions(repointBack.getOptions().toBuilder().addHeader("Content-Type", "application/json"));
            Response repointResponse = client().performRequest(repointBack);
            assertEquals(RestStatus.OK.getStatus(), repointResponse.getStatusLine().getStatusCode());
            assertEquals(
                "fragment path must follow the repoint back to v1",
                6,
                extractIntPath(readAll(postJson("/" + indexName + "/_search", matchAllBody)), "hits", "total", "value")
            );
            assertBusy(
                () -> { assertEquals("shard engine must follow the repoint back to v1", 6, engineDocCount(indexName)); },
                60,
                java.util.concurrent.TimeUnit.SECONDS
            );
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    /**
     * Row count as the shard engine reader sees it ({@code _stats}
     * docs.count comes from {@code Engine#docStats}).
     */
    private static int engineDocCount(String indexName) throws IOException {
        String stats = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_stats/docs")));
        return extractIntPath(stats, "indices", indexName, "primaries", "docs", "count");
    }
}
