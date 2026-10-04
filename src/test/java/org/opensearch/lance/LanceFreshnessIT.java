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

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;

/**
 * The freshness check on the node holding the shard, driven through
 * {@code POST /_plugins/_lance/sync/{index}}: an append is a move that advances
 * the engine reader without a mapping update, a new column is a move
 * that updates the mapping, a check at the table's version does nothing,
 * a pinned index is never checked, and {@code GET /_plugins/_lance/stats} counts
 * all of it under {@code freshness}. Without a sync, the engine reads
 * the table's current version on every read.
 */
public class LanceFreshnessIT extends LanceRestTestCase {

    public void testSyncAdvancesTheReaderAndUpdatesTheMappingOnlyWhenItChanged() throws Exception {
        String suffix = "sync-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_plugins/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            ensureGreen(indexName);
            long attachedVersion = LanceTableFactory.currentVersion(tableUri);

            // At the table's version: nothing moves, nothing changes.
            Map<String, Object> idle = sync(indexName);
            assertEquals(true, idle.get("checked"));
            assertEquals(false, idle.get("moved"));
            assertEquals(false, idle.get("mapping_changed"));
            assertEquals(attachedVersion, ((Number) idle.get("served_version")).longValue());
            assertEquals(attachedVersion, ((Number) idle.get("target_version")).longValue());

            // An append: a move, the reader follows, the mapping is the same.
            // The scheduled check runs every second in the test cluster and
            // may take the move before this call does; either way the reader
            // serves the appended rows when the sync answers, and the
            // versions the sync reports are the ones it saw.
            LanceTableFactory.appendRows(tableUri, 6, 4);
            long appended = LanceTableFactory.currentVersion(tableUri);
            Map<String, Object> afterAppend = sync(indexName);
            assertEquals(appended, ((Number) afterAppend.get("target_version")).longValue());
            if ((Boolean) afterAppend.get("moved")) {
                assertEquals(attachedVersion, ((Number) afterAppend.get("served_version")).longValue());
            } else {
                assertEquals("the scheduled check took the move first", appended, ((Number) afterAppend.get("served_version")).longValue());
            }
            assertEquals("same schema, no mapping update", false, afterAppend.get("mapping_changed"));
            assertEquals(false, afterAppend.get("rebuilt"));
            assertEquals("the engine reader serves the appended rows right after the sync", 10, engineDocCount(indexName));

            // A new column: a move whose mapping differs, so it is applied
            // (by this sync, or by the scheduled check that beat it).
            LanceTableFactory.addColumn(tableUri, "score", new ArrowType.Int(64, true));
            Map<String, Object> afterColumn = sync(indexName);
            assertEquals(afterColumn.get("moved"), afterColumn.get("mapping_changed"));
            String mapping = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_mapping")));
            assertTrue("the mapping gained the column: " + mapping, mapping.contains("\"score\":{"));

            // Nothing moved since: the check is idempotent.
            Map<String, Object> again = sync(indexName);
            assertEquals(false, again.get("moved"));
            assertEquals(false, again.get("mapping_changed"));

            // The node counts every check.
            Map<String, Object> freshness = freshness();
            assertTrue("tracked: " + freshness, ((Number) freshness.get("tracked")).intValue() >= 1);
            assertTrue("checks: " + freshness, ((Number) freshness.get("checks")).intValue() >= 4);
            assertTrue("moves: " + freshness, ((Number) freshness.get("moves")).intValue() >= 2);
            assertTrue("mapping_updates: " + freshness, ((Number) freshness.get("mapping_updates")).intValue() >= 1);
            assertTrue("mapping_unchanged: " + freshness, ((Number) freshness.get("mapping_unchanged")).intValue() >= 1);
            assertTrue("last_check_millis: " + freshness, ((Number) freshness.get("last_check_millis")).longValue() > 0L);
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testSyncOfAPinnedIndexIsNotAChecked() throws Exception {
        String suffix = "syncpin-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        long version = LanceTableFactory.currentVersion(tableUri);
        String indexName = tableName;
        try {
            Response attach = postJson("/_plugins/_lance/attach", "{\"table\":\"" + tableUri + "\",\"version\":" + version + "}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            ensureGreen(indexName);
            LanceTableFactory.appendRows(tableUri, 6, 4);
            Map<String, Object> outcome = sync(indexName);
            assertEquals(false, outcome.get("checked"));
            assertTrue("reason names the pin: " + outcome, ((String) outcome.get("reason")).contains("index.plugins.lance.version"));
            assertEquals(false, outcome.get("moved"));
            assertEquals("a pinned index keeps its rows", 6, engineDocCount(indexName));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testScheduledCheckFollowsAnAppendWithoutASync() throws Exception {
        // The scheduled check (1s cadence in the test cluster) does what the
        // sync endpoint does, on its own.
        try (LanceTestCluster cluster = LanceTestCluster.setUp(6, "sched")) {
            String indexName = cluster.indexName();
            assertEquals(6, engineDocCount(indexName));
            LanceTableFactory.appendRows(cluster.tableUri(), 6, 3);
            assertBusy(() -> {
                try {
                    assertEquals(9, engineDocCount(indexName));
                } catch (IOException e) {
                    throw new AssertionError("index temporarily unavailable: " + e.getMessage(), e);
                }
            });
            client().performRequest(new Request("DELETE", "/" + indexName));
        }
    }

    public void testEngineReadsSeeACommitOnTheNextCallWithoutASyncOrACadence() throws Exception {
        // The engine refreshes before every read of its own, so a row
        // committed to the table is answered by GET and _mget, counted by
        // _stats and read by a mixed target _search on the call right
        // after the commit, with no sync and no wait for the scheduled
        // check. A string PK table gives GET something to look up.
        String suffix = "engine-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        String tableUri = LanceTableFactory.writeStringPkTable(scratchDir, tableName, 4);
        String indexName = tableName;
        String plain = "plain-" + suffix;
        try {
            Response attach = postJson("/_plugins/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            ensureGreen(indexName);
            Request create = new Request("PUT", "/" + plain);
            create.setJsonEntity(
                "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0},"
                    + "\"mappings\":{\"properties\":{\"key\":{\"type\":\"keyword\"},\"label\":{\"type\":\"keyword\"}}}}"
            );
            client().performRequest(create);
            Request doc = new Request("PUT", "/" + plain + "/_doc/1?refresh=true");
            doc.setJsonEntity("{\"key\":\"plain-1\",\"label\":\"row-1\"}");
            client().performRequest(doc);
            assertEquals(4, engineDocCount(indexName));
            ResponseException missing = expectThrows(
                ResponseException.class,
                () -> client().performRequest(new Request("GET", "/" + indexName + "/_doc/alpha-5"))
            );
            assertEquals(404, missing.getResponse().getStatusLine().getStatusCode());

            // alpha-4 and alpha-5 are committed; the very next GET answers one.
            LanceTableFactory.appendStringPkRows(tableUri, 4, 2);
            Response hit = client().performRequest(new Request("GET", "/" + indexName + "/_doc/alpha-5"));
            String hitBody = readAll(hit);
            assertEquals(hitBody, 200, hit.getStatusLine().getStatusCode());
            @SuppressWarnings("unchecked")
            Map<String, Object> source = (Map<String, Object>) parseJson(hitBody).get("_source");
            assertEquals(hitBody, "col-5", source.get("label"));
            // _mget goes through the same searcher acquisition and finds the row too.
            String mgetBody = readAll(postJson("/" + indexName + "/_mget", "{\"ids\":[\"alpha-5\"]}"));
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> mgetDocs = (List<Map<String, Object>>) parseJson(mgetBody).get("docs");
            assertEquals(mgetBody, 1, mgetDocs.size());
            assertEquals(mgetBody, Boolean.TRUE, mgetDocs.get(0).get("found"));
            @SuppressWarnings("unchecked")
            Map<String, Object> mgetSource = (Map<String, Object>) mgetDocs.get(0).get("_source");
            assertEquals(mgetBody, "col-5", mgetSource.get("label"));
            assertEquals("the next _stats counts the appended rows", 6, engineDocCount(indexName));

            // A third commit, read by the stock search path over a mixed
            // target (the Lance index next to a plain one) on its next call.
            LanceTableFactory.appendStringPkRows(tableUri, 6, 1);
            String mixed = readAll(postJson("/" + indexName + "," + plain + "/_search", "{\"size\":0,\"track_total_hits\":true}"));
            assertEquals("the mixed target search read the shard reader", 2, extractIntPath(mixed, "_shards", "total"));
            assertEquals(mixed, 7 + 1, extractIntPath(mixed, "hits", "total", "value"));
            assertEquals(7, engineDocCount(indexName));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName + "," + plain));
            } catch (Exception ignored) {}
        }
    }

    private static Map<String, Object> sync(String indexName) throws IOException {
        Response response = client().performRequest(new Request("POST", "/_plugins/_lance/sync/" + indexName));
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
        return parseJson(readAll(response));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> freshness() throws IOException {
        Map<String, Object> parsed = parseJson(readAll(client().performRequest(new Request("GET", "/_plugins/_lance/stats"))));
        Map<String, Object> nodes = (Map<String, Object>) parsed.get("nodes");
        assertEquals("single node cluster", 1, nodes.size());
        Map<String, Object> node = (Map<String, Object>) nodes.values().iterator().next();
        return (Map<String, Object>) node.get("freshness");
    }

    /** {@code docs.count} of the shard: the engine reader's view, which only the freshness check advances. */
    private static int engineDocCount(String indexName) throws IOException {
        String stats = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_stats/docs")));
        return extractIntPath(stats, "indices", indexName, "primaries", "docs", "count");
    }
}
