/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

/**
 * {@code GET /_lance/stats} against the single node test cluster: the
 * envelope, the counters moving with fragment path requests, and the shard
 * path (GET, {@code _stats}) sharing the fragment path's snapshot.
 */
public class LanceStatsIT extends LanceRestTestCase {

    public void testStatsReportSnapshotSharingAndColumnStoreTraffic() throws Exception {
        // The hint fixture has 2 fragments of 100 rows with a nullable
        // int rating. The column store traffic below comes from the
        // Lucene aggregator reading rating through the fragment leaf
        // reader; with the aggregation pushdown on, a size 0 sum runs
        // inside the Lance scan and never touches the store, so the
        // pushdown is turned off for this test.
        Request disablePushdown = new Request("PUT", "/_cluster/settings");
        disablePushdown.setJsonEntity("{\"transient\":{\"lance.aggregation.pushdown\":false}}");
        client().performRequest(disablePushdown);
        try (LanceTestCluster fixture = LanceTestCluster.setUpHintFixture(2, 100, "stats")) {
            String index = fixture.indexName();

            // Surfacing the index opened the shard engine, whose reader
            // leases the table's snapshot: one snapshot, one build, no
            // fragment path traffic yet.
            Map<String, Object> afterSurface = nodeStats();
            assertEquals(1, snapshots(afterSurface).get("count"));
            assertEquals(0, snapshots(afterSurface).get("retired"));
            assertEquals(true, snapshots(afterSurface).get("enabled"));
            int buildsAfterSurface = number(snapshots(afterSurface).get("snapshot_build_count"));
            int hitsAfterSurface = number(snapshots(afterSurface).get("snapshot_hit_count"));
            long limitBytes = ((Number) columnStore(afterSurface).get("limit_bytes")).longValue();
            assertTrue("column store limit must be positive, saw " + limitBytes, limitBytes > 0L);
            assertEquals(1_000_000, fts(afterSurface).get("subset_probe_limit"));
            Map<String, Object> nativeMemory = nativeMemory(afterSurface);
            assertTrue(nativeMemory.containsKey("estimated_bytes"));
            assertTrue(nativeMemory.containsKey("session_bytes"));
            assertEquals(columnStore(afterSurface).get("bytes"), nativeMemory.get("column_store_bytes"));
            // The index cache sizing is fixed at startup: a positive
            // capacity split into at least one shard, and the share is
            // the capacity divided by the shard count.
            long indexCacheCapacity = ((Number) nativeMemory.get("index_cache_capacity")).longValue();
            int indexCacheShards = number(nativeMemory.get("index_cache_shards"));
            long indexCacheShardShare = ((Number) nativeMemory.get("index_cache_shard_share")).longValue();
            assertTrue("index cache capacity must be positive, saw " + indexCacheCapacity, indexCacheCapacity > 0L);
            assertTrue("index cache shards must be at least 1, saw " + indexCacheShards, indexCacheShards >= 1);
            assertEquals(indexCacheCapacity / indexCacheShards, indexCacheShardShare);

            // Shard path: _stats docs.count reads the engine's reader over
            // the snapshot (GET on this fixture is answered 404 before the
            // reader is touched, because the table declares no primary
            // key; testGetSharesTheSnapshot covers GET).
            String stats = readAll(client().performRequest(new Request("GET", "/" + index + "/_stats/docs")));
            assertEquals(200, extractIntPath(stats, "indices", index, "primaries", "docs", "count"));

            // Fragment path: a sum over rating loads the column into the
            // store; the request finds the engine's snapshot instead of
            // building its own.
            String sum = "{\"size\":0,\"query\":{\"match_all\":{}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}";
            String first = readAll(postJson("/" + index + "/_search", sum));
            assertEquals(200, extractIntPath(first, "hits", "total", "value"));
            Map<String, Object> afterFirst = nodeStats();
            assertEquals("GET, _stats and _search share the engine's snapshot", 1, snapshots(afterFirst).get("count"));
            assertEquals("no second snapshot build", buildsAfterSurface, number(snapshots(afterFirst).get("snapshot_build_count")));
            assertTrue(
                "the fragment path request hit the cached snapshot",
                number(snapshots(afterFirst).get("snapshot_hit_count")) > hitsAfterSurface
            );
            int loadsAfterFirst = number(columnStore(afterFirst).get("loads"));
            int hitsAfterFirst = number(columnStore(afterFirst).get("hits"));
            long bytesAfterFirst = ((Number) columnStore(afterFirst).get("bytes")).longValue();
            assertTrue("the sum loaded rating into the store", loadsAfterFirst >= 1);
            assertTrue("entries after the load", number(columnStore(afterFirst).get("entries")) >= 2);
            assertTrue("bytes " + bytesAfterFirst + " within limit " + limitBytes, bytesAfterFirst > 0L && bytesAfterFirst <= limitBytes);

            // The same request again reads rating from the store.
            String second = readAll(postJson("/" + index + "/_search", sum));
            assertEquals(
                extractDoublePath(first, "aggregations", "s", "value"),
                extractDoublePath(second, "aggregations", "s", "value"),
                0.0d
            );
            Map<String, Object> afterSecond = nodeStats();
            assertEquals("no load on the second request", loadsAfterFirst, number(columnStore(afterSecond).get("loads")));
            assertTrue("the second request hit the store", number(columnStore(afterSecond).get("hits")) > hitsAfterFirst);
            assertEquals(1, snapshots(afterSecond).get("count"));
            assertEquals(buildsAfterSurface, number(snapshots(afterSecond).get("snapshot_build_count")));
            assertEquals(0, columnStore(afterSecond).get("budget_misses"));

            // Deleting the index releases the engine's lease and retires the
            // snapshot; the store gives its columns back.
            client().performRequest(new Request("DELETE", "/" + index));
            assertBusy(() -> {
                Map<String, Object> afterDelete = nodeStats();
                assertEquals(0, snapshots(afterDelete).get("count"));
                assertEquals(0, snapshots(afterDelete).get("retired"));
                assertEquals(0, columnStore(afterDelete).get("entries"));
                assertEquals(0L, ((Number) columnStore(afterDelete).get("bytes")).longValue());
            });
        } finally {
            Request enablePushdown = new Request("PUT", "/_cluster/settings");
            enablePushdown.setJsonEntity("{\"transient\":{\"lance.aggregation.pushdown\":null}}");
            client().performRequest(enablePushdown);
        }
    }

    public void testGetSharesTheSnapshot() throws Exception {
        // An attached Utf8 primary key table: GET resolves the key through
        // a Lance scan on the engine reader's dataset, which is the cached
        // snapshot's. _count then runs on the fragment path against the
        // same snapshot.
        String suffix = "statsget-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeStringPkTable(scratchDir, tableName, 4);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(readAll(attach), 200, attach.getStatusLine().getStatusCode());
            ensureGreen(tableName);

            Map<String, Object> afterAttach = nodeStats();
            assertEquals(1, snapshots(afterAttach).get("count"));
            int builds = number(snapshots(afterAttach).get("snapshot_build_count"));
            int opens = number(snapshots(afterAttach).get("dataset_open_count"));

            Response hit = client().performRequest(new Request("GET", "/" + tableName + "/_doc/alpha-2"));
            assertEquals(200, hit.getStatusLine().getStatusCode());
            assertTrue(readAll(hit).contains("\"_id\":\"alpha-2\""));
            String count = readAll(client().performRequest(new Request("GET", "/" + tableName + "/_count")));
            assertEquals(4, extractIntPath(count, "count"));
            String search = readAll(postJson("/" + tableName + "/_search", "{\"size\":4,\"sort\":[{\"key\":\"asc\"}]}"));
            assertEquals(4, extractIntPath(search, "hits", "total", "value"));

            Map<String, Object> after = nodeStats();
            assertEquals("GET, _count and _search read one snapshot", 1, snapshots(after).get("count"));
            assertEquals("none of them built a snapshot", builds, number(snapshots(after).get("snapshot_build_count")));
            assertEquals("none of them opened a dataset through the cache", opens, number(snapshots(after).get("dataset_open_count")));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + tableName));
            } catch (Exception ignored) {
                // best-effort cleanup; the base class wipes indices too
            }
            deleteRecursively(scratchDir);
        }
    }

    public void testStatsEnvelopeAndNodeFilter() throws Exception {
        String body = readAll(client().performRequest(new Request("GET", "/_lance/stats")));
        Map<String, Object> parsed = parse(body);
        assertEquals(1, extractIntPath(body, "_nodes", "total"));
        assertEquals(1, extractIntPath(body, "_nodes", "successful"));
        assertEquals(0, extractIntPath(body, "_nodes", "failed"));
        assertTrue(body, parsed.containsKey("cluster_name"));
        @SuppressWarnings("unchecked")
        Map<String, Object> nodes = (Map<String, Object>) parsed.get("nodes");
        assertEquals(1, nodes.size());
        String nodeId = nodes.keySet().iterator().next();

        String filtered = readAll(client().performRequest(new Request("GET", "/_lance/stats/" + nodeId)));
        assertEquals(1, extractIntPath(filtered, "_nodes", "total"));
        @SuppressWarnings("unchecked")
        Map<String, Object> filteredNodes = (Map<String, Object>) parse(filtered).get("nodes");
        assertTrue(filtered, filteredNodes.containsKey(nodeId));

        String none = readAll(client().performRequest(new Request("GET", "/_lance/stats/no-such-node")));
        assertEquals(0, extractIntPath(none, "_nodes", "total"));
    }

    /** The single node's stats object. */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> nodeStats() throws IOException {
        Map<String, Object> parsed = parse(readAll(client().performRequest(new Request("GET", "/_lance/stats"))));
        Map<String, Object> nodes = (Map<String, Object>) parsed.get("nodes");
        assertEquals("single node cluster", 1, nodes.size());
        return (Map<String, Object>) nodes.values().iterator().next();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> snapshots(Map<String, Object> node) {
        return (Map<String, Object>) node.get("snapshots");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> columnStore(Map<String, Object> node) {
        return (Map<String, Object>) node.get("column_store");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> nativeMemory(Map<String, Object> node) {
        return (Map<String, Object>) node.get("native_memory");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> fts(Map<String, Object> node) {
        return (Map<String, Object>) node.get("fts");
    }

    private static int number(Object value) {
        return ((Number) value).intValue();
    }

    private static Map<String, Object> parse(String json) throws IOException {
        try (XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, json)) {
            return parser.map();
        }
    }
}
