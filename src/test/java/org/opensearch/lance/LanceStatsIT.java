/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
            Map<String, Object> requestCache = requestCache(afterSurface);
            assertEquals(true, requestCache.get("enabled"));
            assertTrue(
                "result cache limit must be positive, saw " + requestCache,
                ((Number) requestCache.get("limit_bytes")).longValue() > 0L
            );
            for (String counter : List.of("size_bytes", "entries", "hits", "misses", "evictions", "invalidations", "skipped")) {
                assertTrue("request_cache." + counter + ": " + requestCache, requestCache.containsKey(counter));
            }
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
            // building its own. The result cache is opted out of so the
            // second request below reads the store instead of the cache.
            String sum = "{\"size\":0,\"query\":{\"match_all\":{}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}";
            String first = readAll(postJson("/" + index + "/_search?request_cache=false", sum));
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
            String second = readAll(postJson("/" + index + "/_search?request_cache=false", sum));
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
            // The column came from the store, so nothing sits on the
            // request breaker for it. The refusal counter is cumulative
            // since node start and other tests in the same cluster may
            // have driven it, so only its presence is checked here.
            assertEquals(0, columnStore(afterSecond).get("heap_fallback_bytes"));
            assertTrue(columnStore(afterSecond).containsKey("heap_fallback_rejections"));

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
            assertEquals(
                "GET opened nothing; _count and _search each opened the table once on the coordinator",
                opens + 2,
                number(snapshots(after).get("dataset_open_count"))
            );
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + tableName));
            } catch (Exception ignored) {
                // best-effort cleanup; the base class wipes indices too
            }
            deleteRecursively(scratchDir);
        }
    }

    public void testWarmUpRunsAfterAttachAndAfterNamespaceSurface() throws Exception {
        // A table with one index of every kind the warm-up scans: the
        // attach creates the index, the node sees it in cluster state and
        // warms every Lance index; the stats report one entry per index
        // with state done.
        String suffix = "warm-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeIndexedFixtureTable(scratchDir, tableName, 2, 150);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        try {
            long sessionBytesBefore = ((Number) nativeMemory(nodeStats()).get("session_bytes")).longValue();
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(readAll(attach), 200, attach.getStatusLine().getStatusCode());
            ensureGreen(tableName);

            Map<String, Object> warmUp = awaitWarmUp(tableName, "done");
            // The indexes the warm-up opened sit in the Lance Session
            // cache, so its live size has grown; a warm-up that reported
            // done without scanning would leave it where it was.
            long sessionBytesAfter = ((Number) nativeMemory(nodeStats()).get("session_bytes")).longValue();
            assertTrue("session bytes " + sessionBytesBefore + " -> " + sessionBytesAfter, sessionBytesAfter > sessionBytesBefore);
            assertEquals("metadata", warmUp.get("mode"));
            assertEquals(tableUri, warmUp.get("table"));
            assertTrue(warmUp.toString(), ((Number) warmUp.get("version")).longValue() >= 1L);
            assertTrue(warmUp.containsKey("started_at"));
            Map<String, Map<String, Object>> indexes = indexesByName(warmUp);
            assertEquals(indexes.toString(), 4, indexes.size());
            assertEquals("done", indexes.get("rating_btree").get("state"));
            assertEquals("BTree", indexes.get("rating_btree").get("type"));
            assertEquals("rating", indexes.get("rating_btree").get("column"));
            assertEquals("done", indexes.get("category_bitmap").get("state"));
            assertEquals("done", indexes.get("body_fts").get("state"));
            assertEquals("done", indexes.get("embedding_ivf").get("state"));

            // The requests then find the indexes loaded and answer as
            // before.
            String term = readAll(postJson("/" + tableName + "/_search", "{\"size\":0,\"query\":{\"term\":{\"rating\":37}}}"));
            assertEquals(1, extractIntPath(term, "hits", "total", "value"));
            String match = readAll(postJson("/" + tableName + "/_search", "{\"size\":0,\"query\":{\"match\":{\"body\":\"tok7\"}}}"));
            assertEquals(1, extractIntPath(match, "hits", "total", "value"));

            // Deleting the index drops its entry.
            client().performRequest(new Request("DELETE", "/" + tableName));
            assertBusy(() -> assertNull(warmUpOf(tableName)));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + tableName));
            } catch (Exception ignored) {
                // best-effort cleanup; the base class wipes indices too
            }
            deleteRecursively(scratchDir);
        }

        // The namespace poll surfacing a table takes the same path: the
        // index appears in cluster state and the node warms it.
        try (LanceTestCluster fixture = LanceTestCluster.setUpHintFixture(2, 100, "warmns")) {
            Map<String, Object> warmUp = awaitWarmUp(fixture.indexName(), "done");
            Map<String, Map<String, Object>> indexes = indexesByName(warmUp);
            assertEquals(indexes.toString(), 1, indexes.size());
            assertEquals("done", indexes.get("body_fts").get("state"));
            assertEquals("Inverted", indexes.get("body_fts").get("type"));
            client().performRequest(new Request("DELETE", "/" + fixture.indexName()));
        }
    }

    public void testWarmUpNoneRecordsSkipped() throws Exception {
        Request none = new Request("PUT", "/_cluster/settings");
        none.setJsonEntity("{\"transient\":{\"lance.attach.warm_indexes\":\"none\"}}");
        client().performRequest(none);
        try (LanceTestCluster fixture = LanceTestCluster.setUpHintFixture(2, 100, "warmnone")) {
            Map<String, Object> node = nodeStats();
            assertEquals("none", warmUpBlock(node).get("mode"));
            Map<String, Object> warmUp = awaitWarmUp(fixture.indexName(), "skipped");
            assertEquals("none", warmUp.get("mode"));
            assertEquals(0, ((List<?>) warmUp.get("indexes")).size());
            assertEquals(0.0d, ((Number) warmUp.get("seconds")).doubleValue(), 0.0d);
            client().performRequest(new Request("DELETE", "/" + fixture.indexName()));
        } finally {
            Request reset = new Request("PUT", "/_cluster/settings");
            reset.setJsonEntity("{\"transient\":{\"lance.attach.warm_indexes\":null}}");
            client().performRequest(reset);
        }
    }

    public void testFetchTakesAreCountedForHitsAndNotForAggregations() throws Exception {
        // Two fragments of 10,000 rows: the token sp7 matches i % 625 == 7,
        // 16 rows per fragment, below the reader's sparse ratio, so a sort
        // over its hits takes the sort column for those rows instead of
        // loading the whole column.
        try (LanceTestCluster fixture = LanceTestCluster.setUpHintFixture(2, 10_000, "fetch")) {
            String index = fixture.indexName();
            Map<String, Object> before = fetch(nodeStats());
            for (String key : List.of(
                "take_count",
                "take_rows",
                "take_columns",
                "take_millis_total",
                "take_max_millis",
                "stored_fields_takes",
                "column_takes"
            )) {
                assertTrue("fetch reports " + key + ": " + before, before.containsKey(key));
            }

            // A page of ten hits: the rows behind the hits are taken once
            // per leaf that holds a hit, and the takes address the ten rows.
            String page = readAll(postJson("/" + index + "/_search", "{\"size\":10,\"query\":{\"match_all\":{}}}"));
            @SuppressWarnings("unchecked")
            Map<String, Object> pageHits = (Map<String, Object>) parse(page).get("hits");
            assertEquals(page, 10, ((List<?>) pageHits.get("hits")).size());
            Map<String, Object> afterPage = fetch(nodeStats());
            long storedFieldsTakes = number(afterPage.get("stored_fields_takes")) - number(before.get("stored_fields_takes"));
            assertTrue("the page took its rows: " + afterPage, storedFieldsTakes >= 1);
            assertEquals("no column take for an unsorted match_all page", before.get("column_takes"), afterPage.get("column_takes"));
            assertEquals(
                "every take is counted once",
                storedFieldsTakes,
                number(afterPage.get("take_count")) - number(before.get("take_count"))
            );
            assertEquals("the takes addressed the ten hits", 10, number(afterPage.get("take_rows")) - number(before.get("take_rows")));
            assertTrue(
                "each take projects the surfaced columns",
                number(afterPage.get("take_columns")) > number(before.get("take_columns"))
            );
            assertTrue(number(afterPage.get("take_millis_total")) >= number(before.get("take_millis_total")));
            assertTrue(number(afterPage.get("take_max_millis")) >= number(before.get("take_max_millis")));

            // A sort over the sparse full text hit set takes the sort column
            // for the 32 hits (one take per leaf) next to the stored fields
            // take of the page.
            String sorted = readAll(
                postJson(
                    "/" + index + "/_search",
                    "{\"size\":5,\"query\":{\"match\":{\"body\":\"sp7\"}},\"sort\":[{\"rating\":\"desc\"}],\"track_total_hits\":true}"
                )
            );
            assertEquals(sorted, 32, extractIntPath(sorted, "hits", "total", "value"));
            Map<String, Object> afterSorted = fetch(nodeStats());
            logger.info("fetch stats after a page of ten and a sorted page over 32 sparse hits: {}", afterSorted);
            assertTrue(
                "the page took its rows",
                number(afterSorted.get("stored_fields_takes")) > number(afterPage.get("stored_fields_takes"))
            );
            assertEquals(
                "the sort column was taken once per leaf",
                2,
                number(afterSorted.get("column_takes")) - number(afterPage.get("column_takes"))
            );
            assertEquals(
                "the column takes addressed the hits, the stored fields takes the page",
                32 + 5,
                number(afterSorted.get("take_rows")) - number(afterPage.get("take_rows"))
            );

            // An aggregation without hits renders no row, so it issues no
            // take.
            String sum = readAll(
                postJson(
                    "/" + index + "/_search",
                    "{\"size\":0,\"track_total_hits\":true,\"query\":{\"match_all\":{}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}"
                )
            );
            assertEquals(20_000, extractIntPath(sum, "hits", "total", "value"));
            Map<String, Object> afterSum = fetch(nodeStats());
            assertEquals("a size 0 aggregation takes nothing", afterSorted.get("take_count"), afterSum.get("take_count"));
            assertEquals(afterSorted.get("take_rows"), afterSum.get("take_rows"));
            assertEquals(afterSorted.get("stored_fields_takes"), afterSum.get("stored_fields_takes"));
            assertEquals(afterSorted.get("column_takes"), afterSum.get("column_takes"));
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> fetch(Map<String, Object> node) {
        return (Map<String, Object>) node.get("fetch");
    }

    /** Wait for the warm-up entry of {@code index} to reach {@code state} and return it. */
    private static Map<String, Object> awaitWarmUp(String index, String state) throws Exception {
        assertBusy(() -> {
            Map<String, Object> entry = warmUpOf(index);
            assertNotNull("no warm_up entry for " + index, entry);
            assertEquals(entry.toString(), state, entry.get("state"));
        }, 60, TimeUnit.SECONDS);
        return warmUpOf(index);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> warmUpOf(String index) throws IOException {
        for (Object table : (List<Object>) warmUpBlock(nodeStats()).get("tables")) {
            Map<String, Object> entry = (Map<String, Object>) table;
            if (index.equals(entry.get("index"))) {
                return entry;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Map<String, Object>> indexesByName(Map<String, Object> warmUp) {
        Map<String, Map<String, Object>> byName = new HashMap<>();
        for (Object index : (List<Object>) warmUp.get("indexes")) {
            Map<String, Object> entry = (Map<String, Object>) index;
            byName.put((String) entry.get("name"), entry);
        }
        return byName;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> warmUpBlock(Map<String, Object> node) {
        return (Map<String, Object>) node.get("warm_up");
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
    private static Map<String, Object> requestCache(Map<String, Object> node) {
        return (Map<String, Object>) node.get("request_cache");
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
