/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.opensearch.client.Request;
import org.opensearch.client.Response;

/**
 * The fetch cache against the single node test cluster: the second page
 * of the same rows is rendered without a take, a narrower projection's
 * cells serve a wider request for their column alone, an append to the
 * table is a new version and misses, {@code POST /<index>/_cache/clear}
 * and {@code DELETE /<index>} drop the entries, and turning the cache
 * off drops them and counts nothing.
 */
public class LanceFetchCacheIT extends LanceRestTestCase {

    /** The fixture table's surfaced columns: {@code id}, {@code body}, {@code title} (the vector column is not surfaced). */
    private static final int COLUMNS = 3;

    private static String page(String sourceElement, String sort, int size) {
        return "{\"size\":"
            + size
            + ",\"profile\":true"
            + sourceElement
            + ",\"sort\":[{\"id\":\""
            + sort
            + "\"}],\"query\":{\"match_all\":{}}}";
    }

    public void testRepeatedPageIsRenderedWithoutATakeUntilTheTableMovesOn() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(20, "fcache")) {
            String index = fixture.indexName();
            Map<String, Object> before = fetchCache();
            assertEquals(true, before.get("enabled"));
            assertTrue("a limit derived from the heap: " + before, count(before, "limit_bytes") > 0L);
            Map<String, Object> takesBefore = fetch();

            // The first page takes its ten rows and stores every cell.
            String first = readAll(postJson("/" + index + "/_search", page("", "asc", 10)));
            assertEquals(20, extractIntPath(first, "hits", "total", "value"));
            assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), sourceIds(first));
            assertEquals("the first page takes once", 1L, profileTakeCount(first));
            Map<String, Object> afterFirst = fetchCache();
            assertEquals(count(before, "misses") + 10L * COLUMNS, count(afterFirst, "misses"));
            assertEquals(count(before, "hits"), count(afterFirst, "hits"));
            assertEquals(count(before, "entries") + 10L * COLUMNS, count(afterFirst, "entries"));
            assertEquals(count(before, "rows_served"), count(afterFirst, "rows_served"));
            assertTrue("the entries weigh something: " + afterFirst, count(afterFirst, "size_bytes") > count(before, "size_bytes"));
            assertEquals(count(takesBefore, "stored_fields_takes") + 1, count(fetch(), "stored_fields_takes"));

            // The same page again: every cell is held, no take, the same
            // hits.
            Map<String, Object> takesAfterFirst = fetch();
            String second = readAll(postJson("/" + index + "/_search", page("", "asc", 10)));
            assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), sourceIds(second));
            assertEquals(sourceBodies(first), sourceBodies(second));
            assertEquals("the repeat takes nothing", 0L, profileTakeCount(second));
            Map<String, Object> afterSecond = fetchCache();
            assertEquals(count(afterFirst, "hits") + 10L * COLUMNS, count(afterSecond, "hits"));
            assertEquals(count(afterFirst, "misses"), count(afterSecond, "misses"));
            assertEquals(count(afterFirst, "rows_served") + 10L, count(afterSecond, "rows_served"));
            assertEquals(count(takesAfterFirst, "take_count"), count(fetch(), "take_count"));
            logger.info("--> fetch cache took: first {} ms, second {} ms", extractIntPath(first, "took"), extractIntPath(second, "took"));
            logger.info("--> profile of the repeat: {}", parseJson(second).get("profile"));
            logger.info("--> fetch cache stats after the repeat: {}", afterSecond);

            // A page over other rows that renders the key alone stores
            // one cell per row; a full page over them finds the key and
            // misses the rest, so it takes the rows whole; the third such
            // page is served.
            String keyOnly = readAll(postJson("/" + index + "/_search", page(",\"_source\":{\"includes\":[\"id\"]}", "desc", 10)));
            assertEquals(List.of(19, 18, 17, 16, 15, 14, 13, 12, 11, 10), sourceIds(keyOnly));
            assertEquals(1L, profileTakeCount(keyOnly));
            Map<String, Object> afterKeyOnly = fetchCache();
            assertEquals(count(afterSecond, "entries") + 10L, count(afterKeyOnly, "entries"));
            String widened = readAll(postJson("/" + index + "/_search", page("", "desc", 10)));
            assertEquals(List.of(19, 18, 17, 16, 15, 14, 13, 12, 11, 10), sourceIds(widened));
            assertEquals("a partial hit is taken whole", 1L, profileTakeCount(widened));
            assertEquals(10L, profileTakeRows(widened));
            Map<String, Object> afterWidened = fetchCache();
            assertEquals("the key cells were hits", count(afterKeyOnly, "hits") + 10L, count(afterWidened, "hits"));
            assertEquals(count(afterKeyOnly, "misses") + 10L * (COLUMNS - 1), count(afterWidened, "misses"));
            assertEquals("no row was served whole", count(afterKeyOnly, "rows_served"), count(afterWidened, "rows_served"));
            assertEquals(count(afterKeyOnly, "entries") + 10L * (COLUMNS - 1), count(afterWidened, "entries"));
            String widenedAgain = readAll(postJson("/" + index + "/_search", page("", "desc", 10)));
            assertEquals(sourceBodies(widened), sourceBodies(widenedAgain));
            assertEquals(0L, profileTakeCount(widenedAgain));
            assertEquals(count(afterWidened, "rows_served") + 10L, count(fetchCache(), "rows_served"));

            // An append advances the manifest: the same page is a miss
            // and takes again, and the previous version's entries go when
            // the freshness check retires its snapshot.
            LanceTableFactory.appendRows(fixture.tableUri(), 20, 4);
            Map<String, Object> beforeAppend = fetchCache();
            String appended = readAll(postJson("/" + index + "/_search", page("", "asc", 10)));
            assertEquals(24, extractIntPath(appended, "hits", "total", "value"));
            assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), sourceIds(appended));
            assertEquals("a new version is a new key", 1L, profileTakeCount(appended));
            Map<String, Object> afterAppend = fetchCache();
            assertEquals(count(beforeAppend, "misses") + 10L * COLUMNS, count(afterAppend, "misses"));
            assertEquals(count(beforeAppend, "hits"), count(afterAppend, "hits"));
            assertEquals(0L, profileTakeCount(readAll(postJson("/" + index + "/_search", page("", "asc", 10)))));
            assertBusy(() -> {
                Map<String, Object> retired = fetchCache();
                assertTrue(
                    "the previous version's entries were invalidated when its snapshot closed: " + beforeAppend + " -> " + retired,
                    count(retired, "invalidations") >= count(beforeAppend, "invalidations") + count(beforeAppend, "entries")
                );
            });

            // The stock cache clear drops the index's entries.
            Map<String, Object> beforeClear = fetchCache();
            assertTrue(count(beforeClear, "entries") >= 10L * COLUMNS);
            Response clear = client().performRequest(new Request("POST", "/" + index + "/_cache/clear?request=true"));
            assertEquals(200, clear.getStatusLine().getStatusCode());
            Map<String, Object> afterClear = fetchCache();
            assertEquals(
                "the clear invalidated the index's entries: " + beforeClear + " -> " + afterClear,
                count(beforeClear, "invalidations") + count(beforeClear, "entries"),
                count(afterClear, "invalidations")
            );
            assertEquals(0L, count(afterClear, "entries"));
            assertEquals(
                "after the clear the page takes again",
                1L,
                profileTakeCount(readAll(postJson("/" + index + "/_search", page("", "asc", 10))))
            );

            // Deleting the index drops its entries.
            Map<String, Object> beforeDelete = fetchCache();
            assertTrue(count(beforeDelete, "entries") >= 10L * COLUMNS);
            client().performRequest(new Request("DELETE", "/" + index));
            assertBusy(() -> {
                Map<String, Object> afterDelete = fetchCache();
                assertTrue(
                    "the deletion invalidated the entries: " + beforeDelete + " -> " + afterDelete,
                    count(afterDelete, "invalidations") >= count(beforeDelete, "invalidations") + count(beforeDelete, "entries")
                );
                assertEquals(0L, count(afterDelete, "entries"));
            });
        }
    }

    public void testDisablingTheCacheDropsTheEntriesAndStopsCaching() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(20, "fcacheoff")) {
            String index = fixture.indexName();
            readAll(postJson("/" + index + "/_search", page("", "asc", 10)));
            assertTrue(count(fetchCache(), "entries") >= 10L * COLUMNS);
            Request off = new Request("PUT", "/_cluster/settings");
            off.setJsonEntity("{\"transient\":{\"lance.fetch_cache.enabled\":false}}");
            client().performRequest(off);
            try {
                Map<String, Object> disabled = fetchCache();
                assertEquals(false, disabled.get("enabled"));
                assertEquals(0L, count(disabled, "entries"));
                assertEquals(0L, count(disabled, "size_bytes"));
                String first = readAll(postJson("/" + index + "/_search", page("", "asc", 10)));
                String second = readAll(postJson("/" + index + "/_search", page("", "asc", 10)));
                assertEquals("every page takes while off", 1L, profileTakeCount(first));
                assertEquals(1L, profileTakeCount(second));
                assertEquals(sourceBodies(first), sourceBodies(second));
                Map<String, Object> after = fetchCache();
                assertEquals("nothing counted while off", count(disabled, "hits"), count(after, "hits"));
                assertEquals(count(disabled, "misses"), count(after, "misses"));
                assertEquals(count(disabled, "rows_served"), count(after, "rows_served"));
                assertEquals(0L, count(after, "entries"));
            } finally {
                Request on = new Request("PUT", "/_cluster/settings");
                on.setJsonEntity("{\"transient\":{\"lance.fetch_cache.enabled\":null}}");
                client().performRequest(on);
            }
            assertEquals(true, fetchCache().get("enabled"));
            readAll(postJson("/" + index + "/_search", page("", "asc", 10)));
            assertEquals(
                "back on, the page is stored again",
                0L,
                profileTakeCount(readAll(postJson("/" + index + "/_search", page("", "asc", 10))))
            );
            client().performRequest(new Request("DELETE", "/" + index));
        }
    }

    /** The {@code fetch_cache} object of the single node's stats. */
    static Map<String, Object> fetchCache() throws IOException {
        return statsBlock("fetch_cache");
    }

    /** The {@code fetch} object of the single node's stats. */
    static Map<String, Object> fetch() throws IOException {
        return statsBlock("fetch");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> statsBlock(String name) throws IOException {
        Map<String, Object> parsed = parseJson(readAll(client().performRequest(new Request("GET", "/_lance/stats"))));
        Map<String, Object> nodes = (Map<String, Object>) parsed.get("nodes");
        assertEquals("single node cluster", 1, nodes.size());
        Map<String, Object> node = (Map<String, Object>) nodes.values().iterator().next();
        Map<String, Object> block = (Map<String, Object>) node.get(name);
        assertNotNull(name + " block: " + node.keySet(), block);
        return block;
    }

    static long count(Map<String, Object> block, String key) {
        return ((Number) block.get(key)).longValue();
    }

    /** {@code fetch.take_count} summed over the nodes of the response's {@code profile.lance}. */
    private static long profileTakeCount(String response) {
        return profileFetchSum(response, "take_count");
    }

    /** {@code fetch.take_rows} summed over the nodes of the response's {@code profile.lance}. */
    private static long profileTakeRows(String response) {
        return profileFetchSum(response, "take_rows");
    }

    @SuppressWarnings("unchecked")
    private static long profileFetchSum(String response, String key) {
        Map<String, Object> parsed = parseJson(response);
        Map<String, Object> profile = (Map<String, Object>) parsed.get("profile");
        assertNotNull("the response carries a profile: " + response, profile);
        Map<String, Object> lance = (Map<String, Object>) profile.get("lance");
        Map<String, Object> nodes = (Map<String, Object>) lance.get("nodes");
        assertNotNull("the profile carries the nodes: " + response, nodes);
        long sum = 0L;
        for (Object node : nodes.values()) {
            Map<String, Object> fetch = (Map<String, Object>) ((Map<String, Object>) node).get("fetch");
            sum += ((Number) fetch.get(key)).longValue();
        }
        return sum;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> hits(String response) {
        Map<String, Object> hits = (Map<String, Object>) parseJson(response).get("hits");
        return (List<Map<String, Object>>) hits.get("hits");
    }

    /** {@code _source.id} of every hit, in response order. */
    @SuppressWarnings("unchecked")
    private static List<Integer> sourceIds(String response) {
        List<Integer> ids = new ArrayList<>();
        for (Map<String, Object> hit : hits(response)) {
            ids.add(((Number) ((Map<String, Object>) hit.get("_source")).get("id")).intValue());
        }
        return ids;
    }

    /** The whole {@code _source} of every hit, in response order. */
    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> sourceBodies(String response) {
        List<Map<String, Object>> sources = new ArrayList<>();
        for (Map<String, Object> hit : hits(response)) {
            sources.add((Map<String, Object>) hit.get("_source"));
        }
        return sources;
    }
}
