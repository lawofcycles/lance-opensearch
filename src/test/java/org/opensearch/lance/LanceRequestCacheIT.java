/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;

/**
 * The coordinator result cache against the single node test cluster:
 * a repeated {@code size: 0} aggregation body is served from the cache
 * with the same answer, the {@code request_cache} counters of
 * {@code GET /_lance/stats} move with hits, misses, skips and
 * invalidations, an append to the table advances the version and
 * misses, {@code POST /<index>/_cache/clear} and {@code DELETE /<index>}
 * drop the entries, and the explain endpoint says which bodies qualify.
 */
public class LanceRequestCacheIT extends LanceRestTestCase {

    private static final String SUM = "{\"size\":0,\"query\":{\"match_all\":{}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}";
    private static final String SUM_REORDERED = "{\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}},\"query\":{\"match_all\":{}},\"size\":0}";

    public void testRepeatedAggregationIsServedFromTheCacheUntilTheTableMovesOn() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(6, "rcache")) {
            String index = fixture.indexName();
            Map<String, Object> before = requestCache();
            assertEquals(true, before.get("enabled"));
            assertTrue("a limit derived from the heap: " + before, count(before, "limit_bytes") > 0L);

            // The first request misses and stores; the second is a hit
            // with the same answer and a took of its own.
            String first = readAll(postJson("/" + index + "/_search", SUM));
            assertEquals(6, extractIntPath(first, "hits", "total", "value"));
            assertEquals(15.0d, extractDoublePath(first, "aggregations", "s", "value"), 0.0d);
            Map<String, Object> afterFirst = requestCache();
            assertEquals(count(before, "misses") + 1, count(afterFirst, "misses"));
            assertEquals(count(before, "hits"), count(afterFirst, "hits"));
            assertEquals(1L, count(afterFirst, "entries") - count(before, "entries"));
            assertTrue("the entry weighs something: " + afterFirst, count(afterFirst, "size_bytes") > count(before, "size_bytes"));

            String second = readAll(postJson("/" + index + "/_search", SUM));
            Map<String, Object> afterSecond = requestCache();
            assertEquals("the repeat is a hit", count(afterFirst, "hits") + 1, count(afterSecond, "hits"));
            assertEquals(count(afterFirst, "misses"), count(afterSecond, "misses"));
            assertEquals(6, extractIntPath(second, "hits", "total", "value"));
            assertEquals(15.0d, extractDoublePath(second, "aggregations", "s", "value"), 0.0d);
            assertEquals(1, extractIntPath(second, "_shards", "total"));
            assertEquals("false", String.valueOf(parseJson(second).get("timed_out")));
            logger.info("--> result cache took: first {} ms, second {} ms", extractIntPath(first, "took"), extractIntPath(second, "took"));

            // The same body in another key order is the same key.
            readAll(postJson("/" + index + "/_search", SUM_REORDERED));
            assertEquals(count(afterSecond, "hits") + 1, count(requestCache(), "hits"));

            // _count is a size 0 request too.
            String count = readAll(client().performRequest(new Request("GET", "/" + index + "/_count")));
            assertEquals(6, extractIntPath(count, "count"));
            Map<String, Object> afterCount = requestCache();
            assertEquals(6, extractIntPath(readAll(client().performRequest(new Request("GET", "/" + index + "/_count"))), "count"));
            assertEquals("the repeated count is a hit", count(afterCount, "hits") + 1, count(requestCache(), "hits"));

            // Requests the cache does not take: hits requested, opted out.
            Map<String, Object> beforeSkips = requestCache();
            readAll(postJson("/" + index + "/_search", "{\"size\":10,\"query\":{\"match_all\":{}}}"));
            assertEquals("size > 0 is skipped", count(beforeSkips, "skipped") + 1, count(requestCache(), "skipped"));
            readAll(postJson("/" + index + "/_search?request_cache=false", SUM));
            Map<String, Object> afterOptOut = requestCache();
            assertEquals("request_cache=false is skipped", count(beforeSkips, "skipped") + 2, count(afterOptOut, "skipped"));
            assertEquals("and neither hits", count(beforeSkips, "hits"), count(afterOptOut, "hits"));
            assertEquals(count(beforeSkips, "misses"), count(afterOptOut, "misses"));

            // Explain says which bodies qualify.
            assertEquals(true, explain(index, SUM).get("cacheable"));
            Map<String, Object> page = explain(index, "{\"size\":10,\"query\":{\"match_all\":{}}}");
            assertEquals(false, page.get("cacheable"));
            assertEquals("size > 0", page.get("cacheable_reason"));
            Map<String, Object> from = explain(index, "{\"size\":0,\"from\":3}");
            assertEquals(false, from.get("cacheable"));
            assertEquals("from > 0", from.get("cacheable_reason"));

            // An append advances the manifest: the same body is a miss
            // and answers over the new rows.
            LanceTableFactory.appendRows(fixture.tableUri(), 6, 4);
            Map<String, Object> beforeAppend = requestCache();
            String appended = readAll(postJson("/" + index + "/_search", SUM));
            assertEquals(10, extractIntPath(appended, "hits", "total", "value"));
            assertEquals(45.0d, extractDoublePath(appended, "aggregations", "s", "value"), 0.0d);
            Map<String, Object> afterAppend = requestCache();
            assertEquals("a new version is a new key", count(beforeAppend, "misses") + 1, count(afterAppend, "misses"));
            assertEquals(count(beforeAppend, "hits"), count(afterAppend, "hits"));
            assertEquals(10, extractIntPath(readAll(postJson("/" + index + "/_search", SUM)), "hits", "total", "value"));
            assertEquals(count(afterAppend, "hits") + 1, count(requestCache(), "hits"));

            // The stock cache clear drops the index's entries.
            Map<String, Object> beforeClear = requestCache();
            logger.info("--> request_cache stats before the clear: {}", beforeClear);
            Response clear = client().performRequest(new Request("POST", "/" + index + "/_cache/clear?request=true"));
            assertEquals(200, clear.getStatusLine().getStatusCode());
            Map<String, Object> afterClear = requestCache();
            assertTrue(
                "the clear invalidated the index's entries: " + beforeClear + " -> " + afterClear,
                count(afterClear, "invalidations") > count(beforeClear, "invalidations")
            );
            assertEquals(
                count(beforeClear, "entries") - count(afterClear, "entries"),
                count(afterClear, "invalidations") - count(beforeClear, "invalidations")
            );
            readAll(postJson("/" + index + "/_search", SUM));
            assertEquals("after the clear the body misses again", count(afterClear, "misses") + 1, count(requestCache(), "misses"));

            // A clear that names another cache leaves the entries alone.
            Map<String, Object> beforeQueryClear = requestCache();
            client().performRequest(new Request("POST", "/" + index + "/_cache/clear?query=true"));
            assertEquals(count(beforeQueryClear, "invalidations"), count(requestCache(), "invalidations"));
            assertEquals(count(beforeQueryClear, "entries"), count(requestCache(), "entries"));

            // Deleting the index drops its entries.
            Map<String, Object> beforeDelete = requestCache();
            assertTrue(count(beforeDelete, "entries") >= 1L);
            client().performRequest(new Request("DELETE", "/" + index));
            assertBusy(() -> {
                Map<String, Object> afterDelete = requestCache();
                assertTrue(
                    "the deletion invalidated the entries: " + beforeDelete + " -> " + afterDelete,
                    count(afterDelete, "invalidations") >= count(beforeDelete, "invalidations") + count(beforeDelete, "entries")
                );
            });
        }
    }

    public void testDisablingTheCacheDropsTheEntriesAndStopsCaching() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(6, "rcacheoff")) {
            String index = fixture.indexName();
            readAll(postJson("/" + index + "/_search", SUM));
            assertTrue(count(requestCache(), "entries") >= 1L);
            Request off = new Request("PUT", "/_cluster/settings");
            off.setJsonEntity("{\"transient\":{\"lance.request_cache.enabled\":false}}");
            client().performRequest(off);
            try {
                Map<String, Object> disabled = requestCache();
                assertEquals(false, disabled.get("enabled"));
                assertEquals(0L, count(disabled, "entries"));
                assertEquals(0L, count(disabled, "size_bytes"));
                readAll(postJson("/" + index + "/_search", SUM));
                readAll(postJson("/" + index + "/_search", SUM));
                Map<String, Object> after = requestCache();
                assertEquals("nothing counted while off", count(disabled, "hits"), count(after, "hits"));
                assertEquals(count(disabled, "misses"), count(after, "misses"));
                assertEquals(count(disabled, "skipped"), count(after, "skipped"));
                Map<String, Object> explain = explain(index, SUM);
                assertEquals(false, explain.get("cacheable"));
                assertEquals("disabled", explain.get("cacheable_reason"));
            } finally {
                Request on = new Request("PUT", "/_cluster/settings");
                on.setJsonEntity("{\"transient\":{\"lance.request_cache.enabled\":null}}");
                client().performRequest(on);
            }
            assertEquals(true, requestCache().get("enabled"));
            client().performRequest(new Request("DELETE", "/" + index));
        }
    }

    public void testExpireDropsAnEntryAfterItsAge() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(6, "rcacheexp")) {
            String index = fixture.indexName();
            Request expire = new Request("PUT", "/_cluster/settings");
            expire.setJsonEntity("{\"transient\":{\"lance.request_cache.expire\":\"1s\"}}");
            client().performRequest(expire);
            try {
                readAll(postJson("/" + index + "/_search", SUM));
                Map<String, Object> stored = requestCache();
                readAll(postJson("/" + index + "/_search", SUM));
                assertEquals("within the age the repeat hits", count(stored, "hits") + 1, count(requestCache(), "hits"));
                // Once the age has passed the repeat misses (and stores
                // anew); until then it hits and the check tries again.
                assertBusy(() -> {
                    try {
                        Map<String, Object> aged = requestCache();
                        readAll(postJson("/" + index + "/_search", SUM));
                        Map<String, Object> after = requestCache();
                        assertEquals("past the age the repeat misses", count(aged, "misses") + 1, count(after, "misses"));
                        assertEquals("the aged entry counts as evicted", count(aged, "evictions") + 1, count(after, "evictions"));
                    } catch (ResponseException e) {
                        throw new AssertionError("index temporarily unavailable: " + e.getMessage(), e);
                    }
                }, 10, TimeUnit.SECONDS);
            } finally {
                Request reset = new Request("PUT", "/_cluster/settings");
                reset.setJsonEntity("{\"transient\":{\"lance.request_cache.expire\":null}}");
                client().performRequest(reset);
            }
            client().performRequest(new Request("DELETE", "/" + index));
        }
    }

    /** The {@code request_cache} object of the single node's stats. */
    @SuppressWarnings("unchecked")
    static Map<String, Object> requestCache() throws IOException {
        Map<String, Object> parsed = parseJson(readAll(client().performRequest(new Request("GET", "/_lance/stats"))));
        Map<String, Object> nodes = (Map<String, Object>) parsed.get("nodes");
        assertEquals("single node cluster", 1, nodes.size());
        Map<String, Object> node = (Map<String, Object>) nodes.values().iterator().next();
        Map<String, Object> requestCache = (Map<String, Object>) node.get("request_cache");
        assertNotNull("request_cache block: " + node.keySet(), requestCache);
        return requestCache;
    }

    static long count(Map<String, Object> block, String key) {
        return ((Number) block.get(key)).longValue();
    }

    private static Map<String, Object> explain(String index, String body) throws IOException {
        Request explain = new Request("GET", "/" + index + "/_lance/explain");
        explain.setJsonEntity(body);
        return parseJson(readAll(client().performRequest(explain)));
    }
}
