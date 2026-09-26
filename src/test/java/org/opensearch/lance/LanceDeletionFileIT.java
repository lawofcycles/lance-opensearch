/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.core.rest.RestStatus;

/**
 * The fragment path over a table whose fragments carry deletion files:
 * a sorted page through the pushed Lance scan and through the Lucene
 * collector, {@code _count}, a metric and a terms aggregation, and a
 * term query on a deleted row all see the live rows alone, and the
 * page takes its rows once per fragment that still holds one.
 *
 * <p>Fixture: {@link LanceTableFactory#writeMultiFragmentTable} with
 * twelve rows in four fragments of three ({@code id} 0 to 11, fragment
 * {@code f} holding {@code 3f} to {@code 3f + 2}), then
 * {@link LanceTableFactory#deleteRows} of the last row of every
 * fragment, so each fragment has a deletion file and two live rows.
 * A second delete empties fragment 0. The table declares no primary
 * key, so {@code _id} is synthesised from the row address and the
 * rows are told apart by {@code _source.id}.
 */
public class LanceDeletionFileIT extends LanceRestTestCase {

    private static final String SORTED_PAGE = "{\"size\":10,\"profile\":true,\"sort\":[{\"id\":\"asc\"}],\"query\":{\"match_all\":{}}}";
    /** The same page kept on the Lucene collector by a {@code post_filter} every live row passes. */
    private static final String COLLECTOR_PAGE = "{\"size\":10,\"profile\":true,\"sort\":[{\"id\":\"asc\"}],\"query\":{\"match_all\":{}},"
        + "\"post_filter\":{\"range\":{\"id\":{\"gte\":0}}}}";

    public void testFragmentsWithDeletionFilesServeTheLiveRowsOnly() throws Exception {
        String suffix = "deletions-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        String tableUri = LanceTableFactory.writeMultiFragmentTable(scratchDir, tableName, 12, 3);
        LanceTableFactory.deleteRows(tableUri, "id IN (2, 5, 8, 11)");
        String index = tableName;
        try {
            // title.raw gives the terms aggregation a keyword key: one
            // bucket per live row.
            Response attach = postJson(
                "/_lance/attach",
                "{\"table\":\"" + tableUri + "\",\"overrides\":{\"title\":{\"fields\":{\"raw\":{\"type\":\"keyword\"}}}}}"
            );
            String attachBody = readAll(attach);
            assertEquals(attachBody, RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            assertEquals("four fragments, each with a deletion file: " + attachBody, 4, extractIntPath(attachBody, "fragments"));
            ensureGreen(index);

            // Both page shapes run on the fragment path, one through the
            // pushed sorted scan and one through the Lucene collector, so
            // the deletion file is applied by Lance's scan on the first
            // and by the leaf's live docs on the second.
            assertEquals("PUSHED_SCAN", planKind(index, SORTED_PAGE));
            assertEquals("LUCENE_TOPK", planKind(index, COLLECTOR_PAGE));

            assertLiveRows(index, List.of(0, 1, 3, 4, 6, 7, 9, 10), List.of(2, 5, 8, 11), 4);

            // Fragment 0 loses its two remaining rows: a fragment with
            // every row deleted, next to three with live rows.
            LanceTableFactory.deleteRows(tableUri, "id IN (0, 1)");
            assertLiveRows(index, List.of(3, 4, 6, 7, 9, 10), List.of(0, 1, 2, 5, 8, 11), 3);
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + index));
            } catch (Exception ignored) {}
            deleteRecursively(scratchDir);
        }
    }

    /**
     * Every request shape over {@code index} answers {@code live} and
     * nothing of {@code deleted}; the sorted page takes its rows once per
     * fragment with a live row ({@code fragmentsWithRows}) and the
     * collector page over the same rows is served from the fetch cache.
     */
    private static void assertLiveRows(String index, List<Integer> live, List<Integer> deleted, int fragmentsWithRows) throws Exception {
        int total = live.size();

        // The pushed sorted page: the live ids in order, one take per
        // fragment with a live row, the takes addressing every live row.
        Map<String, Object> statsBefore = fetchStats();
        String sorted = readAll(postJson("/" + index + "/_search", SORTED_PAGE));
        Map<String, Object> statsAfter = fetchStats();
        assertEquals(sorted, total, extractIntPath(sorted, "hits", "total", "value"));
        assertEquals(sorted, "eq", stringPath(sorted, "hits", "total", "relation"));
        assertEquals(sorted, live, sourceIds(sorted));
        Map<String, Object> sortedFetch = profileFetch(sorted);
        assertEquals("one take per fragment with a live row: " + sorted, (long) fragmentsWithRows, number(sortedFetch.get("take_count")));
        assertEquals("the takes addressed the live rows: " + sorted, (long) total, number(sortedFetch.get("take_rows")));
        assertEquals(
            "the node counters moved by the same takes: " + statsAfter,
            (long) fragmentsWithRows,
            number(statsAfter.get("take_count")) - number(statsBefore.get("take_count"))
        );
        assertEquals((long) total, number(statsAfter.get("take_rows")) - number(statsBefore.get("take_rows")));

        // The collector page: the same rows, now behind the hits in the
        // node's fetch cache, so it takes nothing and every row is served
        // from the cache. The live docs of the leaf still decide which
        // rows are on the page, so a deleted row cannot come back from
        // the cache.
        Map<String, Object> cacheBefore = fetchCacheStats();
        String collected = readAll(postJson("/" + index + "/_search", COLLECTOR_PAGE));
        Map<String, Object> cacheAfter = fetchCacheStats();
        assertEquals(collected, total, extractIntPath(collected, "hits", "total", "value"));
        assertEquals(collected, live, sourceIds(collected));
        Map<String, Object> collectedFetch = profileFetch(collected);
        assertEquals("the collector page took nothing: " + collected, 0L, number(collectedFetch.get("take_count")));
        assertEquals(collected, 0L, number(collectedFetch.get("take_rows")));
        assertEquals(
            "the fetch cache served every live row: " + cacheAfter,
            (long) total,
            number(cacheAfter.get("rows_served")) - number(cacheBefore.get("rows_served"))
        );

        // _count and a value_count over id agree with the page.
        String count = readAll(client().performRequest(new Request("GET", "/" + index + "/_count")));
        assertEquals(count, total, extractIntPath(count, "count"));
        String valueCount = readAll(
            postJson(
                "/" + index + "/_search",
                "{\"size\":0,\"track_total_hits\":true,\"aggs\":{\"n\":{\"value_count\":{\"field\":\"id\"}}}}"
            )
        );
        assertEquals(valueCount, total, extractIntPath(valueCount, "hits", "total", "value"));
        assertEquals(valueCount, total, extractIntPath(valueCount, "aggregations", "n", "value"));

        // A terms aggregation opens one bucket per live row and none for
        // a deleted one.
        String terms = readAll(
            postJson(
                "/" + index + "/_search",
                "{\"size\":0,\"aggs\":{\"t\":{\"terms\":{\"field\":\"title.raw\",\"size\":20,\"order\":{\"_key\":\"asc\"}}}}}"
            )
        );
        List<String> buckets = bucketsOf(terms, "t");
        assertEquals(terms, total, buckets.size());
        int docCount = 0;
        for (String bucket : buckets) {
            docCount += Integer.parseInt(bucket.substring(bucket.lastIndexOf('=') + 1));
        }
        assertEquals("the buckets count the live rows: " + terms, total, docCount);
        assertEquals(terms, 0, extractIntPath(terms, "aggregations", "t", "doc_count_error_upper_bound"));
        assertEquals(terms, 0, extractIntPath(terms, "aggregations", "t", "sum_other_doc_count"));
        for (int id : live) {
            assertTrue("a bucket for the live row " + id + ": " + terms, buckets.contains(title(id) + "=1"));
        }
        for (int id : deleted) {
            assertFalse("no bucket for the deleted row " + id + ": " + terms, terms.contains("\"" + title(id) + "\""));
        }

        // A term query on a deleted id matches nothing; on a live id, the row.
        for (int id : deleted) {
            String gone = readAll(postJson("/" + index + "/_search", "{\"query\":{\"term\":{\"id\":" + id + "}}}"));
            assertEquals("the deleted row " + id + " is not found: " + gone, 0, extractIntPath(gone, "hits", "total", "value"));
        }
        String kept = readAll(postJson("/" + index + "/_search", "{\"query\":{\"term\":{\"id\":" + live.get(0) + "}}}"));
        assertEquals(kept, 1, extractIntPath(kept, "hits", "total", "value"));
        assertEquals(kept, List.of(live.get(0)), sourceIds(kept));
    }

    /** {@code title} of row {@code i}, see {@link LanceTableFactory#writeTable}. */
    private static String title(int i) {
        return (i % 2 == 0 ? "sunny morning " : "cloudy morning ") + i;
    }

    /** The {@code fragment_plan.kind} of {@code body}'s explain: which operator answers the page. */
    @SuppressWarnings("unchecked")
    private static String planKind(String index, String body) throws Exception {
        Request explain = new Request("GET", "/" + index + "/_lance/explain");
        explain.setJsonEntity(body);
        String explained = readAll(client().performRequest(explain));
        assertEquals(explained, "fragment", stringPath(explained, "route"));
        Map<String, Object> plan = (Map<String, Object>) parseJson(explained).get("fragment_plan");
        assertNotNull("the explain carries the fragment plan: " + explained, plan);
        return (String) plan.get("kind");
    }

    /** {@code _source.id} of every hit, in response order. */
    @SuppressWarnings("unchecked")
    private static List<Integer> sourceIds(String searchBody) throws Exception {
        List<Integer> ids = new ArrayList<>();
        for (Map<String, Object> hit : hitsOf(searchBody)) {
            Map<String, Object> source = (Map<String, Object>) hit.get("_source");
            assertNotNull("every hit carries its source: " + searchBody, source);
            ids.add(((Number) source.get("id")).intValue());
        }
        return ids;
    }

    /** The {@code fetch} section of the single data node's profile in {@code searchBody}. */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> profileFetch(String searchBody) {
        Map<String, Object> profile = (Map<String, Object>) parseJson(searchBody).get("profile");
        assertNotNull("the response carries a profile: " + searchBody, profile);
        Map<String, Object> nodes = (Map<String, Object>) ((Map<String, Object>) profile.get("lance")).get("nodes");
        assertEquals("single node cluster: " + searchBody, 1, nodes.size());
        Map<String, Object> node = (Map<String, Object>) nodes.values().iterator().next();
        Map<String, Object> fetch = (Map<String, Object>) node.get("fetch");
        assertNotNull("the node profile carries fetch: " + searchBody, fetch);
        return fetch;
    }

    /** The {@code fetch} counters of the single data node in {@code GET /_lance/stats}. */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> fetchStats() throws Exception {
        return statsBlock("fetch");
    }

    private static Map<String, Object> fetchCacheStats() throws Exception {
        return statsBlock("fetch_cache");
    }

    private static Map<String, Object> statsBlock(String name) throws Exception {
        Map<String, Object> stats = parseJson(readAll(client().performRequest(new Request("GET", "/_lance/stats"))));
        Map<String, Object> nodes = (Map<String, Object>) stats.get("nodes");
        assertEquals("single node cluster: " + stats, 1, nodes.size());
        Map<String, Object> node = (Map<String, Object>) nodes.values().iterator().next();
        Map<String, Object> block = (Map<String, Object>) node.get(name);
        assertNotNull("the node stats carry " + name + ": " + stats, block);
        return block;
    }

    private static long number(Object value) {
        assertNotNull(value);
        return ((Number) value).longValue();
    }
}
