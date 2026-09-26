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
 * {@code profile: true} against the single node test cluster: the
 * response carries {@code profile.lance.nodes.<node id>} with the
 * executor's query and fetch timings and the take scans of the request,
 * a {@code size: 0} request issues no take, a page does, the columns the
 * takes project follow the body's {@code _source} and {@code fields},
 * a bounded full text page runs one Lance scan for its hits and its
 * count ({@code query.fts_scans}), and an answer from the result cache
 * says so instead.
 */
public class LanceProfileIT extends LanceRestTestCase {

    public void testProfileReportsTheExecutorsTimingsAndTakesPerNode() throws Exception {
        // Two fragments of 10,000 rows: the token sp7 matches 16 rows per
        // fragment, below the reader's sparse ratio, so a sort over its
        // hits takes the sort column for those rows.
        try (LanceTestCluster fixture = LanceTestCluster.setUpHintFixture(2, 10_000, "profile")) {
            String index = fixture.indexName();
            String nodeId = localNodeId();

            // A page of ten hits: the rows behind the hits are taken.
            String page = readAll(postJson("/" + index + "/_search", "{\"size\":10,\"profile\":true,\"query\":{\"match_all\":{}}}"));
            Map<String, Object> pageBody = parseJson(page);
            assertEquals(page, 10, ((List<?>) ((Map<?, ?>) pageBody.get("hits")).get("hits")).size());
            Map<String, Object> pageNode = lanceNode(pageBody, nodeId);
            Map<String, Object> pageFetch = section(pageNode, "fetch");
            assertTrue("the page took its rows: " + page, number(pageFetch.get("take_count")) >= 1L);
            assertEquals("the takes addressed the ten hits: " + page, 10L, number(pageFetch.get("take_rows")));
            // Without a _source element the take projects every surfaced
            // column: id, body, rating, category, tags and flag.
            assertEquals(
                "the takes projected the six surfaced columns: " + page,
                6L * number(pageFetch.get("take_count")),
                number(pageFetch.get("take_columns"))
            );
            assertTrue(number(pageFetch.get("take_millis")) >= 0L);
            assertTrue(number(pageFetch.get("millis")) >= 0L);
            assertTrue(number(section(pageNode, "query").get("millis")) >= 0L);
            assertNull("the fragment path reports no shard profile", ((Map<?, ?>) pageBody.get("profile")).get("shards"));

            // _source: false on a table without a declared primary key:
            // _id is synthesised from the row address, so the page issues
            // no take at all.
            String noSource = readAll(
                postJson("/" + index + "/_search", "{\"size\":10,\"profile\":true,\"_source\":false,\"query\":{\"match_all\":{}}}")
            );
            Map<String, Object> noSourceBody = parseJson(noSource);
            List<Map<String, Object>> noSourceHits = hitsOf(noSource);
            assertEquals(noSource, 10, noSourceHits.size());
            assertEquals("0-0", noSourceHits.get(0).get("_id"));
            assertNull("_source: false renders no source: " + noSource, noSourceHits.get(0).get("_source"));
            Map<String, Object> noSourceFetch = section(lanceNode(noSourceBody, nodeId), "fetch");
            assertEquals("no column to take, no take: " + noSource, 0L, number(noSourceFetch.get("take_count")));
            assertEquals(0L, number(noSourceFetch.get("take_columns")));

            // An includes list: the take projects the two kept columns
            // only, and _source carries them alone.
            String included = readAll(
                postJson(
                    "/" + index + "/_search",
                    "{\"size\":10,\"profile\":true,\"_source\":{\"includes\":[\"rating\",\"cat*\"]},\"query\":{\"match_all\":{}}}"
                )
            );
            List<Map<String, Object>> includedHits = hitsOf(included);
            assertEquals(included, 10, includedHits.size());
            @SuppressWarnings("unchecked")
            Map<String, Object> includedSource = (Map<String, Object>) includedHits.get(1).get("_source");
            assertEquals(included, List.of("rating", "category"), new ArrayList<>(includedSource.keySet()));
            Map<String, Object> includedFetch = section(lanceNode(parseJson(included), nodeId), "fetch");
            assertEquals(
                "the takes projected rating and category: " + included,
                2L * number(includedFetch.get("take_count")),
                number(includedFetch.get("take_columns"))
            );

            // fields with _source: false: the take projects the fields'
            // column alone, and the hit carries it under fields.
            String fields = readAll(
                postJson(
                    "/" + index + "/_search",
                    "{\"size\":10,\"profile\":true,\"_source\":false,\"fields\":[\"rating\"],\"query\":{\"match_all\":{}}}"
                )
            );
            List<Map<String, Object>> fieldsHits = hitsOf(fields);
            assertEquals(fields, 10, fieldsHits.size());
            assertNull(fields, fieldsHits.get(1).get("_source"));
            assertEquals(fields, Map.of("rating", List.of(37)), fieldsHits.get(1).get("fields"));
            Map<String, Object> fieldsFetch = section(lanceNode(parseJson(fields), nodeId), "fetch");
            assertEquals(
                "the takes projected the fields' column: " + fields,
                number(fieldsFetch.get("take_count")),
                number(fieldsFetch.get("take_columns"))
            );

            // A sort over the sparse full text hit set takes the sort
            // column for the 32 hits (one take per leaf) next to the
            // stored fields take of the page of five.
            String sorted = readAll(
                postJson(
                    "/" + index + "/_search",
                    "{\"size\":5,\"profile\":true,\"query\":{\"match\":{\"body\":\"sp7\"}},\"sort\":[{\"rating\":\"desc\"}],\"track_total_hits\":true}"
                )
            );
            assertEquals(sorted, 32, extractIntPath(sorted, "hits", "total", "value"));
            Map<String, Object> sortedFetch = section(lanceNode(parseJson(sorted), nodeId), "fetch");
            logger.info("profile of a sorted page over 32 sparse hits: {}", sorted);
            assertTrue("the sort column takes and the page take: " + sorted, number(sortedFetch.get("take_count")) >= 3L);
            assertEquals("the column takes addressed the hits, the page take the page", 32L + 5L, number(sortedFetch.get("take_rows")));

            // A size 0 aggregation renders no row and issues no take.
            String sum = readAll(
                postJson(
                    "/" + index + "/_search",
                    "{\"size\":0,\"profile\":true,\"track_total_hits\":true,\"query\":{\"match_all\":{}},"
                        + "\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}"
                )
            );
            assertEquals(20_000, extractIntPath(sum, "hits", "total", "value"));
            Map<String, Object> sumNode = lanceNode(parseJson(sum), nodeId);
            Map<String, Object> sumFetch = section(sumNode, "fetch");
            assertEquals("a size 0 aggregation takes nothing: " + sum, 0L, number(sumFetch.get("take_count")));
            assertEquals(0L, number(sumFetch.get("take_rows")));
            assertEquals(0L, number(sumFetch.get("take_columns")));
            assertEquals(0L, number(sumFetch.get("take_millis")));
            assertTrue(number(section(sumNode, "query").get("millis")) >= 0L);

            // The same body again is served from the result cache: no
            // executor ran, and the profile says so.
            String cached = readAll(
                postJson(
                    "/" + index + "/_search",
                    "{\"size\":0,\"profile\":true,\"track_total_hits\":true,\"query\":{\"match_all\":{}},"
                        + "\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}"
                )
            );
            assertEquals(20_000, extractIntPath(cached, "hits", "total", "value"));
            Map<String, Object> cachedLance = lance(parseJson(cached));
            assertEquals(cached, Boolean.TRUE, cachedLance.get("cached"));
            assertNull(cached, cachedLance.get("nodes"));

            // Without profile: true the response carries no profile.
            String plain = readAll(postJson("/" + index + "/_search", "{\"size\":10,\"query\":{\"match_all\":{}}}"));
            assertNull(plain, parseJson(plain).get("profile"));
        }
    }

    public void testTakeProjectsThePrimaryKeyOnATableWithADeclaredKey() throws Exception {
        // The struct table: id is the declared primary key, meta a struct
        // of region, score and flags, so the full take is two columns.
        // The three requests read the same six rows, so the fetch cache
        // is turned off for the test: with it on, the second and third
        // requests would find their cells and take nothing.
        String suffix = "profilepk-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeStructTable(scratchDir, tableName, 0);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String index = tableName;
        try {
            Request off = new Request("PUT", "/_cluster/settings");
            off.setJsonEntity("{\"transient\":{\"lance.fetch_cache.enabled\":false}}");
            client().performRequest(off);
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(readAll(attach), RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            String nodeId = localNodeId();

            // Every column by default: id and meta.
            String full = readAll(postJson("/" + index + "/_search", "{\"size\":6,\"profile\":true,\"sort\":[{\"id\":\"asc\"}]}"));
            Map<String, Object> fullFetch = section(lanceNode(parseJson(full), nodeId), "fetch");
            assertEquals(full, 1L, number(fullFetch.get("take_count")));
            assertEquals(full, 6L, number(fullFetch.get("take_rows")));
            assertEquals("id and meta: " + full, 2L, number(fullFetch.get("take_columns")));
            assertEquals(List.of("0", "1", "2", "3", "4", "5"), idsOf(hitsOf(full)));

            // _source: false: the key alone, and _id still comes from it.
            String noSource = readAll(
                postJson("/" + index + "/_search", "{\"size\":6,\"profile\":true,\"_source\":false,\"sort\":[{\"id\":\"asc\"}]}")
            );
            Map<String, Object> noSourceFetch = section(lanceNode(parseJson(noSource), nodeId), "fetch");
            assertEquals(noSource, 1L, number(noSourceFetch.get("take_count")));
            assertEquals("the key only: " + noSource, 1L, number(noSourceFetch.get("take_columns")));
            assertEquals(List.of("0", "1", "2", "3", "4", "5"), idsOf(hitsOf(noSource)));
            assertNull(noSource, hitsOf(noSource).get(0).get("_source"));

            // A child include takes the parent struct and the key; the
            // fetch phase keeps the child alone in _source.
            String child = readAll(
                postJson(
                    "/" + index + "/_search",
                    "{\"size\":6,\"profile\":true,\"_source\":{\"includes\":[\"meta.region\"]},\"sort\":[{\"id\":\"asc\"}]}"
                )
            );
            Map<String, Object> childFetch = section(lanceNode(parseJson(child), nodeId), "fetch");
            assertEquals(child, 1L, number(childFetch.get("take_count")));
            assertEquals("meta and the key: " + child, 2L, number(childFetch.get("take_columns")));
            assertEquals(child, Map.of("meta", Map.of("region", "east")), hitsOf(child).get(0).get("_source"));
        } finally {
            try {
                Request on = new Request("PUT", "/_cluster/settings");
                on.setJsonEntity("{\"transient\":{\"lance.fetch_cache.enabled\":null}}");
                client().performRequest(on);
            } catch (Exception ignored) {}
            try {
                client().performRequest(new Request("DELETE", "/" + index));
            } catch (Exception ignored) {}
        }
    }

    public void testABoundedFullTextPageRunsOneLanceScanForThePageAndTheCount() throws Exception {
        // Two fragments of 10,000 rows on one node: hello matches every
        // row (20,000, above the default bound of 10,000), sp7 matches
        // 32 rows (below it). The executor holds every fragment, so its
        // hits scan is one whole table scan whose limit is the page
        // widened to bound + 1 rows; the rows it returned settle the
        // count, and no count only scan follows.
        try (LanceTestCluster fixture = LanceTestCluster.setUpHintFixture(2, 10_000, "profilefts")) {
            String index = fixture.indexName();
            String nodeId = localNodeId();
            String hello = "\"query\":{\"match\":{\"body\":\"hello\"}}";
            String sp7 = "\"query\":{\"match\":{\"body\":\"sp7\"}}";

            // The default bound over a match set above it: the page is
            // the top ten of the widened scan, the count is the bound
            // with gte, and the executor ran one full text scan.
            String bounded = readAll(postJson("/" + index + "/_search", "{\"size\":10,\"profile\":true," + hello + "}"));
            assertEquals(bounded, 10, hitsOf(bounded).size());
            assertEquals(bounded, 10_000, extractIntPath(bounded, "hits", "total", "value"));
            assertEquals(bounded, "gte", totalRelation(bounded));
            assertEquals("one scan served the page and the count: " + bounded, 1L, ftsScans(bounded, nodeId));

            // An explicit bound below the match count: the same, at that bound.
            String narrow = readAll(
                postJson("/" + index + "/_search", "{\"size\":10,\"profile\":true,\"track_total_hits\":20," + hello + "}")
            );
            assertEquals(narrow, 10, hitsOf(narrow).size());
            assertEquals(narrow, 20, extractIntPath(narrow, "hits", "total", "value"));
            assertEquals(narrow, "gte", totalRelation(narrow));
            assertEquals("one scan under an explicit bound: " + narrow, 1L, ftsScans(narrow, nodeId));

            // The default bound over a match set below it: the widened
            // scan comes back short of its limit, so the count is exact
            // from the same scan.
            String below = readAll(postJson("/" + index + "/_search", "{\"size\":10,\"profile\":true," + sp7 + "}"));
            assertEquals(below, 10, hitsOf(below).size());
            assertEquals(below, 32, extractIntPath(below, "hits", "total", "value"));
            assertEquals(below, "eq", totalRelation(below));
            assertEquals("one scan, exact count from it: " + below, 1L, ftsScans(below, nodeId));

            // track_total_hits: true keeps the exact count path: the page
            // scan of ten rows and a count only scan of the whole match set.
            String exact = readAll(
                postJson("/" + index + "/_search", "{\"size\":10,\"profile\":true,\"track_total_hits\":true," + hello + "}")
            );
            assertEquals(exact, 10, hitsOf(exact).size());
            assertEquals(exact, 20_000, extractIntPath(exact, "hits", "total", "value"));
            assertEquals(exact, "eq", totalRelation(exact));
            assertEquals("the page scan and the count only scan: " + exact, 2L, ftsScans(exact, nodeId));

            // track_total_hits: false: the page scan alone, no hits.total.
            String disabled = readAll(
                postJson("/" + index + "/_search", "{\"size\":10,\"profile\":true,\"track_total_hits\":false," + hello + "}")
            );
            assertEquals(disabled, 10, hitsOf(disabled).size());
            assertFalse("no hits.total: " + disabled, disabled.contains("\"total\":{"));
            assertEquals("the page scan alone: " + disabled, 1L, ftsScans(disabled, nodeId));

            // size 0 under the default bound: no page scan, so the count
            // only scan stopped at bound + 1 rows is the one scan.
            String countOnly = readAll(postJson("/" + index + "/_search", "{\"size\":0,\"profile\":true," + hello + "}"));
            assertEquals(countOnly, 10_000, extractIntPath(countOnly, "hits", "total", "value"));
            assertEquals(countOnly, "gte", totalRelation(countOnly));
            assertEquals("the count only scan alone: " + countOnly, 1L, ftsScans(countOnly, nodeId));
        }
    }

    /** {@code profile.lance.nodes.<nodeId>.query.fts_scans} of {@code body}. */
    private static long ftsScans(String body, String nodeId) throws Exception {
        return number(section(lanceNode(parseJson(body), nodeId), "query").get("fts_scans"));
    }

    @SuppressWarnings("unchecked")
    private static String totalRelation(String body) throws Exception {
        Map<String, Object> total = (Map<String, Object>) ((Map<String, Object>) parseJson(body).get("hits")).get("total");
        assertNotNull("hits.total is present: " + body, total);
        return (String) total.get("relation");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> lance(Map<String, Object> body) {
        Map<String, Object> profile = (Map<String, Object>) body.get("profile");
        assertNotNull("the response carries a profile: " + body, profile);
        Map<String, Object> lance = (Map<String, Object>) profile.get("lance");
        assertNotNull("the profile carries the lance object: " + body, lance);
        return lance;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> lanceNode(Map<String, Object> body, String nodeId) {
        Map<String, Object> nodes = (Map<String, Object>) lance(body).get("nodes");
        assertNotNull("the profile carries the nodes: " + body, nodes);
        assertEquals("single node cluster: " + nodes, 1, nodes.size());
        Map<String, Object> node = (Map<String, Object>) nodes.get(nodeId);
        assertNotNull("the profile names the data node " + nodeId + ": " + nodes, node);
        return node;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> section(Map<String, Object> node, String name) {
        Map<String, Object> section = (Map<String, Object>) node.get(name);
        assertNotNull("the node profile carries " + name + ": " + node, section);
        return section;
    }

    private static long number(Object value) {
        assertNotNull(value);
        return ((Number) value).longValue();
    }

    @SuppressWarnings("unchecked")
    private static String localNodeId() throws Exception {
        Map<String, Object> nodes = (Map<String, Object>) parseJson(readAll(client().performRequest(new Request("GET", "/_nodes")))).get(
            "nodes"
        );
        assertEquals("single node cluster", 1, nodes.size());
        return nodes.keySet().iterator().next();
    }
}
