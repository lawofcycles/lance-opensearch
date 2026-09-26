/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.util.List;
import java.util.Map;

import org.opensearch.client.Request;

/**
 * {@code profile: true} against the single node test cluster: the
 * response carries {@code profile.lance.nodes.<node id>} with the
 * executor's query and fetch timings and the take scans of the request,
 * a {@code size: 0} request issues no take, a page does, and an answer
 * from the result cache says so instead.
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
            assertTrue(number(pageFetch.get("take_millis")) >= 0L);
            assertTrue(number(pageFetch.get("millis")) >= 0L);
            assertTrue(number(section(pageNode, "query").get("millis")) >= 0L);
            assertNull("the fragment path reports no shard profile", ((Map<?, ?>) pageBody.get("profile")).get("shards"));

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
