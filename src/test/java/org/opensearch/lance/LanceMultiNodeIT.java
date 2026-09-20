/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.rest.OpenSearchRestTestCase;

/**
 * Focused subset of the integTest suite that exercises the paths
 * where multi-node behaviour differs from single-node:
 *
 * <ul>
 *   <li>Coordinator fan-out over multiple data nodes for
 *       fragment-mode search.</li>
 *   <li>Namespace metadata propagation through cluster state.</li>
 *   <li>Manager-routed register / unregister through the transport
 *       action.</li>
 * </ul>
 *
 * <p>The rest of the integTest suite runs against a single-node
 * cluster; only tests that depend on node topology live here, so the
 * three-node cluster does not slow down every run.
 *
 * <p>Filtered in and out through Gradle {@code filter} blocks: the
 * default {@code integTest} task excludes this class, and
 * {@code multiNodeIntegTest} includes only it. Both point at the
 * same test source set to avoid duplicating helper code.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceMultiNodeIT extends OpenSearchRestTestCase {

    private static Path sharedRoot() throws IOException {
        String property = System.getProperty("tests.lance.shared_tables_dir");
        assertNotNull("tests.lance.shared_tables_dir must be set by the Gradle build", property);
        Path base = Path.of(property);
        Files.createDirectories(base);
        return base;
    }

    public void testFragmentDispatchAcrossThreeNodes() throws Exception {
        // The coordinator's fan-out must return the full count and hits
        // when the cluster has three data nodes.
        String suffix = "mn-dispatch-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            String body = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match_all\":{}}}"));
            assertEquals(6, extractIntPath(body, "hits", "total", "value"));
            // Every hit is materialised from Lance regardless of
            // which node scanned the fragment.
            assertTrue("expected _rowaddr-derived hit ids: " + body, body.contains("\"_id\":\"0-0\""));
            assertTrue("expected _source rendered from Arrow: " + body, body.contains("hello lance"));

            String sumBody = readAll(postJson("/" + indexName + "/_search", "{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}"));
            assertEquals(6, extractIntPath(sumBody, "hits", "total", "value"));
            assertEquals(15.0d, extractDoublePath(sumBody, "aggregations", "s", "value"), 0.0d);
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFtsAcrossFragmentsOnThreeNodeCluster() throws Exception {
        // 12 rows written 4 per file give fragments 0, 1 and 2. The
        // coordinator pins the fan-out to the node hosting the primary
        // shard (see TransportLanceCoordinatorAction.nodeListForTarget),
        // so on the three-node cluster one executor still receives all
        // three fragments and its FTS scan runs without a fragmentIds
        // restriction, while the other two nodes forward the request.
        // The hits, their per-fragment _id layout and _count must match
        // what the single-node LanceFtsQueryIT asserts for the same
        // table.
        String suffix = "mn-fts-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeMultiFragmentTable(scratchDir, tableName, 12, 4);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            assertEquals(3, extractIntPath(readAll(attach), "fragments"));

            String helloBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}")
            );
            assertEquals("even rows across three fragments: " + helloBody, 6, extractIntPath(helloBody, "hits", "total", "value"));
            assertEquals(
                "row i lives at fragment i / 4, offset i % 4",
                Set.of("0-0", "0-2", "1-0", "1-2", "2-0", "2-2"),
                new HashSet<>(hitIds(helloBody))
            );

            String singleBody = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"4\"}}}")
            );
            assertEquals("token 4 appears in one row only: " + singleBody, 1, extractIntPath(singleBody, "hits", "total", "value"));
            assertEquals(List.of("1-0"), hitIds(singleBody));

            String countBody = readAll(
                postJson("/" + indexName + "/_count", "{\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}")
            );
            assertEquals("_count for hello: " + countBody, 6, extractIntPath(countBody, "count"));
            String singleCountBody = readAll(
                postJson("/" + indexName + "/_count", "{\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"4\"}}}")
            );
            assertEquals("_count for token 4: " + singleCountBody, 1, extractIntPath(singleCountBody, "count"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    /**
     * With a shard copy on every data node the coordinator hands each
     * fragment to a different node and has to merge three sorted (or
     * scored) lists. The same requests are run first against the
     * single-copy index (one node answers, no merge) and then after
     * {@code auto_expand_replicas: 0-all} put a copy on all three
     * nodes; ids, sort values, totals and buckets must not change.
     * The fixture interleaves ids across fragments so a merge that
     * only concatenated per-node lists would reorder every page.
     */
    public void testFanOutAcrossReplicaHostsMatchesSingleHost() throws Exception {
        String suffix = "mn-merge-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        int fragments = 3;
        int rowsPerFragment = 4;
        LanceTableFactory.writeInterleavedTable(scratchDir, tableName, fragments, rowsPerFragment);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        String[] requests = new String[] {
            "{\"query\":{\"match_all\":{}},\"sort\":[{\"id\":\"asc\"}],\"size\":10}",
            "{\"sort\":[{\"ts\":\"desc\"}],\"size\":5}",
            "{\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"lance\"}},\"size\":12}",
            "{\"from\":2,\"size\":3,\"sort\":[{\"id\":\"asc\"}]}",
            "{\"size\":0,\"aggs\":{\"by_category\":{\"terms\":{\"field\":\"category\",\"size\":10}}}}" };
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            client().performRequest(new Request("GET", "/_cluster/health/" + indexName + "?wait_for_status=green&timeout=60s"));
            assertEquals(1, activeShards(indexName));

            List<Map<String, Object>> single = new ArrayList<>();
            for (String request : requests) {
                single.add(parse(readAll(postJson("/" + indexName + "/_search", request))));
            }
            // Sanity on the single-host baseline before comparing.
            assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), sourceIds(single.get(0)));
            assertEquals(List.of(11, 10, 9, 8, 7), sourceIds(single.get(1)));
            assertEquals(12, sourceIds(single.get(2)).size());
            assertEquals(List.of(2, 3, 4), sourceIds(single.get(3)));
            assertEquals(3, buckets(single.get(4)).size());
            for (Map<String, Object> response : single) {
                assertEquals(fragments * rowsPerFragment, extractIntPath(response, "hits", "total", "value"));
            }

            Request expand = new Request("PUT", "/" + indexName + "/_settings");
            expand.setJsonEntity("{\"index.auto_expand_replicas\":\"0-all\"}");
            assertEquals(RestStatus.OK.getStatus(), client().performRequest(expand).getStatusLine().getStatusCode());
            client().performRequest(
                new Request("GET", "/_cluster/health/" + indexName + "?wait_for_status=green&wait_for_active_shards=3&timeout=60s")
            );
            assertEquals(3, activeShards(indexName));

            for (int i = 0; i < requests.length; i++) {
                Map<String, Object> multi = parse(readAll(postJson("/" + indexName + "/_search", requests[i])));
                Map<String, Object> expected = single.get(i);
                String label = "request " + requests[i];
                assertEquals(label, sourceIds(expected), sourceIds(multi));
                assertEquals(label, sortValues(expected), sortValues(multi));
                assertEquals(label, extractIntPath(expected, "hits", "total", "value"), extractIntPath(multi, "hits", "total", "value"));
                assertEquals(label, expected.get("aggregations"), multi.get("aggregations"));
            }
            // The scored request must come back in strictly descending
            // score order after the merge; every row has a distinct
            // term frequency so no two scores tie.
            List<Double> scores = scores(parse(readAll(postJson("/" + indexName + "/_search", requests[2]))));
            for (int i = 1; i < scores.size(); i++) {
                assertTrue("scores not descending: " + scores, scores.get(i - 1) > scores.get(i));
            }
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testNamespaceRegisterPropagatesToAllNodes() throws Exception {
        // GET reads cluster state on the responding node, so the entry
        // only appears if the registration propagated.
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-mn-register-" + randomAlphaOfLength(8)));
        String path = scratchDir.toString();
        try {
            Response register = postJson("/_lance/namespace", "{\"path\":\"" + path + "\"}");
            assertEquals(RestStatus.OK.getStatus(), register.getStatusLine().getStatusCode());

            Response listing = client().performRequest(new Request("GET", "/_lance/namespace"));
            String body = readAll(listing);
            assertTrue("expected namespace " + path + " in listing: " + body, body.contains(path));

            Response unregister = deleteJson("/_lance/namespace", "{\"path\":\"" + path + "\"}");
            assertEquals(RestStatus.OK.getStatus(), unregister.getStatusLine().getStatusCode());
            String unregisterBody = readAll(unregister);
            assertTrue("expected unregistered:true, saw: " + unregisterBody, unregisterBody.contains("\"unregistered\":true"));

            Response after = client().performRequest(new Request("GET", "/_lance/namespace"));
            String afterBody = readAll(after);
            assertFalse("expected namespace " + path + " to be gone: " + afterBody, afterBody.contains(path));
        } finally {
            // Best-effort cleanup in case an earlier assertion left
            // the namespace registered.
            try {
                deleteJson("/_lance/namespace", "{\"path\":\"" + path + "\"}");
            } catch (Exception ignored) {}
        }
    }

    private static Response postJson(String path, String body) throws IOException {
        Request request = new Request("POST", path);
        request.setJsonEntity(body);
        return client().performRequest(request);
    }

    private static Response deleteJson(String path, String body) throws IOException {
        Request request = new Request("DELETE", path);
        request.setJsonEntity(body);
        return client().performRequest(request);
    }

    private static void updateClusterSetting(String key, String value) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"transient\":{\"" + key + "\":\"" + value + "\"}}");
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
    }

    private static String readAll(Response response) throws IOException {
        try (var stream = response.getEntity().getContent()) {
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @SuppressWarnings("unchecked")
    private static List<String> hitIds(String searchBody) throws IOException {
        try (XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, searchBody)) {
            Map<String, Object> map = parser.map();
            List<Object> hits = (List<Object>) ((Map<String, Object>) map.get("hits")).get("hits");
            List<String> ids = new ArrayList<>(hits.size());
            for (Object hit : hits) {
                ids.add((String) ((Map<String, Object>) hit).get("_id"));
            }
            return ids;
        }
    }

    private static Map<String, Object> parse(String json) {
        try (XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, json)) {
            return parser.map();
        } catch (IOException e) {
            throw new AssertionError("could not parse JSON: " + json, e);
        }
    }

    private static int activeShards(String indexName) throws IOException {
        String body = readAll(client().performRequest(new Request("GET", "/_cluster/health/" + indexName)));
        return extractIntPath(body, "active_shards");
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> hitList(Map<String, Object> response) {
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        return (List<Map<String, Object>>) hits.get("hits");
    }

    /** {@code _source.id} of every hit, in response order. */
    @SuppressWarnings("unchecked")
    private static List<Integer> sourceIds(Map<String, Object> response) {
        List<Integer> ids = new ArrayList<>();
        for (Map<String, Object> hit : hitList(response)) {
            Map<String, Object> source = (Map<String, Object>) hit.get("_source");
            ids.add(((Number) source.get("id")).intValue());
        }
        return ids;
    }

    /** The {@code sort} array of every hit, in response order; null entries for unsorted requests. */
    private static List<Object> sortValues(Map<String, Object> response) {
        List<Object> values = new ArrayList<>();
        for (Map<String, Object> hit : hitList(response)) {
            values.add(hit.get("sort"));
        }
        return values;
    }

    private static List<Double> scores(Map<String, Object> response) {
        List<Double> scores = new ArrayList<>();
        for (Map<String, Object> hit : hitList(response)) {
            scores.add(((Number) hit.get("_score")).doubleValue());
        }
        return scores;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> buckets(Map<String, Object> response) {
        Map<String, Object> aggregations = (Map<String, Object>) response.get("aggregations");
        Map<String, Object> byCategory = (Map<String, Object>) aggregations.get("by_category");
        return (List<Map<String, Object>>) byCategory.get("buckets");
    }

    private static int extractIntPath(Map<String, Object> parsed, String... path) {
        Object value = parsed;
        for (String step : path) {
            if (value instanceof Map<?, ?> map) {
                value = map.get(step);
            } else {
                throw new AssertionError("cannot descend into " + value + " with step " + step);
            }
            if (value == null) {
                throw new AssertionError("missing key " + step + " in path " + String.join(".", path) + ", json=" + parsed);
            }
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        throw new AssertionError("expected number at " + String.join(".", path) + ", saw " + value);
    }

    private static int extractIntPath(String json, String... path) {
        try (XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Object value = parser.map();
            for (String step : path) {
                if (value instanceof java.util.Map<?, ?> map) {
                    value = map.get(step);
                } else if (value instanceof java.util.List<?> list) {
                    value = list.get(Integer.parseInt(step));
                } else {
                    throw new AssertionError("cannot descend into " + value + " with step " + step);
                }
                if (value == null) {
                    throw new AssertionError("missing key " + step + " in path " + String.join(".", path) + ", json=" + json);
                }
            }
            if (value instanceof Number number) {
                return number.intValue();
            }
            throw new AssertionError("expected number at " + String.join(".", path) + ", saw " + value);
        } catch (IOException e) {
            throw new AssertionError("could not parse JSON: " + json, e);
        }
    }

    private static double extractDoublePath(String json, String... path) {
        try (XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Object value = parser.map();
            for (String step : path) {
                if (value instanceof java.util.Map<?, ?> map) {
                    value = map.get(step);
                } else if (value instanceof java.util.List<?> list) {
                    value = list.get(Integer.parseInt(step));
                } else {
                    throw new AssertionError("cannot descend into " + value + " with step " + step);
                }
                if (value == null) {
                    throw new AssertionError("missing key " + step + " in path " + String.join(".", path) + ", json=" + json);
                }
            }
            if (value instanceof Number number) {
                return number.doubleValue();
            }
            throw new AssertionError("expected number at " + String.join(".", path) + ", saw " + value);
        } catch (IOException e) {
            throw new AssertionError("could not parse JSON: " + json, e);
        }
    }
}
