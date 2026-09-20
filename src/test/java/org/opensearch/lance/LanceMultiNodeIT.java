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
