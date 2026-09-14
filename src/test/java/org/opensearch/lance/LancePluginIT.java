/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

/**
 * Smoke integration test for the plugin. Spins up a full OpenSearch test
 * cluster with the plugin installed and drives it through REST. Runs under
 * {@code ./gradlew integTest}.
 *
 * <p>Split into two layers:
 * <ul>
 *   <li>Route-level checks (plugin installed, endpoints reachable,
 *       register / attach error paths) that need no Lance table on disk.</li>
 *   <li>End-to-end checks that write a real Lance table onto the local
 *       filesystem via {@link LanceTableFactory}, register its parent
 *       directory as a namespace, wait for the poll cycle to surface the
 *       table as an OpenSearch index, then exercise match / GET / knn.</li>
 * </ul>
 * The cluster is configured with {@code lance.namespace.poll_cadence=1s}
 * (see {@code build.gradle}) so the end-to-end tests do not wait ten
 * seconds each for surfacing.
 */
// Lance JNI spins up native worker threads that outlive a single test
// method. Randomized test framework flags those as leaks; suppress at the
// suite level to match the plugin's other Lance-touching tests.
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LancePluginIT extends OpenSearchRestTestCase {

    public void testPluginIsInstalled() throws IOException {
        // Ask the cluster for its plugin list and confirm the entry exists.
        // This catches classpath / metadata regressions before any Lance code
        // ever gets touched.
        Response response = client().performRequest(new Request("GET", "/_cat/plugins?format=json"));
        String body = readAll(response);
        assertTrue("expected opensearch-lance in _cat/plugins, saw: " + body, body.contains("opensearch-lance"));
    }

    public void testNamespaceEndpointIsRegistered() throws IOException {
        // GET /_lance/namespace lists registered namespaces. On a fresh
        // cluster it returns an empty list, but the important assertion is
        // that the route is wired up and returns a 200 instead of 404.
        Response response = client().performRequest(new Request("GET", "/_lance/namespace"));
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
        String body = readAll(response);
        assertTrue("expected namespaces JSON, saw: " + body, body.contains("namespaces"));
    }

    public void testRegisterNamespace() throws IOException {
        String path = scratchPathString("register");
        Response post = postJson("/_lance/namespace", "{\"path\":\"" + path + "\"}");
        assertEquals(RestStatus.OK.getStatus(), post.getStatusLine().getStatusCode());

        // The register call should surface the path back through GET.
        Response listing = client().performRequest(new Request("GET", "/_lance/namespace"));
        String body = readAll(listing);
        assertTrue("expected namespace " + path + " in listing, saw: " + body, body.contains(path));
    }

    public void testRegisterNamespaceIsIdempotent() throws IOException {
        // Registering the same path twice must not duplicate the entry, so a
        // second POST is a no-op. If the polling loop scanned the same
        // catalog twice per cycle every table would surface twice as well.
        String path = scratchPathString("idempotent");
        postJson("/_lance/namespace", "{\"path\":\"" + path + "\"}");
        postJson("/_lance/namespace", "{\"path\":\"" + path + "\"}");

        Response listing = client().performRequest(new Request("GET", "/_lance/namespace"));
        String body = readAll(listing);
        int occurrences = countOccurrences(body, path);
        assertEquals("registered path should appear exactly once, saw: " + body, 1, occurrences);
    }

    public void testAttachRejectsMissingTable() throws IOException {
        // POST /_lance/attach with a path that does not exist on disk lets
        // Dataset.open throw. The plugin must surface this as an HTTP error
        // rather than crash the request thread or leak an unhandled 500.
        String bogus = scratchPathString("missing") + ".lance";
        ResponseException failure = expectThrows(
            ResponseException.class,
            () -> postJson("/_lance/attach", "{\"table\":\"" + bogus + "\"}")
        );
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertTrue("expected 4xx / 5xx for missing table, saw: " + status, status >= 400);
    }

    public void testBuildIndexesOnUnknownIndexFails() throws IOException {
        // The manual build endpoint targets a specific OpenSearch index. When
        // the index does not exist the call must fail rather than silently
        // no-op, so operators using the recovery path see the mistake.
        String unknown = "does-not-exist-" + randomAlphaOfLength(8);
        ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/_lance/build_indexes/" + unknown, "{}"));
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertTrue("expected 4xx / 5xx for unknown index, saw: " + status, status >= 400);
    }

    public void testAttachAndMatch() throws Exception {
        // End-to-end: build a real Lance table, wait for the polling loop to
        // surface it as an OpenSearch index, then confirm a match query
        // routes into the plugin engine and returns the rows whose body
        // matches the search term.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "attachAndMatch")) {
            String indexName = fixture.indexName();

            Response search = postJson("/" + indexName + "/_search", "{\"query\":{\"match\":{\"body\":\"hello\"}}}");
            String body = readAll(search);
            // 16 rows total, even rows say "hello lance i", odd rows say
            // "quick brown fox i". Half the rows should match.
            int totalHits = extractIntPath(body, "hits", "total", "value");
            assertEquals("expected 8 hits (even rows), saw response: " + body, 8, totalHits);
        }
    }

    public void testAttachAndGetById() throws Exception {
        // GET /{index}/_doc/{id} must route through the Lance PK lookup and
        // materialize the row's fields into _source.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(8, "attachAndGetById")) {
            String indexName = fixture.indexName();

            Response get = client().performRequest(new Request("GET", "/" + indexName + "/_doc/3"));
            String body = readAll(get);
            assertTrue("expected _source in GET response, saw: " + body, body.contains("\"_source\""));
            assertTrue("expected body field with id 3, saw: " + body, body.contains("quick brown fox 3"));
        }
    }

    public void testAttachAndKnn() throws Exception {
        // Row i sits at coordinate (i, 0, 0, ...) so the nearest neighbour
        // of (2.4, 0, ...) is row 2 followed by row 3. Vector index build is
        // skipped (256-row floor); the plugin falls back to a brute-force
        // scan, which is fine for a 12-row table.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(12, "attachAndKnn")) {
            String indexName = fixture.indexName();

            String queryVector = "[2.4,0,0,0,0,0,0,0]";
            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"size\":2,\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":" + queryVector + ",\"k\":2}}}"
            );
            String body = readAll(search);
            int firstId = extractIntPath(body, "hits", "hits", "0", "_source", "id");
            int secondId = extractIntPath(body, "hits", "hits", "1", "_source", "id");
            assertEquals("expected row 2 as nearest, saw: " + body, 2, firstId);
            assertEquals("expected row 3 as second, saw: " + body, 3, secondId);
        }
    }

    public void testDefaultSearchReturnsAtLeastTenHits() throws Exception {
        // Regression for the FetchPhase sequential-stored-fields path: with
        // >= 10 adjacent doc ids and no deletions the fetch phase calls
        // getSequentialStoredFieldsReader on the leaf reader. Before the
        // LanceSequentialLeafReader wrapper this threw "requires a
        // CodecReader or a SequentialStoredFieldsLeafReader", so GET
        // /demo/_search with the default size=10 returned 500. Sixteen rows
        // exercises the >= 10 hits case; every hit must materialise cleanly.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "defaultsearch")) {
            String indexName = fixture.indexName();

            Response search = client().performRequest(new Request("GET", "/" + indexName + "/_search"));
            String body = readAll(search);
            int totalHits = extractIntPath(body, "hits", "total", "value");
            assertEquals("expected 16 total hits, saw response: " + body, 16, totalHits);
            // Default size is 10; hits array must be full and each entry
            // must carry the Lance-backed _source.
            assertTrue("expected hits[0]._source in response, saw: " + body, body.contains("\"_source\""));
        }
    }

    public void testStatsAPIsSucceedForLanceIndex() throws Exception {
        // Regression for LanceReadOnlyEngine.docStats() / segmentsStats():
        // OpenSearch's default implementations traverse leaves via
        // Lucene.segmentReader(reader), which throws for Lance leaves and
        // takes out _stats / _cat/indices docs.count / _nodes/stats /
        // _cluster/stats node-wide. Confirm the plugin overrides succeed
        // and the node-level stats aggregation runs without shard failures.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "statsapi")) {
            String indexName = fixture.indexName();

            Response indexStats = client().performRequest(new Request("GET", "/" + indexName + "/_stats"));
            assertEquals(RestStatus.OK.getStatus(), indexStats.getStatusLine().getStatusCode());
            String body = readAll(indexStats);
            int failed = extractIntPath(body, "_shards", "failed");
            assertEquals("expected zero shard failures on _stats, saw: " + body, 0, failed);

            Response segments = client().performRequest(new Request("GET", "/" + indexName + "/_segments"));
            assertEquals(RestStatus.OK.getStatus(), segments.getStatusLine().getStatusCode());

            Response nodesStats = client().performRequest(new Request("GET", "/_nodes/stats/indices/docs"));
            assertEquals(RestStatus.OK.getStatus(), nodesStats.getStatusLine().getStatusCode());
            String nodesBody = readAll(nodesStats);
            int nodesFailed = extractIntPath(nodesBody, "_nodes", "failed");
            assertEquals("expected zero node failures on _nodes/stats, saw: " + nodesBody, 0, nodesFailed);
        }
    }

    /**
     * Fixture that writes a Lance table into a scratch directory, registers
     * that directory as a Lance namespace, waits for the polling loop to
     * surface the table, and cleans everything up on close. The scratch
     * directory sits under {@code java.io.tmpdir} so the child test-cluster
     * process (which lives in a separate work directory) can still open it.
     */
    private static final class LanceTestCluster implements AutoCloseable {
        private final Path scratchDir;
        private final String indexName;

        private LanceTestCluster(Path scratchDir, String indexName) {
            this.scratchDir = scratchDir;
            this.indexName = indexName;
        }

        String indexName() {
            return indexName;
        }

        static LanceTestCluster setUp(int rowCount, String testHint) throws Exception {
            // Anchor the shared directory at the path build.gradle passes in
            // via tests.lance.shared_tables_dir. Test JVM and cluster JVM see
            // the same location without relying on a system-wide tmpdir.
            Path base = sharedRoot();
            // Lowercase everything. OpenSearch rejects index names that
            // contain any uppercase character, and the surfaced index name
            // is derived from the table directory name.
            String suffix = testHint.toLowerCase(java.util.Locale.ROOT) + "-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
            Path scratchDir = Files.createDirectories(base.resolve("lance-it-" + suffix));
            String tableName = "demo-" + suffix;
            LanceTableFactory.writeTable(scratchDir, tableName, rowCount);

            Response register = postJson("/_lance/namespace", "{\"path\":\"" + scratchDir.toString() + "\"}");
            assertEquals(
                "namespace register failed: " + readAll(register),
                RestStatus.OK.getStatus(),
                register.getStatusLine().getStatusCode()
            );

            // Poll cadence is 1s (see build.gradle); the assertBusy default
            // (10s) leaves plenty of headroom for the surface path to run.
            String indexName = "demo-" + suffix;
            assertBusy(() -> {
                Response cat = client().performRequest(new Request("GET", "/_cat/indices?format=json"));
                String body = readAll(cat);
                assertTrue("waiting for index " + indexName + ", saw: " + body, body.contains("\"" + indexName + "\""));
            });
            // Recovery still races the first request on newer OpenSearch
            // versions; block until the shard is green before returning so
            // GET / search do not see RECOVERING.
            ensureGreen(indexName);
            return new LanceTestCluster(scratchDir, indexName);
        }

        @Override
        public void close() throws IOException {
            // Best-effort cleanup so leftover scratch dirs do not pile up
            // when a test fails.
            deleteRecursively(scratchDir);
        }
    }

    private static Response postJson(String path, String body) throws IOException {
        Request request = new Request("POST", path);
        request.setJsonEntity(body);
        return client().performRequest(request);
    }

    private static Path sharedRoot() {
        String configured = System.getProperty("tests.lance.shared_tables_dir");
        if (configured == null || configured.isEmpty()) {
            throw new AssertionError("tests.lance.shared_tables_dir is unset; build.gradle should pass it into the integTest task");
        }
        return Path.of(configured);
    }

    private static String scratchPathString(String label) {
        return sharedRoot().resolve("lance-it-" + label + "-" + randomAlphaOfLength(8)).toString();
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0;
        int i = 0;
        while ((i = haystack.indexOf(needle, i)) != -1) {
            count++;
            i += needle.length();
        }
        return count;
    }

    private static String readAll(Response response) throws IOException {
        return new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
    }

    /**
     * Small helper to pluck a value out of a JSON response without pulling
     * in Jackson. Traverses object keys or array indices in order.
     */
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

    private static void deleteRecursively(Path root) throws IOException {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try (var stream = Files.walk(root)) {
            stream.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // best-effort cleanup
                }
            });
        }
    }
}
