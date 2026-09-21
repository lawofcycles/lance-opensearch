/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.core5.http.HttpHost;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.ResponseListener;
import org.opensearch.client.RestClient;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

/**
 * Shared harness for the plugin's REST integration tests. Provides the
 * {@link LanceTestCluster} fixture (writes a Lance table, registers its
 * directory as a namespace, waits for the polling loop to surface it as an
 * index) and small JSON / request helpers. The cluster is configured with
 * {@code lance.namespace.poll_cadence=1s} (see {@code build.gradle}) so
 * surfacing does not add ten seconds per test.
 */
// Lance JNI spins up native worker threads that outlive a single test
// method. Randomized test framework flags those as leaks; suppress at the
// suite level to match the plugin's other Lance-touching tests.
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public abstract class LanceRestTestCase extends OpenSearchRestTestCase {

    /**
     * Fixture that writes a Lance table into a scratch directory, registers
     * that directory as a Lance namespace, waits for the polling loop to
     * surface the table, and cleans everything up on close. The scratch
     * directory sits under the path {@code build.gradle} passes in as
     * {@code tests.lance.shared_tables_dir} so the test JVM and the cluster
     * JVM see the same location.
     */
    static final class LanceTestCluster implements AutoCloseable {
        private final Path scratchDir;
        private final String indexName;

        private LanceTestCluster(Path scratchDir, String indexName) {
            this.scratchDir = scratchDir;
            this.indexName = indexName;
        }

        String indexName() {
            return indexName;
        }

        /**
         * Absolute filesystem URI of the underlying Lance table. Tests that
         * need to mutate the table (drop columns, append rows) can hand
         * this to {@link LanceTableFactory}.
         */
        String tableUri() {
            return scratchDir.resolve("demo-" + scratchDir.getFileName().toString().substring("lance-it-".length()) + ".lance").toString();
        }

        static LanceTestCluster setUp(int rowCount, String testHint) throws Exception {
            Path base = sharedRoot();
            // Index names are derived from the directory name and must be
            // lowercase.
            String suffix = testHint.toLowerCase(java.util.Locale.ROOT) + "-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
            Path scratchDir = Files.createDirectories(base.resolve("lance-it-" + suffix));
            String tableName = "demo-" + suffix;
            LanceTableFactory.writeTable(scratchDir, tableName, rowCount);
            return registerAndWait(scratchDir, "demo-" + suffix);
        }

        static LanceTestCluster setUpNullable(String testHint) throws Exception {
            Path base = sharedRoot();
            String suffix = testHint.toLowerCase(java.util.Locale.ROOT) + "-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
            Path scratchDir = Files.createDirectories(base.resolve("lance-it-" + suffix));
            String tableName = "demo-" + suffix;
            LanceTableFactory.writeNullableTable(scratchDir, tableName);
            return registerAndWait(scratchDir, "demo-" + suffix);
        }

        /**
         * Same rows and indexes as {@link #setUp(int, String)} but written
         * with {@code maxRowsPerFile} rows per fragment, so the surfaced
         * index has several Lance fragments (see
         * {@link LanceTableFactory#writeMultiFragmentTable}).
         */
        static LanceTestCluster setUpMultiFragment(int rowCount, int maxRowsPerFile, String testHint) throws Exception {
            Path base = sharedRoot();
            String suffix = testHint.toLowerCase(Locale.ROOT) + "-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
            Path scratchDir = Files.createDirectories(base.resolve("lance-it-" + suffix));
            String tableName = "demo-" + suffix;
            LanceTableFactory.writeMultiFragmentTable(scratchDir, tableName, rowCount, maxRowsPerFile);
            return registerAndWait(scratchDir, "demo-" + suffix);
        }

        /**
         * The doc value fixture of
         * {@link LanceTableFactory#writeHintFixtureTable}: {@code fragments}
         * contiguous fragments of {@code rowsPerFragment} rows with an FTS
         * body, numeric, keyword, multi-valued keyword, boolean and vector
         * columns. Row {@code i} lives at fragment {@code i / rowsPerFragment},
         * offset {@code i % rowsPerFragment}, so its {@code _id} is
         * {@code (i / rowsPerFragment) + "-" + (i % rowsPerFragment)}.
         */
        static LanceTestCluster setUpHintFixture(int fragments, int rowsPerFragment, String testHint) throws Exception {
            Path base = sharedRoot();
            String suffix = testHint.toLowerCase(Locale.ROOT) + "-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
            Path scratchDir = Files.createDirectories(base.resolve("lance-it-" + suffix));
            String tableName = "demo-" + suffix;
            LanceTableFactory.writeHintFixtureTable(scratchDir, tableName, fragments, rowsPerFragment);
            return registerAndWait(scratchDir, "demo-" + suffix);
        }

        private static LanceTestCluster registerAndWait(Path scratchDir, String indexName) throws Exception {
            Response register = postJson("/_lance/namespace", "{\"path\":\"" + scratchDir.toString() + "\"}");
            assertEquals(
                "namespace register failed: " + readAll(register),
                RestStatus.OK.getStatus(),
                register.getStatusLine().getStatusCode()
            );

            // Poll cadence is 1s (see build.gradle); the assertBusy default
            // (10s) leaves plenty of headroom for the surface path to run.
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

    static Response postJson(String path, String body) throws IOException {
        Request request = new Request("POST", path);
        request.setJsonEntity(body);
        return client().performRequest(request);
    }

    static Response deleteJson(String path, String body) throws IOException {
        Request request = new Request("DELETE", path);
        request.setJsonEntity(body);
        return client().performRequest(request);
    }

    static Path sharedRoot() {
        String configured = System.getProperty("tests.lance.shared_tables_dir");
        if (configured == null || configured.isEmpty()) {
            throw new AssertionError("tests.lance.shared_tables_dir is unset; build.gradle should pass it into the integTest task");
        }
        return Path.of(configured);
    }

    static String scratchPathString(String label) {
        return sharedRoot().resolve("lance-it-" + label + "-" + randomAlphaOfLength(8)).toString();
    }

    /**
     * Directory the test clusters accept as an {@code fs} snapshot
     * repository location. {@code build.gradle} sets {@code path.repo} to
     * the sibling of the shared tables directory, so the same path is
     * derived here from {@code tests.lance.shared_tables_dir}.
     */
    static Path repoRoot() {
        return sharedRoot().resolveSibling("shared-repo");
    }

    static int countOccurrences(String haystack, String needle) {
        int count = 0;
        int i = 0;
        while ((i = haystack.indexOf(needle, i)) != -1) {
            count++;
            i += needle.length();
        }
        return count;
    }

    static String readAll(Response response) throws IOException {
        return new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
    }

    /**
     * Small helper to pluck a value out of a JSON response without pulling
     * in Jackson. Traverses object keys or array indices in order.
     */
    static int extractIntPath(String json, String... path) {
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

    /** The string at {@code path} in {@code json}; fails when the path is missing or not a string. */
    static String stringPath(String json, String... path) {
        Object value = parseJson(json);
        for (String step : path) {
            if (value instanceof Map<?, ?> map) {
                value = map.get(step);
            } else if (value instanceof List<?> list) {
                value = list.get(Integer.parseInt(step));
            } else {
                throw new AssertionError("cannot descend into " + value + " with step " + step);
            }
            if (value == null) {
                throw new AssertionError("missing key " + step + " in path " + String.join(".", path) + ", json=" + json);
            }
        }
        if (value instanceof String string) {
            return string;
        }
        throw new AssertionError("expected string at " + String.join(".", path) + ", saw " + value);
    }

    /** {@code json} as the map {@code XContentParser.map()} produces. */
    static Map<String, Object> parseJson(String json) {
        try (XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, json)) {
            return parser.map();
        } catch (IOException e) {
            throw new AssertionError("could not parse JSON: " + json, e);
        }
    }

    static double extractDoublePath(String json, String... path) {
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

    static void deleteRecursively(Path root) throws IOException {
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

    @SuppressWarnings("unchecked")
    static java.util.List<java.util.Map<String, Object>> hitsOf(String searchBody) throws IOException {
        try (XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, searchBody)) {
            java.util.Map<String, Object> map = parser.map();
            java.util.List<Object> hits = (java.util.List<Object>) ((java.util.Map<String, Object>) map.get("hits")).get("hits");
            java.util.List<java.util.Map<String, Object>> out = new java.util.ArrayList<>(hits.size());
            for (Object hit : hits) {
                java.util.Map<String, Object> copy = new java.util.LinkedHashMap<>((java.util.Map<String, Object>) hit);
                // _score is NaN on both paths and serialises as null;
                // drop it so the comparison is about order, ids, sort
                // values and _source only.
                copy.remove("_score");
                out.add(copy);
            }
            return out;
        }
    }

    static java.util.List<String> idsOf(java.util.List<java.util.Map<String, Object>> hits) {
        java.util.List<String> ids = new java.util.ArrayList<>(hits.size());
        for (java.util.Map<String, Object> hit : hits) {
            ids.add((String) hit.get("_id"));
        }
        return ids;
    }

    @SuppressWarnings("unchecked")
    static java.util.List<Object> sortValuesOf(java.util.Map<String, Object> hit) {
        return (java.util.List<Object>) hit.get("sort");
    }

    /**
     * {@code (key, doc_count)} pairs of the buckets of the named
     * aggregation, in response order, as {@code "key=count"} strings so
     * two responses can be compared with a plain list equality.
     */
    @SuppressWarnings("unchecked")
    static List<String> bucketsOf(String searchBody, String aggregationName) throws IOException {
        try (XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, searchBody)) {
            Map<String, Object> map = parser.map();
            Map<String, Object> aggregations = (Map<String, Object>) map.get("aggregations");
            assertNotNull("no aggregations in " + searchBody, aggregations);
            Map<String, Object> aggregation = (Map<String, Object>) aggregations.get(aggregationName);
            assertNotNull("no aggregation " + aggregationName + " in " + searchBody, aggregation);
            List<Map<String, Object>> buckets = (List<Map<String, Object>>) aggregation.get("buckets");
            List<String> out = new ArrayList<>(buckets.size());
            for (Map<String, Object> bucket : buckets) {
                out.add(bucket.get("key") + "=" + bucket.get("doc_count"));
            }
            return out;
        }
    }

    /** {@code _id} followed by the {@code sort} array of every hit, in response order. */
    static List<String> idsAndSortValuesOf(String searchBody) throws IOException {
        List<String> out = new ArrayList<>();
        for (Map<String, Object> hit : hitsOf(searchBody)) {
            out.add(hit.get("_id") + " " + sortValuesOf(hit));
        }
        return out;
    }

    /**
     * A REST client over {@code hosts} that keeps up to
     * {@code connections} connections open at once, so a test can have
     * that many requests in flight. The suite's {@link #client()} caps a
     * route at ten connections and would serialise the rest.
     */
    static RestClient concurrentClient(List<HttpHost> hosts, int connections) {
        return RestClient.builder(hosts.toArray(new HttpHost[0]))
            .setHttpClientConfigCallback(
                builder -> builder.setConnectionManager(
                    PoolingAsyncClientConnectionManagerBuilder.create().setMaxConnPerRoute(connections).setMaxConnTotal(connections).build()
                )
            )
            .build();
    }

    /** Status code and body of one request of {@link #postConcurrently}. */
    record ConcurrentResult(int status, String body) {
    }

    /**
     * POST {@code body} to {@code path} {@code count} times at once
     * through {@code client} and wait for every answer. A non 2xx
     * status is returned as a result like any other; a transport level
     * failure (connection refused, timeout) is returned with status
     * {@code -1} and the exception text as body.
     */
    static List<ConcurrentResult> postConcurrently(RestClient client, String path, String body, int count) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(count);
        List<ConcurrentResult> results = Collections.synchronizedList(new ArrayList<>(count));
        for (int i = 0; i < count; i++) {
            Request request = new Request("POST", path);
            request.setJsonEntity(body);
            client.performRequestAsync(request, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    results.add(toResult(response));
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    if (e instanceof ResponseException responseException) {
                        results.add(toResult(responseException.getResponse()));
                    } else {
                        results.add(new ConcurrentResult(-1, e.toString()));
                    }
                    latch.countDown();
                }
            });
        }
        assertTrue("concurrent requests did not all complete", latch.await(120, TimeUnit.SECONDS));
        return new ArrayList<>(results);
    }

    private static ConcurrentResult toResult(Response response) {
        try {
            return new ConcurrentResult(response.getStatusLine().getStatusCode(), readAll(response));
        } catch (IOException e) {
            return new ConcurrentResult(response.getStatusLine().getStatusCode(), "unreadable body: " + e);
        }
    }

    /**
     * POST {@code body} to {@code path} without waiting for the answer.
     * The future completes with the status and body of the response, a
     * non 2xx status included; a transport level failure completes it
     * with status {@code -1} and the exception text as body.
     */
    static CompletableFuture<ConcurrentResult> postAsync(RestClient client, String path, String body) {
        CompletableFuture<ConcurrentResult> future = new CompletableFuture<>();
        Request request = new Request("POST", path);
        request.setJsonEntity(body);
        client.performRequestAsync(request, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                future.complete(toResult(response));
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof ResponseException responseException) {
                    future.complete(toResult(responseException.getResponse()));
                } else {
                    future.complete(new ConcurrentResult(-1, e.toString()));
                }
            }
        });
        return future;
    }

    /**
     * A {@code script} query whose Painless script spins
     * {@code iterations} times per document before it matches every
     * document, so a request over a small fixture still takes long
     * enough to time out or to be cancelled while it runs. Painless
     * caps a script at one million loop iterations per execution; the
     * fragment executor evaluates the script once per row for the hits
     * and once more for the count, so 900,000 iterations over a few
     * hundred rows keep a request busy for a second or more on the
     * test cluster.
     */
    static String slowScriptQuery(int iterations) {
        return "{\"script\":{\"script\":{\"source\":\"long x = 0; for (int i = 0; i < "
            + iterations
            + "; i++) { x += i; } return x >= 0 && doc['id'].value >= 0;\"}}}";
    }

    /** The tasks {@code GET /_tasks?actions=<actions>} lists right now, each with its node id under {@code node}. */
    @SuppressWarnings("unchecked")
    static List<Map<String, Object>> tasksOf(RestClient client, String actions) throws IOException {
        String body = readAll(client.performRequest(new Request("GET", "/_tasks?actions=" + actions)));
        List<Map<String, Object>> tasks = new ArrayList<>();
        try (XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, body)) {
            Map<String, Object> nodes = (Map<String, Object>) parser.map().get("nodes");
            if (nodes == null) {
                return tasks;
            }
            for (Object node : nodes.values()) {
                Map<String, Object> nodeTasks = (Map<String, Object>) ((Map<String, Object>) node).get("tasks");
                if (nodeTasks == null) {
                    continue;
                }
                for (Object task : nodeTasks.values()) {
                    tasks.add((Map<String, Object>) task);
                }
            }
        }
        return tasks;
    }

    /**
     * Poll {@code GET /_tasks?actions=<actions>} every few milliseconds
     * until at least {@code count} tasks are listed, for at most thirty
     * seconds. The poll is tight on purpose: a request that is being
     * cancelled from the test runs for a second or two, and
     * {@code assertBusy}'s backoff would miss that window.
     */
    static List<Map<String, Object>> awaitTasks(RestClient client, String actions, int count) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        List<Map<String, Object>> tasks = tasksOf(client, actions);
        while (tasks.size() < count && System.nanoTime() < deadline) {
            Thread.sleep(5);
            tasks = tasksOf(client, actions);
        }
        assertTrue("expected at least " + count + " task(s) for " + actions + ", saw " + tasks, tasks.size() >= count);
        return tasks;
    }

    /**
     * The rows of a {@code _cat} response asked for with
     * {@code format=json}: a JSON array of objects whose values are
     * all strings.
     */
    @SuppressWarnings("unchecked")
    static List<Map<String, Object>> catRowsOf(String jsonArray) {
        try (XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, jsonArray)) {
            return (List<Map<String, Object>>) (List<?>) parser.list();
        } catch (IOException e) {
            throw new AssertionError("could not parse JSON: " + jsonArray, e);
        }
    }

    /** Sum of the integer column {@code column} over the rows of a {@code _cat} response. */
    static int sumCatColumn(String jsonArray, String column) {
        int sum = 0;
        for (Map<String, Object> row : catRowsOf(jsonArray)) {
            sum += Integer.parseInt(String.valueOf(row.get(column)));
        }
        return sum;
    }
}
