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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
import org.opensearch.core.xcontent.XContentBuilder;
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

    /** Suffix of the empty ordinary index {@link #withStockOracle} puts next to a Lance index. */
    private static final String STOCK_ORACLE_SUFFIX = "-stock-oracle";

    /**
     * {@code target} (a {@code _search} index expression) with an empty
     * ordinary index appended, so the request runs on OpenSearch's stock
     * search action instead of the fragment coordinator. The dispatch
     * filter hands a target with an index that is not Lance backed to the
     * stock action, which searches the Lance index through the shard
     * engine's whole table reader: one shard's Lucene query phase over
     * the same rows the fragment executors read. That answer is the
     * oracle the fragment path tests compare against. The appended index
     * holds no document, so it adds no hit, no count and no bucket; it
     * copies the mapping of the target's first index so that a sort, a
     * collapse, a nested aggregation or a doc values clause resolves on
     * its shard too, and both shards refuse the same requests with the
     * same message (a failure on one shard alone would answer 200 with
     * partial results). The index is created on first use and the test
     * framework wipes it with every other index after each test.
     */
    @SuppressWarnings("unchecked")
    static String withStockOracle(String target) throws IOException {
        String first = target.split(",")[0];
        String oracle = first.replaceAll("[^a-z0-9_-]", "-") + STOCK_ORACLE_SUFFIX;
        Request exists = new Request("HEAD", "/" + oracle);
        exists.addParameter("ignore", "404");
        if (client().performRequest(exists).getStatusLine().getStatusCode() == 404) {
            Map<String, Object> mappingResponse = parseJson(
                readAll(client().performRequest(new Request("GET", "/" + first + "/_mapping")))
            );
            Map<String, Object> mappings = new LinkedHashMap<>(
                (Map<String, Object>) ((Map<String, Object>) mappingResponse.values().iterator().next()).get("mappings")
            );
            mappings.remove("_meta");
            Map<String, Object> body = Map.of(
                "settings",
                Map.of("index.number_of_shards", 1, "index.number_of_replicas", 0),
                "mappings",
                mappings
            );
            try (XContentBuilder builder = MediaTypeRegistry.JSON.contentBuilder()) {
                builder.map(body);
                Request create = new Request("PUT", "/" + oracle);
                create.setJsonEntity(builder.toString());
                client().performRequest(create);
            }
        }
        return target + "," + oracle;
    }

    /**
     * {@code aggregations} as the block to compare between the two
     * paths: the map itself, or {@code null} when the response carries
     * no block or an empty one, as a response without aggregations has
     * no block at all.
     */
    @SuppressWarnings("unchecked")
    static Map<String, Object> aggregationsBlock(Object aggregations) {
        if (!(aggregations instanceof Map<?, ?> map) || map.isEmpty()) {
            return null;
        }
        return (Map<String, Object>) map;
    }

    /**
     * How many fragment requests the data nodes have executed so far:
     * the sum over the nodes of {@code plan.executed} (both the Lance
     * scan and the Lucene counters) in {@code GET /_lance/stats}. A
     * request served by the fragment path advances it by one per
     * executor; a request the stock search action served leaves it
     * unchanged.
     */
    @SuppressWarnings("unchecked")
    static long fragmentRequestsExecuted() throws IOException {
        Map<String, Object> stats = parseJson(readAll(client().performRequest(new Request("GET", "/_lance/stats"))));
        Map<String, Object> nodes = (Map<String, Object>) stats.get("nodes");
        long total = 0L;
        for (Object node : nodes.values()) {
            Map<String, Object> plan = (Map<String, Object>) ((Map<String, Object>) node).get("plan");
            Map<String, Object> executed = (Map<String, Object>) plan.get("executed");
            for (Object count : executed.values()) {
                total += ((Number) count).longValue();
            }
        }
        return total;
    }

    /**
     * How many fragments the data nodes' executors have skipped under
     * zone map pruning so far: the sum over the nodes of
     * {@code plan.pruned.fragments} in {@code GET /_lance/stats}.
     */
    @SuppressWarnings("unchecked")
    static long prunedFragments() throws IOException {
        Map<String, Object> stats = parseJson(readAll(client().performRequest(new Request("GET", "/_lance/stats"))));
        Map<String, Object> nodes = (Map<String, Object>) stats.get("nodes");
        long total = 0L;
        for (Object node : nodes.values()) {
            Map<String, Object> plan = (Map<String, Object>) ((Map<String, Object>) node).get("plan");
            Map<String, Object> pruned = (Map<String, Object>) plan.get("pruned");
            total += ((Number) pruned.get("fragments")).longValue();
        }
        return total;
    }

    /**
     * The planner's table statistics counters of {@code GET /_lance/stats}
     * summed over the nodes: {@code tables} (entries held),
     * {@code pending} (collections queued or running),
     * {@code planned_without} (plans made without statistics) and
     * {@code collect_millis_total}.
     */
    @SuppressWarnings("unchecked")
    static Map<String, Long> planStatistics() throws IOException {
        Map<String, Object> stats = parseJson(readAll(client().performRequest(new Request("GET", "/_lance/stats"))));
        Map<String, Object> nodes = (Map<String, Object>) stats.get("nodes");
        Map<String, Long> totals = new LinkedHashMap<>();
        for (Object node : nodes.values()) {
            Map<String, Object> plan = (Map<String, Object>) ((Map<String, Object>) node).get("plan");
            Map<String, Object> statistics = (Map<String, Object>) plan.get("statistics");
            for (Map.Entry<String, Object> counter : statistics.entrySet()) {
                totals.merge(counter.getKey(), ((Number) counter.getValue()).longValue(), Long::sum);
            }
        }
        return totals;
    }

    /**
     * Wait until no node is collecting planner table statistics
     * ({@code plan.statistics.pending} is zero on every node). A request
     * that misses the statistics of its version, and the shard start that
     * builds the version's snapshot, start the collection before they
     * return, so a wait placed after them leaves the statistics in the
     * cache for the requests that follow.
     */
    static void awaitTableStatistics() throws Exception {
        assertBusy(() -> {
            Map<String, Long> statistics = planStatistics();
            assertEquals("statistics collections still pending: " + statistics, 0L, statistics.getOrDefault("pending", 0L).longValue());
        }, 60, TimeUnit.SECONDS);
    }

    /**
     * Leave the planner's table statistics of {@code indexName}'s current
     * version in the cache of the node the client talks to: one explain
     * of the empty body starts their collection when nothing has yet,
     * then {@link #awaitTableStatistics()}. The first request that plans
     * against a version otherwise runs without statistics, so a test
     * whose assertions read them (zone map pruning, the admission gate's
     * index estimates) calls this after attaching.
     */
    static void warmTableStatistics(String indexName) throws Exception {
        Request explain = new Request("GET", "/" + indexName + "/_lance/explain");
        explain.setJsonEntity("{\"size\":0}");
        client().performRequest(explain);
        awaitTableStatistics();
    }

    /**
     * The hits of {@code searchBody} with every rendered key ({@code _score}
     * included) except {@code _shard} and {@code _node}, which
     * {@code explain: true} adds and which name the node that rendered
     * the hit (the coordinating node on the fragment path, the data
     * node on the stock search action).
     */
    @SuppressWarnings("unchecked")
    static List<Map<String, Object>> fullHitsOf(String searchBody) {
        Map<String, Object> map = parseJson(searchBody);
        List<Object> hits = (List<Object>) ((Map<String, Object>) map.get("hits")).get("hits");
        List<Map<String, Object>> out = new ArrayList<>(hits.size());
        for (Object hit : hits) {
            Map<String, Object> copy = new java.util.LinkedHashMap<>((Map<String, Object>) hit);
            copy.remove("_shard");
            copy.remove("_node");
            out.add(copy);
        }
        return out;
    }

    /** The {@code hits.total} object of {@code searchBody}, or null when the response has none. */
    @SuppressWarnings("unchecked")
    static Map<String, Object> totalOf(String searchBody) {
        return (Map<String, Object>) ((Map<String, Object>) parseJson(searchBody).get("hits")).get("total");
    }

    /** Status code and body of a {@code POST} whose status may be an error. */
    static ConcurrentResult postForStatus(String path, String body) throws IOException {
        try {
            return toResult(postJson(path, body));
        } catch (ResponseException e) {
            return toResult(e.getResponse());
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
     * Wait until a fragment path request is running: at least one
     * {@code internal:lance/coordinator_search} task and at least
     * {@code executors} {@code internal:lance/fragment_query} tasks are
     * listed at the same time. Returns the executor tasks. Uses
     * {@code assertBusy} (backoff from a millisecond, ten seconds in
     * all); the request the tests wait for runs for a second or more,
     * and its tasks appear within the first few checks.
     */
    static List<Map<String, Object>> awaitFragmentQueryRunning(RestClient client, int executors) throws Exception {
        AtomicReference<List<Map<String, Object>>> found = new AtomicReference<>();
        assertBusy(() -> {
            List<Map<String, Object>> coordinators = tasksOf(client, "*lance/coordinator*");
            List<Map<String, Object>> tasks = tasksOf(client, "*lance/fragment_query*");
            assertFalse("no coordinator task yet, executors: " + tasks, coordinators.isEmpty());
            assertTrue("expected at least " + executors + " executor task(s), saw " + tasks, tasks.size() >= executors);
            found.set(tasks);
        });
        return found.get();
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
