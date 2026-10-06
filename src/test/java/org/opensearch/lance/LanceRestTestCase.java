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
import java.util.Set;
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
 * {@code plugins.lance.namespace.poll_cadence=1s} (see {@code build.gradle}) so
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
            Response register = postJson("/_plugins/_lance/namespace", "{\"path\":\"" + scratchDir.toString() + "\"}");
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

    /** Suffix of the ordinary index {@link #oracleIndexFor} loads next to a Lance index. */
    static final String ORACLE_SUFFIX = "-oracle";

    /** Sub field name under which an oracle index keeps the whole value of a full text column, for prefix, wildcard and regexp. */
    static final String ORACLE_RAW_SUBFIELD = "oracle_raw";

    /**
     * The {@code lance_text} columns of every oracle index this JVM has
     * loaded, by oracle index name; {@link #oracleBody} rewrites the
     * string pattern clauses on them to the {@link #ORACLE_RAW_SUBFIELD}.
     */
    private static final Map<String, Set<String>> ORACLE_TEXT_COLUMNS = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * The ordinary index that holds the same rows as the Lance index
     * {@code lanceIndex}, created and loaded on first use. Its mapping
     * is the Lance index's mapping with the plugin's own types replaced:
     * {@code lance_text} becomes an indexed {@code text} field under the
     * standard analyzer with a keyword sub field named
     * {@link #ORACLE_RAW_SUBFIELD}, {@code lance_vector} is left out
     * (core has no vector type), and every other property keeps its
     * type and its {@code index: false, doc_values: true} options so a
     * term, range, prefix or sort resolves through doc values on both
     * indexes and scores a filter 1.0 on both. The field {@code meta}
     * and the mapping {@code _meta} are dropped. The rows are read from
     * the Lance table with {@link LanceTableRows#readRows} at the
     * version the index pins (or the latest) and written with
     * {@code _bulk}, one document per row under the {@code _id} the
     * plugin renders, projected onto the mapped columns the way the
     * fragment path renders {@code _source}; a {@code _refresh} follows.
     * The test framework wipes the index with every other index after
     * each test.
     */
    @SuppressWarnings("unchecked")
    static String oracleIndexFor(String lanceIndex) throws Exception {
        String oracle = lanceIndex + ORACLE_SUFFIX;
        Request exists = new Request("HEAD", "/" + oracle);
        exists.addParameter("ignore", "404");
        if (client().performRequest(exists).getStatusLine().getStatusCode() != 404) {
            return oracle;
        }
        Map<String, Object> settingsResponse = parseJson(
            readAll(client().performRequest(new Request("GET", "/" + lanceIndex + "/_settings?flat_settings=true")))
        );
        Map<String, Object> settings = (Map<String, Object>) ((Map<String, Object>) settingsResponse.get(lanceIndex)).get("settings");
        String tableUri = (String) settings.get("index.plugins.lance.table");
        assertNotNull("index " + lanceIndex + " is not Lance backed: " + settings, tableUri);
        Long version = null;
        Object pinned = settings.get("index.plugins.lance.version");
        if (pinned != null && !pinned.toString().isEmpty() && Long.parseLong(pinned.toString()) >= 0) {
            version = Long.parseLong(pinned.toString());
        }
        Object tag = settings.get("index.plugins.lance.tag");
        if (tag != null && !tag.toString().isEmpty()) {
            version = LanceTableRows.tagVersion(tableUri, tag.toString());
        }

        Map<String, Object> mappingResponse = parseJson(
            readAll(client().performRequest(new Request("GET", "/" + lanceIndex + "/_mapping")))
        );
        Map<String, Object> mappings = (Map<String, Object>) ((Map<String, Object>) mappingResponse.get(lanceIndex)).get("mappings");
        Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
        Set<String> textColumns = new java.util.HashSet<>();
        Map<String, Object> oracleProperties = oracleProperties(properties, textColumns);
        ORACLE_TEXT_COLUMNS.put(oracle, textColumns);

        try (XContentBuilder builder = MediaTypeRegistry.JSON.contentBuilder()) {
            builder.map(
                Map.of(
                    "settings",
                    Map.of("index.number_of_shards", 1, "index.number_of_replicas", 0),
                    "mappings",
                    Map.of("dynamic", "strict", "properties", oracleProperties)
                )
            );
            Request create = new Request("PUT", "/" + oracle);
            create.setJsonEntity(builder.toString());
            client().performRequest(create);
        }

        List<LanceTableRows.TableRow> rows = LanceTableRows.readRows(tableUri, version);
        StringBuilder bulk = new StringBuilder();
        int inChunk = 0;
        for (LanceTableRows.TableRow row : rows) {
            try (
                XContentBuilder action = MediaTypeRegistry.JSON.contentBuilder();
                XContentBuilder source = MediaTypeRegistry.JSON.contentBuilder()
            ) {
                action.map(Map.of("index", Map.of("_id", row.id())));
                source.map(oracleSource(row.values(), properties));
                bulk.append(action.toString()).append('\n').append(source.toString()).append('\n');
            }
            if (++inChunk == 1000) {
                postBulk(oracle, bulk.toString());
                bulk.setLength(0);
                inChunk = 0;
            }
        }
        if (inChunk > 0) {
            postBulk(oracle, bulk.toString());
        }
        client().performRequest(new Request("POST", "/" + oracle + "/_refresh"));
        // One segment, so a collector that keeps a bounded set per
        // segment (the samplers) sees the rows in the one order the
        // fragment path's single slice does.
        client().performRequest(new Request("POST", "/" + oracle + "/_forcemerge?max_num_segments=1"));
        client().performRequest(new Request("POST", "/" + oracle + "/_refresh"));
        return oracle;
    }

    /** {@code target} (a comma separated {@code _search} index expression) with every index replaced by its oracle. */
    static String oracleTargetFor(String target) throws Exception {
        List<String> oracles = new ArrayList<>();
        for (String index : target.split(",")) {
            oracles.add(oracleIndexFor(index));
        }
        return String.join(",", oracles);
    }

    private static void postBulk(String oracle, String body) throws IOException {
        Request request = new Request("POST", "/" + oracle + "/_bulk");
        request.setJsonEntity(body);
        String response = readAll(client().performRequest(request));
        assertFalse("bulk into " + oracle + " failed: " + response, response.contains("\"errors\":true"));
    }

    /**
     * The oracle's {@code properties} for the Lance index's
     * {@code properties}; see {@link #oracleIndexFor}. Collects the
     * dotted paths of the {@code lance_text} columns into
     * {@code textColumns}.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> oracleProperties(Map<String, Object> properties, Set<String> textColumns) {
        return oracleProperties(properties, "", textColumns);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> oracleProperties(Map<String, Object> properties, String prefix, Set<String> textColumns) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            String name = entry.getKey();
            Map<String, Object> definition = new LinkedHashMap<>((Map<String, Object>) entry.getValue());
            definition.remove("meta");
            String type = (String) definition.get("type");
            if ("lance_vector".equals(type)) {
                continue;
            }
            if ("lance_text".equals(type)) {
                textColumns.add(prefix + name);
                Map<String, Object> fields = new LinkedHashMap<>();
                if (definition.get("fields") instanceof Map<?, ?> subFields) {
                    fields.putAll(oracleProperties((Map<String, Object>) subFields, prefix + name + ".", textColumns));
                }
                fields.put(ORACLE_RAW_SUBFIELD, Map.of("type", "keyword"));
                out.put(name, Map.of("type", "text", "analyzer", "standard", "fields", fields));
                continue;
            }
            if (definition.get("properties") instanceof Map<?, ?> children) {
                definition.put("properties", oracleProperties((Map<String, Object>) children, prefix + name + ".", textColumns));
            }
            if (definition.get("fields") instanceof Map<?, ?> subFields) {
                definition.put("fields", oracleProperties((Map<String, Object>) subFields, prefix + name + ".", textColumns));
            }
            if ("date".equals(type) && definition.get("format") instanceof String format && !format.contains("epoch_millis")) {
                // The rows carry dates as epoch millis, as the fragment
                // path renders them; the declared pattern stays first so
                // docvalue_fields and fields print through it.
                definition.put("format", format + "||epoch_millis");
            }
            out.put(name, definition);
        }
        return out;
    }

    /**
     * The row's values projected onto {@code properties}, the way the
     * fragment path renders {@code _source}: only mapped columns, a
     * struct recursing into its mapped children with nulls kept, a
     * nested column as the array of its mapped elements, and a
     * {@code geo_point} column as {@code {lat, lon}} whatever the Arrow
     * storage was.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> oracleSource(Map<String, Object> values, Map<String, Object> properties) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            Map<String, Object> definition = (Map<String, Object>) properties.get(entry.getKey());
            if (definition == null || "lance_vector".equals(definition.get("type"))) {
                continue;
            }
            Object value = oracleValue(entry.getValue(), definition);
            if (value != null) {
                out.put(entry.getKey(), value);
            }
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private static Object oracleValue(Object value, Map<String, Object> definition) {
        if (value == null) {
            return null;
        }
        String type = (String) definition.get("type");
        if ("geo_point".equals(type)) {
            Map<String, Object> meta = (Map<String, Object>) definition.get("meta");
            boolean latFirst = meta == null || !"lon_lat".equals(meta.get("lance_geo_order"));
            List<Object> pair;
            if (value instanceof Map<?, ?> struct) {
                pair = new ArrayList<>(struct.values());
            } else {
                pair = (List<Object>) value;
            }
            if (pair.size() != 2 || pair.get(0) == null || pair.get(1) == null) {
                return null;
            }
            double first = ((Number) pair.get(0)).doubleValue();
            double second = ((Number) pair.get(1)).doubleValue();
            Map<String, Object> point = new LinkedHashMap<>();
            point.put("lat", latFirst ? first : second);
            point.put("lon", latFirst ? second : first);
            return point;
        }
        Map<String, Object> children = (Map<String, Object>) definition.get("properties");
        if (children != null && value instanceof Map<?, ?> struct) {
            Map<String, Object> out = new LinkedHashMap<>();
            for (Map.Entry<?, ?> child : struct.entrySet()) {
                Map<String, Object> childDefinition = children.get(child.getKey()) instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
                if (childDefinition == null) {
                    continue;
                }
                out.put((String) child.getKey(), oracleValue(child.getValue(), childDefinition));
            }
            return out;
        }
        if (children != null && value instanceof List<?> elements) {
            List<Object> out = new ArrayList<>(elements.size());
            for (Object element : elements) {
                out.add(element == null ? null : oracleValue(element, definition));
            }
            return out;
        }
        return value;
    }

    /**
     * {@code body} as the oracle index of {@code lanceIndex} accepts it:
     * every {@code lance_match}, {@code lance_match_phrase} and
     * {@code lance_multi_match} clause becomes the core clause of the
     * same name and arguments, and a {@code prefix}, {@code wildcard}
     * or {@code regexp} on a {@code lance_text} column moves to the
     * column's {@link #ORACLE_RAW_SUBFIELD}, where it reads the whole
     * value as the Lance scan does. Everything else is copied.
     */
    static String oracleBody(String lanceIndex, String body) {
        Set<String> textColumns = ORACLE_TEXT_COLUMNS.getOrDefault(lanceIndex + ORACLE_SUFFIX, Set.of());
        Object rewritten = rewriteForOracle(parseJson(body), textColumns);
        try (XContentBuilder builder = MediaTypeRegistry.JSON.contentBuilder()) {
            builder.value(rewritten);
            return builder.toString();
        } catch (IOException e) {
            throw new AssertionError("could not render the oracle body of " + body, e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Object rewriteForOracle(Object node, Set<String> textColumns) {
        if (node instanceof List<?> list) {
            List<Object> out = new ArrayList<>(list.size());
            for (Object element : list) {
                out.add(rewriteForOracle(element, textColumns));
            }
            return out;
        }
        if (!(node instanceof Map<?, ?> map)) {
            return node;
        }
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String key = (String) entry.getKey();
            Object value = rewriteForOracle(entry.getValue(), textColumns);
            switch (key) {
                case "lance_match", "lance_match_phrase" -> {
                    Map<String, Object> clause = new LinkedHashMap<>((Map<String, Object>) value);
                    String field = (String) clause.remove("field");
                    out.put(key.substring("lance_".length()), Map.of(field, clause));
                }
                case "lance_multi_match" -> {
                    Map<String, Object> clause = new LinkedHashMap<>((Map<String, Object>) value);
                    List<String> fields = new ArrayList<>();
                    Map<String, Object> boosts = (Map<String, Object>) clause.remove("boosts");
                    for (Object field : (List<Object>) clause.get("fields")) {
                        Object boost = boosts == null ? null : boosts.get(field);
                        fields.add(boost == null ? (String) field : field + "^" + boost);
                    }
                    clause.put("fields", fields);
                    out.put("multi_match", clause);
                }
                case "prefix", "wildcard", "regexp" -> {
                    Map<String, Object> clause = (Map<String, Object>) value;
                    Map<String, Object> moved = new LinkedHashMap<>();
                    for (Map.Entry<String, Object> argument : clause.entrySet()) {
                        String field = argument.getKey();
                        moved.put(textColumns.contains(field) ? field + "." + ORACLE_RAW_SUBFIELD : field, argument.getValue());
                    }
                    out.put(key, moved);
                }
                default -> out.put(key, value);
            }
        }
        return out;
    }

    /** {@code index} with the oracle suffix removed when it carries one, so a hit of an oracle index names the Lance index. */
    static String lanceIndexNameOf(Object index) {
        String name = String.valueOf(index);
        return name.endsWith(ORACLE_SUFFIX) ? name.substring(0, name.length() - ORACLE_SUFFIX.length()) : name;
    }

    /**
     * Assert that {@code body} selects the same rows on the Lance index
     * and on its oracle: the same {@code hits.total} and the same set of
     * hit {@code _id} values, in any order. The comparison of a full text
     * query, whose scores and order of equal scores Lance's tokenizer
     * and BM25 do not share with Lucene's. Returns the Lance index's
     * body.
     */
    static String assertSameHitIdsAsOracle(String indexName, String body) throws Exception {
        String fragmentBody = readAll(postJson("/" + indexName + "/_search", body));
        String oracleBody = readAll(postJson("/" + oracleIndexFor(indexName) + "/_search", oracleBody(indexName, body)));
        assertEquals(body, totalOf(oracleBody), totalOf(fragmentBody));
        assertEquals(body, new java.util.HashSet<>(idsOf(hitsOf(oracleBody))), new java.util.HashSet<>(idsOf(hitsOf(fragmentBody))));
        return fragmentBody;
    }

    /**
     * Whether {@code body} reads a {@code lance_text} column through a
     * clause Lance scores: a {@code match}, {@code match_phrase},
     * {@code multi_match}, {@code simple_query_string},
     * {@code query_string}, {@code fuzzy} or any {@code lance_*} clause.
     * The comparison with the oracle of such a body is
     * {@link #assertSameHitIdsAsOracle}.
     */
    static boolean isFullTextBody(String body) {
        for (String clause : new String[] {
            "\"match\"",
            "\"match_phrase\"",
            "\"multi_match\"",
            "\"simple_query_string\"",
            "\"query_string\"",
            "\"fuzzy\"",
            "\"lance_" }) {
            if (body.contains(clause)) {
                return true;
            }
        }
        return false;
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
     * scan and the Lucene counters) in {@code GET /_plugins/_lance/stats}. A
     * request served by the fragment path advances it by one per
     * executor; a request the stock search action served leaves it
     * unchanged.
     */
    @SuppressWarnings("unchecked")
    static long fragmentRequestsExecuted() throws IOException {
        Map<String, Object> stats = parseJson(readAll(client().performRequest(new Request("GET", "/_plugins/_lance/stats"))));
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
     * {@code plan.pruned.fragments} in {@code GET /_plugins/_lance/stats}.
     */
    @SuppressWarnings("unchecked")
    static long prunedFragments() throws IOException {
        Map<String, Object> stats = parseJson(readAll(client().performRequest(new Request("GET", "/_plugins/_lance/stats"))));
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
     * The planner's table statistics counters of {@code GET /_plugins/_lance/stats}
     * summed over the nodes: {@code tables} (entries held),
     * {@code pending} (collections queued or running),
     * {@code planned_without} (plans made without statistics) and
     * {@code collect_millis_total}.
     */
    @SuppressWarnings("unchecked")
    static Map<String, Long> planStatistics() throws IOException {
        Map<String, Object> stats = parseJson(readAll(client().performRequest(new Request("GET", "/_plugins/_lance/stats"))));
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
        Request explain = new Request("GET", "/_plugins/_lance/explain/" + indexName);
        explain.setJsonEntity("{\"size\":0}");
        client().performRequest(explain);
        awaitTableStatistics();
    }

    /**
     * The hits of {@code searchBody} with every rendered key ({@code _score}
     * included) except {@code _shard} and {@code _node}, which
     * {@code explain: true} adds and which name the node that rendered
     * the hit. A hit of an oracle index names its Lance index in
     * {@code _index}, so the hits of the two can be compared.
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
            if (copy.containsKey("_index")) {
                copy.put("_index", lanceIndexNameOf(copy.get("_index")));
            }
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
                if (copy.containsKey("_index")) {
                    copy.put("_index", lanceIndexNameOf(copy.get("_index")));
                }
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
