/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;

/**
 * The attach body's {@code indexes} clause end to end: the build creates
 * the requested Lance index types instead of the defaults, {@code none}
 * leaves a column without an index, {@code GET /_lance/stats} reports
 * the types present per column, queries through the chosen types return
 * the same hits as an oracle index over the same table attached without
 * a preference, and the one-shot {@code indexes} object on
 * {@code build_indexes} builds the requested type without changing any
 * persisted preference. Attach validation of the clause is covered too.
 */
public class LanceIndexTypesIT extends LanceRestTestCase {

    private static final String INDEXES_CLAUSE = "\"indexes\":{"
        + "\"rating\":{\"scalar\":\"zonemap\",\"params\":{\"rows_per_zone\":8}},"
        + "\"category\":{\"scalar\":\"bitmap\"},"
        + "\"flag\":{\"scalar\":\"none\"},"
        + "\"embedding\":{\"vector\":\"ivf_flat\",\"params\":{\"num_partitions\":2}}}";

    public void testIndexesClausePicksTheTypesEndToEnd() throws Exception {
        String suffix = "idxtypes-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        String tableUri = LanceTableFactory.writeHintFixtureTable(scratchDir, tableName, 2, 10);
        String prefIndex = tableName + "-pref";
        String oracleIndex = tableName + "-oracle";
        try {
            Response attach = postJson(
                "/_lance/attach",
                "{\"table\":\"" + tableUri + "\",\"name\":\"" + prefIndex + "\"," + INDEXES_CLAUSE + "}"
            );
            assertEquals("attach failed: " + readAll(attach), RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            ensureGreen(prefIndex);

            // Oracle: the same table under another name, no preference.
            Response oracle = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\",\"name\":\"" + oracleIndex + "\"}");
            assertEquals("attach failed: " + readAll(oracle), RestStatus.OK.getStatus(), oracle.getStatusLine().getStatusCode());
            ensureGreen(oracleIndex);

            // The preference persists inside the overrides setting.
            String settings = readAll(client().performRequest(new Request("GET", "/" + prefIndex + "/_settings")));
            assertTrue("expected the indexes preference in index.lance.overrides: " + settings, settings.contains("zonemap"));

            // Build with the persisted preference. The columns filter
            // keeps the untargeted columns (id, tags, body) out of this
            // build so only the preference-driven types are created.
            String build = readAll(
                postJson("/_lance/build_indexes/" + prefIndex, "{\"columns\":[\"rating\",\"category\",\"flag\",\"embedding\"]}")
            );
            assertTrue("expected zonemap on rating: " + build, build.contains("{\"column\":\"rating\",\"type\":\"ZONEMAP\"}"));
            assertTrue("expected bitmap on category: " + build, build.contains("{\"column\":\"category\",\"type\":\"BITMAP\"}"));
            assertTrue(
                "expected ivf_flat on embedding: " + build,
                build.contains("\"vector\":[{\"column\":\"embedding\",\"type\":\"IVF_FLAT\"}]")
            );
            assertTrue(
                "expected flag skipped with the none reason: " + build,
                build.contains("{\"column\":\"flag\",\"reason\":\"index type none\"}")
            );

            // The stats surface reports the types present per column, and
            // flag stays without an index.
            assertBusy(() -> {
                String stats = readAll(client().performRequest(new Request("GET", "/_lance/stats")));
                Map<String, Object> indexTypes = statsIndexTypes(stats, prefIndex);
                assertNotNull("stats must report index_types for " + prefIndex + ": " + stats, indexTypes);
                assertEquals("ZONEMAP", normalisedType(indexTypes, "rating"));
                assertEquals("BITMAP", normalisedType(indexTypes, "category"));
                assertEquals("IVFFLAT", normalisedType(indexTypes, "embedding"));
                assertEquals("INVERTED", normalisedType(indexTypes, "body"));
                assertFalse("flag must carry no index: " + indexTypes, indexTypes.containsKey("flag"));
            });

            // Queries through the chosen index types agree with the
            // oracle: the reader queries through Lance's engine and does
            // not care which index type answers.
            String termQuery = "{\"size\":20,\"query\":{\"term\":{\"category\":\"c1\"}}}";
            assertSameHits(termQuery, prefIndex, oracleIndex);
            String rangeQuery = "{\"size\":20,\"query\":{\"range\":{\"rating\":{\"gte\":100,\"lt\":500}}}}";
            assertSameHits(rangeQuery, prefIndex, oracleIndex);
            String knnQuery = "{\"size\":3,\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":[5,0,0,0,0,0,0,0],\"k\":3}}}";
            assertSameHits(knnQuery, prefIndex, oracleIndex);

            // One-shot override on build_indexes: builds the requested
            // type for a column no persisted preference names, without
            // writing any preference. Runs against the oracle index,
            // whose settings carry no overrides at all.
            String oneShot = readAll(
                postJson("/_lance/build_indexes/" + oracleIndex, "{\"columns\":[\"flag\"],\"indexes\":{\"flag\":{\"scalar\":\"bitmap\"}}}")
            );
            assertTrue("expected the one-shot bitmap on flag: " + oneShot, oneShot.contains("{\"column\":\"flag\",\"type\":\"BITMAP\"}"));
            String oracleSettings = readAll(client().performRequest(new Request("GET", "/" + oracleIndex + "/_settings")));
            assertFalse("the one-shot preference must not persist: " + oracleSettings, oracleSettings.contains("bitmap"));
            String prefSettings = readAll(client().performRequest(new Request("GET", "/" + prefIndex + "/_settings")));
            assertTrue("the attach-time preference must survive: " + prefSettings, prefSettings.contains("zonemap"));
        } finally {
            for (String index : new String[] { prefIndex, oracleIndex }) {
                try {
                    client().performRequest(new Request("DELETE", "/" + index));
                } catch (Exception ignored) {}
            }
            deleteRecursively(scratchDir);
        }
    }

    public void testAttachRejectsWrongKindAndUnknownColumn() throws Exception {
        String suffix = "idxtypesbad-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        String tableUri = LanceTableFactory.writeHintFixtureTable(scratchDir, tableName, 1, 10);
        try {
            // A vector type on a scalar column names the column and its
            // Arrow type.
            ResponseException wrongKind = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/_lance/attach",
                    "{\"table\":\"" + tableUri + "\",\"name\":\"" + tableName + "\",\"indexes\":{\"rating\":{\"vector\":\"ivf_flat\"}}}"
                )
            );
            assertEquals(400, wrongKind.getResponse().getStatusLine().getStatusCode());
            String wrongKindBody = readAll(wrongKind.getResponse());
            assertTrue("expected the column and its Arrow type: " + wrongKindBody, wrongKindBody.contains("[rating] is Int"));

            // A scalar type on the vector column likewise.
            ResponseException scalarOnVector = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/_lance/attach",
                    "{\"table\":\"" + tableUri + "\",\"name\":\"" + tableName + "\",\"indexes\":{\"embedding\":{\"scalar\":\"bitmap\"}}}"
                )
            );
            assertEquals(400, scalarOnVector.getResponse().getStatusLine().getStatusCode());
            String scalarOnVectorBody = readAll(scalarOnVector.getResponse());
            assertTrue("expected the column named: " + scalarOnVectorBody, scalarOnVectorBody.contains("[embedding] is"));

            // An unknown column is refused at attach.
            ResponseException unknown = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/_lance/attach",
                    "{\"table\":\"" + tableUri + "\",\"name\":\"" + tableName + "\",\"indexes\":{\"nope\":{\"scalar\":\"btree\"}}}"
                )
            );
            assertEquals(400, unknown.getResponse().getStatusLine().getStatusCode());
            assertTrue(readAll(unknown.getResponse()).contains("unknown column [nope]"));

            // An unknown type name is refused before the table is opened.
            ResponseException unknownType = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/_lance/attach",
                    "{\"table\":\"" + tableUri + "\",\"name\":\"" + tableName + "\",\"indexes\":{\"rating\":{\"scalar\":\"hash\"}}}"
                )
            );
            assertEquals(400, unknownType.getResponse().getStatusLine().getStatusCode());
            assertTrue(readAll(unknownType.getResponse()).contains("scalar=hash] is not supported"));
        } finally {
            deleteRecursively(scratchDir);
        }
    }

    /** Run {@code query} against both indexes and assert equal totals and id sets. */
    private void assertSameHits(String query, String left, String right) throws Exception {
        String leftBody = readAll(postJson("/" + left + "/_search", query));
        String rightBody = readAll(postJson("/" + right + "/_search", query));
        assertEquals(
            "totals must agree for " + query + ": " + leftBody + " vs " + rightBody,
            extractIntPath(rightBody, "hits", "total", "value"),
            extractIntPath(leftBody, "hits", "total", "value")
        );
        assertEquals("hit ids must agree for " + query, idsOf(hitsOf(rightBody)), idsOf(hitsOf(leftBody)));
    }

    /**
     * The {@code index_types} object of {@code index} from a
     * {@code GET /_lance/stats} body, searched across the per-node
     * {@code indices} blocks (only the shard-hosting node reports the
     * index). {@code null} when no node reports it.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> statsIndexTypes(String statsBody, String index) {
        Map<String, Object> parsed = parseJson(statsBody);
        Object nodes = parsed.get("nodes");
        if (!(nodes instanceof Map<?, ?> nodeMap)) {
            return null;
        }
        for (Object node : nodeMap.values()) {
            if (node instanceof Map<?, ?> nodeBody
                && nodeBody.get("indices") instanceof Map<?, ?> indices
                && indices.get(index) instanceof Map<?, ?> indexBody
                && indexBody.get("index_types") instanceof Map<?, ?> indexTypes) {
                return (Map<String, Object>) indexTypes;
            }
        }
        return null;
    }

    /**
     * The one index type reported for {@code column}, normalised to
     * upper case without underscores so Lance's display forms
     * ({@code ZoneMap}, {@code IVF_FLAT}) compare stably.
     */
    private static String normalisedType(Map<String, Object> indexTypes, String column) {
        Object types = indexTypes.get(column);
        assertTrue("expected one index type for " + column + ": " + indexTypes, types instanceof List<?> list && list.size() == 1);
        return ((List<?>) types).get(0).toString().replace("_", "").toUpperCase(Locale.ROOT);
    }
}
