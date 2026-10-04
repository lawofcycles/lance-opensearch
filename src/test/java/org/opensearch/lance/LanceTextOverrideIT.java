/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;

/**
 * The {@code type: lance_text} override end to end. A Utf8 column without
 * an inverted index maps {@code keyword} by default and a stock
 * {@code match} on it is an exact match (0 hits); declared
 * {@code lance_text} the same column answers the {@code match} from
 * Lance's flat BM25 scan with scored hits, and the same ids as the column
 * answers once its writer builds the inverted index. A writer building
 * the index afterwards does not make the freshness check rebuild the
 * declared index. A phrase on the declared column runs through Lance's
 * flat phrase path. The admission gate judges the flat scan as its own
 * kind with its own message, and leaves the message of an indexed column
 * as it was. A keyword sub-field next to the declaration serves exact
 * match and terms aggregations.
 *
 * <p>The fixture is {@link LanceTableFactory#writeEnglishTextTable}: six
 * rows, {@code body} without an index, the token {@code the} in rows 0,
 * 1, 2, 3 and 5 (row 4 is null), the phrase {@code the park} in row 0
 * only. The two paths tokenise differently: the flat path runs Lance's
 * plain simple tokenizer (no lower casing, stemming or stop word
 * removal), an index built with Lance's defaults lower cases, stems and
 * drops English stop words. {@code the} is therefore found by the flat
 * path and not by the index; the id set comparison uses one token per
 * row ({@code park}, {@code field}, {@code windowsill}, {@code store},
 * {@code marathon}) that both paths see the same way.
 */
public class LanceTextOverrideIT extends LanceRestTestCase {

    private static final String MATCH_THE = "{\"size\":10,\"query\":{\"match\":{\"body\":\"the\"}}}";
    private static final List<Integer> ROWS_WITH_THE = List.of(0, 1, 2, 3, 5);
    /** One token per non null row, none a stop word or an inflected form, so the flat path and the index agree on it. */
    private static final String MATCH_WORDS = "{\"size\":10,\"query\":{\"match\":{\"body\":\"park field windowsill store marathon\"}}}";
    private static final List<Integer> ROWS_WITH_WORDS = List.of(0, 1, 2, 3, 5);

    public void testOverrideMapsLanceTextAndMatchesWithoutAnIndexWhileTheDefaultStaysKeyword() throws Exception {
        String suffix = "lancetext-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        String tableUri = LanceTableFactory.writeEnglishTextTable(scratchDir, tableName);
        String declared = tableName;
        String plain = "plain-" + suffix;
        String indexed = "indexed-" + suffix;
        try {
            // The default is unchanged: without the override the column
            // maps keyword and a match on it is an exact match that
            // finds no row.
            attach(tableUri, plain, null);
            String plainMapping = readAll(client().performRequest(new Request("GET", "/" + plain + "/_mapping")));
            assertTrue("body maps keyword by default: " + plainMapping, plainMapping.contains("\"body\":{\"type\":\"keyword\""));
            assertFalse(plainMapping, plainMapping.contains("lance_override_type"));
            String plainMatch = readAll(postJson("/" + plain + "/_search", MATCH_THE));
            assertEquals(
                "match on a keyword column is an exact match: " + plainMatch,
                0,
                extractIntPath(plainMatch, "hits", "total", "value")
            );

            // Declared lance_text the same column is full text: the
            // mapping says so and records the declaration, and the
            // match is answered by Lance's flat BM25 scan with scores.
            attach(tableUri, declared, "{\"body\":{\"type\":\"lance_text\"}}");
            String mapping = readAll(client().performRequest(new Request("GET", "/" + declared + "/_mapping")));
            assertTrue("body maps lance_text under the override: " + mapping, mapping.contains("\"body\":{\"type\":\"lance_text\""));
            assertTrue("meta records the declaration: " + mapping, mapping.contains("\"lance_override_type\":\"lance_text\""));
            String flat = readAll(postJson("/" + declared + "/_search?request_cache=false", MATCH_THE));
            assertEquals("the flat scan finds the five rows: " + flat, 5, extractIntPath(flat, "hits", "total", "value"));
            assertEquals(ROWS_WITH_THE, sortedSourceIds(flat));
            logger.info(
                "--> flat BM25 match over {} rows took {} ms; admission after it: {}",
                LanceTableFactory.ENGLISH_SENTENCES.length,
                extractIntPath(flat, "took"),
                admissionStats()
            );
            for (Map<String, Object> hit : fullHitsOf(flat)) {
                Object score = hit.get("_score");
                assertTrue("every hit is BM25 scored: " + flat, score instanceof Number && ((Number) score).doubleValue() > 0d);
            }
            // The multi word match, OR semantics, finds one row per token.
            String flatWords = readAll(postJson("/" + declared + "/_search", MATCH_WORDS));
            assertEquals("the flat scan finds one row per token: " + flatWords, ROWS_WITH_WORDS, sortedSourceIds(flatWords));
            // lance_match reaches the same path.
            String explicit = readAll(
                postJson("/" + declared + "/_search", "{\"size\":10,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"the\"}}}")
            );
            assertEquals(ROWS_WITH_THE, sortedSourceIds(explicit));

            // The flat phrase path: the phrase sits in row 0 only, and
            // Lance answers it without an index (no positions to miss).
            String phrase = readAll(
                postJson(
                    "/" + declared + "/_search",
                    "{\"size\":10,\"query\":{\"lance_match_phrase\":{\"field\":\"body\",\"query\":\"the park\"}}}"
                )
            );
            assertEquals("the flat phrase path finds row 0: " + phrase, List.of(0), sortedSourceIds(phrase));
            String reversed = readAll(
                postJson(
                    "/" + declared + "/_search",
                    "{\"size\":10,\"query\":{\"lance_match_phrase\":{\"field\":\"body\",\"query\":\"park the\"}}}"
                )
            );
            assertEquals("the reversed phrase finds nothing: " + reversed, 0, extractIntPath(reversed, "hits", "total", "value"));

            // The default index is dropped before the writer builds the
            // index: its keyword mapping would otherwise flip, which is
            // the existing behaviour and not what this test is about.
            client().performRequest(new Request("DELETE", "/" + plain));
            String uuidBefore = indexUuid(declared);
            LanceTableFactory.createFtsIndex(tableUri, "body", "simple", true);

            // The same table attached once the index exists answers the
            // same ids for tokens both analyzers see alike (the order
            // and the scores differ: the two paths see different corpus
            // statistics). The index's default analyzer drops English
            // stop words, so `the` finds nothing through it where the
            // flat path found five rows.
            attach(tableUri, indexed, null);
            String indexedMapping = readAll(client().performRequest(new Request("GET", "/" + indexed + "/_mapping")));
            assertTrue(
                "the index makes body lance_text by itself: " + indexedMapping,
                indexedMapping.contains("\"body\":{\"type\":\"lance_text\"")
            );
            assertFalse(indexedMapping, indexedMapping.contains("lance_override_type"));
            String fromIndex = readAll(postJson("/" + indexed + "/_search", MATCH_WORDS));
            assertEquals("the index and the flat scan agree on the ids", sortedSourceIds(flatWords), sortedSourceIds(fromIndex));
            String stopWord = readAll(postJson("/" + indexed + "/_search", MATCH_THE));
            assertEquals("the index's analyzer drops the stop word: " + stopWord, 0, extractIntPath(stopWord, "hits", "total", "value"));

            // The declared index sees the writer's commit as a move
            // without a mapping change: the uuid stands (no rebuild),
            // the mapping is still lance_text and the match still
            // answers, now through the index (so with its analyzer).
            Map<String, Object> outcome = sync(declared);
            assertEquals(outcome.toString(), true, outcome.get("checked"));
            assertEquals("a declared column never flips, so nothing is rebuilt: " + outcome, false, outcome.get("rebuilt"));
            assertEquals("the mapping did not change: " + outcome, false, outcome.get("mapping_changed"));
            assertEquals("the index was not recreated", uuidBefore, indexUuid(declared));
            String afterIndex = readAll(client().performRequest(new Request("GET", "/" + declared + "/_mapping")));
            assertTrue(afterIndex, afterIndex.contains("\"body\":{\"type\":\"lance_text\""));
            assertTrue(afterIndex, afterIndex.contains("\"lance_override_type\":\"lance_text\""));
            String afterSync = readAll(postJson("/" + declared + "/_search", MATCH_WORDS));
            assertEquals(ROWS_WITH_WORDS, sortedSourceIds(afterSync));
            String afterSyncStopWord = readAll(postJson("/" + declared + "/_search", MATCH_THE));
            assertEquals(
                "the declared column now answers through the index: " + afterSyncStopWord,
                0,
                extractIntPath(afterSyncStopWord, "hits", "total", "value")
            );
        } finally {
            for (String index : List.of(declared, plain, indexed)) {
                try {
                    client().performRequest(new Request("DELETE", "/" + index));
                } catch (Exception ignored) {
                    // best-effort cleanup; the base class wipes indices too
                }
            }
            deleteRecursively(scratchDir);
        }
    }

    public void testFlatScanIsJudgedAsItsOwnAdmissionKindAndTheIndexedMessageIsUnchanged() throws Exception {
        String suffix = "lancetextadm-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        String tableUri = LanceTableFactory.writeEnglishTextTable(scratchDir, tableName);
        String declared = tableName;
        try (LanceTestCluster withIndex = LanceTestCluster.setUp(16, "lancetextidx")) {
            attach(tableUri, declared, "{\"body\":{\"type\":\"lance_text\"}}");
            String admitted = readAll(postJson("/" + declared + "/_search?request_cache=false", MATCH_THE));
            assertEquals(5, extractIntPath(admitted, "hits", "total", "value"));

            // A one byte shard share makes every estimate count in full
            // and a scripted reading below the headroom leaves nothing
            // to admit it with.
            updateClusterSetting("plugins.lance.test.index_cache_shard_share", "\"1b\"");
            updateClusterSetting("plugins.lance.test.admission_available_memory", "[\"1kb\"]");
            try {
                Map<String, Object> before = admissionStats();
                ResponseException refused = expectThrows(
                    ResponseException.class,
                    () -> postJson("/" + declared + "/_search?request_cache=false", MATCH_THE)
                );
                String body = readAll(refused.getResponse());
                assertEquals(body, RestStatus.TOO_MANY_REQUESTS.getStatus(), refused.getResponse().getStatusLine().getStatusCode());
                assertTrue(body, body.contains("[lance_admission] fts_flat estimate"));
                assertTrue(body, body.contains("full text scan without an inverted index over [" + declared + "]"));
                assertTrue(body, body.contains("flat BM25 scan of [6] rows on column [body] at [100b] each"));
                assertTrue(
                    body,
                    body.contains("Create an inverted index on column [body] with the table's writer (pylance create_scalar_index)")
                );
                assertFalse("the indexed wording is not used: " + body, body.contains("inverted index document set"));
                Map<String, Object> after = admissionStats();
                assertEquals(after.toString(), rejections(before, "fts_flat") + 1, rejections(after, "fts_flat"));
                assertEquals(after.toString(), rejections(before, "fts"), rejections(after, "fts"));
                assertEquals(after.toString(), "fts_flat", after.get("last_kind"));
                assertEquals(after.toString(), 6L * 100L, ((Number) after.get("last_estimate_bytes")).longValue());

                // The fixture whose body carries an inverted index keeps
                // the fts kind and its document set wording.
                ResponseException indexedRefusal = expectThrows(
                    ResponseException.class,
                    () -> postJson(
                        "/" + withIndex.indexName() + "/_search?request_cache=false",
                        "{\"size\":5,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}"
                    )
                );
                String indexedBody = readAll(indexedRefusal.getResponse());
                assertEquals(
                    indexedBody,
                    RestStatus.TOO_MANY_REQUESTS.getStatus(),
                    indexedRefusal.getResponse().getStatusLine().getStatusCode()
                );
                assertTrue(indexedBody, indexedBody.contains("[lance_admission] fts estimate"));
                assertTrue(indexedBody, indexedBody.contains("inverted index document set of"));
                assertFalse(indexedBody, indexedBody.contains("without an inverted index"));
                assertEquals(rejections(after, "fts") + 1, rejections(admissionStats(), "fts"));
            } finally {
                updateClusterSetting("plugins.lance.test.admission_available_memory", null);
                updateClusterSetting("plugins.lance.test.index_cache_shard_share", null);
            }
            String restored = readAll(postJson("/" + declared + "/_search?request_cache=false", MATCH_THE));
            assertEquals(5, extractIntPath(restored, "hits", "total", "value"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + declared));
            } catch (Exception ignored) {
                // best-effort cleanup; the base class wipes indices too
            }
            deleteRecursively(scratchDir);
        }
    }

    public void testDisabledGateRefusesTheFlatScanWith400AndLeavesTheIndexedColumnAlone() throws Exception {
        String suffix = "lancetextoff-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        String tableUri = LanceTableFactory.writeEnglishTextTable(scratchDir, tableName);
        String declared = tableName;
        String indexedMatch = "{\"size\":5,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}";
        try (LanceTestCluster withIndex = LanceTestCluster.setUp(16, "lancetextidxoff")) {
            attach(tableUri, declared, "{\"body\":{\"type\":\"lance_text\"}}");
            String admitted = readAll(postJson("/" + declared + "/_search?request_cache=false", MATCH_THE));
            assertEquals(5, extractIntPath(admitted, "hits", "total", "value"));

            updateClusterSetting("plugins.lance.admission.enabled", "false");
            try {
                Map<String, Object> before = admissionStats();
                assertEquals(before.toString(), false, before.get("enabled"));
                // Off, the gate cannot bound the flat scan, so the match
                // on the declared column is refused as a client error
                // naming the setting; nothing is counted as a 429.
                ResponseException refused = expectThrows(
                    ResponseException.class,
                    () -> postJson("/" + declared + "/_search?request_cache=false", MATCH_THE)
                );
                String body = readAll(refused.getResponse());
                assertEquals(body, RestStatus.BAD_REQUEST.getStatus(), refused.getResponse().getStatusLine().getStatusCode());
                assertEquals(body, "illegal_argument_exception", stringPath(body, "error", "type"));
                assertTrue(
                    body,
                    body.contains(
                        "[lance_admission] full text scan without an inverted index on column [body] is refused while "
                            + "plugins.lance.admission.enabled is false"
                    )
                );
                assertTrue(body, body.contains("create an inverted index on the column with the table's writer, or enable admission"));
                Map<String, Object> after = admissionStats();
                assertEquals(after.toString(), rejections(before, "fts_flat"), rejections(after, "fts_flat"));

                // The column that carries an inverted index answers with
                // the gate off, as every other kind does.
                String indexed = readAll(postJson("/" + withIndex.indexName() + "/_search?request_cache=false", indexedMatch));
                assertEquals(indexed, 5, hitsOf(indexed).size());
            } finally {
                updateClusterSetting("plugins.lance.admission.enabled", null);
            }
            // Back on, the flat scan is admitted again.
            String restored = readAll(postJson("/" + declared + "/_search?request_cache=false", MATCH_THE));
            assertEquals(5, extractIntPath(restored, "hits", "total", "value"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + declared));
            } catch (Exception ignored) {
                // best-effort cleanup; the base class wipes indices too
            }
            deleteRecursively(scratchDir);
        }
    }

    public void testKeywordSubFieldNextToTheOverrideServesExactMatchAndTerms() throws Exception {
        String suffix = "lancetextsub-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        String tableUri = LanceTableFactory.writeEnglishTextTable(scratchDir, tableName);
        try {
            attach(tableUri, tableName, "{\"body\":{\"type\":\"lance_text\",\"fields\":{\"raw\":{\"type\":\"keyword\"}}}}");
            String mapping = readAll(client().performRequest(new Request("GET", "/" + tableName + "/_mapping")));
            assertTrue(mapping, mapping.contains("\"body\":{\"type\":\"lance_text\""));
            assertTrue(mapping, mapping.contains("\"fields\":{\"raw\":{\"type\":\"keyword\""));

            // The full text match still answers on the base column.
            assertEquals(ROWS_WITH_THE, sortedSourceIds(readAll(postJson("/" + tableName + "/_search", MATCH_THE))));

            // The sub-field is exact: the whole sentence finds its row.
            String term = readAll(
                postJson(
                    "/" + tableName + "/_search",
                    "{\"size\":10,\"query\":{\"term\":{\"body.raw\":\"" + LanceTableFactory.ENGLISH_SENTENCES[1] + "\"}}}"
                )
            );
            assertEquals("term body.raw finds row 1: " + term, List.of(1), sortedSourceIds(term));

            // Five distinct sentences (row 4 is null) make five buckets.
            String agg = readAll(
                postJson(
                    "/" + tableName + "/_search?request_cache=false",
                    "{\"size\":0,\"aggs\":{\"per_body\":{\"terms\":{\"field\":\"body.raw\",\"size\":10}}}}"
                )
            );
            assertEquals("terms on body.raw opens one bucket per sentence: " + agg, 5, bucketCount(agg, "per_body"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + tableName));
            } catch (Exception ignored) {
                // best-effort cleanup; the base class wipes indices too
            }
            deleteRecursively(scratchDir);
        }
    }

    public void testOverrideOnAListOfUtf8ColumnIsRefused() throws Exception {
        String suffix = "lancetextlist-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        String tableUri = LanceTableFactory.writeIpTable(scratchDir, tableName);
        try {
            ResponseException refused = expectThrows(
                ResponseException.class,
                () -> attach(tableUri, tableName, "{\"addrs\":{\"type\":\"lance_text\"}}")
            );
            String body = readAll(refused.getResponse());
            assertEquals(body, RestStatus.BAD_REQUEST.getStatus(), refused.getResponse().getStatusLine().getStatusCode());
            assertTrue(body, body.contains("type=lance_text] needs a Utf8 column"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + tableName));
            } catch (Exception ignored) {
                // best-effort cleanup; the base class wipes indices too
            }
            deleteRecursively(scratchDir);
        }
    }

    public void testExplainAndProfileNameTheFlatScanAndThenTheIndex() throws Exception {
        String suffix = "lancetextexp-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        String tableUri = LanceTableFactory.writeEnglishTextTable(scratchDir, tableName);
        String declared = tableName;
        String profiled = "{\"size\":10,\"profile\":true,\"query\":{\"match\":{\"body\":\"the\"}}}";
        try {
            attach(tableUri, declared, "{\"body\":{\"type\":\"lance_text\"}}");

            // Without an index the pushed match says so in the plan text
            // and in the fragment plan, and the executor reports the
            // flat kind the gate judged the request under.
            String flat = explain(declared, MATCH_THE);
            String flatPhysical = stringPath(flat, "physical");
            assertTrue("the pushed match names the missing index: " + flatPhysical, flatPhysical.contains("columns=[body], index=none,"));
            assertEquals("lance_match", stringPath(flat, "fragment_plan", "lance_clause"));
            assertEquals("none", stringPath(flat, "fragment_plan", "fts_index"));
            String flatSearch = readAll(postJson("/" + declared + "/_search?request_cache=false", profiled));
            assertEquals(5, extractIntPath(flatSearch, "hits", "total", "value"));
            assertEquals(List.of("fts_flat"), admissionKinds(flatSearch));

            // The writer builds the index; the declared index follows the
            // table's new version on sync and the same body now reports
            // the inverted index on both endpoints.
            LanceTableFactory.createFtsIndex(tableUri, "body", "simple", true);
            Map<String, Object> outcome = sync(declared);
            assertEquals(outcome.toString(), true, outcome.get("checked"));
            String indexed = explain(declared, MATCH_THE);
            String indexedPhysical = stringPath(indexed, "physical");
            assertTrue("the pushed match names the index: " + indexedPhysical, indexedPhysical.contains("columns=[body], index=inverted,"));
            assertEquals("inverted", stringPath(indexed, "fragment_plan", "fts_index"));
            String indexedSearch = readAll(postJson("/" + declared + "/_search?request_cache=false", profiled));
            assertEquals(List.of("fts"), admissionKinds(indexedSearch));

            // A body without a full text clause is gated by nothing and
            // reports no kind.
            String ungated = readAll(
                postJson("/" + declared + "/_search?request_cache=false", "{\"size\":1,\"profile\":true,\"query\":{\"term\":{\"id\":1}}}")
            );
            assertEquals(List.of(), admissionKinds(ungated));
            String noFts = explain(declared, "{\"size\":1,\"query\":{\"term\":{\"id\":1}}}");
            assertFalse("no full text clause, no fts_index: " + noFts, noFts.contains("fts_index"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + declared));
            } catch (Exception ignored) {
                // best-effort cleanup; the base class wipes indices too
            }
            deleteRecursively(scratchDir);
        }
    }

    private static String explain(String indexName, String body) throws IOException {
        Request request = new Request("GET", "/_plugins/_lance/explain/" + indexName);
        request.setJsonEntity(body);
        return readAll(client().performRequest(request));
    }

    /** The {@code admission_kind} every node's {@code query} block of {@code profile.lance} reports, in node id order; a node without one adds nothing. */
    @SuppressWarnings("unchecked")
    private static List<String> admissionKinds(String searchBody) {
        Map<String, Object> profile = (Map<String, Object>) parseJson(searchBody).get("profile");
        assertNotNull("the response carries a profile: " + searchBody, profile);
        Map<String, Object> lance = (Map<String, Object>) profile.get("lance");
        Map<String, Object> nodes = (Map<String, Object>) lance.get("nodes");
        assertNotNull("the profile carries the nodes: " + searchBody, nodes);
        List<String> kinds = new ArrayList<>();
        for (Object node : nodes.values()) {
            Map<String, Object> query = (Map<String, Object>) ((Map<String, Object>) node).get("query");
            if (query.containsKey("admission_kind")) {
                kinds.add((String) query.get("admission_kind"));
            }
        }
        return kinds;
    }

    private static void attach(String tableUri, String indexName, String overridesJson) throws IOException {
        String body = "{\"table\":\""
            + tableUri
            + "\",\"name\":\""
            + indexName
            + "\""
            + (overridesJson == null ? "" : ",\"overrides\":" + overridesJson)
            + "}";
        Response attach = postJson("/_plugins/_lance/attach", body);
        assertEquals("attach failed: " + readAll(attach), RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
        ensureGreen(indexName);
    }

    private static Map<String, Object> sync(String indexName) throws IOException {
        Response response = client().performRequest(new Request("POST", "/_plugins/_lance/sync/" + indexName));
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
        return parseJson(readAll(response));
    }

    private static String indexUuid(String indexName) throws IOException {
        String settings = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_settings")));
        return stringPath(settings, indexName, "settings", "index", "uuid");
    }

    /** The sorted {@code _source.id} values of every hit. */
    private static List<Integer> sortedSourceIds(String searchBody) throws IOException {
        List<Integer> ids = new ArrayList<>();
        for (Map<String, Object> hit : hitsOf(searchBody)) {
            @SuppressWarnings("unchecked")
            Map<String, Object> source = (Map<String, Object>) hit.get("_source");
            ids.add(((Number) source.get("id")).intValue());
        }
        Collections.sort(ids);
        return ids;
    }

    @SuppressWarnings("unchecked")
    private static int bucketCount(String searchBody, String aggregation) {
        Map<String, Object> aggregations = (Map<String, Object>) parseJson(searchBody).get("aggregations");
        Map<String, Object> agg = (Map<String, Object>) aggregations.get(aggregation);
        return ((List<Object>) agg.get("buckets")).size();
    }

    /** The single node's {@code admission} stats block. */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> admissionStats() throws IOException {
        String stats = readAll(client().performRequest(new Request("GET", "/_plugins/_lance/stats")));
        Map<String, Object> nodes = (Map<String, Object>) parseJson(stats).get("nodes");
        assertEquals("single node cluster: " + stats, 1, nodes.size());
        Map<String, Object> node = (Map<String, Object>) nodes.values().iterator().next();
        return (Map<String, Object>) node.get("admission");
    }

    @SuppressWarnings("unchecked")
    private static long rejections(Map<String, Object> admission, String kind) {
        Map<String, Object> rejections = (Map<String, Object>) admission.get("rejections");
        return ((Number) rejections.get(kind)).longValue();
    }

    private static void updateClusterSetting(String key, String jsonValue) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"transient\":{\"" + key + "\":" + (jsonValue == null ? "null" : jsonValue) + "}}");
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }
}
