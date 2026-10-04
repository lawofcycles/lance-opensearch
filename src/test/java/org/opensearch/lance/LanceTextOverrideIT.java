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
                // naming the setting; nothing is counted as a 429 and the
                // refusal is counted under refused_while_disabled.
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
                assertEquals(after.toString(), refusedWhileDisabled(before, "fts_flat") + 1, refusedWhileDisabled(after, "fts_flat"));
                assertEquals(after.toString(), refusedWhileDisabled(before, "fts"), refusedWhileDisabled(after, "fts"));

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

    public void testHidingWrapperFiltersTheFlatPathHidesAColumnAndWithholdsStats() throws Exception {
        // The test hook installs a reader wrapper shaped like the security
        // plugin's DLS / FLS reader on the indexes under the prefix: rows
        // whose rating is below 200 are hidden and the body column is
        // dropped from the field infos. The hint fixture's rating is
        // (i * 37) % 1000 with a null every fifth row, so of twelve rows
        // the visible ones are 6, 7, 8, 10 and 11; category is "c" + (i % 3)
        // with a null every fourth row, so the visible rows with a
        // category are 6 (c0), 8 (c2) and 10 (c1). The same table attached
        // outside the prefix answers unfiltered next to it.
        String suffix = "lancetextdls-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        String tableUri = LanceTableFactory.writeHintFixtureTable(scratchDir, tableName, 2, 6);
        String wrapped = "wrapped-" + suffix;
        String plain = "plain-" + suffix;
        String overrides = "{\"category\":{\"type\":\"lance_text\"}}";
        String matchAll = "{\"size\":10,\"query\":{\"match_all\":{}}}";
        String flatMatch = "{\"size\":10,\"profile\":true,\"query\":{\"match\":{\"category\":\"c0 c1 c2\"}}}";
        String termVisible = "{\"size\":10,\"query\":{\"term\":{\"rating\":222}}}";
        String termHidden = "{\"size\":10,\"query\":{\"term\":{\"rating\":37}}}";
        String termCategory = "{\"size\":10,\"query\":{\"term\":{\"category\":\"c0\"}}}";
        String matchHiddenColumn = "{\"size\":10,\"query\":{\"match\":{\"body\":\"hello\"}}}";
        String countOnly = "{\"size\":0,\"query\":{\"match_all\":{}}}";
        updateClusterSetting("plugins.lance.test.hiding_wrapper_index_prefix", "\"" + wrapped + ":body:rating:200\"");
        try {
            attach(tableUri, wrapped, overrides);
            attach(tableUri, plain, overrides);
            String wrappedMapping = readAll(client().performRequest(new Request("GET", "/" + wrapped + "/_mapping")));
            assertTrue(wrappedMapping, wrappedMapping.contains("\"category\":{\"type\":\"lance_text\""));

            // The DLS shaped filter: the page and the total count only the
            // visible rows, on every path a wrapper changes.
            String wrappedAll = readAll(postJson("/" + wrapped + "/_search?request_cache=false", matchAll));
            assertEquals("match_all sees the visible rows only: " + wrappedAll, 5, extractIntPath(wrappedAll, "hits", "total", "value"));
            assertEquals(List.of(6, 7, 8, 10, 11), sortedSourceIds(wrappedAll));
            String plainAll = readAll(postJson("/" + plain + "/_search?request_cache=false", matchAll));
            assertEquals(12, extractIntPath(plainAll, "hits", "total", "value"));
            String wrappedCount = readAll(client().performRequest(new Request("GET", "/" + wrapped + "/_count")));
            assertEquals("_count sees the visible rows only: " + wrappedCount, 5, extractIntPath(wrappedCount, "count"));

            // The flat BM25 scan of the declared column runs over every
            // row of the table; the wrapper hides its hits afterwards, so
            // the match answers the visible rows with a category. The
            // single node renders the page on the query round (no fetch
            // round trip) and the gate judged the flat kind.
            Map<String, Object> fetchBefore = fetchCacheStats();
            assertEquals(fetchBefore.toString(), true, fetchBefore.get("enabled"));
            String wrappedFlat = readAll(postJson("/" + wrapped + "/_search?request_cache=false", flatMatch));
            assertEquals(
                "the flat match sees the visible rows only: " + wrappedFlat,
                3,
                extractIntPath(wrappedFlat, "hits", "total", "value")
            );
            assertEquals(List.of(6, 8, 10), sortedSourceIds(wrappedFlat));
            assertEquals(List.of("fts_flat"), admissionKinds(wrappedFlat));
            assertEquals(0, fetchRoundTrips(wrappedFlat));
            String plainFlat = readAll(postJson("/" + plain + "/_search?request_cache=false", flatMatch));
            assertEquals(9, extractIntPath(plainFlat, "hits", "total", "value"));
            // The rows behind the wrapped page bypass the fetch cache: a
            // row rendered under one wrapper must not serve the next
            // request from the cache.
            Map<String, Object> fetchAfter = fetchCacheStats();
            assertTrue(
                "the wrapped rows are counted as skipped by the fetch cache: " + fetchBefore + " -> " + fetchAfter,
                ((Number) fetchAfter.get("skipped")).longValue() > ((Number) fetchBefore.get("skipped")).longValue()
            );

            // A term on a visible row answers it, a term on a hidden row
            // answers nothing, and a term on the declared column (a Lance
            // match) sees the visible rows only.
            assertEquals(List.of(6), sortedSourceIds(readAll(postJson("/" + wrapped + "/_search?request_cache=false", termVisible))));
            String hidden = readAll(postJson("/" + wrapped + "/_search?request_cache=false", termHidden));
            assertEquals("a hidden row is not found: " + hidden, 0, extractIntPath(hidden, "hits", "total", "value"));
            assertEquals(List.of(1), sortedSourceIds(readAll(postJson("/" + plain + "/_search?request_cache=false", termHidden))));
            assertEquals(List.of(6), sortedSourceIds(readAll(postJson("/" + wrapped + "/_search?request_cache=false", termCategory))));
            assertEquals(List.of(0, 6, 9), sortedSourceIds(readAll(postJson("/" + plain + "/_search?request_cache=false", termCategory))));

            // The FLS shaped hiding: a full text clause on the column the
            // wrapper drops from the field infos contributes no hit, so
            // the clause cannot probe the hidden data, while the plain
            // index answers every row.
            String hiddenColumn = readAll(postJson("/" + wrapped + "/_search?request_cache=false", matchHiddenColumn));
            assertEquals(
                "a match on the hidden column finds nothing: " + hiddenColumn,
                0,
                extractIntPath(hiddenColumn, "hits", "total", "value")
            );
            String visibleColumn = readAll(postJson("/" + plain + "/_search?request_cache=false", matchHiddenColumn));
            assertEquals(12, extractIntPath(visibleColumn, "hits", "total", "value"));

            // The coordinator result cache does not take an answer the
            // wrapper shaped: a size 0 body against the wrapped index is
            // skipped, the same body against the plain index is stored,
            // and explain names the reason.
            Map<String, Object> cacheBefore = requestCacheStats();
            String wrappedCountOnly = readAll(postJson("/" + wrapped + "/_search", countOnly));
            assertEquals(5, extractIntPath(wrappedCountOnly, "hits", "total", "value"));
            Map<String, Object> cacheAfterWrapped = requestCacheStats();
            assertEquals(
                "the wrapped request is skipped: " + cacheAfterWrapped,
                ((Number) cacheBefore.get("skipped")).longValue() + 1,
                ((Number) cacheAfterWrapped.get("skipped")).longValue()
            );
            assertEquals(((Number) cacheBefore.get("misses")).longValue(), ((Number) cacheAfterWrapped.get("misses")).longValue());
            readAll(postJson("/" + plain + "/_search", countOnly));
            Map<String, Object> cacheAfterPlain = requestCacheStats();
            assertEquals(((Number) cacheAfterWrapped.get("skipped")).longValue(), ((Number) cacheAfterPlain.get("skipped")).longValue());
            assertEquals(
                "the plain request is a miss that stores: " + cacheAfterPlain,
                ((Number) cacheAfterWrapped.get("misses")).longValue() + 1,
                ((Number) cacheAfterPlain.get("misses")).longValue()
            );
            String explained = explain(wrapped, countOnly);
            assertEquals(explained, "false", String.valueOf(parseJson(explained).get("cacheable")));
            assertEquals(explained, "dls", stringPath(explained, "cacheable_reason"));

            // The stats report withholds the figures the wrapper filters
            // for the wrapped index and keeps them for the plain one.
            Map<String, Object> wrappedStats = indexStats(wrapped);
            assertFalse("rows are withheld: " + wrappedStats, wrappedStats.containsKey("rows"));
            assertFalse("shard_reader_rows are withheld: " + wrappedStats, wrappedStats.containsKey("shard_reader_rows"));
            assertFalse("index types are withheld: " + wrappedStats, wrappedStats.containsKey("index_types"));
            assertEquals(wrappedStats.toString(), false, wrappedStats.get("lucene_bound_exceeded"));
            Map<String, Object> plainStats = indexStats(plain);
            assertEquals(plainStats.toString(), 12, ((Number) plainStats.get("rows")).intValue());
            assertEquals(plainStats.toString(), 12, ((Number) plainStats.get("shard_reader_rows")).intValue());
        } finally {
            updateClusterSetting("plugins.lance.test.hiding_wrapper_index_prefix", null);
            for (String index : List.of(wrapped, plain)) {
                try {
                    client().performRequest(new Request("DELETE", "/" + index));
                } catch (Exception ignored) {
                    // best-effort cleanup; the base class wipes indices too
                }
            }
            deleteRecursively(scratchDir);
        }
    }

    /** {@code profile.lance.coordinator.fetch_round_trips} of a response. */
    @SuppressWarnings("unchecked")
    private static int fetchRoundTrips(String searchBody) {
        Map<String, Object> profile = (Map<String, Object>) parseJson(searchBody).get("profile");
        assertNotNull("the response carries a profile: " + searchBody, profile);
        Map<String, Object> lance = (Map<String, Object>) profile.get("lance");
        Map<String, Object> coordinator = (Map<String, Object>) lance.get("coordinator");
        assertNotNull("the profile carries the coordinator: " + searchBody, coordinator);
        return ((Number) coordinator.get("fetch_round_trips")).intValue();
    }

    /** The single node's {@code indices.<index>} stats block. */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> indexStats(String indexName) throws IOException {
        Map<String, Object> indices = (Map<String, Object>) nodeStats().get("indices");
        Map<String, Object> stats = (Map<String, Object>) indices.get(indexName);
        assertNotNull("stats for " + indexName + ": " + indices.keySet(), stats);
        return stats;
    }

    /** The single node's {@code request_cache} stats block. */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> requestCacheStats() throws IOException {
        return (Map<String, Object>) nodeStats().get("request_cache");
    }

    /** The single node's {@code fetch_cache} stats block. */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> fetchCacheStats() throws IOException {
        return (Map<String, Object>) nodeStats().get("fetch_cache");
    }

    /** The single node's stats object. */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> nodeStats() throws IOException {
        String stats = readAll(client().performRequest(new Request("GET", "/_plugins/_lance/stats")));
        Map<String, Object> nodes = (Map<String, Object>) parseJson(stats).get("nodes");
        assertEquals("single node cluster: " + stats, 1, nodes.size());
        return (Map<String, Object>) nodes.values().iterator().next();
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
        return (Map<String, Object>) nodeStats().get("admission");
    }

    @SuppressWarnings("unchecked")
    private static long rejections(Map<String, Object> admission, String kind) {
        Map<String, Object> rejections = (Map<String, Object>) admission.get("rejections");
        return ((Number) rejections.get(kind)).longValue();
    }

    @SuppressWarnings("unchecked")
    private static long refusedWhileDisabled(Map<String, Object> admission, String kind) {
        Map<String, Object> refused = (Map<String, Object>) admission.get("refused_while_disabled");
        assertNotNull("the admission block carries refused_while_disabled: " + admission, refused);
        return ((Number) refused.get(kind)).longValue();
    }

    private static void updateClusterSetting(String key, String jsonValue) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"transient\":{\"" + key + "\":" + (jsonValue == null ? "null" : jsonValue) + "}}");
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }
}
