/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Full-text query types backed by the Lance inverted index: {@code
 * lance_match}, {@code lance_match_phrase}, {@code lance_multi_match},
 * {@code lance_fts_boost} and {@code lance_fts_bool}, including their
 * validation errors. Also the {@code tokenizer} option of
 * {@code POST /_lance/build_indexes/{index}}, checked against Japanese
 * text where the choice of tokenizer decides whether a one-word query
 * matches at all.
 */
public class LanceFtsQueryIT extends LanceRestTestCase {

    public void testAttachAndMatch() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "attachAndMatch")) {
            String indexName = fixture.indexName();

            Response search = postJson("/" + indexName + "/_search", "{\"query\":{\"match\":{\"body\":\"hello\"}}}");
            String body = readAll(search);
            // Even rows say "hello lance i", odd rows "quick brown fox i".
            int totalHits = extractIntPath(body, "hits", "total", "value");
            assertEquals("expected 8 hits (even rows), saw response: " + body, 8, totalHits);
        }
    }

    public void testLanceMatchAcrossSeveralFragmentsOnOneNode() throws Exception {
        // 12 rows written 4 per file give fragments 0, 1 and 2. On the
        // single-node cluster the executor holds all three, so both the
        // hits scan and the count scan run without a fragmentIds
        // restriction; the assertions pin the row set, the per-fragment
        // _id layout and the score order that path must reproduce.
        try (LanceTestCluster fixture = LanceTestCluster.setUpMultiFragment(12, 4, "lmatchmultifrag")) {
            String indexName = fixture.indexName();

            String helloBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}")
            );
            assertEquals("even rows across three fragments: " + helloBody, 6, extractIntPath(helloBody, "hits", "total", "value"));
            List<Map<String, Object>> hits = hitsOf(helloBody);
            assertEquals(
                "row i lives at fragment i / 4, offset i % 4",
                Set.of("0-0", "0-2", "1-0", "1-2", "2-0", "2-2"),
                new HashSet<>(idsOf(hits))
            );
            List<Double> scores = scoresOf(helloBody);
            for (int i = 1; i < scores.size(); i++) {
                assertTrue("_score must be non-increasing, saw " + scores, scores.get(i - 1) >= scores.get(i));
            }
            assertTrue("BM25 scores must be positive, saw " + scores, scores.get(scores.size() - 1) > 0d);

            // A token unique to row 4 (body "hello lance 4") pins the
            // hit to fragment 1, offset 0.
            String singleBody = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"4\"}}}")
            );
            assertEquals("token 4 appears in one row only: " + singleBody, 1, extractIntPath(singleBody, "hits", "total", "value"));
            assertEquals(List.of("1-0"), idsOf(hitsOf(singleBody)));

            // A bounded size still reports the full total.
            String pagedBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":2,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}")
            );
            assertEquals(6, extractIntPath(pagedBody, "hits", "total", "value"));
            assertEquals(2, hitsOf(pagedBody).size());

            // _count is answered by the shard engine's reader over the
            // same three fragments and must agree with hits.total.value.
            String countBody = readAll(
                postJson("/" + indexName + "/_count", "{\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}")
            );
            assertEquals("_count for hello: " + countBody, 6, extractIntPath(countBody, "count"));
            String singleCountBody = readAll(
                postJson("/" + indexName + "/_count", "{\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"4\"}}}")
            );
            assertEquals("_count for token 4: " + singleCountBody, 1, extractIntPath(singleCountBody, "count"));
        }
    }

    private static List<Double> scoresOf(String searchBody) throws IOException {
        try (XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, searchBody)) {
            Map<String, Object> map = parser.map();
            @SuppressWarnings("unchecked")
            List<Object> hits = (List<Object>) ((Map<String, Object>) map.get("hits")).get("hits");
            List<Double> scores = new ArrayList<>(hits.size());
            for (Object hit : hits) {
                Object score = ((Map<?, ?>) hit).get("_score");
                assertTrue("_score must be numeric, saw " + score, score instanceof Number);
                scores.add(((Number) score).doubleValue());
            }
            return scores;
        }
    }

    public void testLanceMatchPhraseHonoursPhraseOrder() throws Exception {
        // The fixture's FTS index is built with positions, so phrase
        // order matters: "hello lance" hits the eight even rows and
        // "lance hello" hits none.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lmphraseorder")) {
            String indexName = fixture.indexName();

            Response ordered = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_match_phrase\":{\"field\":\"body\",\"query\":\"hello lance\"}}}"
            );
            int orderedHits = extractIntPath(readAll(ordered), "hits", "total", "value");
            assertEquals("expected 8 hits for 'hello lance' phrase", 8, orderedHits);

            Response reversed = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_match_phrase\":{\"field\":\"body\",\"query\":\"lance hello\"}}}"
            );
            int reversedHits = extractIntPath(readAll(reversed), "hits", "total", "value");
            assertEquals("expected 0 hits for 'lance hello' reversed phrase", 0, reversedHits);
        }
    }

    public void testLanceMatchPhraseSlopBridgesGap() throws Exception {
        // "quick fox" needs slop 1 to bridge "brown".
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lmphraseslop")) {
            String indexName = fixture.indexName();

            Response strict = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_match_phrase\":{\"field\":\"body\",\"query\":\"quick fox\"}}}"
            );
            int strictHits = extractIntPath(readAll(strict), "hits", "total", "value");
            assertEquals("expected 0 hits for tight 'quick fox' phrase", 0, strictHits);

            Response withSlop = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_match_phrase\":{\"field\":\"body\",\"query\":\"quick fox\",\"slop\":1}}}"
            );
            int slopHits = extractIntPath(readAll(withSlop), "hits", "total", "value");
            assertEquals("expected 8 hits for 'quick fox' phrase with slop=1", 8, slopHits);
        }
    }

    public void testLanceMatchAndOperatorRestrictsToDocumentsMatchingAllTokens() throws Exception {
        // "hello quick": every row has one of the tokens (OR hits 16),
        // no row has both (AND hits 0).
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lmatchand")) {
            String indexName = fixture.indexName();

            Response orQuery = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello quick\"}}}"
            );
            int orHits = extractIntPath(readAll(orQuery), "hits", "total", "value");
            assertEquals("expected 16 hits for OR 'hello quick'", 16, orHits);

            Response andQuery = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello quick\",\"operator\":\"and\"}}}"
            );
            int andHits = extractIntPath(readAll(andQuery), "hits", "total", "value");
            assertEquals("expected 0 hits for AND 'hello quick'", 0, andHits);

            Response andSameRow = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello lance\",\"operator\":\"and\"}}}"
            );
            int andSameRowHits = extractIntPath(readAll(andSameRow), "hits", "total", "value");
            assertEquals("expected 8 hits for AND 'hello lance'", 8, andSameRowHits);
        }
    }

    public void testLanceMatchFuzzinessAllowsSingleEdit() throws Exception {
        // "helo" is one edit from "hello": zero hits without fuzziness,
        // the eight even rows with fuzziness 1.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lmatchfuzz")) {
            String indexName = fixture.indexName();

            Response strict = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"helo\"}}}"
            );
            int strictHits = extractIntPath(readAll(strict), "hits", "total", "value");
            assertEquals("expected 0 hits for exact 'helo'", 0, strictHits);

            Response fuzzy = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"helo\",\"fuzziness\":1}}}"
            );
            int fuzzyHits = extractIntPath(readAll(fuzzy), "hits", "total", "value");
            assertEquals("expected 8 hits for fuzzy 'helo' (edit distance 1 to hello)", 8, fuzzyHits);
        }
    }

    public void testLanceMatchRejectsUnknownField() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "lmatchnofield")) {
            String indexName = fixture.indexName();
            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_match\":{\"field\":\"noSuchField\",\"query\":\"hello\"}}}"
                )
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for unknown field, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about noSuchField: " + body, body.contains("noSuchField"));
        }
    }

    public void testLanceMatchRejectsScalarField() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "lmatchscalar")) {
            String indexName = fixture.indexName();
            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson("/" + indexName + "/_search", "{\"query\":{\"lance_match\":{\"field\":\"id\",\"query\":\"hello\"}}}")
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for scalar field, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about non-lance_text: " + body, body.contains("lance_text"));
        }
    }

    public void testLanceMultiMatchHitsEitherField() throws Exception {
        // Even rows: body "hello lance i", title "sunny morning i". Odd
        // rows: body "quick brown fox i", title "cloudy morning i".
        // "hello cloudy" over both fields hits every row.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lmmboth")) {
            String indexName = fixture.indexName();

            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_multi_match\":{\"fields\":[\"body\",\"title\"],\"query\":\"hello cloudy\"}}}"
            );
            int hits = extractIntPath(readAll(search), "hits", "total", "value");
            assertEquals("expected 16 hits across body+title, saw response above", 16, hits);
        }
    }

    public void testLanceMultiMatchLimitsToListedFields() throws Exception {
        // "morning" only appears in title.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lmmscope")) {
            String indexName = fixture.indexName();

            Response bodyOnly = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_multi_match\":{\"fields\":[\"body\"],\"query\":\"morning\"}}}"
            );
            int bodyHits = extractIntPath(readAll(bodyOnly), "hits", "total", "value");
            assertEquals("expected 0 hits when only body is searched", 0, bodyHits);

            Response both = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_multi_match\":{\"fields\":[\"body\",\"title\"],\"query\":\"morning\"}}}"
            );
            int bothHits = extractIntPath(readAll(both), "hits", "total", "value");
            assertEquals("expected 16 hits when title is included", 16, bothHits);
        }
    }

    public void testLanceMultiMatchAndOperator() throws Exception {
        // "hello cloudy" over both fields: OR hits 16, AND hits 0 because
        // no row has both tokens.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lmmand")) {
            String indexName = fixture.indexName();

            Response or = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_multi_match\":{\"fields\":[\"body\",\"title\"],\"query\":\"hello cloudy\"}}}"
            );
            int orHits = extractIntPath(readAll(or), "hits", "total", "value");
            assertEquals("expected 16 hits for OR 'hello cloudy'", 16, orHits);

            Response and = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_multi_match\":{\"fields\":[\"body\",\"title\"],\"query\":\"hello cloudy\",\"operator\":\"and\"}}}"
            );
            int andHits = extractIntPath(readAll(and), "hits", "total", "value");
            assertEquals("expected 0 hits for AND 'hello cloudy' (no row has both)", 0, andHits);
        }
    }

    public void testLanceMultiMatchWithBoostsSmoke() throws Exception {
        // Per-field boosts parse and execute.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lmmboosts")) {
            String indexName = fixture.indexName();

            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_multi_match\":{\"fields\":[\"body\",\"title\"]," + "\"query\":\"hello sunny\",\"boosts\":[2.0,1.0]}}}"
            );
            int hits = extractIntPath(readAll(search), "hits", "total", "value");
            assertEquals("expected 8 hits for even rows matching hello+sunny", 8, hits);
        }
    }

    public void testLanceMultiMatchRejectsUnknownField() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "lmmnofield")) {
            String indexName = fixture.indexName();
            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_multi_match\":{\"fields\":[\"body\",\"noSuchField\"],\"query\":\"hello\"}}}"
                )
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for unknown field, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about noSuchField: " + body, body.contains("noSuchField"));
        }
    }

    public void testLanceMultiMatchRejectsScalarField() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "lmmscalar")) {
            String indexName = fixture.indexName();
            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_multi_match\":{\"fields\":[\"body\",\"id\"],\"query\":\"hello\"}}}"
                )
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for scalar field, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about non-lance_text: " + body, body.contains("lance_text"));
        }
    }

    public void testLanceFtsBoostReturnsPositiveMatches() throws Exception {
        // The positive clause defines the hit set; a disjoint negative
        // clause must not remove any.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lfbboost")) {
            String indexName = fixture.indexName();

            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_fts_boost\":{"
                    + "\"positive\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},"
                    + "\"negative\":{\"lance_match\":{\"field\":\"body\",\"query\":\"fox\"}},"
                    + "\"negative_boost\":0.1}}}"
            );
            int hits = extractIntPath(readAll(search), "hits", "total", "value");
            assertEquals("expected 8 hits from positive 'hello'", 8, hits);
        }
    }

    public void testLanceFtsBoostPenalisesOverlappingNegative() throws Exception {
        // Positive and negative clauses match the same rows; the top
        // score with negative_boost 0.1 must be below the unboosted one.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lfbpenalise")) {
            String indexName = fixture.indexName();

            Response baseline = postJson(
                "/" + indexName + "/_search",
                "{\"size\":1,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}"
            );
            String baselineBody = readAll(baseline);
            int baselineHits = extractIntPath(baselineBody, "hits", "total", "value");
            assertEquals("baseline expects 8 hits", 8, baselineHits);
            double baselineScore = extractDoublePath(baselineBody, "hits", "hits", "0", "_score");

            Response boosted = postJson(
                "/" + indexName + "/_search",
                "{\"size\":1,\"query\":{\"lance_fts_boost\":{"
                    + "\"positive\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},"
                    + "\"negative\":{\"lance_match\":{\"field\":\"body\",\"query\":\"lance\"}},"
                    + "\"negative_boost\":0.1}}}"
            );
            String boostedBody = readAll(boosted);
            int boostedHits = extractIntPath(boostedBody, "hits", "total", "value");
            assertEquals("boosted expects 8 hits (same positive set)", 8, boostedHits);
            double boostedScore = extractDoublePath(boostedBody, "hits", "hits", "0", "_score");
            assertTrue("expected boosted score < baseline (" + boostedScore + " vs " + baselineScore + ")", boostedScore < baselineScore);
        }
    }

    public void testLanceFtsBoostRejectsNonLanceFtsClause() throws Exception {
        // Sub-clauses must be Lance FTS queries; a stock match is
        // rejected rather than mixed into Lance's score composition.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "lfbwrongclause")) {
            String indexName = fixture.indexName();

            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_fts_boost\":{"
                        + "\"positive\":{\"match\":{\"body\":\"hello\"}},"
                        + "\"negative\":{\"lance_match\":{\"field\":\"body\",\"query\":\"stale\"}}}}}"
                )
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for non-Lance-FTS positive, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about Lance FTS query: " + body, body.contains("Lance FTS query"));
        }
    }

    public void testLanceFtsBoolMustClauseFiltersToMatchingRows() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lbmust")) {
            String indexName = fixture.indexName();

            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_fts_bool\":{" + "\"must\":[{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}]}}}"
            );
            int hits = extractIntPath(readAll(search), "hits", "total", "value");
            assertEquals("expected 8 hits from must body:hello", 8, hits);
        }
    }

    public void testLanceFtsBoolMustNotExcludesOverlappingClause() throws Exception {
        // must and must_not target the same rows, leaving none.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lbmustnot")) {
            String indexName = fixture.indexName();

            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_fts_bool\":{"
                    + "\"must\":[{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}],"
                    + "\"must_not\":[{\"lance_match\":{\"field\":\"body\",\"query\":\"lance\"}}]}}}"
            );
            int hits = extractIntPath(readAll(search), "hits", "total", "value");
            assertEquals("expected 0 hits after must_not:lance eliminates every hello match", 0, hits);
        }
    }

    public void testLanceFtsBoolShouldUnionsAcrossClauses() throws Exception {
        // Disjoint should clauses on two fields union to every row.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lbshould")) {
            String indexName = fixture.indexName();

            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_fts_bool\":{"
                    + "\"should\":["
                    + "{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},"
                    + "{\"lance_match\":{\"field\":\"title\",\"query\":\"cloudy\"}}]}}}"
            );
            int hits = extractIntPath(readAll(search), "hits", "total", "value");
            assertEquals("expected 16 hits from union of body:hello ∪ title:cloudy", 16, hits);
        }
    }

    public void testLanceFtsBoolMustAcrossFieldsIntersects() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lbmustintersect")) {
            String indexName = fixture.indexName();

            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_fts_bool\":{"
                    + "\"must\":["
                    + "{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},"
                    + "{\"lance_match\":{\"field\":\"title\",\"query\":\"sunny\"}}]}}}"
            );
            int hits = extractIntPath(readAll(search), "hits", "total", "value");
            assertEquals("expected 8 hits from intersection body:hello ∩ title:sunny", 8, hits);
        }
    }

    public void testLanceFtsBoolRejectsNonLanceFtsClause() throws Exception {
        // Sub-clauses must be Lance FTS queries.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "lbwrongclause")) {
            String indexName = fixture.indexName();

            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_fts_bool\":{" + "\"must\":[{\"match\":{\"body\":\"hello\"}}]}}}"
                )
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for non-Lance-FTS must, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about Lance FTS: " + body, body.contains("Lance FTS"));
        }
    }

    public void testLanceFtsBoolEmptyClausesRejected() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "lbempty")) {
            String indexName = fixture.indexName();

            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson("/" + indexName + "/_search", "{\"query\":{\"lance_fts_bool\":{}}}")
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for empty bool, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about must/should/must_not: " + body, body.contains("must_not"));
        }
    }

    public void testBuildIndexesWithoutFtsColumnsGivesUtf8ColumnABtreeIndex() throws Exception {
        // derive() classifies a Utf8 column without an FTS index as
        // keyword, so a plain build gives it a BTree scalar index and the
        // mapping stays keyword. fts_columns is the only way to ask for
        // an inverted index on such a column.
        try (JapaneseIndex fixture = JapaneseIndex.surface("jabtree")) {
            String indexName = fixture.indexName();
            String build = readAll(postJson("/_lance/build_indexes/" + indexName, "{}"));
            assertTrue("expected no FTS index built: " + build, build.contains("\"fts\":[]"));
            assertTrue("expected text among the scalar builds: " + build, build.contains("\"scalar\":[\"id\",\"text\"]"));

            String mapping = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_mapping")));
            assertTrue("text must stay keyword: " + mapping, mapping.contains("\"text\":{\"type\":\"keyword\""));
        }
    }

    public void testBuildIndexesDefaultTokenizerKeepsJapaneseSentenceWhole() throws Exception {
        // Baseline for the tokenizer option: Lance's simple tokenizer
        // splits on whitespace and punctuation only, so a Japanese
        // sentence without either is indexed as one token. A one-word
        // query finds nothing; the whole sentence finds its own row.
        try (JapaneseIndex fixture = JapaneseIndex.surface("jasimple")) {
            String indexName = fixture.indexName();
            String build = readAll(postJson("/_lance/build_indexes/" + indexName, "{\"fts_columns\":[\"text\"]}"));
            assertTrue("expected text in fts built list: " + build, build.contains("\"fts\":[\"text\"]"));
            assertTrue("text must not also get a BTree index: " + build, build.contains("\"scalar\":[\"id\"]"));
            awaitLanceTextMapping(indexName);

            assertEquals("simple tokenizer must not find 天気 inside a sentence", 0, lanceMatchHits(indexName, "天気"));
            assertEquals("simple tokenizer matches the whole sentence as one token", 1, lanceMatchHits(indexName, "東京の天気は晴れです"));
        }
    }

    public void testBuildIndexesWithIcuTokenizerMatchesJapaneseWords() throws Exception {
        // icu is compiled into the Lance native library with its own
        // segmentation data, so it needs no dictionary download and runs
        // on every CI host. 天気 sits in rows 0 and 1, 東京 in rows 0 and 3,
        // 京都 in row 2.
        try (JapaneseIndex fixture = JapaneseIndex.surface("jaicu")) {
            String indexName = fixture.indexName();
            String build = readAll(postJson("/_lance/build_indexes/" + indexName, "{\"fts_columns\":[\"text\"],\"tokenizer\":\"icu\"}"));
            assertTrue("expected text in fts built list: " + build, build.contains("\"fts\":[\"text\"]"));
            awaitLanceTextMapping(indexName);

            assertEquals("icu must split 天気 out of the sentences", 2, lanceMatchHits(indexName, "天気"));
            assertEquals("icu must split 東京 out of the sentences", 2, lanceMatchHits(indexName, "東京"));
            assertEquals("icu must split 京都 out of the sentence", 1, lanceMatchHits(indexName, "京都"));

            // A second build naming another tokenizer does not touch the
            // existing index: the column is skipped, the response lists
            // nothing under fts, and queries keep the icu segmentation.
            String rebuild = readAll(
                postJson("/_lance/build_indexes/" + indexName, "{\"fts_columns\":[\"text\"],\"tokenizer\":\"simple\"}")
            );
            assertTrue("expected empty fts list on rebuild: " + rebuild, rebuild.contains("\"fts\":[]"));
            assertEquals("existing index keeps its tokenizer after a rebuild request", 2, lanceMatchHits(indexName, "天気"));
        }
    }

    public void testBuildIndexesWithLinderaIpadicMatchesJapaneseWords() throws Exception {
        // lindera/ipadic needs a compiled IPADIC dictionary and a
        // config.yml under $LANCE_LANGUAGE_MODEL_HOME/lindera/ipadic
        // (docs/features.md, "Full-text search"). build.gradle forwards
        // the variable to the cluster JVM and exposes it to this JVM as
        // tests.lance.language_model_home; without it the test skips.
        String home = System.getProperty("tests.lance.language_model_home");
        assumeTrue(
            "LANCE_LANGUAGE_MODEL_HOME is not set; lindera/ipadic needs a compiled IPADIC dictionary and config.yml under "
                + "$LANCE_LANGUAGE_MODEL_HOME/lindera/ipadic, so this test only runs where an operator prepared one",
            home != null && !home.isEmpty()
        );
        Path ipadic = Path.of(home).resolve("lindera").resolve("ipadic");
        assumeTrue(
            "LANCE_LANGUAGE_MODEL_HOME=" + home + " has no lindera/ipadic/config.yml; prepare the dictionary as docs/features.md describes",
            Files.isRegularFile(ipadic.resolve("config.yml"))
        );
        try (JapaneseIndex fixture = JapaneseIndex.surface("jalindera")) {
            String indexName = fixture.indexName();
            String build = readAll(
                postJson("/_lance/build_indexes/" + indexName, "{\"fts_columns\":[\"text\"],\"tokenizer\":\"lindera/ipadic\"}")
            );
            assertTrue("expected text in fts built list: " + build, build.contains("\"fts\":[\"text\"]"));
            awaitLanceTextMapping(indexName);

            assertEquals("lindera/ipadic must split 天気 out of the sentences", 2, lanceMatchHits(indexName, "天気"));
            assertEquals("lindera/ipadic must split 東京 out of the sentences", 2, lanceMatchHits(indexName, "東京"));
            assertEquals("lindera/ipadic must split 京都 out of the sentence", 1, lanceMatchHits(indexName, "京都"));
        }
    }

    public void testBuildIndexesRejectsUnknownTokenizerWithLanceMessage() throws Exception {
        // The plugin has no allowlist; Lance's InvalidInput for the name
        // reaches the caller as 400 with Lance's own wording.
        try (JapaneseIndex fixture = JapaneseIndex.surface("jabadtok")) {
            String indexName = fixture.indexName();
            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson("/_lance/build_indexes/" + indexName, "{\"fts_columns\":[\"text\"],\"tokenizer\":\"no-such-tokenizer\"}")
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for unknown tokenizer, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected Lance's message naming the tokenizer: " + body, body.contains("no-such-tokenizer"));
            assertTrue("expected Lance's 'unknown base tokenizer' wording: " + body, body.contains("unknown base tokenizer"));

            // Nothing was committed: the column is still keyword.
            String mapping = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_mapping")));
            assertTrue(
                "text must still be keyword after the rejected build: " + mapping,
                mapping.contains("\"text\":{\"type\":\"keyword\"")
            );
        }
    }

    public void testBuildIndexesRejectsMalformedFtsColumnsAndTokenizer() throws Exception {
        try (JapaneseIndex fixture = JapaneseIndex.surface("jatokopt")) {
            String indexName = fixture.indexName();

            // tokenizer only shapes indexes this request creates.
            assertBuildIndexesRejected(indexName, "{\"tokenizer\":\"icu\"}", "name them in fts_columns");
            // optimize extends existing indexes and creates none.
            assertBuildIndexesRejected(
                indexName,
                "{\"optimize\":true,\"fts_columns\":[\"text\"]}",
                "fts_columns is only valid with optimize=false"
            );
            // Shape checks happen in the REST layer.
            assertBuildIndexesRejected(indexName, "{\"fts_columns\":[\"text\"],\"tokenizer\":[\"icu\"]}", "tokenizer must be a string");
            assertBuildIndexesRejected(indexName, "{\"fts_columns\":\"text\"}", "fts_columns must be an array");
            // Only Utf8 columns can carry an inverted index.
            assertBuildIndexesRejected(indexName, "{\"fts_columns\":[\"id\"]}", "is not a Utf8 column");
            assertBuildIndexesRejected(indexName, "{\"fts_columns\":[\"nope\"]}", "is not a Utf8 column");
        }
    }

    private static void assertBuildIndexesRejected(String indexName, String body, String expectedMessage) throws IOException {
        ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/_lance/build_indexes/" + indexName, body));
        int status = failure.getResponse().getStatusLine().getStatusCode();
        String response = readAll(failure.getResponse());
        assertEquals("expected 400 for " + body + ", saw " + status + ": " + response, 400, status);
        assertTrue("expected '" + expectedMessage + "' for " + body + ": " + response, response.contains(expectedMessage));
    }

    private static int lanceMatchHits(String indexName, String query) throws IOException {
        String body = readAll(
            postJson("/" + indexName + "/_search", "{\"query\":{\"lance_match\":{\"field\":\"text\",\"query\":\"" + query + "\"}}}")
        );
        return extractIntPath(body, "hits", "total", "value");
    }

    /**
     * Waits until the namespace poll has noticed the FTS index the build
     * committed and re-derived the mapping. The keyword to lance_text
     * change cannot go through PutMapping, so the poll deletes and
     * recreates the index; a GET in that window answers 404 and counts
     * as "not yet".
     */
    private static void awaitLanceTextMapping(String indexName) throws Exception {
        assertBusy(() -> {
            String mapping;
            try {
                mapping = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_mapping")));
            } catch (ResponseException e) {
                throw new AssertionError("index " + indexName + " is between delete and recreate: " + e.getMessage());
            }
            assertTrue("waiting for text to become lance_text: " + mapping, mapping.contains("\"text\":{\"type\":\"lance_text\""));
        }, 30, TimeUnit.SECONDS);
        ensureGreen(indexName);
    }

    /**
     * Japanese fixture surfaced through a namespace registration rather
     * than attach, so the poll keeps following the table after the
     * build_indexes commit and the keyword to lance_text rebuild.
     */
    private static final class JapaneseIndex implements AutoCloseable {
        private final Path scratchDir;
        private final String indexName;

        private JapaneseIndex(Path scratchDir, String indexName) {
            this.scratchDir = scratchDir;
            this.indexName = indexName;
        }

        String indexName() {
            return indexName;
        }

        static JapaneseIndex surface(String testHint) throws Exception {
            String suffix = testHint.toLowerCase(Locale.ROOT) + "-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
            Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
            String indexName = "demo-" + suffix;
            LanceTableFactory.writeJapaneseTable(scratchDir, indexName);

            Response register = postJson("/_lance/namespace", "{\"path\":\"" + scratchDir + "\"}");
            assertEquals("namespace register failed: " + readAll(register), 200, register.getStatusLine().getStatusCode());
            assertBusy(() -> {
                String cat = readAll(client().performRequest(new Request("GET", "/_cat/indices?format=json")));
                assertTrue("waiting for index " + indexName + ", saw: " + cat, cat.contains("\"" + indexName + "\""));
            });
            ensureGreen(indexName);
            String mapping = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_mapping")));
            assertTrue("text must start as keyword (no FTS index yet): " + mapping, mapping.contains("\"text\":{\"type\":\"keyword\""));
            return new JapaneseIndex(scratchDir, indexName);
        }

        @Override
        public void close() throws IOException {
            try {
                deleteJson("/_lance/namespace", "{\"path\":\"" + scratchDir + "\"}");
            } catch (Exception ignored) {
                // best-effort cleanup
            }
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {
                // best-effort cleanup
            }
            deleteRecursively(scratchDir);
        }
    }
}
