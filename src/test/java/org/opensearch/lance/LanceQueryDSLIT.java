/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.core.rest.RestStatus;

/**
 * The stock query DSL against a Lance backed index, one case per query
 * type: every case runs on the fragment path under four envelopes (a
 * count, a page in score order, a page ordered by a column, and an
 * aggregation) and must answer what the stock search action answers for
 * the same body against the target of {@link #withStockOracle}, while
 * {@code GET /<index>/_lance/explain} must report the route the matrix
 * in {@code docs/features.md} promises: a predicate the Lance scan
 * evaluates, the Lance inverted index, or the Lucene composition of the
 * request's own builder over the fragment leaves. The score shaping
 * compounds ({@code constant_score}, {@code dis_max}, {@code boosting},
 * {@code function_score}) take the scan where no score is read and the
 * Lucene composition on the score ordered page. The fixture is the
 * three fragment hint table: {@code body} is {@code lance_text} with
 * positions, {@code rating} an integer with nulls, {@code category} a
 * keyword with nulls, {@code tags} a keyword array, {@code flag} a
 * boolean with nulls.
 */
public class LanceQueryDSLIT extends LanceRestTestCase {

    /** How the planner routes a query type. */
    private enum Route {
        /** The bare scan: no predicate travels and nothing is unplanned. */
        BARE_SCAN,
        /** A predicate the Lance scan evaluates under every envelope. */
        SCAN,
        /** A predicate where no score is read; the Lucene composition on the score ordered page. */
        SCAN_UNLESS_SCORED,
        /** A Lance FTS clause (the stock clause rewritten, or fused into one). */
        FTS,
        /** The Lucene composition of the request's own builder under every envelope. */
        LUCENE
    }

    /**
     * One query type: the clause, its route, the {@code unplanned}
     * message explain reports for a Lucene route (null on the scan and
     * FTS routes), and whether the {@code _score} of a page can be
     * compared with the stock search action (a predicate the scan
     * evaluates scores every row 1.0, so a {@code bool} of several
     * scoring clauses differs from Lucene's sum while a single clause
     * does not).
     */
    private record Case(String name, String query, Route route, String unplanned, boolean scoresComparable) {
        static Case scan(String name, String query) {
            return new Case(name, query, Route.SCAN, null, true);
        }

        static Case scanNoScores(String name, String query) {
            return new Case(name, query, Route.SCAN, null, false);
        }

        static Case unlessScored(String name, String query) {
            return unlessScored(name, query, "query type [" + name + "] on a scored request");
        }

        static Case unlessScored(String name, String query, String unplanned) {
            return new Case(name, query, Route.SCAN_UNLESS_SCORED, unplanned, true);
        }

        static Case fts(String name, String query) {
            return new Case(name, query, Route.FTS, null, true);
        }

        static Case lucene(String name, String query) {
            return new Case(name, query, Route.LUCENE, "query type [" + name + "]", true);
        }

        static Case lucene(String name, String query, String unplanned) {
            return new Case(name, query, Route.LUCENE, unplanned, true);
        }
    }

    /** The message a {@code bool} whose optional clauses would score its matches apart reports on the score ordered page. */
    private static final String OPTIONAL_SHOULD = "bool with optional should clauses on a scored request";

    private static final String WRAPPED_TERM = Base64.getEncoder()
        .encodeToString("{\"term\":{\"category\":\"c1\"}}".getBytes(StandardCharsets.UTF_8));

    private static final List<Case> CASES = List.of(
        new Case("match_all", "{\"match_all\":{}}", Route.BARE_SCAN, null, true),
        Case.scan("match_none", "{\"match_none\":{}}"),
        Case.scan("term", "{\"term\":{\"category\":\"c1\"}}"),
        Case.scan("term_integer", "{\"term\":{\"rating\":37}}"),
        Case.scan("term_boolean", "{\"term\":{\"flag\":true}}"),
        Case.scan("terms", "{\"terms\":{\"category\":[\"c0\",\"c2\"]}}"),
        Case.scan("exists", "{\"exists\":{\"field\":\"category\"}}"),
        Case.scan("range", "{\"range\":{\"rating\":{\"gte\":100,\"lt\":600}}}"),
        Case.scan("prefix", "{\"prefix\":{\"category\":\"c\"}}"),
        Case.scan("wildcard", "{\"wildcard\":{\"category\":\"*1\"}}"),
        Case.scan("regexp", "{\"regexp\":{\"category\":\"c[02]\"}}"),
        Case.scan("prefix_lance_text", "{\"prefix\":{\"body\":\"hello tok1\"}}"),
        Case.scan("wildcard_lance_text", "{\"wildcard\":{\"body\":\"*grp3 *\"}}"),
        Case.scan("regexp_lance_text", "{\"regexp\":{\"body\":\"hello tok[0-9] .*\"}}"),
        Case.scan("wrapper", "{\"wrapper\":{\"query\":\"" + WRAPPED_TERM + "\"}}"),
        Case.scanNoScores(
            "bool_must",
            "{\"bool\":{\"must\":[{\"term\":{\"category\":\"c0\"}},{\"range\":{\"rating\":{\"gte\":100}}}],"
                + "\"must_not\":[{\"term\":{\"flag\":true}}]}}"
        ),
        Case.scan(
            "bool_single_should",
            "{\"bool\":{\"should\":[{\"term\":{\"category\":\"c0\"}}],\"must_not\":[{\"term\":{\"flag\":true}}]}}"
        ),
        Case.unlessScored(
            "bool",
            "{\"bool\":{\"must\":[{\"term\":{\"category\":\"c0\"}}],\"filter\":[{\"range\":{\"rating\":{\"gte\":100}}}],"
                + "\"must_not\":[{\"term\":{\"flag\":true}}],\"should\":[{\"term\":{\"rating\":37}}]}}",
            OPTIONAL_SHOULD
        ),
        Case.unlessScored(
            "bool_should",
            "{\"bool\":{\"should\":[{\"term\":{\"category\":\"c0\"}},{\"range\":{\"rating\":{\"gte\":900}}}]}}",
            OPTIONAL_SHOULD
        ),
        Case.unlessScored(
            "bool_of_bool",
            "{\"bool\":{\"must\":[{\"bool\":{\"should\":[{\"term\":{\"category\":\"c0\"}},{\"term\":{\"category\":\"c1\"}}]}}],"
                + "\"filter\":[{\"bool\":{\"must_not\":[{\"exists\":{\"field\":\"flag\"}}]}}]}}",
            OPTIONAL_SHOULD
        ),
        Case.unlessScored("constant_score", "{\"constant_score\":{\"filter\":{\"term\":{\"category\":\"c0\"}},\"boost\":3}}"),
        Case.unlessScored(
            "dis_max",
            "{\"dis_max\":{\"tie_breaker\":0.5,\"queries\":[{\"term\":{\"category\":\"c0\"}},{\"range\":{\"rating\":{\"gte\":800}}}]}}"
        ),
        Case.unlessScored(
            "boosting",
            "{\"boosting\":{\"positive\":{\"range\":{\"rating\":{\"gte\":500}}},\"negative\":{\"term\":{\"flag\":true}},"
                + "\"negative_boost\":0.2}}"
        ),
        Case.unlessScored(
            "function_score",
            "{\"function_score\":{\"query\":{\"range\":{\"rating\":{\"gte\":500}}},"
                + "\"field_value_factor\":{\"field\":\"rating\",\"missing\":1},\"boost_mode\":\"replace\"}}"
        ),
        Case.lucene(
            "function_score_script",
            "{\"function_score\":{\"query\":{\"range\":{\"rating\":{\"gte\":500}}},"
                + "\"script_score\":{\"script\":{\"source\":\"doc['rating'].size() == 0 ? 0 : doc['rating'].value\"}},"
                + "\"boost_mode\":\"replace\"}}",
            "function_score with a script_score function"
        ),
        Case.lucene(
            "script_score",
            "{\"script_score\":{\"query\":{\"term\":{\"category\":\"c0\"}},"
                + "\"script\":{\"source\":\"doc['rating'].size() == 0 ? 0 : doc['rating'].value\"}}}"
        ),
        Case.lucene("script", "{\"script\":{\"script\":{\"source\":\"doc['rating'].size() > 0 && doc['rating'].value % 7 == 0\"}}}"),
        Case.lucene("simple_query_string", "{\"simple_query_string\":{\"query\":\"tok3 | tok7 | tok11\",\"fields\":[\"body\"]}}"),
        Case.lucene(
            "simple_query_string_keyword",
            "{\"simple_query_string\":{\"query\":\"c0\",\"fields\":[\"category\"]}}",
            "query type [simple_query_string]"
        ),
        Case.lucene("query_string", "{\"query_string\":{\"query\":\"body:tok3 OR (category:c1 AND rating:>900)\"}}"),
        Case.lucene("fuzzy", "{\"fuzzy\":{\"category\":{\"value\":\"c9\",\"fuzziness\":1}}}"),
        Case.lucene("fuzzy_lance_text", "{\"fuzzy\":{\"body\":{\"value\":\"tok13\",\"fuzziness\":1}}}", "query type [fuzzy]"),
        Case.lucene(
            "terms_set",
            "{\"terms_set\":{\"tags\":{\"terms\":[\"t0\",\"t1\",\"t3\"],\"minimum_should_match_script\":{\"source\":\"2\"}}}}"
        ),
        Case.lucene(
            "terms_keyword_array",
            "{\"terms\":{\"tags\":[\"t3\"]}}",
            "column [tags] behind field [tags] is not a supported scalar column"
        ),
        Case.lucene("match", "{\"match\":{\"category\":\"c2\"}}"),
        Case.lucene("multi_match", "{\"multi_match\":{\"query\":\"tok5\",\"fields\":[\"body\"],\"type\":\"most_fields\"}}"),
        Case.fts("match_lance_text", "{\"match\":{\"body\":\"grp3\"}}"),
        Case.fts("match_phrase", "{\"match_phrase\":{\"body\":\"hello tok5\"}}"),
        Case.fts("multi_match_best_fields", "{\"multi_match\":{\"query\":\"tok5\",\"fields\":[\"body\"]}}"),
        Case.fts("bool_fts_with_filter", "{\"bool\":{\"must\":[{\"match\":{\"body\":\"grp3\"}}],\"filter\":[{\"term\":{\"flag\":true}}]}}")
    );

    private static String explainOk(String indexName, String body) throws IOException {
        Request request = new Request("GET", "/" + indexName + "/_lance/explain");
        request.setJsonEntity(body);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
        return readAll(response);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> fragmentPlanOf(String explained) {
        Map<String, Object> plan = (Map<String, Object>) parseJson(explained).get("fragment_plan");
        assertNotNull("the fragment route carries a plan: " + explained, plan);
        return plan;
    }

    /**
     * Runs {@code body} on the fragment path and on the stock search
     * action and asserts the two {@code hits} blocks agree: the total,
     * the max score and every rendered hit key when scores are
     * comparable, the total and every hit key but {@code _score}
     * otherwise. Also asserts the fragment path served the body (the
     * executed counter advanced). Returns the fragment path body.
     */
    private static String assertSameAsStockSearch(String indexName, String body, boolean scoresComparable) throws IOException {
        long executedBefore = fragmentRequestsExecuted();
        String fragmentBody = readAll(postJson("/" + indexName + "/_search", body));
        assertEquals("the fragment path served " + body, executedBefore + 1, fragmentRequestsExecuted());
        String shardBody = readAll(postJson("/" + withStockOracle(indexName) + "/_search", body));
        assertEquals(body, totalOf(shardBody), totalOf(fragmentBody));
        if (scoresComparable) {
            assertEquals(body, maxScoreOf(shardBody), maxScoreOf(fragmentBody));
            assertEquals(body, fullHitsOf(shardBody), fullHitsOf(fragmentBody));
        } else {
            assertEquals(body, hitsOf(shardBody), hitsOf(fragmentBody));
        }
        assertEquals(
            body,
            aggregationsBlock(parseJson(shardBody).get("aggregations")),
            aggregationsBlock(parseJson(fragmentBody).get("aggregations"))
        );
        return fragmentBody;
    }

    @SuppressWarnings("unchecked")
    private static Object maxScoreOf(String searchBody) {
        return ((Map<String, Object>) parseJson(searchBody).get("hits")).get("max_score");
    }

    /** The four request envelopes every case runs under. */
    private enum Envelope {
        /** {@code size: 0} with an exact total: no score is read, the count route. */
        COUNT,
        /** A page in score order: scores are read. */
        PAGE,
        /** A page ordered by a column: no score is read, a predicate folds into the scan. */
        SORTED,
        /** {@code size: 0} with an aggregation: no score is read, a predicate folds into the scan. */
        AGGREGATION;

        boolean scored() {
            return this == PAGE;
        }

        boolean pushable() {
            return this == SORTED || this == AGGREGATION;
        }
    }

    /** Asserts the explain output of {@code body} matches the case's route under {@code envelope}. */
    private static void assertRoute(String indexName, Case c, String body, Envelope envelope) throws IOException {
        String explained = explainOk(indexName, body);
        assertEquals(body, "fragment", stringPath(explained, "route"));
        Map<String, Object> plan = fragmentPlanOf(explained);
        Object unplanned = parseJson(explained).get("unplanned");
        String what = c.name() + " under " + body + ": " + explained;
        switch (c.route()) {
            case BARE_SCAN -> {
                assertNull("nothing is unplanned: " + what, unplanned);
                assertFalse("no predicate travels: " + what, plan.containsKey("filter_sql"));
                assertNull("no Lance clause: " + what, plan.get("lance_clause"));
                if (envelope.pushable()) {
                    assertEquals("the envelope folds into the scan: " + what, "PUSHED_SCAN", plan.get("kind"));
                }
            }
            case SCAN -> assertPredicate(what, plan, unplanned, envelope);
            case SCAN_UNLESS_SCORED -> {
                if (envelope.scored()) {
                    assertLucene(what, plan, unplanned, c.unplanned());
                } else {
                    assertPredicate(what, plan, unplanned, envelope);
                }
            }
            case FTS -> {
                assertNotNull("the Lance clause travels: " + what, plan.get("lance_clause"));
                assertNull("nothing is unplanned: " + what, unplanned);
            }
            case LUCENE -> assertLucene(what, plan, unplanned, c.unplanned());
        }
    }

    /**
     * A predicate travels as Lance SQL (with Substrait bytes where the
     * planner chose them): the count route counts it in a scan that
     * returns no rows, a sorted page and an aggregation fold into the
     * scan, and a page in score order is the pushed page or the Lucene
     * collector over the scan filter, whichever the planner priced.
     */
    private static void assertPredicate(String what, Map<String, Object> plan, Object unplanned, Envelope envelope) {
        assertNull("nothing is unplanned: " + what, unplanned);
        assertNull("no Lance clause: " + what, plan.get("lance_clause"));
        assertTrue("the predicate travels as SQL: " + what, plan.containsKey("filter_sql"));
        String kind = (String) plan.get("kind");
        if (envelope == Envelope.COUNT) {
            assertEquals("the count route: " + what, "LUCENE_COUNT", kind);
        } else if (envelope.pushable()) {
            assertEquals("the envelope folds into the scan: " + what, "PUSHED_SCAN", kind);
        }
    }

    private static void assertLucene(String what, Map<String, Object> plan, Object unplanned, String message) {
        assertEquals("the query is named as unplanned: " + what, message, unplanned);
        assertNotEquals("nothing is pushed: " + what, "PUSHED_SCAN", plan.get("kind"));
        assertFalse("no predicate travels: " + what, plan.containsKey("filter_sql"));
        assertFalse("no predicate travels: " + what, plan.containsKey("filter_substrait_bytes"));
        assertNull("no Lance clause: " + what, plan.get("lance_clause"));
    }

    public void testEveryQueryTypeAnswersLikeTheStockSearchActionOnItsRoute() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUpHintFixture(3, 40, "querydsl")) {
            String indexName = fixture.indexName();
            for (Case c : CASES) {
                String count = "{\"size\":0,\"track_total_hits\":true,\"query\":" + c.query() + "}";
                assertSameAsStockSearch(indexName, count, false);
                assertRoute(indexName, c, count, Envelope.COUNT);

                String page = "{\"size\":20,\"query\":" + c.query() + "}";
                assertSameAsStockSearch(indexName, page, c.scoresComparable());
                assertRoute(indexName, c, page, Envelope.PAGE);

                String sorted = "{\"size\":20,\"query\":" + c.query() + ",\"sort\":[{\"id\":\"asc\"}]}";
                assertSameAsStockSearch(indexName, sorted, false);
                assertRoute(indexName, c, sorted, Envelope.SORTED);

                String aggregation = "{\"size\":0,\"query\":" + c.query() + ",\"aggs\":{\"n\":{\"value_count\":{\"field\":\"id\"}}}}";
                assertSameAsStockSearch(indexName, aggregation, false);
                if (c.route() != Route.FTS) {
                    // An aggregation over a full text root stays on the
                    // aggregators, which the FTS route does not pin.
                    assertRoute(indexName, c, aggregation, Envelope.AGGREGATION);
                }
            }
        }
    }

    /**
     * The score shaping compounds change the page order on the Lucene
     * composition the way the query asks, and the pushed count and page
     * select the same rows: a {@code function_score} that replaces the
     * score by {@code rating} orders the page by rating, a
     * {@code boosting} demotes the rows its negative clause matches, a
     * {@code dis_max} scores the union by its best clause plus the tie
     * breaker, and {@code constant_score} carries its boost.
     */
    public void testScoreShapingCompoundsOrderThePageAndCountTheSameRows() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUpHintFixture(3, 40, "scored")) {
            String indexName = fixture.indexName();

            String functionScore = "{\"function_score\":{\"query\":{\"range\":{\"rating\":{\"gte\":900}}},"
                + "\"field_value_factor\":{\"field\":\"rating\"},\"boost_mode\":\"replace\"}}";
            String page = readAll(postJson("/" + indexName + "/_search", "{\"size\":3,\"query\":" + functionScore + "}"));
            List<Map<String, Object>> hits = fullHitsOf(page);
            assertEquals(3, hits.size());
            double previous = Double.MAX_VALUE;
            for (Map<String, Object> hit : hits) {
                double score = ((Number) hit.get("_score")).doubleValue();
                @SuppressWarnings("unchecked")
                Map<String, Object> source = (Map<String, Object>) hit.get("_source");
                assertEquals("the score is the rating: " + page, ((Number) source.get("rating")).doubleValue(), score, 0.0d);
                assertTrue("the page is in rating order: " + page, score <= previous);
                previous = score;
            }
            // rating = (i * 37) % 1000 >= 900 over 120 rows, minus the null
            // ratings of i % 5 == 4: the count is the same through the
            // pushed predicate and the Lucene composition.
            int expected = 0;
            for (int i = 0; i < 120; i++) {
                if (i % 5 != 4 && (i * 37) % 1000 >= 900) {
                    expected++;
                }
            }
            assertEquals(expected, extractIntPath(page, "hits", "total", "value"));
            String count = readAll(postJson("/" + indexName + "/_count", "{\"query\":" + functionScore + "}"));
            assertEquals(expected, extractIntPath(count, "count"));

            String boosting = "{\"boosting\":{\"positive\":{\"term\":{\"category\":\"c0\"}},\"negative\":{\"term\":{\"flag\":true}},"
                + "\"negative_boost\":0.25}}";
            String demoted = readAll(postJson("/" + indexName + "/_search", "{\"size\":40,\"query\":" + boosting + "}"));
            boolean sawDemoted = false;
            for (Map<String, Object> hit : fullHitsOf(demoted)) {
                double score = ((Number) hit.get("_score")).doubleValue();
                @SuppressWarnings("unchecked")
                Map<String, Object> source = (Map<String, Object>) hit.get("_source");
                assertEquals("c0", source.get("category"));
                if (Boolean.TRUE.equals(source.get("flag"))) {
                    assertEquals("a negative match scores the negative boost: " + demoted, 0.25d, score, 0.0001d);
                    sawDemoted = true;
                } else {
                    assertEquals("a positive only match keeps its score: " + demoted, 1.0d, score, 0.0001d);
                    assertFalse("demoted rows sort after the others: " + demoted, sawDemoted);
                }
            }
            assertTrue("the fixture has c0 rows with flag true: " + demoted, sawDemoted);

            String disMax = "{\"dis_max\":{\"tie_breaker\":0.5,\"queries\":[{\"term\":{\"category\":\"c0\"}},{\"term\":{\"flag\":true}}]}}";
            String best = readAll(postJson("/" + indexName + "/_search", "{\"size\":1,\"query\":" + disMax + "}"));
            // A row matching both clauses scores 1.0 + 0.5 * 1.0.
            assertEquals(1.5d, extractDoublePath(best, "hits", "max_score"), 0.0001d);
            String disMaxCount = readAll(postJson("/" + indexName + "/_count", "{\"query\":" + disMax + "}"));
            int union = 0;
            for (int i = 0; i < 120; i++) {
                boolean c0 = i % 4 != 3 && i % 3 == 0;
                boolean flagTrue = i % 7 != 6 && i % 2 == 0;
                if (c0 || flagTrue) {
                    union++;
                }
            }
            assertEquals(union, extractIntPath(disMaxCount, "count"));

            String constantScore = "{\"constant_score\":{\"filter\":{\"term\":{\"category\":\"c0\"}},\"boost\":3.5}}";
            String boosted = readAll(postJson("/" + indexName + "/_search", "{\"size\":1,\"query\":" + constantScore + "}"));
            assertEquals(3.5d, extractDoublePath(boosted, "hits", "max_score"), 0.0001d);
        }
    }

    /**
     * {@code ids} compares the primary key column; the string key
     * fixture declares one. The stock search action is no oracle here:
     * its whole table reader answers {@code ids} from {@code _id}
     * postings the fragment leaves do not have, so the expected keys are
     * pinned instead.
     */
    public void testIdsQueryComparesThePrimaryKey() throws Exception {
        String suffix = "querydsl-ids-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String indexName = "demo-" + suffix;
        String tableUri = LanceTableFactory.writeStringPkTable(scratchDir, indexName, 12, 4);
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            String ids = "{\"ids\":{\"values\":[\"alpha-2\",\"alpha-9\",\"alpha-99\"]}}";
            String count = "{\"size\":0,\"track_total_hits\":true,\"query\":" + ids + "}";
            String explained = explainOk(indexName, count);
            assertNull("ids translates: " + explained, parseJson(explained).get("unplanned"));
            assertEquals("key IN ('alpha-2', 'alpha-9', 'alpha-99')", stringPath(explained, "fragment_plan", "filter_sql"));
            long executedBefore = fragmentRequestsExecuted();
            assertEquals(2, extractIntPath(readAll(postJson("/" + indexName + "/_search", count)), "hits", "total", "value"));
            assertEquals("the fragment path served the count", executedBefore + 1, fragmentRequestsExecuted());
            String sorted = "{\"size\":10,\"query\":" + ids + ",\"sort\":[{\"key\":\"asc\"}]}";
            assertEquals(List.of("alpha-2", "alpha-9"), idsOf(hitsOf(readAll(postJson("/" + indexName + "/_search", sorted)))));
            String page = "{\"size\":10,\"query\":" + ids + "}";
            assertEquals(List.of("alpha-2", "alpha-9"), idsOf(hitsOf(readAll(postJson("/" + indexName + "/_search", page)))));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
            deleteRecursively(scratchDir);
        }
    }

    /**
     * The query types the mapping refuses answer the same 400 on both
     * paths: {@code intervals} and {@code match_phrase_prefix} need a
     * {@code text} field, {@code has_child} / {@code has_parent} a join
     * field no Lance table maps.
     */
    public void testRefusedQueryTypesAnswerTheSame400AsTheStockSearchAction() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(6, "querydsl-refused")) {
            String indexName = fixture.indexName();
            assertSameRefusalAsStockSearch(
                indexName,
                "{\"query\":{\"intervals\":{\"body\":{\"match\":{\"query\":\"hello lance\"}}}}}",
                "Can only use interval queries on text fields - not on [body] which is of type [lance_text]"
            );
            assertSameRefusalAsStockSearch(
                indexName,
                "{\"query\":{\"match_phrase_prefix\":{\"body\":\"hello lan\"}}}",
                "Can only use phrase prefix queries on text fields - not on [body] which is of type [lance_text]"
            );
            assertSameRefusalAsStockSearch(
                indexName,
                "{\"query\":{\"has_child\":{\"type\":\"child\",\"query\":{\"match_all\":{}}}}}",
                "[has_child] no join field has been configured"
            );
        }
    }

    /**
     * The query types that read postings or term statistics the fragment
     * leaves do not carry are not refused: the Lucene composition runs
     * them over empty structures and they match nothing, on both paths,
     * while the same text matches through the Lance inverted index.
     */
    public void testQueryTypesWithoutPostingsMatchNothingOnBothPaths() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(6, "querydsl-nopostings")) {
            String indexName = fixture.indexName();
            String control = "{\"size\":0,\"track_total_hits\":true,\"query\":{\"match\":{\"body\":\"hello\"}}}";
            assertEquals(3, extractIntPath(assertSameAsStockSearch(indexName, control, false), "hits", "total", "value"));
            for (String query : List.of(
                "{\"span_term\":{\"body\":\"hello\"}}",
                "{\"span_near\":{\"clauses\":[{\"span_term\":{\"body\":\"hello\"}},{\"span_term\":{\"body\":\"lance\"}}],\"slop\":0,\"in_order\":true}}",
                "{\"more_like_this\":{\"fields\":[\"body\"],\"like\":\"hello lance\",\"min_term_freq\":1,\"min_doc_freq\":1}}"
            )) {
                String body = "{\"size\":0,\"track_total_hits\":true,\"query\":" + query + "}";
                assertEquals(query, 0, extractIntPath(assertSameAsStockSearch(indexName, body, false), "hits", "total", "value"));
            }
        }
    }

    /**
     * Assert that {@code body} is refused with 400 on both paths with a
     * root cause reason ending in {@code reason} (the shard level query
     * build prefixes {@code failed to create query: }); see
     * {@code LanceHitShapeIT} for why the oracle target asks for no
     * partial results.
     */
    private static void assertSameRefusalAsStockSearch(String indexName, String body, String reason) throws IOException {
        ConcurrentResult fragmentPath = postForStatus("/" + indexName + "/_search", body);
        ConcurrentResult shardPath = postForStatus("/" + withStockOracle(indexName) + "/_search?allow_partial_search_results=false", body);
        assertEquals(fragmentPath.body(), 400, fragmentPath.status());
        assertEquals(shardPath.body(), 400, shardPath.status());
        String fragmentReason = stringPath(fragmentPath.body(), "error", "root_cause", "0", "reason");
        String shardReason = stringPath(shardPath.body(), "error", "root_cause", "0", "reason");
        assertTrue(fragmentReason, fragmentReason.endsWith(reason));
        assertTrue(shardReason, shardReason.endsWith(reason));
    }
}
