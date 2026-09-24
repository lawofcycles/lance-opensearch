/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.opensearch.client.Request;
import org.opensearch.client.Response;

/**
 * The hits envelope through the planner: a sorted page folds into the
 * scan as a pushed top-k (visible in the explain output), round trips
 * {@code _id} / {@code _source} / sort values, a {@code size: 0}
 * request keeps the count path, and a {@code search_after}
 * continuation folds as a cursor bound and pages through the table in
 * agreement with the Lucene collector. The per hit projections
 * ({@code _source}, {@code stored_fields}, {@code docvalue_fields},
 * {@code fields}, {@code explain}) and the score order cursor are
 * compared with the stock search action's answers, which the same body
 * against the target of {@link #withStockOracle} runs on.
 */
public class LanceHitShapeIT extends LanceRestTestCase {

    private static String explainBody(String indexName, String body) throws IOException {
        Request request = new Request("GET", "/" + indexName + "/_lance/explain");
        request.setJsonEntity(body);
        Response response = client().performRequest(request);
        return readAll(response);
    }

    public void testSortedPageFoldsAndRoundTripsTheEnvelope() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUpNullable("hitshape")) {
            String indexName = fixture.indexName();

            // The explain output shows the fold: the logical plan is the
            // hit shape over the top-k over the scan, the physical plan
            // is the coordinator's merge and fan out over the scan
            // carrying the pushed page.
            String explained = explainBody(indexName, "{\"size\":3,\"sort\":[{\"count16\":\"desc\"}]}");
            String logical = stringPath(explained, "logical");
            assertTrue("logical plan carries the hit shape: " + logical, logical.contains("LanceHitShape"));
            assertTrue("logical plan carries the top-k: " + logical, logical.contains("LanceTopK"));
            String physical = stringPath(explained, "physical");
            assertTrue("the coordinator merge leads: " + physical, physical.startsWith("MergeExec("));
            assertTrue("the per node plan is the scan: " + physical, physical.contains("LanceTableScan("));
            assertTrue("the page is pushed: " + physical, physical.contains("topk{"));
            assertTrue("the page carries its fetch: " + physical, physical.contains("fetch=3"));

            // The page itself: _id, _source and typed sort values.
            String body = readAll(postJson("/" + indexName + "/_search", "{\"size\":3,\"sort\":[{\"count16\":\"desc\"}]}"));
            List<Map<String, Object>> hits = hitsOf(body);
            assertEquals(List.of("0-11", "0-10", "0-9"), idsOf(hits));
            assertEquals(List.of(1100), sortValuesOf(hits.get(0)));
            @SuppressWarnings("unchecked")
            Map<String, Object> source = (Map<String, Object>) hits.get(0).get("_source");
            assertEquals(11, source.get("id"));
            assertEquals(12, extractIntPath(body, "hits", "total", "value"));
        }
    }

    public void testSizeZeroKeepsTheCountPath() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUpNullable("hitszero")) {
            String indexName = fixture.indexName();
            String body = readAll(postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"range\":{\"id\":{\"gte\":6}}}}"));
            assertEquals(6, extractIntPath(body, "hits", "total", "value"));
            assertTrue("a size 0 request returns no hits: " + body, body.contains("\"hits\":[]"));
        }
    }

    /**
     * The nested bool query of depth {@code depth} the Substrait filter
     * test runs, and the rows of the twelve row multi fragment fixture
     * it selects. Level {@code i} wraps the tree so far: in a
     * {@code filter} with {@code id <= 11 - i / 4} when {@code i % 4 == 0},
     * in a {@code should} with a {@code prefix} on {@code title}
     * ({@code "cloudy"}, the odd rows) when {@code i % 4 == 1}, in a
     * {@code filter} with a {@code must_not} on {@code body}
     * ({@code "hello lance " + i}, one even row) when {@code i % 4 == 2},
     * and in a {@code should} with {@code id >= 10} when {@code i % 4 == 3}.
     * The tree crosses three columns and alternates conjunctions and
     * disjunctions level by level (two conjunctions in a row would nest
     * an AND inside an AND, which the translator does not flatten below
     * the root), the shape whose SQL and Substrait spellings differ
     * most.
     */
    private static String deepBoolQuery(int depth) {
        String tree = "{\"range\":{\"id\":{\"gte\":1}}}";
        for (int level = 0; level < depth; level++) {
            switch (level % 4) {
                case 0 -> tree = "{\"bool\":{\"filter\":[" + tree + ",{\"range\":{\"id\":{\"lte\":" + (11 - level / 4) + "}}}]}}";
                case 1 -> tree = "{\"bool\":{\"should\":[" + tree + ",{\"prefix\":{\"title\":\"cloudy\"}}]}}";
                case 2 -> tree = "{\"bool\":{\"filter\":["
                    + tree
                    + "],\"must_not\":[{\"wildcard\":{\"body\":\"hello lance "
                    + level
                    + "\"}}]}}";
                default -> tree = "{\"bool\":{\"should\":[" + tree + ",{\"range\":{\"id\":{\"gte\":10}}}]}}";
            }
        }
        return tree;
    }

    /** The ids the fixture rows {@link #deepBoolQuery} of {@code depth} selects, in id order. */
    private static List<Integer> deepBoolExpected(int depth) {
        List<Integer> selected = new java.util.ArrayList<>();
        for (int id = 0; id < 12; id++) {
            boolean matches = id >= 1;
            for (int level = 0; level < depth; level++) {
                switch (level % 4) {
                    case 0 -> matches = matches && id <= 11 - level / 4;
                    case 1 -> matches = matches || id % 2 == 1;
                    case 2 -> matches = matches && !(id % 2 == 0 && id == level);
                    default -> matches = matches || id >= 10;
                }
            }
            if (matches) {
                selected.add(id);
            }
        }
        return selected;
    }

    public void testDeepMultiColumnBoolTreeRunsAsASubstraitFilter() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUpMultiFragment(12, 4, "substraitfilter")) {
            String indexName = fixture.indexName();
            int depth = 10;
            String query = deepBoolQuery(depth);
            List<Integer> expected = deepBoolExpected(depth);
            assertTrue("the fixture predicate selects a proper subset: " + expected, expected.size() > 2 && expected.size() < 12);

            // The count shape plans the filter into the scan; the
            // planner picks the Substrait encoding and the plan carries
            // the SQL next to the bytes.
            String countBody = "{\"size\":0,\"query\":" + query + "}";
            String explained = explainBody(indexName, countBody);
            @SuppressWarnings("unchecked")
            Map<String, Object> countPlan = (Map<String, Object>) parseJson(explained).get("fragment_plan");
            assertNotNull("the fragment route carries a plan: " + explained, countPlan);
            assertEquals("LUCENE_COUNT", countPlan.get("kind"));
            String filterSql = (String) countPlan.get("filter_sql");
            assertNotNull("the SQL travels next to the bytes: " + explained, filterSql);
            int substraitBytes = ((Number) countPlan.get("filter_substrait_bytes")).intValue();
            assertTrue("the planner chose the Substrait encoding: " + explained, substraitBytes > 0);
            assertTrue(
                "the physical plan names the encoding: " + explained,
                stringPath(explained, "physical").contains("substrait_bytes=")
            );
            logger.info(
                "deep bool tree of depth {}: Lance SQL {} chars, Substrait {} bytes; sql={}",
                depth,
                filterSql.length(),
                substraitBytes,
                filterSql
            );

            // hits.total comes from the Substrait count scan.
            String counted = readAll(postJson("/" + indexName + "/_search", countBody));
            assertEquals(expected.size(), extractIntPath(counted, "hits", "total", "value"));
            String count = readAll(postJson("/" + indexName + "/_count", "{\"query\":" + query + "}"));
            assertEquals(expected.size(), extractIntPath(count, "count"));

            // A page the top-k pushdown does not fold (a sort mode the
            // Lance ordering does not spell) runs the same filter through
            // Lucene's collector: the plan is a Lucene page whose scalar
            // filter carries both encodings, and the hits are the
            // expected rows.
            String pageBody = "{\"size\":12,\"query\":" + query + ",\"sort\":[{\"id\":{\"order\":\"asc\",\"mode\":\"min\"}}]}";
            @SuppressWarnings("unchecked")
            Map<String, Object> pagePlan = (Map<String, Object>) parseJson(explainBody(indexName, pageBody)).get("fragment_plan");
            assertEquals("LUCENE_TOPK", pagePlan.get("kind"));
            assertEquals(filterSql, pagePlan.get("filter_sql"));
            assertEquals(substraitBytes, ((Number) pagePlan.get("filter_substrait_bytes")).intValue());
            List<Map<String, Object>> hits = hitsOf(readAll(postJson("/" + indexName + "/_search", pageBody)));
            List<Integer> ids = new java.util.ArrayList<>();
            for (Map<String, Object> hit : hits) {
                @SuppressWarnings("unchecked")
                Map<String, Object> source = (Map<String, Object>) hit.get("_source");
                ids.add((Integer) source.get("id"));
            }
            assertEquals(expected, ids);

            // The same predicate on the sorted page the pushdown folds
            // takes the SQL encoding and agrees with the collector.
            String sortedBody = "{\"size\":12,\"query\":" + query + ",\"sort\":[{\"id\":\"asc\"}]}";
            @SuppressWarnings("unchecked")
            Map<String, Object> sortedPlan = (Map<String, Object>) parseJson(explainBody(indexName, sortedBody)).get("fragment_plan");
            assertEquals("PUSHED_SCAN", sortedPlan.get("kind"));
            assertFalse("a pushed page takes SQL only: " + sortedPlan, sortedPlan.containsKey("filter_substrait_bytes"));
            assertEquals(hits.size(), hitsOf(readAll(postJson("/" + indexName + "/_search", sortedBody))).size());
        }
    }

    public void testSearchAfterFoldsAndPagesLikeTheLuceneCollector() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUpNullable("hitsafter")) {
            String indexName = fixture.indexName();

            // The cursor page folds: the pushed page carries the strict
            // bound the scan ANDs into its filter.
            String explained = explainBody(indexName, "{\"size\":4,\"sort\":[{\"count64\":\"asc\"}],\"search_after\":[3000]}");
            String physical = stringPath(explained, "physical");
            assertTrue("the cursor page is pushed: " + physical, physical.contains("topk{"));
            assertTrue(
                "the pushed page carries the cursor bound: " + physical,
                physical.contains("cursor=(count64 > 3000 OR count64 IS NULL)")
            );

            // Page through the table with search_after and compare each
            // page with the Lucene collector, which a trivial
            // aggregation forces (the planner refuses aggregations on
            // the hits fold).
            String oracleAgg = ",\"aggs\":{\"n\":{\"value_count\":{\"field\":\"id\"}}}";
            List<String> pagedIds = new java.util.ArrayList<>();
            String cursor = null;
            for (int page = 0; page < 3; page++) {
                String shape = "{\"size\":4,\"sort\":[{\"count64\":\"asc\"}]"
                    + (cursor == null ? "" : ",\"search_after\":[" + cursor + "]");
                List<Map<String, Object>> hits = hitsOf(readAll(postJson("/" + indexName + "/_search", shape + "}")));
                List<Map<String, Object>> oracle = hitsOf(readAll(postJson("/" + indexName + "/_search", shape + oracleAgg + "}")));
                assertEquals("page " + page + " must match the Lucene collector", oracle, hits);
                for (Map<String, Object> hit : hits) {
                    pagedIds.add((String) hit.get("_id"));
                }
                if (hits.isEmpty()) {
                    break;
                }
                cursor = String.valueOf(sortValuesOf(hits.get(hits.size() - 1)).get(0));
            }
            // The union of the pages is the whole sorted table: the one
            // null row (id=5) sorts last through the missing sentinel,
            // whose cursor stays on the Lucene path and still answers.
            List<Map<String, Object>> whole = hitsOf(
                readAll(postJson("/" + indexName + "/_search", "{\"size\":12,\"sort\":[{\"count64\":\"asc\"}]}"))
            );
            assertEquals(idsOf(whole), pagedIds);
        }
    }

    /**
     * Assert that {@code body} answers the same {@code hits} block (total,
     * max_score and every rendered hit key) on the fragment path as on
     * the stock search action, which the same body against the target of
     * {@link #withStockOracle} runs on. Returns the fragment path body.
     */
    private static String assertSameHitsAsStockSearch(String indexName, String body) throws IOException {
        String fragmentBody = readAll(postJson("/" + indexName + "/_search", body));
        String shardBody = readAll(postJson("/" + withStockOracle(indexName) + "/_search", body));
        Map<String, Object> fragmentHits = new LinkedHashMap<>(hitsBlockOf(fragmentBody));
        Map<String, Object> shardHits = new LinkedHashMap<>(hitsBlockOf(shardBody));
        fragmentHits.put("hits", fullHitsOf(fragmentBody));
        shardHits.put("hits", fullHitsOf(shardBody));
        assertEquals(body, shardHits, fragmentHits);
        return fragmentBody;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> hitsBlockOf(String searchBody) {
        return (Map<String, Object>) parseJson(searchBody).get("hits");
    }

    /**
     * Assert that {@code body} is refused with {@code status} and a root
     * cause reason of {@code reason} on both paths. The oracle target
     * carries an empty second shard whose fetch phase never runs, so a
     * refusal raised per hit would leave that shard green and the
     * response at 200 with a partial failure; asking for no partial
     * results makes the one shard's refusal the request's answer, as it
     * is on a single shard.
     */
    private static void assertSameRefusalAsStockSearch(String indexName, String body, int status, String reason) throws IOException {
        ConcurrentResult fragmentPath = postForStatus("/" + indexName + "/_search", body);
        ConcurrentResult shardPath = postForStatus("/" + withStockOracle(indexName) + "/_search?allow_partial_search_results=false", body);
        assertEquals(fragmentPath.body(), status, fragmentPath.status());
        assertEquals(shardPath.body(), status, shardPath.status());
        assertEquals(reason, stringPath(fragmentPath.body(), "error", "root_cause", "0", "reason"));
        assertEquals(reason, stringPath(shardPath.body(), "error", "root_cause", "0", "reason"));
    }

    /** Attach the table at {@code tableUri}; the index takes the table's name. */
    private static String attachTable(String tableUri, String name) throws IOException {
        Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
        assertEquals(200, attach.getStatusLine().getStatusCode());
        return name;
    }

    public void testSourceFilterAppliesOnTheFragmentPath() throws Exception {
        // The _source element of the body (false, an includes list, an
        // excludes object) is applied by the stock FetchSourcePhase over
        // the leaf reader's synthesised source, as on the stock search path.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(6, "sourcefilter")) {
            String indexName = fixture.indexName();

            String off = "{\"size\":1,\"query\":{\"match_all\":{}},\"_source\":false}";
            long executedBefore = fragmentRequestsExecuted();
            String offBody = assertSameHitsAsStockSearch(indexName, off);
            assertEquals("the fragment path served the plain body only", executedBefore + 1, fragmentRequestsExecuted());
            Map<String, Object> hit = fullHitsOf(offBody).get(0);
            assertEquals("0-0", hit.get("_id"));
            assertFalse("_source: false leaves the source out: " + offBody, hit.containsKey("_source"));

            String includes = "{\"size\":1,\"query\":{\"match_all\":{}},\"_source\":[\"id\"]}";
            String includesBody = assertSameHitsAsStockSearch(indexName, includes);
            assertEquals(Map.of("id", 0), fullHitsOf(includesBody).get(0).get("_source"));

            String excludes = "{\"size\":1,\"query\":{\"match_all\":{}},\"_source\":{\"excludes\":[\"body\"]}}";
            String excludesBody = assertSameHitsAsStockSearch(indexName, excludes);
            assertEquals(Map.of("id", 0, "title", "sunny morning 0"), fullHitsOf(excludesBody).get(0).get("_source"));

            // A sorted page the planner pushes into the Lance scan renders
            // through the same fetch phase.
            String sorted = "{\"size\":2,\"sort\":[{\"id\":\"desc\"}],\"_source\":[\"title\"]}";
            String sortedBody = assertSameHitsAsStockSearch(indexName, sorted);
            assertEquals(List.of("0-5", "0-4"), idsOf(hitsOf(sortedBody)));
            assertEquals(Map.of("title", "cloudy morning 5"), fullHitsOf(sortedBody).get(0).get("_source"));
        }
    }

    public void testStoredFieldsProjectTheHitOnTheFragmentPath() throws Exception {
        // stored_fields drives the fetch phase's stored fields visitor:
        // _none_ renders a hit without _id or _source, a named list
        // renders _id and drops the source unless _source is named, and
        // the leaf reader stores nothing else.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(6, "storedfields")) {
            String indexName = fixture.indexName();

            String none = "{\"size\":1,\"query\":{\"match_all\":{}},\"stored_fields\":\"_none_\"}";
            String noneBody = assertSameHitsAsStockSearch(indexName, none);
            Map<String, Object> noneHit = fullHitsOf(noneBody).get(0);
            assertEquals(noneBody, Map.of("_index", indexName, "_score", 1.0d), noneHit);

            String named = "{\"size\":1,\"query\":{\"match_all\":{}},\"stored_fields\":[\"id\"]}";
            String namedBody = assertSameHitsAsStockSearch(indexName, named);
            assertEquals(namedBody, Map.of("_index", indexName, "_id", "0-0", "_score", 1.0d), fullHitsOf(namedBody).get(0));

            String withSource = "{\"size\":1,\"query\":{\"match_all\":{}},\"stored_fields\":[\"_source\"]}";
            String withSourceBody = assertSameHitsAsStockSearch(indexName, withSource);
            assertTrue(withSourceBody, fullHitsOf(withSourceBody).get(0).containsKey("_source"));

            String wildcard = "{\"size\":1,\"query\":{\"match_all\":{}},\"stored_fields\":[\"*\"]}";
            String wildcardBody = assertSameHitsAsStockSearch(indexName, wildcard);
            assertEquals(wildcardBody, Map.of("_index", indexName, "_id", "0-0", "_score", 1.0d), fullHitsOf(wildcardBody).get(0));

            // _none_ cannot be combined with a requested source or with
            // fields, the check SearchService.parseSource applies.
            ConcurrentResult conflict = postForStatus(
                "/" + indexName + "/_search",
                "{\"size\":1,\"query\":{\"match_all\":{}},\"stored_fields\":\"_none_\",\"fields\":[\"id\"]}"
            );
            assertEquals(conflict.body(), 400, conflict.status());
            assertEquals(
                "[stored_fields] cannot be disabled when using the [fields] option",
                stringPath(conflict.body(), "error", "root_cause", "0", "reason")
            );
        }
    }

    public void testDocValueFieldsOnTheFragmentPath() throws Exception {
        // docvalue_fields reads the leaf readers' doc values through the
        // stock FetchDocValuesPhase: numeric, keyword and date columns,
        // the date with a format, and a pattern the mapping expands.
        // lance_text has no doc values and is refused as on the stock search path.
        String suffix = "docvalues-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String dated = attachTable(LanceTableFactory.writeDatedTable(scratchDir, "dated-" + suffix), "dated-" + suffix);
        String demo = attachTable(LanceTableFactory.writeTable(scratchDir, "demo-" + suffix, 6), "demo-" + suffix);
        try {
            String plain = "{\"size\":2,\"query\":{\"match_all\":{}},\"docvalue_fields\":[\"id\",\"category\",\"ts\"]}";
            String plainBody = assertSameHitsAsStockSearch(dated, plain);
            assertEquals(
                Map.of("id", List.of(0), "category", List.of("even"), "ts", List.of("2024-01-15T00:00:00.000Z")),
                fullHitsOf(plainBody).get(0).get("fields")
            );

            String formatted = "{\"size\":2,\"query\":{\"match_all\":{}},\"docvalue_fields\":["
                + "{\"field\":\"ts\",\"format\":\"yyyy-MM-dd\"},{\"field\":\"id\",\"format\":\"000\"}]}";
            String formattedBody = assertSameHitsAsStockSearch(dated, formatted);
            assertEquals(Map.of("id", List.of("000"), "ts", List.of("2024-01-15")), fullHitsOf(formattedBody).get(0).get("fields"));

            String pattern = "{\"size\":1,\"query\":{\"match_all\":{}},\"docvalue_fields\":[\"cat*\"]}";
            String patternBody = assertSameHitsAsStockSearch(dated, pattern);
            assertEquals(Map.of("category", List.of("even")), fullHitsOf(patternBody).get(0).get("fields"));

            String numeric = "{\"size\":1,\"query\":{\"match_all\":{}},\"docvalue_fields\":[\"id\"]}";
            assertEquals(Map.of("id", List.of(0)), fullHitsOf(assertSameHitsAsStockSearch(demo, numeric)).get(0).get("fields"));

            String unmapped = "{\"size\":1,\"query\":{\"match_all\":{}},\"docvalue_fields\":[\"nosuch\"]}";
            assertFalse(fullHitsOf(assertSameHitsAsStockSearch(demo, unmapped)).get(0).containsKey("fields"));

            assertSameRefusalAsStockSearch(
                demo,
                "{\"size\":1,\"query\":{\"match_all\":{}},\"docvalue_fields\":[\"body\"]}",
                400,
                "Fielddata is not supported on field [body] of type [lance_text]"
            );
        } finally {
            client().performRequest(new Request("DELETE", "/" + dated + "," + demo));
        }
    }

    public void testFieldsOnTheFragmentPath() throws Exception {
        // fields reads the values from the synthesised _source through the
        // stock FetchFieldsPhase: a wildcard pattern, named columns of
        // every type, and a date with a format.
        String suffix = "fields-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String dated = attachTable(LanceTableFactory.writeDatedTable(scratchDir, "dated-" + suffix), "dated-" + suffix);
        String demo = attachTable(LanceTableFactory.writeTable(scratchDir, "demo-" + suffix, 6), "demo-" + suffix);
        try {
            String wildcard = "{\"size\":1,\"query\":{\"match_all\":{}},\"fields\":[\"ti*\"]}";
            String wildcardBody = assertSameHitsAsStockSearch(demo, wildcard);
            assertEquals(Map.of("title", List.of("sunny morning 0")), fullHitsOf(wildcardBody).get(0).get("fields"));

            String named = "{\"size\":1,\"query\":{\"match_all\":{}},\"fields\":[\"id\",\"body\"],\"_source\":false}";
            String namedBody = assertSameHitsAsStockSearch(demo, named);
            Map<String, Object> namedHit = fullHitsOf(namedBody).get(0);
            assertFalse(namedBody, namedHit.containsKey("_source"));
            assertEquals(Map.of("id", List.of(0), "body", List.of("hello lance 0")), namedHit.get("fields"));

            String formatted = "{\"size\":1,\"query\":{\"match_all\":{}},\"fields\":[{\"field\":\"ts\",\"format\":\"yyyy\"},\"category\"]}";
            String formattedBody = assertSameHitsAsStockSearch(dated, formatted);
            assertEquals(Map.of("ts", List.of("2024"), "category", List.of("even")), fullHitsOf(formattedBody).get(0).get("fields"));

            String all = "{\"size\":1,\"query\":{\"match_all\":{}},\"fields\":[\"*\"]}";
            assertSameHitsAsStockSearch(dated, all);
        } finally {
            client().performRequest(new Request("DELETE", "/" + dated + "," + demo));
        }
    }

    public void testExplainOnTheFragmentPath() throws Exception {
        // "explain": true runs the stock ExplainPhase against the
        // executor's query. A pushed full text or knn hit is explained by
        // the Lance Weight (its real score, from the scan that served the
        // hits); a scalar filter the planner spelled as Lance SQL is
        // explained by the scan filter Weight with the constant score the
        // stock search path's doc values query also reports.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(6, "explain")) {
            String indexName = fixture.indexName();

            String matchAll = "{\"size\":1,\"query\":{\"match_all\":{}},\"explain\":true}";
            String matchAllBody = assertSameHitsAsStockSearch(indexName, matchAll);
            assertEquals(1.0d, extractDoublePath(matchAllBody, "hits", "hits", "0", "_explanation", "value"), 0d);

            String fts = "{\"size\":2,\"query\":{\"match\":{\"body\":\"lance\"}},\"explain\":true}";
            String ftsBody = assertSameHitsAsStockSearch(indexName, fts);
            assertEquals(0.7361701d, extractDoublePath(ftsBody, "hits", "hits", "0", "_explanation", "value"), 1e-6d);
            assertTrue(ftsBody, stringPath(ftsBody, "hits", "hits", "0", "_explanation", "description").startsWith("lance fts"));

            // Row i sits at (i, 0, ...): the two nearest to 2.4 are rows
            // 2 and 3, and the explanation carries the hit's own score.
            String knn =
                "{\"size\":2,\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":[2.4,0,0,0,0,0,0,0],\"k\":2}},\"explain\":true}";
            String knnBody = assertSameHitsAsStockSearch(indexName, knn);
            assertEquals(List.of("0-2", "0-3"), idsOf(hitsOf(knnBody)));
            assertEquals(
                extractDoublePath(knnBody, "hits", "hits", "0", "_score"),
                extractDoublePath(knnBody, "hits", "hits", "0", "_explanation", "value"),
                1e-6d
            );
            assertTrue(knnBody, stringPath(knnBody, "hits", "hits", "0", "_explanation", "description").startsWith("lance knn"));

            // The scalar filter: the stock search path explains its doc values
            // range query ("id:[2 TO 2]"), the fragment path the Lance
            // scan filter that answered it; both score 1.0.
            String term = "{\"size\":1,\"query\":{\"term\":{\"id\":2}},\"explain\":true}";
            String termBody = readAll(postJson("/" + indexName + "/_search", term));
            assertEquals(List.of("0-2"), idsOf(hitsOf(termBody)));
            assertEquals(1.0d, extractDoublePath(termBody, "hits", "hits", "0", "_explanation", "value"), 0d);
            assertEquals("lance scan filter", stringPath(termBody, "hits", "hits", "0", "_explanation", "description"));
            String termOnStockSearch = readAll(postJson("/" + withStockOracle(indexName) + "/_search", term));
            assertEquals(1.0d, extractDoublePath(termOnStockSearch, "hits", "hits", "0", "_explanation", "value"), 0d);
        }
    }

    public void testSearchAfterInScoreOrderOnTheFragmentPath() throws Exception {
        // A cursor whose first clause is _score is typed against the
        // Lucene sort (the JSON double becomes the Float the score
        // comparator reads, the JSON integer the Long of the id field) and
        // pages like the stock search path. A cursor without a sort, including
        // the lone descending _score that builds no Lucene sort, is
        // refused with the stock search path's message.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(6, "scorecursor")) {
            String indexName = fixture.indexName();
            String sort = "\"sort\":[{\"_score\":\"desc\"},{\"id\":\"asc\"}]";
            String query = "\"query\":{\"match\":{\"body\":\"lance\"}}";

            // All three "hello lance" rows score 0.7361701, below the
            // cursor's 1.0, so the page starts at the first of them.
            String firstPage = "{\"size\":2," + query + "," + sort + ",\"search_after\":[1.0,100]}";
            String firstBody = assertSameHitsAsStockSearch(indexName, firstPage);
            assertEquals(List.of("0-0", "0-2"), idsOf(hitsOf(firstBody)));
            assertEquals(List.of(0.7361701d, 0), sortValuesOf(hitsOf(firstBody).get(0)));

            // The next page continues from the last hit's sort values.
            String nextPage = "{\"size\":2," + query + "," + sort + ",\"search_after\":[0.7361701,2]}";
            String nextBody = assertSameHitsAsStockSearch(indexName, nextPage);
            assertEquals(List.of("0-4"), idsOf(hitsOf(nextBody)));

            // A lone ascending _score is a real sort; the cursor is a Float.
            String ascending = "{\"size\":2," + query + ",\"sort\":[{\"_score\":\"asc\"}],\"search_after\":[0.1]}";
            String ascendingBody = assertSameHitsAsStockSearch(indexName, ascending);
            assertEquals(List.of("0-0", "0-2"), idsOf(hitsOf(ascendingBody)));

            // A lone descending _score builds no Lucene sort, and neither
            // does an absent sort; both refuse the cursor. The absent
            // sort is refused before planning with the stock message on
            // both paths. The lone _score is a page in score order whose
            // ties have no reproducible order, which the planner refuses
            // for the rewritten match as it does for lance_match; the
            // stock oracle keeps its own message.
            String reason = "Sort must contain at least one field.";
            String scoreCursor = "{\"size\":2," + query + ",\"sort\":[\"_score\"],\"search_after\":[1.0]}";
            ConcurrentResult scoreCursorFragment = postForStatus("/" + indexName + "/_search", scoreCursor);
            assertEquals(scoreCursorFragment.body(), 400, scoreCursorFragment.status());
            assertTrue(
                scoreCursorFragment.body(),
                stringPath(scoreCursorFragment.body(), "error", "root_cause", "0", "reason").contains(
                    "search_after requires TieStability [stable_key]"
                )
            );
            ConcurrentResult scoreCursorStock = postForStatus(
                "/" + withStockOracle(indexName) + "/_search?allow_partial_search_results=false",
                scoreCursor
            );
            assertEquals(scoreCursorStock.body(), 400, scoreCursorStock.status());
            assertEquals(reason, stringPath(scoreCursorStock.body(), "error", "root_cause", "0", "reason"));
            assertSameRefusalAsStockSearch(indexName, "{\"size\":2," + query + ",\"search_after\":[1.0]}", 400, reason);

            // A cursor of the wrong length is refused with the shard
            // path's message.
            assertSameRefusalAsStockSearch(
                indexName,
                "{\"size\":2,\"sort\":[{\"id\":\"asc\"}],\"search_after\":[1,2]}",
                400,
                "search_after has 2 value(s) but sort has 1."
            );
        }
    }
}
