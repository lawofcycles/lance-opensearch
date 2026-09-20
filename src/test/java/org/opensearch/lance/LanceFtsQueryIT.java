/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.EnumSet;
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

            // _count runs on the fragment path with track_total_hits:
            // true and must agree with hits.total.value.
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

    public void testLanceMatchHonoursTrackTotalHits() throws Exception {
        // 12 rows in three fragments, six of them "hello". The default
        // bound (10,000) is out of reach for a fixture this size, so an
        // explicit track_total_hits: 3 stands in for it: the executor
        // stops counting past the bound and the coordinator reports
        // the capped value with relation gte. Every size is covered
        // because the executor takes a different count path for each:
        // size 10 (the bounded hits scan returns all 6, short of its
        // limit, so its own count is exact), size 2 (the hits scan is
        // clipped, so a count scan limited to bound + 1 runs), size 0
        // (no hits scan; the same limited count scan runs) and
        // size 0 with an aggregation (the count comes from the
        // aggregation's scan).
        try (LanceTestCluster fixture = LanceTestCluster.setUpMultiFragment(12, 4, "lmatchtrackhits")) {
            String indexName = fixture.indexName();
            String hello = "{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}";
            String terms = ",\"aggs\":{\"ids\":{\"terms\":{\"field\":\"id\",\"size\":20}}}";

            for (String shape : new String[] { "\"size\":10", "\"size\":2", "\"size\":0", "\"size\":0" + terms }) {
                String bounded = readAll(
                    postJson("/" + indexName + "/_search", "{" + shape + ",\"track_total_hits\":3,\"query\":" + hello + "}")
                );
                assertEquals(
                    "track_total_hits:3 caps the value (" + shape + "): " + bounded,
                    3,
                    extractIntPath(bounded, "hits", "total", "value")
                );
                assertEquals("track_total_hits:3 relation (" + shape + "): " + bounded, "gte", totalRelation(bounded));

                String accurate = readAll(
                    postJson("/" + indexName + "/_search", "{" + shape + ",\"track_total_hits\":true,\"query\":" + hello + "}")
                );
                assertEquals(
                    "track_total_hits:true is exact (" + shape + "): " + accurate,
                    6,
                    extractIntPath(accurate, "hits", "total", "value")
                );
                assertEquals("track_total_hits:true relation (" + shape + "): " + accurate, "eq", totalRelation(accurate));

                String omitted = readAll(postJson("/" + indexName + "/_search", "{" + shape + ",\"query\":" + hello + "}"));
                assertEquals(
                    "default bound is exact below 10,000 (" + shape + "): " + omitted,
                    6,
                    extractIntPath(omitted, "hits", "total", "value")
                );
                assertEquals("default bound relation (" + shape + "): " + omitted, "eq", totalRelation(omitted));

                String disabled = readAll(
                    postJson("/" + indexName + "/_search", "{" + shape + ",\"track_total_hits\":false,\"query\":" + hello + "}")
                );
                assertFalse("track_total_hits:false omits hits.total (" + shape + "): " + disabled, disabled.contains("\"total\":{"));
                assertTrue("hits array is still present (" + shape + "): " + disabled, disabled.contains("\"hits\":["));
            }

            // A bound the total does not reach stays exact.
            String wide = readAll(postJson("/" + indexName + "/_search", "{\"size\":0,\"track_total_hits\":100,\"query\":" + hello + "}"));
            assertEquals(6, extractIntPath(wide, "hits", "total", "value"));
            assertEquals("eq", totalRelation(wide));

            // With the aggregation the buckets themselves are unaffected
            // by the bound: six hello rows, one bucket each.
            String aggBounded = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":0,\"track_total_hits\":3,\"query\":" + hello + terms + "}")
            );
            assertEquals("terms buckets ignore track_total_hits: " + aggBounded, 6, countOccurrences(aggBounded, "\"doc_count\":1"));

            // The bound also applies to a bool collapsed into a
            // prefiltered FTS scan (rows 4, 6, 8, 10 are the hello
            // rows with id >= 4).
            String prefiltered = "{\"bool\":{\"must\":[" + hello + "],\"filter\":[{\"range\":{\"id\":{\"gte\":4}}}]}}";
            String prefilteredBounded = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":2,\"track_total_hits\":3,\"query\":" + prefiltered + "}")
            );
            assertEquals(3, extractIntPath(prefilteredBounded, "hits", "total", "value"));
            assertEquals("gte", totalRelation(prefilteredBounded));
            String prefilteredExact = readAll(postJson("/" + indexName + "/_search", "{\"size\":2,\"query\":" + prefiltered + "}"));
            assertEquals(4, extractIntPath(prefilteredExact, "hits", "total", "value"));
            assertEquals("eq", totalRelation(prefilteredExact));
        }
    }

    private static String totalRelation(String searchBody) throws IOException {
        try (XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, searchBody)) {
            Map<String, Object> map = parser.map();
            @SuppressWarnings("unchecked")
            Map<String, Object> total = (Map<String, Object>) ((Map<String, Object>) map.get("hits")).get("total");
            return (String) total.get("relation");
        }
    }

    public void testLanceMatchWithScalarFilterMatchesLuceneComposition() throws Exception {
        // bool { must: [lance_match], filter: [term] } is collapsed on
        // the executor into one Lance FTS scan with a SQL prefilter.
        // The control query wraps the same filter in constant_score,
        // which the SQL translator refuses, so it runs as the plain
        // Lucene BooleanQuery over the unfiltered FTS scan. Both
        // must agree on ids, order, scores and the total.
        try (LanceTestCluster fixture = LanceTestCluster.setUpMultiFragment(12, 4, "lmatchprefilter")) {
            String indexName = fixture.indexName();
            String fts = "{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}";

            // term on the id column: one hit, row 4 lives at fragment 1 offset 0.
            String termFilter = "{\"term\":{\"id\":4}}";
            assertPushdownMatchesControl(indexName, fts, termFilter, null, List.of("1-0"));

            // range keeps rows 4..11; the hello rows among them are 4, 6, 8, 10.
            String rangeFilter = "{\"range\":{\"id\":{\"gte\":4}}}";
            assertPushdownMatchesControl(indexName, fts, rangeFilter, null, List.of("1-0", "1-2", "2-0", "2-2"));

            // A filter that excludes every hello row: empty result on both paths.
            String oddOnly = "{\"terms\":{\"id\":[1,3,5,7,9,11]}}";
            assertPushdownMatchesControl(indexName, fts, oddOnly, null, List.of());

            // The one-hit token 4 with a filter that keeps it and one that drops it.
            String token4 = "{\"lance_match\":{\"field\":\"body\",\"query\":\"4\"}}";
            assertPushdownMatchesControl(indexName, token4, "{\"range\":{\"id\":{\"lt\":8}}}", null, List.of("1-0"));
            assertPushdownMatchesControl(indexName, token4, "{\"range\":{\"id\":{\"gte\":8}}}", null, List.of());
        }
    }

    public void testLanceMatchWithRangeFilterAndMustNotMatchesLuceneComposition() throws Exception {
        // bool { must: [lance_match], filter: [range], must_not: [term] }:
        // rows 2..10 minus 6 leaves the hello rows 2, 4, 8, 10.
        try (LanceTestCluster fixture = LanceTestCluster.setUpMultiFragment(12, 4, "lmatchprefilternot")) {
            String indexName = fixture.indexName();
            String fts = "{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}";
            String rangeFilter = "{\"range\":{\"id\":{\"gte\":2,\"lte\":10}}}";
            String mustNot = "{\"term\":{\"id\":6}}";
            assertPushdownMatchesControl(indexName, fts, rangeFilter, mustNot, List.of("0-2", "1-0", "2-0", "2-2"));

            // must_not alone (no filter clause) is pushed down as well.
            String pushed = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":10,\"query\":{\"bool\":{\"must\":[" + fts + "],\"must_not\":[" + mustNot + "]}}}"
                )
            );
            assertEquals(5, extractIntPath(pushed, "hits", "total", "value"));
            assertEquals(Set.of("0-0", "0-2", "1-0", "2-0", "2-2"), new HashSet<>(idsOf(hitsOf(pushed))));

            // A boost on the FTS clause is kept: scores double, ids stay.
            String boosted = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":10,\"query\":{\"bool\":{\"must\":[{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\",\"boost\":2.0}}],"
                        + "\"filter\":["
                        + rangeFilter
                        + "],\"must_not\":["
                        + mustNot
                        + "]}}}"
                )
            );
            String plain = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":10,\"query\":{\"bool\":{\"must\":["
                        + fts
                        + "],\"filter\":["
                        + rangeFilter
                        + "],\"must_not\":["
                        + mustNot
                        + "]}}}"
                )
            );
            assertEquals(idsOf(hitsOf(plain)), idsOf(hitsOf(boosted)));
            List<Double> plainScores = scoresOf(plain);
            List<Double> boostedScores = scoresOf(boosted);
            for (int i = 0; i < plainScores.size(); i++) {
                assertEquals("boost 2.0 doubles the score at rank " + i, plainScores.get(i) * 2d, boostedScores.get(i), 1e-4);
            }
        }
    }

    public void testLanceMatchSortAndAggregationsMatchScalarReference() throws Exception {
        // Three fragments of 200 rows. grp3 matches 8 rows per fragment
        // (4 percent, below the reader's sparse ratio), so the sort and
        // aggregation columns are fetched for those rows only. The
        // reference is a terms filter on id selecting the same rows,
        // which runs on the scalar path and never sees a hint; the
        // constant_score wrapping of the same FTS clause is compared as
        // well. Ids, sort values and buckets must agree on every path.
        try (LanceTestCluster fixture = LanceTestCluster.setUpHintFixture(3, 200, "lmatchhintsort")) {
            String indexName = fixture.indexName();
            String fts = "{\"lance_match\":{\"field\":\"body\",\"query\":\"grp3\"}}";
            String constantScore = "{\"constant_score\":{\"filter\":" + fts + "}}";
            StringBuilder ids = new StringBuilder();
            for (int i = 3; i < 600; i += 25) {
                if (ids.length() > 0) {
                    ids.append(',');
                }
                ids.append(i);
            }
            String reference = "{\"terms\":{\"id\":[" + ids + "]}}";

            // Numeric sort: rating is distinct per row and never null on
            // grp3 rows; id breaks no ties but keeps the order total.
            String ratingSort = "\"sort\":[{\"rating\":\"desc\"},{\"id\":\"asc\"}]";
            String ftsByRating = readAll(postJson("/" + indexName + "/_search", "{\"size\":30,\"query\":" + fts + "," + ratingSort + "}"));
            String refByRating = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":30,\"query\":" + reference + "," + ratingSort + "}")
            );
            String csByRating = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":30,\"query\":" + constantScore + "," + ratingSort + "}")
            );
            assertEquals(24, extractIntPath(ftsByRating, "hits", "total", "value"));
            assertEquals(24, hitsOf(ftsByRating).size());
            assertEquals(idsAndSortValuesOf(refByRating), idsAndSortValuesOf(ftsByRating));
            assertEquals(idsAndSortValuesOf(refByRating), idsAndSortValuesOf(csByRating));
            // Row 378 has the highest rating among grp3 rows: 378 * 37 mod 1000 = 986.
            assertEquals("1-178 [986, 378]", idsAndSortValuesOf(ftsByRating).get(0));

            // Keyword sort: category is null on rows with id % 4 == 3, so
            // missing docs and ties are part of the comparison.
            String categorySort = "\"sort\":[{\"category\":\"asc\"},{\"id\":\"asc\"}]";
            String ftsByCategory = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":30,\"query\":" + fts + "," + categorySort + "}")
            );
            String refByCategory = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":30,\"query\":" + reference + "," + categorySort + "}")
            );
            String csByCategory = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":30,\"query\":" + constantScore + "," + categorySort + "}")
            );
            assertEquals(idsAndSortValuesOf(refByCategory), idsAndSortValuesOf(ftsByCategory));
            assertEquals(idsAndSortValuesOf(refByCategory), idsAndSortValuesOf(csByCategory));

            // A page smaller than the hits of one fragment makes Lucene
            // look the bottom term up on later fragments before scoring
            // starts; those fragments read the full dictionary and the
            // page must still agree.
            String smallPage = readAll(postJson("/" + indexName + "/_search", "{\"size\":5,\"query\":" + fts + "," + categorySort + "}"));
            assertEquals(idsAndSortValuesOf(refByCategory).subList(0, 5), idsAndSortValuesOf(smallPage));

            // Aggregations over keyword, multi-valued keyword, numeric and
            // boolean columns, with size 0 (no hits phase before the
            // aggregator is built) and with a hits page in front.
            String aggs = "\"aggs\":{"
                + "\"by_category\":{\"terms\":{\"field\":\"category\",\"size\":10}},"
                + "\"by_tag\":{\"terms\":{\"field\":\"tags\",\"size\":10}},"
                + "\"by_rating\":{\"terms\":{\"field\":\"rating\",\"size\":50}},"
                + "\"by_flag\":{\"terms\":{\"field\":\"flag\",\"size\":10}},"
                + "\"avg_rating\":{\"avg\":{\"field\":\"rating\"}}}";
            for (String size : List.of("0", "5")) {
                String ftsAggs = readAll(
                    postJson("/" + indexName + "/_search", "{\"size\":" + size + ",\"query\":" + fts + "," + aggs + "}")
                );
                String refAggs = readAll(
                    postJson("/" + indexName + "/_search", "{\"size\":" + size + ",\"query\":" + reference + "," + aggs + "}")
                );
                for (String name : List.of("by_category", "by_tag", "by_rating", "by_flag")) {
                    assertEquals("size " + size + ", " + name, bucketsOf(refAggs, name), bucketsOf(ftsAggs, name));
                }
                assertEquals(
                    extractDoublePath(refAggs, "aggregations", "avg_rating", "value"),
                    extractDoublePath(ftsAggs, "aggregations", "avg_rating", "value"),
                    1e-9
                );
                assertEquals(24, extractIntPath(ftsAggs, "hits", "total", "value"));
            }
            // grp3 rows: id % 3 cycles, id % 4 == 3 is null. 24 rows, 6 null.
            String ftsCategory = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"query\":" + fts + ",\"aggs\":{\"by_category\":{\"terms\":{\"field\":\"category\",\"size\":10}}}}"
                )
            );
            assertEquals(List.of("c0=6", "c1=6", "c2=6"), bucketsOf(ftsCategory, "by_category"));
            // The map execution mode builds no global ordinals, so every
            // fragment reads the hinted rows only; the buckets must agree.
            String mapMode = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"query\":"
                        + fts
                        + ",\"aggs\":{\"by_category\":{\"terms\":{\"field\":\"category\",\"size\":10,\"execution_hint\":\"map\"}}}}"
                )
            );
            assertEquals(List.of("c0=6", "c1=6", "c2=6"), bucketsOf(mapMode, "by_category"));

            // One hit with size 0: no hits phase runs before the
            // aggregator is built, so the hit set the executor delivers
            // ahead of the aggregators is the only hint the keyword
            // dictionary can use. The constant_score wrapping is not a
            // bare Lance clause and gets no early hint, so it is the
            // hint-free reference. Row 250 is c1.
            String singleTerms = ",\"aggs\":{\"by_category\":{\"terms\":{\"field\":\"category\",\"size\":10}}}}";
            String singleFts = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"tok250\"}}" + singleTerms
                )
            );
            String singleCs = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"query\":{\"constant_score\":{\"filter\":{\"lance_match\":{\"field\":\"body\",\"query\":\"tok250\"}}}}"
                        + singleTerms
                )
            );
            assertEquals(List.of("c1=1"), bucketsOf(singleFts, "by_category"));
            assertEquals(bucketsOf(singleCs, "by_category"), bucketsOf(singleFts, "by_category"));
            assertEquals(1, extractIntPath(singleFts, "hits", "total", "value"));

            // post_filter narrows the page to the c0 rows but leaves the
            // aggregation over every grp3 row; the early hint covers both
            // because the page can only lose rows from the hit set.
            String postFilter = ",\"post_filter\":{\"term\":{\"category\":\"c0\"}}";
            String postFilteredFts = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":30,\"query\":" + fts + postFilter + "," + categorySort + singleTerms)
            );
            String postFilteredRef = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":30,\"query\":" + reference + postFilter + "," + categorySort + singleTerms
                )
            );
            assertEquals(6, extractIntPath(postFilteredFts, "hits", "total", "value"));
            assertEquals(6, hitsOf(postFilteredFts).size());
            assertEquals(idsAndSortValuesOf(postFilteredRef), idsAndSortValuesOf(postFilteredFts));
            assertEquals(List.of("c0=6", "c1=6", "c2=6"), bucketsOf(postFilteredFts, "by_category"));

            // One hit: row 250 is fragment 1, offset 50.
            String single = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":10,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"tok250\"}}," + ratingSort + "}"
                )
            );
            assertEquals(List.of("1-50 [250, 250]"), idsAndSortValuesOf(single));

            // Every row matches hello (above the sparse ratio): the full
            // column path answers and agrees with match_all.
            String dense = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":10,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}," + ratingSort + "}"
                )
            );
            String matchAll = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"match_all\":{}}," + ratingSort + "}")
            );
            assertEquals(600, extractIntPath(dense, "hits", "total", "value"));
            assertEquals(idsAndSortValuesOf(matchAll), idsAndSortValuesOf(dense));
        }
    }

    public void testLanceMatchInsideDisjunctionsSortsLikeTheScalarReference() throws Exception {
        // Shapes where Lucene collects docs the FTS scorer did not
        // produce: a should with a second clause, and a should beside a
        // filter (minimum_should_match 0, so the filter alone decides).
        // The reader must answer those docs from the full column and
        // agree with the scalar reference for the same rows.
        try (LanceTestCluster fixture = LanceTestCluster.setUpHintFixture(3, 200, "lmatchhintunion")) {
            String indexName = fixture.indexName();
            String fts = "{\"lance_match\":{\"field\":\"body\",\"query\":\"grp3\"}}";
            String categorySort = "\"sort\":[{\"category\":\"asc\"},{\"id\":\"asc\"}]";
            String ratingSort = "\"sort\":[{\"rating\":\"desc\"},{\"id\":\"asc\"}]";

            StringBuilder unionIds = new StringBuilder("1");
            for (int i = 3; i < 600; i += 25) {
                unionIds.append(',').append(i);
            }
            String union = "{\"bool\":{\"should\":[" + fts + ",{\"term\":{\"id\":1}}]}}";
            String unionReference = "{\"terms\":{\"id\":[" + unionIds + "]}}";
            for (String sort : List.of(categorySort, ratingSort)) {
                String actual = readAll(postJson("/" + indexName + "/_search", "{\"size\":30,\"query\":" + union + "," + sort + "}"));
                String expected = readAll(
                    postJson("/" + indexName + "/_search", "{\"size\":30,\"query\":" + unionReference + "," + sort + "}")
                );
                assertEquals(25, extractIntPath(actual, "hits", "total", "value"));
                assertEquals(sort, idsAndSortValuesOf(expected), idsAndSortValuesOf(actual));
            }

            String shouldWithFilter = "{\"bool\":{\"should\":[" + fts + "],\"filter\":[{\"range\":{\"id\":{\"lt\":60}}}]}}";
            String filterReference = "{\"range\":{\"id\":{\"lt\":60}}}";
            for (String sort : List.of(categorySort, ratingSort)) {
                String actual = readAll(
                    postJson("/" + indexName + "/_search", "{\"size\":60,\"query\":" + shouldWithFilter + "," + sort + "}")
                );
                String expected = readAll(
                    postJson("/" + indexName + "/_search", "{\"size\":60,\"query\":" + filterReference + "," + sort + "}")
                );
                assertEquals(60, extractIntPath(actual, "hits", "total", "value"));
                assertEquals(sort, idsAndSortValuesOf(expected), idsAndSortValuesOf(actual));
            }
            String unionAggs = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"query\":" + union + ",\"aggs\":{\"by_category\":{\"terms\":{\"field\":\"category\",\"size\":10}}}}"
                )
            );
            String unionAggsReference = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"query\":"
                        + unionReference
                        + ",\"aggs\":{\"by_category\":{\"terms\":{\"field\":\"category\",\"size\":10}}}}"
                )
            );
            assertEquals(bucketsOf(unionAggsReference, "by_category"), bucketsOf(unionAggs, "by_category"));
        }
    }

    /**
     * Runs {@code bool { must: [fts], filter: [filter], must_not: [mustNot] }}
     * twice: once as written (collapsed into a prefiltered Lance FTS
     * scan on the executor) and once with the filter wrapped in
     * {@code constant_score} so the SQL translator refuses it and the
     * bool stays a Lucene BooleanQuery. Asserts the two agree on ids
     * (in score order), scores, {@code hits.total.value} with and
     * without {@code size:0}, and that the ids are {@code expectedIds}.
     */
    private static void assertPushdownMatchesControl(String indexName, String fts, String filter, String mustNot, List<String> expectedIds)
        throws IOException {
        String mustNotClause = mustNot == null ? "" : ",\"must_not\":[" + mustNot + "]";
        String pushdownBool = "{\"bool\":{\"must\":[" + fts + "],\"filter\":[" + filter + "]" + mustNotClause + "}}";
        String controlBool = "{\"bool\":{\"must\":["
            + fts
            + "],\"filter\":[{\"constant_score\":{\"filter\":"
            + filter
            + "}}]"
            + mustNotClause
            + "}}";

        String pushed = readAll(postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":" + pushdownBool + "}"));
        String control = readAll(postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":" + controlBool + "}"));

        List<String> pushedIds = idsOf(hitsOf(pushed));
        assertEquals("ids in score order, pushdown vs Lucene: " + pushed, idsOf(hitsOf(control)), pushedIds);
        assertEquals("expected id set: " + pushed, new HashSet<>(expectedIds), new HashSet<>(pushedIds));
        assertEquals(expectedIds.size(), extractIntPath(pushed, "hits", "total", "value"));
        assertEquals(extractIntPath(control, "hits", "total", "value"), extractIntPath(pushed, "hits", "total", "value"));

        List<Double> pushedScores = scoresOf(pushed);
        List<Double> controlScores = scoresOf(control);
        assertEquals(controlScores.size(), pushedScores.size());
        for (int i = 0; i < pushedScores.size(); i++) {
            assertEquals("score at rank " + i + ": " + pushed, controlScores.get(i), pushedScores.get(i), 1e-4);
            if (i > 0) {
                assertTrue("_score must be non-increasing, saw " + pushedScores, pushedScores.get(i - 1) >= pushedScores.get(i));
            }
        }

        String pushedCount = readAll(postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":" + pushdownBool + "}"));
        String controlCount = readAll(postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":" + controlBool + "}"));
        assertEquals(expectedIds.size(), extractIntPath(pushedCount, "hits", "total", "value"));
        assertEquals(extractIntPath(controlCount, "hits", "total", "value"), extractIntPath(pushedCount, "hits", "total", "value"));
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
        try (SurfacedIndex fixture = SurfacedIndex.japanese("jabtree")) {
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
        try (SurfacedIndex fixture = SurfacedIndex.japanese("jasimple")) {
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
        try (SurfacedIndex fixture = SurfacedIndex.japanese("jaicu")) {
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
        try (SurfacedIndex fixture = SurfacedIndex.japanese("jalindera")) {
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
        // reaches the caller as 400 with Lance's own wording, under
        // failed.fts so the caller can tell which column it was.
        try (SurfacedIndex fixture = SurfacedIndex.japanese("jabadtok")) {
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
            assertTrue(
                "expected the column under failed.fts: " + body,
                body.contains("\"failed\":{\"fts\":[{\"column\":\"text\",\"reason\":\"")
            );
            assertTrue("expected an empty fts built list: " + body, body.contains("\"built\":{\"fts\":[]"));

            // Nothing was committed: the column is still keyword.
            String mapping = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_mapping")));
            assertTrue(
                "text must still be keyword after the rejected build: " + mapping,
                mapping.contains("\"text\":{\"type\":\"keyword\"")
            );
        }
    }

    public void testBuildIndexesOnReadOnlyTableAnswers500WithLanceMessage() throws Exception {
        // The OpenSearch process can read the table but not write into
        // it. Lance's CreateIndex fails with an I/O error, which must
        // reach the caller as 500 with the column under failed.scalar
        // and Lance's message, not as 200 with an empty built list.
        try (SurfacedIndex fixture = SurfacedIndex.japanese("jareadonly")) {
            String indexName = fixture.indexName();
            Path table = fixture.tablePath();
            setReadOnlyRecursively(table);
            try {
                assumeFalse(
                    "the table stayed writable after chmod (running as root?), so the write cannot be refused",
                    canCreateFileIn(table)
                );
                ResponseException failure = expectThrows(
                    ResponseException.class,
                    () -> postJson("/_lance/build_indexes/" + indexName, "{\"columns\":[\"id\"]}")
                );
                int status = failure.getResponse().getStatusLine().getStatusCode();
                String body = readAll(failure.getResponse());
                assertEquals("expected 500 for a refused write, saw " + status + ": " + body, 500, status);
                assertTrue("expected an empty built list: " + body, body.contains("\"built\":{\"fts\":[],\"scalar\":[],\"vector\":[]}"));
                assertTrue(
                    "expected id under failed.scalar: " + body,
                    body.contains("\"failed\":{\"fts\":[],\"scalar\":[{\"column\":\"id\",\"reason\":\"")
                );
                assertTrue("expected Lance's permission message: " + body, body.contains("Permission denied"));
            } finally {
                setWritableRecursively(table);
            }
        }
    }

    public void testBuildIndexesReportsAlreadyIndexedColumnAsSkipped() throws Exception {
        // First build covers id only. The second build, without a column
        // filter, builds text and reports id as skipped with the reason,
        // and answers 200 because a skip is not a failure.
        try (SurfacedIndex fixture = SurfacedIndex.japanese("jaskipped")) {
            String indexName = fixture.indexName();
            String first = readAll(postJson("/_lance/build_indexes/" + indexName, "{\"columns\":[\"id\"]}"));
            assertTrue("expected id built: " + first, first.contains("\"built\":{\"fts\":[],\"scalar\":[\"id\"],\"vector\":[]}"));
            assertTrue("expected nothing skipped: " + first, first.contains("\"skipped\":{\"fts\":[],\"scalar\":[],\"vector\":[]}"));
            assertTrue("expected nothing failed: " + first, first.contains("\"failed\":{\"fts\":[],\"scalar\":[],\"vector\":[]}"));

            Response second = postJson("/_lance/build_indexes/" + indexName, "{}");
            assertEquals(200, second.getStatusLine().getStatusCode());
            String body = readAll(second);
            assertTrue("expected text built: " + body, body.contains("\"built\":{\"fts\":[],\"scalar\":[\"text\"],\"vector\":[]}"));
            assertTrue(
                "expected id skipped with the reason: " + body,
                body.contains(
                    "\"skipped\":{\"fts\":[],\"scalar\":[{\"column\":\"id\",\"reason\":\""
                        + "scalar index already exists; use optimize=true to extend it over new fragments\"}],\"vector\":[]}"
                )
            );
            assertTrue("expected nothing failed: " + body, body.contains("\"failed\":{\"fts\":[],\"scalar\":[],\"vector\":[]}"));
        }
    }

    public void testBuildIndexesWithPositionEnablesLanceMatchPhrase() throws Exception {
        // with_position: true makes Lance store token positions, so a
        // phrase query resolves. Labels are "row-i"; the simple tokenizer
        // splits them into "row" and "i", so "row 3" is a two-token
        // phrase that only row 3 satisfies and "3 row" nothing does.
        try (SurfacedIndex fixture = SurfacedIndex.keywordOnly("withpos", 5)) {
            String indexName = fixture.indexName();
            String build = readAll(postJson("/_lance/build_indexes/" + indexName, "{\"fts_columns\":[\"label\"],\"with_position\":true}"));
            assertTrue("expected label in fts built list: " + build, build.contains("\"fts\":[\"label\"]"));
            awaitLanceTextMapping(indexName, "label");

            String ordered = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"lance_match_phrase\":{\"field\":\"label\",\"query\":\"row 3\"}}}")
            );
            assertEquals("phrase 'row 3' must hit row 3 only: " + ordered, 1, extractIntPath(ordered, "hits", "total", "value"));
            assertEquals(3, extractIntPath(ordered, "hits", "hits", "0", "_source", "id"));

            String reversed = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"lance_match_phrase\":{\"field\":\"label\",\"query\":\"3 row\"}}}")
            );
            assertEquals("reversed phrase must hit nothing: " + reversed, 0, extractIntPath(reversed, "hits", "total", "value"));
        }
    }

    public void testBuildIndexesWithoutPositionRejectsLanceMatchPhraseWithLanceMessage() throws Exception {
        // Lance's default (and the plugin's) is no positions. Term
        // queries work; a phrase query is refused by Lance at query
        // time and the caller sees Lance's wording. The status is not
        // pinned: LanceFtsQuery.ensureShardScan wraps Lance's
        // IllegalArgumentException in an IOException, which OpenSearch
        // maps to 500 today, and turning that into the 400 an input
        // error deserves is a change to LanceFtsQuery, not to the build.
        try (SurfacedIndex fixture = SurfacedIndex.keywordOnly("nopos", 5)) {
            String indexName = fixture.indexName();
            String build = readAll(postJson("/_lance/build_indexes/" + indexName, "{\"fts_columns\":[\"label\"]}"));
            assertTrue("expected label in fts built list: " + build, build.contains("\"fts\":[\"label\"]"));
            awaitLanceTextMapping(indexName, "label");

            String term = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"lance_match\":{\"field\":\"label\",\"query\":\"3\"}}}")
            );
            assertEquals("term query needs no positions: " + term, 1, extractIntPath(term, "hits", "total", "value"));

            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_match_phrase\":{\"field\":\"label\",\"query\":\"row 3\"}}}"
                )
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            String body = readAll(failure.getResponse());
            assertTrue("expected an error status for a phrase query without positions, saw " + status + ": " + body, status >= 400);
            assertTrue(
                "expected Lance's message about positions: " + body,
                body.contains("position is not found but required for phrase queries")
            );
        }
    }

    public void testBuildIndexesRejectsMalformedWithPosition() throws Exception {
        try (SurfacedIndex fixture = SurfacedIndex.japanese("jawithposopt")) {
            String indexName = fixture.indexName();
            // Shape check happens in the REST layer.
            assertBuildIndexesRejected(
                indexName,
                "{\"fts_columns\":[\"text\"],\"with_position\":\"yes\"}",
                "with_position must be a boolean"
            );
            // with_position only shapes indexes this request creates.
            assertBuildIndexesRejected(indexName, "{\"with_position\":true}", "name them in fts_columns");
        }
    }

    /**
     * Takes write permission away from every file and directory under
     * {@code root} (owner read, plus execute on directories). Lance's local
     * object store then gets EACCES when it tries to create the index
     * directory or the new manifest.
     */
    private static void setReadOnlyRecursively(Path root) throws IOException {
        try (var stream = Files.walk(root)) {
            for (Path p : (Iterable<Path>) stream::iterator) {
                Files.setPosixFilePermissions(
                    p,
                    Files.isDirectory(p)
                        ? EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_EXECUTE)
                        : EnumSet.of(PosixFilePermission.OWNER_READ)
                );
            }
        }
    }

    /** Undoes {@link #setReadOnlyRecursively} so the fixture can delete the tree. */
    private static void setWritableRecursively(Path root) throws IOException {
        try (var stream = Files.walk(root)) {
            for (Path p : (Iterable<Path>) stream::iterator) {
                Files.setPosixFilePermissions(
                    p,
                    Files.isDirectory(p)
                        ? EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE)
                        : EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE)
                );
            }
        }
    }

    /**
     * True when this process can still create a file in {@code dir}. A
     * root user ignores the mode bits, in which case the read-only test
     * cannot observe a refused write and skips itself.
     */
    private static boolean canCreateFileIn(Path dir) throws IOException {
        Path probe = dir.resolve("write-probe");
        try {
            Files.createFile(probe);
        } catch (IOException denied) {
            return false;
        }
        Files.delete(probe);
        return true;
    }

    public void testBuildIndexesRejectsMalformedFtsColumnsAndTokenizer() throws Exception {
        try (SurfacedIndex fixture = SurfacedIndex.japanese("jatokopt")) {
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
        awaitLanceTextMapping(indexName, "text");
    }

    private static void awaitLanceTextMapping(String indexName, String column) throws Exception {
        assertBusy(() -> {
            String mapping;
            try {
                mapping = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_mapping")));
            } catch (ResponseException e) {
                throw new AssertionError("index " + indexName + " is between delete and recreate: " + e.getMessage());
            }
            assertTrue(
                "waiting for " + column + " to become lance_text: " + mapping,
                mapping.contains("\"" + column + "\":{\"type\":\"lance_text\"")
            );
        }, 30, TimeUnit.SECONDS);
        ensureGreen(indexName);
    }

    /**
     * Table without an FTS index, surfaced through a namespace
     * registration rather than attach, so the poll keeps following the
     * table after the build_indexes commit and the keyword to lance_text
     * rebuild. {@link #japanese} writes the five Japanese sentences into
     * a {@code text} column; {@link #keywordOnly} writes {@code row-i}
     * labels into a {@code label} column.
     */
    private static final class SurfacedIndex implements AutoCloseable {
        private final Path scratchDir;
        private final String indexName;

        private SurfacedIndex(Path scratchDir, String indexName) {
            this.scratchDir = scratchDir;
            this.indexName = indexName;
        }

        String indexName() {
            return indexName;
        }

        /** Filesystem path of the Lance table directory behind the index. */
        Path tablePath() {
            return scratchDir.resolve(indexName + ".lance");
        }

        static SurfacedIndex japanese(String testHint) throws Exception {
            return surface(testHint, "text", (dir, name) -> LanceTableFactory.writeJapaneseTable(dir, name));
        }

        static SurfacedIndex keywordOnly(String testHint, int rowCount) throws Exception {
            return surface(testHint, "label", (dir, name) -> LanceTableFactory.writeKeywordOnlyTable(dir, name, rowCount));
        }

        private interface TableWriter {
            void write(Path dir, String name) throws Exception;
        }

        private static SurfacedIndex surface(String testHint, String utf8Column, TableWriter writer) throws Exception {
            String suffix = testHint.toLowerCase(Locale.ROOT) + "-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
            Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
            String indexName = "demo-" + suffix;
            writer.write(scratchDir, indexName);

            Response register = postJson("/_lance/namespace", "{\"path\":\"" + scratchDir + "\"}");
            assertEquals("namespace register failed: " + readAll(register), 200, register.getStatusLine().getStatusCode());
            assertBusy(() -> {
                String cat = readAll(client().performRequest(new Request("GET", "/_cat/indices?format=json")));
                assertTrue("waiting for index " + indexName + ", saw: " + cat, cat.contains("\"" + indexName + "\""));
            });
            ensureGreen(indexName);
            String mapping = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_mapping")));
            assertTrue(
                utf8Column + " must start as keyword (no FTS index yet): " + mapping,
                mapping.contains("\"" + utf8Column + "\":{\"type\":\"keyword\"")
            );
            return new SurfacedIndex(scratchDir, indexName);
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
