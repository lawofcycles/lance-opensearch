/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.core.rest.RestStatus;

/**
 * Search request shapes served by the fragment dispatch path (scalar
 * filters, date ranges, unmapped fields, top-k, pagination, post_filter,
 * search_after, scripts, response envelope), the shapes it refuses, and
 * the mixed targets the stock search action keeps.
 */
public class LanceSearchDispatchIT extends LanceRestTestCase {

    public void testFragmentDispatchModeAnswersFilterQueries() throws Exception {
        // match_all and scalar filters (term / range / bool / match)
        // through the fragment dispatch path: hits.total.value and the
        // hits themselves are derived from the same Lance filter.
        String suffix = "dispatch-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            String matchAllBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match_all\":{}}}"));
            int matchAllHits = extractIntPath(matchAllBody, "hits", "total", "value");
            assertEquals("fragment path match_all must return the true row count", 6, matchAllHits);
            // Six rows, default size 10. The table has no primary key, so
            // _id is the synthesised "<fragmentId>-<offset>". The fixture
            // alternates "hello lance" and "quick brown fox" in body.
            assertTrue("fragment path match_all must populate the hits array: " + matchAllBody, matchAllBody.contains("\"_id\":\"0-0\""));
            assertTrue(
                "fragment path match_all must render _source with the body column: " + matchAllBody,
                matchAllBody.contains("hello lance")
            );
            assertTrue(
                "fragment path match_all must render _source with the id column: " + matchAllBody,
                matchAllBody.contains("\"id\":0")
            );

            String sizeBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match_all\":{}},\"size\":2}"));
            assertEquals("size clause must not affect total", 6, extractIntPath(sizeBody, "hits", "total", "value"));
            int returnedHits = countOccurrences(sizeBody, "\"_id\":");
            assertEquals("size=2 must return exactly two hits: " + sizeBody, 2, returnedHits);

            String termBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"term\":{\"id\":3}}}"));
            assertEquals("fragment path term must match exactly one row", 1, extractIntPath(termBody, "hits", "total", "value"));
            assertTrue("fragment path term must return the id=3 hit: " + termBody, termBody.contains("\"_id\":\"0-3\""));
            assertTrue("fragment path term must render the matching row: " + termBody, termBody.contains("\"id\":3"));

            // id in {2, 3, 4}
            String rangeBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"id\":{\"gte\":2,\"lt\":5}}}}"));
            assertEquals("fragment path range must count matching rows", 3, extractIntPath(rangeBody, "hits", "total", "value"));
            assertTrue("fragment path range must include id=2: " + rangeBody, rangeBody.contains("\"id\":2"));
            assertTrue("fragment path range must include id=4: " + rangeBody, rangeBody.contains("\"id\":4"));

            // Nested translation: id >= 2 AND id = 3 leaves one row.
            String boolBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"bool\":{\"filter\":[{\"range\":{\"id\":{\"gte\":2}}},{\"term\":{\"id\":3}}]}}}"
                )
            );
            assertEquals("fragment path bool must count the intersection", 1, extractIntPath(boolBody, "hits", "total", "value"));
            assertTrue("fragment path bool must return the id=3 hit: " + boolBody, boolBody.contains("\"_id\":\"0-3\""));

            // A stock match query on a lance_text field resolves to the
            // Lance FTS scorer through the field mapper.
            int matchHits = extractIntPath(
                readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match\":{\"body\":\"lance\"}}}")),
                "hits",
                "total",
                "value"
            );
            assertTrue("match query must return hits on fragment path (got " + matchHits + ")", matchHits > 0);
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAnswersDateRangeQuery() throws Exception {
        // ISO-8601 string literals in a range on a Lance Timestamp
        // column translate to `timestamp '...'` SQL, so the same range
        // DSL that works on a stock search path date field works here.
        String suffix = "date-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeDatedTable(scratchDir, tableName);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // Date-only literals: ids 2 (2024-03-10) and 3 (2024-03-25)
            // are the March rows.
            String dateOnly = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"ts\":{\"gte\":\"2024-03-01\",\"lt\":\"2024-04-01\"}}}}")
            );
            assertEquals(
                "date-only range must count only the two March rows: " + dateOnly,
                2,
                extractIntPath(dateOnly, "hits", "total", "value")
            );
            assertTrue("date-only range must include the id=2 hit: " + dateOnly, dateOnly.contains("\"id\":2"));
            assertTrue("date-only range must include the id=3 hit: " + dateOnly, dateOnly.contains("\"id\":3"));

            // Datetime literals: ids 1 and 2 fall inside
            // [2024-02-01T00:00:00Z, 2024-03-15T12:00:00Z); id 3 is above
            // the upper bound.
            String dateTime = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"range\":{\"ts\":{\"gte\":\"2024-02-01T00:00:00Z\",\"lt\":\"2024-03-15T12:00:00Z\"}}}}"
                )
            );
            assertEquals(
                "datetime range must count Feb + early-March rows only: " + dateTime,
                2,
                extractIntPath(dateTime, "hits", "total", "value")
            );
            assertTrue("datetime range must include the id=1 hit: " + dateTime, dateTime.contains("\"id\":1"));
            assertTrue("datetime range must include the id=2 hit: " + dateTime, dateTime.contains("\"id\":2"));

            // term category=odd ({1, 3, 5}) AND ts in
            // [2024-01-01, 2024-05-01) excludes id 5 (2024-05-30).
            String boolBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"bool\":{\"filter\":["
                        + "{\"term\":{\"category\":\"odd\"}},"
                        + "{\"range\":{\"ts\":{\"gte\":\"2024-01-01\",\"lt\":\"2024-05-01\"}}}"
                        + "]}}}"
                )
            );
            assertEquals(
                "bool filter must intersect the keyword and date-range subsets: " + boolBody,
                2,
                extractIntPath(boolBody, "hits", "total", "value")
            );
            assertTrue("bool filter must include id=1: " + boolBody, boolBody.contains("\"id\":1"));
            assertTrue("bool filter must include id=3: " + boolBody, boolBody.contains("\"id\":3"));

            // Monthly date_histogram: {Jan:1, Feb:1, Mar:2, Apr:1, May:1}.
            String hist = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"aggs\":{\"per_month\":{\"date_histogram\":"
                        + "{\"field\":\"ts\",\"calendar_interval\":\"month\",\"min_doc_count\":1}}}}"
                )
            );
            assertEquals("date_histogram must see every row: " + hist, 6, extractIntPath(hist, "hits", "total", "value"));
            int monthBuckets = countOccurrences(hist, "\"doc_count\":");
            assertEquals("date_histogram must open five monthly buckets: " + hist, 5, monthBuckets);
            assertTrue("date_histogram must carry a bucket with two docs (March): " + hist, hist.contains("\"doc_count\":2"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAnswersDateRangeQueryWithEpochMillisLiterals() throws Exception {
        // Numeric epoch-millis literals in a range on a date-mapped
        // column translate to to_timestamp_millis(...) SQL; the
        // translator decides from the field mapping, not the literal
        // shape.
        String suffix = "dateml-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeDatedTable(scratchDir, tableName);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // 2024-03-01T00:00:00Z = 1709251200000
            // 2024-04-01T00:00:00Z = 1711929600000
            // 2024-05-01T00:00:00Z = 1714521600000

            // ids 2 and 3 fall inside [1709251200000, 1711929600000).
            String numeric = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"ts\":{\"gte\":1709251200000,\"lt\":1711929600000}}}}")
            );
            assertEquals(
                "numeric range must count only the two March rows: " + numeric,
                2,
                extractIntPath(numeric, "hits", "total", "value")
            );
            assertTrue("numeric range must include id=2: " + numeric, numeric.contains("\"id\":2"));
            assertTrue("numeric range must include id=3: " + numeric, numeric.contains("\"id\":3"));

            // term category=odd ({1, 3, 5}) AND ts in
            // [2024-01-01, 2024-05-01) excludes id 5.
            String bool = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"bool\":{\"filter\":["
                        + "{\"term\":{\"category\":\"odd\"}},"
                        + "{\"range\":{\"ts\":{\"gte\":1704067200000,\"lt\":1714521600000}}}"
                        + "]}}}"
                )
            );
            assertEquals("bool + numeric range must intersect: " + bool, 2, extractIntPath(bool, "hits", "total", "value"));
            assertTrue("bool + numeric range must include id=1: " + bool, bool.contains("\"id\":1"));
            assertTrue("bool + numeric range must include id=3: " + bool, bool.contains("\"id\":3"));

            // The ISO-8601 form of the same interval resolves identically.
            String iso = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"ts\":{\"gte\":\"2024-03-01\",\"lt\":\"2024-04-01\"}}}}")
            );
            assertEquals("ISO-8601 regression fence: " + iso, 2, extractIntPath(iso, "hits", "total", "value"));
            assertTrue("ISO regression must include id=2: " + iso, iso.contains("\"id\":2"));
            assertTrue("ISO regression must include id=3: " + iso, iso.contains("\"id\":3"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testZoneMapPruningSkipsFragmentsWithoutChangingTheAnswer() throws Exception {
        // Four fragments of 100 rows with a zone map on id (ids
        // contiguous per fragment): a range on id excludes whole
        // fragments at the coordinator, the executors skip them, and
        // every shape answers exactly what the unpruned scan answers.
        String suffix = "prune-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeZoneMappedFixtureTable(scratchDir, tableName, 4, 100, 50);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // id >= 250 lives in fragments 2 and 3 only; 0 and 1 are pruned.
            String range = "{\"range\":{\"id\":{\"gte\":250}}}";
            long before = prunedFragments();

            String hits = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":" + range + ",\"size\":200,\"track_total_hits\":true}")
            );
            assertEquals("the range matches ids 250..399: " + hits, 150, extractIntPath(hits, "hits", "total", "value"));
            assertEquals(150, countOccurrences(hits, "\"_id\":"));
            assertFalse("no pruned row is returned: " + hits, hits.contains("\"id\":249,") || hits.contains("\"id\":100,"));
            assertTrue("a kept row is returned: " + hits, hits.contains("\"id\":250,") || hits.contains("\"id\":250}"));
            assertEquals("two fragments skipped on the hits request", before + 2, prunedFragments());

            String count = readAll(postJson("/" + indexName + "/_count", "{\"query\":" + range + "}"));
            assertEquals("_count sees the same rows: " + count, 150, extractIntPath(count, "count"));
            assertEquals("two fragments skipped on the count", before + 4, prunedFragments());

            // The pushed aggregate over the pruned scan: sum(250..399).
            String sum = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":" + range + ",\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}")
            );
            assertEquals(150, extractIntPath(sum, "hits", "total", "value"));
            assertEquals((250 + 399) * 150 / 2, extractIntPath(sum, "aggregations", "s", "value"));
            assertEquals("two fragments skipped on the aggregate", before + 6, prunedFragments());

            // A sorted page over the pruned scan starts at the lowest kept id.
            String page = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":" + range + ",\"size\":3,\"sort\":[{\"id\":\"asc\"}]}")
            );
            assertEquals(List.of(250, 251, 252), sortValuesOf(page));
            assertEquals("two fragments skipped on the page", before + 8, prunedFragments());

            // A full text clause with the range as its prefilter: every
            // row carries the token, so the prefilter decides the count.
            String fts = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"track_total_hits\":true,\"query\":{\"bool\":{\"must\":[{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}],"
                        + "\"filter\":["
                        + range
                        + "]}}}"
                )
            );
            assertEquals("the prefilter decides the count: " + fts, 150, extractIntPath(fts, "hits", "total", "value"));
            assertEquals("two fragments skipped on the full text request", before + 10, prunedFragments());

            // Every fragment pruned: the answer is the empty one, with the
            // aggregations block an empty table produces.
            String none = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"query\":{\"range\":{\"id\":{\"gte\":1000}}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}"
                )
            );
            assertEquals(0, extractIntPath(none, "hits", "total", "value"));
            assertEquals(0, extractIntPath(none, "aggregations", "s", "value"));
            assertEquals("four fragments skipped when nothing can match", before + 14, prunedFragments());

            // A range inside every fragment prunes nothing.
            String all = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"id\":{\"gte\":50}}},\"size\":0,\"track_total_hits\":true}")
            );
            assertEquals(350, extractIntPath(all, "hits", "total", "value"));
            assertEquals("nothing skipped when every fragment may match", before + 14, prunedFragments());
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    /** The first sort value of every hit of {@code searchBody}, in hit order. */
    @SuppressWarnings("unchecked")
    private static List<Integer> sortValuesOf(String searchBody) {
        Map<String, Object> map = parseJson(searchBody);
        List<Object> hits = (List<Object>) ((Map<String, Object>) map.get("hits")).get("hits");
        List<Integer> values = new java.util.ArrayList<>(hits.size());
        for (Object hit : hits) {
            List<Object> sort = (List<Object>) ((Map<String, Object>) hit).get("sort");
            values.add(((Number) sort.get(0)).intValue());
        }
        return values;
    }

    public void testFragmentDispatchModeAnswersQueriesAgainstUnmappedFieldsWithoutError() throws Exception {
        // A range against an unmapped field behaves as on the shard
        // path: RangeQueryBuilder.doRewrite folds it to match_none and
        // the response is 200 with zero hits, also when the clause is
        // nested inside a bool.
        String suffix = "unmapped-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 4);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            String numeric = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"unmapped_int\":{\"gte\":1}}}}"));
            assertEquals("unmapped range must return 0 hits: " + numeric, 0, extractIntPath(numeric, "hits", "total", "value"));

            String stringRange = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"unmapped_str\":{\"gte\":\"aa\"}}}}")
            );
            assertEquals(
                "unmapped string range must return 0 hits: " + stringRange,
                0,
                extractIntPath(stringRange, "hits", "total", "value")
            );

            // The unmapped clause inside a bool must collapses the whole
            // bool to match_none; hits.total must not report the
            // term-clause matches.
            String bool = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"bool\":{\"must\":["
                        + "{\"term\":{\"body\":\"alpha\"}},"
                        + "{\"range\":{\"unmapped_int\":{\"gte\":1}}}"
                        + "]}}}"
                )
            );
            assertEquals("bool must with unmapped clause must return 0 hits: " + bool, 0, extractIntPath(bool, "hits", "total", "value"));

            // A range on a mapped column on the same index still resolves.
            String mapped = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"id\":{\"gte\":1,\"lt\":3}}},\"size\":10}")
            );
            assertEquals("mapped range must return 2 hits: " + mapped, 2, extractIntPath(mapped, "hits", "total", "value"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeCountsFtsHitsWithoutWeightMaterialisation() throws Exception {
        // size:0 on a pure FTS query is counted by a Lance scan that
        // projects no columns; the count must be exact for match and
        // phrase, and a post_filter must route the count through Lucene
        // because it narrows below what Lance would report.
        String suffix = "ftscount-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        // Even ids carry "hello lance N", odd ids "quick brown fox N".
        LanceTableFactory.writeTable(scratchDir, tableName, 20);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            String matchBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match\":{\"body\":\"lance\"}},\"size\":0}"));
            assertEquals("match size:0 total must be 10", 10, extractIntPath(matchBody, "hits", "total", "value"));
            assertEquals("size:0 must return no hits", 0, countOccurrences(matchBody, "\"_id\":"));

            String phraseBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_match_phrase\":{\"field\":\"body\",\"query\":\"hello lance\"}},\"size\":0}"
                )
            );
            assertEquals("phrase size:0 total must be 10", 10, extractIntPath(phraseBody, "hits", "total", "value"));

            // No match: the count path must return 0 from empty batches.
            String zeroBody = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"match\":{\"body\":\"nonexistent\"}},\"size\":0}")
            );
            assertEquals("no-match size:0 total must be 0", 0, extractIntPath(zeroBody, "hits", "total", "value"));

            // post_filter id >= 10 keeps five of the ten "hello lance" rows.
            String postFilterBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"match\":{\"body\":\"lance\"}}," + "\"post_filter\":{\"range\":{\"id\":{\"gte\":10}}}," + "\"size\":0}"
                )
            );
            assertEquals(
                "match + post_filter size:0 must narrow to 5 via Weight fallback",
                5,
                extractIntPath(postFilterBody, "hits", "total", "value")
            );
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAppliesTopKPushdownForFtsHits() throws Exception {
        // A pure FTS query with no sort, aggregation or post_filter
        // pushes size into the Lance scan as limit. The clip must not
        // leak into hits.total, and shapes that need the full match set
        // (sort by a field, aggregations) must not be clipped.
        String suffix = "ftstop-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        // Even ids carry "hello lance N", odd ids "quick brown fox N".
        LanceTableFactory.writeTable(scratchDir, tableName, 20);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            String matchBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match\":{\"body\":\"lance\"}},\"size\":3}"));
            assertEquals("match size:3 total must be 10", 10, extractIntPath(matchBody, "hits", "total", "value"));
            int matchReturnedHits = countOccurrences(matchBody, "\"_id\":");
            assertEquals("match size:3 must return three hits: " + matchBody, 3, matchReturnedHits);

            String allMatchBody = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"match\":{\"body\":\"lance\"}},\"size\":50}")
            );
            assertEquals("match size:50 total must be 10", 10, extractIntPath(allMatchBody, "hits", "total", "value"));
            int allMatchReturnedHits = countOccurrences(allMatchBody, "\"_id\":");
            assertEquals("match size:50 must return ten hits: " + allMatchBody, 10, allMatchReturnedHits);

            String countBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match\":{\"body\":\"lance\"}},\"size\":0}"));
            assertEquals("match size:0 total must be 10", 10, extractIntPath(countBody, "hits", "total", "value"));
            int countReturnedHits = countOccurrences(countBody, "\"_id\":");
            assertEquals("size:0 must return no hits", 0, countReturnedHits);

            // Sort by a field needs every match: id 18 must come first.
            String sortBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"match\":{\"body\":\"lance\"}},\"size\":3,\"sort\":[{\"id\":\"desc\"}]}"
                )
            );
            assertEquals("match sort size:3 total must be 10", 10, extractIntPath(sortBody, "hits", "total", "value"));
            int sortReturnedHits = countOccurrences(sortBody, "\"_id\":");
            assertEquals("match sort size:3 must return three hits: " + sortBody, 3, sortReturnedHits);
            assertTrue("match sort desc must put id=18 first: " + sortBody, sortBody.contains("\"id\":18"));

            // Aggregations need every match: sum of even ids 0..18 is 90.
            String aggBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"match\":{\"body\":\"lance\"}},\"size\":3,\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}"
                )
            );
            assertEquals("match agg size:3 total must be 10", 10, extractIntPath(aggBody, "hits", "total", "value"));
            assertEquals("sum(id) over match must be 90", 90.0d, extractDoublePath(aggBody, "aggregations", "s", "value"), 0.0d);

            String phraseBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_match_phrase\":{\"field\":\"body\",\"query\":\"hello lance\"}},\"size\":4}"
                )
            );
            assertEquals("phrase size:4 total must be 10", 10, extractIntPath(phraseBody, "hits", "total", "value"));
            int phraseReturnedHits = countOccurrences(phraseBody, "\"_id\":");
            assertEquals("phrase size:4 must return four hits: " + phraseBody, 4, phraseReturnedHits);
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAppliesTopKPushdownForScalarFilterHits() throws Exception {
        // A pure scalar filter with no sort, aggregation or post_filter
        // pushes size into the Lance scan as limit. The clip must not
        // leak into hits.total, and shapes that need the full match set
        // must not be clipped.
        String suffix = "topk-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 20);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            String rangeBody = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"id\":{\"gte\":0,\"lt\":20}}},\"size\":5}")
            );
            assertEquals("range size:5 total must be 20", 20, extractIntPath(rangeBody, "hits", "total", "value"));
            int rangeReturnedHits = countOccurrences(rangeBody, "\"_id\":");
            assertEquals("range size:5 must return five hits: " + rangeBody, 5, rangeReturnedHits);

            String termBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"term\":{\"id\":3}},\"size\":5}"));
            assertEquals("term id=3 total must be 1", 1, extractIntPath(termBody, "hits", "total", "value"));
            assertTrue("term id=3 must return the id=3 hit: " + termBody, termBody.contains("\"_id\":\"0-3\""));

            String countBody = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"id\":{\"gte\":0,\"lt\":20}}},\"size\":0}")
            );
            assertEquals("range size:0 total must be 20", 20, extractIntPath(countBody, "hits", "total", "value"));
            int countReturnedHits = countOccurrences(countBody, "\"_id\":");
            assertEquals("size:0 must return no hits", 0, countReturnedHits);

            // Sort by a field needs every match: id 19 must come first.
            String sortBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"range\":{\"id\":{\"gte\":0,\"lt\":20}}},\"size\":5,\"sort\":[{\"id\":\"desc\"}]}"
                )
            );
            assertEquals("sort size:5 total must be 20", 20, extractIntPath(sortBody, "hits", "total", "value"));
            int sortReturnedHits = countOccurrences(sortBody, "\"_id\":");
            assertEquals("sort size:5 must return five hits: " + sortBody, 5, sortReturnedHits);
            assertTrue("sort desc must put id=19 first: " + sortBody, sortBody.contains("\"id\":19"));
            assertTrue("sort desc must put id=15 last: " + sortBody, sortBody.contains("\"id\":15"));

            // Aggregations need every match: sum of 0..19 is 190.
            String aggBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"range\":{\"id\":{\"gte\":0,\"lt\":20}}},\"size\":5,\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}"
                )
            );
            assertEquals("agg size:5 total must be 20", 20, extractIntPath(aggBody, "hits", "total", "value"));
            assertEquals("sum(id) over range must be 190", 190.0d, extractDoublePath(aggBody, "aggregations", "s", "value"), 0.0d);
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testMinScoreRunsOnTheFragmentPath() throws Exception {
        // min_score is applied by the stock MinimumScoreCollector around
        // the executor's hits, count and aggregation collectors, so the
        // documents below the threshold are neither returned, counted
        // nor aggregated. Every shape is compared with the stock search path's
        // answer (the same body with a highlighter, which routes
        // there) and the executed counter proves the fragment path
        // served the plain body.
        String suffix = "min-score-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // Every match_all hit scores 1.0: a threshold above it
            // empties the page and the count, one below keeps both.
            String above = "{\"size\":10,\"query\":{\"match_all\":{}},\"min_score\":2.0}";
            long executedBefore = fragmentRequestsExecuted();
            String aboveBody = readAll(postJson("/" + indexName + "/_search", above));
            assertEquals("the fragment path served the request", executedBefore + 1, fragmentRequestsExecuted());
            assertEquals(0, extractIntPath(aboveBody, "hits", "total", "value"));
            assertEquals("eq", stringPath(aboveBody, "hits", "total", "relation"));
            assertTrue("no hit reaches min_score 2.0: " + aboveBody, fullHitsOf(aboveBody).isEmpty());
            assertSameHitsAsStockSearch(indexName, above);

            String below = "{\"size\":10,\"query\":{\"match_all\":{}},\"min_score\":0.5}";
            String belowBody = readAll(postJson("/" + indexName + "/_search", below));
            assertEquals(6, extractIntPath(belowBody, "hits", "total", "value"));
            assertEquals(6, fullHitsOf(belowBody).size());
            assertSameHitsAsStockSearch(indexName, below);

            // size 0 counts through the same collector.
            String countOnly = "{\"size\":0,\"query\":{\"match_all\":{}},\"min_score\":2.0}";
            assertEquals(0, extractIntPath(readAll(postJson("/" + indexName + "/_search", countOnly)), "hits", "total", "value"));
            assertSameHitsAsStockSearch(indexName, countOnly);

            // A scalar filter planned as a Lance scan scores 1.0 too.
            String term = "{\"size\":10,\"query\":{\"term\":{\"id\":2}},\"min_score\":0.5}";
            String termBody = readAll(postJson("/" + indexName + "/_search", term));
            assertEquals(1, extractIntPath(termBody, "hits", "total", "value"));
            assertEquals(List.of("0-2"), idsOf(hitsOf(termBody)));
            assertSameHitsAsStockSearch(indexName, term);

            // A pushed FTS query: the BM25 scores of the three "hello
            // lance" rows are 0.7361701, so a low threshold keeps all
            // three (counted through the collector, not the Lance scan)
            // and a high one drops them.
            String ftsLow = "{\"size\":10,\"query\":{\"match\":{\"body\":\"lance\"}},\"min_score\":0.01}";
            String ftsLowBody = readAll(postJson("/" + indexName + "/_search", ftsLow));
            assertEquals(3, extractIntPath(ftsLowBody, "hits", "total", "value"));
            assertEquals(List.of("0-0", "0-2", "0-4"), idsOf(hitsOf(ftsLowBody)));
            assertEquals(0.7361701d, extractDoublePath(ftsLowBody, "hits", "max_score"), 1e-6d);
            assertSameHitsAsStockSearch(indexName, ftsLow);

            String ftsHigh = "{\"size\":10,\"query\":{\"match\":{\"body\":\"lance\"}},\"min_score\":100}";
            String ftsHighBody = readAll(postJson("/" + indexName + "/_search", ftsHigh));
            assertEquals(0, extractIntPath(ftsHighBody, "hits", "total", "value"));
            assertTrue(fullHitsOf(ftsHighBody).isEmpty());
            assertSameHitsAsStockSearch(indexName, ftsHigh);

            String ftsCount = "{\"size\":0,\"query\":{\"match\":{\"body\":\"lance\"}},\"min_score\":0.01}";
            assertEquals(3, extractIntPath(readAll(postJson("/" + indexName + "/_search", ftsCount)), "hits", "total", "value"));
            assertSameHitsAsStockSearch(indexName, ftsCount);

            // Aggregations see only the documents above the threshold.
            String agg = "{\"size\":0,\"query\":{\"match_all\":{}},\"min_score\":2.0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}";
            String aggBody = readAll(postJson("/" + indexName + "/_search", agg));
            assertEquals(0, extractIntPath(aggBody, "hits", "total", "value"));
            assertEquals(0.0d, extractDoublePath(aggBody, "aggregations", "s", "value"), 0.0d);
            assertSameHitsAsStockSearch(indexName, agg);
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testTerminateAfterRunsOnTheFragmentPath() throws Exception {
        // terminate_after stops the executor's collection after that
        // many documents and the response says terminated_early, as the
        // stock search path does; without the knob the response carries no
        // terminated_early at all. hits.total is what the collection
        // counted: the documents collected for a page, and for size 0
        // the count Lucene's TotalHitCountCollector answers, which for
        // match_all is the leaf's document count before termination,
        // exactly as on the stock search path.
        String suffix = "terminate-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            String plain = readAll(postJson("/" + indexName + "/_search", "{\"size\":1,\"query\":{\"match_all\":{}}}"));
            assertFalse("no terminate_after, no terminated_early: " + plain, plain.contains("terminated_early"));

            String countOnly = "{\"size\":0,\"query\":{\"match_all\":{}},\"terminate_after\":2}";
            long executedBefore = fragmentRequestsExecuted();
            String countOnlyBody = readAll(postJson("/" + indexName + "/_search", countOnly));
            assertEquals("the fragment path served the request", executedBefore + 1, fragmentRequestsExecuted());
            assertTrue("terminated_early: " + countOnlyBody, countOnlyBody.contains("\"terminated_early\":true"));
            assertEquals(6, extractIntPath(countOnlyBody, "hits", "total", "value"));
            assertSameHitsAsStockSearch(indexName, countOnly);

            String page = "{\"size\":10,\"query\":{\"match_all\":{}},\"terminate_after\":2}";
            String pageBody = readAll(postJson("/" + indexName + "/_search", page));
            assertTrue("terminated_early: " + pageBody, pageBody.contains("\"terminated_early\":true"));
            assertEquals(2, extractIntPath(pageBody, "hits", "total", "value"));
            assertEquals("eq", stringPath(pageBody, "hits", "total", "relation"));
            assertEquals(List.of("0-0", "0-1"), idsOf(hitsOf(pageBody)));
            assertSameHitsAsStockSearch(indexName, page);

            String smallPage = "{\"size\":1,\"query\":{\"match_all\":{}},\"terminate_after\":2}";
            String smallPageBody = readAll(postJson("/" + indexName + "/_search", smallPage));
            assertEquals(2, extractIntPath(smallPageBody, "hits", "total", "value"));
            assertEquals(List.of("0-0"), idsOf(hitsOf(smallPageBody)));
            assertSameHitsAsStockSearch(indexName, smallPage);

            String accurate = "{\"size\":0,\"query\":{\"match_all\":{}},\"terminate_after\":2,\"track_total_hits\":true}";
            String accurateBody = readAll(postJson("/" + indexName + "/_search", accurate));
            assertTrue(accurateBody.contains("\"terminated_early\":true"));
            assertEquals(6, extractIntPath(accurateBody, "hits", "total", "value"));
            assertSameHitsAsStockSearch(indexName, accurate);

            String notReached = "{\"size\":0,\"query\":{\"match_all\":{}},\"terminate_after\":20}";
            String notReachedBody = readAll(postJson("/" + indexName + "/_search", notReached));
            assertTrue("bound above the row count: " + notReachedBody, notReachedBody.contains("\"terminated_early\":false"));
            assertEquals(6, extractIntPath(notReachedBody, "hits", "total", "value"));
            assertSameHitsAsStockSearch(indexName, notReached);

            // A pushed FTS query stops after two of its three hits.
            String fts = "{\"size\":10,\"query\":{\"match\":{\"body\":\"lance\"}},\"terminate_after\":2}";
            String ftsBody = readAll(postJson("/" + indexName + "/_search", fts));
            assertTrue(ftsBody.contains("\"terminated_early\":true"));
            assertEquals(2, extractIntPath(ftsBody, "hits", "total", "value"));
            assertEquals(List.of("0-0", "0-2"), idsOf(hitsOf(ftsBody)));
            assertSameHitsAsStockSearch(indexName, fts);

            // The aggregators see the two documents collected before the
            // bound: ids 0 and 1 sum to 1.
            String agg = "{\"size\":0,\"query\":{\"match_all\":{}},\"terminate_after\":2,\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}";
            String aggBody = readAll(postJson("/" + indexName + "/_search", agg));
            assertTrue(aggBody.contains("\"terminated_early\":true"));
            assertEquals(6, extractIntPath(aggBody, "hits", "total", "value"));
            assertEquals(1.0d, extractDoublePath(aggBody, "aggregations", "s", "value"), 0.0d);
            assertSameHitsAsStockSearch(indexName, agg);
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testRescoreRunsOnTheFragmentPath() throws Exception {
        // The executor collects the largest rescore window through the
        // Lucene collector, runs each rescorer over it and cuts the page;
        // every shape is compared with the stock search path's answer to the
        // same body. Twelve rows over three fragments: body scores are
        // distinct (BM25 grows with id), category is c<id % 3>.
        String suffix = "rescore-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeBucketedInterleavedTable(scratchDir, tableName, 3, 4);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // A scalar first pass (every hit scores 1.0) re scored by a
            // full text query: the rescored window sorts by BM25 and the
            // rows past the window keep their first pass score.
            String scalarThenFts = "{\"size\":5,\"query\":{\"range\":{\"id\":{\"gte\":2}}},"
                + "\"rescore\":{\"window_size\":10,\"query\":{\"rescore_query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"lance\"}}}}}";
            long executedBefore = fragmentRequestsExecuted();
            String scalarThenFtsBody = readAll(postJson("/" + indexName + "/_search", scalarThenFts));
            assertEquals("the fragment path served the request", executedBefore + 1, fragmentRequestsExecuted());
            assertEquals(10, extractIntPath(scalarThenFtsBody, "hits", "total", "value"));
            assertEquals(List.of("2-3", "1-3", "0-3", "2-2", "1-2"), idsOf(hitsOf(scalarThenFtsBody)));
            assertSameHitsAsStockSearch(indexName, scalarThenFts);

            // A full text first pass re scored by a scalar query under
            // explicit weights: rows of category c1 gain the rescore
            // weight, the others keep the weighted first pass score.
            String ftsThenScalar = "{\"size\":6,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"lance\"}},"
                + "\"rescore\":{\"window_size\":12,\"query\":{\"rescore_query\":{\"term\":{\"category\":\"c1\"}},"
                + "\"query_weight\":0.5,\"rescore_query_weight\":2.0}}}";
            String ftsThenScalarBody = readAll(postJson("/" + indexName + "/_search", ftsThenScalar));
            assertEquals(12, extractIntPath(ftsThenScalarBody, "hits", "total", "value"));
            assertSameHitsAsStockSearch(indexName, ftsThenScalar);

            // A window smaller than the page: the three top rows are re
            // scored, the rest of the page is not.
            String smallWindow = "{\"size\":8,\"query\":{\"match_all\":{}},"
                + "\"rescore\":{\"window_size\":3,\"query\":{\"rescore_query\":{\"term\":{\"category\":\"c2\"}}}}}";
            String smallWindowBody = readAll(postJson("/" + indexName + "/_search", smallWindow));
            assertEquals(8, fullHitsOf(smallWindowBody).size());
            assertSameHitsAsStockSearch(indexName, smallWindow);

            // Two rescorers in a row, the second over the first's scores.
            String chained = "{\"size\":5,\"query\":{\"match_all\":{}},\"rescore\":["
                + "{\"window_size\":12,\"query\":{\"rescore_query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"lance\"}}}},"
                + "{\"window_size\":4,\"query\":{\"rescore_query\":{\"term\":{\"category\":\"c0\"}},\"rescore_query_weight\":10}}]}";
            assertSameHitsAsStockSearch(indexName, chained);

            // score_mode max with weights on both sides.
            String maxMode = "{\"size\":5,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"lance\"}},"
                + "\"rescore\":{\"window_size\":12,\"query\":{\"rescore_query\":{\"term\":{\"bucket\":1}},"
                + "\"query_weight\":0.3,\"rescore_query_weight\":1.5,\"score_mode\":\"max\"}}}";
            assertSameHitsAsStockSearch(indexName, maxMode);

            // from skips the leading rescored rows.
            String paged = "{\"from\":2,\"size\":3,\"query\":{\"match_all\":{}},"
                + "\"rescore\":{\"window_size\":12,\"query\":{\"rescore_query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"lance\"}}}}}";
            assertSameHitsAsStockSearch(indexName, paged);

            // The refusals core applies, with its messages.
            ConcurrentResult sorted = postForStatus(
                "/" + indexName + "/_search",
                "{\"size\":2,\"sort\":[{\"id\":\"asc\"}],\"query\":{\"match_all\":{}},"
                    + "\"rescore\":{\"query\":{\"rescore_query\":{\"match_all\":{}}}}}"
            );
            assertEquals(sorted.body(), 400, sorted.status());
            assertTrue(sorted.body(), sorted.body().contains("Cannot use [sort] option in conjunction with [rescore]."));
            ConcurrentResult scrolled = postForStatus(
                "/" + indexName + "/_search?scroll=1m",
                "{\"size\":2,\"query\":{\"match_all\":{}},\"rescore\":{\"query\":{\"rescore_query\":{\"match_all\":{}}}}}"
            );
            assertEquals(scrolled.body(), 400, scrolled.status());
            assertTrue(scrolled.body(), scrolled.body().contains("using [rescore] is not allowed in a scroll context"));
            ConcurrentResult wideWindow = postForStatus(
                "/" + indexName + "/_search",
                "{\"size\":2,\"query\":{\"match_all\":{}},\"rescore\":{\"window_size\":10001,\"query\":{\"rescore_query\":{\"match_all\":{}}}}}"
            );
            assertEquals(wideWindow.body(), 400, wideWindow.status());
            assertTrue(wideWindow.body(), wideWindow.body().contains("Rescore window [10001] is too large."));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testCollapseRunsOnTheFragmentPath() throws Exception {
        // The executor collects one hit per distinct value through the
        // collapsing collector and the coordinator keeps one per value
        // across executors, expanding inner_hits with one search per
        // group; every shape is compared with the stock search path's answer.
        // Twelve rows: category c<id % 3>, bucket id % 4, body scores
        // grow with id.
        String suffix = "collapse-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeBucketedInterleavedTable(scratchDir, tableName, 3, 4);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // Keyword field under match_all: the first row of every
            // category in doc order, hits.total counts every row and the
            // collapse value rides as the field's doc value.
            String keyword = "{\"size\":10,\"query\":{\"match_all\":{}},\"collapse\":{\"field\":\"category\"}}";
            long executedBefore = fragmentRequestsExecuted();
            String keywordBody = readAll(postJson("/" + indexName + "/_search", keyword));
            assertEquals("the fragment path served the request", executedBefore + 1, fragmentRequestsExecuted());
            assertEquals(12, extractIntPath(keywordBody, "hits", "total", "value"));
            assertEquals(List.of("0-0", "1-0", "2-0"), idsOf(hitsOf(keywordBody)));
            assertTrue(
                "the collapse value is a field of the hit: " + keywordBody,
                keywordBody.contains("\"fields\":{\"category\":[\"c0\"]}")
            );
            assertSameHitsAsStockSearch(indexName, keyword);

            // Numeric field with a sort on another field: the highest id
            // of each bucket.
            String numeric = "{\"size\":10,\"sort\":[{\"id\":\"desc\"}],\"collapse\":{\"field\":\"bucket\"}}";
            String numericBody = readAll(postJson("/" + indexName + "/_search", numeric));
            assertEquals(List.of("2-3", "1-3", "0-3", "2-2"), idsOf(hitsOf(numericBody)));
            assertSameHitsAsStockSearch(indexName, numeric);

            // from skips leading groups; a full text first pass picks the
            // best scored row of every category.
            String paged = "{\"from\":1,\"size\":2,\"sort\":[{\"ts\":\"desc\"}],\"collapse\":{\"field\":\"category\"}}";
            assertSameHitsAsStockSearch(indexName, paged);
            String fts =
                "{\"size\":10,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"lance\"}},\"collapse\":{\"field\":\"category\"}}";
            String ftsBody = readAll(postJson("/" + indexName + "/_search", fts));
            assertEquals(List.of("2-3", "1-3", "0-3"), idsOf(hitsOf(ftsBody)));
            assertSameHitsAsStockSearch(indexName, fts);

            // inner_hits: one group search per collapsed hit with the
            // block's size, sort and source filter. The stock search path
            // refuses inner_hits on every Lance field (core wants the
            // collapse field indexed, and the derived mapping says index:
            // false), so the expansion is compared with the group search
            // it issues instead of with the stock search path's answer.
            String innerHits = "{\"size\":10,\"query\":{\"range\":{\"id\":{\"gte\":1}}},\"sort\":[{\"id\":\"asc\"}],"
                + "\"collapse\":{\"field\":\"category\",\"inner_hits\":{\"name\":\"top\",\"size\":2,\"sort\":[{\"id\":\"desc\"}],"
                + "\"_source\":[\"id\"]}}}";
            String innerHitsBody = readAll(postJson("/" + indexName + "/_search", innerHits));
            List<Map<String, Object>> collapsed = hitsOf(innerHitsBody);
            assertEquals(List.of("1-0", "2-0", "0-1"), idsOf(collapsed));
            @SuppressWarnings("unchecked")
            Map<String, Object> firstInner = (Map<String, Object>) ((Map<String, Object>) collapsed.get(0).get("inner_hits")).get("top");
            @SuppressWarnings("unchecked")
            Map<String, Object> firstInnerHits = (Map<String, Object>) firstInner.get("hits");
            assertEquals(4, ((Number) castMap(firstInnerHits.get("total")).get("value")).intValue());
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> firstInnerRows = (List<Map<String, Object>>) firstInnerHits.get("hits");
            assertEquals(List.of("1-3", "1-2"), idsOf(firstInnerRows));
            String groupSearch = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":2,\"sort\":[{\"id\":\"desc\"}],\"_source\":[\"id\"],\"query\":{\"bool\":{"
                        + "\"filter\":[{\"match\":{\"category\":\"c1\"}}],\"must\":[{\"range\":{\"id\":{\"gte\":1}}}]}}}"
                )
            );
            assertEquals(fullHitsOf(groupSearch), firstInnerRows);
            assertEquals(totalOf(groupSearch), castMap(firstInnerHits.get("total")));
            ConcurrentResult shardPathInnerHits = postForStatus("/" + withStockOracle(indexName) + "/_search", innerHits);
            assertEquals(shardPathInnerHits.body(), 400, shardPathInnerHits.status());
            assertTrue(shardPathInnerHits.body(), shardPathInnerHits.body().contains("only indexed field can retrieve `inner_hits`"));

            // max_concurrent_group_searches bounds the expansion; every
            // bucket (id % 4) still expands to its three rows.
            String bounded = "{\"size\":10,\"collapse\":{\"field\":\"bucket\",\"max_concurrent_group_searches\":1,"
                + "\"inner_hits\":{\"name\":\"rows\",\"size\":3}}}";
            List<Map<String, Object>> buckets = hitsOf(readAll(postJson("/" + indexName + "/_search", bounded)));
            assertEquals(4, buckets.size());
            for (Map<String, Object> bucket : buckets) {
                @SuppressWarnings("unchecked")
                Map<String, Object> rows = (Map<String, Object>) ((Map<String, Object>) bucket.get("inner_hits")).get("rows");
                @SuppressWarnings("unchecked")
                Map<String, Object> rowsHits = (Map<String, Object>) rows.get("hits");
                assertEquals(bucket.toString(), 3, ((Number) castMap(rowsHits.get("total")).get("value")).intValue());
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> rowList = (List<Map<String, Object>>) rowsHits.get("hits");
                assertEquals(bucket.toString(), 3, rowList.size());
            }

            // search_after on the collapse field itself.
            String cursor =
                "{\"size\":2,\"sort\":[{\"category\":\"asc\"}],\"search_after\":[\"c0\"],\"collapse\":{\"field\":\"category\"}}";
            String cursorBody = readAll(postJson("/" + indexName + "/_search", cursor));
            assertEquals(List.of("1-0", "2-0"), idsOf(hitsOf(cursorBody)));
            assertSameHitsAsStockSearch(indexName, cursor);

            // The refusals core applies, with its messages.
            ConcurrentResult text = postForStatus("/" + indexName + "/_search", "{\"size\":2,\"collapse\":{\"field\":\"body\"}}");
            assertEquals(text.body(), 400, text.status());
            assertTrue(text.body(), text.body().contains("unknown type for collapse field `body`, only keywords and numbers are accepted"));
            ConcurrentResult unmapped = postForStatus("/" + indexName + "/_search", "{\"size\":2,\"collapse\":{\"field\":\"nope\"}}");
            assertEquals(unmapped.body(), 400, unmapped.status());
            assertTrue(unmapped.body(), unmapped.body().contains("no mapping found for `nope` in order to collapse on"));
            ConcurrentResult withRescore = postForStatus(
                "/" + indexName + "/_search",
                "{\"size\":2,\"collapse\":{\"field\":\"category\"},\"rescore\":{\"query\":{\"rescore_query\":{\"match_all\":{}}}}}"
            );
            assertEquals(withRescore.body(), 400, withRescore.status());
            assertTrue(withRescore.body(), withRescore.body().contains("cannot use `collapse` in conjunction with `rescore`"));
            ConcurrentResult otherSortCursor = postForStatus(
                "/" + indexName + "/_search",
                "{\"size\":2,\"sort\":[{\"id\":\"asc\"}],\"search_after\":[3],\"collapse\":{\"field\":\"category\"}}"
            );
            assertEquals(otherSortCursor.body(), 400, otherSortCursor.status());
            assertTrue(
                otherSortCursor.body(),
                otherSortCursor.body()
                    .contains("collapse field and sort field must be the same when use `collapse` in conjunction with `search_after`")
            );
            ConcurrentResult scrolled = postForStatus(
                "/" + indexName + "/_search?scroll=1m",
                "{\"size\":2,\"collapse\":{\"field\":\"category\"}}"
            );
            assertEquals(scrolled.body(), 400, scrolled.status());
            assertTrue(scrolled.body(), scrolled.body().contains("cannot use `collapse` in a scroll context"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    /**
     * Assert that {@code body} answers the same {@code hits} (total,
     * max_score and every rendered hit key), the same
     * {@code terminated_early} and the same {@code aggregations} on the
     * fragment path as on the stock search action, which the same body
     * against the target of {@link #withStockOracle} runs on.
     */
    private static void assertSameHitsAsStockSearch(String indexName, String body) throws IOException {
        String fragmentBody = readAll(postJson("/" + indexName + "/_search", body));
        String shardBody = readAll(postJson("/" + withStockOracle(indexName) + "/_search", body));
        Map<String, Object> fragmentPath = parseJson(fragmentBody);
        Map<String, Object> shardPath = parseJson(shardBody);
        Map<String, Object> fragmentHits = new java.util.LinkedHashMap<>(hitsBlock(fragmentPath));
        Map<String, Object> shardHits = new java.util.LinkedHashMap<>(hitsBlock(shardPath));
        fragmentHits.put("hits", fullHitsOf(fragmentBody));
        shardHits.put("hits", fullHitsOf(shardBody));
        assertEquals(body, shardHits, fragmentHits);
        assertEquals(body, shardPath.get("terminated_early"), fragmentPath.get("terminated_early"));
        assertEquals(body, aggregationsBlock(shardPath.get("aggregations")), aggregationsBlock(fragmentPath.get("aggregations")));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> hitsBlock(Map<String, Object> response) {
        return (Map<String, Object>) response.get("hits");
    }

    public void testTrackTotalHitsAndCountRunOnFragmentPath() throws Exception {
        // track_total_hits in every form, and therefore _count (which
        // sends track_total_hits: true with size 0), are answered by
        // the fragment path. The match_all count comes from Lance
        // metadata and is exact on the executor; the coordinator
        // applies the bound, so an integer bound below the total
        // yields the capped value with relation gte, `true` and the
        // default yield the exact value, and `false` drops hits.total.
        String suffix = "s3-track-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeMultiFragmentTable(scratchDir, tableName, 12, 4);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            String trackBoundBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"match_all\":{}},\"track_total_hits\":3}")
            );
            assertEquals(
                "track_total_hits:3 caps the value: " + trackBoundBody,
                3,
                extractIntPath(trackBoundBody, "hits", "total", "value")
            );
            assertTrue("track_total_hits:3 should return relation=gte: " + trackBoundBody, trackBoundBody.contains("\"relation\":\"gte\""));

            String trackTrueBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"match_all\":{}},\"track_total_hits\":true}")
            );
            assertEquals(12, extractIntPath(trackTrueBody, "hits", "total", "value"));
            assertTrue("track_total_hits:true is exact: " + trackTrueBody, trackTrueBody.contains("\"relation\":\"eq\""));

            String trackFalseBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"match_all\":{}},\"track_total_hits\":false}")
            );
            assertFalse("track_total_hits:false should omit hits.total: " + trackFalseBody, trackFalseBody.contains("\"total\":{"));

            // A scalar filter is counted on the executor by a Lance
            // scan restricted to its fragments: under an integer
            // bound the scan stops at bound + 1 rows and reports a
            // lower bound, which the coordinator answers as the bound
            // with gte; with track_total_hits: true (and so _count)
            // Lance counts natively and the value is exact. id >= 4
            // matches eight of the twelve rows, all in fragments 1
            // and 2.
            String filterBound = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"range\":{\"id\":{\"gte\":4}}},\"track_total_hits\":5}")
            );
            assertEquals(5, extractIntPath(filterBound, "hits", "total", "value"));
            assertTrue(filterBound.contains("\"relation\":\"gte\""));
            // Bound one below the match count: the limit of eight
            // fills exactly, which still means more than seven.
            String filterEdge = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"range\":{\"id\":{\"gte\":4}}},\"track_total_hits\":7}")
            );
            assertEquals(7, extractIntPath(filterEdge, "hits", "total", "value"));
            assertTrue(filterEdge.contains("\"relation\":\"gte\""));
            // Bound equal to the match count: the scan of nine rows
            // comes back with eight, so the count is exact.
            String filterAtBound = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"range\":{\"id\":{\"gte\":4}}},\"track_total_hits\":8}")
            );
            assertEquals(8, extractIntPath(filterAtBound, "hits", "total", "value"));
            assertTrue(filterAtBound.contains("\"relation\":\"eq\""));
            // Default bound (10,000) with hits: exact because the
            // table has fewer matches than the bound.
            String filterDefault = readAll(postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"term\":{\"id\":5}}}"));
            assertEquals(1, extractIntPath(filterDefault, "hits", "total", "value"));
            assertTrue(filterDefault.contains("\"relation\":\"eq\""));
            assertTrue("hit for id 5 sits at fragment 1 offset 1: " + filterDefault, filterDefault.contains("\"_id\":\"1-1\""));
            String filterExact = readAll(postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"range\":{\"id\":{\"gte\":4}}}}"));
            assertEquals(8, extractIntPath(filterExact, "hits", "total", "value"));
            assertTrue(filterExact.contains("\"relation\":\"eq\""));
            String filterTrue = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"range\":{\"id\":{\"gte\":4}}},\"track_total_hits\":true}")
            );
            assertEquals(8, extractIntPath(filterTrue, "hits", "total", "value"));
            assertTrue(filterTrue.contains("\"relation\":\"eq\""));

            // _count agrees with `_search size 0` for match_all, a
            // scalar filter and an FTS query, and each _count request
            // adds one coordinator fan-out line for this index to the
            // node log (one line per request on a single-node
            // cluster), which is what shows it ran on the fragment
            // path rather than through the shard engine.
            String[] queries = new String[] {
                "{\"match_all\":{}}",
                "{\"range\":{\"id\":{\"gte\":4}}}",
                "{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}" };
            int[] expected = new int[queries.length];
            for (int i = 0; i < queries.length; i++) {
                String search = readAll(postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":" + queries[i] + "}"));
                expected[i] = extractIntPath(search, "hits", "total", "value");
            }
            assertEquals(12, expected[0]);
            assertEquals(8, expected[1]);
            assertEquals(6, expected[2]);
            long fanOutsBefore = fanOutLogLines(indexName);
            for (int i = 0; i < queries.length; i++) {
                String count = readAll(postJson("/" + indexName + "/_count", "{\"query\":" + queries[i] + "}"));
                assertEquals(
                    "_count must agree with hits.total.value for " + queries[i] + ": " + count,
                    expected[i],
                    extractIntPath(count, "count")
                );
            }
            assertEquals(12, extractIntPath(readAll(client().performRequest(new Request("GET", "/" + indexName + "/_count"))), "count"));
            assertBusy(() -> {
                long fanOuts = fanOutLogLines(indexName);
                assertEquals("every _count request must fan out on the fragment path", fanOutsBefore + queries.length + 1, fanOuts);
            });
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    /**
     * Number of coordinator fan-out log lines for {@code indexName}
     * across the node logs of the test cluster. The testclusters plugin
     * writes them under {@code build/testclusters/<task>-<n>/logs}, the
     * sibling of the shared tables directory the build passes in. Only
     * the log4j file ({@code <task>.log}) is read; the plugin also keeps
     * the process's captured stdout ({@code opensearch.stdout.log}),
     * which repeats every line.
     */
    private static long fanOutLogLines(String indexName) throws IOException {
        Path clustersDir = sharedRoot().resolveSibling("testclusters");
        assertTrue(
            "testclusters directory not found at " + clustersDir + " (expected next to tests.lance.shared_tables_dir)",
            Files.isDirectory(clustersDir)
        );
        String marker = "lance.dispatch: fan-out index [" + indexName + "]";
        long count = 0;
        int nodeLogs = 0;
        try (Stream<Path> files = Files.walk(clustersDir)) {
            for (Path file : files.filter(Files::isRegularFile).toList()) {
                String name = file.getFileName().toString();
                if (!name.endsWith(".log") || name.startsWith("opensearch.") || !file.getParent().getFileName().toString().equals("logs")) {
                    continue;
                }
                nodeLogs++;
                for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                    if (line.contains(marker)) {
                        count++;
                    }
                }
            }
        }
        assertTrue("no node log (<task>-<n>/logs/<task>.log) found under " + clustersDir, nodeLogs > 0);
        return count;
    }

    public void testFragmentDispatchModeStampsIndexAndVersionEnvelope() throws Exception {
        // Every hit carries _index; _version and _seq_no /
        // _primary_term appear only when the request opts in. The
        // fragment path has no per-doc versions, so the constants match
        // what the stock search path reports for a freshly indexed doc.
        String suffix = "s3-env-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 4);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            String plain = readAll(postJson("/" + indexName + "/_search", "{\"size\":2,\"query\":{\"match_all\":{}}}"));
            assertTrue("expected _index=[" + indexName + "] on each hit: " + plain, plain.contains("\"_index\":\"" + indexName + "\""));
            assertFalse("_version must be omitted by default: " + plain, plain.contains("\"_version\""));
            assertFalse("_seq_no must be omitted by default: " + plain, plain.contains("\"_seq_no\""));
            assertFalse("_primary_term must be omitted by default: " + plain, plain.contains("\"_primary_term\""));

            String versioned = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":1,\"query\":{\"match_all\":{}},\"version\":true}")
            );
            assertTrue("expected _index on each hit: " + versioned, versioned.contains("\"_index\":\"" + indexName + "\""));
            assertTrue("expected _version=1 when version:true: " + versioned, versioned.contains("\"_version\":1"));
            assertFalse("_seq_no still off: " + versioned, versioned.contains("\"_seq_no\""));

            String seqno = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":1,\"query\":{\"match_all\":{}},\"seq_no_primary_term\":true}")
            );
            assertTrue("expected _seq_no=0: " + seqno, seqno.contains("\"_seq_no\":0"));
            assertTrue("expected _primary_term=1: " + seqno, seqno.contains("\"_primary_term\":1"));
            assertFalse("_version still off: " + seqno, seqno.contains("\"_version\""));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    /**
     * A search whose target expands to several indexes routes by the
     * backing of every target. Two Lance backed indexes (one table of six
     * rows, one of twelve rows in three fragments) fan out once per index
     * and the coordinator merges the pages, sums the totals and reduces
     * the two indexes' aggregation trees together; the answer is the
     * stock search path's for hits, totals, metrics, buckets and a pipeline over
     * the buckets. A Lance backed index next to an ordinary Lucene index,
     * named directly or through an alias, keeps the stock search path (no
     * fragment request runs) and the stock search path answers hits and metrics
     * over both.
     */
    @SuppressWarnings("unchecked")
    public void testCrossIndexSearchRoutesByTheTargetsBacking() throws Exception {
        String suffix = "cross-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String small = "small-" + suffix;
        String large = "large-" + suffix;
        String lucene = "lucene-" + suffix;
        String alias = "alias-" + suffix;
        LanceTableFactory.writeTable(scratchDir, small, 6);
        LanceTableFactory.writeMultiFragmentTable(scratchDir, large, 12, 4);
        try {
            for (String table : List.of(small, large)) {
                Response attach = postJson("/_lance/attach", "{\"table\":\"" + scratchDir.resolve(table + ".lance") + "\"}");
                assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            }
            Request create = new Request("PUT", "/" + lucene);
            create.setJsonEntity(
                "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0},"
                    + "\"mappings\":{\"properties\":{\"id\":{\"type\":\"long\"},\"body\":{\"type\":\"text\"}}},"
                    + "\"aliases\":{\""
                    + alias
                    + "\":{}}}"
            );
            client().performRequest(create);
            for (int id = 100; id < 102; id++) {
                Request doc = new Request("PUT", "/" + lucene + "/_doc/" + id + "?refresh=true");
                doc.setJsonEntity("{\"id\":" + id + ",\"body\":\"lucene row " + id + "\"}");
                client().performRequest(doc);
            }

            // Two Lance backed indexes: one fragment request per index
            // (one data node), the merged page, the summed total, and
            // the aggregations reduced across the two tables' trees.
            String hitsAndMetrics = "{\"size\":30,\"sort\":[{\"id\":\"asc\"}],\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}},"
                + "\"st\":{\"stats\":{\"field\":\"id\"}},\"t\":{\"terms\":{\"field\":\"id\",\"size\":20,\"order\":{\"_key\":\"asc\"}}}}}";
            long executedBefore = fragmentRequestsExecuted();
            String twoLance = readAll(postJson("/" + small + "," + large + "/_search", hitsAndMetrics));
            assertEquals("one fragment request per Lance backed index", executedBefore + 2, fragmentRequestsExecuted());
            assertEquals(18, extractIntPath(twoLance, "hits", "total", "value"));
            assertEquals(18, fullHitsOf(twoLance).size());
            // ids 0..5 twice and 6..11 once: 15 + 66.
            assertEquals(81.0d, extractDoublePath(twoLance, "aggregations", "s", "value"), 0d);
            assertEquals(18, extractIntPath(twoLance, "aggregations", "st", "count"));
            List<Map<String, Object>> buckets = (List<Map<String, Object>>) castMap(
                castMap(parseJson(twoLance).get("aggregations")).get("t")
            ).get("buckets");
            assertEquals(12, buckets.size());
            assertEquals(2, ((Number) buckets.get(0).get("doc_count")).intValue());
            assertEquals(1, ((Number) buckets.get(11).get("doc_count")).intValue());
            assertSameHitsAsStockSearchInAnyOrder(small + "," + large, hitsAndMetrics);

            String pipeline = "{\"size\":0,\"aggs\":{\"h\":{\"histogram\":{\"field\":\"id\",\"interval\":4},"
                + "\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}},\"cs\":{\"cumulative_sum\":{\"buckets_path\":\"s\"}}}},"
                + "\"ab\":{\"avg_bucket\":{\"buckets_path\":\"h>s\"}}}}";
            assertSameHitsAsStockSearchInAnyOrder(small + "," + large, pipeline);
            String filtered = "{\"size\":5,\"query\":{\"range\":{\"id\":{\"gte\":3}}},\"sort\":[{\"id\":\"desc\"}],"
                + "\"aggs\":{\"m\":{\"max\":{\"field\":\"id\"}}}}";
            assertSameHitsAsStockSearchInAnyOrder(small + "," + large, filtered);
            assertEquals("every request above fanned out once per index", executedBefore + 2 + 3 * 2, fragmentRequestsExecuted());

            // A Lance backed index next to a Lucene index, directly and
            // through the alias: the stock search path answers over both and no
            // fragment request runs.
            for (String target : List.of(small + "," + lucene, small + "," + alias)) {
                String mixed = readAll(postJson("/" + target + "/_search", hitsAndMetrics));
                assertEquals("the stock search path served " + target, executedBefore + 8, fragmentRequestsExecuted());
                assertEquals(target, 8, extractIntPath(mixed, "hits", "total", "value"));
                assertEquals(target, 2, extractIntPath(mixed, "_shards", "total"));
                List<Map<String, Object>> hits = fullHitsOf(mixed);
                assertEquals(target, 8, hits.size());
                assertEquals(target, small, hits.get(0).get("_index"));
                assertEquals(target, lucene, hits.get(7).get("_index"));
                // 15 from the table, 201 from the two Lucene documents.
                assertEquals(target, 216.0d, extractDoublePath(mixed, "aggregations", "s", "value"), 0d);
                assertEquals(target, 8, extractIntPath(mixed, "aggregations", "st", "count"));
            }
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + small + "," + large + "," + lucene));
            } catch (Exception ignored) {}
        }
    }

    /**
     * As {@link #assertSameHitsAsStockSearch}, for a target of several
     * indexes: the hits are compared as a set keyed on {@code _index} and
     * {@code _id}, since the two paths order equal sort values of
     * different indexes differently (the stock search action by shard
     * iteration order, the fragment path by the request's target order).
     */
    private static void assertSameHitsAsStockSearchInAnyOrder(String target, String body) throws IOException {
        String fragmentBody = readAll(postJson("/" + target + "/_search", body));
        String shardBody = readAll(postJson("/" + withStockOracle(target) + "/_search", body));
        Map<String, Object> fragmentPath = parseJson(fragmentBody);
        Map<String, Object> shardPath = parseJson(shardBody);
        assertEquals(body, hitsBlock(shardPath).get("total"), hitsBlock(fragmentPath).get("total"));
        assertEquals(body, hitsBlock(shardPath).get("max_score"), hitsBlock(fragmentPath).get("max_score"));
        assertEquals(body, keyedHits(shardBody), keyedHits(fragmentBody));
        assertEquals(body, aggregationsBlock(shardPath.get("aggregations")), aggregationsBlock(fragmentPath.get("aggregations")));
    }

    private static Map<String, Map<String, Object>> keyedHits(String searchBody) {
        Map<String, Map<String, Object>> keyed = new java.util.TreeMap<>();
        for (Map<String, Object> hit : fullHitsOf(searchBody)) {
            keyed.put(hit.get("_index") + "/" + hit.get("_id"), hit);
        }
        return keyed;
    }

    public void testFragmentDispatchModeAnswersFromPagination() throws Exception {
        // The coordinator asks each node for from + size hits and drops
        // the leading from after the merge.
        String suffix = "s3-from-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // id desc is [5, 4, 3, 2, 1, 0]; from=2, size=2 keeps [3, 2].
            String body = readAll(
                postJson("/" + indexName + "/_search", "{\"from\":2,\"size\":2,\"query\":{\"match_all\":{}},\"sort\":[{\"id\":\"desc\"}]}")
            );
            assertEquals(6, extractIntPath(body, "hits", "total", "value"));
            assertTrue("expected sort value [3] on the first paged hit: " + body, body.contains("\"sort\":[3]"));
            assertTrue("expected sort value [2] on the second paged hit: " + body, body.contains("\"sort\":[2]"));
            assertFalse("hit sort value [5] must have been skipped by from=2: " + body, body.contains("\"sort\":[5]"));
            assertFalse("hit sort value [4] must have been skipped by from=2: " + body, body.contains("\"sort\":[4]"));
            assertFalse("only two hits should remain after size=2: " + body, body.contains("\"sort\":[1]"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAnswersPostFilter() throws Exception {
        // post_filter narrows hits and hits.total but not aggregations.
        String suffix = "s3-pf-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            String body = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":10,\"query\":{\"match_all\":{}},"
                        + "\"post_filter\":{\"range\":{\"id\":{\"gte\":4}}},"
                        + "\"aggs\":{\"total\":{\"value_count\":{\"field\":\"id\"}}}}"
                )
            );
            assertEquals(2, extractIntPath(body, "hits", "total", "value"));
            assertEquals(6, extractIntPath(body, "aggregations", "total", "value"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAnswersSearchAfter() throws Exception {
        String suffix = "s3-sa-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // id desc is [5, 4, 3, 2, 1, 0].
            String first = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":2,\"query\":{\"match_all\":{}},\"sort\":[{\"id\":\"desc\"}]}")
            );
            assertTrue("expected first page to include sort value [5]: " + first, first.contains("\"sort\":[5]"));
            assertTrue("expected first page to include sort value [4]: " + first, first.contains("\"sort\":[4]"));

            String second = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":2,\"query\":{\"match_all\":{}},\"sort\":[{\"id\":\"desc\"}],\"search_after\":[4]}"
                )
            );
            assertTrue("expected second page to include sort value [3]: " + second, second.contains("\"sort\":[3]"));
            assertTrue("expected second page to include sort value [2]: " + second, second.contains("\"sort\":[2]"));
            assertFalse("search_after cursor value [4] must be excluded: " + second, second.contains("\"sort\":[4]"));
            assertFalse("hit above the cursor must be excluded: " + second, second.contains("\"sort\":[5]"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAnswersScriptQueryAndScriptSort() throws Exception {
        // Script queries and script sorts read doc values through the
        // standard DocValues API, so they work on the fragment path
        // without dedicated plumbing.
        String suffix = "s3-scq-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            String body = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":10,\"query\":{\"script\":{\"script\":{\"source\":\"doc['id'].value > 2\"}}}}"
                )
            );
            assertEquals(3, extractIntPath(body, "hits", "total", "value"));

            String sortBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":3,\"query\":{\"match_all\":{}},"
                        + "\"sort\":[{\"_script\":{\"script\":{\"source\":\"doc['id'].value\"},\"type\":\"number\",\"order\":\"desc\"}}]}"
                )
            );
            assertEquals(6, extractIntPath(sortBody, "hits", "total", "value"));
            assertTrue("script sort first hit should carry sort value 5: " + sortBody, sortBody.contains("\"sort\":[5"));
            assertTrue("script sort second hit should carry sort value 4: " + sortBody, sortBody.contains("\"sort\":[4"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testDefaultSearchReturnsAtLeastTenHits() throws Exception {
        // With ten or more adjacent doc ids and no deletions, FetchPhase
        // asks the leaf for a sequential stored-fields reader; the Lance
        // leaf must provide one.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "defaultsearch")) {
            String indexName = fixture.indexName();

            Response search = client().performRequest(new Request("GET", "/" + indexName + "/_search"));
            String body = readAll(search);
            int totalHits = extractIntPath(body, "hits", "total", "value");
            assertEquals("expected 16 total hits, saw response: " + body, 16, totalHits);
            assertTrue("expected hits[0]._source in response, saw: " + body, body.contains("\"_source\""));
        }
    }

    public void testCoordinatorPoolOverloadReturns429AndLeavesNoTaskBehind() throws Exception {
        // The test cluster runs the lance_coordinator pool with one
        // thread and a queue of one (build.gradle), so a burst of
        // concurrent requests makes the pool refuse some of them. A
        // refused request must come back as 429 naming the pool, the
        // served ones must be complete and correct, and afterwards no
        // coordinator or search task may remain: the request whose
        // work was refused has to be failed, not dropped.
        try (LanceTestCluster fixture = LanceTestCluster.setUpMultiFragment(120, 20, "overload")) {
            String indexName = fixture.indexName();
            String body = "{\"size\":5,\"sort\":[{\"id\":\"desc\"}],\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}";
            int served = 0;
            int rejected = 0;
            try (RestClient wide = concurrentClient(getClusterHosts(), 32)) {
                // Whether a burst hits the bound depends on how the 32
                // arrivals interleave with the pool's one thread, so
                // bursts are repeated until one request was refused.
                for (int round = 0; round < 20 && rejected == 0; round++) {
                    for (ConcurrentResult result : postConcurrently(wide, "/" + indexName + "/_search", body, 32)) {
                        if (result.status() == RestStatus.OK.getStatus()) {
                            served++;
                            assertEquals(
                                "served request must be complete: " + result.body(),
                                120,
                                extractIntPath(result.body(), "hits", "total", "value")
                            );
                            assertEquals(7140.0d, extractDoublePath(result.body(), "aggregations", "s", "value"), 0.0d);
                            assertEquals(List.of("5-19", "5-18", "5-17", "5-16", "5-15"), idsOf(hitsOf(result.body())));
                        } else if (result.status() == RestStatus.TOO_MANY_REQUESTS.getStatus()) {
                            rejected++;
                            assertTrue(
                                "429 body must carry the pool's rejection: " + result.body(),
                                result.body().contains("rejected execution")
                            );
                            assertTrue("429 body must name the pool: " + result.body(), result.body().contains("lance_coordinator"));
                        } else {
                            fail("unexpected status " + result.status() + " from the fragment path: " + result.body());
                        }
                    }
                }
            }
            assertTrue("expected the one-thread, one-slot pool to refuse a request in 20 bursts of 32", rejected > 0);
            assertTrue("expected the pool to serve requests as well", served > 0);

            // Every request has answered, so no task of the coordinator
            // action or of the search it was started for may remain.
            assertBusy(() -> {
                String tasks = readAll(
                    client().performRequest(new Request("GET", "/_tasks?actions=*lance/coordinator*,indices:data/read/search*"))
                );
                assertEquals("tasks left behind: " + tasks, 0, countOccurrences(tasks, "\"action\""));
            });

            String pool = readAll(
                client().performRequest(new Request("GET", "/_cat/thread_pool/lance_coordinator?format=json&h=name,rejected"))
            );
            assertTrue("thread pool stats must count the rejections: " + pool, sumCatColumn(pool, "rejected") > 0);
        }
    }

    public void testTimeoutAnswersPartialResultsWithTimedOutAndCancelsTheExecutor() throws Exception {
        // The request's timeout is the transport timeout of every per-node
        // request. A script query that spins per document keeps the one
        // executor of this single-node cluster busy well past 100 ms, so
        // the coordinator gets no answer in time, cancels the executor
        // task, and answers from zero nodes: timed_out true, no hits, and
        // hits.total as a lower bound. The executor sees the cancellation
        // between documents and ends, so no fragment query task remains.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(400, "timeout")) {
            String indexName = fixture.indexName();
            String body = readAll(
                postJson("/" + indexName + "/_search", "{\"timeout\":\"100ms\",\"size\":5,\"query\":" + slowScriptQuery(900_000) + "}")
            );
            Map<String, Object> response = parseJson(body);
            assertEquals("the response must say it timed out: " + body, Boolean.TRUE, response.get("timed_out"));
            assertEquals("no node answered, so no hits: " + body, 0, hitsOf(body).size());
            assertEquals("no node answered, so the count is zero: " + body, 0, extractIntPath(body, "hits", "total", "value"));
            assertEquals("a partial count is a lower bound: " + body, "gte", stringPath(body, "hits", "total", "relation"));
            assertEquals("the fan-out is still one logical unit: " + body, 1, extractIntPath(body, "_shards", "successful"));

            assertBusy(() -> {
                List<Map<String, Object>> left = tasksOf(client(), "*lance/fragment_query*,*lance/coordinator*,indices:data/read/search*");
                assertEquals("the timed out executor must have been cancelled, tasks left: " + left, 0, left.size());
            });

            // The same query without a timeout answers in full.
            String complete = readAll(postJson("/" + indexName + "/_search", "{\"size\":5,\"query\":" + slowScriptQuery(1_000) + "}"));
            assertEquals(Boolean.FALSE, parseJson(complete).get("timed_out"));
            assertEquals(400, extractIntPath(complete, "hits", "total", "value"));
            assertEquals("eq", stringPath(complete, "hits", "total", "relation"));
        }
    }

    public void testTimeoutWithoutPartialResultsFailsWithGatewayTimeout() throws Exception {
        // allow_partial_search_results=false turns a node that did not
        // answer in time into a failure of the whole request: HTTP 504
        // with the transport timeout as the cause. The low level REST
        // client retries a 504 once on the same host (its only one), so
        // the cluster sees two timed out requests; both executors must
        // be cancelled.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(400, "timeout-strict")) {
            String indexName = fixture.indexName();
            Request request = new Request("POST", "/" + indexName + "/_search?allow_partial_search_results=false");
            request.setJsonEntity("{\"timeout\":\"100ms\",\"size\":5,\"query\":" + slowScriptQuery(900_000) + "}");
            ResponseException failure = expectThrows(ResponseException.class, () -> client().performRequest(request));
            String body = readAll(failure.getResponse());
            assertEquals(
                "expected 504, got: " + body,
                RestStatus.GATEWAY_TIMEOUT.getStatus(),
                failure.getResponse().getStatusLine().getStatusCode()
            );
            assertEquals("timeout_exception", stringPath(body, "error", "type"));
            assertTrue("the body must name the node that did not answer: " + body, body.contains("did not complete in time"));
            assertTrue("the transport timeout must be the cause: " + body, body.contains("receive_timeout_transport_exception"));

            assertBusy(() -> {
                List<Map<String, Object>> left = tasksOf(client(), "*lance/fragment_query*,*lance/coordinator*,indices:data/read/search*");
                assertEquals("the timed out executor must have been cancelled, tasks left: " + left, 0, left.size());
            });
        }
    }

    public void testCancellingTheSearchTaskCancelsTheCoordinatorAndItsExecutor() throws Exception {
        // The coordinator task is a child of the search task and the
        // executor task a child of the coordinator task, so
        // _tasks/_cancel on the search task (what a client that closes
        // its connection also triggers) reaches the executor: the
        // request ends with task_cancelled_exception and no task of the
        // three actions remains.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(400, "cancel")) {
            String indexName = fixture.indexName();
            CompletableFuture<ConcurrentResult> pending = postAsync(
                client(),
                "/" + indexName + "/_search",
                "{\"size\":5,\"query\":" + slowScriptQuery(900_000) + "}"
            );
            awaitFragmentQueryRunning(client(), 1);

            String cancelled = readAll(postJson("/_tasks/_cancel?actions=indices:data/read/search", ""));
            assertTrue("the cancel must name the search task: " + cancelled, cancelled.contains("indices:data/read/search"));

            ConcurrentResult result = pending.get(60, TimeUnit.SECONDS);
            assertTrue("a cancelled request must fail, got " + result.status() + ": " + result.body(), result.status() >= 500);
            assertEquals("task_cancelled_exception", stringPath(result.body(), "error", "type"));

            assertBusy(() -> {
                List<Map<String, Object>> left = tasksOf(client(), "*lance/fragment_query*,*lance/coordinator*,indices:data/read/search*");
                assertEquals("tasks left behind after the cancel: " + left, 0, left.size());
            });
        }
    }

    public void testCancellingTheCoordinatorTaskDirectlyCancelsItsExecutor() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(400, "cancel-coordinator")) {
            String indexName = fixture.indexName();
            CompletableFuture<ConcurrentResult> pending = postAsync(
                client(),
                "/" + indexName + "/_search",
                "{\"size\":5,\"query\":" + slowScriptQuery(900_000) + "}"
            );
            awaitFragmentQueryRunning(client(), 1);

            postJson("/_tasks/_cancel?actions=*lance/coordinator*", "");

            ConcurrentResult result = pending.get(60, TimeUnit.SECONDS);
            assertTrue("a cancelled request must fail, got " + result.status() + ": " + result.body(), result.status() >= 500);
            assertEquals("task_cancelled_exception", stringPath(result.body(), "error", "type"));
            assertBusy(() -> {
                List<Map<String, Object>> left = tasksOf(client(), "*lance/fragment_query*,*lance/coordinator*,indices:data/read/search*");
                assertEquals("tasks left behind after the cancel: " + left, 0, left.size());
            });
        }
    }

    public void testTableAboveTheLuceneBoundIsServedInFragmentGroups() throws Exception {
        // A table with more rows than one Lucene reader may hold cannot
        // be tested at its real size, so the bound is lowered to one
        // fragment of the fixture: 120 rows in 6 fragments of 20 under a
        // bound of 20. The index must come up green with a shard reader
        // over the first fragment, every search shape must answer from
        // all 120 rows through six fragment requests, GET must reach
        // every row, a body no plan answers must be refused, and attach
        // and _lance/stats must say what happened.
        updateClusterSetting("lance.test.max_docs_per_reader", "20");
        String suffix = "bound-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeMultiFragmentTable(scratchDir, tableName, 120, 20);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String pkTable = "pk-" + suffix;
        LanceTableFactory.writeStringPkTable(scratchDir, pkTable, 12, 4);
        String pkTableUri = scratchDir.resolve(pkTable + ".lance").toString();
        try {
            String attach = readAll(postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}"));
            assertEquals("attach must succeed for a table above the bound: " + attach, 120, extractIntPath(attach, "rows"));
            assertTrue("attach must flag the bound: " + attach, attach.contains("\"lucene_bound_exceeded\":true"));
            ensureGreen(tableName);
            String pkAttach = readAll(postJson("/_lance/attach", "{\"table\":\"" + pkTableUri + "\"}"));
            assertFalse("a table of 12 rows in 3 fragments of 4 is under the bound of 20: " + pkAttach, pkAttach.contains("lucene_bound"));
            ensureGreen(pkTable);

            // The shard reader holds the first fragment only; _stats
            // counts it, _lance/stats reports both figures.
            String docStats = readAll(client().performRequest(new Request("GET", "/" + tableName + "/_stats/docs")));
            assertEquals(20, extractIntPath(docStats, "indices", tableName, "primaries", "docs", "count"));
            String lanceStats = readAll(client().performRequest(new Request("GET", "/_lance/stats")));
            Map<String, Object> nodes = castMap(parseJson(lanceStats).get("nodes"));
            Map<String, Object> indices = castMap(castMap(nodes.values().iterator().next()).get("indices"));
            Map<String, Object> indexStats = castMap(indices.get(tableName));
            assertEquals(lanceStats, 120, ((Number) indexStats.get("rows")).intValue());
            assertEquals(lanceStats, 20, ((Number) indexStats.get("shard_reader_rows")).intValue());
            assertEquals(lanceStats, true, indexStats.get("lucene_bound_exceeded"));
            Map<String, Object> pkStats = castMap(indices.get(pkTable));
            assertEquals(lanceStats, 12, ((Number) pkStats.get("rows")).intValue());
            assertEquals(lanceStats, 12, ((Number) pkStats.get("shard_reader_rows")).intValue());
            assertEquals(lanceStats, false, pkStats.get("lucene_bound_exceeded"));

            // Every fragment path shape over all 120 rows.
            String matchAll = "{\"query\":{\"match_all\":{}},\"size\":10}";
            String sortDesc = "{\"size\":5,\"sort\":[{\"id\":\"desc\"}]}";
            String sum = "{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}";
            String terms = "{\"size\":0,\"aggs\":{\"t\":{\"terms\":{\"field\":\"id\",\"size\":3,\"order\":{\"_key\":\"desc\"}}}}}";
            String range = "{\"size\":0,\"track_total_hits\":true,\"query\":{\"range\":{\"id\":{\"gte\":30,\"lt\":100}}}}";
            String knn = "{\"size\":3,\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":[57.4,0,0,0,0,0,0,0],\"k\":3}}}";
            String matchAllBody = readAll(postJson("/" + tableName + "/_search", matchAll));
            assertEquals(matchAllBody, 120, extractIntPath(matchAllBody, "hits", "total", "value"));
            assertEquals(matchAllBody, 10, countOccurrences(matchAllBody, "\"_id\":"));
            String sortBody = readAll(postJson("/" + tableName + "/_search", sortDesc));
            assertEquals(List.of("5-19", "5-18", "5-17", "5-16", "5-15"), idsOf(hitsOf(sortBody)));
            String sumBody = readAll(postJson("/" + tableName + "/_search", sum));
            assertEquals(7140.0d, extractDoublePath(sumBody, "aggregations", "s", "value"), 0.0d);
            assertEquals(List.of("119=1", "118=1", "117=1"), bucketsOf(readAll(postJson("/" + tableName + "/_search", terms)), "t"));
            String rangeBody = readAll(postJson("/" + tableName + "/_search", range));
            assertEquals(rangeBody, 70, extractIntPath(rangeBody, "hits", "total", "value"));
            assertEquals("eq", stringPath(rangeBody, "hits", "total", "relation"));
            String knnBody = readAll(postJson("/" + tableName + "/_search", knn));
            assertEquals(List.of("2-17", "2-18", "2-16"), idsOf(hitsOf(knnBody)));
            String count = readAll(client().performRequest(new Request("GET", "/" + tableName + "/_count")));
            assertEquals(120, extractIntPath(count, "count"));

            // GET resolves the key through the Lance scan filter, so a
            // row in a fragment the shard reader does not hold is found.
            // The bound is lowered to one fragment of the key table too.
            updateClusterSetting("lance.test.max_docs_per_reader", "4");
            client().performRequest(new Request("POST", "/" + pkTable + "/_close"));
            client().performRequest(new Request("POST", "/" + pkTable + "/_open"));
            ensureGreen(pkTable);
            String pkDocStats = readAll(client().performRequest(new Request("GET", "/" + pkTable + "/_stats/docs")));
            assertEquals(4, extractIntPath(pkDocStats, "indices", pkTable, "primaries", "docs", "count"));
            for (String key : List.of("alpha-0", "alpha-5", "alpha-11")) {
                Response hit = client().performRequest(new Request("GET", "/" + pkTable + "/_doc/" + key));
                assertEquals(200, hit.getStatusLine().getStatusCode());
                assertTrue(readAll(hit).contains("\"_id\":\"" + key + "\""));
            }
            String pkCount = readAll(client().performRequest(new Request("GET", "/" + pkTable + "/_count")));
            assertEquals(12, extractIntPath(pkCount, "count"));

            // A body no plan answers (a highlighter) is refused with 400
            // naming the element, whatever the table's size: no search
            // over a Lance backed target alone reads the shard reader.
            ResponseException refused = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/" + pkTable + "/_search",
                    "{\"size\":1,\"query\":{\"match_all\":{}},\"highlight\":{\"fields\":{\"label\":{}}}}"
                )
            );
            assertEquals(400, refused.getResponse().getStatusLine().getStatusCode());
            String refusedBody = readAll(refused.getResponse());
            assertEquals("illegal_argument_exception", stringPath(refusedBody, "error", "type"));
            assertEquals(
                "search body carries a `highlight` clause which needs full-text APIs Lance does not surface.",
                stringPath(refusedBody, "error", "reason")
            );

            // With the bound back at its default the same requests run as
            // one fragment request and answer the same.
            updateClusterSetting("lance.test.max_docs_per_reader", null);
            String oneGroup = readAll(postJson("/" + tableName + "/_search", matchAll));
            assertEquals(120, extractIntPath(oneGroup, "hits", "total", "value"));
            assertEquals(idsOf(hitsOf(matchAllBody)), idsOf(hitsOf(oneGroup)));
            assertEquals(
                List.of("5-19", "5-18", "5-17", "5-16", "5-15"),
                idsOf(hitsOf(readAll(postJson("/" + tableName + "/_search", sortDesc))))
            );
            assertEquals(
                7140.0d,
                extractDoublePath(readAll(postJson("/" + tableName + "/_search", sum)), "aggregations", "s", "value"),
                0.0d
            );
            assertEquals(List.of("119=1", "118=1", "117=1"), bucketsOf(readAll(postJson("/" + tableName + "/_search", terms)), "t"));
            assertEquals(List.of("2-17", "2-18", "2-16"), idsOf(hitsOf(readAll(postJson("/" + tableName + "/_search", knn)))));
            // The highlighter is refused under the default bound as well:
            // the refusal is about the element, not the table.
            long executed = fragmentRequestsExecuted();
            ResponseException stillRefused = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/" + pkTable + "/_search",
                    "{\"size\":1,\"query\":{\"match_all\":{}},\"highlight\":{\"fields\":{\"label\":{}}}}"
                )
            );
            assertEquals(400, stillRefused.getResponse().getStatusLine().getStatusCode());
            assertEquals("the refusal ran no fragment request", executed, fragmentRequestsExecuted());
        } finally {
            updateClusterSetting("lance.test.max_docs_per_reader", null);
            for (String index : List.of(tableName, pkTable)) {
                try {
                    client().performRequest(new Request("DELETE", "/" + index));
                } catch (Exception ignored) {}
            }
            deleteRecursively(scratchDir);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castMap(Object value) {
        return (Map<String, Object>) value;
    }

    private static void updateClusterSetting(String key, String value) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        String encoded = value == null ? "null" : "\"" + value + "\"";
        request.setJsonEntity("{\"transient\":{\"" + key + "\":" + encoded + "}}");
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
    }
}
