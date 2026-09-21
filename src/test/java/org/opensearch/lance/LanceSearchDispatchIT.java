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
 * search_after, scripts, response envelope) and the shapes that fall through
 * to the shard path.
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
        // DSL that works on a shard-path date field works here.
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

    public void testStoredFieldsDocValueFieldsExplainFallThroughToShardPath() throws Exception {
        // stored_fields, docvalue_fields and explain are not implemented
        // by the fragment dispatch path; the dispatch filter sends those
        // requests to the shard path, whose fetch phase handles them.
        String suffix = "s3-storedfields-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 4);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            String noneBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":1,\"query\":{\"match_all\":{}},\"stored_fields\":\"_none_\"}")
            );
            assertFalse("stored_fields:_none_ should suppress _source: " + noneBody, noneBody.contains("\"_source\""));

            String docvalueBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":1,\"query\":{\"match_all\":{}},\"docvalue_fields\":[\"id\"]}")
            );
            assertTrue(
                "docvalue_fields should populate hits.fields on the shard path: " + docvalueBody,
                docvalueBody.contains("\"fields\":{\"id\"")
            );

            String explainBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":1,\"query\":{\"match_all\":{}},\"explain\":true}")
            );
            assertTrue("explain:true should add _explanation on the shard path: " + explainBody, explainBody.contains("\"_explanation\""));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testMinScoreTerminateAfterFallThroughToShardPath() throws Exception {
        // min_score and terminate_after are not implemented by the
        // fragment dispatch path (it counts matches from Lance without
        // those knobs); the dispatch filter sends those requests to
        // the shard path.
        String suffix = "s3-reject-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // Every match_all hit scores 1.0, so min_score above that
            // excludes everything.
            String minScoreBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"match_all\":{}},\"min_score\":2.0}")
            );
            assertEquals(0, extractIntPath(minScoreBody, "hits", "total", "value"));

            // The shard path signals terminate_after through
            // terminated_early; hits.total keeps the pre-terminate count,
            // so only the flag is asserted.
            String terminateBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"match_all\":{}},\"terminate_after\":2}")
            );
            assertTrue(
                "terminate_after should set terminated_early=true on the shard path: " + terminateBody,
                terminateBody.contains("\"terminated_early\":true")
            );
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
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
        // what the shard path reports for a freshly indexed doc.
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
        // every row, a shape only the shard path serves must be refused,
        // and attach and _lance/stats must say what happened.
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

            // A shape only the shard path serves would see the shard
            // reader's rows; it is refused with 400 naming both counts.
            ResponseException refused = expectThrows(
                ResponseException.class,
                () -> postJson("/" + pkTable + "/_search", "{\"size\":1,\"query\":{\"match_all\":{}},\"explain\":true}")
            );
            assertEquals(400, refused.getResponse().getStatusLine().getStatusCode());
            String refusedBody = readAll(refused.getResponse());
            assertEquals("illegal_argument_exception", stringPath(refusedBody, "error", "type"));
            assertTrue(refusedBody, refusedBody.contains("has 12 rows, above the Lucene bound of 4"));
            assertTrue(refusedBody, refusedBody.contains("would see only 4 rows"));

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
            // The shard path is open again for a table under the default bound.
            String explained = readAll(postJson("/" + pkTable + "/_search", "{\"size\":1,\"query\":{\"match_all\":{}},\"explain\":true}"));
            assertTrue(explained, explained.contains("\"_explanation\""));
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
