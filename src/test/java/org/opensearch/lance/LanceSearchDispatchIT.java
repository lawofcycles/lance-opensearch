/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.nio.file.Files;
import java.nio.file.Path;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
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

    public void testMinScoreTerminateAfterTrackTotalHitsFallThroughToShardPath() throws Exception {
        // min_score, terminate_after and track_total_hits are not
        // implemented by the fragment dispatch path (it counts matches
        // from Lance without those knobs); the dispatch filter sends
        // those requests to the shard path.
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

            String trackBoundBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"match_all\":{}},\"track_total_hits\":3}")
            );
            assertTrue("track_total_hits:3 should return relation=gte: " + trackBoundBody, trackBoundBody.contains("\"relation\":\"gte\""));

            // track_total_hits=false omits hits.total entirely.
            String trackFalseBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"match_all\":{}},\"track_total_hits\":false}")
            );
            assertFalse("track_total_hits:false should omit hits.total: " + trackFalseBody, trackFalseBody.contains("\"total\":{"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
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
}
