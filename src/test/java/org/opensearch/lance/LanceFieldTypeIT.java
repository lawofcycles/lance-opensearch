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
 * Mapping and query behaviour of Arrow column types beyond the basic
 * fixture: Utf8 without an FTS index, Float32 / Float64, and nullable narrow
 * / wide integers and booleans.
 */
public class LanceFieldTypeIT extends LanceRestTestCase {

    public void testAttachKeywordOnlyUtf8TableGoesGreen() throws Exception {
        // Regression for the SHA 403576c FieldInfos-duplicate bug: a
        // Utf8 column with no inverted index is loaded through the
        // keyword doc-values path AND through the text-column loop
        // that e21bf3c added for FLS visibility, and the old code
        // added both to the leaf reader's FieldInfos. Shard recovery
        // then failed with IllegalArgumentException: duplicate field
        // names and every FTS-less string-column table was red.
        // Verify the attach succeeds, the shard settles green, and a
        // term-level search against the keyword column returns hits
        // instead of a shards.failed response.
        String suffix = "keywordonly-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeKeywordOnlyTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(
                "attach on keyword-only Utf8 table failed: " + readAll(attach),
                RestStatus.OK.getStatus(),
                attach.getStatusLine().getStatusCode()
            );

            // Health must not stay red. The recovery previously threw
            // IllegalArgumentException during LanceFragmentLeafReader
            // construction and marked the shard failed permanently.
            Response health = client().performRequest(
                new Request("GET", "/_cluster/health/" + indexName + "?wait_for_status=yellow&timeout=30s")
            );
            String healthBody = readAll(health);
            assertFalse("index went red: " + healthBody, healthBody.contains("\"status\":\"red\""));

            // Confirm derive() surfaced the column as keyword by
            // running a term query, which only works on keyword doc
            // values. If FieldInfos was duplicated the shard wouldn't
            // respond at all.
            Response search = postJson("/" + indexName + "/_search", "{\"query\":{\"term\":{\"label\":\"row-3\"}}}");
            String searchBody = readAll(search);
            int hits = extractIntPath(searchBody, "hits", "total", "value");
            assertEquals("expected exactly one match for label=row-3, saw: " + searchBody, 1, hits);

            Response mapping = client().performRequest(new Request("GET", "/" + indexName + "/_mapping"));
            String mappingBody = readAll(mapping);
            assertTrue("expected label mapped as keyword: " + mappingBody, mappingBody.contains("\"label\":{\"type\":\"keyword\""));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAnswersFloatQueries() throws Exception {
        // Issue #44: Float32 and Float64 scalar columns used to fall
        // through derive() into the "stored only" notes bucket,
        // leaving the column with no mapping. `range price` returned
        // 500 `Rewrite first` (unknown-field query path),
        // `sort price` returned 400 `No mapping found`, metric
        // aggregations returned 200 but with a null value, and the
        // JSON _source omitted the column entirely despite the
        // "stored only" wording.
        //
        // The fix wires float32 → `float` and float64 → `double`
        // through derive(), and the leaf reader routes both through
        // the shared NumericDocValues path via
        // NumericUtils.floatToSortableInt / doubleToSortableLong.
        // _source rendering decodes the sortable encoding on the
        // way out so the JSON number equals the original value.
        //
        // Shapes exercised:
        // (a) range on float32 (price >= 25.0 && price < 55.0)
        // (b) sort ascending on float64 (weight)
        // (c) metric aggregations (sum / avg / min / max) on both columns
        // (d) _source round trip of the exact float32 / float64 values
        String suffix = "float-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeFloatColumnTable(scratchDir, tableName);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // Mapping must expose price / weight with the correct
            // OpenSearch numeric types. Regression fence for the
            // derive() branch: if a future refactor drops the
            // FloatingPoint case, this assertion catches it before
            // the query-side asserts explain why.
            String mappingBody = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_mapping")));
            assertTrue("mapping must expose price as float: " + mappingBody, mappingBody.contains("\"price\":{\"type\":\"float\""));
            assertTrue("mapping must expose weight as double: " + mappingBody, mappingBody.contains("\"weight\":{\"type\":\"double\""));

            // (a) range on price: 6 rows, price = i * 12.5f, so
            // values are {0.0, 12.5, 25.0, 37.5, 50.0, 62.5}. The
            // half-open range [25.0, 55.0) picks {25.0, 37.5, 50.0},
            // rows id={2, 3, 4}.
            String rangeBody = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"price\":{\"gte\":25.0,\"lt\":55.0}}}}")
            );
            assertEquals("range price total: " + rangeBody, 3, extractIntPath(rangeBody, "hits", "total", "value"));
            assertTrue("range price must include id=2: " + rangeBody, rangeBody.contains("\"id\":2"));
            assertTrue("range price must include id=3: " + rangeBody, rangeBody.contains("\"id\":3"));
            assertTrue("range price must include id=4: " + rangeBody, rangeBody.contains("\"id\":4"));

            // (b) sort weight ascending: values are {0/3, 1/3, 2/3,
            // 1.0, 4/3, 5/3}. Ascending sort returns id order 0..5.
            // Reading id from the first hit is enough to prove the
            // double doc-value comparator ordered them correctly.
            String sortBody = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"match_all\":{}},\"sort\":[{\"weight\":\"asc\"}],\"size\":6}")
            );
            assertEquals("sort weight total: " + sortBody, 6, extractIntPath(sortBody, "hits", "total", "value"));
            assertEquals(
                "sort weight asc first hit must be id=0: " + sortBody,
                0,
                extractIntPath(sortBody, "hits", "hits", "0", "_source", "id")
            );
            assertEquals(
                "sort weight asc last hit must be id=5: " + sortBody,
                5,
                extractIntPath(sortBody, "hits", "hits", "5", "_source", "id")
            );

            // (c) metric aggs: expected values for six rows.
            // sum(price) = 12.5 * (0+1+2+3+4+5) = 12.5 * 15 = 187.5
            // avg(price) = 187.5 / 6 = 31.25
            // min(price) = 0.0
            // max(price) = 62.5
            // sum(weight) = 0/3+1/3+2/3+3/3+4/3+5/3 = 15/3 = 5.0
            // min(weight) = 0.0
            // max(weight) = 5/3 ≈ 1.6666666
            String aggBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"aggs\":{"
                        + "\"ps\":{\"sum\":{\"field\":\"price\"}},"
                        + "\"pa\":{\"avg\":{\"field\":\"price\"}},"
                        + "\"pm\":{\"min\":{\"field\":\"price\"}},"
                        + "\"pM\":{\"max\":{\"field\":\"price\"}},"
                        + "\"ws\":{\"sum\":{\"field\":\"weight\"}},"
                        + "\"wm\":{\"min\":{\"field\":\"weight\"}},"
                        + "\"wM\":{\"max\":{\"field\":\"weight\"}}"
                        + "}}"
                )
            );
            // Small tolerance for float rounding: even sortableInt
            // decoding preserves the original float32 exactly, but
            // sum accumulates in double and the intermediate float32
            // representations round.
            assertEquals("sum(price) == 187.5", 187.5d, extractDoublePath(aggBody, "aggregations", "ps", "value"), 1e-4);
            assertEquals("avg(price) == 31.25", 31.25d, extractDoublePath(aggBody, "aggregations", "pa", "value"), 1e-4);
            assertEquals("min(price) == 0.0", 0.0d, extractDoublePath(aggBody, "aggregations", "pm", "value"), 1e-4);
            assertEquals("max(price) == 62.5", 62.5d, extractDoublePath(aggBody, "aggregations", "pM", "value"), 1e-4);
            assertEquals("sum(weight) == 5.0", 5.0d, extractDoublePath(aggBody, "aggregations", "ws", "value"), 1e-9);
            assertEquals("min(weight) == 0.0", 0.0d, extractDoublePath(aggBody, "aggregations", "wm", "value"), 1e-9);
            assertEquals("max(weight) == 5/3", 5.0d / 3.0d, extractDoublePath(aggBody, "aggregations", "wM", "value"), 1e-9);

            // (d) _source round trip: the hit for id=4 must carry
            // price = 50.0 (exact float32) and weight = 4/3
            // (double, 6-decimal digit match is enough — the JSON
            // codec emits the shortest round-trippable form).
            String hitBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"term\":{\"id\":4}},\"size\":1}"));
            assertEquals("term id=4 total", 1, extractIntPath(hitBody, "hits", "total", "value"));
            assertEquals("id=4 price must round trip", 50.0d, extractDoublePath(hitBody, "hits", "hits", "0", "_source", "price"), 0.0d);
            assertEquals(
                "id=4 weight must round trip",
                4.0d / 3.0d,
                extractDoublePath(hitBody, "hits", "hits", "0", "_source", "weight"),
                1e-9
            );
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testNullableAndWideIntColumnsBehaveCorrectly() throws Exception {
        // Regression for B4 (per-column presence bitmaps, int8/int16/int64
        // mapping, boolean null). Before the fix a Lance table with any of
        // - nullable int32 / date / timestamp column
        // - int8 or int16 column
        // - a null in a boolean column
        // - int64 that exceeds Integer.MAX_VALUE
        // took the shard red on recovery or produced silently wrong
        // results. The nullable table has 12 rows; id=5 is all-null.
        try (LanceTestCluster fixture = LanceTestCluster.setUpNullable("nullableints")) {
            String indexName = fixture.indexName();

            // Mapping must reflect the Arrow int widths as byte / short / long.
            Response mappingResp = client().performRequest(new Request("GET", "/" + indexName + "/_mapping"));
            String mapping = readAll(mappingResp);
            assertTrue("expected count8 as byte in mapping: " + mapping, mapping.contains("\"count8\":{\"type\":\"byte\""));
            assertTrue("expected count16 as short in mapping: " + mapping, mapping.contains("\"count16\":{\"type\":\"short\""));
            assertTrue("expected count64 as long in mapping: " + mapping, mapping.contains("\"count64\":{\"type\":\"long\""));

            // Range on count64 must handle > Integer.MAX_VALUE values.
            // count64 = 4_000_000_000 + i, so gte 4_000_000_006 hits i>=6.
            // id=5 is null so exactly six rows should match.
            Response gteResp = postJson(
                "/" + indexName + "/_search",
                "{\"size\":0,\"query\":{\"range\":{\"count64\":{\"gte\":4000000006}}}}"
            );
            int gteHits = extractIntPath(readAll(gteResp), "hits", "total", "value");
            assertEquals("range count64 gte 4000000006 should hit rows i=6..11", 6, gteHits);

            // exists on any nullable column must skip the all-null row.
            Response existsResp = postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"exists\":{\"field\":\"count8\"}}}");
            int existsHits = extractIntPath(readAll(existsResp), "hits", "total", "value");
            assertEquals("exists count8 should count 11 present rows (12 minus one null)", 11, existsHits);

            // flag=true holds when i in {0,3,6,9}: four rows.
            Response flagTrue = postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"term\":{\"flag\":true}}}");
            int trueHits = extractIntPath(readAll(flagTrue), "hits", "total", "value");
            assertEquals("term flag=true should match i in {0,3,6,9}", 4, trueHits);

            // flag=false must NOT include the null row (id=5). Non-null
            // false rows are i in {1,2,4,7,8,10,11} = 7.
            Response flagFalse = postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"term\":{\"flag\":false}}}");
            int falseHits = extractIntPath(readAll(flagFalse), "hits", "total", "value");
            assertEquals("term flag=false should count non-null false rows and skip the null row", 7, falseHits);
        }
    }
}
