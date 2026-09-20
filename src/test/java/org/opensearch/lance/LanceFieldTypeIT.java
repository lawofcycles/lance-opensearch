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
        // A Utf8 column without an inverted index is mapped as keyword.
        // The leaf reader must register it once in FieldInfos (it is
        // both a keyword doc-values column and a text column for FLS),
        // or shard recovery fails on duplicate field names.
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

            Response health = client().performRequest(
                new Request("GET", "/_cluster/health/" + indexName + "?wait_for_status=yellow&timeout=30s")
            );
            String healthBody = readAll(health);
            assertFalse("index went red: " + healthBody, healthBody.contains("\"status\":\"red\""));

            // A term query only resolves on keyword doc values.
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
        // Float32 maps to float and Float64 to double. Both ride the
        // numeric doc-value path in their sortable encoding, so range,
        // sort, metric aggregations and the _source round trip must all
        // see the original values.
        String suffix = "float-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeFloatColumnTable(scratchDir, tableName);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            String mappingBody = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_mapping")));
            assertTrue("mapping must expose price as float: " + mappingBody, mappingBody.contains("\"price\":{\"type\":\"float\""));
            assertTrue("mapping must expose weight as double: " + mappingBody, mappingBody.contains("\"weight\":{\"type\":\"double\""));

            // price = i * 12.5; [25.0, 55.0) keeps ids 2, 3, 4.
            String rangeBody = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"price\":{\"gte\":25.0,\"lt\":55.0}}}}")
            );
            assertEquals("range price total: " + rangeBody, 3, extractIntPath(rangeBody, "hits", "total", "value"));
            assertTrue("range price must include id=2: " + rangeBody, rangeBody.contains("\"id\":2"));
            assertTrue("range price must include id=3: " + rangeBody, rangeBody.contains("\"id\":3"));
            assertTrue("range price must include id=4: " + rangeBody, rangeBody.contains("\"id\":4"));

            // weight = i / 3; ascending order is id order.
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

            // sum(price) 187.5, avg 31.25, min 0.0, max 62.5;
            // sum(weight) 5.0, min 0.0, max 5/3.
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
            // sum accumulates in double over float32 inputs.
            assertEquals("sum(price) == 187.5", 187.5d, extractDoublePath(aggBody, "aggregations", "ps", "value"), 1e-4);
            assertEquals("avg(price) == 31.25", 31.25d, extractDoublePath(aggBody, "aggregations", "pa", "value"), 1e-4);
            assertEquals("min(price) == 0.0", 0.0d, extractDoublePath(aggBody, "aggregations", "pm", "value"), 1e-4);
            assertEquals("max(price) == 62.5", 62.5d, extractDoublePath(aggBody, "aggregations", "pM", "value"), 1e-4);
            assertEquals("sum(weight) == 5.0", 5.0d, extractDoublePath(aggBody, "aggregations", "ws", "value"), 1e-9);
            assertEquals("min(weight) == 0.0", 0.0d, extractDoublePath(aggBody, "aggregations", "wm", "value"), 1e-9);
            assertEquals("max(weight) == 5/3", 5.0d / 3.0d, extractDoublePath(aggBody, "aggregations", "wM", "value"), 1e-9);

            // id=4: price 50.0 exactly, weight 4/3 to six decimals.
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
        // int8 / int16 / int64 widths map to byte / short / long, int64
        // values above Integer.MAX_VALUE survive, and Arrow nulls in
        // numeric and boolean columns are absent rather than zero /
        // false. The fixture has 12 rows; id=5 is null in every column
        // except id.
        try (LanceTestCluster fixture = LanceTestCluster.setUpNullable("nullableints")) {
            String indexName = fixture.indexName();

            Response mappingResp = client().performRequest(new Request("GET", "/" + indexName + "/_mapping"));
            String mapping = readAll(mappingResp);
            assertTrue("expected count8 as byte in mapping: " + mapping, mapping.contains("\"count8\":{\"type\":\"byte\""));
            assertTrue("expected count16 as short in mapping: " + mapping, mapping.contains("\"count16\":{\"type\":\"short\""));
            assertTrue("expected count64 as long in mapping: " + mapping, mapping.contains("\"count64\":{\"type\":\"long\""));

            // count64 = 4_000_000_000 + i; gte 4_000_000_006 keeps i >= 6.
            Response gteResp = postJson(
                "/" + indexName + "/_search",
                "{\"size\":0,\"query\":{\"range\":{\"count64\":{\"gte\":4000000006}}}}"
            );
            int gteHits = extractIntPath(readAll(gteResp), "hits", "total", "value");
            assertEquals("range count64 gte 4000000006 should hit rows i=6..11", 6, gteHits);

            Response existsResp = postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"exists\":{\"field\":\"count8\"}}}");
            int existsHits = extractIntPath(readAll(existsResp), "hits", "total", "value");
            assertEquals("exists count8 should count 11 present rows (12 minus one null)", 11, existsHits);

            // flag is true for i in {0, 3, 6, 9}.
            Response flagTrue = postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"term\":{\"flag\":true}}}");
            int trueHits = extractIntPath(readAll(flagTrue), "hits", "total", "value");
            assertEquals("term flag=true should match i in {0,3,6,9}", 4, trueHits);

            // flag=false must not include the null row: seven rows.
            Response flagFalse = postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"term\":{\"flag\":false}}}");
            int falseHits = extractIntPath(readAll(flagFalse), "hits", "total", "value");
            assertEquals("term flag=false should count non-null false rows and skip the null row", 7, falseHits);
        }
    }
}
