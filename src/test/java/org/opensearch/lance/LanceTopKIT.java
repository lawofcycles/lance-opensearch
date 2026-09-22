/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;

/**
 * The top-k pushdown end to end: a {@code _score} sort keeps the bare
 * page's Lance scan top-k, a value sort through a keyword sub-field
 * and through a renamed column folds and agrees with the Lucene
 * collector, and the shapes the planner does not fold (a sort mixing
 * {@code _score} with a column, {@code _geo_distance}) still answer
 * through the Lucene side.
 */
public class LanceTopKIT extends LanceRestTestCase {

    private static String explainBody(String indexName, String body) throws IOException {
        Request request = new Request("GET", "/" + indexName + "/_lance/explain");
        request.setJsonEntity(body);
        return readAll(client().performRequest(request));
    }

    public void testScoreSortMatchesTheBarePageForFtsAndKnn() throws Exception {
        String suffix = "topkscore-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // An explicit _score sort answers exactly what the bare page
            // answers, ids and scores alike. The page holds every match
            // (three rows say "lance"), so the bounded bare scan and the
            // unbounded sorted scan see the same set and the tie caveat
            // of the bounded cut does not apply.
            String fts = "\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"lance\"}}";
            List<Map<String, Object>> bare = hitsOf(readAll(postJson("/" + indexName + "/_search", "{\"size\":3," + fts + "}")));
            List<Map<String, Object>> scored = hitsOf(
                readAll(postJson("/" + indexName + "/_search", "{\"size\":3," + fts + ",\"sort\":[\"_score\"]}"))
            );
            assertEquals(3, scored.size());
            assertEquals(idsOf(bare), idsOf(scored));
            assertEquals(bare.stream().map(h -> h.get("_score")).toList(), scored.stream().map(h -> h.get("_score")).toList());

            String queryVector = "[0.5,0.0,0.0,0.0,0.0,0.0,0.0,0.0]";
            String knn = "\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":" + queryVector + ",\"k\":3}}";
            List<Map<String, Object>> knnBare = hitsOf(readAll(postJson("/" + indexName + "/_search", "{\"size\":3," + knn + "}")));
            List<Map<String, Object>> knnScored = hitsOf(
                readAll(postJson("/" + indexName + "/_search", "{\"size\":3," + knn + ",\"sort\":[\"_score\"]}"))
            );
            assertEquals(idsOf(knnBare), idsOf(knnScored));

            // The explain output shows the score page folded onto the
            // scan next to the pushed FTS.
            String explained = explainBody(indexName, "{\"size\":3," + fts + ",\"sort\":[\"_score\"]}");
            String physical = stringPath(explained, "physical");
            assertTrue("the FTS is pushed: " + physical, physical.contains("fts{"));
            assertTrue("the score page is pushed: " + physical, physical.contains("topk{"));

            // A sort mixing _score with a column stays on the Lucene
            // collector and still answers; the control wraps the FTS in
            // constant_score, which never plans, and must agree on ids.
            String mixed = "{\"size\":3," + fts + ",\"sort\":[\"_score\",{\"id\":\"desc\"}],\"track_scores\":true}";
            String control =
                "{\"size\":3,\"query\":{\"constant_score\":{\"filter\":{\"lance_match\":{\"field\":\"body\",\"query\":\"lance\"}}}},"
                    + "\"sort\":[\"_score\",{\"id\":\"desc\"}]}";
            List<Map<String, Object>> mixedHits = hitsOf(readAll(postJson("/" + indexName + "/_search", mixed)));
            List<Map<String, Object>> controlHits = hitsOf(readAll(postJson("/" + indexName + "/_search", control)));
            assertEquals(3, mixedHits.size());
            // Constant scores tie every hit, so the control's page is id
            // descending; the scored page breaks by BM25 first. Both
            // must hold the same document set (the three body matches).
            assertEquals(idsOf(controlHits).stream().sorted().toList(), idsOf(mixedHits).stream().sorted().toList());
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testValueSortThroughSubFieldAndRename() throws Exception {
        String suffix = "topkvalue-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        String tableUri = LanceTableFactory.writeEpochMillisTable(scratchDir, tableName);
        String indexName = tableName;
        try {
            Response attach = postJson(
                "/_lance/attach",
                "{\"table\":\""
                    + tableUri
                    + "\",\"overrides\":{\"label\":{\"type\":\"keyword\",\"fields\":{\"raw\":{\"type\":\"keyword\"}}}}}"
            );
            assertEquals("attach failed: " + readAll(attach), RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // Sort through the keyword sub-field, which the resolution
            // routes to the Utf8 base column. Labels sort "hello lance
            // 0/2/4" before "quick brown fox 1/3/5". The oracle forces
            // the Lucene comparator with a trivial aggregation.
            String sub = "{\"size\":6,\"sort\":[{\"label.raw\":\"asc\"},{\"id\":\"asc\"}]";
            String oracleAgg = ",\"aggs\":{\"n\":{\"value_count\":{\"field\":\"id\"}}}";
            List<Map<String, Object>> subHits = hitsOf(readAll(postJson("/" + indexName + "/_search", sub + "}")));
            List<Map<String, Object>> subOracle = hitsOf(readAll(postJson("/" + indexName + "/_search", sub + oracleAgg + "}")));
            assertEquals("label.raw sort must match the Lucene collector", subOracle, subHits);
            assertEquals(List.of("0-0", "0-2", "0-4", "0-1", "0-3", "0-5"), idsOf(subHits));
            String explained = explainBody(indexName, sub + "}");
            assertTrue(
                "the sub-field sort folds: " + stringPath(explained, "physical"),
                stringPath(explained, "physical").contains("topk{")
            );

            // Rename the column; the poll re-derives the mapping and the
            // sort by the live name folds the same way.
            LanceTableFactory.renameColumn(tableUri, "label", "tag");
            assertBusy(() -> {
                String mapping = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_mapping")));
                assertTrue("tag must arrive with the rename: " + mapping, mapping.contains("\"tag\":{\"type\":\"keyword\""));
            }, 30, TimeUnit.SECONDS);
            String renamed = "{\"size\":6,\"sort\":[{\"tag\":\"desc\"},{\"id\":\"asc\"}]";
            List<Map<String, Object>> renamedHits = hitsOf(readAll(postJson("/" + indexName + "/_search", renamed + "}")));
            List<Map<String, Object>> renamedOracle = hitsOf(readAll(postJson("/" + indexName + "/_search", renamed + oracleAgg + "}")));
            assertEquals("tag sort must match the Lucene collector", renamedOracle, renamedHits);
            assertEquals(List.of("0-5", "0-3", "0-1", "0-4", "0-2", "0-0"), idsOf(renamedHits));

            // The stale name refuses the fold and the Lucene side serves
            // nothing behind it either: the sort answers with the
            // missing sentinel for every row, in stable id order.
            String staleExplain = explainBody(indexName, "{\"size\":6,\"sort\":[{\"tag\":\"asc\"}]}");
            assertTrue(stringPath(staleExplain, "physical").contains("topk{"));
            ResponseException stale = expectThrows(ResponseException.class, () -> {
                Request request = new Request("GET", "/" + indexName + "/_lance/explain");
                request.setJsonEntity("{\"size\":6,\"sort\":[{\"label\":\"asc\"}]}");
                client().performRequest(request);
            });
            assertEquals(RestStatus.BAD_REQUEST.getStatus(), stale.getResponse().getStatusLine().getStatusCode());
            assertTrue(readAll(stale.getResponse()).contains("sort field [label] was renamed to [tag] in the Lance table"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testGeoDistanceSortStaysOnTheLuceneSide() throws Exception {
        String suffix = "topkgeo-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String indexName = "demo-" + suffix;
        String tableUri = LanceTableFactory.writeGeoStructTable(scratchDir, indexName);
        try {
            Response attach = postJson(
                "/_lance/attach",
                "{\"table\":\"" + tableUri + "\",\"overrides\":{\"location\":{\"type\":\"geo_point\"}}}"
            );
            assertEquals("attach failed: " + readAll(attach), RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // The planner refuses the shape (the explain endpoint names
            // it) and the doc-values comparator answers it: the nearest
            // rows to the landmark come back in distance order either
            // way, with or without a constant_score wrap forcing the
            // Lucene composition.
            String sort =
                "\"sort\":[{\"_geo_distance\":{\"location\":{\"lat\":35.6812,\"lon\":139.7671},\"order\":\"asc\",\"unit\":\"m\"}}]";
            String direct = "{\"size\":4,\"query\":{\"exists\":{\"field\":\"location\"}}," + sort + "}";
            String control = "{\"size\":4,\"query\":{\"constant_score\":{\"filter\":{\"exists\":{\"field\":\"location\"}}}}," + sort + "}";
            List<Map<String, Object>> directHits = hitsOf(readAll(postJson("/" + indexName + "/_search", direct)));
            List<Map<String, Object>> controlHits = hitsOf(readAll(postJson("/" + indexName + "/_search", control)));
            assertEquals(idsOf(controlHits), idsOf(directHits));
            assertEquals(List.of("0-0", "0-6", "0-2", "0-1"), idsOf(directHits));

            ResponseException refused = expectThrows(ResponseException.class, () -> {
                Request request = new Request("GET", "/" + indexName + "/_lance/explain");
                request.setJsonEntity(direct);
                client().performRequest(request);
            });
            assertEquals(RestStatus.BAD_REQUEST.getStatus(), refused.getResponse().getStatusLine().getStatusCode());
            assertTrue(readAll(refused.getResponse()).contains("sort type [_geo_distance]"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }
}
