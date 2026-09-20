/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.nio.file.Files;
import java.nio.file.Path;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Primary key handling: {@code _id} derivation for integer, unsigned, string
 * and absent primary keys, GET by id, and rows masked by a Lance deletion
 * file.
 */
public class LancePrimaryKeyIT extends LanceRestTestCase {

    public void testAttachAndGetById() throws Exception {
        // The default fixture declares no primary key, so attach records
        // an empty primary_key_field and GET /_doc answers 404 instead
        // of running a filter on an empty column.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(8, "attachAndGet")) {
            String indexName = fixture.indexName();

            Response settings = client().performRequest(new Request("GET", "/" + indexName + "/_settings"));
            String settingsBody = readAll(settings);
            assertTrue(
                "expected empty primary_key_field in settings, saw: " + settingsBody,
                settingsBody.contains("\"primary_key_field\":\"\"")
            );

            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> client().performRequest(new Request("GET", "/" + indexName + "/_doc/3"))
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 404 when PK is not declared, saw " + status, 404, status);
        }
    }

    public void testAttachOfPkLessTableDisablesGet() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "nopkget")) {
            String indexName = fixture.indexName();

            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> client().performRequest(new Request("GET", "/" + indexName + "/_doc/0"))
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 404 when PK is not declared, saw " + status, 404, status);
        }
    }

    public void testPkLessTableSynthesisesUniqueIdsInSearchResults() throws Exception {
        // Without a primary key every hit still needs a distinct _id;
        // clients that dedupe by _id would otherwise drop rows.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(6, "nopksearch")) {
            String indexName = fixture.indexName();

            Response search = postJson("/" + indexName + "/_search", "{\"size\":6}");
            String body = readAll(search);
            int totalHits = extractIntPath(body, "hits", "total", "value");
            assertEquals("expected 6 hits, saw response: " + body, 6, totalHits);
            java.util.Set<String> ids = new java.util.HashSet<>();
            try (XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, body)) {
                java.util.Map<String, Object> map = parser.map();
                @SuppressWarnings("unchecked")
                java.util.List<Object> hits = (java.util.List<Object>) ((java.util.Map<String, Object>) map.get("hits")).get("hits");
                for (Object hitObj : hits) {
                    @SuppressWarnings("unchecked")
                    java.util.Map<String, Object> hit = (java.util.Map<String, Object>) hitObj;
                    ids.add((String) hit.get("_id"));
                }
            }
            assertEquals("expected 6 unique synthesised ids, saw: " + ids + " (body=" + body + ")", 6, ids.size());
        }
    }

    public void testStringPrimaryKeyEchoesInHitsAndResolvesInGet() throws Exception {
        // A Utf8 primary key is echoed verbatim as _id and GET looks it
        // up through a quoted SQL literal.
        String suffix = "strpk-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeStringPkTable(scratchDir, tableName, 4);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(
                "attach on Utf8 PK table failed: " + readAll(attach),
                RestStatus.OK.getStatus(),
                attach.getStatusLine().getStatusCode()
            );

            Response settings = client().performRequest(new Request("GET", "/" + indexName + "/_settings"));
            String settingsBody = readAll(settings);
            assertTrue("expected primary_key_field: key, saw: " + settingsBody, settingsBody.contains("\"primary_key_field\":\"key\""));
            assertTrue(
                "expected primary_key_type: keyword, saw: " + settingsBody,
                settingsBody.contains("\"primary_key_type\":\"keyword\"")
            );

            String searchBody = readAll(postJson("/" + indexName + "/_search", "{\"size\":4}"));
            assertEquals(4, extractIntPath(searchBody, "hits", "total", "value"));
            java.util.Set<String> ids = new java.util.HashSet<>();
            try (XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, searchBody)) {
                java.util.Map<String, Object> map = parser.map();
                @SuppressWarnings("unchecked")
                java.util.List<Object> hits = (java.util.List<Object>) ((java.util.Map<String, Object>) map.get("hits")).get("hits");
                for (Object hitObj : hits) {
                    @SuppressWarnings("unchecked")
                    java.util.Map<String, Object> hit = (java.util.Map<String, Object>) hitObj;
                    ids.add((String) hit.get("_id"));
                }
            }
            assertEquals(
                "expected 4 alpha-N ids, saw: " + ids + " (body=" + searchBody + ")",
                java.util.Set.of("alpha-0", "alpha-1", "alpha-2", "alpha-3"),
                ids
            );

            Response getResponse = client().performRequest(new Request("GET", "/" + indexName + "/_doc/alpha-2"));
            assertEquals(
                "expected 200 for GET on Utf8 PK, saw " + getResponse.getStatusLine().getStatusCode(),
                200,
                getResponse.getStatusLine().getStatusCode()
            );
            String getBody = readAll(getResponse);
            assertTrue("expected found:true, saw: " + getBody, getBody.contains("\"found\":true"));
            assertTrue("expected _id:alpha-2, saw: " + getBody, getBody.contains("\"_id\":\"alpha-2\""));
            assertTrue("expected key:alpha-2 in _source, saw: " + getBody, getBody.contains("\"key\":\"alpha-2\""));
            assertTrue("expected label:row-2 in _source, saw: " + getBody, getBody.contains("\"label\":\"row-2\""));

            ResponseException notFound = expectThrows(
                ResponseException.class,
                () -> client().performRequest(new Request("GET", "/" + indexName + "/_doc/alpha-999"))
            );
            assertEquals(
                "expected 404 for missing Utf8 PK, saw " + notFound.getResponse().getStatusLine().getStatusCode(),
                404,
                notFound.getResponse().getStatusLine().getStatusCode()
            );

            // A single quote in the id must be escaped in the SQL literal,
            // not break the filter.
            ResponseException quoted = expectThrows(
                ResponseException.class,
                () -> client().performRequest(new Request("GET", "/" + indexName + "/_doc/al'pha"))
            );
            assertEquals(
                "expected 404 for quoted Utf8 id, saw " + quoted.getResponse().getStatusLine().getStatusCode(),
                404,
                quoted.getResponse().getStatusLine().getStatusCode()
            );
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testUnsignedLongPrimaryKeyRoundTripsThroughIdAndGet() throws Exception {
        // A UInt64 primary key round-trips through _id and GET for values
        // above Long.MAX_VALUE: the reader keeps the raw bit pattern,
        // _id renders it unsigned, and GET parses through BigInteger.
        String suffix = "ulongpk-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeUnsignedLongPkTable(scratchDir, tableName);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(
                "attach on UInt64 PK table failed: " + readAll(attach),
                RestStatus.OK.getStatus(),
                attach.getStatusLine().getStatusCode()
            );

            String settingsBody = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_settings")));
            assertTrue(
                "expected primary_key_type: unsigned_long, saw: " + settingsBody,
                settingsBody.contains("\"primary_key_type\":\"unsigned_long\"")
            );
            String mappingBody = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_mapping")));
            assertTrue("expected mapping type unsigned_long: " + mappingBody, mappingBody.contains("\"type\":\"unsigned_long\""));

            // 0, 42, Long.MAX_VALUE and 2^64 - 6.
            String searchBody = readAll(postJson("/" + indexName + "/_search", "{\"size\":4}"));
            assertEquals(4, extractIntPath(searchBody, "hits", "total", "value"));
            java.util.Set<String> ids = new java.util.HashSet<>();
            try (XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, searchBody)) {
                java.util.Map<String, Object> map = parser.map();
                @SuppressWarnings("unchecked")
                java.util.List<Object> hits = (java.util.List<Object>) ((java.util.Map<String, Object>) map.get("hits")).get("hits");
                for (Object hitObj : hits) {
                    @SuppressWarnings("unchecked")
                    java.util.Map<String, Object> hit = (java.util.Map<String, Object>) hitObj;
                    ids.add((String) hit.get("_id"));
                }
            }
            assertEquals(
                "expected the four canonical UInt64 ids, saw: " + ids + " (body=" + searchBody + ")",
                java.util.Set.of("0", "42", "9223372036854775807", "18446744073709551610"),
                ids
            );

            Response getLow = client().performRequest(new Request("GET", "/" + indexName + "/_doc/42"));
            assertEquals(200, getLow.getStatusLine().getStatusCode());
            String getLowBody = readAll(getLow);
            assertTrue("expected _id:42, saw: " + getLowBody, getLowBody.contains("\"_id\":\"42\""));

            // Above Long.MAX_VALUE.
            Response getHigh = client().performRequest(new Request("GET", "/" + indexName + "/_doc/18446744073709551610"));
            assertEquals(200, getHigh.getStatusLine().getStatusCode());
            String getHighBody = readAll(getHigh);
            assertTrue("expected _id:18446744073709551610, saw: " + getHighBody, getHighBody.contains("\"_id\":\"18446744073709551610\""));

            // Negative and oversized ids cannot match a UInt64 row.
            ResponseException notFound = expectThrows(
                ResponseException.class,
                () -> client().performRequest(new Request("GET", "/" + indexName + "/_doc/-1"))
            );
            assertEquals(404, notFound.getResponse().getStatusLine().getStatusCode());
            ResponseException tooLarge = expectThrows(
                ResponseException.class,
                () -> client().performRequest(new Request("GET", "/" + indexName + "/_doc/99999999999999999999"))
            );
            assertEquals(404, tooLarge.getResponse().getStatusLine().getStatusCode());
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testDeletedRowsStayOutOfHitsTotalsAndSource() throws Exception {
        // A fragment with a deletion file: the reader's liveDocs (built
        // from the deletion file) and the per-hit fetch of _id / _source
        // must agree with Lance on which rows exist.
        String suffix = "deleted-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeStringPkTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        // Six physical rows remain; offsets 1 and 4 become liveDocs holes.
        LanceTableFactory.deleteRows(tableUri, "key IN ('alpha-1', 'alpha-4')");
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals("attach failed: " + readAll(attach), RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // _count (engine path) and hits.total (fragment path) must
            // both exclude the deleted rows.
            String countBody = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_count")));
            assertEquals("_count body=" + countBody, 4, extractIntPath(countBody, "count"));

            // match_all iterates every doc id in Lucene, so this shape
            // depends on liveDocs.
            String searchBody = readAll(postJson("/" + indexName + "/_search", "{\"size\":10,\"sort\":[{\"key\":\"asc\"}]}"));
            assertEquals("hits.total body=" + searchBody, 4, extractIntPath(searchBody, "hits", "total", "value"));
            java.util.List<String> ids = new java.util.ArrayList<>();
            java.util.List<String> labels = new java.util.ArrayList<>();
            try (XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, searchBody)) {
                java.util.Map<String, Object> map = parser.map();
                @SuppressWarnings("unchecked")
                java.util.List<Object> hits = (java.util.List<Object>) ((java.util.Map<String, Object>) map.get("hits")).get("hits");
                for (Object hitObj : hits) {
                    @SuppressWarnings("unchecked")
                    java.util.Map<String, Object> hit = (java.util.Map<String, Object>) hitObj;
                    ids.add((String) hit.get("_id"));
                    @SuppressWarnings("unchecked")
                    java.util.Map<String, Object> source = (java.util.Map<String, Object>) hit.get("_source");
                    labels.add((String) source.get("label"));
                }
            }
            assertEquals(
                "expected the four surviving keys in sort order, saw " + ids + " (body=" + searchBody + ")",
                java.util.List.of("alpha-0", "alpha-2", "alpha-3", "alpha-5"),
                ids
            );
            // Labels are "row-N" for even N and "col-N" for odd N.
            assertEquals(
                "expected surviving labels aligned with ids, saw " + labels,
                java.util.List.of("row-0", "row-2", "col-3", "col-5"),
                labels
            );

            // The Lance filter scan skips deleted rows on its own.
            String deletedTerm = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"term\":{\"key\":\"alpha-1\"}}}")
            );
            assertEquals("deleted key must not match, body=" + deletedTerm, 0, extractIntPath(deletedTerm, "hits", "total", "value"));

            ResponseException notFound = expectThrows(
                ResponseException.class,
                () -> client().performRequest(new Request("GET", "/" + indexName + "/_doc/alpha-4"))
            );
            assertEquals("expected 404 for deleted key", 404, notFound.getResponse().getStatusLine().getStatusCode());
            String getBody = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_doc/alpha-5")));
            assertTrue("expected found:true for surviving key, saw " + getBody, getBody.contains("\"found\":true"));
            assertTrue("expected label:col-5, saw " + getBody, getBody.contains("\"label\":\"col-5\""));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }
}
