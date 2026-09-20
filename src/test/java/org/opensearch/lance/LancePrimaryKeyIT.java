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
        // Without a declared primary key on the Lance side (see LanceTableFactory
        // — adding the metadata breaks the C Data serialisation of the
        // FixedSizeList vector column), attach must publish an empty
        // primary_key_field. B10 guarantees that engine.get returns
        // NOT_EXISTS immediately in that case rather than 500-ing on an
        // empty filter column, so GET /_doc must return 404.
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
        // Same intent as testAttachAndGetById but uses a second, independent
        // fixture so the assertion still covers the derivation branch when
        // the primary integer column is not the first field.
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
        // Regression for #24: a table with no declared primary key used to
        // emit _id: "0" for every hit because the reader's values[] array
        // stayed at its default long[] zeros. Every hit collapsed to the
        // same id and any client that dedup'd by _id (Dashboards result
        // grids, _mget by hits, etc.) silently lost rows. Synthesised ids
        // must at least be unique within the shard.
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
        // Regression for #24: a Utf8 primary key column used to be read
        // through readAsLong (returning 0 for every row) and looked up
        // through Long.parseLong (which either 500'd Lance with "Received
        // literal Int64(0) and could not convert to literal of type 'Utf8'"
        // or short-circuited to 404 for non-numeric ids). Now the reader
        // holds string PK values in a parallel array, _search emits them
        // as _id verbatim, and GET builds a SQL-quoted filter so the
        // Lance scan finds the row.
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

            // Derivation must surface the Utf8 PK column as the
            // declared primary_key_field, and the new type setting must
            // carry the string form so the engine picks the KEYWORD
            // lookup path on reopen.
            Response settings = client().performRequest(new Request("GET", "/" + indexName + "/_settings"));
            String settingsBody = readAll(settings);
            assertTrue("expected primary_key_field: key, saw: " + settingsBody, settingsBody.contains("\"primary_key_field\":\"key\""));
            assertTrue(
                "expected primary_key_type: keyword, saw: " + settingsBody,
                settingsBody.contains("\"primary_key_type\":\"keyword\"")
            );

            // _search must return the Utf8 PK values as _id verbatim.
            // Old behaviour returned _id: "0" for every row.
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

            // GET by a known key resolves through the quoted Lance
            // filter. Previously this either 500'd or 404'd.
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

            // Unknown key must return 404 (not 500). This exercises the
            // negative branch of the SQL-quoted filter.
            ResponseException notFound = expectThrows(
                ResponseException.class,
                () -> client().performRequest(new Request("GET", "/" + indexName + "/_doc/alpha-999"))
            );
            assertEquals(
                "expected 404 for missing Utf8 PK, saw " + notFound.getResponse().getStatusLine().getStatusCode(),
                404,
                notFound.getResponse().getStatusLine().getStatusCode()
            );

            // Single quote in the id must not break the filter or open
            // an injection path. `''` is the SQL escape for a literal
            // quote inside a quoted string; the escape puts an
            // unmatched-in-the-data id past the filter, so Lance
            // returns no rows and the engine reports 404.
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
        // Issue #24 remainder: a UInt64 PK column must survive the round
        // trip through _search / _id / GET even when the values sit
        // above Long.MAX_VALUE. The reader holds them as raw long bit
        // patterns; _id decodes with Long.toUnsignedString and GET
        // parses through BigInteger before handing a wide decimal
        // literal to Lance's SQL filter.
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

            // Settings must carry primary_key_type: unsigned_long, and
            // mapping must expose the PK column as unsigned_long so
            // OpenSearch's built-in field type handles the doc value
            // interpretation.
            String settingsBody = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_settings")));
            assertTrue(
                "expected primary_key_type: unsigned_long, saw: " + settingsBody,
                settingsBody.contains("\"primary_key_type\":\"unsigned_long\"")
            );
            String mappingBody = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_mapping")));
            assertTrue("expected mapping type unsigned_long: " + mappingBody, mappingBody.contains("\"type\":\"unsigned_long\""));

            // _search must return four distinct _id strings: 0, 42,
            // Long.MAX_VALUE (9223372036854775807), and 2^64 - 6
            // (18446744073709551610). Previously the top-half value
            // would have shown as -6 or 0.
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

            // GET by a low-half key resolves through Long.parseLong /
            // BigInteger and hits the Lance filter with a literal
            // Lance understands.
            Response getLow = client().performRequest(new Request("GET", "/" + indexName + "/_doc/42"));
            assertEquals(200, getLow.getStatusLine().getStatusCode());
            String getLowBody = readAll(getLow);
            assertTrue("expected _id:42, saw: " + getLowBody, getLowBody.contains("\"_id\":\"42\""));

            // GET by the top-half key exercises the BigInteger path.
            // Previously Long.parseLong would have thrown
            // NumberFormatException and the engine short-circuited to
            // 404.
            Response getHigh = client().performRequest(new Request("GET", "/" + indexName + "/_doc/18446744073709551610"));
            assertEquals(200, getHigh.getStatusLine().getStatusCode());
            String getHighBody = readAll(getHigh);
            assertTrue("expected _id:18446744073709551610, saw: " + getHighBody, getHighBody.contains("\"_id\":\"18446744073709551610\""));

            // Negative / oversized ids never match a UInt64 row and
            // must be rejected as 404 before Lance sees them.
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
        // The leaf reader used to learn which physical rows are live by
        // scanning `_rowaddr` (plus the primary key) for every fragment
        // at open time. That scan was also what fed `_id`. Both now
        // come from different places: liveDocs from the fragment's
        // deletion file (a `_rowaddr`-only scan runs only when the
        // fragment metadata reports one), `_id` and `_source` from a
        // per-hit `_rowaddr IN (...)` take. This test creates a table
        // with a deletion file and checks that the two paths agree
        // with each other and with Lance: deleted rows are not counted,
        // not returned, and the survivors' `_id` / `_source` still
        // round-trip the primary key.
        String suffix = "deleted-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeStringPkTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        // Delete two rows out of six. The fragment keeps six physical
        // rows (maxDoc stays 6) and gains a deletion file, so docids 1
        // and 4 become liveDocs holes.
        LanceTableFactory.deleteRows(tableUri, "key IN ('alpha-1', 'alpha-4')");
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals("attach failed: " + readAll(attach), RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // _count goes through the engine's reader; hits.total goes
            // through the fragment path's Lance-side count. Both must
            // exclude the two deleted rows.
            String countBody = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_count")));
            assertEquals("_count body=" + countBody, 4, extractIntPath(countBody, "count"));

            // match_all with size covering the whole table: Lucene
            // iterates 0..maxDoc here (no Lance scan produces the doc
            // ids), so this is the shape that depends on liveDocs
            // being built from the deletion file.
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
            // _id comes from the per-hit take of the PK column; the
            // deleted keys must not appear and the survivors must be
            // the operator's original strings, not synthesised ids.
            assertEquals(
                "expected the four surviving keys in sort order, saw " + ids + " (body=" + searchBody + ")",
                java.util.List.of("alpha-0", "alpha-2", "alpha-3", "alpha-5"),
                ids
            );
            // _source comes from the same take; labels are "row-N" for
            // even N and "col-N" for odd N in the fixture.
            assertEquals(
                "expected surviving labels aligned with ids, saw " + labels,
                java.util.List.of("row-0", "row-2", "col-3", "col-5"),
                labels
            );

            // A term query on a deleted key resolves through the Lance
            // scalar filter, which skips deleted rows on its own; the
            // count path must agree (0), not report the pre-deletion
            // presence.
            String deletedTerm = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"term\":{\"key\":\"alpha-1\"}}}")
            );
            assertEquals("deleted key must not match, body=" + deletedTerm, 0, extractIntPath(deletedTerm, "hits", "total", "value"));

            // GET by primary key: the engine path (SQL filter on the PK)
            // must also honour the deletion.
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
