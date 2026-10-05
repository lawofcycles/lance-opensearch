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

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;

/**
 * {@code GET /_doc/{id}} and {@code _mget} on a Lance backed index are
 * answered from the table by the plugin's action filter: at the version
 * the index follows (latest, pinned or tag), with the request's
 * {@code _source} filter honoured, with a row the reader wrapper hides
 * answered as absent, and without the shard's get service being entered.
 * {@code _termvectors}, {@code _mtermvectors} and {@code _explain/{id}}
 * on such an index are refused with 400.
 */
public class LanceGetFilterIT extends LanceRestTestCase {

    public void testGetReadsTheVersionTheIndexFollows() throws Exception {
        // Version A holds alpha-0..alpha-3, version B adds alpha-4 and
        // alpha-5. An index pinned to A does not see alpha-5 and reports
        // A as _version; an index following the table sees it and reports
        // B; an index on a tag follows the tag's moves.
        String suffix = "getver-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        String tableUri = LanceTableFactory.writeStringPkTable(scratchDir, tableName, 4);
        long versionA = LanceTableFactory.currentVersion(tableUri);
        LanceTableFactory.appendStringPkRows(tableUri, 4, 2);
        long versionB = LanceTableFactory.currentVersion(tableUri);
        assertTrue(versionB > versionA);
        LanceTableFactory.createTag(tableUri, "v1", versionA);

        String latest = tableName + "-latest";
        String pinned = tableName + "-pinned";
        String tagged = tableName + "-tag";
        try {
            attach("{\"table\":\"" + tableUri + "\",\"name\":\"" + latest + "\"}");
            attach("{\"table\":\"" + tableUri + "\",\"name\":\"" + pinned + "\",\"version\":" + versionA + "}");
            attach("{\"table\":\"" + tableUri + "\",\"name\":\"" + tagged + "\",\"tag\":\"v1\"}");
            for (String index : new String[] { latest, pinned, tagged }) {
                ensureGreen(index);
            }

            Map<String, Object> onLatest = getDoc(latest, "alpha-5");
            assertEquals(onLatest.toString(), Boolean.TRUE, onLatest.get("found"));
            assertEquals("the latest index reports the latest version", versionB, ((Number) onLatest.get("_version")).longValue());
            assertEquals(onLatest.toString(), "col-5", sourceOf(onLatest).get("label"));

            assertEquals("a row above the pin is absent", 404, getStatus(pinned, "alpha-5"));
            Map<String, Object> onPinned = getDoc(pinned, "alpha-2");
            assertEquals(onPinned.toString(), Boolean.TRUE, onPinned.get("found"));
            assertEquals("the pinned index reports the pinned version", versionA, ((Number) onPinned.get("_version")).longValue());
            assertEquals("row-2", sourceOf(onPinned).get("label"));

            assertEquals("a row above the tag's version is absent", 404, getStatus(tagged, "alpha-4"));
            Map<String, Object> onTag = getDoc(tagged, "alpha-1");
            assertEquals(versionA, ((Number) onTag.get("_version")).longValue());

            // The tag moves to B: the very next GET reads B.
            LanceTableFactory.updateTag(tableUri, "v1", versionB);
            Map<String, Object> onMovedTag = getDoc(tagged, "alpha-4");
            assertEquals(onMovedTag.toString(), Boolean.TRUE, onMovedTag.get("found"));
            assertEquals(versionB, ((Number) onMovedTag.get("_version")).longValue());
            assertEquals("row-4", sourceOf(onMovedTag).get("label"));

            // The shard's get service was never entered: the filter
            // answered every GET above before the stock action resolved a
            // shard, so the index's get counter stays at zero.
            for (String index : new String[] { latest, pinned, tagged }) {
                String stats = readAll(client().performRequest(new Request("GET", "/" + index + "/_stats/get")));
                assertEquals("get.total of " + index + ": " + stats, 0, extractIntPath(stats, "indices", index, "total", "get", "total"));
            }
        } finally {
            for (String index : new String[] { latest, pinned, tagged }) {
                try {
                    client().performRequest(new Request("DELETE", "/" + index));
                } catch (Exception ignored) {}
            }
        }
    }

    public void testGetHonoursTheSourceFilterAndStoredFields() throws Exception {
        String suffix = "getsrc-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        String tableUri = LanceTableFactory.writeStringPkTable(scratchDir, tableName, 4);
        String indexName = tableName;
        try {
            attach("{\"table\":\"" + tableUri + "\"}");
            ensureGreen(indexName);

            Map<String, Object> whole = getDoc(indexName, "alpha-3");
            assertEquals(Map.of("key", "alpha-3", "label", "col-3"), sourceOf(whole));
            assertEquals("alpha-3", whole.get("_id"));
            assertEquals(indexName, whole.get("_index"));

            Map<String, Object> includes = getDoc(indexName, "alpha-3?_source_includes=label");
            assertEquals(Map.of("label", "col-3"), sourceOf(includes));

            Map<String, Object> excludes = getDoc(indexName, "alpha-3?_source_excludes=label");
            assertEquals(Map.of("key", "alpha-3"), sourceOf(excludes));

            Map<String, Object> noSource = getDoc(indexName, "alpha-3?_source=false");
            assertEquals(Boolean.TRUE, noSource.get("found"));
            assertFalse("_source is left out: " + noSource, noSource.containsKey("_source"));

            // stored_fields without _source: the row is found and no source
            // is rendered, as the stock get path answers for fields that
            // are not stored.
            Map<String, Object> storedOnly = getDoc(indexName, "alpha-3?stored_fields=label");
            assertEquals(Boolean.TRUE, storedOnly.get("found"));
            assertFalse(storedOnly.toString(), storedOnly.containsKey("_source"));

            Response head = client().performRequest(new Request("HEAD", "/" + indexName + "/_doc/alpha-0"));
            assertEquals(200, head.getStatusLine().getStatusCode());
            assertEquals(404, getStatus(indexName, "alpha-9"));
            assertEquals("a quote in the id neither matches nor fails", 404, getStatus(indexName, "al'pha"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testMultiGetAnswersLanceItemsNextToStockItemsInRequestOrder() throws Exception {
        String suffix = "mget-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        String tableUri = LanceTableFactory.writeStringPkTable(scratchDir, tableName, 4);
        String indexName = tableName;
        String plain = "plain-" + suffix;
        try {
            attach("{\"table\":\"" + tableUri + "\"}");
            ensureGreen(indexName);
            Request create = new Request("PUT", "/" + plain);
            create.setJsonEntity("{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0}}");
            client().performRequest(create);
            Request doc = new Request("PUT", "/" + plain + "/_doc/p1?refresh=true");
            doc.setJsonEntity("{\"label\":\"plain-row\"}");
            client().performRequest(doc);

            String body = "{\"docs\":["
                + "{\"_index\":\""
                + indexName
                + "\",\"_id\":\"alpha-1\"},"
                + "{\"_index\":\""
                + plain
                + "\",\"_id\":\"p1\"},"
                + "{\"_index\":\""
                + indexName
                + "\",\"_id\":\"alpha-9\"},"
                + "{\"_index\":\""
                + indexName
                + "\",\"_id\":\"alpha-2\",\"_source\":[\"label\"]}"
                + "]}";
            String response = readAll(postJson("/_mget", body));
            List<Map<String, Object>> docs = docsOf(response);
            assertEquals(response, 4, docs.size());

            assertEquals(indexName, docs.get(0).get("_index"));
            assertEquals("alpha-1", docs.get(0).get("_id"));
            assertEquals(Boolean.TRUE, docs.get(0).get("found"));
            assertEquals("col-1", sourceOf(docs.get(0)).get("label"));

            assertEquals(plain, docs.get(1).get("_index"));
            assertEquals("p1", docs.get(1).get("_id"));
            assertEquals("the stock action answered the plain item", Boolean.TRUE, docs.get(1).get("found"));
            assertEquals("plain-row", sourceOf(docs.get(1)).get("label"));

            assertEquals("alpha-9", docs.get(2).get("_id"));
            assertEquals(Boolean.FALSE, docs.get(2).get("found"));

            assertEquals("alpha-2", docs.get(3).get("_id"));
            assertEquals("the item's _source filter is honoured", Map.of("label", "row-2"), sourceOf(docs.get(3)));

            // The index level form names the index once.
            String byIds = readAll(postJson("/" + indexName + "/_mget", "{\"ids\":[\"alpha-0\",\"alpha-3\"]}"));
            List<Map<String, Object>> byIdsDocs = docsOf(byIds);
            assertEquals(2, byIdsDocs.size());
            assertEquals("row-0", sourceOf(byIdsDocs.get(0)).get("label"));
            assertEquals("col-3", sourceOf(byIdsDocs.get(1)).get("label"));

            String stats = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_stats/get")));
            assertEquals(
                "the shard's get service was not entered: " + stats,
                0,
                extractIntPath(stats, "indices", indexName, "total", "get", "total")
            );
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName + "," + plain));
            } catch (Exception ignored) {}
        }
    }

    public void testGetUnderAReaderWrapperAnswersAHiddenRowAsAbsent() throws Exception {
        // The test hook installs a wrapper shaped like the security
        // plugin's document level security reader on the index under the
        // prefix, hiding the rows whose id is below 3. The filter reads
        // the row through the wrapped leaf, so a hidden row is a 404 and
        // a visible one answers; the same table attached outside the
        // prefix answers the hidden row.
        String suffix = "getwrap-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        String tableUri = LanceTableFactory.writeStructTable(scratchDir, tableName, 0);
        String wrapped = "wrapped-" + suffix;
        String plain = "plain-" + suffix;
        setHidingWrapperIndexPrefix(wrapped + ":meta.region:id:3");
        try {
            attach("{\"table\":\"" + tableUri + "\",\"name\":\"" + wrapped + "\"}");
            attach("{\"table\":\"" + tableUri + "\",\"name\":\"" + plain + "\"}");
            ensureGreen(wrapped);
            ensureGreen(plain);

            Map<String, Object> visible = getDoc(wrapped, "4");
            assertEquals(Boolean.TRUE, visible.get("found"));
            assertEquals(4, sourceOf(visible).get("id"));

            ResponseException hidden = expectThrows(
                ResponseException.class,
                () -> client().performRequest(new Request("GET", "/" + wrapped + "/_doc/1"))
            );
            String hiddenBody = readAll(hidden.getResponse());
            assertEquals("a hidden row is a 404: " + hiddenBody, 404, hidden.getResponse().getStatusLine().getStatusCode());
            assertEquals(Boolean.FALSE, parseJson(hiddenBody).get("found"));

            Map<String, Object> unwrapped = getDoc(plain, "1");
            assertEquals(1, sourceOf(unwrapped).get("id"));

            String mget = readAll(postJson("/" + wrapped + "/_mget", "{\"ids\":[\"1\",\"4\"]}"));
            List<Map<String, Object>> docs = docsOf(mget);
            assertEquals(Boolean.FALSE, docs.get(0).get("found"));
            assertEquals(Boolean.TRUE, docs.get(1).get("found"));
        } finally {
            setHidingWrapperIndexPrefix(null);
            for (String index : new String[] { wrapped, plain }) {
                try {
                    client().performRequest(new Request("DELETE", "/" + index));
                } catch (Exception ignored) {}
            }
        }
    }

    public void testTermVectorsAndExplainAreRefusedWith400() throws Exception {
        String suffix = "refuse-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        String tableUri = LanceTableFactory.writeStringPkTable(scratchDir, tableName, 4);
        String indexName = tableName;
        try {
            attach("{\"table\":\"" + tableUri + "\"}");
            ensureGreen(indexName);

            String termVectors = refused(new Request("GET", "/" + indexName + "/_termvectors/alpha-1"));
            assertTrue(termVectors, termVectors.contains("[_termvectors] is not served by a Lance backed index [" + indexName + "]"));

            Request multi = new Request("POST", "/_mtermvectors");
            multi.setJsonEntity("{\"docs\":[{\"_index\":\"" + indexName + "\",\"_id\":\"alpha-1\"}]}");
            String multiTermVectors = refused(multi);
            assertTrue(
                multiTermVectors,
                multiTermVectors.contains("[_mtermvectors] is not served by a Lance backed index [" + indexName + "]")
            );

            Request explain = new Request("GET", "/" + indexName + "/_explain/alpha-1");
            explain.setJsonEntity("{\"query\":{\"match_all\":{}}}");
            String explained = refused(explain);
            assertTrue(explained, explained.contains("[_explain] is not served by a Lance backed index [" + indexName + "]"));
            assertTrue("the plugin's explain is named: " + explained, explained.contains("/_plugins/_lance/explain/" + indexName));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    private static void attach(String payload) throws IOException {
        Response attach = postJson("/_plugins/_lance/attach", payload);
        assertEquals("attach failed: " + readAll(attach), RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
    }

    /** The parsed body of a {@code GET /<index>/_doc/<idAndParams>} that answers 200. */
    private static Map<String, Object> getDoc(String indexName, String idAndParams) throws IOException {
        Response response = client().performRequest(new Request("GET", "/" + indexName + "/_doc/" + idAndParams));
        String body = readAll(response);
        assertEquals(body, 200, response.getStatusLine().getStatusCode());
        return parseJson(body);
    }

    /** The status of a {@code GET /<index>/_doc/<id>}, 404 included. */
    private static int getStatus(String indexName, String id) throws IOException {
        try {
            return client().performRequest(new Request("GET", "/" + indexName + "/_doc/" + id)).getStatusLine().getStatusCode();
        } catch (ResponseException e) {
            return e.getResponse().getStatusLine().getStatusCode();
        }
    }

    /** The body of a request that answers 400. */
    private static String refused(Request request) throws IOException {
        ResponseException failure = expectThrows(ResponseException.class, () -> client().performRequest(request));
        String body = readAll(failure.getResponse());
        assertEquals(body, 400, failure.getResponse().getStatusLine().getStatusCode());
        return body;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> sourceOf(Map<String, Object> doc) {
        return (Map<String, Object>) doc.get("_source");
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> docsOf(String mgetBody) {
        return (List<Map<String, Object>>) parseJson(mgetBody).get("docs");
    }

    private static void setHidingWrapperIndexPrefix(String value) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        String encoded = value == null ? "null" : "\"" + value + "\"";
        request.setJsonEntity("{\"transient\":{\"plugins.lance.test.hiding_wrapper_index_prefix\":" + encoded + "}}");
        assertEquals(RestStatus.OK.getStatus(), client().performRequest(request).getStatusLine().getStatusCode());
    }
}
