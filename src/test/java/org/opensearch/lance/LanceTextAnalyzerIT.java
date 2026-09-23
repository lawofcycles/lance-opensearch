/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;

/**
 * The OpenSearch analyzer mode end to end: a {@code type: text_analyzer}
 * override on the attach body creates and backfills the derived tokens
 * column, the mapping surfaces the base column as {@code lance_text}
 * with {@code tokens_column} and {@code meta.lance_analyzer}, and the
 * FTS queries analyze the query text with the same analyzer before
 * they reach Lance (so an {@code english}-analyzed column matches
 * {@code running} to {@code runs}), while {@code _source} never shows
 * the derived column. Also the {@code whitespace} analyzer's case
 * boundary, the {@code derive: async} attach path (mapping flips after
 * the background backfill), and the 400s of the clause.
 */
public class LanceTextAnalyzerIT extends LanceRestTestCase {

    private String writeTable(String hint) throws Exception {
        String suffix = hint + "-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        return LanceTableFactory.writeEnglishTextTable(scratchDir, "demo-" + suffix);
    }

    private static String indexNameOf(String tableUri) {
        String base = tableUri.substring(tableUri.lastIndexOf('/') + 1);
        return base.substring(0, base.length() - ".lance".length());
    }

    public void testEnglishAnalyzerModeServesStemmedMatches() throws Exception {
        String tableUri = writeTable("analyzer");
        String indexName = indexNameOf(tableUri);
        try {
            Response attach = postJson(
                "/_lance/attach",
                "{\"table\":\"" + tableUri + "\",\"overrides\":{\"body\":{\"type\":\"text_analyzer\",\"analyzer\":\"english\"}}}"
            );
            assertEquals("attach failed: " + readAll(attach), RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // The base column maps as lance_text in the analyzer mode;
            // the derived column stays out of the mapping.
            String mapping = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_mapping")));
            assertTrue("body must map as lance_text: " + mapping, mapping.contains("\"body\":{\"type\":\"lance_text\""));
            assertTrue(
                "tokens_column must point at the derived column: " + mapping,
                mapping.contains("\"tokens_column\":\"body__lance_tokens\"")
            );
            assertTrue("meta must record the analyzer: " + mapping, mapping.contains("\"lance_analyzer\":\"english\""));
            assertFalse("derived column must not surface: " + mapping, mapping.contains("\"body__lance_tokens\":{\"type\""));

            // Rows 0, 1 and 5 stem to `run` (running / runs / run); the
            // query text stems the same way, so `running` finds them.
            // Row 3 holds the irregular past `ran`, which Porter does
            // not fold, and stays out.
            String matchBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":10,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"running\"}}}"
                )
            );
            assertEquals(
                "lance_match running must hit the three run-stem rows: " + matchBody,
                3,
                extractIntPath(matchBody, "hits", "total", "value")
            );

            // The stock match query analyzes through the same path.
            String stockMatch = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"match\":{\"body\":\"running\"}}}")
            );
            assertEquals(
                "stock match running must hit the same rows: " + stockMatch,
                3,
                extractIntPath(stockMatch, "hits", "total", "value")
            );

            // Phrase order over the analyzed tokens: `running quickly`
            // stems to `run quickli`, consecutive only in row 0 (`dogs
            // running` would also hit row 1, whose tokens are `dog run
            // across wide field`).
            String phrase = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":10,\"query\":{\"lance_match_phrase\":{\"field\":\"body\",\"query\":\"running quickly\"}}}"
                )
            );
            assertEquals("phrase must hit row 0 only: " + phrase, 1, extractIntPath(phrase, "hits", "total", "value"));
            assertEquals(0, extractIntPath(phrase, "hits", "hits", "0", "_source", "id"));

            // _source renders the raw column and never the derived one.
            Map<String, Object> hit = hitsOf(phrase).get(0);
            @SuppressWarnings("unchecked")
            Map<String, Object> source = (Map<String, Object>) hit.get("_source");
            assertTrue("_source keeps the raw text: " + source, source.get("body").toString().contains("running"));
            assertFalse("_source must not carry the derived column: " + source, source.containsKey("body__lance_tokens"));

            // Re-attach is idempotent: the derived column and its index
            // already exist.
            Response again = postJson(
                "/_lance/attach",
                "{\"table\":\"" + tableUri + "\",\"overrides\":{\"body\":{\"type\":\"text_analyzer\",\"analyzer\":\"english\"}}}"
            );
            assertEquals(RestStatus.OK.getStatus(), again.getStatusLine().getStatusCode());
            String againBody = readAll(again);
            assertTrue("second attach reports already_attached: " + againBody, againBody.contains("already_attached"));
        } finally {
            deleteIndexQuietly(indexName);
        }
    }

    public void testWhitespaceAnalyzerKeepsCase() throws Exception {
        String tableUri = writeTable("whitespace");
        String indexName = indexNameOf(tableUri);
        try {
            Response attach = postJson(
                "/_lance/attach",
                "{\"table\":\"" + tableUri + "\",\"overrides\":{\"body\":{\"type\":\"text_analyzer\",\"analyzer\":\"whitespace\"}}}"
            );
            assertEquals("attach failed: " + readAll(attach), RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // The whitespace analyzer splits on whitespace only and
            // keeps case, so `Cats` (stored capitalised in row 2)
            // matches and the lowercased form does not.
            String exact = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"Cats\"}}}")
            );
            assertEquals("Cats must hit row 2: " + exact, 1, extractIntPath(exact, "hits", "total", "value"));
            String lowercased = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"cats\"}}}")
            );
            assertEquals("lowercase cats must miss: " + lowercased, 0, extractIntPath(lowercased, "hits", "total", "value"));
        } finally {
            deleteIndexQuietly(indexName);
        }
    }

    public void testAsyncDeriveFlipsMappingAfterBackfill() throws Exception {
        String tableUri = writeTable("async");
        String indexName = indexNameOf(tableUri);
        try {
            Response attach = postJson(
                "/_lance/attach",
                "{\"table\":\""
                    + tableUri
                    + "\",\"derive\":\"async\",\"overrides\":{\"body\":{\"type\":\"text_analyzer\",\"analyzer\":\"english\"}}}"
            );
            String attachBody = readAll(attach);
            assertEquals("attach failed: " + attachBody, RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // The async response describes the backfill it started: the
            // derived column is streamed into the table (no spool), on
            // the node's configured thread count, and the estimate is
            // the sampled value length over the table's rows.
            Map<String, Object> body = parseJson(attachBody);
            @SuppressWarnings("unchecked")
            Map<String, Object> backfill = (Map<String, Object>) body.get("backfill");
            assertNotNull("async attach must report its backfill: " + attachBody, backfill);
            assertEquals("none", backfill.get("spool_path"));
            assertTrue("threads must be at least 1: " + backfill, ((Number) backfill.get("threads")).intValue() >= 1);
            assertTrue("estimated_bytes must be positive: " + backfill, ((Number) backfill.get("estimated_bytes")).longValue() > 0L);

            // The backfill runs in the background; the namespace poll
            // re-derives the mapping when its commit advances the
            // manifest (the keyword to lance_text flip goes through the
            // established index rebuild), after which the analyzer mode
            // serves stemmed matches. Allow generous time for the poll
            // and the rebuild. The rebuild deletes and recreates the
            // index, so a poll that lands in between answers 404, and a
            // search against the fresh index may be refused before its
            // shard is allocated; assertBusy retries only on
            // AssertionError, so such responses are converted into one
            // and count as "not yet".
            assertBusy(() -> {
                String mapping = readAll(performRetrying(new Request("GET", "/" + indexName + "/_mapping")));
                assertTrue(
                    "mapping must flip to the analyzer mode: " + mapping,
                    mapping.contains("\"tokens_column\":\"body__lance_tokens\"")
                );
                Request search = new Request("POST", "/" + indexName + "/_search");
                search.setJsonEntity("{\"size\":10,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"running\"}}}");
                String matchBody = readAll(performRetrying(search));
                assertEquals(
                    "running must hit the three run-stem rows: " + matchBody,
                    3,
                    extractIntPath(matchBody, "hits", "total", "value")
                );
            }, 60, TimeUnit.SECONDS);
        } finally {
            deleteIndexQuietly(indexName);
        }
    }

    public void testUnknownAnalyzerRefused() throws Exception {
        String tableUri = writeTable("unknown");
        ResponseException e = expectThrows(
            ResponseException.class,
            () -> postJson(
                "/_lance/attach",
                "{\"table\":\"" + tableUri + "\",\"overrides\":{\"body\":{\"type\":\"text_analyzer\",\"analyzer\":\"no_such_analyzer\"}}}"
            )
        );
        assertEquals(RestStatus.BAD_REQUEST.getStatus(), e.getResponse().getStatusLine().getStatusCode());
        assertTrue(e.getMessage(), e.getMessage().contains("no_such_analyzer"));
    }

    public void testDeriveWithoutTextAnalyzerOverrideRefused() throws Exception {
        String tableUri = writeTable("derivekey");
        ResponseException e = expectThrows(
            ResponseException.class,
            () -> postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\",\"derive\":\"async\"}")
        );
        assertEquals(RestStatus.BAD_REQUEST.getStatus(), e.getResponse().getStatusLine().getStatusCode());
        assertTrue(e.getMessage(), e.getMessage().contains("[derive] is only accepted"));
    }

    public void testPinnedVersionCannotBackfill() throws Exception {
        String tableUri = writeTable("pinned");
        ResponseException e = expectThrows(
            ResponseException.class,
            () -> postJson(
                "/_lance/attach",
                "{\"table\":\""
                    + tableUri
                    + "\",\"version\":1,\"overrides\":{\"body\":{\"type\":\"text_analyzer\",\"analyzer\":\"english\"}}}"
            )
        );
        assertEquals(RestStatus.BAD_REQUEST.getStatus(), e.getResponse().getStatusLine().getStatusCode());
        assertTrue(e.getMessage(), e.getMessage().contains("pinned"));
    }

    /**
     * Performs {@code request} and reports a non-2xx answer as an
     * {@link AssertionError} so that an enclosing {@code assertBusy}
     * retries it instead of failing on the first {@link ResponseException}.
     */
    private static Response performRetrying(Request request) throws IOException {
        try {
            return client().performRequest(request);
        } catch (ResponseException e) {
            throw new AssertionError("index temporarily unavailable during the rebuild: " + e.getMessage(), e);
        }
    }

    private static void deleteIndexQuietly(String indexName) {
        try {
            client().performRequest(new Request("DELETE", "/" + indexName));
        } catch (Exception ignored) {
            // The test already failed or the index never surfaced.
        }
    }
}
