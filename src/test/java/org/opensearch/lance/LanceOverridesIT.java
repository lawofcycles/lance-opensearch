/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;

/**
 * The attach body's per-column mapping overrides end to end: a
 * {@code type: date} override on an Int64 epoch-millis column (mapping,
 * ISO range, date_histogram, sort, {@code _source} shape), a
 * {@code type: keyword} override on a Utf8 column that carries a Lance
 * inverted index (term query, terms aggregation, {@code lance_match}
 * refusal), survival of both across a manifest version advance, the
 * legacy {@code multi_fields} clause, and the namespace register's
 * {@code overrides} applied to tables that do and do not carry the
 * named column.
 */
public class LanceOverridesIT extends LanceRestTestCase {

    private static final String OVERRIDES_CLAUSE = "\"overrides\":{"
        + "\"ts\":{\"type\":\"date\"},"
        + "\"label\":{\"type\":\"keyword\",\"fields\":{\"raw\":{\"type\":\"keyword\"}}}}";

    public void testDateAndKeywordOverridesServeQueries() throws Exception {
        String suffix = "overrides-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        String tableUri = LanceTableFactory.writeEpochMillisTable(scratchDir, tableName);
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"," + OVERRIDES_CLAUSE + "}");
            assertEquals("attach failed: " + readAll(attach), RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            assertOverriddenMapping(indexName);

            // The canonical JSON lands in the new setting only.
            String settings = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_settings")));
            assertTrue("expected index.lance.overrides in settings: " + settings, settings.contains("\"overrides\""));
            assertFalse("multi_fields setting must not be written by new attaches: " + settings, settings.contains("multi_fields"));

            // ISO date range on the Int64 column: March holds rows 2 and 3.
            // The coordinator lowers this filter to Lance SQL, so the range
            // also proves the SQL literals stay numeric for a
            // date-overridden integer column.
            String rangeBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":10,\"query\":{\"range\":{\"ts\":{\"gte\":\"2024-03-01\",\"lt\":\"2024-04-01\"}}}}"
                )
            );
            assertEquals(
                "range on ts must return the two March rows: " + rangeBody,
                2,
                extractIntPath(rangeBody, "hits", "total", "value")
            );

            // Numeric epoch-millis bounds resolve the same rows.
            long march1 = java.time.Instant.parse("2024-03-01T00:00:00Z").toEpochMilli();
            long april1 = java.time.Instant.parse("2024-04-01T00:00:00Z").toEpochMilli();
            String numericRangeBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":10,\"query\":{\"range\":{\"ts\":{\"gte\":" + march1 + ",\"lt\":" + april1 + "}}}}"
                )
            );
            assertEquals(2, extractIntPath(numericRangeBody, "hits", "total", "value"));

            // date_histogram buckets by calendar month: five months, the
            // March bucket holds two rows.
            String histogramBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"aggs\":{\"per_month\":{\"date_histogram\":{\"field\":\"ts\",\"calendar_interval\":\"month\"}}}}"
                )
            );
            List<String> buckets = bucketsOf(histogramBody, "per_month");
            assertEquals("expected five month buckets: " + histogramBody, 5, buckets.size());
            long march = java.time.Instant.parse("2024-03-01T00:00:00Z").toEpochMilli();
            assertTrue("March bucket must hold 2 docs: " + buckets, buckets.contains(march + "=2"));

            // Sort by the overridden date column: 2024-05-30 (id 5) leads
            // the descending page.
            String sortBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":6,\"query\":{\"match_all\":{}},\"sort\":[{\"ts\":\"desc\"}]}")
            );
            assertEquals(6, extractIntPath(sortBody, "hits", "total", "value"));
            assertEquals(5, extractIntPath(sortBody, "hits", "hits", "0", "_source", "id"));
            assertEquals(0, extractIntPath(sortBody, "hits", "hits", "5", "_source", "id"));

            // _source renders the raw integer, not a formatted date.
            Map<String, Object> firstHit = hitsOf(sortBody).get(0);
            @SuppressWarnings("unchecked")
            Map<String, Object> source = (Map<String, Object>) firstHit.get("_source");
            assertTrue("_source.ts must be a number: " + source, source.get("ts") instanceof Number);
            assertEquals(java.time.Instant.parse("2024-05-30T00:00:00Z").toEpochMilli(), ((Number) source.get("ts")).longValue());

            // The keyword-overridden column answers exact matches and
            // terms aggregations through doc values.
            String termBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"term\":{\"label\":\"hello lance 0\"}}}")
            );
            assertEquals("term on label must return 1 hit: " + termBody, 1, extractIntPath(termBody, "hits", "total", "value"));
            assertEquals(0, extractIntPath(termBody, "hits", "hits", "0", "_source", "id"));

            String termsAggBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"aggs\":{\"per_label\":{\"terms\":{\"field\":\"label\",\"size\":10}}}}"
                )
            );
            assertEquals("six distinct labels: " + termsAggBody, 6, bucketsOf(termsAggBody, "per_label").size());

            // The declared sub-field resolves alongside the type override.
            String subFieldBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"term\":{\"label.raw\":\"hello lance 0\"}}}")
            );
            assertEquals(1, extractIntPath(subFieldBody, "hits", "total", "value"));

            // lance_match on a keyword field is refused like on any other
            // keyword field; the override took the column off the FTS path.
            ResponseException ftsRefused = expectThrows(
                ResponseException.class,
                () -> postJson("/" + indexName + "/_search", "{\"query\":{\"lance_match\":{\"field\":\"label\",\"query\":\"hello\"}}}")
            );
            assertEquals(400, ftsRefused.getResponse().getStatusLine().getStatusCode());

            // A manifest version advance re-derives the mapping with the
            // stored overrides: the appended rows become visible and the
            // overridden types survive.
            LanceTableFactory.appendEpochMillisRows(tableUri, 6, 2);
            assertBusy(() -> {
                String count = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_count")));
                assertEquals("appended rows must become visible: " + count, 8, extractIntPath(count, "count"));
            });
            assertOverriddenMapping(indexName);
            String juneRange = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"range\":{\"ts\":{\"gte\":\"2024-06-01\"}}}}")
            );
            assertEquals("both appended June rows must match: " + juneRange, 2, extractIntPath(juneRange, "hits", "total", "value"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    private static void assertOverriddenMapping(String indexName) throws Exception {
        String mapping = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_mapping")));
        assertTrue("ts must map as date: " + mapping, mapping.contains("\"ts\":{\"type\":\"date\""));
        assertTrue("ts must carry the epoch_millis format: " + mapping, mapping.contains("\"format\":\"epoch_millis\""));
        assertTrue("ts meta must keep the Arrow type: " + mapping, mapping.contains("\"lance_arrow_type\":\"Int(64, true)\""));
        assertTrue("label must map as keyword: " + mapping, mapping.contains("\"label\":{\"type\":\"keyword\""));
        assertFalse("label must not map as lance_text: " + mapping, mapping.contains("lance_text"));
        assertTrue("label.raw sub-field must persist: " + mapping, mapping.contains("\"fields\":{\"raw\":{\"type\":\"keyword\""));
    }

    public void testLegacyMultiFieldsClauseStillResolvesSubField() throws Exception {
        // The legacy clause folds into overrides at parse time and the
        // sub-field keeps answering term queries. Indexes created by
        // earlier builds persist the spec in index.lance.multi_fields;
        // that fallback is pinned by LanceOverridesTests since a Final
        // setting cannot be injected through REST.
        String suffix = "legacymf-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson(
                "/_lance/attach",
                "{\"table\":\"" + tableUri + "\",\"multi_fields\":{\"body\":{\"raw\":{\"type\":\"keyword\"}}}}"
            );
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            String termBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"term\":{\"body.raw\":\"hello lance 0\"}}}")
            );
            assertEquals(1, extractIntPath(termBody, "hits", "total", "value"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testNamespaceOverridesApplyToTablesThatCarryTheColumn() throws Exception {
        // One override list for the whole namespace: the table with the
        // ts column gets the date mapping, the table without it surfaces
        // untouched.
        String suffix = "nsoverrides-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String withTs = "dated-" + suffix;
        String withoutTs = "plain-" + suffix;
        LanceTableFactory.writeEpochMillisTable(scratchDir, withTs);
        LanceTableFactory.writeTable(scratchDir, withoutTs, 4);
        try {
            Response register = postJson(
                "/_lance/namespace",
                "{\"path\":\"" + scratchDir.toString() + "\",\"overrides\":{\"ts\":{\"type\":\"date\"}}}"
            );
            assertEquals("register failed: " + readAll(register), RestStatus.OK.getStatus(), register.getStatusLine().getStatusCode());

            assertBusy(() -> {
                String cat = readAll(client().performRequest(new Request("GET", "/_cat/indices?format=json")));
                assertTrue("waiting for " + withTs + ": " + cat, cat.contains("\"" + withTs + "\""));
                assertTrue("waiting for " + withoutTs + ": " + cat, cat.contains("\"" + withoutTs + "\""));
            });

            String datedMapping = readAll(client().performRequest(new Request("GET", "/" + withTs + "/_mapping")));
            assertTrue("ts must map as date: " + datedMapping, datedMapping.contains("\"ts\":{\"type\":\"date\""));

            String plainMapping = readAll(client().performRequest(new Request("GET", "/" + withoutTs + "/_mapping")));
            assertTrue("body must stay lance_text: " + plainMapping, plainMapping.contains("\"body\":{\"type\":\"lance_text\""));
            assertFalse("no ts field on the plain table: " + plainMapping, plainMapping.contains("\"ts\""));
        } finally {
            for (String index : List.of(withTs, withoutTs)) {
                try {
                    client().performRequest(new Request("DELETE", "/" + index));
                } catch (Exception ignored) {}
            }
            try {
                deleteJson("/_lance/namespace", "{\"path\":\"" + scratchDir.toString() + "\"}");
            } catch (Exception ignored) {}
        }
    }

    public void testAttachRejectsBadOverrides() throws Exception {
        String suffix = "badoverrides-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        String tableUri = LanceTableFactory.writeEpochMillisTable(scratchDir, tableName);

        // Unknown type value: refused at parse time, naming the accepted set.
        ResponseException badType = expectThrows(
            ResponseException.class,
            () -> postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\",\"overrides\":{\"label\":{\"type\":\"text\"}}}")
        );
        assertEquals(400, badType.getResponse().getStatusLine().getStatusCode());
        assertTrue(readAll(badType.getResponse()).contains("[date], [keyword], [ip], [wildcard], [geo_point]"));

        // Arrow type outside the accepted set: refused at derive time.
        ResponseException dateOnUtf8 = expectThrows(
            ResponseException.class,
            () -> postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\",\"overrides\":{\"label\":{\"type\":\"date\"}}}")
        );
        assertEquals(400, dateOnUtf8.getResponse().getStatusLine().getStatusCode());
        assertTrue(readAll(dateOnUtf8.getResponse()).contains("signed 32 or 64 bit integer"));

        // Unknown column: the operator named this specific table, so a
        // missing column is an error rather than a skip.
        ResponseException unknownColumn = expectThrows(
            ResponseException.class,
            () -> postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\",\"overrides\":{\"nope\":{\"type\":\"date\"}}}")
        );
        assertEquals(400, unknownColumn.getResponse().getStatusLine().getStatusCode());
        assertTrue(readAll(unknownColumn.getResponse()).contains("unknown column"));
    }
}
