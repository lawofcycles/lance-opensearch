/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;

/**
 * Schema drift end to end: a Lance column rename carries the operator's
 * mapping overrides to the new name (mapping, setting, queries), the
 * old name is hidden from the read paths (0 hits, explain refusal
 * naming the rename, {@code renamed_fields} in the stats) while
 * {@code GET _mapping} still lists it with {@code lance_dropped}, and a
 * schema reset (column cast) keeps a compatible override in the setting
 * and drops an incompatible one.
 */
public class LanceRenameIT extends LanceRestTestCase {

    private static final String OVERRIDES_CLAUSE = "\"overrides\":{"
        + "\"ts\":{\"type\":\"date\"},"
        + "\"label\":{\"type\":\"keyword\",\"fields\":{\"raw\":{\"type\":\"keyword\"}}}}";

    public void testRenameCarriesOverridesAndHidesTheOldNames() throws Exception {
        String suffix = "rename-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        String tableUri = LanceTableFactory.writeEpochMillisTable(scratchDir, tableName);
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"," + OVERRIDES_CLAUSE + "}");
            assertEquals("attach failed: " + readAll(attach), RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // Rename both overridden columns. The field ids stay, so the
            // poll detects a rename rather than a drop plus add.
            LanceTableFactory.renameColumn(tableUri, "ts", "event_ts");
            LanceTableFactory.renameColumn(tableUri, "label", "tag");

            // The poll rewrites the setting and re-derives the mapping:
            // the new names arrive with the operator's rules applied.
            assertBusy(() -> {
                String mapping = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_mapping")));
                assertTrue("event_ts must map as date: " + mapping, mapping.contains("\"event_ts\":{\"type\":\"date\""));
                assertTrue("tag must map as keyword: " + mapping, mapping.contains("\"tag\":{\"type\":\"keyword\""));
                assertTrue("tag.raw sub-field must carry over: " + mapping, mapping.contains("\"fields\":{\"raw\":{\"type\":\"keyword\""));
            }, 30, TimeUnit.SECONDS);
            // The overrides setting is a JSON string value, so its keys
            // appear with escaped quotes in the settings response.
            String settings = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_settings")));
            assertTrue("overrides must be keyed by event_ts: " + settings, settings.contains("\\\"event_ts\\\""));
            assertTrue("overrides must be keyed by tag: " + settings, settings.contains("\\\"tag\\\""));
            assertFalse("overrides must not keep the old ts key: " + settings, settings.contains("\\\"ts\\\""));

            // The old names stay in the mapping, marked lance_dropped,
            // because PutMapping cannot remove properties.
            String mapping = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_mapping")));
            assertTrue("old names stay in the mapping marked dropped: " + mapping, mapping.contains("\"lance_dropped\":\"true\""));

            // Queries against the new names serve through the carried
            // overrides.
            String term = readAll(postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"term\":{\"tag\":\"hello lance 0\"}}}"));
            assertEquals("term on tag must hit: " + term, 1, extractIntPath(term, "hits", "total", "value"));
            String sub = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"term\":{\"tag.raw\":\"hello lance 0\"}}}")
            );
            assertEquals(1, extractIntPath(sub, "hits", "total", "value"));
            String range = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":10,\"query\":{\"range\":{\"event_ts\":{\"gte\":\"2024-03-01\",\"lt\":\"2024-04-01\"}}}}"
                )
            );
            assertEquals("ISO range on event_ts must hit the March rows: " + range, 2, extractIntPath(range, "hits", "total", "value"));

            // The old names are invisible to the read paths: scalar
            // queries answer 0 hits rather than an error, exists finds
            // nothing.
            String stale = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"term\":{\"label\":\"hello lance 0\"}}}")
            );
            assertEquals("term on the old name must answer 0 hits: " + stale, 0, extractIntPath(stale, "hits", "total", "value"));
            String exists = readAll(postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"exists\":{\"field\":\"label\"}}}"));
            assertEquals("exists on the old name must answer 0 hits: " + exists, 0, extractIntPath(exists, "hits", "total", "value"));
            // The old date name keeps its epoch_millis format, so a
            // numeric bound parses; the doc values behind it are gone,
            // so the range matches nothing.
            long march1 = java.time.Instant.parse("2024-03-01T00:00:00Z").toEpochMilli();
            String staleRange = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"range\":{\"ts\":{\"gte\":" + march1 + "}}}}")
            );
            assertEquals(0, extractIntPath(staleRange, "hits", "total", "value"));

            // The explain endpoint names the rename.
            Request explain = new Request("GET", "/" + indexName + "/_lance/explain");
            explain.setJsonEntity("{\"size\":0,\"aggs\":{\"t\":{\"terms\":{\"field\":\"label\"}}}}");
            ResponseException refused = expectThrows(ResponseException.class, () -> client().performRequest(explain));
            assertEquals(400, refused.getResponse().getStatusLine().getStatusCode());
            String reason = readAll(refused.getResponse());
            assertTrue("explain must name the rename: " + reason, reason.contains("field [label] was renamed to [tag] in the Lance table"));

            // The stats list what to update in clients.
            String stats = readAll(client().performRequest(new Request("GET", "/_lance/stats")));
            assertTrue("stats must report the label rename: " + stats, stats.contains("\"from\":\"label\",\"to\":\"tag\""));
            assertTrue("stats must report the ts rename: " + stats, stats.contains("\"from\":\"ts\",\"to\":\"event_ts\""));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testSchemaResetKeepsCompatibleOverrideAndDropsIncompatibleOne() throws Exception {
        // Lance's alter-columns cast only rewrites within a type family
        // (integer to integer, temporal to temporal, Utf8 to LargeUtf8),
        // so the drift cases use the two producible shapes: a temporal
        // recast that still admits a date override, and a LargeUtf8
        // recast that a keyword override does not admit.
        String suffix = "reset-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        String tableUri = LanceTableFactory.writeDatedTable(scratchDir, tableName);
        String indexName = tableName;
        try {
            Response attach = postJson(
                "/_lance/attach",
                "{\"table\":\"" + tableUri + "\",\"overrides\":{\"ts\":{\"type\":\"date\"},\"category\":{\"type\":\"keyword\"}}}"
            );
            assertEquals("attach failed: " + readAll(attach), RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // Compatible: ts Timestamp(us) -> Date64 still admits the date
            // override. The fixture's instants are midnight UTC, so the
            // truncation to days keeps the same values.
            LanceTableFactory.castColumn(tableUri, "ts", new ArrowType.Date(DateUnit.MILLISECOND));
            // Incompatible: category Utf8 -> LargeUtf8 does not admit a
            // keyword override; the poll drops it from the setting.
            LanceTableFactory.castColumn(tableUri, "category", new ArrowType.LargeUtf8());

            assertBusy(() -> {
                String settings = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_settings")));
                assertTrue("the compatible date override must stay: " + settings, settings.contains("\\\"ts\\\""));
                assertFalse("the incompatible keyword override must leave the setting: " + settings, settings.contains("category"));
                String mapping = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_mapping")));
                assertTrue("ts must stay date: " + mapping, mapping.contains("\"ts\":{\"type\":\"date\""));
                // LargeUtf8 is not surfaced by derivation, so the stale
                // keyword entry is marked dropped.
                assertTrue("category must be marked dropped: " + mapping, mapping.contains("\"lance_dropped\":\"true\""));
            }, 30, TimeUnit.SECONDS);

            // The recast ts keeps answering through the carried override.
            String range = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":10,\"query\":{\"range\":{\"ts\":{\"gte\":\"2024-03-01\",\"lt\":\"2024-04-01\"}}}}"
                )
            );
            assertEquals(
                "ISO range on the recast ts must hit the March rows: " + range,
                2,
                extractIntPath(range, "hits", "total", "value")
            );
            // The dropped column answers 0 hits, not an error.
            String stale = readAll(postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"term\":{\"category\":\"even\"}}}"));
            assertEquals(0, extractIntPath(stale, "hits", "total", "value"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }
}
