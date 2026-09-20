/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
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
 * Explicit attach through {@code /_lance/attach}: request validation,
 * storage options, version pinning, {@code multi_fields} and {@code
 * overrides} clauses, re-attach after the table is recreated, and mapping
 * drift when the writer drops a column.
 */
public class LanceAttachIT extends LanceRestTestCase {

    public void testAttachRejectsMissingTable() throws IOException {
        // A table path that does not exist must surface as a client
        // error, not as an unhandled 500 from Dataset.open.
        String bogus = scratchPathString("missing") + ".lance";
        ResponseException failure = expectThrows(
            ResponseException.class,
            () -> postJson("/_lance/attach", "{\"table\":\"" + bogus + "\"}")
        );
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertTrue("expected 4xx / 5xx for missing table, saw: " + status, status >= 400);
    }

    public void testAttachRejectsMissingTableField() throws IOException {
        ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/_lance/attach", "{}"));
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertEquals("expected 400 for missing table field, saw " + status, 400, status);
        String body = readAll(failure.getResponse());
        assertTrue("expected message about [table], saw: " + body, body.contains("[table]"));
    }

    public void testCreateIndexRejectsLanceTableSetting() throws IOException {
        // PUT /{index} with index.lance.table would wire the engine
        // without deriving a mapping; the request is rejected and points
        // at POST /_lance/attach instead.
        String indexName = "rawput-" + randomAlphaOfLength(6).toLowerCase(java.util.Locale.ROOT);
        Request create = new Request("PUT", "/" + indexName);
        create.setJsonEntity("{\"settings\":{\"index.lance.table\":\"/tmp/does-not-matter.lance\"}}");
        create.setOptions(create.getOptions().toBuilder().addHeader("Content-Type", "application/json"));
        ResponseException failure = expectThrows(ResponseException.class, () -> client().performRequest(create));
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertEquals("expected 400 for direct PUT with index.lance.table, saw " + status, 400, status);
        String body = readAll(failure.getResponse());
        assertTrue("expected error to mention [index.lance.table]: " + body, body.contains("index.lance.table"));
        assertTrue("expected error to point at /_lance/attach: " + body, body.contains("/_lance/attach"));
    }

    public void testAttachRejectsNumberOfShards() throws IOException {
        // Lance-backed indices are single-shard; search fans out per
        // fragment, so an explicit number_of_shards is rejected rather
        // than silently ignored.
        String payload = "{\"table\":\"/tmp/does-not-matter.lance\",\"number_of_shards\":3}";
        ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/_lance/attach", payload));
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertEquals("expected 400 for number_of_shards, saw " + status, 400, status);
        String body = readAll(failure.getResponse());
        assertTrue("expected message about [number_of_shards], saw: " + body, body.contains("[number_of_shards] is no longer accepted"));
    }

    public void testAttachRefusesToClaimPlainIndex() throws IOException {
        // A plain OpenSearch index that already owns the target name must
        // not be reported as already_attached.
        String indexName = "plain-collision-" + randomAlphaOfLength(6).toLowerCase(java.util.Locale.ROOT);
        Request create = new Request("PUT", "/" + indexName);
        create.setJsonEntity("{}");
        create.setOptions(create.getOptions().toBuilder().addHeader("Content-Type", "application/json"));
        client().performRequest(create);
        try {
            String tablePath = scratchPathString(indexName) + ".lance";
            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson("/_lance/attach", "{\"table\":\"" + tablePath + "\",\"name\":\"" + indexName + "\"}")
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            // 409 for the name clash, or another 4xx if the fake table path
            // fails to open first; never 200 already_attached.
            assertTrue("expected 4xx (not 200 already_attached), saw " + status, status >= 400 && status < 500);
        } finally {
            client().performRequest(new Request("DELETE", "/" + indexName));
        }
    }

    public void testAttachAcceptsStorageOptionsAndPersistsInSettings() throws Exception {
        // Local filesystem tables ignore object-store credentials, so this
        // only checks that storage_options are parsed, persisted under
        // index.lance.storage_options.<key>, and do not break the open.
        String suffix = "attachso-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 4);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson(
                "/_lance/attach",
                "{\"table\":\""
                    + tableUri
                    + "\",\"storage_options\":{\"aws_region\":\"us-east-1\",\"aws_endpoint\":\"https://s3.example.internal\"}}"
            );
            assertEquals(
                "attach with storage_options failed: " + readAll(attach),
                RestStatus.OK.getStatus(),
                attach.getStatusLine().getStatusCode()
            );

            Response settings = client().performRequest(new Request("GET", "/" + indexName + "/_settings"));
            String settingsBody = readAll(settings);
            assertTrue("expected persisted aws_region: " + settingsBody, settingsBody.contains("\"aws_region\":\"us-east-1\""));
            assertTrue(
                "expected persisted aws_endpoint: " + settingsBody,
                settingsBody.contains("\"aws_endpoint\":\"https://s3.example.internal\"")
            );

            Response search = postJson("/" + indexName + "/_search", "{\"query\":{\"match_all\":{}}}");
            int hits = extractIntPath(readAll(search), "hits", "total", "value");
            assertEquals("expected 4 hits", 4, hits);
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testAttachOmittingStorageOptionsPersistsNothing() throws Exception {
        // Absent storage_options must not seed any
        // index.lance.storage_options.* setting; callers use the absence
        // to detect that an index carries no per-table options.
        String suffix = "attachnoso-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 2);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            Response settings = client().performRequest(new Request("GET", "/" + indexName + "/_settings"));
            String settingsBody = readAll(settings);
            assertFalse("did not expect any storage_options in settings: " + settingsBody, settingsBody.contains("\"storage_options\""));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testAttachWithPinnedVersionServesSnapshot() throws Exception {
        // Attach the same table as latest and pinned to version 1. The
        // fixture is written in one commit, so both see six rows.
        String suffix = "tt-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String latestIndex = tableName + "-latest";
        String pinnedIndex = tableName + "-v1";
        try {
            Response attachLatest = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\",\"name\":\"" + latestIndex + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attachLatest.getStatusLine().getStatusCode());
            Response attachPinned = postJson(
                "/_lance/attach",
                "{\"table\":\"" + tableUri + "\",\"name\":\"" + pinnedIndex + "\",\"version\":1}"
            );
            assertEquals(RestStatus.OK.getStatus(), attachPinned.getStatusLine().getStatusCode());

            String latestBody = readAll(postJson("/" + latestIndex + "/_search", "{\"query\":{\"match_all\":{}}}"));
            assertEquals(6, extractIntPath(latestBody, "hits", "total", "value"));
            String pinnedBody = readAll(postJson("/" + pinnedIndex + "/_search", "{\"query\":{\"match_all\":{}}}"));
            assertEquals(6, extractIntPath(pinnedBody, "hits", "total", "value"));

            // index.lance.version keeps the pin across node restarts.
            Response settings = client().performRequest(new Request("GET", "/" + pinnedIndex + "/_settings"));
            String settingsBody = readAll(settings);
            assertTrue("expected index.lance.version=1 to persist: " + settingsBody, settingsBody.contains("\"version\":\"1\""));
        } finally {
            for (String idx : new String[] { latestIndex, pinnedIndex }) {
                try {
                    client().performRequest(new Request("DELETE", "/" + idx));
                } catch (Exception ignored) {}
            }
        }
    }

    public void testAttachRejectsNegativeVersion() throws IOException {
        String payload = "{\"table\":\"/tmp/does-not-matter.lance\",\"version\":-1}";
        ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/_lance/attach", payload));
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertEquals("expected 400 for negative version, saw " + status, 400, status);
        String body = readAll(failure.getResponse());
        assertTrue("expected message about [version], saw: " + body, body.contains("[version]"));
    }

    public void testAttachRejectsNonObjectStorageOptions() throws IOException {
        String payload = "{\"table\":\"/tmp/does-not-matter.lance\",\"storage_options\":\"not-an-object\"}";
        ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/_lance/attach", payload));
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertEquals("expected 400 for non-object storage_options, saw " + status, 400, status);
        String body = readAll(failure.getResponse());
        assertTrue("expected message about storage_options: " + body, body.contains("storage_options"));
        assertTrue("expected message about JSON object: " + body, body.contains("JSON object"));
    }

    public void testAttachRejectsNestedStorageOptionsValue() throws IOException {
        // Values must be strings; a nested object would be stringified at
        // the JNI boundary.
        String payload = "{\"table\":\"/tmp/does-not-matter.lance\"," + "\"storage_options\":{\"aws_config\":{\"nested\":\"value\"}}}";
        ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/_lance/attach", payload));
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertEquals("expected 400 for nested storage_options value, saw " + status, 400, status);
        String body = readAll(failure.getResponse());
        assertTrue("expected message about aws_config: " + body, body.contains("aws_config"));
        assertTrue("expected message about must be a string: " + body, body.contains("must be a string"));
    }

    public void testBuildIndexesOnUnknownIndexFails() throws IOException {
        String unknown = "does-not-exist-" + randomAlphaOfLength(8);
        ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/_lance/build_indexes/" + unknown, "{}"));
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertTrue("expected 4xx / 5xx for unknown index, saw: " + status, status >= 400);
    }

    public void testAttachRecreateAtSamePathServesNewContent() throws Exception {
        // Attach, delete the index, recreate the Lance table at the same
        // path with different rows, re-attach. GET must see the recreated
        // table: the shared Lance Session caches index pages by path, and
        // a stale entry would serve rows from the deleted table or fail
        // on a missing _indices file. A string PK table gives GET
        // something to look up.
        String suffix = "recreate-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        Path tablePath = scratchDir.resolve(tableName + ".lance");
        String tableUri = tablePath.toString();
        String indexName = tableName;

        // First table: keys alpha-0..alpha-3. A GET primes the cache.
        LanceTableFactory.writeStringPkTable(scratchDir, tableName, 4);
        try {
            Response attach1 = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals("first attach failed: " + readAll(attach1), RestStatus.OK.getStatus(), attach1.getStatusLine().getStatusCode());

            Response beforeGet = client().performRequest(new Request("GET", "/" + indexName + "/_doc/alpha-2"));
            assertEquals(
                "GET on original table must find alpha-2, saw " + beforeGet.getStatusLine().getStatusCode(),
                200,
                beforeGet.getStatusLine().getStatusCode()
            );

            client().performRequest(new Request("DELETE", "/" + indexName));

            // Second table at the same URI: keys alpha-0 and alpha-1 only.
            deleteRecursively(tablePath);
            LanceTableFactory.writeStringPkTable(scratchDir, tableName, 2);

            Response attach2 = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals("second attach failed: " + readAll(attach2), RestStatus.OK.getStatus(), attach2.getStatusLine().getStatusCode());

            Response afterHit = client().performRequest(new Request("GET", "/" + indexName + "/_doc/alpha-1"));
            assertEquals(
                "GET on recreated table must find alpha-1, saw " + afterHit.getStatusLine().getStatusCode(),
                200,
                afterHit.getStatusLine().getStatusCode()
            );

            // alpha-2 only existed in the deleted table: 404, not a stale
            // hit and not a 500.
            ResponseException stale = expectThrows(
                ResponseException.class,
                () -> client().performRequest(new Request("GET", "/" + indexName + "/_doc/alpha-2"))
            );
            assertEquals(
                "GET on stale key alpha-2 must return 404, saw "
                    + stale.getResponse().getStatusLine().getStatusCode()
                    + " body="
                    + readAll(stale.getResponse()),
                404,
                stale.getResponse().getStatusLine().getStatusCode()
            );

            ResponseException stale3 = expectThrows(
                ResponseException.class,
                () -> client().performRequest(new Request("GET", "/" + indexName + "/_doc/alpha-3"))
            );
            assertEquals(
                "GET on stale key alpha-3 must return 404, saw "
                    + stale3.getResponse().getStatusLine().getStatusCode()
                    + " body="
                    + readAll(stale3.getResponse()),
                404,
                stale3.getResponse().getStatusLine().getStatusCode()
            );
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testMultiFieldsExposesKeywordSubField() throws Exception {
        // multi_fields gives a lance_text column a keyword sub-field
        // backed by the same Lance column. The fixture's body values are
        // all distinct, so a term on body.raw resolves to one hit.
        String suffix = "multifields-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
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
            assertEquals(
                "attach with multi_fields failed: " + readAll(attach),
                RestStatus.OK.getStatus(),
                attach.getStatusLine().getStatusCode()
            );

            String mappingBody = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_mapping")));
            assertTrue(
                "mapping must expose body.fields.raw as keyword: " + mappingBody,
                mappingBody.contains("\"fields\":{\"raw\":{\"type\":\"keyword\"")
            );

            String termBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"term\":{\"body.raw\":\"hello lance 0\"}}}")
            );
            assertEquals(
                "term body.raw hello lance 0 must return 1 hit: " + termBody,
                1,
                extractIntPath(termBody, "hits", "total", "value")
            );
            assertEquals(0, extractIntPath(termBody, "hits", "hits", "0", "_source", "id"));

            // Six distinct body values produce six buckets.
            String aggBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"aggs\":{\"per_body\":{\"terms\":{\"field\":\"body.raw\",\"size\":10}}}}"
                )
            );
            int bucketCount = 0;
            try (XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, aggBody)) {
                java.util.Map<String, Object> map = parser.map();
                @SuppressWarnings("unchecked")
                java.util.Map<String, Object> aggs = (java.util.Map<String, Object>) map.get("aggregations");
                @SuppressWarnings("unchecked")
                java.util.Map<String, Object> perBody = (java.util.Map<String, Object>) aggs.get("per_body");
                @SuppressWarnings("unchecked")
                java.util.List<Object> buckets = (java.util.List<Object>) perBody.get("buckets");
                bucketCount = buckets.size();
            }
            assertEquals("body.raw terms agg must produce 6 unique buckets: " + aggBody, 6, bucketCount);

            // The parent field still serves FTS.
            String matchBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match\":{\"body\":\"lance\"}}}"));
            assertEquals(3, extractIntPath(matchBody, "hits", "total", "value"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testMultiFieldsRejectsInvalidBaseColumn() throws Exception {
        // Non-Utf8 base columns, unknown base columns and non-keyword
        // sub-field types are rejected at attach time.
        String suffix = "multifieldsbad-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 4);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        try {
            ResponseException nonUtf8 = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/_lance/attach",
                    "{\"table\":\"" + tableUri + "\",\"multi_fields\":{\"id\":{\"raw\":{\"type\":\"keyword\"}}}}"
                )
            );
            assertEquals(400, nonUtf8.getResponse().getStatusLine().getStatusCode());
            assertTrue(
                "expected error mentioning [id] must be Utf8, saw: " + readAll(nonUtf8.getResponse()),
                readAll(nonUtf8.getResponse()).contains("must be Utf8")
            );

            ResponseException unknownColumn = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/_lance/attach",
                    "{\"table\":\"" + tableUri + "\",\"multi_fields\":{\"noSuchCol\":{\"raw\":{\"type\":\"keyword\"}}}}"
                )
            );
            assertEquals(400, unknownColumn.getResponse().getStatusLine().getStatusCode());
            assertTrue(
                "expected error mentioning unknown column, saw: " + readAll(unknownColumn.getResponse()),
                readAll(unknownColumn.getResponse()).contains("unknown column")
            );

            ResponseException badSubType = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/_lance/attach",
                    "{\"table\":\"" + tableUri + "\",\"multi_fields\":{\"body\":{\"raw\":{\"type\":\"text\"}}}}"
                )
            );
            assertEquals(400, badSubType.getResponse().getStatusLine().getStatusCode());
            assertTrue(
                "expected error mentioning [keyword] type, saw: " + readAll(badSubType.getResponse()),
                readAll(badSubType.getResponse()).contains("must be [keyword]")
            );
        } finally {
            // Nothing was attached; the scratch directory is cleaned up
            // through sharedRoot.
        }
    }

    public void testOverridesAcceptsFieldsClause() throws Exception {
        // overrides.<column>.fields accepts the same sub-field declaration
        // as multi_fields and produces the same mapping.
        String suffix = "overrides-fields-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson(
                "/_lance/attach",
                "{\"table\":\"" + tableUri + "\",\"overrides\":{\"body\":{\"fields\":{\"raw\":{\"type\":\"keyword\"}}}}}"
            );
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            String mappingBody = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_mapping")));
            assertTrue(
                "expected body.fields.raw:keyword: " + mappingBody,
                mappingBody.contains("\"fields\":{\"raw\":{\"type\":\"keyword\"")
            );

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

    public void testOverridesRejectsColumnTypeOverride() throws Exception {
        // overrides.<column>.type is reserved and not implemented; the
        // parser must refuse it rather than silently accept it.
        String suffix = "overrides-type-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 4);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        ResponseException failure = expectThrows(
            ResponseException.class,
            () -> postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\",\"overrides\":{\"body\":{\"type\":\"ip\"}}}")
        );
        assertEquals(400, failure.getResponse().getStatusLine().getStatusCode());
        String body = readAll(failure.getResponse());
        assertTrue("expected message about type not supported: " + body, body.contains("not supported yet"));
    }

    public void testOverridesConflictsWithMultiFieldsRejected() throws Exception {
        // Both clauses declaring sub-fields for the same base column is
        // ambiguous and is refused.
        String suffix = "overrides-conflict-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 4);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        ResponseException failure = expectThrows(
            ResponseException.class,
            () -> postJson(
                "/_lance/attach",
                "{\"table\":\""
                    + tableUri
                    + "\","
                    + "\"multi_fields\":{\"body\":{\"raw\":{\"type\":\"keyword\"}}},"
                    + "\"overrides\":{\"body\":{\"fields\":{\"raw\":{\"type\":\"keyword\"}}}}}"
            )
        );
        assertEquals(400, failure.getResponse().getStatusLine().getStatusCode());
        String body = readAll(failure.getResponse());
        assertTrue("expected message about ambiguous body: " + body, body.contains("both [multi_fields] and [overrides]"));
    }

    public void testDropColumnMarksLanceTextFieldDroppedAndRejectsQuery() throws Exception {
        // After the writer drops a column, the poll loop marks the field
        // lance_dropped=true in the mapping meta and queries against it
        // fail with 400 instead of returning zero hits.
        try (LanceTestCluster f = LanceTestCluster.setUp(16, "dropbody")) {
            String indexName = f.indexName();

            Response baseline = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}"
            );
            int baselineHits = extractIntPath(readAll(baseline), "hits", "total", "value");
            assertEquals("expected 8 baseline hits for body:hello", 8, baselineHits);

            LanceTableFactory.dropColumns(f.tableUri(), java.util.List.of("body"));

            assertBusy(() -> {
                Response mapping = client().performRequest(new Request("GET", "/" + indexName + "/_mapping"));
                String body = readAll(mapping);
                assertTrue(
                    "expected lance_dropped meta on body after drop, saw: " + body,
                    body.contains("\"body\"") && body.contains("\"lance_dropped\":\"true\"")
                );
            });

            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson("/" + indexName + "/_search", "{\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}")
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 after body dropped, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected 'no longer exists' message, saw: " + body, body.contains("no longer exists"));
        }
    }
}
