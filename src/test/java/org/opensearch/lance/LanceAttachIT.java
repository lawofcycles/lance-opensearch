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
        // POST /_lance/attach with a path that does not exist on disk lets
        // Dataset.open throw. The plugin must surface this as an HTTP error
        // rather than crash the request thread or leak an unhandled 500.
        String bogus = scratchPathString("missing") + ".lance";
        ResponseException failure = expectThrows(
            ResponseException.class,
            () -> postJson("/_lance/attach", "{\"table\":\"" + bogus + "\"}")
        );
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertTrue("expected 4xx / 5xx for missing table, saw: " + status, status >= 400);
    }

    public void testAttachRejectsMissingTableField() throws IOException {
        // The `table` field is required. Attach must return 400 with a
        // useful message rather than a 500 NullPointerException.
        ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/_lance/attach", "{}"));
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertEquals("expected 400 for missing table field, saw " + status, 400, status);
        String body = readAll(failure.getResponse());
        assertTrue("expected message about [table], saw: " + body, body.contains("[table]"));
    }

    public void testCreateIndexRejectsLanceTableSetting() throws IOException {
        // Sending index.lance.table through PUT /{index} used to
        // succeed silently: the engine wired up without the derive
        // step running, so the resulting index had no mapping and
        // typed queries failed with "No mapping found" even
        // though _count returned the Lance metadata count. Reject
        // the request up front and point at POST /_lance/attach
        // so callers land on the entry point that actually
        // derives the mapping. Case 6 of issue #37.
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
        // Attach used to derive a shard count from the row count and accept an
        // explicit `number_of_shards` override. The fragment path is now the
        // only search implementation and fans out per fragment regardless of
        // shard count, so attach rejects the option to avoid silently
        // ignoring it.
        String payload = "{\"table\":\"/tmp/does-not-matter.lance\",\"number_of_shards\":3}";
        ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/_lance/attach", payload));
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertEquals("expected 400 for number_of_shards, saw " + status, 400, status);
        String body = readAll(failure.getResponse());
        assertTrue("expected message about [number_of_shards], saw: " + body, body.contains("[number_of_shards] is no longer accepted"));
    }

    public void testAttachRefusesToClaimPlainIndex() throws IOException {
        // If someone (or a previous run) already created a plain OpenSearch
        // index sharing the name attach would default to, we must not return
        // `already_attached: true` and pretend it is a Lance index. The
        // expected outcome is 409 so the operator picks a different `name`.
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
            // Either 409 (index already exists as non-Lance) or another 4xx
            // when the fake table path fails to open. The critical property
            // is that we do NOT return 200 already_attached, which the old
            // attach path did.
            assertTrue("expected 4xx (not 200 already_attached), saw " + status, status >= 400 && status < 500);
        } finally {
            client().performRequest(new Request("DELETE", "/" + indexName));
        }
    }

    public void testAttachAcceptsStorageOptionsAndPersistsInSettings() throws Exception {
        // Local FS tables ignore Lance's object-store credentials, so the
        // payload here is a syntactic smoke test: the plugin has to parse
        // storage_options, persist every entry under
        // index.lance.storage_options.<key>, and still open the dataset
        // successfully. The values (aws_region / aws_endpoint) don't
        // affect the local scan.
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

            // Sanity: the persisted options do not prevent the engine from
            // serving reads against the local table.
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
        // No storage_options field on the request must not seed any
        // index.lance.storage_options.* entry. Callers depend on this to
        // detect whether a Lance-backed index carries per-table options.
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
        // Attach the same table twice: once without version (latest,
        // registered with the namespace poller) and once with version=1
        // (readonly snapshot, no poller). Both queries must succeed and
        // return the same 6-row match_all count because the table is
        // written in a single commit so version 1 is also the latest.
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

            // The pinned index must record index.lance.version=1 so a
            // node restart or shard reallocation keeps reading the same
            // Lance manifest version.
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
        // Sending storage_options as a string used to slip past parse into
        // Lance and surface as a confusing "map required" native error.
        // Reject at 400 with a message pointing at storage_options.
        String payload = "{\"table\":\"/tmp/does-not-matter.lance\",\"storage_options\":\"not-an-object\"}";
        ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/_lance/attach", payload));
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertEquals("expected 400 for non-object storage_options, saw " + status, 400, status);
        String body = readAll(failure.getResponse());
        assertTrue("expected message about storage_options: " + body, body.contains("storage_options"));
        assertTrue("expected message about JSON object: " + body, body.contains("JSON object"));
    }

    public void testAttachRejectsNestedStorageOptionsValue() throws IOException {
        // Values must be strings; nested objects would silently
        // toString() at the JNI boundary. Reject up front.
        String payload = "{\"table\":\"/tmp/does-not-matter.lance\"," + "\"storage_options\":{\"aws_config\":{\"nested\":\"value\"}}}";
        ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/_lance/attach", payload));
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertEquals("expected 400 for nested storage_options value, saw " + status, 400, status);
        String body = readAll(failure.getResponse());
        assertTrue("expected message about aws_config: " + body, body.contains("aws_config"));
        assertTrue("expected message about must be a string: " + body, body.contains("must be a string"));
    }

    public void testBuildIndexesOnUnknownIndexFails() throws IOException {
        // The manual build endpoint targets a specific OpenSearch index. When
        // the index does not exist the call must fail rather than silently
        // no-op, so operators using the recovery path see the mistake.
        String unknown = "does-not-exist-" + randomAlphaOfLength(8);
        ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/_lance/build_indexes/" + unknown, "{}"));
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertTrue("expected 4xx / 5xx for unknown index, saw: " + status, status >= 400);
    }

    public void testAttachRecreateAtSamePathServesNewContent() throws Exception {
        // Issue #46: recreating a Lance table at the same filesystem
        // path left stale index-page entries in the shared Lance
        // Session cache. The re-attach opened its Dataset against
        // the same Session, so GET on the re-attached index either
        // returned rows that only existed in the deleted table or
        // 500'd with "Not found: tables/<path>/_indices/<old-uuid>/page_lookup.lance"
        // depending on which pages the cache still held. The fix
        // hooks a listener onto every Lance-backed IndexModule and
        // reinstalls the shared Session on DELETE, so any subsequent
        // openDataset picks up the fresh manifest.
        //
        // This test walks the whole attach → delete → recreate →
        // re-attach loop the QA report described and checks the
        // observable outcome: GET on the second attach must resolve
        // to the recreated table, not to the deleted one. It is a
        // regression fence for the lifecycle rather than a strict
        // reproducer for the underlying cache pathology, because
        // Lance's cache keying is opaque to Java and the specific
        // manifest-version collision the QA report captured
        // (version 8 on both writes) is not reliably reproducible
        // from an in-JVM writer with fresh small tables. The listener
        // still fires here — the reinstall runs during
        // {@code DELETE /{index}} — so a regression that broke the
        // reinstall path or dropped the listener registration would
        // still change observable behaviour on this test.
        //
        // Table shape: Utf8 PK table so GET has something to look up.
        // {@code writeTable} would not do because its PK metadata
        // clashes with the FixedSizeList column and the fixture
        // decides to leave the PK undeclared (see the comment on
        // {@code LanceTableFactory.writeTable}). {@code writeStringPkTable}
        // sets the PK metadata field-only so GET / _id both resolve.
        String suffix = "recreate-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        Path tablePath = scratchDir.resolve(tableName + ".lance");
        String tableUri = tablePath.toString();
        String indexName = tableName;

        // First attach: 4 rows with keys alpha-0..alpha-3. Prime
        // the Session cache with a GET so the fix has something to
        // invalidate on DELETE.
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

            // Delete the OS index. This is what triggers the
            // IndexEventListener the plugin registers on every
            // Lance-backed IndexModule, which in turn reinstalls
            // the shared Session so its cache does not survive the
            // upcoming path reuse.
            client().performRequest(new Request("DELETE", "/" + indexName));

            // Recreate the Lance table at the exact same URI with a
            // smaller row set: keys alpha-0 and alpha-1 only. The
            // filesystem contents at tablePath are entirely
            // replaced; without the Session reinstall the cache
            // would still hand back pages that reference the
            // deleted _indices/<old-uuid>/ files and GET on alpha-2
            // or alpha-3 would either 500 or resolve to stale rows.
            deleteRecursively(tablePath);
            LanceTableFactory.writeStringPkTable(scratchDir, tableName, 2);

            Response attach2 = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals("second attach failed: " + readAll(attach2), RestStatus.OK.getStatus(), attach2.getStatusLine().getStatusCode());

            // alpha-1 exists in the new table: GET returns 200.
            Response afterHit = client().performRequest(new Request("GET", "/" + indexName + "/_doc/alpha-1"));
            assertEquals(
                "GET on recreated table must find alpha-1, saw " + afterHit.getStatusLine().getStatusCode(),
                200,
                afterHit.getStatusLine().getStatusCode()
            );

            // alpha-2 existed in the old table but not the new one:
            // the response must be 404, not a stale hit and not a
            // 500 caused by the cache pointing at a deleted file.
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

            // Same fence for alpha-3.
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
        // Issue #8: attach body accepts a multi_fields clause so an Utf8
        // FTS column can carry a keyword sub-field for exact-match or
        // aggregation without duplicating source. The primary field stays
        // lance_text (Lance FTS index) and the sub-field gets its own
        // keyword mapping backed by the same underlying Lance column.
        //
        // Uses the standard 6-row fixture where body is
        // "hello lance 0" / "quick brown fox 1" / "hello lance 2" / ...
        // and every value is unique. Row i's body is unique so a term
        // query on body.raw resolves to exactly one hit.
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

            // Mapping must carry the sub-field under fields.raw with
            // type keyword. This confirms derive() emitted the block.
            String mappingBody = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_mapping")));
            assertTrue(
                "mapping must expose body.fields.raw as keyword: " + mappingBody,
                mappingBody.contains("\"fields\":{\"raw\":{\"type\":\"keyword\"")
            );

            // Term query on body.raw must return exactly one hit for a
            // known body value. Previously the sub-field did not exist
            // in FieldInfos so the query resolved to zero hits or 400.
            String termBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"term\":{\"body.raw\":\"hello lance 0\"}}}")
            );
            assertEquals(
                "term body.raw hello lance 0 must return 1 hit: " + termBody,
                1,
                extractIntPath(termBody, "hits", "total", "value")
            );
            assertEquals(0, extractIntPath(termBody, "hits", "hits", "0", "_source", "id"));

            // Aggregation over body.raw produces 6 buckets (one per row)
            // because every body value is unique. This exercises the
            // SortedSetDocValues path on the sub-field.
            String aggBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"aggs\":{\"per_body\":{\"terms\":{\"field\":\"body.raw\",\"size\":10}}}}"
                )
            );
            // Buckets count varies with terms aggregation ordering; assert
            // the total unique bucket count via bucket array length in the
            // response body. Six distinct body values means at least six
            // hello / quick lines in the JSON.
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

            // Primary field body still resolves as lance_text: a match
            // query returns hits for the tokens "hello lance" occurring
            // on rows 0/2/4. The sub-field must not disturb the parent
            // field's FTS path.
            String matchBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match\":{\"body\":\"lance\"}}}"));
            assertEquals(3, extractIntPath(matchBody, "hits", "total", "value"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testMultiFieldsRejectsInvalidBaseColumn() throws Exception {
        // Non-Utf8 base column (integer id) with a keyword sub-field is
        // rejected at attach time so the operator gets a 400 rather than
        // an index that silently fails to serve body.raw queries. Same
        // for unknown base columns and non-keyword sub-field types.
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
            // No successful attach here so no index cleanup required, but
            // the scratch dir cleanup will happen through sharedRoot.
        }
    }

    public void testOverridesAcceptsFieldsClause() throws Exception {
        // Issue #2: `overrides` on the attach body accepts the same
        // sub-field declaration `multi_fields` accepts, and produces the
        // same mapping / doc value shape. This exercises the forward
        // path so future callers can drop `multi_fields` and use
        // `overrides` exclusively.
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

            // term query on body.raw should resolve through the sub-field
            // doc values exactly as it does with the multi_fields clause.
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
        // The `type` field on an overrides entry is the reservation
        // point for future work (#6 ip / wildcard, #7 analyzer mode,
        // #11 preferred index type). Today it is not implemented, so
        // the parser must refuse rather than silently accept and
        // return 400 with a message that points at the reserved
        // shape.
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
        // Both clauses declaring sub-fields for the same base column
        // is ambiguous. Refuse rather than pick a rule the operator
        // did not know about.
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
        // Drop the body column on the Lance side. The polling loop must
        // detect the dropped Lance field id, add lance_dropped=true to
        // the mapping meta, and cause subsequent lance_match queries
        // against `body` to fail with 400 instead of silently returning
        // zero hits.
        //
        // Note: LanceTableFactory.writeTable occasionally fails with
        // "The FixedSizeList type requires an integer parameter" on
        // Lance 11.0.0 when the FFI schema handoff races the first
        // native table creation of a JVM under certain random seeds
        // (observed at seed 2519BC84C706C3F8 and 3CFD7BCD1F5069CD).
        // Retry the setUp once to swallow that flake; a persistent
        // failure still surfaces on the second attempt.
        LanceTestCluster fixture;
        try {
            fixture = LanceTestCluster.setUp(16, "dropbody");
        } catch (RuntimeException e) {
            if (e.getMessage() != null && e.getMessage().contains("FixedSizeList type requires an integer parameter")) {
                fixture = LanceTestCluster.setUp(16, "dropbody");
            } else {
                throw e;
            }
        }
        try (LanceTestCluster f = fixture) {
            String indexName = f.indexName();

            Response baseline = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}"
            );
            int baselineHits = extractIntPath(readAll(baseline), "hits", "total", "value");
            assertEquals("expected 8 baseline hits for body:hello", 8, baselineHits);

            LanceTableFactory.dropColumns(f.tableUri(), java.util.List.of("body"));

            // Wait for the poll to detect the version bump and PutMapping
            // lance_dropped=true. Poll cadence is 1s in build.gradle; the
            // mapping update fires on the next syncTable that sees the
            // dropped id.
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
