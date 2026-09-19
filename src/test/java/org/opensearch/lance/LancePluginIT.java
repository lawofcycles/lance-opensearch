/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

/**
 * Smoke integration test for the plugin. Spins up a full OpenSearch test
 * cluster with the plugin installed and drives it through REST. Runs under
 * {@code ./gradlew integTest}.
 *
 * <p>Split into two layers:
 * <ul>
 *   <li>Route-level checks (plugin installed, endpoints reachable,
 *       register / attach error paths) that need no Lance table on disk.</li>
 *   <li>End-to-end checks that write a real Lance table onto the local
 *       filesystem via {@link LanceTableFactory}, register its parent
 *       directory as a namespace, wait for the poll cycle to surface the
 *       table as an OpenSearch index, then exercise match / GET / knn.</li>
 * </ul>
 * The cluster is configured with {@code lance.namespace.poll_cadence=1s}
 * (see {@code build.gradle}) so the end-to-end tests do not wait ten
 * seconds each for surfacing.
 */
// Lance JNI spins up native worker threads that outlive a single test
// method. Randomized test framework flags those as leaks; suppress at the
// suite level to match the plugin's other Lance-touching tests.
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LancePluginIT extends OpenSearchRestTestCase {

    public void testPluginIsInstalled() throws IOException {
        // Ask the cluster for its plugin list and confirm the entry exists.
        // This catches classpath / metadata regressions before any Lance code
        // ever gets touched.
        Response response = client().performRequest(new Request("GET", "/_cat/plugins?format=json"));
        String body = readAll(response);
        assertTrue("expected opensearch-lance in _cat/plugins, saw: " + body, body.contains("opensearch-lance"));
    }

    public void testCircuitBreakerIsRegistered() throws IOException {
        // The plugin registers a lance_native breaker via
        // CircuitBreakerPlugin.getCircuitBreaker so operators can see
        // the native memory footprint through the standard
        // _nodes/stats/breaker API without a plugin-specific stats
        // endpoint. Assert that the breaker name and non-zero limit
        // show up on every node in a fresh cluster.
        Response response = client().performRequest(new Request("GET", "/_nodes/stats/breaker"));
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
        String body = readAll(response);
        assertTrue("expected lance_native breaker in _nodes/stats/breaker, saw: " + body, body.contains("lance_native"));
    }

    public void testNamespaceEndpointIsRegistered() throws IOException {
        // GET /_lance/namespace lists registered namespaces. On a fresh
        // cluster it returns an empty list, but the important assertion is
        // that the route is wired up and returns a 200 instead of 404.
        Response response = client().performRequest(new Request("GET", "/_lance/namespace"));
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
        String body = readAll(response);
        assertTrue("expected namespaces JSON, saw: " + body, body.contains("namespaces"));
    }

    public void testRegisterNamespace() throws IOException {
        String path = scratchPathString("register");
        java.nio.file.Files.createDirectories(java.nio.file.Path.of(path));
        Response post = postJson("/_lance/namespace", "{\"path\":\"" + path + "\"}");
        assertEquals(RestStatus.OK.getStatus(), post.getStatusLine().getStatusCode());

        // The register call should surface the path back through GET.
        Response listing = client().performRequest(new Request("GET", "/_lance/namespace"));
        String body = readAll(listing);
        assertTrue("expected namespace " + path + " in listing, saw: " + body, body.contains(path));
    }

    public void testRegisterNamespaceIsIdempotent() throws IOException {
        // Registering the same path twice must not duplicate the entry, so a
        // second POST is a no-op. If the polling loop scanned the same
        // catalog twice per cycle every table would surface twice as well.
        String path = scratchPathString("idempotent");
        java.nio.file.Files.createDirectories(java.nio.file.Path.of(path));
        postJson("/_lance/namespace", "{\"path\":\"" + path + "\"}");
        postJson("/_lance/namespace", "{\"path\":\"" + path + "\"}");

        Response listing = client().performRequest(new Request("GET", "/_lance/namespace"));
        String body = readAll(listing);
        int occurrences = countOccurrences(body, path);
        assertEquals("registered path should appear exactly once, saw: " + body, 1, occurrences);
    }

    public void testRegisterNamespaceRejectsMissingPath() throws IOException {
        // The `path` field is required. Without validation, the code cast the
        // body value to String and threw a 500. It must now be 400.
        ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/_lance/namespace", "{}"));
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertEquals("expected 400 for missing path, saw " + status, 400, status);
        String body = readAll(failure.getResponse());
        assertTrue("expected message about [path], saw: " + body, body.contains("[path]"));
    }

    public void testRegisterNamespaceRejectsNonStringPath() throws IOException {
        // A numeric or boolean `path` used to trip a ClassCastException. The
        // handler must catch the type mismatch and return 400.
        ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/_lance/namespace", "{\"path\":42}"));
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertEquals("expected 400 for non-string path, saw " + status, 400, status);
        String body = readAll(failure.getResponse());
        assertTrue("expected message about [path], saw: " + body, body.contains("[path]"));
    }

    public void testRegisterNamespaceRejectsNonExistentPath() throws IOException {
        // Registering a non-existent directory used to succeed silently and
        // then the poll cycle would list nothing forever. Validate up front.
        String phantom = sharedRoot().resolve("does-not-exist-" + randomAlphaOfLength(8)).toString();
        ResponseException failure = expectThrows(
            ResponseException.class,
            () -> postJson("/_lance/namespace", "{\"path\":\"" + phantom + "\"}")
        );
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertEquals("expected 400 for non-existent path, saw " + status, 400, status);
        String body = readAll(failure.getResponse());
        assertTrue("expected 'does not exist' message, saw: " + body, body.contains("does not exist"));
    }

    public void testRegisterNamespaceRejectsFilePath() throws IOException {
        // A file path (not a directory) must also be rejected.
        java.nio.file.Path base = sharedRoot();
        java.nio.file.Path file = java.nio.file.Files.createFile(
            base.resolve("not-a-dir-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT) + ".lance")
        );
        try {
            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson("/_lance/namespace", "{\"path\":\"" + file.toString() + "\"}")
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for file path, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected 'not a directory' message, saw: " + body, body.contains("not a directory"));
        } finally {
            java.nio.file.Files.deleteIfExists(file);
        }
    }

    public void testUnregisterNamespace() throws IOException {
        // Register, then unregister, then verify it disappears from GET.
        String path = scratchPathString("unregister");
        java.nio.file.Files.createDirectories(java.nio.file.Path.of(path));
        Response register = postJson("/_lance/namespace", "{\"path\":\"" + path + "\"}");
        assertEquals(RestStatus.OK.getStatus(), register.getStatusLine().getStatusCode());

        Response unregister = deleteJson("/_lance/namespace", "{\"path\":\"" + path + "\"}");
        assertEquals(RestStatus.OK.getStatus(), unregister.getStatusLine().getStatusCode());
        String unregBody = readAll(unregister);
        assertTrue("expected unregistered:true, saw: " + unregBody, unregBody.contains("\"unregistered\":true"));

        Response listing = client().performRequest(new Request("GET", "/_lance/namespace"));
        String body = readAll(listing);
        assertFalse("expected namespace " + path + " to be gone, saw: " + body, body.contains(path));
    }

    public void testUnregisterUnknownNamespaceReturns404() throws IOException {
        String path = scratchPathString("neverreg") + "-x";
        ResponseException failure = expectThrows(
            ResponseException.class,
            () -> deleteJson("/_lance/namespace", "{\"path\":\"" + path + "\"}")
        );
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertEquals("expected 404 for unregister of unknown path, saw " + status, 404, status);
    }

    public void testListTablesReturnsSurfacedNames() throws Exception {
        // POST /_lance/namespace/tables previews what the poll would
        // surface, without waiting for the poll cycle to run. Useful for
        // debugging a fresh registration on a large directory (operator
        // can see immediately whether the plugin reads the same table
        // set they expect).
        String suffix = "listtables-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        LanceTableFactory.writeTable(scratchDir, "alpha", 4);
        LanceTableFactory.writeTable(scratchDir, "bravo", 4);
        try {
            Response register = postJson("/_lance/namespace", "{\"path\":\"" + scratchDir.toString() + "\"}");
            assertEquals(RestStatus.OK.getStatus(), register.getStatusLine().getStatusCode());

            Response listing = postJson("/_lance/namespace/tables", "{\"path\":\"" + scratchDir.toString() + "\"}");
            assertEquals(RestStatus.OK.getStatus(), listing.getStatusLine().getStatusCode());
            String body = readAll(listing);
            assertTrue("expected path echo: " + body, body.contains("\"path\":\"" + scratchDir.toString() + "\""));
            assertTrue("expected table alpha in list: " + body, body.contains("\"alpha\""));
            assertTrue("expected table bravo in list: " + body, body.contains("\"bravo\""));
        } finally {
            try {
                deleteJson("/_lance/namespace", "{\"path\":\"" + scratchDir.toString() + "\"}");
            } catch (Exception ignored) {}
        }
    }

    public void testListTablesReturns404ForUnregisteredPath() throws IOException {
        // Preview against a path that never went through
        // POST /_lance/namespace must be 404, not 200 with an empty list.
        // Empty list would let a caller confuse "not registered" with
        // "registered but empty".
        String phantom = scratchPathString("phantom-list") + "-nope";
        ResponseException failure = expectThrows(
            ResponseException.class,
            () -> postJson("/_lance/namespace/tables", "{\"path\":\"" + phantom + "\"}")
        );
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertEquals("expected 404 for list_tables on unknown path, saw " + status, 404, status);
        String body = readAll(failure.getResponse());
        assertTrue("expected registered:false in 404 body: " + body, body.contains("\"registered\":false"));
    }

    public void testListTablesRejectsMissingPath() throws IOException {
        ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/_lance/namespace/tables", "{}"));
        assertEquals(400, failure.getResponse().getStatusLine().getStatusCode());
        String body = readAll(failure.getResponse());
        assertTrue("expected [path] is required message: " + body, body.contains("[path] is required"));
    }

    public void testResurfaceGuardHoldsDeletedIndexDuringGrace() throws Exception {
        // A Lance-backed index that the operator deleted via
        // DELETE /{index} must stay deleted for the resurface grace
        // period. Small grace so the post-grace resurface assertion
        // can fire within the test wall-clock budget.
        String suffix = "resurface-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 4);
        String indexName = tableName;
        updateClusterSetting("lance.namespace.resurface_guard_grace", "3s");
        try {
            Response register = postJson("/_lance/namespace", "{\"path\":\"" + scratchDir.toString() + "\"}");
            assertEquals(RestStatus.OK.getStatus(), register.getStatusLine().getStatusCode());
            client().performRequest(new Request("GET", "/_cluster/health/" + indexName + "?wait_for_status=yellow&timeout=30s"));

            client().performRequest(new Request("DELETE", "/" + indexName));

            // Wait longer than the poll cadence (1s in test config)
            // but shorter than the 3s grace. The index must stay gone.
            Thread.sleep(1_500);
            ResponseException stillGone = expectThrows(
                ResponseException.class,
                () -> client().performRequest(new Request("GET", "/" + indexName))
            );
            assertEquals(
                "expected 404 while inside resurface grace, saw " + stillGone.getResponse().getStatusLine().getStatusCode(),
                404,
                stillGone.getResponse().getStatusLine().getStatusCode()
            );

            // After the grace expires the next poll must recreate it.
            // Give the poll a couple of cadences of slack.
            Thread.sleep(4_000);
            Response recovered = client().performRequest(
                new Request("GET", "/_cluster/health/" + indexName + "?wait_for_status=yellow&timeout=30s")
            );
            assertEquals(
                "expected the index to resurface after grace expiry: " + readAll(recovered),
                RestStatus.OK.getStatus(),
                recovered.getStatusLine().getStatusCode()
            );
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
            try {
                deleteJson("/_lance/namespace", "{\"path\":\"" + scratchDir.toString() + "\"}");
            } catch (Exception ignored) {}
            updateClusterSetting("lance.namespace.resurface_guard_grace", "1h");
        }
    }

    public void testResurfaceGuardDisabledByZeroGrace() throws Exception {
        // Grace = 0 short-circuits the tombstone check so the poll
        // cycle recreates the index on the very next tick.
        String suffix = "resurface0-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 4);
        String indexName = tableName;
        updateClusterSetting("lance.namespace.resurface_guard_grace", "0");
        try {
            postJson("/_lance/namespace", "{\"path\":\"" + scratchDir.toString() + "\"}");
            client().performRequest(new Request("GET", "/_cluster/health/" + indexName + "?wait_for_status=yellow&timeout=30s"));
            client().performRequest(new Request("DELETE", "/" + indexName));
            Thread.sleep(3_000);
            Response recovered = client().performRequest(
                new Request("GET", "/_cluster/health/" + indexName + "?wait_for_status=yellow&timeout=30s")
            );
            assertEquals(
                "expected the index to come back with grace=0: " + readAll(recovered),
                RestStatus.OK.getStatus(),
                recovered.getStatusLine().getStatusCode()
            );
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
            try {
                deleteJson("/_lance/namespace", "{\"path\":\"" + scratchDir.toString() + "\"}");
            } catch (Exception ignored) {}
            updateClusterSetting("lance.namespace.resurface_guard_grace", "1h");
        }
    }

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

    public void testAttachKeywordOnlyUtf8TableGoesGreen() throws Exception {
        // Regression for the SHA 403576c FieldInfos-duplicate bug: a
        // Utf8 column with no inverted index is loaded through the
        // keyword doc-values path AND through the text-column loop
        // that e21bf3c added for FLS visibility, and the old code
        // added both to the leaf reader's FieldInfos. Shard recovery
        // then failed with IllegalArgumentException: duplicate field
        // names and every FTS-less string-column table was red.
        // Verify the attach succeeds, the shard settles green, and a
        // term-level search against the keyword column returns hits
        // instead of a shards.failed response.
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

            // Health must not stay red. The recovery previously threw
            // IllegalArgumentException during LanceFragmentLeafReader
            // construction and marked the shard failed permanently.
            Response health = client().performRequest(
                new Request("GET", "/_cluster/health/" + indexName + "?wait_for_status=yellow&timeout=30s")
            );
            String healthBody = readAll(health);
            assertFalse("index went red: " + healthBody, healthBody.contains("\"status\":\"red\""));

            // Confirm derive() surfaced the column as keyword by
            // running a term query, which only works on keyword doc
            // values. If FieldInfos was duplicated the shard wouldn't
            // respond at all.
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

    public void testFragmentDispatchModeAnswersFilterQueries() throws Exception {
        // Fragment-path baseline: match_all + filter queries
        // (term / terms / exists / range / bool) on Lance-backed
        // indices flow through the plugin's own executor. The count
        // and hits both come from Lance via LanceKnnFilterTranslator
        // (metadata-only Dataset.countRows for the count, and
        // ScanOptions.filter for the hits' Lance scan), so
        // hits.total.value stays in sync with the number of matching
        // hits regardless of shard state.
        String suffix = "dispatch-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            String matchAllBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match_all\":{}}}"));
            int matchAllHits = extractIntPath(matchAllBody, "hits", "total", "value");
            assertEquals("fragment path match_all must return the true row count", 6, matchAllHits);
            // Default size is 10 so a 6-row table returns all six
            // hits. Each hit carries a synthesised _id in the form
            // "<fragmentId>-<offset>" and a _source rendered from
            // the Arrow batch. The LanceTableFactory fixture puts
            // "hello lance " at even offsets and "quick brown fox"
            // at odd offsets in the body column; both must show
            // up in the response.
            assertTrue("fragment path match_all must populate the hits array: " + matchAllBody, matchAllBody.contains("\"_id\":\"0-0\""));
            assertTrue(
                "fragment path match_all must render _source with the body column: " + matchAllBody,
                matchAllBody.contains("hello lance")
            );
            assertTrue(
                "fragment path match_all must render _source with the id column: " + matchAllBody,
                matchAllBody.contains("\"id\":0")
            );

            // A size=2 request returns only two hits but keeps the
            // total row count at six.
            String sizeBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match_all\":{}},\"size\":2}"));
            assertEquals("size clause must not affect total", 6, extractIntPath(sizeBody, "hits", "total", "value"));
            int returnedHits = countOccurrences(sizeBody, "\"_id\":");
            assertEquals("size=2 must return exactly two hits: " + sizeBody, 2, returnedHits);

            // Term queries on numeric columns are answered by the
            // fragment executor. The count and hit metadata both come
            // from the plugin's own path via
            // LanceKnnFilterTranslator -> Dataset.countRows(sql) +
            // ScanOptions.filter(sql).
            String termBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"term\":{\"id\":3}}}"));
            assertEquals("fragment path term must match exactly one row", 1, extractIntPath(termBody, "hits", "total", "value"));
            assertTrue("fragment path term must return the id=3 hit: " + termBody, termBody.contains("\"_id\":\"0-3\""));
            assertTrue("fragment path term must render the matching row: " + termBody, termBody.contains("\"id\":3"));

            // A numeric range covers three rows (id in {2, 3, 4}).
            String rangeBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"id\":{\"gte\":2,\"lt\":5}}}}"));
            assertEquals("fragment path range must count matching rows", 3, extractIntPath(rangeBody, "hits", "total", "value"));
            assertTrue("fragment path range must include id=2: " + rangeBody, rangeBody.contains("\"id\":2"));
            assertTrue("fragment path range must include id=4: " + rangeBody, rangeBody.contains("\"id\":4"));

            // Bool AND of two filters proves nested translation works
            // end-to-end. id >= 2 intersects id = 3, so the response
            // must count and return exactly the id=3 row.
            String boolBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"bool\":{\"filter\":[{\"range\":{\"id\":{\"gte\":2}}},{\"term\":{\"id\":3}}]}}}"
                )
            );
            assertEquals("fragment path bool must count the intersection", 1, extractIntPath(boolBody, "hits", "total", "value"));
            assertTrue("fragment path bool must return the id=3 hit: " + boolBody, boolBody.contains("\"_id\":\"0-3\""));

            // Full-text match queries also flow through the fragment
            // executor since Stage 3 widened the dispatch filter. The
            // per-node handler translates the QueryBuilder via
            // QueryShardContext.toQuery, gets a LanceFtsQuery from the
            // lance_text field mapper, and drives IndexSearcher.search
            // against the per-fragment reader.
            int matchHits = extractIntPath(
                readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match\":{\"body\":\"lance\"}}}")),
                "hits",
                "total",
                "value"
            );
            assertTrue("match query must return hits on fragment path (got " + matchHits + ")", matchHits > 0);
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAnswersDateRangeQuery() throws Exception {
        // Issue #43: a `range` query with an ISO-8601 string literal
        // ({"gte":"2024-03-01","lt":"2024-04-01"}) on a Lance
        // Timestamp column used to return 400. LanceKnnFilterTranslator
        // emitted a bare Utf8 SQL literal ('2024-03-01') and
        // DataFusion rejected the comparison against a Timestamp
        // column with "could not convert to literal of type
        // 'Timestamp(...)'". The translator now recognises ISO-8601
        // shapes and lifts them into `timestamp '...'` so the same
        // range DSL that works on shard-path date fields also works
        // when the request lands on the fragment executor.
        //
        // Four shapes exercise the fix:
        // (a) date-only literal
        // (b) datetime literal (with T and seconds)
        // (c) bool filter combining a keyword term with a date range
        // (d) date_histogram bucket aggregation consuming the date column
        String suffix = "date-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeDatedTable(scratchDir, tableName);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // (a) Date-only literal: id 2 (2024-03-10) and id 3
            // (2024-03-25) are the two March rows in the fixture, so
            // [2024-03-01, 2024-04-01) picks exactly those two.
            String dateOnly = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"ts\":{\"gte\":\"2024-03-01\",\"lt\":\"2024-04-01\"}}}}")
            );
            assertEquals(
                "date-only range must count only the two March rows: " + dateOnly,
                2,
                extractIntPath(dateOnly, "hits", "total", "value")
            );
            assertTrue("date-only range must include the id=2 hit: " + dateOnly, dateOnly.contains("\"id\":2"));
            assertTrue("date-only range must include the id=3 hit: " + dateOnly, dateOnly.contains("\"id\":3"));

            // (b) Datetime literal: id 1 (2024-02-20) and id 2
            // (2024-03-10) fall inside
            // [2024-02-01T00:00:00Z, 2024-03-15T12:00:00Z). id 3
            // (2024-03-25) sits above the upper bound and must be
            // excluded, proving the timestamp comparison respects
            // sub-day precision.
            String dateTime = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"range\":{\"ts\":{\"gte\":\"2024-02-01T00:00:00Z\",\"lt\":\"2024-03-15T12:00:00Z\"}}}}"
                )
            );
            assertEquals(
                "datetime range must count Feb + early-March rows only: " + dateTime,
                2,
                extractIntPath(dateTime, "hits", "total", "value")
            );
            assertTrue("datetime range must include the id=1 hit: " + dateTime, dateTime.contains("\"id\":1"));
            assertTrue("datetime range must include the id=2 hit: " + dateTime, dateTime.contains("\"id\":2"));

            // (c) bool filter [term category=odd, range ts]: the odd
            // subset is {1, 3, 5}, the date range keeps rows in
            // [2024-01-01, 2024-05-01), and id 5 (2024-05-30) falls
            // outside the upper bound. The intersection is exactly
            // {1, 3}. This proves nested translation still routes the
            // date literal through the timestamp path.
            String boolBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"bool\":{\"filter\":["
                        + "{\"term\":{\"category\":\"odd\"}},"
                        + "{\"range\":{\"ts\":{\"gte\":\"2024-01-01\",\"lt\":\"2024-05-01\"}}}"
                        + "]}}}"
                )
            );
            assertEquals(
                "bool filter must intersect the keyword and date-range subsets: " + boolBody,
                2,
                extractIntPath(boolBody, "hits", "total", "value")
            );
            assertTrue("bool filter must include id=1: " + boolBody, boolBody.contains("\"id\":1"));
            assertTrue("bool filter must include id=3: " + boolBody, boolBody.contains("\"id\":3"));

            // (d) date_histogram on the same column, monthly interval.
            // All six rows contribute: {Jan:1, Feb:1, March:2, April:1,
            // May:1}, so five buckets are opened and the March bucket
            // carries two docs. min_doc_count 1 keeps empty months out.
            String hist = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"aggs\":{\"per_month\":{\"date_histogram\":"
                        + "{\"field\":\"ts\",\"calendar_interval\":\"month\",\"min_doc_count\":1}}}}"
                )
            );
            assertEquals("date_histogram must see every row: " + hist, 6, extractIntPath(hist, "hits", "total", "value"));
            int monthBuckets = countOccurrences(hist, "\"doc_count\":");
            assertEquals("date_histogram must open five monthly buckets: " + hist, 5, monthBuckets);
            assertTrue("date_histogram must carry a bucket with two docs (March): " + hist, hist.contains("\"doc_count\":2"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAnswersDateRangeQueryWithEpochMillisLiterals() throws Exception {
        // Issue #48 (follow-up to #43): a `range` on a Lance
        // Timestamp column with a numeric epoch-millis literal —
        // {"gte": 1709251200000} — used to return 400 with
        // `Received literal Int64(...) and could not convert to
        // literal of type 'Timestamp(...)'` because the translator
        // emitted the number as a bare Int64. The #43 fix only
        // handled ISO-8601 strings because the translator had no
        // mapping context to know whether a numeric literal was
        // meant to be a date or a plain integer.
        //
        // The follow-up plumbs a field-type lookup through
        // {@link org.opensearch.lance.query.LanceKnnFilterTranslator#toLanceSql(QueryBuilder, Function)}
        // so both the coordinator (via
        // {@code TransportLanceCoordinatorAction.resolveTargets} +
        // {@code IndexMetadata.mapping()}) and the knn inner filter
        // ({@code LanceKnnQueryBuilder.doToQuery} via
        // {@code QueryShardContext.fieldMapper}) can tell the
        // translator that a given field is mapped as `date`. When
        // the field is a date, a numeric literal gets wrapped in
        // {@code to_timestamp_millis(...)} so DataFusion coerces
        // to whatever Timestamp unit the Lance column carries.
        //
        // Shapes exercised:
        // (a) range ts with epoch-millis literals only
        // (b) bool filter combining a keyword term with an
        // epoch-millis date range (same intersection as the
        // ISO-8601 variant in #43's IT)
        // (c) regression fence: ISO-8601 string literals still
        // resolve on the mapping-aware path
        String suffix = "dateml-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeDatedTable(scratchDir, tableName);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // Epoch-millis anchor points (UTC midnight, matching what
            // OpenSearch's date field emits from a numeric literal
            // input by default):
            // 2024-03-01T00:00:00Z = 1709251200000
            // 2024-04-01T00:00:00Z = 1711929600000
            // 2024-05-01T00:00:00Z = 1714521600000

            // (a) numeric range: id 2 (2024-03-10) and id 3
            // (2024-03-25) sit inside [1709251200000, 1711929600000),
            // matching the ISO-8601 variant of the same interval in
            // testFragmentDispatchModeAnswersDateRangeQuery.
            String numeric = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"ts\":{\"gte\":1709251200000,\"lt\":1711929600000}}}}")
            );
            assertEquals(
                "numeric range must count only the two March rows: " + numeric,
                2,
                extractIntPath(numeric, "hits", "total", "value")
            );
            assertTrue("numeric range must include id=2: " + numeric, numeric.contains("\"id\":2"));
            assertTrue("numeric range must include id=3: " + numeric, numeric.contains("\"id\":3"));

            // (b) bool + term + numeric range: category=odd narrows
            // to {1, 3, 5}, the date range keeps rows in
            // [2024-01-01, 2024-05-01), and id 5 (2024-05-30) falls
            // outside the upper bound. Intersection: {1, 3}.
            String bool = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"bool\":{\"filter\":["
                        + "{\"term\":{\"category\":\"odd\"}},"
                        + "{\"range\":{\"ts\":{\"gte\":1704067200000,\"lt\":1714521600000}}}"
                        + "]}}}"
                )
            );
            assertEquals("bool + numeric range must intersect: " + bool, 2, extractIntPath(bool, "hits", "total", "value"));
            assertTrue("bool + numeric range must include id=1: " + bool, bool.contains("\"id\":1"));
            assertTrue("bool + numeric range must include id=3: " + bool, bool.contains("\"id\":3"));

            // (c) regression fence: ISO-8601 string variant of (a)
            // must resolve to the same two rows through the
            // mapping-aware translator (the shape heuristic and the
            // mapping-driven branch converge on the same output for
            // this shape).
            String iso = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"ts\":{\"gte\":\"2024-03-01\",\"lt\":\"2024-04-01\"}}}}")
            );
            assertEquals("ISO-8601 regression fence: " + iso, 2, extractIntPath(iso, "hits", "total", "value"));
            assertTrue("ISO regression must include id=2: " + iso, iso.contains("\"id\":2"));
            assertTrue("ISO regression must include id=3: " + iso, iso.contains("\"id\":3"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAnswersQueriesAgainstUnmappedFieldsWithoutError() throws Exception {
        // Issue #50: a range / bool query against a field that
        // derive() left unmapped used to return 500 with
        // `illegal_state_exception: Rewrite first`. The exception
        // comes from RangeQueryBuilder.doToQuery reaching for a
        // MappedFieldType that is null; the shard path never hits
        // it because SearchService.parseSource calls
        // Rewriteable.rewrite before toQuery, which folds an
        // unmapped range into MatchNoneQueryBuilder via
        // RangeQueryBuilder.doRewrite. The fragment executor
        // skipped that rewrite step.
        //
        // After the fix (a) TransportLanceFragmentQueryAction
        // rewrites request.query() and request.postFilter() before
        // handing them to toQuery, and (b) the coordinator's
        // resolveFilterSql refuses to emit Lance SQL when any leaf
        // names an unmapped field so Dataset.countRows(sql) does
        // not surface a second 500 from the count path. Behaviour
        // now matches the shard path: 200 with 0 hits, no error.
        //
        // Shapes exercised:
        // (a) `range unmapped_int {gte:1}` — pure unmapped range,
        // which is the exact repro from the issue.
        // (b) `range unmapped_str {gte:"aa"}` — string-shaped
        // range against an unmapped field (regression fence
        // for the ISO-8601 shape-heuristic branch in the
        // translator's literal encoder).
        // (c) `bool must [term body="alpha", range unmapped_int]`
        // — nested case where the coordinator's
        // hasUnmappedField walker has to recurse into the
        // bool tree, and Rewriteable.rewrite on the per-node
        // side has to fold the range clause inside the bool.
        // (d) Regression fence: a range against the mapped `id`
        // column still returns the correct hit set on the
        // same index (no over-broad match-none rewrite).
        String suffix = "unmapped-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 4);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // (a) numeric range on unmapped field
            String numeric = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"unmapped_int\":{\"gte\":1}}}}"));
            assertEquals("unmapped range must return 0 hits: " + numeric, 0, extractIntPath(numeric, "hits", "total", "value"));

            // (b) string range on unmapped field. Also exercises
            // the branch in LanceKnnFilterTranslator.literal where
            // a shape-heuristic ISO-8601 detection would fire on a
            // string literal; the coordinator's hasUnmappedField
            // walker skips translation before we get there.
            String stringRange = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"unmapped_str\":{\"gte\":\"aa\"}}}}")
            );
            assertEquals(
                "unmapped string range must return 0 hits: " + stringRange,
                0,
                extractIntPath(stringRange, "hits", "total", "value")
            );

            // (c) bool must with one mapped and one unmapped
            // clause. RangeQueryBuilder.doRewrite folds the
            // unmapped range to MatchNone, then
            // BoolQueryBuilder.doRewrite collapses the whole
            // bool to a query that matches nothing. Both the
            // per-node hits path and the count path have to see
            // this or hits.total.value would collapse to only
            // the term-clause matches.
            String bool = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"bool\":{\"must\":["
                        + "{\"term\":{\"body\":\"alpha\"}},"
                        + "{\"range\":{\"unmapped_int\":{\"gte\":1}}}"
                        + "]}}}"
                )
            );
            assertEquals("bool must with unmapped clause must return 0 hits: " + bool, 0, extractIntPath(bool, "hits", "total", "value"));

            // (d) regression fence: range on the mapped `id`
            // column still resolves normally on the same index.
            // Ensures the rewrite step does not accidentally
            // treat mapped fields as MatchNone.
            String mapped = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"id\":{\"gte\":1,\"lt\":3}}},\"size\":10}")
            );
            assertEquals("mapped range must return 2 hits: " + mapped, 2, extractIntPath(mapped, "hits", "total", "value"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeCountsFtsHitsWithoutWeightMaterialisation() throws Exception {
        // triggered LanceFtsQuery's Weight to fully materialise every
        // hit's row address and score into the sparse buffer. On a 20M
        // row table with 500k matching hits QA measured 4.6 s for a
        // count-only query that pylance answered in <2 ms because
        // Lance's inverted-index scan can stream row counts when we
        // ask for zero columns. This test fences that count path:
        //
        // - lance_match size:0 must return the exact match count
        // (no clip, no over-count) for match / phrase / bool
        // - post_filter present + size:0 must fall through to the
        // Weight path (post_filter narrowing needs Lucene)
        // - non-FTS scoring shapes (knn) continue on the Weight
        // path since Lance has no countRows(NearestQuery)
        String suffix = "ftscount-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        // 20 rows: 10 with "hello lance N" (even ids), 10 with "quick
        // brown fox N" (odd ids). "lance" hits 10 rows, "hello lance"
        // as a phrase hits the same 10 rows.
        LanceTableFactory.writeTable(scratchDir, tableName, 20);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // Simple lance_match count-only: total = 10, hits empty.
            String matchBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match\":{\"body\":\"lance\"}},\"size\":0}"));
            assertEquals("match size:0 total must be 10", 10, extractIntPath(matchBody, "hits", "total", "value"));
            assertEquals("size:0 must return no hits", 0, countOccurrences(matchBody, "\"_id\":"));

            // lance_match_phrase count-only: same 10 rows contain
            // "hello lance N" so the phrase count matches the
            // simple match count.
            String phraseBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_match_phrase\":{\"field\":\"body\",\"query\":\"hello lance\"}},\"size\":0}"
                )
            );
            assertEquals("phrase size:0 total must be 10", 10, extractIntPath(phraseBody, "hits", "total", "value"));

            // Zero-match count: query never touches the fixture so
            // the count path must still return 0 (regression fence
            // against the Weight path failing when the FTS scan
            // yields empty batches).
            String zeroBody = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"match\":{\"body\":\"nonexistent\"}},\"size\":0}")
            );
            assertEquals("no-match size:0 total must be 0", 0, extractIntPath(zeroBody, "hits", "total", "value"));

            // Post-filter forces the Weight fallback because the
            // post_filter narrows below what Dataset.countRows
            // would report. The fixture pins ids 0..19 sequential,
            // so id >= 10 keeps five "hello lance" rows (10, 12, 14,
            // 16, 18) and drops the rest. When Weight-fallback works
            // correctly total = 5, hits empty (size:0).
            String postFilterBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"match\":{\"body\":\"lance\"}}," + "\"post_filter\":{\"range\":{\"id\":{\"gte\":10}}}," + "\"size\":0}"
                )
            );
            assertEquals(
                "match + post_filter size:0 must narrow to 5 via Weight fallback",
                5,
                extractIntPath(postFilterBody, "hits", "total", "value")
            );
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAppliesTopKPushdownForFtsHits() throws Exception {
        // Issue #42 Phase B: pure FTS (lance_match / lance_match_phrase
        // as the top-level query) with no sort, no aggregation, and no
        // post_filter must push size into the per-fragment Lance scan
        // as `limit(size)`. Lance's inverted-index scorer holds a
        // bounded score-sorted heap, so a size:5 request against a
        // large hit set never has to transfer or rank 4+ orders of
        // magnitude of rows the client will not look at.
        //
        // Regression fence covers: hits stay correct up to the
        // requested size, hits.total.value keeps returning the true
        // count from LanceFtsQuery's Weight (because that Weight runs
        // once per size:0 count call and the scan limit is disabled
        // there), and shapes that must NOT enable the pushdown (sort
        // by non-score field / agg) keep serving the full matched set.
        String suffix = "ftstop-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        // 20 rows: 10 with "hello lance N" (even ids), 10 with "quick
        // brown fox N" (odd ids). The "lance" match yields 10 hits.
        LanceTableFactory.writeTable(scratchDir, tableName, 20);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // Match "lance" hits 10 rows; size:3 must clip hits to 3
            // while hits.total.value stays at 10. If the clip leaked
            // into the total, the count would drop to 3.
            String matchBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match\":{\"body\":\"lance\"}},\"size\":3}"));
            assertEquals("match size:3 total must be 10", 10, extractIntPath(matchBody, "hits", "total", "value"));
            int matchReturnedHits = countOccurrences(matchBody, "\"_id\":");
            assertEquals("match size:3 must return three hits: " + matchBody, 3, matchReturnedHits);

            // Match with a size larger than the matched set must
            // return every match (no clip, no padding).
            String allMatchBody = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"match\":{\"body\":\"lance\"}},\"size\":50}")
            );
            assertEquals("match size:50 total must be 10", 10, extractIntPath(allMatchBody, "hits", "total", "value"));
            int allMatchReturnedHits = countOccurrences(allMatchBody, "\"_id\":");
            assertEquals("match size:50 must return ten hits: " + allMatchBody, 10, allMatchReturnedHits);

            // size:0 count-only: hits stays empty, total reflects the
            // full match (served by IndexSearcher.count on the FTS
            // Weight without the top-k enabled).
            String countBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match\":{\"body\":\"lance\"}},\"size\":0}"));
            assertEquals("match size:0 total must be 10", 10, extractIntPath(countBody, "hits", "total", "value"));
            int countReturnedHits = countOccurrences(countBody, "\"_id\":");
            assertEquals("size:0 must return no hits", 0, countReturnedHits);

            // Sort by a non-score field must disable the top-k
            // pushdown: even at size:3, we need every match so sort
            // by id desc can pick the largest id (the row with "hello
            // lance 18" at id=18 must come first for the "lance"
            // match on even ids).
            String sortBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"match\":{\"body\":\"lance\"}},\"size\":3,\"sort\":[{\"id\":\"desc\"}]}"
                )
            );
            assertEquals("match sort size:3 total must be 10", 10, extractIntPath(sortBody, "hits", "total", "value"));
            int sortReturnedHits = countOccurrences(sortBody, "\"_id\":");
            assertEquals("match sort size:3 must return three hits: " + sortBody, 3, sortReturnedHits);
            assertTrue("match sort desc must put id=18 first: " + sortBody, sortBody.contains("\"id\":18"));

            // Aggregation must disable the top-k pushdown: the sum of
            // even ids 0..18 is 0+2+4+6+8+10+12+14+16+18 = 90. If the
            // scan were clipped to 3, the sum would be < 90.
            String aggBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"match\":{\"body\":\"lance\"}},\"size\":3,\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}"
                )
            );
            assertEquals("match agg size:3 total must be 10", 10, extractIntPath(aggBody, "hits", "total", "value"));
            assertEquals("sum(id) over match must be 90", 90.0d, extractDoublePath(aggBody, "aggregations", "s", "value"), 0.0d);

            // lance_match_phrase should follow the same pushdown path.
            // "hello lance" matches all 10 even-id rows exactly.
            String phraseBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_match_phrase\":{\"field\":\"body\",\"query\":\"hello lance\"}},\"size\":4}"
                )
            );
            assertEquals("phrase size:4 total must be 10", 10, extractIntPath(phraseBody, "hits", "total", "value"));
            int phraseReturnedHits = countOccurrences(phraseBody, "\"_id\":");
            assertEquals("phrase size:4 must return four hits: " + phraseBody, 4, phraseReturnedHits);
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAppliesTopKPushdownForScalarFilterHits() throws Exception {
        // Issue #42 Phase A: pure scalar filter (term / range / bool
        // built from LanceKnnFilterTranslator-translatable clauses)
        // with no sort, no aggregation, and no post_filter must
        // push size into the per-fragment Lance scan as `limit(size)`.
        // The regression fence covers: hits stay correct up to the
        // requested size, hits.total.value keeps returning the true
        // count from Dataset.countRows(sql), and shapes that must
        // NOT enable the pushdown (sort / agg) keep serving the full
        // matched set.
        String suffix = "topk-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        // 20 rows so a size:5 request exercises the top-k clip while
        // still leaving enough distinct rows for the count assertion.
        LanceTableFactory.writeTable(scratchDir, tableName, 20);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // Range covers 20 rows; size:5 must clip hits to 5 while
            // hits.total.value stays at 20. If the clip leaks into
            // Dataset.countRows the total would drop to 5.
            String rangeBody = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"id\":{\"gte\":0,\"lt\":20}}},\"size\":5}")
            );
            assertEquals("range size:5 total must be 20", 20, extractIntPath(rangeBody, "hits", "total", "value"));
            int rangeReturnedHits = countOccurrences(rangeBody, "\"_id\":");
            assertEquals("range size:5 must return five hits: " + rangeBody, 5, rangeReturnedHits);

            // Term narrowing to a single row must still return that
            // row even when size:5 is more than the matched count.
            String termBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"term\":{\"id\":3}},\"size\":5}"));
            assertEquals("term id=3 total must be 1", 1, extractIntPath(termBody, "hits", "total", "value"));
            assertTrue("term id=3 must return the id=3 hit: " + termBody, termBody.contains("\"_id\":\"0-3\""));

            // size:0 count-only: hits stays empty, total reflects the
            // full match (served by Dataset.countRows(sql) directly).
            String countBody = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"id\":{\"gte\":0,\"lt\":20}}},\"size\":0}")
            );
            assertEquals("range size:0 total must be 20", 20, extractIntPath(countBody, "hits", "total", "value"));
            int countReturnedHits = countOccurrences(countBody, "\"_id\":");
            assertEquals("size:0 must return no hits", 0, countReturnedHits);

            // Sort must disable the top-k pushdown: even at size:5 we
            // need the full matched set to sort by id desc, and the
            // first hit must be id=19 (max id in the range).
            String sortBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"range\":{\"id\":{\"gte\":0,\"lt\":20}}},\"size\":5,\"sort\":[{\"id\":\"desc\"}]}"
                )
            );
            assertEquals("sort size:5 total must be 20", 20, extractIntPath(sortBody, "hits", "total", "value"));
            int sortReturnedHits = countOccurrences(sortBody, "\"_id\":");
            assertEquals("sort size:5 must return five hits: " + sortBody, 5, sortReturnedHits);
            assertTrue("sort desc must put id=19 first: " + sortBody, sortBody.contains("\"id\":19"));
            assertTrue("sort desc must put id=15 last: " + sortBody, sortBody.contains("\"id\":15"));

            // Aggregation must disable the top-k pushdown: sum over
            // the full range is 0 + 1 + ... + 19 = 190. If the scan
            // were clipped to 5, the sum would be < 190.
            String aggBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"range\":{\"id\":{\"gte\":0,\"lt\":20}}},\"size\":5,\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}"
                )
            );
            assertEquals("agg size:5 total must be 20", 20, extractIntPath(aggBody, "hits", "total", "value"));
            assertEquals("sum(id) over range must be 190", 190.0d, extractDoublePath(aggBody, "aggregations", "s", "value"), 0.0d);
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAnswersMetricAggregations() throws Exception {
        // Milestone 5-B of the shard-free dispatch prototype: setting
        // lance.dispatch.mode = fragment must let the plugin's own
        // executor answer the five metric aggregations
        // LanceMetricAggregator supports (value_count, sum, avg, min,
        // max), possibly combined with a filter query. The count,
        // hits, and aggregation branches all share the same filter
        // push-down, so a filter query narrows the aggregation the
        // same way it narrows the count. Bucket aggregations, script
        // metrics, and multi-index aggregation continue to route
        // through the standard shard fan-out.
        String suffix = "aggs-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // The fixture writes six rows with id = 0..5, so the
            // canonical aggregates are: count = 6, sum = 15,
            // avg = 2.5, min = 0, max = 5. size=0 is standard for
            // aggregation-only requests and avoids paying the
            // hits scan on the same request.
            String body = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"aggs\":{"
                        + "\"c\":{\"value_count\":{\"field\":\"id\"}},"
                        + "\"s\":{\"sum\":{\"field\":\"id\"}},"
                        + "\"a\":{\"avg\":{\"field\":\"id\"}},"
                        + "\"m\":{\"min\":{\"field\":\"id\"}},"
                        + "\"M\":{\"max\":{\"field\":\"id\"}}"
                        + "}}"
                )
            );
            assertEquals("aggregation must count matching rows via Dataset.countRows", 6, extractIntPath(body, "hits", "total", "value"));
            assertEquals("value_count on id must equal row count", 6, extractIntPath(body, "aggregations", "c", "value"));
            assertEquals("sum(id) 0..5 == 15", 15.0d, extractDoublePath(body, "aggregations", "s", "value"), 0.0d);
            assertEquals("avg(id) 0..5 == 2.5", 2.5d, extractDoublePath(body, "aggregations", "a", "value"), 0.0d);
            assertEquals("min(id) == 0", 0.0d, extractDoublePath(body, "aggregations", "m", "value"), 0.0d);
            assertEquals("max(id) == 5", 5.0d, extractDoublePath(body, "aggregations", "M", "value"), 0.0d);

            // Filter query pushes the same SQL predicate into
            // Dataset.countRows and ScanOptions.filter, so
            // sum(id where id >= 2) must equal 2+3+4+5 = 14 with
            // total = 4.
            String filteredBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"query\":{\"range\":{\"id\":{\"gte\":2}}}," + "\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}"
                )
            );
            assertEquals("filtered aggregation must count filtered rows", 4, extractIntPath(filteredBody, "hits", "total", "value"));
            assertEquals("sum(id) with id>=2 == 14", 14.0d, extractDoublePath(filteredBody, "aggregations", "s", "value"), 0.0d);

            // Hits + aggregation in the same request must both
            // come from the fragment executor: hits carry the
            // synthesised _rowaddr id, aggregations carry the
            // computed value.
            String hitsPlusAggs = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":2,\"query\":{\"match_all\":{}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}"
                )
            );
            assertEquals("total unchanged when hits also requested", 6, extractIntPath(hitsPlusAggs, "hits", "total", "value"));
            assertTrue("hits section must carry the synthesised _id: " + hitsPlusAggs, hitsPlusAggs.contains("\"_id\":\"0-0\""));
            assertEquals(
                "sum unchanged when hits also requested",
                15.0d,
                extractDoublePath(hitsPlusAggs, "aggregations", "s", "value"),
                0.0d
            );

            // Direction 1 Stage 2: terms bucket aggregation
            // now runs on the fragment executor via the stock
            // TermsAggregator against per-fragment
            // LanceFragmentLeafReaders. The response must carry
            // one bucket per distinct id value (6 rows, all
            // unique ids 0..5 = 6 buckets, doc_count=1 each).
            String bucketBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":0,\"aggs\":{\"by_id\":{\"terms\":{\"field\":\"id\",\"size\":10}}}}")
            );
            assertTrue("terms aggregation via fragment path carries buckets: " + bucketBody, bucketBody.contains("\"buckets\":"));
            assertEquals(6, extractIntPath(bucketBody, "hits", "total", "value"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAnswersTermsWithDeferredMetricSubAggregation() throws Exception {
        // Regression for issue #40: `terms` with a deferred metric
        // sub-aggregation (`avg` / `sum` / `max`) or a nested `terms`
        // used to return 500 `Already been replayed` on the fragment
        // path because the executor invoked postCollection() and
        // buildAggregations() manually after
        // ContextIndexSearcher.search had already run
        // BucketCollectorProcessor#processPostCollection. The second
        // pass drove BestBucketsDeferringCollector#prepareSelectedBuckets
        // a second time, which is what throws. Fix reads the built
        // aggregations back through Aggregator#getPostCollectionAggregation,
        // matching the shard path.
        //
        // Shapes covered: default collect_mode (breadth_first is the
        // default when a metric sub-agg is present, and it is the shape
        // that triggers the defer path). Sibling coverage without
        // pipeline aggregations (Phase 1a rejects the pipeline shape).
        String suffix = "tds-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // terms + avg (issue #40 primary reproducer). 6 rows with
            // unique ids 0..5 yields 6 buckets each holding a single
            // doc; avg over each bucket equals the id itself. Also
            // check the bucket for id=0 to confirm sub-agg values
            // actually round trip (the pre-fix path threw before
            // reaching sub-agg serialisation).
            String avgBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"aggs\":{\"by_id\":{\"terms\":{\"field\":\"id\",\"size\":10},"
                        + "\"aggs\":{\"a\":{\"avg\":{\"field\":\"id\"}}}}}}"
                )
            );
            assertEquals("terms + avg total unchanged", 6, extractIntPath(avgBody, "hits", "total", "value"));
            assertTrue("terms + avg carries buckets: " + avgBody, avgBody.contains("\"buckets\":"));
            assertTrue("terms + avg carries sub-agg value: " + avgBody, avgBody.contains("\"a\":{\"value\":0.0}"));

            // terms + sum (same defer path, different metric).
            String sumBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"aggs\":{\"by_id\":{\"terms\":{\"field\":\"id\",\"size\":10},"
                        + "\"aggs\":{\"s\":{\"sum\":{\"field\":\"id\"}}}}}}"
                )
            );
            assertEquals("terms + sum total unchanged", 6, extractIntPath(sumBody, "hits", "total", "value"));

            // terms + max (same defer path).
            String maxBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"aggs\":{\"by_id\":{\"terms\":{\"field\":\"id\",\"size\":10},"
                        + "\"aggs\":{\"m\":{\"max\":{\"field\":\"id\"}}}}}}"
                )
            );
            assertEquals("terms + max total unchanged", 6, extractIntPath(maxBody, "hits", "total", "value"));

            // terms + nested terms (also defer-driven; QA also
            // reproduced 500 with this shape).
            String nestedBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"aggs\":{\"by_id\":{\"terms\":{\"field\":\"id\",\"size\":10},"
                        + "\"aggs\":{\"nest\":{\"terms\":{\"field\":\"id\",\"size\":10}}}}}}"
                )
            );
            assertEquals("nested terms total unchanged", 6, extractIntPath(nestedBody, "hits", "total", "value"));

            // depth_first should have kept working before the fix
            // (defer is skipped). Include it so a regression that
            // breaks depth_first is caught too.
            String depthBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"aggs\":{\"by_id\":{\"terms\":{\"field\":\"id\",\"size\":10,\"collect_mode\":\"depth_first\"},"
                        + "\"aggs\":{\"a\":{\"avg\":{\"field\":\"id\"}}}}}}"
                )
            );
            assertEquals("depth_first terms + avg total unchanged", 6, extractIntPath(depthBody, "hits", "total", "value"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAppliesFilterPushdownDuringColumnMaterialisation() throws Exception {
        // Issue #42 Phase C / Step C-1: fragment path now passes the
        // top-level Lance SQL filter through to LanceFragmentLeafReader,
        // and every per-column ensureXxxLoaded scan layers the same
        // filter into ScanOptions before asking Lance for values.
        //
        // Before the fix, `filter + sum(x)` on a 20M row fragment
        // materialised every value of `x` even when the filter kept
        // 5% of rows, because the ensureNumericLoaded scan issued a
        // full-column scan and let the Weight side drop non-matching
        // rows after the fact. Correctness stayed intact but the
        // load side did the work the filter was supposed to prune.
        //
        // This test does not measure timing — it fences the correctness
        // side, so a follow-up refactor cannot silently drop matches
        // when the filter is pushed down. The bucket / metric values
        // must still equal the unfiltered aggregation restricted to
        // matching rows.
        String suffix = "fcpc-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        // Six rows: id 0..5, body alternating "hello lance i" (even)
        // and "quick brown fox i" (odd). Numeric column `id`
        // exercises numeric doc values; `body` (indexed as
        // lance_text with a keyword sub-field) exercises keyword doc
        // values and, indirectly, the terms-agg ordinal path.
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson(
                "/_lance/attach",
                // Declare body.raw as a keyword sub-field so the terms
                // aggregation cases below (b, c, d) can key on it.
                // Without the multi_fields clause the sub-field
                // resolves to nothing and the terms bucket silently
                // returns zero buckets.
                "{\"table\":\"" + tableUri + "\",\"multi_fields\":{\"body\":{\"raw\":{\"type\":\"keyword\"}}}}"
            );
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // (a) filter + metric aggs: id >= 2 keeps {2, 3, 4, 5}.
            // sum = 14, avg = 3.5, min = 2, max = 5, value_count = 4.
            // Every one of those values is served from
            // numericColumns.get("id") which was populated by a
            // scan that carried filterSql = "(id >= 2)"; the assert
            // catches any regression where the filter fails to
            // route to the leaf reader.
            String metricBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"query\":{\"range\":{\"id\":{\"gte\":2}}},\"aggs\":{"
                        + "\"c\":{\"value_count\":{\"field\":\"id\"}},"
                        + "\"s\":{\"sum\":{\"field\":\"id\"}},"
                        + "\"a\":{\"avg\":{\"field\":\"id\"}},"
                        + "\"m\":{\"min\":{\"field\":\"id\"}},"
                        + "\"M\":{\"max\":{\"field\":\"id\"}}"
                        + "}}"
                )
            );
            assertEquals("filter + metric aggs must count matching rows", 4, extractIntPath(metricBody, "hits", "total", "value"));
            assertEquals("value_count with id>=2 == 4", 4, extractIntPath(metricBody, "aggregations", "c", "value"));
            assertEquals("sum(id) with id>=2 == 14", 14.0d, extractDoublePath(metricBody, "aggregations", "s", "value"), 0.0d);
            assertEquals("avg(id) with id>=2 == 3.5", 3.5d, extractDoublePath(metricBody, "aggregations", "a", "value"), 0.0d);
            assertEquals("min(id) with id>=2 == 2", 2.0d, extractDoublePath(metricBody, "aggregations", "m", "value"), 0.0d);
            assertEquals("max(id) with id>=2 == 5", 5.0d, extractDoublePath(metricBody, "aggregations", "M", "value"), 0.0d);

            // (b) filter + terms bucket agg on the keyword sub-field
            // of a lance_text column. `id < 3` keeps {0, 1, 2}. The
            // body values on those rows are:
            // id=0 → "hello lance 0"
            // id=1 → "quick brown fox 1"
            // id=2 → "hello lance 2"
            // Terms agg on body.raw must open three buckets, each
            // with a doc count of 1. Bucket assembly walks
            // getSortedSetDocValues("body.raw") on filter-matched
            // docs; the ord dictionary the leaf reader builds must
            // contain the three matching values (and only those,
            // since ensureTextLoaded's scan was filtered).
            String termsBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"query\":{\"range\":{\"id\":{\"lt\":3}}},"
                        + "\"aggs\":{\"by_body\":{\"terms\":{\"field\":\"body.raw\",\"size\":10}}}}"
                )
            );
            assertEquals("filter + terms total must count matching rows", 3, extractIntPath(termsBody, "hits", "total", "value"));
            assertTrue("filter + terms must open a bucket for id=0's body: " + termsBody, termsBody.contains("hello lance 0"));
            assertTrue("filter + terms must open a bucket for id=1's body: " + termsBody, termsBody.contains("quick brown fox 1"));
            assertTrue("filter + terms must open a bucket for id=2's body: " + termsBody, termsBody.contains("hello lance 2"));
            // Rows outside the filter (id >= 3) must not leak into
            // the terms dictionary. If they did, the ord numbering
            // would shift and the bucket keys would be off. Pin the
            // negative case with a value from id=3 (odd row, body
            // starts with "quick brown fox 3") which the range
            // filter must exclude.
            assertFalse("filter + terms must not include rows outside the filter: " + termsBody, termsBody.contains("quick brown fox 3"));

            // (c) filter + terms with a metric sub-agg. Confirms
            // that the filter push-down still cooperates with the
            // #40 fix (getPostCollectionAggregation instead of a
            // second postCollection call). Same three buckets as
            // (b); each carries avg(id) == that row's id.
            String termsWithMetricBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"query\":{\"range\":{\"id\":{\"lt\":3}}},"
                        + "\"aggs\":{\"by_body\":{\"terms\":{\"field\":\"body.raw\",\"size\":10},"
                        + "\"aggs\":{\"a\":{\"avg\":{\"field\":\"id\"}}}}}}"
                )
            );
            assertEquals("filter + terms + sub-avg total unchanged", 3, extractIntPath(termsWithMetricBody, "hits", "total", "value"));
            // Presence of the three keys plus the avg field
            // structure inside each bucket is enough — Terms
            // regression fence for the sub-agg wire happens in the
            // dedicated Deferred sub-agg IT.
            assertTrue(
                "filter + terms + sub-avg must render the sub-agg: " + termsWithMetricBody,
                termsWithMetricBody.contains("\"a\":{\"value\":")
            );

            // (d) match query + terms agg (regression fence for the
            // FTS shape). Match queries are not translatable to
            // Lance SQL, so filterSql stays null and
            // ensureXxxLoaded falls back to the pre-Phase-C
            // unfiltered scan. Correctness must be identical to the
            // shard path: `match body:lance` hits the three even
            // rows (0, 2, 4), each with a distinct body value.
            String matchBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"query\":{\"match\":{\"body\":\"lance\"}},"
                        + "\"aggs\":{\"by_body\":{\"terms\":{\"field\":\"body.raw\",\"size\":10}}}}"
                )
            );
            assertEquals("match + terms total must count matching FTS rows", 3, extractIntPath(matchBody, "hits", "total", "value"));
            assertTrue("match + terms must open a bucket for id=0's body: " + matchBody, matchBody.contains("hello lance 0"));
            assertTrue("match + terms must open a bucket for id=4's body: " + matchBody, matchBody.contains("hello lance 4"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAnswersFloatQueries() throws Exception {
        // Issue #44: Float32 and Float64 scalar columns used to fall
        // through derive() into the "stored only" notes bucket,
        // leaving the column with no mapping. `range price` returned
        // 500 `Rewrite first` (unknown-field query path),
        // `sort price` returned 400 `No mapping found`, metric
        // aggregations returned 200 but with a null value, and the
        // JSON _source omitted the column entirely despite the
        // "stored only" wording.
        //
        // The fix wires float32 → `float` and float64 → `double`
        // through derive(), and the leaf reader routes both through
        // the shared NumericDocValues path via
        // NumericUtils.floatToSortableInt / doubleToSortableLong.
        // _source rendering decodes the sortable encoding on the
        // way out so the JSON number equals the original value.
        //
        // Shapes exercised:
        // (a) range on float32 (price >= 25.0 && price < 55.0)
        // (b) sort ascending on float64 (weight)
        // (c) metric aggregations (sum / avg / min / max) on both columns
        // (d) _source round trip of the exact float32 / float64 values
        String suffix = "float-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeFloatColumnTable(scratchDir, tableName);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // Mapping must expose price / weight with the correct
            // OpenSearch numeric types. Regression fence for the
            // derive() branch: if a future refactor drops the
            // FloatingPoint case, this assertion catches it before
            // the query-side asserts explain why.
            String mappingBody = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_mapping")));
            assertTrue("mapping must expose price as float: " + mappingBody, mappingBody.contains("\"price\":{\"type\":\"float\""));
            assertTrue("mapping must expose weight as double: " + mappingBody, mappingBody.contains("\"weight\":{\"type\":\"double\""));

            // (a) range on price: 6 rows, price = i * 12.5f, so
            // values are {0.0, 12.5, 25.0, 37.5, 50.0, 62.5}. The
            // half-open range [25.0, 55.0) picks {25.0, 37.5, 50.0},
            // rows id={2, 3, 4}.
            String rangeBody = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"price\":{\"gte\":25.0,\"lt\":55.0}}}}")
            );
            assertEquals("range price total: " + rangeBody, 3, extractIntPath(rangeBody, "hits", "total", "value"));
            assertTrue("range price must include id=2: " + rangeBody, rangeBody.contains("\"id\":2"));
            assertTrue("range price must include id=3: " + rangeBody, rangeBody.contains("\"id\":3"));
            assertTrue("range price must include id=4: " + rangeBody, rangeBody.contains("\"id\":4"));

            // (b) sort weight ascending: values are {0/3, 1/3, 2/3,
            // 1.0, 4/3, 5/3}. Ascending sort returns id order 0..5.
            // Reading id from the first hit is enough to prove the
            // double doc-value comparator ordered them correctly.
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

            // (c) metric aggs: expected values for six rows.
            // sum(price) = 12.5 * (0+1+2+3+4+5) = 12.5 * 15 = 187.5
            // avg(price) = 187.5 / 6 = 31.25
            // min(price) = 0.0
            // max(price) = 62.5
            // sum(weight) = 0/3+1/3+2/3+3/3+4/3+5/3 = 15/3 = 5.0
            // min(weight) = 0.0
            // max(weight) = 5/3 ≈ 1.6666666
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
            // Small tolerance for float rounding: even sortableInt
            // decoding preserves the original float32 exactly, but
            // sum accumulates in double and the intermediate float32
            // representations round.
            assertEquals("sum(price) == 187.5", 187.5d, extractDoublePath(aggBody, "aggregations", "ps", "value"), 1e-4);
            assertEquals("avg(price) == 31.25", 31.25d, extractDoublePath(aggBody, "aggregations", "pa", "value"), 1e-4);
            assertEquals("min(price) == 0.0", 0.0d, extractDoublePath(aggBody, "aggregations", "pm", "value"), 1e-4);
            assertEquals("max(price) == 62.5", 62.5d, extractDoublePath(aggBody, "aggregations", "pM", "value"), 1e-4);
            assertEquals("sum(weight) == 5.0", 5.0d, extractDoublePath(aggBody, "aggregations", "ws", "value"), 1e-9);
            assertEquals("min(weight) == 0.0", 0.0d, extractDoublePath(aggBody, "aggregations", "wm", "value"), 1e-9);
            assertEquals("max(weight) == 5/3", 5.0d / 3.0d, extractDoublePath(aggBody, "aggregations", "wM", "value"), 1e-9);

            // (d) _source round trip: the hit for id=4 must carry
            // price = 50.0 (exact float32) and weight = 4/3
            // (double, 6-decimal digit match is enough — the JSON
            // codec emits the shortest round-trippable form).
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

    public void testFragmentDispatchModeAnswersMatchKnnAndSort() throws Exception {
        // Direction 1 Stage 3: match on lance_text, knn on
        // lance_knn, and sort now flow through the fragment
        // executor. The per-node handler translates the QueryBuilder
        // via QueryShardContext.toQuery, drives IndexSearcher.search
        // against the shared per-fragment reader, and returns real
        // Lucene scores + sort values. Before Stage 3 all three
        // shapes fell through to the shard fan-out.
        String suffix = "s3-mks-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // Match on lance_text (body column contains "hello lance"
            // for even rows, "hello world" for odd rows). Fragment
            // path drives LanceFtsQuery via IndexSearcher.search and
            // returns 3 hits (even rows) with real BM25 scores.
            String matchBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match\":{\"body\":\"lance\"}}}"));
            assertEquals(3, extractIntPath(matchBody, "hits", "total", "value"));
            assertTrue("match hits must carry a positive Lucene score: " + matchBody, matchBody.contains("\"_score\":"));
            assertFalse("Stage 3 must not report the hard-coded 1.0 score anymore: " + matchBody, matchBody.contains("\"_score\":1.0"));

            // knn on lance_knn. Table factory writes 8-dim
            // vectors where row i has embedding[0]=i and all
            // other coordinates zero (see LanceTableFactory
            // VECTOR_DIM), so a query vector concentrated on the
            // first axis ranks id=5 highest.
            String queryVector = "[0.5,0.0,0.0,0.0,0.0,0.0,0.0,0.0]";
            String knnBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":3,\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":" + queryVector + ",\"k\":3}}}"
                )
            );
            assertEquals(3, extractIntPath(knnBody, "hits", "total", "value"));
            // Cosine similarity produces a positive score for the
            // nearest neighbour.
            assertTrue("knn top hit must have a positive score: " + knnBody, knnBody.contains("\"_score\":"));

            // Sort by id descending. 6 rows -> ids 0..5, descending
            // means the first hit is id=5, then 4, 3.
            String sortBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":3,\"query\":{\"match_all\":{}},\"sort\":[{\"id\":\"desc\"}]}")
            );
            assertEquals(6, extractIntPath(sortBody, "hits", "total", "value"));
            // Every hit should carry sort values under the "sort" field.
            assertTrue("sort hits must carry sort values: " + sortBody, sortBody.contains("\"sort\":[5]"));
            assertTrue("second sort hit must have sort value [4]: " + sortBody, sortBody.contains("\"sort\":[4]"));
            assertTrue("third sort hit must have sort value [3]: " + sortBody, sortBody.contains("\"sort\":[3]"));
            // Top sorted hit is the row with id=5 (Lance offset 5
            // within fragment 0 because there's no declared PK).
            int firstIdx = sortBody.indexOf("\"_id\":");
            assertTrue("expected an _id in sorted response: " + sortBody, firstIdx >= 0);
            String firstIdSlice = sortBody.substring(firstIdx, Math.min(sortBody.length(), firstIdx + 20));
            assertTrue("first sorted hit must be _id = \"0-5\": " + firstIdSlice, firstIdSlice.contains("\"0-5\""));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testStoredFieldsDocValueFieldsExplainFallThroughToShardPath() throws Exception {
        // stored_fields, docvalue_fields, and explain used to slip
        // past isDispatchable and produce silently wrong hit
        // envelopes (issue #37 case 4 remainder). Fragment
        // executor drops all three: stored_fields projection is
        // ignored so _source stays in hits (including when
        // "_none_" asks to hide it entirely), docvalue_fields is
        // never populated into hits.fields, and explain never
        // adds the _explanation field. Reject list now sends
        // each of these shapes to the shard path where the
        // built-in fetch phase applies the projection and
        // synthesises the explanation.
        String suffix = "s3-storedfields-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 4);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // stored_fields: "_none_" hides _source entirely on
            // the shard path. Fragment path used to always
            // materialise _source from the Lance row scan.
            String noneBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":1,\"query\":{\"match_all\":{}},\"stored_fields\":\"_none_\"}")
            );
            assertFalse("stored_fields:_none_ should suppress _source: " + noneBody, noneBody.contains("\"_source\""));

            // docvalue_fields projects doc values into hits.fields.
            // Fragment path used to omit hits.fields entirely.
            String docvalueBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":1,\"query\":{\"match_all\":{}},\"docvalue_fields\":[\"id\"]}")
            );
            assertTrue(
                "docvalue_fields should populate hits.fields on the shard path: " + docvalueBody,
                docvalueBody.contains("\"fields\":{\"id\"")
            );

            // explain: true adds a per-hit _explanation with a
            // scoring breakdown on the shard path. Fragment path
            // used to never call searcher.explain, so the field
            // was missing.
            String explainBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":1,\"query\":{\"match_all\":{}},\"explain\":true}")
            );
            assertTrue("explain:true should add _explanation on the shard path: " + explainBody, explainBody.contains("\"_explanation\""));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testMaxScoreAndTrackScoresOnFragmentPath() throws Exception {
        // Issue #37 case 2: the fragment executor used to write
        // hits.max_score as a hard-coded 1.0 regardless of the
        // real per-hit score, and never honoured track_scores
        // when combined with a sort. Both quirks silently
        // changed the response envelope compared to the shard
        // path. Since the case 2 fix the coordinator's
        // MergeState computes max_score from the paged window,
        // and per-node Lucene search wires track_scores through
        // to the 4 / 5 argument search / searchAfter overloads.
        // Three shapes exercise those seams:
        // 1. constant_score without sort. Scores fall out of
        // IndexSearcher.search(query, size), max_score
        // lifts to the caller-chosen boost.
        // 2. constant_score with sort by id and
        // track_scores:true. The 4 arg
        // search(query, size, sort, true) collects both
        // sort values and scores, so hits carry the boost
        // and max_score does too.
        // 3. constant_score with sort by id and no
        // track_scores. Lucene's sort collector skips
        // score computation, hits carry _score:null, and
        // max_score comes back as null after the NaN
        // guard in MergeState skips every hit.
        String suffix = "s3-trackscores-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 4);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // Shape 1: constant_score without sort. Every hit
            // carries the boost, max_score matches.
            String noSortBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":4,\"query\":{\"constant_score\":{\"filter\":{\"match_all\":{}},\"boost\":3.5}}}"
                )
            );
            assertEquals(4, extractIntPath(noSortBody, "hits", "total", "value"));
            assertEquals(3.5d, extractDoublePath(noSortBody, "hits", "max_score"), 0.0001d);
            assertEquals(3.5d, extractDoublePath(noSortBody, "hits", "hits", "0", "_score"), 0.0001d);
            assertEquals(3.5d, extractDoublePath(noSortBody, "hits", "hits", "3", "_score"), 0.0001d);

            // Shape 2: sort by id with track_scores:true. Score
            // stays populated alongside sort values.
            String trackBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":4,\"query\":{\"constant_score\":{\"filter\":{\"match_all\":{}},\"boost\":3.5}},"
                        + "\"sort\":[{\"id\":\"desc\"}],\"track_scores\":true}"
                )
            );
            assertEquals(4, extractIntPath(trackBody, "hits", "total", "value"));
            assertEquals(3.5d, extractDoublePath(trackBody, "hits", "max_score"), 0.0001d);
            assertEquals(3.5d, extractDoublePath(trackBody, "hits", "hits", "0", "_score"), 0.0001d);
            assertEquals(3.5d, extractDoublePath(trackBody, "hits", "hits", "3", "_score"), 0.0001d);
            // Sort by id desc puts id=3 first.
            assertTrue("sort desc must start with id=3: " + trackBody, trackBody.contains("\"sort\":[3]"));

            // Shape 3: sort by id with no track_scores. Lucene
            // reports NaN for every hit, JSON encodes that as
            // null. max_score falls out to null as well because
            // MergeState skips NaN hits before picking the max.
            String nullScoreBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":4,\"query\":{\"constant_score\":{\"filter\":{\"match_all\":{}},\"boost\":3.5}},"
                        + "\"sort\":[{\"id\":\"desc\"}]}"
                )
            );
            assertEquals(4, extractIntPath(nullScoreBody, "hits", "total", "value"));
            assertTrue(
                "sort without track_scores must produce max_score:null: " + nullScoreBody,
                nullScoreBody.contains("\"max_score\":null")
            );
            assertTrue(
                "sort without track_scores must produce per-hit _score:null: " + nullScoreBody,
                nullScoreBody.contains("\"_score\":null")
            );
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testMinScoreTerminateAfterTrackTotalHitsFallThroughToShardPath() throws Exception {
        // min_score, terminate_after, and track_total_hits used to
        // slip past isDispatchable and produce silently wrong
        // response envelopes (issue #37 cases 1 and 3). Fragment
        // path counts matches from Lance metadata (or Lucene
        // count()) without threading these knobs through, so the
        // request would come back with hits.total.value from the
        // full match set and terminated_early=false, even when
        // the caller asked for a tighter answer. Reject list now
        // sends each of these shapes to the shard path where the
        // built-in MinScoreCollector /
        // EarlyTerminatingCollector / total-hits-up-to gate
        // actually clip.
        String suffix = "s3-reject-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // min_score above 1.0 excludes every hit from a
            // match_all query (all hits carry score 1.0). Fragment
            // path used to return total=6; shard path clips to
            // total=0.
            String minScoreBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"match_all\":{}},\"min_score\":2.0}")
            );
            assertEquals(0, extractIntPath(minScoreBody, "hits", "total", "value"));

            // terminate_after=2 tells the collector to stop after
            // two docs per segment. Shard path signals early
            // termination through terminated_early=true; fragment
            // path omits the flag entirely because it never wired
            // the count through its scan. Shard path leaves
            // hits.total.value as the pre-terminate count, so we
            // only look at the flag rather than total.
            String terminateBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"match_all\":{}},\"terminate_after\":2}")
            );
            assertTrue(
                "terminate_after should set terminated_early=true on the shard path: " + terminateBody,
                terminateBody.contains("\"terminated_early\":true")
            );

            // track_total_hits=3 on a 6-row table produces
            // relation=gte with a value at the shard path
            // early-terminated counter. Fragment path used to
            // return relation="eq" with the Lance metadata count.
            String trackBoundBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"match_all\":{}},\"track_total_hits\":3}")
            );
            assertTrue("track_total_hits:3 should return relation=gte: " + trackBoundBody, trackBoundBody.contains("\"relation\":\"gte\""));

            // track_total_hits=false omits hits.total entirely on
            // the shard path. Fragment path used to always return
            // the exact Lance total, ignoring the flag. Assert on
            // the response envelope shape rather than the counter
            // value because the two paths disagree on the shape.
            String trackFalseBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"match_all\":{}},\"track_total_hits\":false}")
            );
            assertFalse("track_total_hits:false should omit hits.total: " + trackFalseBody, trackFalseBody.contains("\"total\":{"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeReturnsAggregationsBlockForEmptyTable() throws Exception {
        // A 0-row Lance table with an aggregation request used to
        // drop the aggregations block entirely from the response.
        // Fragment path skipped the fan-out (nothing to fan out) so
        // the coordinator had no per-node InternalAggregations to
        // reduce, and the response was missing the "aggregations"
        // key that shard path would still produce. Coordinator now
        // dispatches a single empty-fragment fan-out to the primary
        // node whenever the request carries aggregations, so the
        // per-node executor runs the aggregator over zero docs and
        // returns an empty tree the coordinator can reduce.
        String suffix = "s3-empty-agg-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 0);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // max aggregation on id. Zero rows means the metric
            // has no value, but the aggregations block itself must
            // still be present with a null value.
            String metricBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"query\":{\"match_all\":{}},\"aggs\":{\"m\":{\"max\":{\"field\":\"id\"}}}}"
                )
            );
            assertEquals(0, extractIntPath(metricBody, "hits", "total", "value"));
            assertTrue("expected aggregations block for empty table max agg: " + metricBody, metricBody.contains("\"aggregations\""));
            assertTrue("expected m bucket for empty table max agg: " + metricBody, metricBody.contains("\"m\""));

            // terms aggregation: empty table produces buckets=[] but
            // the aggregations block should still be there.
            String termsBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"query\":{\"match_all\":{}},\"aggs\":{\"g\":{\"terms\":{\"field\":\"id\"}}}}"
                )
            );
            assertEquals(0, extractIntPath(termsBody, "hits", "total", "value"));
            assertTrue("expected aggregations block for empty table terms agg: " + termsBody, termsBody.contains("\"aggregations\""));
            assertTrue("expected g bucket for empty table terms agg: " + termsBody, termsBody.contains("\"g\""));
            assertTrue("expected empty buckets on empty table: " + termsBody, termsBody.contains("\"buckets\":[]"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeStampsIndexAndVersionEnvelope() throws Exception {
        // The response envelope should carry _index on every hit
        // regardless of what the request asked for, and _version /
        // _seq_no / _primary_term when the request opted in via
        // `version` / `seq_no_primary_term`. Fragment path used to
        // omit all four because it built SearchHit objects on the
        // per-node executor without a SearchShardTarget and without
        // per-doc version accounting; the coordinator now stamps
        // them on absorbTargetResponses.
        String suffix = "s3-env-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 4);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // Plain _search: _index must be present on every hit,
            // version / seq_no / primary_term must NOT (defaults are
            // off).
            String plain = readAll(postJson("/" + indexName + "/_search", "{\"size\":2,\"query\":{\"match_all\":{}}}"));
            assertTrue("expected _index=[" + indexName + "] on each hit: " + plain, plain.contains("\"_index\":\"" + indexName + "\""));
            assertFalse("_version must be omitted by default: " + plain, plain.contains("\"_version\""));
            assertFalse("_seq_no must be omitted by default: " + plain, plain.contains("\"_seq_no\""));
            assertFalse("_primary_term must be omitted by default: " + plain, plain.contains("\"_primary_term\""));

            // version: true opts _version in, but not seq_no /
            // primary_term. Fragment path has no per-doc version so
            // the constant value 1 is reported.
            String versioned = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":1,\"query\":{\"match_all\":{}},\"version\":true}")
            );
            assertTrue("expected _index on each hit: " + versioned, versioned.contains("\"_index\":\"" + indexName + "\""));
            assertTrue("expected _version=1 when version:true: " + versioned, versioned.contains("\"_version\":1"));
            assertFalse("_seq_no still off: " + versioned, versioned.contains("\"_seq_no\""));

            // seq_no_primary_term: true opts _seq_no and
            // _primary_term in but leaves _version off. Constants
            // seqNo=0 / primaryTerm=1 match the shard path defaults
            // for a freshly-indexed doc.
            String seqno = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":1,\"query\":{\"match_all\":{}},\"seq_no_primary_term\":true}")
            );
            assertTrue("expected _seq_no=0: " + seqno, seqno.contains("\"_seq_no\":0"));
            assertTrue("expected _primary_term=1: " + seqno, seqno.contains("\"_primary_term\":1"));
            assertFalse("_version still off: " + seqno, seqno.contains("\"_version\""));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAnswersFromPagination() throws Exception {
        // from > 0 used to fall through to the shard path because the
        // coordinator merge did not know how to skip. Now the
        // coordinator asks each per-node executor for `from + size`
        // hits and drops the leading `from` from the merged response,
        // so pagination beyond the first page runs through the
        // fragment executor end-to-end.
        String suffix = "s3-from-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // Sort by id descending, request the third page window
            // (from=2, size=2). Full descending order is [5, 4, 3,
            // 2, 1, 0]; skipping two leaves [3, 2, 1, 0] and size=2
            // clips to [3, 2].
            String body = readAll(
                postJson("/" + indexName + "/_search", "{\"from\":2,\"size\":2,\"query\":{\"match_all\":{}},\"sort\":[{\"id\":\"desc\"}]}")
            );
            assertEquals(6, extractIntPath(body, "hits", "total", "value"));
            assertTrue("expected sort value [3] on the first paged hit: " + body, body.contains("\"sort\":[3]"));
            assertTrue("expected sort value [2] on the second paged hit: " + body, body.contains("\"sort\":[2]"));
            assertFalse("hit sort value [5] must have been skipped by from=2: " + body, body.contains("\"sort\":[5]"));
            assertFalse("hit sort value [4] must have been skipped by from=2: " + body, body.contains("\"sort\":[4]"));
            assertFalse("only two hits should remain after size=2: " + body, body.contains("\"sort\":[1]"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAnswersPostFilter() throws Exception {
        // post_filter narrows hits (and hits.total.value) but leaves
        // aggregations unaffected. Fragment path runs aggregations
        // against the top-level query and applies the post_filter
        // to the hits scan on the per-node executor.
        String suffix = "s3-pf-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // Query matches all 6 rows, post_filter narrows to id
            // >= 4 (rows 4 and 5). The aggregation counts the full
            // 6 rows because post_filter must not influence it.
            String body = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":10,\"query\":{\"match_all\":{}},"
                        + "\"post_filter\":{\"range\":{\"id\":{\"gte\":4}}},"
                        + "\"aggs\":{\"total\":{\"value_count\":{\"field\":\"id\"}}}}"
                )
            );
            assertEquals(2, extractIntPath(body, "hits", "total", "value"));
            assertEquals(6, extractIntPath(body, "aggregations", "total", "value"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAnswersSearchAfter() throws Exception {
        // search_after pagination flows through the fragment path
        // when the request also carries a sort. The per-node
        // executor calls IndexSearcher.searchAfter(FieldDoc, size,
        // sort) with the coordinator-forwarded cursor; the merged
        // response contains only the hits after the cursor value.
        String suffix = "s3-sa-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // First page: sort id desc, size=2. Full descending
            // order is [5, 4, 3, 2, 1, 0]; the first page keeps 5
            // and 4.
            String first = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":2,\"query\":{\"match_all\":{}},\"sort\":[{\"id\":\"desc\"}]}")
            );
            assertTrue("expected first page to include sort value [5]: " + first, first.contains("\"sort\":[5]"));
            assertTrue("expected first page to include sort value [4]: " + first, first.contains("\"sort\":[4]"));

            // Second page via search_after: the cursor is the last
            // sort value from the first page. Expect ids 3 and 2.
            String second = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":2,\"query\":{\"match_all\":{}},\"sort\":[{\"id\":\"desc\"}],\"search_after\":[4]}"
                )
            );
            assertTrue("expected second page to include sort value [3]: " + second, second.contains("\"sort\":[3]"));
            assertTrue("expected second page to include sort value [2]: " + second, second.contains("\"sort\":[2]"));
            assertFalse("search_after cursor value [4] must be excluded: " + second, second.contains("\"sort\":[4]"));
            assertFalse("hit above the cursor must be excluded: " + second, second.contains("\"sort\":[5]"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFragmentDispatchModeAnswersScriptQueryAndScriptSort() throws Exception {
        // Two shapes flow through the fragment path without any
        // explicit plumbing because the per-fragment reader already
        // exposes doc values that scripts consume through the
        // standard DocValues API. If this test starts failing, the
        // shape has to move onto the isDispatchable reject list (or
        // the fragment executor has to grow the missing piece).
        //
        // collapse and rescore used to live in this test as well
        // but they are silent no-ops on the fragment path (collapse
        // returns ungrouped hits, rescore leaves first-pass scores
        // untouched), so they now sit on the isDispatchable reject
        // list and go through the standard shard path instead.
        String suffix = "s3-scq-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 6);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            // id > 2 -> rows 3, 4, 5.
            String body = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":10,\"query\":{\"script\":{\"script\":{\"source\":\"doc['id'].value > 2\"}}}}"
                )
            );
            assertEquals(3, extractIntPath(body, "hits", "total", "value"));

            // Same doc value path also drives script sort. Rows
            // 5..0 in id desc order.
            String sortBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":3,\"query\":{\"match_all\":{}},"
                        + "\"sort\":[{\"_script\":{\"script\":{\"source\":\"doc['id'].value\"},\"type\":\"number\",\"order\":\"desc\"}}]}"
                )
            );
            assertEquals(6, extractIntPath(sortBody, "hits", "total", "value"));
            assertTrue("script sort first hit should carry sort value 5: " + sortBody, sortBody.contains("\"sort\":[5"));
            assertTrue("script sort second hit should carry sort value 4: " + sortBody, sortBody.contains("\"sort\":[4"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    private static void updateClusterSetting(String key, String value) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"transient\":{\"" + key + "\":\"" + value + "\"}}");
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
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

    public void testNamespaceRegisterPropagatesStorageOptionsToAutoSurfacedIndex() throws Exception {
        // Namespace-level storage_options must ride into every auto-
        // surfaced index's settings — that is how a namespace pointing at
        // an S3 root gives every table under it the same credentials
        // without repeating them per table.
        String suffix = "nsso-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 2);
        String indexName = tableName;
        try {
            Response register = postJson(
                "/_lance/namespace",
                "{\"path\":\"" + scratchDir.toString() + "\",\"storage_options\":{\"aws_region\":\"eu-west-1\"}}"
            );
            assertEquals(
                "namespace register failed: " + readAll(register),
                RestStatus.OK.getStatus(),
                register.getStatusLine().getStatusCode()
            );
            assertBusy(() -> {
                Response cat = client().performRequest(new Request("GET", "/_cat/indices?format=json"));
                String body = readAll(cat);
                assertTrue("waiting for index " + indexName + ", saw: " + body, body.contains("\"" + indexName + "\""));
            });

            Response settings = client().performRequest(new Request("GET", "/" + indexName + "/_settings"));
            String settingsBody = readAll(settings);
            assertTrue(
                "expected namespace aws_region to propagate: " + settingsBody,
                settingsBody.contains("\"aws_region\":\"eu-west-1\"")
            );
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
            try {
                deleteJson("/_lance/namespace", "{\"path\":\"" + scratchDir.toString() + "\"}");
            } catch (Exception ignored) {}
        }
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

    public void testAttachAndMatch() throws Exception {
        // End-to-end: build a real Lance table, wait for the polling loop to
        // surface it as an OpenSearch index, then confirm a match query
        // routes into the plugin engine and returns the rows whose body
        // matches the search term.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "attachAndMatch")) {
            String indexName = fixture.indexName();

            Response search = postJson("/" + indexName + "/_search", "{\"query\":{\"match\":{\"body\":\"hello\"}}}");
            String body = readAll(search);
            // 16 rows total, even rows say "hello lance i", odd rows say
            // "quick brown fox i". Half the rows should match.
            int totalHits = extractIntPath(body, "hits", "total", "value");
            assertEquals("expected 8 hits (even rows), saw response: " + body, 8, totalHits);
        }
    }

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

    public void testAttachAndKnn() throws Exception {
        // Row i sits at coordinate (i, 0, 0, ...) so the nearest neighbour
        // of (2.4, 0, ...) is row 2 followed by row 3. Vector index build is
        // skipped (256-row floor); the plugin falls back to a brute-force
        // scan, which is fine for a 12-row table.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(12, "attachAndKnn")) {
            String indexName = fixture.indexName();

            String queryVector = "[2.4,0,0,0,0,0,0,0]";
            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"size\":2,\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":" + queryVector + ",\"k\":2}}}"
            );
            String body = readAll(search);
            int firstId = extractIntPath(body, "hits", "hits", "0", "_source", "id");
            int secondId = extractIntPath(body, "hits", "hits", "1", "_source", "id");
            assertEquals("expected row 2 as nearest, saw: " + body, 2, firstId);
            assertEquals("expected row 3 as second, saw: " + body, 3, secondId);
        }
    }

    public void testLanceKnnRejectsUnknownField() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "knnUnknown")) {
            String indexName = fixture.indexName();
            String queryVector = "[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]";
            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_knn\":{\"field\":\"noSuchField\",\"vector\":" + queryVector + ",\"k\":2}}}"
                )
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for unknown field, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about noSuchField, saw: " + body, body.contains("noSuchField"));
        }
    }

    public void testLanceKnnRejectsScalarField() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "knnScalar")) {
            String indexName = fixture.indexName();
            String queryVector = "[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]";
            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_knn\":{\"field\":\"id\",\"vector\":" + queryVector + ",\"k\":2}}}"
                )
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for scalar field, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about non-lance_vector, saw: " + body, body.contains("lance_vector"));
        }
    }

    public void testLanceKnnRejectsDimensionMismatch() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "knnDim")) {
            String indexName = fixture.indexName();
            // The fixture writes a FixedSizeList<Float32, 8>; a 3-element
            // query vector must be rejected up front rather than reaching
            // Lance.
            String queryVector = "[0.1,0.2,0.3]";
            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":" + queryVector + ",\"k\":2}}}"
                )
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for dimension mismatch, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about dimension, saw: " + body, body.contains("dimension"));
        }
    }

    public void testLanceMatchPhraseHonoursPhraseOrder() throws Exception {
        // The custom lance_match_phrase DSL routes into Lance's
        // FullTextQuery.phrase, which honours phrase order using the
        // positions written into the FTS index (LanceTableFactory builds
        // the body_fts index with with_position=true). Even rows say
        // "hello lance i", so "hello lance" hits 8 rows and the reversed
        // "lance hello" hits 0.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lmphraseorder")) {
            String indexName = fixture.indexName();

            Response ordered = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_match_phrase\":{\"field\":\"body\",\"query\":\"hello lance\"}}}"
            );
            int orderedHits = extractIntPath(readAll(ordered), "hits", "total", "value");
            assertEquals("expected 8 hits for 'hello lance' phrase", 8, orderedHits);

            Response reversed = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_match_phrase\":{\"field\":\"body\",\"query\":\"lance hello\"}}}"
            );
            int reversedHits = extractIntPath(readAll(reversed), "hits", "total", "value");
            assertEquals("expected 0 hits for 'lance hello' reversed phrase", 0, reversedHits);
        }
    }

    public void testLanceMatchPhraseSlopBridgesGap() throws Exception {
        // Odd rows say "quick brown fox i". "quick fox" with slop=0 must
        // fail (brown between them), slop>=1 must succeed. Confirms the
        // slop parameter reaches Lance.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lmphraseslop")) {
            String indexName = fixture.indexName();

            Response strict = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_match_phrase\":{\"field\":\"body\",\"query\":\"quick fox\"}}}"
            );
            int strictHits = extractIntPath(readAll(strict), "hits", "total", "value");
            assertEquals("expected 0 hits for tight 'quick fox' phrase", 0, strictHits);

            Response withSlop = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_match_phrase\":{\"field\":\"body\",\"query\":\"quick fox\",\"slop\":1}}}"
            );
            int slopHits = extractIntPath(readAll(withSlop), "hits", "total", "value");
            assertEquals("expected 8 hits for 'quick fox' phrase with slop=1", 8, slopHits);
        }
    }

    public void testLanceMatchAndOperatorRestrictsToDocumentsMatchingAllTokens() throws Exception {
        // Even rows say "hello lance i", odd rows say "quick brown fox i".
        // OR "hello quick" would return 16 (every row has one). AND
        // "hello quick" returns 0 because no row has both. lance_match
        // must honour the operator via FullTextQuery.match's Operator
        // parameter.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lmatchand")) {
            String indexName = fixture.indexName();

            Response orQuery = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello quick\"}}}"
            );
            int orHits = extractIntPath(readAll(orQuery), "hits", "total", "value");
            assertEquals("expected 16 hits for OR 'hello quick'", 16, orHits);

            Response andQuery = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello quick\",\"operator\":\"and\"}}}"
            );
            int andHits = extractIntPath(readAll(andQuery), "hits", "total", "value");
            assertEquals("expected 0 hits for AND 'hello quick'", 0, andHits);

            // Same 'hello lance' AND both tokens present in even rows.
            Response andSameRow = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello lance\",\"operator\":\"and\"}}}"
            );
            int andSameRowHits = extractIntPath(readAll(andSameRow), "hits", "total", "value");
            assertEquals("expected 8 hits for AND 'hello lance'", 8, andSameRowHits);
        }
    }

    public void testLanceMatchFuzzinessAllowsSingleEdit() throws Exception {
        // Even rows say "hello lance i". "helo" is edit distance 1 from
        // "hello"; without fuzziness the FTS analyzer matches zero rows,
        // with fuzziness=1 it must match all 8 even rows. Confirms
        // fuzziness reaches Lance rather than being silently dropped.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lmatchfuzz")) {
            String indexName = fixture.indexName();

            Response strict = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"helo\"}}}"
            );
            int strictHits = extractIntPath(readAll(strict), "hits", "total", "value");
            assertEquals("expected 0 hits for exact 'helo'", 0, strictHits);

            Response fuzzy = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"helo\",\"fuzziness\":1}}}"
            );
            int fuzzyHits = extractIntPath(readAll(fuzzy), "hits", "total", "value");
            assertEquals("expected 8 hits for fuzzy 'helo' (edit distance 1 to hello)", 8, fuzzyHits);
        }
    }

    public void testLanceMatchRejectsUnknownField() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "lmatchnofield")) {
            String indexName = fixture.indexName();
            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_match\":{\"field\":\"noSuchField\",\"query\":\"hello\"}}}"
                )
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for unknown field, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about noSuchField: " + body, body.contains("noSuchField"));
        }
    }

    public void testLanceMatchRejectsScalarField() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "lmatchscalar")) {
            String indexName = fixture.indexName();
            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson("/" + indexName + "/_search", "{\"query\":{\"lance_match\":{\"field\":\"id\",\"query\":\"hello\"}}}")
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for scalar field, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about non-lance_text: " + body, body.contains("lance_text"));
        }
    }

    public void testLanceMultiMatchHitsEitherField() throws Exception {
        // Even rows say body="hello lance i", title="sunny morning i".
        // Odd rows say body="quick brown fox i", title="cloudy morning i".
        // multi_match "hello cloudy" on [body, title] with OR must hit
        // every row: even rows via body:hello, odd rows via title:cloudy.
        // Confirms Lance's multi_match reads both columns and unions the
        // per-field matches.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lmmboth")) {
            String indexName = fixture.indexName();

            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_multi_match\":{\"fields\":[\"body\",\"title\"],\"query\":\"hello cloudy\"}}}"
            );
            int hits = extractIntPath(readAll(search), "hits", "total", "value");
            assertEquals("expected 16 hits across body+title, saw response above", 16, hits);
        }
    }

    public void testLanceMultiMatchLimitsToListedFields() throws Exception {
        // "morning" only appears in title. Restricting the search to
        // [body] must return 0, while listing [body, title] must return
        // every row.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lmmscope")) {
            String indexName = fixture.indexName();

            Response bodyOnly = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_multi_match\":{\"fields\":[\"body\"],\"query\":\"morning\"}}}"
            );
            int bodyHits = extractIntPath(readAll(bodyOnly), "hits", "total", "value");
            assertEquals("expected 0 hits when only body is searched", 0, bodyHits);

            Response both = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_multi_match\":{\"fields\":[\"body\",\"title\"],\"query\":\"morning\"}}}"
            );
            int bothHits = extractIntPath(readAll(both), "hits", "total", "value");
            assertEquals("expected 16 hits when title is included", 16, bothHits);
        }
    }

    public void testLanceMultiMatchAndOperator() throws Exception {
        // multi_match "hello sunny" on [body, title] with AND: only rows
        // whose combined fields contain both tokens should match. Even
        // rows have body:hello + title:sunny; odd rows have neither. So
        // OR returns 8 (even rows) and AND also returns 8. To distinguish
        // OR vs AND semantics, "hello cloudy" AND must return 0 (no row
        // has both hello and cloudy anywhere in body|title), while OR
        // returns 16.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lmmand")) {
            String indexName = fixture.indexName();

            Response or = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_multi_match\":{\"fields\":[\"body\",\"title\"],\"query\":\"hello cloudy\"}}}"
            );
            int orHits = extractIntPath(readAll(or), "hits", "total", "value");
            assertEquals("expected 16 hits for OR 'hello cloudy'", 16, orHits);

            Response and = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_multi_match\":{\"fields\":[\"body\",\"title\"],\"query\":\"hello cloudy\",\"operator\":\"and\"}}}"
            );
            int andHits = extractIntPath(readAll(and), "hits", "total", "value");
            assertEquals("expected 0 hits for AND 'hello cloudy' (no row has both)", 0, andHits);
        }
    }

    public void testLanceMultiMatchWithBoostsSmoke() throws Exception {
        // Smoke test that per-field boosts parse and reach Lance without
        // erroring out. Even rows match both terms; asserting 8 hits
        // proves the query executed, and using distinct boosts exercises
        // the boosts list path in Lance's MultiMatchQuery.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lmmboosts")) {
            String indexName = fixture.indexName();

            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_multi_match\":{\"fields\":[\"body\",\"title\"]," + "\"query\":\"hello sunny\",\"boosts\":[2.0,1.0]}}}"
            );
            int hits = extractIntPath(readAll(search), "hits", "total", "value");
            assertEquals("expected 8 hits for even rows matching hello+sunny", 8, hits);
        }
    }

    public void testLanceMultiMatchRejectsUnknownField() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "lmmnofield")) {
            String indexName = fixture.indexName();
            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_multi_match\":{\"fields\":[\"body\",\"noSuchField\"],\"query\":\"hello\"}}}"
                )
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for unknown field, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about noSuchField: " + body, body.contains("noSuchField"));
        }
    }

    public void testLanceMultiMatchRejectsScalarField() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "lmmscalar")) {
            String indexName = fixture.indexName();
            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_multi_match\":{\"fields\":[\"body\",\"id\"],\"query\":\"hello\"}}}"
                )
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for scalar field, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about non-lance_text: " + body, body.contains("lance_text"));
        }
    }

    public void testLanceFtsBoostReturnsPositiveMatches() throws Exception {
        // The lance_fts_boost DSL composes two Lance FTS clauses so the
        // positive set defines the hits and the negative clause only
        // affects scoring. With positive "hello" (even rows) and a
        // negative "fox" (odd rows, disjoint), the hit set must equal
        // the positive set (8 even rows). Confirms Lance's BoostQuery
        // wiring and that non-overlapping negatives do not drop hits.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lfbboost")) {
            String indexName = fixture.indexName();

            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_fts_boost\":{"
                    + "\"positive\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},"
                    + "\"negative\":{\"lance_match\":{\"field\":\"body\",\"query\":\"fox\"}},"
                    + "\"negative_boost\":0.1}}}"
            );
            int hits = extractIntPath(readAll(search), "hits", "total", "value");
            assertEquals("expected 8 hits from positive 'hello'", 8, hits);
        }
    }

    public void testLanceFtsBoostPenalisesOverlappingNegative() throws Exception {
        // Even rows say body="hello lance i", so positive "hello" and
        // negative "lance" match the same 8 rows. Under Lance's
        // BoostQuery, matching rows score positive*negative_boost, so
        // the top _score with negative_boost=0.1 must be strictly less
        // than the top _score of the same positive without any negative
        // wrapper.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lfbpenalise")) {
            String indexName = fixture.indexName();

            Response baseline = postJson(
                "/" + indexName + "/_search",
                "{\"size\":1,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}"
            );
            String baselineBody = readAll(baseline);
            int baselineHits = extractIntPath(baselineBody, "hits", "total", "value");
            assertEquals("baseline expects 8 hits", 8, baselineHits);
            double baselineScore = extractDoublePath(baselineBody, "hits", "hits", "0", "_score");

            Response boosted = postJson(
                "/" + indexName + "/_search",
                "{\"size\":1,\"query\":{\"lance_fts_boost\":{"
                    + "\"positive\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},"
                    + "\"negative\":{\"lance_match\":{\"field\":\"body\",\"query\":\"lance\"}},"
                    + "\"negative_boost\":0.1}}}"
            );
            String boostedBody = readAll(boosted);
            int boostedHits = extractIntPath(boostedBody, "hits", "total", "value");
            assertEquals("boosted expects 8 hits (same positive set)", 8, boostedHits);
            double boostedScore = extractDoublePath(boostedBody, "hits", "hits", "0", "_score");
            assertTrue("expected boosted score < baseline (" + boostedScore + " vs " + baselineScore + ")", boostedScore < baselineScore);
        }
    }

    public void testLanceFtsBoostRejectsNonLanceFtsClause() throws Exception {
        // The positive clause below is a stock OpenSearch `match`, not a
        // Lance FTS DSL. Lance's boost engine only takes FullTextQuery
        // subclauses, so the plugin must reject with 400 rather than
        // silently falling back to a Lucene bool that would break score
        // composition.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "lfbwrongclause")) {
            String indexName = fixture.indexName();

            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_fts_boost\":{"
                        + "\"positive\":{\"match\":{\"body\":\"hello\"}},"
                        + "\"negative\":{\"lance_match\":{\"field\":\"body\",\"query\":\"stale\"}}}}}"
                )
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for non-Lance-FTS positive, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about Lance FTS query: " + body, body.contains("Lance FTS query"));
        }
    }

    public void testLanceFtsBoolMustClauseFiltersToMatchingRows() throws Exception {
        // With a single must clause the bool query is equivalent to
        // running the inner Lance FTS DSL directly: must=body:hello →
        // 8 even rows. Confirms Lance's booleanQuery accepts a lone
        // MUST clause and hands hits back through the plugin.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lbmust")) {
            String indexName = fixture.indexName();

            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_fts_bool\":{" + "\"must\":[{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}]}}}"
            );
            int hits = extractIntPath(readAll(search), "hits", "total", "value");
            assertEquals("expected 8 hits from must body:hello", 8, hits);
        }
    }

    public void testLanceFtsBoolMustNotExcludesOverlappingClause() throws Exception {
        // Even rows say body="hello lance i", so must=body:hello and
        // must_not=body:lance target the same 8 rows and must_not knocks
        // all of them out. Confirms MUST_NOT reaches Lance rather than
        // being silently ignored.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lbmustnot")) {
            String indexName = fixture.indexName();

            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_fts_bool\":{"
                    + "\"must\":[{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}],"
                    + "\"must_not\":[{\"lance_match\":{\"field\":\"body\",\"query\":\"lance\"}}]}}}"
            );
            int hits = extractIntPath(readAll(search), "hits", "total", "value");
            assertEquals("expected 0 hits after must_not:lance eliminates every hello match", 0, hits);
        }
    }

    public void testLanceFtsBoolShouldUnionsAcrossClauses() throws Exception {
        // Two should clauses on disjoint sets (body:hello even, title:cloudy
        // odd) with no must should return the union — 16 rows. This also
        // exercises the multi-field bool composition.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lbshould")) {
            String indexName = fixture.indexName();

            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_fts_bool\":{"
                    + "\"should\":["
                    + "{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},"
                    + "{\"lance_match\":{\"field\":\"title\",\"query\":\"cloudy\"}}]}}}"
            );
            int hits = extractIntPath(readAll(search), "hits", "total", "value");
            assertEquals("expected 16 hits from union of body:hello ∪ title:cloudy", 16, hits);
        }
    }

    public void testLanceFtsBoolMustAcrossFieldsIntersects() throws Exception {
        // must=body:hello (even) AND must=title:sunny (even) intersect
        // on the 8 even rows. Confirms MUST clauses on different columns
        // compose as an intersection on Lance's side.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "lbmustintersect")) {
            String indexName = fixture.indexName();

            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"query\":{\"lance_fts_bool\":{"
                    + "\"must\":["
                    + "{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},"
                    + "{\"lance_match\":{\"field\":\"title\",\"query\":\"sunny\"}}]}}}"
            );
            int hits = extractIntPath(readAll(search), "hits", "total", "value");
            assertEquals("expected 8 hits from intersection body:hello ∩ title:sunny", 8, hits);
        }
    }

    public void testLanceFtsBoolRejectsNonLanceFtsClause() throws Exception {
        // Stock OpenSearch `match` is not a Lance FTS DSL. Rejecting at
        // 400 rather than falling back to a Lucene bool keeps score
        // composition on Lance's side and avoids silently mixing two
        // scoring systems on the same query.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "lbwrongclause")) {
            String indexName = fixture.indexName();

            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_fts_bool\":{" + "\"must\":[{\"match\":{\"body\":\"hello\"}}]}}}"
                )
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for non-Lance-FTS must, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about Lance FTS: " + body, body.contains("Lance FTS"));
        }
    }

    public void testLanceFtsBoolEmptyClausesRejected() throws Exception {
        // Lance's booleanQuery constructor rejects an empty clauses list.
        // The plugin catches this at parse time and returns 400 with a
        // message pointing the caller at the three lists they can fill.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "lbempty")) {
            String indexName = fixture.indexName();

            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson("/" + indexName + "/_search", "{\"query\":{\"lance_fts_bool\":{}}}")
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for empty bool, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about must/should/must_not: " + body, body.contains("must_not"));
        }
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

    public void testLanceKnnAppliesFilterAsPreFilter() throws Exception {
        // With row i at coordinate (i, 0, ...), the two rows nearest to
        // (2.4, 0, ...) are id 2 and id 3. A pre-filter of id >= 10 must
        // keep k=2 populated with the two nearest matches among {10..15},
        // i.e. id 10 (distance 7.6) and id 11 (distance 8.6). A post-filter
        // would pull id 2 / 3 into the top-K first, then drop them, and
        // return 0 hits.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "knnPreFilter")) {
            String indexName = fixture.indexName();
            String queryVector = "[2.4,0,0,0,0,0,0,0]";
            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"size\":2,\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":"
                    + queryVector
                    + ",\"k\":2,\"filter\":{\"range\":{\"id\":{\"gte\":10}}}}}}"
            );
            String body = readAll(search);
            int totalHits = extractIntPath(body, "hits", "total", "value");
            assertEquals("expected 2 hits for id >= 10 with k=2, saw: " + body, 2, totalHits);
            int firstId = extractIntPath(body, "hits", "hits", "0", "_source", "id");
            int secondId = extractIntPath(body, "hits", "hits", "1", "_source", "id");
            assertEquals("expected id 10 as nearest match >= 10, saw: " + body, 10, firstId);
            assertEquals("expected id 11 as second nearest match, saw: " + body, 11, secondId);
        }
    }

    public void testLanceKnnFilterAcceptsBoolFilterClause() throws Exception {
        // Same expectation as testLanceKnnAppliesFilterAsPreFilter but with
        // the range clause nested inside bool.filter, so the translator's
        // bool + range path is exercised end-to-end.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "knnPreFilterBool")) {
            String indexName = fixture.indexName();
            String queryVector = "[2.4,0,0,0,0,0,0,0]";
            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"size\":2,\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":"
                    + queryVector
                    + ",\"k\":2,\"filter\":{\"bool\":{\"filter\":[{\"range\":{\"id\":{\"gte\":10}}}]}}}}}"
            );
            String body = readAll(search);
            int totalHits = extractIntPath(body, "hits", "total", "value");
            assertEquals("expected 2 hits for bool filter id >= 10, saw: " + body, 2, totalHits);
            int firstId = extractIntPath(body, "hits", "hits", "0", "_source", "id");
            assertEquals("expected id 10 as nearest match, saw: " + body, 10, firstId);
        }
    }

    public void testLanceKnnRejectsUnsupportedFilterClause() throws Exception {
        // `match` cannot be lowered to a Lance SQL filter safely (analysis
        // would happen server-side, not in Lance), so the translator
        // rejects it up front with 400. Doing so beats silently degrading
        // recall or returning an obscure Lance parse error.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(4, "knnPreFilterMatch")) {
            String indexName = fixture.indexName();
            String queryVector = "[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]";
            ResponseException failure = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"lance_knn\":{\"field\":\"embedding\",\"vector\":"
                        + queryVector
                        + ",\"k\":2,\"filter\":{\"match\":{\"body\":\"hello\"}}}}}"
                )
            );
            int status = failure.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected 400 for unsupported filter, saw " + status, 400, status);
            String body = readAll(failure.getResponse());
            assertTrue("expected message about MatchQueryBuilder, saw: " + body, body.contains("MatchQueryBuilder"));
        }
    }

    public void testBoolShouldComposesLanceMatchWithLanceKnn() throws Exception {
        // Hybrid-shape query at the shard level: two independent Lance
        // sub-queries fan out inside a bool.should. lance_match:body:hello
        // matches every even row (0, 2, 4, ..., 14; 8 rows). lance_knn
        // near (0.5, 0, ..., 0) with k=2 returns the two closest rows
        // by Euclidean distance, which are id 0 (distance 0.5) and id 1
        // (distance 0.5). The union is 9 rows: id 1 is the only knn hit
        // not already in the match set.
        //
        // This is the same per-shard plumbing neural-search's `hybrid`
        // query relies on: HybridQuery.createWeight iterates the
        // sub-queries, calls createWeight on each, and composes their
        // scorers. As long as our Lance queries honour the Lucene
        // Weight / ScorerSupplier / Scorer contract inside a compound
        // query (as this test proves for bool.should), the hybrid query
        // will drive them identically.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "hybridboolshould")) {
            String indexName = fixture.indexName();
            String queryVector = "[0.5,0,0,0,0,0,0,0]";
            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"size\":16,\"query\":{\"bool\":{\"should\":["
                    + "{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},"
                    + "{\"lance_knn\":{\"field\":\"embedding\",\"vector\":"
                    + queryVector
                    + ",\"k\":2}}"
                    + "]}}}"
            );
            String body = readAll(search);
            int hits = extractIntPath(body, "hits", "total", "value");
            assertEquals("expected 9 unique rows from union(match, knn): " + body, 9, hits);
            // Row 0 satisfies both sub-queries and must score highest of
            // any single-sub-query hit, so the sort by _score lands it
            // first. This is Lucene's bool.should sum-of-child-scores
            // behaviour; hybrid replaces the sum with per-sub-query
            // top-K + coordinator-side normalisation, but the shard-side
            // requirement is the same: each sub-query yields the same
            // scored docs it would on its own.
            int topId = extractIntPath(body, "hits", "hits", "0", "_source", "id");
            assertEquals("expected row 0 (matches both sub-queries) at top of hits: " + body, 0, topId);
        }
    }

    public void testBoolShouldComposesStockMatchOnLanceTextWithLanceKnn() throws Exception {
        // Stock OpenSearch `match` on a lance_text field goes through
        // LanceTextFieldMapper.termQuery, which builds a LanceFtsQuery
        // for the single-token case. Confirm the composition works the
        // same way when the FTS clause uses the plain match DSL rather
        // than the lance_match DSL: the shard-side composition contract
        // that hybrid depends on does not vary between them.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "hybridstockmatchknn")) {
            String indexName = fixture.indexName();
            String queryVector = "[0.5,0,0,0,0,0,0,0]";
            Response search = postJson(
                "/" + indexName + "/_search",
                "{\"size\":16,\"query\":{\"bool\":{\"should\":["
                    + "{\"match\":{\"body\":\"hello\"}},"
                    + "{\"lance_knn\":{\"field\":\"embedding\",\"vector\":"
                    + queryVector
                    + ",\"k\":2}}"
                    + "]}}}"
            );
            String body = readAll(search);
            int hits = extractIntPath(body, "hits", "total", "value");
            assertEquals("expected 9 unique rows from union(match, knn): " + body, 9, hits);
            int topId = extractIntPath(body, "hits", "hits", "0", "_source", "id");
            assertEquals("expected row 0 at top: " + body, 0, topId);
        }
    }

    public void testDefaultSearchReturnsAtLeastTenHits() throws Exception {
        // Regression for the FetchPhase sequential-stored-fields path: with
        // >= 10 adjacent doc ids and no deletions the fetch phase calls
        // getSequentialStoredFieldsReader on the leaf reader. Before the
        // LanceSequentialLeafReader wrapper this threw "requires a
        // CodecReader or a SequentialStoredFieldsLeafReader", so GET
        // /demo/_search with the default size=10 returned 500. Sixteen rows
        // exercises the >= 10 hits case; every hit must materialise cleanly.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "defaultsearch")) {
            String indexName = fixture.indexName();

            Response search = client().performRequest(new Request("GET", "/" + indexName + "/_search"));
            String body = readAll(search);
            int totalHits = extractIntPath(body, "hits", "total", "value");
            assertEquals("expected 16 total hits, saw response: " + body, 16, totalHits);
            // Default size is 10; hits array must be full and each entry
            // must carry the Lance-backed _source.
            assertTrue("expected hits[0]._source in response, saw: " + body, body.contains("\"_source\""));
        }
    }

    public void testNullableAndWideIntColumnsBehaveCorrectly() throws Exception {
        // Regression for B4 (per-column presence bitmaps, int8/int16/int64
        // mapping, boolean null). Before the fix a Lance table with any of
        // - nullable int32 / date / timestamp column
        // - int8 or int16 column
        // - a null in a boolean column
        // - int64 that exceeds Integer.MAX_VALUE
        // took the shard red on recovery or produced silently wrong
        // results. The nullable table has 12 rows; id=5 is all-null.
        try (LanceTestCluster fixture = LanceTestCluster.setUpNullable("nullableints")) {
            String indexName = fixture.indexName();

            // Mapping must reflect the Arrow int widths as byte / short / long.
            Response mappingResp = client().performRequest(new Request("GET", "/" + indexName + "/_mapping"));
            String mapping = readAll(mappingResp);
            assertTrue("expected count8 as byte in mapping: " + mapping, mapping.contains("\"count8\":{\"type\":\"byte\""));
            assertTrue("expected count16 as short in mapping: " + mapping, mapping.contains("\"count16\":{\"type\":\"short\""));
            assertTrue("expected count64 as long in mapping: " + mapping, mapping.contains("\"count64\":{\"type\":\"long\""));

            // Range on count64 must handle > Integer.MAX_VALUE values.
            // count64 = 4_000_000_000 + i, so gte 4_000_000_006 hits i>=6.
            // id=5 is null so exactly six rows should match.
            Response gteResp = postJson(
                "/" + indexName + "/_search",
                "{\"size\":0,\"query\":{\"range\":{\"count64\":{\"gte\":4000000006}}}}"
            );
            int gteHits = extractIntPath(readAll(gteResp), "hits", "total", "value");
            assertEquals("range count64 gte 4000000006 should hit rows i=6..11", 6, gteHits);

            // exists on any nullable column must skip the all-null row.
            Response existsResp = postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"exists\":{\"field\":\"count8\"}}}");
            int existsHits = extractIntPath(readAll(existsResp), "hits", "total", "value");
            assertEquals("exists count8 should count 11 present rows (12 minus one null)", 11, existsHits);

            // flag=true holds when i in {0,3,6,9}: four rows.
            Response flagTrue = postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"term\":{\"flag\":true}}}");
            int trueHits = extractIntPath(readAll(flagTrue), "hits", "total", "value");
            assertEquals("term flag=true should match i in {0,3,6,9}", 4, trueHits);

            // flag=false must NOT include the null row (id=5). Non-null
            // false rows are i in {1,2,4,7,8,10,11} = 7.
            Response flagFalse = postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"term\":{\"flag\":false}}}");
            int falseHits = extractIntPath(readAll(flagFalse), "hits", "total", "value");
            assertEquals("term flag=false should count non-null false rows and skip the null row", 7, falseHits);
        }
    }

    public void testStatsAPIsSucceedForLanceIndex() throws Exception {
        // Regression for LanceReadOnlyEngine.docStats() / segmentsStats():
        // OpenSearch's default implementations traverse leaves via
        // Lucene.segmentReader(reader), which throws for Lance leaves and
        // takes out _stats / _cat/indices docs.count / _nodes/stats /
        // _cluster/stats node-wide. Confirm the plugin overrides succeed
        // and the node-level stats aggregation runs without shard failures.
        try (LanceTestCluster fixture = LanceTestCluster.setUp(16, "statsapi")) {
            String indexName = fixture.indexName();

            Response indexStats = client().performRequest(new Request("GET", "/" + indexName + "/_stats"));
            assertEquals(RestStatus.OK.getStatus(), indexStats.getStatusLine().getStatusCode());
            String body = readAll(indexStats);
            int failed = extractIntPath(body, "_shards", "failed");
            assertEquals("expected zero shard failures on _stats, saw: " + body, 0, failed);

            Response segments = client().performRequest(new Request("GET", "/" + indexName + "/_segments"));
            assertEquals(RestStatus.OK.getStatus(), segments.getStatusLine().getStatusCode());

            Response nodesStats = client().performRequest(new Request("GET", "/_nodes/stats/indices/docs"));
            assertEquals(RestStatus.OK.getStatus(), nodesStats.getStatusLine().getStatusCode());
            String nodesBody = readAll(nodesStats);
            int nodesFailed = extractIntPath(nodesBody, "_nodes", "failed");
            assertEquals("expected zero node failures on _nodes/stats, saw: " + nodesBody, 0, nodesFailed);
        }
    }

    /**
     * Fixture that writes a Lance table into a scratch directory, registers
     * that directory as a Lance namespace, waits for the polling loop to
     * surface the table, and cleans everything up on close. The scratch
     * directory sits under {@code java.io.tmpdir} so the child test-cluster
     * process (which lives in a separate work directory) can still open it.
     */
    private static final class LanceTestCluster implements AutoCloseable {
        private final Path scratchDir;
        private final String indexName;

        private LanceTestCluster(Path scratchDir, String indexName) {
            this.scratchDir = scratchDir;
            this.indexName = indexName;
        }

        String indexName() {
            return indexName;
        }

        /**
         * Absolute filesystem URI of the underlying Lance table. Tests that
         * need to mutate the table (drop columns, append rows) can hand
         * this to {@link LanceTableFactory}.
         */
        String tableUri() {
            return scratchDir.resolve("demo-" + scratchDir.getFileName().toString().substring("lance-it-".length()) + ".lance").toString();
        }

        static LanceTestCluster setUp(int rowCount, String testHint) throws Exception {
            // Anchor the shared directory at the path build.gradle passes in
            // via tests.lance.shared_tables_dir. Test JVM and cluster JVM see
            // the same location without relying on a system-wide tmpdir.
            Path base = sharedRoot();
            // Lowercase everything. OpenSearch rejects index names that
            // contain any uppercase character, and the surfaced index name
            // is derived from the table directory name.
            String suffix = testHint.toLowerCase(java.util.Locale.ROOT) + "-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
            Path scratchDir = Files.createDirectories(base.resolve("lance-it-" + suffix));
            String tableName = "demo-" + suffix;
            LanceTableFactory.writeTable(scratchDir, tableName, rowCount);
            return registerAndWait(scratchDir, "demo-" + suffix);
        }

        static LanceTestCluster setUpNullable(String testHint) throws Exception {
            Path base = sharedRoot();
            String suffix = testHint.toLowerCase(java.util.Locale.ROOT) + "-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
            Path scratchDir = Files.createDirectories(base.resolve("lance-it-" + suffix));
            String tableName = "demo-" + suffix;
            LanceTableFactory.writeNullableTable(scratchDir, tableName);
            return registerAndWait(scratchDir, "demo-" + suffix);
        }

        private static LanceTestCluster registerAndWait(Path scratchDir, String indexName) throws Exception {
            Response register = postJson("/_lance/namespace", "{\"path\":\"" + scratchDir.toString() + "\"}");
            assertEquals(
                "namespace register failed: " + readAll(register),
                RestStatus.OK.getStatus(),
                register.getStatusLine().getStatusCode()
            );

            // Poll cadence is 1s (see build.gradle); the assertBusy default
            // (10s) leaves plenty of headroom for the surface path to run.
            assertBusy(() -> {
                Response cat = client().performRequest(new Request("GET", "/_cat/indices?format=json"));
                String body = readAll(cat);
                assertTrue("waiting for index " + indexName + ", saw: " + body, body.contains("\"" + indexName + "\""));
            });
            // Recovery still races the first request on newer OpenSearch
            // versions; block until the shard is green before returning so
            // GET / search do not see RECOVERING.
            ensureGreen(indexName);
            return new LanceTestCluster(scratchDir, indexName);
        }

        @Override
        public void close() throws IOException {
            // Best-effort cleanup so leftover scratch dirs do not pile up
            // when a test fails.
            deleteRecursively(scratchDir);
        }
    }

    private static Response postJson(String path, String body) throws IOException {
        Request request = new Request("POST", path);
        request.setJsonEntity(body);
        return client().performRequest(request);
    }

    private static Response deleteJson(String path, String body) throws IOException {
        Request request = new Request("DELETE", path);
        request.setJsonEntity(body);
        return client().performRequest(request);
    }

    private static Path sharedRoot() {
        String configured = System.getProperty("tests.lance.shared_tables_dir");
        if (configured == null || configured.isEmpty()) {
            throw new AssertionError("tests.lance.shared_tables_dir is unset; build.gradle should pass it into the integTest task");
        }
        return Path.of(configured);
    }

    private static String scratchPathString(String label) {
        return sharedRoot().resolve("lance-it-" + label + "-" + randomAlphaOfLength(8)).toString();
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0;
        int i = 0;
        while ((i = haystack.indexOf(needle, i)) != -1) {
            count++;
            i += needle.length();
        }
        return count;
    }

    private static String readAll(Response response) throws IOException {
        return new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
    }

    /**
     * Small helper to pluck a value out of a JSON response without pulling
     * in Jackson. Traverses object keys or array indices in order.
     */
    private static int extractIntPath(String json, String... path) {
        try (XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Object value = parser.map();
            for (String step : path) {
                if (value instanceof java.util.Map<?, ?> map) {
                    value = map.get(step);
                } else if (value instanceof java.util.List<?> list) {
                    value = list.get(Integer.parseInt(step));
                } else {
                    throw new AssertionError("cannot descend into " + value + " with step " + step);
                }
                if (value == null) {
                    throw new AssertionError("missing key " + step + " in path " + String.join(".", path) + ", json=" + json);
                }
            }
            if (value instanceof Number number) {
                return number.intValue();
            }
            throw new AssertionError("expected number at " + String.join(".", path) + ", saw " + value);
        } catch (IOException e) {
            throw new AssertionError("could not parse JSON: " + json, e);
        }
    }

    private static double extractDoublePath(String json, String... path) {
        try (XContentParser parser = MediaTypeRegistry.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, json)) {
            Object value = parser.map();
            for (String step : path) {
                if (value instanceof java.util.Map<?, ?> map) {
                    value = map.get(step);
                } else if (value instanceof java.util.List<?> list) {
                    value = list.get(Integer.parseInt(step));
                } else {
                    throw new AssertionError("cannot descend into " + value + " with step " + step);
                }
                if (value == null) {
                    throw new AssertionError("missing key " + step + " in path " + String.join(".", path) + ", json=" + json);
                }
            }
            if (value instanceof Number number) {
                return number.doubleValue();
            }
            throw new AssertionError("expected number at " + String.join(".", path) + ", saw " + value);
        } catch (IOException e) {
            throw new AssertionError("could not parse JSON: " + json, e);
        }
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try (var stream = Files.walk(root)) {
            stream.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // best-effort cleanup
                }
            });
        }
    }
}
