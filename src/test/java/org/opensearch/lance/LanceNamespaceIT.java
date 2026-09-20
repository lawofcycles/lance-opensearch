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

/**
 * Namespace registration, listing and unregistration through {@code
 * /_lance/namespace}, the polling loop that surfaces tables as indices, the
 * resurface guard, and storage option propagation to auto-surfaced indices.
 */
public class LanceNamespaceIT extends LanceRestTestCase {

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

    static void updateClusterSetting(String key, String value) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"transient\":{\"" + key + "\":\"" + value + "\"}}");
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
    }
}
