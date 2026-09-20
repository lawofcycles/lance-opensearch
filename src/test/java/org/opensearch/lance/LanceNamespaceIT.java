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

        Response listing = client().performRequest(new Request("GET", "/_lance/namespace"));
        String body = readAll(listing);
        assertTrue("expected namespace " + path + " in listing, saw: " + body, body.contains(path));
    }

    public void testRegisterNamespaceIsIdempotent() throws IOException {
        // A second POST for the same path is a no-op; a duplicate entry
        // would make the poll loop scan the catalog twice per cycle.
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
        ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/_lance/namespace", "{}"));
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertEquals("expected 400 for missing path, saw " + status, 400, status);
        String body = readAll(failure.getResponse());
        assertTrue("expected message about [path], saw: " + body, body.contains("[path]"));
    }

    public void testRegisterNamespaceRejectsNonStringPath() throws IOException {
        ResponseException failure = expectThrows(ResponseException.class, () -> postJson("/_lance/namespace", "{\"path\":42}"));
        int status = failure.getResponse().getStatusLine().getStatusCode();
        assertEquals("expected 400 for non-string path, saw " + status, 400, status);
        String body = readAll(failure.getResponse());
        assertTrue("expected message about [path], saw: " + body, body.contains("[path]"));
    }

    public void testRegisterNamespaceRejectsNonExistentPath() throws IOException {
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
        // surface without waiting for a poll cycle.
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
        // 404 rather than an empty list, so "not registered" and
        // "registered but empty" stay distinguishable.
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
        // An index deleted through DELETE /{index} must stay deleted for
        // the resurface grace period. A short grace keeps the post-grace
        // assertion inside the test's wall-clock budget.
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

            // Longer than the 1s poll cadence, shorter than the 3s grace.
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

            // After the grace expires the next poll recreates the index.
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
        // Grace 0 disables the tombstone check: the next poll recreates
        // the index immediately.
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
        // Namespace-level storage_options apply to every auto-surfaced
        // index under it, so a namespace pointing at an S3 root carries
        // the credentials once for all of its tables.
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
            // The surface path creates indexes with the same replica
            // expansion as attach, so every data node gets a copy.
            assertTrue(
                "expected index.auto_expand_replicas 0-all in settings: " + settingsBody,
                settingsBody.contains("\"auto_expand_replicas\":\"0-all\"")
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
