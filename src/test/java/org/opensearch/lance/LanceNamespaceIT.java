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
        // A directory entry emits the path as both its name and its path
        // field, so count registrations by the name field.
        int occurrences = countOccurrences(body, "\"name\":\"" + path + "\"");
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
            base.resolve("not-a-dir-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT) + ".lance")
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
        String suffix = "listtables-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        LanceTableFactory.writeTable(scratchDir, "alpha", 4);
        LanceTableFactory.writeTable(scratchDir, "bravo", 4);
        try {
            Response register = postJson("/_lance/namespace", "{\"path\":\"" + scratchDir.toString() + "\"}");
            assertEquals(RestStatus.OK.getStatus(), register.getStatusLine().getStatusCode());

            Response listing = postJson("/_lance/namespace/tables", "{\"path\":\"" + scratchDir.toString() + "\"}");
            assertEquals(RestStatus.OK.getStatus(), listing.getStatusLine().getStatusCode());
            String body = readAll(listing);
            assertTrue("expected name echo: " + body, body.contains("\"name\":\"" + scratchDir.toString() + "\""));
            assertTrue("expected table alpha in list: " + body, body.contains("\"alpha\""));
            assertTrue("expected table bravo in list: " + body, body.contains("\"bravo\""));
        } finally {
            try {
                deleteJson("/_lance/namespace", "{\"path\":\"" + scratchDir.toString() + "\"}");
            } catch (Exception ignored) {}
        }
    }

    public void testPollTriggerSurfacesATableDroppedIntoTheDirectory() throws Exception {
        // POST /_lance/namespace/_poll runs the listing cycle now: a table
        // written into a registered directory gets its index without
        // waiting for the cadence, and the answer names it. The scheduled
        // cycle (1s here) may surface it first; the trigger then finds the
        // index and reports nothing, and the index exists either way.
        String suffix = "polltrigger-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String first = "first-" + suffix;
        String second = "second-" + suffix;
        LanceTableFactory.writeTable(scratchDir, first, 4);
        try {
            Response register = postJson("/_lance/namespace", "{\"path\":\"" + scratchDir.toString() + "\"}");
            assertEquals(RestStatus.OK.getStatus(), register.getStatusLine().getStatusCode());
            client().performRequest(new Request("GET", "/_cluster/health/" + first + "?wait_for_status=yellow&timeout=30s"));

            LanceTableFactory.writeTable(scratchDir, second, 4);
            Response poll = postJson("/_lance/namespace/_poll?name=" + scratchDir.toString(), "");
            assertEquals(RestStatus.OK.getStatus(), poll.getStatusLine().getStatusCode());
            String body = readAll(poll);
            java.util.Map<String, Object> parsed = parseJson(body);
            assertTrue("surfaced is a list: " + body, parsed.get("surfaced") instanceof List<?>);
            assertTrue("skipped is a list: " + body, parsed.get("skipped") instanceof List<?>);
            assertTrue("unavailable is an object: " + body, parsed.get("unavailable") instanceof java.util.Map<?, ?>);
            assertFalse("an existing index of the table is not a skip: " + body, body.contains("\"index\":\"" + first + "\""));
            Response health = client().performRequest(
                new Request("GET", "/_cluster/health/" + second + "?wait_for_status=yellow&timeout=30s")
            );
            assertEquals("the new table has its index after the trigger: " + readAll(health), 200, health.getStatusLine().getStatusCode());

            // A registration nobody knows: nothing to list, nothing done.
            String unknown = readAll(postJson("/_lance/namespace/_poll?name=/no-such-registration-" + suffix, ""));
            assertEquals("{\"surfaced\":[],\"skipped\":[],\"unavailable\":{}}", unknown);
        } finally {
            for (String index : List.of(first, second)) {
                try {
                    client().performRequest(new Request("DELETE", "/" + index));
                } catch (Exception ignored) {}
            }
            try {
                deleteJson("/_lance/namespace", "{\"path\":\"" + scratchDir.toString() + "\"}");
            } catch (Exception ignored) {}
        }
    }

    public void testPollTriggerReportsANameCollision() throws Exception {
        // An index that exists under the table's name and is not backed by
        // the table is left alone, and the trigger says why.
        String suffix = "pollclash-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String clashing = "clash-" + suffix;
        LanceTableFactory.writeTable(scratchDir, clashing, 4);
        try {
            Request plain = new Request("PUT", "/" + clashing);
            plain.setJsonEntity("{\"settings\":{\"index.number_of_shards\":1,\"index.number_of_replicas\":0}}");
            client().performRequest(plain);
            Response register = postJson("/_lance/namespace", "{\"path\":\"" + scratchDir.toString() + "\"}");
            assertEquals(RestStatus.OK.getStatus(), register.getStatusLine().getStatusCode());

            String body = readAll(postJson("/_lance/namespace/_poll", ""));
            assertTrue("the clashing table is skipped: " + body, body.contains("\"index\":\"" + clashing + "\""));
            assertTrue("with the collision as the reason: " + body, body.contains("name collision"));
            assertTrue("the plain index is untouched: " + body, body.contains("\"surfaced\":[]"));
            String settings = readAll(client().performRequest(new Request("GET", "/" + clashing + "/_settings")));
            assertFalse("the plain index stays plain: " + settings, settings.contains("index.lance.table"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + clashing));
            } catch (Exception ignored) {}
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
        assertTrue("expected [name] required message: " + body, body.contains("[name]"));
    }

    public void testResurfaceGuardHoldsDeletedIndexDuringGrace() throws Exception {
        // An index deleted through DELETE /{index} must stay deleted for
        // the resurface grace period. A short grace keeps the post-grace
        // assertion inside the test's wall-clock budget.
        String suffix = "resurface-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
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
        String suffix = "resurface0-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
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
        String suffix = "nsso-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
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
            // The surface path leaves the replica settings at
            // number_of_replicas 0 like attach; distribution does not
            // rely on replica copies.
            assertFalse("did not expect auto_expand_replicas in settings: " + settingsBody, settingsBody.contains("auto_expand_replicas"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
            try {
                deleteJson("/_lance/namespace", "{\"path\":\"" + scratchDir.toString() + "\"}");
            } catch (Exception ignored) {}
        }
    }

    public void testRegisterNamespaceRejectsUnknownType() throws IOException {
        ResponseException failure = expectThrows(
            ResponseException.class,
            () -> postJson("/_lance/namespace", "{\"type\":\"hive\",\"name\":\"h\"}")
        );
        assertEquals(400, failure.getResponse().getStatusLine().getStatusCode());
        String body = readAll(failure.getResponse());
        assertTrue("expected the accepted values in the message: " + body, body.contains("directory"));
        assertTrue("expected the accepted values in the message: " + body, body.contains("rest"));
        assertTrue("expected the accepted values in the message: " + body, body.contains("glue"));
        assertTrue("expected the accepted values in the message: " + body, body.contains("iceberg"));
        assertTrue("expected the accepted values in the message: " + body, body.contains("polaris"));
        assertTrue("expected the accepted values in the message: " + body, body.contains("unity"));
    }

    public void testRegisterCatalogTypesRequireTheirConfigKeys() throws IOException {
        // iceberg and polaris need an endpoint and a warehouse; unity
        // needs an endpoint and a catalog. Each miss answers 400 naming
        // the key before anything reaches the cluster manager.
        for (String type : List.of("iceberg", "polaris")) {
            ResponseException noEndpoint = expectThrows(
                ResponseException.class,
                () -> postJson("/_lance/namespace", "{\"type\":\"" + type + "\",\"name\":\"c\",\"config\":{\"warehouse\":\"wh\"}}")
            );
            assertEquals(400, noEndpoint.getResponse().getStatusLine().getStatusCode());
            assertTrue(readAll(noEndpoint.getResponse()).contains("[config.endpoint]"));

            ResponseException noWarehouse = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/_lance/namespace",
                    "{\"type\":\"" + type + "\",\"name\":\"c\",\"config\":{\"endpoint\":\"http://127.0.0.1:1\"}}"
                )
            );
            assertEquals(400, noWarehouse.getResponse().getStatusLine().getStatusCode());
            assertTrue(readAll(noWarehouse.getResponse()).contains("[config.warehouse]"));
        }
        ResponseException noCatalog = expectThrows(
            ResponseException.class,
            () -> postJson("/_lance/namespace", "{\"type\":\"unity\",\"name\":\"c\",\"config\":{\"endpoint\":\"http://127.0.0.1:1\"}}")
        );
        assertEquals(400, noCatalog.getResponse().getStatusLine().getStatusCode());
        assertTrue(readAll(noCatalog.getResponse()).contains("[config.catalog]"));
    }

    public void testRegisterRestNamespaceRequiresNameAndUri() throws IOException {
        ResponseException noName = expectThrows(
            ResponseException.class,
            () -> postJson("/_lance/namespace", "{\"type\":\"rest\",\"config\":{\"uri\":\"http://127.0.0.1:1\"}}")
        );
        assertEquals(400, noName.getResponse().getStatusLine().getStatusCode());
        assertTrue(readAll(noName.getResponse()).contains("[name]"));

        ResponseException noUri = expectThrows(
            ResponseException.class,
            () -> postJson("/_lance/namespace", "{\"type\":\"rest\",\"name\":\"cat\"}")
        );
        assertEquals(400, noUri.getResponse().getStatusLine().getStatusCode());
        assertTrue(readAll(noUri.getResponse()).contains("[config.uri]"));

        ResponseException withPath = expectThrows(
            ResponseException.class,
            () -> postJson(
                "/_lance/namespace",
                "{\"type\":\"rest\",\"name\":\"cat\",\"path\":\"/tmp\",\"config\":{\"uri\":\"http://127.0.0.1:1\"}}"
            )
        );
        assertEquals(400, withPath.getResponse().getStatusLine().getStatusCode());
        assertTrue(readAll(withPath.getResponse()).contains("[path]"));
    }

    static void updateClusterSetting(String key, String value) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"transient\":{\"" + key + "\":\"" + value + "\"}}");
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
    }
}
