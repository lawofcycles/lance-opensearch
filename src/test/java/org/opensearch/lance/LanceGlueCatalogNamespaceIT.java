/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.core.rest.RestStatus;

/**
 * End-to-end coverage for the {@code glue} namespace type authenticating
 * through the AWS SDK's default credential chain: the registration
 * carries no static keys, the credentials sit in a shared credentials
 * file under the process user's {@code ~/.aws}, and the cluster reads
 * that file under the Java agent. An in-test HTTP server speaks the
 * Glue JSON protocol for one database holding one Lance table, records
 * the SigV4 {@code Authorization} header of every request so the test
 * can tell which access key signed it, the table surfaces as an index,
 * and a search answers from the Lance data the table's location points
 * at.
 *
 * <p>The credentials file is written by the test JVM at the path
 * {@code build.gradle} hands both JVMs ({@code tests.lance.aws_credentials_file}
 * here, {@code AWS_SHARED_CREDENTIALS_FILE} on the cluster): a project
 * specific sub directory of {@code ~/.aws}, because the plugin policy
 * grants the read under that directory only and the operator's own
 * {@code ~/.aws/credentials} must stay untouched. The file and the
 * directories the test created are removed when the test ends, and by
 * a shutdown hook when the test JVM dies before that.
 *
 * <p>The suppression covers {@code com.sun.net.httpserver}: the test
 * needs an HTTP listener inside the test JVM with no extra dependency,
 * and the JDK's built-in server is the standard choice for that in
 * OpenSearch test code.
 */
@SuppressForbidden(reason = "in-test HTTP server implementing the Glue JSON protocol for the poll to talk to")
public class LanceGlueCatalogNamespaceIT extends LanceRestTestCase {

    public void testGlueNamespaceAuthenticatesFromTheSharedCredentialsFile() throws Exception {
        String configured = System.getProperty("tests.lance.aws_credentials_file");
        assertNotNull("tests.lance.aws_credentials_file is unset; build.gradle should pass it into the integTest task", configured);
        Path credentialsFile = Path.of(configured);

        String suffix = "gluecat-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 4);
        String tableLocation = scratchDir.resolve(tableName + ".lance").toString();

        // A key id only this run uses, so the assertion on the signature
        // cannot be satisfied by a credential from anywhere else.
        String accessKeyId = "AKIALANCEIT" + randomAlphaOfLength(9).toUpperCase(Locale.ROOT);
        String secretAccessKey = randomAlphaOfLength(40);
        List<Path> createdDirectories = writeCredentialsFile(credentialsFile, accessKeyId, secretAccessKey);
        // The file sits under the operator's real ~/.aws, so a test JVM
        // killed between the write and the finally block must not leave
        // it behind: a shutdown hook runs the same cleanup.
        Thread cleanupOnExit = new Thread(() -> {
            try {
                removeCredentialsFile(credentialsFile, createdDirectories);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }, "lance-glue-it-credentials-cleanup");
        Runtime.getRuntime().addShutdownHook(cleanupOnExit);

        List<String> authorizationHeaders = new CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", glueHandler("db1", tableName, tableLocation, authorizationHeaders));
        server.start();
        String namespaceName = "glue-" + suffix;
        try {
            Response register = postJson(
                "/_lance/namespace",
                "{\"type\":\"glue\",\"name\":\""
                    + namespaceName
                    + "\",\"config\":{\"region\":\"us-east-1\",\"endpoint\":\"http://127.0.0.1:"
                    + server.getAddress().getPort()
                    + "\"}}"
            );
            String registerBody = readAll(register);
            assertEquals("register failed: " + registerBody, RestStatus.OK.getStatus(), register.getStatusLine().getStatusCode());
            logger.info("glue registration response: {}", registerBody);

            client().performRequest(new Request("GET", "/_cluster/health/" + tableName + "?wait_for_status=yellow&timeout=60s"));

            assertBusy(() -> {
                Response search = client().performRequest(new Request("GET", "/" + tableName + "/_search?q=*:*"));
                String body = readAll(search);
                assertTrue("expected 4 hits from the glue-surfaced index: " + body, body.contains("\"value\":4"));
            });

            // Every Glue request the cluster sent was signed with the key
            // the credentials file holds: the SDK's chain reached the
            // profile provider and the agent let it read the file.
            assertFalse("the fake Glue endpoint saw no request", authorizationHeaders.isEmpty());
            for (String authorization : authorizationHeaders) {
                assertNotNull("a Glue request arrived unsigned", authorization);
                assertTrue(
                    "expected the request signed with the credentials file's key: " + authorization,
                    authorization.contains("Credential=" + accessKeyId + "/")
                );
            }

            Response listing = client().performRequest(new Request("GET", "/_lance/namespace"));
            String listingBody = readAll(listing);
            logger.info("glue namespace listing: {}", listingBody);
            assertTrue(
                "expected the glue namespace in the listing: " + listingBody,
                listingBody.contains("\"name\":\"" + namespaceName + "\"")
            );
            assertTrue("expected type glue: " + listingBody, listingBody.contains("\"type\":\"glue\""));
            assertTrue("expected available status: " + listingBody, listingBody.contains("\"status\":\"available\""));
            assertFalse("the access key must not appear in the listing: " + listingBody, listingBody.contains(accessKeyId));
            assertFalse("the secret must not appear in the listing: " + listingBody, listingBody.contains(secretAccessKey));

            Response tables = postJson("/_lance/namespace/tables", "{\"name\":\"" + namespaceName + "\"}");
            String tablesBody = readAll(tables);
            logger.info("glue tables preview: {}", tablesBody);
            assertTrue("expected the fixture table in the preview: " + tablesBody, tablesBody.contains("\"" + tableName + "\""));
        } finally {
            server.stop(0);
            try {
                client().performRequest(new Request("DELETE", "/" + tableName));
            } catch (Exception ignored) {}
            try {
                deleteJson("/_lance/namespace", "{\"name\":\"" + namespaceName + "\"}");
            } catch (Exception ignored) {}
            removeCredentialsFile(credentialsFile, createdDirectories);
            Runtime.getRuntime().removeShutdownHook(cleanupOnExit);
        }
    }

    /**
     * Write a {@code [default]} profile with static keys at
     * {@code credentialsFile}, creating the missing parents. Returns the
     * directories this call created, deepest first, so the cleanup
     * removes exactly those and leaves a pre-existing {@code ~/.aws}
     * alone.
     */
    private static List<Path> writeCredentialsFile(Path credentialsFile, String accessKeyId, String secretAccessKey) throws IOException {
        List<Path> created = new ArrayList<>();
        Path parent = credentialsFile.getParent();
        List<Path> missing = new ArrayList<>();
        for (Path dir = parent; dir != null && Files.exists(dir) == false; dir = dir.getParent()) {
            missing.add(dir);
        }
        for (int i = missing.size() - 1; i >= 0; i--) {
            Files.createDirectory(missing.get(i));
            created.add(0, missing.get(i));
        }
        String profile = "[default]\naws_access_key_id = " + accessKeyId + "\naws_secret_access_key = " + secretAccessKey + "\n";
        Files.writeString(credentialsFile, profile, StandardCharsets.UTF_8);
        return created;
    }

    /**
     * Remove the credentials file and the directories
     * {@link #writeCredentialsFile} created. A directory another
     * concurrent run still uses is left in place.
     */
    private static void removeCredentialsFile(Path credentialsFile, List<Path> createdDirectories) throws IOException {
        Files.deleteIfExists(credentialsFile);
        for (Path dir : createdDirectories) {
            try {
                Files.deleteIfExists(dir);
            } catch (DirectoryNotEmptyException stillInUse) {
                return;
            }
        }
    }

    /**
     * Answers the Glue operations the poll's walk hits, dispatched on the
     * {@code X-Amz-Target} header the AWS JSON protocol carries:
     * {@code GetDatabases} (the namespace walk's first level),
     * {@code GetTables} of that database (the table listing, filtered by
     * the client to {@code table_type} lance) and {@code GetTable} (the
     * poll reads the location from it before surfacing). The
     * {@code Authorization} header of every request is recorded for the
     * test's signature assertion.
     */
    private static HttpHandler glueHandler(String databaseName, String tableName, String tableLocation, List<String> authorizationHeaders) {
        String table = "{\"Name\":\""
            + tableName
            + "\",\"DatabaseName\":\""
            + databaseName
            + "\",\"Parameters\":{\"table_type\":\"lance\"},\"StorageDescriptor\":{\"Location\":\""
            + tableLocation
            + "\"}}";
        return exchange -> {
            exchange.getRequestBody().readAllBytes();
            authorizationHeaders.add(exchange.getRequestHeaders().getFirst("Authorization"));
            String target = exchange.getRequestHeaders().getFirst("X-Amz-Target");
            String json;
            int status = 200;
            if ("AWSGlue.GetDatabases".equals(target)) {
                json = "{\"DatabaseList\":[{\"Name\":\"" + databaseName + "\"}]}";
            } else if ("AWSGlue.GetTables".equals(target)) {
                json = "{\"TableList\":[" + table + "]}";
            } else if ("AWSGlue.GetTable".equals(target)) {
                json = "{\"Table\":" + table + "}";
            } else {
                status = 400;
                json = "{\"__type\":\"InvalidInputException\",\"Message\":\"unsupported test operation " + target + "\"}";
            }
            byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/x-amz-json-1.1");
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        };
    }
}
