/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.lance.namespace.unity.UnityModels;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.core.rest.RestStatus;

/**
 * End-to-end coverage for the {@code unity} namespace type: an in-test
 * HTTP server speaks the Unity Catalog API for one Lance table inside
 * one schema of the configured catalog, the cluster's bundled
 * UnityNamespace client polls it, and the surfaced index answers a
 * search. Unity's namespace shape is the fixed two-level
 * catalog.schema, which the poll's walk descends without extra config.
 *
 * <p>The suppression covers {@code com.sun.net.httpserver}: the test
 * needs an HTTP listener inside the test JVM with no extra dependency,
 * and the JDK's built-in server is the standard choice for that in
 * OpenSearch test code.
 */
@SuppressForbidden(reason = "in-test HTTP server implementing the Unity Catalog API for the poll to talk to")
public class LanceUnityCatalogNamespaceIT extends LanceRestTestCase {

    public void testUnityNamespaceSurfacesTableAndAnswersSearch() throws Exception {
        String suffix = "unicat-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 4);
        String tableLocation = scratchDir.resolve(tableName + ".lance").toString();

        HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", unityCatalogHandler(tableName, tableLocation));
        server.start();
        String namespaceName = "uni-" + suffix;
        try {
            Response register = postJson(
                "/_lance/namespace",
                "{\"type\":\"unity\",\"name\":\""
                    + namespaceName
                    + "\",\"config\":{\"endpoint\":\"http://127.0.0.1:"
                    + server.getAddress().getPort()
                    + "\",\"catalog\":\"main\",\"auth_token\":\"test-token\"}}"
            );
            assertEquals("register failed: " + readAll(register), RestStatus.OK.getStatus(), register.getStatusLine().getStatusCode());

            client().performRequest(new Request("GET", "/_cluster/health/" + tableName + "?wait_for_status=yellow&timeout=60s"));

            assertBusy(() -> {
                Response search = client().performRequest(new Request("GET", "/" + tableName + "/_search?q=*:*"));
                String body = readAll(search);
                assertTrue("expected 4 hits from the unity-surfaced index: " + body, body.contains("\"value\":4"));
            });

            Response listing = client().performRequest(new Request("GET", "/_lance/namespace"));
            String listingBody = readAll(listing);
            assertTrue(
                "expected the unity namespace in the listing: " + listingBody,
                listingBody.contains("\"name\":\"" + namespaceName + "\"")
            );
            assertTrue("expected type unity: " + listingBody, listingBody.contains("\"type\":\"unity\""));
            assertTrue("expected available status: " + listingBody, listingBody.contains("\"status\":\"available\""));
            assertFalse("bearer token must not appear in the listing: " + listingBody, listingBody.contains("test-token"));

            Response tables = postJson("/_lance/namespace/tables", "{\"name\":\"" + namespaceName + "\"}");
            String tablesBody = readAll(tables);
            assertTrue("expected the fixture table in the preview: " + tablesBody, tablesBody.contains("\"" + tableName + "\""));
        } finally {
            server.stop(0);
            try {
                client().performRequest(new Request("DELETE", "/" + tableName));
            } catch (Exception ignored) {}
            try {
                deleteJson("/_lance/namespace", "{\"name\":\"" + namespaceName + "\"}");
            } catch (Exception ignored) {}
        }
    }

    /**
     * Answers the Unity Catalog endpoints the poll's walk hits under
     * the client's default {@code /api/2.1/unity-catalog} base path:
     * the schema listing of the configured catalog, the table listing
     * of the schema (the client filters on the {@code table_type}
     * property client-side), and the per-table load the poll reads the
     * storage location from. The response JSON is produced by
     * serialising the bundled client's own model classes.
     */
    private static HttpHandler unityCatalogHandler(String tableName, String tableLocation) {
        ObjectMapper mapper = new ObjectMapper();
        return exchange -> {
            String path = URLDecoder.decode(exchange.getRequestURI().getPath(), StandardCharsets.UTF_8);
            exchange.getRequestBody().readAllBytes();
            String json;
            if (path.equals("/api/2.1/unity-catalog/schemas")) {
                UnityModels.SchemaInfo schema = new UnityModels.SchemaInfo();
                schema.setName("default");
                schema.setCatalogName("main");
                UnityModels.ListSchemasResponse response = new UnityModels.ListSchemasResponse();
                response.setSchemas(List.of(schema));
                json = mapper.writeValueAsString(response);
            } else if (path.equals("/api/2.1/unity-catalog/tables")) {
                UnityModels.ListTablesResponse response = new UnityModels.ListTablesResponse();
                response.setTables(List.of(unityTable(tableName, tableLocation)));
                json = mapper.writeValueAsString(response);
            } else if (path.equals("/api/2.1/unity-catalog/tables/main.default." + tableName)) {
                json = mapper.writeValueAsString(unityTable(tableName, tableLocation));
            } else {
                byte[] notFound = "{\"error\":\"unsupported test endpoint\"}".getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(404, notFound.length);
                exchange.getResponseBody().write(notFound);
                exchange.close();
                return;
            }
            byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        };
    }

    private static UnityModels.TableInfo unityTable(String tableName, String tableLocation) {
        UnityModels.TableInfo table = new UnityModels.TableInfo();
        table.setName(tableName);
        table.setCatalogName("main");
        table.setSchemaName("default");
        table.setStorageLocation(tableLocation);
        table.setProperties(Map.of("table_type", "lance"));
        return table;
    }
}
