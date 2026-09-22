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

import org.lance.namespace.iceberg.IcebergModels;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.core.rest.RestStatus;

/**
 * End-to-end coverage for the {@code iceberg} namespace type: an
 * in-test HTTP server speaks the Iceberg REST catalog protocol for one
 * Lance table inside one namespace under a warehouse, the cluster's
 * bundled IcebergNamespace client polls it, the table surfaces as an
 * index, and a search answers from the Lance data the table metadata's
 * location points at.
 *
 * <p>The suppression covers {@code com.sun.net.httpserver}: the test
 * needs an HTTP listener inside the test JVM with no extra dependency,
 * and the JDK's built-in server is the standard choice for that in
 * OpenSearch test code.
 */
@SuppressForbidden(reason = "in-test HTTP server implementing the Iceberg REST catalog protocol for the poll to talk to")
public class LanceIcebergCatalogNamespaceIT extends LanceRestTestCase {

    public void testIcebergNamespaceSurfacesTableAndAnswersSearch() throws Exception {
        String suffix = "icecat-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 4);
        String tableLocation = scratchDir.resolve(tableName + ".lance").toString();

        HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", icebergCatalogHandler(tableName, tableLocation));
        server.start();
        String namespaceName = "ice-" + suffix;
        try {
            Response register = postJson(
                "/_lance/namespace",
                "{\"type\":\"iceberg\",\"name\":\""
                    + namespaceName
                    + "\",\"config\":{\"endpoint\":\"http://127.0.0.1:"
                    + server.getAddress().getPort()
                    + "\",\"warehouse\":\"wh\",\"auth_token\":\"test-token\"}}"
            );
            assertEquals("register failed: " + readAll(register), RestStatus.OK.getStatus(), register.getStatusLine().getStatusCode());

            client().performRequest(new Request("GET", "/_cluster/health/" + tableName + "?wait_for_status=yellow&timeout=60s"));

            assertBusy(() -> {
                Response search = client().performRequest(new Request("GET", "/" + tableName + "/_search?q=*:*"));
                String body = readAll(search);
                assertTrue("expected 4 hits from the iceberg-surfaced index: " + body, body.contains("\"value\":4"));
            });

            Response listing = client().performRequest(new Request("GET", "/_lance/namespace"));
            String listingBody = readAll(listing);
            assertTrue(
                "expected the iceberg namespace in the listing: " + listingBody,
                listingBody.contains("\"name\":\"" + namespaceName + "\"")
            );
            assertTrue("expected type iceberg: " + listingBody, listingBody.contains("\"type\":\"iceberg\""));
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
     * Answers the Iceberg REST endpoints the poll's walk hits: the
     * warehouse config probe, the namespace listing under the
     * warehouse, the table listing of that namespace, and the per-table
     * load (the client checks {@code table_type} during listing and the
     * poll reads the location from the same response). The response
     * JSON is produced by serialising the bundled client's own model
     * classes so the shape tracks the bundled artifact.
     */
    private static HttpHandler icebergCatalogHandler(String tableName, String tableLocation) {
        ObjectMapper mapper = new ObjectMapper();
        return exchange -> {
            String path = URLDecoder.decode(exchange.getRequestURI().getPath(), StandardCharsets.UTF_8);
            String query = exchange.getRequestURI().getQuery();
            exchange.getRequestBody().readAllBytes();
            String json;
            if (path.equals("/v1/config")) {
                // No defaults: the client falls back to the warehouse
                // name as the API prefix.
                json = mapper.writeValueAsString(new IcebergModels.ConfigResponse());
            } else if (path.equals("/v1/wh/namespaces") && (query == null || query.contains("parent") == false)) {
                IcebergModels.ListNamespacesResponse response = new IcebergModels.ListNamespacesResponse();
                response.setNamespaces(List.of(List.of("ns1")));
                json = mapper.writeValueAsString(response);
            } else if (path.equals("/v1/wh/namespaces")) {
                // The walk's second level: ns1 has no child namespaces.
                IcebergModels.ListNamespacesResponse response = new IcebergModels.ListNamespacesResponse();
                response.setNamespaces(List.of());
                json = mapper.writeValueAsString(response);
            } else if (path.equals("/v1/wh/namespaces/ns1/tables")) {
                IcebergModels.TableIdentifier identifier = new IcebergModels.TableIdentifier();
                identifier.setNamespace(List.of("ns1"));
                identifier.setName(tableName);
                IcebergModels.ListTablesResponse response = new IcebergModels.ListTablesResponse();
                response.setIdentifiers(List.of(identifier));
                json = mapper.writeValueAsString(response);
            } else if (path.equals("/v1/wh/namespaces/ns1/tables/" + tableName)) {
                IcebergModels.TableMetadata metadata = new IcebergModels.TableMetadata();
                metadata.setLocation(tableLocation);
                metadata.setProperties(Map.of("table_type", "lance"));
                IcebergModels.LoadTableResponse response = new IcebergModels.LoadTableResponse();
                response.setMetadata(metadata);
                json = mapper.writeValueAsString(response);
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
}
