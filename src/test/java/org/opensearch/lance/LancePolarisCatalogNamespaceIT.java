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

import org.lance.namespace.polaris.PolarisModels;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.core.rest.RestStatus;

/**
 * End-to-end coverage for the {@code polaris} namespace type: an
 * in-test HTTP server speaks the Polaris generic-table protocol (the
 * Iceberg REST namespace endpoints plus Polaris's generic-tables
 * endpoints) for one Lance table inside one namespace under a catalog,
 * the cluster's bundled PolarisNamespace client polls it with a static
 * bearer token, and the surfaced index answers a search.
 *
 * <p>The suppression covers {@code com.sun.net.httpserver}: the test
 * needs an HTTP listener inside the test JVM with no extra dependency,
 * and the JDK's built-in server is the standard choice for that in
 * OpenSearch test code.
 */
@SuppressForbidden(reason = "in-test HTTP server implementing the Polaris generic-table protocol for the poll to talk to")
public class LancePolarisCatalogNamespaceIT extends LanceRestTestCase {

    public void testPolarisNamespaceSurfacesTableAndAnswersSearch() throws Exception {
        String suffix = "polcat-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 4);
        String tableLocation = scratchDir.resolve(tableName + ".lance").toString();

        HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", polarisCatalogHandler(tableName, tableLocation));
        server.start();
        String namespaceName = "pol-" + suffix;
        try {
            Response register = postJson(
                "/_lance/namespace",
                "{\"type\":\"polaris\",\"name\":\""
                    + namespaceName
                    + "\",\"config\":{\"endpoint\":\"http://127.0.0.1:"
                    + server.getAddress().getPort()
                    + "\",\"warehouse\":\"cat\",\"auth_token\":\"test-token\"}}"
            );
            assertEquals("register failed: " + readAll(register), RestStatus.OK.getStatus(), register.getStatusLine().getStatusCode());

            client().performRequest(new Request("GET", "/_cluster/health/" + tableName + "?wait_for_status=yellow&timeout=60s"));

            assertBusy(() -> {
                Response search = client().performRequest(new Request("GET", "/" + tableName + "/_search?q=*:*"));
                String body = readAll(search);
                assertTrue("expected 4 hits from the polaris-surfaced index: " + body, body.contains("\"value\":4"));
            });

            Response listing = client().performRequest(new Request("GET", "/_lance/namespace"));
            String listingBody = readAll(listing);
            assertTrue(
                "expected the polaris namespace in the listing: " + listingBody,
                listingBody.contains("\"name\":\"" + namespaceName + "\"")
            );
            assertTrue("expected type polaris: " + listingBody, listingBody.contains("\"type\":\"polaris\""));
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
     * Answers the endpoints the poll's walk hits under Polaris's
     * {@code /api/catalog} base path: the namespace listing of the
     * catalog, the (empty) namespace listing below the namespace, the
     * generic-tables listing, and the per-table load the poll reads the
     * base location from. The response JSON is produced by serialising
     * the bundled client's own model classes.
     */
    private static HttpHandler polarisCatalogHandler(String tableName, String tableLocation) {
        ObjectMapper mapper = new ObjectMapper();
        return exchange -> {
            String path = URLDecoder.decode(exchange.getRequestURI().getPath(), StandardCharsets.UTF_8);
            exchange.getRequestBody().readAllBytes();
            String json;
            if (path.equals("/api/catalog/v1/cat/namespaces")) {
                json = mapper.writeValueAsString(new PolarisModels.ListNamespacesResponse(null, List.of(List.of("ns1"))));
            } else if (path.equals("/api/catalog/v1/cat/namespaces/ns1/namespaces")) {
                json = mapper.writeValueAsString(new PolarisModels.ListNamespacesResponse(null, List.of()));
            } else if (path.equals("/api/catalog/polaris/v1/cat/namespaces/ns1/generic-tables")) {
                json = mapper.writeValueAsString(
                    new PolarisModels.ListGenericTablesResponse(null, List.of(new PolarisModels.TableIdentifier(List.of("ns1"), tableName)))
                );
            } else if (path.equals("/api/catalog/polaris/v1/cat/namespaces/ns1/generic-tables/" + tableName)) {
                PolarisModels.GenericTable table = new PolarisModels.GenericTable(
                    tableName,
                    "lance",
                    tableLocation,
                    null,
                    Map.of("table_type", "lance")
                );
                json = mapper.writeValueAsString(new PolarisModels.LoadGenericTableResponse(table));
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
