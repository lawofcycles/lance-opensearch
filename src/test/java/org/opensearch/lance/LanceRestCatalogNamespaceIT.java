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
import java.util.Locale;
import java.util.Set;

import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesResponse;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.core.rest.RestStatus;

/**
 * End-to-end coverage for the {@code rest} namespace type: an in-test
 * HTTP server speaks the Lance Namespace REST protocol for one fixture
 * table, the cluster's RestNamespace client polls it, the table
 * surfaces as an index, and a search answers from the Lance data the
 * described location points at.
 *
 * <p>The suppression covers {@code com.sun.net.httpserver}: the test
 * needs an HTTP listener inside the test JVM with no extra dependency,
 * and the JDK's built-in server is the standard choice for that in
 * OpenSearch test code.
 */
@SuppressForbidden(reason = "in-test HTTP server implementing the Lance Namespace REST protocol for the poll to talk to")
public class LanceRestCatalogNamespaceIT extends LanceRestTestCase {

    public void testRestNamespaceSurfacesTableAndAnswersSearch() throws Exception {
        String suffix = "restcat-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeTable(scratchDir, tableName, 4);
        String tableLocation = scratchDir.resolve(tableName + ".lance").toString();

        HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", restCatalogHandler(tableName, tableLocation));
        server.start();
        String namespaceName = "cat-" + suffix;
        try {
            Response register = postJson(
                "/_lance/namespace",
                "{\"type\":\"rest\",\"name\":\""
                    + namespaceName
                    + "\",\"config\":{\"uri\":\"http://127.0.0.1:"
                    + server.getAddress().getPort()
                    + "\",\"header.Authorization\":\"Bearer test-token\"}}"
            );
            assertEquals("register failed: " + readAll(register), RestStatus.OK.getStatus(), register.getStatusLine().getStatusCode());

            // The poll surfaces the table named by the catalog listing.
            client().performRequest(new Request("GET", "/_cluster/health/" + tableName + "?wait_for_status=yellow&timeout=60s"));

            assertBusy(() -> {
                Response search = client().performRequest(new Request("GET", "/" + tableName + "/_search?q=*:*"));
                String body = readAll(search);
                assertTrue("expected 4 hits from the rest-surfaced index: " + body, body.contains("\"value\":4"));
            });

            // The listing names the registration, its type, and redacts
            // the credential-bearing config key.
            Response listing = client().performRequest(new Request("GET", "/_lance/namespace"));
            String listingBody = readAll(listing);
            assertTrue(
                "expected the rest namespace in the listing: " + listingBody,
                listingBody.contains("\"name\":\"" + namespaceName + "\"")
            );
            assertTrue("expected type rest: " + listingBody, listingBody.contains("\"type\":\"rest\""));
            assertTrue("expected available status: " + listingBody, listingBody.contains("\"status\":\"available\""));
            assertFalse("bearer token must not appear in the listing: " + listingBody, listingBody.contains("test-token"));

            // The tables preview goes through the registration name.
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
     * Answers the three calls the poll makes against a REST catalog:
     * the root table listing, the table description (location points at
     * the fixture written on shared storage), and the child namespace
     * listing some client versions probe first. The response JSON is
     * produced by serialising the client's own model classes so the
     * shape tracks the bundled lance-namespace version.
     */
    private static HttpHandler restCatalogHandler(String tableName, String tableLocation) {
        ObjectMapper mapper = new ObjectMapper();
        return exchange -> {
            String path = URLDecoder.decode(exchange.getRequestURI().getPath(), StandardCharsets.UTF_8);
            // Consume the request body (describe is a POST) before responding.
            exchange.getRequestBody().readAllBytes();
            String json;
            if (path.matches("/v1/namespace/[^/]*/table/list")) {
                json = mapper.writeValueAsString(new ListTablesResponse().tables(Set.of(tableName)));
            } else if (path.equals("/v1/table/" + tableName + "/describe")) {
                DescribeTableResponse response = new DescribeTableResponse();
                response.setLocation(tableLocation);
                json = mapper.writeValueAsString(response);
            } else if (path.matches("/v1/namespace/[^/]*/list")) {
                json = mapper.writeValueAsString(new ListNamespacesResponse().namespaces(Set.of()));
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
