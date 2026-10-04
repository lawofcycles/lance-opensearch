/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.util.List;
import java.util.Map;

import org.opensearch.lance.StorageOptions;
import org.opensearch.test.OpenSearchTestCase;

public class AllowedCatalogEndpointsTests extends OpenSearchTestCase {

    private static LanceNamespaceMetadata.Entry entry(String type, Map<String, String> config) {
        return new LanceNamespaceMetadata.Entry("cat", type, null, StorageOptions.empty(), config);
    }

    public void testEmptyListRefusesLoopbackAndLinkLocalOnly() {
        AllowedCatalogEndpoints endpoints = new AllowedCatalogEndpoints(List.of());
        assertTrue(endpoints.isEmpty());
        assertNull(endpoints.refusal("uri", "https://catalog.example.com/"));
        assertNull(endpoints.refusal("uri", "http://catalog.example.com:8080/v1"));
        assertNull(endpoints.refusal("endpoint", "https://10.0.0.5:8181/"));
        for (String local : List.of(
            "http://169.254.169.254/",
            "http://169.254.169.254/latest/meta-data/",
            "http://169.254.170.2/v2/credentials",
            "http://127.0.0.1:9200/",
            "http://127.1.2.3/",
            "http://LOCALHOST/",
            "http://[::1]:8080/",
            "http://[fe80::1]/",
            "http://[fe80::1%25eth0]/",
            "http://[::ffff:127.0.0.1]/",
            "http://0.0.0.0/",
            "http://[::]/"
        )) {
            String refusal = endpoints.refusal("uri", local);
            assertNotNull(local, refusal);
            assertTrue(refusal, refusal.contains("is a link local or loopback address"));
            assertTrue(refusal, refusal.contains("plugins.lance.allowed_catalog_endpoints"));
        }
    }

    public void testEmptyListRefusesTheUnspecifiedAddressOfBothFamilies() {
        // 0.0.0.0 and :: are the wildcard addresses; a client that
        // connects to one reaches the local host, so the empty list
        // treats them as loopback.
        AllowedCatalogEndpoints endpoints = new AllowedCatalogEndpoints(List.of());
        for (String wildcard : List.of(
            "http://0.0.0.0/",
            "http://0.0.0.0:8181/v1",
            "http://[::]/",
            "http://[::]:8181/v1",
            "https://[::]/"
        )) {
            String refusal = endpoints.refusal("uri", wildcard);
            assertNotNull(wildcard, refusal);
            assertTrue(refusal, refusal.contains("is a link local or loopback address"));
        }
    }

    public void testEmptyListDoesNotResolveHostnames() {
        // The rule is on the address as written; a name that resolves to
        // a loopback or link local address is for the allowlist to catch.
        AllowedCatalogEndpoints endpoints = new AllowedCatalogEndpoints(List.of());
        assertNull(endpoints.refusal("uri", "http://metadata.internal/"));
    }

    public void testConfiguredPrefixesAcceptOnlyWhatTheyCover() {
        AllowedCatalogEndpoints endpoints = new AllowedCatalogEndpoints(
            List.of("https://catalog.example.com/", "http://glue.example.com:8080/v1")
        );
        assertFalse(endpoints.isEmpty());
        assertEquals(List.of("https://catalog.example.com/", "http://glue.example.com:8080/v1/"), endpoints.configuredPrefixes());
        assertNull(endpoints.refusal("uri", "https://catalog.example.com"));
        assertNull(endpoints.refusal("uri", "https://CATALOG.example.com/api/"));
        // A prefix without a port covers every port.
        assertNull(endpoints.refusal("uri", "https://catalog.example.com:8443/"));
        // A prefix with a port covers that port and the path below it.
        assertNull(endpoints.refusal("endpoint", "http://glue.example.com:8080/v1/catalogs"));
        assertNotNull(endpoints.refusal("endpoint", "http://glue.example.com:8080/v2"));
        assertNotNull(endpoints.refusal("endpoint", "http://glue.example.com/v1"));
        assertNotNull(endpoints.refusal("endpoint", "http://glue.example.com:8080/v1/../v2"));
        // Scheme and host are exact.
        assertNotNull(endpoints.refusal("uri", "http://catalog.example.com/"));
        assertNotNull(endpoints.refusal("uri", "https://catalog.example.com.evil.net/"));
        assertNotNull(endpoints.refusal("uri", "https://evil.net/https://catalog.example.com/"));
        String outside = endpoints.refusal("uri", "https://other.example.com/");
        assertNotNull(outside);
        assertTrue(outside, outside.contains("[https://other.example.com/]"));
        assertTrue(outside, outside.contains("is not under plugins.lance.allowed_catalog_endpoints"));
        // With prefixes set, a link local address is simply outside them.
        String imds = endpoints.refusal("uri", "http://169.254.169.254/");
        assertNotNull(imds);
        assertTrue(imds, imds.contains("is not under plugins.lance.allowed_catalog_endpoints"));
    }

    public void testPrefixMayNameALoopbackHost() {
        AllowedCatalogEndpoints endpoints = new AllowedCatalogEndpoints(List.of("http://127.0.0.1/"));
        assertNull(endpoints.refusal("uri", "http://127.0.0.1:41235/"));
        assertNotNull(endpoints.refusal("uri", "http://127.0.0.2/"));
    }

    public void testWildcardHostCoversOneLabel() {
        AllowedCatalogEndpoints endpoints = new AllowedCatalogEndpoints(List.of("https://*.internal/"));
        assertEquals(List.of("https://*.internal/"), endpoints.configuredPrefixes());
        assertNull(endpoints.refusal("uri", "https://catalog.internal/"));
        assertNull(endpoints.refusal("uri", "https://Polaris.INTERNAL:8181/api"));
        assertNotNull(endpoints.refusal("uri", "https://a.b.internal/"));
        assertNotNull(endpoints.refusal("uri", "https://internal/"));
        assertNotNull(endpoints.refusal("uri", "https://xinternal/"));
        assertNotNull(endpoints.refusal("uri", "http://catalog.internal/"));
    }

    public void testUnparseableEndpointIsRefusedWithoutEchoingIt() {
        AllowedCatalogEndpoints endpoints = new AllowedCatalogEndpoints(List.of());
        for (String bad : List.of("catalog.example.com", "not a uri", "https://", "https:///path", "https://host/../up")) {
            String refusal = endpoints.refusal("uri", bad);
            assertNotNull(bad, refusal);
            assertEquals("[lance_namespace] [config.uri] is not an absolute URI with a scheme and a host", refusal);
        }
    }

    public void testMessageDropsUserInfoAndQuery() {
        AllowedCatalogEndpoints endpoints = new AllowedCatalogEndpoints(List.of("https://catalog.example.com/"));
        String refusal = endpoints.refusal("uri", "https://user:secret@other.example.com/api?token=abc#frag");
        assertNotNull(refusal);
        assertTrue(refusal, refusal.contains("[https://other.example.com/api/]"));
        assertFalse(refusal, refusal.contains("secret"));
        assertFalse(refusal, refusal.contains("token=abc"));
    }

    public void testEntryChecksUriAndEndpointKeysOfCatalogTypes() {
        AllowedCatalogEndpoints endpoints = new AllowedCatalogEndpoints(List.of());
        assertNull(endpoints.refusal(entry(LanceNamespaceMetadata.Entry.TYPE_REST, Map.of("uri", "https://catalog.example.com/"))));
        assertNotNull(endpoints.refusal(entry(LanceNamespaceMetadata.Entry.TYPE_REST, Map.of("uri", "http://169.254.169.254/"))));
        assertNotNull(
            endpoints.refusal(
                entry(LanceNamespaceMetadata.Entry.TYPE_ICEBERG, Map.of("endpoint", "http://127.0.0.1:8181/", "warehouse", "wh"))
            )
        );
        assertNotNull(
            endpoints.refusal(entry(LanceNamespaceMetadata.Entry.TYPE_GLUE, Map.of("region", "us-east-1", "endpoint", "http://[::1]/")))
        );
        // A glue registration without an endpoint names no server here.
        assertNull(endpoints.refusal(entry(LanceNamespaceMetadata.Entry.TYPE_GLUE, Map.of("region", "us-east-1"))));
        // A directory registration has no catalog server.
        assertNull(endpoints.refusal(new LanceNamespaceMetadata.Entry("/data/lance", StorageOptions.empty())));
    }

    public void testInvalidPrefixFailsConstruction() {
        for (String bad : List.of("catalog.example.com", "https://", "https://*/", "https://host/../up")) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new AllowedCatalogEndpoints(List.of(bad)));
            assertTrue(e.getMessage(), e.getMessage().contains("plugins.lance.allowed_catalog_endpoints entry [" + bad + "]"));
        }
        assertTrue(new AllowedCatalogEndpoints(null).isEmpty());
        assertTrue(new AllowedCatalogEndpoints(List.of("")).isEmpty());
    }
}
