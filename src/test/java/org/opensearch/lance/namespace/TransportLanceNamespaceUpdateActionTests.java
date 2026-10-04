/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.namespace.TransportLanceNamespaceUpdateAction.RegisterDecision;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Pins the order and outcomes of the register-side checks the cluster
 * manager runs before it submits a namespace registration.
 */
public class TransportLanceNamespaceUpdateActionTests extends OpenSearchTestCase {

    /** The default endpoint allowlist: everything but loopback and link local. */
    private static final AllowedCatalogEndpoints ANY_ENDPOINT = new AllowedCatalogEndpoints(List.of());

    private static LanceNamespaceMetadata registered(String root) {
        return LanceNamespaceMetadata.EMPTY.withRegistered(new LanceNamespaceMetadata.Entry(root, StorageOptions.empty()));
    }

    private static LanceNamespaceMetadata.Entry directoryEntry(String root) {
        return new LanceNamespaceMetadata.Entry(root, StorageOptions.empty());
    }

    public void testAllowlistRejectsRootEvenWhenAlreadyRegistered() {
        // A root that was registered before it fell out of the allowlist
        // must not be acknowledged as a duplicate.
        AllowedTableRoots roots = new AllowedTableRoots(List.of("/data/lance"));
        OpenSearchStatusException e = expectThrows(
            OpenSearchStatusException.class,
            () -> TransportLanceNamespaceUpdateAction.decideRegister(
                roots,
                ANY_ENDPOINT,
                registered("/other/root"),
                directoryEntry("/other/root")
            )
        );
        assertEquals(RestStatus.FORBIDDEN, e.status());
        assertTrue(e.getMessage(), e.getMessage().contains("plugins.lance.allowed_table_roots"));
    }

    public void testAlreadyRegisteredRootShortCircuitsBeforeExistenceCheck() {
        // The directory does not exist on disk, yet a repeated register of
        // a known root is a no-op rather than a 400.
        String phantom = createTempDir().resolve("gone").toString();
        RegisterDecision decision = TransportLanceNamespaceUpdateAction.decideRegister(
            new AllowedTableRoots(List.of()),
            ANY_ENDPOINT,
            registered(phantom),
            directoryEntry(phantom)
        );
        assertEquals(RegisterDecision.ALREADY_REGISTERED, decision);
    }

    public void testMissingDirectoryIsRejectedWith400Message() {
        String phantom = createTempDir().resolve("does-not-exist").toString();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportLanceNamespaceUpdateAction.decideRegister(
                new AllowedTableRoots(List.of()),
                ANY_ENDPOINT,
                null,
                directoryEntry(phantom)
            )
        );
        assertTrue(e.getMessage(), e.getMessage().contains("does not exist"));
    }

    public void testFileIsRejectedAsNotADirectory() throws Exception {
        Path file = Files.createFile(createTempDir().resolve("table.lance"));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportLanceNamespaceUpdateAction.decideRegister(
                new AllowedTableRoots(List.of()),
                ANY_ENDPOINT,
                null,
                directoryEntry(file.toString())
            )
        );
        assertTrue(e.getMessage(), e.getMessage().contains("not a directory"));
    }

    public void testExistingDirectoryProceeds() {
        Path dir = createTempDir();
        RegisterDecision decision = TransportLanceNamespaceUpdateAction.decideRegister(
            new AllowedTableRoots(List.of(dir.toString())),
            ANY_ENDPOINT,
            LanceNamespaceMetadata.EMPTY,
            directoryEntry(dir.toString())
        );
        assertEquals(RegisterDecision.PROCEED, decision);
    }

    public void testObjectStoreRootSkipsExistenceCheck() {
        RegisterDecision decision = TransportLanceNamespaceUpdateAction.decideRegister(
            new AllowedTableRoots(List.of("s3://bucket")),
            ANY_ENDPOINT,
            LanceNamespaceMetadata.EMPTY,
            directoryEntry("s3://bucket/lance")
        );
        assertEquals(RegisterDecision.PROCEED, decision);
    }

    public void testCatalogRegistrationSkipsAllowlistAndExistenceChecks() {
        // A rest / glue registration names no root the manager could
        // check; the root allowlist applies at surface time to the table
        // locations the catalog returns instead.
        AllowedTableRoots roots = new AllowedTableRoots(List.of("/data/lance"));
        LanceNamespaceMetadata.Entry entry = new LanceNamespaceMetadata.Entry(
            "cat",
            LanceNamespaceMetadata.Entry.TYPE_REST,
            null,
            StorageOptions.empty(),
            Map.of("uri", "http://catalog.example:8080")
        );
        assertEquals(
            RegisterDecision.PROCEED,
            TransportLanceNamespaceUpdateAction.decideRegister(roots, ANY_ENDPOINT, LanceNamespaceMetadata.EMPTY, entry)
        );
    }

    public void testCatalogRegistrationIsDuplicateByName() {
        LanceNamespaceMetadata.Entry entry = new LanceNamespaceMetadata.Entry(
            "cat",
            LanceNamespaceMetadata.Entry.TYPE_GLUE,
            null,
            StorageOptions.empty(),
            Map.of("region", "ap-northeast-1")
        );
        LanceNamespaceMetadata current = LanceNamespaceMetadata.EMPTY.withRegistered(entry);
        assertEquals(
            RegisterDecision.ALREADY_REGISTERED,
            TransportLanceNamespaceUpdateAction.decideRegister(new AllowedTableRoots(List.of()), ANY_ENDPOINT, current, entry)
        );
    }

    public void testCatalogEndpointOnALinkLocalAddressIsRejectedWith400Message() {
        // The default allowlist refuses the instance metadata service
        // whatever else it admits, so the poll never sends the node's
        // request there.
        LanceNamespaceMetadata.Entry entry = new LanceNamespaceMetadata.Entry(
            "imds",
            LanceNamespaceMetadata.Entry.TYPE_REST,
            null,
            StorageOptions.empty(),
            Map.of("uri", "http://169.254.169.254/")
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportLanceNamespaceUpdateAction.decideRegister(
                new AllowedTableRoots(List.of()),
                ANY_ENDPOINT,
                LanceNamespaceMetadata.EMPTY,
                entry
            )
        );
        assertEquals(
            "[lance_namespace] catalog endpoint [http://169.254.169.254/] (config.uri) is a link local or loopback address; "
                + "it is refused unless plugins.lance.allowed_catalog_endpoints names it",
            e.getMessage()
        );
    }

    public void testCatalogEndpointOutsideTheConfiguredPrefixesIsRejectedEvenWhenAlreadyRegistered() {
        // An endpoint registered before the allowlist was tightened is
        // refused rather than acknowledged as a duplicate.
        AllowedCatalogEndpoints endpoints = new AllowedCatalogEndpoints(List.of("https://catalog.example.com/"));
        LanceNamespaceMetadata.Entry entry = new LanceNamespaceMetadata.Entry(
            "ice",
            LanceNamespaceMetadata.Entry.TYPE_ICEBERG,
            null,
            StorageOptions.empty(),
            Map.of("endpoint", "https://other.example.com/", "warehouse", "wh")
        );
        LanceNamespaceMetadata current = LanceNamespaceMetadata.EMPTY.withRegistered(entry);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportLanceNamespaceUpdateAction.decideRegister(new AllowedTableRoots(List.of()), endpoints, current, entry)
        );
        assertEquals(
            "[lance_namespace] catalog endpoint [https://other.example.com/] (config.endpoint) is not under "
                + "plugins.lance.allowed_catalog_endpoints",
            e.getMessage()
        );
        LanceNamespaceMetadata.Entry inside = new LanceNamespaceMetadata.Entry(
            "ice",
            LanceNamespaceMetadata.Entry.TYPE_ICEBERG,
            null,
            StorageOptions.empty(),
            Map.of("endpoint", "https://catalog.example.com/api", "warehouse", "wh")
        );
        assertEquals(
            RegisterDecision.PROCEED,
            TransportLanceNamespaceUpdateAction.decideRegister(
                new AllowedTableRoots(List.of()),
                endpoints,
                LanceNamespaceMetadata.EMPTY,
                inside
            )
        );
    }
}
