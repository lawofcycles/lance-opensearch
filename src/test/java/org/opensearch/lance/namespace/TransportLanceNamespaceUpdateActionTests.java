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
            () -> TransportLanceNamespaceUpdateAction.decideRegister(roots, registered("/other/root"), directoryEntry("/other/root"))
        );
        assertEquals(RestStatus.FORBIDDEN, e.status());
        assertTrue(e.getMessage(), e.getMessage().contains("lance.allowed_table_roots"));
    }

    public void testAlreadyRegisteredRootShortCircuitsBeforeExistenceCheck() {
        // The directory does not exist on disk, yet a repeated register of
        // a known root is a no-op rather than a 400.
        String phantom = createTempDir().resolve("gone").toString();
        RegisterDecision decision = TransportLanceNamespaceUpdateAction.decideRegister(
            new AllowedTableRoots(List.of()),
            registered(phantom),
            directoryEntry(phantom)
        );
        assertEquals(RegisterDecision.ALREADY_REGISTERED, decision);
    }

    public void testMissingDirectoryIsRejectedWith400Message() {
        String phantom = createTempDir().resolve("does-not-exist").toString();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportLanceNamespaceUpdateAction.decideRegister(new AllowedTableRoots(List.of()), null, directoryEntry(phantom))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("does not exist"));
    }

    public void testFileIsRejectedAsNotADirectory() throws Exception {
        Path file = Files.createFile(createTempDir().resolve("table.lance"));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportLanceNamespaceUpdateAction.decideRegister(
                new AllowedTableRoots(List.of()),
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
            LanceNamespaceMetadata.EMPTY,
            directoryEntry(dir.toString())
        );
        assertEquals(RegisterDecision.PROCEED, decision);
    }

    public void testObjectStoreRootSkipsExistenceCheck() {
        RegisterDecision decision = TransportLanceNamespaceUpdateAction.decideRegister(
            new AllowedTableRoots(List.of("s3://bucket")),
            LanceNamespaceMetadata.EMPTY,
            directoryEntry("s3://bucket/lance")
        );
        assertEquals(RegisterDecision.PROCEED, decision);
    }

    public void testCatalogRegistrationSkipsAllowlistAndExistenceChecks() {
        // A rest / glue registration names no root the manager could
        // check; the allowlist applies at surface time to the table
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
            TransportLanceNamespaceUpdateAction.decideRegister(roots, LanceNamespaceMetadata.EMPTY, entry)
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
            TransportLanceNamespaceUpdateAction.decideRegister(new AllowedTableRoots(List.of()), current, entry)
        );
    }
}
