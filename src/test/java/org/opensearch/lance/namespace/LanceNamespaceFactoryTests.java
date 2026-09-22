/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.util.Map;

import org.lance.namespace.LanceNamespace;
import org.opensearch.lance.StorageOptions;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The factory's type dispatch and the {@code initialize} property
 * shapes it hands each implementation. The stub seam replaces
 * instantiation so no native or network resource is touched; the
 * dispatch-by-type test asserts on the recorded properties, secrets
 * included, because redaction applies to display paths only.
 */
public class LanceNamespaceFactoryTests extends OpenSearchTestCase {

    @Override
    public void tearDown() throws Exception {
        LanceNamespaceFactory.resetInstantiatorForTests();
        super.tearDown();
    }

    public void testDirectoryEntryInitializesWithRootProperty() {
        RecordingLanceNamespace recording = new RecordingLanceNamespace();
        LanceNamespaceFactory.setInstantiatorForTests(type -> recording);
        LanceNamespaceMetadata.Entry entry = new LanceNamespaceMetadata.Entry("/data/lance", StorageOptions.empty());
        LanceNamespace created = LanceNamespaceFactory.create(entry, null);
        assertSame(recording, created);
        assertEquals(1, recording.initializeCalls.size());
        assertEquals(Map.of("root", "/data/lance"), recording.initializeCalls.get(0));
    }

    public void testRestEntryPassesConfigThroughWithSecretsIntact() {
        RecordingLanceNamespace recording = new RecordingLanceNamespace();
        LanceNamespaceFactory.setInstantiatorForTests(type -> {
            assertEquals(LanceNamespaceMetadata.Entry.TYPE_REST, type);
            return recording;
        });
        Map<String, String> config = Map.of(
            "uri",
            "http://catalog.example:8080",
            "header.Authorization",
            "Bearer hunter2",
            "custom_client_knob",
            "42"
        );
        LanceNamespaceMetadata.Entry entry = new LanceNamespaceMetadata.Entry(
            "cat",
            LanceNamespaceMetadata.Entry.TYPE_REST,
            null,
            StorageOptions.empty(),
            config
        );
        LanceNamespaceFactory.create(entry, null);
        // The implementation receives the raw values: unknown keys pass
        // through untouched and the secret is not redacted on this path.
        assertEquals(config, recording.initializeCalls.get(0));
    }

    public void testGlueEntryPassesCredentialKeysIntact() {
        RecordingLanceNamespace recording = new RecordingLanceNamespace();
        LanceNamespaceFactory.setInstantiatorForTests(type -> {
            assertEquals(LanceNamespaceMetadata.Entry.TYPE_GLUE, type);
            return recording;
        });
        Map<String, String> config = Map.of(
            "region",
            "ap-northeast-1",
            "catalog_id",
            "123456789012",
            "root",
            "s3://bucket/prefix",
            "access_key_id",
            "AKIA123",
            "secret_access_key",
            "sekrit"
        );
        LanceNamespaceMetadata.Entry entry = new LanceNamespaceMetadata.Entry(
            "glue-tokyo",
            LanceNamespaceMetadata.Entry.TYPE_GLUE,
            null,
            StorageOptions.empty(),
            config
        );
        LanceNamespaceFactory.create(entry, null);
        assertEquals(config, recording.initializeCalls.get(0));
    }

    public void testInitializeFailurePropagatesToTheCaller() {
        RecordingLanceNamespace recording = new RecordingLanceNamespace();
        recording.initializeFailure = new IllegalStateException("bad credentials");
        LanceNamespaceFactory.setInstantiatorForTests(type -> recording);
        LanceNamespaceMetadata.Entry entry = new LanceNamespaceMetadata.Entry(
            "cat",
            LanceNamespaceMetadata.Entry.TYPE_REST,
            null,
            StorageOptions.empty(),
            Map.of("uri", "http://catalog.example:8080")
        );
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> LanceNamespaceFactory.create(entry, null));
        assertEquals("bad credentials", e.getMessage());
    }
}
