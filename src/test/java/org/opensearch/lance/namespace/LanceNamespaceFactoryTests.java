/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.io.Closeable;

import java.util.Map;

import org.lance.namespace.LanceNamespace;
import org.lance.namespace.glue.GlueNamespace;
import org.lance.namespace.iceberg.IcebergNamespace;
import org.lance.namespace.polaris.PolarisNamespace;
import org.lance.namespace.unity.UnityNamespace;
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

    public void testRealGlueDispatchInitialisesWithoutNetworkAccess() throws Exception {
        // Without the seam, the glue type instantiates the bundled
        // GlueNamespace, which was compiled against lance-namespace
        // 0.7.7; loading and initialising it against the bundled 0.11.1
        // interface proves the binary compatibility the dependency
        // declaration relies on. An explicit region and static
        // credentials keep the AWS client build fully offline.
        LanceNamespaceFactory.resetInstantiatorForTests();
        LanceNamespaceMetadata.Entry entry = new LanceNamespaceMetadata.Entry(
            "glue-offline",
            LanceNamespaceMetadata.Entry.TYPE_GLUE,
            null,
            StorageOptions.empty(),
            Map.of("region", "us-east-1", "access_key_id", "test-access-key", "secret_access_key", "test-secret")
        );
        LanceNamespace created = LanceNamespaceFactory.create(entry, null);
        assertTrue("expected the bundled GlueNamespace, saw " + created.getClass().getName(), created instanceof GlueNamespace);
        ((Closeable) created).close();
    }

    public void testCatalogTypesDispatchAndPassConfigThroughWithSecretsIntact() {
        // Dispatch and property passthrough for the three catalog types
        // added after glue. Each type's credential-bearing keys reach
        // initialize raw; redaction applies to display paths only.
        Map<String, Map<String, String>> configByType = Map.of(
            LanceNamespaceMetadata.Entry.TYPE_ICEBERG,
            Map.of(
                "endpoint",
                "http://catalog.example:8181",
                "warehouse",
                "wh",
                "auth_token",
                "hunter2",
                "credential",
                "client-id:client-secret"
            ),
            LanceNamespaceMetadata.Entry.TYPE_POLARIS,
            Map.of("endpoint", "http://polaris.example:8181", "warehouse", "cat", "auth_token", "hunter2"),
            LanceNamespaceMetadata.Entry.TYPE_UNITY,
            Map.of("endpoint", "http://unity.example:8080", "catalog", "main", "auth_token", "hunter2")
        );
        for (Map.Entry<String, Map<String, String>> typeAndConfig : configByType.entrySet()) {
            String type = typeAndConfig.getKey();
            RecordingLanceNamespace recording = new RecordingLanceNamespace();
            LanceNamespaceFactory.setInstantiatorForTests(seen -> {
                assertEquals(type, seen);
                return recording;
            });
            LanceNamespaceMetadata.Entry entry = new LanceNamespaceMetadata.Entry(
                type + "-cat",
                type,
                null,
                StorageOptions.empty(),
                typeAndConfig.getValue()
            );
            LanceNamespaceFactory.create(entry, null);
            assertEquals(typeAndConfig.getValue(), recording.initializeCalls.get(0));
        }
    }

    public void testRealIcebergDispatchInitialisesWithoutNetworkAccess() throws Exception {
        // Same binary-compatibility probe as the glue test above for the
        // bundled IcebergNamespace (0.4.1 / lance-namespace 0.7.7): the
        // class loads, its config parses, and its HTTP client builds on
        // the plugin classpath without a connection.
        LanceNamespaceFactory.resetInstantiatorForTests();
        LanceNamespaceMetadata.Entry entry = new LanceNamespaceMetadata.Entry(
            "iceberg-offline",
            LanceNamespaceMetadata.Entry.TYPE_ICEBERG,
            null,
            StorageOptions.empty(),
            Map.of("endpoint", "http://127.0.0.1:1", "warehouse", "wh", "auth_token", "test-token")
        );
        LanceNamespace created = LanceNamespaceFactory.create(entry, null);
        assertTrue("expected the bundled IcebergNamespace, saw " + created.getClass().getName(), created instanceof IcebergNamespace);
        ((Closeable) created).close();
    }

    public void testRealPolarisDispatchInitialisesWithoutNetworkAccess() throws Exception {
        LanceNamespaceFactory.resetInstantiatorForTests();
        LanceNamespaceMetadata.Entry entry = new LanceNamespaceMetadata.Entry(
            "polaris-offline",
            LanceNamespaceMetadata.Entry.TYPE_POLARIS,
            null,
            StorageOptions.empty(),
            Map.of("endpoint", "http://127.0.0.1:1", "warehouse", "cat", "auth_token", "test-token")
        );
        LanceNamespace created = LanceNamespaceFactory.create(entry, null);
        assertTrue("expected the bundled PolarisNamespace, saw " + created.getClass().getName(), created instanceof PolarisNamespace);
        ((Closeable) created).close();
    }

    public void testRealUnityDispatchInitialisesWithoutNetworkAccess() throws Exception {
        LanceNamespaceFactory.resetInstantiatorForTests();
        LanceNamespaceMetadata.Entry entry = new LanceNamespaceMetadata.Entry(
            "unity-offline",
            LanceNamespaceMetadata.Entry.TYPE_UNITY,
            null,
            StorageOptions.empty(),
            Map.of("endpoint", "http://127.0.0.1:1", "catalog", "main", "auth_token", "test-token")
        );
        LanceNamespace created = LanceNamespaceFactory.create(entry, null);
        assertTrue("expected the bundled UnityNamespace, saw " + created.getClass().getName(), created instanceof UnityNamespace);
        ((Closeable) created).close();
    }
}
