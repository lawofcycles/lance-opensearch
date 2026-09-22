/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.io.IOException;

import java.util.List;
import java.util.Map;

import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.lance.StorageOptions;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Round trips for {@link LanceNamespaceMetadata}: the wire format with
 * its leading marker, the old wire format without one, the XContent
 * both ways, old JSON parsing as a directory entry, and the redaction
 * of sensitive config keys everywhere except gateway persistence.
 */
public class LanceNamespaceMetadataTests extends OpenSearchTestCase {

    private static LanceNamespaceMetadata sample() {
        LanceNamespaceMetadata.Entry directory = new LanceNamespaceMetadata.Entry(
            "/data/lance",
            StorageOptions.of(Map.of("aws_region", "us-west-2"))
        );
        LanceNamespaceMetadata.Entry rest = new LanceNamespaceMetadata.Entry(
            "cat",
            LanceNamespaceMetadata.Entry.TYPE_REST,
            null,
            StorageOptions.empty(),
            Map.of("uri", "http://catalog.example:8080", "header.Authorization", "Bearer hunter2")
        );
        LanceNamespaceMetadata.Entry glue = new LanceNamespaceMetadata.Entry(
            "glue-tokyo",
            LanceNamespaceMetadata.Entry.TYPE_GLUE,
            null,
            StorageOptions.empty(),
            Map.of("region", "ap-northeast-1", "catalog_id", "123456789012", "secret_access_key", "sekrit")
        );
        return new LanceNamespaceMetadata(List.of(directory, rest, glue));
    }

    public void testWireRoundTrip() throws Exception {
        LanceNamespaceMetadata original = sample();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                LanceNamespaceMetadata restored = new LanceNamespaceMetadata(in);
                assertEquals(original, restored);
                // Secrets ride the wire raw; every node needs them for initialize.
                assertEquals("sekrit", restored.entries().get(2).config().get("secret_access_key"));
            }
        }
    }

    public void testOldWireFormatReadsAsDirectoryEntries() throws Exception {
        // Simulate a stream produced before the format marker existed:
        // entry count first, then rootUri + storage options + overrides
        // JSON per entry (the last marker-less format).
        StorageOptions options = StorageOptions.of(Map.of("aws_region", "eu-west-1"));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(1);
            out.writeString("/old/root");
            options.writeTo(out);
            out.writeString("");
            try (StreamInput in = out.bytes().streamInput()) {
                LanceNamespaceMetadata restored = new LanceNamespaceMetadata(in);
                assertEquals(1, restored.entries().size());
                LanceNamespaceMetadata.Entry entry = restored.entries().get(0);
                assertEquals("/old/root", entry.name());
                assertEquals(LanceNamespaceMetadata.Entry.TYPE_DIRECTORY, entry.type());
                assertEquals("/old/root", entry.rootUri());
                assertEquals(options, entry.storageOptions());
                assertTrue(entry.config().isEmpty());
            }
        }
    }

    public void testGatewayXContentRoundTripKeepsSecrets() throws Exception {
        LanceNamespaceMetadata original = sample();
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        original.toXContent(builder, new ToXContent.MapParams(Map.of(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY)));
        builder.endObject();
        String json = builder.toString();
        assertTrue("gateway persistence must keep raw secrets: " + json, json.contains("sekrit"));
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken();
            LanceNamespaceMetadata restored = LanceNamespaceMetadata.fromXContent(parser);
            assertEquals(original, restored);
        }
    }

    public void testApiXContentRedactsSensitiveConfigKeys() throws Exception {
        LanceNamespaceMetadata original = sample();
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        // No context param: the default context is API and must redact.
        original.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String json = builder.toString();
        assertFalse("secret value must not appear in API output: " + json, json.contains("sekrit"));
        assertFalse("token value must not appear in API output: " + json, json.contains("hunter2"));
        assertTrue("redaction marker expected: " + json, json.contains("***"));
        // Non-sensitive keys stay readable.
        assertTrue(json.contains("ap-northeast-1"));
        assertTrue(json.contains("http://catalog.example:8080"));
    }

    public void testOldJsonParsesAsDirectoryEntry() throws Exception {
        String oldJson = "{\"entries\":[{\"root_uri\":\"/old/root\",\"storage_options\":{\"aws_region\":\"eu-west-1\"}}]}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, oldJson)) {
            parser.nextToken();
            LanceNamespaceMetadata restored = LanceNamespaceMetadata.fromXContent(parser);
            assertEquals(1, restored.entries().size());
            LanceNamespaceMetadata.Entry entry = restored.entries().get(0);
            assertEquals("/old/root", entry.name());
            assertEquals(LanceNamespaceMetadata.Entry.TYPE_DIRECTORY, entry.type());
            assertEquals("/old/root", entry.rootUri());
            assertTrue(entry.config().isEmpty());
        }
    }

    public void testToStringRedactsConfig() {
        LanceNamespaceMetadata metadata = sample();
        String rendered = metadata.toString();
        assertFalse("toString must not leak secrets: " + rendered, rendered.contains("sekrit"));
        assertFalse("toString must not leak tokens: " + rendered, rendered.contains("hunter2"));
    }

    public void testSensitiveKeyMatching() {
        assertTrue(LanceNamespaceMetadata.Entry.isSensitiveConfigKey("secret_access_key"));
        assertTrue(LanceNamespaceMetadata.Entry.isSensitiveConfigKey("access_key_id"));
        assertTrue(LanceNamespaceMetadata.Entry.isSensitiveConfigKey("session_token"));
        assertTrue(LanceNamespaceMetadata.Entry.isSensitiveConfigKey("header.Authorization-Token"));
        assertTrue(LanceNamespaceMetadata.Entry.isSensitiveConfigKey("PASSWORD"));
        // The Iceberg REST client's static bearer and OAuth pair.
        assertTrue(LanceNamespaceMetadata.Entry.isSensitiveConfigKey("auth_token"));
        assertTrue(LanceNamespaceMetadata.Entry.isSensitiveConfigKey("credential"));
        assertFalse(LanceNamespaceMetadata.Entry.isSensitiveConfigKey("region"));
        assertFalse(LanceNamespaceMetadata.Entry.isSensitiveConfigKey("uri"));
        assertFalse(LanceNamespaceMetadata.Entry.isSensitiveConfigKey("catalog_id"));
        assertFalse(LanceNamespaceMetadata.Entry.isSensitiveConfigKey("endpoint"));
        assertFalse(LanceNamespaceMetadata.Entry.isSensitiveConfigKey("warehouse"));
        assertFalse(LanceNamespaceMetadata.Entry.isSensitiveConfigKey("catalog"));
        assertFalse(LanceNamespaceMetadata.Entry.isSensitiveConfigKey("max_namespace_depth"));
    }

    public void testWithRegisteredIsNoOpForDuplicateName() {
        LanceNamespaceMetadata current = sample();
        LanceNamespaceMetadata.Entry duplicate = new LanceNamespaceMetadata.Entry(
            "cat",
            LanceNamespaceMetadata.Entry.TYPE_REST,
            null,
            StorageOptions.empty(),
            Map.of("uri", "http://elsewhere.example")
        );
        assertSame(current, current.withRegistered(duplicate));
    }

    public void testWithRegisteredIsNoOpForDuplicateDirectoryRootUnderDifferentName() {
        LanceNamespaceMetadata current = sample();
        LanceNamespaceMetadata.Entry samePathNewName = new LanceNamespaceMetadata.Entry(
            "alias",
            LanceNamespaceMetadata.Entry.TYPE_DIRECTORY,
            "/data/lance",
            StorageOptions.empty(),
            Map.of()
        );
        assertSame(current, current.withRegistered(samePathNewName));
    }

    public void testWithUnregisteredMatchesNameAndDirectoryRoot() {
        LanceNamespaceMetadata current = sample();
        assertEquals(2, current.withUnregistered("cat").entries().size());
        // A directory entry unregisters by its path too.
        assertEquals(2, current.withUnregistered("/data/lance").entries().size());
        assertSame(current, current.withUnregistered("unknown"));
    }

    public void testWireRejectsUnknownTypeString() throws Exception {
        // The stream constructor guards the type the same way the
        // primary constructor does; a value outside the accepted set
        // means a corrupted stream or an unknown sender build.
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(LanceNamespaceMetadata.WIRE_FORMAT_VERSION);
            out.writeVInt(1);
            out.writeString("x");
            out.writeString("hive");
            try (StreamInput in = out.bytes().streamInput()) {
                IOException e = expectThrows(IOException.class, () -> new LanceNamespaceMetadata(in));
                assertTrue(e.getMessage(), e.getMessage().contains("hive"));
                assertTrue(e.getMessage(), e.getMessage().contains("directory"));
            }
        }
    }

    public void testEntryRejectsUnknownType() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new LanceNamespaceMetadata.Entry("x", "hive", null, StorageOptions.empty(), Map.of())
        );
        assertTrue(e.getMessage(), e.getMessage().contains("directory"));
    }

    public void testDirectoryEntryRequiresRoot() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new LanceNamespaceMetadata.Entry("x", LanceNamespaceMetadata.Entry.TYPE_DIRECTORY, null, StorageOptions.empty(), Map.of())
        );
    }
}
