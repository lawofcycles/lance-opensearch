/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

public class StorageOptionsTests extends OpenSearchTestCase {

    public void testEmptyIsSingleton() {
        assertSame(StorageOptions.empty(), StorageOptions.empty());
        assertSame(StorageOptions.empty(), StorageOptions.of(null));
        assertSame(StorageOptions.empty(), StorageOptions.of(Map.of()));
        assertTrue(StorageOptions.empty().isEmpty());
    }

    public void testOfCopiesInputMap() {
        // The wrapper must snapshot the caller's map so a later mutation
        // of the input does not leak into the persisted / propagated
        // options. This is the same defensive-copy contract lance-flink
        // and lance-spark use around their storage-option maps.
        Map<String, String> input = new HashMap<>();
        input.put("aws_region", "us-west-2");
        StorageOptions options = StorageOptions.of(input);
        input.put("aws_endpoint", "https://s3.example");
        assertFalse("later mutation must not leak into options", options.asMap().containsKey("aws_endpoint"));
    }

    public void testAsMapIsUnmodifiable() {
        StorageOptions options = StorageOptions.of(Map.of("aws_region", "us-east-1"));
        expectThrows(UnsupportedOperationException.class, () -> options.asMap().put("aws_endpoint", "x"));
    }

    public void testOfRejectsNullKey() {
        Map<String, String> input = new HashMap<>();
        input.put(null, "value");
        Exception e = expectThrows(IllegalArgumentException.class, () -> StorageOptions.of(input));
        assertTrue("unexpected: " + e.getMessage(), e.getMessage().contains("keys must be non-empty"));
    }

    public void testOfRejectsEmptyKey() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> StorageOptions.of(Map.of("", "v")));
        assertTrue("unexpected: " + e.getMessage(), e.getMessage().contains("keys must be non-empty"));
    }

    public void testOfRejectsNullValue() {
        Map<String, String> input = new HashMap<>();
        input.put("aws_region", null);
        Exception e = expectThrows(IllegalArgumentException.class, () -> StorageOptions.of(input));
        assertTrue("unexpected: " + e.getMessage(), e.getMessage().contains("aws_region"));
    }

    public void testParseFromRequestFieldAcceptsNull() {
        assertSame(StorageOptions.empty(), StorageOptions.parseFromRequestField(null, "[lance_attach]"));
    }

    public void testParseFromRequestFieldReadsStringMap() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("aws_access_key_id", "AKID");
        body.put("aws_region", "us-east-1");
        StorageOptions options = StorageOptions.parseFromRequestField(body, "[lance_attach]");
        assertEquals("AKID", options.asMap().get("aws_access_key_id"));
        assertEquals("us-east-1", options.asMap().get("aws_region"));
    }

    public void testParseFromRequestFieldRejectsNonObject() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> StorageOptions.parseFromRequestField("not-an-object", "[lance_attach]")
        );
        assertTrue("unexpected: " + e.getMessage(), e.getMessage().contains("must be a JSON object"));
    }

    public void testParseFromRequestFieldRejectsNestedObject() {
        // Nested / array values would silently get toString()-ed if we
        // did not reject them up front; keep the 400 explicit so callers
        // notice the shape mismatch before it reaches Lance's Rust side.
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("aws_config", Map.of("nested", "value"));
        Exception e = expectThrows(IllegalArgumentException.class, () -> StorageOptions.parseFromRequestField(body, "[lance_attach]"));
        assertTrue("unexpected: " + e.getMessage(), e.getMessage().contains("must be a string"));
        assertTrue("unexpected: " + e.getMessage(), e.getMessage().contains("aws_config"));
    }

    public void testParseFromRequestFieldRejectsNumericValue() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("timeout_seconds", 30);
        Exception e = expectThrows(IllegalArgumentException.class, () -> StorageOptions.parseFromRequestField(body, "[lance_attach]"));
        assertTrue("unexpected: " + e.getMessage(), e.getMessage().contains("must be a string"));
    }

    public void testFromIndexSettingsAndWriteToSettingsRoundTrip() {
        StorageOptions original = StorageOptions.of(Map.of("aws_region", "us-west-2", "aws_endpoint", "https://s3.example"));
        Settings.Builder builder = Settings.builder();
        original.writeToSettings(builder);

        Settings settings = builder.build();
        assertEquals("us-west-2", settings.get(StorageOptions.INDEX_SETTING_PREFIX + "aws_region"));
        assertEquals("https://s3.example", settings.get(StorageOptions.INDEX_SETTING_PREFIX + "aws_endpoint"));

        StorageOptions roundTripped = StorageOptions.fromIndexSettings(settings);
        assertEquals(original, roundTripped);
    }

    public void testFromIndexSettingsIgnoresUnrelatedKeys() {
        // Only entries under the canonical prefix belong to
        // storage_options; other index settings must not bleed in.
        Settings settings = Settings.builder()
            .put(StorageOptions.INDEX_SETTING_PREFIX + "aws_region", "us-east-1")
            .put("index.number_of_shards", 1)
            .put("index.lance.table", "/tmp/table.lance")
            .build();
        StorageOptions options = StorageOptions.fromIndexSettings(settings);
        assertEquals(Map.of("aws_region", "us-east-1"), options.asMap());
    }

    public void testStreamRoundTrip() throws Exception {
        StorageOptions original = StorageOptions.of(
            Map.of("aws_access_key_id", "AKID", "aws_secret_access_key", "SECRET", "aws_region", "us-east-1")
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                StorageOptions copy = StorageOptions.readFromStream(in);
                assertEquals(original, copy);
                assertEquals(original.hashCode(), copy.hashCode());
            }
        }
    }

    public void testEmptyStreamRoundTrip() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            StorageOptions.empty().writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertSame(StorageOptions.empty(), StorageOptions.readFromStream(in));
            }
        }
    }

    public void testToReadOptionsOrNullReturnsNullWhenEmpty() {
        assertNull(StorageOptions.empty().toReadOptionsOrNull());
    }

    public void testToReadOptionsOrNullCarriesMap() {
        StorageOptions options = StorageOptions.of(Map.of("aws_region", "us-east-1"));
        assertEquals("us-east-1", options.toReadOptionsOrNull().getStorageOptions().get("aws_region"));
    }

    public void testToStringRedactsCredentialsLikeValues() {
        // Every place StorageOptions is toString'd could end up in a
        // shard-failed error body or a log line. The public API of the
        // plugin doesn't need to see credential values there, so redact
        // anything whose key hints at a secret; keep non-sensitive
        // values (region, endpoint) visible so operators can still
        // debug misrouted requests.
        StorageOptions options = StorageOptions.of(
            Map.of(
                "aws_access_key_id",
                "AKID",
                "aws_secret_access_key",
                "SECRET",
                "aws_session_token",
                "TOKEN",
                "aws_region",
                "us-east-1",
                "aws_endpoint",
                "https://s3.example"
            )
        );
        String text = options.toString();
        assertFalse("access key must be redacted: " + text, text.contains("AKID"));
        assertFalse("secret must be redacted: " + text, text.contains("SECRET"));
        assertFalse("session token must be redacted: " + text, text.contains("TOKEN"));
        assertTrue("region must remain visible: " + text, text.contains("us-east-1"));
        assertTrue("endpoint must remain visible: " + text, text.contains("s3.example"));
    }

    public void testEqualityIgnoresInsertionOrder() {
        Map<String, String> a = new LinkedHashMap<>();
        a.put("aws_region", "us-east-1");
        a.put("aws_endpoint", "https://s3.example");
        Map<String, String> b = new LinkedHashMap<>();
        b.put("aws_endpoint", "https://s3.example");
        b.put("aws_region", "us-east-1");
        assertEquals(StorageOptions.of(a), StorageOptions.of(b));
        assertEquals(StorageOptions.of(a).hashCode(), StorageOptions.of(b).hashCode());
    }
}
