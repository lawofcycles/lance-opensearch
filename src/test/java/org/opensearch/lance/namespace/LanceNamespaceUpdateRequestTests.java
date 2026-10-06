/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.lance.StorageOptions;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The two namespace body parsers and the {@code config} bounds.
 *
 * <p>{@link LanceNamespaceUpdateRequest#REGISTER_PARSER} and
 * {@link LanceNamespaceUpdateRequest#IDENTIFIER_PARSER} refuse a field
 * outside their declared set naming it, and the identifier parser does
 * not know the register only fields. {@code build} / {@code resolve}
 * fill the directory defaults and refuse the shapes each type cannot
 * use with the message the operator sees.
 *
 * <p>The {@code config} map is bounded like {@code storage_options}:
 * the same entry count, key length and value length limits of
 * {@link StorageOptions}, refused with an {@link IllegalArgumentException}
 * that names the count or the key and the length and never quotes a value.
 */
public class LanceNamespaceUpdateRequestTests extends OpenSearchTestCase {

    private LanceNamespaceUpdateRequest register(String json) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            return LanceNamespaceUpdateRequest.REGISTER_PARSER.parse(parser, null).build();
        }
    }

    private String identifier(String json) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            return LanceNamespaceUpdateRequest.IDENTIFIER_PARSER.parse(parser, null).resolve();
        }
    }

    public void testRegisterReadsEveryDeclaredField() throws IOException {
        LanceNamespaceUpdateRequest request = register(
            "{\"path\":\"/tmp\",\"type\":\"directory\",\"name\":\"n\",\"config\":{\"k\":\"v\"},"
                + "\"storage_options\":{\"aws_region\":\"us-east-1\"},\"overrides\":{\"body\":{\"type\":\"keyword\"}}}"
        );
        assertEquals(LanceNamespaceUpdateRequest.Operation.REGISTER, request.operation());
        assertEquals("n", request.name());
        assertEquals("directory", request.type());
        assertEquals("/tmp", request.rootUri());
        assertEquals(Map.of("k", "v"), request.config());
        assertEquals(Map.of("aws_region", "us-east-1"), request.storageOptions().asMap());
        assertEquals("{\"body\":{\"type\":\"keyword\"}}", request.overridesJson());
    }

    public void testRegisterDefaultsToADirectoryNamedAfterItsPath() throws IOException {
        LanceNamespaceUpdateRequest request = register("{\"path\":\"/tmp\"}");
        assertEquals("directory", request.type());
        assertEquals("/tmp", request.name());
        assertEquals("/tmp", request.rootUri());
        assertEquals(Map.of(), request.config());
        assertEquals("", request.overridesJson());
    }

    public void testRegisterRefusesAnUnknownField() {
        XContentParseException e = expectThrows(
            XContentParseException.class,
            () -> register("{\"path\":\"/tmp\",\"poll_cadence\":\"1s\"}")
        );
        assertTrue(e.getMessage(), e.getMessage().contains("[lance_namespace] unknown field [poll_cadence]"));
    }

    public void testRegisterRefusesTheShapesEachTypeCannotUse() {
        assertEquals("[path] is required", expectThrows(IllegalArgumentException.class, () -> register("{}")).getMessage());
        assertEquals(
            "[path] must not be empty",
            expectThrows(IllegalArgumentException.class, () -> register("{\"path\":\"\"}")).getMessage()
        );
        assertTrue(
            expectThrows(IllegalArgumentException.class, () -> register("{\"type\":\"hive\",\"name\":\"h\"}")).getMessage()
                .startsWith("unknown namespace type [hive]; accepted values are ")
        );
        assertEquals(
            "[name] is required for type [rest]",
            expectThrows(IllegalArgumentException.class, () -> register("{\"type\":\"rest\",\"config\":{\"uri\":\"http://x\"}}"))
                .getMessage()
        );
        assertEquals(
            "[path] is only accepted for type [directory]; a [rest] namespace is rooted by its config",
            expectThrows(IllegalArgumentException.class, () -> register("{\"type\":\"rest\",\"name\":\"r\",\"path\":\"/tmp\"}"))
                .getMessage()
        );
        assertEquals(
            "[config.uri] is required for type [rest]",
            expectThrows(IllegalArgumentException.class, () -> register("{\"type\":\"rest\",\"name\":\"r\"}")).getMessage()
        );
        assertEquals(
            "[config.endpoint] is required for type [iceberg]",
            expectThrows(
                IllegalArgumentException.class,
                () -> register("{\"type\":\"iceberg\",\"name\":\"c\",\"config\":{\"warehouse\":\"wh\"}}")
            ).getMessage()
        );
        assertEquals(
            "[config.warehouse] is required for type [polaris]; it names the warehouse (catalog) whose namespaces are polled",
            expectThrows(
                IllegalArgumentException.class,
                () -> register("{\"type\":\"polaris\",\"name\":\"c\",\"config\":{\"endpoint\":\"http://x\"}}")
            ).getMessage()
        );
        assertEquals(
            "[config.catalog] is required for type [unity]",
            expectThrows(
                IllegalArgumentException.class,
                () -> register("{\"type\":\"unity\",\"name\":\"c\",\"config\":{\"endpoint\":\"http://x\"}}")
            ).getMessage()
        );
    }

    public void testIdentifierPrefersNameOverPath() throws IOException {
        assertEquals("n", identifier("{\"name\":\"n\"}"));
        assertEquals("/tmp", identifier("{\"path\":\"/tmp\"}"));
        assertEquals("n", identifier("{\"path\":\"/tmp\",\"name\":\"n\"}"));
        assertEquals(
            "[name] (or [path] for a directory registration) is required",
            expectThrows(IllegalArgumentException.class, () -> identifier("{}")).getMessage()
        );
        assertEquals(
            "[name] (or [path] for a directory registration) is required",
            expectThrows(IllegalArgumentException.class, () -> new LanceNamespaceUpdateRequest.Identifier().resolve()).getMessage()
        );
        assertEquals(
            "[name] must not be empty",
            expectThrows(IllegalArgumentException.class, () -> identifier("{\"name\":\"\"}")).getMessage()
        );
    }

    public void testIdentifierDoesNotKnowTheRegisterOnlyFields() {
        // /tables and DELETE take only an identifier; the register body's
        // other fields are unknown there and say so.
        XContentParseException e = expectThrows(XContentParseException.class, () -> identifier("{\"name\":\"n\",\"type\":\"directory\"}"));
        assertTrue(e.getMessage(), e.getMessage().contains("[lance_namespace] unknown field [type]"));
    }

    private static Map<String, Object> entries(int count) {
        Map<String, Object> config = new LinkedHashMap<>();
        for (int i = 0; i < count; i++) {
            config.put("key_" + i, "value_" + i);
        }
        return config;
    }

    public void testParseConfigAcceptsNullAndTheEntryBound() {
        assertEquals(Map.of(), LanceNamespaceUpdateRequest.parseConfig(null));
        assertEquals(StorageOptions.MAX_ENTRIES, LanceNamespaceUpdateRequest.parseConfig(entries(StorageOptions.MAX_ENTRIES)).size());
    }

    public void testParseConfigRejectsOneEntryAboveTheBound() {
        Map<String, Object> config = entries(StorageOptions.MAX_ENTRIES + 1);
        Exception e = expectThrows(IllegalArgumentException.class, () -> LanceNamespaceUpdateRequest.parseConfig(config));
        assertEquals(
            "[lance_namespace] config has [" + (StorageOptions.MAX_ENTRIES + 1) + "] entries, the limit is " + StorageOptions.MAX_ENTRIES,
            e.getMessage()
        );
    }

    public void testParseConfigAcceptsTheKeyBound() {
        String key = "k".repeat(StorageOptions.MAX_KEY_BYTES);
        Map<String, Object> config = new LinkedHashMap<>();
        config.put(key, "v");
        assertEquals("v", LanceNamespaceUpdateRequest.parseConfig(config).get(key));
    }

    public void testParseConfigRejectsOneByteAboveTheKeyBound() {
        String key = "k".repeat(StorageOptions.MAX_KEY_BYTES + 1);
        Map<String, Object> config = new LinkedHashMap<>();
        config.put(key, "v");
        Exception e = expectThrows(IllegalArgumentException.class, () -> LanceNamespaceUpdateRequest.parseConfig(config));
        assertEquals(
            "[lance_namespace] config key ["
                + key
                + "] is ["
                + (StorageOptions.MAX_KEY_BYTES + 1)
                + "] bytes, the limit is "
                + StorageOptions.MAX_KEY_BYTES,
            e.getMessage()
        );
    }

    public void testParseConfigRejectsOneByteAboveTheKeyBoundReachedByAMultiByteCharacter() {
        // The key is MAX_KEY_BYTES chars long but one byte over: the
        // bound counts UTF 8 bytes, not chars, on the namespace path too.
        String key = "k".repeat(StorageOptions.MAX_KEY_BYTES - 1) + "\u00e9";
        assertEquals(StorageOptions.MAX_KEY_BYTES, key.length());
        Map<String, Object> config = new LinkedHashMap<>();
        config.put(key, "v");
        Exception e = expectThrows(IllegalArgumentException.class, () -> LanceNamespaceUpdateRequest.parseConfig(config));
        assertEquals(
            "[lance_namespace] config key ["
                + key
                + "] is ["
                + (StorageOptions.MAX_KEY_BYTES + 1)
                + "] bytes, the limit is "
                + StorageOptions.MAX_KEY_BYTES,
            e.getMessage()
        );
    }

    public void testParseConfigAcceptsTheValueBound() {
        String value = "v".repeat(StorageOptions.MAX_VALUE_BYTES);
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("auth_token", value);
        assertEquals(value, LanceNamespaceUpdateRequest.parseConfig(config).get("auth_token"));
    }

    public void testParseConfigRejectsOneByteAboveTheValueBoundWithoutQuotingIt() {
        // A multi byte character shows the bound counts UTF 8 bytes, not chars.
        String value = "v".repeat(StorageOptions.MAX_VALUE_BYTES - 1) + "\u00e9";
        assertEquals(StorageOptions.MAX_VALUE_BYTES, value.length());
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("auth_token", value);
        Exception e = expectThrows(IllegalArgumentException.class, () -> LanceNamespaceUpdateRequest.parseConfig(config));
        assertEquals(
            "[lance_namespace] config value for [auth_token] is ["
                + (StorageOptions.MAX_VALUE_BYTES + 1)
                + "] bytes, the limit is "
                + StorageOptions.MAX_VALUE_BYTES,
            e.getMessage()
        );
        assertFalse("the value must not be quoted: " + e.getMessage(), e.getMessage().contains("vvvv"));
    }
}
