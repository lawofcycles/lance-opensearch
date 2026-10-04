/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.rest;

import java.util.LinkedHashMap;
import java.util.Map;

import org.opensearch.lance.StorageOptions;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The namespace {@code config} map is bounded like {@code storage_options}:
 * the same entry count, key length and value length limits of
 * {@link StorageOptions}, refused with an {@link IllegalArgumentException}
 * that names the count or the key and the length and never quotes a value.
 */
public class RestNamespaceActionTests extends OpenSearchTestCase {

    private static Map<String, Object> entries(int count) {
        Map<String, Object> config = new LinkedHashMap<>();
        for (int i = 0; i < count; i++) {
            config.put("key_" + i, "value_" + i);
        }
        return config;
    }

    public void testParseConfigAcceptsNullAndTheEntryBound() {
        assertEquals(Map.of(), RestNamespaceAction.parseConfig(null));
        assertEquals(StorageOptions.MAX_ENTRIES, RestNamespaceAction.parseConfig(entries(StorageOptions.MAX_ENTRIES)).size());
    }

    public void testParseConfigRejectsOneEntryAboveTheBound() {
        Map<String, Object> config = entries(StorageOptions.MAX_ENTRIES + 1);
        Exception e = expectThrows(IllegalArgumentException.class, () -> RestNamespaceAction.parseConfig(config));
        assertEquals(
            "[lance_namespace] config has [" + (StorageOptions.MAX_ENTRIES + 1) + "] entries, the limit is " + StorageOptions.MAX_ENTRIES,
            e.getMessage()
        );
    }

    public void testParseConfigAcceptsTheKeyBound() {
        String key = "k".repeat(StorageOptions.MAX_KEY_BYTES);
        Map<String, Object> config = new LinkedHashMap<>();
        config.put(key, "v");
        assertEquals("v", RestNamespaceAction.parseConfig(config).get(key));
    }

    public void testParseConfigRejectsOneByteAboveTheKeyBound() {
        String key = "k".repeat(StorageOptions.MAX_KEY_BYTES + 1);
        Map<String, Object> config = new LinkedHashMap<>();
        config.put(key, "v");
        Exception e = expectThrows(IllegalArgumentException.class, () -> RestNamespaceAction.parseConfig(config));
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
        assertEquals(value, RestNamespaceAction.parseConfig(config).get("auth_token"));
    }

    public void testParseConfigRejectsOneByteAboveTheValueBoundWithoutQuotingIt() {
        // A multi byte character shows the bound counts UTF 8 bytes, not chars.
        String value = "v".repeat(StorageOptions.MAX_VALUE_BYTES - 1) + "\u00e9";
        assertEquals(StorageOptions.MAX_VALUE_BYTES, value.length());
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("auth_token", value);
        Exception e = expectThrows(IllegalArgumentException.class, () -> RestNamespaceAction.parseConfig(config));
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
