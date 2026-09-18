/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit-level assertions for {@link LanceEngineFactory}. The actual
 * {@code newReadWriteEngine} path opens a Lance {@code Dataset} through JNI
 * and therefore lives in an integration test; here we lock in the setting
 * keys that {@code LancePlugin.getSettings()} and the README both refer to,
 * so a rename cannot slip through unnoticed.
 */
public class LanceEngineFactoryTests extends OpenSearchTestCase {

    public void testTableSettingKey() {
        assertEquals("index.lance.table", LanceEngineFactory.TABLE_SETTING);
    }

    public void testPrimaryKeyFieldSettingKey() {
        assertEquals("index.lance.primary_key_field", LanceEngineFactory.PRIMARY_KEY_FIELD_SETTING);
    }

    public void testPrimaryKeyTypeSettingKey() {
        assertEquals("index.lance.primary_key_type", LanceEngineFactory.PRIMARY_KEY_TYPE_SETTING);
    }

    public void testMultiFieldsSettingKey() {
        assertEquals("index.lance.multi_fields", LanceEngineFactory.MULTI_FIELDS_SETTING);
    }

    public void testMultiFieldsSerialiseDeserialiseRoundTrip() {
        // Empty map round-trips to empty string and back to empty map so
        // absence of a multi_fields clause never persists a setting.
        assertEquals("", org.opensearch.lance.rest.RestAttachAction.serialiseMultiFields(java.util.Collections.emptyMap()));
        assertTrue(org.opensearch.lance.rest.RestAttachAction.deserialiseMultiFields("").isEmpty());
        assertTrue(org.opensearch.lance.rest.RestAttachAction.deserialiseMultiFields(null).isEmpty());

        // Nested map with one keyword sub-field survives the JSON round
        // trip so the engine sees exactly what attach persisted.
        java.util.LinkedHashMap<String, java.util.LinkedHashMap<String, String>> in = new java.util.LinkedHashMap<>();
        java.util.LinkedHashMap<String, String> bodySubs = new java.util.LinkedHashMap<>();
        bodySubs.put("raw", "keyword");
        in.put("body", bodySubs);
        String json = org.opensearch.lance.rest.RestAttachAction.serialiseMultiFields(in);
        assertEquals("{\"body\":{\"raw\":\"keyword\"}}", json);

        java.util.Map<String, java.util.LinkedHashMap<String, String>> out = org.opensearch.lance.rest.RestAttachAction
            .deserialiseMultiFields(json);
        assertEquals(1, out.size());
        assertEquals("keyword", out.get("body").get("raw"));
    }

    public void testPrimaryKeyTypeFromSettingFallsBackToLong() {
        // Empty and unknown strings must return LONG so pre-#24 indices
        // without the setting continue to open with the integer lookup
        // path. Known values map to their enum. NONE is only ever set at
        // runtime when the field name is empty, but the enum still round
        // trips through fromSetting so callers that persist "none"
        // (e.g. a future migration tool) see the same value on read.
        assertEquals(LanceEngineFactory.LancePrimaryKeyType.LONG, LanceEngineFactory.LancePrimaryKeyType.fromSetting(""));
        assertEquals(LanceEngineFactory.LancePrimaryKeyType.LONG, LanceEngineFactory.LancePrimaryKeyType.fromSetting(null));
        assertEquals(LanceEngineFactory.LancePrimaryKeyType.LONG, LanceEngineFactory.LancePrimaryKeyType.fromSetting("gibberish"));
        assertEquals(LanceEngineFactory.LancePrimaryKeyType.LONG, LanceEngineFactory.LancePrimaryKeyType.fromSetting("long"));
        assertEquals(LanceEngineFactory.LancePrimaryKeyType.KEYWORD, LanceEngineFactory.LancePrimaryKeyType.fromSetting("keyword"));
        assertEquals(LanceEngineFactory.LancePrimaryKeyType.NONE, LanceEngineFactory.LancePrimaryKeyType.fromSetting("none"));
    }
}
