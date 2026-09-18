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
