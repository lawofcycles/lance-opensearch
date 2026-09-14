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
}
