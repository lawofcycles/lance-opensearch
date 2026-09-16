/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link LanceDispatchMode}. The parse routine is the only
 * behaviour worth covering here: everything else on the enum is
 * generated. Verify both the accepted spellings and the shape of the
 * error surface so operators see a helpful validation message when
 * they misspell the setting.
 */
public class LanceDispatchModeTests extends OpenSearchTestCase {

    public void testParsesAcceptedSpellings() {
        assertEquals(LanceDispatchMode.SHARD, LanceDispatchMode.parse("shard"));
        assertEquals(LanceDispatchMode.FRAGMENT, LanceDispatchMode.parse("fragment"));
    }

    public void testParseIsCaseInsensitive() {
        assertEquals(LanceDispatchMode.SHARD, LanceDispatchMode.parse("SHARD"));
        assertEquals(LanceDispatchMode.FRAGMENT, LanceDispatchMode.parse("Fragment"));
        assertEquals(LanceDispatchMode.FRAGMENT, LanceDispatchMode.parse("  fragment  "));
    }

    public void testParseRejectsUnknownValue() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> LanceDispatchMode.parse("both"));
        assertTrue(e.getMessage(), e.getMessage().contains("shard"));
        assertTrue(e.getMessage(), e.getMessage().contains("fragment"));
        assertTrue(e.getMessage(), e.getMessage().contains("[both]"));
    }

    public void testParseRejectsNull() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> LanceDispatchMode.parse(null));
        assertTrue(e.getMessage(), e.getMessage().contains("null"));
    }
}
