/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit-level tests for {@link LanceRegistry}. Only covers the pieces that
 * stand on their own without the Lance JNI runtime, which now means just
 * the shared Arrow allocator; the historical per-index Dataset cache has
 * been removed along with the {@code _scan} / {@code _query} endpoints
 * that were its only consumers.
 */
public class LanceRegistryTests extends OpenSearchTestCase {

    public void testAllocatorIsSingletonAndNonNull() {
        assertNotNull(LanceRegistry.allocator());
        // The allocator is a static RootAllocator; two lookups must return the same instance.
        assertSame(LanceRegistry.allocator(), LanceRegistry.allocator());
    }
}
