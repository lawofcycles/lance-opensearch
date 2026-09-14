/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit-level tests for {@link LanceRegistry}. The {@code attach} path itself
 * needs the Lance JNI runtime, so it belongs in an integration test; here we
 * only cover the pieces that stand on their own without the native library.
 */
public class LanceRegistryTests extends OpenSearchTestCase {

    public void testAllocatorIsSingletonAndNonNull() {
        assertNotNull(LanceRegistry.allocator());
        // The allocator is a static RootAllocator; two lookups must return the same instance.
        assertSame(LanceRegistry.allocator(), LanceRegistry.allocator());
    }

    public void testGetReturnsNullForUnknownName() {
        // Reading a name that was never attached must be null rather than throwing;
        // this is the contract callers depend on when the polling loop discovers a table.
        assertNull(LanceRegistry.get("never-attached-" + randomAlphaOfLength(6)));
    }
}
