/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

/**
 * Process-wide Arrow allocator shared by every plugin-owned code path that
 * needs to open a Lance dataset. Historically this class also cached a
 * per-index {@code Dataset} for two REST endpoints ({@code _scan} /
 * {@code _query}); the endpoints were undocumented PoC leftovers and both
 * they and the caching layer have been removed. The allocator remains and
 * is the only public surface.
 */
public final class LanceRegistry {

    private static final BufferAllocator ALLOCATOR = new RootAllocator(Long.MAX_VALUE);

    private LanceRegistry() {}

    public static BufferAllocator allocator() {
        return ALLOCATOR;
    }
}
