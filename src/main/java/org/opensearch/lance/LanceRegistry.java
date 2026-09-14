/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.lance.Dataset;

/** Node local registry of attached Lance tables. */
public final class LanceRegistry {

    private static final BufferAllocator ALLOCATOR = new RootAllocator(Long.MAX_VALUE);
    private static final Map<String, Dataset> TABLES = new ConcurrentHashMap<>();

    private LanceRegistry() {}

    public static BufferAllocator allocator() {
        return ALLOCATOR;
    }

    public static Dataset attach(String name, String tableUri) {
        Dataset dataset = Dataset.open(tableUri, ALLOCATOR);
        Dataset previous = TABLES.put(name, dataset);
        if (previous != null) {
            previous.close();
        }
        return dataset;
    }

    public static Dataset get(String name) {
        return TABLES.get(name);
    }
}
