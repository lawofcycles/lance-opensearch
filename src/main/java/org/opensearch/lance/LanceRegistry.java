/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.lance.Dataset;
import org.lance.OpenDatasetBuilder;
import org.lance.ReadOptions;

/**
 * Process-wide Arrow allocator shared by every plugin-owned code path that
 * needs to open a Lance dataset, plus the single-source helper for
 * constructing that {@link Dataset} instance. Every call site that used to
 * build the {@link OpenDatasetBuilder} inline now goes through
 * {@link #openDataset(String, StorageOptions)} so the storage-options
 * plumbing lives in one place.
 *
 * <p>Historically this class also cached a per-index {@code Dataset} for
 * two REST endpoints ({@code _scan} / {@code _query}); the endpoints were
 * undocumented PoC leftovers and both they and the caching layer have
 * been removed.
 */
public final class LanceRegistry {

    private static final BufferAllocator ALLOCATOR = new RootAllocator(Long.MAX_VALUE);

    private LanceRegistry() {}

    public static BufferAllocator allocator() {
        return ALLOCATOR;
    }

    /**
     * Open a Lance dataset against {@code uri} using {@code storageOptions}
     * for object-store credentials, endpoints, and timeouts. Callers pass
     * {@link StorageOptions#empty()} when the URI is a local filesystem
     * path; Lance's Rust {@code object_store} then relies on its own
     * environment-variable fallback (AWS_*, GCS_*, AZURE_*) for remote
     * URIs when the map is empty.
     */
    public static Dataset openDataset(String uri, StorageOptions storageOptions) {
        OpenDatasetBuilder builder = Dataset.open().allocator(ALLOCATOR).uri(uri);
        ReadOptions readOptions = storageOptions == null ? null : storageOptions.toReadOptionsOrNull();
        if (readOptions != null) {
            builder = builder.readOptions(readOptions);
        }
        return builder.build();
    }
}
