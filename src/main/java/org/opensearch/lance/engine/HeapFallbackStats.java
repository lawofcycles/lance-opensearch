/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Node wide view of the heap column fallback: the bytes every open
 * {@link LanceShardColumnCache} on this node currently has charged to
 * the request circuit breaker for columns it materialised in heap
 * because the off-heap {@link ColumnStore} had no room (or the reader
 * has no store), and the number of loads the breaker refused. Read by
 * {@code GET /_lance/stats} as {@code column_store.heap_fallback_bytes}
 * and {@code column_store.heap_fallback_rejections}.
 *
 * <p>Static because the readers that charge the breaker are opened from
 * the fragment executor, the shard engine and tests alike, none of which
 * share an object the plugin could hand a counter through. The bytes
 * gauge returns to zero once every reader that charged it has closed.
 */
public final class HeapFallbackStats {

    private static final AtomicLong BYTES = new AtomicLong();
    private static final AtomicLong REJECTIONS = new AtomicLong();

    private HeapFallbackStats() {}

    /** Bytes currently charged to the request breaker for heap columns across every open reader on this node. */
    public static long bytes() {
        return BYTES.get();
    }

    /** Number of heap column loads the request breaker refused since node start. */
    public static long rejections() {
        return REJECTIONS.get();
    }

    static void charged(long bytes) {
        BYTES.addAndGet(bytes);
    }

    static void released(long bytes) {
        BYTES.addAndGet(-bytes);
    }

    static void rejected() {
        REJECTIONS.incrementAndGet();
    }
}
