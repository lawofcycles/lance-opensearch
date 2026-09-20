/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * One column of one Lance fragment held off-heap by {@link ColumnStore}:
 * a {@link CachedColumn} (numeric or boolean), a
 * {@link CachedKeywordColumn} or a {@link CachedKeywordArrayColumn}.
 * The store keys, pins, evicts and accounts every kind the same way; the
 * subclasses differ only in the Arrow buffers they hold and the reads
 * they offer.
 *
 * <p>An entry stays in the store until it is evicted; a leaf that reads
 * it {@link #pin pins} it for the life of its request so the store does
 * not release the buffers underneath a running doc values iterator.
 */
abstract class StoreEntry {

    private final AtomicInteger pins = new AtomicInteger();

    /** Off-heap bytes the entry's buffers occupy (sum of their capacities). */
    public abstract long bytes();

    /** Release the buffers back to the allocator. Only the store calls this, and only when nothing pins the entry. */
    abstract void close();

    /** Keep the buffers alive while a request reads them. Balanced by {@link #unpin}. */
    void pin() {
        pins.incrementAndGet();
    }

    void unpin() {
        pins.decrementAndGet();
    }

    boolean isPinned() {
        return pins.get() > 0;
    }
}
