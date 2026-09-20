/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

/**
 * One single-valued keyword column of one Lance fragment, held off-heap
 * by {@link ColumnStore}: the fragment's sorted term dictionary (a
 * {@link VarCharVector}, see {@link CachedTermDictionary}) and one
 * ordinal per physical row (an {@link IntVector}, {@code -1} for Arrow
 * null and for deleted rows, which the load never sees). A
 * {@link org.apache.lucene.index.SortedDocValues} over this column reads
 * {@link #ord} for the doc and {@link #term} for the ordinal.
 *
 * <p>The ordinal vector's validity buffer is not maintained; the
 * {@code -1} sentinel in the data buffer is the only null marker, so
 * reads are one {@code getInt} on the data buffer.
 */
public final class CachedKeywordColumn extends StoreEntry {

    private final CachedTermDictionary dictionary;
    private final IntVector ordinals;
    private final ArrowBuf ordinalData;
    private final int rows;
    private final long bytes;

    CachedKeywordColumn(VarCharVector terms, IntVector ordinals, int rows) {
        this.dictionary = new CachedTermDictionary(terms);
        this.ordinals = ordinals;
        this.ordinalData = ordinals.getDataBuffer();
        this.rows = rows;
        this.bytes = dictionary.bytes() + ordinals.getValidityBuffer().capacity() + ordinalData.capacity();
    }

    /** Ordinal of row {@code doc}, or {@code -1} when the row has no value. */
    public int ord(int doc) {
        return ordinalData.getInt((long) doc << 2);
    }

    /** Number of distinct terms on this fragment. */
    public int valueCount() {
        return dictionary.valueCount();
    }

    /** Term {@code ord}, copied into {@code scratch}. */
    public BytesRef term(int ord, BytesRefBuilder scratch) {
        return dictionary.term(ord, scratch);
    }

    /** Ordinal of {@code key}, or {@code -(insertionPoint + 1)} when absent. */
    public int lookupTerm(BytesRef key) {
        return dictionary.lookupTerm(key);
    }

    /** Number of physical rows the column covers ({@code maxDoc} of the leaf). */
    public int rows() {
        return rows;
    }

    @Override
    public long bytes() {
        return bytes;
    }

    @Override
    void close() {
        dictionary.close();
        ordinals.close();
    }
}
