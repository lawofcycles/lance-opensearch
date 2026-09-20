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
 * One multi-valued keyword ({@code List<Utf8>}) column of one Lance
 * fragment, held off-heap by {@link ColumnStore}: the fragment's sorted
 * term dictionary (see {@link CachedTermDictionary}), one row offset per
 * physical row plus one ({@code rows + 1} ints) and the rows' ordinals
 * flattened into one {@link IntVector}. Row {@code doc} owns the ordinals
 * at flat indexes {@code [rowStart(doc), rowEnd(doc))}, strictly
 * ascending and duplicate free as
 * {@link org.apache.lucene.index.SortedSetDocValues#nextOrd} requires.
 *
 * <p>A row with no ordinals (Arrow null list, empty list, or a deleted
 * row the load never saw) has {@code rowStart == rowEnd}; the heap path
 * reports all three as "no value" too, so no separate null marker is
 * kept. Neither vector's validity buffer is maintained.
 */
public final class CachedKeywordArrayColumn extends StoreEntry {

    private final CachedTermDictionary dictionary;
    private final IntVector offsets;
    private final IntVector ordinals;
    private final ArrowBuf offsetData;
    private final ArrowBuf ordinalData;
    private final int rows;
    private final long bytes;

    CachedKeywordArrayColumn(VarCharVector terms, IntVector offsets, IntVector ordinals, int rows) {
        this.dictionary = new CachedTermDictionary(terms);
        this.offsets = offsets;
        this.ordinals = ordinals;
        this.offsetData = offsets.getDataBuffer();
        this.ordinalData = ordinals.getDataBuffer();
        this.rows = rows;
        this.bytes = dictionary.bytes() + offsets.getValidityBuffer().capacity() + offsetData.capacity() + ordinals.getValidityBuffer()
            .capacity() + ordinalData.capacity();
    }

    /** Flat index of the first ordinal of row {@code doc}. */
    public int rowStart(int doc) {
        return offsetData.getInt((long) doc << 2);
    }

    /** Flat index one past the last ordinal of row {@code doc}. */
    public int rowEnd(int doc) {
        return offsetData.getInt((long) (doc + 1) << 2);
    }

    /** Ordinal at flat index {@code index}. */
    public int ordinal(int index) {
        return ordinalData.getInt((long) index << 2);
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
        offsets.close();
        ordinals.close();
    }
}
