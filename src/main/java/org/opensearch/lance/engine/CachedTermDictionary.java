/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.VarCharVector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

/**
 * Sorted, duplicate-free term dictionary of one keyword column on one
 * fragment, held in an Arrow {@link VarCharVector}. Ordinal {@code ord}
 * is the term at index {@code ord} of the vector; the vector is filled
 * in unsigned byte order by {@link KeywordDictionaryBuilder#writeTerms},
 * which is the order {@link org.apache.lucene.index.SortedDocValues}
 * requires.
 *
 * <p>Reads address the offset and data buffers directly. A term is
 * handed out by copying its bytes into the caller's scratch
 * {@link BytesRefBuilder}: a {@link BytesRef} needs a {@code byte[]}, so
 * a copy is unavoidable, and Lucene's own doc values codecs return their
 * terms the same way. Comparisons for {@link #lookupTerm} read the
 * off-heap bytes in place and copy nothing.
 */
final class CachedTermDictionary {

    private final VarCharVector vector;
    private final ArrowBuf offsets;
    private final ArrowBuf data;
    private final int count;
    private final long bytes;

    CachedTermDictionary(VarCharVector vector) {
        this.vector = vector;
        this.offsets = vector.getOffsetBuffer();
        this.data = vector.getDataBuffer();
        this.count = vector.getValueCount();
        this.bytes = vector.getValidityBuffer().capacity() + offsets.capacity() + data.capacity();
    }

    /** Number of distinct terms. */
    int valueCount() {
        return count;
    }

    /** Copy term {@code ord} into {@code scratch} and return the scratch's {@link BytesRef}. */
    BytesRef term(int ord, BytesRefBuilder scratch) {
        int start = offsets.getInt((long) ord << 2);
        int length = offsets.getInt((long) (ord + 1) << 2) - start;
        scratch.grow(length);
        data.getBytes(start, scratch.bytes(), 0, length);
        scratch.setLength(length);
        return scratch.get();
    }

    /**
     * Ordinal of {@code key}, or {@code -(insertionPoint + 1)} when the
     * dictionary does not hold it, as
     * {@link org.apache.lucene.index.SortedDocValues#lookupTerm} defines.
     * Binary search comparing the off-heap bytes unsigned, without
     * copying any term.
     */
    int lookupTerm(BytesRef key) {
        int low = 0;
        int high = count - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int cmp = compare(mid, key);
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return -(low + 1);
    }

    /** Unsigned byte comparison of term {@code ord} against {@code key}, negative when the term sorts first. */
    private int compare(int ord, BytesRef key) {
        int start = offsets.getInt((long) ord << 2);
        int length = offsets.getInt((long) (ord + 1) << 2) - start;
        int common = Math.min(length, key.length);
        for (int i = 0; i < common; i++) {
            int diff = (data.getByte(start + i) & 0xFF) - (key.bytes[key.offset + i] & 0xFF);
            if (diff != 0) {
                return diff;
            }
        }
        return length - key.length;
    }

    /** Sum of the validity, offset and data buffer capacities. */
    long bytes() {
        return bytes;
    }

    void close() {
        vector.close();
    }
}
