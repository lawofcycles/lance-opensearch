/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;

/**
 * One numeric or boolean column of one Lance fragment, held off-heap in
 * an Arrow vector owned by {@link ColumnStore}. Values are already
 * normalised the way {@link LanceFragmentLeafReader#readAsLong} does
 * (sortable long for floats, epoch millis for dates, raw bit pattern for
 * unsigned), so a {@link org.apache.lucene.index.NumericDocValues} over
 * this column returns {@link #get} for a doc whose {@link #isSet} is true.
 *
 * <p>Reads go straight to the Arrow buffers: the value buffer holds one
 * {@code long} per physical row (a {@code BigIntVector}) or one bit per
 * row (a {@code BitVector} for boolean columns) and the validity buffer
 * holds one bit per row, set for rows that carry a value. Deleted rows
 * and Arrow nulls leave their validity bit clear.
 */
public final class CachedColumn extends StoreEntry {

    private final FieldVector vector;
    private final ArrowBuf validity;
    private final ArrowBuf data;
    private final boolean bit;
    private final int rows;
    private final long bytes;

    CachedColumn(FieldVector vector, int rows) {
        this.vector = vector;
        this.validity = vector.getValidityBuffer();
        this.data = vector.getDataBuffer();
        this.bit = vector instanceof BitVector;
        this.rows = rows;
        this.bytes = validity.capacity() + data.capacity();
    }

    /** Whether row {@code doc} carries a value. */
    public boolean isSet(int doc) {
        return BitVectorHelper.get(validity, doc) != 0;
    }

    /** Normalised value of row {@code doc}; only meaningful when {@link #isSet} is true. */
    public long get(int doc) {
        return bit ? BitVectorHelper.get(data, doc) : data.getLong((long) doc << 3);
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
        vector.close();
    }
}
