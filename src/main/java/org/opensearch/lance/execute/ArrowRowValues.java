/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.execute;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

/**
 * Reads one row's scalar out of an Arrow vector of an aggregate scan
 * batch: the integer family and booleans as a {@code long}, the
 * floating point family as a {@code double}, a null as the caller's
 * neutral element, and UTF-8 bytes into a reused scratch. These are
 * the only Arrow readers the pushdown uses; the spec records, the
 * columnar state and the scan loop all go through them, so a new
 * Arrow type is accepted in one place. Owns no state and knows
 * nothing about groups, metrics or the plan.
 */
final class ArrowRowValues {

    private ArrowRowValues() {}

    static double doubleKeyOf(FieldVector vector, int row) {
        if (vector instanceof Float4Vector v) {
            return v.get(row);
        }
        if (vector instanceof Float8Vector v) {
            return v.get(row);
        }
        throw new IllegalStateException(
            "unexpected Arrow vector " + vector.getClass().getSimpleName() + " for a floating point key " + vector.getName()
        );
    }

    /**
     * The row's UTF-8 bytes read into {@code scratch} through the
     * vector's offset and data buffers, so the row loop copies bytes
     * instead of allocating an array per row as {@code VarCharVector#get}
     * does.
     */
    static BytesRef readUtf8(VarCharVector vector, int row, BytesRefBuilder scratch) {
        long start = vector.getOffsetBuffer().getInt(row * 4L);
        int length = vector.getOffsetBuffer().getInt((row + 1) * 4L) - (int) start;
        scratch.grow(length);
        vector.getDataBuffer().getBytes(start, scratch.bytes(), 0, length);
        scratch.setLength(length);
        return scratch.get();
    }

    static long longOrZero(FieldVector vector, int row) {
        return vector.isNull(row) ? 0L : asLong(vector, row);
    }

    static double doubleOrZero(FieldVector vector, int row) {
        return doubleOr(vector, row, 0d);
    }

    static double doubleOr(FieldVector vector, int row, double whenNull) {
        if (vector.isNull(row)) {
            return whenNull;
        }
        if (vector instanceof Float4Vector v) {
            return v.get(row);
        }
        if (vector instanceof Float8Vector v) {
            return v.get(row);
        }
        return (double) asLong(vector, row);
    }

    static long asLong(FieldVector vector, int row) {
        if (vector instanceof BigIntVector v) {
            return v.get(row);
        }
        if (vector instanceof IntVector v) {
            return v.get(row);
        }
        if (vector instanceof SmallIntVector v) {
            return v.get(row);
        }
        if (vector instanceof TinyIntVector v) {
            return v.get(row);
        }
        if (vector instanceof BitVector v) {
            return v.get(row);
        }
        throw new IllegalStateException(
            "unexpected Arrow vector " + vector.getClass().getSimpleName() + " for aggregate column " + vector.getName()
        );
    }
}
