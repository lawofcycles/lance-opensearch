/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.io.IOException;

import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;

/**
 * Point values of a geo column: one root cell holding every present
 * row, no children. {@code PointValues.intersect} and the estimate
 * methods drive the visits; a cell relation of
 * {@code CELL_INSIDE_QUERY} takes {@link PointTree#visitDocIDs} and
 * {@code CELL_CROSSES_QUERY} falls to
 * {@link PointTree#visitDocValues} because the tree has no children
 * to descend into.
 *
 * <p>Owns the points view and the {@code LatLonPoint} packing of the
 * encoded {@code lat|lon} longs only. The encoded array, its presence
 * bitmap and the bounds come from {@code LanceColumnLoader}'s geo
 * column load; the doc values over the same array are served by
 * {@code LanceDocValues}, and the leaf maps rows to doc ids.
 */
final class GeoPointPointValues extends PointValues {
    /** Leaf the points belong to; maps physical rows to the doc ids the visitor receives. */
    private final LanceFragmentLeafReader leaf;
    private final long[] column;
    private final FixedBitSet presence;
    private final int[] bounds;

    GeoPointPointValues(LanceFragmentLeafReader leaf, long[] column, FixedBitSet presence, int[] bounds) {
        this.leaf = leaf;
        this.column = column;
        this.presence = presence;
        this.bounds = bounds;
    }

    /** Pack an encoded (lat, lon) pair into the {@code LatLonPoint} 2x4-byte comparable form. */
    static byte[] packGeo(int latEncoded, int lonEncoded) {
        byte[] packed = new byte[2 * Integer.BYTES];
        NumericUtils.intToSortableBytes(latEncoded, packed, 0);
        NumericUtils.intToSortableBytes(lonEncoded, packed, Integer.BYTES);
        return packed;
    }

    @Override
    public PointTree getPointTree() {
        return new GeoPointTree();
    }

    @Override
    public byte[] getMinPackedValue() {
        return packGeo(bounds[0], bounds[2]);
    }

    @Override
    public byte[] getMaxPackedValue() {
        return packGeo(bounds[1], bounds[3]);
    }

    @Override
    public int getNumDimensions() {
        return 2;
    }

    @Override
    public int getNumIndexDimensions() {
        return 2;
    }

    @Override
    public int getBytesPerDimension() {
        return Integer.BYTES;
    }

    @Override
    public long size() {
        return bounds[4];
    }

    @Override
    public int getDocCount() {
        return bounds[4];
    }

    private final class GeoPointTree implements PointTree {
        @Override
        public PointTree clone() {
            return new GeoPointTree();
        }

        @Override
        public boolean moveToChild() {
            return false;
        }

        @Override
        public boolean moveToSibling() {
            return false;
        }

        @Override
        public boolean moveToParent() {
            return false;
        }

        @Override
        public byte[] getMinPackedValue() {
            return GeoPointPointValues.this.getMinPackedValue();
        }

        @Override
        public byte[] getMaxPackedValue() {
            return GeoPointPointValues.this.getMaxPackedValue();
        }

        @Override
        public long size() {
            return bounds[4];
        }

        @Override
        public void visitDocIDs(IntersectVisitor visitor) throws IOException {
            visitor.grow(bounds[4]);
            for (int row = presence.nextSetBit(0); row != DocIdSetIterator.NO_MORE_DOCS; row = row + 1 < presence.length()
                ? presence.nextSetBit(row + 1)
                : DocIdSetIterator.NO_MORE_DOCS) {
                visitor.visit(leaf.docOfRow(row));
            }
        }

        @Override
        public void visitDocValues(IntersectVisitor visitor) throws IOException {
            visitor.grow(bounds[4]);
            for (int row = presence.nextSetBit(0); row != DocIdSetIterator.NO_MORE_DOCS; row = row + 1 < presence.length()
                ? presence.nextSetBit(row + 1)
                : DocIdSetIterator.NO_MORE_DOCS) {
                long value = column[row];
                visitor.visit(leaf.docOfRow(row), packGeo((int) (value >>> 32), (int) value));
            }
        }
    }
}
