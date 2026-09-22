/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import org.apache.lucene.util.FixedBitSet;

/**
 * Doc id layout of one fragment whose table has {@code List<Struct>}
 * (nested) columns. OpenSearch stores each element of a nested field as
 * its own hidden Lucene document placed immediately before the parent
 * document, so a leaf over such a fragment exposes more docs than the
 * fragment has rows. For row {@code r} whose nested columns hold
 * {@code n_1, ..., n_k} elements the layout emits the child docs of
 * column 1 ({@code n_1} docs), then column 2, ..., then the parent doc;
 * the parent is always the last doc of its block, which is what
 * {@code ToParentBlockJoinQuery} requires.
 *
 * <p>Derived once per (fragment, table version) from the list offsets of
 * every nested column and cached on the fragment metadata, like the
 * live-row bitmap. Rows a deletion file hides are skipped by the scan
 * that reads the offsets, so a deleted row contributes no child docs and
 * only its parent doc is masked by the live-doc bitmap.
 *
 * <p>Memory per fragment: 4 bytes per doc ({@link #rowOfDoc}), plus a
 * fixed per row overhead of 4 bytes ({@link #parentDocOf}) and, per
 * nested column, 4 bytes per row plus one entry (the element ordinal
 * prefix sums in {@code elemStart}).
 */
final class NestedDocLayout {

    private final String[] columns;
    private final int rows;
    private final int maxDoc;
    /** Parent doc id of each row, ascending. */
    private final int[] parentDocOf;
    /** Row of each doc (the row whose block the doc belongs to). */
    private final int[] rowOfDoc;
    /**
     * Per column, prefix sums of the element counts over the rows:
     * {@code elemStart[c][r]} is the element ordinal of row {@code r}'s
     * first element in column {@code c}, and {@code elemStart[c][rows]}
     * is the column's total element count.
     */
    private final int[][] elemStart;

    private NestedDocLayout(String[] columns, int rows, int maxDoc, int[] parentDocOf, int[] rowOfDoc, int[][] elemStart) {
        this.columns = columns;
        this.rows = rows;
        this.maxDoc = maxDoc;
        this.parentDocOf = parentDocOf;
        this.rowOfDoc = rowOfDoc;
        this.elemStart = elemStart;
    }

    /**
     * Build the layout from per-column, per-row element counts.
     *
     * @param columns nested column names, in the order the counts are
     *                keyed; kept by reference
     * @param rows    physical rows of the fragment
     * @param counts  {@code counts[c][r]} is the number of elements of
     *                column {@code c} in row {@code r} (0 for an Arrow
     *                null list, an empty list, or a deleted row)
     */
    static NestedDocLayout build(String[] columns, int rows, int[][] counts) {
        int k = columns.length;
        int[][] elemStart = new int[k][rows + 1];
        long totalElements = 0L;
        for (int c = 0; c < k; c++) {
            int cursor = 0;
            for (int r = 0; r < rows; r++) {
                elemStart[c][r] = cursor;
                cursor += counts[c][r];
            }
            elemStart[c][rows] = cursor;
            totalElements += cursor;
        }
        long docs = rows + totalElements;
        if (docs > Integer.MAX_VALUE) {
            throw new IllegalStateException(
                "fragment has " + rows + " rows and " + totalElements + " nested elements, more docs than one Lucene leaf may hold"
            );
        }
        int maxDoc = (int) docs;
        int[] parentDocOf = new int[rows];
        int[] rowOfDoc = new int[maxDoc];
        int doc = 0;
        for (int r = 0; r < rows; r++) {
            for (int c = 0; c < k; c++) {
                for (int e = 0; e < counts[c][r]; e++) {
                    rowOfDoc[doc++] = r;
                }
            }
            rowOfDoc[doc] = r;
            parentDocOf[r] = doc++;
        }
        return new NestedDocLayout(columns, rows, maxDoc, parentDocOf, rowOfDoc, elemStart);
    }

    int maxDoc() {
        return maxDoc;
    }

    int rows() {
        return rows;
    }

    /** Child docs across all nested columns. */
    int nestedDocCount() {
        return maxDoc - rows;
    }

    int parentDocOf(int row) {
        return parentDocOf[row];
    }

    int rowOfDoc(int doc) {
        return rowOfDoc[doc];
    }

    boolean isParent(int doc) {
        return parentDocOf[rowOfDoc[doc]] == doc;
    }

    /** Index of {@code column} in the layout's column order, or -1. */
    int columnIndexOf(String column) {
        for (int c = 0; c < columns.length; c++) {
            if (columns[c].equals(column)) {
                return c;
            }
        }
        return -1;
    }

    /** Total elements of column {@code c} across the fragment's rows. */
    int totalElements(int c) {
        return elemStart[c][rows];
    }

    /** Element ordinal of row {@code r}'s first element in column {@code c}. */
    int elementBase(int c, int r) {
        return elemStart[c][r];
    }

    /** Number of elements of column {@code c} in row {@code r}. */
    int childCount(int c, int r) {
        return elemStart[c][r + 1] - elemStart[c][r];
    }

    /** First doc id of the block of row {@code r} (its first child doc, or the parent when it has none). */
    private int blockStart(int r) {
        return r == 0 ? 0 : parentDocOf[r - 1] + 1;
    }

    /** First doc id of column {@code c}'s child docs in row {@code r}. */
    int childDocStart(int c, int r) {
        int start = blockStart(r);
        for (int before = 0; before < c; before++) {
            start += childCount(before, r);
        }
        return start;
    }

    /**
     * Nested column index of a child doc, or -1 when {@code doc} is a
     * parent doc.
     */
    int childColumnOf(int doc) {
        int r = rowOfDoc[doc];
        if (parentDocOf[r] == doc) {
            return -1;
        }
        int offset = doc - blockStart(r);
        for (int c = 0; c < columns.length; c++) {
            int n = childCount(c, r);
            if (offset < n) {
                return c;
            }
            offset -= n;
        }
        throw new IllegalStateException("doc " + doc + " is neither a parent nor a child of any nested column");
    }

    /**
     * Element ordinal (within its column, across the fragment's rows) of
     * a child doc. Only valid when {@link #childColumnOf} returned the
     * same column.
     */
    int childElementOrdinalOf(int c, int doc) {
        int r = rowOfDoc[doc];
        return elemStart[c][r] + (doc - childDocStart(c, r));
    }

    /**
     * The live-doc bitmap of the leaf's doc space for a fragment whose
     * row-space bitmap is {@code rowLive}, or {@code null} when every
     * row is live. Deleted rows contribute no child docs (the offsets
     * scan skips them), so only their parent docs are masked.
     */
    FixedBitSet docLiveDocs(FixedBitSet rowLive) {
        if (rowLive == null) {
            return null;
        }
        FixedBitSet live = new FixedBitSet(maxDoc);
        live.set(0, maxDoc);
        for (int r = 0; r < rows; r++) {
            if (!rowLive.get(r)) {
                live.clear(parentDocOf[r]);
            }
        }
        return live;
    }

    /**
     * Live docs of the leaf given the fragment's live row count: every
     * child doc is live (deleted rows have none), so only dead parents
     * are subtracted.
     */
    int numDocs(int liveRows) {
        return maxDoc - (rows - liveRows);
    }
}
