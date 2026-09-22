/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import org.apache.lucene.util.FixedBitSet;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The doc id layout of a fragment with nested columns: parent doc ids,
 * reverse lookups, zero element rows, deleted rows and the live-doc
 * remap.
 */
public class NestedDocLayoutTests extends OpenSearchTestCase {

    /**
     * One nested column over four rows with 1, 3, 0 and 2 elements.
     * Blocks: [c0, p0=1], [c2, c3, c4, p1=5], [p2=6], [c7, c8, p3=9].
     */
    private static NestedDocLayout singleColumn() {
        return NestedDocLayout.build(new String[] { "items" }, 4, new int[][] { { 1, 3, 0, 2 } });
    }

    public void testParentDocIds() {
        NestedDocLayout layout = singleColumn();
        assertEquals(10, layout.maxDoc());
        assertEquals(4, layout.rows());
        assertEquals(6, layout.nestedDocCount());
        assertEquals(1, layout.parentDocOf(0));
        assertEquals(5, layout.parentDocOf(1));
        assertEquals(6, layout.parentDocOf(2));
        assertEquals(9, layout.parentDocOf(3));
    }

    public void testReverseLookup() {
        NestedDocLayout layout = singleColumn();
        int[] expectedRow = { 0, 0, 1, 1, 1, 1, 2, 3, 3, 3 };
        boolean[] expectedParent = { false, true, false, false, false, true, true, false, false, true };
        for (int doc = 0; doc < layout.maxDoc(); doc++) {
            assertEquals("row of doc " + doc, expectedRow[doc], layout.rowOfDoc(doc));
            assertEquals("isParent of doc " + doc, expectedParent[doc], layout.isParent(doc));
        }
        // Child docs resolve to their column and element ordinal.
        assertEquals(0, layout.childColumnOf(0));
        assertEquals(0, layout.childElementOrdinalOf(0, 0));
        assertEquals(0, layout.childColumnOf(4));
        assertEquals(3, layout.childElementOrdinalOf(0, 4));
        assertEquals(0, layout.childColumnOf(8));
        assertEquals(5, layout.childElementOrdinalOf(0, 8));
        // Parents report no column.
        assertEquals(-1, layout.childColumnOf(1));
        assertEquals(-1, layout.childColumnOf(6));
    }

    public void testTwoColumnsInterleave() {
        // Column a: 2, 0 elements; column b: 1, 1. Blocks:
        // [a0, a1, b0, p0=3], [b1, p1=5].
        NestedDocLayout layout = NestedDocLayout.build(new String[] { "a", "b" }, 2, new int[][] { { 2, 0 }, { 1, 1 } });
        assertEquals(6, layout.maxDoc());
        assertEquals(3, layout.parentDocOf(0));
        assertEquals(5, layout.parentDocOf(1));
        assertEquals(0, layout.childColumnOf(0));
        assertEquals(0, layout.childColumnOf(1));
        assertEquals(1, layout.childColumnOf(2));
        assertEquals(1, layout.childColumnOf(4));
        assertEquals(0, layout.childElementOrdinalOf(1, 2));
        assertEquals(1, layout.childElementOrdinalOf(1, 4));
        assertEquals(2, layout.childDocStart(1, 0));
        assertEquals(4, layout.childDocStart(1, 1));
        assertEquals(2, layout.totalElements(0));
        assertEquals(2, layout.totalElements(1));
        assertEquals(0, layout.columnIndexOf("a"));
        assertEquals(1, layout.columnIndexOf("b"));
        assertEquals(-1, layout.columnIndexOf("missing"));
    }

    public void testDeletedRowMasksParentOnly() {
        // Row 1 deleted: the offsets scan skipped it, so it has zero
        // elements; its parent doc is the only dead doc.
        NestedDocLayout layout = NestedDocLayout.build(new String[] { "items" }, 3, new int[][] { { 2, 0, 1 } });
        FixedBitSet rowLive = new FixedBitSet(3);
        rowLive.set(0);
        rowLive.set(2);
        FixedBitSet docLive = layout.docLiveDocs(rowLive);
        assertNotNull(docLive);
        // Blocks: [c0, c1, p0=2], [p1=3], [c4, p2=5].
        assertTrue(docLive.get(0));
        assertTrue(docLive.get(1));
        assertTrue(docLive.get(2));
        assertFalse(docLive.get(3));
        assertTrue(docLive.get(4));
        assertTrue(docLive.get(5));
        assertEquals(5, layout.numDocs(2));
    }

    public void testAllRowsLiveNeedsNoBitmap() {
        NestedDocLayout layout = singleColumn();
        assertNull(layout.docLiveDocs(null));
        assertEquals(layout.maxDoc(), layout.numDocs(layout.rows()));
    }
}
