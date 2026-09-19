/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.query;

import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.test.OpenSearchTestCase;

public class LanceSparseHitIteratorTests extends OpenSearchTestCase {

    public void testEmptyIteratorReturnsNoMoreDocsImmediately() {
        LanceSparseHitIterator it = new LanceSparseHitIterator(new int[0], new float[0], 0);
        assertEquals(-1, it.docID());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());
    }

    public void testNextDocWalksAscendingDocIds() {
        int[] docs = { 3, 7, 42, 100 };
        float[] scores = { 0.5f, 1.0f, 2.5f, 5.0f };
        LanceSparseHitIterator it = new LanceSparseHitIterator(docs, scores, docs.length);
        assertEquals(-1, it.docID());
        assertEquals(3, it.nextDoc());
        assertEquals(0.5f, it.currentScore(), 0f);
        assertEquals(7, it.nextDoc());
        assertEquals(1.0f, it.currentScore(), 0f);
        assertEquals(42, it.nextDoc());
        assertEquals(2.5f, it.currentScore(), 0f);
        assertEquals(100, it.nextDoc());
        assertEquals(5.0f, it.currentScore(), 0f);
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());
    }

    public void testAdvanceJumpsToOrPastTarget() {
        int[] docs = { 3, 7, 42, 100 };
        float[] scores = { 0.5f, 1.0f, 2.5f, 5.0f };
        LanceSparseHitIterator it = new LanceSparseHitIterator(docs, scores, docs.length);
        assertEquals(42, it.advance(42));
        assertEquals(2.5f, it.currentScore(), 0f);
        assertEquals(100, it.advance(50));
        assertEquals(5.0f, it.currentScore(), 0f);
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.advance(999));
    }

    public void testAdvanceRespectsCurrentCursorForwardOnly() {
        int[] docs = { 1, 2, 3, 4 };
        float[] scores = { 0.1f, 0.2f, 0.3f, 0.4f };
        LanceSparseHitIterator it = new LanceSparseHitIterator(docs, scores, docs.length);
        assertEquals(2, it.advance(2));
        // advance past current position: never rewinds even if target < current.
        assertEquals(3, it.advance(2));
    }

    public void testCostReturnsHitCount() {
        int[] docs = { 5, 10, 20 };
        float[] scores = { 0.1f, 0.2f, 0.3f };
        LanceSparseHitIterator it = new LanceSparseHitIterator(docs, scores, docs.length);
        assertEquals(3L, it.cost());
    }

    public void testPartialArraysHonourSizeArgument() {
        // docs / scores arrays are sized larger than actual hits; the size arg bounds iteration.
        int[] docs = { 1, 2, 99, 99 };
        float[] scores = { 0.1f, 0.2f, -1f, -1f };
        LanceSparseHitIterator it = new LanceSparseHitIterator(docs, scores, 2);
        assertEquals(1, it.nextDoc());
        assertEquals(2, it.nextDoc());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());
    }
}
