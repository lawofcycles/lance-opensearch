/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import org.opensearch.test.OpenSearchTestCase;

/**
 * The per-fragment hit buffer the Lance Weights fill from their
 * shard-level scan: growth, the sorted view, and its stability.
 */
public class LanceFragmentHitsTests extends OpenSearchTestCase {

    public void testSortedViewOrdersByOffsetAndKeepsScoresAligned() {
        LanceFragmentHits hits = new LanceFragmentHits();
        // Lance returns rows in score order, not offset order.
        hits.add(40, 0.9f);
        hits.add(7, 0.5f);
        hits.add(1000, 0.1f);
        hits.add(12, 0.7f);
        assertEquals(4, hits.size());
        assertArrayEquals(new int[] { 7, 12, 40, 1000 }, hits.sortedDocIds());
        assertArrayEquals(new float[] { 0.5f, 0.7f, 0.9f, 0.1f }, hits.sortedScores(), 0f);
    }

    public void testSortedViewIsTheSameArrayOnEveryCall() {
        // The executor hints the fragment reader with sortedDocIds()
        // before the scorer for the leaf exists, and the scorer hands
        // the reader the array it gets from the same call. The reader
        // recognises a hint it already holds by reference, so the two
        // calls must return one array.
        LanceFragmentHits hits = new LanceFragmentHits();
        hits.add(3, 1f);
        hits.add(1, 2f);
        assertSame(hits.sortedDocIds(), hits.sortedDocIds());
        assertSame(hits.sortedScores(), hits.sortedScores());
        assertEquals(0, LanceFragmentHits.EMPTY.size());
        assertEquals(0, LanceFragmentHits.EMPTY.sortedDocIds().length);
        assertSame(LanceFragmentHits.EMPTY.sortedDocIds(), LanceFragmentHits.EMPTY.sortedDocIds());
    }

    public void testBufferGrowsPastEightEntries() {
        // Eight entries is the initial capacity, so a scan that returns
        // at most k rows per fragment does not reallocate for a typical
        // top-10 query; the ninth add must still be kept.
        LanceFragmentHits hits = new LanceFragmentHits();
        for (int i = 0; i < 9; i++) {
            hits.add(8 - i, (float) i);
        }
        assertEquals(9, hits.size());
        int[] docIds = hits.sortedDocIds();
        float[] scores = hits.sortedScores();
        for (int i = 0; i < 9; i++) {
            assertEquals(i, docIds[i]);
            assertEquals((float) (8 - i), scores[i], 0f);
        }
    }
}
