/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
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

    public void testEveryArrayIsReservedBeforeItIsAllocatedAndReportedByHeapBytes() {
        // The (offset, score) buffers are reserved at each growth step
        // (8, 16, 32 entries), the sorted view when it is first built;
        // the packed array the sort uses is reserved for the build only.
        // heapBytes is what the owner gives back, so it must equal the
        // bytes still reserved at every point.
        CircuitBreaker breaker = LanceFtsQueryTests.requestBreaker("1mb");
        LanceHitsAccounting accounting = new LanceHitsAccounting(breaker);
        LanceFragmentHits hits = new LanceFragmentHits(accounting);
        assertEquals(0L, hits.heapBytes());
        assertEquals(0L, breaker.getUsed());

        hits.add(5, 1f);
        assertEquals(8L * LanceFragmentHits.BYTES_PER_HIT, breaker.getUsed());
        for (int i = 1; i < 8; i++) {
            hits.add(i, 1f);
        }
        assertEquals("eight entries fit the initial capacity", 8L * LanceFragmentHits.BYTES_PER_HIT, breaker.getUsed());
        hits.add(100, 1f);
        assertEquals("the ninth entry doubles the buffers", 16L * LanceFragmentHits.BYTES_PER_HIT, breaker.getUsed());
        for (int i = 9; i < 17; i++) {
            hits.add(100 + i, 1f);
        }
        assertEquals(32L * LanceFragmentHits.BYTES_PER_HIT, breaker.getUsed());
        assertEquals(17, hits.size());
        assertEquals(breaker.getUsed(), hits.heapBytes());

        hits.sortedDocIds();
        assertEquals(
            "sorted view adds size entries; the packed array is returned",
            (32L + 17L) * LanceFragmentHits.BYTES_PER_HIT,
            breaker.getUsed()
        );
        assertEquals(breaker.getUsed(), hits.heapBytes());
        hits.sortedScores();
        assertEquals("the sorted view is built once", (32L + 17L) * LanceFragmentHits.BYTES_PER_HIT, breaker.getUsed());
        assertEquals(breaker.getUsed(), accounting.reservedBytes());

        accounting.release(hits.heapBytes());
        assertEquals(0L, breaker.getUsed());
    }

    public void testRefusedGrowthLeavesTheBufferIntact() {
        // Limit for one buffer at the initial capacity: the ninth add
        // is refused, the eight hits already added stay readable, and
        // nothing beyond the first buffer is counted.
        long oneBuffer = 8L * LanceFragmentHits.BYTES_PER_HIT;
        CircuitBreaker breaker = LanceFtsQueryTests.requestBreaker(oneBuffer + "b");
        LanceHitsAccounting accounting = new LanceHitsAccounting(breaker);
        LanceFragmentHits hits = new LanceFragmentHits(accounting);
        for (int i = 0; i < 8; i++) {
            hits.add(i, (float) i);
        }
        expectThrows(CircuitBreakingException.class, () -> hits.add(8, 8f));
        assertEquals(8, hits.size());
        assertEquals(oneBuffer, breaker.getUsed());
        assertEquals(oneBuffer, hits.heapBytes());
        // The sorted view of eight entries needs more than the limit too.
        expectThrows(CircuitBreakingException.class, hits::sortedDocIds);
        assertEquals(oneBuffer, breaker.getUsed());
        accounting.close();
        assertEquals(0L, breaker.getUsed());
    }
}
