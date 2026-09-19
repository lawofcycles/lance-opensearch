/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.query;

import org.apache.lucene.search.DocIdSetIterator;

/**
 * DocIdSetIterator over a sparse hit set.
 *
 * <p>{@link LanceFtsQuery} and {@link LanceKnnQuery} both used to
 * materialise hits by writing scores into a {@code float[maxDoc]}
 * and setting bits in a {@code FixedBitSet(maxDoc)}. On a table with
 * fragment {@code maxDoc} of 250,000 that is 1 MB of scores plus
 * 31 KB of bitset per fragment per query, and the sparse Lance FTS
 * scorer or nearest scan actually populates a handful of entries.
 * QA measured this as the main driver behind
 * {@code lance.fragment_dispatch.max_concurrent} not scaling
 * (issue #47): 64 concurrent sort queries filled the parent breaker
 * with per-query {@code float[maxDoc]} allocations that G1 refused
 * to reclaim until an explicit {@code System.gc}.
 *
 * <p>This iterator stores hits in two parallel arrays sized to the
 * actual hit count, sorted by ascending docId to satisfy the
 * {@link DocIdSetIterator} contract. The caller pushes the score
 * for the current cursor into the accompanying {@code Scorer} via
 * {@link #currentScore()} rather than looking it up by docId.
 *
 * <p>Not thread-safe: iterators are per-slice/leaf state and Lucene
 * never shares them across threads.
 */
public final class LanceSparseHitIterator extends DocIdSetIterator {

    private final int[] docIds;
    private final float[] scores;
    private final int size;
    private int cursor = -1;

    /**
     * @param docIds hit docIds sorted ascending (invariant checked in assertions only)
     * @param scores parallel array of hit scores; index i corresponds to docIds[i]
     * @param size number of valid entries at the head of docIds / scores
     */
    public LanceSparseHitIterator(int[] docIds, float[] scores, int size) {
        assert docIds.length >= size : "docIds too short";
        assert scores.length >= size : "scores too short";
        assert isSortedAscending(docIds, size) : "docIds must be ascending";
        this.docIds = docIds;
        this.scores = scores;
        this.size = size;
    }

    /**
     * Score at the current cursor position. Undefined before the
     * first {@link #nextDoc}. Meant to be called from the enclosing
     * Scorer's {@code score()} implementation.
     */
    public float currentScore() {
        return scores[cursor];
    }

    @Override
    public int docID() {
        if (cursor < 0) {
            return -1;
        }
        if (cursor >= size) {
            return NO_MORE_DOCS;
        }
        return docIds[cursor];
    }

    @Override
    public int nextDoc() {
        cursor++;
        return docID();
    }

    @Override
    public int advance(int target) {
        int lo = Math.max(0, cursor + 1);
        int hi = size;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (docIds[mid] < target) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        cursor = lo;
        return docID();
    }

    @Override
    public long cost() {
        return size;
    }

    private static boolean isSortedAscending(int[] arr, int size) {
        for (int i = 1; i < size; i++) {
            if (arr[i - 1] > arr[i]) {
                return false;
            }
        }
        return true;
    }
}
