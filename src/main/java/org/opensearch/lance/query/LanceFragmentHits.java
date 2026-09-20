/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.util.Arrays;

/**
 * Hits of one shard-wide Lance scan that fall into one fragment: the
 * row offsets Lance returned, in the order it returned them, with the
 * Lucene score of each row.
 *
 * <p>{@link LanceFtsQuery} and {@link LanceKnnQuery} fill one instance
 * per fragment while they read the scan, then serve every per-leaf
 * {@link org.apache.lucene.search.ScorerSupplier} of the Weight from
 * it. Lucene's {@link org.apache.lucene.search.DocIdSetIterator}
 * contract wants ascending doc ids, so {@link #sortedDocIds()} and
 * {@link #sortedScores()} expose the hits sorted by offset. The sorted
 * arrays are built once and shared: the executor hints the fragment
 * reader with {@link #sortedDocIds()} before it builds aggregators and
 * sort comparators, and the scorer the same Weight later produces for
 * the leaf hands the reader the identical array, so the reader
 * recognises the hint it already holds by reference and keeps the
 * sparse structures it took for it. The arrays are never modified
 * after they are built.
 *
 * <p>Sizes are typically at most the scan limit (or {@code k}) for the
 * fragments that carry hits, and many fragments carry none, so the
 * buffers grow geometrically from a small initial capacity.
 */
final class LanceFragmentHits {

    /** Shared instance for a fragment the scan returned no rows for. */
    static final LanceFragmentHits EMPTY = new LanceFragmentHits();

    private int[] offsets = new int[8];
    private float[] scores = new float[offsets.length];
    private int size = 0;
    private int[] sortedDocIds;
    private float[] sortedScores;

    void add(int offset, float score) {
        if (size == offsets.length) {
            offsets = Arrays.copyOf(offsets, offsets.length * 2);
            scores = Arrays.copyOf(scores, scores.length * 2);
        }
        offsets[size] = offset;
        scores[size] = score;
        size++;
    }

    /** Number of hits in this fragment. */
    int size() {
        return size;
    }

    /** Hit offsets sorted ascending; the same array on every call. */
    synchronized int[] sortedDocIds() {
        ensureSorted();
        return sortedDocIds;
    }

    /** Scores parallel to {@link #sortedDocIds()}; the same array on every call. */
    synchronized float[] sortedScores() {
        ensureSorted();
        return sortedScores;
    }

    private void ensureSorted() {
        if (sortedDocIds != null) {
            return;
        }
        // Pack (offset, score) into longs so one sort orders both
        // arrays. Offsets are non-negative ints, so signed long
        // ordering is offset-ascending.
        long[] packed = new long[size];
        for (int i = 0; i < size; i++) {
            packed[i] = ((long) offsets[i] << 32) | (Float.floatToIntBits(scores[i]) & 0xFFFFFFFFL);
        }
        Arrays.sort(packed);
        int[] docIds = new int[size];
        float[] hitScores = new float[size];
        for (int i = 0; i < size; i++) {
            docIds[i] = (int) (packed[i] >>> 32);
            hitScores[i] = Float.intBitsToFloat((int) (packed[i] & 0xFFFFFFFFL));
        }
        sortedScores = hitScores;
        sortedDocIds = docIds;
    }
}
