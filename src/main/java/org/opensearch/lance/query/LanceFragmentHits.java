/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.util.Arrays;
import java.util.Objects;

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
 * buffers start empty and grow geometrically from a small capacity.
 * Every array is reserved with the {@link LanceHitsAccounting} of the
 * request before it is allocated: the (offset, score) buffers at each
 * growth step, the sorted view when it is first asked for. An
 * unbounded full text scan over a large table holds one entry per
 * matching row, so the reservation is what lets the request breaker
 * refuse a hit set the heap cannot hold instead of the JVM failing on
 * it. {@link #heapBytes()} is what the owner gives back when it drops
 * the instance.
 */
final class LanceFragmentHits {

    /** Capacity the buffers take on the first hit; a typical top k page never grows past it. */
    static final int INITIAL_CAPACITY = 8;

    /** Bytes one hit takes in the (offset, score) buffers, and again in the sorted view. */
    static final int BYTES_PER_HIT = Integer.BYTES + Float.BYTES;

    private static final int[] NO_INTS = new int[0];
    private static final float[] NO_FLOATS = new float[0];

    /** Shared instance for a fragment the scan returned no rows for. */
    static final LanceFragmentHits EMPTY = new LanceFragmentHits();

    private final LanceHitsAccounting accounting;
    private int[] offsets = NO_INTS;
    private float[] scores = NO_FLOATS;
    private int size = 0;
    private int[] sortedDocIds;
    private float[] sortedScores;

    /** Hits whose buffers are counted but never refused. */
    LanceFragmentHits() {
        this(LanceHitsAccounting.unlimited());
    }

    /** Hits whose buffers are reserved with {@code accounting} before they are allocated. */
    LanceFragmentHits(LanceHitsAccounting accounting) {
        this.accounting = Objects.requireNonNull(accounting, "accounting must not be null");
    }

    /**
     * Append one hit, growing the buffers when they are full. The
     * growth is reserved with the accounting first, so a refusal
     * leaves the buffers as they were.
     */
    void add(int offset, float score) {
        if (size == offsets.length) {
            int capacity = offsets.length == 0 ? INITIAL_CAPACITY : offsets.length * 2;
            accounting.reserve((long) (capacity - offsets.length) * BYTES_PER_HIT);
            offsets = Arrays.copyOf(offsets, capacity);
            scores = Arrays.copyOf(scores, capacity);
        }
        offsets[size] = offset;
        scores[size] = score;
        size++;
    }

    /** Number of hits in this fragment. */
    int size() {
        return size;
    }

    /**
     * Bytes of the arrays this instance holds and has reserved: the
     * (offset, score) buffers at their allocated capacity, plus the
     * sorted view once it has been built. The owner releases exactly
     * this amount when it drops the instance.
     */
    synchronized long heapBytes() {
        long bytes = (long) offsets.length * BYTES_PER_HIT;
        if (sortedDocIds != null) {
            bytes += (long) size * BYTES_PER_HIT;
        }
        return bytes;
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
        // ordering is offset-ascending. The packed array lives only
        // until the two sorted arrays are filled from it, so its bytes
        // are reserved for the build and given back at the end.
        long packedBytes = (long) size * Long.BYTES;
        long sortedBytes = (long) size * BYTES_PER_HIT;
        accounting.reserve(packedBytes + sortedBytes);
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
        accounting.release(packedBytes);
    }
}
