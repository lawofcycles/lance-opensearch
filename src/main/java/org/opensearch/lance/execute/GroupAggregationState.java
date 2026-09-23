/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.execute;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefHash;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.BitMixer;
import org.opensearch.common.util.Comparators;
import org.opensearch.lance.execute.AggregateSpecResolver.KeyKind;
import org.opensearch.lance.execute.AggregateSpecResolver.Metric;
import org.opensearch.lance.execute.AggregateSpecResolver.MetricKind;
import org.opensearch.lance.execute.AggregateSpecResolver.TopKSpec;
import org.opensearch.search.aggregations.metrics.HyperLogLogPlusPlus;
import org.opensearch.search.aggregations.metrics.TDigestState;

import static org.opensearch.lance.execute.ArrowRowValues.asLong;
import static org.opensearch.lance.execute.ArrowRowValues.doubleOr;
import static org.opensearch.lance.execute.ArrowRowValues.doubleOrZero;
import static org.opensearch.lance.execute.ArrowRowValues.longOrZero;
import static org.opensearch.lance.execute.ArrowRowValues.readUtf8;

/**
 * The accumulation state of a pushed aggregation between the Lance
 * scans and the bucket assembly: what one scan returns
 * ({@link Partial}), the columnar group table the main scan fills and
 * the partials of several scans merge into ({@link GroupTable} with
 * its {@link MetricStore} columns, fed per batch through
 * {@link MetricBatch}), the bounded per scan selection of the single
 * level {@code terms} shape ({@link TopKGroups}), and the boxed forms
 * the assembly reads once every partial is merged ({@link Group},
 * {@link Candidate}, {@link GroupState}, {@link MetricState}). The
 * merge rules live here: counts, sums and value counts add, min and
 * max take the extreme, sketches merge. Nothing here opens a scan,
 * reads the request, or builds an {@code InternalAggregation}; the
 * spec records it indexes by ({@link Metric}, {@link KeyKind},
 * {@link TopKSpec}) come from the resolver.
 */
final class GroupAggregationState {

    private GroupAggregationState() {}

    /**
     * Running values of one metric over the rows of one group. A fresh
     * state holds the neutral element of every measure (0 for sums and
     * counts, the infinities for min and max, no sketch), which is also
     * what the aggregator reports for a bucket without documents, so
     * merging a state into a fresh one copies it and merging two partial
     * states adds the sums and counts and keeps the smaller min and
     * larger max. The sketches are lazy: a row state carries one hash
     * (cardinality) or one bin (percentiles), the first merge into a
     * group state opens the sketch and inserts it, and merging two group
     * states merges the sketches. A sketch is copied before it is merged
     * into a fresh state, so the states of the scan's groups can be
     * folded into an outer bucket and still feed the nested level.
     */
    static final class MetricState {
        double sum;
        long count;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        double sumOfSquares;

        /** Cardinality: the hash of one row's value, and the sketch of a group. */
        long hash;
        boolean hashed;
        int precision;
        HyperLogLogPlusPlus sketch;

        /** Percentiles: the values and weights of one bin, and the digest of a group. */
        double[] binValues;
        long[] binWeights;
        double compression;
        TDigestState digest;

        void merge(MetricState other) {
            sum += other.sum;
            count += other.count;
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);
            sumOfSquares += other.sumOfSquares;
            // This state may still be the single row it was read as;
            // its own value goes into the sketch before the other's.
            materialize();
            if (other.sketch != null) {
                if (sketch == null) {
                    sketch = new HyperLogLogPlusPlus(other.sketch.precision(), BigArrays.NON_RECYCLING_INSTANCE, 1);
                }
                sketch.merge(0, other.sketch, 0);
            } else if (other.hashed) {
                collect(other.precision, other.hash);
            }
            if (other.digest != null) {
                if (digest == null) {
                    digest = new TDigestState(other.digest.compression());
                }
                digest.add(other.digest);
            } else if (other.binValues != null) {
                addBin(other.compression, other.binValues, other.binWeights);
            }
        }

        /**
         * Moves a row's own hash or bin into this state's sketch. A state
         * read from one row keeps the value itself until it is merged
         * with another or reported, so the scan does not allocate a
         * sketch per row.
         */
        void materialize() {
            if (hashed) {
                hashed = false;
                collect(precision, hash);
            }
            if (binValues != null) {
                double[] values = binValues;
                long[] weights = binWeights;
                binValues = null;
                binWeights = null;
                addBin(compression, values, weights);
            }
        }

        private void collect(int precision, long hash) {
            if (sketch == null) {
                sketch = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
            }
            sketch.collect(0, hash);
        }

        private void addBin(double compression, double[] values, long[] weights) {
            if (digest == null) {
                digest = new TDigestState(compression);
            }
            for (int i = 0; i < values.length; i++) {
                // TDigest weights are ints; a bin of more rows is added in
                // slices.
                long remaining = weights[i];
                while (remaining > 0L) {
                    int slice = (int) Math.min(remaining, Integer.MAX_VALUE);
                    digest.add(values[i], slice);
                    remaining -= slice;
                }
            }
        }
    }

    /**
     * Running values of one group: its row count and one state per
     * metric of the plan, by slot. The same merge folds the partials of
     * several scans into one group and the rows of one outer key into
     * the outer bucket.
     */
    static final class GroupState {
        long count;
        final MetricState[] metrics;

        GroupState(long count, MetricState[] metrics) {
            this.count = count;
            this.metrics = metrics;
        }

        /** The state of a group no row contributed to: count 0 and every metric at its neutral element. */
        static GroupState empty(int metricCount) {
            MetricState[] metrics = new MetricState[metricCount];
            for (int i = 0; i < metricCount; i++) {
                metrics[i] = new MetricState();
            }
            return new GroupState(0L, metrics);
        }

        GroupState merge(GroupState other) {
            count += other.count;
            for (int i = 0; i < metrics.length; i++) {
                metrics[i].merge(other.metrics[i]);
            }
            return this;
        }
    }

    /**
     * What one scan returned, and the merge of several. {@code total}
     * is the row count over every group, null keyed rows included. A
     * plan without groupings keeps its single row in
     * {@code metricsOnly} ({@code null} until a row arrived). A plan
     * with groupings keeps one state per key list in the primitive
     * columns of {@code groups}; the single level {@code terms} shape
     * ordered by {@code _count} or by one metric keeps the bounded per
     * scan selection in {@code topK} instead, with the row count over
     * every keyed group in {@code keyedCount}
     * ({@link LanceAggregateResults#mergePartials} folds the top-k partials into a
     * {@code groups} table before the buckets are built).
     */
    static final class Partial {
        long total;
        long keyedCount;
        GroupState metricsOnly;
        GroupTable groups;
        TopKGroups topK;

        void merge(Partial other) {
            total += other.total;
            keyedCount += other.keyedCount;
            if (other.metricsOnly != null) {
                metricsOnly = metricsOnly == null ? other.metricsOnly : metricsOnly.merge(other.metricsOnly);
            }
            if (other.groups != null) {
                if (groups == null) {
                    groups = other.groups;
                } else {
                    groups.mergeFrom(other.groups);
                }
            }
        }
    }

    /**
     * Columnar running values of every metric of the plan over the
     * groups of a {@link GroupTable} or {@link TopKGroups}: one
     * primitive array per measure per metric slot, indexed by group, so
     * a scan reading millions of group rows writes into arrays instead
     * of allocating a {@link MetricState} per row and metric. The
     * sketches (the HyperLogLog++ of a {@code cardinality}, the TDigest
     * a percentiles bin scan feeds) stay one object per group, created
     * on the group's first value; their group count is bounded by the
     * plan's group estimate, not by the rows read. {@link #box} turns
     * one group's values back into the {@link MetricState} the bucket
     * assembly folds and reports.
     */
    static final class MetricStore {
        private final List<Metric> metrics;
        private final double[][] sums;
        private final long[][] counts;
        private final double[][] mins;
        private final double[][] maxes;
        private final double[][] squares;
        private final HyperLogLogPlusPlus[][] sketches;
        private final TDigestState[][] digests;
        private int capacity;

        MetricStore(List<Metric> metrics, int initialCapacity) {
            this.metrics = metrics;
            this.capacity = initialCapacity;
            int slots = metrics.size();
            sums = new double[slots][];
            counts = new long[slots][];
            mins = new double[slots][];
            maxes = new double[slots][];
            squares = new double[slots][];
            sketches = new HyperLogLogPlusPlus[slots][];
            digests = new TDigestState[slots][];
            for (Metric metric : metrics) {
                int slot = metric.slot();
                switch (metric.kind()) {
                    case SUM -> sums[slot] = new double[capacity];
                    case MIN -> mins[slot] = filled(new double[capacity], 0, Double.POSITIVE_INFINITY);
                    case MAX -> maxes[slot] = filled(new double[capacity], 0, Double.NEGATIVE_INFINITY);
                    case VALUE_COUNT -> counts[slot] = new long[capacity];
                    case AVG -> {
                        sums[slot] = new double[capacity];
                        counts[slot] = new long[capacity];
                    }
                    case STATS, EXTENDED_STATS -> {
                        counts[slot] = new long[capacity];
                        sums[slot] = new double[capacity];
                        mins[slot] = filled(new double[capacity], 0, Double.POSITIVE_INFINITY);
                        maxes[slot] = filled(new double[capacity], 0, Double.NEGATIVE_INFINITY);
                        if (metric.kind() == MetricKind.EXTENDED_STATS) {
                            squares[slot] = new double[capacity];
                        }
                    }
                    case CARDINALITY -> sketches[slot] = new HyperLogLogPlusPlus[capacity];
                    case PERCENTILES, PERCENTILE_RANKS -> {
                        mins[slot] = filled(new double[capacity], 0, Double.POSITIVE_INFINITY);
                        maxes[slot] = filled(new double[capacity], 0, Double.NEGATIVE_INFINITY);
                        digests[slot] = new TDigestState[capacity];
                    }
                }
            }
        }

        private static double[] filled(double[] array, int from, double value) {
            Arrays.fill(array, from, array.length, value);
            return array;
        }

        void ensureCapacity(int needed) {
            if (needed <= capacity) {
                return;
            }
            int grown = Math.max(needed, capacity * 2);
            for (int slot = 0; slot < metrics.size(); slot++) {
                if (sums[slot] != null) {
                    sums[slot] = Arrays.copyOf(sums[slot], grown);
                }
                if (counts[slot] != null) {
                    counts[slot] = Arrays.copyOf(counts[slot], grown);
                }
                if (mins[slot] != null) {
                    mins[slot] = filled(Arrays.copyOf(mins[slot], grown), capacity, Double.POSITIVE_INFINITY);
                }
                if (maxes[slot] != null) {
                    maxes[slot] = filled(Arrays.copyOf(maxes[slot], grown), capacity, Double.NEGATIVE_INFINITY);
                }
                if (squares[slot] != null) {
                    squares[slot] = Arrays.copyOf(squares[slot], grown);
                }
                if (sketches[slot] != null) {
                    sketches[slot] = Arrays.copyOf(sketches[slot], grown);
                }
                if (digests[slot] != null) {
                    digests[slot] = Arrays.copyOf(digests[slot], grown);
                }
            }
            capacity = grown;
        }

        /** Back to the neutral element at {@code idx}, for a reused top-k slot. */
        void reset(int idx) {
            for (int slot = 0; slot < metrics.size(); slot++) {
                if (sums[slot] != null) {
                    sums[slot][idx] = 0d;
                }
                if (counts[slot] != null) {
                    counts[slot][idx] = 0L;
                }
                if (mins[slot] != null) {
                    mins[slot][idx] = Double.POSITIVE_INFINITY;
                }
                if (maxes[slot] != null) {
                    maxes[slot][idx] = Double.NEGATIVE_INFINITY;
                }
                if (squares[slot] != null) {
                    squares[slot][idx] = 0d;
                }
                if (sketches[slot] != null) {
                    sketches[slot][idx] = null;
                }
                if (digests[slot] != null) {
                    digests[slot][idx] = null;
                }
            }
        }

        void addSum(int slot, int idx, double value) {
            sums[slot][idx] += value;
        }

        void addCount(int slot, int idx, long value) {
            counts[slot][idx] += value;
        }

        void addMin(int slot, int idx, double value) {
            if (value < mins[slot][idx]) {
                mins[slot][idx] = value;
            }
        }

        void addMax(int slot, int idx, double value) {
            if (value > maxes[slot][idx]) {
                maxes[slot][idx] = value;
            }
        }

        void addSquare(int slot, int idx, double value) {
            squares[slot][idx] += value;
        }

        void collectHash(int slot, int idx, long hash, int precision) {
            HyperLogLogPlusPlus sketch = sketches[slot][idx];
            if (sketch == null) {
                sketch = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
                sketches[slot][idx] = sketch;
            }
            sketch.collect(0, hash);
        }

        /**
         * One bin scan row into the group's digest: the bin's rows as
         * one value at each bin edge and the rest at the centre, the
         * spread {@link Metric#readBin} documents.
         */
        void addBin(int slot, int idx, double compression, double min, double max, double width, int bins, long ordinal, long count) {
            TDigestState digest = digests[slot][idx];
            if (digest == null) {
                digest = new TDigestState(compression);
                digests[slot][idx] = digest;
            }
            long bin = Math.max(0L, Math.min(ordinal, bins - 1L));
            double lower = bin == 0L ? min : min + bin * width;
            double upper = bin == bins - 1L || max == min ? max : Math.min(max, min + (bin + 1L) * width);
            double centre = (lower + upper) / 2d;
            if (count == 1L) {
                digest.add(centre, 1);
                return;
            }
            digest.add(lower, 1);
            long remaining = count - 2L;
            while (remaining > 0L) {
                int slice = (int) Math.min(remaining, Integer.MAX_VALUE);
                digest.add(centre, slice);
                remaining -= slice;
            }
            digest.add(upper, 1);
        }

        void merge(int destIdx, MetricStore src, int srcIdx) {
            for (Metric metric : metrics) {
                int slot = metric.slot();
                if (src.sums[slot] != null) {
                    sums[slot][destIdx] += src.sums[slot][srcIdx];
                }
                if (src.counts[slot] != null) {
                    counts[slot][destIdx] += src.counts[slot][srcIdx];
                }
                if (src.mins[slot] != null) {
                    addMin(slot, destIdx, src.mins[slot][srcIdx]);
                }
                if (src.maxes[slot] != null) {
                    addMax(slot, destIdx, src.maxes[slot][srcIdx]);
                }
                if (src.squares[slot] != null) {
                    squares[slot][destIdx] += src.squares[slot][srcIdx];
                }
                if (src.sketches[slot] != null && src.sketches[slot][srcIdx] != null) {
                    HyperLogLogPlusPlus other = src.sketches[slot][srcIdx];
                    HyperLogLogPlusPlus sketch = sketches[slot][destIdx];
                    if (sketch == null) {
                        sketch = new HyperLogLogPlusPlus(other.precision(), BigArrays.NON_RECYCLING_INSTANCE, 1);
                        sketches[slot][destIdx] = sketch;
                    }
                    sketch.merge(0, other, 0);
                }
                if (src.digests[slot] != null && src.digests[slot][srcIdx] != null) {
                    TDigestState other = src.digests[slot][srcIdx];
                    TDigestState digest = digests[slot][destIdx];
                    if (digest == null) {
                        digest = new TDigestState(other.compression());
                        digests[slot][destIdx] = digest;
                    }
                    digest.add(other);
                }
            }
        }

        /**
         * The value of a single value metric at {@code idx}, as the
         * order comparator reads it from the built aggregation: the
         * sum, the min / max (an infinity when no row had a value), the
         * value count, or the average ({@code NaN} over a count of 0,
         * which sorts last in either direction).
         */
        double sortValue(int slot, int idx) {
            return switch (metrics.get(slot).kind()) {
                case SUM -> sums[slot][idx];
                case MIN -> mins[slot][idx];
                case MAX -> maxes[slot][idx];
                case VALUE_COUNT -> (double) counts[slot][idx];
                case AVG -> sums[slot][idx] / counts[slot][idx];
                default -> throw new IllegalStateException("not a single value metric: " + metrics.get(slot).kind());
            };
        }

        /** The smallest group minimum of {@code slot} over the first {@code size} groups, for the percentiles bin bounds. */
        double minOf(int slot, int size) {
            double min = Double.POSITIVE_INFINITY;
            for (int idx = 0; idx < size; idx++) {
                min = Math.min(min, mins[slot][idx]);
            }
            return min;
        }

        double maxOf(int slot, int size) {
            double max = Double.NEGATIVE_INFINITY;
            for (int idx = 0; idx < size; idx++) {
                max = Math.max(max, maxes[slot][idx]);
            }
            return max;
        }

        /** One group's values as the states the bucket assembly reads, by slot. */
        MetricState[] boxAll(int idx) {
            MetricState[] states = new MetricState[metrics.size()];
            for (int slot = 0; slot < states.length; slot++) {
                states[slot] = box(slot, idx);
            }
            return states;
        }

        private MetricState box(int slot, int idx) {
            MetricState state = new MetricState();
            switch (metrics.get(slot).kind()) {
                case SUM -> state.sum = sums[slot][idx];
                case MIN -> state.min = mins[slot][idx];
                case MAX -> state.max = maxes[slot][idx];
                case VALUE_COUNT -> state.count = counts[slot][idx];
                case AVG -> {
                    state.sum = sums[slot][idx];
                    state.count = counts[slot][idx];
                }
                case STATS, EXTENDED_STATS -> {
                    state.count = counts[slot][idx];
                    state.sum = sums[slot][idx];
                    state.min = mins[slot][idx];
                    state.max = maxes[slot][idx];
                    if (squares[slot] != null) {
                        state.sumOfSquares = squares[slot][idx];
                    }
                }
                case CARDINALITY -> state.sketch = sketches[slot][idx];
                case PERCENTILES, PERCENTILE_RANKS -> {
                    state.min = mins[slot][idx];
                    state.max = maxes[slot][idx];
                    state.digest = digests[slot][idx];
                }
            }
            return state;
        }
    }

    /**
     * The output vectors of one metric in one batch of a main scan,
     * resolved once per batch so the row loop reads primitives only:
     * the columnar counterpart of {@link Metric#read}.
     */
    static final class MetricBatch {
        private final Metric metric;
        private final int slot;
        private final FieldVector value;
        private final FieldVector count;
        private final FieldVector min;
        private final FieldVector max;
        private final FieldVector square;
        private final FieldVector distinct;
        private final int precision;
        private final MurmurHash3.Hash128 hashScratch;
        private final BytesRefBuilder bytesScratch;

        MetricBatch(Metric metric, VectorSchemaRoot root) {
            this.metric = metric;
            this.slot = metric.slot();
            String prefix = metric.prefix();
            FieldVector value = null;
            FieldVector count = null;
            FieldVector min = null;
            FieldVector max = null;
            FieldVector square = null;
            FieldVector distinct = null;
            int precision = 0;
            switch (metric.kind()) {
                case SUM, MIN, MAX, VALUE_COUNT -> value = root.getVector(prefix);
                case AVG -> {
                    value = root.getVector(prefix + "_s");
                    count = root.getVector(prefix + "_c");
                }
                case STATS, EXTENDED_STATS -> {
                    count = root.getVector(prefix + "_c");
                    value = root.getVector(prefix + "_s");
                    min = root.getVector(prefix + "_mn");
                    max = root.getVector(prefix + "_mx");
                    if (metric.kind() == MetricKind.EXTENDED_STATS) {
                        square = root.getVector(prefix + "_q");
                    }
                }
                case CARDINALITY -> {
                    distinct = root.getVector(metric.distinctColumn());
                    precision = metric.precision();
                }
                case PERCENTILES, PERCENTILE_RANKS -> {
                    min = root.getVector(prefix + "_mn");
                    max = root.getVector(prefix + "_mx");
                }
            }
            this.value = value;
            this.count = count;
            this.min = min;
            this.max = max;
            this.square = square;
            this.distinct = distinct;
            this.precision = precision;
            this.hashScratch = distinct instanceof VarCharVector ? new MurmurHash3.Hash128() : null;
            this.bytesScratch = distinct instanceof VarCharVector ? new BytesRefBuilder() : null;
        }

        static MetricBatch[] resolve(List<Metric> metrics, VectorSchemaRoot root) {
            MetricBatch[] batches = new MetricBatch[metrics.size()];
            for (int i = 0; i < batches.length; i++) {
                batches[i] = new MetricBatch(metrics.get(i), root);
            }
            return batches;
        }

        /** The metric's values on one group row, into the group's columns. */
        void add(MetricStore store, int idx, int row) {
            switch (metric.kind()) {
                case SUM -> store.addSum(slot, idx, doubleOrZero(value, row));
                case MIN -> store.addMin(slot, idx, doubleOr(value, row, Double.POSITIVE_INFINITY));
                case MAX -> store.addMax(slot, idx, doubleOr(value, row, Double.NEGATIVE_INFINITY));
                case VALUE_COUNT -> store.addCount(slot, idx, longOrZero(value, row));
                case AVG -> {
                    store.addSum(slot, idx, doubleOrZero(value, row));
                    store.addCount(slot, idx, longOrZero(count, row));
                }
                case STATS, EXTENDED_STATS -> {
                    store.addCount(slot, idx, longOrZero(count, row));
                    store.addSum(slot, idx, doubleOrZero(value, row));
                    store.addMin(slot, idx, doubleOr(min, row, Double.POSITIVE_INFINITY));
                    store.addMax(slot, idx, doubleOr(max, row, Double.NEGATIVE_INFINITY));
                    if (square != null) {
                        store.addSquare(slot, idx, doubleOrZero(square, row));
                    }
                }
                case CARDINALITY -> {
                    if (!distinct.isNull(row)) {
                        store.collectHash(slot, idx, hash(row), precision);
                    }
                }
                case PERCENTILES, PERCENTILE_RANKS -> {
                    store.addMin(slot, idx, doubleOr(min, row, Double.POSITIVE_INFINITY));
                    store.addMax(slot, idx, doubleOr(max, row, Double.NEGATIVE_INFINITY));
                }
            }
        }

        /**
         * The hash the cardinality aggregator computes for the row's
         * distinct value, without the per row copies of
         * {@link Metric#hash}: the string bytes are read into a reused
         * scratch.
         */
        private long hash(int row) {
            if (distinct instanceof VarCharVector v) {
                BytesRef bytes = readUtf8(v, row, bytesScratch);
                return MurmurHash3.hash128(bytes.bytes, bytes.offset, bytes.length, 0, hashScratch).h1;
            }
            if (distinct instanceof Float4Vector v) {
                return BitMixer.mix64(Double.doubleToLongBits(v.get(row)));
            }
            if (distinct instanceof Float8Vector v) {
                return BitMixer.mix64(Double.doubleToLongBits(v.get(row)));
            }
            return BitMixer.mix64(asLong(distinct, row));
        }

        /** The row's value under {@link MetricStore#sortValue}, for the top-k test before the row is retained. */
        double rowSortValue(int row) {
            return switch (metric.kind()) {
                case SUM -> doubleOrZero(value, row);
                case MIN -> doubleOr(value, row, Double.POSITIVE_INFINITY);
                case MAX -> doubleOr(value, row, Double.NEGATIVE_INFINITY);
                case VALUE_COUNT -> (double) longOrZero(value, row);
                case AVG -> doubleOrZero(value, row) / longOrZero(count, row);
                default -> throw new IllegalStateException("not a single value metric: " + metric.kind());
            };
        }
    }

    /**
     * The groups of one scan (and the merge of several), keyed by the
     * full key list, in primitive columns: one {@code long} per key and
     * group (the value of an integer or date key, the
     * {@link Double#doubleToLongBits} of a floating point key, the id
     * of a string key in {@code dict}), a null bit set, the row counts,
     * and the metric columns of a {@link MetricStore}. Group identity
     * is an open addressing hash over the encoded keys, so merging a
     * row allocates nothing; the string dictionary is a
     * {@link BytesRefHash}, whose pooled bytes outlive the Arrow batch
     * the key was read from. {@link #box} converts the groups into the
     * {@link Group} list the bucket assembly folds, once, after every
     * partial is merged.
     */
    static final class GroupTable {

        /** Ordering of two groups by index, for {@link #selectTop}. */
        interface GroupOrder {
            int compare(int a, int b);
        }

        private static final int INITIAL_CAPACITY = 16;

        private final KeyKind[] kinds;
        private final long[][] keys;
        private long[] nullBits;
        private long[] counts;
        final MetricStore metrics;
        private BytesRefHash dict;
        private final BytesRef spare = new BytesRef();
        private final BytesRef otherSpare = new BytesRef();
        private int[] slots;
        private int slotMask;
        private int size;

        GroupTable(KeyKind[] kinds, List<Metric> metrics) {
            this.kinds = kinds;
            this.keys = new long[kinds.length][];
            for (int k = 0; k < kinds.length; k++) {
                keys[k] = new long[INITIAL_CAPACITY];
            }
            this.nullBits = new long[INITIAL_CAPACITY];
            this.counts = new long[INITIAL_CAPACITY];
            this.metrics = new MetricStore(metrics, INITIAL_CAPACITY);
            this.slots = new int[INITIAL_CAPACITY * 2];
            this.slotMask = slots.length - 1;
        }

        int size() {
            return size;
        }

        /** The dictionary id of {@code bytes}, adding it when new. */
        long intern(BytesRef bytes) {
            if (dict == null) {
                dict = new BytesRefHash();
            }
            int id = dict.add(bytes);
            return id < 0 ? -id - 1 : id;
        }

        /** The group's index, adding an empty group when the key list is new. */
        int findOrAdd(long[] encoded, long nulls) {
            int slot = (int) (hashOf(encoded, nulls) & slotMask);
            while (true) {
                int entry = slots[slot];
                if (entry == 0) {
                    return add(encoded, nulls, slot);
                }
                int idx = entry - 1;
                if (nullBits[idx] == nulls && keysEqual(idx, encoded)) {
                    return idx;
                }
                slot = (slot + 1) & slotMask;
            }
        }

        private int add(long[] encoded, long nulls, int slot) {
            if (size == counts.length) {
                grow();
                // The slot table was rebuilt; probe again for the
                // insertion point.
                slot = (int) (hashOf(encoded, nulls) & slotMask);
                while (slots[slot] != 0) {
                    slot = (slot + 1) & slotMask;
                }
            }
            int idx = size++;
            for (int k = 0; k < kinds.length; k++) {
                keys[k][idx] = encoded[k];
            }
            nullBits[idx] = nulls;
            slots[slot] = idx + 1;
            return idx;
        }

        private void grow() {
            int grown = counts.length * 2;
            for (int k = 0; k < kinds.length; k++) {
                keys[k] = Arrays.copyOf(keys[k], grown);
            }
            nullBits = Arrays.copyOf(nullBits, grown);
            counts = Arrays.copyOf(counts, grown);
            metrics.ensureCapacity(grown);
            slots = new int[grown * 2];
            slotMask = slots.length - 1;
            for (int idx = 0; idx < size; idx++) {
                int slot = (int) (hashAt(idx) & slotMask);
                while (slots[slot] != 0) {
                    slot = (slot + 1) & slotMask;
                }
                slots[slot] = idx + 1;
            }
        }

        private long hashOf(long[] encoded, long nulls) {
            long hash = BitMixer.mix64(nulls);
            for (long value : encoded) {
                hash = BitMixer.mix64(hash ^ value);
            }
            return hash;
        }

        private long hashAt(int idx) {
            long hash = BitMixer.mix64(nullBits[idx]);
            for (int k = 0; k < kinds.length; k++) {
                hash = BitMixer.mix64(hash ^ keys[k][idx]);
            }
            return hash;
        }

        private boolean keysEqual(int idx, long[] encoded) {
            for (int k = 0; k < kinds.length; k++) {
                if (keys[k][idx] != encoded[k]) {
                    return false;
                }
            }
            return true;
        }

        void addCount(int idx, long count) {
            counts[idx] += count;
        }

        long countAt(int idx) {
            return counts[idx];
        }

        /** Sum of the counts of every group, for {@code sum_other_doc_count} under a key order. */
        long totalCount() {
            long total = 0L;
            for (int idx = 0; idx < size; idx++) {
                total += counts[idx];
            }
            return total;
        }

        void mergeFrom(GroupTable src) {
            long[] encoded = new long[kinds.length];
            for (int g = 0; g < src.size; g++) {
                for (int k = 0; k < kinds.length; k++) {
                    if (kinds[k] == KeyKind.STRING && (src.nullBits[g] & (1L << k)) == 0L) {
                        src.dict.get((int) src.keys[k][g], src.spare);
                        encoded[k] = intern(src.spare);
                    } else {
                        encoded[k] = src.keys[k][g];
                    }
                }
                int idx = findOrAdd(encoded, src.nullBits[g]);
                counts[idx] += src.counts[g];
                metrics.merge(idx, src.metrics, g);
            }
        }

        /** Key order of the first key column, ascending, the terms tie break. */
        int compareKeys(int a, int b) {
            return switch (kinds[0]) {
                case LONG -> Long.compare(keys[0][a], keys[0][b]);
                case DOUBLE -> Double.compare(Double.longBitsToDouble(keys[0][a]), Double.longBitsToDouble(keys[0][b]));
                case STRING -> {
                    dict.get((int) keys[0][a], spare);
                    dict.get((int) keys[0][b], otherSpare);
                    yield spare.compareTo(otherSpare);
                }
            };
        }

        /**
         * The indices of the first {@code n} groups by {@code order},
         * best first: a bounded heap over every group, the primitive
         * counterpart of the terms selection queue.
         */
        int[] selectTop(int n, GroupOrder order) {
            int keep = Math.min(n, size);
            int[] heap = new int[keep];
            int heapSize = 0;
            for (int idx = 0; idx < size; idx++) {
                if (heapSize < keep) {
                    heap[heapSize] = idx;
                    siftUp(heap, heapSize++, order);
                } else if (keep > 0 && order.compare(idx, heap[0]) < 0) {
                    heap[0] = idx;
                    siftDown(heap, heapSize, 0, order);
                }
            }
            int[] selected = new int[heapSize];
            for (int i = heapSize - 1; i >= 0; i--) {
                selected[i] = heap[0];
                heap[0] = heap[--heapSize];
                siftDown(heap, heapSize, 0, order);
            }
            return selected;
        }

        /** The heap's top is the worst retained group: a parent sorts after its children. */
        private static void siftUp(int[] heap, int pos, GroupOrder order) {
            while (pos > 0) {
                int parent = (pos - 1) / 2;
                if (order.compare(heap[pos], heap[parent]) <= 0) {
                    return;
                }
                int swap = heap[pos];
                heap[pos] = heap[parent];
                heap[parent] = swap;
                pos = parent;
            }
        }

        private static void siftDown(int[] heap, int heapSize, int pos, GroupOrder order) {
            while (true) {
                int worst = pos;
                int left = pos * 2 + 1;
                int right = left + 1;
                if (left < heapSize && order.compare(heap[left], heap[worst]) > 0) {
                    worst = left;
                }
                if (right < heapSize && order.compare(heap[right], heap[worst]) > 0) {
                    worst = right;
                }
                if (worst == pos) {
                    return;
                }
                int swap = heap[pos];
                heap[pos] = heap[worst];
                heap[worst] = swap;
                pos = worst;
            }
        }

        /** One group's key at position {@code k} as the object the bucket types expect. */
        Object boxKey(int k, int idx) {
            if ((nullBits[idx] & (1L << k)) != 0L) {
                return null;
            }
            return switch (kinds[k]) {
                case LONG -> keys[k][idx];
                case DOUBLE -> Double.longBitsToDouble(keys[k][idx]);
                case STRING -> {
                    dict.get((int) keys[k][idx], spare);
                    yield BytesRef.deepCopyOf(spare);
                }
            };
        }

        /** Every group as the boxed form the nested and composite assembly reads. */
        List<Group> box() {
            List<Group> groups = new ArrayList<>(size);
            for (int idx = 0; idx < size; idx++) {
                Object[] boxedKeys = new Object[kinds.length];
                for (int k = 0; k < kinds.length; k++) {
                    boxedKeys[k] = boxKey(k, idx);
                }
                groups.add(new Group(Arrays.asList(boxedKeys), new GroupState(counts[idx], metrics.boxAll(idx))));
            }
            return groups;
        }
    }

    /**
     * The bounded selection of one scan of the single level
     * {@code terms} shape ordered by {@code _count} or by one single
     * value metric: at most {@code limit} groups ({@code shard_size}
     * times {@code lance.aggregation.pushdown_topk_slack}) are kept in
     * a primitive heap whose top is the weakest retained group under
     * the request order (count descending or the metric in its
     * direction, key ascending as the tie breaker); a row that does not
     * beat it only adds its count to {@code keyedCount}, which
     * {@code sum_other_doc_count} is later computed from, so the scan's
     * memory and allocations stop growing at {@code limit} groups
     * however many groups Lance returns.
     *
     * <p>Entries are not coalesced here: Lance runs the aggregate to
     * completion before emitting rows, so one scan returns each group
     * at most once. The same key retained by several fragment group
     * scans is summed when {@link LanceAggregateResults#mergePartials} folds the
     * per scan selections into one {@link GroupTable} and the final
     * {@code shard_size} cut re-evaluates the summed counts. A key a
     * scan dropped loses that scan's rows the way a term a shard did
     * not return loses that shard's: the doc count error the reduce
     * derives from the smallest returned bucket keeps its meaning, and
     * a slack large enough to retain every group makes the result
     * exact.
     */
    static final class TopKGroups {
        private final KeyKind keyKind;
        private final int sortSlot;
        private final boolean sortAsc;
        private final int limit;
        final MetricStore metrics;
        private long[] keys;
        private long[] counts;
        private double[] sortValues;
        private BytesRefHash dict;
        private final BytesRef spare = new BytesRef();
        private final BytesRef otherSpare = new BytesRef();
        private int[] heap;
        private int size;
        private long keyedCount;

        TopKGroups(TopKSpec spec, List<Metric> allMetrics) {
            this.keyKind = spec.keyKind();
            this.sortSlot = spec.sortSlot();
            this.sortAsc = spec.sortAsc();
            this.limit = spec.perScanLimit();
            int initial = Math.min(limit, 16);
            this.keys = new long[initial];
            this.counts = new long[initial];
            this.sortValues = sortSlot >= 0 ? new double[initial] : null;
            this.metrics = new MetricStore(allMetrics, initial);
            this.heap = new int[initial];
        }

        long keyedCount() {
            return keyedCount;
        }

        /**
         * One group row: retained when the selection has room or the
         * row beats the weakest retained group. Returns the entry index
         * the caller writes the row's metrics into, or -1 when the row
         * only counts toward {@code keyedCount}.
         */
        int offer(long numericKey, BytesRef stringKey, long count, double sortValue) {
            keyedCount += count;
            if (size < limit) {
                if (size == counts.length) {
                    int grown = (int) Math.min((long) counts.length * 2, limit);
                    keys = Arrays.copyOf(keys, grown);
                    counts = Arrays.copyOf(counts, grown);
                    if (sortValues != null) {
                        sortValues = Arrays.copyOf(sortValues, grown);
                    }
                    metrics.ensureCapacity(grown);
                    heap = Arrays.copyOf(heap, grown);
                }
                int idx = size;
                write(idx, numericKey, stringKey, count, sortValue);
                heap[size] = idx;
                siftUp(size++);
                return idx;
            }
            int worst = heap[0];
            if (compareCandidate(numericKey, stringKey, count, sortValue, worst) >= 0) {
                return -1;
            }
            write(worst, numericKey, stringKey, count, sortValue);
            metrics.reset(worst);
            siftDown(0);
            return worst;
        }

        private void write(int idx, long numericKey, BytesRef stringKey, long count, double sortValue) {
            if (keyKind == KeyKind.STRING) {
                if (dict == null) {
                    dict = new BytesRefHash();
                }
                int id = dict.add(stringKey);
                keys[idx] = id < 0 ? -id - 1 : id;
            } else {
                keys[idx] = numericKey;
            }
            counts[idx] = count;
            if (sortValues != null) {
                sortValues[idx] = sortValue;
            }
        }

        /** Negative when the candidate sorts before (better than) entry {@code idx}. */
        private int compareCandidate(long numericKey, BytesRef stringKey, long count, double sortValue, int idx) {
            int byValue = sortSlot < 0
                ? Long.compare(counts[idx], count)
                : Comparators.compareDiscardNaN(sortValue, sortValues[idx], sortAsc);
            if (byValue != 0) {
                return byValue;
            }
            if (keyKind == KeyKind.STRING) {
                dict.get((int) keys[idx], spare);
                return stringKey.compareTo(spare);
            }
            if (keyKind == KeyKind.DOUBLE) {
                return Double.compare(Double.longBitsToDouble(numericKey), Double.longBitsToDouble(keys[idx]));
            }
            return Long.compare(numericKey, keys[idx]);
        }

        /** Negative when entry {@code a} sorts before entry {@code b} in the selection order. */
        private int compare(int a, int b) {
            int byValue = sortSlot < 0
                ? Long.compare(counts[b], counts[a])
                : Comparators.compareDiscardNaN(sortValues[a], sortValues[b], sortAsc);
            if (byValue != 0) {
                return byValue;
            }
            if (keyKind == KeyKind.STRING) {
                dict.get((int) keys[a], spare);
                dict.get((int) keys[b], otherSpare);
                return spare.compareTo(otherSpare);
            }
            if (keyKind == KeyKind.DOUBLE) {
                return Double.compare(Double.longBitsToDouble(keys[a]), Double.longBitsToDouble(keys[b]));
            }
            return Long.compare(keys[a], keys[b]);
        }

        /** The heap's top is the weakest retained entry: a parent sorts after its children. */
        private void siftUp(int pos) {
            while (pos > 0) {
                int parent = (pos - 1) / 2;
                if (compare(heap[pos], heap[parent]) <= 0) {
                    return;
                }
                int swap = heap[pos];
                heap[pos] = heap[parent];
                heap[parent] = swap;
                pos = parent;
            }
        }

        private void siftDown(int pos) {
            while (true) {
                int worst = pos;
                int left = pos * 2 + 1;
                int right = left + 1;
                if (left < size && compare(heap[left], heap[worst]) > 0) {
                    worst = left;
                }
                if (right < size && compare(heap[right], heap[worst]) > 0) {
                    worst = right;
                }
                if (worst == pos) {
                    return;
                }
                int swap = heap[pos];
                heap[pos] = heap[worst];
                heap[worst] = swap;
                pos = worst;
            }
        }

        /** Folds the retained entries into {@code dest}, summing the counts and metrics of a key another scan also kept. */
        void mergeInto(GroupTable dest) {
            long[] encoded = new long[1];
            for (int e = 0; e < size; e++) {
                if (keyKind == KeyKind.STRING) {
                    dict.get((int) keys[e], spare);
                    encoded[0] = dest.intern(spare);
                } else {
                    encoded[0] = keys[e];
                }
                int idx = dest.findOrAdd(encoded, 0L);
                dest.addCount(idx, counts[e]);
                dest.metrics.merge(idx, metrics, e);
            }
        }
    }

    /**
     * One merged group: the key of every level or source (a null where
     * the rows have no value at that level) and its row count and
     * metric states.
     */
    record Group(List<Object> keys, GroupState state) {
        long count() {
            return state.count;
        }
    }

    /** The rows that share one key at one level, before the terms selection decides whether the bucket is built. */
    record Candidate(Object key, long count, List<Group> rows) {
    }
}
