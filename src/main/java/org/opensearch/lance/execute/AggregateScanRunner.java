/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.execute;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Executor;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.lance.Dataset;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.lance.engine.FragmentGroupScan;
import org.opensearch.lance.engine.LanceCancellation;
import org.opensearch.lance.execute.AggregateSpecResolver.KeyKind;
import org.opensearch.lance.execute.AggregateSpecResolver.Level;
import org.opensearch.lance.execute.AggregateSpecResolver.Metric;
import org.opensearch.lance.execute.GroupAggregationState.GroupState;
import org.opensearch.lance.execute.GroupAggregationState.GroupTable;
import org.opensearch.lance.execute.GroupAggregationState.MetricBatch;
import org.opensearch.lance.execute.GroupAggregationState.MetricState;
import org.opensearch.lance.execute.GroupAggregationState.Partial;
import org.opensearch.lance.execute.GroupAggregationState.TopKGroups;
import org.opensearch.lance.plan.substrait.LanceSubstraitProducer;
import org.opensearch.lance.query.ScanAdmission;

import static org.opensearch.lance.execute.ArrowRowValues.asLong;
import static org.opensearch.lance.execute.ArrowRowValues.doubleKeyOf;
import static org.opensearch.lance.execute.ArrowRowValues.longOrZero;
import static org.opensearch.lance.execute.ArrowRowValues.readUtf8;

/**
 * Runs the Lance scans of one resolved aggregate and folds their rows
 * into the {@link GroupAggregationState}: the main scan over the
 * node's fragments in up to {@code pushdown_parallelism} contiguous
 * groups ({@link FragmentGroupScan}), the per metric bin scans a
 * tdigest percentiles needs once the main scan has returned its
 * bounds, the merge of the per group partials, and the cancellation
 * checks at every batch boundary. This is the only class that calls
 * {@code Dataset.newScan}; the Substrait bytes it hands Lance come
 * from the resolved plan and, for a bin scan, from
 * {@link LanceSubstraitProducer#toLancePercentilesBins}. It does not
 * read the request and builds no {@code InternalAggregation}; what it
 * returns is the merged {@link Partial} and the number of scans it
 * took, which the result assembler turns into buckets.
 */
final class AggregateScanRunner {

    private static final String COUNT_COLUMN = "n";
    /** Output columns of the bucket keys, one per level or composite source: {@code k0}, {@code k1}, ... */
    private static final String KEY_COLUMN_PREFIX = "k";

    private final ResolvedAggregate resolved;

    AggregateScanRunner(ResolvedAggregate resolved) {
        this.resolved = resolved;
    }

    /**
     * The plan of one Lance scan: the main scan carries every
     * grouping and measure of the request; a percentiles metric adds
     * a bin scan of its own afterwards, grouped by the same bucket
     * keys plus its bin, whose rows carry that metric's bin counts
     * only. {@code min}, {@code max} and {@code width} are the bin
     * bounds of a bin scan.
     */
    private record ScanPlan(ByteBuffer substrait, Metric percentiles, double min, double max, double width) {
        boolean isMain() {
            return percentiles == null;
        }
    }

    /** What the scans returned: the merged partial and the number of Lance scans that produced it (fragment groups times rounds). */
    record Scanned(Partial merged, int scans) {
    }

    /**
     * Runs every scan of the plan over {@code fragmentIds} (null means
     * every fragment) with {@code filterSql} (null means no filter).
     *
     * <p>The fragments are cut into {@code min(fragments, parallelism)}
     * contiguous groups and every group is scanned with its own copy
     * of the plan ({@link FragmentGroupScan}: the groups after the
     * first on {@code executor}, the first on the calling thread); the
     * partial results are merged per key list in Java before the
     * buckets are built. Lance runs
     * the aggregate of one scan in a single DataFusion partition, so
     * this is what gives a node with many fragments more than one
     * core for the hash aggregation. One fragment, or a parallelism
     * of 1, means one scan over {@code fragmentIds} as given.
     *
     * <p>A tdigest {@code percentiles} / {@code percentile_ranks}
     * takes two rounds: the first scan returns the field's minimum
     * and maximum next to the other measures, then one more round of
     * scans (same fragment groups, same filter) groups the rows by
     * {@code floor((value - min) / width)} with
     * {@code width = (max - min) / bins} and counts them, and the
     * executor feeds each bin's centre and count to the TDigest.
     * Two rounds because the bins cannot be laid out before the
     * bounds are known. A single round with a fixed number of
     * quantiles per node (DataFusion's {@code approx_percentile_cont}
     * or {@code NTILE}) was not taken: the coordinator merges one
     * sketch per node, and per node quantiles cannot be merged into
     * an answer over every node. The bounds are node wide over the
     * rows the filter keeps, so every bucket's histogram shares one
     * bin width and a reported percentile is within that width of
     * the exact value; {@code min == max} makes one bin. A field
     * without a value skips the second round and reports an empty
     * digest, as the aggregator does. Each percentiles metric runs
     * its own second round: grouping several metrics' bins in one
     * scan would multiply the rows by the bins of each.
     *
     * <p>Failures inside Lance (a plan it cannot parse, a function
     * its DataFusion build lacks) propagate: falling back to the
     * aggregator path would hide the regression behind a slow answer.
     *
     * <p>{@code cancellation} is checked before every group scan
     * starts and at every batch boundary inside a scan; a cancelled
     * task ends the pushdown with {@code TaskCancelledException} the
     * same way a failing group does.
     */
    Scanned run(
        Dataset dataset,
        List<Integer> fragmentIds,
        String filterSql,
        int parallelism,
        Executor executor,
        LanceCancellation cancellation
    ) throws Exception {
        return run(dataset, fragmentIds, filterSql, parallelism, executor, cancellation, Long.MAX_VALUE);
    }

    /**
     * As {@link #run(Dataset, List, String, int, Executor, LanceCancellation)},
     * with {@code heapRoomBytes} the room left in the request breaker
     * the admission gate compares the group state's heap with.
     */
    Scanned run(
        Dataset dataset,
        List<Integer> fragmentIds,
        String filterSql,
        int parallelism,
        Executor executor,
        LanceCancellation cancellation,
        long heapRoomBytes
    ) throws Exception {
        List<List<Integer>> fragmentGroups = FragmentGroupScan.splitContiguous(fragmentIds, parallelism);
        // The parallel scans hold their read queues and decoded batches
        // in native memory outside every breaker; the gate refuses the
        // aggregate with 429 before the first scan when the node cannot
        // hold the estimate. The rows are those of the fragments the
        // scans cover, from the planner's table statistics (0, and an
        // estimate of the batches alone, when they are unavailable).
        ScanAdmission.admitAggregateScan(
            dataset.uri(),
            dataset,
            filterSql,
            fragmentGroups.size(),
            ScanAdmission.fragmentRows(dataset, fragmentIds),
            resolved.projectedRowBytes(),
            resolved.estimatedGroups(),
            resolved.allMetrics().size(),
            heapRoomBytes
        );
        FragmentGroupScan scans = new FragmentGroupScan(executor, parallelism, cancellation);
        ScanPlan main = new ScanPlan(resolved.substrait(), null, 0d, 0d, 0d);
        // The gate credits memory earlier scans left behind only while
        // no gated scan runs.
        ScanAdmission.scanStarted();
        try {
            Partial merged = mergePartials(scans.runGroups(fragmentGroups, group -> scan(dataset, group, filterSql, main, cancellation)));
            int scanCount = fragmentGroups.size();
            for (Metric metric : resolved.allMetrics()) {
                if (!metric.isPercentiles()) {
                    continue;
                }
                ScanPlan bins = binScan(metric, merged);
                if (bins == null) {
                    continue;
                }
                merged.merge(mergePartials(scans.runGroups(fragmentGroups, group -> scan(dataset, group, filterSql, bins, cancellation))));
                scanCount += fragmentGroups.size();
            }
            return new Scanned(merged, scanCount);
        } finally {
            ScanAdmission.scanFinished();
        }
    }

    /**
     * Merges the per fragment group partials. The top-k partials of
     * the single level terms shape are folded into one
     * {@link GroupTable}, so a key several scans retained has its
     * counts and metrics summed before the final {@code shard_size}
     * selection re-evaluates it.
     */
    private Partial mergePartials(List<Partial> partials) {
        if (resolved.topK() != null && partials.get(0).topK != null) {
            Partial merged = new Partial();
            GroupTable table = new GroupTable(resolved.keyKinds(), resolved.allMetrics());
            merged.groups = table;
            for (Partial partial : partials) {
                merged.total += partial.total;
                merged.keyedCount += partial.topK.keyedCount();
                partial.topK.mergeInto(table);
            }
            return merged;
        }
        if (partials.size() == 1) {
            return partials.get(0);
        }
        Partial merged = new Partial();
        for (Partial partial : partials) {
            merged.merge(partial);
        }
        return merged;
    }

    /**
     * The bin scan of one percentiles metric from the bounds the main
     * scan returned, node wide over every group; null when no row had
     * a value.
     */
    private ScanPlan binScan(Metric metric, Partial merged) {
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        if (merged.metricsOnly != null) {
            min = Math.min(min, merged.metricsOnly.metrics[metric.slot()].min);
            max = Math.max(max, merged.metricsOnly.metrics[metric.slot()].max);
        }
        if (merged.groups != null) {
            min = Math.min(min, merged.groups.metrics.minOf(metric.slot(), merged.groups.size()));
            max = Math.max(max, merged.groups.metrics.maxOf(metric.slot(), merged.groups.size()));
        }
        if (min == Double.POSITIVE_INFINITY) {
            return null;
        }
        // Every value falls into bin 0 when they are all equal; any
        // positive width does that, and keeps the centre at the value.
        // The producer computes the same width from the same bounds.
        double width = max > min ? (max - min) / resolved.percentilesBins() : 1d;
        final double minBound = min;
        final double maxBound = max;
        ByteBuffer bins = LanceSubstraitProducer.toLancePercentilesBins(
            resolved.substrait(),
            metric.slot(),
            min,
            max,
            resolved.percentilesBins()
        )
            .orElseThrow(
                () -> new IllegalStateException(
                    "the Substrait producer refused the percentiles bin scan of ["
                        + metric.name()
                        + "] over ["
                        + minBound
                        + ", "
                        + maxBound
                        + "]"
                )
            );
        return new ScanPlan(bins, metric, min, max, width);
    }

    /**
     * One scan of {@code plan} over {@code fragmentIds} (null: every
     * fragment). The rows are read column-wise: the key and measure
     * vectors are resolved once per batch, every key is encoded
     * into a {@code long} (the value itself, the bits of a double,
     * or a dictionary id for a string) and the counts and metric
     * values go into the primitive columns of the partial's group
     * table, so the row loop allocates no objects however many
     * groups Lance returns. The single level terms shape with a
     * top-k order keeps only the bounded selection instead. A main
     * scan row carries the row count and the metrics' partial
     * values; a bin scan row carries only its metric's bin, and
     * adds nothing to the totals.
     */
    private Partial scan(Dataset dataset, List<Integer> fragmentIds, String filterSql, ScanPlan plan, LanceCancellation cancellation)
        throws Exception {
        ScanOptions.Builder options = new ScanOptions.Builder().substraitAggregate(plan.substrait().duplicate());
        if (fragmentIds != null) {
            options.fragmentIds(fragmentIds);
        }
        if (filterSql != null) {
            options.filter(filterSql);
        }
        Partial partial = new Partial();
        try (LanceScanner scanner = dataset.newScan(options.build()); ArrowReader reader = scanner.scanBatches()) {
            if (resolved.keyCount() == 0) {
                scanMetricsOnly(reader, plan, partial, cancellation);
            } else if (resolved.topK() != null) {
                scanTopK(reader, partial, cancellation);
            } else {
                scanGrouped(reader, plan, partial, cancellation);
            }
        }
        return partial;
    }

    /** A plan without groupings: Lance returns one row per scan, read into a {@link GroupState} as before. */
    private void scanMetricsOnly(ArrowReader reader, ScanPlan plan, Partial partial, LanceCancellation cancellation) throws Exception {
        while (reader.loadNextBatch()) {
            cancellation.checkCancelled();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            FieldVector counts = plan.isMain() ? root.getVector(COUNT_COLUMN) : null;
            for (int row = 0; row < root.getRowCount(); row++) {
                MetricState[] states = new MetricState[resolved.allMetrics().size()];
                for (int i = 0; i < states.length; i++) {
                    Metric metric = resolved.allMetrics().get(i);
                    if (plan.isMain()) {
                        states[i] = metric.read(root, row);
                    } else if (metric == plan.percentiles()) {
                        states[i] = metric.readBin(root, row, plan.min(), plan.max(), plan.width(), resolved.percentilesBins());
                    } else {
                        states[i] = new MetricState();
                    }
                }
                GroupState state = new GroupState(counts != null ? longOrZero(counts, row) : 0L, states);
                partial.total += state.count;
                partial.metricsOnly = partial.metricsOnly == null ? state : partial.metricsOnly.merge(state);
            }
        }
    }

    /** The single level terms shape: every row is offered to the bounded top-k selection. */
    private void scanTopK(ArrowReader reader, Partial partial, LanceCancellation cancellation) throws Exception {
        TopKGroups groups = new TopKGroups(resolved.topK(), resolved.allMetrics());
        partial.topK = groups;
        BytesRefBuilder scratch = new BytesRefBuilder();
        while (reader.loadNextBatch()) {
            cancellation.checkCancelled();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            FieldVector counts = root.getVector(COUNT_COLUMN);
            FieldVector keyVector = root.getVector(KEY_COLUMN_PREFIX + 0);
            MetricBatch[] batches = MetricBatch.resolve(resolved.allMetrics(), root);
            MetricBatch orderBatch = resolved.topK().sortSlot() >= 0 ? batches[resolved.topK().sortSlot()] : null;
            for (int row = 0; row < root.getRowCount(); row++) {
                long count = longOrZero(counts, row);
                partial.total += count;
                if (keyVector.isNull(row)) {
                    // Documents without a value open no bucket.
                    continue;
                }
                long numericKey = 0L;
                BytesRef stringKey = null;
                switch (resolved.topK().keyKind()) {
                    case STRING -> stringKey = readUtf8((VarCharVector) keyVector, row, scratch);
                    case DOUBLE -> numericKey = Double.doubleToLongBits(doubleKeyOf(keyVector, row));
                    case LONG -> numericKey = asLong(keyVector, row);
                }
                double sortValue = orderBatch != null ? orderBatch.rowSortValue(row) : 0d;
                int idx = groups.offer(numericKey, stringKey, count, sortValue);
                if (idx >= 0) {
                    for (MetricBatch batch : batches) {
                        batch.add(groups.metrics, idx, row);
                    }
                }
            }
        }
    }

    /** Every other keyed shape: the full key list is encoded and merged into the partial's group table. */
    private void scanGrouped(ArrowReader reader, ScanPlan plan, Partial partial, LanceCancellation cancellation) throws Exception {
        int keyCount = resolved.keyCount();
        GroupTable table = new GroupTable(resolved.keyKinds(), resolved.allMetrics());
        partial.groups = table;
        long[] encoded = new long[keyCount];
        BytesRefBuilder scratch = new BytesRefBuilder();
        Metric percentiles = plan.percentiles();
        double compression = percentiles != null ? percentiles.compression() : 0d;
        while (reader.loadNextBatch()) {
            cancellation.checkCancelled();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            FieldVector counts = plan.isMain() ? root.getVector(COUNT_COLUMN) : null;
            FieldVector[] keyVectors = new FieldVector[keyCount];
            for (int key = 0; key < keyCount; key++) {
                keyVectors[key] = root.getVector(KEY_COLUMN_PREFIX + key);
            }
            MetricBatch[] batches = plan.isMain() ? MetricBatch.resolve(resolved.allMetrics(), root) : null;
            FieldVector bin = percentiles != null ? root.getVector(percentiles.binColumn()) : null;
            FieldVector binCount = percentiles != null ? root.getVector(percentiles.binCountColumn()) : null;
            for (int row = 0; row < root.getRowCount(); row++) {
                if (counts != null) {
                    partial.total += longOrZero(counts, row);
                }
                long nullBits = 0L;
                boolean opensBucket = true;
                for (int key = 0; key < keyCount && opensBucket; key++) {
                    if (keyVectors[key].isNull(row)) {
                        // Documents without a value open no bucket at
                        // that level; without the outermost value
                        // (or any composite source value) they open
                        // no bucket at all.
                        nullBits |= 1L << key;
                        encoded[key] = 0L;
                        opensBucket = key > 0 && resolved.composite() == null;
                    } else {
                        encoded[key] = encodeKey(keyVectors[key], resolved.keyKind(key), row, resolved.dateInterval(key), table, scratch);
                        // A mask of 0 at the outermost level with no
                        // other bucket to hold it is the same: the
                        // row is in no bucket of the tree.
                        opensBucket = key > 0 || resolved.composite() != null || inSomeBucket(resolved.levels().get(0), encoded[key]);
                    }
                }
                if (!opensBucket) {
                    continue;
                }
                int idx = table.findOrAdd(encoded, nullBits);
                if (counts != null) {
                    table.addCount(idx, longOrZero(counts, row));
                }
                if (batches != null) {
                    for (MetricBatch batch : batches) {
                        batch.add(table.metrics, idx, row);
                    }
                } else if (bin != null && !bin.isNull(row)) {
                    long binRows = longOrZero(binCount, row);
                    if (binRows > 0L) {
                        // count(field), not count(*), in the bin scan:
                        // a row without a value has a null bin.
                        table.metrics.addBin(
                            percentiles.slot(),
                            idx,
                            compression,
                            plan.min(),
                            plan.max(),
                            plan.width(),
                            resolved.percentilesBins(),
                            asLong(bin, row),
                            binRows
                        );
                    }
                }
            }
        }
    }

    /** Whether an encoded key at level 0 puts its row into at least one bucket; every key of a non mask level does. */
    private static boolean inSomeBucket(Level level, long encodedKey) {
        return !level.kind().isMask() || encodedKey != 0L || level.otherBucketKey() != null;
    }

    /**
     * The group key encoded as one {@code long}: the (interval
     * multiplied) value of an integer or date key, the
     * {@link Double#doubleToLongBits} of a floating point key (whose
     * bit equality is {@link Double#equals} equality), or the id of a
     * string key in the table's dictionary. The encoding allocates
     * nothing: string bytes go through the reused {@code scratch}.
     */
    private static long encodeKey(FieldVector vector, KeyKind kind, int row, long dateInterval, GroupTable table, BytesRefBuilder scratch) {
        return switch (kind) {
            case STRING -> table.intern(readUtf8((VarCharVector) vector, row, scratch));
            case DOUBLE -> Double.doubleToLongBits(doubleKeyOf(vector, row));
            case LONG -> {
                long value = asLong(vector, row);
                yield dateInterval > 0L ? value * dateInterval : value;
            }
        };
    }
}
