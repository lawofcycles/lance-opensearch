/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.lance.Dataset;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Computes fragment-mode metric aggregations. Called from
 * {@link LanceDispatchActionFilter} when a request's aggregations
 * block only contains metric aggregations this class supports.
 *
 * <p>Supported types:
 * <ul>
 *   <li>{@code value_count} — non-null count of the field</li>
 *   <li>{@code sum} — sum of the field's numeric values</li>
 *   <li>{@code avg} — sum divided by non-null count</li>
 *   <li>{@code min} / {@code max} — extremes of the field's values</li>
 * </ul>
 *
 * <p>Unsupported shapes (bucket aggregations, script metrics, missing
 * value handling, sub-aggregations, or metric aggregations on
 * non-numeric fields) return {@link Optional#empty()} from
 * {@link #parseSupported(SearchSourceBuilder)} so the caller can fall
 * back to the standard shard-based path instead of failing the
 * request.
 *
 * <p>The class exposes two entry points that keep the fragment
 * dispatch path independent of OpenSearch's aggregator machinery for
 * the numeric baseline:
 * <ul>
 *   <li>{@link #parseSupported(SearchSourceBuilder)} classifies the
 *       request and, on success, returns the metric specs the
 *       fragment executor will run through OpenSearch's stock
 *       aggregators (Direction 1 / Stage 1). It is what the
 *       dispatch filter consults to decide whether to route to the
 *       fragment path or fall back to shard dispatch.</li>
 *   <li>{@link #aggregatePartials} keeps a Lance-only reference
 *       implementation that a prototype test
 *       ({@code PerFragmentAggregatorPrototypeTests}) uses as the
 *       oracle when comparing per-fragment IndexReader results to
 *       the direct Lance-scan numbers.</li>
 * </ul>
 *
 * <p>Cross-node partial merge lives on the transport layer now:
 * {@link TransportLanceFragmentQueryAction} ships
 * {@link org.opensearch.search.aggregations.InternalAggregations} on
 * the wire and
 * {@link TransportLanceCoordinatorAction.MergeState#buildResponse}
 * reduces them through
 * {@link org.opensearch.search.aggregations.InternalAggregations#topLevelReduce}.
 * The plugin-specific {@code mergePartials} that previously folded
 * {@link PartialState} lists into an
 * {@link org.opensearch.search.aggregations.InternalAggregations} is
 * gone as of the Stage 2 wire-format switch.
 */
public final class LanceMetricAggregator {

    private LanceMetricAggregator() {}

    /** Supported metric aggregation kinds. */
    public enum MetricType {
        VALUE_COUNT,
        SUM,
        AVG,
        MIN,
        MAX
    }

    /**
     * The subset of an {@link org.opensearch.search.aggregations.AggregationBuilder}
     * the aggregator needs. Extracted so the aggregator does not
     * depend on the full builder graph and can be constructed
     * defensively at parse time.
     *
     * <p>{@link Writeable} so per-node dispatch requests can carry
     * the spec list from coordinator to nodes without depending on
     * server-side re-parsing.
     */
    public record MetricSpec(String name, MetricType type, String field) implements Writeable {

        public MetricSpec(StreamInput in) throws IOException {
            this(in.readString(), MetricType.values()[in.readVInt()], in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVInt(type.ordinal());
            out.writeString(field);
        }
    }

    /**
     * Per-metric running totals produced by a single node's scan.
     * All four fields are always populated so a partial can be
     * merged into any metric type: {@code value_count} uses
     * {@link #count}, {@code sum} / {@code avg} use {@link #sum}
     * (and {@link #count} for {@code avg}), and {@code min} /
     * {@code max} use their respective extremes.
     *
     * <p>Serialisable so it can travel between nodes in transport
     * requests / responses. Read/write order matches the constructor
     * so a snapshot round-trips exactly.
     */
    public record PartialState(long count, double sum, double min, double max) implements Writeable {

        /** Neutral element for merges: preserves any incoming partial. */
        public static final PartialState EMPTY = new PartialState(0L, 0.0d, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);

        public PartialState(StreamInput in) throws IOException {
            this(in.readVLong(), in.readDouble(), in.readDouble(), in.readDouble());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(count);
            out.writeDouble(sum);
            out.writeDouble(min);
            out.writeDouble(max);
        }

        /**
         * Combine two partials into one, in the same associative
         * fashion the reduce phase of the standard metric
         * aggregators uses. Empty partials are absorbed by
         * {@link #EMPTY}.
         */
        public PartialState merge(PartialState other) {
            return new PartialState(count + other.count, sum + other.sum, Math.min(min, other.min), Math.max(max, other.max));
        }
    }

    /**
     * Extract the list of supported metrics from the request source.
     *
     * <p>Returns:
     * <ul>
     *   <li>{@link Optional#empty()} if any aggregation is outside the
     *       supported set (bucket, sub-aggs, script, missing values,
     *       non-{@code ValuesSourceAggregationBuilder} builder). The
     *       caller falls through to the shard path so those shapes
     *       still get an answer.</li>
     *   <li>{@code Optional.of(emptyList)} when the request has no
     *       aggregations block. This is the common hits-only case.</li>
     *   <li>{@code Optional.of(list)} for a supported combination of
     *       metrics ready to feed to {@link #aggregatePartials}.</li>
     * </ul>
     */
    public static Optional<List<MetricSpec>> parseSupported(SearchSourceBuilder source) {
        if (source == null || source.aggregations() == null) {
            return Optional.of(Collections.emptyList());
        }
        List<MetricSpec> specs = new ArrayList<>();
        for (AggregationBuilder builder : source.aggregations().getAggregatorFactories()) {
            if (!builder.getSubAggregations().isEmpty()) {
                // Fragment aggregation does not run its own bucket
                // pipeline; nested aggregations require the standard
                // aggregator tree to hand child buckets around.
                return Optional.empty();
            }
            MetricType type = resolveType(builder);
            if (type == null) {
                return Optional.empty();
            }
            if (!(builder instanceof ValuesSourceAggregationBuilder<?> valuesSource)) {
                return Optional.empty();
            }
            String field = valuesSource.field();
            if (field == null || field.isEmpty()) {
                // Script-based metrics or field-less builders route
                // through the shard path. Fragment mode has no
                // scripting sandbox.
                return Optional.empty();
            }
            specs.add(new MetricSpec(builder.getName(), type, field));
        }
        return Optional.of(specs);
    }

    private static MetricType resolveType(AggregationBuilder builder) {
        if (builder instanceof SumAggregationBuilder) {
            return MetricType.SUM;
        }
        if (builder instanceof AvgAggregationBuilder) {
            return MetricType.AVG;
        }
        if (builder instanceof MinAggregationBuilder) {
            return MetricType.MIN;
        }
        if (builder instanceof MaxAggregationBuilder) {
            return MetricType.MAX;
        }
        if (builder instanceof ValueCountAggregationBuilder) {
            return MetricType.VALUE_COUNT;
        }
        return null;
    }

    /**
     * Scan the specified {@code fragmentIds} of {@code dataset} under
     * {@code filterSql} and produce one {@link PartialState} per
     * metric spec (parallel to {@code specs}). When {@code specs} is
     * empty this returns an empty list without touching Lance so
     * callers can call it unconditionally.
     *
     * <p>The scan projects only the fields the metrics need to keep
     * I/O bounded, and pushes the filter into Lance so the numbers
     * agree with {@link Dataset#countRows(String)} on the same
     * fragments.
     *
     * <p>Passing {@code null} for {@code fragmentIds} scans every
     * fragment in the dataset. Passing a non-empty list restricts the
     * scan to those fragment ids, which is the entry point the
     * per-node handler uses during multi-node dispatch.
     */
    public static List<PartialState> aggregatePartials(Dataset dataset, List<Integer> fragmentIds, String filterSql, List<MetricSpec> specs)
        throws Exception {
        if (specs.isEmpty()) {
            return Collections.emptyList();
        }
        Map<String, State> stateByName = new LinkedHashMap<>();
        Set<String> requestedColumns = new HashSet<>();
        for (MetricSpec spec : specs) {
            stateByName.put(spec.name(), new State());
            requestedColumns.add(spec.field());
        }

        ScanOptions.Builder builder = new ScanOptions.Builder().columns(new ArrayList<>(requestedColumns));
        if (filterSql != null) {
            builder.filter(filterSql);
        }
        if (fragmentIds != null) {
            builder.fragmentIds(fragmentIds);
        }

        try (LanceScanner scanner = dataset.newScan(builder.build()); ArrowReader reader = scanner.scanBatches()) {
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                int rowCount = root.getRowCount();
                for (MetricSpec spec : specs) {
                    FieldVector vector = (FieldVector) root.getVector(spec.field());
                    if (vector == null) {
                        // Field absent from the scan schema. Skip this
                        // metric: shard mode would surface a mapping
                        // error, but here we just leave the metric at
                        // its default (0 / +-Infinity) so the response
                        // stays well-formed. The caller has already
                        // whitelisted the field via parseSupported.
                        continue;
                    }
                    State state = stateByName.get(spec.name());
                    updateState(spec, vector, rowCount, state);
                }
            }
        }

        List<PartialState> partials = new ArrayList<>(specs.size());
        for (MetricSpec spec : specs) {
            State state = stateByName.get(spec.name());
            partials.add(new PartialState(state.count, state.sum, state.min, state.max));
        }
        return partials;
    }

    private static void updateState(MetricSpec spec, FieldVector vector, int rowCount, State state) {
        for (int i = 0; i < rowCount; i++) {
            if (vector.isNull(i)) {
                continue;
            }
            switch (spec.type()) {
                case VALUE_COUNT -> state.count++;
                case SUM -> {
                    state.sum += readAsDouble(vector, i);
                    state.count++;
                }
                case AVG -> {
                    state.sum += readAsDouble(vector, i);
                    state.count++;
                }
                case MIN -> {
                    double v = readAsDouble(vector, i);
                    if (v < state.min) {
                        state.min = v;
                    }
                    state.count++;
                }
                case MAX -> {
                    double v = readAsDouble(vector, i);
                    if (v > state.max) {
                        state.max = v;
                    }
                    state.count++;
                }
            }
        }
    }

    /**
     * Coerce a numeric Arrow value to double. Only the integer and
     * floating-point widths Lance surfaces on typical text / numeric
     * columns are handled; other types cause the aggregator to
     * silently skip the row rather than throw, matching the
     * "skip unsupported cells" behaviour the retired Arrow-batch
     * source renderer used.
     */
    private static double readAsDouble(FieldVector vector, int rowIndex) {
        if (vector instanceof TinyIntVector v) {
            return v.get(rowIndex);
        }
        if (vector instanceof SmallIntVector v) {
            return v.get(rowIndex);
        }
        if (vector instanceof IntVector v) {
            return v.get(rowIndex);
        }
        if (vector instanceof BigIntVector v) {
            return v.get(rowIndex);
        }
        if (vector instanceof Float4Vector v) {
            return v.get(rowIndex);
        }
        if (vector instanceof Float8Vector v) {
            return v.get(rowIndex);
        }
        return 0.0d;
    }

    /**
     * Mutable per-metric running state used inside a single
     * {@link #aggregatePartials} call. Converted to an immutable
     * {@link PartialState} before returning.
     */
    private static final class State {
        long count = 0L;
        double sum = 0.0d;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
    }
}
