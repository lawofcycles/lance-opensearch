/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.execute;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
import org.lance.ipc.ColumnOrdering;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.lance.WireVersion;
import org.opensearch.lance.execute.LanceAggregateResults;
import org.opensearch.lance.plan.cost.AggregateProfile;
import org.opensearch.lance.plan.cost.CostInputs;
import org.opensearch.lance.plan.cost.CostModel;
import org.opensearch.lance.plan.cost.StorageKind;
import org.opensearch.lance.plan.lancesql.RexToLanceSql;
import org.opensearch.lance.plan.rel.LanceAggregate;
import org.opensearch.lance.plan.rel.LanceFtsMatch;
import org.opensearch.lance.plan.rel.LanceHitShape;
import org.opensearch.lance.plan.rel.LanceKnnSearch;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.LanceTopK;
import org.opensearch.lance.plan.rel.MetricSpec;
import org.opensearch.lance.plan.rel.PushedOperation.PushedAggregate;
import org.opensearch.lance.plan.rel.PushedOperation.PushedFilter;
import org.opensearch.lance.plan.rel.PushedOperation.PushedFts;
import org.opensearch.lance.plan.rel.PushedOperation.PushedKnn;
import org.opensearch.lance.plan.rel.PushedOperation.PushedTopK;
import org.opensearch.lance.plan.substrait.LanceSubstraitFilterProducer;
import org.opensearch.lance.plan.rel.physical.HeapTopKExec;
import org.opensearch.lance.plan.rel.physical.LuceneAggregateExec;
import org.opensearch.lance.query.LanceKnnQueryBuilder;
import org.opensearch.lance.query.LanceScanFilter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The per node plan the coordinator ships with every fragment request:
 * the physical subtree the Volcano run chose for one target, reduced
 * to what the fragment executor needs to run it. The executor does not
 * plan; it reads this object, applies the node local guards
 * ({@link FragmentPlanRefiner}) and executes.
 *
 * <p>Three parts make up a plan. The query part is the same for every
 * kind: {@link #filterSql()} is the Lance SQL of the scalar predicate
 * (a pushed filter, the prefilter of a pushed full text or knn
 * operation, the filter under a pushed aggregate, or the {@code Filter}
 * below a Lucene operator printed by {@link RexToLanceSql}),
 * {@link #filterSubstrait()} the Substrait bytes of the same predicate
 * when the planner chose a Substrait pushed filter (the scalar scans
 * then evaluate the bytes and the SQL, when present, only feeds the
 * column loads that take SQL), and {@link #lanceClause()} the full text
 * or knn clause whose Lucene {@code Query} the executor builds through
 * the mapping; all are null when the query has no Lance spelling and
 * the executor builds the Lucene query from the request's own builder. The envelope part is
 * one of {@link #topK()} (an ordered, cut page the Lance scan returns)
 * or {@link #aggregate()} (a Substrait aggregate the Lance scan
 * computes), or neither when Lucene's collector and aggregators run.
 * {@link #kind()} names which physical root produced the plan. A fourth
 * part is optional: {@link #excludedFragmentIds()} lists the fragments
 * the coordinator's zone map pruning proved empty of matching rows
 * ({@link org.opensearch.lance.plan.prune.ZoneMapPruner}); the executor
 * leaves them out of every scan of the request. Empty when nothing was
 * pruned.
 *
 * <p>The wire format is internal to the plugin. The stream opens with
 * {@link #WIRE_VERSION} (see {@link WireVersion}), followed by the
 * base fields of version 1 (the kind, the filter SQL, the Lance
 * clause, the pushed page and the pushed aggregate) and one block per
 * later version: version 2 added the excluded fragment ids, version 3
 * the Substrait filter bytes. A reader of an older plugin version
 * steps over the pruning block, because scanning the pruned fragments
 * too still answers correctly, and refuses a plan whose Substrait
 * filter it cannot evaluate, because ignoring the filter would answer
 * wrongly; a reader of a newer version takes the fallbacks of the
 * blocks an older writer did not send. The plan travels inside
 * {@link org.opensearch.lance.dispatch.LanceFragmentQueryRequest}
 * and {@link org.opensearch.lance.plan.explain.LanceExplainResponse},
 * which carry markers of their own for the fields around it.
 */
public final class FragmentPlan implements Writeable, ToXContentObject {

    /**
     * The wire format's version, the first field written and the first
     * read; 2 added the excluded fragment ids, 3 the Substrait filter.
     */
    public static final int WIRE_VERSION = 3;

    private static final int[] NO_EXCLUDED_FRAGMENTS = new int[0];

    /** Which physical root the plan came from, and so how the envelope executes. */
    public enum Kind {
        /** The root is the {@link LanceTableScan}: the scan computes the page or the aggregate. */
        PUSHED_SCAN,
        /** Lucene's stock aggregators run over the fragment readers. */
        LUCENE_AGGREGATE,
        /** Lucene's top docs collector cuts the hits page. */
        LUCENE_TOPK,
        /** {@code size} 0 without aggregations: the matched count route. */
        LUCENE_COUNT;

        static Kind read(StreamInput in) throws IOException {
            return in.readEnum(Kind.class);
        }
    }

    /**
     * One ordering of a pushed page, the wire form of a Lance
     * {@link ColumnOrdering}.
     */
    public record ScanOrdering(String column, boolean ascending, boolean nullsFirst) implements Writeable {

        public ScanOrdering {
            Objects.requireNonNull(column, "column");
        }

        public static ScanOrdering of(ColumnOrdering ordering) {
            return new ScanOrdering(ordering.getColumnName(), ordering.isAscending(), ordering.isNullFirst());
        }

        public static ScanOrdering read(StreamInput in) throws IOException {
            return new ScanOrdering(in.readString(), in.readBoolean(), in.readBoolean());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(column);
            out.writeBoolean(ascending);
            out.writeBoolean(nullsFirst);
        }

        /** The Lance ordering the executor hands to {@code ScanOptions.setColumnOrderings}. */
        public ColumnOrdering toColumnOrdering() {
            ColumnOrdering.Builder builder = new ColumnOrdering.Builder();
            builder.setColumnName(column);
            builder.setAscending(ascending);
            builder.setNullFirst(nullsFirst);
            return builder.build();
        }
    }

    /**
     * The page a pushed top-k cuts: the Lance orderings (empty when the
     * FTS or knn scan's own order is the page order and the scan is only
     * limited), the rows kept, and the SQL of the {@code search_after}
     * cursor bound ANDed into the scan filter, or null for a first page.
     */
    public record TopK(List<ScanOrdering> orderings, int fetch, String cursorSql) implements Writeable {

        public TopK {
            orderings = List.copyOf(Objects.requireNonNull(orderings, "orderings"));
            if (fetch < 1) {
                throw new IllegalArgumentException("fetch must be at least 1, got " + fetch);
            }
        }

        public static TopK read(StreamInput in) throws IOException {
            return new TopK(in.readList(ScanOrdering::read), in.readVInt(), in.readOptionalString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(orderings);
            out.writeVInt(fetch);
            out.writeOptionalString(cursorSql);
        }

        /** The orderings as Lance {@link ColumnOrdering}s. */
        public List<ColumnOrdering> toColumnOrderings() {
            List<ColumnOrdering> result = new ArrayList<>(orderings.size());
            for (ScanOrdering ordering : orderings) {
                result.add(ordering.toColumnOrdering());
            }
            return result;
        }

        /**
         * The filter of the ordered scan: {@code filterSql} ANDed with
         * the cursor bound, either alone when the other is absent, null
         * when the page is an unfiltered first page.
         */
        public String scanFilterSql(String filterSql) {
            if (filterSql == null) {
                return cursorSql;
            }
            if (cursorSql == null) {
                return filterSql;
            }
            return "(" + filterSql + ") AND (" + cursorSql + ")";
        }
    }

    /** One aggregate call of a pushed aggregate: the request's aggregation name and the metric kind, in call order. */
    public record MetricSlot(String name, MetricSpec.Kind kind) implements Writeable {

        public MetricSlot {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(kind, "kind");
        }

        public static MetricSlot of(MetricSpec spec) {
            return new MetricSlot(spec.aggregationName(), spec.kind());
        }

        public static MetricSlot read(StreamInput in) throws IOException {
            return new MetricSlot(in.readString(), in.readEnum(MetricSpec.Kind.class));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeEnum(kind);
        }
    }

    /**
     * A pushed aggregate: the Substrait bytes of the main scan, the
     * number of group keys and the metric slots, which is what the
     * executor's resolution checks the request's builders against, and
     * the cost of the alternative the coordinator chose against, for
     * the data node's column store guard: {@link #pushedMillis()} is
     * what the cost model predicted for this scan, {@link #luceneWarmMillis()}
     * what it predicts for the Lucene aggregators when every column
     * they read is already resident in the node's column store (the
     * run's inputs over local storage), and {@link #luceneColumns()}
     * names those columns. Both numbers are zero for a table below the
     * fitted model's range, where the model is a placeholder and the
     * guard never fires.
     */
    public static final class Aggregate implements Writeable {

        private final byte[] substrait;
        private final int groupCount;
        private final List<MetricSlot> metrics;
        private final double pushedMillis;
        private final double luceneWarmMillis;
        private final List<String> luceneColumns;

        /** An aggregate without cost numbers: the placeholder regime, where the column store guard never fires. */
        public Aggregate(byte[] substrait, int groupCount, List<MetricSlot> metrics) {
            this(substrait, groupCount, metrics, 0.0, 0.0, List.of());
        }

        public Aggregate(
            byte[] substrait,
            int groupCount,
            List<MetricSlot> metrics,
            double pushedMillis,
            double luceneWarmMillis,
            List<String> luceneColumns
        ) {
            this.substrait = Objects.requireNonNull(substrait, "substrait").clone();
            if (groupCount < 0) {
                throw new IllegalArgumentException("groupCount must not be negative, got " + groupCount);
            }
            this.groupCount = groupCount;
            this.metrics = List.copyOf(Objects.requireNonNull(metrics, "metrics"));
            if (pushedMillis < 0.0 || luceneWarmMillis < 0.0) {
                throw new IllegalArgumentException("costs must not be negative, got " + pushedMillis + " and " + luceneWarmMillis);
            }
            this.pushedMillis = pushedMillis;
            this.luceneWarmMillis = luceneWarmMillis;
            this.luceneColumns = List.copyOf(Objects.requireNonNull(luceneColumns, "luceneColumns"));
        }

        /**
         * From the pushed operation the planner folded into {@code scan},
         * costed under {@code inputs}, the same inputs the planner
         * compared the two forms with. Over a table in the fitted
         * model's range the profile is rebuilt from the aggregate the
         * scan carries, exactly as the scan's own cost computed it;
         * below the range the numbers are zero.
         */
        public static Aggregate of(PushedAggregate pushed, LanceTableScan scan, CostInputs inputs) {
            ByteBuffer bytes = pushed.substrait();
            byte[] copy = new byte[bytes.remaining()];
            bytes.get(copy);
            List<MetricSlot> slots = new ArrayList<>();
            for (MetricSpec spec : pushed.aggregate().metricSpecs()) {
                slots.add(MetricSlot.of(spec));
            }
            List<String> columns = AggregateProfile.columnNames(pushed.aggregate(), scan);
            double pushedMillis = 0.0;
            double luceneWarmMillis = 0.0;
            if (CostModel.usesFittedModel(scan.getTable().getRowCount())) {
                AggregateProfile profile = AggregateProfile.of(pushed.aggregate(), scan, scan.getCluster().getMetadataQuery());
                pushedMillis = CostModel.pushedAggregateMillis(inputs, profile);
                luceneWarmMillis = CostModel.luceneAggregateMillis(inputs.withStorage(StorageKind.LOCAL), profile);
            }
            return new Aggregate(copy, pushed.aggregate().getGroupCount(), slots, pushedMillis, luceneWarmMillis, columns);
        }

        public static Aggregate read(StreamInput in) throws IOException {
            return new Aggregate(
                in.readByteArray(),
                in.readVInt(),
                in.readList(MetricSlot::read),
                in.readDouble(),
                in.readDouble(),
                in.readStringList()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByteArray(substrait);
            out.writeVInt(groupCount);
            out.writeList(metrics);
            out.writeDouble(pushedMillis);
            out.writeDouble(luceneWarmMillis);
            out.writeStringCollection(luceneColumns);
        }

        /**
         * The encoded main scan in a fresh direct buffer, the form the
         * JNI side reads ({@code GetDirectBufferAddress} returns null for
         * a heap buffer).
         */
        public ByteBuffer substraitDirect() {
            ByteBuffer direct = ByteBuffer.allocateDirect(substrait.length);
            direct.put(substrait);
            direct.flip();
            return direct;
        }

        /** The encoded main scan's bytes, for equality checks and tests. */
        public byte[] substraitBytes() {
            return substrait.clone();
        }

        /** The shape {@link LanceAggregateResults#resolve} checks the request's builders against. */
        public LanceAggregateResults.PushedShape toPushedShape() {
            List<LanceAggregateResults.MetricSlot> slots = new ArrayList<>(metrics.size());
            for (MetricSlot slot : metrics) {
                slots.add(new LanceAggregateResults.MetricSlot(slot.name(), slot.kind()));
            }
            return new LanceAggregateResults.PushedShape(groupCount, slots);
        }

        public int groupCount() {
            return groupCount;
        }

        public List<MetricSlot> metrics() {
            return metrics;
        }

        /** Predicted milliseconds of this pushed scan under the coordinator's inputs; zero below the fitted model's range. */
        public double pushedMillis() {
            return pushedMillis;
        }

        /**
         * Predicted milliseconds of the Lucene aggregators over resident
         * columns under the coordinator's inputs; zero below the fitted
         * model's range.
         */
        public double luceneWarmMillis() {
            return luceneWarmMillis;
        }

        /** The table columns the Lucene aggregators would read, in the table's row type order. */
        public List<String> luceneColumns() {
            return luceneColumns;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Aggregate other)) {
                return false;
            }
            return groupCount == other.groupCount
                && metrics.equals(other.metrics)
                && Arrays.equals(substrait, other.substrait)
                && Double.compare(pushedMillis, other.pushedMillis) == 0
                && Double.compare(luceneWarmMillis, other.luceneWarmMillis) == 0
                && luceneColumns.equals(other.luceneColumns);
        }

        @Override
        public int hashCode() {
            return Objects.hash(groupCount, metrics, Arrays.hashCode(substrait), pushedMillis, luceneWarmMillis, luceneColumns);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("aggregate{groups=").append(groupCount)
                .append(", metrics=")
                .append(metrics)
                .append(", substraitBytes=")
                .append(substrait.length)
                .append(", luceneColumns=")
                .append(luceneColumns);
            if (pushedMillis > 0.0 || luceneWarmMillis > 0.0) {
                sb.append(", pushedMs=").append(Math.round(pushedMillis)).append(", luceneWarmMs=").append(Math.round(luceneWarmMillis));
            }
            return sb.append('}').toString();
        }
    }

    private final Kind kind;
    private final String filterSql;
    private final byte[] filterSubstrait;
    private final QueryBuilder lanceClause;
    private final TopK topK;
    private final Aggregate aggregate;
    private final int[] excludedFragmentIds;

    /** A plan whose scalar predicate, when set, is spelled as Lance SQL only, excluding no fragment. */
    public FragmentPlan(Kind kind, String filterSql, QueryBuilder lanceClause, TopK topK, Aggregate aggregate) {
        this(kind, filterSql, null, lanceClause, topK, aggregate, NO_EXCLUDED_FRAGMENTS);
    }

    /**
     * A plan whose scalar predicate, when set, is spelled as Lance SQL only.
     *
     * @param excludedFragmentIds the fragments the executor leaves out
     *     of its scans, in ascending order without duplicates; the empty
     *     array when nothing was pruned
     */
    public FragmentPlan(Kind kind, String filterSql, QueryBuilder lanceClause, TopK topK, Aggregate aggregate, int[] excludedFragmentIds) {
        this(kind, filterSql, null, lanceClause, topK, aggregate, excludedFragmentIds);
    }

    /** A plan excluding no fragment. */
    public FragmentPlan(Kind kind, String filterSql, byte[] filterSubstrait, QueryBuilder lanceClause, TopK topK, Aggregate aggregate) {
        this(kind, filterSql, filterSubstrait, lanceClause, topK, aggregate, NO_EXCLUDED_FRAGMENTS);
    }

    /**
     * @param filterSql the Lance SQL of the scalar predicate, or null
     * @param filterSubstrait the Substrait bytes of the scalar predicate
     *     the scans evaluate instead of the SQL, or null; only a scalar
     *     shape carries them (a full text or knn clause takes its
     *     prefilter as SQL)
     * @param excludedFragmentIds the fragments the executor leaves out
     *     of its scans, in ascending order without duplicates; the empty
     *     array when nothing was pruned
     */
    public FragmentPlan(
        Kind kind,
        String filterSql,
        byte[] filterSubstrait,
        QueryBuilder lanceClause,
        TopK topK,
        Aggregate aggregate,
        int[] excludedFragmentIds
    ) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.filterSql = filterSql;
        this.filterSubstrait = filterSubstrait == null ? null : filterSubstrait.clone();
        this.lanceClause = lanceClause;
        this.topK = topK;
        this.aggregate = aggregate;
        this.excludedFragmentIds = Objects.requireNonNull(excludedFragmentIds, "excludedFragmentIds").clone();
        if (topK != null && aggregate != null) {
            throw new IllegalArgumentException("a plan carries a pushed page or a pushed aggregate, not both");
        }
        if (kind != Kind.PUSHED_SCAN && (topK != null || aggregate != null)) {
            throw new IllegalArgumentException("only a " + Kind.PUSHED_SCAN + " plan carries a pushed page or aggregate, got " + kind);
        }
        if (filterSubstrait != null && lanceClause != null) {
            throw new IllegalArgumentException("a full text or knn clause takes its prefilter as SQL, not as Substrait bytes");
        }
        if (filterSubstrait != null && filterSubstrait.length == 0) {
            throw new IllegalArgumentException("the Substrait filter must not be empty");
        }
        for (int i = 1; i < this.excludedFragmentIds.length; i++) {
            if (this.excludedFragmentIds[i] <= this.excludedFragmentIds[i - 1]) {
                throw new IllegalArgumentException(
                    "excluded fragment ids must be ascending and distinct, got " + Arrays.toString(this.excludedFragmentIds)
                );
            }
        }
    }

    public FragmentPlan(StreamInput in) throws IOException {
        this(read(in, WIRE_VERSION));
    }

    private FragmentPlan(FragmentPlan read) {
        this(read.kind, read.filterSql, read.filterSubstrait, read.lanceClause, read.topK, read.aggregate, read.excludedFragmentIds);
    }

    /**
     * Reads a plan as a node whose plugin is at wire version
     * {@code asVersion} would: the blocks of later versions are stepped
     * over or refused as {@link WireVersion.Reader} describes. The
     * transport reads with {@link #WIRE_VERSION}; the mixed version
     * tests read with the versions before it.
     */
    static FragmentPlan read(StreamInput in, int asVersion) throws IOException {
        WireVersion.Reader reader = WireVersion.read(in, "FragmentPlan", asVersion);
        Kind kind = Kind.read(in);
        String filterSql = in.readOptionalString();
        QueryBuilder lanceClause = in.readOptionalNamedWriteable(QueryBuilder.class);
        TopK topK = in.readOptionalWriteable(TopK::read);
        Aggregate aggregate = in.readOptionalWriteable(Aggregate::read);
        int[] excludedFragmentIds = reader.block(2, StreamInput::readVIntArray, NO_EXCLUDED_FRAGMENTS);
        byte[] filterSubstrait = reader.block(3, FragmentPlan::readSubstraitBlock, null);
        reader.finish();
        return new FragmentPlan(kind, filterSql, filterSubstrait, lanceClause, topK, aggregate, excludedFragmentIds);
    }

    /**
     * The Substrait block's fields: the filter bytes, or null when the
     * block is empty. The block itself is the flag: a writer without a
     * filter writes an empty block, so the bytes need no boolean in
     * front of them.
     */
    private static byte[] readSubstraitBlock(StreamInput in) throws IOException {
        return in.available() == 0 ? null : in.readByteArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        WireVersion.write(out, WIRE_VERSION);
        out.writeEnum(kind);
        out.writeOptionalString(filterSql);
        out.writeOptionalNamedWriteable(lanceClause);
        out.writeOptionalWriteable(topK);
        out.writeOptionalWriteable(aggregate);
        // An older node that ignores the pruning list scans the pruned
        // fragments too and still answers correctly, so the block is
        // never critical.
        WireVersion.writeBlock(out, false, o -> o.writeVIntArray(excludedFragmentIds));
        // An older node that ignores the Substrait filter would scan
        // without the predicate, so the block is critical whenever a
        // filter is set. Without a filter the block is empty, which is
        // what the reader takes as absent.
        WireVersion.writeBlock(out, filterSubstrait != null, o -> {
            if (filterSubstrait != null) {
                o.writeByteArray(filterSubstrait);
            }
        });
    }

    /**
     * A plan without a planned tree: the request's envelope alone
     * decides the kind and the executor builds the Lucene query from
     * the request's builder, with {@code filterSql} (may be null) as the
     * scalar filter. The coordinator uses it when the translator cannot
     * spell the query at all.
     */
    public static FragmentPlan lucene(Kind kind, String filterSql) {
        if (kind == Kind.PUSHED_SCAN) {
            throw new IllegalArgumentException("a Lucene plan needs a Lucene kind, got " + kind);
        }
        return new FragmentPlan(kind, filterSql, null, null, null);
    }

    /** The Lucene kind the request's envelope selects: aggregations, a page, or the count. */
    public static Kind luceneKind(boolean hasAggregations, boolean hits) {
        if (hasAggregations) {
            return Kind.LUCENE_AGGREGATE;
        }
        return hits ? Kind.LUCENE_TOPK : Kind.LUCENE_COUNT;
    }

    /**
     * Reads the per node plan off the physical root the planner
     * returned. A {@link LanceTableScan} root carrying a pushed
     * aggregate or a pushed page is a {@link Kind#PUSHED_SCAN}; a scan
     * carrying only the query (a filter, a full text or knn operation,
     * or nothing) executes its envelope on Lucene and takes the kind the
     * request shape selects. A {@link LuceneAggregateExec} or
     * {@link HeapTopKExec} root names its kind directly. Any other root
     * (a logical tree the planner could not lower) takes the shape's
     * kind. In every case the query part is read from the chain between
     * the root and the scan: a {@code Filter} is encoded for Lance, a
     * full text or knn node contributes its clause, a scan its pushed
     * operations. A {@code Filter} left in a Lucene operator's wrapped
     * tree (a page or an aggregation the pushdown rules did not fold)
     * is the scalar query of a Lucene kind plan, so it takes the
     * encoding the pushed filter of the same predicate would have
     * taken: both encodings are produced and
     * {@link CostModel#filterEncodingMillis} orders them under
     * {@code inputs}, the same comparison the planner makes between the
     * two pushed forms. A {@code Filter} neither encoder can spell
     * leaves the whole query to the Lucene side (both parts null),
     * because the Lucene composition of the request's builder is the
     * only form that evaluates every clause; a {@code Filter} under a
     * full text or knn node is that node's prefilter and travels as SQL
     * only.
     *
     * @param root the physical (or unlowered logical) per node plan
     * @param hasAggregations whether the request carries aggregations
     * @param hits whether the request asks for a page ({@code size} above 0)
     * @param inputs the cost inputs the planner chose {@code root} under,
     *     which a pushed aggregate's alternative cost and a wrapped
     *     filter's encoding are computed with
     */
    public static FragmentPlan of(RelNode root, boolean hasAggregations, boolean hits, CostInputs inputs) {
        Kind shapeKind = luceneKind(hasAggregations, hits);
        QueryPart query = queryPart(root, inputs);
        if (root instanceof LanceTableScan scan) {
            Optional<PushedAggregate> pushedAggregate = scan.pushedAggregate();
            if (pushedAggregate.isPresent()) {
                return new FragmentPlan(
                    Kind.PUSHED_SCAN,
                    query.filterSql(),
                    query.lanceClause(),
                    null,
                    Aggregate.of(pushedAggregate.get(), scan, inputs)
                );
            }
            Optional<PushedTopK> pushedTopK = scan.pushedTopK();
            if (pushedTopK.isPresent()) {
                PushedTopK pushed = pushedTopK.get();
                List<ScanOrdering> orderings = new ArrayList<>();
                for (ColumnOrdering ordering : pushed.toScanOrderings()) {
                    orderings.add(ScanOrdering.of(ordering));
                }
                TopK topK = new TopK(orderings, pushed.fetch(), pushed.cursorSql());
                return new FragmentPlan(Kind.PUSHED_SCAN, query.filterSql(), query.filterSubstrait(), query.lanceClause(), topK, null);
            }
            return new FragmentPlan(shapeKind, query.filterSql(), query.filterSubstrait(), query.lanceClause(), null, null);
        }
        if (root instanceof LuceneAggregateExec) {
            return new FragmentPlan(Kind.LUCENE_AGGREGATE, query.filterSql(), query.filterSubstrait(), query.lanceClause(), null, null);
        }
        if (root instanceof HeapTopKExec) {
            return new FragmentPlan(Kind.LUCENE_TOPK, query.filterSql(), query.filterSubstrait(), query.lanceClause(), null, null);
        }
        return new FragmentPlan(shapeKind, query.filterSql(), query.filterSubstrait(), query.lanceClause(), null, null);
    }

    /** The query part read off a plan chain. */
    private record QueryPart(String filterSql, byte[] filterSubstrait, QueryBuilder lanceClause) {
        static final QueryPart NONE = new QueryPart(null, null, null);
    }

    private static QueryPart queryPart(RelNode root, CostInputs inputs) {
        String filterSql = null;
        RexNode filterCondition = null;
        QueryBuilder clause = null;
        RelNode node = root;
        while (node != null) {
            if (node instanceof LanceTableScan scan) {
                Optional<PushedAggregate> aggregate = scan.pushedAggregate();
                if (aggregate.isPresent()) {
                    return new QueryPart(aggregate.get().filterSql(), null, null);
                }
                Optional<PushedFts> fts = scan.pushedFts();
                if (fts.isPresent()) {
                    return new QueryPart(fts.get().filterSql(), null, fts.get().fts().ftsClause());
                }
                Optional<PushedKnn> knn = scan.pushedKnn();
                if (knn.isPresent()) {
                    return new QueryPart(knn.get().filterSql(), null, knn.get().knn().knnClause());
                }
                Optional<PushedFilter> filter = scan.pushedFilter();
                if (filter.isPresent()) {
                    // A pushed filter under a full text or knn node is
                    // the node's prefilter, which travels as SQL only.
                    byte[] bytes = clause == null ? substraitBytes(filter.get()) : null;
                    return new QueryPart(filter.get().sql(), bytes, clause);
                }
                if (filterCondition != null && clause == null) {
                    // The Filter stayed in a Lucene operator's tree:
                    // encode it as the pushed filter of the same
                    // predicate would be, and let the cost order the two.
                    PushedFilter chosen = encodeWrappedFilter(filterCondition, scan, inputs);
                    if (chosen == null) {
                        return QueryPart.NONE;
                    }
                    return new QueryPart(chosen.sql(), substraitBytes(chosen), null);
                }
                return new QueryPart(filterSql, null, clause);
            }
            if (node instanceof LuceneAggregateExec exec) {
                node = exec.aggregate();
            } else if (node instanceof HeapTopKExec exec) {
                node = exec.topK();
            } else if (node instanceof Filter filter) {
                if (filterCondition != null) {
                    // Two filters in one chain never occur in a translated
                    // tree; leave the whole query to the Lucene side.
                    return QueryPart.NONE;
                }
                Optional<String> sql = RexToLanceSql.print(filter.getCondition(), filter.getInput().getRowType());
                if (sql.isEmpty() && clause != null) {
                    // A prefilter travels as SQL only.
                    return QueryPart.NONE;
                }
                filterSql = sql.orElse(null);
                filterCondition = filter.getCondition();
                node = filter.getInput();
            } else if (node instanceof LanceFtsMatch fts) {
                clause = fts.ftsClause();
                node = fts.getInput();
            } else if (node instanceof LanceKnnSearch knn) {
                clause = knn.knnClause();
                node = knn.getInput();
            } else if (node instanceof Project
                || node instanceof LanceAggregate
                || node instanceof LanceTopK
                || node instanceof LanceHitShape) {
                    node = node.getInput(0);
                } else {
                    return QueryPart.NONE;
                }
        }
        return QueryPart.NONE;
    }

    /**
     * The encoding a {@code Filter} left in a Lucene operator's tree
     * takes: the SQL and the Substrait candidates of {@code condition}
     * over {@code scan}, ordered by {@link CostModel#filterEncodingMillis}
     * under {@code inputs} exactly as the scan's cost orders the two
     * pushed forms; null when neither encoder can spell the predicate.
     */
    private static PushedFilter encodeWrappedFilter(RexNode condition, LanceTableScan scan, CostInputs inputs) {
        Optional<String> sql = RexToLanceSql.print(condition, scan.getRowType());
        Optional<ByteBuffer> bytes = LanceSubstraitFilterProducer.toLanceFilter(
            condition,
            scan.getRowType(),
            scan.getCluster().getTypeFactory()
        );
        if (bytes.isEmpty() && sql.isEmpty()) {
            return null;
        }
        if (bytes.isEmpty()) {
            return new PushedFilter(condition, sql.get());
        }
        PushedFilter substrait = new PushedFilter(condition, sql.orElse(null), bytes.get());
        if (sql.isEmpty()) {
            return substrait;
        }
        PushedFilter plain = new PushedFilter(condition, sql.get());
        return CostModel.filterEncodingMillis(inputs, substrait) <= CostModel.filterEncodingMillis(inputs, plain) ? substrait : plain;
    }

    /** The pushed filter's Substrait bytes as a heap array, or null for a SQL only filter. */
    private static byte[] substraitBytes(PushedFilter filter) {
        ByteBuffer bytes = filter.substrait();
        if (bytes == null) {
            return null;
        }
        byte[] copy = new byte[bytes.remaining()];
        bytes.get(copy);
        return copy;
    }

    public Kind kind() {
        return kind;
    }

    /**
     * The Lance SQL of the scalar predicate: the whole query for a
     * scalar shape, the prefilter of a full text or knn shape, null when
     * the query has no Lance spelling or is {@code match_all}. When
     * {@link #filterSubstrait()} is set as well, the scans evaluate the
     * bytes and this SQL only feeds the column loads that take SQL.
     */
    public String filterSql() {
        return filterSql;
    }

    /**
     * The Substrait {@code ExtendedExpression} bytes of the scalar
     * predicate, which the scalar scans hand to
     * {@code ScanOptions.substraitFilter} instead of the SQL; null when
     * the planner chose the SQL encoding or the query has no scalar
     * predicate. A copy: the array is never shared.
     */
    public byte[] filterSubstrait() {
        return filterSubstrait == null ? null : filterSubstrait.clone();
    }

    /**
     * The scalar filter of the scan the executor's Lucene side runs: the
     * filter SQL when the query is a scalar shape, null when a full text
     * or knn clause carries the query (the SQL is then that clause's
     * prefilter and travels inside the Lance query).
     */
    public String scalarFilterSql() {
        return lanceClause == null ? filterSql : null;
    }

    /**
     * The scalar filter the executor's scalar scans evaluate, in the
     * encoding the planner chose: the Substrait bytes when present, the
     * SQL otherwise; null when a full text or knn clause carries the
     * query or the query has no scalar predicate.
     */
    public LanceScanFilter scalarFilter() {
        if (lanceClause != null) {
            return null;
        }
        if (filterSubstrait != null) {
            return LanceScanFilter.substrait(filterSubstrait, filterSql);
        }
        return filterSql == null ? null : LanceScanFilter.sql(filterSql);
    }

    /** The full text or knn clause the executor builds the Lance query from, or null for a scalar shape. */
    public QueryBuilder lanceClause() {
        return lanceClause;
    }

    /** Whether the Lance clause is a knn search. */
    public boolean isKnn() {
        return lanceClause instanceof LanceKnnQueryBuilder;
    }

    /** The pushed page, or null. */
    public TopK topK() {
        return topK;
    }

    /** The pushed aggregate, or null. */
    public Aggregate aggregate() {
        return aggregate;
    }

    /**
     * The fragments the executor leaves out of every scan of the
     * request, in ascending order: the ones the coordinator's zone map
     * pruning proved empty of rows matching the query predicate. Empty
     * when nothing was pruned.
     */
    public int[] excludedFragmentIds() {
        return excludedFragmentIds.clone();
    }

    /** Whether the plan excludes at least one fragment. */
    public boolean excludesFragments() {
        return excludedFragmentIds.length > 0;
    }

    /**
     * {@code fragmentIds} without the excluded fragments, in the order
     * given; the same list when the plan excludes nothing or none of
     * them is listed.
     */
    public List<Integer> retainedFragments(List<Integer> fragmentIds) {
        if (excludedFragmentIds.length == 0) {
            return fragmentIds;
        }
        List<Integer> retained = new ArrayList<>(fragmentIds.size());
        for (Integer id : fragmentIds) {
            if (Arrays.binarySearch(excludedFragmentIds, id) < 0) {
                retained.add(id);
            }
        }
        return retained.size() == fragmentIds.size() ? fragmentIds : retained;
    }

    /** The same plan excluding {@code fragmentIds} (ascending, distinct); this plan when the array is empty and nothing was excluded. */
    public FragmentPlan withExcludedFragments(int[] fragmentIds) {
        if (Arrays.equals(fragmentIds, excludedFragmentIds)) {
            return this;
        }
        return new FragmentPlan(kind, filterSql, filterSubstrait, lanceClause, topK, aggregate, fragmentIds);
    }

    /** The same plan with the pushed aggregate dropped: the aggregators run over the same query. */
    public FragmentPlan withoutAggregate() {
        if (aggregate == null) {
            return this;
        }
        return new FragmentPlan(Kind.LUCENE_AGGREGATE, filterSql, filterSubstrait, lanceClause, null, null, excludedFragmentIds);
    }

    /** The same plan with the pushed page dropped: the collector cuts the page over the same query. */
    public FragmentPlan withoutTopK() {
        if (topK == null) {
            return this;
        }
        return new FragmentPlan(Kind.LUCENE_TOPK, filterSql, filterSubstrait, lanceClause, null, null, excludedFragmentIds);
    }

    /**
     * The same plan with the Lance clause and its prefilter dropped:
     * the executor builds the Lucene composition of the request's own
     * query builder instead. The kind stays, the pushed page or
     * aggregate must have been dropped before. The excluded fragments
     * stay as well: the Lucene composition evaluates the same predicate
     * the pruning judged.
     */
    public FragmentPlan withoutLanceClause() {
        if (lanceClause == null) {
            return this;
        }
        return new FragmentPlan(kind, null, null, null, topK, aggregate, excludedFragmentIds);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FragmentPlan other)) {
            return false;
        }
        return kind == other.kind
            && Objects.equals(filterSql, other.filterSql)
            && Arrays.equals(filterSubstrait, other.filterSubstrait)
            && Objects.equals(lanceClause, other.lanceClause)
            && Objects.equals(topK, other.topK)
            && Objects.equals(aggregate, other.aggregate)
            && Arrays.equals(excludedFragmentIds, other.excludedFragmentIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            kind,
            filterSql,
            Arrays.hashCode(filterSubstrait),
            lanceClause,
            topK,
            aggregate,
            Arrays.hashCode(excludedFragmentIds)
        );
    }

    /** Names the kind and every set part, for the executor's debug log. */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(kind.name());
        if (filterSql != null) {
            sb.append(" filter=").append(filterSql);
        }
        if (filterSubstrait != null) {
            sb.append(" filterSubstraitBytes=").append(filterSubstrait.length);
        }
        if (lanceClause != null) {
            sb.append(" clause=").append(lanceClause.getWriteableName());
        }
        if (topK != null) {
            sb.append(" topk{orderings=").append(topK.orderings()).append(", fetch=").append(topK.fetch());
            if (topK.cursorSql() != null) {
                sb.append(", cursor=").append(topK.cursorSql());
            }
            sb.append('}');
        }
        if (aggregate != null) {
            sb.append(' ').append(aggregate);
        }
        if (excludedFragmentIds.length > 0) {
            sb.append(" excluded=").append(Arrays.toString(excludedFragmentIds));
        }
        return sb.toString();
    }

    /**
     * The same parts {@link #toString} names, as JSON for the explain
     * endpoint: {@code kind}, then {@code filter_sql},
     * {@code filter_substrait_bytes} (the length of the Substrait
     * encoding the scans evaluate, present only when the planner chose
     * it), {@code lance_clause}
     * (the clause's query name), {@code top_k} ({@code orderings} with
     * {@code column} / {@code ascending} / {@code nulls_first},
     * {@code fetch}, {@code cursor_sql}), {@code aggregate}
     * ({@code group_count}, {@code metrics} with {@code name} /
     * {@code kind}, {@code substrait_bytes}) and
     * {@code excluded_fragment_ids} (the pruned fragments, ascending),
     * each present only when set. The Substrait bytes themselves are
     * not rendered, only their length.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("kind", kind.name());
        if (filterSql != null) {
            builder.field("filter_sql", filterSql);
        }
        if (filterSubstrait != null) {
            builder.field("filter_substrait_bytes", filterSubstrait.length);
        }
        if (lanceClause != null) {
            builder.field("lance_clause", lanceClause.getWriteableName());
        }
        if (topK != null) {
            builder.startObject("top_k");
            builder.startArray("orderings");
            for (ScanOrdering ordering : topK.orderings()) {
                builder.startObject();
                builder.field("column", ordering.column());
                builder.field("ascending", ordering.ascending());
                builder.field("nulls_first", ordering.nullsFirst());
                builder.endObject();
            }
            builder.endArray();
            builder.field("fetch", topK.fetch());
            if (topK.cursorSql() != null) {
                builder.field("cursor_sql", topK.cursorSql());
            }
            builder.endObject();
        }
        if (aggregate != null) {
            builder.startObject("aggregate");
            builder.field("group_count", aggregate.groupCount());
            builder.startArray("metrics");
            for (MetricSlot slot : aggregate.metrics()) {
                builder.startObject();
                builder.field("name", slot.name());
                builder.field("kind", slot.kind().name());
                builder.endObject();
            }
            builder.endArray();
            builder.field("substrait_bytes", aggregate.substrait.length);
            builder.endObject();
        }
        if (excludedFragmentIds.length > 0) {
            builder.array("excluded_fragment_ids", excludedFragmentIds);
        }
        return builder.endObject();
    }
}
