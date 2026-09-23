/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.execute;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.lance.ipc.ColumnOrdering;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilder;
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
import org.opensearch.lance.plan.rel.physical.HeapTopKExec;
import org.opensearch.lance.plan.rel.physical.LuceneAggregateExec;
import org.opensearch.lance.query.LanceKnnQueryBuilder;

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
 * below a Lucene operator printed by {@link RexToLanceSql}) and
 * {@link #lanceClause()} the full text or knn clause whose Lucene
 * {@code Query} the executor builds through the mapping; both are null
 * when the query has no Lance spelling and the executor builds the
 * Lucene query from the request's own builder. The envelope part is
 * one of {@link #topK()} (an ordered, cut page the Lance scan returns)
 * or {@link #aggregate()} (a Substrait aggregate the Lance scan
 * computes), or neither when Lucene's collector and aggregators run.
 * {@link #kind()} names which physical root produced the plan.
 *
 * <p>The wire format is internal to the plugin and assumes every node
 * runs the same plugin version; there is no version negotiation.
 */
public final class FragmentPlan implements Writeable, ToXContentObject {

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
    private final QueryBuilder lanceClause;
    private final TopK topK;
    private final Aggregate aggregate;

    public FragmentPlan(Kind kind, String filterSql, QueryBuilder lanceClause, TopK topK, Aggregate aggregate) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.filterSql = filterSql;
        this.lanceClause = lanceClause;
        this.topK = topK;
        this.aggregate = aggregate;
        if (topK != null && aggregate != null) {
            throw new IllegalArgumentException("a plan carries a pushed page or a pushed aggregate, not both");
        }
        if (kind != Kind.PUSHED_SCAN && (topK != null || aggregate != null)) {
            throw new IllegalArgumentException("only a " + Kind.PUSHED_SCAN + " plan carries a pushed page or aggregate, got " + kind);
        }
    }

    public FragmentPlan(StreamInput in) throws IOException {
        this(
            Kind.read(in),
            in.readOptionalString(),
            in.readOptionalNamedWriteable(QueryBuilder.class),
            in.readOptionalWriteable(TopK::read),
            in.readOptionalWriteable(Aggregate::read)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(kind);
        out.writeOptionalString(filterSql);
        out.writeOptionalNamedWriteable(lanceClause);
        out.writeOptionalWriteable(topK);
        out.writeOptionalWriteable(aggregate);
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
     * the root and the scan: a {@code Filter} prints to Lance SQL, a
     * full text or knn node contributes its clause, a scan its pushed
     * operations. A {@code Filter} the printer cannot spell leaves the
     * whole query to the Lucene side (both parts null), because the
     * Lucene composition of the request's builder is the only form that
     * evaluates every clause.
     *
     * @param root the physical (or unlowered logical) per node plan
     * @param hasAggregations whether the request carries aggregations
     * @param hits whether the request asks for a page ({@code size} above 0)
     * @param inputs the cost inputs the planner chose {@code root} under,
     *     which a pushed aggregate's alternative cost is computed with
     */
    public static FragmentPlan of(RelNode root, boolean hasAggregations, boolean hits, CostInputs inputs) {
        Kind shapeKind = luceneKind(hasAggregations, hits);
        QueryPart query = queryPart(root);
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
                return new FragmentPlan(Kind.PUSHED_SCAN, query.filterSql(), query.lanceClause(), topK, null);
            }
            return new FragmentPlan(shapeKind, query.filterSql(), query.lanceClause(), null, null);
        }
        if (root instanceof LuceneAggregateExec) {
            return new FragmentPlan(Kind.LUCENE_AGGREGATE, query.filterSql(), query.lanceClause(), null, null);
        }
        if (root instanceof HeapTopKExec) {
            return new FragmentPlan(Kind.LUCENE_TOPK, query.filterSql(), query.lanceClause(), null, null);
        }
        return new FragmentPlan(shapeKind, query.filterSql(), query.lanceClause(), null, null);
    }

    /** The query part read off a plan chain. */
    private record QueryPart(String filterSql, QueryBuilder lanceClause) {
        static final QueryPart NONE = new QueryPart(null, null);
    }

    private static QueryPart queryPart(RelNode root) {
        String filterSql = null;
        QueryBuilder clause = null;
        RelNode node = root;
        while (node != null) {
            if (node instanceof LanceTableScan scan) {
                Optional<PushedAggregate> aggregate = scan.pushedAggregate();
                if (aggregate.isPresent()) {
                    return new QueryPart(aggregate.get().filterSql(), null);
                }
                Optional<PushedFts> fts = scan.pushedFts();
                if (fts.isPresent()) {
                    return new QueryPart(fts.get().filterSql(), fts.get().fts().ftsClause());
                }
                Optional<PushedKnn> knn = scan.pushedKnn();
                if (knn.isPresent()) {
                    return new QueryPart(knn.get().filterSql(), knn.get().knn().knnClause());
                }
                Optional<PushedFilter> filter = scan.pushedFilter();
                if (filter.isPresent()) {
                    return new QueryPart(filter.get().sql(), clause);
                }
                return new QueryPart(filterSql, clause);
            }
            if (node instanceof LuceneAggregateExec exec) {
                node = exec.aggregate();
            } else if (node instanceof HeapTopKExec exec) {
                node = exec.topK();
            } else if (node instanceof Filter filter) {
                Optional<String> sql = RexToLanceSql.print(filter.getCondition(), filter.getInput().getRowType());
                if (sql.isEmpty()) {
                    return QueryPart.NONE;
                }
                filterSql = sql.get();
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

    public Kind kind() {
        return kind;
    }

    /**
     * The Lance SQL of the scalar predicate: the whole query for a
     * scalar shape, the prefilter of a full text or knn shape, null when
     * the query has no Lance spelling or is {@code match_all}.
     */
    public String filterSql() {
        return filterSql;
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

    /** The same plan with the pushed aggregate dropped: the aggregators run over the same query. */
    public FragmentPlan withoutAggregate() {
        if (aggregate == null) {
            return this;
        }
        return new FragmentPlan(Kind.LUCENE_AGGREGATE, filterSql, lanceClause, null, null);
    }

    /** The same plan with the pushed page dropped: the collector cuts the page over the same query. */
    public FragmentPlan withoutTopK() {
        if (topK == null) {
            return this;
        }
        return new FragmentPlan(Kind.LUCENE_TOPK, filterSql, lanceClause, null, null);
    }

    /**
     * The same plan with the Lance clause and its prefilter dropped:
     * the executor builds the Lucene composition of the request's own
     * query builder instead. The kind stays, the pushed page or
     * aggregate must have been dropped before.
     */
    public FragmentPlan withoutLanceClause() {
        if (lanceClause == null) {
            return this;
        }
        return new FragmentPlan(kind, null, null, topK, aggregate);
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
            && Objects.equals(lanceClause, other.lanceClause)
            && Objects.equals(topK, other.topK)
            && Objects.equals(aggregate, other.aggregate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, filterSql, lanceClause, topK, aggregate);
    }

    /** Names the kind and every set part, for the executor's debug log. */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(kind.name());
        if (filterSql != null) {
            sb.append(" filter=").append(filterSql);
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
        return sb.toString();
    }

    /**
     * The same parts {@link #toString} names, as JSON for the explain
     * endpoint: {@code kind}, then {@code filter_sql}, {@code lance_clause}
     * (the clause's query name), {@code top_k} ({@code orderings} with
     * {@code column} / {@code ascending} / {@code nulls_first},
     * {@code fetch}, {@code cursor_sql}) and {@code aggregate}
     * ({@code group_count}, {@code metrics} with {@code name} /
     * {@code kind}, {@code substrait_bytes}), each present only when set.
     * The Substrait bytes themselves are not rendered, only their length.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("kind", kind.name());
        if (filterSql != null) {
            builder.field("filter_sql", filterSql);
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
        return builder.endObject();
    }
}
