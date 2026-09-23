/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.index.query.QueryBuilder;

import java.util.List;
import java.util.Objects;

/**
 * The physical shape of one hit: which parts of the hits envelope the
 * response renders per row. The node sits at the top of a hits plan,
 * over the {@link LanceTopK} that orders and cuts the page, and marks
 * the plan as a hits request; the executor materialises the envelope
 * through the fragment readers' stored fields path whatever the plan
 * below looks like, so today the node carries the desired output
 * rather than driving a projection (projection pushdown is later
 * work).
 *
 * <p>{@code outputColumns} names the table columns {@code _source}
 * renders, in schema order. The row type is the envelope itself, one
 * synthetic column per rendered part: {@code _id}, {@code _source},
 * {@code _score} and {@code _sort} (the per-hit sort values), each
 * present when its flag is on.
 *
 * <p>{@code postFilter} is the request's {@code post_filter}, a
 * predicate applied to the page after the query matched and outside
 * the aggregations' view; a page under a post filter cannot fold into
 * the Lance scan, because the scan would cut the page before the
 * filter narrows it. {@code from} is the number of leading hits the
 * coordinator skips; the top-k below already fetches {@code from}
 * plus the page, so the node only records it.
 *
 * <p>The node is logical and carries the trait defs' defaults. The
 * physical form that stands in for the whole hits plan (the scan with
 * the page pushed, or {@code HeapTopKExec}) declares
 * {@link org.opensearch.lance.plan.traits.Accuracy#EXACT} and the
 * {@link org.opensearch.lance.plan.traits.TieStability} of the
 * {@link LanceTopK} below ({@link LanceTopK#tieStability()}); the
 * envelope renders rows, it does not reorder them.
 */
public final class LanceHitShape extends SingleRel {

    private final List<String> outputColumns;
    private final boolean includeSource;
    private final boolean includeId;
    private final boolean includeScore;
    private final boolean includeSortValues;
    private final QueryBuilder postFilter;
    private final int from;

    /**
     * @param outputColumns the table columns {@code _source} renders
     * @param includeSource whether hits carry {@code _source}
     * @param includeId whether hits carry {@code _id}
     * @param includeScore whether hits carry a numeric {@code _score}
     * @param includeSortValues whether hits carry their sort values
     */
    public LanceHitShape(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        List<String> outputColumns,
        boolean includeSource,
        boolean includeId,
        boolean includeScore,
        boolean includeSortValues
    ) {
        this(cluster, traitSet, input, outputColumns, includeSource, includeId, includeScore, includeSortValues, null, 0);
    }

    /**
     * @param postFilter the request's {@code post_filter}, or null
     * @param from the leading hits the coordinator skips, 0 for the first page
     */
    public LanceHitShape(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        List<String> outputColumns,
        boolean includeSource,
        boolean includeId,
        boolean includeScore,
        boolean includeSortValues,
        QueryBuilder postFilter,
        int from
    ) {
        super(cluster, traitSet, input);
        this.outputColumns = List.copyOf(Objects.requireNonNull(outputColumns, "outputColumns"));
        this.includeSource = includeSource;
        this.includeId = includeId;
        this.includeScore = includeScore;
        this.includeSortValues = includeSortValues;
        this.postFilter = postFilter;
        if (from < 0) {
            throw new IllegalArgumentException("from must not be negative, got " + from);
        }
        this.from = from;
    }

    /** The table columns {@code _source} renders, in schema order. */
    public List<String> outputColumns() {
        return outputColumns;
    }

    /** Whether hits carry {@code _source}. */
    public boolean includeSource() {
        return includeSource;
    }

    /** Whether hits carry {@code _id}. */
    public boolean includeId() {
        return includeId;
    }

    /** Whether hits carry a numeric {@code _score}. */
    public boolean includeScore() {
        return includeScore;
    }

    /** Whether hits carry their sort values. */
    public boolean includeSortValues() {
        return includeSortValues;
    }

    /** The request's {@code post_filter}, or null when it carries none. */
    public QueryBuilder postFilter() {
        return postFilter;
    }

    /** The leading hits the coordinator skips; 0 for the first page. */
    public int from() {
        return from;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LanceHitShape(
            getCluster(),
            traitSet,
            sole(inputs),
            outputColumns,
            includeSource,
            includeId,
            includeScore,
            includeSortValues,
            postFilter,
            from
        );
    }

    /** One synthetic column per rendered part of the hits envelope. */
    @Override
    protected RelDataType deriveRowType() {
        RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        if (includeId) {
            builder.add("_id", typeFactory.createSqlType(SqlTypeName.VARCHAR));
        }
        if (includeSource) {
            builder.add("_source", typeFactory.createSqlType(SqlTypeName.VARBINARY));
        }
        if (includeScore) {
            builder.add("_score", typeFactory.createSqlType(SqlTypeName.REAL));
        }
        if (includeSortValues) {
            builder.add("_sort", typeFactory.createSqlType(SqlTypeName.ANY));
        }
        return builder.build();
    }

    /** Prints the columns and the flags (and the post filter and offset when set), so the digest and the explain output carry them. */
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("columns", outputColumns)
            .item("source", includeSource)
            .item("id", includeId)
            .item("score", includeScore)
            .item("sortValues", includeSortValues)
            .itemIf("postFilter", postFilter == null ? null : LanceFtsMatch.compactJson(postFilter), postFilter != null)
            .itemIf("from", from, from > 0);
    }
}
