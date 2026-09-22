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
 */
public final class LanceHitShape extends SingleRel {

    private final List<String> outputColumns;
    private final boolean includeSource;
    private final boolean includeId;
    private final boolean includeScore;
    private final boolean includeSortValues;

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
        super(cluster, traitSet, input);
        this.outputColumns = List.copyOf(Objects.requireNonNull(outputColumns, "outputColumns"));
        this.includeSource = includeSource;
        this.includeId = includeId;
        this.includeScore = includeScore;
        this.includeSortValues = includeSortValues;
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
            includeSortValues
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

    /** Prints the columns and the flags, so the digest and the explain output carry them. */
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("columns", outputColumns)
            .item("source", includeSource)
            .item("id", includeId)
            .item("score", includeScore)
            .item("sortValues", includeSortValues);
    }
}
