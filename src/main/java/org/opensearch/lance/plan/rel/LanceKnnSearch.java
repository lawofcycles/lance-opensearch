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
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.lance.query.LanceKnnQueryBuilder;

import java.util.List;
import java.util.Objects;

/**
 * Vector nearest-neighbour search over a Lance table: the logical form
 * of a top-level {@code lance_knn} clause. The input is the index's
 * scan, or a {@code Filter} over it carrying the clause's inner
 * {@code filter}; the fuse rule folds both shapes into the scan as a
 * pushed operation so the Lance dataset scan runs the nearest lookup
 * (with the filter as a Lance prefilter, evaluated before the top-k
 * cutoff, when it prints as SQL).
 *
 * <p>The node carries the parsed {@code lance_knn} builder with its
 * inner filter stripped (the {@code Filter} input carries that): the
 * builder holds the column, query vector, {@code k} and every search
 * knob ({@code nprobes}, {@code refine_factor}, {@code ef},
 * {@code metric}, {@code use_index}, boost), and the executor reuses
 * its mapping validation when it builds the Lucene-side query for the
 * pushed scan. The row type is the input's plus the {@code _distance}
 * column the nearest scan returns.
 *
 * <p>The node is logical and carries the trait defs' defaults; a page
 * cut in its distance order declares
 * {@link org.opensearch.lance.plan.traits.TieStability#UNSTABLE} on
 * whichever physical form cuts it (see {@link LanceTopK#tieStability()}),
 * because equal distances have no reproducible order out of the
 * nearest scan. Its figures are exact
 * ({@link org.opensearch.lance.plan.traits.Accuracy#EXACT}): the
 * distances are computed, not sketched.
 */
public final class LanceKnnSearch extends SingleRel {

    /** Name of the distance column the nearest scan adds to the row type. */
    public static final String DISTANCE_FIELD = "_distance";

    private final LanceKnnQueryBuilder knnClause;
    private final String queryJson;

    /**
     * @param knnClause the parsed {@code lance_knn} builder with its
     *     inner {@code filter} removed; the {@code Filter} input node
     *     carries the filter instead
     */
    public LanceKnnSearch(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, LanceKnnQueryBuilder knnClause) {
        super(cluster, traitSet, input);
        this.knnClause = Objects.requireNonNull(knnClause, "knnClause");
        if (knnClause.filter() != null) {
            throw new IllegalArgumentException("the knn clause's filter belongs on the Filter input, not inside the node");
        }
        this.queryJson = LanceFtsMatch.compactJson(knnClause);
    }

    /** The {@code lance_vector} column the nearest search runs on. */
    public String column() {
        return knnClause.field();
    }

    /** How many nearest rows the scan returns. */
    public int k() {
        return knnClause.k();
    }

    /** The parsed builder (filter stripped) the executor turns into the Lance nearest query. */
    public LanceKnnQueryBuilder knnClause() {
        return knnClause;
    }

    /** The clause as one-line JSON: every knn parameter, stable across instances. */
    public String queryJson() {
        return queryJson;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LanceKnnSearch(getCluster(), traitSet, sole(inputs), knnClause);
    }

    /** The input's row type plus the {@link #DISTANCE_FIELD} column the scan returns. */
    @Override
    protected RelDataType deriveRowType() {
        RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        builder.addAll(getInput().getRowType().getFieldList());
        builder.add(DISTANCE_FIELD, typeFactory.createSqlType(SqlTypeName.REAL));
        return builder.build();
    }

    /** The nearest scan returns at most {@code k} rows. */
    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return Math.min(knnClause.k(), super.estimateRowCount(mq));
    }

    /** Prints every parameter, so the digest and the explain output carry them. */
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("column", knnClause.field()).item("k", knnClause.k()).item("query", queryJson);
    }
}
