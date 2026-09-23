/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rel.physical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.lance.plan.calcite.ShardPathConvention;
import org.opensearch.lance.plan.calcite.ShardPathRel;
import org.opensearch.lance.plan.rel.ShardPathReason;
import org.opensearch.lance.plan.traits.Accuracy;
import org.opensearch.lance.plan.traits.TieStability;

import java.util.List;
import java.util.Objects;

/**
 * A search request answered through OpenSearch's regular shard search
 * path: the request carries at least one element the fragment fan-out
 * does not serve, each named by a {@link ShardPathReason}. The node
 * takes the index's bare scan as its input, like the Lucene fallback
 * operators do, and its presence at the plan root is the routing
 * decision itself: the dispatch filter forwards the whole
 * {@code SearchRequest} to the standard {@code TransportSearchAction}
 * per request, leaving the planner's tree, so no executor traverses
 * below this node.
 *
 * <p>The row type is the standard search response envelope the shard
 * path answers with, one synthetic column for the hits block and one
 * for the aggregations block, matching the logical
 * {@link org.opensearch.lance.plan.rel.LanceShardPathShape} it stands
 * in for. The node declares {@link Accuracy#EXACT} and
 * {@link TieStability#STABLE_ROWADDR}: the shard path answers what
 * OpenSearch answers over the whole table reader, exact figures in
 * Lucene doc order, which is the table's row address order.
 */
public final class ShardPathFallbackExec extends SingleRel implements ShardPathRel {

    private final List<ShardPathReason> reasons;

    /**
     * @param traitSet must carry {@link ShardPathConvention#INSTANCE};
     *     the accuracy and tie stability are replaced by the node's own
     * @param input the index's bare scan
     * @param reasons the request elements only the shard path serves;
     *     never empty
     */
    public ShardPathFallbackExec(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<ShardPathReason> reasons) {
        super(cluster, traitSet.plus(Accuracy.EXACT).plus(TieStability.STABLE_ROWADDR), input);
        this.reasons = List.copyOf(Objects.requireNonNull(reasons, "reasons"));
        if (this.reasons.isEmpty()) {
            throw new IllegalArgumentException("a shard path fallback needs at least one reason");
        }
    }

    /** The request elements only the shard path serves. */
    public List<ShardPathReason> reasons() {
        return reasons;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new ShardPathFallbackExec(getCluster(), traitSet, sole(inputs), reasons);
    }

    /** One synthetic column for the hits block and one for the aggregations block. */
    @Override
    protected RelDataType deriveRowType() {
        RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        return typeFactory.builder()
            .add("_hits", typeFactory.createSqlType(SqlTypeName.ANY))
            .add("_aggregations", typeFactory.createSqlType(SqlTypeName.ANY))
            .build();
    }

    /**
     * A constant until the cost model gets real coefficients, pinned
     * well above the {@code LuceneAggregateExec} / {@code HeapTopKExec}
     * constant (the tiny cost plus one unit in every slot) and above
     * the zero cost of {@link LuceneHandoffExec}: the shard path runs
     * the whole request through one node's shard reader, so whenever a
     * Lance pushdown or a Lucene fallback of the same tree exists, the
     * planner must prefer it.
     */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        RelOptCostFactory factory = planner.getCostFactory();
        return factory.makeTinyCost().plus(factory.makeCost(100, 100, 100));
    }

    /** Prints the reasons, so two fallbacks with different reasons never share a digest. */
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("reasons", reasons);
    }
}
