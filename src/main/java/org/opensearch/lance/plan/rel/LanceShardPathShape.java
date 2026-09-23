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
 * Logical marker for a search request whose shape only OpenSearch's
 * regular shard search path serves: the request carries at least one
 * element ({@code suggest}, {@code highlight}, or a pipeline
 * aggregation) the fragment fan-out
 * does not answer, each named by a {@link ShardPathReason}. The
 * translator's dispatch entry point wraps the index's scan with this
 * node when it detects such an element; the planner's
 * {@code PlanToShardPathRule} converts it to the
 * {@link org.opensearch.lance.plan.rel.physical.ShardPathFallbackExec}
 * physical operator, whose presence at the plan root is what routes
 * the request to the shard path.
 *
 * <p>The input is the index's bare scan: the query tree below plays no
 * part in the routing decision (the shard path re-parses the whole
 * request itself), so the node does not carry it.
 *
 * <p>The row type is the standard search response envelope the shard
 * path answers with, one synthetic column for the hits block and one
 * for the aggregations block; nothing consumes it today, it exists so
 * the logical and physical forms agree.
 */
public final class LanceShardPathShape extends SingleRel {

    private final List<ShardPathReason> reasons;

    /**
     * @param input the index's bare scan
     * @param reasons the request elements only the shard path serves;
     *     never empty (a request without one never builds this node)
     */
    public LanceShardPathShape(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<ShardPathReason> reasons) {
        super(cluster, traitSet, input);
        this.reasons = List.copyOf(Objects.requireNonNull(reasons, "reasons"));
        if (this.reasons.isEmpty()) {
            throw new IllegalArgumentException("a shard path shape needs at least one reason");
        }
    }

    /** The request elements only the shard path serves. */
    public List<ShardPathReason> reasons() {
        return reasons;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LanceShardPathShape(getCluster(), traitSet, sole(inputs), reasons);
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

    /** Prints the reasons, so two shapes with different reasons never share a digest. */
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("reasons", reasons);
    }
}
