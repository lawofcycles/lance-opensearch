/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rules;

import io.substrait.proto.AggregateRel;
import io.substrait.proto.Plan;
import io.substrait.proto.Rel;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.lance.plan.calcite.LanceConvention;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.rel.BucketSpec;
import org.opensearch.lance.plan.rel.LanceAggregate;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.MetricSpec;
import org.opensearch.lance.plan.rel.PushedOperation;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * The pushdown rule: it replaces the aggregate with the scan carrying
 * the producer's bytes for the three operand shapes, it leaves the
 * plan alone when the producer refuses an expression, and the Volcano
 * planner terminates either way.
 */
public class PushAggregateIntoLanceScanTests extends OpenSearchTestCase {

    /**
     * Runs the Volcano planner over {@code logical}; the pushdown rules
     * are registered by {@code LancePlannerFactory.newCluster}, which
     * built the cluster the tree lives in.
     */
    static RelNode volcanoPlan(RelNode logical) {
        VolcanoPlanner planner = (VolcanoPlanner) logical.getCluster().getPlanner();
        RelNode root = planner.changeTraits(logical, logical.getTraitSet().replace(LanceConvention.INSTANCE));
        planner.setRoot(root);
        return planner.findBestExp();
    }

    private static PushedOperation.PushedAggregate pushedRoot(RelNode physical) {
        assertTrue("physical root is the scan: " + physical, physical instanceof LanceTableScan);
        LanceTableScan scan = (LanceTableScan) physical;
        assertTrue("the scan carries the pushed aggregate", scan.pushedAggregate().isPresent());
        return scan.pushedAggregate().get();
    }

    private static AggregateRel decode(ByteBuffer buffer) throws Exception {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        Plan plan = Plan.parseFrom(bytes);
        assertEquals(1, plan.getRelationsCount());
        Rel input = plan.getRelations(0).getRoot().getInput();
        assertTrue("the bytes decode to an aggregate", input.hasAggregate());
        return input.getAggregate();
    }

    private static AggregateCall sumCall(RelNode input, String name, int argument) {
        return AggregateCall.create(SqlStdOperatorTable.SUM, false, false, List.of(argument), -1, 0, input, null, name);
    }

    public void testFiresOnAggregateOverScan() throws Exception {
        RelNode logical = PlanTestFixtures.translate(
            PlanTestFixtures.parse("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}")
        );
        RelNode physical = volcanoPlan(logical);
        PushedOperation.PushedAggregate pushed = pushedRoot(physical);
        AggregateRel rel = decode(pushed.substrait());
        // count(*) and the sum.
        assertEquals(2, rel.getMeasuresCount());
        assertEquals(0, rel.getGroupingsCount());
        assertEquals(logical.getRowType(), physical.getRowType());
    }

    public void testFiresOnAggregateOverProjectOverScan() throws Exception {
        RelNode logical = PlanTestFixtures.translate(
            PlanTestFixtures.parse(
                "{\"size\":0,\"aggs\":{\"t\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"a\":{\"avg\":{\"field\":\"rating\"}}}}}}"
            )
        );
        RelNode physical = volcanoPlan(logical);
        PushedOperation.PushedAggregate pushed = pushedRoot(physical);
        AggregateRel rel = decode(pushed.substrait());
        assertEquals(1, rel.getGroupingsCount());
        assertEquals(1, rel.getGroupings(0).getGroupingExpressionsCount());
        // count(*), the avg's sum and the avg's count.
        assertEquals(3, rel.getMeasuresCount());
        assertEquals(logical.getRowType(), physical.getRowType());
    }

    public void testFiresOnAggregateOverFilterOverScan() throws Exception {
        RelBuilder builder = PlanTestFixtures.factory().relBuilder(PlanTestFixtures.model().schema());
        builder.scan(LancePlannerFactory.SCHEMA_NAME, "idx");
        builder.filter(builder.call(SqlStdOperatorTable.GREATER_THAN, builder.field("rating"), builder.literal(5)));
        RelNode filtered = builder.build();
        LanceAggregate aggregate = LanceAggregate.create(
            filtered,
            ImmutableBitSet.of(),
            List.of(sumCall(filtered, "s", 1)),
            List.of(),
            List.of(MetricSpec.of(MetricSpec.Kind.SUM, "s")),
            List.of()
        );
        RelNode physical = volcanoPlan(aggregate);
        PushedOperation.PushedAggregate pushed = pushedRoot(physical);
        AggregateRel rel = decode(pushed.substrait());
        assertEquals(2, rel.getMeasuresCount());
    }

    public void testDoesNotFireWhenTheProducerRefuses() {
        // CEIL is outside the Lance consumer's vocabulary, so the
        // producer returns empty and no physical plan exists: the
        // planner terminates with a cannot-plan error instead of
        // looping or pushing a plan Lance cannot run.
        RelBuilder builder = PlanTestFixtures.factory().relBuilder(PlanTestFixtures.model().schema());
        builder.scan(LancePlannerFactory.SCHEMA_NAME, "idx");
        builder.project(
            List.of(builder.cast(builder.call(SqlStdOperatorTable.CEIL, builder.field("price")), SqlTypeName.BIGINT)),
            List.of("k"),
            true
        );
        RelNode projected = builder.build();
        LanceAggregate aggregate = LanceAggregate.create(
            projected,
            ImmutableBitSet.of(0),
            List.of(),
            List.of(BucketSpec.of(BucketSpec.Kind.TERMS, "k")),
            List.of(),
            List.of(List.of())
        );
        expectThrows(RelOptPlanner.CannotPlanException.class, () -> volcanoPlan(aggregate));
    }
}
