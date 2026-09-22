/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rules;

import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.rel.BucketSpec;
import org.opensearch.lance.plan.rel.LanceAggregate;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.MetricSpec;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * The filter pushdown rule: it fires on a printable predicate and
 * refuses an unprintable one, and it composes with the aggregate
 * pushdown through the planner's memo, so a filtered aggregation plans
 * as one scan carrying both pushed operations while an unprintable
 * filter rides inside the pushed aggregate's rebuilt input.
 */
public class PushFilterIntoLanceScanTests extends OpenSearchTestCase {

    private RelBuilder builder;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        builder = PlanTestFixtures.factory().relBuilder(PlanTestFixtures.model().schema()).transform(config -> config.withSimplify(false));
        builder.scan(LancePlannerFactory.SCHEMA_NAME, "idx");
    }

    private RexNode printableCondition() {
        return builder.call(SqlStdOperatorTable.EQUALS, builder.field("category"), builder.literal("c0"));
    }

    private RexNode unprintableCondition() {
        return builder.call(
            SqlStdOperatorTable.EQUALS,
            builder.call(SqlStdOperatorTable.PLUS, builder.field("rating"), builder.literal(1)),
            builder.literal(2)
        );
    }

    private static RelNode hep(RelNode root) {
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().addRuleInstance(PushFilterIntoLanceScan.rules().get(0)).build());
        planner.setRoot(root);
        return planner.findBestExp();
    }

    private LanceAggregate countOver(RelNode input) {
        AggregateCall count = AggregateCall.create(SqlStdOperatorTable.COUNT, false, false, List.of(0), -1, 0, input, null, "m0");
        return LanceAggregate.create(
            input,
            ImmutableBitSet.of(),
            List.of(count),
            List.of(),
            List.of(MetricSpec.of(MetricSpec.Kind.VALUE_COUNT, "c")),
            List.of()
        );
    }

    public void testFiresOnAPrintablePredicate() {
        RexNode condition = printableCondition();
        RelNode best = hep(LogicalFilter.create(builder.build(), condition));
        LanceTableScan scan = (LanceTableScan) best;
        assertEquals("category = 'c0'", scan.pushedFilter().orElseThrow().sql());
        assertEquals(condition, scan.pushedFilter().orElseThrow().condition());
    }

    public void testDoesNotFireOnAnUnprintablePredicate() {
        RexNode condition = unprintableCondition();
        RelNode best = hep(LogicalFilter.create(builder.build(), condition));
        assertTrue("the filter must stay in place: " + best, best instanceof Filter);
        assertTrue(((Filter) best).getInput() instanceof LanceTableScan);
    }

    public void testFilteredMetricAggregatePlansAsOneScanWithBothOperations() {
        RexNode condition = printableCondition();
        RelNode filtered = LogicalFilter.create(builder.build(), condition);
        RelNode physical = PushAggregateIntoLanceScanTests.volcanoPlan(countOver(filtered));
        LanceTableScan scan = (LanceTableScan) physical;
        assertEquals("category = 'c0'", scan.pushedFilter().orElseThrow().sql());
        assertTrue("the aggregate lands on the filtered scan: " + physical, scan.pushedAggregate().isPresent());
    }

    public void testFilteredBucketAggregatePlansAsOneScanWithBothOperations() {
        RexNode condition = printableCondition();
        builder.push(LogicalFilter.create(builder.build(), condition));
        builder.project(List.of(builder.field("category"), builder.field("rating")), List.of("k", "v"), true);
        RelNode projected = builder.build();
        AggregateCall count = AggregateCall.create(SqlStdOperatorTable.COUNT, false, false, List.of(1), -1, 1, projected, null, "m0");
        LanceAggregate aggregate = LanceAggregate.create(
            projected,
            ImmutableBitSet.of(0),
            List.of(count),
            List.of(BucketSpec.of(BucketSpec.Kind.TERMS, "t")),
            List.of(MetricSpec.of(MetricSpec.Kind.VALUE_COUNT, "c")),
            List.of(List.of())
        );
        RelNode physical = PushAggregateIntoLanceScanTests.volcanoPlan(aggregate);
        LanceTableScan scan = (LanceTableScan) physical;
        assertEquals("category = 'c0'", scan.pushedFilter().orElseThrow().sql());
        assertTrue("the aggregate lands on the filtered scan: " + physical, scan.pushedAggregate().isPresent());
    }

    public void testUnprintableFilterRidesInsideThePushedAggregate() {
        RexNode condition = unprintableCondition();
        RelNode filtered = LogicalFilter.create(builder.build(), condition);
        RelNode physical = PushAggregateIntoLanceScanTests.volcanoPlan(countOver(filtered));
        LanceTableScan scan = (LanceTableScan) physical;
        assertTrue("the filter is not pushed: " + physical, scan.pushedFilter().isEmpty());
        assertTrue("the aggregate carries the filter in its input: " + physical, scan.pushedAggregate().isPresent());
        assertTrue("the rebuilt input keeps the filter", scan.pushedAggregate().orElseThrow().aggregate().getInput() instanceof Filter);
    }
}
