/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.calcite;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.lance.plan.rel.LanceAggregate;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

/**
 * Pins what the builder produces over a Lance backed table: the scan node
 * class, its convention, and the plan strings the translator will produce
 * for scan, scan with filter, and scan with an aggregate.
 */
public class LancePlannerFactoryTests extends OpenSearchTestCase {

    private static final Schema TWO_COLUMNS = new Schema(
        List.of(
            field("id", new ArrowType.Int(32, true), false),
            field("price", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), true)
        )
    );

    private static Field field(String name, ArrowType arrowType, boolean nullable) {
        return new Field(name, new FieldType(nullable, arrowType, null), null);
    }

    private RelBuilder relBuilder() {
        LancePlannerFactory factory = new LancePlannerFactory(1L << 30, 1L << 30);
        LanceSchema schema = new LanceSchema(Map.of("t", new LanceTable("t", TWO_COLUMNS, () -> 512L)));
        return factory.relBuilder(schema);
    }

    public void testScanYieldsLanceTableScan() {
        RelNode scan = relBuilder().scan("lance", "t").build();
        assertTrue(scan instanceof LanceTableScan);
        assertSame(LanceConvention.INSTANCE, scan.getConvention());
        assertEquals("LanceTableScan(table=[[lance, t]])\n", RelOptUtil.toString(scan));
    }

    public void testScanRowCountFromStatistic() {
        RelNode scan = relBuilder().scan("lance", "t").build();
        assertEquals(512.0, scan.estimateRowCount(scan.getCluster().getMetadataQuery()), 0.0);
    }

    public void testFilterOverScan() {
        RelBuilder builder = relBuilder();
        RelNode rel = builder.scan("lance", "t").filter(builder.equals(builder.field("id"), builder.literal(42))).build();
        assertEquals("LogicalFilter(condition=[=($0, 42)])\n  LanceTableScan(table=[[lance, t]])\n", RelOptUtil.toString(rel));
    }

    public void testAggregateOverScan() {
        RelBuilder builder = relBuilder();
        RelNode rel = builder.scan("lance", "t").aggregate(builder.groupKey(), builder.count(false, "cnt")).build();
        assertEquals("LogicalAggregate(group=[{}], cnt=[COUNT()])\n  LanceTableScan(table=[[lance, t]])\n", RelOptUtil.toString(rel));
    }

    public void testHepPlannerRunsEmptyProgramUnchanged() {
        RelNode scan = relBuilder().scan("lance", "t").build();
        HepPlanner hepPlanner = new LancePlannerFactory(1L << 30, 1L << 30).newHepPlanner();
        hepPlanner.setRoot(scan);
        assertSame(scan, hepPlanner.findBestExp());
    }

    public void testPlanKeepsTheLogicalRootWhenARuleThrows() throws Exception {
        // The fragment routing promises a Lucene aggregator fallback for
        // every plan it does not push, so a planner failure has to come
        // back as the logical root, not as an exception.
        RelNode logical = PlanTestFixtures.translate(
            PlanTestFixtures.parse("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}")
        );
        RelOptPlanner planner = logical.getCluster().getPlanner();
        planner.addRule(new RelOptRule(RelOptRule.operand(LanceAggregate.class, RelOptRule.any()), "ThrowingRule") {
            @Override
            public void onMatch(RelOptRuleCall call) {
                throw new IllegalStateException("boom");
            }
        });
        assertSame(logical, PlanTestFixtures.factory().plan(logical));
    }
}
