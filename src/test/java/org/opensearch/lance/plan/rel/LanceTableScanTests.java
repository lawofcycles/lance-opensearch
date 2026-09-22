/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rel;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.opensearch.lance.plan.calcite.LanceConvention;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchema;
import org.opensearch.lance.plan.calcite.LanceTable;
import org.opensearch.lance.plan.substrait.LanceSubstraitProducer;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

/**
 * The scan's self cost reads rows as the milliseconds proxy and models no
 * bytes yet, and {@code copy} keeps the node a {@link LanceTableScan} over
 * the same table.
 */
public class LanceTableScanTests extends OpenSearchTestCase {

    private static final Schema ONE_COLUMN = new Schema(
        List.of(new Field("id", new FieldType(false, new ArrowType.Int(32, true), null), null))
    );

    private LanceTableScan scan() {
        LancePlannerFactory factory = new LancePlannerFactory(1L << 30, 1L << 30);
        LanceSchema schema = new LanceSchema(Map.of("t", new LanceTable("t", ONE_COLUMN, () -> 100L)));
        return (LanceTableScan) factory.relBuilder(schema).scan("lance", "t").build();
    }

    public void testComputeSelfCostUsesRowsAsMillisProxy() {
        LanceTableScan scan = scan();
        RelOptCost cost = scan.computeSelfCost(scan.getCluster().getPlanner(), scan.getCluster().getMetadataQuery());
        assertNotNull(cost);
        assertEquals(100.0, cost.getRows(), 0.0);
        assertEquals(0.0, cost.getCpu(), 0.0);
        assertEquals(0.0, cost.getIo(), 0.0);
    }

    public void testCopyKeepsScan() {
        LanceTableScan scan = scan();
        RelNode copy = scan.copy(scan.getTraitSet(), List.of());
        assertTrue(copy instanceof LanceTableScan);
        assertSame(LanceConvention.INSTANCE, copy.getConvention());
        assertEquals(scan.getTable(), ((LanceTableScan) copy).getTable());
        assertEquals(scan.getRowType(), copy.getRowType());
    }

    public void testPushedAggregateChangesRowTypeCostAndDigest() throws Exception {
        RelNode logical = PlanTestFixtures.translate(
            PlanTestFixtures.parse(
                "{\"size\":0,\"aggs\":{\"t\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"a\":{\"avg\":{\"field\":\"rating\"}}}}}}"
            )
        );
        LanceAggregate aggregate = (LanceAggregate) logical;
        RelNode project = aggregate.getInput();
        LanceTableScan bare = (LanceTableScan) project.getInput(0);
        java.nio.ByteBuffer bytes = LanceSubstraitProducer.toLanceAggregate(aggregate).orElseThrow();
        LanceTableScan pushed = bare.withPushedAggregate(aggregate, bytes);

        assertEquals(aggregate.getRowType(), pushed.getRowType());
        assertTrue(pushed.pushedAggregate().isPresent());
        assertNotEquals("the pushed operations are part of the digest", bare.getDigest(), pushed.getDigest());

        RelOptCost bareCost = bare.computeSelfCost(bare.getCluster().getPlanner(), bare.getCluster().getMetadataQuery());
        RelOptCost pushedCost = pushed.computeSelfCost(pushed.getCluster().getPlanner(), pushed.getCluster().getMetadataQuery());
        assertTrue("groups cost less than rows: " + pushedCost + " vs " + bareCost, pushedCost.getRows() < bareCost.getRows());
        assertEquals("one pushed operation costs one constant", 1.0, pushedCost.getCpu(), 0.0);

        RelNode copy = pushed.copy(pushed.getTraitSet(), List.of());
        assertTrue(((LanceTableScan) copy).pushedAggregate().isPresent());
        assertEquals(pushed.getRowType(), copy.getRowType());

        IllegalStateException second = expectThrows(IllegalStateException.class, () -> pushed.withPushedAggregate(aggregate, bytes));
        assertTrue(second.getMessage().contains("already carries"));
    }
}
