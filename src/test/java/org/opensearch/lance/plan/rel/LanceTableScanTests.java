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
import org.apache.calcite.rex.RexNode;
import org.opensearch.lance.plan.calcite.LanceConvention;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchema;
import org.opensearch.lance.plan.calcite.LanceTable;
import org.opensearch.lance.plan.substrait.LanceSubstraitProducer;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.lance.query.LanceKnnQueryBuilder;
import org.opensearch.lance.query.LanceMatchQueryBuilder;
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

    public void testPushedFtsChangesRowTypeCostAndDigest() {
        LanceTableScan bare = (LanceTableScan) PlanTestFixtures.factory()
            .relBuilder(PlanTestFixtures.model().schema())
            .scan(LancePlannerFactory.SCHEMA_NAME, "idx")
            .build();
        LanceFtsMatch fts = new LanceFtsMatch(
            bare.getCluster(),
            bare.getCluster().traitSetOf(org.apache.calcite.plan.Convention.NONE),
            bare,
            LanceFtsMatch.Kind.MATCH,
            List.of("body"),
            new LanceMatchQueryBuilder("body", "hello")
        );
        LanceTableScan pushed = bare.withPushedFts(fts, "category = 'c0'");

        assertEquals(fts.getRowType(), pushed.getRowType());
        assertEquals(LanceFtsMatch.SCORE_FIELD, pushed.getRowType().getFieldList().get(pushed.getRowType().getFieldCount() - 1).getName());
        assertTrue(pushed.pushedFts().isPresent());
        assertEquals("category = 'c0'", pushed.pushedFts().orElseThrow().filterSql());
        assertNotEquals("the pushed operation is part of the digest", bare.getDigest(), pushed.getDigest());
        assertTrue("the digest names the FTS parameters: " + pushed.getDigest(), pushed.getDigest().contains("lance_match"));

        RelOptCost cost = pushed.computeSelfCost(pushed.getCluster().getPlanner(), pushed.getCluster().getMetadataQuery());
        assertEquals("one pushed operation costs one constant", 1.0, cost.getCpu(), 0.0);

        RelNode copy = pushed.copy(pushed.getTraitSet(), List.of());
        assertTrue(((LanceTableScan) copy).pushedFts().isPresent());
        assertEquals(pushed.getRowType(), copy.getRowType());

        IllegalStateException second = expectThrows(IllegalStateException.class, () -> pushed.withPushedFts(fts, null));
        assertTrue(second.getMessage().contains("already carries"));
    }

    public void testPushedKnnChangesRowTypeCostAndDigest() {
        LanceTableScan bare = (LanceTableScan) PlanTestFixtures.factory()
            .relBuilder(PlanTestFixtures.model().schema())
            .scan(LancePlannerFactory.SCHEMA_NAME, "idx")
            .build();
        LanceKnnSearch knn = new LanceKnnSearch(
            bare.getCluster(),
            bare.getCluster().traitSetOf(org.apache.calcite.plan.Convention.NONE),
            bare,
            new LanceKnnQueryBuilder("embedding", new float[] { 0.1f, 0.2f }, 3)
        );
        LanceTableScan pushed = bare.withPushedKnn(knn, "rating = 5");

        assertEquals(knn.getRowType(), pushed.getRowType());
        assertEquals(
            LanceKnnSearch.DISTANCE_FIELD,
            pushed.getRowType().getFieldList().get(pushed.getRowType().getFieldCount() - 1).getName()
        );
        assertTrue(pushed.pushedKnn().isPresent());
        assertEquals("rating = 5", pushed.pushedKnn().orElseThrow().filterSql());
        assertNotEquals("the pushed operation is part of the digest", bare.getDigest(), pushed.getDigest());
        assertEquals("a pushed knn returns at most k rows", 3.0, pushed.estimateRowCount(pushed.getCluster().getMetadataQuery()), 0.0);

        RelNode copy = pushed.copy(pushed.getTraitSet(), List.of());
        assertTrue(((LanceTableScan) copy).pushedKnn().isPresent());
        assertEquals(pushed.getRowType(), copy.getRowType());

        IllegalStateException second = expectThrows(IllegalStateException.class, () -> pushed.withPushedKnn(knn, null));
        assertTrue(second.getMessage().contains("already carries"));
    }

    public void testWithPushedFilterRefusesAScanCarryingAnotherPush() throws Exception {
        // Aggregate-carrying scan refuses a filter push.
        RelNode logical = PlanTestFixtures.translate(
            PlanTestFixtures.parse("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"price\"}}}}")
        );
        LanceAggregate aggregate = (LanceAggregate) logical;
        LanceTableScan bare = (LanceTableScan) aggregate.getInput();
        LanceTableScan aggregatePushed = bare.withPushedAggregate(
            aggregate,
            LanceSubstraitProducer.toLanceAggregate(aggregate).orElseThrow()
        );
        RexNode condition = bare.getCluster()
            .getRexBuilder()
            .makeCall(
                org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_NULL,
                bare.getCluster().getRexBuilder().makeInputRef(bare.getRowType().getFieldList().get(1).getType(), 1)
            );
        IllegalStateException overAggregate = expectThrows(
            IllegalStateException.class,
            () -> aggregatePushed.withPushedFilter(condition, "rating IS NOT NULL")
        );
        assertTrue(overAggregate.getMessage(), overAggregate.getMessage().contains("already carries a pushed operation"));

        // FTS-carrying scan refuses a filter push.
        LanceTableScan bareFts = (LanceTableScan) PlanTestFixtures.factory()
            .relBuilder(PlanTestFixtures.model().schema())
            .scan(LancePlannerFactory.SCHEMA_NAME, "idx")
            .build();
        LanceFtsMatch fts = new LanceFtsMatch(
            bareFts.getCluster(),
            bareFts.getCluster().traitSetOf(org.apache.calcite.plan.Convention.NONE),
            bareFts,
            LanceFtsMatch.Kind.MATCH,
            List.of("body"),
            new LanceMatchQueryBuilder("body", "hello")
        );
        LanceTableScan ftsPushed = bareFts.withPushedFts(fts, null);
        RexNode ftsCondition = bareFts.getCluster()
            .getRexBuilder()
            .makeCall(
                org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_NULL,
                bareFts.getCluster().getRexBuilder().makeInputRef(bareFts.getRowType().getFieldList().get(1).getType(), 1)
            );
        IllegalStateException overFts = expectThrows(
            IllegalStateException.class,
            () -> ftsPushed.withPushedFilter(ftsCondition, "rating IS NOT NULL")
        );
        assertTrue(overFts.getMessage(), overFts.getMessage().contains("already carries a pushed operation"));

        // KNN-carrying scan refuses a filter push.
        LanceTableScan bareKnn = (LanceTableScan) PlanTestFixtures.factory()
            .relBuilder(PlanTestFixtures.model().schema())
            .scan(LancePlannerFactory.SCHEMA_NAME, "idx")
            .build();
        LanceKnnSearch knn = new LanceKnnSearch(
            bareKnn.getCluster(),
            bareKnn.getCluster().traitSetOf(org.apache.calcite.plan.Convention.NONE),
            bareKnn,
            new LanceKnnQueryBuilder("embedding", new float[] { 0.1f, 0.2f }, 3)
        );
        LanceTableScan knnPushed = bareKnn.withPushedKnn(knn, null);
        RexNode knnCondition = bareKnn.getCluster()
            .getRexBuilder()
            .makeCall(
                org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_NULL,
                bareKnn.getCluster().getRexBuilder().makeInputRef(bareKnn.getRowType().getFieldList().get(1).getType(), 1)
            );
        IllegalStateException overKnn = expectThrows(
            IllegalStateException.class,
            () -> knnPushed.withPushedFilter(knnCondition, "rating IS NOT NULL")
        );
        assertTrue(overKnn.getMessage(), overKnn.getMessage().contains("already carries a pushed operation"));
    }

    public void testWithPushedAggregateRefusesAScanCarryingAnotherPush() throws Exception {
        RelNode logical = PlanTestFixtures.translate(
            PlanTestFixtures.parse("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"price\"}}}}")
        );
        LanceAggregate aggregate = (LanceAggregate) logical;
        LanceTableScan bare = (LanceTableScan) aggregate.getInput();
        java.nio.ByteBuffer bytes = LanceSubstraitProducer.toLanceAggregate(aggregate).orElseThrow();

        // Filter-carrying scan refuses an aggregate push.
        RexNode condition = bare.getCluster()
            .getRexBuilder()
            .makeCall(
                org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_NULL,
                bare.getCluster().getRexBuilder().makeInputRef(bare.getRowType().getFieldList().get(1).getType(), 1)
            );
        LanceTableScan filterPushed = bare.withPushedFilter(condition, "rating IS NOT NULL");
        IllegalStateException overFilter = expectThrows(
            IllegalStateException.class,
            () -> filterPushed.withPushedAggregate(aggregate, bytes)
        );
        assertTrue(overFilter.getMessage(), overFilter.getMessage().contains("already carries a pushed operation"));

        // FTS-carrying scan refuses an aggregate push.
        LanceTableScan bareFts = (LanceTableScan) PlanTestFixtures.factory()
            .relBuilder(PlanTestFixtures.model().schema())
            .scan(LancePlannerFactory.SCHEMA_NAME, "idx")
            .build();
        LanceFtsMatch fts = new LanceFtsMatch(
            bareFts.getCluster(),
            bareFts.getCluster().traitSetOf(org.apache.calcite.plan.Convention.NONE),
            bareFts,
            LanceFtsMatch.Kind.MATCH,
            List.of("body"),
            new LanceMatchQueryBuilder("body", "hello")
        );
        LanceTableScan ftsPushed = bareFts.withPushedFts(fts, null);
        IllegalStateException overFts = expectThrows(IllegalStateException.class, () -> ftsPushed.withPushedAggregate(aggregate, bytes));
        assertTrue(overFts.getMessage(), overFts.getMessage().contains("already carries a pushed operation"));

        // KNN-carrying scan refuses an aggregate push.
        LanceTableScan bareKnn = (LanceTableScan) PlanTestFixtures.factory()
            .relBuilder(PlanTestFixtures.model().schema())
            .scan(LancePlannerFactory.SCHEMA_NAME, "idx")
            .build();
        LanceKnnSearch knn = new LanceKnnSearch(
            bareKnn.getCluster(),
            bareKnn.getCluster().traitSetOf(org.apache.calcite.plan.Convention.NONE),
            bareKnn,
            new LanceKnnQueryBuilder("embedding", new float[] { 0.1f, 0.2f }, 3)
        );
        LanceTableScan knnPushed = bareKnn.withPushedKnn(knn, null);
        IllegalStateException overKnn = expectThrows(IllegalStateException.class, () -> knnPushed.withPushedAggregate(aggregate, bytes));
        assertTrue(overKnn.getMessage(), overKnn.getMessage().contains("already carries a pushed operation"));
    }
}
