/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.rel.LanceKnnSearch;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.lance.query.LanceKnnQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The knn fuse rule: it fires on a bare {@link LanceKnnSearch} over a
 * scan and on the filtered shape when the filter prints as Lance SQL,
 * and leaves the filtered shape in place when the predicate has no SQL
 * spelling (the executor answers that shape with a 400, keeping the
 * lance_knn filter contract).
 */
public class FuseKnnWithFilterTests extends OpenSearchTestCase {

    private RelBuilder builder;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        builder = PlanTestFixtures.factory().relBuilder(PlanTestFixtures.model().schema()).transform(config -> config.withSimplify(false));
        builder.scan(LancePlannerFactory.SCHEMA_NAME, "idx");
    }

    private LanceKnnSearch knn(RelNode input) {
        LanceKnnQueryBuilder clause = new LanceKnnQueryBuilder("embedding", new float[] { 0.1f, 0.2f }, 3).nprobes(10);
        return new LanceKnnSearch(input.getCluster(), input.getCluster().traitSetOf(Convention.NONE), input, clause);
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
        HepProgramBuilder program = new HepProgramBuilder();
        for (FuseKnnWithFilter rule : FuseKnnWithFilter.rules()) {
            program.addRuleInstance(rule);
        }
        HepPlanner planner = new HepPlanner(program.build());
        planner.setRoot(root);
        return planner.findBestExp();
    }

    public void testFiresOnABareKnnOverAScan() {
        LanceKnnSearch node = knn(builder.build());
        RelNode best = hep(node);
        LanceTableScan scan = (LanceTableScan) best;
        assertTrue(scan.pushedKnn().isPresent());
        assertNull("a bare knn carries no filter SQL", scan.pushedKnn().orElseThrow().filterSql());
        assertEquals("the scan's row type gains the distance column", node.getRowType(), scan.getRowType());
        assertEquals(LanceKnnSearch.DISTANCE_FIELD, scan.getRowType().getFieldList().get(scan.getRowType().getFieldCount() - 1).getName());
    }

    public void testFiresOnAFilteredKnnAndCarriesTheSql() {
        RexNode condition = printableCondition();
        LanceKnnSearch node = knn(LogicalFilter.create(builder.build(), condition));
        RelNode best = hep(node);
        LanceTableScan scan = (LanceTableScan) best;
        assertEquals("category = 'c0'", scan.pushedKnn().orElseThrow().filterSql());
        assertEquals(node.getRowType(), scan.getRowType());
    }

    public void testDoesNotFireOnAnUnprintablePredicate() {
        RexNode condition = unprintableCondition();
        LanceKnnSearch node = knn(LogicalFilter.create(builder.build(), condition));
        RelNode best = hep(node);
        assertTrue("the knn node must stay in place: " + best, best instanceof LanceKnnSearch);
        assertTrue(((LanceKnnSearch) best).getInput() instanceof LogicalFilter);
    }
}
