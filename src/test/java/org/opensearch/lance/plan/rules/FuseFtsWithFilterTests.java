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
import org.opensearch.lance.plan.rel.LanceFtsMatch;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.lance.query.LanceMatchQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * The FTS fuse rule: it fires on a bare {@link LanceFtsMatch} over a
 * scan and on the filtered shape when the filter prints as Lance SQL,
 * leaves the filtered shape in place when the predicate has no SQL
 * spelling, and declines when the scan already carries a pushed
 * operation.
 */
public class FuseFtsWithFilterTests extends OpenSearchTestCase {

    private RelBuilder builder;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        builder = PlanTestFixtures.factory().relBuilder(PlanTestFixtures.model().schema()).transform(config -> config.withSimplify(false));
        builder.scan(LancePlannerFactory.SCHEMA_NAME, "idx");
    }

    private LanceFtsMatch fts(RelNode input) {
        return new LanceFtsMatch(
            input.getCluster(),
            input.getCluster().traitSetOf(Convention.NONE),
            input,
            LanceFtsMatch.Kind.MATCH,
            List.of("body"),
            new LanceMatchQueryBuilder("body", "hello")
        );
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
        for (FuseFtsWithFilter rule : FuseFtsWithFilter.rules()) {
            program.addRuleInstance(rule);
        }
        HepPlanner planner = new HepPlanner(program.build());
        planner.setRoot(root);
        return planner.findBestExp();
    }

    public void testFiresOnABareFtsOverAScan() {
        LanceFtsMatch node = fts(builder.build());
        RelNode best = hep(node);
        LanceTableScan scan = (LanceTableScan) best;
        assertTrue(scan.pushedFts().isPresent());
        assertNull("a bare FTS carries no filter SQL", scan.pushedFts().orElseThrow().filterSql());
        assertEquals("the scan's row type gains the score column", node.getRowType(), scan.getRowType());
        assertEquals(LanceFtsMatch.SCORE_FIELD, scan.getRowType().getFieldList().get(scan.getRowType().getFieldCount() - 1).getName());
    }

    public void testFiresOnAFilteredFtsAndCarriesTheSql() {
        RexNode condition = printableCondition();
        LanceFtsMatch node = fts(LogicalFilter.create(builder.build(), condition));
        RelNode best = hep(node);
        LanceTableScan scan = (LanceTableScan) best;
        assertEquals("category = 'c0'", scan.pushedFts().orElseThrow().filterSql());
        assertEquals(node.getRowType(), scan.getRowType());
    }

    public void testDoesNotFireOnAnUnprintablePredicate() {
        RexNode condition = unprintableCondition();
        LanceFtsMatch node = fts(LogicalFilter.create(builder.build(), condition));
        RelNode best = hep(node);
        assertTrue("the FTS node must stay in place: " + best, best instanceof LanceFtsMatch);
        assertTrue(((LanceFtsMatch) best).getInput() instanceof LogicalFilter);
    }

    public void testDoesNotFireOnAScanAlreadyCarryingAPushedOperation() {
        RexNode condition = printableCondition();
        LanceTableScan pushed = ((LanceTableScan) builder.build()).withPushedFilter(condition, "category = 'c0'");
        RelNode best = hep(fts(pushed));
        assertTrue("the FTS node must stay over the pushed scan: " + best, best instanceof LanceFtsMatch);
    }
}
