/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rules;

import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * The filter pushdown rule: it fires on a printable predicate over a
 * bare scan (or a project over one), refuses an unprintable one, and
 * declines when the scan already carries a pushed operation. A filter
 * under a {@link org.opensearch.lance.plan.rel.LanceAggregate} is not a
 * shape here; {@link PushAggregateIntoLanceScan} matches that tree and
 * rebuilds the filter inside the pushed aggregate's input.
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
        HepProgramBuilder program = new HepProgramBuilder();
        for (PushFilterIntoLanceScan rule : PushFilterIntoLanceScan.rules()) {
            program.addRuleInstance(rule);
        }
        HepPlanner planner = new HepPlanner(program.build());
        planner.setRoot(root);
        return planner.findBestExp();
    }

    public void testFiresOnAFilterOverAScan() {
        RexNode condition = printableCondition();
        RelNode best = hep(LogicalFilter.create(builder.build(), condition));
        LanceTableScan scan = (LanceTableScan) best;
        assertEquals("category = 'c0'", scan.pushedFilter().orElseThrow().sql());
        assertEquals(condition, scan.pushedFilter().orElseThrow().condition());
    }

    public void testFiresOnAProjectOverTheFilterAndKeepsTheProject() {
        RexNode condition = printableCondition();
        builder.push(LogicalFilter.create(builder.build(), condition));
        builder.project(List.of(builder.field("category")), List.of("k"), true);
        RelNode best = hep(builder.build());
        assertTrue("the project stays the root: " + best, best instanceof Project);
        RelNode input = ((Project) best).getInput();
        assertTrue(input instanceof LanceTableScan);
        assertEquals("category = 'c0'", ((LanceTableScan) input).pushedFilter().orElseThrow().sql());
    }

    public void testDoesNotFireOnAnUnprintablePredicate() {
        RexNode condition = unprintableCondition();
        RelNode best = hep(LogicalFilter.create(builder.build(), condition));
        assertTrue("the filter must stay in place: " + best, best instanceof Filter);
        assertTrue(((Filter) best).getInput() instanceof LanceTableScan);
    }
}
