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
import org.opensearch.lance.plan.rel.PushedOperation.PushedFilter;
import org.opensearch.lance.plan.rules.PushFilterIntoLanceScan.Encoding;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * The filter pushdown rules: the SQL rule fires on a printable predicate
 * over a bare scan (or a project over one) and refuses an unprintable
 * one; the Substrait rule fires on every predicate the producer
 * encodes, carrying the SQL next to the bytes when the printer can
 * spell it, and refuses what the producer refuses; both decline when the
 * scan already carries a pushed operation. A filter under a
 * {@link org.opensearch.lance.plan.rel.LanceAggregate} is not a shape
 * here; {@link PushAggregateIntoLanceScan} matches that tree and
 * rebuilds the filter inside the pushed aggregate's input. Which of the
 * two pushed forms the Volcano planner keeps is
 * {@code PlannerConventionChoiceTests}' concern.
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

    /** Arithmetic: outside the SQL printer's vocabulary, inside the Substrait producer's. */
    private RexNode unprintableCondition() {
        return builder.call(
            SqlStdOperatorTable.EQUALS,
            builder.call(SqlStdOperatorTable.PLUS, builder.field("rating"), builder.literal(1)),
            builder.literal(2)
        );
    }

    /** A struct child reference: the printer spells the dotted path, the producer refuses it. */
    private static RelNode structChildFilter() {
        RelBuilder nested = PlanTestFixtures.factory()
            .relBuilder(PlanTestFixtures.queryModel().schema())
            .transform(config -> config.withSimplify(false));
        nested.scan(LancePlannerFactory.SCHEMA_NAME, "idx");
        RexNode child = nested.getRexBuilder().makeFieldAccess(nested.field("meta"), "region", true);
        RexNode condition = nested.call(SqlStdOperatorTable.EQUALS, child, nested.literal("eu"));
        return LogicalFilter.create(nested.build(), condition);
    }

    private static RelNode hep(RelNode root, Encoding encoding) {
        HepProgramBuilder program = new HepProgramBuilder();
        for (PushFilterIntoLanceScan rule : PushFilterIntoLanceScan.rules(encoding)) {
            program.addRuleInstance(rule);
        }
        HepPlanner planner = new HepPlanner(program.build());
        planner.setRoot(root);
        return planner.findBestExp();
    }

    public void testRegistersOneRulePerEncodingAndShape() {
        assertEquals(4, PushFilterIntoLanceScan.rules().size());
        assertEquals(2, PushFilterIntoLanceScan.rules(Encoding.SQL).size());
        assertEquals(2, PushFilterIntoLanceScan.rules(Encoding.SUBSTRAIT).size());
    }

    public void testSqlRuleFiresOnAFilterOverAScan() {
        RexNode condition = printableCondition();
        RelNode best = hep(LogicalFilter.create(builder.build(), condition), Encoding.SQL);
        LanceTableScan scan = (LanceTableScan) best;
        PushedFilter pushed = scan.pushedFilter().orElseThrow();
        assertEquals("category = 'c0'", pushed.sql());
        assertFalse(pushed.usesSubstrait());
        assertEquals(condition, pushed.condition());
    }

    public void testSubstraitRuleFiresAndCarriesTheSql() {
        RexNode condition = printableCondition();
        RelNode best = hep(LogicalFilter.create(builder.build(), condition), Encoding.SUBSTRAIT);
        LanceTableScan scan = (LanceTableScan) best;
        PushedFilter pushed = scan.pushedFilter().orElseThrow();
        assertTrue(pushed.usesSubstrait());
        assertTrue(pushed.substraitLength() > 0);
        assertEquals("category = 'c0'", pushed.sql());
        assertEquals(condition, pushed.condition());
    }

    public void testTheTwoEncodingsHaveDifferentDigests() {
        RexNode condition = printableCondition();
        RelNode filter = LogicalFilter.create(builder.build(), condition);
        RelNode sql = hep(filter, Encoding.SQL);
        RelNode substrait = hep(filter, Encoding.SUBSTRAIT);
        assertNotEquals(sql.getDigest(), substrait.getDigest());
        assertTrue(substrait.getDigest().contains("substrait_bytes="));
    }

    public void testFiresOnAProjectOverTheFilterAndKeepsTheProject() {
        for (Encoding encoding : Encoding.values()) {
            setUp0();
            RexNode condition = printableCondition();
            builder.push(LogicalFilter.create(builder.build(), condition));
            builder.project(List.of(builder.field("category")), List.of("k"), true);
            RelNode best = hep(builder.build(), encoding);
            assertTrue("the project stays the root: " + best, best instanceof Project);
            RelNode input = ((Project) best).getInput();
            assertTrue(input instanceof LanceTableScan);
            PushedFilter pushed = ((LanceTableScan) input).pushedFilter().orElseThrow();
            assertEquals("category = 'c0'", pushed.sql());
            assertEquals(encoding == Encoding.SUBSTRAIT, pushed.usesSubstrait());
        }
    }

    private void setUp0() {
        builder = PlanTestFixtures.factory().relBuilder(PlanTestFixtures.model().schema()).transform(config -> config.withSimplify(false));
        builder.scan(LancePlannerFactory.SCHEMA_NAME, "idx");
    }

    public void testSqlRuleDoesNotFireOnAnUnprintablePredicate() {
        RexNode condition = unprintableCondition();
        RelNode best = hep(LogicalFilter.create(builder.build(), condition), Encoding.SQL);
        assertTrue("the filter must stay in place: " + best, best instanceof Filter);
        assertTrue(((Filter) best).getInput() instanceof LanceTableScan);
    }

    public void testSubstraitRuleFiresOnAnUnprintablePredicateWithoutSql() {
        RexNode condition = unprintableCondition();
        RelNode best = hep(LogicalFilter.create(builder.build(), condition), Encoding.SUBSTRAIT);
        LanceTableScan scan = (LanceTableScan) best;
        PushedFilter pushed = scan.pushedFilter().orElseThrow();
        assertTrue(pushed.usesSubstrait());
        assertNull("the printer has no spelling for arithmetic", pushed.sql());
    }

    public void testSubstraitRuleDoesNotFireOnAStructChildReference() {
        RelNode best = hep(structChildFilter(), Encoding.SUBSTRAIT);
        assertTrue("the filter must stay in place: " + best, best instanceof Filter);
        RelNode sql = hep(structChildFilter(), Encoding.SQL);
        assertEquals("meta.region = 'eu'", ((LanceTableScan) sql).pushedFilter().orElseThrow().sql());
    }
}
