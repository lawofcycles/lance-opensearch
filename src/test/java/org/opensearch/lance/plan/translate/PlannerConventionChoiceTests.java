/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.translate;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.cost.CostInputs;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.PushedOperation.PushedAggregate;
import org.opensearch.lance.plan.rel.PushedOperation.PushedTopK;
import org.opensearch.lance.plan.rel.physical.HeapTopKExec;
import org.opensearch.lance.plan.rel.physical.LuceneAggregateExec;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * Which convention {@code LancePlannerFactory.plan} answers a shape
 * with: a tree the pushdown rules fold arrives as the Lance scan
 * carrying the pushed operations even though the Lucene alternative is
 * enumerated next to it, a tree they cannot fold arrives as the Lucene
 * operator instead of falling back to the logical plan, and a tree
 * with no physical form under either convention still comes back as
 * the logical plan itself.
 */
public class PlannerConventionChoiceTests extends OpenSearchTestCase {

    private static RelNode plan(String body) throws IOException {
        RelNode logical = PlanTestFixtures.translate(PlanTestFixtures.parse(body));
        return PlanTestFixtures.factory().plan(logical);
    }

    public void testFoldableAggregateAnswersWithTheLanceScan() throws IOException {
        RelNode physical = plan(
            "{\"size\":0,\"aggs\":{\"by\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"a\":{\"avg\":{\"field\":\"rating\"}}}}}}"
        );
        assertTrue("the pushed scan wins the cost comparison: " + physical, physical instanceof LanceTableScan);
        assertTrue(((LanceTableScan) physical).pushedAggregate().isPresent());
    }

    public void testFilteredBucketTreeAnswersWithThePushedScan() throws IOException {
        // The key projection over the query filter is an operand of the
        // aggregate pushdown rule, so the whole tree folds into one
        // scan whose pushed aggregate carries the filter's SQL; this is
        // the plan the fragment executor runs for the same request.
        RelNode physical = plan(
            "{\"size\":0,\"query\":{\"term\":{\"category\":\"c0\"}},\"aggs\":{\"by\":{\"terms\":{\"field\":\"category\"}}}}"
        );
        assertTrue("the pushed scan wins the cost comparison: " + physical, physical instanceof LanceTableScan);
        PushedAggregate pushed = ((LanceTableScan) physical).pushedAggregate().orElseThrow();
        assertEquals("category = 'c0'", pushed.filterSql());
    }

    public void testFilteredCardinalityAnswersWithTheLuceneOperator() throws IOException {
        // The pushdown rule folds the cardinality like any other tree,
        // but the pushed scan's placeholder carries the cardinality
        // penalty on this small table, so the converter rule's
        // alternative wins the cost comparison; the filter stays inside
        // the wrapped logical tree for the Lucene side.
        RelNode physical = plan(
            "{\"size\":0,\"query\":{\"term\":{\"category\":\"c0\"}},\"aggs\":{\"u\":{\"cardinality\":{\"field\":\"rating\"}}}}"
        );
        assertTrue("the Lucene operator answers the shape: " + physical, physical instanceof LuceneAggregateExec);
        LuceneAggregateExec exec = (LuceneAggregateExec) physical;
        assertTrue("the operator runs over the bare scan", exec.getInput() instanceof LanceTableScan);
        assertTrue(((LanceTableScan) exec.getInput()).pushedOperations().isEmpty());
        assertTrue("the filter stays in the wrapped tree: " + exec.aggregate(), exec.aggregate().getInput() instanceof Filter);
    }

    public void testMetricOnlySumAnswersWithThePushedScan() throws IOException {
        RelNode physical = plan("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"price\"}}}}");
        assertTrue("the pushed scan wins the cost comparison: " + physical, physical instanceof LanceTableScan);
        assertTrue(((LanceTableScan) physical).pushedAggregate().isPresent());
    }

    public void testCardinalityAnswersWithTheLuceneOperator() throws IOException {
        // Both physical forms exist; the pushed scan of a tree with a
        // cardinality metric is priced above the Lucene operator (the
        // pushed form is slower than the aggregator), so the operator
        // wins.
        RelNode physical = plan("{\"size\":0,\"aggs\":{\"c\":{\"cardinality\":{\"field\":\"category\"}}}}");
        assertTrue("the Lucene operator answers the shape: " + physical, physical instanceof LuceneAggregateExec);
        LuceneAggregateExec exec = (LuceneAggregateExec) physical;
        assertTrue("the operator runs over the bare scan", exec.getInput() instanceof LanceTableScan);
        assertTrue(((LanceTableScan) exec.getInput()).pushedOperations().isEmpty());
    }

    public void testBucketTreeWithACardinalityChildAnswersWithTheLuceneOperator() throws IOException {
        // One cardinality anywhere in the tree keeps the whole tree on
        // the aggregators: the executor has no way to split one request
        // between the pushed scan and the aggregator machinery.
        RelNode physical = plan(
            "{\"size\":0,\"aggs\":{\"by\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"u\":{\"cardinality\":{\"field\":\"rating\"}}}}}}"
        );
        assertTrue("the Lucene operator answers the shape: " + physical, physical instanceof LuceneAggregateExec);
    }

    public void testEveryTranslatedShapeHasALuceneFormWhenThePushdownIsOff() throws IOException {
        // With lance.aggregation.pushdown off the pushed scan costs
        // infinity for every aggregate, so the planner must find the
        // Lucene operator for every tree the translator accepts; a shape
        // without one would come back as the logical plan, which the
        // executor could not run.
        CostInputs off = CostInputs.local().withPushdownEnabled(false);
        for (String fixture : AggregationToRelFixtureTests.FIXTURES) {
            RelNode logical = PlanTestFixtures.translate(PlanTestFixtures.parse(AggregationToRelFixtureTests.resource(fixture + ".json")));
            RelNode physical = PlanTestFixtures.factory().plan(logical, off);
            assertTrue(
                "[" + fixture + "] with the pushdown off answers with the Lucene operator: " + physical,
                physical instanceof LuceneAggregateExec
            );
            LuceneAggregateExec exec = (LuceneAggregateExec) physical;
            assertTrue("[" + fixture + "] runs over the bare scan", exec.getInput() instanceof LanceTableScan);
            assertTrue(((LanceTableScan) exec.getInput()).pushedOperations().isEmpty());
        }
    }

    public void testAGroupBoundBelowTheEstimateAnswersWithTheLuceneOperator() throws IOException {
        // A range with two ranges is two groups whatever the table, so a
        // bound of 1 sends it to the aggregators; a metric only tree
        // (one group) stays pushed. The fixture model has no statistics,
        // so a terms key's domain is a guess (Calcite's share of the
        // rows) and is not judged against the bound: terms stays pushed.
        CostInputs oneGroup = CostInputs.local().withMaxGroups(1L);
        RelNode range = planUnder(
            "{\"size\":0,\"aggs\":{\"r\":{\"range\":{\"field\":\"rating\",\"ranges\":[{\"to\":5},{\"from\":5}]}}}}",
            oneGroup
        );
        assertTrue("two ranges over a bound of one go to the aggregators: " + range, range instanceof LuceneAggregateExec);
        RelNode sum = planUnder("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"price\"}}}}", oneGroup);
        assertTrue("one group fits a bound of one: " + sum, sum instanceof LanceTableScan);
        RelNode terms = planUnder("{\"size\":0,\"aggs\":{\"by\":{\"terms\":{\"field\":\"category\"}}}}", oneGroup);
        assertTrue("a guessed terms domain is not judged against the bound: " + terms, terms instanceof LanceTableScan);
    }

    private static RelNode planUnder(String body, CostInputs inputs) throws IOException {
        return PlanTestFixtures.factory().plan(PlanTestFixtures.translate(PlanTestFixtures.parse(body)), inputs);
    }

    public void testFoldableHitsPageAnswersWithTheLanceScan() throws IOException {
        RelNode physical = plan("{\"size\":3,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},\"sort\":[\"_score\"]}");
        assertTrue("the pushed scan wins the cost comparison: " + physical, physical instanceof LanceTableScan);
        LanceTableScan scan = (LanceTableScan) physical;
        assertTrue("the FTS lookup is pushed", scan.pushedFts().isPresent());
        PushedTopK pushed = scan.pushedTopK().orElseThrow();
        assertEquals(3, pushed.fetch());
    }

    public void testUnfoldableHitsPageAnswersWithTheLuceneOperator() throws IOException {
        // A page mixing the score order with a column collation needs
        // Lucene's field collector, so the top-k pushdown rule refuses
        // and the heap operator is the only physical form.
        RelNode physical = plan(
            "{\"size\":3,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},\"sort\":[\"_score\",{\"rating\":\"asc\"}]}"
        );
        assertTrue("the Lucene operator answers the shape: " + physical, physical instanceof HeapTopKExec);
        assertNotNull(((HeapTopKExec) physical).hitShape());
    }

    public void testShapeWithoutAPhysicalFormKeepsTheLogicalPlan() {
        // A bare projection over the scan has no pushdown rule and no
        // Lucene operator, so the planner cannot reach either
        // convention and the logical plan itself comes back.
        LancePlannerFactory factory = PlanTestFixtures.factory();
        RelBuilder builder = factory.relBuilder(PlanTestFixtures.model().schema());
        RelNode logical = builder.scan(LancePlannerFactory.SCHEMA_NAME, "idx").project(builder.field("rating")).build();
        assertSame(logical, factory.plan(logical));
    }
}
