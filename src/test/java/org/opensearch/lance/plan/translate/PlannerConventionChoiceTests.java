/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.translate;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
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
        // The pushdown rule's operand rejects the cardinality whatever
        // the chain below it, so the converter rule's alternative is
        // the only physical form of this tree; the filter stays inside
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
        // The pushdown rule's operand rejects a tree with a cardinality
        // metric (the pushed form is slower than the aggregator), so the
        // Lucene operator is the only physical form.
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
