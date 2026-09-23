/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.rules;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.PushedOperation.PushedTopK;
import org.opensearch.lance.plan.rel.physical.HeapTopKExec;
import org.opensearch.lance.plan.rel.physical.LuceneAggregateExec;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

/**
 * The converter rule and the pushdown rules coexist inside one Volcano
 * run: both match the same logical chains, so these tests pin that
 * registering the converter does not stop
 * {@link PushSortLimitIntoLanceScan} or
 * {@link PushAggregateIntoLanceScan} from folding a foldable tree, and
 * that the cost pin (the operators' constant offset over the zero cost
 * handoff) makes the Lance form win whenever both alternatives exist.
 */
public class ConverterRuleWithPushSortLimitTests extends OpenSearchTestCase {

    private static RelNode translate(String body) throws IOException {
        return PlanTestFixtures.translate(PlanTestFixtures.parse(body));
    }

    /** Every rule registered: the foldable page still folds into the scan. */
    public void testFoldablePageFoldsNextToTheConverterAlternative() throws IOException {
        RelNode logical = translate("{\"size\":5,\"query\":{\"term\":{\"category\":\"c0\"}},\"sort\":[{\"rating\":\"desc\"}]}");
        RelNode physical = PlanTestFixtures.factory().plan(logical);
        assertTrue("the fold wins over the enumerated Lucene alternative: " + physical, physical instanceof LanceTableScan);
        LanceTableScan scan = (LanceTableScan) physical;
        assertTrue(scan.pushedFilter().isPresent());
        PushedTopK pushed = scan.pushedTopK().orElseThrow();
        assertEquals(5, pushed.fetch());
        assertNotNull(pushed.hitShape());
    }

    /**
     * The same tree without the top-k pushdown rules: the converter's
     * alternative is really enumerated for it, so the choice in the
     * full rule set is a cost decision, not an accident of matching.
     */
    public void testTheSamePageHasTheLuceneAlternative() throws IOException {
        RelNode logical = translate("{\"size\":5,\"query\":{\"term\":{\"category\":\"c0\"}},\"sort\":[{\"rating\":\"desc\"}]}");
        RelOptPlanner planner = logical.getCluster().getPlanner();
        for (RelOptRule rule : List.copyOf(planner.getRules())) {
            if (rule instanceof PushSortLimitIntoLanceScan) {
                planner.removeRule(rule);
            }
        }
        RelNode physical = PlanTestFixtures.factory().plan(logical);
        assertTrue("without the fold the converter answers: " + physical, physical instanceof HeapTopKExec);
    }

    /** A page the fold refuses takes the converter's alternative under the full rule set. */
    public void testUnfoldablePageTakesTheLuceneAlternative() throws IOException {
        // A two collation search_after cursor has no strict SQL bound,
        // so the top-k pushdown rule does not transform.
        RelNode logical = translate("{\"size\":5,\"sort\":[{\"rating\":\"asc\"},{\"id\":\"asc\"}],\"search_after\":[100,5]}");
        RelNode physical = PlanTestFixtures.factory().plan(logical);
        assertTrue("the converter answers the refused fold: " + physical, physical instanceof HeapTopKExec);
    }

    /** Every rule registered: the foldable aggregate still folds into the scan. */
    public void testFoldableAggregateFoldsNextToTheConverterAlternative() throws IOException {
        RelNode logical = translate("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}");
        RelNode physical = PlanTestFixtures.factory().plan(logical);
        assertTrue("the fold wins over the enumerated Lucene alternative: " + physical, physical instanceof LanceTableScan);
        assertTrue(((LanceTableScan) physical).pushedAggregate().isPresent());
    }

    /** The same aggregate without its pushdown rules: the converter's alternative is enumerated. */
    public void testTheSameAggregateHasTheLuceneAlternative() throws IOException {
        RelNode logical = translate("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}");
        RelOptPlanner planner = logical.getCluster().getPlanner();
        for (RelOptRule rule : List.copyOf(planner.getRules())) {
            if (rule instanceof PushAggregateIntoLanceScan) {
                planner.removeRule(rule);
            }
        }
        RelNode physical = PlanTestFixtures.factory().plan(logical);
        assertTrue("without the fold the converter answers: " + physical, physical instanceof LuceneAggregateExec);
    }
}
