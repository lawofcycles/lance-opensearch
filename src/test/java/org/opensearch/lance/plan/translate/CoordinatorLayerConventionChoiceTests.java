/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.translate;

import org.apache.calcite.rel.RelNode;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.physical.FanOutExec;
import org.opensearch.lance.plan.rel.physical.HeapTopKExec;
import org.opensearch.lance.plan.rel.physical.LuceneAggregateExec;
import org.opensearch.lance.plan.rel.physical.LuceneHandoffExec;
import org.opensearch.lance.plan.rel.physical.MergeExec;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * The convention choice below the coordinator layer, pinned through
 * the Volcano run: adding the {@code MergeExec} / {@code FanOutExec}
 * converters must not change which convention the per-node subtree
 * reaches, and the coordinator converters must not fire into the
 * middle of a pushdown chain. Every shape here plans once bare and
 * once wrapped, and the per-node answer has to be the same operator
 * carrying the same pushed operations.
 */
public class CoordinatorLayerConventionChoiceTests extends OpenSearchTestCase {

    private static RelNode planBare(String body) throws IOException {
        return PlanTestFixtures.factory().plan(PlanTestFixtures.translate(PlanTestFixtures.parse(body)));
    }

    private static RelNode planWrapped(String body) throws IOException {
        RelNode logical = PlanTestFixtures.translate(PlanTestFixtures.parse(body));
        RelNode physical = PlanTestFixtures.factory().plan(SearchRequestToRel.withCoordinatorLayer(logical, 3));
        assertTrue("the merge is the physical root: " + physical, physical instanceof MergeExec);
        RelNode fan = ((MergeExec) physical).getInput();
        assertTrue("the fan-out is below the merge: " + physical, fan instanceof FanOutExec);
        RelNode perNode = ((FanOutExec) fan).getInput();
        // The bare run unwraps the root handoff; do the same for the
        // per-node subtree so the two forms compare directly.
        return perNode instanceof LuceneHandoffExec handoff ? handoff.getInput() : perNode;
    }

    public void testAggregatePushdownFiresTheSameUnderTheCoordinatorLayer() throws IOException {
        String body = "{\"size\":0,\"query\":{\"term\":{\"category\":\"c0\"}},\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}";
        RelNode bare = planBare(body);
        RelNode wrapped = planWrapped(body);
        assertTrue(bare instanceof LanceTableScan);
        assertTrue(wrapped instanceof LanceTableScan);
        assertEquals(((LanceTableScan) bare).pushedOperations().toString(), ((LanceTableScan) wrapped).pushedOperations().toString());
    }

    public void testSortLimitPushdownFiresTheSameUnderTheCoordinatorLayer() throws IOException {
        String body = "{\"size\":4,\"query\":{\"term\":{\"category\":\"c0\"}},\"sort\":[{\"rating\":\"asc\"}]}";
        RelNode bare = planBare(body);
        RelNode wrapped = planWrapped(body);
        assertTrue(bare instanceof LanceTableScan);
        assertTrue(wrapped instanceof LanceTableScan);
        assertTrue(((LanceTableScan) wrapped).pushedTopK().isPresent());
        assertEquals(((LanceTableScan) bare).pushedOperations().toString(), ((LanceTableScan) wrapped).pushedOperations().toString());
    }

    public void testFtsFuseFiresTheSameUnderTheCoordinatorLayer() throws IOException {
        String body = "{\"size\":3,\"query\":{\"bool\":{\"must\":[{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}],"
            + "\"filter\":[{\"term\":{\"category\":\"c0\"}}]}},\"sort\":[\"_score\"]}";
        RelNode bare = planBare(body);
        RelNode wrapped = planWrapped(body);
        assertTrue(bare instanceof LanceTableScan);
        assertTrue(wrapped instanceof LanceTableScan);
        assertTrue(((LanceTableScan) wrapped).pushedFts().isPresent());
        assertEquals(((LanceTableScan) bare).pushedOperations().toString(), ((LanceTableScan) wrapped).pushedOperations().toString());
    }

    public void testLuceneFallbackStaysTheSameUnderTheCoordinatorLayer() throws IOException {
        String aggregateBody =
            "{\"size\":0,\"query\":{\"term\":{\"category\":\"c0\"}},\"aggs\":{\"by\":{\"terms\":{\"field\":\"category\"}}}}";
        assertTrue(planBare(aggregateBody) instanceof LuceneAggregateExec);
        assertTrue(planWrapped(aggregateBody) instanceof LuceneAggregateExec);
        String pageBody = "{\"size\":3,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},"
            + "\"sort\":[\"_score\",{\"rating\":\"asc\"}]}";
        assertTrue(planBare(pageBody) instanceof HeapTopKExec);
        assertTrue(planWrapped(pageBody) instanceof HeapTopKExec);
    }
}
