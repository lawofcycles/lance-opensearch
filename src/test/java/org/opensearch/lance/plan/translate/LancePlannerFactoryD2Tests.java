/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.translate;

import org.apache.calcite.rel.RelNode;
import org.opensearch.lance.plan.calcite.LuceneConvention;
import org.opensearch.lance.plan.rel.LanceAggregate;
import org.opensearch.lance.plan.rel.LanceFtsMatch;
import org.opensearch.lance.plan.rel.LanceHitShape;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.lance.plan.rel.physical.FanOutExec;
import org.opensearch.lance.plan.rel.physical.HeapTopKExec;
import org.opensearch.lance.plan.rel.physical.LuceneAggregateExec;
import org.opensearch.lance.plan.rel.physical.LuceneHandoffExec;
import org.opensearch.lance.plan.rel.physical.MergeExec;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * The coordinator layer through the Volcano run: a tree wrapped by
 * {@code SearchRequestToRel.withCoordinatorLayer} folds to the
 * {@code MergeExec (FanOutExec (per node plan))} physical form, with
 * the per-node subtree still reaching whichever convention it reaches
 * without the wrapper. The wrapper derives the reduce kind from the
 * plan root; the fan-out width and partitioning ride through the
 * converters unchanged.
 */
public class LancePlannerFactoryD2Tests extends OpenSearchTestCase {

    private static RelNode wrapAndPlan(String body, int fanOut) throws IOException {
        RelNode logical = PlanTestFixtures.translate(PlanTestFixtures.parse(body));
        return PlanTestFixtures.factory().plan(SearchRequestToRel.withCoordinatorLayer(logical, fanOut));
    }

    /** The coordinator pair at the root of a planned tree, both in the Lucene convention. */
    private static FanOutExec coordinatorLayerOf(RelNode physical, MergeExec.ReduceKind kind, int fanOut) {
        assertTrue("the merge is the physical root: " + physical, physical instanceof MergeExec);
        MergeExec merge = (MergeExec) physical;
        assertSame(LuceneConvention.INSTANCE, merge.getConvention());
        assertSame(kind, merge.reduceKind());
        assertTrue("the fan-out is below the merge: " + physical, merge.getInput() instanceof FanOutExec);
        FanOutExec fan = (FanOutExec) merge.getInput();
        assertSame(LuceneConvention.INSTANCE, fan.getConvention());
        assertEquals(fanOut, fan.fanOut());
        assertSame(FanOutExec.Partitioning.EQUAL_FRAGMENT_GROUPS, fan.partitioning());
        return fan;
    }

    public void testWrapperDerivesTheReduceKindFromThePlanRoot() throws IOException {
        RelNode aggregate = PlanTestFixtures.translate(
            PlanTestFixtures.parse("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}")
        );
        assertTrue(aggregate instanceof LanceAggregate);
        assertSame(
            MergeExec.ReduceKind.AGGREGATE_INTERNAL,
            ((MergeExec) SearchRequestToRel.withCoordinatorLayer(aggregate, 2)).reduceKind()
        );
        RelNode hits = PlanTestFixtures.translate(PlanTestFixtures.parse("{\"size\":3,\"query\":{\"term\":{\"category\":\"c0\"}}}"));
        assertTrue(hits instanceof LanceHitShape);
        assertSame(MergeExec.ReduceKind.HITS_TOP_K, ((MergeExec) SearchRequestToRel.withCoordinatorLayer(hits, 2)).reduceKind());
        RelNode count = PlanTestFixtures.translate(
            PlanTestFixtures.parse("{\"size\":0,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}")
        );
        assertTrue(count instanceof LanceFtsMatch);
        assertSame(MergeExec.ReduceKind.COUNT_SUM, ((MergeExec) SearchRequestToRel.withCoordinatorLayer(count, 2)).reduceKind());
    }

    public void testWrapperKeepsThePerNodeTreeUntouched() throws IOException {
        RelNode logical = PlanTestFixtures.translate(
            PlanTestFixtures.parse("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}")
        );
        RelNode wrapped = SearchRequestToRel.withCoordinatorLayer(logical, 4);
        assertSame(logical, wrapped.getInput(0).getInput(0));
        assertEquals(logical.getRowType(), wrapped.getRowType());
    }

    public void testFoldableAggregateFoldsUnderTheCoordinatorLayer() throws IOException {
        RelNode physical = wrapAndPlan(
            "{\"size\":0,\"aggs\":{\"by\":{\"terms\":{\"field\":\"category\"},\"aggs\":{\"a\":{\"avg\":{\"field\":\"rating\"}}}}}}",
            3
        );
        FanOutExec fan = coordinatorLayerOf(physical, MergeExec.ReduceKind.AGGREGATE_INTERNAL, 3);
        assertTrue("the pushed scan arrives behind the handoff: " + physical, fan.getInput() instanceof LuceneHandoffExec);
        LanceTableScan scan = (LanceTableScan) ((LuceneHandoffExec) fan.getInput()).getInput();
        assertTrue(scan.pushedAggregate().isPresent());
    }

    public void testUnfoldableAggregateReachesTheLuceneOperatorUnderTheCoordinatorLayer() throws IOException {
        RelNode physical = wrapAndPlan(
            "{\"size\":0,\"query\":{\"term\":{\"category\":\"c0\"}},\"aggs\":{\"u\":{\"cardinality\":{\"field\":\"rating\"}}}}",
            2
        );
        FanOutExec fan = coordinatorLayerOf(physical, MergeExec.ReduceKind.AGGREGATE_INTERNAL, 2);
        assertTrue("the Lucene operator answers the per-node shape: " + physical, fan.getInput() instanceof LuceneAggregateExec);
    }

    public void testFoldableHitsPageFoldsUnderTheCoordinatorLayer() throws IOException {
        RelNode physical = wrapAndPlan(
            "{\"size\":3,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},\"sort\":[\"_score\"]}",
            3
        );
        FanOutExec fan = coordinatorLayerOf(physical, MergeExec.ReduceKind.HITS_TOP_K, 3);
        assertTrue("the pushed scan arrives behind the handoff: " + physical, fan.getInput() instanceof LuceneHandoffExec);
        LanceTableScan scan = (LanceTableScan) ((LuceneHandoffExec) fan.getInput()).getInput();
        assertTrue(scan.pushedFts().isPresent());
        assertEquals(3, scan.pushedTopK().orElseThrow().fetch());
    }

    public void testUnfoldableHitsPageReachesTheHeapOperatorUnderTheCoordinatorLayer() throws IOException {
        RelNode physical = wrapAndPlan(
            "{\"size\":3,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}},\"sort\":[\"_score\",{\"rating\":\"asc\"}]}",
            2
        );
        FanOutExec fan = coordinatorLayerOf(physical, MergeExec.ReduceKind.HITS_TOP_K, 2);
        assertTrue("the heap operator answers the per-node shape: " + physical, fan.getInput() instanceof HeapTopKExec);
    }

    public void testCountShapeFoldsUnderTheCoordinatorLayer() throws IOException {
        RelNode physical = wrapAndPlan("{\"size\":0,\"query\":{\"lance_match\":{\"field\":\"body\",\"query\":\"hello\"}}}", 5);
        FanOutExec fan = coordinatorLayerOf(physical, MergeExec.ReduceKind.COUNT_SUM, 5);
        assertTrue("the pushed scan arrives behind the handoff: " + physical, fan.getInput() instanceof LuceneHandoffExec);
        LanceTableScan scan = (LanceTableScan) ((LuceneHandoffExec) fan.getInput()).getInput();
        assertTrue(scan.pushedFts().isPresent());
    }
}
