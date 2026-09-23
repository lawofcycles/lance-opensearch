/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.execute;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.lucene.search.TotalHits;
import org.opensearch.Version;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.action.ActionListener;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.dispatch.LanceFragmentQueryAction;
import org.opensearch.lance.dispatch.LanceFragmentQueryRequest;
import org.opensearch.lance.dispatch.LanceFragmentQueryResponse;
import org.opensearch.lance.plan.rel.physical.MergeExec;
import org.opensearch.lance.plan.translate.PlanTestFixtures;
import org.opensearch.lance.plan.translate.SearchRequestToRel;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHit;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ReceiveTimeoutTransportException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;

/**
 * {@link PlanExecutor#execute} traverses the coordinator plan and
 * drives the operators: the {@code FanOutExec} sends one request per
 * fragment group in slot order through the sender, and the
 * {@code MergeExec} absorbs the gathered responses into the
 * {@link MergeReducer}, whose built response carries the same merged
 * hits and summed counts the coordinator produced before the
 * operators existed. A plan without the coordinator pair is refused
 * loudly instead of half executing.
 */
public class PlanExecutorTests extends OpenSearchTestCase {

    private static final DiscoveryNode NODE_A = new DiscoveryNode("a", buildNewFakeTransportAddress(), Version.CURRENT);
    private static final DiscoveryNode NODE_B = new DiscoveryNode("b", buildNewFakeTransportAddress(), Version.CURRENT);

    private TestThreadPool threadPool;
    private ClusterService clusterService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
    }

    @Override
    public void tearDown() throws Exception {
        clusterService.close();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    private static PlanExecutor executor() {
        return new PlanExecutor(PlanTestFixtures.factory());
    }

    private static RelNode perNodeTree() throws IOException {
        return PlanTestFixtures.translate(PlanTestFixtures.parse("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}"));
    }

    /** A coordinator plan over the fixture tree, in the logical form the wrapper builds. */
    private static RelNode plan(int fanOut) throws IOException {
        return SearchRequestToRel.withCoordinatorLayer(perNodeTree(), MergeExec.ReduceKind.COUNT_SUM, fanOut);
    }

    private MergeReducer reducer(int trackTotalHitsUpTo) {
        return new MergeReducer(
            clusterService,
            BigArrays.NON_RECYCLING_INSTANCE,
            null,
            null,
            List.of(),
            0,
            10,
            false,
            false,
            false,
            trackTotalHitsUpTo
        );
    }

    private static PlanExecutor.FanOutContext context(
        List<PlanExecutor.FragmentGroup> groups,
        FragmentFanOut.Sender sender,
        boolean allowPartial
    ) {
        return new PlanExecutor.FanOutContext(
            groups,
            group -> new LanceFragmentQueryRequest(
                "table",
                "idx",
                StorageOptions.empty(),
                -1L,
                FragmentPlan.lucene(FragmentPlan.Kind.LUCENE_TOPK, null),
                null,
                null,
                List.of(),
                null,
                10,
                null,
                group.fragmentIds(),
                false,
                SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO
            ),
            sender,
            Runnable::run,
            Runnable::run,
            (node, request, cause) -> {},
            allowPartial,
            null
        );
    }

    private static LanceFragmentQueryResponse response(long matched, List<SearchHit> hits, long[] rowAddrs) {
        return new LanceFragmentQueryResponse(matched, false, 1, hits, rowAddrs, null);
    }

    private static SearchHit hit(long id) {
        SearchHit hit = new SearchHit((int) id, Long.toString(id), Map.of(), Map.of());
        hit.score(Float.NaN);
        hit.sortValues(new Object[] { id }, new DocValueFormat[] { DocValueFormat.RAW });
        return hit;
    }

    public void testExecuteRefusesAPlanWithoutTheMergeRoot() throws IOException {
        RelNode bare = perNodeTree();
        IllegalArgumentException refused = expectThrows(
            IllegalArgumentException.class,
            () -> executor().execute(
                bare,
                context(List.of(), (n, r, h) -> {}, true),
                reducer(SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO),
                "idx",
                ActionListener.wrap(() -> {})
            )
        );
        assertThat(refused.getMessage(), containsString("MergeExec"));
    }

    public void testExecuteRefusesAMergeWithoutTheFanOut() throws IOException {
        RelNode bare = perNodeTree();
        MergeExec merge = new MergeExec(
            bare.getCluster(),
            bare.getCluster().traitSetOf(Convention.NONE),
            bare,
            MergeExec.ReduceKind.COUNT_SUM
        );
        IllegalArgumentException refused = expectThrows(
            IllegalArgumentException.class,
            () -> executor().execute(
                merge,
                context(List.of(), (n, r, h) -> {}, true),
                reducer(SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO),
                "idx",
                ActionListener.wrap(() -> {})
            )
        );
        assertThat(refused.getMessage(), containsString("FanOutExec"));
    }

    public void testExecuteSendsOneRequestPerGroupInSlotOrderAndSumsTheCounts() throws Exception {
        List<PlanExecutor.FragmentGroup> groups = List.of(
            new PlanExecutor.FragmentGroup(NODE_A, List.of(0, 2), 20L, 0, 1),
            new PlanExecutor.FragmentGroup(NODE_B, List.of(1, 3), 20L, 0, 1)
        );
        List<String> sentNodes = new ArrayList<>();
        List<List<Integer>> sentFragments = new ArrayList<>();
        FragmentFanOut.Sender sender = (node, request, handler) -> {
            sentNodes.add(node.getId());
            sentFragments.add(request.fragmentIds());
            handler.handleResponse(response(node == NODE_A ? 5L : 7L, List.of(), new long[0]));
        };
        MergeReducer reducer = reducer(SearchContext.TRACK_TOTAL_HITS_ACCURATE);
        AtomicInteger completions = new AtomicInteger();
        executor().execute(
            plan(groups.size()),
            context(groups, sender, true),
            reducer,
            "idx",
            ActionListener.wrap(v -> completions.incrementAndGet(), e -> fail("unexpected failure: " + e))
        );
        assertEquals(List.of("a", "b"), sentNodes);
        assertEquals(List.of(List.of(0, 2), List.of(1, 3)), sentFragments);
        assertEquals(1, completions.get());
        SearchResponse response = reducer.buildResponse(System.currentTimeMillis());
        assertEquals(12L, response.getHits().getTotalHits().value());
        assertEquals(TotalHits.Relation.EQUAL_TO, response.getHits().getTotalHits().relation());
        assertFalse(response.isTimedOut());
    }

    public void testExecuteMergesTheHitPagesTheWayTheCoordinatorDid() throws Exception {
        List<PlanExecutor.FragmentGroup> groups = List.of(
            new PlanExecutor.FragmentGroup(NODE_A, List.of(0), 10L, 0, 1),
            new PlanExecutor.FragmentGroup(NODE_B, List.of(1), 10L, 0, 1)
        );
        FragmentFanOut.Sender sender = (node, request, handler) -> {
            if (node == NODE_A) {
                handler.handleResponse(response(2L, List.of(hit(0), hit(3)), new long[] { 0L, 3L }));
            } else {
                handler.handleResponse(response(2L, List.of(hit(1), hit(2)), new long[] { 1L, 2L }));
            }
        };
        MergeReducer reducer = reducer(SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO);
        AtomicReference<Exception> failure = new AtomicReference<>();
        executor().execute(plan(groups.size()), context(groups, sender, true), reducer, "idx", ActionListener.wrap(v -> {}, failure::set));
        assertNull(failure.get());
        SearchResponse response = reducer.buildResponse(System.currentTimeMillis());
        // No sort clause and NaN scores: the merge falls back to the
        // target then row address tie break, the order one reader over
        // the whole table would produce.
        SearchHit[] merged = response.getHits().getHits();
        assertEquals(4, merged.length);
        assertEquals("0", merged[0].getId());
        assertEquals("1", merged[1].getId());
        assertEquals("2", merged[2].getId());
        assertEquals("3", merged[3].getId());
    }

    public void testExecuteReportsATimedOutNodeThroughTheReducer() throws Exception {
        List<PlanExecutor.FragmentGroup> groups = List.of(
            new PlanExecutor.FragmentGroup(NODE_A, List.of(0), 10L, 0, 1),
            new PlanExecutor.FragmentGroup(NODE_B, List.of(1), 10L, 0, 1)
        );
        FragmentFanOut.Sender sender = (node, request, handler) -> {
            if (node == NODE_A) {
                handler.handleResponse(response(5L, List.of(), new long[0]));
            } else {
                handler.handleException(
                    new ReceiveTimeoutTransportException(node, LanceFragmentQueryAction.NAME, "request_id [1] timed out after [10ms]")
                );
            }
        };
        MergeReducer reducer = reducer(SearchContext.TRACK_TOTAL_HITS_ACCURATE);
        AtomicInteger completions = new AtomicInteger();
        executor().execute(
            plan(groups.size()),
            context(groups, sender, true),
            reducer,
            "idx",
            ActionListener.wrap(v -> completions.incrementAndGet(), e -> fail("partial results were allowed: " + e))
        );
        assertEquals(1, completions.get());
        SearchResponse response = reducer.buildResponse(System.currentTimeMillis());
        assertTrue(response.isTimedOut());
        // The nodes that answered are a lower bound of the true count.
        assertEquals(5L, response.getHits().getTotalHits().value());
        assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, response.getHits().getTotalHits().relation());
    }
}
