/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.Version;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.dispatch.LanceFragmentQueryResponse.DeferredHits;
import org.opensearch.lance.plan.execute.MergeReducer;
import org.opensearch.lance.query.LanceKnnQueryBuilder;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHit;
import org.opensearch.search.collapse.CollapseBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ReceiveTimeoutTransportException;
import org.opensearch.transport.TransportResponseHandler;

/**
 * The coordinator side of the fetch round: deferred hits merge by their
 * own score and sort values next to rendered ones, the round asks each
 * node for the rows of the page it holds and nothing else, the rendered
 * hits land in page order with the query round's score and sort values,
 * and a node that fails or times out fails the request. Also pins the
 * decision the coordinator takes per body and per target.
 */
public class CoordinatorFetchPhaseTests extends OpenSearchTestCase {

    private static final DiscoveryNode NODE_A = new DiscoveryNode("a", buildNewFakeTransportAddress(), Version.CURRENT);
    private static final DiscoveryNode NODE_B = new DiscoveryNode("b", buildNewFakeTransportAddress(), Version.CURRENT);
    private static final CoordinatorFetchPhase.Target TARGET = new CoordinatorFetchPhase.Target("/t.lance", StorageOptions.empty(), 3L);

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

    private MergeReducer reducer(List<SortBuilder<?>> sorts, int from, int size) {
        return new MergeReducer(
            clusterService,
            BigArrays.NON_RECYCLING_INSTANCE,
            null,
            null,
            sorts,
            from,
            size,
            false,
            false,
            false,
            SearchContext.TRACK_TOTAL_HITS_ACCURATE
        );
    }

    /** A response whose page is deferred: sorted descending by {@code id} when {@code ids} is given, score ordered otherwise. */
    private static LanceFragmentQueryResponse deferred(long[] rowAddrs, float[] scores, long[] ids) {
        Object[][] sortValues = new Object[rowAddrs.length][];
        for (int i = 0; i < rowAddrs.length; i++) {
            sortValues[i] = ids == null ? new Object[0] : new Object[] { ids[i] };
        }
        DeferredHits hits = new DeferredHits(
            rowAddrs,
            scores,
            sortValues,
            ids == null ? null : new DocValueFormat[] { DocValueFormat.RAW }
        );
        return new LanceFragmentQueryResponse(rowAddrs.length, false, 1, List.of(), new long[0], null, null, null, hits);
    }

    private static SearchHit rendered(String id, float score) {
        SearchHit hit = new SearchHit(0, id, Collections.emptyMap(), Collections.emptyMap());
        hit.score(score);
        hit.sourceRef(new BytesArray("{\"id\":\"" + id + "\"}"));
        return hit;
    }

    /** A sender answering every request from {@code render}, recording what was asked. */
    private static final class RecordingSender implements CoordinatorFetchPhase.Sender {
        final List<DiscoveryNode> nodes = new ArrayList<>();
        final List<LanceFragmentFetchRequest> requests = new ArrayList<>();

        @Override
        public void send(
            DiscoveryNode node,
            LanceFragmentFetchRequest request,
            TransportResponseHandler<LanceFragmentFetchResponse> handler
        ) {
            nodes.add(node);
            requests.add(request);
            List<SearchHit> hits = new ArrayList<>();
            for (long rowAddr : request.rowAddrs()) {
                hits.add(rendered(node.getId() + "-" + rowAddr, Float.NaN));
            }
            handler.handleResponse(
                new LanceFragmentFetchResponse(hits, new LanceFragmentQueryResponse.Profile(0L, 1L, 1L, hits.size(), 2L, 3L, 0L))
            );
        }
    }

    private void runRound(MergeReducer merged, CoordinatorFetchPhase.Sender sender, LanceSearchProfile profile, ActionListener<Void> done) {
        CoordinatorFetchPhase.run(
            merged,
            name -> TARGET,
            HitProjection.NONE,
            sender,
            threadPool.generic(),
            threadPool.generic(),
            null,
            profile,
            (node, request, cause) -> {},
            done
        );
    }

    private static <T> T await(AtomicReference<T> value, CountDownLatch latch) throws InterruptedException {
        assertTrue("the round completed", latch.await(10, TimeUnit.SECONDS));
        return value.get();
    }

    public void testDeferredHitsMergeBySortValueAndTheRoundAsksEachNodeForItsRowsOfThePage() throws Exception {
        List<SortBuilder<?>> sorts = List.of(new FieldSortBuilder("id").order(SortOrder.DESC));
        MergeReducer merged = reducer(sorts, 0, 3);
        // Node a holds ids 9, 5, 1 (fragment 0); node b holds ids 8, 7,
        // 0 (fragment 1). The page keeps 9, 8, 7: one row of a, two of b.
        merged.absorbTarget(
            "idx",
            List.of(
                deferred(new long[] { 0L, 1L, 2L }, new float[] { Float.NaN, Float.NaN, Float.NaN }, new long[] { 9L, 5L, 1L }),
                deferred(
                    new long[] { (1L << 32), (1L << 32) | 1L, (1L << 32) | 2L },
                    new float[] { Float.NaN, Float.NaN, Float.NaN },
                    new long[] { 8L, 7L, 0L }
                )
            ),
            List.of(NODE_A, NODE_B),
            false
        );
        List<MergeReducer.RankedHit> page = merged.page();
        assertEquals(3, page.size());
        assertNull(page.get(0).hit());
        assertEquals(9L, page.get(0).rawSortValues()[0]);
        assertEquals(NODE_A, page.get(0).deferred().node());
        assertEquals(NODE_B, page.get(1).deferred().node());
        assertEquals(NODE_B, page.get(2).deferred().node());

        RecordingSender sender = new RecordingSender();
        LanceSearchProfile profile = new LanceSearchProfile();
        AtomicReference<Exception> failure = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        runRound(merged, sender, profile, ActionListener.wrap(v -> latch.countDown(), e -> {
            failure.set(e);
            latch.countDown();
        }));
        assertNull(await(failure, latch));

        assertEquals("one request per node holding rows of the page", List.of(NODE_A, NODE_B), sender.nodes);
        assertArrayEquals(new long[] { 0L }, sender.requests.get(0).rowAddrs());
        assertArrayEquals(new long[] { (1L << 32), (1L << 32) | 1L }, sender.requests.get(1).rowAddrs());
        assertEquals("/t.lance", sender.requests.get(0).tableUri());
        assertEquals(3L, sender.requests.get(0).version());
        assertEquals("idx", sender.requests.get(0).indexName());
        assertEquals(2, profile.fetchRoundTrips());
        assertEquals(1L, profile.nodes().get("a").takeRows());
        assertEquals(2L, profile.nodes().get("b").takeRows());

        SearchResponse response = merged.buildResponse(System.currentTimeMillis());
        SearchHit[] hits = response.getHits().getHits();
        assertEquals(3, hits.length);
        assertEquals("a-0", hits[0].getId());
        assertEquals("b-" + (1L << 32), hits[1].getId());
        assertEquals("b-" + ((1L << 32) | 1L), hits[2].getId());
        assertEquals("the sort values of the query round are stamped on the rendered hit", 9L, hits[0].getSortValues()[0]);
        assertEquals(7L, hits[2].getSortValues()[0]);
        assertEquals(6L, response.getHits().getTotalHits().value());
    }

    public void testRenderedAndDeferredHitsMergeTogetherAndOnlyTheDeferredOnesAreFetched() throws Exception {
        // Score order. Node a (an executor of the previous plugin
        // version) rendered its hits; node b deferred. The page is the
        // top two scores: b's 3.0 and a's 2.0.
        MergeReducer merged = reducer(List.of(), 0, 2);
        LanceFragmentQueryResponse renderedResponse = new LanceFragmentQueryResponse(
            2L,
            false,
            1,
            List.of(rendered("a-first", 2.0f), rendered("a-second", 1.0f)),
            new long[] { 0L, 1L },
            null
        );
        merged.absorbTarget(
            "idx",
            List.of(renderedResponse, deferred(new long[] { (1L << 32), (1L << 32) | 1L }, new float[] { 3.0f, 0.5f }, null)),
            List.of(NODE_A, NODE_B),
            false
        );
        List<MergeReducer.RankedHit> page = merged.page();
        assertEquals(2, page.size());
        assertNull(page.get(0).hit());
        assertEquals(3.0f, page.get(0).score(), 0f);
        assertEquals("a-first", page.get(1).hit().getId());

        RecordingSender sender = new RecordingSender();
        AtomicReference<Exception> failure = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        runRound(merged, sender, null, ActionListener.wrap(v -> latch.countDown(), e -> {
            failure.set(e);
            latch.countDown();
        }));
        assertNull(await(failure, latch));
        assertEquals(List.of(NODE_B), sender.nodes);
        assertArrayEquals(new long[] { (1L << 32) }, sender.requests.get(0).rowAddrs());

        SearchResponse response = merged.buildResponse(System.currentTimeMillis());
        SearchHit[] hits = response.getHits().getHits();
        assertEquals("b-" + (1L << 32), hits[0].getId());
        assertEquals("the query round's score is stamped on the rendered hit", 3.0f, hits[0].getScore(), 0f);
        assertEquals("a-first", hits[1].getId());
        assertEquals(3.0f, response.getHits().getMaxScore(), 0f);
    }

    public void testFromSkipsDeferredRowsWithoutFetchingThem() throws Exception {
        MergeReducer merged = reducer(List.of(), 2, 2);
        merged.absorbTarget(
            "idx",
            List.of(
                deferred(new long[] { 0L, 1L, 2L, 3L }, new float[] { 4f, 3f, 2f, 1f }, null),
                deferred(new long[] { (1L << 32) }, new float[] { 5f }, null)
            ),
            List.of(NODE_A, NODE_B),
            false
        );
        RecordingSender sender = new RecordingSender();
        CountDownLatch latch = new CountDownLatch(1);
        runRound(merged, sender, null, ActionListener.wrap(v -> latch.countDown(), e -> latch.countDown()));
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        // Scores 5, 4 are skipped; the page is 3, 2, both on node a.
        assertEquals(List.of(NODE_A), sender.nodes);
        assertArrayEquals(new long[] { 1L, 2L }, sender.requests.get(0).rowAddrs());
        SearchHit[] hits = merged.buildResponse(System.currentTimeMillis()).getHits().getHits();
        assertEquals(2, hits.length);
        assertEquals(3f, hits[0].getScore(), 0f);
        assertEquals(2f, hits[1].getScore(), 0f);
    }

    public void testAPageWithoutDeferredHitsSendsNothing() throws Exception {
        MergeReducer merged = reducer(List.of(), 0, 2);
        merged.absorbTarget(
            "idx",
            List.of(new LanceFragmentQueryResponse(1L, false, 1, List.of(rendered("a", 1f)), new long[] { 0L }, null)),
            List.of(NODE_A),
            false
        );
        RecordingSender sender = new RecordingSender();
        AtomicInteger completions = new AtomicInteger();
        runRound(merged, sender, null, ActionListener.wrap(v -> completions.incrementAndGet(), e -> fail(e.toString())));
        assertEquals(1, completions.get());
        assertTrue(sender.requests.isEmpty());
        assertEquals("a", merged.buildResponse(System.currentTimeMillis()).getHits().getHits()[0].getId());
    }

    public void testANodeThatTimesOutFailsTheRequestWith504() throws Exception {
        MergeReducer merged = reducer(List.of(), 0, 2);
        merged.absorbTarget(
            "idx",
            List.of(deferred(new long[] { 0L }, new float[] { 2f }, null), deferred(new long[] { (1L << 32) }, new float[] { 1f }, null)),
            List.of(NODE_A, NODE_B),
            false
        );
        AtomicReference<DiscoveryNode> incomplete = new AtomicReference<>();
        CoordinatorFetchPhase.Sender sender = (node, request, handler) -> {
            if (node == NODE_B) {
                handler.handleException(new ReceiveTimeoutTransportException(node, LanceFragmentFetchAction.NAME, "timed out"));
            } else {
                handler.handleResponse(new LanceFragmentFetchResponse(List.of(rendered("a-0", Float.NaN)), null));
            }
        };
        AtomicReference<Exception> failure = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        CountDownLatch reported = new CountDownLatch(1);
        CoordinatorFetchPhase.run(
            merged,
            name -> TARGET,
            HitProjection.NONE,
            sender,
            threadPool.generic(),
            threadPool.generic(),
            null,
            null,
            (node, request, cause) -> {
                incomplete.set(node);
                reported.countDown();
            },
            ActionListener.wrap(v -> latch.countDown(), e -> {
                failure.set(e);
                latch.countDown();
            })
        );
        Exception e = await(failure, latch);
        assertTrue(String.valueOf(e), e instanceof OpenSearchTimeoutException);
        assertTrue(e.getMessage(), e.getMessage().contains("node [b]"));
        assertTrue(reported.await(10, TimeUnit.SECONDS));
        assertEquals(NODE_B, incomplete.get());
        IllegalStateException unrendered = expectThrows(
            IllegalStateException.class,
            () -> merged.buildResponse(System.currentTimeMillis())
        );
        assertTrue(unrendered.getMessage(), unrendered.getMessage().contains("never rendered"));
    }

    public void testANodeThatRendersTheWrongNumberOfRowsFailsTheRequest() throws Exception {
        MergeReducer merged = reducer(List.of(), 0, 2);
        merged.absorbTarget("idx", List.of(deferred(new long[] { 0L, 1L }, new float[] { 2f, 1f }, null)), List.of(NODE_A), false);
        CoordinatorFetchPhase.Sender sender = (node, request, handler) -> handler.handleResponse(
            new LanceFragmentFetchResponse(List.of(rendered("a-0", Float.NaN)), null)
        );
        AtomicReference<Exception> failure = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        runRound(merged, sender, null, ActionListener.wrap(v -> latch.countDown(), e -> {
            failure.set(e);
            latch.countDown();
        }));
        Exception e = await(failure, latch);
        assertTrue(String.valueOf(e), e instanceof IllegalStateException);
        assertTrue(e.getMessage(), e.getMessage().contains("rendered 1 rows of the 2 asked"));
    }

    public void testAbsorbingDeferredHitsWithoutTheirNodeIsRefused() {
        MergeReducer merged = reducer(List.of(), 0, 2);
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> merged.absorbTarget("idx", List.of(deferred(new long[] { 0L }, new float[] { 1f }, null)), false)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("needs the node"));
    }

    public void testDeferFetchDecision() {
        HitProjection explain = new HitProjection(null, null, List.of(), List.of(), true);
        QueryBuilder match = new MatchAllQueryBuilder();
        List<SortBuilder<?>> noSort = List.of();
        assertTrue(TransportLanceCoordinatorAction.deferFetchCandidate(true, match, 0, 10, noSort, null, HitProjection.NONE));
        assertFalse(
            "setting off",
            TransportLanceCoordinatorAction.deferFetchCandidate(false, match, 0, 10, noSort, null, HitProjection.NONE)
        );
        assertFalse("size 0", TransportLanceCoordinatorAction.deferFetchCandidate(true, match, 0, 0, noSort, null, HitProjection.NONE));
        assertFalse(
            "collapse",
            TransportLanceCoordinatorAction.deferFetchCandidate(true, match, 0, 10, noSort, new CollapseBuilder("f"), HitProjection.NONE)
        );
        assertFalse("explain", TransportLanceCoordinatorAction.deferFetchCandidate(true, match, 0, 10, noSort, null, explain));

        assertTrue(TransportLanceCoordinatorAction.deferFetch(true, 2, false));
        assertTrue(TransportLanceCoordinatorAction.deferFetch(true, 3, false));
        assertFalse("one executor", TransportLanceCoordinatorAction.deferFetch(true, 1, false));
        assertFalse("reader wrapper", TransportLanceCoordinatorAction.deferFetch(true, 3, true));
        assertFalse("body refused", TransportLanceCoordinatorAction.deferFetch(false, 3, false));
    }

    /**
     * A body whose executors already return only the rows of the page is
     * rendered on the query round: a lone {@code lance_knn} with
     * {@code k} at most {@code from + size} in score order. Every
     * executor hands back the rows of the global top {@code k} in its own
     * fragments, so the round would render the same rows one round trip
     * later. The same knn inside a {@code bool}, under a field sort, or
     * with {@code k} above {@code from + size} defers.
     */
    public void testPlainKnnPageStaysOnTheQueryRound() {
        LanceKnnQueryBuilder knn = new LanceKnnQueryBuilder("embedding", new float[] { 1f, 0f }, 10);
        List<SortBuilder<?>> noSort = List.of();
        List<SortBuilder<?>> scoreSort = List.of(new ScoreSortBuilder());
        List<SortBuilder<?>> priceSort = List.of(new FieldSortBuilder("price").order(SortOrder.DESC));
        List<SortBuilder<?>> scoreThenPrice = List.of(new ScoreSortBuilder(), new FieldSortBuilder("price"));

        assertTrue(TransportLanceCoordinatorAction.executorsReturnOnlyKeptRows(knn, 0, 10, noSort));
        assertTrue("k below the page", TransportLanceCoordinatorAction.executorsReturnOnlyKeptRows(knn, 0, 20, noSort));
        assertTrue("from counts toward the page", TransportLanceCoordinatorAction.executorsReturnOnlyKeptRows(knn, 5, 5, noSort));
        assertTrue("_score sort is score order", TransportLanceCoordinatorAction.executorsReturnOnlyKeptRows(knn, 0, 10, scoreSort));
        assertFalse("k above the page", TransportLanceCoordinatorAction.executorsReturnOnlyKeptRows(knn, 0, 9, noSort));
        assertFalse("field sort", TransportLanceCoordinatorAction.executorsReturnOnlyKeptRows(knn, 0, 10, priceSort));
        assertFalse("field sort after _score", TransportLanceCoordinatorAction.executorsReturnOnlyKeptRows(knn, 0, 10, scoreThenPrice));
        assertFalse(
            "knn inside a bool",
            TransportLanceCoordinatorAction.executorsReturnOnlyKeptRows(new BoolQueryBuilder().must(knn), 0, 10, noSort)
        );
        assertFalse("not a knn", TransportLanceCoordinatorAction.executorsReturnOnlyKeptRows(new MatchAllQueryBuilder(), 0, 10, noSort));
        assertFalse("no query", TransportLanceCoordinatorAction.executorsReturnOnlyKeptRows(null, 0, 10, noSort));

        assertFalse(
            "plain knn page renders on the query round",
            TransportLanceCoordinatorAction.deferFetchCandidate(true, knn, 0, 10, noSort, null, HitProjection.NONE)
        );
        assertTrue(
            "knn inside a bool defers",
            TransportLanceCoordinatorAction.deferFetchCandidate(
                true,
                new BoolQueryBuilder().must(knn),
                0,
                10,
                noSort,
                null,
                HitProjection.NONE
            )
        );
        assertTrue(
            "knn under a field sort defers",
            TransportLanceCoordinatorAction.deferFetchCandidate(true, knn, 0, 10, priceSort, null, HitProjection.NONE)
        );
    }

    public void testProfileRendersTheCoordinatorBlock() throws Exception {
        LanceSearchProfile profile = new LanceSearchProfile();
        profile.record(NODE_A, new LanceFragmentQueryResponse.Profile(1L, 0L, 0L, 0L, 0L, 0L, 0L));
        profile.record(NODE_A, new LanceFragmentQueryResponse.Profile(0L, 2L, 1L, 4L, 3L, 8L, 0L));
        profile.fetchRoundTrip();
        MergeReducer merged = reducer(List.of(), 0, 1);
        SearchResponse response = profile.attachTo(merged.buildResponse(System.currentTimeMillis()), false);
        Map<String, Object> parsed = XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), response.toString(), false);
        @SuppressWarnings("unchecked")
        Map<String, Object> lance = (Map<String, Object>) ((Map<String, Object>) parsed.get("profile")).get("lance");
        @SuppressWarnings("unchecked")
        Map<String, Object> coordinator = (Map<String, Object>) lance.get("coordinator");
        assertEquals(1, coordinator.get("fetch_round_trips"));
        @SuppressWarnings("unchecked")
        Map<String, Object> nodeA = (Map<String, Object>) ((Map<String, Object>) lance.get("nodes")).get("a");
        @SuppressWarnings("unchecked")
        Map<String, Object> fetch = (Map<String, Object>) nodeA.get("fetch");
        assertEquals("the fetch round's figures add to the node's", 4, fetch.get("take_rows"));
        assertEquals(2, fetch.get("millis"));
    }
}
