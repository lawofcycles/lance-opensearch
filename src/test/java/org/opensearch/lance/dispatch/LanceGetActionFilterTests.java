/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.opensearch.Version;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.get.MultiGetAction;
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.index.get.GetResult;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.tasks.Task;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

/**
 * {@link LanceGetActionFilter} sits with the dispatch filter, lets a GET
 * or an {@code _mget} without a Lance backed target proceed to the stock
 * action, and answers a Lance backed target itself without proceeding;
 * {@link LancePointLookup}'s key filter spells the id for each primary
 * key type and refuses the ids no row can match.
 */
public class LanceGetActionFilterTests extends OpenSearchTestCase {

    private TestThreadPool threadPool;
    private ClusterService clusterService;
    private StockClient client;
    private LanceGetActionFilter filter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(
            getTestName(),
            new FixedExecutorBuilder(
                Settings.EMPTY,
                LancePlugin.LANCE_COORDINATOR_THREAD_POOL,
                1,
                16,
                "thread_pool." + LancePlugin.LANCE_COORDINATOR_THREAD_POOL
            )
        );
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        ClusterState state = ClusterState.builder(clusterService.state())
            .metadata(
                Metadata.builder(clusterService.state().metadata())
                    .put(
                        indexMetadata("lance", "uuid-l", Settings.builder().put(LanceEngineFactory.TABLE_SETTING, "/tables/demo.lance")),
                        false
                    )
                    .put(indexMetadata("plain", "uuid-p", Settings.builder()), false)
            )
            .build();
        ClusterServiceUtils.setState(clusterService, state);
        client = new StockClient(threadPool);
        // No IndicesService and no cache: the lookups these tests reach
        // stop before either is needed (the index declares no primary key).
        filter = new LanceGetActionFilter(
            clusterService,
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            client,
            threadPool,
            new LancePointLookup(() -> null, null)
        );
    }

    @Override
    public void tearDown() throws Exception {
        clusterService.close();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    public void testOrderSitsWithTheDispatchFilter() {
        assertEquals(Integer.MIN_VALUE + 100, filter.order());
    }

    public void testGetOnAnIndexThatIsNotLanceBackedProceeds() throws Exception {
        List<String> proceeded = new ArrayList<>();
        PlainActionFuture<GetResponse> future = PlainActionFuture.newFuture();
        ActionFilterChain<GetRequest, GetResponse> chain = (t, action, request, listener) -> {
            proceeded.add(action + " " + request.index());
            listener.onResponse(null);
        };
        filter.apply(task(GetAction.NAME), GetAction.NAME, new GetRequest("plain", "1"), null, future, chain);
        assertEquals(List.of(GetAction.NAME + " plain"), proceeded);
        assertNull(future.actionGet(10, TimeUnit.SECONDS));
    }

    public void testGetOnAnIndexThatDoesNotExistProceeds() throws Exception {
        // The stock action reports the missing index.
        List<String> proceeded = new ArrayList<>();
        PlainActionFuture<GetResponse> future = PlainActionFuture.newFuture();
        ActionFilterChain<GetRequest, GetResponse> chain = (t, action, request, listener) -> {
            proceeded.add(request.index());
            listener.onResponse(null);
        };
        filter.apply(task(GetAction.NAME), GetAction.NAME, new GetRequest("missing", "1"), null, future, chain);
        assertEquals(List.of("missing"), proceeded);
        future.actionGet(10, TimeUnit.SECONDS);
    }

    public void testGetOnALanceBackedIndexIsAnsweredWithoutProceeding() throws Exception {
        // The index declares no primary key, so the lookup answers
        // found: false; the chain is never entered, so the stock action
        // and the shard engine behind it are not reached.
        PlainActionFuture<GetResponse> future = PlainActionFuture.newFuture();
        ActionFilterChain<GetRequest, GetResponse> chain = (t, action, request, listener) -> fail(
            "a GET on a Lance backed index must not proceed to the stock action"
        );
        filter.apply(task(GetAction.NAME), GetAction.NAME, new GetRequest("lance", "7"), null, future, chain);
        GetResponse response = future.actionGet(10, TimeUnit.SECONDS);
        assertFalse(response.isExists());
        assertEquals("lance", response.getIndex());
        assertEquals("7", response.getId());
        assertEquals(-1L, response.getVersion());
    }

    public void testMultiGetWithoutALanceBackedItemProceeds() throws Exception {
        List<String> proceeded = new ArrayList<>();
        PlainActionFuture<MultiGetResponse> future = PlainActionFuture.newFuture();
        ActionFilterChain<MultiGetRequest, MultiGetResponse> chain = (t, action, request, listener) -> {
            proceeded.add(action + " " + request.getItems().size());
            listener.onResponse(null);
        };
        MultiGetRequest request = new MultiGetRequest().add("plain", "1").add("plain", "2");
        filter.apply(task(MultiGetAction.NAME), MultiGetAction.NAME, request, null, future, chain);
        assertEquals(List.of(MultiGetAction.NAME + " 2"), proceeded);
        future.actionGet(10, TimeUnit.SECONDS);
        assertTrue("nothing was sent through the client", client.requests.isEmpty());
    }

    public void testMultiGetAnswersTheLanceItemsAndSendsTheRestAsOneRequest() throws Exception {
        // Three items, the middle one on the plain index: the Lance items
        // are answered here (found: false, no primary key), the plain one
        // goes to the stock action as an _mget of its own, and the three
        // answers keep the request's order.
        PlainActionFuture<MultiGetResponse> future = PlainActionFuture.newFuture();
        ActionFilterChain<MultiGetRequest, MultiGetResponse> chain = (t, action, request, listener) -> fail(
            "an _mget with a Lance backed item must not proceed whole"
        );
        MultiGetRequest request = new MultiGetRequest().add("lance", "a").add("plain", "b").add("lance", "c");
        request.preference("_local");
        filter.apply(task(MultiGetAction.NAME), MultiGetAction.NAME, request, null, future, chain);
        MultiGetResponse response = future.actionGet(10, TimeUnit.SECONDS);

        MultiGetItemResponse[] items = response.getResponses();
        assertEquals(3, items.length);
        assertEquals("lance", items[0].getIndex());
        assertEquals("a", items[0].getId());
        assertFalse(items[0].getResponse().isExists());
        assertEquals("plain", items[1].getIndex());
        assertEquals("b", items[1].getId());
        assertTrue("the stock answer took the plain item's place", items[1].getResponse().isExists());
        assertEquals("lance", items[2].getIndex());
        assertEquals("c", items[2].getId());
        assertFalse(items[2].getResponse().isExists());

        assertEquals(1, client.requests.size());
        MultiGetRequest sent = (MultiGetRequest) client.requests.get(0);
        assertEquals(1, sent.getItems().size());
        assertEquals("plain", sent.getItems().get(0).index());
        assertEquals("b", sent.getItems().get(0).id());
        assertEquals("_local", sent.preference());
    }

    public void testOtherActionsProceed() throws Exception {
        List<String> proceeded = new ArrayList<>();
        PlainActionFuture<ActionResponse> future = PlainActionFuture.newFuture();
        ActionFilterChain<ActionRequest, ActionResponse> chain = (t, action, request, listener) -> {
            proceeded.add(action);
            listener.onResponse(null);
        };
        filter.apply(task("indices:data/read/search"), "indices:data/read/search", new GetRequest("lance", "1"), null, future, chain);
        assertEquals(List.of("indices:data/read/search"), proceeded);
    }

    public void testKeyFilterSpellsTheIdForEachKeyType() {
        assertEquals("id = 42", LancePointLookup.keyFilter("id", LancePrimaryKeyType.LONG, "42"));
        assertEquals("id = -1", LancePointLookup.keyFilter("id", LancePrimaryKeyType.LONG, "-1"));
        assertNull("not a long", LancePointLookup.keyFilter("id", LancePrimaryKeyType.LONG, "alpha"));
        assertNull("too wide for a long", LancePointLookup.keyFilter("id", LancePrimaryKeyType.LONG, "99999999999999999999"));

        assertEquals(
            "id = 18446744073709551610",
            LancePointLookup.keyFilter("id", LancePrimaryKeyType.UNSIGNED_LONG, "18446744073709551610")
        );
        assertNull("negative", LancePointLookup.keyFilter("id", LancePrimaryKeyType.UNSIGNED_LONG, "-1"));
        assertNull("above 2^64", LancePointLookup.keyFilter("id", LancePrimaryKeyType.UNSIGNED_LONG, "99999999999999999999"));
        assertNull("not a number", LancePointLookup.keyFilter("id", LancePrimaryKeyType.UNSIGNED_LONG, "alpha"));

        assertEquals("key = 'alpha-2'", LancePointLookup.keyFilter("key", LancePrimaryKeyType.KEYWORD, "alpha-2"));
        assertEquals("the quote is doubled", "key = 'o''brien'", LancePointLookup.keyFilter("key", LancePrimaryKeyType.KEYWORD, "o'brien"));
        assertNull("an empty id", LancePointLookup.keyFilter("key", LancePrimaryKeyType.KEYWORD, ""));

        assertNull("no key declared", LancePointLookup.keyFilter("", LancePrimaryKeyType.KEYWORD, "x"));
        assertNull("no key type", LancePointLookup.keyFilter("id", LancePrimaryKeyType.NONE, "1"));
    }

    public void testNormalizeFetchSourceFollowsTheStockGetPath() {
        FetchSourceContext own = new FetchSourceContext(true, new String[] { "a" }, new String[0]);
        assertSame(own, LancePointLookup.normalizeFetchSource(own, new String[] { "b" }));
        assertSame(FetchSourceContext.FETCH_SOURCE, LancePointLookup.normalizeFetchSource(null, null));
        assertSame(FetchSourceContext.FETCH_SOURCE, LancePointLookup.normalizeFetchSource(null, new String[] { "a", "_source" }));
        assertSame(FetchSourceContext.DO_NOT_FETCH_SOURCE, LancePointLookup.normalizeFetchSource(null, new String[] { "a" }));
    }

    public void testNotFoundCarriesNoVersionOrSequenceNumber() {
        GetResult absent = LancePointLookup.notFound("lance", "9");
        assertFalse(absent.isExists());
        assertEquals("lance", absent.getIndex());
        assertEquals("9", absent.getId());
        assertEquals(-1L, absent.getVersion());
        assertNull(absent.internalSourceRef());
    }

    private static Task task(String action) {
        return new Task(1L, "transport", action, "", TaskId.EMPTY_TASK_ID, Map.of());
    }

    private static IndexMetadata indexMetadata(String name, String uuid, Settings.Builder extra) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, uuid)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(extra.build())
            )
            .build();
    }

    /** Stands in for the stock {@code _mget}: answers every item as found and records the requests it saw. */
    private static final class StockClient extends NoOpClient {
        final List<ActionRequest> requests = new ArrayList<>();

        StockClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            requests.add(request);
            if (request instanceof MultiGetRequest mget) {
                MultiGetItemResponse[] items = new MultiGetItemResponse[mget.getItems().size()];
                for (int i = 0; i < items.length; i++) {
                    MultiGetRequest.Item item = mget.getItems().get(i);
                    items[i] = new MultiGetItemResponse(
                        new GetResponse(new GetResult(item.index(), item.id(), 0L, 1L, 1L, true, null, null, null)),
                        null
                    );
                }
                listener.onResponse((Response) new MultiGetResponse(items));
                return;
            }
            super.doExecute(action, request, listener);
        }
    }
}
