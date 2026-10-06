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
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.tasks.Task;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

/**
 * {@link LanceDispatchActionFilter} refuses a search whose resolved
 * targets put a Lance backed index next to one that is not Lance backed
 * with an {@link IllegalArgumentException} that names the Lance backed
 * index, whether the targets are listed, matched by a pattern or reached
 * through an alias, and lets a search over indexes that are not Lance
 * backed proceed to the stock action.
 */
public class LanceDispatchActionFilterTests extends OpenSearchTestCase {

    private TestThreadPool threadPool;
    private ClusterService clusterService;
    private LanceDispatchActionFilter filter;
    private final List<String> proceeded = new ArrayList<>();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        ClusterState state = ClusterState.builder(clusterService.state())
            .metadata(
                Metadata.builder(clusterService.state().metadata())
                    .put(
                        indexMetadata("lance-a", "uuid-a", Settings.builder().put(LanceEngineFactory.TABLE_SETTING, "/tables/a.lance"))
                            .putAlias(AliasMetadata.builder("both").build())
                    )
                    .put(indexMetadata("lance-b", "uuid-b", Settings.builder().put(LanceEngineFactory.TABLE_SETTING, "/tables/b.lance")))
                    .put(indexMetadata("plain-a", "uuid-p", Settings.builder()).putAlias(AliasMetadata.builder("both").build()))
                    .put(indexMetadata("plain-b", "uuid-q", Settings.builder()))
            )
            .build();
        ClusterServiceUtils.setState(clusterService, state);
        // The refusing and the proceeding branches never reach the
        // client, which only the fragment path delegation uses.
        filter = new LanceDispatchActionFilter(
            clusterService,
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            null,
            threadPool
        );
    }

    @Override
    public void tearDown() throws Exception {
        clusterService.close();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    public void testAListedLanceBackedIndexNextToAPlainOneIsRefused() {
        IllegalArgumentException refused = refusal(new SearchRequest("lance-a", "plain-a"));
        assertEquals(
            "cannot search Lance backed index [lance-a] together with a target that is not Lance backed; "
                + "run the request against the Lance backed target alone, or split the targets",
            refused.getMessage()
        );
        assertTrue(proceeded.isEmpty());
    }

    public void testAPatternThatExpandsToBothKindsIsRefusedNamingEveryLanceBackedIndex() {
        IllegalArgumentException refused = refusal(new SearchRequest("lance-*", "plain-*"));
        assertTrue(
            refused.getMessage(),
            refused.getMessage().startsWith("cannot search Lance backed indexes [lance-a, lance-b] together with")
        );
        // No index at all expands to every index of the cluster.
        IllegalArgumentException everyIndex = refusal(new SearchRequest());
        assertEquals(refused.getMessage(), everyIndex.getMessage());
        assertTrue(proceeded.isEmpty());
    }

    public void testAnAliasThatSpansBothKindsIsRefused() {
        IllegalArgumentException refused = refusal(new SearchRequest("both"));
        assertTrue(refused.getMessage(), refused.getMessage().startsWith("cannot search Lance backed index [lance-a] together with"));
        assertTrue(proceeded.isEmpty());
    }

    public void testASearchOverPlainIndexesProceeds() {
        apply(new SearchRequest("plain-a", "plain-b"));
        apply(new SearchRequest("plain-*"));
        assertEquals(List.of(SearchAction.NAME, SearchAction.NAME), proceeded);
    }

    public void testASearchOverAnIndexTheClusterDoesNotHaveProceedsSoTheStockActionReportsIt() {
        apply(new SearchRequest("lance-a", "missing"));
        assertEquals(List.of(SearchAction.NAME), proceeded);
    }

    public void testOtherActionsProceedWhateverTheIndex() {
        PlainActionFuture<ActionResponse> future = PlainActionFuture.newFuture();
        filter.apply(
            task("indices:data/read/count"),
            "indices:data/read/count",
            new SearchRequest("lance-a", "plain-a"),
            null,
            future,
            chain()
        );
        assertEquals(List.of("indices:data/read/count"), proceeded);
    }

    private IllegalArgumentException refusal(SearchRequest request) {
        PlainActionFuture<ActionResponse> future = apply(request);
        return expectThrows(IllegalArgumentException.class, () -> future.actionGet(10, TimeUnit.SECONDS));
    }

    private PlainActionFuture<ActionResponse> apply(SearchRequest request) {
        PlainActionFuture<ActionResponse> future = PlainActionFuture.newFuture();
        filter.apply(task(SearchAction.NAME), SearchAction.NAME, request, null, future, chain());
        return future;
    }

    private ActionFilterChain<SearchRequest, ActionResponse> chain() {
        return (t, name, r, listener) -> {
            proceeded.add(name);
            listener.onResponse(null);
        };
    }

    private static Task task(String action) {
        return new Task(1L, "transport", action, "", TaskId.EMPTY_TASK_ID, Map.of());
    }

    private static IndexMetadata.Builder indexMetadata(String name, String uuid, Settings.Builder extra) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, uuid)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(extra.build())
            );
    }
}
