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
import org.opensearch.action.explain.ExplainAction;
import org.opensearch.action.explain.ExplainRequest;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.termvectors.MultiTermVectorsAction;
import org.opensearch.action.termvectors.MultiTermVectorsRequest;
import org.opensearch.action.termvectors.TermVectorsAction;
import org.opensearch.action.termvectors.TermVectorsRequest;
import org.opensearch.cluster.ClusterState;
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
 * {@link LanceUnsupportedReadActionFilter} refuses {@code _termvectors},
 * {@code _mtermvectors} and {@code _explain} on a Lance backed index with
 * an {@link IllegalArgumentException} that names the API and the index,
 * and lets the same actions on other indexes, and every other action,
 * proceed.
 */
public class LanceUnsupportedReadActionFilterTests extends OpenSearchTestCase {

    private TestThreadPool threadPool;
    private ClusterService clusterService;
    private LanceUnsupportedReadActionFilter filter;
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
                        indexMetadata("lance", "uuid-l", Settings.builder().put(LanceEngineFactory.TABLE_SETTING, "/tables/demo.lance")),
                        false
                    )
                    .put(indexMetadata("plain", "uuid-p", Settings.builder()), false)
            )
            .build();
        ClusterServiceUtils.setState(clusterService, state);
        filter = new LanceUnsupportedReadActionFilter(clusterService, new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)));
    }

    @Override
    public void tearDown() throws Exception {
        clusterService.close();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    public void testOrderSitsWithTheOtherLanceFilters() {
        assertEquals(Integer.MIN_VALUE + 100, filter.order());
    }

    public void testTermVectorsOnALanceBackedIndexIsRefused() {
        IllegalArgumentException refused = refusal(TermVectorsAction.NAME, new TermVectorsRequest("lance", "1"));
        assertEquals("[_termvectors] is not served by a Lance backed index [lance]", refused.getMessage());
        assertTrue(proceeded.isEmpty());
    }

    public void testMultiTermVectorsWithALanceBackedDocumentIsRefused() {
        MultiTermVectorsRequest request = new MultiTermVectorsRequest();
        request.add(new TermVectorsRequest("plain", "1"));
        request.add(new TermVectorsRequest("lance", "2"));
        IllegalArgumentException refused = refusal(MultiTermVectorsAction.NAME, request);
        assertEquals("[_mtermvectors] is not served by a Lance backed index [lance]", refused.getMessage());
        assertTrue(proceeded.isEmpty());
    }

    public void testExplainOnALanceBackedIndexIsRefusedAndNamesThePluginsExplain() {
        IllegalArgumentException refused = refusal(ExplainAction.NAME, new ExplainRequest("lance", "1"));
        assertEquals(
            "[_explain] is not served by a Lance backed index [lance]; GET /_plugins/_lance/explain/lance explains how a search body runs on the table",
            refused.getMessage()
        );
        assertTrue(proceeded.isEmpty());
    }

    public void testTheSameActionsOnOtherIndexesProceed() {
        apply(TermVectorsAction.NAME, new TermVectorsRequest("plain", "1"));
        apply(ExplainAction.NAME, new ExplainRequest("plain", "1"));
        MultiTermVectorsRequest multi = new MultiTermVectorsRequest();
        multi.add(new TermVectorsRequest("plain", "1"));
        apply(MultiTermVectorsAction.NAME, multi);
        // An index the cluster does not have: the stock action reports it.
        apply(TermVectorsAction.NAME, new TermVectorsRequest("missing", "1"));
        assertEquals(List.of(TermVectorsAction.NAME, ExplainAction.NAME, MultiTermVectorsAction.NAME, TermVectorsAction.NAME), proceeded);
    }

    public void testOtherActionsProceedWhateverTheIndex() {
        apply("indices:data/read/get", new TermVectorsRequest("lance", "1"));
        assertEquals(List.of("indices:data/read/get"), proceeded);
    }

    private IllegalArgumentException refusal(String action, ActionRequest request) {
        PlainActionFuture<ActionResponse> future = apply(action, request);
        return expectThrows(IllegalArgumentException.class, () -> future.actionGet(10, TimeUnit.SECONDS));
    }

    private PlainActionFuture<ActionResponse> apply(String action, ActionRequest request) {
        PlainActionFuture<ActionResponse> future = PlainActionFuture.newFuture();
        ActionFilterChain<ActionRequest, ActionResponse> chain = (t, name, r, listener) -> {
            proceeded.add(name);
            listener.onResponse(null);
        };
        filter.apply(new Task(1L, "transport", action, "", TaskId.EMPTY_TASK_ID, Map.of()), action, request, null, future, chain);
        return future;
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
}
