/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.List;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.explain.ExplainAction;
import org.opensearch.action.explain.ExplainRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.action.termvectors.MultiTermVectorsAction;
import org.opensearch.action.termvectors.MultiTermVectorsRequest;
import org.opensearch.action.termvectors.TermVectorsAction;
import org.opensearch.action.termvectors.TermVectorsRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.index.Index;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.tasks.Task;

/**
 * ActionFilter that refuses, with 400, the read APIs a Lance backed index
 * does not serve: {@code _termvectors} ({@code indices:data/read/tv}),
 * {@code _mtermvectors} ({@code indices:data/read/mtv}) and
 * {@code _explain/<id>} ({@code indices:data/read/explain}). The stock
 * actions reach the shard's reader for them; a Lance leaf carries no
 * Lucene postings or term vectors, so there is nothing to report, and
 * the plan of a search body over the table is what
 * {@code GET /_plugins/_lance/explain/<index>} answers.
 *
 * <p>The refusal names the API and the index. A request whose targets are
 * all other indexes proceeds untouched; an {@code _mtermvectors} is refused
 * when any of its documents names a Lance backed index. An index
 * expression that does not resolve proceeds too, so the stock action
 * reports the error.
 */
public final class LanceUnsupportedReadActionFilter implements ActionFilter {

    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public LanceUnsupportedReadActionFilter(ClusterService clusterService, IndexNameExpressionResolver indexNameExpressionResolver) {
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    /** After the security plugin's filter ({@code Integer.MIN_VALUE}), with the other Lance filters. */
    @Override
    public int order() {
        return Integer.MIN_VALUE + 100;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionRequestMetadata<Request, Response> actionRequestMetadata,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        String api;
        List<? extends IndicesRequest> targets;
        if (TermVectorsAction.NAME.equals(action) && request instanceof TermVectorsRequest termVectors) {
            api = "_termvectors";
            targets = List.of(termVectors);
        } else if (MultiTermVectorsAction.NAME.equals(action) && request instanceof MultiTermVectorsRequest multi) {
            api = "_mtermvectors";
            targets = multi.getRequests();
        } else if (ExplainAction.NAME.equals(action) && request instanceof ExplainRequest explain) {
            api = "_explain";
            targets = List.of(explain);
        } else {
            chain.proceed(task, action, request, listener);
            return;
        }
        for (IndicesRequest target : targets) {
            String lanceIndex = lanceBackedIndex(target);
            if (lanceIndex != null) {
                listener.onFailure(new IllegalArgumentException(message(api, lanceIndex)));
                return;
            }
        }
        chain.proceed(task, action, request, listener);
    }

    /** The refusal's message: the API, the index, and for {@code _explain} the plugin's endpoint that explains a search body. */
    static String message(String api, String indexName) {
        String message = "[" + api + "] is not served by a Lance backed index [" + indexName + "]";
        if ("_explain".equals(api)) {
            message += "; GET /_plugins/_lance/explain/" + indexName + " explains how a search body runs on the table";
        }
        return message;
    }

    /**
     * The name of the Lance backed index {@code request} names, or
     * {@code null} when the expression does not resolve to one index or
     * that index is not Lance backed.
     */
    private String lanceBackedIndex(IndicesRequest request) {
        ClusterState state = clusterService.state();
        Index index;
        try {
            index = indexNameExpressionResolver.concreteSingleIndex(state, request);
        } catch (Exception e) {
            return null;
        }
        IndexMetadata metadata = state.metadata().index(index);
        if (metadata == null) {
            return null;
        }
        String table = LanceEngineFactory.tableOf(metadata.getSettings());
        return table == null || table.isEmpty() ? null : index.getName();
    }
}
