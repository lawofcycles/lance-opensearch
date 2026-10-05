/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.get.MultiGetAction;
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.index.Index;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.query.LanceInvalidInput;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

/**
 * ActionFilter that answers {@code indices:data/read/get} and
 * {@code indices:data/read/mget} on a Lance backed index from the table,
 * before the stock actions resolve a shard. The lookup itself is
 * {@link LancePointLookup}; this class decides which requests it serves
 * and runs it off the transport thread.
 *
 * <p>A {@code GET /<index>/_doc/<id>} whose index is Lance backed is
 * answered whole. An {@code _mget} is answered item by item: the items
 * whose index is Lance backed are looked up here, each failure becoming
 * that item's failure as the stock shard action reports one, and the
 * other items are sent as one {@code _mget} of their own through the
 * stock action, whose answers take their places in the response. A
 * request without a Lance backed target proceeds untouched, as does one
 * whose index expression does not resolve (the stock action reports the
 * error).
 *
 * <p>The filter runs on the transport worker that received the request,
 * so the lookup (a manifest read, a Lance scan, a row take) is forked onto
 * the plugin's {@code lance_coordinator} pool as the search dispatch is;
 * a rejection by the pool fails the request with its
 * {@code OpenSearchRejectedExecutionException} (HTTP 429).
 *
 * <p>The filter never calls {@code chain.proceed} for a Lance backed GET,
 * so {@code TransportGetAction} and the shard engine's {@code get} are not
 * reached for it; {@code realtime}, {@code refresh}, {@code preference}
 * and {@code version} have no effect on a table that is read at the
 * version the index follows, per request, and are ignored.
 */
public final class LanceGetActionFilter implements ActionFilter {

    private static final Logger LOGGER = LogManager.getLogger(LanceGetActionFilter.class);

    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Client client;
    private final ThreadPool threadPool;
    private final LancePointLookup lookup;

    public LanceGetActionFilter(
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client,
        ThreadPool threadPool,
        LancePointLookup lookup
    ) {
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.client = client;
        this.threadPool = threadPool;
        this.lookup = lookup;
    }

    /** After the security plugin's filter ({@code Integer.MIN_VALUE}), with {@link LanceDispatchActionFilter}. */
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
        if (GetAction.NAME.equals(action) && request instanceof GetRequest get) {
            IndexMetadata index = lanceBackedIndex(get);
            if (index == null) {
                chain.proceed(task, action, request, listener);
                return;
            }
            @SuppressWarnings("unchecked")
            ActionListener<GetResponse> typed = (ActionListener<GetResponse>) listener;
            fork(
                "get [" + get.id() + "] on [" + index.getIndex().getName() + "]",
                typed,
                () -> new GetResponse(lookup.lookup(index, get.id(), get.storedFields(), get.fetchSourceContext()))
            );
            return;
        }
        if (MultiGetAction.NAME.equals(action) && request instanceof MultiGetRequest mget) {
            List<MultiGetRequest.Item> items = mget.getItems();
            IndexMetadata[] lanceIndexes = new IndexMetadata[items.size()];
            boolean anyLance = false;
            for (int i = 0; i < items.size(); i++) {
                lanceIndexes[i] = lanceBackedIndex(items.get(i));
                anyLance |= lanceIndexes[i] != null;
            }
            if (!anyLance) {
                chain.proceed(task, action, request, listener);
                return;
            }
            @SuppressWarnings("unchecked")
            ActionListener<MultiGetResponse> typed = (ActionListener<MultiGetResponse>) listener;
            fork("mget of " + items.size() + " items", typed, () -> {
                multiGet(task, mget, lanceIndexes, typed);
                return null;
            });
            return;
        }
        chain.proceed(task, action, request, listener);
    }

    /**
     * Answers the Lance backed items of {@code mget} and hands the rest to
     * the stock action as one request, completing {@code listener} once
     * every item has its answer.
     */
    private void multiGet(Task task, MultiGetRequest mget, IndexMetadata[] lanceIndexes, ActionListener<MultiGetResponse> listener) {
        List<MultiGetRequest.Item> items = mget.getItems();
        MultiGetItemResponse[] responses = new MultiGetItemResponse[items.size()];
        MultiGetRequest rest = null;
        List<Integer> restPositions = new ArrayList<>();
        for (int i = 0; i < items.size(); i++) {
            MultiGetRequest.Item item = items.get(i);
            IndexMetadata index = lanceIndexes[i];
            if (index == null) {
                if (rest == null) {
                    rest = new MultiGetRequest().preference(mget.preference()).realtime(mget.realtime()).refresh(mget.refresh());
                    rest.setParentTask(clusterService.localNode().getId(), task.getId());
                }
                rest.add(item);
                restPositions.add(i);
                continue;
            }
            String indexName = index.getIndex().getName();
            try {
                responses[i] = new MultiGetItemResponse(
                    new GetResponse(lookup.lookup(index, item.id(), item.storedFields(), item.fetchSourceContext())),
                    null
                );
            } catch (Exception e) {
                LOGGER.debug("mget item [{}] on [{}] failed", item.id(), indexName, e);
                responses[i] = new MultiGetItemResponse(null, new MultiGetResponse.Failure(indexName, item.id(), reported(e)));
            }
        }
        if (rest == null) {
            listener.onResponse(new MultiGetResponse(responses));
            return;
        }
        client.execute(MultiGetAction.INSTANCE, rest, ActionListener.wrap(stock -> {
            MultiGetItemResponse[] stockResponses = stock.getResponses();
            for (int j = 0; j < restPositions.size(); j++) {
                responses[restPositions.get(j)] = stockResponses[j];
            }
            listener.onResponse(new MultiGetResponse(responses));
        }, e -> listener.onFailure(reported(e))));
    }

    /**
     * Runs {@code body} on the {@code lance_coordinator} pool and completes
     * {@code listener} with its result; a null result means {@code body}
     * completes the listener itself. A rejection or a failure completes
     * the listener with the exception.
     */
    private <T> void fork(String description, ActionListener<T> listener, CheckedSupplier<T, Exception> body) {
        AbstractRunnable entry = new AbstractRunnable() {
            @Override
            protected void doRun() throws Exception {
                T result = body.get();
                if (result != null) {
                    listener.onResponse(result);
                }
            }

            @Override
            public void onRejection(Exception e) {
                LOGGER.warn("lance {} rejected by the coordinator pool; returning 429: {}", description, e.getMessage());
                listener.onFailure(e);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(reported(e));
            }

            @Override
            public String toString() {
                return "lance " + description;
            }
        };
        try {
            threadPool.executor(LancePlugin.LANCE_COORDINATOR_THREAD_POOL).execute(entry);
        } catch (Exception e) {
            // A rejection reaches entry.onRejection inside execute; what
            // execute throws itself means the runnable never ran, so the
            // listener is still open.
            listener.onFailure(e);
        }
    }

    /**
     * The exception the client sees: Lance's invalid input keeps its own
     * class so the status is 400, and an object store message that echoes
     * a credential is redacted.
     */
    private static Exception reported(Exception e) {
        return StorageOptions.redactCredentials(LanceInvalidInput.unwrap(e));
    }

    /**
     * The metadata of the one Lance backed index {@code request} names, or
     * {@code null} when the expression does not resolve to one index or
     * that index is not Lance backed (judged by
     * {@link LanceEngineFactory#TABLE_SETTING}, as the dispatch filter
     * judges a search target).
     */
    IndexMetadata lanceBackedIndex(IndicesRequest request) {
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
        return table == null || table.isEmpty() ? null : metadata;
    }
}
