/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.lance.LanceInternalHeaders;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;

/**
 * ActionFilter that rejects {@code indices:admin/create} requests
 * carrying {@code index.lance.table} in their settings unless the
 * request originates inside the plugin (attach or namespace
 * surface). Users who tried to bypass {@code POST /_lance/attach}
 * by sending {@code PUT /{index} {settings:{index.lance.table:...}}}
 * used to end up with a half-broken index: the engine wired up
 * without the derive step running, so mapping stayed empty and
 * every typed query failed with "No mapping found" while {@code
 * _count} returned the Lance metadata count. Rejecting up front
 * points the caller at the correct entry point instead of leaving
 * behind an inconsistent index that only surfaces the mistake on
 * the next {@code _search}.
 *
 * <p>Plugin-internal callers stamp {@link
 * LanceInternalHeaders#LANCE_INTERNAL_CREATE_INDEX} on their
 * ThreadContext before calling {@code client.admin().indices().create(...)}
 * so this filter can tell them apart from user-facing requests.
 */
public final class LanceCreateIndexActionFilter implements ActionFilter {

    /** Transport action name for {@code CreateIndexAction}. */
    private static final String CREATE_INDEX_ACTION_NAME = "indices:admin/create";

    private final ThreadPool threadPool;

    public LanceCreateIndexActionFilter(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    @Override
    public int order() {
        // Reject before other filters get to touch the request so
        // failures do not depend on the ordering of unrelated
        // security or auditing filters. Integer.MIN_VALUE is
        // reserved for security, keep some headroom.
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
        if (!CREATE_INDEX_ACTION_NAME.equals(action) || !(request instanceof CreateIndexRequest createRequest)) {
            chain.proceed(task, action, request, listener);
            return;
        }
        String lanceTable = createRequest.settings().get(LanceEngineFactory.TABLE_SETTING);
        if (lanceTable == null || lanceTable.isEmpty()) {
            chain.proceed(task, action, request, listener);
            return;
        }
        ThreadContext threadContext = threadPool.getThreadContext();
        if ("true".equals(threadContext.getHeader(LanceInternalHeaders.LANCE_INTERNAL_CREATE_INDEX))) {
            chain.proceed(task, action, request, listener);
            return;
        }
        listener.onFailure(
            new IllegalArgumentException(
                "["
                    + LanceEngineFactory.TABLE_SETTING
                    + "] cannot be set through PUT /{index}; use POST /_lance/attach so the mapping is derived from the Lance schema"
            )
        );
    }
}
