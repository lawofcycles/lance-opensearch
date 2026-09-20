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
 * surface). A bare {@code PUT /{index} {settings:{index.lance.table:...}}}
 * would wire up the engine without the derive step, leaving the mapping
 * empty so every typed query fails with "No mapping found" while
 * {@code _count} returns the Lance metadata count. Rejecting up front
 * points the caller at {@code POST /_lance/attach} instead.
 *
 * <p>Plugin-internal callers stamp {@link
 * LanceInternalHeaders#LANCE_INTERNAL_CREATE_INDEX} on their
 * ThreadContext before calling {@code client.admin().indices().create(...)}
 * so this filter can tell them apart from user-facing requests.
 *
 * <p>The header check assumes the create request is executed on the
 * node that stamped the header. That holds because both internal
 * callers already run on the elected cluster manager: attach is a
 * cluster-manager routed transport action, and the namespace poll only
 * runs on the manager. The create therefore never crosses transport
 * before it reaches this filter. A security plugin stashes the
 * ThreadContext on every outbound transport request and copies only
 * the headers it knows, so a create that had to be forwarded to the
 * manager would arrive here without the header and be rejected.
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
