/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Serves {@link LanceNamespaceListAction}. Running the listing behind
 * a transport action puts it behind the {@link ActionFilters} chain,
 * so a security plugin evaluates the caller's privileges before any
 * namespace name or table name leaves the node.
 *
 * <p>The root listing is a local cluster-state read and completes on
 * the calling thread. The table listing goes to the namespace's
 * storage (a directory listing, or an object-store call for
 * {@code s3://} roots), so it is handed to the generic pool rather
 * than blocking the transport thread the REST layer called from.
 */
public final class TransportLanceNamespaceListAction extends HandledTransportAction<LanceNamespaceListRequest, LanceNamespaceListResponse> {

    private final ThreadPool threadPool;
    private final LanceNamespaceService namespaceService;

    @Inject
    public TransportLanceNamespaceListAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        LanceNamespaceService namespaceService
    ) {
        super(LanceNamespaceListAction.NAME, transportService, actionFilters, LanceNamespaceListRequest::new, ThreadPool.Names.GENERIC);
        this.threadPool = threadPool;
        this.namespaceService = namespaceService;
    }

    @Override
    protected void doExecute(Task task, LanceNamespaceListRequest request, ActionListener<LanceNamespaceListResponse> listener) {
        String identifier = request.path();
        if (identifier == null) {
            listener.onResponse(LanceNamespaceListResponse.namespaces(namespaceService.namespaceInfos()));
            return;
        }
        threadPool.executor(ThreadPool.Names.GENERIC).execute(ActionRunnable.wrap(listener, l -> l.onResponse(listTables(identifier))));
    }

    private LanceNamespaceListResponse listTables(String identifier) {
        Optional<Set<String>> tables;
        try {
            tables = namespaceService.listTables(identifier);
        } catch (Exception e) {
            throw new OpenSearchStatusException(
                "list tables for [" + identifier + "] failed: " + e.getMessage(),
                RestStatus.INTERNAL_SERVER_ERROR,
                e
            );
        }
        if (tables.isEmpty()) {
            return LanceNamespaceListResponse.unregistered(identifier);
        }
        List<String> sorted = new ArrayList<>(tables.get());
        Collections.sort(sorted);
        return LanceNamespaceListResponse.tables(identifier, sorted);
    }
}
