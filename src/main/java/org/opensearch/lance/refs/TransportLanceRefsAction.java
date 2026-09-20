/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.refs;

import java.util.ArrayList;
import java.util.List;

import org.lance.Branch;
import org.lance.Dataset;
import org.lance.Tag;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Serves {@link LanceRefsAction}: resolves the Lance table behind the
 * index from cluster state, opens it at the latest version, and lists its
 * tags ({@code Dataset.tags().list()}) and branches
 * ({@code Dataset.branches().list()}). Running this behind a transport
 * action places it after the {@link ActionFilters} chain, so a security
 * plugin rejects a caller without {@code indices:monitor/lance/refs} on
 * the index before the plugin opens the table.
 *
 * <p>Threading: {@link Dataset#open} blocks on native I/O, so
 * {@link #doExecute} hands the listing to the generic pool. The cluster
 * state read that resolves the table is a local lookup and runs before the
 * hand-off; a missing index is a 404 and a non-Lance index a 400 from
 * there.
 */
public final class TransportLanceRefsAction extends HandledTransportAction<LanceRefsRequest, LanceRefsResponse> {

    private final ThreadPool threadPool;
    private final ClusterService clusterService;

    @Inject
    public TransportLanceRefsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        ClusterService clusterService
    ) {
        super(LanceRefsAction.NAME, transportService, actionFilters, LanceRefsRequest::new, ThreadPool.Names.GENERIC);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, LanceRefsRequest request, ActionListener<LanceRefsResponse> listener) {
        String indexName = request.index();
        IndexMetadata metadata = clusterService.state().metadata().index(indexName);
        if (metadata == null) {
            listener.onFailure(new IndexNotFoundException(indexName));
            return;
        }
        String tableUri = metadata.getSettings().get(LanceEngineFactory.TABLE_SETTING);
        if (tableUri == null) {
            listener.onFailure(new IllegalArgumentException("index " + indexName + " is not a Lance index"));
            return;
        }
        StorageOptions storageOptions = StorageOptions.fromIndexSettings(metadata.getSettings());
        threadPool.executor(ThreadPool.Names.GENERIC)
            .execute(ActionRunnable.supply(listener, () -> listRefs(indexName, tableUri, storageOptions)));
    }

    private static LanceRefsResponse listRefs(String indexName, String tableUri, StorageOptions storageOptions) {
        List<LanceRefsResponse.Tag> tags = new ArrayList<>();
        List<String> branches = new ArrayList<>();
        try (Dataset dataset = LanceRegistry.openDataset(tableUri, storageOptions)) {
            for (Tag tag : dataset.tags().list()) {
                tags.add(new LanceRefsResponse.Tag(tag.getName(), tag.getVersion()));
            }
            for (Branch branch : dataset.branches().list()) {
                branches.add(branch.getName());
            }
        }
        return new LanceRefsResponse(indexName, tableUri, tags, branches);
    }
}
