/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.attach;

import java.util.Optional;

import org.lance.Dataset;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.lance.LanceInternalHeaders;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.namespace.AllowedTableRoots;
import org.opensearch.lance.namespace.LanceNamespaceService;
import org.opensearch.lance.rest.RestAttachAction;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

/**
 * Serves {@link LanceAttachAction}: opens the table, derives the
 * mapping, creates the index, and registers it with the namespace
 * poller. Running this behind a transport action places it after the
 * {@link ActionFilters} chain, so a security plugin rejects a caller
 * without {@code cluster:admin/lance/attach} before the plugin touches
 * the table.
 *
 * <p>Threading: {@link Dataset#open} and the schema walk block on
 * native I/O, so {@link #doExecute} hands the whole operation to the
 * generic pool instead of running it on the transport thread the REST
 * layer called from.
 *
 * <p>The index is created through the node client with the internal
 * create-index header stamped on a stashed thread context. The
 * caller's privilege has already been evaluated against the attach
 * action; the create runs as the plugin so a role does not also need
 * {@code indices:admin/create} on a name derived from the table path.
 *
 * <p>Existing-index handling: when the target index already exists the
 * action reads its settings and only reports {@code already_attached}
 * if the existing index is a Lance index pointing to the same table.
 * Any other clash (plain index reusing the name, Lance index for a
 * different table) is a 409 so the operator picks a different name
 * explicitly.
 */
public final class TransportLanceAttachAction extends HandledTransportAction<LanceAttachRequest, LanceAttachResponse> {

    private final ThreadPool threadPool;
    private final Client client;
    private final LanceNamespaceService namespaceService;
    private final AllowedTableRoots allowedRoots;

    @Inject
    public TransportLanceAttachAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Client client,
        LanceNamespaceService namespaceService,
        AllowedTableRoots allowedRoots
    ) {
        super(LanceAttachAction.NAME, transportService, actionFilters, LanceAttachRequest::new, ThreadPool.Names.GENERIC);
        this.threadPool = threadPool;
        this.client = client;
        this.namespaceService = namespaceService;
        this.allowedRoots = allowedRoots;
    }

    @Override
    protected void doExecute(Task task, LanceAttachRequest request, ActionListener<LanceAttachResponse> listener) {
        threadPool.executor(ThreadPool.Names.GENERIC).execute(ActionRunnable.wrap(listener, l -> attach(request, l)));
    }

    private void attach(LanceAttachRequest request, ActionListener<LanceAttachResponse> listener) throws Exception {
        String table = request.table();
        if (!allowedRoots.allows(table)) {
            throw new OpenSearchStatusException(
                "table [" + table + "] is not under any of the configured lance.allowed_table_roots",
                RestStatus.FORBIDDEN
            );
        }
        String indexName = request.indexName() != null ? request.indexName() : tableName(table);
        RestAttachAction.Derivation derivation;
        try (Dataset dataset = LanceRegistry.openDataset(table, request.storageOptions(), request.pinnedVersion())) {
            derivation = RestAttachAction.derive(dataset, request.multiFields());
        }
        createIndex(indexName, table, derivation, request.storageOptions(), request.pinnedVersion(), listener);
    }

    private void createIndex(
        String indexName,
        String table,
        RestAttachAction.Derivation derivation,
        StorageOptions storageOptions,
        Optional<Long> pinnedVersion,
        ActionListener<LanceAttachResponse> listener
    ) {
        Settings.Builder settings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put(LanceEngineFactory.TABLE_SETTING, table)
            .put(LanceEngineFactory.PRIMARY_KEY_FIELD_SETTING, derivation.keyField())
            .put(LanceEngineFactory.PRIMARY_KEY_TYPE_SETTING, derivation.keyFieldType());
        if (!derivation.multiFieldsJson().isEmpty()) {
            settings.put(LanceEngineFactory.MULTI_FIELDS_SETTING, derivation.multiFieldsJson());
        }
        pinnedVersion.ifPresent(v -> settings.put(LanceEngineFactory.VERSION_SETTING, v));
        storageOptions.writeToSettings(settings);
        CreateIndexRequest create = new CreateIndexRequest(indexName).settings(settings.build()).mapping(derivation.mappingJson());

        // LanceCreateIndexActionFilter blocks user PUT /{index} that
        // tries to set index.lance.table. Stamp the internal header so
        // this plugin-issued call is recognised as legitimate. The stash
        // also drops the caller's identity for this one call, which is
        // intended: the caller was authorised against the attach action,
        // not against creating an index of this name.
        ThreadContext threadContext = client.threadPool().getThreadContext();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader(LanceInternalHeaders.LANCE_INTERNAL_CREATE_INDEX, "true");
            client.admin().indices().create(create, new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse response) {
                    // Register the attach-created index with the namespace
                    // poller only when the operator is following the latest
                    // version. Pinned indices stay on their manifest version
                    // by design (readonly snapshot for reproducibility), so
                    // the poll cycle does not need to touch them and would
                    // otherwise burn cycles probing for a manifest advance
                    // that must not change the reader.
                    if (pinnedVersion.isEmpty()) {
                        namespaceService.registerAttachedIndex(indexName, table, derivation.version(), storageOptions);
                    }
                    listener.onResponse(response(indexName, table, derivation, false));
                }

                @Override
                public void onFailure(Exception e) {
                    if (!isAlreadyExists(e)) {
                        listener.onFailure(e);
                        return;
                    }
                    // The index already exists. Verify it is a Lance index
                    // for the same table before claiming success; otherwise
                    // attach would silently take credit for an unrelated
                    // index.
                    verifyExistingLanceIndex(indexName, table, derivation, storageOptions, pinnedVersion, listener);
                }
            });
        }
    }

    private void verifyExistingLanceIndex(
        String indexName,
        String table,
        RestAttachAction.Derivation derivation,
        StorageOptions storageOptions,
        Optional<Long> pinnedVersion,
        ActionListener<LanceAttachResponse> listener
    ) {
        ClusterStateRequest stateRequest = new ClusterStateRequest();
        stateRequest.clear().metadata(true).indices(indexName);
        client.admin().cluster().state(stateRequest, ActionListener.wrap((ClusterStateResponse response) -> {
            IndexMetadata md = response.getState().metadata().index(indexName);
            if (md == null) {
                // Race: the index disappeared between create and state.
                // Treat as conflict rather than pretend attach succeeded.
                throw new OpenSearchStatusException("index " + indexName + " conflicts with a concurrent request", RestStatus.CONFLICT);
            }
            String existing = md.getSettings().get(LanceEngineFactory.TABLE_SETTING);
            if (existing == null) {
                throw new OpenSearchStatusException(
                    "index " + indexName + " already exists and is not a Lance index; choose a different `name`",
                    RestStatus.CONFLICT
                );
            }
            if (!existing.equals(table)) {
                throw new OpenSearchStatusException(
                    "index " + indexName + " already attached to a different table: " + existing,
                    RestStatus.CONFLICT
                );
            }
            // Same table, so record the (index, table) pair with the
            // namespace poller in case this node has forgotten it
            // (cluster restart after attach, for example). Pinned
            // indices are readonly snapshots and stay outside the poll
            // cycle so a manifest advance does not race with the
            // intended version.
            if (pinnedVersion.isEmpty()) {
                namespaceService.registerAttachedIndex(indexName, table, derivation.version(), storageOptions);
            }
            listener.onResponse(response(indexName, table, derivation, true));
        }, listener::onFailure));
    }

    private static LanceAttachResponse response(
        String indexName,
        String table,
        RestAttachAction.Derivation derivation,
        boolean alreadyAttached
    ) {
        return new LanceAttachResponse(
            indexName,
            table,
            derivation.version(),
            derivation.rows(),
            derivation.fragments(),
            derivation.keyField(),
            derivation.mappingJson(),
            derivation.notes(),
            alreadyAttached
        );
    }

    private static boolean isAlreadyExists(Throwable e) {
        Throwable cursor = e;
        while (cursor != null) {
            if (cursor instanceof ResourceAlreadyExistsException) {
                return true;
            }
            cursor = cursor.getCause();
        }
        return false;
    }

    private static String tableName(String table) {
        String base = table.substring(table.lastIndexOf('/') + 1);
        return base.endsWith(".lance") ? base.substring(0, base.length() - 6) : base;
    }
}
