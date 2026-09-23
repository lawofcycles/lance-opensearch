/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.lance.LanceInternalHeaders;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.rest.RestAttachAction;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

/**
 * The plugin internal {@code CreateIndex} for a Lance backed index: the
 * request shape shared by the namespace surface step (a table without an
 * index yet) and the freshness rebuild (an index whose mapping cannot be
 * updated in place), and the dispatch that stamps the internal header the
 * create index filter looks for.
 */
final class LanceIndexCreation {

    private LanceIndexCreation() {}

    /**
     * A single shard, no replica index over {@code table} whose settings
     * persist what the derivation needs to run again later: the table
     * URI, the primary key column and type, the overrides and the storage
     * options. {@code carried} are further {@code index.lance.*} settings
     * of a previous incarnation of the index (the tag it follows, the
     * uncovered fragment policy) that a rebuild keeps; the settings the
     * derivation recomputes win over it.
     */
    static CreateIndexRequest request(
        String indexName,
        String table,
        StorageOptions storageOptions,
        RestAttachAction.Derivation derivation,
        Settings carried
    ) {
        Settings.Builder settings = Settings.builder();
        // Copied key by key: the previous settings may be the shard's
        // merged node and index settings, and the node's secure settings
        // must not travel into a CreateIndex request.
        for (String key : carried.keySet()) {
            if (key.startsWith("index.lance.")) {
                settings.put(key, carried.get(key));
            }
        }
        settings.put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put(LanceEngineFactory.TABLE_SETTING, table)
            .put(LanceEngineFactory.PRIMARY_KEY_FIELD_SETTING, derivation.keyField())
            .put(LanceEngineFactory.PRIMARY_KEY_TYPE_SETTING, derivation.keyFieldType());
        if (derivation.overridesJson().isEmpty()) {
            settings.remove(LanceEngineFactory.OVERRIDES_SETTING);
        } else {
            settings.put(LanceEngineFactory.OVERRIDES_SETTING, derivation.overridesJson());
        }
        storageOptions.writeToSettings(settings);
        return new CreateIndexRequest(indexName).settings(settings.build()).mapping(derivation.mappingJson());
    }

    /**
     * Send {@code request} with the internal create index header set, so
     * the filter that blocks user {@code PUT /{index}} with
     * {@code index.lance.table} lets it through. The caller's context is
     * stashed for the call and restored afterwards.
     */
    static void create(Client client, ThreadPool threadPool, CreateIndexRequest request, ActionListener<CreateIndexResponse> listener) {
        ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader(LanceInternalHeaders.LANCE_INTERNAL_CREATE_INDEX, "true");
            client.admin().indices().create(request, listener);
        }
    }
}
