/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.Dataset;
import org.lance.namespace.DirectoryNamespace;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;
import org.lance.schema.LanceField;
import org.lance.schema.LanceSchema;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

/**
 * The RFC's namespace registration.
 *
 * <p>A registered catalog is polled at a configurable cadence via the Lance
 * Namespace API. Tables in the catalog surface as OpenSearch indexes
 * automatically, with mappings and shard counts derived from the Lance
 * schema. Version changes propagate through a shard-level refresh that
 * swaps the reader without closing the index. Today the implementation
 * uses {@link DirectoryNamespace} for filesystem catalogs; the same
 * abstraction will accept REST catalogs (Glue, Unity, Iceberg REST) as
 * the Lance Namespace ecosystem matures.
 */
public final class LanceNamespaceService {

    private static final Logger LOG = LogManager.getLogger(LanceNamespaceService.class);

    private final Client client;
    private final TimeValue cadence;
    private final long builderMaxRows;
    private final List<RegisteredNamespace> namespaces = new CopyOnWriteArrayList<>();
    private final Map<String, Long> servedVersions = new ConcurrentHashMap<>();
    // Index names created via /_lance/attach along with the absolute Lance
    // table path they point at. Tracked here so poll() can extend append
    // coverage to attach-only indexes and not just namespace-registered
    // tables (see C6: the previous behaviour was that appending to a table
    // whose index came from attach never surfaced through _search until the
    // operator hit refresh manually).
    private final Map<String, String> attachedIndexes = new ConcurrentHashMap<>();
    // Index names we've already flagged as unowned, so the poll doesn't shout
    // the same warning every ten seconds. Cleared if the collision resolves.
    private final Set<String> warnedUnowned = ConcurrentHashMap.newKeySet();
    // Track rename warnings so a table that renamed the same field is not
    // logged on every poll. Keyed by "indexName:fieldId:oldName->newName" so
    // the same rename fires once, but a later re-rename still warns.
    private final Set<String> warnedRenamed = ConcurrentHashMap.newKeySet();

    public LanceNamespaceService(Client client, ThreadPool threadPool, TimeValue cadence, long builderMaxRows) {
        this.client = client;
        this.cadence = cadence;
        this.builderMaxRows = builderMaxRows;
        threadPool.scheduleWithFixedDelay(this::poll, cadence, ThreadPool.Names.GENERIC);
    }

    /** Namespace poll cadence in effect for this service. */
    public TimeValue cadence() {
        return cadence;
    }

    public void register(String rootUri) {
        for (RegisteredNamespace ns : namespaces) {
            if (ns.rootUri.equals(rootUri)) {
                return;
            }
        }
        try {
            DirectoryNamespace namespace = new DirectoryNamespace();
            Map<String, String> config = new HashMap<>();
            config.put("root", rootUri);
            namespace.initialize(config, LanceRegistry.allocator());
            namespaces.add(new RegisteredNamespace(rootUri, namespace));
        } catch (Exception e) {
            LOG.warn("failed to initialise namespace {} through DirectoryNamespace", rootUri, e);
        }
    }

    public List<String> namespaces() {
        List<String> uris = new ArrayList<>(namespaces.size());
        for (RegisteredNamespace ns : namespaces) {
            uris.add(ns.rootUri);
        }
        return List.copyOf(uris);
    }

    private void poll() {
        for (RegisteredNamespace ns : namespaces) {
            try {
                ListTablesResponse response = ns.namespace.listTables(new ListTablesRequest());
                Set<String> tables = response.getTables();
                if (tables == null) {
                    continue;
                }
                for (String tableName : tables) {
                    syncTable(ns.rootUri, tableName);
                }
            } catch (Exception e) {
                LOG.warn("namespace poll failed for {}", ns.rootUri, e);
            }
        }
        // Sync attach-created indexes so append fragments surface on the
        // same schedule as namespace-registered tables. attach records the
        // (indexName -> tablePath) pair; the sync path is the same, just
        // without the rootUri / tableName join namespace tables use.
        for (Map.Entry<String, String> entry : attachedIndexes.entrySet()) {
            try {
                syncAttachedTable(entry.getKey(), entry.getValue());
            } catch (Exception e) {
                LOG.warn("attach poll failed for index {} at {}", entry.getKey(), entry.getValue(), e);
            }
        }
    }

    private void syncTable(String rootUri, String tableName) {
        String table = rootUri + "/" + tableName + ".lance";
        runSyncCycle(table, tableName);
    }

    // Attach-created indexes carry the fully-qualified table path already,
    // so the rootUri / tableName join namespace tables use doesn't apply.
    // Everything downstream of the path resolution is identical.
    private void syncAttachedTable(String indexName, String tablePath) {
        runSyncCycle(tablePath, indexName);
    }

    private void runSyncCycle(String table, String indexName) {
        try {
            boolean exists = client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists();
            if (!exists) {
                surface(indexName, table);
                return;
            }
            Long served = servedVersions.get(indexName);
            if (served == null) {
                // Two ways to get here: the index name already existed before we saw
                // the table (a classic OpenSearch index or another namespace beat us
                // to the name), or the cluster restarted and we have not been asked
                // to re-register. Both are recoverable with operator action, so we
                // log once per index instead of silently skipping every poll.
                if (warnedUnowned.add(indexName)) {
                    LOG.warn(
                        "skipping table {}: index {} exists but is not tracked by this namespace "
                            + "(name collision or cluster restart without re-registration)",
                        table,
                        indexName
                    );
                }
                return;
            }
            // In case the collision has resolved (index deleted and re-created by
            // us), allow future warnings again.
            warnedUnowned.remove(indexName);
            String policy = readUncoveredFragmentPolicy(indexName);
            long latest;
            String rederivedMappingJson = null;
            try (Dataset dataset = Dataset.open().allocator(LanceRegistry.allocator()).uri(table).build()) {
                latest = dataset.version();
                if (latest > served) {
                    // The RFC's Mapping interface states the mapping is re-derived at
                    // every checkout. We derive first so the builder only touches
                    // columns that derived to lance_text; keyword columns stay untouched.
                    RestAttachAction.Derivation derivation = RestAttachAction.derive(dataset, null);
                    rederivedMappingJson = derivation.mappingJson();
                    warnOnLanceFieldRename(indexName, dataset.getLanceSchema());
                    if ("wait".equals(policy)) {
                        // Extend every existing index so appended fragments are
                        // folded in before the new version is exposed. Creating
                        // new indexes automatically is disabled (see C3 / C9):
                        // the old auto path built an IVF_PQ with
                        // numPartitions=1 that quietly degraded recall and
                        // committed BTrees that then blocked subsequent
                        // alter_columns. Operators trigger index builds
                        // explicitly through POST /_lance/build_indexes.
                        List<String> ftsOptimized = LanceIndexBuilder.optimizeExistingFtsIndexes(dataset, derivation.ftsColumns(), false);
                        List<String> scalarOptimized = LanceIndexBuilder.optimizeExistingScalarIndexes(
                            dataset,
                            derivation.scalarColumns(),
                            false
                        );
                        List<String> vectorOptimized = LanceIndexBuilder.optimizeExistingVectorIndexes(
                            dataset,
                            derivation.vectorColumns(),
                            false
                        );
                        if (!ftsOptimized.isEmpty() || !scalarOptimized.isEmpty() || !vectorOptimized.isEmpty()) {
                            latest = dataset.version();
                        }
                    }
                    // else "immediate": expose the new version at once and let Lance
                    // fall back to scan evaluation on any fragment the index does not
                    // cover yet; the builder catches up on a subsequent overwrite.
                }
            }
            if (latest > served) {
                LOG.info("table {} moved to version {} (serving {}), refreshing {}", table, latest, served, indexName);
                if (rederivedMappingJson != null) {
                    try {
                        client.admin()
                            .indices()
                            .preparePutMapping(indexName)
                            .setSource(rederivedMappingJson, MediaTypeRegistry.JSON)
                            .execute()
                            .actionGet();
                    } catch (Exception e) {
                        String message = e.getMessage() == null ? "" : e.getMessage();
                        if (message.contains("cannot be changed from type")) {
                            // A Utf8 column has flipped between keyword and
                            // lance_text after the user created / dropped an
                            // FTS index on the Lance side. PutMapping refuses
                            // the type change, but leaving the stale mapping
                            // in place makes the column silently unsearchable.
                            // Rebuild the index (delete + recreate with the
                            // new mapping) so the reader sees the correct
                            // field type. The underlying Lance table keeps
                            // its data intact, so nothing is lost.
                            LOG.warn(
                                "mapping re-derivation for {} at version {} hit a keyword <-> lance_text type change ({}); "
                                    + "rebuilding the OpenSearch index (Lance data is untouched)",
                                indexName,
                                latest,
                                message
                            );
                            try {
                                client.admin()
                                    .indices()
                                    .delete(new org.opensearch.action.admin.indices.delete.DeleteIndexRequest(indexName))
                                    .actionGet();
                                servedVersions.remove(indexName);
                                surface(indexName, table);
                                return;
                            } catch (Exception rebuild) {
                                LOG.warn("rebuild after type change failed for {}: {}", indexName, rebuild.getMessage());
                            }
                        } else {
                            LOG.warn("mapping re-derivation failed for {} at version {}: {}", indexName, latest, message);
                        }
                    }
                }
                client.admin().indices().refresh(new RefreshRequest(indexName)).actionGet();
                servedVersions.put(indexName, latest);
            }
        } catch (Exception e) {
            LOG.warn("sync failed for table {}", table, e);
        }
    }

    private void surface(String indexName, String table) throws Exception {
        RestAttachAction.Derivation derivation;
        try (Dataset dataset = Dataset.open().allocator(LanceRegistry.allocator()).uri(table).build()) {
            // Derive first so the CreateIndex settings and mapping reflect
            // the current Lance schema. Automatic index creation is off by
            // default (see C3 / C9); operators build indexes explicitly
            // through POST /_lance/build_indexes.
            derivation = RestAttachAction.derive(dataset, null);
        }
        // Fire the CreateIndex asynchronously so a red shard on this table
        // does not block the poll thread for 30 seconds waiting for ack.
        // Every other table in the same namespace was previously stuck
        // behind that block. See issue #29.
        final long version = derivation.version();
        final int shards = derivation.shards();
        client.admin()
            .indices()
            .create(
                new CreateIndexRequest(indexName).settings(
                    Settings.builder()
                        .put("index.number_of_shards", shards)
                        .put("index.number_of_replicas", 0)
                        .put(LanceEngineFactory.TABLE_SETTING, table)
                        .put(LanceEngineFactory.PRIMARY_KEY_FIELD_SETTING, derivation.keyField())
                        .build()
                ).mapping(derivation.mappingJson()),
                new ActionListener<org.opensearch.action.admin.indices.create.CreateIndexResponse>() {
                    @Override
                    public void onResponse(org.opensearch.action.admin.indices.create.CreateIndexResponse response) {
                        servedVersions.put(indexName, version);
                        LOG.info("surfaced table {} as index {} (version {}, {} shards)", table, indexName, version, shards);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // ResourceAlreadyExistsException means another node
                        // (or an earlier poll) already surfaced the table;
                        // the syncTable path will pick it up next cycle.
                        Throwable cursor = e;
                        while (cursor != null) {
                            if (cursor instanceof org.opensearch.ResourceAlreadyExistsException) {
                                LOG.debug("surface for {} raced with an existing index", indexName);
                                return;
                            }
                            cursor = cursor.getCause();
                        }
                        LOG.warn("surface failed for {} at version {}: {}", indexName, version, e.getMessage());
                    }
                }
            );
    }

    void recordServedVersion(String indexName, long version) {
        servedVersions.put(indexName, version);
    }

    /**
     * Records an attach-created index and its Lance table path so poll()
     * can pick up appends for it, just as it would for a namespace-registered
     * table. Idempotent: repeated calls with the same (indexName, tablePath)
     * are a no-op beyond overwriting the served version.
     */
    public void registerAttachedIndex(String indexName, String tablePath, long version) {
        attachedIndexes.put(indexName, tablePath);
        servedVersions.put(indexName, version);
    }

    /**
     * Compare the Lance table's current schema against the OpenSearch mapping
     * to spot column renames. Field ids are immutable on the Lance side, so a
     * pairing of "same id, different name" means the writer renamed a column
     * since we last checked out.
     *
     * <p>Reacting to the rename (rewriting the mapping to the new name) is a
     * follow-up; today we log a warning per rename per session so operators
     * see it and can decide whether to recreate the index.
     */
    private void warnOnLanceFieldRename(String indexName, LanceSchema lanceSchema) {
        Map<Integer, String> mappingFieldIdToName;
        try {
            mappingFieldIdToName = readMappingFieldIds(indexName);
        } catch (Exception e) {
            LOG.debug("could not inspect mapping meta for {}: {}", indexName, e.getMessage());
            return;
        }
        if (mappingFieldIdToName.isEmpty()) {
            return;
        }
        java.util.Set<Integer> lanceIds = new java.util.HashSet<>(lanceSchema.fields().size());
        for (LanceField field : lanceSchema.fields()) {
            lanceIds.add(field.getId());
            String previousName = mappingFieldIdToName.get(field.getId());
            if (previousName == null || previousName.equals(field.getName())) {
                continue;
            }
            String key = indexName + ":rename:" + field.getId() + ":" + previousName + "->" + field.getName();
            if (warnedRenamed.add(key)) {
                LOG.warn(
                    "Lance table for {} renamed field id {} from '{}' to '{}'. "
                        + "The mapping still exposes both names; queries against the old name '{}' will now return 0 hits "
                        + "because the underlying column no longer maps to it. Recreate the index to drop the stale mapping.",
                    indexName,
                    field.getId(),
                    previousName,
                    field.getName(),
                    previousName
                );
            }
        }
        // drop_columns / overwrite on the Lance side removes a field id
        // entirely. OpenSearch's PutMapping cannot remove properties, so
        // the stale name lingers and queries against it fail silently with
        // 0 hits (numeric doc values just return their default, term
        // queries never match). Surface the drift so operators know to
        // recreate the index.
        for (Map.Entry<Integer, String> mapped : mappingFieldIdToName.entrySet()) {
            int id = mapped.getKey();
            if (lanceIds.contains(id)) {
                continue;
            }
            String staleName = mapped.getValue();
            String key = indexName + ":dropped:" + id + ":" + staleName;
            if (warnedRenamed.add(key)) {
                LOG.warn(
                    "Lance table for {} no longer contains field id {} ('{}'). "
                        + "The mapping still exposes '{}' so queries against it will silently return 0 hits; recreate the index to drop it.",
                    indexName,
                    id,
                    staleName,
                    staleName
                );
            }
        }
    }

    /**
     * Read the {@code meta.lance_field_id} value out of every top-level field
     * in the current mapping. Returns a map from Lance field id to the name
     * the field currently has in the OpenSearch mapping. Fields without
     * {@code meta.lance_field_id} (older indexes, non-Lance mappings) are
     * skipped.
     */
    private Map<Integer, String> readMappingFieldIds(String indexName) {
        GetMappingsResponse response = client.admin().indices().prepareGetMappings(indexName).execute().actionGet();
        MappingMetadata mapping = response.mappings().get(indexName);
        if (mapping == null) {
            return Map.of();
        }
        Map<String, Object> source = mapping.sourceAsMap();
        Object properties = source.get("properties");
        if (!(properties instanceof Map<?, ?> propsMap)) {
            return Map.of();
        }
        Map<Integer, String> result = new HashMap<>();
        for (Map.Entry<?, ?> entry : propsMap.entrySet()) {
            if (!(entry.getKey() instanceof String name) || !(entry.getValue() instanceof Map<?, ?> field)) {
                continue;
            }
            Object meta = field.get("meta");
            if (!(meta instanceof Map<?, ?> metaMap)) {
                continue;
            }
            Object raw = metaMap.get("lance_field_id");
            if (!(raw instanceof String s)) {
                continue;
            }
            try {
                result.put(Integer.parseInt(s), name);
            } catch (NumberFormatException e) {
                // A hand-crafted mapping may put anything here; skip malformed entries.
            }
        }
        return result;
    }

    private String readUncoveredFragmentPolicy(String indexName) {
        try {
            var state = client.admin().cluster().prepareState().execute().actionGet().getState();
            var metadata = state.metadata().index(indexName);
            if (metadata == null) {
                return "wait";
            }
            return metadata.getSettings().get("index.lance.uncovered_fragment_policy", "wait");
        } catch (Exception e) {
            LOG.warn("failed to read uncovered_fragment_policy for {}: {}", indexName, e.getMessage());
            return "wait";
        }
    }

    private record RegisteredNamespace(String rootUri, DirectoryNamespace namespace) {
    }
}
