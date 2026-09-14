/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    }

    private void syncTable(String rootUri, String tableName) {
        String table = rootUri + "/" + tableName + ".lance";
        String indexName = tableName;
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
                        // re-run the index builder before exposing the new version, so
                        // the reader sees fully-covered indices for the newly appended
                        // fragments; an overwrite may have dropped indexes too.
                        //
                        // Two steps in tandem:
                        // (1) optimize existing indexes so the appended fragments
                        // are folded in via Lance's incremental merge, and
                        // (2) ensure indexes on columns that still lack one.
                        // Without step (1) the `wait` policy was equivalent to
                        // `immediate` for any column that already carried an index
                        // (the append never made it into the index, so the reader
                        // saw the flat-scan fallback all the same).
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
                        List<String> ftsBuilt = LanceIndexBuilder.ensureFtsIndexes(
                            dataset,
                            derivation.ftsColumns(),
                            builderMaxRows,
                            Optional.empty()
                        );
                        List<String> scalarBuilt = LanceIndexBuilder.ensureScalarIndexes(
                            dataset,
                            derivation.scalarColumns(),
                            builderMaxRows,
                            Optional.empty()
                        );
                        List<String> vectorBuilt = LanceIndexBuilder.ensureVectorIndexes(
                            dataset,
                            derivation.vectorColumns(),
                            builderMaxRows,
                            Optional.empty()
                        );
                        if (!ftsBuilt.isEmpty()
                            || !scalarBuilt.isEmpty()
                            || !vectorBuilt.isEmpty()
                            || !ftsOptimized.isEmpty()
                            || !scalarOptimized.isEmpty()
                            || !vectorOptimized.isEmpty()) {
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
                        LOG.warn("mapping re-derivation failed for {} at version {}: {}", indexName, latest, e.getMessage());
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
            // Derive first so the builder only sees the columns that derived to
            // lance_text / scalar-eligible / vector-eligible; other columns stay
            // untouched.
            derivation = RestAttachAction.derive(dataset, null);
            List<String> ftsBuilt = LanceIndexBuilder.ensureFtsIndexes(dataset, derivation.ftsColumns(), builderMaxRows, Optional.empty());
            List<String> scalarBuilt = LanceIndexBuilder.ensureScalarIndexes(
                dataset,
                derivation.scalarColumns(),
                builderMaxRows,
                Optional.empty()
            );
            List<String> vectorBuilt = LanceIndexBuilder.ensureVectorIndexes(
                dataset,
                derivation.vectorColumns(),
                builderMaxRows,
                Optional.empty()
            );
            if (!ftsBuilt.isEmpty() || !scalarBuilt.isEmpty() || !vectorBuilt.isEmpty()) {
                LOG.info("index builder created fts={} scalar={} vector={} for table {}", ftsBuilt, scalarBuilt, vectorBuilt, table);
            }
        }
        client.admin()
            .indices()
            .create(
                new CreateIndexRequest(indexName).settings(
                    Settings.builder()
                        .put("index.number_of_shards", derivation.shards())
                        .put("index.number_of_replicas", 0)
                        .put(LanceEngineFactory.TABLE_SETTING, table)
                        .put(LanceEngineFactory.PRIMARY_KEY_FIELD_SETTING, derivation.keyField())
                        .build()
                ).mapping(derivation.mappingJson())
            )
            .actionGet();
        servedVersions.put(indexName, derivation.version());
        LOG.info("surfaced table {} as index {} (version {}, {} shards)", table, indexName, derivation.version(), derivation.shards());
    }

    void recordServedVersion(String indexName, long version) {
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
        for (LanceField field : lanceSchema.fields()) {
            String previousName = mappingFieldIdToName.get(field.getId());
            if (previousName == null || previousName.equals(field.getName())) {
                continue;
            }
            String key = indexName + ":" + field.getId() + ":" + previousName + "->" + field.getName();
            if (warnedRenamed.add(key)) {
                LOG.warn(
                    "Lance table for {} renamed field id {} from '{}' to '{}'; the mapping still exposes the old name, "
                        + "queries against the new name will not match until the index is recreated",
                    indexName,
                    field.getId(),
                    previousName,
                    field.getName()
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
